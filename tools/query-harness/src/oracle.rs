mod fixture;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use chrono::{Datelike, SecondsFormat, TimeZone, Timelike, Utc};
use clap::Args;
use regex::Regex;
use reqwest::blocking::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use wait_timeout::ChildExt;

#[derive(Args)]
pub(crate) struct OracleArgs {
    #[arg(value_enum)]
    command: OracleCommand,
    #[arg(long, default_value = "tests/query_oracles/manifest.json")]
    manifest: String,
    #[arg(long, default_value = "docker")]
    runtime: String,
}

#[derive(Clone, clap::ValueEnum)]
enum OracleCommand {
    Validate,
    Probe,
    PrometheusSmoke,
    PrometheusApi,
    VictoriaMetricsApi,
    VictoriaLogsApi,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OracleManifest {
    schema_version: u64,
    selected_at: String,
    oracles: BTreeMap<String, OracleDefinition>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OracleDefinition {
    language: String,
    role: String,
    version: String,
    source_commit: String,
    image: String,
    linux_amd64_digest: String,
    version_entrypoint: String,
    version_args: Vec<String>,
    version_contains: String,
    fixtures: Vec<String>,
}

type OperatorExpectation = (String, Map<String, Value>, String, Vec<Value>);

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn validate_manifest(root: &Path, manifest: &OracleManifest) -> Result<Vec<String>> {
    let sha = Regex::new(r"^sha256:[0-9a-f]{64}$")?;
    let commit = Regex::new(r"^[0-9a-f]{40}$")?;
    let mut errors = Vec::new();
    if manifest.schema_version != 1 {
        errors.push("manifest schema_version must be 1".to_owned());
    }
    let expected: BTreeSet<_> = ["prometheus", "victoriametrics", "victorialogs"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    let actual: BTreeSet<_> = manifest.oracles.keys().cloned().collect();
    if actual != expected {
        errors.push(
            "manifest must define exactly prometheus, victoriametrics, and victorialogs".to_owned(),
        );
        return Ok(errors);
    }
    let docs = fs::read_to_string(root.join("docs/QUERY_ORACLES.md"))?;
    let mut images = BTreeSet::new();
    for (name, oracle) in &manifest.oracles {
        let prefix = format!("oracle {name}");
        if !oracle.image.contains("@sha256:")
            || oracle
                .image
                .rsplit_once('@')
                .is_none_or(|(_, digest)| digest.is_empty())
        {
            errors.push(format!(
                "{prefix}: image must use an immutable sha256 digest"
            ));
        }
        if oracle.image.contains(":latest") || !oracle.image.contains('@') {
            errors.push(format!("{prefix}: floating image reference is forbidden"));
        }
        if !images.insert(oracle.image.clone()) {
            errors.push(format!("{prefix}: duplicate image pin"));
        }
        if !commit.is_match(&oracle.source_commit) {
            errors.push(format!(
                "{prefix}: source_commit must be a 40-character lowercase SHA"
            ));
        }
        if !sha.is_match(&oracle.linux_amd64_digest) {
            errors.push(format!(
                "{prefix}: linux_amd64_digest must be sha256:<64 hex>"
            ));
        }
        if oracle.version_contains.is_empty() {
            errors.push(format!("{prefix}: version_contains is required"));
        }
        for field in [
            &oracle.version,
            &oracle.source_commit,
            &oracle.image,
            &oracle.linux_amd64_digest,
        ] {
            if !field.is_empty() && !docs.contains(field) {
                errors.push(format!(
                    "{prefix}: docs/QUERY_ORACLES.md is missing {field}"
                ));
            }
        }
        for relative in &oracle.fixtures {
            if !root.join(relative).is_file() {
                errors.push(format!("{prefix}: missing fixture {relative}"));
            }
        }
    }
    Ok(errors)
}

fn command_output(program: &str, args: &[String], timeout: Duration) -> Result<Output> {
    let mut child = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("start {program}"))?;
    if child.wait_timeout(timeout)?.is_none() {
        child.kill().ok();
        let _ = child.wait();
        bail!("{program} timed out after {} seconds", timeout.as_secs());
    }
    child.wait_with_output().context("collect child output")
}

fn container_args(oracle: &OracleDefinition, extra: &[String]) -> Vec<String> {
    let mut args = vec![
        "run".to_owned(),
        "--rm".to_owned(),
        "--platform".to_owned(),
        "linux/amd64".to_owned(),
    ];
    if !oracle.version_entrypoint.is_empty() {
        args.extend(["--entrypoint".to_owned(), oracle.version_entrypoint.clone()]);
    }
    args.push(oracle.image.clone());
    args.extend_from_slice(extra);
    args
}

fn probe(runtime: &str, manifest: &OracleManifest) -> Result<()> {
    let mut failures = 0;
    for (name, oracle) in &manifest.oracles {
        let output = command_output(
            runtime,
            &container_args(oracle, &oracle.version_args),
            Duration::from_secs(180),
        )?;
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !output.status.success() || !combined.contains(&oracle.version_contains) {
            eprintln!(
                "{name}: version probe failed ({})\n{}",
                output.status,
                combined.trim()
            );
            failures += 1;
        } else {
            println!(
                "{name}: {}",
                combined
                    .lines()
                    .find(|line| !line.trim().is_empty())
                    .unwrap_or("")
            );
        }
    }
    if failures != 0 {
        bail!("{failures} oracle version probe(s) failed");
    }
    Ok(())
}

fn prometheus_smoke(root: &Path, runtime: &str, oracle: &OracleDefinition) -> Result<()> {
    let relative = oracle
        .fixtures
        .iter()
        .find(|path| path.ends_with("promql_smoke.yml"))
        .context("Prometheus smoke fixture is not declared")?;
    let fixture = root.join(relative).canonicalize()?;
    let args = vec![
        "run".to_owned(),
        "--rm".to_owned(),
        "--platform".to_owned(),
        "linux/amd64".to_owned(),
        "--entrypoint".to_owned(),
        "/bin/promtool".to_owned(),
        "--mount".to_owned(),
        format!(
            "type=bind,src={},dst=/work/promql_smoke.yml,readonly",
            fixture.display()
        ),
        oracle.image.clone(),
        "test".to_owned(),
        "rules".to_owned(),
        "/work/promql_smoke.yml".to_owned(),
    ];
    let output = command_output(runtime, &args, Duration::from_secs(180))?;
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    if !output.status.success() {
        bail!("Prometheus promtool smoke fixture failed");
    }
    Ok(())
}

fn free_port() -> Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

struct ContainerGuard<'a> {
    runtime: &'a str,
    name: String,
}

impl Drop for ContainerGuard<'_> {
    fn drop(&mut self) {
        let _ = command_output(
            self.runtime,
            &["rm".to_owned(), "-f".to_owned(), self.name.clone()],
            Duration::from_secs(30),
        );
    }
}

fn response_json(response: Response) -> Result<(u16, Value)> {
    let status = response.status().as_u16();
    let bytes = response.bytes()?;
    let body = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode HTTP {status} body as JSON"))?;
    Ok((status, body))
}

fn response_text(response: Response) -> Result<(u16, String, String)> {
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    Ok((status, content_type, response.text()?))
}

fn post_remote_write(client: &Client, base: &str, sample_timestamp_ms: i64) -> Result<()> {
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(fixture::prometheus_remote_write(sample_timestamp_ms))
        .send()?;
    if response.status().as_u16() != 204 {
        bail!("query oracle Remote Write returned {}", response.status());
    }
    Ok(())
}

fn query(
    client: &Client,
    base: &str,
    endpoint: &str,
    params: &Map<String, Value>,
) -> Result<(u16, Value)> {
    let pairs: Vec<(String, String)> = params
        .iter()
        .map(|(key, value)| {
            let value = value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string());
            (key.clone(), value)
        })
        .collect();
    response_json(
        client
            .get(format!("{base}{endpoint}"))
            .query(&pairs)
            .send()?,
    )
}

fn value_string(value: &Value) -> Result<String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => bail!("expected a string or number, got {value}"),
    }
}

fn timestamp(ms: i64) -> Value {
    if ms % 1_000 == 0 {
        json!(ms / 1_000)
    } else {
        json!(ms as f64 / 1_000.0)
    }
}

fn offset(object: &Map<String, Value>, name: &str) -> Result<i64> {
    object
        .get(name)
        .and_then(Value::as_i64)
        .with_context(|| format!("case is missing integer {name}"))
}

fn values(object: &Map<String, Value>, name: &str) -> Result<Vec<Value>> {
    object
        .get(name)
        .and_then(Value::as_array)
        .cloned()
        .with_context(|| format!("case is missing array {name}"))
}

fn string(object: &Map<String, Value>, name: &str) -> Result<String> {
    object
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("case is missing string {name}"))
}

fn case_id(case: &Map<String, Value>) -> &str {
    case.get("id")
        .and_then(Value::as_str)
        .unwrap_or("unknown-case")
}

fn object_cases<'a>(fixture: &'a Value, name: &str) -> Result<Vec<&'a Map<String, Value>>> {
    fixture
        .get(name)
        .and_then(Value::as_array)
        .with_context(|| format!("fixture is missing array {name}"))?
        .iter()
        .map(|value| {
            value
                .as_object()
                .with_context(|| format!("{name} contains a non-object case"))
        })
        .collect()
}

fn sorted_strings(value: Option<&Value>) -> Result<Vec<String>> {
    let mut values: Vec<String> = match value {
        None => Vec::new(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .context("annotation must be a string")
            })
            .collect::<Result<_>>()?,
        Some(_) => bail!("annotations must be an array"),
    };
    values.sort();
    Ok(values)
}

fn warning_contract_matches(case: &Map<String, Value>, actual: Option<&Value>) -> Result<bool> {
    let Some(contract) = case
        .get("expected_warning_contract")
        .map(|value| {
            value
                .as_object()
                .context("expected_warning_contract must be an object")
        })
        .transpose()?
    else {
        return Ok(sorted_strings(actual)? == sorted_strings(case.get("expected_warnings"))?);
    };
    if case.contains_key("expected_warnings") {
        bail!("expected_warnings and expected_warning_contract are mutually exclusive");
    }
    let expected_count = contract
        .get("count")
        .and_then(Value::as_u64)
        .context("expected_warning_contract.count must be an unsigned integer")?
        as usize;
    let required = sorted_strings(contract.get("required"))?;
    let allowed = sorted_strings(contract.get("allowed"))?;
    let actual = sorted_strings(actual)?;
    let unique_count = actual
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    Ok(actual.len() == expected_count
        && unique_count == actual.len()
        && required.iter().all(|warning| actual.contains(warning))
        && actual.iter().all(|warning| allowed.contains(warning)))
}

fn results_equal(expected: &[Value], actual: &Value, ordered: bool) -> bool {
    let Some(actual) = actual.as_array() else {
        return false;
    };
    if expected.len() != actual.len() {
        return false;
    }
    if ordered {
        return expected == actual;
    }
    let mut expected: Vec<String> = expected
        .iter()
        .map(|value| serde_json::to_string(value).unwrap())
        .collect();
    let mut actual: Vec<String> = actual
        .iter()
        .map(|value| serde_json::to_string(value).unwrap())
        .collect();
    expected.sort();
    actual.sort();
    expected == actual
}

fn print_verdict(case: &Map<String, Value>, valid: bool, detail: impl FnOnce() -> String) -> usize {
    if valid {
        println!("{}: ok", case_id(case));
        0
    } else {
        eprintln!("{}: {}", case_id(case), detail());
        1
    }
}

fn exact_cases(client: &Client, base: &str, fixture: &Value) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "cases")? {
        let endpoint = string(case, "endpoint")?;
        let params = case
            .get("params")
            .and_then(Value::as_object)
            .context("case params")?;
        let (status, body) = query(client, base, &endpoint, params)?;
        let expected_status = case
            .get("status")
            .and_then(Value::as_u64)
            .context("case status")? as u16;
        let expected_body = case.get("body").context("case body")?;
        failures += print_verdict(
            case,
            status == expected_status && &body == expected_body,
            || format!("expected {expected_status} {expected_body}; got {status} {body}"),
        );
    }
    Ok(failures)
}

fn lookback_cases(client: &Client, base: &str, fixture: &Value, sample_ms: i64) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "lookback_cases")? {
        let evaluation_ms = sample_ms + offset(case, "evaluation_offset_ms")?;
        let params = Map::from_iter([
            ("query".to_owned(), json!("oracle_lookback")),
            ("time".to_owned(), timestamp(evaluation_ms)),
            (
                "lookback_delta".to_owned(),
                json!(string(case, "lookback_delta")?),
            ),
        ]);
        let (status, body) = query(client, base, "/api/v1/query", &params)?;
        let result = body.pointer("/data/result").and_then(Value::as_array);
        let expected_count = case
            .get("expected_result_count")
            .and_then(Value::as_u64)
            .context("expected_result_count")? as usize;
        let expected = if expected_count == 1 {
            vec![json!({
                "metric": {"__name__": "oracle_lookback", "job": "oracle"},
                "value": [timestamp(evaluation_ms), "7"]
            })]
        } else {
            Vec::new()
        };
        let valid = status == 200
            && body.get("status") == Some(&json!("success"))
            && body.pointer("/data/resultType") == Some(&json!("vector"))
            && result.is_some_and(|result| result.len() == expected_count && result == &expected);
        failures += print_verdict(case, valid, || {
            format!("unexpected response {status} {body}")
        });
    }
    Ok(failures)
}

fn temporal_cases(client: &Client, base: &str, fixture: &Value, sample_ms: i64) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "temporal_cases")? {
        let mut expression = string(case, "query")?;
        if case.contains_key("at_offset_ms") {
            expression = expression.replace(
                "{at}",
                &format_timestamp(sample_ms + offset(case, "at_offset_ms")?),
            );
        }
        let expected_values = values(case, "expected_values")?;
        let (endpoint, params, result_type, result) =
            if let Some(range) = case.get("range").and_then(Value::as_object) {
                let start_ms = sample_ms + offset(range, "start_offset_ms")?;
                let end_ms = sample_ms + offset(range, "end_offset_ms")?;
                let timestamps = evenly_spaced(start_ms, end_ms, expected_values.len());
                let points: Result<Vec<_>> = timestamps
                    .iter()
                    .zip(&expected_values)
                    .map(|(at, value)| Ok(json!([timestamp(*at), value_string(value)?])))
                    .collect();
                (
                    "/api/v1/query_range",
                    Map::from_iter([
                        ("query".to_owned(), json!(expression)),
                        ("start".to_owned(), timestamp(start_ms)),
                        ("end".to_owned(), timestamp(end_ms)),
                        ("step".to_owned(), json!(string(range, "step")?)),
                    ]),
                    "matrix",
                    vec![json!({
                        "metric": {"__name__": "oracle_temporal", "job": "oracle"},
                        "values": points?
                    })],
                )
            } else {
                let evaluation_ms = sample_ms + offset(case, "evaluation_offset_ms")?;
                (
                    "/api/v1/query",
                    Map::from_iter([
                        ("query".to_owned(), json!(expression)),
                        ("time".to_owned(), timestamp(evaluation_ms)),
                    ]),
                    "vector",
                    vec![json!({
                        "metric": {"__name__": "oracle_temporal", "job": "oracle"},
                        "value": [timestamp(evaluation_ms), value_string(&expected_values[0])?]
                    })],
                )
            };
        let (status, body) = query(client, base, endpoint, &params)?;
        let valid = status == 200
            && body.get("status") == Some(&json!("success"))
            && body.pointer("/data/resultType") == Some(&json!(result_type))
            && body.pointer("/data/result") == Some(&json!(result));
        failures += print_verdict(case, valid, || format!("expected {result:?}; got {body}"));
    }
    Ok(failures)
}

fn subquery_cases(client: &Client, base: &str, fixture: &Value, sample_ms: i64) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "subquery_cases")? {
        let evaluation_ms = sample_ms + offset(case, "evaluation_offset_ms")?;
        let mut expression = string(case, "query")?;
        if case.contains_key("at_offset_ms") {
            expression = expression.replace(
                "{at}",
                &format_timestamp(sample_ms + offset(case, "at_offset_ms")?),
            );
        }
        let (endpoint, params, result_type, result) =
            if let Some(range) = case.get("range").and_then(Value::as_object) {
                let start_ms = sample_ms + offset(range, "start_offset_ms")?;
                let end_ms = sample_ms + offset(range, "end_offset_ms")?;
                let expected_values = values(case, "expected_values")?;
                let timestamps = evenly_spaced(start_ms, end_ms, expected_values.len());
                let points: Result<Vec<_>> = timestamps
                    .iter()
                    .zip(&expected_values)
                    .map(|(at, value)| Ok(json!([timestamp(*at), value_string(value)?])))
                    .collect();
                (
                    "/api/v1/query_range",
                    Map::from_iter([
                        ("query".to_owned(), json!(expression)),
                        ("start".to_owned(), timestamp(start_ms)),
                        ("end".to_owned(), timestamp(end_ms)),
                        ("step".to_owned(), json!(string(range, "step")?)),
                    ]),
                    "matrix",
                    vec![json!({"metric": {"job": "oracle"}, "values": points?})],
                )
            } else if let Some(matrix) = case.get("expected_matrix").and_then(Value::as_array) {
                let mut metric = json!({"job": "oracle"});
                if !case
                    .get("drop_metric_name")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    metric["__name__"] = json!("oracle_temporal");
                }
                let points: Result<Vec<_>> = matrix
                    .iter()
                    .map(|point| {
                        let point = point.as_array().context("expected_matrix point")?;
                        let at = point[0].as_i64().context("expected_matrix offset")?;
                        Ok(json!([timestamp(sample_ms + at), value_string(&point[1])?]))
                    })
                    .collect();
                (
                    "/api/v1/query",
                    Map::from_iter([
                        ("query".to_owned(), json!(expression)),
                        ("time".to_owned(), timestamp(evaluation_ms)),
                    ]),
                    "matrix",
                    vec![json!({"metric": metric, "values": points?})],
                )
            } else {
                let expected = values(case, "expected_values")?;
                (
                    "/api/v1/query",
                    Map::from_iter([
                        ("query".to_owned(), json!(expression)),
                        ("time".to_owned(), timestamp(evaluation_ms)),
                    ]),
                    "vector",
                    vec![json!({
                        "metric": {"job": "oracle"},
                        "value": [timestamp(evaluation_ms), value_string(&expected[0])?]
                    })],
                )
            };
        let (status, body) = query(client, base, endpoint, &params)?;
        let valid = status == 200
            && body.get("status") == Some(&json!("success"))
            && body.pointer("/data/resultType") == Some(&json!(result_type))
            && body.pointer("/data/result") == Some(&json!(result));
        failures += print_verdict(case, valid, || format!("expected {result:?}; got {body}"));
    }
    Ok(failures)
}

fn evenly_spaced(start: i64, end: i64, count: usize) -> Vec<i64> {
    match count {
        0 => Vec::new(),
        1 => vec![start],
        _ => (0..count)
            .map(|index| start + index as i64 * (end - start) / (count as i64 - 1))
            .collect(),
    }
}

fn format_timestamp(ms: i64) -> String {
    if ms % 1_000 == 0 {
        (ms / 1_000).to_string()
    } else {
        format!("{}.{:03}", ms / 1_000, ms.unsigned_abs() % 1_000)
    }
}

fn calendar_value(name: &str, evaluation_ms: i64) -> Result<i64> {
    let at = Utc
        .timestamp_millis_opt(evaluation_ms)
        .single()
        .context("evaluation time")?;
    Ok(match name {
        "minute" => at.minute() as i64,
        "hour" => at.hour() as i64,
        "day_of_week" => at.weekday().num_days_from_sunday() as i64,
        "day_of_month" => at.day() as i64,
        "day_of_year" => at.ordinal() as i64,
        "days_in_month" => {
            let (year, month) = if at.month() == 12 {
                (at.year() + 1, 1)
            } else {
                (at.year(), at.month() + 1)
            };
            Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
                .single()
                .context("next month")?
                .signed_duration_since(
                    Utc.with_ymd_and_hms(at.year(), at.month(), 1, 0, 0, 0)
                        .single()
                        .context("month")?,
                )
                .num_days()
        }
        "month" => at.month() as i64,
        "year" => at.year() as i64,
        _ => bail!("unknown calendar field {name}"),
    })
}

fn operator_expected(
    case: &Map<String, Value>,
    sample_ms: i64,
    evaluation_ms: i64,
) -> Result<OperatorExpectation> {
    let mut expression = string(case, "query")?;
    if case.contains_key("at_offset_ms") {
        expression = expression.replace(
            "{at}",
            &format_timestamp(sample_ms + offset(case, "at_offset_ms")?),
        );
    }
    let mut expected_values = case
        .get("expected_values")
        .and_then(Value::as_array)
        .cloned();
    if let Some(offsets) = case
        .get("expected_value_offsets_ms")
        .and_then(Value::as_array)
    {
        expected_values = Some(
            offsets
                .iter()
                .map(|value| timestamp(sample_ms + value.as_i64().unwrap()))
                .collect(),
        );
    }
    let mut expected_scalar = case.get("expected_scalar").cloned();
    if case.contains_key("expected_scalar_offset_ms") {
        expected_scalar = Some(timestamp(
            sample_ms + offset(case, "expected_scalar_offset_ms")?,
        ));
    }
    if let Some(name) = case
        .get("expected_calendar_at_evaluation")
        .and_then(Value::as_str)
    {
        expected_values = Some(vec![json!(calendar_value(name, evaluation_ms)?)]);
    }

    if let Some(range) = case.get("range").and_then(Value::as_object) {
        let start_ms = sample_ms + offset(range, "start_offset_ms")?;
        let end_ms = sample_ms + offset(range, "end_offset_ms")?;
        let mut params = Map::from_iter([
            ("query".to_owned(), json!(expression)),
            ("start".to_owned(), timestamp(start_ms)),
            ("end".to_owned(), timestamp(end_ms)),
            ("step".to_owned(), json!(string(range, "step")?)),
        ]);
        if case.contains_key("lookback_delta") {
            params.insert(
                "lookback_delta".to_owned(),
                json!(string(case, "lookback_delta")?),
            );
        }
        if case.contains_key("max_lookback") {
            params.insert(
                "max_lookback".to_owned(),
                json!(string(case, "max_lookback")?),
            );
        }
        let mut result = if let Some(expected_results) =
            case.get("expected_results").and_then(Value::as_array)
        {
            let mut result = Vec::new();
            for expected in expected_results {
                let expected = expected.as_object().context("expected result")?;
                let result_values = values(expected, "values")?;
                let timestamps =
                    if let Some(offsets) = expected.get("offsets_ms").and_then(Value::as_array) {
                        offsets
                            .iter()
                            .map(|value| sample_ms + value.as_i64().unwrap())
                            .collect()
                    } else {
                        evenly_spaced(start_ms, end_ms, result_values.len())
                    };
                let points: Result<Vec<_>> = timestamps
                    .iter()
                    .zip(&result_values)
                    .map(|(at, value)| Ok(json!([timestamp(*at), value_string(value)?])))
                    .collect();
                result.push(json!({"metric": expected.get("metric").cloned().unwrap_or_else(|| json!({})), "values": points?}));
            }
            result
        } else {
            let result_values = expected_values.context("expected_values")?;
            let timestamps =
                if let Some(offsets) = case.get("expected_offsets_ms").and_then(Value::as_array) {
                    offsets
                        .iter()
                        .map(|value| sample_ms + value.as_i64().unwrap())
                        .collect()
                } else {
                    evenly_spaced(start_ms, end_ms, result_values.len())
                };
            let points: Result<Vec<_>> = timestamps
                .iter()
                .zip(&result_values)
                .map(|(at, value)| Ok(json!([timestamp(*at), value_string(value)?])))
                .collect();
            vec![json!({
                "metric": case.get("expected_metric").cloned().unwrap_or_else(|| json!({"job": "oracle"})),
                "values": points?
            })]
        };
        if !result.is_empty()
            && !case.contains_key("expected_results")
            && !case.contains_key("expected_metric")
            && !case
                .get("drop_metric_name")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        {
            result[0]["metric"]["__name__"] = json!("oracle_temporal");
        }
        return Ok((
            "/api/v1/query_range".to_owned(),
            params,
            "matrix".to_owned(),
            result,
        ));
    }

    let mut params = Map::from_iter([
        ("query".to_owned(), json!(expression)),
        ("time".to_owned(), timestamp(evaluation_ms)),
    ]);
    if case.contains_key("step") {
        params.insert("step".to_owned(), json!(string(case, "step")?));
    }
    if case.contains_key("lookback_delta") {
        params.insert(
            "lookback_delta".to_owned(),
            json!(string(case, "lookback_delta")?),
        );
    }
    if case.contains_key("max_lookback") {
        params.insert(
            "max_lookback".to_owned(),
            json!(string(case, "max_lookback")?),
        );
    }
    if case
        .get("expected_empty")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok((
            "/api/v1/query".to_owned(),
            params,
            "vector".to_owned(),
            Vec::new(),
        ));
    }
    if let Some(value) = expected_scalar {
        return Ok((
            "/api/v1/query".to_owned(),
            params,
            "scalar".to_owned(),
            vec![timestamp(evaluation_ms), json!(value_string(&value)?)],
        ));
    }
    let mut result = if let Some(expected_results) =
        case.get("expected_results").and_then(Value::as_array)
    {
        let mut result = Vec::new();
        for expected in expected_results {
            let expected = expected.as_object().context("expected result")?;
            result.push(json!({
                "metric": expected.get("metric").cloned().unwrap_or_else(|| json!({})),
                "value": [timestamp(evaluation_ms), value_string(expected.get("value").context("expected value")?)?]
            }));
        }
        result
    } else {
        let expected_values = expected_values.context("expected_values")?;
        vec![json!({
            "metric": case.get("expected_metric").cloned().unwrap_or_else(|| json!({"job": "oracle"})),
            "value": [timestamp(evaluation_ms), value_string(&expected_values[0])?]
        })]
    };
    if !result.is_empty()
        && !case.contains_key("expected_results")
        && !case.contains_key("expected_metric")
        && !case
            .get("drop_metric_name")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        result[0]["metric"]["__name__"] = json!("oracle_temporal");
    }
    Ok((
        "/api/v1/query".to_owned(),
        params,
        "vector".to_owned(),
        result,
    ))
}

fn operator_cases(client: &Client, base: &str, fixture: &Value, sample_ms: i64) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "operator_cases")? {
        let evaluation_ms = sample_ms + offset(case, "evaluation_offset_ms")?;
        let (endpoint, params, result_type, result) =
            operator_expected(case, sample_ms, evaluation_ms)?;
        let (status, body) = query(client, base, &endpoint, &params)?;
        let actual = body.pointer("/data/result").unwrap_or(&Value::Null);
        let result_matches = if result_type == "scalar" {
            actual == &json!(result)
        } else {
            results_equal(
                &result,
                actual,
                case.get("result_order").and_then(Value::as_str) == Some("ordered"),
            )
        };
        let evaluation = Utc
            .timestamp_millis_opt(evaluation_ms)
            .single()
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        let mut expected_infos = sorted_strings(case.get("expected_infos"))?;
        for value in &mut expected_infos {
            *value = value.replace("{evaluation_rfc3339}", &evaluation);
        }
        expected_infos.sort();
        let actual_infos = sorted_strings(body.get("infos"))?;
        let warnings_match = warning_contract_matches(case, body.get("warnings"))?;
        let valid = status == 200
            && body.get("status") == Some(&json!("success"))
            && body.pointer("/data/resultType") == Some(&json!(result_type))
            && result_matches
            && warnings_match
            && actual_infos == expected_infos;
        failures += print_verdict(case, valid, || format!("expected {result:?}; got {body}"));
    }
    Ok(failures)
}

fn operator_error_cases(
    client: &Client,
    base: &str,
    fixture: &Value,
    sample_ms: i64,
) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "operator_error_cases")? {
        let evaluation_ms = sample_ms + offset(case, "evaluation_offset_ms")?;
        let params = Map::from_iter([
            ("query".to_owned(), json!(string(case, "query")?)),
            ("time".to_owned(), timestamp(evaluation_ms)),
        ]);
        let (status, body) = query(client, base, "/api/v1/query", &params)?;
        let expected_status = case
            .get("status")
            .and_then(Value::as_u64)
            .context("status")? as u16;
        let error_contains = string(case, "error_contains")?;
        let valid = status == expected_status
            && body.get("status") == Some(&json!("error"))
            && body.get("errorType") == case.get("error_type")
            && body
                .get("error")
                .and_then(Value::as_str)
                .is_some_and(|error| error.contains(&error_contains));
        failures += print_verdict(case, valid, || {
            format!("unexpected response {status} {body}")
        });
    }
    Ok(failures)
}

fn prometheus_api(root: &Path, runtime: &str, oracle: &OracleDefinition) -> Result<()> {
    let relative = oracle
        .fixtures
        .iter()
        .find(|path| path.ends_with("api_cases.json"))
        .context("Prometheus API fixture is not declared")?;
    let fixture: Value = load_json(&root.join(relative))?;
    let port = free_port()?;
    let name = format!("timeless-promql-oracle-{}", std::process::id());
    let args = vec![
        "run".to_owned(),
        "--rm".to_owned(),
        "-d".to_owned(),
        "--name".to_owned(),
        name.clone(),
        "--platform".to_owned(),
        "linux/amd64".to_owned(),
        "-p".to_owned(),
        format!("127.0.0.1:{port}:9090"),
        oracle.image.clone(),
        "--config.file=/etc/prometheus/prometheus.yml".to_owned(),
        "--storage.tsdb.path=/prometheus".to_owned(),
        "--web.listen-address=0.0.0.0:9090".to_owned(),
        "--web.enable-remote-write-receiver".to_owned(),
    ];
    let output = command_output(runtime, &args, Duration::from_secs(180))?;
    if !output.status.success() {
        bail!(
            "failed to start Prometheus oracle: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let _guard = ContainerGuard { runtime, name };
    let base = format!("http://127.0.0.1:{port}");
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if client
            .get(format!("{base}/-/ready"))
            .send()
            .is_ok_and(|response| response.status().is_success())
        {
            break;
        }
        if Instant::now() >= deadline {
            bail!("Prometheus API oracle did not become ready");
        }
        thread::sleep(Duration::from_millis(100));
    }
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    let sample_ms = (now_ms / 60_000 - 1) * 60_000;
    post_remote_write(&client, &base, sample_ms)?;

    let failures = exact_cases(&client, &base, &fixture)?
        + lookback_cases(&client, &base, &fixture, sample_ms)?
        + temporal_cases(&client, &base, &fixture, sample_ms)?
        + subquery_cases(&client, &base, &fixture, sample_ms)?
        + operator_cases(&client, &base, &fixture, sample_ms)?
        + operator_error_cases(&client, &base, &fixture, sample_ms)?;
    if failures != 0 {
        bail!("{failures} Prometheus API oracle case(s) failed");
    }
    Ok(())
}

fn victoriametrics_cases(
    client: &Client,
    base: &str,
    fixture: &Value,
    sample_ms: i64,
) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "cases")? {
        let range = case
            .get("range")
            .and_then(Value::as_object)
            .context("VictoriaMetrics case range")?;
        let start_ms = sample_ms + offset(range, "start_offset_ms")?;
        let end_ms = sample_ms + offset(range, "end_offset_ms")?;
        let expected_values = values(case, "expected_values")?;
        let timestamps = evenly_spaced(start_ms, end_ms, expected_values.len());
        let points: Result<Vec<_>> = timestamps
            .iter()
            .zip(&expected_values)
            .map(|(at, value)| Ok(json!([timestamp(*at), value_string(value)?])))
            .collect();
        let metric = case
            .get("expected_metric")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let expected = json!({
            "resultType": "matrix",
            "result": [{"metric": metric, "values": points?}]
        });
        let params = Map::from_iter([
            ("query".to_owned(), json!(string(case, "query")?)),
            ("start".to_owned(), timestamp(start_ms)),
            ("end".to_owned(), timestamp(end_ms)),
            ("step".to_owned(), json!(string(range, "step")?)),
        ]);
        let (status, body) = query(client, base, "/api/v1/query_range", &params)?;
        let valid = status == 200
            && body.get("status") == Some(&json!("success"))
            && body.get("data") == Some(&expected);
        failures += print_verdict(case, valid, || {
            format!("expected {expected}; got {status} {body}")
        });
    }
    Ok(failures)
}

fn victoriametrics_error_cases(client: &Client, base: &str, fixture: &Value) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "error_cases")? {
        let params = case
            .get("params")
            .and_then(Value::as_object)
            .context("VictoriaMetrics error case params")?;
        let (status, body) = query(client, base, "/api/v1/query_range", params)?;
        let expected_status = case
            .get("status")
            .and_then(Value::as_u64)
            .context("VictoriaMetrics error case status")? as u16;
        let error_contains = string(case, "error_contains")?;
        let valid = status == expected_status
            && body.get("status") == Some(&json!("error"))
            && body.get("errorType") == case.get("error_type")
            && body
                .get("error")
                .and_then(Value::as_str)
                .is_some_and(|error| error.contains(&error_contains));
        failures += print_verdict(case, valid, || {
            format!("unexpected response {status} {body}")
        });
    }
    Ok(failures)
}

fn victoriametrics_api(root: &Path, runtime: &str, oracle: &OracleDefinition) -> Result<()> {
    let relative = oracle
        .fixtures
        .iter()
        .find(|path| path.ends_with("victoriametrics/api_cases.json"))
        .context("VictoriaMetrics API fixture is not declared")?;
    let fixture: Value = load_json(&root.join(relative))?;
    let port = free_port()?;
    let name = format!("timeless-metricsql-oracle-{}", std::process::id());
    let args = vec![
        "run".to_owned(),
        "--rm".to_owned(),
        "-d".to_owned(),
        "--name".to_owned(),
        name.clone(),
        "--platform".to_owned(),
        "linux/amd64".to_owned(),
        "-p".to_owned(),
        format!("127.0.0.1:{port}:8428"),
        oracle.image.clone(),
        "-storageDataPath=/victoria-metrics-data".to_owned(),
        "-httpListenAddr=:8428".to_owned(),
        "-retentionPeriod=1d".to_owned(),
    ];
    let output = command_output(runtime, &args, Duration::from_secs(180))?;
    if !output.status.success() {
        bail!(
            "failed to start VictoriaMetrics oracle: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let _guard = ContainerGuard { runtime, name };
    let base = format!("http://127.0.0.1:{port}");
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if client
            .get(format!("{base}/health"))
            .send()
            .is_ok_and(|response| response.status().is_success())
        {
            break;
        }
        if Instant::now() >= deadline {
            bail!("VictoriaMetrics API oracle did not become ready");
        }
        thread::sleep(Duration::from_millis(100));
    }

    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    let sample_ms = (now_ms / 60_000 - 1) * 60_000;
    post_remote_write(&client, &base, sample_ms)?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let params = Map::from_iter([
            ("query".to_owned(), json!("oracle_step")),
            ("time".to_owned(), timestamp(sample_ms)),
            ("step".to_owned(), json!("1s")),
        ]);
        let (status, body) = query(&client, &base, "/api/v1/query", &params)?;
        if status == 200
            && body
                .pointer("/data/result")
                .and_then(Value::as_array)
                .is_some_and(|result| result.len() == 1)
        {
            break;
        }
        if Instant::now() >= deadline {
            bail!("VictoriaMetrics oracle did not make the Remote Write fixture query-visible");
        }
        thread::sleep(Duration::from_millis(100));
    }

    let failures = victoriametrics_cases(&client, &base, &fixture, sample_ms)?
        + victoriametrics_error_cases(&client, &base, &fixture)?
        + operator_cases(&client, &base, &fixture, sample_ms)?
        + operator_error_cases(&client, &base, &fixture, sample_ms)?;
    if failures != 0 {
        bail!("{failures} VictoriaMetrics API oracle case(s) failed");
    }
    Ok(())
}

fn victorialogs_request(client: &Client, base: &str, query: &str) -> Result<(u16, String, String)> {
    response_text(
        client
            .get(format!("{base}/select/logsql/query"))
            .query(&[("query", query), ("limit", "1000")])
            .send()?,
    )
}

fn victorialogs_query(case: &Map<String, Value>, base_us: i64) -> Result<String> {
    let mut query = string(case, "query")?;
    let Some(times) = case.get("query_times") else {
        return Ok(query);
    };
    for (name, value) in times.as_object().context("query_times must be an object")? {
        let spec = value
            .as_object()
            .with_context(|| format!("query_times.{name} must be an object"))?;
        let at_us = base_us
            .checked_add(offset(spec, "offset_us")?)
            .with_context(|| format!("query_times.{name} timestamp overflow"))?;
        let format = string(spec, "format")?;
        let replacement = match format.as_str() {
            "rfc3339" => Utc
                .timestamp_micros(at_us)
                .single()
                .with_context(|| format!("query_times.{name} is outside RFC3339 range"))?
                .to_rfc3339_opts(SecondsFormat::Micros, true),
            "unix_s" => {
                if at_us % 1_000_000 != 0 {
                    bail!("query_times.{name} is not an exact Unix second");
                }
                (at_us / 1_000_000).to_string()
            }
            "unix_ms" => {
                if at_us % 1_000 != 0 {
                    bail!("query_times.{name} is not an exact Unix millisecond");
                }
                (at_us / 1_000).to_string()
            }
            "unix_us" => at_us.to_string(),
            "unix_ns" => at_us
                .checked_mul(1_000)
                .with_context(|| format!("query_times.{name} nanosecond overflow"))?
                .to_string(),
            _ => bail!("unknown query_times.{name} format {format:?}"),
        };
        let marker = format!("{{{name}}}");
        if !query.contains(&marker) {
            bail!("query does not contain query_times marker {marker}");
        }
        query = query.replace(&marker, &replacement);
    }
    Ok(query)
}

fn victorialogs_response_cases(
    case: &Map<String, Value>,
    status: u16,
    expected_status: u16,
    content_type: &str,
    body: &str,
) -> Result<Vec<String>> {
    if status != expected_status || !content_type.starts_with("application/stream+json") {
        return Ok(Vec::new());
    }
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| -> Result<String> {
            let row: Value = serde_json::from_str(line)
                .with_context(|| format!("decode VictoriaLogs row for {}", case_id(case)))?;
            row.get("case")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("VictoriaLogs oracle row is missing case")
        })
        .collect()
}

fn victorialogs_cases(client: &Client, base: &str, fixture: &Value, base_us: i64) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "query_cases")? {
        let query = victorialogs_query(case, base_us)?;
        let (status, content_type, body) = victorialogs_request(client, base, &query)?;
        let expected_status = case
            .get("status")
            .and_then(Value::as_u64)
            .context("case status")? as u16;
        let mut actual_cases =
            victorialogs_response_cases(case, status, expected_status, &content_type, &body)?;
        let mut expected_cases = case
            .get("expected_cases")
            .and_then(Value::as_array)
            .context("case expected_cases")?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .context("expected_cases entry must be a string")
            })
            .collect::<Result<Vec<_>>>()?;
        if case.get("result_order").and_then(Value::as_str) != Some("ordered") {
            actual_cases.sort();
            expected_cases.sort();
        }
        let valid = status == expected_status
            && content_type.starts_with("application/stream+json")
            && actual_cases == expected_cases;
        failures += print_verdict(case, valid, || {
            format!(
                "expected {expected_status} {expected_cases:?}; got {status} {content_type:?} {actual_cases:?}: {body:?}"
            )
        });
    }
    Ok(failures)
}

fn victorialogs_error_cases(client: &Client, base: &str, fixture: &Value) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "error_cases")? {
        let query = string(case, "query")?;
        let (status, content_type, body) = victorialogs_request(client, base, &query)?;
        let expected_status = case
            .get("status")
            .and_then(Value::as_u64)
            .context("case status")? as u16;
        let body_contains = string(case, "body_contains")?;
        let expected_content_type = string(case, "content_type")?;
        let valid = status == expected_status
            && content_type == expected_content_type
            && body.contains(&body_contains);
        failures += print_verdict(case, valid, || {
            format!(
                "expected {expected_status} {expected_content_type:?} containing {body_contains:?}; got {status} {content_type:?} {body:?}"
            )
        });
    }
    Ok(failures)
}

fn victorialogs_stats_cases(client: &Client, base: &str, fixture: &Value) -> Result<usize> {
    let mut failures = 0;
    for case in object_cases(fixture, "stats_cases")? {
        let query = string(case, "query")?;
        let (status, content_type, body) = victorialogs_request(client, base, &query)?;
        let actual = body
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<Value>(line).context("decode VictoriaLogs stats row")
            })
            .collect::<Result<Vec<_>>>()?;
        let expected = case
            .get("expected_rows")
            .and_then(Value::as_array)
            .context("stats case expected_rows")?;
        let valid = status == 200
            && content_type.starts_with("application/stream+json")
            && actual == *expected;
        failures += print_verdict(case, valid, || {
            format!("expected {expected:?}; got {status} {content_type:?} {actual:?}: {body:?}")
        });
    }
    Ok(failures)
}

fn victorialogs_api(root: &Path, runtime: &str, oracle: &OracleDefinition) -> Result<()> {
    let relative = oracle
        .fixtures
        .iter()
        .find(|path| path.ends_with("victorialogs/api_cases.json"))
        .context("VictoriaLogs API fixture is not declared")?;
    let fixture: Value = load_json(&root.join(relative))?;
    let port = free_port()?;
    let name = format!("timeless-logsql-oracle-{}", std::process::id());
    let args = vec![
        "run".to_owned(),
        "--rm".to_owned(),
        "-d".to_owned(),
        "--name".to_owned(),
        name.clone(),
        "--platform".to_owned(),
        "linux/amd64".to_owned(),
        "-p".to_owned(),
        format!("127.0.0.1:{port}:9428"),
        oracle.image.clone(),
        "-retentionPeriod=1d".to_owned(),
    ];
    let output = command_output(runtime, &args, Duration::from_secs(180))?;
    if !output.status.success() {
        bail!(
            "failed to start VictoriaLogs oracle: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let _guard = ContainerGuard { runtime, name };
    let base = format!("http://127.0.0.1:{port}");
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if client
            .get(format!("{base}/health"))
            .send()
            .is_ok_and(|response| response.status().is_success())
        {
            break;
        }
        if Instant::now() >= deadline {
            bail!("VictoriaLogs API oracle did not become ready");
        }
        thread::sleep(Duration::from_millis(100));
    }

    let rows = fixture
        .get("rows")
        .and_then(Value::as_array)
        .context("VictoriaLogs fixture is missing rows")?;
    let mut ndjson = String::new();
    let now_us = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as i64;
    let base_us = (now_us / 1_000_000)
        .saturating_mul(1_000_000)
        .saturating_sub(10_000_000);
    for (position, row) in rows.iter().enumerate() {
        let mut row = row
            .as_object()
            .cloned()
            .context("VictoriaLogs fixture row must be an object")?;
        let offset_us = row
            .remove("time_offset_us")
            .map(|value| value.as_i64().context("time_offset_us must be an integer"))
            .transpose()?
            .unwrap_or(position as i64);
        row.insert(
            "oracle_time".to_owned(),
            json!(base_us.saturating_add(offset_us)),
        );
        ndjson.push_str(&serde_json::to_string(&row)?);
        ndjson.push('\n');
    }
    let response = client
        .post(format!("{base}/insert/jsonline"))
        .query(&[
            ("_stream_fields", "case"),
            ("_time_field", "oracle_time"),
            ("_msg_field", "_msg"),
        ])
        .header("content-type", "application/stream+json")
        .body(ndjson)
        .send()?;
    if !response.status().is_success() {
        bail!("VictoriaLogs ingestion returned {}", response.status());
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, _, body) = victorialogs_request(&client, &base, "*")?;
        let count = body.lines().filter(|line| !line.trim().is_empty()).count();
        if status == 200 && count == rows.len() {
            break;
        }
        if Instant::now() >= deadline {
            bail!(
                "VictoriaLogs ingested {count} of {} oracle rows before the deadline",
                rows.len()
            );
        }
        thread::sleep(Duration::from_millis(100));
    }

    let failures = victorialogs_cases(&client, &base, &fixture, base_us)?
        + victorialogs_stats_cases(&client, &base, &fixture)?
        + victorialogs_error_cases(&client, &base, &fixture)?;
    if failures != 0 {
        bail!("{failures} VictoriaLogs API oracle case(s) failed");
    }
    Ok(())
}

pub(crate) fn run(root: &Path, args: OracleArgs) -> Result<()> {
    let manifest_path = root.join(&args.manifest);
    let manifest: OracleManifest = load_json(&manifest_path)?;
    let errors = validate_manifest(root, &manifest)?;
    if !errors.is_empty() {
        for error in errors {
            eprintln!("query-oracle: {error}");
        }
        bail!("query oracle manifest validation failed");
    }
    match args.command {
        OracleCommand::Validate => println!("query oracle manifest: ok"),
        OracleCommand::Probe => probe(&args.runtime, &manifest)?,
        OracleCommand::PrometheusSmoke => {
            prometheus_smoke(root, &args.runtime, &manifest.oracles["prometheus"])?
        }
        OracleCommand::PrometheusApi => {
            prometheus_api(root, &args.runtime, &manifest.oracles["prometheus"])?
        }
        OracleCommand::VictoriaMetricsApi => {
            victoriametrics_api(root, &args.runtime, &manifest.oracles["victoriametrics"])?
        }
        OracleCommand::VictoriaLogsApi => {
            victorialogs_api(root, &args.runtime, &manifest.oracles["victorialogs"])?
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn repository_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf()
    }

    #[test]
    fn checked_manifest_matches_docs_and_fixtures() {
        let root = repository_root();
        let manifest: OracleManifest =
            load_json(&root.join("tests/query_oracles/manifest.json")).unwrap();
        assert_eq!(
            validate_manifest(&root, &manifest).unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn floating_images_and_short_commits_are_rejected() {
        let root = repository_root();
        let mut manifest: OracleManifest =
            load_json(&root.join("tests/query_oracles/manifest.json")).unwrap();
        manifest.oracles.get_mut("prometheus").unwrap().image =
            "docker.io/prom/prometheus:latest".to_owned();
        manifest
            .oracles
            .get_mut("victorialogs")
            .unwrap()
            .source_commit = "deadbeef".to_owned();
        let errors = validate_manifest(&root, &manifest).unwrap();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("floating image reference")),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|error| error.contains("source_commit")),
            "{errors:?}"
        );
    }

    #[test]
    fn manifest_serialization_is_stable_json() {
        let root = repository_root();
        let manifest: OracleManifest =
            load_json(&root.join("tests/query_oracles/manifest.json")).unwrap();
        let encoded = serde_json::to_string_pretty(&manifest).unwrap();
        assert!(encoded.contains("\"schema_version\": 1"));
    }

    #[test]
    fn fixture_ids_are_unique_and_row_addressed() {
        let root = repository_root();
        let fixture: Value =
            load_json(&root.join("tests/query_oracles/prometheus/api_cases.json")).unwrap();
        let mut identifiers = BTreeSet::new();
        for name in [
            "cases",
            "lookback_cases",
            "temporal_cases",
            "subquery_cases",
            "operator_cases",
            "operator_error_cases",
        ] {
            for case in object_cases(&fixture, name).unwrap() {
                let identifier = case_id(case);
                assert!(identifier.starts_with("PQL-"), "{identifier}");
                assert!(
                    identifiers.insert(identifier.to_owned()),
                    "duplicate {identifier}"
                );
                if let Some(order) = case.get("result_order").and_then(Value::as_str) {
                    assert!(matches!(order, "ordered" | "unordered"));
                }
            }
        }
    }

    #[test]
    fn warning_contract_pins_cap_without_assuming_go_map_iteration_order() {
        let case = json!({
            "expected_warning_contract": {
                "count": 3,
                "required": ["1 more warning annotations omitted"],
                "allowed": ["a", "b", "c", "1 more warning annotations omitted"]
            }
        });
        let case = case.as_object().unwrap();
        assert!(warning_contract_matches(
            case,
            Some(&json!(["c", "a", "1 more warning annotations omitted"]))
        )
        .unwrap());
        assert!(!warning_contract_matches(
            case,
            Some(&json!(["a", "a", "1 more warning annotations omitted"]))
        )
        .unwrap());
        assert!(!warning_contract_matches(
            case,
            Some(&json!([
                "a",
                "unknown",
                "1 more warning annotations omitted"
            ]))
        )
        .unwrap());
    }

    #[test]
    fn victorialogs_fixture_ids_and_stream_cases_are_unique() {
        let root = repository_root();
        let fixture: Value =
            load_json(&root.join("tests/query_oracles/victorialogs/api_cases.json")).unwrap();
        assert_eq!(fixture.get("schema_version"), Some(&json!(1)));

        let mut identifiers = BTreeSet::new();
        for name in ["query_cases", "stats_cases", "error_cases"] {
            for case in object_cases(&fixture, name).unwrap() {
                let identifier = case_id(case);
                assert!(identifier.starts_with("LQL-"), "{identifier}");
                assert!(
                    identifiers.insert(identifier.to_owned()),
                    "duplicate {identifier}"
                );
                if let Some(order) = case.get("result_order").and_then(Value::as_str) {
                    assert!(matches!(order, "ordered" | "unordered"));
                }
            }
        }

        let mut stream_cases = BTreeSet::new();
        for row in fixture.get("rows").and_then(Value::as_array).unwrap() {
            let case = row.get("case").and_then(Value::as_str).unwrap();
            assert!(stream_cases.insert(case.to_owned()), "duplicate {case}");
        }
    }

    #[test]
    fn victorialogs_documented_case_count_matches_the_checked_fixture() {
        let root = repository_root();
        let fixture: Value =
            load_json(&root.join("tests/query_oracles/victorialogs/api_cases.json")).unwrap();
        let count = ["query_cases", "stats_cases", "error_cases"]
            .into_iter()
            .map(|name| fixture.get(name).and_then(Value::as_array).unwrap().len())
            .sum::<usize>();
        let documentation =
            fs::read_to_string(root.join("docs/QUERY_ORACLES.md")).expect("read oracle docs");
        assert!(
            documentation.contains(&format!("fixture now contains {count} cases")),
            "docs/QUERY_ORACLES.md must name the exact {count}-case VictoriaLogs fixture"
        );
    }

    #[test]
    fn victorialogs_error_response_reports_a_case_failure_instead_of_aborting_the_corpus() {
        let case = json!({"id": "LQL-TEST", "status": 200});
        let case = case.as_object().unwrap();
        assert!(victorialogs_response_cases(
            case,
            400,
            200,
            "text/plain; charset=utf-8",
            "cannot parse query",
        )
        .unwrap()
        .is_empty());
        assert_eq!(
            victorialogs_response_cases(
                case,
                200,
                200,
                "application/stream+json",
                "{\"case\":\"row\"}\n",
            )
            .unwrap(),
            ["row"]
        );
    }

    #[test]
    fn unordered_vector_comparison_does_not_invent_order() {
        let expected = vec![
            json!({"metric": {"value": "first"}, "value": [1, "1"]}),
            json!({"metric": {"value": "second"}, "value": [1, "2"]}),
        ];
        let mut reordered = expected.clone();
        reordered.reverse();
        assert!(results_equal(&expected, &json!(reordered), false));
        assert!(!results_equal(&expected, &json!(reordered), true));
        reordered[0]["value"][1] = json!("3");
        assert!(!results_equal(&expected, &json!(reordered), false));
    }

    #[test]
    fn raw_snappy_remote_write_is_deterministic() {
        let first = fixture::prometheus_remote_write(1_700_000_000_000);
        let second = fixture::prometheus_remote_write(1_700_000_000_000);
        assert_eq!(first, second);
        assert!(first.len() > 1_000);
    }
}
