use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use chrono::{SecondsFormat, Utc};
use clap::Args;
use reqwest::blocking::Client;
use reqwest::header::CONTENT_TYPE;
use rusqlite::Connection;
use serde_json::{json, Map, Number, Value};
use tempfile::TempDir;
use wait_timeout::ChildExt;

#[derive(Args, Debug)]
pub(crate) struct EvidenceArgs {
    #[arg(long, default_value = "target/release/libtimeless_ext.so")]
    extension: PathBuf,
    #[arg(long, default_value = "servers/target/release/timeless-metrics-api")]
    metrics_binary: PathBuf,
    #[arg(long, default_value = "servers/target/release/timeless-logs-api")]
    logs_binary: PathBuf,
    #[arg(long, default_value_t = 50)]
    iterations: usize,
    #[arg(long, default_value_t = 5)]
    warmup: usize,
    #[arg(long, default_value_t = 512)]
    metric_series: usize,
    #[arg(long, default_value_t = 32)]
    metric_points: usize,
    #[arg(long, default_value_t = 8192)]
    log_entries: usize,
    #[arg(long, required = true)]
    output: PathBuf,
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind(("127.0.0.1", 0))?
        .local_addr()?
        .port())
}

fn git_commit(root: &Path) -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()?;
    if !output.status.success() {
        bail!("git rev-parse HEAD failed");
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn require_clean_worktree(root: &Path) -> Result<()> {
    let output = Command::new("git")
        // Evidence identifies the committed source used to build the binaries.
        // Untracked operator notes and the not-yet-created evidence output do
        // not change that source identity, while staged or unstaged changes to
        // tracked files do.
        .args(["status", "--porcelain", "--untracked-files=no"])
        .current_dir(root)
        .output()?;
    if !output.status.success() {
        bail!("git status failed while validating evidence source");
    }
    let status = String::from_utf8(output.stdout)?;
    if !status.trim().is_empty() {
        bail!(
            "evidence requires clean tracked files so artifact identity describes the exact source; pending paths:\n{status}"
        );
    }
    Ok(())
}

fn preserve_failed_evidence<T>(temporary: TempDir, result: Result<T>) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            let preserved = temporary.keep();
            Err(error.context(format!(
                "failed evidence database and server logs preserved at {}",
                preserved.display()
            )))
        }
    }
}

fn binary_identity(binary: &Path) -> Result<Value> {
    let output = Command::new(binary)
        .arg("--version")
        .output()
        .with_context(|| format!("run {} --version", binary.display()))?;
    if !output.status.success() {
        bail!("{} --version exited {}", binary.display(), output.status);
    }
    serde_json::from_slice(&output.stdout).context("decode binary build identity")
}

fn validate_build_identity(
    identity: &Value,
    expected_commit: &str,
    artifact: &str,
) -> Result<Value> {
    let commit = identity.get("commit").and_then(Value::as_str);
    if commit != Some(expected_commit) {
        bail!(
            "{artifact} build commit {commit:?} does not match evidence source {expected_commit:?}"
        );
    }
    Ok(identity.clone())
}

fn require_binary_identity(binary: &Path, expected_commit: &str) -> Result<Value> {
    validate_build_identity(
        &binary_identity(binary)?,
        expected_commit,
        binary
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("binary"),
    )
}

fn extension_identity(extension: &Path, expected_commit: &str) -> Result<Value> {
    let connection = Connection::open_in_memory()?;
    unsafe {
        connection.load_extension_enable()?;
        connection.load_extension(extension, None::<&str>)?;
        connection.load_extension_disable()?;
    }
    let encoded: String =
        connection.query_row("SELECT timeless_capabilities()", [], |row| row.get(0))?;
    let capabilities: Value = serde_json::from_str(&encoded)?;
    validate_build_identity(
        capabilities.get("build").unwrap_or(&Value::Null),
        expected_commit,
        "extension",
    )
}

struct Server {
    binary_name: String,
    base: String,
    child: Child,
    log_path: PathBuf,
    closed: bool,
}

impl Server {
    fn start(
        binary: &Path,
        extension: &Path,
        database: &Path,
        environment: &[(&str, &str)],
        client: &Client,
    ) -> Result<Self> {
        let port = free_port()?;
        let base = format!("http://127.0.0.1:{port}");
        let log_path = database.with_extension("server.log");
        let stdout = File::create(&log_path)?;
        let stderr = stdout.try_clone()?;
        let mut command = Command::new(binary);
        command
            .args([
                extension.as_os_str(),
                database.as_os_str(),
                format!("127.0.0.1:{port}").as_ref(),
            ])
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        for (name, value) in environment {
            command.env(name, value);
        }
        let child = command
            .spawn()
            .with_context(|| format!("start {}", binary.display()))?;
        let mut server = Self {
            binary_name: binary
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("server")
                .to_owned(),
            base,
            child,
            log_path,
            closed: false,
        };
        if let Err(error) = server.wait_live(client) {
            let _ = server.shutdown(false);
            return Err(error);
        }
        Ok(server)
    }

    fn wait_live(&mut self, client: &Client) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait()? {
                bail!("{} exited during startup with {status}", self.binary_name);
            }
            if client
                .get(format!("{}/live", self.base))
                .timeout(Duration::from_millis(500))
                .send()
                .is_ok_and(|response| response.status().as_u16() == 200)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("{} did not become live at {}", self.binary_name, self.base);
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn shutdown(&mut self, require_clean: bool) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        if self.child.try_wait()?.is_none() {
            unsafe {
                libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
            }
            if self.child.wait_timeout(Duration::from_secs(30))?.is_none() {
                self.child.kill()?;
                let _ = self.child.wait_timeout(Duration::from_secs(5))?;
            }
        }
        let status = self.child.wait()?;
        if require_clean && !status.success() {
            let output = fs::read_to_string(&self.log_path).unwrap_or_default();
            bail!("{} shutdown={status}:\n{output}", self.binary_name);
        }
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.shutdown(false);
    }
}

fn request_bytes(
    client: &Client,
    method: reqwest::Method,
    url: &str,
    body: Option<(&[u8], &str)>,
) -> Result<(u16, Vec<u8>)> {
    let mut request = client.request(method, url);
    if let Some((body, content_type)) = body {
        request = request
            .header(CONTENT_TYPE, content_type)
            .body(body.to_vec());
    }
    let response = request.send()?;
    let status = response.status().as_u16();
    Ok((status, response.bytes()?.to_vec()))
}

fn stats(client: &Client, base: &str, path: &str) -> Result<Value> {
    let (status, body) =
        request_bytes(client, reqwest::Method::GET, &format!("{base}{path}"), None)?;
    if status != 200 {
        bail!(
            "stats returned {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    Ok(serde_json::from_slice(&body)?)
}

fn number_delta(before: &Number, after: &Number) -> Option<Number> {
    if let (Some(before), Some(after)) = (before.as_i64(), after.as_i64()) {
        let delta = after - before;
        return (delta != 0).then(|| Number::from(delta));
    }
    if let (Some(before), Some(after)) = (before.as_u64(), after.as_u64()) {
        let delta = after as i128 - before as i128;
        return (delta != 0).then(|| Number::from(delta as i64));
    }
    let delta = after.as_f64()? - before.as_f64()?;
    (delta != 0.0).then(|| Number::from_f64(delta).expect("finite stats delta"))
}

fn numeric_delta(before: &Value, after: &Value) -> Value {
    let Some(before) = before.as_object() else {
        return json!({});
    };
    let Some(after) = after.as_object() else {
        return json!({});
    };
    let mut delta = Map::new();
    for (key, value) in after {
        if let (Some(Value::Number(prior)), Value::Number(value)) = (before.get(key), value) {
            if let Some(value) = number_delta(prior, value) {
                delta.insert(key.clone(), Value::Number(value));
            }
        }
    }
    Value::Object(delta)
}

fn require_same_public_query_work(
    queries: &Map<String, Value>,
    control_key: &str,
    sampled_key: &str,
) -> Result<()> {
    require_same_public_query_work_with_scans(queries, control_key, sampled_key, 1, 0)
}

fn require_no_public_query_work(queries: &Map<String, Value>, keys: &[&str]) -> Result<()> {
    const PUBLIC_WORK_FIELDS: &[&str] = &[
        "query_count",
        "native_count_count",
        "query_bounded_count",
        "query_bounded_requested_entries",
        "query_candidate_blocks",
        "query_decoded_entries",
        "query_payload_bytes_read",
        "query_matched_entries",
        "query_returned_entries",
        "query_materialize_ns",
        "query_snapshot_ns",
        "query_stable_location_snapshots",
        "query_total_ns",
    ];
    for key in keys {
        let evidence = queries
            .get(*key)
            .with_context(|| format!("missing scan-free evidence shape {key}"))?;
        for field in PUBLIC_WORK_FIELDS {
            let value = evidence
                .pointer(&format!("/stats_delta/{field}"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if value != 0 {
                bail!("scan-free evidence {key} unexpectedly recorded {field}={value}");
            }
        }
    }
    Ok(())
}

fn require_same_public_query_work_with_scans(
    queries: &Map<String, Value>,
    control_key: &str,
    sampled_key: &str,
    scans_per_request: u64,
    sampled_work_reservation_per_request: u64,
) -> Result<()> {
    if scans_per_request == 0 {
        bail!("public query evidence must require at least one scan per request");
    }
    let control = queries
        .get(control_key)
        .with_context(|| format!("missing evidence control {control_key}"))?;
    let sampled = queries
        .get(sampled_key)
        .with_context(|| format!("missing evidence shape {sampled_key}"))?;
    let iterations = control
        .get("iterations")
        .and_then(Value::as_u64)
        .with_context(|| format!("missing evidence iterations for {control_key}"))?;
    let sampled_iterations = sampled
        .get("iterations")
        .and_then(Value::as_u64)
        .with_context(|| format!("missing evidence iterations for {sampled_key}"))?;
    if sampled_iterations != iterations {
        bail!(
            "sample evidence {control_key}/{sampled_key} changed measured iterations: {iterations} != {sampled_iterations}"
        );
    }
    let expected_queries = iterations
        .checked_mul(scans_per_request)
        .context("public query evidence query-count overflow")?;
    let expected_work_reservation = iterations
        .checked_mul(sampled_work_reservation_per_request)
        .context("public query evidence work-reservation overflow")?;
    for (key, evidence) in [(control_key, control), (sampled_key, sampled)] {
        let query_count = evidence
            .pointer("/stats_delta/query_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let native_count = evidence
            .pointer("/stats_delta/native_count_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if query_count != expected_queries || native_count != 0 {
            bail!(
                "sample evidence {key} must use {expected_queries} public row queries ({scans_per_request} scans/request across {iterations} requests) and no native-count fast path; got query_count={query_count}, native_count_count={native_count}"
            );
        }
    }
    let control_requested_entries = control
        .pointer("/stats_delta/query_bounded_requested_entries")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let sampled_requested_entries = sampled
        .pointer("/stats_delta/query_bounded_requested_entries")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let expected_sampled_requested_entries = control_requested_entries
        .checked_sub(expected_work_reservation)
        .with_context(|| {
            format!(
                "sample evidence {sampled_key} declares more work reservation than control {control_key} requested"
            )
        })?;
    if sampled_requested_entries != expected_sampled_requested_entries {
        bail!(
            "sample evidence {control_key}/{sampled_key} changed public query_bounded_requested_entries outside the declared {sampled_work_reservation_per_request}-entry/request sampled state reservation: expected {expected_sampled_requested_entries}, got {sampled_requested_entries} (control {control_requested_entries})"
        );
    }
    for field in [
        "query_candidate_blocks",
        "query_decoded_entries",
        "query_payload_bytes_read",
        "query_matched_entries",
        "query_returned_entries",
    ] {
        let pointer = format!("/stats_delta/{field}");
        let control_value = control
            .pointer(&pointer)
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let sampled_value = sampled
            .pointer(&pointer)
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if control_value != sampled_value {
            bail!(
                "sample evidence {control_key}/{sampled_key} changed public {field}: {control_value} != {sampled_value}"
            );
        }
    }
    Ok(())
}

fn percentile(values: &[u128], quantile: f64) -> u128 {
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let index = ((ordered.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(ordered.len() - 1);
    ordered[index]
}

#[derive(Clone, Copy)]
enum Cardinality {
    ResultSeries,
    MatrixPoints,
    Scalar,
    String,
    BadData,
    ExecutionError,
    Ndjson,
}

fn cardinality(body: &[u8], kind: Cardinality) -> Result<usize> {
    if matches!(kind, Cardinality::Ndjson) {
        return Ok(body
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count());
    }
    let response: Value = serde_json::from_slice(body)?;
    if matches!(kind, Cardinality::BadData | Cardinality::ExecutionError) {
        let object = response
            .as_object()
            .context("error envelope must be an object")?;
        let expected = if matches!(kind, Cardinality::BadData) {
            "bad_data"
        } else {
            "execution"
        };
        if object.len() != 3
            || response.get("status") != Some(&json!("error"))
            || response.get("errorType") != Some(&json!(expected))
            || !response.get("error").is_some_and(Value::is_string)
        {
            bail!("unexpected error envelope: {response}");
        }
        return Ok(1);
    }
    if response.get("status") != Some(&json!("success")) {
        bail!("query failed: {response}");
    }
    match kind {
        Cardinality::ResultSeries => Ok(response
            .pointer("/data/result")
            .and_then(Value::as_array)
            .context("result array")?
            .len()),
        Cardinality::MatrixPoints => Ok(response
            .pointer("/data/result")
            .and_then(Value::as_array)
            .context("matrix result")?
            .iter()
            .map(|series| {
                series
                    .get("values")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len)
            })
            .sum()),
        Cardinality::Scalar | Cardinality::String => {
            let expected = if matches!(kind, Cardinality::Scalar) {
                "scalar"
            } else {
                "string"
            };
            let result = response
                .pointer("/data/result")
                .and_then(Value::as_array)
                .context("scalar/string result")?;
            if response.pointer("/data/resultType") != Some(&json!(expected)) || result.len() != 2 {
                bail!("invalid {expected} result: {response}");
            }
            if matches!(kind, Cardinality::String) && !result[1].is_string() {
                bail!("invalid string value: {response}");
            }
            Ok(1)
        }
        _ => unreachable!(),
    }
}

fn measure(
    name: &str,
    request: &mut dyn FnMut() -> Result<Vec<u8>>,
    kind: Cardinality,
    expected: usize,
    stats_before: &mut dyn FnMut() -> Result<Value>,
    iterations: usize,
    warmup: usize,
) -> Result<Value> {
    for _ in 0..warmup {
        let body = request()?;
        let actual = cardinality(&body, kind)?;
        if actual != expected {
            bail!("{name} warmup cardinality {actual}, expected {expected}");
        }
    }
    let before = stats_before()?;
    let mut latencies = Vec::with_capacity(iterations);
    let mut response_bytes = 0;
    for _ in 0..iterations {
        let started = Instant::now();
        let body = request()?;
        latencies.push(started.elapsed().as_nanos());
        response_bytes = body.len();
        let actual = cardinality(&body, kind)?;
        if actual != expected {
            bail!("{name} cardinality {actual}, expected {expected}");
        }
    }
    let after = stats_before()?;
    Ok(json!({
        "iterations": iterations,
        "warmup": warmup,
        "latency_ns": {
            "min": latencies.iter().min().copied().unwrap_or(0),
            "p50": percentile(&latencies, 0.50),
            "p95": percentile(&latencies, 0.95),
            "p99": percentile(&latencies, 0.99),
            "max": latencies.iter().max().copied().unwrap_or(0),
        },
        "result_cardinality": expected,
        "response_bytes": response_bytes,
        "stats_delta": numeric_delta(&before, &after),
    }))
}

fn hwm_kib(pid: u32) -> Result<u64> {
    let content = fs::read_to_string(format!("/proc/{pid}/status"))?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest
                .split_whitespace()
                .next()
                .context("VmHWM value")?
                .parse()
                .context("VmHWM integer");
        }
    }
    bail!("VmHWM is unavailable for pid {pid}")
}

#[derive(Clone)]
enum MetricRequest {
    Instant(String),
    InstantPost(String),
    MetricsQlInstant(String),
    Range {
        expression: String,
        start: i64,
        end: i64,
        step: String,
    },
    MetricsQlRange {
        expression: String,
        start: i64,
        end: i64,
        step: String,
    },
    Grid(String),
    Expected {
        path: String,
        body: Option<Vec<u8>>,
        status: u16,
        contains: Option<String>,
    },
}

struct MetricSpec {
    key: String,
    name: String,
    request: MetricRequest,
    cardinality: Cardinality,
    expected: usize,
}

#[derive(Clone, Copy)]
struct SignalEvidence<'a> {
    root: &'a Path,
    extension: &'a Path,
    binary: &'a Path,
    directory: &'a Path,
    iterations: usize,
    warmup: usize,
    client: &'a Client,
}

fn metric_request(
    client: &Client,
    base: &str,
    at: i64,
    request: &MetricRequest,
) -> Result<Vec<u8>> {
    let (status, body) = match request {
        MetricRequest::Instant(expression) => {
            let url = reqwest::Url::parse_with_params(
                &format!("{base}/api/v1/query"),
                [("query", expression.as_str()), ("time", &at.to_string())],
            )?;
            request_bytes(client, reqwest::Method::GET, url.as_str(), None)?
        }
        MetricRequest::InstantPost(expression) => {
            let body = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("query", expression)
                .append_pair("time", &at.to_string())
                .finish();
            request_bytes(
                client,
                reqwest::Method::POST,
                &format!("{base}/api/v1/query"),
                Some((body.as_bytes(), "application/x-www-form-urlencoded")),
            )?
        }
        MetricRequest::MetricsQlInstant(expression) => {
            let url = reqwest::Url::parse_with_params(
                &format!("{base}/metricsql/api/v1/query"),
                [("query", expression.as_str()), ("time", &at.to_string())],
            )?;
            request_bytes(client, reqwest::Method::GET, url.as_str(), None)?
        }
        MetricRequest::Range {
            expression,
            start,
            end,
            step,
        } => {
            let url = reqwest::Url::parse_with_params(
                &format!("{base}/api/v1/query_range"),
                [
                    ("query", expression.as_str()),
                    ("start", &start.to_string()),
                    ("end", &end.to_string()),
                    ("step", step),
                ],
            )?;
            request_bytes(client, reqwest::Method::GET, url.as_str(), None)?
        }
        MetricRequest::MetricsQlRange {
            expression,
            start,
            end,
            step,
        } => {
            let url = reqwest::Url::parse_with_params(
                &format!("{base}/metricsql/api/v1/query_range"),
                [
                    ("query", expression.as_str()),
                    ("start", &start.to_string()),
                    ("end", &end.to_string()),
                    ("step", step),
                ],
            )?;
            request_bytes(client, reqwest::Method::GET, url.as_str(), None)?
        }
        MetricRequest::Grid(expression) => {
            let start = (at - 10).to_string();
            let end = at.to_string();
            let url = reqwest::Url::parse_with_params(
                &format!("{base}/api/v1/query_range"),
                [
                    ("query", expression.as_str()),
                    ("start", &start),
                    ("end", &end),
                    ("step", "500ms"),
                    ("lookback_delta", "10001ms"),
                ],
            )?;
            request_bytes(client, reqwest::Method::GET, url.as_str(), None)?
        }
        MetricRequest::Expected {
            path,
            body,
            status: expected,
            contains,
        } => {
            let method = if body.is_some() {
                reqwest::Method::POST
            } else {
                reqwest::Method::GET
            };
            let response = request_bytes(
                client,
                method,
                &format!("{base}{path}"),
                body.as_ref()
                    .map(|body| (body.as_slice(), "application/x-www-form-urlencoded")),
            )?;
            if response.0 != *expected {
                bail!(
                    "expected HTTP {expected} for {path}, got {}: {}",
                    response.0,
                    String::from_utf8_lossy(&response.1)
                );
            }
            if let Some(contains) = contains {
                let decoded: Value = serde_json::from_slice(&response.1)?;
                if !decoded
                    .get("error")
                    .and_then(Value::as_str)
                    .is_some_and(|error| error.contains(contains))
                {
                    bail!("unexpected execution error for {path}: {decoded}");
                }
            }
            response
        }
    };
    if !matches!(request, MetricRequest::Expected { .. }) && !(200..300).contains(&status) {
        bail!(
            "metric request returned HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    Ok(body)
}

fn instant(
    key: impl Into<String>,
    name: impl Into<String>,
    expression: impl Into<String>,
    expected: usize,
) -> MetricSpec {
    MetricSpec {
        key: key.into(),
        name: name.into(),
        request: MetricRequest::Instant(expression.into()),
        cardinality: Cardinality::ResultSeries,
        expected,
    }
}

fn range(
    key: impl Into<String>,
    name: impl Into<String>,
    expression: impl Into<String>,
    at: i64,
    expected: usize,
) -> MetricSpec {
    MetricSpec {
        key: key.into(),
        name: name.into(),
        request: MetricRequest::Range {
            expression: expression.into(),
            start: at - 30,
            end: at,
            step: "10".to_owned(),
        },
        cardinality: Cardinality::MatrixPoints,
        expected,
    }
}

fn metricsql_instant(
    key: impl Into<String>,
    name: impl Into<String>,
    expression: impl Into<String>,
    expected: usize,
) -> MetricSpec {
    MetricSpec {
        key: key.into(),
        name: name.into(),
        request: MetricRequest::MetricsQlInstant(expression.into()),
        cardinality: Cardinality::ResultSeries,
        expected,
    }
}

fn metricsql_range(
    key: impl Into<String>,
    name: impl Into<String>,
    expression: impl Into<String>,
    at: i64,
    expected: usize,
) -> MetricSpec {
    MetricSpec {
        key: key.into(),
        name: name.into(),
        request: MetricRequest::MetricsQlRange {
            expression: expression.into(),
            start: at - 30,
            end: at,
            step: "10".to_owned(),
        },
        cardinality: Cardinality::MatrixPoints,
        expected,
    }
}

fn metric_specs(series: usize, selector_names: usize, at: i64) -> Vec<MetricSpec> {
    let mut specs = vec![
        instant(
            "narrow",
            "metrics-narrow",
            r#"query_contract_cpu{host="h0000"}"#,
            1,
        ),
        instant("wide", "metrics-wide", "query_contract_cpu", series),
        instant(
            "nameless_narrow",
            "metrics-nameless-narrow",
            r#"{selector_id="s0000"}"#,
            1,
        ),
        instant(
            "nameless_wide",
            "metrics-nameless-wide",
            r#"{selector_group="wide"}"#,
            selector_names,
        ),
        instant(
            "metric_name_regex_narrow",
            "metrics-name-regex-narrow",
            r#"{__name__=~"query_selector_metric_000[0-3]"}"#,
            4,
        ),
        instant(
            "metric_name_negative_wide",
            "metrics-name-negative-wide",
            r#"{__name__!="query_selector_metric_0000",selector_group="wide"}"#,
            selector_names - 1,
        ),
        instant(
            "offset_positive_narrow",
            "metrics-offset-positive-narrow",
            r#"query_contract_cpu{host="h0000"} offset 20s"#,
            1,
        ),
        MetricSpec {
            key: "offset_negative_wide".to_owned(),
            name: "metrics-offset-negative-wide".to_owned(),
            request: MetricRequest::Range {
                expression: "query_contract_cpu offset -20s".to_owned(),
                start: at - 50,
                end: at - 20,
                step: "10".to_owned(),
            },
            cardinality: Cardinality::MatrixPoints,
            expected: series * 4,
        },
        instant(
            "at_numeric_narrow",
            "metrics-at-numeric-narrow",
            format!(r#"query_contract_cpu{{host="h0000"}} @ {}"#, at - 20),
            1,
        ),
        range(
            "at_end_wide",
            "metrics-at-end-wide",
            "query_contract_cpu @ end()",
            at,
            series * 4,
        ),
        MetricSpec {
            key: "subquery_root_narrow".to_owned(),
            name: "metrics-subquery-root-narrow".to_owned(),
            request: MetricRequest::Instant(
                r#"query_contract_cpu{host="h0000"}[5m:10s]"#.to_owned(),
            ),
            cardinality: Cardinality::MatrixPoints,
            expected: 30,
        },
        MetricSpec {
            key: "subquery_root_wide".to_owned(),
            name: "metrics-subquery-root-wide".to_owned(),
            request: MetricRequest::Instant("query_contract_cpu[5m:10s]".to_owned()),
            cardinality: Cardinality::MatrixPoints,
            expected: series * 30,
        },
        instant(
            "subquery_avg_narrow",
            "metrics-subquery-avg-narrow",
            r#"avg_over_time(query_contract_cpu{host="h0000"}[5m:10s])"#,
            1,
        ),
        range(
            "subquery_avg_wide",
            "metrics-subquery-avg-wide",
            "avg_over_time(query_contract_cpu[5m:10s])",
            at,
            series * 4,
        ),
    ];
    let functions = [
        ("range_avg", "avg_over_time", None),
        ("range_min", "min_over_time", None),
        ("range_max", "max_over_time", None),
        ("range_sum", "sum_over_time", None),
        ("range_count", "count_over_time", None),
        ("range_present", "present_over_time", None),
        ("range_quantile", "quantile_over_time", Some("0.95, ")),
        ("range_stddev", "stddev_over_time", None),
        ("range_stdvar", "stdvar_over_time", None),
        ("range_rate", "rate", None),
        ("range_irate", "irate", None),
        ("range_increase", "increase", None),
        ("range_delta", "delta", None),
        ("range_idelta", "idelta", None),
        ("range_deriv", "deriv", None),
        ("range_predict_linear", "predict_linear", Some("")),
        ("range_changes", "changes", None),
        ("range_resets", "resets", None),
        ("range_last", "last_over_time", None),
    ];
    for (key, function, prefix) in functions {
        let narrow_expression = if function == "predict_linear" {
            r#"predict_linear(query_contract_cpu{host="h0000"}[5m], 60)"#.to_owned()
        } else {
            format!(
                r#"{function}({}query_contract_cpu{{host="h0000"}}[5m])"#,
                prefix.unwrap_or("")
            )
        };
        let wide_expression = if function == "predict_linear" {
            "predict_linear(query_contract_cpu[5m], 60)".to_owned()
        } else {
            format!("{function}({}query_contract_cpu[5m])", prefix.unwrap_or(""))
        };
        let narrow_key = format!("{key}_narrow");
        let wide_key = format!("{key}_wide");
        let narrow_name = format!(
            "metrics-{}-narrow",
            key.replace('_', "-").trim_start_matches("range-")
        );
        let wide_name = format!(
            "metrics-{}-wide",
            key.replace('_', "-").trim_start_matches("range-")
        );
        specs.push(instant(narrow_key, narrow_name, narrow_expression, 1));
        specs.push(range(wide_key, wide_name, wide_expression, at, series * 4));
    }
    let transforms = [
        ("unary_minus", "unary-minus", "-", ""),
        ("abs", "abs", "abs(", ")"),
        ("round", "round", "round(", ", 0.5)"),
        ("clamp", "clamp", "clamp(", ", 0, 10000)"),
        ("math", "ln", "ln(", ")"),
        ("sgn", "sgn", "sgn(", ")"),
        ("inverse", "atan", "atan(", ")"),
        ("trig", "sin", "sin(", ")"),
        ("angle", "deg", "deg(", ")"),
    ];
    for (key, label, prefix, suffix) in transforms {
        let narrow_key = format!("{key}_narrow");
        let wide_key = format!("{key}_wide");
        let narrow_name = format!("metrics-{label}-narrow");
        let wide_name = format!("metrics-{label}-wide");
        specs.push(instant(
            narrow_key,
            narrow_name,
            format!(r#"{prefix}query_contract_cpu{{host="h0000"}}{suffix}"#),
            1,
        ));
        specs.push(range(
            wide_key,
            wide_name,
            format!("{prefix}query_contract_cpu{suffix}"),
            at,
            series * 4,
        ));
    }
    specs.extend([
        instant("label_replace_narrow", "metrics-label-replace-narrow", r#"label_replace(query_contract_cpu{host="h0000"}, "node", "$1", "host", "(.*)")"#, 1),
        range("label_replace_wide", "metrics-label-replace-wide", r#"label_replace(query_contract_cpu, "node", "$1", "host", "(.*)")"#, at, series * 4),
        instant("label_join_narrow", "metrics-label-join-narrow", r#"label_join(query_contract_cpu{host="h0000"}, "node", "/", "host", "rack")"#, 1),
        range("label_join_wide", "metrics-label-join-wide", r#"label_join(query_contract_cpu, "node", "/", "host", "rack")"#, at, series * 4),
        instant("absent_narrow", "metrics-absent-missing-narrow", r#"absent(query_contract_cpu{host="missing"})"#, 1),
        range("absent_wide", "metrics-absent-present-wide", "absent(query_contract_cpu)", at, 0),
        instant("absent_over_time_narrow", "metrics-absent-over-time-missing-narrow", r#"absent_over_time(query_contract_cpu{host="missing"}[30s])"#, 1),
        range("absent_over_time_wide", "metrics-absent-over-time-present-wide", "absent_over_time(query_contract_cpu[30s])", at, 0),
        instant("sort_narrow", "metrics-sort-narrow", r#"sort(query_contract_cpu{host="h0000"})"#, 1),
        instant("sort_wide", "metrics-sort-desc-wide", "sort_desc(query_contract_cpu)", series),
        MetricSpec { key: "conversion_narrow".to_owned(), name: "metrics-scalar-single-narrow".to_owned(), request: MetricRequest::Instant(r#"scalar(query_contract_cpu{host="h0000"})"#.to_owned()), cardinality: Cardinality::Scalar, expected: 1 },
        MetricSpec { key: "conversion_wide".to_owned(), name: "metrics-scalar-cardinality-wide".to_owned(), request: MetricRequest::Instant("scalar(query_contract_cpu)".to_owned()), cardinality: Cardinality::Scalar, expected: 1 },
        instant("timestamp_narrow", "metrics-timestamp-narrow", r#"timestamp(query_contract_cpu{host="h0000"})"#, 1),
        instant("timestamp_wide", "metrics-timestamp-wide", "timestamp(query_contract_cpu)", series),
        instant("calendar_narrow", "metrics-calendar-minute-narrow", r#"minute(query_contract_cpu{host="h0000"})"#, 1),
        instant("calendar_wide", "metrics-calendar-day-of-week-wide", "day_of_week(query_contract_cpu)", series),
        instant("calendar_part_two_narrow", "metrics-calendar-year-narrow", r#"year(query_contract_cpu{host="h0000"})"#, 1),
        instant("calendar_part_two_wide", "metrics-calendar-day-of-year-wide", "day_of_year(query_contract_cpu)", series),
        instant("histogram_quantile_narrow", "metrics-histogram-quantile-narrow", r#"histogram_quantile(0.95, query_contract_histogram_bucket{host="h0000"})"#, 1),
        instant("histogram_quantile_wide", "metrics-histogram-quantile-wide", "histogram_quantile(0.95, query_contract_histogram_bucket)", series),
        instant("quoted_name_narrow", "metrics-quoted-name-narrow", r#"{"query.contract/温度","node.name"="n0000"}"#, 1),
        range("quoted_name_wide", "metrics-quoted-name-wide", r#"{"query.contract/温度"}"#, at, series * 4),
        instant("comments_narrow", "metrics-comments-narrow", "# narrow query\nquery_contract_cpu{host=\"h0000\"} # trailing", 1),
        range("comments_wide", "metrics-comments-wide", "query_contract_cpu # all retained series", at, series * 4),
        instant("histogram_fraction_narrow", "metrics-histogram-fraction-narrow", r#"histogram_fraction(0.1, 0.5, query_contract_histogram_bucket{host="h0000"})"#, 1),
        instant("histogram_fraction_wide", "metrics-histogram-fraction-wide", "histogram_fraction(0.1, 0.5, query_contract_histogram_bucket)", series),
        instant("native_histogram_float_narrow", "metrics-native-histogram-float-narrow", r#"histogram_count(query_contract_cpu{host="h0000"})"#, 0),
        range("native_histogram_float_wide", "metrics-native-histogram-float-wide", "histogram_count(query_contract_cpu)", at, 0),
        instant("native_histogram_float_control_narrow", "metrics-native-histogram-float-control-narrow", r#"query_contract_cpu{host="h0000"} > 1000000"#, 0),
        range("native_histogram_float_control_wide", "metrics-native-histogram-float-control-wide", "query_contract_cpu > 1000000", at, 0),
        instant("atan2_narrow", "metrics-atan2-narrow", r#"query_contract_cpu{host="h0000"} atan2 2"#, 1),
        range("atan2_wide", "metrics-atan2-wide", "query_contract_cpu atan2 2", at, series * 4),
        instant("annotations_narrow", "metrics-annotations-warning-narrow", r#"quantile(-1, query_contract_cpu{host="h0000"})"#, 1),
        range("annotations_wide", "metrics-annotations-info-wide", "rate(query_contract_cpu[5m])", at, series * 4),
        metricsql_instant("metricsql_default_narrow", "metrics-metricsql-default-narrow", r#"(query_contract_cpu{host="h0000"} > 10000) default 0"#, 1),
        metricsql_range("metricsql_default_wide", "metrics-metricsql-default-wide", "(query_contract_cpu > 10000) default 0", at, series * 4),
        metricsql_instant("metricsql_keep_names_narrow", "metrics-metricsql-keep-names-narrow", r#"abs(query_contract_cpu{host="h0000"}) keep_metric_names"#, 1),
        metricsql_range("metricsql_keep_names_wide", "metrics-metricsql-keep-names-wide", "abs(query_contract_cpu) keep_metric_names", at, series * 4),
        metricsql_instant("metricsql_alias_narrow", "metrics-metricsql-alias-narrow", r#"alias(query_contract_cpu{host="h0000"}, "query_contract_alias")"#, 1),
        metricsql_range("metricsql_alias_wide", "metrics-metricsql-alias-wide", r#"alias(query_contract_cpu, "query_contract_alias")"#, at, series * 4),
        metricsql_instant("metricsql_union_narrow", "metrics-metricsql-union-narrow", r#"union(alias(query_contract_cpu{host="h0000"}, "query_contract_union_a"), alias(query_contract_cpu{host="h0000"}, "query_contract_union_b"))"#, 2),
        metricsql_range("metricsql_union_wide", "metrics-metricsql-union-wide", r#"union(alias(query_contract_cpu, "query_contract_union_a"), alias(query_contract_cpu, "query_contract_union_b"))"#, at, series * 8),
        metricsql_instant("metricsql_label_set_narrow", "metrics-metricsql-label-set-narrow", r#"label_set(query_contract_cpu{host="h0000"}, "environment", "production", "host", "rewritten")"#, 1),
        metricsql_range("metricsql_label_set_wide", "metrics-metricsql-label-set-wide", r#"label_set(query_contract_cpu, "environment", "production")"#, at, series * 4),
        metricsql_instant("metricsql_label_del_narrow", "metrics-metricsql-label-del-narrow", r#"label_del(query_contract_cpu{host="h0000"}, "host")"#, 1),
        metricsql_range("metricsql_label_del_wide", "metrics-metricsql-label-del-wide", r#"label_del(query_contract_cpu, "__name__")"#, at, series * 4),
        metricsql_instant("metricsql_default_rollup_narrow", "metrics-metricsql-default-rollup-narrow", r#"query_contract_cpu{host="h0000"}"#, 1),
        metricsql_range("metricsql_default_rollup_wide", "metrics-metricsql-default-rollup-wide", "query_contract_cpu", at, series * 4),
        metricsql_instant("metricsql_windowless_avg_narrow", "metrics-metricsql-windowless-avg-narrow", r#"avg_over_time(query_contract_cpu{host="h0000"})"#, 1),
        metricsql_range("metricsql_windowless_avg_wide", "metrics-metricsql-windowless-avg-wide", "avg_over_time(query_contract_cpu)", at, series * 4),
        metricsql_instant("metricsql_windowless_rate_narrow", "metrics-metricsql-windowless-rate-narrow", r#"rate(query_contract_cpu{host="h0000"})"#, 1),
        metricsql_range("metricsql_windowless_rate_wide", "metrics-metricsql-windowless-rate-wide", "rate(query_contract_cpu)", at, series * 4),
        metricsql_range("metricsql_range_avg_narrow", "metrics-metricsql-range-avg-narrow", r#"range_avg(query_contract_cpu{host="h0000"})"#, at, 4),
        metricsql_range("metricsql_range_avg_wide", "metrics-metricsql-range-avg-wide", "range_avg(query_contract_cpu)", at, series * 4),
        metricsql_range("metricsql_range_sum_narrow", "metrics-metricsql-range-sum-narrow", r#"range_sum(query_contract_cpu{host="h0000"})"#, at, 4),
        metricsql_range("metricsql_range_sum_wide", "metrics-metricsql-range-sum-wide", "range_sum(query_contract_cpu)", at, series * 4),
        metricsql_range("metricsql_running_avg_narrow", "metrics-metricsql-running-avg-narrow", r#"running_avg(query_contract_cpu{host="h0000"})"#, at, 4),
        metricsql_range("metricsql_running_avg_wide", "metrics-metricsql-running-avg-wide", "running_avg(query_contract_cpu)", at, series * 4),
        metricsql_range("metricsql_running_sum_narrow", "metrics-metricsql-running-sum-narrow", r#"running_sum(query_contract_cpu{host="h0000"})"#, at, 4),
        metricsql_range("metricsql_running_sum_wide", "metrics-metricsql-running-sum-wide", "running_sum(query_contract_cpu)", at, series * 4),
        metricsql_range("metricsql_step_window_narrow", "metrics-metricsql-step-window-narrow", r#"count_over_time(query_contract_cpu{host="h0000"}[5i])"#, at, 4),
        metricsql_range("metricsql_step_window_wide", "metrics-metricsql-step-window-wide", "count_over_time(query_contract_cpu[5i])", at, series * 4),
        metricsql_range("metricsql_step_offset_narrow", "metrics-metricsql-step-offset-narrow", r#"query_contract_cpu{host="h0000"} offset 5i"#, at, 4),
        metricsql_range("metricsql_step_offset_wide", "metrics-metricsql-step-offset-wide", "query_contract_cpu offset 5i", at, series * 4),
        metricsql_range("metricsql_step_zero_rate_narrow", "metrics-metricsql-step-zero-rate-narrow", r#"rate(query_contract_cpu{host="h0000"}[0i])"#, at, 4),
        metricsql_range("metricsql_step_zero_rate_wide", "metrics-metricsql-step-zero-rate-wide", "rate(query_contract_cpu[0i])", at, series * 4),
        metricsql_range("metricsql_context_scalar", "metrics-metricsql-context-scalar", "time() - start() + step() - step()", at, 4),
        metricsql_range("metricsql_context_narrow", "metrics-metricsql-context-narrow", r#"query_contract_cpu{host="h0000"} + (end() - end())"#, at, 4),
        metricsql_range("metricsql_context_wide", "metrics-metricsql-context-wide", "query_contract_cpu + (start() - start())", at, series * 4),
        metricsql_instant("metricsql_histogram_quantiles_one_narrow", "metrics-metricsql-histogram-quantiles-one-narrow", r#"histogram_quantiles("phi", 0.95, query_contract_histogram_bucket{host="h0000"})"#, 1),
        metricsql_instant("metricsql_histogram_quantiles_one_wide", "metrics-metricsql-histogram-quantiles-one-wide", r#"histogram_quantiles("phi", 0.95, query_contract_histogram_bucket)"#, series),
        metricsql_instant("metricsql_histogram_quantiles_multi_narrow", "metrics-metricsql-histogram-quantiles-multi-narrow", r#"histogram_quantiles("phi", 0.25, 0.75, query_contract_histogram_bucket{host="h0000"})"#, 2),
        metricsql_instant("metricsql_histogram_quantiles_multi_wide", "metrics-metricsql-histogram-quantiles-multi-wide", r#"histogram_quantiles("phi", 0.25, 0.75, query_contract_histogram_bucket)"#, series * 2),
        instant("arithmetic_vector_scalar_narrow", "metrics-arithmetic-vector-scalar-narrow", r#"query_contract_cpu{host="h0000"} * 2"#, 1),
        range("arithmetic_one_to_one_wide", "metrics-arithmetic-one-to-one-wide", "query_contract_cpu + query_contract_cpu", at, series * 4),
        instant("comparison_filter_narrow", "metrics-comparison-filter-narrow", r#"query_contract_cpu{host="h0000"} > 30"#, 1),
        range("comparison_bool_wide", "metrics-comparison-bool-wide", "query_contract_cpu > bool 0", at, series * 4),
        instant("set_and_narrow", "metrics-set-and-narrow", r#"query_contract_cpu{host="h0000"} and query_contract_cpu{host="h0000"}"#, 1),
        range("set_or_wide", "metrics-set-or-wide", "query_contract_cpu or query_contract_cpu", at, series * 4),
        instant("matching_on_narrow", "metrics-matching-on-narrow", r#"query_contract_cpu{host="h0000"} + on(host) query_contract_cpu{host="h0000"}"#, 1),
        range("matching_on_wide", "metrics-matching-on-wide", "query_contract_cpu + on(host) query_contract_cpu", at, series * 4),
        instant("group_left_narrow", "metrics-group-left-narrow", r#"query_contract_cpu{host="h0000"} + on(service) group_left(team) query_contract_service_factor"#, 1),
        range("group_right_wide", "metrics-group-right-wide", "query_contract_service_factor - on(service) group_right(team) query_contract_cpu", at, series * 4),
        instant("sum_by_narrow", "metrics-sum-by-narrow", r#"sum by(host) (query_contract_cpu{host="h0000"})"#, 1),
        range("sum_by_wide", "metrics-sum-by-wide", "sum by(service) (query_contract_cpu)", at, 8),
        instant("avg_by_narrow", "metrics-avg-by-narrow", r#"avg by(host) (query_contract_cpu{host="h0000"})"#, 1),
        range("avg_by_wide", "metrics-avg-by-wide", "avg by(service) (query_contract_cpu)", at, 8),
        instant("min_by_narrow", "metrics-min-by-narrow", r#"min by(host) (query_contract_cpu{host="h0000"})"#, 1),
        range("max_by_wide", "metrics-max-by-wide", "max by(service) (query_contract_cpu)", at, 8),
        instant("count_by_narrow", "metrics-count-by-narrow", r#"count by(host) (query_contract_cpu{host="h0000"})"#, 1),
        range("group_by_wide", "metrics-group-by-wide", "group by(service) (query_contract_cpu)", at, 8),
        instant("stdvar_by_narrow", "metrics-stdvar-by-narrow", r#"stdvar by(host) (query_contract_cpu{host="h0000"})"#, 1),
        range("stddev_by_wide", "metrics-stddev-by-wide", "stddev by(service) (query_contract_cpu)", at, 8),
        instant("topk_narrow", "metrics-topk-narrow", r#"topk(1, query_contract_cpu{host="h0000"})"#, 1),
        range("bottomk_wide", "metrics-bottomk-wide", "bottomk by(service) (4, query_contract_cpu)", at, 32),
        instant("quantile_narrow", "metrics-quantile-narrow", r#"quantile(0.5, query_contract_cpu{host="h0000"})"#, 1),
        range("quantile_wide", "metrics-quantile-wide", "quantile by(service) (0.95, query_contract_cpu)", at, 8),
        instant("count_values_narrow", "metrics-count-values-narrow", r#"count_values("value", query_contract_cpu{host="h0000"})"#, 1),
        range("count_values_wide", "metrics-count-values-wide", r#"count_values by(service) ("value", query_contract_cpu)"#, at, series * 4),
        instant("range_vector_narrow", "metrics-range-vector-narrow", r#"query_contract_cpu{host="h0000"}[5m]"#, 1),
        instant("range_vector_wide", "metrics-range-vector-wide", "query_contract_cpu[5m]", series),
        instant("duration_range_vector_narrow", "metrics-duration-range-vector-narrow", r#"query_contract_cpu{host="h0000"}[5m250ms]"#, 1),
        instant("duration_range_vector_wide", "metrics-duration-range-vector-wide", "query_contract_cpu[5m250ms]", series),
        MetricSpec { key: "scalar_instant".to_owned(), name: "metrics-scalar-instant".to_owned(), request: MetricRequest::Instant("NaN".to_owned()), cardinality: Cardinality::Scalar, expected: 1 },
        MetricSpec { key: "scalar_range_11000".to_owned(), name: "metrics-scalar-range-limit".to_owned(), request: MetricRequest::Range { expression: "NaN".to_owned(), start: at, end: at + 10_999, step: "1".to_owned() }, cardinality: Cardinality::ResultSeries, expected: 1 },
        MetricSpec { key: "string_instant".to_owned(), name: "metrics-string-instant".to_owned(), request: MetricRequest::Instant(r#""contract\nvalue""#.to_owned()), cardinality: Cardinality::String, expected: 1 },
        MetricSpec { key: "string_64k".to_owned(), name: "metrics-string-64k".to_owned(), request: MetricRequest::InstantPost(format!("\"{}\"", "x".repeat(65_536))), cardinality: Cardinality::String, expected: 1 },
        MetricSpec { key: "grid_lookback_narrow".to_owned(), name: "metrics-grid-lookback-narrow".to_owned(), request: MetricRequest::Grid(r#"query_contract_cpu{host="h0000"}"#.to_owned()), cardinality: Cardinality::ResultSeries, expected: 1 },
        MetricSpec { key: "grid_lookback_wide".to_owned(), name: "metrics-grid-lookback-wide".to_owned(), request: MetricRequest::Grid("query_contract_cpu".to_owned()), cardinality: Cardinality::ResultSeries, expected: series },
        MetricSpec { key: "error_narrow".to_owned(), name: "metrics-error-narrow".to_owned(), request: MetricRequest::Expected { path: "/prometheus/api/v1/query_range?query=1&start=0&end=1&step=bad".to_owned(), body: None, status: 400, contains: None }, cardinality: Cardinality::BadData, expected: 1 },
        MetricSpec { key: "error_64k".to_owned(), name: "metrics-error-64k".to_owned(), request: MetricRequest::Expected { path: "/prometheus/api/v1/query".to_owned(), body: Some(url::form_urlencoded::Serializer::new(String::new()).append_pair("query", "up").append_pair("extra", &"x".repeat(65_536)).finish().into_bytes()), status: 400, contains: None }, cardinality: Cardinality::BadData, expected: 1 },
        MetricSpec { key: "near_result_limit".to_owned(), name: "metrics-result-limit-near".to_owned(), request: MetricRequest::Range { expression: "query_contract_cpu".to_owned(), start: at - 194, end: at, step: "1".to_owned() }, cardinality: Cardinality::MatrixPoints, expected: series * 195 },
    ]);
    let over = reqwest::Url::parse_with_params(
        "http://unused/api/v1/query_range",
        [
            ("query", "query_contract_cpu"),
            ("start", &(at - 195).to_string()),
            ("end", &at.to_string()),
            ("step", "1"),
        ],
    )
    .unwrap();
    specs.push(MetricSpec {
        key: "result_limit_rejected".to_owned(),
        name: "metrics-result-limit-rejected".to_owned(),
        request: MetricRequest::Expected {
            path: format!("/api/v1/query_range?{}", over.query().unwrap()),
            body: None,
            status: 422,
            contains: Some("result-point limit of 100000".to_owned()),
        },
        cardinality: Cardinality::ExecutionError,
        expected: 1,
    });
    specs
}

fn append_json_line(payload: &mut Vec<u8>, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *payload, value)?;
    payload.push(b'\n');
    Ok(())
}

fn selected_stats(stats: &Value, keys: &[&str]) -> Value {
    let mut selected = Map::new();
    for key in keys {
        selected.insert(
            (*key).to_owned(),
            stats.get(*key).cloned().unwrap_or(Value::Null),
        );
    }
    Value::Object(selected)
}

fn metrics_evidence(context: &SignalEvidence<'_>, series: usize, points: usize) -> Result<Value> {
    let expected_commit = git_commit(context.root)?;
    let identity = require_binary_identity(context.binary, &expected_commit)?;
    let mut server = Server::start(
        context.binary,
        context.extension,
        &context.directory.join("metrics.db"),
        &[
            ("TIMELESS_AUTH_MODE", "disabled"),
            ("TIMELESS_METRICS_FLUSH_INTERVAL_SECS", "3600"),
            ("TIMELESS_METRICS_COMPACT_INTERVAL_SECS", "3600"),
            ("TIMELESS_METRICS_RETENTION_INTERVAL_SECS", "3600"),
        ],
        context.client,
    )?;
    let result = (|| {
        let base_ts = 1_800_000_000_i64;
        let timestamps: Vec<_> = (0..points)
            .map(|point| (base_ts + point as i64 * 10) * 1_000)
            .collect();
        let mut payload = Vec::new();
        for index in 0..series {
            append_json_line(
                &mut payload,
                &json!({
                    "metric": {"__name__": "query_contract_cpu", "host": format!("h{index:04}"), "service": if index % 2 == 0 { "api" } else { "worker" }},
                    "values": (0..points).map(|point| (index + point) as f64).collect::<Vec<_>>(),
                    "timestamps": timestamps,
                }),
            )?;
        }
        for (service, team, value) in [("api", "frontend", 2.0), ("worker", "backend", 3.0)] {
            append_json_line(
                &mut payload,
                &json!({
                    "metric": {"__name__": "query_contract_service_factor", "service": service, "team": team},
                    "values": vec![value; points], "timestamps": timestamps,
                }),
            )?;
        }
        let selector_names = 64;
        for index in 0..selector_names {
            append_json_line(
                &mut payload,
                &json!({
                    "metric": {"__name__": format!("query_selector_metric_{index:04}"), "selector_group": "wide", "selector_id": format!("s{index:04}")},
                    "values": (0..points).map(|point| (index + point) as f64).collect::<Vec<_>>(), "timestamps": timestamps,
                }),
            )?;
        }
        for index in 0..series {
            append_json_line(
                &mut payload,
                &json!({
                    "metric": {"__name__": "query.contract/温度", "node.name": format!("n{index:04}"), "service": if index % 2 == 0 { "api" } else { "worker" }},
                    "values": (0..points).map(|point| (index + point) as f64).collect::<Vec<_>>(),
                    "timestamps": timestamps,
                }),
            )?;
        }
        let histogram_bounds = [("0.1", 10.0), ("0.5", 20.0), ("1", 30.0), ("+Inf", 40.0)];
        for index in 0..series {
            for (bound, count) in histogram_bounds {
                append_json_line(
                    &mut payload,
                    &json!({
                        "metric": {"__name__": "query_contract_histogram_bucket", "host": format!("h{index:04}"), "service": if index % 2 == 0 { "api" } else { "worker" }, "le": bound},
                        "values": [count + index as f64], "timestamps": [timestamps[points - 1]],
                    }),
                )?;
            }
        }
        let started = Instant::now();
        let (status, body) = request_bytes(
            context.client,
            reqwest::Method::POST,
            &format!("{}/api/v1/import", server.base),
            Some((&payload, "application/json")),
        )?;
        let admission_ns = started.elapsed().as_nanos();
        if status != 204 {
            bail!(
                "metrics import returned {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        let started = Instant::now();
        let (status, body) = request_bytes(
            context.client,
            reqwest::Method::POST,
            &format!("{}/api/v1/flush", server.base),
            Some((&[], "application/json")),
        )?;
        let durable_ns = started.elapsed().as_nanos();
        if status != 200 {
            bail!(
                "metrics flush returned {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        let after_flush = stats(context.client, &server.base, "/select/metrics/stats")?;
        let expected_points =
            (series * 2 + selector_names + 2) * points + series * histogram_bounds.len();
        if after_flush.get("completed_points").and_then(Value::as_u64)
            != Some(expected_points as u64)
            || after_flush.get("queued_points").and_then(Value::as_u64) != Some(0)
        {
            bail!("metrics durable watermark mismatch: {after_flush}");
        }
        let at = base_ts + (points as i64 - 1) * 10;
        let mut queries = Map::new();
        for spec in metric_specs(series, selector_names, at) {
            let mut request = || metric_request(context.client, &server.base, at, &spec.request);
            let mut stat = || stats(context.client, &server.base, "/select/metrics/stats");
            let evidence = measure(
                &spec.name,
                &mut request,
                spec.cardinality,
                spec.expected,
                &mut stat,
                context.iterations,
                context.warmup,
            )?;
            queries.insert(spec.key.to_owned(), evidence);
        }

        let limit_series = 25;
        let limit_points = 4_001;
        let limit_timestamps: Vec<_> = (0..limit_points)
            .map(|point| (base_ts + point as i64) * 1_000)
            .collect();
        let mut limit_payload = Vec::new();
        for index in 0..limit_series {
            append_json_line(
                &mut limit_payload,
                &json!({
                    "metric": {"__name__": "query_limit_work", "host": format!("limit-{index:02}")},
                    "values": (0..limit_points).map(|point| (index + point) as f64).collect::<Vec<_>>(), "timestamps": limit_timestamps,
                }),
            )?;
        }
        let started = Instant::now();
        let (status, body) = request_bytes(
            context.client,
            reqwest::Method::POST,
            &format!("{}/api/v1/import", server.base),
            Some((&limit_payload, "application/json")),
        )?;
        let limit_admission_ns = started.elapsed().as_nanos();
        if status != 204 {
            bail!(
                "limit fixture import returned {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        let started = Instant::now();
        let (status, body) = request_bytes(
            context.client,
            reqwest::Method::POST,
            &format!("{}/api/v1/flush", server.base),
            Some((&[], "application/json")),
        )?;
        let limit_durable_ns = started.elapsed().as_nanos();
        if status != 200 {
            bail!(
                "limit fixture flush returned {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        let after_limit_flush = stats(context.client, &server.base, "/select/metrics/stats")?;
        let limit_logical_points = limit_series * limit_points;
        if after_limit_flush
            .get("completed_points")
            .and_then(Value::as_u64)
            != Some((expected_points + limit_logical_points) as u64)
        {
            bail!("limit fixture durable watermark mismatch: {after_limit_flush}");
        }
        let query = reqwest::Url::parse_with_params(
            "http://unused/api/v1/query",
            [
                ("query", "query_limit_work[4001s]"),
                ("time", &(base_ts + limit_points as i64 - 1).to_string()),
            ],
        )?;
        let work_spec = MetricSpec {
            key: "work_limit_rejected".to_owned(),
            name: "metrics-work-limit-rejected".to_owned(),
            request: MetricRequest::Expected {
                path: format!("/api/v1/query?{}", query.query().unwrap()),
                body: None,
                status: 422,
                contains: Some("work point limit 100000 exceeded".to_owned()),
            },
            cardinality: Cardinality::ExecutionError,
            expected: 1,
        };
        let mut request = || metric_request(context.client, &server.base, at, &work_spec.request);
        let mut stat = || stats(context.client, &server.base, "/select/metrics/stats");
        let work_evidence = measure(
            &work_spec.name,
            &mut request,
            work_spec.cardinality,
            work_spec.expected,
            &mut stat,
            context.iterations,
            context.warmup,
        )?;
        if work_evidence
            .pointer("/stats_delta/raw_batch_query_payload_bytes_read")
            .and_then(Value::as_i64)
            .unwrap_or(0)
            != 0
        {
            bail!("work-limit rejection read persisted payload bytes before failing");
        }
        queries.insert(work_spec.key.to_owned(), work_evidence);
        let final_stats = stats(context.client, &server.base, "/select/metrics/stats")?;
        let hwm = hwm_kib(server.pid())?;
        Ok(json!({
            "build": identity,
            "fixture": {"exact_metric_series": series, "quoted_metric_series": series, "selector_metric_names": selector_names, "points_per_series": points, "logical_points": expected_points},
            "ingestion": {"wire_bytes": payload.len(), "admission_ns": admission_ns, "durability_barrier_ns": durable_ns, "completed_points": after_flush["completed_points"], "failed_points": after_flush["failed_points"], "queued_points": after_flush["queued_points"]},
            "limit_fixture_ingestion": {"series": limit_series, "points_per_series": limit_points, "logical_points": limit_logical_points, "wire_bytes": limit_payload.len(), "admission_ns": limit_admission_ns, "durability_barrier_ns": limit_durable_ns, "completed_points_after": after_limit_flush["completed_points"], "failed_points_after": after_limit_flush["failed_points"], "queued_points_after": after_limit_flush["queued_points"]},
            "queries": queries,
            "storage": selected_stats(&final_stats, &["bytes_on_disk", "sqlite_index_bytes", "database_file_bytes", "database_wal_bytes", "database_shm_bytes", "physical_database_bytes", "buffer_memory_bytes", "chunks", "series"]),
            "rss_hwm_kib": hwm,
            "limits": {"points_per_series": 11_000, "result_points": 100_000, "work_points": 100_000, "response_bytes": 16 * 1024 * 1024, "default_subquery_step_ms": 15_000, "deadline_ms": 30_000, "contract_test": "session_two_promql_limits_bound_grid_work_results_response_and_deadline"},
            "cancellation": {"cancelled_requests": final_stats["api_read_cancelled"], "contract_test": "session_four_cancels_dropped_promql_requests_and_reuses_the_reader"},
        }))
    })();
    let shutdown = server.shutdown(true);
    match (result, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn logs_evidence(context: &SignalEvidence<'_>, entries: usize) -> Result<Value> {
    let expected_commit = git_commit(context.root)?;
    let identity = require_binary_identity(context.binary, &expected_commit)?;
    let mut server = Server::start(
        context.binary,
        context.extension,
        &context.directory.join("logs.db"),
        &[
            ("TIMELESS_AUTH_MODE", "disabled"),
            ("TIMELESS_LOGS_FLUSH_INTERVAL_SECS", "3600"),
            ("TIMELESS_LOGS_OPTIMIZE_INTERVAL_SECS", "3600"),
        ],
        context.client,
    )?;
    let result = (|| {
        let severities = [
            "debug",
            "info",
            "notice",
            "warning",
            "error",
            "critical",
            "alert",
            "emergency",
        ];
        let base_ts = 1_800_000_000_000_000_i64;
        let mut payload = Vec::new();
        for index in 0..entries {
            append_json_line(
                &mut payload,
                &json!({
                    "_time": base_ts + index as i64, "_msg": format!("query contract event {index}"),
                    "level": severities[index % severities.len()], "service": if index % 4 == 0 { "api" } else { "worker" },
                    "host": format!("h{:02}", index % 64), "status": if index % 8 == 4 { 500 } else { 200 },
                    "context": {"retry": index % 3 == 0, "attempt": index % 5},
                    "tags": ["query", true],
                    "client_ip": format!("10.0.{}.{}", (index / 256) % 256, index % 256),
                    "client_ipv6": format!("2001:db8::{index:x}"),
                    "range_key": format!("key-{index:04x}"),
                }),
            )?;
        }
        let started = Instant::now();
        let (status, body) = request_bytes(
            context.client,
            reqwest::Method::POST,
            &format!("{}/insert/jsonline", server.base),
            Some((&payload, "application/x-ndjson")),
        )?;
        let admission_ns = started.elapsed().as_nanos();
        if status != 204 {
            bail!(
                "logs ingest returned {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        let started = Instant::now();
        let (status, body) = request_bytes(
            context.client,
            reqwest::Method::GET,
            &format!("{}/api/v1/flush", server.base),
            None,
        )?;
        let durable_ns = started.elapsed().as_nanos();
        if status != 200 {
            bail!(
                "logs flush returned {status}: {}",
                String::from_utf8_lossy(&body)
            );
        }
        let after_flush = stats(context.client, &server.base, "/select/logsql/stats")?;
        if after_flush.get("completed_entries").and_then(Value::as_u64) != Some(entries as u64)
            || after_flush.get("queued_entries").and_then(Value::as_u64) != Some(0)
        {
            bail!("logs durable watermark mismatch: {after_flush}");
        }
        let mut queries = Map::new();
        let exact_matches = (0..entries).filter(|index| index % 8 == 4).count();
        let host_matches = (0..entries).filter(|index| index % 64 == 0).count();
        let numeric_matches = (0..entries)
            .filter(|index| matches!(index % 5, 2 | 3))
            .count();
        let host_numeric_matches = (0..entries)
            .filter(|index| index % 64 == 0 && matches!(index % 5, 2 | 3))
            .count();
        let typed_exact_prefix_matches = (0..entries).filter(|index| index % 5 == 1).count();
        let host_typed_exact_prefix_matches = (0..entries)
            .filter(|index| index % 64 == 0 && index % 5 == 1)
            .count();
        let multi_exact_message_matches = usize::from(entries > 0) + usize::from(entries > 1);
        let host_multi_exact_message_matches = usize::from(entries > 0) + usize::from(entries > 64);
        let typed_multi_exact_matches = (0..entries)
            .filter(|index| matches!(index % 5, 1 | 3))
            .count();
        let host_typed_multi_exact_matches = (0..entries)
            .filter(|index| index % 64 == 0 && matches!(index % 5, 1 | 3))
            .count();
        let attempt_zero_matches = (0..entries).filter(|index| index % 5 == 0).count();
        let host_attempt_zero_matches = (0..entries)
            .filter(|index| index % 64 == 0 && index % 5 == 0)
            .count();
        let service_api_matches = entries.div_ceil(4);
        for (key, name, expression, expected, expected_total) in [
            (
                "narrow",
                "logs-narrow",
                "level:error service:api status:=500 | sort by (_time) desc | offset 1 | limit 100",
                exact_matches.saturating_sub(1).min(100),
                None,
            ),
            (
                "wide",
                "logs-wide-phrase",
                "\"query contract\" | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "count",
                "logs-native-count",
                "service:api | stats count() as total",
                1,
                Some(entries.div_ceil(4) as u64),
            ),
            (
                "word_narrow",
                "logs-word-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "word_wide",
                "logs-word-wide",
                "query | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "prefix_narrow",
                "logs-prefix-narrow",
                "host:=\"h00\" AND quer* | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "prefix_wide",
                "logs-prefix-wide",
                "quer* | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "substring_narrow",
                "logs-substring-narrow",
                "host:=\"h00\" AND *contract* | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "substring_wide",
                "logs-substring-wide",
                "*contract* | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "regexp_narrow",
                "logs-regexp-narrow",
                "host:=\"h00\" AND ~\"event [0-9]+$\" | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "regexp_wide",
                "logs-regexp-wide",
                "~\"event [0-9]+$\" | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "pattern_match_narrow",
                "logs-pattern-match-narrow",
                "host:=\"h00\" AND pattern_match_full(\"query contract event <N>\") | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "pattern_match_wide",
                "logs-pattern-match-wide",
                "pattern_match_full(\"query contract event <N>\") | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "pattern_match_typed_field_narrow",
                "logs-pattern-match-typed-field-narrow",
                "host:=\"h00\" AND context.attempt:pattern_match_full(\"<N>\") | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "pattern_match_typed_field_wide",
                "logs-pattern-match-typed-field-wide",
                "context.attempt:pattern_match_full(\"<N>\") | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "case_insensitive_narrow",
                "logs-case-insensitive-narrow",
                "host:=\"h00\" AND i(QUERY) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "case_insensitive_wide",
                "logs-case-insensitive-wide",
                "i(QUERY) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "exact_narrow",
                "logs-exact-narrow",
                "host:=\"h00\" AND =\"query contract event 0\" | limit 10000",
                1,
                None,
            ),
            (
                "exact_wide",
                "logs-exact-wide-absent",
                "=\"query contract event absent\" | limit 10000",
                0,
                None,
            ),
            (
                "exact_prefix_narrow",
                "logs-exact-prefix-narrow",
                "host:=\"h00\" AND =\"query contract\"* | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "exact_prefix_wide",
                "logs-exact-prefix-wide",
                "=\"query contract\"* | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "exact_prefix_typed_field_narrow",
                "logs-exact-prefix-typed-field-narrow",
                "host:=\"h00\" AND context.attempt:=\"1\"* | sort by (_time) asc | limit 10000",
                host_typed_exact_prefix_matches,
                None,
            ),
            (
                "exact_prefix_typed_field_wide",
                "logs-exact-prefix-typed-field-wide",
                "context.attempt:=\"1\"* | sort by (_time) asc | limit 10000",
                typed_exact_prefix_matches,
                None,
            ),
            (
                "multi_exact_narrow",
                "logs-multi-exact-narrow",
                "host:=\"h00\" AND in(\"query contract event 0\", \"query contract event 64\") | sort by (_time) asc | limit 10000",
                host_multi_exact_message_matches,
                None,
            ),
            (
                "multi_exact_wide",
                "logs-multi-exact-wide",
                "in(\"query contract event 0\", \"query contract event 1\") | sort by (_time) asc | limit 10000",
                multi_exact_message_matches,
                None,
            ),
            (
                "multi_exact_typed_field_narrow",
                "logs-multi-exact-typed-field-narrow",
                "host:=\"h00\" AND context.attempt:in(1, 3) | sort by (_time) asc | limit 10000",
                host_typed_multi_exact_matches,
                None,
            ),
            (
                "multi_exact_typed_field_wide",
                "logs-multi-exact-typed-field-wide",
                "context.attempt:in(1, 3) | sort by (_time) asc | limit 10000",
                typed_multi_exact_matches,
                None,
            ),
            (
                "query_backed_in_control_narrow",
                "logs-query-backed-in-control-narrow",
                "host:=\"h00\" AND context.attempt:in(0) | sort by (_time) asc | limit 10000",
                host_attempt_zero_matches,
                None,
            ),
            (
                "query_backed_in_narrow",
                "logs-query-backed-in-narrow",
                "host:=\"h00\" AND context.attempt:in(host:=\"h00\" AND context.attempt:=0 | fields context.attempt) | sort by (_time) asc | limit 10000",
                host_attempt_zero_matches,
                None,
            ),
            (
                "query_backed_in_control_wide",
                "logs-query-backed-in-control-wide",
                "context.attempt:in(0) | sort by (_time) asc | limit 10000",
                attempt_zero_matches,
                None,
            ),
            (
                "query_backed_in_wide",
                "logs-query-backed-in-wide",
                "context.attempt:in(host:=\"h00\" AND context.attempt:=0 | fields context.attempt) | sort by (_time) asc | limit 10000",
                attempt_zero_matches,
                None,
            ),
            (
                "equals_common_case_control_narrow",
                "logs-equals-common-case-control-narrow",
                "host:=\"h00\" AND service:in(api, Api, API) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "equals_common_case_narrow",
                "logs-equals-common-case-narrow",
                "host:=\"h00\" AND service:equals_common_case(Api) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "equals_common_case_control_wide",
                "logs-equals-common-case-control-wide",
                "service:in(api, Api, API) | sort by (_time) asc | limit 10000",
                service_api_matches,
                None,
            ),
            (
                "equals_common_case_wide",
                "logs-equals-common-case-wide",
                "service:equals_common_case(Api) | sort by (_time) asc | limit 10000",
                service_api_matches,
                None,
            ),
            (
                "contains_common_case_control_narrow",
                "logs-contains-common-case-control-narrow",
                "host:=\"h00\" AND contains_any(query, Query, QUERY) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "contains_common_case_narrow",
                "logs-contains-common-case-narrow",
                "host:=\"h00\" AND contains_common_case(Query) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "contains_common_case_control_wide",
                "logs-contains-common-case-control-wide",
                "contains_any(query, Query, QUERY) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "contains_common_case_wide",
                "logs-contains-common-case-wide",
                "contains_common_case(Query) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "sample_control_narrow",
                "logs-sample-control-narrow",
                "host:=\"h00\" | sample 1 | stats count() as total",
                1,
                None,
            ),
            (
                "sample_narrow",
                "logs-sample-four-narrow",
                "host:=\"h00\" | sample 4 | stats count() as total",
                1,
                None,
            ),
            (
                "sample_control_wide",
                "logs-sample-control-wide",
                "* | sample 1 | stats count() as total",
                1,
                None,
            ),
            (
                "sample_wide",
                "logs-sample-four-wide",
                "* | sample 4 | stats count() as total",
                1,
                None,
            ),
            (
                "field_noop_narrow",
                "logs-field-noop-narrow",
                "host:=\"h00\" AND never_present:contains_any(*) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "field_noop_wide",
                "logs-field-noop-wide",
                "never_present:contains_all(*) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "contains_all_narrow",
                "logs-contains-all-narrow",
                "host:=\"h00\" AND contains_all(query, contract) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "contains_all_wide",
                "logs-contains-all-wide",
                "contains_all(query, contract) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "contains_all_typed_field_narrow",
                "logs-contains-all-typed-field-narrow",
                "host:=\"h00\" AND context:contains_all(attempt, retry) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "contains_all_typed_field_wide",
                "logs-contains-all-typed-field-wide",
                "context:contains_all(attempt, retry) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "sequence_narrow",
                "logs-sequence-narrow",
                "host:=\"h00\" AND seq(query, contract, event) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "sequence_wide",
                "logs-sequence-wide",
                "seq(query, contract, event) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "sequence_typed_field_narrow",
                "logs-sequence-typed-field-narrow",
                "host:=\"h00\" AND context:seq(attempt, retry) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "sequence_typed_field_wide",
                "logs-sequence-typed-field-wide",
                "context:seq(attempt, retry) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "contains_any_narrow",
                "logs-contains-any-narrow",
                "host:=\"h00\" AND contains_any(query, absent) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "contains_any_wide",
                "logs-contains-any-wide",
                "contains_any(query, absent) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "contains_any_typed_field_narrow",
                "logs-contains-any-typed-field-narrow",
                "host:=\"h00\" AND context:contains_any(attempt, absent) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "contains_any_typed_field_wide",
                "logs-contains-any-typed-field-wide",
                "context:contains_any(attempt, absent) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "json_array_contains_any_narrow",
                "logs-json-array-contains-any-narrow",
                "host:=\"h00\" AND tags:json_array_contains_any(query, absent) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "json_array_contains_any_wide",
                "logs-json-array-contains-any-wide",
                "tags:json_array_contains_any(query, absent) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "json_array_contains_any_primitive_narrow",
                "logs-json-array-contains-any-primitive-narrow",
                "host:=\"h00\" AND tags:json_array_contains_any(true, absent) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "json_array_contains_any_primitive_wide",
                "logs-json-array-contains-any-primitive-wide",
                "tags:json_array_contains_any(true, absent) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "ipv4_range_narrow",
                "logs-ipv4-range-cidr-narrow",
                "host:=\"h00\" AND client_ip:ipv4_range(10.0.0.0/19) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "ipv4_range_wide",
                "logs-ipv4-range-cidr-wide",
                "client_ip:ipv4_range(10.0.0.0/19) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "ipv4_range_bounds_narrow",
                "logs-ipv4-range-bounds-narrow",
                "host:=\"h00\" AND client_ip:ipv4_range(10.0.0.0, 10.0.31.255) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "ipv4_range_bounds_wide",
                "logs-ipv4-range-bounds-wide",
                "client_ip:ipv4_range(10.0.0.0, 10.0.31.255) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "ipv6_range_narrow",
                "logs-ipv6-range-cidr-narrow",
                "host:=\"h00\" AND client_ipv6:ipv6_range(2001:db8::/115) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "ipv6_range_wide",
                "logs-ipv6-range-cidr-wide",
                "client_ipv6:ipv6_range(2001:db8::/115) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "ipv6_range_bounds_narrow",
                "logs-ipv6-range-bounds-narrow",
                "host:=\"h00\" AND client_ipv6:ipv6_range(2001:db8::, 2001:db8::1fff) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "ipv6_range_bounds_wide",
                "logs-ipv6-range-bounds-wide",
                "client_ipv6:ipv6_range(2001:db8::, 2001:db8::1fff) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "string_range_narrow",
                "logs-string-range-narrow",
                "host:=\"h00\" AND range_key:string_range(key-0000, key-2000) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "string_range_wide",
                "logs-string-range-wide",
                "range_key:string_range(key-0000, key-2000) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "string_range_typed_field_narrow",
                "logs-string-range-typed-field-narrow",
                "host:=\"h00\" AND context.attempt:string_range(0, 5) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "string_range_typed_field_wide",
                "logs-string-range-typed-field-wide",
                "context.attempt:string_range(0, 5) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "len_range_narrow",
                "logs-len-range-narrow",
                "host:=\"h00\" AND range_key:len_range(8, 8) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "len_range_wide",
                "logs-len-range-wide",
                "range_key:len_range(8, 8) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "len_range_typed_field_narrow",
                "logs-len-range-typed-field-narrow",
                "host:=\"h00\" AND context.attempt:len_range(1, 1) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "len_range_typed_field_wide",
                "logs-len-range-typed-field-wide",
                "context.attempt:len_range(1, 1) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "field_eq_narrow",
                "logs-field-eq-narrow",
                "host:=\"h00\" AND range_key:eq_field(range_key) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "field_eq_wide",
                "logs-field-eq-wide",
                "range_key:eq_field(range_key) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "field_lt_typed_narrow",
                "logs-field-lt-typed-narrow",
                "host:=\"h00\" AND context.attempt:lt_field(status) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "field_lt_typed_wide",
                "logs-field-lt-typed-wide",
                "context.attempt:lt_field(status) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "field_prefix_word_narrow",
                "logs-field-prefix-word-narrow",
                "host:=\"h00\" AND range_*:key | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "field_prefix_word_wide",
                "logs-field-prefix-word-wide",
                "range_*:key | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "field_prefix_typed_narrow",
                "logs-field-prefix-typed-narrow",
                "host:=\"h00\" AND context.*:value_type(uint64) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "field_prefix_typed_wide",
                "logs-field-prefix-typed-wide",
                "context.*:value_type(uint64) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "day_range_narrow",
                "logs-day-range-narrow",
                "host:=\"h00\" AND _time:day_range[08:00, 09:00) offset 0h | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "day_range_wide",
                "logs-day-range-wide",
                "_time:day_range[08:00, 09:00) offset 0h | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "week_range_narrow",
                "logs-week-range-narrow",
                "host:=\"h00\" AND _time:week_range[Fri, Fri] offset 0h | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "week_range_wide",
                "logs-week-range-wide",
                "_time:week_range[Fri, Fri] offset 0h | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "comments_narrow",
                "logs-comments-narrow",
                "host:=\"h00\" # retain indexed pruning\nAND query\n| sort by (_time) asc\n| limit 10000; # terminal",
                host_matches,
                None,
            ),
            (
                "comments_wide",
                "logs-comments-wide",
                "# leading parser comment\nquery\n| sort by (_time) asc\n| limit 10000; # terminal",
                entries,
                None,
            ),
            (
                "delete_narrow",
                "logs-delete-narrow",
                "host:=\"h00\" AND query | delete context.*, range_key | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "delete_wide",
                "logs-delete-wide",
                "query | delete context.*, range_key | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "query_stats_narrow",
                "logs-query-stats-narrow",
                "host:=\"h00\" AND query | query_stats",
                1,
                None,
            ),
            (
                "query_stats_wide",
                "logs-query-stats-wide",
                "query | query_stats",
                1,
                None,
            ),
            (
                "first_narrow",
                "logs-first-narrow",
                "host:=\"h00\" AND query | first 8 by (context.attempt, range_key) partition by (context.retry) rank as position | fields _time, _msg, host, status, context.attempt, range_key, position",
                16,
                None,
            ),
            (
                "first_wide",
                "logs-first-wide",
                "query | first 8 by (context.attempt, range_key) partition by (service, level) rank as position | fields _time, _msg, service, level, status, context.attempt, range_key, position",
                64,
                None,
            ),
            (
                "last_narrow",
                "logs-last-narrow",
                "host:=\"h00\" AND query | last 8 by (context.attempt, range_key) partition by (context.retry) rank as position | fields _time, _msg, host, status, context.attempt, range_key, position",
                16,
                None,
            ),
            (
                "last_wide",
                "logs-last-wide",
                "query | last 8 by (context.attempt, range_key) partition by (service, level) rank as position | fields _time, _msg, service, level, status, context.attempt, range_key, position",
                64,
                None,
            ),
            (
                "top_narrow",
                "logs-top-narrow",
                "host:=\"h00\" AND query | top 5 by (context.attempt) hits as hits rank as position",
                5,
                None,
            ),
            (
                "top_wide",
                "logs-top-wide",
                "query | top 8 by (service, level) hits as hits rank as position",
                8,
                None,
            ),
            (
                "top_control_narrow",
                "logs-top-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 5 | fields context.attempt",
                5,
                None,
            ),
            (
                "top_control_wide",
                "logs-top-control-wide",
                "query | sort by (_time) asc | limit 8 | fields service, level",
                8,
                None,
            ),
            (
                "uniq_narrow",
                "logs-uniq-narrow",
                "host:=\"h00\" AND query | uniq by (context.attempt) with hits",
                5,
                None,
            ),
            (
                "uniq_wide",
                "logs-uniq-wide",
                "query | uniq by (service, level) with hits",
                8,
                None,
            ),
            (
                "uniq_control_narrow",
                "logs-uniq-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 5 | fields context.attempt",
                5,
                None,
            ),
            (
                "uniq_control_wide",
                "logs-uniq-control-wide",
                "query | sort by (_time) asc | limit 8 | fields service, level",
                8,
                None,
            ),
            (
                "facets_narrow",
                "logs-facets-narrow",
                "host:=\"h00\" AND query | fields context.attempt, context.retry, status | facets",
                7,
                None,
            ),
            (
                "facets_wide",
                "logs-facets-wide",
                "query | fields service, level, status, context.attempt, context.retry | facets",
                19,
                None,
            ),
            (
                "facets_control_narrow",
                "logs-facets-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 7 | fields context.attempt, context.retry, status",
                7,
                None,
            ),
            (
                "facets_control_wide",
                "logs-facets-control-wide",
                "query | sort by (_time) asc | limit 19 | fields service, level, status, context.attempt, context.retry",
                19,
                None,
            ),
            (
                "coalesce_narrow",
                "logs-coalesce-narrow",
                "host:=\"h00\" AND query | fields context, status | coalesce(context.*, status) as selected | limit 64 | fields selected",
                64,
                None,
            ),
            (
                "coalesce_wide",
                "logs-coalesce-wide",
                "query | fields context, status | coalesce(context.*, status) as selected | limit 64 | fields selected",
                64,
                None,
            ),
            (
                "coalesce_control_narrow",
                "logs-coalesce-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields status",
                64,
                None,
            ),
            (
                "coalesce_control_wide",
                "logs-coalesce-control-wide",
                "query | sort by (_time) asc | limit 64 | fields status",
                64,
                None,
            ),
            (
                "copy_narrow",
                "logs-copy-narrow",
                "host:=\"h00\" AND query | fields context, status | copy context.* as copied.* | limit 64 | fields copied",
                64,
                None,
            ),
            (
                "copy_wide",
                "logs-copy-wide",
                "query | fields context, status | copy context.* as copied.* | limit 64 | fields copied",
                64,
                None,
            ),
            (
                "copy_control_narrow",
                "logs-copy-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields context",
                64,
                None,
            ),
            (
                "copy_control_wide",
                "logs-copy-control-wide",
                "query | sort by (_time) asc | limit 64 | fields context",
                64,
                None,
            ),
            (
                "rename_narrow",
                "logs-rename-narrow",
                "host:=\"h00\" AND query | fields context, status | rename context.* as moved.* | limit 64 | fields moved",
                64,
                None,
            ),
            (
                "rename_wide",
                "logs-rename-wide",
                "query | fields context, status | rename context.* as moved.* | limit 64 | fields moved",
                64,
                None,
            ),
            (
                "rename_control_narrow",
                "logs-rename-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields context",
                64,
                None,
            ),
            (
                "rename_control_wide",
                "logs-rename-control-wide",
                "query | sort by (_time) asc | limit 64 | fields context",
                64,
                None,
            ),
            (
                "format_narrow",
                "logs-format-narrow",
                "host:=\"h00\" AND query | fields context, status | format '<context.attempt>|<context.retry>|<status>' as rendered | limit 64 | fields rendered",
                64,
                None,
            ),
            (
                "format_wide",
                "logs-format-wide",
                "query | fields context, status | format '<context.attempt>|<context.retry>|<status>' as rendered | limit 64 | fields rendered",
                64,
                None,
            ),
            (
                "format_control_narrow",
                "logs-format-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "format_control_wide",
                "logs-format-control-wide",
                "query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "math_narrow",
                "logs-math-narrow",
                "host:=\"h00\" AND query | fields context, status | math context.attempt * 2 + status as computed | limit 64 | fields computed",
                64,
                None,
            ),
            (
                "math_wide",
                "logs-math-wide",
                "query | fields context, status | math context.attempt * 2 + status as computed | limit 64 | fields computed",
                64,
                None,
            ),
            (
                "math_control_narrow",
                "logs-math-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields status",
                64,
                None,
            ),
            (
                "math_control_wide",
                "logs-math-control-wide",
                "query | sort by (_time) asc | limit 64 | fields status",
                64,
                None,
            ),
            (
                "len_narrow",
                "logs-len-narrow",
                "host:=\"h00\" AND query | fields context, status | len(context.attempt) as computed | limit 64 | fields computed",
                64,
                None,
            ),
            (
                "len_wide",
                "logs-len-wide",
                "query | fields context, status | len(context.attempt) as computed | limit 64 | fields computed",
                64,
                None,
            ),
            (
                "len_control_narrow",
                "logs-len-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields status",
                64,
                None,
            ),
            (
                "len_control_wide",
                "logs-len-control-wide",
                "query | sort by (_time) asc | limit 64 | fields status",
                64,
                None,
            ),
            (
                "hash_narrow",
                "logs-hash-narrow",
                "host:=\"h00\" AND query | fields range_key | hash(range_key) as computed | limit 64 | fields computed",
                64,
                None,
            ),
            (
                "hash_wide",
                "logs-hash-wide",
                "query | fields range_key | hash(range_key) as computed | limit 64 | fields computed",
                64,
                None,
            ),
            (
                "hash_control_narrow",
                "logs-hash-control-narrow",
                "host:=\"h00\" AND query | fields range_key | copy range_key as computed | limit 64 | fields computed",
                64,
                None,
            ),
            (
                "hash_control_wide",
                "logs-hash-control-wide",
                "query | fields range_key | copy range_key as computed | limit 64 | fields computed",
                64,
                None,
            ),
            (
                "collapse_nums_narrow",
                "logs-collapse-nums-narrow",
                "host:=\"h00\" AND query | fields range_key | collapse_nums at range_key | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "collapse_nums_wide",
                "logs-collapse-nums-wide",
                "query | fields range_key | collapse_nums at range_key | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "collapse_nums_control_narrow",
                "logs-collapse-nums-control-narrow",
                "host:=\"h00\" AND query | fields range_key | format 'key-&lt;N&gt;' as range_key | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "collapse_nums_control_wide",
                "logs-collapse-nums-control-wide",
                "query | fields range_key | format 'key-&lt;N&gt;' as range_key | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "decolorize_narrow",
                "logs-decolorize-narrow",
                r#"host:="h00" AND query | fields range_key | format '\x1b[31m<range_key>\x1b[0m' as rendered | decolorize rendered | limit 64 | fields rendered"#,
                64,
                None,
            ),
            (
                "decolorize_wide",
                "logs-decolorize-wide",
                r#"query | fields range_key | format '\x1b[31m<range_key>\x1b[0m' as rendered | decolorize rendered | limit 64 | fields rendered"#,
                64,
                None,
            ),
            (
                "decolorize_control_narrow",
                "logs-decolorize-control-narrow",
                "host:=\"h00\" AND query | fields range_key | format '<range_key>' as rendered | limit 64 | fields rendered",
                64,
                None,
            ),
            (
                "decolorize_control_wide",
                "logs-decolorize-control-wide",
                "query | fields range_key | format '<range_key>' as rendered | limit 64 | fields rendered",
                64,
                None,
            ),
            (
                "split_narrow",
                "logs-split-narrow",
                r#"host:="h00" AND query | fields range_key | split "-" range_key parts | limit 64 | fields parts"#,
                64,
                None,
            ),
            (
                "split_wide",
                "logs-split-wide",
                r#"query | fields range_key | split "-" range_key parts | limit 64 | fields parts"#,
                64,
                None,
            ),
            (
                "split_control_narrow",
                "logs-split-control-narrow",
                r#"host:="h00" AND query | fields range_key | extract 'key-<suffix>' from range_key | format '["key","<suffix>"]' as parts | limit 64 | fields parts"#,
                64,
                None,
            ),
            (
                "split_control_wide",
                "logs-split-control-wide",
                r#"query | fields range_key | extract 'key-<suffix>' from range_key | format '["key","<suffix>"]' as parts | limit 64 | fields parts"#,
                64,
                None,
            ),
            (
                "drop_empty_fields_narrow",
                "logs-drop-empty-fields-narrow",
                "host:=\"h00\" AND query | fields context, status | drop_empty_fields | limit 64 | fields context, status",
                64,
                None,
            ),
            (
                "drop_empty_fields_wide",
                "logs-drop-empty-fields-wide",
                "query | fields context, status | drop_empty_fields | limit 64 | fields context, status",
                64,
                None,
            ),
            (
                "drop_empty_fields_control_narrow",
                "logs-drop-empty-fields-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields context, status",
                64,
                None,
            ),
            (
                "drop_empty_fields_control_wide",
                "logs-drop-empty-fields-control-wide",
                "query | sort by (_time) asc | limit 64 | fields context, status",
                64,
                None,
            ),
            (
                "replace_narrow",
                "logs-replace-narrow",
                "host:=\"h00\" AND query | fields range_key | replace (key, log) at range_key | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "replace_wide",
                "logs-replace-wide",
                "query | fields range_key | replace (key, log) at range_key | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "replace_control_narrow",
                "logs-replace-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "replace_control_wide",
                "logs-replace-control-wide",
                "query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "replace_regexp_narrow",
                "logs-replace-regexp-narrow",
                r#"host:="h00" AND query | fields range_key | replace_regexp ("^key-([0-9a-f]+)$", "log-$1") at range_key | limit 64 | fields range_key"#,
                64,
                None,
            ),
            (
                "replace_regexp_wide",
                "logs-replace-regexp-wide",
                r#"query | fields range_key | replace_regexp ("^key-([0-9a-f]+)$", "log-$1") at range_key | limit 64 | fields range_key"#,
                64,
                None,
            ),
            (
                "replace_regexp_control_narrow",
                "logs-replace-regexp-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "replace_regexp_control_wide",
                "logs-replace-regexp-control-wide",
                "query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "extract_narrow",
                "logs-extract-narrow",
                r#"host:="h00" AND query | fields range_key | extract "key-<extracted_key>" from range_key | limit 64 | fields extracted_key"#,
                64,
                None,
            ),
            (
                "extract_wide",
                "logs-extract-wide",
                r#"query | fields range_key | extract "key-<extracted_key>" from range_key | limit 64 | fields extracted_key"#,
                64,
                None,
            ),
            (
                "extract_control_narrow",
                "logs-extract-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "extract_control_wide",
                "logs-extract-control-wide",
                "query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "extract_regexp_narrow",
                "logs-extract-regexp-narrow",
                r#"host:="h00" AND query | fields range_key | extract_regexp "^key-(?P<extracted_key>[0-9a-f]+)$" from range_key | limit 64 | fields extracted_key"#,
                64,
                None,
            ),
            (
                "extract_regexp_wide",
                "logs-extract-regexp-wide",
                r#"query | fields range_key | extract_regexp "^key-(?P<extracted_key>[0-9a-f]+)$" from range_key | limit 64 | fields extracted_key"#,
                64,
                None,
            ),
            (
                "extract_regexp_control_narrow",
                "logs-extract-regexp-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "extract_regexp_control_wide",
                "logs-extract-regexp-control-wide",
                "query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "pack_json_narrow",
                "logs-pack-json-narrow",
                r#"host:="h00" AND query | fields range_key | pack_json fields (range_key) as packed | limit 64 | fields packed"#,
                64,
                None,
            ),
            (
                "pack_json_wide",
                "logs-pack-json-wide",
                r#"query | fields range_key | pack_json fields (range_key) as packed | limit 64 | fields packed"#,
                64,
                None,
            ),
            (
                "pack_json_control_narrow",
                "logs-pack-json-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "pack_json_control_wide",
                "logs-pack-json-control-wide",
                "query | sort by (_time) asc | limit 64 | fields range_key",
                64,
                None,
            ),
            (
                "pack_logfmt_narrow",
                "logs-pack-logfmt-narrow",
                r#"host:="h00" AND query | fields range_key | pack_logfmt fields (range_key) as packed | limit 64 | fields packed"#,
                64,
                None,
            ),
            (
                "pack_logfmt_wide",
                "logs-pack-logfmt-wide",
                r#"query | fields range_key | pack_logfmt fields (range_key) as packed | limit 64 | fields packed"#,
                64,
                None,
            ),
            (
                "pack_logfmt_control_narrow",
                "logs-pack-logfmt-control-narrow",
                r#"host:="h00" AND query | fields range_key | format 'range_key=<range_key>' as packed | limit 64 | fields packed"#,
                64,
                None,
            ),
            (
                "pack_logfmt_control_wide",
                "logs-pack-logfmt-control-wide",
                r#"query | fields range_key | format 'range_key=<range_key>' as packed | limit 64 | fields packed"#,
                64,
                None,
            ),
            (
                "unpack_logfmt_narrow",
                "logs-unpack-logfmt-narrow",
                r#"host:="h00" AND query | fields range_key | pack_logfmt fields (range_key) as packed | unpack_logfmt from packed fields (range_key) result_prefix decoded_ | limit 64 | fields decoded_range_key"#,
                64,
                None,
            ),
            (
                "unpack_logfmt_wide",
                "logs-unpack-logfmt-wide",
                r#"query | fields range_key | pack_logfmt fields (range_key) as packed | unpack_logfmt from packed fields (range_key) result_prefix decoded_ | limit 64 | fields decoded_range_key"#,
                64,
                None,
            ),
            (
                "unpack_logfmt_control_narrow",
                "logs-unpack-logfmt-control-narrow",
                r#"host:="h00" AND query | fields range_key | pack_logfmt fields (range_key) as packed | copy range_key as decoded_range_key | limit 64 | fields decoded_range_key"#,
                64,
                None,
            ),
            (
                "unpack_logfmt_control_wide",
                "logs-unpack-logfmt-control-wide",
                r#"query | fields range_key | pack_logfmt fields (range_key) as packed | copy range_key as decoded_range_key | limit 64 | fields decoded_range_key"#,
                64,
                None,
            ),
            (
                "unpack_syslog_narrow",
                "logs-unpack-syslog-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key | format '1 2023-06-03T17:42:00Z bench-host bench-app 1 ID47 - <range_key>' as packed | unpack_syslog from packed result_prefix decoded_ | fields decoded_message"#,
                64,
                None,
            ),
            (
                "unpack_syslog_wide",
                "logs-unpack-syslog-wide",
                r#"query | sort by (_time) asc | limit 64 | fields range_key | format '1 2023-06-03T17:42:00Z bench-host bench-app 1 ID47 - <range_key>' as packed | unpack_syslog from packed result_prefix decoded_ | fields decoded_message"#,
                64,
                None,
            ),
            (
                "unpack_syslog_control_narrow",
                "logs-unpack-syslog-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key | format '1 2023-06-03T17:42:00Z bench-host bench-app 1 ID47 - <range_key>' as packed | copy range_key as decoded_message | fields decoded_message"#,
                64,
                None,
            ),
            (
                "unpack_syslog_control_wide",
                "logs-unpack-syslog-control-wide",
                r#"query | sort by (_time) asc | limit 64 | fields range_key | format '1 2023-06-03T17:42:00Z bench-host bench-app 1 ID47 - <range_key>' as packed | copy range_key as decoded_message | fields decoded_message"#,
                64,
                None,
            ),
            (
                "unpack_words_narrow",
                "logs-unpack-words-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key | unpack_words range_key words drop_duplicates | fields words"#,
                64,
                None,
            ),
            (
                "unpack_words_wide",
                "logs-unpack-words-wide",
                r#"query | sort by (_time) asc | limit 64 | fields range_key | unpack_words range_key words drop_duplicates | fields words"#,
                64,
                None,
            ),
            (
                "unpack_words_control_narrow",
                "logs-unpack-words-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key | copy range_key as words | fields words"#,
                64,
                None,
            ),
            (
                "unpack_words_control_wide",
                "logs-unpack-words-control-wide",
                r#"query | sort by (_time) asc | limit 64 | fields range_key | copy range_key as words | fields words"#,
                64,
                None,
            ),
            (
                "json_array_concat_narrow",
                "logs-json-array-concat-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields tags | json_array_concat "," tags joined | fields joined"#,
                64,
                None,
            ),
            (
                "json_array_concat_wide",
                "logs-json-array-concat-wide",
                r#"query | sort by (_time) asc | limit 64 | fields tags | json_array_concat "," tags joined | fields joined"#,
                64,
                None,
            ),
            (
                "json_array_concat_control_narrow",
                "logs-json-array-concat-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields tags | format 'query,true' as joined | fields joined"#,
                64,
                None,
            ),
            (
                "json_array_concat_control_wide",
                "logs-json-array-concat-control-wide",
                r#"query | sort by (_time) asc | limit 64 | fields tags | format 'query,true' as joined | fields joined"#,
                64,
                None,
            ),
            (
                "unroll_narrow",
                "logs-unroll-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields tags | unroll tags | fields tags"#,
                128,
                None,
            ),
            (
                "unroll_wide",
                "logs-unroll-wide",
                r#"query | sort by (_time) asc | limit 64 | fields tags | unroll tags | fields tags"#,
                128,
                None,
            ),
            (
                "unroll_control_narrow",
                "logs-unroll-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields tags | json_array_concat "," tags | fields tags"#,
                64,
                None,
            ),
            (
                "unroll_control_wide",
                "logs-unroll-control-wide",
                r#"query | sort by (_time) asc | limit 64 | fields tags | json_array_concat "," tags | fields tags"#,
                64,
                None,
            ),
            (
                "join_narrow",
                "logs-join-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key | join by (range_key) (host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key | format 'joined' as joined) | fields joined"#,
                64,
                None,
            ),
            (
                "join_wide",
                "logs-join-wide",
                r#"query | sort by (_time) asc | limit 64 | fields range_key | join by (range_key) (query | sort by (_time) asc | limit 64 | fields range_key | format 'joined' as joined) | fields joined"#,
                64,
                None,
            ),
            (
                "join_control_narrow",
                "logs-join-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key | filter range_key:in(host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key) | format 'joined' as joined | fields joined"#,
                64,
                None,
            ),
            (
                "join_control_wide",
                "logs-join-control-wide",
                r#"query | sort by (_time) asc | limit 64 | fields range_key | filter range_key:in(query | sort by (_time) asc | limit 64 | fields range_key) | format 'joined' as joined | fields joined"#,
                64,
                None,
            ),
            (
                "union_narrow",
                "logs-union-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key | format 'joined' as joined | fields joined | union (host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key | format 'joined' as joined | fields joined)"#,
                128,
                None,
            ),
            (
                "union_wide",
                "logs-union-wide",
                r#"query | sort by (_time) asc | limit 64 | fields range_key | format 'joined' as joined | fields joined | union (query | sort by (_time) asc | limit 64 | fields range_key | format 'joined' as joined | fields joined)"#,
                128,
                None,
            ),
            (
                "union_control_narrow",
                "logs-union-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key | filter range_key:in(host:="h00" AND query | sort by (_time) asc | limit 64 | fields range_key) | format '["joined","joined"]' as joined | unroll joined | fields joined"#,
                128,
                None,
            ),
            (
                "union_control_wide",
                "logs-union-control-wide",
                r#"query | sort by (_time) asc | limit 64 | fields range_key | filter range_key:in(query | sort by (_time) asc | limit 64 | fields range_key) | format '["joined","joined"]' as joined | unroll joined | fields joined"#,
                128,
                None,
            ),
            (
                "running_stats_narrow",
                "logs-running-stats-narrow",
                r#"host:="h00" AND query | running_stats by (service, level) count() as running | limit 64 | fields running"#,
                64,
                None,
            ),
            (
                "running_stats_wide",
                "logs-running-stats-wide",
                r#"query | running_stats by (service, level) count() as running | limit 64 | fields running"#,
                64,
                None,
            ),
            (
                "running_stats_control_narrow",
                "logs-running-stats-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | math 1 as running | fields running"#,
                64,
                None,
            ),
            (
                "running_stats_control_wide",
                "logs-running-stats-control-wide",
                r#"query | sort by (_time) asc | limit 64 | math 1 as running | fields running"#,
                64,
                None,
            ),
            (
                "total_stats_narrow",
                "logs-total-stats-narrow",
                r#"host:="h00" AND query | total_stats by (service, level) count() as total | limit 64 | fields total"#,
                64,
                None,
            ),
            (
                "total_stats_wide",
                "logs-total-stats-wide",
                r#"query | total_stats by (service, level) count() as total | limit 64 | fields total"#,
                64,
                None,
            ),
            (
                "total_stats_control_narrow",
                "logs-total-stats-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | math 128 as total | fields total"#,
                64,
                None,
            ),
            (
                "total_stats_control_wide",
                "logs-total-stats-control-wide",
                r#"query | sort by (_time) asc | limit 64 | math 1024 as total | fields total"#,
                64,
                None,
            ),
            (
                "time_add_narrow",
                "logs-time-add-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | time_add 1s | fields _time"#,
                64,
                None,
            ),
            (
                "time_add_wide",
                "logs-time-add-wide",
                r#"query | sort by (_time) asc | limit 64 | time_add 1s | fields _time"#,
                64,
                None,
            ),
            (
                "time_add_control_narrow",
                "logs-time-add-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields _time"#,
                64,
                None,
            ),
            (
                "time_add_control_wide",
                "logs-time-add-control-wide",
                r#"query | sort by (_time) asc | limit 64 | fields _time"#,
                64,
                None,
            ),
            (
                "time_offset_narrow",
                "logs-time-offset-narrow",
                r#"options(time_offset=1s) host:="h00" AND query | sort by (_time) asc | limit 64 | fields _time"#,
                64,
                None,
            ),
            (
                "time_offset_wide",
                "logs-time-offset-wide",
                r#"options(time_offset=1s) query | sort by (_time) asc | limit 64 | fields _time"#,
                64,
                None,
            ),
            (
                "time_offset_control_narrow",
                "logs-time-offset-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | fields _time"#,
                64,
                None,
            ),
            (
                "time_offset_control_wide",
                "logs-time-offset-control-wide",
                r#"query | sort by (_time) asc | limit 64 | fields _time"#,
                64,
                None,
            ),
            (
                "set_stream_fields_narrow",
                "logs-set-stream-fields-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | set_stream_fields host | fields _stream"#,
                64,
                None,
            ),
            (
                "set_stream_fields_wide",
                "logs-set-stream-fields-wide",
                r#"query | sort by (_time) asc | limit 64 | set_stream_fields host | fields _stream"#,
                64,
                None,
            ),
            (
                "set_stream_fields_control_narrow",
                "logs-set-stream-fields-control-narrow",
                r#"host:="h00" AND query | sort by (_time) asc | limit 64 | format '{host="<host>"}' as _stream | fields _stream"#,
                64,
                None,
            ),
            (
                "set_stream_fields_control_wide",
                "logs-set-stream-fields-control-wide",
                r#"query | sort by (_time) asc | limit 64 | format '{host="<host>"}' as _stream | fields _stream"#,
                64,
                None,
            ),
            (
                "generate_sequence_narrow",
                "logs-generate-sequence-narrow",
                r#"host:="h00" AND query | generate_sequence 64"#,
                64,
                None,
            ),
            (
                "generate_sequence_wide",
                "logs-generate-sequence-wide",
                r#"query | generate_sequence 64"#,
                64,
                None,
            ),
            (
                "json_values_narrow",
                "logs-json-values-narrow",
                r#"host:="h00" AND query | stats json_values(range_key, context.attempt) sort by (range_key desc) limit 64 as values"#,
                1,
                None,
            ),
            (
                "json_values_wide",
                "logs-json-values-wide",
                r#"query | stats json_values(range_key, context.attempt) sort by (range_key desc) limit 64 as values"#,
                1,
                None,
            ),
            (
                "json_values_control_narrow",
                "logs-json-values-control-narrow",
                r#"host:="h00" AND query | stats values(range_key, context.attempt) limit 64 as values"#,
                1,
                None,
            ),
            (
                "json_values_control_wide",
                "logs-json-values-control-wide",
                r#"query | stats values(range_key, context.attempt) limit 64 as values"#,
                1,
                None,
            ),
            (
                "histogram_narrow",
                "logs-histogram-narrow",
                r#"host:="h00" AND query | stats histogram(context.attempt) as buckets"#,
                1,
                None,
            ),
            (
                "histogram_wide",
                "logs-histogram-wide",
                r#"query | stats histogram(context.attempt) as buckets"#,
                1,
                None,
            ),
            (
                "histogram_control_narrow",
                "logs-histogram-control-narrow",
                r#"host:="h00" AND query | stats values(context.attempt) limit 64 as buckets"#,
                1,
                None,
            ),
            (
                "histogram_control_wide",
                "logs-histogram-control-wide",
                r#"query | stats values(context.attempt) limit 64 as buckets"#,
                1,
                None,
            ),
            (
                "unpack_json_narrow",
                "logs-unpack-json-narrow",
                r#"host:="h00" AND query | fields range_key | pack_json fields (range_key) as packed | unpack_json from packed fields (range_key) result_prefix decoded_ | limit 64 | fields decoded_range_key"#,
                64,
                None,
            ),
            (
                "unpack_json_wide",
                "logs-unpack-json-wide",
                r#"query | fields range_key | pack_json fields (range_key) as packed | unpack_json from packed fields (range_key) result_prefix decoded_ | limit 64 | fields decoded_range_key"#,
                64,
                None,
            ),
            (
                "unpack_json_control_narrow",
                "logs-unpack-json-control-narrow",
                r#"host:="h00" AND query | fields range_key | pack_json fields (range_key) as packed | copy range_key as decoded_range_key | limit 64 | fields decoded_range_key"#,
                64,
                None,
            ),
            (
                "unpack_json_control_wide",
                "logs-unpack-json-control-wide",
                r#"query | fields range_key | pack_json fields (range_key) as packed | copy range_key as decoded_range_key | limit 64 | fields decoded_range_key"#,
                64,
                None,
            ),
            (
                "json_array_len_narrow",
                "logs-json-array-len-narrow",
                r#"host:="h00" AND query | json_array_len(tags) as array_length | limit 64 | fields array_length"#,
                64,
                None,
            ),
            (
                "json_array_len_wide",
                "logs-json-array-len-wide",
                r#"query | json_array_len(tags) as array_length | limit 64 | fields array_length"#,
                64,
                None,
            ),
            (
                "json_array_len_control_narrow",
                "logs-json-array-len-control-narrow",
                r#"host:="h00" AND query | format '2' as array_length | limit 64 | fields array_length"#,
                64,
                None,
            ),
            (
                "json_array_len_control_wide",
                "logs-json-array-len-control-wide",
                r#"query | format '2' as array_length | limit 64 | fields array_length"#,
                64,
                None,
            ),
            (
                "first_control_narrow",
                "logs-first-control-narrow",
                "host:=\"h00\" AND query | sort by (_time) asc | limit 16 | fields _time, _msg, host, status, context.attempt, range_key",
                16,
                None,
            ),
            (
                "first_control_wide",
                "logs-first-control-wide",
                "query | sort by (_time) asc | limit 64 | fields _time, _msg, service, level, status, context.attempt, range_key",
                64,
                None,
            ),
            (
                "empty_narrow",
                "logs-empty-narrow",
                "host:=\"h00\" AND optional:(\"\") | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "empty_wide",
                "logs-empty-wide",
                "optional:(\"\") | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "any_narrow",
                "logs-any-narrow",
                "host:=\"h00\" AND context.retry:* | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "any_wide",
                "logs-any-wide",
                "context.retry:* | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "numeric_narrow",
                "logs-numeric-narrow",
                "host:=\"h00\" AND context.attempt:range[2,4) | sort by (_time) asc | limit 10000",
                host_numeric_matches,
                None,
            ),
            (
                "numeric_wide",
                "logs-numeric-wide",
                "context.attempt:range[2,4) | sort by (_time) asc | limit 10000",
                numeric_matches,
                None,
            ),
            (
                "value_type_narrow",
                "logs-value-type-narrow",
                "host:=\"h00\" AND context.attempt:value_type(uint64) | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "value_type_wide",
                "logs-value-type-wide",
                "context.attempt:value_type(uint64) | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "logical_narrow",
                "logs-logical-narrow",
                "host:=\"h00\" AND (query OR =\"never\") | sort by (_time) asc | limit 10000",
                host_matches,
                None,
            ),
            (
                "logical_wide",
                "logs-logical-wide",
                "query AND (contract OR =\"never\") | sort by (_time) asc | limit 10000",
                entries,
                None,
            ),
            (
                "field_values_narrow",
                "logs-field-values-narrow",
                "host:=\"h00\" | field_values context.attempt",
                5,
                None,
            ),
            (
                "field_values_wide",
                "logs-field-values-wide",
                "* | field_values context.attempt",
                5,
                None,
            ),
            (
                "field_names_narrow",
                "logs-field-names-narrow",
                "host:=\"h00\" | field_names",
                11,
                None,
            ),
            (
                "field_names_wide",
                "logs-field-names-wide",
                "* | field_names",
                11,
                None,
            ),
            (
                "projection_narrow",
                "logs-projection-narrow",
                "host:=\"h00\" | fields _time, _msg, context.attempt | limit 10000",
                host_matches,
                None,
            ),
            (
                "projection_wide",
                "logs-projection-wide",
                "* | fields _time, _msg, context.attempt | limit 10000",
                entries,
                None,
            ),
            (
                "pipeline_filter_narrow",
                "logs-pipeline-filter-narrow",
                "host:=\"h00\" | fields _msg, context | filter context.attempt:range[2,4) | limit 10000",
                host_numeric_matches,
                None,
            ),
            (
                "pipeline_filter_wide",
                "logs-pipeline-filter-wide",
                "* | fields _msg, context | filter context.attempt:range[2,4) | limit 10000",
                numeric_matches,
                None,
            ),
            (
                "field_stats_narrow",
                "logs-field-stats-narrow",
                "host:=\"h00\" | stats count(context.attempt) as present, count_empty(optional) as empty",
                1,
                None,
            ),
            (
                "field_stats_wide",
                "logs-field-stats-wide",
                "* | stats count(context.attempt) as present, count_empty(optional) as empty",
                1,
                None,
            ),
            (
                "unique_stats_narrow",
                "logs-unique-stats-narrow",
                "host:=\"h00\" | stats count_uniq(context.attempt) as exact, count_uniq_hash(context.attempt) as hashed",
                1,
                None,
            ),
            (
                "unique_stats_wide",
                "logs-unique-stats-wide",
                "* | stats count_uniq(context.attempt) as exact, count_uniq_hash(context.attempt) as hashed",
                1,
                None,
            ),
            (
                "value_stats_narrow",
                "logs-value-stats-narrow",
                "host:=\"h00\" | stats uniq_values(context.attempt) as unique, values(context.attempt) limit 10000 as values",
                1,
                None,
            ),
            (
                "value_stats_wide",
                "logs-value-stats-wide",
                "* | stats uniq_values(context.attempt) as unique, values(context.attempt) limit 10000 as values",
                1,
                None,
            ),
            (
                "numeric_stats_narrow",
                "logs-numeric-stats-narrow",
                "host:=\"h00\" | stats sum(context.attempt) as sum, avg(context.attempt) as avg, min(context.attempt) as min, max(context.attempt) as max, median(context.attempt) as median",
                1,
                None,
            ),
            (
                "numeric_stats_wide",
                "logs-numeric-stats-wide",
                "* | stats sum(context.attempt) as sum, avg(context.attempt) as avg, min(context.attempt) as min, max(context.attempt) as max, median(context.attempt) as median",
                1,
                None,
            ),
            (
                "quantile_narrow",
                "logs-quantile-narrow",
                "host:=\"h00\" | stats quantile(0.5, context.attempt) as value",
                1,
                None,
            ),
            (
                "quantile_wide",
                "logs-quantile-wide",
                "* | stats quantile(0.5, context.attempt) as value",
                1,
                None,
            ),
            (
                "quantile_control_narrow",
                "logs-quantile-control-narrow",
                "host:=\"h00\" | stats median(context.attempt) as value",
                1,
                None,
            ),
            (
                "quantile_control_wide",
                "logs-quantile-control-wide",
                "* | stats median(context.attempt) as value",
                1,
                None,
            ),
            (
                "stddev_narrow",
                "logs-stddev-narrow",
                "host:=\"h00\" | stats stddev(context.attempt) as value",
                1,
                None,
            ),
            (
                "stddev_wide",
                "logs-stddev-wide",
                "* | stats stddev(context.attempt) as value",
                1,
                None,
            ),
            (
                "stddev_control_narrow",
                "logs-stddev-control-narrow",
                "host:=\"h00\" | stats avg(context.attempt) as value",
                1,
                None,
            ),
            (
                "stddev_control_wide",
                "logs-stddev-control-wide",
                "* | stats avg(context.attempt) as value",
                1,
                None,
            ),
            (
                "sum_len_narrow",
                "logs-sum-len-narrow",
                "host:=\"h00\" | stats sum_len(context.attempt) as value",
                1,
                None,
            ),
            (
                "sum_len_wide",
                "logs-sum-len-wide",
                "* | stats sum_len(context.attempt) as value",
                1,
                None,
            ),
            (
                "sum_len_control_narrow",
                "logs-sum-len-control-narrow",
                "host:=\"h00\" | stats sum(context.attempt) as value",
                1,
                None,
            ),
            (
                "sum_len_control_wide",
                "logs-sum-len-control-wide",
                "* | stats sum(context.attempt) as value",
                1,
                None,
            ),
            (
                "any_stat_narrow",
                "logs-any-stat-narrow",
                "host:=\"h00\" | stats any(context.attempt) as value",
                1,
                None,
            ),
            (
                "any_stat_wide",
                "logs-any-stat-wide",
                "* | stats any(context.attempt) as value",
                1,
                None,
            ),
            (
                "any_stat_control_narrow",
                "logs-any-stat-control-narrow",
                "host:=\"h00\" | stats min(context.attempt) as value",
                1,
                None,
            ),
            (
                "any_stat_control_wide",
                "logs-any-stat-control-wide",
                "* | stats min(context.attempt) as value",
                1,
                None,
            ),
            (
                "field_extrema_narrow",
                "logs-field-extrema-narrow",
                "host:=\"h00\" | stats field_min(context.attempt, context.attempt) as minimum, field_max(context.attempt, context.attempt) as maximum",
                1,
                None,
            ),
            (
                "field_extrema_wide",
                "logs-field-extrema-wide",
                "* | stats field_min(context.attempt, context.attempt) as minimum, field_max(context.attempt, context.attempt) as maximum",
                1,
                None,
            ),
            (
                "field_extrema_control_narrow",
                "logs-field-extrema-control-narrow",
                "host:=\"h00\" | stats min(context.attempt) as minimum, max(context.attempt) as maximum",
                1,
                None,
            ),
            (
                "field_extrema_control_wide",
                "logs-field-extrema-control-wide",
                "* | stats min(context.attempt) as minimum, max(context.attempt) as maximum",
                1,
                None,
            ),
            (
                "row_any_narrow",
                "logs-row-any-narrow",
                "host:=\"h00\" | stats row_any(context.attempt) as value",
                1,
                None,
            ),
            (
                "row_any_wide",
                "logs-row-any-wide",
                "* | stats row_any(context.attempt) as value",
                1,
                None,
            ),
            (
                "row_any_control_narrow",
                "logs-row-any-control-narrow",
                "host:=\"h00\" | stats any(context.attempt) as value",
                1,
                None,
            ),
            (
                "row_any_control_wide",
                "logs-row-any-control-wide",
                "* | stats any(context.attempt) as value",
                1,
                None,
            ),
            (
                "row_extrema_narrow",
                "logs-row-extrema-narrow",
                "host:=\"h00\" | stats row_min(context.attempt, context.attempt, context.retry) as minimum, row_max(context.attempt, context.attempt, context.retry) as maximum",
                1,
                None,
            ),
            (
                "row_extrema_wide",
                "logs-row-extrema-wide",
                "* | stats row_min(context.attempt, context.attempt, context.retry) as minimum, row_max(context.attempt, context.attempt, context.retry) as maximum",
                1,
                None,
            ),
            (
                "row_extrema_control_narrow",
                "logs-row-extrema-control-narrow",
                "host:=\"h00\" | stats field_min(context.attempt, context.attempt) as minimum, field_max(context.attempt, context.attempt) as maximum",
                1,
                None,
            ),
            (
                "row_extrema_control_wide",
                "logs-row-extrema-control-wide",
                "* | stats field_min(context.attempt, context.attempt) as minimum, field_max(context.attempt, context.attempt) as maximum",
                1,
                None,
            ),
            (
                "rate_narrow",
                "logs-rate-narrow",
                "host:=\"h00\" _time:[1800000000000000,1800000001000000) | stats rate() as rate, rate_sum(context.attempt) as rate_sum",
                1,
                None,
            ),
            (
                "rate_wide",
                "logs-rate-wide",
                "_time:[1800000000000000,1800000001000000) | stats rate() as rate, rate_sum(context.attempt) as rate_sum",
                1,
                None,
            ),
        ] {
            let mut request = || {
                let body = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("query", expression)
                    .finish();
                let (status, response) = request_bytes(
                    context.client,
                    reqwest::Method::POST,
                    &format!("{}/select/logsql/query", server.base),
                    Some((body.as_bytes(), "application/x-www-form-urlencoded")),
                )?;
                if status != 200 {
                    bail!(
                        "LogsQL returned {status}: {}",
                        String::from_utf8_lossy(&response)
                    );
                }
                if let Some(expected_total) = expected_total {
                    let row: Value = serde_json::from_slice(
                        response
                            .split(|byte| *byte == b'\n')
                            .find(|line| !line.is_empty())
                            .context("LogsQL count response is empty")?,
                    )?;
                    if row != json!({"total": expected_total}) {
                        bail!(
                            "LogsQL numeric count mismatch: expected {expected_total}, got {row}"
                        );
                    }
                }
                Ok(response)
            };
            let mut stat = || stats(context.client, &server.base, "/select/logsql/stats");
            let measured = measure(
                name,
                &mut request,
                Cardinality::Ndjson,
                expected,
                &mut stat,
                context.iterations,
                context.warmup,
            )
            .with_context(|| format!("measure LogsQL evidence {key} ({expression})"))?;
            queries.insert(key.to_owned(), measured);
        }
        require_same_public_query_work(&queries, "sample_control_narrow", "sample_narrow")?;
        require_same_public_query_work(&queries, "sample_control_wide", "sample_wide")?;
        require_same_public_query_work(&queries, "hash_control_narrow", "hash_narrow")?;
        require_same_public_query_work(&queries, "hash_control_wide", "hash_wide")?;
        require_same_public_query_work(
            &queries,
            "collapse_nums_control_narrow",
            "collapse_nums_narrow",
        )?;
        require_same_public_query_work(
            &queries,
            "collapse_nums_control_wide",
            "collapse_nums_wide",
        )?;
        require_same_public_query_work(&queries, "decolorize_control_narrow", "decolorize_narrow")?;
        require_same_public_query_work(&queries, "decolorize_control_wide", "decolorize_wide")?;
        require_same_public_query_work(&queries, "split_control_narrow", "split_narrow")?;
        require_same_public_query_work(&queries, "split_control_wide", "split_wide")?;
        require_same_public_query_work(
            &queries,
            "pack_logfmt_control_narrow",
            "pack_logfmt_narrow",
        )?;
        require_same_public_query_work(&queries, "pack_logfmt_control_wide", "pack_logfmt_wide")?;
        require_same_public_query_work(
            &queries,
            "unpack_logfmt_control_narrow",
            "unpack_logfmt_narrow",
        )?;
        require_same_public_query_work(
            &queries,
            "unpack_logfmt_control_wide",
            "unpack_logfmt_wide",
        )?;
        require_same_public_query_work(
            &queries,
            "unpack_syslog_control_narrow",
            "unpack_syslog_narrow",
        )?;
        require_same_public_query_work(
            &queries,
            "unpack_syslog_control_wide",
            "unpack_syslog_wide",
        )?;
        require_same_public_query_work(
            &queries,
            "unpack_words_control_narrow",
            "unpack_words_narrow",
        )?;
        require_same_public_query_work(&queries, "unpack_words_control_wide", "unpack_words_wide")?;
        require_same_public_query_work(
            &queries,
            "json_array_concat_control_narrow",
            "json_array_concat_narrow",
        )?;
        require_same_public_query_work(
            &queries,
            "json_array_concat_control_wide",
            "json_array_concat_wide",
        )?;
        require_same_public_query_work(&queries, "unroll_control_narrow", "unroll_narrow")?;
        require_same_public_query_work(&queries, "unroll_control_wide", "unroll_wide")?;
        require_same_public_query_work_with_scans(
            &queries,
            "join_control_narrow",
            "join_narrow",
            2,
            192,
        )?;
        require_same_public_query_work_with_scans(
            &queries,
            "join_control_wide",
            "join_wide",
            2,
            192,
        )?;
        require_same_public_query_work_with_scans(
            &queries,
            "union_control_narrow",
            "union_narrow",
            2,
            128,
        )?;
        require_same_public_query_work_with_scans(
            &queries,
            "union_control_wide",
            "union_wide",
            2,
            128,
        )?;
        require_same_public_query_work(
            &queries,
            "running_stats_control_narrow",
            "running_stats_narrow",
        )?;
        require_same_public_query_work(
            &queries,
            "running_stats_control_wide",
            "running_stats_wide",
        )?;
        require_same_public_query_work(
            &queries,
            "total_stats_control_narrow",
            "total_stats_narrow",
        )?;
        require_same_public_query_work(&queries, "total_stats_control_wide", "total_stats_wide")?;
        require_same_public_query_work(&queries, "time_add_control_narrow", "time_add_narrow")?;
        require_same_public_query_work(&queries, "time_add_control_wide", "time_add_wide")?;
        require_same_public_query_work(
            &queries,
            "time_offset_control_narrow",
            "time_offset_narrow",
        )?;
        require_same_public_query_work(&queries, "time_offset_control_wide", "time_offset_wide")?;
        require_same_public_query_work(
            &queries,
            "set_stream_fields_control_narrow",
            "set_stream_fields_narrow",
        )?;
        require_same_public_query_work(
            &queries,
            "set_stream_fields_control_wide",
            "set_stream_fields_wide",
        )?;
        require_no_public_query_work(
            &queries,
            &["generate_sequence_narrow", "generate_sequence_wide"],
        )?;
        require_same_public_query_work(
            &queries,
            "json_values_control_narrow",
            "json_values_narrow",
        )?;
        require_same_public_query_work(&queries, "json_values_control_wide", "json_values_wide")?;
        require_same_public_query_work(&queries, "histogram_control_narrow", "histogram_narrow")?;
        require_same_public_query_work(&queries, "histogram_control_wide", "histogram_wide")?;
        let final_stats = stats(context.client, &server.base, "/select/logsql/stats")?;
        let hwm = hwm_kib(server.pid())?;
        Ok(json!({
            "build": identity,
            "fixture": {"logical_entries": entries, "severities": severities, "typed_nested_metadata": true},
            "ingestion": {"wire_bytes": payload.len(), "admission_ns": admission_ns, "durability_barrier_ns": durable_ns, "completed_entries": after_flush["completed_entries"], "queued_entries": after_flush["queued_entries"]},
            "queries": queries,
            "storage": selected_stats(&final_stats, &["total_bytes", "disk_size", "index_size", "database_file_bytes", "database_wal_bytes", "database_shm_bytes", "physical_database_bytes", "raw_blocks", "compressed_blocks", "buffered_entries"]),
            "rss_hwm_kib": hwm,
            "limits": {"result_rows": 100_000, "work_entries": 100_000, "response_bytes": 16 * 1024 * 1024, "deadline_ms": 30_000, "contract_test": "session_ten_logsql_limits_cancel_errors_and_direct_sql_reuse_the_reader"},
            "cancellation": {"cancelled_requests": final_stats["api_query_cancelled"], "in_flight_at_capture": final_stats["api_query_in_flight"], "contract_test": "session_ten_logsql_limits_cancel_errors_and_direct_sql_reuse_the_reader"},
        }))
    })();
    let shutdown = server.shutdown(true);
    match (result, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn uname(flag: &str) -> String {
    Command::new("uname")
        .arg(flag)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned())
        .unwrap_or_default()
}

pub(crate) fn run(root: &Path, args: EvidenceArgs) -> Result<()> {
    if args.iterations == 0
        || args.metric_series == 0
        || args.metric_points == 0
        || args.log_entries == 0
    {
        bail!("workload sizes and iterations must be positive; warmup must be non-negative");
    }
    require_clean_worktree(root)?;
    let extension = fs::canonicalize(root.join(&args.extension))
        .with_context(|| format!("missing release artifact: {}", args.extension.display()))?;
    let metrics_binary = fs::canonicalize(root.join(&args.metrics_binary)).with_context(|| {
        format!(
            "missing release artifact: {}",
            args.metrics_binary.display()
        )
    })?;
    let logs_binary = fs::canonicalize(root.join(&args.logs_binary))
        .with_context(|| format!("missing release artifact: {}", args.logs_binary.display()))?;
    let expected_commit = git_commit(root)?;
    let extension_build = extension_identity(&extension, &expected_commit)?;
    let temporary = TempDir::with_prefix("timeless-query-evidence-")?;
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let metrics_context = SignalEvidence {
        root,
        extension: &extension,
        binary: &metrics_binary,
        directory: temporary.path(),
        iterations: args.iterations,
        warmup: args.warmup,
        client: &client,
    };
    let logs_context = SignalEvidence {
        binary: &logs_binary,
        ..metrics_context
    };
    let evidence = (|| {
        Ok(json!({
            "schema_version": 1,
            "captured_at": Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true),
            "git_commit": expected_commit,
            "extension_build": extension_build,
            "host": {"system": uname("-s"), "release": uname("-r"), "machine": uname("-m"), "processor": uname("-p")},
            "workload": {"iterations": args.iterations, "warmup": args.warmup, "single_client": true, "loopback_http": true, "release_build": true},
            "metrics": metrics_evidence(&metrics_context, args.metric_series, args.metric_points)?,
            "logs": logs_evidence(&logs_context, args.log_entries)?,
        }))
    })();
    let evidence = preserve_failed_evidence(temporary, evidence)?;
    let output = root.join(&args.output);
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut encoded = serde_json::to_string_pretty(&evidence)?;
    encoded.push('\n');
    fs::write(&output, encoded)?;
    println!("{}", output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_git(root: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn evidence_identity_ignores_untracked_notes_but_rejects_tracked_changes() {
        let repository = TempDir::with_prefix("timeless-evidence-git-test-").unwrap();
        run_git(repository.path(), &["init", "--quiet"]);
        fs::write(repository.path().join("tracked.txt"), "committed\n").unwrap();
        run_git(repository.path(), &["add", "tracked.txt"]);
        run_git(
            repository.path(),
            &[
                "-c",
                "user.name=Timeless test",
                "-c",
                "user.email=test@timeless.invalid",
                "commit",
                "--quiet",
                "-m",
                "fixture",
            ],
        );

        fs::write(repository.path().join("operator-notes.md"), "untracked\n").unwrap();
        require_clean_worktree(repository.path()).unwrap();

        fs::write(repository.path().join("tracked.txt"), "modified\n").unwrap();
        let error = require_clean_worktree(repository.path()).unwrap_err();
        assert!(format!("{error:#}").contains("tracked.txt"));
    }

    #[test]
    fn nearest_rank_percentiles_are_stable() {
        let values: Vec<u128> = (1..=100).collect();
        assert_eq!(percentile(&values, 0.50), 50);
        assert_eq!(percentile(&values, 0.95), 95);
        assert_eq!(percentile(&values, 0.99), 99);
    }

    #[test]
    fn sample_controls_must_use_the_same_public_row_work() {
        let public = |candidate_blocks, decoded_entries, payload_bytes, matched_entries| {
            json!({
                "iterations": 50,
                "stats_delta": {
                    "query_count": 50,
                    "query_bounded_requested_entries": 5_000_050,
                    "query_candidate_blocks": candidate_blocks,
                    "query_decoded_entries": decoded_entries,
                    "query_payload_bytes_read": payload_bytes,
                    "query_matched_entries": matched_entries,
                    "query_returned_entries": matched_entries,
                }
            })
        };
        let mut queries = Map::from_iter([
            (
                "control".to_owned(),
                public(200, 409_600, 95_702_750, 409_600),
            ),
            (
                "sampled".to_owned(),
                public(200, 409_600, 95_702_750, 409_600),
            ),
        ]);
        require_same_public_query_work(&queries, "control", "sampled").unwrap();

        queries.insert(
            "control".to_owned(),
            json!({
                "iterations": 50,
                "stats_delta": {"native_count_count": 50}
            }),
        );
        let error = require_same_public_query_work(&queries, "control", "sampled").unwrap_err();
        assert!(format!("{error:#}").contains("native-count fast path"));
    }

    #[test]
    fn multi_scan_controls_must_declare_the_exact_scan_count() {
        let public = |requested_entries| {
            json!({
                "iterations": 50,
                "stats_delta": {
                    "query_count": 100,
                    "query_bounded_requested_entries": requested_entries,
                    "query_candidate_blocks": 400,
                    "query_decoded_entries": 819_200,
                    "query_payload_bytes_read": 191_405_500,
                    "query_matched_entries": 819_200,
                    "query_returned_entries": 6_400,
                }
            })
        };
        let queries = Map::from_iter([
            ("control".to_owned(), public(10_000_100)),
            ("sampled".to_owned(), public(9_990_500)),
        ]);

        require_same_public_query_work_with_scans(&queries, "control", "sampled", 2, 192).unwrap();

        let error = require_same_public_query_work(&queries, "control", "sampled").unwrap_err();
        assert!(format!("{error:#}").contains("1 scans/request"));

        let error = require_same_public_query_work_with_scans(&queries, "control", "sampled", 2, 0)
            .unwrap_err();
        assert!(format!("{error:#}").contains("state reservation"));

        let union_queries = Map::from_iter([
            ("control".to_owned(), public(10_000_100)),
            ("sampled".to_owned(), public(9_993_700)),
        ]);
        require_same_public_query_work_with_scans(&union_queries, "control", "sampled", 2, 128)
            .unwrap();
        let error =
            require_same_public_query_work_with_scans(&union_queries, "control", "sampled", 2, 192)
                .unwrap_err();
        assert!(format!("{error:#}").contains("state reservation"));
    }

    #[test]
    fn input_independent_shapes_must_record_zero_public_storage_work() {
        let mut queries = Map::from_iter([(
            "sequence".to_owned(),
            json!({
                "iterations": 50,
                "stats_delta": {
                    "api_query_count": 50,
                    "api_query_result_rows": 3_200,
                    "read_permit_count": 51
                }
            }),
        )]);
        require_no_public_query_work(&queries, &["sequence"]).unwrap();

        queries["sequence"]["stats_delta"]["query_decoded_entries"] = json!(1);
        let error = require_no_public_query_work(&queries, &["sequence"]).unwrap_err();
        assert!(format!("{error:#}").contains("query_decoded_entries=1"));
    }

    #[test]
    fn failed_evidence_preserves_the_database_and_server_logs() {
        let temporary = TempDir::with_prefix("timeless-evidence-failure-test-").unwrap();
        let path = temporary.path().to_path_buf();
        fs::write(path.join("logs.db"), b"diagnostic database").unwrap();
        fs::write(path.join("logs.server.log"), b"diagnostic log").unwrap();

        let error =
            preserve_failed_evidence::<()>(temporary, Err(anyhow::anyhow!("decode failed")))
                .unwrap_err();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("decode failed"));
        assert!(rendered.contains(&path.display().to_string()));
        assert_eq!(
            fs::read(path.join("logs.db")).unwrap(),
            b"diagnostic database"
        );
        assert_eq!(
            fs::read(path.join("logs.server.log")).unwrap(),
            b"diagnostic log"
        );
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn numeric_delta_omits_unchanged_gauges_and_strings() {
        assert_eq!(
            numeric_delta(
                &json!({"count": 2, "bytes": 10, "module": "logs", "steady": 4}),
                &json!({"count": 5, "bytes": 22, "module": "logs", "steady": 4}),
            ),
            json!({"count": 3, "bytes": 12})
        );
    }

    #[test]
    fn cardinality_parsers_reject_data_loss() {
        assert_eq!(
            cardinality(
                br#"{"status":"success","data":{"result":[{},{}]}}"#,
                Cardinality::ResultSeries
            )
            .unwrap(),
            2
        );
        assert_eq!(cardinality(b"{}\n{}\n", Cardinality::Ndjson).unwrap(), 2);
        assert!(cardinality(br#"{"status":"error"}"#, Cardinality::ResultSeries).is_err());
        assert_eq!(
            cardinality(
                br#"{"status":"success","data":{"resultType":"scalar","result":[2,"NaN"]}}"#,
                Cardinality::Scalar
            )
            .unwrap(),
            1
        );
    }

    #[test]
    fn stale_binary_and_extension_identities_are_rejected() {
        let identity = json!({"commit": "old", "name": "test"});
        assert!(validate_build_identity(&identity, "new", "binary")
            .unwrap_err()
            .to_string()
            .contains("does not match evidence source"));
        assert!(validate_build_identity(&identity, "new", "extension")
            .unwrap_err()
            .to_string()
            .contains("does not match evidence source"));
    }

    #[test]
    fn metric_spec_keys_are_unique_and_include_session_fifteen_rows() {
        let specs = metric_specs(512, 64, 1_800_000_310);
        let mut keys = std::collections::BTreeSet::new();
        for spec in &specs {
            assert!(keys.insert(spec.key.as_str()), "duplicate {}", spec.key);
        }
        // The work-limit query is appended only after its 100,025-point
        // fixture crosses the second durability barrier.
        assert!(keys.insert("work_limit_rejected"));
        assert_eq!(keys.len(), 184);
        assert!(keys.contains("histogram_quantile_narrow"));
        assert!(keys.contains("histogram_quantile_wide"));
        assert!(keys.contains("quoted_name_narrow"));
        assert!(keys.contains("quoted_name_wide"));
        assert!(keys.contains("comments_narrow"));
        assert!(keys.contains("comments_wide"));
        assert!(keys.contains("histogram_fraction_narrow"));
        assert!(keys.contains("histogram_fraction_wide"));
        assert!(keys.contains("native_histogram_float_narrow"));
        assert!(keys.contains("native_histogram_float_wide"));
        assert!(keys.contains("native_histogram_float_control_narrow"));
        assert!(keys.contains("native_histogram_float_control_wide"));
        assert!(keys.contains("atan2_narrow"));
        assert!(keys.contains("atan2_wide"));
        assert!(keys.contains("annotations_narrow"));
        assert!(keys.contains("annotations_wide"));
        assert!(keys.contains("metricsql_default_narrow"));
        assert!(keys.contains("metricsql_default_wide"));
        assert!(keys.contains("metricsql_keep_names_narrow"));
        assert!(keys.contains("metricsql_keep_names_wide"));
        assert!(keys.contains("metricsql_alias_narrow"));
        assert!(keys.contains("metricsql_alias_wide"));
        assert!(keys.contains("metricsql_union_narrow"));
        assert!(keys.contains("metricsql_union_wide"));
        assert!(keys.contains("metricsql_label_set_narrow"));
        assert!(keys.contains("metricsql_label_set_wide"));
        assert!(keys.contains("metricsql_label_del_narrow"));
        assert!(keys.contains("metricsql_label_del_wide"));
        assert!(keys.contains("metricsql_default_rollup_narrow"));
        assert!(keys.contains("metricsql_default_rollup_wide"));
        assert!(keys.contains("metricsql_windowless_avg_narrow"));
        assert!(keys.contains("metricsql_windowless_avg_wide"));
        assert!(keys.contains("metricsql_windowless_rate_narrow"));
        assert!(keys.contains("metricsql_windowless_rate_wide"));
        assert!(keys.contains("metricsql_range_avg_narrow"));
        assert!(keys.contains("metricsql_range_avg_wide"));
        assert!(keys.contains("metricsql_range_sum_narrow"));
        assert!(keys.contains("metricsql_range_sum_wide"));
        assert!(keys.contains("metricsql_running_avg_narrow"));
        assert!(keys.contains("metricsql_running_avg_wide"));
        assert!(keys.contains("metricsql_running_sum_narrow"));
        assert!(keys.contains("metricsql_running_sum_wide"));
        assert!(keys.contains("metricsql_step_window_narrow"));
        assert!(keys.contains("metricsql_step_window_wide"));
        assert!(keys.contains("metricsql_step_offset_narrow"));
        assert!(keys.contains("metricsql_step_offset_wide"));
        assert!(keys.contains("metricsql_step_zero_rate_narrow"));
        assert!(keys.contains("metricsql_step_zero_rate_wide"));
        assert!(keys.contains("metricsql_context_scalar"));
        assert!(keys.contains("metricsql_context_narrow"));
        assert!(keys.contains("metricsql_context_wide"));
        assert!(keys.contains("metricsql_histogram_quantiles_one_narrow"));
        assert!(keys.contains("metricsql_histogram_quantiles_one_wide"));
        assert!(keys.contains("metricsql_histogram_quantiles_multi_narrow"));
        assert!(keys.contains("metricsql_histogram_quantiles_multi_wide"));
        assert!(keys.contains("result_limit_rejected"));

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let prior: Value = serde_json::from_slice(
            &fs::read(root.join("docs/evidence/2026-08-04_session9_pql_h01.json")).unwrap(),
        )
        .unwrap();
        let mut expected: std::collections::BTreeSet<&str> = prior
            .pointer("/metrics/queries")
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        expected.extend([
            "atan2_narrow",
            "atan2_wide",
            "annotations_narrow",
            "annotations_wide",
            "quoted_name_narrow",
            "quoted_name_wide",
            "comments_narrow",
            "comments_wide",
            "histogram_fraction_narrow",
            "histogram_fraction_wide",
            "native_histogram_float_narrow",
            "native_histogram_float_wide",
            "native_histogram_float_control_narrow",
            "native_histogram_float_control_wide",
            "metricsql_default_narrow",
            "metricsql_default_wide",
            "metricsql_keep_names_narrow",
            "metricsql_keep_names_wide",
            "metricsql_alias_narrow",
            "metricsql_alias_wide",
            "metricsql_union_narrow",
            "metricsql_union_wide",
            "metricsql_label_set_narrow",
            "metricsql_label_set_wide",
            "metricsql_label_del_narrow",
            "metricsql_label_del_wide",
            "metricsql_default_rollup_narrow",
            "metricsql_default_rollup_wide",
            "metricsql_windowless_avg_narrow",
            "metricsql_windowless_avg_wide",
            "metricsql_windowless_rate_narrow",
            "metricsql_windowless_rate_wide",
            "metricsql_range_avg_narrow",
            "metricsql_range_avg_wide",
            "metricsql_range_sum_narrow",
            "metricsql_range_sum_wide",
            "metricsql_running_avg_narrow",
            "metricsql_running_avg_wide",
            "metricsql_running_sum_narrow",
            "metricsql_running_sum_wide",
            "metricsql_step_window_narrow",
            "metricsql_step_window_wide",
            "metricsql_step_offset_narrow",
            "metricsql_step_offset_wide",
            "metricsql_step_zero_rate_narrow",
            "metricsql_step_zero_rate_wide",
            "metricsql_context_scalar",
            "metricsql_context_narrow",
            "metricsql_context_wide",
            "metricsql_histogram_quantiles_one_narrow",
            "metricsql_histogram_quantiles_one_wide",
            "metricsql_histogram_quantiles_multi_narrow",
            "metricsql_histogram_quantiles_multi_wide",
        ]);
        assert_eq!(keys, expected);
    }
}
