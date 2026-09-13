//! A deliberately bounded, reproducible public-HTTP comparison. Every response
//! is checked against independently generated expected data outside the timer.
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use chrono::{SecondsFormat, TimeZone, Utc};
use clap::Args;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[derive(Args, Debug)]
pub(crate) struct CompetitiveArgs {
    #[arg(long, default_value = "podman")]
    runtime: String,
    /// Extracted, checksum-verified published Linux bundle (never a checkout).
    #[arg(long)]
    bundle: PathBuf,
    /// Immutable native Linux image with sh, cat and GNU du, for the bundle.
    #[arg(long)]
    base_image: String,
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
    #[arg(long)]
    output: PathBuf,
}

fn command(runtime: &str, args: &[String]) -> Result<String> {
    let output = super::oracle::command_output(runtime, args, Duration::from_secs(180))?;
    ensure!(
        output.status.success(),
        "{runtime} {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| (*s).to_owned()).collect()
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn verify_bundle(bundle: &Path) -> Result<Value> {
    let checksums = fs::read_to_string(bundle.join("SHA256SUMS"))?;
    let mut verified = Vec::new();
    for line in checksums.lines() {
        let (expected, name) = line.split_once("  ").context("malformed bundle checksum")?;
        ensure!(
            !Path::new(name).is_absolute()
                && !Path::new(name)
                    .components()
                    .any(|c| c == std::path::Component::ParentDir),
            "unsafe bundle path"
        );
        ensure!(
            digest(&fs::read(bundle.join(name))?) == expected,
            "bundle checksum mismatch: {name}"
        );
        verified.push(name);
    }
    for required in [
        "artifact-manifest.json",
        "lib/libtimeless_ext.so",
        "bin/timeless-metrics-api",
        "bin/timeless-logs-api",
    ] {
        ensure!(
            verified.contains(&required),
            "bundle checksums omit {required}"
        );
    }
    let manifest: Value =
        serde_json::from_slice(&fs::read(bundle.join("artifact-manifest.json"))?)?;
    ensure!(
        manifest["dirty"] == false,
        "bundle must have clean source identity"
    );
    Ok(manifest)
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    TimelessMetrics,
    Prometheus,
    VictoriaMetrics,
    TimelessLogs,
    VictoriaLogs,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::TimelessMetrics => "timeless-metrics",
            Self::Prometheus => "prometheus",
            Self::VictoriaMetrics => "victoriametrics",
            Self::TimelessLogs => "timeless-logs",
            Self::VictoriaLogs => "victorialogs",
        }
    }
    fn logs(self) -> bool {
        matches!(self, Self::TimelessLogs | Self::VictoriaLogs)
    }
    fn timeless(self) -> bool {
        matches!(self, Self::TimelessMetrics | Self::TimelessLogs)
    }
}

struct Server {
    runtime: String,
    name: String,
    volume: String,
    base: String,
    kind: Kind,
    identity: Value,
    startup_storage: Value,
}

impl Drop for Server {
    fn drop(&mut self) {
        // The names are unique to this run. Never prune unrelated containers.
        let _ = command(&self.runtime, &strings(&["rm", "-f", "-v", &self.name]));
        let _ = command(&self.runtime, &strings(&["volume", "rm", &self.volume]));
    }
}

impl Server {
    fn start(
        args: &CompetitiveArgs,
        kind: Kind,
        image: &str,
        platform: &str,
        manifest: &Value,
        client: &Client,
    ) -> Result<Self> {
        ensure!(
            image.contains("@sha256:"),
            "floating image is forbidden: {image}"
        );
        command(
            &args.runtime,
            &strings(&["pull", "--platform", platform, image]),
        )?;
        let inspected: Value = serde_json::from_str(&command(
            &args.runtime,
            &strings(&["image", "inspect", image]),
        )?)?;
        let selected = &inspected[0];
        ensure!(
            selected["Architecture"] == platform.trim_start_matches("linux/")
                && selected["Os"] == "linux",
            "image platform mismatch: {selected}"
        );
        let index_output = super::oracle::command_output(
            &args.runtime,
            &strings(&["manifest", "inspect", image]),
            Duration::from_secs(180),
        )?;
        let index: Value = if index_output.status.success() {
            serde_json::from_slice(&index_output.stdout)?
        } else {
            // Podman 5.x rejects a single-platform manifest here; image
            // inspection still resolves its digest and exact config identity.
            ensure!(
                String::from_utf8_lossy(&index_output.stderr)
                    .contains("Treating single images as manifest lists is not implemented"),
                "manifest inspection failed: {}",
                String::from_utf8_lossy(&index_output.stderr)
            );
            Value::Null
        };
        let platform_digest = if let Some(manifests) = index["manifests"].as_array() {
            manifests
                .iter()
                .find(|entry| {
                    entry["platform"]["architecture"] == selected["Architecture"]
                        && entry["platform"]["os"] == "linux"
                })
                .and_then(|entry| entry["digest"].as_str())
                .context("native platform manifest")?
                .to_owned()
        } else {
            image.rsplit_once('@').context("image digest")?.1.to_owned()
        };
        let platform_image = format!(
            "{}@{platform_digest}",
            image.rsplit_once('@').context("image repository")?.0
        );
        let platform_manifest: Value = serde_json::from_str(&command(
            &args.runtime,
            &strings(&["image", "inspect", &platform_image]),
        )?)?;
        ensure!(
            platform_manifest[0]["Id"] == selected["Id"],
            "selected image differs from pinned platform manifest"
        );
        let name = format!(
            "timeless-competition-{}-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis(),
            kind.name()
        );
        let port = std::net::TcpListener::bind(("127.0.0.1", 0))?
            .local_addr()?
            .port();
        let volume = format!("{name}-data");
        let mut server = Self {
            runtime: args.runtime.clone(),
            name,
            volume,
            base: format!("http://127.0.0.1:{port}"),
            kind,
            identity: json!({"image": image, "image_id": selected["Id"], "selected_digest": platform_digest, "architecture": selected["Architecture"]}),
            startup_storage: Value::Null,
        };
        command(
            &args.runtime,
            &strings(&["volume", "create", &server.volume]),
        )?;
        let mut run = strings(&[
            "run",
            "-d",
            "--name",
            &server.name,
            "--platform",
            platform,
            "--cpus",
            "4",
            "--memory",
            "4g",
            "--user",
            "0",
            "-v",
            &format!("{}:/data", server.volume),
            "-p",
            &format!("127.0.0.1:{port}:8080"),
        ]);
        let flags = match kind {
            Kind::TimelessMetrics | Kind::TimelessLogs => {
                run.extend(strings(&[
                    "-v",
                    &format!("{}:/opt/timeless:ro", args.bundle.display()),
                    "-e",
                    "TIMELESS_AUTH_MODE=disabled",
                    "-e",
                    "TIMELESS_ALLOW_NON_LOOPBACK=1",
                ]));
                let binary = if kind.logs() {
                    "timeless-logs-api"
                } else {
                    "timeless-metrics-api"
                };
                let version = command(
                    &args.runtime,
                    &strings(&[
                        "run",
                        "--rm",
                        "--platform",
                        platform,
                        "-v",
                        &format!("{}:/opt/timeless:ro", args.bundle.display()),
                        image,
                        &format!("/opt/timeless/bin/{binary}"),
                        "--version",
                    ]),
                )?;
                let version: Value = serde_json::from_str(&version)?;
                ensure!(
                    version["commit"] == manifest["commit"]
                        && version["profile"] == "release"
                        && version["target"] == manifest["extension"]["build"]["target"],
                    "bundle/server build mismatch"
                );
                server.identity["build"] = version;
                strings(&[
                    &format!("/opt/timeless/bin/{binary}"),
                    "/opt/timeless/lib/libtimeless_ext.so",
                    "/data/timeless.db",
                    "0.0.0.0:8080",
                ])
            }
            Kind::Prometheus => strings(&[
                "--config.file=/dev/null",
                "--storage.tsdb.path=/data/prometheus",
                "--web.listen-address=0.0.0.0:8080",
                "--web.enable-remote-write-receiver",
            ]),
            Kind::VictoriaMetrics => strings(&[
                "-storageDataPath=/data/victoriametrics",
                "-httpListenAddr=:8080",
                "-retentionPeriod=1d",
            ]),
            Kind::VictoriaLogs => strings(&[
                "-storageDataPath=/data/victorialogs",
                "-httpListenAddr=:8080",
                "-retentionPeriod=30d",
            ]),
        };
        server.identity["server_arguments"] = json!(flags);
        run.push(image.to_owned());
        run.extend(flags);
        command(&args.runtime, &run)?;
        server.ready(client)?;
        server.startup_storage = server.storage(&args.base_image, platform)?;
        Ok(server)
    }

    fn ready(&self, client: &Client) -> Result<()> {
        let path = if self.kind == Kind::Prometheus {
            "/-/ready"
        } else if self.kind.timeless() {
            "/ready"
        } else {
            "/health"
        };
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            if client
                .get(format!("{}{path}", self.base))
                .timeout(Duration::from_secs(1))
                .send()
                .is_ok_and(|r| r.status().is_success())
            {
                return Ok(());
            }
            let running = command(
                &self.runtime,
                &strings(&["inspect", "--format", "{{.State.Running}}", &self.name]),
            )?;
            if running != "true" || Instant::now() >= deadline {
                let logs = super::oracle::command_output(
                    &self.runtime,
                    &strings(&["logs", "--tail", "30", &self.name]),
                    Duration::from_secs(10),
                )?;
                bail!(
                    "{} did not become ready: {}{}",
                    self.kind.name(),
                    String::from_utf8_lossy(&logs.stdout),
                    String::from_utf8_lossy(&logs.stderr)
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn restart(&self, client: &Client) -> Result<()> {
        command(
            &self.runtime,
            &strings(&["stop", "--time", "30", &self.name]),
        )?;
        let state: Value =
            serde_json::from_str(&command(&self.runtime, &strings(&["inspect", &self.name]))?)?;
        ensure!(
            state[0]["State"]["ExitCode"] == 0 && state[0]["State"]["OOMKilled"] == false,
            "{} unclean shutdown: {}",
            self.kind.name(),
            state[0]["State"]
        );
        command(&self.runtime, &strings(&["start", &self.name]))?;
        self.ready(client)
    }

    fn storage(&self, base_image: &str, platform: &str) -> Result<Value> {
        let mut sizes = Vec::new();
        for flag in ["--apparent-size", "--block-size=1"] {
            let output = command(
                &self.runtime,
                &strings(&[
                    "run",
                    "--rm",
                    "--platform",
                    platform,
                    "-v",
                    &format!("{}:/data:ro", self.volume),
                    base_image,
                    "du",
                    "-s",
                    "--block-size=1",
                    flag,
                    "/data",
                ]),
            )?;
            sizes.push(
                output
                    .split_whitespace()
                    .next()
                    .context("du size")?
                    .parse::<u64>()?,
            );
        }
        Ok(
            json!({"apparent_bytes": sizes[0], "allocated_bytes": sizes[1], "scope": "entire data volume including WAL, indexes and metadata; no compaction or VACUUM forced"}),
        )
    }

    fn memory(&self, base_image: &str, platform: &str) -> Result<Value> {
        let status = command(
            &self.runtime,
            &strings(&[
                "run",
                "--rm",
                "--platform",
                platform,
                "--pid",
                &format!("container:{}", self.name),
                base_image,
                "cat",
                "/proc/1/status",
            ]),
        )?;
        let get = |name: &str| -> Result<u64> {
            status
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .context("missing proc status field")?
                .split_whitespace()
                .next()
                .context("missing proc value")?
                .parse()
                .context("parse proc value")
        };
        Ok(
            json!({"rss_kib": get("VmRSS:")?, "rss_hwm_kib": get("VmHWM:")?, "scope": "server PID 1 since durable restart; excludes page cache and runtime VM"}),
        )
    }
}

fn post(client: &Client, url: &str, content_type: &str, body: &[u8]) -> Result<()> {
    let response = client
        .post(url)
        .header("content-type", content_type)
        .body(body.to_vec())
        .send()?;
    let status = response.status();
    let body = response.text()?;
    ensure!(
        status.is_success(),
        "ingestion/flush {url}: {status}: {body}"
    );
    Ok(())
}

// Dependency-free protobuf/raw-Snappy, also independently decoded in tests.
fn varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
    out
}
fn bytes(field: u64, value: &[u8]) -> Vec<u8> {
    let mut out = varint((field << 3) | 2);
    out.extend(varint(value.len() as u64));
    out.extend(value);
    out
}
fn snappy(value: &[u8]) -> Vec<u8> {
    let mut out = varint(value.len() as u64);
    for chunk in value.chunks(65536) {
        let n = chunk.len() - 1;
        if n < 60 {
            out.push((n << 2) as u8);
        } else {
            out.push(61 << 2);
            out.extend((n as u16).to_le_bytes());
        }
        out.extend(chunk);
    }
    out
}

fn labels(index: usize, name: bool) -> Value {
    let mut labels = json!({"host": format!("h{index:05}"), "service": if index.is_multiple_of(2) { "api" } else { "worker" }});
    if name {
        labels["__name__"] = json!("competition_counter");
    }
    labels
}

fn metrics_fixture(series: usize, points: usize, at: i64) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut ndjson = Vec::new();
    let mut proto = Vec::new();
    for index in 0..series {
        let labels = labels(index, true);
        let times: Vec<_> = (0..points)
            .map(|p| (at - (points - 1 - p) as i64 * 10) * 1000)
            .collect();
        let values: Vec<_> = (0..points).map(|p| (index % 7 + p) as f64).collect();
        ndjson.extend(serde_json::to_vec(
            &json!({"metric": labels, "timestamps": times, "values": values}),
        )?);
        ndjson.push(b'\n');
        let mut encoded = Vec::new();
        for (key, value) in labels.as_object().context("labels")? {
            let mut label = bytes(1, key.as_bytes());
            label.extend(bytes(2, value.as_str().context("label value")?.as_bytes()));
            encoded.extend(bytes(1, &label));
        }
        for (time, value) in times.iter().zip(&values) {
            let mut sample = vec![9];
            sample.extend(value.to_le_bytes());
            sample.push(16);
            sample.extend(varint(*time as u64));
            encoded.extend(bytes(2, &sample));
        }
        proto.extend(bytes(1, &encoded));
    }
    Ok((ndjson, snappy(&proto)))
}

fn log_row(index: usize, at: i64) -> Value {
    json!({"_time": Utc.timestamp_micros(at * 1_000_000 + index as i64).single().unwrap().to_rfc3339_opts(SecondsFormat::Micros, true),
        "_msg": format!("competition event {index:08}"), "level": if index.is_multiple_of(8) { "error" } else { "info" },
        "service": if index.is_multiple_of(4) { "api" } else { "worker" }, "host": format!("h{:02}", index % 64)})
}

#[derive(Clone)]
struct Query {
    name: &'static str,
    expression: String,
    range: bool,
    expected: Value,
}

fn metric_queries(series: usize, points: usize, at: i64) -> Vec<Query> {
    let vector = |entries: Vec<(Value, f64)>| json!({"resultType":"vector", "result": entries.into_iter().map(|(metric,value)| json!({"metric":metric,"value":[at,value.to_string()]})).collect::<Vec<_>>()});
    let last = |i| (i % 7 + points - 1) as f64;
    let wide = (0..series).map(|i| (labels(i, true), last(i))).collect();
    let by_service = ["api", "worker"]
        .iter()
        .enumerate()
        .map(|(parity, service)| {
            (
                json!({"service":service}),
                (0..series).filter(|i| i % 2 == parity).map(last).sum(),
            )
        })
        .collect();
    vec![
        Query {
            name: "exact",
            expression: "competition_counter{host=\"h00000\"}".into(),
            range: false,
            expected: vector(vec![(labels(0, true), last(0))]),
        },
        Query {
            name: "wide",
            expression: "competition_counter".into(),
            range: false,
            expected: vector(wide),
        },
        Query {
            name: "sum",
            expression: "sum(competition_counter)".into(),
            range: false,
            expected: vector(vec![(json!({}), (0..series).map(last).sum())]),
        },
        Query {
            name: "grouped_sum",
            expression: "sum by (service) (competition_counter)".into(),
            range: false,
            expected: vector(by_service),
        },
        Query {
            name: "rate",
            expression: "rate(competition_counter[60s])".into(),
            range: false,
            expected: vector((0..series).map(|i| (labels(i, false), 0.1)).collect()),
        },
        Query {
            name: "range",
            expression: "competition_counter".into(),
            range: true,
            expected: json!({"resultType":"matrix","result":(0..series).map(|i|json!({"metric":labels(i,true),"values":(0..points).map(|p|json!([at-(points-1-p) as i64*10,(i%7+p).to_string()])).collect::<Vec<_>>()})).collect::<Vec<_>>()}),
        },
    ]
}

fn log_queries(entries: usize, at: i64) -> Vec<Query> {
    let fields = " | fields _time, _msg, level, service, host";
    let rows = |filter: fn(usize) -> bool| {
        Value::Array(
            (0..entries)
                .filter(|i| filter(*i))
                .map(|i| log_row(i, at))
                .collect(),
        )
    };
    let query = |name, expression: String, expected| Query {
        name,
        expression,
        range: false,
        expected,
    };
    vec![
        query(
            "count",
            "* | stats count() as n".into(),
            json!([{"n":entries.to_string()}]),
        ),
        query(
            "filtered_count",
            "service:=api | stats count() as n".into(),
            json!([{"n":entries.div_ceil(4).to_string()}]),
        ),
        query(
            "grouped_count",
            "* | stats by (service) count() as n | sort by (service)".into(),
            json!([{"service":"api","n":entries.div_ceil(4).to_string()},{"service":"worker","n":(entries-entries.div_ceil(4)).to_string()}]),
        ),
        query(
            "exact_message",
            format!("_msg:=\"competition event 00000042\"{fields}"),
            rows(|i| i == 42),
        ),
        query(
            "indexed_rows",
            format!("host:=h00 | sort by (_time){fields}"),
            rows(|i| i % 64 == 0),
        ),
        query(
            "wide_rows",
            format!("* | sort by (_time){fields}"),
            rows(|_| true),
        ),
    ]
}

// Only vector/matrix series ordering and numeric spellings are normalized.
// Labels, timestamp values, sample order, result types and all log fields stay.
fn canonical_metrics(data: &Value) -> Result<Value> {
    let mut result = data.clone();
    let kind = data["resultType"].as_str().context("resultType")?;
    ensure!(
        matches!(kind, "vector" | "matrix"),
        "unexpected metric type: {kind}"
    );
    let rows = result["result"].as_array_mut().context("metric result")?;
    for row in rows.iter_mut() {
        ensure!(row["metric"].is_object(), "missing metric labels");
        let samples: Vec<&mut Value> = if kind == "vector" {
            vec![row.get_mut("value").context("vector value")?]
        } else {
            row["values"]
                .as_array_mut()
                .context("matrix values")?
                .iter_mut()
                .collect()
        };
        for sample in samples {
            let pair = sample.as_array_mut().context("sample pair")?;
            ensure!(pair.len() == 2, "sample pair length");
            let timestamp = pair[0].as_f64().context("numeric timestamp")?;
            let value: f64 = pair[1].as_str().context("string sample")?.parse()?;
            ensure!(
                timestamp.is_finite() && value.is_finite(),
                "non-finite sample in finite fixture"
            );
            // Timestamp identity is exact; float value tolerance must never
            // scale with epoch seconds and hide a shifted evaluation grid.
            pair[0] = json!(timestamp.to_string());
            pair[1] = json!(value);
        }
    }
    rows.sort_by_cached_key(|row| row["metric"].to_string());
    Ok(result)
}

fn canonical_logs(rows: &Value) -> Result<Value> {
    let mut rows = rows.clone();
    for row in rows.as_array_mut().context("log array")? {
        let object = row.as_object_mut().context("log object")?;
        if let Some(time) = object.get_mut("_time") {
            *time = json!(chrono::DateTime::parse_from_rfc3339(
                time.as_str().context("log timestamp string")?
            )?
            .timestamp_micros());
        }
    }
    Ok(rows)
}

fn equivalent(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                return a == b;
            }
            let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            (a - b).abs() <= 1e-9 * b.abs().max(1.0)
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| equivalent(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, a)| b.get(k).is_some_and(|b| equivalent(a, b)))
        }
        _ => actual == expected,
    }
}

fn request(
    client: &Client,
    server: &Server,
    query: &Query,
    at: i64,
    points: usize,
) -> Result<(u64, usize, Value)> {
    let path = if server.kind.logs() {
        "/select/logsql/query"
    } else if query.range {
        "/api/v1/query_range"
    } else {
        "/api/v1/query"
    };
    let mut params = vec![("query", query.expression.clone())];
    if server.kind.logs() {
        params.extend([
            ("start", (at - 1).to_string()),
            ("end", (at + 1).to_string()),
            ("limit", "100000".into()),
        ]);
    } else if query.range {
        params.extend([
            ("start", (at - (points - 1) as i64 * 10).to_string()),
            ("end", at.to_string()),
            ("step", "10s".into()),
        ]);
    } else {
        params.push(("time", at.to_string()));
    }
    let url = format!("{}{path}", server.base);
    let request = if server.kind.logs() {
        client.post(&url).form(&params)
    } else {
        client.get(&url).query(&params)
    }
    .build()?;
    let started = Instant::now();
    let response = client.execute(request)?;
    let status = response.status();
    let body = response.bytes()?;
    let elapsed = started.elapsed().as_nanos() as u64;
    ensure!(
        status.is_success(),
        "{} {} HTTP {status}: {}",
        server.kind.name(),
        query.name,
        String::from_utf8_lossy(&body)
    );
    let actual = if server.kind.logs() {
        let rows: Result<Vec<Value>, _> = body
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(serde_json::from_slice)
            .collect();
        canonical_logs(&json!(rows?))?
    } else {
        let envelope: Value = serde_json::from_slice(&body)?;
        ensure!(
            envelope["status"] == "success"
                && envelope.get("warnings").is_none()
                && envelope.get("infos").is_none(),
            "unexpected query diagnostics: {envelope}"
        );
        canonical_metrics(&envelope["data"])?
    };
    Ok((elapsed, body.len(), actual))
}

fn checked_request(
    client: &Client,
    server: &Server,
    query: &Query,
    at: i64,
    points: usize,
) -> Result<(u64, usize)> {
    let (elapsed, size, actual) = request(client, server, query, at, points)?;
    let expected = if server.kind.logs() {
        canonical_logs(&query.expected)?
    } else {
        canonical_metrics(&query.expected)?
    };
    ensure!(
        equivalent(&actual, &expected),
        "{} {} result differs: actual={} expected={}",
        server.kind.name(),
        query.name,
        actual.to_string().chars().take(1200).collect::<String>(),
        expected.to_string().chars().take(1200).collect::<String>()
    );
    Ok((elapsed, size))
}

fn percentile(sorted: &[u64], percent: usize) -> u64 {
    sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)]
}

fn measure(
    client: &Client,
    servers: &[Server],
    queries: &[Query],
    at: i64,
    args: &CompetitiveArgs,
) -> Result<Value> {
    let mut results = serde_json::Map::new();
    for query in queries {
        let mut samples = vec![Vec::new(); servers.len()];
        let mut sizes = vec![Vec::new(); servers.len()];
        for round in 0..args.warmup + args.iterations {
            // Rotate first engine to reduce order bias; requests are serialized.
            for offset in 0..servers.len() {
                let i = (round + offset) % servers.len();
                let (elapsed, bytes) =
                    checked_request(client, &servers[i], query, at, args.metric_points)?;
                if round >= args.warmup {
                    samples[i].push(elapsed);
                    sizes[i].push(bytes);
                }
            }
        }
        let mut engines = serde_json::Map::new();
        for (i, server) in servers.iter().enumerate() {
            let mut sorted = samples[i].clone();
            sorted.sort_unstable();
            engines.insert(server.kind.name().into(),json!({"latency_ns":{"p50":percentile(&sorted,50),"p95":percentile(&sorted,95),"p99":percentile(&sorted,99),"min":sorted[0],"max":sorted[sorted.len()-1]},"samples_ns":samples[i],"response_bytes":{"min":sizes[i].iter().min(),"max":sizes[i].iter().max()}}));
        }
        results.insert(query.name.into(),json!({"query":query.expression,"range":query.range,"expected_result_sha256":digest(serde_json::to_string(&query.expected)?.as_bytes()),"engines":engines}));
        println!(
            "competitive: {} {} verified for every engine and iteration",
            if servers[0].kind.logs() {
                "logs"
            } else {
                "metrics"
            },
            query.name
        );
    }
    Ok(json!(results))
}

pub(crate) fn run(root: &Path, mut args: CompetitiveArgs) -> Result<()> {
    ensure!(args.iterations>=10 && args.warmup>=1 && args.metric_series>=2 && args.metric_points>=7 && args.log_entries>=43 && args.log_entries<1_000_000,"invalid workload: iterations>=10, warmup>=1, series>=2, points>=7, 43<=logs<1000000 required");
    ensure!(
        args.base_image.contains("@sha256:"),
        "base image must be immutable"
    );
    let source = command(
        "git",
        &strings(&["-C", &root.to_string_lossy(), "rev-parse", "HEAD"]),
    )?;
    ensure!(
        command(
            "git",
            &strings(&[
                "-C",
                &root.to_string_lossy(),
                "status",
                "--porcelain",
                "--untracked-files=no"
            ])
        )?
        .is_empty(),
        "competitive evidence requires clean tracked source"
    );
    args.bundle = fs::canonicalize(root.join(&args.bundle))?;
    let manifest = verify_bundle(&args.bundle)?;
    let oracle: Value =
        serde_json::from_slice(&fs::read(root.join("tests/query_oracles/manifest.json"))?)?;
    let info: Value = serde_json::from_str(&command(
        &args.runtime,
        &strings(&["info", "--format", "json"]),
    )?)?;
    let arch = info
        .pointer("/host/arch")
        .or_else(|| info.get("Architecture"))
        .and_then(Value::as_str)
        .context("runtime host architecture")?;
    let arch = match arch {
        "arm64" | "aarch64" => "arm64",
        "amd64" | "x86_64" => "amd64",
        _ => bail!("unsupported native architecture {arch}"),
    };
    let platform = format!("linux/{arch}");
    let target = if arch == "arm64" {
        "aarch64-unknown-linux-gnu"
    } else {
        "x86_64-unknown-linux-gnu"
    };
    ensure!(
        manifest["extension"]["build"]["target"] == target,
        "bundle target must match native runtime"
    );
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let at = (Utc::now().timestamp() / 60 - 2) * 60;
    let mut report = json!({"schema_version":1,"captured_at":Utc::now().to_rfc3339(),"harness_commit":source,"timeless_artifact":manifest,
        "host":{"runtime":args.runtime,"platform":platform,"runtime_host":info.get("host"),"runtime_version":info.get("version")},
        "workload":{"metric_series":args.metric_series,"metric_points_per_series":args.metric_points,"log_entries":args.log_entries,"evaluation_unix_seconds":at,
            "iterations":args.iterations,"warmup":args.warmup,"single_client":true,"engine_order":"rotating per iteration","cpu_limit_per_engine":4,"memory_limit_bytes_per_engine":4_u64*1024*1024*1024,
            "http_path":"same host client through published loopback ports into one native Linux runtime","cache_policy":"engine defaults; warmed repeated exact queries, including any upstream result cache",
            "latency_scope":"HTTP request through complete response body; excludes request construction, JSON decoding and validation",
            "validation":"independent fixture expectations on every response; all metrics points and all projected log fields verified after graceful restart before timing",
            "limitations":["bounded synthetic read benchmark, not sustained ingest, concurrency, large-cardinality or cluster scaling","ingestion formats differ; admission time is not durable throughput","same string-field log model; no typed nested metadata or streams parity claim","storage includes startup/preallocation overhead and is not a long-term compression ratio"]},"signals":{}});
    for logs in [false, true] {
        let kinds = if logs {
            vec![Kind::TimelessLogs, Kind::VictoriaLogs]
        } else {
            vec![
                Kind::TimelessMetrics,
                Kind::Prometheus,
                Kind::VictoriaMetrics,
            ]
        };
        let mut servers = Vec::new();
        for kind in kinds {
            let image = if kind.timeless() {
                args.base_image.as_str()
            } else {
                oracle["oracles"][kind.name()]["image"]
                    .as_str()
                    .context("oracle image")?
            };
            let mut server = Server::start(&args, kind, image, &platform, &manifest, &client)?;
            if !kind.timeless() {
                let definition = &oracle["oracles"][kind.name()];
                let mut probe = strings(&[
                    "exec",
                    &server.name,
                    definition["version_entrypoint"]
                        .as_str()
                        .context("version entrypoint")?,
                ]);
                for arg in definition["version_args"]
                    .as_array()
                    .context("version args")?
                {
                    probe.push(arg.as_str().context("version arg")?.into());
                }
                let output =
                    super::oracle::command_output(&args.runtime, &probe, Duration::from_secs(30))?;
                let version = format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                ensure!(
                    output.status.success()
                        && version.contains(
                            definition["version_contains"]
                                .as_str()
                                .context("version contains")?
                        ),
                    "upstream version mismatch: {version}"
                );
                server.identity["reported_version"] = json!(version.trim());
                server.identity["oracle_pin"] = definition.clone();
            }
            servers.push(server);
        }
        let queries = if logs {
            log_queries(args.log_entries, at)
        } else {
            metric_queries(args.metric_series, args.metric_points, at)
        };
        let (logical, remote) = if logs {
            let mut ndjson = Vec::new();
            for i in 0..args.log_entries {
                ndjson.extend(serde_json::to_vec(&log_row(i, at))?);
                ndjson.push(b'\n');
            }
            (ndjson, Vec::new())
        } else {
            metrics_fixture(args.metric_series, args.metric_points, at)?
        };
        let mut details = BTreeMap::new();
        for server in &servers {
            let body = if server.kind == Kind::Prometheus {
                &remote
            } else {
                &logical
            };
            let path = if logs {
                "/insert/jsonline"
            } else if server.kind == Kind::Prometheus {
                "/api/v1/write"
            } else {
                "/api/v1/import"
            };
            let start = Instant::now();
            if server.kind == Kind::Prometheus {
                let response = client
                    .post(format!("{}{path}", server.base))
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Prometheus-Remote-Write-Version", "0.1.0")
                    .body(body.clone())
                    .send()?;
                ensure!(
                    response.status().is_success(),
                    "remote write failed: {}",
                    response.text()?
                );
            } else {
                post(
                    &client,
                    &format!("{}{path}", server.base),
                    "application/x-ndjson",
                    body,
                )?;
            }
            let admission = start.elapsed().as_nanos();
            if server.kind.timeless() {
                post(
                    &client,
                    &format!("{}/api/v1/flush", server.base),
                    "application/json",
                    &[],
                )?;
            }
            server.restart(&client)?;
            // Full matrix (all points) or full rows proves retained fixture;
            // readiness alone and selector counts cannot prove this.
            checked_request(
                &client,
                server,
                queries.last().context("full fixture query")?,
                at,
                args.metric_points,
            )?;
            details.insert(server.kind.name(),json!({"identity":server.identity,"ingestion":{"wire_bytes":body.len(),"wire_sha256":digest(body),"admission_ns":admission,"graceful_restart_verified":true},"startup_storage":server.startup_storage}));
        }
        let measurements = measure(&client, &servers, &queries, at, &args)?;
        for server in &servers {
            let value = details
                .get_mut(server.kind.name())
                .context("server details")?;
            value["memory"] = server.memory(&args.base_image, &platform)?;
            value["storage_after_queries"] = server.storage(&args.base_image, &platform)?;
            server.restart(&client)?;
            checked_request(
                &client,
                server,
                queries.last().context("full query")?,
                at,
                args.metric_points,
            )?;
            value["storage_after_final_restart"] = server.storage(&args.base_image, &platform)?;
        }
        report["signals"][if logs { "logs" } else { "metrics" }] = json!({"fixture_sha256":digest(&logical),"logical_wire_bytes":logical.len(),"engines":details,"queries":measurements});
        // Explicitly drop all engines for this signal before the next signal.
        drop(servers);
    }
    let output = root.join(args.output);
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        &output,
        format!("{}\n", serde_json::to_string_pretty(&report)?),
    )?;
    println!("{}", output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_comparison_preserves_labels_timestamps_and_missing_samples() {
        let expected = canonical_metrics(&metric_queries(2, 7, 1000)[5].expected).unwrap();
        let mut changed = expected.clone();
        changed["result"][0]["values"][0][0] = json!("941");
        assert!(!equivalent(&changed, &expected));
        changed = expected.clone();
        changed["result"][0]["metric"]["host"] = json!("wrong");
        assert!(!equivalent(&changed, &expected));
        changed = expected.clone();
        changed["result"][0]["values"].as_array_mut().unwrap().pop();
        assert!(!equivalent(&changed, &expected));
        assert!(!equivalent(&json!("1"), &json!(1)));
    }

    #[test]
    fn vector_order_is_unordered_but_log_order_and_fields_are_strict() {
        let mut data = metric_queries(2, 7, 1000)[1].expected.clone();
        let expected = canonical_metrics(&data).unwrap();
        data["result"].as_array_mut().unwrap().reverse();
        assert_eq!(canonical_metrics(&data).unwrap(), expected);
        let expected = canonical_logs(&log_queries(64, 1000)[5].expected).unwrap();
        let mut reversed = expected.clone();
        reversed.as_array_mut().unwrap().reverse();
        assert!(!equivalent(&reversed, &expected));
        let mut missing = expected.clone();
        missing[0].as_object_mut().unwrap().remove("level");
        assert!(!equivalent(&missing, &expected));
        assert!(!equivalent(
            &json!(1_789_000_000_000_000_i64),
            &json!(1_789_000_000_000_001_i64)
        ));
    }

    #[test]
    fn snappy_literal_payload_round_trips_across_length_boundaries() {
        for length in [1, 59, 60, 61, 255, 256, 65535, 65536, 65537, 200000] {
            let input: Vec<u8> = (0..length).map(|i| (i % 251) as u8).collect();
            let encoded = snappy(&input);
            let mut position = 0;
            let mut declared = 0;
            let mut shift = 0;
            loop {
                let byte = encoded[position];
                position += 1;
                declared |= ((byte & 127) as usize) << shift;
                if byte & 128 == 0 {
                    break;
                }
                shift += 7;
            }
            assert_eq!(declared, length);
            let mut decoded = Vec::new();
            while position < encoded.len() {
                let tag = encoded[position];
                position += 1;
                assert_eq!(tag & 3, 0);
                let mut count = (tag >> 2) as usize;
                if count >= 60 {
                    let width = count - 59;
                    count = 0;
                    for i in 0..width {
                        count |= (encoded[position] as usize) << (8 * i);
                        position += 1;
                    }
                }
                count += 1;
                decoded.extend_from_slice(&encoded[position..position + count]);
                position += count;
            }
            assert_eq!(decoded, input);
        }
    }
}
