use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{SecondsFormat, Utc};
use clap::{Args, ValueEnum};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Method;
use serde_json::{json, Map, Number, Value};
use tempfile::TempDir;
use wait_timeout::ChildExt;

/// Fixture timestamps are offsets from the most recent UTC midnight, NOT a
/// constant. The original constant base (2026-08-02) was a time bomb: the
/// metrics server prunes raw data at `now - 7 days` of wall-clock time, so
/// exactly seven days after the constant was written the gate started
/// failing everywhere ("overlap snapshot outside admission window") with no
/// code change — the fixture's points were simply aging out of retention
/// mid-run. A midnight-aligned dynamic base keeps rollup-granule alignment
/// and per-run determinism while the data stays at most 24h old.
fn base_seconds() -> u64 {
    use std::sync::OnceLock;
    static BASE: OnceLock<u64> = OnceLock::new();
    *BASE.get_or_init(|| {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time precedes Unix epoch")
            .as_secs();
        now / 86_400 * 86_400
    })
}

const ORDINALS_PER_SECOND: u64 = 256;

fn base_milliseconds() -> u64 {
    base_seconds() * 1_000
}

fn base_nanoseconds() -> u64 {
    base_seconds() * 1_000_000_000
}
const MIN_RELEASE_SECONDS_PER_SIGNAL: f64 = 2.0 * 60.0 * 60.0;
const RELEASE_AGGREGATE_SIGNAL_HOURS: f64 = 8.0;
const DEFAULT_RELEASE_SECONDS: f64 = RELEASE_AGGREGATE_SIGNAL_HOURS * 60.0 * 60.0 / 3.0;
const COUNTER_FIELDS: &[&str] = &[
    "checkpoint_count",
    "checkpoint_errors",
    "backup_count",
    "backup_errors",
    "compact_count",
    "compact_errors",
    "optimize_count",
    "optimize_errors",
    "prune_count",
    "prune_errors",
    "scheduled_flush_count",
    "scheduled_flush_errors",
    "api_read_cancelled",
    "extension_query_cancelled",
    "api_read_retries",
    "read_conflicts",
    "extension_read_conflicts",
    "writer_timeouts",
    "extension_writer_timeouts",
];

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Signal {
    Metrics,
    Logs,
    Traces,
}

const SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Traces];

impl Signal {
    fn name(self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Logs => "logs",
            Self::Traces => "traces",
        }
    }

    fn binary(self) -> &'static str {
        match self {
            Self::Metrics => "timeless-metrics-api",
            Self::Logs => "timeless-logs-api",
            Self::Traces => "timeless-traces-api",
        }
    }

    fn count_field(self) -> &'static str {
        match self {
            Self::Metrics => "total_points",
            Self::Logs => "total_entries",
            Self::Traces => "total_spans",
        }
    }

    fn stats_path(self) -> &'static str {
        match self {
            Self::Metrics => "/select/metrics/stats",
            Self::Logs => "/select/logsql/stats",
            Self::Traces => "/select/traces/stats",
        }
    }

    fn flush_method(self) -> Method {
        match self {
            Self::Logs => Method::GET,
            Self::Metrics | Self::Traces => Method::POST,
        }
    }

    fn expected_ingest_status(self) -> u16 {
        match self {
            Self::Metrics | Self::Logs => 204,
            Self::Traces => 200,
        }
    }

    fn query_shapes(self) -> &'static [&'static str] {
        match self {
            Self::Metrics => &[
                "exact_latest",
                "narrow_range",
                "wide_range",
                "scalar_avg",
                "discovery",
            ],
            Self::Logs => &["exact", "narrow", "wide", "scalar_count", "discovery"],
            Self::Traces => &[
                "exact_trace",
                "narrow_search",
                "wide_search",
                "operations",
                "services",
            ],
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum Mode {
    Short,
    Release,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Short => "short",
            Self::Release => "release",
        }
    }
}

#[derive(Args, Debug)]
pub(crate) struct ProductionArgs {
    #[arg(long, value_enum, default_value = "short")]
    mode: Mode,
    #[arg(long)]
    duration_seconds: Option<f64>,
    #[arg(long)]
    sample_seconds: Option<f64>,
    #[arg(long, default_value_t = 4.0)]
    write_hz: f64,
    #[arg(long, default_value_t = 5.0)]
    query_hz: f64,
    #[arg(long, default_value_t = 64)]
    batch: usize,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long, required = true)]
    output: PathBuf,
    #[arg(long, default_value = "target/release/libtimeless_ext.so")]
    extension: PathBuf,
    #[arg(long, default_value = "servers/target/release")]
    server_dir: PathBuf,
    #[arg(long)]
    skip_faults: bool,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    max_wal_bytes: u64,
    #[arg(long, default_value_t = 10_000.0)]
    max_p99_ms: f64,
    #[arg(long, default_value_t = 16_384.0)]
    max_rss_slope_kib_hour: f64,
    #[arg(long = "max-rss-kib")]
    max_rss_kib: Vec<String>,
}

#[derive(Clone, Debug)]
struct Config {
    mode: Mode,
    duration_seconds: f64,
    sample_seconds: f64,
    write_hz: f64,
    query_hz: f64,
    batch: usize,
    data_dir: Option<PathBuf>,
    output: PathBuf,
    extension: PathBuf,
    server_dir: PathBuf,
    skip_faults: bool,
    max_wal_bytes: u64,
    max_p99_ms: f64,
    max_rss_slope_kib_hour: f64,
    max_rss_kib: BTreeMap<Signal, u64>,
}

impl Config {
    fn new(root: &Path, args: ProductionArgs) -> Result<Self> {
        let duration_seconds = args.duration_seconds.unwrap_or(match args.mode {
            Mode::Short => 120.0,
            Mode::Release => DEFAULT_RELEASE_SECONDS,
        });
        let sample_seconds = args.sample_seconds.unwrap_or(match args.mode {
            Mode::Short => 5.0,
            Mode::Release => 30.0,
        });
        if duration_seconds <= 0.0
            || sample_seconds <= 0.0
            || args.write_hz <= 0.0
            || args.query_hz <= 0.0
        {
            bail!("durations and rates must be positive");
        }
        if args.mode == Mode::Release && duration_seconds < MIN_RELEASE_SECONDS_PER_SIGNAL {
            bail!("release mode requires at least two hours per concurrently running signal");
        }
        if args.batch == 0 || !args.batch.is_multiple_of(4) {
            bail!("--batch must be positive and divisible by four");
        }
        let mut max_rss_kib = BTreeMap::from([
            (Signal::Metrics, 512 * 1024),
            (Signal::Logs, 512 * 1024),
            (Signal::Traces, 768 * 1024),
        ]);
        for item in args.max_rss_kib {
            let (name, encoded) = item
                .split_once('=')
                .ok_or_else(|| anyhow!("--max-rss-kib must be SIGNAL=KIB"))?;
            let signal = SIGNALS
                .into_iter()
                .find(|signal| signal.name() == name)
                .ok_or_else(|| anyhow!("--max-rss-kib must be SIGNAL=KIB"))?;
            max_rss_kib.insert(
                signal,
                encoded
                    .parse::<u64>()
                    .context("--max-rss-kib must be SIGNAL=KIB")?,
            );
        }
        let absolute = |path: PathBuf| {
            if path.is_absolute() {
                path
            } else {
                root.join(path)
            }
        };
        Ok(Self {
            mode: args.mode,
            duration_seconds,
            sample_seconds,
            write_hz: args.write_hz,
            query_hz: args.query_hz,
            batch: args.batch,
            data_dir: args.data_dir.map(&absolute),
            output: absolute(args.output),
            extension: absolute(args.extension),
            server_dir: absolute(args.server_dir),
            skip_faults: args.skip_faults,
            max_wal_bytes: args.max_wal_bytes,
            max_p99_ms: args.max_p99_ms,
            max_rss_slope_kib_hour: args.max_rss_slope_kib_hour,
            max_rss_kib,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct Target {
    signal: Signal,
    port: u16,
}

#[derive(Clone, Copy, Debug)]
enum ChildLimits {
    None,
    Descriptors(u64),
    FileSize(u64),
}

struct Server {
    signal: Signal,
    binary: PathBuf,
    extension: PathBuf,
    database: PathBuf,
    port: u16,
    log_dir: PathBuf,
    short_maintenance: bool,
    child: Option<Child>,
    generation: u64,
}

impl Server {
    fn target(&self) -> Target {
        Target {
            signal: self.signal,
            port: self.port,
        }
    }

    fn environment(&self) -> Vec<(&'static str, &'static str)> {
        let mut values = vec![("TIMELESS_AUTH_MODE", "disabled")];
        if self.short_maintenance {
            match self.signal {
                Signal::Metrics => values.extend([
                    ("TIMELESS_METRICS_FLUSH_INTERVAL_SECS", "2"),
                    ("TIMELESS_METRICS_COMPACT_INTERVAL_SECS", "5"),
                    ("TIMELESS_METRICS_RETENTION_INTERVAL_SECS", "15"),
                ]),
                Signal::Logs => values.extend([
                    ("TIMELESS_LOGS_FLUSH_INTERVAL_SECS", "1"),
                    ("TIMELESS_LOGS_OPTIMIZE_INTERVAL_SECS", "5"),
                ]),
                Signal::Traces => values.extend([
                    ("TIMELESS_TRACES_FLUSH_INTERVAL_SECS", "1"),
                    ("TIMELESS_TRACES_OPTIMIZE_INTERVAL_SECS", "5"),
                ]),
            }
        }
        values
    }

    fn start(&mut self, client: &Client, limits: ChildLimits, timeout: Duration) -> Result<()> {
        if self
            .child
            .as_mut()
            .is_some_and(|child| child.try_wait().ok().flatten().is_none())
        {
            bail!("{} server is already running", self.signal.name());
        }
        self.generation += 1;
        fs::create_dir_all(&self.log_dir)?;
        let log_path = self.log_dir.join(format!(
            "{}-{}-g{}.log",
            self.signal.name(),
            self.port,
            self.generation
        ));
        let output = File::options().create(true).append(true).open(&log_path)?;
        let error = output.try_clone()?;
        let mut command = Command::new(&self.binary);
        command
            .args([
                self.extension.as_os_str(),
                self.database.as_os_str(),
                format!("127.0.0.1:{}", self.port).as_ref(),
            ])
            .stdout(Stdio::from(output))
            .stderr(Stdio::from(error))
            .envs(self.environment());
        if !matches!(limits, ChildLimits::None) {
            // SAFETY: pre_exec performs only async-signal-safe libc calls and
            // returns before the child executes any Rust runtime code.
            unsafe {
                command.pre_exec(move || apply_child_limits(limits));
            }
        }
        self.child = Some(
            command
                .spawn()
                .with_context(|| format!("start {}", self.binary.display()))?,
        );
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child_mut()?.try_wait()? {
                let tail = fs::read_to_string(&log_path).unwrap_or_default();
                let tail = tail.chars().rev().take(4_000).collect::<String>();
                bail!(
                    "{} exited during startup with {status}: {}",
                    self.signal.name(),
                    tail.chars().rev().collect::<String>()
                );
            }
            let last_error = match http_request(
                client,
                self.target(),
                Method::GET,
                "/ready",
                None,
                &[],
                Duration::from_secs(1),
            ) {
                Ok(response) if response.status == 200 => return Ok(()),
                Ok(response) => format!("HTTP {}: {:?}", response.status, response.body),
                Err(error) => format!("{error:#}"),
            };
            if Instant::now() >= deadline {
                self.kill()?;
                bail!("{} readiness timeout: {last_error}", self.signal.name());
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    fn stop(&mut self, timeout: Duration) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        if child.try_wait()?.is_none() {
            // SAFETY: child.id is a live child PID owned by this harness.
            let result = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
            if result != 0 {
                return Err(io::Error::last_os_error()).context("send SIGTERM");
            }
            if child.wait_timeout(timeout)?.is_none() {
                child.kill()?;
                let _ = child.wait_timeout(Duration::from_secs(10))?;
                bail!("{} did not gracefully stop", self.signal.name());
            }
        }
        let status = child
            .try_wait()?
            .unwrap_or_else(|| child.wait().expect("wait child"));
        if !status.success() {
            bail!("{} graceful stop exited {status}", self.signal.name());
        }
        Ok(())
    }

    fn kill(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            if child.try_wait()?.is_none() {
                child.kill()?;
                let _ = child.wait_timeout(Duration::from_secs(10))?;
            }
        }
        Ok(())
    }

    fn child_mut(&mut self) -> Result<&mut Child> {
        self.child.as_mut().context("server process is absent")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.kill();
    }
}

// glibc types setrlimit's resource as its own enum; every other libc
// (macOS included) uses a plain int. The RLIMIT_* constants carry the
// right type per platform, so an alias is all portability needs.
#[cfg(target_os = "linux")]
type RlimitResource = libc::__rlimit_resource_t;
#[cfg(not(target_os = "linux"))]
type RlimitResource = libc::c_int;

fn apply_child_limits(limits: ChildLimits) -> io::Result<()> {
    unsafe fn set(resource: RlimitResource, value: u64) -> io::Result<()> {
        let limit = libc::rlimit {
            rlim_cur: value,
            rlim_max: value,
        };
        // SAFETY: the pointer is valid for this call.
        if unsafe { libc::setrlimit(resource, &limit) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    match limits {
        ChildLimits::None => Ok(()),
        ChildLimits::Descriptors(value) => unsafe { set(libc::RLIMIT_NOFILE, value) },
        ChildLimits::FileSize(value) => {
            // SAFETY: SIG_IGN is a valid disposition and setrlimit receives a
            // valid rlimit pointer.
            unsafe {
                libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
                set(libc::RLIMIT_FSIZE, value)
            }
        }
    }
}

#[derive(Default)]
struct StateData {
    accepted: u64,
    next_ordinal: u64,
    write_latencies: Vec<f64>,
    query_latencies: BTreeMap<String, Vec<f64>>,
    query_response_bytes_hwm: BTreeMap<String, u64>,
    query_result_rows_hwm: BTreeMap<String, u64>,
    ingest_body_bytes_hwm: u64,
    errors: Vec<String>,
    rss_samples: Vec<(u64, f64, u64)>,
    resource_samples: Vec<Value>,
    max_watermarks: BTreeMap<String, u64>,
    epoch_stats: Vec<Value>,
    memory_hwm_kib: u64,
}

struct SignalState {
    signal: Signal,
    batch: usize,
    server: Mutex<Server>,
    operation: Mutex<()>,
    data: Mutex<StateData>,
}

impl SignalState {
    fn server(&self) -> Result<MutexGuard<'_, Server>> {
        self.server
            .lock()
            .map_err(|_| anyhow!("server mutex poisoned"))
    }

    fn operation(&self) -> Result<MutexGuard<'_, ()>> {
        self.operation
            .lock()
            .map_err(|_| anyhow!("operation mutex poisoned"))
    }

    fn data(&self) -> Result<MutexGuard<'_, StateData>> {
        self.data
            .lock()
            .map_err(|_| anyhow!("state mutex poisoned"))
    }

    fn target(&self) -> Result<Target> {
        Ok(self.server()?.target())
    }
}

#[derive(Debug)]
struct HttpResult {
    status: u16,
    body: Vec<u8>,
    headers: HeaderMap,
    elapsed_ms: f64,
    request_bytes: u64,
}

fn free_port() -> Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

fn http_request(
    client: &Client,
    target: Target,
    method: Method,
    path: &str,
    body: Option<Vec<u8>>,
    headers: &[(&str, &str)],
    timeout: Duration,
) -> Result<HttpResult> {
    let payload_len = body.as_ref().map_or(0, Vec::len) as u64;
    let started = Instant::now();
    let mut request = client
        .request(method, format!("http://127.0.0.1:{}{path}", target.port))
        .timeout(timeout);
    for (name, value) in headers {
        request = request.header(
            HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(value)?,
        );
    }
    if let Some(payload) = body {
        request = request.body(payload);
    }
    let response = request.send()?;
    let status = response.status().as_u16();
    let response_headers = response.headers().clone();
    let bytes = response.bytes()?.to_vec();
    Ok(HttpResult {
        status,
        body: bytes,
        headers: response_headers,
        elapsed_ms: started.elapsed().as_secs_f64() * 1_000.0,
        request_bytes: payload_len,
    })
}

fn require_status(result: HttpResult, expected: u16, operation: &str) -> Result<HttpResult> {
    if result.status != expected {
        let body = String::from_utf8_lossy(&result.body[..result.body.len().min(2_000)]);
        bail!("{operation}: HTTP {}: {body:?}", result.status);
    }
    Ok(result)
}

fn request_json(
    client: &Client,
    target: Target,
    method: Method,
    path: &str,
    body: Option<Vec<u8>>,
    headers: &[(&str, &str)],
    expected: u16,
) -> Result<(Value, f64)> {
    let result = require_status(
        http_request(
            client,
            target,
            method,
            path,
            body,
            headers,
            Duration::from_secs(60),
        )?,
        expected,
        path,
    )?;
    let value = serde_json::from_slice(&result.body)
        .with_context(|| format!("{path}: incomplete/non-JSON response"))?;
    Ok((value, result.elapsed_ms))
}

fn stats(client: &Client, target: Target) -> Result<Value> {
    let (value, _) = request_json(
        client,
        target,
        Method::GET,
        target.signal.stats_path(),
        None,
        &[],
        200,
    )?;
    if !value.is_object() {
        bail!("{} stats is not an object", target.signal.name());
    }
    Ok(value)
}

fn flush(client: &Client, target: Target) -> Result<Value> {
    request_json(
        client,
        target,
        target.signal.flush_method(),
        "/api/v1/flush",
        None,
        &[],
        200,
    )
    .map(|value| value.0)
}

fn metrics_body(start: u64, count: usize) -> Vec<u8> {
    let mut grouped: [Vec<(f64, u64)>; 4] = std::array::from_fn(|_| Vec::new());
    for ordinal in start..start + count as u64 {
        grouped[ordinal as usize % 4].push((
            ordinal as f64 + 0.5,
            base_milliseconds() + ordinal * 1_000 / ORDINALS_PER_SECOND,
        ));
    }
    let mut output = String::new();
    for (host, points) in grouped.into_iter().enumerate() {
        if points.is_empty() {
            continue;
        }
        let values = points.iter().map(|point| point.0).collect::<Vec<_>>();
        let timestamps = points.iter().map(|point| point.1).collect::<Vec<_>>();
        output.push_str(
            &serde_json::to_string(&json!({
                "metric": {"__name__": "release_gate_metric", "host": format!("host-{host}")},
                "values": values,
                "timestamps": timestamps,
            }))
            .expect("serialize metrics fixture"),
        );
        output.push('\n');
    }
    output.into_bytes()
}

fn logs_body(start: u64, count: usize) -> Vec<u8> {
    let levels = ["debug", "info", "warning", "error"];
    let mut output = String::new();
    for ordinal in start..start + count as u64 {
        output.push_str(
            &serde_json::to_string(&json!({
                "_time": base_seconds() + ordinal / ORDINALS_PER_SECOND,
                "_msg": format!("release-gate-{ordinal}"),
                "level": levels[ordinal as usize % levels.len()],
                "service": "release-gate",
                "host": format!("host-{}", ordinal % 4),
                "status": if ordinal % 4 == 3 { 500 } else { 200 },
                "attempt": ordinal,
                "sampled": ordinal % 2 == 0,
                "nested": {"worker": ordinal % 8},
                "tags": ["soak", format!("lane-{}", ordinal % 4)],
            }))
            .expect("serialize logs fixture"),
        );
        output.push('\n');
    }
    output.into_bytes()
}

fn traces_body(start: u64, count: usize) -> Vec<u8> {
    let spans = (start..start + count as u64)
        .map(|ordinal| {
            let trace_number = ordinal / 4 + 1;
            let root_ordinal = ordinal / 4 * 4;
            let start_ns = base_nanoseconds() + ordinal * 1_000_000_000 / ORDINALS_PER_SECOND;
            json!({
                "traceId": format!("{trace_number:032x}"),
                "spanId": format!("{:016x}", ordinal + 1),
                "parentSpanId": if ordinal % 4 == 0 { String::new() } else { format!("{:016x}", root_ordinal + 1) },
                "name": if ordinal % 4 == 0 { "GET /release-gate" } else { "db.query" },
                "kind": if ordinal % 4 == 0 { 2 } else { 3 },
                "startTimeUnixNano": start_ns.to_string(),
                "endTimeUnixNano": (start_ns + 500_000 + ordinal % 17).to_string(),
                "status": {"code": if ordinal % 17 == 0 { 2 } else { 1 }, "message": "gate"},
                "attributes": [
                    {"key": "gate.ordinal", "value": {"intValue": ordinal.to_string()}},
                    {"key": "gate.bool", "value": {"boolValue": ordinal % 2 == 0}},
                    {"key": "http.method", "value": {"stringValue": "GET"}},
                ],
                "events": [{
                    "timeUnixNano": (start_ns + 100).to_string(),
                    "name": "gate.event",
                    "attributes": [{"key": "event.ordinal", "value": {"intValue": ordinal.to_string()}}],
                }],
                "links": [],
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "release-gate"}},
                {"key": "deployment.environment", "value": {"stringValue": "soak"}},
            ]},
            "scopeSpans": [{
                "scope": {"name": "production-gate", "version": "1"},
                "spans": spans,
            }],
        }],
    }))
    .expect("serialize traces fixture")
}

fn ingest_result(client: &Client, target: Target, start: u64, count: usize) -> Result<HttpResult> {
    let (path, body, content_type) = match target.signal {
        Signal::Metrics => (
            "/api/v1/import",
            metrics_body(start, count),
            "application/x-ndjson",
        ),
        Signal::Logs => (
            "/insert/jsonline",
            logs_body(start, count),
            "application/x-ndjson",
        ),
        Signal::Traces => (
            "/insert/opentelemetry/v1/traces",
            traces_body(start, count),
            "application/json",
        ),
    };
    http_request(
        client,
        target,
        Method::POST,
        path,
        Some(body),
        &[("content-type", content_type)],
        Duration::from_secs(60),
    )
}

fn write_once(client: &Client, state: &SignalState) -> Result<()> {
    let start = state.data()?.next_ordinal;
    let result = require_status(
        ingest_result(client, state.target()?, start, state.batch)?,
        state.signal.expected_ingest_status(),
        &format!("{} ingest", state.signal.name()),
    )?;
    let mut data = state.data()?;
    if start != data.next_ordinal {
        bail!("{} writer ordinal raced", state.signal.name());
    }
    data.next_ordinal += state.batch as u64;
    data.accepted += state.batch as u64;
    data.write_latencies.push(result.elapsed_ms);
    data.ingest_body_bytes_hwm = data.ingest_body_bytes_hwm.max(result.request_bytes);
    Ok(())
}

fn decode_ndjson(result: &HttpResult) -> Result<Vec<Value>> {
    result
        .body
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).context("partial NDJSON response"))
        .collect::<Result<Vec<_>>>()
}

fn validate_ndjson(result: &HttpResult, operation: &str) -> Result<Vec<Value>> {
    let rows = decode_ndjson(result)?;
    if let Some(declared) = result.headers.get("x-timeless-result-rows") {
        let declared = declared.to_str()?.parse::<usize>()?;
        if declared != rows.len() {
            bail!(
                "{operation}: declared {declared} rows, decoded {}",
                rows.len()
            );
        }
    }
    Ok(rows)
}

fn result_rows(result: &HttpResult) -> Result<u64> {
    result
        .headers
        .get("x-timeless-result-rows")
        .map(|value| Ok(value.to_str()?.parse::<u64>()?))
        .unwrap_or(Ok(0))
}

fn metrics_query(client: &Client, state: &SignalState, shape: &str) -> Result<(f64, u64, u64)> {
    let newest = state.data()?.next_ordinal.saturating_sub(1);
    let newest_seconds = base_seconds() + newest / ORDINALS_PER_SECOND;
    let from_seconds = base_seconds().max(newest_seconds.saturating_sub(300));
    let path = match shape {
        "exact_latest" => "/api/v1/query?metric=release_gate_metric&host=host-0".to_owned(),
        "narrow_range" => format!(
            "/api/v1/query_range?metric=release_gate_metric&host=host-0&from={from_seconds}&to={newest_seconds}&step=10&aggregate=avg"
        ),
        "wide_range" => format!(
            "/api/v1/query_range?metric=release_gate_metric&from={}&to={newest_seconds}&step=10&aggregate=avg",
            base_seconds().max(newest_seconds.saturating_sub(60))
        ),
        "scalar_avg" => format!(
            "/api/v1/query_range?metric=release_gate_metric&host=host-1&from={from_seconds}&to={newest_seconds}&step=300&aggregate=avg"
        ),
        "discovery" => "/api/v1/label/host/values?metric=release_gate_metric".to_owned(),
        _ => bail!("unknown metrics query shape {shape}"),
    };
    let result = require_status(
        http_request(
            client,
            state.target()?,
            Method::GET,
            &path,
            None,
            &[],
            Duration::from_secs(60),
        )?,
        200,
        shape,
    )?;
    let _: Value = serde_json::from_slice(&result.body)
        .with_context(|| format!("metrics {shape}: partial JSON"))?;
    Ok((
        result.elapsed_ms,
        result.body.len() as u64,
        result_rows(&result)?,
    ))
}

fn logs_query(client: &Client, state: &SignalState, shape: &str) -> Result<(f64, u64, u64)> {
    let newest = state.data()?.next_ordinal.saturating_sub(1);
    let newest_seconds = base_seconds() + newest / ORDINALS_PER_SECOND;
    let (method, path, body, headers) = match shape {
        "exact" => (Method::GET, "/select/logsql/query?message=release-gate-0&limit=1&order=asc".to_owned(), None, vec![]),
        "narrow" => (Method::GET, "/select/logsql/query?level=error&service=release-gate&limit=100&order=desc".to_owned(), None, vec![]),
        "wide" => (Method::GET, format!(
            "/select/logsql/query?service=release-gate&limit=1000&order=desc&start={}&end={newest_seconds}",
            base_seconds().max(newest_seconds.saturating_sub(4_000))
        ), None, vec![]),
        "scalar_count" => (Method::POST, "/select/logsql/query".to_owned(), Some(b"query=level%3Aerror+%7C+stats+count%28*%29".to_vec()), vec![("content-type", "application/x-www-form-urlencoded")]),
        "discovery" => (Method::GET, "/select/logsql/field_values?field=host&service=release-gate&limit=10".to_owned(), None, vec![]),
        _ => bail!("unknown logs query shape {shape}"),
    };
    let result = require_status(
        http_request(
            client,
            state.target()?,
            method,
            &path,
            body,
            &headers,
            Duration::from_secs(60),
        )?,
        200,
        shape,
    )?;
    let rows = if shape == "discovery" {
        let _: Value = serde_json::from_slice(&result.body)?;
        result_rows(&result)?
    } else {
        validate_ndjson(&result, &format!("logs {shape}"))?.len() as u64
    };
    Ok((result.elapsed_ms, result.body.len() as u64, rows))
}

fn traces_query(client: &Client, state: &SignalState, shape: &str) -> Result<(f64, u64, u64)> {
    let trace_one = format!("{:032x}", 1);
    let path = match shape {
        "exact_trace" => format!("/select/jaeger/api/traces/{trace_one}"),
        "narrow_search" => "/select/jaeger/api/traces?service=release-gate&operation=GET%20%2Frelease-gate&limit=20".to_owned(),
        "wide_search" => "/select/jaeger/api/traces?service=release-gate&limit=100".to_owned(),
        "operations" => "/select/jaeger/api/services/release-gate/operations".to_owned(),
        "services" => "/select/jaeger/api/services".to_owned(),
        _ => bail!("unknown traces query shape {shape}"),
    };
    let result = require_status(
        http_request(
            client,
            state.target()?,
            Method::GET,
            &path,
            None,
            &[],
            Duration::from_secs(60),
        )?,
        200,
        shape,
    )?;
    let value: Value = serde_json::from_slice(&result.body)
        .with_context(|| format!("traces {shape}: partial JSON"))?;
    if value.get("data").is_none() {
        bail!("traces {shape}: invalid response shape");
    }
    Ok((
        result.elapsed_ms,
        result.body.len() as u64,
        result_rows(&result)?,
    ))
}

fn query_once(client: &Client, state: &SignalState, shape: &str) -> Result<()> {
    let (latency, response_bytes, rows) = match state.signal {
        Signal::Metrics => metrics_query(client, state, shape)?,
        Signal::Logs => logs_query(client, state, shape)?,
        Signal::Traces => traces_query(client, state, shape)?,
    };
    let mut data = state.data()?;
    data.query_latencies
        .entry(shape.to_owned())
        .or_default()
        .push(latency);
    data.query_response_bytes_hwm
        .entry(shape.to_owned())
        .and_modify(|value| *value = (*value).max(response_bytes))
        .or_insert(response_bytes);
    data.query_result_rows_hwm
        .entry(shape.to_owned())
        .and_modify(|value| *value = (*value).max(rows))
        .or_insert(rows);
    Ok(())
}

fn semantic_oracle(client: &Client, target: Target) -> Result<()> {
    match target.signal {
        Signal::Metrics => {
            let base = base_seconds();
            let path = format!(
                "/api/v1/export?metric=release_gate_metric&host=host-0&from={base}&to={base}"
            );
            let result = require_status(
                http_request(
                    client,
                    target,
                    Method::GET,
                    &path,
                    None,
                    &[],
                    Duration::from_secs(60),
                )?,
                200,
                "metrics sentinel",
            )?;
            // Metrics export returns one line per series while the result-row
            // header counts points. Do not apply the log-row header contract
            // to this endpoint.
            let rows = decode_ndjson(&result)?;
            let values = rows
                .first()
                .and_then(|row| row.get("values"))
                .and_then(Value::as_array);
            let timestamps = rows
                .first()
                .and_then(|row| row.get("timestamps"))
                .and_then(Value::as_array);
            let has_value = values.is_some_and(|values| values.contains(&json!(0.5)));
            let has_timestamp =
                timestamps.is_some_and(|values| values.contains(&json!(base_milliseconds())));
            if rows.len() != 1
                || !has_value
                || !has_timestamp
                || values.map(Vec::len) != timestamps.map(Vec::len)
            {
                bail!("metrics sentinel changed: {rows:?}");
            }
        }
        Signal::Logs => {
            let result = require_status(
                http_request(
                    client,
                    target,
                    Method::GET,
                    "/select/logsql/query?message=release-gate-0&limit=2&order=asc",
                    None,
                    &[],
                    Duration::from_secs(60),
                )?,
                200,
                "logs sentinel",
            )?;
            let rows = validate_ndjson(&result, "logs sentinel")?;
            if rows.len() != 1
                || rows[0].get("_msg") != Some(&json!("release-gate-0"))
                || rows[0].get("attempt") != Some(&json!(0))
                || rows[0].get("nested") != Some(&json!({"worker": 0}))
            {
                bail!("logs sentinel changed: {rows:?}");
            }
        }
        Signal::Traces => {
            let path = format!("/select/jaeger/api/traces/{:032x}", 1);
            let (value, _) = request_json(client, target, Method::GET, &path, None, &[], 200)?;
            let traces = value
                .get("data")
                .and_then(Value::as_array)
                .context("trace sentinel data")?;
            let spans = traces
                .first()
                .and_then(|trace| trace.get("spans"))
                .and_then(Value::as_array)
                .context("trace sentinel spans")?;
            let roots = spans
                .iter()
                .filter(|span| {
                    span.get("references")
                        .and_then(Value::as_array)
                        .is_none_or(Vec::is_empty)
                })
                .count();
            if traces.len() != 1 || spans.len() != 4 || roots != 1 {
                bail!("trace relationship sentinel changed: {value}");
            }
        }
    }
    Ok(())
}

fn spawn_worker(
    state: Arc<SignalState>,
    client: Client,
    active: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    interval: Duration,
    query: bool,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name(format!(
            "{}-{}",
            state.signal.name(),
            if query { "reader" } else { "writer" }
        ))
        .spawn(move || {
            let mut deadline = Instant::now();
            let mut shape_number = 0usize;
            while !stop.load(Ordering::Acquire) {
                if !active.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                deadline += interval;
                let result = (|| -> Result<()> {
                    let _operation = state.operation()?;
                    if !active.load(Ordering::Acquire) || stop.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    if query {
                        let shapes = state.signal.query_shapes();
                        let shape = shapes[shape_number % shapes.len()];
                        shape_number += 1;
                        query_once(&client, &state, shape).with_context(|| format!("query {shape}"))
                    } else {
                        write_once(&client, &state).context("writer")
                    }
                })();
                if let Err(error) = result {
                    if let Ok(mut data) = state.data() {
                        data.errors.push(format!("{error:#}"));
                    }
                    stop.store(true, Ordering::Release);
                    return;
                }
                while Instant::now() < deadline && !stop.load(Ordering::Acquire) {
                    thread::sleep((deadline - Instant::now()).min(Duration::from_millis(100)));
                }
            }
        })
        .expect("spawn production gate worker")
}

fn proc_memory(pid: Option<u32>) -> BTreeMap<String, u64> {
    let Some(pid) = pid else {
        return BTreeMap::new();
    };
    fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .into_iter()
        .flat_map(|status| {
            status
                .lines()
                .filter_map(|line| {
                    let (name, rest) = line.split_once(':')?;
                    if !matches!(name, "VmRSS" | "VmHWM") {
                        return None;
                    }
                    let value = rest.split_whitespace().next()?.parse::<u64>().ok()?;
                    Some((format!("{}_kib", name.to_ascii_lowercase()), value))
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn numeric_u64(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_f64().map(|value| value as u64))
    })
}

fn sample_state(client: &Client, state: &SignalState, elapsed: f64) -> Result<()> {
    let _operation = state.operation()?;
    let target = state.target()?;
    let current = stats(client, target)?;
    let server = state.server()?;
    let generation = server.generation;
    let memory = proc_memory(server.pid());
    drop(server);
    let rss = memory.get("vmrss_kib").copied().unwrap_or_default();
    let hwm = memory.get("vmhwm_kib").copied().unwrap_or_default();
    let keys = [
        state.signal.count_field(),
        "database_file_bytes",
        "database_wal_bytes",
        "database_shm_bytes",
        "physical_database_bytes",
        "bytes_on_disk",
        "disk_size",
        "freelist_bytes",
        "buffer_memory_bytes",
        "buffered_points",
        "buffered_entries",
        "buffered_spans",
        "queued_batches",
        "queued_requests",
        "queued_points",
        "queued_entries",
        "queued_spans",
        "queued_body_bytes",
        "in_flight_batches",
        "in_flight_requests",
        "command_queue_capacity_batches",
        "command_queue_capacity_requests",
        "query_snapshot_payload_max_bytes",
        "extension_query_snapshot_payload_max_bytes",
    ];
    let mut sample = Map::from_iter([
        ("elapsed_seconds".to_owned(), number(elapsed)),
        ("rss_kib".to_owned(), json!(rss)),
        ("hwm_kib".to_owned(), json!(hwm)),
    ]);
    let mut data = state.data()?;
    data.memory_hwm_kib = data.memory_hwm_kib.max(hwm);
    data.rss_samples.push((generation, elapsed, rss));
    for key in keys {
        if let Some(value) = numeric_u64(&current, key) {
            sample.insert(key.to_owned(), json!(value));
            data.max_watermarks
                .entry(key.to_owned())
                .and_modify(|existing| *existing = (*existing).max(value))
                .or_insert(value);
        }
    }
    data.resource_samples.push(Value::Object(sample));
    Ok(())
}

fn durable_barrier_unlocked(client: &Client, state: &SignalState) -> Result<(Value, Value)> {
    let target = state.target()?;
    let flush_report = flush(client, target)?;
    let current = stats(client, target)?;
    let expected = state.data()?.accepted;
    let actual = numeric_u64(&current, state.signal.count_field())
        .with_context(|| format!("missing {}", state.signal.count_field()))?;
    if actual != expected {
        bail!(
            "{} durable count mismatch: expected {expected}, got {actual}",
            state.signal.name()
        );
    }
    for field in [
        "queued_batches",
        "queued_requests",
        "in_flight_batches",
        "in_flight_requests",
    ] {
        if numeric_u64(&current, field).unwrap_or_default() != 0 {
            bail!("{} did not drain {field}", state.signal.name());
        }
    }
    semantic_oracle(client, target)?;
    Ok((flush_report, current))
}

fn durable_barrier(client: &Client, state: &SignalState) -> Result<(Value, Value)> {
    let _operation = state.operation()?;
    durable_barrier_unlocked(client, state)
}

fn restart_all(
    client: &Client,
    states: &BTreeMap<Signal, Arc<SignalState>>,
    active: &AtomicBool,
    abrupt: bool,
    events: &mut Vec<Value>,
    elapsed: f64,
) -> Result<()> {
    active.store(false, Ordering::Release);
    let result = (|| {
        for signal in SIGNALS {
            let state = &states[&signal];
            let _operation = state.operation()?;
            let (_, before) = durable_barrier_unlocked(client, state)?;
            state.data()?.epoch_stats.push(before);
            {
                let mut server = state.server()?;
                if abrupt {
                    server.kill()?;
                } else {
                    server.stop(Duration::from_secs(30))?;
                }
                server.start(client, ChildLimits::None, Duration::from_secs(30))?;
            }
            let reopened = stats(client, state.target()?)?;
            let expected = state.data()?.accepted;
            let actual = numeric_u64(&reopened, signal.count_field()).unwrap_or_default();
            if actual != expected {
                bail!(
                    "{} cold restart count mismatch: expected {expected}, got {actual}",
                    signal.name()
                );
            }
            semantic_oracle(client, state.target()?)?;
        }
        events.push(json!({
            "elapsed_seconds": elapsed,
            "fault": if abrupt { "sigkill_restart" } else { "graceful_restart" },
            "result": "passed",
        }));
        Ok(())
    })();
    active.store(true, Ordering::Release);
    result
}

fn set_linger_reset(stream: &TcpStream) -> Result<()> {
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    // SAFETY: the stream owns a valid socket descriptor and linger points to
    // a properly initialized value for the duration of the call.
    let result = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            (&linger as *const libc::linger).cast(),
            std::mem::size_of::<libc::linger>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).context("set SO_LINGER")
    }
}

fn slow_and_cancel_storm(
    client: &Client,
    states: &BTreeMap<Signal, Arc<SignalState>>,
    events: &mut Vec<Value>,
    elapsed: f64,
) -> Result<()> {
    use std::io::Write;
    let mut slow = Vec::new();
    for state in states.values() {
        let target = state.target()?;
        for _ in 0..8 {
            let mut stream = TcpStream::connect(("127.0.0.1", target.port))?;
            stream.set_write_timeout(Some(Duration::from_secs(2)))?;
            stream.write_all(
                b"POST /insert/jsonline HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000000\r\nContent-Type: application/x-ndjson\r\n\r\n{}",
            )?;
            slow.push(stream);
        }
        let path = match target.signal {
            Signal::Metrics => "/api/v1/query_range?metric=release_gate_metric&from=2000000000&to=2100000000&step=1&aggregate=p95",
            Signal::Logs => "/select/logsql/query?service=release-gate&limit=10000&order=desc",
            Signal::Traces => "/select/jaeger/api/traces?service=release-gate&limit=10000",
        };
        for _ in 0..16 {
            let mut stream = TcpStream::connect(("127.0.0.1", target.port))?;
            set_linger_reset(&stream)?;
            stream.write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )?;
        }
    }
    thread::sleep(Duration::from_millis(250));
    drop(slow);
    thread::sleep(Duration::from_millis(250));
    for state in states.values() {
        require_status(
            http_request(
                client,
                state.target()?,
                Method::GET,
                "/live",
                None,
                &[],
                Duration::from_secs(60),
            )?,
            200,
            "post-cancellation liveness",
        )?;
    }
    events.push(json!({
        "elapsed_seconds": elapsed,
        "fault": "slow_disconnect_cancellation_storm",
        "result": "passed",
    }));
    Ok(())
}

fn sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut input = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn backup_overlap_probe(
    client: &Client,
    state: &SignalState,
    root: &Path,
    events: &mut Vec<Value>,
    elapsed: f64,
) -> Result<()> {
    let destination = root.join(format!(
        "overlap-{}-{}.db",
        state.signal.name(),
        events.len()
    ));
    let before = state.data()?.accepted;
    let result = require_status(
        http_request(
            client,
            state.target()?,
            Method::POST,
            "/api/v1/backup",
            Some(serde_json::to_vec(&json!({"destination": destination}))?),
            &[("content-type", "application/json")],
            Duration::from_secs(180),
        )?,
        200,
        &format!("{} overlap backup", state.signal.name()),
    )?;
    let report: Value = serde_json::from_slice(&result.body)?;
    let after = state.data()?.accepted;
    let digest = sha256(&destination)?;
    let refused = http_request(
        client,
        state.target()?,
        Method::POST,
        "/api/v1/backup",
        Some(serde_json::to_vec(&json!({"destination": destination}))?),
        &[("content-type", "application/json")],
        Duration::from_secs(180),
    )?;
    if refused.status != 500 || sha256(&destination)? != digest {
        bail!("{} backup no-clobber contract failed", state.signal.name());
    }
    let server = state.server()?;
    let mut cold = Server {
        signal: state.signal,
        binary: server.binary.clone(),
        extension: server.extension.clone(),
        database: destination,
        port: free_port()?,
        log_dir: root.join("cold-backup-logs"),
        short_maintenance: server.short_maintenance,
        child: None,
        generation: 0,
    };
    drop(server);
    cold.start(client, ChildLimits::None, Duration::from_secs(30))?;
    let copied = (|| {
        let current = stats(client, cold.target())?;
        let copied = numeric_u64(&current, state.signal.count_field()).unwrap_or_default();
        if copied < before || copied > after {
            bail!(
                "{} overlap snapshot {copied} outside admission window [{before}, {after}]",
                state.signal.name()
            );
        }
        semantic_oracle(client, cold.target())?;
        Ok(copied)
    })();
    let stop_result = cold.stop(Duration::from_secs(30));
    let copied = copied?;
    stop_result?;
    events.push(json!({
        "elapsed_seconds": elapsed,
        "fault": format!("{}_backup_overlap", state.signal.name()),
        "result": "passed",
        "accepted_before": before,
        "accepted_after": after,
        "snapshot_count": copied,
        "sha256": digest,
        "report": report,
    }));
    Ok(())
}

fn spawn_capture(
    command: &mut Command,
    timeout: Duration,
) -> Result<(std::process::ExitStatus, Vec<u8>)> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let status = match child.wait_timeout(timeout)? {
        Some(status) => status,
        None => {
            child.kill()?;
            let _ = child.wait();
            bail!(
                "child process timed out after {} seconds",
                timeout.as_secs()
            );
        }
    };
    let mut output = Vec::new();
    if let Some(mut stdout) = child.stdout.take() {
        stdout.read_to_end(&mut output)?;
    }
    if let Some(mut stderr) = child.stderr.take() {
        stderr.read_to_end(&mut output)?;
    }
    Ok((status, output))
}

fn address_conflict_probe(state: &SignalState, root: &Path) -> Result<()> {
    let server = state.server()?;
    let database = root.join(format!("address-conflict-{}.db", state.signal.name()));
    let mut command = Command::new(&server.binary);
    command
        .args([
            server.extension.as_os_str(),
            database.as_os_str(),
            format!("127.0.0.1:{}", server.port).as_ref(),
        ])
        .envs(server.environment());
    let (status, output) = spawn_capture(&mut command, Duration::from_secs(30))?;
    let output = String::from_utf8_lossy(&output);
    if status.success() || !output.to_ascii_lowercase().contains("bind") {
        bail!(
            "{} address conflict did not fail closed: status={status}, output={output:?}",
            state.signal.name()
        );
    }
    Ok(())
}

fn expect_start_failure(
    client: &Client,
    state: &SignalState,
    database: PathBuf,
    root: &Path,
    label: &str,
) -> Result<()> {
    let server = state.server()?;
    let mut probe = Server {
        signal: state.signal,
        binary: server.binary.clone(),
        extension: server.extension.clone(),
        database,
        port: free_port()?,
        log_dir: root.join(format!("{label}-logs")),
        short_maintenance: server.short_maintenance,
        child: None,
        generation: 0,
    };
    drop(server);
    match probe.start(client, ChildLimits::None, Duration::from_secs(8)) {
        Err(_) => Ok(()),
        Ok(()) => {
            probe.kill()?;
            bail!(
                "{} {label} storage unexpectedly became ready",
                state.signal.name()
            )
        }
    }
}

fn invalid_storage_probes(client: &Client, state: &SignalState, root: &Path) -> Result<()> {
    let source = root.join(format!("probe-source-{}.db", state.signal.name()));
    require_status(
        http_request(
            client,
            state.target()?,
            Method::POST,
            "/api/v1/backup",
            Some(serde_json::to_vec(&json!({"destination": source}))?),
            &[("content-type", "application/json")],
            Duration::from_secs(180),
        )?,
        200,
        "probe backup",
    )?;
    let corrupt = root.join(format!("corrupt-{}.db", state.signal.name()));
    fs::copy(&source, &corrupt)?;
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut database = File::options().read(true).write(true).open(&corrupt)?;
        database.seek(SeekFrom::Start(0))?;
        database.write_all(b"not-a-sqlite-db!")?;
        database.sync_all()?;
    }
    expect_start_failure(client, state, corrupt, root, "corrupt")?;

    let readonly_root = root.join(format!("readonly-{}", state.signal.name()));
    fs::create_dir(&readonly_root)?;
    let readonly = readonly_root.join("telemetry.db");
    fs::copy(&source, &readonly)?;
    fs::set_permissions(&readonly, fs::Permissions::from_mode(0o444))?;
    fs::set_permissions(&readonly_root, fs::Permissions::from_mode(0o555))?;
    let result = expect_start_failure(client, state, readonly.clone(), root, "read-only");
    fs::set_permissions(&readonly_root, fs::Permissions::from_mode(0o755))?;
    fs::set_permissions(&readonly, fs::Permissions::from_mode(0o644))?;
    result
}

fn descriptor_pressure_probe(client: &Client, state: &SignalState, root: &Path) -> Result<()> {
    let server = state.server()?;
    let mut probe = Server {
        signal: state.signal,
        binary: server.binary.clone(),
        extension: server.extension.clone(),
        database: root.join(format!("descriptor-{}.db", state.signal.name())),
        port: free_port()?,
        log_dir: root.join("descriptor-logs"),
        short_maintenance: server.short_maintenance,
        child: None,
        generation: 0,
    };
    drop(server);
    probe.start(
        client,
        ChildLimits::Descriptors(64),
        Duration::from_secs(30),
    )?;
    let result = (|| {
        let mut connections = Vec::new();
        for _ in 0..96 {
            match TcpStream::connect_timeout(
                &format!("127.0.0.1:{}", probe.port).parse()?,
                Duration::from_millis(200),
            ) {
                Ok(stream) => connections.push(stream),
                Err(_) => break,
            }
        }
        drop(connections);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if probe.child_mut()?.try_wait()?.is_some() {
                bail!("{} exited under descriptor pressure", state.signal.name());
            }
            if http_request(
                client,
                probe.target(),
                Method::GET,
                "/ready",
                None,
                &[],
                Duration::from_secs(1),
            )
            .is_ok_and(|response| response.status == 200)
            {
                break;
            }
            if Instant::now() >= deadline {
                bail!(
                    "{} did not recover from descriptor pressure",
                    state.signal.name()
                );
            }
            thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    })();
    let stop = probe.stop(Duration::from_secs(30));
    result?;
    stop
}

fn disk_full_probe(client: &Client, state: &SignalState, root: &Path) -> Result<Value> {
    let limit_bytes = 1_048_576u64;
    let server = state.server()?;
    let mut probe = Server {
        signal: state.signal,
        binary: server.binary.clone(),
        extension: server.extension.clone(),
        database: root.join(format!("disk-full-{}.db", state.signal.name())),
        port: free_port()?,
        log_dir: root.join("disk-full-logs"),
        short_maintenance: server.short_maintenance,
        child: None,
        generation: 0,
    };
    drop(server);
    probe.start(
        client,
        ChildLimits::FileSize(limit_bytes),
        Duration::from_secs(30),
    )?;
    let batch = match state.signal {
        Signal::Metrics => 4_096,
        Signal::Logs => 2_048,
        Signal::Traces => 1_024,
    };
    let mut accepted = 0u64;
    let mut durable = 0u64;
    let mut failure = String::new();
    for _ in 0..32 {
        if probe.child_mut()?.try_wait()?.is_some() {
            failure = "process_exit".to_owned();
            break;
        }
        let result = ingest_result(client, probe.target(), accepted, batch)?;
        if result.status != state.signal.expected_ingest_status() {
            failure = format!("ingest_http_{}", result.status);
            break;
        }
        accepted += batch as u64;
        let barrier = http_request(
            client,
            probe.target(),
            state.signal.flush_method(),
            "/api/v1/flush",
            None,
            &[],
            Duration::from_secs(60),
        )?;
        if barrier.status != 200 {
            failure = format!("flush_http_{}", barrier.status);
            break;
        }
        let current = stats(client, probe.target())?;
        if numeric_u64(&current, state.signal.count_field()) != Some(accepted) {
            bail!(
                "{} disk-full prefix mismatch before fault",
                state.signal.name()
            );
        }
        durable = accepted;
    }
    if failure.is_empty() {
        probe.kill()?;
        bail!(
            "{} did not reach the 1 MiB file-size fault",
            state.signal.name()
        );
    }
    probe.kill()?;
    let database = probe.database.clone();
    let server = state.server()?;
    let mut reopened = Server {
        signal: state.signal,
        binary: server.binary.clone(),
        extension: server.extension.clone(),
        database,
        port: free_port()?,
        log_dir: root.join("disk-full-reopen-logs"),
        short_maintenance: server.short_maintenance,
        child: None,
        generation: 0,
    };
    drop(server);
    reopened.start(client, ChildLimits::None, Duration::from_secs(30))?;
    let current = stats(client, reopened.target())?;
    let recovered = numeric_u64(&current, state.signal.count_field()).unwrap_or_default();
    let stop = reopened.stop(Duration::from_secs(30));
    if recovered < durable || recovered > accepted {
        bail!(
            "{} disk-full recovery {recovered} outside [{durable}, {accepted}]",
            state.signal.name()
        );
    }
    stop?;
    Ok(json!({
        "limit_bytes": limit_bytes,
        "accepted_before_failure": accepted,
        "durable_before_failure": durable,
        "recovered": recovered,
        "failure": failure,
    }))
}

fn initial_fault_matrix(
    client: &Client,
    states: &BTreeMap<Signal, Arc<SignalState>>,
    root: &Path,
    events: &mut Vec<Value>,
) -> Result<()> {
    for state in states.values() {
        address_conflict_probe(state, root)?;
        invalid_storage_probes(client, state, root)?;
        descriptor_pressure_probe(client, state, root)?;
        let disk = disk_full_probe(client, state, root)?;
        events.push(json!({
            "elapsed_seconds": 0.0,
            "fault": format!("{}_startup_descriptor_disk_faults", state.signal.name()),
            "result": "passed",
            "disk_full": disk,
        }));
    }
    Ok(())
}

fn percentile(values: &[f64], fraction: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    ordered[((ordered.len() - 1) as f64 * fraction) as usize]
}

fn latency_summary(values: &[f64]) -> Value {
    if values.is_empty() {
        return json!({"requests": 0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "mean_ms": 0.0});
    }
    json!({
        "requests": values.len(),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "mean_ms": values.iter().sum::<f64>() / values.len() as f64,
    })
}

fn linear_slope_per_hour(samples: &[(f64, u64)], warmup_seconds: f64) -> f64 {
    let points = samples
        .iter()
        .copied()
        .filter(|point| point.0 >= warmup_seconds)
        .collect::<Vec<_>>();
    if points.len() < 3 {
        return 0.0;
    }
    let mean_x = points.iter().map(|point| point.0).sum::<f64>() / points.len() as f64;
    let mean_y = points.iter().map(|point| point.1 as f64).sum::<f64>() / points.len() as f64;
    let denominator = points
        .iter()
        .map(|point| (point.0 - mean_x).powi(2))
        .sum::<f64>();
    if denominator == 0.0 {
        return 0.0;
    }
    let numerator = points
        .iter()
        .map(|point| (point.0 - mean_x) * (point.1 as f64 - mean_y))
        .sum::<f64>();
    numerator / denominator * 3_600.0
}

fn generation_slopes(samples: &[(u64, f64, u64)]) -> BTreeMap<String, Value> {
    let mut grouped: BTreeMap<u64, Vec<(f64, u64)>> = BTreeMap::new();
    for &(generation, elapsed, rss) in samples {
        grouped.entry(generation).or_default().push((elapsed, rss));
    }
    grouped
        .into_iter()
        .map(|(generation, points)| {
            let first = points.first().map(|point| point.0).unwrap_or_default();
            let span = points.last().map(|point| point.0).unwrap_or(first) - first;
            let warmup = (span * 0.25).min(30.0 * 60.0);
            let relative = points
                .iter()
                .map(|point| (point.0 - first, point.1))
                .collect::<Vec<_>>();
            (
                generation.to_string(),
                json!({
                    "samples": points.len(),
                    "span_seconds": span,
                    "warmup_seconds": warmup,
                    "slope_kib_per_hour": linear_slope_per_hour(&relative, warmup),
                }),
            )
        })
        .collect()
}

fn aggregate_counters(data: &StateData, final_stats: &Value) -> BTreeMap<String, u64> {
    COUNTER_FIELDS
        .iter()
        .filter_map(|key| {
            let total = data
                .epoch_stats
                .iter()
                .chain(std::iter::once(final_stats))
                .filter_map(|stats| numeric_u64(stats, key))
                .sum::<u64>();
            (total > 0).then(|| ((*key).to_owned(), total))
        })
        .collect()
}

fn result_for_state(
    state: &SignalState,
    final_stats: &Value,
    duration_seconds: f64,
) -> Result<Value> {
    let data = state.data()?;
    let physical = numeric_u64(final_stats, "physical_database_bytes")
        .or_else(|| numeric_u64(final_stats, "disk_size"))
        .unwrap_or_default();
    let logical = numeric_u64(final_stats, "bytes_on_disk")
        .or_else(|| numeric_u64(final_stats, "total_bytes"))
        .unwrap_or_default();
    let slopes = generation_slopes(&data.rss_samples);
    let long_slope = slopes
        .values()
        .filter(|value| {
            value
                .get("span_seconds")
                .and_then(Value::as_f64)
                .unwrap_or_default()
                >= 7_200.0
        })
        .filter_map(|value| value.get("slope_kib_per_hour").and_then(Value::as_f64))
        .fold(0.0, f64::max);
    let query_latency = data
        .query_latencies
        .iter()
        .map(|(key, values)| (key.clone(), latency_summary(values)))
        .collect::<Map<_, _>>();
    let generation = state.server()?.generation;
    Ok(json!({
        "accepted_and_durable_records": data.accepted,
        "durable_records_per_second": data.accepted as f64 / duration_seconds,
        "write_latency": latency_summary(&data.write_latencies),
        "query_latency": query_latency,
        "body_and_result_watermarks": {
            "ingest_body_bytes_hwm": data.ingest_body_bytes_hwm,
            "query_response_bytes_hwm": data.query_response_bytes_hwm,
            "query_result_rows_hwm": data.query_result_rows_hwm,
        },
        "rss_hwm_kib": data.memory_hwm_kib,
        "rss_slope_kib_per_hour_after_warmup": long_slope,
        "rss_slope_kib_per_hour_by_process_generation": slopes,
        "logical_storage_bytes": logical,
        "physical_storage_bytes": physical,
        "physical_bytes_per_record": if data.accepted == 0 { 0.0 } else { physical as f64 / data.accepted as f64 },
        "wal_hwm_bytes": data.max_watermarks.get("database_wal_bytes").copied().unwrap_or_default(),
        "resource_watermarks": data.max_watermarks,
        "maintenance_and_fault_counters": aggregate_counters(&data, final_stats),
        "process_generations": generation,
        "errors": data.errors,
        "final_stats": final_stats,
        "samples": data.resource_samples,
    }))
}

fn enforce_gates(config: &Config, signals: &Map<String, Value>) -> Vec<String> {
    let mut failures = Vec::new();
    for signal in SIGNALS {
        let result = &signals[signal.name()];
        if result
            .get("errors")
            .and_then(Value::as_array)
            .is_some_and(|errors| !errors.is_empty())
        {
            failures.push(format!("{}: workload errors", signal.name()));
        }
        if numeric_u64(result, "accepted_and_durable_records").unwrap_or_default() == 0 {
            failures.push(format!("{}: no durable work", signal.name()));
        }
        let wal = numeric_u64(result, "wal_hwm_bytes").unwrap_or_default();
        if wal > config.max_wal_bytes {
            failures.push(format!(
                "{}: WAL HWM {wal} > {}",
                signal.name(),
                config.max_wal_bytes
            ));
        }
        let rss = numeric_u64(result, "rss_hwm_kib").unwrap_or_default();
        if rss > config.max_rss_kib[&signal] {
            failures.push(format!(
                "{}: RSS HWM {rss} KiB > {} KiB",
                signal.name(),
                config.max_rss_kib[&signal]
            ));
        }
        let slope = result
            .get("rss_slope_kib_per_hour_after_warmup")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        if config.duration_seconds >= 7_200.0 && slope > config.max_rss_slope_kib_hour {
            failures.push(format!(
                "{}: RSS slope {slope:.1} KiB/h > {:.1} KiB/h",
                signal.name(),
                config.max_rss_slope_kib_hour
            ));
        }
        if let Some(latencies) = result.get("query_latency").and_then(Value::as_object) {
            for (shape, latency) in latencies {
                let requests = numeric_u64(latency, "requests").unwrap_or_default();
                let p99 = latency
                    .get("p99_ms")
                    .and_then(Value::as_f64)
                    .unwrap_or_default();
                if requests == 0 {
                    failures.push(format!("{}/{shape}: no queries", signal.name()));
                } else if p99 > config.max_p99_ms {
                    failures.push(format!(
                        "{}/{shape}: p99 {p99:.2} ms > {:.2} ms",
                        signal.name(),
                        config.max_p99_ms
                    ));
                }
                let rows = result
                    .pointer(&format!(
                        "/body_and_result_watermarks/query_result_rows_hwm/{shape}"
                    ))
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                if rows == 0 {
                    failures.push(format!(
                        "{}/{shape}: every completed query returned zero rows",
                        signal.name()
                    ));
                }
            }
        }
        let final_stats = &result["final_stats"];
        let capacity = numeric_u64(final_stats, "command_queue_capacity_batches")
            .or_else(|| numeric_u64(final_stats, "command_queue_capacity_requests"))
            .unwrap_or_default();
        let queue_key = if signal == Signal::Traces {
            "queued_requests"
        } else {
            "queued_batches"
        };
        let queued_hwm = result
            .pointer(&format!("/resource_watermarks/{queue_key}"))
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if capacity > 0 && queued_hwm > capacity {
            failures.push(format!(
                "{}: {queue_key} HWM {queued_hwm} exceeds configured capacity {capacity}",
                signal.name()
            ));
        }
        for key in [
            "queued_batches",
            "queued_requests",
            "in_flight_batches",
            "in_flight_requests",
        ] {
            if let Some(value) = numeric_u64(final_stats, key).filter(|value| *value != 0) {
                failures.push(format!("{}: final {key}={value}", signal.name()));
            }
        }
        let counters = result
            .get("maintenance_and_fault_counters")
            .and_then(Value::as_object);
        let expected_backup_errors = u64::from(!config.skip_faults);
        let backup_errors = counters
            .and_then(|values| values.get("backup_errors"))
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if backup_errors != expected_backup_errors {
            failures.push(format!("{}: expected {expected_backup_errors} observed no-clobber backup errors, got {backup_errors}", signal.name()));
        }
        if let Some(counters) = counters {
            for (key, value) in counters {
                if key != "backup_errors"
                    && (key.ends_with("_errors") || key.ends_with("_timeouts"))
                    && value.as_u64().unwrap_or_default() != 0
                {
                    failures.push(format!("{}: {key}={value}", signal.name()));
                }
            }
        }
    }
    failures
}

fn number(value: f64) -> Value {
    Value::Number(Number::from_f64(value).unwrap_or_else(|| Number::from(0)))
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn join_workers(handles: &mut Vec<JoinHandle<()>>, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while handles.iter().any(|handle| !handle.is_finished()) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    if handles.iter().any(|handle| !handle.is_finished()) {
        bail!("one or more production gate workers did not stop");
    }
    while let Some(handle) = handles.pop() {
        handle
            .join()
            .map_err(|_| anyhow!("production gate worker panicked"))?;
    }
    Ok(())
}

fn run_gate(config: &Config, root: &Path, temporary: Option<TempDir>) -> Result<()> {
    if !config.extension.is_file() {
        bail!("missing extension {}", config.extension.display());
    }
    let client = Client::builder().build()?;
    let log_dir = root.join("server-logs");
    let mut states = BTreeMap::new();
    for signal in SIGNALS {
        let binary = config.server_dir.join(signal.binary());
        if !binary.is_file() {
            bail!("missing release binary {}", binary.display());
        }
        states.insert(
            signal,
            Arc::new(SignalState {
                signal,
                batch: config.batch,
                server: Mutex::new(Server {
                    signal,
                    binary,
                    extension: config.extension.clone(),
                    database: root.join(format!("{}.db", signal.name())),
                    port: free_port()?,
                    log_dir: log_dir.clone(),
                    short_maintenance: config.mode == Mode::Short,
                    child: None,
                    generation: 0,
                }),
                operation: Mutex::new(()),
                data: Mutex::new(StateData::default()),
            }),
        );
    }
    let active = Arc::new(AtomicBool::new(true));
    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    let gate_started = Instant::now();
    let mut soak_started = None;
    let mut events = Vec::new();
    let mut report = json!({
        "schema": 1,
        "mode": config.mode.name(),
        "started_at": now(),
        "configured_duration_seconds": config.duration_seconds,
        "configured_aggregate_signal_hours": config.duration_seconds * 3.0 / 3_600.0,
        "write_hz_per_signal": config.write_hz,
        "query_hz_per_signal": config.query_hz,
        "batch_records": config.batch,
        "limits": {
            "max_wal_bytes": config.max_wal_bytes,
            "max_p99_ms": config.max_p99_ms,
            "max_rss_kib": SIGNALS.into_iter().map(|signal| (signal.name(), config.max_rss_kib[&signal])).collect::<BTreeMap<_, _>>(),
            "max_rss_slope_kib_hour": config.max_rss_slope_kib_hour,
        },
    });

    let execution = (|| -> Result<()> {
        for state in states.values() {
            state
                .server()?
                .start(&client, ChildLimits::None, Duration::from_secs(30))?;
            let _operation = state.operation()?;
            write_once(&client, state)?;
            durable_barrier_unlocked(&client, state)?;
        }
        if !config.skip_faults {
            initial_fault_matrix(&client, &states, root, &mut events)?;
        }
        report["preflight_seconds"] = number(gate_started.elapsed().as_secs_f64());
        report["soak_started_at"] = json!(now());
        let started = Instant::now();
        soak_started = Some(started);

        for state in states.values() {
            workers.push(spawn_worker(
                Arc::clone(state),
                client.clone(),
                Arc::clone(&active),
                Arc::clone(&stop),
                Duration::from_secs_f64(1.0 / config.write_hz),
                false,
            ));
            workers.push(spawn_worker(
                Arc::clone(state),
                client.clone(),
                Arc::clone(&active),
                Arc::clone(&stop),
                Duration::from_secs_f64(1.0 / config.query_hz),
                true,
            ));
        }

        let schedule = if config.skip_faults {
            Vec::new()
        } else {
            vec![
                (0.12, "slow"),
                (0.22, "backup_metrics"),
                (0.30, "graceful"),
                (0.42, "backup_logs"),
                (0.52, "abrupt"),
                (0.64, "backup_traces"),
                (0.74, "slow"),
                (0.84, "graceful"),
                (0.92, "abrupt"),
            ]
        };
        let mut next_fault = 0usize;
        let mut next_sample = 0.0;
        while !stop.load(Ordering::Acquire) {
            let elapsed = started.elapsed().as_secs_f64();
            if elapsed >= config.duration_seconds {
                break;
            }
            if elapsed >= next_sample {
                for state in states.values() {
                    sample_state(&client, state, elapsed)?;
                }
                next_sample += config.sample_seconds;
            }
            if let Some(&(fraction, fault)) = schedule.get(next_fault) {
                if elapsed >= config.duration_seconds * fraction {
                    match fault {
                        "slow" => slow_and_cancel_storm(&client, &states, &mut events, elapsed)?,
                        "graceful" => {
                            restart_all(&client, &states, &active, false, &mut events, elapsed)?
                        }
                        "abrupt" => {
                            restart_all(&client, &states, &active, true, &mut events, elapsed)?
                        }
                        _ => {
                            let signal_name =
                                fault.strip_prefix("backup_").context("unknown fault")?;
                            let signal = SIGNALS
                                .into_iter()
                                .find(|signal| signal.name() == signal_name)
                                .context("unknown backup signal")?;
                            backup_overlap_probe(
                                &client,
                                &states[&signal],
                                root,
                                &mut events,
                                elapsed,
                            )?;
                        }
                    }
                    next_fault += 1;
                }
            }
            thread::sleep(Duration::from_millis(100).min(Duration::from_secs_f64(
                (config.duration_seconds - elapsed).max(0.0),
            )));
        }

        stop.store(true, Ordering::Release);
        active.store(true, Ordering::Release);
        join_workers(&mut workers, Duration::from_secs(120))?;
        let elapsed = started.elapsed().as_secs_f64();
        let mut finals = BTreeMap::new();
        let mut barriers = Map::new();
        for signal in SIGNALS {
            let state = &states[&signal];
            let (flush_report, final_stats) = durable_barrier(&client, state)?;
            barriers.insert(signal.name().to_owned(), flush_report);
            finals.insert(signal, final_stats);
            sample_state(&client, state, elapsed)?;
        }
        let signal_results = SIGNALS
            .into_iter()
            .map(|signal| {
                Ok((
                    signal.name().to_owned(),
                    result_for_state(&states[&signal], &finals[&signal], elapsed)?,
                ))
            })
            .collect::<Result<Map<_, _>>>()?;
        report["elapsed_seconds"] = number(elapsed);
        report["elapsed_aggregate_signal_hours"] = number(elapsed * 3.0 / 3_600.0);
        report["finished_at"] = json!(now());
        report["faults"] = Value::Array(events.clone());
        report["final_barriers"] = Value::Object(barriers);
        report["signals"] = Value::Object(signal_results.clone());
        let failures = enforce_gates(config, &signal_results);
        if failures.is_empty() {
            report["verdict"] = json!("passed");
            report["failures"] = json!([]);
            Ok(())
        } else {
            report["verdict"] = json!("failed");
            report["failures"] = serde_json::to_value(&failures)?;
            bail!("{}", failures.join("; "))
        }
    })();

    stop.store(true, Ordering::Release);
    active.store(true, Ordering::Release);
    let join_result = join_workers(&mut workers, Duration::from_secs(5));
    for state in states.values() {
        let _ = state.server().and_then(|mut server| server.kill());
    }
    if let Err(error) = execution.as_ref() {
        report["verdict"] = json!("failed");
        report["fatal_error"] = json!(format!("{error:#}"));
        report["elapsed_seconds"] = number(
            soak_started
                .map(|started| started.elapsed().as_secs_f64())
                .unwrap_or_else(|| gate_started.elapsed().as_secs_f64()),
        );
        if report.get("preflight_seconds").is_none() {
            report["preflight_seconds"] = number(gate_started.elapsed().as_secs_f64());
        }
        report["finished_at"] = json!(now());
        report["faults"] = Value::Array(events);
    }
    if let Some(parent) = config.output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        &config.output,
        format!("{}\n", serde_json::to_string_pretty(&report)?),
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "verdict": report.get("verdict"),
            "output": config.output,
            "elapsed_seconds": report.get("elapsed_seconds"),
            "fatal_error": report.get("fatal_error"),
        }))?
    );
    drop(temporary);
    execution.and(join_result)
}

pub(crate) fn run(root: &Path, args: ProductionArgs) -> Result<()> {
    let config = Config::new(root, args)?;
    let (data_root, temporary) = if let Some(path) = &config.data_dir {
        fs::create_dir(path).with_context(|| {
            format!(
                "create --data-dir {}; it must not already exist",
                path.display()
            )
        })?;
        (path.clone(), None)
    } else {
        let temporary = tempfile::Builder::new()
            .prefix("timeless-production-gate-")
            .tempdir()?;
        (temporary.path().to_path_buf(), Some(temporary))
    };
    run_gate(&config, &data_root, temporary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_matches_the_existing_floor_rank_contract() {
        assert_eq!(percentile(&[], 0.95), 0.0);
        assert_eq!(percentile(&[4.0, 1.0, 3.0, 2.0], 0.50), 2.0);
        assert_eq!(percentile(&[4.0, 1.0, 3.0, 2.0], 0.95), 3.0);
        assert_eq!(percentile(&[4.0, 1.0, 3.0, 2.0], 0.99), 3.0);
    }

    #[test]
    fn fixtures_preserve_batch_counts_and_sentinels() {
        let metrics = String::from_utf8(metrics_body(0, 64)).unwrap();
        assert_eq!(metrics.lines().count(), 4);
        assert!(metrics.contains("release_gate_metric"));
        assert!(metrics.contains("1785628800000"));

        let logs = String::from_utf8(logs_body(0, 64)).unwrap();
        assert_eq!(logs.lines().count(), 64);
        assert!(logs.contains("release-gate-0"));
        assert!(logs.contains("\"nested\":{\"worker\":0}"));

        let traces: Value = serde_json::from_slice(&traces_body(0, 64)).unwrap();
        let spans = traces
            .pointer("/resourceSpans/0/scopeSpans/0/spans")
            .and_then(Value::as_array)
            .unwrap();
        assert_eq!(spans.len(), 64);
        assert_eq!(spans[0]["traceId"], json!(format!("{:032x}", 1)));
        assert_eq!(spans[1]["parentSpanId"], spans[0]["spanId"]);
    }

    #[test]
    fn metrics_export_series_are_not_mistaken_for_point_rows() {
        let mut headers = HeaderMap::new();
        headers.insert("x-timeless-result-rows", HeaderValue::from_static("16"));
        let result = HttpResult {
            status: 200,
            body: br#"{"metric":{"__name__":"release_gate_metric"},"values":[0.5],"timestamps":[1785628800000]}
"#
            .to_vec(),
            headers,
            elapsed_ms: 1.0,
            request_bytes: 0,
        };

        assert_eq!(decode_ndjson(&result).unwrap().len(), 1);
        assert!(validate_ndjson(&result, "log-shaped response").is_err());
    }

    #[test]
    fn slope_is_generation_local_and_warmup_bounded() {
        let samples = vec![
            (1, 0.0, 100),
            (1, 10.0, 110),
            (1, 20.0, 120),
            (1, 30.0, 130),
            (2, 40.0, 80),
            (2, 50.0, 80),
            (2, 60.0, 80),
            (2, 70.0, 80),
        ];
        let slopes = generation_slopes(&samples);
        assert!(slopes["1"]["slope_kib_per_hour"].as_f64().unwrap() > 0.0);
        assert_eq!(slopes["2"]["slope_kib_per_hour"], json!(0.0));
    }
}
