use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use chrono::{SecondsFormat, Utc};
use clap::Args;
use reqwest::blocking::Client;
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection};
use serde::Serialize;
use serde_json::{json, Map, Number, Value};
use tempfile::TempDir;
use wait_timeout::ChildExt;

const TABLE: &str = "traces";
const BATCH_SPANS: usize = 8_192;
const HOUR_NS: i64 = 3_600_000_000_000;
const BASE_NS: i64 = (1_700_000_000_000_000_000 / HOUR_NS) * HOUR_NS;
const BROAD_RESULT_LIMIT: usize = 4_096;

#[derive(Args, Clone, Debug)]
pub(crate) struct TraceBaselineArgs {
    #[arg(long, default_value = "target/release/libtimeless_ext.so")]
    extension: PathBuf,
    #[arg(long, default_value = "servers/target/release/timeless-traces-api")]
    traces_binary: PathBuf,
    #[arg(long, default_value_t = 16)]
    batches: usize,
    #[arg(long, default_value_t = 20)]
    iterations: usize,
    #[arg(long, default_value_t = 3)]
    warmup: usize,
    #[arg(long, required = true)]
    output: PathBuf,
    /// Preserve the temporary database and server log when the run fails.
    #[arg(long, default_value_t = false)]
    retain_on_failure: bool,
    /// Create the fixture with Session 7's bounded attribute-index fields.
    /// The default preserves every earlier trace-baseline invocation.
    #[arg(long, default_value_t = false)]
    attribute_indexes: bool,
}

#[derive(Args, Clone, Debug)]
pub(crate) struct TraceBaselineSqlArgs {
    #[arg(long)]
    extension: PathBuf,
    #[arg(long)]
    database: PathBuf,
    #[arg(long)]
    start_ns: i64,
    #[arg(long)]
    stop_ns: i64,
    #[arg(long, default_value_t = 20)]
    iterations: usize,
    #[arg(long, default_value_t = 3)]
    warmup: usize,
    #[arg(long)]
    expected_spans: usize,
    #[arg(long, default_value_t = false)]
    attribute_indexes: bool,
}

#[derive(Clone)]
struct FixtureSpan {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    name: &'static str,
    kind: u8,
    status: u8,
    start_ts: i64,
    duration_ns: i64,
    attributes: String,
    status_description: &'static str,
    events: String,
    links: String,
    trace_state: &'static str,
    trace_flags: u32,
    dropped_attributes_count: u32,
    dropped_events_count: u32,
    dropped_links_count: u32,
}

#[derive(Debug, Serialize)]
struct BucketRow {
    bucket_ts: i64,
    service: String,
    spans: i64,
    errors: i64,
    dur_sum: i64,
    dur_min: i64,
    dur_max: i64,
    dur_p50: i64,
    dur_p95: i64,
    dur_p99: i64,
}

#[derive(Debug, Serialize)]
struct TraceSummaryRow {
    trace_id: String,
    span_rows: i64,
    distinct_span_ids: i64,
    error_rows: i64,
    start_ts: i64,
    end_ts: i64,
    duration_ns: i64,
    invalid_end_rows: i64,
    root_rows: i64,
    root_span_id: Option<String>,
    root_name: Option<String>,
    root_service: Option<String>,
    root_state: String,
    service_count: i64,
    completeness: String,
}

struct FixtureReport {
    spans: usize,
    stop_ns: i64,
    report: Value,
}

struct RichProbe {
    service: String,
    attributes: String,
    events: String,
    resource: String,
    scope: String,
    links: String,
    trace_state: String,
    trace_flags: i64,
    dropped_attributes: i64,
    dropped_events: i64,
    dropped_links: i64,
    resource_schema_url: String,
    scope_schema_url: String,
    resource_dropped_attributes: i64,
    scope_dropped_attributes: i64,
}

struct TraceServer {
    child: Child,
    base: String,
    log: PathBuf,
    stopped: bool,
}

impl TraceServer {
    fn start(
        binary: &Path,
        extension: &Path,
        database: &Path,
        directory: &Path,
        log_name: &str,
    ) -> Result<Self> {
        let port = std::net::TcpListener::bind(("127.0.0.1", 0))?
            .local_addr()?
            .port();
        let base = format!("http://127.0.0.1:{port}");
        let bind = format!("127.0.0.1:{port}");
        let log = directory.join(log_name);
        let stdout = File::create(&log)?;
        let stderr = stdout.try_clone()?;
        let child = Command::new(binary)
            .args([extension.as_os_str(), database.as_os_str(), bind.as_ref()])
            .env("TIMELESS_AUTH_MODE", "disabled")
            .env("TIMELESS_TRACES_RETENTION_SECS", "0")
            .env("TIMELESS_TRACES_FLUSH_INTERVAL_SECS", "3600")
            .env("TIMELESS_TRACES_OPTIMIZE_INTERVAL_SECS", "3600")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .with_context(|| format!("start {}", binary.display()))?;
        let mut server = Self {
            child,
            base,
            log,
            stopped: false,
        };
        server.wait_ready()?;
        Ok(server)
    }

    fn wait_ready(&mut self) -> Result<()> {
        let client = Client::builder().timeout(Duration::from_secs(1)).build()?;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait()? {
                bail!(
                    "trace server exited during readiness with {status}: {}",
                    fs::read_to_string(&self.log).unwrap_or_default()
                );
            }
            if client
                .get(format!("{}/ready", self.base))
                .send()
                .is_ok_and(|response| response.status().is_success())
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "trace server did not become ready: {}",
                    fs::read_to_string(&self.log).unwrap_or_default()
                );
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn stop(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        self.stopped = true;
        if self.child.try_wait()?.is_none() {
            // SAFETY: this PID belongs to the exact child process above.
            let result = unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
            if result != 0 {
                return Err(std::io::Error::last_os_error()).context("signal trace server");
            }
        }
        let Some(status) = self.child.wait_timeout(Duration::from_secs(30))? else {
            self.child.kill()?;
            let _ = self.child.wait();
            bail!("trace server did not drain after SIGTERM");
        };
        if !status.success() {
            bail!(
                "trace server shutdown {status}: {}",
                fs::read_to_string(&self.log).unwrap_or_default()
            );
        }
        Ok(())
    }
}

impl Drop for TraceServer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub(crate) fn run(root: &Path, args: TraceBaselineArgs) -> Result<()> {
    if args.batches < 16 {
        bail!("trace baseline requires at least 16 authoritative batches");
    }
    if args.iterations == 0 {
        bail!("iterations must be positive");
    }
    require_clean_tracked_worktree(root)?;
    let extension = fs::canonicalize(root.join(&args.extension))
        .with_context(|| format!("missing release extension {}", args.extension.display()))?;
    let binary = fs::canonicalize(root.join(&args.traces_binary)).with_context(|| {
        format!(
            "missing release trace binary {}",
            args.traces_binary.display()
        )
    })?;
    let commit = git_commit(root)?;
    let extension_build = extension_identity(&extension, &commit)?;
    let server_build = binary_identity(&binary, &commit)?;
    let temporary = TempDir::with_prefix("timeless-trace-baseline-")?;
    let database = temporary.path().join("traces.db");

    let execution: Result<Value> = (|| {
        let fixture = build_fixture(&extension, &database, args.batches, args.attribute_indexes)?;
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;

        let exact_trace = format!("{:032x}", 1);
        let (exact, _) = measure_isolated_http(
            &binary,
            &extension,
            &database,
            temporary.path(),
            "exact-server.log",
            &client,
            &format!("/select/jaeger/api/traces/{exact_trace}"),
            1,
            8,
            args.iterations,
            args.warmup,
        )?;
        let (broad_decode_miss, _) = measure_isolated_http(
            &binary,
            &extension,
            &database,
            temporary.path(),
            "broad-miss-server.log",
            &client,
            "/select/jaeger/api/traces?service=bench&minDuration=500000&maxDuration=500000&limit=100",
            0,
            0,
            args.iterations,
            args.warmup,
        )?;
        let (broad_result, final_stats) = measure_isolated_http(
            &binary,
            &extension,
            &database,
            temporary.path(),
            "broad-result-server.log",
            &client,
            &format!(
                "/select/jaeger/api/traces?service=bench&minDuration=1&limit={BROAD_RESULT_LIMIT}"
            ),
            BROAD_RESULT_LIMIT / 8,
            BROAD_RESULT_LIMIT,
            args.iterations,
            args.warmup,
        )?;

        let sql = isolated_sql_baseline(
            root,
            &extension,
            &database,
            BASE_NS,
            fixture.stop_ns,
            fixture.spans,
            args.iterations,
            args.warmup,
            args.attribute_indexes,
        )?;
        let storage_after_queries = storage_files(&database);
        Ok(json!({
            "schema_version": 2,
            "captured_at": Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true),
            "git_commit": commit,
            "branch": current_branch(root)?,
            "host": host_identity(),
            "build": {
                "profile": "release",
                "extension": extension_build,
                "traces_binary": server_build,
            },
            "workload": {
                "authoritative_batch_spans": BATCH_SPANS,
                "batches": args.batches,
                "logical_spans": fixture.spans,
                "time_boxes": args.batches,
                "attribute_indexes": args.attribute_indexes,
                "rich_fields": [
                    "attributes", "status_description", "events", "resource",
                    "instrumentation_scope", "links", "trace_state", "trace_flags",
                    "dropped_attributes_count", "dropped_events_count",
                    "dropped_links_count", "resource_schema_url", "scope_schema_url",
                    "resource_dropped_attributes_count", "scope_dropped_attributes_count"
                ],
                "duration_values_ns": [100_000, 900_000],
                "iterations": args.iterations,
                "warmup": args.warmup,
                "single_client": true,
                "loopback_http": true,
            },
            "fixture": fixture.report,
            "jaeger": {
                "exact_trace_control": exact,
                "broad_full_decode_miss": broad_decode_miss,
                "broad_result_4096_spans": broad_result,
                "process_isolation": "each shape runs in a fresh trace-server process",
                "final_storage_stats": select_fields(&final_stats, &[
                    "blocks", "raw_blocks", "compressed_blocks", "total_spans", "bytes_on_disk",
                    "database_file_bytes", "database_wal_bytes", "database_shm_bytes",
                    "physical_database_bytes", "sqlite_page_bytes", "sqlite_index_bytes",
                    "freelist_pages", "freelist_bytes", "attribute_index_fields",
                    "attribute_bloom_rows", "attribute_bloom_bytes"
                ]),
            },
            "direct_sql": sql,
            "storage_after_queries": storage_after_queries,
        }))
    })();

    let evidence = match execution {
        Ok(value) => value,
        Err(error) if args.retain_on_failure => {
            let retained = temporary.keep();
            return Err(error.context(format!(
                "failed trace baseline retained at {}",
                retained.display()
            )));
        }
        Err(error) => return Err(error),
    };
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

pub(crate) fn run_sql(args: TraceBaselineSqlArgs) -> Result<()> {
    if args.iterations == 0 || args.expected_spans == 0 {
        bail!("iterations and expected spans must be positive");
    }
    let connection = open(&args.database, &args.extension)?;
    let count: i64 = connection.query_row("SELECT count(*) FROM traces", [], |row| row.get(0))?;
    ensure!(
        count == args.expected_spans as i64,
        "direct-SQL fixture count changed"
    );
    let broad = measure_buckets(
        &connection,
        None,
        args.start_ns,
        args.stop_ns,
        HOUR_NS,
        args.expected_spans,
        args.iterations,
        args.warmup,
    )?;
    let narrow_stop = args.start_ns + (BATCH_SPANS as i64 - 1) * 1_000;
    let narrow = measure_buckets(
        &connection,
        Some("bench"),
        args.start_ns,
        narrow_stop,
        HOUR_NS,
        BATCH_SPANS,
        args.iterations,
        args.warmup,
    )?;
    let posting_windows = json!({
        "narrow_one_time_box": measure_posting_window(
            &connection,
            args.start_ns,
            1,
            args.iterations,
            args.warmup,
        )?,
        "wide_four_time_boxes": measure_posting_window(
            &connection,
            args.start_ns,
            4,
            args.iterations,
            args.warmup,
        )?,
    });
    let retained_trace_summaries = json!({
        "two_scan_control": {
            "exact_one_trace": measure_trace_summaries(
                &connection,
                Some(&fixed_be::<16>(1)),
                1,
                8,
                false,
                args.iterations,
                args.warmup,
            )?,
            "broad_all_traces": measure_trace_summaries(
                &connection,
                None,
                args.expected_spans / 8,
                args.expected_spans,
                false,
                args.iterations,
                args.warmup,
            )?,
        },
        "single_scan": {
            "exact_one_trace": measure_trace_summaries(
                &connection,
                Some(&fixed_be::<16>(1)),
                1,
                8,
                true,
                args.iterations,
                args.warmup,
            )?,
            "broad_all_traces": measure_trace_summaries(
                &connection,
                None,
                args.expected_spans / 8,
                args.expected_spans,
                true,
                args.iterations,
                args.warmup,
            )?,
        },
    });
    let attribute_equality = measure_attribute_equality(
        &connection,
        args.start_ns,
        args.stop_ns,
        args.expected_spans,
        args.attribute_indexes,
        args.iterations,
        args.warmup,
    )?;
    let report = json!({
        "process_isolation": "fresh child; fixture generation and HTTP response allocation excluded",
        "broad_all_time_boxes": broad,
        "narrow_one_time_box_control": narrow,
        "posting_windows": posting_windows,
        "retained_trace_summaries": retained_trace_summaries,
        "attribute_equality": attribute_equality,
        "rss": process_memory(std::process::id())?,
    });
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

#[derive(Clone, Copy)]
enum AttributeShape {
    ExactCount,
    BooleanTrue,
}

impl AttributeShape {
    fn name(self) -> &'static str {
        match self {
            Self::ExactCount => "exact_typed_high_cardinality",
            Self::BooleanTrue => "exact_typed_low_cardinality",
        }
    }

    fn control_sql(self) -> &'static str {
        match self {
            Self::ExactCount => {
                "SELECT lower(hex(trace_id)),lower(hex(span_id)),start_ts FROM traces \
                 WHERE start_ts>=?1 AND start_ts<=?2 \
                   AND json_type(attributes,'$.count')='integer' \
                   AND attributes->'$.count'=?3 \
                 ORDER BY start_ts,span_id"
            }
            Self::BooleanTrue => {
                "SELECT count(*) FROM traces WHERE start_ts>=?1 AND start_ts<=?2 \
                   AND json_type(attributes,'$.bool')='true'"
            }
        }
    }

    fn candidate_sql(self) -> &'static str {
        match self {
            Self::ExactCount => {
                "SELECT lower(hex(trace_id)),lower(hex(span_id)),start_ts FROM traces \
                 WHERE start_ts>=?1 AND start_ts<=?2 AND attribute_filter=?3 \
                 ORDER BY start_ts,span_id"
            }
            Self::BooleanTrue => {
                "SELECT count(*) FROM traces WHERE start_ts>=?1 AND start_ts<=?2 \
                   AND attribute_filter=?3"
            }
        }
    }

    fn candidate_filter(self, target: usize) -> String {
        match self {
            Self::ExactCount => {
                format!(r#"{{"scope":"span","path":"/count","value":{target}}}"#)
            }
            Self::BooleanTrue => {
                r#"{"scope":"span","path":"/bool","value":true}"#.to_owned()
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn measure_attribute_equality(
    connection: &Connection,
    start: i64,
    stop: i64,
    expected_spans: usize,
    candidate_available: bool,
    iterations: usize,
    warmup: usize,
) -> Result<Value> {
    let narrow_stop = start + (BATCH_SPANS as i64 - 1) * 1_000;
    let cases = [
        ("narrow_one_time_box", start, narrow_stop, 123_usize),
        ("wide_all_time_boxes", start, stop, expected_spans / 2),
    ];
    let mut control = Map::new();
    let mut candidate = Map::new();
    for shape in [AttributeShape::ExactCount, AttributeShape::BooleanTrue] {
        let mut control_windows = Map::new();
        let mut candidate_windows = Map::new();
        for (window, lower, upper, target) in cases {
            let expected_rows = match shape {
                AttributeShape::ExactCount => 1,
                AttributeShape::BooleanTrue => {
                    if window == "narrow_one_time_box" {
                        BATCH_SPANS
                    } else {
                        expected_spans
                    }
                }
            };
            control_windows.insert(
                window.to_owned(),
                measure_attribute_shape(
                    connection,
                    shape,
                    false,
                    lower,
                    upper,
                    target,
                    expected_rows,
                    iterations,
                    warmup,
                )?,
            );
            if candidate_available {
                candidate_windows.insert(
                    window.to_owned(),
                    measure_attribute_shape(
                        connection,
                        shape,
                        true,
                        lower,
                        upper,
                        target,
                        expected_rows,
                        iterations,
                        warmup,
                    )?,
                );
            }
        }
        control.insert(shape.name().to_owned(), Value::Object(control_windows));
        if candidate_available {
            candidate.insert(shape.name().to_owned(), Value::Object(candidate_windows));
        }
    }
    Ok(json!({
        "control": control,
        "candidate": if candidate_available { Value::Object(candidate) } else { Value::Null },
        "candidate_available": candidate_available,
        "typed_semantics": "count uses JSON integer equality; bool uses JSON true; missing/null/string values do not match",
    }))
}

#[allow(clippy::too_many_arguments)]
fn measure_attribute_shape(
    connection: &Connection,
    shape: AttributeShape,
    candidate: bool,
    start: i64,
    stop: i64,
    target: usize,
    expected_rows: usize,
    iterations: usize,
    warmup: usize,
) -> Result<Value> {
    let sql = if candidate {
        shape.candidate_sql()
    } else {
        shape.control_sql()
    };
    let operand = if candidate {
        shape.candidate_filter(target)
    } else {
        target.to_string()
    };
    let execute = |connection: &Connection| -> Result<(usize, usize)> {
        match shape {
            AttributeShape::ExactCount => {
                let mut statement = connection.prepare_cached(sql)?;
                let rows = statement
                    .query_map(params![start, stop, operand], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((rows.len(), serde_json::to_vec(&rows)?.len()))
            }
            AttributeShape::BooleanTrue => {
                let count: i64 = if candidate {
                    connection.query_row(sql, params![start, stop, operand], |row| row.get(0))?
                } else {
                    connection.query_row(sql, params![start, stop], |row| row.get(0))?
                };
                let count = usize::try_from(count).context("negative attribute row count")?;
                Ok((count, serde_json::to_vec(&count)?.len()))
            }
        }
    };
    for _ in 0..warmup {
        ensure!(execute(connection)?.0 == expected_rows);
    }
    let before = sqlite_stats(connection)?;
    let mut elapsed = Vec::with_capacity(iterations);
    let mut result_bytes = BTreeSet::new();
    for _ in 0..iterations {
        let started = Instant::now();
        let (rows, bytes) = execute(connection)?;
        elapsed.push(started.elapsed().as_nanos());
        ensure!(rows == expected_rows);
        result_bytes.insert(bytes);
    }
    let after = sqlite_stats(connection)?;
    ensure!(result_bytes.len() == 1);
    Ok(json!({
        "implementation": if candidate { "configured block filter plus exact extension recheck" } else { "public virtual table plus SQLite JSON1" },
        "sql": sql,
        "filter": candidate.then_some(operand),
        "iterations": iterations,
        "warmup": warmup,
        "latency_ns": latency_summary(&elapsed),
        "result_rows": expected_rows,
        "result_json_bytes": result_bytes.first(),
        "extension_work_delta": prefix_numeric_delta(&before, &after, "query_"),
    }))
}

#[derive(Clone, Copy)]
enum PostingShape {
    Service,
    Name,
    Kind,
    Status,
}

impl PostingShape {
    fn name(self) -> &'static str {
        match self {
            Self::Service => "service",
            Self::Name => "name",
            Self::Kind => "kind",
            Self::Status => "status",
        }
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Service => {
                "SELECT count(*) FROM traces \
                 WHERE service='bench' AND start_ts>=?1 AND start_ts<=?2"
            }
            Self::Name => {
                "SELECT count(*) FROM traces \
                 WHERE name='GET /baseline' AND start_ts>=?1 AND start_ts<=?2"
            }
            Self::Kind => {
                "SELECT count(*) FROM traces \
                 WHERE kind='internal' AND start_ts>=?1 AND start_ts<=?2"
            }
            Self::Status => {
                "SELECT count(*) FROM traces \
                 WHERE status='error' AND start_ts>=?1 AND start_ts<=?2"
            }
        }
    }

    fn matches(self, global: usize) -> bool {
        match self {
            Self::Service => true,
            Self::Name => global.is_multiple_of(8),
            Self::Kind => global.is_multiple_of(5),
            Self::Status => global % 3 == 2,
        }
    }

    fn candidate_blocks_per_box(self) -> usize {
        match self {
            // Flush and optimize retain one block per status partition. Each
            // of these terms occurs in all three partitions in every box.
            Self::Service | Self::Name | Self::Kind => 3,
            // A status term names exactly its one status-pure partition.
            Self::Status => 1,
        }
    }
}

fn measure_posting_window(
    connection: &Connection,
    start: i64,
    boxes: usize,
    iterations: usize,
    warmup: usize,
) -> Result<Value> {
    let stop = start + (boxes as i64 - 1) * HOUR_NS + (BATCH_SPANS as i64 - 1) * 1_000;
    let spans = boxes * BATCH_SPANS;
    let fixture_blocks = stat_i64(&sqlite_stats(connection)?, "blocks")?;
    let mut shapes = Map::new();
    for shape in [
        PostingShape::Service,
        PostingShape::Name,
        PostingShape::Kind,
        PostingShape::Status,
    ] {
        let expected_rows = (0..spans).filter(|global| shape.matches(*global)).count();
        let expected_candidates = boxes * shape.candidate_blocks_per_box();
        let expected_decoded = if matches!(shape, PostingShape::Status) {
            expected_rows
        } else {
            spans
        };
        shapes.insert(
            shape.name().into(),
            measure_posting_count(
                connection,
                shape,
                start,
                stop,
                expected_rows,
                expected_candidates,
                expected_decoded,
                iterations,
                warmup,
            )?,
        );
    }
    Ok(json!({
        "start_ns": start,
        "stop_ns": stop,
        "time_boxes": boxes,
        "fixture_blocks": fixture_blocks,
        "shapes": shapes,
    }))
}

#[allow(clippy::too_many_arguments)]
fn measure_posting_count(
    connection: &Connection,
    shape: PostingShape,
    start: i64,
    stop: i64,
    expected_rows: usize,
    expected_candidates: usize,
    expected_decoded: usize,
    iterations: usize,
    warmup: usize,
) -> Result<Value> {
    let mut statement = connection.prepare_cached(shape.sql())?;
    for _ in 0..warmup {
        let rows: i64 = statement.query_row(params![start, stop], |row| row.get(0))?;
        ensure!(rows == expected_rows as i64);
    }
    let before = sqlite_stats(connection)?;
    let mut elapsed = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let rows: i64 = statement.query_row(params![start, stop], |row| row.get(0))?;
        elapsed.push(started.elapsed().as_nanos());
        ensure!(rows == expected_rows as i64);
    }
    let after = sqlite_stats(connection)?;
    let work = prefix_numeric_delta(&before, &after, "query_");
    let actual = |key: &str| -> Result<i64> {
        work.get(key)
            .and_then(Value::as_i64)
            .with_context(|| format!("posting profile omitted {key}"))
    };
    ensure!(actual("query_count")? == iterations as i64);
    ensure!(
        actual("query_candidate_blocks")? == (expected_candidates * iterations) as i64,
        "{} posting query considered time-disjoint blocks",
        shape.name()
    );
    ensure!(actual("query_payload_blocks_read")? == (expected_candidates * iterations) as i64);
    ensure!(actual("query_decoded_spans")? == (expected_decoded * iterations) as i64);
    ensure!(actual("query_matched_spans")? == (expected_rows * iterations) as i64);
    ensure!(actual("query_returned_spans")? == (expected_rows * iterations) as i64);
    Ok(json!({
        "sql": shape.sql(),
        "iterations": iterations,
        "warmup": warmup,
        "expected_rows": expected_rows,
        "expected_candidate_blocks_per_query": expected_candidates,
        "expected_decoded_spans_per_query": expected_decoded,
        "latency_ns": latency_summary(&elapsed),
        "extension_work_delta": work,
    }))
}

fn trace_summary_prefix(exact_trace: bool) -> String {
    let predicate = if exact_trace {
        " WHERE trace_id=?1"
    } else {
        ""
    };
    format!(
        "WITH retained AS (\
           SELECT trace_id,span_id,parent_span_id,name,service,status,start_ts,duration_ns,\
                  CASE WHEN duration_ns>=0 \
                             AND start_ts<=9223372036854775807-duration_ns \
                       THEN start_ts+duration_ns END AS valid_end_ts \
             FROM traces{predicate}\
         )"
    )
}

fn trace_summary_single_scan_sql(exact_trace: bool) -> String {
    format!(
        "{} \
         SELECT lower(hex(trace_id)),count(*) AS span_rows,\
                count(DISTINCT span_id) AS distinct_span_ids,\
                count(*) FILTER (WHERE status='error') AS error_rows,\
                min(start_ts) AS start_ts,max(valid_end_ts) AS end_ts,\
                CASE WHEN count(*) FILTER (WHERE valid_end_ts IS NULL)<>0 THEN NULL \
                     WHEN min(start_ts)>=0 THEN max(valid_end_ts)-min(start_ts) \
                     WHEN max(valid_end_ts)<=9223372036854775807+min(start_ts) \
                       THEN max(valid_end_ts)-min(start_ts) END AS duration_ns,\
                count(*) FILTER (WHERE valid_end_ts IS NULL) AS invalid_end_rows,\
                count(*) FILTER (WHERE parent_span_id IS NULL) AS root_rows,\
                CASE WHEN count(*) FILTER (WHERE parent_span_id IS NULL)=1 \
                     THEN lower(hex(min(span_id) FILTER (WHERE parent_span_id IS NULL))) END,\
                CASE WHEN count(*) FILTER (WHERE parent_span_id IS NULL)=1 \
                     THEN min(name) FILTER (WHERE parent_span_id IS NULL) END,\
                CASE WHEN count(*) FILTER (WHERE parent_span_id IS NULL)=1 \
                     THEN min(service) FILTER (WHERE parent_span_id IS NULL) END,\
                CASE count(*) FILTER (WHERE parent_span_id IS NULL) WHEN 0 THEN 'missing' \
                     WHEN 1 THEN 'unique' ELSE 'ambiguous' END AS root_state,\
                count(DISTINCT service) AS service_count,'unknown' AS completeness \
           FROM retained GROUP BY trace_id ORDER BY trace_id",
        trace_summary_prefix(exact_trace)
    )
}

fn trace_summary_two_scan_sql(exact_trace: bool) -> String {
    format!(
        "{}, totals AS (\
           SELECT trace_id,count(*) AS span_rows,\
                  count(DISTINCT span_id) AS distinct_span_ids,\
                  count(*) FILTER (WHERE status='error') AS error_rows,\
                  min(start_ts) AS start_ts,max(valid_end_ts) AS end_ts,\
                  count(*) FILTER (WHERE valid_end_ts IS NULL) AS invalid_end_rows,\
                  count(DISTINCT service) AS service_count \
             FROM retained GROUP BY trace_id\
         ), roots AS (\
           SELECT trace_id,count(*) AS root_rows,\
                  CASE WHEN count(*)=1 THEN lower(hex(min(span_id))) END AS root_span_id,\
                  CASE WHEN count(*)=1 THEN min(name) END AS root_name,\
                  CASE WHEN count(*)=1 THEN min(service) END AS root_service \
             FROM retained WHERE parent_span_id IS NULL GROUP BY trace_id\
         ) \
         SELECT lower(hex(totals.trace_id)),totals.span_rows,\
                totals.distinct_span_ids,totals.error_rows,totals.start_ts,totals.end_ts,\
                CASE WHEN totals.invalid_end_rows<>0 THEN NULL \
                     WHEN totals.start_ts>=0 THEN totals.end_ts-totals.start_ts \
                     WHEN totals.end_ts<=9223372036854775807+totals.start_ts \
                       THEN totals.end_ts-totals.start_ts END AS duration_ns,\
                totals.invalid_end_rows,coalesce(roots.root_rows,0),\
                roots.root_span_id,roots.root_name,roots.root_service,\
                CASE coalesce(roots.root_rows,0) WHEN 0 THEN 'missing' \
                     WHEN 1 THEN 'unique' ELSE 'ambiguous' END AS root_state,\
                totals.service_count,'unknown' AS completeness \
           FROM totals LEFT JOIN roots USING(trace_id) ORDER BY totals.trace_id",
        trace_summary_prefix(exact_trace)
    )
}

fn execute_trace_summaries(
    connection: &Connection,
    trace_id: Option<&[u8; 16]>,
    single_scan: bool,
) -> Result<Vec<TraceSummaryRow>> {
    let sql = if single_scan {
        trace_summary_single_scan_sql(trace_id.is_some())
    } else {
        trace_summary_two_scan_sql(trace_id.is_some())
    };
    let mut statement = connection.prepare_cached(&sql)?;
    let map = |row: &rusqlite::Row<'_>| {
        Ok(TraceSummaryRow {
            trace_id: row.get(0)?,
            span_rows: row.get(1)?,
            distinct_span_ids: row.get(2)?,
            error_rows: row.get(3)?,
            start_ts: row.get(4)?,
            end_ts: row.get(5)?,
            duration_ns: row.get(6)?,
            invalid_end_rows: row.get(7)?,
            root_rows: row.get(8)?,
            root_span_id: row.get(9)?,
            root_name: row.get(10)?,
            root_service: row.get(11)?,
            root_state: row.get(12)?,
            service_count: row.get(13)?,
            completeness: row.get(14)?,
        })
    };
    let rows = match trace_id {
        Some(trace_id) => statement.query_map([trace_id.as_slice()], map)?,
        None => statement.query_map([], map)?,
    };
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn require_trace_summaries(
    rows: &[TraceSummaryRow],
    expected_rows: usize,
    expected_spans: usize,
) -> Result<()> {
    ensure!(
        rows.len() == expected_rows,
        "trace summary rows {}, expected {expected_rows}",
        rows.len()
    );
    let spans = rows.iter().try_fold(0_i64, |total, row| {
        total
            .checked_add(row.span_rows)
            .context("trace summary span count overflow")
    })?;
    ensure!(spans == expected_spans as i64);
    ensure!(rows.iter().all(|row| {
        row.span_rows == 8
            && row.distinct_span_ids == 8
            && row.invalid_end_rows == 0
            && row.root_rows == 1
            && row.root_state == "unique"
            && row.service_count == 1
            && row.completeness == "unknown"
            && row.root_span_id.is_some()
            && row.root_name.as_deref() == Some("GET /baseline")
            && row.root_service.as_deref() == Some("bench")
            && row.end_ts >= row.start_ts
            && row.duration_ns == row.end_ts - row.start_ts
            && (row.error_rows == 2 || row.error_rows == 3)
    }));
    Ok(())
}

fn measure_trace_summaries(
    connection: &Connection,
    trace_id: Option<&[u8; 16]>,
    expected_rows: usize,
    expected_spans: usize,
    single_scan: bool,
    iterations: usize,
    warmup: usize,
) -> Result<Value> {
    for _ in 0..warmup {
        let rows = execute_trace_summaries(connection, trace_id, single_scan)?;
        require_trace_summaries(&rows, expected_rows, expected_spans)?;
    }
    let before = sqlite_stats(connection)?;
    let mut elapsed = Vec::with_capacity(iterations);
    let mut cardinality = BTreeSet::new();
    let mut result_bytes = BTreeSet::new();
    for _ in 0..iterations {
        let started = Instant::now();
        let rows = execute_trace_summaries(connection, trace_id, single_scan)?;
        elapsed.push(started.elapsed().as_nanos());
        require_trace_summaries(&rows, expected_rows, expected_spans)?;
        cardinality.insert(rows.len());
        result_bytes.insert(serde_json::to_vec(&rows)?.len());
    }
    let after = sqlite_stats(connection)?;
    ensure!(cardinality.len() == 1 && result_bytes.len() == 1);
    let sql = if single_scan {
        trace_summary_single_scan_sql(trace_id.is_some())
    } else {
        trace_summary_two_scan_sql(trace_id.is_some())
    };
    Ok(json!({
        "implementation": if single_scan { "single conditional aggregate scan" } else { "two CTE consumers control" },
        "sql": sql,
        "trace_id": trace_id.map(|value| format!("{:032x}", u128::from_be_bytes(*value))),
        "iterations": iterations,
        "warmup": warmup,
        "latency_ns": latency_summary(&elapsed),
        "result_rows": cardinality.first(),
        "result_spans": expected_spans,
        "result_json_bytes": result_bytes.first(),
        "extension_work_delta": prefix_numeric_delta(&before, &after, "query_"),
    }))
}

fn build_fixture(
    extension: &Path,
    database: &Path,
    batches: usize,
    attribute_indexes: bool,
) -> Result<FixtureReport> {
    let connection = open(database, extension)?;
    let journal_mode: String =
        connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    let create = if attribute_indexes {
        "CREATE VIRTUAL TABLE traces USING timeless_traces(\
           attribute_indexes='[{\"scope\":\"span\",\"path\":\"/count\"},{\"scope\":\"span\",\"path\":\"/bool\"}]'\
         )"
    } else {
        "CREATE VIRTUAL TABLE traces USING timeless_traces"
    };
    connection.execute(create, [])?;
    let started = Instant::now();
    let mut public_batch_bytes = 0_u64;
    for batch_number in 0..batches {
        let blob = rich_batch(batch_number);
        public_batch_bytes += blob.len() as u64;
        connection.execute("INSERT INTO traces(traces) VALUES (?1)", params![blob])?;
    }
    let insert_ns = started.elapsed().as_nanos();
    let before_optimize = sqlite_stats(&connection)?;
    let storage_after_insert = storage_files(database);
    let optimize_started = Instant::now();
    connection.execute("INSERT INTO traces(traces) VALUES ('optimize')", [])?;
    let optimize_ns = optimize_started.elapsed().as_nanos();
    let after_optimize = sqlite_stats(&connection)?;
    let storage_after_optimize = storage_files(database);
    let raw_blocks = stat_i64(&after_optimize, "raw_blocks")?;
    ensure!(
        raw_blocks == 0,
        "fixture optimize left {raw_blocks} raw blocks"
    );
    let checkpoint_started = Instant::now();
    let checkpoint: (i64, i64, i64) =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    let checkpoint_ns = checkpoint_started.elapsed().as_nanos();
    drop(connection);

    let connection = open(database, extension)?;
    let spans = batches * BATCH_SPANS;
    let stop_ns = BASE_NS + (batches as i64 - 1) * HOUR_NS + (BATCH_SPANS as i64 - 1) * 1_000;
    let (count, minimum, maximum): (i64, i64, i64) = connection.query_row(
        "SELECT count(*), min(start_ts), max(start_ts) FROM traces",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    ensure!(
        count == spans as i64,
        "fixture count {count}, expected {spans}"
    );
    ensure!(
        minimum == BASE_NS && maximum == stop_ns,
        "fixture timestamp range changed"
    );
    let rich: RichProbe = connection.query_row(
        "SELECT service,attributes,events,resource,instrumentation_scope,links,trace_state,trace_flags, \
                dropped_attributes_count,dropped_events_count,dropped_links_count,resource_schema_url, \
                scope_schema_url,resource_dropped_attributes_count,scope_dropped_attributes_count \
           FROM traces ORDER BY start_ts LIMIT 1",
        [],
        |row| {
            Ok(RichProbe {
                service: row.get(0)?,
                attributes: row.get(1)?,
                events: row.get(2)?,
                resource: row.get(3)?,
                scope: row.get(4)?,
                links: row.get(5)?,
                trace_state: row.get(6)?,
                trace_flags: row.get(7)?,
                dropped_attributes: row.get(8)?,
                dropped_events: row.get(9)?,
                dropped_links: row.get(10)?,
                resource_schema_url: row.get(11)?,
                scope_schema_url: row.get(12)?,
                resource_dropped_attributes: row.get(13)?,
                scope_dropped_attributes: row.get(14)?,
            })
        },
    )?;
    ensure!(
        rich.service == "bench",
        "service.name precedence was not preserved"
    );
    ensure!(serde_json::from_str::<Value>(&rich.attributes)?["nested"]["unicode"] == "空🔥");
    ensure!(serde_json::from_str::<Value>(&rich.events)?[0]["name"] == "exception");
    ensure!(serde_json::from_str::<Value>(&rich.resource)?["deployment.environment"] == "baseline");
    ensure!(serde_json::from_str::<Value>(&rich.scope)?["name"] == "trace-baseline");
    ensure!(serde_json::from_str::<Value>(&rich.scope)?["attributes"]["debug"] == false);
    ensure!(serde_json::from_str::<Value>(&rich.links)?[0]["attributes"]["reason"] == "baseline");
    ensure!(rich.trace_state == "bench=root" && rich.trace_flags == i64::from(u32::MAX));
    ensure!(
        (
            rich.dropped_attributes,
            rich.dropped_events,
            rich.dropped_links
        ) == (1, 2, 3)
    );
    ensure!(rich.resource_schema_url == "https://example.test/resource/1");
    ensure!(rich.scope_schema_url == "https://example.test/scope/2");
    ensure!(
        (
            rich.resource_dropped_attributes,
            rich.scope_dropped_attributes
        ) == (4, 5)
    );
    let reopen_stats = sqlite_stats(&connection)?;
    let builder_process_memory = process_memory(std::process::id())?;
    drop(connection);
    let mut before_storage = select_fields(
        &before_optimize,
        &[
            "blocks",
            "raw_blocks",
            "buffered_spans",
            "total_spans",
            "bytes_on_disk",
        ],
    );
    before_storage
        .as_object_mut()
        .expect("selected stats are an object")
        .insert(
            "compressed_blocks".to_owned(),
            json!(
                stat_i64(&before_optimize, "blocks")? - stat_i64(&before_optimize, "raw_blocks")?
            ),
        );
    let mut reopen_storage = select_fields(
        &reopen_stats,
        &[
            "blocks",
            "raw_blocks",
            "buffered_spans",
            "total_spans",
            "bytes_on_disk",
            "terms",
            "trace_index_rows",
            "index_bytes",
            "duration_bounded_blocks",
            "duration_unknown_blocks",
            "attribute_index_fields",
            "attribute_bloom_rows",
            "attribute_bloom_bytes",
        ],
    );
    reopen_storage
        .as_object_mut()
        .expect("selected stats are an object")
        .insert(
            "compressed_blocks".to_owned(),
            json!(stat_i64(&reopen_stats, "blocks")? - stat_i64(&reopen_stats, "raw_blocks")?),
        );

    Ok(FixtureReport {
        spans,
        stop_ns,
        report: json!({
            "journal_mode": journal_mode,
            "public_batch_bytes": public_batch_bytes,
            "insert_ns": insert_ns,
            "durable_spans_per_second": spans as f64 / (insert_ns as f64 / 1_000_000_000.0),
            "optimize_ns": optimize_ns,
            "checkpoint": {
                "busy": checkpoint.0,
                "log_frames": checkpoint.1,
                "checkpointed_frames": checkpoint.2,
                "elapsed_ns": checkpoint_ns
            },
            "stats_before_optimize": before_storage,
            "stats_after_reopen": reopen_storage,
            "storage_after_insert_before_optimize": storage_after_insert,
            "storage_after_optimize_before_checkpoint": storage_after_optimize,
            "storage_after_checkpoint": storage_files(database),
            "builder_process_memory": builder_process_memory,
            "exact_reopen_count": count,
            "timestamp_range_ns": [minimum, maximum],
            "rich_span_probe": true,
        }),
    })
}

fn rich_batch(batch_number: usize) -> Vec<u8> {
    let first = batch_number * BATCH_SPANS;
    let spans = (0..BATCH_SPANS)
        .map(|offset| {
            let global = first + offset;
            let trace_number = global / 8 + 1;
            let span_number = global + 1;
            let root_number = global - global % 8 + 1;
            let start_ts =
                BASE_NS + batch_number as i64 * HOUR_NS + offset as i64 * 1_000;
            FixtureSpan {
                trace_id: fixed_be(trace_number as u64),
                span_id: fixed_be(span_number as u64),
                parent_span_id: (!global.is_multiple_of(8))
                    .then(|| fixed_be(root_number as u64)),
                name: if global.is_multiple_of(8) {
                    "GET /baseline"
                } else {
                    "db.query"
                },
                kind: (global % 5) as u8,
                status: (global % 3) as u8,
                start_ts,
                duration_ns: fixture_duration(offset),
                attributes: format!(
                    "{{\"array\":[1,\"two\",false,null],\"bool\":true,\"count\":{global},\"nested\":{{\"ratio\":1.25,\"unicode\":\"空🔥\"}},\"service.name\":\"bench\"}}"
                ),
                status_description: if global % 3 == 2 {
                    "baseline failure 🚀"
                } else {
                    ""
                },
                events: format!(
                    "[{{\"attributes\":{{\"attempt\":{},\"fatal\":false}},\"dropped_attributes_count\":{},\"name\":\"exception\",\"timestamp\":{}}}]",
                    global % 5,
                    global % 3,
                    start_ts + 50_000
                ),
                links: if global.is_multiple_of(8) {
                    format!(
                        "[{{\"attributes\":{{\"reason\":\"baseline\"}},\"dropped_attributes_count\":6,\"flags\":257,\"span_id\":\"{:016x}\",\"trace_id\":\"{:032x}\",\"trace_state\":\"linked=yes\"}}]",
                        trace_number + 2_000_000,
                        trace_number + 1_000_000
                    )
                } else {
                    "[]".to_owned()
                },
                trace_state: if global.is_multiple_of(8) {
                    "bench=root"
                } else {
                    ""
                },
                trace_flags: if global.is_multiple_of(8) {
                    u32::MAX
                } else {
                    1
                },
                dropped_attributes_count: (global % 4 + 1) as u32,
                dropped_events_count: (global % 3 + 2) as u32,
                dropped_links_count: (global % 2 + 3) as u32,
            }
        })
        .collect::<Vec<_>>();
    let resource = br#"{"deployment.environment":"baseline","replica":7,"service.name":"resource-fallback","service.version":"1.2.3"}"#;
    let scope = br#"{"attributes":{"debug":false},"name":"trace-baseline","version":"1.0.0"}"#;
    let mut output = vec![3, 0, 0, 0];
    output.extend_from_slice(&(spans.len() as u32).to_le_bytes());
    for span in &spans {
        output.extend_from_slice(&span.trace_id);
    }
    for span in &spans {
        output.extend_from_slice(&span.span_id);
    }
    for span in &spans {
        output.extend_from_slice(&span.parent_span_id.unwrap_or([0; 8]));
    }
    for span in &spans {
        framed(&mut output, span.name.as_bytes());
    }
    for _ in &spans {
        framed(&mut output, b"fallback-service");
    }
    output.extend(spans.iter().map(|span| span.kind));
    output.extend(spans.iter().map(|span| span.status));
    for span in &spans {
        output.extend_from_slice(&span.start_ts.to_le_bytes());
    }
    for span in &spans {
        output.extend_from_slice(&span.duration_ns.to_le_bytes());
    }
    for span in &spans {
        framed(&mut output, span.attributes.as_bytes());
    }
    for span in &spans {
        framed(&mut output, span.status_description.as_bytes());
    }
    for span in &spans {
        framed(&mut output, span.events.as_bytes());
    }
    for _ in &spans {
        framed(&mut output, resource);
    }
    for _ in &spans {
        framed(&mut output, scope);
    }
    for span in &spans {
        framed(&mut output, span.links.as_bytes());
    }
    for span in &spans {
        framed(&mut output, span.trace_state.as_bytes());
    }
    u32_column(&mut output, spans.iter().map(|span| span.trace_flags));
    u32_column(
        &mut output,
        spans.iter().map(|span| span.dropped_attributes_count),
    );
    u32_column(
        &mut output,
        spans.iter().map(|span| span.dropped_events_count),
    );
    u32_column(
        &mut output,
        spans.iter().map(|span| span.dropped_links_count),
    );
    for _ in &spans {
        framed(&mut output, b"https://example.test/resource/1");
    }
    for _ in &spans {
        framed(&mut output, b"https://example.test/scope/2");
    }
    u32_column(&mut output, spans.iter().map(|_| 4));
    u32_column(&mut output, spans.iter().map(|_| 5));
    output
}

fn fixed_be<const N: usize>(number: u64) -> [u8; N] {
    let mut result = [0; N];
    let bytes = number.to_be_bytes();
    let copied = bytes.len().min(N);
    result[N - copied..].copy_from_slice(&bytes[bytes.len() - copied..]);
    result
}

fn framed(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u32).to_le_bytes());
    output.extend_from_slice(value);
}

fn u32_column(output: &mut Vec<u8>, values: impl Iterator<Item = u32>) {
    for value in values {
        output.extend_from_slice(&value.to_le_bytes());
    }
}

fn fixture_duration(batch_offset: usize) -> i64 {
    if (batch_offset / 3).is_multiple_of(2) {
        100_000
    } else {
        900_000
    }
}

#[allow(clippy::too_many_arguments)]
fn measure_http(
    client: &Client,
    base: &str,
    pid: u32,
    path: &str,
    expected_traces: usize,
    expected_spans: usize,
    iterations: usize,
    warmup: usize,
) -> Result<Value> {
    let startup_memory = process_memory(pid)?;
    for _ in 0..warmup {
        let body = http_get(client, base, path)?;
        require_jaeger_cardinality(&body, expected_traces, expected_spans)?;
    }
    let after_warmup_memory = process_memory(pid)?;
    let before = http_json(client, base, "/select/traces/stats")?;
    let mut elapsed = Vec::with_capacity(iterations);
    let mut response_bytes = BTreeSet::new();
    for _ in 0..iterations {
        let started = Instant::now();
        let body = http_get(client, base, path)?;
        elapsed.push(started.elapsed().as_nanos());
        response_bytes.insert(body.len());
        require_jaeger_cardinality(&body, expected_traces, expected_spans)?;
    }
    let after = http_json(client, base, "/select/traces/stats")?;
    let after_measured_memory = process_memory(pid)?;
    ensure!(
        response_bytes.len() == 1,
        "{path} response size was not deterministic"
    );
    Ok(json!({
        "path": path,
        "iterations": iterations,
        "warmup": warmup,
        "latency_ns": latency_summary(&elapsed),
        "result_traces": expected_traces,
        "result_spans": expected_spans,
        "response_bytes": response_bytes.first(),
        "memory": {
            "startup": startup_memory,
            "after_warmup": after_warmup_memory,
            "after_measured": after_measured_memory,
        },
        "api_work_delta": prefix_numeric_delta(&before, &after, "api_read_"),
        "extension_work_delta": prefix_numeric_delta(&before, &after, "extension_query_"),
    }))
}

#[allow(clippy::too_many_arguments)]
fn measure_isolated_http(
    binary: &Path,
    extension: &Path,
    database: &Path,
    directory: &Path,
    log_name: &str,
    client: &Client,
    path: &str,
    expected_traces: usize,
    expected_spans: usize,
    iterations: usize,
    warmup: usize,
) -> Result<(Value, Value)> {
    let mut server = TraceServer::start(binary, extension, database, directory, log_name)?;
    let result = (|| {
        let measurement = measure_http(
            client,
            &server.base,
            server.pid(),
            path,
            expected_traces,
            expected_spans,
            iterations,
            warmup,
        )?;
        let stats = http_json(client, &server.base, "/select/traces/stats")?;
        Ok((measurement, stats))
    })();
    let shutdown = server.stop();
    match (result, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn require_jaeger_cardinality(
    body: &[u8],
    expected_traces: usize,
    expected_spans: usize,
) -> Result<()> {
    let value: Value = serde_json::from_slice(body)?;
    let traces = value
        .get("data")
        .and_then(Value::as_array)
        .context("Jaeger data array")?;
    let spans = traces
        .iter()
        .map(|trace| {
            trace
                .get("spans")
                .and_then(Value::as_array)
                .map_or(0, Vec::len)
        })
        .sum::<usize>();
    ensure!(
        traces.len() == expected_traces && spans == expected_spans,
        "Jaeger cardinality traces={} spans={}, expected {expected_traces}/{expected_spans}",
        traces.len(),
        spans
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn measure_buckets(
    connection: &Connection,
    service: Option<&str>,
    start: i64,
    stop: i64,
    step: i64,
    expected_spans: usize,
    iterations: usize,
    warmup: usize,
) -> Result<Value> {
    for _ in 0..warmup {
        let rows = execute_buckets(connection, service, start, stop, step)?;
        require_bucket_spans(&rows, expected_spans)?;
    }
    let before = sqlite_stats(connection)?;
    let mut elapsed = Vec::with_capacity(iterations);
    let mut cardinality = BTreeSet::new();
    let mut result_bytes = BTreeSet::new();
    for _ in 0..iterations {
        let started = Instant::now();
        let rows = execute_buckets(connection, service, start, stop, step)?;
        elapsed.push(started.elapsed().as_nanos());
        require_bucket_spans(&rows, expected_spans)?;
        cardinality.insert(rows.len());
        result_bytes.insert(serde_json::to_vec(&rows)?.len());
    }
    let after = sqlite_stats(connection)?;
    ensure!(
        cardinality.len() == 1 && result_bytes.len() == 1,
        "bucket output changed between iterations"
    );
    Ok(json!({
        "sql": "SELECT bucket_ts,service,spans,errors,dur_sum,dur_min,dur_max,dur_p50,dur_p95,dur_p99 FROM timeless_trace_buckets(?1,?2,?3,?4,?5) ORDER BY bucket_ts,service",
        "parameters": {
            "table": TABLE,
            "service": service,
            "start_ns": start,
            "stop_ns": stop,
            "step_ns": step
        },
        "iterations": iterations,
        "warmup": warmup,
        "latency_ns": latency_summary(&elapsed),
        "result_rows": cardinality.first(),
        "result_spans": expected_spans,
        "result_json_bytes": result_bytes.first(),
        "extension_work_delta": prefix_numeric_delta(&before, &after, "query_"),
    }))
}

fn execute_buckets(
    connection: &Connection,
    service: Option<&str>,
    start: i64,
    stop: i64,
    step: i64,
) -> Result<Vec<BucketRow>> {
    let mut statement = connection.prepare_cached(
        "SELECT bucket_ts,service,spans,errors,dur_sum,dur_min,dur_max,dur_p50,dur_p95,dur_p99 \
         FROM timeless_trace_buckets(?1,?2,?3,?4,?5) ORDER BY bucket_ts,service",
    )?;
    let rows = statement
        .query_map(params![TABLE, service, start, stop, step], |row| {
            Ok(BucketRow {
                bucket_ts: row.get(0)?,
                service: row.get(1)?,
                spans: row.get(2)?,
                errors: row.get(3)?,
                dur_sum: row.get(4)?,
                dur_min: row.get(5)?,
                dur_max: row.get(6)?,
                dur_p50: row.get(7)?,
                dur_p95: row.get(8)?,
                dur_p99: row.get(9)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn require_bucket_spans(rows: &[BucketRow], expected: usize) -> Result<()> {
    let spans = rows.iter().try_fold(0_i64, |total, row| {
        total.checked_add(row.spans).context("bucket span overflow")
    })?;
    ensure!(
        spans == expected as i64,
        "bucket total {spans}, expected {expected}"
    );
    ensure!(rows
        .iter()
        .all(|row| row.dur_min == 100_000 && row.dur_max == 900_000));
    ensure!(rows
        .iter()
        .all(|row| { row.dur_p50 == 100_000 && row.dur_p95 == 900_000 && row.dur_p99 == 900_000 }));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn isolated_sql_baseline(
    root: &Path,
    extension: &Path,
    database: &Path,
    start_ns: i64,
    stop_ns: i64,
    expected_spans: usize,
    iterations: usize,
    warmup: usize,
    attribute_indexes: bool,
) -> Result<Value> {
    let executable = std::env::current_exe()?;
    let output = Command::new(executable)
        .args([
            "--root",
            root.to_string_lossy().as_ref(),
            "trace-baseline-sql",
        ])
        .args(["--extension", extension.to_string_lossy().as_ref()])
        .args(["--database", database.to_string_lossy().as_ref()])
        .args(["--start-ns", &start_ns.to_string()])
        .args(["--stop-ns", &stop_ns.to_string()])
        .args(["--expected-spans", &expected_spans.to_string()])
        .args(["--iterations", &iterations.to_string()])
        .args(["--warmup", &warmup.to_string()])
        .args(attribute_indexes.then_some("--attribute-indexes"))
        .output()?;
    if !output.status.success() {
        bail!(
            "isolated direct-SQL baseline failed {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    serde_json::from_slice(&output.stdout).context("decode isolated direct-SQL evidence")
}

fn open(database: &Path, extension: &Path) -> Result<Connection> {
    let connection = Connection::open(database)?;
    unsafe {
        connection.load_extension_enable()?;
        connection.load_extension(extension, None::<&str>)?;
        connection.load_extension_disable()?;
    }
    Ok(connection)
}

fn http_get(client: &Client, base: &str, path: &str) -> Result<Vec<u8>> {
    let response = client.get(format!("{base}{path}")).send()?;
    let status = response.status().as_u16();
    let body = response.bytes()?.to_vec();
    if status != 200 {
        bail!(
            "GET {path} returned {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    Ok(body)
}

fn http_json(client: &Client, base: &str, path: &str) -> Result<Value> {
    let body = http_get(client, base, path)?;
    serde_json::from_slice(&body).with_context(|| format!("decode GET {path}"))
}

fn sqlite_stats(connection: &Connection) -> Result<Value> {
    let mut statement = connection.prepare("SELECT key,value FROM timeless_stats('traces')")?;
    let entries = statement
        .query_map([], |row| {
            let key: String = row.get(0)?;
            let value = match row.get_ref(1)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(value) => json!(value),
                ValueRef::Real(value) => Number::from_f64(value).map_or(Value::Null, Value::Number),
                ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).into_owned()),
                ValueRef::Blob(value) => Value::String(format!("{} bytes", value.len())),
            };
            Ok((key, value))
        })?
        .collect::<rusqlite::Result<Map<_, _>>>()?;
    Ok(Value::Object(entries))
}

fn stat_i64(stats: &Value, key: &str) -> Result<i64> {
    stats
        .get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("missing integer trace stat {key}"))
}

fn prefix_numeric_delta(before: &Value, after: &Value, prefix: &str) -> Value {
    let mut result = Map::new();
    let (Some(before), Some(after)) = (before.as_object(), after.as_object()) else {
        return Value::Object(result);
    };
    for (key, value) in after {
        if !key.starts_with(prefix) {
            continue;
        }
        if let (Some(previous), Some(current)) =
            (before.get(key).and_then(Value::as_i64), value.as_i64())
        {
            result.insert(key.clone(), json!(current - previous));
        } else if let (Some(previous), Some(current)) =
            (before.get(key).and_then(Value::as_u64), value.as_u64())
        {
            result.insert(key.clone(), json!(current.saturating_sub(previous)));
        }
    }
    Value::Object(result)
}

fn select_fields(value: &Value, fields: &[&str]) -> Value {
    let mut result = Map::new();
    for field in fields {
        result.insert(
            (*field).to_owned(),
            value.get(*field).cloned().unwrap_or(Value::Null),
        );
    }
    Value::Object(result)
}

fn latency_summary(values: &[u128]) -> Value {
    json!({
        "min": values.iter().min().copied().unwrap_or(0),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": values.iter().max().copied().unwrap_or(0),
    })
}

fn percentile(values: &[u128], fraction: f64) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = ((sorted.len() as f64 * fraction).ceil() as usize).saturating_sub(1);
    sorted[rank.min(sorted.len() - 1)]
}

fn process_memory(pid: u32) -> Result<Value> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let mut result = Map::new();
    for line in status.lines() {
        for (prefix, key) in [("VmRSS:", "rss_kib"), ("VmHWM:", "hwm_kib")] {
            if let Some(value) = line.strip_prefix(prefix) {
                let value = value
                    .split_whitespace()
                    .next()
                    .context("memory value")?
                    .parse::<u64>()?;
                result.insert(key.to_owned(), json!(value));
            }
        }
    }
    Ok(Value::Object(result))
}

fn storage_files(database: &Path) -> Value {
    let encoded = database.to_string_lossy();
    let mut result = Map::new();
    let mut physical = 0_u64;
    for (suffix, key) in [("", "database"), ("-wal", "wal"), ("-shm", "shm")] {
        let bytes = fs::metadata(format!("{encoded}{suffix}"))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        physical += bytes;
        result.insert(format!("{key}_bytes"), json!(bytes));
    }
    result.insert("physical_bytes".to_owned(), json!(physical));
    Value::Object(result)
}

fn require_clean_tracked_worktree(root: &Path) -> Result<()> {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .current_dir(root)
        .output()?;
    ensure!(output.status.success(), "git status failed");
    let status = String::from_utf8(output.stdout)?;
    ensure!(
        status.trim().is_empty(),
        "baseline requires clean tracked source:\n{status}"
    );
    Ok(())
}

fn git_commit(root: &Path) -> Result<String> {
    git_line(root, &["rev-parse", "HEAD"])
}

fn current_branch(root: &Path) -> Result<String> {
    git_line(root, &["branch", "--show-current"])
}

fn git_line(root: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git").args(args).current_dir(root).output()?;
    ensure!(output.status.success(), "git {} failed", args.join(" "));
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn binary_identity(binary: &Path, expected_commit: &str) -> Result<Value> {
    let output = Command::new(binary).arg("--version").output()?;
    ensure!(
        output.status.success(),
        "{} --version failed",
        binary.display()
    );
    let identity: Value = serde_json::from_slice(&output.stdout)?;
    ensure!(
        identity.get("commit").and_then(Value::as_str) == Some(expected_commit),
        "trace binary build commit mismatch"
    );
    Ok(identity)
}

fn extension_identity(extension: &Path, expected_commit: &str) -> Result<Value> {
    let connection = open(Path::new(":memory:"), extension)?;
    let encoded: String =
        connection.query_row("SELECT timeless_capabilities()", [], |row| row.get(0))?;
    let capabilities: Value = serde_json::from_str(&encoded)?;
    let build = capabilities
        .get("build")
        .cloned()
        .context("extension build identity")?;
    ensure!(
        build.get("commit").and_then(Value::as_str) == Some(expected_commit),
        "extension build commit mismatch"
    );
    Ok(build)
}

fn host_identity() -> Value {
    let uname = |flag: &str| {
        Command::new("uname")
            .arg(flag)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|value| value.trim().to_owned())
            .unwrap_or_default()
    };
    json!({
        "system": uname("-s"),
        "release": uname("-r"),
        "machine": uname("-m"),
        "processor": uname("-p")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles_are_stable() {
        let values = [40, 10, 30, 20];
        assert_eq!(percentile(&values, 0.50), 20);
        assert_eq!(percentile(&values, 0.95), 40);
        assert_eq!(percentile(&values, 0.99), 40);
    }

    #[test]
    fn fixed_width_big_endian_identity_preserves_low_bits() {
        assert_eq!(fixed_be::<8>(0x0102), [0, 0, 0, 0, 0, 0, 1, 2]);
        assert_eq!(&fixed_be::<16>(1)[8..], &[0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn every_authoritative_batch_has_the_pinned_duration_quantiles() {
        let mut durations = (0..BATCH_SPANS).map(fixture_duration).collect::<Vec<_>>();
        durations.sort_unstable();
        assert_eq!(durations[4_095], 100_000);
        assert_eq!(durations[7_782], 900_000);
        assert_eq!(durations[8_109], 900_000);
    }

    #[test]
    fn attribute_evidence_binds_each_query_shape_exactly() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE traces(\
                   trace_id BLOB,span_id BLOB,start_ts INTEGER,attributes TEXT,\
                   attribute_filter TEXT\
                 )",
            )
            .unwrap();
        assert_eq!(
            connection
                .prepare(AttributeShape::ExactCount.control_sql())
                .unwrap()
                .parameter_count(),
            3
        );
        assert_eq!(
            connection
                .prepare(AttributeShape::BooleanTrue.control_sql())
                .unwrap()
                .parameter_count(),
            2
        );
        for shape in [AttributeShape::ExactCount, AttributeShape::BooleanTrue] {
            assert_eq!(
                connection
                    .prepare(shape.candidate_sql())
                    .unwrap()
                    .parameter_count(),
                3
            );
        }
    }
}
