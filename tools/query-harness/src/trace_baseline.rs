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

struct FixtureReport {
    spans: usize,
    stop_ns: i64,
    report: Value,
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
        let fixture = build_fixture(&extension, &database, args.batches)?;
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
                "rich_fields": ["attributes", "status_description", "events", "resource", "instrumentation_scope"],
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
                    "freelist_pages", "freelist_bytes"
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
    let report = json!({
        "process_isolation": "fresh child; fixture generation and HTTP response allocation excluded",
        "broad_all_time_boxes": broad,
        "narrow_one_time_box_control": narrow,
        "posting_windows": posting_windows,
        "rss": process_memory(std::process::id())?,
    });
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
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

fn build_fixture(extension: &Path, database: &Path, batches: usize) -> Result<FixtureReport> {
    let connection = open(database, extension)?;
    let journal_mode: String =
        connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    connection.execute("CREATE VIRTUAL TABLE traces USING timeless_traces", [])?;
    let started = Instant::now();
    let mut public_batch_bytes = 0_u64;
    for batch_number in 0..batches {
        let blob = rich_batch(batch_number);
        public_batch_bytes += blob.len() as u64;
        connection.execute("INSERT INTO traces(traces) VALUES (?1)", params![blob])?;
    }
    let insert_ns = started.elapsed().as_nanos();
    let before_optimize = sqlite_stats(&connection)?;
    let optimize_started = Instant::now();
    connection.execute("INSERT INTO traces(traces) VALUES ('optimize')", [])?;
    let optimize_ns = optimize_started.elapsed().as_nanos();
    let after_optimize = sqlite_stats(&connection)?;
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
    let rich: (String, String, String, String, String) = connection.query_row(
        "SELECT service,attributes,events,resource,instrumentation_scope \
           FROM traces ORDER BY start_ts LIMIT 1",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    ensure!(
        rich.0 == "bench",
        "service.name precedence was not preserved"
    );
    ensure!(serde_json::from_str::<Value>(&rich.1)?["nested"]["unicode"] == "空🔥");
    ensure!(serde_json::from_str::<Value>(&rich.2)?[0]["name"] == "exception");
    ensure!(serde_json::from_str::<Value>(&rich.3)?["deployment.environment"] == "baseline");
    ensure!(serde_json::from_str::<Value>(&rich.4)?["name"] == "trace-baseline");
    let reopen_stats = sqlite_stats(&connection)?;
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
            "storage_after_checkpoint": storage_files(database),
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
                    "[{{\"attributes\":{{\"attempt\":{},\"fatal\":false}},\"name\":\"exception\",\"timestamp\":{}}}]",
                    global % 5,
                    start_ts + 50_000
                ),
            }
        })
        .collect::<Vec<_>>();
    let resource = br#"{"deployment.environment":"baseline","replica":7,"service.name":"resource-fallback","service.version":"1.2.3"}"#;
    let scope = br#"{"attributes":{"debug":false},"name":"trace-baseline","version":"1.0.0"}"#;
    let mut output = vec![2, 0, 0, 0];
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
}
