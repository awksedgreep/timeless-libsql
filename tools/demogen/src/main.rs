//! timeless-demogen: seed a timeless database with a realistic synthetic
//! fleet — tens of thousands to hundreds of thousands of metric series,
//! millions of logs, millions of spans — for demos and screencasts.
//!
//!   timeless-demogen seed --db demo.db                  # medium profile
//!   timeless-demogen seed --db demo.db --profile large  # ~245k series
//!   timeless-demogen seed --db demo.db --follow         # seed, then tail
//!   timeless-demogen live --db demo.db                  # append live data
//!
//! The generator itself lives in demogen-core; this binary is the
//! batch/CLI front end. For the pure in-shell experience (`.load` the
//! generator next to the telemetry extension and call `timeless_demo()`
//! from SQL) build ext/ instead. Standalone on purpose: this crate is NOT
//! a member of the parent workspace and is never linked into the
//! extension or the servers.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection};

use demogen_core::drive::{
    drive_logs, drive_metrics, drive_traces, format_report, profile, warm_states, SignalReport,
    LIVE_LOG_RATE, LIVE_TRACE_RATE,
};
use demogen_core::fleet::{build_catalog, Config, Incident, Rng, SeriesSpec, TraceReservoir};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Opts {
    cmd: String,
    db: String,
    ext: Option<String>,
    profile: String,
    seed: u64,
    services: Option<usize>,
    pods: Option<usize>,
    paths: Option<usize>,
    minutes: Option<u64>,
    step_secs: Option<u64>,
    logs: Option<usize>,
    traces: Option<usize>,
    follow: bool,
    append: bool,
    log_rate: usize,
    trace_rate: usize,
    tick_secs: u64,
}

fn usage() -> ! {
    eprintln!(
        r#"timeless-demogen — synthetic telemetry for timeless demos

USAGE:
  timeless-demogen seed --db PATH [options]   create + populate a database
  timeless-demogen live --db PATH [options]   append live data to an existing one

OPTIONS:
  --db PATH          database file (required)
  --ext PATH         libtimeless_ext artifact (default: search ./target and ../../target)
  --profile NAME     small | medium | large   (default medium)
                       small  ≈  4k series, 200k logs, 200k spans, 30 min
                       medium ≈ 35k series, 2M logs, 1M spans, 60 min
                       large  ≈ 245k series, 5M logs, 3M spans, 60 min
  --seed N           deterministic seed (default 42)
  --services N       override service count
  --pods N           override pods per service
  --paths N          override HTTP paths per pod (7 series each)
  --minutes N        override seeded window length
  --step-secs N      override metric scrape interval
  --logs N           override log entry count
  --traces N         override trace count (spans ≈ 10x)
  --append           allow seeding into an existing file
  --follow           after seeding, keep appending live data (implies live)
  --log-rate N       live mode: log entries per second (default 1500)
  --trace-rate N     live mode: traces per second (default 40)
  --tick-secs N      live mode: seconds between appends (default 2)
"#
    );
    std::process::exit(2);
}

fn parse_opts() -> Opts {
    let mut args = std::env::args().skip(1);
    let cmd = match args.next() {
        Some(c) if c == "seed" || c == "live" => c,
        _ => usage(),
    };
    let mut o = Opts {
        cmd,
        db: String::new(),
        ext: None,
        profile: "medium".into(),
        seed: 42,
        services: None,
        pods: None,
        paths: None,
        minutes: None,
        step_secs: None,
        logs: None,
        traces: None,
        follow: false,
        append: false,
        log_rate: LIVE_LOG_RATE,
        trace_rate: LIVE_TRACE_RATE,
        tick_secs: 2,
    };
    let need = |a: Option<String>| a.unwrap_or_else(|| usage());
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--db" => o.db = need(args.next()),
            "--ext" => o.ext = Some(need(args.next())),
            "--profile" => o.profile = need(args.next()),
            "--seed" => o.seed = need(args.next()).parse().unwrap_or_else(|_| usage()),
            "--services" => o.services = need(args.next()).parse().ok(),
            "--pods" => o.pods = need(args.next()).parse().ok(),
            "--paths" => o.paths = need(args.next()).parse().ok(),
            "--minutes" => o.minutes = need(args.next()).parse().ok(),
            "--step-secs" => o.step_secs = need(args.next()).parse().ok(),
            "--logs" => o.logs = need(args.next()).parse().ok(),
            "--traces" => o.traces = need(args.next()).parse().ok(),
            "--append" => o.append = true,
            "--follow" => o.follow = true,
            "--log-rate" => o.log_rate = need(args.next()).parse().unwrap_or_else(|_| usage()),
            "--trace-rate" => o.trace_rate = need(args.next()).parse().unwrap_or_else(|_| usage()),
            "--tick-secs" => o.tick_secs = need(args.next()).parse().unwrap_or_else(|_| usage()),
            "--help" | "-h" => usage(),
            other => {
                eprintln!("unknown flag: {other}");
                usage()
            }
        }
    }
    if o.db.is_empty() {
        eprintln!("--db is required");
        usage();
    }
    o
}

fn build_config(o: &Opts) -> Config {
    let Some(mut spec) = profile(&o.profile) else {
        eprintln!("unknown profile: {}", o.profile);
        usage()
    };
    if let Some(v) = o.services {
        spec.services = v.max(1);
    }
    if let Some(v) = o.pods {
        spec.pods = v.max(1);
    }
    if let Some(v) = o.paths {
        spec.paths = v.clamp(1, demogen_core::fleet::PATH_POOL.len());
    }
    if let Some(v) = o.minutes {
        spec.minutes = v.max(1);
    }
    if let Some(v) = o.step_secs {
        spec.step_secs = v.max(1);
    }
    if let Some(v) = o.logs {
        spec.logs = v;
    }
    if let Some(v) = o.traces {
        spec.traces = v;
    }
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    spec.config(o.seed, now_ms)
}

// ---------------------------------------------------------------------------
// Database plumbing
// ---------------------------------------------------------------------------

fn find_ext(explicit: &Option<String>) -> String {
    if let Some(p) = explicit {
        if !std::path::Path::new(p).exists() {
            eprintln!("extension artifact not found: {p}");
            std::process::exit(1);
        }
        return p.clone();
    }
    let stem = if cfg!(target_os = "macos") {
        "libtimeless_ext.dylib"
    } else {
        "libtimeless_ext.so"
    };
    for dir in [
        "target/release",
        "target/debug",
        "../../target/release",
        "../../target/debug",
    ] {
        let p = format!("{dir}/{stem}");
        if std::path::Path::new(&p).exists() {
            return p;
        }
    }
    eprintln!(
        "could not find {stem}; build it with `cargo build --release -p timeless-ext` \
         at the repo root, or pass --ext PATH"
    );
    std::process::exit(1);
}

fn open_db(path: &str, ext: &str) -> Connection {
    let conn = Connection::open(path).expect("open db");
    unsafe {
        conn.load_extension_enable().expect("enable ext loading");
        conn.load_extension(ext, None::<&str>).expect("load extension");
    }
    conn.load_extension_disable().expect("disable ext loading");
    conn
}

fn ensure_tables(conn: &Connection) {
    // auto_vacuum FIRST: it only takes effect before anything touches the
    // file, and even the WAL switch writes page 1. WAL so a demo reader
    // (sqlite3 shell, server) can query while live mode keeps writing.
    conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;")
        .expect("auto_vacuum");
    conn.pragma_update(None, "journal_mode", "WAL").expect("WAL");
    conn.pragma_update(None, "synchronous", "NORMAL").expect("synchronous");
    conn.execute_batch(
        "CREATE VIRTUAL TABLE IF NOT EXISTS metrics USING timeless_metrics;
         CREATE VIRTUAL TABLE IF NOT EXISTS logs
             USING timeless_logs(index_keys='service,path,status');
         CREATE VIRTUAL TABLE IF NOT EXISTS spans USING timeless_traces;",
    )
    .expect("create virtual tables");
}

fn command(conn: &Connection, table: &str, cmd: &str) {
    conn.execute(&format!("INSERT INTO {table}({table}) VALUES ('{cmd}')"), [])
        .unwrap_or_else(|e| panic!("{table} {cmd}: {e}"));
}

fn fmt_count(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Sink adapter: insert each blob through a prepared statement and print
/// `\r` progress with the running item count.
macro_rules! insert_sink {
    ($stmt:expr, $label:literal, $unit:literal) => {
        |blob: &[u8], total: usize| {
            $stmt.execute(params![blob]).map_err(|e| e.to_string())?;
            eprint!("\r  {}: {} {}", $label, fmt_count(total), $unit);
            Ok(())
        }
    };
}

// ---------------------------------------------------------------------------
// Seeding
// ---------------------------------------------------------------------------

/// One integer counter from the public timeless_stats surface.
fn stat(conn: &Connection, tbl: &str, key: &str) -> u64 {
    conn.query_row(
        "SELECT CAST(value AS INTEGER) FROM timeless_stats(?1) WHERE key = ?2",
        params![tbl, key],
        |r| r.get::<_, i64>(0),
    )
    .unwrap_or_else(|e| panic!("timeless_stats({tbl}).{key}: {e}")) as u64
}

fn seed(conn: &Connection, cfg: &Config, incident: &Incident, catalog: &[SeriesSpec]) -> TraceReservoir {
    let mut reservoir = TraceReservoir::new(20_000);
    // Raw logical bytes per signal (metrics, logs, spans) for the report.
    let mut raw = (0u64, 0u64, 0u64);
    let t0 = Instant::now();

    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn.prepare("INSERT INTO spans(spans) VALUES (?1)").unwrap();
        let mut rng = Rng::new(cfg.seed ^ 0x0724_7CE5);
        let mut sink = insert_sink!(stmt, "traces", "spans");
        let t = drive_traces(
            cfg, incident, &mut rng, cfg.start_ms(), cfg.end_ms, cfg.traces,
            &mut reservoir, &mut sink,
        )
        .expect("trace ingest");
        drop(sink);
        raw.2 = t.raw_bytes;
        let secs = t0.elapsed().as_secs_f64();
        eprintln!(
            "\r  traces: {} spans in {} traces, {:.1}s ({:.0}k spans/s)",
            fmt_count(t.items), fmt_count(cfg.traces), secs,
            t.items as f64 / secs / 1e3
        );
    }
    conn.execute_batch("COMMIT").unwrap();

    let t1 = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn.prepare("INSERT INTO logs(logs) VALUES (?1)").unwrap();
        let mut rng = Rng::new(cfg.seed ^ 0x1065);
        let mut sink = insert_sink!(stmt, "logs", "entries");
        let t = drive_logs(
            cfg, incident, &mut rng, cfg.start_ms(), cfg.end_ms, cfg.logs,
            &reservoir, &mut sink,
        )
        .expect("log ingest");
        drop(sink);
        raw.1 = t.raw_bytes;
        let secs = t1.elapsed().as_secs_f64();
        eprintln!(
            "\r  logs: {} entries in {:.1}s ({:.2}M entries/s)",
            fmt_count(t.items), secs,
            t.items as f64 / secs / 1e6
        );
    }
    conn.execute_batch("COMMIT").unwrap();

    let t2 = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn.prepare("INSERT INTO metrics(metrics) VALUES (?1)").unwrap();
        let mut states = warm_states(catalog, cfg, incident, 0);
        let step_ms = (cfg.step_secs * 1000) as i64;
        let start = cfg.start_ms();
        let mut sink = insert_sink!(stmt, "metrics", "samples");
        let t = drive_metrics(
            cfg, incident, catalog, &mut states, 0, cfg.steps(),
            &|i| start + i as i64 * step_ms, &mut sink,
        )
        .expect("metric ingest");
        drop(sink);
        raw.0 = t.raw_bytes;
        let secs = t2.elapsed().as_secs_f64();
        eprintln!(
            "\r  metrics: {} series, {} samples in {:.1}s ({:.2}M samples/s)",
            fmt_count(catalog.len()), fmt_count(t.items), secs,
            t.items as f64 / secs / 1e6
        );
    }
    conn.execute_batch("COMMIT").unwrap();

    let tm = Instant::now();
    command(conn, "metrics", "flush");
    command(conn, "logs", "flush");
    command(conn, "spans", "flush");
    command(conn, "metrics", "compact");
    command(conn, "logs", "optimize");
    command(conn, "spans", "optimize");
    eprintln!("  maintenance (flush+compact+optimize): {:.1}s", tm.elapsed().as_secs_f64());

    // Keep bookkeeping out of the compression story: fold the WAL back
    // into the main file and return freed pages before measuring.
    let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
    {
        let mut stmt = conn.prepare("PRAGMA incremental_vacuum;").expect("prepare vacuum");
        let mut rows = stmt.query([]).expect("run vacuum");
        while rows.next().expect("step vacuum").is_some() {}
    }
    let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));

    let signals = [
        SignalReport {
            label: "metrics", unit: "samples", per: "sample",
            items: stat(conn, "metrics", "disk_points"),
            raw_bytes: raw.0,
            payload_bytes: stat(conn, "metrics", "bytes_on_disk"),
            index_bytes: stat(conn, "metrics", "index_bytes"),
        },
        SignalReport {
            label: "logs", unit: "entries", per: "entry",
            items: stat(conn, "logs", "disk_entries"),
            raw_bytes: raw.1,
            payload_bytes: stat(conn, "logs", "bytes_on_disk"),
            index_bytes: stat(conn, "logs", "index_bytes"),
        },
        SignalReport {
            label: "traces", unit: "spans", per: "span",
            items: stat(conn, "spans", "disk_spans"),
            raw_bytes: raw.2,
            payload_bytes: stat(conn, "spans", "bytes_on_disk"),
            index_bytes: stat(conn, "spans", "index_bytes"),
        },
    ];
    let (file, free): (i64, i64) = conn
        .query_row(
            "SELECT page_count * page_size, freelist_count * page_size
               FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size()",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("size pragmas");
    println!("{}", format_report(&signals, file as u64, free as u64));
    reservoir
}

// ---------------------------------------------------------------------------
// Live mode
// ---------------------------------------------------------------------------

fn live(
    conn: &Connection,
    cfg: &Config,
    incident: &Incident,
    catalog: &[SeriesSpec],
    reservoir: &mut TraceReservoir,
    o: &Opts,
    mut step_index: usize,
) {
    let mut states = warm_states(catalog, cfg, incident, step_index);
    let mut rng = Rng::new(cfg.seed ^ 0x11FE);
    let tick = Duration::from_secs(o.tick_secs.max(1));
    let scrape_ms = (cfg.step_secs * 1000) as i64;
    let now_ms = || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    };
    let mut last_ms = now_ms();
    let mut next_scrape = last_ms - last_ms.rem_euclid(scrape_ms) + scrape_ms;

    let mut log_stmt = conn.prepare("INSERT INTO logs(logs) VALUES (?1)").unwrap();
    let mut span_stmt = conn.prepare("INSERT INTO spans(spans) VALUES (?1)").unwrap();
    let mut metric_stmt = conn.prepare("INSERT INTO metrics(metrics) VALUES (?1)").unwrap();

    eprintln!(
        "live: {} logs/s, {} traces/s, metric scrape every {}s — Ctrl-C to stop",
        o.log_rate, o.trace_rate, cfg.step_secs
    );
    loop {
        std::thread::sleep(tick);
        let now = now_ms();
        let t0 = Instant::now();

        let mut spans = 0usize;
        let n_traces = o.trace_rate * o.tick_secs as usize;
        if n_traces > 0 {
            let mut sink = |blob: &[u8], total: usize| {
                span_stmt.execute(params![blob]).map_err(|e| e.to_string())?;
                spans = total;
                Ok(())
            };
            drive_traces(cfg, incident, &mut rng, last_ms, now, n_traces, reservoir, &mut sink)
                .expect("live traces");
        }

        let mut entries = 0usize;
        let n_logs = o.log_rate * o.tick_secs as usize;
        if n_logs > 0 {
            let mut sink = |blob: &[u8], total: usize| {
                log_stmt.execute(params![blob]).map_err(|e| e.to_string())?;
                entries = total;
                Ok(())
            };
            drive_logs(cfg, incident, &mut rng, last_ms, now, n_logs, reservoir, &mut sink)
                .expect("live logs");
        }

        let mut scraped = 0usize;
        while now >= next_scrape {
            let ts = next_scrape;
            let mut points = 0usize;
            let mut sink = |blob: &[u8], total: usize| {
                metric_stmt.execute(params![blob]).map_err(|e| e.to_string())?;
                points = total;
                Ok(())
            };
            drive_metrics(cfg, incident, catalog, &mut states, step_index, 1, &|_| ts, &mut sink)
                .expect("live metrics");
            scraped += points;
            step_index += 1;
            next_scrape += scrape_ms;
        }
        if scraped > 0 {
            command(conn, "metrics", "flush");
        }
        command(conn, "logs", "flush");
        command(conn, "spans", "flush");

        let mut line = format!("  +{} logs, +{} spans", fmt_count(entries), fmt_count(spans));
        if scraped > 0 {
            line.push_str(&format!(", scraped {} samples", fmt_count(scraped)));
        }
        eprintln!("{line} ({} ms)", t0.elapsed().as_millis());
        last_ms = now;
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn cheat_sheet(cfg: &Config, incident: &Incident) {
    let svc = cfg.service_name(incident.service);
    let (start, stop) = (cfg.start_ms(), cfg.end_ms);
    println!(
        r#"
Try these in `sqlite3 <db>` (after .load of the extension):

  -- the whole series catalog
  SELECT count(*) FROM timeless_series('metrics');

  -- 1-minute average cpu for one pod of the incident service ({svc})
  SELECT ts, round(value,1) FROM timeless_window('metrics','cpu_usage_percent',
    '{{"instance":"{svc}-000","mode":"user"}}', {start}, {stop}, 60000, 60000, 'avg');

  -- error volume per service (indexed metadata columns)
  SELECT n FROM timeless_log_count('logs','{{"level":"error","service":"{svc}"}}',NULL,{start},{stop});

  -- read the incident's error logs, newest first
  SELECT ts, level, message FROM logs
   WHERE service='{svc}' AND level='error' ORDER BY ts DESC LIMIT 20;

  -- slow error traces during the incident window ({} .. {})
  SELECT lower(hex(trace_id)), start_ts, duration_ns/1000000 AS ms FROM spans
   WHERE status='error' AND start_ts BETWEEN {}000000 AND {}000000
   ORDER BY duration_ns DESC LIMIT 10;

Error logs carry a real trace_id in their metadata about half the time —
json_extract(metadata,'$.trace_id') pivots a log line to its trace.
"#,
        incident.start_ms, incident.end_ms, incident.start_ms, incident.end_ms
    );
}

fn main() {
    let o = parse_opts();
    let cfg = build_config(&o);
    let incident = cfg.incident();
    let ext = find_ext(&o.ext);

    let exists = std::path::Path::new(&o.db).exists();
    if o.cmd == "seed" && exists && !o.append {
        eprintln!(
            "{} already exists — pass --append to add more data, or remove it first",
            o.db
        );
        std::process::exit(1);
    }
    if o.cmd == "live" && !exists {
        eprintln!("{} does not exist — run `seed` first (or `seed --follow`)", o.db);
        std::process::exit(1);
    }

    let conn = open_db(&o.db, &ext);
    ensure_tables(&conn);

    let catalog = build_catalog(&cfg);
    let mut reservoir = TraceReservoir::new(20_000);

    if o.cmd == "seed" {
        println!(
            "seeding {} — profile {}: {} services x {} pods = {} series, {} min window, incident on '{}'",
            o.db,
            o.profile,
            cfg.services,
            cfg.pods,
            fmt_count(catalog.len()),
            cfg.minutes,
            cfg.service_name(incident.service),
        );
        let t0 = Instant::now();
        reservoir = seed(&conn, &cfg, &incident, &catalog);
        let bytes = std::fs::metadata(&o.db).map(|m| m.len()).unwrap_or(0);
        println!(
            "done in {:.1}s — {} ({:.1} MB)",
            t0.elapsed().as_secs_f64(),
            o.db,
            bytes as f64 / 1e6
        );
        cheat_sheet(&cfg, &incident);
    }

    if o.cmd == "live" || o.follow {
        let steps = if o.cmd == "seed" || exists { cfg.steps() } else { 0 };
        live(&conn, &cfg, &incident, &catalog, &mut reservoir, &o, steps);
    }
}
