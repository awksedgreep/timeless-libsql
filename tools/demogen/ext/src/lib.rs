//! Demogen as a loadable SQLite extension: the whole demo runs inside one
//! sqlite3 session.
//!
//! ```text
//! sqlite3 demo.db
//! .load ./libtimeless_ext.so
//! .load ./libtimeless_demogen.so
//! .timer on
//! SELECT timeless_demo('seed', 'medium');   -- progress on stderr, summary as the result
//! SELECT timeless_demo('follow', 60);       -- append live data for 60s
//! ```
//!
//! The scalar runs its inserts reentrantly on the calling connection (the
//! same technique as SQLite's own eval() extension), through exactly the
//! public Tier 2 batch surface any producer would use. A tiny
//! `demogen_state` table in the demo database lets `tick`/`follow` continue
//! every random walk and counter across calls and sessions.

use std::os::raw::{c_char, c_int};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::ffi;
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::{params, Connection, OptionalExtension, Result};

use demogen_core::drive::{
    drive_logs, drive_metrics, drive_traces, format_report, profile, warm_states, SignalReport,
    LIVE_LOG_RATE, LIVE_TRACE_RATE,
};
use demogen_core::fleet::{build_catalog, Config, Incident, Rng, SeriesSpec, TraceReservoir};

/// Entry points. SQLite's `.load` looks for `sqlite3_extension_init` and,
/// as a fallback, the name derived from the file name
/// (`libtimeless_demogen.so` → `sqlite3_timelessdemogen_init`). Export
/// both, like the telemetry artifact does.
/// # Safety
/// Called by SQLite with valid pointers during extension loading.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, extension_init)
}

/// # Safety
/// Called by SQLite with valid pointers during extension loading.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_timelessdemogen_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, extension_init)
}

fn extension_init(db: Connection) -> Result<bool> {
    db.create_scalar_function(
        c"timeless_demo",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY,
        |ctx| dispatch(ctx).map_err(|e| rusqlite::Error::UserFunctionError(e.into())),
    )?;
    Ok(false)
}

const INFO: &str = r#"timeless_demo — synthetic telemetry, generated in-session

  SELECT timeless_demo('seed', 'small'|'medium'|'large'[, seed_int]);
      Populate this database: metrics fleet + logs + traces, with one
      incident baked into the middle third of the window. Progress goes
      to stderr; the result is an ingest/compression summary.
        small  ≈  4k series, 200k logs, ~200k spans, 30 min
        medium ≈ 35k series,   2M logs,   ~1M spans, 60 min
        large  ≈ 245k series,  5M logs,   ~3M spans, 60 min

  SELECT timeless_demo('tick'[, seconds]);      -- default 15
      Instantly append the last N seconds of fleet activity.

  SELECT timeless_demo('follow'[, seconds]);    -- default 60
      Append in real time (flushing every ~2s) for N seconds — pair with
      a live tail or a dashboard in another terminal.

  SELECT timeless_demo('report');
      Compression/storage report: raw logical bytes ingested vs the engine
      block bytes stored for that data — indexes listed separately, WAL
      checkpointed and free pages vacuumed first so neither muddies it.

Load order matters: .load libtimeless_ext.so first, then this artifact.
On a fresh database, run these two BEFORE seeding (in this order — the
first must precede any write to the file):
  PRAGMA auto_vacuum=INCREMENTAL;   -- lets report/vacuum return freed pages
  PRAGMA journal_mode=WAL;          -- concurrent readers during follow

Then explore (see docs/SQL_API_REFERENCE.md for the full surface):
  SELECT count(*) FROM timeless_series('metrics');
  SELECT ts, level, message FROM logs WHERE service='auth' AND level='error'
   ORDER BY ts DESC LIMIT 20;
  SELECT key, value FROM timeless_stats('metrics');"#;

fn dispatch(ctx: &Context) -> std::result::Result<String, String> {
    if ctx.len() == 0 {
        return Ok(INFO.to_string());
    }
    let cmd: String = ctx.get(0).map_err(|e| e.to_string())?;
    // Reentrant handle on the calling connection; single-threaded use only.
    let conn = unsafe { ctx.get_connection() }.map_err(|e| e.to_string())?;
    match cmd.as_str() {
        "info" | "help" => Ok(INFO.to_string()),
        "seed" => {
            let profile_name: String = if ctx.len() > 1 {
                ctx.get(1).map_err(|e| e.to_string())?
            } else {
                "medium".to_string()
            };
            let seed: i64 = if ctx.len() > 2 {
                ctx.get(2).map_err(|e| e.to_string())?
            } else {
                42
            };
            seed_cmd(&conn, &profile_name, seed as u64)
        }
        "tick" => {
            let secs: i64 = if ctx.len() > 1 { ctx.get(1).map_err(|e| e.to_string())? } else { 15 };
            tick_cmd(&conn, secs.clamp(1, 3600))
        }
        "follow" => {
            let secs: i64 = if ctx.len() > 1 { ctx.get(1).map_err(|e| e.to_string())? } else { 60 };
            follow_cmd(&conn, secs.clamp(1, 86_400))
        }
        "report" => report_cmd(&conn),
        other => Err(format!(
            "unknown command '{other}' — try SELECT timeless_demo('info');"
        )),
    }
}

// ---------------------------------------------------------------------------
// Persistent generator state (one row; lets tick/follow continue walks)
// ---------------------------------------------------------------------------

struct DemoState {
    cfg: Config,
    steps_done: usize,
    next_scrape_ms: i64,
    last_ms: i64,
    /// Cumulative raw logical bytes ingested per signal (seed + ticks),
    /// the numerator of every compression claim in `report`.
    metrics_raw: u64,
    logs_raw: u64,
    spans_raw: u64,
}

fn ensure_state_table(conn: &Connection) -> std::result::Result<(), String> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS demogen_state(key TEXT PRIMARY KEY, value TEXT);")
        .map_err(|e| e.to_string())
}

fn load_state(conn: &Connection) -> std::result::Result<Option<DemoState>, String> {
    ensure_state_table(conn)?;
    let row: Option<String> = conn
        .query_row("SELECT value FROM demogen_state WHERE key='v1'", [], |r| r.get(0))
        .optional()
        .map_err(|e| e.to_string())?;
    let Some(row) = row else { return Ok(None) };
    let v: Vec<i64> = row.split_whitespace().filter_map(|t| t.parse().ok()).collect();
    // 12 fields was the pre-report layout; raw counters default to zero.
    if v.len() != 12 && v.len() != 15 {
        return Err("demogen_state is corrupt — remove the row or start a fresh db".into());
    }
    let raw = |i: usize| v.get(i).copied().unwrap_or(0) as u64;
    Ok(Some(DemoState {
        cfg: Config {
            seed: v[0] as u64,
            services: v[1] as usize,
            pods: v[2] as usize,
            paths: v[3] as usize,
            minutes: v[4] as u64,
            step_secs: v[5] as u64,
            logs: v[6] as usize,
            traces: v[7] as usize,
            end_ms: v[8],
        },
        steps_done: v[9] as usize,
        next_scrape_ms: v[10],
        last_ms: v[11],
        metrics_raw: raw(12),
        logs_raw: raw(13),
        spans_raw: raw(14),
    }))
}

fn save_state(conn: &Connection, st: &DemoState) -> std::result::Result<(), String> {
    let c = &st.cfg;
    let value = format!(
        "{} {} {} {} {} {} {} {} {} {} {} {} {} {} {}",
        c.seed, c.services, c.pods, c.paths, c.minutes, c.step_secs, c.logs, c.traces, c.end_ms,
        st.steps_done, st.next_scrape_ms, st.last_ms,
        st.metrics_raw, st.logs_raw, st.spans_raw
    );
    conn.execute(
        "INSERT OR REPLACE INTO demogen_state(key, value) VALUES ('v1', ?1)",
        params![value],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
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

fn ensure_tables(conn: &Connection) -> std::result::Result<(), String> {
    // Best-effort on a fresh database: incremental auto_vacuum lets
    // `report` return freed pages to the OS view of the file. Both can
    // fail depending on when we're called — never fatal.
    let _ = conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;");
    let _ = conn.execute_batch("PRAGMA synchronous = NORMAL;");
    conn.execute_batch(
        "CREATE VIRTUAL TABLE IF NOT EXISTS metrics USING timeless_metrics;
         CREATE VIRTUAL TABLE IF NOT EXISTS logs
             USING timeless_logs(index_keys='service,path,status');
         CREATE VIRTUAL TABLE IF NOT EXISTS spans USING timeless_traces;",
    )
    .map_err(|e| {
        format!("{e} — is libtimeless_ext loaded? (.load it before libtimeless_demogen)")
    })
}

fn command(conn: &Connection, table: &str, cmd: &str) -> std::result::Result<(), String> {
    conn.execute(&format!("INSERT INTO {table}({table}) VALUES ('{cmd}')"), [])
        .map(|_| ())
        .map_err(|e| format!("{table} {cmd}: {e}"))
}

/// One integer counter from the public timeless_stats surface.
fn stat(conn: &Connection, tbl: &str, key: &str) -> std::result::Result<i64, String> {
    conn.query_row(
        "SELECT CAST(value AS INTEGER) FROM timeless_stats(?1) WHERE key = ?2",
        params![tbl, key],
        |r| r.get(0),
    )
    .map_err(|e| format!("timeless_stats({tbl}).{key}: {e}"))
}

/// Keep bookkeeping out of the compression story: move WAL frames back
/// into the main file and hand freed pages back. Both are best-effort —
/// a concurrent reader can legitimately block a full checkpoint.
fn checkpoint_and_vacuum(conn: &Connection) {
    let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
    if let Ok(mut stmt) = conn.prepare("PRAGMA incremental_vacuum;") {
        if let Ok(mut rows) = stmt.query([]) {
            while let Ok(Some(_)) = rows.next() {}
        }
    }
    let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
}

/// The compression/storage table: raw logical bytes (tracked at
/// generation time in demogen_state) vs the engine's own on-disk payload
/// and index counters.
fn storage_report(conn: &Connection, st: &DemoState) -> std::result::Result<String, String> {
    checkpoint_and_vacuum(conn);
    let signals = [
        SignalReport {
            label: "metrics",
            unit: "samples",
            per: "sample",
            items: stat(conn, "metrics", "disk_points")? as u64,
            raw_bytes: st.metrics_raw,
            payload_bytes: stat(conn, "metrics", "bytes_on_disk")? as u64,
            index_bytes: stat(conn, "metrics", "index_bytes")? as u64,
        },
        SignalReport {
            label: "logs",
            unit: "entries",
            per: "entry",
            items: stat(conn, "logs", "disk_entries")? as u64,
            raw_bytes: st.logs_raw,
            payload_bytes: stat(conn, "logs", "bytes_on_disk")? as u64,
            index_bytes: stat(conn, "logs", "index_bytes")? as u64,
        },
        SignalReport {
            label: "traces",
            unit: "spans",
            per: "span",
            items: stat(conn, "spans", "disk_spans")? as u64,
            raw_bytes: st.spans_raw,
            payload_bytes: stat(conn, "spans", "bytes_on_disk")? as u64,
            index_bytes: stat(conn, "spans", "index_bytes")? as u64,
        },
    ];
    let (file, free): (i64, i64) = conn
        .query_row(
            "SELECT page_count * page_size, freelist_count * page_size
               FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size()",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|e| e.to_string())?;
    Ok(format_report(&signals, file as u64, free as u64))
}

fn report_cmd(conn: &Connection) -> std::result::Result<String, String> {
    let st = load_state(conn)?
        .ok_or("no demogen state in this database — run timeless_demo('seed', ...) first")?;
    command(conn, "metrics", "flush")?;
    command(conn, "logs", "flush")?;
    command(conn, "spans", "flush")?;
    storage_report(conn, &st)
}

// ---------------------------------------------------------------------------
// seed
// ---------------------------------------------------------------------------

fn seed_cmd(conn: &Connection, profile_name: &str, seed: u64) -> std::result::Result<String, String> {
    if load_state(conn)?.is_some() {
        return Err(
            "this database is already seeded — use timeless_demo('tick'/'follow'), \
             or start a fresh db file"
                .into(),
        );
    }
    let Some(spec) = profile(profile_name) else {
        return Err(format!("unknown profile '{profile_name}' (small|medium|large)"));
    };
    ensure_tables(conn)?;

    let cfg = spec.config(seed, now_ms());
    let incident = cfg.incident();
    let catalog = build_catalog(&cfg);
    let mut reservoir = TraceReservoir::new(20_000);
    eprintln!(
        "seeding profile {profile_name}: {} services x {} pods = {} series, {} min window, incident on '{}'",
        cfg.services,
        cfg.pods,
        fmt_count(catalog.len()),
        cfg.minutes,
        cfg.service_name(incident.service)
    );

    let t0 = Instant::now();
    let mut stmt = conn
        .prepare("INSERT INTO spans(spans) VALUES (?1)")
        .map_err(|e| e.to_string())?;
    let mut rng = Rng::new(cfg.seed ^ 0x0724_7CE5);
    let mut sink = |blob: &[u8], total: usize| {
        stmt.execute(params![blob]).map_err(|e| e.to_string())?;
        eprint!("\r  traces: {} spans", fmt_count(total));
        Ok(())
    };
    let traces_t = drive_traces(
        &cfg, &incident, &mut rng, cfg.start_ms(), cfg.end_ms, cfg.traces,
        &mut reservoir, &mut sink,
    )?;
    drop(sink);
    drop(stmt);
    let trace_secs = t0.elapsed().as_secs_f64();
    eprintln!("\r  traces: {} spans in {:.1}s", fmt_count(traces_t.items), trace_secs);

    let t1 = Instant::now();
    let mut stmt = conn
        .prepare("INSERT INTO logs(logs) VALUES (?1)")
        .map_err(|e| e.to_string())?;
    let mut rng = Rng::new(cfg.seed ^ 0x1065);
    let mut sink = |blob: &[u8], total: usize| {
        stmt.execute(params![blob]).map_err(|e| e.to_string())?;
        eprint!("\r  logs: {} entries", fmt_count(total));
        Ok(())
    };
    let logs_t = drive_logs(
        &cfg, &incident, &mut rng, cfg.start_ms(), cfg.end_ms, cfg.logs, &reservoir, &mut sink,
    )?;
    drop(sink);
    drop(stmt);
    let log_secs = t1.elapsed().as_secs_f64();
    eprintln!("\r  logs: {} entries in {:.1}s", fmt_count(logs_t.items), log_secs);

    let t2 = Instant::now();
    let mut stmt = conn
        .prepare("INSERT INTO metrics(metrics) VALUES (?1)")
        .map_err(|e| e.to_string())?;
    let mut states = warm_states(&catalog, &cfg, &incident, 0);
    let step_ms = (cfg.step_secs * 1000) as i64;
    let start = cfg.start_ms();
    let mut sink = |blob: &[u8], total: usize| {
        stmt.execute(params![blob]).map_err(|e| e.to_string())?;
        eprint!("\r  metrics: {} samples", fmt_count(total));
        Ok(())
    };
    let metrics_t = drive_metrics(
        &cfg, &incident, &catalog, &mut states, 0, cfg.steps(),
        &|i| start + i as i64 * step_ms, &mut sink,
    )?;
    drop(sink);
    drop(stmt);
    let metric_secs = t2.elapsed().as_secs_f64();
    eprintln!("\r  metrics: {} samples in {:.1}s", fmt_count(metrics_t.items), metric_secs);

    let tm = Instant::now();
    command(conn, "metrics", "flush")?;
    command(conn, "logs", "flush")?;
    command(conn, "spans", "flush")?;
    command(conn, "metrics", "compact")?;
    command(conn, "logs", "optimize")?;
    command(conn, "spans", "optimize")?;
    let publish_secs = tm.elapsed().as_secs_f64();
    eprintln!("  publish (flush+compact+optimize): {publish_secs:.1}s");

    let state = DemoState {
        steps_done: cfg.steps(),
        next_scrape_ms: cfg.end_ms + step_ms,
        last_ms: cfg.end_ms,
        metrics_raw: metrics_t.raw_bytes,
        logs_raw: logs_t.raw_bytes,
        spans_raw: traces_t.raw_bytes,
        cfg: cfg.clone(),
    };
    save_state(conn, &state)?;

    let report = storage_report(conn, &state)?;
    Ok(format!(
        "seeded profile '{profile_name}' (seed {seed})\n\
         \x20 series:  {} ({} services x {} pods)\n\
         \x20 metrics: {} samples in {:.1}s ({:.1}M samples/s)\n\
         \x20 logs:    {} entries in {:.1}s ({:.0}k entries/s)\n\
         \x20 traces:  {} spans in {:.1}s ({:.0}k spans/s)\n\
         \x20 publish: {:.1}s (flush + compact + optimize)\n\
         {report}\n\
         \x20 window:  {} .. {} (unix ms), incident on '{}' {} .. {}\n\
         next: SELECT timeless_demo('info');",
        fmt_count(catalog.len()),
        cfg.services,
        cfg.pods,
        fmt_count(metrics_t.items),
        metric_secs,
        metrics_t.items as f64 / metric_secs.max(1e-9) / 1e6,
        fmt_count(logs_t.items),
        log_secs,
        logs_t.items as f64 / log_secs.max(1e-9) / 1e3,
        fmt_count(traces_t.items),
        trace_secs,
        traces_t.items as f64 / trace_secs.max(1e-9) / 1e3,
        publish_secs,
        cfg.start_ms(),
        cfg.end_ms,
        cfg.service_name(cfg.incident().service),
        cfg.incident().start_ms,
        cfg.incident().end_ms,
    ))
}

// ---------------------------------------------------------------------------
// tick / follow
// ---------------------------------------------------------------------------

struct LiveSession {
    cfg: Config,
    incident: Incident,
    catalog: Vec<SeriesSpec>,
    states: Vec<demogen_core::fleet::SeriesState>,
    reservoir: TraceReservoir,
    rng: Rng,
    st: DemoState,
}

fn open_session(conn: &Connection) -> std::result::Result<LiveSession, String> {
    let st = load_state(conn)?
        .ok_or("no demogen state in this database — run timeless_demo('seed', ...) first")?;
    let cfg = st.cfg.clone();
    let incident = cfg.incident();
    let catalog = build_catalog(&cfg);
    let states = warm_states(&catalog, &cfg, &incident, st.steps_done);
    let rng = Rng::new(cfg.seed ^ st.last_ms as u64 ^ 0x11FE);
    Ok(LiveSession { cfg, incident, catalog, states, reservoir: TraceReservoir::new(20_000), rng, st })
}

/// Append fleet activity for (from_ms, to_ms]; returns (logs, spans, samples).
fn append_window(
    conn: &Connection,
    s: &mut LiveSession,
    from_ms: i64,
    to_ms: i64,
) -> std::result::Result<(usize, usize, usize), String> {
    let secs = (to_ms - from_ms).max(0) as f64 / 1000.0;

    let mut spans = 0usize;
    let n_traces = (LIVE_TRACE_RATE as f64 * secs) as usize;
    if n_traces > 0 {
        let mut stmt = conn
            .prepare("INSERT INTO spans(spans) VALUES (?1)")
            .map_err(|e| e.to_string())?;
        let mut sink = |blob: &[u8], _total: usize| {
            stmt.execute(params![blob]).map_err(|e| e.to_string())?;
            Ok(())
        };
        let t = drive_traces(
            &s.cfg, &s.incident, &mut s.rng, from_ms, to_ms, n_traces,
            &mut s.reservoir, &mut sink,
        )?;
        spans = t.items;
        s.st.spans_raw += t.raw_bytes;
    }

    let mut entries = 0usize;
    let n_logs = (LIVE_LOG_RATE as f64 * secs) as usize;
    if n_logs > 0 {
        let mut stmt = conn
            .prepare("INSERT INTO logs(logs) VALUES (?1)")
            .map_err(|e| e.to_string())?;
        let mut sink = |blob: &[u8], _total: usize| {
            stmt.execute(params![blob]).map_err(|e| e.to_string())?;
            Ok(())
        };
        let t = drive_logs(
            &s.cfg, &s.incident, &mut s.rng, from_ms, to_ms, n_logs, &s.reservoir, &mut sink,
        )?;
        entries = t.items;
        s.st.logs_raw += t.raw_bytes;
    }

    let mut samples = 0usize;
    let step_ms = (s.cfg.step_secs * 1000) as i64;
    while s.st.next_scrape_ms <= to_ms {
        let ts = s.st.next_scrape_ms;
        let step = s.st.steps_done;
        let mut stmt = conn
            .prepare("INSERT INTO metrics(metrics) VALUES (?1)")
            .map_err(|e| e.to_string())?;
        let mut sink = |blob: &[u8], _total: usize| {
            stmt.execute(params![blob]).map_err(|e| e.to_string())?;
            Ok(())
        };
        let t = drive_metrics(
            &s.cfg, &s.incident, &s.catalog, &mut s.states, step, 1, &|_| ts, &mut sink,
        )?;
        samples += t.items;
        s.st.metrics_raw += t.raw_bytes;
        s.st.steps_done += 1;
        s.st.next_scrape_ms += step_ms;
    }

    if samples > 0 {
        command(conn, "metrics", "flush")?;
    }
    command(conn, "logs", "flush")?;
    command(conn, "spans", "flush")?;
    s.st.last_ms = to_ms;
    save_state(conn, &s.st)?;
    Ok((entries, spans, samples))
}

fn tick_cmd(conn: &Connection, secs: i64) -> std::result::Result<String, String> {
    let mut s = open_session(conn)?;
    let now = now_ms();
    let from = s.st.last_ms.max(now - secs * 1000);
    let (logs, spans, samples) = append_window(conn, &mut s, from, now)?;
    Ok(format!(
        "appended {:.1}s of activity: +{} logs, +{} spans, +{} metric samples",
        (now - from) as f64 / 1000.0,
        fmt_count(logs),
        fmt_count(spans),
        fmt_count(samples)
    ))
}

fn follow_cmd(conn: &Connection, secs: i64) -> std::result::Result<String, String> {
    let mut s = open_session(conn)?;
    let deadline = Instant::now() + Duration::from_secs(secs as u64);
    let (mut logs, mut spans, mut samples, mut ticks) = (0usize, 0usize, 0usize, 0usize);
    eprintln!("following for {secs}s (flush every ~2s) — Ctrl-C interrupts");
    while Instant::now() < deadline {
        std::thread::sleep(
            Duration::from_secs(2).min(deadline.saturating_duration_since(Instant::now())),
        );
        let now = now_ms();
        let t0 = Instant::now();
        let from = s.st.last_ms;
        let (l, sp, sa) = append_window(conn, &mut s, from, now)?;
        logs += l;
        spans += sp;
        samples += sa;
        ticks += 1;
        let mut line = format!("  +{} logs, +{} spans", fmt_count(l), fmt_count(sp));
        if sa > 0 {
            line.push_str(&format!(", scraped {} samples", fmt_count(sa)));
        }
        eprintln!("{line} ({} ms)", t0.elapsed().as_millis());
    }
    Ok(format!(
        "followed for {secs}s ({ticks} ticks): +{} logs, +{} spans, +{} metric samples",
        fmt_count(logs),
        fmt_count(spans),
        fmt_count(samples)
    ))
}
