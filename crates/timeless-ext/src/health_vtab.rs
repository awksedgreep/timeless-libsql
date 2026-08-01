//! timeless_health: dbhealth — SQLite health telemetry stored, compressed,
//! in the database it measures (docs/DBHEALTH.md).
//!
//!   CREATE VIRTUAL TABLE dbhealth USING timeless_health;
//!   -- that's it: collection begins (see SCHEDULER below)
//!   SELECT * FROM dbhealth_report;
//!
//! The vtab IS a timeless_metrics table (HealthTab wraps MetricsTab and
//! delegates schema, reads, pushdown, transactions, savepoints, DROP —
//! including the F2 retention= and F3 rollups= CREATE arguments, which
//! are forwarded). It adds:
//!
//!   1. The 'sample' command: read SQLite's own counters on the CALLING
//!      connection (sqlite3_db_status), db-level pragmas, and file
//!      sizes, appending them as points (docs/DBHEALTH.md inventory).
//!      'sample:auto' is the scheduler's variant: db-level gauges only,
//!      because connection-scoped counters measured from an idle
//!      sampler connection would be noise dressed as data.
//!   2. `flush_every=N` (default 1: every sample durable — the cron
//!      pattern loses everything otherwise) persisted in _meta.
//!   3. THE SCHEDULER: `every=N` seconds (default 60; every=0 disables).
//!      Creating OR re-opening a dbhealth table starts a background
//!      sampler thread for that (db file, table): its own connection,
//!      first sample ~2s after open, then every N seconds. Loading the
//!      extension and opening the database IS the collector — the way
//!      every monitoring tool since the beginning of time has worked.
//!      The thread stops when the engine is dropped (all connections
//!      closed), when the table is dropped, or on repeated failures.
//!      In-memory and temp databases get no scheduler (no file for an
//!      independent connection to open) — sample by command there.
//!
//! Companion views (created with the table, dropped with it):
//!   <t>_report   one row per health check: status ok|warn|attention|
//!                'no data', a human value, one piece of advice —
//!                worst first. Thresholds visible in the view SQL.
//!   <t>_now      latest value per series + age.
//!   <t>_trends   per-series daily min/avg/max over 7 days.

use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::{c_int, CStr, CString};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use rusqlite::ffi;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    CreateVTab, IndexInfo, Inserts, Module, TransactionVTab, UpdateVTab, Updates, VTab,
    VTabConnection, VTabKind,
};
use rusqlite::{Connection, Error, Result};
use timeless_core::Engine;

use crate::metrics_vtab::{MetricsCursor, MetricsTab};
use crate::shadow_meta;
use crate::shared::{DbGuard, SharedEngine};
use crate::sql_ident;
use crate::table_args;
use crate::vtab_tx::{self, SavepointVTab};

/// Register the dbhealth module (both spellings) on a connection.
pub(crate) fn register(db: &Connection) -> Result<()> {
    const MODULE: Module<HealthTab> = vtab_tx::update_module_with_savepoints();
    db.create_module(c"timeless_health", &MODULE, None::<()>)?;
    db.create_module(c"dbhealth", &MODULE, None::<()>)
}

const DEFAULT_FLUSH_EVERY: u32 = 1;
const DEFAULT_EVERY_SECS: u64 = 60;
const META_FLUSH_EVERY: &str = "health_flush_every";
const META_EVERY: &str = "health_every";
const META_VIEWS_VER: &str = "health_views_version";
/// Bump when views_ddl changes; connect refreshes older databases.
const VIEWS_VERSION: &str = "2";
/// The scheduler's first sample lands shortly after open, not a full
/// interval later — "collection begins" should be observable.
const SCHEDULER_INITIAL_DELAY: Duration = Duration::from_secs(2);

fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------

/// Cumulative per-connection counters captured at the previous 'sample',
/// the baseline for this sample's deltas.
struct CounterSnapshot {
    cache_hits: i64,
    cache_misses: i64,
    cache_writes: i64,
    cache_spills: i64,
    lookaside_hits: i64,
    lookaside_miss_full: i64,
}

/// One sqlite3_db_status counter's current value. resetFlg is ALWAYS 0:
/// sampling must not disturb counters other components may also read.
fn db_status(db: *mut ffi::sqlite3, op: c_int) -> Result<i64> {
    let mut current: c_int = 0;
    let mut highwater: c_int = 0;
    let rc = unsafe { ffi::sqlite3_db_status(db, op, &mut current, &mut highwater, 0) };
    if rc != ffi::SQLITE_OK {
        return Err(module_err(format!(
            "sqlite3_db_status(op={op}) failed with rc={rc}"
        )));
    }
    Ok(current as i64)
}

fn counter_snapshot(db: *mut ffi::sqlite3) -> Result<CounterSnapshot> {
    Ok(CounterSnapshot {
        cache_hits: db_status(db, ffi::SQLITE_DBSTATUS_CACHE_HIT)?,
        cache_misses: db_status(db, ffi::SQLITE_DBSTATUS_CACHE_MISS)?,
        cache_writes: db_status(db, ffi::SQLITE_DBSTATUS_CACHE_WRITE)?,
        cache_spills: db_status(db, ffi::SQLITE_DBSTATUS_CACHE_SPILL)?,
        lookaside_hits: db_status(db, ffi::SQLITE_DBSTATUS_LOOKASIDE_HIT)?,
        lookaside_miss_full: db_status(db, ffi::SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL)?,
    })
}

/// The on-disk path of this schema's database file, None for temp or
/// in-memory databases (sqlite3_db_filename returns NULL or "").
fn db_file_path(db: *mut ffi::sqlite3, database_name: &str) -> Option<String> {
    let schema = CString::new(database_name).ok()?;
    let ptr = unsafe { ffi::sqlite3_db_filename(db, schema.as_ptr()) };
    if ptr.is_null() {
        return None;
    }
    let path = unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned();
    if path.is_empty() {
        None
    } else {
        Some(path)
    }
}

/// Database-level gauges: memory, page counts, bloat, file sizes.
/// Valid from ANY connection (the scheduler's included).
fn gauge_points(db: *mut ffi::sqlite3, database_name: &str) -> Result<Vec<(&'static str, f64)>> {
    let mut points: Vec<(&'static str, f64)> = Vec::with_capacity(16);
    points.push((
        "cache_used_bytes",
        db_status(db, ffi::SQLITE_DBSTATUS_CACHE_USED)? as f64,
    ));
    points.push((
        "schema_used_bytes",
        db_status(db, ffi::SQLITE_DBSTATUS_SCHEMA_USED)? as f64,
    ));
    points.push((
        "stmt_used_bytes",
        db_status(db, ffi::SQLITE_DBSTATUS_STMT_USED)? as f64,
    ));
    points.push(("memory_used_bytes", unsafe { ffi::sqlite3_memory_used() }
        as f64));

    let host = unsafe { Connection::from_handle(db) }?;
    let schema = sql_ident::quote(database_name);
    let page_count: i64 =
        host.query_row(&format!("PRAGMA {schema}.page_count"), [], |r| r.get(0))?;
    let freelist: i64 =
        host.query_row(&format!("PRAGMA {schema}.freelist_count"), [], |r| r.get(0))?;
    points.push(("db_pages", page_count as f64));
    points.push(("freelist_pages", freelist as f64));
    if page_count > 0 {
        points.push(("bloat_ratio", freelist as f64 / page_count as f64));
    }
    if let Some(path) = db_file_path(db, database_name) {
        if let Ok(md) = std::fs::metadata(&path) {
            points.push(("db_file_bytes", md.len() as f64));
        }
        let wal_len = std::fs::metadata(format!("{path}-wal"))
            .map(|md| md.len())
            .unwrap_or(0);
        points.push(("wal_file_bytes", wal_len as f64));
    }
    Ok(points)
}

/// Read a _meta value written by ANY dbhealth version: v1 stored these
/// keys as BLOBs (raw bytes of the decimal string), v2/master stores
/// TEXT. Never let an old database fail to open over an encoding.
fn load_meta_relaxed(
    host: &Connection,
    database: &str,
    table: &str,
    key: &str,
) -> Result<Option<String>> {
    use rusqlite::OptionalExtension;
    let meta = sql_ident::qualified_shadow(database, table, "meta");
    host.query_row(
        &format!("SELECT v FROM {meta} WHERE k = ?1"),
        [key],
        |row| {
            Ok(match row.get_ref(0)? {
                ValueRef::Text(t) => Some(String::from_utf8_lossy(t).into_owned()),
                ValueRef::Blob(b) => Some(String::from_utf8_lossy(b).into_owned()),
                ValueRef::Integer(i) => Some(i.to_string()),
                ValueRef::Real(f) => Some(f.to_string()),
                ValueRef::Null => None,
            })
        },
    )
    .optional()
    .map(Option::flatten)
}

// ---------------------------------------------------------------------------
// The scheduler — collection begins when the table exists
// ---------------------------------------------------------------------------

struct SchedulerSlot {
    stop: Arc<AtomicBool>,
}

fn schedulers() -> &'static Mutex<HashMap<String, SchedulerSlot>> {
    static S: OnceLock<Mutex<HashMap<String, SchedulerSlot>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(HashMap::new()))
}

fn scheduler_key(file: &str, table: &str) -> String {
    format!("{file}\u{1}{table}")
}

/// Idempotently start the sampler thread for (db file, table). The
/// thread owns its own connection so it never touches anyone else's;
/// the shared-engine registry makes its samples land in the SAME engine
/// every other connection sees.
fn ensure_scheduler(
    file: Option<String>,
    table: &str,
    every_secs: u64,
    engine: &Arc<SharedEngine<Engine>>,
) {
    let Some(file) = file else { return }; // :memory:/temp — command-only
    if every_secs == 0 {
        return;
    }
    // Under sqld the server VIRTUALIZES the WAL for replication; an
    // out-of-band connection writing to the managed file could desync
    // the replication log. sqld's conventional layout keeps databases
    // under a `<name>.sqld/` directory — skip the embedded scheduler
    // there and let collection flow through the front door instead
    // (an HTTP 'sample' on a timer measures REAL pooled connections,
    // which is better data anyway; see docs/DBHEALTH.md).
    if file.contains(".sqld/") {
        return;
    }
    let key = scheduler_key(&file, table);
    let mut map = schedulers().lock().unwrap();
    if map.contains_key(&key) {
        return;
    }
    let stop = Arc::new(AtomicBool::new(false));
    map.insert(
        key.clone(),
        SchedulerSlot {
            stop: Arc::clone(&stop),
        },
    );
    drop(map);

    let weak: Weak<SharedEngine<Engine>> = Arc::downgrade(engine);
    let table = table.to_owned();
    std::thread::Builder::new()
        .name(format!("dbhealth:{table}"))
        .spawn(move || scheduler_main(file, table, every_secs, weak, stop))
        .ok();
}

fn stop_scheduler(file: Option<&str>, table: &str) {
    if let Some(file) = file {
        let key = scheduler_key(file, table);
        if let Some(slot) = schedulers().lock().unwrap().remove(&key) {
            slot.stop.store(true, Ordering::Relaxed);
        }
    }
}

/// Interruptible sleep: false = told to stop / engine gone.
fn scheduler_wait(d: Duration, weak: &Weak<SharedEngine<Engine>>, stop: &AtomicBool) -> bool {
    let mut remaining = d;
    let step = Duration::from_millis(250);
    while remaining > Duration::ZERO {
        if stop.load(Ordering::Relaxed) || weak.upgrade().is_none() {
            return false;
        }
        std::thread::sleep(remaining.min(step));
        remaining = remaining.saturating_sub(step);
    }
    !(stop.load(Ordering::Relaxed) || weak.upgrade().is_none())
}

fn scheduler_main(
    file: String,
    table: String,
    every_secs: u64,
    weak: Weak<SharedEngine<Engine>>,
    stop: Arc<AtomicBool>,
) {
    let key = scheduler_key(&file, &table);
    let quoted = sql_ident::quote(&table);
    let sample_sql = format!("INSERT INTO {quoted}({quoted}) VALUES ('sample:auto')");

    let mut conn: Option<Connection> = None;
    let mut consecutive_errors = 0u32;
    let mut wait = SCHEDULER_INITIAL_DELAY;
    loop {
        if !scheduler_wait(wait, &weak, &stop) {
            break;
        }
        wait = Duration::from_secs(every_secs.max(1));

        let result = (|| -> Result<()> {
            if conn.is_none() {
                let c = Connection::open(&file)?;
                c.busy_timeout(Duration::from_secs(5))?;
                register(&c)?;
                conn = Some(c);
            }
            conn.as_ref().unwrap().execute(&sample_sql, [])?;
            Ok(())
        })();
        match result {
            Ok(()) => consecutive_errors = 0,
            Err(_) => {
                // A failed cycle drops the connection (it may be stale)
                // and retries next tick; a persistent failure gives up
                // rather than spinning forever against a broken db.
                conn = None;
                consecutive_errors += 1;
                if consecutive_errors >= 5 {
                    break;
                }
            }
        }
    }
    schedulers().lock().unwrap().remove(&key);
}

// ---------------------------------------------------------------------------
// Companion views (identical contract to the pre-0.2.0 dbhealth)
// ---------------------------------------------------------------------------

/// Threshold cheat-sheet (the opinions, kept in one place):
///   sampling freshness   ok ≤ 2h,   warn ≤ 2d,      else attention
///   cache_hit_ratio_24h  ok ≥ 0.90, warn ≥ 0.60,    else attention
///   bloat (free pages)   ok < 10%,  warn < 25%,     else attention
///   wal_size             ok ≤ max(4MB, db size), warn ≤ 4x db, else attention
///   cache_spills_24h     ok = 0,    warn ≤ 100,     else attention
///   stmt_memory          ok < 4MB,  warn < 32MB,    else attention
///   db_growth_7d         ok < 10MB or ≤ 1.5x, warn ≤ 3x, else attention
fn views_ddl(database: &str, table: &str) -> String {
    let t = sql_ident::quote(table);
    let now = sql_ident::qualified(database, &format!("{table}_now"));
    let now_ref = sql_ident::quote(&format!("{table}_now"));
    let report = sql_ident::qualified(database, &format!("{table}_report"));
    let trends = sql_ident::qualified(database, &format!("{table}_trends"));

    format!(
        r#"
CREATE VIEW IF NOT EXISTS {now} AS
SELECT a.name, a.value, a.ts, unixepoch() - a.ts AS age_seconds
  FROM {t} a
  JOIN (SELECT name, max(ts) AS ts FROM {t} GROUP BY name) m
    ON a.name = m.name AND a.ts = m.ts
 GROUP BY a.name;

CREATE VIEW IF NOT EXISTS {trends} AS
SELECT name,
       date(ts, 'unixepoch') AS day,
       count(*)              AS samples,
       round(min(value), 3)  AS min_value,
       round(avg(value), 3)  AS avg_value,
       round(max(value), 3)  AS max_value
  FROM {t}
 WHERE ts > unixepoch() - 7 * 86400
 GROUP BY name, day
 ORDER BY name, day;

CREATE VIEW IF NOT EXISTS {report} AS
SELECT "check", status, value, advice FROM (

SELECT 'sampling' AS "check",
  CASE WHEN t IS NULL THEN 'no data'
       WHEN age <= 7200 THEN 'ok'
       WHEN age <= 172800 THEN 'warn'
       ELSE 'attention' END AS status,
  CASE WHEN t IS NULL THEN '—'
       WHEN age < 120 THEN CAST(age AS TEXT) || 's ago'
       WHEN age < 7200 THEN CAST(age / 60 AS TEXT) || 'm ago'
       ELSE CAST(age / 3600 AS TEXT) || 'h ago' END AS value,
  CASE WHEN t IS NULL THEN
         'no samples yet — the scheduler lands one within seconds of open'
       WHEN age <= 7200 THEN '—'
       ELSE 'sampling has stopped; is the database ever opened?' END AS advice
FROM (SELECT max(ts) AS t, unixepoch() - max(ts) AS age FROM {t})

UNION ALL
SELECT 'cache_hit_ratio_24h',
  CASE WHEN v IS NULL THEN 'no data'
       WHEN v >= 0.90 THEN 'ok'
       WHEN v >= 0.60 THEN 'warn'
       ELSE 'attention' END,
  CASE WHEN v IS NULL THEN '—' ELSE printf('%.3f', v) END,
  CASE WHEN v IS NULL THEN
         'ratio needs in-connection samples: run ''sample'' from your app'
       WHEN v >= 0.90 THEN '—'
       ELSE 'page cache misses are high; raise PRAGMA cache_size' END
FROM (SELECT avg(value) AS v FROM {t}
       WHERE name = 'cache_hit_ratio' AND ts > unixepoch() - 86400)

UNION ALL
SELECT 'bloat',
  CASE WHEN v IS NULL THEN 'no data'
       WHEN v < 0.10 THEN 'ok'
       WHEN v < 0.25 THEN 'warn'
       ELSE 'attention' END,
  CASE WHEN v IS NULL THEN '—'
       ELSE printf('%d%% free pages', CAST(round(v * 100) AS INTEGER)) END,
  CASE WHEN v IS NULL THEN 'needs at least one sample'
       WHEN v < 0.10 THEN '—'
       ELSE 'free pages accumulate after deletes; PRAGMA incremental_vacuum(1000) returns them to the OS in slices' END
FROM (SELECT (SELECT value FROM {now_ref} WHERE name = 'bloat_ratio') AS v)

UNION ALL
SELECT 'wal_size',
  CASE WHEN wal IS NULL OR db IS NULL THEN 'no data'
       WHEN wal <= 4194304 OR wal <= db THEN 'ok'
       WHEN wal <= 4 * db THEN 'warn'
       ELSE 'attention' END,
  CASE WHEN wal IS NULL OR db IS NULL THEN '—'
       ELSE printf('%.1f MB (db %.1f MB)', wal / 1048576.0, db / 1048576.0) END,
  CASE WHEN wal IS NULL OR db IS NULL THEN 'needs at least one sample'
       WHEN wal <= 4194304 OR wal <= db THEN '—'
       ELSE 'the WAL outgrows the database between checkpoints; look for long-lived read transactions, or run PRAGMA wal_checkpoint(TRUNCATE)' END
FROM (SELECT (SELECT value FROM {now_ref} WHERE name = 'wal_file_bytes') AS wal,
             (SELECT value FROM {now_ref} WHERE name = 'db_file_bytes')  AS db)

UNION ALL
SELECT 'cache_spills_24h',
  CASE WHEN s IS NULL THEN 'no data'
       WHEN s = 0 THEN 'ok'
       WHEN s <= 100 THEN 'warn'
       ELSE 'attention' END,
  CASE WHEN s IS NULL THEN '—' ELSE CAST(CAST(s AS INTEGER) AS TEXT) END,
  CASE WHEN s IS NULL THEN
         'spill deltas need in-connection samples: run ''sample'' from your app'
       WHEN s = 0 THEN '—'
       ELSE 'transactions overflow the page cache mid-write; raise PRAGMA cache_size' END
FROM (SELECT sum(value) AS s FROM {t}
       WHERE name = 'cache_spills' AND ts > unixepoch() - 86400)

UNION ALL
SELECT 'stmt_memory',
  CASE WHEN v IS NULL THEN 'no data'
       WHEN v < 4194304 THEN 'ok'
       WHEN v < 33554432 THEN 'warn'
       ELSE 'attention' END,
  CASE WHEN v IS NULL THEN '—' ELSE printf('%.1f MB', v / 1048576.0) END,
  CASE WHEN v IS NULL THEN 'needs at least one sample'
       WHEN v < 4194304 THEN '—'
       ELSE 'prepared-statement memory keeps growing; look for statements prepared but never finalized' END
FROM (SELECT (SELECT value FROM {now_ref} WHERE name = 'stmt_used_bytes') AS v)

UNION ALL
SELECT 'db_growth',
  CASE WHEN o IS NULL OR l IS NULL THEN 'no data'
       WHEN l < 10485760 OR o <= 0 THEN 'ok'
       WHEN l <= 1.5 * o THEN 'ok'
       WHEN l <= 3 * o THEN 'warn'
       ELSE 'attention' END,
  -- The window is whatever data actually exists (up to 7d): saying
  -- "in 7d" on a four-minute-old database was a lie with decimals.
  CASE WHEN o IS NULL OR l IS NULL THEN '—'
       WHEN o <= 0 THEN printf('%.1f MB', l / 1048576.0)
       ELSE printf('%.1fx over ', l * 1.0 / o)
            || CASE WHEN age < 5400 THEN CAST(age / 60 AS TEXT) || 'm'
                    WHEN age < 172800 THEN CAST(age / 3600 AS TEXT) || 'h'
                    ELSE CAST(age / 86400 AS TEXT) || 'd' END
            || printf(' (now %.1f MB)', l / 1048576.0) END,
  CASE WHEN o IS NULL OR l IS NULL THEN 'needs at least one sample'
       WHEN l < 10485760 OR o <= 0 OR l <= 1.5 * o THEN '—'
       ELSE 'the file is growing fast; check that ''prune''/retention runs for your telemetry tables' END
FROM (SELECT (SELECT value FROM {t}
               WHERE name = 'db_file_bytes' AND ts > unixepoch() - 604800
               ORDER BY ts LIMIT 1) AS o,
             unixepoch() - (SELECT ts FROM {t}
               WHERE name = 'db_file_bytes' AND ts > unixepoch() - 604800
               ORDER BY ts LIMIT 1) AS age,
             (SELECT value FROM {now_ref} WHERE name = 'db_file_bytes') AS l)
)
ORDER BY CASE status WHEN 'attention' THEN 0 WHEN 'warn' THEN 1
                     WHEN 'no data' THEN 2 ELSE 3 END, "check";
"#
    )
}

fn drop_views_ddl(database: &str, table: &str) -> String {
    let now = sql_ident::qualified(database, &format!("{table}_now"));
    let report = sql_ident::qualified(database, &format!("{table}_report"));
    let trends = sql_ident::qualified(database, &format!("{table}_trends"));
    format!(
        "DROP VIEW IF EXISTS {report};\n\
         DROP VIEW IF EXISTS {trends};\n\
         DROP VIEW IF EXISTS {now};"
    )
}

// ---------------------------------------------------------------------------
// The virtual table: MetricsTab wrapped, health added
// ---------------------------------------------------------------------------

/// `#[repr(C)]` with the wrapped MetricsTab FIRST: MetricsTab is itself
/// repr(C) with `sqlite3_vtab base` first, so a pointer to HealthTab is
/// a valid pointer to sqlite3_vtab — the same C-style inheritance
/// contract every vtab in this crate relies on.
#[repr(C)]
pub struct HealthTab {
    inner: MetricsTab,
    flush_every: u32,
    samples_since_flush: u32,
    prev: Option<CounterSnapshot>,
}

impl HealthTab {
    fn connect_create(
        db: &mut VTabConnection,
        aux: Option<&()>,
        module_name: &[u8],
        database_name: &[u8],
        table_name: &[u8],
        args: &[&[u8]],
        is_create: bool,
    ) -> Result<(Cow<'static, CStr>, Self)> {
        // Split health-owned arguments from the ones MetricsTab knows
        // (retention=, rollups= pass straight through).
        let mut flush_every = DEFAULT_FLUSH_EVERY;
        let mut every_secs = DEFAULT_EVERY_SECS;
        let mut forwarded: Vec<Vec<u8>> = Vec::new();
        if is_create {
            for (name, value) in table_args::parse_kv_args(args).map_err(module_err)? {
                match name.as_str() {
                    "flush_every" => {
                        flush_every =
                            value
                                .parse::<u32>()
                                .ok()
                                .filter(|n| *n >= 1)
                                .ok_or_else(|| {
                                    module_err(format!(
                                        "flush_every: expected N >= 1, got {value:?}"
                                    ))
                                })?;
                    }
                    "every" => {
                        every_secs = value.parse::<u64>().map_err(|_| {
                            module_err(format!(
                                "every: expected seconds (0 disables), got {value:?}"
                            ))
                        })?;
                    }
                    _ => forwarded.push(format!("{name}={value}").into_bytes()),
                }
            }
        }
        let forwarded_refs: Vec<&[u8]> = forwarded.iter().map(|v| v.as_slice()).collect();

        let (schema, inner) = MetricsTab::connect_create(
            db,
            aux,
            module_name,
            database_name,
            table_name,
            if is_create { &forwarded_refs } else { &[] },
            is_create,
        )?;

        let _bind = DbGuard::bind(inner.db);
        let host = unsafe { Connection::from_handle(inner.db) }?;
        if is_create {
            shadow_meta::save_meta_text(
                &host,
                &inner.database_name,
                &inner.table_name,
                META_FLUSH_EVERY,
                &flush_every.to_string(),
            )
            .map_err(module_err)?;
            shadow_meta::save_meta_text(
                &host,
                &inner.database_name,
                &inner.table_name,
                META_EVERY,
                &every_secs.to_string(),
            )
            .map_err(module_err)?;
            host.execute_batch(&views_ddl(&inner.database_name, &inner.table_name))?;
            shadow_meta::save_meta_text(
                &host,
                &inner.database_name,
                &inner.table_name,
                META_VIEWS_VER,
                VIEWS_VERSION,
            )
            .map_err(module_err)?;
        } else {
            let fe = load_meta_relaxed(
                &host,
                &inner.database_name,
                &inner.table_name,
                META_FLUSH_EVERY,
            )?;
            let ev = load_meta_relaxed(&host, &inner.database_name, &inner.table_name, META_EVERY)?;
            flush_every = fe
                .as_deref()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(DEFAULT_FLUSH_EVERY);
            every_secs = ev
                .as_deref()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(DEFAULT_EVERY_SECS);
            // Best-effort view refresh: databases created by older
            // builds get the current report/trends definitions on open
            // (ignored on read-only connections; old views keep working).
            let ver = load_meta_relaxed(
                &host,
                &inner.database_name,
                &inner.table_name,
                META_VIEWS_VER,
            )?;
            if ver.as_deref() != Some(VIEWS_VERSION) {
                let refreshed = host
                    .execute_batch(&format!(
                        "{}\n{}",
                        drop_views_ddl(&inner.database_name, &inner.table_name),
                        views_ddl(&inner.database_name, &inner.table_name)
                    ))
                    .is_ok();
                if refreshed {
                    let _ = shadow_meta::save_meta_text(
                        &host,
                        &inner.database_name,
                        &inner.table_name,
                        META_VIEWS_VER,
                        VIEWS_VERSION,
                    );
                }
            }
            // Best-effort migration: rewrite v1 BLOB values as TEXT so
            // standard readers work next time. Ignore failures (the
            // connection may be read-only; relaxed reads cover us).
            let _ = shadow_meta::save_meta_text(
                &host,
                &inner.database_name,
                &inner.table_name,
                META_FLUSH_EVERY,
                &flush_every.to_string(),
            );
            let _ = shadow_meta::save_meta_text(
                &host,
                &inner.database_name,
                &inner.table_name,
                META_EVERY,
                &every_secs.to_string(),
            );
        }

        // Collection begins: create AND every later re-open (re)arm the
        // sampler for this file+table.
        ensure_scheduler(
            db_file_path(inner.db, &inner.database_name),
            &inner.table_name,
            every_secs,
            &inner.shared,
        );

        Ok((
            schema,
            HealthTab {
                inner,
                flush_every,
                samples_since_flush: 0,
                prev: None,
            },
        ))
    }

    /// Take a sample. `auto` (the scheduler) records db-level gauges
    /// only; interactive 'sample' adds this connection's counter deltas
    /// and hit ratio. Returns points written (the synthetic rowid).
    fn run_sample(&mut self, auto: bool) -> Result<i64> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let db = self.inner.db;

        let mut points = gauge_points(db, &self.inner.database_name)?;

        if !auto {
            let snap = counter_snapshot(db)?;
            if let Some(prev) = &self.prev {
                let mut delta = |name: &'static str, cur: i64, prev: i64| -> Option<f64> {
                    let d = cur - prev;
                    if d >= 0 {
                        points.push((name, d as f64));
                        Some(d as f64)
                    } else {
                        None // counter reset: skip, never fabricate
                    }
                };
                let dh = delta("cache_hits", snap.cache_hits, prev.cache_hits);
                let dm = delta("cache_misses", snap.cache_misses, prev.cache_misses);
                delta("cache_writes", snap.cache_writes, prev.cache_writes);
                delta("cache_spills", snap.cache_spills, prev.cache_spills);
                delta("lookaside_hits", snap.lookaside_hits, prev.lookaside_hits);
                delta(
                    "lookaside_miss_full",
                    snap.lookaside_miss_full,
                    prev.lookaside_miss_full,
                );
                if let (Some(h), Some(m)) = (dh, dm) {
                    if h + m > 0.0 {
                        points.push(("cache_hit_ratio", h / (h + m)));
                    }
                }
            }
            self.prev = Some(snap);
        }

        let labels: HashMap<String, String> =
            HashMap::from([("db".to_owned(), self.inner.database_name.clone())]);
        let written = points.len() as i64;
        for (name, value) in points {
            let sid = self
                .inner
                .shared
                .engine
                .resolve_cached(name, &labels)
                .map_err(module_err)?;
            self.inner.shared.engine.write_point(sid, ts, value);
        }

        self.samples_since_flush += 1;
        if self.samples_since_flush >= self.flush_every {
            self.inner.shared.engine.flush_all().map_err(module_err)?;
            self.samples_since_flush = 0;
        }
        Ok(written)
    }
}

unsafe impl<'vtab> VTab<'vtab> for HealthTab {
    type Aux = ();
    type Cursor = MetricsCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        module_name: &[u8],
        database_name: &[u8],
        table_name: &[u8],
        args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        Self::connect_create(db, aux, module_name, database_name, table_name, args, false)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        self.inner.best_index(info)
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        self.inner.open()
    }
}

impl CreateVTab<'_> for HealthTab {
    const KIND: VTabKind = VTabKind::Default;

    fn create(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        module_name: &[u8],
        database_name: &[u8],
        table_name: &[u8],
        args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        Self::connect_create(db, aux, module_name, database_name, table_name, args, true)
    }

    fn destroy(&self) -> Result<()> {
        stop_scheduler(
            db_file_path(self.inner.db, &self.inner.database_name).as_deref(),
            &self.inner.table_name,
        );
        let _bind = DbGuard::bind(self.inner.db);
        let host = unsafe { Connection::from_handle(self.inner.db) }?;
        host.execute_batch(&drop_views_ddl(
            &self.inner.database_name,
            &self.inner.table_name,
        ))?;
        self.inner.destroy()
    }
}

impl UpdateVTab<'_> for HealthTab {
    /// argv layout matches MetricsTab: [6] is the hidden command column.
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        if let Some(ValueRef::Text(_)) = args.iter().nth(6) {
            let cmd: String = args.get(6)?;
            let _bind = DbGuard::bind(self.inner.db);
            self.inner.acquire_write_gate()?;
            return match cmd.as_str() {
                "sample" => self.run_sample(false),
                "sample:auto" => self.run_sample(true),
                _ => self.inner.run_command(&cmd).map_err(|err| match err {
                    Error::ModuleError(msg) if msg.starts_with("unknown command") => {
                        module_err(format!(
                            "unknown command {cmd:?}; supported: 'sample', 'flush', \
                             'compact', 'rollup', 'prune:<unix_ts>'"
                        ))
                    }
                    other => other,
                }),
            };
        }
        self.inner.insert(args)
    }

    fn delete(&mut self, arg: ValueRef<'_>) -> Result<()> {
        self.inner.delete(arg)
    }

    fn update(&mut self, args: &Updates<'_>) -> Result<()> {
        self.inner.update(args)
    }
}

impl TransactionVTab<'_> for HealthTab {
    fn begin(&mut self) -> Result<()> {
        self.inner.begin()
    }

    fn commit(&mut self) -> Result<()> {
        self.inner.commit()
    }

    fn rollback(&mut self) -> Result<()> {
        self.inner.rollback()
    }
}

impl SavepointVTab for HealthTab {
    fn savepoint(&mut self, id: c_int) {
        self.inner.savepoint(id)
    }

    fn release(&mut self, id: c_int) {
        self.inner.release(id)
    }

    fn rollback_to(&mut self, id: c_int) {
        self.inner.rollback_to(id)
    }
}
