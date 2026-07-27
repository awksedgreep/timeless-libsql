//! timeless_health: dbhealth — SQLite health telemetry stored, compressed,
//! in the database it measures (docs/DBHEALTH.md).
//!
//!   CREATE VIRTUAL TABLE dbhealth USING timeless_health(flush_every=20);
//!   INSERT INTO dbhealth(dbhealth) VALUES ('sample');
//!   SELECT ts, value FROM dbhealth WHERE name='cache_hit_ratio' AND ts > :t0;
//!
//! The vtab IS a timeless_metrics table (HealthTab wraps MetricsTab and
//! delegates everything: schema, reads, pushdown, Tier 1/2/Prometheus
//! ingest, transactions, savepoints, DROP). It adds exactly two things:
//!
//!   1. The 'sample' command: read SQLite's own counters on the CALLING
//!      connection (sqlite3_db_status), db-level pragmas, and file sizes,
//!      and append them as points — v1 inventory in docs/DBHEALTH.md.
//!   2. A `flush_every=N` CREATE argument (persisted in `_meta` like
//!      logs' index_keys): every Nth sample also flushes. The DEFAULT IS
//!      1 — every sample durable — because the cron pattern (`sqlite3 db
//!      "...('sample')"` then process exit) would otherwise lose every
//!      sample silently: the buffer dies with the process and a fresh
//!      process never reaches sample N. Long-lived apps that sample on a
//!      timer should raise it (e.g. flush_every=20) to trade a bounded
//!      loss window for fewer, larger chunks; 'compact' cleans up either
//!      way.
//!
//! v1 is deliberately HOOK-FREE (no wal_hook/commit_hook/trace_v2 — sqld's
//! replication owns the WAL hook and host apps own tracing; see the design
//! doc). Statement profiling is deferred. Sampling only READS counters:
//! the reset flag is never passed to sqlite3_db_status, so co-resident
//! readers of the same counters are undisturbed.
//!
//! COMPANION VIEWS — xCreate also creates three ordinary SQL views next
//! to the vtab (dropped with it), so evaluating db health needs no DBA
//! knowledge, just SELECT *:
//!
//!   <t>_now     latest value per series + its age
//!   <t>_report  one row per health CHECK: status ok|warn|attention|
//!               'no data', a human-readable value, and one concrete
//!               piece of advice — worst rows first. The thresholds are
//!               deliberately visible in the view SQL: opinionated but
//!               inspectable, and users can define their own variants.
//!   <t>_trends  per-series daily min/avg/max over the last 7 days
//!
//! The views are plain SQL over the vtab — they version with the
//! extension, work identically over sqld, and cost nothing at rest.
//!
//! COUNTER SCOPE (documented limit): sqlite3_db_status counters are per
//! CONNECTION, so delta series describe the connection that issued
//! 'sample'. Delta state therefore lives in the per-connection vtab
//! instance — which is exactly the correct granularity — and the first
//! sample on a fresh connection emits gauges only (no baseline yet).
//! Pragma- and file-derived series are db-global regardless of caller.

use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::{c_int, CStr, CString};

use rusqlite::ffi;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    CreateVTab, IndexInfo, Inserts, Module, TransactionVTab, UpdateVTab, Updates, VTab,
    VTabConnection, VTabKind,
};
use rusqlite::{Connection, Error, Result};

use crate::metrics_vtab::{MetricsCursor, MetricsTab};
use crate::shared::DbGuard;
use crate::sql_ident;
use crate::vtab_tx::{self, SavepointVTab};

/// Register the "timeless_health" module on a freshly-loaded connection.
pub(crate) fn register(db: &Connection) -> Result<()> {
    const MODULE: Module<HealthTab> = vtab_tx::update_module_with_savepoints();
    db.create_module(c"timeless_health", &MODULE, None::<()>)
}

const DEFAULT_FLUSH_EVERY: u32 = 1;
const META_KEY: &str = "health_flush_every";

fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

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

/// `#[repr(C)]` with the wrapped MetricsTab FIRST: MetricsTab is itself
/// repr(C) with `sqlite3_vtab base` first, so a pointer to HealthTab is a
/// valid pointer to sqlite3_vtab — the same C-style inheritance contract
/// every vtab in this crate relies on.
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
        // Build the underlying metrics vtab first (shadow DDL, engine
        // registry, declared schema — its hidden command column is named
        // after OUR table, which is what makes the command idiom work).
        // MetricsTab ignores args, so none are forwarded.
        let (schema, inner) = MetricsTab::connect_create(
            db,
            aux,
            module_name,
            database_name,
            table_name,
            &[],
            is_create,
        )?;

        // flush_every: parsed from CREATE args and persisted in _meta so
        // xConnect restores it from storage (same policy as logs'
        // index_keys — it's a property of the table, not of whoever
        // reconnects). The _meta table already exists: inner DDL ran.
        let _bind = DbGuard::bind(inner.db);
        let host = unsafe { Connection::from_handle(inner.db) }?;
        let meta = sql_ident::qualified_shadow(&inner.database_name, &inner.table_name, "meta");
        let flush_every = if is_create {
            let n = parse_flush_every_args(args).map_err(module_err)?;
            host.execute(
                &format!("INSERT OR REPLACE INTO {meta}(k, v) VALUES(?1, ?2)"),
                rusqlite::params![META_KEY, n.to_string().as_bytes()],
            )?;
            // Companion views ride the same (transactional) create.
            host.execute_batch(&views_ddl(&inner.database_name, &inner.table_name))?;
            n
        } else {
            use rusqlite::OptionalExtension;
            host.query_row(
                &format!("SELECT v FROM {meta} WHERE k = ?1"),
                [META_KEY],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .and_then(|s| s.parse::<u32>().ok())
            // _meta row missing/garbled (shouldn't happen): fall back to
            // the args SQLite replays, then the default.
            .or_else(|| parse_flush_every_args(args).ok())
            .unwrap_or(DEFAULT_FLUSH_EVERY)
        };

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

    /// The 'sample' command: capture the v1 inventory as points at "now".
    /// Returns the number of points written as the synthetic rowid so
    /// callers can sanity-check via last_insert_rowid().
    fn run_sample(&mut self) -> Result<i64> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let db = self.inner.db;

        // ── Per-connection counters (sqlite3_db_status, never reset) ──
        let snap = CounterSnapshot {
            cache_hits: db_status(db, ffi::SQLITE_DBSTATUS_CACHE_HIT)?,
            cache_misses: db_status(db, ffi::SQLITE_DBSTATUS_CACHE_MISS)?,
            cache_writes: db_status(db, ffi::SQLITE_DBSTATUS_CACHE_WRITE)?,
            cache_spills: db_status(db, ffi::SQLITE_DBSTATUS_CACHE_SPILL)?,
            lookaside_hits: db_status(db, ffi::SQLITE_DBSTATUS_LOOKASIDE_HIT)?,
            lookaside_miss_full: db_status(db, ffi::SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL)?,
        };

        let mut points: Vec<(&'static str, f64)> = Vec::with_capacity(16);

        // Memory gauges are always emitted.
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

        // ── Counter deltas: only with a baseline from THIS connection. ──
        // A negative delta means the counter reset underneath us (c_int
        // wrap or an external reset); that interval is unknowable, so the
        // point is skipped and the new cumulative becomes the baseline —
        // one missing sample instead of one absurd one.
        if let Some(prev) = &self.prev {
            let mut delta = |name: &'static str, cur: i64, prev: i64| -> Option<f64> {
                let d = cur - prev;
                if d >= 0 {
                    points.push((name, d as f64));
                    Some(d as f64)
                } else {
                    None
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
            // Ratio over THIS interval; no page traffic → no point (a
            // fabricated 0 or 1 would poison averages).
            if let (Some(h), Some(m)) = (dh, dm) {
                if h + m > 0.0 {
                    points.push(("cache_hit_ratio", h / (h + m)));
                }
            }
        }
        self.prev = Some(snap);

        // ── Db-global gauges: pragmas on the calling connection. ──────
        let host = unsafe { Connection::from_handle(db) }?;
        let schema = sql_ident::quote(&self.inner.database_name);
        let page_count: i64 =
            host.query_row(&format!("PRAGMA {schema}.page_count"), [], |r| r.get(0))?;
        let freelist: i64 =
            host.query_row(&format!("PRAGMA {schema}.freelist_count"), [], |r| r.get(0))?;
        points.push(("db_pages", page_count as f64));
        points.push(("freelist_pages", freelist as f64));
        if page_count > 0 {
            points.push(("bloat_ratio", freelist as f64 / page_count as f64));
        }

        // ── File sizes: main db + -wal (0 when absent, e.g. rollback
        // journal mode). Empty filename = temp/in-memory db → skipped. ──
        if let Some(path) = db_file_path(db, &self.inner.database_name) {
            if let Ok(md) = std::fs::metadata(&path) {
                points.push(("db_file_bytes", md.len() as f64));
            }
            let wal_len = std::fs::metadata(format!("{path}-wal"))
                .map(|md| md.len())
                .unwrap_or(0);
            points.push(("wal_file_bytes", wal_len as f64));
        }

        // ── Append through the shared engine; rides the host txn. ─────
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

        // Bounded loss window: every flush_every-th sample also flushes.
        // (Note: rolled-back samples still advance this counter and the
        // delta baseline — a rollback leaves at most one delta interval
        // unattributed, never a wrong value.)
        self.samples_since_flush += 1;
        if self.samples_since_flush >= self.flush_every {
            self.inner.shared.engine.flush_all().map_err(module_err)?;
            self.samples_since_flush = 0;
        }

        Ok(written)
    }
}

/// DDL for the three companion views. All SQL is resolved lazily by
/// SQLite (views never validate their FROM at CREATE time), and every
/// check in _report degrades to a 'no data' row instead of NULLs — a
/// fresh table renders a sane report immediately.
///
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
         'no samples yet — run the ''sample'' command from cron or an app timer'
       WHEN age <= 7200 THEN '—'
       ELSE 'sampling has stopped; check the cron job or timer that issues ''sample''' END AS advice
FROM (SELECT max(ts) AS t, unixepoch() - max(ts) AS age FROM {t})

UNION ALL
SELECT 'cache_hit_ratio_24h',
  CASE WHEN v IS NULL THEN 'no data'
       WHEN v >= 0.90 THEN 'ok'
       WHEN v >= 0.60 THEN 'warn'
       ELSE 'attention' END,
  CASE WHEN v IS NULL THEN '—' ELSE printf('%.3f', v) END,
  CASE WHEN v IS NULL THEN
         'ratio deltas need a connection that samples more than once (docs/DBHEALTH.md)'
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
  COALESCE(CAST(CAST(s AS INTEGER) AS TEXT), '—'),
  CASE WHEN s IS NULL THEN
         'spill deltas need a connection that samples more than once (docs/DBHEALTH.md)'
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
SELECT 'db_growth_7d',
  CASE WHEN o IS NULL OR l IS NULL THEN 'no data'
       WHEN l < 10485760 OR o <= 0 THEN 'ok'
       WHEN l <= 1.5 * o THEN 'ok'
       WHEN l <= 3 * o THEN 'warn'
       ELSE 'attention' END,
  CASE WHEN o IS NULL OR l IS NULL THEN '—'
       WHEN o <= 0 THEN printf('%.1f MB', l / 1048576.0)
       ELSE printf('%.1fx in 7d (now %.1f MB)', l * 1.0 / o, l / 1048576.0) END,
  CASE WHEN o IS NULL OR l IS NULL THEN 'needs at least one sample'
       WHEN l < 10485760 OR o <= 0 OR l <= 1.5 * o THEN '—'
       ELSE 'the file is growing fast; check that ''prune'' retention runs for your telemetry tables' END
FROM (SELECT (SELECT value FROM {t}
               WHERE name = 'db_file_bytes' AND ts > unixepoch() - 604800
               ORDER BY ts LIMIT 1) AS o,
             (SELECT value FROM {now_ref} WHERE name = 'db_file_bytes') AS l)
)
ORDER BY CASE status WHEN 'attention' THEN 0 WHEN 'warn' THEN 1
                     WHEN 'no data' THEN 2 ELSE 3 END, "check";
"#
    )
}

/// DROP DDL for the companion views (xDestroy, before the shadow drop).
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

/// `flush_every=N` (N ≥ 1) is the only supported argument; absent → 1.
fn parse_flush_every_args(args: &[&[u8]]) -> std::result::Result<u32, String> {
    let mut result = DEFAULT_FLUSH_EVERY;
    for raw in args {
        let arg = String::from_utf8_lossy(raw);
        let arg = arg.trim();
        let Some((name, value)) = arg.split_once('=') else {
            return Err(format!(
                "unrecognized argument {arg:?}; expected flush_every=N"
            ));
        };
        if name.trim() != "flush_every" {
            return Err(format!(
                "unrecognized argument {:?}; the only supported argument is flush_every",
                name.trim()
            ));
        }
        let value = value.trim();
        let value = value
            .strip_prefix('\'')
            .and_then(|v| v.strip_suffix('\''))
            .or_else(|| value.strip_prefix('"').and_then(|v| v.strip_suffix('"')))
            .unwrap_or(value);
        let n: u32 = value
            .parse()
            .map_err(|_| format!("flush_every: expected a positive integer, got {value:?}"))?;
        if n == 0 {
            return Err("flush_every must be at least 1".into());
        }
        result = n;
    }
    Ok(result)
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

/// The on-disk path of this schema's database file, None for temp or
/// in-memory databases (sqlite3_db_filename returns NULL or "").
fn db_file_path(db: *mut ffi::sqlite3, database_name: &str) -> Option<String> {
    let schema = CString::new(database_name).ok()?;
    let ptr = unsafe { ffi::sqlite3_db_filename(db, schema.as_ptr()) };
    if ptr.is_null() {
        return None;
    }
    let path = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
    if path.is_empty() {
        None
    } else {
        Some(path)
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
        // Views first (they reference the vtab), then the inherited
        // shadow-table drop. DbGuard nests safely under inner.destroy()'s.
        let _bind = DbGuard::bind(self.inner.db);
        let host = unsafe { Connection::from_handle(self.inner.db) }?;
        host.execute_batch(&drop_views_ddl(&self.inner.database_name, &self.inner.table_name))?;
        self.inner.destroy()
    }
}

impl UpdateVTab<'_> for HealthTab {
    /// argv layout matches MetricsTab: [6] is the hidden command column.
    /// 'sample' is handled here; every other payload (data rows, blobs,
    /// the inherited commands) delegates to the wrapped metrics vtab.
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        if let Some(ValueRef::Text(_)) = args.iter().nth(6) {
            let cmd: String = args.get(6)?;
            // Route store SQL to the calling connection and make sure
            // this connection's transaction owns the shared engine —
            // both 'sample' (pragma reads, opportunistic flush) and the
            // inherited commands need it.
            let _bind = DbGuard::bind(self.inner.db);
            self.inner.acquire_write_gate()?;
            return if cmd == "sample" {
                self.run_sample()
            } else {
                self.inner.run_command(&cmd).map_err(|err| match err {
                    // Extend the unknown-command message with 'sample';
                    // other errors pass through untouched.
                    Error::ModuleError(msg) if msg.starts_with("unknown command") => {
                        module_err(format!(
                            "unknown command {cmd:?}; supported: 'sample', 'flush', \
                             'compact', 'prune:<unix_ts>'"
                        ))
                    }
                    other => other,
                })
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

#[cfg(test)]
mod tests {
    use super::parse_flush_every_args;

    #[test]
    fn flush_every_defaults_and_parses() {
        assert_eq!(parse_flush_every_args(&[]).unwrap(), 1);
        assert_eq!(parse_flush_every_args(&[b"flush_every=1"]).unwrap(), 1);
        assert_eq!(parse_flush_every_args(&[b"flush_every='7'"]).unwrap(), 7);
        assert_eq!(
            parse_flush_every_args(&[b" flush_every = \"300\" "]).unwrap(),
            300
        );
    }

    #[test]
    fn flush_every_rejects_garbage() {
        assert!(parse_flush_every_args(&[b"flush_every=0"]).is_err());
        assert!(parse_flush_every_args(&[b"flush_every=-1"]).is_err());
        assert!(parse_flush_every_args(&[b"flush_every=soon"]).is_err());
        assert!(parse_flush_every_args(&[b"index_keys=a"]).is_err());
        assert!(parse_flush_every_args(&[b"bare"]).is_err());
    }
}
