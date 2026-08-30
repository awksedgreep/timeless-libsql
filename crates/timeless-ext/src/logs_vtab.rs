//! timeless_logs: the Phase 2 log-store vtab (PLAN.md Session 5),
//! backed by a timeless_core::BlockEngine persisting through
//! ShadowBlockStore into `<table>_blocks` / `<table>_terms` /
//! `<table>_meta` on the host db. Same skeleton as metrics_vtab.rs —
//! read that file first; only the differences are commented here.
//!
//!   CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service,path,status');
//!
//! Declared schema (runtime-built):
//!
//!   CREATE TABLE x(ts INTEGER, level TEXT, message TEXT, metadata TEXT,
//!                  "service" TEXT HIDDEN, "path" TEXT HIDDEN,
//!                  "status" TEXT HIDDEN, message_contains TEXT HIDDEN,
//!                  max_work_entries INTEGER HIDDEN,
//!                  `"<table>"` HIDDEN)
//!
//! THE DESIGN IMPROVEMENT over the Elixir donor's query API: each
//! index key gets its own HIDDEN column. `WHERE service = 'api'`
//! then arrives at best_index as a plain column-equality constraint we
//! can push into the `_terms` posting lists — no JSON operators, no
//! special syntax, it reads like a real column. column() also RETURNS
//! the value (extracted from entry metadata), so `SELECT service FROM
//! logs` works even though the column is hidden.
//!
//! Write path:  INSERT INTO logs(ts, level, message, metadata) — one
//!              entry into the engine buffer (auto-flushes at the
//!              threshold). Index-key hidden columns may be used as
//!              INSERT shorthand: a non-NULL value is merged into the
//!              metadata pairs.
//! Commands:    INSERT INTO logs(logs) VALUES ('flush' | 'optimize' |
//!              `optimize:<max_entries>` | `prune:<ts>` |
//!              `reindex:<keys>`) — the same
//!              FTS5 idiom as metrics. The bounded optimize form lets
//!              embedded hosts cap one maintenance turn.
//! Read path:   flushed blocks and the in-memory buffer are merged, so
//!              entries are queryable immediately after INSERT and
//!              durable (as durable as the enclosing transaction) after
//!              'flush'.
//! Append-only: DELETE/UPDATE rejected; retention is `prune:<ts>`.

use std::borrow::Cow;
use std::ffi::{c_int, CStr, CString};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use rusqlite::ffi;
use rusqlite::types::{Null, ValueRef};
use rusqlite::vtab::{
    escape_double_quote, Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, Inserts,
    Module, TransactionVTab, UpdateVTab, Updates, VTab, VTabConnection, VTabCursor, VTabKind,
};
use rusqlite::{Connection, Error, Result};
use timeless_core::{
    canonical_severity, level_from_name, BlockEngine, BlockEngineConfig, BlockStore, LogEntry,
    LogQuery, LogQueryExecutionReport, LogQueryOrder,
};

use crate::batch::BatchReader;
use crate::flatjson::{pairs_to_json, parse_labels_json};
use crate::query_report::LogQueryReportState;
use crate::shadow_block_store::{self, ShadowBlockStore};
use crate::shadow_meta;
use crate::shared::{self, DbGuard, RegistryKey, SharedEngine};
use crate::sql_ident;
use crate::table_args;
use crate::vtab_tx::{self, SavepointVTab};

/// Register the "timeless_logs" module on a freshly-loaded connection.
pub(crate) fn register(db: &Connection, query_reports: Arc<LogQueryReportState>) -> Result<()> {
    const MODULE: Module<LogsTab> = vtab_tx::update_module_with_savepoints();
    db.create_module(c"timeless_logs", &MODULE, Some(query_reports))
}

/// Engine parameters (see BlockEngineConfig for what each knob means).
const FLUSH_THRESHOLD: usize = 8192; // buffered entries before auto-flush
const ZSTD_LEVEL: i32 = 7;
pub(crate) const MERGE_TARGET_ENTRIES: usize = 8192;
/// Auto-optimize riding the flush path (the embedded Elixir engines send
/// 'flush' on a 1s heartbeat but never 'optimize' — compression must not
/// depend on the host knowing to schedule it). 30 flushes ≈ the API
/// services' 30s optimize cadence; the budget bounds each pause and doubles
/// as the raw-backlog size that triggers a pass immediately.
const AUTO_OPTIMIZE_INTERVAL_FLUSHES: usize = 30;
const AUTO_OPTIMIZE_BUDGET_ENTRIES: usize = 32_768;
/// HARD CAP on merged-block ts span: 1 hour in the table's declared unit.
/// PLAN.md "Pruning & retention": merge
/// compaction must never produce blocks straddling retention
/// boundaries, or expired entries stay pinned until the whole merged
/// block ages out. 1h granules keep 'prune:<ts>' effective at typical
/// (hours-to-days) log retention windows.
#[derive(Clone, Copy)]
struct TimestampUnit {
    name: &'static str,
    per_second: i64,
    hour: i64,
}

fn timestamp_unit(value: &str) -> std::result::Result<TimestampUnit, String> {
    match value {
        "ms" => Ok(TimestampUnit {
            name: "ms",
            per_second: 1_000,
            hour: 3_600_000,
        }),
        "us" => Ok(TimestampUnit {
            name: "us",
            per_second: 1_000_000,
            hour: 3_600_000_000,
        }),
        other => Err(format!("timestamp_unit={other:?}; expected 'ms' or 'us'")),
    }
}

fn load_timestamp_unit(
    conn: &Connection,
    database: &str,
    table: &str,
) -> std::result::Result<TimestampUnit, String> {
    let value = shadow_meta::load_meta_text(conn, database, table, "timestamp_unit")?
        .unwrap_or_else(|| "ms".into());
    timestamp_unit(&value)
}

/// best_index bitmask layout (fixed bits first, then one bit per index
/// key). c_int gives 31 usable bits → 3 fixed + up to 28 index keys.
const BIT_LEVEL: c_int = 1;
const BIT_TS_LO: c_int = 2;
const BIT_TS_HI: c_int = 4;
/// F6: message LIKE pattern claimed for trigram block pruning (top
/// usable bit; index keys occupy 3..=29, capping MAX_INDEX_KEYS at 27).
const BIT_MSG_LIKE: c_int = 1 << 30;
/// Exact case-insensitive substring constraint on the public hidden
/// `message_contains` column. idxNum is an arbitrary signed C integer, so the
/// sign bit is available without reducing the 27-key compatibility limit.
const BIT_MSG_CONTAINS: c_int = c_int::MIN;
const FIRST_KEY_BIT_SHIFT: usize = 3;
const MAX_INDEX_KEYS: usize = 27;

const PLAN_BOUNDED_TS_ASC: &str = "bounded-ts-asc";
const PLAN_BOUNDED_TS_ASC_OFFSET: &str = "bounded-ts-asc-offset";
const PLAN_BOUNDED_TS_DESC: &str = "bounded-ts-desc";
const PLAN_BOUNDED_TS_DESC_OFFSET: &str = "bounded-ts-desc-offset";
const PLAN_WORK_LIMIT: &str = "work-limit";
const PLAN_WORK_LIMIT_SUFFIX: &str = "+work-limit";

/// Number of fixed (non-hidden) columns before the index-key columns:
/// 0=ts 1=level 2=message 3=metadata.
const FIXED_COLS: usize = 4;

fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn canonical_rich_metadata(
    text: &str,
) -> std::result::Result<(String, Vec<(String, String)>), String> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|error| format!("invalid JSON: {error}"))?;
    let serde_json::Value::Object(object) = value else {
        return Err("expected a JSON object".into());
    };
    let sorted: std::collections::BTreeMap<String, serde_json::Value> =
        object.into_iter().collect();
    let pairs = sorted
        .iter()
        .map(|(key, value)| {
            let value = match value {
                serde_json::Value::String(value) => value.clone(),
                other => serde_json::to_string(other).unwrap_or_default(),
            };
            (key.clone(), value)
        })
        .collect();
    let canonical =
        serde_json::to_string(&sorted).map_err(|error| format!("encode JSON: {error}"))?;
    Ok((canonical, pairs))
}

fn metadata_is_flat_strings(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| value.as_object().cloned())
        .is_some_and(|object| object.values().all(serde_json::Value::is_string))
}

/// Load the persisted F6 message_index setting ('trigram' => true).
fn load_message_index(
    conn: &Connection,
    database: &str,
    table: &str,
) -> std::result::Result<bool, String> {
    Ok(
        shadow_meta::load_meta_text(conn, database, table, "message_index")?.as_deref()
            == Some("trigram"),
    )
}

// ---------------------------------------------------------------------------
// The virtual table
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct LogsTab {
    base: ffi::sqlite3_vtab,
    /// Raw handle to the HOST connection, kept for xDestroy's DDL.
    db: *mut ffi::sqlite3,
    table_name: String,
    database_name: String,
    /// The allowlist of indexed metadata keys, in declared-column order
    /// (position k ↔ column FIXED_COLS+k ↔ bitmask bit 8<<k).
    index_keys: Vec<String>,
    /// Native timestamp ticks per second — the unit context runtime
    /// commands need (e.g. parsing 'retention:<n>[s|m|h|d]').
    native_per_second: i64,
    /// Shared process-wide across connections via the R4 registry —
    /// see metrics_vtab.rs and shared.rs for the full story.
    shared: Arc<SharedEngine<BlockEngine>>,
    key: RegistryKey,
    query_reports: Arc<LogQueryReportState>,
    /// True while THIS connection's write txn holds the writer gate.
    gate_held: bool,
    rowid_counter: i64,
}

impl LogsTab {
    /// Resolve the shared engine for an EXISTING timeless_logs table on
    /// this connection — the read-side entry point for stats/kernel TVFs
    /// (query_tvf.rs). Mirrors the xConnect tail: index_keys come from
    /// `_meta` (a property of the data, never of the caller), instance
    /// identity from shadow_meta, engine from the process registry with
    /// the same builder. A table that was never a timeless_logs vtab
    /// fails on the `_meta` read with SQLite's own "no such table".
    ///
    /// Caller must hold a DbGuard binding for `handle`.
    pub(crate) fn shared_engine_for(
        handle: *mut ffi::sqlite3,
        database: &str,
        table: &str,
    ) -> Result<Arc<SharedEngine<BlockEngine>>> {
        let host = unsafe { Connection::from_handle(handle) }?;
        let instance_id =
            shadow_meta::ensure_instance_id(&host, database, table).map_err(module_err)?;
        let store = ShadowBlockStore::new(database, table);
        let index_keys = match store.load_meta("index_keys").map_err(module_err)? {
            Some(bytes) => {
                let joined = String::from_utf8(bytes).map_err(|_| {
                    module_err(format!("{table}: index_keys in _meta is not UTF-8"))
                })?;
                if joined.is_empty() {
                    Vec::new()
                } else {
                    joined.split(',').map(str::to_owned).collect()
                }
            }
            None => Vec::new(),
        };
        let message_trigrams = load_message_index(&host, database, table).map_err(module_err)?;
        let timestamp_unit = load_timestamp_unit(&host, database, table).map_err(module_err)?;
        let key = shared::registry_key(handle, database.as_bytes(), table, instance_id);
        let shared = shared::get_or_create(&key, move || {
            BlockEngine::new(
                Box::new(store),
                BlockEngineConfig {
                    flush_threshold: FLUSH_THRESHOLD,
                    zstd_level: ZSTD_LEVEL,
                    merge_target_entries: MERGE_TARGET_ENTRIES,
                    merge_max_ts_span: timestamp_unit.hour,
                    message_trigrams,
                    index_keys,
                    auto_optimize_interval_flushes: AUTO_OPTIMIZE_INTERVAL_FLUSHES,
                    auto_optimize_budget_entries: AUTO_OPTIMIZE_BUDGET_ENTRIES,
                },
            )
            .map_err(module_err)
        })?;
        // P1: connection-lifetime pin (see shared::pin_engine).
        shared::pin_engine(handle, &key, shared.clone());
        shared.engine.set_retention(
            shadow_meta::load_retention(&host, database, table).map_err(module_err)?,
        );
        Ok(shared)
    }

    fn connect_create(
        db: &mut VTabConnection,
        aux: Option<&Arc<LogQueryReportState>>,
        _module_name: &[u8],
        database_name: &[u8],
        table_name: &[u8],
        args: &[&[u8]],
        is_create: bool,
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let query_reports = aux.cloned().ok_or_else(|| {
            module_err("timeless_logs: missing connection query-report state".into())
        })?;
        let table = String::from_utf8_lossy(table_name).into_owned();
        let database = String::from_utf8_lossy(database_name).into_owned();
        let handle = unsafe { db.handle() };
        // Bind the calling connection for every store operation below
        // (DDL, _meta reads/writes, recovery scans). RAII unbind.
        let _bind = DbGuard::bind(handle);

        let host = unsafe { Connection::from_handle(handle) }?;
        let store = ShadowBlockStore::new(&database, &table);

        if is_create {
            // Same incremental auto-vacuum attempt as metrics (no-op on
            // a non-empty db; see metrics_vtab.rs for the rationale).
            let _ = host.execute_batch(&sql_ident::incremental_auto_vacuum(&database));
            host.execute_batch(&shadow_block_store::ddl(&database, &table))?;
        }
        let instance_id =
            shadow_meta::ensure_instance_id(&host, &database, &table).map_err(module_err)?;

        let (index_keys, retention, timestamp_unit) = if is_create {
            // index_keys comes from the CREATE args and is PERSISTED in
            // _meta: the key set is baked into the terms already written
            // to `_terms`, so it is a property of the DATA, not of
            // whoever reconnects. xConnect reads it back from _meta and
            // never trusts (or receives) fresh args. F2 retention rides
            // the same convention (unit-resolved to ms, persisted).
            let mut keys_value: Option<String> = None;
            let mut retention_value: Option<String> = None;
            let mut message_index: Option<bool> = None;
            let mut timestamp_unit_value = "ms".to_owned();
            for (name, value) in table_args::parse_kv_args(args).map_err(module_err)? {
                match name.as_str() {
                    "index_keys" => keys_value = Some(value),
                    "retention" => retention_value = Some(value),
                    "timestamp_unit" => timestamp_unit_value = value,
                    "message_index" => {
                        message_index = Some(match value.as_str() {
                            "trigram" => true,
                            "none" => false,
                            other => {
                                return Err(module_err(format!(
                                    "message_index={other:?}; expected 'trigram' or 'none'"
                                )));
                            }
                        });
                    }
                    other => {
                        return Err(module_err(format!(
                            "unrecognized argument {other:?}; timeless_logs supports: \
                             index_keys, retention, message_index, timestamp_unit"
                        )));
                    }
                }
            }
            let keys = parse_index_keys_value(&table, keys_value.as_deref().unwrap_or(""))
                .map_err(module_err)?;
            store
                .save_meta("index_keys", keys.join(",").as_bytes())
                .map_err(module_err)?;
            let timestamp_unit = timestamp_unit(&timestamp_unit_value).map_err(module_err)?;
            shadow_meta::save_meta_text(
                &host,
                &database,
                &table,
                "timestamp_unit",
                timestamp_unit.name,
            )
            .map_err(module_err)?;
            let retention = retention_value
                .as_deref()
                .map(|value| table_args::parse_retention(value, timestamp_unit.per_second))
                .transpose()
                .map_err(module_err)?;
            if let Some(native) = retention {
                shadow_meta::save_meta_text(
                    &host,
                    &database,
                    &table,
                    "retention",
                    &native.to_string(),
                )
                .map_err(module_err)?;
            }
            if message_index == Some(true) {
                shadow_meta::save_meta_text(&host, &database, &table, "message_index", "trigram")
                    .map_err(module_err)?;
            }
            (keys, retention, timestamp_unit)
        } else {
            let keys = match store.load_meta("index_keys").map_err(module_err)? {
                Some(bytes) => {
                    let joined = String::from_utf8(bytes).map_err(|_| {
                        module_err(format!("{table}: index_keys in _meta is not UTF-8"))
                    })?;
                    if joined.is_empty() {
                        Vec::new()
                    } else {
                        joined.split(',').map(str::to_owned).collect()
                    }
                }
                // _meta row missing (shouldn't happen — xCreate always
                // writes it). Treat as "no index keys".
                None => Vec::new(),
            };
            let timestamp_unit =
                load_timestamp_unit(&host, &database, &table).map_err(module_err)?;
            (
                keys,
                shadow_meta::load_retention(&host, &database, &table).map_err(module_err)?,
                timestamp_unit,
            )
        };

        // R4: one engine per (db file, schema alias, table, instance). First
        // connection in builds it — BlockEngine::new recovers the block
        // index via store.scan() (a re-entrant SELECT routed to the
        // calling connection by the DbGuard above, safe because THIS
        // thread already holds the connection mutex recursively) —
        // every later xConnect just bumps the Arc, no re-recovery.
        let message_trigrams = load_message_index(&host, &database, &table).map_err(module_err)?;
        let key = shared::registry_key(handle, database_name, &table, instance_id);
        let index_keys_for_engine = index_keys.clone();
        let shared_engine = shared::get_or_create(&key, move || {
            BlockEngine::new(
                Box::new(store),
                BlockEngineConfig {
                    flush_threshold: FLUSH_THRESHOLD,
                    zstd_level: ZSTD_LEVEL,
                    merge_target_entries: MERGE_TARGET_ENTRIES,
                    merge_max_ts_span: timestamp_unit.hour,
                    message_trigrams,
                    index_keys: index_keys_for_engine,
                    auto_optimize_interval_flushes: AUTO_OPTIMIZE_INTERVAL_FLUSHES,
                    auto_optimize_budget_entries: AUTO_OPTIMIZE_BUDGET_ENTRIES,
                },
            )
            .map_err(module_err)
        })?;
        shared_engine.engine.set_retention(retention);

        // Declared schema, built at runtime: fixed columns + one HIDDEN
        // TEXT column per index key + exact message search input + an optional
        // hard decode-work bound + the hidden command column named after the
        // table (FTS5 idiom).
        let mut schema =
            String::from("CREATE TABLE x(ts INTEGER, level TEXT, message TEXT, metadata TEXT");
        for key in &index_keys {
            schema.push_str(&format!(", \"{}\" TEXT HIDDEN", escape_double_quote(key)));
        }
        schema.push_str(", message_contains TEXT HIDDEN");
        schema.push_str(", max_work_entries INTEGER HIDDEN");
        schema.push_str(&format!(", \"{}\" HIDDEN)", escape_double_quote(&table)));
        let schema = CString::new(schema)
            .map_err(|_| module_err(format!("table/key name contains NUL: {table:?}")))?;

        Ok((
            Cow::Owned(schema),
            LogsTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
                table_name: table,
                database_name: database,
                index_keys,
                native_per_second: timestamp_unit.per_second,
                shared: shared_engine,
                key,
                query_reports,
                gate_held: false,
                rowid_counter: 0,
            },
        ))
    }

    /// Writer-gate helpers — identical shape to metrics_vtab.rs (read
    /// the comments there); begin() is the primary acquire site.
    fn acquire_write_gate(&mut self) -> Result<()> {
        if self.gate_held {
            return Ok(());
        }
        self.shared
            .write_gate
            .acquire(self.db as usize, &self.table_name)
            .map_err(module_err)?;
        self.gate_held = true;
        Ok(())
    }

    fn release_write_gate(&mut self) {
        if self.gate_held {
            self.shared.write_gate.release(self.db as usize);
            self.gate_held = false;
        }
    }

    /// Versioned Tier 2 batch ingest. Layouts are little-endian.
    ///
    /// v0 keeps the original millisecond/four-bucket/flat-string contract:
    ///   0    u8   version = 0x01
    ///   1    u8   flags = 0
    ///   2    u16  reserved
    ///   4    u32  n_entries
    ///   —    ts[]       n × i64 (ms)
    ///   —    level[]    n × u8 (0..=3, strict vocabulary)
    ///   —    message[]  n × { u32 len, utf8 }
    ///   —    metadata[] n × { u32 len, flat-JSON; '' = {} }
    ///
    /// All-or-nothing: the whole blob is parsed and validated before a
    /// single entry reaches the engine buffer; durability is identical
    /// to row inserts (buffered until 'flush', auto-flush included).
    /// v1 starts with version 0x02 and replaces the level byte column with
    /// length-prefixed exact severity strings. Metadata is canonical typed
    /// JSON, and the table's persisted timestamp_unit declares ms or us.
    fn ingest_batch(&self, blob: &[u8]) -> Result<i64> {
        let decode_started = std::time::Instant::now();
        let mut r = BatchReader::new(blob);
        let version = r.u8("version")?;
        if version != 0x01 && version != 0x02 {
            return Err(module_err(format!(
                "batch blob: unsupported version 0x{version:02x} (this build speaks v0 = 0x01 and v1 = 0x02)"
            )));
        }
        let flags = r.u8("flags")?;
        if flags != 0 {
            return Err(module_err(format!(
                "batch blob: unknown flags 0x{flags:02x} (v0/v1 define none; must be 0)"
            )));
        }
        r.skip(2, "reserved header bytes")?;
        let n = r.u32("n_entries")? as usize;

        let ts_bytes = r.take_array(n, 8, "timestamp column")?;
        let mut severities = Vec::with_capacity(n);
        let mut levels = Vec::with_capacity(n);
        if version == 0x01 {
            let level_bytes = r.take(n, "level column")?;
            if let Some(bad) = level_bytes.iter().find(|&&l| l > 3) {
                return Err(module_err(format!(
                    "batch blob: invalid level byte {bad} (0=debug 1=info 2=warning 3=error); batch rejected"
                )));
            }
            levels.extend_from_slice(level_bytes);
            severities.resize(n, None);
        } else {
            for i in 0..n {
                let severity = canonical_severity(r.str(&format!("severity {i}"))?)
                    .map_err(|error| module_err(format!("batch blob: entry {i}: {error}")))?;
                levels.push(level_from_name(severity).map_err(module_err)?);
                severities.push(Some(severity.to_owned()));
            }
        }
        let mut entries = Vec::with_capacity(n);
        let mut messages = Vec::with_capacity(n);
        for i in 0..n {
            messages.push(r.str(&format!("message {i}"))?.to_owned());
        }
        for (i, message) in messages.into_iter().enumerate() {
            let meta_txt = r.str(&format!("metadata {i}"))?;
            let (metadata, metadata_json) = if version == 0x01 {
                let metadata = if meta_txt.is_empty() {
                    Vec::new()
                } else {
                    parse_labels_json(meta_txt)
                        .map_err(|e| module_err(format!("batch blob: entry {i} metadata: {e}")))?
                        .into_iter()
                        .collect()
                };
                (metadata, None)
            } else {
                let (canonical, metadata) = canonical_rich_metadata(meta_txt)
                    .map_err(|e| module_err(format!("batch blob: entry {i} metadata: {e}")))?;
                (metadata, Some(canonical))
            };
            entries.push(LogEntry {
                ts: i64::from_le_bytes(ts_bytes[i * 8..i * 8 + 8].try_into().unwrap()),
                level: levels[i],
                severity: severities[i].clone(),
                message,
                metadata,
                metadata_json,
            });
        }
        if r.remaining() != 0 {
            return Err(module_err(format!(
                "batch blob: {} trailing byte(s) (corrupt or wrong n_entries)",
                r.remaining()
            )));
        }
        self.shared
            .engine
            .record_ingest_wire_decode(decode_started.elapsed());
        let count = self.shared.engine.push_batch(entries).map_err(module_err)?;
        Ok(count as i64)
    }

    /// Hidden-column command insert ('flush' | 'optimize' |
    /// 'optimize:<max_entries>' | 'prune:<ts>').
    fn run_command(&self, cmd: &str) -> Result<i64> {
        if cmd == "flush" {
            // Drain the buffer into one RAW block (+ terms). Durable as
            // soon as the enclosing SQLite transaction commits.
            self.shared.engine.flush().map_err(module_err)?;
        } else if cmd == "optimize" {
            // Unbounded manual maintenance: raw compression plus eligible
            // size-tiered compressed merges in one atomic swap.
            self.shared.engine.optimize().map_err(module_err)?;
        } else if let Some(value) = cmd.strip_prefix("optimize:") {
            let max_entries: usize = value.trim().parse().map_err(|_| {
                module_err(format!(
                    "optimize: expected 'optimize:<positive max entries>', got {cmd:?}"
                ))
            })?;
            self.shared
                .engine
                .optimize_budgeted(max_entries)
                .map_err(module_err)?;
        } else if let Some(keys) = cmd.strip_prefix("reindex:") {
            // Rewrite every block's postings against a new index_keys
            // allowlist and persist it. Postings are written at insert time,
            // so widening the allowlist without this makes pruning on a newly
            // indexed key skip every block written before the change — the
            // entries survive, but queries stop finding them.
            //
            // The connection keeps the allowlist it loaded at xConnect; the
            // persisted value is what the next connect reads. Callers that
            // need the new keys applied to *their* session reconnect after
            // this returns.
            let parsed = parse_index_keys_value(&self.table_name, keys.trim())
                .map_err(|e| module_err(format!("reindex: {e}")))?;

            return self
                .shared
                .engine
                .reindex(&parsed)
                .map(|rewritten| rewritten as i64)
                .map_err(module_err);
        } else if let Some(ts_str) = cmd.strip_prefix("prune:") {
            // Retention: whole-block deletes by ts_max, term rows
            // removed in the same operation.
            let ts: i64 = ts_str
                .trim()
                .parse()
                .map_err(|_| module_err(format!("prune: expected 'prune:<ts>', got {cmd:?}")))?;
            self.shared.engine.prune(ts).map_err(module_err)?;
        } else if let Some(value) = cmd.strip_prefix("message_index:") {
            // Opt out of (or back into) the F6 trigram message index.
            // 'none' persists the choice and drops every tg: posting now;
            // 'trigram' persists the opt-in — postings for existing
            // blocks backfill via 'reindex:<keys>' after reconnect, and
            // new blocks index from the next connect on (a live
            // connection keeps the setting it loaded).
            match value.trim() {
                "none" => {
                    self.shared
                        .engine
                        .save_message_index_meta("none")
                        .map_err(module_err)?;
                    let removed = self
                        .shared
                        .engine
                        .purge_trigram_postings()
                        .map_err(module_err)?;
                    return Ok(removed as i64);
                }
                "trigram" => {
                    self.shared
                        .engine
                        .save_message_index_meta("trigram")
                        .map_err(module_err)?;
                }
                other => {
                    return Err(module_err(format!(
                        "message_index: expected 'none' or 'trigram', got {other:?}"
                    )));
                }
            }
        } else if let Some(value) = cmd.strip_prefix("retention:") {
            // Change the retention window on a live store — same value
            // grammar as the CREATE arg (<n>[s|m|h|d]). Persisted for
            // future connects and applied to this engine immediately;
            // enforcement happens at the next flush/optimize boundary.
            let native = table_args::parse_retention(value.trim(), self.native_per_second)
                .map_err(|e| module_err(format!("retention: {e}")))?;
            self.shared
                .engine
                .set_retention_persistent(native)
                .map_err(module_err)?;
        } else {
            return Err(module_err(format!(
                "unknown command {cmd:?}; supported: 'flush', 'optimize', \
                 'optimize:<max_entries>', 'prune:<ts>', 'reindex:<keys>', \
                 'retention:<n[s|m|h|d]>', 'message_index:<none|trigram>'"
            )));
        }
        Ok(0)
    }
}

/// Parse `index_keys='a,b,c'` from the CREATE VIRTUAL TABLE args.
/// No args (or an empty list) is allowed: level + ts + message-scan
/// queries still work, there are just no metadata posting lists.
fn parse_index_keys_value(table: &str, value: &str) -> std::result::Result<Vec<String>, String> {
    let mut keys: Vec<String> = Vec::new();
    {
        for k in value.split(',') {
            let k = k.trim();
            if k.is_empty() {
                continue; // index_keys='' means "none"
            }
            // Each key becomes a declared column name: reject collisions
            // with the fixed columns and the hidden command column now,
            // with a message better than SQLite's "duplicate column".
            if [
                "ts",
                "level",
                "message",
                "metadata",
                "message_contains",
                "max_work_entries",
            ]
            .contains(&k)
                || k == table
            {
                return Err(format!(
                    "index key {k:?} collides with a built-in column name"
                ));
            }
            if !keys.iter().any(|e| e == k) {
                keys.push(k.to_owned());
            }
        }
    }
    if keys.len() > MAX_INDEX_KEYS {
        return Err(format!(
            "too many index keys ({}); the pushdown bitmask supports at most {MAX_INDEX_KEYS}",
            keys.len()
        ));
    }
    Ok(keys)
}

unsafe impl<'vtab> VTab<'vtab> for LogsTab {
    type Aux = Arc<LogQueryReportState>;
    type Cursor = LogsCursor<'vtab>;

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

    /// Pushdown: level equality, ts range, and equality on any of the
    /// index-key hidden columns (each becomes a posting-list term).
    ///
    /// idx_num bitmask: 1 = level eq, 2 = ts lower, 4 = ts upper,
    /// 8<<k = equality on index key k. argv slots are claimed in that
    /// canonical order so filter() decodes positions from the mask.
    ///
    /// `message LIKE '%...%'` remains a compatibility path: the vtab uses it
    /// for sound trigram block pruning and SQLite rechecks individual rows.
    /// `message_contains = ?` is the exact case-insensitive substring path;
    /// it filters inside the engine and can therefore participate in bounded
    /// ORDER BY ts LIMIT/OFFSET execution.
    ///
    /// `ORDER BY ts ASC|DESC LIMIT/OFFSET` is consumed only when every
    /// row-filtering constraint is exact in the engine. SQLite still rechecks
    /// those constraints and still applies LIMIT/OFFSET; xFilter returns the
    /// already ordered `LIMIT + OFFSET` prefix so that rechecking is harmless.
    /// Strict timestamp bounds and compatibility message LIKE remain
    /// unbounded because their exact semantics still live above this boundary.
    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        use IndexConstraintOp::*;

        // Pass 1 (immutable): find the first usable constraint of each
        // kind. Columns: 0 ts, 1 level, 2 message, 3 metadata, then
        // FIXED_COLS + k for index key k.
        let mut level_c: Option<usize> = None;
        let mut lo_c: Option<usize> = None;
        let mut hi_c: Option<usize> = None;
        let mut like_c: Option<usize> = None;
        let mut contains_c: Option<usize> = None;
        let mut work_limit_c: Option<usize> = None;
        let mut limit_c: Option<usize> = None;
        let mut offset_c: Option<usize> = None;
        let mut key_c: Vec<Option<usize>> = vec![None; self.index_keys.len()];
        let mut bounded_safe = true;
        for (i, c) in info.constraints().enumerate() {
            if !c.is_usable() {
                if !matches!(
                    c.operator(),
                    SQLITE_INDEX_CONSTRAINT_LIMIT | SQLITE_INDEX_CONSTRAINT_OFFSET
                ) {
                    bounded_safe = false;
                }
                continue;
            }
            match (c.column(), c.operator()) {
                (_, SQLITE_INDEX_CONSTRAINT_LIMIT) if limit_c.is_none() => limit_c = Some(i),
                (_, SQLITE_INDEX_CONSTRAINT_OFFSET) if offset_c.is_none() => offset_c = Some(i),
                (1, SQLITE_INDEX_CONSTRAINT_EQ) if level_c.is_none() => level_c = Some(i),
                // F6: message LIKE — claimed for trigram PRUNING only.
                // omit is never set, so SQLite still rechecks the LIKE
                // row-exactly (and ESCAPE'd LIKEs never reach vtabs).
                (2, SQLITE_INDEX_CONSTRAINT_LIKE) if like_c.is_none() => {
                    like_c = Some(i);
                    bounded_safe = false;
                }
                (col, SQLITE_INDEX_CONSTRAINT_EQ)
                    if col as usize == FIXED_COLS + self.index_keys.len()
                        && contains_c.is_none() =>
                {
                    contains_c = Some(i);
                }
                (col, SQLITE_INDEX_CONSTRAINT_EQ)
                    if col as usize == FIXED_COLS + self.index_keys.len() + 1
                        && work_limit_c.is_none() =>
                {
                    work_limit_c = Some(i);
                }
                (0, SQLITE_INDEX_CONSTRAINT_GE) if lo_c.is_none() => lo_c = Some(i),
                (0, SQLITE_INDEX_CONSTRAINT_LE) if hi_c.is_none() => hi_c = Some(i),
                (0, SQLITE_INDEX_CONSTRAINT_GT) if lo_c.is_none() => {
                    lo_c = Some(i);
                    bounded_safe = false;
                }
                (0, SQLITE_INDEX_CONSTRAINT_LT) if hi_c.is_none() => {
                    hi_c = Some(i);
                    bounded_safe = false;
                }
                (col, SQLITE_INDEX_CONSTRAINT_EQ) => {
                    let col = col as usize;
                    if col >= FIXED_COLS && col < FIXED_COLS + self.index_keys.len() {
                        let k = col - FIXED_COLS;
                        if key_c[k].is_none() {
                            key_c[k] = Some(i);
                        } else {
                            bounded_safe = false;
                        }
                    } else {
                        bounded_safe = false;
                    }
                }
                _ => bounded_safe = false,
            }
        }

        let bounded_order = if bounded_safe && limit_c.is_some() && info.num_of_order_by() == 1 {
            let mut order_bys = info.order_bys();
            order_bys.next().and_then(|order_by| {
                (order_by.column() == 0).then_some(if order_by.is_order_by_desc() {
                    LogQueryOrder::Desc
                } else {
                    LogQueryOrder::Asc
                })
            })
        } else {
            None
        };

        // Pass 2 (mutable): claim argv slots in canonical order.
        let mut mask: c_int = 0;
        let mut slot: c_int = 1;
        let mut claim = |info: &mut IndexInfo, c: Option<usize>, bit: c_int| {
            if let Some(i) = c {
                info.constraint_usage(i).set_argv_index(slot);
                slot += 1;
                mask |= bit;
            }
        };
        claim(info, level_c, BIT_LEVEL);
        claim(info, lo_c, BIT_TS_LO);
        claim(info, hi_c, BIT_TS_HI);
        claim(info, like_c, BIT_MSG_LIKE);
        for (k, c) in key_c.iter().enumerate() {
            claim(info, *c, 1 << (FIRST_KEY_BIT_SHIFT + k));
        }
        claim(info, contains_c, BIT_MSG_CONTAINS);
        if bounded_order.is_some() {
            claim(info, limit_c, 0);
            claim(info, offset_c, 0);
        }
        claim(info, work_limit_c, 0);

        info.set_idx_num(mask);
        let bounded_plan = bounded_order.map(|order| match (order, offset_c.is_some()) {
            (LogQueryOrder::Asc, false) => PLAN_BOUNDED_TS_ASC,
            (LogQueryOrder::Asc, true) => PLAN_BOUNDED_TS_ASC_OFFSET,
            (LogQueryOrder::Desc, false) => PLAN_BOUNDED_TS_DESC,
            (LogQueryOrder::Desc, true) => PLAN_BOUNDED_TS_DESC_OFFSET,
        });
        if let Some(plan) = bounded_plan {
            let plan = if work_limit_c.is_some() {
                format!("{plan}{PLAN_WORK_LIMIT_SUFFIX}")
            } else {
                plan.to_owned()
            };
            info.set_idx_str(&plan);
            info.set_order_by_consumed(true);
            info.set_estimated_rows(100);
        } else if work_limit_c.is_some() {
            info.set_idx_str(PLAN_WORK_LIMIT);
        }
        // Any pushed constraint prunes blocks via terms or ts range; a
        // bare scan decompresses everything. Steer the planner.
        info.set_estimated_cost(if mask != 0 { 1e3 } else { 1e6 });
        Ok(true)
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        Ok(LogsCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            shared: Arc::clone(&self.shared),
            db: self.db,
            table_name: self.table_name.clone(),
            database_name: self.database_name.clone(),
            index_keys: self.index_keys.clone(),
            query_reports: Arc::clone(&self.query_reports),
            rows: Vec::new(),
            pos: 0,
            message_contains: None,
            max_work_entries: None,
            phantom: PhantomData,
        })
    }
}

/// Defensive gate release on teardown — see metrics_vtab.rs.
impl Drop for LogsTab {
    fn drop(&mut self) {
        self.release_write_gate();
        self.query_reports
            .clear(&self.database_name, &self.table_name);
    }
}

impl CreateVTab<'_> for LogsTab {
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
        shared::pin_for_drop(self.db, &self.key, &self.shared);
        let _bind = DbGuard::bind(self.db);
        let host = unsafe { Connection::from_handle(self.db) }?;
        host.execute_batch(&shadow_block_store::drop_ddl(
            &self.database_name,
            &self.table_name,
        ))
    }
}

impl UpdateVTab<'_> for LogsTab {
    /// INSERT. argv: [0] NULL, [1] requested rowid, then declared
    /// columns from index 2: 2=ts, 3=level, 4=message, 5=metadata,
    /// 6..6+K = index keys, 6+K = message_contains query input,
    /// 7+K = max_work_entries query input, 8+K = hidden command column.
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        // Connection routing + writer gate, as in metrics_vtab.rs
        // (gate is normally taken by begin(); this is the defensive
        // re-check). Matters even more here: push() AUTO-FLUSHES at
        // the threshold, so an insert can write real block rows.
        let _bind = DbGuard::bind(self.db);
        self.acquire_write_gate()?;

        let cmd_idx = 2 + FIXED_COLS + self.index_keys.len() + 2;
        // Command idiom, dispatched by TYPE like metrics: TEXT command,
        // BLOB reserved for a future Tier 2 batch format, NULL = data.
        match args.iter().nth(cmd_idx) {
            Some(ValueRef::Null) | None => {} // plain data row
            Some(ValueRef::Blob(blob)) => {
                // Versioned public batches. Unknown revisions fail loudly.
                return match blob.first() {
                    Some(0x01 | 0x02) => self.ingest_batch(blob),
                    Some(b @ (0x00 | 0x03..=0x08)) => Err(module_err(format!(
                        "unknown batch version 0x{b:02x} (this build speaks v0 = 0x01 and v1 = 0x02)"
                    ))),
                    Some(b) => Err(module_err(format!(
                        "unknown blob format (first byte 0x{b:02x}; logs batches start with 0x01/0x02)"
                    ))),
                    None => Err(module_err("empty blob".into())),
                };
            }
            Some(_) => {
                let cmd: String = args.get(cmd_idx)?;
                return self.run_command(&cmd);
            }
        }

        let ts: Option<i64> = args.get(2)?;
        let Some(ts) = ts else {
            return Err(module_err("ts is required (INTEGER)".into()));
        };
        let level_txt: Option<String> = args.get(3)?;
        let Some(level_txt) = level_txt else {
            return Err(module_err(
                "level is required (TEXT product severity)".into(),
            ));
        };
        let severity = canonical_severity(&level_txt)
            .map_err(module_err)?
            .to_owned();
        let level = level_from_name(&severity).map_err(module_err)?;
        let message: Option<String> = args.get(4)?;
        let Some(message) = message else {
            return Err(module_err("message is required (TEXT)".into()));
        };

        // Rich metadata is retained as canonical typed JSON. Scalar/nested
        // values also derive stable string projections for equality indexes.
        let metadata_json: Option<String> = args.get(5)?;
        let (mut canonical_json, mut metadata): (String, Vec<(String, String)>) =
            match metadata_json {
                Some(txt) => canonical_rich_metadata(&txt).map_err(module_err)?,
                None => ("{}".into(), Vec::new()),
            };

        // Index-key hidden columns as INSERT shorthand: a non-NULL
        // value is merged into the metadata pairs (overriding a same-key
        // pair from the JSON — the more specific binding wins).
        let mut metadata_overridden = false;
        for (k, key_name) in self.index_keys.iter().enumerate() {
            let v: Option<String> = args.get(6 + k)?;
            if let Some(v) = v {
                metadata_overridden = true;
                metadata.retain(|(mk, _)| mk != key_name);
                metadata.push((key_name.clone(), v));
            }
        }
        if metadata_overridden {
            let mut value: serde_json::Value = serde_json::from_str(&canonical_json)
                .map_err(|error| module_err(format!("decode metadata JSON: {error}")))?;
            let object = value
                .as_object_mut()
                .ok_or_else(|| module_err("metadata JSON must be an object".into()))?;
            for (k, key_name) in self.index_keys.iter().enumerate() {
                let v: Option<String> = args.get(6 + k)?;
                if let Some(v) = v {
                    object.insert(key_name.clone(), serde_json::Value::String(v));
                }
            }
            let sorted: std::collections::BTreeMap<String, serde_json::Value> =
                object.clone().into_iter().collect();
            canonical_json = serde_json::to_string(&sorted)
                .map_err(|error| module_err(format!("encode metadata JSON: {error}")))?;
        }

        // push() canonicalizes (sorts) metadata, validates, and
        // auto-flushes at the threshold.
        let rich = severity != timeless_core::level_name(level)
            || !metadata_is_flat_strings(&canonical_json);
        self.shared
            .engine
            .push(LogEntry {
                ts,
                level,
                severity: rich.then_some(severity),
                message,
                metadata,
                metadata_json: rich.then_some(canonical_json),
            })
            .map_err(module_err)?;

        // Synthetic rowid, same as metrics: entries live in blocks, not
        // addressable rows.
        self.rowid_counter += 1;
        Ok(self.rowid_counter)
    }

    fn delete(&mut self, _arg: ValueRef<'_>) -> Result<()> {
        Err(module_err(
            "timeless_logs is append-only; DELETE is not supported \
             (use INSERT INTO t(t) VALUES('prune:<ts>') for retention)"
                .into(),
        ))
    }

    fn update(&mut self, _args: &Updates<'_>) -> Result<()> {
        Err(module_err(
            "timeless_logs is append-only; UPDATE is not supported".into(),
        ))
    }
}

/// Real transaction semantics (PLAN.md R5 — FIXED), same shape as
/// metrics_vtab.rs (read the full comment there): xBegin activates the
/// BlockEngine's journal (cheap on purpose — SQLite brackets every
/// autocommit write statement with xBegin/xCommit, verified
/// empirically), xCommit drops it, xRollback undoes engine memory to
/// mirror the host rollback of `_blocks`/`_terms`.
///
/// This matters MORE here than for metrics: push() AUTO-FLUSHES at the
/// threshold, so a big INSERT inside a transaction writes real block
/// rows mid-txn. On ROLLBACK those rows vanish — the journal removes
/// their index entries (no dangling locs) and returns any pre-txn
/// buffered entries the flush drained back to the buffer. All commands
/// ('flush', 'optimize', 'optimize:<entries>', 'prune:<ts>') are journaled and roll back
/// fully. vtab_tx.rs supplies the savepoint callbacks missing from
/// rusqlite so failed statements and explicit ROLLBACK TO are covered.
/// R4 ADDITION — writer gate brackets the journal exactly as in
/// metrics_vtab.rs (read the comment there): acquire before
/// txn_begin, holder-only commit/rollback, release after.
impl TransactionVTab<'_> for LogsTab {
    fn begin(&mut self) -> Result<()> {
        let _bind = DbGuard::bind(self.db);
        self.acquire_write_gate()?;
        self.shared.engine.txn_begin();
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let _bind = DbGuard::bind(self.db);
        if self.gate_held {
            self.shared.engine.txn_commit();
            self.release_write_gate();
        }
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        let _bind = DbGuard::bind(self.db);
        if self.gate_held {
            self.shared.engine.txn_rollback();
            self.release_write_gate();
        }
        Ok(())
    }
}

impl SavepointVTab for LogsTab {
    fn savepoint(&mut self, id: c_int) {
        let _bind = DbGuard::bind(self.db);
        if self.gate_held {
            self.shared.engine.txn_savepoint(id);
        }
    }

    fn release(&mut self, id: c_int) {
        let _bind = DbGuard::bind(self.db);
        if self.gate_held {
            self.shared.engine.txn_release(id);
        }
    }

    fn rollback_to(&mut self, id: c_int) {
        let _bind = DbGuard::bind(self.db);
        if self.gate_held {
            self.shared.engine.txn_rollback_to(id);
        }
    }
}

// ---------------------------------------------------------------------------
// The cursor
// ---------------------------------------------------------------------------

/// One output row, materialized at filter() time. Keeps the decoded
/// entry (column() digs index-key values out of its metadata) plus the
/// metadata pre-rendered to canonical sorted flat JSON.
struct OutRow {
    entry: LogEntry,
    metadata_json: String,
}

#[repr(C)]
pub struct LogsCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    shared: Arc<SharedEngine<BlockEngine>>,
    /// The connection driving this scan (bound in filter()).
    db: *mut ffi::sqlite3,
    table_name: String,
    database_name: String,
    index_keys: Vec<String>,
    query_reports: Arc<LogQueryReportState>,
    rows: Vec<OutRow>,
    pos: usize,
    /// Bound value of the public exact-search hidden column. Returning it from
    /// column() lets SQLite safely recheck `message_contains = ?`.
    message_contains: Option<String>,
    max_work_entries: Option<usize>,
    phantom: PhantomData<&'vtab LogsTab>,
}

unsafe impl VTabCursor for LogsCursor<'_> {
    /// Decode the pushed constraints per the best_index bitmask, run
    /// one engine query (sequential block reads — no rayon anywhere on
    /// this path, per the Session 3 deadlock lesson), materialize rows.
    fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        let started = Instant::now();
        // A failed or cancelled scan must never leave a prior successful
        // statement's report observable.
        self.query_reports
            .clear(&self.database_name, &self.table_name);
        // Route block reads to the connection running this SELECT.
        let _bind = DbGuard::bind(self.db);
        // argv slots were claimed in canonical order (level, ts lo,
        // ts hi, index keys), so the mask alone tells us which
        // positional arg is which. Exact message_contains follows all dynamic
        // index keys, then bounded LIMIT/OFFSET arguments follow it.
        let mut arg = 0usize;
        let mut next = || {
            let i = arg;
            arg += 1;
            i
        };

        // Level: pushed as TEXT. An unknown level name (or NULL) can
        // match nothing — empty result, not an error (WHERE level='oops'
        // is a valid query that happens to select zero rows).
        let mut impossible = false;
        let (level, severity): (Option<u8>, Option<String>) = if idx_num & BIT_LEVEL != 0 {
            let v: Option<String> = args.get(next())?;
            match v.as_deref().map(canonical_severity) {
                Some(Ok(severity)) => (
                    Some(level_from_name(severity).map_err(module_err)?),
                    Some(severity.to_owned()),
                ),
                _ => {
                    impossible = true;
                    (None, None)
                }
            }
        } else {
            (None, None)
        };
        let ts_min: i64 = if idx_num & BIT_TS_LO != 0 {
            let v: Option<i64> = args.get(next())?;
            match v {
                Some(v) => v,
                None => {
                    impossible = true; // ts >= NULL matches nothing
                    i64::MIN
                }
            }
        } else {
            i64::MIN
        };
        let ts_max: i64 = if idx_num & BIT_TS_HI != 0 {
            let v: Option<i64> = args.get(next())?;
            match v {
                Some(v) => v,
                None => {
                    impossible = true;
                    i64::MAX
                }
            }
        } else {
            i64::MAX
        };
        // F6: the LIKE pattern, for trigram block pruning only (SQLite
        // rechecks rows — omit was never set). NULL pattern: LIKE NULL
        // matches nothing, but leave that to SQLite's recheck.
        let message_like_prune: Option<String> = if idx_num & BIT_MSG_LIKE != 0 {
            args.get(next())?
        } else {
            None
        };
        let mut metadata_eq: Vec<(String, String)> = Vec::new();
        for (k, key_name) in self.index_keys.iter().enumerate() {
            if idx_num & (1 << (FIRST_KEY_BIT_SHIFT + k)) != 0 {
                let v: Option<String> = args.get(next())?;
                match v {
                    Some(v) => metadata_eq.push((key_name.clone(), v)),
                    None => impossible = true, // key = NULL matches nothing
                }
            }
        }
        let message_contains: Option<String> = if idx_num & BIT_MSG_CONTAINS != 0 {
            let value: Option<String> = args.get(next())?;
            if value.is_none() {
                impossible = true;
            }
            value
        } else {
            None
        };
        self.message_contains = message_contains.clone();

        let (bounded_idx_str, has_work_limit) = match idx_str {
            Some(PLAN_WORK_LIMIT) => (None, true),
            Some(plan) if plan.ends_with(PLAN_WORK_LIMIT_SUFFIX) => (
                Some(&plan[..plan.len() - PLAN_WORK_LIMIT_SUFFIX.len()]),
                true,
            ),
            plan => (plan, false),
        };
        let bounded_order = match bounded_idx_str {
            Some(PLAN_BOUNDED_TS_ASC) => Some((LogQueryOrder::Asc, false)),
            Some(PLAN_BOUNDED_TS_ASC_OFFSET) => Some((LogQueryOrder::Asc, true)),
            Some(PLAN_BOUNDED_TS_DESC) => Some((LogQueryOrder::Desc, false)),
            Some(PLAN_BOUNDED_TS_DESC_OFFSET) => Some((LogQueryOrder::Desc, true)),
            _ => None,
        };
        let bounded = if let Some((order, has_offset)) = bounded_order {
            let limit: Option<i64> = args.get(next())?;
            let offset: Option<i64> = if has_offset {
                args.get(next())?
            } else {
                Some(0)
            };
            let capacity = match (limit, offset) {
                (Some(0), _) => Some(0),
                (Some(limit), Some(offset)) if limit > 0 => {
                    let offset = offset.max(0);
                    limit
                        .checked_add(offset)
                        .and_then(|n| usize::try_from(n).ok())
                }
                // LIMIT -1 means unbounded. NULL/non-integral values are
                // rejected by SQLite's own LIMIT bytecode after xFilter.
                _ => None,
            };
            Some((order, capacity))
        } else {
            None
        };
        let max_work_entries = if has_work_limit {
            let value: Option<i64> = args.get(next())?;
            let value = value.ok_or_else(|| {
                module_err("max_work_entries must not be NULL and must be positive".into())
            })?;
            if value <= 0 {
                return Err(module_err("max_work_entries must be positive".into()));
            }
            Some(usize::try_from(value).map_err(|_| {
                module_err(format!(
                    "max_work_entries {value} exceeds this platform's usize"
                ))
            })?)
        } else {
            None
        };
        self.max_work_entries = max_work_entries;

        let (entries, mut report) = if impossible {
            (Vec::new(), LogQueryExecutionReport::default())
        } else {
            let read = self
                .shared
                .write_gate
                .acquire_read(self.db as usize, &self.table_name)
                .map_err(module_err)?;
            let query = LogQuery {
                ts_min,
                ts_max,
                level,
                severity,
                metadata_eq,
                message_contains,
                message_like_prune,
            };
            let (order, capacity) = bounded.unwrap_or((LogQueryOrder::Asc, None));
            self.shared
                .engine
                .query_ordered_with_work_limit_report_after_snapshot(
                    &query,
                    order,
                    capacity,
                    max_work_entries,
                    move || drop(read),
                )
                .map_err(module_err)?
        };

        self.rows = entries
            .into_iter()
            .map(|entry| {
                let metadata_json = entry
                    .metadata_json
                    .clone()
                    .unwrap_or_else(|| pairs_to_json(&entry.metadata));
                OutRow {
                    metadata_json,
                    entry,
                }
            })
            .collect();
        report.query_total_ns = elapsed_ns(started);
        report.returned_entries = self.rows.len() as u64;
        self.query_reports
            .publish(&self.database_name, &self.table_name, report);
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        let row = &self.rows[self.pos];
        let i = i as usize;
        match i {
            0 => ctx.set_result(&row.entry.ts),
            1 => ctx.set_result(&row.entry.severity_name()),
            2 => ctx.set_result(&row.entry.message),
            3 => ctx.set_result(&row.metadata_json),
            _ if i >= FIXED_COLS && i < FIXED_COLS + self.index_keys.len() => {
                // Index-key hidden column: surface the value from the
                // entry's metadata so SELECT service works. NULL when
                // the entry has no such key.
                match row.entry.meta_value(&self.index_keys[i - FIXED_COLS]) {
                    Some(v) => ctx.set_result(&v),
                    None => ctx.set_result(&Null),
                }
            }
            _ if i == FIXED_COLS + self.index_keys.len() => match &self.message_contains {
                Some(value) => ctx.set_result(value),
                None => ctx.set_result(&Null),
            },
            _ if i == FIXED_COLS + self.index_keys.len() + 1 => match self.max_work_entries {
                Some(value) => ctx.set_result(&(value as i64)),
                None => ctx.set_result(&Null),
            },
            // The hidden command column reads as NULL.
            _ => ctx.set_result(&Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}
