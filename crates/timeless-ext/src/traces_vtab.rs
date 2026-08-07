//! timeless_traces: the Phase 2 trace-store vtab (PLAN.md Session 6),
//! backed by a timeless_core::SpanBlockEngine persisting through
//! ShadowSpanStore into `<table>_blocks` / `<table>_terms` /
//! `<table>_trace_blocks` / `<table>_meta` on the host db. Same
//! skeleton as logs_vtab.rs (which says "read metrics_vtab.rs first");
//! only the trace-specific parts are commented in depth here.
//!
//!   CREATE VIRTUAL TABLE traces USING timeless_traces;
//!
//! Declared schema (fixed — traces need no index_keys arg; the four
//! indexed dimensions are OTel-conventional, see spans/mod.rs):
//!
//!   CREATE TABLE x(trace_id BLOB, span_id BLOB, parent_span_id BLOB,
//!                  name TEXT, service TEXT, kind TEXT, status TEXT,
//!                  start_ts INTEGER, duration_ns INTEGER,
//!                  attributes TEXT, status_description TEXT,
//!                  events TEXT, resource TEXT,
//!                  instrumentation_scope TEXT, `"<table>"` HIDDEN)
//!
//! Ids: trace_id/span_id/parent_span_id accept either a BLOB of the
//! exact packed length (16/8/8 bytes) or a hex TEXT string (32/16/16
//! chars) on INSERT — OTel tooling hands out hex, storage wants packed
//! (the timeless_traces lesson). They are ALWAYS returned as BLOBs;
//! use hex(trace_id) in SQL for display.
//!
//! kind/status are TEXT in SQL (internal/server/client/producer/
//! consumer, unset/ok/error) mapped to the storage bytes at the
//! boundary — same strict-vocabulary policy as log levels.
//!
//! start_ts is NANOSECONDS (OTel convention; logs are ms, metrics s).
//! The unit is recorded in `_meta` under 'ts_unit' for tooling.
//!
//! Write path:  INSERT INTO traces(trace_id, span_id, ...) — one span
//!              into the engine buffer (auto-flush at threshold).
//! Commands:    INSERT INTO traces(traces) VALUES ('flush' | 'optimize'
//!              | `optimize:<max_spans>` | `prune:<ts>`) — the FTS5
//!              idiom, ts in ns. The budgeted form bounds one
//!              maintenance call while preserving the same planner.
//! Read path:   flushed blocks + in-memory buffer merged; the HERO
//!              query `WHERE trace_id = x'...'` goes through the
//!              `_trace_blocks` index and decompresses only blocks
//!              containing that trace.
//! Append-only: DELETE/UPDATE rejected; retention is `prune:<ts>`.

use std::borrow::Cow;
use std::ffi::{c_int, CStr, CString};
use std::marker::PhantomData;
use std::sync::Arc;

use rusqlite::ffi;
use rusqlite::types::{Null, Value, ValueRef};
use rusqlite::vtab::{
    escape_double_quote, Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, Inserts,
    Module, TransactionVTab, UpdateVTab, Updates, VTab, VTabConnection, VTabCursor, VTabKind,
};
use rusqlite::{Connection, Error, Result};
use timeless_core::{
    kind_from_name, kind_name, status_from_name, status_name, SpanBlockEngine, SpanBlockStore,
    SpanEngineConfig, SpanEntry, SpanQuery, SpanQueryOrder, SpanQueryStream,
};

use crate::batch::BatchReader;
use crate::flatjson::{pairs_to_json, parse_labels_json};
use crate::otel_json;
use crate::shadow_meta;
use crate::shadow_span_store::{self, ShadowSpanStore};
use crate::shared::{self, DbGuard, RegistryKey, SharedEngine};
use crate::sql_ident;
use crate::table_args;
use crate::vtab_tx::{self, SavepointVTab};

/// Register the "timeless_traces" module on a freshly-loaded connection.
pub(crate) fn register(db: &Connection) -> Result<()> {
    const MODULE: Module<TracesTab> = vtab_tx::update_module_with_savepoints();
    db.create_module(c"timeless_traces", &MODULE, None::<()>)
}

/// Engine parameters (see SpanEngineConfig for what each knob means).
const FLUSH_THRESHOLD: usize = 8192; // buffered spans before auto-flush
const ZSTD_LEVEL: i32 = 7;
const MERGE_TARGET_ENTRIES: usize = 8192;
/// HARD CAP on merged-block ts span: 1 hour in NANOSECONDS (this vtab
/// documents start_ts as unix ns). Same retention-boundary rule as the
/// logs vtab (which passes 1h in ms) — the engine is unit-agnostic, the
/// vtab supplies the unit.
const MERGE_MAX_TS_SPAN: i64 = 3_600_000_000_000;
/// F2 retention unit conversion: traces start_ts is epoch NANOSECONDS.
const NATIVE_PER_SECOND: i64 = 1_000_000_000;

/// best_index bitmask. BIT_TRACE is the star: trace_id equality routes
/// the cursor through the `_trace_blocks` index, so it gets a
/// near-point-lookup cost estimate that beats every other plan.
const BIT_TRACE: c_int = 1;
const BIT_SERVICE: c_int = 2;
const BIT_KIND: c_int = 4;
const BIT_STATUS: c_int = 8;
const BIT_NAME: c_int = 16;
const BIT_TS_LO: c_int = 32;
const BIT_TS_HI: c_int = 64;
const BIT_DURATION_LO: c_int = 128;
const BIT_DURATION_HI: c_int = 256;

const PLAN_BOUNDED_TS_ASC: &str = "bounded-ts-asc";
const PLAN_BOUNDED_TS_ASC_OFFSET: &str = "bounded-ts-asc-offset";
const PLAN_BOUNDED_TS_DESC: &str = "bounded-ts-desc";
const PLAN_BOUNDED_TS_DESC_OFFSET: &str = "bounded-ts-desc-offset";

/// Declared column indices (argv in xUpdate = these + 2).
const COL_TRACE_ID: usize = 0;
const COL_SPAN_ID: usize = 1;
const COL_PARENT: usize = 2;
const COL_NAME: usize = 3;
const COL_SERVICE: usize = 4;
const COL_KIND: usize = 5;
const COL_STATUS: usize = 6;
const COL_START_TS: usize = 7;
const COL_DURATION: usize = 8;
const COL_ATTRS: usize = 9;
const COL_STATUS_DESCRIPTION: usize = 10;
const COL_EVENTS: usize = 11;
const COL_RESOURCE: usize = 12;
const COL_SCOPE: usize = 13;
/// The hidden command column (named after the table, FTS5 idiom).
const COL_COMMAND: usize = 14;

fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

// ---------------------------------------------------------------------------
// Id parsing: BLOB (packed) or hex TEXT in, packed [u8; N] out
// ---------------------------------------------------------------------------

/// Decode a hex string of exactly 2N chars into N bytes. Hand-rolled
/// (no new deps for 12 lines); case-insensitive like every hex tool.
fn hex_to_bytes<const N: usize>(s: &str) -> Option<[u8; N]> {
    let b = s.as_bytes();
    if b.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (i, pair) in b.chunks_exact(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out[i] = ((hi << 4) | lo) as u8;
    }
    Some(out)
}

/// Parse an id column value: BLOB of exactly N bytes, or TEXT of
/// exactly 2N hex chars. `what` names the column in errors ("trace_id
/// must be a 16-byte BLOB or 32-char hex string").
fn parse_id<const N: usize>(v: ValueRef<'_>, what: &str) -> Result<[u8; N]> {
    match v {
        ValueRef::Blob(b) => <[u8; N]>::try_from(b).map_err(|_| {
            module_err(format!(
                "{what} BLOB is {} byte(s); expected exactly {N}",
                b.len()
            ))
        }),
        ValueRef::Text(t) => {
            let s = std::str::from_utf8(t)
                .map_err(|_| module_err(format!("{what} TEXT is not valid UTF-8")))?;
            hex_to_bytes::<N>(s).ok_or_else(|| {
                module_err(format!(
                    "{what} TEXT {s:?} is not a {}-char hex string",
                    N * 2
                ))
            })
        }
        _ => Err(module_err(format!(
            "{what} must be a {N}-byte BLOB or {}-char hex TEXT",
            N * 2
        ))),
    }
}

// ---------------------------------------------------------------------------
// The virtual table
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct TracesTab {
    base: ffi::sqlite3_vtab,
    /// Raw handle to the HOST connection, kept for xDestroy's DDL.
    db: *mut ffi::sqlite3,
    table_name: String,
    database_name: String,
    /// Shared process-wide across connections via the R4 registry —
    /// see metrics_vtab.rs and shared.rs for the full story.
    shared: Arc<SharedEngine<SpanBlockEngine>>,
    key: RegistryKey,
    /// True while THIS connection's write txn holds the writer gate.
    gate_held: bool,
    rowid_counter: i64,
}

impl TracesTab {
    /// Resolve the shared engine for an EXISTING timeless_traces table on
    /// this connection — the read-side entry point for stats/kernel TVFs
    /// (query_tvf.rs). Mirrors the xConnect tail. A table that was never
    /// a timeless_traces vtab fails on the `_meta` read with SQLite's
    /// own "no such table". Caller must hold a DbGuard binding.
    pub(crate) fn shared_engine_for(
        handle: *mut ffi::sqlite3,
        database: &str,
        table: &str,
    ) -> Result<Arc<SharedEngine<SpanBlockEngine>>> {
        let host = unsafe { Connection::from_handle(handle) }?;
        let instance_id =
            shadow_meta::ensure_instance_id(&host, database, table).map_err(module_err)?;
        let store = ShadowSpanStore::new(database, table);
        let key = shared::registry_key(handle, database.as_bytes(), table, instance_id);
        let shared = shared::get_or_create(&key, move || {
            SpanBlockEngine::new(
                Box::new(store),
                SpanEngineConfig {
                    flush_threshold: FLUSH_THRESHOLD,
                    zstd_level: ZSTD_LEVEL,
                    merge_target_entries: MERGE_TARGET_ENTRIES,
                    merge_max_ts_span: MERGE_MAX_TS_SPAN,
                },
            )
            .map_err(module_err)
        })?;
        shared.engine.set_retention(
            shadow_meta::load_retention(&host, database, table).map_err(module_err)?,
        );
        Ok(shared)
    }

    fn connect_create(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        database_name: &[u8],
        table_name: &[u8],
        args: &[&[u8]],
        is_create: bool,
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let table = String::from_utf8_lossy(table_name).into_owned();
        let database = String::from_utf8_lossy(database_name).into_owned();
        let handle = unsafe { db.handle() };
        // Bind the calling connection for every store operation below
        // (DDL, _meta writes, recovery scans). RAII unbind.
        let _bind = DbGuard::bind(handle);

        // Unlike logs there is no index_keys knob (spans/mod.rs explains
        // why the four span dimensions are indexed unconditionally); the
        // only supported argument is F2's retention, parsed after the
        // engine exists below. Typo'd args still fail loudly there.

        let host = unsafe { Connection::from_handle(handle) }?;
        let store = ShadowSpanStore::new(&database, &table);
        if is_create {
            // Same incremental auto-vacuum attempt as metrics/logs
            // (no-op on a non-empty db; see metrics_vtab.rs).
            let _ = host.execute_batch(&sql_ident::incremental_auto_vacuum(&database));
            host.execute_batch(&shadow_span_store::ddl(&database, &table))?;
            // PLAN.md: the shared block code never assumes a ts unit —
            // record OURS in _meta so tooling (and future readers of
            // this db) know these blocks speak nanoseconds.
            store.save_meta("ts_unit", b"ns").map_err(module_err)?;
        }
        let instance_id =
            shadow_meta::ensure_instance_id(&host, &database, &table).map_err(module_err)?;

        // R4: one engine per (db file, schema alias, table, instance). First
        // connection in builds it — SpanBlockEngine::new recovers the
        // block index via scan() and status partitions via the
        // `status:` posting lists (re-entrant SELECTs routed to the
        // calling connection by the DbGuard above, safe because THIS
        // thread holds the connection mutex recursively) — every later
        // xConnect just bumps the Arc, no re-recovery.
        let key = shared::registry_key(handle, database_name, &table, instance_id);
        let shared_engine = shared::get_or_create(&key, move || {
            SpanBlockEngine::new(
                Box::new(store),
                SpanEngineConfig {
                    flush_threshold: FLUSH_THRESHOLD,
                    zstd_level: ZSTD_LEVEL,
                    merge_target_entries: MERGE_TARGET_ENTRIES,
                    merge_max_ts_span: MERGE_MAX_TS_SPAN,
                },
            )
            .map_err(module_err)
        })?;

        // F2 retention: unit-resolved (ns) at create, persisted in
        // _meta; xConnect loads it back and ignores replayed args.
        let retention = if is_create {
            let mut retention = None;
            for (name, value) in table_args::parse_kv_args(args).map_err(module_err)? {
                match name.as_str() {
                    "retention" => {
                        retention = Some(
                            table_args::parse_retention(&value, NATIVE_PER_SECOND)
                                .map_err(module_err)?,
                        );
                    }
                    other => {
                        return Err(module_err(format!(
                            "unrecognized argument {other:?}; timeless_traces supports: retention"
                        )));
                    }
                }
            }
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
            retention
        } else {
            shadow_meta::load_retention(&host, &database, &table).map_err(module_err)?
        };
        shared_engine.engine.set_retention(retention);

        let schema = format!(
            "CREATE TABLE x(trace_id BLOB, span_id BLOB, parent_span_id BLOB, \
             name TEXT, service TEXT, kind TEXT, status TEXT, \
             start_ts INTEGER, duration_ns INTEGER, attributes TEXT, \
             status_description TEXT, events TEXT, resource TEXT, \
             instrumentation_scope TEXT, \
             \"{}\" HIDDEN)",
            escape_double_quote(&table)
        );
        let schema = CString::new(schema)
            .map_err(|_| module_err(format!("table name contains NUL: {table:?}")))?;

        Ok((
            Cow::Owned(schema),
            TracesTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
                table_name: table,
                database_name: database,
                shared: shared_engine,
                key,
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

    /// Versioned Tier 2 batch ingest. Both layouts are little-endian.
    /// v0 (0x01) remains byte-for-byte compatible. v1 (0x02) retains
    /// its complete prefix and appends rich columns after attributes.
    ///   0    u8   version = 0x01 (v0) | 0x02 (v1)
    ///   1    u8   flags = 0
    ///   2    u16  reserved
    ///   4    u32  n_spans
    ///   —    trace_id[]  n × 16 bytes (packed)
    ///   —    span_id[]   n × 8 bytes
    ///   —    parent_id[] n × 8 bytes (all-zero = root/None)
    ///   —    name[]      n × { u32 len, utf8 }
    ///   —    service[]   n × { u32 len, utf8 }
    ///   —    kind[]      n × u8 (0..=4)
    ///   —    status[]    n × u8 (0..=2)
    ///   —    start_ts[]  n × i64 (ns)
    ///   —    duration[]  n × i64 (ns)
    ///   —    attributes[] n × { u32 len, JSON object; '' = {} }
    /// v1 continues with:
    ///   —    status_description[] n × { u32 len, utf8 }
    ///   —    events[] n × { u32 len, JSON array; '' = [] }
    ///   —    resource[] n × { u32 len, JSON object; '' = {} }
    ///   —    instrumentation_scope[] n × { u32 len, JSON object; '' = {} }
    ///
    /// All-or-nothing, same durability contract as row inserts.
    fn ingest_batch(&self, blob: &[u8]) -> Result<i64> {
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
        let n = r.u32("n_spans")? as usize;

        let width = |bytes: usize, label: &str| {
            n.checked_mul(bytes)
                .ok_or_else(|| module_err(format!("batch blob: n_spans overflows {label} length")))
        };
        let trace_ids = r.take(width(16, "trace_id")?, "trace_id column")?;
        let span_ids = r.take(width(8, "span_id")?, "span_id column")?;
        let parent_ids = r.take(width(8, "parent_id")?, "parent_id column")?;
        let mut names = Vec::with_capacity(n);
        for i in 0..n {
            names.push(r.str(&format!("name {i}"))?.to_owned());
        }
        let mut services = Vec::with_capacity(n);
        for i in 0..n {
            services.push(r.str(&format!("service {i}"))?.to_owned());
        }
        let kinds = r.take(n, "kind column")?;
        if let Some(bad) = kinds.iter().find(|&&k| k > 4) {
            return Err(module_err(format!(
                "batch blob: invalid kind byte {bad} (0..=4); batch rejected"
            )));
        }
        let statuses = r.take(n, "status column")?;
        if let Some(bad) = statuses.iter().find(|&&st| st > 2) {
            return Err(module_err(format!(
                "batch blob: invalid status byte {bad} (0..=2); batch rejected"
            )));
        }
        let start_bytes = r.take(width(8, "start_ts")?, "start_ts column")?;
        let dur_bytes = r.take(width(8, "duration")?, "duration column")?;

        let mut attributes: Vec<Cow<'static, str>> = Vec::with_capacity(n);
        for i in 0..n {
            let text = r.str(&format!("attributes {i}"))?;
            let canonical = if version == 0x01 {
                if text.is_empty() {
                    Cow::Borrowed("{}")
                } else {
                    let pairs: Vec<(String, String)> = parse_labels_json(text)
                        .map_err(|error| {
                            module_err(format!("batch blob: span {i} attributes: {error}"))
                        })?
                        .into_iter()
                        .collect();
                    Cow::Owned(pairs_to_json(&pairs))
                }
            } else {
                Cow::Owned(
                    otel_json::object(Some(text), "attributes")
                        .map_err(|error| module_err(format!("batch blob: span {i} {error}")))?,
                )
            };
            attributes.push(canonical);
        }

        let mut status_descriptions = vec![Cow::Borrowed(""); n];
        let mut events = vec![Cow::Borrowed("[]"); n];
        let mut resources = vec![Cow::Borrowed("{}"); n];
        let mut scopes = vec![Cow::Borrowed("{}"); n];
        if version == 0x02 {
            for (i, status_description) in status_descriptions.iter_mut().enumerate() {
                *status_description =
                    Cow::Owned(r.str(&format!("status_description {i}"))?.to_owned());
            }
            for (i, event) in events.iter_mut().enumerate() {
                *event = Cow::Owned(
                    otel_json::array(Some(r.str(&format!("events {i}"))?), "events")
                        .map_err(|error| module_err(format!("batch blob: span {i} {error}")))?,
                );
            }
            for (i, resource) in resources.iter_mut().enumerate() {
                *resource = Cow::Owned(
                    otel_json::object(Some(r.str(&format!("resource {i}"))?), "resource")
                        .map_err(|error| module_err(format!("batch blob: span {i} {error}")))?,
                );
            }
            for (i, scope) in scopes.iter_mut().enumerate() {
                *scope = Cow::Owned(
                    otel_json::object(
                        Some(r.str(&format!("instrumentation_scope {i}"))?),
                        "instrumentation_scope",
                    )
                    .map_err(|error| module_err(format!("batch blob: span {i} {error}")))?,
                );
            }
        }

        if r.remaining() != 0 {
            return Err(module_err(format!(
                "batch blob: {} trailing byte(s) (corrupt or wrong n_spans)",
                r.remaining()
            )));
        }

        let mut entries = Vec::with_capacity(n);
        for i in 0..n {
            let parent: [u8; 8] = parent_ids[i * 8..i * 8 + 8].try_into().unwrap();
            let service = otel_json::derive_service(
                attributes[i].as_ref(),
                resources[i].as_ref(),
                Some(std::mem::take(&mut services[i])),
            )
            .map_err(|error| module_err(format!("batch blob: span {i}: {error}")))?;
            entries.push(SpanEntry {
                trace_id: trace_ids[i * 16..i * 16 + 16].try_into().unwrap(),
                span_id: span_ids[i * 8..i * 8 + 8].try_into().unwrap(),
                parent_span_id: (parent != [0u8; 8]).then_some(parent),
                name: std::mem::take(&mut names[i]),
                service,
                kind: kinds[i],
                status: statuses[i],
                status_description: std::mem::take(&mut status_descriptions[i]),
                start_ts: i64::from_le_bytes(start_bytes[i * 8..i * 8 + 8].try_into().unwrap()),
                duration_ns: i64::from_le_bytes(dur_bytes[i * 8..i * 8 + 8].try_into().unwrap()),
                attributes: std::mem::take(&mut attributes[i]),
                events: std::mem::take(&mut events[i]),
                resource: std::mem::take(&mut resources[i]),
                instrumentation_scope: std::mem::take(&mut scopes[i]),
            });
        }
        let count = self.shared.engine.push_batch(entries).map_err(module_err)?;
        Ok(count as i64)
    }

    /// Hidden-column command insert ('flush' | 'optimize' |
    /// 'optimize:<max_spans>' | 'prune:<ts>').
    fn run_command(&self, cmd: &str) -> Result<i64> {
        if cmd == "flush" {
            self.shared.engine.flush().map_err(module_err)?;
        } else if cmd == "optimize" {
            self.shared.engine.optimize().map_err(module_err)?;
        } else if let Some(max_entries) = cmd.strip_prefix("optimize:") {
            let max_entries: usize = max_entries.trim().parse().map_err(|_| {
                module_err(format!(
                    "optimize: expected 'optimize:<max_spans>', got {cmd:?}"
                ))
            })?;
            self.shared
                .engine
                .optimize_budgeted(max_entries)
                .map_err(module_err)?;
        } else if let Some(ts_str) = cmd.strip_prefix("prune:") {
            let ts: i64 = ts_str.trim().parse().map_err(|_| {
                module_err(format!("prune: expected 'prune:<ts>' (ns), got {cmd:?}"))
            })?;
            self.shared.engine.prune(ts).map_err(module_err)?;
        } else {
            return Err(module_err(format!(
                "unknown command {cmd:?}; supported: 'flush', 'optimize', \
                 'optimize:<max_spans>', 'prune:<ts>'"
            )));
        }
        Ok(0)
    }
}

unsafe impl<'vtab> VTab<'vtab> for TracesTab {
    type Aux = ();
    type Cursor = TracesCursor<'vtab>;

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

    /// Pushdown, in priority order:
    ///   1. trace_id equality — THE hero plan (cost ~10): filter() will
    ///      hit the `_trace_blocks` index and decompress only blocks
    ///      containing that trace;
    ///   2. service/kind/status/name equality — posting-list terms;
    ///   3. start_ts range — block ts-overlap pruning.
    ///
    /// idx_num bitmask: 1 trace, 2 service, 4 kind, 8 status, 16 name,
    /// 32 ts lower, 64 ts upper. argv slots are claimed in that
    /// canonical order so filter() decodes positions from the mask.
    ///
    /// omit flags: NOT set for anything except trace_id (SQLite
    /// re-checks the rest above us, so treating strict ts bounds as
    /// inclusive stays safe, same as metrics/logs). trace_id is the
    /// exception BY DESIGN: `WHERE trace_id = 'af3e...'` (hex TEXT —
    /// what OTel tooling hands people to copy-paste) must work, but
    /// our column returns BLOBs and SQLite's own re-check would reject
    /// every row because BLOB = TEXT is never true in SQL. Setting
    /// omit makes OUR equality the authority; that is sound because
    /// filter() applies exact per-span trace-id equality itself
    /// (entry_matches), after parsing the value as packed BLOB or hex
    /// TEXT — anything unparseable yields zero rows, exactly like the
    /// SQL comparison it replaces.
    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        use IndexConstraintOp::*;

        // Pass 1 (immutable): first usable constraint of each kind.
        let mut trace_c: Option<usize> = None;
        let mut svc_c: Option<usize> = None;
        let mut kind_c: Option<usize> = None;
        let mut status_c: Option<usize> = None;
        let mut name_c: Option<usize> = None;
        let mut lo_c: Option<usize> = None;
        let mut hi_c: Option<usize> = None;
        let mut duration_lo_c: Option<usize> = None;
        let mut duration_hi_c: Option<usize> = None;
        let mut limit_c: Option<usize> = None;
        let mut offset_c: Option<usize> = None;
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
            match (c.column() as usize, c.operator()) {
                (_, SQLITE_INDEX_CONSTRAINT_LIMIT) if limit_c.is_none() => limit_c = Some(i),
                (_, SQLITE_INDEX_CONSTRAINT_OFFSET) if offset_c.is_none() => offset_c = Some(i),
                (COL_TRACE_ID, SQLITE_INDEX_CONSTRAINT_EQ) if trace_c.is_none() => {
                    trace_c = Some(i)
                }
                (COL_SERVICE, SQLITE_INDEX_CONSTRAINT_EQ) if svc_c.is_none() => svc_c = Some(i),
                (COL_KIND, SQLITE_INDEX_CONSTRAINT_EQ) if kind_c.is_none() => kind_c = Some(i),
                (COL_STATUS, SQLITE_INDEX_CONSTRAINT_EQ) if status_c.is_none() => {
                    status_c = Some(i)
                }
                (COL_NAME, SQLITE_INDEX_CONSTRAINT_EQ) if name_c.is_none() => name_c = Some(i),
                (COL_START_TS, SQLITE_INDEX_CONSTRAINT_GE) if lo_c.is_none() => lo_c = Some(i),
                (COL_START_TS, SQLITE_INDEX_CONSTRAINT_LE) if hi_c.is_none() => hi_c = Some(i),
                (COL_START_TS, SQLITE_INDEX_CONSTRAINT_GT) if lo_c.is_none() => {
                    lo_c = Some(i);
                    bounded_safe = false;
                }
                (COL_START_TS, SQLITE_INDEX_CONSTRAINT_LT) if hi_c.is_none() => {
                    hi_c = Some(i);
                    bounded_safe = false;
                }
                (COL_DURATION, SQLITE_INDEX_CONSTRAINT_GE) if duration_lo_c.is_none() => {
                    duration_lo_c = Some(i)
                }
                (COL_DURATION, SQLITE_INDEX_CONSTRAINT_LE) if duration_hi_c.is_none() => {
                    duration_hi_c = Some(i)
                }
                (COL_DURATION, SQLITE_INDEX_CONSTRAINT_GT) if duration_lo_c.is_none() => {
                    duration_lo_c = Some(i);
                    bounded_safe = false;
                }
                (COL_DURATION, SQLITE_INDEX_CONSTRAINT_LT) if duration_hi_c.is_none() => {
                    duration_hi_c = Some(i);
                    bounded_safe = false;
                }
                _ => bounded_safe = false,
            }
        }

        let bounded_order = if bounded_safe && limit_c.is_some() {
            let order = info.order_bys().collect::<Vec<_>>();
            match order.as_slice() {
                [first] if first.column() as usize == COL_START_TS => {
                    Some(if first.is_order_by_desc() {
                        SpanQueryOrder::Desc
                    } else {
                        SpanQueryOrder::Asc
                    })
                }
                [first, second]
                    if first.column() as usize == COL_START_TS
                        && second.column() as usize == COL_SPAN_ID
                        && first.is_order_by_desc() == second.is_order_by_desc() =>
                {
                    Some(if first.is_order_by_desc() {
                        SpanQueryOrder::Desc
                    } else {
                        SpanQueryOrder::Asc
                    })
                }
                _ => None,
            }
        } else {
            None
        };

        // Pass 2 (mutable): claim argv slots in canonical order.
        let mut mask: c_int = 0;
        let mut slot: c_int = 1;
        let mut claim = |info: &mut IndexInfo, c: Option<usize>, bit: c_int| {
            if let Some(i) = c {
                info.constraint_usage(i).set_argv_index(slot);
                if bit == BIT_TRACE {
                    // See the omit note in the doc comment above: hex
                    // TEXT lookups only work if SQLite skips its own
                    // BLOB-vs-TEXT re-check and trusts our equality.
                    info.constraint_usage(i).set_omit(true);
                }
                slot += 1;
                mask |= bit;
            }
        };
        claim(info, trace_c, BIT_TRACE);
        claim(info, svc_c, BIT_SERVICE);
        claim(info, kind_c, BIT_KIND);
        claim(info, status_c, BIT_STATUS);
        claim(info, name_c, BIT_NAME);
        claim(info, lo_c, BIT_TS_LO);
        claim(info, hi_c, BIT_TS_HI);
        claim(info, duration_lo_c, BIT_DURATION_LO);
        claim(info, duration_hi_c, BIT_DURATION_HI);
        if bounded_order.is_some() {
            claim(info, limit_c, 0);
            claim(info, offset_c, 0);
        }

        info.set_idx_num(mask);
        if let Some(order) = bounded_order {
            info.set_idx_str(match (order, offset_c.is_some()) {
                (SpanQueryOrder::Asc, false) => PLAN_BOUNDED_TS_ASC,
                (SpanQueryOrder::Asc, true) => PLAN_BOUNDED_TS_ASC_OFFSET,
                (SpanQueryOrder::Desc, false) => PLAN_BOUNDED_TS_DESC,
                (SpanQueryOrder::Desc, true) => PLAN_BOUNDED_TS_DESC_OFFSET,
            });
            info.set_order_by_consumed(true);
            info.set_estimated_rows(100);
        }
        // Cost ladder steers the planner: a trace_id lookup is a
        // point probe of the trace index (the entire reason this vtab
        // exists) and must win against any other join order SQLite
        // considers; term/range plans prune blocks; a bare scan
        // decompresses everything.
        let pruning_mask =
            BIT_TRACE | BIT_SERVICE | BIT_KIND | BIT_STATUS | BIT_NAME | BIT_TS_LO | BIT_TS_HI;
        info.set_estimated_cost(if mask & BIT_TRACE != 0 {
            10.0
        } else if mask & pruning_mask != 0 {
            1e3
        } else {
            1e6
        });
        Ok(true)
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        Ok(TracesCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            shared: Arc::clone(&self.shared),
            db: self.db,
            table_name: self.table_name.clone(),
            rows: Vec::new(),
            pos: 0,
            stream: None,
            current: None,
            phantom: PhantomData,
        })
    }
}

/// Defensive gate release on teardown — see metrics_vtab.rs.
impl Drop for TracesTab {
    fn drop(&mut self) {
        self.release_write_gate();
    }
}

impl CreateVTab<'_> for TracesTab {
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
        host.execute_batch(&shadow_span_store::drop_ddl(
            &self.database_name,
            &self.table_name,
        ))
    }
}

impl UpdateVTab<'_> for TracesTab {
    /// INSERT. argv: [0] NULL, [1] requested rowid, then declared
    /// columns from index 2 (COL_* + 2); the hidden command column is
    /// argv[16].
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        // Connection routing + writer gate, as in metrics_vtab.rs
        // (gate is normally taken by begin(); this is the defensive
        // re-check). push() auto-flushes, so inserts can write rows.
        let _bind = DbGuard::bind(self.db);
        self.acquire_write_gate()?;

        let cmd_idx = 2 + COL_COMMAND;
        // Command idiom, dispatched by TYPE like metrics/logs: TEXT =
        // command, BLOB reserved for a future Tier 2 batch, NULL = data.
        match args.iter().nth(cmd_idx) {
            Some(ValueRef::Null) | None => {} // plain data row
            Some(ValueRef::Blob(blob)) => {
                // Dispatch by version byte. v0 stays readable forever;
                // v1 carries the rich span shape.
                return match blob.first() {
                    Some(0x01 | 0x02) => self.ingest_batch(blob),
                    Some(b @ (0x00 | 0x03..=0x08)) => Err(module_err(format!(
                        "unknown batch version 0x{b:02x} (this build speaks v0 = 0x01 and v1 = 0x02)"
                    ))),
                    Some(b) => Err(module_err(format!(
                        "unknown blob format (first byte 0x{b:02x}; traces batches start with 0x01/0x02)"
                    ))),
                    None => Err(module_err("empty blob".into())),
                };
            }
            Some(_) => {
                let cmd: String = args.get(cmd_idx)?;
                return self.run_command(&cmd);
            }
        }

        // Collect the column ValueRefs once (ids need TYPE dispatch,
        // not just FromSql conversion).
        let vals: Vec<ValueRef<'_>> = args.iter().collect();
        let col = |c: usize| vals[2 + c];

        // Required ids: packed BLOB or hex TEXT (see module header).
        let v = col(COL_TRACE_ID);
        if matches!(v, ValueRef::Null) {
            return Err(module_err(
                "trace_id is required (16-byte BLOB or 32-char hex TEXT)".into(),
            ));
        }
        let trace_id = parse_id::<16>(v, "trace_id")?;
        let v = col(COL_SPAN_ID);
        if matches!(v, ValueRef::Null) {
            return Err(module_err(
                "span_id is required (8-byte BLOB or 16-char hex TEXT)".into(),
            ));
        }
        let span_id = parse_id::<8>(v, "span_id")?;
        // parent is optional (NULL = root span).
        let parent_span_id = match col(COL_PARENT) {
            ValueRef::Null => None,
            v => Some(parse_id::<8>(v, "parent_span_id")?),
        };

        let name: Option<String> = args.get(2 + COL_NAME)?;
        let Some(name) = name else {
            return Err(module_err("name is required (TEXT)".into()));
        };
        let explicit_service: Option<String> = args.get(2 + COL_SERVICE)?;

        // kind/status: strict vocabularies; NULL takes the OTel default
        // (kind=internal, status=unset) — the one place we default
        // rather than reject, because the defaults ARE part of the
        // OTel data model, not guesses.
        let kind_txt: Option<String> = args.get(2 + COL_KIND)?;
        let kind = match kind_txt {
            Some(k) => kind_from_name(&k).map_err(module_err)?,
            None => 0, // internal
        };
        let status_txt: Option<String> = args.get(2 + COL_STATUS)?;
        let status = match status_txt {
            Some(s) => status_from_name(&s).map_err(module_err)?,
            None => 0, // unset
        };

        let start_ts: Option<i64> = args.get(2 + COL_START_TS)?;
        let Some(start_ts) = start_ts else {
            return Err(module_err("start_ts is required (INTEGER, unix ns)".into()));
        };
        // duration defaults to 0 (a point event; OTel allows it).
        let duration_ns: Option<i64> = args.get(2 + COL_DURATION)?;
        let duration_ns = duration_ns.unwrap_or(0);

        // Rich OTel fields are public typed JSON, never flattened into
        // metrics/logs-style string pairs.
        let attrs_json: Option<String> = args.get(2 + COL_ATTRS)?;
        let attributes = match attrs_json.as_deref() {
            Some(text) => {
                Cow::Owned(otel_json::object(Some(text), "attributes").map_err(module_err)?)
            }
            None => Cow::Borrowed("{}"),
        };
        let status_description: Option<String> = args.get(2 + COL_STATUS_DESCRIPTION)?;
        let status_description = status_description
            .map(Cow::Owned)
            .unwrap_or(Cow::Borrowed(""));
        let events_json: Option<String> = args.get(2 + COL_EVENTS)?;
        let events = match events_json.as_deref() {
            Some(text) => Cow::Owned(otel_json::array(Some(text), "events").map_err(module_err)?),
            None => Cow::Borrowed("[]"),
        };
        let resource_json: Option<String> = args.get(2 + COL_RESOURCE)?;
        let resource = match resource_json.as_deref() {
            Some(text) => {
                Cow::Owned(otel_json::object(Some(text), "resource").map_err(module_err)?)
            }
            None => Cow::Borrowed("{}"),
        };
        let scope_json: Option<String> = args.get(2 + COL_SCOPE)?;
        let instrumentation_scope = match scope_json.as_deref() {
            Some(text) => Cow::Owned(
                otel_json::object(Some(text), "instrumentation_scope").map_err(module_err)?,
            ),
            None => Cow::Borrowed("{}"),
        };
        let service =
            otel_json::derive_service(attributes.as_ref(), resource.as_ref(), explicit_service)
                .map_err(module_err)?;

        // Rich JSON is canonical now; push() validates compact enums
        // and auto-flushes at the threshold.
        self.shared
            .engine
            .push(SpanEntry {
                trace_id,
                span_id,
                parent_span_id,
                name,
                service,
                kind,
                status,
                status_description,
                start_ts,
                duration_ns,
                attributes,
                events,
                resource,
                instrumentation_scope,
            })
            .map_err(module_err)?;

        // Synthetic rowid, same as metrics/logs: spans live in blocks,
        // not addressable rows.
        self.rowid_counter += 1;
        Ok(self.rowid_counter)
    }

    fn delete(&mut self, _arg: ValueRef<'_>) -> Result<()> {
        Err(module_err(
            "timeless_traces is append-only; DELETE is not supported \
             (use INSERT INTO t(t) VALUES('prune:<ts>') for retention)"
                .into(),
        ))
    }

    fn update(&mut self, _args: &Updates<'_>) -> Result<()> {
        Err(module_err(
            "timeless_traces is append-only; UPDATE is not supported".into(),
        ))
    }
}

/// Real transaction semantics (PLAN.md R5 — FIXED), same shape as
/// metrics/logs (read metrics_vtab.rs for the full comment): xBegin
/// activates the SpanBlockEngine's journal, xCommit drops it,
/// xRollback undoes engine memory to mirror the host rollback of
/// `_blocks`/`_terms`/`_trace_blocks` (the trace-index rows ride the
/// same host transaction, so they vanish and reappear with their
/// blocks — never-dangle holds through rollback too). Auto-flush
/// inside a transaction is fully covered, as are all commands.
/// vtab_tx.rs supplies the savepoint callbacks missing from rusqlite.
/// R4 ADDITION — writer gate brackets the journal exactly as in
/// metrics_vtab.rs (read the comment there): acquire before
/// txn_begin, holder-only commit/rollback, release after.
impl TransactionVTab<'_> for TracesTab {
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

impl SavepointVTab for TracesTab {
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

#[repr(C)]
pub struct TracesCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    shared: Arc<SharedEngine<SpanBlockEngine>>,
    /// The connection driving this scan (bound in filter()).
    db: *mut ffi::sqlite3,
    table_name: String,
    /// Ordered LIMIT/OFFSET plans retain only their bounded prefix.
    rows: Vec<SpanEntry>,
    pos: usize,
    /// Unbounded plans decode one block at a time. `current` is the sole row
    /// exposed to SQLite; stopping a SELECT early cannot leave a database-
    /// sized result vector behind the extension boundary.
    stream: Option<SpanQueryStream>,
    current: Option<SpanEntry>,
    phantom: PhantomData<&'vtab TracesTab>,
}

impl Drop for TracesCursor<'_> {
    fn drop(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            self.shared.engine.finish_query_stream(&mut stream);
        }
    }
}

unsafe impl VTabCursor for TracesCursor<'_> {
    /// Decode the pushed constraints per the best_index bitmask, run
    /// one engine query (sequential block reads — no rayon anywhere on
    /// this path, per the Session 3 deadlock lesson). Bounded plans retain a
    /// LIMIT+OFFSET prefix; unbounded plans prime a one-block stream.
    fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        // Route block reads to the connection running this SELECT.
        let _bind = DbGuard::bind(self.db);
        if let Some(mut old) = self.stream.take() {
            self.shared.engine.finish_query_stream(&mut old);
        }
        self.rows.clear();
        self.current = None;
        self.pos = 0;

        // argv slots were claimed in canonical order (trace, service,
        // kind, status, name, ts lo, ts hi) — the mask alone tells us
        // which positional arg is which.
        let mut arg = 0usize;
        let mut next = || {
            let i = arg;
            arg += 1;
            i
        };

        // Any constraint value that can't possibly match (bad hex, a
        // NULL, an unknown kind name) yields an EMPTY result, not an
        // error — `WHERE status='oops'` is a valid query that selects
        // zero rows, same convention as the logs vtab.
        let mut impossible = false;

        // trace_id: pushed as whatever the user wrote — BLOB literal
        // (x'...') or hex TEXT both work here, because WE parse the
        // value; only the returned column is always BLOB.
        let trace_id: Option<[u8; 16]> = if idx_num & BIT_TRACE != 0 {
            let v: Value = args.get(next())?;
            let parsed = match &v {
                Value::Blob(b) => <[u8; 16]>::try_from(b.as_slice()).ok(),
                Value::Text(s) => hex_to_bytes::<16>(s),
                _ => None,
            };
            if parsed.is_none() {
                impossible = true;
            }
            parsed
        } else {
            None
        };
        let service: Option<String> = if idx_num & BIT_SERVICE != 0 {
            let v: Option<String> = args.get(next())?;
            if v.is_none() {
                impossible = true;
            }
            v
        } else {
            None
        };
        let kind: Option<u8> = if idx_num & BIT_KIND != 0 {
            let v: Option<String> = args.get(next())?;
            match v.as_deref().map(kind_from_name) {
                Some(Ok(k)) => Some(k),
                _ => {
                    impossible = true;
                    None
                }
            }
        } else {
            None
        };
        let status: Option<u8> = if idx_num & BIT_STATUS != 0 {
            let v: Option<String> = args.get(next())?;
            match v.as_deref().map(status_from_name) {
                Some(Ok(s)) => Some(s),
                _ => {
                    impossible = true;
                    None
                }
            }
        } else {
            None
        };
        let name: Option<String> = if idx_num & BIT_NAME != 0 {
            let v: Option<String> = args.get(next())?;
            if v.is_none() {
                impossible = true;
            }
            v
        } else {
            None
        };
        let ts_min: i64 = if idx_num & BIT_TS_LO != 0 {
            match args.get::<Option<i64>>(next())? {
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
            match args.get::<Option<i64>>(next())? {
                Some(v) => v,
                None => {
                    impossible = true;
                    i64::MAX
                }
            }
        } else {
            i64::MAX
        };
        let duration_min: i64 = if idx_num & BIT_DURATION_LO != 0 {
            match args.get::<Option<i64>>(next())? {
                Some(value) => value,
                None => {
                    impossible = true;
                    i64::MIN
                }
            }
        } else {
            i64::MIN
        };
        let duration_max: i64 = if idx_num & BIT_DURATION_HI != 0 {
            match args.get::<Option<i64>>(next())? {
                Some(value) => value,
                None => {
                    impossible = true;
                    i64::MAX
                }
            }
        } else {
            i64::MAX
        };

        let bounded_order = match idx_str {
            Some(PLAN_BOUNDED_TS_ASC) => Some((SpanQueryOrder::Asc, false)),
            Some(PLAN_BOUNDED_TS_ASC_OFFSET) => Some((SpanQueryOrder::Asc, true)),
            Some(PLAN_BOUNDED_TS_DESC) => Some((SpanQueryOrder::Desc, false)),
            Some(PLAN_BOUNDED_TS_DESC_OFFSET) => Some((SpanQueryOrder::Desc, true)),
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
                (Some(limit), Some(offset)) if limit > 0 => limit
                    .checked_add(offset.max(0))
                    .and_then(|value| usize::try_from(value).ok()),
                _ => None,
            };
            Some((order, capacity))
        } else {
            None
        };

        if impossible {
            return Ok(());
        }
        let query = SpanQuery {
            ts_min,
            ts_max,
            trace_id,
            service,
            kind,
            status,
            name,
        };
        let read = self
            .shared
            .write_gate
            .acquire_read(self.db as usize, &self.table_name)
            .map_err(module_err)?;
        match bounded {
            Some((order, capacity)) => {
                self.rows = self
                    .shared
                    .engine
                    .query_ordered_with_duration_after_snapshot(
                        &query,
                        duration_min,
                        duration_max,
                        order,
                        capacity,
                        move || drop(read),
                    )
                    .map_err(module_err)?;
            }
            None => {
                let mut stream = self
                    .shared
                    .engine
                    .query_stream_with_duration_after_snapshot(
                        &query,
                        duration_min,
                        duration_max,
                        move || drop(read),
                    )
                    .map_err(module_err)?;
                self.current = match self.shared.engine.query_stream_next(&mut stream) {
                    Ok(row) => row,
                    Err(error) => {
                        self.shared.engine.finish_query_stream(&mut stream);
                        return Err(module_err(error));
                    }
                };
                self.stream = Some(stream);
            }
        }
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        let _bind = DbGuard::bind(self.db);
        self.pos += 1;
        if let Some(stream) = self.stream.as_mut() {
            self.current = self
                .shared
                .engine
                .query_stream_next(stream)
                .map_err(module_err)?;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        if self.stream.is_some() {
            self.current.is_none()
        } else {
            self.pos >= self.rows.len()
        }
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        let row = self
            .current
            .as_ref()
            .unwrap_or_else(|| &self.rows[self.pos]);
        match i as usize {
            // Ids come back as BLOBs, always (hex() in SQL to display).
            COL_TRACE_ID => ctx.set_result(&&row.trace_id[..]),
            COL_SPAN_ID => ctx.set_result(&&row.span_id[..]),
            COL_PARENT => match &row.parent_span_id {
                Some(p) => ctx.set_result(&&p[..]),
                None => ctx.set_result(&Null),
            },
            COL_NAME => ctx.set_result(&row.name),
            COL_SERVICE => ctx.set_result(&row.service),
            COL_KIND => ctx.set_result(&kind_name(row.kind)),
            COL_STATUS => ctx.set_result(&status_name(row.status)),
            COL_START_TS => ctx.set_result(&row.start_ts),
            COL_DURATION => ctx.set_result(&row.duration_ns),
            COL_ATTRS => ctx.set_result(&row.attributes.as_ref()),
            COL_STATUS_DESCRIPTION => ctx.set_result(&row.status_description.as_ref()),
            COL_EVENTS => ctx.set_result(&row.events.as_ref()),
            COL_RESOURCE => ctx.set_result(&row.resource.as_ref()),
            COL_SCOPE => ctx.set_result(&row.instrumentation_scope.as_ref()),
            // The hidden command column reads as NULL.
            _ => ctx.set_result(&Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}
