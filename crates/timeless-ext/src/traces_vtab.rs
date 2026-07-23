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
//!                  attributes TEXT, "<table>" HIDDEN)
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
//!              | 'prune:<ts>') — the FTS5 idiom, ts in ns.
//! Read path:   flushed blocks + in-memory buffer merged; the HERO
//!              query `WHERE trace_id = x'...'` goes through the
//!              `_trace_blocks` index and decompresses only blocks
//!              containing that trace.
//! Append-only: DELETE/UPDATE rejected; retention is 'prune:<ts>'.

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
    SpanEngineConfig, SpanEntry, SpanQuery,
};

use crate::flatjson::{pairs_to_json, parse_labels_json};
use crate::shadow_span_store::{self, ShadowSpanStore};

/// Register the "timeless_traces" module on a freshly-loaded connection.
pub(crate) fn register(db: &Connection) -> Result<()> {
    const MODULE: Module<TracesTab> = Module::update_module_with_tx();
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
/// The hidden command column (named after the table, FTS5 idiom).
const COL_COMMAND: usize = 10;

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
    engine: Arc<SpanBlockEngine>,
    rowid_counter: i64,
}

impl TracesTab {
    fn connect_create(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        table_name: &[u8],
        args: &[&[u8]],
        is_create: bool,
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let table = String::from_utf8_lossy(table_name).into_owned();
        let handle = unsafe { db.handle() };

        // No creation args: unlike logs there is no index_keys knob
        // (spans/mod.rs explains why the four span dimensions are
        // indexed unconditionally). Reject anything passed so a typo'd
        // arg fails loudly instead of silently doing nothing.
        if !args.is_empty() {
            return Err(module_err(format!(
                "timeless_traces takes no arguments; got {:?}",
                args.iter()
                    .map(|a| String::from_utf8_lossy(a).into_owned())
                    .collect::<Vec<_>>()
            )));
        }

        let store = ShadowSpanStore::new(handle, &table);
        if is_create {
            let host = unsafe { Connection::from_handle(handle) }?;
            // Same incremental auto-vacuum attempt as metrics/logs
            // (no-op on a non-empty db; see metrics_vtab.rs).
            let _ = host.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;");
            host.execute_batch(&shadow_span_store::ddl(&table))?;
            // PLAN.md: the shared block code never assumes a ts unit —
            // record OURS in _meta so tooling (and future readers of
            // this db) know these blocks speak nanoseconds.
            store.save_meta("ts_unit", b"ns").map_err(module_err)?;
        }

        // SpanBlockEngine::new recovers the block index via scan() and
        // status partitions via the `status:` posting lists — re-entrant
        // SELECTs, safe because THIS thread holds the connection.
        let engine = SpanBlockEngine::new(
            Box::new(store),
            SpanEngineConfig {
                flush_threshold: FLUSH_THRESHOLD,
                zstd_level: ZSTD_LEVEL,
                merge_target_entries: MERGE_TARGET_ENTRIES,
                merge_max_ts_span: MERGE_MAX_TS_SPAN,
            },
        )
        .map_err(module_err)?;

        let schema = format!(
            "CREATE TABLE x(trace_id BLOB, span_id BLOB, parent_span_id BLOB, \
             name TEXT, service TEXT, kind TEXT, status TEXT, \
             start_ts INTEGER, duration_ns INTEGER, attributes TEXT, \
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
                engine: Arc::new(engine),
                rowid_counter: 0,
            },
        ))
    }

    /// Hidden-column command insert ('flush' | 'optimize' | 'prune:<ts>').
    fn run_command(&self, cmd: &str) -> Result<i64> {
        if cmd == "flush" {
            self.engine.flush().map_err(module_err)?;
        } else if cmd == "optimize" {
            self.engine.optimize().map_err(module_err)?;
        } else if let Some(ts_str) = cmd.strip_prefix("prune:") {
            let ts: i64 = ts_str.trim().parse().map_err(|_| {
                module_err(format!("prune: expected 'prune:<ts>' (ns), got {cmd:?}"))
            })?;
            self.engine.prune(ts).map_err(module_err)?;
        } else {
            return Err(module_err(format!(
                "unknown command {cmd:?}; supported: 'flush', 'optimize', 'prune:<ts>'"
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
        for (i, c) in info.constraints().enumerate() {
            if !c.is_usable() {
                continue;
            }
            match (c.column() as usize, c.operator()) {
                (COL_TRACE_ID, SQLITE_INDEX_CONSTRAINT_EQ) if trace_c.is_none() => {
                    trace_c = Some(i)
                }
                (COL_SERVICE, SQLITE_INDEX_CONSTRAINT_EQ) if svc_c.is_none() => svc_c = Some(i),
                (COL_KIND, SQLITE_INDEX_CONSTRAINT_EQ) if kind_c.is_none() => kind_c = Some(i),
                (COL_STATUS, SQLITE_INDEX_CONSTRAINT_EQ) if status_c.is_none() => {
                    status_c = Some(i)
                }
                (COL_NAME, SQLITE_INDEX_CONSTRAINT_EQ) if name_c.is_none() => name_c = Some(i),
                (COL_START_TS, SQLITE_INDEX_CONSTRAINT_GE)
                | (COL_START_TS, SQLITE_INDEX_CONSTRAINT_GT)
                    if lo_c.is_none() =>
                {
                    lo_c = Some(i)
                }
                (COL_START_TS, SQLITE_INDEX_CONSTRAINT_LE)
                | (COL_START_TS, SQLITE_INDEX_CONSTRAINT_LT)
                    if hi_c.is_none() =>
                {
                    hi_c = Some(i)
                }
                _ => {}
            }
        }

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

        info.set_idx_num(mask);
        // Cost ladder steers the planner: a trace_id lookup is a
        // point probe of the trace index (the entire reason this vtab
        // exists) and must win against any other join order SQLite
        // considers; term/range plans prune blocks; a bare scan
        // decompresses everything.
        info.set_estimated_cost(if mask & BIT_TRACE != 0 {
            10.0
        } else if mask != 0 {
            1e3
        } else {
            1e6
        });
        Ok(true)
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        Ok(TracesCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            engine: Arc::clone(&self.engine),
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
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
        let host = unsafe { Connection::from_handle(self.db) }?;
        host.execute_batch(&shadow_span_store::drop_ddl(&self.table_name))
    }
}

impl UpdateVTab<'_> for TracesTab {
    /// INSERT. argv: [0] NULL, [1] requested rowid, then declared
    /// columns from index 2 (COL_* + 2); the hidden command column is
    /// argv[12].
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        let cmd_idx = 2 + COL_COMMAND;
        // Command idiom, dispatched by TYPE like metrics/logs: TEXT =
        // command, BLOB reserved for a future Tier 2 batch, NULL = data.
        match args.iter().nth(cmd_idx) {
            Some(ValueRef::Null) | None => {} // plain data row
            Some(ValueRef::Blob(_)) => {
                return Err(module_err(
                    "timeless_traces batch-blob ingest is not implemented yet \
                     (Tier 2 for traces is future work; use row INSERTs)"
                        .into(),
                ));
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
        let service: Option<String> = args.get(2 + COL_SERVICE)?;
        let Some(service) = service else {
            return Err(module_err("service is required (TEXT)".into()));
        };

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

        // attributes: optional flat JSON object of string values (same
        // parser as metrics labels and logs metadata — the three tables
        // agree on the format by construction).
        let attrs_json: Option<String> = args.get(2 + COL_ATTRS)?;
        let attributes: Vec<(String, String)> = match attrs_json {
            Some(txt) => parse_labels_json(&txt)
                .map_err(module_err)?
                .into_iter()
                .collect(),
            None => Vec::new(),
        };

        // push() canonicalizes (sorts) attributes, validates, and
        // auto-flushes at the threshold.
        self.engine
            .push(SpanEntry {
                trace_id,
                span_id,
                parent_span_id,
                name,
                service,
                kind,
                status,
                start_ts,
                duration_ns,
                attributes,
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
/// inside a transaction is fully covered, as are all commands. Same
/// savepoint limitation as the others (xSavepoint not wired).
impl TransactionVTab<'_> for TracesTab {
    fn begin(&mut self) -> Result<()> {
        self.engine.txn_begin();
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.engine.txn_commit();
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        self.engine.txn_rollback();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The cursor
// ---------------------------------------------------------------------------

/// One output row, materialized at filter() time: the decoded span plus
/// its attributes pre-rendered to canonical sorted flat JSON.
struct OutRow {
    entry: SpanEntry,
    attributes_json: String,
}

#[repr(C)]
pub struct TracesCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    engine: Arc<SpanBlockEngine>,
    rows: Vec<OutRow>,
    pos: usize,
    phantom: PhantomData<&'vtab TracesTab>,
}

unsafe impl VTabCursor for TracesCursor<'_> {
    /// Decode the pushed constraints per the best_index bitmask, run
    /// one engine query (sequential block reads — no rayon anywhere on
    /// this path, per the Session 3 deadlock lesson), materialize rows.
    fn filter(
        &mut self,
        idx_num: c_int,
        _idx_str: Option<&str>,
        args: &Filters<'_>,
    ) -> Result<()> {
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
                    i64::MIN + 1
                }
            }
        } else {
            i64::MIN + 1
        };
        let ts_max: i64 = if idx_num & BIT_TS_HI != 0 {
            match args.get::<Option<i64>>(next())? {
                Some(v) => v,
                None => {
                    impossible = true;
                    i64::MAX - 1
                }
            }
        } else {
            i64::MAX - 1
        };

        let entries = if impossible {
            Vec::new()
        } else {
            self.engine
                .query(&SpanQuery {
                    ts_min,
                    ts_max,
                    trace_id,
                    service,
                    kind,
                    status,
                    name,
                })
                .map_err(module_err)?
        };

        self.rows = entries
            .into_iter()
            .map(|entry| OutRow {
                attributes_json: pairs_to_json(&entry.attributes),
                entry,
            })
            .collect();
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
        match i as usize {
            // Ids come back as BLOBs, always (hex() in SQL to display).
            COL_TRACE_ID => ctx.set_result(&&row.entry.trace_id[..]),
            COL_SPAN_ID => ctx.set_result(&&row.entry.span_id[..]),
            COL_PARENT => match &row.entry.parent_span_id {
                Some(p) => ctx.set_result(&&p[..]),
                None => ctx.set_result(&Null),
            },
            COL_NAME => ctx.set_result(&row.entry.name),
            COL_SERVICE => ctx.set_result(&row.entry.service),
            COL_KIND => ctx.set_result(&kind_name(row.entry.kind)),
            COL_STATUS => ctx.set_result(&status_name(row.entry.status)),
            COL_START_TS => ctx.set_result(&row.entry.start_ts),
            COL_DURATION => ctx.set_result(&row.entry.duration_ns),
            COL_ATTRS => ctx.set_result(&row.attributes_json),
            // The hidden command column reads as NULL.
            _ => ctx.set_result(&Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}
