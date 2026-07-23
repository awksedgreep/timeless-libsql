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
//!                  "status" TEXT HIDDEN, "<table>" HIDDEN)
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
//!              'prune:<ts>') — the same FTS5 idiom as metrics.
//! Read path:   flushed blocks and the in-memory buffer are merged, so
//!              entries are queryable immediately after INSERT and
//!              durable (as durable as the enclosing transaction) after
//!              'flush'.
//! Append-only: DELETE/UPDATE rejected; retention is 'prune:<ts>'.

use std::borrow::Cow;
use std::ffi::{c_int, CStr, CString};
use std::marker::PhantomData;
use std::sync::Arc;

use rusqlite::ffi;
use rusqlite::types::{Null, ValueRef};
use rusqlite::vtab::{
    escape_double_quote, Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, Inserts,
    Module, TransactionVTab, UpdateVTab, Updates, VTab, VTabConnection, VTabCursor, VTabKind,
};
use rusqlite::{Connection, Error, Result};
use timeless_core::{
    level_from_name, level_name, BlockEngine, BlockEngineConfig, BlockStore, LogEntry, LogQuery,
};

use crate::flatjson::{pairs_to_json, parse_labels_json};
use crate::shadow_block_store::{self, ShadowBlockStore};

/// Register the "timeless_logs" module on a freshly-loaded connection.
pub(crate) fn register(db: &Connection) -> Result<()> {
    const MODULE: Module<LogsTab> = Module::update_module_with_tx();
    db.create_module(c"timeless_logs", &MODULE, None::<()>)
}

/// Engine parameters (see BlockEngineConfig for what each knob means).
const FLUSH_THRESHOLD: usize = 8192; // buffered entries before auto-flush
const ZSTD_LEVEL: i32 = 7;
const MERGE_TARGET_ENTRIES: usize = 8192;
/// HARD CAP on merged-block ts span: 1 hour in MILLISECONDS (this vtab
/// documents ts as unix millis). PLAN.md "Pruning & retention": merge
/// compaction must never produce blocks straddling retention
/// boundaries, or expired entries stay pinned until the whole merged
/// block ages out. 1h granules keep 'prune:<ts>' effective at typical
/// (hours-to-days) log retention windows.
const MERGE_MAX_TS_SPAN: i64 = 3_600_000;

/// best_index bitmask layout (fixed bits first, then one bit per index
/// key). c_int gives 31 usable bits → 3 fixed + up to 28 index keys.
const BIT_LEVEL: c_int = 1;
const BIT_TS_LO: c_int = 2;
const BIT_TS_HI: c_int = 4;
const FIRST_KEY_BIT_SHIFT: usize = 3;
const MAX_INDEX_KEYS: usize = 28;

/// Number of fixed (non-hidden) columns before the index-key columns:
/// 0=ts 1=level 2=message 3=metadata.
const FIXED_COLS: usize = 4;

fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
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
    /// The allowlist of indexed metadata keys, in declared-column order
    /// (position k ↔ column FIXED_COLS+k ↔ bitmask bit 8<<k).
    index_keys: Vec<String>,
    engine: Arc<BlockEngine>,
    rowid_counter: i64,
}

impl LogsTab {
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

        let store = ShadowBlockStore::new(handle, &table);

        let index_keys = if is_create {
            let host = unsafe { Connection::from_handle(handle) }?;
            // Same incremental auto-vacuum attempt as metrics (no-op on
            // a non-empty db; see metrics_vtab.rs for the rationale).
            let _ = host.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;");
            host.execute_batch(&shadow_block_store::ddl(&table))?;

            // index_keys comes from the CREATE args and is PERSISTED in
            // _meta: the key set is baked into the terms already written
            // to `_terms`, so it is a property of the DATA, not of
            // whoever reconnects. xConnect reads it back from _meta and
            // never trusts (or receives) fresh args.
            let keys = parse_index_keys_args(&table, args).map_err(module_err)?;
            store
                .save_meta("index_keys", keys.join(",").as_bytes())
                .map_err(module_err)?;
            keys
        } else {
            match store.load_meta("index_keys").map_err(module_err)? {
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
                // writes it). Fall back to the args SQLite replays.
                None => parse_index_keys_args(&table, args).map_err(module_err)?,
            }
        };

        // BlockEngine::new recovers the block index via store.scan() —
        // a re-entrant SELECT, safe because THIS thread already holds
        // the connection mutex (recursively).
        let engine = BlockEngine::new(
            Box::new(store),
            BlockEngineConfig {
                flush_threshold: FLUSH_THRESHOLD,
                zstd_level: ZSTD_LEVEL,
                merge_target_entries: MERGE_TARGET_ENTRIES,
                merge_max_ts_span: MERGE_MAX_TS_SPAN,
                index_keys: index_keys.clone(),
            },
        )
        .map_err(module_err)?;

        // Declared schema, built at runtime: fixed columns + one HIDDEN
        // TEXT column per index key + the hidden command column named
        // after the table (FTS5 idiom).
        let mut schema = String::from("CREATE TABLE x(ts INTEGER, level TEXT, message TEXT, metadata TEXT");
        for key in &index_keys {
            schema.push_str(&format!(", \"{}\" TEXT HIDDEN", escape_double_quote(key)));
        }
        schema.push_str(&format!(", \"{}\" HIDDEN)", escape_double_quote(&table)));
        let schema = CString::new(schema)
            .map_err(|_| module_err(format!("table/key name contains NUL: {table:?}")))?;

        Ok((
            Cow::Owned(schema),
            LogsTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
                table_name: table,
                index_keys,
                engine: Arc::new(engine),
                rowid_counter: 0,
            },
        ))
    }

    /// Hidden-column command insert ('flush' | 'optimize' | 'prune:<ts>').
    fn run_command(&self, cmd: &str) -> Result<i64> {
        if cmd == "flush" {
            // Drain the buffer into one RAW block (+ terms). Durable as
            // soon as the enclosing SQLite transaction commits.
            self.engine.flush().map_err(module_err)?;
        } else if cmd == "optimize" {
            // Two-tier compaction: raw → zstd-columnar, plus merge of
            // small compressed blocks (span-capped) — one atomic swap.
            self.engine.optimize().map_err(module_err)?;
        } else if let Some(ts_str) = cmd.strip_prefix("prune:") {
            // Retention: whole-block deletes by ts_max, term rows
            // removed in the same operation.
            let ts: i64 = ts_str.trim().parse().map_err(|_| {
                module_err(format!("prune: expected 'prune:<ts>', got {cmd:?}"))
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

/// Parse `index_keys='a,b,c'` from the CREATE VIRTUAL TABLE args.
/// No args (or an empty list) is allowed: level + ts + message-scan
/// queries still work, there are just no metadata posting lists.
fn parse_index_keys_args(table: &str, args: &[&[u8]]) -> std::result::Result<Vec<String>, String> {
    let mut keys: Vec<String> = Vec::new();
    for raw in args {
        let arg = String::from_utf8_lossy(raw);
        let arg = arg.trim();
        let Some((name, value)) = arg.split_once('=') else {
            return Err(format!(
                "unrecognized argument {arg:?}; expected index_keys='k1,k2,...'"
            ));
        };
        if name.trim() != "index_keys" {
            return Err(format!(
                "unrecognized argument {:?}; the only supported argument is index_keys",
                name.trim()
            ));
        }
        // Accept 'a,b', "a,b", or bare a,b.
        let value = value.trim();
        let value = value
            .strip_prefix('\'')
            .and_then(|v| v.strip_suffix('\''))
            .or_else(|| value.strip_prefix('"').and_then(|v| v.strip_suffix('"')))
            .unwrap_or(value);
        for k in value.split(',') {
            let k = k.trim();
            if k.is_empty() {
                continue; // index_keys='' means "none"
            }
            // Each key becomes a declared column name: reject collisions
            // with the fixed columns and the hidden command column now,
            // with a message better than SQLite's "duplicate column".
            if ["ts", "level", "message", "metadata"].contains(&k) || k == table {
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
    type Aux = ();
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
    /// `message LIKE '%...%'` is deliberately LEFT TO SQLITE: the vtab
    /// returns candidate rows (already block-pruned by whatever other
    /// constraints exist) and SQLite applies the LIKE above us. An
    /// in-module substring scan (never materializing non-matching rows)
    /// is a later optimization — correctness is identical, this just
    /// materializes more rows than strictly necessary.
    ///
    /// As in metrics: no `omit` flags set, so SQLite re-checks every
    /// constraint — treating strict bounds as inclusive stays safe.
    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        use IndexConstraintOp::*;

        // Pass 1 (immutable): find the first usable constraint of each
        // kind. Columns: 0 ts, 1 level, 2 message, 3 metadata, then
        // FIXED_COLS + k for index key k.
        let mut level_c: Option<usize> = None;
        let mut lo_c: Option<usize> = None;
        let mut hi_c: Option<usize> = None;
        let mut key_c: Vec<Option<usize>> = vec![None; self.index_keys.len()];
        for (i, c) in info.constraints().enumerate() {
            if !c.is_usable() {
                continue;
            }
            match (c.column(), c.operator()) {
                (1, SQLITE_INDEX_CONSTRAINT_EQ) if level_c.is_none() => level_c = Some(i),
                (0, SQLITE_INDEX_CONSTRAINT_GE) | (0, SQLITE_INDEX_CONSTRAINT_GT)
                    if lo_c.is_none() =>
                {
                    lo_c = Some(i)
                }
                (0, SQLITE_INDEX_CONSTRAINT_LE) | (0, SQLITE_INDEX_CONSTRAINT_LT)
                    if hi_c.is_none() =>
                {
                    hi_c = Some(i)
                }
                (col, SQLITE_INDEX_CONSTRAINT_EQ) => {
                    let col = col as usize;
                    if col >= FIXED_COLS && col < FIXED_COLS + self.index_keys.len() {
                        let k = col - FIXED_COLS;
                        if key_c[k].is_none() {
                            key_c[k] = Some(i);
                        }
                    }
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
                slot += 1;
                mask |= bit;
            }
        };
        claim(info, level_c, BIT_LEVEL);
        claim(info, lo_c, BIT_TS_LO);
        claim(info, hi_c, BIT_TS_HI);
        for (k, c) in key_c.iter().enumerate() {
            claim(info, *c, 1 << (FIRST_KEY_BIT_SHIFT + k));
        }

        info.set_idx_num(mask);
        // Any pushed constraint prunes blocks via terms or ts range; a
        // bare scan decompresses everything. Steer the planner.
        info.set_estimated_cost(if mask != 0 { 1e3 } else { 1e6 });
        Ok(true)
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        Ok(LogsCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            engine: Arc::clone(&self.engine),
            index_keys: self.index_keys.clone(),
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
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
        let host = unsafe { Connection::from_handle(self.db) }?;
        host.execute_batch(&shadow_block_store::drop_ddl(&self.table_name))
    }
}

impl UpdateVTab<'_> for LogsTab {
    /// INSERT. argv: [0] NULL, [1] requested rowid, then declared
    /// columns from index 2: 2=ts, 3=level, 4=message, 5=metadata,
    /// 6..6+K = index keys, 6+K = hidden command column.
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        let cmd_idx = 2 + FIXED_COLS + self.index_keys.len();
        // Command idiom, dispatched by TYPE like metrics: TEXT command,
        // BLOB reserved for a future Tier 2 batch format, NULL = data.
        match args.iter().nth(cmd_idx) {
            Some(ValueRef::Null) | None => {} // plain data row
            Some(ValueRef::Blob(_)) => {
                return Err(module_err(
                    "timeless_logs batch-blob ingest is not implemented yet \
                     (Tier 2 for logs is future work; use row INSERTs)"
                        .into(),
                ));
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
                "level is required (TEXT: debug|info|warning|error)".into(),
            ));
        };
        let level = level_from_name(&level_txt).map_err(module_err)?;
        let message: Option<String> = args.get(4)?;
        let Some(message) = message else {
            return Err(module_err("message is required (TEXT)".into()));
        };

        // metadata: optional flat JSON object (same parser as metrics
        // labels — the two tables agree on the format by construction).
        let metadata_json: Option<String> = args.get(5)?;
        let mut metadata: Vec<(String, String)> = match metadata_json {
            Some(txt) => parse_labels_json(&txt)
                .map_err(module_err)?
                .into_iter()
                .collect(),
            None => Vec::new(),
        };

        // Index-key hidden columns as INSERT shorthand: a non-NULL
        // value is merged into the metadata pairs (overriding a same-key
        // pair from the JSON — the more specific binding wins).
        for (k, key_name) in self.index_keys.iter().enumerate() {
            let v: Option<String> = args.get(6 + k)?;
            if let Some(v) = v {
                metadata.retain(|(mk, _)| mk != key_name);
                metadata.push((key_name.clone(), v));
            }
        }

        // push() canonicalizes (sorts) metadata, validates, and
        // auto-flushes at the threshold.
        self.engine
            .push(LogEntry {
                ts,
                level,
                message,
                metadata,
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
/// ('flush', 'optimize', 'prune:<ts>') are journaled and roll back
/// fully. Same savepoint limitation as metrics (xSavepoint not wired).
impl TransactionVTab<'_> for LogsTab {
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
    engine: Arc<BlockEngine>,
    index_keys: Vec<String>,
    rows: Vec<OutRow>,
    pos: usize,
    phantom: PhantomData<&'vtab LogsTab>,
}

unsafe impl VTabCursor for LogsCursor<'_> {
    /// Decode the pushed constraints per the best_index bitmask, run
    /// one engine query (sequential block reads — no rayon anywhere on
    /// this path, per the Session 3 deadlock lesson), materialize rows.
    fn filter(
        &mut self,
        idx_num: c_int,
        _idx_str: Option<&str>,
        args: &Filters<'_>,
    ) -> Result<()> {
        // argv slots were claimed in canonical order (level, ts lo,
        // ts hi, index keys), so the mask alone tells us which
        // positional arg is which.
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
        let level: Option<u8> = if idx_num & BIT_LEVEL != 0 {
            let v: Option<String> = args.get(next())?;
            match v.as_deref().map(level_from_name) {
                Some(Ok(l)) => Some(l),
                _ => {
                    impossible = true;
                    None
                }
            }
        } else {
            None
        };
        let ts_min: i64 = if idx_num & BIT_TS_LO != 0 {
            let v: Option<i64> = args.get(next())?;
            match v {
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
            let v: Option<i64> = args.get(next())?;
            match v {
                Some(v) => v,
                None => {
                    impossible = true;
                    i64::MAX - 1
                }
            }
        } else {
            i64::MAX - 1
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

        let entries = if impossible {
            Vec::new()
        } else {
            self.engine
                .query(&LogQuery {
                    ts_min,
                    ts_max,
                    level,
                    metadata_eq,
                    // message LIKE stays above us in SQLite for now
                    // (see best_index).
                    message_contains: None,
                })
                .map_err(module_err)?
        };

        self.rows = entries
            .into_iter()
            .map(|entry| OutRow {
                metadata_json: pairs_to_json(&entry.metadata),
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
        let i = i as usize;
        match i {
            0 => ctx.set_result(&row.entry.ts),
            1 => ctx.set_result(&level_name(row.entry.level)),
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
            // The hidden command column reads as NULL.
            _ => ctx.set_result(&Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}
