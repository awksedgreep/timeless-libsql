//! ShadowSpanStore: a `timeless_core::SpanBlockStore` backend that
//! persists span blocks + their inverted term index + the TRACE INDEX
//! into shadow tables on the HOST SQLite connection — the traces twin
//! of shadow_block_store.rs (read that header, and shadow_store.rs
//! before it, for the re-entrancy / no-transactions / Mutex<HostHandle>
//! reasoning; every word applies here too).
//!
//! What is different from the logs block store:
//!   - a THIRD index table, `"<name>_trace_blocks"`, maps each packed
//!     16-byte trace id to the blocks holding its spans. The PLAN.md
//!     never-dangle rule covers it exactly like `_terms`: any operation
//!     that writes or removes a block row writes/removes its trace rows
//!     in the same operation (the host transaction makes the trio
//!     atomic).
//!   - query_trace() answers the hero pushdown IN SQL: one primary-key
//!     probe of the trace index joined against the block metadata —
//!     `WHERE trace_id = x'...'` never scans anything.

use std::sync::{Mutex, MutexGuard};

use rusqlite::types::Value;
use rusqlite::vtab::escape_double_quote;
use rusqlite::{ffi, params, params_from_iter, Connection, OptionalExtension};
use timeless_core::{BlockLoc, BlockMeta, EncodedSpanBlock, SpanBlockStore};

use crate::shadow_store::HostHandle;

/// Shadow-table DDL for a traces vtab named `table` (executed by
/// xCreate; the store assumes the tables exist).
///
/// Schema notes (on top of the shadow_block_store.rs notes, which all
/// apply — explicit INTEGER PRIMARY KEY, WITHOUT ROWID posting list,
/// ts_min index):
/// - `_trace_blocks` stores PACKED 16-byte BLOBs (the timeless_traces
///   lesson: no hex text anywhere in storage — half the bytes, and
///   blob comparison is memcmp). It is WITHOUT ROWID for the same
///   reason as `_terms`: the (trace_id, block_id) pair IS the primary
///   key, so the table is its own covering index and a trace lookup is
///   one b-tree descent.
pub(crate) fn ddl(table: &str) -> String {
    let t = escape_double_quote(table);
    format!(
        r#"
CREATE TABLE IF NOT EXISTS "{t}_blocks" (
  id          INTEGER PRIMARY KEY,
  ts_min      INTEGER NOT NULL,
  ts_max      INTEGER NOT NULL,
  entry_count INTEGER NOT NULL,
  codec       INTEGER NOT NULL,
  data        BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS "{t}_blocks_ts" ON "{t}_blocks"(ts_min);
CREATE TABLE IF NOT EXISTS "{t}_terms" (
  term     TEXT NOT NULL,
  block_id INTEGER NOT NULL,
  PRIMARY KEY(term, block_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS "{t}_trace_blocks" (
  trace_id BLOB NOT NULL,
  block_id INTEGER NOT NULL,
  PRIMARY KEY(trace_id, block_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS "{t}_meta" (k TEXT PRIMARY KEY, v BLOB);
"#
    )
}

/// Statements to remove the shadow tables again (vtab xDestroy).
pub(crate) fn drop_ddl(table: &str) -> String {
    let t = escape_double_quote(table);
    format!(
        r#"DROP TABLE IF EXISTS "{t}_blocks"; DROP TABLE IF EXISTS "{t}_terms"; DROP TABLE IF EXISTS "{t}_trace_blocks"; DROP TABLE IF EXISTS "{t}_meta";"#
    )
}

pub(crate) struct ShadowSpanStore {
    host: Mutex<HostHandle>,
    // Pre-formatted SQL, built once (table names cannot be parameters;
    // prepare_cached keyed by these strings makes every statement a
    // one-time parse — the Session 1 lesson).
    insert_block_sql: String,
    insert_term_sql: String,
    insert_trace_sql: String,
    read_sql: String,
    scan_sql: String,
    save_meta_sql: String,
    load_meta_sql: String,
    /// "DELETE FROM ... IN (" prefixes, completed per call with the id
    /// list (ids are i64s we produced ourselves — injection-safe).
    delete_blocks_prefix: String,
    delete_terms_prefix: String,
    delete_traces_prefix: String,
    /// query_terms building blocks (term count varies per query; each
    /// distinct term-count SQL string is prepared once via
    /// prepare_cached).
    query_base: String,
    term_select: String,
    /// The hero query, fully preformatted (fixed shape).
    query_trace_sql: String,
}

impl ShadowSpanStore {
    pub(crate) fn new(db: *mut ffi::sqlite3, table: &str) -> Self {
        let t = escape_double_quote(table);
        let blocks = format!("\"{t}_blocks\"");
        let terms = format!("\"{t}_terms\"");
        let traces = format!("\"{t}_trace_blocks\"");
        let meta = format!("\"{t}_meta\"");
        ShadowSpanStore {
            host: Mutex::new(HostHandle(db)),
            insert_block_sql: format!(
                "INSERT INTO {blocks} (ts_min, ts_max, entry_count, codec, data) \
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            ),
            // OR IGNORE on both index tables: the engine deduplicates
            // terms and trace ids per block, but a duplicate arriving
            // anyway must not abort a flush.
            insert_term_sql: format!(
                "INSERT OR IGNORE INTO {terms} (term, block_id) VALUES (?1, ?2)"
            ),
            insert_trace_sql: format!(
                "INSERT OR IGNORE INTO {traces} (trace_id, block_id) VALUES (?1, ?2)"
            ),
            read_sql: format!("SELECT data FROM {blocks} WHERE id = ?1"),
            // scan() runs at every xConnect: metadata only, never blobs.
            scan_sql: format!("SELECT id, ts_min, ts_max, entry_count, codec FROM {blocks}"),
            save_meta_sql: format!("INSERT OR REPLACE INTO {meta} (k, v) VALUES (?1, ?2)"),
            load_meta_sql: format!("SELECT v FROM {meta} WHERE k = ?1"),
            delete_blocks_prefix: format!("DELETE FROM {blocks} WHERE id IN ("),
            delete_terms_prefix: format!("DELETE FROM {terms} WHERE block_id IN ("),
            delete_traces_prefix: format!("DELETE FROM {traces} WHERE block_id IN ("),
            query_base: format!(
                "SELECT b.id, b.ts_min, b.ts_max, b.entry_count, b.codec \
                 FROM {blocks} b WHERE b.ts_min <= ?1 AND b.ts_max >= ?2"
            ),
            term_select: format!("SELECT block_id FROM {terms} WHERE term = ?"),
            // One PK probe of the trace index (WITHOUT ROWID: the probe
            // IS the b-tree walk), then metadata rows for the matching
            // blocks. ORDER BY ts_min keeps downstream merges
            // near-sorted, same as query_terms.
            query_trace_sql: format!(
                "SELECT b.id, b.ts_min, b.ts_max, b.entry_count, b.codec \
                 FROM {blocks} b WHERE b.id IN \
                 (SELECT block_id FROM {traces} WHERE trace_id = ?1) \
                 ORDER BY b.ts_min"
            ),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HostHandle> {
        self.host.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Borrow (never own) the host connection — see shadow_store.rs.
    fn conn(guard: &MutexGuard<'_, HostHandle>) -> Result<Connection, String> {
        unsafe { Connection::from_handle(guard.0) }
            .map_err(|e| format!("from_handle failed: {e}"))
    }

    /// INSERT one block row + its term rows + its trace-index rows.
    /// The caller's enclosing host transaction makes the trio atomic —
    /// a block is never visible without BOTH kinds of index rows.
    fn insert_block(
        &self,
        conn: &Connection,
        block: &EncodedSpanBlock,
    ) -> Result<BlockLoc, String> {
        let mut stmt = conn
            .prepare_cached(&self.insert_block_sql)
            .map_err(|e| format!("prepare block insert failed: {e}"))?;
        stmt.execute(params![
            block.meta.ts_min,
            block.meta.ts_max,
            block.meta.entry_count,
            block.meta.codec,
            &block.data,
        ])
        .map_err(|e| format!("block insert failed: {e}"))?;
        // `id INTEGER PRIMARY KEY` aliases the rowid, so
        // last_insert_rowid() IS the id we just wrote.
        let id = conn.last_insert_rowid();

        let mut tstmt = conn
            .prepare_cached(&self.insert_term_sql)
            .map_err(|e| format!("prepare term insert failed: {e}"))?;
        for term in &block.terms {
            tstmt
                .execute(params![term, id])
                .map_err(|e| format!("term insert ({term:?}) failed: {e}"))?;
        }

        let mut trstmt = conn
            .prepare_cached(&self.insert_trace_sql)
            .map_err(|e| format!("prepare trace-index insert failed: {e}"))?;
        for tid in &block.trace_ids {
            trstmt
                .execute(params![&tid[..], id])
                .map_err(|e| format!("trace-index insert failed: {e}"))?;
        }
        Ok(BlockLoc { id })
    }

    /// DELETE term rows, trace rows, then block rows for `ids` — one
    /// operation, so neither index ever outlives its blocks (order
    /// within the transaction is invisible to other connections).
    fn delete_ids(&self, conn: &Connection, ids: &[i64]) -> Result<(), String> {
        if ids.is_empty() {
            return Ok(());
        }
        let list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(&format!("{}{})", self.delete_terms_prefix, list), [])
            .map_err(|e| format!("term delete failed: {e}"))?;
        conn.execute(&format!("{}{})", self.delete_traces_prefix, list), [])
            .map_err(|e| format!("trace-index delete failed: {e}"))?;
        conn.execute(&format!("{}{})", self.delete_blocks_prefix, list), [])
            .map_err(|e| format!("block delete failed: {e}"))?;
        Ok(())
    }

    /// Shared row-mapper for the two block-metadata queries.
    fn meta_rows(
        stmt: &mut rusqlite::CachedStatement<'_>,
        binds: Vec<Value>,
        what: &str,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        stmt.query_map(params_from_iter(binds), |r| {
            Ok((
                BlockLoc { id: r.get(0)? },
                BlockMeta {
                    ts_min: r.get(1)?,
                    ts_max: r.get(2)?,
                    entry_count: r.get::<_, i64>(3)? as u32,
                    codec: r.get::<_, i64>(4)? as u8,
                },
            ))
        })
        .map_err(|e| format!("{what} failed: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{what} row failed: {e}"))
    }
}

impl SpanBlockStore for ShadowSpanStore {
    /// Batch insert for the status-partitioned flush (up to three
    /// blocks per flush). One lock acquisition + one from_handle for
    /// the whole batch; insert_block's prepare_cached statements are
    /// reused across the loop. No transaction opened here (store
    /// contract) — the caller's enclosing host transaction makes the
    /// batch atomic.
    fn put_blocks(&self, blocks: &[EncodedSpanBlock]) -> Result<Vec<BlockLoc>, String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        blocks
            .iter()
            .map(|block| self.insert_block(&conn, block))
            .collect()
    }

    /// Compaction swap: inserts, index-swap callback, deletes — all
    /// riding the host's enclosing transaction (same free-atomicity
    /// argument as the logs store).
    fn replace_blocks(
        &self,
        add: &[EncodedSpanBlock],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;

        let mut locs = Vec::with_capacity(add.len());
        for block in add {
            locs.push(self.insert_block(&conn, block)?);
        }
        on_committed(&locs);

        let ids: Vec<i64> = remove.iter().map(|l| l.id).collect();
        self.delete_ids(&conn, &ids)?;
        Ok(locs)
    }

    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        let mut stmt = conn
            .prepare_cached(&self.read_sql)
            .map_err(|e| format!("prepare block read failed: {e}"))?;
        stmt.query_row([loc.id], |r| r.get::<_, Vec<u8>>(0))
            .map_err(|e| format!("block row {} read failed: {e}", loc.id))
    }

    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String> {
        let ids: Vec<i64> = locs.iter().map(|l| l.id).collect();
        let guard = self.lock();
        let conn = match Self::conn(&guard) {
            Ok(c) => c,
            Err(e) => return vec![e],
        };
        match self.delete_ids(&conn, &ids) {
            Ok(()) => Vec::new(),
            Err(e) => vec![e],
        }
    }

    /// Recovery: metadata for every persisted block (payloads
    /// untouched) so SpanBlockEngine::new can rebuild its index at
    /// xCreate/xConnect.
    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        let mut stmt = conn
            .prepare_cached(&self.scan_sql)
            .map_err(|e| format!("prepare block scan failed: {e}"))?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    BlockMeta {
                        ts_min: r.get(1)?,
                        ts_max: r.get(2)?,
                        entry_count: r.get::<_, i64>(3)? as u32,
                        codec: r.get::<_, i64>(4)? as u8,
                    },
                    BlockLoc { id: r.get(0)? },
                ))
            })
            .map_err(|e| format!("block scan failed: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("block scan row failed: {e}"))?;
        Ok(rows)
    }

    /// Posting-list intersection + ts overlap, identical SQL shape to
    /// the logs store (INTERSECT walks the (term, block_id) primary
    /// key — an index merge, no table scan).
    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        let mut sql = self.query_base.clone();
        if !terms.is_empty() {
            sql.push_str(" AND b.id IN (");
            for (i, _) in terms.iter().enumerate() {
                if i > 0 {
                    sql.push_str(" INTERSECT ");
                }
                sql.push_str(&self.term_select);
            }
            sql.push(')');
        }
        sql.push_str(" ORDER BY b.ts_min");

        // ?1 = query ts_max (vs ts_min column), ?2 = query ts_min (vs
        // ts_max column) — interval overlap — then one string per term.
        let mut binds: Vec<Value> = Vec::with_capacity(2 + terms.len());
        binds.push(Value::Integer(ts_max));
        binds.push(Value::Integer(ts_min));
        for t in terms {
            binds.push(Value::Text(t.clone()));
        }

        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        let mut stmt = conn
            .prepare_cached(&sql)
            .map_err(|e| format!("prepare term query failed: {e}"))?;
        Self::meta_rows(&mut stmt, binds, "term query")
    }

    /// The hero pushdown: which blocks hold this trace's spans? One
    /// primary-key probe of `_trace_blocks` (packed BLOB comparison =
    /// memcmp), block metadata joined in — payload blobs untouched
    /// until the engine reads the survivors.
    fn query_trace(&self, trace_id: &[u8; 16]) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        let mut stmt = conn
            .prepare_cached(&self.query_trace_sql)
            .map_err(|e| format!("prepare trace query failed: {e}"))?;
        Self::meta_rows(
            &mut stmt,
            vec![Value::Blob(trace_id.to_vec())],
            "trace query",
        )
    }

    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        let mut stmt = conn
            .prepare_cached(&self.save_meta_sql)
            .map_err(|e| format!("prepare meta save failed: {e}"))?;
        stmt.execute(params![key, value])
            .map_err(|e| format!("meta save ({key:?}) failed: {e}"))?;
        Ok(())
    }

    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        let mut stmt = conn
            .prepare_cached(&self.load_meta_sql)
            .map_err(|e| format!("prepare meta load failed: {e}"))?;
        stmt.query_row([key], |r| r.get::<_, Vec<u8>>(0))
            .optional()
            .map_err(|e| format!("meta load ({key:?}) failed: {e}"))
    }
}
