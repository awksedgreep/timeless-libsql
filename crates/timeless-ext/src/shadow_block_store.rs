//! ShadowBlockStore: a `timeless_core::blocks::BlockStore` backend that
//! persists log blocks + their inverted term index into shadow tables
//! on the HOST SQLite connection — the logs twin of shadow_store.rs
//! (read that file's header for the re-entrancy, no-transactions, and
//! Mutex<HostHandle> reasoning; every word applies here too).
//!
//! What is different from the metrics chunk store:
//!   - a `_terms` posting-list table rides along with `_blocks`, and the
//!     PLAN.md pruning rule is enforced HERE: any operation that removes
//!     a block row removes its term rows in the same operation, so
//!     posting lists can never dangle (the host transaction makes the
//!     pair atomic).
//!   - query_terms() answers the "which blocks can match?" question IN
//!     SQL: posting lists are intersected with INTERSECT and joined
//!     against the blocks' ts range — the whole point of keeping term
//!     storage on the store side of the seam.

use std::sync::{Mutex, MutexGuard};

use rusqlite::types::Value;
use rusqlite::vtab::escape_double_quote;
use rusqlite::{ffi, params, params_from_iter, Connection, OptionalExtension};
use timeless_core::{BlockLoc, BlockMeta, BlockStore, EncodedBlock};

use crate::shadow_store::HostHandle;

/// Shadow-table DDL for a logs vtab named `table` (executed by xCreate;
/// the store assumes the tables exist).
///
/// Schema notes:
/// - `id INTEGER PRIMARY KEY` is EXPLICIT for the same reason as the
///   metrics `_chunks` table: bare rowids can be renumbered by VACUUM,
///   and BlockLoc ids live in engine memory — a silent renumber would
///   corrupt the index.
/// - `_terms` is WITHOUT ROWID: it IS its own (term, block_id) primary
///   key — a covering index, no separate b-tree, exactly what a posting
///   list wants.
/// - the ts_min index serves both query_terms' range join and future
///   retention scans.
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
CREATE TABLE IF NOT EXISTS "{t}_meta" (k TEXT PRIMARY KEY, v BLOB);
"#
    )
}

/// Statements to remove the shadow tables again (vtab xDestroy).
pub(crate) fn drop_ddl(table: &str) -> String {
    let t = escape_double_quote(table);
    format!(
        r#"DROP TABLE IF EXISTS "{t}_blocks"; DROP TABLE IF EXISTS "{t}_terms"; DROP TABLE IF EXISTS "{t}_meta";"#
    )
}

pub(crate) struct ShadowBlockStore {
    host: Mutex<HostHandle>,
    // Pre-formatted SQL, built once (table names cannot be parameters).
    insert_block_sql: String,
    insert_term_sql: String,
    read_sql: String,
    scan_sql: String,
    save_meta_sql: String,
    load_meta_sql: String,
    /// "DELETE FROM ... IN (" prefixes, completed per call with the id
    /// list (ids are i64s we produced ourselves — injection-safe).
    delete_blocks_prefix: String,
    delete_terms_prefix: String,
    /// query_terms building blocks (the term count varies per query, so
    /// the final SQL is assembled per call; prepare_cached keyed by the
    /// SQL string means each distinct term-count is prepared once).
    query_base: String,
    term_select: String,
}

impl ShadowBlockStore {
    pub(crate) fn new(db: *mut ffi::sqlite3, table: &str) -> Self {
        let t = escape_double_quote(table);
        let blocks = format!("\"{t}_blocks\"");
        let terms = format!("\"{t}_terms\"");
        let meta = format!("\"{t}_meta\"");
        ShadowBlockStore {
            host: Mutex::new(HostHandle(db)),
            insert_block_sql: format!(
                "INSERT INTO {blocks} (ts_min, ts_max, entry_count, codec, data) \
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            ),
            // OR IGNORE: the engine deduplicates terms per block, but a
            // duplicate arriving anyway must not abort a flush.
            insert_term_sql: format!(
                "INSERT OR IGNORE INTO {terms} (term, block_id) VALUES (?1, ?2)"
            ),
            read_sql: format!("SELECT data FROM {blocks} WHERE id = ?1"),
            // scan() runs at every xConnect and needs metadata only —
            // never the payload blobs.
            scan_sql: format!("SELECT id, ts_min, ts_max, entry_count, codec FROM {blocks}"),
            save_meta_sql: format!("INSERT OR REPLACE INTO {meta} (k, v) VALUES (?1, ?2)"),
            load_meta_sql: format!("SELECT v FROM {meta} WHERE k = ?1"),
            delete_blocks_prefix: format!("DELETE FROM {blocks} WHERE id IN ("),
            delete_terms_prefix: format!("DELETE FROM {terms} WHERE block_id IN ("),
            // Selects the meta columns alongside the id: query_terms
            // returns (loc, meta) pairs so callers never re-read rows
            // this query already visited (Session 5 friction fix).
            query_base: format!(
                "SELECT b.id, b.ts_min, b.ts_max, b.entry_count, b.codec \
                 FROM {blocks} b WHERE b.ts_min <= ?1 AND b.ts_max >= ?2"
            ),
            term_select: format!("SELECT block_id FROM {terms} WHERE term = ?"),
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

    /// INSERT one block row + its term rows. The caller's enclosing host
    /// transaction makes the pair atomic — a block is never visible
    /// without its posting-list entries.
    fn insert_block(&self, conn: &Connection, block: &EncodedBlock) -> Result<BlockLoc, String> {
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
        Ok(BlockLoc { id })
    }

    /// DELETE term rows then block rows for `ids` — one operation, so
    /// posting lists never outlive their blocks (or vice versa: order
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
        conn.execute(&format!("{}{})", self.delete_blocks_prefix, list), [])
            .map_err(|e| format!("block delete failed: {e}"))?;
        Ok(())
    }
}

impl BlockStore for ShadowBlockStore {
    fn put_block(&self, block: &EncodedBlock) -> Result<BlockLoc, String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        self.insert_block(&conn, block)
    }

    /// Batch insert for the level-partitioned flush (up to four blocks
    /// per flush — one per level present). Overrides the default
    /// loop-of-put_block so the whole batch shares ONE lock acquisition
    /// and one from_handle, and insert_block's prepare_cached statements
    /// are reused across the loop. Still no transaction opened here
    /// (store contract): the caller's enclosing host transaction makes
    /// the batch atomic, exactly as for a single put_block.
    fn put_blocks(&self, blocks: &[EncodedBlock]) -> Result<Vec<BlockLoc>, String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        blocks
            .iter()
            .map(|block| self.insert_block(&conn, block))
            .collect()
    }

    /// Compaction swap: inserts, index-swap callback, deletes — all
    /// riding the host's enclosing transaction (same free-atomicity
    /// argument as ShadowTableStore::replace_chunks).
    fn replace_blocks(
        &self,
        add: &[EncodedBlock],
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

    /// Recovery: metadata for every persisted block (payloads untouched)
    /// so BlockEngine::new can rebuild its index at xCreate/xConnect.
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

    /// The pushdown query: intersect the posting list of every term IN
    /// SQL (INTERSECT walks the (term, block_id) primary key — an index
    /// merge, no table scan) and join the survivors against the blocks'
    /// time range. No terms → a pure ts-overlap scan on the ts_min
    /// index. Ordered by ts_min so downstream merges stay near-sorted.
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

        // Params: ?1 = query ts_max (vs ts_min column), ?2 = query
        // ts_min (vs ts_max column) — the classic interval-overlap
        // test — then one string per term, in order.
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
        let rows = stmt
            .query_map(params_from_iter(binds), |r| {
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
            .map_err(|e| format!("term query failed: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("term query row failed: {e}"))?;
        Ok(rows)
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
