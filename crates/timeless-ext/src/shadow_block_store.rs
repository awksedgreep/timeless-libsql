//! ShadowBlockStore: a `timeless_core::blocks::BlockStore` backend that
//! persists log blocks + their inverted term index into shadow tables
//! on the HOST SQLite connection — the logs twin of shadow_store.rs
//! (read that file's header for the re-entrancy, no-transactions, and
//! thread-local connection-routing reasoning (R4); every word applies
//! here too: this store holds only SQL strings and fetches the CALLING
//! connection via shared::current_conn per operation).
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

use std::sync::Mutex;

use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use timeless_core::{
    blocks::{is_raw_codec, BlockStorageStats},
    BlockLoc, BlockMeta, BlockStore, EncodedBlock,
};

use crate::{shared, sql_ident};

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
pub(crate) fn ddl(database: &str, table: &str) -> String {
    let blocks = sql_ident::qualified_shadow(database, table, "blocks");
    let blocks_local = sql_ident::quoted_shadow(table, "blocks");
    let blocks_index = sql_ident::qualified_shadow(database, table, "blocks_ts");
    let terms = sql_ident::qualified_shadow(database, table, "terms");
    let meta = sql_ident::qualified_shadow(database, table, "meta");
    format!(
        r#"
CREATE TABLE IF NOT EXISTS {blocks} (
  id          INTEGER PRIMARY KEY,
  ts_min      INTEGER NOT NULL,
  ts_max      INTEGER NOT NULL,
  entry_count INTEGER NOT NULL,
  codec       INTEGER NOT NULL,
  data        BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS {blocks_index} ON {blocks_local}(ts_min);
CREATE TABLE IF NOT EXISTS {terms} (
  term     TEXT NOT NULL,
  block_id INTEGER NOT NULL,
  PRIMARY KEY(term, block_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS {meta} (k TEXT PRIMARY KEY, v BLOB);
INSERT OR IGNORE INTO {meta}(k, v) VALUES
  ('stats_disk_entries', 0),
  ('stats_block_bytes', 0),
  ('stats_raw_bytes', 0),
  ('stats_optimize_source_entries', 0),
  ('stats_optimize_source_bytes', 0),
  ('stats_term_rows', 0);
"#
    )
}

/// Statements to remove the shadow tables again (vtab xDestroy).
pub(crate) fn drop_ddl(database: &str, table: &str) -> String {
    let blocks = sql_ident::qualified_shadow(database, table, "blocks");
    let terms = sql_ident::qualified_shadow(database, table, "terms");
    let meta = sql_ident::qualified_shadow(database, table, "meta");
    format!(
        r#"DROP TABLE IF EXISTS {blocks}; DROP TABLE IF EXISTS {terms}; DROP TABLE IF EXISTS {meta};"#
    )
}

pub(crate) struct ShadowBlockStore {
    // Pre-formatted SQL, built once (table names cannot be parameters).
    insert_block_sql: String,
    insert_term_sql: String,
    read_sql: String,
    scan_sql: String,
    stats_counter_sql: String,
    stats_fallback_sql: String,
    ensure_stats_sql: String,
    initialize_stats_sql: String,
    adjust_stats_sql: String,
    stats_fallback: Mutex<Option<BlockStorageStats>>,
    save_meta_sql: String,
    load_meta_sql: String,
    /// "DELETE FROM ... IN (" prefixes, completed per call with the id
    /// list (ids are i64s we produced ourselves — injection-safe).
    delete_blocks_prefix: String,
    delete_terms_prefix: String,
    purge_term_prefix_sql: String,
    blocks_table: String,
    terms_table: String,
    /// query_terms building blocks (the term count varies per query, so
    /// the final SQL is assembled per call; prepare_cached keyed by the
    /// SQL string means each distinct term-count is prepared once).
    query_base: String,
    term_select: String,
}

impl ShadowBlockStore {
    pub(crate) fn new(database: &str, table: &str) -> Self {
        let blocks = sql_ident::qualified_shadow(database, table, "blocks");
        let terms = sql_ident::qualified_shadow(database, table, "terms");
        let meta = sql_ident::qualified_shadow(database, table, "meta");
        ShadowBlockStore {
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
            stats_counter_sql: format!(
                "SELECT (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_disk_entries'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_block_bytes'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_raw_bytes'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_optimize_source_entries'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_optimize_source_bytes'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_term_rows')"
            ),
            stats_fallback_sql: format!(
                "SELECT COALESCE(SUM(entry_count),0), \
                        COALESCE(SUM(length(data)),0), \
                        COALESCE(SUM(CASE WHEN codec IN (1,6) THEN length(data) ELSE 0 END),0), \
                        COALESCE(SUM(CASE WHEN codec IN (1,6) OR entry_count < {target} \
                                          THEN entry_count ELSE 0 END),0), \
                        COALESCE(SUM(CASE WHEN codec IN (1,6) OR entry_count < {target} \
                                          THEN length(data) ELSE 0 END),0), \
                        (SELECT COUNT(*) FROM {terms}) FROM {blocks}",
                target = crate::logs_vtab::MERGE_TARGET_ENTRIES,
            ),
            ensure_stats_sql: format!(
                "INSERT OR IGNORE INTO {meta}(k,v) \
                 SELECT 'stats_disk_entries', COALESCE(SUM(entry_count),0) FROM {blocks} \
                 UNION ALL SELECT 'stats_block_bytes', COALESCE(SUM(length(data)),0) FROM {blocks} \
                 UNION ALL SELECT 'stats_raw_bytes', \
                   COALESCE(SUM(CASE WHEN codec IN (1,6) THEN length(data) ELSE 0 END),0) FROM {blocks} \
                 UNION ALL SELECT 'stats_optimize_source_entries', \
                   COALESCE(SUM(CASE WHEN codec IN (1,6) OR entry_count < {target} \
                                     THEN entry_count ELSE 0 END),0) FROM {blocks} \
                 UNION ALL SELECT 'stats_optimize_source_bytes', \
                   COALESCE(SUM(CASE WHEN codec IN (1,6) OR entry_count < {target} \
                                     THEN length(data) ELSE 0 END),0) FROM {blocks} \
                 UNION ALL SELECT 'stats_term_rows', COUNT(*) FROM {terms}",
                target = crate::logs_vtab::MERGE_TARGET_ENTRIES,
            ),
            initialize_stats_sql: format!(
                "INSERT OR IGNORE INTO {meta}(k,v) VALUES \
                 ('stats_disk_entries',?1),('stats_block_bytes',?2),('stats_raw_bytes',?3), \
                 ('stats_optimize_source_entries',?4),('stats_optimize_source_bytes',?5), \
                 ('stats_term_rows',?6)"
            ),
            adjust_stats_sql: format!(
                "INSERT INTO {meta}(k,v) VALUES \
                 ('stats_disk_entries',?1),('stats_block_bytes',?2),('stats_raw_bytes',?3), \
                 ('stats_optimize_source_entries',?4),('stats_optimize_source_bytes',?5), \
                 ('stats_term_rows',?6) \
                 ON CONFLICT(k) DO UPDATE SET v=CAST(v AS INTEGER)+excluded.v"
            ),
            stats_fallback: Mutex::new(None),
            save_meta_sql: format!("INSERT OR REPLACE INTO {meta} (k, v) VALUES (?1, ?2)"),
            load_meta_sql: format!("SELECT v FROM {meta} WHERE k = ?1"),
            delete_blocks_prefix: format!("DELETE FROM {blocks} WHERE id IN ("),
            delete_terms_prefix: format!("DELETE FROM {terms} WHERE block_id IN ("),
            purge_term_prefix_sql: format!("DELETE FROM {terms} WHERE term >= ?1 AND term < ?2"),
            blocks_table: blocks.clone(),
            terms_table: terms.clone(),
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

    /// Borrow (never own) the CALLING connection — the thread-local
    /// binding set by the current vtab callback (see shadow_store.rs).
    fn conn() -> Result<Connection, String> {
        shared::current_conn()
    }

    fn stats_from_values(values: [i64; 6]) -> BlockStorageStats {
        BlockStorageStats {
            disk_entries: values[0].max(0) as u64,
            bytes_on_disk: values[1].max(0) as u64,
            raw_bytes: values[2].max(0) as u64,
            optimize_source_entries: values[3].max(0) as u64,
            optimize_source_bytes: values[4].max(0) as u64,
            term_rows: values[5].max(0) as u64,
        }
    }

    fn read_storage_counters(
        &self,
        conn: &Connection,
    ) -> Result<Option<BlockStorageStats>, String> {
        let values: [Option<i64>; 6] = conn
            .query_row(&self.stats_counter_sql, [], |row| {
                Ok([
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ])
            })
            .map_err(|error| format!("read log storage counters failed: {error}"))?;
        let Some(values) = values.into_iter().collect::<Option<Vec<_>>>() else {
            return Ok(None);
        };
        Ok(Some(Self::stats_from_values(values.try_into().unwrap())))
    }

    fn scan_storage_stats(&self, conn: &Connection) -> Result<BlockStorageStats, String> {
        conn.query_row(&self.stats_fallback_sql, [], |row| {
            Ok(Self::stats_from_values([
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ]))
        })
        .map_err(|error| format!("scan log storage accounting failed: {error}"))
    }

    fn ensure_storage_stats(&self, conn: &Connection) -> Result<(), String> {
        if self.read_storage_counters(conn)?.is_some() {
            return Ok(());
        }
        let cached = *self
            .stats_fallback
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(stats) = cached {
            conn.execute(
                &self.initialize_stats_sql,
                params![
                    stats.disk_entries as i64,
                    stats.bytes_on_disk as i64,
                    stats.raw_bytes as i64,
                    stats.optimize_source_entries as i64,
                    stats.optimize_source_bytes as i64,
                    stats.term_rows as i64,
                ],
            )
            .map_err(|error| format!("initialize cached log storage counters failed: {error}"))?;
        } else {
            conn.execute(&self.ensure_stats_sql, [])
                .map_err(|error| format!("initialize log storage counters failed: {error}"))?;
        }
        Ok(())
    }

    fn adjust_storage_stats(&self, conn: &Connection, delta: [i64; 6]) -> Result<(), String> {
        conn.execute(&self.adjust_stats_sql, params_from_iter(delta))
            .map_err(|error| format!("update log storage counters failed: {error}"))?;
        Ok(())
    }

    fn block_delta(block: &EncodedBlock) -> Result<[i64; 6], String> {
        let entries = i64::from(block.meta.entry_count);
        let bytes = i64::try_from(block.data.len())
            .map_err(|_| "log block bytes exceed i64::MAX".to_string())?;
        let raw = is_raw_codec(block.meta.codec);
        let optimize_source =
            raw || (block.meta.entry_count as usize) < crate::logs_vtab::MERGE_TARGET_ENTRIES;
        let terms = i64::try_from(block.terms.len())
            .map_err(|_| "log term count exceeds i64::MAX".to_string())?;
        Ok([
            entries,
            bytes,
            if raw { bytes } else { 0 },
            if optimize_source { entries } else { 0 },
            if optimize_source { bytes } else { 0 },
            terms,
        ])
    }

    fn selected_storage_stats(&self, conn: &Connection, ids: &str) -> Result<[i64; 6], String> {
        let sql = format!(
            "SELECT COALESCE(SUM(entry_count),0), COALESCE(SUM(length(data)),0), \
                    COALESCE(SUM(CASE WHEN codec IN (1,6) THEN length(data) ELSE 0 END),0), \
                    COALESCE(SUM(CASE WHEN codec IN (1,6) OR entry_count < {target} \
                                      THEN entry_count ELSE 0 END),0), \
                    COALESCE(SUM(CASE WHEN codec IN (1,6) OR entry_count < {target} \
                                      THEN length(data) ELSE 0 END),0), \
                    (SELECT COUNT(*) FROM {terms} WHERE block_id IN ({ids})) \
             FROM {blocks} WHERE id IN ({ids})",
            target = crate::logs_vtab::MERGE_TARGET_ENTRIES,
            terms = self.terms_table,
            blocks = self.blocks_table,
        );
        conn.query_row(&sql, [], |row| {
            Ok([
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ])
        })
        .map_err(|error| format!("read removed log accounting failed: {error}"))
    }

    /// INSERT one block row + its term rows. The caller's enclosing host
    /// transaction makes the pair atomic — a block is never visible
    /// without its posting-list entries.
    fn insert_block(&self, conn: &Connection, block: &EncodedBlock) -> Result<BlockLoc, String> {
        self.ensure_storage_stats(conn)?;
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
        let mut inserted_terms = 0_i64;
        for term in &block.terms {
            inserted_terms += tstmt
                .execute(params![term, id])
                .map_err(|e| format!("term insert ({term:?}) failed: {e}"))?
                as i64;
        }
        let mut delta = Self::block_delta(block)?;
        delta[5] = inserted_terms;
        self.adjust_storage_stats(conn, delta)?;
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
        self.ensure_storage_stats(conn)?;
        let removed = self.selected_storage_stats(conn, &list)?;
        conn.execute(&format!("{}{})", self.delete_terms_prefix, list), [])
            .map_err(|e| format!("term delete failed: {e}"))?;
        conn.execute(&format!("{}{})", self.delete_blocks_prefix, list), [])
            .map_err(|e| format!("block delete failed: {e}"))?;
        self.adjust_storage_stats(conn, removed.map(|value| -value))?;
        Ok(())
    }
}

impl BlockStore for ShadowBlockStore {
    fn query_snapshot_keeps_locations_readable(&self) -> bool {
        // Every engine query runs inside the host virtual-table SELECT on the
        // same connection used by read_block(). SQLite keeps that statement's
        // read snapshot stable across WAL commits; in rollback-journal mode
        // its shared lock likewise keeps deleted rows readable until the
        // statement finishes. The engine may therefore retain row ids and
        // stream one payload at a time after releasing publication guards.
        true
    }

    fn check_cancelled(&self) -> Result<(), String> {
        Self::conn()?
            .query_row("SELECT 1", [], |_| Ok(()))
            .map_err(|error| format!("log query cancellation checkpoint failed: {error}"))
    }

    fn storage_stats(&self) -> Result<BlockStorageStats, String> {
        let conn = Self::conn()?;
        if let Some(stats) = self.read_storage_counters(&conn)? {
            return Ok(stats);
        }
        let mut fallback = self
            .stats_fallback
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(stats) = *fallback {
            return Ok(stats);
        }
        let stats = self.scan_storage_stats(&conn)?;
        *fallback = Some(stats);
        Ok(stats)
    }

    fn put_block(&self, block: &EncodedBlock) -> Result<BlockLoc, String> {
        let conn = Self::conn()?;
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
        let conn = Self::conn()?;
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
        let conn = Self::conn()?;

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
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.read_sql)
            .map_err(|e| format!("prepare block read failed: {e}"))?;
        stmt.query_row([loc.id], |r| r.get::<_, Vec<u8>>(0))
            .map_err(|e| format!("block row {} read failed: {e}", loc.id))
    }

    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String> {
        let ids: Vec<i64> = locs.iter().map(|l| l.id).collect();
        let conn = match Self::conn() {
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
        let conn = Self::conn()?;
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

        let conn = Self::conn()?;
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

    /// Rewrite one block's postings without touching its payload.
    ///
    /// No transaction of its own: this runs inside the vtab's update hook, so
    /// a statement is already in progress and an explicit COMMIT here fails
    /// with "SQL statements in progress". Atomicity comes from the caller's
    /// enclosing host transaction, exactly as it does for put_block's
    /// block-plus-terms insert.
    fn purge_term_prefix(&self, prefix: &str) -> Result<u64, String> {
        if prefix.is_empty() {
            return Err("purge_term_prefix: empty prefix".into());
        }
        // Exact byte-range prefix match: [prefix, prefix-with-last-byte-
        // incremented). Precise and case-sensitive where LIKE is not, and
        // the WITHOUT ROWID (term, block_id) key makes it a range delete.
        let mut upper = prefix.as_bytes().to_vec();
        let last = upper.last_mut().expect("checked non-empty");
        if *last == u8::MAX {
            return Err("purge_term_prefix: prefix ends in 0xff".into());
        }
        *last += 1;
        let upper = String::from_utf8(upper)
            .map_err(|_| "purge_term_prefix: prefix bound is not UTF-8".to_string())?;
        let conn = Self::conn()?;
        self.ensure_storage_stats(&conn)?;
        let removed = conn
            .execute(&self.purge_term_prefix_sql, params![prefix, upper])
            .map_err(|e| format!("term purge ({prefix:?}) failed: {e}"))?;
        self.adjust_storage_stats(&conn, [0, 0, 0, 0, 0, -(removed as i64)])?;
        Ok(removed as u64)
    }

    fn replace_terms(&self, loc: &BlockLoc, terms: &[String]) -> Result<(), String> {
        let conn = Self::conn()?;
        self.ensure_storage_stats(&conn)?;
        let previous: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {} WHERE block_id=?1",
                    self.terms_table
                ),
                [loc.id],
                |row| row.get(0),
            )
            .map_err(|error| format!("read reindex term accounting failed: {error}"))?;

        conn.execute(&format!("{}{})", self.delete_terms_prefix, loc.id), [])
            .map_err(|e| format!("reindex term delete failed: {e}"))?;

        let mut stmt = conn
            .prepare_cached(&self.insert_term_sql)
            .map_err(|e| format!("prepare reindex term insert failed: {e}"))?;

        let mut inserted = 0_i64;
        for term in terms {
            inserted += stmt
                .execute(params![term, loc.id])
                .map_err(|e| format!("reindex term insert ({term:?}) failed: {e}"))?
                as i64;
        }
        self.adjust_storage_stats(&conn, [0, 0, 0, 0, 0, inserted - previous])?;

        Ok(())
    }

    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.save_meta_sql)
            .map_err(|e| format!("prepare meta save failed: {e}"))?;
        stmt.execute(params![key, value])
            .map_err(|e| format!("meta save ({key:?}) failed: {e}"))?;
        Ok(())
    }

    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.load_meta_sql)
            .map_err(|e| format!("prepare meta load failed: {e}"))?;
        stmt.query_row([key], |r| r.get::<_, Vec<u8>>(0))
            .optional()
            .map_err(|e| format!("meta load ({key:?}) failed: {e}"))
    }
}
