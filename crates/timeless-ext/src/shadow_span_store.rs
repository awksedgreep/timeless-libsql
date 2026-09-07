//! ShadowSpanStore: a `timeless_core::SpanBlockStore` backend that
//! persists span blocks + their inverted term index + the TRACE INDEX
//! into shadow tables on the HOST SQLite connection — the traces twin
//! of shadow_block_store.rs (read that header, and shadow_store.rs
//! before it, for the re-entrancy / no-transactions / thread-local
//! connection-routing (R4) reasoning; every word applies here too:
//! this store holds only SQL strings and fetches the CALLING
//! connection via shared::current_conn per operation).
//!
//! What is different from the logs block store:
//!   - a THIRD index table, `"<name>_trace_blocks"`, maps each packed
//!     16-byte trace id to the blocks holding its spans. The PLAN.md
//!     never-dangle rule covers it exactly like `_terms`: any operation
//!     that writes or removes a block row writes/removes its trace rows
//!     in the same operation.
//!   - a tiny `"<name>_duration_bounds"` side table stores optional
//!     per-block duration extrema. Keeping it separate prevents legacy
//!     metadata backfill from rewriting compressed payload pages into WAL.
//!     The host transaction makes the block and all three metadata/index
//!     tables atomic.
//!   - query_trace() answers the hero pushdown IN SQL: one primary-key
//!     probe of the trace index joined against the block metadata —
//!     `WHERE trace_id = x'...'` never scans anything.

use std::collections::HashMap;
use std::sync::Mutex;

use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use timeless_core::spans::SpanStorageStats;
use timeless_core::{
    span_attribute_bloom_checksum, validate_span_attribute_bloom, BlockLoc, BlockMeta,
    EncodedSpanBlock, SpanAttributeBloom, SpanAttributeFilter, SpanBlockStore, SpanDurationBounds,
    SPAN_ATTRIBUTE_BLOOM_VERSION,
};

use crate::{shared, sql_ident};

// Keep the per-statement bind count and Bloom-row working set independent of
// the total candidate-block count. Two leading binds are added for scope/path.
const ATTRIBUTE_BLOOM_QUERY_BLOCKS: usize = 256;

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
pub(crate) fn ddl(database: &str, table: &str) -> String {
    let blocks = sql_ident::qualified_shadow(database, table, "blocks");
    let blocks_local = sql_ident::quoted_shadow(table, "blocks");
    let blocks_index = sql_ident::qualified_shadow(database, table, "blocks_ts");
    let terms = sql_ident::qualified_shadow(database, table, "terms");
    let traces = sql_ident::qualified_shadow(database, table, "trace_blocks");
    let durations = sql_ident::qualified_shadow(database, table, "duration_bounds");
    let attributes = sql_ident::qualified_shadow(database, table, "attribute_blooms");
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
CREATE TABLE IF NOT EXISTS {traces} (
  trace_id BLOB NOT NULL,
  block_id INTEGER NOT NULL,
  PRIMARY KEY(trace_id, block_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS {durations} (
  block_id     INTEGER PRIMARY KEY,
  duration_min INTEGER NOT NULL,
  duration_max INTEGER NOT NULL,
  CHECK(duration_min <= duration_max)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS {attributes} (
  scope        TEXT NOT NULL,
  path         TEXT NOT NULL,
  block_id     INTEGER NOT NULL,
  hash_version INTEGER NOT NULL,
  bits         BLOB NOT NULL,
  checksum     BLOB NOT NULL,
  PRIMARY KEY(scope, path, block_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS {meta} (k TEXT PRIMARY KEY, v BLOB);
INSERT OR IGNORE INTO {meta}(k, v) VALUES
  ('stats_block_bytes', 0),
  ('stats_raw_bytes', 0),
  ('stats_optimize_source_entries', 0),
  ('stats_optimize_source_bytes', 0),
  ('stats_duration_bounded_blocks', 0),
  ('stats_term_rows', 0),
  ('stats_trace_index_rows', 0),
  ('stats_attribute_bloom_rows', 0),
  ('stats_attribute_bloom_bytes', 0);
"#
    )
}

/// Add the duration-extrema side table to databases created by an older
/// extension. A missing row means unknown: the legacy block remains exact
/// through decode-time filtering until ordinary optimize backfills it. Keeping
/// these tiny rows separate avoids rewriting payload-sized block records and
/// WAL frames during the one-time backfill.
pub(crate) fn ensure_duration_bounds_table(
    conn: &Connection,
    database: &str,
    table: &str,
) -> rusqlite::Result<()> {
    let durations = sql_ident::qualified_shadow(database, table, "duration_bounds");
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {durations} (\
           block_id INTEGER PRIMARY KEY,\
           duration_min INTEGER NOT NULL,\
           duration_max INTEGER NOT NULL,\
           CHECK(duration_min <= duration_max)\
         ) WITHOUT ROWID"
    ))
}

/// Add the optional fixed-size attribute filter table to legacy databases.
/// Missing rows always mean exact decode fallback, so creating the empty table
/// is a format-compatible schema migration.
pub(crate) fn ensure_attribute_blooms_table(
    conn: &Connection,
    database: &str,
    table: &str,
) -> rusqlite::Result<()> {
    let attributes = sql_ident::qualified_shadow(database, table, "attribute_blooms");
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {attributes} (\
           scope TEXT NOT NULL,\
           path TEXT NOT NULL,\
           block_id INTEGER NOT NULL,\
           hash_version INTEGER NOT NULL,\
           bits BLOB NOT NULL,\
           checksum BLOB NOT NULL,\
           PRIMARY KEY(scope,path,block_id)\
         ) WITHOUT ROWID"
    ))
}

pub(crate) fn require_read_schema(
    conn: &Connection,
    database: &str,
    table: &str,
) -> rusqlite::Result<()> {
    let blocks = sql_ident::qualified_shadow(database, table, "blocks");
    let durations = sql_ident::qualified_shadow(database, table, "duration_bounds");
    let attributes = sql_ident::qualified_shadow(database, table, "attribute_blooms");
    let upgrade = || {
        rusqlite::Error::ModuleError(format!(
            "{database}.{table} requires a legacy schema upgrade; run \
             SELECT timeless_upgrade('{database}.{table}') on a writable connection"
        ))
    };
    conn.prepare(&format!("SELECT 1 FROM {blocks} LIMIT 0"))?;
    conn.prepare(&format!("SELECT block_id FROM {durations} LIMIT 0"))
        .map_err(|_| upgrade())?;
    conn.prepare(&format!("SELECT block_id FROM {attributes} LIMIT 0"))
        .map_err(|_| upgrade())?;
    Ok(())
}

/// Statements to remove the shadow tables again (vtab xDestroy).
pub(crate) fn drop_ddl(database: &str, table: &str) -> String {
    let blocks = sql_ident::qualified_shadow(database, table, "blocks");
    let terms = sql_ident::qualified_shadow(database, table, "terms");
    let traces = sql_ident::qualified_shadow(database, table, "trace_blocks");
    let durations = sql_ident::qualified_shadow(database, table, "duration_bounds");
    let attributes = sql_ident::qualified_shadow(database, table, "attribute_blooms");
    let meta = sql_ident::qualified_shadow(database, table, "meta");
    format!(
        r#"DROP TABLE IF EXISTS {blocks}; DROP TABLE IF EXISTS {terms}; DROP TABLE IF EXISTS {traces}; DROP TABLE IF EXISTS {durations}; DROP TABLE IF EXISTS {attributes}; DROP TABLE IF EXISTS {meta};"#
    )
}

pub(crate) struct ShadowSpanStore {
    // Pre-formatted SQL, built once (table names cannot be parameters;
    // prepare_cached keyed by these strings makes every statement a
    // one-time parse — the Session 1 lesson).
    insert_block_sql: String,
    insert_duration_sql: String,
    insert_term_sql: String,
    insert_trace_sql: String,
    insert_attribute_bloom_sql: String,
    read_sql: String,
    scan_sql: String,
    validate_duration_sql: String,
    validate_attribute_rows_sql: String,
    missing_duration_sql: String,
    update_duration_sql: String,
    duration_exists_sql: String,
    stats_counter_sql: String,
    stats_fallback_sql: String,
    initialize_stats_sql: String,
    adjust_stats_sql: String,
    stats_fallback: Mutex<Option<SpanStorageStats>>,
    save_meta_sql: String,
    load_meta_sql: String,
    /// "DELETE FROM ... IN (" prefixes, completed per call with the id
    /// list (ids are i64s we produced ourselves — injection-safe).
    delete_blocks_prefix: String,
    delete_terms_prefix: String,
    delete_traces_prefix: String,
    delete_durations_prefix: String,
    delete_attribute_blooms_prefix: String,
    /// query_terms building blocks (term count varies per query; each
    /// distinct term-count SQL string is prepared once via
    /// prepare_cached).
    query_base: String,
    term_select: String,
    blocks_table: String,
    terms_table: String,
    traces_table: String,
    durations_table: String,
    attribute_blooms_table: String,
    /// The hero query, fully preformatted (fixed shape).
    query_trace_sql: String,
}

impl ShadowSpanStore {
    pub(crate) fn new(database: &str, table: &str) -> Self {
        let blocks = sql_ident::qualified_shadow(database, table, "blocks");
        let terms = sql_ident::qualified_shadow(database, table, "terms");
        let traces = sql_ident::qualified_shadow(database, table, "trace_blocks");
        let durations = sql_ident::qualified_shadow(database, table, "duration_bounds");
        let attributes = sql_ident::qualified_shadow(database, table, "attribute_blooms");
        let meta = sql_ident::qualified_shadow(database, table, "meta");
        ShadowSpanStore {
            insert_block_sql: format!(
                "INSERT INTO {blocks} (ts_min, ts_max, entry_count, codec, data) \
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            ),
            insert_duration_sql: format!(
                "INSERT INTO {durations} (block_id, duration_min, duration_max) \
                 VALUES (?1, ?2, ?3)"
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
            insert_attribute_bloom_sql: format!(
                "INSERT INTO {attributes} \
                 (scope,path,block_id,hash_version,bits,checksum) \
                 VALUES (?1,?2,?3,?4,?5,?6)"
            ),
            read_sql: format!("SELECT data FROM {blocks} WHERE id = ?1"),
            // scan() runs at every xConnect: metadata only, never blobs.
            scan_sql: format!(
                "SELECT b.id, b.ts_min, b.ts_max, b.entry_count, b.codec, \
                        d.duration_min, d.duration_max \
                 FROM {blocks} b LEFT JOIN {durations} d ON d.block_id = b.id"
            ),
            validate_duration_sql: format!(
                "SELECT d.block_id, d.duration_min, d.duration_max \
                 FROM {durations} d LEFT JOIN {blocks} b ON b.id = d.block_id \
                 WHERE b.id IS NULL OR d.duration_min > d.duration_max LIMIT 1"
            ),
            validate_attribute_rows_sql: format!(
                "SELECT a.scope,a.path,a.block_id FROM {attributes} a \
                 LEFT JOIN {blocks} b ON b.id=a.block_id \
                 WHERE b.id IS NULL LIMIT 1"
            ),
            missing_duration_sql: format!(
                "SELECT b.id, b.ts_min, b.ts_max, b.entry_count, b.codec \
                 FROM {blocks} b LEFT JOIN {durations} d ON d.block_id = b.id \
                 WHERE d.block_id IS NULL ORDER BY b.ts_min, b.id"
            ),
            update_duration_sql: format!(
                "INSERT OR REPLACE INTO {durations} \
                 (block_id, duration_min, duration_max) VALUES (?3, ?1, ?2)"
            ),
            duration_exists_sql: format!(
                "SELECT EXISTS(SELECT 1 FROM {durations} WHERE block_id=?1)"
            ),
            stats_counter_sql: format!(
                "SELECT (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_block_bytes'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_raw_bytes'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_optimize_source_entries'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_optimize_source_bytes'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_duration_bounded_blocks'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_term_rows'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_trace_index_rows'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_attribute_bloom_rows'), \
                        (SELECT CAST(v AS INTEGER) FROM {meta} WHERE k='stats_attribute_bloom_bytes')"
            ),
            stats_fallback_sql: format!(
                "SELECT COALESCE(SUM(length(data)),0), \
                        COALESCE(SUM(CASE WHEN codec=1 THEN length(data) ELSE 0 END),0), \
                        COALESCE(SUM(CASE WHEN codec=1 OR entry_count < {target} \
                                          THEN entry_count ELSE 0 END),0), \
                        COALESCE(SUM(CASE WHEN codec=1 OR entry_count < {target} \
                                          THEN length(data) ELSE 0 END),0), \
                        (SELECT COUNT(*) FROM {durations}), \
                        (SELECT COUNT(*) FROM {terms}), \
                        (SELECT COUNT(*) FROM {traces}), \
                        (SELECT COUNT(*) FROM {attributes}), \
                        (SELECT COALESCE(SUM(length(bits)),0) FROM {attributes}) \
                 FROM {blocks}",
                target = crate::traces_vtab::MERGE_TARGET_ENTRIES,
            ),
            initialize_stats_sql: format!(
                "INSERT OR IGNORE INTO {meta}(k,v) VALUES \
                 ('stats_block_bytes',?1),('stats_raw_bytes',?2), \
                 ('stats_optimize_source_entries',?3),('stats_optimize_source_bytes',?4), \
                 ('stats_duration_bounded_blocks',?5),('stats_term_rows',?6), \
                 ('stats_trace_index_rows',?7),('stats_attribute_bloom_rows',?8), \
                 ('stats_attribute_bloom_bytes',?9)"
            ),
            adjust_stats_sql: format!(
                "INSERT INTO {meta}(k,v) VALUES \
                 ('stats_block_bytes',?1),('stats_raw_bytes',?2), \
                 ('stats_optimize_source_entries',?3),('stats_optimize_source_bytes',?4), \
                 ('stats_duration_bounded_blocks',?5),('stats_term_rows',?6), \
                 ('stats_trace_index_rows',?7),('stats_attribute_bloom_rows',?8), \
                 ('stats_attribute_bloom_bytes',?9) \
                 ON CONFLICT(k) DO UPDATE SET v=CAST(v AS INTEGER)+excluded.v"
            ),
            stats_fallback: Mutex::new(None),
            save_meta_sql: format!("INSERT OR REPLACE INTO {meta} (k, v) VALUES (?1, ?2)"),
            load_meta_sql: format!("SELECT v FROM {meta} WHERE k = ?1"),
            delete_blocks_prefix: format!("DELETE FROM {blocks} WHERE id IN ("),
            delete_terms_prefix: format!("DELETE FROM {terms} WHERE block_id IN ("),
            delete_traces_prefix: format!("DELETE FROM {traces} WHERE block_id IN ("),
            delete_durations_prefix: format!("DELETE FROM {durations} WHERE block_id IN ("),
            delete_attribute_blooms_prefix: format!("DELETE FROM {attributes} WHERE block_id IN ("),
            query_base: format!(
                "SELECT b.id, b.ts_min, b.ts_max, b.entry_count, b.codec \
                 FROM {blocks} b LEFT JOIN {durations} d ON d.block_id = b.id \
                 WHERE b.ts_min <= ?1 AND b.ts_max >= ?2 \
                 AND (d.duration_max IS NULL OR d.duration_max >= ?3) \
                 AND (d.duration_min IS NULL OR d.duration_min <= ?4)"
            ),
            term_select: format!("SELECT block_id FROM {terms} WHERE term = ?"),
            blocks_table: blocks.clone(),
            terms_table: terms.clone(),
            traces_table: traces.clone(),
            durations_table: durations.clone(),
            attribute_blooms_table: attributes.clone(),
            // One PK probe of the trace index (WITHOUT ROWID: the probe
            // IS the b-tree walk), then metadata rows for the matching
            // blocks. ORDER BY ts_min keeps downstream merges
            // near-sorted, same as query_terms.
            query_trace_sql: format!(
                "SELECT b.id, b.ts_min, b.ts_max, b.entry_count, b.codec \
                 FROM {blocks} b LEFT JOIN {durations} d ON d.block_id = b.id \
                 WHERE b.id IN \
                 (SELECT block_id FROM {traces} WHERE trace_id = ?1) \
                 AND b.ts_min <= ?2 AND b.ts_max >= ?3 \
                 AND (d.duration_max IS NULL OR d.duration_max >= ?4) \
                 AND (d.duration_min IS NULL OR d.duration_min <= ?5) \
                 ORDER BY b.ts_min"
            ),
        }
    }

    /// Borrow (never own) the CALLING connection — the thread-local
    /// binding set by the current vtab callback (see shadow_store.rs).
    fn conn() -> Result<Connection, String> {
        shared::current_conn()
    }

    fn stats_from_values(values: [i64; 9]) -> SpanStorageStats {
        SpanStorageStats {
            bytes_on_disk: values[0].max(0) as u64,
            raw_bytes: values[1].max(0) as u64,
            optimize_source_entries: values[2].max(0) as u64,
            optimize_source_bytes: values[3].max(0) as u64,
            duration_bounded_blocks: values[4].max(0) as u64,
            term_rows: values[5].max(0) as u64,
            trace_index_rows: values[6].max(0) as u64,
            attribute_bloom_rows: values[7].max(0) as u64,
            attribute_bloom_bytes: values[8].max(0) as u64,
        }
    }

    fn read_storage_counters(&self, conn: &Connection) -> Result<Option<SpanStorageStats>, String> {
        let values: [Option<i64>; 9] = conn
            .query_row(&self.stats_counter_sql, [], |row| {
                Ok([
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ])
            })
            .map_err(|error| format!("read trace storage counters failed: {error}"))?;
        let Some(values) = values.into_iter().collect::<Option<Vec<_>>>() else {
            return Ok(None);
        };
        Ok(Some(Self::stats_from_values(values.try_into().unwrap())))
    }

    fn scan_storage_stats(&self, conn: &Connection) -> Result<SpanStorageStats, String> {
        conn.query_row(&self.stats_fallback_sql, [], |row| {
            Ok(Self::stats_from_values([
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            ]))
        })
        .map_err(|error| format!("scan trace storage accounting failed: {error}"))
    }

    fn ensure_storage_stats(&self, conn: &Connection) -> Result<(), String> {
        if self.read_storage_counters(conn)?.is_some() {
            return Ok(());
        }
        let cached = *self
            .stats_fallback
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // See ShadowBlockStore::ensure_storage_stats: split the legacy
        // baseline read from the counter write to avoid self-locking SQLite
        // with a nested INSERT ... SELECT inside xUpdate.
        let stats = match cached {
            Some(stats) => stats,
            None => self.scan_storage_stats(conn)?,
        };
        conn.execute(
            &self.initialize_stats_sql,
            params![
                stats.bytes_on_disk as i64,
                stats.raw_bytes as i64,
                stats.optimize_source_entries as i64,
                stats.optimize_source_bytes as i64,
                stats.duration_bounded_blocks as i64,
                stats.term_rows as i64,
                stats.trace_index_rows as i64,
                stats.attribute_bloom_rows as i64,
                stats.attribute_bloom_bytes as i64,
            ],
        )
        .map_err(|error| format!("initialize trace storage counters failed: {error}"))?;
        Ok(())
    }

    fn adjust_storage_stats(&self, conn: &Connection, delta: [i64; 9]) -> Result<(), String> {
        conn.execute(&self.adjust_stats_sql, params_from_iter(delta))
            .map_err(|error| format!("update trace storage counters failed: {error}"))?;
        Ok(())
    }

    fn block_delta(
        block: &EncodedSpanBlock,
        duration_bounds: Option<SpanDurationBounds>,
    ) -> Result<[i64; 9], String> {
        let entries = i64::from(block.meta.entry_count);
        let bytes = i64::try_from(block.data.len())
            .map_err(|_| "trace block bytes exceed i64::MAX".to_string())?;
        let raw = block.meta.codec == 1;
        let optimize_source =
            raw || (block.meta.entry_count as usize) < crate::traces_vtab::MERGE_TARGET_ENTRIES;
        let terms = i64::try_from(block.terms.len())
            .map_err(|_| "trace term count exceeds i64::MAX".to_string())?;
        let traces = i64::try_from(block.trace_ids.len())
            .map_err(|_| "trace index count exceeds i64::MAX".to_string())?;
        let bloom_rows = i64::try_from(block.attribute_blooms.len())
            .map_err(|_| "trace attribute bloom count exceeds i64::MAX".to_string())?;
        let bloom_bytes = block
            .attribute_blooms
            .iter()
            .try_fold(0_i64, |total, bloom| {
                let bytes = i64::try_from(bloom.bits.len())
                    .map_err(|_| "trace attribute bloom bytes exceed i64::MAX".to_string())?;
                total
                    .checked_add(bytes)
                    .ok_or_else(|| "trace attribute bloom bytes exceed i64::MAX".to_string())
            })?;
        Ok([
            bytes,
            if raw { bytes } else { 0 },
            if optimize_source { entries } else { 0 },
            if optimize_source { bytes } else { 0 },
            i64::from(duration_bounds.is_some()),
            terms,
            traces,
            bloom_rows,
            bloom_bytes,
        ])
    }

    fn selected_storage_stats(&self, conn: &Connection, ids: &str) -> Result<[i64; 9], String> {
        let sql = format!(
            "SELECT COALESCE(SUM(length(data)),0), \
                    COALESCE(SUM(CASE WHEN codec=1 THEN length(data) ELSE 0 END),0), \
                    COALESCE(SUM(CASE WHEN codec=1 OR entry_count < {target} \
                                      THEN entry_count ELSE 0 END),0), \
                    COALESCE(SUM(CASE WHEN codec=1 OR entry_count < {target} \
                                      THEN length(data) ELSE 0 END),0), \
                    (SELECT COUNT(*) FROM {durations} WHERE block_id IN ({ids})), \
                    (SELECT COUNT(*) FROM {terms} WHERE block_id IN ({ids})), \
                    (SELECT COUNT(*) FROM {traces} WHERE block_id IN ({ids})), \
                    (SELECT COUNT(*) FROM {attributes} WHERE block_id IN ({ids})), \
                    (SELECT COALESCE(SUM(length(bits)),0) FROM {attributes} \
                       WHERE block_id IN ({ids})) \
             FROM {blocks} WHERE id IN ({ids})",
            target = crate::traces_vtab::MERGE_TARGET_ENTRIES,
            blocks = self.blocks_table,
            terms = self.terms_table,
            traces = self.traces_table,
            durations = self.durations_table,
            attributes = self.attribute_blooms_table,
        );
        conn.query_row(&sql, [], |row| {
            Ok([
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            ])
        })
        .map_err(|error| format!("read removed trace accounting failed: {error}"))
    }

    /// INSERT one block row + its duration, term, and trace-index rows.
    /// The caller's enclosing host transaction makes the operation atomic.
    fn insert_block(
        &self,
        conn: &Connection,
        block: &EncodedSpanBlock,
        duration_bounds: Option<SpanDurationBounds>,
    ) -> Result<BlockLoc, String> {
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

        if let Some(bounds) = duration_bounds {
            conn.prepare_cached(&self.insert_duration_sql)
                .map_err(|e| format!("prepare duration-bound insert failed: {e}"))?
                .execute(params![id, bounds.min_ns, bounds.max_ns])
                .map_err(|e| format!("duration-bound insert for block {id} failed: {e}"))?;
        }

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

        let mut trstmt = conn
            .prepare_cached(&self.insert_trace_sql)
            .map_err(|e| format!("prepare trace-index insert failed: {e}"))?;
        let mut inserted_traces = 0_i64;
        for tid in &block.trace_ids {
            inserted_traces += trstmt
                .execute(params![&tid[..], id])
                .map_err(|e| format!("trace-index insert failed: {e}"))?
                as i64;
        }
        let mut astmt = conn
            .prepare_cached(&self.insert_attribute_bloom_sql)
            .map_err(|error| format!("prepare trace attribute bloom insert failed: {error}"))?;
        for bloom in &block.attribute_blooms {
            validate_span_attribute_bloom(&bloom.bits)?;
            let checksum = span_attribute_bloom_checksum(&bloom.bits);
            astmt
                .execute(params![
                    bloom.index.scope().name(),
                    bloom.index.path(),
                    id,
                    i64::from(SPAN_ATTRIBUTE_BLOOM_VERSION),
                    &bloom.bits,
                    &checksum[..],
                ])
                .map_err(|error| {
                    format!(
                        "trace attribute bloom insert for {}:{} block {id} failed: {error}",
                        bloom.index.scope().name(),
                        bloom.index.path()
                    )
                })?;
        }
        let mut delta = Self::block_delta(block, duration_bounds)?;
        delta[5] = inserted_terms;
        delta[6] = inserted_traces;
        self.adjust_storage_stats(conn, delta)?;
        Ok(BlockLoc { id })
    }

    /// DELETE term, trace, and duration rows, then block rows for `ids` —
    /// one operation, so no metadata ever outlives its blocks (order
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
        conn.execute(&format!("{}{})", self.delete_traces_prefix, list), [])
            .map_err(|e| format!("trace-index delete failed: {e}"))?;
        conn.execute(&format!("{}{})", self.delete_durations_prefix, list), [])
            .map_err(|e| format!("duration-bound delete failed: {e}"))?;
        conn.execute(
            &format!("{}{})", self.delete_attribute_blooms_prefix, list),
            [],
        )
        .map_err(|error| format!("trace attribute bloom delete failed: {error}"))?;
        conn.execute(&format!("{}{})", self.delete_blocks_prefix, list), [])
            .map_err(|e| format!("block delete failed: {e}"))?;
        self.adjust_storage_stats(conn, removed.map(|value| -value))?;
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
    fn query_snapshot_keeps_locations_readable(&self) -> bool {
        // The outer virtual-table SELECT pins the host connection's SQLite
        // snapshot. WAL retains replaced rows and rollback-journal mode keeps
        // its shared lock until the statement ends, so row ids captured by
        // xFilter remain readable while xNext streams payloads.
        true
    }

    fn storage_stats(&self) -> Result<SpanStorageStats, String> {
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

    /// Batch insert for the status-partitioned flush (up to three
    /// blocks per flush). One lock acquisition + one from_handle for
    /// the whole batch; insert_block's prepare_cached statements are
    /// reused across the loop. No transaction opened here (store
    /// contract) — the caller's enclosing host transaction makes the
    /// batch atomic.
    fn put_blocks(&self, blocks: &[EncodedSpanBlock]) -> Result<Vec<BlockLoc>, String> {
        let conn = Self::conn()?;
        blocks
            .iter()
            .map(|block| self.insert_block(&conn, block, None))
            .collect()
    }

    fn put_blocks_with_duration_bounds(
        &self,
        blocks: &[EncodedSpanBlock],
        duration_bounds: &[SpanDurationBounds],
    ) -> Result<Vec<BlockLoc>, String> {
        if blocks.len() != duration_bounds.len() {
            return Err(format!(
                "span block/duration metadata length mismatch: {} blocks, {} bounds",
                blocks.len(),
                duration_bounds.len()
            ));
        }
        let conn = Self::conn()?;
        blocks
            .iter()
            .zip(duration_bounds)
            .map(|(block, bounds)| self.insert_block(&conn, block, Some(*bounds)))
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
        let conn = Self::conn()?;

        let mut locs = Vec::with_capacity(add.len());
        for block in add {
            locs.push(self.insert_block(&conn, block, None)?);
        }
        on_committed(&locs);

        let ids: Vec<i64> = remove.iter().map(|l| l.id).collect();
        self.delete_ids(&conn, &ids)?;
        Ok(locs)
    }

    fn replace_blocks_with_duration_bounds(
        &self,
        add: &[EncodedSpanBlock],
        duration_bounds: &[SpanDurationBounds],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        if add.len() != duration_bounds.len() {
            return Err(format!(
                "span block/duration metadata length mismatch: {} blocks, {} bounds",
                add.len(),
                duration_bounds.len()
            ));
        }
        let conn = Self::conn()?;
        let mut locations = Vec::with_capacity(add.len());
        for (block, bounds) in add.iter().zip(duration_bounds) {
            locations.push(self.insert_block(&conn, block, Some(*bounds))?);
        }
        on_committed(&locations);
        let ids: Vec<i64> = remove.iter().map(|location| location.id).collect();
        self.delete_ids(&conn, &ids)?;
        Ok(locations)
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

    /// Recovery: metadata for every persisted block (payloads
    /// untouched) so SpanBlockEngine::new can rebuild its index at
    /// xCreate/xConnect.
    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
        let conn = Self::conn()?;
        let invalid = conn
            .prepare_cached(&self.validate_duration_sql)
            .map_err(|error| format!("prepare duration-bound validation failed: {error}"))?
            .query_row([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .optional()
            .map_err(|error| format!("duration-bound validation failed: {error}"))?;
        if let Some((block_id, minimum, maximum)) = invalid {
            return Err(format!(
                "trace duration metadata is corrupt at block {block_id}: invalid duration bounds or orphaned row {minimum}..{maximum}"
            ));
        }
        let orphan = conn
            .prepare_cached(&self.validate_attribute_rows_sql)
            .map_err(|error| format!("prepare trace attribute bloom validation failed: {error}"))?
            .query_row([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .optional()
            .map_err(|error| format!("trace attribute bloom validation failed: {error}"))?;
        if let Some((scope, path, block_id)) = orphan {
            return Err(format!(
                "trace attribute bloom metadata is corrupt: orphaned {scope}:{path} row for block {block_id}"
            ));
        }
        let mut stmt = conn
            .prepare_cached(&self.scan_sql)
            .map_err(|e| format!("prepare block scan failed: {e}"))?;
        let rows = stmt
            .query_map([], |r| {
                let id: i64 = r.get(0)?;
                let duration_min: Option<i64> = r.get(5)?;
                let duration_max: Option<i64> = r.get(6)?;
                let invalid = match (duration_min, duration_max) {
                    (None, None) => None,
                    (Some(minimum), Some(maximum)) if minimum <= maximum => None,
                    (Some(minimum), Some(maximum)) => Some(format!(
                        "trace block {id} has invalid duration bounds: minimum {minimum} exceeds maximum {maximum}"
                    )),
                    _ => Some(format!(
                        "trace block {id} has incomplete duration bounds; minimum and maximum must both be NULL or both be present"
                    )),
                };
                if let Some(message) = invalid {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        5,
                        rusqlite::types::Type::Integer,
                        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, message)),
                    ));
                }
                Ok((
                    BlockMeta {
                        ts_min: r.get(1)?,
                        ts_max: r.get(2)?,
                        entry_count: r.get::<_, i64>(3)? as u32,
                        codec: r.get::<_, i64>(4)? as u8,
                    },
                    BlockLoc { id },
                ))
            })
            .map_err(|e| format!("block scan failed: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("block scan row failed: {e}"))?;
        Ok(rows)
    }

    fn blocks_missing_duration_bounds(&self) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        let conn = Self::conn()?;
        let mut statement = conn
            .prepare_cached(&self.missing_duration_sql)
            .map_err(|error| format!("prepare duration-bound scan failed: {error}"))?;
        Self::meta_rows(&mut statement, Vec::new(), "duration-bound scan")
    }

    fn update_duration_bounds(
        &self,
        updates: &[(BlockLoc, SpanDurationBounds)],
    ) -> Result<(), String> {
        let conn = Self::conn()?;
        self.ensure_storage_stats(&conn)?;
        let mut statement = conn
            .prepare_cached(&self.update_duration_sql)
            .map_err(|error| format!("prepare duration-bound update failed: {error}"))?;
        let mut added = 0_i64;
        for (location, bounds) in updates {
            let existed = conn
                .query_row(&self.duration_exists_sql, [location.id], |row| {
                    row.get::<_, bool>(0)
                })
                .map_err(|error| {
                    format!(
                        "read duration-bound accounting for block {} failed: {error}",
                        location.id
                    )
                })?;
            let changed = statement
                .execute(params![bounds.min_ns, bounds.max_ns, location.id])
                .map_err(|error| {
                    format!(
                        "duration-bound update for block {} failed: {error}",
                        location.id
                    )
                })?;
            if changed != 1 {
                return Err(format!(
                    "duration-bound update for block {} changed {changed} rows; expected 1",
                    location.id
                ));
            }
            if !existed {
                added += 1;
            }
        }
        self.adjust_storage_stats(&conn, [0, 0, 0, 0, added, 0, 0, 0, 0])?;
        Ok(())
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
        self.query_terms_with_duration_bounds(terms, ts_min, ts_max, i64::MIN, i64::MAX)
    }

    fn query_terms_with_duration_bounds(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
        duration_min_ns: i64,
        duration_max_ns: i64,
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
        let mut binds: Vec<Value> = Vec::with_capacity(4 + terms.len());
        binds.push(Value::Integer(ts_max));
        binds.push(Value::Integer(ts_min));
        binds.push(Value::Integer(duration_min_ns));
        binds.push(Value::Integer(duration_max_ns));
        for t in terms {
            binds.push(Value::Text(t.clone()));
        }

        let conn = Self::conn()?;
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
        self.query_trace_with_duration_bounds(trace_id, i64::MIN, i64::MAX, i64::MIN, i64::MAX)
    }

    fn query_trace_with_duration_bounds(
        &self,
        trace_id: &[u8; 16],
        ts_min: i64,
        ts_max: i64,
        duration_min_ns: i64,
        duration_max_ns: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.query_trace_sql)
            .map_err(|e| format!("prepare trace query failed: {e}"))?;
        Self::meta_rows(
            &mut stmt,
            vec![
                Value::Blob(trace_id.to_vec()),
                Value::Integer(ts_max),
                Value::Integer(ts_min),
                Value::Integer(duration_min_ns),
                Value::Integer(duration_max_ns),
            ],
            "trace query",
        )
    }

    fn filter_attribute_blocks(
        &self,
        filter: &SpanAttributeFilter,
        blocks: &[(BlockLoc, BlockMeta)],
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        if blocks.is_empty() {
            return Ok(Vec::new());
        }
        let conn = Self::conn()?;
        let mut retained = Vec::with_capacity(blocks.len());
        for chunk in blocks.chunks(ATTRIBUTE_BLOOM_QUERY_BLOCKS) {
            let placeholders = (0..chunk.len())
                .map(|position| format!("?{}", position + 3))
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT block_id,hash_version,bits,checksum \
                 FROM {} WHERE scope=?1 AND path=?2 AND block_id IN ({placeholders})",
                self.attribute_blooms_table
            );
            let mut binds = Vec::with_capacity(chunk.len() + 2);
            binds.push(Value::Text(filter.index().scope().name().to_owned()));
            binds.push(Value::Text(filter.index().path().to_owned()));
            binds.extend(
                chunk
                    .iter()
                    .map(|(location, _)| Value::Integer(location.id)),
            );
            let mut statement = conn
                .prepare_cached(&sql)
                .map_err(|error| format!("prepare trace attribute bloom query failed: {error}"))?;
            let rows = statement
                .query_map(params_from_iter(binds), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        (
                            row.get::<_, i64>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                            row.get::<_, Vec<u8>>(3)?,
                        ),
                    ))
                })
                .map_err(|error| format!("trace attribute bloom query failed: {error}"))?
                .collect::<Result<HashMap<_, _>, _>>()
                .map_err(|error| format!("trace attribute bloom row failed: {error}"))?;

            for (location, meta) in chunk {
                let Some((version, bits, checksum)) = rows.get(&location.id) else {
                    // Legacy or deliberately unindexed block: exact fallback.
                    retained.push((*location, *meta));
                    continue;
                };
                if *version != i64::from(SPAN_ATTRIBUTE_BLOOM_VERSION) {
                    return Err(format!(
                        "trace attribute bloom for block {} has version {version}; expected {}",
                        location.id, SPAN_ATTRIBUTE_BLOOM_VERSION
                    ));
                }
                validate_span_attribute_bloom(bits).map_err(|error| {
                    format!(
                        "trace attribute bloom for block {} is corrupt: {error}",
                        location.id
                    )
                })?;
                if checksum.as_slice() != span_attribute_bloom_checksum(bits).as_slice() {
                    return Err(format!(
                        "trace attribute bloom checksum mismatch for block {}",
                        location.id
                    ));
                }
                let bloom = SpanAttributeBloom {
                    index: filter.index().clone(),
                    bits: bits.clone(),
                };
                if bloom.might_contain(filter.scalar_json())? {
                    retained.push((*location, *meta));
                }
            }
        }
        Ok(retained)
    }

    fn query_term_values(&self, prefix: &str) -> Result<Option<Vec<String>>, String> {
        let conn = Self::conn()?;
        let sql = format!(
            "SELECT DISTINCT substr(term, length(?1) + 1) FROM {} \
             WHERE substr(term, 1, length(?1)) = ?1 ORDER BY 1",
            self.terms_table
        );
        let mut statement = conn
            .prepare_cached(&sql)
            .map_err(|error| format!("prepare trace term discovery failed: {error}"))?;
        let values = statement
            .query_map([prefix], |row| row.get::<_, String>(0))
            .map_err(|error| format!("execute trace term discovery failed: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("read trace term discovery failed: {error}"))?;
        Ok(Some(values))
    }

    fn check_cancelled(&self) -> Result<(), String> {
        Self::conn()?
            .query_row("SELECT 1", [], |_| Ok(()))
            .map_err(|error| format!("trace query cancellation checkpoint failed: {error}"))
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
