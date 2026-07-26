//! ShadowTableStore: a `timeless_core::ChunkStore` backend that persists
//! chunks into "shadow tables" on the HOST SQLite connection — the same
//! database file the user's vtab lives in. This is the FTS5 storage model:
//! the vtab is a facade, the real bytes live in ordinary tables named
//! `<vtab>_chunks` / `<vtab>_meta` next to it.
//!
//! Division of labor across the seam (see timeless-core/src/store/mod.rs):
//! the ENGINE owns encoding/decoding and the in-memory chunk index; this
//! store owns bytes-at-rest, addressed by `ChunkLoc::Row { rowid }`.
//!
//! Why this file never opens a transaction: every store method here runs
//! re-entrantly inside a vtab callback (xUpdate/xFilter/...), which means
//! the statements already execute inside the host's enclosing transaction
//! (or the implicit autocommit transaction of the triggering statement).
//! A `BEGIN` here would either fail or fight the host — the enclosing
//! transaction IS our atomicity, which is also why `replace_chunks` needs
//! no manifest/rename machinery like FsStore does.
//!
//! CONNECTION ROUTING (R4). This store holds NO connection at all —
//! only pre-formatted SQL strings. The engine it serves is SHARED
//! across every connection of the pool (see shared.rs), so "the host
//! connection" is not a property of the store; it is a property of
//! whichever vtab callback is currently executing. Each method fetches
//! the CALLING connection from the thread-local binding
//! (shared::current_conn) that the vtab callback established via
//! DbGuard. That guarantees:
//!   - store SQL always runs in the caller's transaction context
//!     (connection B's insert commits/rolls back with B's txn, never
//!     A's), and
//!   - no cross-connection re-entry: we only ever touch the connection
//!     whose mutex the current thread already holds.
//! A call with no binding is a hard error — the permanent guard
//! against the old rayon trap (a worker thread reaching the store used
//! to deadlock on the host connection's mutex; now it gets a message).
//!
//! With the raw handle gone, this struct is Strings-only and the
//! compiler derives Send + Sync — the `unsafe impl Send for HostHandle`
//! that used to live here is deleted, not relocated.

use std::sync::Arc;

use rusqlite::{params, Connection, OptionalExtension};
use timeless_core::{
    ChunkBytes, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, ResolvedSeries, StoredChunk,
    StoredSeries,
};

use crate::{shared, sql_ident};

/// Shadow-table DDL for a vtab named `table`. The vtab layer executes this
/// in xCreate (the store assumes the tables exist).
///
/// Schema notes:
/// - `id INTEGER PRIMARY KEY` is EXPLICIT, not a bare rowid: bare rowids
///   can be renumbered by VACUUM, and the engine's index holds rowids in
///   memory — a silent renumber would corrupt every ChunkLoc.
/// - ts/val payloads are TWO blob columns (no concat on the write path;
///   read_chunk stitches them into the one contiguous buffer + ranges
///   shape that `ChunkBytes` wants).
/// - `resolution INTEGER DEFAULT 0` is the v2 rollup-ladder column from
///   PLAN.md "Pruning & retention" — costs one column now, saves a schema
///   migration later. 0 = raw resolution.
pub(crate) fn ddl(database: &str, table: &str) -> String {
    let chunks = sql_ident::qualified_shadow(database, table, "chunks");
    let chunks_local = sql_ident::quoted_shadow(table, "chunks");
    let chunks_index = sql_ident::qualified_shadow(database, table, "chunks_series_ts");
    let meta = sql_ident::qualified_shadow(database, table, "meta");
    let series = sql_ident::qualified_shadow(database, table, "series");
    format!(
        r#"
CREATE TABLE IF NOT EXISTS {chunks} (
  id          INTEGER PRIMARY KEY,
  series_id   INTEGER NOT NULL,
  ts_min      INTEGER NOT NULL,
  ts_max      INTEGER NOT NULL,
  point_count INTEGER NOT NULL,
  min_val     REAL NOT NULL,
  max_val     REAL NOT NULL,
  sum_val     REAL NOT NULL,
  encoding    INTEGER NOT NULL,
  resolution  INTEGER NOT NULL DEFAULT 0,
  ts_data     BLOB NOT NULL,
  val_data    BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS {chunks_index} ON {chunks_local}(series_id, ts_min);
CREATE TABLE IF NOT EXISTS {meta} (k TEXT PRIMARY KEY, v BLOB);
CREATE TABLE IF NOT EXISTS {series} (
  id               INTEGER PRIMARY KEY,
  name             TEXT NOT NULL,
  canonical_labels BLOB NOT NULL,
  UNIQUE(name, canonical_labels)
);
"#
    )
}

/// Catalog-only DDL used by xConnect to upgrade databases created before the
/// normalized series table existed.
pub(crate) fn series_ddl(database: &str, table: &str) -> String {
    let series = sql_ident::qualified_shadow(database, table, "series");
    format!(
        r#"
CREATE TABLE IF NOT EXISTS {series} (
  id               INTEGER PRIMARY KEY,
  name             TEXT NOT NULL,
  canonical_labels BLOB NOT NULL,
  UNIQUE(name, canonical_labels)
);
"#
    )
}

/// Statements to remove the shadow tables again (vtab xDestroy).
pub(crate) fn drop_ddl(database: &str, table: &str) -> String {
    let chunks = sql_ident::qualified_shadow(database, table, "chunks");
    let meta = sql_ident::qualified_shadow(database, table, "meta");
    let series = sql_ident::qualified_shadow(database, table, "series");
    format!(
        r#"DROP TABLE IF EXISTS {chunks};
DROP TABLE IF EXISTS {meta};
DROP TABLE IF EXISTS {series};"#
    )
}

pub(crate) struct ShadowTableStore {
    // Pre-formatted SQL, built once in the constructor so the trait
    // methods never allocate query strings on the hot path. (The table
    // name is baked in — SQLite cannot parameterize identifiers.)
    insert_sql: String,
    read_sql: String,
    scan_sql: String,
    stats_sql: String,
    save_registry_sql: String,
    load_registry_sql: String,
    load_series_sql: String,
    insert_series_sql: String,
    select_series_sql: String,
    migrate_series_sql: String,
    /// "DELETE FROM ... WHERE id IN (" — completed per delete_chunks call
    /// with the actual rowid list (rowids are i64s we produced ourselves,
    /// so inlining them is injection-safe).
    delete_prefix: String,
    /// Qualified `_series` identifier, kept for the bulk resolver whose
    /// multi-row VALUES statements are sized per call and so cannot be
    /// pre-formatted like the fixed-arity SQL above.
    series_ident: String,
    /// One-row read backing catalog_generation(): (max series id, chunk
    /// generation counter). See the trait docs for the soundness argument.
    generation_sql: String,
    /// Upsert bumping the chunk half of the generation. Executed once per
    /// chunk-mutating call, inside the caller's transaction, so other
    /// processes observe the bump if and only if they observe the change.
    bump_chunk_gen_sql: String,
}

impl ShadowTableStore {
    pub(crate) fn new(database: &str, table: &str) -> Self {
        let chunks = sql_ident::qualified_shadow(database, table, "chunks");
        let meta = sql_ident::qualified_shadow(database, table, "meta");
        let series = sql_ident::qualified_shadow(database, table, "series");
        ShadowTableStore {
            insert_sql: format!(
                "INSERT INTO {chunks} (series_id, ts_min, ts_max, point_count, \
                 min_val, max_val, sum_val, encoding, resolution, ts_data, val_data) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10)"
            ),
            read_sql: format!("SELECT ts_data, val_data FROM {chunks} WHERE id = ?1"),
            // scan() deliberately does NOT select the blob columns: it runs
            // at every reopen and only needs metadata for the index.
            scan_sql: format!(
                "SELECT id, series_id, ts_min, ts_max, point_count, \
                 min_val, max_val, sum_val, encoding FROM {chunks}"
            ),
            // POC accounting: a full aggregate over the table. Fine while
            // tables are small; should become an incrementally-maintained
            // counter in _meta once ingest volume matters.
            stats_sql: format!(
                "SELECT COUNT(*), COALESCE(SUM(length(ts_data) + length(val_data)), 0) \
                 FROM {chunks}"
            ),
            save_registry_sql: format!(
                "INSERT OR REPLACE INTO {meta} (k, v) VALUES ('series_registry', ?1)"
            ),
            load_registry_sql: format!("SELECT v FROM {meta} WHERE k = 'series_registry'"),
            load_series_sql: format!("SELECT id, name, canonical_labels FROM {series} ORDER BY id"),
            insert_series_sql: format!(
                "INSERT INTO {series}(name, canonical_labels) VALUES(?1, ?2) \
                 ON CONFLICT(name, canonical_labels) DO NOTHING"
            ),
            select_series_sql: format!(
                "SELECT id FROM {series} WHERE name = ?1 AND canonical_labels = ?2"
            ),
            migrate_series_sql: format!(
                "INSERT OR IGNORE INTO {series}(id, name, canonical_labels) \
                 VALUES(?1, ?2, ?3)"
            ),
            delete_prefix: format!("DELETE FROM {chunks} WHERE id IN ("),
            series_ident: series.clone(),
            generation_sql: format!(
                "SELECT (SELECT COALESCE(MAX(id), 0) FROM {series}), \
                 COALESCE((SELECT v FROM {meta} WHERE k = 'chunk_gen'), 0)"
            ),
            bump_chunk_gen_sql: format!(
                "INSERT INTO {meta} (k, v) VALUES ('chunk_gen', 1) \
                 ON CONFLICT(k) DO UPDATE SET v = v + 1"
            ),
        }
    }

    /// Bump the chunk generation inside the caller's transaction. Called
    /// by every chunk-mutating trait method; failing to bump would let a
    /// stale reader skip a refresh, so errors propagate.
    fn bump_chunk_generation(&self, conn: &Connection) -> Result<(), String> {
        conn.prepare_cached(&self.bump_chunk_gen_sql)
            .map_err(|e| format!("prepare chunk generation bump failed: {e}"))?
            .execute([])
            .map_err(|e| format!("chunk generation bump failed: {e}"))?;
        Ok(())
    }

    /// Borrow the CALLING connection for one store operation — the
    /// thread-local binding established by the current vtab callback's
    /// DbGuard (see module docs and shared.rs). from_handle borrows
    /// without owning; the per-borrow statement cache is acceptable at
    /// chunk granularity (unchanged from the pre-R4 pattern).
    fn conn() -> Result<Connection, String> {
        shared::current_conn()
    }

    fn encode_labels(labels: &[(String, String)]) -> Result<Vec<u8>, String> {
        let count = u32::try_from(labels.len())
            .map_err(|_| "too many labels to encode in series catalog".to_string())?;
        let mut out = Vec::new();
        out.extend_from_slice(&count.to_be_bytes());
        for (key, value) in labels {
            let key = key.as_bytes();
            let value = value.as_bytes();
            let key_len = u32::try_from(key.len())
                .map_err(|_| "series label key is too large".to_string())?;
            let value_len = u32::try_from(value.len())
                .map_err(|_| "series label value is too large".to_string())?;
            out.extend_from_slice(&key_len.to_be_bytes());
            out.extend_from_slice(key);
            out.extend_from_slice(&value_len.to_be_bytes());
            out.extend_from_slice(value);
        }
        Ok(out)
    }

    fn decode_labels(data: &[u8]) -> Result<Vec<(String, String)>, String> {
        fn take_u32(data: &[u8], pos: &mut usize, what: &str) -> Result<usize, String> {
            let end = pos
                .checked_add(4)
                .ok_or_else(|| format!("series label catalog overflow reading {what}"))?;
            let bytes = data
                .get(*pos..end)
                .ok_or_else(|| format!("truncated series label catalog reading {what}"))?;
            *pos = end;
            Ok(u32::from_be_bytes(bytes.try_into().unwrap()) as usize)
        }

        fn take_string(
            data: &[u8],
            pos: &mut usize,
            len: usize,
            what: &str,
        ) -> Result<String, String> {
            let end = pos
                .checked_add(len)
                .ok_or_else(|| format!("series label catalog overflow reading {what}"))?;
            let bytes = data
                .get(*pos..end)
                .ok_or_else(|| format!("truncated series label catalog reading {what}"))?;
            *pos = end;
            String::from_utf8(bytes.to_vec())
                .map_err(|e| format!("invalid UTF-8 in series catalog {what}: {e}"))
        }

        let mut pos = 0;
        let count = take_u32(data, &mut pos, "label count")?;
        let max_possible = data.len().saturating_sub(pos) / 8;
        if count > max_possible {
            return Err(format!(
                "series label catalog count {count} exceeds payload capacity {max_possible}"
            ));
        }
        let mut labels = Vec::with_capacity(count);
        let mut previous_key: Option<String> = None;
        for index in 0..count {
            let key_len = take_u32(data, &mut pos, "label key length")?;
            let key = take_string(data, &mut pos, key_len, &format!("label {index} key"))?;
            if previous_key
                .as_deref()
                .is_some_and(|previous| previous >= key.as_str())
            {
                return Err(format!(
                    "series labels are not in strict canonical order at key {key:?}"
                ));
            }
            let value_len = take_u32(data, &mut pos, "label value length")?;
            let value = take_string(data, &mut pos, value_len, &format!("label {index} value"))?;
            previous_key = Some(key.clone());
            labels.push((key, value));
        }
        if pos != data.len() {
            return Err(format!(
                "series label catalog has {} trailing bytes",
                data.len() - pos
            ));
        }
        Ok(labels)
    }

    /// INSERT one row per chunk; shared by put_chunks and replace_chunks.
    fn insert_chunks(
        &self,
        conn: &Connection,
        chunks: &[EncodedChunk],
    ) -> Result<Vec<ChunkLoc>, String> {
        let mut stmt = conn
            .prepare_cached(&self.insert_sql)
            .map_err(|e| format!("prepare chunk insert failed: {e}"))?;
        let mut locs = Vec::with_capacity(chunks.len());
        for cp in chunks {
            stmt.execute(params![
                cp.series_id,
                cp.min_ts,
                cp.max_ts,
                cp.point_count,
                cp.min_val,
                cp.max_val,
                cp.sum_val,
                cp.encoding,
                &cp.ts_bytes,
                &cp.val_bytes,
            ])
            .map_err(|e| format!("chunk insert for series {} failed: {e}", cp.series_id))?;
            // `id INTEGER PRIMARY KEY` aliases the rowid, so
            // last_insert_rowid() IS the id we just wrote.
            locs.push(ChunkLoc::Row {
                rowid: conn.last_insert_rowid(),
            });
        }
        Ok(locs)
    }
}

impl ChunkStore for ShadowTableStore {
    fn put_chunks(&self, chunks: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        let conn = Self::conn()?;
        let locs = self.insert_chunks(&conn, chunks)?;
        self.bump_chunk_generation(&conn)?;
        Ok(locs)
    }

    /// Compaction swap. Unlike FsStore there is no pending/manifest/rename
    /// dance: the inserts and deletes all happen inside the host's
    /// enclosing SQLite transaction, so a crash either rolls the whole
    /// swap back or commits it whole — exactly the "never lose both"
    /// contract, for free. `on_committed` fires after the inserts (new
    /// rows readable through this same connection/transaction) and before
    /// the deletes, so the engine can swap its index without a window
    /// where queries could hit a removed row.
    fn replace_chunks(
        &self,
        add: &[EncodedChunk],
        remove: &[ChunkLoc],
        on_committed: &mut dyn FnMut(&[ChunkLoc]),
    ) -> Result<Vec<ChunkLoc>, String> {
        let conn = Self::conn()?;

        let locs = self.insert_chunks(&conn, add)?;
        on_committed(&locs);

        let mut ids = Vec::with_capacity(remove.len());
        for loc in remove {
            match loc {
                ChunkLoc::Row { rowid } => ids.push(rowid.to_string()),
                other => return Err(format!("ShadowTableStore cannot remove {other:?}")),
            }
        }
        if !ids.is_empty() {
            let sql = format!("{}{})", self.delete_prefix, ids.join(","));
            conn.execute(&sql, [])
                .map_err(|e| format!("compaction delete failed: {e}"))?;
        }
        self.bump_chunk_generation(&conn)?;
        Ok(locs)
    }

    fn read_chunk(&self, loc: &ChunkLoc) -> Result<ChunkBytes, String> {
        let ChunkLoc::Row { rowid } = loc else {
            return Err(format!("ShadowTableStore cannot read {loc:?}"));
        };
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.read_sql)
            .map_err(|e| format!("prepare chunk read failed: {e}"))?;
        let (ts, val): (Vec<u8>, Vec<u8>) = stmt
            .query_row([rowid], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(|e| format!("chunk row {rowid} read failed: {e}"))?;

        // ChunkBytes wants ONE contiguous buffer plus ts/val ranges (fs
        // chunks are slices of a cached whole file). We store the payloads
        // as two columns, so stitch them together here on the read path.
        let ts_len = ts.len();
        let val_len = val.len();
        let mut buf = ts;
        buf.extend_from_slice(&val);
        Ok(ChunkBytes {
            data: Arc::new(buf),
            ts_range: 0..ts_len,
            val_range: ts_len..ts_len + val_len,
        })
    }

    /// Batched delete. Per-loc error strings mirror FsStore's contract;
    /// a rowid that no longer exists is simply not matched by the IN list
    /// (missing units are non-fatal per the trait, and SQLite gives us no
    /// cheap per-row missing report from a batched DELETE anyway).
    fn delete_chunks(&self, locs: &[ChunkLoc]) -> Vec<String> {
        let mut errors = Vec::new();
        let mut ids = Vec::with_capacity(locs.len());
        for loc in locs {
            match loc {
                ChunkLoc::Row { rowid } => ids.push(rowid.to_string()),
                other => errors.push(format!("ShadowTableStore cannot delete {other:?}")),
            }
        }
        if ids.is_empty() {
            return errors;
        }
        let conn = match Self::conn() {
            Ok(c) => c,
            Err(e) => {
                errors.push(e);
                return errors;
            }
        };
        let sql = format!("{}{})", self.delete_prefix, ids.join(","));
        if let Err(e) = conn.execute(&sql, []) {
            errors.push(format!("batched chunk delete failed: {e}"));
        } else if let Err(e) = self.bump_chunk_generation(&conn) {
            errors.push(e);
        }
        errors
    }

    /// Recovery: enumerate every persisted chunk's metadata so the engine
    /// can rebuild its in-memory index (Engine::with_store → rebuild_index
    /// calls this at every xCreate/xConnect).
    fn scan(&self) -> Result<Vec<StoredChunk>, String> {
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.scan_sql)
            .map_err(|e| format!("prepare chunk scan failed: {e}"))?;
        let rows = stmt
            .query_map([], |r| {
                Ok(StoredChunk {
                    series_id: r.get(1)?,
                    meta: ChunkMeta {
                        min_ts: r.get(2)?,
                        max_ts: r.get(3)?,
                        point_count: r.get::<_, i64>(4)? as u32,
                        min_val: r.get(5)?,
                        max_val: r.get(6)?,
                        sum_val: r.get(7)?,
                        loc: ChunkLoc::Row { rowid: r.get(0)? },
                        encoding: r.get::<_, i64>(8)? as u8,
                    },
                })
            })
            .map_err(|e| format!("chunk scan failed: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("chunk scan row failed: {e}"))?;
        Ok(rows)
    }

    fn save_registry(&self, bytes: &[u8]) -> Result<(), String> {
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.save_registry_sql)
            .map_err(|e| format!("prepare registry save failed: {e}"))?;
        stmt.execute([bytes])
            .map_err(|e| format!("registry save failed: {e}"))?;
        Ok(())
    }

    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.load_registry_sql)
            .map_err(|e| format!("prepare registry load failed: {e}"))?;
        stmt.query_row([], |r| r.get::<_, Vec<u8>>(0))
            .optional()
            .map_err(|e| format!("registry load failed: {e}"))
    }

    fn has_authoritative_series(&self) -> bool {
        true
    }

    /// (max series id, chunk generation). The series half needs no
    /// write-side bump because committed `_series` rows are append-only;
    /// the chunk half is the `_meta` counter the mutating methods bump.
    /// One cached single-row SELECT — cheap enough for every query.
    fn catalog_generation(&self) -> Result<Option<(i64, i64)>, String> {
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.generation_sql)
            .map_err(|e| format!("prepare catalog generation read failed: {e}"))?;
        let gen = stmt
            .query_row([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
            .map_err(|e| format!("catalog generation read failed: {e}"))?;
        Ok(Some(gen))
    }

    fn load_series(&self) -> Result<Vec<StoredSeries>, String> {
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.load_series_sql)
            .map_err(|e| format!("prepare series catalog load failed: {e}"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(|e| format!("series catalog load failed: {e}"))?;

        let mut series = Vec::new();
        for row in rows {
            let (id, name, labels) = row.map_err(|e| format!("series catalog row failed: {e}"))?;
            series.push(StoredSeries {
                id,
                name,
                labels: Self::decode_labels(&labels)
                    .map_err(|e| format!("series {id} labels are invalid: {e}"))?,
            });
        }
        Ok(series)
    }

    fn resolve_series(
        &self,
        name: &str,
        labels: &[(String, String)],
    ) -> Result<ResolvedSeries, String> {
        let conn = Self::conn()?;
        let labels = Self::encode_labels(labels)?;
        let created = conn
            .prepare_cached(&self.insert_series_sql)
            .map_err(|e| format!("prepare series insert failed: {e}"))?
            .execute(params![name, &labels])
            .map_err(|e| format!("series insert failed: {e}"))?
            > 0;
        let id = conn
            .prepare_cached(&self.select_series_sql)
            .map_err(|e| format!("prepare series resolution failed: {e}"))?
            .query_row(params![name, &labels], |row| row.get(0))
            .map_err(|e| format!("series resolution failed: {e}"))?;
        Ok(ResolvedSeries { id, created })
    }

    /// Bulk resolve: one multi-row INSERT (RETURNING the ids THIS call
    /// created, so `created` stays exact under cross-process races) plus
    /// one VALUES-join SELECT per chunk — instead of two statements and a
    /// lock cycle per series. Everything rides the caller's transaction,
    /// same as resolve_series.
    fn resolve_series_bulk(
        &self,
        entries: &[(&str, Vec<(String, String)>)],
    ) -> Result<Vec<ResolvedSeries>, String> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let conn = Self::conn()?;
        let encoded: Vec<Vec<u8>> = entries
            .iter()
            .map(|(_, labels)| Self::encode_labels(labels))
            .collect::<Result<_, _>>()?;

        // Stay far below SQLITE_MAX_VARIABLE_NUMBER (3 params/row on the
        // wider statement).
        const CHUNK: usize = 4000;
        let series = &self.series_ident;
        let mut out = Vec::with_capacity(entries.len());
        let mut start = 0;
        while start < entries.len() {
            let end = (start + CHUNK).min(entries.len());
            let rows = end - start;

            let mut insert_sql = format!("INSERT INTO {series}(name, canonical_labels) VALUES ");
            for i in 0..rows {
                if i > 0 {
                    insert_sql.push(',');
                }
                insert_sql.push_str("(?,?)");
            }
            insert_sql.push_str(" ON CONFLICT(name, canonical_labels) DO NOTHING RETURNING id");
            let mut created_ids = std::collections::HashSet::new();
            {
                let mut stmt = conn
                    .prepare(&insert_sql)
                    .map_err(|e| format!("prepare bulk series insert failed: {e}"))?;
                let params = rusqlite::params_from_iter((start..end).flat_map(|i| {
                    [
                        rusqlite::types::Value::Text(entries[i].0.to_owned()),
                        rusqlite::types::Value::Blob(encoded[i].clone()),
                    ]
                }));
                let mut returned = stmt
                    .query(params)
                    .map_err(|e| format!("bulk series insert failed: {e}"))?;
                while let Some(row) = returned
                    .next()
                    .map_err(|e| format!("bulk series insert row failed: {e}"))?
                {
                    created_ids.insert(
                        row.get::<_, i64>(0)
                            .map_err(|e| format!("bulk series insert id failed: {e}"))?,
                    );
                }
            }

            // Ordinals are literals we generate, not user input.
            let mut select_sql = String::from("WITH req(ord, name, labels) AS (VALUES ");
            for i in 0..rows {
                if i > 0 {
                    select_sql.push(',');
                }
                select_sql.push_str(&format!("({i},?,?)"));
            }
            select_sql.push_str(&format!(
                ") SELECT req.ord, s.id FROM req JOIN {series} s \
                 ON s.name = req.name AND s.canonical_labels = req.labels \
                 ORDER BY req.ord"
            ));
            let mut stmt = conn
                .prepare(&select_sql)
                .map_err(|e| format!("prepare bulk series resolution failed: {e}"))?;
            let params = rusqlite::params_from_iter((start..end).flat_map(|i| {
                [
                    rusqlite::types::Value::Text(entries[i].0.to_owned()),
                    rusqlite::types::Value::Blob(encoded[i].clone()),
                ]
            }));
            let resolved: Vec<(i64, i64)> = stmt
                .query_map(params, |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(|e| format!("bulk series resolution failed: {e}"))?
                .collect::<Result<_, _>>()
                .map_err(|e| format!("bulk series resolution row failed: {e}"))?;
            if resolved.len() != rows {
                return Err(format!(
                    "bulk series resolution returned {} of {} rows",
                    resolved.len(),
                    rows
                ));
            }
            for (ord, id) in resolved {
                if ord as usize != out.len() - start {
                    return Err(format!("bulk series resolution ordinal {ord} out of order"));
                }
                out.push(ResolvedSeries {
                    id,
                    created: created_ids.contains(&id),
                });
            }
            start = end;
        }
        Ok(out)
    }

    fn migrate_series(&self, series: &[StoredSeries]) -> Result<(), String> {
        let conn = Self::conn()?;
        let mut stmt = conn
            .prepare_cached(&self.migrate_series_sql)
            .map_err(|e| format!("prepare legacy series migration failed: {e}"))?;
        for item in series {
            let labels = Self::encode_labels(&item.labels)?;
            stmt.execute(params![item.id, &item.name, labels])
                .map_err(|e| format!("legacy series {} migration failed: {e}", item.id))?;
        }
        Ok(())
    }

    /// (total_bytes, row_count) for Engine::info(). Infallible signature,
    /// so errors degrade to zeros. See stats_sql comment: full aggregate
    /// now, incrementally-maintained counter later.
    fn storage_stats(&self) -> (u64, usize) {
        let Ok(conn) = Self::conn() else {
            return (0, 0);
        };
        conn.query_row(&self.stats_sql, [], |r| {
            Ok((r.get::<_, i64>(1)? as u64, r.get::<_, i64>(0)? as usize))
        })
        .unwrap_or((0, 0))
    }

    /// No backend cache to sweep — SQLite's page cache does this job.
    fn sweep_cache(&self) {}
}
