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
//! THREAD SAFETY. `ChunkStore` is `Send + Sync` (the engine may call it
//! from rayon worker threads), but a raw `*mut sqlite3` is neither. We
//! wrap the handle in a Mutex so all connection access is serialized:
//! lock → `Connection::from_handle` (borrow, does NOT close on drop) →
//! run pre-formatted SQL → unlock. Serializing store access is correct
//! and fine for the POC (SQLite would serialize the connection anyway).
//!
//! CAUTION that still stands: if the host thread is blocked inside a vtab
//! callback it holds SQLite's per-connection mutex (serialized threading
//! mode), so a rayon worker calling into this store would block on that
//! mutex until the callback returns — a deadlock if the callback is
//! waiting on the workers. The vtab layer therefore avoids the engine's
//! rayon-parallel query paths (see metrics_vtab.rs::filter).

use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::vtab::escape_double_quote;
use rusqlite::{ffi, params, Connection, OptionalExtension};
use timeless_core::{ChunkBytes, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, StoredChunk};

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
pub(crate) fn ddl(table: &str) -> String {
    let t = escape_double_quote(table);
    format!(
        r#"
CREATE TABLE IF NOT EXISTS "{t}_chunks" (
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
CREATE INDEX IF NOT EXISTS "{t}_chunks_series_ts" ON "{t}_chunks"(series_id, ts_min);
CREATE TABLE IF NOT EXISTS "{t}_meta" (k TEXT PRIMARY KEY, v BLOB);
"#
    )
}

/// Statements to remove the shadow tables again (vtab xDestroy).
pub(crate) fn drop_ddl(table: &str) -> String {
    let t = escape_double_quote(table);
    format!(r#"DROP TABLE IF EXISTS "{t}_chunks"; DROP TABLE IF EXISTS "{t}_meta";"#)
}

/// Newtype around the raw host-connection pointer so we can promise the
/// compiler it may cross threads. The promise is sound because every use
/// goes through the Mutex below AND the host SQLite library is built in
/// serialized threading mode (the default), which allows one connection
/// to be used from multiple threads.
///
/// pub(crate): shadow_block_store.rs (the logs BlockStore backend) wraps
/// the same host connection with the same Mutex discipline.
pub(crate) struct HostHandle(pub(crate) *mut ffi::sqlite3);
unsafe impl Send for HostHandle {}

pub(crate) struct ShadowTableStore {
    host: Mutex<HostHandle>,
    // Pre-formatted SQL, built once in the constructor so the trait
    // methods never allocate query strings on the hot path. (The table
    // name is baked in — SQLite cannot parameterize identifiers.)
    insert_sql: String,
    read_sql: String,
    scan_sql: String,
    stats_sql: String,
    save_registry_sql: String,
    load_registry_sql: String,
    /// "DELETE FROM ... WHERE id IN (" — completed per delete_chunks call
    /// with the actual rowid list (rowids are i64s we produced ourselves,
    /// so inlining them is injection-safe).
    delete_prefix: String,
}

impl ShadowTableStore {
    pub(crate) fn new(db: *mut ffi::sqlite3, table: &str) -> Self {
        let t = escape_double_quote(table);
        let chunks = format!("\"{t}_chunks\"");
        let meta = format!("\"{t}_meta\"");
        ShadowTableStore {
            host: Mutex::new(HostHandle(db)),
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
            delete_prefix: format!("DELETE FROM {chunks} WHERE id IN ("),
        }
    }

    /// Lock the handle for the duration of one store operation. A poisoned
    /// mutex (a panic while locked) still yields the guard — matching the
    /// lock style used across timeless-core.
    fn lock(&self) -> MutexGuard<'_, HostHandle> {
        self.host.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Borrow the host connection. `Connection::from_handle` wraps the raw
    /// pointer WITHOUT taking ownership — dropping the returned Connection
    /// does not close the user's database (the FTS5 re-entrancy trick,
    /// same as the spike). Note this also means the prepared-statement
    /// cache is per-borrow; acceptable at POC chunk granularity.
    fn conn(guard: &MutexGuard<'_, HostHandle>) -> Result<Connection, String> {
        unsafe { Connection::from_handle(guard.0) }
            .map_err(|e| format!("from_handle failed: {e}"))
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
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        self.insert_chunks(&conn, chunks)
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
        let guard = self.lock();
        let conn = Self::conn(&guard)?;

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
        Ok(locs)
    }

    fn read_chunk(&self, loc: &ChunkLoc) -> Result<ChunkBytes, String> {
        let ChunkLoc::Row { rowid } = loc else {
            return Err(format!("ShadowTableStore cannot read {loc:?}"));
        };
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
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
        let guard = self.lock();
        let conn = match Self::conn(&guard) {
            Ok(c) => c,
            Err(e) => {
                errors.push(e);
                return errors;
            }
        };
        let sql = format!("{}{})", self.delete_prefix, ids.join(","));
        if let Err(e) = conn.execute(&sql, []) {
            errors.push(format!("batched chunk delete failed: {e}"));
        }
        errors
    }

    /// Recovery: enumerate every persisted chunk's metadata so the engine
    /// can rebuild its in-memory index (Engine::with_store → rebuild_index
    /// calls this at every xCreate/xConnect).
    fn scan(&self) -> Result<Vec<StoredChunk>, String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
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
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        let mut stmt = conn
            .prepare_cached(&self.save_registry_sql)
            .map_err(|e| format!("prepare registry save failed: {e}"))?;
        stmt.execute([bytes])
            .map_err(|e| format!("registry save failed: {e}"))?;
        Ok(())
    }

    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        let guard = self.lock();
        let conn = Self::conn(&guard)?;
        let mut stmt = conn
            .prepare_cached(&self.load_registry_sql)
            .map_err(|e| format!("prepare registry load failed: {e}"))?;
        stmt.query_row([], |r| r.get::<_, Vec<u8>>(0))
            .optional()
            .map_err(|e| format!("registry load failed: {e}"))
    }

    /// (total_bytes, row_count) for Engine::info(). Infallible signature,
    /// so errors degrade to zeros. See stats_sql comment: full aggregate
    /// now, incrementally-maintained counter later.
    fn storage_stats(&self) -> (u64, usize) {
        let guard = self.lock();
        let Ok(conn) = Self::conn(&guard) else {
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
