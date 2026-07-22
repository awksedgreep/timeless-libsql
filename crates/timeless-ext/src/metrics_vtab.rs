//! timeless_metrics: the real writable vtab, modeled on the spike but
//! backed by a full timeless-core Engine persisting through
//! ShadowTableStore into `<table>_chunks` / `<table>_meta` on the host db.
//!
//! Exposed schema (declared at runtime because the hidden command column
//! is named after the table — the FTS5 command idiom):
//!
//!   CREATE TABLE x(name TEXT, ts INTEGER, value REAL, labels TEXT,
//!                  "<table>" HIDDEN)
//!
//! Write path:  INSERT INTO metrics(name, ts, value, labels) VALUES (...)
//!              → resolve series → in-memory partition buffer (Tier 1).
//! Batch path:  INSERT INTO metrics(metrics) VALUES (:blob) — Tier 2.
//!              The hidden column is overloaded by TYPE: TEXT values are
//!              commands (below), BLOB values are batch-blob-v0 ingest
//!              batches (see PLAN.md "Batch blob format v0"). This is
//!              unambiguous — commands stay TEXT, batches are BLOB — and
//!              needs zero schema change. Durability semantics are
//!              IDENTICAL to Tier 1: points land in the same engine
//!              buffers and become durable at the same 'flush'.
//! Commands:    INSERT INTO metrics(metrics) VALUES ('flush' | 'compact'
//!              | 'prune:<unix_ts>') — the FTS5 idiom: an insert that sets
//!              only the hidden column runs maintenance instead of
//!              storing a row.
//! Read path:   buffered points and flushed chunks are merged by the
//!              engine, so data is queryable immediately after INSERT and
//!              durable after 'flush'.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
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
use timeless_core::{Engine, Labels};

use crate::shadow_store::{self, ShadowTableStore};

/// Register the "timeless_metrics" module on a freshly-loaded connection.
pub(crate) fn register(db: &Connection) -> Result<()> {
    const MODULE: Module<MetricsTab> = Module::update_module_with_tx();
    db.create_module(c"timeless_metrics", &MODULE, None::<()>)
}

/// Engine parameters for the POC (see PLAN.md Session 3).
const FLUSH_THRESHOLD: usize = 4096; // points per series before auto-queue
const MIN_FLUSH_SIZE: usize = 0; // flush everything, however small
const COMPRESSION_LEVEL: usize = 8; // pco level
const MEMORY_BUDGET: usize = 256 * 1024 * 1024; // 256 MiB of buffers
const DEFER_COMPRESSION: bool = false; // compress at flush, not later

/// Map an engine error String into the vtab error type SQLite surfaces
/// to the user (rusqlite renders ModuleError's message verbatim).
fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

// ---------------------------------------------------------------------------
// The virtual table
// ---------------------------------------------------------------------------

/// One instance per CREATE VIRTUAL TABLE / per re-connect. `#[repr(C)]` +
/// `base` first is mandatory: SQLite treats a pointer to this struct as a
/// pointer to sqlite3_vtab (C-style inheritance).
#[repr(C)]
pub struct MetricsTab {
    base: ffi::sqlite3_vtab,
    /// Raw handle to the HOST connection, kept for xDestroy's DDL.
    db: *mut ffi::sqlite3,
    /// The vtab's own name — needed to drop its shadow tables.
    table_name: String,
    /// The whole timeless-core engine, chunk-persisting into shadow
    /// tables via ShadowTableStore. Arc so cursors can hold a reference
    /// without lifetime gymnastics.
    engine: Arc<Engine>,
    /// Synthetic rowid source for inserts (see insert()).
    rowid_counter: i64,
}

impl MetricsTab {
    fn connect_create(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        table_name: &[u8],
        _args: &[&[u8]],
        is_create: bool,
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let table = String::from_utf8_lossy(table_name).into_owned();
        let handle = unsafe { db.handle() };

        if is_create {
            // Re-entrant SQL against the host connection (the FTS5 trick
            // proven by the spike): from_handle borrows without owning.
            let host = unsafe { Connection::from_handle(handle) }?;

            // Retention plan (PLAN.md "Pruning & retention"): incremental
            // auto-vacuum lets maintenance return freed pages to the OS in
            // small slices instead of a full VACUUM rewrite. The pragma
            // only takes effect if the database has no pages yet (it
            // changes the file format), so on a non-empty db it is a
            // silent no-op — hence: attempt and ignore errors.
            let _ = host.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;");

            host.execute_batch(&shadow_store::ddl(&table))?;
        }
        // xConnect: the shadow tables already exist in the reopened db.

        // Engine::with_store performs recovery itself: it loads the series
        // registry via store.load_registry() and rebuilds the chunk index
        // via store.scan() — both re-entrant SELECTs on the host
        // connection, which is safe here because THIS thread already
        // holds the connection mutex (recursively).
        let store = ShadowTableStore::new(handle, &table);
        let engine = Engine::with_store(
            Box::new(store),
            FLUSH_THRESHOLD,
            MIN_FLUSH_SIZE,
            COMPRESSION_LEVEL,
            MEMORY_BUDGET,
            DEFER_COMPRESSION,
        );

        // Declared schema. The hidden 5th column is named after the table
        // itself so `INSERT INTO metrics(metrics) VALUES('flush')` works.
        let schema = format!(
            "CREATE TABLE x(name TEXT, ts INTEGER, value REAL, labels TEXT, \"{}\" HIDDEN)",
            escape_double_quote(&table)
        );
        let schema = CString::new(schema)
            .map_err(|_| module_err(format!("table name contains NUL: {table:?}")))?;

        Ok((
            Cow::Owned(schema),
            MetricsTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
                table_name: table,
                engine: Arc::new(engine),
                rowid_counter: 0,
            },
        ))
    }

    /// Handle a hidden-column command insert. Returns the (synthetic,
    /// meaningless) rowid 0 — commands do not create rows.
    fn run_command(&self, cmd: &str) -> Result<i64> {
        if cmd == "flush" {
            // Drain every partition buffer into pco chunks in _chunks and
            // persist the series registry into _meta. After this the data
            // is exactly as durable as the enclosing SQLite transaction.
            self.engine.flush_all().map_err(module_err)?;
        } else if cmd == "compact" {
            // Merge small/raw chunks into large high-compression chunks.
            // POC: cutoff i64::MAX makes every persisted chunk eligible.
            // Production would pass now - 3600 (the engine's
            // COMPACT_MIN_AGE_SECS recent-window rule) so narrow
            // dashboard queries keep cheap small chunks; for the POC we
            // want compaction observable immediately.
            self.engine
                .compact_partitions(i64::MAX)
                .map_err(module_err)?;
        } else if let Some(ts_str) = cmd.strip_prefix("prune:") {
            // Retention: drop whole chunks whose max_ts < the cutoff.
            // Block-granular deletes — one DELETE row removes a whole
            // compressed chunk (see PLAN.md "Pruning & retention").
            let ts: i64 = ts_str.trim().parse().map_err(|_| {
                module_err(format!("prune: expected 'prune:<unix_ts>', got {cmd:?}"))
            })?;
            let (_chunks, _units, errors) = self.engine.delete_before(ts);
            if !errors.is_empty() {
                return Err(module_err(format!("prune errors: {}", errors.join("; "))));
            }
        } else {
            return Err(module_err(format!(
                "unknown command {cmd:?}; supported: 'flush', 'compact', 'prune:<unix_ts>'"
            )));
        }
        Ok(0)
    }

    /// Tier 2 ingest: decode one batch blob (format v0, PLAN.md) and push
    /// every point into the engine's partition buffers in one call.
    ///
    /// All-or-nothing: the ENTIRE blob is validated — header, series
    /// table, column lengths, and every per-point series index — before a
    /// single point is written. A malformed batch is a hard error and
    /// stores nothing.
    ///
    /// Deliberately does NOT flush: same durability contract as Tier 1
    /// (the caller sends 'flush' when it wants chunks on disk). Returns
    /// the point count as the synthetic rowid so callers can sanity-check
    /// via last_insert_rowid().
    fn ingest_batch(&mut self, blob: &[u8]) -> Result<i64> {
        // ── 1. Header (12 bytes, all little-endian) ──────────────────
        let mut r = BatchReader::new(blob);
        let version = r.u8("version")?;
        if version != 0x01 {
            return Err(module_err(format!(
                "batch blob: unsupported version 0x{version:02x} (this build speaks v0 = 0x01)"
            )));
        }
        let flags = r.u8("flags")?;
        if flags != 0 {
            return Err(module_err(format!(
                "batch blob: unknown flags 0x{flags:02x} (v0 defines none; must be 0)"
            )));
        }
        r.skip(2, "reserved header bytes")?;
        let n_series = r.u32("n_series")? as usize;
        let n_points = r.u32("n_points")? as usize;

        // ── 2. Series table: n_series × { name, labels-JSON } ────────
        let mut entries: Vec<(String, Labels)> = Vec::with_capacity(n_series);
        for i in 0..n_series {
            let name_len = r.u32("series name length")? as usize;
            let name_bytes = r.take(name_len, "series name")?;
            let name = std::str::from_utf8(name_bytes)
                .map_err(|_| {
                    module_err(format!("batch blob: series {i}: name is not valid UTF-8"))
                })?
                .to_owned();

            let labels_len = r.u32("series labels length")? as usize;
            let labels_bytes = r.take(labels_len, "series labels")?;
            // Empty labels field = no labels; otherwise it must be the
            // same flat JSON object Tier 1 accepts (same parser, so the
            // two tiers can never disagree about what a label set means).
            let labels: Labels = if labels_bytes.is_empty() {
                BTreeMap::new()
            } else {
                let txt = std::str::from_utf8(labels_bytes).map_err(|_| {
                    module_err(format!("batch blob: series {i}: labels are not valid UTF-8"))
                })?;
                parse_labels_json(txt)
                    .map_err(|e| module_err(format!("batch blob: series {i}: {e}")))?
                    .into_iter()
                    .collect() // HashMap -> BTreeMap (engine's Labels)
            };
            entries.push((name, labels));
        }

        // ── 3. The three columnar sections, sized exactly by n_points ─
        // take() bounds-checks each one, so a truncated blob fails with a
        // message naming the section that fell short.
        let idx_bytes = r.take(n_points * 4, "per-point series index column")?;
        let ts_bytes = r.take(n_points * 8, "timestamp column")?;
        let val_bytes = r.take(n_points * 8, "value column")?;
        if r.remaining() != 0 {
            return Err(module_err(format!(
                "batch blob: {} trailing byte(s) after value column (corrupt or wrong n_points)",
                r.remaining()
            )));
        }

        // ── 4. Validate EVERY series index before writing anything ───
        // (all-or-nothing contract: write_batch_raw below cannot be
        // un-done, so nothing may reach it until the whole batch checks
        // out).
        for (i, chunk) in idx_bytes.chunks_exact(4).enumerate() {
            let idx = u32::from_le_bytes(chunk.try_into().unwrap()) as usize;
            if idx >= n_series {
                return Err(module_err(format!(
                    "batch blob: point {i}: series index {idx} out of range \
                     (series table has {n_series} entries); batch rejected"
                )));
            }
        }

        // ── 5. Resolve the whole series table in ONE registry pass ───
        let sids = self
            .engine
            .resolve_series_batch(&entries)
            .map_err(module_err)?;

        // ── 6. Re-pack to the engine's raw wire format and write once ─
        // Engine format: n × [series_id i64, ts i64, val f64] in NATIVE
        // endianness, 24 bytes/entry. The blob is little-endian; on the
        // LE targets we run on, from_le_bytes → to_ne_bytes compiles down
        // to a straight copy, but writing it this way stays correct on a
        // big-endian machine too (never assume byte order — read LE
        // explicitly, exactly as PLAN.md says).
        let mut raw: Vec<u8> = Vec::with_capacity(n_points * 24);
        for i in 0..n_points {
            let idx =
                u32::from_le_bytes(idx_bytes[i * 4..i * 4 + 4].try_into().unwrap()) as usize;
            let sid = sids[idx]; // idx proven in-range in step 4
            let ts = i64::from_le_bytes(ts_bytes[i * 8..i * 8 + 8].try_into().unwrap());
            // Values are opaque 8-byte payloads here: round-tripping the
            // BITS through u64 avoids ever "interpreting" the float, so
            // NaN payloads etc. survive byte-exact.
            let val_bits = u64::from_le_bytes(val_bytes[i * 8..i * 8 + 8].try_into().unwrap());
            raw.extend_from_slice(&sid.to_ne_bytes());
            raw.extend_from_slice(&ts.to_ne_bytes());
            raw.extend_from_slice(&val_bits.to_ne_bytes());
        }
        self.engine.write_batch_raw(&raw).map_err(module_err)?;

        Ok(n_points as i64)
    }
}

// ---------------------------------------------------------------------------
// Batch blob format v0 reader (PLAN.md "Batch blob format v0")
// ---------------------------------------------------------------------------

/// A bounds-checked cursor over the raw batch blob. Every read names what
/// it was reading, so truncation errors point at the exact field — this
/// is a public wire format and its error messages are part of the API.
struct BatchReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BatchReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        BatchReader { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Take exactly `n` bytes or fail with a message naming `what`.
    /// checked_add guards against a hostile length that would overflow
    /// usize arithmetic (u32 lengths can't overflow on 64-bit, but the
    /// habit is free and the compiler removes it when provably safe).
    fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(|| {
            module_err(format!("batch blob: length overflow reading {what}"))
        })?;
        if end > self.buf.len() {
            return Err(module_err(format!(
                "batch blob truncated: need {n} byte(s) for {what} at offset {}, \
                 but only {} remain",
                self.pos,
                self.remaining()
            )));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn skip(&mut self, n: usize, what: &str) -> Result<()> {
        self.take(n, what).map(|_| ())
    }

    fn u8(&mut self, what: &str) -> Result<u8> {
        Ok(self.take(1, what)?[0])
    }

    fn u32(&mut self, what: &str) -> Result<u32> {
        let b = self.take(4, what)?;
        Ok(u32::from_le_bytes(b.try_into().unwrap()))
    }
}

unsafe impl<'vtab> VTab<'vtab> for MetricsTab {
    type Aux = ();
    type Cursor = MetricsCursor<'vtab>;

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

    /// Query planning: recognize the constraints we can push down and
    /// tell SQLite which ones to hand to filter() as arguments.
    ///
    /// idx_num bitmask:  1 = name equality,  2 = ts lower bound,
    ///                   4 = ts upper bound.
    /// argv slots are assigned in that canonical order, so filter() can
    /// decode positions from the mask alone.
    ///
    /// We deliberately do NOT set omit on any constraint: SQLite keeps
    /// double-checking each row after we return it. That makes it safe to
    /// treat strict bounds (>, <) as their inclusive cousins (>=, <=) —
    /// we may return one extra edge row, SQLite filters it back out.
    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        use IndexConstraintOp::*;

        // Pass 1 (immutable borrow): find the first usable constraint of
        // each kind. Column order: 0 name, 1 ts, 2 value, 3 labels.
        let mut name_c: Option<usize> = None;
        let mut lo_c: Option<usize> = None;
        let mut hi_c: Option<usize> = None;
        for (i, c) in info.constraints().enumerate() {
            if !c.is_usable() {
                continue;
            }
            match (c.column(), c.operator()) {
                (0, SQLITE_INDEX_CONSTRAINT_EQ) if name_c.is_none() => name_c = Some(i),
                (1, SQLITE_INDEX_CONSTRAINT_GE) | (1, SQLITE_INDEX_CONSTRAINT_GT)
                    if lo_c.is_none() =>
                {
                    lo_c = Some(i)
                }
                (1, SQLITE_INDEX_CONSTRAINT_LE) | (1, SQLITE_INDEX_CONSTRAINT_LT)
                    if hi_c.is_none() =>
                {
                    hi_c = Some(i)
                }
                _ => {}
            }
        }

        // Pass 2 (mutable borrows): claim argv slots in canonical order.
        let mut mask: c_int = 0;
        let mut slot: c_int = 1; // argv indexes are 1-based
        if let Some(i) = name_c {
            info.constraint_usage(i).set_argv_index(slot);
            slot += 1;
            mask |= 1;
        }
        if let Some(i) = lo_c {
            info.constraint_usage(i).set_argv_index(slot);
            slot += 1;
            mask |= 2;
        }
        if let Some(i) = hi_c {
            info.constraint_usage(i).set_argv_index(slot);
            mask |= 4;
        }

        info.set_idx_num(mask);
        // A name-equality plan touches one metric's series; a bare scan
        // touches everything. Rough costs steer the planner accordingly.
        info.set_estimated_cost(if mask & 1 != 0 { 1e3 } else { 1e6 });
        Ok(true)
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        Ok(MetricsCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            engine: Arc::clone(&self.engine),
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

impl CreateVTab<'_> for MetricsTab {
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

    /// DROP TABLE on the vtab removes the shadow tables too.
    fn destroy(&self) -> Result<()> {
        let host = unsafe { Connection::from_handle(self.db) }?;
        host.execute_batch(&shadow_store::drop_ddl(&self.table_name))
    }
}

impl UpdateVTab<'_> for MetricsTab {
    /// INSERT. argv layout: [0] NULL, [1] requested rowid, then the
    /// declared columns from index 2:
    ///   2 = name, 3 = ts, 4 = value, 5 = labels, 6 = hidden command.
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        // The FTS5 command idiom, extended for Tier 2: a non-NULL hidden
        // column means this "insert" is NOT a data row. We dispatch on the
        // hidden column's SQLite TYPE (which we can only see through the
        // raw ValueRef — args.get::<String> would stringify blobs):
        //   TEXT → maintenance command ('flush', 'compact', ...)
        //   BLOB → Tier 2 batch ingest (batch blob format v0)
        //   NULL → ordinary Tier 1 data row (fall through below)
        match args.iter().nth(6) {
            Some(ValueRef::Blob(batch)) => return self.ingest_batch(batch),
            Some(ValueRef::Null) | None => {} // plain data row
            Some(_) => {
                // TEXT (or something coercible to it — anything else gets
                // rusqlite's clear InvalidType error) is a command.
                let cmd: String = args.get(6)?;
                return self.run_command(&cmd);
            }
        }

        let name: Option<String> = args.get(2)?;
        let Some(name) = name else {
            return Err(module_err("name is required (TEXT)".into()));
        };
        let ts: Option<i64> = args.get(3)?;
        let Some(ts) = ts else {
            return Err(module_err("ts is required (INTEGER)".into()));
        };
        let value: Option<f64> = args.get(4)?;
        let Some(value) = value else {
            return Err(module_err("value is required (REAL)".into()));
        };
        // labels: optional flat JSON object; NULL means "no labels".
        let labels_json: Option<String> = args.get(5)?;
        let labels: HashMap<String, String> = match labels_json {
            Some(txt) => parse_labels_json(&txt).map_err(module_err)?,
            None => HashMap::new(),
        };

        let sid = self
            .engine
            .resolve_cached(&name, &labels)
            .map_err(module_err)?;
        self.engine.write_point(sid, ts, value);

        // Vtab rowids here are SYNTHETIC: points live in partition
        // buffers/chunks, not addressable rows, so we just hand SQLite a
        // monotonically increasing number to satisfy the interface.
        self.rowid_counter += 1;
        Ok(self.rowid_counter)
    }

    /// The vtab is append-only: points are folded into compressed chunks
    /// and have no per-row identity to delete by.
    fn delete(&mut self, _arg: ValueRef<'_>) -> Result<()> {
        Err(module_err(
            "timeless_metrics is append-only; DELETE is not supported \
             (use INSERT INTO t(t) VALUES('prune:<unix_ts>') for retention)"
                .into(),
        ))
    }

    /// Same story for UPDATE.
    fn update(&mut self, _args: &Updates<'_>) -> Result<()> {
        Err(module_err(
            "timeless_metrics is append-only; UPDATE is not supported".into(),
        ))
    }
}

/// POC transaction semantics (PLAN.md risk R5, accepted for now):
/// - commit(): no-op. Buffered points are queryable immediately and
///   become durable at 'flush' — we do NOT flush per-commit, because a
///   flush per tiny transaction would produce confetti chunks and defeat
///   the whole buffering design.
/// - rollback(): no-op, which means buffered writes SURVIVE a rollback
///   (they only touched engine memory, not the database). Documented POC
///   limitation; tests/cli.sh section 6 demonstrates it. The real fix
///   (R5) is journaling buffered points per savepoint.
impl TransactionVTab<'_> for MetricsTab {
    fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The cursor (one per active SELECT scan)
// ---------------------------------------------------------------------------

/// One output row, fully materialized at filter() time.
struct OutRow {
    name: String,
    ts: i64,
    value: f64,
    labels_json: String,
}

#[repr(C)]
pub struct MetricsCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    engine: Arc<Engine>,
    rows: Vec<OutRow>,
    pos: usize,
    /// Ties the cursor lifetime to its vtab so Rust prevents use-after-free.
    phantom: PhantomData<&'vtab MetricsTab>,
}

impl MetricsCursor<'_> {
    /// Query every series of one metric SEQUENTIALLY on this thread.
    ///
    /// Deliberate deviation: we do NOT call engine.query_range_labeled()
    /// here. That path fans out over rayon workers, and each worker would
    /// re-enter SQLite (store.read_chunk) on the HOST connection — whose
    /// per-connection mutex THIS thread is currently holding (we are
    /// inside xFilter). Workers would block on that mutex while we block
    /// on the workers: deadlock. query_range_by_id is rayon-free, so
    /// looping it here keeps every SQLite call on the mutex-owning thread.
    fn collect_metric(&self, metric: &str, t0: i64, t1: i64) -> Result<Vec<OutRow>> {
        // Snapshot (series_id, labels) pairs, then drop the registry lock
        // before querying (queries take their own locks).
        let candidates: Vec<(i64, Labels)> = {
            let reg = self.engine.series_read();
            reg.find_series(metric, &BTreeMap::new())
                .into_iter()
                .filter_map(|sid| reg.info_for(sid).map(|info| (sid, info.labels.clone())))
                .collect()
        };

        let mut out = Vec::new();
        for (sid, labels) in candidates {
            let points = self
                .engine
                .query_range_by_id(sid, t0, t1)
                .map_err(module_err)?;
            if points.is_empty() {
                continue;
            }
            let labels_json = labels_to_json(&labels);
            for (ts, value) in points {
                out.push(OutRow {
                    name: metric.to_string(),
                    ts,
                    value,
                    labels_json: labels_json.clone(),
                });
            }
        }
        Ok(out)
    }
}

unsafe impl VTabCursor for MetricsCursor<'_> {
    /// Start of a scan: decode the pushed-down constraints per the
    /// best_index bitmask, materialize all matching rows, iterate.
    fn filter(
        &mut self,
        idx_num: c_int,
        _idx_str: Option<&str>,
        args: &Filters<'_>,
    ) -> Result<()> {
        // argv slots were assigned in canonical order (name, lo, hi), so
        // the mask alone tells us which positional arg is which.
        let mut arg = 0usize;
        let name: Option<String> = if idx_num & 1 != 0 {
            let v = args.get(arg)?;
            arg += 1;
            v // NULL name matches nothing, handled below
        } else {
            None
        };
        // Unconstrained bounds default to (almost) the full i64 range;
        // the ±1 keeps them safely away from any sentinel arithmetic.
        let t0: i64 = if idx_num & 2 != 0 {
            let v = args.get(arg)?;
            arg += 1;
            v
        } else {
            i64::MIN + 1
        };
        let t1: i64 = if idx_num & 4 != 0 {
            args.get(arg)?
        } else {
            i64::MAX - 1
        };

        let mut rows = Vec::new();
        if idx_num & 1 != 0 {
            // Name pushdown: only this metric's series.
            if let Some(name) = name {
                rows = self.collect_metric(&name, t0, t1)?;
            }
            // WHERE name = NULL matches nothing: rows stays empty.
        } else {
            // Full scan: every metric the registry knows about.
            let metrics = self.engine.series_read().list_metrics();
            for metric in metrics {
                rows.extend(self.collect_metric(&metric, t0, t1)?);
            }
        }

        // Deterministic output order: ts ascending, then name/labels as
        // tie-breakers (points inside one series are already ts-sorted,
        // but rows from different series interleave).
        rows.sort_by(|a, b| {
            (a.ts, &a.name, &a.labels_json).cmp(&(b.ts, &b.name, &b.labels_json))
        });

        self.rows = rows;
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
        match i {
            0 => ctx.set_result(&row.name),
            1 => ctx.set_result(&row.ts),
            2 => ctx.set_result(&row.value),
            3 => ctx.set_result(&row.labels_json),
            // 4 = the hidden command column: always NULL when read.
            _ => ctx.set_result(&Null),
        }
    }

    /// Synthetic rowid = position in the materialized result. Only stable
    /// within one scan, which is all SQLite requires of us here.
    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

// ---------------------------------------------------------------------------
// Labels: flat JSON object <-> maps, WITHOUT serde
// ---------------------------------------------------------------------------
// A whole serde dependency for `{"key":"value"}` objects would be the
// heaviest crate in the extension. Instead: a tiny hand parser.
//
// KNOWN LIMITS (deliberate — reject rather than misparse):
//   - values must be strings: numbers, booleans, null, nested objects and
//     arrays are errors ("flat JSON object of string values" only);
//   - \uXXXX escapes cover the Basic Multilingual Plane only — surrogate
//     pairs (emoji etc. written as 😀) are rejected; literal
//     UTF-8 in the string works fine;
//   - duplicate keys: last one wins (like most JSON parsers).

/// Serialize labels back to a canonical JSON string: keys in BTreeMap
/// (sorted) order, minimal escaping. Canonical form means equal label
/// sets always render byte-identical, so it is safe to compare/GROUP BY.
fn labels_to_json(labels: &Labels) -> String {
    let mut out = String::with_capacity(2 + labels.len() * 16);
    out.push('{');
    let mut first = true;
    for (k, v) in labels {
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        json_escape_into(&mut out, k);
        out.push_str("\":\"");
        json_escape_into(&mut out, v);
        out.push('"');
    }
    out.push('}');
    out
}

fn json_escape_into(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
}

/// Character-cursor over the input; the parse functions below advance it.
struct JsonCursor {
    chars: Vec<char>,
    pos: usize,
}

impl JsonCursor {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, want: char) -> Result<(), String> {
        match self.bump() {
            Some(c) if c == want => Ok(()),
            Some(c) => Err(format!("labels JSON: expected '{want}', found '{c}'")),
            None => Err(format!("labels JSON: expected '{want}', found end of input")),
        }
    }

    /// Parse a JSON string (cursor on the opening quote).
    fn parse_string(&mut self) -> Result<String, String> {
        self.expect('"')?;
        let mut out = String::new();
        loop {
            match self.bump() {
                None => return Err("labels JSON: unterminated string".into()),
                Some('"') => return Ok(out),
                Some('\\') => match self.bump() {
                    Some('"') => out.push('"'),
                    Some('\\') => out.push('\\'),
                    Some('/') => out.push('/'),
                    Some('b') => out.push('\u{0008}'),
                    Some('f') => out.push('\u{000C}'),
                    Some('n') => out.push('\n'),
                    Some('r') => out.push('\r'),
                    Some('t') => out.push('\t'),
                    Some('u') => {
                        let mut code: u32 = 0;
                        for _ in 0..4 {
                            let d = self
                                .bump()
                                .and_then(|c| c.to_digit(16))
                                .ok_or_else(|| {
                                    "labels JSON: \\u needs 4 hex digits".to_string()
                                })?;
                            code = code * 16 + d;
                        }
                        // Surrogate halves are not valid chars on their
                        // own; pairing them is more parser than labels
                        // deserve. Use literal UTF-8 instead.
                        let c = char::from_u32(code).ok_or_else(|| {
                            format!(
                                "labels JSON: \\u{code:04x} is a surrogate half; \
                                 surrogate pairs unsupported, use literal UTF-8"
                            )
                        })?;
                        out.push(c);
                    }
                    Some(c) => return Err(format!("labels JSON: bad escape '\\{c}'")),
                    None => return Err("labels JSON: unterminated escape".into()),
                },
                Some(c) => out.push(c),
            }
        }
    }
}

/// Parse a FLAT JSON object of string keys and string values into a map.
fn parse_labels_json(input: &str) -> Result<HashMap<String, String>, String> {
    let mut cur = JsonCursor {
        chars: input.chars().collect(),
        pos: 0,
    };
    let mut out = HashMap::new();

    cur.skip_ws();
    cur.expect('{')?;
    cur.skip_ws();
    if cur.peek() == Some('}') {
        cur.bump();
    } else {
        loop {
            cur.skip_ws();
            let key = cur.parse_string()?;
            cur.skip_ws();
            cur.expect(':')?;
            cur.skip_ws();
            match cur.peek() {
                Some('"') => {
                    let val = cur.parse_string()?;
                    out.insert(key, val);
                }
                Some(c @ ('{' | '[')) => {
                    return Err(format!(
                        "labels must be a FLAT JSON object of string values; \
                         found nested '{c}' at key {key:?}"
                    ));
                }
                Some(c) => {
                    return Err(format!(
                        "labels values must be JSON strings; found '{c}' at key {key:?} \
                         (numbers/booleans/null are not supported)"
                    ));
                }
                None => return Err("labels JSON: unexpected end of input".into()),
            }
            cur.skip_ws();
            match cur.bump() {
                Some(',') => continue,
                Some('}') => break,
                Some(c) => {
                    return Err(format!("labels JSON: expected ',' or '}}', found '{c}'"))
                }
                None => return Err("labels JSON: unexpected end of input".into()),
            }
        }
    }
    cur.skip_ws();
    if cur.pos != cur.chars.len() {
        return Err("labels JSON: trailing characters after object".into());
    }
    Ok(out)
}
