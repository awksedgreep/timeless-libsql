//! timeless_metrics: the real writable vtab, modeled on the spike but
//! backed by a full timeless-core Engine persisting through
//! ShadowTableStore into `<table>_chunks` / `<table>_meta` on the host db.
//!
//! Exposed schema (declared at runtime because the hidden command column
//! is named after the table — the FTS5 command idiom):
//!
//!   CREATE TABLE x(name TEXT, ts INTEGER, value REAL, labels TEXT,
//!                  series_id INTEGER HIDDEN, `"<table>"` HIDDEN)
//!
//! Write path:  INSERT INTO metrics(name, ts, value, labels) VALUES (...)
//!              → resolve series → in-memory partition buffer (Tier 1).
//!
//! The hidden command column accepts THREE payload kinds, dispatched by
//! SQLite TYPE and then (for blobs) by the first byte:
//!
//!   TEXT  → maintenance command: 'flush' | 'compact' | 'prune:<unix_ts>'
//!           (the FTS5 idiom: an insert that sets only the hidden column
//!           runs maintenance instead of storing a row).
//!   BLOB, first byte 0x01
//!         → Tier 2 batch-blob-v0 ingest (PLAN.md "Batch blob format
//!           v0"; 0x01 is the v0 version byte).
//!   BLOB, first byte 0x02
//!         → resolved-series batch v1 ingest (durable series ids +
//!           timestamp/value columns).
//!   BLOB, first byte anything else printable
//!         → Prometheus text exposition body — a raw scrape:
//!             INSERT INTO metrics(metrics) VALUES (readfile('scrape'));
//!           Valid exposition text can only start with a metric-name
//!           byte, '#', or whitespace, so it can never collide with the
//!           batch version byte. Bytes 0x00 and 0x03–0x08 are RESERVED
//!           for future batch versions and rejected loudly ("unknown
//!           blob format") so a future v1 blob fed to an old build fails
//!           instead of being mis-parsed as text.
//!
//! ── TIMESTAMP UNIT: EPOCH SECONDS ────────────────────────────────────
//! The Prometheus spec says explicit sample timestamps are MILLISECONDS,
//! but engine.ingest_prometheus NORMALIZES them: any explicit ts >
//! 1_000_000_000_000 (i.e. an epoch in ms) is divided by 1000, and
//! samples WITHOUT a timestamp receive default_ts verbatim. ts is an
//! opaque i64 to the engine — the only thing that matters is that one
//! table stays internally consistent — so we pass default_ts as the
//! current wall clock in EPOCH SECONDS, matching what the normalizer
//! produces for explicit timestamps, matching Tier 1 usage throughout
//! this repo, and matching 'prune:<unix_ts>'. Everything in a
//! timeless_metrics table is epoch SECONDS.
//!
//! Prometheus error semantics (engine contract, mirrored here): NaN and
//! ±Inf are valid float-series values and retain their IEEE bits. Malformed
//! non-comment lines are COUNTED but do not abort the body — partial success
//! (some samples + some errors) succeeds silently. Only a
//! body that yields ZERO samples with ≥1 error is rejected, because
//! that means the payload wasn't exposition text at all.
//!
//! Durability semantics are IDENTICAL across all ingest paths: points
//! land in the same engine buffers and become durable at the same
//! 'flush'.
//!
//! Read path:   buffered points and flushed chunks are merged by the
//!              engine, so data is queryable immediately after INSERT and
//!              durable after 'flush'.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_int, CStr, CString};
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

use rusqlite::ffi;
use rusqlite::types::{Null, Value, ValueRef};
use rusqlite::vtab::{
    escape_double_quote, Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, Inserts,
    Module, TransactionVTab, UpdateVTab, Updates, VTab, VTabConnection, VTabCursor, VTabKind,
};
use rusqlite::{Connection, Error, Result};
use timeless_core::{Engine, Labels};

use crate::batch::BatchReader;
use crate::flatjson::{labels_to_json, parse_labels_json};
use crate::shadow_meta;
use crate::shadow_store::{self, ShadowTableStore};
use crate::shared::{self, DbGuard, RegistryKey, SharedEngine};
use crate::sql_ident;
use crate::sql_value::integer_affinity;
use crate::table_args;
use crate::vtab_tx::{self, SavepointVTab};

/// Register the "timeless_metrics" module on a freshly-loaded connection.
pub(crate) fn register(db: &Connection) -> Result<()> {
    const MODULE: Module<MetricsTab> = vtab_tx::update_module_with_savepoints();
    db.create_module(c"timeless_metrics", &MODULE, None::<()>)
}

/// Engine parameters for the POC (see PLAN.md Session 3).
const FLUSH_THRESHOLD: usize = 4096; // points per series before auto-queue
const MIN_FLUSH_SIZE: usize = 0; // flush everything, however small
const COMPRESSION_LEVEL: usize = 8; // pco level
const MEMORY_BUDGET: usize = 256 * 1024 * 1024; // 256 MiB of buffers
const DEFER_COMPRESSION: bool = false; // compress at flush, not later
/// F2 retention unit conversion: metrics ts is epoch SECONDS.
const NATIVE_PER_SECOND: i64 = 1;

/// Map an engine error String into the vtab error type SQLite surfaces
/// to the user (rusqlite renders ModuleError's message verbatim).
fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

fn validate_named_series_count(n_series: usize, remaining: usize) -> Result<()> {
    const MIN_SERIES_BYTES: usize = 2 * size_of::<u32>();
    let minimum = n_series.checked_mul(MIN_SERIES_BYTES).ok_or_else(|| {
        module_err("batch blob: n_series overflows minimum series table length".into())
    })?;
    if minimum > remaining {
        return Err(module_err(format!(
            "batch blob truncated: {n_series} series require at least {minimum} series-table byte(s), but only {remaining} remain"
        )));
    }
    Ok(())
}

/// Load the persisted F3 ladder ("res:ret,..." native units) if any.
fn load_rollups(
    conn: &Connection,
    database: &str,
    table: &str,
) -> std::result::Result<Option<Vec<timeless_core::RollupTier>>, String> {
    match crate::shadow_meta::load_meta_text(conn, database, table, "rollups")? {
        None => Ok(None),
        Some(spec) => timeless_core::parse_ladder(&spec)
            .map(Some)
            .map_err(|e| format!("{table}: rollups in _meta is invalid: {e}")),
    }
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
    /// pub(crate): health_vtab wraps MetricsTab and samples through it.
    pub(crate) db: *mut ffi::sqlite3,
    /// The vtab's own name — needed to drop its shadow tables.
    pub(crate) table_name: String,
    /// Owning SQLite schema ("main", "temp", or an ATTACH alias).
    pub(crate) database_name: String,
    /// The whole timeless-core engine, chunk-persisting into shadow
    /// tables via ShadowTableStore — SHARED process-wide with every
    /// other connection's vtab instance over the same (db file, table)
    /// via the R4 registry (see shared.rs). Arc so cursors can hold a
    /// reference without lifetime gymnastics, and so instances across
    /// connections co-own one engine.
    pub(crate) shared: Arc<SharedEngine<Engine>>,
    /// Durable instance key used to bridge transactional xDestroy rollback.
    key: RegistryKey,
    /// True while THIS connection's write transaction holds the shared
    /// engine's writer gate (acquired in begin(), released in commit()/
    /// rollback()). Lives in the vtab instance because the instance is
    /// per-connection — exactly the granularity a "who holds it" flag
    /// needs — and it lets the insert hot path skip the gate mutex.
    gate_held: bool,
    /// Exact authoritative token captured in xSync after this transaction's
    /// final shadow-table mutation. xCommit publishes it together with the
    /// already-updated shared engine state, preventing the next reader from
    /// redundantly reloading the complete series and chunk catalogs. xRollback
    /// discards it, so an aborted transaction can never publish its token.
    pending_catalog_generation: Option<(i64, i64)>,
    /// Synthetic rowid source for inserts (see insert()).
    rowid_counter: i64,
}

impl MetricsTab {
    pub(crate) fn connect_create(
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
        // Innocuous (the FTS5 precedent): reads have no side effects, so
        // the vtab may be referenced from VIEWS under trusted_schema=off
        // — dbhealth's companion report views require this.
        db.config(rusqlite::vtab::VTabConfig::Innocuous)?;
        let handle = unsafe { db.handle() };
        // Bind the calling connection for the store operations below
        // (DDL, and the recovery SELECTs Engine::with_store performs
        // through ShadowTableStore). RAII: unbinds when we return.
        let _bind = DbGuard::bind(handle);

        // Re-entrant SQL against the host connection (the FTS5 trick
        // proven by the spike): from_handle borrows without owning.
        let host = unsafe { Connection::from_handle(handle) }?;
        if is_create {
            // Retention plan (PLAN.md "Pruning & retention"): incremental
            // auto-vacuum lets maintenance return freed pages to the OS in
            // small slices instead of a full VACUUM rewrite. The pragma
            // only takes effect if the database has no pages yet (it
            // changes the file format), so on a non-empty db it is a
            // silent no-op — hence: attempt and ignore errors.
            let _ = host.execute_batch(&sql_ident::incremental_auto_vacuum(&database));

            host.execute_batch(&shadow_store::ddl(&database, &table))?;
        } else {
            // Databases created before R2 do not have the normalized catalog.
            // Create only that new shadow table here; Engine::with_store then
            // imports and validates the legacy registry blob.
            host.execute_batch(&shadow_store::series_ddl(&database, &table))?;
        }
        shadow_store::ensure_max_ts_val_column(&host, &database, &table)?;
        let instance_id =
            shadow_meta::ensure_instance_id(&host, &database, &table).map_err(module_err)?;
        // xConnect: the shadow tables already exist in the reopened db.

        // R4: one engine per (db file, schema alias, table, instance)
        // per process. First connection in builds it (Engine::with_store
        // performs recovery itself: it loads the series registry via
        // store.load_registry() and rebuilds the chunk index via
        // store.scan() — both re-entrant SELECTs routed to the calling
        // connection by the DbGuard above, safe because THIS thread
        // already holds the connection mutex recursively); every later
        // xConnect just bumps the Arc.
        let key = shared::registry_key(handle, database_name, &table, instance_id);
        let shared_engine = shared::get_or_create(&key, || {
            let store = ShadowTableStore::new(&database, &table);
            Engine::with_store(
                Box::new(store),
                FLUSH_THRESHOLD,
                MIN_FLUSH_SIZE,
                COMPRESSION_LEVEL,
                MEMORY_BUDGET,
                DEFER_COMPRESSION,
            )
            .map_err(module_err)
        })?;

        // F2 retention: the CREATE argument is unit-resolved (epoch
        // seconds here) and PERSISTED in _meta — a property of the data,
        // like logs' index_keys. xConnect loads it back and never trusts
        // the replayed args.
        let (retention, rollups) = if is_create {
            let mut retention = None;
            let mut rollups: Option<(Vec<timeless_core::RollupTier>, String)> = None;
            for (name, value) in table_args::parse_kv_args(args).map_err(module_err)? {
                match name.as_str() {
                    "retention" => {
                        retention = Some(
                            table_args::parse_retention(&value, NATIVE_PER_SECOND)
                                .map_err(module_err)?,
                        );
                    }
                    "rollups" => {
                        rollups = Some(
                            table_args::parse_rollups(&value, NATIVE_PER_SECOND)
                                .map_err(module_err)?,
                        );
                    }
                    other => {
                        return Err(module_err(format!(
                            "unrecognized argument {other:?}; timeless_metrics supports: retention, rollups"
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
            if let Some((_, spec)) = &rollups {
                shadow_meta::save_meta_text(&host, &database, &table, "rollups", spec)
                    .map_err(module_err)?;
            }
            (retention, rollups.map(|(tiers, _)| tiers))
        } else {
            (
                shadow_meta::load_retention(&host, &database, &table).map_err(module_err)?,
                load_rollups(&host, &database, &table).map_err(module_err)?,
            )
        };
        shared_engine.engine.set_retention(retention);
        shared_engine
            .engine
            .set_rollups(rollups.unwrap_or_default());

        // Declared schema. `series_id` is an embedding fast path: callers
        // may resolve once and write by durable catalog id. The final hidden
        // column is named after the table
        // itself so `INSERT INTO metrics(metrics) VALUES('flush')` works.
        let schema = format!(
            "CREATE TABLE x(name TEXT, ts INTEGER, value REAL, labels TEXT, \
             series_id INTEGER HIDDEN, \"{}\" HIDDEN)",
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
                database_name: database,
                shared: shared_engine,
                key,
                gate_held: false,
                pending_catalog_generation: None,
                rowid_counter: 0,
            },
        ))
    }

    /// Resolve the shared engine for an EXISTING timeless_metrics table
    /// on this connection — the read-side entry point for the Q2 TVFs
    /// (query_tvf.rs). Mirrors the xConnect tail exactly (legacy catalog
    /// upgrade, durable instance identity, process registry with the
    /// same builder), so a TVF query on a fresh connection constructs
    /// the same engine xConnect would have. It never runs the CREATE
    /// path: a table that was never a timeless_metrics vtab fails on the
    /// `<table>_meta` read with SQLite's own "no such table" error.
    ///
    /// Caller must hold a DbGuard binding for `handle`.
    pub(crate) fn shared_engine_for(
        handle: *mut ffi::sqlite3,
        database: &str,
        table: &str,
    ) -> Result<Arc<SharedEngine<Engine>>> {
        let host = unsafe { Connection::from_handle(handle) }?;
        let instance_id =
            shadow_meta::ensure_instance_id(&host, database, table).map_err(module_err)?;
        host.execute_batch(&shadow_store::series_ddl(database, table))?;
        shadow_store::ensure_max_ts_val_column(&host, database, table)?;
        let key = shared::registry_key(handle, database.as_bytes(), table, instance_id);
        let shared = shared::get_or_create(&key, || {
            let store = ShadowTableStore::new(database, table);
            Engine::with_store(
                Box::new(store),
                FLUSH_THRESHOLD,
                MIN_FLUSH_SIZE,
                COMPRESSION_LEVEL,
                MEMORY_BUDGET,
                DEFER_COMPRESSION,
            )
            .map_err(module_err)
        })?;
        // P1: keep the engine alive for this connection's lifetime, so
        // TVF-only readers stop rebuilding it every statement.
        shared::pin_engine(handle, &key, shared.clone());
        shared.engine.set_retention(
            shadow_meta::load_retention(&host, database, table).map_err(module_err)?,
        );
        shared.engine.set_rollups(
            load_rollups(&host, database, table)
                .map_err(module_err)?
                .unwrap_or_default(),
        );
        Ok(shared)
    }

    /// Take the shared engine's writer gate for THIS connection if we
    /// do not hold it already. Primary call site is begin() — SQLite
    /// fires xBegin before the first write statement of every
    /// transaction — with a defensive re-check in insert().
    pub(crate) fn acquire_write_gate(&mut self) -> Result<()> {
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

    /// Release the gate at the end of this connection's transaction
    /// (commit or rollback). No-op if this connection never wrote.
    fn release_write_gate(&mut self) {
        if self.gate_held {
            self.shared.write_gate.release(self.db as usize);
            self.gate_held = false;
        }
    }

    /// Handle a hidden-column command insert. Returns the (synthetic,
    /// meaningless) rowid 0 — commands do not create rows.
    pub(crate) fn run_command(&self, cmd: &str) -> Result<i64> {
        if cmd == "flush" {
            // Drain every partition buffer into pco chunks in _chunks and
            // persist the series registry into _meta. After this the data
            // is exactly as durable as the enclosing SQLite transaction.
            self.shared.engine.flush_all().map_err(module_err)?;
        } else if cmd == "compact" {
            // Merge small/raw chunks into large high-compression chunks.
            // POC: cutoff i64::MAX makes every persisted chunk eligible.
            // Production would pass now - 3600 (the engine's
            // COMPACT_MIN_AGE_SECS recent-window rule) so narrow
            // dashboard queries keep cheap small chunks; for the POC we
            // want compaction observable immediately.
            self.shared
                .engine
                .compact_partitions(i64::MAX)
                .map_err(module_err)?;
            // F3: compaction is the natural rollup moment (both are
            // "reorganize storage" maintenance).
            self.shared.engine.rollup().map_err(module_err)?;
        } else if cmd == "rollup" {
            // F3: produce settled buckets for every declared tier. A
            // no-op (0 chunks) without a rollups= ladder.
            self.shared.engine.rollup().map_err(module_err)?;
        } else if let Some(ts_str) = cmd.strip_prefix("prune:") {
            // Retention: drop whole chunks whose max_ts < the cutoff.
            // Block-granular deletes — one DELETE row removes a whole
            // compressed chunk (see PLAN.md "Pruning & retention").
            let ts: i64 = ts_str.trim().parse().map_err(|_| {
                module_err(format!("prune: expected 'prune:<unix_ts>', got {cmd:?}"))
            })?;
            let (_chunks, _units, errors) = self.shared.engine.delete_before(ts);
            if !errors.is_empty() {
                return Err(module_err(format!("prune errors: {}", errors.join("; "))));
            }
        } else {
            return Err(module_err(format!(
                "unknown command {cmd:?}; supported: 'flush', 'compact', 'rollup', 'prune:<unix_ts>'"
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
    /// Series below the 4,096-point threshold remain buffered with the Tier 1
    /// durability contract. Series reaching it are drained through the
    /// engine's existing pending-flush path before the statement commits.
    /// Returns the point count as the synthetic rowid so callers can
    /// sanity-check via last_insert_rowid().
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
        // Every entry needs at least two u32 length fields. Prove the blob
        // can contain that much structure before allowing its count to drive
        // an allocation, then keep allocation failure on the SQLite-error
        // path instead of letting it abort the host.
        validate_named_series_count(n_series, r.remaining())?;
        let mut entries: Vec<(String, Labels)> = Vec::new();
        entries.try_reserve(n_series).map_err(|_| {
            module_err(format!(
                "batch blob: cannot allocate series table for {n_series} entries"
            ))
        })?;
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
                    module_err(format!(
                        "batch blob: series {i}: labels are not valid UTF-8"
                    ))
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
        for (i, chunk) in idx_bytes.as_chunks::<4>().0.iter().enumerate() {
            let idx = u32::from_le_bytes(*chunk) as usize;
            if idx >= n_series {
                return Err(module_err(format!(
                    "batch blob: point {i}: series index {idx} out of range \
                     (series table has {n_series} entries); batch rejected"
                )));
            }
        }

        // ── 5. Resolve the whole series table in ONE registry pass ───
        let sids = self
            .shared
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
            let idx = u32::from_le_bytes(idx_bytes[i * 4..i * 4 + 4].try_into().unwrap()) as usize;
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
        self.shared
            .engine
            .write_batch_raw(&raw)
            .map_err(module_err)?;
        self.shared.engine.flush_pending().map_err(module_err)?;

        Ok(n_points as i64)
    }

    /// Resolved-series batch v1 (version byte 0x02). This is the embedded
    /// host fast path: resolve each durable catalog id once, then send only
    /// columnar ids/timestamps/value bits on subsequent batches.
    ///
    /// Layout, little-endian:
    ///   version:u8=2, flags:u8=0, reserved:u16=0, n_points:u32,
    ///   series_id:i64[n], ts:i64[n], value_bits:u64[n].
    fn ingest_resolved_batch(&mut self, blob: &[u8]) -> Result<i64> {
        let mut r = BatchReader::new(blob);
        let version = r.u8("version")?;
        if version != 0x02 {
            return Err(module_err(format!(
                "resolved batch: unsupported version 0x{version:02x}"
            )));
        }
        let flags = r.u8("flags")?;
        if flags != 0 {
            return Err(module_err(format!(
                "resolved batch: unknown flags 0x{flags:02x}; must be 0"
            )));
        }
        r.skip(2, "reserved header bytes")?;
        let n_points = r.u32("n_points")? as usize;
        let column_bytes = n_points
            .checked_mul(8)
            .ok_or_else(|| module_err("resolved batch: point count overflows this host".into()))?;
        let sid_bytes = r.take(column_bytes, "series id column")?;
        let ts_bytes = r.take(column_bytes, "timestamp column")?;
        let val_bytes = r.take(column_bytes, "value column")?;
        if r.remaining() != 0 {
            return Err(module_err(format!(
                "resolved batch: {} trailing byte(s) after value column",
                r.remaining()
            )));
        }

        // Validate all ids before mutating any partition buffer.
        {
            let registry = self.shared.engine.series_read();
            for (i, bytes) in sid_bytes.as_chunks::<8>().0.iter().enumerate() {
                let sid = i64::from_le_bytes(*bytes);
                if registry.info_for(sid).is_none() {
                    return Err(module_err(format!(
                        "resolved batch: point {i}: unknown series id {sid}; batch rejected"
                    )));
                }
            }
        }

        let mut raw = Vec::with_capacity(n_points * 24);
        for i in 0..n_points {
            let sid = i64::from_le_bytes(sid_bytes[i * 8..i * 8 + 8].try_into().unwrap());
            let ts = i64::from_le_bytes(ts_bytes[i * 8..i * 8 + 8].try_into().unwrap());
            let val_bits = u64::from_le_bytes(val_bytes[i * 8..i * 8 + 8].try_into().unwrap());
            raw.extend_from_slice(&sid.to_ne_bytes());
            raw.extend_from_slice(&ts.to_ne_bytes());
            raw.extend_from_slice(&val_bits.to_ne_bytes());
        }
        self.shared
            .engine
            .write_batch_raw(&raw)
            .map_err(module_err)?;
        self.shared.engine.flush_pending().map_err(module_err)?;
        Ok(n_points as i64)
    }

    /// Prometheus text-exposition ingest: the blob is a raw scrape body
    /// (`curl target:9100/metrics`), parsed and buffered in one fused
    /// pass by the engine. The scraping LOOP stays external by design —
    /// cron/curl/Elixir drive it; the vtab is passive.
    ///
    /// ── UNIT DECISION (see module docs): default_ts is EPOCH SECONDS ─
    /// engine.ingest_prometheus divides explicit millisecond timestamps
    /// (the Prometheus wire unit) by 1000, so within one body explicit
    /// timestamps come out as seconds. Passing wall-clock seconds for
    /// the timestamp-less samples is therefore the ONLY choice that
    /// keeps a single body — and the whole table — internally
    /// consistent. (ts is opaque i64 to the engine; consistency is the
    /// contract, not the unit itself.)
    ///
    /// Error semantics (engine contract, documented in module docs):
    /// malformed lines are counted, not fatal; NaN/Inf are valid samples —
    /// partial success succeeds silently, matching how a real
    /// Prometheus server treats a scrape. Only "zero samples AND at
    /// least one error" is rejected: that body was not exposition text.
    ///
    /// Like the batch path, this flushes only series which reach the engine's
    /// 4,096-point threshold; smaller buffers retain the Tier 1 durability
    /// contract. Returns the sample count as the synthetic rowid, visible via
    /// last_insert_rowid().
    fn ingest_prometheus_text(&self, body: &[u8]) -> Result<i64> {
        // Wall clock in EPOCH SECONDS (the unit decision above). A
        // pre-1970 system clock would make duration_since fail; falling
        // back to 0 keeps ingest alive on such a broken clock (ts 0 is
        // as good as any other wrong answer there).
        let default_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let (count, errors) = self
            .shared
            .engine
            .ingest_prometheus(body, default_ts)
            .map_err(module_err)?;

        if count == 0 && errors > 0 {
            return Err(module_err(format!(
                "prometheus body: 0 samples ingested, {errors} malformed line(s)"
            )));
        }
        self.shared.engine.flush_pending().map_err(module_err)?;
        Ok(count as i64)
    }
}

// ---------------------------------------------------------------------------
// Batch blob format v0 reader (PLAN.md "Batch blob format v0")
// ---------------------------------------------------------------------------

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
    ///                   4 = ts upper bound, 8 = series_id equality.
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
        // each kind. Column order: 0 name, 1 ts, 2 value, 3 labels,
        // 4 hidden series_id.
        let mut name_c: Option<usize> = None;
        let mut lo_c: Option<usize> = None;
        let mut hi_c: Option<usize> = None;
        let mut series_c: Option<usize> = None;
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
                (4, SQLITE_INDEX_CONSTRAINT_EQ) if series_c.is_none() => series_c = Some(i),
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
            slot += 1;
            mask |= 4;
        }
        if let Some(i) = series_c {
            let mut usage = info.constraint_usage(i);
            usage.set_argv_index(slot);
            usage.set_omit(true);
            mask |= 8;
        }

        info.set_idx_num(mask);
        // A name-equality plan touches one metric's series; a bare scan
        // touches everything. Rough costs steer the planner accordingly.
        if mask & 8 != 0 {
            info.set_estimated_cost(10.0);
            info.set_estimated_rows(100);
        } else {
            info.set_estimated_cost(if mask & 1 != 0 { 1e3 } else { 1e6 });
            info.set_estimated_rows(if mask & 1 != 0 { 1000 } else { 1_000_000 });
        }
        Ok(true)
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        Ok(MetricsCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            shared: Arc::clone(&self.shared),
            // The cursor re-binds this connection in filter(): its
            // chunk reads must run on the connection driving the scan.
            db: self.db,
            table_name: self.table_name.clone(),
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

/// Defensive gate release: xDisconnect/xDestroy drop the vtab instance,
/// and the normal paths (commit/rollback) have already released by
/// then — but if SQLite ever tears a vtab down mid-transaction, a
/// leaked holder token would lock the table for every other connection
/// until process exit. Drop makes that impossible.
impl Drop for MetricsTab {
    fn drop(&mut self) {
        self.release_write_gate();
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

    /// DROP TABLE removes the shadow tables. The registry entry is left
    /// untouched until its Weak dies: rollback reconnects with the restored
    /// instance_id, while committed recreate receives a new identity.
    fn destroy(&self) -> Result<()> {
        shared::pin_for_drop(self.db, &self.key, &self.shared);
        let _bind = DbGuard::bind(self.db);
        let host = unsafe { Connection::from_handle(self.db) }?;
        host.execute_batch(&shadow_store::drop_ddl(
            &self.database_name,
            &self.table_name,
        ))
    }
}

impl UpdateVTab<'_> for MetricsTab {
    /// INSERT. argv layout: [0] NULL, [1] requested rowid, then the
    /// declared columns from index 2:
    ///   2 = name, 3 = ts, 4 = value, 5 = labels,
    ///   6 = hidden series_id, 7 = hidden command.
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        // Route this callback's store operations (flush/compact/prune
        // rows, registry saves) to the calling connection...
        let _bind = DbGuard::bind(self.db);
        // ...and make sure this connection's transaction owns the
        // shared engine. Normally already true — SQLite fired begin()
        // for this statement's transaction — this is the defensive
        // re-check (no-op when gate_held).
        self.acquire_write_gate()?;

        // The FTS5 command idiom, extended for Tier 2: a non-NULL hidden
        // column means this "insert" is NOT a data row. We dispatch on the
        // hidden column's SQLite TYPE (which we can only see through the
        // raw ValueRef — args.get::<String> would stringify blobs):
        //   TEXT → maintenance command ('flush', 'compact', ...)
        //   BLOB → binary payload, sub-dispatched on the FIRST BYTE:
        //          0x01        = named batch blob v0
        //          0x02        = resolved-series batch v1
        //          0x00, 0x03–0x08 = RESERVED future batch versions →
        //                        loud error, never mis-parsed as text
        //          anything else = Prometheus text exposition body (valid
        //                        exposition starts with a name byte, '#',
        //                        or whitespace — all ≥ 0x09)
        //   NULL → ordinary Tier 1 data row (fall through below)
        match args.iter().nth(7) {
            Some(ValueRef::Blob(blob)) => {
                return match blob.first().copied() {
                    Some(0x01) => self.ingest_batch(blob),
                    Some(0x02) => self.ingest_resolved_batch(blob),
                    Some(v @ (0x00 | 0x03..=0x08)) => Err(module_err(format!(
                        "unknown blob format: version byte 0x{v:02x} \
                         (this build speaks named batch 0x01, resolved batch 0x02, \
                          and Prometheus text)"
                    ))),
                    Some(_) => self.ingest_prometheus_text(blob),
                    None => Err(module_err(
                        "empty blob: cannot determine payload format \
                         (batch v0 starts with 0x01; Prometheus text is non-empty)"
                            .into(),
                    )),
                };
            }
            Some(ValueRef::Null) | None => {} // plain data row
            Some(_) => {
                // TEXT (or something coercible to it — anything else gets
                // rusqlite's clear InvalidType error) is a command.
                let cmd: String = args.get(7)?;
                if cmd == "resolve" {
                    let name: Option<String> = args.get(2)?;
                    let name =
                        name.ok_or_else(|| module_err("resolve requires name (TEXT)".into()))?;
                    let labels_json: Option<String> = args.get(5)?;
                    let labels: HashMap<String, String> = match labels_json {
                        Some(txt) => parse_labels_json(&txt).map_err(module_err)?,
                        None => HashMap::new(),
                    };
                    return self
                        .shared
                        .engine
                        .resolve_cached(&name, &labels)
                        .map_err(module_err);
                }
                return self.run_command(&cmd);
            }
        }

        let ts: Option<i64> = args.get(3)?;
        let Some(ts) = ts else {
            return Err(module_err("ts is required (INTEGER)".into()));
        };
        let value: Option<f64> = args.get(4)?;
        let Some(value) = value else {
            return Err(module_err("value is required (REAL)".into()));
        };
        let requested_sid: Option<i64> = args.get(6)?;
        let sid = match requested_sid {
            Some(sid) => {
                if self.shared.engine.series_read().info_for(sid).is_none() {
                    return Err(module_err(format!("unknown series_id {sid}")));
                }
                sid
            }
            None => {
                let name: Option<String> = args.get(2)?;
                let name = name.ok_or_else(|| module_err("name is required (TEXT)".into()))?;
                let labels_json: Option<String> = args.get(5)?;
                let labels: HashMap<String, String> = match labels_json {
                    Some(txt) => parse_labels_json(&txt).map_err(module_err)?,
                    None => HashMap::new(),
                };
                self.shared
                    .engine
                    .resolve_cached(&name, &labels)
                    .map_err(module_err)?
            }
        };
        self.shared.engine.write_point(sid, ts, value);
        self.shared.engine.flush_pending().map_err(module_err)?;

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

/// Real transaction semantics (PLAN.md risk R5 — FIXED):
///
/// SQLite calls xBegin before the FIRST write to the vtab in ANY
/// transaction — verified empirically: in autocommit mode every bare
/// INSERT statement gets its own xBegin/xSync/xCommit bracket, and an
/// explicit BEGIN...COMMIT gets exactly one for all its statements.
/// (SELECTs never trigger xBegin. One quirk seen in the wild: CREATE
/// VIRTUAL TABLE produces a lone xCommit with no matching xBegin —
/// txn_commit on an inactive journal is a deliberate no-op.) That
/// per-statement reality is why Engine::txn_begin is O(active
/// partitions) marks into reused, capacity-retaining collections — it
/// is on the autocommit hot path.
///
/// - begin(): activate the engine's transaction journal. From here,
///   buffered-point growth, intra-txn flush/compact/prune index
///   mutations, and pre-txn points drained by an intra-txn flush are
///   all recorded.
/// - commit(): drop the journal — the host transaction just made the
///   shadow-table side permanent, and engine memory already reflects
///   it. We still do NOT flush per-commit: a flush per tiny transaction
///   would produce confetti chunks and defeat the buffering design.
///   Durability of buffered points still begins at 'flush'.
/// - rollback(): undo engine memory to mirror what the host rollback
///   did to the shadow tables — txn-era buffered points vanish, index
///   entries for rolled-back chunk rows are removed (no dangling locs),
///   entries whose rows were restored come back, and pre-txn points
///   drained by an intra-txn flush return to the buffer.
///
/// ALL commands ('flush', 'compact', 'prune:<ts>') are allowed inside
/// explicit transactions and roll back fully — the journal covers their
/// index mutations, and their row mutations ride the host transaction.
///
/// SAVEPOINT ADDITION — rusqlite's update_module_with_tx does not wire
/// xSavepoint/xRelease/xRollbackTo, so vtab_tx.rs fills those version-2
/// module slots. The engine keeps one undo frame per SQLite savepoint;
/// statement failures and explicit ROLLBACK TO therefore restore engine
/// memory together with the shadow rows, including authoritative series
/// rows first created inside the rolled-back frame.
/// R4 ADDITION — the writer gate brackets the journal: begin() takes
/// the shared engine's gate BEFORE activating the journal, and
/// commit()/rollback() release it after closing the journal. Why here
/// and not at the first insert: SQLite fires xBegin on connection B
/// before B's first xUpdate, and txn_begin() RESETS the journal — if B
/// could reach it while A's transaction is journaling, A's rollback
/// state would be clobbered. Gating xBegin keeps the engine-global
/// journal provably single-writer, and it is still lazy in the way
/// that matters: xBegin only fires for transactions that WRITE to this
/// vtab, so reads never wait. A blocked begin() times out after 5s
/// with a busy-style error (see shared.rs for the deadlock analysis).
impl TransactionVTab<'_> for MetricsTab {
    fn begin(&mut self) -> Result<()> {
        let _bind = DbGuard::bind(self.db);
        self.pending_catalog_generation = None;
        self.acquire_write_gate()?;
        if let Err(err) = self.shared.engine.refresh_authoritative_state() {
            self.release_write_gate();
            return Err(module_err(err));
        }
        self.shared.engine.txn_begin();
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        let _bind = DbGuard::bind(self.db);
        if self.gate_held {
            // xSync is the prepare phase: all virtual-table writes are done,
            // the caller sees its own uncommitted shadow rows, and SQLite's
            // write transaction still excludes another writer. Capturing now
            // avoids the post-commit race that reading the token in xCommit
            // would introduce. A later commit publishes it; rollback drops it.
            self.pending_catalog_generation = self
                .shared
                .engine
                .capture_catalog_generation()
                .map_err(module_err)?;
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let _bind = DbGuard::bind(self.db);
        // Only the gate holder may close the journal: an xCommit that
        // arrives WITHOUT a gated xBegin on this connection (the lone
        // xCommit SQLite emits at CREATE VIRTUAL TABLE is the known
        // case) must not touch a journal that may belong to ANOTHER
        // connection's in-flight transaction.
        if self.gate_held {
            let generation = self.pending_catalog_generation.take();
            self.shared.engine.txn_commit_published(generation);
            self.release_write_gate();
        }
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        let _bind = DbGuard::bind(self.db);
        // Same holder-only rule as commit().
        if self.gate_held {
            self.pending_catalog_generation = None;
            self.shared.engine.txn_rollback();
            self.release_write_gate();
        }
        Ok(())
    }
}

impl SavepointVTab for MetricsTab {
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
// The cursor (one per active SELECT scan)
// ---------------------------------------------------------------------------

/// One output row, fully materialized at filter() time.
struct OutRow {
    series_id: i64,
    name: String,
    ts: i64,
    value: f64,
    labels_json: String,
}

#[repr(C)]
pub struct MetricsCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    shared: Arc<SharedEngine<Engine>>,
    /// The connection driving this scan — filter() binds it so the
    /// engine's chunk reads run on the caller (see shared.rs).
    db: *mut ffi::sqlite3,
    table_name: String,
    rows: Vec<OutRow>,
    pos: usize,
    /// Ties the cursor lifetime to its vtab so Rust prevents use-after-free.
    phantom: PhantomData<&'vtab MetricsTab>,
}

impl MetricsCursor<'_> {
    /// Query one durable catalog handle without enumerating a metric's series.
    /// An optional name constraint is intersected here before any chunk read.
    fn collect_series(
        &self,
        series_id: i64,
        expected_name: Option<&str>,
        t0: i64,
        t1: i64,
    ) -> Result<Vec<OutRow>> {
        let Some((name, labels)) = ({
            let reg = self.shared.engine.series_read();
            reg.info_for(series_id).and_then(|info| {
                expected_name
                    .is_none_or(|expected| info.metric_name == expected)
                    .then(|| (info.metric_name.clone(), info.labels.clone()))
            })
        }) else {
            return Ok(Vec::new());
        };

        let labels_json = labels_to_json(&labels);
        self.shared
            .engine
            .query_range_by_id(series_id, t0, t1)
            .map_err(module_err)
            .map(|points| {
                points
                    .into_iter()
                    .map(|(ts, value)| OutRow {
                        series_id,
                        name: name.clone(),
                        ts,
                        value,
                        labels_json: labels_json.clone(),
                    })
                    .collect()
            })
    }

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
            let reg = self.shared.engine.series_read();
            reg.find_series(metric, &BTreeMap::new())
                .into_iter()
                .filter_map(|sid| reg.info_for(sid).map(|info| (sid, info.labels.clone())))
                .collect()
        };

        let mut out = Vec::new();
        for (sid, labels) in candidates {
            let points = self
                .shared
                .engine
                .query_range_by_id(sid, t0, t1)
                .map_err(module_err)?;
            if points.is_empty() {
                continue;
            }
            let labels_json = labels_to_json(&labels);
            for (ts, value) in points {
                out.push(OutRow {
                    series_id: sid,
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
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        // Route chunk reads to the connection running this SELECT.
        let _bind = DbGuard::bind(self.db);
        let _read = self
            .shared
            .write_gate
            .acquire_read(self.db as usize, &self.table_name)
            .map_err(module_err)?;
        self.shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;

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
        let mut impossible = idx_num & 1 != 0 && name.is_none();
        // Unconstrained bounds cover the full i64 range. A pushed NULL
        // bound makes the SQL predicate UNKNOWN, so the scan is empty.
        let t0: i64 = if idx_num & 2 != 0 {
            let v: Option<i64> = args.get(arg)?;
            arg += 1;
            match v {
                Some(v) => v,
                None => {
                    impossible = true;
                    i64::MIN
                }
            }
        } else {
            i64::MIN
        };
        let t1: i64 = if idx_num & 4 != 0 {
            let value = args.get::<Option<i64>>(arg)?;
            arg += 1;
            match value {
                Some(v) => v,
                None => {
                    impossible = true;
                    i64::MAX
                }
            }
        } else {
            i64::MAX
        };
        let series_id = if idx_num & 8 != 0 {
            match integer_affinity(args.get::<Value>(arg)?) {
                Some(series_id) => Some(series_id),
                None => {
                    impossible = true;
                    None
                }
            }
        } else {
            None
        };

        let mut rows = Vec::new();
        if !impossible {
            if let Some(series_id) = series_id {
                rows = self.collect_series(series_id, name.as_deref(), t0, t1)?;
            } else if idx_num & 1 != 0 {
                // Name pushdown: only this metric's series.
                if let Some(name) = name {
                    rows = self.collect_metric(&name, t0, t1)?;
                }
            } else {
                // Full scan: every metric the registry knows about.
                let metrics = self.shared.engine.series_read().list_metrics();
                for metric in metrics {
                    rows.extend(self.collect_metric(&metric, t0, t1)?);
                }
            }
        }

        // Deterministic output order: ts ascending, then name/labels as
        // tie-breakers (points inside one series are already ts-sorted,
        // but rows from different series interleave).
        rows.sort_by(|a, b| (a.ts, &a.name, &a.labels_json).cmp(&(b.ts, &b.name, &b.labels_json)));

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
            4 => ctx.set_result(&row.series_id),
            // 5 = the hidden command column: always NULL when read.
            _ => ctx.set_result(&Null),
        }
    }

    /// Synthetic rowid = position in the materialized result. Only stable
    /// within one scan, which is all SQLite requires of us here.
    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::validate_named_series_count;
    use rusqlite::Error;

    #[test]
    fn named_batch_rejects_unrepresentable_series_table_before_allocation() {
        let error = validate_named_series_count(u32::MAX as usize, 0).unwrap_err();
        let Error::ModuleError(message) = error else {
            panic!("expected module error");
        };
        assert!(
            message.contains("n_series overflows minimum series table length")
                || (message.contains("4294967295 series require at least")
                    && message.contains("but only 0 remain"))
        );
    }
}
