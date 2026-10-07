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
//!   TEXT  → maintenance/configuration command: `flush` | `compact` |
//!           `compact-step:<series>[:<points>:<bytes>][:<cutoff>[:<sweep>]]` |
//!           `prune:<unix_ts>` |
//!           `prune-after:<unix_ts>` (repair chunks stored under a mistaken
//!           timestamp unit) | `rollups:none|<ladder>` | `clear-rollups` |
//!           `clear-rollups-step:<chunks>`
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
use crate::schema;
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
                                                // Keep ingest flushes cheap and make first compression an explicit bounded
                                                // maintenance phase. The size-tiered planner never mixes these raw arrivals
                                                // directly into an existing compressed tail.
const DEFER_COMPRESSION: bool = true;
/// F2 retention unit conversion: metrics ts is epoch SECONDS.
const NATIVE_PER_SECOND: i64 = 1;
const PLAN_LIMIT: &str = "limit";
const PLAN_LIMIT_OFFSET: &str = "limit-offset";

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
    /// Address plus connection-registration generation. Unlike `db`, this
    /// cannot alias a later connection if SQLite reuses the allocation.
    connection: shared::ConnectionIdentity,
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
    pub(crate) fn upgrade_legacy_schema(
        handle: *mut ffi::sqlite3,
        database: &str,
        table: &str,
    ) -> Result<()> {
        let _bind = DbGuard::bind(handle);
        let host = unsafe { Connection::from_handle(handle) }?;
        host.execute_batch(&shadow_store::series_ddl(database, table))?;
        shadow_store::ensure_max_ts_val_column(&host, database, table)?;
        shadow_meta::ensure_instance_id(&host, database, table).map_err(module_err)?;
        let store = ShadowTableStore::new(database, table);
        Engine::with_store(
            Box::new(store),
            FLUSH_THRESHOLD,
            MIN_FLUSH_SIZE,
            COMPRESSION_LEVEL,
            MEMORY_BUDGET,
            DEFER_COMPRESSION,
        )
        .map_err(module_err)?;
        Ok(())
    }

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
        let connection = shared::connection_identity(handle);
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
            shadow_store::ensure_max_ts_val_column(&host, &database, &table)?;
            // Observability schema companions (#20/#21), same contract
            // as traces/logs: same-transaction install, explicit upgrades,
            // owned-only removal on drop.
            schema::install_metric_views(&host, &database, &table)
                .map_err(|error| module_err(format!("install observability schema: {error}")))?;
        } else {
            shadow_store::require_read_schema(&host, &database, &table)?;
        }
        let instance_id = if is_create {
            shadow_meta::ensure_instance_id(&host, &database, &table)
        } else {
            shadow_meta::require_instance_id(&host, &database, &table)
        }
        .map_err(module_err)?;
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
            let store = if is_create {
                ShadowTableStore::new(&database, &table)
            } else {
                ShadowTableStore::new_read_only(&database, &table)
            };
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
                connection,
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
        shadow_store::require_read_schema(&host, database, table)?;
        let instance_id =
            shadow_meta::require_instance_id(&host, database, table).map_err(module_err)?;
        let key = shared::registry_key(handle, database.as_bytes(), table, instance_id);
        let shared = shared::get_or_create(&key, || {
            let store = ShadowTableStore::new_read_only(database, table);
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
            .acquire(self.connection, &self.table_name)
            .map_err(module_err)?;
        self.gate_held = true;
        Ok(())
    }

    /// Release the gate at the end of this connection's transaction
    /// (commit or rollback). No-op if this connection never wrote.
    fn release_write_gate(&mut self) {
        if self.gate_held {
            self.shared.write_gate.release(self.connection);
            self.gate_held = false;
        }
    }

    /// Handle a hidden-column command insert. Returns the (synthetic,
    /// meaningless) rowid 0 — commands do not create rows.
    pub(crate) fn run_command(&self, cmd: &str) -> Result<i64> {
        if cmd == "schema" {
            let host = unsafe { Connection::from_handle(self.db) }?;
            schema::install_metric_views(&host, &self.database_name, &self.table_name)?;
        } else if cmd == "flush" {
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
        } else if let Some(raw_budget) = cmd.strip_prefix("compact-step:") {
            let invalid = || {
                module_err(format!(
                    "compact-step: expected 'compact-step:<positive series>[:<positive points>:<positive bytes>][:<cutoff unix seconds>[:<positive sweep>]]', got {cmd:?}"
                ))
            };
            let parts: Vec<_> = raw_budget.trim().split(':').collect();
            if !matches!(parts.len(), 1..=5) {
                return Err(invalid());
            }
            let series: usize = parts[0].parse().map_err(|_| invalid())?;
            let (points, bytes) = if parts.len() >= 3 {
                (
                    parts[1].parse().map_err(|_| invalid())?,
                    parts[2].parse().map_err(|_| invalid())?,
                )
            } else {
                (
                    timeless_core::METRICS_COMPACTION_STEP_INPUT_POINTS,
                    timeless_core::METRICS_COMPACTION_STEP_INPUT_BYTES,
                )
            };
            // The cutoff is the sweep's: chunks whose newest sample is at or
            // past it are left for the next sweep, so that a sweep over a
            // store that is being written to can end. A sweep keeps passing
            // the same one, and is planned once for it. Without one, every
            // chunk is eligible, as the unbounded command has it.
            let cutoff_ts: i64 = match parts.len() {
                2 => parts[1].parse().map_err(|_| invalid())?,
                4 | 5 => parts[3].parse().map_err(|_| invalid())?,
                _ => i64::MAX,
            };
            // A host that names its sweep gets one rollup cycle per sweep
            // (#123); without a name, every step may begin a cycle.
            let sweep: Option<u64> = match parts.len() {
                5 => Some(
                    parts[4]
                        .parse()
                        .ok()
                        .filter(|sweep| *sweep > 0)
                        .ok_or_else(invalid)?,
                ),
                _ => None,
            };
            if series == 0 || points == 0 || bytes == 0 {
                return Err(invalid());
            }
            // One public INSERT is one SQLite transaction and therefore one
            // writer-gate hold. Bound raw-series and rollup-group work to the
            // same small budget; the server repeats this command with a pause
            // between transactions until both backlogs complete a cycle.
            let (_, _, raw_more) = self
                .shared
                .engine
                .compact_partitions_budgeted(
                    cutoff_ts,
                    timeless_core::MetricsCompactionBudget {
                        max_series: series,
                        max_input_points: points,
                        max_input_bytes: bytes,
                    },
                )
                .map_err(module_err)?;
            let (_, _, rollup_more) = match sweep {
                Some(sweep) => self.shared.engine.rollup_bounded_for_sweep(series, sweep),
                None => self.shared.engine.rollup_bounded(series),
            }
            .map_err(module_err)?;
            return Ok(i64::from(raw_more || rollup_more));
        } else if cmd == "rollup" {
            // F3: produce settled buckets for every declared tier. A
            // no-op (0 chunks) without a rollups= ladder.
            self.shared.engine.rollup().map_err(module_err)?;
        } else if let Some(spec) = cmd.strip_prefix("rollups:") {
            let host = unsafe { Connection::from_handle(self.db) }?;
            let spec = spec.trim();
            if spec.eq_ignore_ascii_case("none") {
                shadow_meta::delete_meta_key(
                    &host,
                    &self.database_name,
                    &self.table_name,
                    "rollups",
                )
                .map_err(module_err)?;
                self.shared.engine.set_rollups_transactional(Vec::new());
            } else {
                let (tiers, persisted) =
                    table_args::parse_rollups(spec, NATIVE_PER_SECOND).map_err(module_err)?;
                shadow_meta::save_meta_text(
                    &host,
                    &self.database_name,
                    &self.table_name,
                    "rollups",
                    &persisted,
                )
                .map_err(module_err)?;
                self.shared.engine.set_rollups_transactional(tiers);
            }
        } else if let Some(raw_budget) = cmd.strip_prefix("clear-rollups-step:") {
            if !self.shared.engine.rollup_tiers().is_empty() {
                return Err(module_err(
                    "clear-rollups-step requires rollups:none first; refusing to delete an active tier"
                        .into(),
                ));
            }
            let budget: usize = raw_budget.trim().parse().map_err(|_| {
                module_err(format!(
                    "clear-rollups-step: expected 'clear-rollups-step:<positive chunks>', got {cmd:?}"
                ))
            })?;
            if budget == 0 {
                return Err(module_err(
                    "clear-rollups-step: chunk budget must be positive".into(),
                ));
            }
            let (_deleted, more, errors) = self.shared.engine.clear_rollups_bounded(budget);
            if !errors.is_empty() {
                return Err(module_err(format!(
                    "clear-rollups-step errors: {}",
                    errors.join("; ")
                )));
            }
            return Ok(i64::from(more));
        } else if cmd == "clear-rollups" {
            if !self.shared.engine.rollup_tiers().is_empty() {
                return Err(module_err(
                    "clear-rollups requires rollups:none first; refusing to delete an active tier"
                        .into(),
                ));
            }
            let (deleted, _more, errors) = self.shared.engine.clear_rollups_batch();
            if !errors.is_empty() {
                return Err(module_err(format!(
                    "clear-rollups errors: {}",
                    errors.join("; ")
                )));
            }
            return i64::try_from(deleted)
                .map_err(|_| module_err("clear-rollups count exceeds i64::MAX".into()));
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
        } else if let Some(ts_str) = cmd.strip_prefix("prune-after:") {
            // Repair: drop whole chunks whose coverage STARTS after the cutoff,
            // raw and rollup. This is the bounded operator path for chunks
            // stored under a mistaken timestamp unit (e.g. milliseconds in a
            // seconds store), which retention can never remove because they
            // sit newer than every cutoff. Returns 1 while more rollup chunks
            // remain to sweep.
            let ts: i64 = ts_str.trim().parse().map_err(|_| {
                module_err(format!(
                    "prune-after: expected 'prune-after:<unix_ts>', got {cmd:?}"
                ))
            })?;
            let (_deleted, more, errors) = self.shared.engine.prune_after(ts);
            if !errors.is_empty() {
                return Err(module_err(format!(
                    "prune-after errors: {}",
                    errors.join("; ")
                )));
            }
            return Ok(i64::from(more));
        } else {
            return Err(module_err(format!(
                "unknown command {cmd:?}; supported: 'schema', 'flush', 'compact', \
                 'compact-step:<series>[:<points>:<bytes>][:<cutoff>[:<sweep>]]', 'rollup', \
                 'rollups:none|<ladder>', \
                 'clear-rollups', 'clear-rollups-step:<chunks>', \
                 'prune:<unix_ts>', 'prune-after:<unix_ts>'"
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
        let reserved = r.take(2, "reserved header bytes")?;
        if reserved[0] != 0 || reserved[1] != 0 {
            return Err(module_err(format!(
                "batch blob: reserved bytes must be zero (got {:02x}{:02x})",
                reserved[0], reserved[1]
            )));
        }
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
        let idx_bytes = r.take_array(n_points, 4, "per-point series index column")?;
        let ts_bytes = r.take_array(n_points, 8, "timestamp column")?;
        let val_bytes = r.take_array(n_points, 8, "value column")?;
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

        // ── 6. Scatter points into per-series runs and append once ──
        // Counting sort on the dense series index: one partition lookup,
        // one reserve+extend, and one memory update per series instead of
        // per point. No intermediate byte packing — columns decode
        // straight into the flat per-series runs. All-or-nothing holds:
        // every index was validated in step 4 (and rechecked below).
        let mut counts: Vec<usize> = Vec::new();
        counts.try_reserve_exact(n_series).map_err(|_| {
            module_err(format!(
                "batch blob: cannot allocate series counts for {n_series} entries"
            ))
        })?;
        counts.resize(n_series, 0);
        for chunk in idx_bytes.as_chunks::<4>().0 {
            let idx = u32::from_le_bytes(*chunk) as usize;
            *counts.get_mut(idx).ok_or_else(|| {
                module_err(format!("batch blob: series index {idx} out of range"))
            })? += 1;
        }
        let mut starts: Vec<usize> = Vec::new();
        starts
            .try_reserve_exact(n_series.saturating_add(1))
            .map_err(|_| module_err("batch blob: cannot allocate series run table".into()))?;
        let mut acc = 0usize;
        starts.push(0);
        for &c in &counts {
            acc = acc
                .checked_add(c)
                .ok_or_else(|| module_err("batch blob: point count overflows run table".into()))?;
            starts.push(acc);
        }
        let mut flat_ts: Vec<i64> = Vec::new();
        flat_ts.try_reserve_exact(n_points).map_err(|_| {
            module_err(format!(
                "batch blob: cannot allocate timestamp run for {n_points} points"
            ))
        })?;
        flat_ts.resize(n_points, 0);
        let mut flat_val: Vec<f64> = Vec::new();
        flat_val.try_reserve_exact(n_points).map_err(|_| {
            module_err(format!(
                "batch blob: cannot allocate value run for {n_points} points"
            ))
        })?;
        flat_val.resize(n_points, 0.0);
        let mut pos: Vec<usize> = Vec::new();
        pos.try_reserve_exact(n_series)
            .map_err(|_| module_err("batch blob: cannot allocate series write cursors".into()))?;
        pos.extend_from_slice(
            starts
                .get(..n_series)
                .ok_or_else(|| module_err("batch blob: series run table is short".into()))?,
        );
        for i in 0..n_points {
            let idx = u32::from_le_bytes(BatchReader::fixed::<4>(
                idx_bytes,
                i,
                "series index column",
            )?) as usize;
            let slot = pos.get_mut(idx).ok_or_else(|| {
                module_err(format!(
                    "batch blob: point {i}: series index {idx} out of range"
                ))
            })?;
            let ts = i64::from_le_bytes(BatchReader::fixed::<8>(ts_bytes, i, "timestamp column")?);
            // Values stay opaque 8-byte payloads: from_bits/to_bits round-trips
            // without interpreting the float, so NaN payloads survive byte-exact.
            let val = f64::from_bits(u64::from_le_bytes(BatchReader::fixed::<8>(
                val_bytes,
                i,
                "value column",
            )?));
            if let (Some(slot_ts), Some(slot_val)) =
                (flat_ts.get_mut(*slot), flat_val.get_mut(*slot))
            {
                *slot_ts = ts;
                *slot_val = val;
            } else {
                return Err(module_err(format!(
                    "batch blob: point {i}: run slot out of range"
                )));
            }
            *slot += 1;
        }
        self.shared
            .engine
            .write_batch_partitioned(&sids, &starts, &flat_ts, &flat_val)
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
        let reserved = r.take(2, "reserved header bytes")?;
        if reserved[0] != 0 || reserved[1] != 0 {
            return Err(module_err(format!(
                "resolved batch: reserved bytes must be zero (got {:02x}{:02x})",
                reserved[0], reserved[1]
            )));
        }
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

        // Validate all ids before mutating any partition buffer, and
        // renumber them dense for the counting-sort scatter below.
        let mut dense_of: HashMap<i64, usize> = HashMap::new();
        let mut uniq: Vec<i64> = Vec::new();
        {
            let registry = self.shared.engine.series_read();
            for (i, bytes) in sid_bytes.as_chunks::<8>().0.iter().enumerate() {
                let sid = i64::from_le_bytes(*bytes);
                if registry.info_for(sid).is_none() {
                    return Err(module_err(format!(
                        "resolved batch: point {i}: unknown series id {sid}; batch rejected"
                    )));
                }
                if let std::collections::hash_map::Entry::Vacant(e) = dense_of.entry(sid) {
                    e.insert(uniq.len());
                    uniq.push(sid);
                }
            }
        }
        if uniq.len() > n_points {
            return Err(module_err(
                "resolved batch: more distinct series than points".into(),
            ));
        }

        let n_dense = uniq.len();
        let mut counts: Vec<usize> = Vec::new();
        counts
            .try_reserve_exact(n_dense)
            .map_err(|_| module_err("resolved batch: cannot allocate series counts".into()))?;
        counts.resize(n_dense, 0);
        for bytes in sid_bytes.as_chunks::<8>().0 {
            let sid = i64::from_le_bytes(*bytes);
            // Validated + interned in the loop above.
            let d = dense_of
                .get(&sid)
                .copied()
                .ok_or_else(|| module_err(format!("resolved batch: series id {sid} vanished")))?;
            *counts.get_mut(d).ok_or_else(|| {
                module_err(format!("resolved batch: series id {sid} out of range"))
            })? += 1;
        }
        let mut starts: Vec<usize> = Vec::new();
        starts
            .try_reserve_exact(n_dense.saturating_add(1))
            .map_err(|_| module_err("resolved batch: cannot allocate series run table".into()))?;
        let mut acc = 0usize;
        starts.push(0);
        for &c in &counts {
            acc = acc.checked_add(c).ok_or_else(|| {
                module_err("resolved batch: point count overflows run table".into())
            })?;
            starts.push(acc);
        }
        let mut flat_ts: Vec<i64> = Vec::new();
        flat_ts.try_reserve_exact(n_points).map_err(|_| {
            module_err(format!(
                "resolved batch: cannot allocate timestamp run for {n_points} points"
            ))
        })?;
        flat_ts.resize(n_points, 0);
        let mut flat_val: Vec<f64> = Vec::new();
        flat_val.try_reserve_exact(n_points).map_err(|_| {
            module_err(format!(
                "resolved batch: cannot allocate value run for {n_points} points"
            ))
        })?;
        flat_val.resize(n_points, 0.0);
        let mut pos: Vec<usize> = Vec::new();
        pos.try_reserve_exact(n_dense).map_err(|_| {
            module_err("resolved batch: cannot allocate series write cursors".into())
        })?;
        pos.extend_from_slice(
            starts
                .get(..n_dense)
                .ok_or_else(|| module_err("resolved batch: series run table is short".into()))?,
        );
        for i in 0..n_points {
            let sid = i64::from_le_bytes(BatchReader::fixed::<8>(
                sid_bytes,
                i,
                "resolved batch series id column",
            )?);
            let d = dense_of.get(&sid).copied().ok_or_else(|| {
                module_err(format!(
                    "resolved batch: point {i}: unknown series id {sid}"
                ))
            })?;
            let slot = pos.get_mut(d).ok_or_else(|| {
                module_err(format!("resolved batch: point {i}: run slot out of range"))
            })?;
            let ts = i64::from_le_bytes(BatchReader::fixed::<8>(
                ts_bytes,
                i,
                "resolved batch timestamp column",
            )?);
            let val = f64::from_bits(u64::from_le_bytes(BatchReader::fixed::<8>(
                val_bytes,
                i,
                "resolved batch value column",
            )?));
            if let (Some(slot_ts), Some(slot_val)) =
                (flat_ts.get_mut(*slot), flat_val.get_mut(*slot))
            {
                *slot_ts = ts;
                *slot_val = val;
            } else {
                return Err(module_err(format!(
                    "resolved batch: point {i}: run slot out of range"
                )));
            }
            *slot += 1;
        }
        self.shared
            .engine
            .write_batch_partitioned(&uniq, &starts, &flat_ts, &flat_val)
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
        let mut limit_c: Option<usize> = None;
        let mut offset_c: Option<usize> = None;
        let mut bounded_safe = info.num_of_order_by() == 0;
        for (i, c) in info.constraints().enumerate() {
            if !c.is_usable() {
                if !matches!(
                    c.operator(),
                    SQLITE_INDEX_CONSTRAINT_LIMIT | SQLITE_INDEX_CONSTRAINT_OFFSET
                ) {
                    bounded_safe = false;
                }
                continue;
            }
            match (c.column(), c.operator()) {
                (_, SQLITE_INDEX_CONSTRAINT_LIMIT) if limit_c.is_none() => limit_c = Some(i),
                (_, SQLITE_INDEX_CONSTRAINT_OFFSET) if offset_c.is_none() => offset_c = Some(i),
                (0, SQLITE_INDEX_CONSTRAINT_EQ) if name_c.is_none() => name_c = Some(i),
                (1, SQLITE_INDEX_CONSTRAINT_GE) | (1, SQLITE_INDEX_CONSTRAINT_GT)
                    if lo_c.is_none() =>
                {
                    lo_c = Some(i);
                    bounded_safe &= c.operator() == SQLITE_INDEX_CONSTRAINT_GE;
                }
                (1, SQLITE_INDEX_CONSTRAINT_LE) | (1, SQLITE_INDEX_CONSTRAINT_LT)
                    if hi_c.is_none() =>
                {
                    hi_c = Some(i);
                    bounded_safe &= c.operator() == SQLITE_INDEX_CONSTRAINT_LE;
                }
                (4, SQLITE_INDEX_CONSTRAINT_EQ) if series_c.is_none() => series_c = Some(i),
                _ => bounded_safe = false,
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
        if bounded_safe && limit_c.is_some() {
            if let Some(i) = limit_c {
                info.constraint_usage(i).set_argv_index(slot);
                slot += 1;
            }
            if let Some(i) = offset_c {
                info.constraint_usage(i).set_argv_index(slot);
            }
            info.set_idx_str(if offset_c.is_some() {
                PLAN_LIMIT_OFFSET
            } else {
                PLAN_LIMIT
            });
            info.set_estimated_rows(100);
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
            connection: self.connection,
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
        shared::pin_for_drop(self.db, self.connection, &self.key, &self.shared);
        let _bind = DbGuard::bind(self.db);
        let host = unsafe { Connection::from_handle(self.db) }?;
        schema::drop_objects(&host, &self.database_name, &self.table_name)
            .map_err(|error| module_err(format!("remove observability schema: {error}")))?;
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
                    // Empty/whitespace labels mean no labels, matching the
                    // batch path (empty field = {}) and TVF filters.
                    Some(txt) if txt.trim().is_empty() => HashMap::new(),
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
/// ALL commands (`flush`, `compact`, `prune:<ts>`, rollup configuration and
/// cleanup) are allowed inside explicit transactions and roll back fully —
/// the journal covers their index/configuration mutations, and their row/meta
/// mutations ride the host transaction.
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
    connection: shared::ConnectionIdentity,
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
        capacity: Option<usize>,
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
        let points = match capacity {
            Some(capacity) => self
                .shared
                .engine
                .query_range_prefix_by_id(series_id, t0, t1, capacity),
            None => self.shared.engine.query_range_by_id(series_id, t0, t1),
        };
        points.map_err(module_err).map(|points| {
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
    fn collect_metric(
        &self,
        metric: &str,
        t0: i64,
        t1: i64,
        capacity: Option<usize>,
    ) -> Result<Vec<OutRow>> {
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
            let remaining = capacity.map(|capacity| capacity.saturating_sub(out.len()));
            if remaining == Some(0) {
                break;
            }
            let points = match remaining {
                Some(remaining) => self
                    .shared
                    .engine
                    .query_range_prefix_by_id(sid, t0, t1, remaining),
                None => self.shared.engine.query_range_by_id(sid, t0, t1),
            }
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
    fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        // Route chunk reads to the connection running this SELECT.
        let _bind = DbGuard::bind(self.db);
        let _read = self
            .shared
            .write_gate
            .acquire_read(self.connection, &self.table_name)
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
            let value = args.get::<Value>(arg)?;
            arg += 1;
            match integer_affinity(value) {
                Some(series_id) => Some(series_id),
                None => {
                    impossible = true;
                    None
                }
            }
        } else {
            None
        };
        let capacity = if matches!(idx_str, Some(PLAN_LIMIT | PLAN_LIMIT_OFFSET)) {
            let limit: Option<i64> = args.get(arg)?;
            arg += 1;
            let offset: Option<i64> = if idx_str == Some(PLAN_LIMIT_OFFSET) {
                args.get(arg)?
            } else {
                Some(0)
            };
            match (limit, offset) {
                (Some(0), _) => Some(0),
                (Some(limit), Some(offset)) if limit > 0 => limit
                    .checked_add(offset.max(0))
                    .and_then(|value| usize::try_from(value).ok()),
                _ => None,
            }
        } else {
            None
        };

        let mut rows = Vec::new();
        if !impossible {
            if let Some(series_id) = series_id {
                rows = self.collect_series(series_id, name.as_deref(), t0, t1, capacity)?;
            } else if idx_num & 1 != 0 {
                // Name pushdown: only this metric's series.
                if let Some(name) = name {
                    rows = self.collect_metric(&name, t0, t1, capacity)?;
                }
            } else {
                // Full scan: every metric the registry knows about.
                let metrics = self.shared.engine.series_read().list_metrics();
                for metric in metrics {
                    let remaining = capacity.map(|capacity| capacity.saturating_sub(rows.len()));
                    if remaining == Some(0) {
                        break;
                    }
                    rows.extend(self.collect_metric(&metric, t0, t1, remaining)?);
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
        // `eof` guards the position, but a desync must be a SQL error,
        // never a Rust panic across the FFI boundary.
        let row = self
            .rows
            .get(self.pos)
            .ok_or_else(|| module_err("metrics cursor has no current row".into()))?;
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

#[cfg(all(test, feature = "embedded"))]
mod schema_tests {
    use super::*;

    fn inventory_names(db: &Connection) -> Vec<(String, String, i64)> {
        let mut stmt = db
            .prepare(
                "SELECT object_name, object_kind, schema_version \
                 FROM timeless_schema_inventory ORDER BY object_name",
            )
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[test]
    fn schema_installs_series_and_latest() {
        // Phase 4 (#22 MVP): series catalog plus latest values over the
        // public TVFs and base table, inventoried exactly.
        let db = Connection::open_in_memory().unwrap();
        crate::register_telemetry(&db).unwrap();
        db.execute_batch("CREATE VIRTUAL TABLE metrics USING timeless_metrics;")
            .unwrap();
        assert_eq!(
            inventory_names(&db),
            vec![
                ("timeless_metrics_latest".to_string(), "view".to_string(), 1),
                (
                    "timeless_metrics_series".to_string(),
                    "table".to_string(),
                    2
                ),
            ]
        );
        // Same series, labels in different key order across inserts:
        // canonical grouping must keep it one series.
        db.execute_batch(
            "INSERT INTO metrics(name, ts, value, labels) VALUES \
             ('cpu', 1700000000, 1.5, '{\"host\":\"a\",\"region\":\"x\"}'); \
             INSERT INTO metrics(name, ts, value, labels) VALUES \
             ('cpu', 1700000060, 2.5, '{\"region\":\"x\",\"host\":\"a\"}'); \
             INSERT INTO metrics(name, ts, value, labels) VALUES \
             ('mem', 1700000000, 512.0, '{\"host\":\"a\"}');",
        )
        .unwrap();
        db.execute("INSERT INTO metrics(metrics) VALUES ('flush');", [])
            .unwrap();

        let series: Vec<(String, String)> = db
            .prepare("SELECT name, labels FROM timeless_metrics_series ORDER BY name, labels;")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            series,
            vec![
                (
                    "cpu".to_string(),
                    "{\"host\":\"a\",\"region\":\"x\"}".to_string()
                ),
                ("mem".to_string(), "{\"host\":\"a\"}".to_string()),
            ]
        );

        let latest: Vec<(String, i64, f64, String)> = db
            .prepare("SELECT name, ts, value, ts_time FROM timeless_metrics_latest;")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(latest.len(), 2, "one row per series: {latest:?}");
        let cpu = latest.iter().find(|row| row.0 == "cpu").unwrap();
        assert_eq!((cpu.1, cpu.2), (1700000060, 2.5), "newest wins");
        assert!(
            cpu.3.starts_with("2023-") && cpu.3.ends_with(".000Z"),
            "friendly UTC seconds: {}",
            cpu.3
        );
    }

    #[test]
    fn schema_drop_removes_owned_views() {
        let db = Connection::open_in_memory().unwrap();
        crate::register_telemetry(&db).unwrap();
        db.execute_batch(
            "CREATE VIRTUAL TABLE metrics USING timeless_metrics; \
             CREATE TABLE user_data(id INTEGER);",
        )
        .unwrap();
        db.execute_batch("DROP TABLE metrics;").unwrap();
        let names: Vec<String> = db
            .prepare("SELECT name FROM sqlite_master ORDER BY name;")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            !names.iter().any(|n| n.starts_with("timeless_metrics_")),
            "{names:?}"
        );
        assert!(names.contains(&"user_data".to_string()));
        assert!(inventory_names(&db).is_empty());
    }

    use rusqlite::params;

    fn series_rows(db: &Connection, table: &str) -> Vec<String> {
        db.prepare(&format!("SELECT name FROM {table}_series ORDER BY name"))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn stat(db: &Connection, table: &str, key: &str) -> i64 {
        db.query_row(
            "SELECT value FROM timeless_stats(?1) WHERE key = ?2",
            params![table, key],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// A series retention has left nothing of goes with its last chunk:
    /// out of the catalog row, the registry, and the public views. A
    /// rollup chunk keeps it, until that expires too.
    #[test]
    fn retention_removes_a_series_with_its_last_chunk() {
        let db = Connection::open_in_memory().unwrap();
        crate::register_telemetry(&db).unwrap();
        db.execute_batch(
            "CREATE VIRTUAL TABLE m USING timeless_metrics(retention='100s', rollups='10s@1000s');",
        )
        .unwrap();
        let write = |name: &str, ts: i64| {
            db.execute(
                "INSERT INTO m(name, ts, value, labels) VALUES (?1, ?2, 1.0, '{}')",
                params![name, ts],
            )
            .unwrap();
        };
        let command = |what: &str| {
            db.execute("INSERT INTO m(m) VALUES (?1)", [what]).unwrap();
        };
        // Both begin at 1000; `kept` goes on to 1050, which settles the
        // 1000 bucket, and the rollup for both is built.
        write("gone", 1000);
        write("kept", 1000);
        write("kept", 1050);
        command("flush");
        command("compact");
        assert_eq!(series_rows(&db, "m"), ["gone", "kept"]);
        assert_eq!(stat(&db, "m", "rollup_chunks"), 2);

        // At 1200 the raw cutoff is 1100: `gone`'s only raw chunk is
        // pruned, and its rollup chunk keeps it a series.
        write("kept", 1200);
        command("flush");
        assert_eq!(
            series_rows(&db, "m"),
            ["gone", "kept"],
            "kept by its rollup"
        );
        assert_eq!(stat(&db, "m", "retention_series_removed"), 0);

        // At 2100 the rollup cutoff is 1100: the rollup goes, and with it
        // the series.
        write("kept", 2100);
        command("flush");
        assert_eq!(series_rows(&db, "m"), ["kept"]);
        assert_eq!(stat(&db, "m", "retention_series_removed"), 1);
        assert_eq!(stat(&db, "m", "series"), 1);
        let names: Vec<String> = db
            .prepare("SELECT name FROM timeless_m_series ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(names, ["kept"], "and out of the public catalog");

        // The name is free to be a new series.
        write("gone", 2100);
        command("flush");
        assert_eq!(series_rows(&db, "m"), ["gone", "kept"]);
        let points: i64 = db
            .query_row("SELECT count(*) FROM m WHERE name = 'gone'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(points, 1);
    }

    /// Removed inside a transaction that is rolled back, the series is
    /// back: its row with SQLite's rollback, its registry entry with the
    /// engine's journal, and its data readable as before.
    #[test]
    fn rollback_restores_a_series_retention_removed() {
        let db = Connection::open_in_memory().unwrap();
        crate::register_telemetry(&db).unwrap();
        db.execute_batch("CREATE VIRTUAL TABLE m USING timeless_metrics(retention='100s');")
            .unwrap();
        let write = |name: &str, ts: i64| {
            db.execute(
                "INSERT INTO m(name, ts, value, labels) VALUES (?1, ?2, 1.0, '{}')",
                params![name, ts],
            )
            .unwrap();
        };
        write("gone", 1000);
        write("kept", 1000);
        db.execute("INSERT INTO m(m) VALUES ('flush')", []).unwrap();
        let id: i64 = db
            .query_row("SELECT id FROM m_series WHERE name = 'gone'", [], |r| {
                r.get(0)
            })
            .unwrap();

        db.execute_batch("BEGIN;").unwrap();
        write("kept", 1200);
        db.execute("INSERT INTO m(m) VALUES ('flush')", []).unwrap();
        assert_eq!(
            series_rows(&db, "m"),
            ["kept"],
            "removed inside the transaction"
        );
        db.execute_batch("ROLLBACK;").unwrap();

        assert_eq!(series_rows(&db, "m"), ["gone", "kept"]);
        let again: i64 = db
            .query_row("SELECT id FROM m_series WHERE name = 'gone'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(again, id, "with the id it had");
        let points: Vec<i64> = db
            .prepare("SELECT ts FROM m WHERE name = 'gone'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(points, [1000], "and its chunk, and its place in the index");
        assert_eq!(stat(&db, "m", "series"), 2);
    }

    /// Another connection on the same database sees the removal: the
    /// catalog token changes, and its next refresh drops the series.
    #[test]
    fn another_connection_sees_a_removed_series() {
        let dir = std::env::temp_dir().join(format!(
            "timeless_ext_series_removal_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("m.db");
        let writer = Connection::open(&path).unwrap();
        crate::register_telemetry(&writer).unwrap();
        writer
            .execute_batch("CREATE VIRTUAL TABLE m USING timeless_metrics(retention='100s');")
            .unwrap();
        let write = |name: &str, ts: i64| {
            writer
                .execute(
                    "INSERT INTO m(name, ts, value, labels) VALUES (?1, ?2, 1.0, '{}')",
                    params![name, ts],
                )
                .unwrap();
        };
        write("gone", 1000);
        write("kept", 1000);
        writer
            .execute("INSERT INTO m(m) VALUES ('flush')", [])
            .unwrap();

        let reader = Connection::open(&path).unwrap();
        crate::register_telemetry(&reader).unwrap();
        let catalog = |db: &Connection| -> Vec<String> {
            db.prepare("SELECT name FROM timeless_m_series ORDER BY name")
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        assert_eq!(catalog(&reader), ["gone", "kept"]);

        write("kept", 1200);
        writer
            .execute("INSERT INTO m(m) VALUES ('flush')", [])
            .unwrap();
        assert_eq!(catalog(&reader), ["kept"]);
        assert_eq!(stat(&reader, "m", "series"), 1);

        drop(reader);
        drop(writer);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Rollup chunks written a pass at a time are merged into few, and
    /// what they say does not change.
    #[test]
    fn rollup_chunks_are_merged_and_say_the_same() {
        let db = Connection::open_in_memory().unwrap();
        crate::register_telemetry(&db).unwrap();
        db.execute_batch("CREATE VIRTUAL TABLE m USING timeless_metrics(rollups='10s@0');")
            .unwrap();
        let rollup = |db: &Connection| -> Vec<(i64, f64)> {
            db.prepare(
                "SELECT ts, value FROM timeless_rollup('m', 'cpu', NULL, 10, 0, 1000000, 'sum') \
                 ORDER BY ts",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
        };
        // Twenty passes, each settling one more bucket of two samples, and
        // each writing a rollup chunk of its own: as an hourly pass does.
        let mut expected = Vec::new();
        for pass in 0..20_i64 {
            let bucket = 1000 + pass * 10;
            for (ts, value) in [(bucket, 1.0), (bucket + 5, 2.0)] {
                db.execute(
                    "INSERT INTO m(name, ts, value, labels) VALUES ('cpu', ?1, ?2, '{}')",
                    params![ts, value],
                )
                .unwrap();
            }
            // A bucket settles once a whole bucket has passed it.
            db.execute(
                "INSERT INTO m(name, ts, value, labels) VALUES ('cpu', ?1, 0.0, '{}')",
                params![bucket + 20],
            )
            .unwrap();
            db.execute("INSERT INTO m(m) VALUES ('flush')", []).unwrap();
            db.execute("INSERT INTO m(m) VALUES ('rollup')", [])
                .unwrap();
            expected.push((bucket, 3.0));
        }
        let chunks = stat(&db, "m", "rollup_chunks");
        assert!(chunks < 8, "twenty passes left {chunks} chunks, not twenty");
        assert!(stat(&db, "m", "rollup_merge_chunks_removed") >= 12);
        assert!(stat(&db, "m", "rollup_merge_chunks_written") >= 1);
        let rolled = rollup(&db);
        // The settle margin keeps the last bucket or two in raw.
        assert!(rolled.len() >= 18, "{rolled:?}");
        assert_eq!(rolled, expected[..rolled.len()], "every bucket as it was");

        // Rolled back, the pieces are back and say the same.
        let before = stat(&db, "m", "rollup_chunks");
        db.execute_batch("BEGIN;").unwrap();
        for pass in 20..30_i64 {
            let bucket = 1000 + pass * 10;
            db.execute(
                "INSERT INTO m(name, ts, value, labels) VALUES ('cpu', ?1, 3.0, '{}')",
                params![bucket],
            )
            .unwrap();
            db.execute(
                "INSERT INTO m(name, ts, value, labels) VALUES ('cpu', ?1, 0.0, '{}')",
                params![bucket + 20],
            )
            .unwrap();
            db.execute("INSERT INTO m(m) VALUES ('flush')", []).unwrap();
            db.execute("INSERT INTO m(m) VALUES ('rollup')", [])
                .unwrap();
        }
        assert!(rollup(&db).len() >= 28);
        db.execute_batch("ROLLBACK;").unwrap();
        assert_eq!(stat(&db, "m", "rollup_chunks"), before);
        assert_eq!(rollup(&db), rolled);
    }

    /// A shortened tier is applied at the next maintenance pass, and not
    /// once the newest sample has moved a slice of the old window.
    #[test]
    fn a_shortened_rollup_tier_is_applied_at_once() {
        let db = Connection::open_in_memory().unwrap();
        crate::register_telemetry(&db).unwrap();
        // A long window to begin with, so that maintenance has set its
        // floor under it.
        db.execute_batch("CREATE VIRTUAL TABLE m USING timeless_metrics(rollups='10s@100000s');")
            .unwrap();
        // Twelve passes of five buckets each, so the tier is in several
        // chunks and a cutoff can fall between them.
        for pass in 0..12_i64 {
            for bucket in (0..5).map(|n| 1000 + pass * 50 + n * 10) {
                db.execute(
                    "INSERT INTO m(name, ts, value, labels) VALUES ('cpu', ?1, 1.0, '{}')",
                    params![bucket],
                )
                .unwrap();
            }
            db.execute(
                "INSERT INTO m(name, ts, value, labels) VALUES ('cpu', ?1, 0.0, '{}')",
                params![1000 + pass * 50 + 60],
            )
            .unwrap();
            db.execute("INSERT INTO m(m) VALUES ('flush')", []).unwrap();
            db.execute("INSERT INTO m(m) VALUES ('compact')", [])
                .unwrap();
        }
        let buckets = |db: &Connection| -> i64 {
            db.query_row(
                "SELECT count(*) FROM timeless_rollup('m', 'cpu', NULL, 10, 0, 100000, 'sum')",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        let all = buckets(&db);
        assert!(all >= 55, "{all}");

        // Then for a hundred seconds: the next compaction
        // prunes the chunks wholly older than that behind the newest
        // sample, with nothing new written.
        db.execute("INSERT INTO m(m) VALUES ('rollups:10s@100s')", [])
            .unwrap();
        db.execute("INSERT INTO m(m) VALUES ('compact')", [])
            .unwrap();
        let left = buckets(&db);
        assert!(left < all / 2, "{left} buckets left of {all}");
    }
}
