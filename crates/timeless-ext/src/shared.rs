//! Cross-connection engine sharing (PLAN.md risk R4 — FIXED).
//!
//! ── THE PROBLEM ──────────────────────────────────────────────────────
//! sqld (and any pooled host) loads this extension into EVERY
//! connection, and SQLite instantiates one vtab object per connection
//! (xConnect) over the SAME shadow tables. Before this module, each
//! vtab instance built its own Engine: N connections = N private
//! buffers, N private chunk/block indexes, N series registries — split
//! state over one set of tables. Consequences: a point buffered on
//! connection A was invisible to B until a full reopen; B's engine
//! index went stale the moment A flushed; and two connections flushing
//! concurrently could interleave writes that each engine believed it
//! solely owned.
//!
//! ── THE FIX, IN THREE PARTS (this module provides all three) ────────
//! 1. A process-global ENGINE REGISTRY keyed by (canonical db file
//!    path, connection-local schema alias, table name, instance id).
//!    Extensions are one shared library per
//!    process, so a `static` here is exactly one registry per process,
//!    shared by every connection that loaded the .so. xCreate/xConnect
//!    upgrade the registry's Weak or build the engine once; every vtab
//!    instance holds an Arc to the same [`SharedEngine`]; xDisconnect
//!    drops the Arc (last one out frees the engine). xDestroy leaves
//!    the Weak entry alone: DROP rollback reuses the restored instance
//!    identity, while committed recreate cannot collide with it.
//!
//! 2. THREAD-LOCAL CONNECTION ROUTING. The engines call back into the
//!    shadow stores for every byte at rest — but a shared engine can be
//!    entered from ANY connection, and store SQL must run on the
//!    CALLING connection (that's where the transaction context lives,
//!    and re-entering a *different* connection would try to take a
//!    SQLite connection mutex some other thread may hold — deadlock).
//!    So the stores hold NO connection at all anymore: every vtab
//!    callback binds its host connection into a thread-local
//!    ([`DbGuard`], RAII, panic-safe) and the stores read it back via
//!    [`current_conn`]. A store call with nothing bound is a hard
//!    error — which also permanently guards the old rayon trap (an
//!    engine worker thread calling the store would find no binding
//!    instead of deadlocking on the host connection's mutex).
//!
//! 3. TRANSACTION ACCESS SERIALIZATION ([`WriterGate`]). Each engine keeps ONE
//!    transaction journal (R5), so only one connection may be inside a
//!    write transaction on a given table at a time. The gate is
//!    acquired in xBegin — which SQLite fires only at the FIRST WRITE
//!    statement of a transaction touching the vtab (never for reads),
//!    so acquisition is lazy in exactly the way that matters: read
//!    transactions and pure SELECT traffic never touch the gate. It is
//!    held across callbacks until that connection's xCommit/xRollback.
//!    A second writer waits up to [`WRITE_GATE_TIMEOUT`] and then
//!    fails with a clear "locked by another connection" error —
//!    SQLITE_BUSY semantics, not a hang.
//!
//!    WHY xBegin AND NOT the first insert: SQLite calls xBegin on
//!    connection B *before* B's first xUpdate. If B could reach
//!    engine.txn_begin() while A's transaction holds the gate, B would
//!    clobber A's active journal (txn_begin resets the marks — it
//!    debug-asserts against exactly this). Gating xBegin means the
//!    journal is provably single-writer: nobody activates it without
//!    holding the gate.
//!
//! ── DEADLOCK ANALYSIS (WriterGate vs SQLite's own file locks) ───────
//! The common case is safe by construction: a blocked writer B is
//! parked inside its own xBegin, BEFORE its statement wrote any page —
//! B holds no SQLite file write lock, so gate-holder A can always
//! finish its transaction and release. Two residual interleavings are
//! BOUNDED rather than impossible, and both degrade to a busy error:
//!   - B wrote to a PLAIN table earlier in its transaction (B holds
//!     the file's write lock) and then blocks on A's gate, while A
//!     needs the file write lock to commit shadow rows: A gets
//!     SQLITE_BUSY (bounded by A's busy_timeout), B times out on the
//!     gate after 5s. One of them errors; nothing hangs forever.
//!   - Two connections take two DIFFERENT tables' gates in opposite
//!     orders inside explicit transactions (classic lock-order
//!     inversion): both time out at 5s; the app retries — again,
//!     SQLITE_BUSY semantics.
//!
//! EMPIRICAL FACT (cli.sh section 21, VDBE bytecode verified): on
//! stock SQLite a vtab write statement executes OP_Transaction
//! (wrflag=1 → the file write lock) BEFORE OP_VBegin, so two writers
//! collide on SQLITE_BUSY at the file level before the second one can
//! even reach the gate. That makes the gate DEFENSE-IN-DEPTH on stock
//! SQLite — the layer that keeps the engine-global journal provably
//! single-writer no matter the host's locking behavior — and the
//! ACTIVE protection under hosts that relax writer exclusivity (libsql
//! BEGIN CONCURRENT / MVCC branches, where two file-level write
//! transactions CAN coexist). Its timeout path is unit-tested below
//! rather than through SQL, because stock SQLite cannot reach it.
//!
//! ── SHARED-BUFFER SEMANTICS ─────────────────────────────────────────
//! One engine per table means one in-memory buffer per table: committed but
//! unflushed points written by connection A are immediately visible to
//! connection B even though they remain pre-durable. While A has any active
//! write transaction, new readers on other connections receive a bounded
//! busy-style conflict and retry after A commits or rolls back. A reader that
//! starts first holds a short read permit while it materializes engine
//! results, so a writer cannot make the shared index transactional
//! underneath it. This closes the former window where an intra-txn
//! flush made the shared index point at rows another connection could
//! not yet see (surfacing as `chunk row ... Query returned no rows`).
//! The same writer connection may still read its own transaction.
//!
//! ── WHY :memory: / temp DATABASES ARE NOT SHARED ─────────────────────
//! sqlite3_db_filename() returns an empty string for ":memory:" and
//! temp databases. Two connections opening ":memory:" get two
//! completely UNRELATED databases — sharing an engine between them
//! would corrupt both (one engine, two disjoint sets of shadow
//! tables). So an empty filename falls back to a per-connection key
//! (the db handle address): each :memory: db keeps a private engine,
//! which is exactly the pre-R4 behavior and exactly correct there.

use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use rusqlite::{ffi, Connection, Error};

use crate::shadow_meta::InstanceId;

/// How long a second writer waits for the gate before failing with the
/// busy-style error. Mirrors the spirit of SQLite's busy_timeout.
const WRITE_GATE_TIMEOUT: Duration = Duration::from_secs(5);

fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

// ═══════════════════════════════════════════════════════════════════════
// Part 2 — thread-local connection routing
// ═══════════════════════════════════════════════════════════════════════

thread_local! {
    /// The host connection of the vtab callback currently executing on
    /// THIS thread (null when no callback is active). SQLite invokes
    /// every callback on the caller's thread while holding that
    /// connection's mutex, so "current thread" identifies "current
    /// connection" exactly for the duration of a callback.
    static CURRENT_DB: Cell<*mut ffi::sqlite3> = const { Cell::new(std::ptr::null_mut()) };
}

/// RAII binding of a host connection to the current thread. Every vtab
/// callback that can reach a shadow store (create/connect, insert incl.
/// commands, begin/commit/rollback, cursor filter, destroy) constructs
/// one of these first; Drop restores the previous value, so the guard
/// is panic-safe and nests correctly (a re-entrant callback — however
/// unlikely — restores its caller's binding on the way out).
pub(crate) struct DbGuard {
    prev: *mut ffi::sqlite3,
}

impl DbGuard {
    pub(crate) fn bind(db: *mut ffi::sqlite3) -> DbGuard {
        DbGuard {
            prev: CURRENT_DB.replace(db),
        }
    }
}

impl Drop for DbGuard {
    fn drop(&mut self) {
        CURRENT_DB.set(self.prev);
    }
}

/// Borrow the CALLING connection for one store operation.
/// `Connection::from_handle` wraps the raw pointer WITHOUT taking
/// ownership (the FTS5 re-entrancy trick) — dropping the returned
/// Connection does not close the user's database. Note the prepared-
/// statement cache is per-borrow; acceptable at chunk/block granularity
/// (this was already the stores' pattern before R4).
///
/// The unset-error doubles as the permanent rayon guard: engine code
/// running on a worker thread has no binding and gets this message
/// instead of the old silent deadlock on the host connection's mutex.
pub(crate) fn current_conn() -> Result<Connection, String> {
    let db = CURRENT_DB.get();
    if db.is_null() {
        return Err(
            "timeless-ext: no host connection bound to this thread — shadow-store \
             operations may only run inside a vtab callback on the calling \
             connection's thread (an engine worker thread must never touch the \
             store; see the rayon-deadlock lesson in PLAN.md Session 3)"
                .into(),
        );
    }
    unsafe { Connection::from_handle(db) }.map_err(|e| format!("from_handle failed: {e}"))
}

// ═══════════════════════════════════════════════════════════════════════
// Part 3 — the writer gate
// ═══════════════════════════════════════════════════════════════════════

/// Serializes write transactions on one shared engine. The holder is
/// identified by a connection id (the raw sqlite3* as usize — stable
/// for the lifetime of the connection, and the natural "who" here
/// because transactions are per-connection, not per-thread: sqld may
/// run consecutive statements of one connection on different threads).
///
/// Deliberately NOT a MutexGuard held across callbacks: guard lifetimes
/// cannot span separate FFI entries. Instead the lock protects a plain
/// holder token and a Condvar wakes waiters on release.
#[derive(Default)]
struct GateState {
    /// Some(conn_id) while that connection's write txn holds the gate.
    writer: Option<usize>,
    /// Engine reads currently materializing on connections that started
    /// before a writer. The permit never survives a vtab xFilter callback.
    readers: usize,
    /// Writers that found an active reader/writer and are waiting on the
    /// condition variable. While non-zero, later readers must retry instead
    /// of barging ahead and extending writer latency without bound.
    waiting_writers: usize,
}

pub(crate) struct WriterGate {
    state: Mutex<GateState>,
    released: Condvar,
    read_permit_count: AtomicU64,
    read_permit_hold_ns: AtomicU64,
    read_conflicts: AtomicU64,
    read_barge_rejections: AtomicU64,
    writer_wait_count: AtomicU64,
    writer_wait_ns: AtomicU64,
    writer_timeouts: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct WriterGateProfileSnapshot {
    pub(crate) read_permit_count: u64,
    pub(crate) read_permit_hold_ns: u64,
    pub(crate) read_conflicts: u64,
    pub(crate) read_barge_rejections: u64,
    pub(crate) waiting_writers: u64,
    pub(crate) writer_wait_count: u64,
    pub(crate) writer_wait_ns: u64,
    pub(crate) writer_timeouts: u64,
}

/// Short read-side permit. It prevents a writer from publishing
/// transaction-private chunk locations while this callback materializes its
/// result. Reads on the active writer connection need no permit because that
/// connection can see its own shadow rows and SQLite serializes its callbacks.
pub(crate) struct ReadPermit<'a> {
    gate: &'a WriterGate,
    active: bool,
    started: Option<Instant>,
}

impl Drop for ReadPermit<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.gate.lock();
        debug_assert!(state.readers > 0);
        state.readers -= 1;
        self.gate.released.notify_all();
        self.gate.read_permit_count.fetch_add(1, Ordering::Relaxed);
        if let Some(started) = self.started {
            self.gate
                .read_permit_hold_ns
                .fetch_add(elapsed_ns(started), Ordering::Relaxed);
        }
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

impl WriterGate {
    fn new() -> Self {
        WriterGate {
            state: Mutex::new(GateState::default()),
            released: Condvar::new(),
            read_permit_count: AtomicU64::new(0),
            read_permit_hold_ns: AtomicU64::new(0),
            read_conflicts: AtomicU64::new(0),
            read_barge_rejections: AtomicU64::new(0),
            writer_wait_count: AtomicU64::new(0),
            writer_wait_ns: AtomicU64::new(0),
            writer_timeouts: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, GateState> {
        // Poisoned = a panic while holding; GateState remains structurally valid,
        // so continue (matching the lock style used across the repo).
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Acquire for `conn_id`, waiting up to WRITE_GATE_TIMEOUT for the
    /// current holder to commit/rollback. Re-entrant for the same
    /// connection (autocommit fires xBegin per statement; an explicit
    /// transaction's later statements find their own connection already
    /// holding). Timeout → clear busy-style error, never a hang (see
    /// the module-level deadlock analysis).
    pub(crate) fn acquire(&self, conn_id: usize, table: &str) -> Result<(), String> {
        self.acquire_timeout(conn_id, table, WRITE_GATE_TIMEOUT)
    }

    /// Timeout-parameterized body (unit tests use a short timeout; the
    /// production path always passes WRITE_GATE_TIMEOUT).
    fn acquire_timeout(
        &self,
        conn_id: usize,
        table: &str,
        timeout: Duration,
    ) -> Result<(), String> {
        let started = Instant::now();
        let mut waited = false;
        let mut state = self.lock();
        if state.writer == Some(conn_id) {
            return Ok(()); // re-entrant: same connection, same txn
        }
        let deadline = Instant::now() + timeout;
        if state.writer.is_some() || state.readers > 0 {
            state.waiting_writers += 1;
            waited = true;
        }
        while state.writer.is_some() || state.readers > 0 {
            let now = Instant::now();
            if now >= deadline {
                debug_assert!(state.waiting_writers > 0);
                state.waiting_writers -= 1;
                self.writer_wait_count.fetch_add(1, Ordering::Relaxed);
                self.writer_wait_ns
                    .fetch_add(elapsed_ns(started), Ordering::Relaxed);
                self.writer_timeouts.fetch_add(1, Ordering::Relaxed);
                return Err(format!(
                    "table {table:?} is busy (timed out after {:?} waiting for {} \
                     writer and {} active reader(s) — retry, as for SQLITE_BUSY)",
                    timeout,
                    if state.writer.is_some() {
                        "another"
                    } else {
                        "no"
                    },
                    state.readers
                ));
            }
            let (g, _) = self
                .released
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|e| e.into_inner());
            state = g;
        }
        if waited {
            debug_assert!(state.waiting_writers > 0);
            state.waiting_writers -= 1;
        }
        state.writer = Some(conn_id);
        if waited {
            self.writer_wait_count.fetch_add(1, Ordering::Relaxed);
            self.writer_wait_ns
                .fetch_add(elapsed_ns(started), Ordering::Relaxed);
        }
        Ok(())
    }

    /// Acquire a short result-materialization permit. If another connection
    /// already owns the write transaction, fail immediately with a clear
    /// busy-style conflict: waiting inside xFilter could deadlock rollback-
    /// journal SQLite because the SELECT may already hold a shared file lock
    /// that the writer needs to commit. Callers may retry normally.
    pub(crate) fn acquire_read(
        &self,
        conn_id: usize,
        table: &str,
    ) -> Result<ReadPermit<'_>, String> {
        let mut state = self.lock();
        match state.writer {
            Some(writer) if writer == conn_id => Ok(ReadPermit {
                gate: self,
                active: false,
                started: None,
            }),
            Some(_) => {
                self.read_conflicts.fetch_add(1, Ordering::Relaxed);
                Err(format!(
                    "table {table:?} read is blocked by another connection's active write \
                     transaction — retry, as for SQLITE_BUSY"
                ))
            }
            None if state.waiting_writers == 0 => {
                state.readers += 1;
                Ok(ReadPermit {
                    gate: self,
                    active: true,
                    started: Some(Instant::now()),
                })
            }
            None => {
                self.read_conflicts.fetch_add(1, Ordering::Relaxed);
                self.read_barge_rejections.fetch_add(1, Ordering::Relaxed);
                Err(format!(
                    "table {table:?} read is blocked by a pending writer \
                     transaction — retry, as for SQLITE_BUSY"
                ))
            }
        }
    }

    pub(crate) fn profile(&self) -> WriterGateProfileSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let waiting_writers = self.lock().waiting_writers as u64;
        WriterGateProfileSnapshot {
            read_permit_count: load(&self.read_permit_count),
            read_permit_hold_ns: load(&self.read_permit_hold_ns),
            read_conflicts: load(&self.read_conflicts),
            read_barge_rejections: load(&self.read_barge_rejections),
            waiting_writers,
            writer_wait_count: load(&self.writer_wait_count),
            writer_wait_ns: load(&self.writer_wait_ns),
            writer_timeouts: load(&self.writer_timeouts),
        }
    }

    /// Release, but only if `conn_id` is actually the holder — commit
    /// and rollback paths can call this unconditionally.
    pub(crate) fn release(&self, conn_id: usize) {
        let mut state = self.lock();
        if state.writer == Some(conn_id) {
            state.writer = None;
            self.released.notify_all();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Part 1 — the process-global engine registry
// ═══════════════════════════════════════════════════════════════════════

/// One shared engine + its writer gate. `E` is one of the three
/// timeless-core engines (Engine / BlockEngine / SpanBlockEngine).
///
/// Send + Sync AUDIT (why `Arc<SharedEngine<E>>` may cross connections
/// and threads with no unsafe impls anywhere): after R4 the engines
/// hold NO connection state — the shadow stores contain only
/// pre-formatted SQL Strings and route every call through
/// [`current_conn`], so the raw sqlite3* never lives inside the engine
/// graph (the old `unsafe impl Send for HostHandle` is deleted, not
/// justified). Everything else in the engines is DashMap / RwLock /
/// Mutex / atomics over owned data, and the store trait objects are
/// `Box<dyn ...Store>` whose traits require Send + Sync. The compiler
/// derives Send + Sync for the whole structure — if anyone ever sneaks
/// a raw pointer back into a store, registration below stops compiling,
/// which is exactly the alarm we want.
pub(crate) struct SharedEngine<E> {
    pub(crate) engine: E,
    pub(crate) write_gate: WriterGate,
}

/// Registry key. File-backed databases share when path, schema alias, table,
/// and durable instance all match. The alias matters because store SQL is
/// qualified with that connection-local name. :memory:/temp databases also
/// include the connection pointer because their contents are connection-local.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum RegistryKey {
    File {
        path: String,
        database: Vec<u8>,
        table: String,
        instance: InstanceId,
    },
    Private {
        db: usize,
        database: Vec<u8>,
        table: String,
        instance: InstanceId,
    },
}

impl RegistryKey {
    fn same_slot(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::File {
                    path: left_path,
                    database: left_database,
                    table: left_table,
                    ..
                },
                Self::File {
                    path: right_path,
                    database: right_database,
                    table: right_table,
                    ..
                },
            ) => {
                left_path == right_path
                    && left_database == right_database
                    && left_table == right_table
            }
            (
                Self::Private {
                    db: left_db,
                    database: left_database,
                    table: left_table,
                    ..
                },
                Self::Private {
                    db: right_db,
                    database: right_database,
                    table: right_table,
                    ..
                },
            ) => {
                left_db == right_db && left_database == right_database && left_table == right_table
            }
            _ => false,
        }
    }
}

/// Sole-owner engines destroyed inside an explicit transaction need one
/// temporary strong reference: xDestroy frees the vtab object immediately,
/// before SQLite knows whether DROP will commit or roll back.
struct DropPin {
    connection: usize,
    _engine: Arc<dyn Any + Send + Sync>,
}

static DROP_PINS: LazyLock<Mutex<HashMap<RegistryKey, DropPin>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn drop_pins_lock() -> MutexGuard<'static, HashMap<RegistryKey, DropPin>> {
    DROP_PINS.lock().unwrap_or_else(|e| e.into_inner())
}

/// Pin a sole-owner engine across transactional xDestroy. Autocommit DROP
/// cannot roll back after the statement returns, and an engine already owned
/// by another vtab does not need an additional reference.
pub(crate) fn pin_for_drop<E>(
    db: *mut ffi::sqlite3,
    key: &RegistryKey,
    shared: &Arc<SharedEngine<E>>,
) where
    E: Send + Sync + 'static,
{
    let explicit_transaction = unsafe { ffi::sqlite3_get_autocommit(db) == 0 };
    if !explicit_transaction || Arc::strong_count(shared) > 1 {
        return;
    }
    let erased: Arc<dyn Any + Send + Sync> = shared.clone();
    drop_pins_lock().insert(
        key.clone(),
        DropPin {
            connection: db as usize,
            _engine: erased,
        },
    );
}

/// P1: per-CONNECTION engine pins. The process registry below holds
/// Weak references on purpose, and eponymous TVF vtabs die with their
/// statement — so before P1, a connection that only ever ran TVF
/// queries (a dashboard reader) held no strong reference between
/// statements and rebuilt the engine on EVERY query (416ms + ~360MB
/// alloc churn at 100k series; see FEATURE_PLAN "Cardinality
/// mitigation"). Each engine resolution now deposits one strong Arc
/// per (connection, key), released when the connection closes.
///
/// Lifecycle: the outer map entry is created by [`ConnPinScope`],
/// whose owner is the boxed state of the `timeless_pins()` scalar
/// function registered at extension init — SQLite drops function
/// state at sqlite3_close, so the scope's Drop clears the pins
/// exactly when the connection goes away, with no reliance on any
/// newer clientdata API. A connection without the anchor function
/// (pin_engine finds no entry) simply keeps pre-P1 behavior.
///
/// DROP TABLE semantics are unchanged: a dropped table's instance id
/// rotates, so a recreate builds a fresh engine under a fresh key;
/// the stale pin holds only memory and dies with the connection.
type ErasedEngine = Arc<dyn Any + Send + Sync>;
type ConnectionPins = HashMap<usize, HashMap<RegistryKey, ErasedEngine>>;

static CONN_PINS: LazyLock<Mutex<ConnectionPins>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn conn_pins_lock() -> MutexGuard<'static, ConnectionPins> {
    CONN_PINS.lock().unwrap_or_else(|e| e.into_inner())
}

/// Owner of one connection's pin set. Held as `timeless_pins()`
/// function state; its Drop is the connection-close hook.
pub(crate) struct ConnPinScope(usize);

impl ConnPinScope {
    pub(crate) fn new(db: *mut ffi::sqlite3) -> Self {
        conn_pins_lock().insert(db as usize, HashMap::new());
        ConnPinScope(db as usize)
    }

    /// Number of engines this connection currently pins — the
    /// `timeless_pins()` return value, and the deterministic
    /// observable the test suite asserts on.
    pub(crate) fn count(&self) -> i64 {
        conn_pins_lock()
            .get(&self.0)
            .map_or(0, |pins| pins.len() as i64)
    }
}

impl Drop for ConnPinScope {
    fn drop(&mut self) {
        conn_pins_lock().remove(&self.0);
        drop_pins_lock().retain(|_, pin| pin.connection != self.0);
    }
}

/// Deposit a strong reference for (connection, key). First resolution
/// wins; later calls for the same key are no-ops, so the pin set is
/// bounded by the number of distinct timeless tables the connection
/// touches.
pub(crate) fn pin_engine(
    db: *mut ffi::sqlite3,
    key: &RegistryKey,
    engine: Arc<dyn Any + Send + Sync>,
) {
    if let Some(pins) = conn_pins_lock().get_mut(&(db as usize)) {
        pins.entry(key.clone()).or_insert(engine);
    }
}

/// The process-global registry. Weak values: the registry must never
/// keep an engine alive by itself — when the last vtab instance
/// disconnects, the engine (and its buffered, pre-durable points) is
/// dropped, matching the documented "buffered = lost with the process"
/// contract. Dead Weaks are swept lazily on every registry access.
///
/// Values are type-erased (`dyn Any`) so one registry serves all three
/// engine types; the key's table name makes a type collision
/// impossible in practice (one table = one module), and get_or_create
/// still checks the downcast and errors loudly rather than trusting it.
static REGISTRY: LazyLock<Mutex<HashMap<RegistryKey, Weak<dyn Any + Send + Sync>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn registry_lock() -> MutexGuard<'static, HashMap<RegistryKey, Weak<dyn Any + Send + Sync>>> {
    REGISTRY.lock().unwrap_or_else(|e| e.into_inner())
}

/// Compute the registry key for (connection, database, table).
///
/// `database_name` is the second xCreate/xConnect argument ("main",
/// "temp", or an ATTACH alias) — passing it to sqlite3_db_filename
/// resolves the physical file. The alias remains in the key because
/// each store's qualified SQL is connection-local; two different aliases
/// therefore use separate process caches over the same durable file.
///
/// The path is canonicalized so `db.sqlite` and `./db.sqlite` and a
/// symlink all land on one key. sqlite3_db_filename already returns an
/// absolute path; canonicalize can still fail for a brand-new database
/// whose file has not been created yet (SQLite creates lazily), so we
/// fall back to canonical-parent + filename, then to the raw absolute
/// path — deterministic for any single path spelling, which is what
/// sqld's pool (one config, one spelling) needs.
pub(crate) fn registry_key(
    db: *mut ffi::sqlite3,
    database_name: &[u8],
    table: &str,
    instance: InstanceId,
) -> RegistryKey {
    let private = || RegistryKey::Private {
        db: db as usize,
        database: database_name.to_vec(),
        table: table.to_owned(),
        instance,
    };
    let Ok(dbname) = CString::new(database_name) else {
        return private(); // NUL in a database name: never shareable
    };
    let raw = unsafe {
        let ptr = ffi::sqlite3_db_filename(db, dbname.as_ptr());
        if ptr.is_null() {
            return private();
        }
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    };
    if raw.is_empty() {
        // ":memory:" or temp database — private to its connection.
        return private();
    }
    let path = std::path::Path::new(&raw);
    let canonical = std::fs::canonicalize(path)
        .ok()
        .or_else(|| {
            // File not on disk yet: canonicalize the parent, keep the
            // final component verbatim.
            let parent = std::fs::canonicalize(path.parent()?).ok()?;
            Some(parent.join(path.file_name()?))
        })
        .unwrap_or_else(|| path.to_path_buf());
    RegistryKey::File {
        path: canonical.to_string_lossy().into_owned(),
        database: database_name.to_vec(),
        table: table.to_owned(),
        instance,
    }
}

/// Look up or build the shared engine for `key`. The registry mutex is
/// held across `build` ON PURPOSE: two pooled connections racing
/// through xConnect must not construct two engines over the same
/// shadow tables (the second would re-run recovery against rows the
/// first is about to buffer around). `build` runs re-entrant SQL on the
/// calling connection only (recovery scans), never touches the
/// registry, and never takes another process-global lock — so holding
/// the mutex is deadlock-free, just briefly serializing engine
/// construction process-wide.
pub(crate) fn get_or_create<E, F>(
    key: &RegistryKey,
    build: F,
) -> rusqlite::Result<Arc<SharedEngine<E>>>
where
    E: Send + Sync + 'static,
    F: FnOnce() -> rusqlite::Result<E>,
{
    let mut map = registry_lock();
    {
        let mut pins = drop_pins_lock();
        // A new durable instance in the same physical slot proves the old
        // DROP committed. A matching instance is a rollback reconnect and is
        // released after get_or_create clones its Arc below.
        pins.retain(|pinned, _| !pinned.same_slot(key) || pinned == key);
    }
    // Lazy sweep: drop entries whose engines are gone (all their vtab
    // instances disconnected). Keeps the map from accumulating one dead
    // Weak per dropped table forever.
    map.retain(|_, w| w.strong_count() > 0);

    if let Some(weak) = map.get(key) {
        if let Some(alive) = weak.upgrade() {
            let shared = alive.downcast::<SharedEngine<E>>().map_err(|_| {
                module_err(format!(
                    "registry entry for {key:?} holds a different engine type \
                     (was this table name reused across timeless modules \
                     without DROP TABLE?)"
                ))
            })?;
            drop_pins_lock().remove(key);
            return Ok(shared);
        }
    }

    let shared = Arc::new(SharedEngine {
        engine: build()?,
        write_gate: WriterGate::new(),
    });
    let erased: Arc<dyn Any + Send + Sync> = shared.clone();
    map.insert(key.clone(), Arc::downgrade(&erased));
    Ok(shared)
}

/// Test cleanup. Production entries are removed only by lazy Weak sweeping;
/// eager removal in xDestroy breaks transactional DROP rollback.
#[cfg(test)]
pub(crate) fn remove(key: &RegistryKey) {
    let mut map = registry_lock();
    map.remove(key);
    map.retain(|_, w| w.strong_count() > 0);
}

// ═══════════════════════════════════════════════════════════════════════
// Unit tests. The WriterGate timeout path lives here (not in cli.sh)
// because stock SQLite cannot reach it through SQL: OP_Transaction
// takes the file write lock before OP_VBegin, so a second writer gets
// SQLITE_BUSY before its xBegin runs (see the module docs). These
// tests prove the gate itself is correct for the hosts where it IS
// reachable (concurrent-writer libsql branches).
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    const SHORT: Duration = Duration::from_millis(150);

    #[test]
    fn gate_reentrant_for_same_connection() {
        let g = WriterGate::new();
        g.acquire_timeout(1, "t", SHORT).unwrap();
        // Same connection re-acquires instantly (explicit-txn statements
        // 2..n, and the defensive re-check in insert()).
        g.acquire_timeout(1, "t", SHORT).unwrap();
        g.release(1);
    }

    #[test]
    fn gate_blocks_second_connection_until_release() {
        let g = Arc::new(WriterGate::new());
        g.acquire_timeout(1, "t", SHORT).unwrap();

        let g2 = Arc::clone(&g);
        let released = Arc::new(AtomicBool::new(false));
        let released2 = Arc::clone(&released);
        let waiter = thread::spawn(move || {
            // Generous timeout: must succeed BECAUSE of the release
            // below, not by racing it.
            g2.acquire_timeout(2, "t", Duration::from_secs(10))?;
            Ok::<bool, String>(released2.load(Ordering::SeqCst))
        });

        thread::sleep(Duration::from_millis(50));
        released.store(true, Ordering::SeqCst);
        g.release(1); // Condvar wakes the waiter
        let saw_release_first = waiter.join().unwrap().unwrap();
        assert!(saw_release_first, "waiter ran before the holder released");
        g.release(2);
    }

    #[test]
    fn gate_times_out_with_busy_style_error() {
        let g = WriterGate::new();
        g.acquire_timeout(1, "metrics", SHORT).unwrap();
        let t0 = Instant::now();
        let err = g.acquire_timeout(2, "metrics", SHORT).unwrap_err();
        assert!(t0.elapsed() >= SHORT, "returned before the bounded wait");
        assert!(
            err.contains("table \"metrics\" is busy"),
            "unexpected message: {err}"
        );
        // Holder unaffected by the failed acquire; release frees it.
        g.release(1);
        g.acquire_timeout(2, "metrics", SHORT).unwrap();
    }

    #[test]
    fn gate_release_by_non_holder_is_ignored() {
        let g = WriterGate::new();
        g.acquire_timeout(1, "t", SHORT).unwrap();
        g.release(2); // stray release (e.g. lone xCommit) must not unlock
        assert!(g.acquire_timeout(3, "t", SHORT).is_err());
        g.release(1);
    }

    #[test]
    fn read_permit_blocks_a_writer_until_materialization_finishes() {
        let gate = Arc::new(WriterGate::new());
        let permit = gate.acquire_read(1, "metrics").unwrap();

        let writer_gate = Arc::clone(&gate);
        let writer = thread::spawn(move || {
            writer_gate.acquire_timeout(2, "metrics", Duration::from_secs(10))?;
            writer_gate.release(2);
            Ok::<(), String>(())
        });

        thread::sleep(Duration::from_millis(50));
        assert!(
            !writer.is_finished(),
            "writer crossed an active read permit"
        );
        drop(permit);
        writer.join().unwrap().unwrap();
    }

    #[test]
    fn waiting_writer_prevents_later_readers_from_barging() {
        let gate = Arc::new(WriterGate::new());
        let first_reader = gate.acquire_read(1, "logs").unwrap();
        let (writer_acquired_tx, writer_acquired_rx) = std::sync::mpsc::channel();
        let (release_writer_tx, release_writer_rx) = std::sync::mpsc::channel();

        let writer_gate = Arc::clone(&gate);
        let writer = thread::spawn(move || {
            writer_gate.acquire_timeout(2, "logs", Duration::from_secs(10))?;
            writer_acquired_tx.send(()).unwrap();
            release_writer_rx.recv().unwrap();
            writer_gate.release(2);
            Ok::<(), String>(())
        });

        let deadline = Instant::now() + Duration::from_secs(2);
        while gate.profile().waiting_writers == 0 && Instant::now() < deadline {
            thread::yield_now();
        }
        assert_eq!(gate.profile().waiting_writers, 1);
        let err = match gate.acquire_read(3, "logs") {
            Ok(_) => panic!("later reader barged ahead of a waiting writer"),
            Err(err) => err,
        };
        assert!(err.contains("pending writer"), "{err}");
        assert_eq!(gate.profile().read_barge_rejections, 1);

        drop(first_reader);
        writer_acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer did not progress after the first reader released");
        release_writer_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();

        drop(gate.acquire_read(3, "logs").unwrap());
    }

    #[test]
    fn timed_out_writer_removes_its_reader_barrier() {
        let gate = WriterGate::new();
        let first_reader = gate.acquire_read(1, "logs").unwrap();
        let err = gate.acquire_timeout(2, "logs", SHORT).unwrap_err();
        assert!(err.contains("timed out"), "{err}");
        assert_eq!(gate.profile().waiting_writers, 0);

        // The timed-out writer must not leave future readers blocked.
        drop(gate.acquire_read(3, "logs").unwrap());
        drop(first_reader);
    }

    #[test]
    fn other_connection_read_gets_busy_during_write_transaction() {
        let gate = WriterGate::new();
        gate.acquire_timeout(1, "metrics", SHORT).unwrap();

        let err = match gate.acquire_read(2, "metrics") {
            Ok(_) => panic!("other connection acquired a read permit during a write"),
            Err(err) => err,
        };
        assert!(err.contains("active write transaction"), "{err}");

        // The writer connection can read its own transactional rows.
        let own = gate.acquire_read(1, "metrics").unwrap();
        assert!(!own.active);
        drop(own);
        gate.release(1);
    }

    #[test]
    fn registry_shares_same_key_and_isolates_different_keys() {
        let k1 = RegistryKey::File {
            path: "/tmp/r4-test.db".into(),
            database: b"main".to_vec(),
            table: "m".into(),
            instance: [1; 16],
        };
        let k2 = RegistryKey::File {
            path: "/tmp/r4-test.db".into(),
            database: b"main".to_vec(),
            table: "other".into(),
            instance: [1; 16],
        };
        let k3 = RegistryKey::File {
            path: "/tmp/r4-test.db".into(),
            database: b"main".to_vec(),
            table: "m".into(),
            instance: [2; 16],
        };
        let k4 = RegistryKey::File {
            path: "/tmp/r4-test.db".into(),
            database: b"aux".to_vec(),
            table: "m".into(),
            instance: [1; 16],
        };
        // A stand-in "engine": any Send+Sync 'static type works — the
        // registry is type-erased and generic over E.
        let a: Arc<SharedEngine<String>> = get_or_create(&k1, || Ok("engine".to_owned())).unwrap();
        let erased: Arc<dyn Any + Send + Sync> = a.clone();
        drop_pins_lock().insert(
            k1.clone(),
            DropPin {
                connection: 1,
                _engine: erased,
            },
        );
        let b: Arc<SharedEngine<String>> =
            get_or_create(&k1, || panic!("must reuse, not rebuild")).unwrap();
        assert!(Arc::ptr_eq(&a, &b), "same key must share one engine");
        assert!(
            !drop_pins_lock().contains_key(&k1),
            "rollback reconnect releases matching DROP pin"
        );
        let c: Arc<SharedEngine<String>> = get_or_create(&k2, || Ok("engine2".to_owned())).unwrap();
        assert!(!Arc::ptr_eq(&a, &c), "different table = different engine");
        let erased: Arc<dyn Any + Send + Sync> = a.clone();
        drop_pins_lock().insert(
            k1.clone(),
            DropPin {
                connection: 1,
                _engine: erased,
            },
        );
        let d: Arc<SharedEngine<String>> = get_or_create(&k3, || Ok("engine3".to_owned())).unwrap();
        assert!(
            !Arc::ptr_eq(&a, &d),
            "different instance = different engine"
        );
        assert!(
            !drop_pins_lock().contains_key(&k1),
            "recreate releases prior-instance DROP pin"
        );
        let e: Arc<SharedEngine<String>> = get_or_create(&k4, || Ok("engine4".to_owned())).unwrap();
        assert!(!Arc::ptr_eq(&a, &e), "different alias = different engine");
        remove(&k1);
        remove(&k2);
        remove(&k3);
        remove(&k4);
    }

    #[test]
    fn registry_weak_dies_with_last_arc_and_gets_swept() {
        let k = RegistryKey::Private {
            db: 0xdead,
            database: b"main".to_vec(),
            table: "m".into(),
            instance: [1; 16],
        };
        let a: Arc<SharedEngine<u64>> = get_or_create(&k, || Ok(7)).unwrap();
        drop(a);
        // Last vtab disconnected, so the next lookup must rebuild rather
        // than return stale state through the dead Weak.
        let rebuilt = std::cell::Cell::new(false);
        let b: Arc<SharedEngine<u64>> = get_or_create(&k, || {
            rebuilt.set(true);
            Ok(8)
        })
        .unwrap();
        assert!(rebuilt.get());
        assert_eq!(b.engine, 8);
        remove(&k);
    }

    #[test]
    fn conn_pin_keeps_engine_alive_until_scope_drops() {
        let k = RegistryKey::Private {
            db: 0xf1a0,
            database: b"main".to_vec(),
            table: "m".into(),
            instance: [1; 16],
        };
        let db = 0x7157_0001 as *mut ffi::sqlite3;
        let scope = ConnPinScope::new(db);
        assert_eq!(scope.count(), 0);

        // Resolve + pin, then drop the caller's Arc — the TVF-only
        // pattern that used to leave nothing alive between statements.
        let a: Arc<SharedEngine<u64>> = get_or_create(&k, || Ok(7)).unwrap();
        pin_engine(db, &k, a.clone());
        assert_eq!(scope.count(), 1);
        drop(a);

        // The pin must keep the Weak upgradable: no rebuild.
        let b: Arc<SharedEngine<u64>> =
            get_or_create(&k, || panic!("pinned engine must be reused, not rebuilt")).unwrap();
        assert_eq!(b.engine, 7);
        // Same key re-pin is a no-op (bounded pin set).
        pin_engine(db, &k, b.clone());
        assert_eq!(scope.count(), 1);
        drop(b);

        // Scope drop = connection close: pins released, engine dies,
        // next resolution rebuilds — the pre-P1 lifecycle resumes.
        drop(scope);
        let rebuilt = std::cell::Cell::new(false);
        let c: Arc<SharedEngine<u64>> = get_or_create(&k, || {
            rebuilt.set(true);
            Ok(8)
        })
        .unwrap();
        assert!(rebuilt.get(), "close must release the connection's pins");
        assert_eq!(c.engine, 8);
        remove(&k);
    }

    #[test]
    fn conn_scope_drop_reclaims_only_its_drop_pins() {
        let db = 0x7157_0012 as *mut ffi::sqlite3;
        let other_db = 0x7157_0013 as *mut ffi::sqlite3;
        let scope = ConnPinScope::new(db);
        let key = RegistryKey::Private {
            db: db as usize,
            database: b"main".to_vec(),
            table: "dropped".into(),
            instance: [1; 16],
        };
        let other_key = RegistryKey::Private {
            db: other_db as usize,
            database: b"main".to_vec(),
            table: "other".into(),
            instance: [1; 16],
        };
        let engine: Arc<dyn Any + Send + Sync> = Arc::new(7_u64);
        let other_engine: Arc<dyn Any + Send + Sync> = Arc::new(8_u64);
        let engine_weak = Arc::downgrade(&engine);
        let other_weak = Arc::downgrade(&other_engine);
        {
            let mut pins = drop_pins_lock();
            pins.insert(
                key,
                DropPin {
                    connection: db as usize,
                    _engine: engine,
                },
            );
            pins.insert(
                other_key.clone(),
                DropPin {
                    connection: other_db as usize,
                    _engine: other_engine,
                },
            );
        }

        drop(scope);

        assert!(engine_weak.upgrade().is_none());
        assert!(other_weak.upgrade().is_some());
        drop_pins_lock().remove(&other_key);
        assert!(other_weak.upgrade().is_none());
    }

    #[test]
    fn pin_without_scope_is_a_no_op() {
        let k = RegistryKey::Private {
            db: 0xf1a1,
            database: b"main".to_vec(),
            table: "m".into(),
            instance: [1; 16],
        };
        let db = 0x7157_0002 as *mut ffi::sqlite3;
        // No ConnPinScope registered for this connection (e.g. an
        // embedded host that skipped register_telemetry's anchor):
        // pinning silently does nothing and pre-P1 behavior holds.
        let a: Arc<SharedEngine<u64>> = get_or_create(&k, || Ok(1)).unwrap();
        pin_engine(db, &k, a.clone());
        drop(a);
        let rebuilt = std::cell::Cell::new(false);
        let _b: Arc<SharedEngine<u64>> = get_or_create(&k, || {
            rebuilt.set(true);
            Ok(2)
        })
        .unwrap();
        assert!(rebuilt.get(), "no scope = no pin = rebuild");
        remove(&k);
    }

    #[test]
    fn registry_type_mismatch_is_a_loud_error() {
        let k = RegistryKey::Private {
            db: 0xbeef,
            database: b"main".to_vec(),
            table: "m".into(),
            instance: [1; 16],
        };
        let _keep: Arc<SharedEngine<u64>> = get_or_create(&k, || Ok(1)).unwrap();
        let err = match get_or_create::<String, _>(&k, || Ok("x".into())) {
            Err(e) => e,
            Ok(_) => panic!("type mismatch must not be silently accepted"),
        };
        assert!(err.to_string().contains("different engine type"));
        remove(&k);
    }

    #[test]
    fn db_guard_nests_and_restores() {
        // Fake pointers: the guard only stores/restores them, it never
        // dereferences (only current_conn does, and we don't call it).
        let p1 = 0x1000 as *mut ffi::sqlite3;
        let p2 = 0x2000 as *mut ffi::sqlite3;
        assert!(CURRENT_DB.get().is_null());
        {
            let _a = DbGuard::bind(p1);
            assert_eq!(CURRENT_DB.get(), p1);
            {
                let _b = DbGuard::bind(p2);
                assert_eq!(CURRENT_DB.get(), p2);
            }
            assert_eq!(CURRENT_DB.get(), p1); // inner guard restored
        }
        assert!(CURRENT_DB.get().is_null());
    }

    #[test]
    fn current_conn_unbound_names_the_rayon_guard() {
        // On a thread with no binding, the store path must fail with
        // the teaching message, not deadlock (the permanent guard for
        // the Session 3 rayon lesson).
        thread::spawn(|| {
            let err = current_conn().unwrap_err();
            assert!(err.contains("no host connection bound"));
        })
        .join()
        .unwrap();
    }
}
