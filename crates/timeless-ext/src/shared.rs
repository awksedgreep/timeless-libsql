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
//!    path, table name). Extensions are one shared library per
//!    process, so a `static` here is exactly one registry per process,
//!    shared by every connection that loaded the .so. xCreate/xConnect
//!    upgrade the registry's Weak or build the engine once; every vtab
//!    instance holds an Arc to the same [`SharedEngine`]; xDisconnect
//!    drops the Arc (last one out frees the engine); xDestroy removes
//!    the registry entry (the shadow tables are being dropped).
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
//! 3. WRITER SERIALIZATION ([`WriterGate`]). Each engine keeps ONE
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
//! ── SHARED-BUFFER SEMANTICS (documented, accepted) ──────────────────
//! One engine per table means one in-memory buffer per table: points
//! connection A has inserted but not yet committed are visible to
//! connection B's queries IMMEDIATELY (a dirty read of buffered
//! telemetry). If A rolls back, B stops seeing them. This is the
//! deliberate trade: buffered points were already documented as
//! pre-durable (lost on crash), so exposing them pre-commit keeps the
//! same mental model — FLUSHED data remains fully transactional. One
//! sharp edge inherited from that trade: if A performs an intra-
//! transaction 'flush' and lingers before COMMIT, the shared index
//! briefly points at chunk rows other connections cannot see yet, so a
//! concurrent query on B can fail with a "row read" error until A
//! commits — the same bounded window SQLITE_BUSY users already live
//! with. (In autocommit — the normal telemetry path — the window is a
//! single statement.)
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
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use rusqlite::{ffi, Connection, Error};

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
pub(crate) struct WriterGate {
    /// Some(conn_id) while that connection's write txn holds the gate.
    holder: Mutex<Option<usize>>,
    released: Condvar,
}

impl WriterGate {
    fn new() -> Self {
        WriterGate {
            holder: Mutex::new(None),
            released: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<usize>> {
        // Poisoned = a panic while holding; the Option is always valid,
        // so continue (matching the lock style used across the repo).
        self.holder.lock().unwrap_or_else(|e| e.into_inner())
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
        let mut holder = self.lock();
        if *holder == Some(conn_id) {
            return Ok(()); // re-entrant: same connection, same txn
        }
        let deadline = Instant::now() + timeout;
        while holder.is_some() {
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "table {table:?} is locked by another connection's write \
                     transaction (timed out after {:?} waiting for it to commit \
                     or roll back — retry, as for SQLITE_BUSY)",
                    timeout
                ));
            }
            let (g, _) = self
                .released
                .wait_timeout(holder, deadline - now)
                .unwrap_or_else(|e| e.into_inner());
            holder = g;
        }
        *holder = Some(conn_id);
        Ok(())
    }

    /// Release, but only if `conn_id` is actually the holder — commit
    /// and rollback paths can call this unconditionally.
    pub(crate) fn release(&self, conn_id: usize) {
        let mut holder = self.lock();
        if *holder == Some(conn_id) {
            *holder = None;
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

/// Registry key. File-backed databases share by (canonical path,
/// table); :memory:/temp databases get a per-connection Private key —
/// see the module docs for why sharing is meaningless (and harmful)
/// there.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum RegistryKey {
    File { path: String, table: String },
    Private { db: usize, table: String },
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
/// means an ATTACHed file resolves to ITS path, so the same file
/// ATTACHed by two connections under different aliases still shares
/// one engine (the alias is connection-local, the file is not).
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
) -> RegistryKey {
    let private = || RegistryKey::Private {
        db: db as usize,
        table: table.to_owned(),
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
        table: table.to_owned(),
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
    // Lazy sweep: drop entries whose engines are gone (all their vtab
    // instances disconnected). Keeps the map from accumulating one dead
    // Weak per dropped table forever.
    map.retain(|_, w| w.strong_count() > 0);

    if let Some(weak) = map.get(key) {
        if let Some(alive) = weak.upgrade() {
            return alive.downcast::<SharedEngine<E>>().map_err(|_| {
                module_err(format!(
                    "registry entry for {key:?} holds a different engine type \
                     (was this table name reused across timeless modules \
                     without DROP TABLE?)"
                ))
            });
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

/// xDestroy: the shadow tables are being dropped, so the key must not
/// resolve to the (now table-less) engine ever again. Connections still
/// holding the old Arc will fail their next store call with "no such
/// table" and reconnect to a fresh engine after the table is recreated
/// (SQLite's schema-cookie bump forces their re-prepare → xConnect).
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
            err.contains("locked by another connection"),
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
    fn registry_shares_same_key_and_isolates_different_keys() {
        let k1 = RegistryKey::File {
            path: "/tmp/r4-test.db".into(),
            table: "m".into(),
        };
        let k2 = RegistryKey::File {
            path: "/tmp/r4-test.db".into(),
            table: "other".into(),
        };
        // A stand-in "engine": any Send+Sync 'static type works — the
        // registry is type-erased and generic over E.
        let a: Arc<SharedEngine<String>> =
            get_or_create(&k1, || Ok("engine".to_owned())).unwrap();
        let b: Arc<SharedEngine<String>> =
            get_or_create(&k1, || panic!("must reuse, not rebuild")).unwrap();
        assert!(Arc::ptr_eq(&a, &b), "same key must share one engine");
        let c: Arc<SharedEngine<String>> =
            get_or_create(&k2, || Ok("engine2".to_owned())).unwrap();
        assert!(!Arc::ptr_eq(&a, &c), "different table = different engine");
        remove(&k1);
        remove(&k2);
    }

    #[test]
    fn registry_weak_dies_with_last_arc_and_gets_swept() {
        let k = RegistryKey::Private {
            db: 0xdead,
            table: "m".into(),
        };
        let a: Arc<SharedEngine<u64>> = get_or_create(&k, || Ok(7)).unwrap();
        drop(a); // last vtab instance disconnects → engine dropped
        // Next get_or_create must BUILD (the Weak is dead), proving no
        // stale engine survives its last holder.
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
    fn registry_type_mismatch_is_a_loud_error() {
        let k = RegistryKey::Private {
            db: 0xbeef,
            table: "m".into(),
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
