//! timeless-ext: loadable SQLite extension exposing the timeless-core
//! time-series engine as virtual tables.
//!
//! Four modules are registered on every connection that loads the .so:
//!   - "timeless_spike"   (spike.rs)        - the Session 1 proof-of-concept,
//!     kept as a compiling reference for the vtab + re-entrancy patterns.
//!   - "timeless_metrics" (metrics_vtab.rs) - the real thing: a writable
//!     vtab whose chunks persist into shadow tables on the HOST database
//!     through shadow_store::ShadowTableStore (timeless_core::ChunkStore).
//!   - "timeless_logs"    (logs_vtab.rs)    - Phase 2: compressed log
//!     blocks + inverted term index in `<name>_blocks`/`<name>_terms`
//!     via shadow_block_store::ShadowBlockStore (BlockStore).
//!   - "timeless_traces"  (traces_vtab.rs)  - Phase 2, Session 6: span
//!     blocks + term index + packed-trace-id index in `<name>_blocks`/
//!     `<name>_terms`/`<name>_trace_blocks` via
//!     shadow_span_store::ShadowSpanStore (SpanBlockStore). Three
//!     signals, one .so (PLAN.md R8).
//!
//! MULTI-CONNECTION (PLAN.md R4, shared.rs): all connections in one
//! process that open the same (db file, table) share ONE engine via a
//! process-global registry; store SQL is routed to the calling
//! connection through a thread-local, and write transactions are
//! serialized per table by a 5s-bounded writer gate (busy-style error
//! on contention). Short read permits prevent cross-connection readers
//! from following transaction-private shadow locations; a read that arrives
//! during another connection's write transaction gets a retryable busy-style
//! error. See shared.rs for the full design + semantics notes.
//!
//! Usage:
//!   .load target/release/libtimeless_ext
//!   CREATE VIRTUAL TABLE metrics USING timeless_metrics;
//!   INSERT INTO metrics(name, ts, value, labels)
//!     VALUES ('cpu', 1700000000, 0.42, '{"host":"a"}');
//!   INSERT INTO metrics(metrics) VALUES ('flush');   -- FTS5-style command
//!   SELECT * FROM metrics WHERE name = 'cpu' AND ts >= 1700000000;

// The dbhealth shell build (default-features = false) compiles this
// crate without the entry points, leaving the telemetry registers
// unused by design — silence dead-code for that config wholesale.
#![cfg_attr(not(feature = "entrypoints"), allow(dead_code))]

mod batch;
mod flatjson;
mod health_vtab;
mod logs_vtab;
mod metrics_vtab;
mod otel_json;
pub mod query_frame;
mod query_tvf;
mod shadow_block_store;
mod shadow_meta;
mod shadow_span_store;
mod shadow_store;
mod shared;
mod spike;
mod sql_ident;
mod sql_value;
mod table_args;
mod traces_vtab;
mod vtab_tx;

#[cfg(feature = "entrypoints")]
use std::ffi::{c_char, c_int};

#[cfg(feature = "entrypoints")]
use rusqlite::ffi;
use rusqlite::{Connection, Result};

// ---------------------------------------------------------------------------
// Extension entry points
// ---------------------------------------------------------------------------
// SQLite dlopen()s the .so and calls an init function. Depending on version
// and filename it looks for "sqlite3_extension_init" or a name derived from
// the filename (libtimeless_ext.so -> sqlite3_timelessext_init, possibly
// keeping the underscore). We export all three; they share one body.
//
// `#[no_mangle]` keeps the symbol name exactly as written (Rust normally
// mangles names). `extern "C"` uses the C calling convention. `unsafe`
// because we're trusting raw pointers handed to us by SQLite.

/// Register ONLY the dbhealth module family on a connection — the
/// public API the separate `dbhealth-ext` cdylib builds its extension
/// entry points from (its .so registers health and nothing else).
pub fn register_dbhealth(db: &Connection) -> Result<()> {
    health_vtab::register(db)
}

/// Register the telemetry virtual tables and query functions on an existing
/// SQLite connection.
///
/// This is the embedding API used by hosts which link `timeless-ext` as a Rust
/// library and provide their own loadable-extension entry points. It deliberately
/// excludes the development-only `timeless_spike` module and the separately
/// packaged dbhealth modules.
pub fn register_telemetry(db: &Connection) -> Result<()> {
    metrics_vtab::register(db)?;
    logs_vtab::register(db)?;
    traces_vtab::register(db)?;
    query_tvf::register(db)
}

#[cfg(feature = "entrypoints")]
unsafe extern "C" fn init_common(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, extension_init)
}

#[cfg(feature = "entrypoints")]
#[no_mangle]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}

#[cfg(feature = "entrypoints")]
#[no_mangle]
pub unsafe extern "C" fn sqlite3_timelessext_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}

#[cfg(feature = "entrypoints")]
#[no_mangle]
pub unsafe extern "C" fn sqlite3_timeless_ext_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}

/// Runs once per connection when the extension loads: register both modules.
#[cfg(feature = "entrypoints")]
fn extension_init(db: Connection) -> Result<bool> {
    spike::register(&db)?;
    register_telemetry(&db)?;
    // false = loaded per-connection (fine here; sqld preloads into every
    // connection anyway).
    Ok(false)
}
