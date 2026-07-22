//! timeless-ext: loadable SQLite extension exposing the timeless-core
//! time-series engine as virtual tables.
//!
//! Two modules are registered on every connection that loads the .so:
//!   - "timeless_spike"   (spike.rs)        - the Session 1 proof-of-concept,
//!     kept as a compiling reference for the vtab + re-entrancy patterns.
//!   - "timeless_metrics" (metrics_vtab.rs) - the real thing: a writable
//!     vtab whose chunks persist into shadow tables on the HOST database
//!     through shadow_store::ShadowTableStore (timeless_core::ChunkStore).
//!
//! Usage:
//!   .load target/release/libtimeless_ext
//!   CREATE VIRTUAL TABLE metrics USING timeless_metrics;
//!   INSERT INTO metrics(name, ts, value, labels)
//!     VALUES ('cpu', 1700000000, 0.42, '{"host":"a"}');
//!   INSERT INTO metrics(metrics) VALUES ('flush');   -- FTS5-style command
//!   SELECT * FROM metrics WHERE name = 'cpu' AND ts >= 1700000000;

mod metrics_vtab;
mod shadow_store;
mod spike;

use std::ffi::{c_char, c_int};

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

unsafe extern "C" fn init_common(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, extension_init)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_timelessext_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_timeless_ext_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}

/// Runs once per connection when the extension loads: register both modules.
fn extension_init(db: Connection) -> Result<bool> {
    spike::register(&db)?;
    metrics_vtab::register(&db)?;
    // false = loaded per-connection (fine here; sqld preloads into every
    // connection anyway).
    Ok(false)
}
