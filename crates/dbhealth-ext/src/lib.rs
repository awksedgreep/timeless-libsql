//! dbhealth — the standalone health-monitoring extension for SQLite and
//! libSQL (docs/DBHEALTH.md).
//!
//!   .load ./libdbhealth_ext
//!   CREATE VIRTUAL TABLE dbhealth USING dbhealth;   -- collection begins
//!   SELECT * FROM dbhealth_report;
//!
//! All the machinery lives in the timeless-ext library crate (compiled
//! in statically — the compressed metrics engine and the health vtab);
//! this crate is the loadable-extension shell: entry points that
//! register the dbhealth module family and nothing else.

use std::ffi::{c_char, c_int};

use rusqlite::ffi;
use rusqlite::{Connection, Result};

fn dbhealth_init(db: Connection) -> Result<bool> {
    timeless_ext::register_dbhealth(&db)?;
    // false = loaded per-connection.
    Ok(false)
}

unsafe extern "C" fn init_common(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, dbhealth_init)
}

#[no_mangle]
/// SQLite's conventional loadable-extension entry point.
///
/// # Safety
///
/// The arguments must satisfy SQLite's loadable-extension ABI.
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}

#[no_mangle]
/// Filename-derived SQLite entry-point alias.
///
/// # Safety
///
/// The arguments must satisfy SQLite's loadable-extension ABI.
pub unsafe extern "C" fn sqlite3_dbhealthext_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}

#[no_mangle]
/// Underscore-preserving filename-derived SQLite entry-point alias.
///
/// # Safety
///
/// The arguments must satisfy SQLite's loadable-extension ABI.
pub unsafe extern "C" fn sqlite3_dbhealth_ext_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    init_common(db, pz_err_msg, p_api)
}
