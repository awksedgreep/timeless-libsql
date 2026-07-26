//! Savepoint callbacks missing from rusqlite's `TransactionVTab` adapter.
//!
//! rusqlite 0.40.1 populates xBegin/xSync/xCommit/xRollback, but leaves the
//! version-2 xSavepoint/xRelease/xRollbackTo slots empty. This module keeps
//! rusqlite's module descriptor and callback implementations intact and only
//! fills those three trailing slots.

use std::ffi::c_int;

use rusqlite::ffi;
use rusqlite::vtab::{Module, TransactionVTab};

pub(crate) trait SavepointVTab {
    fn savepoint(&mut self, id: c_int);
    fn release(&mut self, id: c_int);
    fn rollback_to(&mut self, id: c_int);
}

/// Extend rusqlite's writable transaction module with SQLite's savepoint
/// callbacks.
///
/// # Safety
///
/// `Module` is `repr(transparent)` over `ffi::sqlite3_module`. The cast only
/// mutates that wrapped value, and each virtual table is `repr(C)` with its
/// `sqlite3_vtab` base as the first field, as required by rusqlite's `VTab`
/// safety contract.
pub(crate) const fn update_module_with_savepoints<'vtab, T>() -> Module<'vtab, T>
where
    T: TransactionVTab<'vtab> + SavepointVTab,
{
    let mut module = Module::update_module_with_tx();
    let base = unsafe { &mut *((&raw mut module).cast::<ffi::sqlite3_module>()) };
    base.iVersion = 2;
    base.xSavepoint = Some(rust_savepoint::<T>);
    base.xRelease = Some(rust_release::<T>);
    base.xRollbackTo = Some(rust_rollback_to::<T>);
    module
}

unsafe extern "C" fn rust_savepoint<T>(vtab: *mut ffi::sqlite3_vtab, id: c_int) -> c_int
where
    T: SavepointVTab,
{
    unsafe { (*vtab.cast::<T>()).savepoint(id) };
    ffi::SQLITE_OK
}

unsafe extern "C" fn rust_release<T>(vtab: *mut ffi::sqlite3_vtab, id: c_int) -> c_int
where
    T: SavepointVTab,
{
    unsafe { (*vtab.cast::<T>()).release(id) };
    ffi::SQLITE_OK
}

unsafe extern "C" fn rust_rollback_to<T>(vtab: *mut ffi::sqlite3_vtab, id: c_int) -> c_int
where
    T: SavepointVTab,
{
    unsafe { (*vtab.cast::<T>()).rollback_to(id) };
    ffi::SQLITE_OK
}
