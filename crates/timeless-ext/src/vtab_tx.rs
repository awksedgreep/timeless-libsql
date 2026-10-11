//! Savepoint callbacks missing from rusqlite's `TransactionVTab` adapter.
//!
//! rusqlite 0.40.1 populates xBegin/xSync/xCommit/xRollback, but leaves the
//! version-2 xSavepoint/xRelease/xRollbackTo slots empty. This module keeps
//! rusqlite's module descriptor and callback implementations intact and only
//! fills those three trailing slots.

use std::ffi::c_int;

use rusqlite::ffi;
use rusqlite::vtab::{Module, TransactionVTab};

/// The three savepoint callbacks. Each may fail: a table that writes rows
/// when a savepoint opens (so that SQLite's own savepoint undoes them) can
/// fail to write them, and SQLite must be told.
pub(crate) trait SavepointVTab {
    fn savepoint(&mut self, id: c_int) -> Result<(), String>;
    fn release(&mut self, id: c_int) -> Result<(), String>;
    fn rollback_to(&mut self, id: c_int) -> Result<(), String>;
}

/// Run one savepoint hook: its error, or a Rust panic, becomes
/// `SQLITE_ERROR`, with the error's text left for SQLite to report.
///
/// These hooks run inside `unsafe extern "C"` callbacks: unwinding
/// through them into SQLite would be undefined behavior (and in
/// practice aborts the host), so a panic is caught and reported as an
/// error like any other.
///
/// `AssertUnwindSafe` is required because `&mut T` is not
/// `UnwindSafe` for all `T`. That is sound here: on panic the table is
/// left in whatever state the hook reached, exactly as if the hook had
/// returned early, and SQLite rolls the savepoint back.
fn invoke_savepoint_op<T: SavepointVTab>(
    tab: &mut T,
    id: c_int,
    op: fn(&mut T, c_int) -> Result<(), String>,
) -> Result<(), String> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| op(tab, id))) {
        Ok(result) => result,
        Err(_) => Err("a savepoint callback panicked".to_string()),
    }
}

/// Hand a hook's outcome to SQLite: `SQLITE_OK`, or `SQLITE_ERROR` with the
/// message in the table's `zErrMsg`, which SQLite reports and frees.
///
/// # Safety
///
/// `vtab` is the live `sqlite3_vtab` SQLite passed to the callback.
unsafe fn report(vtab: *mut ffi::sqlite3_vtab, outcome: Result<(), String>) -> c_int {
    let Err(message) = outcome else {
        return ffi::SQLITE_OK;
    };
    // SQLite frees `zErrMsg` with its own allocator, so the text is copied
    // into memory from it. An interior NUL would end the text early.
    let text: Vec<u8> = message.bytes().filter(|byte| *byte != 0).collect();
    unsafe {
        if !(*vtab).zErrMsg.is_null() {
            ffi::sqlite3_free((*vtab).zErrMsg.cast());
            (*vtab).zErrMsg = std::ptr::null_mut();
        }
        let copy = ffi::sqlite3_malloc64(text.len() as u64 + 1).cast::<u8>();
        if !copy.is_null() {
            std::ptr::copy_nonoverlapping(text.as_ptr(), copy, text.len());
            *copy.add(text.len()) = 0;
            (*vtab).zErrMsg = copy.cast();
        }
    }
    ffi::SQLITE_ERROR
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
    let outcome = invoke_savepoint_op(unsafe { &mut *vtab.cast::<T>() }, id, T::savepoint);
    unsafe { report(vtab, outcome) }
}

unsafe extern "C" fn rust_release<T>(vtab: *mut ffi::sqlite3_vtab, id: c_int) -> c_int
where
    T: SavepointVTab,
{
    let outcome = invoke_savepoint_op(unsafe { &mut *vtab.cast::<T>() }, id, T::release);
    unsafe { report(vtab, outcome) }
}

unsafe extern "C" fn rust_rollback_to<T>(vtab: *mut ffi::sqlite3_vtab, id: c_int) -> c_int
where
    T: SavepointVTab,
{
    let outcome = invoke_savepoint_op(unsafe { &mut *vtab.cast::<T>() }, id, T::rollback_to);
    unsafe { report(vtab, outcome) }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Calm(bool);

    impl SavepointVTab for Calm {
        fn savepoint(&mut self, _id: c_int) -> Result<(), String> {
            self.0 = true;
            Ok(())
        }
        fn release(&mut self, _id: c_int) -> Result<(), String> {
            Ok(())
        }
        fn rollback_to(&mut self, _id: c_int) -> Result<(), String> {
            Err("could not write the rows".to_string())
        }
    }

    struct Alarmed;

    impl SavepointVTab for Alarmed {
        fn savepoint(&mut self, _id: c_int) -> Result<(), String> {
            panic!("simulated engine bug");
        }
        fn release(&mut self, _id: c_int) -> Result<(), String> {
            panic!("simulated engine bug");
        }
        fn rollback_to(&mut self, _id: c_int) -> Result<(), String> {
            panic!("simulated engine bug");
        }
    }

    #[test]
    fn hooks_pass_their_outcome_through() {
        let mut tab = Calm(false);
        assert_eq!(invoke_savepoint_op(&mut tab, 1, Calm::savepoint), Ok(()));
        assert!(tab.0);
        assert_eq!(invoke_savepoint_op(&mut tab, 1, Calm::release), Ok(()));
        assert_eq!(
            invoke_savepoint_op(&mut tab, 1, Calm::rollback_to),
            Err("could not write the rows".to_string())
        );
    }

    #[test]
    fn hook_panic_becomes_an_error_not_an_abort() {
        // Regression test (issue #44): without catch_unwind this panic
        // would unwind through an `extern "C"` frame — UB that aborts
        // the host. It must surface as an error instead. (These run
        // in the default build: no SQLite API is touched.)
        let mut tab = Alarmed;
        for op in [Alarmed::savepoint, Alarmed::release, Alarmed::rollback_to] {
            assert!(invoke_savepoint_op(&mut tab, 1, op).is_err());
        }
    }
}
