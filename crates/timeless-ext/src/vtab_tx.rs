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

/// Run one savepoint hook, converting a Rust panic into `SQLITE_ERROR`.
///
/// These hooks run inside `unsafe extern "C"` callbacks: unwinding
/// through them into SQLite would be undefined behavior (and in
/// practice aborts the host). Engine savepoint hooks are infallible by
/// construction, so `SQLITE_ERROR` here means "a bug panicked", never a
/// routine failure — but a clean error beats an abort.
///
/// `AssertUnwindSafe` is required because `&mut T` is not
/// `UnwindSafe` for all `T`. That is sound here: on panic the table is
/// left in whatever state the hook reached, exactly as if the hook had
/// returned early, and SQLite rolls the savepoint back.
fn invoke_savepoint_op<T: SavepointVTab>(tab: &mut T, id: c_int, op: fn(&mut T, c_int)) -> c_int {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| op(tab, id))) {
        Ok(()) => ffi::SQLITE_OK,
        Err(_) => ffi::SQLITE_ERROR,
    }
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
    invoke_savepoint_op(unsafe { &mut *vtab.cast::<T>() }, id, T::savepoint)
}

unsafe extern "C" fn rust_release<T>(vtab: *mut ffi::sqlite3_vtab, id: c_int) -> c_int
where
    T: SavepointVTab,
{
    invoke_savepoint_op(unsafe { &mut *vtab.cast::<T>() }, id, T::release)
}

unsafe extern "C" fn rust_rollback_to<T>(vtab: *mut ffi::sqlite3_vtab, id: c_int) -> c_int
where
    T: SavepointVTab,
{
    invoke_savepoint_op(unsafe { &mut *vtab.cast::<T>() }, id, T::rollback_to)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Calm(bool);

    impl SavepointVTab for Calm {
        fn savepoint(&mut self, _id: c_int) {
            self.0 = true;
        }
        fn release(&mut self, _id: c_int) {}
        fn rollback_to(&mut self, _id: c_int) {}
    }

    struct Alarmed;

    impl SavepointVTab for Alarmed {
        fn savepoint(&mut self, _id: c_int) {
            panic!("simulated engine bug");
        }
        fn release(&mut self, _id: c_int) {
            panic!("simulated engine bug");
        }
        fn rollback_to(&mut self, _id: c_int) {
            panic!("simulated engine bug");
        }
    }

    #[test]
    fn hooks_pass_through_ok() {
        let mut tab = Calm(false);
        assert_eq!(
            invoke_savepoint_op(&mut tab, 1, Calm::savepoint),
            ffi::SQLITE_OK
        );
        assert!(tab.0);
        assert_eq!(
            invoke_savepoint_op(&mut tab, 1, Calm::release),
            ffi::SQLITE_OK
        );
        assert_eq!(
            invoke_savepoint_op(&mut tab, 1, Calm::rollback_to),
            ffi::SQLITE_OK
        );
    }

    #[test]
    fn hook_panic_becomes_sqlite_error_not_abort() {
        // Regression test (issue #44): without catch_unwind this panic
        // would unwind through an `extern "C"` frame — UB that aborts
        // the host. It must surface as SQLITE_ERROR instead. (These run
        // in the default build: no SQLite API is touched.)
        let mut tab = Alarmed;
        assert_eq!(
            invoke_savepoint_op(&mut tab, 1, Alarmed::savepoint),
            ffi::SQLITE_ERROR
        );
        assert_eq!(
            invoke_savepoint_op(&mut tab, 1, Alarmed::release),
            ffi::SQLITE_ERROR
        );
        assert_eq!(
            invoke_savepoint_op(&mut tab, 1, Alarmed::rollback_to),
            ffi::SQLITE_ERROR
        );
    }
}
