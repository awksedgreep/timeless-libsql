//! The metrics engine inside a host transaction.
//!
//! Points a transaction writes are buffered until it stores them: when it
//! commits, when it opens a savepoint, and before it reads or maintains the
//! table. Buffers therefore never hold anything but the open transaction's
//! own points, and the engine keeps no record of how to put them back: a
//! rollback drops what is buffered, and the rows the transaction did store
//! are the host's to undo. `MemStore::begin`/`rollback`/`commit` stand in
//! for the host database here, as SQLite does for the extension.

use std::collections::HashMap;

use timeless_core::{Engine, MemStore};

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("timeless_txn_test_{name}_{}", std::process::id()))
}

fn engine(dir: &std::path::Path) -> (Engine, MemStore) {
    (
        Engine::in_memory(dir, 1000, 0, 8, 64 * 1024 * 1024, false).unwrap(),
        MemStore::named(dir),
    )
}

fn count(e: &Engine, sid: i64) -> usize {
    e.query_range_by_id(sid, i64::MIN + 1, i64::MAX - 1)
        .unwrap()
        .len()
}

/// What the virtual table does as a transaction commits.
fn commit(e: &Engine, store: &MemStore) {
    e.flush_all().unwrap();
    e.txn_commit();
    store.commit();
}

fn begin(e: &Engine, store: &MemStore) {
    store.begin();
    e.txn_begin();
}

fn rollback(e: &Engine, store: &MemStore) {
    e.txn_rollback();
    store.rollback();
}

#[test]
fn a_read_sees_stored_points_and_not_buffered_ones() {
    let (e, store) = engine(&temp_dir("rows_only"));
    let sid = e.resolve_cached("cpu", &HashMap::new()).unwrap();
    begin(&e, &store);
    e.write_point(sid, 100, 1.0);
    e.write_point(sid, 200, 2.0);
    assert!(e.has_buffered_points());
    assert_eq!(count(&e, sid), 0, "buffered, not yet stored");
    e.flush_all().unwrap();
    assert!(!e.has_buffered_points());
    assert_eq!(count(&e, sid), 2);
    commit(&e, &store);
    assert_eq!(count(&e, sid), 2);
}

#[test]
fn a_rollback_takes_buffered_points_and_stored_ones() {
    let (e, store) = engine(&temp_dir("rollback"));
    let sid = e.resolve_cached("cpu", &HashMap::new()).unwrap();
    begin(&e, &store);
    e.write_point(sid, 100, 1.0);
    commit(&e, &store);

    begin(&e, &store);
    // One point reaches a row inside the transaction, one stays buffered,
    // and a series is made that nothing outside the transaction knows.
    e.write_point(sid, 200, 2.0);
    e.flush_all().unwrap();
    e.write_point(sid, 300, 3.0);
    let other = e.resolve_cached("mem", &HashMap::new()).unwrap();
    e.write_point(other, 300, 9.0);
    assert_eq!(count(&e, sid), 2);
    rollback(&e, &store);

    assert!(!e.has_buffered_points());
    assert_eq!(count(&e, sid), 1, "only what was committed");
    assert_eq!(e.info().chunk_count, 1);
    assert!(!e.metric_names().unwrap().contains(&"mem".to_string()));

    // The engine is as it was: the same series, written again, is stored.
    begin(&e, &store);
    e.write_point(sid, 200, 2.0);
    commit(&e, &store);
    assert_eq!(count(&e, sid), 2);
}

#[test]
fn a_commit_keeps_everything_and_leaves_nothing_buffered() {
    let (e, store) = engine(&temp_dir("commit"));
    let sid = e.resolve_cached("cpu", &HashMap::new()).unwrap();
    for round in 0..3_i64 {
        begin(&e, &store);
        e.write_point(sid, 100 + round, round as f64);
        commit(&e, &store);
        assert!(!e.has_buffered_points());
    }
    assert_eq!(count(&e, sid), 3);
    // A later rollback cannot reach what an earlier transaction committed.
    begin(&e, &store);
    e.write_point(sid, 500, 5.0);
    rollback(&e, &store);
    assert_eq!(count(&e, sid), 3);
}

#[test]
fn rolling_back_to_a_savepoint_undoes_only_what_came_after_it() {
    let (e, store) = engine(&temp_dir("savepoint"));
    let sid = e.resolve_cached("cpu", &HashMap::new()).unwrap();
    begin(&e, &store);
    e.write_point(sid, 100, 1.0);

    // A savepoint opens: what has been written is stored first, so the
    // rows are outside it.
    let savepoint = |id: i32| {
        e.flush_all().unwrap();
        store.begin();
        e.txn_savepoint(id);
    };
    let rollback_to = |id: i32| {
        e.txn_rollback_to(id);
        store.rollback();
        store.begin();
    };
    savepoint(0);
    e.write_point(sid, 200, 2.0);
    e.flush_all().unwrap();
    e.write_point(sid, 300, 3.0);
    assert_eq!(count(&e, sid), 2);

    rollback_to(0);
    assert!(!e.has_buffered_points());
    assert_eq!(count(&e, sid), 1, "the point before the savepoint stands");

    // It can be rolled back to again, and written past.
    e.write_point(sid, 400, 4.0);
    rollback_to(0);
    assert_eq!(count(&e, sid), 1);
    e.write_point(sid, 500, 5.0);

    // A nested savepoint, released, still goes with the outer rollback.
    savepoint(1);
    e.write_point(sid, 600, 6.0);
    e.flush_all().unwrap();
    e.txn_release(1);
    store.commit();
    assert_eq!(count(&e, sid), 3);
    rollback_to(0);
    assert_eq!(count(&e, sid), 1);

    e.txn_release(0);
    store.commit();
    commit(&e, &store);
    assert_eq!(count(&e, sid), 1);
}

#[test]
fn a_series_that_reaches_the_threshold_is_stored_inside_the_transaction() {
    let (e, store) = engine(&temp_dir("threshold"));
    let sid = e.resolve_cached("cpu", &HashMap::new()).unwrap();
    begin(&e, &store);
    // The threshold is 1,000 points: the buffer is queued when it gets
    // there, and stored by the flush that follows the statement.
    for ts in 0..1_500_i64 {
        e.write_point(sid, ts, ts as f64);
    }
    assert_eq!(e.flush_pending().unwrap(), 1);
    assert_eq!(count(&e, sid), 1_500);
    rollback(&e, &store);
    assert_eq!(count(&e, sid), 0);
    assert_eq!(e.info().chunk_count, 0);
}
