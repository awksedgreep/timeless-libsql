//! Metrics Engine transaction journal tests (PLAN.md R5).
//!
//! Scope note (same as the blocks/spans journal unit tests): FsStore is
//! NOT transactional — chunk files written during a "transaction" stay
//! on disk after txn_rollback. These tests therefore verify the
//! ENGINE-MEMORY half of rollback: buffer truncation, restoration of
//! pre-txn points drained by an intra-txn flush, index add/remove
//! bookkeeping, and flush-queue rebuild. The store-side half (chunk
//! ROWS vanishing/reappearing with the host transaction) only exists
//! over the SQLite shadow store and is asserted end-to-end by
//! tests/cli.sh and the oracle property test.

use timeless_core::{Engine, MemStore};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("timeless_txn_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn engine(dir: &std::path::Path) -> Engine {
    // flush_threshold 1000, min_flush_size 0, level 8, 64MiB budget,
    // no deferred compression — the roundtrip test parameters.
    Engine::in_memory(dir, 1000, 0, 8, 64 * 1024 * 1024, false).unwrap()
}

fn count(e: &Engine, sid: i64) -> usize {
    e.query_range_by_id(sid, i64::MIN + 1, i64::MAX - 1)
        .unwrap()
        .len()
}

#[test]
fn txn_rollback_discards_buffered_points() {
    let dir = temp_dir("buffered");
    let e = engine(&dir);
    let store = MemStore::named(&dir);
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();
    e.write_point(sid, 100, 1.0);
    e.write_point(sid, 200, 2.0);

    store.begin();

    e.txn_begin();
    e.write_point(sid, 300, 3.0);
    // A series created inside the txn: its buffer has no mark and must
    // truncate to zero on rollback.
    let sid2 = e.resolve_cached("mem", &Default::default()).unwrap();
    e.write_point(sid2, 300, 9.0);
    e.txn_rollback();
    store.rollback();

    assert_eq!(count(&e, sid), 2);
    assert_eq!(count(&e, sid2), 0);
    // The series made during the transaction goes with it: its catalog
    // row is the host's to undo.
    assert!(!e.metric_names().unwrap().contains(&"mem".to_string()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn txn_rollback_restores_points_drained_by_intra_txn_flush() {
    // THE R5 nightmare case: points buffered by COMMITTED statements
    // are drained into chunks by a flush INSIDE a later transaction;
    // the chunk rows roll back with the host txn, so the points must
    // return to the buffer or committed data would be silently lost.
    let dir = temp_dir("drain");
    let e = engine(&dir);
    let store = MemStore::named(&dir);
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();
    e.write_point(sid, 100, 1.0);
    e.write_point(sid, 200, 2.0);

    store.begin();

    e.txn_begin();
    e.write_point(sid, 300, 3.0);
    e.flush_all().unwrap(); // drains all 3 points into a chunk
    e.txn_rollback();
    store.rollback();

    // The chunk's index entry is gone; the two pre-txn points are back
    // in the buffer (the txn point is not); a fresh committed flush
    // persists exactly them.
    assert_eq!(e.info().chunk_count, 0);
    let pts = e
        .query_range_by_id(sid, i64::MIN + 1, i64::MAX - 1)
        .unwrap();
    assert_eq!(pts, vec![(100, 1.0), (200, 2.0)]);
    e.flush_all().unwrap();
    assert_eq!(e.info().chunk_count, 1);
    assert_eq!(count(&e, sid), 2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn txn_commit_keeps_everything() {
    let dir = temp_dir("commit");
    let e = engine(&dir);
    let store = MemStore::named(&dir);
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();

    store.begin();

    e.txn_begin();
    e.write_point(sid, 100, 1.0);
    e.flush_all().unwrap();
    e.write_point(sid, 200, 2.0);
    e.txn_commit();
    store.commit();

    assert_eq!(e.info().chunk_count, 1);
    assert_eq!(count(&e, sid), 2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn txn_rollback_rebuilds_flush_queue() {
    // flush_threshold is 1000: pushing 1000+ points inside the txn
    // queues the partition for flush. Rollback truncates the buffer
    // below the threshold and must reset the queued flag — otherwise
    // the partition could never auto-queue again (write_point only
    // queues when !queued_for_flush).
    let dir = temp_dir("queue");
    let e = engine(&dir);
    let store = MemStore::named(&dir);
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();
    e.write_point(sid, 1, 0.5); // one committed pre-txn point

    store.begin();

    e.txn_begin();
    for i in 0..1200 {
        e.write_point(sid, 100 + i, i as f64);
    }
    e.txn_rollback();
    store.rollback();
    assert_eq!(count(&e, sid), 1);

    // flush_pending after rollback must not see a stale queue entry
    // pointing at a (now tiny) buffer... and after re-crossing the
    // threshold with committed writes, auto-queue + flush must work.
    e.flush_pending().unwrap();
    for i in 0..1200 {
        e.write_point(sid, 5000 + i, i as f64);
    }
    e.flush_pending().unwrap();
    assert!(e.info().chunk_count >= 1);
    assert_eq!(count(&e, sid), 1201);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn savepoint_rollback_is_repeatable_and_unwinds_nested_frames() {
    let dir = temp_dir("savepoint_nested");
    let e = engine(&dir);
    let store = MemStore::named(&dir);
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();

    store.begin();

    e.txn_begin();
    e.write_point(sid, 100, 1.0);
    store.begin();
    e.txn_savepoint(0);
    e.write_point(sid, 200, 2.0);
    store.begin();
    e.txn_savepoint(1);
    e.write_point(sid, 300, 3.0);
    e.txn_rollback_to(0);
    store.rollback();
    store.begin();
    assert_eq!(count(&e, sid), 1);

    // ROLLBACK TO leaves the target savepoint active.
    e.write_point(sid, 400, 4.0);
    e.txn_rollback_to(0);
    store.rollback();
    store.begin();
    e.txn_release(0);
    store.commit();
    e.txn_commit();
    store.commit();
    assert_eq!(count(&e, sid), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn released_flush_frame_still_rolls_back_with_outer_transaction() {
    let dir = temp_dir("savepoint_release");
    let e = engine(&dir);
    let store = MemStore::named(&dir);
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();
    e.write_point(sid, 100, 1.0);

    store.begin();

    e.txn_begin();
    e.write_point(sid, 200, 2.0);
    store.begin();
    e.txn_savepoint(0);
    e.flush_all().unwrap();
    e.txn_release(0);
    store.commit();
    e.txn_rollback();
    store.rollback();

    assert_eq!(e.info().chunk_count, 0);
    assert_eq!(e.info().buffered_points, 1);
    let _ = std::fs::remove_dir_all(&dir);
}

// Buffer marks are recorded on a partition's first mutation inside a
// frame, not for every partition at begin (#120). These cases pin the
// places where a lazily recorded mark could be missing or stale.

#[test]
fn mark_from_previous_transaction_is_not_reused() {
    let dir = temp_dir("lazy_stale_epoch");
    let e = engine(&dir);
    let store = MemStore::named(&dir);
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();

    store.begin();

    e.txn_begin();
    e.write_point(sid, 100, 1.0);
    e.txn_commit();
    store.commit();

    // The buffer still carries the committed transaction's mark stamp;
    // this transaction must record its own mark at length 1.
    store.begin();
    e.txn_begin();
    e.write_point(sid, 200, 2.0);
    e.write_point(sid, 300, 3.0);
    e.txn_rollback();
    store.rollback();

    assert_eq!(count(&e, sid), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn partition_first_touched_in_released_savepoint_rolls_back_with_outer() {
    let dir = temp_dir("lazy_release");
    let e = engine(&dir);
    let store = MemStore::named(&dir);
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();
    let other = e.resolve_cached("mem", &Default::default()).unwrap();
    e.write_point(sid, 100, 1.0);
    e.write_point(other, 100, 5.0);

    store.begin();

    e.txn_begin();
    e.write_point(other, 200, 6.0);
    // `sid` is untouched by the outer frame until inside the savepoint:
    // its first mark must land in the outer frame too.
    store.begin();
    e.txn_savepoint(0);
    e.write_point(sid, 200, 2.0);
    e.txn_release(0);
    store.commit();
    e.write_point(sid, 300, 3.0);
    e.txn_rollback();
    store.rollback();

    assert_eq!(count(&e, sid), 1);
    assert_eq!(count(&e, other), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rollback_to_savepoint_remarks_partitions_written_again() {
    let dir = temp_dir("lazy_rollback_to");
    let e = engine(&dir);
    let store = MemStore::named(&dir);
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();
    e.write_point(sid, 100, 1.0);

    store.begin();

    e.txn_begin();
    e.write_point(sid, 200, 2.0);
    store.begin();
    e.txn_savepoint(0);
    e.write_point(sid, 300, 3.0);
    e.txn_rollback_to(0);
    store.rollback();
    store.begin();
    assert_eq!(count(&e, sid), 2);

    // The savepoint's frame was reset: writing again must re-mark, or a
    // second rollback to it would keep this point.
    e.write_point(sid, 400, 4.0);
    e.txn_rollback_to(0);
    store.rollback();
    store.begin();
    assert_eq!(count(&e, sid), 2);

    e.txn_rollback();

    store.rollback();
    assert_eq!(count(&e, sid), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn drain_of_partition_untouched_in_frame_saves_all_its_points() {
    let dir = temp_dir("lazy_drain_untouched");
    let e = engine(&dir);
    let store = MemStore::named(&dir);
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();
    let other = e.resolve_cached("mem", &Default::default()).unwrap();
    e.write_point(sid, 100, 1.0);
    e.write_point(sid, 200, 2.0);
    e.write_point(other, 100, 5.0);

    store.begin();

    e.txn_begin();
    e.write_point(other, 200, 6.0);
    store.begin();
    e.txn_savepoint(0);
    // `sid` has no mark in either frame when the flush drains it: every
    // point in it predates both, and all of them must come back.
    e.flush_all().unwrap();
    e.txn_rollback_to(0);
    store.rollback();
    store.begin();

    assert_eq!(e.info().chunk_count, 0);
    assert_eq!(count(&e, sid), 2);
    assert_eq!(count(&e, other), 2);

    e.txn_rollback();

    store.rollback();
    assert_eq!(count(&e, sid), 2);
    assert_eq!(count(&e, other), 1);
    let _ = std::fs::remove_dir_all(&dir);
}
