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

use timeless_core::Engine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "timeless_txn_test_{name}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn engine(dir: &std::path::Path) -> Engine {
    // flush_threshold 1000, min_flush_size 0, level 8, 64MiB budget,
    // no deferred compression — the roundtrip test parameters.
    Engine::new(dir.to_path_buf(), 1000, 0, 8, 64 * 1024 * 1024, false)
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
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();
    e.write_point(sid, 100, 1.0);
    e.write_point(sid, 200, 2.0);

    e.txn_begin();
    e.write_point(sid, 300, 3.0);
    // A series created inside the txn: its buffer has no mark and must
    // truncate to zero on rollback.
    let sid2 = e.resolve_cached("mem", &Default::default()).unwrap();
    e.write_point(sid2, 300, 9.0);
    e.txn_rollback();

    assert_eq!(count(&e, sid), 2);
    assert_eq!(count(&e, sid2), 0);
    // The series NAME registered during the txn stays registered — the
    // documented accepted leftover (a harmless empty series).
    assert!(e.series_read().list_metrics().contains(&"mem".to_string()));
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
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();
    e.write_point(sid, 100, 1.0);
    e.write_point(sid, 200, 2.0);

    e.txn_begin();
    e.write_point(sid, 300, 3.0);
    e.flush_all().unwrap(); // drains all 3 points into a chunk
    e.txn_rollback();

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
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();

    e.txn_begin();
    e.write_point(sid, 100, 1.0);
    e.flush_all().unwrap();
    e.write_point(sid, 200, 2.0);
    e.txn_commit();

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
    let sid = e.resolve_cached("cpu", &Default::default()).unwrap();
    e.write_point(sid, 1, 0.5); // one committed pre-txn point

    e.txn_begin();
    for i in 0..1200 {
        e.write_point(sid, 100 + i, i as f64);
    }
    e.txn_rollback();
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
