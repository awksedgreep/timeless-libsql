//! Regression tests for the chunk-index min_ts shadowing bug
//! (the chunk-index shadowing fix (2026-07-22, see git history)): the index used to be keyed
//! (series, min_ts), so two chunks of one series sharing a min_ts
//! silently shadowed each other. The key is now widened with a
//! per-engine monotonic chunk_seq (ported from the donor fix in
//! timeless_metrics), making collisions impossible.

use timeless_core::Engine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "timeless_dup_min_ts_{name}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn engine(dir: &std::path::Path) -> Engine {
    // min_flush_size 1 → every flush_all writes individual chunks,
    // matching the donor regression test's parameters.
    Engine::new(dir.to_path_buf(), 100, 1, 8, usize::MAX, false)
}

/// Donor regression test `duplicate_min_ts_chunks_do_not_shadow`,
/// adapted: chunk count is asserted through the public info() instead
/// of the private index.
#[test]
fn duplicate_min_ts_chunks_do_not_shadow() {
    // Two flush cycles producing chunks with the same (series, min_ts)
    // — e.g. backfill re-ingesting an overlapping export. Both chunks
    // must stay queryable.
    let dir = temp_dir("shadow");
    let e = engine(&dir);

    e.write_point(1, 100, 1.0);
    e.flush_all().unwrap(); // chunk A: min_ts=100
    e.write_point(1, 100, 2.0);
    e.write_point(1, 200, 3.0);
    e.flush_all().unwrap(); // chunk B: min_ts=100

    assert_eq!(e.info().chunk_count, 2);
    let points = e.query_range_by_id(1, 0, 1000).unwrap();
    assert_eq!(
        points.iter().map(|&(ts, _)| ts).collect::<Vec<_>>(),
        vec![100, 100, 200]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Restart-recovery half: the scan path re-assigns fresh chunk_seq
/// values, so duplicate-min_ts chunks on disk must both survive a
/// rebuild_index after reopen (pre-fix, whichever chunk the scan
/// visited last won and the other's points vanished).
#[test]
fn duplicate_min_ts_chunks_survive_restart_recovery() {
    let dir = temp_dir("restart");
    {
        let e = engine(&dir);
        e.write_point(1, 100, 1.0);
        e.flush_all().unwrap(); // chunk A: min_ts=100
        e.write_point(1, 100, 2.0);
        e.write_point(1, 200, 3.0);
        e.flush_all().unwrap(); // chunk B: min_ts=100
        e.shutdown().unwrap();
    }

    let restarted = engine(&dir);
    assert_eq!(restarted.info().chunk_count, 2);
    let points = restarted.query_range_by_id(1, 0, 1000).unwrap();
    assert_eq!(points.len(), 3);
    assert_eq!(
        points.iter().map(|&(ts, _)| ts).collect::<Vec<_>>(),
        vec![100, 100, 200]
    );
    let _ = std::fs::remove_dir_all(&dir);
}
