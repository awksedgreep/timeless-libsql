//! F2: automatic retention (FEATURE_PLAN.md). Data-time cutoffs, chunk/
//! block-granular pruning at maintenance boundaries, recovery of the
//! high-water mark from the index, and backfill inertness.

use std::collections::HashMap;

use timeless_core::{BlockEngine, BlockEngineConfig, Engine, LogEntry, LogQuery, MemBlockStore};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("timeless_f2_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn count_points(engine: &Engine, sid: i64) -> usize {
    engine
        .query_range_by_id(sid, i64::MIN, i64::MAX)
        .unwrap()
        .len()
}

/// Retention prunes old chunks at flush boundaries, keyed to DATA time,
/// and the applied window survives reopen (high-water mark is derived
/// from the recovered index, not persisted state).
#[test]
fn retention_prunes_and_recovers() {
    let dir = temp_dir("prune");
    let labels: HashMap<String, String> = HashMap::new();
    let sid;
    {
        let engine = Engine::new(dir.clone(), 100_000, 0, 3, 64 << 20, false).unwrap();
        engine.set_retention(Some(100));
        sid = engine.resolve_cached("cpu", &labels).unwrap();

        // Epoch 1 at ts 1000..1010, flushed into its own chunk.
        engine.write_point(sid, 1000, 1.0);
        engine.write_point(sid, 1010, 2.0);
        engine.flush_all().unwrap();
        assert_eq!(count_points(&engine, sid), 2, "epoch 1 alone survives");

        // Epoch 2 at ts 1200+: cutoff 1210-100=1110 > epoch-1 max 1010,
        // so the flush that lands epoch 2 must prune epoch 1.
        engine.write_point(sid, 1200, 3.0);
        engine.write_point(sid, 1210, 4.0);
        engine.flush_all().unwrap();
        let rows = engine.query_range_by_id(sid, i64::MIN, i64::MAX).unwrap();
        assert_eq!(
            rows.iter().map(|&(ts, _)| ts).collect::<Vec<_>>(),
            vec![1200, 1210],
            "epoch 1 pruned by retention at the epoch-2 flush"
        );
        engine.shutdown().unwrap();
    }

    // Reopen: retention keeps working from the RECOVERED index.
    {
        let engine = Engine::new(dir, 100_000, 0, 3, 64 << 20, false).unwrap();
        engine.set_retention(Some(100));
        let sid2 = engine.resolve_cached("cpu", &labels).unwrap();
        assert_eq!(sid2, sid, "series identity recovered");
        assert_eq!(count_points(&engine, sid), 2, "epoch 2 survived reopen");

        engine.write_point(sid, 1400, 5.0);
        engine.flush_all().unwrap();
        let rows = engine.query_range_by_id(sid, i64::MIN, i64::MAX).unwrap();
        assert_eq!(
            rows.iter().map(|&(ts, _)| ts).collect::<Vec<_>>(),
            vec![1400],
            "post-reopen flush prunes epoch 2 (cutoff 1300 from recovered high water)"
        );
        engine.shutdown().unwrap();
    }
}

/// Backfill must not move the cutoff backward, and the contract is
/// CHUNK-granular + guarded: a backfill-only chunk below the cutoff
/// survives the flush that lands it (the advance guard skips an
/// unmoved cutoff) and is pruned at the next maintenance where the
/// cutoff has advanced. In-window data is never touched.
#[test]
fn backfill_does_not_move_cutoff() {
    let dir = temp_dir("backfill");
    let labels: HashMap<String, String> = HashMap::new();
    let engine = Engine::new(dir, 100_000, 0, 3, 64 << 20, false).unwrap();
    engine.set_retention(Some(100));
    let sid = engine.resolve_cached("cpu", &labels).unwrap();

    engine.write_point(sid, 2000, 1.0);
    engine.flush_all().unwrap(); // floor = 1900

    // Backfill-only chunk far below the cutoff. The cutoff has not
    // advanced, so the guard skips — the chunk survives THIS flush...
    engine.write_point(sid, 500, 9.0);
    engine.write_point(sid, 510, 9.5);
    engine.flush_all().unwrap();
    let ts: Vec<i64> = engine
        .query_range_by_id(sid, i64::MIN, i64::MAX)
        .unwrap()
        .iter()
        .map(|&(t, _)| t)
        .collect();
    assert_eq!(ts, vec![500, 510, 2000], "guard skips an unmoved cutoff");

    // ...and dies at the next flush that advances the cutoff past the
    // guard slice (2010-100=1910 >= 1900 + 100/16).
    engine.write_point(sid, 2010, 2.0);
    engine.flush_all().unwrap();
    let ts: Vec<i64> = engine
        .query_range_by_id(sid, i64::MIN, i64::MAX)
        .unwrap()
        .iter()
        .map(|&(t, _)| t)
        .collect();
    assert_eq!(
        ts,
        vec![2000, 2010],
        "backfill chunk below the advanced cutoff pruned; in-window data intact"
    );
    engine.shutdown().unwrap();
}

/// Disabled retention (the default) prunes nothing, ever.
#[test]
fn disabled_retention_is_inert() {
    let dir = temp_dir("inert");
    let labels: HashMap<String, String> = HashMap::new();
    let engine = Engine::new(dir, 100_000, 0, 3, 64 << 20, false).unwrap();
    let sid = engine.resolve_cached("cpu", &labels).unwrap();
    engine.write_point(sid, 0, 1.0);
    engine.flush_all().unwrap();
    engine.write_point(sid, i64::MAX - 1, 2.0);
    engine.flush_all().unwrap();
    assert_eq!(count_points(&engine, sid), 2);
    engine.shutdown().unwrap();
}

/// BlockEngine (logs): retention fires from flush() — including the
/// auto-flush inside push() — with block-granular pruning.
#[test]
fn block_engine_retention() {
    let engine = BlockEngine::new(
        Box::new(MemBlockStore::new()),
        BlockEngineConfig {
            auto_optimize_interval_flushes: 0,
            auto_optimize_budget_entries: 32_768,
            flush_threshold: 100_000,
            ..Default::default()
        },
    )
    .unwrap();
    engine.set_retention(Some(1_000));

    let entry = |ts: i64| LogEntry {
        ts,
        level: 1,
        severity: None,
        message: format!("m{ts}"),
        metadata: vec![],
        metadata_json: None,
    };
    engine.push(entry(10_000)).unwrap();
    engine.push(entry(10_050)).unwrap();
    engine.flush().unwrap();
    engine.push(entry(12_000)).unwrap();
    engine.flush().unwrap();

    let q = LogQuery {
        ts_min: i64::MIN,
        ts_max: i64::MAX,
        level: None,
        severity: None,
        metadata_eq: vec![],
        message_contains: None,
        message_like_prune: None,
    };
    let rows = engine.query(&q).unwrap();
    assert_eq!(
        rows.iter().map(|e| e.ts).collect::<Vec<_>>(),
        vec![12_000],
        "old block pruned at the second flush (cutoff 11_000)"
    );
}

/// Repair: `prune_after` drops whole chunks whose coverage begins after the
/// cutoff. This is the bounded operator path for samples stored under a
/// mistaken timestamp unit (milliseconds in a seconds store), which land
/// tens of thousands of years out, never match a query, and are newer than
/// every retention cutoff so retention can never remove them.
#[test]
fn prune_after_removes_future_chunks() {
    let dir = temp_dir("prune_after");
    let labels: HashMap<String, String> = HashMap::new();
    let sid;
    {
        let engine = Engine::new(dir.clone(), 100_000, 0, 3, 64 << 20, false).unwrap();
        sid = engine.resolve_cached("cpu", &labels).unwrap();

        // A normal chunk at sane epoch seconds, flushed on its own.
        engine.write_point(sid, 1000, 1.0);
        engine.write_point(sid, 1010, 2.0);
        engine.flush_all().unwrap();

        // A chunk from a collector that emitted milliseconds.
        engine.write_point(sid, 1_787_288_405_969, 3.0);
        engine.flush_all().unwrap();
        assert_eq!(count_points(&engine, sid), 3, "both chunks present");

        // The cutoff is far above sane seconds but far below the ms chunk.
        let (deleted, more, errors) = engine.prune_after(2_000_000_000);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(deleted, 1, "the future chunk is the only victim");
        assert!(!more, "nothing left to sweep");
        assert_eq!(
            engine
                .query_range_by_id(sid, i64::MIN, i64::MAX)
                .unwrap()
                .iter()
                .map(|&(t, _)| t)
                .collect::<Vec<_>>(),
            vec![1000, 1010],
            "sane chunk retained; future chunk gone"
        );
        engine.shutdown().unwrap();
    }
}

/// A series retention has left nothing of is removed with its last
/// chunk: from the registry, so that what the store holds in memory is
/// what it holds on disk. A series with data still buffered, and one
/// created in the transaction that prunes, are left alone.
#[test]
fn retention_removes_series_it_has_left_nothing_of() {
    let dir = temp_dir("sweep");
    let labels: HashMap<String, String> = HashMap::new();
    let engine = Engine::new(dir.clone(), 100_000, 0, 3, 64 << 20, false).unwrap();
    engine.set_retention(Some(100));
    let gone = engine.resolve_cached("gone", &labels).unwrap();
    let kept = engine.resolve_cached("kept", &labels).unwrap();

    // Both have a chunk at 1000; only `kept` goes on.
    engine.write_point(gone, 1000, 1.0);
    engine.write_point(kept, 1000, 1.0);
    engine.flush_all().unwrap();
    assert_eq!(engine.info().series_count, 2);

    engine.write_point(kept, 1200, 2.0);
    engine.flush_all().unwrap();
    let info = engine.info();
    assert_eq!(
        info.series_count, 1,
        "the series whose only chunk was pruned is gone"
    );
    assert_eq!(info.retention_series_removed, 1);
    assert!(engine.series_read().list_metrics() == vec!["kept".to_string()]);
    assert_eq!(count_points(&engine, gone), 0);
    assert_eq!(count_points(&engine, kept), 1);

    // A series with points buffered and not yet flushed has data.
    let buffered = engine.resolve_cached("buffered", &labels).unwrap();
    engine.write_point(buffered, 1250, 3.0);
    engine.write_point(kept, 1400, 4.0);
    // Compaction applies retention too, and `buffered` is still in its
    // buffer while the cutoff passes `kept`'s first chunk.
    engine.compact_partitions(i64::MAX).unwrap();
    assert!(engine
        .series_read()
        .list_metrics()
        .contains(&"buffered".to_string()));

    // Once it has flushed and expired, it goes like any other.
    engine.flush_all().unwrap();
    engine.write_point(kept, 1600, 5.0);
    engine.flush_all().unwrap();
    assert!(!engine
        .series_read()
        .list_metrics()
        .contains(&"buffered".to_string()));
    assert_eq!(engine.info().retention_series_removed, 2);

    // And the name can be used again, as a new series.
    let again = engine.resolve_cached("gone", &labels).unwrap();
    engine.write_point(again, 1600, 6.0);
    engine.flush_all().unwrap();
    assert_eq!(count_points(&engine, again), 1);
    engine.shutdown().unwrap();

    // The registry that was saved knows nothing of them either.
    let engine = Engine::new(dir.clone(), 100_000, 0, 3, 64 << 20, false).unwrap();
    let mut names = engine.series_read().list_metrics();
    names.sort();
    assert_eq!(names, vec!["gone".to_string(), "kept".to_string()]);
    engine.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The removal is journaled: rolled back, the series is back, with its
/// id, and can be written to as before.
#[test]
fn rollback_restores_a_series_retention_removed() {
    let dir = temp_dir("sweep_rollback");
    let labels: HashMap<String, String> = HashMap::new();
    let engine = Engine::new(dir.clone(), 100_000, 0, 3, 64 << 20, false).unwrap();
    engine.set_retention(Some(100));
    let gone = engine.resolve_cached("gone", &labels).unwrap();
    let kept = engine.resolve_cached("kept", &labels).unwrap();
    engine.write_point(gone, 1000, 1.0);
    engine.write_point(kept, 1000, 1.0);
    engine.flush_all().unwrap();

    engine.txn_begin();
    engine.write_point(kept, 1200, 2.0);
    engine.flush_all().unwrap();
    assert_eq!(
        engine.info().series_count,
        1,
        "removed inside the transaction"
    );
    engine.txn_rollback();

    assert_eq!(engine.info().series_count, 2, "and back with the rollback");
    assert_eq!(
        engine.resolve_cached("gone", &labels).unwrap(),
        gone,
        "with the id it had"
    );
    // The chunk itself is the host's to bring back with its rollback, and
    // this file store has no host: what comes back with the data is shown
    // over SQLite, in the extension's tests.
    engine.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Retention skips its high-water walk on a cheap upper bound (#125); the
/// bound is never lowered, so after a rollback it can sit above the real
/// high water. A cutoff must still come from the real one.
#[test]
fn a_rolled_back_write_does_not_move_the_cutoff() {
    let dir = temp_dir("bound_rollback");
    let labels: HashMap<String, String> = HashMap::new();
    let engine = Engine::new(dir.clone(), 100_000, 0, 3, 64 << 20, false).unwrap();
    engine.set_retention(Some(1_600));
    let sid = engine.resolve_cached("cpu", &labels).unwrap();
    let timestamps = |engine: &Engine| {
        engine
            .query_range_by_id(sid, i64::MIN, i64::MAX)
            .unwrap()
            .into_iter()
            .map(|(ts, _)| ts)
            .collect::<Vec<_>>()
    };
    for ts in [3_000, 4_000, 5_000] {
        engine.write_point(sid, ts, 1.0);
        engine.flush_all().unwrap();
    }
    assert_eq!(timestamps(&engine), vec![4_000, 5_000], "cutoff 5000-1600");

    // A write far ahead that never happened raises the bound only.
    engine.txn_begin();
    engine.write_point(sid, 9_000, 9.0);
    engine.txn_rollback();
    engine.flush_all().unwrap();
    assert_eq!(
        timestamps(&engine),
        vec![4_000, 5_000],
        "the cutoff is not 9000-1600"
    );

    engine.write_point(sid, 6_000, 1.0);
    engine.flush_all().unwrap();
    assert_eq!(timestamps(&engine), vec![5_000, 6_000], "cutoff 6000-1600");
    drop(engine);
    let _ = std::fs::remove_dir_all(dir);
}
