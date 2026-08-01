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
            flush_threshold: 100_000,
            ..Default::default()
        },
    )
    .unwrap();
    engine.set_retention(Some(1_000));

    let entry = |ts: i64| LogEntry {
        ts,
        level: 1,
        message: format!("m{ts}"),
        metadata: vec![],
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
