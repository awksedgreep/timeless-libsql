//! The engine over its store: what it recovers on reopen, and what it does
//! when the store fails or answers short. The store is the in-memory one
//! (`MemStore`), which answers the same questions the SQLite store does and
//! can be told to fail.

use std::collections::HashMap;

use timeless_core::{Engine, MemFault, MemStore};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("timeless_core_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn new_engine(dir: &std::path::Path) -> Engine {
    // (data_dir, flush_threshold, min_flush_size, compression_level,
    //  memory_budget, defer_compression)
    Engine::in_memory(dir, 1000, 0, 8, 64 * 1024 * 1024, false).unwrap()
}

#[test]
fn a_reopened_engine_reads_what_was_stored() {
    let dir = temp_dir("store_seam");

    let labels: HashMap<String, String> = [("host".to_string(), "pvm1".to_string())]
        .into_iter()
        .collect();

    let n_points: i64 = 5_000;
    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("cpu_usage", &labels).unwrap();
        for ts in 0..n_points {
            engine.write_point(sid, ts, ts as f64 * 2.0);
        }
        engine.shutdown().unwrap();
    }

    // "Restart": another engine over the same store finds the series and
    // every chunk, with nothing held from the first.
    {
        let engine = Engine::with_store(
            Box::new(MemStore::named(&dir)),
            1000,
            0,
            8,
            64 * 1024 * 1024,
            false,
        )
        .unwrap();
        let sid = engine.resolve_cached("cpu_usage", &labels).unwrap();
        let rows = engine.query_range_by_id(sid, 0, n_points).unwrap();
        assert_eq!(
            rows.len(),
            n_points as usize,
            "a reopened engine reads every stored point"
        );
        assert_eq!(rows[4_999], (4_999, 4_999.0 * 2.0));

        let info = engine.info();
        assert_eq!(info.series_count, 1);
        assert!(info.total_bytes > 0, "storage_stats sees the chunk rows");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn compaction_failure_restart_and_backfill_preserve_exact_metrics() {
    let dir = temp_dir("compact_failure_restart_backfill");
    let store = MemStore::named(&dir);
    let labels = HashMap::new();
    let engine = Engine::with_store(
        Box::new(store.clone()),
        100_000,
        0,
        8,
        64 * 1024 * 1024,
        true,
    )
    .unwrap();
    let sid = engine.resolve_cached("failure_restart", &labels).unwrap();
    for ts in 0..32_768i64 {
        engine.write_point(sid, ts, ts as f64);
    }
    engine.flush_all().unwrap();

    store.inject(MemFault::ReplaceFails);
    let error = engine.compact_partitions(i64::MAX).unwrap_err();
    assert_eq!(error, "injected compaction replacement failure");
    let rows = engine.query_range_by_id(sid, i64::MIN, i64::MAX).unwrap();
    assert_eq!(rows.len(), 32_768);
    assert_eq!(rows.first(), Some(&(0, 0.0)));
    assert_eq!(rows.last(), Some(&(32_767, 32_767.0)));

    store.inject(MemFault::None);
    engine.compact_partitions(i64::MAX).unwrap();
    drop(engine);

    let reopened = Engine::with_store(
        Box::new(store.clone()),
        100_000,
        0,
        8,
        64 * 1024 * 1024,
        true,
    )
    .unwrap();
    let sid = reopened.resolve_cached("failure_restart", &labels).unwrap();
    for ts in -1024..0i64 {
        reopened.write_point(sid, ts, ts as f64);
    }
    reopened.flush_all().unwrap();
    reopened.compact_partitions(i64::MAX).unwrap();

    let rows = reopened.query_range_by_id(sid, i64::MIN, i64::MAX).unwrap();
    assert_eq!(rows.len(), 33_792);
    assert_eq!(rows.first(), Some(&(-1024, -1024.0)));
    assert_eq!(rows.last(), Some(&(32_767, 32_767.0)));
    drop(reopened);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn aggregate_and_latest_batches_propagate_store_contract_failures() {
    let dir = temp_dir("batch_store_failures");
    let store = MemStore::named(&dir);
    let engine =
        Engine::with_store(Box::new(store.clone()), 1000, 0, 8, 64 * 1024 * 1024, false).unwrap();
    let sid = engine.resolve_cached("cpu", &HashMap::new()).unwrap();
    for timestamp in 0..10 {
        engine.write_point(sid, timestamp, timestamp as f64);
    }
    engine.flush_all().unwrap();

    // The strict subrange forces both primitives off their metadata-only fast
    // paths so the injected batch-store behavior is observed.
    store.inject(MemFault::BatchReadFails);
    for error in [
        engine
            .query_aggregate_summary_batch_by_id(&[sid], 1, 8)
            .unwrap_err(),
        engine.query_latest_batch_by_id(&[sid], 1, 8).unwrap_err(),
    ] {
        assert!(error.contains("injected batch read failure"), "{error}");
    }

    store.inject(MemFault::BatchReadShort);
    for error in [
        engine
            .query_aggregate_summary_batch_by_id(&[sid], 1, 8)
            .unwrap_err(),
        engine.query_latest_batch_by_id(&[sid], 1, 8).unwrap_err(),
    ] {
        assert!(
            error.contains("returned 0 payloads for 1 locations"),
            "{error}"
        );
    }

    store.inject(MemFault::None);
    engine.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn latest_work_limit_precedes_payload_reads_and_preserves_metadata_fast_path() {
    let dir = temp_dir("latest_budget");
    let store = MemStore::named(&dir);
    let engine =
        Engine::with_store(Box::new(store.clone()), 1000, 0, 8, 64 * 1024 * 1024, false).unwrap();
    let sid = engine.resolve_cached("cpu", &HashMap::new()).unwrap();
    for timestamp in 0..10 {
        engine.write_point(sid, timestamp, timestamp as f64);
    }
    engine.flush_all().unwrap();

    store.inject(MemFault::BatchReadFails);
    let error = engine
        .query_latest_batch_by_id_limited(&[sid], 0, 8, 9)
        .unwrap_err();
    assert!(
        error.contains("work point limit 9 exceeded"),
        "payload read occurred before admission: {error}"
    );
    assert!(engine
        .query_latest_batch_by_id_limited(&[sid], 0, 8, 10)
        .unwrap_err()
        .contains("injected batch read failure"));

    store.inject(MemFault::None);
    assert_eq!(
        engine
            .query_latest_batch_by_id_limited(&[sid], 0, 9, 1)
            .unwrap(),
        vec![(sid, Some((9, 9.0)))]
    );
    engine.write_point(sid, 10, 10.0);
    assert!(engine
        .query_latest_batch_by_id_limited(&[sid], 0, 10, 1)
        .unwrap_err()
        .contains("work point limit"));
    assert_eq!(
        engine
            .query_latest_batch_by_id_limited(&[sid], 0, 10, 2)
            .unwrap(),
        vec![(sid, Some((10, 10.0)))]
    );
    engine.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
