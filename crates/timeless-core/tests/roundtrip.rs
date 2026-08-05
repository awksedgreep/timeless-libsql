//! Extraction acceptance test: the engine behaves standalone exactly as it
//! did inside the NIF — write, query before AND after flush, compress,
//! recover from disk after a restart.

use std::collections::HashMap;
use timeless_core::{AggFn, Engine, WindowOp};

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
    Engine::new(dir.to_path_buf(), 1000, 0, 8, 64 * 1024 * 1024, false).unwrap()
}

#[test]
fn write_flush_query_recover() {
    let dir = temp_dir("roundtrip");

    let labels: HashMap<String, String> = [("host".to_string(), "pvm1".to_string())]
        .into_iter()
        .collect();

    let n_points: i64 = 10_000;
    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("cpu_usage", &labels).unwrap();

        for ts in 0..n_points {
            engine.write_point(sid, ts, ts as f64 * 1.5);
        }

        // Timeless property: queryable BEFORE flush.
        let rows = engine.query_range_by_id(sid, 0, n_points).unwrap();
        assert_eq!(
            rows.len(),
            n_points as usize,
            "pre-flush query sees buffered points"
        );
        assert_eq!(rows[42], (42, 63.0));
        let buffered_batch = engine
            .query_range_batch_by_id(&[sid + 10_000, sid], 0, n_points)
            .unwrap();
        assert_eq!(buffered_batch[0], (sid + 10_000, Vec::new()));
        assert_eq!(buffered_batch[1], (sid, rows.clone()));
        let error = engine
            .query_range_batch_by_id_limited(&[sid], 0, n_points, 9_999)
            .unwrap_err();
        assert_eq!(
            error,
            "raw batch work point limit 9999 exceeded (candidate points: 10000)"
        );
        assert_eq!(
            engine
                .query_range_batch_by_id_limited(&[sid], 0, n_points, 10_000)
                .unwrap(),
            vec![(sid, rows.clone())],
            "the work-point limit is inclusive for buffered data"
        );
        assert_eq!(
            engine
                .query_window_op_batch_by_id_limited(
                    &[sid],
                    0,
                    9,
                    1,
                    10,
                    WindowOp::Agg(AggFn::Avg),
                    9,
                )
                .unwrap_err(),
            "window batch work point limit 9 exceeded (possible output points: 10)"
        );
        assert_eq!(
            engine
                .query_window_op_batch_by_id_limited(
                    &[sid],
                    n_points - 1,
                    n_points - 1,
                    1,
                    n_points,
                    WindowOp::Agg(AggFn::Avg),
                    9_999,
                )
                .unwrap_err(),
            "window batch work point limit 9999 exceeded (candidate input points: 10000)"
        );

        engine.flush_all().unwrap();

        // ... and AFTER flush.
        let rows = engine.query_range_by_id(sid, 0, n_points).unwrap();
        assert_eq!(
            rows.len(),
            n_points as usize,
            "post-flush query sees persisted points"
        );
        let persisted_batch = engine.query_range_batch_by_id(&[sid], 0, n_points).unwrap();
        assert_eq!(persisted_batch, vec![(sid, rows)]);
        let error = engine
            .query_range_batch_by_id_limited(&[sid], 0, n_points, 9_999)
            .unwrap_err();
        assert_eq!(
            error, "raw batch work point limit 9999 exceeded (candidate points: 10000)",
            "persisted chunks are rejected before their payloads are decoded"
        );

        // Aggregate path.
        let aggs = engine
            .query_aggregate_labeled("cpu_usage", &Default::default(), 0, n_points, AggFn::Max)
            .unwrap();
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].1, (n_points - 1) as f64 * 1.5);

        engine.shutdown().unwrap();
    }

    // "Restart": brand-new engine over the same data_dir must recover the
    // series registry and chunk index from disk.
    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("cpu_usage", &labels).unwrap();
        let rows = engine.query_range_by_id(sid, 0, n_points).unwrap();
        assert_eq!(
            rows.len(),
            n_points as usize,
            "recovery rebuilds index from chunk files"
        );
        assert_eq!(rows[9_999], (9_999, 9_999.0 * 1.5));

        let info = engine.info();
        assert_eq!(info.series_count, 1);
        assert!(info.total_points >= n_points as u64);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn distinct_nan_payloads_survive_buffer_flush_and_reopen() {
    let dir = temp_dir("nan_payload_roundtrip");
    let labels = HashMap::new();
    let ordinary_nan = f64::from_bits(0x7ff8_0000_0000_0000);
    let prometheus_stale_nan = f64::from_bits(0x7ff0_0000_0000_0002);

    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("nan_payloads", &labels).unwrap();
        engine.write_point(sid, 10, ordinary_nan);
        engine.write_point(sid, 20, prometheus_stale_nan);

        let buffered = engine.query_range_by_id(sid, 0, 30).unwrap();
        assert_eq!(buffered[0].1.to_bits(), ordinary_nan.to_bits());
        assert_eq!(buffered[1].1.to_bits(), prometheus_stale_nan.to_bits());
        assert_ne!(buffered[0].1.to_bits(), buffered[1].1.to_bits());

        engine.flush_all().unwrap();
        let persisted = engine.query_range_by_id(sid, 0, 30).unwrap();
        assert_eq!(persisted[0].1.to_bits(), ordinary_nan.to_bits());
        assert_eq!(persisted[1].1.to_bits(), prometheus_stale_nan.to_bits());
        engine.shutdown().unwrap();
    }

    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("nan_payloads", &labels).unwrap();
        let reopened = engine.query_range_by_id(sid, 0, 30).unwrap();
        assert_eq!(reopened[0].1.to_bits(), ordinary_nan.to_bits());
        assert_eq!(reopened[1].1.to_bits(), prometheus_stale_nan.to_bits());
        assert_ne!(reopened[0].1.to_bits(), reopened[1].1.to_bits());
        engine.shutdown().unwrap();
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn aggregate_summary_covers_chunks_boundaries_buffer_and_reopen() {
    let dir = temp_dir("aggregate_summary");
    let labels: HashMap<String, String> = [("host".to_string(), "a".to_string())]
        .into_iter()
        .collect();

    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("cpu", &labels).unwrap();
        for (ts, value) in [(-5, -2.0), (20, 8.0), (0, 4.0), (10, 6.0)] {
            engine.write_point(sid, ts, value);
        }
        engine.flush_all().unwrap();
        engine.write_point(sid, 30, 10.0);

        let all = engine
            .query_aggregate_summary_by_id(sid, -5, 30)
            .unwrap()
            .unwrap();
        assert_eq!(all.count(), 5);
        assert_eq!(all.value(AggFn::Sum), 26.0);
        assert_eq!(all.value(AggFn::Avg), 5.2);
        assert_eq!(all.value(AggFn::Min), -2.0);
        assert_eq!(all.value(AggFn::Max), 10.0);

        let boundary = engine
            .query_aggregate_summary_by_id(sid, 0, 10)
            .unwrap()
            .unwrap();
        assert_eq!(boundary.count(), 2);
        assert_eq!(boundary.value(AggFn::Sum), 10.0);
        assert!(engine
            .query_aggregate_summary_by_id(sid, 100, 200)
            .unwrap()
            .is_none());

        let nan_labels: HashMap<String, String> = [("host".to_string(), "nan".to_string())]
            .into_iter()
            .collect();
        let nan_sid = engine.resolve_cached("nan_metric", &nan_labels).unwrap();
        engine.write_point(nan_sid, 0, f64::NAN);
        engine.write_point(nan_sid, 1, 2.0);
        engine.write_point(nan_sid, 2, 4.0);
        engine.flush_all().unwrap();
        let nan_summary = engine
            .query_aggregate_summary_by_id(nan_sid, 0, 2)
            .unwrap()
            .unwrap();
        assert_eq!(nan_summary.count(), 3);
        assert!(nan_summary.value(AggFn::Sum).is_nan());
        assert!(nan_summary.value(AggFn::Avg).is_nan());
        assert_eq!(nan_summary.value(AggFn::Min), 2.0);
        assert_eq!(nan_summary.value(AggFn::Max), 4.0);

        let ids = [sid + 10_000, sid, nan_sid, sid];
        let batch = engine
            .query_aggregate_summary_batch_by_id(&ids, 0, 10)
            .unwrap();
        assert_eq!(batch.len(), ids.len());
        for ((result_id, actual), expected_id) in batch.into_iter().zip(ids) {
            assert_eq!(result_id, expected_id);
            let expected = engine
                .query_aggregate_summary_by_id(expected_id, 0, 10)
                .unwrap();
            match (actual, expected) {
                (None, None) => {}
                (Some(actual), Some(expected)) => {
                    assert_eq!(actual.count(), expected.count());
                    for aggregate in [AggFn::Avg, AggFn::Sum, AggFn::Min, AggFn::Max, AggFn::Count]
                    {
                        assert_eq!(
                            actual.value(aggregate).to_bits(),
                            expected.value(aggregate).to_bits()
                        );
                    }
                }
                other => panic!("batch/individual aggregate mismatch: {other:?}"),
            }
        }
        assert!(engine
            .query_aggregate_summary_batch_by_id(&[sid, nan_sid], 10, 0)
            .unwrap()
            .into_iter()
            .all(|(_, summary)| summary.is_none()));

        engine.flush_all().unwrap();
        engine.shutdown().unwrap();
    }

    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("cpu", &labels).unwrap();
        let recovered = engine
            .query_aggregate_summary_by_id(sid, -5, 30)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.count(), 5);
        assert_eq!(recovered.value(AggFn::Sum), 26.0);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn latest_matches_stable_range_order_across_chunks_buffer_and_reopen() {
    let dir = temp_dir("latest");
    let labels: HashMap<String, String> = [("host".to_string(), "a".to_string())]
        .into_iter()
        .collect();

    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("cpu", &labels).unwrap();

        engine.write_point(sid, 10, 1.0);
        engine.write_point(sid, 30, 3.0);
        engine.flush_all().unwrap();

        // This later-created chunk sorts before the first one in the chunk
        // index because its min timestamp is smaller. Its duplicate at ts=30
        // therefore wins the stable range-query tie.
        engine.write_point(sid, 30, 4.0);
        engine.write_point(sid, 5, 0.5);
        engine.flush_all().unwrap();

        // Buffered duplicates follow persisted chunks and must not replace the
        // first persisted point at the same maximum timestamp.
        engine.write_point(sid, 30, 5.0);
        assert_eq!(
            engine.query_latest_by_id(sid, 0, 30).unwrap(),
            Some((30, 4.0))
        );
        assert_eq!(engine.query_latest_by_id(sid, 11, 29).unwrap(), None);

        engine.write_point(sid, 40, 6.0);
        assert_eq!(
            engine.query_latest_by_id(sid, 0, 100).unwrap(),
            Some((40, 6.0))
        );
        assert_eq!(engine.query_latest_by_id(sid, 41, 100).unwrap(), None);
        assert_eq!(engine.query_latest_by_id(sid, 10, 9).unwrap(), None);

        let ids = [sid + 10_000, sid, sid];
        let batch = engine.query_latest_batch_by_id(&ids, 0, 100).unwrap();
        let expected = engine.query_latest_by_id(sid, 0, 100).unwrap();
        assert_eq!(
            batch,
            vec![(sid + 10_000, None), (sid, expected), (sid, expected)]
        );
        assert_eq!(
            engine.query_latest_batch_by_id(&[sid], 10, 9).unwrap(),
            vec![(sid, None)]
        );

        engine.shutdown().unwrap();
    }

    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("cpu", &labels).unwrap();
        let points = engine.query_range_by_id(sid, 0, 100).unwrap();
        let max_ts = points.iter().map(|(ts, _)| *ts).max().unwrap();
        let expected = points.iter().copied().find(|(ts, _)| *ts == max_ts);
        assert_eq!(engine.query_latest_by_id(sid, 0, 100).unwrap(), expected);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn compression_kicks_in() {
    let dir = temp_dir("compress");
    let engine = new_engine(&dir);

    let labels: HashMap<String, String> = HashMap::new();
    let sid = engine.resolve_cached("gauge", &labels).unwrap();

    // A well-behaved series: slow drift, the pco sweet spot.
    let n: i64 = 100_000;
    for ts in 0..n {
        engine.write_point(sid, ts, 20.0 + (ts % 100) as f64 * 0.01);
    }
    engine.flush_all().unwrap();

    let info = engine.info();
    assert!(info.total_points >= n as u64);
    // 16 bytes/point raw (i64 ts + f64 val). pco should crush this.
    assert!(
        info.bytes_per_point < 2.0,
        "expected <2 bytes/point, got {}",
        info.bytes_per_point
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn print_compression_stats() {
    let dir = temp_dir("stats");
    let engine = new_engine(&dir);
    let sid = engine.resolve_cached("gauge", &HashMap::new()).unwrap();
    for ts in 0..1_000_000i64 {
        engine.write_point(sid, ts, 20.0 + (ts % 100) as f64 * 0.01);
    }
    engine.flush_all().unwrap();
    let info = engine.info();
    println!(
        "1M points: {} bytes total, {:.3} bytes/point ({}x vs 16B raw)",
        info.total_bytes,
        info.bytes_per_point,
        (16.0 / info.bytes_per_point) as u64
    );
    let _ = std::fs::remove_dir_all(&dir);
}
