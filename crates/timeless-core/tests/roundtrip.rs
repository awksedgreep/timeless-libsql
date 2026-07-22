//! Extraction acceptance test: the engine behaves standalone exactly as it
//! did inside the NIF — write, query before AND after flush, compress,
//! recover from disk after a restart.

use std::collections::HashMap;
use timeless_core::{AggFn, Engine};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("timeless_core_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn new_engine(dir: &std::path::Path) -> Engine {
    // (data_dir, flush_threshold, min_flush_size, compression_level,
    //  memory_budget, defer_compression)
    Engine::new(dir.to_path_buf(), 1000, 0, 8, 64 * 1024 * 1024, false)
}

#[test]
fn write_flush_query_recover() {
    let dir = temp_dir("roundtrip");

    let labels: HashMap<String, String> =
        [("host".to_string(), "pvm1".to_string())].into_iter().collect();

    let n_points: i64 = 10_000;
    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("cpu_usage", &labels).unwrap();

        for ts in 0..n_points {
            engine.write_point(sid, ts, ts as f64 * 1.5);
        }

        // Timeless property: queryable BEFORE flush.
        let rows = engine.query_range_by_id(sid, 0, n_points).unwrap();
        assert_eq!(rows.len(), n_points as usize, "pre-flush query sees buffered points");
        assert_eq!(rows[42], (42, 63.0));

        engine.flush_all().unwrap();

        // ... and AFTER flush.
        let rows = engine.query_range_by_id(sid, 0, n_points).unwrap();
        assert_eq!(rows.len(), n_points as usize, "post-flush query sees persisted points");

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
        assert_eq!(rows.len(), n_points as usize, "recovery rebuilds index from chunk files");
        assert_eq!(rows[9_999], (9_999, 9_999.0 * 1.5));

        let info = engine.info();
        assert_eq!(info.series_count, 1);
        assert!(info.total_points >= n_points as u64);
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
