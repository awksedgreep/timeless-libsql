//! F1: Engine::series_overview must agree with a naive per-series walk
//! over the engine's own query results (FEATURE_PLAN.md F1).

use std::collections::HashMap;

use timeless_core::Engine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("timeless_f1_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn overview_matches_naive_walk() {
    let dir = temp_dir("overview");
    let engine = Engine::new(dir, 10_000, 0, 3, 64 * 1024 * 1024, false).unwrap();

    // Three series across two metrics; one gets flushed data + buffered
    // overlap, one flushed only, one buffered only.
    let mut sids = Vec::new();
    for (metric, host) in [("cpu", "a"), ("cpu", "b"), ("mem", "a")] {
        let labels: HashMap<String, String> =
            [("host".to_string(), host.to_string())].into_iter().collect();
        sids.push(engine.resolve_cached(metric, &labels).unwrap());
    }
    for ts in 0..500 {
        engine.write_point(sids[0], ts * 10, ts as f64);
        engine.write_point(sids[1], ts * 10 + 5, ts as f64 * 2.0);
    }
    engine.flush_all().unwrap();
    for ts in 0..100 {
        engine.write_point(sids[0], 4000 + ts, 1.0); // overlaps flushed range
        engine.write_point(sids[2], 100_000 + ts, 3.0); // buffered-only series
    }

    let overview = engine.series_overview();
    assert_eq!(overview.len(), 3);

    for row in &overview {
        // Naive truth: everything the engine says is queryable.
        let all = engine
            .query_range_by_id(row.series_id, i64::MIN, i64::MAX)
            .unwrap();
        assert_eq!(
            row.disk_points as usize + row.buffered,
            all.len(),
            "series {} point count",
            row.series_id
        );
        assert_eq!(
            row.min_ts,
            all.first().map(|&(ts, _)| ts),
            "series {} min_ts",
            row.series_id
        );
        assert_eq!(
            row.max_ts,
            all.last().map(|&(ts, _)| ts),
            "series {} max_ts",
            row.series_id
        );
    }

    // Buffered-only series: no chunks, everything in the buffer.
    let mem = overview.iter().find(|r| r.name == "mem").unwrap();
    assert_eq!((mem.chunks, mem.disk_points, mem.buffered), (0, 0, 100));

    // Ordering contract: sorted by (name, series_id).
    let names: Vec<_> = overview.iter().map(|r| (&r.name, r.series_id)).collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted);

    engine.shutdown().unwrap();
}
