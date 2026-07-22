//! Response to a fair challenge: "120x compression - are you sure the round
//! trip actually works?" These tests (a) verify EVERY point decodes exactly
//! on the series that produced the 120x number, (b) measure bytes/point from
//! actual on-disk file sizes rather than engine bookkeeping, and (c) measure
//! a realistic random-walk series for an honest number.

use std::collections::HashMap;
use timeless_core::Engine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("timeless_honesty_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn new_engine(dir: &std::path::Path) -> Engine {
    Engine::new(dir.to_path_buf(), 1000, 0, 8, 64 * 1024 * 1024, false)
}

/// Sum of all file bytes under dir - the disk truth, no engine bookkeeping.
fn disk_bytes(dir: &std::path::Path) -> u64 {
    fn walk(d: &std::path::Path, total: &mut u64) {
        for e in std::fs::read_dir(d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, total);
            } else {
                *total += e.metadata().unwrap().len();
            }
        }
    }
    let mut total = 0;
    walk(dir, &mut total);
    total
}

const N: i64 = 1_000_000;

#[test]
fn periodic_series_every_point_survives() {
    let dir = temp_dir("periodic");
    let expect = |ts: i64| 20.0 + (ts % 100) as f64 * 0.01;

    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("gauge", &HashMap::new()).unwrap();
        for ts in 0..N {
            engine.write_point(sid, ts, expect(ts));
        }
        engine.flush_all().unwrap();
        engine.shutdown().unwrap();
    }

    // Fresh engine = recovery from disk only. Then verify ALL 1M points.
    let engine = new_engine(&dir);
    let sid = engine.resolve_cached("gauge", &HashMap::new()).unwrap();
    let rows = engine.query_range_by_id(sid, 0, N).unwrap();
    assert_eq!(rows.len(), N as usize, "row count after recovery");
    for (i, (ts, val)) in rows.iter().enumerate() {
        assert_eq!(*ts, i as i64, "timestamp mismatch at {i}");
        assert_eq!(*val, expect(*ts), "value mismatch at ts={ts}");
    }

    let bytes = disk_bytes(&dir);
    println!(
        "PERIODIC (best case): {} points, {} bytes ON DISK = {:.3} bytes/point ({:.0}x vs 16B)",
        N, bytes, bytes as f64 / N as f64, 16.0 / (bytes as f64 / N as f64)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn random_walk_every_point_survives() {
    let dir = temp_dir("walk");

    // Deterministic xorshift PRNG - no dependencies, reproducible.
    let mut state: u64 = 0x9E3779B97F4A7C15;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    // Realistic shape: scrape every ~10s with 0-999ms jitter (unix seconds
    // would all collide; use millis), CPU-like random walk clamped 0..100
    // with float noise. This is deliberately unfriendly compared to the
    // periodic sawtooth.
    let mut points: Vec<(i64, f64)> = Vec::with_capacity(N as usize);
    let mut ts: i64 = 1_753_000_000_000;
    let mut val: f64 = 40.0;
    for _ in 0..N {
        ts += 10_000 + (next() % 1000) as i64;
        val += ((next() % 2001) as f64 - 1000.0) / 500.0; // +/- 2.0 drift
        val = val.clamp(0.0, 100.0);
        let noisy = val + ((next() % 1000) as f64) / 10_000.0; // 4 decimal noise
        points.push((ts, noisy));
    }

    {
        let engine = new_engine(&dir);
        let sid = engine.resolve_cached("cpu", &HashMap::new()).unwrap();
        for &(ts, v) in &points {
            engine.write_point(sid, ts, v);
        }
        engine.flush_all().unwrap();
        engine.shutdown().unwrap();
    }

    let engine = new_engine(&dir);
    let sid = engine.resolve_cached("cpu", &HashMap::new()).unwrap();
    let rows = engine
        .query_range_by_id(sid, 0, i64::MAX)
        .unwrap();
    assert_eq!(rows.len(), N as usize, "row count after recovery");
    for (i, (ts, val)) in rows.iter().enumerate() {
        assert_eq!(*ts, points[i].0, "timestamp mismatch at {i}");
        assert_eq!(*val, points[i].1, "value mismatch at {i} (lossless check)");
    }

    let bytes = disk_bytes(&dir);
    println!(
        "RANDOM WALK (honest): {} points, {} bytes ON DISK = {:.3} bytes/point ({:.0}x vs 16B)",
        N, bytes, bytes as f64 / N as f64, 16.0 / (bytes as f64 / N as f64)
    );
    let _ = std::fs::remove_dir_all(&dir);
}
