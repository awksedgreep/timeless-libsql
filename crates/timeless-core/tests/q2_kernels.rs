//! Q2 kernel bit-exactness (PLAN.md "Query interface tiers").
//!
//! The kernels are only allowed to exist because they are semantics-free
//! and mechanically verifiable: a naive evaluator over the same raw
//! samples must agree on EVERY BIT. These tests are that verifier —
//! independent naive implementations of grid-last and window-agg,
//! compared by f64 bit pattern against the engine kernels across a
//! deterministic randomized sweep over buffered + flushed + duplicate
//! timestamps.

use std::collections::HashMap;

use timeless_core::{AggFn, Engine, WindowOp};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("timeless_q2_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn new_engine(dir: &std::path::Path) -> Engine {
    Engine::new(dir.to_path_buf(), 100_000, 0, 3, 64 * 1024 * 1024, false).unwrap()
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64* — deterministic across platforms.
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Naive grid-last: for each grid point scan the WHOLE sorted sample
/// slice for the last sample in (t - lookback, t].
fn naive_grid_last(
    samples: &[(i64, f64)],
    start: i64,
    stop: i64,
    step: i64,
    lookback: i64,
) -> Vec<(i64, f64)> {
    let mut out = Vec::new();
    let mut t = start;
    while t <= stop {
        let hit = samples
            .iter()
            .rfind(|&&(ts, _)| ts <= t && (ts as i128) > (t as i128) - (lookback as i128));
        if let Some(&(_, val)) = hit {
            out.push((t, val));
        }
        match t.checked_add(step) {
            Some(next) => t = next,
            None => break,
        }
    }
    out
}

fn reference_compensated_average(values: &[f64]) -> f64 {
    let add = |increment: f64, sum: f64, compensation: f64| {
        let total = sum + increment;
        let compensation = if total.is_infinite() {
            0.0
        } else if sum.abs() >= increment.abs() {
            compensation + ((sum - total) + increment)
        } else {
            compensation + ((increment - total) + sum)
        };
        (total, compensation)
    };
    let mut sum = values[0];
    let mut compensation = 0.0;
    let mut mean = sum;
    let mut count = 1.0;
    let mut incremental = false;
    for &value in &values[1..] {
        count += 1.0;
        if !incremental {
            let (next_sum, next_compensation) = add(value, sum, compensation);
            if !next_sum.is_infinite() {
                sum = next_sum;
                compensation = next_compensation;
                continue;
            }
            incremental = true;
            mean = sum / (count - 1.0);
            compensation /= count - 1.0;
        }
        let weight = (count - 1.0) / count;
        (mean, compensation) = add(value / count, weight * mean, weight * compensation);
    }
    if incremental {
        mean + compensation
    } else {
        sum / count + compensation / count
    }
}

/// Reference window agg: collect the complete window and apply each pinned fold.
fn naive_window_agg(
    samples: &[(i64, f64)],
    start: i64,
    stop: i64,
    step: i64,
    window: i64,
    agg: AggFn,
) -> Vec<(i64, f64)> {
    let mut out = Vec::new();
    let mut t = start;
    while t <= stop {
        let win: Vec<f64> = samples
            .iter()
            .filter(|&&(ts, _)| ts <= t && (ts as i128) > (t as i128) - (window as i128))
            .map(|&(_, v)| v)
            .collect();
        if !win.is_empty() {
            let value = match agg {
                AggFn::Count => win.len() as f64,
                AggFn::Sum => win.iter().fold(0.0f64, |a, &v| a + v),
                AggFn::Avg => reference_compensated_average(&win),
                AggFn::Min => win[1..].iter().fold(win[0], |a, &v| f64::min(a, v)),
                AggFn::Max => win[1..].iter().fold(win[0], |a, &v| f64::max(a, v)),
            };
            out.push((t, value));
        }
        match t.checked_add(step) {
            Some(next) => t = next,
            None => break,
        }
    }
    out
}

fn assert_bit_eq(kernel: &[(i64, f64)], naive: &[(i64, f64)], what: &str) {
    assert_eq!(
        kernel.len(),
        naive.len(),
        "{what}: kernel returned {} points, naive {}",
        kernel.len(),
        naive.len()
    );
    for (i, (k, n)) in kernel.iter().zip(naive).enumerate() {
        assert_eq!(k.0, n.0, "{what}: grid ts mismatch at point {i}");
        assert_eq!(
            k.1.to_bits(),
            n.1.to_bits(),
            "{what}: value bits differ at point {i} (ts {}): kernel {} naive {}",
            k.0,
            k.1,
            n.1
        );
    }
}

/// Randomized sweep: several series with ms-jitter timestamps, duplicate
/// timestamps, a flushed portion AND a buffered portion, checked across
/// random (start, stop, step, lookback/window) draws and every AggFn.
#[test]
fn kernels_match_naive_reference() {
    let dir = temp_dir("sweep");
    let engine = new_engine(&dir);
    let mut rng = Rng(0x5EED_CAFE_F00D_D00D);

    let n_series = 5usize;
    let mut sids = Vec::new();
    for s in 0..n_series {
        let labels: HashMap<String, String> = [("host".to_string(), format!("h{s}"))]
            .into_iter()
            .collect();
        sids.push(engine.resolve_cached("q2.metric", &labels).unwrap());
    }

    // Flushed portion: 400 points per series, jittered, some duplicates.
    let base = 1_700_000_000i64;
    for &sid in &sids {
        let mut ts = base;
        for _ in 0..400 {
            ts += rng.below(20) as i64; // 0..19 step: duplicates happen
            let val = (rng.next() as f64) / (u64::MAX as f64) * 1000.0 - 500.0;
            engine.write_point(sid, ts, val);
        }
    }
    engine.flush_all().unwrap();
    // Buffered portion on top, overlapping the flushed range.
    for &sid in &sids {
        let mut ts = base + 3000;
        for _ in 0..200 {
            ts += rng.below(20) as i64;
            let val = (rng.next() as f64) / (u64::MAX as f64) * 1000.0 - 500.0;
            engine.write_point(sid, ts, val);
        }
    }

    let aggs = [AggFn::Sum, AggFn::Min, AggFn::Max, AggFn::Count, AggFn::Avg];
    for round in 0..50 {
        let start = base + rng.below(9000) as i64 - 500;
        let stop = start + rng.below(4000) as i64;
        let step = 1 + rng.below(60) as i64;
        let lookback = rng.below(150) as i64;
        let window = 1 + rng.below(150) as i64;

        for &sid in &sids {
            // The reference consumes the engine's own sorted sample order
            // (that order IS part of the contract; ties = last wins).
            let all = engine.query_range_by_id(sid, i64::MIN, i64::MAX).unwrap();

            let kernel = engine
                .query_grid_last_by_id(sid, start, stop, step, lookback)
                .unwrap();
            let naive = naive_grid_last(&all, start, stop, step, lookback);
            assert_bit_eq(
                &kernel,
                &naive,
                &format!("round {round} sid {sid} grid_last(start={start},stop={stop},step={step},lookback={lookback})"),
            );

            for agg in aggs {
                let kernel = engine
                    .query_window_agg_by_id(sid, start, stop, step, window, agg)
                    .unwrap();
                let naive = naive_window_agg(&all, start, stop, step, window, agg);
                assert_bit_eq(
                    &kernel,
                    &naive,
                    &format!("round {round} sid {sid} window_agg({agg:?},start={start},stop={stop},step={step},window={window})"),
                );
            }
        }
    }
    engine.shutdown().unwrap();
}

/// The labeled parallel wrappers must return exactly what the per-series
/// kernels return, keyed by the same label sets.
#[test]
fn labeled_wrappers_match_by_id() {
    let dir = temp_dir("labeled");
    let engine = new_engine(&dir);
    let mut rng = Rng(0xBEEF_BEEF_BEEF_BEEF);

    let mut by_labels = Vec::new();
    for s in 0..4 {
        let labels: HashMap<String, String> = [("host".to_string(), format!("h{s}"))]
            .into_iter()
            .collect();
        let sid = engine.resolve_cached("q2.labeled", &labels).unwrap();
        let mut ts = 1000i64;
        for _ in 0..300 {
            ts += rng.below(15) as i64;
            engine.write_point(sid, ts, (rng.below(1_000_000) as f64) * 0.001);
        }
        by_labels.push((labels, sid));
    }
    engine.flush_all().unwrap();

    let (start, stop, step, lookback, window) = (900i64, 6000i64, 30i64, 45i64, 120i64);
    let grid = engine
        .query_grid_last(
            "q2.labeled",
            &Default::default(),
            start,
            stop,
            step,
            lookback,
        )
        .unwrap();
    assert_eq!(grid.len(), 4, "all four series produce grid rows");
    for (labels, points) in &grid {
        let host = labels.get("host").unwrap();
        let (_, sid) = by_labels
            .iter()
            .find(|(l, _)| l.get("host").unwrap() == host)
            .unwrap();
        let expect = engine
            .query_grid_last_by_id(*sid, start, stop, step, lookback)
            .unwrap();
        assert_bit_eq(points, &expect, &format!("labeled grid host {host}"));
    }

    let win = engine
        .query_window_agg(
            "q2.labeled",
            &Default::default(),
            start,
            stop,
            step,
            window,
            AggFn::Avg,
        )
        .unwrap();
    assert_eq!(win.len(), 4);
    for (labels, points) in &win {
        let host = labels.get("host").unwrap();
        let (_, sid) = by_labels
            .iter()
            .find(|(l, _)| l.get("host").unwrap() == host)
            .unwrap();
        let expect = engine
            .query_window_agg_by_id(*sid, start, stop, step, window, AggFn::Avg)
            .unwrap();
        assert_bit_eq(points, &expect, &format!("labeled window host {host}"));
    }

    let sids: Vec<i64> = by_labels.iter().map(|(_, sid)| *sid).collect();
    let batch = engine
        .query_window_op_batch_by_id(&sids, start, stop, step, window, WindowOp::Agg(AggFn::Avg))
        .unwrap();
    assert_eq!(batch.len(), sids.len());
    for ((result_sid, points), expected_sid) in batch.iter().zip(&sids) {
        assert_eq!(result_sid, expected_sid, "batch retains input series order");
        let expect = engine
            .query_window_agg_by_id(*expected_sid, start, stop, step, window, AggFn::Avg)
            .unwrap();
        assert_bit_eq(
            points,
            &expect,
            &format!("batched window sid {expected_sid}"),
        );
    }
    engine.shutdown().unwrap();
}

#[test]
fn window_average_compensates_cancellation_and_falls_back_before_overflow() {
    let dir = temp_dir("compensated_average");
    let engine = new_engine(&dir);
    let labels = HashMap::new();

    let precision = engine.resolve_cached("precision", &labels).unwrap();
    for (timestamp, value) in [(10, 1e16), (20, 1.0), (30, -1e16)] {
        engine.write_point(precision, timestamp, value);
    }
    let average = engine
        .query_window_agg_by_id(precision, 30, 30, 1, 30, AggFn::Avg)
        .unwrap();
    assert_eq!(average[0].1.to_bits(), (1.0_f64 / 3.0).to_bits());

    let overflow = engine.resolve_cached("overflow", &labels).unwrap();
    engine.write_point(overflow, 20, f64::MAX);
    engine.write_point(overflow, 30, f64::MAX);
    let average = engine
        .query_window_agg_by_id(overflow, 30, 30, 1, 20, AggFn::Avg)
        .unwrap();
    assert_eq!(average[0].1.to_bits(), f64::MAX.to_bits());

    for (name, values, expected) in [
        ("nan", [f64::NAN, 1.0], f64::NAN),
        ("positive", [f64::INFINITY, 1.0], f64::INFINITY),
        ("mixed", [f64::INFINITY, f64::NEG_INFINITY], f64::NAN),
    ] {
        let series = engine.resolve_cached(name, &labels).unwrap();
        engine.write_point(series, 20, values[0]);
        engine.write_point(series, 30, values[1]);
        let average = engine
            .query_window_agg_by_id(series, 30, 30, 1, 20, AggFn::Avg)
            .unwrap()[0]
            .1;
        if expected.is_nan() {
            assert!(average.is_nan(), "{name}");
        } else {
            assert_eq!(average.to_bits(), expected.to_bits(), "{name}");
        }
    }
    engine.shutdown().unwrap();
}

/// Argument validation and empty-grid behavior.
#[test]
fn kernel_argument_contract() {
    let dir = temp_dir("args");
    let engine = new_engine(&dir);
    let labels: HashMap<String, String> = HashMap::new();
    let sid = engine.resolve_cached("q2.args", &labels).unwrap();
    engine.write_point(sid, 100, 1.0);

    // step must be positive
    assert!(engine.query_grid_last_by_id(sid, 0, 100, 0, 10).is_err());
    assert!(engine.query_grid_last_by_id(sid, 0, 100, -5, 10).is_err());
    // lookback must be >= 0; window must be > 0
    assert!(engine.query_grid_last_by_id(sid, 0, 100, 10, -1).is_err());
    assert!(engine
        .query_window_agg_by_id(sid, 0, 100, 10, 0, AggFn::Sum)
        .is_err());
    // stop < start: empty, not an error
    assert!(engine
        .query_grid_last_by_id(sid, 100, 0, 10, 10)
        .unwrap()
        .is_empty());
    // grid cap: an epoch-wide range at step 1 must be rejected loudly
    assert!(engine
        .query_grid_last_by_id(sid, 0, i64::MAX - 1, 1, 10)
        .is_err());
    // lookback 0 = exact grid hits only
    let exact = engine.query_grid_last_by_id(sid, 100, 100, 1, 0).unwrap();
    assert!(exact.is_empty(), "(t-0, t] is empty by half-open contract");
    let one = engine.query_grid_last_by_id(sid, 100, 100, 1, 1).unwrap();
    assert_eq!(one, vec![(100, 1.0)]);
    engine.shutdown().unwrap();
}
