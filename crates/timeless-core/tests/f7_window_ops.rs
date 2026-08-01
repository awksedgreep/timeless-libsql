//! F7: the window vocabulary vs naive evaluators implementing the
//! PINNED definitions from FEATURE_PLAN.md F7's semantic-line section,
//! quoted here so a drift in either place fails loudly:
//!
//!   delta    = last − first (engine-order ties)
//!   increase = Σ over consecutive pairs of (v[i] − v[i−1]) if
//!              v[i] ≥ v[i−1] else v[i]; first sample contributes
//!              nothing; NO extrapolation
//!   rate     = increase ÷ window (native ts units)
//!   pNN      = nearest-rank: exclude NaNs, sort by total_cmp, index
//!              ceil(N/100 × n) − 1; empty after exclusion → no row
//!   tavg:N   = drop floor(n × N/100) from EACH tail of the
//!              NaN-excluded sort, average remainder left-to-right;
//!              empty after trimming → no row

use std::collections::HashMap;

use timeless_core::{
    Engine, MemSpanStore, SpanBlockEngine, SpanEngineConfig, SpanEntry, SpanQuery, WindowOp,
};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
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

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("timeless_f7_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn naive_op(win: &[(i64, f64)], window: i64, op: WindowOp) -> Option<f64> {
    match op {
        WindowOp::Agg(_) => unreachable!("classic aggs covered by q2_kernels"),
        WindowOp::Delta => Some(win.last().unwrap().1 - win.first().unwrap().1),
        WindowOp::Increase => Some(naive_increase(win)),
        WindowOp::Rate => Some(naive_increase(win) / window as f64),
        WindowOp::Percentile(q) => {
            let s = sorted_nanless(win);
            if s.is_empty() {
                return None;
            }
            let rank = ((q / 100.0) * s.len() as f64).ceil() as usize;
            Some(s[rank.clamp(1, s.len()) - 1])
        }
        WindowOp::TrimmedMean(q) => {
            let s = sorted_nanless(win);
            let k = ((s.len() as f64) * (q / 100.0)).floor() as usize;
            if s.is_empty() || 2 * k >= s.len() {
                return None;
            }
            let kept = &s[k..s.len() - k];
            Some(kept.iter().fold(0.0f64, |a, &v| a + v) / kept.len() as f64)
        }
    }
}

fn naive_increase(win: &[(i64, f64)]) -> f64 {
    let mut acc = 0.0;
    for i in 1..win.len() {
        let (p, c) = (win[i - 1].1, win[i].1);
        acc += if c >= p { c - p } else { c };
    }
    acc
}

fn sorted_nanless(win: &[(i64, f64)]) -> Vec<f64> {
    let mut v: Vec<f64> = win
        .iter()
        .map(|&(_, x)| x)
        .filter(|x| !x.is_nan())
        .collect();
    v.sort_unstable_by(f64::total_cmp);
    v
}

#[test]
fn window_ops_match_naive() {
    let engine = Engine::new(temp_dir("ops"), 100_000, 0, 3, 64 << 20, false).unwrap();
    let mut rng = Rng(0xF7F7_0001);
    let labels: HashMap<String, String> = HashMap::new();
    let sid = engine.resolve_cached("counter", &labels).unwrap();

    // Counter-shaped data WITH resets and NaN staleness markers,
    // duplicate timestamps included; half flushed, half buffered.
    let mut ts = 1_000i64;
    let mut v = 0.0f64;
    let mut all: Vec<(i64, f64)> = Vec::new();
    for i in 0..800 {
        ts += rng.below(20) as i64; // dup ts possible
        v += rng.below(50) as f64;
        let sample = match i % 97 {
            13 => {
                v = rng.below(10) as f64; // counter reset
                v
            }
            41 => f64::NAN, // staleness marker
            _ => v,
        };
        engine.write_point(sid, ts, sample);
        all.push((ts, sample));
        if i == 400 {
            engine.flush_all().unwrap();
        }
    }

    let ops = [
        WindowOp::Delta,
        WindowOp::Increase,
        WindowOp::Rate,
        WindowOp::Percentile(50.0),
        WindowOp::Percentile(95.0),
        WindowOp::Percentile(99.9),
        WindowOp::TrimmedMean(0.0),
        WindowOp::TrimmedMean(5.0),
        WindowOp::TrimmedMean(49.9),
    ];
    for round in 0..25 {
        let start = 1_000 + (round * 311) % 4_000;
        let stop = start + 300 + (round * 137) % 3_000;
        let step = 1 + (round * 61) % 240;
        let window = 1 + (round * 97) % 500;
        // Engine-ordered samples for the naive side.
        let sorted = engine.query_range_by_id(sid, i64::MIN, i64::MAX).unwrap();
        for op in ops {
            let kernel = engine
                .query_window_op_by_id(sid, start, stop, step, window, op)
                .unwrap();
            // Naive: walk the grid, slice the window, apply the pinned
            // definition.
            let mut naive: Vec<(i64, f64)> = Vec::new();
            let mut t = start;
            while t <= stop {
                let win: Vec<(i64, f64)> = sorted
                    .iter()
                    .copied()
                    .filter(|&(s, _)| s <= t && (s as i128) > (t as i128 - window as i128))
                    .collect();
                if !win.is_empty() {
                    if let Some(val) = naive_op(&win, window, op) {
                        naive.push((t, val));
                    }
                }
                t += step;
            }
            assert_eq!(kernel.len(), naive.len(), "round {round} {op:?}: row count");
            for (k, n) in kernel.iter().zip(&naive) {
                assert_eq!(k.0, n.0, "round {round} {op:?}: grid ts");
                assert_eq!(
                    k.1.to_bits(),
                    n.1.to_bits(),
                    "round {round} {op:?} @ {}: {} vs {}",
                    k.0,
                    k.1,
                    n.1
                );
            }
        }
    }
    engine.shutdown().unwrap();
}

#[test]
fn window_op_edges() {
    let engine = Engine::new(temp_dir("edges"), 100_000, 0, 3, 64 << 20, false).unwrap();
    let labels: HashMap<String, String> = HashMap::new();
    let sid = engine.resolve_cached("m", &labels).unwrap();
    // One window: [10.0, NaN, 30.0, 20.0] at ts 1..4.
    for (ts, v) in [(1, 10.0), (2, f64::NAN), (3, 30.0), (4, 20.0)] {
        engine.write_point(sid, ts, v);
    }
    let one = |op| engine.query_window_op_by_id(sid, 4, 4, 1, 10, op).unwrap();
    // p50 over NaN-excluded sort [10, 20, 30]: rank ceil(1.5)=2 → 20.
    assert_eq!(one(WindowOp::Percentile(50.0)), vec![(4, 20.0)]);
    // p99.9 on 3 values → rank 3 → 30. p1 → rank 1 → 10.
    assert_eq!(one(WindowOp::Percentile(99.9)), vec![(4, 30.0)]);
    assert_eq!(one(WindowOp::Percentile(1.0)), vec![(4, 10.0)]);
    // tavg:34 on n=3 → k=1 → keep [20] → 20.
    assert_eq!(one(WindowOp::TrimmedMean(34.0)), vec![(4, 20.0)]);
    // delta = 20 − 10; increase: NaN poisons its two steps (NaN
    // comparisons are false → the reset arm adds the current value):
    // pairs (10,NaN)→NaN? No: NaN >= 10 is false → += NaN. Documented
    // reality: NaN in a counter window poisons increase — staleness
    // handling stays above the waist. Assert the poison honestly.
    assert_eq!(one(WindowOp::Delta), vec![(4, 10.0)]);
    let inc = one(WindowOp::Increase);
    assert!(inc[0].1.is_nan(), "NaN poisons increase, documented");

    // All-NaN window → percentile/tavg emit no row.
    let sid2 = engine.resolve_cached("allnan", &labels).unwrap();
    engine.write_point(sid2, 1, f64::NAN);
    assert!(engine
        .query_window_op_by_id(sid2, 1, 1, 1, 10, WindowOp::Percentile(95.0))
        .unwrap()
        .is_empty());

    // Validation: out-of-range parameters are errors.
    assert!(engine
        .query_window_op_by_id(sid, 1, 4, 1, 10, WindowOp::Percentile(0.0))
        .is_err());
    assert!(engine
        .query_window_op_by_id(sid, 1, 4, 1, 10, WindowOp::Percentile(100.1))
        .is_err());
    assert!(engine
        .query_window_op_by_id(sid, 1, 4, 1, 10, WindowOp::TrimmedMean(50.0))
        .is_err());
    engine.shutdown().unwrap();
}

/// Trace duration percentiles: exact nearest-rank vs naive over the
/// bucket's sorted i64 durations.
#[test]
fn trace_duration_percentiles_match_naive() {
    let engine = SpanBlockEngine::new(
        Box::new(MemSpanStore::new()),
        SpanEngineConfig {
            flush_threshold: 1_000_000,
            ..Default::default()
        },
    )
    .unwrap();
    let mut rng = Rng(0xF7F7_0002);
    let mut all: Vec<SpanEntry> = Vec::new();
    for i in 0..500 {
        let e = SpanEntry {
            trace_id: [(i % 200) as u8; 16],
            span_id: [(i % 100) as u8; 8],
            parent_span_id: None,
            name: "op".into(),
            service: format!("svc{}", rng.below(2)),
            kind: 1,
            status: 1,
            start_ts: 1_000 + rng.below(10_000) as i64,
            duration_ns: rng.below(1_000_000) as i64,
            attributes: vec![],
        };
        all.push(e.clone());
        engine.push(e).unwrap();
        if i == 250 {
            engine.flush().unwrap();
        }
    }
    let q = SpanQuery {
        ts_min: 1_000,
        ts_max: 11_000,
        trace_id: None,
        service: None,
        kind: None,
        status: None,
        name: None,
    };
    for step in [500i64, 2_000, 100_000] {
        let stats = engine.bucket_stats(&q, step).unwrap();
        assert!(!stats.is_empty());
        for b in &stats {
            let mut durs: Vec<i64> = all
                .iter()
                .filter(|s| {
                    s.service == b.service
                        && s.start_ts >= 1_000
                        && s.start_ts <= 11_000
                        && 1_000 + ((s.start_ts - 1_000) / step) * step == b.bucket_ts
                })
                .map(|s| s.duration_ns)
                .collect();
            durs.sort_unstable();
            let rank = |p: f64| {
                durs[((p / 100.0 * durs.len() as f64).ceil() as usize).clamp(1, durs.len()) - 1]
            };
            assert_eq!(b.dur_p50, rank(50.0), "p50 @ {} {}", b.bucket_ts, b.service);
            assert_eq!(b.dur_p95, rank(95.0), "p95 @ {} {}", b.bucket_ts, b.service);
            assert_eq!(b.dur_p99, rank(99.0), "p99 @ {} {}", b.bucket_ts, b.service);
        }
    }
}
