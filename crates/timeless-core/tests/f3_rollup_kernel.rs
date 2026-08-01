//! F3: the rollup bucket kernel must agree BIT FOR BIT with an
//! independent naive bucket computation over the same samples
//! (FEATURE_PLAN.md — the amended invariant-6 contract for
//! pre-aggregation).

use timeless_core::{rollup_buckets, RollupBucket};

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

/// Naive reference: recompute each bucket from scratch by filtering the
/// whole sample slice — different code shape, same defined semantics.
fn naive_buckets(samples: &[(i64, f64)], resolution: i64) -> Vec<RollupBucket> {
    let mut starts: Vec<i64> = samples
        .iter()
        .map(|&(ts, _)| ts.div_euclid(resolution) * resolution)
        .collect();
    starts.sort();
    starts.dedup();
    starts
        .into_iter()
        .map(|b| {
            let members: Vec<(i64, f64)> = samples
                .iter()
                .copied()
                .filter(|&(ts, _)| ts.div_euclid(resolution) * resolution == b)
                .collect();
            let sum = members.iter().fold(0.0f64, |a, &(_, v)| a + v);
            let min = members[1..]
                .iter()
                .fold(members[0].1, |a, &(_, v)| f64::min(a, v));
            let max = members[1..]
                .iter()
                .fold(members[0].1, |a, &(_, v)| f64::max(a, v));
            // last = max ts, later element wins ties
            let mut last = members[0];
            for &m in &members[1..] {
                if m.0 >= last.0 {
                    last = m;
                }
            }
            RollupBucket {
                bucket_ts: b,
                count: members.len() as u64,
                sum,
                min,
                max,
                last_ts: last.0,
                last_val: last.1,
            }
        })
        .collect()
}

fn assert_buckets_bit_eq(kernel: &[RollupBucket], naive: &[RollupBucket], what: &str) {
    assert_eq!(kernel.len(), naive.len(), "{what}: bucket count");
    for (k, n) in kernel.iter().zip(naive) {
        assert_eq!(k.bucket_ts, n.bucket_ts, "{what}: bucket_ts");
        assert_eq!(k.count, n.count, "{what}: count @ {}", k.bucket_ts);
        assert_eq!(
            k.sum.to_bits(),
            n.sum.to_bits(),
            "{what}: sum @ {}",
            k.bucket_ts
        );
        assert_eq!(
            k.min.to_bits(),
            n.min.to_bits(),
            "{what}: min @ {}",
            k.bucket_ts
        );
        assert_eq!(
            k.max.to_bits(),
            n.max.to_bits(),
            "{what}: max @ {}",
            k.bucket_ts
        );
        assert_eq!(k.last_ts, n.last_ts, "{what}: last_ts @ {}", k.bucket_ts);
        assert_eq!(
            k.last_val.to_bits(),
            n.last_val.to_bits(),
            "{what}: last_val @ {}",
            k.bucket_ts
        );
    }
}

#[test]
fn kernel_matches_naive_randomized() {
    let mut rng = Rng(0xF3F3_F3F3_1234_5678);
    for round in 0..200 {
        let resolution = 1 + rng.below(500) as i64;
        let n = rng.below(400) as usize;
        // Ascending jittered ts starting possibly NEGATIVE (Euclidean
        // floor coverage), with duplicates.
        let mut ts = -(rng.below(2000) as i64);
        let mut samples = Vec::with_capacity(n);
        for _ in 0..n {
            ts += rng.below(30) as i64; // 0 step → duplicate ts
            let val = (rng.next() as f64) / (u64::MAX as f64) * 2000.0 - 1000.0;
            samples.push((ts, val));
        }
        let kernel = rollup_buckets(&samples, resolution);
        let naive = naive_buckets(&samples, resolution);
        assert_buckets_bit_eq(&kernel, &naive, &format!("round {round} res {resolution}"));
    }
}

#[test]
fn kernel_edges() {
    // Empty input → no buckets.
    assert!(rollup_buckets(&[], 60).is_empty());

    // Samples exactly ON bucket boundaries belong to the bucket they
    // start: [B, B+R).
    let buckets = rollup_buckets(&[(0, 1.0), (59, 2.0), (60, 3.0)], 60);
    assert_eq!(buckets.len(), 2);
    assert_eq!((buckets[0].bucket_ts, buckets[0].count), (0, 2));
    assert_eq!((buckets[1].bucket_ts, buckets[1].count), (60, 1));

    // Negative ts: Euclidean floor, so -1 lands in bucket -60, not 0.
    let buckets = rollup_buckets(&[(-1, 5.0), (-60, 6.0), (-61, 7.0)], 60);
    // Input must be ascending for the kernel; re-sort as the engine does.
    let mut samples = vec![(-61i64, 7.0f64), (-60, 6.0), (-1, 5.0)];
    samples.sort_by_key(|&(ts, _)| ts);
    let buckets2 = rollup_buckets(&samples, 60);
    assert_eq!(buckets2.len(), 2);
    assert_eq!((buckets2[0].bucket_ts, buckets2[0].count), (-120, 1)); // -61
    assert_eq!((buckets2[1].bucket_ts, buckets2[1].count), (-60, 2)); // -60, -1
    let _ = buckets;

    // Duplicate max-ts: the LATER element wins last_val (engine order).
    let buckets = rollup_buckets(&[(10, 1.0), (10, 2.0)], 60);
    assert_eq!(buckets[0].last_val, 2.0);
    assert_eq!(buckets[0].count, 2);
}
