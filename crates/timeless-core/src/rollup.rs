//! F3 rollup ladder (FEATURE_PLAN.md): bucket aggregation kernel and the
//! rollup chunk payload codec.
//!
//! ── THE BUCKET CONTRACT (the whole semantic surface — verbatim in the
//! docs, pinned by property tests) ─────────────────────────────────────
//! For resolution R > 0, bucket B covers `[B, B + R)` with
//! `B = ts.div_euclid(R) * R` (Euclidean floor, correct for negative
//! ts). Per bucket per series:
//!   - count:    samples in the bucket (u64)
//!   - sum:      f64 fold, LEFT-TO-RIGHT in ascending engine ts order
//!   - min/max:  f64::min / f64::max folds, seeded by the first sample
//!   - last_ts/last_val: the max-ts sample; ties resolved by engine
//!     order (the LATER element wins, same rule as the Q2 grid kernel)
//!     `avg` is sum/count computed at READ time, never stored.
//!
//! This is PRE-AGGREGATION: bit-parity with raw-sample folds across
//! arbitrary query windows is impossible for float sums and is NOT
//! claimed. What IS claimed — and property-tested — is that every
//! stored bucket equals the naive bucket computation over the raw
//! samples, bit for bit.
//!
//! ── PAYLOAD (ENC_ROLLUP_V1) ──────────────────────────────────────────
//! Column-major, all little-endian, zstd-compressed as one frame:
//!   u32 n_buckets, then n × i64 bucket_ts, n × u64 count, n × f64 sum,
//!   n × f64 min, n × f64 max, n × i64 last_ts, n × f64 last_val.
//! Values round-trip by BITS (NaN payloads survive). Dumb on purpose —
//! pco per-column is a measured optimization for later, not a v1 risk.

/// Chunk encoding id for rollup payloads (ENC_PCO = 0, ENC_RAW = 1).
pub const ENC_ROLLUP_V1: u8 = 2;

/// zstd level for rollup payloads: cheap, they're written at rollup
/// cadence, not ingest cadence.
const ROLLUP_ZSTD_LEVEL: i32 = 7;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RollupBucket {
    /// Bucket start B; the bucket covers [B, B + resolution).
    pub bucket_ts: i64,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub last_ts: i64,
    pub last_val: f64,
}

/// The kernel: fold ts-ascending samples into buckets. Pure; the caller
/// validates resolution > 0 and supplies samples in engine order (the
/// order query_range_by_id returns). Output buckets are ascending and
/// contain at least one sample each (empty buckets do not exist).
pub fn rollup_buckets(samples: &[(i64, f64)], resolution: i64) -> Vec<RollupBucket> {
    debug_assert!(resolution > 0, "caller validates resolution");
    let mut out: Vec<RollupBucket> = Vec::new();
    for &(ts, val) in samples {
        let bucket_ts = ts.div_euclid(resolution).wrapping_mul(resolution);
        match out.last_mut() {
            Some(b) if b.bucket_ts == bucket_ts => {
                b.count += 1;
                b.sum += val;
                b.min = f64::min(b.min, val);
                b.max = f64::max(b.max, val);
                if ts >= b.last_ts {
                    b.last_ts = ts;
                    b.last_val = val;
                }
            }
            _ => out.push(RollupBucket {
                bucket_ts,
                count: 1,
                sum: val,
                min: val,
                max: val,
                last_ts: ts,
                last_val: val,
            }),
        }
    }
    out
}

pub fn encode_rollup_payload(buckets: &[RollupBucket]) -> Result<Vec<u8>, String> {
    let n = buckets.len();
    let mut raw = Vec::with_capacity(4 + n * 56);
    raw.extend_from_slice(&(n as u32).to_le_bytes());
    for b in buckets {
        raw.extend_from_slice(&b.bucket_ts.to_le_bytes());
    }
    for b in buckets {
        raw.extend_from_slice(&b.count.to_le_bytes());
    }
    for b in buckets {
        raw.extend_from_slice(&b.sum.to_le_bytes());
    }
    for b in buckets {
        raw.extend_from_slice(&b.min.to_le_bytes());
    }
    for b in buckets {
        raw.extend_from_slice(&b.max.to_le_bytes());
    }
    for b in buckets {
        raw.extend_from_slice(&b.last_ts.to_le_bytes());
    }
    for b in buckets {
        raw.extend_from_slice(&b.last_val.to_le_bytes());
    }
    zstd::bulk::compress(&raw, ROLLUP_ZSTD_LEVEL)
        .map_err(|e| format!("rollup payload compression failed: {e}"))
}

pub fn decode_rollup_payload(payload: &[u8]) -> Result<Vec<RollupBucket>, String> {
    // 64 MiB cap: a corrupt header must not allocate the moon.
    let raw = zstd::bulk::decompress(payload, 64 << 20)
        .map_err(|e| format!("rollup payload decompression failed: {e}"))?;
    if raw.len() < 4 {
        return Err("rollup payload truncated (no header)".into());
    }
    let n = u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
    let expected = 4 + n * 56;
    if raw.len() != expected {
        return Err(format!(
            "rollup payload is {} byte(s); {n} buckets require {expected}",
            raw.len()
        ));
    }
    let col = |idx: usize| 4 + n * 8 * idx;
    let i64_at = |base: usize, i: usize| {
        i64::from_le_bytes(raw[base + i * 8..base + i * 8 + 8].try_into().unwrap())
    };
    let u64_at = |base: usize, i: usize| {
        u64::from_le_bytes(raw[base + i * 8..base + i * 8 + 8].try_into().unwrap())
    };
    let f64_at = |base: usize, i: usize| {
        f64::from_le_bytes(raw[base + i * 8..base + i * 8 + 8].try_into().unwrap())
    };
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(RollupBucket {
            bucket_ts: i64_at(col(0), i),
            count: u64_at(col(1), i),
            sum: f64_at(col(2), i),
            min: f64_at(col(3), i),
            max: f64_at(col(4), i),
            last_ts: i64_at(col(5), i),
            last_val: f64_at(col(6), i),
        });
    }
    Ok(out)
}

/// One tier of the declared ladder: keep `resolution`-bucket rollups for
/// `retention` native ts units (0 = keep forever).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RollupTier {
    pub resolution: i64,
    pub retention: i64,
}

/// Parse the persisted ladder spec: "300:2592000,3600:0" — pairs of
/// resolution:retention in NATIVE units, ascending resolutions, each a
/// multiple of the previous (so coarser tiers could roll from finer
/// ones), retention 0 = forever. The SQL-facing duration syntax is the
/// vtab's job; this is the storage form.
pub fn parse_ladder(spec: &str) -> Result<Vec<RollupTier>, String> {
    let mut tiers: Vec<RollupTier> = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (res, ret) = part
            .split_once(':')
            .ok_or_else(|| format!("rollup tier {part:?}: expected resolution:retention"))?;
        let resolution: i64 = res
            .trim()
            .parse()
            .map_err(|_| format!("rollup tier {part:?}: bad resolution"))?;
        let retention: i64 = ret
            .trim()
            .parse()
            .map_err(|_| format!("rollup tier {part:?}: bad retention"))?;
        if resolution <= 0 {
            return Err(format!(
                "rollup resolution must be positive, got {resolution}"
            ));
        }
        if retention < 0 {
            return Err(format!("rollup retention must be >= 0, got {retention}"));
        }
        if let Some(prev) = tiers.last() {
            if resolution <= prev.resolution || resolution % prev.resolution != 0 {
                return Err(format!(
                    "rollup resolutions must ascend and each be a multiple of the previous \
                     ({} then {resolution})",
                    prev.resolution
                ));
            }
        }
        tiers.push(RollupTier {
            resolution,
            retention,
        });
    }
    Ok(tiers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_round_trips_bit_exact() {
        let buckets = vec![
            RollupBucket {
                bucket_ts: -300,
                count: 3,
                sum: f64::NAN, // NaN bits must survive
                min: -1.5,
                max: 2.5,
                last_ts: -101,
                last_val: 0.25,
            },
            RollupBucket {
                bucket_ts: 0,
                count: 1,
                sum: 42.0,
                min: 42.0,
                max: 42.0,
                last_ts: 7,
                last_val: 42.0,
            },
        ];
        let decoded = decode_rollup_payload(&encode_rollup_payload(&buckets).unwrap()).unwrap();
        assert_eq!(decoded.len(), buckets.len());
        for (d, b) in decoded.iter().zip(&buckets) {
            assert_eq!(d.bucket_ts, b.bucket_ts);
            assert_eq!(d.count, b.count);
            assert_eq!(d.sum.to_bits(), b.sum.to_bits());
            assert_eq!(d.min.to_bits(), b.min.to_bits());
            assert_eq!(d.max.to_bits(), b.max.to_bits());
            assert_eq!(d.last_ts, b.last_ts);
            assert_eq!(d.last_val.to_bits(), b.last_val.to_bits());
        }
    }

    #[test]
    fn ladder_parsing() {
        assert_eq!(
            parse_ladder("300:100,3600:0").unwrap(),
            vec![
                RollupTier {
                    resolution: 300,
                    retention: 100
                },
                RollupTier {
                    resolution: 3600,
                    retention: 0
                },
            ]
        );
        assert!(parse_ladder("0:5").is_err());
        assert!(parse_ladder("300:-1").is_err());
        assert!(parse_ladder("300:0,500:0").is_err()); // not a multiple
        assert!(parse_ladder("3600:0,300:0").is_err()); // not ascending
        assert!(parse_ladder("nope").is_err());
    }
}
