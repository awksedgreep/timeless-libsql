//! F4: log/trace bucket kernels vs independent naive binning
//! (FEATURE_PLAN.md). Counts and integer stats compare EXACTLY.

use timeless_core::{
    BlockEngine, BlockEngineConfig, LogEntry, LogQuery, MemBlockStore, MemSpanStore,
    SpanBlockEngine, SpanEngineConfig, SpanEntry, SpanQuery,
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

fn log_engine() -> BlockEngine {
    BlockEngine::new(
        Box::new(MemBlockStore::new()),
        BlockEngineConfig {
            flush_threshold: 1_000_000,
            index_keys: vec!["service".into()],
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn log_bucket_counts_match_naive() {
    let engine = log_engine();
    let mut rng = Rng(0xF4F4_0001);
    let mut all: Vec<LogEntry> = Vec::new();
    for i in 0..800 {
        let e = LogEntry {
            ts: 10_000 + rng.below(5_000) as i64,
            level: (rng.below(4)) as u8,
            message: format!("m{i}"),
            metadata: if i % 5 == 0 {
                vec![] // no service key → group ""
            } else {
                vec![("service".into(), format!("svc{}", rng.below(3)))]
            },
        };
        all.push(e.clone());
        engine.push(e).unwrap();
        if i == 400 {
            engine.flush().unwrap(); // half flushed, half buffered
        }
    }

    for round in 0..30 {
        let start = 10_000 + (round * 137) % 3_000;
        let stop = start + 500 + (round * 61) % 2_000;
        let step = 1 + (round * 97) % 400;
        for group_by in ["level", "service"] {
            let q = LogQuery {
                ts_min: start,
                ts_max: stop,
                level: None,
                metadata_eq: vec![],
                message_contains: None,
                message_like_prune: None,
            };
            let kernel = engine.bucket_counts(&q, group_by, step).unwrap();
            // Naive: filter + bin independently.
            let mut naive: std::collections::BTreeMap<(i64, String), u64> = Default::default();
            for e in &all {
                if e.ts < start || e.ts > stop {
                    continue;
                }
                let b = start + ((e.ts - start) / step) * step;
                let g = if group_by == "level" {
                    timeless_core::level_name(e.level).to_string()
                } else {
                    e.meta_value("service").unwrap_or("").to_string()
                };
                *naive.entry((b, g)).or_insert(0) += 1;
            }
            let naive: Vec<(i64, String, u64)> =
                naive.into_iter().map(|((b, g), n)| (b, g, n)).collect();
            assert_eq!(
                kernel, naive,
                "round {round} group_by {group_by} step {step}"
            );
        }
    }

    // Edges: step larger than range = one bucket; exact-boundary entry.
    let q = LogQuery {
        ts_min: 10_000,
        ts_max: 14_999,
        level: None,
        metadata_eq: vec![],
        message_contains: None,
        message_like_prune: None,
    };
    let one = engine.bucket_counts(&q, "level", 1_000_000).unwrap();
    assert!(one.iter().all(|(b, _, _)| *b == 10_000), "single bucket");
    let total: u64 = one.iter().map(|(_, _, n)| n).sum();
    assert_eq!(total, all.len() as u64);

    // Errors.
    assert!(engine.bucket_counts(&q, "level", 0).is_err());
    assert!(engine.bucket_counts(&q, "nope", 10).is_err());
    let wide = LogQuery {
        ts_min: 0,
        ts_max: i64::MAX - 1,
        level: None,
        metadata_eq: vec![],
        message_contains: None,
        message_like_prune: None,
    };
    assert!(
        engine.bucket_counts(&wide, "level", 1).is_err(),
        "bucket cap"
    );
}

#[test]
fn trace_bucket_stats_match_naive() {
    let engine = SpanBlockEngine::new(
        Box::new(MemSpanStore::new()),
        SpanEngineConfig {
            flush_threshold: 1_000_000,
            ..Default::default()
        },
    )
    .unwrap();
    let mut rng = Rng(0xF4F4_0002);
    let mut all: Vec<SpanEntry> = Vec::new();
    for i in 0..600 {
        let e = SpanEntry {
            trace_id: [(i % 250) as u8; 16],
            span_id: [(i % 200) as u8; 8],
            parent_span_id: None,
            name: format!("op{}", i % 4),
            service: format!("svc{}", rng.below(3)),
            kind: 1,
            status: (rng.below(3)) as u8,
            start_ts: 1_000_000 + rng.below(900_000) as i64,
            duration_ns: rng.below(5_000_000) as i64,
            attributes: vec![],
        };
        all.push(e.clone());
        engine.push(e).unwrap();
        if i == 300 {
            engine.flush().unwrap();
        }
    }

    for round in 0..30 {
        let start = 1_000_000 + (round * 7_919) % 500_000;
        let stop = start + 100_000 + (round * 13_337) % 300_000;
        let step = 1_000 + (round * 977) % 90_000;
        let q = SpanQuery {
            ts_min: start,
            ts_max: stop,
            trace_id: None,
            service: None,
            kind: None,
            status: None,
            name: None,
        };
        let kernel = engine.bucket_stats(&q, step).unwrap();
        let mut naive: std::collections::BTreeMap<(i64, String), (u64, u64, i64, i64, i64)> =
            Default::default();
        for s in &all {
            if s.start_ts < start || s.start_ts > stop {
                continue;
            }
            let b = start + ((s.start_ts - start) / step) * step;
            let e = naive
                .entry((b, s.service.clone()))
                .or_insert((0, 0, 0, i64::MAX, i64::MIN));
            e.0 += 1;
            if s.status == 2 {
                e.1 += 1;
            }
            e.2 += s.duration_ns;
            e.3 = e.3.min(s.duration_ns);
            e.4 = e.4.max(s.duration_ns);
        }
        assert_eq!(kernel.len(), naive.len(), "round {round}");
        for k in &kernel {
            let n = &naive[&(k.bucket_ts, k.service.clone())];
            assert_eq!(
                (k.spans, k.errors, k.dur_sum, k.dur_min, k.dur_max),
                *n,
                "round {round} bucket {} svc {}",
                k.bucket_ts,
                k.service
            );
        }
    }

    // Service filter narrows to that service only.
    let q = SpanQuery {
        ts_min: 1_000_000,
        ts_max: 2_000_000,
        trace_id: None,
        service: Some("svc1".into()),
        kind: None,
        status: None,
        name: None,
    };
    let filtered = engine.bucket_stats(&q, 100_000).unwrap();
    assert!(!filtered.is_empty());
    assert!(filtered.iter().all(|b| b.service == "svc1"));
}
