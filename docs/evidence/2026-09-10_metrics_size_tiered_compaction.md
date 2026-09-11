# Metrics size-tiered compaction evidence — 2026-09-10

Issue: [#52](https://github.com/awksedgreep/timeless-libsql/issues/52)

## Method

The release harness `metrics_compaction_bench` prebuilt one fixed metrics batch
shape in its driver, then spawned a fresh worker using the real metrics
`Storage` path and release extension. Each round shifted the batch to the next
non-overlapping timestamp window before the ingest clock, submitted 320
production-shaped labeled series with 1,024 points per series (327,680 points),
flushed, waited for the entire scheduled compact sweep, and only then submitted
the next append. Ingest/flush clocks are separate from the active maintenance
clock.

```text
servers/target/release/metrics_compaction_bench \
  target/release/libtimeless_ext.so \
  /tmp/timeless-metrics-compaction-issue52-20260910.db 32
```

- Platform: `Linux 7.2.3-arch1-3 x86_64 GNU/Linux`
- Extension SHA-256:
  `5fb575a838b43d329f66d8b22ddd03bbc8c26ca6ad3bc913b4ba4145df61a8ae`
- Harness SHA-256:
  `17f4cf2c9ccf68adb23d1ef5b792664558e120e99a9ad36901df29f70e2de26c`
- Total durable points: 10,485,760; buffered/queued points at completion: 0.

## Curve

| Round/tier | Active sweep | Transactions | Raw input → output | Compressed merge input → output | RSS / HWM | Chunks after |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 982 ms | 5 | 5,242,880 → 981,910 B | none | 29,052 / 42,380 KiB | 320 |
| 10 | 985 ms | 5 | 5,242,880 → 981,910 B | none | 76,888 / 76,888 KiB | 3,200 |
| 15 | 1,070 ms | 5 | 5,242,880 → 981,910 B | none | 80,104 / 80,104 KiB | 4,800 |
| 16 (16K tier) | 2,139 ms | 25 | 5,242,880 → 981,910 B | 15,710,560 → 2,506,887 B (5,242,880 points) | 82,756 / 82,756 KiB | 320 |
| 17 | 1,061 ms | 5 | 5,242,880 → 981,910 B | none | 87,344 / 87,344 KiB | 640 |
| 31 | 1,038 ms | 5 | 5,242,880 → 981,910 B | none | 91,840 / 91,840 KiB | 5,120 |
| 32 (32K tier) | 3,032 ms | 45 | 5,242,880 → 981,910 B | 18,217,447 → 2,592,320 B (10,485,760 points) | 91,840 / 91,840 KiB | 320 |

All non-merge sweeps were 974–1,070 ms. Their five transactions each handled
64 series / 65,536 points / 1 MiB raw input, below all three configured
ceilings. The process-wide step-time high-water was 224.160 ms. Ingest was
12.3–18.5 ms and flush was 9.4–15.2 ms, recorded outside the maintenance
clock.

## Disposition

- First compression was invariant in every round: 327,680 raw points and
  5,242,880 bytes in, 981,910 bytes out. It never absorbed an existing
  compressed chunk.
- Compressed merges occurred only when a tier reached the half-full + 2x
  growth rule: rounds 16 and 32. No rounds between those boundaries rewrote a
  compressed tail.
- Cumulative compressed rewrite work was 15,728,640 points, exactly 1.5x the
  10,485,760 ingested points. Including mandatory first compression, total
  source-point work was 2.5x ingest. The old append-to-tail behavior grows
  quadratically under this fixture.
- Final logical payload was 2,592,320 bytes in 320 chunks. RSS rose while
  underfilled tiers accumulated, then stayed within 87,344–91,840 KiB from
  round 17 through 32 and did not track cumulative points or rewrite the
  terminal 16K tier on ordinary arrivals.
- Deterministic core contracts separately pin exact results after every
  arrival, the 262,144-point / 4 MiB transaction ceilings, cascading tiers,
  raw filesystem-batch reopen, reader publication, injected replacement
  failure, restart, late/backfilled data, and SQLite rollback.
