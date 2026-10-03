# Pre-perf-work baseline — 2026-10-03 (same host as July baselines)

Starting revision: `df6ceee` on `main` (all 8 correctness fixes #98–#105
landed, no perf work yet). clean tree. Purpose: regression reference for
perf issues #106–#115. Method follows TESTING.md and the RESULTS.md
checkpoints: release build, each binary run twice, second run quoted.

```sh
cargo build --release -p timeless-ext --locked
cargo build --release --manifest-path tools/bench/Cargo.toml --locked
EXT="$PWD/target/release/libtimeless_ext.so"
./tools/bench/target/release/bench "$EXT"            # x2
./tools/bench/target/release/bench-logs "$EXT"       # x2
./tools/bench/target/release/bench-traces "$EXT"     # x2
./tools/bench/target/release/bench-codec             # x2
./tools/bench/target/release/query-read "$EXT"       # x2
```

Environment:

- Arch Linux, x86-64, 93 GB RAM
- Intel Core Ultra 9 185H, 22 logical CPUs
- CPU governor: `powersave`
- Rust/Cargo 1.98.1 (July baselines used 1.97.0), release profile LTO + 1 codegen unit
- system SQLite 3.53.4 / bundled 3.53.2 (query-read reports 3.53.2)
- benchmark databases: `/tmp` on `tmpfs`, removed by default

## Metrics (`bench`, 1M points; run2 quoted, run1 in parens)

| workload | run2 | run1 |
|---|---:|---:|
| plain ingest | 3.62M pts/s | 3.58M pts/s |
| Tier 1 ingest | 1.37M pts/s | 1.46M pts/s |
| Tier 1 / plain | 0.378x | 0.408x |
| Tier 2 ingest | 16.55M pts/s | 17.23M pts/s |
| Tier 2 / plain | 4.57x | 4.81x |
| Tier 1 flush | 34.9 ms | 31.3 ms |
| Tier 2 flush | 34.0 ms | 28.9 ms |
| name + range query (10001 rows) | 4.7 ms | 3.9 ms |
| full-scan count (1M rows) | 235.9 ms | 221.8 ms |
| compressed size | 18.694 B/pt | 18.694 B/pt |
| grid TVF (16600 rows) | 3.3 ms | 3.0 ms |
| window avg TVF | 3.3 ms | 3.1 ms |
| window exact p95 TVF | 10.9 ms | 10.1 ms |
| rollup build (1000 chunks) | 115.4 ms | 105.0 ms |
| rollup tier read | 8.7 ms | 7.9 ms |

## Logs (`bench-logs`, 1M entries; run2 quoted, run1 in parens)

| workload | run2 | run1 |
|---|---:|---:|
| tier1 ingest | 0.29M entries/s | 0.29M entries/s |
| tier2 ingest | 0.46M entries/s | 0.45M entries/s |
| vtab flush | 0.7 ms | 0.8 ms |
| vtab optimize (small) | 30.4 ms | 29.2 ms |
| trigram optimize | 1228.5 ms | 1332.3 ms |
| level=error, cold / warm (49857 rows) | 48.0 / 4.9 ms | 48.3 / 4.8 ms |
| service+level+range, cold / pushdown (1704 rows) | 140.5 / 7.3 ms | 143.7 / 7.5 ms |
| LIKE %timeout%, cold / indexed (58479 rows) | 92.1 / 28.5 ms | 95.0 / 28.5 ms |
| count(*) after reopen (1M rows) | 61.0 ms | 60.5 ms |
| log_buckets (200 rows) | 229.2 ms | 218.9 ms |
| storage | 9.10 B/entry | 9.10 B/entry |

## Traces (`bench-traces`, ~960k spans; run2 quoted, run1 in parens)

| workload | run2 | run1 |
|---|---:|---:|
| tier1 ingest | 0.16M spans/s | 0.16M spans/s |
| batch-v0 ingest | 0.25M spans/s | 0.24M spans/s |
| batch-v1 rich ingest | 0.19M spans/s | 0.18M spans/s |
| batch-v0 flush / optimize | 113.0 / 1003.2 ms | 123.5 / 993.0 ms |
| batch-v1 flush / optimize | 122.5 / 1173.2 ms | 126.3 / 1193.3 ms |
| point lookup, cold / warm (936 spans) | 0.005 / 0.309 ms | 0.005 / 0.305 ms |
| status=error, cold / warm (10220 rows) | 58.9 / 1.3 ms | 58.2 / 1.3 ms |
| service+range, cold / pushdown (32072 rows) | 61.4 / 30.0 ms | 60.8 / 29.9 ms |
| trace_buckets (500 rows) | 223.6 ms | 228.4 ms |
| storage | 34.62 B/span | 34.61 B/span |

## Codec (`bench-codec`; run2)

| dataset | codec5 encode | codec5 decode | size vs codec4 |
|---|---|---:|---:|
| logs (110.30 MB raw) | 110 MB/s (0.99M e/s) | 538 MB/s (4.88M e/s) | -8.1% |
| traces (176.04 MB raw) | 140 MB/s (0.76M e/s) | 477 MB/s (2.60M e/s) | +0.0% |

## Query-read (`query-read`, 12000 series x 60 pts; run2 median_us)

Full CSVs: `/tmp/baseline_20261003/query_read_run{1,2}.txt` (retained on
this host only). Hot medians: `scalar_aggregate_native` ~14.6ms vs
fallback ~42.7ms; `latest_native` ~15.2ms vs fallback ~43.1ms;
`latest_frame` ~3.3ms; `grid_count` ~39.5ms; `rollup_avg_count` ~138ms.

## Vs July R1-R8 checkpoint (same host/governor, indicative only)

Two months of engine work plus Rust 1.97.0 -> 1.98.1 sit between these
numbers, and some workloads changed shape, so this is not an A/B:

| workload | R1-R8 (Jul) | now (run2) | note |
|---|---:|---:|---|
| metrics T1 / plain | 0.460x | 0.378x | normalized ingest down; needs A/B to attribute |
| metrics T2 / plain | 3.792x | 4.57x | up |
| metrics T2 flush | 210.7 ms | 34.0 ms | workload shape changed; not comparable |
| metrics name+range | 5.6 ms | 4.7 ms | same direction |
| metrics full-scan | 201.5 ms | 235.9 ms | slower; needs A/B |
| metrics B/pt | 8.344 | 18.694 | storage model changed since July |
| logs ingest | 0.81M e/s | 0.29M (t1) / 0.46M (t2) | workload shape changed; not comparable |
| logs B/entry | 8.93 | 9.10 | close |
| traces ingest | 0.55M s/s | 0.16M (t1) / 0.25M (v0) | workload shape changed; not comparable |
| traces B/span | 37.36 | 34.62 | close |
| traces point lookup | 3.637 ms | 0.309 ms warm | workload shape changed; not comparable |

Raw outputs: `/tmp/baseline_20261003/` (`bench_run{1,2}.txt`,
`bench_logs_run{1,2}.txt`, `bench_traces_run{1,2}.txt`,
`bench_codec_run2.txt`, `query_read_run{1,2}.txt`).

## Retest: #111 flatjson byte cursor (rev 6c9def2, same protocol)

`bench` double run, second quoted. Host ran slower in absolute terms
(accounting load), so the normalized ratio is the signal:

| metric | baseline r1 | baseline r2 | #111 r1 | #111 r2 |
|---|---:|---:|---:|---:|
| plain | 3.58M | 3.62M | 3.44M | 3.21M |
| tier1 | 1.46M | 1.37M | 1.73M | 1.61M |
| **tier1 / plain** | **0.408x** | **0.378x** | **0.503x** | **0.502x** |
| tier2 | 17.23M | 16.55M | 16.69M | 18.46M |
| tier2 / plain | 4.81x | 4.57x | 4.85x | 5.75x |
| name+range | 3.9 ms | 4.7 ms | 6.9 ms | 4.8 ms |
| full-scan | 221.8 ms | 235.9 ms | 243.0 ms | 225.1 ms |

Verdict: tier1 admission **+25–33% relative** (0.378–0.408x -> stable
0.502–0.503x across both runs — outside noise). Tier2 also up
(series-table labels decode shares the parser). Queries unchanged
within noise. Raw: `/tmp/baseline_20261003/bench_111_run{1,2}.txt`.

## Retest: #110 partitioned bulk append (rev f790f3d, same protocol)

`bench` double run, second quoted. Bit-exact f64 spot checks pass on
both runs; storage byte-identical (18.694 B/pt).

| metric | baseline r1 | baseline r2 | #110 r1 | #110 r2 |
|---|---:|---:|---:|---:|
| plain | 3.58M | 3.62M | 3.24M | 3.61M |
| tier2 | 17.23M | 16.55M | 35.31M | 38.84M |
| **tier2 / plain** | **4.81x** | **4.57x** | **10.9x** | **10.8x** |
| tier1 | 1.46M | 1.37M | 1.60M | 1.76M |
| tier1 / plain | 0.408x | 0.378x | 0.494x | 0.488x |
| flush / compact | 28.9 / 1211 ms | 34.0 / 1330 ms | 33.9 / 1297 ms | 30.7 / 1182 ms |

Verdict: tier2 admission **~2.3x relative** (4.6–4.8x -> stable
10.8–10.9x across both runs). Tier1 steady at the #111 level.
Flush/compact/queries/storage unchanged. Raw:
`/tmp/baseline_20261003/bench_110_run{1,2}.txt`.

## Retest: #106 chunked term/trace-index inserts (rev b221bd9)

Tracked workloads (`bench-logs`/`bench-traces` double runs): counts
verified equal, term rows identical (88713 tg), storage byte-identical,
flush/optimize/query latencies within noise. No regression.

Dedicated A/B (pre/post binaries, one 8191-row buffer, high-cardinality
`user` index keys + trigrams = 9500 terms/block, interleaved 6+6,
fresh DB each trial): pre mean 20.3 ms, post mean 20.0 ms — **neutral**.
Block encode (zstd) and b-tree index maintenance dominate the flush;
per-statement VDBE overhead was only ~1–2 ms of ~20 ms on tmpfs.
Kept anyway: 9500 statements -> 24 per block with identical semantics
(OR IGNORE + inserted-row accounting verified), which matters more on
durable (non-tmpfs) storage with real sync costs.

## Retest: #107 single-parse log metadata (rev c26303c, same protocol)

`bench-logs` double run, second quoted. Counts verified equal,
storage byte-identical (9.10 B/entry).

| metric | baseline r1 | baseline r2 | #107 r1 | #107 r2 |
|---|---:|---:|---:|---:|
| plain | 3.24M | 2.86M | 3.17M | 2.87M |
| tier1 | 0.29M | 0.29M | 0.32M | 0.32M |
| **tier1 / plain** | **0.089x** | **0.101x** | **0.101x** | **0.112x** |
| tier2 | 0.45M | 0.46M | 0.49M | 0.46M |
| flush / trigram optimize | 0.8 / 1332 ms | 0.7 / 1229 ms | 0.8 / 1313 ms | 0.8 / 1313 ms |

Verdict: tier1 admission **~+10% relative**, borderline against the
plain-driven variance (baseline ratios themselves span 0.089–0.101x).
Expected order: the bench uses small flat metadata, so serde was a
modest share; the removed work (2 parses + 1 serialize + full-map
clone per row) is strictly less. Kept as simplification + small win.
Raw: `/tmp/baseline_20261003/bench_logs_107_run{1,2}.txt`.

## Retest: #112 regex cache + filter-before-clone (rev aa15e8e)

`query-read` double run (median_us) + `bench` double run. Anchoring,
cache hits, and per-query invalid-pattern errors verified live.

| query-read query | baseline r1 | baseline r2 | #112 r1 | #112 r2 |
|---|---:|---:|---:|---:|
| selective_regex_raw_batches | 1983 | 1815 | 420 | 1026 |
| selective_negative_raw_batches | 23785 | 23273 | 20988 | 21795 |
| selective_series_discovery | 583 | 586 | 421 | 536 |
| selective_label_values | 542 | 505 | 465 | 544 |

Verdict: regex-filtered queries clearly faster (selective_regex medians
1983/1815 -> 420/1026; ranges overlap only at extremes). `bench`
ingest steady (T1 1.52/1.74M, T2 35.1/35.8M — #110 level held),
queries/storage within noise. Raw:
`/tmp/baseline_20261003/query_read_112_run{1,2}.txt`,
`/tmp/baseline_20261003/bench_112_run{1,2}.txt`.

## Retest: #109 single-sweep batch range (rev b88780a, same protocol)

`query-read` double run (median_us) + `bench` double run. Core +
ext + query-harness suites green (traversal order preserved).

| query-read query (12000-series) | baseline r1 | baseline r2 | #109 r1 | #109 r2 |
|---|---:|---:|---:|---:|
| wide_raw_batches | 42705 | 41496 | 37631 | 38063 |
| wide_raw_frame | 37960 | 37090 | 35397 | 34620 |
| narrow_raw_batches | 660 | 589 | 572 | 574 |
| exact_raw_batches | 34 | 35 | 29 | 30 |

Verdict: wide batch path **~-10%** on both runs (single sweep +
pre-sized buffers + unstable sort). `bench` ingest/queries/storage
steady. Raw: `/tmp/baseline_20261003/query_read_109_run{1,2}.txt`,
`/tmp/baseline_20261003/bench_109_run{1,2}.txt`.

## Retest: #108 batched single-series reads (rev 652720b, same protocol)

Tracked workloads steady (`query-read` wide_raw 37615 vs 37631/38063;
`bench` T1 1.66M, T2 38.5M, queries/storage within noise).

Dedicated A/B (pre/post binaries, one 800k-point series in 100
wave-flushed chunks, 5x `timeless_raw` full-range counts): pre ~51.8ms,
post ~51.8ms — **neutral**. Chunk decode dominates (~50ms); the ~100
saved borrows+prepares are ~1–2ms on tmpfs. Prefix/latest/aggregate
single reads intentionally unchanged (early-exit would over-read).
The cross-borrow statement-cache half is deferred: stale-pointer risk
across close/reopen needs generation-validated eviction, and the
measured prize (~1ms/100 chunks here) doesn't justify it now.
Raw: microbench inline above (no saved output).

## Retest: #113 substring pruning without materialization (rev 5359945)

Dedicated A/B (pre/post binaries, 200k-row table, 25 blocks,
order-balanced 3x(6+6) trials, medians): `message_contains`
selective 53.1 -> 49.1 ms (-7.5%), absent-needle 22.8 -> 19.1 ms
(-16%). Debugging note: the first attempt scanned the whole
concatenation at once and REGRESSED absent to +17% — one wide
`windows()` scan measures ~2x slower than per-row scans of the same
bytes; the kept version scans borrowed rows. Parity test
(`columnar_feasibility_matches_full_decode`) guards present/absent/
cross-boundary needles at 3 sizes.

Tracked `bench-logs` steady (T1 0.34M, LIKE/buckets within band,
storage byte-identical). Raw:
`/tmp/baseline_20261003/bench_logs_113_run{1,2}.txt`.
