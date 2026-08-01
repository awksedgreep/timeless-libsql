# Matcher and discovery pushdown — 2026-07-31

This is the direct-extension result for Session 7 of
[`QUERY_PERFORMANCE_PLAN.md`](../../../QUERY_PERFORMANCE_PLAN.md). All measured
queries use the public loadable-extension SQL surface.

## Change

The existing metric query TVFs already applied equality, inequality, anchored
regex, and negative-regex matchers to registry labels before reading chunks.
Session 7 extends the same filter contract to discovery:

```sql
SELECT labels
  FROM timeless_series('metrics', 'cpu_usage',
    '{"host":{"re":"web-.*"},"env":{"neq":"dev"}}');

SELECT value
  FROM timeless_label_values('metrics', 'cpu_usage', 'host',
    '{"env":{"neq":"dev"}}');
```

The original one-argument `timeless_series` and three-argument
`timeless_label_values` calls are unchanged. Filtered series discovery applies
all matchers to the registry candidate list first, then aggregates chunk and
buffer metadata only for the surviving series IDs. It never decompresses a
chunk.

## Reproduction

Starting revision: `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` on
`feat/timeless-metrics-embedding`, with the query work uncommitted.

```sh
cargo build --release -p timeless-ext
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin query-read -- target/release/libtimeless_ext.so \
  --series 12000 --points 60 --runs 5
```

Environment: Linux 7.1.3 x86-64, Intel Core Ultra 9 185H, Rust/Cargo 1.97.0,
bundled SQLite 3.53.2, and a `/tmp` database on `tmpfs`. The deterministic
dataset contains 12,000 series and 720,000 points.

## Direct SQL result

| Shape | Result | Median | P95 |
|---|---:|---:|---:|
| Exact equality raw | 1 series / 60 points | 0.074ms | 0.090ms |
| Selective anchored regex raw | 1 series / 60 points | 4.947ms | 5.771ms |
| Negative equality raw | 6K series / 360K points | 47.338ms | 51.456ms |
| Wide raw | 12K series / 720K points | 74.722ms | 75.193ms |
| Filtered series discovery | 1 catalog row | 0.731ms | 0.993ms |
| Filtered label values | 1 value | 0.903ms | 1.152ms |

Regex selection scans 12,000 small registry label maps but reads only the one
surviving series' chunk. The negative workload returns exactly half the
dataset and takes 63% of the wide-read median, consistent with avoiding the
other half's chunk reads and transport.

## Correctness

The extension and adapter suites cover absent labels as empty strings, empty
equality, equality/negative/regex/negative-regex combinations, full anchoring,
invalid regex errors for direct SQL, buffered visibility, rollback, two live
connections, and reopen. The 38-section CLI suite exercises the new discovery
calls independently of TimelessMetrics.

No on-disk format changed. The discovery additions are optional trailing TVF
arguments and the raw fallback remains available.
