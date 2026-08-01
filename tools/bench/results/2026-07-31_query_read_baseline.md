# Direct extension query-read baseline — 2026-07-31

This is the first Session 0 result for
[`QUERY_PERFORMANCE_PLAN.md`](../../../QUERY_PERFORMANCE_PLAN.md). It measures
only public SQL exposed by the loadable extension; it does not call
`timeless-core` directly.

## Reproduction

Starting revision: `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` on
`feat/timeless-metrics-embedding`, with the query benchmark and other planned
embedding work uncommitted. The paired TimelessMetrics revision was
`d355d3d1ed43ad2f77e90e47b7a3455d57ba9d6b` on
`feat/libsql-storage-engine`, also dirty.

```sh
cargo build --release -p timeless-ext
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin query-read -- target/release/libtimeless_ext.so --runs 10
```

Environment:

- Linux 7.1.3, x86-64
- Intel Core Ultra 9 185H, 22 logical CPUs
- CPU governor: `powersave`
- benchmark database: `/tmp` on `tmpfs`
- Rust/Cargo 1.97.0, release profile with LTO and one codegen unit
- bundled SQLite 3.53.2
- deterministic labels and values; 12,000 series × 60 points = 720,000 points

## Final recorded process

```text
# ingest_us=103442
# flush_us=352098
# database_bytes=9916744
metric,median_us,p95_us,min_us,max_us,runs,result_series,result_points
first_exact_after_flush,39443,39443,39443,39443,1,1,60
exact_raw_batches,29,39,26,50,100,1,60
narrow_raw_batches,1496,1669,1463,1669,10,188,11280
wide_raw_batches,99975,102117,98684,102117,10,12000,720000
scalar_aggregate_raw_fallback,101135,109894,99537,109894,10,12000,720000
latest_raw_fallback,106445,110685,103145,110685,10,12000,12000
grid_count,111462,117277,110949,117277,10,0,72000
window_avg_count,114419,123954,113559,123954,10,0,72000
rollup_avg_count,170079,224265,161733,224265,10,0,60000
```

The database byte count includes the database, WAL/SHM files, and the rollups
built before the final measurement; it is not directly comparable to a store
without the same rollup lifecycle.

## Fresh-process variance

Three sequential processes produced these medians (milliseconds):

| Shape | Process 1 | Process 2 | Process 3 | Range |
|---|---:|---:|---:|---:|
| Wide raw batches | 109.319 | 108.028 | 99.975 | 9.344 |
| Scalar raw fallback | 109.237 | 108.137 | 101.135 | 8.102 |
| Latest raw fallback | 105.551 | 107.240 | 106.445 | 1.689 |

The scalar fallback decodes every value and computes one average per series;
it has almost the same cost as merely transferring the packed raw rows. This
confirms that selection, chunk reads, and transport dominate the arithmetic.
The paired BEAM attribution and Rust-engine comparison are recorded in
`timeless_metrics/bench/results/2026-07-31_libsql_query_baseline.md`.
