# Native latest-point result — 2026-07-31

Session 2 added the public `timeless_latest` TVF and persisted the first value
at each new chunk's maximum timestamp as nullable metadata:

```sql
SELECT series_id, labels, ts, value
  FROM timeless_latest(
    'metrics', 'cpu_usage', '{"env":"prod"}', :start, :stop);
```

Starting revision was `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` on
`feat/timeless-metrics-embedding`, with the paired implementation uncommitted.

## Environment and commands

Core Ultra 9 185H, Linux 7.1.3, 22 logical CPUs, `powersave`, `/tmp` on
tmpfs, SQLite 3.53.2. The standard dataset is 12,000 series × 60 points; the
long-history gate is 2,000 series × 600 points. Both contain 1 metric and four
labels per series.

```sh
cargo build --release -p timeless-ext
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin query-read -- target/release/libtimeless_ext.so \
  --series 12000 --points 60 --runs 20
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin query-read -- target/release/libtimeless_ext.so \
  --series 2000 --points 600 --runs 20
```

## Result

| Dataset | Raw latest fallback | Native latest | Speedup |
|---|---:|---:|---:|
| 12K × 60 | 102.515ms | 15.368ms | 6.67x |
| 2K × 600 | 27.659ms | 1.858ms | 14.89x |

On the standard run, native p95 was 16.667ms versus 103.570ms raw. On the
long-history run, native p95 was 2.354ms versus 27.960ms raw. The latter clears
the Session 2 10x gate. The standard database grew from the prior aggregate
run's 9,908,528 bytes to 10,163,224 bytes (2.57%) for the nullable eight-byte
summary plus SQLite record overhead.

## Contract and compatibility

- Bounds are inclusive; reversed/empty ranges and empty series emit no row.
- The greatest timestamp wins. Duplicate maxima keep the first point in stable
  raw engine order: chunk index, then in-chunk, then buffered insertion order.
- Candidate chunks are considered newest-first and older candidates are
  skipped once they cannot change the winner.
- Buffered points and transaction visibility remain part of the result.
- New chunks use `max_ts_val` without PCO decompression when the chunk maximum
  is inside the query bound. Legacy filesystem and SQLite chunks decode.
- SQLite databases missing the nullable column migrate during xConnect; old
  rows remain NULL and compact into the new form later.

The paired public-API result is in
`timeless_metrics/bench/results/2026-07-31_native_latest.md`.

Validation:

- `cargo test --workspace`: passed
- `tests/cli.sh`: all 36 sections passed, including three 50K-op oracle seeds,
  five kill/reopen crash rounds, and the legacy-column migration
- focused and full TimelessMetrics suites: passed
- formatting and `git diff --check`: passed
