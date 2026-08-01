# Native scalar aggregate result — 2026-07-31

Session 1 added the public `timeless_aggregate` TVF:

```sql
SELECT series_id, labels, value
  FROM timeless_aggregate(
    'metrics', 'cpu_usage', '{"env":"prod"}', :start, :stop, 'avg');
```

Starting revision was `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` on
`feat/timeless-metrics-embedding`, with the paired implementation uncommitted.
The machine and dataset are identical to
[`2026-07-31_query_read_baseline.md`](2026-07-31_query_read_baseline.md):
Core Ultra 9 185H, `powersave`, `/tmp` on tmpfs, SQLite 3.53.2, 12,000
series × 60 points.

## Command

```sh
cargo build --release -p timeless-ext
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin query-read -- target/release/libtimeless_ext.so --runs 10
```

## Fresh-process result

| Process | Raw scalar fallback | Native aggregate | Speedup |
|---|---:|---:|---:|
| 1 | 96.995ms | 14.133ms | 6.86x |
| 2 | 109.284ms | 16.180ms | 6.75x |
| 3 | 102.635ms | 16.080ms | 6.38x |

The final process also recorded 0.029ms exact raw, 1.476ms narrow raw,
101.491ms wide raw, and 39.286ms for the first exact read after publication.
The scalar kernel returns 12,000 compact rows instead of 720,000 raw points.

## Contract and validation

- Inclusive bounds and no row for an empty series/range.
- Operations: `avg`, `sum`, `min`, `max`, and `count`; count is a SQLite
  INTEGER and includes every stored point.
- Fully covered chunks use persisted statistics. Partial chunks are decoded.
- Sum/avg accumulation is left-to-right inside each chunk, followed by chunk
  sums in index order; flat-scan floating results may differ by rounding.
- NaNs propagate through sum/avg as SQL `NULL`; min/max ignore NaNs when a
  numeric value exists and otherwise return `NULL`.
- Direct CLI coverage includes buffered data, flush, rollback, matcher
  filtering, duplicate timestamps, negative timestamps, a second connection,
  reopen, malformed calls, and a flat-SQL oracle.

Validation:

- `cargo test --workspace`: passed
- `tests/cli.sh`: all 35 sections passed, including three 50K-op oracle seeds
  and five kill/reopen crash rounds
- standalone benchmark formatting and `git diff --check`: passed

The paired public-API result is in
`timeless_metrics/bench/results/2026-07-31_native_aggregate.md`.
