# Native packed-window result — 2026-07-31

Session 3 added `timeless_window_batches`, a public one-row-per-series form of
the existing window kernel, plus a callback-safe batch core primitive and a
batched SQLite shadow-chunk read.

```sql
SELECT series_id, labels, buckets
  FROM timeless_window_batches(
    'metrics', 'cpu_usage', '{"env":"prod"}',
    :start, :stop, :step, :window, 'avg');
```

`buckets` is `TWB1`, u32 LE count, i64 LE timestamps, a low-bit-first
validity bitmap, then f64-bit LE values. The bitmap represents the optional
`fill='null'` result without changing the row-oriented `timeless_window` API.

Starting revision was `a84eddc30971cf5a83c3e1b4d08c51d42bfc8e69` on
`feat/timeless-metrics-embedding`, with the paired work uncommitted.

## Environment and commands

Core Ultra 9 185H, Linux 7.1.3, 22 BEAM schedulers, `powersave`, `/tmp` on
tmpfs, Elixir 1.20.2/OTP 29, SQLite 3.53.2. Dataset: one metric, 12,000
series, four labels, 60 points per series, 720,000 total points and 72,000
10-second output buckets.

```sh
cargo build --release -p timeless-ext
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin query-read -- target/release/libtimeless_ext.so \
  --series 12000 --points 60 --runs 20

MIX_ENV=test mix run bench/engine_query_bench.exs --engine rust --runs 20
MIX_ENV=test mix run bench/engine_query_bench.exs --engine libsql --runs 20
```

## Direct extension result

| Public SQL shape | Median | P95 | Rows / buckets |
|---|---:|---:|---:|
| Raw materialization plus host scalar fold | 117.640ms | 123.610ms | 12K / 720K |
| Row `timeless_window`, SQLite `COUNT(*)` | 108.825ms | 119.962ms | 72K |
| Packed `timeless_window_batches`, blobs consumed | 67.796ms | 69.236ms | 12K / 72K |

The packed call is 1.61x faster even though the row comparison performs only a
SQLite count. It also returns every timestamp/value bit to the host rather
than discarding them. The gain comes from one engine transition, one batched
shadow-table read, and 12K SQLite rows instead of 72K.

## TimelessMetrics result

| Engine / implementation | Median | P95 | Relative to old libSQL |
|---|---:|---:|---:|
| libSQL raw fallback baseline | 495.455ms | 506.141ms | 1.00x |
| libSQL row-window adapter | 273.657ms | 283.301ms | 1.78x faster |
| libSQL packed-window final | 127.532ms | 135.006ms | 3.88x faster |
| Rust block engine, matching final run | 119.937ms | 124.265ms | — |

Final libSQL is only 1.06x the Rust median and 1.09x its p95, inside the
migration plan's 20% representative-query gate for this workload.
Fixed-reader attribution in the final libSQL process was 96.516ms packed fetch,
6.114ms decode/shape, and 126.756ms combined; allocation/GC explains why the
combined measurement is not the sum of separately warmed stages.

## Correctness boundary

TimelessMetrics buckets are inclusive integer-second intervals aligned to
`from`. A complete `[bucket_start, bucket_start + step - 1]` bucket maps
exactly to the native `(t - step, t]` window at
`t = bucket_start + step - 1`. Only complete ranges use the native path.
Partial terminal buckets and `first`, `last`, and Timeless rate carry-in stay
on raw fallback. Current pointwise transforms run after the proven-equivalent
native aggregate.

Validation:

- raw-oracle matrix for avg/sum/min/max/count, negative timestamps, edges,
  duplicate timestamps, sparse/empty series, buffered/flushed visibility,
  matchers, transforms, and integer count
- observable partial-terminal-bucket and rate carry-in fallback cases
- VictoriaMetrics differential: 182/182
- `cargo test --workspace`: passed
- `tests/cli.sh`: all 36 sections, 150K-op oracle, and five crash rounds passed
- full TimelessMetrics suite: 467 passed
