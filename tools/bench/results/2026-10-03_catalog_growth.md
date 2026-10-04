# Metrics catalog-growth baseline and gap-seeking reader (#116)

## Baseline

Extension source: `8ce05d3cf91f09077d537858596cb4d6be815585` (0.8.8).
The new benchmark was run before changing the engine. The extension's build
identity reports this commit, `release`, and `x86_64-unknown-linux-gnu`.
Baseline extension SHA-256:
`c6a5a1167cbc1ae30347da6bcb86cf80027fe811cfff11338be73b6f1acd92ec`.

Environment: Linux 7.2.5-3-omarchy, Intel Core Ultra 9 185H, Rust 1.98.1,
bundled SQLite 3.53.2, CPU governor `powersave`, temporary databases on `/tmp`.
No competing builds or tests ran during these captures. Normal desktop load
and frequency scaling remain sources of variance.

The catalog-growth fixture holds 161 target series and their samples/labels
constant while growing an unrelated metric. In the interleaved layout, target
IDs span the whole catalog; in the adjacent layout they occupy the first 161
IDs. Every series has one persisted sample. Queries use a 30-second lookback
and a node equality matcher. The broad control selects the unrelated metric;
the half control also selects an alternating label, exercising many short
gaps. The catalog-only control uses the bounded public series TVF, as PromQL
does. The writer is closed and recovery is warmed before measurement.

Every raw result is checked against generated IDs, timestamps, and value bits
before timing. CSV files retain counts, byte sizes, checksums, and latency
statistics; five warmups precede 60 timed reads per growth shape. Timings
include frame transfer/checksum work, exclude HTTP and concurrent ingestion,
and do not model a production database's historical chunk distribution.

Commands (run twice, sequentially, saving each stdout as CSV):

```sh
cargo build --release --locked -p timeless-ext
cargo build --release --locked --manifest-path tools/bench/Cargo.toml --bin query-read
cp target/release/libtimeless_ext.so target/issue116-baseline.so
for total in 50000 152000; do
  tools/bench/target/release/query-read "$PWD/target/issue116-baseline.so" \
    --catalog-growth --series "$total" --points 1 --runs 60
done
tools/bench/target/release/query-read "$PWD/target/issue116-baseline.so" --runs 20
```

The last command retains the existing 12,000-series × 60-point workload.
The copied binary is an ignored local comparison artifact, not another checkout.
Scratch databases are automatically removed.

Baseline medians, in milliseconds; independent process 1 / process 2:

| Workload | 50k catalog | 152k catalog |
|---|---:|---:|
| Fixed 161, interleaved | 0.525 / 0.458 | 2.693 / 2.924 |
| Fixed 161, adjacent | 0.173 / 0.176 | 0.186 / 0.174 |
| Broad, interleaved | 64.947 / 58.309 | 226.899 / 233.236 |
| Half, interleaved | 33.792 / 32.860 | 123.238 / 128.628 |

Existing standard workload medians: wide raw batches 39.545 / 39.743 ms;
wide raw frame 35.579 / 36.667 ms; narrow raw batches 0.560 / 0.581 ms;
single-series raw batches 0.030 / 0.029 ms.

Raw captures are in [2026-10-03_catalog_growth](2026-10-03_catalog_growth/):
`before_{50000,152000,standard}_{1,2}.csv`. The selective result stays fixed
at 161 series, 161 points, and 4,524 frame bytes. Its scaling comes from the
continuous index scan through unrelated IDs, not additional selected data.
