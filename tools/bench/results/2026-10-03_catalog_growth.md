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

## Reader change

Engine source: `37e618327e0c771f4c34a4944fbb63cbbc598866`.
The release extension reports this exact build commit. SHA-256:
`944343b6a8b9eb46e34d8da5e98a0f6f606f39afd3cd5c1d240e898a318f2211`.

The reader keeps the ordered scan and buffer preallocation from #109. After
eight unrelated chunk-index entries it seeks directly to the next requested
series. Short gaps stay sequential, so a half-catalog selection does not
require a B-tree seek for every other series. Long gaps cost bounded scanning
plus a seek, independent of how many unrelated chunks occupy the gap. B-tree
depth and cache effects still depend on overall index size; this does not
promise perfectly constant latency as the database grows.

The same executable measured both sequential captures (SHA-256
`29ebb1fd577c2017421d84a055685b2121905393ea8f8a56b67dc12ff8d91556`).
After files are `after_{50000,152000,standard}_{1,2}.csv` in the raw-capture
directory. All before/after result counts, byte sizes, and available checksums
match. Second-process medians:

| Workload | Before, ms | After, ms |
|---|---:|---:|
| Fixed 161, interleaved, 50k | 0.458 | 0.182 |
| Fixed 161, interleaved, 152k | 2.924 | 0.226 |
| Fixed 161, adjacent, 50k | 0.176 | 0.205 |
| Fixed 161, adjacent, 152k | 0.174 | 0.152 |
| Standard wide raw batches | 39.743 | 38.194 |
| Standard wide raw frame | 36.667 | 33.279 |

The separate growth-fixture processes showed more variation on broad reads
(for example, +9% for the interleaved 152k broad control), so those captures
alone cannot settle the broad-query tradeoff. The paired measurement below
uses the same persisted data and alternates readers to address that ambiguity.

## Paired comparison

The benchmark also accepts `--compare-extension`. Both extensions open their
own reader on one persisted fixture. Frames are verified byte-for-byte before
timing; result summaries are checked on every iteration. Five warmups precede
30 measured reads per extension, alternating which goes first. Process 2
reverses which library is primary, so load order is not confused with the fix.

```sh
tools/bench/target/release/query-read "$PWD/target/issue116-gap-seek.so" \
  --catalog-growth --compare-extension "$PWD/target/issue116-baseline.so" \
  --series 152000 --points 1 --runs 30
# Repeat at 50000 series, then repeat both sizes with extension paths swapped.
```

This paired executable includes three equivalent `as_chunks` lint updates to
the older benchmark byte loops, applied identically to both readers. Its
SHA-256 is
`5edaceee5701aca30e5109da8d74f8ce28162ac5dc28ef9158f140e687c11eff`.
Raw files: `paired_{50000,152000}_{1,2}.csv`. In process 1, `primary_` is the
new reader and `comparison_` is the baseline; process 2 reverses those roles.

Interleaved-layout medians, milliseconds, shown as **before → after**:

| Workload | Process 1 | Process 2 (reversed load order) |
|---|---:|---:|
| Fixed 161, 50k | 0.542 → 0.228 | 0.681 → 0.225 |
| Fixed 161, 152k | 2.808 → 0.333 | 2.960 → 0.359 |
| Broad, 50k | 62.060 → 62.319 | 65.251 → 65.027 |
| Broad, 152k | 243.886 → 241.396 | 245.335 → 241.055 |
| Half, 50k | 39.561 → 40.825 | 39.468 → 39.966 |
| Half, 152k | 133.947 → 134.589 | 136.859 → 140.510 |

The 152k selective query is **8.2–8.4× faster** in the paired runs. Its p95
falls from 3.027 / 3.390 ms to 0.457 / 0.501 ms. Across both layouts, both
catalog sizes, and both load orders, broad/half-query median changes range
from **−2.4% to +3.2%**. Adjacent fixed-161 medians change by 0–3.3%.
Single-series differences are a few microseconds and reverse direction with
library load order; these are not evidence of a repeatable single-series gain.

Verdict: the catalog-span regression is removed while broad-query throughput
is preserved within the variation observed here. The original standard broad
benchmark also holds up. These are local SQL results, not production HTTP
latency measurements or a cross-version server comparison.

## Correctness and tooling checks

- Root `cargo test --workspace --locked`: 295 passed, one pre-existing
  production-scale memory fixture ignored.
- Metrics API package with the new release extension and `--include-ignored`:
  193 passed, including 108 storage/API contracts.
- Query harness: 65 passed; public SQL gate: 135 recipes / 173 statements.
- Focused shell correctness suites: `r1`, `r2`, `r3`, `r4`, `r8` passed.
- Root workspace Clippy and query-read Clippy with `-D warnings` passed;
  root and bench formatting checks passed.
- New batch-vs-single regression covers dense and sparse selections, missing
  IDs, duplicate IDs, buffered-only series, multiple chunks with equal minimum
  timestamps, stable timestamp ties, empty/reversed ranges, and inclusive
  work limits. Returned values are compared by their bits.
- Paired benchmark smoke and standard benchmark smoke passed after the
  benchmark refactor. All 16 saved captures have matching compared results.
