# Index-cache: chunk index on disk (#132 phase 0, part 3) — 2026-10-09

Same harness, host, and fixture as parts 1 and 2. Each series holds 4 raw
(`encoding = 1`) chunks, so 2M series are 8M chunk rows. The disk side runs in
the same fresh plain-SQLite process as part 2, after the series-index
workload. "Today" is the extension's `timeless_raw_frame`, end to end:
in-memory chunk index, payload read, decode, and frame build. Raw CSV in
`2026-10-09_index_cache_chunks/`.

## Reads

Median µs, 10 runs. Disk rows exclude decoding and frame building, which
both paths share; these raw chunks decode as a fixed-width copy.

| read | 10k | 50k | 500k | 2M |
|---|---:|---:|---:|---:|
| **19 series, all points: today** | 77 | 89 | 118 | **332** |
| 19 series: disk metadata, one probe per series | 33 | 37 | 39 | 38 |
| 19 series: disk metadata + blobs | 109 | 111 | 117 | **110** |
| 19 series: disk metadata, covering index | 23 | 20 | 35 | 25 |
| **one metric (20% of series), all points: today** | 5,626 | 36,266 | 724,202 | **3,392,447** |
| one metric: disk metadata, one probe per series | 4,182 | 22,554 | 245,895 | 1,013,890 |
| one metric: disk metadata, covering index, one probe per series | 2,413 | 12,416 | 127,061 | 627,371 |
| one metric: disk metadata, one joined statement | 823 | 4,526 | 54,267 | 233,152 |
| one metric: disk metadata + blobs, one joined statement | 2,636 | 14,370 | 144,352 | **678,367** |
| one metric: per-series stats, one probe per series | 4,246 | 22,678 | 233,054 | 1,105,336 |

"One joined statement" is postings joined to `metrics_chunks` through the
covering index, ordered by `(series_id, ts_min)`: the batched shape of
today's ordered chunk reader. The first run on a fresh connection is within
noise of the median in every row.

## Maintenance and size

| | 10k | 50k | 500k | 2M |
|---|---:|---:|---:|---:|
| chunk rows | 39,900 | 199,880 | 2.0M | 8.0M |
| compaction plan: full scan of the covering index | 2.7 ms | 15.7 ms | 146 ms | 627 ms |
| existing `(series_id, ts_min)` index | 0.7 MB | 3.5 MB | 37 MB | 148 MB |
| covering index `(series_id, resolution, ts_min, ts_max, point_count, encoding)` | 0.9 MB | 4.9 MB | 50 MB | 201 MB |
| covering index build | 8 ms | 44 ms | 0.58 s | 2.5 s |

## Memory

| | 10k | 50k | 500k | 2M |
|---|---:|---:|---:|---:|
| today: writer RSS growth | 43 MB | 159 MB | 1.54 GB | 5.83 GB |
| today: fresh process after open | 30 MB | 121 MB | 1.15 GB | 4.58 GB |
| disk: process RSS (series + chunk workloads) | 4.3 MB | 4.7 MB | 33 MB | 28 MB |

## What this settles

1. **The chunk index can leave memory.** Narrow reads are flat at ~110 µs
   for metadata and blobs, against today's 77–332 µs, which grows with the
   catalog. A whole-metric read of 1.6M chunks is 0.68 s on disk against
   today's 3.39 s end to end.
2. **Reads must be one statement per selection, not per series.**
   Per-series probes cost ~0.5 µs of statement overhead each, 4.4× the
   joined statement at 2M.
3. **The covering index replaces the existing `(series_id, ts_min)` index;
   it is not added beside it.** Net +53 MB at 2M (~7 B/chunk), and metadata
   reads never touch payload rows.
4. **Compaction planning by full scan is 0.63 s at 8M chunks.** That is
   acceptable per sweep at this scale, but phase 3 should plan
   incrementally from the series written since the last sweep (known from
   the flush path) and keep the full scan for recovery.
5. **Per-series stats** (`timeless_series` min/max ts, points, chunks)
   should come from the same joined statement grouped by series, not a
   probe per series.
6. Not yet measured: retention deletes by time across all series (needs
   `(resolution, ts_max)` access), the rollup tiers, and the write path's
   index maintenance.

## Implication for the design

With postings, chunk metadata, and selectors all at least as fast on disk
as in memory, **the in-memory structures are no longer needed in any
mode**. The only cache that buys ingest speed is the hash → id resolve
cache (31 B/series), plus SQLite's page cache. `unbounded` for
timeless-stack then means a full resolve cache (62 MB at 2M) and a large
page cache, not 4.6 GB of maps, and label interning (#128) stops mattering
for either mode.
