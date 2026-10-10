# Chunk index on disk in the engine (#132 phase 2) — 2026-10-10

Branch `feat/chunk-index-on-disk` @ `cfbb4e5`, release extension, same host,
harness and fleet fixture as the phase 1 runs
([2026-10-10_index_cache_engine.md](2026-10-10_index_cache_engine.md)):
`query-read EXT --index-cache --series N --runs 10`, with
`TIMELESS_BENCH_INDEX_CACHE` choosing the table's `index_cache`. With the
catalog on disk the raw chunk index is now read from the chunk rows as well;
`unbounded` holds both in memory as before. Every series holds 4 raw chunks.
SQLite's page cache is its 2 MB default. Raw CSV in
`2026-10-10_chunk_index_engine/`.

## 2M series (8M chunks)

| measure | memory | disk, 128 MB cache | disk, no cache | phase 1 disk, 128 MB |
|---|---:|---:|---:|---:|
| fresh process: RSS after open | 4.58 GB | **6.4 MB** | 6.4 MB | 1.91 GB |
| fresh process: peak RSS during open | 5.48 GB | 76 MB | 75 MB | 2.81 GB |
| fresh process: open to first read | 13.3 s | 3.1 s | 3.1 s | 4.0 s |
| writer RSS growth (create + 3 cycles + reads) | 5.85 GB | 1.28 GB | 1.16 GB | 3.31 GB |
| steady ingest cycle | 9.81 s | 8.59 s | 16.4 s | 8.45 s |
| create cycle (2M new series) | 35.9 s | 34.0 s | 31.4 s | 44.1 s |
| `timeless_series` name + `cm_mac`, bounded (19 rows) | 93 µs | 298 µs | 297 µs | 215 µs |
| `timeless_raw_frame` name + `cm_mac` (19 series) | 97 µs | 485 µs | 294 µs | 147 µs |
| `timeless_raw_frame` exact name (400k series) | 2.97 s | 4.77 s | 5.19 s | 3.02 s |
| `timeless_series` exact name, bounded (400k rows) | 1.59 s | 3.59 s | 3.64 s | 2.81 s |
| `timeless_series` name + `role=~`, bounded | 0.62 s | 1.44 s | 1.45 s | 1.73 s |
| `timeless_label_names` | 2.2 ms | 4.3 ms | 4.4 ms | 4.0 ms |

## 500k and 50k series

| measure | 500k memory | 500k 128 MB | 500k 0 | 50k memory | 50k 128 MB | 50k 0 |
|---|---:|---:|---:|---:|---:|---:|
| RSS after open | 1.15 GB | 6.4 MB | 6.4 MB | 118 MB | 6.6 MB | 6.5 MB |
| open to first read | 3.07 s | 0.68 s | 0.68 s | 279 ms | 75 ms | 70 ms |
| writer RSS growth | 1.54 GB | 326 MB | 376 MB | 155 MB | 72 MB | 86 MB |
| steady ingest cycle | 1.99 s | 1.79 s | 3.77 s | 183 ms | 159 ms | 369 ms |
| name + `cm_mac`, bounded | 91 µs | 239 µs | 203 µs | 58 µs | 182 µs | 174 µs |
| raw frame, name + `cm_mac` | 79 µs | 216 µs | 215 µs | 69 µs | 186 µs | 182 µs |
| raw frame, exact name | 636 ms | 1.00 s | 0.96 s | 32.6 ms | 70.2 ms | 66.3 ms |
| catalog, exact name | 236 ms | 581 ms | 547 ms | 16.8 ms | 48.8 ms | 52.4 ms |

## What it shows

1. **A disk-mode table holds almost nothing once opened**: about 6 MB at
   every scale, where memory mode holds ~2.3 KB a series (4.6 GB at 2M).
   Phase 1 alone left 1.9 GB at 2M, all of it the chunk index.
2. **While writing and reading, a 2M-series process holds ~1.2 GB**,
   against 5.9 GB in memory mode and 3.3 GB after phase 1. What remains is
   not an index: one partition buffer per series written (kept after it
   drains), the rollup index, allocator retention, and SQLite's caches.
   Dropping drained buffers is the next memory lever.
3. **Ingest is unchanged**: with every series' identity cached, a cycle is
   as fast as in memory (8.6 s vs 9.8 s at 2M); with no cache it is 5.5% of
   a five-minute interval.
4. **Reads pay for it: about 2 to 3 times memory mode**, more than phase 1.
   A 19-series read is ~200–300 µs against ~90 µs (one run at 2M measured
   485 µs; a rerun with a 256 MB page cache measured 252 µs, so run-to-run
   variation at this size is large). Whole-metric reads are 1.6–2.3×. The
   added cost over phase 1 is reading each chunk's metadata from its row
   and then its payload in a second lookup.
5. **Opening still takes 3.1 s at 2M**, down from 4.0 s (13.3 s in memory).
   It is no longer loading: it is three scans that check and summarize the
   store (the series ids, the distinct series that have chunks, and the
   chunk totals). They can be made incremental or deferred.

A follow-up change (probing up to 256 series one cached statement each
instead of one join) moved the 500k selective raw read from 216 to 191 µs.

## Open items

- Read latency: fetch a chunk's metadata and payload in one statement
  instead of two lookups; a covering index for metadata-only reads
  (statistics, planning). Any such index is index bytes and stays out of
  every compression figure.
- Open time: keep the store's totals and its chunk-series set
  incrementally instead of scanning for them.
- Memory while running: drop a partition buffer once it has drained and
  stayed idle; the rollup index.
- Not yet measured in disk mode: compaction sweeps at scale (planning is a
  scan of the eligible chunks), retention deletes at scale.
