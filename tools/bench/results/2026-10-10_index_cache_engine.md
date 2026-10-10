# Index-cache in the engine (#132 phase 1) — 2026-10-10

The real engine on `exp/index-cache-phase0` @ `0ffa231`, release extension,
same host, harness and fleet fixture as the phase 0 runs
(`query-read EXT --index-cache --series N --runs 10`, with
`TIMELESS_BENCH_INDEX_CACHE` choosing the table's `index_cache`). Three
profiles: **memory** (`unbounded`, today's catalog in memory), **128 MB**
(disk catalog, resolve cache large enough for every series: the stack's
medium profile), and **0** (disk catalog, no resolve cache: the router
profile). The chunk index is still in memory in all three (phase 2).
Raw CSV in `2026-10-10_index_cache_engine/`.

## 2M series

| measure | memory | 128 MB | 0 |
|---|---:|---:|---:|
| steady ingest cycle (re-resolve all) | 9.48 s | **8.45 s** | 16.2 s |
| … as a share of a 300 s interval | 3.2% | 2.8% | 5.4% |
| create cycle (2M new series) | 64.4 s | 44.1 s | 44.6 s |
| fresh process: open to first read | 13.8 s | **4.0 s** | 4.2 s |
| fresh process: RSS after open | 4.58 GB | **1.91 GB** | 1.91 GB |
| writer RSS growth | 5.85 GB | 3.31 GB | 3.19 GB |
| database bytes / series | 941 | 941 | 941 |
| `timeless_series` name + `cm_mac`, bounded (19 rows) | 343 µs | **215 µs** | 218 µs |
| … unbounded | 233 µs | **145 µs** | 177 µs |
| `timeless_raw_frame` name + `cm_mac` | 247 µs | **147 µs** | 167 µs |
| `timeless_raw_frame` exact name (400k series) | 3.03 s | 3.02 s | 3.12 s |
| `timeless_series` exact name, bounded (400k rows) | 1.45 s | 2.81 s | 2.55 s |
| … unbounded | 1.31 s | 1.61 s | 1.97 s |
| `timeless_series` name + `role=~`, bounded (126k rows) | 0.66 s | 1.73 s | 2.14 s |
| `timeless_label_names` | 2.1 ms | 4.0 ms | 4.9 ms |
| `timeless_label_values(cm_mac)` (21k) | 7.4 ms | 10.2 ms | 8.2 ms |
| `timeless_label_values(__name__)` | 0.03 ms | 0.04 ms | 0.05 ms |

## 500k and 50k series

| measure | 500k memory | 500k 128 MB | 500k 0 | 50k memory | 50k 128 MB | 50k 0 |
|---|---:|---:|---:|---:|---:|---:|
| steady ingest cycle | 2.12 s | 1.96 s | 4.03 s | 174 ms | 182 ms | 388 ms |
| open to first read | 3.2 s | 0.98 s | 1.14 s | 309 ms | 101 ms | 98 ms |
| RSS after open | 1.15 GB | 482 MB | 482 MB | 118 MB | 54 MB | 54 MB |
| writer RSS growth | 1.54 GB | 837 MB | 875 MB | 156 MB | 94 MB | 112 MB |
| name + `cm_mac`, bounded | 126 µs | 155 µs | 173 µs | 58 µs | 132 µs | 138 µs |
| exact name, bounded | 242 ms | 386 ms | 420 ms | 17.0 ms | 34.3 ms | 31.5 ms |
| name + `role=~`, bounded | 121 ms | 226 ms | 262 ms | 6.5 ms | 19.2 ms | 18.8 ms |

## Against the criteria

| criterion | result |
|---|---|
| medium-cache profile within 5% of today's ingest | **pass**: 8.45 s vs 9.48 s at 2M (11% faster); 1.96 vs 2.12 s at 500k; 182 vs 174 ms at 50k (+5%) |
| zero-hit ingest under 10% of the interval | **pass**: 5.4% at 2M |
| reads no slower than today (stack) | **mixed**: selective reads are faster at 2M (1.6×); a whole metric's raw read is equal; catalog reads of a whole metric are 1.2–1.9× slower and a regex selector 2.6× slower |
| 50k series under ~100 MB RSS | **borderline**: 94 MB writer growth at 128 MB budget, 54 MB after open; what is left is the chunk index and partition buffers (phase 2) |

## Notes

1. **Selective reads got faster in memory mode too.** The bounded
   `timeless_series` path now selects through the label index (#133):
   343 µs at 2M, down from 102 ms in the phase 0 baseline.
2. **Whole-metric catalog reads are the remaining read gap.** Memory mode
   borrows labels from the registry; disk mode decodes each row. A regex
   matcher is worse because it reads every series of the metric: it
   should resolve the regex against the key's distinct values in
   `_labels` and select through their postings, as the phase 0 prototype
   did (275 ms at 2M).
3. **Creating series costs more than in the phase 0 baseline** (27 s for
   2M then; 44 s on disk, 64 s in memory now): every new series writes
   ~9 posting rows, and memory mode also fills the registry. It is a
   one-time cost per series; a churn-heavy fleet pays it per new series.
   Memory mode maintaining an index it does not read is the decision
   that makes switching modes rebuild-free; it could instead be made
   lazy.
4. **Disk adds ~103 B/series** (941 vs 838), as phase 0 measured.
5. Open time and resident memory fall by the registry's share (4.0 s
   and 1.9 GB at 2M, from 13.8 s and 4.6 GB); the rest is the chunk
   index, which phase 2 moves.
