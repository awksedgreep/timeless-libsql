# Index-cache baseline (#132 phase 0) — 2026-10-09

Today's all-in-memory metrics engine (branch `exp/index-cache-phase0` from
`76e0ce3`, v0.8.12 source), release extension, Intel Core Ultra 9 185H,
Linux 7.2.5, bundled SQLite 3.53.2. One run per scale; read medians over
10 runs after 3 warm-ups. Raw CSV in `2026-10-09_index_cache_baseline/`.

Command: `query-read EXT --index-cache --series N --runs 10`
(`tools/bench/src/bin/query_read/index_cache.rs`).

Fixture: fleet-shaped, 95 series per gateway (5 interface metrics × 19
interfaces), 8 labels (~270 B) per series, `cm_mac` per gateway; written as
20,000-point named batches, one transaction each. One create cycle, then
three steady cycles of one point per series (flushed after each), so each
series holds 4 raw chunks.

| measure | 10k | 50k | 500k | 2M |
|---|---:|---:|---:|---:|
| create cycle (new series) | 100 ms | 562 ms | 6.4 s | 27.1 s |
| steady cycle (re-resolve all) | 24 ms | 159 ms | 1.63 s | 8.0 s |
| steady ns / series | 2,440 | 3,173 | 3,258 | 4,009 |
| `timeless_series` exact name, bounded | 2.2 ms | 15.4 ms | 221 ms | 1.72 s |
| exact name, unbounded | 2.0 ms | 15.3 ms | 210 ms | 1.54 s |
| name + `cm_mac` (19 rows), bounded | 0.14 ms | 1.25 ms | 19.1 ms | 102 ms |
| name + `cm_mac` (19 rows), unbounded | 0.05 ms | 0.06 ms | 0.10 ms | 0.28 ms |
| name + `role=~` regex, bounded | 0.87 ms | 8.0 ms | 109 ms | 722 ms |
| `timeless_label_names` | 0.03 ms | 0.05 ms | 0.34 ms | 1.43 ms |
| `timeless_label_values(__name__)` | 0.03 ms | 0.03 ms | 0.03 ms | 0.03 ms |
| `timeless_label_values(cm_mac)` | 0.04 ms | 0.10 ms | 1.02 ms | 8.1 ms |
| writer RSS growth | 43 MB | 151 MB | 1.37 GB | 5.44 GB |
| writer RSS / series | 4.6 KB | 3.2 KB | 2.9 KB | 2.9 KB |
| database bytes / series | 833 | 831 | 847 | 838 |
| fresh process: open to first read | 57 ms | 292 ms | 3.47 s | 14.3 s |
| fresh process: RSS after open | 30 MB | 121 MB | 1.15 GB | 4.58 GB |

## Findings

1. **Bounded selectors do not use the posting lists.** The bounded
   `timeless_series` path, which servers always use, iterates every series of
   the metric and tests each one's labels; only the unbounded path
   intersects `label_index`. A 19-series selector costs 102 ms at 2M series
   bounded and 0.28 ms unbounded (360×). This is independent of #132 and
   fixable on its own.
2. **Exact-name reads are dominated by per-row output**, ~3.8 µs/row at 2M,
   the same bounded or unbounded, so index lookup is not their cost.
3. **Steady ingest resolves at 2.4–4.0 µs/series**, mostly not the hash
   lookup. A full 2M-series cycle is 8 s (2.7% of a 300 s interval). This
   is the budget the disk path's cache misses must fit inside.
4. **Open cost is linear: ~7 µs and ~2.4 KB resident per series.** 2M series
   take 14 s and 4.6 GB before the first read. This is the cost an on-disk
   index removes.
5. On disk the whole store is ~840 B/series, including 4 raw chunks.
