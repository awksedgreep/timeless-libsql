# Index-cache disk prototype (#132 phase 0, part 2) — 2026-10-09

Same harness, host, and fixture as the
[baseline](2026-10-09_index_cache_baseline.md). After building each store
with today's engine, a **fresh process with a plain SQLite connection** (no
extension, no engine, SQLite's default 2 MB page cache) backfills the
proposed tables from `metrics_series` and runs the workload against them:

```sql
CREATE TABLE metrics_labels (id INTEGER PRIMARY KEY, key TEXT, value TEXT,
  series INTEGER NOT NULL DEFAULT 0, UNIQUE(key, value));   -- names as ('__name__', name)
CREATE TABLE metrics_postings (label_id INTEGER, series_id INTEGER,
  PRIMARY KEY(label_id, series_id)) WITHOUT ROWID;
CREATE TABLE metrics_series_hash (hash INTEGER PRIMARY KEY, series_id INTEGER);  -- optional
```

Selectors scan only the shortest required posting list (`series` count) and
probe the rest by primary key, as `find_series` does in memory. Raw CSV in
`2026-10-09_index_cache_disk/`; `run_N.csv` repeats the baseline rows too.

## Size and migration

| | 10k | 50k | 500k | 2M |
|---|---:|---:|---:|---:|
| postings bytes / (label, series) row | 10.4 | 10.9 | 11.3 | 11.3 |
| added bytes / series (all three tables) | 115 | 119 | 122 | 122 |
| one-time backfill | 57 ms | 0.33 s | 4.9 s | 18.4 s |

`metrics_series_hash` is ~20 B/series of that.

## Ingest resolve: one full cycle, every series once, write order

Extra time per series over the no-lookup floor (hashing and fixture
generation alone), after one filling cycle; best of two steady cycles.

| cache budget (of working set) | policy | miss path | 50k | 500k | 2M | hit rate |
|---|---|---|---:|---:|---:|---:|
| 100% | either | — | ~0 ns | 104–133 ns | 187–236 ns | 1.00 |
| 50% | pinned | hash table | 483 ns | 989 ns | 1,075 ns | 0.50 |
| 10% | pinned | hash table | 915 ns | 1,741 ns | 2,093 ns | 0.10 |
| ≤ 50% | CLOCK | hash table | 1,110 ns | 1,541 ns | 2,144 ns | **0.00** |
| ≤ 50% | CLOCK | `UNIQUE(name, labels)` | 1,667 ns | 1,886 ns | 2,368 ns | **0.00** |

A full hash → id resolve cache is **62 MB at 2M series** (31 B/series).

## Reads

Median µs, 10 runs; the second figure is the first run on a fresh
connection. In-memory comparisons are from the same runs.

| read (2M series) | disk | in memory, unbounded | in memory, bounded (servers) |
|---|---:|---:|---:|
| name + `cm_mac`, 19 rows | **34** (290 fresh) | 211 | 101,842 |
| name + `role=~`, 126k rows | 275,641 | — | 721,503 |
| exact name, 400k rows | 184,952 | 1,497,383 | 1,721,224 |
| label names | 449 | — | 1,434 |
| `cm_mac` values (21k) | 1,344 | — | 8,075 |
| metric names | 1 | — | 30 |

The 19-row selector on disk is flat across scale: 29, 30, 31, 34 µs at
10k, 50k, 500k, 2M. Exact-name rows are not like for like: the extension's
`timeless_series` also reports each series' chunk range and counts, while
the prototype only decodes labels.

## Memory and open

| | 10k | 50k | 500k | 2M |
|---|---:|---:|---:|---:|
| today: fresh-process open to first read | 59 ms | 0.28 s | 3.7 s | 13.2 s |
| today: RSS after open | 30 MB | 121 MB | 1.15 GB | 4.58 GB |
| disk: process RSS after the whole workload | 4.2 MB | 4.7 MB | 33 MB | 28 MB |
| disk: process high-water (incl. the 100% caches) | 7.4 MB | 9.0 MB | 36 MB | 121 MB |

## Against the go/no-go criteria

| criterion | result |
|---|---|
| ingest under a small budget < 10% of the 5-minute interval | **pass**: zero hits costs +2.1–2.4 µs/series, so a 2M cycle is ~9.3 s + 4.7 s ≈ 14 s, **4.7%** |
| warm selector within 2× of today | **pass**: 6× *faster* than in-memory postings for a selective query; regex and discovery faster than today's server path |
| 50k series at 64 MB under ~100 MB RSS | **pass for the series side** (4.7 MB + a 1.5 MB full resolve cache); chunk index not yet included |
| `unbounded` within 5% of today | **not yet testable**: a full cache costs 190–240 ns/series here because it is an extra map; in the engine it replaces `series_map`, so it should be ~0, to be confirmed in phase 2 |

## What this settles

1. **The resolve cache holds hash → id only.** 31 B/series; labels are never
   needed on the ingest path. Even "unbounded" at 2M is 62 MB, not 4.6 GB.
2. **LRU/CLOCK cannot be the policy.** A cycle over more series than fit
   evicts every entry before it is reused (0% hits at any budget below
   100%). Admit-until-full gives exactly the budget fraction, the optimum
   for cyclic access. Phase 2 uses admission with a slow frequency-based
   refresh, not recency.
3. **Misses go to the existing `UNIQUE(name, canonical_labels)`.** The hash
   table saves ~10–30% per miss for ~20 B/series and a fourth table; not
   worth it at these costs.
4. **Selectors must drive from the shortest posting list,** with a
   maintained per-label series count. The first prototype intersected
   lists whole (6.9 ms at 2M); driving from the shortest made it 34 µs.
5. **The series side of the edge footprint is solved.** What remains of the
   ~2.9 KB/series is the chunk index (~195 B/chunk) and partition buffers,
   which is phase 3. That is the remaining risk.
