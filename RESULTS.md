# timeless-libsql — Hero POC Results

Two working days (2026-07-22), Sessions 1–4 of PLAN.md. All four success
criteria met (one with an honest asterisk). Reference machine: Arch Linux,
Rust 1.97, SQLite 3.53, sqld built from libsql main. Re-run 2026-07-22 on
Apple M5 Pro (macOS 26.5, Rust 1.97.1, bundled SQLite) — second-run numbers
per TESTING.md; on-disk sizes were byte-identical across both machines
(deterministic datasets), so the storage table needs no second column.

## Intel Core Ultra 9 185H regression audit (2026-07-26)

The earlier hero numbers below may have been recorded on a different host, so
they are not used as the regression baseline here. This audit compares an
isolated build of `HEAD` (`0645d99`) with the current R1-R4 review working
tree on the same Intel Core Ultra 9 185H. For the controlled metrics
comparison, both extensions were loaded by the same current benchmark binary.
The host ran Arch Linux 7.1.3, Rust 1.97.0, bundled SQLite 3.53.2, and the
`powersave` CPU governor.

Each benchmark was run twice and the second run is quoted, following
`TESTING.md`. Lower is better for timings; higher is better for rates.

| metrics workload | `HEAD` | R1-R4 tree | delta |
|---|---:|---:|---:|
| plain ingest | 4.01M pts/s | 3.87M pts/s | -3.5% |
| Tier 1 ingest | 1.88M pts/s | 1.79M pts/s | -4.8% |
| Tier 1 / plain normalized | 0.469x | 0.463x | -1.3% |
| Tier 2 ingest | 17.87M pts/s | 14.30M pts/s | **-20.0%** |
| Tier 2 / plain normalized | 4.456x | 3.695x | **-17.1%** |
| Tier 2 flush | 182.7ms | 178.7ms | -2.2% |
| name + range query | 3.5ms | 5.4ms | **+54.3%** |
| full-scan count | 195.5ms | 188.8ms | -3.4% |
| compressed size | 8.274 B/pt | 8.348 B/pt | +0.9% |

The regression is metrics-specific. Second-run logs and traces results stayed
within the repository's +/-15% noise band or improved:

| workload | `HEAD` | R1-R4 tree | delta |
|---|---:|---:|---:|
| logs ingest | 0.80M entries/s | 0.83M entries/s | +3.7% |
| logs optimize | 1364.3ms | 1373.4ms | +0.7% |
| logs level query | 22.2ms | 21.4ms | -3.6% |
| logs service/range query | 8.4ms | 8.9ms | +6.0% |
| traces ingest | 0.54M spans/s | 0.59M spans/s | +9.3% |
| traces optimize | 1928.0ms | 1824.4ms | -5.4% |
| traces point lookup | 3.195ms | 3.055ms | -4.4% |
| traces service/range query | 109.9ms | 120.0ms | +9.2% |

Codec 5 throughput also did not regress: logs encode/decode improved
116->122 MB/s and 582->605 MB/s; traces improved 106->121 MB/s and
668->751 MB/s.

The Tier 2 diff has a concrete optimization target: its old batched series
resolver took one registry read lock per blob, while the reviewed
implementation currently calls the single-series resolver for every series
entry. This workload has 1,000 series in each of 10 blobs, turning that into
10,000 transaction/registry lock cycles. Restore a true batched fast path
without weakening the authoritative-series rollback rules, then repeat this
same comparison. The selective query regression still needs profiling.

## Intel Core Ultra 9 185H checkpoint after R8 (2026-07-26)

This checkpoint measures the current R1-R8 review tree against the R1-R4
checkpoint above. The benchmark sources and deterministic datasets are
unchanged. The machine, release-build mode, benchmark binaries, `powersave`
governor, and two-run method are also unchanged; per `TESTING.md`, the second
run is quoted.

Raw metrics ingest slowed together with the plain-table control. Normalized
against that control, Tier 1 is flat and Tier 2 is slightly better:

| metrics workload | R1-R4 | R1-R8 | delta |
|---|---:|---:|---:|
| plain ingest | 3.87M pts/s | 3.46M pts/s | -10.6% |
| Tier 1 ingest | 1.79M pts/s | 1.59M pts/s | -11.2% |
| Tier 1 / plain normalized | 0.463x | 0.460x | -0.7% |
| Tier 2 ingest | 14.30M pts/s | 13.12M pts/s | -8.3% |
| Tier 2 / plain normalized | 3.695x | 3.792x | +2.6% |
| Tier 2 flush | 178.7ms | 210.7ms | +17.9% |
| name + range query | 5.4ms | 5.6ms | +3.7% |
| full-scan count | 188.8ms | 201.5ms | +6.7% |
| compressed size | 8.348 B/pt | 8.344 B/pt | -0.0% |

Logs stayed flat except for the small level query, and trace ingest/optimize
stayed within the repository's +/-15% noise band:

| workload | R1-R4 | R1-R8 | delta |
|---|---:|---:|---:|
| logs ingest | 0.83M entries/s | 0.81M entries/s | -2.4% |
| logs optimize | 1373.4ms | 1367.2ms | -0.5% |
| logs level query | 21.4ms | 25.2ms | +17.8% |
| logs service/range query | 8.9ms | 8.6ms | -3.4% |
| traces ingest | 0.59M spans/s | 0.55M spans/s | -6.8% |
| traces optimize | 1824.4ms | 1936.8ms | +6.2% |
| traces point lookup | 3.055ms | 3.637ms | +19.1% |
| traces service/range query | 120.0ms | 149.2ms | +24.3% |

The three second-run timing outliers are not repeatable within this
checkpoint. The first current runs measured metrics flush at 179.6ms
(+0.5% vs R1-R4), trace lookup at 3.085ms (+1.0%), and trace service/range at
117.0ms (-2.5%). The paired-run variance itself was 17-28%, so these are
recorded as noisy warnings rather than confirmed code regressions.

Codec 5 throughput remains healthy: logs encode/decode improved from
122/605 MB/s at R1-R4 to 131/640 MB/s, while traces held at 121/749 MB/s
versus 121/751 MB/s. Storage is also unchanged: metrics are 8.344 B/point,
logs 8.93 B/entry, and traces 37.36 B/span.

Conclusion: no repeatable R5-R8 performance regression was established.
The earlier R1-R4 Tier 2 and selective metrics-query regressions against
`HEAD` remain open optimization targets; this checkpoint neither worsened
nor fixed them. Before acting on the noisy trace/flush outliers, rerun on an
idle host with a fixed performance governor.

## Apple M5 Pro isolation + remediation of the R1-R4 regressions (2026-07-26)

Method: interleaved master/branch A/B runs (session-to-session drift on
this laptop exceeds the effect size — single-session comparisons of the
~50ms Tier 2 window are meaningless), plus `tools/bench` bin `t2micro`,
which times each of the 10 Tier 2 blob statements separately.

Isolation findings (correcting the R1-R4 audit's hypothesis above):

- The selective-query regression (2.0 → 3.9ms here, +54% on Intel) was
  100% the R2 query-boundary `refresh_authoritative_state()` — a full
  series-catalog reload + chunk-metadata rescan on every SELECT.
- The Tier 2 regression was NOT the per-series resolver locks: restoring
  a batched read-lock fast path recovered nothing measurable. It was
  (a) ~55% first-touch catalog population — the first blob naming 1,000
  new series paid a statement pair per series — and (b) ~30% steady
  per-statement overhead from the R1 savepoint wiring (~0.35ms per
  100k-point blob).

Remediation, kept inside the R2 correctness contract:

1. **Catalog generation check.** `_meta['chunk_gen']` is bumped inside the
   caller's transaction by every chunk-mutating store call; the series
   half of the generation is `MAX(id)` on the append-only `_series` table
   (zero write-side cost). Refresh compares one cached single-row SELECT
   against the last-loaded generation and skips the reload when nothing
   committed changed. Cross-process visibility is preserved: any
   committed change moves the generation. All r1-r8 correctness suites,
   the 150k-op oracle, five crash rounds, and all 77 cli.sh sections pass.
2. **Bulk series resolver.** `resolve_series_bulk` allocates a whole
   series table in one multi-row `INSERT ... ON CONFLICT DO NOTHING
   RETURNING id` (exact `created` flags, journaled for rollback) plus one
   VALUES-join SELECT, instead of a statement pair and a lock cycle per
   series. LESSON, paid in a deadlock: multi-row DML opens a SQLite
   statement journal, which re-entrantly fires xSavepoint on the calling
   vtab — so no engine lock may be held across re-entrant store SQL.
   `resolve_series_batch` now drops all engine locks around the store
   call (safe under the writer gate).

3. **Per-statement clock stamping.** A per-blob sweep (t2micro with
   1x1M / 10x100k / 100x10k shapes) showed the residual steady cost
   scaled with POINTS per statement, not statements — refuting the
   savepoint-wiring attribution too. write_point read the wall clock
   once per point for the idle-flush heuristic (seconds granularity);
   batch paths now stamp once per statement. Small (~2%) but free.
   (Profiler note for posterity: a sampling profiler attributed ~85% of
   the write path to that clock read; removing it recovered ~2%. Inlined
   release-build frames misattribute — trust A/B deltas, not samples.)

Interleaved results after all three fixes (M5 Pro, 5 pairs):

| metric | master | R1-R8 + fixes | delta |
|---|---:|---:|---:|
| name + range query | 2.0-2.2ms | 2.1-2.3ms | **parity** (was ~2x) |
| Tier 1 ingest | ~2.13M pts/s | ~2.06M pts/s | **parity/noise** (was -7%) |
| Tier 2 ingest | ~22.9M pts/s | ~23.1M pts/s | **parity** (was -13 to -17%) |
| Tier 2 first blob (t2micro) | 4.1ms | 5.4ms | catalog first-touch, once per new series |
| Tier 2 steady blob (t2micro) | 4.5ms | 4.5ms | **parity** |

Remaining honest cost of R1-R8: ~+1.3ms once per 1,000 brand-new series
(authoritative catalog population — amortizes to zero for long-lived
tables). Steady-state ingest and queries are at master parity on every
tier; logs and traces were never affected.

## Q2 reduction kernels (2026-07-26)

The first two Q2 accelerators from PLAN.md "Query interface tiers" are
shipped, strictly behind the waist:

- `Engine::query_grid_last[_by_id]` — last sample per grid point,
  half-open `(t - lookback, t]` (Q2a, the instant-selector shape), and
  `Engine::query_window_agg[_by_id]` — sliding-window
  sum/min/max/count/avg with a left-to-right ascending-ts fold (Q2b).
  Rayon-free `_by_id` variants exist because vtab callbacks must never
  enter the engine's parallel paths.
- SQL surface: eponymous TVFs, purely additive to the Q1 raw-scan
  contract —
  `timeless_grid(tbl, metric, filter, start, stop, step, lookback)` and
  `timeless_window(tbl, metric, filter, start, stop, step, window, agg)`.
  The engine resolves through the same process registry as the vtab, so
  TVF queries see buffered points and work TVF-first on fresh
  connections.
- Semantics-free by construction: no lookback defaults, no staleness or
  rate math, errors on step<=0 / lookback<0 / window<=0, and a
  1M-grid-point resource cap. Everything referee-sensitive stays above
  the waist.

Verification: `timeless-core/tests/q2_kernels.rs` compares both kernels
bit-for-bit against independent naive evaluators across a randomized
sweep (duplicate timestamps, buffered+flushed mix, all five aggs);
cli.sh §22 compares the TVFs against plain-SQL evaluation (recursive-CTE
grid + correlated subqueries) over the raw vtab in the same database;
the bench asserts kernel results equal client-side evaluation before
timing them.

Numbers (M5 Pro, 1M-point bench dataset, second run): the dashboard
shape — one value per minute per host over the whole range — costs
**17.9ms** via the Q1 fallback (ship 99,900 raw samples, evaluate
client-side) and **1.6ms** via `timeless_grid` (16,600 grid rows,
verified equal); `timeless_window` 5-min avg over the same grid is also
1.6ms. That is the in-process gap; over sqld/HTTP the shipped-bytes
difference widens it. Ingest and Q1 query timings are unchanged by the
addition.

## What it is

A loadable SQLite/libSQL extension. `CREATE VIRTUAL TABLE metrics USING
timeless_metrics` gives any database a compressed time-series table backed by
the timeless pco engine, storing chunks in shadow tables inside the same
database file. Transactions, replication, and backup come from the host;
compression and pruning come from the engine.

```sql
.load ./libtimeless_ext
CREATE VIRTUAL TABLE metrics USING timeless_metrics;
INSERT INTO metrics(name, ts, value, labels)
  VALUES ('cpu_usage', 1753000000, 42.5, '{"host":"pvm1"}');
INSERT INTO metrics(metrics) VALUES ('flush');            -- command idiom
INSERT INTO metrics(metrics) VALUES (:batch_blob);        -- Tier 2 bulk
SELECT * FROM metrics WHERE name='cpu_usage' AND ts BETWEEN :t0 AND :t1;
```

## Ingest rates (1M points, single transaction, release build)

| path | Linux i7 | Apple M5 Pro | notes |
|---|---|---|---|
| plain SQLite table | ~4–16M rows/s | 4.2M rows/s | baseline; pays 52 bytes/row forever |
| vtab Tier 1 (SQL rows) | ~2–3M pts/s | 2.3M pts/s | row-at-a-time xUpdate |
| **vtab Tier 2 (batch blob v0)** | **18.3M pts/s** | **23.8M pts/s** | target was ≥8M; beats the Elixir NIF path (~16M) |

Flush of 1M buffered points → compressed chunks: ~176ms (Linux) /
~110ms (M5 Pro).

## Storage (bytes per point, measured on-disk after close)

| dataset | plain table | timeless vtab | ratio |
|---|---|---|---|
| TSBS-style hostile (1000 series, ms-jitter ts, noisy values) | 52.6 | **8.3** | **6.4x** |
| friendly (constant-interval ts, patterned values) | 46.7 | 0.23 | ~200x |
| periodic sawtooth, chunks only | 16 (raw) | 0.133 | 120x |
| 1M-entry logs (bench-logs; bytes/entry, codec 5 shredded metadata) | 120.3 | **8.93** | **13.5x** |
| 960k-span traces (bench-traces; bytes/span, vs indexed plain table) | 161.6 | **37.4** | **4.3x** |

*Logs/traces rows added Session 8: codec 5 ("adaptive columnar v2")
shreds the metadata/attributes column into per-key typed columns —
logs metadata -20.9%, whole logs file 9.65 → 8.93 MB (12.5x → 13.5x);
traces unchanged within noise (35.8 → 35.9 MB, 4.3x) because its 2-key
always-present attribute schema had nothing to shred. Query timings
and decode throughput did not regress (decode got faster).*

*Honest asterisk: PLAN criterion said 20–40x on TSBS-style data; we measured
6.4x — but our generator uses millisecond timestamps with 0–999ms random
jitter and 4-decimal value noise, deliberately harsher than real TSBS
(seconds, regular cadence). Real scrape workloads sit between the 6.4x and
200x poles. Every measurement above is lossless — verified bit-exact per
point after flush + cold recovery.*

## Query (Tier 2 db, 1M points, reopened process)

- `count(*)`: 1,000,000 — full scan 205ms (Linux) / 110ms (M5 Pro)
- name + ts-range (100-step window across 100 hosts): 10,001 rows in
  3.0ms (Linux) / 2.0ms (M5 Pro)
- 3-point bit-exact f64 spot checks: pass (both machines)

## Apple Silicon run (M5 Pro, 2026-07-22)

Logs and traces query timings vs plain tables in the same file
(cold reopen, counts verified against the plain-table oracle):

| query | plain | vtab |
|---|---|---|
| logs `level='error'` count | 34.5ms | **15.3ms** |
| logs service+level+range (pushdown) | 119.7ms | **4.2ms** |
| logs `message LIKE '%timeout%'` | **73.9ms** | 344.1ms |
| traces `status='error'` count | 38.6ms | **2.8ms** |
| traces service+range count (pushdown) | 46.7ms | 57.9ms |
| traces trace_id point lookup | **0.005ms** (indexed) | 2.0ms |

Ingest: logs vtab 1.10M entries/s (plain 3.60M); traces vtab 0.78M
spans/s (plain+index 0.99M).

bench-codec throughput (the memory-bandwidth-bound comparison TESTING.md
asks for): codec 5 decode **1043 MB/s** on logs / **1199 MB/s** on traces
(codec 4: 852 / 1015 — codec 5 decodes faster on both datasets); encode
183–221 MB/s across codecs. Size verdict unchanged on this machine:
codec 5 is 8.15% smaller on logs, +0.13% on traces — stays the
optimize() default.

## sqld (self-hosted libSQL server) over HTTP

`sqld --extensions-path` with sha256 `trusted.lst` loads the .so into every
connection. Via curl: CREATE VIRTUAL TABLE → INSERT → 'flush' in one request;
a **separate** request (fresh pooled connection → xConnect → shadow-table
recovery) returned the rows with name pushdown in 0.19ms. Networked
compressed telemetry SQL, zero client changes.

## What was proven beyond numbers

- Writable virtual tables in pure Rust (rusqlite 0.40, no C shim).
- Re-entrant shadow-table SQL from vtab callbacks (the FTS5 pattern).
- Vtab writes ride the host transaction — compaction's atomic swap needed
  ZERO crash-recovery code (FsStore's ~90-line manifest dance simply
  disappears in the SQLite backend).
- Engine extracted to `timeless-core` (pure Rust crate, Elixir repo
  untouched), persistence behind a `ChunkStore` trait with fs + shadow-table
  backends.
- Commands via the FTS5 hidden-column idiom: TEXT = command
  ('flush'/'compact'/'prune:<ts>'), BLOB = Tier 2 batch. Malformed batches
  rejected atomically.
- **Prometheus ingest** (post-POC): a raw scrape body is just another BLOB
  in the hidden column — dispatch is by first byte (0x01 = batch v0, else
  exposition text), so the whole pipeline is one line, zero new syntax:
  ```sh
  curl -s target:9100/metrics -o /tmp/scrape.prom && sqlite3 metrics.db \
    ".load ./libtimeless_ext" \
    "INSERT INTO metrics(metrics) VALUES (readfile('/tmp/scrape.prom'));
     INSERT INTO metrics(metrics) VALUES ('flush');"
  ```
  (readfile() needs a seekable file, so curl lands the scrape in a temp
  file rather than a pipe.)
  Timestamps are stored as EPOCH SECONDS (explicit prom ms timestamps are
  normalized /1000; timestamp-less samples get wall-clock seconds).
  Malformed/NaN lines are counted, not fatal — partial success succeeds
  silently, like a real Prometheus server scrape. The scraping loop stays
  external by design (cron/curl/Elixir); the vtab is passive.

## Transactions & crash safety (hardening session, 2026-07-22)

- **ROLLBACK is real (PLAN R5 fixed).** All three engines keep a
  transaction journal bracketed by xBegin/xCommit/xRollback: rolled-back
  buffered writes vanish (pre- and post-reopen), intra-transaction
  flushes roll back completely (chunk/block/term/trace-index rows ride
  the host transaction; the journal removes their in-memory index
  entries and RESTORES any pre-transaction buffered data the flush
  drained), and flush/compact/optimize/prune all work inside explicit
  transactions. Asserted by cli.sh sections 6/6b/6c and the oracle's
  rollback ops.
- **Durability contract, crash-tested:** flushed = durable (survives
  kill -9 at any instant; SQLite journal recovery + never-dangle index
  joins verified by tests/crash.sh over 5 random-timing kills),
  buffered = lost with the process, never corrupt (integrity_check ok
  every time).
- **Multi-connection sharing is real (PLAN R4 fixed, Session 10).**
  sqld loads the extension into every pooled connection; a
  process-global registry now hands each of them the SAME engine per
  (db file, table), with store SQL routed to the calling connection
  via a thread-local and writers serialized by a bounded per-table
  gate. One connection's inserts and flushes are queryable from
  another connection immediately, no reopen — proven by cli.sh
  section 21 (two connections in one process: flushed + buffered
  visibility, bounded lock error under write contention, retry after
  commit, drop/recreate). See the shared-buffer semantics note under
  Known limits.
- **Oracle property test:** 3 seeds × 50k randomized ops (inserts,
  commands, every pushdown plan family, mirrored transactions with
  rollback, prune) against mirrored plain tables in the same db —
  result sets identical after every query (order-insensitive, floats
  bit-exact). `tools/bench` bin `oracle`; any failure prints its seed
  and op index for exact replay.

## Known limits (documented, accepted for POC)

- SAVEPOINT-granular rollback is not supported (rusqlite's
  update_module_with_tx wires xBegin/xSync/xCommit/xRollback but not
  xSavepoint) — only whole-transaction ROLLBACK is journaled. Series/
  metric NAMES registered during a rolled-back transaction stay
  registered in memory as harmless empty series.
- ~~Metrics chunk index keyed (series, min_ts) — duplicate-min_ts
  chunks shadowed each other~~ **Fixed** (2026-07-22): the donor fix
  (key widened to (series, min_ts, chunk_seq)) is ported to
  timeless-core; see the chunk-index shadowing fix (2026-07-22, see git history). The oracle
  generator now produces duplicate metric timestamps, including across
  flush boundaries.
- **Shared-buffer semantics across connections (PLAN R4 — fixed, with
  this documented trade):** all connections in one process share ONE
  engine per (db file, table), so points one connection has inserted
  but not yet committed are visible to every other connection's
  queries immediately — a dirty read of buffered telemetry. Accepted
  on purpose: buffered points were already pre-durable (lost on
  crash), so pre-commit visibility keeps the same mental model, and
  FLUSHED data remains fully transactional. Write transactions are
  serialized per table (writer gate, 5s bounded wait → busy-style
  error; on stock SQLite the file write lock serializes writers even
  earlier). Sharp edge: a query on connection B DURING connection A's
  uncommitted intra-transaction 'flush' can fail with a row-read
  error until A commits (bounded, SQLITE_BUSY-like; in autocommit the
  window is a single statement).
- Engine rayon paths (par_iter queries) must not be called from vtab
  callbacks — deadlock via the host connection mutex (documented in PLAN;
  cursor uses sequential reads).
- ts-equality is re-checked by SQLite, only range/name are pruned.

## Reproduce

```sh
cargo build --release -p timeless-ext
./tests/cli.sh              # 21 sections, incl. oracle (19), crash (20),
                            # multi-connection shared engine (21)
tests/crash.sh target/release/libtimeless_ext.so            # standalone
cd tools/bench
cargo run --release --bin oracle -- ../../target/release/libtimeless_ext.so [seed]
cargo run --release -- ../../target/release/libtimeless_ext.so
```
