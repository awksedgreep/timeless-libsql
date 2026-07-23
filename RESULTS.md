# timeless-libsql — Hero POC Results

Two working days (2026-07-22), Sessions 1–4 of PLAN.md. All four success
criteria met (one with an honest asterisk). Reference machine: Arch Linux,
Rust 1.97, SQLite 3.53, sqld built from libsql main. Re-run 2026-07-22 on
Apple M5 Pro (macOS 26.5, Rust 1.97.1, bundled SQLite) — second-run numbers
per TESTING.md; on-disk sizes were byte-identical across both machines
(deterministic datasets), so the storage table needs no second column.

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
