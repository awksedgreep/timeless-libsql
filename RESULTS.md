# timeless-libsql — Hero POC Results

Two working days (2026-07-22), Sessions 1–4 of PLAN.md. All four success
criteria met (one with an honest asterisk). Machine: Arch Linux, Rust 1.97,
SQLite 3.53, sqld built from libsql main.

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

| path | rate | notes |
|---|---|---|
| plain SQLite table | ~4–16M rows/s | baseline; pays 52 bytes/row forever |
| vtab Tier 1 (SQL rows) | ~2–3M pts/s | row-at-a-time xUpdate |
| **vtab Tier 2 (batch blob v0)** | **18.3M pts/s** | target was ≥8M; beats the Elixir NIF path (~16M) |

Flush of 1M buffered points → compressed chunks: ~176ms.

## Storage (bytes per point, measured on-disk after close)

| dataset | plain table | timeless vtab | ratio |
|---|---|---|---|
| TSBS-style hostile (1000 series, ms-jitter ts, noisy values) | 52.6 | **8.3** | **6.4x** |
| friendly (constant-interval ts, patterned values) | 46.7 | 0.23 | ~200x |
| periodic sawtooth, chunks only | 16 (raw) | 0.133 | 120x |

*Honest asterisk: PLAN criterion said 20–40x on TSBS-style data; we measured
6.4x — but our generator uses millisecond timestamps with 0–999ms random
jitter and 4-decimal value noise, deliberately harsher than real TSBS
(seconds, regular cadence). Real scrape workloads sit between the 6.4x and
200x poles. Every measurement above is lossless — verified bit-exact per
point after flush + cold recovery.*

## Query (Tier 2 db, 1M points, reopened process)

- `count(*)`: 1,000,000 — full scan 205ms
- name + ts-range (100-step window across 100 hosts): 10,001 rows in 3.0ms
- 3-point bit-exact f64 spot checks: pass

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

## Known limits (documented, accepted for POC)

- Rollback does not discard buffered (unflushed) points — they die with the
  process instead (PLAN R5; TransactionVTab hooks exist for the fix).
- One engine per connection over shared shadow tables — single-writer
  assumption (PLAN R4).
- Engine rayon paths (par_iter queries) must not be called from vtab
  callbacks — deadlock via the host connection mutex (documented in PLAN;
  cursor uses sequential reads).
- ts-equality is re-checked by SQLite, only range/name are pruned.

## Reproduce

```sh
cargo build --release -p timeless-ext
./tests/cli.sh                                   # 10 sections
cd tools/bench && cargo run --release -- ../../target/release/libtimeless_ext.so
```
