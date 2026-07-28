# User's Guide

**For SQL users who want compressed, fast telemetry inside SQLite — and don't
want to learn a new system to get it.**

Everything here is plain SQL. If you can write `INSERT` and `SELECT`, you can
use this extension. No query DSL, no server, no config files.

- [1. Install and load](#1-install-and-load)
- [2. Five-minute tour](#2-five-minute-tour)
- [3. The one concept you must know: flush](#3-the-one-concept-you-must-know-flush)
- [4. Storing metrics](#4-storing-metrics)
- [5. Storing logs](#5-storing-logs)
- [6. Storing traces](#6-storing-traces)
- [7. Querying: what's fast, what's slow](#7-querying-whats-fast-whats-slow)
- [8. Housekeeping: optimize, compact, prune](#8-housekeeping-optimize-compact-prune)
- [9. Common errors and what they mean](#9-common-errors-and-what-they-mean)
- [10. Monitor the database itself: dbhealth](#10-monitor-the-database-itself-dbhealth)
- [11. Cheat sheet](#11-cheat-sheet)

---

## 1. Install and load

Build once:

```sh
cargo build --release -p timeless-ext
# produces: target/release/libtimeless_ext.so   (.dylib on macOS)
```

Load it like any SQLite extension:

```sh
# sqlite3 CLI
sqlite3 mydata.db
sqlite> .load ./target/release/libtimeless_ext
```

```sql
-- or from SQL, if your client allows it
SELECT load_extension('./target/release/libtimeless_ext');
```

That's it. The extension registers three new table types you can use in
`CREATE VIRTUAL TABLE`: `timeless_metrics`, `timeless_logs`, and
`timeless_traces`.

> **macOS note:** Apple's built-in `/usr/bin/sqlite3` refuses to load
> extensions (".load" fails with *not authorized*). Install a real one with
> `brew install sqlite` and use `$(brew --prefix sqlite)/bin/sqlite3`.

Everything lives inside your existing `.db` file — same transactions, same
backups, same replication. There is nothing else to run.

## 2. Five-minute tour

```sql
.load ./target/release/libtimeless_ext

-- Create a metrics table. No arguments needed.
CREATE VIRTUAL TABLE metrics USING timeless_metrics;

-- Insert rows exactly like a normal table.
INSERT INTO metrics(name, ts, value, labels) VALUES
  ('cpu_usage', 1753000000, 42.5, '{"host":"web1"}'),
  ('cpu_usage', 1753000015, 43.1, '{"host":"web1"}'),
  ('cpu_usage', 1753000030, 41.9, '{"host":"web1"}');

-- Compress what's buffered to disk (more on this in section 3).
INSERT INTO metrics(metrics) VALUES ('flush');

-- Query with ordinary SQL.
SELECT name, ts, value FROM metrics
 WHERE name = 'cpu_usage' AND ts >= 1753000015;

-- Aggregate with ordinary SQL.
SELECT min(value), avg(value), max(value)
  FROM metrics WHERE name = 'cpu_usage';
```

What you get for that: on hostile real-world data, **~6x less disk** than a
plain table (up to ~200x on regular, well-behaved data), and time-range /
name filters that skip straight to the matching compressed chunks instead of
scanning everything.

The odd-looking line is this one:

```sql
INSERT INTO metrics(metrics) VALUES ('flush');
```

That's not a data insert — it's a **command**. Every timeless table has a
hidden column named after the table itself, and inserting a string into it
runs a maintenance command (`flush`, `optimize`, `compact`, `prune:<ts>`).
This is the same idiom SQLite's own FTS5 uses. It looks strange the first
time and becomes muscle memory the second.

## 3. The one concept you must know: flush

Inserts don't compress immediately — they land in an **in-memory buffer**:

- Buffered rows are **immediately queryable**. `SELECT` sees them right away;
  you never have to flush just to read your own writes.
- Buffered rows are **not yet durable**. If the process exits, buffered
  (unflushed) rows are gone. Flushed rows are safe — this is a hard
  guarantee, proven with `kill -9` crash testing. You lose at most the
  buffer; you never get corruption.
- `'flush'` compresses the buffer into blocks inside the database file,
  riding whatever transaction you're in.
- The buffer also **auto-flushes** when it gets big (4,096 points per series
  for metrics; 8,192 entries for logs and traces), so you can't grow it
  forever by forgetting.

The rule of thumb:

> **Flush at the cadence at which you're willing to lose data.**

An app that logs continuously might flush every few seconds or after each
batch. A cron job that ingests a file should flush at the end. If you're
inside a `BEGIN ... COMMIT`, flush before the commit and both the data and
the flush commit (or roll back) together.

One more property of the storage model: tables are **append-only**.
`UPDATE` and `DELETE` are rejected. Retention is explicit and cheap — see
[prune](#8-housekeeping-optimize-compact-prune).

## 4. Storing metrics

```sql
CREATE VIRTUAL TABLE metrics USING timeless_metrics;
```

| column | type | required | notes |
|---|---|---|---|
| `name` | TEXT | yes | metric name, e.g. `'cpu_usage'` |
| `ts` | INTEGER | yes | unix timestamp in **seconds** |
| `value` | REAL | yes | the measurement |
| `labels` | TEXT | no | flat JSON object of strings, e.g. `'{"host":"web1"}'` |

Each distinct `(name, labels)` pair is a separate series, compressed
independently — so `cpu_usage{host=web1}` and `cpu_usage{host=web2}` don't
pollute each other's compression.

`labels` must be a **flat JSON object with string values**:
`'{"host":"web1","region":"us-east"}'` is fine; numbers, booleans, or nested
objects are rejected with an error. Store numbers as strings if you need
them in labels.

### Ingesting a Prometheus scrape directly

If you already have Prometheus-format exporters, you can skip row-by-row
INSERTs entirely — a raw scrape body is valid input:

```sh
curl -s myhost:9100/metrics -o /tmp/scrape.prom
sqlite3 mydata.db \
  ".load ./target/release/libtimeless_ext" \
  "INSERT INTO metrics(metrics) VALUES (readfile('/tmp/scrape.prom'));
   INSERT INTO metrics(metrics) VALUES ('flush');"
```

Malformed lines are counted and skipped, not fatal — same behavior as a real
Prometheus server. Run that from cron and you have a metrics pipeline in one
file.

(There is also a high-throughput packed binary format — ~24M points/sec — for
programs that batch off the hot path; see the Tier 2 spec in
[PLAN.md](../PLAN.md). For hand-written SQL you won't need it.)

### Typical queries

```sql
-- Latest values in a window
SELECT ts, value FROM metrics
 WHERE name = 'cpu_usage' AND ts >= 1753000000 AND ts < 1753003600;

-- Downsample to 5-minute averages: plain GROUP BY
SELECT (ts / 300) * 300 AS bucket, avg(value)
  FROM metrics
 WHERE name = 'cpu_usage' AND ts >= 1753000000
 GROUP BY bucket ORDER BY bucket;

-- Filter by label (labels is JSON text — use SQLite's json functions)
SELECT ts, value FROM metrics
 WHERE name = 'cpu_usage'
   AND json_extract(labels, '$.host') = 'web1';
```

`name = ...` and `ts` ranges are pushed down into the storage engine — only
matching chunks are decompressed. Label filters via `json_extract` are
applied after decompression, so always include `name` and a `ts` range when
you can.

## 5. Storing logs

Here you make one decision at table-creation time: **which metadata keys do
you want to filter on quickly?** Declare them as `index_keys`:

```sql
CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service,path,status');
```

| column | type | required | notes |
|---|---|---|---|
| `ts` | INTEGER | yes | unix timestamp in **milliseconds** |
| `level` | TEXT | yes | exactly one of `debug`, `info`, `warning`, `error` |
| `message` | TEXT | yes | the log line |
| `metadata` | TEXT | no | flat JSON object of strings |
| *(each index key)* | TEXT | hidden | usable in SELECT, WHERE, and INSERT |

Every `index_keys` key becomes a real (hidden) column. You can filter on it,
select it, and even insert through it:

```sql
-- Insert with metadata JSON...
INSERT INTO logs(ts, level, message, metadata) VALUES
  (1753000000123, 'error', 'payment declined: card_expired',
   '{"service":"payments","path":"/api/charge","status":"402"}');

-- ...or use the index-key columns as insert shorthand (merged into metadata):
INSERT INTO logs(ts, level, message, service, path, status) VALUES
  (1753000000456, 'info', 'charge ok', 'payments', '/api/charge', '200');

INSERT INTO logs(logs) VALUES ('flush');

-- Fast: every predicate here is index-accelerated
SELECT ts, level, message FROM logs
 WHERE service = 'payments' AND level = 'error' AND ts > 1753000000000;
```

Equality filters on `level` and on index keys intersect an inverted index —
only blocks that can match are decompressed. That's the difference between
the 4ms query and the 120ms query in the [benchmarks](../README.md#numbers).

Choosing index keys:

- Pick keys you'll put in `WHERE` clauses: `service`, `host`, `status`,
  `env` — low-to-medium cardinality identifiers.
- Don't index high-cardinality values like `request_id`; keep those in
  `metadata` (they'll still be stored and returned, just not
  filter-accelerated).
- Up to 28 keys per table. The list is fixed at creation and persisted;
  to change it, create a new table.

Full-text search works but is honest about its cost:

```sql
SELECT * FROM logs WHERE message LIKE '%timeout%';   -- works, but scans
```

`LIKE` on `message` decompresses every block. Fine for occasional forensics
on a bounded `ts` range; add `AND ts > ...` to keep it cheap. If substring
search is your *primary* workload, a plain table with FTS5 is the better
tool — this table is optimized for dimensional filtering and compression.

## 6. Storing traces

OpenTelemetry-shaped spans, fixed schema, no arguments:

```sql
CREATE VIRTUAL TABLE traces USING timeless_traces;
```

| column | type | required | notes |
|---|---|---|---|
| `trace_id` | BLOB/TEXT | yes | 16-byte BLOB or 32-char hex TEXT |
| `span_id` | BLOB/TEXT | yes | 8-byte BLOB or 16-char hex TEXT |
| `parent_span_id` | BLOB/TEXT | no | NULL = root span |
| `name` | TEXT | yes | span name, e.g. `'GET /api/charge'` |
| `service` | TEXT | yes | emitting service |
| `kind` | TEXT | no | `internal` (default), `server`, `client`, `producer`, `consumer` |
| `status` | TEXT | no | `unset` (default), `ok`, `error` |
| `start_ts` | INTEGER | yes | unix timestamp in **nanoseconds** |
| `duration_ns` | INTEGER | no | span duration in ns (default 0) |
| `attributes` | TEXT | no | flat JSON object of strings |

Ids are flexible on the way in — OTel tooling hands you hex strings, so hex
TEXT and packed BLOBs are both accepted. On the way out they're always
BLOBs; wrap in `hex()` for display:

```sql
INSERT INTO traces(trace_id, span_id, name, service, kind, status, start_ts, duration_ns)
VALUES ('4bf92f3577b34da6a3ce929d0e0e4736', '00f067aa0ba902b7',
        'GET /api/charge', 'payments', 'server', 'error',
        1753000000123000000, 8500000);
INSERT INTO traces(traces) VALUES ('flush');

-- The hero query: reassemble one trace
SELECT hex(span_id), hex(parent_span_id), name, service, duration_ns
  FROM traces
 WHERE trace_id = x'4bf92f3577b34da6a3ce929d0e0e4736'
 ORDER BY start_ts;
```

Trace lookup goes through a dedicated trace-id index — only blocks that
contain that trace are touched. Equality on `service`, `name`, `kind`,
`status` and ranges on `start_ts` are also pushed down:

```sql
-- All failed spans for one service in a window
SELECT hex(trace_id), name, duration_ns FROM traces
 WHERE service = 'payments' AND status = 'error'
   AND start_ts >= 1753000000000000000;

-- Slowest operations, plain SQL
SELECT name, count(*), avg(duration_ns) / 1e6 AS avg_ms
  FROM traces WHERE service = 'payments'
 GROUP BY name ORDER BY avg_ms DESC LIMIT 10;
```

## 7. Querying: what's fast, what's slow

You don't need to learn a query planner — just know which predicates the
storage engine can use to *skip* decompression:

| table | accelerated predicates |
|---|---|
| metrics | `name = ...`, `ts` ranges (`>`, `>=`, `<`, `<=`, `BETWEEN`) |
| logs | `level = ...`, any index-key `= ...`, `ts` ranges |
| traces | `trace_id = ...`, `service`/`name`/`kind`/`status` `= ...`, `start_ts` ranges |

Everything else still *works* — it's ordinary SQL over the decompressed rows
— it just reads more blocks. Practical guidance:

- **Always constrain the time range** when you can. It's the universal
  pruner on all three tables.
- **Aggregations are just SQL.** `count`, `avg`, `min/max`, `GROUP BY`,
  window functions — no special syntax, and they benefit from the same
  pushdown as row queries.
- **Joins work.** These are real tables to SQLite; join telemetry against
  your application tables in the same file.
- Mind the timestamp units — they follow each signal's native convention:
  metrics = **seconds**, logs = **milliseconds**, traces = **nanoseconds**.
- Two things a compressed store will never win: unbounded
  `LIKE '%substring%'` scans, and single-row point lookups against a B-tree
  index. If a query is one of those two and it's hot, that's what plain
  tables are for.

When row order matters, say so with `ORDER BY ts` — metrics results come back
time-ordered, but logs and traces blocks can overlap, and it's ordinary cheap
SQL either way. Don't rely on `rowid`; it's synthetic and only stable within
a single scan.

## 8. Housekeeping: optimize, compact, prune

All commands are the same idiom: insert a string into the table's hidden
command column. All are safe to run anytime, transactional, and idempotent.

```sql
INSERT INTO metrics(metrics) VALUES ('flush');      -- persist the buffer (all tables)
INSERT INTO metrics(metrics) VALUES ('compact');    -- metrics: merge small chunks
INSERT INTO logs(logs)       VALUES ('optimize');   -- logs/traces: re-encode into
INSERT INTO traces(traces)   VALUES ('optimize');   --   larger, better-compressed blocks
```

- **`flush`** — you know this one (section 3).
- **`optimize` / `compact`** — frequent small flushes create many small
  blocks, which cost a little disk and read speed. Run this occasionally
  (e.g. daily, or after a big backfill) to merge them into optimally
  compressed blocks. Never required for correctness.
- **`prune:<ts>`** — retention. Drops all data older than the timestamp,
  which is given in *that table's* unit:

```sql
-- keep 30 days of metrics (ts in seconds)
INSERT INTO metrics(metrics) VALUES ('prune:' || (unixepoch() - 30*86400));

-- keep 7 days of logs (ts in milliseconds)
INSERT INTO logs(logs) VALUES ('prune:' || ((unixepoch() - 7*86400) * 1000));

-- keep 72 hours of traces (ts in nanoseconds)
INSERT INTO traces(traces) VALUES ('prune:' || ((unixepoch() - 72*3600) * 1000000000));
```

Prune deletes whole compressed blocks, so it's fast and doesn't fragment the
file. It only removes blocks entirely older than the cutoff — a block
straddling the boundary is kept, so retention is "at least this much", which
is what you want. There are no background jobs by design: put a prune (and
maybe an optimize) in the same cron job that does your backups, and you have
a complete retention story in two lines of SQL.

Dropping a table cleans up after itself — `DROP TABLE metrics` removes all
of its shadow storage.

## 9. Common errors and what they mean

| error | cause / fix |
|---|---|
| `not authorized` on `.load` | macOS system sqlite3 — see [section 1](#1-install-and-load) |
| `name is required (TEXT)` / `ts is required (INTEGER)` / `value is required (REAL)` | metrics insert with a NULL/missing required column |
| `level is required (TEXT: debug\|info\|warning\|error)` | logs `level` is NULL or not one of the four exact words — vocabularies are strict, `'warn'` and `'ERROR'` are rejected, not coerced |
| `trace_id is required (16-byte BLOB or 32-char hex TEXT)` | wrong id length/format; same pattern for `span_id` (8 bytes / 16 hex chars) |
| flat-JSON errors on `labels` / `metadata` / `attributes` | value must be a flat JSON object with **string** values only — no numbers, booleans, nesting |
| errors on `UPDATE` / `DELETE` | by design; the store is append-only — use `prune:<ts>` for retention |
| unknown command string | typo in a command insert, e.g. `'optimise'` — commands are exact: `flush`, `compact` (metrics), `optimize` (logs/traces), `prune:<ts>` |

Two behaviors that surprise people but are intentional:

- **Rows visible before flush** — inserts are queryable immediately from any
  connection in the same process, even before `'flush'` or `COMMIT`. Flushed
  data is fully transactional.
- **Rows gone after a crash** — anything inserted but not yet flushed (or
  auto-flushed) is lost if the process dies. Never corrupted, just not yet
  durable. Flush at the cadence you can afford to lose.

## 10. Monitor the database itself: dbhealth

The extension can also store SQLite's *own* health history — cache hit
rates, bloat, WAL growth, memory — compressed, inside the same file:

```sql
-- with libdbhealth_ext.so loaded (the standalone health extension):
CREATE VIRTUAL TABLE dbhealth USING dbhealth;
-- collection has begun: a background sampler records db-level gauges
-- every 60s (every=N to tune, every=0 for manual-only), and resumes
-- whenever the database is reopened.

-- Your app's heartbeat adds the connection-level series (cache hit
-- ratio and friends) that a background sampler cannot see:
INSERT INTO dbhealth(dbhealth) VALUES ('sample');
-- NOTE: under sqld the built-in sampler stays off (replication
-- safety) — collection is one cron line; copy it from DBHEALTH.md
-- ("Collecting under sqld: the exact cron line").

-- It reads like any metrics table:
SELECT ts, value FROM dbhealth
 WHERE name = 'cache_hit_ratio' AND ts > unixepoch() - 86400;

-- Is the file bloating? Should you vacuum? Ask the data:
SELECT max(value) FROM dbhealth
 WHERE name = 'bloat_ratio' AND ts > unixepoch() - 7*86400;
```

And you don't have to know what any of the numbers *mean* — creating the
table also creates three views, and **`dbhealth_report`** is the one to
remember. One row per health check, worst first, with a concrete action:

```
sqlite> SELECT * FROM dbhealth_report;
check                 status   value       advice
--------------------  -------  ----------  ------------------------------------
cache_hit_ratio_24h   warn     0.835       page cache misses are high; raise
                                           PRAGMA cache_size
cache_spills_24h      warn     5           transactions overflow the page cache
                                           mid-write; raise PRAGMA cache_size
bloat                 ok       0% free pages   —
sampling              ok       0s ago          —
wal_size              ok       0.0 MB (db 0.0 MB)  —
...
```

The thresholds live in plain view SQL (inspect them with `.schema
dbhealth_report`), so if you disagree with an opinion you can define your
own variant. `dbhealth_now` gives the latest value per series with its
age, and `dbhealth_trends` gives daily min/avg/max per series over the
last week — "is this getting worse?" as a `SELECT`.

Each `'sample'` records ~16 series (`cache_hits`, `cache_misses`,
`cache_hit_ratio`, `cache_spills`, `db_pages`, `freelist_pages`,
`bloat_ratio`, `db_file_bytes`, `wal_file_bytes`, memory gauges, …) and
by default flushes immediately, so a cron-driven `sqlite3` one-liner is
durable. Storage is negligible — health data compresses to ~0.23 bytes
per point, about 2 MB for a *year* of 1-minute samples; run `'compact'`
occasionally (same cron) to merge the small chunks.

Two things to know: cache/counter *deltas* describe the connection that
issued `'sample'`, so one-shot CLI sampling records the 9 database-level
gauges while the full 16-series inventory (including `cache_hit_ratio`)
needs a connection that samples more than once — an app timer, or several
samples in one script. And if your app holds the connection open, create
the table with `flush_every=20` to make sampling ~100x cheaper in
exchange for a bounded loss window. Full details, design, and the metric
inventory: [DBHEALTH.md](DBHEALTH.md).

## 11. Cheat sheet

```sql
.load ./libtimeless_ext

-- CREATE
CREATE VIRTUAL TABLE metrics  USING timeless_metrics;
CREATE VIRTUAL TABLE logs     USING timeless_logs(index_keys='service,host,status');
CREATE VIRTUAL TABLE traces   USING timeless_traces;
CREATE VIRTUAL TABLE dbhealth USING timeless_health;   -- the db monitors itself

-- INSERT                         units:
INSERT INTO metrics(name, ts, value, labels) VALUES (:n, :s,  :v, :json);      -- ts: seconds
INSERT INTO logs(ts, level, message, metadata) VALUES (:ms, :lvl, :msg, :json);-- ts: milliseconds
INSERT INTO traces(trace_id, span_id, name, service, start_ts) VALUES (...);   -- ts: nanoseconds

-- COMMANDS (INSERT INTO <table>(<table>) VALUES ...)
'flush'        -- persist buffer; do this at your loss-tolerance cadence
'optimize'     -- logs/traces: merge + recompress blocks (occasionally)
'compact'      -- metrics/dbhealth: merge small chunks (occasionally)
'prune:<ts>'   -- retention; ts in the table's own unit (s / ms / ns)
'sample'       -- dbhealth only: snapshot SQLite health counters now

-- QUERY (accelerated predicates)
... WHERE name = 'cpu' AND ts BETWEEN :a AND :b                 -- metrics
... WHERE service = 'api' AND level = 'error' AND ts > :t       -- logs
... WHERE trace_id = x'...'                                     -- traces
... WHERE service = 'api' AND status = 'error' AND start_ts > :t

-- Vocabularies (strict, lowercase)
level:  debug | info | warning | error
kind:   internal | server | client | producer | consumer
status: unset | ok | error
```

---

*Want this over the network? [SQLD.md](SQLD.md) serves these same tables
over HTTP with self-hosted sqld — everything in this guide works verbatim
through it, from any language ([interactive tour](tour.livemd)). Want the
internals — how compression works, benchmark methodology, the durability
proof? See the [README](../README.md), [RESULTS.md](../RESULTS.md), and
[TESTING.md](../TESTING.md).*
