# User's Guide

**For SQL users who want compressed, fast telemetry inside SQLite — and don't
want to learn a new system to get it.**

Everything here is plain SQL. If you can write `INSERT` and `SELECT`, you can
use this extension. No query DSL, no server, no config files.

Use this guide for the walkthrough and the
[SQLite extension API reference](SQL_API_REFERENCE.md) for the canonical
module, schema, hidden-input, command, batch, frame, and compatibility
contracts.

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

`v0.7.6` passed the complete four-platform archive and outer-checksum matrix
and published the archives plus `SHA256SUMS` as permanent GitHub Release
assets. Build from source or follow the exact current download status in the
[artifact guide](ARTIFACTS.md).

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
whole-database backups, and the host's supported SQLite/libSQL replication.
There is nothing else to run for the SQL-only path.

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
- After commit, other connections in the same process see buffered rows through
  the shared engine without a flush. During your active write transaction they
  receive a retryable busy-style error instead of seeing uncommitted shadow
  rows; retry the `SELECT` as you would `SQLITE_BUSY`.
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

The current metric sample type is exactly one IEEE-754 float per timestamp.
Check it directly when embedding:

```sql
SELECT json_extract(
  timeless_capabilities(),
  '$.signals.metrics.sample_types'
); -- ["float64"]
```

The same capability reports `native_histograms=false`. Classic Prometheus
histograms remain ordinary `_bucket`, `_sum`, and `_count` float series; the
extension does not reinterpret them as typed native histograms. A future
native-histogram format must be separately versioned and capability-advertised.

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
Prometheus server. Standard label-value `\n`, `\"`, and `\\` escapes are
decoded before the `(name, labels)` series identity is resolved. Run that from
cron and you have a metrics pipeline in one file.

(There is also a high-throughput packed binary format — ~24M points/sec — for
programs that batch off the hot path; see the Tier 2 spec in
[the SQL API reference](SQL_API_REFERENCE.md#ingestion-batch-formats). For
hand-written SQL you won't need it.)

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
| `level` | TEXT | yes | exactly one of `debug`, `info`, `notice`, `warning`, `error`, `critical`, `alert`, `emergency` |
| `message` | TEXT | yes | the log line |
| `metadata` | TEXT | no | canonical typed JSON object; nested objects, arrays, strings, numbers, booleans, and null survive |
| *(each index key)* | TEXT | hidden | usable in SELECT, WHERE, and INSERT |
| `message_contains` | TEXT | hidden query input | exact case-insensitive literal substring, evaluated before rows cross SQLite |
| `max_work_entries` | INTEGER | hidden query input | optional positive cap on buffered/candidate entries examined before decode |

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
the measured query tradeoffs in
[performance and evidence](../README.md#performance-and-evidence).

Choosing index keys:

- Pick keys you'll put in `WHERE` clauses: `service`, `host`, `status`,
  `env` — low-to-medium cardinality identifiers.
- Don't index high-cardinality values like `request_id`; keep those in
  `metadata` (they'll still be stored and returned, just not
  filter-accelerated).
- Up to 28 keys per table. The list is fixed at creation and persisted;
  to change it, create a new table.

Full SQL `LIKE` works and keeps SQLite's exact semantics:

```sql
SELECT * FROM logs WHERE message LIKE '%timeout%';   -- works, but scans
```

Without `message_index='trigram'`, `LIKE` on `message` decompresses every
candidate block. The trigram option prunes blocks soundly, but SQLite still
rechecks rows. For a literal case-insensitive substring, use the exact engine
predicate; it can also bound an ordered result before rows cross SQLite:

```sql
SELECT * FROM logs
WHERE message_contains='timeout' AND ts >= :start
  AND max_work_entries=100000
ORDER BY ts DESC LIMIT 100;
```

For an exact scalar count without materializing rows, or bounded field-value
discovery, use the public TVFs. Existing shorter arities remain compatible;
the optional final argument is the same positive pre-decode work guard:

```sql
SELECT n FROM timeless_log_count(
  'logs', '{"level":"error","service":"api"}', NULL,
  :start, :stop, :max_work_entries);

SELECT value FROM timeless_log_values(
  'logs', 'host', '{"level":"error"}', NULL,
  :start, :stop, 1000, :max_work_entries);
```

To inspect one scan's actual block/entry/byte work, fully consume that scan and
then consume its report on the same connection:

```sql
SELECT ts, level, message FROM logs
 WHERE service='api' AND max_work_entries=100000
 ORDER BY ts;

SELECT payload_bytes_read, processed_blocks, processed_entries,
       matched_entries, returned_entries
  FROM timeless_log_query_stats('logs');
```

The report is table-scoped and single-use. A new, failed, or cancelled scan
clears it. Use
[`SQL-LOG-026`](QUERY_SQL_EQUIVALENTS.md#sql-log-026-request-local-log-query-statistics)
for every column and the LogsQL-compatible string mapping.

For the first few rows in each textual partition, direct SQLite/libSQL callers
can apply `row_number() over (partition by ... order by ...)` to a bounded
public `logs` scan. Use executable
[`SQL-LOG-027`](QUERY_SQL_EQUIVALENTS.md#sql-log-027-first-numeric-rows-per-partition)
for the complete parameterized numeric recipe. The Rust LogsQL API adds exact
integer and VictoriaLogs natural ordering, current-row pipeline composition,
rich paths, per-field direction, rank insertion, strict errors, cancellation,
and state/result limits; ordinary SQLite collation is not presented as an
equivalent for those language semantics.
Use executable
[`SQL-LOG-028`](QUERY_SQL_EQUIVALENTS.md#sql-log-028-last-numeric-rows-per-partition)
for the reverse numeric form. LogsQL `last` reverses the complete `first`
order—including each field's selected direction—and shares the same bounded
partition/rank implementation.

For the most frequent values of a public log field, use executable
[`SQL-LOG-029`](QUERY_SQL_EQUIVALENTS.md#sql-log-029-top-values-by-hit-count).
It maps missing/null to empty text, groups a bounded public JSON path, orders
hits descending with a deterministic bytewise tie-break, and emits hits/rank
as text. The Rust LogsQL API adds optional `by`, parenthesized or bare
multi-field grouping, `hits [as]` and `rank [as]`, collision-safe names,
current-row composition, hard state/result limits, cancellation, and errors.

Generic tables default to epoch milliseconds. The release Rust logs server
declares microseconds and binds every timestamp and work guard in that native
unit. Inspect `timeless_capabilities()` before depending on an additive query
input.

For tokenized/ranked full-text search, a plain table with FTS5 remains the
better tool; this table is optimized for dimensional filtering, literal
substring search, and compression.

## 6. Storing traces

OpenTelemetry-shaped spans have a fixed row schema plus optional retention and
a bounded attribute-index allowlist:

```sql
CREATE VIRTUAL TABLE traces USING timeless_traces(
  attribute_indexes='[{"scope":"span","path":"/http.method"}]'
);
```

| column | type | required | notes |
|---|---|---|---|
| `trace_id` | BLOB/TEXT | yes | 16-byte BLOB or 32-char hex TEXT |
| `span_id` | BLOB/TEXT | yes | 8-byte BLOB or 16-char hex TEXT |
| `parent_span_id` | BLOB/TEXT | no | NULL = root span |
| `name` | TEXT | yes | span name, e.g. `'GET /api/charge'` |
| `service` | TEXT | conditional | compatibility value; may be derived from `service.name` below |
| `kind` | TEXT | no | `internal` (default), `server`, `client`, `producer`, `consumer` |
| `status` | TEXT | no | `unset` (default), `ok`, `error` |
| `start_ts` | INTEGER | yes | unix timestamp in **nanoseconds** |
| `duration_ns` | INTEGER | no | span duration in ns (default 0) |
| `attributes` | TEXT | no | typed JSON object; nested OTel values are preserved |
| `status_description` | TEXT | no | OTel status message (default empty) |
| `events` | TEXT | no | typed JSON array of span events (default `[]`) |
| `resource` | TEXT | no | typed JSON object of resource attributes (default `{}`) |
| `instrumentation_scope` | TEXT | no | typed JSON scope object (default `{}`) |
| `links` | TEXT | no | typed JSON array of OTLP link objects (default `[]`) |
| `trace_state` | TEXT | no | W3C trace state (default empty) |
| `trace_flags` | INTEGER | no | OTLP unsigned 32-bit flags (default 0) |
| `dropped_attributes_count` | INTEGER | no | span dropped attributes (default 0) |
| `dropped_events_count` | INTEGER | no | span dropped events (default 0) |
| `dropped_links_count` | INTEGER | no | span dropped links (default 0) |
| `resource_schema_url` | TEXT | no | resource semantic-convention schema URL |
| `scope_schema_url` | TEXT | no | scope semantic-convention schema URL |
| `resource_dropped_attributes_count` | INTEGER | no | resource dropped attributes (default 0) |
| `scope_dropped_attributes_count` | INTEGER | no | scope dropped attributes (default 0) |

The indexed `service` value follows product semantics: a string
`service.name` in `attributes` wins, then a string `service.name` in
`resource`, then the explicit `service` value. At least one source must supply
a non-empty string.

Rich-span v2 retains OTLP links, trace state/flags, resource and scope schema
URLs, resource/scope/span dropped counts, per-event dropped counts, and
per-link flags, trace state, typed attributes, and dropped counts. Scope
attributes remain inside `instrumentation_scope`. The additive public batch
and compatibility defaults are specified in the
[rich-span v2 contract](2026-08-08_trace_rich_span_v2_contract.md).

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
`status`, ranges on `start_ts`, and inclusive lower/upper bounds on
`duration_ns` are also pushed down:

```sql
-- All failed spans for one service in a window
SELECT hex(trace_id), name, duration_ns FROM traces
 WHERE service = 'payments' AND status = 'error'
   AND start_ts >= 1753000000000000000;

-- Reject blocks whose longest span is still below 250 ms
SELECT hex(trace_id), name, duration_ns FROM traces
 WHERE service = 'payments' AND duration_ns >= 250000000
 ORDER BY start_ts DESC LIMIT 20;

-- Slowest operations, plain SQL
SELECT name, count(*), avg(duration_ns) / 1e6 AS avg_ms
  FROM traces WHERE service = 'payments'
 GROUP BY name ORDER BY avg_ms DESC LIMIT 10;
```

When a deployment repeatedly filters a small known set of typed attributes,
declare up to eight exact RFC 6901 paths at table creation and use the hidden
`attribute_filter` input:

```sql
SELECT hex(trace_id), name, duration_ns
  FROM traces
 WHERE start_ts BETWEEN :start_ns AND :stop_ns
   AND attribute_filter =
       '{"scope":"span","path":"/http.method","value":"GET"}'
 ORDER BY start_ts, span_id;
```

Scopes are `span`, `resource`, and `scope`; event and link arrays are not
supported. Matching is exact and typed, so missing, null, empty text, text
`"1"`, integer `1`, real `1.0`, and booleans are different. The filters are
fixed-size per block and can return false positives internally, but surviving
spans are always rechecked. Low-cardinality values present in every block do
not prune and still pay the fixed index cost. Use ordinary JSON1 for
unconfigured paths, existence, arrays/objects, ranges, or regex-like work.
The complete setup, SQL control, compatibility, and accounting contract is in
the [SQL API reference](SQL_API_REFERENCE.md#bounded-typed-attribute-equality).

New blocks carry exact per-block duration extrema in a private side table. A
database created by an older extension gains that table when it is opened; a
missing metadata row means the block's duration range is unknown, so unknown
blocks continue to return exact results by decoding. Run the normal public
maintenance command to backfill them without recompressing the payload:

```sql
SELECT key, value FROM timeless_stats('traces')
 WHERE key IN ('duration_bounded_blocks', 'duration_unknown_blocks');
INSERT INTO traces(traces) VALUES ('optimize');
```

Use `optimize:<positive max source spans>` when the one-time decode should be
spread across maintenance windows. A crash or rollback cannot leave a block
partially summarized: both extrema publish in the surrounding SQLite
transaction, and corrupt/incomplete extrema fail closed on reopen.

For a summary of one trace's currently retained rows, use the copyable
[retained trace summary SQL](SQL_API_REFERENCE.md#retained-trace-summaries).
It reports physical and distinct span counts, errors, checked envelope bounds,
root ambiguity, and service cardinality. Completeness is deliberately
`unknown`: OTLP has no trace-finalization or retry identity, so root presence
or an inactivity timeout would not make that claim truthful. The
[trace query matrix](2026-08-08_trace_query_matrix.md) defines the boundary
between this retained-row summary and a future versioned complete-trace API.

## 7. Querying: what's fast, what's slow

You don't need to learn a query planner — just know which predicates the
storage engine can use to *skip* decompression:

| table | accelerated predicates |
|---|---|
| metrics | `series_id = ...`, `name = ...`, `ts` ranges (`>`, `>=`, `<`, `<=`, `BETWEEN`) |
| logs | `level = ...`, any index-key `= ...`, `ts` ranges, `message_contains = ...`; `max_work_entries = ...` bounds examined entries |
| traces | `trace_id = ...`, `service`/`name`/`kind`/`status` `= ...`, `start_ts` ranges, inclusive `duration_ns` lower/upper bounds, configured `attribute_filter = ...` typed scalar equality |

Optimized generation-2 and generation-3 trace blocks also honor SQLite's projection. The
engine first decodes only columns required by pushed predicates, then
materializes selected rich columns for matching rows. The ten rich-span-v2
columns are one late-materialized group. Thus `count(*)`, scalar
projections, misses, and `timeless_trace_buckets` avoid expanding unrelated
rich JSON. Older readable block formats use the exact full-decoder fallback.
Inspect `query_decoded_columns`, `query_decoded_column_bytes`,
`query_materialized_values`, and `query_materialized_rich_values` through
`timeless_stats('traces')` when attributing a workload.

Everything else still *works* — it's ordinary SQL over the decompressed rows
— it just reads more blocks. Practical guidance:

- **Always constrain the time range** when you can. It's the universal
  pruner on all three tables.
- **Aggregations are just SQL.** `avg`, `min/max`, `GROUP BY`, and window
  functions work normally. For a large logs count, `timeless_log_count`
  avoids constructing the matching rowset and can use block metadata.
- **Joins work.** These are real tables to SQLite; join telemetry against
  your application tables in the same file.
- Mind the timestamp units — they follow each signal's native convention:
  metrics = **seconds**, logs = **milliseconds**, traces = **nanoseconds**.
- Two shapes still favor specialized tables: ranked/tokenized full-text
  search and single-row point lookups against a B-tree index. Use the native
  exact substring/count surfaces when those simpler semantics fit.

When row order matters, say so with `ORDER BY ts` — metrics results come back
time-ordered, but logs and traces blocks can overlap, and it's ordinary cheap
SQL either way. Don't rely on `rowid`; it's synthetic and only stable within
a single scan.

For a client that repeatedly reads the same metrics series, resolve its durable
handle once and reuse it:

```sql
SELECT series_id
  FROM timeless_series('metrics', 'cpu_usage', '{"host":"web1"}');

SELECT ts, value
  FROM timeless_raw('metrics', 'cpu_usage', NULL, :start, :stop)
 WHERE series_id = :series_id;
```

The equality constraint works on the base metrics table and every per-series
metrics query function. It still intersects with the function's metric and
label filter. IDs survive reopen and backup/restore, are scoped to one metrics
table, and must not be copied between independently created databases.

Ordinary row results are the best interface for SQL composition. A host
language fetching thousands of aggregate or latest rows can instead cross
SQLite once:

```sql
SELECT frame FROM timeless_aggregate_frame(
  'metrics', 'cpu_usage', NULL, :start, :stop, 'avg');

SELECT frame FROM timeless_latest_frame(
  'metrics', 'cpu_usage', NULL, :start, :stop);
```

These return versioned `TAF1` and `TLF1` blobs. Use the maintained strict Rust
decoders in `timeless_ext::query_frame`. See the
[query cookbook](QUERIES.md#packed-aggregate-frame) for the exact byte layouts
and NULL rules. The frames are result transport only; database storage,
transactions, backups, and replication are unchanged.

## 8. Housekeeping: optimize, compact, prune

All commands are the same idiom: insert a string into the table's hidden
command column. All are safe to run anytime, transactional, and idempotent.

```sql
INSERT INTO metrics(metrics) VALUES ('flush');      -- persist the buffer (all tables)
INSERT INTO metrics(metrics) VALUES ('compact');    -- metrics: merge small chunks
INSERT INTO logs(logs)       VALUES ('optimize');   -- logs/traces: re-encode into
INSERT INTO traces(traces)   VALUES ('optimize');   --   larger, better-compressed blocks
INSERT INTO logs(logs)       VALUES ('optimize:65536'); -- bounded logs source entries
INSERT INTO traces(traces)   VALUES ('optimize:65536'); -- bounded trace source spans
```

- **`flush`** — you know this one (section 3).
- **`optimize` / `compact`** — frequent small flushes create many small
  blocks, which cost a little disk and read speed. Run this occasionally
  (e.g. daily, or after a big backfill) to merge them into optimally
  compressed blocks. Never required for correctness.
- **`optimize:<entries>`** — the same optimizer with a per-call source-work
  budget for logs or traces. One complete planner group may exceed the budget
  so a call always makes progress. Inspect `optimize_pending_*`,
  `optimize_merge_ready_*`, and the raw/merge phase counters in
  `timeless_stats(...)` to schedule it without guessing at block structure.
  For logs, `optimize_source_entries` and `optimize_source_bytes` expose the
  current raw-or-undersized source sample used to translate a byte budget into
  an entry budget. These are public extension rows; hosts must not inspect
  shadow block tables.
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
| `level is required` / invalid severity | logs `level` is NULL or not one of the eight exact lowercase names — `'warn'` and `'ERROR'` are rejected, not coerced |
| `trace_id is required (16-byte BLOB or 32-char hex TEXT)` | wrong id length/format; same pattern for `span_id` (8 bytes / 16 hex chars) |
| flat-JSON errors on metric `labels` | labels must be a flat JSON object with **string** values only; rich log `metadata` instead accepts canonical typed/nested JSON |
| JSON shape errors on trace rich fields | `attributes`, `resource`, and `instrumentation_scope` must be JSON objects; `events` must be an array; typed and nested values are supported |
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
level:  debug | info | notice | warning | error | critical | alert | emergency
kind:   internal | server | client | producer | consumer
status: unset | ok | error
```

---

*Want public SQL over the network? [SQLD.md](SQLD.md) shows the pinned
self-hosted sqld/Hrana boundary. The SQL schemas and commands are the same;
transactions, authentication, limits, maintenance, shutdown, and typed wire
values remain host responsibilities and must follow that guide. Want the
internals—how compression works, benchmark methodology, and the durability
proof? See the [README](../README.md),
[historical benchmark archive](../RESULTS.md), and
[TESTING.md](../TESTING.md).*
