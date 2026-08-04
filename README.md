# timeless-libsql

**Compressed metrics, logs, and traces inside any SQLite or libSQL database —
one loadable extension, three virtual tables.** Think *"FTS5 for telemetry."*

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Rust 1.95+](https://img.shields.io/badge/rust-1.95%2B-orange.svg)
![Status: experimental](https://img.shields.io/badge/status-experimental-red.svg)

```sql
.load ./libtimeless_ext

CREATE VIRTUAL TABLE metrics USING timeless_metrics;
CREATE VIRTUAL TABLE logs    USING timeless_logs(index_keys='service,path,status');
CREATE VIRTUAL TABLE traces  USING timeless_traces;

INSERT INTO metrics(name, ts, value, labels)
  VALUES ('cpu_usage', 1753000000, 42.5, '{"host":"pvm1"}');
INSERT INTO metrics(metrics) VALUES ('flush');   -- FTS5-style command idiom

SELECT * FROM logs   WHERE service='payments' AND level='error' AND ts > :t0;
SELECT * FROM traces WHERE trace_id = x'4bf92f3577b34da6a3ce929d0e0e4736';

-- A raw Prometheus scrape body is just another blob:
INSERT INTO metrics(metrics) VALUES (readfile('scrape.prom'));
```

Chunks and blocks are compressed ([pco](https://github.com/pcodec/pcodec) +
adaptive columnar encoding) and stored in shadow tables **inside the same
database file** — so transactions, backup, and libSQL replication come from
the host, while compression, pruning, and predicate pushdown come from the
engines. Works in the `sqlite3` CLI, the rusqlite/libsql crates, and
self-hosted `sqld` (SQL over HTTP, zero client changes).

## Why

Telemetry in SQLite usually means a plain table that grows ~50–160 bytes per
row forever, or shipping data out to a second system (Prometheus, Loki,
ClickHouse) with its own storage, backup, and replication story. This
extension keeps telemetry *in the database you already have*:

- **One file.** Metrics, logs, traces, and your application data — same
  `.db`, same `BEGIN/COMMIT`, same backup, same libSQL replication stream.
- **6–200x smaller.** Lossless compression, verified bit-exact per point
  after flush and cold recovery (see [Numbers](#numbers)).
- **It's just SQL.** No query DSL: indexed dimensions are real (hidden)
  columns, so `WHERE service='api' AND level='error'` pushes down into an
  inverted term index and reads like a normal query plan.
- **Append-only with honest retention.** DELETE/UPDATE are rejected;
  retention is an explicit `'prune:<ts>'` command.

**Born for the edge.** Compression happens at the point of collection —
points buffer in memory and land in the WAL only as compressed blocks —
so libSQL replication (to a hub, to S3-compatible storage via bottomless,
to an embedded replica) ships the *compressed* bytes, never the raw
points. On a metered uplink that is the whole story. One device recording
100 metrics at 1 Hz for a month:

| storage | wire format | monthly upstream |
|---|---|---:|
| plain SQLite table | raw rows (~52.6 B/pt) | **~13.6 GB** |
| timeless, hostile data | pco blocks (8.3 B/pt) | **~2.2 GB** |
| timeless, friendly data | pco blocks (0.23 B/pt) | **~60 MB** |

For IoT on cellular backhaul the flush cadence doubles as radio
discipline — one batched, compressed write per interval instead of a
row-trickle keeping the modem awake — and with [dbhealth](docs/DBHEALTH.md)
on the same device, every unit self-monitors and its health history rides
the same replication stream home. Sensors at the edge, pennies on the
uplink, one file to sync.

## Quick start

```sh
cargo build --release -p timeless-ext
# artifact: target/release/libtimeless_ext.so   (.dylib on macOS)
```

```sh
sqlite3 demo.db \
  ".load target/release/libtimeless_ext" \
  "CREATE VIRTUAL TABLE m USING timeless_metrics;
   INSERT INTO m(name, ts, value) VALUES ('cpu', 1, 42.5);
   INSERT INTO m(m) VALUES ('flush');
   SELECT * FROM m;"
# → cpu|1|42.5|{}
```

> **macOS:** Apple's system `/usr/bin/sqlite3` disables extension loading
> (`.load` fails with "not authorized"). `brew install sqlite` and use
> `$(brew --prefix sqlite)/bin/sqlite3` — or use the bundled-SQLite bench
> binaries in `tools/bench`, which work everywhere. Rust 1.95+ required.

## The three virtual tables

| module | row shape | ts unit | indexed dimensions (pushdown) |
|---|---|---|---|
| `timeless_metrics` | `(name, ts, value, labels)` | **seconds** | `name` =, `ts` ranges |
| `timeless_logs` | `(ts, level, message, metadata, …index keys)` | **milliseconds** | `level` =, `ts` ranges, every `index_keys` column =, exact `message_contains` = |
| `timeless_traces` | core IDs/timing plus typed `attributes`, `status_description`, `events`, `resource`, `instrumentation_scope` | **nanoseconds** | `trace_id` =, `service`/`name`/`kind`/`status` =, `start_ts` ranges |

All three share the same lifecycle: inserts land in an in-memory buffer
(queryable immediately, auto-flushed at a size threshold), `'flush'` encodes
the buffer into compressed blocks riding the host transaction, and reads
transparently merge flushed blocks with the live buffer. Commands use the
FTS5 hidden-column idiom — an INSERT into the column named after the table:

```sql
INSERT INTO metrics(metrics) VALUES ('flush');       -- make buffered points durable
INSERT INTO metrics(metrics) VALUES ('compact');     -- merge small chunks
INSERT INTO logs(logs)       VALUES ('optimize');    -- re-encode into larger, purer blocks
INSERT INTO logs(logs)       VALUES ('optimize:65536'); -- cap source entries this turn
INSERT INTO traces(traces)   VALUES ('optimize:65536'); -- same bounded maintenance for spans
INSERT INTO traces(traces)   VALUES ('prune:<ns>');  -- drop everything older than <ts>
```

`optimize:<entries>` is the incremental logs/traces maintenance form. Raw
compression runs before eligible size-tiered merges, and one complete merge
cohort may slightly exceed the requested budget so maintenance always makes
progress. `timeless_stats('logs'|'traces')` exposes actionable/deferred backlog
plus separate raw and merge entry, byte, and time counters. (`prune:` takes the
table's own ts unit: seconds / ms / ns.)

Retention can also be automatic — declared at CREATE, applied during the
maintenance the engine already performs (no background threads; the vtab
stays passive):

```sql
CREATE VIRTUAL TABLE metrics USING timeless_metrics(retention='30d');
CREATE VIRTUAL TABLE logs    USING timeless_logs(index_keys='service', retention='7d');
CREATE VIRTUAL TABLE traces  USING timeless_traces(retention='72h');
```

The cutoff is *data time* (newest ingested timestamp minus the window),
so it's deterministic, replay-safe, and inert for backfills; pruning is
chunk/block-granular at flush/compact/optimize boundaries.

**Rollup ladder (metrics)** — declare downsampling tiers and raw ages
out while coarse aggregates survive, which is what makes "a year of
metrics in one SQLite file" plausible:

```sql
CREATE VIRTUAL TABLE metrics USING timeless_metrics(
  retention='14d',                  -- raw tier
  rollups='5m@90d,1h@0');          -- resolution@retention, 0 = forever

INSERT INTO metrics(metrics) VALUES ('rollup');  -- also runs at 'compact'

SELECT labels, ts, value                         -- explicit tier reads
  FROM timeless_rollup('metrics', 'cpu_usage', NULL, 300, :t0, :t1, 'avg');
--                                           agg: avg|sum|min|max|count|last

SELECT series_id, labels, buckets                -- all fields, one row/series
  FROM timeless_rollup_batches(
    'metrics', 'cpu_usage', NULL, 300, :t0, :t1);
```

Buckets are `[B, B+R)` with exactly-documented aggregate math
(bit-verified against naive bucket computation); tiers fill as buckets
settle (one bucket-width margin) and are append-only. Tier reads are
explicit — no silent substitution for raw. On the 1M-point bench, the
1-minute tier answers the per-bucket average in **4.1ms** vs 34.5ms for
the GROUP BY over raw, and building the whole tier costs 80ms. The packed
`timeless_rollup_batches` form keeps the row TVF intact while avoiding six
separate scans for hosts that need the complete rollup record.

### Metrics

```sql
CREATE VIRTUAL TABLE metrics USING timeless_metrics;
INSERT INTO metrics(name, ts, value, labels) VALUES
  ('cpu_usage', 1753000000, 42.5, '{"host":"pvm1"}'),
  ('cpu_usage', 1753000015, 43.1, '{"host":"pvm1"}');
INSERT INTO metrics(metrics) VALUES ('flush');

SELECT name, ts, value, labels FROM metrics
 WHERE name='cpu_usage' AND ts >= 1753000015;
-- cpu_usage|1753000015|43.1|{"host":"pvm1"}
```

Aggregation is plain SQL — `avg(value)`, `min`, `max`, `GROUP BY` all work;
the vtab prunes chunks by name and ts range before SQLite ever sees a row.
For a scalar result per series, `timeless_aggregate` is the chunk-aware fast
path and avoids shipping raw samples through SQLite.

Three ingest paths, one durability contract (same buffers, same flush):

1. **Tier 1 — SQL rows** (above): ~2.3M pts/s. The compatibility floor;
   works from any SQLite client.
2. **Tier 2 — batch blob**: a packed columnar blob (version byte `0x01`,
   then series table + ts/value arrays, spec in [PLAN.md](PLAN.md)) inserted
   into the hidden column: **23.8M pts/s**. For agents that batch off the
   hot path.
3. **Prometheus exposition text**: any non-batch blob is parsed as a raw
   scrape body — malformed/NaN lines are counted, not fatal, exactly like a
   real Prometheus server scrape:

```sh
curl -s target:9100/metrics -o /tmp/scrape.prom && sqlite3 metrics.db \
  ".load ./libtimeless_ext" \
  "INSERT INTO metrics(metrics) VALUES (readfile('/tmp/scrape.prom'));
   INSERT INTO metrics(metrics) VALUES ('flush');"
```

The scraping loop stays external by design (cron, curl, your app); the
vtab is passive.

Embedded hosts can resolve a durable series id once and omit metric/label
strings from steady-state writes:

```sql
INSERT INTO metrics(metrics, name, labels)
  VALUES ('resolve', 'cpu_usage', '{"host":"pvm1"}');
SELECT last_insert_rowid();                     -- durable series_id

INSERT INTO metrics(series_id, ts, value)
  VALUES (:series_id, 1753000015, 43.1);
```

The corresponding resolved batch begins with version byte `0x02`, followed
by `flags:u8=0`, `reserved:u16=0`, `n_points:u32 LE`, then columnar
`series_id:i64[n]`, `timestamp:i64[n]`, and `value_bits:u64[n]` arrays, all
little-endian. The entire id set is validated before any point is buffered.
Named batch `0x01` remains the portable path when the caller has no durable
id cache.

The same durable ID is a read handle. SQLite pushes an equality constraint
through the base metrics table and every per-series metrics query TVF:

```sql
SELECT ts, value FROM metrics
 WHERE series_id = :series_id AND ts BETWEEN :t0 AND :t1;

SELECT value
  FROM timeless_aggregate('metrics', 'cpu_usage', NULL, :t0, :t1, 'avg')
 WHERE series_id = :series_id;
```

The ID is intersected with the TVF's metric and matcher arguments and composes
with joins against `timeless_series`. It is durable and table-scoped, but is not
portable between independently created database files.

**Query kernels** — table-valued functions evaluate the dominant
dashboard shapes inside the engine, so remote deployments (sqld, HTTP)
ship grid points instead of every raw sample:

```sql
-- Matcher-aware raw narrow waist (one row per sample):
SELECT series_id, labels, ts, value
  FROM timeless_raw('metrics', 'cpu_usage', '{"host":"pvm1"}', :t0, :t1);

-- Same waist, one packed point blob per series for embedded hosts:
SELECT series_id, labels, points
  FROM timeless_raw_batches('metrics', 'cpu_usage', '{"host":"pvm1"}', :t0, :t1);

-- Wide fanout form: every non-empty series in one versioned columnar frame:
SELECT frame
  FROM timeless_raw_frame('metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1);

-- one scalar reduction per matched series; inclusive [:t0, :t1]
-- aggregate: avg | sum | min | max | count
SELECT series_id, labels, value
  FROM timeless_aggregate('metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1, 'avg');

-- every scalar result in one TAF1 frame:
SELECT frame
  FROM timeless_aggregate_frame(
    'metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1, 'avg');

-- newest point per matched series; inclusive bounds
SELECT series_id, labels, ts, value
  FROM timeless_latest('metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1);

-- every newest point in one TLF1 frame:
SELECT frame
  FROM timeless_latest_frame('metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1);

-- last sample per grid point, per series (instant-selector shape):
--                 table      metric       label filter    start  stop   step lookback
SELECT labels, ts, value
  FROM timeless_grid('metrics', 'cpu_usage', '{"host":"pvm1"}', :t0, :t1, 60, 90);

-- sliding-window operations per grid point:
--   folds:       sum | min | max | count | avg
--   counters:    delta | increase | rate
--   percentiles: pNN (exact nearest-rank: p50, p95, p99.9, …)
--   robust:      tavg:N (trimmed mean, drop N% from each tail)
SELECT labels, ts, value
  FROM timeless_window('metrics', 'requests_total', NULL, :t0, :t1, 60, 300, 'rate');

-- Same result, one versioned bucket blob per series for embedded/remote hosts:
SELECT series_id, labels, buckets
  FROM timeless_window_batches(
    'metrics', 'requests_total', NULL, :t0, :t1, 60, 300, 'rate');
```

`timeless_window_batches` uses `TWB1 | count:u32 LE | timestamps:i64 LE[] |
validity bitmap | value_bits:u64 LE[]`. The bitmap also preserves the optional
`fill='null'` shape. See [the query cookbook](docs/QUERIES.md#packed-window-batches)
for the exact byte contract. The row-oriented `timeless_window` API remains
unchanged.

`timeless_raw_frame` uses `TRF1 | series_count:u32 LE | total_points:u64 LE |
series_ids:i64 LE[] | point_counts:u32 LE[] | timestamps:i64 LE[] |
value_bits:u64 LE[]`. Counts partition the point columns by series; empty
series are omitted and series slice order is unspecified. It complements,
rather than replaces, the row-oriented and per-series batch raw APIs. See
[the packed-raw contract](docs/QUERIES.md#packed-raw-frame).

Rollup hosts can likewise use `timeless_rollup_batches`, whose `TRB1` blob
contains bucket timestamps, exact integer counts, all six aggregate values,
and stored last-sample timestamps in one row per series. See
[the packed-rollup contract](docs/QUERIES.md#packed-rollup-batches).

`timeless_aggregate` emits no row for an empty series/range and returns
`count` as a SQLite INTEGER. Fully covered chunks use persisted statistics;
only boundary chunks are decoded. `sum`/`avg` therefore accumulate points
left-to-right within each chunk, then chunk sums in index order, so a flat SQL
scan can differ by floating-point rounding. Count is exact. NaNs count as
stored points, make `sum`/`avg` SQL `NULL`, and are ignored by `min`/`max`
unless every value is NaN, in which case those are `NULL` too.

`timeless_latest` emits at most one row per matched series and no row for an
empty range. The greatest timestamp wins; duplicate maximum timestamps keep
the first point in the raw engine's stable order. New chunks persist that
point as metadata, so the usual unbounded-latest query does not decompress
history. Databases created by older versions are upgraded additively: legacy
chunks use the decode fallback until compaction rewrites them.

For high-cardinality host-language reads, `timeless_aggregate_frame` and
`timeless_latest_frame` return the same logical row results in one versioned
columnar blob. Their `TAF1`/`TLF1` layouts, strict Rust decoders, and a
dependency-free Python decoder are documented in
[the query cookbook](docs/QUERIES.md#packed-aggregate-frame). The row TVFs
remain the normal relational interface; frames are an additive transport and
never change the on-disk format.

**Counter kernels are raw window folds, NOT PromQL**: `increase` uses
the standard reset-adjustment rule but does **no** boundary
extrapolation, lookback, or staleness inference — for conformance-grade
PromQL semantics, use the evaluator layer above the waist. Percentiles
are the opposite story: because raw samples are kept, `p95` is the
**exact** value — no `le`-bucket interpolation, no sketch error — which
Prometheus-lineage systems cannot offer. `timeless_trace_buckets` gains
`dur_p50/dur_p95/dur_p99` for the same reason: exact p95 latency per
service per bucket. Outlier handling is always parameter-explicit
(`tavg:5`) — the database never decides what an outlier is.

Both windows are half-open `(t - width, t]`; grid points with no sample
produce no row — unless you ask for a dense grid with the optional
trailing **fill** argument (`'none'` default, `'null'` = every grid
point emitted per matched series, value `NULL` where the window is
empty; a series with no points in range stays absent either way):

```sql
SELECT labels, ts, value
  FROM timeless_grid('metrics', 'cpu_usage', NULL, :t0, :t1, 60, 90, 'null');
```

The kernels are deliberately *semantics-free* — no
lookback defaults, no staleness or rate math (that belongs to the query
layer above) — which is what makes them safe: every result is verified
bit-for-bit against naive evaluation, in the test suite and in the
benchmarks. Callers that don't know about them lose nothing: the raw
scan they replace still works everywhere.

Measured on the 1M-point bench dataset (1-minute grid × 100 hosts over
the whole range): the raw-scan fallback ships 99,900 samples and
evaluates client-side in **17.9ms**; `timeless_grid` returns the same
16,600 grid rows in **1.6ms** — 11x, before a network is even involved.

**Label matchers** — every kernel TVF's filter argument accepts matcher
objects alongside plain equality strings:

```sql
SELECT labels, ts, value FROM timeless_grid('metrics', 'cpu_usage',
  '{"host": {"re": "web-.*"}, "env": {"neq": "dev"}}', :t0, :t1, 60, 90);
```

| Filter value | Meaning |
|---|---|
| `"v"` | equality (pushed into the label index, as before) |
| `{"neq": "v"}` | not equal |
| `{"re": "pat"}` | regex match ([Rust `regex`](https://docs.rs/regex) dialect — RE2 family: no backrefs/lookaround) |
| `{"nre": "pat"}` | regex non-match |

Regexes are **fully anchored** (PromQL-style): the pattern must match
the whole value — `web-.*` means *starts with* `web-`, `.*web.*` means
*contains*. A label absent from a series matches as the empty string
`""`, so `{"neq": "prod"}` includes series without the label and
`{"nre": ".+"}` means "label absent or empty". Matchers prune the
candidate *series list* before any chunks are read — cost is
per-series, not per-point. Invalid patterns are loud errors naming the
pattern and the label.

**Introspection** — three more table-valued functions answer "what's in
here?" without decompressing anything:

```sql
SELECT * FROM timeless_series('metrics');  -- one row per series: name, labels,
                                           -- min/max ts (incl. buffered), points,
                                           -- chunks, buffered  (~3ms for 1000 series)
SELECT * FROM timeless_stats('metrics');   -- key/value health rows; works for
                                           -- logs and traces tables too
SELECT value FROM timeless_label_values('metrics', 'cpu_usage', 'host');
                                           -- sorted distinct label values —
                                           -- the dropdown-population query

-- Packed metric reads expose cumulative candidate/decode/byte/return work:
SELECT key, value FROM timeless_stats('metrics')
 WHERE key LIKE 'raw_batch_query_%' ORDER BY key;

-- Optional discovery filters use the same matcher JSON and are applied
-- before unrelated catalog rows cross SQLite:
SELECT labels FROM timeless_series('metrics', 'cpu_usage',
  '{"host":{"re":"web-.*"},"env":{"neq":"dev"}}');
SELECT value FROM timeless_label_values('metrics', 'cpu_usage', 'host',
  '{"env":{"neq":"dev"}}');
```

For worked recipes — reset-corrected counter math in pure SQL, top-k
per bucket, cross-metric joins, IQR/σ outlier exclusion, gap-fill
patterns — see **[docs/QUERIES.md](docs/QUERIES.md)**; every recipe in
it is executed by the test suite, so the cookbook can't rot.

### Logs

`index_keys` declares which metadata keys get inverted-index treatment —
each one becomes a real hidden column you can SELECT and filter on:

```sql
CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service,path,status');
INSERT INTO logs(ts, level, message, metadata) VALUES
  (1753000000123, 'error', 'payment declined: card_expired',
   '{"service":"payments","path":"/api/charge","status":"402"}');
INSERT INTO logs(logs) VALUES ('flush');

SELECT ts, level, message FROM logs
 WHERE service='payments' AND level='error' AND ts > 1753000000000;
-- 1753000000123|error|payment declined: card_expired
```

- `level` is a strict vocabulary: `debug | info | warning | error`.
- Index-key equality intersects posting lists in the `_terms` shadow table;
  only matching blocks are decompressed.
- `message LIKE '%…%'` scans by default — or declare
  `message_index='trigram'` and substring search becomes a block-pruning
  problem: every 3-byte window of the pattern's literal runs must appear
  in a block for it to decode, and SQLite still rechecks rows exactly.
  1M entries: `LIKE '%timeout%'` in **48.3ms** vs 334.5ms unindexed —
  and vs 74.5ms for the plain table, flipping the one benchmark row this
  extension used to lose (index overhead ~1.5 MB, opt-in).
- For a literal case-insensitive substring rather than full SQL LIKE syntax,
  use the exact hidden input `message_contains`. It filters before rows cross
  the virtual-table boundary and can consume `ORDER BY ts ... LIMIT/OFFSET`:

  ```sql
  SELECT ts, level, message FROM logs
   WHERE message_contains='timeout'
   ORDER BY ts DESC LIMIT 100;
  ```

  ASCII matching is allocation-free; non-ASCII matching uses Unicode
  lowercase equivalence. Trigram pruning remains conservative and is disabled
  for a non-ASCII needle so it cannot introduce false negatives.
- Index keys can also be used as INSERT shorthand: a non-NULL value in the
  hidden column merges into the metadata JSON.
- **Batch ingest**: a columnar blob (v0 spec in the vtab docs) into the
  hidden column ingests a whole batch in one statement, validated
  all-or-nothing — one round trip per batch for remote writers, +16-46%
  throughput in-process (logs/traces ingest is flush-bound, unlike
  metrics where the blob path is 10x).

**Bucket kernel** — the dominant logs-dashboard shape (volume histogram
by level or any index key) evaluated engine-side; level-pure blocks that
fit inside one bucket are counted from metadata without decoding:

```sql
SELECT bucket_ts, group_key, n FROM timeless_log_buckets(
  'logs', 'level', '{"service":"api"}', :t0, :t1, 60000);
-- buckets are [t, t+step) aligned to :t0 — histograms bin forward
```

1M entries, whole range, 1-minute buckets: **95.9ms** vs 543.5ms for the
GROUP BY over the raw vtab (5.7x, totals verified equal).

**Scalar count** — exact count without materializing a log rowset. `filter` is
a flat JSON object: `level` selects severity and every other member is a
metadata equality. The remaining arguments are optional exact substring and
inclusive timestamp bounds:

```sql
SELECT n FROM timeless_log_count(
  'logs', '{"level":"error","service":"api"}', 'timeout', :t0, :t1
);
```

Only the table name is required. Fully covered unfiltered or level-pure blocks
use persisted `entry_count` without reading payloads. Boundary, legacy-mixed,
metadata-filtered, and message-filtered blocks decode one at a time.

### Traces

OTel-shaped spans; the hero query is trace reassembly by id, routed through
a dedicated packed-trace-id index so only blocks containing that trace are
decompressed:

```sql
CREATE VIRTUAL TABLE traces USING timeless_traces;
INSERT INTO traces(trace_id, span_id, name, service, kind, status, start_ts, duration_ns)
  VALUES ('4bf92f3577b34da6a3ce929d0e0e4736', '00f067aa0ba902b7',
          'GET /api/charge', 'payments', 'server', 'error',
          1753000000123000000, 8500000);
INSERT INTO traces(traces) VALUES ('flush');

SELECT hex(trace_id), name, service, status, duration_ns FROM traces
 WHERE trace_id = x'4bf92f3577b34da6a3ce929d0e0e4736';
-- 4BF92F3577B34DA6A3CE929D0E0E4736|GET /api/charge|payments|error|8500000
```

- Ids accept packed BLOBs (16/8/8 bytes) *or* hex TEXT (32/16/16 chars) on
  insert — OTel tooling hands out hex, storage wants packed. Always returned
  as BLOBs; use `hex()` for display.
- `kind` (`internal|server|client|producer|consumer`) and `status`
  (`unset|ok|error`) are strict TEXT vocabularies mapped to storage bytes.
- `attributes`, `resource`, and `instrumentation_scope` are typed JSON
  objects; `events` is a typed JSON array. Booleans, numbers, nulls, arrays,
  and nested objects survive flush, optimize, and reopen.
- `service` is derived with the same precedence as timeless_traces:
  string `attributes["service.name"]`, then resource `service.name`, then the
  explicit compatibility column. The derived value is what gets indexed.
- Batch byte `0x01` is the stable core-span v0 format. Byte `0x02` is the
  additive rich-span v1 format; both remain readable. The extension's
  8,192-span auto-flush threshold is authoritative for both paths.

**Bucket kernel** — per-service span stats per time bucket (count,
errors, duration sum/min/max; percentiles stay above the waist):

```sql
SELECT bucket_ts, service, spans, errors, dur_sum, dur_min, dur_max
  FROM timeless_trace_buckets('traces', NULL, :t0, :t1, 60000000000);
```

960k spans, whole range: **246ms** vs 582ms for the GROUP BY equivalent
(2.4x — duration math requires decoding every span, so this one is
decode-bound).

## Query language roadmaps

The SQL primitives above are the storage/query foundation, not an implicit
claim of complete PromQL or LogsQL compatibility. The living feature maps say
exactly what is implemented, where each missing construct belongs, and what
must be tested before it can be marked shipped:

- [Query feature ownership and workflow](docs/QUERY_FEATURES.md)
- [PromQL feature matrix](docs/PROMQL_FEATURE_MATRIX.md)
- [LogsQL feature matrix](docs/LOGSQL_FEATURE_MATRIX.md)
- [SQL equivalents for query-language features](docs/QUERY_SQL_EQUIVALENTS.md)
- [Query/storage findings](docs/QUERY_STORAGE_FINDINGS.md)
- [Shipped-row conformance references](docs/QUERY_TEST_REFERENCES.md)
- [Pinned query semantic oracles](docs/QUERY_ORACLES.md)
- [Query benchmark and evidence protocol](docs/QUERY_EVIDENCE.md)
- [Sequential query implementation plan](docs/2026-08-04_query_surface_implementation_plan.md)

PromQL and LogsQL parsing/evaluation live in the Rust signal APIs. The
extension exposes general SQLite/libSQL primitives and receives a new query
vector only when measurements prove that storage-aware pushdown materially
avoids reads, decode, copies, or row crossings. Saved queries, subscriptions,
rules, dashboards, and control-plane state remain higher-order library work.

## How it works

```mermaid
flowchart LR
    subgraph app ["your process"]
        SQL["SQL<br/>(any SQLite client)"]
    end
    subgraph ext ["libtimeless_ext (loadable .so/.dylib)"]
        VT["vtab modules<br/>timeless_metrics / _logs / _traces"]
        ENG["timeless-core engines<br/>buffer → encode → prune/compact"]
        COD["timeless-codec<br/>pco + adaptive columnar"]
    end
    subgraph db ["one database file"]
        MAIN["your tables"]
        SHADOW["shadow tables<br/>_chunks / _blocks / _terms /<br/>_trace_blocks / _meta"]
    end
    SQL --> VT --> ENG --> COD
    ENG -->|"re-entrant SQL,<br/>rides host txn"| SHADOW
    MAIN ~~~ SHADOW
```

`CREATE VIRTUAL TABLE metrics USING timeless_metrics` creates ordinary
shadow tables next to it (`metrics_chunks`, `metrics_meta`, …) — the FTS5
pattern. Engine writes are re-entrant SQL on the host connection, so **vtab
writes ride the host transaction**: `ROLLBACK` rolls back buffered inserts
*and* intra-transaction flushes; compaction's atomic swap needed zero
crash-recovery code because SQLite's journal already provides it.

Column encoding is adaptive per block ("codec 5"): timestamps get
delta + pco, low-cardinality strings get RLE or dictionary encoding, log
metadata is shredded by key, and rich trace JSON remains lossless in adaptive
string columns. Everything else falls back to zstd — whichever is smallest
wins, decided by the data, per column, per block.

**Durability contract:** flushed = durable, buffered = lost with the
process, never corrupt. Proven by `kill -9` crash rounds
(`tests/crash.sh`): reopen, `PRAGMA integrity_check`, every flushed
watermark present, no index row dangling.

**Multi-connection:** all connections in one process share one engine per
(db file, table) via a process-global registry. Committed buffered inserts are
queryable from another connection without a flush, and writers are serialized
by a bounded per-table gate. A connection can read its own active write
transaction; another connection selecting during that transaction receives a
retryable busy-style error (`active write transaction`; retry as for
`SQLITE_BUSY`) instead of observing transaction-private shadow rows. This is
what makes `sqld`'s connection pool work safely:

```sh
sqld --extensions-path ./ext-dir   # sha256 trusted.lst; loads into every connection
# curl one request: CREATE VIRTUAL TABLE → INSERT → 'flush'
# curl another (fresh pooled connection): rows come back, name pushdown, 0.19ms
```

## Numbers

Measured 2026-07-22 on an Apple M5 Pro (macOS, Rust 1.97), second run
quoted per [TESTING.md](TESTING.md); Linux reference run in
[RESULTS.md](RESULTS.md). All datasets are deterministic and deliberately
hostile (ms-jitter timestamps, per-point noise, random ids) — friendly data
compresses far better. Every number is lossless: bit-exact f64 round-trips
verified after flush + cold recovery.

### Ingest (1M points / entries / spans, single transaction)

| path | rate | vs plain table |
|---|---|---|
| metrics, Tier 1 SQL rows | 2.3M pts/s | plain: 4.2M |
| **metrics, Tier 2 batch blob** | **23.8M pts/s** | **5.6x faster than plain** |
| logs vtab | 1.1M entries/s | plain: 3.6M |
| traces vtab | 0.8M spans/s | plain + trace_id index: 1.0M |

Flush of 1M buffered points: ~110ms, paid at flush cadence, not per point.

### Storage (bytes per row, on-disk after close)

| dataset | plain | vtab | ratio |
|---|---|---|---|
| metrics, hostile (1000 series, ms-jitter, noisy values) | 52.6 | **8.3** | **6.3x** |
| metrics, friendly (regular interval, patterned values) | 46.7 | 0.23 | ~200x |
| logs, 1M entries | 120.3 | **8.9** | **13.5x** |
| traces, 960k spans (plain has a trace_id index) | 161.6 | **37.4** | **4.3x** |

### Queries (cold reopen, vtab vs plain table in the same file)

| query | plain | vtab | |
|---|---|---|---|
| logs `level='error'` count (50k rows) | 34.5ms | **15.3ms** | 2.3x |
| logs `service+level+ts` range | 119.7ms | **4.2ms** | 28x |
| traces `status='error'` count | 38.6ms | **2.8ms** | 13.8x |
| metrics name+range (10k rows of 1M) | — | **2.0ms** | pushdown |
| metrics 1-min dashboard grid (Q2 kernel) | 17.9ms raw + client eval | **1.6ms** | 11x |
| logs `message LIKE '%timeout%'` (default) | **73.9ms** | 344ms | scan: plain wins |
| logs `LIKE` with `message_index='trigram'` | 74.5ms | **48.3ms** | 1.5x |
| traces `trace_id` point lookup | **0.005ms** | 2.0ms | B-tree wins |

The last two rows are the honest ones: a compressed store decompresses to
scan, and nothing beats a native B-tree at point lookups. The trade is 4–14x
less disk and telemetry that lives inside your database.

Codec decode throughput on this machine: 1.0–1.2 GB/s (logs/traces,
`bench-codec`). Every query result in the benchmarks is checked against a
plain-table oracle in the same database.

## Migrating from timeless FsStore data

`tools/bench`'s `import` binary replays an existing timeless data
directory into a `timeless_metrics` vtab through the Tier 2 blob path,
then verifies **every point** before reporting success (bit-exact for
finite values; NaN samples — e.g. Prometheus staleness markers — are
preserved in storage and surface as SQL `NULL`, an inherent SQLite
REAL limitation; engine-level reads return the true NaN bits):

```sh
cargo run --release --bin import -- /var/lib/timeless/data \
  ./libtimeless_ext.dylib metrics.db metrics
```

For the query side of the swap, `timeless_core::waist` pins the two-call
contract (`query_multi` + `list_metrics`) the PromQL evaluator binds
against, with `=`/`!=` matchers in-waist and regex matchers evaluated
above it via `list_series` + `query_multi_ids`.

## Trust but verify

The test harness is the most serious part of this repo:

- **Randomized property oracle** (`tools/bench`, bin `oracle`): ~50k random
  ops per seed — inserts, flush/optimize/compact at random points, every
  pushdown plan family, transactions with rollback, prunes — every result
  compared against mirrored plain tables, order-insensitive, floats by bit
  pattern. It found a real engine bug (chunk-index shadowing); trust it.
- **Crash suite** (`tests/crash.sh`): five rounds of `kill -9` at random
  moments mid-ingest, then integrity + watermark + no-dangle assertions.
- **Compression honesty** (`timeless-core` tests): 1M points verified
  bit-exact after recovery, sizes measured from disk, not bookkeeping.
- **CLI integration suite** (`tests/cli.sh`): ~25 sections through the real
  sqlite3 CLI — lifecycles, `EXPLAIN QUERY PLAN` pushdown proofs, reopen
  recovery, rollback (including auto-flush inside `BEGIN`), malformed-input
  rejection, Prometheus ingest, two-connections-one-process sharing.

```sh
cargo test -p timeless-codec -p timeless-core   # unit + property tests
./tests/cli.sh                                  # full integration suite
cd tools/bench
cargo run --release --bin oracle -- ../../target/release/libtimeless_ext.dylib
cargo run --release --bin bench  -- ../../target/release/libtimeless_ext.dylib
cargo run --release --bin query-read -- ../../target/release/libtimeless_ext.dylib
```

`query-read` is the direct-SQL read baseline used by
[QUERY_PERFORMANCE_PLAN.md](QUERY_PERFORMANCE_PLAN.md). It covers exact,
narrow, and full raw fan-out; host-side aggregate/latest fallbacks; grid,
window, and rollup kernels; and the first read after publishing a flush.

See [TESTING.md](TESTING.md) for the full guide and the rules for fair
benchmark numbers.

Rust hosts that provide their own loadable-extension entry points can call
`timeless_ext::register_telemetry(&connection)` to register the three
telemetry tables and query TVFs. The embedding surface deliberately excludes
the development spike and separately packaged dbhealth modules.

## Status & limits

**Experimental.** Built as a rapid POC (2026-07); the engine lineage is
production (extracted from
[timeless_metrics](https://github.com/awksedgreep/timeless_metrics)' Rust
core), the harness is serious, but the extension itself is days old. Known
limits, kept honestly ([full list](RESULTS.md#known-limits-documented-accepted-for-poc)):

- Whole-transaction ROLLBACK only — no SAVEPOINT-granular rollback.
- Buffered (pre-flush) points are visible across connections in the same
  process before COMMIT — a deliberate dirty-read trade, documented in
  RESULTS.md; *flushed* data is fully transactional.
- `ts` equality is re-checked by SQLite; only ranges and indexed dimensions
  are pruned.
- Retention is manual (`'prune:<ts>'`) — no background jobs by design; the
  vtab is passive.

## Repository layout

```
crates/
  timeless-core/    engines: pco chunk store (metrics), columnar block store
                    (logs), span block store (traces) — no SQLite dependency
  timeless-codec/   typed column encoders with adaptive strategy selection
  timeless-ext/     the loadable extension: three vtabs + shadow-table stores
tests/              cli.sh (integration), crash.sh (kill -9 durability)
tools/bench/        bench, bench-logs, bench-traces, bench-codec, oracle
                    (bundled SQLite — no system sqlite3 needed)
PLAN.md             design history and decision log
RESULTS.md          measured results, honest asterisks, known limits
TESTING.md          how to run everything yourself
```

## License

[MIT](LICENSE)
