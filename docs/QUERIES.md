# The query cookbook

This cookbook documents the public SQL foundation. Language compatibility and
planned query vectors are tracked separately in the
[query feature maps](QUERY_FEATURES.md), including the
[PromQL](PROMQL_FEATURE_MATRIX.md) and
[LogsQL](LOGSQL_FEATURE_MATRIX.md) matrices. A similarly named SQL kernel is
not by itself a claim of complete language semantics. Copyable statements
mapped back to individual language rows are in
[SQL equivalents for query-language features](QUERY_SQL_EQUIVALENTS.md).

Recipes for the query surface: the raw vtabs, the kernel TVFs
(`timeless_aggregate`, `timeless_aggregate_frame`, `timeless_latest`,
`timeless_latest_frame`, `timeless_grid`, `timeless_window`,
`timeless_window_batches`, `timeless_raw_frame`, `timeless_rollup`,
`timeless_rollup_batches`), the bucket TVFs, and the catalog TVFs. Every recipe is
executed by `tests/cli.sh` against a fixed dataset with hand-verified output
(cookbook recipes in §33, aggregate contract in §35, latest contract in §36),
so drift fails the suite.

Conventions used throughout:

- `ts` is in the table's native unit (`timeless_metrics` = epoch
  **seconds**, logs = ms, traces = ns). The kernels are unit-agnostic:
  `step`, `lookback`, and `window` are in the same unit as `ts`.
- All kernel windows are half-open **`(t − width, t]`**: a sample
  exactly at `t` counts, a sample exactly at `t − width` does not.
- Results are **sparse by default** — grid points with no sample in
  the window produce no row. See [Gap-fill](#gap-fill) to change that.
- Counter kernels are **NOT PromQL**: no extrapolation, no lookback
  defaults, no staleness inference. Exact-percentile kernels are the
  flip side: raw samples are kept, so `p95` is exact, not an
  `le`-bucket estimate.

## The dashboard patterns, one per TVF

```sql
-- instant-selector shape: last sample per grid point, per series
SELECT labels, ts, value
  FROM timeless_grid('metrics', 'cpu_usage', NULL, :t0, :t1, 60, 90);

-- range-vector shape: sliding-window op per grid point
--   folds sum|min|max|count|avg, counters delta|increase|rate,
--   exact percentiles pNN, trimmed mean tavg:N
SELECT labels, ts, value
  FROM timeless_window('metrics', 'requests_total', NULL, :t0, :t1, 60, 300, 'rate');

-- the same kernel, packed into one versioned blob per series for embedded hosts
SELECT series_id, labels, buckets
  FROM timeless_window_batches(
    'metrics', 'requests_total', NULL, :t0, :t1, 60, 300, 'rate');

-- pre-aggregated tier read (declared via rollups='60@0' on the vtab)
SELECT labels, ts, value
  FROM timeless_rollup('metrics', 'cpu_usage', NULL, 60, :t0, :t1, 'avg');

-- every rollup field in one versioned blob per matched series
SELECT series_id, labels, buckets
  FROM timeless_rollup_batches(
    'metrics', 'cpu_usage', NULL, 60, :t0, :t1);

-- one scalar reduction per matched series over inclusive bounds
SELECT series_id, labels, value
  FROM timeless_aggregate('metrics', 'cpu_usage', NULL, :t0, :t1, 'avg');

-- the same scalar result for every series in one versioned frame
SELECT frame
  FROM timeless_aggregate_frame(
    'metrics', 'cpu_usage', NULL, :t0, :t1, 'avg');

-- newest point per matched series over inclusive bounds
SELECT series_id, labels, ts, value
  FROM timeless_latest('metrics', 'cpu_usage', NULL, :t0, :t1);

-- every newest point in one versioned frame
SELECT frame
  FROM timeless_latest_frame('metrics', 'cpu_usage', NULL, :t0, :t1);

-- every raw series in one versioned columnar frame for wide embedded reads
SELECT frame
  FROM timeless_raw_frame('metrics', 'cpu_usage', NULL, :t0, :t1);

-- label filters: plain string = equality; {"neq"|"re"|"nre": ...} match
-- against the whole value (anchored); absent label matches as ""
SELECT labels, ts, value FROM timeless_grid('metrics', 'cpu_usage',
  '{"host": {"re": "web-.*"}, "env": {"neq": "dev"}}', :t0, :t1, 60, 90);

-- discovery: what metrics/series/labels exist? (no chunk reads)
SELECT * FROM timeless_series('metrics');
SELECT value FROM timeless_label_values('metrics', 'cpu_usage', 'host');
-- optional metric/matcher arguments filter before catalog rows cross SQLite
SELECT labels FROM timeless_series('metrics', 'cpu_usage',
  '{"host": {"re": "web-.*"}, "env": {"neq": "dev"}}');
SELECT value FROM timeless_label_values('metrics', 'cpu_usage', 'host',
  '{"env": {"neq": "dev"}}');
SELECT * FROM timeless_stats('metrics');

-- logs/traces frequency + latency dashboards
SELECT bucket_ts, group_key, n
  FROM timeless_log_buckets('logs', 'level', NULL, :t0, :t1, 60000);
SELECT bucket_ts, service, n, dur_p50, dur_p95, dur_p99
  FROM timeless_trace_buckets('traces', NULL, :t0, :t1, 60000000000);

-- trace discovery from block metadata, including the live 8,192-span buffer
SELECT value FROM timeless_trace_services('traces');
SELECT value FROM timeless_trace_operations('traces', 'checkout');

-- bounded newest-first span search; LIMIT+OFFSET is pushed into the engine
SELECT * FROM traces
 WHERE service = 'checkout' AND duration_ns >= 1000000
 ORDER BY start_ts DESC, span_id DESC LIMIT 100 OFFSET 0;
```

Unbounded `timeless_traces` scans stream one decoded block at a time. Inclusive
`start_ts` and `duration_ns` bounds plus exact service/kind/status/name filters
are applied inside the engine. When SQLite supplies an exact
`ORDER BY start_ts[,span_id] ASC|DESC LIMIT/OFFSET` shape, the engine retains
only `LIMIT + OFFSET` rows and stops at block timestamp bounds. Strict bounds
and unrecognized row predicates remain above the vtab and deliberately disable
bounded planning.

Trace service/operation discovery reads posting-list metadata rather than span
payloads. New blocks carry a collision-free service/operation pair term; if a
selected legacy block lacks the generation marker, operation discovery falls
back to exact block-at-a-time decode. Upgrades therefore remain complete.

## Scalar aggregate without raw materialization

```sql
SELECT series_id, labels, value
  FROM timeless_aggregate(
    'metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1, 'avg');
-- operations: avg | sum | min | max | count
```

Bounds are inclusive. Each non-empty matched series produces one row; empty
series and empty ranges produce none. `count` is a SQLite INTEGER. Fully
covered chunks use their persisted count/sum/min/max metadata and only partial
boundary chunks are decoded. As a result, `sum` and `avg` use chunk-local
left-to-right accumulation followed by chunk-index order; a completely flat
SQL scan can differ by normal floating-point rounding.

NaN handling is explicit: every NaN is included in `count`; any NaN propagates
through `sum` and `avg` and surfaces as SQL `NULL`; `min` and `max` ignore NaNs
when a numeric value exists and otherwise return `NULL`. Label matchers have
the same equality/anchored-regex/negative semantics as the other metric TVFs.

## Latest point without raw materialization

```sql
SELECT series_id, labels, ts, value
  FROM timeless_latest(
    'metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1);
```

Bounds are inclusive and every non-empty matched series emits at most one row.
The greatest timestamp wins. If several points share that timestamp, the first
point in stable raw engine order wins: chunk-index order, then in-chunk order,
then buffered insertion order. The engine searches candidate chunks by newest
possible timestamp and stops when an older chunk cannot change the winner.

New chunks persist the first value at their maximum timestamp as nullable
metadata, avoiding decompression for the common unbounded-latest query. On
reopen, databases created by an older extension add the column automatically;
old rows keep `NULL` and use the exact decode fallback until compaction.

## Choosing and detecting a query interface

- Use ordinary row TVFs for SQL joins, filtering, ordering, and modest result
  sets.
- Use the `*_batches` TVFs when a host wants one independently consumable blob
  per series, especially for raw, window, or rollup streams.
- Use a whole-result `*_frame` TVF when a high-cardinality host or remote
  boundary wants to fetch one columnar aggregate, latest, or raw result with a
  single SQLite row.

Detect additive modules through SQLite rather than comparing extension version
strings:

```sql
SELECT name
  FROM pragma_module_list
 WHERE name IN ('timeless_aggregate_frame', 'timeless_latest_frame')
 ORDER BY name;
```

Both rows mean both frame APIs are available. A host can prepare and reuse the
same ID-selected statement in the normal SQLite fashion; no extension-specific
binding API is involved:

```python
read_one = db.cursor()
sql = """SELECT value FROM timeless_aggregate(
           'metrics', 'cpu_usage', NULL, ?, ?, 'avg')
         WHERE series_id = ?"""
value = read_one.execute(sql, (start, stop, series_id)).fetchone()
```

## Durable series IDs as relational read handles

Resolve a catalog ID once, cache it in the host, and use ordinary equality
constraints on later reads:

```sql
SELECT series_id
  FROM timeless_series('metrics', 'cpu_usage', '{"host":"web-1"}');

SELECT ts, value
  FROM timeless_raw('metrics', 'cpu_usage', NULL, :t0, :t1)
 WHERE series_id = :series_id;

SELECT value
  FROM timeless_aggregate('metrics', 'cpu_usage', NULL, :t0, :t1, 'avg')
 WHERE series_id = :series_id;

SELECT ts, value
  FROM metrics
 WHERE series_id = :series_id AND ts BETWEEN :t0 AND :t1;
```

`series_id = ?` pushes into the base metrics table, `timeless_series`, and
every per-series metrics TVF. On the row-oriented grid, window, and rollup
TVFs it is an explicitly selectable hidden column, so old `SELECT *` shapes and
function arities do not change. The constraint intersects with the TVF's table,
metric, and matcher arguments: an ID from another metric or one rejected by the
filter returns no rows and reads no chunks.

The same constraint composes with catalog-driven joins:

```sql
SELECT s.labels, q.ts, q.value
  FROM timeless_series('metrics', 'cpu_usage', '{"env":"prod"}') AS s
  JOIN timeless_latest('metrics', 'cpu_usage', NULL, :t0, :t1) AS q
    ON q.series_id = s.series_id;
```

IDs are durable and table-scoped. They survive flush, reopen, backup, restore,
compaction, and retention, but an ID from one independently created database
must not be used in another. INTEGER affinity is honored (`1`, `1.0`, and
`'1'` select the same handle); NULL, non-integral, malformed, and missing IDs
match nothing. The initial API intentionally supports equality only, not
`IN (...)`.

## Packed aggregate frame

`timeless_aggregate_frame` has the same arguments and semantics as
`timeless_aggregate`, but emits one row for the complete non-empty result:

```sql
SELECT frame
  FROM timeless_aggregate_frame(
    'metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1, 'avg');
```

The versioned little-endian `TAF1` layout is:

```text
"TAF1" | aggregate_kind:u8 | flags:u8=0 | reserved:u16=0 |
series_count:u32 | series_ids:i64[series_count] |
validity_bitmap:u8[ceil(series_count/8)] |
value_words:u64[series_count]
```

Aggregate kinds are `avg=0`, `sum=1`, `min=2`, `max=3`, and `count=4`.
Valid float words contain IEEE-754 bits. Count words are nonnegative SQLite
INTEGER values. A clear validity bit is SQL `NULL`, its word must be zero, and
count is never NULL. Empty series are omitted; if every series is empty the TVF
emits no row. Series order is unspecified and labels attach through
`timeless_series`.

Rust callers can use
`timeless_ext::query_frame::decode_aggregate_frame`; the dependency-free
[Python decoder](../examples/query_frames.py) consumes the same test fixtures.
Both reject unknown versions, flags, reserved bits, nonzero bitmap padding,
non-canonical NULLs, invalid kinds, and inconsistent lengths.
`series_count` is limited to `u32`; the encoder uses checked host-size
arithmetic and also remains subject to SQLite's configured maximum BLOB size.

## Packed latest frame

`timeless_latest_frame` likewise returns the complete non-empty
`timeless_latest` result in one row:

```sql
SELECT frame
  FROM timeless_latest_frame(
    'metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1);
```

The versioned little-endian `TLF1` layout is:

```text
"TLF1" | series_count:u32 |
series_ids:i64[series_count] | timestamps:i64[series_count] |
validity_bitmap:u8[ceil(series_count/8)] |
value_bits:u64[series_count]
```

Only series with a point in the inclusive range appear. Timestamp and
duplicate-winner semantics are identical to the row TVF. A clear validity bit
represents the row interface's SQL NULL for a NaN value and requires a zero
word. Use `timeless_ext::query_frame::decode_latest_frame` or the
[Python decoder](../examples/query_frames.py). `TLF1`, like every packed query
format, is an additive result envelope and never appears in shadow tables or
replication-visible storage.
`series_count` is limited to `u32`; frame-size arithmetic is checked before
allocation and SQLite's configured maximum BLOB size remains the practical
upper bound.

## Packed raw frame

`timeless_raw_frame` accepts the same table, metric, matcher filter, and
inclusive bounds as `timeless_raw_batches`, but returns one row for the whole
non-empty result set:

```sql
SELECT frame
  FROM timeless_raw_frame(
    'metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1);
```

The versioned little-endian frame is:

```text
"TRF1" | series_count:u32 | total_points:u64 |
series_ids:i64[series_count] | point_counts:u32[series_count] |
timestamps:i64[total_points] | value_bits:u64[total_points]
```

The point counts partition both point columns into consecutive per-series
slices. Empty series are omitted, timestamps retain the stable raw-query order
inside each slice, and IEEE-754 value bits are preserved. Series slice order
is unspecified, just like SQL rows without `ORDER BY`; use the IDs to attach
catalog labels. Reject unknown magic, a length inconsistent with either header
count, or point counts whose sum differs from `total_points`.

The row-oriented `timeless_raw` and one-row-per-series
`timeless_raw_batches` interfaces remain available. `TRF1` is additive and
changes neither shadow-table storage nor replication-visible formats.

## Packed window batches

`timeless_window_batches` accepts the same arguments and returns the same
per-series grid as `timeless_window`, but crosses SQLite once per series rather
than once per grid point:

```sql
SELECT series_id, labels, buckets
  FROM timeless_window_batches(
    'metrics', 'cpu_usage', '{"env":"prod"}',
    :t0, :t1, 60, 300, 'avg');
```

The `buckets` blob is versioned and little-endian:

```text
"TWB1" | count:u32 | timestamps:i64[count] |
validity_bitmap:u8[ceil(count/8)] | value_bits:u64[count]
```

Validity bit `i` is bit `(i % 8)` of byte `(i / 8)`. Sparse calls contain
only present points, so every bit is set. With trailing `fill='null'`, every
grid timestamp is encoded and a clear bit represents SQL `NULL`; the matching
value slot is zero and must be ignored. Reject unknown magic and lengths that
do not match the count. The row-oriented TVF remains the convenient SQL form;
the packed form is for host-language and remote boundaries where row crossings
are measurable.

## Packed rollup batches

`timeless_rollup_batches` takes the table, metric, filter, resolution, and
inclusive query bounds used by `timeless_rollup`, but returns every aggregate
at once. One row is emitted per non-empty matched series:

```sql
SELECT series_id, labels, buckets
  FROM timeless_rollup_batches(
    'metrics', 'cpu_usage', '{"env":"prod"}', 300, :t0, :t1);
```

The versioned little-endian blob is:

```text
"TRB1" | count:u32 |
bucket_ts:i64[count] | count:u64[count] | avg_bits:u64[count] |
sum_bits:u64[count] | min_bits:u64[count] | max_bits:u64[count] |
last_ts:i64[count] | last_value_bits:u64[count]
```

`avg` is computed from the stored sum and count at read time, just as it is in
the row TVF. Count stays integer-exact instead of passing through SQLite REAL;
the float columns preserve their IEEE-754 bits. `last_ts` exposes the timestamp
used to choose the stored last value and lets direct users retain the complete
rollup contract. Reject unknown magic and any length other than `8 + count *
64` bytes. The on-disk rollup payload and replication-visible shadow rows are
unchanged; `TRB1` is only the public query envelope. The row-oriented
`timeless_rollup` remains available for ordinary SQL and single-aggregate
queries.

## Gap-fill

Charting libraries want dense grids. Two ways to get one:

**Native (preferred):** the optional trailing `fill` argument on
`timeless_grid` and `timeless_window` — `'none'` (default) or
`'null'`:

```sql
-- every grid point emitted per matched series; value is NULL where
-- the lookback window is empty
SELECT labels, ts, value
  FROM timeless_grid('metrics', 'cpu_usage', NULL, :t0, :t1, 60, 90, 'null');
```

The per-series absence rule still holds: a series with **no** points on
the grid at all emits nothing, filled or not (matching the waist's
`query_multi` omission rule). Gap-fill is presentation mechanics only —
which points *have* values is decided by the same kernel either way.

**Portable SQL alternative** (single series; also useful for
right-edge padding beyond the data):

```sql
SELECT gs.value AS ts, g.value
  FROM generate_series(:t0, :t1, 60) gs
  LEFT JOIN timeless_grid('metrics', 'cpu_usage', '{"host":"web-1"}',
                          :t0, :t1, 60, 90) g
    ON g.ts = gs.value;
```

## Reset-corrected counter rate in pure SQL

When you need counter math over the **raw vtab** (e.g. a range that
mixes filters the kernels don't express), the standard reset-adjustment
rule in window functions:

```sql
WITH s AS (
  SELECT ts, value,
         LAG(value) OVER (PARTITION BY labels ORDER BY ts) AS prev
    FROM metrics
   WHERE name = 'requests_total' AND ts > :t0 AND ts <= :t1
)
SELECT SUM(CASE WHEN prev IS NULL      THEN 0            -- first sample: no step
                 WHEN value >= prev     THEN value - prev -- monotone step
                 ELSE value END) AS increase              -- reset: counter restarted
  FROM s;
-- rate = increase / (:t1 - :t0)
```

This computes exactly what `timeless_window(..., 'increase')` computes
over the window `(:t0, :t1]` — §33 asserts the two agree. **Prefer the
kernel** when the shape fits: it decompresses once in the engine and
ships grid points, not raw samples, and it's the form that stays fast
over sqld/HTTP.

## Top-k per bucket

"Top 2 hosts by average CPU per minute" — `ROW_NUMBER` over a bucketed
aggregate (works on the raw vtab; substitute a `timeless_window` call
as the inner query for big ranges):

```sql
WITH b AS (
  SELECT labels, (ts / 60) * 60 AS bucket_ts, AVG(value) AS v
    FROM metrics
   WHERE name = 'cpu_usage' AND ts >= :t0 AND ts <= :t1
   GROUP BY labels, bucket_ts
),
r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY bucket_ts ORDER BY v DESC) AS rn
    FROM b
)
SELECT bucket_ts, labels, v FROM r WHERE rn <= 2
 ORDER BY bucket_ts, rn;
```

## Cross-metric joins

Error ratio = two kernel calls joined on `(labels, ts)` — grids from
the same `(start, stop, step)` land on identical grid points, which is
what makes this join safe:

```sql
SELECT e.ts, e.labels, e.value / r.value AS error_ratio
  FROM timeless_grid('metrics', 'errors_total',   NULL, :t0, :t1, 60, 90) e
  JOIN timeless_grid('metrics', 'requests_total', NULL, :t0, :t1, 60, 90) r
    ON r.labels = e.labels AND r.ts = e.ts;
```

(`labels` is canonical JSON — sorted keys, minimal escaping — so string
equality is label-set equality.)

## Outlier exclusion, explicitly

The engine never decides what an outlier is; you say so in SQL. Three
escalating options:

**Trimmed mean (kernel):** drop a fixed fraction from each tail —
`timeless_window(..., 'tavg:5')`.

**IQR fences (Tukey):** quartiles from the exact-percentile kernel, cut
raw samples outside `[q1 − 1.5·IQR, q3 + 1.5·IQR]`:

```sql
WITH fences AS (
  SELECT (SELECT value FROM timeless_window('metrics', 'latency', NULL,
                                            :t1, :t1, 1, :t1 - :t0, 'p25')) AS q1,
         (SELECT value FROM timeless_window('metrics', 'latency', NULL,
                                            :t1, :t1, 1, :t1 - :t0, 'p75')) AS q3
)
SELECT AVG(value) AS robust_avg
  FROM metrics, fences
 WHERE name = 'latency' AND ts > :t0 AND ts <= :t1
   AND value BETWEEN q1 - 1.5 * (q3 - q1) AND q3 + 1.5 * (q3 - q1);
```

**σ-based (2-sigma):** population stddev in plain SQL:

```sql
WITH stats AS (
  SELECT AVG(value) AS mu,
         sqrt(AVG(value * value) - AVG(value) * AVG(value)) AS sigma
    FROM metrics WHERE name = 'latency' AND ts > :t0 AND ts <= :t1
)
SELECT AVG(value) AS robust_avg
  FROM metrics, stats
 WHERE name = 'latency' AND ts > :t0 AND ts <= :t1
   AND ABS(value - mu) <= 2 * sigma;
```

Caveat worth knowing: with tiny samples a single huge outlier inflates
σ enough to mask itself — IQR fences and `tavg:N` are the sturdier
tools there.
