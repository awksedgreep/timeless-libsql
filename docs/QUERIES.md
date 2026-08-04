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
  FROM timeless_raw_frame(
    'metrics', 'cpu_usage', NULL, :t0, :t1, :max_work_points);

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

## PromQL nameless and multi-name selectors

The Rust metrics API accepts Prometheus selectors that identify series only
by labels. For example, this instant query selects every metric name whose
series has `job="api"`:

```text
GET /prometheus/api/v1/query?query=%7Bjob%3D%22api%22%7D&time=1700100010
```

The planner first reads metric names and matching series through
`timeless_series`, without decoding chunks. It then issues one bounded,
exact-metric `timeless_raw_frame` read for each selected name and composes the
Prometheus result in Rust. Metric names and canonical labels determine stable
output order. Label matchers retain anchored-regex and missing-as-empty
semantics; the upstream-invalid `{job=~".*"}` form fails because every matcher
can match the empty string.

Direct SQL users perform the same two public steps: enumerate candidate
`name` values with `timeless_series('metrics')`, then bind each name to
[`SQL-PROM-001`](QUERY_SQL_EQUIVALENTS.md#sql-prom-001-instant-selector).
There is intentionally no PromQL parser or special nameless-selector opcode in
the extension.

`__name__` uses the same anchored matcher rules as labels. Regex and negative
forms remain API planning rather than extension syntax:

```promql
{__name__=~"http_.+",job="api"}
{__name__!="http_debug",job="api"}
{__name__=~"http_.+",__name__!~"http_internal_.+",job="api"}
```

Repeated `__name__` matchers are ANDed. A name matcher that can match the empty
string does not by itself make a nameless selector legal, so
`{__name__!="missing"}` fails instead of silently selecting nearly everything.
Catalog rows are tested against every name matcher before any metric payload
is requested.

## PromQL temporal selectors

Temporal modifiers change where a selector reads without changing the outer
evaluation timestamps returned by an instant-vector or range query:

```promql
http_requests_total offset 5m
http_requests_total offset -30s
http_requests_total @ 1700300010
http_requests_total @ start()
http_requests_total @ end() offset 10s
```

The planner resolves `@` first and subtracts the signed offset second. Numeric
`@` supports millisecond precision; `start()` and `end()` use the request's
outer range endpoints. Lookback and range windows are relative to that resolved
lookup time. Root range vectors keep stored sample timestamps, while selector
and function results keep the outer evaluation grid. See executable direct-SQL
forms in
[`SQL-PROM-008`](QUERY_SQL_EQUIVALENTS.md#sql-prom-008-temporal-selector-modifiers).

## PromQL subqueries

The Rust metrics API evaluates shipped instant-vector expressions over a
globally aligned inner grid:

```promql
http_requests_total[30m:30s]
avg_over_time(http_requests_total[30m:30s])
avg_over_time(http_requests_total[30m:])
avg_over_time(http_requests_total[30m:30s] @ end() offset 5m)
avg_over_time(avg_over_time(http_requests_total[5m:30s])[30m:1m])
```

The interval is `(effective_time-range,effective_time]`; explicit resolutions
are aligned to Unix epoch multiples rather than the outer query start. When
the resolution is omitted, the API uses
`TIMELESS_METRICS_PROMQL_DEFAULT_SUBQUERY_STEP_MS` (15 seconds by default).
Subquery `@` is resolved against the original request start/end before signed
`offset` is subtracted. Root subqueries are range vectors and therefore work
only on an instant-query endpoint; range functions return their result on the
outer instant/range grid.

Intermediate points share the hard `max_work_points` bound, serialized inner
matrices share the response-byte bound, and cancellation is checked during
inner execution, decode, and folding. Direct SQLite/libSQL callers can build a
selector subquery with the executable, pre-epoch-safe alignment recipe in
[`SQL-PROM-009`](QUERY_SQL_EQUIVALENTS.md#sql-prom-009-aligned-selector-subquery).
PromQL syntax and arbitrary AST composition remain in Rust, not the extension.

## PromQL unary minus

Unary minus accepts scalar or instant-vector expressions, including nested
shipped expressions:

```promql
-1
-http_requests_total{job="api"}
-avg_over_time(http_request_duration_seconds_sum[5m])
-(-http_requests_total)
```

Vector samples retain every non-name label and their evaluation timestamps,
but Prometheus removes `__name__` even for a double negation. Range evaluation
returns the usual matrix, and scalar/vector `NaN`, `+Inf`, and `-Inf` values use
the Prometheus string representation. Each composed child point counts toward
the configured intermediate-work limit; cancellation is checked while
decoding, transforming, and serializing the bounded result.

Direct SQLite/libSQL users can apply the equivalent ordinary numeric operation
with
[`SQL-PROM-010`](QUERY_SQL_EQUIVALENTS.md#sql-prom-010-unary-minus). The SQL
recipe is the stored-vector foundation, not a claim that SQLite result typing
or IEEE rendering is a PromQL envelope.

## PromQL arithmetic and default vector matching

The Rust API evaluates all stable float arithmetic combinations:

```promql
2 ^ 8
http_requests_total * 1000
100 - queue_depth
errors_total / requests_total
```

Scalar/scalar returns a scalar. Either vector/scalar direction returns the
vector's labels and grid; vector/vector uses one-to-one matching on all labels
except `__name__`, emits only timestamps present on both sides, and uses the
left labels. Every vector arithmetic result removes the metric name. Duplicate
match signatures at the same evaluation timestamp fail as an execution error
instead of producing a Cartesian product. IEEE division, modulo, and power
results use Prometheus `NaN`/`+Inf`/`-Inf` strings.

Both operands execute once across the requested grid. Their cumulative child
points count as bounded intermediate work, and cancellation is checked during
each child read, matching step, arithmetic operation, and serialization. The
public SQL foundations—including vector/scalar arithmetic and an exact-label
join—are executable in
[`SQL-PROM-004`](QUERY_SQL_EQUIVALENTS.md#sql-prom-004-vector-arithmetic-with-label-matching).
The Rust layer remains responsible for AST precedence, cardinality errors,
result types, labels, limits, cancellation, and Prometheus envelopes.

## PromQL comparison filters and `bool`

All six float comparisons work across scalar/scalar, either scalar/vector
direction, and matched vectors:

```promql
queue_depth > 100
0 <= temperature_celsius
errors_total != requests_total
latency_seconds > bool 0.5
```

Scalar/scalar comparisons require `bool` and return a scalar `0` or `1`.
Without `bool`, a vector comparison is a filter: false samples disappear and
true samples retain the vector's original value and metric name. With `bool`,
every matched sample becomes `0` or `1` and `__name__` is removed. Vector
matching, sparse timestamps, duplicate errors, cumulative work, and
cancellation use the same contracts as arithmetic.

Direct SQL users can express both forms with a public-grid `WHERE` predicate
or a `CASE`/boolean cast; see executable
[`SQL-PROM-011`](QUERY_SQL_EQUIVALENTS.md#sql-prom-011-comparison-filter-and-bool).

## PromQL set operators

`and`, `or`, and `unless` compare instant vectors by every non-name label at
each evaluation timestamp. They are true many-to-many membership operations,
not arithmetic joins:

```promql
request_errors and requests_total
request_errors unless maintenance_targets
primary_measurements or fallback_measurements
```

`and` retains every matching left sample. `unless` retains every unmatched
left sample. `or` retains every left sample and adds only right samples whose
matching signature is absent on the left at that step. The contributing
sample keeps its value, labels, and metric name. A range query repeats this
decision independently on every grid point, so a right series can appear only
at steps where no matching left sample exists.

Both child vectors execute once. Their points count toward the cumulative
intermediate-work limit, and membership/output loops check cancellation. Set
operators reject scalar and matrix operands explicitly. Direct SQLite/libSQL
users can implement the exact membership foundation with `EXISTS`,
`NOT EXISTS`, and a left-preferred `UNION ALL`; see executable
[`SQL-PROM-012`](QUERY_SQL_EQUIVALENTS.md#sql-prom-012-set-membership).

## PromQL explicit vector matching

Vector/vector arithmetic, comparisons, and set operators accept both stable
matching modifiers:

```promql
errors_total / on(service, method) requests_total
desired_replicas == ignoring(source) current_replicas
primary or on(instance) fallback
```

`on(...)` builds the match key from exactly the listed labels; a missing label
has the empty-string value. `on()` therefore places every present sample at a
step in one match group. `ignoring(...)` builds the key from every label except
`__name__` and the listed labels; `ignoring()` is default matching. Duplicate
keys fail one-to-one arithmetic/comparisons, while set operators deliberately
remain many-to-many.

For a one-to-one result, `on(...)` retains only the named labels and
`ignoring(...)` removes the ignored labels from the left result. Arithmetic
and `bool` also remove `__name__`; an `on(...)` comparison filter has only its
named labels. Set operators do not project labels and retain the complete
contributing side. Matching is repeated per evaluation timestamp and is
covered by the same cumulative work, result, byte, deadline, and cancellation
limits as default matching.

Direct SQLite/libSQL users can make the label key explicit with SQLite JSON
functions over public grids; executable
[`SQL-PROM-013`](QUERY_SQL_EQUIVALENTS.md#sql-prom-013-on-and-ignoring-label-matching)
shows both forms.

## PromQL many-to-one vector matching

`group_left` and `group_right` explicitly allow multiple series on one side of
an arithmetic or comparison match:

```promql
pod_errors + on(service) group_left(team) service_budget
service_budget - on(service) group_right(team) pod_usage
```

`group_left` makes the right side unique and retains each left-side series;
`group_right` makes the left side unique and retains each right-side series.
The optional label list copies labels from that unique “one” side. A missing
or empty included label removes the corresponding many-side label. The copy
must still leave every result labelset unique at each evaluation timestamp.

Operation direction never changes: `a - ... group_right ... b` still computes
`a - b`. For a non-`bool` comparison, the surviving sample value is likewise
the original left value; `group_right` uses the right metric identity solely
to represent its right-side result cardinality. Arithmetic and `bool` follow
their established metric-name removal rules.

The evaluator validates the one side and result uniqueness independently at
every step, supports an active one-side series changing across a range, and
keeps all child/result work bounded and cancellable. Direct SQL users can
compose the public grids and run an explicit uniqueness preflight; executable
[`SQL-PROM-014`](QUERY_SQL_EQUIVALENTS.md#sql-prom-014-group_left-and-group_right)
shows the complete foundation.

## PromQL cross-series numeric aggregations

The Rust metrics API aggregates every shipped instant-vector expression at
each evaluation timestamp:

```promql
sum(http_requests_total)
sum by (service) (http_requests_total)
sum without (instance, pod) (http_requests_total)
avg by (service) (request_duration_seconds)
min by (service) (queue_depth)
max without (instance) (queue_depth)
```

Without a modifier, all samples form one empty-label group. `by` keeps only
the named, non-empty labels; a missing grouping label therefore joins the
empty group. `without` removes its labels and always removes `__name__`.
Naming `__name__` in `by` explicitly retains it, matching Prometheus. Range
queries repeat grouping per outer grid timestamp, so sparse inputs do not
invent samples. `NaN` propagates, like-signed infinities remain infinite, and
opposite infinities produce `NaN`.

`avg` uses compensated summation for cancellation-prone finite values and
switches to a compensated incremental mean before a running-sum overflow.
This preserves a finite mean for inputs such as two `f64::MAX` samples while
retaining Prometheus `NaN` and infinity behavior.
`min` and `max` ignore NaN when another value exists, retain signed
infinities, and return `NaN` for an all-NaN group.
`count` includes every selected sample regardless of its numeric value;
`group` returns one for every non-empty group. Both accept the same
`by`/`without` modifiers and produce no output for empty input.
`stdvar` is the population variance and `stddev` its square root. The API uses
Prometheus's Welford update, returns zero for a singleton, and returns `NaN`
when any group member is NaN or infinite.

The API evaluates the bounded child once, checks cancellation while grouping,
and charges every child point to the cumulative intermediate-work limit.
Storage remains unchanged. Direct SQLite/libSQL users use ordinary `SUM` over
the public grid in executable
[`SQL-PROM-003`](QUERY_SQL_EQUIVALENTS.md#sql-prom-003-cross-series-sum-by-label).
The corresponding ordinary-SQL average is executable
[`SQL-PROM-015`](QUERY_SQL_EQUIVALENTS.md#sql-prom-015-cross-series-average-by-label);
the API adds Prometheus's compensated edge arithmetic.
Direct SQL extrema are in executable
[`SQL-PROM-016`](QUERY_SQL_EQUIVALENTS.md#sql-prom-016-cross-series-minimum-and-maximum),
including the ordinary-SQL versus packed-NaN distinction.
Cross-series count and presence use executable
[`SQL-PROM-017`](QUERY_SQL_EQUIVALENTS.md#sql-prom-017-cross-series-count-and-group),
which deliberately uses `COUNT(*)` so IEEE NaN rows are not lost as SQL NULL.
For finite, well-scaled data,
[`SQL-PROM-018`](QUERY_SQL_EQUIVALENTS.md#sql-prom-018-cross-series-population-variance-and-standard-deviation)
provides an executable second-moment recipe and states why the API's Welford
arithmetic is required for exact edge semantics.

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
    'metrics', 'cpu_usage', '{"env":"prod"}', :t0, :t1,
    :max_work_points);
```

The optional trailing `max_work_points` is an inclusive positive INTEGER cap
on conservative stored-chunk point counts plus buffered points. It is checked
before persisted payload reads. Exceeding it returns an error and no partial
frame; omit the argument for the backward-compatible unbounded call.

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

Every packed raw/batch call increments public, per-process
`timeless_stats(:table)` rows named `raw_batch_query_count`,
`raw_batch_query_total_ns`, `raw_batch_query_series_considered`,
`raw_batch_query_candidate_chunks`, `raw_batch_query_payload_bytes_read`,
`raw_batch_query_decoded_points`,
`raw_batch_query_buffered_points_considered`, and
`raw_batch_query_returned_points`. Candidate chunks are timestamp-pruned
persisted chunks; decoded points count every point decompressed from those
chunks, even when bounds later discard it. The counters are observability
only, reset on process reopen, and are not persisted or transaction state.

## Packed window batches

`timeless_window_batches` accepts the same arguments and returns the same
per-series grid as `timeless_window`, but crosses SQLite once per series rather
than once per grid point:

```sql
SELECT series_id, labels, buckets
  FROM timeless_window_batches(
    'metrics', 'cpu_usage', '{"env":"prod"}',
    :t0, :t1, 60, 300, 'avg', NULL, :max_work_points);
```

The optional argument after `fill` caps both conservative input points and the
maximum `matched series × grid points` output, independently and inclusively,
before chunk payloads are read. Bind `NULL` for `fill` to request the default
sparse form while supplying the limit. Omit both trailing arguments to retain
the original call. Zero, negative, NULL-as-a-supplied-limit, and non-integer
limits fail explicitly.

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
