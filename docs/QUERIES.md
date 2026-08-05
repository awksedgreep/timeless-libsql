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
`timeless_rollup_batches`), bounded log row/count/value surfaces, bucket TVFs,
and catalog TVFs. The parameterized language mappings live in
[`QUERY_SQL_EQUIVALENTS.md`](QUERY_SQL_EQUIVALENTS.md) and are executed by the
Rust query harness against the real extension so documentation drift fails
local validation.

Conventions used throughout:

- `ts` is in the table's native unit (`timeless_metrics` = epoch
  **seconds**, generic logs = ms, traces = ns). The release logs server creates
  a microsecond table. The kernels are unit-agnostic:
  `step`, `lookback`, and `window` are in the same unit as `ts`.
- All kernel windows are half-open **`(t − width, t]`**: a sample
  exactly at `t` counts, a sample exactly at `t − width` does not.
- Results are **sparse by default** — grid points with no sample in
  the window produce no row. See [Gap-fill](#gap-fill) to change that.
- Counter kernels are **NOT PromQL**: no extrapolation, no lookback
  defaults, no staleness inference. Exact-percentile kernels are the
  flip side: raw samples are kept, so `p95` is exact, not an
  `le`-bucket estimate.
- Binary metric batches and packed raw frames retain all IEEE value bits,
  including distinct NaN payloads. Ordinary SQLite `REAL` projection maps a
  NaN to `NULL`, and the kernels do not infer that every NaN is a Prometheus
  stale marker. See `PQL-S17` in the feature matrix for the explicit deferred
  ingress/query prerequisites.

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

-- bounded ordered log rows: an output LIMIT does not replace the work guard
SELECT ts, level, message, metadata FROM logs
 WHERE service='api' AND ts BETWEEN :t0 AND :t1
   AND max_work_entries=:max_work_entries
 ORDER BY ts DESC LIMIT 100 OFFSET 0;

-- exact count and bounded field discovery without rowset materialization
SELECT n FROM timeless_log_count(
  'logs', '{"level":"error"}', NULL, :t0, :t1, :max_work_entries);
SELECT value FROM timeless_log_values(
  'logs', 'host', '{"level":"error"}', NULL,
  :t0, :t1, 1000, :max_work_entries);
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

## Bounded log queries

`max_work_entries` is an optional positive, inclusive guard over buffered
entries considered plus candidate persisted entries charged before payload
decode. It is independent of result cardinality: `LIMIT 1` cannot conceal a
database-wide scan. Exceeding the cap returns an error and no partial rows,
count, or value set. A bound-pruned block is not charged; a fully covered block
that contributes to `timeless_log_count` from metadata does not consume row-
decode work.

The guard is available on all three direct SQLite/libSQL surfaces:

- hidden equality input `timeless_logs.max_work_entries`;
- optional sixth argument to `timeless_log_count`; and
- optional eighth argument to `timeless_log_values`.

Earlier arities remain backward-compatible and unbounded. Embedded hosts that
need a hard policy should verify the corresponding `query_surfaces` flag from
`timeless_capabilities()` and always bind the cap. SQLite progress handlers and
`sqlite3_interrupt()` are observed between block reads/decodes, and a cancelled
connection remains reusable. LogsQL syntax, typed post-filters, result limits,
deadlines, and HTTP errors remain Rust signal-API responsibilities. The
[SQL cookbook](QUERY_SQL_EQUIVALENTS.md#logsql-foundations-and-equivalents)
contains executable direct-user recipes, including bounded rows/count/value
discovery and `SQL-LOG-007` through `SQL-LOG-009` for substring, exact and
presence predicates, and boolean composition. `SQL-LOG-010` through
`SQL-LOG-013` cover typed field discovery/projection, current-row filters,
empty and unique counts, lossless values, numeric aggregates, median, and
explicit-window rates using only the public `logs` table and SQLite JSON1.
Zero, negative, NULL, and non-integer equality guards fail, including a
supplied NULL positional TVF guard. With no equality guard the hidden column
projects NULL, so `logs.max_work_entries IS NULL` selects the compatible
unbounded form.

The Rust LogsQL layer now composes these public rows into case-sensitive
Unicode word and phrase filters, word/phrase prefixes, literal substring,
bounded RE2-compatible regexp, case-insensitive forms, full-message exactness,
typed numeric comparisons and open/closed ranges, logical retained-value
types, and `NOT`/`AND`/`OR` expressions with parentheses. Indexed service,
severity, time, and configured metadata constraints are pushed only when they
are safe top-level conjuncts; `OR` and `NOT` remain above the extension so
candidate pruning cannot change truth values. Regex and every decoded-row
predicate observe the request cancellation flag and `max_work_entries` before
a matching row may be returned.

### LogsQL pattern matching

The Rust LogsQL API implements VictoriaLogs-compatible `pattern_match(...)`,
`pattern_match_full(...)`, `pattern_match_prefix(...)`, and
`pattern_match_suffix(...)`. They search anywhere, require the complete field,
anchor at the beginning, or anchor at the end, respectively. Function names
are ASCII case-insensitive. A pattern is one quoted argument (or one simple
unquoted compound token):

```text
pattern_match("request_id=<UUID>")
message:pattern_match_prefix("job <N>")
context.attempt:pattern_match_full("<N>")
* | filter peer:pattern_match_suffix("ip=<IP4>")
```

The seven recognized placeholders follow the pinned VictoriaLogs matcher,
including its deliberately structural rather than validating interpretation:

| placeholder | matched shape |
|---|---|
| `<N>` | decimal digits, or an even-length hexadecimal token of at least four characters when hexadecimal letters are present |
| `<UUID>` | five `<N>` components separated by `-` |
| `<IP4>` | four `<N>` components separated by `.`; octet ranges are not validated |
| `<TIME>` | three `<N>` components separated by `:`, with optional `.` or `,` fraction |
| `<DATE>` | three `<N>` components separated by either `-` or `/` |
| `<DATETIME>` | `<DATE>`, `T` or space, `<TIME>`, and an optional `Z` or numeric offset |
| `<W>` | one Unicode General_Category Letter/Decimal_Number/underscore word or one valid quoted string; other number classes and combining marks are boundaries |

Unknown `<...>` text is literal. Empty any/prefix/suffix patterns match every
text value; an empty full pattern matches only textual empty. For this textual
operator only, missing and JSON null project as empty, strings use their exact
UTF-8 bytes, and retained booleans, numbers, arrays, and objects use compact
JSON text without changing the stored type. Typed equality, presence, and
numeric filters retain their stricter missing/null/type distinctions.

Pattern matching is bounded Rust API composition over the public `logs` rows.
It honors the existing work, result, response, deadline, and cancellation
limits and does not inspect private shadow tables. There is no claimed ordinary
SQL equivalent: SQLite `LIKE` and `GLOB` cannot faithfully reproduce the seven
token scanners, quoted-string escapes, Unicode word categories, and partial-
match restart behavior. This evidence does not justify a SQLite extension
primitive because it would not eliminate the already-required field decode.

Static exact membership uses `in(v1, ..., vN)`. Values are case-sensitive,
quoted or unquoted, and matched against the same non-mutating rich textual
projection used by exact-prefix filters. `in()` matches nothing, a trailing
comma is accepted, and a quoted `"*"` is literal. Any standalone unquoted `*`
inside `in`, `contains_any`, or `contains_all` is instead a field-independent
no-op: it matches every bounded row even when the named field is absent.
Query-backed lists remain explicitly unsupported rather than silently
approximated.

`contains_all(v1, ..., vN)` requires every non-empty static argument to match
the same field as a case-sensitive VictoriaLogs phrase. Letter, digit, and
underscore characters at either edge require Unicode word boundaries; quoted
phrases preserve their bytes. Arguments are independent, so
`contains_all(ssh, "login fail")` permits unrelated bytes between the two
matches. Duplicates do not change the result, a trailing comma is accepted,
and `contains_all()` or `contains_all("")` is a field-independent true
predicate. Missing and null project to empty text, strings retain their bytes,
and booleans, numbers, arrays, and objects use compact JSON text only while
matching—the retained metadata type is unchanged.

`contains_any(v1, ..., vN)` uses the same projection and phrase-boundary rules,
but succeeds when at least one static argument matches. `contains_any()`
matches nothing. Any empty-string argument is a field-independent true
predicate, so even `missing:contains_any("")` matches every bounded row.
Duplicates do not change results, a trailing comma is accepted, quoted stars
remain literal, function names are case-insensitive, and a non-empty list does
not match a missing field. For example, `contains_any(error, "login failed")`
matches either phrase; it does not require both.

`field:json_array_contains_any(v1, ..., vN)` selects only a retained JSON
array and succeeds when any top-level primitive element has the same exact
textual representation as a static candidate. Decoded strings compare by
case-sensitive bytes; numbers use their retained semantic JSON spelling;
booleans compare as `true` or `false`; and JSON null compares as `null`.
Nested arrays and objects are ignored rather than stringified. Missing fields,
scalars, objects, and empty arrays do not match. An empty candidate list is
false, while `json_array_contains_any("")` matches only an actual empty-string
array element. A trailing comma is accepted, duplicates are irrelevant,
function names are case-insensitive, a quoted `"*"` is literal, and an
unquoted `*` is invalid for this function. Query-backed lists remain deferred
as `LQL-F38`.

Timeless intentionally applies this operation to its retained semantic JSON.
For example, a stored `"a\u0062"` array element is decoded to `ab` and matches
the candidate `ab`. The pinned VictoriaLogs implementation compares a raw
array lexeme in that shortcut and does not make that escaped-spelling match.
Timeless records this as a stronger typed-data interpretation instead of
retaining private lexical spellings or mutating storage.

Direct SQLite/libSQL users can express static membership with parameterized
`IN` and a field no-op by omitting the field predicate. Executable
`SQL-LOG-015` and `SQL-LOG-016` document both forms, including existing hidden-
column pruning for a declared string-only index key. `SQL-LOG-017` uses public
`json_each` rows for exact top-level JSON-array primitive membership. These API
constructs do not require a private table or new extension primitive.

There is intentionally no `contains_all` or `contains_any` SQL recipe. Portable
SQLite `LIKE`, `GLOB`, and `instr` cannot reproduce the required Unicode-
category word boundaries, and adding a storage primitive would not avoid the
public row decode already required for arbitrary rich fields. Direct SQL users
can compose intentionally looser substring predicates when that is their
desired contract; those predicates are not labeled LogsQL parity.

`field:string_range(minimum, maximum)` compares the complete textual field in
plain unsigned UTF-8 byte order. It includes `minimum`, excludes `maximum`,
and therefore makes equal or inverted bounds empty. The function accepts two
quoted or unquoted bounds, a trailing comma, case-insensitive function names,
message/service/arbitrary nested fields, and logical or pipeline composition.
Missing and null project to the empty string; strings keep their exact bytes;
retained numbers, booleans, arrays, and objects use compact JSON text only for
this predicate. Invalid arity, separators, wildcards, and unterminated input
fail instead of being ignored.

Executable `SQL-LOG-019` implements the exact lower-inclusive/upper-exclusive
byte range for retained text plus missing/null-as-empty using only public
`logs` rows and SQLite JSON1. It casts both candidate and bounds to BLOB so
connection collation cannot alter byte order. Portable SQL intentionally
leaves non-string rich values to the Rust API. VictoriaLogs flattens nested
objects into dotted children before filtering; Timeless retains the object and
can compact-project a selected parent without losing its type. Both behaviors
are pinned, and no extension primitive is added because the operation already
uses the required public decoded rows.

`field:len_range(minimum, maximum)` measures the complete textual projection
in Unicode code points and includes both non-negative bounds. A multibyte
character such as `é` therefore has length one. Missing and null project to
length zero; strings retain their exact text; and retained numbers, booleans,
arrays, and objects use compact JSON text only while this predicate runs. An
inverted range matches nothing. Function names are case-insensitive, a
trailing comma is accepted, and message/service/arbitrary nested fields plus
logical and pipeline composition are supported.

Bounds follow the pinned VictoriaLogs unsigned grammar: quoted or unquoted
integers, base prefixes, digit separators, `inf`, byte-size expressions, and
duration expressions are accepted; negative values, unsuffixed fractions,
bad arity, missing separators, and unterminated calls fail explicitly.
Executable `SQL-LOG-020` uses public `logs` rows, JSON1, and SQLite
`length(TEXT)` for exact retained-string and missing/null-as-empty semantics.
Portable SQL deliberately leaves rich-value projection and language grammar
to the Rust API. VictoriaLogs flattens objects before filtering, while
Timeless retains and can length-project the selected parent without losing
its type. No new extension primitive or storage format is involved.

The retained rich-log model intentionally differs from VictoriaLogs where
flattening would discard information. Numeric strings are not coerced, and
integer comparisons remain exact beyond 2^53. `field:("")` provides the
VictoriaLogs-compatible missing/null/empty predicate, while exact typed forms
continue to distinguish those states. `field:*` includes present zero, false,
arrays, and objects but excludes null and the empty string. `value_type(...)`
reports the stored logical JSON type rather than exposing private block
encoding choices. These decisions preserve embedded SQLite/libSQL value
fidelity without changing batching, compression, indexes, or on-disk formats.

Ordered LogsQL pipeline transforms run on the SQLite reader thread over the
same bounded public rows, so the HTTP deadline and cancellation flag cover
both storage and composition. `fields`/`keep` rebuild dotted nested paths and
a following `filter`/`where` evaluates the transformed row. `field_names`
discovers top-level response fields and counts presence, while
`field_values` returns deterministically ordered typed values and retains
missing as an omitted result field. Neither operation invents `_stream` or
`_stream_id`; those remain deferred until Timeless declares a stored stream
identity.

The API's typed statistic layer supports field/empty counts, exact and hashed
unique cardinality, typed unique values, lossless ordered values, numeric
sum/average/extrema/median, and interval rates. `values(field)` uses an object
with `items` and an exact `missing` count because an ordinary JSON array cannot
distinguish missing from a stored null. Numeric strings are not numbers. An
explicit positive `limit` bounds result or unique state according to the
operator; `limit 0` means no operator-specific cap, but never disables
`max_result_rows`, `max_work_rows`, `max_response_bytes`, or the deadline.
Pipelines fail closed if the bounded public row set would be incomplete; they
do not aggregate a silently truncated prefix.

## Public log storage statistics

Embedded hosts can inspect log storage and schedule maintenance through the
public statistics TVF without depending on private block, term, or metadata
tables:

```sql
SELECT key, value
  FROM timeless_stats('logs')
 WHERE key IN (
   'timestamp_unit',
   'blocks', 'raw_blocks', 'compressed_blocks',
   'buffered_entries', 'disk_entries', 'total_entries',
   'bytes_on_disk', 'raw_bytes', 'compressed_bytes',
   'terms', 'index_bytes',
   'ts_min', 'ts_max',
   'optimize_source_entries', 'optimize_source_bytes'
 )
 ORDER BY key;
```

Timestamps use the table's declared `timestamp_unit`. `total_entries` includes
the live buffer, while `disk_entries` does not. The three payload-byte rows
measure stored block blobs rather than the complete SQLite database, WAL, or
freelist. `terms` is a posting-row count. `index_bytes` is the SQLite page
allocation for the log term/timestamp/metadata structures and is `NULL` when
the SQLite build does not expose `dbstat`. The `optimize_source_*` rows describe
the raw or undersized persisted blocks currently eligible as optimizer source;
they are an observation for choosing a bounded `optimize:<entries>` command,
not a separate storage contract. Statistics keys are additive, so callers
should select the keys they understand and tolerate new rows.

The extension alone owns and interprets its shadow tables. A signal server or
embedded application that needs a missing statistic should extend this public
surface instead of reading private tables or duplicating block policy.

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

## PromQL quoted UTF-8 names and comments

Prometheus 3 quoted metric and label names work through the public text-ingest
and query paths:

```promql
{"http.request/duration-秒","node.name"="東京"}
{"oracle.\"quoted\"\\温度","node.name"="大阪"}
```

The exposition parser preserves decoded UTF-8, quote, and backslash bytes as
series identity. The same identity survives `timeless_series` discovery,
compact, shutdown, and reopen. Direct SQLite/libSQL users bind the decoded
metric name as ordinary TEXT and the decoded label key in the matcher JSON;
[`SQL-PROM-001`](QUERY_SQL_EQUIVALENTS.md#sql-prom-001-instant-selector)
is the executable equivalent.

Line comments begin with `#` outside a quoted string and continue to the next
newline. They may occur before, after, or between expression tokens:

```promql
# compare the current request rate
sum by (service) (
  rate( # one bounded counter window
    http_requests_total[5m]
  )
) # trailing explanation
```

Comments are API syntax and require no extension primitive. Error and warning
positions still refer to the original query text; parentheses, calls, and
brackets inside a comment do not participate in source scanning, while `#`
inside a quoted matcher remains part of its value. A comment-only query fails
as `bad_data` rather than becoming an empty expression.

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
min_over_time(http_requests_total[30m:30s])
max_over_time(http_requests_total[30m:30s])
sum_over_time(http_requests_total[30m:30s])
count_over_time(http_requests_total[30m:30s])
last_over_time(http_requests_total[30m:30s])
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

PromQL `atan2` uses the same matching and result rules, with the left operand
as `Y` and the right operand as `X`:

```promql
vertical_displacement atan2 horizontal_displacement
queue_depth atan2 2
```

It supports scalar/scalar and either scalar/vector direction, default and
explicit vector matching, and range grids. Vector results remove the metric
name. The Rust evaluator uses deterministic Go-compatible arithmetic because
SQLite/C and Go can disagree by one last-place bit for otherwise identical
inputs. Direct SQLite/libSQL users can use ordinary `atan2(Y,X)` through the
bounded scalar/vector and label-join recipes in
[`SQL-PROM-055`](QUERY_SQL_EQUIVALENTS.md#sql-prom-055-atan2); the recipe
documents that last-bit boundary honestly.

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
`topk` and `bottomk` evaluate their scalar parameter and rank independently at
every timestamp and grouping partition while retaining each selected series'
complete original labels and metric name. Fractional `k` truncates toward
zero; values below one return no series; NaN and positive overflow are errors.
Numeric values outrank NaN in both directions, so NaN is returned only when a
partition has fewer than `k` numeric samples.
Prometheus does not define which equal-valued series wins a cutoff tie;
Timeless uses canonical labels as a deterministic tie-break without claiming
that label choice as cross-engine language behavior.
`quantile(q, vector)` sorts each group at each evaluation step and linearly
interpolates rank `q * (N - 1)`. Raw NaN participates at the low end of the
rank; `q < 0`, `q > 1`, and NaN `q` produce `-Inf`, `+Inf`, and NaN.
`count_values("label", vector)` emits one count per distinct sample value and
group at each step. The selected label overwrites an input label of the same
name before grouping. Values use Prometheus fixed shortest formatting,
including `-0`, infinities, NaN, and fully expanded decimal exponents; this
operation can approach input cardinality and remains subject to result limits.

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
Step-local ranking is executable as ordinary window-function SQL in
[`SQL-PROM-005`](QUERY_SQL_EQUIVALENTS.md#sql-prom-005-top-k-per-evaluation-step).
Finite cross-series interpolation is executable in
[`SQL-PROM-019`](QUERY_SQL_EQUIVALENTS.md#sql-prom-019-cross-series-quantile);
the recipe states why packed raw bits are needed for PromQL's NaN rank.
Raw numeric grouping for direct users is executable in
[`SQL-PROM-020`](QUERY_SQL_EQUIVALENTS.md#sql-prom-020-count-series-by-sample-value),
with Prometheus label formatting correctly retained in the API.

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

Packed window calls expose the parallel cumulative keys
`window_batch_query_count`, `window_batch_query_total_ns`,
`window_batch_query_series_considered`,
`window_batch_query_candidate_chunks`,
`window_batch_query_payload_bytes_read`,
`window_batch_query_decoded_points`,
`window_batch_query_buffered_points_considered`, and
`window_batch_query_returned_points` through `timeless_stats(:table)`.
They count the input chunks/points considered by the reduction and the sparse
grid points produced before optional null fill. Like raw counters, they are
per-process observability—not durable storage or transaction state.

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

The `avg` window fold uses compensated summation for cancellation-prone
finite values and an incremental-mean fallback before the running sum would
overflow. The `sum` fold uses the same compensated addition but retains
infinite overflow as its result. `count` includes every stored float regardless
of its IEEE value. `NaN`, infinities, and signed zero remain IEEE values in the
packed frame. The `min` and `max` folds ignore an incoming NaN once they have an
ordered extremum, replace a leading NaN with the first numeric sample, retain
NaN for an all-NaN window, and preserve the first of equal signed zeros. These
are general direct-SQL reductions; PromQL parsing, label/name
policy, timestamps, limits, and envelopes remain in the Rust metrics API.
`last_over_time` maps to the existing `timeless_grid` last-sample kernel and
retains exact IEEE bits; pinned Prometheus also retains its metric name.

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

## Mechanical reset-corrected counter rate in pure SQL

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
over the window `(:t0, :t1]` — §33 asserts the two agree. This is a useful
storage statistic, but neither it nor the native `rate` fold implements
Prometheus edge extrapolation and zero-point clamping. For exact float-series
PromQL `rate`, use executable recipe
[`SQL-PROM-029`](QUERY_SQL_EQUIVALENTS.md#sql-prom-029-rate) or the Rust metrics
API. Exact last-two-sample PromQL `irate` is documented separately as
[`SQL-PROM-030`](QUERY_SQL_EQUIVALENTS.md#sql-prom-030-irate), and exact
extrapolated PromQL `increase` as
[`SQL-PROM-031`](QUERY_SQL_EQUIVALENTS.md#sql-prom-031-increase). Exact
extrapolated gauge `delta` is
[`SQL-PROM-032`](QUERY_SQL_EQUIVALENTS.md#sql-prom-032-delta), and final-pair
`idelta` is [`SQL-PROM-033`](QUERY_SQL_EQUIVALENTS.md#sql-prom-033-idelta).
Timestamp-centered least-squares gauge `deriv` is
[`SQL-PROM-034`](QUERY_SQL_EQUIVALENTS.md#sql-prom-034-deriv); the Rust API
adds Prometheus's compensated and IEEE-exact arithmetic to that finite SQL
foundation.
Evaluation-time-anchored gauge `predict_linear` is
[`SQL-PROM-035`](QUERY_SQL_EQUIVALENTS.md#sql-prom-035-predict_linear); its
`:horizon` is measured in seconds from each outer evaluation timestamp.
Ordered float-transition `changes` is
[`SQL-PROM-036`](QUERY_SQL_EQUIVALENTS.md#sql-prom-036-changes), including the
public row surface's explicit SQL-NULL representation of stored NaN.
Strict float-counter decrease `resets` is
[`SQL-PROM-037`](QUERY_SQL_EQUIVALENTS.md#sql-prom-037-resets); it does not
relabel the extension's mechanical reset-adjusted increase/rate kernels.
Pinned Prometheus 3.13.2 classifies `double_exponential_smoothing` as an
experimental function and disables it by default. The stable Timeless tier
therefore rejects it explicitly. It is not a missing stable range reduction;
enabling it later requires a separately configured experimental API tier and
its own enabled-oracle contract.
Bounded instant-vector `abs` is
[`SQL-PROM-038`](QUERY_SQL_EQUIVALENTS.md#sql-prom-038-abs); ordinary SQLite
is exact for finite values, infinities, and signed zero, while the Rust API
retains packed NaN fidelity and PromQL labels, types, limits, and envelopes.
Bounded `ceil`, `floor`, and nearest-multiple `round` use
[`SQL-PROM-039`](QUERY_SQL_EQUIVALENTS.md#sql-prom-039-ceil-floor-and-round),
including Prometheus's exact tie and scalar-step arithmetic.
Bounded `clamp`, `clamp_min`, and `clamp_max` use
[`SQL-PROM-040`](QUERY_SQL_EQUIVALENTS.md#sql-prom-040-clamp-clamp_min-and-clamp_max);
the Rust API adds bit-exact NaN/signed-zero behavior and per-step scalar-bound
expressions to the ordinary finite SQL foundation.
Bounded `sqrt`, `exp`, `ln`, `log2`, and `log10` use
[`SQL-PROM-041`](QUERY_SQL_EQUIVALENTS.md#sql-prom-041-sqrt-exp-ln-log2-and-log10);
the Rust API preserves Prometheus `NaN`/infinity domain results that ordinary
SQLite reports as SQL NULL.
Bounded `sgn` is [`SQL-PROM-042`](QUERY_SQL_EQUIVALENTS.md#sql-prom-042-sgn);
ordinary SQL preserves row-visible signed zero and the packed Rust path
retains true NaN.
Bounded inverse trigonometric and hyperbolic functions use
[`SQL-PROM-043`](QUERY_SQL_EQUIVALENTS.md#sql-prom-043-inverse-trigonometric-and-hyperbolic-functions);
the Rust API supplies the packed IEEE/domain distinctions ordinary SQLite
reports as SQL NULL.
Bounded trigonometric and hyperbolic functions use
[`SQL-PROM-044`](QUERY_SQL_EQUIVALENTS.md#sql-prom-044-trigonometric-and-hyperbolic-functions)
with the same honest SQL-NULL versus packed-IEEE boundary.
Degree/radian conversion and scalar π use
[`SQL-PROM-045`](QUERY_SQL_EQUIVALENTS.md#sql-prom-045-deg-rad-and-pi),
including a direct SQL evaluation-time recipe for `pi()`.
PromQL label replacement is API-owned composition over the same bounded
public results:

```promql
label_replace(http_requests_total, "region", "$1", "instance", "([^.]+)\\..*")
```

The regular expression is a full-string, dot-all RE2-family match. Missing
source labels are read as empty strings; a matching empty replacement removes
the destination, while a non-match leaves the complete series unchanged.
Numbered and named captures, `__name__` as source or destination, and
Prometheus 3's nonempty UTF-8 destination-label scheme are supported. Invalid
regexes and an empty destination return an `execution` envelope. SQLite's
public JSON functions can set or remove a known constant label, but ordinary
SQLite has no portable RE2-compatible capture-and-expand operation. The
`PQL-F09` matrix foundation is therefore honestly `none`, and the cookbook
does not claim a general SQL equivalent or add an extension primitive solely
for language syntax. Replacement expansion consumes the response byte budget
incrementally, before amplified destination strings can accumulate.
Ordered label joining is also bounded API composition:

```promql
label_join(http_requests_total, "node", "/", "service", "instance")
```

Source labels are read in argument order from the original series. Missing
labels contribute empty strings, duplicate sources remain duplicated, and
zero source labels are valid. An empty joined value removes the destination;
`__name__` can be a source or destination. Values, timestamps, and the metric
name are otherwise preserved. An empty destination returns an `execution`
error. Direct SQLite/libSQL users can perform the same arbitrary-arity
operation with the parameterized public-JSON statement in
[`SQL-PROM-046`](QUERY_SQL_EQUIVALENTS.md#sql-prom-046-label_join). Joined
destination strings use the same incremental response byte budget.
PromQL absence is evaluated at every outer timestamp:

```promql
absent(up{job="api", instance=~"web-.*"})
```

The result is empty whenever any input series has a sample. Otherwise it is a
single value `1`; range queries assemble one sparse series from absent steps.
Only unique, nonempty equality matchers from a direct (optionally
parenthesized) selector become output labels. `__name__`, regex, negative,
empty, duplicate, and composed-expression matchers do not. NaN is still a
present sample. The executable public-grid equivalent is
[`SQL-PROM-047`](QUERY_SQL_EQUIVALENTS.md#sql-prom-047-absent).

Window absence uses the same output-label and sparse-result rules, but tests
the exact PromQL range interval independently at every outer timestamp:

```promql
absent_over_time(up{job="api", instance=~"web-.*"}[5m])
```

Each window is open on the left and closed on the right. Any stored sample,
including NaN, makes that step present and therefore removes it from the
result. Direct range selectors derive their unique nonempty equality labels;
subquery inputs are composed in Rust and derive none. The implementation
reuses the shipped bounded `present_over_time` plan and then performs the
step-local absence inversion, so limits and cancellation cover both stages.
Direct SQLite/libSQL users have the executable public-raw anti-join in
[`SQL-PROM-048`](QUERY_SQL_EQUIVALENTS.md#sql-prom-048-absent_over_time).

PromQL ordering is intentionally an instant-vector presentation operation:

```promql
sort(http_request_duration_seconds)
sort_desc(http_request_duration_seconds)
```

`sort` orders samples by ascending value and `sort_desc` by descending value;
both put NaN last. Labels, metric names, timestamps, IEEE values, and nested
expression label policy are preserved. Equal values have no PromQL ordering
promise, so Timeless uses canonical labels as a deterministic tie-break.
Range-query results remain label-ordered matrices rather than pretending one
series order can represent a different value ordering at every step. Direct
SQLite/libSQL callers can use the parameterized instant statement and range
matrix statement in
[`SQL-PROM-049`](QUERY_SQL_EQUIVALENTS.md#sql-prom-049-sort-and-sort_desc).

PromQL's explicit evaluation-type conversions are also API composition:

```promql
scalar(process_resident_memory_bytes{instance="api-1"})
vector(2 + 3)
```

`scalar(vector)` returns the sole sample value at each evaluation step and
returns NaN when that step has zero or multiple samples. A sole stored NaN is
still NaN. `vector(scalar)` produces one nameless series with the scalar value
at every step. Both compose with other shipped expressions and retain their
distinct scalar/vector instant and range envelopes. Direct SQLite/libSQL users
can use the executable per-step cardinality and nameless-vector statements in
[`SQL-PROM-050`](QUERY_SQL_EQUIVALENTS.md#sql-prom-050-scalar-and-vector).

Evaluation and sample time are distinct PromQL values:

```promql
time()
timestamp(up{job="api"})
```

`time()` is the current evaluation timestamp in Unix seconds and retains
subsecond range-grid precision. `timestamp(direct_selector)` returns the
selected stored sample timestamp as its value while the response sample stays
on the outer evaluation grid; `offset` and `@` alter selection without moving
that response grid. Once a unary, function, binary, aggregation, or range node
creates a new sample, `timestamp()` reports that node's evaluation time.
`timestamp` removes the metric name and preserves all other labels, including
for a stored NaN. Direct SQLite/libSQL equivalents for both clocks are in
[`SQL-PROM-051`](QUERY_SQL_EQUIVALENTS.md#sql-prom-051-time-and-timestamp).

UTC calendar extraction accepts an optional instant vector:

```promql
minute(process_start_time_seconds)
hour()
day_of_week(vector(0))
day_of_month(process_start_time_seconds)
```

With no argument, each function evaluates `vector(time())`. Finite fractional
Unix seconds truncate toward zero, Sunday is day zero, metric names are
removed, and other labels remain. NaN, either infinity, and out-of-range
values follow the pinned Prometheus maximum-Unix-second conversion rather
than becoming NaN. Direct
SQLite/libSQL users can use the parameterized UTC `strftime` foundation in
[`SQL-PROM-052`](QUERY_SQL_EQUIVALENTS.md#sql-prom-052-minute-hour-day_of_week-and-day_of_month),
with its explicitly documented SQLite calendar-range limitation.

The remaining stable UTC calendar fields use the same optional-vector and
conversion contract:

```promql
day_of_year(process_start_time_seconds)
days_in_month(vector(1709208000))
month()
year(process_start_time_seconds)
```

Day-of-year and month are one-indexed, `days_in_month` follows Gregorian leap
years, and all four preserve non-name labels while removing the metric name.
The parameterized direct SQLite/libSQL foundation, including leap-year and
zero-argument forms, is
[`SQL-PROM-053`](QUERY_SQL_EQUIVALENTS.md#sql-prom-053-day_of_year-days_in_month-month-and-year).

Classic float histograms use their ordinary cumulative `*_bucket` series:

```promql
histogram_quantile(
  0.95,
  sum by (service, le) (rate(http_request_duration_seconds_bucket[5m]))
)
```

Buckets are grouped by every label except `le` while retaining the metric name
as an internal family discriminator; both `le` and the metric name are removed
from the result. Equal numeric bounds are coalesced, material decreases are
made monotonic, relative deltas below Prometheus's `1e-12` tolerance are
ignored, and ranks interpolate linearly. A missing `+Inf` bucket, fewer than
two bounds, or a zero total returns NaN. Invalid quantiles retain Prometheus's
NaN/infinity result behavior. Malformed or absent `le` series are excluded.
If distinct metric families would produce the same visible label set at one
step after name removal, evaluation fails instead of emitting an invalid
duplicate vector.
All bucket points and the scalar quantile expression count toward the bounded
work limit, and the operation composes in Rust without another storage read.

This function applies only to classic float bucket series. Native histograms
remain deferred until the extension has an explicit typed storage model.
Direct SQLite/libSQL users can execute the parameterized bounded-grid recipe
in
[`SQL-PROM-054`](QUERY_SQL_EQUIVALENTS.md#sql-prom-054-histogram_quantile-over-classic-buckets),
including the documented distinction between its ordinary-SQL foundation and
the API's complete Prometheus float/tolerance behavior.

`histogram_fraction(lower, upper, buckets)` estimates the fraction of classic
histogram observations between two scalar bounds:

```promql
histogram_fraction(
  0.1,
  0.5,
  sum by (service, le) (rate(http_request_duration_seconds_bucket[5m]))
)
```

The pinned Prometheus classic-bucket algorithm coalesces equal numeric bounds,
requires a `+Inf` total, interpolates inside finite buckets, treats natural
zero and infinite-width buckets specially, and does not apply
`histogram_quantile`'s monotonicity repair. Bounds may be scalar expressions
evaluated per step; inverted bounds return zero, zero totals and missing
`+Inf` return NaN, and output labels omit the metric name and `le`. Metric
families remain distinct internally, so a post-name-removal collision fails
instead of emitting duplicate label sets. Every selected bucket and scalar
bound is charged to cumulative work and cancellation limits.

This support is for ordinary classic float `*_bucket` series only. Native
histograms still require a typed storage model. Direct SQLite/libSQL users can
use the bounded ordinary-SQL CDF foundation in
[`SQL-PROM-056`](QUERY_SQL_EQUIVALENTS.md#sql-prom-056-histogram_fraction-over-classic-buckets).

Prometheus 3.13.2 feature-gates `start()`, `end()`, `step()`, `range()`,
`min_of`, `max_of`, and `histogram_quantiles`; `start_timestamp()` is not a
PromQL function. The stable endpoint returns the pinned disabled/unknown
diagnostics. This does not affect the shipped selector modifiers
`@ start()` and `@ end()`. MetricsQL variants remain separately tracked and
are never enabled by silently broadening PromQL.

## Explicit MetricsQL binary operators

MetricsQL-only syntax is accepted only on the explicitly named Rust API
routes:

```text
GET /metricsql/api/v1/query?query=%28cpu_usage+%3E+90%29+default+0&time=1700100010
GET /metricsql/api/v1/query_range?query=cpu_usage+if+on%28host%29+host_up&start=1700100000&end=1700100060&step=10s
```

`default` fills a missing left value from the matching right value at the
same evaluation step. `if` retains a left value only while a matching right
value exists; `ifnot` retains it only while the right value does not exist.
Matching ignores the metric name by default and accepts `on(...)` and
`ignoring(...)`. A scalar operand is a nameless vector, so a scalar RHS can
broadcast across left label sets. The contributing left series keeps its
labels and metric name. As in pinned VictoriaMetrics 1.148.0, a join modifier
on these set-style operators does not rewrite those labels.

This differs deliberately from the stable PromQL routes, which continue to
reject `default`, `if`, and `ifnot`. MetricsQL scalar instant expressions also
return a nameless vector rather than a PromQL scalar. Timeless retains its
normal JSON error policy: invalid input is HTTP 400 `bad_data`, while
VictoriaMetrics uses HTTP 422 with error type `422`. Execution limits and
cancellation use the same bounded Rust reader path as PromQL.

The extension does not parse MetricsQL. Direct SQLite/libSQL users can express
the storage-visible mechanics with the executable public-grid statements in
[`SQL-MQL-001`](QUERY_SQL_EQUIVALENTS.md#sql-mql-001-default-if-and-ifnot).
The API remains responsible for precedence, implicit scalar vectors, label
policy, response envelopes, limits, and cancellation.

### Retaining metric names in MetricsQL

The MetricsQL routes accept `keep_metric_names` after a supported transform,
rollup, or binary operation:

```text
abs({__name__=~"cpu_usage|memory_usage"}) keep_metric_names
rate(http_requests_total[5m]) keep_metric_names
(cpu_usage / 100) keep_metric_names
sum(abs({__name__=~"cpu_usage|memory_usage"}) keep_metric_names)
```

This is an operation modifier. It retains each contributing input metric name
during evaluation; it does not guess or restore a name after the result has
already collapsed. Multiple input names can therefore remain distinct through
a transform. Default binary matching also includes the metric name while the
modifier is active. An explicit `on(host)` still matches only `host`, then
retains the left metric name in the result. This distinction matches pinned
VictoriaMetrics 1.148.0.

Bare selectors, unary expressions, and aggregations cannot carry the trailing
modifier and fail explicitly. An aggregate may consume a nested modified
operation, as in the final example, and then applies its normal nameless output
policy. The stable PromQL routes reject the syntax. Limits, cancellation,
GET/form-POST behavior, durability, and reopen use the existing bounded Rust
execution path.

SQLite/libSQL does not need a new query primitive: direct users retain the
public metric identity as an ordinary selected column. The executable
[`SQL-MQL-002`](QUERY_SQL_EQUIVALENTS.md#sql-mql-002-keep_metric_names)
recipe shows the exact form and the name-aware/`on(...)` join distinction.

### Combining and renaming MetricsQL series

The explicit MetricsQL routes support both the named and parenthesized union
forms plus `alias`:

```text
union(cpu_usage, memory_usage)
(cpu_usage, memory_usage)
alias(cpu_usage, "host_cpu_usage")
sum(union(alias(cpu_usage, "cpu"), alias(memory_usage, "memory")))
```

`union()` returns an empty vector, `union(q)` returns `q`, and both named and
parenthesized lists accept a trailing comma. Every argument is evaluated as a
bounded existing query plan. The union retains the first complete time series
when later arguments have the same metric name and labels; it never merges
their samples. Result ordering is not an argument-order contract.

`alias(q, "name")` replaces `__name__` on every returned series; an empty name
removes it. Alias does not silently choose among series that become identical
after renaming. A bare alias that creates duplicate output labelsets fails,
matching pinned VictoriaMetrics 1.148.0. Likewise, `union(1, 2)` fails because
both scalar arguments become duplicate nameless vectors. Invalid arity,
non-string alias names, and empty comma slots fail explicitly.
The `union` function name is case-insensitive. `alias` is a lowercase built-in
template in pinned VictoriaMetrics, so `ALIAS(...)` is unsupported and
Timeless preserves that distinction.

Union and alias compose beneath stable operators and aggregations, but are
accepted only by `/metricsql/api/v1/query` and
`/metricsql/api/v1/query_range`; the PromQL routes retain their original
parser and reject both forms. Child results, collision state, output bytes,
and cancellation are charged to the existing bounded Rust query envelope.

The extension does not parse either construct. Direct SQLite/libSQL users use
ordinary public-grid `UNION ALL`, project an alias as the metric-name column,
and select the lowest branch for duplicate complete labelsets. The executable
[`SQL-MQL-003`](QUERY_SQL_EQUIVALENTS.md#sql-mql-003-union-and-alias) recipe
pins that behavior and explains the bare-alias collision check.

### Setting and deleting MetricsQL labels

The explicit MetricsQL routes support bounded label transformation after any
scalar or instant-vector expression:

```text
label_set(cpu_usage, "environment", "production", "host", "rewritten")
label_del(cpu_usage, "pod", "instance")
label_set(cpu_usage, "__name__", "host_cpu_usage")
label_del(cpu_usage, "__name__")
```

`label_set` applies label/value pairs from left to right, so the last repeated
destination wins. An empty value deletes that label rather than retaining an
empty string. Both functions accept no label arguments as an identity
operation, ignore deletion of a missing label, and treat `__name__` as the
metric name. A scalar input becomes a nameless instant vector before the label
operation, matching pinned VictoriaMetrics 1.148.0.

The function names are case-insensitive built-in transforms, in contrast to
VictoriaMetrics's lowercase-only `alias` template. Trailing commas and the
otherwise redundant `keep_metric_names` modifier are accepted. Multiple
functions compose in argument order beneath ordinary operations and
aggregations. Invalid pair counts, non-string names or values, and empty
function calls fail explicitly.

Transforming multiple source series to the same complete output labelset is an
error; the API does not silently choose a winner. Generated label bytes,
intermediate points, response bytes, and cancellation checks use the existing
bounded Rust query envelope. GET and form-encoded POST, instant and range
responses, flush, shutdown, and reopen share that path. The stable PromQL
routes retain their parser and reject both functions.

The extension does not parse this syntax. Direct SQLite/libSQL users can
project the metric-name column and use standard `json_set`/`json_remove` over
public-grid labels. The executable
[`SQL-MQL-004`](QUERY_SQL_EQUIVALENTS.md#sql-mql-004-label_set-and-label_del)
recipe pins empty-value deletion, name handling, JSON paths, ordering, types,
and the duplicate-output check.

### Automatic and window-less MetricsQL rollups

On the explicitly named MetricsQL routes, every bare selector is an implicit
`default_rollup`:

```text
cpu_usage
default_rollup(cpu_usage)
default_rollup(cpu_usage[30s])
```

The first two forms are equivalent. For a range query, Timeless infers each
series' scrape interval from the interpolated 0.6 quantile of its last 20
intervals and applies VictoriaMetrics's jitter allowance. The automatic
window is at least the request step. Bind `max_lookback=30s` on the MetricsQL
request to cap this inferred default window; it does not shorten the explicit
`[30s]` form. Every range remains open on the left and closed on the right.
Instant requests use the request step directly.

The MetricsQL routes also accept the established one-argument rollups without
a bracketed selector range:

```text
avg_over_time(cpu_usage)
FIRST_OVER_TIME(cpu_usage,)
rate(http_requests_total)
changes(build_state)
timestamp(cpu_usage)
```

Supported window-less names are `avg_over_time`, `min_over_time`,
`max_over_time`, `sum_over_time`, `count_over_time`, `present_over_time`,
`stddev_over_time`, `stdvar_over_time`, `first_over_time`, `last_over_time`,
`rate`, `irate`, `increase`, `delta`, `idelta`, `deriv`, `changes`, and
`resets`. Function names are case-insensitive and a trailing comma is valid.
Statistical functions use the request step as their window. `rate`, `irate`,
and `deriv` use the pinned adjustable-window behavior; `increase`, `delta`,
`idelta`, `changes`, and `resets` can consume the bounded previous sample.
Counter functions apply VictoriaMetrics reset correction before calculating
the result.

`default_rollup`, average, minimum, maximum, first, and last retain the input
metric name. Other rollups and `timestamp` remove it. A `default_rollup` of a
scalar is a nameless vector and all forms compose under ordinary MetricsQL
operators and aggregations. Invalid arity fails with HTTP 400 `bad_data`.
Stable `/api/v1/query*` endpoints remain PromQL-only: they reject bracketless
rollups and keep `first_over_time` behind Prometheus's experimental tier.

The packed public raw path distinguishes the exact Prometheus stale-NaN bits
from an ordinary stored NaN. A stale marker ends the visible result at that
step. Timeless preserves and returns an ordinary NaN; pinned VictoriaMetrics
discards that series during ingestion, so this stronger storage-fidelity
behavior is documented rather than presented as oracle equality.

Direct SQLite/libSQL users can run the bounded automatic finite-series
selection and step-window reductions in
[`SQL-MQL-005`](QUERY_SQL_EQUIVALENTS.md#sql-mql-005-default_rollup-and-window-less-rollups).
The Rust API retains language parsing, exact packed-NaN handling, carry-in and
reset composition, names, limits, cancellation, and HTTP envelopes; no
MetricsQL syntax enters the extension.

### Complete-grid MetricsQL range aggregates

The explicit MetricsQL routes support four transformations over the complete
instant-vector evaluation grid of the current request:

```text
range_avg(cpu_usage)
range_min(cpu_usage)
range_max(cpu_usage)
range_sum(cpu_usage)
range_sum(cpu_usage * 2)
```

These are not moving windows. Each input series is evaluated from inclusive
`start` through inclusive `end` at `step`, reduced once, and its final value is
repeated at every requested timestamp. An instant request therefore reduces a
one-point grid. Scalars become nameless vectors, arbitrary shipped scalar or
instant-vector expressions compose as the argument, names are
case-insensitive, and one trailing comma is accepted.

`range_avg` uses VictoriaMetrics's incremental arithmetic. After the first
non-NaN value, every grid slot—including a missing slot—advances the average's
position denominator. `range_sum` uses ordinary binary64 addition rather than
the extension's compensated window sum. Minimum and maximum choose the later
operand when values compare equal. Leading NaNs/missing values are skipped;
later gaps carry the running value. After reduction, the last non-NaN running
value fills the complete output grid, including leading gaps. A series with no
non-NaN value is omitted.

All four functions remove `__name__`. Pinned VictoriaMetrics does this even
for `range_avg(q) keep_metric_names`, so Timeless preserves that upstream
quirk instead of pretending the modifier restores the name. If removing names
collapses two input identities, the request fails with HTTP 422 `execution`
rather than merging samples. Zero/extra arguments fail with HTTP 400
`bad_data`. The stable PromQL routes reject every `range_*` name.

VictoriaMetrics's Remote Write/query path normalizes the signed-zero oracle
fixture to positive zero. Timeless preserves the stronger stored binary64
contract: because the extrema rule chooses the later equal operand,
`range_min` and `range_max` return `-0` when that later operand has its sign bit
set. NaN, infinity, maximum-float overflow avoidance, sparse grids, duplicate
outputs, work/result limits, cancellation, GET/POST, shutdown, and reopen are
all real-extension regressions.

The API performs one bounded child evaluation and no second storage read.
Direct SQLite/libSQL users can run the slot-indexed recursive equivalent over
any public input grid with
[`SQL-MQL-006`](QUERY_SQL_EQUIVALENTS.md#sql-mql-006-range-aggregates).
MetricsQL parsing, arbitrary expression composition, implicit lookback,
collision policy, limits, cancellation, and HTTP result shaping remain Rust
API responsibilities; no extension syntax or storage format changed.

### Cumulative MetricsQL running aggregates

The explicit MetricsQL routes also support cumulative transforms over the
current request grid:

```text
running_avg(cpu_usage)
running_min(cpu_usage)
running_max(cpu_usage)
running_sum(cpu_usage)
running_sum(cpu_usage * 2)
```

Unlike `range_*`, these functions emit the running state at each evaluation
timestamp. For input `11, 13, 15`, `running_avg` returns `11, 12, 13` and
`running_sum` returns `11, 24, 39`. An instant request is a one-slot grid.
Scalars become nameless vectors, shipped scalar/vector expressions compose as
the argument, names are case-insensitive, and one trailing comma is accepted.

Leading missing/NaN slots emit nothing. After the first value, a missing or
stale slot emits the previous state; it still advances `running_avg`'s slot
index, so `1, missing, 2` becomes `1, 1, 1.3333333333333333`. If actual
arithmetic computes NaN, that timestamp is omitted instead of carrying the
prior value. Average uses VictoriaMetrics's incremental update, sum uses
ordinary binary64 addition, and minimum/maximum choose the later equal
operand.

Every running function removes `__name__`, including when followed by
`keep_metric_names`. Post-removal duplicate identities fail with HTTP 422
`execution`; invalid arity fails with HTTP 400 `bad_data`. Stable PromQL routes
reject all four function names. Timeless retains stored signed-zero bits, so
equal extrema expose the later operand's zero sign even though the
VictoriaMetrics Remote Write fixture normalizes both zero orders.

One bounded child evaluation supplies the complete grid; cumulative folding
adds no storage read. Direct SQLite/libSQL users can execute the recursive
public-grid equivalent in
[`SQL-MQL-007`](QUERY_SQL_EQUIVALENTS.md#sql-mql-007-running-aggregates).
MetricsQL parsing, arbitrary expression composition, packed missing/NaN
behavior, collisions, limits, cancellation, and HTTP envelopes remain Rust
API responsibilities; no extension syntax or storage format changed.

### Request-step-relative MetricsQL durations

The explicit MetricsQL routes resolve the `i` duration suffix against the
positive `step` parameter of the current request:

```text
count_over_time(cpu_usage[5i])
max_over_time(vector(time())[5i:1i])
cpu_usage offset 5i
cpu_usage offset -1i-1s
rate(http_requests_total[0i])
rate(http_requests_total[0i:1i])
```

Each `Ni` component contributes `N * request_step` milliseconds. Decimal and
compound components are accumulated as binary64 and the complete duration is
then truncated toward zero to a signed 64-bit millisecond value. Overflow
saturates at the corresponding `int64` limit. Duration suffixes other than an
uppercase standalone `M` are case-insensitive; uppercase `M` remains the
VictoriaMetrics numeric multiplier, while `Ms` is milliseconds. A minus on
the first offset component is inherited by later positive components, so
`offset -1i-1s` means `-(request_step + 1s)`, not `-request_step + 1s`.

Direct selector windows, subquery windows and resolutions, and signed offsets
all use the same request-owned resolution. A zero subquery resolution becomes
one request step. For ordinary range reductions, a resolved zero window also
becomes one request step. For `default_rollup`, `rate`, `irate`, and `deriv`,
an explicit `0i` remains the upstream automatic-window signal: direct
selectors and subqueries retain the scrape-cadence inference documented in
the preceding rollup section. A zero offset remains zero.

The lowering pass ignores quoted strings and comments while accepting
comments between the range/offset delimiter and the duration. It uses a
collision-checked internal marker so a legitimate explicit duration with the
same millisecond value cannot be mistaken for `0i`. Bare `i` is invalid and
returns the pinned parser diagnostic. The stable PromQL endpoints continue to
reject all `i` durations; the syntax never leaks into the primary language
tier.

All forms retain existing work, result, response, deadline, cancellation,
GET/POST, flush, shutdown, and reopen contracts. The extension receives no
MetricsQL grammar or new storage primitive. Direct SQLite/libSQL users can
bind request-step multiplication into the existing public window, grid, and
subquery recipes in
[`SQL-MQL-009`](QUERY_SQL_EQUIVALENTS.md#sql-mql-009-request-step-relative-durations);
adaptive zero-window rollups reuse `SQL-MQL-005`.

### MetricsQL query-context values

The explicit MetricsQL routes expose three case-insensitive, zero-argument
functions whose values come only from the current request:

```text
start()
end()
step()
query_contract_cpu + (start() - start())
```

`start()` and `end()` return the range request's inclusive bounds as
floating-point Unix seconds. For an instant query, both equal its evaluation
timestamp. `step()` returns the positive request step in seconds, including a
subsecond step such as `1.5`. Negative pre-epoch request bounds remain
negative. The values compose as scalar expressions; MetricsQL's established
scalar-to-vector behavior determines the outer result envelope.

Each function requires exactly zero arguments. `start_timestamp()` and
`range()` are not supported by pinned VictoriaMetrics 1.148.0 and fail
explicitly rather than being treated as aliases. Stable PromQL continues to
feature-gate its similarly named functions. Existing selector modifiers
`@ start()` and `@ end()` retain their stable PromQL meaning on both routes;
the MetricsQL context lowering does not reinterpret their direct form.

Pure context expressions perform no extension query. Composition with a
selector performs the selector's one existing bounded public read. All work,
result, response, deadline, cancellation, GET/POST, shutdown, and reopen
contracts remain unchanged. No MetricsQL grammar or request state enters the
extension. Direct SQLite/libSQL users can bind the same request context with
the executable
[`SQL-MQL-010`](QUERY_SQL_EQUIVALENTS.md#sql-mql-010-query-context-values)
recipe.

### MetricsQL histogram quantiles

The explicit MetricsQL routes support VictoriaMetrics' plural classic-
histogram form, whose destination label comes first and whose bucket expression
comes last:

```text
histogram_quantiles("phi", 0.25, 0.75, http_request_duration_seconds_bucket)
histogram_quantiles("phi", (time() - start()) / 20, histogram_bucket)
histogram_quantiles("rank", 0.5, native_vmrange_histogram)
```

Every rank is a scalar expression evaluated across the request grid. The
output label uses the first rank value with VictoriaMetrics formatting (for
example `1e+06`, `1e-05`, `NaN`, and `+Inf`), while the rank itself may vary at
later steps. The destination replaces an existing label; `"__name__"` sets a
new metric name and `""` creates an empty-name label. The source bucket-family
name is otherwise always removed, even with `keep_metric_names`.

Classic cumulative `le` buckets and VictoriaMetrics non-cumulative
`vmrange="lower...upper"` buckets are accepted. `vmrange` groups are converted
to cumulative bounds in bounded Rust memory before quantile evaluation. Equal
numeric bounds are summed, the first NaN count becomes zero, later NaNs and
decreases are clamped to the preceding count, and the last bucket supplies the
total even when `+Inf` is absent. Interpolation starts from zero even when the
first bound is negative. A rank below zero returns `-Inf`, one above one
returns `+Inf`, and a rank landing in the infinite bucket returns the last
finite bound. Zero totals, NaN ranks, and other computed NaN samples are
omitted, matching the pinned VictoriaMetrics response behavior.

The bucket expression executes once regardless of the number of ranks.
Duplicate rank labels or destination replacement that collapses two bucket
groups fail as duplicate output timeseries. Malformed bounds are ignored,
invalid argument types fail explicitly, and all existing work, result,
response, deadline, and cancellation limits apply. The stable PromQL routes
continue to reject this MetricsQL argument order; Prometheus' experimental
vector-first function remains a separate `PQL-H03` disposition.

Timeless preserves ordinary stored NaN bucket bits that VictoriaMetrics Remote
Write drops. The query therefore repairs those stored counts according to the
pinned VictoriaMetrics source algorithm, an intentional stronger-fidelity
storage boundary. No histogram state, MetricsQL syntax, or private-table access
was added to the extension. Direct SQLite/libSQL users can apply the executable
[`SQL-MQL-012`](QUERY_SQL_EQUIVALENTS.md#sql-mql-012-histogram_quantiles)
multi-rank recipe over public cumulative buckets.

## Prometheus warning and info annotations

Successful PromQL responses add top-level `warnings` and/or `infos` only when
Prometheus would emit them. The shipped float-series cases are invalid
quantile ranks, `sort`/`sort_desc` on a range query, `rate`/`increase` applied
to a metric name without a conventional counter suffix, missing or malformed
classic-histogram `le` labels, and material histogram monotonicity repair.
Text and one-indexed line/column positions match the pinned query source,
including nested calls and subqueries. GET and form-encoded POST requests,
instant and range envelopes, and shutdown/reopen use the same contract.

Annotations are deterministically deduplicated by their Prometheus message.
Histogram repair observations merge their timestamp, bucket, maximum-delta,
and sample-count span. Each severity emits at most ten messages plus one
omission summary, and annotation bytes count against the same response-size
limit as `data`. An unaffected query or a counter-like function that produces
no sample omits both fields rather than returning empty arrays. These are API
language diagnostics; no annotation syntax or state is added to SQLite.

Prefer the native kernel only when its explicitly mechanical semantics are the
desired contract; it decompresses once in the engine and ships grid points
rather than raw samples over sqld/HTTP.

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
