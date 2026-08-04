# SQL equivalents for query-language features

This cookbook shows how a direct SQLite/libSQL user can execute the storage
and composition work behind PromQL and LogsQL query vectors. It accompanies
the [PromQL](PROMQL_FEATURE_MATRIX.md) and
[LogsQL](LOGSQL_FEATURE_MATRIX.md) matrices.

An `SQL` foundation in a matrix is not complete documentation until this file
contains an executable, parameterized statement for it. If no honest SQL
equivalent exists, the row must target `API`, `EXT`, `LIB`, or `DEFER` instead
of hand-waving at SQL.

## Contract for every recipe

Each recipe must:

1. use only public virtual tables, TVFs, scalar functions, and ordinary
   SQLite/libSQL features—never shadow tables;
2. include required table setup and named parameter units;
3. state whether it is semantically exact, an execution foundation whose API
   still owes language/result shaping, or an intentionally different SQL
   operation;
4. state ordering, bounds, missing-value, and type behavior;
5. name its matrix row IDs and its executable regression in `tests/cli.sh` or
   an extension-backed server contract; and
6. show `EXPLAIN QUERY PLAN` or measured counters when a claim depends on
   pushdown rather than ordinary row filtering.

The Rust API still owns Prometheus/Victoria HTTP envelopes, language errors,
lookback/staleness policy, output label/name rules, pipeline semantics,
resource limits, and cancellation. SQL recipes let embedded users reach the
same stored data and mechanical reductions without running that API.

PromQL scalar literals are intentionally not labeled `SQL`. A finite value can
of course be bound with `SELECT CAST(:value AS REAL)`, but SQLite commonly
normalizes IEEE NaN to SQL NULL and does not define Prometheus's `"NaN"`,
`"+Inf"`, and `"-Inf"` response strings or evaluation timestamps. Claiming
that statement as an exact `PQL-S11` recipe would be misleading; those
language/value-envelope semantics belong to the Rust API.

## Recipe index

| recipe | matrix rows | state | semantic class |
|---|---|---|---|
| [`SQL-PROM-001`](#sql-prom-001-instant-selector) | `PQL-S01`, `PQL-S02` | current | exact storage selection; API shapes PromQL output |
| [`SQL-PROM-002`](#sql-prom-002-avg_over_time) | `PQL-S06`, `PQL-R01` | current | exact float-window reduction |
| [`SQL-PROM-003`](#sql-prom-003-cross-series-sum-by-label) | `PQL-O09` | current foundation | exact bounded cross-series sum; API owns grouping syntax, labels, IEEE strings, limits, and envelopes |
| [`SQL-PROM-004`](#sql-prom-004-vector-arithmetic-with-label-matching) | `PQL-O02`, `PQL-O05` | current foundation | vector/scalar arithmetic and exact-label joins; API owns language, cardinality, labels, IEEE strings, and envelopes |
| [`SQL-PROM-005`](#sql-prom-005-top-k-per-evaluation-step) | `PQL-O14` | current foundation | per-step top/bottom ranking; API owns language, grouping modifiers, original labels, parameter errors, limits, and envelopes |
| [`SQL-PROM-006`](#sql-prom-006-range-selector) | `PQL-S06` | current | exact root range-vector storage selection; API shapes the matrix |
| [`SQL-PROM-007`](#sql-prom-007-bounded-packed-storage-work) | `PQL-S20` | current foundation | exact pre-decode work bounds; API owns language/result/deadline limits |
| [`SQL-PROM-008`](#sql-prom-008-temporal-selector-modifiers) | `PQL-S07`, `PQL-S08` | current foundation | exact shifted/fixed lookup time; API owns parser and outer query context |
| [`SQL-PROM-009`](#sql-prom-009-aligned-selector-subquery) | `PQL-S09` | current foundation | exact open-left global subquery grid for a stored selector; API owns arbitrary inner expressions and range consumption |
| [`SQL-PROM-010`](#sql-prom-010-unary-minus) | `PQL-O01` | current foundation | exact numeric negation over a bounded public grid; API owns types, envelopes, metric-name policy, limits, and cancellation |
| [`SQL-PROM-011`](#sql-prom-011-comparison-filter-and-bool) | `PQL-O03` | current foundation | exact SQLite predicate/CASE over public grids; API owns AST types, name policy, matching, limits, and envelopes |
| [`SQL-PROM-012`](#sql-prom-012-set-membership) | `PQL-O04` | current foundation | exact step-local many-to-many membership over public grids; API owns language, names, bounds, limits, and envelopes |
| [`SQL-PROM-013`](#sql-prom-013-on-and-ignoring-label-matching) | `PQL-O06` | current foundation | explicit JSON-label projection/equality over public grids; API owns AST/cardinality/name/error semantics |
| [`SQL-PROM-014`](#sql-prom-014-group_left-and-group_right) | `PQL-O07` | current foundation | explicit many/one grid join and label copy; API owns uniqueness failures, name/value direction, limits, and envelopes |
| [`SQL-PROM-015`](#sql-prom-015-cross-series-average-by-label) | `PQL-O10` | current foundation | bounded cross-series average; API owns compensated arithmetic, grouping syntax, labels, limits, and envelopes |
| [`SQL-PROM-016`](#sql-prom-016-cross-series-minimum-and-maximum) | `PQL-O11` | current foundation | bounded cross-series extrema; API owns all-NaN behavior, grouping syntax, labels, limits, and envelopes |
| [`SQL-PROM-017`](#sql-prom-017-cross-series-count-and-group) | `PQL-O12` | current foundation | bounded cross-series row count/presence; API owns grouping syntax, labels, limits, and envelopes |
| [`SQL-PROM-018`](#sql-prom-018-cross-series-population-variance-and-standard-deviation) | `PQL-O13` | current foundation | bounded population second moment; API owns Welford/IEEE arithmetic, language, labels, limits, and envelopes |
| [`SQL-PROM-019`](#sql-prom-019-cross-series-quantile) | `PQL-O15` | current foundation | bounded finite-value linear interpolation; API owns language, raw-NaN rank, parameters, labels, limits, and envelopes |
| [`SQL-PROM-020`](#sql-prom-020-count-series-by-sample-value) | `PQL-O16` | current foundation | exact bounded grouping by raw SQL numeric value; API owns Prometheus label formatting, grouping syntax, raw NaN, limits, and envelopes |
| [`SQL-PROM-021`](#sql-prom-021-min_over_time) | `PQL-R02` | current | exact float-window minimum |
| [`SQL-PROM-022`](#sql-prom-022-max_over_time) | `PQL-R03` | current | exact float-window maximum |
| [`SQL-PROM-023`](#sql-prom-023-sum_over_time) | `PQL-R04` | current | compensated float-window sum |
| [`SQL-PROM-024`](#sql-prom-024-count_over_time) | `PQL-R05` | current | exact float-sample window count |
| [`SQL-PROM-025`](#sql-prom-025-last_over_time) | `PQL-R06` | current | exact last stored float in each window |
| [`SQL-LOG-001`](#sql-log-001-bounded-filter-sort-and-pagination) | `LQL-F01`, `LQL-F02`, `LQL-F06`, `LQL-F07`, `LQL-P01`, `LQL-P02`, `LQL-P03` | current foundation | exact row query for declared index keys |
| [`SQL-LOG-002`](#sql-log-002-message-substring) | `LQL-F08`, `LQL-F12` | current foundation | exact Timeless case-insensitive substring, not LogsQL word semantics |
| [`SQL-LOG-003`](#sql-log-003-exact-count) | `LQL-P09`, `LQL-S01` | current | exact scalar count without row materialization |
| [`SQL-LOG-004`](#sql-log-004-distinct-field-values) | `LQL-P04`, `LQL-S03`, `LQL-S04` | current foundation | bounded lexical values; aggregate syntax remains API work |
| [`SQL-LOG-005`](#sql-log-005-arbitrary-metadata-equality) | `LQL-F05` | reference | correct decoded fallback for a non-indexed field |
| [`SQL-LOG-006`](#sql-log-006-counts-by-field-and-time-bucket) | `LQL-P09`, `LQL-S05`, `LQL-S08` | current foundation | storage bucket vector or ordinary SQL grouping |

`current` means the public SQL surface exists now. `reference` means the SQL
is executable now but the corresponding PromQL/LogsQL parser/evaluator row is
still correctly marked `missing`.

## Setup and parameter conventions

Examples assume the extension has been loaded and these tables exist:

```sql
CREATE VIRTUAL TABLE metrics USING timeless_metrics;
CREATE VIRTUAL TABLE logs USING timeless_logs(
  index_keys='service,host,path,status'
);
```

Metric timestamps, steps, windows, and lookback values are seconds. The
general-purpose `timeless_logs` table uses epoch milliseconds. Release signal
servers may create a table with a different declared timestamp capability;
direct users must bind times in the table's reported native unit.

Named parameters use SQLite notation (`:metric`, `:start`, and so on). Bind
numbers as integers/reals rather than interpolating query text.

## PromQL foundations and equivalents

### SQL-PROM-001: instant selector

At evaluation timestamp `:at`, return the newest sample in
`(:at - :lookback, :at]` for each matching series:

```sql
SELECT labels, ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :at, :at,
  1, :lookback
)
ORDER BY labels;
```

`:filter_json` uses a plain string for equality and an operator object for
the other matcher forms:

```json
{
  "host": {"re": "web-.*"},
  "env": {"neq": "dev"},
  "zone": {"nre": "test-.+"},
  "service": "api"
}
```

Regexes are fully anchored and an absent label is compared as the empty
string. The Rust API remains responsible for parsing PromQL, expanding
multi-metric selectors, preserving duplicate matcher AND semantics, and
formatting the vector response. Direct regression: `tests/cli.sh` sections 22
and 35.

For a nameless selector, first enumerate distinct `name` values from
`timeless_series('metrics')`, applying its optional matcher-aware arguments
where possible, and execute this statement once per selected name. That
catalog/read loop is the honest public SQL composition: SQLite cannot bind a
table-valued function's hidden metric input from a correlated row on every
supported extension host. The Rust API keeps the loop bounded and does not
read unrelated metric payloads. Regex and negative `__name__` matchers are
applied to the catalog's `name` column before executing the per-name statement;
duplicate name predicates use ordinary SQL `AND` composition.

### SQL-PROM-002: `avg_over_time`

Evaluate `avg_over_time(metric{...}[:window])` on the exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'avg'
)
ORDER BY labels, ts;
```

The window is `(T-window,T]`, matching PromQL range boundaries. Set
`:start = :end` for an instant evaluation. This recipe is exact for stored
float samples: the public window kernel uses compensated summation and switches
to an incremental mean before a finite average would overflow. It preserves
Prometheus `NaN`, positive/negative infinity, and signed-zero float behavior;
the API still owns language parsing, metric-name removal, timestamp units,
limits, and result envelopes. Native histogram samples are not stored. Direct
regression: `tests/cli.sh` sections 22, 33, 35, and 45.

### SQL-PROM-021: `min_over_time`

Evaluate `min_over_time(metric{...}[:window])` on an exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'min'
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. Each reduction uses the exact
open-left, closed-right interval `(T-window,T]`; empty windows emit no row.
Samples are visited in timestamp order. An incoming NaN does not replace a
numeric minimum, a leading NaN is replaced by the first ordered value, an
all-NaN window remains NaN in the extension's packed IEEE bits, and equal
signed zeros retain the first sample. Some SQLite hosts or language bindings
normalize a NaN REAL to SQL NULL when projecting or binding it; use
`timeless_window_batches` when exact non-finite bits must cross that boundary.
The Rust API owns PromQL parsing, metric-name removal, outer evaluation
timestamps, subquery composition, limits, cancellation, IEEE response strings,
and result envelopes. Native histogram samples are not stored. Direct
regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_six_promql_min_over_time_boundaries_ieee_limits_and_reopen`.

### SQL-PROM-022: `max_over_time`

Evaluate `max_over_time(metric{...}[:window])` on an exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'max'
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. Each reduction uses
`(T-window,T]`; empty windows emit no row. Samples are visited in timestamp
order. An incoming NaN does not replace a numeric maximum, a leading NaN is
replaced by the first ordered value, an all-NaN window remains NaN in packed
IEEE bits, and equal signed zeros retain the first sample. SQLite hosts or
bindings may normalize a NaN REAL to SQL NULL; use
`timeless_window_batches` to preserve exact non-finite bits across that
boundary. The Rust API owns PromQL parsing, metric-name removal, outer
timestamps, subquery composition, limits, cancellation, IEEE response strings,
and result envelopes. Native histograms are not stored. Direct regression:
`tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_six_promql_max_over_time_boundaries_ieee_limits_and_reopen`.

### SQL-PROM-023: `sum_over_time`

Evaluate `sum_over_time(metric{...}[:window])` on an exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'sum'
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. Each reduction consumes stored
float samples in `(T-window,T]`; empty windows emit no row. The public kernel
uses Prometheus-compatible compensated addition, so cancellation-prone finite
inputs retain their low-order result. A finite overflow remains infinity,
mixed infinities become NaN, and NaN propagates. SQLite hosts or bindings may
normalize a NaN REAL to SQL NULL; `timeless_window_batches` preserves the
exact IEEE bits. The Rust API owns PromQL parsing, metric-name removal, outer
timestamps, subquery composition, limits, cancellation, IEEE response strings,
and result envelopes. Native histograms are not stored. Direct regression:
`tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_six_promql_sum_over_time_is_compensated_ieee_bounded_and_reopenable`.

### SQL-PROM-024: `count_over_time`

Evaluate `count_over_time(metric{...}[:window])` on an exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'count'
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. Each result counts every stored
float sample in `(T-window,T]`, including NaN, either infinity, and either
signed zero. Empty windows emit no row rather than zero. The row TVF returns
the count as a SQLite REAL because every window operation shares one numeric
schema; counts remain exact while bounded by the public work limit. The Rust
API owns PromQL parsing, metric-name removal, outer timestamps, subqueries,
limits, cancellation, string formatting, and result envelopes. Native
histograms are not stored. Direct regression: `tests/cli.sh` sections 22 and
45; HTTP/oracle/reopen regression:
`session_six_promql_count_over_time_includes_ieee_limits_and_reopen`.

### SQL-PROM-025: `last_over_time`

Evaluate `last_over_time(metric{...}[:window])` on an exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. `timeless_grid` selects the last
stored float in `(T-window,T]`; empty windows emit no row. Values—including
NaN, infinities, and signed zero—are returned unchanged. Duplicate timestamps
follow the extension's stable engine order; direct users that admit duplicates
must treat that order as part of their ingest contract because Prometheus
storage normally has one sample per series/timestamp. The Rust API owns
PromQL parsing, multi-metric expansion, outer timestamps, subquery composition,
limits, cancellation, IEEE response strings, and envelopes. Unlike most
PromQL range functions, pinned Prometheus `last_over_time` preserves the input
metric name; direct SQL already returns `:metric` separately from canonical
labels. Native histograms are not stored. Direct regression: `tests/cli.sh`
section 45; HTTP/oracle/reopen regression:
`session_six_promql_last_over_time_preserves_name_ieee_limits_and_reopen`.

### SQL-PROM-006: range selector

At instant evaluation timestamp `:at`, return every stored float sample in
the PromQL range-vector interval `(:at - :window, :at]`:

```sql
SELECT labels, ts, value
FROM timeless_raw(
  'metrics', :metric, :filter_json,
  :at - :window, :at
)
WHERE ts > :at - :window
  AND ts <= :at
ORDER BY labels, ts;
```

`:at` and `:window` are integer seconds for a default `timeless_metrics`
table. The explicit predicates turn the public raw surface's inclusive read
bounds into PromQL's open-left, closed-right interval. Values remain SQLite
REALs, including IEEE NaN/Inf where the SQLite build preserves them; labels
are canonical JSON. The Rust API owns parsing, matrix envelopes, value-string
formatting, limits, and rejection of a root range vector on a range query.
Direct regression: `tests/cli.sh` section 33 and the metrics API
`session_four_pins_promql_selector_window_errors_and_reopen` contract.

### SQL-PROM-007: bounded packed storage work

Direct SQLite/libSQL callers can cap the conservative number of stored or
buffered points inspected by packed raw and window reads:

```sql
SELECT frame
FROM timeless_raw_frame(
  'metrics', :metric, :filter_json,
  :start, :end, :max_work_points
);

SELECT series_id, labels, buckets
FROM timeless_window_batches(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window, :aggregate,
  NULL, :max_work_points
)
ORDER BY labels, series_id;
```

All metric timestamps are integer seconds. Bounds are inclusive at the raw
storage surface; window samples use `(T-window,T]`. `:max_work_points` is a
positive SQLite INTEGER and is inclusive. The extension checks persisted
chunk point counts plus current buffer lengths before reading chunk payloads.
For a packed window it independently checks the conservative input count and
`matched series × grid points`, so neither decode nor packed output can exceed
the bound. An excess returns an error and no partial frame/rows. `NULL`, zero,
negative, and non-integer limits fail; omitting the trailing argument retains
the unbounded backward-compatible SQL call.

This is the storage foundation, not an SQL reimplementation of PromQL resource
semantics. The Rust metrics API additionally owns the fixed 11,000-point grid
ceiling, final result cardinality, serialized response bytes, cancellation,
deadline/error envelopes, and tighter auth-claim policy. Direct regressions:
`tests/cli.sh` sections 22, 34, and 45; core buffered/persisted coverage:
`write_flush_query_recover`; HTTP/reopen coverage:
`session_two_promql_limits_bound_grid_work_results_response_and_deadline`.

### SQL-PROM-008: temporal selector modifiers

For a selector with signed `offset`, shift the lookup grid by `:offset` and
restore the outer evaluation timestamp in the projection. Positive offsets
look into the past; negative offsets look into the future:

```sql
SELECT labels, ts + :offset AS ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start - :offset, :end - :offset, :step, :lookback
)
ORDER BY labels, ts;
```

For `@` forms, first resolve `:anchor` to the numeric timestamp, query once at
`:anchor - :offset`, and cross that value with the outer evaluation grid:

```sql
WITH RECURSIVE evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT labels, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :anchor - :offset, :anchor - :offset, 1, :lookback
  )
)
SELECT selected.labels, evaluation.ts, selected.value
FROM selected CROSS JOIN evaluation
ORDER BY selected.labels, evaluation.ts;
```

All parameters use the metric table's native timestamp unit (seconds for the
default table). `:offset` is signed; bind zero when absent. For numeric `@`,
`:anchor` is that timestamp. For `@ start()` and `@ end()`, bind `:start` and
`:end`, respectively. The order is always anchor first, offset second. Output
timestamps are the outer grid, while lookback and the open-left boundary are
relative to lookup time. Missing series remain sparse. The API owns parsing,
millisecond conversion, overflow errors, range-vector raw timestamps,
function label policy, limits, cancellation, and Prometheus envelopes. Direct
regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_three_promql_temporal_modifiers_preserve_selection_and_output_time`.

### SQL-PROM-009: aligned selector subquery

For a selector subquery `metric{...}[:window::resolution]` at `:at`, derive
the globally aligned evaluation grid and run the ordinary public selector
surface on it:

```sql
WITH timing AS (
  SELECT
    :at - :offset AS effective_at,
    :window AS window,
    :resolution AS resolution
), bounds AS (
  SELECT
    effective_at - window AS lower,
    effective_at,
    resolution,
    ((effective_at - window) % resolution + resolution) % resolution AS lower_mod,
    (effective_at % resolution + resolution) % resolution AS upper_mod
  FROM timing
), aligned AS (
  SELECT
    lower - lower_mod + resolution AS first_ts,
    effective_at - upper_mod AS last_ts,
    resolution
  FROM bounds
)
SELECT labels, ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  (SELECT first_ts FROM aligned),
  (SELECT last_ts FROM aligned),
  (SELECT resolution FROM aligned),
  :lookback
)
WHERE (SELECT first_ts <= last_ts FROM aligned)
ORDER BY labels, ts;
```

All parameters use the metric table's native unit: integer seconds for the
default table. `:window`, `:resolution`, and `:lookback` must be positive;
`:offset` is signed and defaults to zero. The normalized modulo expressions
preserve Euclidean/global alignment for pre-epoch timestamps. Adding one
resolution after the aligned floor makes the range open on the left, so this
returns evaluations strictly inside
`(:at - :offset - :window, :at - :offset]`. `timeless_grid` supplies the
newest stored float in each evaluation point's own open-left lookback window;
missing evaluations remain sparse. Labels are canonical JSON and ordering is
deterministic by labels then timestamp.

This is exact storage work for a selector subquery. The Rust API still owns
PromQL parsing, a configurable default resolution when the colon has no
duration, `@ start()`/`@ end()` outer context, arbitrary/nested instant-vector
inner expressions, matrix envelopes, function consumption, label/name policy,
millisecond conversion, intermediate/result limits, cancellation, and
deadline errors. Direct executable regression: `tests/cli.sh` section 45;
HTTP/oracle/reopen regression:
`session_three_promql_subqueries_align_bound_cancel_and_reopen`.

### SQL-PROM-010: unary minus

For the vector expression `-metric{...}`, negate values from the ordinary
bounded selector grid:

```sql
SELECT labels, ts, -value AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

All timestamp parameters use the virtual table's native unit (integer seconds
for the default metric table). `:start` and `:end` are inclusive output-grid
bounds, `:step` and `:lookback` are positive, and `:filter_json` is either
`NULL` or the documented matcher object. `timeless_grid` returns canonical
label JSON without a metric-name field, sparse rows when no sample exists in
the open-left `(T-lookback,T]` window, and deterministic label/timestamp
ordering. SQLite's unary numeric operator preserves ordinary finite values and
infinities supported by the host build; portable SQL must not claim
Prometheus's exact `NaN` string behavior.

This statement is the exact stored-vector arithmetic foundation. The Rust API
owns PromQL parsing, scalar versus vector types, removal of `__name__`,
millisecond evaluation, `NaN`/`+Inf`/`-Inf` response strings, nested AST
composition, intermediate/result/response limits, cancellation, and the
Prometheus envelope. Direct executable regression: `tests/cli.sh` section 33;
HTTP/oracle/reopen regression:
`session_four_promql_unary_minus_preserves_types_labels_limits_and_reopen`.

### SQL-PROM-011: comparison filter and `bool`

For a vector/scalar filter such as `metric > :threshold`, ordinary SQL returns
the original sample value only when the predicate is true:

```sql
SELECT labels, ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
WHERE value > CAST(:threshold AS REAL)
ORDER BY labels, ts;
```

The `bool` form retains every selected grid sample and maps the predicate to a
floating `0` or `1`:

```sql
SELECT labels, ts,
       CAST(value > CAST(:threshold AS REAL) AS REAL) AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

Substitute `=`, `!=`, `>`, `<`, `>=`, or `<=` as required (`==` in PromQL is
SQLite `=`). Timestamp units, inclusive grid bounds, open-left lookback,
sparse missing samples, canonical labels, and deterministic ordering are the
same as `SQL-PROM-004`. A vector/vector comparison uses the exact-label public
grid join from that recipe and moves the predicate into `WHERE` or `CASE`.

The Rust API additionally enforces scalar/scalar `bool`, performs
per-timestamp PromQL vector matching/cardinality checks, preserves the
vector's original value and metric name for a filter, removes the metric name
for `bool`, renders IEEE results, bounds cumulative work, checks cancellation,
and writes Prometheus envelopes. Direct regression: `tests/cli.sh` section 33;
HTTP/oracle/reopen regression:
`session_four_promql_comparisons_filter_bool_bound_and_reopen`.

### SQL-PROM-012: set membership

PromQL set operators compare label signatures independently at every
evaluation timestamp and ignore sample values while deciding membership. For
two exact metric selectors, public grids make that operation ordinary SQL:

```sql
WITH
lhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
),
rhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT labels, ts, value
FROM lhs
WHERE EXISTS (
  SELECT 1 FROM rhs
  WHERE rhs.labels = lhs.labels AND rhs.ts = lhs.ts
)
ORDER BY labels, ts;
```

That is `lhs and rhs`. Change `EXISTS` to `NOT EXISTS` for `unless`. The
left-preferred union for `or` is:

```sql
WITH
lhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
),
rhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT labels, ts, value FROM lhs
UNION ALL
SELECT rhs.labels, rhs.ts, rhs.value
FROM rhs
WHERE NOT EXISTS (
  SELECT 1 FROM lhs
  WHERE lhs.labels = rhs.labels AND lhs.ts = rhs.ts
)
ORDER BY labels, ts;
```

All time parameters use the metric table's native unit. Grid bounds are
inclusive, each lookup window is open on the left, absent samples do not
participate, values may be any SQLite REAL, and canonical label JSON makes
equality exact and ordering deterministic. Set membership is many-to-many:
every left series in a matching group survives `and`, every such series is
removed by `unless`, and `or` keeps all left series while suppressing matching
right series. For a side containing multiple metric names, `UNION ALL` the
bounded public grids and carry a literal `name` column so the chosen side's
name can be projected.

The Rust API owns PromQL parsing, nameless/multi-name planning, per-step
membership, source metric-name preservation, millisecond timestamps,
intermediate/result/response limits, cancellation, and Prometheus envelopes.
Direct executable regression: `tests/cli.sh` section 33; HTTP/oracle/reopen
regression:
`session_four_promql_set_operators_are_many_to_many_stepwise_and_reopen`.

### SQL-PROM-013: `on` and `ignoring` label matching

For `lhs + on(host) rhs`, treat an absent matching label as the empty string,
join by that value and timestamp, and project only the matching label:

```sql
WITH
lhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
),
rhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object(
    'host', COALESCE(json_extract(lhs.labels, '$.host'), '')
  ) AS labels,
  lhs.ts,
  lhs.value + rhs.value AS value
FROM lhs
JOIN rhs
  ON rhs.ts = lhs.ts
 AND COALESCE(json_extract(rhs.labels, '$.host'), '') =
     COALESCE(json_extract(lhs.labels, '$.host'), '')
ORDER BY labels, lhs.ts;
```

For `lhs + ignoring(zone) rhs`, remove the ignored key before comparing and
projecting the left labels:

```sql
WITH
lhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
),
rhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_remove(lhs.labels, '$.zone') AS labels,
  lhs.ts,
  lhs.value + rhs.value AS value
FROM lhs
JOIN rhs
  ON rhs.ts = lhs.ts
 AND json_remove(rhs.labels, '$.zone') =
     json_remove(lhs.labels, '$.zone')
ORDER BY labels, lhs.ts;
```

Pass more JSON paths to `json_remove` for a multi-label `ignoring` list. For
multi-label `on`, compare each named `COALESCE(json_extract(...), '')` term
and construct the projected canonical label object in lexical key order.
`on()` has no label terms, so every series at a timestamp belongs to one match
group; one-to-one language operations must reject a group with duplicates.

All time parameters use the table's native unit; grids have inclusive output
bounds and open-left lookback windows. Missing samples do not participate.
For arithmetic and `bool`, the metric name is absent; a comparison filter with
`on` also projects only the named labels, while `ignoring` removes only the
listed labels. Set operators use the modified comparison signature but retain
the full contributing labelset. The Rust API enforces those per-operator name
rules, one-to-one cardinality errors, AST validation, bounded work,
cancellation, and Prometheus envelopes. Direct executable regression:
`tests/cli.sh` section 33; HTTP/oracle/reopen regression:
`session_four_promql_on_ignoring_match_labels_names_limits_and_reopen`.

### SQL-PROM-014: `group_left` and `group_right`

For `many + on(service) group_left(team) one`, join a many-side grid to a
one-side grid by the explicit key and copy `team` from the unique right side:

```sql
WITH
many AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :many_metric, :many_filter,
    :start, :end, :step, :lookback
  )
),
one AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :one_metric, :one_filter,
    :start, :end, :step, :lookback
  )
)
SELECT
  CASE
    WHEN COALESCE(json_extract(one.labels, '$.team'), '') = ''
      THEN json_remove(many.labels, '$.team')
    ELSE json_set(
      many.labels, '$.team', json_extract(one.labels, '$.team')
    )
  END AS labels,
  many.ts,
  many.value + one.value AS value
FROM many
JOIN one
  ON one.ts = many.ts
 AND COALESCE(json_extract(one.labels, '$.service'), '') =
     COALESCE(json_extract(many.labels, '$.service'), '')
ORDER BY labels, many.ts;
```

Before executing, the direct caller must prove that the one side is unique at
each match key and timestamp:

```sql
WITH one AS (
  SELECT labels, ts
  FROM timeless_grid(
    'metrics', :one_metric, :one_filter,
    :start, :end, :step, :lookback
  )
)
SELECT
  COALESCE(json_extract(labels, '$.service'), '') AS match_service,
  ts,
  COUNT(*) AS matches
FROM one
GROUP BY match_service, ts
HAVING COUNT(*) > 1;
```

Any returned row is a cardinality error, not permission to execute a Cartesian
join. `group_right` swaps which side supplies the base labelset and which side
must be unique, but never swaps the language operation. For example,
`one - on(service) group_right(team) many` projects from `many`, copies `team`
from `one`, and still calculates `one.value - many.value`.

Time units, inclusive grid bounds, open-left lookback, missing sample behavior,
and canonical label ordering follow `SQL-PROM-013`. A missing or empty included
label removes that label from the result. Included labels that collapse two
many-side results to the same labelset at one timestamp are also an execution
error. Arithmetic removes the metric name. A `group_right` comparison filter
uses the right metric identity but retains the original left value; the Rust
API owns that directionality, per-step uniqueness errors, result splitting,
limits, cancellation, and envelopes. Direct executable regression:
`tests/cli.sh` section 33; HTTP/oracle/reopen regression:
`session_four_promql_group_matching_direction_labels_limits_and_reopen`.

### SQL-PROM-003: cross-series sum by label

Equivalent mechanical reduction for `sum by (service) (metric)`:

```sql
WITH selected AS (
  SELECT
    ts,
    json_extract(labels, '$.service') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object('service', service) AS labels,
  ts,
  SUM(value) AS value
FROM selected
GROUP BY service, ts
ORDER BY service, ts;
```

`:metric` is text; `:filter_json` is a JSON matcher object or SQL `NULL`; and
`:start`, `:end`, `:step`, and `:lookback` use the metrics table's epoch-second
unit. Grid and lookback bounds retain the public `timeless_grid` contract.
SQLite groups a missing `service` and JSON null together here, so callers that
need PromQL's absent-as-empty grouping should normalize with
`COALESCE(json_extract(labels, '$.service'), '')`. Output is ordered by group
and timestamp; values remain SQLite numeric values.

This is the exact bounded numeric reduction foundation. The Rust API adds
`by`/`without` parsing, empty/missing and `__name__` output-label policy,
portable IEEE strings, millisecond evaluation timestamps, resource limits,
cancellation, and Prometheus result/error envelopes. The statement executes
in `tests/cli.sh` section 33; the real-extension/API contract is
`session_five_promql_sum_groups_labels_limits_and_reopen`.

### SQL-PROM-015: cross-series average by label

Equivalent ordinary-SQL reduction for `avg by (service) (metric)`:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object('service', service) AS labels,
  ts,
  AVG(value) AS value
FROM selected
GROUP BY service, ts
ORDER BY service, ts;
```

`:metric` is text, `:filter_json` is a matcher object or SQL `NULL`, and all
four temporal parameters use epoch seconds. The public grid has inclusive
output bounds and open-left lookback. Missing `service` is normalized to the
empty grouping value; rows are ordered by group and timestamp. SQLite returns
numeric `AVG` values using its own accumulation order.

The Rust API uses Prometheus 3.13.2's compensated direct mean and switches to
an incremental compensated mean when the running sum overflows. It also owns
`by`/`without`, output labels, millisecond timestamps, IEEE strings, limits,
cancellation, and envelopes. Therefore ordinary `AVG` is the copyable SQL
foundation but is not advertised as bit-identical for adversarial
cancellation/overflow inputs. This parameterized recipe executes in
`tests/cli.sh` section 45; the exact API contract is
`session_five_promql_avg_is_compensated_grouped_and_reopenable`.

### SQL-PROM-016: cross-series minimum and maximum

Use ordinary SQLite extrema over the public bounded grid:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object('service', service) AS labels,
  ts,
  MIN(value) AS min_value,
  MAX(value) AS max_value
FROM selected
GROUP BY service, ts
ORDER BY service, ts;
```

Parameter types, timestamp units, bounds, missing-label normalization, and
ordering are the same as `SQL-PROM-015`. SQLite ignores SQL NULL in `MIN` and
`MAX`; an IEEE NaN projected through an ordinary SQLite REAL is NULL. The Rust
API reads raw value bits from `TRF1`, ignores NaN when a numeric/infinite value
exists, and returns `NaN` for an all-NaN group exactly like Prometheus. It also
owns language, labels, limits, cancellation, and envelopes. The statement is
executed in `tests/cli.sh` section 45 and the exact API contract is
`session_five_promql_min_max_group_ieee_range_and_reopen`.

### SQL-PROM-017: cross-series count and group

Count every selected series at each evaluation timestamp and derive PromQL's
`group` presence value from the same public bounded grid:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object('service', service) AS labels,
  ts,
  COUNT(*) AS count_value,
  1 AS group_value
FROM selected
GROUP BY service, ts
ORDER BY service, ts;
```

`:metric` and `:filter_json` are text and a JSON matcher object (or SQL
`NULL`); `:start`, `:end`, `:step`, and `:lookback` are epoch seconds. The
public grid's output bounds are inclusive and its lookback is open-left.
Missing `service` is normalized to the empty grouping value. `COUNT(*)`
counts every selected sample, including values whose raw IEEE representation
is NaN or infinity; unlike `COUNT(value)`, it does not discard a NaN exposed
to SQLite as NULL. A non-empty group always yields the integer presence value
one, and an empty input yields no row.

The Rust API supplies `by`/`without`, `__name__` policy, Prometheus float
strings, millisecond timestamps, limits, cancellation, and response/error
envelopes. This parameterized statement executes in `tests/cli.sh` section
45; the exact API contract is
`session_five_promql_count_group_include_all_values_and_reopen`.

### SQL-PROM-018: cross-series population variance and standard deviation

This ordinary-SQL second-moment recipe is the copyable public-grid equivalent
for finite, well-scaled inputs:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
), moments AS (
  SELECT
    service,
    ts,
    AVG(value) AS mean,
    AVG(value * value) AS second_moment
  FROM selected
  GROUP BY service, ts
)
SELECT
  json_object('service', service) AS labels,
  ts,
  MAX(second_moment - mean * mean, 0.0) AS stdvar_value,
  SQRT(MAX(second_moment - mean * mean, 0.0)) AS stddev_value
FROM moments
ORDER BY service, ts;
```

Parameters, epoch-second units, inclusive grid bounds, open-left lookback,
missing-label normalization, and ordering match `SQL-PROM-017`. This computes
population variance (division by `N`), so a singleton is zero and empty input
has no row. `MAX(..., 0.0)` removes a small negative caused by ordinary
floating-point roundoff.

This formula can lose precision through cancellation or overflow while
squaring. SQLite also exposes stored NaN as SQL NULL. The Rust API therefore
uses Prometheus 3.13.2's one-pass Welford update over the raw packed float bits
and returns NaN when any group member is NaN or infinite. It also owns
`by`/`without`, labels, millisecond timestamps, limits, cancellation, and
envelopes. The SQL is an honest efficient foundation for normal finite data,
not a claim of bit-identical adversarial arithmetic. It executes in
`tests/cli.sh` section 45; the exact API contract is
`session_five_promql_stddev_stdvar_are_population_grouped_and_reopenable`.

### SQL-PROM-019: cross-series quantile

For a finite `:q` between zero and one and finite sample values, window
functions provide the same `q * (N - 1)` linear interpolation over each
service and evaluation timestamp:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
  WHERE value IS NOT NULL
), ranked AS (
  SELECT
    service,
    ts,
    value,
    ROW_NUMBER() OVER (
      PARTITION BY service, ts ORDER BY value
    ) - 1 AS value_index,
    COUNT(*) OVER (PARTITION BY service, ts) AS value_count
  FROM selected
), positions AS (
  SELECT DISTINCT
    service,
    ts,
    value_count,
    CAST(:q AS REAL) * (value_count - 1) AS rank
  FROM ranked
), bounds AS (
  SELECT
    *,
    CAST(rank AS INTEGER) AS lower_index,
    MIN(CAST(rank AS INTEGER) + 1, value_count - 1) AS upper_index
  FROM positions
)
SELECT
  json_object('service', bounds.service) AS labels,
  bounds.ts,
  lower.value * (1.0 - (bounds.rank - bounds.lower_index))
    + upper.value * (bounds.rank - bounds.lower_index) AS value
FROM bounds
JOIN ranked AS lower
  ON lower.service = bounds.service
 AND lower.ts = bounds.ts
 AND lower.value_index = bounds.lower_index
JOIN ranked AS upper
  ON upper.service = bounds.service
 AND upper.ts = bounds.ts
 AND upper.value_index = bounds.upper_index
ORDER BY bounds.service, bounds.ts;
```

`:metric`, `:filter_json`, and the epoch-second temporal parameters have the
same types and bounds as `SQL-PROM-018`; `:q` is REAL in `[0,1]`. Missing
`service` is normalized to empty, empty input emits no row, and ordering is
deterministic. A singleton returns its value.

The finite restriction is intentional. SQLite exposes a packed IEEE NaN as
SQL NULL, whereas PromQL sorts raw NaN before numeric samples and lets it
participate in the interpolation rank. PromQL also maps `q < 0` to `-Inf`,
`q > 1` to `+Inf`, and `q = NaN` to NaN. The Rust API operates on packed raw
bits and owns those edge rules, scalar parameter expressions, `by`/`without`,
labels, limits, cancellation, and envelopes. This executable SQL is the
public direct-user foundation for normal finite data, not a false IEEE-parity
claim. It runs in `tests/cli.sh` section 45; the exact API contract is
`session_five_promql_quantile_interpolates_per_step_and_reopens`.

### SQL-PROM-020: count series by sample value

Direct SQLite/libSQL users can group bounded grid rows by their numeric value
without materializing another storage representation:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object('service', service) AS grouping_labels,
  ts,
  value AS sample_value,
  COUNT(*) AS value_count
FROM selected
GROUP BY service, ts, value
ORDER BY service, ts, value;
```

`:metric`, `:filter_json`, and all epoch-second temporal parameters follow
`SQL-PROM-019`; output bounds are inclusive and lookback is open-left.
Missing `service` is normalized to empty. Output is one row per distinct SQL
numeric value per group and timestamp, with an integer count. Empty input
emits no row.

PromQL's `count_values("label", vector)` turns `sample_value` into a label
name/value pair. The Rust API implements Go-compatible fixed shortest float
text—including `-0`, `+Inf`, `-Inf`, NaN, and exponent expansion—using packed
raw float bits; it overwrites an existing label of the same name before
applying `by`/`without`. SQLite exposes raw NaN as NULL, so the SQL statement
is an honest numeric grouping foundation rather than an IEEE-label-formatting
claim. The API also owns UTF-8 label-name validation, per-step range assembly,
cardinality limits, cancellation, and envelopes. This recipe executes in
`tests/cli.sh` section 45; the exact API contract is
`session_five_promql_count_values_formats_groups_ranges_and_reopens`.

### SQL-PROM-004: vector arithmetic with label matching

For vector/scalar arithmetic, apply the ordinary SQLite operator to the public
grid (substitute the required arithmetic operator; `pow(value, :scalar)` is
the `^` form):

```sql
SELECT labels, ts, value * CAST(:scalar AS REAL) AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

For default one-to-one vector matching, canonical `labels` equality is exactly
PromQL's all-labels-except-`__name__` signature because public metric rows
carry the name separately. This parameterized statement shows every numeric
operation over `errors` and `requests`:

```sql
WITH
errors AS (
  SELECT ts, labels, value
  FROM timeless_grid(
    'metrics', 'errors_total', :error_filter,
    :start, :end, :step, :lookback
  )
),
requests AS (
  SELECT ts, labels, value
  FROM timeless_grid(
    'metrics', 'requests_total', :request_filter,
    :start, :end, :step, :lookback
  )
)
SELECT
  e.labels,
  e.ts,
  e.value + r.value AS add_value,
  e.value - r.value AS subtract_value,
  e.value * r.value AS multiply_value,
  e.value / r.value AS divide_value,
  e.value % r.value AS modulo_value,
  pow(e.value, r.value) AS power_value
FROM errors AS e
JOIN requests AS r
  ON r.ts = e.ts AND r.labels = e.labels
ORDER BY e.labels, e.ts;
```

All timestamps use the metric table's native unit; bounds are inclusive output
grid bounds, lookup windows are open on the left, unmatched rows are omitted,
and canonical label JSON plus timestamp ordering is deterministic. SQLite
ordinary arithmetic is the intended direct-user surface. The Rust API adds
parser precedence, scalar/vector result typing, per-timestamp one-to-one
cardinality validation, metric-name policy, millisecond timestamps, portable
IEEE strings, bounded cumulative work, cancellation, and Prometheus envelopes.
`on`, `ignoring`, and group matching remain separate matrix rows. The exact
six-operation public-grid join executes in `tests/cli.sh` section 33.

### SQL-PROM-005: top-k per evaluation step

For `topk(:k, metric)`, rank every bounded evaluation timestamp independently:

```sql
WITH selected AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY ts ORDER BY value DESC, labels
  ) AS rank
  FROM selected
)
SELECT labels, ts, value
FROM ranked
WHERE rank <= :k
ORDER BY ts, value DESC, labels;
```

`:metric` and `:filter_json` are text and a matcher object (or SQL `NULL`);
`:start`, `:end`, `:step`, and `:lookback` are epoch seconds; and `:k` is a
non-negative integer. Grid bounds are inclusive and lookback is open-left.
Each timestamp is ranked separately. Labels are the original canonical JSON,
not the grouping labels. The label tie-breaker makes direct SQL deterministic.
For `bottomk`, change `DESC` to `ASC` in both `ORDER BY` clauses.

To emulate `by (service)`, add
`COALESCE(json_extract(labels, '$.service'), '') AS service` to `selected` and
partition by `service, ts`; a `without` modifier requires projecting a
canonical JSON object with the excluded labels removed. The Rust API owns
that general label projection, scalar parameter expressions and per-step
integer truncation, NaN/overflow errors, Prometheus's rule that NaN ranks
after numeric values for both directions, within-group instant rank ordering
(group order is unspecified), sparse range
series assembly, limits, cancellation, and envelopes. This statement executes
in `tests/cli.sh` section 45; the exact API contract is
`session_five_promql_topk_bottomk_rank_per_step_and_reopen`.

## LogsQL foundations and equivalents

### SQL-LOG-001: bounded filter, sort, and pagination

Use the virtual table's declared index-key columns for posting-list pruning:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND level = :level
  AND service = :service
ORDER BY ts DESC
LIMIT :limit OFFSET :offset;
```

For `_time:5m`, bind `:start_ms = :now_ms - 300000` and
`:end_ms = :now_ms`. Passing `:now_ms` makes evaluation deterministic. Exact
`ORDER BY ts ASC|DESC LIMIT/OFFSET` is consumed by the virtual table when the
remaining predicates allow it. Confirm with:

```sql
EXPLAIN QUERY PLAN
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms AND ts <= :end_ms
  AND level = :level AND service = :service
ORDER BY ts DESC
LIMIT :limit OFFSET :offset;
```

`service` is fast only when declared in `index_keys`. Other declared keys use
the same shape.

### SQL-LOG-002: message substring

Use the hidden exact engine predicate to filter before rows cross SQLite:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND message_contains = :needle
ORDER BY ts DESC
LIMIT :limit OFFSET :offset;
```

This is Timeless's case-insensitive literal substring operation. It is not by
itself VictoriaLogs word, phrase, prefix, or regexp semantics. The matrix
keeps those rows separate.

### SQL-LOG-003: exact count

Return one scalar without materializing matching log rows:

```sql
SELECT n
FROM timeless_log_count(
  'logs',
  :filter_json,
  :message_contains,
  :start_ms,
  :end_ms
);
```

Example `:filter_json`:

```json
{"level":"error","service":"api","status":"500"}
```

Only the table name is required; other arguments may be `NULL`. Fully covered
blocks can answer from metadata, while boundary and content predicates decode
as needed. Direct regression: `tests/cli.sh` section 43.

### SQL-LOG-004: distinct field values

Return bounded distinct values in lexical order:

```sql
SELECT value
FROM timeless_log_values(
  'logs',
  :field,
  :filter_json,
  :message_contains,
  :start_ms,
  :end_ms,
  :max_values
)
ORDER BY value;
```

The default cap is 1,000 and the hard cap is 100,000. This is the direct SQL
foundation for field discovery and distinct-value query work. Extension-backed
coverage lives in the Rust logs server storage contract; add a CLI recipe
regression before marking the corresponding LogsQL rows shipped.

### SQL-LOG-005: arbitrary metadata equality

When a key was not declared in `index_keys`, filter the returned typed JSON
with SQLite JSON functions:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND json_extract(metadata, '$.deployment.region') = :region
ORDER BY ts DESC
LIMIT :limit OFFSET :offset;
```

This is correct but requires decoding candidate rows. If measurements show a
stable, selective key is common, create a new table with that key declared in
`index_keys`; do not read or mutate shadow tables.

### SQL-LOG-006: counts by field and time bucket

For a declared indexed field, use the storage-aware bucket vector:

```sql
SELECT bucket_ts, group_key, n
FROM timeless_log_buckets(
  'logs', :group_key, :filter_json,
  :start_ms, :end_ms, :step_ms
)
ORDER BY bucket_ts, group_key;
```

The buckets are forward `[T,T+step)` intervals aligned to `:start_ms`; this is
not a PromQL-style trailing window. For arbitrary decoded numeric metadata,
ordinary SQL remains available:

```sql
SELECT
  ((ts - :start_ms) / :step_ms) * :step_ms + :start_ms AS bucket_ts,
  COUNT(*) AS n,
  AVG(CAST(json_extract(metadata, '$.duration_ms') AS REAL)) AS avg_duration
FROM logs
WHERE ts >= :start_ms AND ts <= :end_ms
GROUP BY bucket_ts
ORDER BY bucket_ts;
```

The second form is deliberately decode-heavy. Measurements determine whether
a future typed storage-aware aggregate earns a new `EXT` row.

## Adding the next recipe

When a matrix row uses `SQL` as its target or foundation:

1. add its stable row ID to the recipe index;
2. include an executable statement and setup;
3. describe exact and non-equivalent language behavior;
4. add the statement to `tests/cli.sh` with hand-verified output; and
5. link the recipe from the matrix row before changing its status to
   `shipped`.

If the statement needs a private shadow table, application callback, or
undocumented decoder, it is not a valid SQL equivalent.
