# PromQL feature matrix

This living matrix defines PromQL work that belongs in `timeless-libsql`.
It separates public storage/query primitives (`EXT`/`SQL`) from PromQL
language behavior in the Rust metrics API (`API`) and application behavior in
higher-order libraries (`LIB`). See [Query feature maps](QUERY_FEATURES.md)
for the ownership and completion rules.

Baseline: `a72634e`, 2026-08-04. The exact upstream source and container pins
are recorded in [Query semantic oracles](QUERY_ORACLES.md). The language
snapshot is
[PromQL basics](https://prometheus.io/docs/prometheus/latest/querying/basics/),
[operators](https://prometheus.io/docs/prometheus/latest/querying/operators/),
and [functions](https://prometheus.io/docs/prometheus/latest/querying/functions/).
The established Elixir oracle is
[`TimelessMetrics.PromQL`](https://github.com/awksedgreep/timeless_metrics/blob/main/lib/timeless_metrics/promql.ex)
plus its `promql_conformance_test.exs` and VictoriaMetrics differential corpus.

The current Rust server supports the rows marked `shipped` below. Nothing else
should be inferred from native `metric=` query parameters or from extension
kernels with similar names.

## Legend and foundations

Priorities are `P0` (restore behavior already delivered by TimelessMetrics),
`P1` (stable PromQL float-series core), `P2` (useful compatibility after the
core), `EXP` (experimental upstream), and `DEFER` (blocked by an explicit data
model or product decision).

The foundation column uses these public extension surfaces:

| name | public primitive |
|---|---|
| `CAT` | `timeless_series`, `timeless_label_values` |
| `LATEST` | `timeless_latest`, `timeless_latest_frame` |
| `RAW` | `timeless_raw`, `timeless_raw_batches`, `timeless_raw_frame` |
| `GRID` | `timeless_grid` |
| `WINDOW` | `timeless_window`, `timeless_window_batches` |
| `AGG` | `timeless_aggregate`, `timeless_aggregate_frame` |
| `ROLLUP` | `timeless_rollup`, `timeless_rollup_batches` |
| `SQL` | ordinary SQLite expression/composition over public results |

`Elixir` means the existing implementation is a porting/conformance oracle,
not that the Rust server may fall back across the process boundary.
Rows with an `SQL` foundation must link an executable statement from the
[SQL equivalents cookbook](QUERY_SQL_EQUIVALENTS.md) before becoming
`shipped`.

## Language, selectors, and evaluation model

| ID | construct/behavior | Rust now | Elixir | foundation | target | priority | notes |
|---|---|---|---|---|---|---|---|
| `PQL-S01` | exact metric-name instant selector ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-001-instant-selector)) | shipped | yes | `GRID`, `LATEST`, `RAW` | `API` | P0 | Exact 300-second `(T-lookback,T]` last-sample sweep is implemented. |
| `PQL-S02` | label matchers `=`, `!=`, `=~`, `!~` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-001-instant-selector)) | shipped | yes | `CAT`, `RAW` | `API` | P0 | Regex is anchored; missing labels compare as the empty string. |
| `PQL-S03` | duplicate matchers use AND semantics | shipped | yes | `CAT`, `RAW` | `API` | P0 | Keep a regression for duplicates on the same label. |
| `PQL-S04` | nameless selectors such as `{job="api"}` | shipped | yes | `CAT`, `RAW` | `API` | P0 | One bounded streaming public-catalog pass groups matching series by name before exact-metric raw reads; missing labels, limits, ordering, range roots/functions, cancellation, and reopen are pinned. |
| `PQL-S05` | regex/negative `__name__` and multi-metric selection | shipped | yes | `CAT`, `RAW` | `API` | P0 | Anchored regex, negative and duplicate-name matchers run in the bounded catalog pass before exact-metric payload reads; empty-matching legality, limits, ordering, oracle parity, and reopen are pinned. |
| `PQL-S06` | range selector `[W]` with `(T-W,T]` boundaries ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-006-range-selector)) | shipped | yes | `RAW`, `WINDOW` | `API` | P0 | Instant-query roots return matrices; range-query roots fail as `bad_data`; the left boundary is filtered after the inclusive public raw read. |
| `PQL-S07` | positive and negative `offset` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-008-temporal-selector-modifiers)) | shipped | yes | `RAW`, `GRID`, `SQL` | `API` | P0 | Signed millisecond lookup shifts preserve outer timestamps, lookback/window boundaries, limits and cancellation across instant/range/root/function forms. |
| `PQL-S08` | `@ timestamp`, `@ start()`, `@ end()` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-008-temporal-selector-modifiers)) | shipped | yes | `RAW`, `GRID`, `SQL` | `API` | P0 | Fixed selection instants resolve before offset; numeric/pre-epoch, start/end range context, output timestamps, modifier order, oracle parity and reopen are pinned. |
| `PQL-S09` | subqueries `[range:resolution]` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-009-aligned-selector-subquery)) | shipped | yes | `RAW`, `GRID`, `SQL` | `API` | P0 | Open-left/global grids, configurable 15-second default, root/consumer types, nested shipped vector plans, `@`/offset context, outer timestamps, metric-name policy, work/response/intermediate limits, cancellation, oracle parity and reopen are pinned. |
| `PQL-S10` | step-relative ranges such as `[5i]` | missing | yes | `RAW`, `GRID` | `API` | P2 | MetricsQL-compatible extension used by the existing oracle. |
| `PQL-S11` | scalar literals and IEEE `NaN`/`Inf` behavior | shipped | yes | none | `API` | P0 | Decimal/exponent, hexadecimal, octal, underscored, signed, `NaN`, and `Inf` roots use Prometheus value strings; SQLite cannot portably preserve NaN as an ordinary REAL. |
| `PQL-S12` | duration literals, including compound and `ms` | shipped | yes | none | `API` | P0 | Scalar literals, range windows, numeric/duration steps, and numeric/RFC3339 evaluation times use Prometheus's millisecond clock; second-native samples keep their public storage format. |
| `PQL-S13` | string literals and escaping | shipped | yes | none | `API` | P0 | Double-quoted escapes and raw backtick strings return the Prometheus instant `string` envelope; range queries reject string roots. |
| `PQL-S14` | UTF-8 quoted metric names | missing | no | `CAT`, `RAW` | `API` | P2 | Add only with ingestion/name round-trip fixtures. |
| `PQL-S15` | line comments | missing | no | none | `API` | P2 | Parser-only, but still requires complete-expression tests. |
| `PQL-S16` | exact query grid and configurable lookback | shipped | yes | `GRID`, `RAW` | `API` | P0 | Request-scoped `lookback_delta`, the zero/default rule, exact millisecond open-left boundaries, non-aligned ends, and overflow-safe 11,000-point grids apply to every shipped expression. |
| `PQL-S17` | Prometheus stale-marker semantics | partial | partial | `RAW` | `EXT` | P1 | See `QSF-004`; first decide whether ingest/storage preserves stale markers. The API consumes the resulting contract. |
| `PQL-S18` | instant vector, range vector, scalar, and string types | shipped | yes | none | `API` | P0 | Exact instant/empty result typing is pinned across all four Prometheus values; range evaluation accepts scalar/vector roots and returns a matrix while string/range-vector roots fail. Prometheus envelopes have no honest SQL equivalent. |
| `PQL-S19` | deterministic Prometheus error/result envelopes | shipped | yes | none | `API` | P0 | Required range parameters, parameter/type diagnostics, strict unsupported behavior, and GET/POST data/error envelopes are pinned for every shipped node. Each future grammar row must extend this contract; optional warning/info annotations are tracked separately by `PQL-S23`, and documented stricter resource/input choices remain intentional. |
| `PQL-S20` | 11,000 points/series, sample, row, response, and deadline limits | shipped | yes | `RAW`, `WINDOW` | `API` | P0 | Configurable hard owner limits cover pre-decode raw/window work, final points, bounded serialization, and deadlines; auth may only tighten them. |
| `PQL-S21` | cancellation and SQLite reader reuse | shipped | n/a | all | `API` | P0 | Every new evaluator loop must check the same cancellation token. |
| `PQL-S22` | native histogram sample type | deferred | no | none | `DEFER` | DEFER | See `QSF-003`; classic `_bucket` series remain ordinary floats. |
| `PQL-S23` | Prometheus `warnings`/`infos` response annotations, including non-counter metric-name lint | missing | no | none | `API` | P1 | See `QSF-050`; preserve exact text, source position, deduplication, and GET/POST instant/range envelope placement without weakening result parity. |

## Operators and cross-series aggregation

These are expression operations. Existing extension frames should feed them;
they do not justify a PromQL-aware extension API.

| ID | construct | Rust now | Elixir | foundation | target | priority |
|---|---|---|---|---|---|---|
| `PQL-O01` | unary minus ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-010-unary-minus)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O02` | arithmetic `+ - * / % ^` across scalar/vector pairs ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-004-vector-arithmetic-with-label-matching)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O03` | comparisons `== != > < >= <=` and `bool` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-011-comparison-filter-and-bool)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O04` | set operators `and`, `or`, `unless` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-012-set-membership)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O05` | one-to-one vector matching ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-004-vector-arithmetic-with-label-matching)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O06` | `on(...)` and `ignoring(...)` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-013-on-and-ignoring-label-matching)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O07` | `group_left` and `group_right` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-014-group_left-and-group_right)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O08` | trigonometric binary `atan2` | missing | no | `SQL` | `API` | P1 |
| `PQL-O09` | `sum` with `by`/`without` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-003-cross-series-sum-by-label)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O10` | `avg` with `by`/`without` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-015-cross-series-average-by-label)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O11` | `min` and `max` with `by`/`without` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-016-cross-series-minimum-and-maximum)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O12` | `count` and `group` with `by`/`without` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-017-cross-series-count-and-group)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O13` | `stddev` and `stdvar` aggregations ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-018-cross-series-population-variance-and-standard-deviation)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O14` | `topk` and `bottomk` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-005-top-k-per-evaluation-step)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O15` | `quantile` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-019-cross-series-quantile)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O16` | `count_values` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-020-count-series-by-sample-value)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O17` | experimental `limitk` and `limit_ratio` | missing | no | `SQL` | `API` | EXP |
| `PQL-O18` | experimental binary fill modifiers | missing | no | `SQL` | `API` | EXP |
| `PQL-O19` | native-histogram trim operators `</` and `>/` | deferred | no | none | `DEFER` | DEFER |

## Range, counter, and regression functions

`WINDOW` can accelerate many rows, but it is not automatically PromQL. Each
row must prove range boundaries, carry-in, resets, extrapolation, sparse data,
NaN handling, output timestamps, and metric-name policy. `RAW` is the
correctness fallback.

| ID | function | Rust now | Elixir | foundation | target | priority |
|---|---|---|---|---|---|---|
| `PQL-R01` | `avg_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-002-avg_over_time)); compensated cancellation, overflow fallback, IEEE values, subqueries, exact boundaries, limits, and native-window work counters are pinned | shipped | yes | `WINDOW`, `RAW` | `API` | P0 |
| `PQL-R02` | `min_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-021-min_over_time)); exact boundaries, IEEE/NaN and stable signed-zero behavior, subqueries, limits, cancellation, and reopen are pinned | shipped | yes | `WINDOW`, `RAW` | `API` | P0 |
| `PQL-R03` | `max_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-022-max_over_time)); exact boundaries, IEEE/NaN and stable signed-zero behavior, subqueries, limits, cancellation, and reopen are pinned | shipped | yes | `WINDOW`, `RAW` | `API` | P0 |
| `PQL-R04` | `sum_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-023-sum_over_time)); compensated cancellation, overflow, IEEE values, exact boundaries, subqueries, limits, cancellation, and reopen are pinned | shipped | yes | `WINDOW`, `RAW` | `API` | P0 |
| `PQL-R05` | `count_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-024-count_over_time)); every IEEE sample, exact boundaries, subqueries, empty windows, limits, cancellation, and reopen are pinned | shipped | yes | `WINDOW`, `RAW` | `API` | P0 |
| `PQL-R06` | `last_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-025-last_over_time)); exact last IEEE bits, boundaries, subqueries, exceptional metric-name retention, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R07` | experimental `first_over_time`; Prometheus 3.13.2 rejects it unless `promql-experimental-functions` is enabled, so the stable Timeless tier rejects it explicitly until a separately enabled experimental compatibility tier and oracle gate exist | experimental | yes | `RAW`, `SQL` | `API` | EXP |
| `PQL-R08` | `present_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-026-present_over_time)); exact non-empty presence, boundaries, IEEE samples, subqueries, limits, cancellation, and reopen are pinned | shipped | yes | `WINDOW`, `SQL` | `API` | P0 |
| `PQL-R09` | `quantile_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-027-quantile_over_time)); scalar expressions, interpolation, raw-NaN rank, infinities, stable signed-zero order, exact boundaries, subqueries, errors, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R10` | `stddev_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-028-stddev_over_time-and-stdvar_over_time)); population Welford state, wide magnitudes, IEEE values, exact boundaries, subqueries, errors, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R11` | `stdvar_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-028-stddev_over_time-and-stdvar_over_time)); population Welford state, wide magnitudes, exact Prometheus float strings, IEEE values, boundaries, subqueries, errors, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R12` | `rate` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-029-rate)); float-counter resets, edge extrapolation, zero-point clamp, sparse/two-sample omission, boundaries, modifiers, subqueries, NaN/infinities, limits, cancellation, and reopen are pinned; the native kernel is not relabeled | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R13` | `irate` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-030-irate)); last-two-sample reset substitution, actual-interval normalization, zero-interval/singleton omission, boundaries, modifiers, subqueries, NaN/infinities, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R14` | `increase` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-031-increase)); reset correction, edge extrapolation, zero-point clamp, sparse/singleton omission, boundaries, modifiers, subqueries, NaN/infinities, limits, cancellation, and reopen are pinned; the native kernel is not relabeled | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R15` | `delta` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-032-delta)); gauge decreases, edge extrapolation without reset or zero correction, sparse/singleton omission, boundaries, modifiers, subqueries, NaN/infinities, limits, cancellation, and reopen are pinned; the native kernel is not relabeled | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R16` | `idelta` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-033-idelta)); last-two-sample gauge increases/decreases, zero-interval/singleton omission, boundaries, modifiers, subqueries, NaN/infinities, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R17` | `deriv` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-034-deriv)); timestamp-centered compensated least-squares slope, constant/IEEE behavior, exact boundaries, modifiers, subqueries, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R18` | `predict_linear` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-035-predict_linear)); evaluation-time-centered compensated regression, scalar horizons, exact boundaries, modifiers, subqueries, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R19` | `changes` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-036-changes)); transition counting across finite, NaN, infinity, and signed-zero values, exact boundaries, modifiers, subqueries, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R20` | `resets` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-037-resets)); strict float-counter decrease counting across finite, NaN, infinity, and signed-zero values, exact boundaries, modifiers, subqueries, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-R21` | `double_exponential_smoothing` | missing | no | `RAW` | `API` | P1 |
| `PQL-R22` | experimental `mad_over_time` | missing | no | `RAW` | `API` | EXP |
| `PQL-R23` | experimental `ts_of_min/max/first/last_over_time` | missing | no | `RAW` | `API` | EXP |

## Value, label, sorting, and time functions

These are `API` work unless measurement proves a repeated storage-side
reduction is materially cheaper. They should normally operate on already
bounded frames.

| ID | function/family | Rust now | Elixir | foundation | target | priority |
|---|---|---|---|---|---|---|
| `PQL-F01` | `abs` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-038-abs)); finite and IEEE floats, signed zero, metric-name removal, range grids, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F02` | `ceil`, `floor`, `round` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-039-ceil-floor-and-round)); float/IEEE values, signed zero, round ties and scalar steps, range grids, names, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F03` | `clamp`, `clamp_min`, `clamp_max` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-040-clamp-clamp_min-and-clamp_max)); scalar bounds, finite/IEEE values, signed zero, inverted bounds, range grids, names, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F04` | `sqrt`, `exp`, `ln`, `log2`, `log10` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-041-sqrt-exp-ln-log2-and-log10)); valid domains, NaN/infinities/signed zero, range grids, names, nesting, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F05` | `sgn` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-042-sgn)); finite/IEEE values, signed zero, range grids, names, nesting, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F06` | `acos`, `acosh`, `asin`, `asinh`, `atan`, `atanh` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-043-inverse-trigonometric-and-hyperbolic-functions)); domains, endpoint infinities, NaN/signed zero, range grids, names, nesting, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F07` | `cos`, `cosh`, `sin`, `sinh`, `tan`, `tanh` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-044-trigonometric-and-hyperbolic-functions)); finite/IEEE values, infinities, signed zero, range grids, names, nesting, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F08` | `deg`, `rad`, `pi` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-045-deg-rad-and-pi)); conversion order, scalar instant/range types, IEEE values, signed zero, names, nesting, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F09` | `label_replace`; full-match dot-all regex, numbered/named/literal-dollar captures, missing/empty sources, unmatched identity, destination deletion/overwrite, metric names, Prometheus 3 UTF-8 label names, incrementally bounded expansion, range grids, nesting, errors, limits, cancellation, and reopen are pinned | shipped | yes | none | `API` | P0 |
| `PQL-F10` | `label_join` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-046-label_join)); ordered arbitrary sources, missing/empty/duplicate/zero sources, original-label snapshots, deletion/overwrite, metric names, Prometheus 3 UTF-8 label names, incrementally bounded output, ranges, nesting, errors, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F11` | `absent` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-047-absent)); present/empty vectors, step-local sparse ranges, unique nonempty equality-label derivation, metric/regex/negative/empty/duplicate exclusions, composed inputs, NaN presence, grid work, limits, cancellation, and reopen are pinned | shipped | yes | `CAT`, `SQL` | `API` | P0 |
| `PQL-F12` | `absent_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-048-absent_over_time)); exact open-left windows, millisecond boundaries, sparse range steps, equality-label derivation/exclusions, NaN presence, direct selectors, subqueries, limits, cancellation, and reopen are pinned | shipped | yes | `WINDOW`, `RAW`, `SQL` | `API` | P0 |
| `PQL-F13` | `sort` and `sort_desc` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-049-sort-and-sort_desc)); ascending/descending instant order, NaN-last IEEE behavior, signed-zero preservation, labels/names, deterministic ties, nested and empty vectors, range-matrix label order, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F14` | experimental `sort_by_label`, `sort_by_label_desc` | missing | no | `SQL` | `API` | EXP |
| `PQL-F15` | `scalar` and `vector` | missing | yes | `SQL` | `API` | P0 |
| `PQL-F16` | `time` and `timestamp` | missing | yes | `SQL` | `API` | P0 |
| `PQL-F17` | `minute`, `hour`, `day_of_week`, `day_of_month` | missing | yes | `SQL` | `API` | P0 |
| `PQL-F18` | `day_of_year`, `days_in_month`, `month`, `year` | missing | yes | `SQL` | `API` | P0 |
| `PQL-F19` | `start`, `end`, `start_timestamp`, `step`, `range` | missing | partial | `SQL` | `API` | P2 |
| `PQL-F20` | `min_of` and `max_of` | missing | no | `SQL` | `API` | P2 |
| `PQL-F21` | experimental `info` | missing | no | `CAT`, `SQL` | `API` | EXP |

## Histogram functions

Classic Prometheus histograms are ordinary float series named `*_bucket` with
an `le` label; they fit the existing storage model. Native histograms do not.

| ID | function | Rust now | Elixir | foundation | target | priority | notes |
|---|---|---|---|---|---|---|---|
| `PQL-H01` | `histogram_quantile` over classic buckets | missing | yes | `RAW`, `SQL` | `API` | P0 | Preserve monotonicity correction and label grouping. |
| `PQL-H02` | `histogram_fraction` over classic buckets | missing | no | `RAW`, `SQL` | `API` | P2 | Implement only after an upstream differential fixture. |
| `PQL-H03` | `histogram_quantiles` | missing | no | `RAW`, `SQL` | `API` | P2 | Confirm the exact upstream/stability contract first. |
| `PQL-H04` | `histogram_avg/count/sum/stddev/stdvar` on native samples | deferred | no | none | `DEFER` | DEFER | Requires `QSF-003` storage work. |

## MetricsQL compatibility tier

PromQL parity is the first gate. These VictoriaMetrics conveniences already
exist in `TimelessMetrics.PromQL` and are useful compatibility targets after
the P0 PromQL rows. They remain `API` composition, not extension syntax.

| ID | construct | Rust now | Elixir | target | priority |
|---|---|---|---|---|---|
| `MQL-01` | binary `default`, `if`, `ifnot` | missing | yes | `API` | P2 |
| `MQL-02` | `keep_metric_names` | missing | yes | `API` | P2 |
| `MQL-03` | `union` and `alias` | missing | yes | `API` | P2 |
| `MQL-04` | `label_set` and `label_del` | missing | yes | `API` | P2 |
| `MQL-05` | `default_rollup` and window-less rollups | missing | yes | `API` | P2 |
| `MQL-06` | `range_avg/min/max/sum` | missing | yes | `API` | P2 |
| `MQL-07` | `running_avg/min/max/sum` | missing | yes | `API` | P2 |
| `MQL-08` | remaining MetricsQL functions/operators | missing | partial | `API` | DEFER |

## Higher-order library boundary

The following are important metrics features, but they are not PromQL
expression/storage work in `timeless-libsql`:

| concern | owner |
|---|---|
| scrape-target CRUD, service discovery, credentials, and scheduling policy | `timeless_metrics` / TimelessUI (`LIB`); the Rust scraper executes assigned work |
| recording and alerting rule lifecycle, notification routing, and silences | higher-order metrics/control-plane libraries (`LIB`) |
| saved queries, dashboards, variables, and UI state | TimelessUI/Canvas/dashboard libraries (`LIB`) |
| tenant and token issuance policy | Phoenix control plane (`LIB`); Rust enforces claims and limits |
| remote federation or cluster query planning | separate product decision; do not hide it inside a local extension TVF |

## Parity gates

P0 is complete only when the Rust API runs the existing TimelessMetrics corpus
without an Elixir fallback and a fixed upstream referee passes the same
supported classifications. In addition to numeric values, the oracle compares
timestamps, label sets, metric-name retention, ordering where specified,
empty results, errors, NaN/Inf encoding, reset behavior, and range boundaries.

Every row that changes storage access also records narrow/wide p50/p95/p99,
bytes returned by the extension, decoded sample count, response bytes,
cancellation, and RSS HWM. The result determines whether the implementation
continues to use `RAW`, can safely use a packed `WINDOW`/`AGG` primitive, or
justifies a new `EXT` query vector.

Direct SQLite/libSQL equivalents for the current selector/window surface and
planned SQL-composed aggregations, joins, and top-k are maintained in the
[SQL equivalents cookbook](QUERY_SQL_EQUIVALENTS.md).
