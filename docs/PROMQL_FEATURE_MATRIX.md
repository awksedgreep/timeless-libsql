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
| `PQL-S10` | step-relative ranges such as `[5i]` | deferred | yes | `RAW`, `GRID` | `DEFER` | DEFER | Pinned Prometheus rejects this syntax, so it is not part of stable PromQL. Exact request-step scaling is tracked as `MQL-09`; the stable endpoint pins Prometheus's diagnostic through GET, POST, and reopen. |
| `PQL-S11` | scalar literals and IEEE `NaN`/`Inf` behavior | shipped | yes | none | `API` | P0 | Decimal/exponent, hexadecimal, octal, underscored, signed, `NaN`, and `Inf` roots use Prometheus value strings; SQLite cannot portably preserve NaN as an ordinary REAL. |
| `PQL-S12` | duration literals, including compound and `ms` | shipped | yes | none | `API` | P0 | Scalar literals, range windows, numeric/duration steps, and numeric/RFC3339 evaluation times use Prometheus's millisecond clock; second-native samples keep their public storage format. |
| `PQL-S13` | string literals and escaping | shipped | yes | none | `API` | P0 | Double-quoted escapes and raw backtick strings return the Prometheus instant `string` envelope; range queries reject string roots. |
| `PQL-S14` | UTF-8 quoted metric names ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-001-instant-selector)) | shipped | no | `CAT`, `RAW`, `SQL` | `API` | P2 | Prometheus 3 quoted names and label keys preserve UTF-8/escapes through text ingest, catalog identity, GET/POST instant and range selection, compact, and reopen; direct SQL binds the decoded name as ordinary TEXT. |
| `PQL-S15` | line comments | shipped | no | none | `API` | P2 | Leading/trailing/inter-token comments, multiline calls, comment-only errors, and `#` inside quoted strings are pinned; source/annotation scanners skip fake calls and unmatched delimiters in comments. |
| `PQL-S16` | exact query grid and configurable lookback | shipped | yes | `GRID`, `RAW` | `API` | P0 | Request-scoped `lookback_delta`, the zero/default rule, exact millisecond open-left boundaries, non-aligned ends, and overflow-safe 11,000-point grids apply to every shipped expression. |
| `PQL-S17` | Prometheus stale-marker semantics | deferred | partial | `RAW` | `DEFER` | DEFER | Exact NaN payloads survive public binary ingest, flush, and reopen, but exposition `NaN` is an ordinary NaN, Victoria JSON cannot carry a NaN payload, and the Rust server has no bit-preserving remote-write ingress. Shipping requires an explicit marker-capable ingress plus selector/range/window paths that exclude only `0x7ff0000000000002` while preserving ordinary NaNs; see `QSF-004` and `QSF-065`. |
| `PQL-S18` | instant vector, range vector, scalar, and string types | shipped | yes | none | `API` | P0 | Exact instant/empty result typing is pinned across all four Prometheus values; range evaluation accepts scalar/vector roots and returns a matrix while string/range-vector roots fail. Prometheus envelopes have no honest SQL equivalent. |
| `PQL-S19` | deterministic Prometheus error/result envelopes | shipped | yes | none | `API` | P0 | Required range parameters, parameter/type diagnostics, strict unsupported behavior, and GET/POST data/error envelopes are pinned for every shipped node. Each future grammar row must extend this contract; optional warning/info annotations are tracked separately by `PQL-S23`, and documented stricter resource/input choices remain intentional. |
| `PQL-S20` | 11,000 points/series, sample, row, response, and deadline limits | shipped | yes | `RAW`, `WINDOW` | `API` | P0 | Configurable hard owner limits cover pre-decode raw/window work, final points, bounded serialization, and deadlines; auth may only tighten them. |
| `PQL-S21` | cancellation and SQLite reader reuse | shipped | n/a | all | `API` | P0 | Every new evaluator loop must check the same cancellation token. |
| `PQL-S22` | native histogram sample type | deferred | no | none | `DEFER` | DEFER | The additive capability handshake now declares `sample_types=["float64"]` and `native_histograms=false`; classic `_bucket` series remain ordinary floats. Shipping requires a separately versioned typed sample, batch, chunk, SQL, packed-result, ingress, rollup, retention, and migration design; see `QSF-003` and `QSF-258`. |
| `PQL-S23` | Prometheus `warnings`/`infos` response annotations, including non-counter metric-name lint | shipped | no | none | `API` | P1 | Exact quantile, sort-on-range, non-counter-name, malformed-bucket, and histogram-repair annotations preserve text, line/column source positions, type, deterministic deduplication/merge, ten-item caps, GET/POST instant/range placement, response limits, and omission from unaffected/empty results; see `QSF-050`. |

`PQL-S22` is an explicit data-model boundary, not an unimplemented SQL
expression. Current metric row, named/resolved batch, and chunk sample
payloads contain one timestamp plus one IEEE-754 float. Catalogs identify
those float series; `TRF1` and `TWB1` carry float values; `TRB1` carries
float-derived rollup aggregates. None has a typed histogram field.
`timeless_capabilities()` now states that domain machine-readably without
changing data ABI 1 or any readable format. Classic histogram `_bucket`,
`_sum`, and `_count` series remain independent float series and must never be
reinterpreted as a native histogram.

A future typed design must be additive and backward-readable. Before this row
can move, it must specify exponential and custom schemas, zero threshold and
count, total count and sum, positive/negative spans and bucket populations,
counter-reset/gauge hints, mixed float/histogram series, exact validation and
corruption errors, marker/staleness interaction, marker-capable ingress,
public named/resolved batches, SQLite projection, packed raw/window/result
frames, chunk compression and indexes, rollup merge/downsampling, retention,
transactions, migration, capability/data-ABI negotiation, limits,
cancellation, backup/reopen, and downgrade behavior. Pinned Prometheus source
at the recorded commit is the semantic model; no storage schema is inferred
from classic buckets or a query-only operator.

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
| `PQL-O08` | trigonometric binary `atan2` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-055-atan2)); scalar/vector directions, vector matching/modifiers, range grids, IEEE quadrants, deterministic Go-compatible rounding, limits, SQL, oracle parity, and reopen are pinned | shipped | no | `SQL` | `API` | P1 |
| `PQL-O09` | `sum` with `by`/`without` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-003-cross-series-sum-by-label)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O10` | `avg` with `by`/`without` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-015-cross-series-average-by-label)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O11` | `min` and `max` with `by`/`without` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-016-cross-series-minimum-and-maximum)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O12` | `count` and `group` with `by`/`without` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-017-cross-series-count-and-group)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O13` | `stddev` and `stdvar` aggregations ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-018-cross-series-population-variance-and-standard-deviation)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O14` | `topk` and `bottomk` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-005-top-k-per-evaluation-step)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O15` | `quantile` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-019-cross-series-quantile)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O16` | `count_values` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-020-count-series-by-sample-value)) | shipped | yes | `SQL` | `API` | P0 |
| `PQL-O17` | experimental `limitk` and `limit_ratio`; pinned Prometheus 3.13.2 rejects both unless `promql-experimental-functions` is enabled, and the stable Timeless GET/POST/reopen paths preserve that diagnostic without reading storage; implementation requires a separately configured experimental tier and oracle plus bounded group state, while full upstream parity also requires typed native-histogram samples | experimental | no | `RAW` | `API` | EXP |
| `PQL-O18` | experimental binary `fill`, `fill_left`, and `fill_right` modifiers ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-057-fill-missing-one-to-one-vector-matches)); pinned Prometheus 3.13.2 requires `promql-binop-fill-modifiers`, and stable Timeless GET/POST/reopen paths now preserve its diagnostic without reading storage; future execution requires a separately configured feature-enabled oracle and complete bounded matching semantics | experimental | no | `SQL` | `API` | EXP |
| `PQL-O19` | native-histogram trim operators `</` and `>/`; pinned Prometheus 3.13.2 accepts both in its stable grammar, drops float/float pairs with typed infos, and only produces values for native-histogram-left/scalar-right samples; stable Timeless now fails explicitly before storage because its retained metrics model has no typed native-histogram sample | deferred | no | none | `DEFER` | DEFER |

`PQL-O17` is deliberately not an extension or ordinary-SQL row. Prometheus
selects `limit_ratio` members from its canonical label-set hash and `limitk`
from evaluator order after the child vector has been evaluated. Neither
contract is exposed by portable SQLite/libSQL SQL, and neither can prune the
underlying raw scan. The stable API therefore fails before storage. A future
experimental tier belongs in the Rust planner/evaluator over public raw
results; it must be enabled explicitly and tested against an oracle started
with the same feature flag.

`PQL-O18` is also API composition, not an extension primitive. Executable
[`SQL-PROM-057`](QUERY_SQL_EQUIVALENTS.md#sql-prom-057-fill-missing-one-to-one-vector-matches)
shows the honest direct-SQL foundation as a bounded full-outer composition of
two public grids with independently optional defaults. Upstream fill semantics
still require numeric literals, arithmetic/comparison behavior, matching and
group cardinality, label/name projection, uniqueness errors, and per-step
limits. Set operators and histogram samples are rejected upstream. The stable
API fails before either child scan; a future experimental Rust tier must own
the complete language behavior after both child vectors are evaluated.

`PQL-O19` cannot be approximated over classic `_bucket` float series. Upstream
trims the positive, negative, zero, custom, and infinite buckets inside one
typed native-histogram sample, interpolates partial buckets, and recomputes
count and sum. A portable row query has neither that sample identity nor its
schema, spans, zero threshold/count, reset hint, or custom bounds. Timeless
therefore reports the typed-storage prerequisite at the operator position and
performs no storage read. Shipping requires the `PQL-S22` data model, public
batch/SQL representation, backward-readable format, ingress, result envelope,
bucket math, annotations, limits, cancellation, durability, and pinned-oracle
parity; classic histogram support remains unchanged.

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
| `PQL-R21` | experimental `double_exponential_smoothing`; pinned Prometheus 3.13.2 rejects it in the default stable tier, and Timeless does the same explicitly through GET, POST, and reopen; implementation requires a separately enabled experimental compatibility tier and oracle gate | experimental | no | `RAW` | `API` | EXP |
| `PQL-R22` | experimental `mad_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-058-mad_over_time)); pinned Prometheus 3.13.2 rejects it unless `promql-experimental-functions` is enabled, and stable Timeless GET/POST/reopen paths now preserve that diagnostic without reading storage; executable finite-float SQL composes two linear medians over public raw rows | experimental | no | `RAW`, `SQL` | `API` | EXP |
| `PQL-R23` | experimental `ts_of_min/max/first/last_over_time` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-059-timestamp-of-range-functions)); pinned Prometheus 3.13.2 rejects all four unless `promql-experimental-functions` is enabled, and stable Timeless GET/POST/reopen paths now preserve each diagnostic without reading storage; executable finite-float SQL covers first/last and last-tied min/max timestamps over public raw rows | experimental | no | `RAW`, `SQL` | `API` | EXP |

`PQL-R22` is not enabled merely because its finite-float statistic is
expressible in ordinary SQL. Pinned upstream takes the median of float samples
and then the median absolute deviation, ignores all-histogram ranges, and
emits an info for mixed float/native-histogram ranges. Stable Timeless rejects
the feature-gated function before storage. Executable
[`SQL-PROM-058`](QUERY_SQL_EQUIVALENTS.md#sql-prom-058-mad_over_time) gives
direct users the bounded finite-float composition; a future experimental Rust
tier must be separately configured and pinned for boundaries, raw NaN and
signed-zero order, infinities, subqueries, labels, annotations, limits, and
cancellation. Full upstream mixed-range parity also requires `PQL-S22` typed
native-histogram samples.

`PQL-R23` remains disabled in the stable tier even though its float subset is
ordinary bounded SQL. Executable
[`SQL-PROM-059`](QUERY_SQL_EQUIVALENTS.md#sql-prom-059-timestamp-of-range-functions)
selects the first/last source timestamp or the latest timestamp tied for the
minimum/maximum float value. Upstream first/last also consider native
histograms; min/max omit histogram-only ranges and annotate mixed ranges. A
future experimental Rust tier must use a feature-enabled oracle and own exact
millisecond conversion, open-left windows, subqueries, raw NaN and signed-zero
ordering, labels/names, annotations, limits, and cancellation. Complete
first/last and mixed-range parity additionally requires `PQL-S22` typed
native-histogram samples.

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
| `PQL-F14` | experimental `sort_by_label`, `sort_by_label_desc`; pinned Prometheus 3.13.2 rejects both unless `promql-experimental-functions` is enabled, and stable Timeless GET/POST/reopen paths now preserve each diagnostic without reading storage; exact natural multi-label ordering has no portable core-SQL equivalent | experimental | no | `RAW` | `API` | EXP |
| `PQL-F15` | `scalar` and `vector` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-050-scalar-and-vector)); zero/one/multiple per-step cardinality, stored/result NaN, nameless vectors, exact instant/range types, nesting, grid work, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F16` | `time` and `timestamp` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-051-time-and-timestamp)); millisecond evaluation clocks, stored-sample provenance, response timestamps, lookback, `offset`, `@`, composed-sample time, labels/names, IEEE samples, ranges, limits, cancellation, and reopen are pinned | shipped | yes | `RAW`, `SQL` | `API` | P0 |
| `PQL-F17` | `minute`, `hour`, `day_of_week`, `day_of_month` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-052-minute-hour-day_of_week-and-day_of_month)); optional `vector(time())` default, UTC fields, Sunday-zero numbering, fractional truncation, non-finite/out-of-range sentinel conversion, labels/names, nested range grids, errors, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F18` | `day_of_year`, `days_in_month`, `month`, `year` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-053-day_of_year-days_in_month-month-and-year)); optional `vector(time())` default, UTC and one-indexed fields, Gregorian leap years, fractional and non-finite/out-of-range conversion, labels/names, nested range grids, errors, limits, cancellation, and reopen are pinned | shipped | yes | `SQL` | `API` | P0 |
| `PQL-F19` | experimental query-context `start`, `end`, `step`, and `range`; `start_timestamp` is not PromQL; pinned Prometheus requires `promql-duration-expr` for the four context functions and reports `start_timestamp` as unknown; the stable endpoint preserves those exact failures while `@ start()`/`@ end()` remain shipped; MetricsQL ownership is `MQL-10` | experimental | partial | `SQL` | `API` | EXP |
| `PQL-F20` | experimental `min_of` and `max_of`; pinned Prometheus requires `promql-duration-expr`; the stable endpoint rejects both through GET, POST, and reopen; stable MetricsQL ownership is `MQL-11` | experimental | no | `SQL` | `API` | EXP |
| `PQL-F21` | experimental `info`; pinned Prometheus 3.13.2 rejects both arities unless `promql-experimental-functions` is enabled, and stable Timeless GET/POST/reopen paths now preserve that diagnostic before child evaluation or storage; full execution requires `PQL-S17` stale-marker and `PQL-S22` typed-histogram contracts | experimental | no | `CAT`, `RAW` | `API` | EXP |

`PQL-F14` is API-owned composition, not SQL or an extension primitive.
Enabled upstream sorts each requested label using natural order, treats a
missing label as an empty string, then uses the complete Prometheus label set
as a deterministic tie-break; descending reverses every comparison. Floats
and native histograms participate identically, while range results retain
fixed label order and report that sorting is ineffective. Portable
SQLite/libSQL exposes neither this natural comparator nor Prometheus's exact
label-set comparison. A future experimental Rust tier requires a
feature-enabled oracle, bounded vector state, string/variadic validation,
labels and metric names, histogram values, range warnings, limits, and
cancellation. The stable API fails before the child vector or storage scan.

`PQL-F21` is also API-owned composition rather than an ordinary SQL or
extension query vector. Enabled upstream accepts one instant vector and an
optional selector-only argument, defaults info sources to `target_info`, and
joins on the currently fixed identifying labels `job` and `instance`. It
merges only selected non-identifying labels, preserves conflicting base
labels, resolves info-series churn by newest sample timestamp, rejects
same-timestamp duplicates and cross-info-metric label conflicts, and either
drops or preserves an unenriched base sample according to whether every data
matcher accepts the empty string. Lookback, `offset`, `@`, exact stale-marker
termination, float-only info sources, and float/native-histogram base samples
are part of the contract. An application-specific public-raw JSON join can be
useful but is not equivalent under churn or staleness. Future execution needs
`PQL-S17`, `PQL-S22`, a feature-enabled oracle, bounded dual selection,
limits, cancellation, and duplicate-result enforcement. The stable API fails
before evaluating either vector or reading storage.

## Histogram functions

Classic Prometheus histograms are ordinary float series named `*_bucket` with
an `le` label; they fit the existing storage model. Native histograms do not.

| ID | function | Rust now | Elixir | foundation | target | priority | notes |
|---|---|---|---|---|---|---|---|
| `PQL-H01` | `histogram_quantile` over classic buckets ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-054-histogram_quantile-over-classic-buckets)) | shipped | yes | `RAW`, `SQL` | `API` | P0 | Scalar quantiles, classic-family grouping, strict bounds, equal-bound coalescing, precision tolerance, monotonic repair, interpolation, special values, range grids, output collisions, limits, cancellation, and reopen are pinned. |
| `PQL-H02` | `histogram_fraction` over classic buckets ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-056-histogram_fraction-over-classic-buckets)) | shipped | no | `RAW`, `SQL` | `API` | P2 | Pinned Prometheus classic grouping, exact/interpolated/infinite/inverted/NaN bounds, equal-bound coalescing, missing/zero totals, strict `le` warnings, range/subquery composition, name collisions, limits, cancellation, compact, and reopen are covered. Native histogram inputs remain deferred with their storage model. |
| `PQL-H03` | experimental `histogram_quantiles` | experimental | no | `RAW`, `SQL` | `API` | EXP | Prometheus 3.13.2 requires `promql-experimental-functions` and uses vector-first syntax. The stable endpoint pins the disabled diagnostic; VictoriaMetrics has different argument order and is tracked as `MQL-12`. |
| `PQL-H04` | `histogram_avg/count/sum/stddev/stdvar` on native samples | deferred | no | none | `DEFER` | DEFER | Requires `QSF-003` storage work. |

## MetricsQL compatibility tier

PromQL parity is the first gate. These VictoriaMetrics conveniences already
exist in `TimelessMetrics.PromQL` and are useful compatibility targets after
the P0 PromQL rows. They remain `API` composition, not extension syntax.

| ID | construct | Rust now | Elixir | target | priority |
|---|---|---|---|---|---|
| `MQL-01` | binary `default`, `if`, `ifnot` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-mql-001-default-if-and-ifnot)) | shipped | yes | `API` | P2 |
| `MQL-02` | `keep_metric_names` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-mql-002-keep_metric_names)) | shipped | yes | `API` | P2 |
| `MQL-03` | `union` and `alias` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-mql-003-union-and-alias)); named/shorthand/zero/single/multiple/trailing-comma union, first-labelset precedence, alias rename/removal, collision errors, scalar duplicates, nesting, limits, route isolation, and reopen are pinned | shipped | yes | `API` | P2 |
| `MQL-04` | `label_set` and `label_del` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-mql-004-label_set-and-label_del)); add/replace/delete/name/empty-value/duplicate-order behavior, scalar vectorization, case-insensitive transforms, collisions, limits, stable-route isolation, and reopen are pinned | shipped | yes | `API` | P2 |
| `MQL-05` | `default_rollup` and window-less rollups ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-mql-005-default_rollup-and-window-less-rollups)); implicit selectors, 0.6-quantile scrape inference, jitter inflation, `max_lookback`, stale/ordinary-NaN distinction, step windows, previous-sample/reset semantics, first/last/name policy, timestamp provenance, limits, stable-route isolation, and reopen are pinned | shipped | yes | `API` | P2 |
| `MQL-06` | `range_avg/min/max/sum` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-mql-006-range-aggregates)); whole-request-grid reduction/fill, slot-index average, ordinary sum, later-tie extrema, sparse/NaN/infinity behavior, unconditional name removal, scalar/expression composition, collisions, limits, cancellation, route isolation, and reopen are pinned | shipped | yes | `API` | P2 |
| `MQL-07` | `running_avg/min/max/sum` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-mql-007-running-aggregates)); cumulative grid output, slot-index average, ordinary sum, later-tie extrema, leading/missing/stale carry, computed-NaN omission, unconditional name removal, scalar/expression composition, collisions, limits, cancellation, route isolation, and reopen are pinned | shipped | yes | `API` | P2 |
| `MQL-08` | remaining MetricsQL functions/operators | missing | partial | `API` | DEFER |
| `MQL-09` | request-step-relative durations such as `[5i]`, `[5i:1i]`, and `offset 5i` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-mql-009-request-step-relative-durations)); decimal/compound/case behavior, inherited negative offsets, exact `int64` saturation through collision-free markers, comments/strings, adaptive direct/subquery `0i`, limits, stable-route isolation, exact-build evidence, and reopen are pinned | shipped | yes | `API` | P2 |
| `MQL-10` | query-context `start()`, `end()`, and `step()` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-mql-010-query-context-values)); request-owned range/instant values, subsecond/pre-epoch seconds, case-insensitive zero-argument grammar, expression composition, preserved selector `@ start()`/`@ end()`, stable-route isolation, explicit unsupported functions, limits, cancellation, exact-build evidence, and reopen are pinned | shipped | yes | `API` | P2 |
| `MQL-11` | `min_of` and `max_of` | deferred | no | `DEFER` | DEFER |
| `MQL-12` | VictoriaMetrics `histogram_quantiles("label", phi..., buckets)` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-mql-012-histogram_quantiles)); multi/time-varying ranks, first-rank label formatting, destination replacement/empty/name behavior, cumulative and `vmrange` buckets, missing `+Inf`, equal bounds, monotonic/NaN repair, computed-NaN omission, collisions, limits, cancellation, stable-route isolation, one bucket read, and reopen are pinned | shipped | no | `API` | P2 |

The MetricsQL tier uses only `/metricsql/api/v1/query` and
`/metricsql/api/v1/query_range`; the stable PromQL routes never opt into it
implicitly. Pinned VictoriaMetrics 1.148.0 rejects both `min_of` and `max_of`,
so `MQL-11` is deferred unless a later immutable upstream tier or an explicit
Timeless-only contract supplies a real semantic oracle.

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
