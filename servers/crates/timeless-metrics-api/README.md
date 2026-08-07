# timeless-metrics-api release server

This first-class signal server is not a new storage engine. The binary owns HTTP
scheduling and SQLite connections while
the existing `timeless_metrics` extension continues to own series identity,
the 4,096-point per-series buffer threshold, compression, chunks, rollups, and
retention commands.

## Implemented surface

- `GET /live`
- `GET /ready`
- `GET /health`
- `GET /select/metrics/stats`
- `POST /api/v1/flush`
- `POST /api/v1/import/prometheus`
- `POST /api/v1/import`
- `GET|POST /api/v1/query` (native `metric=` exact latest or `query=` PromQL)
- `GET /api/v1/export` (VictoriaMetrics JSON-line raw export)
- `GET|POST /api/v1/query_range` (native exact range aggregation or `query=` PromQL)
- `GET|POST /metricsql/api/v1/query` (explicit MetricsQL compatibility tier)
- `GET|POST /metricsql/api/v1/query_range` (explicit MetricsQL compatibility tier)
- `GET /api/v1/labels`
- `GET /api/v1/label/{name}/values`
- `GET /api/v1/series`
- Prometheus aliases for instant/range queries and label/series discovery

The current PromQL slice supports scalar literals (including `NaN` and
infinities), string literals, exact-name and nameless instant vector
selectors, Prometheus 3 quoted UTF-8 metric and label names, line comments,
anchored regex/negative/duplicate `__name__` matchers, root range selectors on
instant queries, and
`avg_over_time(selector[window])`, `min_over_time(selector[window])`, and
`max_over_time(selector[window])`, `sum_over_time(selector[window])`, and
`count_over_time(selector[window])`, `present_over_time(selector[window])`,
`quantile_over_time(scalar, selector[window])`, plus
`stddev_over_time(selector[window])`, `stdvar_over_time(selector[window])`, and
`last_over_time(selector[window])`, and float-counter
`rate(selector[window])`, `irate(selector[window])`, and
`increase(selector[window])`, plus float-gauge `delta(selector[window])` and
`idelta(selector[window])`, and timestamp-centered least-squares
`deriv(selector[window])`, `predict_linear(selector[window], horizon)`, and
ordered float-transition `changes(selector[window])` and strict float-counter
decrease `resets(selector[window])`.
Pinned Prometheus 3.13.2 marks `double_exponential_smoothing` experimental and
disables it by default; the stable Timeless tier rejects it explicitly until
a separately enabled experimental compatibility tier and oracle gate exist.
Pinned Prometheus likewise feature-gates the `limitk` and `limit_ratio`
aggregators. Stable PromQL requests reject both before any storage read; they
are not approximated with SQL ordering or silently executed through another
runtime. Their matrix row remains experimental until an explicitly enabled
Rust API tier passes a feature-enabled oracle with bounded group state.
Prometheus separately feature-gates binary `fill`, `fill_left`, and
`fill_right`. Stable requests preserve its disabled diagnostic before storage;
they do not execute the recognized parser nodes or cross to another runtime.
The direct-SQL documentation provides bounded one-to-one float composition,
while full PromQL behavior remains an explicitly configured future
experimental tier.
Prometheus's stable native-histogram trim operators `</` and `>/` are a
separate typed-data-model deferral. The server reports `requires typed
native-histogram storage` at the operator position before reading storage;
classic `_bucket` float series are not treated as native histogram samples,
and no SQL, Elixir, or process fallback is used.
Pinned Prometheus also marks `mad_over_time` experimental. Stable requests
preserve its disabled-function diagnostic before storage. Direct users have a
documented finite-float two-median SQL recipe, while PromQL execution remains
deferred to a separately configured experimental Rust tier and oracle.
The same stable gate applies to `ts_of_first_over_time`,
`ts_of_last_over_time`, `ts_of_min_over_time`, and `ts_of_max_over_time`.
Direct SQL documentation covers their bounded finite-float timestamp subset;
the server does not silently enable, approximate, or delegate the functions.
`sort_by_label` and `sort_by_label_desc` are also experimental and fail with
their pinned disabled diagnostics. No lexicographic SQL approximation is
advertised for Prometheus's natural multi-label order; future execution
belongs in a separately enabled Rust API tier.
The bounded instant-vector transforms `abs`, `ceil`, `floor`, `round`,
`clamp`, `clamp_min`, `clamp_max`, `sqrt`, `exp`, `ln`, `log2`, `log10`, and
`sgn`, plus `acos`, `acosh`, `asin`, `asinh`, `atan`, and `atanh`
and `cos`, `cosh`, `sin`, `sinh`, `tan`, and `tanh`. Vector `deg`/`rad` and
scalar `pi()` retain their distinct PromQL result types. These numeric vector
transforms preserve all float sample classes while removing the metric name,
including in range queries and nested expressions. `round` accepts Prometheus's optional
scalar nearest-multiple expression and exact upward-tie behavior. Clamp bounds
are scalar expressions evaluated per step; inverted bounds omit every sample.
`label_replace(vector, destination, replacement, source, regex)` preserves
values, timestamps, and metric names while applying Prometheus's full-match
dot-all regex, numbered or named capture expansion, missing-as-empty source
rule, and empty-result destination deletion. Prometheus 3's nonempty UTF-8
label-name scheme is retained; malformed regexes and an empty destination fail
as execution errors. Capture expansion is charged incrementally to the
response byte limit.
`label_join(vector, destination, separator, sources...)` reads every source
from the original label set in argument order, treats missing sources as
empty, preserves duplicate sources, and accepts zero sources. An empty joined
value deletes the destination; `__name__` works as a source or destination.
Values and timestamps are unchanged, and an empty destination fails with an
execution envelope. Joined destination bytes are bounded before allocation can
amplify across a result set.
`absent(vector)` returns one at each evaluation step with no input sample and
is empty otherwise; range results are sparse. A direct selector derives only
its unique, nonempty equality labels, excluding `__name__`; composed inputs
derive no labels. NaN counts as present, and every inspected grid step is
charged to the cumulative work limit.
`absent_over_time(range-vector)` applies the same sparse output and label rules
to each exact open-left, closed-right sample window. It supports direct range
selectors and subqueries, counts every IEEE sample as present, and charges the
bounded range evaluation plus every inspected outer step to the cumulative
work limit.
`sort(vector)` and `sort_desc(vector)` value-order instant results while
placing NaN last, preserving labels, names, timestamps, and float bits, and
using labels only to make equal-value groups deterministic. Range-query
matrices remain label ordered, matching Prometheus rather than applying one
step's value order to complete series.
`scalar(vector)` emits the sole sample value at each step and NaN for zero or
multiple samples; `vector(scalar)` emits one nameless series. Both preserve
their exact Prometheus instant/range result types and compose without a second
storage read.
`time()` follows the millisecond evaluation grid in Unix seconds.
`timestamp(direct-selector)` exposes selected stored-sample provenance through
lookback, `offset`, and `@`; composed expressions report their newly created
evaluation timestamps. Response timestamps stay on the outer grid, and
`timestamp` removes only the metric name.
`minute`, `hour`, `day_of_week`, and `day_of_month` extract UTC calendar
components from an optional instant vector, defaulting to `vector(time())`.
They remove metric names, preserve other labels, truncate in-range finite
fractional seconds toward zero, and retain Prometheus's pinned non-finite and
out-of-range conversion behavior. The executable direct-SQL foundation is
[`SQL-PROM-052`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-prom-052-minute-hour-day_of_week-and-day_of_month).
`day_of_year`, `days_in_month`, `month`, and `year` share that optional-vector
UTC contract, with one-indexed fields and Gregorian leap-year handling. Their
executable direct-SQL foundation is
[`SQL-PROM-053`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-prom-053-day_of_year-days_in_month-month-and-year).
`histogram_quantile(scalar, vector)` evaluates classic float `*_bucket`
families with exact Prometheus bound coalescing, monotonicity correction,
`1e-12` small-delta suppression, boundary interpolation, invalid-quantile
values, and label/name policy. It composes with aggregation and counter
functions, is bounded by the cumulative work limit, and does not claim native
histogram support. Missing or malformed `le` series are excluded. Exact
warning/info annotations cover malformed bounds, invalid quantiles, and
material monotonicity repair. Direct SQLite/libSQL users have the executable ordinary-SQL
foundation in
[`SQL-PROM-054`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-prom-054-histogram_quantile-over-classic-buckets).
`histogram_fraction(lower, upper, vector)` evaluates classic cumulative
float buckets with pinned Prometheus grouping, bound coalescing, finite and
infinite interpolation, zero/missing-total, inverted-bound, range/subquery,
label/name, warning, work-limit, and cancellation semantics. Unlike
`histogram_quantile`, it deliberately does not repair non-monotonic buckets.
Native histograms remain deferred. The executable direct-SQL foundation is
[`SQL-PROM-056`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-prom-056-histogram_fraction-over-classic-buckets).
Pinned Prometheus feature-gates the query-context `start`, `end`, `step`, and
`range` functions, `min_of`, `max_of`, and `histogram_quantiles`; it does not
define `start_timestamp`. The stable endpoint preserves those exact failures,
while selector modifiers `@ start()` and `@ end()` remain supported. Their
MetricsQL forms are tracked separately rather than silently broadening the
PromQL endpoint.
The explicitly named MetricsQL routes currently add `default`, `if`, `ifnot`,
and the operation-level `keep_metric_names` modifier. The conditional
operators preserve the left series identity, operate step by step, accept
`on(...)` and `ignoring(...)`, and treat an RHS scalar as a nameless vector
broadcast. A `default` can therefore fill gaps left by a filtered comparison,
including a series whose comparison produced no values. MetricsQL scalar
instant results are nameless vectors, matching VictoriaMetrics rather than
the PromQL scalar envelope. Join modifiers accepted by VictoriaMetrics on
these set-style operators do not rewrite the contributing left labels.

`keep_metric_names` follows a supported transform, rollup, or binary
operation. It preserves each input name during the operation, so multi-name
transforms do not collapse into duplicate nameless label sets. Default binary
matching becomes metric-name-aware; explicit `on(...)` still selects only its
listed labels and preserves the left name in the result. Bare selectors,
unary expressions, aggregations, and repeated modifiers fail explicitly.
Nested modified operations remain valid inputs to an aggregate.

`union(q1, ..., qN)` and its `(q1, ..., qN)` shorthand compose bounded child
plans. Zero arguments return an empty vector, a single argument is an
identity, and trailing commas are accepted. If vector arguments produce the
same complete labelset, the earliest union argument wins as a complete series;
samples are not merged. `alias(q, "name")` replaces the metric name and an
empty alias removes it. A bare alias that creates duplicate outputs and a
union of duplicate scalar labelsets fail explicitly, matching pinned
VictoriaMetrics. Both operations nest under ordinary expressions while the
stable PromQL routes reject their syntax.
The `union` function name is case-insensitive; VictoriaMetrics defines
`alias` as a lowercase built-in template, so uppercase `ALIAS` is rejected.

`label_set(q, "label", "value", ...)` and `label_del(q, "label", ...)`
transform complete output identities in argument order. Empty set values
delete labels, the last repeated set destination wins, missing deletions are
no-ops, and `__name__` addresses the metric name. Scalars become nameless
vectors before transformation. Both functions are case-insensitive, accept
trailing commas and identity forms with no label arguments, compose under
ordinary expressions, and reject duplicate output labelsets. Generated label
bytes and intermediate work remain bounded by the existing response/work
limits.

Bare selectors on the MetricsQL routes now apply VictoriaMetrics
`default_rollup` implicitly. Range queries estimate each series' scrape
interval from the interpolated 0.6 quantile of its last 20 intervals, inflate
that interval for jitter, and use the larger of it and the request step. A
positive `max_lookback` caps the automatic default window; an explicitly
written range is not shortened. The exact Prometheus stale-NaN marker ends the
visible series, while an ordinary stored NaN remains a bit-preserved Timeless
sample instead of being discarded at ingestion as it is by VictoriaMetrics.

The same routes accept selector arguments without brackets for
`avg_over_time`, `min_over_time`, `max_over_time`, `sum_over_time`,
`count_over_time`, `present_over_time`, `stddev_over_time`,
`stdvar_over_time`, `first_over_time`, `last_over_time`, `rate`, `irate`,
`increase`, `delta`, `idelta`, `deriv`, `changes`, and `resets`. Ordinary
statistical windows equal the request step. `default_rollup`, `rate`, `irate`,
`deriv`, and selector-backed `timestamp` use their pinned adjustable-window
rules. Counter reset correction and the bounded previous sample are applied
where VictoriaMetrics requires them. Average, minimum, maximum, first, last,
and default rollup retain the source metric name; the other rollups and
`timestamp` drop it. `first_over_time` remains unavailable on the stable
PromQL routes because the pinned Prometheus tier feature-gates it.

`range_avg`, `range_min`, `range_max`, and `range_sum` reduce each series over
the complete request evaluation grid and repeat the final value at every grid
timestamp. Average uses VictoriaMetrics's slot-indexed incremental arithmetic,
sum uses ordinary binary64 addition, and extrema choose the later equal
operand. Leading NaN/missing slots are skipped, later gaps carry, and the last
non-NaN running value fills the whole grid. Every function removes the metric
name even under `keep_metric_names`; post-removal duplicates fail explicitly.
Timeless retains exact signed-zero bits where VictoriaMetrics's Remote Write
path normalizes them. Scalar/expression composition, limits, cancellation,
stable-route isolation, GET/POST, and shutdown/reopen are pinned through the
real extension with one child storage evaluation.

`running_avg`, `running_min`, `running_max`, and `running_sum` use the same
complete request grid but emit the cumulative state at every step. After the
first value, missing or stale steps emit the prior state while still advancing
the average's slot index. A newly computed NaN is omitted rather than replaced
with the previous finite/infinite value. Average uses incremental arithmetic,
sum uses ordinary binary64 addition, equal extrema select the later operand,
and every function removes the metric name even under `keep_metric_names`.
Leading gaps, overflow, infinity, signed zero, collisions, limits,
cancellation, GET/POST, and reopen are pinned against VictoriaMetrics through
one public child evaluation.

Request-step-relative MetricsQL durations resolve each `i` component against
the current request step. Direct ranges, subquery windows and resolutions,
and negative offsets accept decimal, compound, and case-insensitive forms;
compound leading minus signs are inherited exactly as in VictoriaMetrics.
The complete binary64 duration is truncated to milliseconds and saturated to
`int64`. Quoted strings and comments are isolated, including comments between
a delimiter and duration. Bare `i` fails explicitly, while the stable PromQL
routes retain the pinned rejection of all `i` syntax.

For ordinary reductions, `0i` resolves to one request step. For adaptive
`default_rollup`, `rate`, `irate`, and `deriv`, direct and subquery `0i`
retains the automatic cadence-inference behavior rather than becoming a fixed
window. Collision-checked lowering prevents a legitimate explicit duration
from being confused with this zero marker. Limits, cancellation, one-public-
read behavior, GET/POST, durability, and reopen use the existing query
contract.

The MetricsQL routes also expose case-insensitive, zero-argument `start()`,
`end()`, and `step()` request-context functions. Range start/end and instant
evaluation time are returned as floating-point Unix seconds; `step()` retains
subsecond request precision. The values compose with scalars and vectors, and
pure context expressions perform no extension read. Pinned VictoriaMetrics
rejects `start_timestamp()` and `range()`, so Timeless rejects them explicitly
too. Existing direct selector `@ start()`/`@ end()` modifiers and the stable
PromQL feature gates remain unchanged.

VictoriaMetrics `histogram_quantiles("label", phi..., buckets)` is available
only on these MetricsQL routes. It evaluates every scalar rank over one shared
bucket result, formats the destination label from the rank's first value,
accepts cumulative `le` and non-cumulative `vmrange` buckets, and applies the
pinned missing-`+Inf`, monotonic-repair, interpolation, computed-NaN omission,
name-removal, and duplicate-output rules. Timeless retains exact stored NaN
bucket bits that VictoriaMetrics Remote Write omits. Work/result/response
limits and cancellation bound both bucket conversion and every rank.

MetricsQL never enters the SQLite extension as language syntax. The Rust API
parses and composes it over the same public bounded grid used by PromQL; direct
SQLite/libSQL users can execute the corresponding
[`SQL-MQL-001`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-mql-001-default-if-and-ifnot)
and
[`SQL-MQL-002`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-mql-002-keep_metric_names)
and
[`SQL-MQL-003`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-mql-003-union-and-alias)
and
[`SQL-MQL-004`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-mql-004-label_set-and-label_del)
and
[`SQL-MQL-005`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-mql-005-default_rollup-and-window-less-rollups)
and
[`SQL-MQL-006`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-mql-006-range-aggregates)
and
[`SQL-MQL-007`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-mql-007-running-aggregates)
and
[`SQL-MQL-009`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-mql-009-request-step-relative-durations)
and
[`SQL-MQL-010`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-mql-010-query-context-values)
and
[`SQL-MQL-012`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-mql-012-histogram_quantiles)
recipes. Invalid MetricsQL uses Timeless's stable HTTP 400 `bad_data` JSON
envelope; the pinned VictoriaMetrics oracle uses HTTP 422 with error type
`422`. Limits and cancellation continue to use Timeless's existing execution
envelope. The default PromQL routes reject MetricsQL-only operators instead of
silently changing languages.
Unary minus and arithmetic `+ - * / % ^` plus trigonometric binary `atan2`
compose over shipped scalar and
instant-vector expressions, removes the vector metric name, and preserves
Prometheus scalar/vector/range result types. Vector/vector arithmetic uses
default one-to-one matching across every non-name label, filters unmatched
samples, and rejects duplicate match signatures. All six comparisons support
Prometheus filter and `bool` behavior: filters retain original vector values
and names, while `bool` emits `0`/`1` and removes names. Set `and`, `or`, and
`unless` use step-local many-to-many matching and preserve the contributing
sample's value, labels, and metric name. Vector/vector operators accept
`on(...)` and `ignoring(...)`, including empty lists and missing-label
matching, with Prometheus output-label and duplicate-cardinality rules.
`atan2` uses deterministic Go-compatible last-bit rounding; its direct SQLite
foundation is
[`SQL-PROM-055`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-prom-055-atan2).
Many-to-one `group_left` and one-to-many `group_right` preserve operation
direction, copy labels from the unique side, and reject both duplicate unique
sides and non-unique result labelsets.
Cross-series `sum`, compensated `avg`, `min`, and `max` support unmodified, `by(...)`, and `without(...)`
grouping over every shipped instant-vector expression, including exact empty,
missing-label, metric-name, sparse-range, and IEEE behavior.
Selectors and range selectors accept signed
`offset` plus numeric/`start()`/`end()` `@` modifiers. Selector and shipped
range-function expressions may be evaluated as bounded subqueries with
`[range:resolution]`, including omitted/default resolution, nesting, and
subquery timing modifiers. Root subqueries return range vectors only from an
instant query; `avg_over_time` consumes them as instant vectors. It
deliberately rejects every other
function, operator, or aggregation not listed in the matrix with a
Prometheus `bad_data` response. There is no hidden Elixir fallback. The
release binary requires Phoenix-managed policy authentication by default;
cluster and product control routes remain in Phoenix. The
Prometheus route keeps the request as a reference-counted body and passes the
complete exposition through the extension's public ingest surface; Rust does
not parse or copy it at the API boundary. The VictoriaMetrics route parses the
JSON-line body once, interns its series within the request, and encodes one
public named-columnar `0x01` batch. Neither path issues per-point SQL or flushes
at the request boundary.

The authoritative support contract is the
[PromQL feature matrix](../../../docs/PROMQL_FEATURE_MATRIX.md). The shipped
Rust API rows at this revision are listed below for the local documentation
contract checker; prose in this README must not imply a broader language
surface.

<!-- query-contract-shipped: PQL-S01 PQL-S02 PQL-S03 PQL-S04 PQL-S05 PQL-S06 PQL-S07 PQL-S08 PQL-S09 PQL-S11 PQL-S12 PQL-S13 PQL-S14 PQL-S15 PQL-S16 PQL-S18 PQL-S19 PQL-S20 PQL-S21 PQL-S23 PQL-O01 PQL-O02 PQL-O03 PQL-O04 PQL-O05 PQL-O06 PQL-O07 PQL-O08 PQL-O09 PQL-O10 PQL-O11 PQL-O12 PQL-O13 PQL-O14 PQL-O15 PQL-O16 PQL-R01 PQL-R02 PQL-R03 PQL-R04 PQL-R05 PQL-R06 PQL-R08 PQL-R09 PQL-R10 PQL-R11 PQL-R12 PQL-R13 PQL-R14 PQL-R15 PQL-R16 PQL-R17 PQL-R18 PQL-R19 PQL-R20 PQL-F01 PQL-F02 PQL-F03 PQL-F04 PQL-F05 PQL-F06 PQL-F07 PQL-F08 PQL-F09 PQL-F10 PQL-F11 PQL-F12 PQL-F13 PQL-F15 PQL-F16 PQL-F17 PQL-F18 PQL-H01 PQL-H02 MQL-01 MQL-02 MQL-03 MQL-04 MQL-05 MQL-06 MQL-07 MQL-09 MQL-10 MQL-12 -->

Both routes preserve the existing asynchronous empty `204` admission contract.
Valid lines in a partially malformed body are persisted and rejected lines are
counted. A 10 MiB body limit is enforced before admission; an oversized body
returns `413` and does not affect queue or point watermarks.

The process starts one ordered SQLite writer and a configurable reader pool.
It creates the same rollup ladder as `TimelessMetrics.LibsqlEngine`, schedules
the same 10-second flush, five-minute compact/rollup, and hourly seven-day raw
retention prune, and sends only public extension commands. Graceful shutdown
places an ordered `flush` behind all admitted writes.

`POST /api/v1/flush` reports the admitted batch and point watermark covered by
the command together with completed, failed, queued, and in-flight work. It is
a real completion/durability barrier, not queue admission.

Mechanical reads execute on the existing SQLite reader pool. The API discovers
`timeless_latest_frame`, `timeless_raw_frame`, and
`timeless_window_batches` through `pragma_module_list`; it never infers a
capability from an extension version. Bounded PromQL reads additionally
require the explicit `query_surfaces.*.max_work_points` capability; a module
with the same name but without that additive contract fails closed. Older
extensions retain row-oriented
`timeless_latest` and `timeless_raw` fallbacks. Current `TLF1`, `TRF1`, and
`TWB1` results are length/bitmap/version validated. Raw and window response
encoders borrow column offsets from the one returned blob and write final JSON
directly rather than allocating a second timestamp/value object graph.

Native range uses packed extension windows for complete avg/sum/min/max/count
shapes. Partial grids plus first/last/rate use the packed raw frame and the
established host aggregation semantics. Prometheus discovery accepts repeated
`match[]`/`match` selectors as a union, preserves duplicate matcher AND
semantics, fully anchors regexes, and treats a missing label as the empty
string. Native exact routes keep their Session 0 response envelopes and
inclusive timestamp bounds.

PromQL parsing is storage-independent and uses the exactly locked
`promql-parser` 0.10.0 AST frontend. That parser's own historical compatibility
claim is not a Timeless compatibility claim: each AST node remains gated until
its matrix row passes the pinned Prometheus 3.13.2 oracle. Complete upstream
duration literals are accepted as scalar values, selector windows, and range
query steps, including compound forms and millisecond components. PromQL's
request/evaluation clock is millisecond-precise: numeric and RFC3339 request
times retain milliseconds and response timestamps use exact fractional
seconds.

String literals use the PromQL parser's double-quoted escape rules and raw
backtick form. Instant queries return Prometheus's `string` result type with
the evaluation timestamp; range queries reject strings because upstream range
evaluation permits only scalar and instant-vector roots. Strings remain
API-owned values and do not read or mutate extension storage.

Metric storage remains intentionally second-native. Stored samples therefore
remain aligned to whole seconds even when a subsecond evaluation grid or range
boundary is requested. Whole-second `avg_over_time` grids use the packed
extension window path; any subsecond component uses the public packed raw path
and applies exact `(T-window,T]` boundaries in Rust. This preserves the public
storage format and Prometheus semantics without presenting a second-native
extension kernel as millisecond-aware.

Every `avg_over_time` path uses the same compensated average and
overflow-safe incremental-mean fallback as the pinned Prometheus oracle.
Every `sum_over_time` path uses compensated addition while retaining IEEE
overflow and NaN behavior.
Every `count_over_time` path includes all stored float samples, including
non-finite values. Every `present_over_time` path maps a non-empty float
window to `1`, including windows containing only non-finite values, and omits
empty windows.
Every `quantile_over_time` path evaluates its scalar expression on the outer
grid, ranks raw NaN low, retains signed-zero input order, and uses Prometheus
linear interpolation; fixed nearest-rank extension `pXX` kernels are not
misrepresented as this function.
Every `stddev_over_time` and `stdvar_over_time` path uses population Welford
state over exact raw samples, including singleton, wide-magnitude, and IEEE
behavior. Vector and matrix samples use the pinned Prometheus JSON codec's
fixed-versus-scientific threshold, exponent form, signed zero, and non-finite
strings; scalar results retain Prometheus's distinct fixed-format JSON contract.
Every `last_over_time` path preserves the selected sample's IEEE bits and,
unlike the neighboring range reductions, retains the input metric name.
Every `rate` path requires two distinct-timestamp float samples, corrects
counter resets, applies Prometheus's 1.1-times-average edge extrapolation and
left zero-point clamp, normalizes per second, and removes the metric name.
The extension's similarly named mechanical window kernel is not PromQL and is
not used by this API plan.
Every `irate` path uses only the final two samples in the exact window,
substitutes the final value after a reset, divides by their actual interval,
and omits singleton or zero-interval final pairs. It removes the metric name
and does not extrapolate range edges.
Every `increase` path shares `rate`'s reset, edge-extrapolation, and zero-point
semantics but returns the estimated range increase without per-second
normalization. The extension's mechanical `increase` kernel is not used by
this API plan.
Every `delta` path extrapolates the first-to-last gauge difference to the
range edges without reset correction or a zero-point clamp, so decreases stay
negative. The extension's mechanical `delta` kernel is not used by this API
plan.
Every `idelta` path subtracts only the final two gauge samples, preserves
negative changes, omits a singleton or zero-interval final pair, and performs
neither edge extrapolation nor time normalization.
Every `deriv` path returns the per-second least-squares slope of all float
samples in the range. It centers timestamps on the first sample and uses the
same compensated sums as Prometheus, returns zero for a constant finite
series, returns `NaN` for a constant infinity or IEEE-indeterminate
regression, and omits a singleton.
Every `predict_linear` path uses the same regression but centers its intercept
on the outer evaluation timestamp and evaluates the line at the scalar horizon
in seconds. Selector `offset` and `@` affect sample selection without moving
that forecast origin. The horizon may be any shipped scalar expression and is
evaluated separately at every outer step; NaN and infinities remain values.
Every `changes` path counts adjacent float transitions once in sample order.
Repeated NaNs and either signed-zero order are unchanged; NaN-to-number,
number-to-NaN, finite changes, and distinct infinities count normally. A
singleton returns zero, while an empty window emits no series.
Every `resets` path counts only adjacent strict decreases. NaN comparisons,
equal values, and signed-zero transitions are not resets; `+Inf` to finite and
finite to `-Inf` are. A singleton returns zero and an empty window emits no
series.
Every `min_over_time` and `max_over_time` path uses ordered-comparison extrema,
including first-sample signed-zero stability and Prometheus-compatible NaN handling.
The numeric folds and count include packed whole-second windows plus raw
modifier/subsecond fallbacks; `last_over_time` uses the existing packed raw
waist. All support materialized subqueries, and valid `NaN` and infinities
remain values rather than frame errors. All except `last_over_time` remove
`__name__`; every path omits empty windows, enforces cumulative
work/output/deadline limits, and remains cancellation-aware.

Both query endpoints accept Prometheus's request-scoped `lookback_delta`.
Omission and an explicit zero select the five-minute default; positive numeric
seconds or compound duration syntax replace it. Selector samples use the exact
open-left `(T-lookback,T]` boundary at every evaluation point. Range grids
start exactly at `start`, advance by `step`, and include only points at or
before `end`; a non-aligned `end` does not create a shortened final step.

Prometheus-compatible requests return either `{"status":"success","data":…}`
or the three-field `{"status":"error","errorType":…,"error":…}` envelope.
Malformed query/time/range/step/lookback parameters use HTTP `400` and
`bad_data`; execution failures use HTTP `422` and `execution`. The canonical
`/prometheus/api/v1/*` routes require all Prometheus range parameters.
`/api/v1/query` and `/api/v1/query_range` use the same behavior whenever a
`query` parameter is present; an omitted `query` on those unprefixed routes
continues to select the documented native Timeless API for compatibility.

Timeless deliberately rejects unknown parameters instead of silently ignoring
them, rejects non-finite evaluation timestamps, and caps output at 11,000
points rather than Prometheus 3.13.2's 11,000 intervals (11,001 points).
Parser-specific diagnostic wording can differ after the common
parameter/type prefix because the server uses the pinned Rust AST parser;
status, error type, parameter ownership, and unsupported behavior are stable.
Successful responses include Prometheus's optional top-level `warnings` and
`infos` only when applicable. Exact text and source position are pinned for
invalid quantiles, ineffective range sorting, non-counter metric-name lint,
malformed classic-histogram bounds, and material monotonicity repair.
Messages are deterministically deduplicated, capped at ten per severity plus
an omission summary, and charged to the response byte limit. Empty or
unaffected results omit the fields.

PromQL remains bounded even with authentication disabled. Defaults are 11,000
grid points per series, 100,000 final result points, 100,000 storage work
or subquery intermediate points, a 16 MiB serialized response, a 15-second
omitted subquery resolution, and a 30-second deadline. The packed raw
and window limits are pushed into the extension before payload reads; result
and response limits are checked during serialization; cancellation is checked
inside catalog, evaluator, and raw-fold loops. Nameless catalog expansion
streams at most the storage-work series limit and retains at most the response
byte limit in selected metric/label metadata. A deadline returns HTTP `504` with
`errorType="timeout"`; other execution-limit failures return HTTP `422` with
`errorType="execution"`. Auth claims may lower their own row, byte, and time
allowances but cannot raise these owner limits.

The query layer catalog-prunes nameless selectors through `timeless_series`,
applies every regex/negative name matcher before payload access, then lowers
each selected metric name to a bounded `timeless_raw_frame` call
and performs a linear last-sample sweep over the exact
five-minute `(T-lookback,T]` window. Whole-second `avg_over_time`,
`min_over_time`, `max_over_time`, `sum_over_time`, `count_over_time`, and
`present_over_time` lower to
`timeless_window_batches`; its raw fallback preserves `(T-window,T]`, grid
timestamps, and Prometheus metric-name removal. `quantile_over_time` always
uses the bounded packed raw path because its interpolation and IEEE semantics
do not match the nearest-rank storage kernel. `stddev_over_time` and
`stdvar_over_time` also use the packed raw path and apply their population fold
in Rust. `rate`, `irate`, `increase`, `delta`, `idelta`, `deriv`, and
`predict_linear`, `changes`, and `resets` use the same bounded packed raw path
so the Rust evaluator can apply their distinct counter/gauge semantics. A root
range selector reads public raw frames,
applies the same open-left boundary, and returns a matrix only from an instant
query; range-query use fails as `bad_data`. The Rust process writes the final vector/matrix response without
BEAM/NIF or per-series transport. Duplicate matcher AND semantics and the
11,000-point resolution limit
match the existing service contract.

Temporal modifiers resolve `@` before subtracting signed `offset`. They shift
lookup/lookback/window time but never the outer selector/function output grid;
root range vectors continue to expose the selected raw sample timestamps.
Modified window functions deliberately use the bounded public raw path because
the extension's window timestamps are lookup timestamps, not PromQL's outer
evaluation timestamps.

Subquery grids are globally aligned and open on the left. An omitted
resolution uses the configurable 15-second global evaluation interval. The
planner evaluates a shipped instant-vector inner plan once over the union of
needed inner timestamps. Root subqueries can stream that bounded matrix
directly; a consuming range function decodes it into a bounded Rust matrix and
folds windows while checking cancellation. This composition leaves the
ordinary selector/window streaming paths and every extension/storage format
unchanged. Stats report final points and
`api_promql_intermediate_points` separately.

Unary and other composed AST nodes use the same bounded internal value bridge
as subquery consumers. Every child result point is charged to
`max_work_points`; response bytes, final points, deadline, and cancellation
remain independently enforced. Ordinary selectors and packed windows retain
their streaming hot paths.

Numeric transforms use that bridge over already bounded vector frames. They
perform no additional storage read, check cancellation while visiting series
and samples, remove `__name__`, and transform values in place. `round`
evaluates an optional scalar plan over the outer grid and charges those scalar
points to the same cumulative work limit. Clamp functions evaluate one or two
scalar plans the same way and omit empty series after inverted-bound filtering.
Direct SQLite/libSQL users can use
the executable `abs`, `ceil`, `floor`, `round`, `sgn`, clamp, square-root,
exponential, logarithm, and inverse-math recipes over
`timeless_grid`; the packed Rust path is required only to preserve stored NaN
bits and the complete PromQL response contract.

Every read carries a cancellation token. A dropped HTTP future stops host grid
evaluation between points and installs a scoped SQLite progress handler; the
handler is cleared before that reader accepts its next request. Stats expose
PromQL requests plus current/cancelled API reads.

## Run

```bash
cargo build -p timeless-ext --release
cargo build --manifest-path servers/Cargo.toml --release

TIMELESS_AUTH_MODE=disabled servers/target/release/timeless-metrics-api \
  target/release/libtimeless_ext.so \
  /tmp/timeless-metrics-api.db \
  127.0.0.1:19439
```

`TIMELESS_AUTH_MODE=disabled` is only for an isolated local benchmark. A
release omits it and supplies `TIMELESS_AUTH_POLICY_FILE` and
`TIMELESS_TENANT` through the Phoenix supervisor.

The measured default is two readers. The Session 6 1/2/4/8 sweep found that
four readers improved saturated query p95 by only 1.06ms while adding 10.7MiB
of peak RSS, and eight readers regressed. Two readers retained the best
throughput, tail-latency, and memory balance; the environment override remains
available for query-heavy deployments.

## Elixir control-plane integration

Release Session 5 freezes this binary's data/query surface and proves the
default process boundary from `timeless_ui`. `TimelessUI.MetricsDataPlane.Process`
supervises the executable through an Erlang port, and its thin Req client owns
no telemetry connection. Canvas routes historical graph ranges through
`GET /api/v1/export`; every product-oriented Canvas callback stays in Elixir.

The integration gate rejects a second owner, flushes data, kills this process
with `SIGKILL`, waits for OTP to restart it, and checks the exact same result
after reopen. Incomplete content-length and invalid NDJSON responses are one
client error and never a partial graph. Normal OTP shutdown sends `SIGTERM`;
the server handles both `SIGINT` and `SIGTERM` through the same graceful drain
and storage-flush path, the port owner waits for the child to be reaped, and an
admitted unflushed tail is recovered on the next reopen.
Configuration and the reproducible boundary benchmark are documented in the
sibling `timeless_ui` README and `bench/metrics_data_plane_boundary.exs`.

Positive environment overrides:

- `TIMELESS_METRICS_READER_CONNECTIONS` (default `2`)
- `TIMELESS_METRICS_COMMAND_QUEUE_BATCHES` (default `256`)
- `TIMELESS_METRICS_FLUSH_INTERVAL_SECS` (default `10`)
- `TIMELESS_METRICS_COMPACT_INTERVAL_SECS` (default `300`)
- `TIMELESS_METRICS_RETENTION_INTERVAL_SECS` (default `3600`)
- `TIMELESS_METRICS_PROMQL_MAX_POINTS_PER_SERIES` (default `11000`, maximum `11000`)
- `TIMELESS_METRICS_PROMQL_MAX_RESULT_POINTS` (default `100000`)
- `TIMELESS_METRICS_PROMQL_MAX_WORK_POINTS` (default `100000`)
- `TIMELESS_METRICS_PROMQL_MAX_RESPONSE_BYTES` (default `16777216`)
- `TIMELESS_METRICS_PROMQL_DEFAULT_SUBQUERY_STEP_MS` (default `15000`)
- `TIMELESS_METRICS_PROMQL_DEADLINE_MS` (default `30000`)

## Ownership and accounting

The server takes an advisory exclusive lock on
`<database>.timeless-metrics-api.lock`. A second server using this contract is
rejected before opening SQLite. The lease cannot stop an unrelated process
that ignores it, so deployments must still route all runtime access through
the API owner.

Stats separate units instead of conflating them:

- series, raw-chunk, and rollup index entries are row counts;
- `sqlite_index_bytes` is SQLite page allocation for the catalog/chunk indexes;
- logical compressed payload, database/WAL/SHM bytes, SQLite page high-water,
  and freelist bytes are distinct fields;
- admitted/completed/failed batches and points, queued and in-flight work, and
  oldest queue age are distinct; unknown point counts remain explicit while a
  Prometheus request waits for the extension to parse it;
- admitted/completed/queued/in-flight body bytes and per-format request counts
  expose the bounded-memory shape;
- API admission/queue/SQLite/stats/flush timers and maintenance timers are
  cumulative nanoseconds. VictoriaMetrics parse and batch-encode time is owned
  by the API counters; Prometheus parse/point/error counters and time are owned
  by `timeless_stats('metrics')`; its timer covers fused parse, resolve, and
  buffer work, so direct SQLite/libSQL users receive the same observability.
- mechanical/PromQL read request counts by shape, current/cancelled reads,
  total socket-to-result time, errors,
  packed frame bytes, response bytes, returned series, and returned points are
  cumulative and do not require `timeless_stats` work on the query hot path;
- `api_promql_intermediate_points` counts materialized evaluator points that
  fed a parent subquery/range-function node and is distinct from returned
  result points;
- extension `raw_batch_query_*` and `window_batch_query_*` counters attribute
  selected series, candidate chunks, payload bytes, fully decoded/buffered
  points, returned raw/grid points, and engine time for the same packed read
  waists used by direct SQLite clients.

## Validation

```bash
cargo test --manifest-path servers/Cargo.toml -p timeless-metrics-api

TIMELESS_EXT_PATH="$PWD/target/release/libtimeless_ext.so" \
  cargo test --manifest-path servers/Cargo.toml -p timeless-metrics-api \
  --test storage_contract -- --ignored
```

The Session 1 extension-backed test proves 4,095 points remain buffered, point
4,096 automatically becomes one durable raw chunk, an explicit flush persists
a smaller tail, the ordered counters drain to zero, a second owner is rejected,
and all 4,106 points recover after shutdown/reopen.

The Session 2 contract submits both native formats with partially malformed
bodies plus all-malformed requests. A fifth Prometheus request beginning with a
reserved batch-version byte proves the HTTP route cannot switch the extension's
hidden-column protocol. Five admitted/completed requests persist exactly four
valid points across two series, report eight rejected inputs, drain to zero
through the ordered flush, and recover exactly after reopen. It also proves the
10 MiB rejection occurs before admission and pins body-byte, API, and extension
phase counters.

The Session 3 contract adds exact latest/export/range bodies, a forced
partial-grid raw fallback, native/discovery response ordering, repeated and
malformed selectors, missing-label equality/inequality behavior, explicit
PromQL rejection, read accounting, and exact latest after shutdown/reopen.
The Session 4 contracts add selector and `avg_over_time` vector/matrix bodies,
strict lookback/window boundaries, request formats/errors, reopen, and a
4,000-series subquery cancellation/reuse regression. Session 3 additionally
pins explicit/default subquery grids, nested range functions, timing modifiers,
intermediate limits/accounting, SQL composition, and reopen. All extension-backed
contracts pass together.

That test exposed and fixed an existing extension gap: the engine queued a
series at 4,096 points but the metrics virtual table never drained its pending
queue. Every metrics ingest surface now calls the shared pending-flush path,
so direct SQLite/libSQL users receive the advertised behavior. Below threshold
the empty-queue path performs no store write. `tests/correctness.sh r1` pins
the direct-SQL threshold and transaction regressions.

## Session 2 ingest result

On the Session 0 host, two fresh deterministic runs per format used 4,000
series, 1,000 points/request, deferred scheduled maintenance, one SQLite writer,
two readers, and an explicit final flush. The final 3 ms step is limited by the
four-writer HTTP client near the Prometheus result, so it is a demonstrated
clean rate rather than the server's saturation point.

| format | completed points/s | write p95 | write p99 | final queue age | drain | HWM |
|---|---:|---:|---:|---:|---:|---:|
| Prometheus | 855.2–855.6K | 448us | 556us | 7–9ms | 725–745ms | 178,888–180,016KiB |
| VictoriaMetrics JSON lines | 613.2–620.9K | 2.78–2.87ms | 3.48–3.51ms | 7–8ms | 667–738ms | 178,164–179,460KiB |

Both formats had zero HTTP/storage errors, reached exactly 4,000 series, and
drained queued and in-flight work to zero. Prometheus is 9.7% faster than the
779.9K points/s Session 0 Elixir+libSQL no-query control, with 51% lower write
p95 and about 52% lower process HWM. VictoriaMetrics remains the deliberately
uncached named-series floor; resolved batch `0x02` is a later measured
optimization, not a Session 2 storage change.

The first Prometheus profiling run exposed an API observability regression: two
full `timeless_stats` scans surrounded every insert, reducing completion to
179.0K points/s. SQLite already returns the accepted point count in
`last_insert_rowid`; rejected-line totals belong to the extension's cumulative
stats. Removing the redundant scans produced the repeatable result above while
retaining exact completion and error accounting.

Full method and phase attribution are in
`../../../timeless_metrics/bench/results/2026-08-01_metrics_api_session2.md`.

## Session 3 mechanical read result

The fixed comparison used separate fresh processes seeded with the same 4,000
series and exactly 400,000 points. Every shape returned the same response byte
count from Elixir+libSQL and Rust+libSQL.

| shape | Elixir p95 | Rust p95 | result |
|---|---:|---:|---:|
| exact latest | 272us | 651us | Rust 2.39x slower |
| exact 15s range | 310us | 561us | Rust 1.81x slower |
| exact raw export | 248us | 752us | Rust 3.03x slower |
| all label names | 70.35ms | 10.54ms | Rust 6.68x faster |
| metric label values | 722us | 672us | Rust 1.07x faster |
| metric series | 5.15ms | 1.06ms | Rust 4.88x faster |
| exact selector series | 1.08ms | 957us | Rust 1.13x faster |

Same-lifecycle HWM was 56,948KiB for Rust and 244,796KiB for the Elixir
control. In the larger mixed run, Rust completed 866.6K points/s with 10.08ms
query p95 and 180,716KiB HWM; two Elixir controls completed 604.8-782.5K with
70.26-93.71ms query p95 and 457,128-460,152KiB HWM. Exact data reads retain a
small sub-millisecond Rust HTTP/serializer tax, while discovery, mixed tails,
and memory improve substantially. See
`../../../timeless_metrics/bench/results/2026-08-01_metrics_api_session3.md`
for the method, control cache-race observation, and discarded setup runs.

## Session 1 shell smoke result

On the Session 0 host, an empty release server with two readers handled the
three sequential control-route loops below with zero errors:

| route | requests | sequential req/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| `GET /health` | 2,000 | 2,047.2 | 471.2us | 788.8us | 914.1us |
| `GET /select/metrics/stats` | 2,000 | 2,007.9 | 490.5us | 790.2us | 914.6us |
| `POST /api/v1/flush` | 200 | 4,131.6 | 214.4us | 453.2us | 560.3us |

Linux `VmHWM` was 9,176 KiB (`VmRSS` 9,176 KiB after the run). These are shell
sanity numbers, not an ingest comparison with Session 0. Reproduce them with
`python3 bench_shell.py` while the server is running.
