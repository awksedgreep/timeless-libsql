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
- `GET /api/v1/labels`
- `GET /api/v1/label/{name}/values`
- `GET /api/v1/series`
- Prometheus aliases for instant/range queries and label/series discovery

The current PromQL slice supports scalar literals (including `NaN` and
infinities), string literals, exact-name and nameless instant vector
selectors, anchored regex/negative/duplicate `__name__` matchers, root range
selectors on instant queries, and
`avg_over_time(selector[window])`, `min_over_time(selector[window])`, and
`max_over_time(selector[window])`, `sum_over_time(selector[window])`, and
`count_over_time(selector[window])`, `present_over_time(selector[window])`,
`quantile_over_time(scalar, selector[window])`, plus
`stddev_over_time(selector[window])`, `stdvar_over_time(selector[window])`, and
`last_over_time(selector[window])`, and float-counter
`rate(selector[window])`, `irate(selector[window])`, and
`increase(selector[window])`, plus float-gauge `delta(selector[window])` and
`idelta(selector[window])`, and timestamp-centered least-squares
`deriv(selector[window])` and `predict_linear(selector[window], horizon)`.
Unary minus and arithmetic `+ - * / % ^`
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
Rust API rows at this revision are listed below for CI; prose in this README
must not imply a broader language surface.

<!-- query-contract-shipped: PQL-S01 PQL-S02 PQL-S03 PQL-S04 PQL-S05 PQL-S06 PQL-S07 PQL-S08 PQL-S09 PQL-S11 PQL-S12 PQL-S13 PQL-S16 PQL-S18 PQL-S19 PQL-S20 PQL-S21 PQL-O01 PQL-O02 PQL-O03 PQL-O04 PQL-O05 PQL-O06 PQL-O07 PQL-O09 PQL-O10 PQL-O11 PQL-O12 PQL-O13 PQL-O14 PQL-O15 PQL-O16 PQL-R01 PQL-R02 PQL-R03 PQL-R04 PQL-R05 PQL-R06 PQL-R08 PQL-R09 PQL-R10 PQL-R11 PQL-R12 PQL-R13 PQL-R14 PQL-R15 PQL-R16 PQL-R17 PQL-R18 -->

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
Successful responses currently omit Prometheus's optional top-level
`warnings`/`infos` annotations, including the counter-name lint emitted for a
non-`_total` metric. Numeric results and data envelopes remain exact; matrix
row `PQL-S23` tracks annotation parity explicitly rather than hiding the gap.

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
`predict_linear` use the same bounded packed raw path so the Rust evaluator
can apply their distinct counter/gauge semantics. A root range selector reads public raw frames,
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
