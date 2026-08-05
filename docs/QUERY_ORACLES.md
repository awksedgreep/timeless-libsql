# Query semantic oracles

This document pins the external implementations used to judge query-language
compatibility. Query tests must never use a moving image tag, a floating source
branch, or an unrecorded local installation. The pins below are the Session 0
baseline selected on 2026-08-04.

## Pinned versions

| language | role | upstream release | source commit | immutable multi-platform image | Linux amd64 image |
|---|---|---|---|---|---|
| PromQL | primary PromQL oracle | Prometheus `v3.13.2` | `bb5dff00cf8fdfbf5c65e0531aa835fa238a43a2` | `docker.io/prom/prometheus@sha256:508729e0e2d18e11fd742a5a5ca70e557b940a93948c3c95fd0123a6fd538b69` | `sha256:1147c92841726a6fef55fe6124491d6f85480f8de204f7d420304ca5bbd0a8f7` |
| MetricsQL | MetricsQL-only compatibility oracle | VictoriaMetrics community `v1.148.0` | `d94a85a4059b22fd238a0d2516bcb3e9bfb54587` | `docker.io/victoriametrics/victoria-metrics@sha256:407013e902f9a0ba1d4b2d4c077c47bbaf917c893c52ff39b19efe83a654afda` | `sha256:62f3b30fd73e16cc3a2909e3a2339499f8f8c597c77b851aeaf3a95b0a419001` |
| LogsQL | LogsQL oracle | VictoriaLogs `v1.52.0` | `46a54c976fa3d404396050e8a5ee6c5b0320efc5` | `docker.io/victoriametrics/victoria-logs@sha256:47b820890d64c4575a2a0a46415dcd8a4fd59a0f1fcd6a377693d7aea639442e` | `sha256:8f2140dca110705916751b9cdf57c2309555b6f1cf2707be1ee1a774c8c1e1f9` |

The VictoriaMetrics `v1.136.14` release visible when these pins were selected
is an enterprise LTS patch line and has no matching community image. It is not
the MetricsQL oracle. `v1.148.0` is the newest community release available at
the baseline date.

Source tag objects are also recorded so a future audit can distinguish an
annotated tag from its peeled commit:

| release | annotated tag object |
|---|---|
| Prometheus `v3.13.2` | `d08db18ac8e5eb1e30f941446ef954a44f510986` |
| VictoriaMetrics `v1.148.0` | `8509388b22920ec1e62949f54f11d30feb6c7170` |
| VictoriaLogs `v1.52.0` | `b753d73a38e3a779b35dc82e5f7d0e2bed5ec6fb` |

## Compatibility policy

Prometheus decides stable PromQL behavior. VictoriaMetrics is consulted for
PromQL differential coverage, but a disagreement does not silently redefine a
PromQL row. The affected row must record whether Timeless follows Prometheus,
intentionally offers a separately named MetricsQL behavior, or defers the
construct. MetricsQL rows are implemented only after the applicable stable
PromQL rows pass.

VictoriaLogs decides LogsQL behavior where its public language defines the
construct. The earlier `TimelessLogs.LogsQL` and DDNet-oriented tests decide
which P0 compatibility behaviors must be restored, but their silent-ignore
behavior is explicitly excluded: malformed and unsupported syntax must fail.

An oracle result is evidence, not a substitute for a Timeless regression. Each
shipped row must exercise the real `timeless-libsql` extension and compare the
applicable values, timestamps, labels or fields, types, ordering, result type,
and error classification. An upstream bug or intentional divergence is pinned
as a fixture and explained in the matrix row.

## Reproduction contract

Oracle harnesses must:

1. start the image by the immutable multi-platform digest above;
2. record the selected platform manifest and reported build version;
3. load a deterministic fixture with an explicit evaluation clock;
4. wait for durable ingestion before querying;
5. serialize requests, expected results, and normalized responses into the
   repository test fixture;
6. distinguish unordered language results from promised deterministic order;
7. preserve `NaN`, infinities, signed zero, missing/null/empty fields, and
   timestamp units rather than normalizing them away; and
8. stop and remove only the harness-owned container and temporary data.

Network access is needed only when deliberately refreshing an oracle fixture.
Normal CI runs the checked-in fixture against the real Timeless extension and
does not depend on an external service. Updating any pin is its own reviewed
query session: record upstream release notes, regenerate the affected fixtures,
run every prior oracle regression, and update the matrices in the same commit.

Validate the machine-readable pins without network access:

```bash
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- \
  oracle validate
```

When deliberately refreshing oracle evidence, probe the three immutable
containers and execute the baseline Prometheus fixture:

```bash
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle probe
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle prometheus-smoke
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle prometheus-api
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle victoria-metrics-api
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle victoria-logs-api
```

The manifest is
[`tests/query_oracles/manifest.json`](../tests/query_oracles/manifest.json).
The rule smoke fixture pins selector and `avg_over_time` sample semantics. The
row-addressed API fixture pins result types and exact HTTP error envelopes; the
refresh command starts and removes only its uniquely named temporary
Prometheus container. For lookback semantics, the harness encodes one
dependency-free protobuf/raw-Snappy Remote Write sample, waits for its `204`
durability response, then tests the exact boundary, millisecond inclusion, and
zero/default behavior against that real series. Later sessions extend these
fixtures before implementation. Multi-element vector fixtures compare by
complete label/sample identity with `result_order: "unordered"` by default.
They use `result_order: "ordered"` only for an operator such as an instant
`topk` whose output order is part of the language contract.

The VictoriaLogs API fixture ingests deterministic messages into uniquely
named streams around one second-aligned server-owned evaluation instant, waits
until every row is query-visible, and pins normalized case identities plus
exact status and content-type classifications. It covers relative time,
RFC3339 open/closed and comparison bounds, integer Unix seconds, milliseconds,
microseconds, and nanoseconds, deterministic explicit sorting/pagination,
case-sensitive phrase bytes, Unicode word boundaries, literal decoding,
quoted field identifiers, and malformed query envelopes without treating
VictoriaLogs' unspecified default row order as a contract. Time placeholders
are resolved by the Rust harness after the container starts, so the checked
fixture remains deterministic without relying on expired absolute timestamps.
The fixture now contains 142 cases. Its 23 `LQL-F11` cases pin the four pattern
anchors and case-insensitive function names; decimal/even-hex, UUID, IPv4,
time, date, datetime, and Unicode/quoted
word placeholders; exact Unicode Letter/Decimal_Number inclusion and
Letter_Number/Other_Number/Mark exclusion; unknown-placeholder literals; empty
patterns; restart after a failed partial candidate; field scope; upstream
numeric stringification; one simple unquoted compound token; and exact
one-argument errors. The matcher
behavior is also source-audited at VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`.

The eleven successful and two error `LQL-F16` cases pin case-sensitive,
start-anchored exact-prefix matching for messages and fields; lack of word-
boundary behavior; arbitrary UTF-8; logical composition; numeric and boolean
textual projection; empty-prefix matching across missing, null, empty, and
non-empty values; the equivalent case-insensitive `exact(value*)` function
name; and strict one-argument errors. Two companion `LQL-F15` cases correct
the earlier grammar gap by pinning `exact(value)` and unquoted `=value` exact
forms. The behavior is source-audited against `filter_exact_prefix.go`,
`filter_exact.go`, and `parser.go` at the same immutable VictoriaLogs commit;
a companion error case rejects ambiguous unquoted `==value` syntax.

The fifteen successful and two error `LQL-F17` cases pin static multi-exact
membership over messages and fields; quoted and unquoted arguments; quoted
commas, pipes, and literal stars; case-insensitive function names; numeric, boolean,
missing, null, and empty textual projection; duplicate elimination; empty
lists; logical and pipeline composition; and exact malformed-list errors. The
live oracle also records two non-obvious grammar rules: a trailing comma is
accepted, and any standalone unquoted `*` argument turns the whole filter into
a field-independent no-op even when other values are present. Subquery
membership remains a separate `LQL-F38` capability rather than being inferred
from the static list. The behavior is source-audited against `filter_in.go`,
`in_values.go`, and `parser.go` at the immutable VictoriaLogs commit above.

The nine successful and two error `LQL-F20` cases pin the field-independent
no-op produced by any standalone unquoted wildcard in `in(...)`,
`contains_any(...)`, or `contains_all(...)`. They cover missing fields,
case-insensitive function names, mixed value/wildcard lists, `NOT`, pipelines,
the optimized Timeless `service` and `level` aliases, and strict comma/
separator errors. A quoted `"*"` is an ordinary value, while non-wildcard
`contains_all`/`contains_any` values and query-backed lists remain independently
owned by `LQL-F21`, `LQL-F22`, and `LQL-F38`. The behavior is source-audited
against `parseInValues`, `parseArgsInParensPossibleWildcard`, and
`filter_noop.go` at the immutable VictoriaLogs commit above.

The VictoriaMetrics API fixture Remote Writes deterministic one-second and
slow-cadence series, then evaluates MetricsQL-only cases with explicit
range-query steps. It pins decimal, compound, and case-insensitive `Ni`
lookbehind/subquery durations, millisecond steps, positive and inherited-
negative offsets, direct and subquery adaptive `0i` rollups, and the exact
bare-`i` syntax error. The exact Prometheus rejection of the same language is
recorded separately. This oracle is used only for rows explicitly assigned to
the MetricsQL compatibility tier. The fixture contains sixteen success cases
and one explicit syntax error for `MQL-09`. Its `MQL-01` corpus adds ten
operator success cases and three syntax errors covering sparse and completely
filtered identity, non-overlapping samples, vector/scalar behavior,
`on(...)`, a join modifier,
comparison matching, nesting, precedence, and the pinned upstream error
classification. Its `MQL-02` corpus adds six successful operation-level name
cases and three syntax errors covering multi-name transforms, rollups,
scalar/vector and vector/vector binaries, the explicit-`on(...)` matching
exception, nested aggregation, and invalid selector/aggregate/unary targets.
Its `MQL-03` corpus adds thirteen successful union/alias cases and six errors.
It covers named and parenthesized union, zero/single/multiple inputs, trailing
commas, aliases that rename or remove the metric name, nested composition,
first-argument labelset precedence, case-insensitive `union` versus lowercase
template-only `alias`, malformed arguments, and the distinct
duplicate-output behavior of a bare alias or scalar union. The fixture now
adds thirteen successful `MQL-04` label-transform cases and five errors. They
pin add/replace/delete behavior, empty-value deletion, metric-name handling,
last-duplicate precedence, identity forms, nesting, scalar vectorization,
case-insensitive transform names, trailing commas, `keep_metric_names`, typed
arguments, and duplicate-output failures. Its `MQL-05` corpus adds 29 success
cases and two arity errors for implicit and explicit `default_rollup`,
open-left windows, 0.6-quantile scrape inference, jitter inflation,
`max_lookback`, stale markers, every retained one-argument window-less rollup,
previous-sample and counter-reset behavior, metric-name policy, timestamp
provenance, scalar and aggregate composition, case-insensitive names, and
trailing commas. The `MQL-06` corpus adds 17 successful range-aggregate cases
and three errors. It pins whole-grid reduction and repeated output,
slot-indexed incremental average, ordinary sum, later-operand extrema ties,
leading/interior missing grids, NaN/infinity/overflow behavior, scalar and
expression composition, case/trailing-comma grammar, unconditional name
removal, arity, and duplicate outputs. The `MQL-07` corpus adds 21 successful
running-aggregate cases and three errors. It pins cumulative average/minimum/
maximum/sum, slot-indexed arithmetic, missing and stale carry, computed-NaN
omission, overflow, infinity, signed zero, scalar/expression composition,
case/trailing-comma grammar, unconditional name removal, arity, and duplicate
outputs. The `MQL-10` corpus adds eight successful request-context cases and
five errors. It pins range and instant `start()`/`end()`/`step()` values,
subsecond request steps, pre-epoch bounds, case-insensitive names,
scalar/vector composition, zero-argument arity, and explicit rejection of
`start_timestamp()` and `range()`. The fixture now contains 164 MetricsQL
cases before `MQL-12`. The `MQL-12` corpus adds fifteen successful plural-
histogram cases and five errors. It pins multiple and time-varying ranks,
first-value `%g` labels, destination replacement/empty/`__name__`, source-name
removal under `keep_metric_names`, cumulative and `vmrange` buckets, missing
`+Inf`, negative-first interpolation, monotonic/NaN repair, computed-NaN
omission, scalar/empty inputs, arity/type errors, and duplicate-output
collisions. The fixture now contains 184 MetricsQL cases in total.
Timeless compares exact result labels, timestamp grids, and float values while
retaining its documented HTTP 400 `bad_data` envelope in place of
VictoriaMetrics's HTTP 422/error-type-`422` wire policy.

The `MQL-05` fixture deliberately records two storage-boundary observations.
VictoriaMetrics drops an ordinary NaN series during remote-write ingestion,
whereas Timeless's packed float storage preserves the ordinary NaN and
distinguishes it from the exact Prometheus stale-marker bits. Timeless therefore
matches stale-marker suppression but documents its stronger ordinary-NaN
fidelity as an intentional divergence. VictoriaMetrics also adjusts `deriv`'s
window from inferred scrape cadence without fetching the same leading silence
history used by `rate`; the slow-scrape leading evaluation consequently emits
zero. Timeless pins that behavior instead of applying a superficially more
uniform history policy.

The `MQL-06` fixture records an additional representation boundary.
VictoriaMetrics's Remote Write/query path renders both signed-zero input
orders as positive zero, although its pinned transform source chooses the
later operand for equal minima and maxima. Timeless retains exact stored
binary64 bits and therefore returns negative zero when the later operand is
negative zero. The API regression pins that stronger fidelity explicitly;
all non-representation semantics remain oracle-equal.

The `MQL-07` fixture proves that a stale/missing slot after the first value
emits the prior running state while still advancing `running_avg`'s slot
position. If a real arithmetic step computes NaN—for example positive
infinity followed by a finite value—the timestamp is omitted and the NaN
state is not replaced by the prior infinity. Timeless treats an ordinary
stored NaN as the same missing input for this transform while retaining its
stronger bit-exact storage contract outside the language operation. Running
extrema preserve the same Timeless signed-zero representation divergence as
the complete-grid functions.

Session 14 also pins Prometheus 3 quoted UTF-8 metric and label names, comments
and source positions, and classic-bucket `histogram_fraction` grouping and
interpolation. Its classification cases prove that query-context
`start`/`end`/`step`/`range` and `min_of`/`max_of` require
`promql-duration-expr`, vector-first `histogram_quantiles` requires
`promql-experimental-functions`, and `start_timestamp` is unknown to
Prometheus. These are explicit experimental or MetricsQL rows; the stable
Timeless endpoint must not enable them merely because the parser recognizes a
similar construct.

An exact Session 15 audit corrected two roadmap assumptions against the pinned
VictoriaMetrics binary and source commit. VictoriaMetrics 1.148.0 supports the
query-context functions `start()`, `end()`, and `step()`, but rejects
`start_timestamp()` and `range()` as unsupported functions. It also rejects
`min_of()` and `max_of()`; those names are not stable MetricsQL functions in
the pinned tier. The matrix records those dispositions instead of turning a
previous planning assumption into a compatibility claim.

For `MQL-10`, the pinned source implementation returns request start, end,
and step milliseconds divided by 1,000. Range queries therefore expose the
complete request bounds at every evaluation step; instant start and end both
equal the evaluation timestamp while instant step remains the explicit
request parameter. Timeless implements the same values in the Rust MetricsQL
planner. The language syntax does not enter SQLite, and the stable PromQL
endpoint retains its separate feature-gate behavior.

For `MQL-12`, pinned VictoriaMetrics source evaluates the bucket expression
once, copies it per rank, converts `vmrange` buckets to cumulative `le`, groups
after removing the bucket-family name, and formats each destination label from
the rank's first value. Its public Remote Write path drops an ordinary NaN
bucket before evaluation and its API omits computed NaN samples. Timeless
preserves the stored ordinary NaN bits, applies the same source-level repair
(leading NaN to zero; later NaN/decrease to the preceding count), and omits
only the computed NaN output. The resulting stored-NaN case is an intentional
stronger-fidelity representation divergence, while finite inputs and all
public language behavior match the live oracle.

The following Timeless compatibility choices intentionally differ from the
pinned VictoriaLogs wire/storage model and are asserted on both sides rather
than hidden:

- VictoriaLogs returns parser and unsupported-pipe failures as HTTP 400
  `text/plain`. Timeless returns stable JSON, using HTTP 400
  `invalid_query/malformed_logsql` for malformed input and HTTP 422
  `unsupported_capability/unsupported_logsql` for syntax it recognizes but
  does not implement. This preserves the product requirement that unsupported
  behavior never silently broadens or falls back.
- VictoriaLogs serializes `stats count() as total` as a JSON string. The
  established Timeless/DDNet contract returns the exact count as a JSON number;
  direct SQLite/libSQL users receive the same INTEGER from
  `timeless_log_count`.
- VictoriaLogs flattens stored field values to strings for discovery,
  projection, and statistics, collapses missing/null/empty states, coerces
  numeric-looking strings, and may round large integers through binary64.
  Timeless instead preserves its retained rich JSON types, exact integer
  identity, and missing/null/empty distinctions. The matrix records the exact
  result envelope for each affected row.
- VictoriaLogs exposes synthetic `_stream` and `_stream_id` fields.
  Timeless does not invent them because the retained storage model has no
  declared stream identity; stream filters and mutations remain deferred.
- VictoriaLogs `values` emits a flattened string. Timeless emits the lossless
  JSON object `{\"items\":[...],\"missing\":N}`, which is the only supported
  response shape that distinguishes an absent field from a stored null while
  preserving array/object values.

`QSF-063` and `QSF-076` through `QSF-080` record these selected compatibility
behaviors. The fixture now contains 57 row-query cases, six error cases, and
twelve statistics/pipeline cases (75 total). Phrase, escape, identifier,
filtering, ordering, cardinality, pipeline-order, limit-zero, and rate-window
semantics remain exact to the pinned oracle where the retained Timeless
storage model applies.
