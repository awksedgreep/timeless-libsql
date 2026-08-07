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
The fixture now contains 314 cases. Its 23 `LQL-F11` cases pin the four pattern
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
separator errors. A quoted `"*"` is an ordinary value; non-wildcard
`contains_all` and `contains_any` semantics are pinned independently by
`LQL-F21` and `LQL-F22`, while query-backed lists are pinned separately by
shipped `LQL-F38`. The
wildcard behavior is source-audited
against `parseInValues`, `parseArgsInParensPossibleWildcard`, and
`filter_noop.go` at the immutable VictoriaLogs commit above.

The sixteen successful and two error `LQL-F21` cases pin static
`contains_all(...)` semantics independently from the wildcard no-op. Every
non-empty argument is a case-sensitive phrase with Unicode letter, digit, and
underscore word boundaries, and every argument must match the same projected
field value. The cases cover separate word/phrase arguments, field scope,
quoted commas, case-sensitive values, case-insensitive function names,
duplicates, a trailing comma, logical/pipeline composition, and strict
separator/wildcard errors. An empty list or empty-string argument is the
logical identity and therefore does not inspect the field; a non-empty value
does not match a missing field. Query-backed values remain the separate
`LQL-F38` capability. The behavior is source-audited against
`filter_contains_all.go`, `filter_phrase.go`, `in_values.go`, and `parser.go`
at the immutable VictoriaLogs commit above.

The seventeen successful and two error `LQL-F22` cases pin static
`contains_any(...)` independently from both conjunction and wildcard no-op
behavior. Any matching case-sensitive phrase succeeds; an empty list matches
nothing, while any empty-string argument is field-independent true. The cases
cover missing fields, compact numeric and boolean textual projection, Unicode
word boundaries, field scope, quoted commas, duplicates, trailing commas,
case-insensitive function names, logical/pipeline composition, and strict
separator/wildcard errors. Query-backed values remain `LQL-F38`. The behavior
is source-audited against `filter_contains_any.go`, `filter_phrase.go`,
`in_values.go`, and `parser.go` at the immutable VictoriaLogs commit above.

The twenty successful and four error `LQL-F23` cases pin
`json_array_contains_any(...)` over top-level primitive array elements. They
cover exact strings; shared textual matching for stored numbers and strings;
decimal and negative numbers; booleans; null; empty lists and empty-string
elements; ignored nested arrays/objects; scalar/object/missing fields; quoted,
newline, slash, and Unicode escapes; quoted-star literal versus unquoted-star
error; case-insensitive function names; trailing commas; bare-name word
filtering; logical/pipeline composition; and strict separators. The behavior
is source-audited against `filter_json_array_contains_any.go`, its parser, and
tests at the immutable VictoriaLogs commit above. Timeless intentionally
compares decoded retained JSON strings, so a stored Unicode escape matches its
decoded candidate even though VictoriaLogs' raw-lexeme shortcut does not.

The eleven successful and seven error `LQL-F25` cases pin inclusive unsigned IPv4
ordering for one address, one CIDR, or two explicit address bounds. They cover
`/0`, network/broadcast edges, an inverted range that matches nothing,
case-insensitive function names, quoted arguments, decimal octets with leading
zeroes, exact whole-string parsing, arbitrary/message fields, logical and
pipeline composition, missing/null/numeric/invalid/embedded non-matches, and
strict argument/address/prefix errors. The behavior is source-audited against
`filter_ipv4_range.go`, `parser.go`, and `values_encoder.go` at VictoriaLogs
commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`.

The twelve successful and seven error `LQL-F26` cases pin inclusive unsigned
16-byte ordering for one address, one CIDR, or two explicit address bounds.
They cover compressed and uppercase spelling, `/0` and `/128`-space behavior,
network/broadcast edges, host-bit normalization, trailing commas, inverted
ranges, exact whole-string parsing, arbitrary/message fields, logical and
pipeline composition, missing/null/numeric/invalid/embedded non-matches, and
strict errors. VictoriaLogs maps IPv4 input into IPv4-mapped IPv6 space and
applies CIDR prefixes across all 128 bits, so `1.2.3.99/120` selects the mapped
`1.2.3.0/24` range. The behavior is source-audited against
`filter_ipv6_range.go`, `parser.go`, and their tests at VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`.

The sixteen successful and seven error `LQL-F27` cases pin plain bytewise
`string_range(minimum, maximum)` ordering with an inclusive lower and exclusive
upper bound. They cover equal-prefix and inverted ranges, ASCII case, UTF-8,
quoted commas, numbers, booleans, arrays, missing/null/empty projection,
arbitrary nested/message/service fields, trailing commas, case-insensitive
function names, logical/pipeline composition, and strict arity/separator/
wildcard errors. VictoriaLogs flattens an ingested object such as
`ip:{"value":"alpha"}` into `ip.value`, leaving `ip` missing; the oracle pins
that behavior. Timeless retains the complete object and its API may compact-
project the selected parent without discarding fidelity, an explicit retained-
model distinction covered by the real-extension regression. The grammar and
ordering are source-audited against `filter_string_range.go`, `parser.go`, and
their tests at VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`.

The seventeen successful and ten error `LQL-F28` cases pin inclusive
`len_range(minimum, maximum)` bounds over Unicode codepoint counts. They cover
multibyte characters, missing/null/empty length zero, strings, numbers,
booleans, arrays, VictoriaLogs' flattened object child, arbitrary nested,
service, and message fields, inverted ranges, logical/pipeline composition,
case-insensitive function names, trailing commas, quoted bounds, base-
prefixed integers (including base-zero octal), byte-size and duration
expressions, accepted negative zero, `inf`, and strict
arity/value/separator errors. VictoriaLogs flattens a retained object parent
to dotted children; Timeless preserves the parent and compact-projects it only
for evaluation. The behavior and bound grammar are source-audited against
`filter_len_range.go`, `parser.go`, and `values_encoder.go` at VictoriaLogs
commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`.

The twelve successful and twelve error `LQL-F30` cases pin same-row
`eq_field`, `le_field`, and `lt_field` comparisons. Equality compares the two
complete textual projections exactly. Ordering first parses both projections
as VictoriaLogs math values—decimal or base-zero numbers, durations, byte
sizes, RFC3339 timestamps, and IPv4 addresses—and otherwise compares unsigned
UTF-8 bytes. The cases cover equal and ordered values, same-field identities,
missing/null/empty projection, quoted fields, unqualified message scope,
service and nested fields, a right-hand `_time`, case-insensitive function
names, logical/pipeline composition, and strict arity, separator, wildcard,
and unterminated-call errors. `_time:` is reserved for time-filter grammar and
is therefore rejected as the left field even though `_time` is a valid
right-hand projection. VictoriaLogs flattens retained objects; Timeless keeps
the complete typed parent and compact-projects it only for comparison. When
both Timeless values are retained JSON numbers, their exact number ordering is
used before textual math parsing so integers beyond binary64 precision remain
correct. The grammar, projection, and ordering are source-audited against
`filter_eq_field.go`, `filter_le_field.go`, `pipe_math.go`,
`values_encoder.go`, and `parser.go` at VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`.

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
The `MQL-08` disposition adds twelve successful acceptance witnesses, one for
each item in the finite legacy rejection catalog. The cases deliberately use
small deterministic values: they prove that pinned VictoriaMetrics recognizes
`label_keep`, `label_map`, `quantiles`, `distinct`, `increase_pure`,
`remove_resets`, `interpolate`, `keep_last_value`, `keep_next_value`,
`drop_common_labels`, `rate_over_sum`, and `WITH`, without pretending those
unrelated features share one compatibility contract. The fixture now contains
196 MetricsQL cases in total.
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

`PQL-O17` adds two error cases to that same immutable Prometheus oracle. At
source commit `bb5dff00cf8fdfbf5c65e0531aa835fa238a43a2`, the parser classifies
`limitk` and `limit_ratio` as experimental aggregators and rejects them unless
`--enable-feature=promql-experimental-functions` is active. The default pinned
image is intentionally started without that flag, so both cases require the
exact feature-gate diagnostic. The complete 530-case Prometheus API fixture
passes. This is a stable-tier compatibility decision, not evidence that
Timeless implements the experimental operators. A future implementation must
use a separately configured oracle run with the feature enabled.

`PQL-O18` adds two more default-tier cases. The pinned parser accepts
`fill`, `fill_left`, and `fill_right` syntax but reports `binop fill modifiers
are experimental and not enabled` unless `promql-binop-fill-modifiers` is
active. The cases cover symmetric fill and modifier-last
`on`/`group_left`/`fill_right` grammar; the complete Prometheus API fixture is
now 532 cases and passes. Source audit at the same immutable commit confirms
that fill values are numeric literals, either side may be enabled, set
operators are excluded, histogram samples are unsupported, and evaluation
must account for one-to-one plus many/one matching. Future execution parity
requires a separate oracle process started with this feature flag; the default
oracle remains the stable compatibility authority.

`PQL-O19` adds two stable-tier float-input cases, bringing the complete
Prometheus API fixture to 534 cases. The pinned parser accepts `</` and `>/`
without a feature flag. A float vector on the left and scalar float on the
right produces an empty vector plus an exact incompatible-sample-types info;
source audit confirms that only native-histogram-left/scalar-right evaluation
returns a value. `</` keeps observations below the threshold and `>/` keeps
observations above it, interpolating exponential, custom, zero, and infinite
buckets and recomputing count and sum. This oracle evidence defines the future
operator contract; it does not erase Timeless's missing `PQL-S22` typed sample
model. Until that model exists, the stable Timeless endpoint intentionally
returns an explicit typed-storage prerequisite instead of pretending that
classic `_bucket` floats are equivalent.

`PQL-R22` adds the default-tier `mad_over_time` feature-gate case, bringing
the complete Prometheus API fixture to 535 cases. The pinned parser marks the
function experimental and requires `promql-experimental-functions`. Source
audit at the same immutable commit confirms that enabled evaluation computes
the linear median of all float samples, then the linear median of their
absolute deviations. An all-native-histogram range emits no result or info; a
mixed float/histogram range computes from floats and emits the standard
histogram-ignored info. Stable Timeless preserves the default feature-gate
diagnostic. Future execution parity requires a separately feature-enabled
oracle, and full mixed-range parity requires typed native-histogram storage.

`PQL-R23` adds exact default-tier cases for all four timestamp-of-range
functions, bringing the complete Prometheus API fixture to 539 cases. The
pinned parser requires `promql-experimental-functions` for each. Source audit
confirms that `ts_of_first_over_time` and `ts_of_last_over_time` choose the
earliest/latest timestamp across floats and native histograms. The min/max
forms inspect floats only, choose the last timestamp tied for the extreme,
omit histogram-only ranges, and emit a histogram-ignored info for mixed
ranges. Returned values are source timestamps in fractional seconds. Stable
Timeless preserves all four default feature gates; future execution requires a
separately feature-enabled oracle and typed histograms for full parity.

`PQL-F14` adds exact default-tier cases for `sort_by_label` and
`sort_by_label_desc`, bringing the complete Prometheus API fixture to 541
cases. The pinned parser requires `promql-experimental-functions` for both.
Source audit confirms natural ascending/descending comparison across the
ordered variadic label list, empty strings for absent labels, and complete
label-set tie-breaking in the same direction. Float and native-histogram
samples sort identically. Instant queries preserve that order; range queries
retain fixed label order and emit the standard ineffective-sort warning.
Stable Timeless preserves both default feature gates; future execution
requires a separately feature-enabled oracle.

`PQL-F21` adds exact default-tier cases for both `info(v)` and
`info(v, {selector})`, bringing the complete Prometheus API fixture to 543
cases. Pinned source marks the function experimental and requires
`promql-experimental-functions` before arity, selector-only syntax, or
evaluation proceeds. Enabled behavior independently selects info series,
defaults to `target_info`, joins on `job`/`instance`, resolves changing data
labels by newest source timestamp, applies empty-string matcher semantics,
and observes lookback and stale markers. Info sources must be floats while
base values can be floats or native histograms. Stable Timeless preserves the
default feature gate; future execution requires a feature-enabled oracle plus
the retained-model prerequisites `PQL-S17` and `PQL-S22`.

`PQL-S22` adds no executable API case because Timeless has no native-histogram
ingress or result type to compare honestly. The immutable Prometheus 3.13.2
source remains the design oracle: its float histogram model carries schema,
zero threshold/count, total count/sum, positive and negative spans and bucket
populations, custom bounds, and a counter-reset/gauge hint. The TSDB and
PromQL paths also permit float and histogram samples to coexist over a series
history. Timeless now advertises its narrower current contract directly as
`sample_types=["float64"]` and `native_histograms=false`; the 543-case fixture
count is unchanged. A future feature branch must add pinned remote-write,
storage, mixed-sample, reset, staleness, query, and response oracles before
changing this row or capability.

`PQL-H04` adds six stable-tier cases, bringing the complete Prometheus API
fixture to 549 cases. Five instant cases prove that `histogram_avg`,
`histogram_count`, `histogram_sum`, `histogram_stddev`, and `histogram_stdvar`
return an empty vector for float input; one range case proves the corresponding
empty matrix. Pinned parser definitions accept exactly one instant vector and
do not feature-gate these functions. At source commit
`bb5dff00cf8fdfbf5c65e0531aa835fa238a43a2`, `simpleHistogramFunc` processes
only samples whose native-histogram pointer is present. Count and sum read the
stored histogram fields; average divides sum by count; standard deviation and
variance estimate bucket populations using arithmetic or geometric bucket
representatives with compensated accumulation. Timeless ships the complete
retained float-only behavior while leaving all value-producing semantics
deferred behind `PQL-S22`.

An exact Session 15 audit corrected two roadmap assumptions against the pinned
VictoriaMetrics binary and source commit. VictoriaMetrics 1.148.0 supports the
query-context functions `start()`, `end()`, and `step()`, but rejects
`start_timestamp()` and `range()` as unsupported functions. It also rejects
`min_of()` and `max_of()`; those names are not stable MetricsQL functions in
the pinned tier. The matrix records those dispositions instead of turning a
previous planning assumption into a compatibility claim.

The `MQL-08` source audit is also pinned to
`d94a85a4059b22fd238a0d2516bcb3e9bfb54587`. VictoriaMetrics registers
`label_keep`, `label_map`, `drop_common_labels`, `remove_resets`,
`interpolate`, `keep_last_value`, and `keep_next_value` in
`app/vmselect/promql/transform.go`; `distinct` and `quantiles` in
`app/vmselect/promql/aggr.go`; and `increase_pure` and `rate_over_sum` in
`app/vmselect/promql/rollup.go`. Its vendored MetricsQL 0.87.3 parser expands
`WITH` expression, function, selector, and duration templates before
evaluation. This classification is why the catch-all is deferred and split
into future row prerequisites rather than implemented as one function family.

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
- Repeated pinned runs showed that VictoriaLogs may reorder the encoded
  elements returned by `values(field)` as fixture block layout changes, even
  though duplicates remain present. The oracle therefore compares only the
  explicitly marked `all_values` array as an unordered multiset, preserving
  duplicate counts and the surrounding result-row order. Timeless continues
  to promise its own deterministic input order.

`QSF-063`, `QSF-076` through `QSF-080`, `QSF-125`, and `QSF-127` record these
selected compatibility behaviors. `LQL-F33` adds deterministic UTC-time
fixture rows and fifteen successful plus eight error cases for open/closed day
ranges, compact clocks, fixed signed offsets, full-day/equal/inverted bounds,
pipeline composition, and strict grammar. `LQL-F34` adds deterministic
weekday rows and fifteen successful plus eight error cases for open/closed
Sunday-through-Saturday ranges, fixed signed offsets, bracket-wrap edges,
valid empty ranges, pipeline composition, and strict grammar. The pinned
oracle container runs in UTC; Timeless selects UTC explicitly when either
query omits an offset rather than copying VictoriaLogs' mutable process-local
default. `LQL-F40` adds fourteen successful and six error cases for comments
at token boundaries, LF/CRLF multiline queries, quoted hashes, terminal
semicolons, and strict malformed tails. The fixture now contains 296 row-query
cases, 94 error cases, and twelve statistics/pipeline cases. In total, the fixture now contains 402 cases.
Phrase, escape,
identifier,
filtering, ordering, cardinality, pipeline-order, limit-zero, and rate-window
semantics remain exact to the pinned oracle where the retained Timeless
storage model applies.

`LQL-P07` adds eighteen successful pipeline-output cases and eight error cases
for exact, prefix, quoted, special, nested, array, null, empty, unknown, and
all-field deletion; the `delete`/`del`/`drop`/`rm` aliases; ordered projection
and filter composition; idempotence; and strict comma/wildcard grammar. A row
with no fields after deletion is omitted rather than encoded as an empty JSON
object. The fixture now contains 296 row-query cases, 102 error cases, and 30
statistics/pipeline cases; the fixture now contains 428 cases in total.

`LQL-P12` adds five successful statistics/pipeline cases and two error cases.
They pin exact and empty logical `RowsFound`, full-fixture cardinality, a
controlled one-row `limit` composition, one-row output for a later aggregate,
and strict no-argument grammar. The full fourteen-field names, ordering, and
string types are source-audited against `pipe_query_stats.go` and pinned by the
real-extension regression. Physical byte/work counters are not asserted equal
across products: VictoriaLogs reads column files and can stop parallel workers
after `limit`, while Timeless reads one rich block payload and eagerly executes
the complete bounded API rowset. A controlled VictoriaLogs one-row/two-stream
probe returned one found row after `limit 1`, while the 135-row fixture returned
four on a separate run, confirming that early-stop physical work is scheduling
dependent. Both products report actual work; `QSF-150` records the selected
compatibility profile. The fixture now contains 296 row-query cases, 104 error
cases, and 35 statistics/pipeline cases; the fixture now contains 435 cases in
total.

`LQL-P13` adds ten successful statistics/pipeline cases and eight error cases
for default and explicit counts, natural ascending order, per-field descending
order, partition-local rank strings, default rank naming, RFC3339 time order,
filter composition, current-schema no-`by` behavior after deletion and
projection, empty input, and strict count/field/partition/rank/tail grammar.
Source audit pins the coercion chain to exact signed integer, exact unsigned
integer, RFC3339 timestamp, general numeric/duration/byte value, and finally
natural UTF-8 byte order. A controlled live probe pins rank reset per
partition and encoded partition-key order. Equal-key order is not an upstream
contract; Timeless deliberately uses original public-row order as a stable
tie-break. VictoriaLogs flattens values to strings, while Timeless retains its
rich JSON response types. The fixture now contains 296 row-query cases, 112
error cases, and 45 statistics/pipeline cases: 453 cases total, all passing
against the immutable VictoriaLogs v1.52.0 image.
The fixture now contains 453 cases.

`LQL-P14` adds ten successful statistics/pipeline cases and eight error cases.
They prove that `last` accepts the same default/count/by/partition/rank grammar
as `first` and reverses the complete selected order. Per-field `desc` reverses
before the global operation direction, rank strings restart at one per
partition, no-`by` observes the current schema after deletion or projection,
and empty input emits no rows. The cases also pin exact integer/natural order,
RFC3339 time order, filter composition, and strict malformed syntax. Source
audit confirms both operations share `parsePipeLastFirst` and the bounded
top-k processor in VictoriaLogs v1.52.0. The fixture now contains 296
row-query cases, 120 error cases, and 55 statistics/pipeline cases: 471 cases
total, all passing against the immutable image.
The fixture now contains 471 cases.

`LQL-P15` adds eleven successful statistics/pipeline cases and eight error cases.
They pin default-ten and explicit limits, parenthesized and bare multi-field
lists, frequency-descending and bytewise-key tie order, `hits [as]` and
`rank [as]` string fields (including an explicit `hits as hits` default-name
alias), collision-safe result names, case-insensitive keywords,
current-pipeline filter/projection composition, and empty input.
Missing, JSON null, and empty values share one empty-text group whose field is
omitted from stream JSON; numbers and strings use their textual projection.
Source audit confirms the immutable VictoriaLogs v1.52.0 implementation counts
all unique field vectors in bounded state, retains only the requested top
groups, and orders equal counts by encoded key. The fixture now contains 296
row-query cases, 128 error cases, and 66 statistics/pipeline cases: 490 cases
total, all passing against the immutable image.
The fixture now contains 490 cases.

`LQL-P16` adds fourteen successful pipeline cases and twelve error cases for
parenthesized and bare single/multiple fields; optional `by`; `hits` and
`with hits`; collision-safe hits naming; textual numeric and empty-state
projection; case-sensitive substring filtering; zero and bounded limits;
overflow-hit reset; current-pipeline composition; case-insensitive keywords;
empty input; and strict field/filter/hits/limit/tail grammar. Unsorted `uniq`
rows are explicitly compared as an unordered set because the pinned source
emits hash-map state without an ordering contract. Timeless deliberately emits
deterministic bytewise key order, while oracle comparison does not invent that
as an upstream promise. Source audit is against `pipe_uniq.go` and
`hits_map.go` at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture contains 296
row-query cases, 140 error cases, and 80 statistics/pipeline cases: 516 cases
total.
The fixture now contains 516 cases.

`LQL-P18` adds eleven successful statistics/pipeline cases and ten error cases
for default and per-field limits, empty/null/missing exclusion, textual
projection, constant-field suppression and retention, high-cardinality and
long-value field exclusion, modifier reordering/repetition/case, recursively
flattened rich fields, positive fractional-number truncation, empty input, and
strict missing/zero/nonnumeric/trailing syntax. Facet rows are explicitly
compared as an unordered set because the local pinned processor sorts only by
hits within a field, while its distributed rewrite adds a field-value tie
break. Timeless deliberately adds that bytewise tie break everywhere. Source
audit is against `pipe_facets.go`, `pipe_facets_test.go`, and the distributed
rewrite at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture contains 296
row-query cases, 150 error cases, and 91 statistics/pipeline cases: 537 cases
total.
The fixture now contains 537 cases.

`LQL-P19` adds eleven successful pipeline cases and eleven error cases for
first-nonempty priority, missing/null/empty skipping, numeric and boolean
textual projection, default destination `_msg`, quoted defaults and
destinations, recursively flattened exact fields, prefix expansion order,
empty prefix matches, destination replacement, current-row composition,
case-insensitive grammar, trailing comma, empty input, and strict parentheses,
field-list, default, destination, wildcard, function, and tail errors. Source
audit is against `pipe_coalesce.go`, `pipe_coalesce_test.go`,
`parser_utils.go`, and `block_result.go` at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture contains 296
row-query cases, 161 error cases, and 102 statistics/pipeline cases: 559 cases
total.
The fixture now contains 559 cases.

`LQL-P20` adds twenty successful pipeline cases and eight error cases for
typed exact copies; retained sources; missing and flattened object-parent
behavior; `copy`/`cp`; optional `as`; strict pair/comma grammar; sequential
chains, swaps, and overwrite order; exact self-copy; all-field identity;
prefix substitution; prefix-to-exact deterministic last-write behavior;
all-field-to-prefix mapping; unmatched prefixes; wildcard-pair chaining;
exact-to-wildcard literal destinations; the literal empty field produced by
an empty wildcard suffix; current message preservation; and empty input.
Source audit is against `pipe_copy.go`, `pipe_copy_test.go`,
`parser_utils.go`, `block_result.go`, and `prefixfilter/filter.go` at immutable
VictoriaLogs commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`.
The fixture contains 296 row-query cases, 169 error cases, and 122
statistics/pipeline cases: 587 cases total.
The fixture now contains 587 cases.

`LQL-P21` adds twenty successful pipeline cases and nine error cases for
typed exact moves and source removal; missing and flattened object-parent
behavior; `rename`/`mv`; optional `as`; strict pair/comma grammar; sequential
chains, swaps, repeated-source emptiness, and overwrite order; exact and
all-field identity; prefix substitution; prefix-to-exact deterministic
last-write behavior; all-field-to-prefix mapping; unmatched prefixes;
wildcard-pair chaining; exact-to-wildcard literal destinations; the literal
empty field produced by an empty wildcard suffix; and empty input. Source
audit is against `pipe_rename.go`, `pipe_rename_test.go`, `parser_utils.go`,
`block_result.go`, and `prefixfilter/filter.go` at immutable VictoriaLogs
commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`.
The fixture contains 296 row-query cases, 178 error cases, and 142
statistics/pipeline cases: 616 cases total.
The fixture now contains 616 cases.

`LQL-P22` adds twenty-four successful pipeline cases and nine error cases for
default and exact destinations; quoted/unquoted patterns; optional and empty
`if (...)`; HTML-decoded literal prefixes; empty, exact, missing, flattened,
numeric, boolean, array, and null placeholders; simple Unicode case mapping;
JSON quoting; URL, hex, Base64, numeric-hex, IPv4, human-duration,
nanosecond-duration, and Unix-time transforms; invalid-codec fallback;
unknown-option passthrough; keep/skip behavior; exact integer, fractional,
scientific, signed, millisecond, microsecond, and nanosecond timestamp forms;
the pinned minimum-int64 duration edge; and strict pattern, wildcard,
destination, modifier, condition, and tail errors. Source audit is against
`pipe_format.go`, `pipe_format_test.go`, `pattern.go`, `values_encoder.go`,
and vendored `timeutil/time.go` at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. Timeless preserves explicit
empty rich fields where VictoriaLogs stream JSON omits empty-valued columns;
the matrix and `QSF-170` record that intentional retained-model distinction.
The fixture contains 296 row-query cases, 187 error cases, and 166
statistics/pipeline cases: 649 cases total, all passing against the immutable
image.
The fixture now contains 649 cases.

`LQL-P23` adds twenty-two successful pipeline cases and nineteen error cases
for `math`/`eval`; sequential destinations and optional `as`; canonical
default result names; unary operators, parentheses, left-associative binary
precedence, arithmetic, remainder, power, bitwise operators, and NaN-only
`default`; every supported function and strict arity; trailing function
commas; decimal, base-zero, scaled-number, duration, byte-size, RFC3339, and
IPv4 coercion; fixed float and nonfinite rendering; missing/null/empty/rich
invalid values; quoted/dotted fields; volatile `now`/`rand`; format
composition; and the pinned parser ambiguities around `as`, adjacent `*`, and
negative scientific exponents. Two final cases pin VictoriaLogs' unsigned
conversion of negative, nonfinite, and out-of-range values before bitwise
operations. Source audit is against `pipe_math.go`, `pipe_math_test.go`,
`parser.go`, `values_encoder.go`, and the numeric/duration/byte/time/IP parser
helpers at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`.
The fixture contains 296 row-query cases, 206 error cases, and 188
statistics/pipeline cases: 690 cases total, all passing against the immutable
image.
The fixture now contains 690 cases.

`LQL-P24` adds thirteen successful pipeline cases and eight error cases for
the `len` pipe. They pin UTF-8 byte length rather than Unicode codepoint
length; optional parentheses and `as`; default and empty-alias `_msg`
behavior; case-insensitive syntax; sequential overwrite; exact quoted and
dotted fields; canonical `_time` rendering; composition with `math`;
missing/null/empty/object-parent zeroes; and textual number, boolean, array,
and nested-leaf lengths. Wildcards, absent sources, unclosed or multiple
source arguments, and trailing tokens fail. Source audit is against
`pipe_len.go`, its parser helpers, and the result-column encoders at immutable
VictoriaLogs commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`.
The fixture contains 296 row-query cases, 214 error cases, and 201
statistics/pipeline cases: 711 cases total, all passing against the immutable
image.
The fixture now contains 711 cases.

`LQL-P28` adds two successful row-query cases, five exact pipeline-result
cases, and four error cases for `drop_empty_fields`. They pin the argumentless,
case-insensitive grammar; terminal semicolon; null/empty removal; zero, false,
array, nested-leaf, and nonempty retention; a sequentially created empty
field; all-empty row omission; and strict rejection of parentheses, arguments,
attached suffixes, and `as`. Source audit is against
`pipe_drop_empty_fields.go` and `pipe_drop_empty_fields_test.go` at immutable
VictoriaLogs commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`.
The fixture contains 298 row-query cases, 218 error cases, and 206
statistics/pipeline cases: 722 cases total, all passing against the immutable
image.
The fixture now contains 722 cases.

`LQL-P29` adds eleven exact pipeline-result cases and seven error cases for
literal `replace`. They pin default-message and exact-field targets,
case-insensitive syntax, Unicode literals, all/zero/first-`N` replacement,
optional matching and nonmatching `if (...)`, empty-old no-op behavior,
quoted/dotted fields, number/boolean/array textual projection, and sequential
composition. Missing arguments, wrong arity, wildcard targets, nonnumeric or
leading-zero limits, and trailing tokens fail. Source audit is against
`pipe_replace.go`, `pipe_replace_test.go`, `pipe_update.go`, and the unsigned
limit parser at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The direct pipe parser rejects
attached `replace(foo,bar)`, while the complete HTTP query parser can
ambiguously accept that text as an unrelated filter. Timeless deliberately
rejects the attached spelling instead of silently ignoring the intended pipe.
The fixture contains 309 row-query cases, 225 error cases, and 206
statistics/pipeline cases: 740 cases total, all passing against the immutable
image.
The fixture now contains 740 cases.

`LQL-P30` adds fifteen exact pipeline-result cases and ten error cases for
`replace_regexp`. They pin all non-overlapping and first-`N`/zero-unbounded
replacement; default-message and exact-field targets; optional matching and
nonmatching `if (...)`; case-insensitive syntax; dot-newline default and
inline disablement; numbered, named, full-match, missing, maximal-name, and
literal-dollar expansion; UTF-8 empty-pattern boundaries; start/end anchors;
number, boolean, and JSON-array textual projection; and sequential
composition. Wrong arity, invalid regular expressions, backreferences,
lookaround, wildcard targets, nonnumeric or leading-zero limits, and trailing
tokens fail. Source audit is against `pipe_replace_regexp.go`,
`pipe_replace_regexp_test.go`, `pipe_extract_regexp.go`, and Go's
`regexp.ExpandString` implementation at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. After exact-result cases were
classified under `stats_cases`, the fixture contains 298 row-query cases, 235
error cases, and 232 statistics/pipeline cases: 765 cases total, all passing
against the immutable image.

`LQL-P32` adds twenty-three exact pipeline-result cases and fourteen error cases
for literal `extract`. They pin named and anonymous placeholders; HTML-decoded
delimiters; nonempty-prefix search and empty-prefix anchoring; default `_msg`
and exact `from` sources; automatic Go double/single/raw quoted-string
decoding and `plain:`; partial quoted matches; default empty writes;
`keep_original_fields` and `skip_empty_results`; matching and nonmatching
conditions; case-insensitive syntax; numeric textual projection; and
sequential composition. Missing or literal-only patterns, adjacent fields,
wildcard outputs/sources, missing sources, misplaced conditions, conflicting
modifiers, unterminated fields, and trailing tokens fail. Source audit is
against `pipe_extract.go`, `pipe_extract_test.go`, `pattern.go`, and
`pattern_test.go` at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture contains 298
row-query cases, 249 error cases, and 255 statistics/pipeline cases: 802 cases
total, all passing against the immutable image.
The fixture now contains 802 cases.

`LQL-P33` adds nineteen exact pipeline-result cases and fourteen error cases
for `extract_regexp`. They pin named and anonymous groups; first-match-only
behavior; dot-newline default and inline disablement; unmatched optional
captures; default, keep-original, and skip-empty writes; matching and
nonmatching conditions; case-insensitive syntax; exact quoted sources;
sequential composition; number, boolean, and JSON-array textual projection;
message replacement; both named-group spellings; and Unicode case folding.
Missing patterns, anonymous-only patterns, invalid regular expressions,
backreferences, lookaround, wildcard captures/sources, missing sources,
misplaced conditions, conflicting modifiers, and trailing tokens fail.
Source audit is against `pipe_extract_regexp.go` and
`pipe_extract_regexp_test.go` at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture contains 298
row-query cases, 263 error cases, and 274 statistics/pipeline cases: 835 cases
total, all passing against the immutable image.
The fixture now contains 835 cases.

`LQL-P34` adds eleven exact pipeline-result cases and four error cases for
`pack_json`. They pin default `_msg` and explicit/bare destinations; source
snapshotting before destination overwrite; exact, prefix, empty-list, and
all-field selection; case-insensitive grammar; terminal `as`; missing-field
objects; flattened nested fields; numeric textualization; upstream empty-value
omission; and duplicate keys from overlapping upstream selectors. A second
destination, missing parenthesized field list, and wildcard destinations fail.
Source audit is against `pipe_pack_json.go`, `pipe_pack.go`, `rows.go`, and
their tests at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture contains 298
row-query cases, 267 error cases, and 285 statistics/pipeline cases: 850 cases
total, all passing against the immutable image. Timeless selects an explicit
richer retained-model policy: one deterministic JSON object, idempotent
selector union, native JSON types, explicit empty/null values, and reconstructed
nested objects instead of duplicate flattened textual keys.
The fixture now contains 850 cases.

`LQL-P36` adds seventeen exact pipeline/statistics cases and eight error cases
for `unpack_json`. They pin default `_msg`, bare and explicit `from` sources,
exact/missing/prefix/all selection, empty `fields ()`, preserved nested keys,
result prefixes, source snapshots, default/keep-original/skip-empty writes,
matching and nonmatching conditions, case-insensitive and quoted grammar,
surrounding whitespace, native textualization, nonobject no-ops, malformed
object exact-field empties, and the accepted bare `NaN` token. Missing
conditions/sources/prefixes, wildcard sources or preserved keys, malformed
field lists, trailing tokens, and conflicting preservation modifiers fail.
Source audit is against `pipe_unpack_json.go`, `pipe_unpack.go`,
`json_parser.go`, and their tests at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture contains 298
row-query cases, 275 error cases, and 302 statistics/pipeline cases: 875 cases
total, all passing against the immutable image. Timeless selects an explicit
richer retained-model policy: native JSON types, literal dotted keys, explicit
empty/null values, and reconstructed nesting rather than flattened textual
columns.
The fixture now contains 875 cases.

`LQL-P41` adds twelve exact pipeline/statistics cases and ten error cases for
`json_array_len`. They pin top-level counts for string, mixed, nested, and
empty arrays; zero for scalar, object, missing, and malformed sources;
surrounding whitespace; the accepted bare `NaN` token; case-insensitive bare
and parenthesized grammar; quoted fields; default `_msg`; and source
snapshotting before overwrite. Missing or unclosed sources, empty/multiple
arguments, wildcard or prefix sources/destinations, and trailing tokens fail.
Source audit is against `pipe_json_array_len.go` and
`pipe_json_array_len_test.go` at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture contains 298
row-query cases, 285 error cases, and 314 statistics/pipeline cases: 897 cases
total, all passing against the immutable image. Timeless also accepts retained
native arrays and preserves their rich values rather than requiring a
flattened textual source.
The fixture now contains 897 cases.

`LQL-S07` adds five exact statistics cases and five error cases for
`quantile` and `stddev`. They pin required decimal quantile ranks, inclusive
`[0,1]` bounds, upper-step selection, signed/unsigned/timestamp/math/natural
text ordering, default current-field selection, case-insensitive function
names, population deviation, empty input, and strict malformed grammar.
Source audit is against `stats_quantile.go`, `stats_stddev.go`,
`pipe_sort_topk.go`, and `block_result.go` at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. VictoriaLogs flattens values,
coerces numeric strings for deviation, emits textual `NaN` for empty
deviation, omits an empty quantile field from stream JSON, and randomly
reservoir-samples quantiles above 10,000 values. Timeless deliberately retains
rich JSON types, ignores numeric strings in numeric statistics, returns JSON
null for an empty deviation, preserves an explicit empty quantile string, and
fails a deterministic exact quantile when its configured state limit is
exceeded. The fixture now contains 298 row-query cases, 290 error cases, and
319 statistics/pipeline cases; the fixture now contains 907 cases in total,
all passing against the immutable image.

`LQL-S09` adds five exact statistics cases and five error cases for
`sum_len`. They pin textual UTF-8 byte counts, numeric spellings, default
current-field selection, exact and prefix fields, case-insensitive function
names, and strict parentheses/comma/wildcard/trailing-token grammar. Source
audit is against `stats_sum_len.go`, `stats_sum_len_test.go`,
`stats_parser.go`, and `block_result.go` at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. VictoriaLogs returns a decimal
string from one unsigned 64-bit aggregate. Timeless keeps the same byte and
selection semantics but returns a native JSON integer under its retained
typed-statistics policy; retained arrays and objects use compact JSON rather
than upstream flattened columns. The fixture now contains 298 row-query
cases, 295 error cases, and 324 statistics/pipeline cases: 917 cases total,
all passing against the immutable image. The fixture now contains 917 cases.

`LQL-S10` adds five exact statistics cases and six error cases for `any`,
`field_min`, and `field_max`. They pin one-field/two-field arity,
case-insensitive function names, nonempty selection, companion-field lookup,
signed/unsigned/math/natural comparison of numeric text, and strict missing-
parenthesis/arity grammar. Source audit is against `stats_any.go`,
`stats_field_min.go`, `stats_field_max.go`, their tests, `stats_parser.go`, and
the shared string comparator at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. VictoriaLogs deliberately returns
an arbitrary nonempty `any` value; the same multi-value fixture selected
different values after physical re-encoding during the live audit. The pinned
case therefore proves selection only when one nonempty candidate exists.
Timeless explicitly strengthens that contract to the first nonempty value in
deterministic current-pipeline order. VictoriaLogs flattens the selected
companion to text; Timeless retains strings, numbers, booleans, arrays,
objects, null, empty, and missing states, with a missing companion represented
by the stable empty string. The fixture now contains 298 row-query cases, 301
error cases, and 329 statistics/pipeline cases: 928 cases total, all passing
against the immutable image. The fixture now contains 928 cases.

`LQL-S11` adds six exact statistics cases and seven error cases for `row_any`,
`row_min`, and `row_max`. They pin complete same-row field selection, empty
`{}` output, flattened-prefix selection, signed/unsigned/math/natural source
comparison, case-insensitive function names, optional implicit result aliases,
required exact extrema sources, and strict malformed grammar. Source audit is
against `stats_row_any.go`, `stats_row_min.go`, `stats_row_max.go`, their
tests, `pipe_stats.go`, and the shared string comparator at immutable
VictoriaLogs commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`.
VictoriaLogs returns flattened string fields and lets `row_any` choose a row
according to parallel merge order. Timeless deliberately chooses the first
qualifying current row and returns a native nested JSON object; missing
selected paths are omitted while null, empty, false, zero, arrays, and objects
remain distinct. Extrema preserve upstream comparison and strict first-tie
behavior. The live audit also proved that `stats row_max(a,b) result` is a
valid implicit alias, so two trailing words—not one—form the malformed case.
The fixture then contained 298 row-query cases, 308 error cases, and 335
statistics/pipeline cases: 941 cases total, all passing against the immutable
image.

`LQL-F37` adds fifteen row-query cases and six error cases for `seq(...)`.
They pin ordered non-overlapping matching, duplicate phrases, Unicode phrase
boundaries, quoted commas, empty/all-empty identity, one trailing comma,
field scope, missing fields, rich numeric projection, case-insensitive
function names, bare-name word behavior, logical/pipeline composition, and
strict separator/wildcard/termination errors. Source audit covers
`filter_sequence.go`, parser and filter tests, `filter_phrase.go`, and the
shared tokenizer at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture now contains 313
row-query cases, 314 error cases, and 335 statistics/pipeline cases: 962 cases
total, all passing against the immutable image. The fixture now contains 962 cases.

`LQL-F38` adds fourteen row-query cases and five error cases for query-backed
`in`, `contains_any`, and `contains_all`. They pin exact `fields` and `uniq`
output, duplicate removal, numeric and missing-value textual projection,
empty-subquery identities, nested lists, logical and current-row pipeline
composition, case-insensitive function names, subquery pipeline order/limit,
and strict one-exact-field output grammar. Source audit covers `in_values.go`,
`filter_in.go`, `filter_generic.go`, `parser.go`, and `storage_search.go` at
immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. Timeless retains native rich
JSON rather than flattened string columns, but uses the established compact
textual projection only while materializing query values. The fixture now
contains 327 row-query cases, 319 error cases, and 335 statistics/pipeline
cases: 981 cases total, all passing against the immutable image. The fixture now contains 981 cases.

`LQL-F41` adds fourteen row-query cases and eight error cases for
`equals_common_case(...)` and `contains_common_case(...)`. They pin
whole-string Go-simple uppercase expansion, every independent lowercase
combination of input Unicode `Lu` runes, deduplication, the ten-uppercase-rune
limit, exact versus phrase-boundary matching, Turkish uppercase-I, titlecase
and normalization edges, empty-list and empty-phrase behavior, trailing
commas, quoted commas and stars, logical/current-row composition,
case-insensitive function names, and strict separators/wildcards. The first
live run corrected an inferred wildcard rule: unlike `in` and `contains_any`,
these functions use `parseArgsInParens`, so an unquoted `*` is invalid and a
quoted `"*"` is literal. Source audit covers
`filter_equals_common_case.go`, `filter_contains_common_case.go`,
`filter_phrase.go`, `filter_contains_any.go`, `parser.go`, and their tests at
immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture now contains 341
row-query cases, 327 error cases, and 335 statistics/pipeline cases: 1,003
cases total, all passing against the immutable image. The fixture now contains 1003 cases.

`LQL-P17` adds three exact row-query cases, one repeated stochastic case, and
eight error cases for `sample N`. The exact cases pin `sample 1`, quoted
unsigned values, base-zero integers, case-insensitive syntax, field
composition, and unchanged selected rows. The stochastic case performs 96
independent `sample 4` requests and requires valid duplicate-free source
subsets, request-to-request cardinality variation, and an aggregate selection
rate within the deliberately broad `[0.18,0.32]` non-flaky envelope around
`1/4`; random row identity is not falsely represented as a fixed oracle
result. Error cases pin missing, zero, negative, fractional, invalid-octal,
non-numeric, extra, and parenthesized values. Source audit covers
`pipe_sample.go`, `pipe_sample_test.go`, `parser.go`, and the public LogsQL
documentation at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture now contains 344
row-query cases, one stochastic case, 335 error cases, and 335
statistics/pipeline cases: 1,015 cases total, all passing against the immutable
image. The fixture now contains 1015 cases.

`LQL-P25` adds ten exact pipeline-result cases and eight error cases for
`hash`. They pin seed-zero xxHash64 masked to 53 exact float bits; decimal
string output; optional parentheses and `as`; default and empty-alias `_msg`;
case-insensitive syntax; sequential overwrite; quoted fields; missing, null,
empty, numeric, boolean, compact-array, and flattened object-parent textual
projection; and strict source, wildcard, arity, closing-parenthesis, and tail
errors. Source audit covers `pipe_hash.go`, `pipe_hash_test.go`,
`pipe_len.go`'s shared field parser, and parser tests at immutable
VictoriaLogs commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture
now contains 344 row-query cases, one stochastic case, 343 error cases, and
345 statistics/pipeline cases: 1,033 cases total, all passing against the
immutable image. The fixture now contains 1033 cases.

`LQL-P26` adds twelve exact pipeline-result cases and ten error cases for
`collapse_nums`. They pin the default and exact quoted target fields;
case-insensitive keywords; optional empty or logical conditions; strict
condition/target/prettify order; decimal, hexadecimal, underscore, version,
duration, embedded-token, and non-ASCII boundary behavior; ordered UUID,
IPv4, time, date, datetime, fractional-second, and timezone prettification;
typed number/boolean/compact-array projection; sequential composition; and
strict attached-suffix, wildcard, missing-argument, and trailing-token errors.
Source audit covers `pipe_collapse_nums.go`, `pipe_collapse_nums_test.go`,
`pipe_update.go`, `if_filter.go`, `tokenizer.go`, parser tests, and public
documentation at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture now contains 344
row-query cases, one stochastic case, 353 error cases, and 357
statistics/pipeline cases: 1,055 cases total, all passing against the immutable
image. The fixture now contains 1055 cases.

`LQL-P27` adds eight exact pipeline-result cases and six error cases for
`decolorize`. They pin the default and empty quoted `_msg` aliases; exact
quoted and dotted fields; case-insensitive syntax; sequential composition;
parameter, intermediate, and optional final CSI byte classes; incomplete CSI
removal; invalid-final preservation; unchanged OSC sequences; and native
typed-value preservation when textual projection contains no CSI. Error cases
pin wildcard, prefix-wildcard, parenthesized, comma-separated, extra-token,
and attached-suffix syntax. Source audit covers `pipe_decolorize.go`,
`pipe_decolorize_test.go`, `color_sequence.go`, `color_sequence_test.go`,
`pipe_update.go`, and the public LogsQL documentation at immutable
VictoriaLogs commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture
now contains 344 row-query cases, one stochastic case, 359 error cases, and
365 statistics/pipeline cases: 1,069 cases total, all passing against the
immutable image. The fixture now contains 1069 cases.

`LQL-P31` adds eleven exact pipeline-result cases and eleven error cases for
`split`. They pin default-message and explicit source/destination forms;
optional `from`/`as` keywords and shorthand; case-insensitive syntax; literal
non-overlapping multi-byte separators; leading, trailing, and consecutive
empty pieces; missing separators; empty-source behavior; Unicode-scalar
empty-separator behavior; a quoted separator named `from`; flattened
number/boolean/null/missing/array projection; source preservation; sequential
`json_array_len` composition; and VictoriaLogs' exact JSON-string escaping.
Error cases pin missing separator/source/destination, wildcard and prefix
fields, comma-separated near misses, parenthesized call syntax, and attached
suffixes. Source audit covers `pipe_split.go`, `pipe_split_test.go`,
`parser.go` compound-token behavior, `stats_uniq_values.go` JSON-array
marshalling, parser tests, and the public LogsQL documentation at immutable
VictoriaLogs commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture
now contains 344 row-query cases, one stochastic case, 370 error cases, and
376 statistics/pipeline cases: 1,091 cases total, all passing against the
immutable image. The fixture now contains 1091 cases.

`LQL-P35` adds twelve exact pipeline-result cases and eight error cases for
`pack_logfmt`. They pin default `_msg`, explicit and bare destinations,
terminal `as`, pre-write destination snapshots, exact/prefix/empty/all field
selection, missing/null/empty/number/boolean/array/nested textual projection,
whitespace/quote/backslash/control escaping, exact `\u003c` and `\u0027`
wire spelling, case-insensitive keywords, flattened nested prefixes, and
upstream repeated output for overlapping selectors. Error cases pin required
parentheses and comma separation, leading empty fields, second destinations,
wildcard/prefix destinations, and attached suffixes. Source audit covers
`pipe_pack_logfmt.go`, `pipe_pack_logfmt_test.go`, `pipe_pack.go`, `rows.go`,
`rows_test.go`, `parser_utils.go`, and public LogsQL documentation at
immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture now contains 344
row-query cases, one stochastic case, 378 error cases, and 388
statistics/pipeline cases: 1,111 cases total, all passing against the immutable
image. The fixture now contains 1111 cases.

`LQL-P37` adds thirteen exact pipeline/statistics cases and ten error cases for
`unpack_logfmt`. They pin default `_msg`, bare and explicit `from` sources,
matching and nonmatching conditions, exact/missing/prefix/empty/all selection,
result prefixes, pre-write source snapshots, last-duplicate-wins behavior,
default/keep-original/skip-empty writes, case-insensitive and quoted grammar,
unquoted values, double/single/backtick quoting, Go control/hex/Unicode/octal
escapes, raw carriage-return removal, lone names, and malformed-quote fallback
to unquoted parsing. Error cases pin missing conditions and sources, wildcard
sources, malformed field lists, missing prefixes, conflicting preservation
modifiers, attached suffixes, and trailing tokens. Source audit covers
`pipe_unpack_logfmt.go`, `pipe_unpack_logfmt_test.go`, `pipe_unpack.go`,
`logfmt_parser.go`, and the public LogsQL documentation at immutable
VictoriaLogs commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture
now contains 155 source rows, 344 row-query cases, one stochastic case, 388
error cases, and 401 statistics/pipeline cases: 1,134 cases total, all passing
against the immutable image. The fixture now contains 1134 cases.

`LQL-P38` adds eleven exact pipeline/statistics cases and ten error cases for
`unpack_syslog`. They pin default `_msg`, bare and explicit sources, leading
whitespace trimming, matching and nonmatching conditions, case-insensitive
keywords, signed duration offsets, result prefixes, pre-write snapshots,
default and keep-original writes, optional and invalid PRI, facility/severity
mapping, lexical RFC5424 headers and structured-data escaping, classic and
RFC3339 RFC3164 forms, CEF base/extensions, CEE textual flattening and null
omission, missing-source no-ops, and invalid CEF fallback. Error cases pin
missing conditions/sources/offsets/prefixes, wildcard and prefix sources,
out-of-order clauses, attached suffixes, and trailing tokens. Source audit
covers `pipe_unpack_syslog.go`, `pipe_unpack_syslog_test.go`,
`syslog_parser.go`, `syslog_parser_test.go`, `pipe_unpack.go`,
`logfmt_parser.go`, `json_parser.go`, and the public LogsQL documentation at
immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture now contains 162
source rows, 344 row-query cases, one stochastic case, 398 error cases, and
412 statistics/pipeline cases: 1,155 cases total, all passing against the
immutable image. Evaluation-year-dependent RFC3164 leap-day normalization is
additionally pinned directly against the audited Go `time.Date` behavior in
the Rust parser regression rather than encoded as a date-stale oracle row.
The fixture now contains 1155 cases.

`LQL-P39` adds nine exact pipeline/statistics cases and nine error cases for
`unpack_words`. They pin default `_msg`, bare and explicit source/destination
fields, in-place source snapshots, first-seen duplicate removal, missing and
punctuation-only inputs, quoted fields, case-insensitive keywords, numeric and
array textual projection, and the exact Unicode Letter/Decimal_Number/
underscore token rule. Letter_Number, Other_Number, and combining marks are
separators. Error cases pin missing and wildcard source/destination fields,
prefix fields, comma-separated fields, attached suffixes, tokens after
`drop_duplicates`, and trailing text. Source audit covers
`pipe_unpack_words.go`, `pipe_unpack_words_test.go`, `tokenizer.go`, and the
public LogsQL documentation at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. Two adjacent `LQL-F09` cases
pin Letter_Number and Other_Number as word separators for ordinary filters,
closing a shared-boundary regression exposed by this audit. The fixture now
contains 166 source rows, 346 row-query cases, one stochastic case, 407 error
cases, and 421 statistics/pipeline cases: 1,175 cases total, all passing
against the immutable image. The fixture now contains 1175 cases.

`LQL-P40` adds sixteen exact pipeline/statistics cases and eight error cases
for `json_array_concat`. They pin the optional delimiter; default `_msg`;
bare and explicit source/destination forms; source snapshots; string, mixed,
nested, empty, malformed, scalar, object, and missing input; surrounding JSON
whitespace; decoded string escapes; bare `NaN`; case-insensitive keywords;
quoted fields; raw numeric spelling; object key order; and nested JSON escape
spelling. Missing operands, wildcard or prefix sources/destinations, attached
suffixes, and trailing tokens fail. Source audit covers
`pipe_json_array_concat.go`, `pipe_json_array_concat_test.go`, and the shared
`unpackJSONArray` implementation in `pipe_unroll.go` at immutable
VictoriaLogs commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The upstream
processor tests prove that empty/nonarray/missing sources assign an empty
string; the live streaming JSON API omits that empty-valued column. Timeless
records this as an explicit response-encoding boundary and returns the empty
string to preserve its richer missing/null/empty model. The fixture now
contains 166 source rows, 346 row-query cases, one stochastic case, 415 error
cases, and 437 statistics/pipeline cases: 1,199 cases total, all passing
against the immutable image. The fixture now contains 1199 cases.

`LQL-P42` adds fourteen exact pipeline/statistics cases and ten error cases
for `unroll`. They pin case-insensitive optional `if`/`by` grammar; bare,
parenthesized, quoted, and dotted exact fields; parenthesized trailing commas;
decoded top-level strings; compact nested values; longest-array zip with
empty padding; one empty row for empty, missing, malformed, and scalar input;
false-condition pass-through; and later statistics composition. Raw number
spelling, object order, and bare `NaN` survive, while nested JSON string
escapes are decoded and re-encoded (`"\\u0061"` becomes `"a"`), which is an
intentional difference from `json_array_concat`. Missing fields, wildcards,
prefixes, unparenthesized trailing commas, malformed conditions, attached
suffixes, and trailing tokens fail. Source audit covers `pipe_unroll.go` and
`pipe_unroll_test.go` at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The fixture now contains 167
source rows, 346 row-query cases, one stochastic case, 425 error cases, and
451 statistics/pipeline cases: 1,223 cases total, all passing against the
immutable image. The fixture now contains 1223 cases.

`LQL-P43` adds twelve exact pipeline/statistics cases and fourteen error cases
for `join`. They pin optional case-insensitive `by`/`on`; one or more exact
keys; inline `rows(...)` and query-backed right rows; default left and optional
inner behavior; duplicate right-match expansion in source order; right
join-key removal; missing/null/empty textual-key equivalence; nonempty-left
collision precedence; empty-left filling; prefixes; empty inline sources;
modifier order; and strict failures for empty/wildcard keys, missing or
malformed sources, invalid inline rows, missing prefixes, repeated modifiers,
and attached suffixes. Source audit covers `pipe_join.go`,
`pipe_join_test.go`, `pipe_unpack.go`, and the storage join regressions at
immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. Timeless retains typed and
nested right values instead of VictoriaLogs's flattened strings; quoted
dotted names remain literal while unquoted dotted names address nested paths;
and scalar-parent conflicts fail explicitly. Those retained-model choices are
covered separately through the real extension. The fixture still contains
167 source rows, 346 row-query cases, one stochastic case, 439 error cases,
and 463 statistics/pipeline cases: 1,249 cases total, all passing against the
immutable image. The fixture now contains 1249 cases.

`LQL-P44` adds nine exact pipeline/statistics cases and nine error cases for
`union`. They pin inline and query-backed sources, duplicate preservation,
empty `rows()` identity, empty inline-object omission, recursive sources,
later statistics, case-insensitive `union`/`rows`, `:`/`=` inline fields, and
query-backed statistics as ordinary rows. Missing, empty, invalid,
unterminated, trailing, nonscalar-inline, and attached-suffix forms fail.
Source audit covers `pipe_union.go`, `pipe_union_test.go`, shared
`parseRows`, and the storage union regression at immutable VictoriaLogs
commit `46a54c976fa3d404396050e8a5ee6c5b0320efc5`.

The source processor forwards its input and appends the union source during
flush, but the live multi-worker HTTP response can expose query/inline rows in
either order when no later `sort` is present. Oracle cases therefore sort
when row identity differs instead of claiming an upstream response-order
guarantee. Timeless's single-owner evaluator deliberately provides stable
left-then-source order. VictoriaLogs also emits no response row for inline
`{}` because it has no fields. The fixture now contains 167 source rows, 346
row-query cases, one stochastic case, 448 error cases, and 472
statistics/pipeline cases: 1,267 cases total, all passing against the
immutable image. The fixture now contains 1267 cases.

`LQL-P45` adds seventeen exact pipeline cases and sixteen error cases for
`running_stats`. They pin the upstream pipe's implicit lexical `_time` order;
whole-row, exact-field, and prefix-field counts; numeric sum; natural min/max;
independent `by (...)` and shorthand `(...)` groups; case-insensitive pipe
spelling; `first`/`last` offsets; canonical omitted aliases; subsequent
pipeline visibility; result-field overwrite; empty input; initial `NaN`;
rejection of textual `NaN` and infinities as sum inputs after finite state;
and strict function, group, field-filter, alias, offset, comma, tail, and
termination errors. Source audit covers `pipe_running_stats.go`, all six
`running_stats_*.go` processors, their unit tests, and the checked language
reference at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`.

`LQL-S14` catalogs the five documented functions from that same
`running_stats` surface: `count`, `last`, `max`, `min`, and `sum`. The
immutable reference contains no separate `running_count`, `running_last`,
`running_min`, `running_max`, or `running_sum` grammar. Consequently the P45
source audit and live cases are also the S14 oracle; a second parser or
duplicate oracle corpus would describe behavior that VictoriaLogs does not
have. The source parser's additional `first` support remains covered under
P45. The catalog reconciliation re-ran the current complete 1,388-case live
corpus successfully; the 1,300-case count below records the earlier point at
which P45 itself entered the fixture.

The source materializes every input row, partitions by textual group keys,
sorts groups by their encoded key and rows by the formatted `_time` string,
and updates `count`, `sum`, `min`, `max`, `first`, and `last` state before
forwarding each row.
The public reference promises time order inside a group but does not promise
cross-group order. Missing exact fields project as empty text; nonnumeric sums
are skipped and remain `NaN` until a finite number appears; textual `NaN`,
`+Inf`, and `-Inf` are likewise skipped. The pinned source's string
comparison is not chronological when RFC3339 fractional seconds have different
printed widths: `...000031Z` sorts before `...00003Z`. The upstream fixture pins
that observed behavior; Timeless selects numeric microsecond order and records
the intentional compatibility deviation in the P45 storage finding. The
fixture now contains 167 source rows, 346 row-query cases, one stochastic
case, 464 error cases, and 489 statistics/pipeline cases: 1,300 cases total,
all passing against the immutable image. The fixture now contains 1300 cases.

`LQL-P46` adds thirteen exact pipeline cases and sixteen error cases for
`total_stats`. They establish that the pipe uses the same strict group,
function, field-selector, offset, and alias grammar as `running_stats`, but
first consumes the complete bounded group and then writes the same final
count, sum, natural min/max, or time-relative first/last value onto every row
in that group. The cases cover whole-row, exact, and prefix counts; grouped
and shorthand forms; case-insensitive spelling; canonical aliases; later-pipe
visibility; destination overwrite; empty input; initial `NaN`; skipped
nonfinite sum input; and every corresponding malformed form.

Source audit covers `pipe_total_stats.go`, the shared
`pipe_running_stats.go`, all six shared state processors, their unit tests,
and the checked language reference at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The implementation sets an
explicit total mode, sorts and partitions exactly as `running_stats`, updates
each accumulator across the complete group before emitting any row, and then
reuses the final state for every output. It therefore shares the upstream
formatted-time ordering defect and the same unspecified cross-group order;
Timeless retains the P45 numeric-microsecond compatibility correction. The
fixture now contains 167 source rows, 346 row-query cases, one stochastic
case, 480 error cases, and 502 statistics/pipeline cases: 1,329 cases total,
all passing against the immutable image. The fixture now contains 1329 cases.

`LQL-S15` catalogs the six documented functions from this same `total_stats`
surface: `count`, `first`, `last`, `max`, `min`, and `sum`. The immutable
reference contains no separate `total_count`, `total_first`, `total_last`,
`total_min`, `total_max`, or `total_sum` grammar. Consequently the P46 source
audit and live cases are also the S15 oracle; a duplicate parser or corpus
would invent an upstream surface. The catalog reconciliation re-ran the
current complete 1,388-case live corpus successfully; the 1,329-case count
above records the earlier point at which P46 itself entered the fixture.

`LQL-P47` adds six exact pipeline cases and six error cases for `time_add`.
They pin compound and negative durations, nanosecond preservation, timezone
normalization, zone-less UTC behavior in the immutable oracle image, the SQL
space separator, trimmed RFC3339Nano output, invalid-string no-op behavior,
default `_time` composition, and strict missing/invalid/leading-plus/trailing
grammar failures. Timeless' retained native-type behavior is tested against
the real extension because VictoriaLogs stores log field values textually.

Source audit covers `pipe_time_add.go`, `values_encoder.go` timestamp and
duration parsing/formatting, `parser.go` saturating subtraction, their unit
tests, and the checked language reference at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. The implementation changes only
parseable current-row strings, applies the offset in signed nanoseconds, and
writes canonical UTC while leaving durable storage untouched. VictoriaLogs
uses its local process timezone for zone-less values; Timeless fixes that
environmental input to UTC, matching the pinned image and making embedded
behavior deterministic. The fixture remains 167 source rows and now contains
346 row-query cases, one stochastic case, 486 error cases, and 508
statistics/pipeline cases: 1,341 cases total, all passing against the
immutable image. The fixture now contains 1341 cases.

`LQL-P48` adds eight exact pipeline cases and eight error cases for
`generate_sequence`. They pin input independence, last-generator-wins
replacement of every preceding filter and pipe, quoted positive fractional
truncation, underscore/scientific/duration number spellings, decimal-string
output, later math/filter/offset/limit/projection composition, and strict
missing/zero/sub-one/negative/leading-plus/nonnumeric/trailing/attached forms.

Source audit covers `pipe_generate_sequence.go`, its parser and unit tests,
the shared `parseNumber` implementation, and the checked language reference at
immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. Upstream parses the count as
`float64`, requires it to be at least one, converts it to `uint64`, cancels its
input on the first block, and creates `_msg` strings from zero through `N-1`
during flush. Consequently a positive fraction truncates, earlier filters and
limits cannot suppress generation, and only the last generator matters. The
fixture remains 167 source rows and now contains 346 row-query cases, one
stochastic case, 494 error cases, and 516 statistics/pipeline cases: 1,357
cases total, all passing against the immutable image. The fixture now contains 1357 cases.

`LQL-S12` adds eight exact statistics/pipeline cases and nine error cases for
`json_values`. They pin the statistics form and standalone shorthand;
case-insensitive `sort`/`order`, optional `by`, per-field direction, and
natural ordering; bounded top-k; quoted and zero limits; explicit and default
aliases; selector deduplication; empty-argument all-field selection; one `{}`
for every row missing the requested fields; and one JSON-array string even for
empty input. Malformed parentheses, separators, sort fields, limit spellings,
clause order, and attached command suffixes fail explicitly.

Source audit covers `stats_json_values.go`, `stats_json_values_sorted.go`,
`stats_json_values_topk.go`, their parser and unit tests, and the shared
natural-sort and limit helpers at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. Upstream serializes selected
column values textually, omits missing fields, treats a missing sort value as
empty text, and does not promise cross-block tie or unsorted merge order. A
positive limit uses bounded top-k for sorted results; zero means no
operator-specific limit. Timeless keeps those language semantics while
preserving retained native JSON types and nesting and strengthening equal-key
ties to deterministic public-row order. Those retained-model choices are
tested through the real extension rather than falsely attributed to the
textual upstream store. The fixture remains 167 source rows and now contains
346 row-query cases, one stochastic case, 503 error cases, and 524
statistics/pipeline cases: 1,374 cases total, all passing against the
immutable image. The fixture now contains 1374 cases.

`LQL-S13` adds six exact statistics/pipeline cases and eight error cases for
`histogram`. They pin native/text numeric bucketing, zero and sub-`1e-9`
lower-bound behavior, exact internal boundaries, the `1e18`/infinity upper
bucket, ignored negative/NaN values, duration and byte-size parsing,
IPv4/timestamp exclusion, hit aggregation, empty input, standalone shorthand,
canonical case-insensitive aliases, and strict field arity, wildcard, limit,
suffix, and trailing-token failures.

Source audit covers `stats_histogram.go`, its parser and processor tests, the
pinned `github.com/VictoriaMetrics/metrics` histogram implementation, and the
shared number/natural-sort helpers at immutable VictoriaLogs commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5`. Upstream uses 18 logarithmic
buckets per decimal decade from `1e-9` through `1e18`, plus fixed lower and
upper buckets; intervals are lower-exclusive/upper-inclusive except that the
first middle bucket includes exact `1e-9`. It ignores negative and NaN values,
emits only nonempty buckets, naturally sorts their `vmrange` labels, and
returns one string containing a JSON array with native unsigned hit counts.
Timeless preserves those semantics while reading native numbers and rich
nested current-row values directly. The fixture remains 167 source rows and
now contains 346 row-query cases, one stochastic case, 511 error cases, and
530 statistics/pipeline cases: 1,388 cases total, all passing against the
immutable image. The fixture now contains 1388 cases.

`LQL-F35` adds four accepted row-query witnesses for VictoriaLogs stream
selectors. The harness already declares `case` as its sole stream field at
ingestion, so bare `{case="phrase-exact"}`, the equivalent `_stream:{...}`
form, regular-expression matching, static `in(...)`, and `or` composition
exercise the actual stream index rather than an ordinary log-field shortcut.
Source audit at immutable commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5` covers `log_rows.go`,
`stream_tags.go`, `stream_id.go`, `stream_filter.go`, `indexdb.go`, and
`storage_search.go`: nonempty configured fields are canonically sorted and
hashed, the ID is tenant-scoped, and the stream filter is applied before the
ordinary filter. Timeless intentionally defers this row because it does not
store that ingestion-owned identity. The fixture remains 167 source rows and
now contains 350 row-query cases, one stochastic case, 511 error cases, and
530 statistics/pipeline cases: 1,392 cases total, all passing against the
immutable image. The fixture now contains 1392 cases.

`LQL-F36` adds six accepted row-query witnesses and two parser-error witnesses
for the reserved `_stream_id` filter. A one-row immutable-image probe derived
`case="phrase-exact"` as
`00000000000000000b1801f1dd598ec73134b3ae3165d0fd`; the checked fixture then
proves that exact and quoted forms select that stream, static `in(...)`
ignores a valid-but-absent ID, and query-backed `in(...)` consumes projected
IDs. Separate witnesses pin ASCII-case-insensitive unquoted field spelling,
whitespace before `:`, the required 48-hex shape, and rejection of generic
empty equality. Source audit at commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5` covers `stream_id.go`,
`filter_stream_id.go`, `parser.go`, `block_header.go`, and
`storage_search.go`: the first 8 bytes encode tenant account/project, the
remaining 16 bytes are the canonical stream hash, blocks are ordered by ID,
and exact/static/query-backed filters become block-search sets. Timeless
defers the row because it stores none of that identity. The fixture remains
167 source rows and now contains 356 row-query cases, one stochastic case,
513 error cases, and 530 statistics/pipeline cases: 1,400 cases total, all
passing against the immutable image. The fixture now contains 1400 cases.

`LQL-P49` adds eleven successful statistics/pipeline witnesses and eight parser
errors for `set_stream_fields`. They pin exact, trailing-prefix-wildcard, and
all-current-column selection; bytewise field sorting; empty-value omission;
Go string escaping; quoted field names containing spaces; visibility of prior
pipeline writes; direct and query-backed conditions; conditionally preserved
`_stream`/`_stream_id`; empty stream IDs on transformed rows;
ASCII-case-insensitive command spelling; required parenthesized conditions and
commas; and strict wildcard/command boundaries.
Source audit at immutable commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5` covers
`pipe_set_stream_fields.go`, its parser/processor tests, `stream_tags.go`, and
the shared field-filter/prefix helpers. The upstream pipe rewrites only result
columns: it sorts selected nonempty current values into canonical stream tags,
clears `_stream_id` on transformed rows, and preserves both current columns
when its condition is false. It does not mutate stored stream identity. The
fixture remains 167 source rows and now contains 356 row-query cases, one
stochastic case, 521 error cases, and 541 statistics/pipeline cases: 1,419
cases total, all passing against the immutable image. The fixture now contains 1419 cases.

`LQL-P50` adds five successful row-query witnesses and five parser errors for
`stream_context`. They pin before, after, zero-context, case-insensitive option
order, `time_window`, later projection, empty input, required context options,
numeric bounds, and invalid-window rejection. The fixture deliberately keeps
one row per declared stream: exact output confirms accepted grammar and the
selected row, while the immutable source audit proves the additional-read
semantics that Timeless cannot execute without stored stream identity.

Source audit at immutable commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5` covers
`pipe_stream_context.go`, its parser and storage-search tests,
`storage_search.go`, and the shared stream-ID parser. The processor requires
`_stream_id`, groups selected rows by it, derives the tenant from it, issues
additional exact-ID time-range queries, keeps bounded before/after heaps,
deduplicates overlaps, sorts contexts, and emits delimiter rows. The pipe must
be first after the filter; its maximum selected set is 100 streams and 1,000
rows per stream before the additional bounded state budget.

The fixture remains 167 source rows and now contains 361 row-query cases, one
stochastic case, 526 error cases, and 541 statistics/pipeline cases: 1,429
cases total, all passing against the immutable image. The fixture now contains 1429 cases.

`LQL-Q03` adds six successful row-query witnesses and six parser errors for
the leading `options(...)` resource controls. They pin positive and zero
`concurrency`, duplicate-last-wins behavior, independent `parallel_readers`,
combined whitespace/trailing-comma grammar, nested-query overrides, unsigned
integer values, required `=`, and the requirement for a query after options.

Source audit at immutable commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5` covers `parser.go`,
`storage_search.go`, `net_query_runner.go`, the parser/net-runner tests, and
the query-option documentation. `concurrency` bounds CPU pipe workers to the
available cores. `parallel_readers` controls storage readers, inherits
`concurrency` when absent, otherwise falls back to the configured/default
reader count, and is capped at 2,000. Both options propagate independently to
each storage node and therefore describe intra-query topology, not result-only
syntax.

The fixture remains 167 source rows and now contains 367 row-query cases, one
stochastic case, 532 error cases, and 541 statistics/pipeline cases: 1,441
cases total, all passing against the immutable image. The fixture now contains 1441 cases.

`LQL-Q04` adds eleven successful row-query witnesses and eight parser errors
for leading `options(time_offset=<duration>)`. They pin positive and negative
source-bound translation, sub-native and compound fractional durations,
quoted values, duplicate-last-wins and trailing-comma behavior,
case-insensitive `options` with a case-sensitive option name, shifted result
timestamps, leading-versus-later filter order, nested inheritance and explicit
replacement, day/week ranges, and strict malformed or missing values.

Source audit at immutable commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5` covers `parser.go`, query-option
tests, `storage_search.go`, time-filter initialization, and the query
optimizer. VictoriaLogs shifts storage bounds backward by `time_offset`, then
shifts returned `_time` values forward. Nested queries inherit the outer
offset unless they declare their own value, including explicit zero. Day and
week filters compose with the same offset. Consecutive leading `filter` pipes
are optimized into the storage predicate after option-bound rewriting, so
their time comparisons observe original source timestamps; later pipes
observe the shifted result time. A source-replacing `generate_sequence` has no
retained timestamp to shift.

The fixture remains 167 source rows and now contains 378 row-query cases, one
stochastic case, 540 error cases, and 541 statistics/pipeline cases: 1,460
cases total, all passing against the immutable image. The fixture now contains 1460 cases.

`LQL-Q05` adds thirteen successful row-query witnesses and nine parser errors
for leading `options(global_filter=(...))`. They pin conjunction with the base
filter, logical precedence, duplicate-last-wins and trailing-comma behavior,
case-insensitive `options` with a case-sensitive option name, inheritance into
query-backed membership, query-backed pipeline conditions, joins, and unions,
explicit nested replacement, wildcard identity, and strict filter-only
grammar. The time witnesses distinguish three scopes: a sibling
`time_offset` does not rewrite the global value declared beside it, a global
value may declare its own offset, and a nested global declaration inherits the
surrounding offset at the declaration point.

Source audit at immutable commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5` covers `parser.go`, the query
option and global-filter tests, query initialization, nested-query parsing,
and storage-filter construction. VictoriaLogs composes the global predicate
before the local predicate and initializes every nested query with the
surrounding option state. A nested explicit declaration replaces the inherited
global predicate rather than conjoining a second one. The global value is
parsed as a filter-only query: it cannot have a result pipeline, but
query-backed filters inside it receive the parent option context. Every
duplicate declaration is parsed and validated even though the final value is
retained.

The fixture remains 167 source rows and now contains 391 row-query cases, one
stochastic case, 549 error cases, and 541 statistics/pipeline cases: 1,482
cases total, all passing against the immutable image. The fixture now contains 1482 cases.
