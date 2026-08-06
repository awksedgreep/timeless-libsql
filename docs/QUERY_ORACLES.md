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
`LQL-F21` and `LQL-F22`, while query-backed lists remain `LQL-F38`. The
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
