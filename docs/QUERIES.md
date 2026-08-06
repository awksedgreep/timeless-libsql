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

`left:eq_field(right)`, `left:le_field(right)`, and
`left:lt_field(right)` compare two fields from the same bounded public log
row. An omitted left field selects the message. Field identifiers may be
quoted, function names are case-insensitive, and one trailing comma is
accepted. Message, level, service, and arbitrary dotted metadata are valid on
either side. `_time` is valid only on the right because a leading `_time:` is
reserved for the time-filter grammar. Logical expressions and ordered
`filter`/`where` pipelines compose these predicates; malformed arity,
separators, wildcards, and unterminated calls fail explicitly.

Both operands use the same non-mutating textual projection as other LogsQL
text filters. Missing and JSON null become empty text; strings retain their
bytes; numbers and booleans use compact JSON spelling; arrays and objects use
compact JSON only while the predicate runs; and a right-hand `_time` uses the
API's RFC3339 rendering in the table's configured timestamp unit. Equality is
exact textual equality, so `2` and `2.0` differ. Ordering first interprets
*both* projections as VictoriaLogs math values—decimal and base-zero numbers,
durations, byte sizes, RFC3339 timestamps, or IPv4 addresses—and otherwise
uses unsigned UTF-8 byte order. When both Timeless operands are retained JSON
numbers, exact JSON-number ordering takes precedence, preserving integers
beyond binary64 precision. A comparison with itself is therefore true for
`eq_field` and `le_field`, and false for `lt_field`, including when the field
is missing.

Executable `SQL-LOG-021` uses only public `logs` rows and JSON1. It is the
complete retained-model SQL equivalent for `eq_field` and exposes the exact
bytewise fallback for `le_field`/`lt_field`. Portable SQL is not labeled as a
complete ordering equivalent because the language-specific math-value parser
remains in the Rust API. VictoriaLogs flattens objects before filtering;
Timeless retains and can compact-project a selected parent without losing its
type. Both operands already cross the same decoded public row, so a new
extension primitive would not avoid storage reads, decode, allocation, copy,
or row crossing.

A field selector ending in `*` applies its filter to every existing canonical
field whose name begins with the text before the wildcard. `cmp_*:foo` searches
`cmp_left`, `cmp_right`, and any other matching leaf until one succeeds;
`*:foo` and `""*:foo` search every field. Prefixes may be quoted, including
`"foo:bar:"*:exact(needle)`. `_msg`, `_time`, and `level` participate under
their canonical names. Retained metadata objects contribute dotted leaf names,
such as `deployment.region`, while arrays and null remain existing leaf values
and object parents are not implicitly flattened into matchable values.

Each atomic predicate expands independently. Consequently,
`cmp_*:(bar AND foo)` may satisfy `bar` in `cmp_left` and `foo` in `cmp_right`;
`NOT` negates the completed any-field result. A `filter`/`where` pipeline
enumerates the current projected row, so a preceding `fields` operation can
remove candidates. Expansion uses a single recursive path and stops at the
first match instead of allocating a row-wide field list. It observes request
cancellation at each retained node and remains bounded by the already-decoded
row and the API's storage-work, body, response, and deadline limits.

Wildcard field comparisons fail explicitly. VictoriaLogs currently treats a
left operand such as `cmp_*:eq_field(right)` as one literal nonexistent field
rather than expanding it, which can accidentally match missing/null/empty
projections. Timeless selects the strict behavior instead of preserving that
footgun. `SQL-LOG-022` gives direct SQLite/libSQL users the executable public-
row field-set expansion for literal prefix selection and retained string/null
exactness. LogsQL parsing, word/phrase/range/rich-value semantics, RFC3339
`_time` projection, composition, limits, cancellation, and envelopes remain
in the Rust API. No extension primitive or storage-format change is involved.

`_time:day_range[start, end] offset duration` filters by a repeated UTC time-
of-day interval. Bounds accept `HH:MM` or `HHMM`; `[`/`]` include the exact
bound and `(`/`)` exclude it. The bounds are instants rather than minute
buckets, so a closed `12:00` includes exactly that timestamp and not the next
native tick. `24:00` clamps to the final nanosecond of the day, minute `60`
normalizes into the following hour, and an inverted range is valid but empty.
The VictoriaLogs special case `[00:00,00:00)` selects the full day; other equal
half-open ranges are empty. Overnight wrapping is not implicit.

The optional offset is a signed VictoriaLogs compound duration and is added to
UTC before comparison. Timeless deliberately uses UTC when it is omitted. It
does not read the server process's local timezone, so the same request cannot
change with deployment location or daylight-saving state. Use an explicit
fixed offset when local wall time is intended. A pipeline filter reads the
current projected `_time`, so a preceding `fields` pipe can remove it.

`SQL-LOG-023` gives direct SQLite/libSQL users the executable native timestamp
modulo and explicit-offset operation over bounded public rows, including open-
midnight normalization and millisecond/microsecond unit parameters. Clock and
duration grammar, logical/pipeline composition, errors, limits, cancellation,
and HTTP envelopes remain in the Rust API. The repeated daily predicate cannot
independently prune an arbitrary absolute time range, and ordinary SQL already
receives the timestamp, so no extension primitive or storage change is added.

`_time:week_range[start, end] offset duration` filters by a repeated UTC
weekday interval. Weekday names are case-insensitive and accept either short
(`Sun` through `Sat`) or full English spellings. Sunday is the beginning of
the linear range and Saturday is the end. Brackets are normalized before
comparison: an open start advances one weekday and an open end retreats one
weekday, both modulo seven. A resulting start above the end is valid and
empty; ranges do not otherwise wrap across the week boundary.

This preserves VictoriaLogs' edge cases. `[Sun,Sun)` and `(Sat,Sun)` normalize
to the full week, `[Mon,Mon]` selects Monday, and `[Mon,Mon)` is empty. The
optional signed compound offset is added to UTC before weekday selection.
Timeless uses deterministic UTC when it is omitted rather than inheriting the
server process's local timezone. Pipeline filters read the current projected
`_time`; removing that field earlier makes the predicate false.

`SQL-LOG-024` gives direct SQLite/libSQL users the executable public-row
operation with millisecond/microsecond unit parameters, Euclidean pre-epoch
day handling, signed multi-day offsets, and explicit normalized weekday
bounds. LogsQL weekday/bracket/duration grammar, logical and pipeline
composition, errors, limits, cancellation, and envelopes remain in the Rust
API. The predicate cannot independently prune an arbitrary absolute timestamp
window, and ordinary SQL already receives the timestamp, so no extension
primitive or storage-format change is added.

LogsQL line comments begin with `#` outside double-quoted, single-quoted, and
raw-backtick literals and continue through the next LF. CRLF input is accepted;
the CR belongs to the comment and the LF remains the query boundary. A comment
marker may follow a token without intervening whitespace. Hashes inside quoted
field names and values are literal bytes.

Queries may span lines anywhere ordinary grammar permits, including between
logical terms and pipeline stages. One optional terminal semicolon is accepted,
including immediately before a trailing comment. A semicolon before remaining
query text, multiple semicolons, a comment-only query, a dangling pipe, and a
comment that removes a required pipeline argument fail explicitly. Lexical
unterminated-quote and misplaced-semicolon errors include one-based source line
and Unicode-character column positions.

The Rust API scans source once with memory bounded by the request-body limit.
Ordinary one-line queries remain borrowed without a normalization copy; a copy
is made only when comments or a terminal semicolon must be replaced. Replacement
preserves byte offsets and line boundaries, and the normalized LogsQL source
never enters SQLite. Direct SQLite/libSQL users write ordinary parameterized SQL,
so `LQL-F40` has no separate SQL recipe or extension primitive.

`delete`, `del`, `drop`, and `rm` are case-insensitive aliases for the same
ordered row transform. They accept a comma-separated list of exact fields,
literal field prefixes ending in `*`, quoted field names, or the standalone
`*` that removes every field. The empty quoted field `""` is VictoriaLogs'
message alias and therefore removes `_msg`. A missing exact field is a no-op,
repeated deletion is idempotent, and later pipeline stages observe only the
remaining fields. If no fields remain, the row is omitted rather than emitted
as `{}`.

Unquoted dotted names traverse Timeless's retained rich objects. Quoted names
remain literal top-level keys even when they contain dots, commas, pipes, or
asterisks. Prefixes compare case-sensitive canonical dotted paths and recurse
through objects; arrays and scalars remain atomic and are removed only by
their complete field path. Removing the last child prunes its now-empty parent.
This preserves nested values without inventing VictoriaLogs' flattened
storage model. Malformed commas, separated wildcards, leading wildcards, and
embedded unquoted wildcards fail before storage work. Traversal is bounded by
the decoded row/work limits and observes request cancellation.

`SQL-LOG-025` gives direct SQLite/libSQL users an executable `json_remove`
projection for exact retained metadata paths. Direct SQL can omit `ts`,
`message`, or `level` from its projection, but LogsQL aliases, quoted/prefix
grammar, formatted `_time`, special-field deletion, recursive empty-parent
pruning, empty-row omission, composition, limits, cancellation, and envelopes
remain Rust API behavior. Public JSON1 already supplies the exact-path
foundation; no extension primitive or storage-format change is warranted.

Exact-build evidence over 8,192 retained rows measures exact plus nested-
prefix deletion at 4.011/45.768 ms narrow/wide p95. That is 16.9%/17.6%
above same-run word queries while response bytes are 22.4%/22.1% lower. Both
paths read exactly one/four blocks, decode 1,024/8,192 entries, and read
235,778/1,914,055 payload bytes. The cost is bounded row mutation after the
same public decode, not storage amplification.

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

## Request-local log query statistics

Direct SQLite/libSQL callers can inspect the actual work of one public log
scan without subtracting cumulative process counters. Run both statements on
the same connection and fully consume the first result:

```sql
SELECT ts, level, message, metadata
  FROM logs
 WHERE service = :service
   AND ts >= :start_us AND ts <= :end_us
   AND max_work_entries = :max_work_entries
 ORDER BY ts;

SELECT query_total_ns, payload_bytes_read,
       candidate_blocks, processed_blocks,
       decoded_entries, processed_entries,
       matched_entries, returned_entries,
       values_read, timestamps_read
  FROM timeless_log_query_stats('logs');
```

The report is connection- and table-scoped and is consumed exactly once. A
new, failed, or cancelled scan clears an older report; a second read or fresh
connection fails explicitly. Six additional columns expose snapshot and
materialization timing, copied snapshot bytes, blocks skipped by an ordered
bound, buffered entries examined, and whether the snapshot used stable SQLite
locations. See executable [`SQL-LOG-026`](QUERY_SQL_EQUIVALENTS.md#sql-log-026-request-local-log-query-statistics)
for the complete schema and the fourteen-field LogsQL `query_stats` mapping.

Timeless codecs read one complete rich block payload rather than separately
addressable field-column files. Payload, block, entry, and logical-slot
counters therefore describe actual Timeless work; they do not pretend to be
VictoriaLogs per-column byte accounting. The Rust LogsQL API owns query
grammar, typed post-filter `RowsFound`, pipeline duration/composition, string
result values, limits, cancellation, and HTTP envelopes.

## Bounded LogsQL `first` and `last`

The Rust logs API implements the VictoriaLogs-compatible pipeline forms:

```text
* | first
* | first 10 by (status desc, _time)
* | first 3 by (duration, _time) partition by (service, host)
    rank as position
* | last 3 by (duration, _time) partition by (service, host)
    rank as position
```

`N` defaults to one and must be positive. `by` is optional before the
parenthesized exact-field list; each field may specify `asc` or `desc`.
`partition [by] (...)` creates independent groups, and `rank [as] field`
inserts a one-based string rank that restarts in every partition (`rank`
defaults the field name to `rank`). Empty field lists and a trailing comma are
accepted where the pinned upstream grammar accepts them. Wildcard fields,
invalid counts, missing names, and trailing tokens fail before storage work.

`last` accepts exactly the same grammar and returns the reverse of `first`'s
complete order. A per-field `desc` modifier reverses that field first, and the
operation-wide `last` direction reverses it again. Results are emitted from
last to first, and rank one names the first emitted row in each partition.

Missing and JSON null project to empty text. Sort coercion follows pinned
VictoriaLogs order: exact signed integer, exact unsigned integer, RFC3339
timestamp, numeric/duration/byte value, then natural UTF-8 byte order. Sort
directions apply per field. Partition keys use length-framed textual values
and partitions have deterministic encoded-key order. Equal sort keys use the
original public-row order as Timeless's stable tie-break because upstream does
not promise an equal-key order. With no `by` fields, `first` observes the
current pipeline schema: a preceding `fields` or `delete` changes the encoded
row used for comparison. Timeless preserves numbers, booleans, arrays,
objects, nulls, and nested metadata in its response rather than flattening
them to strings.

The operation consumes only bounded public `timeless_logs` rows. Input and
output are capped by `max_work_rows` and `max_result_rows`; sort keys,
partitions, indexes, and selected rows are charged to the existing
`max_response_bytes` memory budget. Cancellation is observed while keys are
built, comparisons run, and output is assembled. No private table or new
extension primitive is used.

Direct SQLite/libSQL users can implement bounded per-partition numeric
selection with `row_number()` over public rows. Executable
[`SQL-LOG-027`](QUERY_SQL_EQUIVALENTS.md#sql-log-027-first-numeric-rows-per-partition)
includes the parameterized statement, timestamp units, order, missing/null,
rank-type, and result-bound contract. It deliberately does not claim full
LogsQL natural collation or exact cross-type integer coercion from ordinary
SQLite `REAL`.
Executable
[`SQL-LOG-028`](QUERY_SQL_EQUIVALENTS.md#sql-log-028-last-numeric-rows-per-partition)
provides the corresponding descending window-rank statement and documents the
same boundary.

## Bounded LogsQL `top`

The Rust logs API implements frequency ranking over the current pipeline row:

```text
* | top by (service)
* | top 5 service, level hits as total rank as position
* | filter level:=error | top 10 by (service) rank
```

The default limit is ten. `by` is optional; fields may be a parenthesized or
bare comma-separated exact list. `hits [as] field` renames the required string
hit count, while `rank [as] field` adds a one-based string rank. Default or
explicit result names gain trailing `s` characters until they no longer
collide with a selected field. Commands and modifiers are case-insensitive;
zero/fractional limits, empty/wildcard fields, missing names, unseparated
fields, and trailing tokens fail before storage work.

Every selected value uses the LogsQL textual projection. Missing, JSON null,
and empty strings therefore share one empty group; its selected field is
omitted from response JSON while hits/rank remain. Strings are unquoted and
numbers, booleans, arrays, and objects use their retained textual forms. A
multi-field vector is framed structurally, so different tuples cannot collide.
Groups order by hits descending and then projected key ascending. The output
is a summary and does not mutate or flatten stored rich metadata.

Input rows and unique groups are bounded by `max_work_rows`; retained keys and
group/sort state are charged to `max_response_bytes`; the requested output is
bounded by `max_result_rows`; and cancellation is checked during grouping,
sorting, and assembly. The operation reads only the public log rowset.

Executable
[`SQL-LOG-029`](QUERY_SQL_EQUIVALENTS.md#sql-log-029-top-values-by-hit-count)
provides the single-field public `GROUP BY`, deterministic ordering, and
window-rank equivalent. Multi-field query grammar, current-row composition,
name collision policy, limits, cancellation, and HTTP envelopes remain API
behavior. No new extension primitive or private storage access is required.

## Bounded LogsQL `uniq` and `facets`

`uniq` emits one textual row for each structural key selected from exact
current-row fields. `facets` instead discovers every current-row field and
emits its most frequent nonempty textual values:

```text
* | uniq service, level with hits limit 20
* | fields service, level, context | facets 5
* | facets max_values_per_field 1000 max_value_len 128 keep_const_fields
```

For `uniq`, optional `filter` is a case-sensitive single-field substring,
`hits` is optional, zero means no language-specific limit, and positive-limit
overflow resets retained hits to string `"0"`. Missing, null, and empty share
one empty key component; the empty response field is omitted. Timeless selects
structural keys bytewise so tuples cannot collide and results are repeatable
even though VictoriaLogs does not promise its hash-map subset or order.

For `facets`, the defaults are ten results per field, at most 1,000 unique
textual values tracked per field, and at most 128 UTF-8 bytes per value. Empty
values are ignored. A field with any longer value or excessive cardinality is
omitted entirely. A single value appearing in every selected row is omitted
unless `keep_const_fields` is present. Objects become dotted leaves and arrays
remain atomic JSON text. Results are deterministic by field name, hits
descending, and bytewise value. Modifiers are case-insensitive, reorderable,
and repeatable; matching VictoriaLogs v1.52.0, positive fractions are
truncated before use.

Both operations preserve rich stored rows and enforce hard input/state/result/
response limits plus cancellation. Executable
[`SQL-LOG-030`](QUERY_SQL_EQUIVALENTS.md#sql-log-030-unique-textual-values)
provides direct SQLite/libSQL grouping for `uniq`.
[`SQL-LOG-031`](QUERY_SQL_EQUIVALENTS.md#sql-log-031-bounded-facets-over-public-log-fields)
provides recursive JSON1 field discovery, canonical `_time`/`_msg`/`level`
projection, cardinality/length/constant exclusion, and per-field window ranks
for `facets`. Neither operation requires a new extension primitive or private
storage access.

## LogsQL `coalesce` over rich current rows

`coalesce` writes the first nonempty textual source value to an exact
destination field:

```text
* | coalesce(trace_id, request_id) default unknown as correlation_id
* | coalesce(context.*, service) as context.primary
* | fields error, message | coalesce(error, message)
```

Sources are parenthesized and may be exact fields, `*`, or suffix-star prefix
filters. Source filters are evaluated left to right and expanded names are
de-duplicated. Missing, JSON null, empty strings, and exact rich-object parents
are skipped; object leaves participate through dotted names. Strings remain
unquoted, numbers and booleans become text, and arrays remain one compact JSON
text value. Wildcard expansion is deterministic by bytewise flattened field
name. A trailing source comma is accepted to match VictoriaLogs.

The destination defaults to `_msg`. `default value` supplies a textual value
when no source is nonempty, and `as field` chooses an exact destination.
Timeless retains an explicitly empty destination in the rich JSON result;
VictoriaLogs omits empty-valued columns when serializing streams. If a nested
destination would replace a retained scalar parent, Timeless returns HTTP 422
with reason `field_conflict` and leaves the row unchanged.

Work, temporary path/de-duplication state, results, response bytes, and
cancellation are bounded. Executable
[`SQL-LOG-032`](QUERY_SQL_EQUIVALENTS.md#sql-log-032-first-nonempty-textual-log-field)
shows the ordinary public `CASE`/`NULLIF`/`COALESCE` equivalent for exact
metadata paths. No extension primitive or private table is needed.

## LogsQL `copy` over rich current rows

`copy` (alias `cp`) preserves source fields while cloning them to one or more
destinations:

```text
* | copy trace_id as correlation_id
* | cp context.* as copied.*, copied.attempt as retry_attempt
* | copy service saved, host service, saved host
```

`as` is optional. Comma-separated pairs execute left to right, so later pairs
observe earlier copies and can form chains or swaps. Sources and destinations
may be exact fields, `*`, or suffix-star prefix filters. A wildcard source is
snapshotted at the start of its pair and expands recursively flattened leaves
in bytewise field-name order; arrays remain atomic values. Prefix destinations
replace the matched source prefix. Copying many wildcard sources to one exact
destination is deterministic last-write-wins. A missing wildcard source is a
no-op.

Exact copies preserve JSON strings, numbers, booleans, arrays, null, and empty
strings without deleting or coercing the source. A missing exact source or an
exact rich-object parent produces an explicit empty string, matching the
upstream flattened-column view; copy an exact dotted leaf or use a prefix to
clone object contents. An exact source paired with a wildcard destination
uses the literal destination name, including `*`, matching VictoriaLogs.
When prefix removal yields an empty destination suffix, it creates a literal
empty field distinct from `_msg`; exact quoted `""` still names `_msg`.

Existing compatible scalar destinations are overwritten. A destination that
would replace a retained object or descend through a scalar fails with HTTP
422 reason `field_conflict`, preserving rich-row fidelity. Source traversal,
temporary cloned values and paths, result rows, response bytes, and
cancellation are bounded. Executable
[`SQL-LOG-033`](QUERY_SQL_EQUIVALENTS.md#sql-log-033-copy-one-exact-retained-metadata-field)
shows the public JSON1 equivalent for one exact retained metadata source and
one exact top-level destination. Sequential/wildcard language composition
remains in the Rust API; no extension primitive or private table is used.

## LogsQL `rename` over rich current rows

`rename` (alias `mv`) moves fields within the current response row:

```text
* | rename trace_id as correlation_id
* | mv context.* as moved.*, moved.attempt as retry_attempt
* | rename service saved, host service, saved host
```

`as` is optional. Comma-separated pairs execute left to right, so later pairs
observe earlier removals and destinations and can implement chains or swaps.
Sources and destinations may be exact fields, `*`, or suffix-star prefix
filters. Each wildcard source snapshots the current recursively flattened
leaves in bytewise field-name order. Arrays remain atomic. All sources for one
pair are removed before its destinations are inserted. Prefix destinations
replace the matched source prefix; multiple wildcard sources moved to one
exact destination are deterministic last-write-wins. An unmatched wildcard
source is a no-op.

Exact strings, numbers, booleans, arrays, null, and empty strings retain their
JSON types. Present leaves are removed from the response and empty rich
parents are pruned, but the stored row remains immutable. A missing exact
source or exact rich-object parent produces an explicit empty destination;
the object remains because VictoriaLogs' flattened view has no parent column.
Rich empty objects likewise have no wildcard leaf and are retained. An exact
source paired with a wildcard destination uses the literal destination name,
including `*`. When prefix removal yields an empty suffix, the destination is
a literal empty field distinct from `_msg`; exact quoted `""` still names the
message.

Compatible scalar destinations are overwritten. A destination that would
replace a retained object or descend through a scalar fails with HTTP 422
reason `field_conflict`. Traversal, temporary moved values and paths, result
rows, response bytes, and cancellation are bounded. Executable
[`SQL-LOG-034`](QUERY_SQL_EQUIVALENTS.md#sql-log-034-rename-one-exact-top-level-retained-metadata-field)
shows the public JSON1 foundation for one exact top-level move. The Rust API
owns nested-parent pruning, wildcard and sequential composition, strict
errors, and hard limits; no extension primitive or private table is used.

## LogsQL `format` over rich current rows

`format` interpolates current-row fields into a textual result:

```text
* | format "request from <client_ip>: <_msg>"
* | format if (level:=error) '<uc:service> <q:_msg>' as summary
* | format '<duration_seconds:elapsed>' as elapsed_seconds keep_original_fields
* | format '<urlencode:user>' as encoded_user skip_empty_results
```

The pattern may be quoted or a single unquoted token. Literal prefixes decode
HTML entities. `<field>` uses recursively retained rich paths and textual
projection: strings remain unquoted, numbers and booleans use JSON spelling,
arrays use compact JSON, and missing/null values are empty. `<_>`, `<*>`, and
`<>` are explicit empty placeholders; wildcard field references are rejected.
An unknown option is a plain interpolation for VictoriaLogs compatibility.

The supported options are `uc`, `lc`, `q`, `urlencode`, `urldecode`,
`hexencode`, `hexdecode`, `base64encode`, `base64decode`, `hexnumencode`,
`hexnumdecode`, `time`, `duration`, `duration_seconds`, and `ipv4`. Invalid
codec inputs retain their source text, except `hexdecode` preserves invalid
byte pairs while decoding valid pairs exactly as the pinned processor does.
`uc`/`lc` use simple one-codepoint Unicode mappings. `time` accepts the exact
VictoriaLogs signed integer, fractional, and scientific Unix s/ms/us/ns
heuristic and emits trimmed nanosecond RFC3339 UTC. `duration` accepts signed
nanoseconds; `duration_seconds` accepts the established human-duration
grammar.

The destination is `_msg` unless `as exact_field` is present. `if (...)`
formats only matching rows; `if ()` matches every row. A nonempty existing
destination is retained by `keep_original_fields`, and by
`skip_empty_results` only when the new result is empty. Timeless preserves an
explicit empty destination in rich JSON, whereas VictoriaLogs stream JSON
omits empty-valued columns. Existing scalar destinations are overwritten. A
destination that would replace a retained object or descend through a scalar
fails with HTTP 422 reason `field_conflict` and leaves durable storage
unchanged.

Pattern/source traversal, transform expansion, temporary output, result rows,
response bytes, and cancellation use the hard request limits. Executable
[`SQL-LOG-035`](QUERY_SQL_EQUIVALENTS.md#sql-log-035-format-two-exact-retained-metadata-fields)
shows a public JSON1/`printf` equivalent for two exact metadata paths. The
Rust API owns LogsQL syntax, arbitrary placeholders and codecs, conditions,
destination preservation, errors, and limits; no extension primitive or
private table is used. Exact-build evidence measures 3.297/39.353 ms
narrow/wide p95 versus 3.090/35.941 ms for byte-identical same-scan controls;
`QSF-171` accepts the +6.7%/+9.5% bounded formatting cost.

## LogsQL `math` / `eval` over rich current rows

`math` and its alias `eval` calculate one or more binary64 expressions and
write string results into the current response row:

```text
* | math duration + 10e9 as adjusted_ns
* | eval attempts + 1 next_attempt, next_attempt * backoff as delay
* | math round(bytes / 1KiB, 0.01) as kib
* | math invalid default 0 as safe_value
```

Comma-separated entries execute left to right, and a later entry can read an
earlier destination. `as` is optional. If the destination is omitted, the
canonical expression—including necessary parentheses—becomes the field name.
Only exact destinations are accepted.

From tightest to loosest, the binary operators are `^`, `*`/`/`/`%`,
`+`/`-`, `&`, `xor`, `or`, and `default`. All associate left, including
power. Unary plus/minus and explicit parentheses are supported. Available
functions are `abs`, `ceil`, `exp`, `floor`, `ln`, `max`, `min`, `now`,
`rand`, and `round`. `max`/`min` require at least two arguments and skip NaN
operands in evaluation order. `round(value)` rounds to an integer;
`round(value, nearest)` uses the pinned VictoriaLogs decimal-scale rule.
`now()` returns a pipeline-invocation Unix timestamp in nanoseconds and
`rand()` returns a value in `[0,1)`.

Numbers may be decimal, base-zero, scaled, durations, byte sizes, RFC3339
timestamps, or IPv4 addresses; durations and timestamps become nanoseconds.
Current-row fields use the same coercion. Missing, null, empty, arrays,
objects, and invalid text become NaN. `default` replaces only NaN—not
infinity. Results use fixed, non-exponent strings, including `NaN`, `+Inf`,
and `-Inf`. Bitwise operands follow the pinned VictoriaLogs unsigned
conversion even for negative, nonfinite, or out-of-range values, so results
do not depend on Rust target cast behavior.

Rich source values are never changed. Scalar destinations are overwritten;
replacing an object or descending through a scalar returns HTTP 422 reason
`field_conflict`. Parser AST size/nesting, evaluated work, temporary state,
result rows, response bytes, and cancellation use the hard request limits.
Executable [`SQL-LOG-036`](QUERY_SQL_EQUIVALENTS.md#sql-log-036-arithmetic-over-exact-retained-numeric-fields)
shows a parameterized public JSON1 equivalent for ordinary arithmetic over
two exact numeric metadata fields. SQL deliberately returns `NULL` for
invalid inputs instead of SQLite's misleading `CAST('bad' AS REAL) = 0`;
the complete LogsQL grammar, coercion chain, functions, sequential mutation,
and error envelopes remain Rust API behavior. No extension primitive or
private table is used.

Exact-build math evidence measures 3.357/39.127 ms narrow/wide p95 while
returning 64 rows, versus 3.292/37.655 ms for byte-identical same-scan
controls. The +2.0%/+3.9% p95 and +2.4%/+9.6% internal API cost follows the
same one/four candidate blocks, 1,024/8,192 decoded entries, and
235,778/1,914,055 payload bytes. `QSF-173` accepts this bounded expression
cost above the unchanged public storage boundary.

## LogsQL `len` over rich current rows

`len` measures the byte length of one current-row field and writes the decimal
result as a string:

```text
* | len(_msg) as message_bytes
* | len unicode byte_length
* | len(nested.value)
* | len(host)
```

The command and `as` are case-insensitive. Parentheses and `as` are optional;
the destination defaults to `_msg`, including the accepted `len(field) as`
form. An empty quoted source or destination is the canonical `_msg` alias.
Only exact quoted or dotted source and destination fields are accepted, and
sequential pipes observe earlier destinations.

Length is measured in UTF-8 bytes, so `len("ßİ")` is four even though the
text contains two Unicode codepoints. Strings use their decoded bytes;
booleans and numbers use their textual representation; arrays use compact
JSON. Missing fields, explicit null, empty strings, and exact retained object
parents have length zero, matching VictoriaLogs' flattened query view. A
nested object leaf remains addressable. Canonical `_msg`, `_time`, and
`level` fields are measured from their current rendered values. Rich source
values and durable storage remain unchanged.

An exact scalar destination is overwritten. A destination that would replace
a retained object or descend through a scalar fails with HTTP 422 reason
`field_conflict`. Array traversal work, temporary result/path state, result
rows, response bytes, and cancellation use the hard request limits.
Executable
[`SQL-LOG-037`](QUERY_SQL_EQUIVALENTS.md#sql-log-037-utf-8-byte-length-of-one-exact-retained-field)
uses only public `logs` rows plus SQLite JSON1 and
`length(CAST(value AS BLOB))`; the `BLOB` cast is required because SQLite
`length(TEXT)` counts codepoints. Grammar, canonical/current-row fields,
sequential destinations, limits, cancellation, and envelopes remain Rust API
work. No extension primitive, private table, or storage-format change is
needed.

Exact-build `len` evidence measures 3.785/40.724 ms narrow/wide p95 while
returning 64 rows, versus 3.620/36.622 ms for byte-identical same-scan
controls. The +4.6%/+11.2% p95 and -1.0%/+12.6% internal API variation
follows the same one/four candidate blocks, 1,024/8,192 decoded entries, and
235,778/1,914,055 payload bytes. `QSF-175` accepts the bounded row-local byte-
length work above the unchanged public storage boundary.

## LogsQL `drop_empty_fields` over current rows

`drop_empty_fields` removes empty fields from each current pipeline row:

```text
* | drop_empty_fields
* | fields case, optional, nested | drop_empty_fields
* | format "" as transient | drop_empty_fields
```

The command is case-insensitive and accepts no arguments. JSON null and an
empty string are empty. Missing fields are already absent. Zero, false,
nonempty strings, and arrays—including `[]`—are retained without changing
their types. Rich objects are traversed recursively: empty leaves and newly
empty parents are removed, but arrays are atomic and their elements are not
fields. If a prior `fields`, `delete`, `format`, or other transformation leaves
no fields at all, the row is omitted. Later pipeline stages observe the
pruned row. Durable stored metadata is never changed.

Traversal is in place over request-owned public rows. JSON nesting is capped
at 128 levels; every visited row/value consumes the hard work allowance;
cancellation is checked before and during traversal; and final result rows and
response bytes use the shared request limits. Invalid arguments, parentheses,
aliases, and trailing tokens fail as malformed LogsQL.

Executable
[`SQL-LOG-038`](QUERY_SQL_EQUIVALENTS.md#sql-log-038-drop-one-empty-retained-metadata-field)
uses only public `logs` rows and SQLite JSON1 to remove one known null/empty
metadata path. A fixed-schema embedded application can repeat that expression
for its known fields. Dynamic field discovery, canonical `_msg`/`_time`/`level`
handling, recursive empty-parent and all-empty-row pruning, resource limits,
cancellation, and HTTP envelopes remain Rust API behavior. No extension
primitive or private storage table is used.

Exact-build `drop_empty_fields` evidence measures 4.542/38.151 ms narrow/wide
p95 while returning 64 rows, versus 6.994/35.779 ms for byte-identical same-
scan controls. The -35.1%/+6.6% p95 and -3.3%/+11.0% internal API variation
follows the same one/four candidate blocks, 1,024/8,192 decoded entries, and
235,778/1,914,055 payload bytes. Responses are byte-identical. `QSF-177`
accepts the bounded in-place rich-row traversal above the unchanged public
storage boundary.

## LogsQL literal `replace` over current rows

`replace` substitutes literal, non-overlapping substrings in one exact
current-row field:

```text
* | replace ("_", "-")
* | replace ("_", "-") at host limit 1
* | replace if (kind:=admin) ("secret", "***") at password
* | replace if () ("a,b", "c|d") at "quoted field" limit 0
```

The command, `if`, `at`, and `limit` keywords are case-insensitive. Two
parenthesized literal substrings are required; quoted values may contain
spaces, commas, pipes, and Unicode. This is byte-for-byte substring matching,
not regular-expression replacement. The target defaults to `_msg` and may be
one quoted or dotted exact field. `limit 0` or an omitted limit replaces every
non-overlapping occurrence; a positive limit replaces only the first `N`.
An empty old substring is a no-op. Optional `if (...)` evaluates against the
current row before replacement, and later pipeline stages observe the result.

VictoriaLogs stores a flattened textual view. Timeless projects strings,
lowercase booleans, numbers, and compact JSON arrays for replacement while
treating a missing field, null, or exact object parent as empty. When the old
literal does not match—or is empty—the original rich native value remains
unchanged. An actual replacement produces a string in the query result only;
durable metadata and canonical fields are never mutated. This retained-model
rule preserves more information without changing the observable replacement
text. Sequential `replace` pipes see prior transformations.

Parsing is strict. In particular, Timeless rejects attached
`replace(foo,bar)` syntax. The pinned VictoriaLogs replace-pipe parser rejects
that spelling too, although its whole-query endpoint can ambiguously accept it
as an unrelated filter; Timeless does not silently reinterpret malformed pipe
syntax. Wildcard targets, invalid or leading-zero limits, wrong arity, and
trailing tokens also fail as malformed LogsQL.

Projected arrays, matches, generated text, field paths, work, result rows,
response bytes, and cancellation use the hard request limits. Executable
[`SQL-LOG-039`](QUERY_SQL_EQUIVALENTS.md#sql-log-039-literal-replacement-in-one-exact-retained-field)
uses only public `logs` rows, SQLite JSON1, and core `replace()` for an
all-occurrence exact-field equivalent. Conditional, first-`N`, current-row,
rich-preservation, limit, cancellation, and HTTP-envelope behavior remains in
the Rust API. No extension primitive, private storage table, or durable format
change is needed.

Exact-build literal-replacement evidence measures 3.240/3.520/3.576 ms
narrow and 36.716/37.711/37.948 ms wide p50/p95/p99 while returning 64 rows
and 1,600 response bytes. Byte-identical same-scan controls measure
3.261/3.908/4.406 and 35.734/36.882/37.846 ms. The -9.9%/+2.2% p95 and
-3.6%/+2.7% internal API variation follows the same one/four candidate
blocks, 1,024/8,192 decoded entries, 235,778/1,914,055 payload bytes, and
128/8,192 public rows. `QSF-179` accepts this bounded literal row transform
above the unchanged public storage boundary.

## LogsQL `replace_regexp` over current rows

`replace_regexp` substitutes non-overlapping RE2-family matches in one exact
current-row field:

```text
* | replace_regexp ("[/ ]", "-")
* | replace_regexp ("(?P<name>[a-z]+)-(?P<id>[0-9]+)", "${id}:${name}") at host
* | replace_regexp if (kind:=admin) ("secret=([^ ]+)", "secret=***") limit 1
* | replace_regexp ("^|$", X) at host limit 0
```

The command, `if`, `at`, and `limit` keywords are case-insensitive. Exactly
two parenthesized arguments are required. The target defaults to `_msg` and
may be one quoted or dotted exact field. A missing or zero limit replaces all
non-overlapping matches; a positive limit replaces only the first `N`.
Optional `if (...)` observes the current row before replacement, and later
pipeline stages observe the transformed value.

Patterns use the pinned VictoriaLogs/Go RE2-family contract: matching is
case-sensitive unless an inline flag changes it, dot matches a newline by
default, `(?-s)` restores single-line dot behavior, and backreferences and
lookaround are rejected. An empty pattern matches UTF-8 boundaries, including
the start and end of a nonempty value. An empty source remains a no-op, as it
does upstream. Patterns are compiled once per request with a one-MiB compiled
program ceiling.

Replacement templates support `$0`, `$1`, `${1}`, `$name`, `${name}`, and
`$$`. Missing or unmatched captures expand to empty text. Unbraced names are
maximal: `$1x` denotes the capture named `1x`, while `${1}x` denotes capture
one followed by `x`. This distinction is covered by the pinned oracle.

Strings, lowercase booleans, numbers, and compact JSON arrays use the same
textual projection as literal `replace`; missing fields, null, and exact
object parents project to empty text. A no-match operation preserves the
original native value. An actual replacement writes a string only to the
request-owned query row, so durable rich metadata remains unchanged.
Sequential transformations see prior results.

Parsing, pattern compilation, captures, replacement expansion, projected
arrays, output sizing, field paths, work, result rows, response bytes, and
cancellation are bounded. Invalid patterns, attached syntax, wrong arity,
wildcard targets, invalid or leading-zero limits, and trailing tokens fail
explicitly.

There is no executable SQL-equivalent recipe for this row. Core SQLite and
the public timeless-libsql extension expose no portable RE2-compatible
replacement function with capture-template expansion. Claiming ordinary SQL
support would therefore be false; applications using SQLite directly must
compose this transformation outside SQL or deliberately load a separate
regexp extension. The existing public `logs` scan remains the storage
boundary, and measurements decide whether a future general-purpose extension
primitive is justified. Promoting LogsQL syntax or a language-specific
replacement helper into the storage extension is not justified.

Exact-build regexp-replacement evidence measures 3.303/3.442/3.533 ms
narrow and 39.620/40.628/45.369 ms wide p50/p95/p99 while returning 64 rows
and 1,600 response bytes. Byte-identical same-scan controls measure
3.200/3.391/3.646 and 33.545/35.822/36.126 ms. The +1.5%/+13.4% p95 and
+0.2%/+19.8% internal API cost follows the same one/four candidate blocks,
1,024/8,192 decoded entries, 235,778/1,914,055 payload bytes, and 128/8,192
public rows. `QSF-181` accepts the bounded API-side regex and capture-
expansion work above the unchanged public storage boundary.

## LogsQL literal `extract` over current rows

`extract` captures text between fixed literal delimiters into named fields:

```text
* | extract 'kind=<kind> id=<id>'
* | extract '<left> &lt; <right>' from comparison
* | extract 'ip=<ip> <_>=<method> path=<path>' from request
* | extract if (service:=api) 'user=<user>' keep_original_fields
* | extract 'value=<plain:raw_value>' from payload skip_empty_results
```

The command and the `if`, `from`, `keep_original_fields`, and
`skip_empty_results` keywords are case-insensitive. The quoted or unquoted
pattern must contain at least one named `<field>`. `<>`, `<_>`, and `<*>` are
anonymous captures. Adjacent placeholders are invalid: every pair needs a
nonempty literal delimiter. Literal pattern text is HTML-decoded, so `&lt;`
matches `<`. The source defaults to `_msg` and may be one exact quoted or
dotted current-row field. A nonempty first literal may begin anywhere in the
source; an empty first literal anchors extraction at the start.

When a capture begins with a valid Go double-quoted, single-quoted, or raw
backtick string, `extract` decodes that quoted prefix and then requires the
next literal immediately after it. The `plain:` field option disables this
automatic decoding. A missing first literal leaves every named result empty.
If a later unquoted delimiter is missing, earlier completed fields remain and
the current/later fields are empty. A successfully decoded quoted field also
remains available when its following delimiter is missing. Explicit empty
result strings remain present in Timeless output rather than becoming
indistinguishable from missing metadata.

By default every named capture replaces its current-row destination, including
an empty capture. `keep_original_fields` preserves each destination whose
existing textual value is nonempty. `skip_empty_results` preserves a nonempty
existing destination only when its new capture is empty; nonempty captures
still replace it. Existing numbers, booleans, arrays, and objects count as
nonempty and remain native whenever preserved. Source strings, lowercase
booleans, numbers, and compact arrays use the established textual projection;
missing, null, and exact object parents project to empty text. A capture may
write a nested leaf while preserving its siblings, but replacing a retained
object with a scalar fails explicitly with 422. All transformations are
request-local; durable log metadata is unchanged, and later pipeline stages
observe earlier results.

Pattern traversal, quoted decoding, projected arrays, captures, destination
paths, work, result rows, response bytes, and cancellation use the shared hard
request limits. Missing/literal-only patterns, wildcard sources or outputs,
adjacent fields, misplaced conditions, both preservation modifiers, malformed
quotes, and trailing tokens fail instead of being ignored.

Executable
[`SQL-LOG-040`](QUERY_SQL_EQUIVALENTS.md#sql-log-040-two-literal-delimited-fields-from-one-exact-retained-field)
uses only public `logs` rows, SQLite JSON1, and core `instr()`/`substr()` to
extract two unquoted fields from a fixed prefix/middle/suffix pattern. General
pattern parsing, Go quoted-string decoding, current-row mutation and preserve
modes, limits, cancellation, and HTTP envelopes remain Rust API behavior. No
extension primitive, private table, storage-format change, or durable
mutation is involved.

Exact-build literal-extraction evidence measures 2.977/3.269/3.673 ms narrow
and 37.121/39.052/43.064 ms wide p50/p95/p99 while returning 64 rows and
1,600 response bytes. Byte-identical same-scan controls measure
2.925/3.201/3.315 and 32.704/33.944/34.306 ms. The +2.1%/+15.0% p95 and
-1.7%/+12.2% internal API variation follows the same one/four candidate
blocks, 1,024/8,192 decoded entries, 235,778/1,914,055 payload bytes, and
128/8,192 public rows. `QSF-183` accepts this bounded API-side literal scan,
quoted decoding, and field-write work above the unchanged public storage
boundary.

## LogsQL RE2 `extract_regexp` over current rows

`extract_regexp` writes the named captures from the first regular-expression
match into request-owned fields:

```text
* | extract_regexp 'user=(?P<user>[A-Za-z]+) id=([0-9]+)'
* | extract_regexp 'kind=(?<kind>[a-z]+)' from payload
* | extract_regexp if (service:=api) 'request=(?P<request>.+)' keep_original_fields
* | extract_regexp '(?P<line>.+)' from "source field" skip_empty_results
```

The command and the `if`, `from`, `keep_original_fields`, and
`skip_empty_results` keywords are case-insensitive. The quoted or unquoted
RE2-family pattern must contain at least one named group. Both
`(?P<name>...)` and `(?<name>...)` are accepted. Anonymous groups affect the
match but create no field. Backreferences and lookaround are rejected. The
source defaults to `_msg` and may be one exact quoted or dotted current-row
field; wildcard sources and capture destinations are rejected.

Only the first match is used. Dot matches newline by default, matching
VictoriaLogs; inline flags such as `(?-s)` may disable that behavior. A
missing match or unmatched optional named group produces an empty capture.
Default mode writes that empty string, `keep_original_fields` preserves every
nonempty existing destination, and `skip_empty_results` preserves a nonempty
destination only when the new capture is empty. Later stages observe earlier
writes. Strings, lowercase booleans, numbers, and compact JSON arrays use the
standard textual projection. Preserved native numbers, booleans, arrays,
objects, nulls, and nested siblings are not rewritten. Replacing a retained
object with a scalar fails with HTTP 422 instead of silently discarding it.

Regex compilation is request-once and size-bounded. Source projection,
captures, paths, work, temporary state, result rows, response bytes,
deadlines, and cancellation use the shared hard limits. Transformations never
mutate durable log metadata and survive flush, optimize, shutdown, and reopen
through the same public storage rows.

There is intentionally no SQL-equivalent recipe for this row. Core SQLite
and the public timeless-libsql extension do not provide a portable
RE2-compatible named-capture extraction scalar. Direct users can apply a
host regex or load a separate general regexp extension; timeless-libsql does
not claim that as ordinary SQL support. No private table, extension storage
primitive, or durable-format change is involved.

Exact-build regexp-extraction evidence measures 3.027/3.154/4.780 ms narrow
and 34.890/35.517/37.085 ms wide p50/p95/p99 while returning 64 rows and
1,600 response bytes. Byte-identical same-scan controls measure
3.034/3.922/4.296 and 32.696/33.808/38.479 ms. The -19.6%/+5.1% p95 and
-1.6%/+6.1% internal API variation follows the same one/four candidate
blocks, 1,024/8,192 decoded entries, 235,778/1,914,055 payload bytes, and
128/8,192 public rows. `QSF-185` accepts the bounded API-side first-match
capture and field-write work above the unchanged public storage boundary.

## LogsQL typed `pack_json` over current rows

`pack_json` snapshots selected request-owned fields and writes one compact
JSON string to a current-row destination:

```text
* | pack_json
* | pack_json as packed
* | pack_json packed
* | pack_json fields (host, status, context.*) as packed
* | pack_json fields ("request."*) as request_json
```

The command, `fields`, and `as` are case-insensitive. The destination defaults
to `_msg`; explicit destinations may follow `as` or be bare. Omitted or empty
`fields (...)` selects all fields, as does `*` anywhere in the list. Exact and
prefix selectors may be quoted. Selection snapshots the row before the
destination write: `pack_json` includes the old `_msg` inside the new `_msg`,
and packing over an existing destination captures its old value. Later stages
observe the packed string. Missing exact fields yield `{}`; overlapping
selectors form one idempotent union in deterministic key order.

Timeless preserves the retained JSON model rather than flattening it.
Numbers, booleans, arrays, objects, explicit nulls, empty strings, and empty
objects retain their native JSON representation, while dotted prefix
selection reconstructs nested objects. This intentionally differs from
VictoriaLogs v1.52.0, which flattens current columns to strings, omits empty
values, follows column order, and can emit duplicate keys for overlapping
selectors. The pinned 850-case oracle records the upstream behavior; real-
extension regressions pin the richer Timeless compatibility policy.

Paths, recursive visits, selected values, temporary JSON bytes, nesting,
work, result rows, response bytes, deadlines, and cancellation are bounded by
the shared request limits. Destinations under scalar parents fail with HTTP
422. Packing is query-local and never changes the public durable `logs` rows,
including after optimize, shutdown, or reopen.

Executable
[`SQL-LOG-041`](QUERY_SQL_EQUIVALENTS.md#sql-log-041-pack-selected-rich-metadata-fields-as-json)
uses public `logs`, `json_type`, `->`, and `json_set` to pack a bounded list
of exact JSON paths while preserving missing/null/empty/type distinctions.
Recursive prefix/all selectors, destination writes, language errors, limits,
cancellation, and HTTP envelopes remain Rust API composition. No extension
primitive, private table, or storage-format change is involved.

Exact-build typed-packing evidence measures 2.946/3.146/4.588 ms narrow and
35.570/37.921/38.085 ms wide p50/p95/p99 while returning 64 rows and 2,688
response bytes. Same-scan plain-field controls measure
2.959/3.098/3.227 and 32.567/35.717/37.101 ms while returning 1,600 bytes.
The +1.5%/+6.2% p95 and -1.3%/+7.4% internal API variation follows the same
one/four candidate blocks, 1,024/8,192 decoded entries,
235,778/1,914,055 payload bytes, and 128/8,192 public rows. `QSF-187`
accepts the bounded rich selection and serialization cost above the unchanged
public storage boundary.

## LogsQL typed `unpack_json` over current rows

`unpack_json` snapshots one request-owned field, parses a JSON object, and
writes selected members back into the current result row:

```text
* | unpack_json
* | unpack_json from payload
* | unpack_json payload fields (host, status, context.*)
* | unpack_json if (kind:=audit) from payload preserve_keys (context)
    result_prefix decoded. keep_original_fields
* | unpack_json from payload fields () skip_empty_results
```

Keywords are case-insensitive. The source defaults to `_msg`; one exact
source may appear bare or after `from`. The source may be whitespace-padded
JSON-object text or a retained native object. Omitted or empty `fields ()`
selects all fields. Exact and prefix selectors may be mixed; missing exact
paths become empty strings, while unmatched prefixes produce no fields.
`preserve_keys` keeps named objects atomic and native. `result_prefix` is
prepended before reconstructed output paths.

Timeless preserves the retained rich JSON model. Strings, numbers, booleans,
arrays, objects, explicit nulls, empty strings, and empty objects retain their
native types. Nested output merges with unrelated existing siblings, and a
literal JSON key containing `.` remains distinct from a nested path. The
source is snapshotted before any destination write, including when an
unpacked member replaces the source itself. By default selected values
overwrite scalar destinations. `keep_original_fields` retains existing
nonempty values; `skip_empty_results` suppresses incoming null and empty
strings. Writes through a scalar parent or scalar replacement of a retained
object fail with HTTP 422.

Whitespace is ignored around object text. Missing, null, scalar, array, and
nonobject strings are no-ops. For compatibility, malformed text beginning
with `{` writes empty strings only for explicitly requested exact paths, and
the pinned bare `NaN` token becomes the string `"NaN"`. Grammar, JSON/path
work, parsed and selected state, result rows, response bytes, deadlines, and
cancellation are bounded by shared request limits. The transform never
changes durable public rows, including after optimize, shutdown, and reopen.

VictoriaLogs v1.52.0 flattens nested values to textual columns, serializes
arrays compactly, textualizes numbers/booleans, and maps null to empty text.
The pinned 875-case oracle records that language behavior; Timeless's native
types and reconstructed nesting are the documented retained-model policy.

Executable
[`SQL-LOG-042`](QUERY_SQL_EQUIVALENTS.md#sql-log-042-unpack-selected-rich-fields-from-a-json-object)
uses public `logs`, `json_valid`, `json_type`, `->`, and `json_set` for a
bounded fixed set of exact paths while preserving missing/null/empty/type
distinctions. Dynamic selectors, request-local mutation and preservation,
language errors, limits, cancellation, and envelopes remain Rust API
composition. No extension primitive, private table, or storage-format change
is involved.

Exact-build typed-unpacking evidence measures 2.933/3.151/3.256 ms narrow and
36.963/40.062/65.292 ms wide p50/p95/p99 while returning 64 rows and 2,112
response bytes. Equal-output pack-plus-copy controls measure
2.962/3.763/4.407 and 36.314/38.694/41.375 ms. The -16.3%/+3.5% p95 and
-0.5%/+3.3% internal API variation follows the same one/four candidate
blocks, 1,024/8,192 decoded entries, 235,778/1,914,055 payload bytes, and
128/8,192 public rows. `QSF-189` accepts the bounded parse/select/write cost
above the unchanged public storage boundary and retains the wide p99 without
hiding it.

## LogsQL top-level JSON array length

`json_array_len` snapshots one exact request-owned field, counts its top-level
array elements, and writes a decimal string to the current result row:

```text
* | json_array_len(tags)
* | json_array_len(tags) as tag_count
* | json_array_len payload.items item_count
* | json_array_len("left field") as "item count"
```

Keywords are case-insensitive. Parenthesized and bare exact sources are
accepted; `as` is optional and a terminal `as` retains the default `_msg`
destination. Sources and destinations may be quoted or dotted exact paths.
Wildcards, prefixes, multiple sources, and trailing tokens fail explicitly.
The source is read before the destination is written, so
`json_array_len(tags) as tags` deterministically replaces the request-local
field with its count without changing storage.

Retained native arrays are counted directly. Strings containing valid JSON
arrays may have surrounding whitespace and are parsed; the pinned
VictoriaLogs bare `NaN` token counts as one element. Nested arrays and objects
each count once. Empty arrays, missing fields, explicit nulls, malformed JSON,
JSON scalar text, and native scalar or object values return `"0"`. Native
source arrays and all of their element types remain unchanged. Writes that
would replace a retained object fail with HTTP 422. Parsing, temporary state,
paths, rows, response bytes, deadlines, and cancellation all use the shared
request limits, and the reader remains reusable after rejection.

The pinned VictoriaLogs v1.52.0 oracle covers the command's grammar, array,
scalar, malformed, default-destination, overwrite, case, and error behavior in
the complete 897-case fixture. The real-extension regression additionally
pins Timeless's native rich arrays, microsecond durability, optimize,
shutdown, and reopen.

Executable
[`SQL-LOG-043`](QUERY_SQL_EQUIVALENTS.md#sql-log-043-top-level-json-array-length)
uses public `logs`, `json_type`, `json_valid`, and `json_array_length` for a
bounded fixed exact path. It returns textual zero for nonarrays and leaves the
source untouched. Language grammar, current-row destination writes,
VictoriaLogs bare-`NaN` compatibility, limits, cancellation, and envelopes
remain Rust API composition. No extension primitive or private storage access
is involved.

Exact-build native-array evidence measures 3.285/3.558/3.788 ms narrow and
39.914/41.563/44.668 ms wide p50/p95/p99 while returning 64 rows and 1,344
response bytes. Equal-output constant-format controls measure
3.276/3.454/3.514 and 39.836/40.607/43.555 ms. The +3.0%/+2.4% p95 and
+2.4%/-0.0% internal API variation follows the same one/four candidate
blocks, 1,024/8,192 decoded entries, 235,778/1,914,055 payload bytes, and
128/8,192 public rows. `QSF-191` accepts the bounded direct native-array count
and request-local write above the unchanged public storage boundary.

## LogsQL upper-step quantiles and population deviation

The bounded `stats` pipeline supports textual upper-step `quantile` and
numeric population `stddev`:

```text
* | stats quantile(0.5, duration_ms) as p50
* | stats quantile(0, duration_ms) as minimum, quantile(1, duration_ms) as maximum
* | fields duration_ms | stats quantile(0.95) as p95
* | stats stddev(duration_ms) as sigma
```

Function names are case-insensitive. `quantile` requires a decimal rank in
the inclusive range `[0,1]`; its fields follow the existing exact, prefix,
and all-current-field selectors, and omitted fields mean all current fields.
Selected values use the LogsQL textual projection: missing and JSON null are
empty, strings retain their contents, and other rich values use compact JSON.
Ordering follows VictoriaLogs signed integer, unsigned integer, RFC3339
timestamp, general math-number, and natural UTF-8 comparisons. For `N`
values, the selected zero-based rank is `min(floor(phi * N), N - 1)` with no
interpolation. An empty selection is the explicit empty string.

`stddev` uses Welford's one-pass population algorithm and divides by `N`, not
`N - 1`. In the retained Timeless typed-statistics profile, only native JSON
numbers participate. Numeric-looking strings, booleans, nulls, missing paths,
arrays, and objects are ignored. A singleton returns zero and an empty numeric
selection returns JSON null.

Every selected quantile value counts against `max_work_rows`; its exact text
state also counts against `max_response_bytes`. Every visited deviation value
counts against `max_work_rows`. Both operations are deadline-cancellable and
leave the SQLite reader reusable after cancellation or limit rejection.
Unlike VictoriaLogs' random reservoir above 10,000 values, Timeless never
silently makes an exact result nondeterministic: it fails with the stable
query-limit envelope.

The complete pinned VictoriaLogs fixture records four intentional retained-
model wire differences: Timeless preserves rich types, does not coerce numeric
strings for deviation, represents empty deviation as JSON null rather than a
textual `NaN`, and preserves an explicitly requested empty quantile field
instead of dropping it from stream JSON.

Executable
[`SQL-LOG-044`](QUERY_SQL_EQUIVALENTS.md#sql-log-044-upper-step-numeric-quantile-and-population-standard-deviation)
uses only public `logs`, JSON1, window functions, and a recursive Welford CTE
for one finite native-number path. Core SQLite has no honest equivalent for
the complete mixed textual natural comparator, so the full language grammar,
projection, ordering, exact state bounds, cancellation, and HTTP envelopes
remain Rust API composition. The required public scan already crosses every
selected row; no extension primitive or private storage access is involved.

Exact-build quantile evidence measures 3.279/3.636/3.834 ms narrow and
37.390/38.585/40.331 ms wide p50/p95/p99, 3.5% below/1.2% above same-run
median p95. Population deviation measures 3.330/3.517/3.676 and
36.149/37.372/37.815 ms, 2.9%/0.1% below same-run average p95. Every
equal-width pair reads the same one/four blocks, decodes the same
1,024/8,192 entries, transfers the same extension payload bytes, and
materializes the same 128/8,192 public rows. `QSF-193` therefore retains the
bounded API implementation and the complete measured tails without adding a
storage primitive.

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
