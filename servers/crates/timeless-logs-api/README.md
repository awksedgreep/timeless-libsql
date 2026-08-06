# timeless-logs-api release server

This first-class signal server was promoted from the completed API-boundary
POC. It is not a replacement storage implementation.

The storage contract is fixed:

- NDJSON requests are parsed into the public rich logs batch-v1 format. Exact
  product severities, epoch microseconds, and canonical typed JSON survive.
- The original flat logs batch-v0 format remains readable.
- `INSERT INTO logs(logs) VALUES (?1)` feeds the existing extension buffer.
- The extension's hard-coded 8,192-entry automatic flush is unchanged.
- The API never flushes at a request or producer-batch boundary.
- A one-second low-volume timer sends the existing `flush` command.
- A 30-second maintenance timer reads the extension's exact actionable
  raw/merge backlog and invokes public `optimize:<entries>` with a budget
  derived from a 32 MiB source-byte target. It does no work for deferred
  singleton/underfilled tails. `TIMELESS_LOGS_OPTIMIZE_INTERVAL_SECS` can
  defer the wake-up for isolated benchmarks without changing the default.
- Graceful shutdown sends an ordered `flush` after all accepted batches.

`204` means the parsed batch was admitted to the bounded SQLite-writer queue,
matching the asynchronous Elixir ingestion contract. It does not claim raw
durability. `/api/v1/flush` is the explicit ordered durability barrier.

## Implemented surface

- `GET /live`
- `GET /ready`
- `GET /health`
- `POST /insert/jsonline`
- `GET /select/logsql/query`
- `POST /select/logsql/query` for the versioned LogsQL compatibility grammar
- `GET /select/logsql/field_values`
- `GET /select/logsql/stats`
- `GET /api/v1/flush`

The authoritative language contract is the
[LogsQL feature matrix](../../../docs/LOGSQL_FEATURE_MATRIX.md). The shipped
Rust API rows at this revision are listed below for the executable contract
audit; native GET parameters do not expand this LogsQL claim.

<!-- query-contract-shipped: LQL-F01 LQL-F02 LQL-F03 LQL-F04 LQL-F05 LQL-F06 LQL-F07 LQL-F08 LQL-F09 LQL-F10 LQL-F11 LQL-F12 LQL-F13 LQL-F14 LQL-F15 LQL-F16 LQL-F17 LQL-F18 LQL-F19 LQL-F20 LQL-F21 LQL-F22 LQL-F23 LQL-F24 LQL-F25 LQL-F26 LQL-F27 LQL-F28 LQL-F29 LQL-F30 LQL-F31 LQL-F32 LQL-F33 LQL-F34 LQL-F39 LQL-F40 LQL-P01 LQL-P02 LQL-P03 LQL-P04 LQL-P05 LQL-P06 LQL-P07 LQL-P08 LQL-P09 LQL-P12 LQL-P13 LQL-P14 LQL-P15 LQL-P16 LQL-P18 LQL-Q01 LQL-Q02 LQL-Q07 LQL-Q08 LQL-S01 LQL-S02 LQL-S03 LQL-S04 LQL-S05 LQL-S06 LQL-S08 -->

The POST grammar includes wildcard selection; upper-exclusive relative
windows; RFC3339 and integer Unix s/ms/us/ns absolute bounds with open or
closed native-unit edges; all eight exact severities; service and arbitrary
typed metadata equality; message word, phrase, word-prefix, phrase-prefix,
case-sensitive substring, bounded RE2-compatible regexp, case-insensitive,
full-message exact, start-anchored exact-prefix, and static
`in(v1, ..., vN)` exact membership; field-independent wildcard no-ops for
`in`, `contains_any`, and `contains_all`; static case-sensitive
`contains_all(v1, ..., vN)` and `contains_any(v1, ..., vN)` with VictoriaLogs
phrase boundaries; retained-array primitive membership through
`json_array_contains_any(v1, ..., vN)`; inclusive one-address, CIDR, or
two-address `ipv4_range(...)` filtering over exact retained strings;
inclusive-lower/exclusive-upper `string_range(minimum, maximum)` bytewise
filtering over the retained rich textual projection; inclusive Unicode-
codepoint `len_range(minimum, maximum)` filtering over that projection;
same-row `eq_field`, `le_field`, and `lt_field` comparisons with exact
equality and VictoriaLogs math-value-or-bytewise ordering;
literal `prefix*:filter` field-set searches, including empty/quoted prefixes,
canonical special fields, recursively dotted rich-object leaves, independent
field-scoped logical operands, and current-row pipeline evaluation;
VictoriaLogs-compatible
any/full/prefix/suffix pattern filters with `<N>`, `<UUID>`, `<IP4>`, `<TIME>`,
`<DATE>`, `<DATETIME>`, and `<W>` placeholders and case-insensitive function
names; time sort, limit, and
offset aliases; and exact count with
an optional output alias. `NOT` binds before `AND`, which binds before `OR`;
parentheses and field-scoped groups override precedence.
Safe top-level indexed conjuncts are pushed into public extension rows before
the bounded Rust predicate evaluator. Predicates below `OR` or `NOT` are not
unsafely pushed.

Source may use LF or CRLF multiline layout and `#` line comments outside
double-quoted, single-quoted, and raw-backtick literals. A hash inside a quoted
field name or value remains literal. Exactly one optional terminal semicolon is
accepted, including before a trailing comment. Nonterminal or repeated
semicolons, comment-only input, dangling pipelines, and comments that remove a
required argument fail explicitly; lexical quote/semicolon errors include
one-based line and Unicode-character column locations. The common one-line
path is borrowed without copying. Comment/semicolon normalization is bounded by
the request body, preserves byte offsets and newlines, and remains entirely in
the Rust API rather than the extension.

Exact-build evidence over 8,192 retained rows measures the combined comment,
multiline, and terminal-semicolon form at 3.335/39.610 ms narrow/wide p95,
versus 3.504/41.410 ms for equal-cardinality plain word queries in the same
run. Both paths read exactly one/four blocks, 1,024/8,192 entries, and
235,778/1,914,055 payload bytes. The small timing difference is retained as
run variation; source preprocessing neither amplifies nor reduces storage work.

The ordered pipeline also accepts `field_values`, `field_names`,
`fields`/`keep`, `filter`/`where`, `stats`, `query_stats`, and bounded
`first`/`last`/`top`.
Projection
accepts exact dotted paths, top-level prefixes, and `*`; a later filter
observes the projected row, not the original one. Field discovery is
deterministic and top-level:
`field_names` counts a field whenever it is present, including JSON null and
empty values, and does not synthesize VictoriaLogs `_stream` fields that are
not in the Timeless storage model. `field_values` keeps JSON types distinct,
represents a missing value by omitting the requested field, and returns a
deterministic type-tagged order with numeric `hits`. A positive operator
`limit` bounds retained values; `limit 0` has the upstream meaning of no
operator-specific limit while the server's hard result/work limits still
apply.

`delete`, `del`, `drop`, and `rm` remove comma-separated exact fields, quoted
literal names, case-sensitive field prefixes, or every field from the current
pipeline row. Unquoted dotted paths recurse through retained JSON objects;
arrays and scalars remain atomic. Empty object parents are pruned, a row with
no remaining fields is omitted, missing fields are no-ops, and later stages
observe the deletion. Strict comma/wildcard grammar, decoded-row work limits,
request cancellation, flush/shutdown/reopen durability, and rich values are
covered by the real-extension regression. `SQL-LOG-025` is the direct
SQLite/libSQL exact-metadata-path foundation; language and recursive pruning
remain Rust API behavior.

Exact-build evidence measures exact plus nested-prefix deletion at
4.011/45.768 ms narrow/wide p95, 16.9%/17.6% above same-run word queries.
Removing the selected fields cuts response bytes by 22.4%/22.1%; candidate
blocks, decoded entries, extension payload bytes, and public rows are
identical. The bounded cost is row mutation and response reconstruction after
the same public decode.

The shipped statistics are `count`, `count_empty`, `count_uniq`,
`count_uniq_hash`, `uniq_values`, `values`, `sum`, `avg`, `min`, `max`,
`median`, `rate`, and `rate_sum`. Missing, null, and empty remain distinct;
`count_empty` deliberately counts all three for compatibility. Exact unique
counts use complete typed tuples, while `count_uniq_hash` uses a documented
stable 64-bit FNV-1a key hash and claims cardinality—not VictoriaLogs hash-bit
identity. `uniq_values` returns typed distinct non-empty values. The lossless
`values` result is `{\"items\":[...],\"missing\":N}` so missing cannot collapse
into JSON null. Numeric aggregates accept only stored JSON numbers; numeric
strings are ignored, integer-only sums remain exact when representable, min
and max preserve the chosen JSON number, and fractional/mixed sums, averages,
medians, and rates use finite binary64. `rate` and `rate_sum` divide by the
explicit query interval in seconds; without a finite two-sided time interval
they return the undivided count or sum.

Typed metadata comparisons accept `>`, `>=`, `<`, `<=`, and open or closed
`range` bounds without coercing numeric strings or losing integer precision.
`field:("")` follows VictoriaLogs empty semantics and matches missing, JSON
null, or an empty string; retained `field:""`, `field:=null`, and `field:=""`
forms remain exact so all three states can be distinguished. `field:*`
requires a present non-null value other than the empty string, while retaining
zero, false, arrays, and objects. `value_type` names the logical retained JSON
type (`string`, `uint64`, `int64`, `float64`, `number`, `bool`, `null`,
`array`, or `object`), not a private block encoding. VictoriaLogs physical
types such as `const` and `dict` fail explicitly.

IPv4 range filters accept exact dotted-decimal addresses, including decimal
octets with leading zeroes. One argument selects an address or expands a CIDR
from `/0` through `/32`; two arguments are inclusive unsigned address bounds,
and an inverted range matches nothing. Missing, null, numeric, object, array,
invalid, and embedded-address values do not match. `SQL-LOG-018` gives direct
SQLite/libSQL users an executable bounded public-row equivalent with packed
integer bounds; LogsQL grammar, composition, limits, cancellation, and errors
remain Rust API behavior.

Exact-build evidence over 8,192 retained rows measures CIDR matching at
3.147/37.651 ms narrow/wide p95 and explicit bounds at 2.827/37.312 ms. The
equivalent same-run word filter measures 3.001/32.456 ms. Every narrow shape
reads one block and 1,024 entries; every wide shape reads four blocks and all
8,192 entries. This is bounded API evaluation over byte-identical public
reads, not a missing storage primitive.

IPv6 range filters likewise accept one exact address, a CIDR from `/0` through
`/128`, or two inclusive bounds. Address spelling is normalized before
comparison, so compressed and uppercase forms compare by the same unsigned
16-byte network order. IPv4 input is mapped into IPv6 space exactly as in
VictoriaLogs; consequently its CIDR prefix is still 128-bit (`/120` is the
mapped equivalent of an IPv4 `/24`). Missing, null, numeric, invalid, and
embedded-address values do not match. LogsQL grammar, normalization,
composition, limits, cancellation, and errors remain bounded Rust API work.
Portable SQLite has no built-in IPv6 parser, so the cookbook does not claim a
misleading SQL equivalent and no extension scalar is added merely to shorten
language-owned evaluation.

Exact-build evidence over 8,192 retained rows measures IPv6 CIDR matching at
3.290/39.210 ms narrow/wide p95 and explicit bounds at 3.961/40.015 ms. The
equivalent same-run word filter measures 3.117/36.940 ms. Every narrow shape
reads one block and 1,024 entries; every wide shape reads four blocks and all
8,192 entries. The difference is bounded 16-byte parsing/evaluation over
byte-identical public reads, not a missing storage primitive.

String-range filters accept exactly two quoted or unquoted bounds and compare
the complete projected field in unsigned UTF-8 byte order. The lower bound is
inclusive and the upper bound is exclusive; equal or inverted bounds match
nothing. Missing and null project to empty, strings retain their bytes, and
numbers, booleans, arrays, and objects use compact JSON text only while the
predicate runs. Stored metadata is unchanged. Unqualified filters select the
message, arbitrary dotted fields and service aliases are composable, a
trailing comma is accepted, and malformed arity/separators/wildcards fail
explicitly. `SQL-LOG-019` gives direct SQLite/libSQL users the executable
string/missing/null foundation with binary BLOB comparison; rich projection,
LogsQL grammar, logical/pipeline composition, limits, cancellation, and errors
remain bounded Rust API work. VictoriaLogs flattens nested objects before
querying them, while Timeless retains and can compact-project the selected
parent object; this fidelity-preserving distinction is documented and tested.

Exact-build evidence over 8,192 retained rows measures string-field range
matching at 3.471/46.353 ms narrow/wide p95 and numeric-field textual range
matching at 3.581/45.926 ms. The same-run word filter measures 3.629/43.185
ms. Every narrow shape reads one block and 1,024 entries; every wide shape
reads four blocks and all 8,192 entries. The range predicates therefore add
no storage amplification or row crossing and do not justify an extension
primitive.

Length-range filters accept exactly two unsigned bounds. They count Unicode
code points rather than UTF-8 bytes, include both endpoints, treat an inverted
range as empty, and project missing/null to length zero. Strings retain their
text while numbers, booleans, arrays, and objects use compact JSON only during
evaluation. Bounds accept VictoriaLogs-compatible quoted integers, base
prefixes, underscores, `inf`, byte-size expressions, duration expressions,
and a trailing comma; malformed values fail explicitly. `SQL-LOG-020` gives
direct SQLite/libSQL users the executable retained-string/missing/null form
through public rows and `length(TEXT)`. Rich projection, LogsQL grammar,
logical/pipeline composition, limits, cancellation, and errors remain bounded
Rust API behavior; no extension or storage contract changed.

Exact-build evidence over 8,192 retained rows measures retained-string
length matching at 3.786/48.626 ms narrow/wide p95 and numeric-field textual
length matching at 4.335/47.566 ms. The same-run word filter measures
4.002/55.308 ms. Every narrow shape reads one block and 1,024 entries; every
wide shape reads four blocks and all 8,192 entries. Storage is byte-identical
to the preceding string-range capture, so the operation remains bounded API
composition rather than a missing extension primitive.

Same-row field comparisons accept exactly one right-hand field:
`left:eq_field(right)`, `left:le_field(right)`, or
`left:lt_field(right)`. An omitted left field selects the message; quoted
identifiers, service/level aliases, nested fields, case-insensitive function
names, one trailing comma, logical composition, and `filter`/`where` pipelines
are supported. `_time` is allowed on the right and rendered in RFC3339 at the
configured timestamp precision. It is rejected on the left because
`_time:` belongs to the time-filter grammar. Invalid arity, separators,
wildcards, and unterminated calls fail before storage work.

Equality compares complete textual projections exactly. Ordering first parses
both projections as VictoriaLogs math values—decimal or base-zero numbers,
durations, byte sizes, RFC3339 timestamps, and IPv4 addresses—and otherwise
uses unsigned UTF-8 byte order. Missing/null project to empty; strings retain
their bytes; and retained numbers, booleans, arrays, and objects use compact
JSON only during evaluation. When both values are retained JSON numbers,
Timeless compares the exact JSON numbers first so integers beyond binary64
precision remain ordered correctly. Stored metadata is never mutated.
`SQL-LOG-021` provides direct SQLite/libSQL users with complete retained-model
equality and the exact bytewise ordering fallback over public rows. The full
math-value ordering branch remains language-owned Rust API behavior; no
extension primitive or storage contract changed.

Exact-build evidence over 8,192 retained rows measures same-field equality at
3.311/43.262 ms narrow/wide p95 and exact retained-number ordering at
3.332/44.726 ms. The same-run word filter measures 3.611/43.125 ms. Every
narrow shape reads one block and 1,024 entries; every wide shape reads four
blocks and all 8,192 entries. The predicates therefore add no storage
amplification or row crossing and do not justify an extension primitive.

Field-set selectors end in one unquoted wildcard: `cmp_*:foo`, `*:foo`, and
`"foo:bar:"*:exact(needle)`. Every atomic filter succeeds when any existing
canonical field with that prefix succeeds. `_msg`, `_time`, and `level` are
canonical special fields; retained objects contribute dotted leaf paths;
arrays and null remain leaves; and object parents are not implicitly matched.
A field-scoped `AND` may use a different matching field for each atom, `NOT`
negates the expanded result, and pipeline filters inspect the current projected
row. Expansion is cancellation-aware, retains only one recursive path, and is
bounded by the decoded row rather than allocating a field list.

Wildcard left operands for `eq_field`, `le_field`, and `lt_field` are rejected.
This intentionally avoids VictoriaLogs' current literal-nonexistent-field
behavior, which can produce surprising empty-projection matches. Direct
SQLite/libSQL callers can use executable `SQL-LOG-022` for literal field-name
prefix selection and retained string/null exactness over public rows. The Rust
API owns the complete LogsQL filter, rich projection, `_time`, grammar, limits,
cancellation, and envelope semantics; the extension storage contract is
unchanged.

Exact-build evidence over 8,192 retained rows measures field-prefix word
search at 3.122/49.085 ms narrow/wide p95 and typed field-prefix search at
3.216/47.324 ms. The matching same-run word and `value_type` baselines measure
3.070/37.476 ms and 3.220/46.281 ms respectively. Every narrow shape reads one
block and 1,024 entries; every wide shape reads four blocks and all 8,192
entries. The visible wide word-search cost is bounded per-row field traversal,
not storage amplification, and does not justify an extension primitive.

Day ranges use `_time:day_range[start, end] offset duration`. Bounds accept
`HH:MM` and `HHMM`; brackets include or exclude the exact timestamp; offsets
accept signed compound VictoriaLogs durations. `24:00` clamps to the last
nanosecond, minute `60` normalizes forward, inverted ranges are valid and
empty, and only `[00:00,00:00)` is the special full-day equal range. Ranges do
not wrap overnight. When `offset` is omitted, Timeless uses UTC explicitly
instead of reading mutable process-local timezone state.

The storage-row evaluator performs one native timestamp remainder and fixed-
offset comparison per decoded row, with no date allocation. Pipeline filters
parse and inspect the current projected RFC3339 `_time`; removal by an earlier
`fields` pipe makes the predicate false. `SQL-LOG-023` exposes the exact millisecond/microsecond public-
row foundation and bracket normalization. The Rust API retains clock/duration
grammar, logical and pipeline composition, errors, limits, cancellation, and
envelopes. Exact-build p95 is 3.697/37.123 ms narrow/wide versus
3.988/43.644 ms for the same-run word baseline. Both paths read exactly one/
four blocks and 1,024/8,192 entries, so the result does not justify an
extension primitive.

Week ranges use `_time:week_range[start, end] offset duration`. Short and full
English weekday names are case-insensitive; Sunday through Saturday are a
linear zero-through-six interval. Open brackets advance/retreat their bound
modulo seven, so `[Sun,Sun)` and `(Sat,Sun)` select the full week while
`[Mon,Mon)` is empty. Other inverted ranges are valid and empty. The offset is
added to UTC before weekday selection, and an omitted offset is deterministic
UTC rather than ambient process-local time.

The storage-row evaluator computes the civil weekday from the native integer
timestamp using Euclidean day arithmetic, preserving pre-epoch dates without
allocating a date object. Pipeline filters parse the current projected RFC3339
`_time`; removal by an earlier `fields` pipe makes the predicate false.
`SQL-LOG-024` exposes the millisecond/microsecond public-row foundation,
normalized bracket inputs, and signed offset operation. The Rust API owns
weekday/bracket/duration grammar, composition, errors, limits, cancellation,
and envelopes. Exact-build p95 is 3.547/39.768 ms narrow/wide versus
3.598/41.150 ms for the same-run word baseline. Both paths read exactly one/
four blocks and 1,024/8,192 entries, so the result does not justify an
extension primitive.

Exact filters accept quoted or unquoted `=value` and the equivalent
case-insensitive `exact(value)` function name. Exact-prefix filters accept
`="prefix"*`, field-scoped forms, and `exact(prefix*)`. They are
case-sensitive and anchored at the first field byte; they do not search later
word boundaries. Strings retain their bytes, while retained numbers,
booleans, arrays, and objects receive compact JSON text only for these
upstream textual predicates. Missing and null receive empty text, so an empty
prefix matches every value. The stored metadata type and bytes are unchanged.
The direct SQL cookbook gives exact message/text-field forms; full rich-value
projection, LogsQL composition, limits, cancellation, and error envelopes
remain API behavior.

Static multi-exact filters accept quoted and unquoted values in `in(...)`,
sort and deduplicate the request-owned list, and apply case-sensitive full-
value membership to the same rich textual projection. `in()` matches nothing;
a trailing comma is accepted; quoted `"*"` is literal; and any standalone
unquoted `*` argument makes the filter a field-independent no-op, matching the
pinned VictoriaLogs behavior. A top-level pipe inside `in(...)` is rejected as
the separately deferred subquery capability instead of being mistaken for a
static value or outer pipeline. `SQL-LOG-015` gives direct SQLite/libSQL users
the parameterized message and retained-text equivalents.

The standalone unquoted wildcard has the same field-independent no-op meaning
inside `contains_any(...)` and `contains_all(...)`, including mixed lists and
missing fields. Function names are case-insensitive, and logical/pipeline
composition treats the result as a constant true predicate. Non-wildcard
`contains_all` requires every static phrase while `contains_any` requires at
least one. `contains_all()` and empty arguments are true identities;
`contains_any()` is false, while any empty argument makes it true without
inspecting the field. Both preserve case, Unicode word boundaries, compact
rich-value projection, aliases, and logical/pipeline composition. Query-backed
lists remain explicitly deferred as `LQL-F38`.
`SQL-LOG-016` shows the direct SQL equivalent: omit the field predicate.

`field:json_array_contains_any(v1, ..., vN)` inspects only a retained JSON
array. It compares top-level strings, numbers, booleans, and null to the exact
static candidate text, ignores nested arrays/objects, and returns false for a
missing field, scalar, object, empty array, or empty candidate list. An empty
candidate matches only an empty-string element. A quoted star is literal; an
unquoted star is invalid; a trailing comma is accepted; and the function name
is case-insensitive. Timeless compares decoded semantic JSON, so escaped stored
strings compare by their decoded value rather than VictoriaLogs' raw-lexeme
shortcut. Grammar, composition, limits, and cancellation stay in this Rust
API. Direct SQLite/libSQL users use public `json_each` through executable
`SQL-LOG-017`; no extension primitive or storage change is involved.

Double- and single-quoted strings decode VictoriaLogs-compatible Go escapes,
backtick strings are raw, and quoted field identifiers select one literal
metadata key. Unsupported syntax is rejected rather than ignored. The exact
compatibility choices and intentional typed-data differences are recorded in
the feature matrix and query findings.

The release binary requires Phoenix-managed policy authentication by default.
Backup and cluster administration remain in Phoenix; this process deliberately
contains no generic metrics/traces abstraction.

## Run

```bash
cargo build -p timeless-ext --release
cargo build --manifest-path servers/Cargo.toml --release

TIMELESS_AUTH_MODE=disabled servers/target/release/timeless-logs-api \
  target/release/libtimeless_ext.so \
  /tmp/timeless-logs-api.db \
  127.0.0.1:19429
```

`TIMELESS_AUTH_MODE=disabled` is only for an isolated local benchmark. A
release omits it and supplies `TIMELESS_AUTH_POLICY_FILE` and
`TIMELESS_TENANT` through the Phoenix supervisor.

The positive release controls are:

- `TIMELESS_LOGS_READER_CONNECTIONS` (default `2`)
- `TIMELESS_LOGS_COMMAND_QUEUE_BATCHES` (default `256`)
- `TIMELESS_LOGS_FLUSH_INTERVAL_SECS` (default `1`)
- `TIMELESS_LOGS_OPTIMIZE_INTERVAL_SECS` (default `30`)
- `TIMELESS_LOGS_LOGSQL_MAX_RESULT_ROWS` (default `100000`, hard maximum
  `100000`)
- `TIMELESS_LOGS_LOGSQL_MAX_WORK_ROWS` (default `100000` decoded/examined
  entries)
- `TIMELESS_LOGS_LOGSQL_MAX_RESPONSE_BYTES` (default `16777216`)
- `TIMELESS_LOGS_LOGSQL_DEADLINE_MS` (default `30000`)

The measured reader default is two: one reader materially increased query tail
latency, while four and eight added memory without a useful throughput or tail
latency return. These are deployment controls only; they do not change query or
storage semantics.

The API uses one SQLite writer and a small pool of SQLite readers. Retryable
extension publication conflicts wait inside the API rather than leaking as
HTTP 500 responses. Health and stats expose admitted/completed work, queue
depth and age, API phase timers, extension flush/query/optimize counters, and
read-permit/writer-wait counters so admission cannot be confused with
completed SQLite ingestion. Query telemetry separately reports
`api_query_in_flight`, `api_query_cancelled`, `api_query_errors`,
`api_query_result_rows`, and `api_query_response_bytes`; in-flight work is not
decremented until the SQLite reader has actually stopped. `index_size` is the
SQLite page bytes allocated to the logs posting/timestamp/meta structures;
`term_postings` is their posting row count. These are deliberately separate
units. Storage totals, the declared timestamp unit, index allocation, and the
optimizer source sample all come from public `timeless_stats('logs')` rows.
The server never reads extension-owned shadow block, term, or metadata tables;
ordinary SQLite page/freelist PRAGMAs provide only whole-database accounting.

The server requires the extension capability
`query_surfaces.{timeless_logs,timeless_log_count,timeless_log_values}.max_work_entries`
and the `query_surfaces.timeless_log_query_stats` flags `request_local`,
`same_connection`, and `single_use`. It binds the positive hard guard on every
row, count, and value-discovery request. Direct callers may use the backward-
compatible unbounded arities or provide the same trailing/hidden input
explicitly:

```sql
SELECT ts, level, message FROM logs
 WHERE service='api' AND max_work_entries=100000
 ORDER BY ts DESC LIMIT 100;

SELECT n FROM timeless_log_count(
  'logs', '{"level":"error"}', NULL, :start_us, :end_us, 100000);

SELECT value FROM timeless_log_values(
  'logs', 'host', NULL, NULL, :start_us, :end_us, 1000, 100000);
```

LogsQL `| query_stats` emits one row with VictoriaLogs' fourteen field names
and string values. The server fully consumes the bounded public row scan and
then consumes `timeless_log_query_stats('logs')` on that same serialized reader
connection. It substitutes complete typed post-filter cardinality for
`RowsFound`, measures duration through the pipeline position, and allows later
pipelines. A new, failed, or cancelled scan clears a stale report, and a report
can be read only once.

When `query_stats` is the first pipe, the API returns that one report row
without formatting every matched log into response JSON first. The bounded
public storage scan and its physical counters are unchanged; later pipes still
run over the report row, and a `query_stats` placed after another transform
retains ordered pipeline behavior.

Exact-build evidence measures the first-pipe path at 3.436/25.046 ms
narrow/wide p95 versus 4.811/41.649 ms for same-run full-row word controls.
Both execute identical one/four-block and 1,024/8,192-entry reads; the report
is 380/385 response bytes instead of 34,677/2,249,775 bytes. The internal API
timer is 4.6%/3.1% below the controls after removing discarded row formatting.

Timeless reads one encoded rich payload instead of separate VictoriaLogs
column files. `BytesReadValues` and `BytesReadTotal` therefore contain the same
actual payload byte count; unavailable component byte fields and
`BytesProcessedUncompressedValues` are zero. `ValuesRead` counts severity,
message, and rich metadata slots, while `TimestampsRead` counts timestamps.
A preceding pipeline `limit` does not undo work already performed by the eager
bounded scan. The complete direct-user contract and executable mapping are in
[`SQL-LOG-026`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-026-request-local-log-query-statistics).

LogsQL `first` accepts an optional positive count, parenthesized exact sort
fields with per-field `asc`/`desc`, optional partition fields, and an optional
string rank field:

```text
service:="api" | first 10 by (status desc, _time)
* | first 3 by (duration, _time) partition by (service) rank as position
```

The default count is one. Missing and null values project to empty text. The
sort chain matches pinned VictoriaLogs exact signed/unsigned integers,
RFC3339 times, numeric/duration/byte values, and natural UTF-8 order. Rank
restarts in every partition, partitions use deterministic encoded-key order,
and original public-row order breaks otherwise equal keys. With no `by`, the
operation compares the current pipeline schema, so preceding projection or
deletion is observable. Timeless retains rich JSON response types instead of
flattening them to strings.

`last` has the same grammar, partitioning, rank strings, coercions, current-row
behavior, rich response, and limits. It reverses the complete `first` order;
an explicit field `desc` is therefore reversed once at the field and once at
the operation. Rank one is the first reverse-ordered row in each partition.

The complete input is bounded by `max_work_rows`, output by
`max_result_rows`, and retained sort/partition/index state by
`max_response_bytes`; state overflow returns the same explicit HTTP 422
`query_limit` envelope and leaves the reader reusable. Cancellation covers
key construction, sorting, and output. The implementation reads only public
rows and changes no extension or storage contract. Executable
[`SQL-LOG-027`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-027-first-numeric-rows-per-partition)
gives direct users the exact bounded numeric window-rank foundation and
documents why default SQLite collation is not full LogsQL natural ordering.
[`SQL-LOG-028`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-028-last-numeric-rows-per-partition)
is the executable reverse-order counterpart.

LogsQL `top` groups one or more exact fields from the current pipeline row,
orders groups by hit count descending and textual key ascending, and emits
only the selected summary fields:

```text
service:="api" | top by (level)
* | top 5 service, level hits as total rank as position
```

The default is ten. Parenthesized and bare comma-separated lists are accepted;
`by` and `as` are optional. Missing, null, and empty values share an empty-text
group whose field is omitted from JSON. Selected values, hits, and optional
one-based rank are strings, matching VictoriaLogs summary semantics while the
stored rich source remains unchanged. Result names are made collision-safe
against selected fields. Work, unique group count, retained key/sort state,
result rows, response bytes, and cancellation use the existing query limits.
Executable
[`SQL-LOG-029`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-029-top-values-by-hit-count)
provides the public single-field `GROUP BY`/window-rank foundation. No
extension primitive or private table is used.

LogsQL `uniq` groups one or more exact fields from the current pipeline row
and emits one textual summary row per unique structural key:

```text
service:="api" | uniq by (level) with hits
* | uniq service, level hits limit 20
```

`by` and parentheses are optional; bare multiple fields remain comma
separated. `filter substring` is case-sensitive and is valid for one selected
field. `hits` and `with hits` add a collision-safe string count. `limit 0`
means no operator-specific limit, while positive-limit overflow resets all
retained hits to string `"0"` because discarded group counts are unknown.
Missing, null, and empty values share one empty-text group whose selected
field is omitted from stream JSON. Selected values remain strings while the
stored rich source is unchanged.

VictoriaLogs does not promise output order or which hash-map groups survive a
positive limit. Timeless deliberately returns the first N bytewise structural
keys in stable order. The complete input, unique groups, retained key state,
result rows, response bytes, and cancellation use the existing hard query
limits. Executable
[`SQL-LOG-030`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-030-unique-textual-values)
provides the public single-field `GROUP BY` foundation and matching
deterministic policy. No extension primitive or private table is used.

LogsQL `facets` finds the most frequent nonempty textual values across every
field in the current pipeline row:

```text
service:="api" | facets
* | fields service, level, context | facets 5 max_values_per_field 1000 max_value_len 128
* | facets keep_const_fields
```

The defaults are ten returned values per field, 1,000 tracked unique values
per field, and 128 UTF-8 bytes per value. A field is omitted entirely when any
nonempty value exceeds the byte limit or when its textual cardinality exceeds
the configured maximum. Missing fields, JSON null, and empty strings do not
contribute a facet. Constant fields are omitted by default and retained by
`keep_const_fields` only when their sole nonempty value occurs in every input
row. Numbers and booleans are textual; arrays remain atomic JSON text; rich
objects are exposed as dotted leaves without mutating the stored source.

Commands and modifiers are case-insensitive and modifiers may be reordered or
repeated; the last numeric value wins. VictoriaLogs v1.52.0 parses the
nominally integer arguments through `float64`, so positive fractions are
truncated. Timeless preserves that pinned behavior and rejects zero, negative,
non-finite, missing, nonnumeric, and trailing syntax. Results order by field
name, hit count descending, and bytewise value. The final value tie break is a
Timeless determinism guarantee because the local upstream implementation does
not define equal-hit order.

Input, field/value state, sorting, output allocation, result cardinality,
response bytes, and cancellation use the existing hard query limits. The
operation reads only public rows. Executable
[`SQL-LOG-031`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-031-bounded-facets-over-public-log-fields)
provides the recursive public JSON1/window-function equivalent, including
native timestamp units and canonical special fields. No extension primitive
or private table is used.

Exact-build partitioned/ranked `first` evidence measures 3.681/44.182 ms
narrow/wide p95 while returning 16/64 rows, versus 3.153/37.107 ms for
same-run equal-cardinality time-sort controls. Every pair reads the identical
one/four blocks, decodes 1,024/8,192 entries, and reads
235,778/1,914,055 payload bytes. The accepted 16.8%/19.1% p95 cost is
language composition after the same public scan, not evidence for an
extension primitive.

Exact-build partitioned/ranked `last` evidence measures 3.060/46.268 ms
narrow/wide p95 while returning the same 16/64 rows, versus 3.290/44.012 ms
for same-run `first` controls. The narrow result is 7.0% faster and the wide
result is 5.1% slower; internal API averages differ by -9.8%/+3.9%. Both
directions perform byte-identical public storage work. The small bidirectional
variation is accepted as the cost of the shared final comparator direction,
not evidence for another extension primitive.

Exact-build `top` evidence measures 3.385/35.948 ms narrow/wide p95 while
returning five/eight frequency groups, versus 3.330/38.060 ms for same-scan,
equal-cardinality time-sort controls. The +1.6%/-5.5% p95 and +3.1%/-2.2%
internal API variation follows byte-identical public storage work. `top`
responses are 255/531 bytes versus 130/299 because they additionally contain
string hits and rank. The bounded grouping cost is accepted in the Rust API;
ordinary public SQL already supplies the direct-user grouping foundation.

Exact-build `uniq` evidence measures 3.416/41.705 ms narrow/wide p95 while
returning five/eight textual groups with hits, versus 3.594/39.851 ms for
same-scan, equal-cardinality time-sort controls. The -5.0%/+4.7% p95 and
-10.7%/+0.2% internal API variation follows byte-identical public storage
work. `uniq` responses are 180/411 bytes versus 130/299 because they contain
the requested string hits. The bounded structural grouping cost is accepted
in the Rust API; `SQL-LOG-030` already supplies the direct-user operation.

Malformed LogsQL returns JSON HTTP 400 with `invalid_query` and
`malformed_logsql`; recognized but unsupported syntax returns JSON HTTP 422
with `unsupported_capability` and `unsupported_logsql`. Limits return JSON
HTTP 422 `query_limit`, and deadlines return JSON HTTP 504 `timeout`. Pinned
VictoriaLogs instead uses HTTP 400 text for both parser classes and encodes a
stats count as a JSON string; Timeless intentionally retains the stricter
error distinction and numeric count documented in `QSF-063`.

The ignored end-to-end contract test pins the storage boundary explicitly:

```bash
TIMELESS_EXT_TEST_PATH="$PWD/target/release/libtimeless_ext.so" \
  cargo test --manifest-path servers/Cargo.toml \
  -p timeless-logs-api \
  --test api_e2e -- --ignored
```

It proves that a 100-entry HTTP request remains buffered with zero raw blocks,
and that reaching exactly 8,192 entries triggers the extension's own four
level-partitioned raw blocks with zero compressed blocks. No API flush occurs
between those requests.

## POC performance history

The deterministic Session 1 baseline reaches 478.7K completed entries/s with
no queries. With one and two query workers, it saturates at 162.3K and 85.5K
completed entries/s respectively while the unchanged Elixir API reaches
489.5K and 465.5K. Extension telemetry locates the difference: mixed queries
held read permits for 7.53–10.31 aggregate seconds while writers waited
7.06–7.56 seconds.

Session 2 writer fairness raises completed ingestion at equal offered load
from 162.3K to 225.5K entries/s with one query worker and from 85.5K to
152.0K with two. New readers retry while a writer is queued, so they cannot
starve it; the logs cursor also releases its permit before metadata JSON
rendering. Both measured runs had zero HTTP errors and drained to zero.

Session 3 then moves payload decoding, filtering, sorting, and JSON rendering
past the publication boundary. SQLite's read snapshot keeps captured block
locations readable while the extension streams one payload at a time, so this
does not retain every candidate payload in memory. With one and two query
workers the API reaches 479.7K and 463.3K completed entries/s respectively.

Session 4 pushes exact `ORDER BY ts ASC|DESC LIMIT/OFFSET` windows through the
virtual-table planner into a bounded engine query. The engine retains at most
`LIMIT + OFFSET` entries and stops on block timestamp bounds. An isolated
latest-100 over 3.109M raw entries returned 100 engine rows in 77.91ms and
skipped 1,424 of 1,492 candidate blocks.

Session 5 moves the remaining broad shapes into shared extension primitives.
The public hidden `message_contains` column performs exact case-insensitive
substring matching inside the engine and participates in bounded timestamp
windows. The existing `message LIKE` path remains for SQLite-compatible LIKE
semantics. Direct callers can use the scalar TVF:

```sql
SELECT n FROM timeless_log_count(
  'logs', '{"level":"error","service":"api"}', 'timeout', :start, :stop
);
```

The API uses these same two surfaces. Fully covered unfiltered or level-pure
blocks count from persisted metadata; other filters stream and decode one
block at a time without materializing matching rows.

With one and two query workers, the pinned mixed workload completed 477.7K
and 471.7K writes/s, query p99 was 237ms and 242ms, and Linux process HWM was
124,504KiB and 105,060KiB. Session 4 measured 458.8K/467.3K, 1.83s/1.95s,
and 5.66GiB/6.84GiB. Both Session 5 runs drained to zero with no HTTP errors
or writer timeouts. The two-reader run answered 102 native counts entirely
from 7,637 metadata rows (2,910,678 entries, zero payload reads), while all
407 row queries—including substring—used bounded execution.

The POC still uses the unchanged storage mechanism. No alternate buffer size,
block layout, partition scheme, or durability policy was introduced to hide
the result. Session 5 closes the whole-workload embedded-memory gate.

Session 6 changes shared extension compaction policy, not API storage. Raw
compression and compressed merges are disjoint, merge generations require
half-full output plus 2x growth, and a bounded 125% target ceiling prevents
equal half-full tiers from becoming stranded. Public stats expose both phases
and actionable/deferred backlog. In the deterministic repeated-maintenance
benchmark, entry rewrite amplification fell from 7.755x to 2.414x, aggregate
optimize time fell 61.2%, optimize p95 fell 40.6%, and compressed payload grew
only 2.1%. The API merely schedules that public capability from observed
backlog bytes.

Session 7 selects two SQLite readers as the measured default. In the pinned
1/2/4/8-reader sweep, two cut query p99 from 383ms to 261ms relative to one;
four saved only another 10ms while adding 34MiB HWM, and eight regressed to
287ms while reaching 125MiB HWM. Completed ingestion stayed 468–478K entries/s
with no final queue or errors, so neither an API query-admission layer nor
host-side transaction grouping had a measured problem to solve.

Against the established Elixir API at two query workers, Rust completed
470.2K versus 466.9K entries/s, query p99 was 261ms versus 1.61s, and process
HWM was 62,340KiB versus 1,663,480KiB. A retained ~3.1M-entry maintenance
drain compressed the Rust payload to 27.6MB in 5.55s aggregate optimize work
and 32,404KiB HWM; Elixir produced 46.8MB in 13.90s and 863,576KiB HWM. SQLite
retains freed pages until vacuum/reuse: Rust's physical file remained 477.9MB
versus Elixir's 223.9MB block-plus-index footprint after drain. The stats
intentionally distinguish logical compressed payload from database file
high-water.

The release-grade Session 12 LogsQL evidence uses the same 8,192-entry rich
fixture and unchanged four raw blocks. Across word, prefix, substring, regexp,
case-insensitive, exact, empty, any-value, numeric, logical-value-type, and
boolean queries, indexed-narrow p95 spans 2.115–2.903ms and full decoded p95
spans 15.653–28.732ms. Narrow plans consider one block/1,024 entries; wide
plans consider four/8,192. Physical database/WAL/SHM bytes remain exactly
1,190,496. Whole-process HWM is 58,500KiB, 4,252KiB above the Session 10 run;
that measured increase is retained in `QSF-075` rather than attributed to a
storage optimization.

Session 13 retains that fixture and storage layout while adding typed field
discovery, projection, ordered filtering, unique/value statistics, numeric
aggregates, median, and rates. Indexed-narrow p95 spans 2.246–4.007ms; full
8,192-entry p95 spans 22.360–31.108ms. Narrow plans consider one block/1,024
entries and wide plans four blocks/8,192 entries. Physical database/WAL/SHM
bytes remain exactly 1,190,496. Whole-process HWM is 64,068KiB, 5,568KiB above
Session 12 after 18 additional typed query shapes; `QSF-081` records the
bounded increase and the decision to keep composition in the Rust API rather
than add a storage primitive without evidence of avoidable direct-user work.

Session 16 adds VictoriaLogs-compatible structural pattern matching without
changing that public storage path. Full-message pattern matching measured
2.329ms/23.139ms narrow/wide p95; matching the textual projection of a nested
numeric field measured 2.381ms/28.023ms. These shapes perform the same
one-block/1,024-entry narrow or four-block/8,192-entry wide reads as the
existing word, regexp, and typed-value filters. Physical bytes remain exactly
1,190,496 and whole-process HWM is 64,812KiB. `QSF-113` keeps the typed-field
composition cost visible and rejects a new extension primitive without
evidence that it would remove storage work for direct SQLite/libSQL users.
`QSF-112` separately preserves one non-reproduced decoder failure and the
new exact rich-block stress/forensic regressions; it is not mislabeled as a
pattern-query fix.

Session 16 exact-prefix matching retains the same decode-first plan. Message
prefixes measured 2.103ms/21.985ms narrow/wide p95, considering one block and
1,024 entries or four blocks and 8,192 entries respectively. A nested numeric
prefix measured 1.935ms/18.252ms while returning only 25/1,639 API rows; it
still crossed the same 128/8,192 public candidate rows and read the same
132,676/1,088,919 payload bytes. Physical storage remains exactly 1,190,496
bytes and whole-process HWM is 65,912KiB. `QSF-115` records that the selective
result size—not storage pushdown—explains the lower typed-prefix latency and
keeps the operation in the Rust API.

Session 16 static multi-exact membership also retains that public plan.
Two-value message membership measured 2.077ms/15.273ms narrow/wide p95 while
returning two rows; nested numeric membership measured 2.235ms/22.864ms while
returning 51/3,277 rows. Both use the same one-block/1,024-entry or four-block/
8,192-entry reads as the other filters. Physical storage remains exactly
1,190,496 bytes and whole-process HWM is 65,780KiB. `SQL-LOG-015` exposes
ordinary parameterized `IN` and existing hidden-column pruning for declared
string-only index keys; `QSF-118` records why rich typed membership remains
bounded Rust composition rather than a new extension primitive.

Session 16 field no-ops measured 2.344ms/23.509ms narrow/wide p95 while
returning 128/8,192 rows. They perform the same one-block/1,024-entry or four-
block/8,192-entry public reads and return the same 21,826/1,424,639 response
bytes as the comparison filters. Wide p95 is 2.7% above the same-run word
query and 11.4% below the empty-field query; these are run/predicate variation
over byte-identical storage work. Physical storage remains 1,190,496 bytes and
whole-process HWM is 65,892KiB. `SQL-LOG-016` is the exact direct-user
constant-true form; `QSF-120` rejects a redundant extension primitive.

Session 16 static `contains_all` measured 2.073ms/26.408ms message and
2.664ms/30.442ms rich-object narrow/wide p95 while returning 128/8,192 rows.
All four shapes perform the same one-block/1,024-entry or four-block/8,192-
entry public reads and return the same 21,826/1,424,639 response bytes as
equal-cardinality comparisons. Message-wide p95 is 11.0% above the same-run
word query; rich-object wide p95 is 11.1% above the existing rich-pattern
query because it projects JSON and checks two phrases per row. Physical
storage remains 1,190,496 bytes and whole-process HWM is 65,004KiB.
`QSF-122` retains the measured decode-first cost and rejects both a redundant
extension primitive and an inexact portable-SQL claim.

Session 16 static `contains_any` measured 2.143ms/22.301ms message and
2.383ms/27.391ms rich-object narrow/wide p95 while returning 128/8,192 rows.
All four shapes perform the same one-block/1,024-entry or four-block/8,192-
entry public reads and return the same 21,826/1,424,639 response bytes as
equal-cardinality comparisons. Message p95 is 1.3% above/4.0% below the
same-run word query; rich-object p95 is 9.6%/8.0% below `contains_all` and
17.8%/5.8% above rich pattern matching. Physical storage remains 1,190,496
bytes and whole-process HWM is 64,604KiB. `QSF-124` retains the bounded API
composition and rejects both a redundant extension primitive and an inexact
portable-SQL claim.

Session 16 `json_array_contains_any` measured 2.416ms/33.611ms string and
2.447ms/33.549ms boolean narrow/wide p95 while returning 128/8,192 rows. All
four shapes perform the same one-block/1,024-entry or four-block/8,192-entry
public reads. Narrow p95 is within 2.0% of the same-run word query; wide p95 is
16.1–16.3% above it from per-row retained-array/type inspection. Executable
`SQL-LOG-017` gives direct users the exact public JSON1 operation. The added
two-element evidence field raises logical storage to 1,269,143 bytes and
physical database/WAL/SHM storage to 1,371,776 bytes; whole-process HWM is
71,996KiB after four additional full-response shapes. `QSF-126` retains those
costs and rejects a new extension primitive without evidence of avoidable
storage work.

The measured follow-up work is organized in
[`LOGS_MIXED_WORKLOAD_PERFORMANCE_PLAN.md`](../../../LOGS_MIXED_WORKLOAD_PERFORMANCE_PLAN.md).
The pinned Session 1 comparison is reproduced in the
[release-plan baseline table](../../../docs/2026-08-02_rust_telemetry_data_plane_release_plan.md#baseline-validation).
