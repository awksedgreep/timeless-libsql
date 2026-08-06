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

<!-- query-contract-shipped: LQL-F01 LQL-F02 LQL-F03 LQL-F04 LQL-F05 LQL-F06 LQL-F07 LQL-F08 LQL-F09 LQL-F10 LQL-F11 LQL-F12 LQL-F13 LQL-F14 LQL-F15 LQL-F16 LQL-F17 LQL-F18 LQL-F19 LQL-F20 LQL-F21 LQL-F22 LQL-F23 LQL-F24 LQL-F25 LQL-F26 LQL-F27 LQL-F28 LQL-F29 LQL-F30 LQL-F31 LQL-F32 LQL-F33 LQL-F34 LQL-F37 LQL-F38 LQL-F39 LQL-F40 LQL-F41 LQL-P01 LQL-P02 LQL-P03 LQL-P04 LQL-P05 LQL-P06 LQL-P07 LQL-P08 LQL-P09 LQL-P12 LQL-P13 LQL-P14 LQL-P15 LQL-P16 LQL-P17 LQL-P18 LQL-P19 LQL-P20 LQL-P21 LQL-P22 LQL-P23 LQL-P24 LQL-P25 LQL-P26 LQL-P27 LQL-P28 LQL-P29 LQL-P30 LQL-P31 LQL-P32 LQL-P33 LQL-P34 LQL-P35 LQL-P36 LQL-P41 LQL-Q01 LQL-Q02 LQL-Q07 LQL-Q08 LQL-S01 LQL-S02 LQL-S03 LQL-S04 LQL-S05 LQL-S06 LQL-S07 LQL-S08 LQL-S09 LQL-S10 LQL-S11 -->

The POST grammar includes wildcard selection; upper-exclusive relative
windows; RFC3339 and integer Unix s/ms/us/ns absolute bounds with open or
closed native-unit edges; all eight exact severities; service and arbitrary
typed metadata equality; message word, phrase, word-prefix, phrase-prefix,
case-sensitive substring, bounded RE2-compatible regexp, case-insensitive,
full-message exact, start-anchored exact-prefix, and static
`in(v1, ..., vN)` exact membership; field-independent wildcard no-ops for
`in`, `contains_any`, and `contains_all`; static case-sensitive
`contains_all(v1, ..., vN)` and `contains_any(v1, ..., vN)` with VictoriaLogs
phrase boundaries; query-backed `in`, `contains_any`, and `contains_all`
using one exact `fields`/`keep` or `uniq` output; ordered non-overlapping
`seq(v1, ..., vN)` matching and bounded `equals_common_case(...)` /
`contains_common_case(...)` expansion with
the same Unicode boundaries; retained-array primitive membership through
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

`seq(v1, ..., vN)` preserves argument order and duplicates, then resumes each
phrase search after the preceding match. Empty phrases are ignored; an empty
or all-empty sequence is a field-independent true predicate. Malformed
separators and unquoted wildcards fail explicitly. The matcher stores only the
request-bounded phrase list, scans each row monotonically without per-row
allocation, and observes the existing work, deadline, and cancellation limits
in base queries and current-row `filter`/`where` pipelines. No extension
primitive or portable exact SQL recipe is claimed.

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
`first`/`last`/`top`/`sample`.
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

The implemented statistics are `count`, `count_empty`, `count_uniq`,
`count_uniq_hash`, `uniq_values`, `values`, `sum`, `avg`, `min`, `max`,
`median`, `quantile`, `stddev`, `sum_len`, `any`, `field_min`, `field_max`,
`row_any`, `row_min`, `row_max`, `rate`, and `rate_sum`. Missing, null, and
empty remain distinct;
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

`quantile(phi[, fields...])` uses VictoriaLogs textual projection and
signed/unsigned/timestamp/math/natural ordering, then selects
`min(floor(phi*N), N-1)` without interpolation. Phi must be in `[0,1]`.
`stddev(fields...)` is Welford population deviation over native JSON numbers;
numeric strings are not coerced, a singleton is zero, and an empty numeric
selection is JSON null. Exact quantile state and both traversals are bounded
and cancellable. Timeless fails above its configured exact-state limit rather
than copying VictoriaLogs' randomized reservoir. Direct users have the finite
native-number public SQL foundation in `SQL-LOG-044`.

`sum_len(fields...)` sums the textual UTF-8 bytes of exact, prefix, or all
current fields into one checked native JSON integer. Missing/null values add
zero, strings add raw bytes, and other retained values add compact JSON bytes.
Each selection is work-bounded and cancellable. Direct users have the exact-
metadata-path public SQL foundation in `SQL-LOG-045`; dynamic selection,
canonical fields, grammar, limits, and envelopes remain API behavior.

`any(field)` requires one exact field, skips missing/null/empty values, and
returns the first nonempty value in deterministic current-pipeline order with
its retained JSON type. This deliberately strengthens VictoriaLogs' arbitrary
physical-encoding-dependent selection. `field_min(source,result)` and
`field_max(source,result)` compare nonempty sources through VictoriaLogs'
signed/unsigned/timestamp/math/natural text order, retain the first tie, and
return the companion's native rich value. Missing companions and empty input
produce an empty string; explicit null and empty remain distinct. Candidate
work and retained rich state are bounded and cancellable. `SQL-LOG-046` gives
direct users the deterministic exact-path and finite-native-number public SQL
foundations; complete comparison, canonical fields, grammar, limits, and
envelopes remain API behavior.

Exact-build p95 is 3.293/33.764 ms narrow/wide for `any` versus
4.476/37.518 ms for equal-output `min` controls. Two companion extrema measure
3.306/36.738 ms versus 3.704/38.270 ms for equal-output numeric extrema
controls. Every comparison scans one/four blocks, decodes 1,024/8,192 entries,
reads 235,778/1,914,055 payload bytes, and materializes 128/8,192 public rows.
The bounded deterministic/rich reductions therefore stay in this Rust API;
`QSF-197` records the complete tails, HWM, and unchanged storage verdict.

`row_any(fields...)` selects the first current row with any nonempty exact,
flattened-prefix, or all-current field and returns the complete selected native
JSON object. Prefixes descend through nested objects and reconstruct their
shape. `row_min(source[,fields...])` and `row_max` require an exact comparison
source, use the complete VictoriaLogs natural comparator, retain the first
tie, and default to all current result fields. Missing selected paths are
omitted; null, empty, false, zero, arrays, and objects remain typed; no match
returns `{}`. Function names are case-insensitive and aliases accept `as name`
or the implicit `name` form. Work, deep traversal, selected state, response
bytes, and cancellation are bounded. `SQL-LOG-047` provides direct users the
fixed-path rich-row and finite-number public SQL foundation; no extension
primitive or private storage access is used.

Exact-build p95 is 3.097/37.829 ms narrow/wide for `row_any` versus
3.660/37.888 ms for scalar `any`; rich row extrema measure 3.219/39.790 ms
versus 3.429/36.227 ms for scalar companion extrema. The rich extrema wide
tail is 9.8% higher and is retained. Each pair scans identical one/four
blocks, decodes 1,024/8,192 entries, reads 235,778/1,914,055 payload bytes,
and materializes 128/8,192 public rows. `QSF-199` records the complete tails,
larger typed-object responses, HWM, and unchanged storage verdict.

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
pinned VictoriaLogs behavior. `SQL-LOG-015` gives direct SQLite/libSQL users
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
lists use the same predicates under `LQL-F38`.
`SQL-LOG-016` shows the direct SQL equivalent: omit the field predicate.

`equals_common_case(v1, ..., vN)` and `contains_common_case(v1, ..., vN)`
implement VictoriaLogs' deliberately narrower common-case expansion. For each
phrase, the API includes the whole-string Go-simple uppercase form and every
combination in which each Unicode uppercase-letter (`Lu`) rune independently
stays uppercase or uses its Go-simple lowercase mapping. It does not perform
general case-insensitive matching or Unicode normalization. Exact common-case
matching uses full textual equality; contains common-case uses the established
Unicode letter/digit/underscore phrase boundaries. Empty lists are false; an
empty exact phrase selects empty textual projections, while an empty contains
phrase is field-independent true. A trailing comma is accepted, a quoted
`"*"` is literal, and an unquoted wildcard is invalid.

One phrase may contain at most ten uppercase-letter runes, and one request may
expand at most 8,192 distinct values and 4 MiB of parser state. Expansion is
sorted, deduplicated, request-owned, and composed into existing exact/phrase
predicates over public rows. The extension and stored data are unchanged.
Direct SQL users may pre-expand a fixed phrase and use parameterized `IN` for
the exact half, but core SQLite has no portable Go-simple Unicode case mapper
or exact VictoriaLogs phrase-boundary predicate, so no complete SQL recipe or
extension primitive is claimed.

Query-backed `in`, `contains_any`, and `contains_all` parse the nested source
as a complete LogsQL plan with the same request clock. The source must end in
one exact `fields`/`keep` field or one-field `uniq`; `fields` output is
deduplicated automatically and is not truncated by the ordinary implicit
100-row response limit. Nested fields are read back through the canonical
flattened `uniq` result name. Missing/null output becomes empty text, while every
rich value uses the established compact projection without storage mutation.
An empty source makes `in`/`contains_any` false and `contains_all` true.
Equivalent sources run once per request; nesting is capped at eight levels and
32 lists; decoded work, materialized state, result rows, and the deadline are
cumulative. The cache is dropped before the outer scan. Every read uses the
public `logs` row/pipeline surface, and unresolved dynamic predicates fail
closed before reaching storage. `SQL-LOG-048` gives direct SQLite/libSQL users
the bounded two-scan retained-string foundation. LogsQL syntax, nested virtual-
table cursors, and private shadow tables remain outside the extension.

Ordered `seq(...)` filters use the same rich textual projection and Unicode
phrase boundaries, but order and duplicates matter. They remain API-owned:
portable SQLite cannot express the boundary contract exactly, while moving the
matcher into the extension would not avoid required public-row decode.

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

LogsQL `sample N` retains a random `1/N` subset at its current ordered
pipeline position:

```text
service:="api" | sample 4
* | fields service, context | sample 100
* | sample 1 | stats count() as total
```

`N` follows pinned VictoriaLogs positive-unsigned parsing: decimal and
base-zero integers, quoted values, byte-size and duration suffixes, and
`inf`/`+inf` are accepted. Zero, negative, invalid-octal, unsuffixed
fractional, missing, extra, and parenthesized values are explicit HTTP 400
errors. Commands are case-insensitive and `sample 1` preserves every row
exactly.

Each request uses a fresh random generator and VictoriaLogs-compatible
exponentially distributed gaps rather than deterministic every-Nth selection.
Retained rows stay in input order with all current strings, numbers, booleans,
arrays, objects, nulls, and nested fields unchanged. A first-stage sample
compacts public `QueryRow` values before metadata JSON materialization; a later
sample operates on preceding projections or transforms. Both paths compact in
place and check cancellation without allocating a second full rowset.

The public scan remains bounded by `max_work_rows`; sampled output remains
subject to `max_result_rows`, `max_response_bytes`, the request deadline, and
cooperative cancellation. Executable
[`SQL-LOG-049`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-049-bounded-random-log-sample)
provides the public SQLite/libSQL `1/N` Bernoulli-sample equivalent. Its random
sequence is intentionally not presented as the upstream exponential-gap RNG.
No extension primitive, private table, storage-format change, or mutation is
used.

Exact release-build evidence over 8,192 rich rows measures `sample 4` plus a
scalar count at 3.027/3.657/3.910 ms narrow and 25.179/26.060/26.533 ms wide
p50/p95/p99. Exact `sample 1` controls measure 3.140/3.307/3.447 and
32.412/33.206/33.678 ms. Wide p95 and request-attributed API time are 21.5%
and 22.4% lower because discarded rows never reach metadata JSON
materialization. Narrow p95 is 10.6% higher while internal API time is 4.4%
lower, so that small-query tail is retained as endpoint variation. Both pairs
perform identical public scans; the capture gate rejects native count or any
control with different requested entries, candidate blocks, decoded entries,
payload bytes, matches, or returned rows.

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

LogsQL `coalesce` writes the first nonempty textual source to an exact current
row destination:

```text
* | coalesce(trace_id, request_id) default unknown as correlation_id
* | coalesce(context.*, service) as context.primary
* | fields error, message | coalesce(error, message)
```

Sources are parenthesized exact, all-field, or suffix-star prefix filters and
are evaluated left to right with duplicate expanded names suppressed.
Missing, null, empty strings, and exact rich-object parents are skipped;
object leaves participate as dotted names, arrays remain atomic JSON text,
and other rich values use the LogsQL textual projection. Wildcard leaves use
deterministic bytewise name order. A trailing source comma is accepted.

The destination defaults to `_msg`; optional `default` and `as` select a
textual fallback and exact destination. Timeless preserves an explicitly
empty destination in its rich response, unlike VictoriaLogs stream JSON's
empty-column omission. A nested destination that collides with a retained
scalar parent returns HTTP 422 `field_conflict` without data loss. Existing
work/state/result/response limits and cancellation apply. Executable
[`SQL-LOG-032`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-032-first-nonempty-textual-log-field)
provides the ordinary public exact-field equivalent; no extension primitive
or private table is used.

LogsQL `copy` (alias `cp`) clones fields while retaining their sources:

```text
* | copy trace_id as correlation_id
* | cp context.* as copied.*, copied.attempt as retry_attempt
* | copy service saved, host service, saved host
```

`as` is optional and comma-separated pairs execute sequentially. Exact,
all-field, and suffix-star prefix filters are accepted on both sides. Each
wildcard source snapshots recursively flattened leaves at that pair in
deterministic bytewise order, arrays remain atomic, and a prefix destination
substitutes the source prefix. Multiple matches copied to one exact field are
last-write-wins; unmatched wildcard sources are no-ops. An exact source with a
wildcard destination keeps the literal destination name, including its `*`.

Exact values retain their JSON type and their source. Missing exact fields and
exact object parents become explicit empty strings to match the upstream
flattened view. A wildcard-generated empty suffix remains a literal empty
field, distinct from `_msg`; exact quoted `""` remains the message alias.
Replacing a retained object or descending through a scalar destination parent
returns HTTP 422 `field_conflict` without mutating storage. Work, temporary
cloned state, result/response size, and cancellation are bounded. Executable
[`SQL-LOG-033`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-033-copy-one-exact-retained-metadata-field)
provides the direct public exact-field JSON1 foundation. No extension
primitive or private table is used.

LogsQL `rename` (alias `mv`) moves fields in the current response row:

```text
* | rename trace_id as correlation_id
* | mv context.* as moved.*, moved.attempt as retry_attempt
* | rename service saved, host service, saved host
```

`as` is optional and comma-separated pairs execute sequentially. Exact,
all-field, and suffix-star prefix filters are accepted on both sides. Each
wildcard source snapshots recursively flattened leaves at that pair in
deterministic bytewise order. All matched sources for the pair are removed
before destinations are inserted. Prefix destinations substitute the source
prefix; multiple matches moved to one exact destination are deterministic
last-write-wins. Unmatched wildcard sources are no-ops, and later pairs
observe earlier removals and destinations.

Exact strings, numbers, booleans, arrays, null, and empty strings retain their
types. Removed rich leaves prune empty parents, but durable storage remains
unchanged. Missing exact fields and exact object parents produce an explicit
empty destination; object parents and rich empty objects remain intact in the
current row because neither is a flattened leaf. A wildcard-generated empty
suffix remains a literal empty field distinct from `_msg`. Exact sources with
wildcard destinations preserve the literal destination, including `*`.

Replacing a retained object or descending through a scalar destination parent
returns HTTP 422 `field_conflict` without data loss. Work, temporary moved
state and paths, result/response size, and cancellation are bounded.
Executable
[`SQL-LOG-034`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-034-rename-one-exact-top-level-retained-metadata-field)
provides the direct public exact top-level JSON1 foundation and explicitly
documents nested-parent and destination-conflict responsibilities. No
extension primitive or private table is used.

LogsQL `format` interpolates current-row values into an exact destination:

```text
* | format "request from <client_ip>: <_msg>"
* | format if (level:=error) '<uc:service> <q:_msg>' as summary
* | format '<duration_seconds:elapsed>' as elapsed_seconds keep_original_fields
* | format '<urlencode:user>' as encoded_user skip_empty_results
```

Patterns may be quoted or one unquoted token. HTML entities in literal
prefixes are decoded. `<field>` uses retained rich paths and textual
projection; missing/null plus `<_>`, `<*>`, and `<>` produce empty text.
Wildcard fields are rejected. Options are `uc`, `lc`, `q`, URL encode/decode,
hex encode/decode, Base64 encode/decode, numeric-hex encode/decode, `time`,
`duration`, `duration_seconds`, and `ipv4`. Invalid inputs retain their raw
text under the pinned VictoriaLogs rules. Unix integer, fractional, and
scientific forms use exact integer inference and nanosecond RFC3339 output.

The destination defaults to `_msg`; `as` accepts one exact field. Optional
`if (...)` uses the current-row filter language and `if ()` matches all rows.
`keep_original_fields` retains a nonempty existing destination;
`skip_empty_results` does so only for an empty formatted result. Timeless
retains explicit empty strings and source JSON types. Replacing a rich object
or descending through a scalar destination parent returns HTTP 422
`field_conflict`. Work, transform expansion, temporary output, results,
response size, and cancellation are bounded. Executable
[`SQL-LOG-035`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-035-format-two-exact-retained-metadata-fields)
provides direct users the public exact-field JSON1/`printf` foundation. No
extension primitive, private table, or durable storage mutation is involved.

LogsQL `math` and its alias `eval` evaluate bounded binary64 expressions over
the current rich row:

```text
* | math duration + 10e9 as adjusted_ns
* | eval attempts + 1 next_attempt, next_attempt * backoff delay
* | math round(bytes / 1KiB, 0.01) as kib
* | math invalid default 0 as safe_value
```

Entries execute sequentially; `as` is optional, and an omitted destination
uses the canonical expression string. Operators, from tightest to loosest,
are `^`, `*`/`/`/`%`, `+`/`-`, `&`, `xor`, `or`, and NaN-only `default`.
All are left-associative. Unary signs and parentheses compose with `abs`,
`ceil`, `exp`, `floor`, `ln`, `max`, `min`, `now`, `rand`, and `round`.

Constants and fields accept VictoriaLogs numeric, duration, byte-size,
RFC3339, and IPv4 coercions. Missing/null/empty/invalid or retained rich
values produce NaN. Results are strings with fixed finite rendering and
`NaN`/`+Inf`/`-Inf`; bitwise operators use the pinned unsigned conversion for
negative, nonfinite, and out-of-range values. Later entries observe earlier
results, while durable typed inputs remain immutable.

Exact scalar destinations may be replaced. Replacing a rich object or
descending through a scalar returns HTTP 422 `field_conflict`. AST
size/nesting, work, temporary state, results, response bytes, and
cancellation are bounded. Executable
[`SQL-LOG-036`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-036-arithmetic-over-exact-retained-numeric-fields)
provides direct users a parameterized JSON1 arithmetic foundation without
claiming SQLite `CAST` implements LogsQL coercion. Language syntax,
functions, sequential composition, limits, and errors stay in this Rust API;
no extension primitive or private storage table is involved.

Exact-build `math` evidence measures 3.357/39.127 ms narrow/wide p95 versus
3.292/37.655 ms for byte-identical same-scan controls. The accepted
+2.0%/+3.9% p95 cost is bounded expression/coercion/output work after the
unchanged one/four-block public scan; `SQL-LOG-036` remains the direct-user
path for ordinary numeric SQL.

LogsQL `len` counts UTF-8 bytes in one exact current-row field:

```text
* | len(_msg) as message_bytes
* | len unicode byte_length
* | len(nested.value)
```

Parentheses and `as` are optional, matching is case-insensitive, and the
destination defaults to `_msg`. Empty quoted source or destination names also
mean `_msg`. Strings count decoded UTF-8 bytes; numbers and booleans count
their textual spelling; arrays count compact JSON. Missing/null/empty values
and exact rich-object parents count as zero under the pinned flattened-view
policy; nested leaves and canonical `_msg`, `_time`, and `level` fields remain
addressable. Sequential pipes observe earlier results without mutating stored
typed sources.

Only exact sources and destinations are accepted. Replacing an object or
descending through a scalar returns HTTP 422 `field_conflict`. Traversal work,
temporary state, result/response size, and cancellation are bounded.
[`SQL-LOG-037`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-037-utf-8-byte-length-of-one-exact-retained-field)
provides direct users the public JSON1 and `length(CAST(... AS BLOB))`
foundation. No extension primitive, private storage table, or durable format
change is involved.

Exact-build `len` evidence measures 3.785/40.724 ms narrow/wide p95 versus
3.620/36.622 ms for byte-identical same-scan controls. The accepted
+4.6%/+11.2% p95 cost is bounded field lookup, byte counting, and destination
work after the unchanged one/four-block public scan; `SQL-LOG-037` remains
the direct-user path.

LogsQL `hash` computes the VictoriaLogs-compatible hash of one exact
current-row field:

```text
* | hash(user_id) as user_hash
* | hash nested.value value_hash
* | hash(_msg)
```

The command is case-insensitive; parentheses and `as` are optional; and the
destination defaults to `_msg`, including empty quoted source/destination
aliases. The result is seed-zero xxHash64 masked to 53 bits and rendered as a
decimal string. Strings use decoded bytes, numbers and booleans use their
textual spelling, and arrays use compact JSON. Missing, null, empty, and exact
rich-object parents hash the empty byte string under the pinned flattened-view
policy; dotted leaves remain addressable. Sequential pipes observe earlier
results without mutating stored rich values.

Array encoding streams into the hasher after a bounded, cancellable traversal.
Work, temporary state, result/response size, and unsafe destination conflicts
use the normal hard query limits and HTTP 422 `field_conflict` envelope. Core
SQLite/libSQL has no portable exact xxHash64 scalar, so the public SQL cookbook
records an explicit no-recipe disposition. No extension primitive, private
storage table, or durable format changes.

Exact-build evidence measures 3.455/36.785 ms narrow/wide p95 versus
3.481/36.223 ms for same-public-work copy controls. Every pair reads the same
one/four blocks, decodes 1,024/8,192 entries, and transfers
235,778/1,914,055 payload bytes. The -0.7%/+1.6% tail variation and 502/506
additional response bytes keep the operation as bounded API/wire work above
the unchanged storage boundary; `QSF-210` records the verdict.

LogsQL `collapse_nums` replaces eligible decimal and hexadecimal tokens in the
current `_msg` or one exact `at` field with `<N>`:

```text
* | collapse_nums
* | collapse_nums at message_template
* | collapse_nums if (service:=api) at message_template prettify
```

The optional condition is evaluated against the current row, and `prettify`
recognizes the pinned UUID, IPv4, time, date, and datetime shapes after number
collapse. Keyword/order, token-boundary, even-length hexadecimal,
underscore/version/duration, fractional-second, and timezone behavior match
the immutable VictoriaLogs oracle. Invalid modifiers, wildcards, attached
syntax, and tails fail explicitly.

Typed numbers, booleans, and arrays use their flattened textual projection;
missing, null, and object parents project as empty. A no-op preserves the
native Timeless value, while a real transform writes a current-row string.
Sequential composition, cumulative work, temporary state, response size,
deadline, cancellation, immutable storage, optimize, and reopen are bounded
and covered by the real-extension regression. Core SQLite/libSQL has no
portable equivalent tokenizer, so the SQL cookbook records an explicit
no-recipe disposition. No private table, extension primitive, or durable
format changes. Exact-build evidence measures 3.135/34.525 ms narrow/wide p95
versus 3.143/36.735 ms for identical-output, same-public-work controls;
`QSF-212` retains the bounded -0.3%/-6.0% variation without changing the
storage boundary.

LogsQL `decolorize` removes exact ANSI Control Sequence Introducer bytes from
the current `_msg` or one exact field:

```text
* | decolorize
* | decolorize "rendered message"
* | decolorize nested.output | format '<nested.output>' as rendered
```

The command is case-insensitive. Empty quoted field names alias `_msg`;
quoted and dotted exact fields are supported. Wildcards, prefixes,
parentheses, comma-separated fields, attached syntax, and extra tokens fail
explicitly. The scanner removes `ESC [` followed by parameter bytes
`0x30..0x3f`, intermediate bytes `0x20..0x2f`, and an optional final byte
`0x30..0x7e`. Incomplete CSI is removed, an invalid final byte remains, and
OSC/DCS sequences are unchanged.

Strings use their decoded bytes. Numbers, booleans, and arrays use the pinned
textual projection; missing, null, and object parents project empty. If no CSI
is removed, Timeless keeps the native value rather than flattening it. A real
removal writes a string to the request-owned current row, later pipes observe
it, and durable rich storage remains immutable. Work, state, result/response,
deadline, cancellation, conflicts, optimize, shutdown, and reopen use the
shared hard contracts.

[`SQL-LOG-050`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-050-strip-csi-color-sequences-from-one-exact-field)
provides direct users an exact BLOB-state recursive CTE over the public `logs`
surface. The Rust API owns language parsing, composition, native no-op
preservation, limits, cancellation, and HTTP envelopes. Every source row has
already crossed that bounded public surface, so no private table, extension
primitive, or durable format change is involved. Exact-build p95 is
3.101/36.385 ms narrow/wide versus 3.169/34.872 ms for identical-output,
same-public-work controls. `QSF-214` accepts the -2.2%/+4.3% endpoint-tail and
+1.4%/+3.7% request-attributed mean differences as bounded current-row work
after identical public storage reads.

LogsQL `split` divides `_msg` or one exact current-row field by a literal
separator and writes compact VictoriaLogs-compatible JSON-array text:

```text
* | split ","
* | split "::" from source as parts
* | split "," source parts
* | split "" unicode runes
```

The command and optional `from`/`as` keywords are case-insensitive and may be
omitted in the upstream shorthand. Source defaults to `_msg`; destination
defaults to source. Exact quoted and dotted fields are accepted. Missing
operands, wildcards, prefixes, commas, parenthesized call syntax, attached
suffixes, and trailing syntax fail explicitly. A keyword-like separator must
be quoted.

Literal splitting is non-overlapping and retains leading, trailing, and
consecutive empty pieces. An empty separator iterates Unicode scalar values;
an empty source therefore yields `[]` for an empty separator and `[""]` for a
nonempty separator. The API emits a JSON-array string with exact VictoriaLogs
escaping, not a native retained array. Numbers, booleans, and arrays use their
textual projection; missing, null, and object parents project empty. Writing
to another destination preserves the original typed source. Sequential
stages see the result, while stored rows remain immutable.

[`SQL-LOG-051`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-051-literal-split-of-one-exact-field)
provides direct users a recursive CTE/JSON1 foundation over bounded public
`logs` rows. The Rust API owns grammar, exact wire spelling, nested current-row
destinations, conflicts, cumulative work/state/result/response limits,
deadline, cancellation, and envelopes. No private table, extension primitive,
or durable storage contract changes.
Exact-build p50/p95/p99 is 3.219/3.481/4.063 ms narrow and
37.529/38.655/40.113 ms wide, versus 3.078/4.786/4.878 and
37.964/40.047/40.151 ms for identical-output, same-public-work controls.
`QSF-216` accepts the 27.3%/3.5% lower endpoint p95 and +3.1%/-1.4%
request-attributed mean differences as bounded whole-run/API variation after
the unchanged public scan; it does not claim storage pushdown.

LogsQL `drop_empty_fields` removes null and empty-string fields from the
current rich row:

```text
* | drop_empty_fields
* | fields case, optional, nested | drop_empty_fields
* | format "" as transient | drop_empty_fields
```

The case-insensitive command accepts no arguments. Missing fields are already
absent; zero, false, nonempty strings, and arrays (including `[]`) retain their
native types. Objects are traversed recursively, empty parents are pruned, and
arrays remain atomic. A row with no fields after pruning is omitted. Earlier
pipeline transformations are visible, but durable source metadata is never
mutated.

Traversal uses request-owned public rows in place, a 128-level JSON nesting
ceiling, the request work allowance, periodic cancellation, and shared final
result/response limits. Executable
[`SQL-LOG-038`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-038-drop-one-empty-retained-metadata-field)
provides direct users the public JSON1 recipe for one known metadata path.
Dynamic field discovery, canonical fields, recursive parent/row pruning,
limits, and envelopes stay in this Rust API. No extension primitive, private
storage table, or durable format change is involved.

Exact-build `drop_empty_fields` evidence measures 4.542/38.151 ms narrow/wide
p95 versus 6.994/35.779 ms for byte-identical same-scan controls. The accepted
-35.1%/+6.6% p95 variation is bounded in-place rich-row traversal after the
unchanged one/four-block public scan; responses are byte-identical and
`SQL-LOG-038` remains the direct-user path for known fixed-schema fields.

LogsQL `replace` performs bounded literal substring replacement in one exact
current-row field:

```text
* | replace ("_", "-")
* | replace ("_", "-") at host limit 1
* | replace if (kind:=admin) ("secret", "***") at password
```

The case-insensitive command requires exactly two parenthesized literal
substrings. `at` defaults to `_msg`; only exact quoted or dotted fields are
accepted. A missing or zero limit replaces every non-overlapping occurrence,
while a positive limit replaces the first `N`. An empty old substring is a
no-op. Optional `if (...)` is evaluated against the current row, and sequential
transforms observe earlier results. Attached `replace(foo,bar)`, wildcard
targets, invalid limits, wrong arity, and trailing syntax fail explicitly.

Strings, lowercase booleans, numbers, and compact JSON arrays use the pinned
VictoriaLogs textual projection. Missing fields, null, and exact object
parents project to empty text. A no-match or empty-old operation preserves the
native retained value; an actual replacement writes a string to the query row
without mutating durable storage. Projected arrays, matches, generated bytes,
work, temporary state, results, response size, and cancellation are bounded.

Executable
[`SQL-LOG-039`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-039-literal-replacement-in-one-exact-retained-field)
provides direct users a parameterized public JSON1/core-`replace()` foundation
for all-occurrence replacement. Conditional, first-`N`, sequential, rich-value,
limit, cancellation, and envelope semantics stay in the Rust API. No extension
primitive, private storage table, or durable format change is involved.

Exact-build `replace` evidence measures 3.520/37.711 ms narrow/wide p95
versus 3.908/36.882 ms for byte-identical same-scan controls. The accepted
-9.9%/+2.2% p95 variation is bounded literal projection, matching, and output
work after the unchanged one/four-block public scan. Responses are
byte-identical and `SQL-LOG-039` remains the direct-user path for ordinary
all-occurrence replacement.

LogsQL `replace_regexp` performs bounded RE2-family replacement in one exact
current-row field:

```text
* | replace_regexp ("[/ ]", "-")
* | replace_regexp ("(?P<name>[a-z]+)-(?P<id>[0-9]+)", "${id}:${name}") at host
* | replace_regexp if (kind:=admin) ("secret=([^ ]+)", "secret=***") limit 1
```

The case-insensitive command requires exactly two parenthesized arguments.
`at` defaults to `_msg`; only exact quoted or dotted fields are accepted. A
missing or zero limit replaces every non-overlapping match, while a positive
limit replaces the first `N`. Optional `if (...)` observes the current row,
and sequential transforms observe earlier results.

Patterns follow the pinned VictoriaLogs/Go RE2-family behavior. Dot matches
newlines by default; `(?-s)` disables that behavior. Backreferences and
lookaround fail during parsing. Empty patterns match UTF-8 boundaries in a
nonempty source. Templates expand `$0`, `$1`, `${1}`, `$name`, `${name}`, and
`$$`; missing captures become empty text, and unbraced capture names consume
the maximal valid name.

Strings, lowercase booleans, numbers, and compact JSON arrays use textual
projection. Missing, null, and exact object-parent values project to empty
text. A no-match preserves the native retained value; an actual match writes
a string only to the request-owned row. Durable storage is never mutated.
Pattern compilation, matching, capture expansion, generated bytes, work,
temporary state, results, response size, and cancellation are bounded.

There is intentionally no SQL recipe for this row. Neither core SQLite nor
the public extension has a portable RE2-compatible capture-replacement
scalar. Direct SQLite users can compose the transformation in their host or
load a separate regexp extension; timeless-libsql does not mislabel that as
ordinary SQL support. The operation remains Rust API composition over public
`logs` rows, with no private-table or durable-format change.

Exact-build `replace_regexp` evidence measures 3.442/40.628 ms narrow/wide
p95 versus 3.391/35.822 ms for byte-identical same-scan controls. The accepted
+1.5%/+13.4% p95 cost is bounded regex matching and capture expansion after
the unchanged one/four-block public scan. Responses and storage work are
byte-identical; no public SQL recipe is claimed where SQLite has no portable
equivalent.

LogsQL `extract` performs bounded literal-delimiter extraction into named
current-row fields:

```text
* | extract 'kind=<kind> id=<id>'
* | extract '<left> &lt; <right>' from comparison
* | extract if (service:=api) 'user=<user>' keep_original_fields
* | extract 'value=<plain:raw_value>' from payload skip_empty_results
```

The case-insensitive command requires at least one named placeholder.
`<>`, `<_>`, and `<*>` are anonymous; adjacent placeholders are invalid; and
literal delimiters are HTML-decoded. The source defaults to `_msg` and may be
one exact quoted or dotted `from` field. A nonempty first delimiter may begin
anywhere, while an empty first delimiter anchors at the start.

Valid Go double/single/raw quoted prefixes are decoded automatically;
`plain:` disables decoding. Missing or partial delimiters produce empty later
captures while retaining completed captures. Default mode writes empty
results, `keep_original_fields` preserves every nonempty destination, and
`skip_empty_results` preserves a nonempty destination only for an empty new
capture. Preserved numbers, booleans, arrays, objects, and nested siblings
remain native. Replacing a retained object with a scalar fails explicitly.
All writes are request-local and sequential; durable metadata is unchanged.

Pattern traversal, quoted decoding, source projection, captures, paths,
work, temporary state, result/response size, and cancellation are bounded.
Executable
[`SQL-LOG-040`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-040-two-literal-delimited-fields-from-one-exact-retained-field)
provides direct users a parameterized public JSON1/core-`instr()`/`substr()`
foundation for two unquoted fixed-delimiter captures. General patterns,
quoted decoding, preservation, limits, cancellation, and envelopes remain in
the Rust API. No extension primitive, private table, or storage-format change
is involved.

Exact-build `extract` evidence measures 3.269/39.052 ms narrow/wide p95
versus 3.201/33.944 ms for byte-identical same-scan controls. The accepted
+2.1%/+15.0% p95 cost is bounded literal scanning, capture decoding, and
current-row writes after the unchanged one/four-block public scan. Responses
and storage work are byte-identical; `SQL-LOG-040` remains the direct-user
foundation for fixed unquoted captures.

LogsQL `extract_regexp` performs bounded first-match RE2-family extraction
into named current-row fields:

```text
* | extract_regexp 'user=(?P<user>[A-Za-z]+) id=([0-9]+)'
* | extract_regexp 'kind=(?<kind>[a-z]+)' from payload
* | extract_regexp if (service:=api) '(?P<request>.+)' from request keep_original_fields
* | extract_regexp '(?P<line>.+)' from "source field" skip_empty_results
```

At least one named `(?P<name>...)` or `(?<name>...)` group is required;
anonymous groups affect matching but create no field. The source defaults to
`_msg` and may be one exact quoted or dotted field. Dot matches newline by
default, inline flags may override it, and backreferences/lookaround or
wildcard fields fail explicitly. Only the first match contributes captures.
No match and unmatched optional groups yield empty results.

Default mode writes empty strings, `keep_original_fields` preserves nonempty
destinations, and `skip_empty_results` preserves a nonempty destination only
for an empty capture. Conditions and writes observe current-row state in
pipeline order. Textual projection covers strings, booleans, numbers, and
compact arrays; preservation retains native rich values. Object-replacing
writes return HTTP 422. Pattern compilation, work, state, captures, paths,
results, response bytes, deadlines, and cancellation are bounded, and no
request-local write mutates durable storage.

There is no claimed SQL recipe. Core SQLite and the public extension have no
portable RE2 named-capture extraction scalar. Direct SQLite/libSQL hosts must
compose it outside SQL or deliberately load a separate regexp extension. The
Rust API uses only public `logs` rows and does not add a private-table,
language-specific extension, or storage-format dependency.

Exact-build `extract_regexp` evidence measures 3.154/35.517 ms narrow/wide
p95 versus 3.922/33.808 ms for byte-identical same-scan controls. The accepted
-19.6%/+5.1% p95 and -1.6%/+6.1% internal API variation is bounded
first-match regex capture and current-row writing after the unchanged
one/four-block public scan. Responses and all storage counters are identical;
no extension primitive can remove the already-required decode and row
crossing.

LogsQL `pack_json` serializes selected current-row fields into a compact JSON
string without changing durable log metadata:

```text
* | pack_json
* | pack_json as packed
* | pack_json packed
* | pack_json fields (host, status, context.*) as packed
* | pack_json fields ("request."*) as request_json
```

The command is case-insensitive. Its destination defaults to `_msg` and may
follow `as` or appear bare. An omitted or empty `fields (...)` list selects
all fields; `*` anywhere in the list also selects all fields. Exact and
prefix selectors may be quoted. Sources are snapshotted before the destination
write, so default packing includes the old `_msg`, and later pipeline stages
observe the new JSON string. A missing exact selection produces `{}`.
Overlapping selectors are an idempotent union with deterministic key order.

Timeless deliberately preserves retained rich metadata: numbers, booleans,
arrays, objects, explicit nulls, empty strings, and empty objects remain their
native JSON types, and dotted prefixes reconstruct nested objects. This is a
documented compatibility difference from VictoriaLogs v1.52.0, which flattens
columns to strings, omits empty values, follows current column order, and may
emit duplicate keys for overlapping selectors. Timeless always emits one
valid deterministic object.

Selection, recursive traversal, paths, temporary JSON bytes, nesting, work,
result rows, response bytes, deadlines, and cancellation use the shared hard
limits. A destination beneath a scalar parent returns HTTP 422. The operation
uses only request-owned public `logs` rows and remains immutable across flush,
optimize, shutdown, and reopen.

Direct SQLite/libSQL users can use executable
[`SQL-LOG-041`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-041-pack-selected-rich-metadata-fields-as-json)
to pack bounded exact metadata paths with public `logs` and JSON1 while
preserving native types and missing/null/empty distinctions. Recursive
LogsQL selectors, current-row destination writes, request limits,
cancellation, and envelopes remain Rust API behavior; no language-specific
extension primitive or private storage access is involved.

Exact-build `pack_json` evidence measures 3.146/37.921 ms narrow/wide p95
versus 3.098/35.717 ms for same-scan plain-field controls. The accepted
+1.5%/+6.2% p95 and -1.3%/+7.4% internal API variation performs identical
public storage work. Packed results are 2,688 bytes versus 1,600 because they
contain the requested JSON object strings. This bounded rich selection and
serialization cost does not justify moving LogsQL syntax into the extension.

LogsQL `pack_logfmt` serializes selected current-row fields into deterministic
logfmt text without changing durable log metadata:

```text
* | pack_logfmt
* | pack_logfmt as packed
* | pack_logfmt packed
* | pack_logfmt fields (host, status, context.*) as packed
* | pack_logfmt fields (missing, *,) as "packed field"
```

The command is case-insensitive. Its destination defaults to `_msg` and may
follow `as` or appear bare; terminal `as` retains the default. Omitted or
empty `fields (...)` selects all current fields, as does `*` anywhere in the
list. Exact and suffix-wildcard prefix selectors may be quoted. Sources are
snapshotted before the destination write, so default packing includes the old
`_msg`; later pipes see the packed text.

Fields are emitted as raw `name=value` pairs in deterministic bytewise-name
order. Missing exact fields, explicit nulls, empty strings, and exact retained
object parents emit an empty value. Recursive all/prefix selection flattens
objects to dotted leaves; arrays remain atomic compact JSON. Values containing
any rune through U+0020, `"`, or `\` use VictoriaLogs-compatible JSON-string
quoting, including `\u003c` and `\u0027` inside quoted values. Other values
remain unquoted.

VictoriaLogs v1.52.0 follows current column order and repeats a field when
selectors overlap. Timeless deliberately uses an idempotent selector union
and deterministic order because its retained rich model has nested objects
and promises stable output. The pinned 1,111-case oracle records upstream
grammar, projection, quoting, and duplicate behavior; direct evaluator and
real-extension tests pin the selected Timeless policy.

Traversal, selected names/values, encoded output, work, results, response
bytes, deadlines, and cancellation use the shared limits. Object-replacing or
scalar-crossing destinations return HTTP 422. Request-local writes never
mutate durable storage and survive optimize, shutdown, and reopen.

Direct SQLite/libSQL users can use executable
[`SQL-LOG-052`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-052-pack-fixed-exact-fields-as-logfmt)
to pack a fixed ordered list of exact public metadata paths with core SQLite
and JSON1, including the same conditional quoting. Dynamic selectors,
canonical/current fields, row mutation, limits, cancellation, and envelopes
remain Rust API behavior. Every value already crossed public `logs`, so no
language-specific extension primitive or private storage access is involved.

Exact-build `pack_logfmt` evidence measures 3.805/39.144 ms narrow/wide p95
versus 3.769/38.212 ms for identical-output `format` controls. The accepted
+1.0%/+2.4% p95 and -0.2%/+8.2% internal API mean performs byte-identical
public storage work and emits the same 2,048-byte responses. Dynamic field
selection and exact conditional encoding remain bounded row-local work and do
not justify moving LogsQL syntax into the extension.

LogsQL `unpack_json` parses one current-row JSON object without changing
durable log metadata:

```text
* | unpack_json
* | unpack_json from payload
* | unpack_json payload fields (host, status, context.*)
* | unpack_json if (kind:=audit) from payload preserve_keys (context)
    result_prefix decoded. keep_original_fields
* | unpack_json from payload fields () skip_empty_results
```

The command and clauses are case-insensitive. The source defaults to `_msg`
and may be one bare or `from` exact field containing whitespace-padded JSON
object text or a native retained object. Omitted or empty `fields ()` selects
all members. Exact missing fields become empty strings; unmatched prefixes add
nothing. `preserve_keys` keeps selected objects atomic and native, while
`result_prefix` prepends a destination namespace.

Timeless preserves strings, numbers, booleans, arrays, objects, explicit
nulls, empty strings, empty objects, nested siblings, and literal dotted JSON
keys. Sources are snapshotted before writes. `keep_original_fields` retains
nonempty destinations, and `skip_empty_results` suppresses null/empty writes.
Malformed object text supplies empties only for explicit exact selectors;
other nonobjects are no-ops. The pinned bare `NaN` token becomes text `NaN`.
Scalar/object path conflicts return HTTP 422. Parsing, paths, selected state,
work, result rows, response bytes, deadlines, and cancellation are bounded.

This deliberately differs from VictoriaLogs v1.52.0 textual flattening. The
pinned 875-case oracle records upstream grammar and behavior; real-extension
tests pin Timeless's rich retained-model policy through optimize and reopen.
Direct SQLite/libSQL users can use executable
[`SQL-LOG-042`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-042-unpack-selected-rich-fields-from-a-json-object)
for fixed exact paths through public `logs` and JSON1. Dynamic LogsQL
selection and request-local writes stay in the Rust API; no private table,
language-specific extension primitive, or storage-format change is involved.

Exact-build `unpack_json` evidence measures 3.151/40.062 ms narrow/wide p95
versus 3.763/38.694 ms for equal-output pack-plus-copy controls. The accepted
-16.3%/+3.5% p95 and -0.5%/+3.3% internal API variation performs identical
public storage work and emits the same 2,112 response bytes. The 65.292 ms
wide p99 is retained honestly. This bounded rich JSON parse/select/write cost
does not justify moving LogsQL syntax into the extension.

LogsQL `json_array_len` counts top-level array elements without changing
durable log metadata:

```text
* | json_array_len(tags)
* | json_array_len(tags) as tag_count
* | json_array_len payload.items item_count
* | json_array_len("left field") as "item count"
```

The command is case-insensitive and accepts one parenthesized or bare exact
source, an optional `as`, and one exact destination that defaults to `_msg`.
Quoted and dotted paths are supported. Wildcards, prefixes, multiple sources,
and trailing tokens fail explicitly. The source is snapshotted before its
request-local destination is written.

Native retained arrays are counted directly. Whitespace-padded JSON-array
text is parsed, including the pinned VictoriaLogs bare `NaN` token. Nested
arrays and objects count as one element. Empty arrays, missing fields, nulls,
malformed text, JSON scalar text, and native scalar or object values return
text `0`. Rich native sources stay unchanged. Object-replacing destinations
return HTTP 422. Parse state, paths, work, result rows, response bytes,
deadlines, and cancellation are bounded, and real-extension tests pin
optimize, shutdown, and reopen behavior.

The complete 897-case pinned VictoriaLogs fixture records upstream grammar
and value behavior. Direct SQLite/libSQL users can use executable
[`SQL-LOG-043`](../../../docs/QUERY_SQL_EQUIVALENTS.md#sql-log-043-top-level-json-array-length)
for a fixed exact native-array or JSON-array-text path through public `logs`
and JSON1. LogsQL grammar, current-row writes, bare-`NaN` compatibility,
limits, cancellation, and envelopes stay in the Rust API; no private table,
language-specific extension primitive, or storage-format change is involved.

Exact-build native-array evidence measures 3.558/41.563 ms narrow/wide p95
versus 3.454/40.607 ms for equal-output constant-format controls. The accepted
+3.0%/+2.4% p95 and +2.4%/-0.0% internal API variation performs identical
public storage work and emits the same 1,344 response bytes. Direct counting
of the already-retained array plus one textual destination write is bounded
API work and does not justify moving LogsQL syntax into the extension.

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

Exact-build recursively flattened `coalesce` evidence measures 3.570/39.597
ms narrow/wide p95 while returning 64 rows, versus 3.329/38.277 ms for
same-scan, equal-cardinality controls. The +7.2%/+3.4% p95 and +10.7%/+5.1%
internal API cost follows byte-identical public storage work. Responses are
1,088 bytes versus 960 because they contain the selected destination field.
The bounded row-local transform is accepted in the Rust API; `SQL-LOG-032`
already supplies direct users the exact-field operation.

Exact-build recursively flattened typed `copy` evidence measures
3.229/46.025 ms narrow/wide p95 while returning 64 rows, versus
3.659/41.958 ms for same-scan, equal-cardinality controls. The
-11.8%/+9.7% p95 and -11.2%/+3.8% internal API variation follows
byte-identical public storage work. Responses are 2,474 bytes versus 2,538
because the copied prefix is one byte shorter than the control field. The
mixed bounded row-transform variation is accepted in the Rust API;
`SQL-LOG-033` already supplies direct users the exact-field operation.

Exact-build recursively flattened typed `rename` evidence measures
3.770/43.696 ms narrow/wide p95 while returning 64 rows, versus
3.553/36.673 ms for same-scan, equal-cardinality controls. The +6.1%/+19.1%
p95 and +11.5%/+18.7% internal API cost follows byte-identical public storage
work. Responses are 2,410 bytes versus 2,538 because destination name `moved`
is two bytes shorter than `context`. The bounded move/prune/rebuild cost is
accepted in the Rust API; `SQL-LOG-034` already supplies direct users the
exact top-level operation.

Exact-build rich textual `format` evidence measures 3.297/39.353 ms
narrow/wide p95 while returning 64 rows, versus 3.090/35.941 ms for
same-scan, equal-cardinality controls. The +6.7%/+9.5% p95 and +4.0%/+11.3%
internal API variation follows byte-identical public storage work. Responses
are 1,706 bytes versus 1,600 because the
formatted field/value is longer than the control. The bounded interpolation,
transform, and output cost is accepted in the Rust API; `SQL-LOG-035` already
supplies direct users exact-field JSON1/`printf` composition.

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

Session 18 ordered `seq` measured 3.906ms/40.608ms message and
3.548ms/48.358ms rich-object narrow/wide p95 while returning 128/8,192 rows.
Same-cardinality `contains_all` controls measured 3.325ms/38.151ms and
3.386ms/47.309ms, so ordered search is 17.5%/6.4% and 4.8%/2.2% higher at p95.
Every pair reads the same one/four blocks, 1,024/8,192 entries, and
235,778/1,914,055 payload bytes and returns identical response bytes. Storage
remains four raw blocks, 1,914,055 logical bytes, and 2,022,736 physical bytes;
whole-process logs HWM is 100,236KiB. `QSF-201` retains the 52.146ms rich wide
p99 and rejects both an inexact SQL claim and a redundant extension primitive.

Session 18 query-backed exact membership measured
5.849/7.341/7.440ms narrow and 32.622/33.501/34.096ms wide p50/p95/p99.
Equivalent static-list controls measured 3.220/4.251/4.312ms and
31.197/35.548/41.786ms. The narrow second scan doubles work to two candidate
blocks, 2,048 decoded entries, and 471,556 payload bytes per request. The wide
shape adds one indexed subquery block to the four-block outer scan: five
blocks, 9,216 entries, and 2,149,833 bytes. Wide internal API time is 1.5%
higher even though endpoint p95/p99 are lower run variation. Both pairs return
identical result cardinality and response bytes. Storage remains four raw
blocks, 1,914,055 logical bytes, and 2,022,736 physical bytes; whole-process
logs HWM is 102,488KiB. `SQL-LOG-048` documents the same two-scan direct SQL
foundation, and `QSF-203` accepts the inherent composition cost without adding
a nested extension cursor.

Session 18 common-case exact matching measured 2.935/4.221/4.440ms narrow and
30.465/31.608/31.980ms wide p50/p95/p99. Equivalent explicit `in` controls
measured 3.161/3.352/3.417ms and 29.714/33.087/39.691ms. Common-case contains
measured 2.967/3.181/3.355ms and 36.014/37.574/37.904ms versus explicit
`contains_any` controls at 2.934/3.555/5.178ms and 35.931/37.140/37.603ms.
Every pair returns identical cardinality and response bytes and reads the same
one/four candidate blocks, 1,024/8,192 entries, and 235,778/1,914,055 payload
bytes. The tail differences are parser/run variation rather than storage
pushdown. Storage remains four raw blocks, 1,914,055 logical bytes, and
2,022,736 physical bytes; whole-process logs HWM is 104,780KiB after eight
additional repeated shapes. `QSF-206` accepts bounded Rust composition and
rejects an inexact SQL claim or redundant extension primitive.

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
