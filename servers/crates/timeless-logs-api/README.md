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

<!-- query-contract-shipped: LQL-F01 LQL-F02 LQL-F03 LQL-F04 LQL-F05 LQL-F06 LQL-F07 LQL-F08 LQL-F09 LQL-F10 LQL-F11 LQL-F12 LQL-F13 LQL-F14 LQL-F15 LQL-F16 LQL-F17 LQL-F18 LQL-F19 LQL-F20 LQL-F21 LQL-F22 LQL-F23 LQL-F24 LQL-F25 LQL-F29 LQL-F31 LQL-F39 LQL-P01 LQL-P02 LQL-P03 LQL-P04 LQL-P05 LQL-P06 LQL-P08 LQL-P09 LQL-Q01 LQL-Q02 LQL-Q07 LQL-Q08 LQL-S01 LQL-S02 LQL-S03 LQL-S04 LQL-S05 LQL-S06 LQL-S08 -->

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

The ordered pipeline also accepts `field_values`, `field_names`,
`fields`/`keep`, `filter`/`where`, and `stats`. Projection accepts exact dotted
paths, top-level prefixes, and `*`; a later filter observes the projected row,
not the original one. Field discovery is deterministic and top-level:
`field_names` counts a field whenever it is present, including JSON null and
empty values, and does not synthesize VictoriaLogs `_stream` fields that are
not in the Timeless storage model. `field_values` keeps JSON types distinct,
represents a missing value by omitting the requested field, and returns a
deterministic type-tagged order with numeric `hits`. A positive operator
`limit` bounds retained values; `limit 0` has the upstream meaning of no
operator-specific limit while the server's hard result/work limits still
apply.

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
and binds the positive hard guard on every row, count, and value-discovery
request. Direct callers may use the backward-compatible unbounded arities or
provide the same trailing/hidden input explicitly:

```sql
SELECT ts, level, message FROM logs
 WHERE service='api' AND max_work_entries=100000
 ORDER BY ts DESC LIMIT 100;

SELECT n FROM timeless_log_count(
  'logs', '{"level":"error"}', NULL, :start_us, :end_us, 100000);

SELECT value FROM timeless_log_values(
  'logs', 'host', NULL, NULL, :start_us, :end_us, 1000, 100000);
```

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
