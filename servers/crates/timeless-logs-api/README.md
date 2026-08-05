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

<!-- query-contract-shipped: LQL-F01 LQL-F02 LQL-F03 LQL-F04 LQL-F05 LQL-F06 LQL-F07 LQL-F08 LQL-F09 LQL-F10 LQL-F11 LQL-F12 LQL-F13 LQL-F14 LQL-F15 LQL-F18 LQL-F19 LQL-F24 LQL-F29 LQL-F31 LQL-F39 LQL-P01 LQL-P02 LQL-P03 LQL-P04 LQL-P05 LQL-P06 LQL-P08 LQL-P09 LQL-Q01 LQL-Q02 LQL-Q07 LQL-Q08 LQL-S01 LQL-S02 LQL-S03 LQL-S04 LQL-S05 LQL-S06 LQL-S08 -->

The POST grammar includes wildcard selection; upper-exclusive relative
windows; RFC3339 and integer Unix s/ms/us/ns absolute bounds with open or
closed native-unit edges; all eight exact severities; service and arbitrary
typed metadata equality; message word, phrase, word-prefix, phrase-prefix,
case-sensitive substring, bounded RE2-compatible regexp, case-insensitive,
full-message exact, and VictoriaLogs-compatible any/full/prefix/suffix pattern
filters with `<N>`, `<UUID>`, `<IP4>`, `<TIME>`, `<DATE>`, `<DATETIME>`, and
`<W>` placeholders and case-insensitive function names; time sort, limit, and
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

The measured follow-up work is organized in
[`LOGS_MIXED_WORKLOAD_PERFORMANCE_PLAN.md`](../../../LOGS_MIXED_WORKLOAD_PERFORMANCE_PLAN.md).
The pinned Session 1 comparison is reproduced in the
[release-plan baseline table](../../../docs/2026-08-02_rust_telemetry_data_plane_release_plan.md#baseline-validation).
