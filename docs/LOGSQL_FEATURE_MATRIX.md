# LogsQL feature matrix

This living matrix maps VictoriaLogs **LogsQL** constructs onto
`timeless-libsql`. The filename retains the common `LOGSQL` spelling, while
the language name follows upstream. See
[Query feature maps](QUERY_FEATURES.md) for ownership and completion rules.

Baseline: `a72634e`, 2026-08-04. The exact upstream source and container pins
are recorded in [Query semantic oracles](QUERY_ORACLES.md). The language
snapshot is the current
[VictoriaLogs LogsQL reference](https://docs.victoriametrics.com/victorialogs/logsql/).
The previous Timeless compatibility oracle is
[`TimelessLogs.LogsQL`](https://github.com/awksedgreep/timeless_logs/blob/main/lib/timeless_logs/logsql.ex)
and its tests. That parser was intentionally DDNet-oriented rather than complete
LogsQL, but its supported syntax must not disappear in the Rust path.

The current Rust parser supports the complete P0 Timeless/DDNet subset:
wildcard selection; relative, bracketed, and comparison time bounds; all eight
severities; service and arbitrary typed/nested field equality; quoted message
phrases and literals; deterministic time sort, limit, and offset; and
zero-argument `stats count()` with an optional alias. The native GET query API
has additional filters, but they do not count as LogsQL support until the
LogsQL parser and executor accept them.

## Legend and foundations

Priorities are `P0` (restore existing Timeless behavior), `P1` (storage-native
LogsQL core), `P2` (composable query/response behavior), `P3` (broader
upstream compatibility), and `DEFER` (blocked by an explicit storage/product
decision).

| name | public primitive |
|---|---|
| `ROWS` | `timeless_logs` row query with time, level, indexed-term, order, limit, and offset planning |
| `COUNT` | `timeless_log_count` |
| `VALUES` | `timeless_log_values` |
| `BUCKETS` | `timeless_log_buckets` |
| `STATS` | `timeless_stats` |
| `SQL` | ordinary SQLite expression/composition over bounded public results |

`API` owns LogsQL grammar, pipelines, transformations, result shape, limits,
and cancellation. `EXT` is used only when block/index awareness avoids decode
or materialization. Typed nested metadata remains lossless even when a query
must decode blocks to inspect a non-indexed field.
Rows with an `SQL` foundation must link an executable statement from the
[SQL equivalents cookbook](QUERY_SQL_EQUIVALENTS.md) before becoming
`shipped`.

## Filters and query syntax

| ID | upstream construct | Rust now | Elixir | foundation | target | priority | notes |
|---|---|---|---|---|---|---|---|
| `LQL-F01` | wildcard/no-op `*` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | yes | `ROWS` | `API` | P0 | Must remain bounded by request limits even without a time filter. |
| `LQL-F02` | relative `_time:5m` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | yes | `ROWS` | `API` | P0 | One injected request clock defines upstream's `[now-duration,now)` window in the table's native unit; both microsecond edges, valid empty `0s`, the pinned live oracle, and reopen pass. |
| `LQL-F03` | absolute/bracket `_time:[start,end)` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | yes | `ROWS` | `API` | P0 | RFC3339 and integer Unix-second/millisecond/microsecond/nanosecond bounds preserve `[`, `(`, `]`, and `)` exactly in the table's native unit; empty intersections fail. |
| `LQL-F04` | `_time:>=...` and `_time:<...` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | yes | `ROWS` | `API` | P0 | `>`, `>=`, `<`, and `<=` intersect in the table's native unit; exclusive edges adjust by one unit and overflow/empty intersections fail. |
| `LQL-F05` | arbitrary exact `field:value` filters ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-005-arbitrary-metadata-equality)) | shipped | yes | `ROWS` | `API` | P0 | Legacy `:` is exact string equality; `:=` accepts typed JSON primitives and dotted paths. Indexed string keys prune before bounded decode; missing/null/empty remain distinct. |
| `LQL-F06` | exact `level:<severity>` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | yes | `ROWS` | `API` | P0 | All eight stored severities are valid. |
| `LQL-F07` | exact `service:<value>` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | yes | `ROWS` | `API` | P0 | Honor semantic service aliases used by storage. |
| `LQL-F08` | quoted message phrase | shipped | yes | `ROWS` | `API` | P0 | Exact case-sensitive phrase bytes plus Unicode letter/digit/underscore word boundaries match the pinned VictoriaLogs oracle. The existing case-insensitive Timeless substring primitive remains separate. |
| `LQL-F09` | word filter | in progress | no | `ROWS` | `API` | P1 | Case-sensitive Unicode letter/digit/underscore boundaries pass the pinned oracle; bounded decode remains the honest plan. |
| `LQL-F10` | prefix filter | in progress | no | `ROWS` | `API` | P1 | Word and phrase prefixes share the pinned boundary semantics; no measured index contract is claimed. |
| `LQL-F11` | pattern-match filter | missing | no | `ROWS` | `API` | P2 | Preserve upstream wildcard/capture semantics. |
| `LQL-F12` | substring filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-007-case-sensitive-message-substring)) | in progress | partial | `ROWS`, `SQL` | `API` | P1 | Case-sensitive literal UTF-8 substring matches the pinned oracle and remains distinct from the established case-insensitive engine predicate. |
| `LQL-F13` | regexp filter | in progress | no | `ROWS` | `API` | P1 | Bounded RE2-compatible Rust regex uses a 1 MiB compiled-size limit and observes cancellation and decoded-row work before returning matches. |
| `LQL-F14` | case-insensitive filter | in progress | no | `ROWS` | `API` | P1 | `i(word)`, `i("phrase")`, and `i(prefix*)` use pinned Unicode case behavior over bounded public rows. |
| `LQL-F15` | exact filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-008-exact-empty-and-any-value-predicates)) | in progress | partial | `ROWS`, `SQL` | `API` | P1 | Full-message exactness and exact typed metadata equality remain distinct from word matching. |
| `LQL-F16` | exact-prefix filter | missing | no | `ROWS` | `API` | P2 | Decode-first candidate. |
| `LQL-F17` | multi-exact filter | missing | no | `ROWS` | `API` | P2 | Plan indexed values as a posting-list union where possible. |
| `LQL-F18` | empty-value filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-008-exact-empty-and-any-value-predicates)) | in progress | no | `ROWS`, `SQL` | `API` | P1 | `field:("")` provides compatible missing/null/empty behavior; legacy and typed exact forms preserve each retained state. |
| `LQL-F19` | any-value filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-008-exact-empty-and-any-value-predicates)) | in progress | no | `ROWS`, `SQL` | `API` | P1 | Present non-null typed values include zero, false, arrays, and objects; only the empty string is excluded. |
| `LQL-F20` | field no-op filter | missing | no | `ROWS` | `API` | P2 | Parser/evaluator behavior only. |
| `LQL-F21` | `contains_all` | missing | no | `ROWS` | `API` | P2 | Decode-first; no extension primitive until measured. |
| `LQL-F22` | `contains_any` | missing | no | `ROWS` | `API` | P2 | Decode-first; no extension primitive until measured. |
| `LQL-F23` | `json_array_contains_any` | missing | no | `ROWS` | `API` | P2 | Operate on retained typed metadata, not flattened strings. |
| `LQL-F24` | numeric range/comparison filter | in progress | no | `ROWS` | `API` | P1 | Typed `>`, `>=`, `<`, `<=`, and all open/closed ranges preserve full integer/float ordering; numeric strings are never coerced. |
| `LQL-F25` | IPv4 range filter | missing | no | `ROWS` | `API` | P2 | API transform over retained value. |
| `LQL-F26` | IPv6 range filter | missing | no | `ROWS` | `API` | P2 | API transform over retained value. |
| `LQL-F27` | string-range filter | missing | no | `ROWS` | `API` | P2 | Pin byte/Unicode collation semantics. |
| `LQL-F28` | length-range filter | missing | no | `ROWS` | `API` | P2 | Define byte versus codepoint length from upstream. |
| `LQL-F29` | `value_type(...)` filter | in progress | no | `ROWS` | `API` | P1 | Exposes retained logical JSON types; VictoriaLogs physical block-encoding names fail explicitly rather than leaking private storage. |
| `LQL-F30` | `eq_field`, `le_field`, `lt_field` | missing | no | `ROWS` | `API` | P2 | Same-row typed comparisons after bounded decode. |
| `LQL-F31` | logical `AND`, `OR`, `NOT`, parentheses ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-009-boolean-composition)) | in progress | no | `ROWS`, `SQL` | `API` | P1 | `NOT` > `AND` > `OR`; safe top-level indexed conjuncts prune before bounded decode, while no predicate below `OR`/`NOT` is pushed. |
| `LQL-F32` | searching multiple fields/prefix field sets | missing | no | `ROWS` | `API` | P2 | Field expansion belongs in planner, not storage schema mutation. |
| `LQL-F33` | day-range filter | missing | no | `ROWS` | `API` | P2 | Normalize timezone explicitly. |
| `LQL-F34` | week-range filter | missing | no | `ROWS` | `API` | P2 | Normalize timezone explicitly. |
| `LQL-F35` | stream selector `{...}` | deferred | no | none | `DEFER` | DEFER | Timeless has no declared VictoriaLogs stream identity; see `QSF-007`. |
| `LQL-F36` | `_stream_id` filter | deferred | no | none | `DEFER` | DEFER | Requires a stable stream-ID storage contract. |
| `LQL-F37` | sequence filter | missing | no | `ROWS` | `API` | P3 | Needs ordered state with strict memory bounds. |
| `LQL-F38` | subquery/`in(...)` filter | missing | no | `SQL` | `API` | P3 | Use bounded SQL/API composition; do not add a nested-query vtab. |
| `LQL-F39` | quoted identifiers/literals and escapes | shipped | partial | none | `API` | P0 | Double/single literals decode pinned Go escapes, backticks remain raw, quoted pipes do not split pipelines, and quoted field names are literal keys. Escapes producing non-UTF-8 bytes fail explicitly because the retained rich-log/JSON model is UTF-8. |
| `LQL-F40` | comments and multi-line queries | missing | no | none | `API` | P2 | Parser-only, with error-location regressions. |
| `LQL-F41` | `equals_common_case` / `contains_common_case` | missing | no | `ROWS` | `API` | P3 | Add only with a precise upstream Unicode/case oracle. |

## Pipes

Pipes usually transform bounded query results and therefore belong in `API`
or ordinary `SQL`. A pipe is not a reason to expose LogsQL syntax inside the
extension.

| ID | pipe | Rust now | Elixir | foundation | target | priority |
|---|---|---|---|---|---|---|
| `LQL-P01` | `limit` / `head` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | yes | `ROWS` | `API` | P0 |
| `LQL-P02` | `offset` / `skip` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | yes | `ROWS` | `API` | P0 |
| `LQL-P03` | `sort` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | yes | `ROWS`, `SQL` | `API` | P0 |
| `LQL-P04` | `field_values` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-004-distinct-field-values)) | missing | no | `VALUES` | `API` | P1 |
| `LQL-P05` | `field_names` | missing | no | `ROWS`, `VALUES` | `API` | P1 |
| `LQL-P06` | `fields` / `keep` | missing | no | `SQL` | `API` | P1 |
| `LQL-P07` | `delete` / `drop` | missing | no | `SQL` | `API` | P2 |
| `LQL-P08` | `filter` / `where` | missing | no | `SQL` | `API` | P1 |
| `LQL-P09` | `stats` ([count SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-003-exact-count), [bucket SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-006-counts-by-field-and-time-bucket)) | shipped | partial | `COUNT`, `SQL` | `API` | P0 |
| `LQL-P10` | `block_stats` | missing | no | `STATS` | `EXT` | P2 |
| `LQL-P11` | `blocks_count` | missing | no | `STATS` | `EXT` | P2 |
| `LQL-P12` | `query_stats` | missing | no | `STATS` | `API` | P2 |
| `LQL-P13` | `first` | missing | no | `ROWS`, `SQL` | `API` | P2 |
| `LQL-P14` | `last` | missing | no | `ROWS`, `SQL` | `API` | P2 |
| `LQL-P15` | `top` | missing | no | `SQL` | `API` | P2 |
| `LQL-P16` | `uniq` | missing | no | `VALUES`, `SQL` | `API` | P2 |
| `LQL-P17` | `sample` | missing | no | `SQL` | `API` | P3 |
| `LQL-P18` | `facets` | missing | no | `VALUES`, `COUNT` | `API` | P2 |
| `LQL-P19` | `coalesce` | missing | no | `SQL` | `API` | P2 |
| `LQL-P20` | `copy` | missing | no | `SQL` | `API` | P2 |
| `LQL-P21` | `rename` | missing | no | `SQL` | `API` | P2 |
| `LQL-P22` | `format` | missing | no | `SQL` | `API` | P2 |
| `LQL-P23` | `math` / `eval` | missing | no | `SQL` | `API` | P2 |
| `LQL-P24` | `len` | missing | no | `SQL` | `API` | P2 |
| `LQL-P25` | `hash` | missing | no | `SQL` | `API` | P3 |
| `LQL-P26` | `collapse_nums` | missing | no | `SQL` | `API` | P3 |
| `LQL-P27` | `decolorize` | missing | no | `SQL` | `API` | P3 |
| `LQL-P28` | `drop_empty_fields` | missing | no | `SQL` | `API` | P2 |
| `LQL-P29` | `replace` | missing | no | `SQL` | `API` | P2 |
| `LQL-P30` | `replace_regexp` | missing | no | `SQL` | `API` | P2 |
| `LQL-P31` | `split` | missing | no | `SQL` | `API` | P3 |
| `LQL-P32` | `extract` | missing | no | `SQL` | `API` | P2 |
| `LQL-P33` | `extract_regexp` | missing | no | `SQL` | `API` | P2 |
| `LQL-P34` | `pack_json` | missing | no | `SQL` | `API` | P2 |
| `LQL-P35` | `pack_logfmt` | missing | no | `SQL` | `API` | P3 |
| `LQL-P36` | `unpack_json` | missing | no | `SQL` | `API` | P2 |
| `LQL-P37` | `unpack_logfmt` | missing | no | `SQL` | `API` | P3 |
| `LQL-P38` | `unpack_syslog` | missing | no | `SQL` | `API` | P3 |
| `LQL-P39` | `unpack_words` | missing | no | `SQL` | `API` | P3 |
| `LQL-P40` | `json_array_concat` | missing | no | `SQL` | `API` | P3 |
| `LQL-P41` | `json_array_len` | missing | no | `SQL` | `API` | P2 |
| `LQL-P42` | `unroll` | missing | no | `SQL` | `API` | P3 |
| `LQL-P43` | `join` | missing | no | `SQL` | `API` | P3 |
| `LQL-P44` | `union` | missing | no | `SQL` | `API` | P3 |
| `LQL-P45` | `running_stats` | missing | no | `SQL` | `API` | P3 |
| `LQL-P46` | `total_stats` | missing | no | `SQL` | `API` | P3 |
| `LQL-P47` | `time_add` | missing | no | `SQL` | `API` | P3 |
| `LQL-P48` | `generate_sequence` | missing | no | `SQL` | `API` | P3 |
| `LQL-P49` | `set_stream_fields` | deferred | no | none | `DEFER` | DEFER |
| `LQL-P50` | `stream_context` | deferred | no | none | `DEFER` | DEFER |

## Statistics functions

The first optimization target is `COUNT`, which already avoids row decode.
Other functions should begin as bounded API/SQL composition over typed fields;
only measured repeated scans should create new extension vectors.

| ID | function | Rust now | foundation | target | priority |
|---|---|---|---|---|---|
| `LQL-S01` | `count()` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-003-exact-count)) | shipped | `COUNT` | `API` | P0 |
| `LQL-S02` | `count(field)` / `count_empty` | missing | `ROWS` | `API` | P1 |
| `LQL-S03` | `count_uniq` / `count_uniq_hash` ([SQL foundation](QUERY_SQL_EQUIVALENTS.md#sql-log-004-distinct-field-values)) | missing | `VALUES`, `SQL` | `API` | P1 |
| `LQL-S04` | `uniq_values` / `values` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-004-distinct-field-values)) | missing | `VALUES`, `SQL` | `API` | P1 |
| `LQL-S05` | `sum` / `avg` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-006-counts-by-field-and-time-bucket)) | missing | `ROWS`, `SQL` | `API` | P1 |
| `LQL-S06` | `min` / `max` / `median` | missing | `ROWS`, `SQL` | `API` | P1 |
| `LQL-S07` | `quantile` / `stddev` | missing | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S08` | `rate` / `rate_sum` ([SQL foundation](QUERY_SQL_EQUIVALENTS.md#sql-log-006-counts-by-field-and-time-bucket)) | missing | `COUNT`, `BUCKETS`, `SQL` | `API` | P1 |
| `LQL-S09` | `sum_len` | missing | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S10` | `any` / `field_min` / `field_max` | missing | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S11` | `row_any` / `row_min` / `row_max` | missing | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S12` | `json_values` | missing | `ROWS`, `SQL` | `API` | P3 |
| `LQL-S13` | `histogram` | missing | `ROWS`, `SQL` | `API` | P3 |
| `LQL-S14` | running `count/last/min/max/sum` | missing | `ROWS`, `SQL` | `API` | P3 |
| `LQL-S15` | total `count/first/last/min/max/sum` | missing | `ROWS`, `SQL` | `API` | P3 |

## Query options and HTTP behavior

| ID | option/behavior | Rust now | target | priority | notes |
|---|---|---|---|---|---|
| `LQL-Q01` | deterministic `asc`/`desc`, limit, and offset ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | `API` | P0 | Time sort uses `(ts, stable engine order)` in either direction; equal timestamps, aliases, zero limits, optimize, and reopen are pinned. |
| `LQL-Q02` | field projection in response | missing | `API` | P1 | Preserve `_time`, `_msg`, and typed metadata rules. |
| `LQL-Q03` | concurrency/parallel-reader options | partial | `API` | P2 | Claims/configuration may lower hard server limits. |
| `LQL-Q04` | `time_offset` | missing | `API` | P2 | Planner-only timestamp shift. |
| `LQL-Q05` | `global_filter` | missing | `API` | P3 | Apply before every subquery without textual substitution. |
| `LQL-Q06` | partial-response option | missing | `API` | P3 | Default remains fail-closed; never silently return incomplete rows. |
| `LQL-Q07` | cancellation, deadline, row/response/sample limits | shipped | `API` | P0 | Hard defaults cap result rows, decoded/examined entries, response bytes, and wall time. Capability-advertised `max_work_entries` guards row/count/value reads before decode; dropped requests cancel SQLite/Rust work and readers remain reusable. |
| `LQL-Q08` | stable VictoriaLogs-compatible errors | shipped | `API` | P0 | Timeless intentionally improves the upstream 400 text envelope: malformed syntax is stable JSON HTTP 400 and unsupported capability is stable JSON HTTP 422, with neither reaching storage. See `QSF-063`. |

## Higher-order library boundary

These features intentionally stay above `timeless-libsql`:

| concern | owner |
|---|---|
| live-tail subscriptions and fan-out | `timeless_logs` / UI integration (`LIB`) |
| saved searches, dashboard variables, display formatting, and user history | TimelessUI/Canvas/dashboard libraries (`LIB`) |
| alert rules, notification routing, and incident workflows | higher-order log/control-plane libraries (`LIB`) |
| tenant/token issuance, role policy, and cluster administration | Phoenix control plane (`LIB`); Rust enforces claims and limits |
| ingestion routing and framework logger bridges | signal integration libraries (`LIB`); the Rust server owns durable admission |

## Parity gates

P0 first restores every useful prior Timeless grammar feature without copying
the old parser's silent-ignore behavior. Invalid fields, levels, timestamps,
numbers, or pipes must fail explicitly.

Each shipped filter or pipe is tested against the real extension with all
eight severities, microsecond timestamps, typed and nested metadata, missing
fields, empty values, pagination, both sort directions, reopen, cancellation,
and response limits. Where VictoriaLogs provides the semantic oracle, a fixed
differential fixture compares selected rows, order where guaranteed, fields,
types, aggregate values, and errors.

Measurements record candidate blocks, decoded blocks/entries, term-index
hits, result rows, p50/p95/p99, response bytes, and RSS HWM. An `EXT` addition
requires evidence that a correct bounded `API`/`SQL` implementation pays an
avoidable storage/decode cost.

Direct SQLite/libSQL equivalents for current filtering, ordering, pagination,
substring, count, field discovery, metadata, and bucket operations live in
the [SQL equivalents cookbook](QUERY_SQL_EQUIVALENTS.md).
