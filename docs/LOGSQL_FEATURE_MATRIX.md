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

The current Rust parser supports the complete P0 Timeless/DDNet subset plus
the shipped P1 filters, the P2 pattern matcher, and the ordered
discovery/projection/statistics pipeline:
wildcard selection; relative, bracketed, and comparison time bounds; all eight
severities; service and arbitrary typed/nested field predicates; message
filters; logical composition; deterministic time sort, limit, and offset;
field names/values; typed projection and current-row filters; bounded
VictoriaLogs-compatible `first`/`last`, `top`, and `uniq` selection; and the
listed count, unique, numeric, and rate statistics. The native GET query API
may have additional parameters, but they do not count as LogsQL support until
the LogsQL parser and executor accept them.

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
| `LQL-F09` | word filter | shipped | no | `ROWS` | `API` | P1 | Case-sensitive Unicode letter/digit/underscore boundaries pass the pinned oracle; bounded decode remains the honest plan. |
| `LQL-F10` | prefix filter | shipped | no | `ROWS` | `API` | P1 | Word and phrase prefixes share the pinned boundary semantics; no measured index contract is claimed. |
| `LQL-F11` | pattern-match filter | shipped | no | `ROWS` | `API` | P2 | Four case-insensitive function names and all seven VictoriaLogs placeholders pass 23 pinned cases, including the exact Unicode Letter/Decimal_Number boundary for `<W>`. Matching uses a textual projection of retained rich values over bounded public rows; missing and null are empty only for this predicate. No portable SQL equivalent is claimed because SQLite `LIKE`/`GLOB` cannot reproduce the placeholder and quoted-Unicode-word grammar. |
| `LQL-F12` | substring filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-007-case-sensitive-message-substring)) | shipped | partial | `ROWS`, `SQL` | `API` | P1 | Case-sensitive literal UTF-8 substring matches the pinned oracle and remains distinct from the established case-insensitive engine predicate. |
| `LQL-F13` | regexp filter | shipped | no | `ROWS` | `API` | P1 | Bounded RE2-compatible Rust regex uses a 1 MiB compiled-size limit and observes cancellation and decoded-row work before returning matches. |
| `LQL-F14` | case-insensitive filter | shipped | no | `ROWS` | `API` | P1 | `i(word)`, `i("phrase")`, and `i(prefix*)` use pinned Unicode case behavior over bounded public rows. |
| `LQL-F15` | exact filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-008-exact-empty-and-any-value-predicates)) | shipped | partial | `ROWS`, `SQL` | `API` | P1 | Full-message exactness, unquoted `=value`, case-insensitive `exact(value)` function names, and exact typed metadata equality remain distinct from word matching. |
| `LQL-F16` | exact-prefix filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-014-exact-prefix)) | shipped | partial | `ROWS`, `SQL` | `API` | P2 | Case-sensitive start anchoring, operator/function forms, UTF-8, strict errors, rich textual projection, empty-prefix behavior, composition, limits, durability, and reopen pass 13 pinned cases. Ordinary SQL exactly covers message and retained-text prefixes; the API owns VictoriaLogs projection for all rich types. Exact-build evidence shows byte-identical storage work to other decode-first filters, so no extension primitive is justified. |
| `LQL-F17` | multi-exact filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-015-static-multi-exact-membership)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Static `in(v1, ..., vN)` provides case-sensitive exact textual membership over messages and rich fields. Seventeen pinned cases cover quoting, commas/pipes, typed projection, duplicates, an empty list, a trailing comma, literal versus standalone wildcard behavior, composition, and strict errors; `in(subquery)` remains explicitly deferred under `LQL-F38`. Direct users can use ordinary parameterized `IN`, including the existing public posting index for a declared string-only key; generic rich membership remains bounded API composition because exact-build evidence proves no avoidable storage work. |
| `LQL-F18` | empty-value filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-008-exact-empty-and-any-value-predicates)) | shipped | no | `ROWS`, `SQL` | `API` | P1 | `field:("")` provides compatible missing/null/empty behavior; legacy and typed exact forms preserve each retained state. |
| `LQL-F19` | any-value filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-008-exact-empty-and-any-value-predicates)) | shipped | no | `ROWS`, `SQL` | `API` | P1 | Present non-null typed values include zero, false, arrays, and objects; only the empty string is excluded. |
| `LQL-F20` | field no-op filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-016-field-no-op)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Any standalone unquoted wildcard in case-insensitive `in`, `contains_any`, or `contains_all` is a field-independent true predicate, including mixed lists, missing fields, `service`/`level` aliases, logical composition, and pipelines. Eleven pinned cases and the real extension pass; static non-wildcard values are independently shipped by `LQL-F21`/`LQL-F22`, while query-backed lists remain the explicit `LQL-F38` boundary. Ordinary SQL omits the field predicate, and exact-build evidence proves no new extension primitive is warranted. |
| `LQL-F21` | `contains_all` | shipped | no | `ROWS` | `API` | P2 | Sixteen successful and two error pinned cases cover case-sensitive all-phrase matching, Unicode word boundaries, numeric textual projection, static-list grammar, field scope, empty-list/empty-value identity, aliases, logical/pipeline composition, and strict errors. Real-extension regressions preserve rich typed/nested projection, work limits, cancellation, durability, and reopen; query-backed lists remain `LQL-F38`. Exact-build evidence records bounded decode-first cost and no avoidable storage work. Portable SQLite has no exact Unicode-category phrase-boundary predicate, so this row intentionally makes no SQL or extension claim. |
| `LQL-F22` | `contains_any` | shipped | no | `ROWS` | `API` | P2 | Seventeen successful and two error pinned cases cover case-sensitive any-phrase matching, Unicode word boundaries, numeric/boolean/rich textual projection, empty-list false versus empty-value true, aliases, static-list grammar, logical/pipeline composition, and strict errors. Real-extension regressions preserve work limits, cancellation, durability, and reopen; query-backed lists remain `LQL-F38`. Exact-build evidence shows byte-identical public reads and no avoidable storage work. Portable SQLite has no exact Unicode-category phrase-boundary predicate, so this row intentionally makes no SQL or extension claim. |
| `LQL-F23` | `json_array_contains_any` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-017-json-array-primitive-membership)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Twenty successful and four error pinned cases cover exact top-level primitive membership, empty-list/value behavior, escaped strings, numeric spelling, ignored nested/scalar values, quoted-star literal versus unquoted-star error, grammar, and the intentional semantic-JSON distinction from VictoriaLogs' raw-lexeme Unicode-escape shortcut. Real-extension regressions preserve retained types, limits, cancellation, durability, and reopen. Executable public `json_each` SQL and exact-build evidence prove bounded decode-first work; no extension primitive is justified. |
| `LQL-F24` | numeric range/comparison filter | shipped | no | `ROWS` | `API` | P1 | Typed `>`, `>=`, `<`, `<=`, and all open/closed ranges preserve full integer/float ordering; numeric strings are never coerced. |
| `LQL-F25` | IPv4 range filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-018-ipv4-range-over-retained-strings)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Eleven successful and seven error pinned cases cover exact whole-string IPv4 parsing, inclusive single/CIDR/two-bound forms, host-bit normalization, trailing commas, `/0`, decimal leading zeroes, inverted ranges, arbitrary/message fields, logical/pipeline composition, case-insensitive function names, and strict errors. Real-extension regressions preserve retained types, work limits, cancellation, durability, and reopen. Executable public JSON1 SQL and exact-build narrow/wide evidence pass without an extension primitive. |
| `LQL-F26` | IPv6 range filter | shipped | no | `ROWS` | `API` | P2 | Twelve successful and seven error pinned cases cover exact whole-string parsing, inclusive address/CIDR/two-bound forms, host-bit normalization, `/0`, IPv4-mapped input and 128-bit prefixes, spelling normalization, inverted ranges, arbitrary/message fields, logical/pipeline composition, and strict errors. Failing-then-passing parser and real-extension regressions preserve retained types, work limits, cancellation, durability, and reopen. Exact-build evidence records bounded decode-first work without an extension primitive. Portable SQLite has no built-in IPv6 parser, so this row intentionally makes no SQL claim. |
| `LQL-F27` | string-range filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-019-bytewise-string-range-over-retained-text)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Sixteen successful and seven error pinned cases cover lower-inclusive/upper-exclusive byte ordering, equal-prefix/inverted ranges, ASCII/UTF-8, typed projection, missing/null/empty, nested/message/service fields, aliases, composition, and strict errors. Real-extension regressions preserve rich objects, limits, cancellation, durability, and reopen. Executable `SQL-LOG-019` and exact-build narrow/wide evidence pass without an extension primitive. |
| `LQL-F28` | length-range filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-020-unicode-codepoint-length-range-over-retained-text)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Seventeen successful and ten error pinned cases plus failing-then-passing parser and real-extension regressions establish inclusive Unicode-codepoint bounds, rich textual projection, missing/null/empty length zero, aliases, composition, strict errors, limits, cancellation, durability, and reopen. Executable `SQL-LOG-020` and exact-build narrow/wide evidence pass without an extension primitive or storage change. |
| `LQL-F29` | `value_type(...)` filter | shipped | no | `ROWS` | `API` | P1 | Exposes retained logical JSON types; VictoriaLogs physical block-encoding names fail explicitly rather than leaking private storage. |
| `LQL-F30` | `eq_field`, `le_field`, `lt_field` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-021-same-row-textual-field-comparison)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Twelve successful and twelve error pinned cases plus failing-then-passing parser and real-extension regressions establish same-row exact textual equality and math-value-or-bytewise ordering. Missing/null/empty projection, retained rich values and exact large integers, quoted/message/service/nested/right-`_time` fields, aliases, logical/pipeline composition, strict errors, limits, cancellation, durability, and reopen pass. Executable `SQL-LOG-021` and exact-build narrow/wide evidence pass; math parsing remains bounded API work without an extension primitive or storage change. |
| `LQL-F31` | logical `AND`, `OR`, `NOT`, parentheses ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-009-boolean-composition)) | shipped | no | `ROWS`, `SQL` | `API` | P1 | `NOT` > `AND` > `OR`; safe top-level indexed conjuncts prune before bounded decode, while no predicate below `OR`/`NOT` is pushed. |
| `LQL-F32` | searching multiple fields/prefix field sets ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-022-prefix-selected-field-set)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Sixteen successful and six error pinned cases plus failing-then-passing parser and real-extension regressions establish literal empty/quoted prefixes, canonical special fields, recursively dotted rich leaves, independent group operands, projected pipelines, strict wildcard-comparison errors, limits, cancellation, durability, and reopen. Executable `SQL-LOG-022` and exact-build narrow/wide word and typed-value evidence pass without an extension primitive or storage change. |
| `LQL-F33` | day-range filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-023-utc-day-range-with-explicit-offset)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Fifteen successful and eight error pinned cases plus failing-then-passing parser and real-extension regressions establish exact brackets, `HH:MM`/`HHMM`, signed compound offsets, `24:00`/minute-60 normalization, midnight/full-day and inverted-range behavior, deterministic UTC default, current-row pipelines, limits, cancellation, durability, and reopen. Executable `SQL-LOG-023` and exact-build narrow/wide evidence pass without an extension primitive or storage change. |
| `LQL-F34` | week-range filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-024-utc-week-range-with-explicit-offset)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Fifteen successful and eight error pinned cases plus failing-then-passing parser and real-extension regressions establish short/full weekdays, exact bracket normalization, non-wrapping and full-week/empty edges, signed offsets, deterministic UTC default, current-row pipelines, limits, cancellation, durability, and reopen. Executable `SQL-LOG-024` and exact-build narrow/wide evidence pass without an extension primitive or storage change. |
| `LQL-F35` | stream selector `{...}` | deferred | no | none | `DEFER` | DEFER | Timeless has no declared VictoriaLogs stream identity; see `QSF-007`. |
| `LQL-F36` | `_stream_id` filter | deferred | no | none | `DEFER` | DEFER | Requires a stable stream-ID storage contract. |
| `LQL-F37` | sequence filter | missing | no | `ROWS` | `API` | P3 | Needs ordered state with strict memory bounds. |
| `LQL-F38` | subquery/`in(...)` filter | missing | no | `SQL` | `API` | P3 | Use bounded SQL/API composition; do not add a nested-query vtab. |
| `LQL-F39` | quoted identifiers/literals and escapes | shipped | partial | none | `API` | P0 | Double/single literals decode pinned Go escapes, backticks remain raw, quoted pipes do not split pipelines, and quoted field names are literal keys. Escapes producing non-UTF-8 bytes fail explicitly because the retained rich-log/JSON model is UTF-8. |
| `LQL-F40` | comments and multi-line queries | shipped | no | none | `API` | P2 | Parser-only. Fourteen successful and six error pinned cases plus failing-then-passing unit and real-extension regressions establish LF/CRLF comments, multiline composition, quoted hashes, one optional terminal semicolon, strict malformed tails, lexical line/column errors, limits, durability, and reopen. Exact-build evidence confirms byte-identical storage work without an extension primitive. |
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
| `LQL-P04` | `field_values` ([indexed SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-004-distinct-field-values), [typed SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-012-typed-unique-values-and-counts)) | shipped | no | `VALUES` | `API` | P1 |
| `LQL-P05` | `field_names` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-010-field-names-and-typed-projection)) | shipped | no | `ROWS`, `VALUES` | `API` | P1 |
| `LQL-P06` | `fields` / `keep` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-010-field-names-and-typed-projection)) | shipped | no | `SQL` | `API` | P1 |
| `LQL-P07` | `delete` / `drop` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-025-delete-exact-retained-metadata-fields)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P08` | `filter` / `where` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-011-current-row-filter-and-empty-counts)) | shipped | no | `SQL` | `API` | P1 |
| `LQL-P09` | `stats` ([count SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-003-exact-count), [bucket SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-006-counts-by-field-and-time-bucket)) | shipped | partial | `COUNT`, `SQL` | `API` | P0 |
| `LQL-P10` | `block_stats` | deferred | no | none | `DEFER` | DEFER |
| `LQL-P11` | `blocks_count` | deferred | no | none | `DEFER` | DEFER |
| `LQL-P12` | `query_stats` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-026-request-local-log-query-statistics)) | shipped | no | `STATS`, `SQL` | `API` | P2 |
| `LQL-P13` | `first` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-027-first-numeric-rows-per-partition)) | shipped | no | `ROWS`, `SQL` | `API` | P2 |
| `LQL-P14` | `last` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-028-last-numeric-rows-per-partition)) | shipped | no | `ROWS`, `SQL` | `API` | P2 |
| `LQL-P15` | `top` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-029-top-values-by-hit-count)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P16` | `uniq` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-030-unique-textual-values)) | shipped | no | `VALUES`, `SQL` | `API` | P2 |
| `LQL-P17` | `sample` | missing | no | `SQL` | `API` | P3 |
| `LQL-P18` | `facets` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-031-bounded-facets-over-public-log-fields)) | shipped | no | `VALUES`, `COUNT`, `SQL` | `API` | P2 |
| `LQL-P19` | `coalesce` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-032-first-nonempty-textual-log-field)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P20` | `copy` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-033-copy-one-exact-retained-metadata-field)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P21` | `rename` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-034-rename-one-exact-top-level-retained-metadata-field)) | shipped | no | `SQL` | `API` | P2 |
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

`LQL-P10` is deliberately deferred, not approximated. VictoriaLogs
`block_stats` exposes one row per physical field column with its internal
encoding type, dictionary item/byte counts, value bytes, bloom bytes, stream
identity, and filesystem part path. Timeless rich-log blocks retain block-level
timestamp/count/codec metadata, posting terms, and a compressed rich envelope;
they do not retain compatible per-field dictionaries, blooms, stream IDs, or
part paths. [`timeless_stats('logs')`](../README.md#logs) remains the public
aggregate operational surface, but it is not a SQL equivalent for this pipe.
Reconsider this row only after a versioned public per-field physical-accounting
contract exists across every readable codec, with a stored stream identity,
an honest SQLite block-location policy, bounded/cancellable enumeration, and
direct-user utility. See `QSF-147`.

`LQL-P11` is also deliberately deferred, for a different reason. VictoriaLogs
increments one count for every non-empty internal `blockResult` reaching the
pipe after all preceding filters and transforms. It returns a string-valued
alias row, returns no row for an empty query, and changes when an earlier
`limit` changes the processing batches. This is neither the persisted `blocks`
total nor the cumulative `query_candidate_blocks` counter in
`timeless_stats('logs')`. Timeless pipelines currently expose bounded rows but
do not expose or retain request-scoped block lineage through filter,
pagination, discovery, or aggregation transforms. Reconsider only after a
public, request-owned execution-batch lineage/report contract defines
persisted and buffered sources at every pipeline stage, remains correct under
concurrency/transactions/optimize/reopen, and demonstrates direct SQLite or
libSQL utility. Global stats deltas and private block IDs are explicitly not
acceptable substitutes. See `QSF-148`.

`LQL-P12` uses the request-owned execution-report prerequisite identified
while evaluating `LQL-P11`, but it does not relabel persisted block totals or
subtract process-wide counters. A fully consumed successful `timeless_logs`
scan publishes one table-scoped report on that SQLite connection;
[`timeless_log_query_stats`](QUERY_SQL_EQUIVALENTS.md#sql-log-026-request-local-log-query-statistics)
consumes it exactly once. New, failed, and cancelled scans clear older reports.
The Rust API replaces the storage-level matched count with the complete typed
LogsQL post-filter cardinality, maps actual work into VictoriaLogs' fourteen
string-valued fields, measures duration through the pipe position, and permits
later pipelines. Timeless reads one encoded payload rather than separately
addressable field-column files, so the complete payload is
`BytesReadValues`/`BytesReadTotal` and unavailable physical components are
zero. A preceding `limit` does not undo work already performed by the eager
bounded API scan; VictoriaLogs' parallel early cancellation also makes its
physical counts scheduling-dependent. See `QSF-149` and `QSF-150`.

Session 13's typed pipe contract is intentionally more faithful than the
flattened VictoriaLogs store. `field_values` uses deterministic type-tag order
for missing, null, bool, exact numbers, strings, arrays, and objects; missing
omits the value field. A positive limit selects a bounded deterministic subset
and reports unknown hits as zero after overflow, while `limit 0` means no
operator-specific limit. `field_names` discovers actual top-level response
fields in name order and counts presence including null/empty; it neither
synthesizes `_stream` fields nor recursively flattens objects. `fields`/`keep`
rebuilds exact dotted paths and supports top-level prefixes and `*`.
`filter`/`where` applies the typed predicate AST to the current transformed
row; storage-safe predicates remain in the initial selector.

`LQL-P16` accepts one or more exact fields after optional `by`, in
parenthesized or bare comma-separated form. Optional `filter substring` is a
case-sensitive textual filter for the single-field form; `hits` and
`with hits` add a collision-safe string count; and `limit 0` means unbounded
within the hard API limits. Missing, null, and empty values share one
empty-text group whose selected field is omitted from stream JSON. Positive
limit overflow resets every returned hit to string `"0"`, matching the pinned
VictoriaLogs contract. Upstream hash-map selection and order are unspecified;
Timeless deliberately returns the first N bytewise structural keys in stable
order. Work, unique groups, retained key state, results, response bytes, and
cancellation are bounded over public rows. Executable `SQL-LOG-030` provides
the direct SQLite/libSQL single-field foundation; no extension primitive or
private storage access is used.

`LQL-P18` emits the most frequent nonempty textual values for every recursively
flattened field in the current pipeline row. The default per-field result
limit is ten, with default exclusion thresholds of 1,000 unique values and
128 UTF-8 bytes. `keep_const_fields` retains a field only otherwise omitted
because one value appears in every selected row. Modifiers are
case-insensitive, reorderable, and repeatable with the last numeric value
winning. The pinned upstream parser accepts positive fractional numbers and
truncates them; Timeless preserves that compatibility quirk explicitly while
rejecting zero, negative, non-finite, missing, and malformed values.

Arrays remain one atomic JSON-text facet and objects become dotted leaves.
Any overlong value or excessive textual cardinality excludes the entire
field. Timeless orders field names bytewise, then values by hits descending
and value bytewise; the value tie break is deliberately deterministic because
the local upstream processor does not promise one. Input rows, field/value
state, sort/output allocations, result rows, response bytes, and cancellation
use existing request limits. Executable `SQL-LOG-031` exposes the complete
public JSON1/window-function foundation, including canonical special fields
and native timestamp units. No storage primitive or private table is used.
Exact-build evidence records 3.239/44.087 ms narrow/wide p95 versus
4.464/41.116 ms for byte-identical same-scan controls; `QSF-163` accepts the
mixed -27.4%/+7.2% variation above the unchanged public storage boundary.

`LQL-P19` selects the first nonempty textual value from parenthesized exact,
all-field, or prefix source filters, suppressing duplicate expanded names.
Missing, null, empty strings, and exact object parents are empty in the
flattened compatibility view; strings remain unquoted, numbers and booleans
become text, arrays remain atomic JSON text, and rich object leaves are dotted
sources. The destination defaults to `_msg`; optional `default` and `as` are
textual and exact. Timeless expands wildcard leaves bytewise for deterministic
current-row behavior and preserves an explicitly empty destination string in
its rich response, while VictoriaLogs stream serialization omits empty-valued
columns. A nested destination colliding with a retained scalar fails with an
explicit HTTP 422 `field_conflict` instead of replacing rich data.

Input rows, temporary de-duplication/path state, result rows, response bytes,
and cancellation use existing request limits. Executable `SQL-LOG-032`
provides the direct exact-field `CASE`/`NULLIF`/`COALESCE` foundation. The
operation requires no extension primitive, private table, or storage-contract
change.
Exact-build evidence records 3.570/39.597 ms narrow/wide p95 versus
3.329/38.277 ms for byte-identical same-scan controls; `QSF-165` accepts the
+7.2%/+3.4% bounded row-transform cost above the public storage boundary.

`LQL-P20` copies exact, all-field, or suffix-star prefix sources to exact,
all-field, or suffix-star destinations. `copy` and `cp` are
case-insensitive, `as` is optional, comma-separated pairs execute from left
to right, and every later pair observes fields created by earlier pairs.
Each wildcard source takes a snapshot at that pair, expands recursively
flattened leaves in deterministic bytewise order, and keeps arrays atomic.
Prefix-to-prefix copies substitute the source prefix; prefix-to-exact copies
every match to one field, so the last deterministic match wins. An exact
source with a wildcard destination preserves VictoriaLogs' unusual literal
destination name, including the `*`.

Exact strings, numbers, booleans, arrays, null, and empty strings retain their
JSON types and the source remains present. A missing exact source or exact
rich-object parent becomes an explicit empty string because VictoriaLogs'
query view contains flattened leaves rather than object parents. A
wildcard-generated empty suffix is a literal empty field and is not the
canonical `_msg`; exact quoted `""` remains the established message alias.
Timeless overwrites compatible scalar destinations but returns HTTP 422
`field_conflict` rather than replacing a retained object or descending
through a scalar parent. Work, cloned temporary values, destination paths,
results, response bytes, and cancellation use existing request limits.
Executable `SQL-LOG-033` provides the public exact-field JSON1 foundation;
the operation requires no extension primitive, private table, or storage
contract change.
Exact-build evidence records 3.229/46.025 ms narrow/wide p95 versus
3.659/41.958 ms for byte-identical same-scan controls; `QSF-167` accepts the
mixed -11.8%/+9.7% bounded row-transform variation above the public storage
boundary.

`LQL-P21` moves exact, all-field, or suffix-star prefix sources to exact,
all-field, or suffix-star destinations. `rename` and `mv` are
case-insensitive, `as` is optional, comma-separated pairs execute from left
to right, and later pairs observe the current row produced by earlier pairs.
Each wildcard source snapshots recursively flattened leaves in deterministic
bytewise order. All sources selected by one pair are removed before that
pair's destinations are written. Prefix substitution, exact-to-wildcard
literal destinations, unmatched-prefix no-ops, and prefix-to-exact
deterministic last-write behavior match the pinned VictoriaLogs processor.

Exact strings, numbers, booleans, arrays, null, and empty strings retain their
JSON types. Present exact leaves and wildcard leaves are removed from the
current response row, with empty rich parents pruned; the durable stored row
is never mutated. A missing exact source or exact rich-object parent writes an
explicit empty destination and does not remove the retained object. Empty
rich objects have no flattened wildcard leaves and remain present. A
wildcard-generated empty suffix is a literal empty field distinct from
canonical `_msg`, while an exact source with a wildcard destination retains
the literal `*`.

Timeless overwrites compatible scalar destinations but returns HTTP 422
`field_conflict` rather than replacing a retained object or descending
through a scalar parent. Work, moved temporary values, source/destination
paths, results, response bytes, and cancellation use existing request limits.
Executable `SQL-LOG-034` provides the direct exact top-level JSON1
foundation and documents nested-parent pruning and conflict responsibilities.
The operation requires no extension primitive, private table, or storage
contract change. Exact-build evidence records 3.770/43.696 ms narrow/wide p95
versus 3.553/36.673 ms for byte-identical same-scan controls; `QSF-169`
accepts the +6.1%/+19.1% bounded move/prune/rebuild cost above the unchanged
public storage boundary.

## Statistics functions

The first optimization target is `COUNT`, which already avoids row decode.
Other functions should begin as bounded API/SQL composition over typed fields;
only measured repeated scans should create new extension vectors.

| ID | function | Rust now | foundation | target | priority |
|---|---|---|---|---|---|
| `LQL-S01` | `count()` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-003-exact-count)) | shipped | `COUNT` | `API` | P0 |
| `LQL-S02` | `count(field)` / `count_empty` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-011-current-row-filter-and-empty-counts)) | shipped | `ROWS` | `API` | P1 |
| `LQL-S03` | `count_uniq` / `count_uniq_hash` ([SQL foundation](QUERY_SQL_EQUIVALENTS.md#sql-log-012-typed-unique-values-and-counts)) | shipped | `VALUES`, `SQL` | `API` | P1 |
| `LQL-S04` | `uniq_values` / `values` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-012-typed-unique-values-and-counts)) | shipped | `VALUES`, `SQL` | `API` | P1 |
| `LQL-S05` | `sum` / `avg` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-013-numeric-aggregates-median-and-rates)) | shipped | `ROWS`, `SQL` | `API` | P1 |
| `LQL-S06` | `min` / `max` / `median` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-013-numeric-aggregates-median-and-rates)) | shipped | `ROWS`, `SQL` | `API` | P1 |
| `LQL-S07` | `quantile` / `stddev` | missing | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S08` | `rate` / `rate_sum` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-013-numeric-aggregates-median-and-rates)) | shipped | `COUNT`, `BUCKETS`, `SQL` | `API` | P1 |
| `LQL-S09` | `sum_len` | missing | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S10` | `any` / `field_min` / `field_max` | missing | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S11` | `row_any` / `row_min` / `row_max` | missing | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S12` | `json_values` | missing | `ROWS`, `SQL` | `API` | P3 |
| `LQL-S13` | `histogram` | missing | `ROWS`, `SQL` | `API` | P3 |
| `LQL-S14` | running `count/last/min/max/sum` | missing | `ROWS`, `SQL` | `API` | P3 |
| `LQL-S15` | total `count/first/last/min/max/sum` | missing | `ROWS`, `SQL` | `API` | P3 |

`count(field)` requires at least one present non-empty selected value;
`count_empty` counts rows whose selected values are all missing, null, or an
empty string. Exact unique cardinality uses complete typed tuples; the bounded
hash variant uses stable FNV-1a cardinality and does not claim VictoriaLogs
hash-bit identity. `uniq_values` returns deterministic typed non-empty values;
`values` uses `{items,missing}` to remain lossless. Numeric functions ignore
numeric strings. Integer-only sums stay exact when representable, extrema
preserve the selected JSON number, and mixed/fractional sums, averages,
medians, and rates use finite binary64. `rate` and `rate_sum` divide by the
explicit final query interval in seconds; without a finite two-sided interval
they return the undivided count or sum, matching the upstream operator
contract.

## Query options and HTTP behavior

| ID | option/behavior | Rust now | target | priority | notes |
|---|---|---|---|---|---|
| `LQL-Q01` | deterministic `asc`/`desc`, limit, and offset ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-001-bounded-filter-sort-and-pagination)) | shipped | `API` | P0 | Time sort uses `(ts, stable engine order)` in either direction; equal timestamps, aliases, zero limits, optimize, and reopen are pinned. |
| `LQL-Q02` | field projection in response ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-010-field-names-and-typed-projection)) | shipped | `API` | P1 | Ordered `fields`/`keep` stages choose response fields; `_time`/`_msg` are retained only when selected, dotted metadata reconstructs its nested shape, and missing paths remain absent. |
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
