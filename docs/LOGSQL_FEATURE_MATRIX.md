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
| `LQL-F17` | multi-exact filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-015-static-multi-exact-membership)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Static `in(v1, ..., vN)` provides case-sensitive exact textual membership over messages and rich fields. Seventeen pinned cases cover quoting, commas/pipes, typed projection, duplicates, an empty list, a trailing comma, literal versus standalone wildcard behavior, composition, and strict errors; query-backed membership is independently implemented by `LQL-F38`. Direct users can use ordinary parameterized `IN`, including the existing public posting index for a declared string-only key; generic rich membership remains bounded API composition because exact-build evidence proves no avoidable storage work. |
| `LQL-F18` | empty-value filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-008-exact-empty-and-any-value-predicates)) | shipped | no | `ROWS`, `SQL` | `API` | P1 | `field:("")` provides compatible missing/null/empty behavior; legacy and typed exact forms preserve each retained state. |
| `LQL-F19` | any-value filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-008-exact-empty-and-any-value-predicates)) | shipped | no | `ROWS`, `SQL` | `API` | P1 | Present non-null typed values include zero, false, arrays, and objects; only the empty string is excluded. |
| `LQL-F20` | field no-op filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-016-field-no-op)) | shipped | no | `ROWS`, `SQL` | `API` | P2 | Any standalone unquoted wildcard in case-insensitive `in`, `contains_any`, or `contains_all` is a field-independent true predicate, including mixed lists, missing fields, `service`/`level` aliases, logical composition, and pipelines. Eleven pinned cases and the real extension pass; static non-wildcard values are independently shipped by `LQL-F21`/`LQL-F22`, and query-backed lists by `LQL-F38`. Ordinary SQL omits the field predicate, and exact-build evidence proves no new extension primitive is warranted. |
| `LQL-F21` | `contains_all` | shipped | no | `ROWS` | `API` | P2 | Sixteen successful and two error pinned cases cover case-sensitive all-phrase matching, Unicode word boundaries, numeric textual projection, static-list grammar, field scope, empty-list/empty-value identity, aliases, logical/pipeline composition, and strict errors. Real-extension regressions preserve rich typed/nested projection, work limits, cancellation, durability, and reopen; `LQL-F38` composes the same predicate with query-backed values. Exact-build evidence records bounded decode-first cost and no avoidable storage work. Portable SQLite has no exact Unicode-category phrase-boundary predicate, so this row intentionally makes no SQL or extension claim. |
| `LQL-F22` | `contains_any` | shipped | no | `ROWS` | `API` | P2 | Seventeen successful and two error pinned cases cover case-sensitive any-phrase matching, Unicode word boundaries, numeric/boolean/rich textual projection, empty-list false versus empty-value true, aliases, static-list grammar, logical/pipeline composition, and strict errors. Real-extension regressions preserve work limits, cancellation, durability, and reopen; `LQL-F38` composes the same predicate with query-backed values. Exact-build evidence shows byte-identical public reads and no avoidable storage work. Portable SQLite has no exact Unicode-category phrase-boundary predicate, so this row intentionally makes no SQL or extension claim. |
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
| `LQL-F37` | sequence filter | shipped | no | `ROWS` | `API` | P3 | Fifteen successful and six error pinned cases plus failing-then-passing parser and real-extension regressions establish ordered non-overlapping case-sensitive phrase matching, duplicates, Unicode token boundaries, quoted/empty/trailing-comma grammar, field scope, rich textual projection, logical/current-row pipeline composition, limits, cancellation, optimize, durability, and reopen. The request-bounded phrase vector and monotonic row-local scan remain API-owned; portable SQLite has no exact Unicode-boundary equivalent and an extension primitive would not avoid public-row decode. Exact-build message p95 is 3.906/40.608 ms narrow/wide and rich-field p95 is 3.548/48.358 ms with byte-identical storage work to same-cardinality `contains_all` controls. |
| `LQL-F38` | subquery/`in(...)` filter ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-048-query-backed-exact-membership)) | shipped | no | `ROWS`, `SQL` | `API` | P3 | Fourteen successful and five error pinned cases plus parser and 8,192-entry real-extension regressions cover exact/`uniq` output, rich/missing/nested projection, empty identities, `in`/`contains_any`/`contains_all`, nesting, logical/current-row composition, strict one-field output, shared request time, eight nesting levels, 32 lists, request-local caching, cumulative work/state/result limits, cancellation, optimize, flush, and reopen. Query-backed exact membership measures 5.849/7.341/7.440 ms narrow and 32.622/33.501/34.096 ms wide p50/p95/p99 versus 3.220/4.251/4.312 and 31.197/35.548/41.786 ms static controls. The inherent second scan doubles narrow work and adds one pruned block to wide work; wide internal query time is 1.5% higher even though its endpoint p95/p99 are lower run variation. `SQL-LOG-048` gives direct users the bounded two-scan retained-string foundation. Composition stays in Rust over public rows; no nested-query vtab, private-table access, extension primitive, or storage change is used. |
| `LQL-F39` | quoted identifiers/literals and escapes | shipped | partial | none | `API` | P0 | Double/single literals decode pinned Go escapes, backticks remain raw, quoted pipes do not split pipelines, and quoted field names are literal keys. Escapes producing non-UTF-8 bytes fail explicitly because the retained rich-log/JSON model is UTF-8. |
| `LQL-F40` | comments and multi-line queries | shipped | no | none | `API` | P2 | Parser-only. Fourteen successful and six error pinned cases plus failing-then-passing unit and real-extension regressions establish LF/CRLF comments, multiline composition, quoted hashes, one optional terminal semicolon, strict malformed tails, lexical line/column errors, limits, durability, and reopen. Exact-build evidence confirms byte-identical storage work without an extension primitive. |
| `LQL-F41` | `equals_common_case` / `contains_common_case` | shipped | no | `ROWS` | `API` | P3 | Fourteen successful and eight error pinned cases plus parser and 8,192-entry real-extension regressions establish Go-simple whole-uppercase/per-`Lu` lowercase expansion, exact versus Unicode-boundary contains behavior, Unicode/titlecase/normalization edges, empty/missing/rich projection, strict grammar, ten-uppercase and cumulative 8,192-value/4-MiB bounds, limits, cancellation, optimize, flush, shutdown, and reopen. Exact common-case measures 2.935/4.221/4.440 ms narrow and 30.465/31.608/31.980 ms wide p50/p95/p99 versus 3.161/3.352/3.417 and 29.714/33.087/39.691 ms static-list controls. Contains common-case measures 2.967/3.181/3.355 and 36.014/37.574/37.904 ms versus 2.934/3.555/5.178 and 35.931/37.140/37.603 ms controls. Every pair has byte-identical public work and output; endpoint differences are run/parser variation. Portable SQLite lacks both Go-simple Unicode expansion and exact phrase boundaries, and pushdown cannot avoid required public-row decode, so no SQL recipe or extension primitive is claimed. |

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
| `LQL-P17` | `sample` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-049-bounded-random-log-sample)) | shipped | no | `SQL` | `API` | P3 |
| `LQL-P18` | `facets` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-031-bounded-facets-over-public-log-fields)) | shipped | no | `VALUES`, `COUNT`, `SQL` | `API` | P2 |
| `LQL-P19` | `coalesce` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-032-first-nonempty-textual-log-field)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P20` | `copy` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-033-copy-one-exact-retained-metadata-field)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P21` | `rename` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-034-rename-one-exact-top-level-retained-metadata-field)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P22` | `format` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-035-format-two-exact-retained-metadata-fields)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P23` | `math` / `eval` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-036-arithmetic-over-exact-retained-numeric-fields)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P24` | `len` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-037-utf-8-byte-length-of-one-exact-retained-field)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P25` | `hash` ([evidence](QUERY_EVIDENCE.md#session-18-logsql-p3-bounded-hash)) | shipped | no | none | `API` | P3 |
| `LQL-P26` | `collapse_nums` ([evidence](QUERY_EVIDENCE.md#session-18-logsql-p3-bounded-collapse-nums)) | shipped | no | none | `API` | P3 |
| `LQL-P27` | `decolorize` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-050-strip-csi-color-sequences-from-one-exact-field)) | shipped | no | `SQL` | `API` | P3 |
| `LQL-P28` | `drop_empty_fields` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-038-drop-one-empty-retained-metadata-field)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P29` | `replace` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-039-literal-replacement-in-one-exact-retained-field)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P30` | `replace_regexp` | shipped | no | none | `API` | P2 |
| `LQL-P31` | `split` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-051-literal-split-of-one-exact-field)) | shipped | no | `SQL` | `API` | P3 |
| `LQL-P32` | `extract` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-040-two-literal-delimited-fields-from-one-exact-retained-field)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P33` | `extract_regexp` | shipped | no | none | `API` | P2 |
| `LQL-P34` | `pack_json` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-041-pack-selected-rich-metadata-fields-as-json)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P35` | `pack_logfmt` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-052-pack-fixed-exact-fields-as-logfmt)) | shipped | no | `SQL` | `API` | P3 |
| `LQL-P36` | `unpack_json` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-042-unpack-selected-rich-fields-from-a-json-object)) | shipped | no | `SQL` | `API` | P2 |
| `LQL-P37` | `unpack_logfmt` ([SQL foundation](QUERY_SQL_EQUIVALENTS.md#sql-log-053-unpack-fixed-fields-from-unquoted-logfmt)) | shipped | no | `SQL` | `API` | P3 |
| `LQL-P38` | `unpack_syslog` ([SQL foundation](QUERY_SQL_EQUIVALENTS.md#sql-log-054-decode-one-fixed-rfc5424-header)) | in progress | no | `SQL` | `API` | P3 |
| `LQL-P39` | `unpack_words` | missing | no | `SQL` | `API` | P3 |
| `LQL-P40` | `json_array_concat` | missing | no | `SQL` | `API` | P3 |
| `LQL-P41` | `json_array_len` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-043-top-level-json-array-length)) | shipped | no | `SQL` | `API` | P2 |
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

`LQL-P22` interpolates quoted or unquoted patterns over the current rich row.
The destination defaults to `_msg`; `as` accepts one exact field. Optional
`if (...)` evaluates the existing filter language, including an empty
match-all condition. Literal prefixes decode HTML entities. `<field>` uses
the established textual projection; `<_>`, `<*>`, and `<>` are empty
placeholders, and wildcard field references fail explicitly. The `uc`, `lc`,
`q`, URL, hex, Base64, numeric-hex, `time`, `duration`, `duration_seconds`,
and `ipv4` options match the pinned VictoriaLogs processor, including raw
fallbacks, simple Unicode case mapping, exact integer/scientific timestamp
inference, and nanosecond RFC3339 output.

`keep_original_fields` retains a nonempty destination; `skip_empty_results`
does so only when the formatted result is empty. Timeless retains explicit
empty strings and rich source types even where VictoriaLogs stream JSON omits
empty columns. A destination that would replace a retained object or descend
through a scalar fails with HTTP 422 `field_conflict`. Pattern traversal,
source-value projection, transform expansion, output, results, response bytes,
and cancellation are bounded. Executable `SQL-LOG-035` provides ordinary
JSON1/`printf` interpolation for exact metadata paths. Language syntax,
arbitrary patterns, codecs, conditional composition, destination mutation,
limits, and HTTP envelopes remain in the Rust API; no extension primitive,
private table, or storage-contract change is required. Exact-build evidence
records 3.297/39.353 ms narrow/wide p95 versus 3.090/35.941 ms for
byte-identical same-scan controls; `QSF-171` accepts the +6.7%/+9.5% bounded
formatting cost.

`LQL-P23` evaluates one or more comma-separated floating-point expressions
with case-insensitive `math` or `eval`. Entries execute from left to right, so
a later expression can read an earlier result. `as` is optional; without an
explicit destination, the canonical parenthesized expression text becomes
the field name. Exact quoted and dotted retained fields are accepted, while
wildcard destinations fail explicitly.

The precedence order is `^`, `*`/`/`/`%`, `+`/`-`, `&`, `xor`, `or`, then
`default`; every binary operator, including power, associates left. Unary
plus/minus and parentheses are supported. Functions are `abs`, `ceil`,
`exp`, `floor`, `ln`, `max`, `min`, `now`, `rand`, and one- or two-argument
`round`. Function calls accept the pinned trailing comma. `default` replaces
only NaN, while `max` and `min` skip NaN operands in VictoriaLogs order.

Fields and constants use the established VictoriaLogs math coercion chain:
decimal/base-zero/scaled numbers, durations as nanoseconds, byte sizes,
RFC3339 timestamps as Unix nanoseconds, and IPv4 addresses as unsigned
integers. Missing, null, empty, arrays, objects, and invalid text become NaN.
Results are string fields using fixed non-exponent rendering plus `NaN`,
`+Inf`, and `-Inf`. Bitwise operations use the pinned unsigned conversion for
negative, nonfinite, and out-of-range inputs rather than Rust's saturating
float cast. `now()` is sampled once for the pipeline invocation; `rand()` is
uniform on `[0,1)`.

Timeless overwrites compatible scalar destinations but returns HTTP 422
`field_conflict` rather than replacing a retained object or descending
through a scalar. AST nodes/nesting, evaluated work, temporary state, result
rows, response bytes, and cancellation are bounded; durable rich source rows
remain unchanged. Executable `SQL-LOG-036` gives direct users an ordinary
JSON1 arithmetic foundation for exact retained numeric fields and documents
why SQLite `CAST` alone is not the complete coercion or language contract.
Grammar, sequential composition, functions, coercions, destinations, limits,
and HTTP envelopes remain Rust API work. The complete 690-case pinned oracle
and `QSF-172` record the language and retained-model boundary; no extension
primitive, private table, or storage-contract change is required.
Exact-build evidence records 3.357/39.127 ms narrow/wide p95 versus
3.292/37.655 ms for byte-identical same-scan controls; `QSF-173` accepts the
+2.0%/+3.9% bounded expression cost.

`LQL-P24` measures one exact current-row field in UTF-8 bytes and writes a
decimal string to one exact destination. Parentheses and `as` are optional;
the default and empty quoted alias are `_msg`; case is ignored; and later
pipeline stages observe the result. Strings count decoded bytes, booleans and
numbers count their textual representation, and arrays count compact JSON.
Missing, null, empty strings, and exact retained object parents count as zero
under the pinned flattened-view policy, while dotted leaves remain
addressable. Canonical `_msg`, `_time`, and `level` fields use their current
rendered values.

Sources remain typed and immutable. A destination that would replace an
object or descend through a scalar fails with HTTP 422 `field_conflict`.
Traversal work, temporary state, result/response size, and cancellation are
bounded. Executable `SQL-LOG-037` uses public rows, JSON1, and a `BLOB` cast to
distinguish UTF-8 bytes from SQLite's codepoint-counting `length(TEXT)`.
Grammar, canonical fields, sequential composition, limits, and HTTP envelopes
remain Rust API work. The complete 711-case pinned oracle and `QSF-174` record
the language and rich-retention boundary; no extension primitive, private
table, or storage-contract change is required.
Exact-build evidence records 3.785/40.724 ms narrow/wide p95 versus
3.620/36.622 ms for byte-identical same-scan controls; `QSF-175` accepts the
+4.6%/+11.2% bounded byte-length cost.

`LQL-P25` hashes one exact current-row field with seed-zero xxHash64, masks the
result to 53 bits so every value is exactly representable as a binary64
integer, and writes its decimal string to one exact destination. Parentheses
and `as` are optional; the default and empty quoted aliases are `_msg`; names
are case-insensitive; and later pipes observe the written value. Strings use
their decoded bytes, numbers and booleans their textual spelling, and arrays
compact JSON. Missing, null, empty, and exact retained object parents use the
empty byte string under the pinned flattened-view policy; dotted leaves and
canonical current `_msg`, `_time`, and `level` fields remain addressable.

The Rust API streams arrays into the hasher after a bounded/cancellable
traversal, caps cumulative work and temporary state, preserves native rich
sources, and returns HTTP 422 `field_conflict` for unsafe destinations. Core
SQLite/libSQL has no portable exact xxHash64 expression, so this row has an
explicit no-SQL disposition rather than a misleading recipe. The complete
1,033-case pinned oracle and `QSF-209` record the language and retained-model
boundary. The required public scan and decode are unchanged. Exact-build
evidence records 3.455/36.785 ms narrow/wide p95 versus 3.481/36.223 ms for
same-public-work controls. `QSF-210` accepts the -0.7%/+1.6% endpoint-tail
variation and the larger decimal-hash response without adding an extension
scalar.

`LQL-P26` is a strict, API-owned current-row text transform. Pinned
VictoriaLogs source defines optional `if (...)`, optional exact `at <field>`,
and terminal optional `prettify` grammar; decimal and eligible hexadecimal
tokens collapse to `<N>`, while ordered prettification recognizes UUID, IPv4,
time, date, and datetime token shapes. Timeless preserves native rich values
when their textual projection is unchanged, writes a string only after an
actual collapse, and never mutates durable source rows. Work, temporary state,
result/response size, deadline, and cancellation are bounded.

Core SQLite/libSQL has no portable tokenizer with these exact boundaries and
prettification semantics, so the foundation is `none`, not a misleading SQL
recipe. The complete 1,055-case pinned oracle, direct evaluator regression,
and real-extension durability regression close the semantic row. Exact-build
evidence measures 3.135/34.525 ms narrow/wide p95 versus 3.143/36.735 ms for
same-public-work controls. `QSF-212` accepts the -0.3%/-6.0% endpoint-tail
variation as whole-run/API variation after identical storage reads and keeps
the transform above the unchanged public extension boundary.

`LQL-P27` removes VictoriaLogs' exact Control Sequence Introducer byte form
from one current-row field. The case-insensitive command defaults to `_msg`
and accepts at most one exact quoted or dotted field. Wildcards, prefixes,
parentheses, comma-separated fields, attached suffixes, and trailing tokens
fail explicitly. It removes `ESC [` followed by zero or more parameter bytes
`0x30..0x3f`, zero or more intermediate bytes `0x20..0x2f`, and an optional
final byte `0x30..0x7e`. Incomplete CSI is removed, a byte outside those
classes remains, and OSC/DCS sequences are unchanged.

Timeless preserves native missing, null, number, boolean, array, and object
states when their textual projection contains no CSI; a real removal writes a
string only to the request-owned current row. Sequential pipes see that
string, while durable rich rows remain immutable. Work, temporary state,
result/response size, deadline, cancellation, optimize, shutdown, and reopen
are bounded by the shared contracts. Executable `SQL-LOG-050` supplies direct
SQLite/libSQL users an exact BLOB-state recursive-CTE foundation over public
rows. The API still owns LogsQL grammar, rich no-op preservation, composition,
limits, cancellation, and envelopes. The complete 1,069-case pinned oracle,
direct evaluator regression, and real-extension durability regression pin the
semantic boundary. Every row has already crossed the bounded public `logs`
surface, so no extension primitive or private-table access is justified.
Exact-build evidence measures 3.101/36.385 ms narrow/wide p95 versus
3.169/34.872 ms for identical-output, same-public-work format controls.
`QSF-214` accepts the -2.2%/+4.3% endpoint-tail variation and +1.4%/+3.7%
request-attributed API mean as bounded current-row construction/scanning work
after identical public reads. Storage formats and extension contracts remain
unchanged.

`LQL-P31` is a strict API-owned current-row literal split. Its
case-insensitive grammar requires one quoted or compound-token separator,
defaults the source and destination to `_msg`, accepts optional `from` and
`as` keywords (including the upstream shorthand without either keyword), and
requires exact quoted or dotted fields. Wildcards, prefixes, commas,
parenthesized call syntax, attached suffixes, missing operands, and trailing
tokens fail before storage work. The separator named `from` remains valid
when quoted.

Splitting is literal and non-overlapping. Leading, trailing, and consecutive
separators retain empty elements; a missing separator produces one element;
an empty separator emits Unicode scalar values; and an empty source therefore
produces `[""]` for a nonempty separator but `[]` for an empty separator.
VictoriaLogs-compatible output is compact JSON-array text, including its
exact `\u003c` and `\u0027` spellings. Numbers, booleans, and arrays use the
pinned textual projection; missing, null, and exact object parents project to
empty text. The source remains typed and unchanged when the destination
differs, and durable rows are never mutated.

The bounded Rust evaluator uses no BEAM, NIF, HTTP fallback, private table, or
extension language syntax. It observes cumulative work/state/result/response,
deadline, cancellation, destination-conflict, optimize, shutdown, and reopen
contracts. The complete 1,091-case pinned oracle, direct evaluator regression,
and real-extension durability regression pin the semantic boundary.
Executable `SQL-LOG-051` gives direct SQLite/libSQL users a public-row
recursive-CTE/JSON1 foundation, including empty-piece and Unicode-scalar
behavior. Every row has already crossed the bounded public `logs` surface, so
the ordinary-SQL implementation does not justify an extension primitive or
storage-contract change.

Exact-build `QSF-216` measures split at 3.219/3.481/4.063 ms narrow and
37.529/38.655/40.113 ms wide p50/p95/p99. Identical-output composition
controls measure 3.078/4.786/4.878 and 37.964/40.047/40.151 ms. Split p95 is
27.3%/3.5% lower, while request-attributed API means are 3.1% higher/1.4%
lower. Each pair returns 64 rows and 1,984 bytes and performs byte-identical
public work: one/four candidate blocks, 1,024/8,192 decoded entries,
128/8,192 returned public rows, and 235,778/1,914,055 payload bytes per query.
The tail differences are retained as whole-run variation; the API means show
bounded row-local splitting after an unchanged storage scan.

`LQL-P28` removes every empty field from the current row. Empty means an
explicit JSON null or a zero-byte string under the pinned VictoriaLogs
textual field policy. Missing fields are already absent. Numeric zero,
boolean false, nonempty strings, and arrays—including an empty array—remain
present. Retained objects are traversed recursively; empty leaves and newly
empty parent objects are pruned, while arrays remain atomic. A row with no
fields after pruning is omitted. The case-insensitive command accepts no
arguments, parentheses, aliases, or trailing syntax, and it observes fields
created or selected by earlier pipeline stages.

Timeless preserves native JSON values instead of flattening durable metadata.
The transformation mutates only request-owned response rows in place, with a
128-level nesting ceiling, a hard traversal-work bound, periodic cancellation,
and the shared final result/response limits. Executable `SQL-LOG-038` gives
direct users an ordinary JSON1 recipe for removing one known empty metadata
path; fixed schemas can repeat it in explicit CTE order. Dynamic current-row
discovery, canonical fields, recursive parent/row pruning, limits, and HTTP
envelopes remain Rust API work. The complete 722-case pinned oracle and
`QSF-176` record the flattened-versus-rich boundary. No extension primitive,
private table, durable mutation, or storage-contract change is required.
Exact-build evidence records 4.542/38.151 ms narrow/wide p95 versus
6.994/35.779 ms for byte-identical same-scan controls; `QSF-177` accepts the
-35.1%/+6.6% bounded in-place traversal variation.

`LQL-P29` performs case-sensitive literal replacement over one exact current-
row field, with optional `if`, `at`, and first-`N`/zero-unbounded `limit`
modifiers. Timeless preserves native retained values when no literal matches
and materializes a query-row string only when a replacement occurs. Public
`SQL-LOG-039` covers ordinary all-occurrence exact-field replacement through
SQLite JSON1 and core `replace()`; conditional, limited, sequential, typed,
bounded, and envelope semantics remain Rust API work. `QSF-178` and
`QSF-179` record complete 740-case oracle parity and exact-build evidence.

`LQL-P30` performs case-sensitive RE2-family replacement over one exact
current-row field. It supports optional `if`, `at`, and zero-unbounded or
first-`N` `limit`; dot-newline behavior and inline flags; numbered, named, and
full-match capture expansion; literal dollar escaping; UTF-8 empty-pattern
boundaries; and sequential current-row composition. Native values remain
native on a no-match, while an actual replacement becomes a query-row string
without mutating durable rich metadata. Pattern compilation, capture work,
projected values, output, result/response size, and cancellation are bounded.

Core SQLite and the public extension have no portable RE2-compatible regexp-
replacement scalar with capture-template expansion, so this row honestly has
no SQL foundation or recipe. Direct users must compose it in their host or
load a separate regexp extension. `QSF-180` records the language/storage
boundary and the complete 765-case pinned oracle. No extension primitive,
private table, or storage-contract change is required. Exact-build evidence
records 3.442/40.628 ms narrow/wide p95 versus 3.391/35.822 ms for byte-
identical same-scan controls; `QSF-181` accepts the +1.5%/+13.4% bounded
regexp/capture-expansion cost above the unchanged public storage boundary.

`LQL-P32` parses literal-delimiter patterns in the Rust logs API and applies
them only to request-owned rows returned by the public `logs` table. It
supports named and anonymous placeholders, HTML-decoded delimiters, nonempty
first-prefix search and empty-prefix anchoring, default `_msg` or exact `from`
sources, automatic Go double/single/raw quoted-string decoding, `plain:`,
conditions, default empty writes, `keep_original_fields`,
`skip_empty_results`, nested destinations, and sequential composition.
Timeless retains explicit empty strings and preserved native rich values and
rejects scalar replacement of a retained object. Work, capture state, paths,
results, response bytes, and cancellation are bounded; durable rows remain
immutable across optimize and reopen.

Public
[`SQL-LOG-040`](QUERY_SQL_EQUIVALENTS.md#sql-log-040-two-literal-delimited-fields-from-one-exact-retained-field)
uses JSON1 and core `instr()`/`substr()` for two fixed unquoted captures.
General LogsQL pattern grammar, quoted decoding, current-row mutation and
preservation, limits, cancellation, and envelopes remain Rust API work. The
complete 802-case pinned VictoriaLogs oracle and real-extension regressions
cover the language boundary. No extension primitive, private table, durable
mutation, or storage-contract change is required. Exact-build evidence
records 3.269/39.052 ms narrow/wide p95 versus 3.201/33.944 ms for byte-
identical same-scan controls; `QSF-183` accepts the +2.1%/+15.0% bounded
literal extraction/field-write cost above the unchanged public storage
boundary.

`LQL-P33` compiles one RE2-family pattern in the Rust logs API and applies it
only to request-owned rows returned by the public `logs` table. The first
match supplies every named capture; anonymous groups participate in matching
but do not create fields. The command supports default `_msg` or one exact
`from` source, optional `if`, case-insensitive syntax, dot-newline matching by
default with inline flag overrides, `(?P<name>...)` and `(?<name>...)` groups,
default empty writes, `keep_original_fields`, `skip_empty_results`, and
sequential current-row composition. Strings, numbers, booleans, and arrays
use the retained textual projection; preserved rich values stay native.
Missing matches and unmatched optional groups become explicit empty strings
in Timeless output. Writes that would replace a retained object fail with an
actionable field-conflict envelope. Pattern compilation, captures, projected
bytes, paths, work, response state, deadlines, and cancellation are bounded;
durable rows remain immutable across optimize and reopen.

Core SQLite and the public extension expose no portable RE2-compatible
named-capture extraction scalar. This row therefore has foundation `none`
and no SQL recipe: direct SQLite/libSQL users must compose the regex in their
host or deliberately load a separate general regexp extension. The language
operation remains API composition over public rows. It does not justify
LogsQL syntax, private-table access, or a language-specific primitive in the
storage extension.

Exact-build evidence records 3.154/35.517 ms narrow/wide p95 versus
3.922/33.808 ms for byte-identical same-scan controls. The -19.6%/+5.1% p95
and -1.6%/+6.1% internal API variation follows exactly the same one/four
blocks, 1,024/8,192 decoded entries, 235,778/1,914,055 payload bytes, and
128/8,192 public rows. `QSF-185` accepts the bounded first-match capture/write
cost above the unchanged public storage boundary.

`LQL-P34` packs an exact, prefix, or all-field snapshot of each request-owned
row into one deterministic compact JSON object. The Rust logs API accepts the
case-insensitive `pack_json` command, optional `fields (...)`, default `_msg`,
and explicit `as` or bare destinations. Selection is evaluated before the
destination write, so packing `_msg` into `_msg` retains the source value in
the generated object and later pipeline stages observe the packed string.
Missing exact fields produce `{}`; prefix selection reconstructs nesting;
overlapping selectors form an idempotent union. Work, selected paths,
temporary JSON bytes, nesting, results, response bytes, deadlines, and
cancellation are bounded, and request-local writes never mutate durable rows.

This is an intentional rich-row compatibility profile rather than byte-for-
byte VictoriaLogs flattening. Timeless preserves JSON numbers, booleans,
arrays, objects, explicit nulls, and empty strings; it emits one valid object
with stable key order. VictoriaLogs v1.52.0 flattens values to strings, omits
empty values, follows current column order, and can emit duplicate keys for
overlapping selectors. The complete 850-case pinned oracle records both the
upstream behavior and this selected difference.

Public
[`SQL-LOG-041`](QUERY_SQL_EQUIVALENTS.md#sql-log-041-pack-selected-rich-metadata-fields-as-json)
uses only `logs` plus SQLite JSON1 to distinguish missing paths and preserve
native JSON values while constructing a bounded object. LogsQL selector
grammar, recursive prefix/all selection, destination mutation, limits,
cancellation, and HTTP envelopes remain Rust API composition over public
rows. No extension primitive, private table, durable mutation, or storage-
contract change is required.

Exact-build evidence records 3.146/37.921 ms narrow/wide p95 versus
3.098/35.717 ms for same-scan controls. The +1.5%/+6.2% p95 and
-1.3%/+7.4% internal API variation follows exactly the same one/four blocks,
1,024/8,192 decoded entries, 235,778/1,914,055 payload bytes, and
128/8,192 public rows. Packed responses are 2,688 bytes versus 1,600 for the
plain-field controls because every selected value is wrapped in a JSON object
string. `QSF-187` accepts the bounded rich selection/serialization cost above
the unchanged public storage boundary.

`LQL-P35` snapshots exact, prefix, empty-list, or all-current-field
selections and writes deterministic logfmt text to `_msg` or one explicit
exact destination. Grammar is case-insensitive; destinations may follow
`as` or appear bare; terminal `as` keeps `_msg`; selectors may be quoted;
and malformed lists, wildcards in destinations, attached suffixes, and
trailing tokens fail before storage work. Selection precedes mutation, so an
overwritten destination contributes its old value.

Timeless emits raw `name=value` pairs in bytewise field-name order. Exact
missing, null, empty, and object-parent values emit empty text. Recursive
prefix/all traversal flattens objects to dotted leaves, arrays remain atomic
compact JSON, and values containing runes through U+0020, quotes, or
backslashes use VictoriaLogs-compatible JSON-string quoting with exact
`\u003c` and `\u0027` spellings. Overlapping selectors form an idempotent
union. This deliberately differs from VictoriaLogs v1.52.0 current-column
ordering and repeated fields for overlaps; the complete 1,111-case pinned
oracle records both the upstream behavior and the retained-model decision.

The bounded Rust evaluator owns language parsing, recursive current-row
selection, destination mutation/conflicts, work/state/result/response limits,
deadline cancellation, and HTTP envelopes. Durable source rows remain
unchanged through optimize, shutdown, and reopen. Executable
[`SQL-LOG-052`](QUERY_SQL_EQUIVALENTS.md#sql-log-052-pack-fixed-exact-fields-as-logfmt)
gives direct SQLite/libSQL users the fixed exact-path core-SQL/JSON1
foundation. Since all selected rows already cross the public `logs` surface,
no extension primitive, private table, codec, format, or storage-contract
change is justified.

Exact-build `QSF-218` measures `pack_logfmt` at 3.459/3.805/4.335 ms narrow
and 37.975/39.144/42.387 ms wide p50/p95/p99. Identical-output `format`
controls measure 3.506/3.769/3.924 and 34.373/38.212/38.572 ms. The
+1.0%/+2.4% p95 and -0.2%/+8.2% request-attributed API mean occurs after
identical public work: one/four candidate blocks, 1,024/8,192 decoded entries,
128/8,192 returned public rows, and 235,778/1,914,055 payload bytes per query.
The wide API mean is retained honestly as bounded current-row traversal,
selection, quoting, and response construction; it does not justify storage
pushdown.

`LQL-P36` parses one exact current-row field as a JSON object and writes the
selected values back into that request-owned row. The Rust logs API accepts
case-insensitive `unpack_json`, optional `if (...)`, default `_msg`, optional
bare or `from` source fields, exact/prefix/all `fields (...)`,
`preserve_keys (...)`, `result_prefix`, `keep_original_fields`, and
`skip_empty_results`. The source is snapshotted before writes, so unpacking a
JSON member over the source itself is deterministic and later pipeline stages
see the replacement. Omitted or empty `fields (...)` selects all fields;
missing exact selections become empty strings, while unmatched prefixes add
nothing.

Timeless accepts a string containing a whitespace-padded JSON object or an
already retained native object. It preserves native strings, numbers,
booleans, arrays, objects, explicit nulls, empty strings, and empty objects;
reconstructs nesting while retaining unrelated existing siblings; and keeps
literal dotted JSON keys distinct from nested paths. `preserve_keys` makes a
selected object atomic without converting it to text. The pinned upstream
bare `NaN` token becomes the string `"NaN"`. A malformed source beginning
with `{` still supplies empty values for requested exact fields, matching the
upstream boundary; other malformed/nonobject sources are no-ops. Default
writes replace scalar values, `keep_original_fields` retains nonempty
destinations, and `skip_empty_results` suppresses null or empty-string writes.
Scalar writes cannot replace retained objects or cross a scalar parent.

VictoriaLogs v1.52.0 flattens nested leaves into textual columns, compact-
serializes arrays, textualizes numbers and booleans, and maps null to empty
text. Timeless deliberately retains its richer nested JSON model. The
complete 875-case pinned oracle records the upstream grammar, selection,
preservation, malformed-input, and textualization behavior; real-extension
regressions pin the selected rich-value policy.

Public
[`SQL-LOG-042`](QUERY_SQL_EQUIVALENTS.md#sql-log-042-unpack-selected-rich-fields-from-a-json-object)
uses only `logs` plus SQLite JSON1 to validate one object source, select a
fixed set of exact paths, distinguish missing/null/empty states, preserve
native JSON subtypes, and reconstruct nesting. Dynamic LogsQL grammar,
conditions, recursive prefix/all selection, current-row mutation,
preservation modes, limits, cancellation, and HTTP envelopes remain bounded
Rust API composition over public rows. No extension primitive, private table,
durable mutation, or storage-contract change is required.

Exact-build evidence records 3.151/40.062 ms narrow/wide p95 versus
3.763/38.694 ms for equal-output pack-plus-copy controls. The -16.3%/+3.5%
p95 and -0.5%/+3.3% internal API variation follows exactly the same one/four
blocks, 1,024/8,192 decoded entries, 235,778/1,914,055 payload bytes, and
128/8,192 public rows. All four responses are 2,112 bytes. The wide
`unpack_json` p99 is retained honestly at 65.292 ms. `QSF-189` accepts the
bounded rich parse/select/write cost above the unchanged public storage
boundary.

`LQL-P37` parses one exact current-row field as logfmt and writes decoded
string values back into that request-owned row. The Rust logs API accepts
case-insensitive `unpack_logfmt`, optional `if (...)`, default `_msg`, optional
bare or `from` source fields, exact/prefix/all `fields (...)`, arbitrary
`result_prefix`, and one terminal `keep_original_fields` or
`skip_empty_results` modifier. The source is snapshotted before writes;
omitted or empty `fields (...)` selects all parsed names; unmatched prefixes
add nothing; and every missing exact selection becomes an empty string.

Unquoted values end at ASCII space. Double- and single-quoted values use the
pinned Go escape forms, including control, octal, `\x`, `\u`, and `\U`
escapes; backtick values are raw and discard carriage returns. A malformed or
unterminated quoted prefix falls back to unquoted parsing, lone names become
empty values, and repeated names are last-wins. All decoded values are
strings. Timeless reconstructs dotted names and prefixes into retained nested
metadata while preserving unrelated siblings; names with empty path segments
remain literal keys. This is the explicit richer-model policy over
VictoriaLogs' flattened textual columns.

Default writes replace scalar destinations, `keep_original_fields` retains
existing nonempty values, and `skip_empty_results` suppresses empty decoded
values. Writes cannot replace retained objects or descend through scalar
parents. Parsing, names, decoded bytes, paths, temporary state, work, results,
response bytes, deadlines, and cancellation are bounded. Request-local
mutation never changes durable source rows through optimize, shutdown, or
reopen. The complete 1,134-case pinned oracle establishes grammar, quote and
escape decoding, malformed fallback, selection, snapshots, duplicates,
conditions, prefixes, preservation, and error behavior.

Public
[`SQL-LOG-053`](QUERY_SQL_EQUIVALENTS.md#sql-log-053-unpack-fixed-fields-from-unquoted-logfmt)
uses only bounded `logs` rows and a recursive CTE for a fixed set of keys in
well-formed unquoted logfmt. Complete quoting/escaping, dynamic selection,
current-row mutation, rich nesting, limits, cancellation, and HTTP envelopes
remain Rust API composition over the same public rows. No extension primitive,
private table, durable mutation, or storage-contract change is required.

Exact-build `QSF-220` measures `unpack_logfmt` at 3.355/3.619/4.494 ms
narrow and 41.166/42.437/43.357 ms wide p50/p95/p99. Identical-output
pack-plus-copy controls measure 3.328/3.573/4.213 and
38.087/41.659/44.310 ms. The +1.3%/+1.9% p95 and +1.2%/+5.9%
request-attributed API mean occur after identical public work: one/four
candidate blocks, 1,024/8,192 decoded entries, 128/8,192 returned public
rows, and 235,778/1,914,055 payload bytes per query. The row-local parse,
escape decode, selection, and nesting cost is bounded and does not justify
storage pushdown.

`LQL-P38` parses one exact current-row field as syslog and writes decoded
string fields back into that request-owned row. The Rust logs API accepts
case-insensitive `unpack_syslog`, an optional leading `if (...)`, default
`_msg`, one optional bare or `from` exact source, an optional signed-duration
`offset`, an optional `result_prefix`, and terminal
`keep_original_fields`. Clause order is strict and unsupported syntax fails
before storage execution. The source is snapshotted, missing sources are
no-ops, and only leading ASCII space, tab, carriage return, and newline are
trimmed; trailing message bytes remain exact.

The bounded parser covers RFC3164 and RFC5424, optional PRI, all 24 facility
keywords, all eight severity levels, partial invalid-input behavior,
RFC5424 structured-data identifiers and quoted parameters, CEF base and
extension fields, and CEE JSON objects. RFC5424 timestamps remain lexical.
RFC3164 RFC3339/ISO timestamps normalize to UTC independently of `offset`;
classic timestamps use the request year and host timezone or explicit fixed
offset, move to the previous year only when more than one day in the future,
and reproduce Go's leap-day normalization. CEE numbers and booleans become
text, arrays become compact JSON text, nested objects flatten to dotted names,
and null members are omitted.

Timeless reconstructs dotted decoded names and result prefixes as retained
nested metadata while preserving unrelated siblings. This is the explicit
richer-model policy over VictoriaLogs' flattened textual columns. Default
writes replace scalar destinations; `keep_original_fields` retains existing
nonempty destinations. Writes cannot replace retained objects or descend
through scalar parents. Parsing, decoded fields and bytes, nesting, paths,
temporary state, work, results, response bytes, deadlines, and cancellation
are bounded. Query-backed conditions resolve under the same cumulative
limits. Request-local mutation never changes durable public rows through
optimize, shutdown, or reopen.

The complete 1,155-case pinned oracle establishes grammar, header and
structured-data parsing, conditions, prefixes, preservation, CEF/CEE,
partial invalid-input behavior, and strict errors. Direct source-parity tests
also pin RFC3164 leap-day normalization that depends on the evaluation year.
Public
[`SQL-LOG-054`](QUERY_SQL_EQUIVALENTS.md#sql-log-054-decode-one-fixed-rfc5424-header)
uses only bounded `logs` rows and core SQLite to decode the fixed RFC5424
header when structured data is `-`. RFC3164, structured RFC5424, CEF/CEE,
timezone/year rules, current-row mutation, limits, cancellation, and HTTP
envelopes remain Rust API composition over the same public rows. No extension
primitive, private table, durable mutation, or storage-contract change is
required.

`LQL-P41` counts the top-level elements of one exact current-row field. The
Rust logs API accepts case-insensitive `json_array_len`, parenthesized or bare
exact source fields, optional `as`, bare or quoted exact destinations, and the
default `_msg` destination. Source and destination may be dotted paths. The
source is snapshotted before the destination write, so replacing it is
deterministic and later pipeline stages observe the textual decimal result.
Wildcard and prefix sources or destinations, multiple sources, missing
parentheses, and trailing tokens fail explicitly.

A retained native array is counted without flattening or reparsing it. A
string containing a whitespace-padded JSON array is parsed and counted; the
pinned VictoriaLogs bare `NaN` token counts as one element. Nested arrays and
objects each count as one top-level element. Empty arrays, missing paths,
explicit nulls, malformed text, JSON scalar text, and native scalar or object
values return `"0"`. The transform preserves the rich source value, protects
retained object destinations with an actionable 422 conflict, and bounds row,
parse-tree, path, result, response, deadline, and cancellation work. Durable
rows remain unchanged across optimize, shutdown, and reopen.

Public
[`SQL-LOG-043`](QUERY_SQL_EQUIVALENTS.md#sql-log-043-top-level-json-array-length)
uses only `logs` plus SQLite JSON1 to count one fixed exact native-array or
JSON-array-text path and return decimal TEXT. LogsQL grammar, current-row
mutation, bare-`NaN` compatibility, limits, cancellation, and HTTP envelopes
remain bounded Rust API composition over public rows. No extension primitive,
private table, durable mutation, or storage-contract change is required. The
complete 897-case pinned oracle records upstream grammar and scalar/malformed
behavior; Timeless additionally pins native rich-array fidelity through the
real extension.

Exact-build native-array evidence records 3.558/41.563 ms narrow/wide p95
versus 3.454/40.607 ms for equal-output constant-format controls. The
+3.0%/+2.4% p95 and +2.4%/-0.0% internal API variation follows exactly the
same one/four candidate blocks, 1,024/8,192 decoded entries,
235,778/1,914,055 payload bytes, 128/8,192 public rows, and 1,344-byte
responses. `QSF-191` accepts the bounded O(1) native-array count and
current-row write above the unchanged public storage boundary.

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
| `LQL-S07` | `quantile` / `stddev` ([SQL foundation](QUERY_SQL_EQUIVALENTS.md#sql-log-044-upper-step-numeric-quantile-and-population-standard-deviation)) | shipped | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S08` | `rate` / `rate_sum` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-013-numeric-aggregates-median-and-rates)) | shipped | `COUNT`, `BUCKETS`, `SQL` | `API` | P1 |
| `LQL-S09` | `sum_len` ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-log-045-summed-utf-8-byte-length-of-one-exact-field)) | shipped | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S10` | `any` / `field_min` / `field_max` ([SQL foundation](QUERY_SQL_EQUIVALENTS.md#sql-log-046-deterministic-any-and-numeric-companion-field-extrema)) | shipped | `ROWS`, `SQL` | `API` | P2 |
| `LQL-S11` | `row_any` / `row_min` / `row_max` ([SQL foundation](QUERY_SQL_EQUIVALENTS.md#sql-log-047-deterministic-rich-row-selection-and-numeric-row-extrema)) | shipped | `ROWS`, `SQL` | `API` | P2 |
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

`sum_len(fields...)` sums UTF-8 bytes across exact, prefix, or all-current-
field selections. Missing and null values contribute zero; strings contribute
raw UTF-8 bytes; and numbers, booleans, arrays, and objects use compact JSON
text. Every selected traversal is work-bounded, the checked unsigned total
fails explicitly on overflow, and cancellation leaves the public reader
reusable. Timeless intentionally returns a native JSON integer rather than
VictoriaLogs' decimal string. `SQL-LOG-045` is the executable single-exact-
metadata-path foundation; dynamic field selection, canonical fields, language
grammar, limits, cancellation, and HTTP envelopes remain Rust API work.

Exact-build evidence records 3.223/3.460/3.940 ms narrow and
34.152/35.691/36.932 ms wide p50/p95/p99 versus 3.109/3.719/4.321 and
35.066/37.760/40.220 ms for same-run numeric-`sum` controls. The 7.0%/5.5%
lower p95 follows exactly the same one/four candidate blocks, 1,024/8,192
decoded entries, 235,778/1,914,055 payload bytes, and 128/8,192 public rows.
`QSF-195` accepts the bounded constant-state Rust reduction above the unchanged
public storage boundary.

`any(field)` selects the first nonempty value in deterministic current-
pipeline order and preserves its native JSON type. Missing, null, and empty
strings do not qualify; zero, false, arrays, and objects do. VictoriaLogs only
promises an arbitrary nonempty value and its selected value changed with
physical encoding order during oracle audit, so deterministic selection is an
explicit Timeless strengthening rather than a false upstream ordering claim.
`field_min(source,result)` and `field_max(source,result)` compare nonempty
source values with the VictoriaLogs signed/unsigned/timestamp/math/natural
text comparator, preserve the first current-row tie, and return the companion
result with its retained native type. A missing companion becomes the stable
empty string; explicit null, empty strings, arrays, and objects remain
distinct. Candidate traversals and retained state are work/byte bounded and
deadline-cancellable. `SQL-LOG-046` gives direct users deterministic exact-
path `any` and finite-native-number companion extrema through public `logs`;
the complete comparator, canonical fields, rich policy, limits, cancellation,
grammar, and envelopes remain Rust API composition.

Exact-build `any` evidence records 3.077/3.293/3.597 ms narrow and
32.767/33.764/36.128 ms wide p50/p95/p99 versus 3.085/4.476/8.065 and
34.858/37.518/38.095 ms for equal-output numeric-minimum controls. Companion
extrema record 3.182/3.306/3.849 and 34.004/36.738/42.084 ms versus
3.356/3.704/4.390 and 37.000/38.270/38.988 ms for equal-output native extrema
controls. Their p95 values are respectively 26.4%/10.0% and 10.7%/4.0%
lower. Every pair performs byte-identical public work: one/four candidate
blocks, 1,024/8,192 decoded entries, 235,778/1,914,055 payload bytes, and
128/8,192 materialized rows. `QSF-197` accepts the bounded deterministic/rich
Rust reductions above the unchanged public storage boundary and retains the
42.084 ms wide companion-extrema p99 honestly.

`row_any(fields...)` selects the first current row where any exact, flattened-
prefix, or all-current field is nonempty, then returns every selected existing
field from that same row as one native nested JSON object. Missing fields are
omitted; selected null, empty strings, false, zero, arrays, and objects remain
typed. `row_min(source[, fields...])` and `row_max` require one exact nonempty
comparison field, use the complete VictoriaLogs signed/unsigned/timestamp/
math/natural text comparator, keep the first strict tie, and default the
result selection to all current fields. Empty selection returns `{}`.
Function names are case-insensitive; result aliases may use `as name` or the
upstream implicit `name` form. Candidate traversal, flattened-prefix descent,
comparison keys, and complete selected objects are work/byte bounded and
deadline-cancellable. `SQL-LOG-047` gives direct users deterministic fixed-
path rich selection and finite-native-number row extrema through public
`logs`; dynamic selectors, canonical fields, complete comparison, limits,
cancellation, grammar, and envelopes remain Rust API composition.

Exact-build `row_any` evidence records 2.880/3.097/4.720 ms narrow and
35.036/37.829/39.595 ms wide p50/p95/p99 versus 3.201/3.660/4.240 and
36.597/37.888/39.081 ms for same-scan scalar `any` controls. Its p95 is
15.4% lower/0.2% lower. Rich row extrema record 2.948/3.219/3.427 and
37.970/39.790/40.474 ms versus 3.022/3.429/3.737 and
34.149/36.227/37.943 ms for scalar companion-extrema controls, or 6.1% lower/
9.8% higher p95. The rich responses are 36 versus 12 bytes and 101 versus 26
bytes; every pair nevertheless performs byte-identical public storage work:
one/four candidate blocks, 1,024/8,192 decoded entries,
235,778/1,914,055 payload bytes, and 128/8,192 materialized rows. `QSF-199`
accepts the bounded rich-object cost and retains the wide tail and whole-
workload HWM honestly above the unchanged public storage boundary.

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
