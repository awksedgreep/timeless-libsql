# Release-grade query and documentation implementation plan

Date: 2026-08-04

Repository: `timeless-libsql`

Canonical integration branch: `main`

Starting implementation history: `release/rust-telemetry-data-plane` at
`a72634e`

This plan turns the query feature matrices into the public contract for the
extension and the three standalone Rust signal APIs. It preserves the existing
storage design and implements language behavior at the Rust API boundary. The
[query maps](QUERY_FEATURES.md), [PromQL matrix](PROMQL_FEATURE_MATRIX.md),
[LogsQL matrix](LOGSQL_FEATURE_MATRIX.md), [SQL equivalents](QUERY_SQL_EQUIVALENTS.md),
[storage findings](QUERY_STORAGE_FINDINGS.md), and
[pinned oracles](QUERY_ORACLES.md) are authoritative.

## Session 0 baseline

The promotion branch contains the completed Rust telemetry data-plane history
and is 36 commits ahead of the old `origin/main`, with no divergent main
commits. Before canonicalization, the preserved working changes add the first
query maps, SQL cookbook, findings log, README/query-guide links, and executable
CLI cookbook regressions. They must be committed without discarding or
rewriting them, pushed on the promotion branch, and then fast-forwarded to
`main`.

Initial matrix inventory:

| matrix | rows | shipped | partial | missing | deferred |
|---|---:|---:|---:|---:|---:|
| PromQL and MetricsQL | 97 | 5 | 7 | 82 | 3 |
| LogsQL | 114 | 6 | 7 | 97 | 4 |

The extension CLI baseline has 45 passing sections, including public SQL
recipes, crash/reopen, transaction, migration, retention, rollup, and packed
query coverage. The Rust baseline has three focused PromQL parser tests and ten
LogsQL library tests. These counts describe the starting point, not sufficient
parity coverage.

The starting language surface is deliberately narrow. Metrics supports exact
instant selectors and `avg_over_time`; logs supports `*`, relative time, exact
level/service, quoted substring, `limit`, and `stats count()`. Native GET
parameters and similarly named extension kernels do not imply language parity.

## Work and integration rules

Each numbered section below is a mergeable session. Related rows share a
session for coherent fixtures and benchmarks, but rows are implemented one at a
time using the workflow in `QUERY_FEATURES.md`. A row may have its own commit;
it may share a commit only with an inseparable grammar/evaluator primitive.

Every session starts from current `main`, may use a short-lived local branch,
and ends only after its exit criteria pass. Then it is merged into `main` with
its full history and `main` is pushed. A failed session is not pushed or merged.
No pull request, tag, package publication, Stack build, or downstream dependency
change is part of this plan.

The common exit criteria for every feature session are:

1. each selected row moves through `in progress` and ends as `shipped` or with
   an honest `library`, `experimental`, or `deferred` disposition;
2. a pre-implementation failure and post-implementation real-extension
   regression exist for each shipped behavior;
3. direct SQLite/libSQL behavior, Rust parsing/planning/execution, HTTP
   envelopes, errors, limits, cancellation, drain/reopen, and durability pass
   wherever applicable;
4. exact semantics pass the pinned oracle, with intentional differences named
   in the row;
5. every SQL target or foundation has a parameterized public-surface recipe
   executed through the real extension by the Rust `timeless-query-harness`;
6. user documentation, matrix state, test references, and any finding change
   in the same commit as implementation;
7. narrow and wide evidence records p50/p95/p99, cardinality, rows/blocks
   considered, blocks/entries decoded, extension/result/response bytes,
   cancellation, and RSS HWM;
8. new extension primitives additionally pass transaction, rollback, reopen,
   corruption, version/capability, and bounded-memory tests and demonstrate a
   material avoidable cost in the correct SQL/API approach; and
9. the complete applicable extension, server, parser, query, crash, migration,
   packaging, and fault gates are green from the session head.

## Sequential sessions

### Session 1: contract automation and evidence harness

Add lightweight CI checks for unique IDs, legal status/owner/priority values,
shipped-row test references, SQL recipe links, local links, and agreement
between matrices and server documentation. Add reusable pinned-oracle fixture
and narrow/wide query benchmark harnesses before expanding the language.

Exit: malformed fixture tests prove each validator fails; the existing 211
rows pass; oracle containers report the pinned builds; baseline query and RSS
evidence is checked in; normal CI can replay fixtures without network access.

### Session 2: PromQL P0 value and evaluation model

Rows: `PQL-S06`, `PQL-S11`–`PQL-S13`, `PQL-S16`, `PQL-S18`–`PQL-S21`.
Generalize range vectors, finish literals and types, and make limits,
cancellation, exact grids, and error/result envelopes properties of every AST
node. Revalidate already shipped cancellation rather than assuming it composes.

Exit: scalar, string, instant-vector, and range-vector values survive instant
and range evaluation with exact JSON special-value behavior; malformed input,
intermediate limits, cancellation, and reopen all pass Prometheus parity.

### Session 3: PromQL P0 selectors and temporal modifiers

Rows: `PQL-S01`–`PQL-S09`, excluding rows already completed in Session 2.
Add nameless and multi-name catalog planning, general range selectors, positive
and negative `offset`, `@` forms, and bounded subqueries while preserving exact
matcher and lookback behavior.

Exit: selectors prune through public catalog/query surfaces before block
decode; boundary, missing-label, duplicate-matcher, lookback, modifier, and
subquery fixtures pass Prometheus and the resource gates.

### Session 4: PromQL P0 operators and vector matching

Rows: `PQL-O01`–`PQL-O07`.

Exit: scalar/vector arithmetic, comparisons and `bool`, set operators,
one-to-one and group matching, cardinality failures, label propagation,
metric-name policy, timestamps, and special floats match Prometheus.

### Session 5: PromQL P0 aggregations

Rows: `PQL-O09`–`PQL-O16`.

Exit: every aggregation handles `by`/`without`, empty groups, special floats,
ties, parameter validation, and exact output labels; SQL recipes are executable
for each SQL-founded family.

### Session 6: PromQL P0 range reductions

Rows: `PQL-R01`–`PQL-R11`.

Exit: all windows use exact `(T-W,T]` boundaries, sparse/empty windows and
special floats match Prometheus, and measurements decide honestly between
`RAW`, existing `WINDOW`, and ordinary Rust composition.

### Session 7: PromQL P0 counters and regressions

Rows: `PQL-R12`–`PQL-R20`.

Exit: resets, extrapolation, carry-in, sparse samples, counter decrease,
regression timestamps, metric-name removal, and cancellation pass Prometheus.
Existing `rate`/`increase` kernels are not relabeled as PromQL unless they pass
the full semantic fixture.

### Session 8: PromQL P0 transforms, labels, sorting, and time

Rows: `PQL-F01`–`PQL-F13` and `PQL-F15`–`PQL-F18`. Experimental
`PQL-F14` is handled later rather than being smuggled into P0.

Exit: numeric transforms, regex replacement, label joining, absent functions,
sorting, conversions, and UTC calendar functions match Prometheus for all
value types and edge cases.

### Session 9: P0 classic Prometheus histograms

Row: `PQL-H01`.

Exit: classic float `_bucket` series pass monotonicity correction, boundary,
missing bucket, interpolation, grouping, and special-value oracle fixtures.
Native histogram rows remain blocked on a typed storage design.

### Session 10: useful Timeless/DDNet LogsQL P0 parity

Rows: revalidate all P0 log rows, then complete `LQL-F03`–`LQL-F05`,
`LQL-F08`, `LQL-F39`, `LQL-P02`, `LQL-P03`, `LQL-P09`, and
`LQL-Q01`, `LQL-Q07`, `LQL-Q08`.

Exit: absolute and relative microsecond time bounds, arbitrary typed field
equality, all eight severities, service/message filters, sort, limit, offset,
and count work through the real extension. Invalid and unsupported syntax fails
explicitly; typed/nested metadata and missing/null/empty remain distinct.

### Session 11: stable PromQL P1 completion

Rows: `PQL-S17`, `PQL-S23`, `PQL-O08`, and classification audit for
`PQL-R21`.

Exit: every applicable P1 PromQL row has passed Prometheus. Stale markers
ship only if ingestion/storage has an explicit preserved contract; otherwise
`PQL-S17` is deferred with that prerequisite rather than approximated.
Prometheus warning/info annotations must preserve exact text, source position,
deduplication, and response-envelope placement.
The pinned Prometheus 3.13.2 metadata supersedes the original plan's P1
classification for `double_exponential_smoothing`: it is experimental and
disabled by default, so Session 11 pins stable-tier rejection and leaves
implementation to a separately enabled experimental tier.

### Session 12: LogsQL P1 filters and logic

Rows: `LQL-F09`, `LQL-F10`, `LQL-F12`–`LQL-F15`, `LQL-F18`,
`LQL-F19`, `LQL-F24`, `LQL-F29`, and `LQL-F31`.

Exit: field/word/prefix/substring/regexp/case filters, typed comparisons,
presence, value types, and boolean composition pass VictoriaLogs. Safe indexed
conjuncts prune before bounded decode without changing logic.

### Session 13: LogsQL P1 discovery, projection, and statistics

Rows: `LQL-P04`–`LQL-P06`, `LQL-P08`, `LQL-S02`–`LQL-S06`,
`LQL-S08`, and `LQL-Q02`.

Exit: field names/values, keep/filter, counts, unique values, numeric
aggregates, rates, and projection preserve field types and deterministic output;
all public SQL foundations execute in CLI regressions.

Outcome: shipped all eleven rows with 75 pinned VictoriaLogs cases, 15
real-extension log API regressions, and executable `SQL-LOG-010` through
`SQL-LOG-013`. The implementation preserves rich typed values and uses bounded
Rust API composition over public extension rows without changing storage.

### Session 14: stable PromQL P2 completion

Rows: `PQL-S10`, `PQL-S14`, `PQL-S15`, `PQL-F19`, `PQL-F20`,
`PQL-H02`, and `PQL-H03`.

Exit: every applicable P2 PromQL row passes Prometheus, including quoted UTF-8
names, comments, calendar/evaluation functions, step-relative ranges, and
classic-histogram boundary behavior. Experimental features remain separate.

Outcome: shipped `PQL-S14`, `PQL-S15`, and classic-bucket `PQL-H02` with
pinned Prometheus 3.13.2 cases, real-extension HTTP/durability regressions,
and executable `SQL-PROM-056`. Oracle audit corrected the original roadmap:
`PQL-S10` is MetricsQL-only and is deferred to `MQL-09`; `PQL-F19`,
`PQL-F20`, and `PQL-H03` are feature-gated experimental PromQL and their
stable MetricsQL variants are tracked as `MQL-10` through `MQL-12`. The
stable endpoint pins the upstream rejection diagnostics without changing the
shipped `@ start()`/`@ end()` modifiers.

### Session 15: MetricsQL compatibility tier

Rows: `MQL-01`–`MQL-07` and `MQL-09`–`MQL-12` in order. `MQL-08` receives a
bounded explicit disposition later, not a blanket compatibility claim.

Exit: each shipped construct is labeled MetricsQL, passes VictoriaMetrics
`v1.148.0`, records every Prometheus difference, and cannot alter the default
PromQL interpretation silently.

Progress: `MQL-01` is shipped on explicit MetricsQL routes with pinned
VictoriaMetrics parity, executable `SQL-MQL-001`, bounded single-read
comparison-identity retention, and stable PromQL route isolation. `MQL-02` is
shipped with operation-level name retention, pinned transform/rollup/binary
matching semantics, executable `SQL-MQL-002`, and measured storage-equivalent
composition. `MQL-03` is shipped with named and shorthand union, alias
rename/removal, first-labelset precedence, duplicate-output errors, executable
`SQL-MQL-003`, stable-route isolation, and bounded public-plan composition.
`MQL-04` is shipped with ordered `label_set`/`label_del`, scalar
vectorization, exact name/empty/collision behavior, executable `SQL-MQL-004`,
stable-route isolation, and bounded label projection over one public read.
`MQL-05` is shipped with implicit and explicit `default_rollup`, bounded
per-series scrape inference, jitter inflation, `max_lookback`, exact stale-bit
handling, step-window rollups, previous-sample/reset correction, name and
timestamp policy, executable `SQL-MQL-005`, stable-route isolation, and one
public packed-raw evaluation plan. `MQL-06` is shipped with complete-grid
`range_avg/min/max/sum`, pinned slot-index/missing/IEEE/name/collision
behavior, executable `SQL-MQL-006`, one bounded child evaluation,
generated-result/work limits, cancellation, and stable-route isolation.
`MQL-07` is shipped with cumulative `running_avg/min/max/sum`, pinned
missing/stale carry, slot-index, IEEE/name/collision behavior, executable
`SQL-MQL-007`, one bounded child evaluation, exact-build evidence, limits,
cancellation, and stable route isolation. `MQL-09` is shipped with
request-step-relative direct/subquery windows and resolutions, signed compound
offsets, exact float/truncation/saturation rules, collision-free zero/extrema
lowering, adaptive direct/subquery rollups, executable `SQL-MQL-009`, one
public extension read, exact-build evidence, stable-route isolation, limits,
cancellation, durability, and reopen. The remaining rows continue in their
listed order. `MQL-10` is shipped with pinned range/instant request-context
values, case/arity/unsupported-function behavior, scalar/vector composition,
executable `SQL-MQL-010`, real-extension limit/cancellation/reopen coverage,
stable-route isolation, and exact-build zero-read/narrow/wide evidence.
`MQL-11`
is already corrected to deferred because the pinned oracle rejects `min_of`
and `max_of`. `MQL-12` is shipped with pinned plural-histogram argument,
rank-label, destination, cumulative/`vmrange`, missing-`+Inf`, repair,
interpolation, computed-NaN, collision, and error behavior; executable
`SQL-MQL-012`; one shared public bucket read; stable-route isolation; bounded
work/cancellation; durability; and reopen. Exact-build narrow/wide evidence
at commit `81e5034f359de840ff7e02eaa7cbb26477562c0a` confirms identical storage
work for one and two ranks and closes the final Session 15 gate.

### Session 16: LogsQL P2 filters

Rows: `LQL-F11`, `LQL-F16`, `LQL-F17`, `LQL-F20`–`LQL-F23`,
`LQL-F25`–`LQL-F28`, `LQL-F30`, `LQL-F32`–`LQL-F34`, and
`LQL-F40`.

Exit: all applicable P2 filters pass VictoriaLogs with explicit byte/Unicode,
timezone, nested-array, field expansion, escape, and error-location behavior.

Progress: `LQL-F11` is shipped with all four case-insensitive pattern anchors,
all seven VictoriaLogs placeholders, strict one-argument grammar, rich-value textual
projection, missing/null/empty behavior, field and pipeline composition,
bounded work/cancellation, 23 pinned oracle cases, real-extension durability
and reopen coverage, and no extension primitive or misleading SQL claim.
Exact-build evidence at commit
`2f4b2c5d8d0e1623da69d41a943b3cad8e517b60` records narrow/wide message and
typed-field pattern costs. A single non-reproduced rich-block decoder failure
is retained honestly in `QSF-112`; the exact 8,192-entry stress regression now
covers 48 full reads across both readers, raw storage, reopen, and compression,
and future failed evidence runs preserve their database and server log. The
`LQL-F16` is shipped with 13 pinned exact-prefix cases, three companion exact-
grammar corrections, operator/function forms, start anchoring, rich-value
projection, empty-prefix behavior, strict errors, logical/pipeline composition,
work limits, executable `SQL-LOG-014`, and real-extension durability/reopen
coverage. Exact-build evidence at commit
`e169a9d310890f72e07f98b002e8cefea84eaeb7` records narrow/wide message and
typed-field costs with storage work unchanged. The remaining rows continue in
their listed order. `LQL-F17` is shipped with 17 pinned static-list cases,
exact rich textual membership, empty/duplicate/trailing-comma/wildcard
behavior, explicit subquery deferral, logical/pipeline composition, work
limits, cancellation, executable `SQL-LOG-015`, and real-extension durability/
reopen coverage. Exact-build evidence at commit
`e782043dc662111f4f6f68b001da64fe9b3a7f00` records narrow/wide message and
typed-field costs with storage work unchanged; direct users retain ordinary
parameterized `IN` and the public string-index hidden column without a new
extension primitive. `LQL-F20` is shipped with 11 pinned field-independent
wildcard cases across `in`, `contains_any`, and `contains_all`; strict
LQL-F21/LQL-F22/LQL-F38 boundaries; service/level alias dispatch; logical and
pipeline composition; executable `SQL-LOG-016`; and real-extension durability/
reopen coverage. Exact-build evidence at commit
`e824450234b6f3cc57faa9d342696b222b3b532b` shows byte-identical public reads
and confirms that ordinary constant-true SQL is the complete direct-user
primitive. `LQL-F21` is shipped with 16 successful and two error pinned
`contains_all` cases; exact phrase/word boundaries, static-list identity rules,
rich typed/nested projection, service/level aliases, logical/pipeline
composition, strict LQL-F38 deferral, work limits, cancellation, durability,
and reopen. Exact-build evidence at commit
`16b372ac31bd01e18dafd3aa29244577a0fcf993` records bounded message and rich-
field costs with storage work unchanged. Portable SQLite cannot reproduce the
Unicode-category phrase boundary, so this remains `ROWS`/`API` without a
misleading SQL or extension primitive. `LQL-F22` is shipped with 17 successful
and two error pinned `contains_any` cases plus failing-then-passing parser and
real-extension regressions. Its disjunction, empty-list false, empty-value
true, typed projection, alias, pipeline, strict-error, limit, cancellation,
durability, and reopen behavior use the same bounded public-row plan. Exact-
build evidence at commit `61ddbe72416f6858fb546c1fb6ace9c2f88ba3bc`
records byte-identical storage work and rejects a misleading SQL or extension
primitive. `LQL-F23` now has twenty successful and four error pinned
`json_array_contains_any` cases, failing-then-passing parser and real-extension
regressions, exact top-level primitive membership over retained typed arrays,
strict list grammar, logical/pipeline composition, limits, cancellation,
durability, and reopen. Executable `SQL-LOG-017` proves the public JSON1 form.
Exact-build evidence at commit
`075d166e962482d2be3aa7622250b13c9dd51768` records narrow/wide string and
boolean membership with byte-identical same-run public reads and closes the
row without an extension primitive. `LQL-F25` now has eleven successful and seven
error pinned `ipv4_range` cases, failing-then-passing parser and real-extension
regressions, exact retained-string parsing, inclusive address/CIDR/two-bound
ordering, strict grammar, logical/pipeline composition, work limits,
cancellation, durability, and reopen. Executable `SQL-LOG-018` proves the
bounded public JSON1 form. Exact-build evidence at commit
`9081a326cbe9a23be8b877f2cae36b11251becf5` records narrow/wide CIDR and
explicit-bound matching with byte-identical same-run public reads and closes
the row without an extension primitive. `LQL-F26` now has twelve successful
and seven error pinned `ipv6_range` cases, source-audited 16-byte and
IPv4-mapped semantics, failing-then-passing parser and real-extension
regressions, exact retained-string parsing, strict grammar, logical/pipeline
composition, work limits, cancellation, durability, and reopen. Portable
SQLite has no built-in IPv6 parser, so no SQL or extension surface is claimed;
exact-build evidence at commit
`756ec7068ca25aef82ea6b1f7d6aa6f4c45b0c97` records narrow/wide CIDR and
explicit-bound matching with byte-identical same-run public reads and closes
the row without an extension primitive. `LQL-F27` now has sixteen successful
and seven error pinned `string_range` cases, source-audited half-open byte
ordering, failing-then-passing parser and real-extension regressions, retained
rich-value projection, strict grammar, aliases, logical/pipeline composition,
work limits, cancellation, durability, and reopen. Executable `SQL-LOG-019`
proves the public retained-text foundation and documents the intentional rich-
object boundary. Exact-build evidence at commit
`de7ccf8bf53b07c9ae7e381a1c595d442028f8a4` records narrow/wide retained-
string and typed-field ranges with byte-identical same-run public reads and
closes the row without an extension primitive.
`LQL-F28` now has seventeen successful and ten error pinned `len_range`
cases, source-audited inclusive Unicode-codepoint semantics and unsigned bound
grammar, failing-then-passing parser and real-extension regressions, retained
rich-value projection, strict errors, aliases, logical/pipeline composition,
work limits, cancellation, durability, and reopen. Executable `SQL-LOG-020`
proves the public retained-text foundation with SQLite `length(TEXT)` and
documents the intentional rich-object boundary. Exact-build evidence at commit
`e624a15faf99690975515d7958428819f37aad84` now records narrow/wide retained-
string and typed-field codepoint lengths with byte-identical same-run public
reads and closes the row without an extension primitive.
`LQL-F30` now has twelve successful and twelve error pinned same-row field-
comparison cases, source-audited exact equality and math-value-or-bytewise
ordering, failing-then-passing parser and real-extension regressions, retained
rich projection with exact large-integer ordering, quoted/message/service/
nested/right-`_time` fields, strict errors, aliases, logical/pipeline
composition, work limits, cancellation, durability, and reopen. Executable
`SQL-LOG-021` proves complete retained-model equality plus the bytewise
ordering fallback and documents why language-specific math parsing remains in
the Rust API. Exact-build evidence at commit
`afd7804edb5d438a3aa400b1f358a0e287b648ae` records narrow/wide equality and
exact retained-number ordering with byte-identical public reads and closes the
row without an extension primitive or storage-contract change.
`LQL-F32` now has sixteen successful and six error pinned field-prefix cases,
source-audited any-matching-field and independent group-atom semantics,
failing-then-passing parser and real-extension regressions, literal empty and
quoted prefixes, canonical special fields, recursively dotted retained object
leaves, array/null leaf fidelity, strict wildcard-comparison errors, projected
pipeline behavior, work limits, cancellation, durability, and reopen.
Executable `SQL-LOG-022` proves a recursive public-row exact-string foundation
without collapsing duplicate log entries. Exact-build evidence at commit
`94e1cd4cd715b6ec2a86e580324cf50057943332` records narrow/wide word and typed-
value field-set searches with byte-identical public reads. It closes the row
without an extension primitive or storage-contract change.
`LQL-F33` now has fifteen successful and eight error pinned day-range cases,
failing-then-passing parser and real-extension regressions, exact open/closed
boundaries, compact and colon clock grammar, signed compound offsets,
`24:00` and minute-60 normalization, the midnight half-open full-day special
case, valid empty inverted ranges, current-row pipelines, work limits,
cancellation, durability, and reopen. Timeless deliberately makes an omitted
offset mean UTC instead of inheriting mutable process-local timezone state.
Executable `SQL-LOG-023` proves the native-unit public-row foundation. The
exact-build evidence at commit
`aa7bc3001dfb354faccf2c2c2d4f3197b9391d6d` records narrow/wide day-range
evaluation with byte-identical public reads. It closes the row without an
extension primitive or storage-contract change.
`LQL-F34` now has fifteen successful and eight error pinned week-range cases,
failing-then-passing parser and real-extension regressions, short/full
case-insensitive weekdays, exact open/closed bracket normalization, the
Sunday-through-Saturday non-wrapping interval, full-week and valid-empty edge
cases, signed compound offsets, current-row pipelines, work limits,
cancellation, durability, and reopen. Timeless deliberately makes an omitted
offset mean UTC. Executable `SQL-LOG-024` proves the native-unit public-row
foundation including Euclidean pre-epoch weekdays. Exact-build evidence at
commit `adb7f027f4a6c5e0d07d8ff3eb3b294e85277818` records narrow/wide weekday
evaluation with byte-identical public reads. It closes the row without an
extension primitive or storage-contract change.
`LQL-F40` now has fourteen successful and six error pinned source-layout cases,
failing-then-passing parser and real-extension regressions, LF/CRLF comments,
literal quoted hashes, multiline logical/pipeline composition, one optional
terminal semicolon, strict malformed tails, lexical line/column errors, request-
bounded scanning, ordinary-query no-copy behavior, work-limit reuse, durability,
and reopen. This is parser-only `API` ownership: direct SQLite/libSQL users write
ordinary SQL, so neither an SQL recipe nor an extension primitive is honest.
Exact-build evidence at commit
`c23bd23428c2a87e49d60e57ac121b8a346ce3c7` records narrow/wide commented
multiline evaluation with byte-identical public reads. It closes the row and
Session 16 without an extension primitive or storage-contract change.

### Session 17: LogsQL P2 pipelines and query behavior

Rows: `LQL-P07`, `LQL-P10`–`LQL-P16`, `LQL-P18`–`LQL-P24`,
`LQL-P28`–`LQL-P30`, `LQL-P32`–`LQL-P34`, `LQL-P36`, `LQL-P41`,
`LQL-S07`, `LQL-S09`–`LQL-S11`, and `LQL-Q03`, `LQL-Q04`.

Exit: transformations and statistics remain bounded, typed, cancellable, and
deterministic where promised. `block_stats` or `blocks_count` becomes `EXT`
only with direct-user utility and measured storage/decode savings.

`LQL-P07` now has eighteen successful and eight error pinned VictoriaLogs
cases, failing-then-passing parser and real-extension regressions, all four
case-insensitive aliases, exact/quoted/prefix/all-field grammar, ordered
composition, recursive rich-object deletion, atomic array/scalar behavior,
empty-parent and empty-row pruning, strict errors, work limits, cancellation,
durability, and reopen. Executable `SQL-LOG-025` proves the exact retained-
metadata-path foundation through public JSON1. Prefix traversal and pruning
remain bounded Rust API composition because they occur after the same public
decode and do not justify an extension primitive. Exact-build evidence at
commit `05e40bf722245817ec6d1ae6338332a762707e31` records narrow/wide
deletion with byte-identical public reads and closes the row without an
extension primitive or storage-contract change. Session 17 continues with
`LQL-P10`.

`LQL-P10` is deferred after auditing VictoriaLogs v1.52.0 source commit
`46a54c976fa3d404396050e8a5ee6c5b0320efc5` and every retained Timeless log
codec. Upstream `block_stats` reports per-field physical encoding,
dictionary/value/bloom sizes, stream identity, and filesystem part path.
Timeless rich codec 7 stores one compressed rich metadata envelope and has no
compatible dictionary, bloom, stream, or part records; aggregate
`timeless_stats('logs')` values cannot honestly substitute for those rows.
The Rust parser and real HTTP path pin an explicit unsupported-capability
response without touching storage. Reconsider only after the versioned
per-field accounting and stream/location prerequisites in `QSF-147` exist.
Session 17 continues with `LQL-P11`.

`LQL-P11` is deferred after source audit and a controlled probe of the pinned
VictoriaLogs image. Upstream `blocks_count` counts opaque non-empty
`blockResult` batches at its exact pipeline position: two selected stream
blocks return `{"blocks_count":"2"}`, an exact missing filter returns no row,
an earlier `limit 1` returns `{"blocks_count":"1"}`, and `as name` or a bare
name changes the string-valued output field. Timeless aggregate/cumulative
stats cannot reproduce that request-local result safely under concurrency,
and the bounded Rust pipeline intentionally discards private physical block
identity after rows cross the public extension. `QSF-148` records the required
request-scoped lineage contract. The parser and real-extension HTTP path pin a
422 unsupported-capability response before storage work. Session 17 continues
with `LQL-P12`.

`LQL-P12` is shipped through a new public request-owned execution report, not
through cumulative counter deltas or shadow-table access. A successful fully
consumed `timeless_logs` scan publishes local work on its SQLite connection;
`timeless_log_query_stats('logs')` consumes that table-scoped report exactly
once, while new/failed/cancelled scans clear stale state. `SQL-LOG-026` exposes
all sixteen native INTEGER counters and an executable mapping to the fourteen
VictoriaLogs string fields. The Rust LogsQL API owns strict no-argument
grammar, complete typed post-filter `RowsFound`, duration through the pipeline
position, later-pipeline composition, limits, cancellation, and envelopes.
Five successful and two error oracle cases pin the applicable semantics.
`QSF-149` records the concurrency-safe public contract; `QSF-150` records the
honest indivisible-payload mapping and the upstream scheduling-dependent
`limit` distinction. Exact-build fast-path evidence at commit
`6530dc232010e3e4169fdc9e95154c22b68f4a4d` records 3.436/25.046 ms
narrow/wide p95 versus 4.811/41.649 ms for same-run full-row controls, with
identical storage work and one 380/385-byte report response. `QSF-151` retains
the pre-optimization cost and `QSF-152` records the accepted result. No
storage format or maintenance contract changed.
Session 17 continues with `LQL-P13`.

`LQL-P13` implements VictoriaLogs-compatible `first [N] [by] (...)`, optional
`partition [by] (...)`, and optional `rank [as] field` as bounded Rust API
composition over public rich rows. Ten successful and eight error oracle
cases pin the complete applicable grammar, coercion order, current-schema
no-`by` behavior, partition-local string ranks, composition, and empty input.
`SQL-LOG-027` gives direct SQLite/libSQL users an executable bounded numeric
window-rank foundation and states where ordinary SQLite lacks VictoriaLogs'
exact integer and natural-text semantics. State, work, result, response,
cancellation, optimize, shutdown, and reopen behavior are pinned without a
new extension primitive, private-table access, or storage-contract change.
`QSF-153` records the semantic boundary. Exact-build commit
`9225cc54db38e66707cd0d04837fb656cfc778ea` measures partitioned/ranked
`first` at 3.681/44.182 ms narrow/wide p95 versus 3.153/37.107 ms for
same-run equal-cardinality time-sort controls, with byte-identical public
storage work. `QSF-154` accepts that bounded API composition cost and records
the final HWM/storage verdict.
Session 17 continues with `LQL-P14`.

`LQL-P14` reuses the bounded `first` parser/state machinery and reverses its
complete comparator only after per-field direction is applied. Ten successful
and eight error oracle cases pin default/count/by/partition/rank grammar,
exact integer and natural order, direction inversion, current-schema no-`by`
behavior, composition, and empty input. The initial inverse-order regression
found an early-return comparator path that bypassed global reversal; it was
fixed before shipment and retained as a unit regression. Executable
`SQL-LOG-028` gives direct SQLite/libSQL users the descending numeric
window-rank foundation. Work/result/response-state limits, cancellation,
optimize, shutdown, reopen, and reader reuse remain shared and bounded without
a new extension primitive or storage-contract change. `QSF-155` records the
semantic boundary. Exact-build commit
`d2bdf12e2b8d91d04cb8716c860fe4105a86428d` measures partitioned/ranked
`last` at 3.060/46.268 ms narrow/wide p95 versus 3.290/44.012 ms for
same-run `first` controls, with byte-identical public storage work. `QSF-156`
accepts the -7.0%/+5.1% endpoint variation, records the final HWM/storage
verdict, and closes the row.
Session 17 continues with `LQL-P15`.

`LQL-P15` implements VictoriaLogs-compatible `top [N] [by] fields`, optional
`hits [as] field`, and optional `rank [as] field` as bounded Rust API
aggregation over public current-pipeline rows. Eleven successful and eight error
oracle cases pin default and explicit limits, parenthesized/bare multi-field
grammar, textual missing/null/type projection, frequency/key order, collision
names including explicit `hits as hits`, case-insensitive modifiers,
composition, and empty input. Executable
`SQL-LOG-029` gives direct SQLite/libSQL users the parameterized public
single-field `GROUP BY` and rank foundation. Work/group/result/response-state
limits, cancellation, optimize, shutdown, reopen, and reader reuse are pinned
without a new extension primitive or storage-contract change. The first
evidence attempt exposed and pinned the explicit-default hits alias before
shipment. `QSF-157` records the semantic boundary. Exact-build commit
`2bb2f4dd0046574e116ae05b6d75a77cef04ef20` measures `top` at
3.385/35.948 ms narrow/wide p95 versus 3.330/38.060 ms for same-scan,
equal-cardinality time-sort controls. `QSF-158` accepts the +1.6%/-5.5% p95
variation, records the byte-identical storage and HWM verdict, and closes the
row.

Before the next row, the complete extension release gate was restored to a
Rust-only execution path. The existing `tools/query-harness` crate now owns
the binary fixtures, persistent SQLite hosts, packed decoders, rich-fidelity
drivers, crash workload generator, and dbhealth lifecycle driver previously
implemented as executable Python. All 45 `tests/cli.sh` sections pass,
including three 50,000-operation oracle seeds, five `kill -9` iterations, and
95 SQL recipes/131 statements; the focused `R1`/`R2`/`R3`/`R4`/`R8`/rich-log
correctness gate passes as well. This restoration found and fixed `QSF-159`:
the dbhealth wrapper's command argument moved from six to seven when metrics
added hidden `series_id`, preventing scheduled collection. The retained Rust
lifecycle regression proves create, reopen, manual mode, drop, legacy meta,
and sqld behavior. No CI or release automation is part of this local gate.
Session 17 continues with `LQL-P16`.

`LQL-P16` implements VictoriaLogs-compatible `uniq [by] fields`, optional
single-field `filter substring`, optional `hits`/`with hits`, and optional
`limit N` as bounded Rust API aggregation over public current-pipeline rows.
Fourteen successful and twelve error oracle cases pin parenthesized/bare
one/multi-field grammar, textual rich-value projection, empty-state
coalescing, case-sensitive filtering, collision-safe string hits, zero and
positive limits, overflow-hit reset, composition, case-insensitive keywords,
empty input, and strict tails. Upstream hash-map order and limited subset are
unspecified; Timeless deliberately selects bytewise structural keys
deterministically. Executable `SQL-LOG-030` gives direct SQLite/libSQL users
the matching parameterized single-field grouping/filter/hits foundation.
Work/group/result/response-state limits, HTTP deadline cancellation, optimize,
shutdown, reopen, and reader reuse are pinned without a new extension
primitive or storage-contract change. The first full oracle run exposed an
over-broad harness comparator edit that accidentally made ordinary row-query
order significant; the complete corpus rejected it, and a focused Rust test
now pins separate row-query and statistics ordering contracts. `QSF-160`
records the semantic boundary. Exact-build commit
`0203fa8b3e959dd5ab76a587d3e90a49961b07b5` measures `uniq` at
3.416/41.705 ms narrow/wide p95 versus 3.594/39.851 ms for same-scan,
equal-cardinality controls. `QSF-161` accepts the -5.0%/+4.7% p95 variation,
records byte-identical storage and the HWM verdict, and closes the row.
Session 17 continues with `LQL-P18`.

`LQL-P18` implements VictoriaLogs-compatible `facets` as bounded Rust API
aggregation over recursively flattened public current-pipeline rows. Eleven
successful and ten error oracle cases pin per-field limits, textual rich
values, empty exclusion, constant retention, whole-field cardinality/length
exclusion, modifier ordering/repetition/case, positive-fraction truncation,
empty input, and strict syntax. Timeless adds deterministic field-name and
equal-hit value ordering where the local pinned processor promises none.
Executable `SQL-LOG-031` gives direct SQLite/libSQL users the complete public
JSON1/window-function foundation, including native timestamp units and
pre-epoch rendering. Input/field/value/traversal/sort/output/result/response
limits, HTTP deadline cancellation, optimize, shutdown, reopen, and reader
reuse are pinned without an extension primitive or storage-contract change.
`QSF-162` records the semantic boundary. Exact-build commit
`85332944a7a9d9c2f476558e1d519eaa6aecf23e` measures `facets` at
3.239/44.087 ms narrow/wide p95 versus 4.464/41.116 ms for byte-identical
same-scan controls. `QSF-163` accepts the -27.4%/+7.2% p95 variation, records
unchanged four-block storage and the HWM verdict, and closes the row. Session
17 continues with `LQL-P19`.

`LQL-P19` implements VictoriaLogs-compatible `coalesce` as a bounded Rust API
row transform over public current-pipeline rows. Eleven successful and eleven
error oracle cases pin ordered exact/all/prefix expansion, duplicate
suppression, first-nonempty textual selection, missing/null/empty behavior,
typed values, recursively flattened objects, atomic arrays, defaults,
destination replacement, trailing commas, quoting, and strict syntax.
Timeless deliberately retains an explicit empty destination and rejects a
nested destination beneath an existing scalar with actionable HTTP 422
`field_conflict`, preserving richer stored rows. Executable `SQL-LOG-032`
gives direct SQLite/libSQL users the exact-field `CASE`/`NULLIF`/`COALESCE`
foundation. Work/path/state/result/response limits, HTTP deadline
cancellation, optimize, shutdown, reopen, and reader reuse are pinned without
an extension primitive or storage-contract change. `QSF-164` records the
semantic boundary and corrected pre-existing HTTP/test-diagnostic gap.
Exact-build commit `1b36c239a58d96d7ce8348064dfe03d2ac58c470`
measures `coalesce` at 3.570/39.597 ms narrow/wide p95 versus 3.329/38.277
ms for byte-identical same-scan controls. `QSF-165` accepts the +7.2%/+3.4%
p95 row-transform cost, records unchanged four-block storage and the HWM
verdict, and closes the row. Session 17 continues with `LQL-P20`.

`LQL-P20` is shipped with strict case-insensitive `copy`/`cp` grammar,
optional `as`, sequential comma-separated pairs, exact/all/prefix sources and
destinations, typed exact cloning, deterministic recursively flattened
wildcard snapshots, prefix substitution, missing/object-parent compatibility,
hard work/state/result/response limits, cancellation, actionable rich-object
destination conflicts, optimize/reopen coverage, and executable public
`SQL-LOG-033`. Twenty successful and eight error cases pass the pinned
VictoriaLogs oracle. An added empty-suffix regression found and corrected an
initial mistaken `_msg` canonicalization: wildcard substitution may create a
literal empty field, while exact quoted `""` remains the message alias.
`QSF-166` records the semantic boundary and regression. Exact-build commit
`aac910be8a13bdc7f96be48175602cef566cffa3` measures `copy` at
3.229/46.025 ms narrow/wide p95 versus 3.659/41.958 ms for byte-identical
same-scan controls. `QSF-167` accepts the -11.8%/+9.7% mixed row-transform
variation, records unchanged four-block storage and the HWM verdict, and
closes the row. Session 17 continues with `LQL-P21`.

`LQL-P21` has strict case-insensitive `rename`/`mv` grammar, optional `as`,
sequential comma-separated pairs, exact/all/prefix sources and destinations,
typed exact moves, deterministic recursively flattened wildcard snapshots,
removal-before-insertion, prefix substitution, missing/object-parent
compatibility, hard work/state/result/response limits, cancellation,
actionable rich-object destination conflicts, optimize/reopen coverage, and
executable public `SQL-LOG-034`. Twenty successful and nine error cases pass
the complete 616-case pinned VictoriaLogs oracle. The retained-model policy
prunes empty response parents while keeping durable source rows immutable;
object parents and rich empty objects that do not exist as flattened columns
remain intact. `QSF-168` records the semantic boundary. Exact-build commit
`0431fbc6b548e4a0153ff9c4e5997dfa0baf5968` measures `rename` at
3.770/43.696 ms narrow/wide p95 versus 3.553/36.673 ms for byte-identical
same-scan controls. `QSF-169` accepts the +6.1%/+19.1% bounded
move/prune/rebuild cost, records unchanged four-block storage and the HWM
verdict, and closes the row. Session 17 continues with `LQL-P22`.

`LQL-P22` now has strict case-insensitive `format` grammar, quoted/unquoted
patterns, optional and empty `if (...)`, default/exact destinations,
HTML-decoded literals, exact/empty/missing/rich placeholders, all applicable
pinned transformations and raw fallbacks, preservation modifiers, exact
checked-decimal Unix-time inference, hard work/state/result/response limits,
cancellation, rich-object conflict errors, optimize/reopen coverage, and
executable public `SQL-LOG-035`. Twenty-four successful and nine error cases
pass the complete 649-case pinned VictoriaLogs oracle. Timeless preserves
typed rich inputs and explicit empty destinations where the flattened
upstream stream response omits them. `QSF-170` records the semantic boundary
and the corrected scientific-time/HTTP-envelope regressions. Exact-build
commit `49761cdc2d980ffc2110a9e3483994b8a4d9b47b` measures `format` at
3.297/39.353 ms narrow/wide p95 versus 3.090/35.941 ms for byte-identical
same-scan controls. `QSF-171` accepts the +6.7%/+9.5% bounded transform cost,
records unchanged four-block storage and the HWM verdict, and closes the row.
Session 17 continues with `LQL-P23`.

`LQL-P23` now has strict case-insensitive `math`/`eval` grammar, sequential
comma-separated expressions, optional/canonical destinations, unary and
left-associative binary precedence, every applicable arithmetic/bitwise/
default operator and function, the pinned number/duration/byte/time/IP
coercion chain, fixed finite/nonfinite rendering, deterministic unsigned
bitwise conversion, hard AST/work/state/result/response limits,
cancellation, rich-object conflict errors, optimize/reopen coverage, and
executable public `SQL-LOG-036`. Twenty-two successful and nineteen error
cases pass the complete 690-case pinned VictoriaLogs oracle. Timeless retains
typed rich sources and treats nonnumeric retained values as NaN rather than
flattening or mutating storage. `QSF-172` records the semantic boundary and
the corrected unsigned-cast/HTTP-envelope/AST-stack regressions. Exact-build
commit `84c1a77352aa33fc32139efe8c814e9dcadcff3c` measures 3.357/39.127 ms
narrow/wide p95 versus 3.292/37.655 ms for byte-identical same-scan controls.
`QSF-173` accepts the +2.0%/+3.9% bounded expression cost, records unchanged
four-block storage and the HWM verdict, and closes `LQL-P23`. Session 17
continues with `LQL-P24`; its remaining declared P2 rows must not be skipped
before Session 18 begins.

`LQL-P24` now has strict case-insensitive `len` grammar, optional parentheses
and `as`, default/empty-alias `_msg`, exact quoted/dotted/current-row fields,
sequential destinations, UTF-8 byte counting, pinned textual/compact-JSON
projection, flattened object-parent behavior, hard work/state/result/response
limits, cancellation, rich-object conflict errors, optimize/reopen coverage,
and executable public `SQL-LOG-037`. Thirteen successful and eight error cases
pass the complete 711-case pinned VictoriaLogs oracle. Timeless preserves rich
typed sources while matching upstream's absent object-parent query view.
`QSF-174` records the semantic boundary. Exact-build performance/HWM evidence
at commit `64ec776668de88fdc3cf8bd6649ba7de2ad47b6e` measures 3.785/40.724 ms
narrow/wide p95 versus 3.620/36.622 ms for byte-identical same-scan controls.
`QSF-175` accepts the +4.6%/+11.2% bounded row-transform cost, records
unchanged four-block storage and the HWM verdict, and closes `LQL-P24`.
Session 17 continues with `LQL-P28`.

`LQL-P28` now has strict argumentless case-insensitive `drop_empty_fields`
grammar, typed null/empty-string removal, recursive rich-object and all-empty-
row pruning, atomic-array retention, current-row composition, a 128-level
nesting ceiling, hard traversal/result/response limits, cancellation,
optimize/reopen coverage, and executable public `SQL-LOG-038`. Two row-query,
five exact-result, and four error cases pass the complete 722-case pinned
VictoriaLogs oracle. Timeless preserves zero, false, arrays, and every other
rich source type while matching the upstream flattened empty-field set.
`QSF-176` records the semantic boundary and the decision to keep dynamic
traversal in the Rust API. Exact-build commit
`88b26b01194a2d863107406f6aba099380683dd7` measures 4.542/38.151 ms
narrow/wide p95 versus 6.994/35.779 ms for byte-identical same-scan controls.
`QSF-177` accepts the -35.1%/+6.6% bounded traversal variation, records
unchanged four-block storage and the HWM verdict, and closes `LQL-P28`.
Session 17 continues with `LQL-P29`; its remaining declared P2 rows must not
be skipped before Session 18 begins.

`LQL-P29` now has strict case-insensitive literal `replace` grammar, optional
current-row `if (...)`, default `_msg` and exact quoted/dotted targets,
zero/unbounded and first-`N` replacement, typed textual projection, native
no-op preservation, hard work/state/result/response limits, cancellation,
optimize/reopen coverage, and executable public `SQL-LOG-039`. Eleven exact
result and seven error cases pass the complete 740-case pinned VictoriaLogs
oracle. Timeless keeps rich missing/null/object/no-match values intact while
actual replacements become query-row strings. `QSF-178` records that retained-
model boundary and the deliberate strict rejection of upstream's ambiguous
attached `replace(foo,bar)` whole-query interpretation. Exact-build commit
`2288307503c1110d9fa5ac9056a35d965afb61a4` measures 3.520/37.711 ms
narrow/wide p95 versus 3.908/36.882 ms for byte-identical same-scan controls.
`QSF-179` accepts the -9.9%/+2.2% bounded literal-transform variation,
records unchanged four-block storage and the HWM verdict, and closes
`LQL-P29`. Session 17 continues with `LQL-P30`; its remaining declared P2
rows must not be skipped before Session 18 begins.

`LQL-P30` now has strict case-insensitive `replace_regexp` grammar, optional
current-row `if (...)`, default `_msg` and exact quoted/dotted targets,
zero/unbounded and first-`N` replacement, RE2-family dot/flag/anchor/UTF-8
boundary behavior, Go-compatible numbered/named/full-match/dollar template
expansion, typed textual projection, native no-op preservation, hard compiled-
pattern/work/state/result/response limits, cancellation, optimize/reopen
coverage, and immutable rich sources. Fifteen exact-result and ten error cases
pass the complete 765-case pinned VictoriaLogs oracle. `QSF-180` records the
retained-model boundary and the absence of a portable SQLite/public-extension
RE2 capture-replacement scalar. Accordingly, this row has no dishonest SQL
recipe and does not justify a language-specific extension primitive. Exact-
build commit `8c7c27d4f98f7bfd58c0b27764c7f48f2b2ff425` measures
3.442/40.628 ms narrow/wide p95 versus 3.391/35.822 ms for byte-identical
same-scan controls. `QSF-181` accepts the +1.5%/+13.4% bounded regex/capture-
expansion cost, records unchanged four-block storage and the HWM verdict, and
closes `LQL-P30`. Session 17 continues with its remaining declared P2 rows;
none may be skipped before Session 18 begins.

`LQL-P32` now has strict case-insensitive literal `extract` grammar; required
named and optional anonymous captures; HTML-decoded delimiters; nonempty-
prefix search and empty-prefix anchoring; bounded Go double/single/raw quoted
decoding plus `plain:`; default `_msg` and exact quoted/dotted sources;
conditions; default, keep-original, and skip-empty write policies; explicit
empty and native rich-value fidelity; hard work/state/result/response limits;
cancellation; optimize/reopen coverage; immutable durable sources; and
executable public `SQL-LOG-040`. Twenty-three exact-result and fourteen error
cases pass the complete 802-case pinned VictoriaLogs oracle. `QSF-182`
records the retained-model and parser-diagnostic boundaries. Exact-build
commit `0656dcf0c752729a2dfa755d322e6de281a8b007` measures 3.269/39.052 ms
narrow/wide p95 versus 3.201/33.944 ms for byte-identical same-scan controls.
`QSF-183` accepts the +2.1%/+15.0% bounded literal extraction/current-row
write cost, records unchanged four-block storage and the HWM verdict, and
closes `LQL-P32`. Session 17 continues with its remaining declared P2 rows;
none may be skipped before Session 18 begins.

`LQL-P33` now has a strict case-insensitive `extract_regexp`
parser; request-once bounded RE2-family compilation; named-capture and
first-match semantics; dot-newline default and inline flags; exact `_msg` or
`from` sources; conditions; default, keep-original, and skip-empty writes;
typed textual projection; sequential current-row composition; explicit rich-
object conflicts; work/state/result/response limits; deadline cancellation;
optimize/reopen coverage; and immutable durable sources. Nineteen exact-result
and fourteen error cases pass the complete 835-case pinned VictoriaLogs
oracle. `QSF-184` records why core SQLite and the public extension have no
honest portable RE2 named-capture SQL recipe and why this stays bounded Rust
API work over public rows. Exact-build commit
`5bb72dafd2d93209c686e89f8ed03361b06ed425` measures 3.154/35.517 ms
narrow/wide p95 versus 3.922/33.808 ms for byte-identical same-scan controls.
`QSF-185` accepts the -19.6%/+5.1% bounded capture/current-row-write
variation, records unchanged four-block storage and the HWM verdict, and
closes `LQL-P33`. Session 17 continues with its remaining declared P2 rows;
none may be skipped before Session 18 begins.

`LQL-P34` now has strict case-insensitive `pack_json` grammar; exact, prefix,
empty-list, and all-field snapshots; deterministic idempotent selector union;
native rich JSON preservation; default and exact destinations; sequential
composition; explicit conflicts; work/state/result/response limits; deadline
cancellation; optimize/reopen coverage; and immutable durable sources. Eleven
exact-result and four error cases pass the complete 850-case pinned
VictoriaLogs oracle with an explicit richer retained-model policy. Executable
`SQL-LOG-041` gives direct users the fixed exact-path JSON1 foundation.
Exact-build commit `1e20d8f49e440e27f0f12930e71778277bf92f06`
measures 3.146/37.921 ms narrow/wide p95 versus 3.098/35.717 ms for
same-scan controls. `QSF-187` accepts the +1.5%/+6.2% bounded selection/write
variation, records unchanged four-block storage and the HWM verdict, and
closes `LQL-P34`.

`LQL-P36` now has strict case-insensitive `unpack_json` grammar; conditional,
exact, prefix, empty-list, all-field, preservation, and prefix selection;
native-object and JSON-text sources; deterministic source snapshots; rich
type/nesting/literal-key fidelity; pinned malformed-object and bare-`NaN`
behavior; sequential composition; explicit conflicts; work/state/result/
response limits; deadline cancellation; optimize/reopen coverage; and
immutable durable sources. Seventeen exact-result/statistics and eight error
cases pass the complete 875-case pinned VictoriaLogs oracle with an explicit
richer retained-model policy. Executable `SQL-LOG-042` gives direct users the
fixed-path JSON1 foundation. Exact-build commit
`035c76baf3b684e29714bd1a2b8f955864fa752a` measures 3.151/40.062 ms
narrow/wide p95 versus 3.763/38.694 ms for equal-output controls. `QSF-189`
accepts the -16.3%/+3.5% bounded parse/select/write variation, retains the
65.292 ms wide p99 honestly, records unchanged four-block storage and the HWM
verdict, and closes `LQL-P36`. Session 17 continues with `LQL-P41`; its
remaining declared P2 rows must not be skipped before Session 18 begins.

`LQL-P41` now has strict case-insensitive `json_array_len` grammar; exact
parenthesized/bare/quoted/dotted sources and destinations; native-array and
JSON-array-text counts; pinned bare-`NaN` and nonarray behavior; deterministic
source snapshots; sequential textual writes; explicit conflicts; work/state/
result/response limits; cancellation; optimize/reopen coverage; and immutable
durable rich sources. Twelve exact-result/statistics and ten error cases pass
the complete 897-case pinned VictoriaLogs oracle. Executable `SQL-LOG-043`
gives direct users the fixed-path JSON1 foundation. Exact-build commit
`745eb01b059b3c0dd6b7b62d152bd23a423f0f00` measures 3.558/41.563 ms
narrow/wide p95 versus 3.454/40.607 ms for equal-output controls. `QSF-191`
accepts the +3.0%/+2.4% bounded native-array count/current-row-write
variation, records unchanged four-block storage and the HWM verdict, and
closes `LQL-P41`. Session 17 continues with `LQL-S07`; none of its remaining
declared P2 rows may be skipped before Session 18 begins.

`LQL-S07` now has strict case-insensitive `quantile`/`stddev` grammar;
required bounded phi; exact, prefix, and all-current-field selection;
VictoriaLogs mixed textual natural ordering and upper-step ranks; native-
number-only population Welford deviation; explicit retained empty/null/type
policy; deterministic work/state limits instead of random sampling;
cancellation; optimize/reopen durability; and reader recovery. Five exact
statistics cases and five errors pass the complete 907-case pinned
VictoriaLogs oracle with documented retained-model differences. Executable
`SQL-LOG-044` gives direct users the finite-native-number foundation.
Exact-build commit `f8d5b81bc16e2dadaf7e764273eb82cbfc0de272` measures
3.636/38.585 ms quantile p95 versus 3.766/38.121 ms for median controls and
3.517/37.372 ms stddev p95 versus 3.622/37.426 ms for average controls.
`QSF-193` accepts the -3.5%/+1.2% and -2.9%/-0.1% bounded variation, records
unchanged four-block storage and the HWM verdict, and closes `LQL-S07`.
Session 17 continues with `LQL-S09`; its remaining declared P2 rows must not
be skipped before Session 18 begins.

`LQL-S09` now has strict case-insensitive `sum_len` grammar; exact, prefix,
and all-current-field selection; UTF-8/compact-JSON byte semantics; explicit
missing/null/type behavior; a checked constant `u64` aggregate; work/result/
response limits; cancellation; optimize/reopen durability; reader recovery;
and immutable durable sources. Five exact statistics cases and five errors
pass the complete 917-case pinned VictoriaLogs oracle with the documented
native-integer retained-model response. Executable `SQL-LOG-045` gives direct
users the exact-metadata-path JSON1 foundation. Exact-build commit
`411de6d35fdf91f383fac7a2fc32e4a8b22dd35a` measures 3.460/35.691 ms
narrow/wide p95 versus 3.719/37.760 ms for numeric-`sum` controls. `QSF-195`
accepts the 7.0%/5.5% lower bounded reduction tails, records unchanged four-
block storage and the HWM verdict, and closes `LQL-S09`. Session 17 continues
with `LQL-S10`; its remaining declared P2 rows must not be skipped before
Session 18 begins.

`LQL-S10` now has strict case-insensitive `any`, `field_min`, and `field_max`
grammar; deterministic first-nonempty selection; the complete VictoriaLogs
text comparator for companion extrema; first-tie behavior; native rich result
fidelity; explicit missing/null/empty handling; bounded traversal and retained
state; cancellation; immutable source rows; and optimize/reopen durability.
Five exact statistics cases and six errors pass the complete 928-case pinned
VictoriaLogs oracle. The upstream `any` result is physically arbitrary, so
the oracle pins selection only for a single candidate and Timeless documents
its deterministic strengthening. Executable `SQL-LOG-046` gives direct users
deterministic exact-path and finite-number foundations. Exact-build commit
`9a0303ff2f9820fb5a20da3686c30fdb33595d7c` measures 3.293/33.764 ms
narrow/wide `any` p95 versus 4.476/37.518 ms for equal-output controls and
3.306/36.738 ms companion-extrema p95 versus 3.704/38.270 ms for controls.
`QSF-197` accepts the 26.4%/10.0% and 10.7%/4.0% lower tails, records
identical public reads, unchanged four-block storage, and the HWM verdict, and
closes `LQL-S10`. Session 17 continues with `LQL-S11`; its remaining declared
P2 rows must not be skipped before Session 18 begins.

`LQL-S11` now has strict case-insensitive `row_any`, `row_min`, and `row_max`
grammar; upstream `as` and implicit aliases; exact/flattened-prefix/all-current
result selection; deterministic first qualifying `row_any`; the complete
VictoriaLogs text comparator and strict first ties for extrema; native nested
JSON fidelity; explicit missing/null/empty behavior; bounded recursive prefix
traversal and retained state; cancellation; immutable source rows; and
optimize/reopen durability. Six exact statistics cases and seven errors pass
the complete 941-case pinned VictoriaLogs oracle. Executable `SQL-LOG-047`
gives direct users deterministic fixed-path and finite-native-number row
foundations without a new extension primitive. Exact-build performance,
storage work, HWM, and the final row disposition remain required before
`LQL-S11` closes and Session 18 begins.

Exact-build commit `74a92f1b6ae927695fbb39d80303482966218e10` closes
that final gate. `row_any` p95 is 3.097/37.829 ms narrow/wide versus
3.660/37.888 ms for same-scan scalar controls; rich row extrema p95 is
3.219/39.790 ms versus 3.429/36.227 ms for scalar companion controls. `QSF-199`
accepts the 15.4%/0.2% lower and 6.1% lower/9.8% higher tails, records the
larger rich responses, byte-identical public reads, unchanged four-block
storage, and HWM verdict, and closes `LQL-S11`. Session 17's declared P2 rows
are complete; the sequential roadmap continues with Session 18.

### Session 18: applicable LogsQL P3

Rows: `LQL-F37`, `LQL-F38`, `LQL-F41`, `LQL-P17`, `LQL-P25`–`LQL-P27`,
`LQL-P31`, `LQL-P35`, `LQL-P37`–`LQL-P40`, `LQL-P42`–`LQL-P48`,
`LQL-S12`–`LQL-S15`, and `LQL-Q05`, `LQL-Q06`.

Exit: each row is individually shipped, classified as higher-library work, or
deferred with a concrete prerequisite. Stateful and multi-query constructs
prove strict memory/cardinality limits; partial-response policy is either
complete and unmistakable or rejected explicitly before storage.

Session 18's `LQL-P17` row is closed.

`LQL-P17` implements VictoriaLogs-compatible `sample N` in the Rust logs API.
The parser accepts exactly one positive unsigned base-zero/byte/duration/
infinite value and rejects malformed syntax before storage work. The evaluator
uses request-local exponentially distributed gaps, compacts retained rows in
place, preserves rich values and input order, supports first-stage
pre-materialization and later current-row composition, and observes the
existing work/result/response/deadline/cancellation limits. Executable
`SQL-LOG-049` gives direct SQLite/libSQL users the bounded public-row `1/N`
Bernoulli equivalent. Because neither implementation can avoid the required
public block scan and decode, no extension primitive or storage change is
warranted.

Exact-build commit `930a1f1c3dd9e4b016456b24c685bf25480bab53` closes
the row. `sample 4` p95 is 3.657/26.060 ms narrow/wide versus
3.307/33.206 ms for exact `sample 1`; every pair performs byte-identical public
work. The evidence harness rejected and pinned the first invalid native-count
control before recapture. `QSF-208` accepts the 10.6% higher narrow endpoint
tail and records the 21.5% lower wide tail, unchanged storage, and HWM verdict.
The real-extension grammar/fidelity/limit/cancellation/optimize/flush/reopen
regression, all 1,015 pinned VictoriaLogs cases, executable `SQL-LOG-049`,
documentation contracts, and complete local gates pass.

Session 18's `LQL-P25` row is closed as an API-owned `hash` pipe. Pinned
VictoriaLogs source and live probes establish seed-zero xxHash64 masked to 53
bits, decimal-string output, optional parentheses/aliases, current-row
sequential behavior, and flattened textual field projection. Ten exact and
eight error cases expand the immutable live oracle to 1,033 cases. The Rust
evaluator streams compact arrays through the hasher, bounds traversal/state,
preserves rich sources, and changes no extension or storage contract. Core
SQLite/libSQL has no portable exact xxHash64 expression, so the SQL cookbook
records an explicit no-recipe disposition rather than inventing an extension
scalar without measured avoidable storage work.

Exact-build commit `36e269fcb875262827f1f6cd49e9fc2d8ae46b3b` measures
3.206/3.455/3.566 ms narrow and 34.628/36.785/37.534 ms wide p50/p95/p99.
Same-public-work copy controls measure 3.168/3.481/3.922 and
35.594/36.223/36.263 ms. Every pair performs identical public reads and
decode: one/four candidate blocks, 1,024/8,192 decoded entries, and
235,778/1,914,055 payload bytes per query. Decimal hashes increase the
64-row response by 502/506 bytes. `QSF-210` accepts the bounded API cost and
records that no extension primitive or storage-contract change is justified.
The real-extension regression, all 1,033 pinned VictoriaLogs cases, explicit
no-SQL disposition, documentation contracts, and complete local gates pass.

Session 18's `LQL-P26` row is closed as an API-owned `collapse_nums` pipe.
Pinned VictoriaLogs source and the complete 1,055-case live oracle establish
optional condition, exact target, terminal prettify, token-boundary,
hexadecimal, underscore/version/duration, Unicode-delimiter, UUID/IPv4/time/
date/datetime, fractional-second, timezone, typed textual projection,
sequential-composition, and strict-error behavior. The bounded Rust evaluator
preserves native values on no-op projections and changes no extension or
storage contract. Core SQLite/libSQL has no portable equivalent tokenizer, so
the SQL cookbook records an explicit no-recipe disposition. Focused parser,
evaluator, and real-extension ingest/flush/optimize/shutdown/reopen regressions
pass.

Exact-build commit `a6047ffe8537188152c882e49b50b98ced7ceced` measures
2.975/3.135/3.175 ms narrow and 33.889/34.525/34.728 ms wide p50/p95/p99.
Same-public-work format controls measure 3.026/3.143/3.414 and
33.720/36.735/42.843 ms. Every pair returns the same 64 rows and 1,536 bytes
while reading the same one/four candidate blocks, decoding 1,024/8,192
entries, and transferring 235,778/1,914,055 extension payload bytes per query.
`QSF-212` accepts the -0.3%/-6.0% p95 variation, unchanged storage, and HWM
verdict without adding an extension primitive. The full 1,055-case oracle,
documentation contracts, real-extension/API suites, SQL cookbook, local
formatting, lint, crash/transaction, and lifecycle gates pass.

Session 18's `LQL-P27` row is closed as an API-owned `decolorize` pipe over
the public logs surface. Pinned VictoriaLogs
source and the complete 1,069-case live oracle establish default `_msg`, one
exact quoted/dotted field, strict grammar, exact CSI parameter/intermediate/
optional-final byte classes, incomplete-sequence removal, invalid-final
preservation, unchanged OSC/DCS, native no-op fidelity, and sequential
composition. The bounded Rust evaluator changes no durable storage or
extension contract. Executable `SQL-LOG-050` gives direct SQLite/libSQL users
the byte-exact recursive-BLOB-CTE foundation over bounded public rows. Focused
parser, evaluator, and real-extension ingest/flush/optimize/shutdown/reopen
regressions pass.

Exact-build commit `6971681946d9e445ebc13eef5fcd9b188e23a8e9` measures
2.969/3.101/3.405 ms narrow and 34.973/36.385/39.133 ms wide p50/p95/p99.
Identical-output format controls measure 3.007/3.169/3.756 and
33.785/34.872/35.393 ms. Every pair returns the same 64 rows and 1,536 bytes
while reading the same one/four candidate blocks, decoding 1,024/8,192
entries, and transferring 235,778/1,914,055 extension payload bytes per query.
`QSF-214` accepts the -2.2%/+4.3% p95 variation and +1.4%/+3.7%
request-attributed mean as bounded current-row construction/scanning work.
The full 1,069-case oracle, executable SQL, documentation contracts,
real-extension/API suites, formatting, lint, crash/transaction, and lifecycle
gates pass.

Session 18's `LQL-P31` row is closed as an API-owned `split` pipe over the
public logs surface. Pinned VictoriaLogs source and the complete 1,091-case
live oracle establish default `_msg`, optional `from`/`as` and shorthand,
strict exact-field grammar, literal non-overlapping separators, preserved
empty pieces, Unicode-scalar empty-separator behavior, flattened textual
projection, exact JSON-array-string escaping, nested destinations, and
sequential composition. The bounded Rust evaluator changes no durable storage
or extension contract. Executable `SQL-LOG-051` gives direct SQLite/libSQL
users a recursive-CTE/JSON1 foundation over bounded public rows. Focused
parser, evaluator, and real-extension ingest/flush/optimize/shutdown/reopen
regressions pass.

Exact-build commit `e80b1afb9d7f0cfc559f2db0338575f1f8caa4e1` measures
3.219/3.481/4.063 ms narrow and 37.529/38.655/40.113 ms wide p50/p95/p99.
Identical-output controls measure 3.078/4.786/4.878 and
37.964/40.047/40.151 ms. Every pair returns the same 64 rows and 1,984 bytes
while reading the same one/four candidate blocks, decoding 1,024/8,192
entries, and transferring 235,778/1,914,055 extension payload bytes per query.
`QSF-216` retains the 27.3%/3.5% lower p95 alongside +3.1%/-1.4%
request-attributed means as bounded whole-run/API variation, not storage
pushdown. The full 1,091-case oracle, executable SQL, documentation contracts,
real-extension/API suites, formatting, lint, crash/transaction, and lifecycle
gates pass.

Session 18's `LQL-P35` row is closed as an API-owned `pack_logfmt` pipe over
the public logs surface. Pinned VictoriaLogs source and the complete
1,111-case live oracle establish optional exact/prefix/all selectors, default
and explicit destinations, source snapshots, current-column ordering,
overlapping-selector duplication, textual projection, conditional quoting,
and strict errors. Timeless deliberately selects a deterministic idempotent
union, recursively flattens retained objects to dotted leaves, keeps arrays
atomic, and preserves explicit empty-state visibility. The bounded Rust
evaluator changes no extension or durable storage contract. Executable
`SQL-LOG-052` gives direct SQLite/libSQL users the fixed exact-path
core-SQL/JSON1 foundation.

Exact-build commit `fd723df39e72500f6e80126131f4e5a9bf0763a8` measures
3.459/3.805/4.335 ms narrow and 37.975/39.144/42.387 ms wide p50/p95/p99.
Identical-output controls measure 3.506/3.769/3.924 and
34.373/38.212/38.572 ms. Every pair returns the same 64 rows and 2,048 bytes
while reading the same one/four candidate blocks, decoding 1,024/8,192
entries, and transferring 235,778/1,914,055 extension payload bytes per query.
`QSF-218` accepts the +1.0%/+2.4% p95 and -0.2%/+8.2% request-attributed
mean as bounded current-row selection and encoding. The full oracle,
executable SQL, documentation contracts, real-extension/API suites,
formatting, lint, crash/transaction, and lifecycle gates pass.

Session 18's `LQL-P37` semantic implementation is complete as an API-owned
`unpack_logfmt` pipe over public `logs` rows. Pinned VictoriaLogs source and
the complete 1,134-case live oracle establish the shared unpack grammar,
unquoted and double/single/backtick quote decoding, Go escapes, malformed
quote fallback, exact/prefix/all selection, missing exact empties, source
snapshots, duplicate handling, preservation modes, and strict errors.
Timeless deliberately reconstructs dotted decoded names into its retained
nested metadata and keeps every decoded value textual. The parser and
evaluator are bounded and cancellable; durable storage and the public
extension remain unchanged. Executable `SQL-LOG-053` gives direct
SQLite/libSQL users the fixed-key, well-formed-unquoted recursive-SQL
foundation. Exact-build commit `66167687b3b4bd464e67f086554914fd468bb4c5`
measures 3.355/3.619/4.494 ms narrow and 41.166/42.437/43.357 ms wide
p50/p95/p99 versus 3.328/3.573/4.213 and 38.087/41.659/44.310 ms for
identical-output controls. `QSF-220` accepts the +1.3%/+1.9% p95 after
byte-identical public storage work. Storage remains four raw blocks; all
8,192 entries complete durably; queues and in-flight cancellation are zero;
and the complete root/server, 64-test logs real-extension, 90-test metrics
real-extension, 1,134-case oracle, 45-section CLI, six-section focused
correctness, dbhealth, SQL, documentation-contract, formatting, and lint gates
close the row.

Session 18's `LQL-P38` semantic implementation is complete as an API-owned
`unpack_syslog` pipe over public `logs` rows. Pinned VictoriaLogs source and
the complete 1,155-case live oracle establish strict grammar, optional PRI,
facility/severity mapping, partial invalid-input behavior, RFC3164 and
RFC5424 headers, structured data, CEF, CEE, conditions, source snapshots,
prefixes, preservation, and errors. Direct source-parity regressions also pin
evaluation-year-dependent Go leap-day normalization. Timeless deliberately
reconstructs dotted decoded names in retained nested metadata while keeping
all decoded values textual. The parser/evaluator and query-backed conditions
are bounded and cancellable; durable storage and the public extension remain
unchanged. Executable `SQL-LOG-054` gives direct SQLite/libSQL users the
fixed RFC5424 header foundation when structured data is `-`; complete syslog
and LogsQL semantics remain Rust API composition after the same public scan.
`QSF-221` records the semantic/storage boundary and `QSF-222` records the
adjacent query-backed unpack-condition regression. `QSF-223` preserves the
default-work-limit failure when all 8,192 rows expand before a terminal limit.
Exact-build `f43af178ba4a7d3208eb2c20d907abd7ae6b3ba5` instead retains the
complete one-/four-block public scan, limits deterministically to 64 rows, and
measures RFC5424 parsing at 3.581/38.081 ms narrow/wide p95 versus
3.439/38.026 ms identical-output controls. `QSF-224` accepts the
+4.1%/+0.1% p95 after byte-identical public storage work. Storage remains
four raw blocks; all 8,192 entries complete durably; queues and in-flight
cancellation are zero. The complete root/server, 65-test logs real-extension,
90-test metrics real-extension, 1,155-case live oracle, 45-section CLI,
six-section focused correctness, dbhealth, 120-recipe/158-statement SQL,
32-test documentation-harness, formatting, and warnings-denied lint gates
close the shipped row.

Session 18's `LQL-P39` semantic implementation is complete as an API-owned
`unpack_words` pipe over public `logs` rows. Pinned VictoriaLogs source and
the complete 1,175-case live oracle establish strict source/destination
grammar, source snapshots, exact Unicode Letter/Decimal_Number/underscore
tokens, first-seen duplicate removal, missing/empty/numeric/array projection,
and errors. Timeless preserves nested typed metadata and treats retained
object parents as missing instead of flattening them away. The parser and
evaluator are bounded and cancellable; durable storage and the public
extension remain unchanged. Core SQLite cannot express the exact Unicode
categories portably, so the row's inferred `SQL` foundation is corrected to
`none`; the unchanged public scan provides no evidence for an extension
scalar. `QSF-225` records the semantic/storage boundary, and `QSF-226` records
the adjacent shared-boundary correction from Rust's broader
`is_alphanumeric()` to the pinned category rule. Exact release build
`d3c0884cccf4896864b59ad447f7f2f0c59d6040` measures 3.740/38.564 ms
narrow/wide p95 versus 3.493/35.662 ms equal-storage-work copy controls.
`QSF-227` accepts the +7.0%/+8.1% p95, +0.8%/+4.5% request-attributed API
mean, and 640-byte response expansion after identical public block, decode,
payload, sort, limit, and row work. All 8,192 entries complete durably; storage
remains four raw blocks; queues and in-flight cancellation are zero. The
complete local gates close the shipped row without an extension primitive or
storage-contract change.

Session 18's `LQL-P40` implementation is complete as API-owned
`json_array_concat` over public `logs` rows with canonical-array
`SQL-LOG-055`. Pinned VictoriaLogs source plus the complete 1,199-case live
oracle establish optional delimiters, default/bare/explicit fields, source
snapshots, decoded strings, compact nonstrings, raw numeric spelling, object
order, nested escape spelling, bare `NaN`, empty/nonarray behavior, and strict
errors. The upstream processor assigns empty text while its streaming JSON
encoder omits that column; Timeless explicitly returns the empty value to
preserve its richer missing/null/empty model.

The Rust evaluator traverses retained native arrays directly and uses a
bounded validating scanner for JSON text rather than a lossy value round trip.
Work/state/result/response/deadline limits, cancellation/reuse, rich dotted
destinations, actionable conflicts, immutable storage, optimize, shutdown,
and reopen are pinned. Exact release build
`51beb50eb359a2ac2f9d03669547dcff12f8d774` measures 3.872/35.216 ms
narrow/wide p95 versus 3.418/40.200 ms equal-output controls. `QSF-229`
accepts the +13.3%/-12.4% p95 and -1.1%/-8.4% request-attributed API mean
after identical public storage work and identical 64-row, 1,536-byte
responses. All 8,192 entries complete durably; storage remains four raw
blocks; queues and in-flight cancellation are zero. No extension primitive,
private table, storage contract, or authoritative batching behavior changed.

Session 18's `LQL-P42` semantic implementation is complete as API-owned
`unroll` over public `logs` rows with canonical single-array `SQL-LOG-056`.
Pinned VictoriaLogs source plus the complete 1,223-case live oracle establish
strict optional condition/`by` grammar, exact field lists, source snapshots,
longest-array zip, empty padding, one-row invalid/empty behavior, raw numeric
spelling, object order, bare `NaN`, nested JSON string normalization, and
strict errors. Timeless traverses retained native arrays without flattening
their durable representation and returns explicit empty strings where the
VictoriaLogs streaming encoder omits empty columns.

The bounded Rust evaluator enforces cumulative work, state, result,
response, deadline, and cancellation limits; query-backed conditions inherit
the parent request limits; overlapping destinations fail actionably; and
optimize, shutdown, and reopen leave source rows unchanged. `QSF-230` records
the semantic/storage boundary. Exact release build
`eff28bb74482bcadcec292b27f28c4462f663a73` measures 3.896/39.121 ms
narrow/wide p95 while returning 128 rows and 2,112 bytes, versus
3.670/39.180 ms for 64-row, 1,408-byte array-concat controls. `QSF-231`
accepts the +6.1%/-0.2% p95 and +8.3%/-0.0% request-attributed API mean after
identical public storage work, with the doubled cardinality and 704 additional
response bytes retained honestly. All 8,192 entries complete durably; storage
remains four raw blocks; queues and in-flight cancellation are zero. No
extension primitive, private table, storage contract, or authoritative
batching behavior changed.

Session 18's `LQL-P43` row is closed as an API-owned bounded join over two
public `logs` scans, with `SQL-LOG-057` as the deterministic one-key
left/inner direct-user foundation. Pinned VictoriaLogs source plus the
complete 1,249-case live oracle establish strict key/source/modifier grammar,
default-left and optional-inner behavior, missing/null/empty textual key
equivalence, duplicate expansion, deterministic left/right order, right-key
removal, nonempty-left collision precedence, prefixes, and strict errors.
Timeless additionally preserves complete typed/nested right values and fails
scalar-parent conflicts explicitly instead of flattening them.

Recursive query-backed plans share the parent request clock and cumulative
work/state/result/response/deadline limits; RHS rows, rich state, textual-key
indexes, duplicate buckets, and output cardinality are all bounded and
cancellable. `QSF-233` records the unchanged storage boundary. Exact release
build `8c718dceccfb56f251f7f54084976cc69233de7e` measures
6.875/73.000 ms narrow/wide p95 versus 6.225/72.087 ms equal-output
query-backed `in(...)` controls. `QSF-236` accepts the +10.4%/+1.3% endpoint
tail and +14.7%/+0.5% request-attributed API work after exactly two scans and
byte-identical physical storage work. `QSF-234`–`QSF-235` pin the evidence
harness's explicit two-scan and 192-item RHS-state reservation contracts. All
8,192 entries complete durably; storage remains four raw blocks; queued and
in-flight work are zero. No extension primitive, private table, storage
contract, or authoritative batching behavior changed.

Session 18's `LQL-P44` semantic implementation is complete as API-owned
bounded ordered concatenation over public `logs` scans, with `SQL-LOG-058` as
the direct-user `UNION ALL` foundation. Pinned VictoriaLogs source plus the
complete 1,267-case live oracle establish strict query/inline source grammar,
duplicate preservation, empty-source behavior, nested composition, later
statistics, and explicit failures. The live multi-worker oracle may expose
unsorted source rows in either order, so oracle comparisons sort instead of
inventing an HTTP response-order promise; Timeless provides deterministic
left-then-source order in its single-owner evaluator.

Recursive query-backed plans share the parent clock and cumulative
work/state/result/response/deadline limits. Complete typed/nested source rows,
inline text rows, traversal, cloning, cardinality, and cancellation are
bounded; optimize, shutdown, and reopen leave public source rows unchanged.
`QSF-237` records the semantic and unchanged-storage boundary. Exact release
build `c3ce658ed9475691fc8e4a51e9fe0e02fb57fe7c` measures
6.794/74.143 ms narrow/wide p95 versus 6.363/73.714 ms equal-output two-scan
controls. `QSF-238` accepts the +6.8%/+0.6% endpoint tail and
+8.9%/-0.5% request-attributed API mean after byte-identical physical
storage work. The explicit evidence contract pins two scans and exactly 128
retained-source work items per request. All 8,192 entries complete durably;
storage remains four raw blocks; queued and in-flight work are zero. No
extension primitive, private table, storage contract, or authoritative
batching behavior changed. `LQL-P44` is closed.

Session 18's `LQL-P45` row is closed as API-owned bounded running state over
one public `logs` scan, with `SQL-LOG-059` as the direct-user fixed-key SQLite
window foundation. Pinned VictoriaLogs source plus the complete 1,300-case
live oracle establish strict grouping/expression grammar, count, sum,
natural-order min/max, offset first/last, recursive prefix selection, and
explicit failures. Timeless deliberately corrects the upstream
variable-width formatted-time ordering bug by using numeric microsecond
chronology, keeps stable ties and deterministic lexical group order, and
preserves native rich exact values.

Group keys, sorting, recursive traversal, accumulators, offset history,
generated values, cardinality, response bytes, deadlines, and cancellation
are cumulative and bounded. Complete real-extension coverage pins alias
snapshots/conflicts, limits, immutable source rows, optimize, shutdown, and
reopen. `QSF-239` records the semantic and unchanged-storage boundary. Exact
release build `3f4ef107973f73361cfd90eff6e31ea53bd58f0c` measures
3.437/48.000 ms narrow/wide p95 versus 3.771/38.690 ms same-scan controls.
`QSF-240` accepts the -8.9%/+24.1% endpoint tail and -3.4%/+30.0%
request-attributed API mean after byte-identical public storage work. All
8,192 entries complete durably; storage remains four raw blocks; queued and
in-flight work are zero. No extension primitive, private table, storage
contract, or authoritative batching behavior changed. `LQL-P45` is closed.

The follow-up matrix reconciliation closes `LQL-S14` against that same P45
implementation and evidence. Upstream documents running `count`, `last`,
`max`, `min`, and `sum` as functions of the `running_stats` pipe; it exposes
no separate `running_*` grammar. `SQL-LOG-059`, the 1,300-case pinned oracle,
the real-extension regression, `QSF-239`–`QSF-240`, and exact-build artifact
`2026-08-06_session18_lql_p45_running_stats.json` already satisfy every S14
exit dimension. The current complete 1,388-case live oracle, targeted
real-extension regression, query contracts, and all 130 executable SQL
recipes/168 statements were re-run at reconciliation. No duplicate parser,
evaluator, SQL recipe, benchmark workload, or extension surface is warranted.
`LQL-S14` is closed as a catalog-alias row, not as a new implementation.

Session 18's `LQL-P46` semantic implementation is complete as API-owned
bounded total state over one public `logs` scan, with `SQL-LOG-060` as the
direct-user fixed-key full-partition SQLite window foundation. Pinned
VictoriaLogs source plus the complete 1,329-case live oracle establish the
shared strict grouping/expression grammar, complete-group count and sum,
natural-order rich min/max, fixed-offset first/last, canonical aliases,
recursive prefix selection, later-pipe visibility, destination overwrite,
empty input, skipped nonfinite sum input, and explicit failures.

Timeless deliberately retains P45's numeric-microsecond correction for the
upstream variable-width formatted-time ordering defect, with stable ties and
deterministic lexical group order. Group keys, chronological sorting,
recursive traversal, complete accumulators, offset values, generated fields,
cardinality, response bytes, deadlines, and cancellation are cumulative and
bounded. Real-extension coverage pins rich final state on every row, limits,
reader reuse after cancellation, immutable source rows, optimize, shutdown,
and reopen. `QSF-241` records the semantic and unchanged-storage boundary.
Exact release build `3bfdf2c843ebc221916d20b35a1df98d111e3eb9`
measures 3.744/47.706 ms narrow/wide p95 versus 3.618/34.716 ms same-scan
controls. `QSF-242` accepts the +3.5%/+37.4% endpoint tail and
+8.6%/+32.6% request-attributed API mean after byte-identical public storage
work. All 8,192 entries complete durably; storage remains four raw blocks;
queued and in-flight work are zero. No extension primitive, private table,
storage contract, or authoritative batching behavior changed. `LQL-P46` is
closed.

The follow-up matrix reconciliation closes `LQL-S15` against that same P46
implementation and evidence. Upstream documents total `count`, `first`,
`last`, `max`, `min`, and `sum` as functions of the `total_stats` pipe; it
exposes no separate `total_*` grammar. `SQL-LOG-060`, the 1,329-case pinned
oracle, the real-extension regression, `QSF-241`–`QSF-242`, and exact-build
artifact `2026-08-06_session18_lql_p46_total_stats.json` already satisfy
every S15 exit dimension. The current complete 1,388-case live oracle,
targeted real-extension regression, query contracts, and all 130 executable
SQL recipes/168 statements were re-run at reconciliation. No duplicate parser,
evaluator, SQL recipe, benchmark workload, or extension surface is warranted.
`LQL-S15` is closed as a catalog-alias row, not as a new implementation.

Session 18's `LQL-P47` semantic implementation is complete as API-owned
bounded timestamp transformation over one public `logs` scan, with
`SQL-LOG-061` as the direct-user native-unit shift and explicit sub-native
nanosecond foundation. Pinned VictoriaLogs source plus the complete
1,341-case live oracle establish strict duration/field grammar, nanosecond
RFC3339 parsing, timezone normalization, zone-less pinned-UTC behavior,
canonical output, invalid-string preservation, default `_time` composition,
and explicit failures. Real-extension coverage additionally preserves missing,
null, numeric, array, object, nested-sibling, and durable-source fidelity that
the upstream textual field model cannot express.

Every visited row and generated result is work/state/result/response/deadline
bounded and cancellation leaves the public reader reusable. Source rows remain
immutable through optimize, shutdown, and reopen. `QSF-243` records the
semantic and unchanged-storage boundary. Exact release build
`db274f37b217862ef5ed35d2100675f7d1183b75` measures 3.519/40.756 ms
narrow/wide p95 versus 3.197/35.390 ms same-scan projection controls, or
+10.1%/+15.2%. Request-attributed API means are +7.6%/+8.3%. Every pair has
byte-identical public block, decode, payload, match, return, and requested-work
counters. All 8,192 entries complete durably, storage remains four raw blocks,
queued and in-flight work are zero, and the complete-workload logs HWM is
106,108 KiB. `QSF-244` accepts the bounded post-scan cost. No extension
primitive, private table, storage contract, timestamp format, batching,
compression, index, retention, transaction, migration, optimize, or
maintenance behavior changed. `LQL-P47` is closed.

Session 18's `LQL-P48` semantic implementation is complete as API-owned,
input-independent bounded generation with `SQL-LOG-062` as the complete
direct-user core-SQL foundation. Pinned VictoriaLogs source plus the complete
1,357-case live oracle establish shared number spellings, positive fractional
truncation, decimal-string output, complete-input replacement,
last-generator-wins behavior, later-pipe composition, and strict failures.

The planner removes the final generator's semantically dead prefix, including
query-backed operations, and the reader executes without opening a public
`logs` cursor. Count admission, allocated rows and strings, later work, result
cardinality, response bytes, deadline, and cancellation are bounded. A
failing-then-passing real-extension regression proves zero storage query,
block, decode, payload, and row work; immutable rich source data; optimize;
shutdown; and reopen. `QSF-245` records the semantic and unchanged-storage
boundary. Exact release build
`8b6a5e7cb722ceeb7a5f25221994d2d8b619ed7f` measures 0.836/0.780 ms p95
for indexed-host/full-fixture source spellings while returning the same 64
rows and 886 bytes. Each records zero public query, block, decode, payload,
match, and row work. The 6.6% lower full-source p95 is accepted loopback
variation between identical scan-free plans. All 8,192 entries complete
durably; storage remains four raw blocks; queued and in-flight work are zero;
and logs HWM is 107,436 KiB. `QSF-246` records the exact-build verdict. No
extension primitive, private table, storage contract, batching, compression,
index, retention, transaction, migration, optimize, or maintenance behavior
changed. `LQL-P48` is closed.

Session 18's `LQL-S12` semantic implementation is complete as API-owned,
bounded `json_values` aggregation with `SQL-LOG-063` as the fixed-path typed
JSON1 foundation for direct users. Pinned VictoriaLogs source plus the
complete 1,374-case live oracle establish the statistics and shorthand
grammar, JSON-array-string envelope, missing-field objects, natural multi-key
ordering, bounded top-k, zero-limit behavior, aliases, and strict errors.

Timeless preserves native nested values rather than flattening every upstream
column to text, and it strengthens unspecified equal-key order to stable
public-row order. Direct parser/evaluator and real-extension regressions pin
those boundaries, immutable source rows, optimize, flush, shutdown, reopen,
and reader reuse. Review caught an initially incomplete limit boundary:
the implicit result name now includes VictoriaLogs' normalized sort/limit
suffix; sort-key projection and multiple `json_values` expressions share one
cumulative work budget; retained result strings are included in the state
budget; and the transient heap-to-index conversion is peak-bounded. `QSF-247`
records these corrections and the unchanged public-storage boundary. Exact
release evidence measures 3.557/37.237 ms narrow/wide p95 versus
3.364/34.563 ms equal-scan controls, or 5.7%/7.7% higher. Every pair reads
the same one/four blocks, 1,024/8,192 decoded entries,
235,778/1,914,055 payload bytes, and 128/8,192 public rows. All 8,192 fixture
entries complete durably; storage remains four raw blocks; queued and
in-flight work are zero; and logs HWM is 106,908 KiB. `QSF-248` records the
accepted exact-build verdict. No extension primitive, private table, storage
contract, batching, compression, index, retention, transaction, migration,
optimize, or maintenance behavior changed. `LQL-S12` is closed.

Session 18's `LQL-S13` semantic implementation is complete as API-owned,
bounded `histogram` aggregation with `SQL-LOG-064` as the fixed-path native-
number foundation for direct users. Pinned VictoriaLogs source plus the
complete 1,388-case live oracle establish strict one-exact-field statistics
and shorthand grammar; decimal/general-number/duration/byte coercions;
VictoriaMetrics' 486 logarithmic middle buckets plus lower/upper buckets;
exact edge handling; ignored negative/NaN/IPv4/timestamp values; natural
`vmrange` order; native hit counts; the JSON-array-string envelope; empty
input; canonical aliases; and strict failures.

Timeless additionally reads native numbers and rich nested paths without
flattening or mutation. Parser/evaluator and real-extension regressions pin a
fixed 488-counter stack state, process-wide immutable range labels,
cumulative multi-expression work, response-state accounting, cancellation
and reader reuse, immutable sources, optimize, flush, shutdown, and reopen.
`QSF-249` records the semantic and unchanged-storage boundary. Exact-build
release evidence records 4.329/39.536 ms narrow/wide p95 versus
3.517/38.826 ms equal-scan bounded-`values` controls, or 23.1%/1.8% higher.
Request-attributed API means are 6.9%/2.0% higher. Every pair reads the same
one/four blocks, 1,024/8,192 decoded entries, 235,778/1,914,055 payload
bytes, and 128/8,192 public rows. All 8,192 fixture entries complete durably;
storage remains four raw blocks; queued and in-flight work are zero; and logs
HWM is 102,964 KiB. `QSF-250` records the accepted exact-build verdict. No
extension primitive, private table, storage contract, batching, compression,
index, retention, transaction, migration, optimize, or maintenance behavior
changed. `LQL-S13` is closed.

### Session 19: experimental and data-model dispositions

Rows: `PQL-O17`, `PQL-O18`, `PQL-O19`, `PQL-R22`, `PQL-R23`,
`PQL-F14`, `PQL-F21`, `PQL-S22`, `PQL-H04`, `MQL-08`,
`LQL-F35`, `LQL-F36`, `LQL-P49`, and `LQL-P50`.

Exit: each experimental row names its stability/cost decision; each deferred
row names a typed native-histogram, stream-identity, or other exact
prerequisite. None is implied by a nearby primitive or counted as parity.

`PQL-O17` is closed as an experimental disposition. Pinned Prometheus 3.13.2
requires `promql-experimental-functions` for both `limitk` and `limit_ratio`;
the stable Timeless GET, POST, and post-reopen paths preserve the exact
diagnostic and perform zero raw/window extension queries. The foundation is
`RAW`/`API`, not SQL or an extension primitive: upstream selection depends on
canonical Prometheus label hashing or evaluator group order after child-vector
evaluation and cannot prune storage. Reconsideration requires an explicit
experimental compatibility configuration and feature-enabled oracle, bounded
group/vector state and cancellation, and typed native-histogram storage for a
claim of full upstream parity. No query latency/storage benchmark is claimed
for an operator that the stable product deliberately does not execute.

`PQL-O18` is closed as an experimental disposition. Pinned Prometheus 3.13.2
requires `promql-binop-fill-modifiers`; the stable Timeless parser previously
recognized fill nodes but returned an internal “not shipped” message. It now
preserves the pinned diagnostic across GET, POST, shutdown, and reopen with
zero raw/window extension queries. Executable `SQL-PROM-057` gives direct
SQLite/libSQL users bounded one-to-one left/right filling over two public
grids. A future experimental Rust tier requires a feature-enabled oracle and
complete numeric-literal, arithmetic/comparison, per-step matching,
group-cardinality, uniqueness, label/name, result-limit, and cancellation
semantics. No extension primitive is justified because both child vectors
must already be scanned, decoded, and evaluated. No API latency/storage
benchmark is claimed for syntax the stable product deliberately rejects.

`PQL-O19` is closed as a typed-data-model deferral. Pinned Prometheus 3.13.2
accepts `</` and `>/` without an experimental flag. Float/float inputs return
no samples plus exact incompatible-type infos; useful results require a typed
native histogram on the left and a scalar on the right. Timeless previously
collapsed the unrecognized tokens into `parse error: invalid promql query`.
The stable GET, POST, shutdown, and reopen paths now report the exact operator
position and the missing typed-native-histogram prerequisite with zero
raw/window extension queries. Quoted strings and line comments cannot trigger
the detector. No SQL recipe or extension primitive is claimed: classic
`_bucket` float series cannot represent the positive/negative spans, custom
bounds, zero bucket, reset hint, count, and sum mutated by the operator.
Shipping requires `PQL-S22` storage, ingress, public batch/SQL, result, bucket
interpolation, annotation, limit, cancellation, durability, and oracle work.
No latency/storage benchmark is claimed for a deliberately rejected row.

`PQL-R22` is closed as an experimental disposition. Pinned Prometheus 3.13.2
requires `promql-experimental-functions` for `mad_over_time`; the stable API
previously parsed the call and leaked `unsupported PromQL expression (parsed
as function call)`. GET, POST, shutdown, and reopen now preserve the pinned
feature-gate diagnostic with zero raw/window extension queries. Executable
`SQL-PROM-058` gives direct SQLite/libSQL users a bounded finite-float
foundation by applying Prometheus's linear median twice over public raw rows.
A future experimental Rust tier requires a feature-enabled oracle and exact
window/subquery, raw NaN, signed-zero, infinity, label/name, annotation,
limit, and cancellation semantics. Complete mixed-range parity additionally
requires `PQL-S22` typed native-histogram samples so all-histogram ranges can
be omitted and mixed ranges can emit the upstream info. No extension primitive
is justified because both median passes require the already-decoded raw
values. No API benchmark is claimed for stable syntax that is rejected.

`PQL-R23` is closed as an experimental disposition. Pinned Prometheus 3.13.2
requires `promql-experimental-functions` for `ts_of_first_over_time`,
`ts_of_last_over_time`, `ts_of_min_over_time`, and `ts_of_max_over_time`.
Stable Timeless previously parsed each call and leaked the same internal
unsupported-expression error as `PQL-R22`; GET, POST, shutdown, and reopen now
preserve all four pinned diagnostics with zero raw/window extension queries.
Executable `SQL-PROM-059` gives direct SQLite/libSQL users bounded finite-float
first/last timestamps and latest-tied min/max timestamps over public raw rows.
A future experimental Rust tier requires a feature-enabled oracle and exact
millisecond conversion, boundaries, subqueries, IEEE ordering, label/name,
annotation, limit, and cancellation behavior. Full upstream first/last and
mixed-range behavior requires `PQL-S22` typed native-histogram samples. No
extension primitive or stable API benchmark is justified.

`PQL-F14` is closed as an experimental disposition. Pinned Prometheus 3.13.2
requires `promql-experimental-functions` for `sort_by_label` and
`sort_by_label_desc`. Stable Timeless previously parsed both calls and leaked
the internal unsupported-expression error; GET, POST, shutdown, and reopen now
preserve both pinned diagnostics with zero raw/window extension queries.
There is deliberately no SQL recipe: upstream uses natural ordering for each
requested label and exact full-label-set tie-breaking, neither of which is a
portable core SQLite/libSQL collation. A future experimental Rust tier must
use a feature-enabled oracle and own missing labels, variadic strings, natural
numeric runs, ascending/descending full-label ties, floats/histograms, range
warnings, limits, cancellation, and response order. Sorting happens after the
bounded child vector, so no extension primitive or stable API benchmark is
justified.

`PQL-F21` is closed as an experimental disposition. Pinned Prometheus 3.13.2
requires `promql-experimental-functions` for both the one-argument `info(v)`
form and its optional selector-only second argument. Stable Timeless
previously leaked an internal unsupported-call error; GET, POST, shutdown,
and reopen now preserve the pinned diagnostic before evaluating the child or
reading storage. There is deliberately no SQL recipe. The enabled function
performs a second lookback-aware info-series selection, uses fixed `job` and
`instance` identity, resolves changing data labels by the newest source
sample, honors exact stale markers, and can return float or native-histogram
base samples. Full execution therefore requires `PQL-S17` marker-capable
ingress and selector semantics plus `PQL-S22` typed native-histogram storage
and results. A future experimental Rust tier can compose the public catalog
and packed-raw surfaces; no extension primitive or stable API benchmark is
justified.

`PQL-S22` is closed as a data-model deferral. The extension capability
document previously named metric batch versions but did not say that every
current sample surface is float-only. It now additively advertises
`sample_types=["float64"]` and `native_histograms=false`; direct SQLite
regression, extension unit coverage, reopen/crash suites, and data ABI 1 pin
that declaration. No existing named/resolved batch, metric chunk, rollup,
catalog row, SQL column, or `TRF1`/`TWB1`/`TRB1` frame can represent a typed
native histogram. Shipping therefore requires one reviewed, versioned,
backward-readable design spanning Prometheus exponential/custom schemas,
bucket populations, zero/count/sum/reset metadata, mixed sample types,
ingress, chunks, public SQL/batches/frames, rollups, retention, transactions,
migration, corruption, capability negotiation, limits, cancellation, and
downgrade. This session deliberately adds no histogram format, SQL recipe, or
performance benchmark.

`PQL-H04` is closed with its retained-model slice shipped and its typed slice
still deferred. Pinned Prometheus 3.13.2 defines all five functions as stable
one-vector calls and silently ignores float samples. Timeless now evaluates
the complete child under ordinary public-read, work, result, response,
deadline, and cancellation ownership, then drops every current float sample;
GET, POST, range, shutdown, and reopen regressions include classic
`_bucket`/`_sum`/`_count` names. Count, sum, average, and bucket-estimated
variance values still require the versioned typed sample design in `PQL-S22`.
No empty-result SQL shortcut or extension primitive is claimed because it
would skip child semantics rather than reproduce the language function. Exact
release evidence measures 0.823/4.203 ms narrow/wide p95 versus 0.786/4.239 ms
equal-read empty-filter controls, or 4.6% higher/0.8% lower. Both pairs have
identical public chunks, decoded/returned points, payload bytes, zero result
cardinality, and 63-byte responses. Metrics HWM is 52,224 KiB after the full
workload.

`MQL-08` is closed as a finite catalog disposition, not a blanket MetricsQL
compatibility row. The old Elixir parser's twelve-name `@metricsql_fns` list
was rejection-only: it implemented none of `label_keep`, `label_map`,
`quantiles`, `distinct`, `increase_pure`, `remove_resets`, `interpolate`,
`keep_last_value`, `keep_next_value`, `drop_common_labels`, `rate_over_sum`, or
`WITH`. Pinned VictoriaMetrics 1.148.0 source and twelve live immutable-image
witnesses classify those names across label transforms, cross-series
aggregates, range/grid transforms, rollups, and parser-time template grammar.
They cannot honestly share one evaluator or SQL contract. Both Timeless
MetricsQL routes now reject each name case-insensitively at its source
position, before public storage reads, through GET, POST, range, shutdown, and
reopen; strings and comments are isolated. Each construct must receive its own
stable matrix row, exact oracle corpus, ownership decision, public-SQL recipe
or no-SQL disposition, limits/cancellation contract, real-extension
regression, and evidence before execution is enabled. No benchmark is claimed
for a catalog the product deliberately rejects, and no extension or storage
contract changed.

`LQL-F35` is closed as a stream-data-model deferral. Pinned VictoriaLogs
1.52.0 assigns stream fields during ingestion, removes empty stream values,
canonically sorts the remaining name/value pairs, combines their 128-bit hash
with the tenant account/project identity, indexes that identity, and applies
`{...}` before ordinary row filters. Four live immutable-image witnesses pin
bare/prefixed equality, regular-expression, static-membership, and disjunction
forms. Timeless rich-log batch v1 retains row metadata but carries no
ingestion-declared stream-field set, tenant-scoped canonical stream ID, or
stream index; treating `{service="api"}` as a row predicate would therefore
claim false semantics. The POST LogsQL route now returns one stable
source-positioned HTTP 422 error before any public storage read across base,
nested filter-pipe, shutdown, and reopen paths, while quoted and commented
braces remain ordinary text and explicit `rows({...})` objects remain inline
data. The full parser suite caught and pinned that distinction after the first
overbroad detector rejected existing `join`, `union`, and discarded-prefix
`generate_sequence` plans. The native-parameter GET route continues not to
accept LogsQL text. Shipping needs one versioned design for stream-field
declaration and canonicalization,
tenant identity, batch/SQL/block representation, indexes and pruning,
ingestion defaults/headers, migration/downgrade/corruption, retention,
optimize, backup, limits, cancellation, and public capabilities. There is no
SQL recipe or benchmark for deliberately rejected syntax, and no extension or
storage contract changed.

`LQL-F36` is closed as the companion stream-ID data-model deferral. Pinned
VictoriaLogs 1.52.0 exposes a lowercase 48-hex `_stream_id` made from the
8-byte tenant account/project prefix plus the 16-byte canonical stream hash;
blocks are ordered by that identity and exact/static/query-backed filters are
converted to stream-ID sets before block search. Six accepted live witnesses
pin exact and quoted spellings, static and query-backed `in(...)`,
case-insensitive unquoted keywords, and whitespace before the colon; two error
witnesses pin malformed IDs and the invalid generic-empty-equality form. The
fixture-derived `case="phrase-exact"` ID is deterministic and was obtained
from the immutable image rather than reimplemented.

Timeless previously accepted `_stream_id:<id>` as arbitrary metadata equality,
silently reading the wrong data model and usually returning no rows. The POST
LogsQL route now returns one stable source-positioned HTTP 422 before storage
for top-level exact, quoted, static-list, query-backed, base, and filter-pipe
forms across shutdown/reopen. Bare words and message values, nested
`foo._stream_id` metadata, field projections, comments, and explicit
`rows({...})` objects remain ordinary retained data. Shipping requires the
same complete versioned identity contract as `LQL-F35`, including the public
48-hex representation and collision/tenant rules. There is no SQL recipe or
latency benchmark for deliberately rejected syntax, and no extension or
storage contract changed.

`LQL-P49` is closed as an applicable result-transform row rather than being
incorrectly grouped with stored stream identity. Pinned VictoriaLogs 1.52.0
source and eleven successful plus eight error witnesses establish that
`set_stream_fields` selects exact/prefix/all fields from each current row,
omits empty values, sorts names, Go-quotes values, writes `_stream`, clears
`_stream_id` on matching rows, resolves direct or query-backed optional
conditions, and preserves both columns when a condition is false. It does not
mutate persisted stream fields or IDs.

The Rust logs API implements that complete bounded transform over public rich
rows. Wildcards recursively expose object leaves under dotted names, arrays
remain atomic compact JSON, overlap is deterministic, earlier and later pipes
compose, strict syntax fails before storage, and work/state/result/response/
deadline/cancellation limits remain cumulative. Real-extension coverage pins
native nested values, immutable durable metadata, optimize, shutdown, and
reopen. There is deliberately no SQL recipe: portable core SQLite/libSQL
cannot combine the dynamic current-pipeline schema with Go Unicode/control-
byte quoting. Since every value already crossed the public row boundary, no
extension primitive, private access, storage format, authoritative batching,
compression, index, retention, transaction, migration, optimize, or
maintenance behavior changes.

Exact release build `bf82f7625a15170b93d2a7ea8e8fd5ec94d6300c`
returns the same 64 rows/1,856 bytes as its equal-read control and performs
byte-identical public storage work. Narrow/wide candidate p95 is 4.564/40.091
ms versus 3.633/38.555 ms for the control; request-attributed API mean is
4.4% lower/1.4% higher. The bounded post-scan cost is accepted without an
extension primitive; exact evidence and the storage verdict are linked by
`QSF-266`.

`LQL-P50` is closed as a data-model deferral. Five successful and five error
witnesses pass in the complete 1,429-case immutable VictoriaLogs fixture.
Pinned source proves that `stream_context` groups selected rows by stored
tenant-scoped `_stream_id`, performs additional exact-stream time-range reads,
retains bounded rows before and after each match, deduplicates overlaps, orders
contexts, and emits stream-aware delimiters. It therefore depends on the full
`LQL-F35`/`LQL-F36` declaration, tenant, canonical hash, and stream-index
contract; sorting Timeless rows by time or grouping arbitrary metadata would
be a false implementation.

The prior generic unsupported-pipeline response is replaced by a
source-positioned HTTP 422 before storage for genuine top-level and nested
pipes. Quoted text, comments, base words, field names, and nested application
metadata remain queryable. Real-extension coverage pins zero read/decode
counters, rich fidelity, optimize, shutdown, and reopen. There is no SQL
recipe or benchmark for deliberately rejected syntax, and no extension,
private access, storage format, authoritative batching, compression, index,
retention, transaction, migration, optimize, or maintenance behavior changed.

The terminal-row audit found that Session 17 had incorrectly claimed every
declared P2 row complete while `LQL-Q03` and `LQL-Q04` remained nonterminal.
`LQL-Q03` is now closed as an architecture deferral. Six successful and six
error witnesses pass the complete 1,441-case immutable VictoriaLogs fixture.
Pinned source distinguishes two intra-query controls: `concurrency` limits
CPU-bound pipe workers, while `parallel_readers` controls I/O readers and may
inherit the former. They apply per storage node and are not output-only hints.

Timeless executes one public SQLite cursor on one reader thread per query.
The configurable reader pool serves independent requests, and authenticated
request admission caps per-subject concurrency; neither setting implements
multiple workers/readers inside one query. Accepting these options as ignored
would therefore make a false CPU, RAM, and I/O promise. Genuine top-level and
nested occurrences now fail with one source-positioned HTTP 422 before
storage, while quoted/commented text and ordinary field names remain data.
There is no SQL recipe or performance benchmark for rejected resource syntax.
A future implementation needs bounded public partitioning, deterministic
merge/order, exact work accounting, cancellation, and reader-reuse evidence.
No extension, storage, batching, compression, index, transaction, migration,
optimize, or maintenance contract changed.

`LQL-Q04` is now implemented as a scoped Rust API query option. Eleven
successful and eight error witnesses pass the complete 1,460-case immutable
VictoriaLogs fixture. The planner shifts logical storage bounds backward by
the signed offset with exact retained-unit ceil/floor handling, while one
cancellation-aware result transform shifts `_time` forward with nanosecond
precision. Nested queries inherit the offset and explicit inner values replace
it; day/week filters compose; duplicate values use the last assignment; and
malformed options fail before storage.

Pinned source also establishes the non-obvious optimizer order: consecutive
leading `filter` pipes are folded into the storage predicate after query-option
bound translation and therefore see source timestamps, while later pipes see
shifted result timestamps. Real-extension coverage pins that order, rich
metadata/array fidelity, limits, cancellation, immutable storage, optimize,
flush, shutdown, and reopen. Executable `SQL-LOG-065` gives direct users exact
integer source-bound translation plus an explicit signed sub-native remainder
through only the public `logs` table. No extension primitive, private access,
storage format, authoritative batching, compression, index, retention,
transaction, migration, optimize, or maintenance behavior changed.

Exact build `c4ea4cf6ba43de36f831e048d9f39e3e60b8d183` measures
3.230/3.538/4.159 ms narrow and 37.799/38.840/39.006 ms wide
p50/p95/p99, versus equal-read controls at 3.032/3.162/3.366 and
33.530/34.322/34.499 ms. The 11.9%/13.2% p95 cost is API-owned timestamp
transformation: every pair has identical public blocks, decoded entries,
payload bytes, matched rows, and returned rows. The 8,192-entry fixture
remains four raw blocks and 2,022,736 physical bytes with zero queued work;
logs RSS HWM is 101,372 KiB.

`LQL-Q05` now has its semantic implementation and pre-evidence gates. A
leading `global_filter` is compiled as a scoped predicate and conjoined with
the base query and every query-backed membership, conditional pipeline, join,
and union scope. Nested queries inherit it, explicit nested declarations
replace it, all duplicate declarations are validated before the last wins,
and result pipelines in the filter-only value fail before storage. Thirteen
successful and nine error witnesses pass in the complete 1,482-case immutable
VictoriaLogs fixture.

The implementation deliberately avoids textual substitution. Compiled scope
preserves quoting, source positions, query-backed initialization, and the
upstream declaration-time interaction with `time_offset`. The real-extension
regression covers cumulative limits, cancellation and reader reuse, rich
fidelity, immutable storage, flush, optimize, shutdown, and reopen. Executable
`SQL-LOG-066` gives direct users the ordinary-SQL foundation by conjoining one
shared predicate with each separately bounded public `logs` scan. A planner
finding requires hidden virtual-table inputs to be bound directly instead of
through a reorderable joined CTE.

Exact build `72a392a0ed28f7c61c3c816a2c300c8caa588400` closes the
row. Global-filter p95 is 6.102/38.893 ms narrow/wide versus 5.040/40.365 ms
for explicit-conjunction controls: 21.1% higher/3.6% lower. The narrow p50 is
lower and request-attributed API mean is only 4.1% higher; wide API mean is
6.8% lower. Every pair has identical candidate blocks, decoded entries,
payload bytes, matched/returned public rows, 64-row/2,560-byte responses, and
unchanged four-block storage. `QSF-273`–`QSF-276` retain the complete semantic,
parser, planner, performance, HWM, and ownership verdicts. `LQL-Q05` is
shipped without an extension or storage change.

`LQL-Q06` is closed as an architecture deferral. Nine successful and seven
error witnesses pass the complete 1,498-case immutable VictoriaLogs fixture.
Pinned source proves that `allow_partial_response` is not a result transform:
it suppresses an unavailable `vlstorage` owner only when another independent
owner succeeds, while all-owner outage and configuration errors remain fatal.
Timeless has one authoritative SQLite/libSQL owner, so accepting the option as
a no-op or hiding its failure would be dishonest.

The Rust parser validates all Go boolean forms, quotes, duplicates, nesting,
and malformed syntax. False executes the complete fail-closed query, true
returns stable HTTP 422 before storage, and malformed values return HTTP 400
for both query options and the separately supported upstream HTTP form
parameter. The final duplicate wins, and an explicit query option overrides a
valid HTTP value. Failing regressions exposed and corrected two adjacent gaps:
nested membership/join/union wrappers had demoted recognized unsupported
capabilities to malformed syntax, and the POST form decoder had silently
ignored the unsupported parameter. The real-extension regression proves exact
rich false results, zero row/count/payload/decode work for rejected true and
malformed input, optimize, shutdown, and reopen.

There is no SQL recipe or benchmark for deliberately rejected multi-owner
failure policy. Reconsideration requires multiple fenced public owners,
unavailable/error classification, deterministic bounded merge/order,
cumulative limits and cancellation, and explicit response-completeness
metadata. `QSF-277`–`QSF-279` retain the architecture and regression verdicts.
No extension, private access, storage format, authoritative batching,
compression, index, rollup, retention, transaction, migration, optimize, or
maintenance behavior changed.

### Session 20: release-grade public documentation

Audit every public virtual table, TVF, scalar function, hidden input, batch
format, capability, signal binary, HTTP route, configuration variable, auth
contract, limit, command, migration state, backup/restore operation, error, and
platform. Replace POC/development-history wording with current product
behavior. Add a versioned changelog, compatibility statement, upgrade guide,
embedded Rust guide, sqld/libSQL guide, complete SQL/API reference, artifact
inventory, and copyable examples.

Exit: every public symbol and server route has one canonical reference entry;
capability negotiation and formats are versioned; examples execute in CI;
README, guides, matrices, and binaries agree; no stale four-severity, flattened
metadata, POC, or unavailable-artifact claim remains.

Session 20a closes the extension/SQL inventory. The canonical
`docs/SQL_API_REFERENCE.md` is derived from the real registrations and loaded
schemas. It covers both loadable artifacts, the Rust embedding entry points,
all registered SQL symbols, stored-table schemas and creation arguments,
hidden inputs, commands, six ingestion formats, six packed query formats,
timestamp units, limits, transactions, concurrency, backup boundaries, and
compatibility rules. A Rust contract compares its marked inventory to every
source `create_module`/`create_scalar_function` registration.

The additive capability handshake now advertises `sql_surface_version=1`,
the exact three storage and twenty-two production query modules, and all
packed format names. Release CLI section 1 loads the real extension and proves
the advertised module set equals `pragma_module_list`; the pure Rust unit pins
the JSON. The pre-existing unversioned raw-series payload is preserved and
labelled `raw-series-v0`/`versioned=false`, with `TRF1` preferred for new wide
consumers. `QSF-280`–`QSF-281` retain both audit findings. Session 20 remains
open for the compatibility and upgrade guides,
embedded/sqld guides, artifact inventory, changelog, stale-wording audit, and
copyable-example gate. The redundant Python `TAF1`/`TLF1` decoder example was
removed in favor of the public maintained Rust decoders.

Session 20b closes the Rust signal-server inventory. The canonical
`docs/SERVER_API_REFERENCE.md` covers all three binary launch contracts, the
exact 45 route/method registrations, 28 runtime variables and defaults,
request formats, hard limits, Ed25519 policy/token validation, scope mapping,
admission, owner fencing, migration ledger, graceful drain, WAL checkpoint,
verified backup/restore boundary, error envelopes, build identity, and checked
release platforms. A Rust documentation contract derives every route and
runtime variable from production source and rejects missing, extra, or
method-mismatched entries. `QSF-282` corrects the stale reference to an
unimplemented Unix-socket listener; `QSF-283` corrects the trace-retention
default from seven days to the actual absent/inherit policy. Current server
READMEs link the canonical contract and no longer frame release binaries as
POCs or require Phoenix for standalone use.

Session 20c closes release versioning, compatibility, and replacement
operations. `CHANGELOG.md`, `docs/COMPATIBILITY.md`, and `docs/UPGRADE.md`
separate source, artifact, data-ABI, SQL-surface, batch/frame, schema-ledger,
and language generations; define supported binary pairings; and give a
copy-based drain/preflight/upgrade/verify/rollback procedure. `QSF-284`
records that tagged `v0.3.0` predates the capability handshake even though
post-tag source still reported `0.3.0`. Both workspaces and mutual peer floors
now advance to unreleased `0.4.0`, while compatible future `0.4.x` patches may
retain that peer floor; data ABI 1 and every storage format remain unchanged.
Rust regressions and a source-derived eight-axis documentation contract prevent
another tag/source identity collision without breaking patch compatibility.

Session 20d closes the remaining embedding, sqld, artifact, install/removal,
and stale-development-wording work. The public Rust API now has two explicit,
mutually exclusive build modes: the default `entrypoints` feature preserves
the loadable `.so`/`.dylib`, while
`--no-default-features --features embedded` links a host SQLite API. The
executable static example registers only production telemetry and validates
metrics, typed nested logs, and rich spans. `QSF-287` records the audit defect
that previously let this API compile and then panic before registration.

The detached Rust `libsql-check` gate no longer treats the compatibility-only
spike module as product evidence or deletes a fixed `/tmp` path. It owns a
temporary database, loads the release artifact on independent writer/reader
connections, verifies the negotiated production inventory and all three
signals, closes the database, and repeats exact reads after reopen. `QSF-288`
records that correction. The sqld guide is pinned to the checksum-verified
official `libsql-server-v0.24.32` release at full commit
`40c272de85ee4e62d722c5ccae5da2e76b4253a1`; live Hrana pipelines proved
capability negotiation, all three flushes, packed metrics, typed logs,
complete rich spans, a closed-stream read, and graceful shutdown. It no longer
claims an implicit pipeline transaction, floating `main`/`latest`, private
shadow inspection, or unmeasured provider/replication behavior (`QSF-289`).

`docs/ARTIFACTS.md` separates unreleased source from publication and gives the
exact four native targets, archive paths, two checksum scopes,
manifest/SBOM/license inventory, immutable install layout, ownership-aware
removal, and coordinated backup/rollback boundary. Rust contracts derive the
target and payload sets from the existing packager and reject missing, extra,
or renamed rows (`QSF-290`). `PLAN.md` and `RESULTS.md` are now explicitly
marked historical design/benchmark evidence; current README/GUIDE replication
and sqld claims are bounded to committed host-managed state.

Session 20's implementation exit is complete through maintained local Rust,
real-extension, CLI, and documentation gates. No workflow was added or
changed: automatic CI remains prohibited, so wiring any new example into a
hosted workflow requires a separate explicit owner request. The complete
verification included 43 Rust query-harness tests, both embedding modes,
libSQL 0.9.30 close/reopen, checksum-pinned sqld HTTP, root/server tests and
Clippy, separate dbhealth build, strict Rustdoc, 93 real-extension metrics
tests, 84 real-extension logs tests, trace suites, and all 45 CLI sections
with 150,000 randomized operations, five kill-9 rounds, and 135 executable
recipes/173 SQL statements.

### Session 21: final release gate and report

Run all extension, crash, transaction, migration, storage, API, parser,
fidelity, query, packaging, and fault suites from `main`. Run sustained query,
cancellation, shutdown/reopen, corruption, and bounded-RSS checks. Reproduce
the final narrow/wide performance evidence and compare it with Session 0.

Exit: every matrix row has an honest terminal disposition; every shipped SQL
row has an executable recipe; all applicable retained-model P0–P3 behavior
passes the pinned oracle; documentation describes the actual product; all
gates are green at pushed `main`. The final report gives exact compatibility,
benchmarks, storage findings, artifacts, remaining prerequisites, and concrete
recommendations for the higher-order Elixir interfaces.

TraceQL remains outside this plan. Trace storage/query findings still enter
`QUERY_STORAGE_FINDINGS.md`; the final report recommends a later trace matrix
organized around selectors, relationship traversal, span/event attributes,
aggregates, structural operators, limits, and public SQL/storage foundations.

#### Session 21 completion evidence

The final matrix audit leaves all 216 rows terminal: 191 shipped, 11
experimental, and 14 deferred behind explicit prerequisites. The Rust
documentation contract derives that summary, requires every non-shipped ID in
the final report, checks exact query-build identities and durable barriers,
rejects absent or failed owned fault evidence, and keeps the append-only
storage-finding IDs, table columns, terminal statuses, and reported range in
sync. A general tracked-document check also rejects inconsistent unescaped
Markdown table columns outside fenced examples. `LQL-P10` and `LQL-P11` received the missing
physical-block-diagnostics prerequisite and explicit no-SQL disposition
without exposing private storage.

The exact-build query artifact for implementation source
`178f75f7f3b1284c6b3940ae7af0986b9e2fe940` executes 184 metric and 307 log
shapes for 24,550 measured requests and reports the required Session 0 versus
final p50/p95/p99, completed work, storage, result bytes, and HWM. The current
two-minute fault artifact passes all 12 scheduled events and durably completes
30,784 records per signal across five process generations with no final
failure. Its SHA-256 is
`d4c927dc2b01327207dda49444fbbfa502c7b810d3e714de0a4f43fe2a1ede37`;
the exact query artifact SHA-256 is
`d576b5bcb3729f79d6e81f4793181551c3c99fd9c9a6bbdb877164c2b7e39660`.

The successful gate set includes 48 Rust query-contract tests; 135 public-SQL
recipes/173 statements; live immutable Prometheus 549, VictoriaMetrics 196,
and VictoriaLogs 1,498-case runs; root and server format/test/strict
Clippy/Rustdoc; separate dbhealth validation; 93 metrics and 84 logs
real-extension contracts plus trace/OTLP/Jaeger suites; all 45 CLI sections
with 150,000 randomized operations and five kill-9 rounds; static embedding;
direct libSQL 0.9.30 multi-connection/cold-reopen; and a native Linux x86-64
package/checksum/manifest/SBOM/notice/install/removal/preservation drill. The
checked-in two-hour release soak remains the sustained-resource authority; it
was not repeated as an unnecessary second two-hour run.

No workflow, tag, publication, downstream repository, dependency pin, or
TraceQL implementation is part of Session 21. The exact pushed `main` object
ID and clean-main repeat are handoff evidence because a commit cannot contain
its own hash.
