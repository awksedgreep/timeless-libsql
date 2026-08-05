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
aggregation over public current-pipeline rows. Ten successful and eight error
oracle cases pin default and explicit limits, parenthesized/bare multi-field
grammar, textual missing/null/type projection, frequency/key order, collision
names, case-insensitive modifiers, composition, and empty input. Executable
`SQL-LOG-029` gives direct SQLite/libSQL users the parameterized public
single-field `GROUP BY` and rank foundation. Work/group/result/response-state
limits, cancellation, optimize, shutdown, reopen, and reader reuse are pinned
without a new extension primitive or storage-contract change. `QSF-157`
records the semantic boundary; exact-build evidence and the final verdict are
recorded in `QSF-158`.
Session 17 continues with `LQL-P16`.

### Session 18: applicable LogsQL P3

Rows: `LQL-F37`, `LQL-F38`, `LQL-F41`, `LQL-P17`, `LQL-P25`–`LQL-P27`,
`LQL-P31`, `LQL-P35`, `LQL-P37`–`LQL-P40`, `LQL-P42`–`LQL-P48`,
`LQL-S12`–`LQL-S15`, and `LQL-Q05`, `LQL-Q06`.

Exit: each row is individually shipped, classified as higher-library work, or
deferred with a concrete prerequisite. Stateful and multi-query constructs
prove strict memory/cardinality limits; partial responses remain opt-in and
unmistakable.

### Session 19: experimental and data-model dispositions

Rows: `PQL-O17`, `PQL-O18`, `PQL-O19`, `PQL-R22`, `PQL-R23`,
`PQL-F14`, `PQL-F21`, `PQL-S22`, `PQL-H04`, `MQL-08`,
`LQL-F35`, `LQL-F36`, `LQL-P49`, and `LQL-P50`.

Exit: each experimental row names its stability/cost decision; each deferred
row names a typed native-histogram, stream-identity, or other exact
prerequisite. None is implied by a nearby primitive or counted as parity.

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
