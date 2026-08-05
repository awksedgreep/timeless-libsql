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

### Session 14: stable PromQL P2 completion

Rows: `PQL-S10`, `PQL-S14`, `PQL-S15`, `PQL-F19`, `PQL-F20`,
`PQL-H02`, and `PQL-H03`.

Exit: every applicable P2 PromQL row passes Prometheus, including quoted UTF-8
names, comments, calendar/evaluation functions, step-relative ranges, and
classic-histogram boundary behavior. Experimental features remain separate.

### Session 15: MetricsQL compatibility tier

Rows: `MQL-01`–`MQL-07` in order. `MQL-08` receives a bounded explicit
disposition later, not a blanket compatibility claim.

Exit: each shipped construct is labeled MetricsQL, passes VictoriaMetrics
`v1.148.0`, records every Prometheus difference, and cannot alter the default
PromQL interpretation silently.

### Session 16: LogsQL P2 filters

Rows: `LQL-F11`, `LQL-F16`, `LQL-F17`, `LQL-F20`–`LQL-F23`,
`LQL-F25`–`LQL-F28`, `LQL-F30`, `LQL-F32`–`LQL-F34`, and
`LQL-F40`.

Exit: all applicable P2 filters pass VictoriaLogs with explicit byte/Unicode,
timezone, nested-array, field expansion, escape, and error-location behavior.

### Session 17: LogsQL P2 pipelines and query behavior

Rows: `LQL-P07`, `LQL-P10`–`LQL-P16`, `LQL-P18`–`LQL-P24`,
`LQL-P28`–`LQL-P30`, `LQL-P32`–`LQL-P34`, `LQL-P36`, `LQL-P41`,
`LQL-S07`, `LQL-S09`–`LQL-S11`, and `LQL-Q03`, `LQL-Q04`.

Exit: transformations and statistics remain bounded, typed, cancellable, and
deterministic where promised. `block_stats` or `blocks_count` becomes `EXT`
only with direct-user utility and measured storage/decode savings.

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
