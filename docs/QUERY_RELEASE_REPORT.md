# Query and public-surface release report

Date: 2026-08-07

Release line: unreleased `0.4.0`

Measured implementation source: `cb47e4e9b6d17d848c9249117d1cd1e8f0c324cc`

<!-- query-release-matrix-summary: LQL deferred=7,shipped=107; MQL deferred=2,shipped=10; PQL deferred=5,experimental=11,shipped=74 -->
<!-- query-release-evidence: docs/evidence/2026-08-07_query_release_gate.json -->
<!-- query-release-workload-summary: metric_shapes=184 log_shapes=307 measured_iterations=24550 -->
<!-- query-release-fault-evidence: docs/evidence/2026-08-07_query_fault_gate.json -->
<!-- query-release-fault-summary: events=12 records_per_signal=30784 -->

## Verdict

The retained float-metric and rich-log models are release candidates for the
documented `0.4.x` source line. All 216 matrix rows have terminal dispositions:
191 are shipped, 11 stable-PromQL rows are deliberately experimental, and 14
rows are deferred behind named data-model, topology, or upstream-oracle
prerequisites. There are no `missing`, `partial`, or `in progress` rows.

This is a source-readiness verdict, not a publication event. No `v0.4.0` tag,
published archive, package, downstream dependency change, or Stack build was
created because those actions are outside this work. The disposable native
package used by the local installation gate is not a release. The
[artifact inventory](ARTIFACTS.md) states exactly what a later tag must
produce.

## Exact compatibility coverage

| language tier | total rows | shipped | experimental | deferred | compatibility claim |
|---|---:|---:|---:|---:|---|
| PromQL | 90 | 74 | 11 | 5 | Stable float-series behavior listed as shipped in the [PromQL matrix](PROMQL_FEATURE_MATRIX.md); experimental functions remain disabled with pinned diagnostics. |
| MetricsQL | 12 | 10 | 0 | 2 | Separately named compatibility tier; it never changes stable PromQL routes. |
| LogsQL | 114 | 107 | 0 | 7 | Rich-log behavior listed as shipped in the [LogsQL matrix](LOGSQL_FEATURE_MATRIX.md); unsupported syntax fails explicitly. |
| **Total** | **216** | **191** | **11** | **14** | Matrix status, tests, SQL recipes, and server advertisement are checked together. |

Every shipped row names a real-extension regression in the
[test-reference inventory](QUERY_TEST_REFERENCES.md). The 135 shipped rows
with an ordinary SQL target or foundation link 135 parameterized public-SQL
recipes; the Rust SQL runner executes 173 statements without private shadow
table access. A parser test or similarly named extension kernel is not counted
as language support.

The immutable upstream API corpora contain 549 Prometheus cases, 196
VictoriaMetrics cases, and 1,498 VictoriaLogs cases. Prometheus `v3.13.2` is
the stable PromQL authority, VictoriaMetrics `v1.148.0` owns only the separate
MetricsQL tier, and VictoriaLogs `v1.52.0` is the LogsQL authority. Exact
source commits and container digests are in the [oracle contract](QUERY_ORACLES.md).
Checked fixtures run locally against the real extension; refreshing them is a
separate, explicit operation against the immutable images.

## Experimental and deferred dispositions

Experimental PromQL behavior is intentionally disabled on the stable routes:

- `PQL-O17`, `PQL-R07`, `PQL-R21`, `PQL-F14`, and `PQL-H03` require a
  separately enabled `promql-experimental-functions` tier and matching oracle.
- `PQL-O18` requires the separate binary-fill feature gate and complete
  bounded vector-matching behavior.
- `PQL-R22` and `PQL-R23` already have executable public-SQL foundations, but
  remain disabled until the experimental PromQL tier exists.
- `PQL-F19` and `PQL-F20` require the upstream duration-expression feature;
  stable MetricsQL context behavior remains separately owned.
- `PQL-F21` additionally depends on marker-capable staleness and typed native
  histogram storage.

The retained model deliberately defers these rows:

- `PQL-S10` is not stable PromQL; its request-step behavior is shipped as
  `MQL-09` instead.
- `PQL-S17` requires bit-preserving stale-marker ingress and end-to-end query
  semantics that distinguish the marker from ordinary NaNs.
- `PQL-S22`, `PQL-O19`, and `PQL-H04` require a versioned native-histogram
  storage, batch, query, rollup, retention, and migration design.
- `MQL-08` is a finite twelve-name rejection catalog whose members need
  individual rows, oracles, and ownership decisions; it is not one feature.
- `MQL-11` requires a later immutable VictoriaMetrics tier or an explicitly
  Timeless-only contract because the pinned release rejects both functions.
- `LQL-F35`, `LQL-F36`, and `LQL-P50` require a versioned tenant/stream-field,
  canonical stream-ID, index, batch, and migration contract.
- `LQL-P10` and `LQL-P11` expose VictoriaLogs physical-block diagnostics.
  Shipping them requires a versioned public Timeless block-stat snapshot that
  can disclose stable logical facts without exposing private tables, paths,
  codec-specific field types, or mutable implementation details.
- `LQL-Q03` requires a query-internal parallel execution design. Independent
  server-reader admission is not equivalent to VictoriaLogs worker controls.
- `LQL-Q06` requires multiple fenced storage owners, deterministic bounded
  merge, failure classification, and response-completeness metadata. With one
  authoritative libSQL owner, `true` fails explicitly and `false` remains
  fail-closed.

TraceQL is not claimed. The later trace matrix should use stable row IDs and
separate selectors, parent/child/ancestor traversal, span/event/resource/scope
attributes, aggregates, structural operators, ordering, limits/cancellation,
and public SQL/storage foundations. Rich trace storage fidelity and existing
SQL discovery remain covered independently.

## Final performance evidence

The exact-build [final evidence](evidence/2026-08-07_query_release_gate.json)
has SHA-256
`ab028ebe23079cefe1d2b5b2063a865d588c65af81a6dc3255f8b5312241390e`.
It exercises 184 metric and 307 log query shapes for five warmups and 50
recorded iterations each: 24,550 measured loopback HTTP requests. Both release
servers and the loaded extension report the measured source commit above.

The required Session 0 comparison is below. Values are p50 / p95 / p99 in
milliseconds.

| signal/query | Session 0 | final | Session 0 cardinality / bytes | final cardinality / bytes |
|---|---:|---:|---:|---:|
| metrics exact/narrow | 0.354 / 0.821 / 0.984 | 0.884 / 1.042 / 1.215 | 1 / 166 | 1 / 164 |
| metrics wide selector | 2.825 / 3.259 / 3.261 | 3.248 / 3.822 / 4.996 | 512 / 54,521 | 512 / 53,497 |
| logs indexed/narrow | 6.253 / 6.990 / 7.443 | 11.652 / 13.355 / 14.297 | 1,024 / 174,625 | 100 / 27,228 |
| logs full/wide | 17.870 / 19.155 / 19.544 | 37.322 / 40.984 / 41.504 | 8,192 / 1,424,639 | 8,192 / 2,249,775 |

These figures preserve real regressions, but they are not a controlled
microbenchmark of parser overhead. The final metric database contains 3,163
series and 136,953 cumulative points after the resource-limit fixture versus
512 series and 16,384 points in Session 0. The final log fixture still has
8,192 entries, but its lossless payload is 2,143,279 wire bytes versus
1,318,143, and the narrow result now applies deterministic offset/limit 100
instead of returning 1,024 rows. The final process HWM also follows 491 query
shapes, not only the four rows above.

| signal | primary durable work | admission | durability barrier | logical storage | live SQLite/WAL/SHM | RSS HWM |
|---|---:|---:|---:|---:|---:|---:|
| metrics | 36,928 points | 12.339 ms | 88.908 ms | 224,688 B | 1,542,312 B | 50,824 KiB |
| logs | 8,192 entries | 14.214 ms | 40.146 ms | 1,914,055 B | 2,022,736 B | 101,252 KiB |

Metrics then adds the 100,025-point limit fixture and reaches 136,953
completed points with zero failed or queued work. Logs finishes with zero
queued or in-flight work. Its authoritative 8,192-entry batch becomes four raw
blocks. The HWM values are complete-process maxima across the whole suite, not
allocations attributed to one query. Cancellation regressions separately force
dropped work and prove reader reuse; the evidence capture ends with no active
cancellation.

The separate two-minute [fault evidence](evidence/2026-08-07_query_fault_gate.json)
has SHA-256
`0108df70b1258aa21b585ca5d582268af18677a4c302a28f9340248fc32cbb2e`.
It executes 12 scheduled events: descriptor/startup and disk-full recovery for
each signal, two slow-client disconnect/cancellation storms, overlapping
backups for all three signals, two graceful restarts, and two `SIGKILL`
restarts. Each signal accepts and durably completes 30,784 records across five
process generations with no reported failure and an `ok` final barrier. HWM
is 15,292 KiB for metrics, 47,192 KiB for logs, and 61,976 KiB for traces;
post-warmup long-running slope is zero for the final generations. This short
fault drill complements rather than replaces the already checked-in
authoritative two-hour release soak.

## Storage and query-boundary findings

The append-only [findings log](QUERY_STORAGE_FINDINGS.md) contains `QSF-001`
through `QSF-301`. The release-significant boundaries are:

- metric storage remains timestamp-plus-float64; millisecond evaluation,
  lookback, staleness, extrapolation, labels, timestamps, and result envelopes
  stay in the Rust API;
- public raw reads are inclusive, while PromQL open-left boundaries remain an
  explicit API/SQL predicate;
- exact float bits survive binary storage, but no ingress currently establishes
  end-to-end stale-marker meaning;
- rich logs retain all eight severities, microsecond release fidelity, and
  typed nested metadata; arbitrary transforms correctly pay bounded decode
  when no honest index exists;
- stream identity and physical-block diagnostics are not inferred from row
  fields or private shadow tables;
- every signal server obtains tier/block/index and optimizer-source accounting
  from public `timeless_stats` rows; only the extension names shadow objects;
- trace duration predicates use exact inclusive block extrema when present;
  legacy blocks remain exact through conservative decode until bounded public
  optimize backfills the small metadata rows without rewriting payloads;
- generation-2 trace reads honor SQLite projection and materialize rich values
  after pushed predicates; older formats retain the conservative full decoder,
  while large Jaeger response construction remains a separate API target;
- required hidden virtual-table inputs must be bound directly in each public
  scan because SQLite may reorder joins;
- `raw-series-v0` remains backward-readable but is not self-identifying;
  `TRF1` is the preferred versioned wide frame; and
- ordinary SQL/Rust composition remains preferred unless counters prove a
  material avoidable read, decode, allocation, copy, or row-crossing cost.

The query work did not repurpose existing storage formats or bypass the public
extension boundary. Capability negotiation is additive: extension data ABI 1,
SQL surface 1, and server schema ledger 1 remain the compatibility floor.

## Public documentation and artifacts

The current product contract is split deliberately:

- [SQL API reference](SQL_API_REFERENCE.md): every scalar, virtual table,
  hidden input, command, batch, frame, capability, timestamp, transaction, and
  embedding entry point;
- [server API reference](SERVER_API_REFERENCE.md): all three binaries, routes,
  configuration, auth, limits, lifecycle, errors, backup, and platforms;
- [SQL equivalents](QUERY_SQL_EQUIVALENTS.md): copyable parameterized
  statements using only public extension surfaces;
- [compatibility](COMPATIBILITY.md), [upgrade/rollback](UPGRADE.md), and
  [changelog](../CHANGELOG.md): version floors and operator transitions;
- [embedded Rust](EMBEDDED_RUST.md) and [sqld](SQLD.md): in-process and
  self-hosted libSQL boundaries; and
- [artifact guide](ARTIFACTS.md): checksums, manifest, SBOM, notices,
  installation, removal, backup, and rollback ownership.

The future tag must produce four native archives:
`x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`,
`x86_64-apple-darwin`, and `aarch64-apple-darwin`. Each contains the metrics,
logs, and traces binaries; matching extension; installer/remover; internal
checksums; artifact manifest; SPDX SBOM; project license; and third-party
notices. Static Rust embedding and direct libSQL 0.9.30 use the same three
production signal surfaces and prove cold reopen without the compatibility
spike.

## Higher-order Elixir interface recommendations

1. Keep Phoenix as the control plane for users, sessions, token issuance,
   authorization policy, tenancy, configuration, cluster administration,
   dashboards, and UI state.
2. Treat each signal-specific Rust process as the sole data owner. Elixir
   adapters should supervise, negotiate capabilities, forward authenticated
   requests, and surface readiness; they should not parse PromQL/LogsQL, read
   shadow tables, buffer authoritative batches, or silently fall back to a
   second storage path.
3. Generate or link language capability displays from the exact
   `timeless-libsql` version's matrices instead of maintaining another query
   list in Elixir.
4. Keep the three public signal APIs distinct. Extract only neutral process,
   capability, authentication-claim, and lifecycle clients; do not introduce a
   generic telemetry server solely to remove repeated names.
5. Offer direct SQL and the opt-in `embedded` Rust feature as first-class
   products for non-BEAM users. No Phoenix, NIF, or web UI should be required
   to use the storage/query extension.
6. When revisiting Elixir libraries, remove duplicate language evaluators only
   after their remaining control-plane behavior and public deprecation path are
   inventoried. Keep UI-facing saved queries, alerts, rules, and subscriptions
   above the Rust query boundary.

## Final gate inventory

The release gate is executed from the report branch and repeated after its
fast-forward into `main`. A session is not integrated if any applicable item
fails. The final handoff reports the resulting Git object ID because a commit
cannot embed its own hash.

| gate | result and evidence |
|---|---|
| matrix/document contracts | Pass: 52 Rust tests, including negative all-signal private-storage-boundary, final-report, fault-evidence, storage-finding, and tracked Markdown-table drift tests. |
| SQL cookbook | Pass: 135 recipes / 173 real-extension statements. |
| immutable oracles | Pass: manifest/image probes plus live Prometheus 549, VictoriaMetrics 196, and VictoriaLogs 1,498 cases. |
| root extension | Pass: format, all targets/tests, strict Clippy/Rustdoc, and separate dbhealth artifact/shell gate. |
| signal servers | Pass: all targets and unit/parser/API/storage/fidelity suites plus strict Clippy/Rustdoc. |
| real extension | Pass: 93 metrics and 84 logs contracts plus traces, OTLP, and Jaeger suites through the release `.so`. |
| fault and durability | Pass: transactions, rollback, corruption, five kill-9 CLI rounds, migration, cancellation, drain/reopen, the 12-event short fault artifact, and retained two-hour soak evidence. |
| embedding | Pass: static Rust example plus direct libSQL 0.9.30 multi-connection/cold-reopen gate. |
| packaging | Pass on native `x86_64-unknown-linux-gnu`: archive checksums, manifest, SPDX SBOM/notices, binary/extension identity, install, ownership-aware removal, and data/config preservation. Other documented targets still require native builds at publication time. |

The exact command results are recorded in the Session 21 completion entry of
the [implementation plan](2026-08-04_query_surface_implementation_plan.md).
