# Changelog

This file records public `timeless-libsql` extension, SQL, storage, and Rust
signal-server changes. Development-session documents are evidence, not a
substitute for this release history.

The repository follows semantic versioning while it is in the `0.x` series:
the minor component is the compatibility release line. The machine-readable
capability document remains authoritative for a particular binary pairing.
See the [compatibility statement](docs/COMPATIBILITY.md) and
[upgrade guide](docs/UPGRADE.md).

<!-- release-target: 0.4.2 -->

## [Unreleased]

### Added

- CLP-style template compression for rich log message columns
  (`CODEC_RICH_TEMPLATE`, codec byte 8; `CLP_PLAN.md`). `optimize()` now
  requests codec 8 for rich log groups: messages split into a per-block
  template dictionary plus typed variable columns (numeric variables ride
  the same pco/zstd encoders as every other column). Every block is
  measured against the codec-7 encoding and silently falls back when
  templates lose, so no block is ever larger than before. Real-corpus
  whole-block wins: 1.29–2.44× on message-dominated rich blocks, ~1.03×
  when the metadata envelope dominates. Ingest/flush is unchanged; all
  prior codecs remain decodable.

## [0.4.2] — 2026-08-08

### Fixed

- Completed the tag release path: after all four native package jobs and the
  aggregate checksum gate pass, the workflow now re-verifies the complete
  asset set and publishes the archives plus `SHA256SUMS` as a GitHub Release.
  Write permission is scoped only to the final publication job.

### Compatibility

- Storage data ABI 1, SQL-surface generation 1, batch/frame formats, and the
  minimum `0.4.0` extension/server pairing remain unchanged. This patch only
  advances package identity and release publication behavior.

## [0.4.1] — 2026-08-08

The tag points at `d14a59503d2f54f608a6d424dca3d62b31d9ce34`. Its
tag-triggered artifact run built and uploaded all four intended Linux/macOS
archives and the complete outer checksum set. These are workflow-retained
artifacts; the workflow did not create a GitHub Release.

### Added

- A dedicated deferred-work index covering every non-shipped PromQL,
  MetricsQL, LogsQL, trace-query, and TraceQL row by stable ID, with links to
  its authoritative matrix, regressions, SQL recipes, evidence, findings, and
  resumption prerequisites.

### Changed

- Reworked the root and signal-server READMEs as current product entry points
  while retaining the complete 216-row query record in the maintained
  matrices and evidence documents.
- Updated compatibility, artifact, upgrade, testing, server, and release
  documentation to distinguish source tags, workflow candidates, and complete
  published artifact sets.
- Linked the release tool against its own bundled SQLite so macOS artifact
  identity verification does not depend on Apple's restricted system SQLite.

### Compatibility

- Storage data ABI 1, SQL-surface generation 1, batch/frame formats, and the
  minimum `0.4.0` extension/server pairing remain unchanged. This patch does
  not intentionally remove or reinterpret a public storage or query surface.

## [0.4.0] — 2026-08-08

`v0.4.0` was tagged from `main` at commit
`512f995a038fb4d9ac21ffc4df3df2d3b5b1a217`. Its manually dispatched query
and production gates passed. The tag-triggered artifact run produced and
validated both Linux candidate bundles, but both macOS jobs failed while
linking the release tool against Apple's restricted system SQLite. The
incomplete matrix did not produce the outer checksum set, and no GitHub
Release was published. See the [artifact guide](docs/ARTIFACTS.md) for the
current distribution status; the tag records source, not a complete binary
release.

### Added

- Three standalone Rust signal servers for metrics, logs, and traces, with
  bounded queues/readers, loopback TCP defaults, explicit authentication,
  request/query/resource limits, graceful drain, verified backup, build
  identity, and fail-closed extension/schema negotiation.
- `timeless_capabilities()` with data ABI 1, SQL-surface generation 1,
  exact signal batching/fidelity declarations, public query/result-format
  capabilities, and a source-checked SQL module inventory.
- Stable PromQL float-series evaluation and an explicitly separate MetricsQL
  compatibility tier in the metrics server. Coverage and intentional
  differences are row-addressed in the public feature matrices.
- Strict LogsQL parsing, filtering, transformations, statistics, sorting, and
  pipelines over rich logs. Unsupported syntax fails explicitly instead of
  disappearing between parser and storage.
- Public storage-aware SQL surfaces for packed raw/latest/aggregate/window/
  rollup metrics, bounded log count/value/query statistics, and trace
  service/operation/time-bucket discovery.
- Additive trace-block duration extrema with inclusive pruning, conservative
  legacy decode fallback, bounded metadata-only optimize backfill, capability
  negotiation, and public coverage/work statistics.
- Opt-in exact typed trace-attribute equality for up to eight configured
  span, resource, or instrumentation-scope JSON Pointers. Fixed-size
  per-block negative filters avoid false negatives, surviving spans are
  rechecked exactly, legacy missing metadata decodes conservatively, and
  public stats expose field/row/byte cost.
- Executable SQL equivalents for every matrix row that honestly has a public
  SQL foundation, plus pinned Prometheus, VictoriaMetrics, and VictoriaLogs
  semantic oracles and measured query evidence.
- Rich log batch/codec versions preserving all eight severities,
  microsecond timestamps, and typed nested metadata, while retaining legacy
  log formats.
- Rich trace batch/codec versions preserving parents, kind/status and status
  description, typed attributes, events, resource, and instrumentation scope,
  while retaining legacy span formats.
- An executable in-process Rust embedding mode that registers the same three
  production SQL/storage/query surfaces without HTTP, a NIF, or a sidecar,
  plus a direct libSQL gate covering typed logs, complete rich spans,
  multi-connection reads, and durable reopen.
- Canonical source-checked SQL, server, compatibility, upgrade, embedded-Rust,
  sqld, and artifact/install references that distinguish unreleased source
  capability from actually published artifacts.
- A source-checked final query release report with exact matrix dispositions,
  pinned-oracle coverage, Session 0 versus final evidence, storage findings,
  artifact inventory, deferred prerequisites, and higher-order Elixir
  interface recommendations. Rust contracts keep the append-only finding IDs,
  table structure, terminal statuses, and reported range synchronized and
  reject inconsistent columns in every tracked Markdown table.

### Changed

- Advanced extension and server source versions together from the tagged
  pre-handshake `0.3.0` line to `0.4.0`. The storage data ABI
  remains 1; this prevents an old `v0.3.0` artifact from masquerading as a
  compatible release-server peer.
- Kept PromQL, MetricsQL, and LogsQL parsing in the Rust signal APIs. The
  SQLite extension exposes reusable storage pruning, reductions, packed
  results, and work guards rather than language-specific syntax.
- Made query work, result cardinality, response bytes, cancellation, and
  deadlines bounded before expensive decode/materialization where the public
  surface can enforce them.
- Removed signal-server reads of private shadow tables. Servers now use only
  public virtual tables, scalars, commands, capabilities, and stats. Metrics
  and traces now obtain index allocation and trace optimizer-source sampling
  from additive `timeless_stats` rows, matching the existing logs boundary.
- Preserved extension-owned authoritative batching: 4,096 metric points per
  series, 8,192 log entries, and 8,192 rich spans.
- Split the default loadable-extension feature from opt-in linked Rust
  embedding. The default `.so`/`.dylib` ABI is unchanged; embedded hosts use
  `default-features = false, features = ["embedded"]`.

### Compatibility

- Existing batch/frame magics retain their meanings. New rich batches and
  packed frames use distinct advertised versions; the unversioned
  `raw-series-v0` result remains readable but is not self-identifying.
- A pre-ledger SQLite telemetry database is schema 0. A `0.4.x` signal writer
  adds the idempotent schema-ledger version 1 row only after extension and
  database preflight succeeds.
- A tagged `v0.3.0` extension is not a valid peer for the `0.4.x` Rust signal
  servers because it predates `timeless_capabilities()`. Replace the extension
  and matching servers together.
- Native histogram storage and VictoriaLogs stream identity remain deferred
  until explicit typed storage designs exist. TraceQL is not part of this
  release line.

### Fixed

- Eliminated full trace-block decode for duration filters whose inclusive
  bounds prove that no persisted block can match, while retaining exact legacy,
  rollback, cold-reopen, corruption, and rich-span behavior.
- Corrected query boundary, lookback, staleness, reset/extrapolation,
  IEEE-754, label/name, timestamp, ordering, warning/info, typed-value,
  cancellation, durability, and cold-reopen defects as recorded—without
  deletion—in [the storage findings log](docs/QUERY_STORAGE_FINDINGS.md).
- Corrected public documentation that implied an unavailable Unix-socket
  server transport or an unimplemented default trace-retention duration.
- Corrected a public Rust embedding API that compiled in loadable-extension
  mode but could not initialize SQLite in an ordinary host, and replaced a
  compatibility-spike libSQL smoke with complete production-signal coverage.
- Removed floating sqld `main`/`latest`, private-shadow inspection, implicit-
  transaction, and unbounded replication claims from the deployment guide.

## [0.3.0] — 2026-07-30

Added the public SQL query tier: complete window reductions for the retained
float model, trace duration percentiles, metric label matchers and value
discovery, gap filling, and the first machine-executed query cookbook.

This tag predates the release-server capability handshake. Its semantic
version must not be used alone to select a peer for the `0.4.x` servers.

## [0.2.0] — 2026-07-26

Added storage-aware query kernels and introspection, automated retention and
metric rollups, public batch blobs for all three signals, log trigram search,
and the first verified filesystem-store importer waist.

## [0.1.1] — 2026-07-26

Hardened statement atomicity, savepoints, multi-process series identity,
attached schemas, transactional drop, filesystem compaction, deadlock
avoidance, extreme timestamps, and performance parity.

[Unreleased]: https://github.com/awksedgreep/timeless-libsql/compare/v0.4.2...HEAD
[0.4.2]: https://github.com/awksedgreep/timeless-libsql/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/awksedgreep/timeless-libsql/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/awksedgreep/timeless-libsql/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.3.0
[0.2.0]: https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.2.0
[0.1.1]: https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.1.1
