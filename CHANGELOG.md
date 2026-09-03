# Changelog

This file records public `timeless-libsql` extension, SQL, storage, and Rust
signal-server changes. Development-session documents are evidence, not a
substitute for this release history.

The repository follows semantic versioning while it is in the `0.x` series:
the minor component is the compatibility release line. The machine-readable
capability document remains authoritative for a particular binary pairing.
See the [compatibility statement](docs/COMPATIBILITY.md) and
[upgrade guide](docs/UPGRADE.md).

<!-- release-target: 0.7.9 -->

## [Unreleased]

### Added

- **Observability schema v1: friendly companion views install with every
  signal table.** Creating a `timeless_metrics`, `timeless_logs`, or
  `timeless_traces` virtual table now installs a family of public views in
  the same transaction, so ordinary SQL users get stable, readable shapes
  without reproducing expert recipes (MySQL Performance Schema spirit;
  #20, #21, #27). Naming is deterministic — `timeless_<source>_<kind>` —
  and every object is recorded with its schema version and a human
  description in a shared `timeless_schema_inventory` table, which is the
  source of truth for removal and the capability query for what is
  installed. Traces: `_spans` (hex IDs, native plus human-readable UTC
  timing), `_summary` (per-trace counts, envelope timing, root
  rows/state, ordered service set, `completeness` always `'unknown'` —
  the exact `TSQ-04`/`TSQ-05` retained-snapshot semantics), `_services`,
  `_operations`, `_errors`, `_roots`. Logs: `_entries` (verbatim typed
  metadata, unit-baked friendly time), `_services` and `_fields` (only
  for declared index keys). Metrics: `_series` (catalog reads only) and
  `_latest` (exact arg-max join; tied timestamps return all tied rows).
  Every body reads only public surfaces — no shadow tables, no new
  evaluators — and is validated against the object catalog in
  `docs/OBSERVABILITY_SCHEMA.md` by the query-contract harness.

### Changed

- **Companion-view lifecycle.** Installs are collision-safe (a foreign
  object at a derived name fails the `CREATE` before anything is
  touched), idempotent, and rollback-safe; `DROP TABLE` removes exactly
  the objects the inventory attributes to that source. Opening a
  database refreshes stale definitions best-effort — per object, by
  version — so releases migrate views automatically with no separate
  upgrade step; read-only opens keep working with what is installed,
  and stale shapes fail loudly at query time rather than returning
  wrong rows. View bodies never schema-qualify their sources, so
  installed views survive direct opens, backup copies, and `ATTACH`
  under any alias. All three signal vtabs are now marked Innocuous
  (metrics already was), so companion views resolve under
  `trusted_schema=off` in a stock `sqlite3` shell. Traces and logs
  base-table reads through the views are vtab scans with their existing
  pushdowns; `timeless_capabilities()` gains an `observability_schema`
  key. Lifecycle and compatibility policy:
  `docs/OBSERVABILITY_SCHEMA.md`.

## [0.7.9] — 2026-09-02

### Added

- **`timeless_stats` reports block ts-span shape for logs and traces.**
  New keys `block_mean_ts_span`, `block_max_ts_span` and
  `block_over_target_count`. Block pruning is by ts range, so block WIDTH —
  not block count — decides how many blocks a range query must decode.
  Nothing in the stats surface reported width, so a pruning regression was
  invisible while block counts, byte totals and the optimize counters all
  looked healthy. A rising `block_over_target_count` means the
  compressed-merge path is re-merging already-compressed blocks. Additive:
  existing consumers select stats keys by name.

### Fixed

- **Incremental log compaction no longer welds time-distant blocks
  together.** `plan_compressed_segment` sorts candidates by entry count so
  similar-sized tiers pair up and satisfy the 2x growth rule; that pairing
  is blind to time, and merging two blocks an hour apart produced one block
  spanning the union of their ranges, gap included. Range pruning then paid
  that width on every overlapping query. Open-window merges now also
  require the merged span to stay within `merge_span_growth_limit` (2x) of
  the span the sources actually cover. Closed windows remain exempt and
  still coalesce unconditionally, so a straggler is deferred to its
  window's close rather than stranded. On the 1M-entry `bench-logs`
  fixture: mean block span 198,935 ms -> 55,777 ms, max 2,948,717 ms ->
  98,635 ms, blocks over target 15 -> 0, the service+level+range query
  15.9 ms -> 6.9 ms, and `optimize` 263.8 ms -> 29.6 ms, for +0.7%
  storage (9.03 -> 9.09 B/entry). Ingest throughput is unchanged. Traces
  is deliberately untouched: `SpanEngine` has no closed-window exemption,
  so the same guard could strand blocks there, and its queries never
  regressed.

## [0.7.8] — 2026-08-30

### Changed

- **All three signal servers now bound the WAL with the same embedded
  profile.** Every writer connection sets `wal_autocheckpoint = 1000` — a
  passive checkpoint attempt near ~16 MiB of WAL on metrics and traces
  (16 KiB pages) and ~4 MiB on logs (default 4 KiB pages) — replacing the
  metrics/traces value of 10000 and the logs writer's inherited SQLite
  default, plus `journal_size_limit = 67108864` so checkpoints truncate the
  WAL file back to at most 64 MiB instead of leaving it at its high-water
  size. Both are per-connection settings applied on the writer's open path
  every boot (the logs store-creation connection gets the same two bounds).
  The logs writer additionally gains the `cache_size = -128000`,
  `mmap_size = 2147483648`, and `temp_store = MEMORY` tuning the metrics and
  traces writers already had, so the three planes' writer profiles match.
- **Each server runs a periodic truncating checkpoint.** A new maintenance
  task sends a `WalCheckpoint` command through the writer queue every 300 s;
  the writer executes one best-effort `PRAGMA wal_checkpoint(TRUNCATE)`
  (shared `periodic_wal_checkpoint` in `timeless-api-common`), bounded by
  the connection's existing 5 s busy timeout. A complete pass is silent; a
  busy pass surfaces its `(busy, log, checkpointed)` counts through the
  maintenance-error log line and is retried on the next interval. Previously
  no plane truncated the WAL outside the backup and shutdown paths. Server
  behavior only; no extension, SQL, or data-ABI change. GUIDE §8 now
  documents the recommended embedded WAL profile and checkpoint cadence.
- Storage virtual tables and raw table-valued functions now push a plain SQL
  `LIMIT` down as a scan bound when no offset or residual filter can require
  more rows. Small exploratory queries no longer decode an entire matching
  range merely to discard it above the virtual-table boundary.
- Authentication route classification is explicit, including read scope for
  both live-tail POST routes, and admin keys supplied in query parameters are
  decoded before comparison.
- The extension FFI panic policy is documented: exported callbacks contain
  Rust panics at the SQLite boundary, while invariant-only internals remain
  fail-fast during development.

### Fixed

- Hardened untrusted-input paths: batch reservations and string decoding are
  allocation-bounded, batch byte arithmetic is checked on 32-bit targets,
  and batch result ordering is validated in release builds before results are
  associated with requests.
- Log virtual tables now reject the wrong shadow-table module reliably, and
  trace attribute filtering handles stored JSON `null` without confusing it
  with a missing key.
- Dropping storage pins now reclaims retired blocks promptly instead of
  waiting for unrelated later work.
- Legacy schemas are upgraded through explicit supported transitions rather
  than being silently accepted by newer code.
- Spike shadow-table identifiers are quoted safely, and dbhealth maintenance
  has a single scheduler owner with canonical database-path matching.
- CI actions are pinned to immutable revisions, and new Rust compiler
  warnings in the batch path have been removed.

## [0.7.7] — 2026-08-23

### Added

- `timeless_stats` gained a persistent `ingest_raw_bytes_total` key for logs
  and traces: the lifetime logical row bytes made durable by flushes (ts +
  level + message + metadata per entry; 50 fixed bytes plus every string
  field per span — the demogen ground-truth definition). It accrues in the
  same transaction that first persists the entries' blocks, so a rolled-back
  ingest never counts, and optimize, merges, retention, and reopen never move
  it. Additive stats key; no data-ABI change. Metrics deliberately has no
  counter — its raw side is exactly `16 × total_points`.

- **All three signal servers now export the honest storage split.** Each
  plane's `/metrics` exposition and stats JSON publish, as separate series:
  `timeless_<signal>_storage_bytes` (engine data-block payload only),
  `timeless_<signal>_index_bytes`, `timeless_<signal>_wal_bytes`, and a raw
  comparator — `timeless_metrics_raw_ingested_bytes` (16 B x total points)
  and `timeless_logs|traces_raw_ingested_bytes_total` (the engine's
  persisted ingest-raw counter). Logs and traces additionally export their
  persisted `compression_input|output_bytes_total`. A compression ratio is
  raw versus storage bytes; index, WAL, freelist, and whole-file sizes are
  operational series and never part of one. Additive gauges; storage
  contract tests reconcile every value against the public `timeless_stats`
  surface. (Pre-existing `*_disk_size_bytes` / `*_index_size_bytes` gauges
  are unchanged for scraper compatibility.)

- `tools/demogen`: on-demand synthetic telemetry for demos — a detached
  CLI plus a `libtimeless_demogen` loadable extension driving seed /
  tick / follow / report from a sqlite3 prompt through the public Tier 2
  batch surface. Repo tooling only; not part of the release artifacts.

### Fixed

- With auth enabled, `POST` to the live-tail routes (`/select/logsql/tail`,
  `/select/timeless/api/spans/tail`) now requires the `:read` scope its GET
  twin and the documentation already claimed, instead of falling through the
  path heuristics to `:write`. A tail streams admitted data; its POST form
  only carries the filter parameters.

## [0.7.6] — 2026-08-20

### Added
- **Traces gained a live tail**: `GET|POST /select/timeless/api/spans/tail`
  streams admitted spans as `application/x-ndjson`, the streaming twin of
  `/select/timeless/api/spans`. Logs have had one since the query surface
  landed; traces had no route and no hub behind one, so a subscriber had
  nothing to attach to and simply received nothing — indistinguishable from a
  system producing no spans.

  Filters are the search surface's live-matchable parameters under the same
  names — `service`, `name`, `kind`, `status` — so one filter moves between a
  search and a tail unchanged. Time bounds and paging are deliberately absent:
  a live stream is already bounded by now, and there is no page to skip to.
  `attributes` additionally takes a JSON object of span attributes that must
  all match exactly, which is how a stream pins itself to one host.

  Matching happens server-side, per subscriber, before serialisation. A row
  carries the dashboard row shape plus `service`, so a streamed span and a
  searched one describe the span identically. A `kind` or `status` outside the
  enumerated set, or an `attributes` value that is not a JSON object of
  scalars, is rejected rather than ignored: accepting `kind=srever` would
  stream nothing and look exactly like a system with no matching spans.

  Spans publish only once storage has durably accepted the batch, so a
  subscriber never sees a span a search would not return. Slow consumers drop
  spans rather than backpressuring ingest.

- `timeless_traces_tail_spans_sent_total`,
  `timeless_traces_tail_spans_dropped_total` and
  `timeless_traces_tail_active_subscribers` in the traces `/metrics`
  exposition — what distinguishes a quiet system from a saturated subscriber.

## [0.7.5] — 2026-08-20

Recorded after the fact; the release itself carried no changelog entry.

### Fixed
- The metrics scrape endpoint reported `samples: 0` for every successful
  scrape. Samples are now counted from the body before it moves into the
  ingest queue, so the number reflects what was actually scraped.

## [0.7.4] — 2026-08-15

Deploy verification of v0.7.3 on production caught the CLP pruning work
landing one layer too low for LogsQL, plus a long-standing merge-planner
flaw the investigation surfaced. Same-day patch, verified against a
production backup before tagging.

### Fixed

- LogsQL word/phrase filters now reach the storage layer: a word-bounded
  match implies substring containment, so the phrase (or the longest
  message literal a predicate's top-level conjunction requires) rides
  the `message_contains` pushdown as a pruning superset while the exact
  word-boundary postfilter keeps its semantics. Previously every LogsQL
  text filter decoded the full window regardless of storage-layer
  pruning — the issue #2 failure shape at a different layer.
- Closed-window final compaction: low-volume level partitions could
  never reach the merge fill floor, stranding trickle blocks in every
  closed hour forever (a production store had 7,655 stranded ~20-entry
  blocks, growing ~2,400/day). A window that ended a full merge span
  before the store's newest data now coalesces unconditionally; open
  windows keep the anti-amplification guards. The production store
  converged 8,030 -> 709 blocks in two optimize passes.
- `message_contains` absence is now provable on every codec: non-template
  blocks scan their self-contained message column alone (no timestamp,
  level, or metadata work), so absent needles prune codec-1/2/4/5/6/7
  blocks too. On the production backup with the default 100k work cap:
  a 6-day absent-needle query went from erroring to 0 entries decoded in
  436ms with all 709 blocks pruned, and decoded work now equals matched
  rows for real needles.

## [0.7.3] — 2026-08-15

### Added

- Prometheus self-metrics: `GET /metrics` on each data plane's own port
  serves the `/health` operational stats in text exposition format
  (version 0.0.4) — counters with `_total` suffixes, gauges for storage,
  queue, and file accounting, and a `timeless_build_info` info-metric
  carrying the plane's build identity (name, version, commit, target,
  profile) so version drift between environments is visible on any
  Prometheus-compatible dashboard. The endpoint is exempt from
  authentication exactly like the probe endpoints; it exposes the same
  data `/health` already serves unauthenticated. New stat surface only —
  no new instrumentation.

- Grouped aggregation: `stats by (fields...)` in LogsQL — one output row
  per distinct group-value tuple, every existing stats function supported
  (the grouped path partitions rows and runs the ungrouped kernel per
  partition, so semantics are identical by construction). Group output is
  ordered lexicographically by group values; combine with
  `first N by (alias desc)` for top-N. Group cardinality is bounded by
  the existing state-item and state-byte limits.
- Logs store policy without SQL: `TIMELESS_LOGS_INDEX_KEYS` (create new
  stores with the allowlist; reindex an existing store once at startup
  when it differs) and `TIMELESS_LOGS_RETENTION` (`<n>[s|m|h|d]`, unit
  suffix required; applied to new and existing stores). New extension
  command `retention:<n[s|m|h|d]>` persists and applies a retention
  window on a live logs store; shadow-meta reads now accept TEXT or
  BLOB representations of the same value.

### Changed

- Trigram message-index upgrade path: a store that did NOT opt into
  `message_index='trigram'` sheds any `tg:` postings automatically at its
  first optimize after upgrade — opt-in is the only way to carry the
  index's storage cost. Opted-in stores are untouched and now compose
  with CLP-dictionary pruning. New command `message_index:<none|trigram>`
  makes the opt-out (immediate posting drop, ~27% of index weight on
  measured corpora) and re-opt-in explicit.
- `message_contains` queries prove absence and skip decode work using the
  CLP dictionaries codec-8 blocks already store. Block-level: a needle
  that cannot occur in a block (template text is digit-free; every
  variable carries a digit; token-confined needles like IPs and hex ids
  resolve as full literals against the Str-variable dictionary) skips the
  block entirely. Sub-block: rows whose template provably cannot render a
  matching message advance the decode cursors without materializing
  strings, and rich metadata parses only for rows that actually match.
  `max_work_entries` now charges the rows actually decoded, so
  wide-window selective queries succeed under budgets far smaller than
  the window. Measured on a 2.1M-entry production corpus: every needle
  class 3–12x faster, zero storage cost. New `timeless_stats` keys:
  `query_clp_pruned_blocks`, `query_clp_skipped_rows`.

## [0.7.2] — 2026-08-12

### Added

- Live tail for logs: `GET/POST /select/logsql/tail` streams admitted
  entries as NDJSON, filtered by one LogsQL filter expression (the full
  filter vocabulary — `host:x`, boolean combinations, query-backed lists
  resolved once at subscribe). VictoriaLogs-compatible endpoint shape.
  Fan-out is in-memory from the ingest admission path; slow consumers drop
  entries rather than backpressuring ingest, with
  `tail_active_subscribers` / `tail_entries_sent` / `tail_entries_dropped`
  exposed in stats. The extension is untouched — this is server-only.

## [0.7.1] — 2026-08-12

The `v0.7.0` tag records source only: its artifact run failed the release
tool's binary identity check because `timeless-authctl` — newly added to
the bundle inventory — did not implement the `--version` JSON contract the
other binaries speak (the v0.4.0/v0.6.3 precedent: the bad tag stays, the
fix is the next patch). authctl now emits the same identity document, built
from the same build-script env, and the packaging step is exercised locally
before tagging.

## [0.7.0] — 2026-08-12 (source-only tag)

This minor selects a new compatibility line because a documented public
surface changed intentionally: the signal server binaries no longer require
authentication by default (AUTH_OPT_IN_PLAN.md, implemented in full). Per
the pre-1.0 policy the pairing floors move with the line — the 0.7.0
extension floors servers at 0.7.0 and the 0.7.0 servers floor extensions at
0.7.0. Storage data ABI 1 and all batch/frame formats are unchanged; reading
older databases is unaffected.

### Changed

- The three signal server binaries now start with authentication **disabled**
  by default, matching the library `Config::default()` and every comparable
  telemetry server. Set `TIMELESS_AUTH_MODE=required` with
  `TIMELESS_AUTH_POLICY_FILE` to enable token verification. Previously an
  unset `TIMELESS_AUTH_MODE` required a policy file and the binary exited
  with code 2 without one. **Operators who relied on the previous default
  must now set `TIMELESS_AUTH_MODE=required` explicitly.** Deployments that
  already set it explicitly — including every `timeless_stack` deployment —
  are unaffected.
- `/ready` and `/health` no longer require a token, so container and load
  balancer probes work without minted credentials. `/live` was already
  exempt.

### Added

- `timeless-authctl`: Ed25519 keygen, policy scaffolding, and token minting,
  so enabling auth no longer requires implementing a JWS signer or running
  the Elixir control plane. A round-trip test pins the minted tokens to the
  servers' verifier.
- `ClaimLimits` and the optional policy fields now have defaults, so a
  policy file need only state the limits it wants to lower; tokens may omit
  the `limits` block entirely.
- `TIMELESS_ADMIN_KEY`: when set, the administrative routes (scrape target
  management, backup, flush, optimize) additionally require it, independent
  of `TIMELESS_AUTH_MODE` — ingest and query stay open while administration
  closes, following the VictoriaMetrics authKey precedent.

### Fixed

- `GET /api/v1/scrape/targets` no longer returns stored scrape bearer tokens
  or basic-auth passwords. The response reports whether credentials are
  configured without disclosing them.
- Scrape targets can no longer be pointed at link-local addresses (including
  cloud instance metadata), and scrape connections are pinned to their
  validated resolved addresses, closing a server-side request forgery path.
- Backup destinations are confined to a backup root (`TIMELESS_BACKUP_DIR`,
  defaulting to `backups/` beside the database file).

## [0.6.4] — 2026-08-11

The `v0.6.3` tag records source only: its artifact run failed on every
platform because the version bump shipped without refreshed lockfiles and
the release builds use `--locked` (the v0.4.0 precedent — the bad tag
stays, the fix is the next patch). This release is that source plus the
lockfile refresh and the documentation the contract gate flagged:
`docs/COMPATIBILITY.md` pairing keys had been left at the 0.5 line through
three 0.6 releases, and `timeless_pins` was registered without an inventory
row. `docs/RELEASING.md` now records the full pre-tag checklist.

Also defuses a time bomb in the production fault gate: fixture timestamps
were offsets from a hardcoded 2026-08-02 base while the metrics server
prunes at wall-clock now minus 7 days, so from 2026-08-09 the gate failed
everywhere with "overlap snapshot outside admission window" on any commit —
including tags that had passed it before. The base is now the most recent
UTC midnight (alignment and determinism preserved; data at most 24h old).

### Fixed (as unreleased `0.6.3`)

### Fixed

- Compression totals now credit merge passes on the output side
  (`out += merge_out - merge_in`, input unchanged). The 0.6.2 accounting
  excluded merges entirely, freezing the reported ratio at the first-pass
  compression of trickle-sized blocks — low-traffic stores displayed ~1.3x
  while their data sat at its true (order-of-magnitude better) density.
  The output total now tracks the current compressed footprint, so the
  ratio converges to the store's real figure as merges consolidate blocks.
  Transition note: merging blocks compressed before 0.6.2 subtracts bytes
  never added; a saturating floor bounds the skew and retention ages the
  affected blocks out.

## [0.6.2] — 2026-08-11

### Added

- Durable compression totals: each optimize pass persists cumulative
  raw-bytes-in / compressed-bytes-out in the store's `_meta`, in the same
  host transaction as the block swap (raw-compression phase only — merge
  passes re-read compressed input and would distort the ratio).
  `timeless_stats` exports them as `compression_input_bytes_total` /
  `compression_output_bytes_total` for logs and traces, and the signal
  servers expose matching fields. The process-local `optimize_raw_*`
  profile counters reset on restart, so a compression-ratio display backed
  by them read "pending" on a fully compressed store after every restart.
  Pre-upgrade stores start counting from their next optimize.

## [0.6.1] — 2026-08-11

Both workspaces move to the 0.6 compatibility line together: the v0.6.0
extension tag had left the signal servers on 0.5.0, and the capability
baseline moved extension-side only. Servers now version 0.6.1 and floor
extensions at 0.6.0; the extension keeps its 0.6.0 server floor.

### Added

- Auto-optimize in the logs and traces block engines: every 30th flush
  call (including empty heartbeat flushes) consults the exact optimize
  planner and runs one budgeted pass (32,768 entries) when it finds
  actionable work; a raw backlog at or past the budget triggers the pass
  on the same flush instead of waiting out the interval. An extension has
  no timer of its own, so maintenance now rides the one call every host
  already makes on a heartbeat — previously a host that only ever issued
  `flush` (the embedded Elixir engines) accumulated raw blocks forever,
  paying full-size storage and raw-scan query costs until retention
  deleted the data uncompressed. Hosts that schedule `optimize`
  externally (the API services) are unaffected beyond finding an emptier
  backlog. Engine-level opt-out: `auto_optimize_interval_flushes: 0`.
- `timeless_stats('traces')` now exports `compressed_blocks`, `raw_bytes`,
  and `compressed_bytes` (additive), matching the keys the logs stats
  already had — embedded hosts read these to display compression state.

## [0.5.0] — 2026-08-08

The tag points at `daabaf7d39e867be551e04f8a315f130fbe8fd27`. Its
tag-triggered artifact run (`31285260705`) built, identity-checked,
install/remove-drilled, and uploaded all four native archives, produced and
verified the complete outer checksum set, and published the `v0.5.0` GitHub
Release with the four archives plus `SHA256SUMS` as permanent release assets.

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

### Compatibility

- This minor selects a new compatibility line because of stored-data
  forward compatibility: after this extension runs rich-logs `optimize()`,
  the database contains codec-8 blocks that pre-`0.5.0` extensions refuse
  to decode (they fail loudly, matching the codec-6/7 posture). Reading
  older databases is unaffected — every prior codec stays decodable, and
  no migration runs at open.
- Storage data ABI 1, SQL-surface generation 1, and the batch/frame
  formats are unchanged. The extension/server pairing floors advance with
  the line, as the pre-1.0 policy requires: the `0.5.0` extension floors
  servers at `0.5.0` and the `0.5.0` servers floor extensions at `0.5.0`,
  so a compatibility set upgrades as one unit. Servers load the extension
  in-process, so any handshake-accepted pairing decodes codec 8 through
  the loaded extension; the rollback caveat is a pre-`0.5.0` extension
  pointed at a database already optimized by `0.5.0` (see the upgrade
  guide).

## [0.4.2] — 2026-08-08

The tag points at `abb3787ffba54b2c02dc2e43a6b9e2d6c371bc91`. Its
tag-triggered artifact run (`31281301520`) passed all four native package
jobs and the aggregate checksum gate and published this repository's first
GitHub Release, validating the release path fixed below.

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

[Unreleased]: https://github.com/awksedgreep/timeless-libsql/compare/v0.7.6...HEAD
[0.7.9]: https://github.com/awksedgreep/timeless-libsql/compare/v0.7.8...v0.7.9
[0.7.8]: https://github.com/awksedgreep/timeless-libsql/compare/v0.7.7...v0.7.8
[0.7.7]: https://github.com/awksedgreep/timeless-libsql/compare/v0.7.6...v0.7.7
[0.7.6]: https://github.com/awksedgreep/timeless-libsql/compare/v0.7.5...v0.7.6
[0.7.5]: https://github.com/awksedgreep/timeless-libsql/compare/v0.7.4...v0.7.5
[0.7.4]: https://github.com/awksedgreep/timeless-libsql/compare/v0.7.3...v0.7.4
[0.7.3]: https://github.com/awksedgreep/timeless-libsql/compare/v0.7.2...v0.7.3
[0.7.2]: https://github.com/awksedgreep/timeless-libsql/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/awksedgreep/timeless-libsql/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/awksedgreep/timeless-libsql/compare/v0.6.4...v0.7.0
[0.6.4]: https://github.com/awksedgreep/timeless-libsql/compare/v0.6.2...v0.6.4
[0.6.2]: https://github.com/awksedgreep/timeless-libsql/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/awksedgreep/timeless-libsql/compare/v0.6.0...v0.6.1
[0.5.0]: https://github.com/awksedgreep/timeless-libsql/compare/v0.4.2...v0.5.0
[0.4.2]: https://github.com/awksedgreep/timeless-libsql/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/awksedgreep/timeless-libsql/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/awksedgreep/timeless-libsql/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.3.0
[0.2.0]: https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.2.0
[0.1.1]: https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.1.1
