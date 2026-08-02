# Rust telemetry data-plane release-promotion plan

Date: 2026-08-02  
Status: Sessions 0–1 complete; implementation sessions 2–8 pending
Branch: `release/rust-telemetry-data-plane`

This plan promotes the completed metrics, logs, and traces Rust API POCs into
the default production data plane. It is not an opt-in preview plan. Existing
installations are converted automatically and crash-safely; fresh
installations start on libSQL. Phoenix remains the product and cluster control
plane, and three signal-specific Rust executables remain the data plane.

The storage waist is fixed:

```text
Phoenix control plane
users, sessions, token issuance, policy, tenancy, configuration,
cluster administration, dashboards and UI state
                         |
             loopback HTTP / Unix socket
                         v
   timeless-metrics-api | timeless-logs-api | timeless-traces-api
   auth enforcement, bounded admission, parsing, query/response work,
   cancellation, retention/resource limits, operational telemetry
                         |
                public SQL/batch surfaces
                         v
   timeless_metrics | timeless_logs | timeless_traces virtual tables
                         |
                    SQLite/libSQL
```

There will not be a generic telemetry server in this release. Only lifecycle,
build identity, configuration validation, owner fencing, shutdown/drain, and
neutral auth-claim validation may be shared after the three implementations
prove that the code is genuinely identical. Routes, wire formats, query
semantics, migration readers, and signal limits remain signal-specific.

## Non-negotiable invariants

1. `timeless-libsql` remains the only target storage implementation. Migration
   writes use `CREATE VIRTUAL TABLE ... USING timeless_*`, public versioned
   batch inserts, and public `flush`, `optimize`, `rollup`, `prune`, stats, and
   query surfaces. No migration or server code may create private blocks,
   indexes, codecs, rollups, or shadow-table writes.
2. The extension's authoritative automatic batching remains 4,096 points per
   series for metrics and 8,192 entries/spans for logs/traces. Migration
   checkpoints align to those thresholds; a final short tail uses the public
   flush command.
3. The legacy source is immutable. Migration never updates its administrative
   database, index database, snapshots, logs, blocks, sidecars, or Rust-engine
   files. It is never automatically deleted.
4. A successful migration is side-by-side, bounded-memory, restartable,
   idempotent, observable, and exact. A failed migration leaves a source the
   previous release can reopen.
5. SQLite transaction completion, extension-buffer durability, HTTP admission,
   HTTP completion, and final drain are reported separately. Benchmarks quote
   completed durable work only.
6. Startup and unsupported routes fail closed. Once a signal selects Rust,
   no request silently falls back to Rocket, a BEAM block store, or a second
   storage owner.
7. Every discovered defect gets a regression. A failed optimization is
   measured, documented, reverted, and does not block independent work.

## Session 0 inventory

### Release branches and immutable baseline heads

Fresh `release/rust-telemetry-data-plane` branches were created from the
completed POC heads, not from stale main branches:

| repository | baseline head | origin of release branch | responsibility |
|---|---|---|---|
| `timeless-libsql` | `2f4117086a86` | completed three-signal POC | extension, three Rust APIs, shared neutral Rust code, artifacts |
| `timeless_metrics` | `a1ce95298945` | completed metrics POC | metrics legacy reader, migration/startup adapter, product compatibility |
| `timeless_logs` | `d2d9602a26e1` | completed logs POC (`main` is identical) | logs legacy reader, migration/startup adapter, product compatibility |
| `timeless_traces` | `77f4be5695bb` | completed traces POC | traces legacy reader, migration/startup adapter, product compatibility |
| `timeless_ui` | `0eb794ed65f2` | completed metrics process seam | Phoenix control plane, auth/token policy, metrics process owner |
| `timeless_traces_dashboard` | `dba4f999eac5` | completed traces process seam | traces process/client/dashboard adapter |
| `timeless_stack` | `fbecd80daf87` | `main` | release configuration, supervision, install/upgrade/rollback |
| `timeless_metrics_dashboard` | `35b1dedad03f` | `main` | metrics dashboard compatibility |
| `timeless_logs_dashboard` | `34662bfa3d5e` | `master` | logs dashboard compatibility |

The pre-existing untracked `timeless_logs/.codex` entry belongs to the user
and is outside this work. It must remain untouched.

### Current runtimes and storage formats

| signal | current product owner and source | current libSQL target | release gap |
|---|---|---|---|
| metrics | BEAM supervision plus `tms_engine` NIF; administrative `metrics.db`; Rust store under `rust_engine/` | `timeless_metrics`, 4,096-point per-series buffering, raw chunks, stored rollup tables, retention | `:rust` is still the default. Existing offline migrator materializes a whole series twice, has no resumable journal, and requires manual activation. |
| logs | BEAM buffers/writer/compactor; `logs_index.db`; block payloads under `blocks/*.raw|*.zst|*.ozl`; older `index.snapshot`/`index.log` generation can coexist during upgrade | `timeless_logs`, 8,192-entry buffering, level-pure raw blocks, public size-tiered optimize, posting indexes | No libSQL migrator or product process seam. Current v0 loses product fidelity described below. |
| traces | BEAM buffers/writer/compactor; `traces_index.db`; block payloads under `blocks/*.raw|*.zst|*.ozl`; older `index.snapshot`/`index.log` generation can coexist during upgrade | rich `timeless_traces` batch v1, 8,192-span buffering, packed trace/term indexes, size-tiered optimize, retention | No libSQL migrator. The traces process/dashboard seam is still opt-in. |

The logs and traces legacy stores are BEAM-authored block stores with NIF
compression, not hidden `timeless-libsql` stores. Their migration readers may
decode those established formats, but target writes still cross only public
libSQL surfaces.

The product databases and target data-plane databases remain separate for a
legacy conversion so rollback is real:

| signal | immutable legacy source | canonical target after legacy conversion | fresh/current-libSQL target |
|---|---|---|---|
| metrics | `metrics.db` plus `rust_engine/` | `metrics.libsql.db` | an already-valid `metrics.db` remains valid; a fresh product uses the release-configured data-plane path |
| logs | `logs_index.db`, `blocks/`, or a recognized snapshot/log generation | `logs.db` | `logs.db` |
| traces | `traces_index.db`, `blocks/`, or a recognized snapshot/log generation | `traces.db` | `traces.db` |

Candidates and their journal live on the same filesystem under
`.timeless-migration/<signal>/`. A completed candidate is committed by an
atomic rename to the canonical target followed by parent-directory fsync. For
metrics, this deliberately does not rename or edit legacy `metrics.db`.

### Fidelity gaps that must close before conversion

The POCs established exact fidelity for metrics and rich spans, but current
logs v0 is not an exact representation of the released logs product:

- the product canonicalizes timestamps to epoch microseconds; logs batch v0
  documents epoch milliseconds;
- the product can retain `notice`, `critical`, `alert`, and `emergency` in
  addition to the four v0 severity buckets; and
- HTTP-ingested product metadata can contain typed JSON scalars/arrays/maps,
  while v0 converts a flat JSON object to string pairs.

Session 1 therefore adds a public logs batch/codec revision and an additive
table capability for microsecond timestamps, the complete declared severity
vocabulary, and canonical typed JSON metadata. Old v0 batches and blocks stay
readable with their documented defaults. Direct SQLite/libSQL users receive
the same richer contract. Equal-timestamp ordering must match the current
product comparator—timestamp followed by canonical message, severity, and
metadata—not merely insertion/block order.

Other pinned migration gaps:

- metrics migration must page the immutable NIF store rather than calling one
  unbounded range query per series, and it must validate/regenerate the public
  libSQL rollup ladder;
- logs/traces snapshot readers must be read-only. Starting the current Index
  process is forbidden because its legacy upgrade path imports and deletes
  `index.snapshot`/`index.log`;
- logs/traces decode at most one validated legacy block at a time, with an
  explicit maximum stored/decoded block bound and actionable oversized or
  corrupt-block failure; and
- only metrics and traces currently have proven OTP executable owners. Logs
  needs the same owner/drain/restart/reap seam before defaults change.

### Baseline validation

Correctness was rerun from the exact heads above before release code changed:

| repository | command | result |
|---|---|---:|
| `timeless-libsql` | `cargo test --workspace` | pass: all unit, integration, and doc tests |
| `timeless_metrics` | `mix format --check-formatted && mix test` | 482 passed |
| `timeless_logs` | `mix format --check-formatted && mix test` | 212 passed |
| `timeless_traces` | `mix format --check-formatted && mix test` | 183 passed |
| `timeless_ui` | `mix precommit` | 77 passed |
| `timeless_traces_dashboard` | `mix format --check-formatted && mix test` | 11 passed |
| `timeless_metrics_dashboard` | `mix format --check-formatted && mix test` | 30 passed |
| `timeless_logs_dashboard` | `mix format --check-formatted && mix test` | 4 passed; existing grouped-clause compiler warning retained |
| `timeless_stack` | `mix format --check-formatted && mix test` | 33 passed |

The first logs run shared a concurrent baseline process and failed two
filesystem assertions. Both tests passed in isolation and the standalone full
suite passed 212/212. Stateful filesystem/port baselines must remain
sequential; the discarded concurrent run is not a product number.

The completed POC performance artifacts are valid current baselines because
the release branches point at those exact POC heads:

| signal/path | completed durable write control | mixed/read tail | process HWM | source artifact |
|---|---:|---:|---:|---|
| metrics Rust API/libSQL | 869.9K points/s final mixed | query p95/p99 8.52/14.35 ms | 181,080 KiB | `timeless_metrics/bench/results/2026-08-01_metrics_api_session6.md` |
| metrics Elixir/Rust block | 806.6K points/s final mixed | query p95/p99 1.22/1.73 ms | 290,452 KiB | same artifact |
| logs Rust API/libSQL, two query workers | 470.2K entries/s | query p99 260.51 ms | 62,340 KiB | `timeless_logs/bench/results/2026-08-01_rust_logs_api_session1.md`, Session 7 |
| logs Elixir/block store, two query workers | 466.9K entries/s | query p99 1.61 s | 1,663,480 KiB | same artifact |
| traces Rust API/libSQL fixed zero-query | 178.5K spans/s | exact/fan-out/duration-miss p95 4.63/32.90/362.43 ms | 271,264 KiB write run; 66,732 KiB read run | `timeless_traces/bench/results/2026-08-02_traces_api_session7.md` |
| traces Elixir/block store zero-query control | 228.5K spans/s | corresponding read shapes are slower under load | 811,812 KiB | same artifact |

These are release regression references, not promises that unlike workload
generators are identical. Every promotion comparison must preserve the
artifact's workload caveats and rerun the fixed parity/load harness after
packaging, auth, migration, and default adapters are present.

## Startup state machine

The detector runs before a signal database owner, legacy buffer, scheduler,
HTTP listener, or dashboard data source starts. It records paths, file types,
SQLite headers, module/schema/capability metadata, journal version, source
manifest, and integrity results. It must return exactly one state:

| state | required evidence | action |
|---|---|---|
| `fresh` | no recognized source, candidate, canonical target, or cutover marker | create the canonical libSQL target, negotiate capabilities, mark ready |
| `valid_libsql` | one canonical database with correct SQLite integrity, vtab identity/schema, supported data/schema version, and no legacy/candidate conflict | open it; apply only supported additive migrations |
| `legacy` | one complete recognized legacy generation, no candidate/target, and exclusive ownership available | preflight and begin automatic side-by-side conversion |
| `resumable_migration` | immutable source fingerprint matches a supported candidate journal and no conflicting canonical target | validate the last committed checkpoint and resume from its exact cursor |
| `completed_cutover` | canonical target contains a verified source manifest/cutover generation matching the retained legacy source | open libSQL; expose retained rollback source and cleanup eligibility |
| `incompatible_version` | recognizable source/target/journal is newer, too old, or requires unavailable binary/extension capabilities | fail closed with required/current versions and supported action |
| `corruption` | malformed/truncated journal, failed integrity, impossible block metadata, missing referenced payload, source fingerprint drift, or semantic validation failure | fail closed; identify the exact artifact/check that failed |
| `ambiguous_dual_store` | two plausible active targets, unlinked legacy plus libSQL data, conflicting old/new index generations, or an unverified canonical target beside a source | fail closed; never choose by mtime, size, or non-emptiness |

Empty placeholder files do not establish a state. SQLite databases are
identified from their header plus schema/module metadata, not a filename.
Symlinks and cross-filesystem candidates are rejected. Unknown non-product
files are inventoried but do not turn a fresh directory into a legacy store.

Detector regressions must cover every row above, zero-byte/truncated files,
wrong signal vtables, missing sidecars/blocks, mixed schema versions,
unsupported future versions, old snapshot-only sources, exact duplicate old
and new index generations, and every ambiguous combination.

## Migration protocol

### Ownership and preflight

1. Stop the signal's admission and prove its legacy children are not running.
2. Acquire a signal-specific OS owner lease, an exclusive lock on every
   applicable legacy SQLite database, and read-only handles to source files.
   Refuse a second process. Existing releases that did not honor the lease
   must be fully stopped; an active SQLite/WAL owner or changing manifest is a
   hard failure.
3. Resolve all paths under the expected data directory; reject symlinks,
   non-regular block files, path escapes, unsupported devices/filesystems, and
   a candidate outside the source filesystem.
4. Run source integrity and schema checks. Build a deterministic manifest of
   relative path, size, stable metadata, and streaming SHA-256 without loading
   a file/tree into memory.
5. Perform a bounded read-only inventory pass to count records/series/blocks,
   derive timestamp ranges and input bytes, validate identities/relationships,
   and estimate target plus WAL/checkpoint headroom. Require the conservative
   target estimate, one maximum migration transaction/WAL allowance, and a
   25% safety margin. Recheck free space before every checkpoint.

### Atomic, restartable journal

The candidate database owns ordinary versioned tables
`_timeless_migration` and `_timeless_migration_events`. They are product
control records, not a replacement block store. Each checkpoint is one SQLite
transaction containing:

1. one public versioned batch insert into the signal vtab;
2. the public flush command when required for the final short threshold tail;
3. updated cursor, counts, timestamp bounds, rolling identity digest, source
   manifest digest, candidate byte/HWM observations, and phase; and
4. an append-only phase/event row.

The batch and journal cursor therefore commit together. A crash can commit
both or neither; it cannot durably advance only one. Full migration batches
align with 4,096 metrics points per series or 8,192 logs/traces records so the
extension, not the migrator, decides normal block publication. The source
cursor is deterministic:

- metrics: canonical metric/label-set order, then bounded legacy-engine point
  page/ordinal; float identity is its exact IEEE-754 bits;
- logs: legacy block generation/block id/row ordinal for progress, with a
  separate canonical response-order digest; and
- traces: legacy block generation/block id/row ordinal, plus trace/span/parent
  relationship digest.

At resume, startup rechecks the complete immutable source manifest, journal
version, last transaction counts/digest, target integrity, and target semantic
checkpoint before reading the next source record. Retries are idempotent.

### Validation and cutover

After import, use public commands to flush, optimize to zero actionable debt,
rebuild/validate the configured metrics rollup ladder, apply retention only
after parity is established, checkpoint/TRUNCATE the WAL, close all
connections, reopen cold, and run `integrity_check` plus signal-specific
oracles:

- metrics: exact series identity and canonical labels, point counts,
  min/max timestamps, IEEE-754 value bits including signed zero/non-finite
  policy, latest/range/scalar/window results, every configured rollup bucket
  and aggregate, and PromQL/dashboard fixtures;
- logs: exact count, timestamp microseconds, full severity, message bytes,
  canonical typed metadata, duplicate preservation, equal-timestamp ASC/DESC
  order and pagination, filters, native count, LogsQL/dashboard envelopes;
- traces: exact IDs/timestamps/durations/kind/status/status description,
  typed attributes, events, resources, scope, trace/parent relationships,
  per-trace span order, OTLP native/Jaeger/dashboard fixtures, and complete
  rich-span cold-reopen fidelity.

The candidate records a signed-by-digest verified source manifest and cutover
generation internally. Cutover is the same-filesystem rename of the closed
candidate to the canonical target plus parent-directory fsync. If the process
dies before rename, state is resumable; after rename, the internal verified
manifest makes the target unambiguously `completed_cutover` even if no later
status write ran. The retained legacy source is never renamed or deleted.

Readiness is false through detection, preflight, copy, optimize, validation,
and cutover. Liveness stays true. Machine-readable stats expose phase,
records/bytes completed and total, percent, durable migration rate, ETA,
source/candidate/peak bytes, process HWM, current cursor, last checkpoint,
retry count, and actionable error. Logs contain no credentials or payloads.

## Version and capability contract

The extension exposes one public machine-readable capability surface with:

- extension semver, build commit/target/profile, SQLite ABI range, data schema
  versions, vtab module generations, batch/codec generations, query/command
  capabilities, and minimum compatible server versions;
- persisted target format generation and additive-migration history; and
- explicit per-signal timestamp unit, retention/rollup arguments, index keys,
  and rich-field support.

Each binary exposes its own semver/build identity and minimum extension/data
versions. Startup negotiates the intersection before binding a listener.
Feature probing remains useful for optional query accelerators, but it cannot
substitute for a release compatibility handshake. Upgrade is additive and
old readable generations remain readable. A binary that cannot safely open a
newer database refuses it without mutation; documented rollback never relies
on a downgrade silently rewriting data.

## Declared first-release surface

The exact supported list is frozen in Session 5 and generated into user and
operator documentation. The POC baseline starts with:

- metrics: health/stats/flush, Prometheus and VictoriaMetrics import, native
  and Prometheus instant/range routes, export, labels/values/series, selectors,
  and `avg_over_time`; all other PromQL/product behavior is explicitly
  unsupported until implemented and parity-tested;
- logs: health/stats/flush, NDJSON insert, the declared LogsQL query slice,
  and dashboard historical reads; backup moves to the coordinated release
  workflow rather than a storage-owner fallback;
- traces: OTLP/HTTP JSON/protobuf/gzip, declared Jaeger services/operations/
  trace lookup/search subset, native dashboard search/detail,
  health/readiness/stats/flush; no implicit OTLP/gRPC or full-Jaeger claim.

Unsupported syntax/routes return a stable explicit 4xx capability error.
They never cross back to Rocket or a legacy storage process.

Phoenix issues short-lived, key-identified Ed25519-signed bearer claims and
owns users, sessions, revocation/auth version, key rotation, tenant/signal
scope, and policy. Rust receives verification keys and validates issuer,
audience, expiry/not-before, key id, tenant, signal, scopes, auth version, and
claim/config limits. The first release is one configured tenant per signal
database; a mismatched tenant is rejected rather than co-mingled. Unix socket
or loopback is the default. Non-loopback bind requires the documented TLS
termination, auth, audit, rotation, and rate-limit envelope.

## Sequential implementation sessions

### Session 0 — inventory, baselines, and checked plan

- Create all release branches from completed POC heads.
- Inventory every repository, owner, storage generation, public surface,
  batching contract, default, process seam, and release artifact.
- Rerun correctness baselines sequentially and pin the current performance
  artifacts to exact commits.
- Identify fidelity and migration gaps before modifying storage or defaults.
- Check in and push this plan; do not open a PR or merge main.

Exit criterion: every repository has an exact branch/head and clean baseline;
every source/target format and current gap is named; the cross-signal plan is
committed and pushed. No production behavior has changed.

### Session 1 — release-grade extension and signal binaries

Status: complete. Evidence:
[`2026-08-02_rust_telemetry_data_plane_session1.md`](2026-08-02_rust_telemetry_data_plane_session1.md).

- Add the public extension/server/data capability handshake and additive
  schema migration ledger with downgrade refusal tests.
- Add exact logs v1 timestamp/severity/typed-metadata fidelity and canonical
  equal-timestamp ordering while keeping v0 readable.
- Move the three crates from `poc/` into first-class versioned release
  locations/workspace membership without merging their route/storage code.
- Compare lifecycle/config modules and extract only identical neutral code.
- Standardize owner leases, Unix-socket/loopback defaults, config validation,
  build identity, readiness, graceful drain, abnormal restart, and exit codes.
- Retain every POC contract and performance harness at its new path.

Exit criterion: packaged-path debug/release builds, fmt, strict Clippy, all
workspace/extension/SQL/oracle/crash tests, all three real-extension API
contracts, v0/v1 compatibility, rich logs and rich spans pass. Every binary
fails before listen on an incompatible extension/database. No target has a
private storage path.

### Session 2 — bounded immutable readers and exact migration writer

Status: complete. Evidence:
[`2026-08-02_rust_telemetry_data_plane_session2.md`](2026-08-02_rust_telemetry_data_plane_session2.md).

- Implement signal-owned read-only legacy iterators. Add a bounded metrics NIF
  cursor; decode one validated logs/traces block at a time; support the exact
  recognized SQLite-index and old snapshot/log generations without invoking
  their mutating upgrade code.
- Implement the candidate database, in-database versioned journal, atomic
  batch/journal transactions, deterministic cursors/digests, preflight disk
  estimate, source manifest, progress stats, failpoints, and cold-reopen
  semantic validators.
- Write only public vtab batches/commands at authoritative thresholds.
- Benchmark migration rate, HWM, candidate/WAL/physical/logical bytes, source
  scan cost, optimize/rollup/checkpoint cost, and final parity.

Exit criterion: deterministic small/large/edge fixtures for all legacy
generations migrate exactly with bounded HWM; source tree hashes and mtimes are
unchanged; every batch-boundary crash resumes without loss/duplication; disk
preflight and injected disk-full fail closed; full signal oracles pass after
optimize/checkpoint/cold reopen.

### Session 3 — automatic startup detection and atomic default cutover

- Put the detector before every signal owner and implement all eight states.
- Automatically start/resume migration for an unambiguous legacy source.
- Add exclusive ownership, readiness/stats integration, cutover rename/fsync,
  complete-target recognition, retained-source reporting, and explicit
  cleanup command that is never automatic.
- Exercise failure at every phase and filesystem operation, including process
  kill, host reboot simulation, retry, source drift, corrupt target/journal,
  ambiguous dual stores, old/new binary and extension combinations, and
  insufficient disk.

Exit criterion: the state matrix and every injected crash point are pinned;
startup either serves one validated libSQL owner or returns one actionable
closed failure. A failed/aborted attempt leaves the previous release able to
open byte-identical legacy data. Successful cutover is recognized after a
crash without relying on a final best-effort marker write.

### Session 4 — Phoenix control plane, auth, limits, and process ownership

- Add Phoenix token/key/policy tables and issuance/rotation/revocation APIs.
- Add Rust claim validation, per-route scopes, audit identity, one-database
  tenant enforcement, request/queue/query/response/retention/resource limits,
  and stable errors.
- Finish the logs OTP owner and standardize metrics/traces owners using the
  neutral lifecycle seam. Prove normal drain/reap and abnormal restart for all
  three without orphan or dual ownership.
- Expose migration and data-plane readiness/stats through Phoenix cluster
  administration while keeping UI/session/config state in Elixir.

Exit criterion: anonymous, expired, future, wrong-audience, wrong-signal,
wrong-tenant, revoked/auth-version, stale/new key, scope, and every limit path
are pinned. Credential material never appears in logs/stats. Three processes
restart/drain/reap independently and cannot own the same target twice.

### Session 5 — switch adapters/defaults and freeze compatibility

- Route existing metrics, logs, and traces application adapters and dashboard
  historical reads to their signal Rust process after successful detection or
  cutover. Make libSQL/Rust the fresh-install and post-migration default.
- Retain an explicit time-limited rollback selector that requires stopping the
  new owner and uses the untouched legacy source. It is never an automatic
  per-request fallback.
- Differential-test every supported Prometheus/Victoria, LogsQL, OTLP,
  Jaeger, dashboard, PromQL, backup-control, and native surface. Generate the
  supported/unsupported compatibility statement and limits reference.
- Rerun fixed read/write baselines and preserve honest regressions.

Exit criterion: a fresh stack and three migrated fixtures use only the Rust/
libSQL data plane by default; no Rocket/legacy owner starts; all declared
surfaces have exact response/storage parity; unsupported behavior fails
explicitly; rollback drill restores the previous release without modifying
the retained source.

### Session 6 — artifacts, install, backup, upgrade, and rollback

- Build supported Linux/macOS artifacts for all three binaries and extension;
  publish deterministic checksums, SBOMs, third-party license notices, build
  provenance/identity, and a complete artifact manifest.
- Add release/Stack discovery, writable-path and service-order checks,
  installation, clean removal that preserves data by default, and documented
  explicit data cleanup.
- Implement coordinated drain/flush/optimize/checkpoint backup, scratch
  restore/integrity/semantic verification, migration-in-progress backup rules,
  and legacy-source inclusion during the rollback window.
- Drill fresh install, legacy upgrade, interrupted upgrade, additive schema
  upgrade, mixed binary/extension rejection, restore, rollback, re-upgrade,
  and uninstall/reinstall.

Exit criterion: clean machines install only declared artifacts and pass smoke
oracles; checksums/SBOM/licenses reproduce; every backup restores exact data
and configuration; supported upgrades and rollback are scripted and passing;
unsupported downgrade fails before mutation.

### Session 7 — sustained production fault, soak, and resource gates

- Run short CI fault versions plus release gates of at least two hours per
  signal and eight hours combined across real maintenance intervals.
- Cover sustained mixed writes/reads, migration under bounded load, slow and
  disconnected clients, cancellation storms, WAL growth/checkpoint/vacuum,
  disk-full/read-only/corrupt data, address/socket conflicts, descriptor
  pressure, repeated kill/restart, retention/rollup/optimize, backup overlap,
  token rotation, and control-plane disconnect/reconnect.
- Record completed durable throughput, p50/p95/p99 by write/query shape,
  migration rate/HWM, process RSS/HWM slope, queue/body/result watermarks,
  logical/physical bytes, WAL high-water, checkpoint/vacuum behavior,
  maintenance amplification, error/retry counts, and exact parity.

Exit criterion: zero silent loss/duplication/cross-tenant access/partial
responses; every admitted-success contract is satisfied; queues/WAL/RSS remain
within declared bounds after warmup; drain reaches zero; fault recovery is
automatic where declared and actionable/closed elsewhere. Any failed gate is
fixed with a regression or listed as a release blocker—never averaged away.

### Session 8 — release verdict and operator handoff

- Produce the exact compatibility and migration statement, operator runbook,
  install/backup/restore/upgrade/rollback/cleanup procedures, limits/SLO/alert
  reference, artifact inventory, benchmark report, and build matrix.
- State the legacy retention window and cleanup eligibility. Cleanup requires
  an explicit command, a verified backup, operator confirmation, and a final
  source manifest; report recoverability after it runs.
- List every remaining blocker/tradeoff with owner and effect. Give a per-
  signal and combined release verdict based on the gates, not schedule.
- Commit and push final successful session heads. Do not open PRs or merge.

Exit criterion: an operator can install, observe, migrate, back up, restore,
upgrade, roll back, and explicitly clean up using only checked documentation
and scripts; artifact and compatibility inventories are exact; the verdict is
`release`, `release with named limitations`, or `blocked` with evidence.

## Commit and evidence discipline

Each successful session gets one or more focused commits in every affected
repository and is pushed directly to `release/rust-telemetry-data-plane`.
Result documents include exact commands, commits, host/build identity,
fixture seed/shape, durable completion definition, storage units, HWM, and
discarded-run reasons. No PR is opened and no release branch is merged to
main during this goal.
