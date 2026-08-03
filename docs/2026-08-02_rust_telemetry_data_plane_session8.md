# Rust telemetry data plane release promotion — Session 8

Date: 2026-08-02
Branch: `release/rust-telemetry-data-plane` in every affected repository

## Outcome

Release with named limitations. Sessions 0–8 and every required gate are
complete. The authoritative exact-build sustained gate passed for 9,602.37
seconds (8.00197 aggregate signal-hours) with all 12 fault events successful,
no workload errors, exact final durable barriers, and zero final queue or
in-flight residue.

The release candidate is the signal-specific Rust data plane backed by the
public `timeless-libsql` extension. It is the default for fresh and migrated
Stack installations. Phoenix remains the control plane; there is no generic
telemetry server and no silent fallback to Rocket or a legacy storage owner.

## Validated implementation and branches

The runtime and native artifacts under sustained validation use exact
`timeless-libsql` build identity
`bab775035785b78e0d9879b7d871bbd938e92991`. The release-promotion branch is
pushed in every changed repository. Final documentation-only commits may
advance branch heads without changing that validated binary identity.

| repository | validated/pushed implementation head |
|---|---|
| `timeless-libsql` | `bab775035785b78e0d9879b7d871bbd938e92991` |
| `timeless_metrics` | `45c449b8996562b187fd4d2e96d958ddd9dd77b1` |
| `timeless_logs` | `24a64204d21ff7ec24059a83bf60f7083cdefa53` |
| `timeless_traces` | `b862a1a1044cbeeeb77b226845e695e19646ea67` |
| `timeless_ui` | `39b845a5e8839021a9742148dae360c2cc70ceac` |
| `timeless_stack` | `fa7b7b75f1eed0b53b1e65517c2663f9bdf13cef` |

No pull request was opened and no release branch was merged to main.

## Release package

The release line is native data plane `0.3.0` with Stack `0.6.11`. GitHub
Actions run `30769695085` built all supported native archives from exact head
`bab7750`, and its aggregate checksum job passed. Run `30769695093` passed
clean root/server tests, real-extension contracts, strict Clippy, and the
two-minute mixed production fault gate at the same head.

| target | archive SHA-256 |
|---|---|
| `aarch64-apple-darwin` | `862979f12e692533ba0e10c4308727bc68d6d71ad3a30d5628b516f28e541760` |
| `aarch64-unknown-linux-gnu` | `3b1436cee660ab2330adc920806b6499dd59e449e4ae37c35823508b9f0e4813` |
| `x86_64-apple-darwin` | `fed469529d14e60a54c5273f502d4d52e8a56e8f0d850570d84b3060b15dbf66` |
| `x86_64-unknown-linux-gnu` | `5350fc93b729b36c85946752f661ee4d221044c1caee5443d42747357653118c` |

Each archive contains all three versioned signal binaries, the matching
extension, internal checksums, build/capability manifest, SPDX SBOM, project
and third-party license notices, and install/uninstall scripts. Independent
archive extraction verified the published and internal checksums, exact build
identity, manifest, notices, and executable inventory.

## Compatibility and migration statement

Fresh Stack installations create libSQL virtual tables and start only the
three signal-specific Rust owners. Startup distinguishes fresh, valid libSQL,
legacy, resumable migration, completed cutover, incompatible version,
corruption, and ambiguous dual-store states before readiness.

Detected legacy data is converted automatically, side-by-side, and under
exclusive ownership. Each product's bounded legacy reader supplies the
extension's public versioned batch/SQL interfaces. The extension retains
authoritative 4,096-point metrics buffering, 8,192-entry logs buffering,
8,192-span trace buffering, codecs, indexes, compression, rollups, retention,
and maintenance. Migration never creates storage blocks privately.

The versioned journal is restartable and idempotent. Preflight checks schema,
capability, source identity, and disk headroom. The legacy source remains
immutable. Final validation covers exact identities and timestamps, metric
float bits/labels/rollups, log order/severity/typed metadata, and complete
rich-span relationships and fields before public flush, maintenance,
checkpoint, cold reopen, semantic oracles, and atomic cutover. Failure is
closed and retains the previous release's source. Legacy data is never deleted
automatically.

The exact supported and explicitly unsupported Metrics/Prometheus/Victoria,
LogsQL, OTLP, Jaeger, dashboard, and PromQL surfaces are recorded in
`timeless_stack/docs/telemetry_data_plane_compatibility.md`. Unsupported
behavior returns an explicit error and never crosses to a second owner.

## Operator handoff

The checked operator sources are:

- `timeless_stack/docs/telemetry_data_plane_operations.md` for native install
  and removal, readiness, coordinated online backup, offline restore, upgrade,
  interrupted migration, rollback, re-upgrade, and explicit legacy cleanup;
- `timeless_stack/docs/telemetry_data_plane_compatibility.md` for ownership,
  supported surfaces, limits, backup contract, and the time-limited rollback
  selector; and
- `timeless_stack/docs/telemetry_data_plane_release_handoff.md` for the exact
  artifact inventory, release decision, alerts, and known tradeoffs.

The legacy rollback source is retained through the first-release window ending
at `0.9.0`. Cleanup requires a verified backup, the complete Stack stopped,
the final source-manifest digest, a second operator confirmation, and one
explicit signal-specific command. There is no wildcard cleanup. Recoverability
after cleanup depends on the verified external backup.

## Declared production limits

The shipped maxima are a 10 MiB request and decompressed body, 16 MiB response,
100,000 query rows, 30-second request duration, 64 concurrent requests per
token, one-second admission wait, 256-request writer queues, and two SQLite
readers per signal. Claims may lower but cannot raise them.

Operational limits are a 512 MiB WAL per signal, 512 MiB process RSS for
metrics/logs, 768 MiB for traces, 16 MiB/hour warm per-process RSS slope, and a
10-second query p99 release ceiling. Readiness, migration progress, queues,
maintenance/backup/checkpoint errors, disk headroom, restart count, WAL, RSS,
and query tails are observable and have documented alert actions.

The 16 MiB/hour slope is evaluated only for a generation alive for at least
two hours. The fault gate deliberately restarted each generation sooner, so
its shorter slopes are diagnostic rather than a passed long-generation leak
assertion. Trace RSS HWM stayed at 206,236 KiB while its retained live index
grew to 2,457,600 spans. Operators should alert on the HWM and on unexplained
slope after accounting for live-set growth until retention reaches steady
state.

## Honest tradeoffs

- Rich logs deliberately keep all eight severities, microsecond timestamps,
  and typed nested metadata even though the lossless format has lower peak
  ingest throughput and higher memory than the discarded flat POC format.
- Trace duration-miss searches remain decode-bound and have the widest known
  trace query tail. They are exact and bounded, but not presented as cheap.
- PromQL and LogsQL support is intentionally narrower than their complete
  languages; unsupported syntax is a stable compatibility error.
- Selecting the retained legacy source after cutover returns to the immutable
  pre-cutover timeline and therefore excludes later libSQL writes. Preserving
  later writes requires restoring a verified Rust/libSQL backup separately.
- Logical optimize is not a physical vacuum. The release does not run a
  blocking full `VACUUM` against an active owner; page reuse, freelist, and
  physical high-water remain separate operational measurements.

## Final verdict

Release with named limitations for metrics, logs, traces, and the combined
Stack. The authoritative evidence is
[`evidence/2026-08-02_release_gate_bab7750.json`](evidence/2026-08-02_release_gate_bab7750.json),
SHA-256
`8d3472a3b7843b759e1a1fda830c2668b89da5c96ffa1025aa2069c10158d11d`.

Metrics completed 2,457,664 durable points at 255.94 points/s, logs completed
1,859,904 durable entries at 193.69 entries/s, and traces completed 2,457,600
durable spans at 255.94 spans/s. Write p99 was 1.226/1.580/2.181 ms. The
slowest query p99 was 2.950 ms for metrics, 3,746.335 ms for logs, and 10.555
ms for traces. RSS HWM was 20,568/62,228/206,236 KiB and WAL HWM was
28,648,400/27,810,032/164,342,560 bytes. All are inside declared release
limits.

The exact named limitations are rich-log peak-throughput/storage cost, slow
log discovery and scalar-count tails, retained-index trace RSS growth before
steady-state retention, decode-bound trace duration misses, deliberately
narrow PromQL/LogsQL support, no blocking live full-file vacuum, and the
pre-cutover-only meaning of legacy-source rollback. None is a correctness,
durability, migration, packaging, or startup blocker. There are no remaining
known release blockers.
