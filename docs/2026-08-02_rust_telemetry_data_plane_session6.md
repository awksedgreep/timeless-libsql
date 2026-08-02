# Rust telemetry data plane release promotion — Session 6

Date: 2026-08-02
Branch: `release/rust-telemetry-data-plane` in every affected repository

## Outcome

Session 6 supplies reproducible native artifacts, versioned install/removal,
online owner-coordinated backup, verified offline restore, exact artifact
pinning in Stack, and executable release/container drills. The local
x86-64 Linux artifact reproduced byte-for-byte, installed into an empty
prefix, validated its own checksums and build identities, uninstalled while
preserving data/configuration sentinels, and passed a clean container startup
and graceful-stop oracle.

The implementation heads exercised by the final drill were:

| repository | commit |
|---|---|
| `timeless-libsql` artifact/data-plane implementation | `a59c1928f024cbdfda8dbb9a92f568114cef750c` |
| `timeless_ui` owner preflight and backup clients | `adb84149a87c5ee5f0fd83f3d192fc08fa9fe3c8` |
| `timeless_stack` coordinator/package/operations | `c224936f3658905aeb02fa61a2bbb7487ee0dcf4` |

Stack pins the full `timeless-libsql` implementation hash in
`telemetry-data-plane.lock`; its container workflow resolves that hash and
embeds the same commit into the extension and all three binaries.

## Native artifacts

`tools/package_release.py` builds the extension and all three signal-specific
servers natively with locked root/server Cargo workspaces. It emits:

- immutable binaries and extension;
- a manifest containing file hashes, target, source epoch, binary and
  extension capability/build identities;
- an SPDX 2.3 SBOM covering 177 unique locked packages from both workspaces;
- third-party license inventory and the project MIT license;
- internal and archive `SHA256SUMS`; and
- version-aware install/uninstall scripts.

Supported build jobs are Linux x86-64/arm64 and macOS Intel/Apple Silicon.
The release-branch workflow runs all four native builders without
cross-compiling identity checks. The package refuses a dirty tree by default,
refuses a non-native target, and uses the source commit timestamp, normalized
tar ownership/modes, path remapping, and gzip epoch zero.

The final clean implementation artifact was built twice on the same host and
both archives had:

```text
195e1e2bcad2ef91ec292bc02763d48081eb215856cc60c3606b026017182158
```

Its identity was version `0.3.0`, target
`x86_64-unknown-linux-gnu`, commit
`a59c1928f024cbdfda8dbb9a92f568114cef750c`.

Installation verifies internal hashes, target/host agreement, and all three
binary identities before atomically switching `/opt/timeless/bin` and
`/opt/timeless/lib` links to a versioned immutable tree. Previous versions
remain available. Uninstall removes only links it owns and its selected
artifact by default; telemetry data, configuration, backups, and retained
legacy rollback sources are never removed.

## Backup and restore contract

Phoenix drains its bounded logs transport first. Each owning Rust writer then
serializes a backup barrier behind admitted writes and uses only public
extension/SQLite behavior:

- metrics flushes raw batches, performs compact/rollup work, checkpoints, and
  copies through SQLite online backup;
- logs and traces flush their authoritative 8,192-entry/span batches, drain
  bounded optimize work, checkpoint, and copy through SQLite online backup.

The common implementation requires an absolute new destination, never
overwrites, retries a busy checkpoint only within its bound, copies through a
staging file, runs `quick_check`, validates the additive schema ledger, fsyncs,
and publishes without clobbering. Backup work is serialized through the same
writer that owns the database; Phoenix never opens telemetry storage.

`TimelessStack.Backup` rejects migration/not-ready state before calling any
owner, includes all three database snapshots, the Phoenix control database,
authorization policies, build/health reports, exact source storage layout,
and immutable retained legacy sources plus their source manifests. It writes
a full checksummed inventory and atomically publishes a new directory.

Offline restore accepts only a new or empty target. It verifies every digest,
reconstructs exact signal target names and control/auth/legacy trees, runs
SQLite integrity and schema-ledger checks, fsyncs copies, and atomically
publishes the restored data directory. Tests pin no-clobber behavior and exact
metrics float values, 16,384 ordered rich logs, and complete rich-span
identity/relationships/typed payloads after cold reopen.

## Upgrade and rollback matrix

The scripted gate reruns the metrics, logs, and traces release-startup suites
against the real extension. Together with the Session 3–5 release drills they
cover fresh creation, immutable legacy upgrade, crash/resume at journal
boundaries, completed cutover, additive schema replay, corrupt/incompatible
and ambiguous stores, wrong signal schema, owner fencing, rollback source
preservation, and idempotent retry/re-upgrade. Future schema and mixed
binary/extension capability states fail before virtual-table initialization or
source mutation.

The Stack backup regression creates all three databases and the control store,
includes a retained source, verifies/no-clobbers, restores into a scratch root,
reopens every SQLite file, validates schema ledgers and exact files, and
rejects restore over a non-empty destination. The artifact drill installs,
uninstalls, confirms data/configuration sentinels survive, and builds a release
containing the offline restore wrapper.

Operator procedures are checked in at
`timeless_stack/docs/telemetry_data_plane_operations.md`. They distinguish the
untouched pre-cutover legacy timeline from an exact post-cutover rollback using
a verified release backup; owners must never alternate against one directory.

## Regressions found and pinned

1. A failed `TimelessUI.Supervisor` start still attempted admin bootstrap,
   masking the actual child error after the Repo had exited. Post-start work
   now runs only after `{:ok, pid}`; a regression forces the already-started
   supervisor error and proves no account call occurs.
2. The minimal runtime image lacked `/usr/bin/kill`, which the neutral owner
   requires for bounded TERM/KILL child reaping. The image installs `procps`,
   and the container gate asserts the executable before startup.
3. Docker called authenticated `/health` without a credential and could never
   become healthy. It now checks the intentionally public `/live` endpoint on
   each loopback owner plus the Phoenix endpoint. Authentication on readiness,
   health, stats, queries, and writes remains unchanged.
4. A coordinated backup initially omitted entries still admitted to Phoenix's
   bounded logs transport. The coordinator now drains that buffer before any
   signal barrier, fails the backup if drain fails, and pins the ordering.
5. The traces startup test relied on its legacy-reader helper being loaded by
   full-suite order. The release script now names both files explicitly so the
   focused gate is hermetic.

## Validation

The final executable gate was:

```text
RUN_CONTAINER_DRILL=1 timeless_stack/scripts/session6_release_drill.sh
```

It passed:

- metrics/logs/traces startup state suites: 5 / 9 / 10 tests;
- common plus all three server workspaces against the real extension,
  including ignored contracts and strict all-target Clippy;
- `timeless_ui`: 24 focused owner/client/application tests;
- `timeless_stack`: 37 tests;
- two identical clean release archives;
- verified install/uninstall with preservation sentinels;
- warning-as-error production compile and a release with the restore wrapper;
- an exact pinned container build with all binaries/extension, required OS
  process tools, fresh Rust/libSQL startup, three signal liveness checks,
  Phoenix HTTP response, and graceful exit zero.

Host/build identity was Linux x86-64, kernel 7.1.3, Rust 1.97.0, Elixir
1.20.2/OTP 29, Python 3.14.6, and Docker 29.6.1.

## Exit verdict

Session 6 meets its exit criterion. Declared artifacts are deterministic and
self-describing; install/removal and container startup are executable and
passing; backups restore exact signal/control/configuration and retained
rollback inputs; upgrade/downgrade state handling fails closed before mutation;
and the supported rollback meanings are scripted and documented. Session 7
still owns sustained fault/soak and long-running resource gates, so this is not
yet the final production release verdict.
