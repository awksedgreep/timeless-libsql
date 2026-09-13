# Release validation: 0.8.4

Date: 2026-09-13. Release source:
[`3453ec77436fde6eb09f1a603ce976227203e6eb`](https://github.com/awksedgreep/timeless-libsql/commit/3453ec77436fde6eb09f1a603ce976227203e6eb).

Both workspaces were bumped to `0.8.4`, their path-package lock entries were
updated without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. The user-approved terminal
recording was committed unchanged before the final checks. All results below
refer to that complete release commit on `main`.

## Local verification

All 41 steps in the local checklist runner passed on macOS Apple Silicon,
using Rust 1.98.1 and an extension-enabled Homebrew SQLite CLI. The separate
production-scale rollup fixture and native package validation also passed.

| Check | Result |
|---|---|
| Formatting | All eight Cargo workspaces passed. |
| Locked dependency checks | Root, servers, and query harness passed. |
| Root workspace tests | 283 passed; the normally ignored 224 MiB rollup fixture was run explicitly and also passed, for 284 total. |
| Server workspace tests | 579 passed with extension-dependent ignored tests enabled. |
| Embedded extension tests | 73 passed; the embedded example also ran successfully. |
| Query harness | 61 tests passed; contracts and pinned oracle-manifest validation passed. |
| Public SQL recipes | 135 recipes / 173 statements passed through the real extension. |
| SQLite CLI suite | All 47 sections passed, including 150,000 randomized operations and five forced-kill recovery rounds. |
| Focused correctness | `r1`, `r2`, `r3`, `r4`, `r8`, and `logs-rich` passed. |
| Standalone crash suite | Five additional forced-kill recovery rounds passed. |
| dbhealth | Separate build, load, sampling, schema, and lifecycle checks passed. |
| Executable onboarding documentation | Five Markdown examples passed first-result and cold-reopen checks. |
| Direct libSQL | Real extension, multiple connections, all three signals, and reopen passed. |
| Lint and API documentation | Required root/server/query/release-tool Clippy gates, embedded-library Clippy, and root/server rustdoc passed with warnings denied. |
| Release tool | Four unit tests and the native Apple Silicon package build passed. The package verified identities/checksums and preserved data/configuration during install/remove. |

Commands and prerequisites are maintained in [TESTING.md](../TESTING.md).
Query compatibility uses the checked-in immutable oracle corpora; this release
did not change or refresh upstream pins.

## Pre-tag CI

Each dispatched workflow passed on the same release commit before the tag was
created:

- [Query contracts and executable documentation](https://github.com/awksedgreep/timeless-libsql/actions/runs/34784033991).
- [Production data-plane gate](https://github.com/awksedgreep/timeless-libsql/actions/runs/34784035123): short mode, 120-second mixed-signal workload, all 12 fault events passed, no failures, and successful final durability barriers. Coverage includes startup descriptor/disk faults, overlapping backups, cancellation/disconnect storms, graceful restarts, and SIGKILL recovery.
- [Native artifact matrix and complete outer checksums](https://github.com/awksedgreep/timeless-libsql/actions/runs/34784036823): all four Linux/macOS architectures passed native build, identity, and install/remove checks. Publication was intentionally skipped on this branch dispatch.

All four candidate archives were downloaded independently and checked for the
exact file inventory, outer/inner checksums, manifest and binary identities,
clean-source marker, SPDX SBOM, and license inventories.

## Publication and downloaded binaries

[`v0.8.4` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.4)
on 2026-09-13 at 21:58:31 UTC. `main` was pushed before the annotated tag;
the tag resolves to the release commit above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/34784815700)
passed every native build, checksum, and publication job.

All five permanent assets were downloaded again. The four archives passed
outer and inner checksum verification, exact file inventory checks, manifest
and binary identity checks, and SPDX/license checks. The release is public,
non-draft, and non-prerelease.

The exact Bash installation block in [ARTIFACTS.md](ARTIFACTS.md#installing)
was executed against the published Apple Silicon archive in an isolated
writable prefix. All four installed binaries reported the expected version
and commit. Each signal server passed ingest, query, POST flush, GET-flush
rejection, graceful restart, coordinated backup, and startup/query from the
backup. Removal used the same prefix spelling as installation and preserved
all live databases, backups, and configuration byte-for-byte.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16150082 | `421b2e87174c20032677ed55fe4b7c5d036845eb20a8246c55666b94ab136927` |
| `aarch64-unknown-linux-gnu` | 15322377 | `db5ef515a995f701b1b10f991768da3ed0224b0b7b194ea303c347b654d4145f` |
| `x86_64-apple-darwin` | 15332171 | `8a1e6750f2cf6787ea30d1d064481ab4b7c1018f6c5bcce37ffa60c357936420` |
| `aarch64-apple-darwin` | 14181336 | `98582b16c0ed2a2fb429f5dec93e7b096c1c0a0b582f9cc6993c4a3f9f504e4b` |

## Upgrade notes

Install the extension and deployed signal servers as one compatibility set.
Metrics no longer apply the implicit seven-day wall-clock cutoff; configure
`TIMELESS_METRICS_RAW_RETENTION_SECS=604800` explicitly when that extra expiry
policy is required. Existing millisecond SQL log tables can be served with
`TIMELESS_LOGS_TIMESTAMP_UNIT=ms`. Companion upgrades are explicit and
transactional. Read the [upgrade guide](UPGRADE.md) and the
[release changelog](../CHANGELOG.md) before replacing a deployment.
