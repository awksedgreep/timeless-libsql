# Release validation: 0.8.6

Date: 2026-09-30. Release source:
[`a24957b211becb0c0af9e85572a9be5a48528061`](https://github.com/awksedgreep/timeless-libsql/commit/a24957b211becb0c0af9e85572a9be5a48528061).

Both workspaces were bumped to `0.8.6`, their path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. All results below refer to that
complete release commit on `main`.

This release bounds the points a metrics query decodes from storage apart from
the points it keeps: `TIMELESS_METRICS_PROMQL_MAX_STORAGE_POINTS` (default
5,000,000) is checked from the chunk index before any payload is read, and
`TIMELESS_METRICS_PROMQL_MAX_WORK_POINTS` (default 100,000, unchanged) bounds
what a query holds. A ranking over a history of large chunks is no longer
refused for what it throws away. See the [changelog](../CHANGELOG.md).

## Local verification

All steps ran on Linux x86-64 with Rust 1.98.1 and an extension-enabled
`sqlite3` CLI, after the version bump, on the release commit.

| Check | Result |
|---|---|
| Formatting | All eight Cargo workspaces passed. |
| Locked dependency checks | Root, servers, query harness, and the other detached tools passed under `--locked`. |
| Root workspace tests | 284 passed, 1 ignored. |
| Server workspace tests | 593 passed with extension-dependent ignored tests enabled, including the new storage-points bound contract. |
| Query harness | Unit tests, `contracts`, and pinned oracle-manifest validation passed. |
| Public SQL recipes | 135 recipes / 173 statements passed through the real extension. |
| SQLite CLI suite | All sections passed. |
| Focused correctness | `r1`, `r2`, `r3`, `r4`, `r8`, and `logs-rich` passed. |
| Crash recovery | Five forced-kill rounds passed. |
| dbhealth | Separate build, load, sampling, schema, and lifecycle checks passed. |
| Executable onboarding documentation | Markdown examples passed first-result and cold-reopen checks. |
| Lint and API documentation | Root, server, and query-harness Clippy and rustdoc passed with warnings denied. |

Commands and prerequisites are maintained in [TESTING.md](../TESTING.md).
Query compatibility uses the checked-in immutable oracle corpora; this release
did not change or refresh upstream pins.

The root workspace's fat-LTO release build of every crate at once fails to
load the dbhealth extension's bitcode (a known toolchain issue noted in that
crate's manifest); the gates built `timeless-ext` and the server crates one at
a time, as documented.

Not run for this release: the pre-tag dispatched CI workflows (query contracts,
production data-plane gate, native artifact matrix) and the release tool's
local package build. The tag-triggered artifact workflow below is the native
build evidence for `0.8.6`.

## Publication and downloaded binaries

[`v0.8.6` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.6)
on 2026-09-30 at 02:34:26 UTC. `main` was pushed before the annotated tag; the
tag resolves to the release commit above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/36659821659)
passed every native build, checksum, and publication job.

All five permanent assets were downloaded again and the four archives passed
`sha256sum -c` against the published `SHA256SUMS`; the `x86_64-unknown-linux-gnu`
archive inventory (`bin/` with the three APIs and `timeless-authctl`,
`lib/libtimeless_ext.so`, `install.sh`, `uninstall.sh`, manifest, SBOM, and
licenses) matched the documented target matrix.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16166713 | `805f0a1c41461a49f174a883570fda4ea212864a0372c9f089c0f33ca3bffd10` |
| `aarch64-unknown-linux-gnu` | 15346399 | `d4a2adab9119e1eea55d59c84f639ad98c1a159a796f8367122a0d9df8083470` |
| `x86_64-apple-darwin` | 15351977 | `22fe27093ed4ebfdfb506b39d07d7edd010784877bc9f60496b1ed1ea1819042` |
| `aarch64-apple-darwin` | 14201285 | `6dab53fc4adb0e4babd2a3acd5a6ba8841d3dce7e5689716d66598958717fffe` |

## Deployment

Not yet deployed. The `timeless-stack` image has not been rebuilt against
`v0.8.6`; `timeless_stack` v0.7.24 builds its data plane from this
repository's latest tag.

## Upgrade notes

- **Storage points.** A query that was refused as `work point limit N
  exceeded (candidate points: M)` for decoding large chunks now runs; set
  `TIMELESS_METRICS_PROMQL_MAX_STORAGE_POINTS` lower than the default
  5,000,000 where decode time on a slow host matters (about 5 ms a million
  points measured), and keep `..._MAX_WORK_POINTS` sized for the points a
  query holds.
- **Refusal message.** A query refused for what it kept now says `work point
  limit N exceeded (M points kept from storage)`; a parser of the old text
  needs the new.
- Read the [upgrade guide](UPGRADE.md) and the
  [release changelog](../CHANGELOG.md) before replacing a deployment.
