# Release validation: 0.8.5

Date: 2026-09-26. Release source:
[`e8a6362a8a3dcc2780b2f48f48b37da0df9dc33b`](https://github.com/awksedgreep/timeless-libsql/commit/e8a6362a8a3dcc2780b2f48f48b37da0df9dc33b).

Both workspaces were bumped to `0.8.5`, their path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. All results below refer to that
complete release commit on `main`.

This release decouples the metrics catalog read budget from the response and
work budgets, streams metrics catalog reads instead of materializing the whole
catalog, rejects implausible VictoriaMetrics import timestamps, and adds the
bounded `prune-after:<unix_seconds>` metrics repair command. See the
[changelog](../CHANGELOG.md).

## Local verification

All steps ran on Linux x86-64 with Rust 1.98.1 and an extension-enabled
`sqlite3` CLI.

| Check | Result |
|---|---|
| Formatting | All eight Cargo workspaces passed. |
| Locked dependency checks | Root, servers, query harness, and the other detached tools passed under `--locked`. |
| Root workspace tests | 284 passed, 1 ignored. |
| Server workspace tests | 592 passed with extension-dependent ignored tests enabled. |
| Query harness | 64 tests passed; `contracts` and pinned oracle-manifest validation passed. |
| Public SQL recipes | 135 recipes / 173 statements passed through the real extension. |
| SQLite CLI suite | All 47 sections passed, including 150,000 randomized operations and five forced-kill recovery rounds. |
| Focused correctness | `r1`, `r2`, `r3`, `r4`, `r8`, and `logs-rich` passed. |
| dbhealth | Separate build, load, sampling, schema, and lifecycle checks passed. |
| Executable onboarding documentation | Markdown examples passed first-result and cold-reopen checks. |
| Embedded Rust and direct libSQL | Embedded example and multi-connection libSQL reopen passed. |
| Lint and API documentation | Root, server, and query-harness Clippy and rustdoc passed with warnings denied. |
| Release tool | The native `x86_64-unknown-linux-gnu` package build passed identity/checksum and install/remove checks, preserving data and configuration. |

Commands and prerequisites are maintained in [TESTING.md](../TESTING.md).
Query compatibility uses the checked-in immutable oracle corpora; this release
did not change or refresh upstream pins.

## Dispatched CI

Each workflow passed on the release commit `e8a6362`:

- [Query contracts and executable documentation](https://github.com/awksedgreep/timeless-libsql/actions/runs/36255986068).
- [Production data-plane gate](https://github.com/awksedgreep/timeless-libsql/actions/runs/36255987913): short mode passed; all fault events were clean and the final durability barriers succeeded.
- [Native artifact matrix and complete outer checksums](https://github.com/awksedgreep/timeless-libsql/actions/runs/36255176975): all four Linux/macOS architectures passed native build, identity, and install/remove checks, and publication.

## Publication and downloaded binaries

[`v0.8.5` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.5)
on 2026-09-26 at 16:33:21 UTC. `main` was pushed before the annotated tag; the
tag resolves to the release commit above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/36255176975)
passed every native build, checksum, and publication job.

All five permanent assets were downloaded again and the four archives passed
`sha256sum -c` against the published `SHA256SUMS`; the archive inventory
(`bin/`, `lib/libtimeless_ext.so`, `install.sh`, `uninstall.sh`, manifest,
SBOM, and licenses) matched the documented target matrix.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16166200 | `9b477de36f8804a60132388351dd77db4a880cd21d1888a8e747442afc1d21cc` |
| `aarch64-unknown-linux-gnu` | 15346849 | `1d16c3961ad98a4044728ccf080b1deff5d3343e45b8b622c1c2fb506bb84572` |
| `x86_64-apple-darwin` | 15350346 | `861008735ea1147cfc79b8a75fb523167a359427918db4eaf6fe19eab40942f5` |
| `aarch64-apple-darwin` | 14199446 | `e949b9660012b21f45bc8d5b3b75d70de730eeac4d6a8e702f4ce7f3504b022d` |

## Deployment

The `timeless-stack` image was rebuilt against `v0.8.5` (data plane commit
`e8a6362`) and deployed to the standalone VPS and `dhcp1`. Both hosts report
`timeless-metrics-api 0.8.5` with `additional wall-clock raw expiry 604800
seconds`; metrics discovery succeeds on the high-cardinality `dhcp1` store
(~56k series).

## Upgrade notes

- **Metrics retention.** The metrics server does not apply an implicit
  wall-clock cutoff. Set `TIMELESS_METRICS_RAW_RETENTION_SECS=604800` (or the
  table's data-time retention) explicitly when raw expiry is required.
- **Catalog limits.** Broad discovery and regex/negative matchers are bounded
  by `TIMELESS_METRICS_PROMQL_MAX_CATALOG_SERIES` (default 1,000,000) and
  `TIMELESS_METRICS_PROMQL_MAX_CATALOG_BYTES` (default 256 MiB); `0` disables
  either. Size these for projected cardinality. Catalog reads are no longer
  charged to `..._MAX_RESPONSE_BYTES` or `..._MAX_WORK_POINTS`.
- **Import validation.** The VictoriaMetrics import now rejects samples whose
  millisecond timestamp falls outside 1900..=2100.
- Existing millisecond SQL log tables can be served with
  `TIMELESS_LOGS_TIMESTAMP_UNIT=ms`. Read the [upgrade guide](UPGRADE.md) and
  the [release changelog](../CHANGELOG.md) before replacing a deployment.
