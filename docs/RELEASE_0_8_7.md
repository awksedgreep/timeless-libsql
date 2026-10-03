# Release validation: 0.8.7

Date: 2026-10-03. Release source:
[`0b8d4ab5b426aa543d9b9ea701fbfca20e1b3fdb`](https://github.com/awksedgreep/timeless-libsql/commit/0b8d4ab5b426aa543d9b9ea701fbfca20e1b3fdb).

Both workspaces were bumped to `0.8.7`, their path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. All results below refer to that
complete release commit on `main`.

This release is for stores whose series come and go. A metrics compaction
sweep ends, is planned once, merges small compressed chunks size-tiered by
count, does not walk the chunk index at each step, and yields to reads in
flight (#92, #93, #94). Retention removes the series it has left nothing of
(#82), rollup chunks are merged (#81), and a changed window applies at the
next pass. Series discovery accepts time windows (#88), strict log time
bounds keep limit pushdown (#89), and unordered logs stream projected
blocks (#77). See the [changelog](../CHANGELOG.md) and the
[sweep evidence](evidence/2026-10-01_compaction_sweep_many_series.md).

## Local verification

All steps ran on Linux x86-64 with Rust 1.98.1 and an extension-enabled
`sqlite3` CLI, in one pass, after the version bump, on the release commit.

| Check | Result |
|---|---|
| Formatting | All eight Cargo workspaces passed. |
| Locked dependency checks | Root, servers, query harness, and the other detached tools passed under `--locked`. |
| Root workspace tests | 293 passed, 1 ignored. |
| Server workspace tests | 596 passed with extension-dependent ignored tests enabled. |
| Query harness | 65 tests passed; `contracts` and pinned oracle-manifest validation passed. |
| Public SQL recipes | 135 recipes / 173 statements passed through the real extension. |
| SQLite CLI suite | All sections passed. |
| Focused correctness | `r1`, `r2`, `r3`, `r4`, `r8`, and `logs-rich` passed. |
| Crash recovery | Five forced-kill rounds passed. |
| dbhealth | Separate build, load, sampling, schema, and lifecycle checks passed. |
| Executable onboarding documentation | Markdown examples passed first-result and cold-reopen checks. |
| Embedded Rust and direct libSQL | Embedded example and multi-connection libSQL reopen passed. |
| Lint and API documentation | Root, server, and query-harness Clippy and root and server rustdoc passed with warnings denied. |

Commands and prerequisites are maintained in [TESTING.md](../TESTING.md).
Query compatibility uses the checked-in immutable oracle corpora; this release
did not change or refresh upstream pins.

Not run for this release: the dispatchable CI workflows (query contracts,
production data-plane gate), which [RELEASING.md](RELEASING.md) leaves to
changes that skipped the local stack, and the release tool's local package
build. The tag-triggered artifact workflow below is the native build
evidence for `0.8.7`.

## Publication and downloaded binaries

[`v0.8.7` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.7)
on 2026-10-03 at 20:18:50 UTC. `main` was pushed before the annotated tag; the
tag resolves to the release commit above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/37150289021)
passed every native build, checksum, and publication job.

All five permanent assets were downloaded again and the four archives passed
`sha256sum -c` against the published `SHA256SUMS`; the `x86_64-unknown-linux-gnu`
archive inventory (`bin/` with the three APIs and `timeless-authctl`,
`lib/libtimeless_ext.so`, `install.sh`, `uninstall.sh`, manifest, SBOM, and
licenses) matched the documented target matrix, and its
`timeless-metrics-api --version` reports `0.8.7` at the release commit.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16193617 | `4d1b3b090c8875da662ce49951ff4f2a6b886c81fad0884c3000becf764e3641` |
| `aarch64-unknown-linux-gnu` | 15342031 | `b7ee1686ed87c3bb6042017138fbae21be8f0b294117a9280ec8aeacc10f7136` |
| `x86_64-apple-darwin` | 15379979 | `331dd3e058987a715fb79cff5ecc2256d645b3903211eb621be0f26a4cc7ccad` |
| `aarch64-apple-darwin` | 14205855 | `967f517eeb95079b89b2fe77724e53845e8417b2f8761b71b66b4575ca3c05f4` |

## Deployment

Not yet deployed. The `timeless-stack` image has not been rebuilt against
`v0.8.7`.

## Upgrade notes

- **The first sweep after the upgrade does the merging that was owed.** A
  store that accumulated small compressed chunks under an earlier release
  has them merged, a series at a time, in the first sweeps: on a store of
  3.3 million chunks of four points that is some 600 steps.
- **`compact-step` takes a cutoff.** A direct SQLite host that drives its own
  sweeps should pass the Unix time the sweep began as the last argument of
  every step of it (`compact-step:<series>:<points>:<bytes>:<cutoff>` or
  `compact-step:<series>:<cutoff>`). Without one, a store that is flushed
  between steps keeps a sweep going, as before.
- **A rollup step's budget is of chunks written.** `compact-step` looks at up
  to 64 times its budget of rollup groups to find those with a settled
  bucket.
- **Retention removes series.** A series with no raw chunk, no rollup chunk,
  and no buffered point is removed from the catalog. A series with a rollup
  chunk in a tier kept `forever` is kept forever.
- **Rewrites.** A point arriving in fixed 1,024-point batches is now
  rewritten 2.5 times over its life rather than 1.5; in exchange, chunks of
  a few points are merged at all.
- New statistics: `compaction_plans`, `compaction_planned_groups`,
  `retention_series_removed`, `rollup_merge_chunks_removed`,
  `rollup_merge_chunks_written` in `timeless_stats`; `compact_yield_count`
  and `compact_yield_total_ns` in the metrics server's statistics;
  `timeless_metrics_compaction_plans_total` and
  `timeless_metrics_compaction_planned_groups` on `/metrics`.
- Read the [upgrade guide](UPGRADE.md) and the
  [release changelog](../CHANGELOG.md) before replacing a deployment.
