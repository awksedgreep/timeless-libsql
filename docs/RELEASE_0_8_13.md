# Release validation: 0.8.13

Date: 2026-10-10 UTC. Release source:
[`a4b176cc8de1191801fa634a8a1afda880b02630`](https://github.com/awksedgreep/timeless-libsql/commit/a4b176cc8de1191801fa634a8a1afda880b02630).

Both workspaces were bumped to `0.8.13`, path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. `server_minimum_extension` stays
at `0.8.11`: the metrics server sends `index_cache` only to an extension that
advertises `query_surfaces.timeless_metrics.index_cache`.

This release moves the metrics series catalog onto disk (#132). Every metrics
table keeps a label index beside its series catalog, and a new `index_cache`
setting chooses how much is held in memory: a byte budget reads the catalog
from disk behind a resolve cache of that size, and `unbounded` also holds the
whole catalog in memory. A table created without the setting uses `64MB`; the
bundled metrics server asks for `unbounded`. Selective label selectors now go
through the label index in both modes (#133). See the
[changelog](../CHANGELOG.md) and the
[measurements](../tools/bench/results/2026-10-10_index_cache_engine.md).

Index bytes are not compression bytes: `bytes_on_disk` and `bytes_per_point`
remain chunk payload only, pinned by a test that compares two tables holding
the same points with label indexes of very different size.

## Pre-tag verification

The complete local checklist ran in one continuous pass on the exact release
commit after the version bump, with the build identity set to that commit,
using Linux x86-64, Rust 1.98.1, and an extension-enabled `sqlite3` CLI. It
finished at 16:21 UTC.

| Check | Result |
|---|---|
| Locked dependency checks and formatting | All eight Cargo workspaces passed. |
| Root workspace tests | 305 passed, 1 existing ignored test. |
| Server workspace tests | 601 passed with extension-dependent ignored tests enabled. |
| Query harness | 65 tests passed; contracts and pinned oracle-manifest validation passed. |
| Public SQL recipes | 135 recipes / 173 statements passed through the real extension. |
| SQLite CLI suite | All sections passed. |
| Focused correctness | `r1`, `r2`, `r3`, `r4`, `r8`, and `logs-rich` passed. |
| Crash recovery | Five forced-kill rounds passed. |
| dbhealth | Separate extension build and full shell suite passed. |
| Executable onboarding documentation | First-result and cold-reopen checks passed. |
| Embedded Rust and direct libSQL | Embedded example and multi-connection libSQL reopen passed. |
| Lint and API documentation | Root, server, and query-harness Clippy and root and server rustdoc passed with warnings denied. |

A first pass of the checklist, on the version-bump commit `fd48051`, failed
two correctness sections, and that commit was not tagged:

- `r2` (`corrupt_legacy`): with the new default, a table in disk mode skipped
  the check that every persisted chunk belongs to a known series, and the
  legacy-registry path, so a store with a missing series table and a corrupt
  legacy registry opened and upgraded without error. Fixed in `a4b176c`: a
  disk catalog is trusted only when it is non-empty and names the series of
  every chunk loaded at open; otherwise the open takes the existing path,
  which migrates a legacy registry or refuses.
- `r3`: the schema inventory did not list the two label index tables.

The whole checklist was rerun on `a4b176c`; only that commit was tagged.

Under the `embedded` feature, which this checklist does not build, the
extension's unit tests pass 91 of 92: the one failure is the logs limit test
tracked in #134, unrelated to this release, and #135 tracks a Clippy lint in
the same configuration. The dispatched CI gates and the local production
fault gate were not run; per [RELEASING.md](RELEASING.md) the local pass is
the gate and the tag-triggered build is CI's unique job. Oracle corpora and
upstream pins were unchanged.

## Publication and downloaded binaries

[`v0.8.13` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.13)
on 2026-10-10 at 16:28:33 UTC. `main` was pushed before the annotated tag,
which resolves to the release source above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/38067350785)
passed all three native builds, checksum verification, and publication.

All four permanent assets were downloaded after publication. Every archive
passed the outer checksum and its inner checksums (11 files each), and every
archive manifest reports version `0.8.13` at the release source commit. The
downloaded Linux x86-64 `timeless-metrics-api` and `timeless-logs-api`
binaries were executed and their `--version` reported the same version and
commit. The downloaded Linux x86-64 extension was loaded into `sqlite3`: its
capability document advertises `timeless_metrics.index_cache`, a table
created without the argument reports `index_cache = 67108864` and
`series_on_disk = 1`, and one created with `index_cache='unbounded'` reports
`unbounded` and `series_on_disk = 0`.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16365421 | `2bda6647d7aad6f437b937917afbb63680feb623366f4052968c0ed530ec312e` |
| `aarch64-unknown-linux-gnu` | 15497947 | `89590ffa4fda328327b8388434facfb69f825ce4ae3e21cb014ab54db879e448` |
| `aarch64-apple-darwin` | 14341873 | `9eb6076144a1c7aa073eff404650cb36b46280f5202ab37daccfa7c96f2a37b7` |

The publication record was committed afterward; the immutable release tag
continues to identify the verified source commit above.
