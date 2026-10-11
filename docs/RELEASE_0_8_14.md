# Release validation: 0.8.14

Date: 2026-10-11 UTC. Release source:
[`b6660e2fa183eb4afefb73d4f1edd01d3f293541`](https://github.com/awksedgreep/timeless-libsql/commit/b6660e2fa183eb4afefb73d4f1edd01d3f293541).

Both workspaces were bumped to `0.8.14`, path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. `server_minimum_extension` stays
at `0.8.11`: the metrics server sends `flush_interval` only to an extension
that advertises `query_surfaces.timeless_metrics.flush_interval`.

This release finishes moving the metrics index onto disk (#132). A table
whose catalog is on disk (`index_cache` a size) now also reads its raw chunk
index from the store's chunk rows, so its memory no longer grows with the
number of series or chunks; write buffers stop holding memory for series
that are not being written; and reads of such a table are faster where they
are selective. A metrics table used directly through the extension now
flushes and compacts for itself on a `flush_interval` (60 seconds unless
set); the bundled metrics server, which has its own timers, sets it `off`.
See the [changelog](../CHANGELOG.md) and the measurements under
`tools/bench/results/2026-10-10_chunk_index_engine.md` and
`tools/bench/results/2026-10-10_write_buffer_release/`.

Index bytes are not compression bytes: `bytes_on_disk` and `bytes_per_point`
remain chunk payload only, pinned by the test added in 0.8.13.

## Pre-tag verification

The complete local checklist ran in one continuous pass on the exact release
commit after the version bump, with the build identity set to that commit,
using Linux x86-64, Rust 1.98.1, and an extension-enabled `sqlite3` CLI. It
finished at 04:31 UTC.

| Check | Result |
|---|---|
| Locked dependency checks and formatting | All eight Cargo workspaces passed. |
| Root workspace tests | 306 passed, 1 existing ignored test. |
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

The bundled server keeps its catalog in memory, so the checklist alone does
not exercise the disk index through the server and CLI suites. On the last
code commit before the bump (`8e00ce2`), the root, server, query-harness,
CLI, dbhealth, correctness, crash, embedded and libSQL suites were therefore
also run against a build that forced every metrics table, the server's
included, onto the disk catalog and disk chunk index with a 64 MB cache;
all 20 steps passed. The same run with no cache passed on `3281c98`. The
forcing edit was never committed.

Under the `embedded` feature, which this checklist does not build, the
extension's unit tests pass 96 of 97: the one failure is the logs limit test
tracked in #134, unrelated to this release, and #135 tracks a Clippy lint in
the same configuration. The dispatched CI gates and the local production
fault gate were not run; per [RELEASING.md](RELEASING.md) the local pass is
the gate and the tag-triggered build is CI's unique job. Oracle corpora and
upstream pins were unchanged.

Known limits of what shipped: a table has no clock, so a buffered point is
flushed by the first write after its `flush_interval`, not by the interval
alone; the selection cache's invalidation when another process writes the
same file relies on the existing catalog-generation check and has no test of
its own; and compaction and retention sweep times at millions of series in
disk mode have not been measured.

## Publication and downloaded binaries

[`v0.8.14` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.14)
on 2026-10-11 at 04:38:25 UTC. `main` was pushed before the annotated tag,
which resolves to the release source above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/38111881077)
passed all three native builds, checksum verification, and publication.

All four permanent assets were downloaded after publication. Every archive
passed the outer checksum and its inner checksums (11 files each), and every
archive manifest reports version `0.8.14` at the release source commit. The
downloaded Linux x86-64 `timeless-metrics-api` and `timeless-logs-api`
binaries were executed and their `--version` reported the same version and
commit. The downloaded Linux x86-64 extension was loaded into SQLite: its
capability document advertises `timeless_metrics.flush_interval` and
`index_cache`; a table created without arguments reports
`flush_interval = 60`, `series_on_disk = 1` and `chunks_on_disk = 1`; and one
created with `flush_interval='off', index_cache='unbounded'` reports
`flush_interval = 0`, `series_on_disk = 0` and `chunks_on_disk = 0`.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16410127 | `a500724c14938ab9d8c4dc0687249d5ef3be055afbbfd69b78f6038b50a0c0b4` |
| `aarch64-unknown-linux-gnu` | 15533236 | `0e21fc1ab6f9f7da4f14a9aedd84a83f604027628bdbdceda2a8756c4b9ce5a2` |
| `aarch64-apple-darwin` | 14378518 | `51766a96b1d9ec2101062a0508105282f1e4b819a5f9a8b6af018c79be2fd47f` |

The publication record was committed afterward; the immutable release tag
continues to identify the verified source commit above.
