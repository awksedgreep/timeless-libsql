# Release validation: 0.8.10

Date: 2026-10-07 UTC. Release source:
[`01630961e1a80a48c640ea3eacbe39780f72f4cb`](https://github.com/awksedgreep/timeless-libsql/commit/01630961e1a80a48c640ea3eacbe39780f72f4cb).

Both workspaces were bumped to `0.8.10`, path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification.

This release fixes a metrics compaction step whose cost grew with the whole
store (#118). To decide which replaced chunks could be deleted, every bounded
step walked the entire chunk index. On a fleet writing ~390,000 points every
five minutes across ~1 million series, a 64-series step took about a second
and the sweep never drained. A chunk stored as its own row is now deleted
directly; the index is consulted only for chunks packed into a shared file.
In an in-process engine measurement, one 64-series step fell from 1,142 ms to
1.3 ms at 4.8 million chunks, and from 40 ms to 1.1 ms at 300,000, so it no
longer depends on index size. These are engine-level measurements, not
production HTTP or end-to-end sweep claims.

## Pre-tag verification

All local steps ran in one continuous pass on the exact release commit after
the version bump, using Linux x86-64, Rust 1.98.1, and an extension-enabled
`sqlite3` CLI. The pass finished at 19:16:04 UTC.

| Check | Result |
|---|---|
| Locked dependency checks and formatting | All eight Cargo workspaces passed. |
| Root workspace tests | 296 passed, 1 existing ignored test. |
| Server workspace tests | 596 passed with extension-dependent ignored tests enabled. |
| Query harness | 65 tests passed; contracts and pinned oracle-manifest validation passed. |
| Public SQL recipes | 135 recipes / 173 statements passed through the real extension. |
| SQLite CLI suite | All sections passed. |
| Focused correctness | `r1`, `r2`, `r3`, `r4`, `r8`, and `logs-rich` passed. |
| Crash recovery | Five forced-kill rounds passed. |
| dbhealth | Separate extension build and full shell suite passed. |
| Executable onboarding documentation | First-result and cold-reopen checks passed. |
| Embedded Rust and direct libSQL | Embedded example and multi-connection libSQL reopen passed. |
| Lint and API documentation | Root, server, and query-harness Clippy and root and server rustdoc passed with warnings denied. |

The new store-seam test (a partial step keeps a batch file its unreached
chunks still use) was confirmed to fail when the fix is changed to delete
every replaced unit unconditionally.

The dispatched CI gates and the local production fault gate were not run for
this release; per [RELEASING.md](RELEASING.md) the local pass is the gate and
the tag-triggered build is CI's unique job. Oracle corpora and upstream pins
were unchanged.

While writing the new test, a pre-existing FsStore defect was found and is
not addressed here: after a partial compaction leaves a shared batch file in
place, reopening the store resurrects the replaced chunks from that file, so
the compacted series returns its points twice. It affects only the filesystem
backend, not the SQLite shadow-table store the extension and servers use.

## Publication and downloaded binaries

[`v0.8.10` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.10)
on 2026-10-07 at 19:24:48 UTC. `main` was pushed before the annotated tag,
which resolves to the release source above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/37673216054)
passed all three native builds, checksum verification, and publication.

All four permanent assets were downloaded after publication. Every archive
passed the outer checksum and its inner checksums (11 files each), and every
archive manifest reports version `0.8.10` at the release source commit. The
downloaded Linux x86-64 `timeless-metrics-api` binary was executed and its
`--version` reported the same version and commit.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16216135 | `4d541628935014676f018905b0614d75a535d18bd8785555eef154460d2f1cc1` |
| `aarch64-unknown-linux-gnu` | 15363419 | `272d96262f3cd31491b997b4568fcea6d5281364dcdf81d3a360cc8285f57612` |
| `aarch64-apple-darwin` | 14223935 | `5d45a05dbd1a62d3d54795afd317cd19320324a93f2a63b6fd7991d228d92028` |

The publication record was committed afterward; the immutable release tag
continues to identify the verified source commit above.
