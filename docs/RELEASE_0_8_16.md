# Release validation: 0.8.16

Date: 2026-10-11 UTC. Release source:
[`e7badd45ada233f327f108a44bce9fccbebc7413`](https://github.com/awksedgreep/timeless-libsql/commit/e7badd45ada233f327f108a44bce9fccbebc7413).

Both workspaces were bumped to `0.8.16`, path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. `server_minimum_extension` stays
at `0.8.11`. The servers gained a direct `libc` dependency, which was already
in the lockfile.

This release follows an investigation of 7–10 second writer stalls on a live
700,000-series stack (#138 to #142).

- **The chunk index exists only where it is read (#138).** With the catalog
  in memory a metrics table has no `(series_id, ts_min)` index; with it on
  disk the index covers raw rows only. Every flush and compaction step used
  to rewrite essentially the whole index.
- **Durations, and time blocked, are reported (#141).** Histograms for
  maintenance steps, flushes, and reads, with the writer thread's CPU time.
- **A write refused as busy is tried again (#140).**
- **Servers warn at startup on btrfs with copy-on-write (#142).**

## Evidence

On a copy of the live store (700,894 series, 7.85M chunk rows after 0.8.15's
rollup merge), replaying real 400,000-point scrapes through the server:

| | written per flush |
|---|---:|
| 0.8.14 (16.7M chunk rows) | 620 MiB |
| 0.8.16 | 46–49 MiB |

One such flush in plain SQLite on that store writes 237 MB of WAL with the
old index, 184 MB with it rebuilt compact, 121 MB with a raw-only index, and
24.5 MB with none. The store opened in 20 s and dropped the index at that
open. With the stats endpoint polled every 3 s through a full sweep, this
build and 0.8.15 both finished with no errors and identical results (628
steps, the same final chunk count).

The startup warning was checked on a btrfs host: a new database in a
copy-on-write directory and an existing copy-on-write file both warn; a new
database in a `chattr +C` directory does not.

These are measurements on one Linux x86-64 host.

## Pre-tag verification

All local steps ran in one continuous pass on the release tree after the
version bump, using Linux x86-64, Rust 1.98.1, and an extension-enabled
`sqlite3` 3.53.4 CLI.

| Check | Result |
|---|---|
| Locked dependency checks and formatting | All eight Cargo workspaces passed. |
| Root workspace tests | 307 passed, 1 existing ignored test. |
| Server workspace tests | 607 passed with extension-dependent ignored tests enabled. |
| Query harness | Tests passed; contracts and pinned oracle-manifest validation passed. |
| Public SQL recipes | 135 recipes / 173 statements passed through the real extension. |
| SQLite CLI suite | All sections passed (214 checks). |
| Focused correctness | `r1`, `r2`, `r3`, `r4`, `r8`, and `logs-rich` passed. |
| Crash recovery | Five forced-kill rounds passed. |
| dbhealth | Separate extension build and full shell suite passed. |
| Executable onboarding documentation | First-result and cold-reopen checks passed. |
| Lint and API documentation | Root, server, and query-harness Clippy and root and server rustdoc passed with warnings denied. |
| Extension tests, `embedded` feature | 97 passed; 1 failed, the known #134. |

For the chunk index change (#138), before the later commits, the server,
CLI, correctness, and crash suites were also run with the on-disk catalog
forced for every table, at a 64 MB cache and at 0: all passed. With that
forcing, three `embedded` tests that assert in-memory behaviour fail, on the
unchanged code as well; the new index test fails there for the same reason.

The dispatched CI gates were not run; per [RELEASING.md](RELEASING.md) the
local pass is the gate.

## Known limits

- An engine first opened from inside a running statement (a table-valued
  function) cannot change the schema, so the chunk index is settled at the
  next open that can. Until then a memory-mode table keeps an index it does
  not need, or a disk-mode table reads its chunk index from memory.
- A table whose catalog is on disk still rewrites its raw-only index on each
  flush.
- Reads are still refused for the length of a write transaction (#140's
  remaining design question); what changed is that the write side no longer
  fails when a read outlasts its wait.

## Publication and downloaded binaries

[`v0.8.16` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.16)
on 2026-10-11 at 08:25:40 UTC. `main` was pushed before the annotated tag,
which resolves to the release source above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/38124405431)
passed all three native builds, checksum verification, and publication.

All four permanent assets were downloaded after publication. Every archive
passed the outer checksum and its inner checksums (11 files each), and every
archive manifest reports version `0.8.16` at the release source commit. The
downloaded Linux x86-64 `timeless-metrics-api` binary was executed, and its
`--version` reported the same version and commit.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16425878 | `def8cff9561cde51b913b1edab3ced4516d873fb09a163ef98a4166ca46bafea` |
| `aarch64-unknown-linux-gnu` | 15541903 | `8bec64eaee8625c25ad5f9a928db2228762766f61ef3f97289cf115a2f90e27d` |
| `aarch64-apple-darwin` | 14394367 | `747b81f69b6fa81ceed8c043dc3803fa9d09e97c2df5e4811f0225df9b3e61b5` |

The publication record was committed afterward; the immutable release tag
continues to identify the verified source commit above.
