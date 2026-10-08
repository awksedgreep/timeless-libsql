# Release validation: 0.8.11

Date: 2026-10-08 UTC. Release source:
[`ad521c07d67438812f07e4acdd8805f2f5e0f6b1`](https://github.com/awksedgreep/timeless-libsql/commit/ad521c07d67438812f07e4acdd8805f2f5e0f6b1).

Both workspaces were bumped to `0.8.11`, path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. `server_minimum_extension` moved
to `0.8.11`: servers now send `compact-step`'s sweep field, which older
extensions reject.

This release makes metrics maintenance affordable at fleet scale: hundreds of
thousands of series, at most one sample each per scrape (#118, #126). It was
measured with `metrics_sparse_compaction_bench` (#119), a synthetic copy of a
live 547,763-series fleet scraped every five minutes with the 1h/1d/30d rollup
ladder, run through the server's `Storage` write path. The comparison is
against a build with only #120, which was itself ~10× below 0.8.10 in sweep
CPU. Over 37 five-minute slots:

| | #120 only | 0.8.11 |
|---|---:|---:|
| sweep CPU, total | 2,712 s | 236 s |
| sweep wall, total | 6,870 s | 309 s |
| bytes written | 523 GB | 24 GB |
| transactions | 310,580 | 5,244 |
| worst sweep | 234 s | 29 s |

Over 72 slots, the longest single maintenance step was 0.63 s. Before, the
4-hourly 1h-tier rollup merge ran as one 50.6 s step. These are benchmark
measurements on one Linux x86-64 host, not production claims.

## Pre-tag verification

All local steps ran in one continuous pass on the release tree after the
version bump, using Linux x86-64, Rust 1.98.1, and an extension-enabled
`sqlite3` 3.53.4 CLI.

| Check | Result |
|---|---|
| Locked dependency checks and formatting | All eight Cargo workspaces passed. |
| Root workspace tests | 304 passed, 1 existing ignored test. |
| Server workspace tests | 596 passed with extension-dependent ignored tests enabled. |
| Query harness | 65 tests passed; contracts and pinned oracle-manifest validation passed. |
| SQLite CLI suite | All sections passed (212 checks). |
| Focused correctness | `r1`, `r2`, `r3`, `r4`, `r8`, and `logs-rich` passed. |
| Crash recovery | Five forced-kill rounds passed. |
| dbhealth | Separate extension build and full shell suite passed. |
| Executable onboarding documentation | First-result and cold-reopen checks passed. |
| Lint and API documentation | Root, server, and query-harness Clippy and root and server rustdoc passed with warnings denied. |

`tests/cli.sh` caught a defect during development that the Rust suites did
not: the raw-run minimum (#122), first applied to the whole engine, also
kept an explicit `compact` from compressing a series' first raw chunks. It
was fixed before the tag (a7071b9). The minimum now applies only to
scheduled `compact-step`s.

The dispatched CI gates and the local production fault gate were not run for
this release; per [RELEASING.md](RELEASING.md) the local pass is the gate and
the tag-triggered build is CI's unique job. Oracle corpora and upstream pins
were unchanged.

## Publication and downloaded binaries

[`v0.8.11` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.11)
on 2026-10-08 at 01:11:56 UTC. `main` was pushed before the annotated tag,
which resolves to the release source above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/37711076332)
passed all three native builds, checksum verification, and publication.

All four permanent assets were downloaded after publication. Every archive
passed the outer checksum and its inner checksums (11 files each), and every
archive manifest reports version `0.8.11` at the release source commit. The
downloaded Linux x86-64 `timeless-metrics-api` binary was executed, and its
`--version` reported the same version and commit.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16228572 | `127c95c31cd5704b4c55887e74f5f3a0fa2db8322aa5c8cc2ea82c6b781d60d7` |
| `aarch64-unknown-linux-gnu` | 15371350 | `84dbcf4f9148a73fd112d9c3f32befde11b306248ae32ff313cde5fd97f4a71d` |
| `aarch64-apple-darwin` | 14231050 | `478dc78045b79473de7b495b58edda09a0ef6078970b32b578f794042605effb` |

The publication record was committed afterward; the immutable release tag
continues to identify the verified source commit above.
