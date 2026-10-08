# Release validation: 0.8.12

Date: 2026-10-08 UTC. Release source:
[`c75b3ebe06f735361f3c09d4828c167522eb936c`](https://github.com/awksedgreep/timeless-libsql/commit/c75b3ebe06f735361f3c09d4828c167522eb936c).

Both workspaces were bumped to `0.8.12`, path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. `server_minimum_extension` stays
at `0.8.11`: the label-index discovery path is used only when the extension
advertises it, and the #129 and #130 changes are server-only.

This release answers selector-less metrics label discovery from the label
index (2.7–3.6 s to 5–10 ms on a 628k-series live store), stops the idle
10-second metrics flush from walking the whole store (#125), accepts the
VictoriaMetrics/VictoriaLogs request forms `match[]` on export and
`query`/`start`/`end`/`limit` on `/select/logsql/query` by GET and POST
(#129), and reads LogsQL exact filters on fields other than `level` in pages
with posting-list pruning on any configured index key (#130). See the
[changelog](../CHANGELOG.md).

## Pre-tag verification

The complete local checklist ran in one continuous pass on the exact release
commit after the version bump, with the build identity set to that commit,
using Linux x86-64, Rust 1.98.1, and an extension-enabled `sqlite3` CLI. It
finished at 14:50 UTC. An earlier independent pass of the same checklist on
the same tree, made before the bump was committed, also passed every step.

| Check | Result |
|---|---|
| Locked dependency checks and formatting | All eight Cargo workspaces passed. |
| Root workspace tests | 305 passed, 1 existing ignored test. |
| Server workspace tests | 600 passed with extension-dependent ignored tests enabled. |
| Query harness | 65 tests passed; contracts and pinned oracle-manifest validation passed. |
| Public SQL recipes | 135 recipes / 173 statements passed through the real extension. |
| SQLite CLI suite | All sections passed. |
| Focused correctness | `r1`, `r2`, `r3`, `r4`, `r8`, and `logs-rich` passed. |
| Crash recovery | Five forced-kill rounds passed. |
| dbhealth | Separate extension build and full shell suite passed. |
| Executable onboarding documentation | First-result and cold-reopen checks passed. |
| Embedded Rust and direct libSQL | Embedded example and multi-connection libSQL reopen passed. |
| Lint and API documentation | Root, server, and query-harness Clippy and root and server rustdoc passed with warnings denied. |

The dispatched CI gates and the local production fault gate were not run;
per [RELEASING.md](RELEASING.md) the local pass is the gate and the
tag-triggered build is CI's unique job.

## Publication and downloaded binaries

[`v0.8.12` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.12)
on 2026-10-08 at 14:53:07 UTC. `main` was pushed before the annotated tag,
which resolves to the release source above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/37795289162)
passed all three native builds, checksum verification, and publication.

All four permanent assets were downloaded after publication. Every archive
passed the outer checksum and its inner checksums (11 files each), and every
archive manifest reports version `0.8.12` at the release source commit. The
downloaded Linux x86-64 `timeless-logs-api` and `timeless-metrics-api`
binaries were executed and their `--version` reported the same version and
commit.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16263445 | `10e7bea8568864dec1d9d69da009f515cf3d9e433d1106dde622970112c9dd84` |
| `aarch64-unknown-linux-gnu` | 15399573 | `e1d241b3c2b0b4e6503d7d406b04daba25f15dd8e4c7c97f018046fd2d8a0c6e` |
| `aarch64-apple-darwin` | 14256038 | `c6a52075cfd4851e8de21ca2be18bb97cfa57c2cfd16c7df40e7f4db34a77eb5` |

The publication record was committed afterward; the immutable release tag
continues to identify the verified source commit above.
