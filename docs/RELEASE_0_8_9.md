# Release validation: 0.8.9

Date: 2026-10-04 UTC. Release source:
[`7126f899acdb53e4429c1bf3b7533b9fac9d75f8`](https://github.com/awksedgreep/timeless-libsql/commit/7126f899acdb53e4429c1bf3b7533b9fac9d75f8).

Both workspaces were bumped to `0.8.9`, path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification.

This release fixes selective metrics reads that became slower as unrelated
series accumulated (#116). The batch reader seeks past long chunk-index
gaps and keeps sequential scans for dense selections. Paired local SQL
benchmarks with 161 matching series in a 152,000-series catalog improved
8.2–8.4×; broad and half-catalog medians stayed within −2.4% to +3.2%.
These are local SQL measurements, not production HTTP latency claims. See
the [benchmark evidence](../tools/bench/results/2026-10-03_catalog_growth.md).

## Pre-tag verification

All local steps ran continuously on the exact release commit after the
version bump, using Linux x86-64, Rust 1.98.1, and an extension-enabled
`sqlite3` CLI. The complete pass finished at 01:55:41 UTC.

| Check | Result |
|---|---|
| Locked dependency checks and formatting | All eight Cargo workspaces passed. |
| Root workspace tests | 295 passed, 1 existing ignored test. |
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
| Local production fault gate | 120 seconds, all 12 fault scenarios passed, no failures. |

An initial pass exposed a stale `logs-rich` fixture: it supplied microseconds
to a legacy batch format whose documented wire unit is milliseconds. The
fixture now encodes milliseconds and verifies conversion into the table's
microseconds. The entire local pass and both dispatched CI gates were rerun
after that correction; only the corrected commit was tagged.

Both dispatched workflows passed on the release commit before tagging:

- [Query documentation contracts](https://github.com/awksedgreep/timeless-libsql/actions/runs/37169075541).
- [Production data-plane gate](https://github.com/awksedgreep/timeless-libsql/actions/runs/37169076829),
  including real-extension storage contracts and all 12 fault scenarios.

The local and CI fault gates each recovered 30,784 durable records per signal
and reported no failures. Commands and prerequisites are maintained in
[TESTING.md](../TESTING.md). Oracle corpora and upstream pins were unchanged.

## Publication and downloaded binaries

[`v0.8.9` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.9)
on 2026-10-04 at 02:15:43 UTC. `main` was pushed before the annotated tag,
which resolves to the release source above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/37169792903)
passed all four native builds, checksum verification, and publication.

All five permanent assets were downloaded after publication. Every archive
passed the outer checksum, exact file inventory, inner checksums, manifest
payload sizes and hashes, clean source identity, all four server identities,
extension capability identity, and SPDX 2.3 inventory checks. Every target
reports version `0.8.9` at the release source commit, with data ABI and SQL
surface version both `1`.

The downloaded Linux x86-64 binaries were also executed locally: every
`--version` matched its manifest, SQLite loaded the extension and returned
matching capabilities, and installation followed by removal preserved test
data and configuration. The native build jobs performed their own
identity and install/remove checks on all four platforms.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16217033 | `5b3c8ae6f40876398d9dc13945cd5303d86388ae8ccc8a743af3341adbd693d0` |
| `aarch64-unknown-linux-gnu` | 15359216 | `c8ccdc2da81767df0c2e026b52ae1fdc80d557bb5e175bbb00393b13490f5b0a` |
| `x86_64-apple-darwin` | 15399571 | `876ab160de8c5e54a32f9d2757c9f11a56039ba19f1974fb5fed65abe90056a8` |
| `aarch64-apple-darwin` | 14223009 | `b3184f6849f00b8295a9a76048a712f3cf7886c4d8b07f9297701531f9380fe9` |

The publication record was committed afterward; the immutable release tag
continues to identify the verified source commit above.

After the tag was pushed, `main` removed Intel macOS from future native
release bundles. This release retains its original four archives; the next
release matrix covers Linux x86-64, Linux AArch64, and macOS Apple Silicon.
Intel Mac users can build the extension and servers from source.
