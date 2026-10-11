# Release validation: 0.8.15

Date: 2026-10-11 UTC. Release source:
[`f705d60fe70ef8c10be25585b95fbd6085398022`](https://github.com/awksedgreep/timeless-libsql/commit/f705d60fe70ef8c10be25585b95fbd6085398022).

Both workspaces were bumped to `0.8.15`, path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. `server_minimum_extension` stays
at `0.8.11`.

This release fixes how the rollup chunks of a live series merge (#137). A
group's chunks were merged as one run in time order, and only once the run
was twice its largest member, so a 64-bucket chunk held back every newer
one-bucket chunk until there were 64 of them. On a live 700,894-series store
after 61 hours, 9,202,112 of 9,831,492 hourly rollup chunks held one bucket,
and the rollup tiers held 663 MB of payload against 395 MB for 464 million raw
points. Runs are now built from the newest chunk backwards over neighbours of
similar size, so a group holds a logarithmic number of chunks.

## Evidence

A new engine test rolls three series through 200 buckets: a group never
exceeds 16 chunks (71 under the previous rule), and every settled bucket reads
back once, in order.

On a copy of the live store, with the 0.8.14 server and this extension:

| | before | after the first sweep |
|---|---:|---:|
| rollup chunks | 11,703,505 | 3,259,957 |
| hourly chunks | 9,831,492 | 1,387,944 |
| hourly buckets | 43,950,156 | 43,950,156 |

The sweep took 906 steps and 134 s, with a longest step of 0.97 s, and wrote
about 6.5 GB, once. `timeless_rollup` sum, count, and max for three sampled
series were byte-identical before and after, and `PRAGMA quick_check` passed.
These are measurements on one Linux x86-64 host.

## Pre-tag verification

All local steps ran in one continuous pass on the release tree after the
version bump, using Linux x86-64, Rust 1.98.1, and an extension-enabled
`sqlite3` 3.53.4 CLI.

| Check | Result |
|---|---|
| Locked dependency checks and formatting | All eight Cargo workspaces passed. |
| Root workspace tests | 307 passed, 1 existing ignored test. |
| Server workspace tests | 601 passed with extension-dependent ignored tests enabled. |
| Query harness | Tests passed; contracts and pinned oracle-manifest validation passed. |
| Public SQL recipes | 135 recipes / 173 statements passed through the real extension. |
| SQLite CLI suite | All sections passed (214 checks). |
| Focused correctness | `r1`, `r2`, `r3`, `r4`, `r8`, and `logs-rich` passed. |
| Crash recovery | Five forced-kill rounds passed. |
| dbhealth | Separate extension build and full shell suite passed. |
| Executable onboarding documentation | First-result and cold-reopen checks passed. |
| Lint and API documentation | Root, server, and query-harness Clippy and root and server rustdoc passed with warnings denied. |

Not run for this release: the suites with the on-disk catalog forced for every
table, and the `embedded`-feature extension tests. The change is confined to
rollup merge planning, and the rollup index is held in memory in both catalog
modes. The dispatched CI gates were not run; per [RELEASING.md](RELEASING.md)
the local pass is the gate.

## Publication and downloaded binaries

[`v0.8.15` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.15)
on 2026-10-11 at 06:28:53 UTC. `main` was pushed before the annotated tag,
which resolves to the release source above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/38117795247)
passed all three native builds, checksum verification, and publication.

All four permanent assets were downloaded after publication. Every archive
passed the outer checksum and its inner checksums (11 files each), and every
archive manifest reports version `0.8.15` at the release source commit. The
downloaded Linux x86-64 `timeless-metrics-api` binary was executed, and its
`--version` reported the same version and commit.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16411747 | `0c21d7fbd3c7747c410bbb99718236838aa2cd9adc9b6ef80bf55a677598ac00` |
| `aarch64-unknown-linux-gnu` | 15533968 | `8ae8ee0b227cd35cc021d3cf3f326f54e8ba49747c16bc698d2c1f549a4b4ae1` |
| `aarch64-apple-darwin` | 14380226 | `a52e8dc7933b91603c9d27390af9309bbc8c0c158cfab7133a238c6c7c244473` |

The publication record was committed afterward; the immutable release tag
continues to identify the verified source commit above.
