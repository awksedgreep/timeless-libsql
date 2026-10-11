# Release validation: 0.8.17

Date: 2026-10-11 UTC. Release source:
[`e14ccd52236171c98b13e33d7506036250f8ec25`](https://github.com/awksedgreep/timeless-libsql/commit/e14ccd52236171c98b13e33d7506036250f8ec25).

Both workspaces were bumped to `0.8.17`, path-package lock entries were
refreshed without dependency upgrades, and the changelog and compatibility
inventory were updated before verification. `server_minimum_extension` stays
at `0.8.11`. `timeless-traces-api` gained tokio's `test-util` feature as a
development dependency only.

- **The rollup index is read from disk where the rest of the index is
  (#144).** A second partial index over rollup rows, and a raw index that
  covers what totals, retention and a compaction plan ask for.
- **A reader in another process has nothing to reload (#146)**, with the
  index on disk.
- **Compressed chunks merge to 2,048 points, and larger ones are cut down
  (#145).**
- **A rollup cycle passes over a tier it has already settled.**
- **`TIMELESS_METRICS_INDEX_CACHE`** chooses the metrics server's index mode.
- **Fixes:** servers create databases with `auto_vacuum = INCREMENTAL` (#79);
  OTLP/JSON `intValue` is a number (#80); a label index is not called stale
  after the newest series is removed; disk-mode chunk totals are moved
  rather than rescanned.

## Evidence

Each figure is from this build unless a version is named. Benchmark runs
were made one at a time on an otherwise idle 22-core host.

**Opening a large store.** An online backup of a live store: 701,977 series,
3,899,713 raw chunks, 5,453,199 rollup chunks, 3.1 GB. Time from process
start until the metrics server answers `/ready`, and anonymous resident
memory then:

| | ready | RssAnon |
|---|---:|---:|
| `unbounded` (0.8.16 and now) | 8.5–8.9 s | 1,945 MiB |
| `64MB`, 0.8.16 | 5.0–5.3 s | 475 MiB |
| `64MB`, now | 0.78–0.84 s | 40–45 MiB |
| first start with `TIMELESS_METRICS_INDEX_CACHE=64MB` on the `unbounded` store | 14.1–14.4 s | |

`series`, `raw_tier_chunks`, `rollup_chunks`, `rollup_single_bucket_chunks`
and `disk_points` read the same in both modes. A full maintenance sweep on
that copy with the index on disk: 248 steps, 17.4 s, no errors, raw chunks
3,899,713 to 3,484,902; 214 steps took 10–50 ms and 3 took 1–5 s (longest
2.85 s, tracked in #148).

**Maintenance cost.** `metrics_sparse_compaction_bench`, 547,763 series, 26
five-minute rounds, rollups on:

| | ingest | flush | sweep CPU | longest step | final file |
|---|---:|---:|---:|---:|---:|
| 0.8.16, index in memory | 36.9 s | 17.5 s | 119.1 s | 0.44 s | 492 MiB |
| now, index in memory | 37.3 s | 17.8 s | 112.8 s | 0.52 s | 492 MiB |
| now, index on disk (`64MB`) | 28.5 s | 20.7 s | 144.5 s | 1.27 s | 578 MiB |

All three end with the same 1,881,884 raw chunks and 547,734 rollup chunks.
Before the on-disk sweep changes in this release the third row's sweep took
about 33 s a round against about 7.

**Chunk size and point reads (#145).** A timeless-acct store sampled every
10 s for 10.6 days (229,813 series, 582M points), recompacted with each
merge target, then the 33 `timeless_latest` reads behind one screen of a
moment, about 2,365 rows:

| target | raw chunks | now | 1 day back | 3 days back | 6 days back |
|---|---:|---:|---:|---:|---:|
| 32,768 (0.8.16) | 251,573 | 99 ms | 433 ms | 866 ms | 977 ms |
| 4,096 | 327,497 | 99 ms | 158 ms | 273 ms | 228 ms |
| 2,048 (this release) | 418,565 | 101 ms | 139 ms | 184 ms | 176 ms |
| 1,024 | 610,248 | 103 ms | 134 ms | 164 ms | 150 ms |

**A reader beside a writer in another process (#146).** The same store, a
long-lived read-only connection reading once every 0.2 s while another
process writes, flushes and compacts every 0.5 s: with the index in memory,
reads that follow a change of chunk shape took a median 1.85 s (29 of them
in a minute); with it on disk, a median 42 ms and at most 56 ms (35 of
them).

**Compatibility.** The 0.8.16 extension opened a store carrying the new
indexes, wrote, flushed and compacted; this build then read it, with totals
equal to the rows.

## Pre-tag verification

Run on the release source after the bump, in one pass:

- `cargo check --workspace --locked` in the root, `servers/` and
  `tools/query-harness/`; `cargo fmt --check` for all eight workspaces
- root: build, 309 tests, clippy and rustdoc with warnings denied
- the extension under `--no-default-features --features embedded`: 101
  tests and clippy with warnings denied (new to the checklist; #134, #135)
- release builds of the extension, `dbhealth-ext` and the servers
- servers: 611 tests with `--include-ignored`, clippy, rustdoc
- `tests/cli.sh`; `tests/correctness.sh` for `r1 r2 r3 r4 r8 logs-rich`;
  `tests/dbhealth.sh`; `tests/crash.sh`
- query harness: tests, clippy, contracts, oracle validation, SQL recipes
  (135 recipes, 173 statements) and docs

Before the bump, the extension, CLI, correctness (all six sections) and
crash suites were also run with every metrics table forced to read its
index from disk, at budgets of 64 MB and zero. They pass, apart from four
extension tests that assert the in-memory mode itself.

## Known limits

- With the index on disk, the steps of a sweep that plan take 1.3–2.9 s on
  these stores against 0.5–0.7 s in memory, and reads wait behind a step
  (#148, #143).
- With the index in memory, a second process reading a table still reloads
  the whole index when the writer reshapes chunks (#146 is fixed for the
  index on disk).
- A read of one metric still walks every series of that metric: about 20 ms
  a read for a metric with 14,917 series in memory and about 40 ms on disk,
  whatever the window returns (#116).
- Chunks written under the old merge target are cut down a bounded step at
  a time by maintenance; until a store has been through that, its older
  point reads cost what they did.
- Databases created by earlier servers keep `auto_vacuum = 0` until a
  one-time `VACUUM`.

## Publication and downloaded binaries

[`v0.8.17` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.17)
on 2026-10-11 at 19:37:36 UTC. `main` was pushed before the annotated tag,
which resolves to the release source above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/38168374564)
passed all three native builds, checksum verification, and publication.

All four permanent assets were downloaded after publication. Every archive
passed the outer checksum and its inner checksums (11 files each), and every
archive manifest reports version `0.8.17` at the release source commit. The
downloaded Linux x86-64 `timeless-metrics-api` binary was executed, and its
`--version` reported the same version and commit.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16456452 | `dfca700aa6deefe5e541e5ad70e7f5a25a8dfa96327694a7a5c58f4188989204` |
| `aarch64-unknown-linux-gnu` | 15569573 | `8ceac8c6ea2d48a691c7bc8216b3b14c2591a3b85fa95eeac93f75b955f8a033` |
| `aarch64-apple-darwin` | 14419427 | `46747c4e14d69273a34822a78ec9d2650dbc140aa7a25ba4d326021e82e0d344` |

The publication record was committed afterward; the immutable release tag
continues to identify the verified source commit above.
