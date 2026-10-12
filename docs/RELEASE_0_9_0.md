# Release validation: 0.9.0

Date: 2026-10-12 UTC. Release source:
[`44508fee6090d648c1f36c0757e5df208ddcbdb7`](https://github.com/awksedgreep/timeless-libsql/commit/44508fee6090d648c1f36c0757e5df208ddcbdb7).

Both workspaces were bumped to `0.9.0`, a new compatibility line: the
capability document gained `stored_at_commit` and
`reads_without_writer_gate` under `query_surfaces.timeless_metrics`, and
`extension_minimum_server` and `server_minimum_extension` both moved to
`0.9.0` with their guard tests. Path-package lock entries were refreshed
without dependency upgrades. The data ABI is 1, as before.

The state before this work is the tag `archive/in-memory-index` (v0.8.17).

- **A metrics point is stored when its transaction commits.** Write buffers
  no longer outlive a transaction; the flush timer and the buffer half of
  the transaction journal are gone.
- **One index, read from the database (#149).** No table's catalog, chunk
  index or rollup index is copied into memory. `index_cache` bounds a cache
  of series identities and selections.
- **A metrics read does not wait for a writer (#143).** No read permit and
  no engine lock; a read sees its connection's snapshot.
- **A sweep is planned a stretch of series at a time (#148).**
- **Batched reads fetch labels only for the series that answer.**
- The engine source went from 9,229 lines to 6,912; across the crates,
  servers and tools the change is 4,030 lines added and 7,993 removed.

## Evidence

Each figure is from this build unless a version is named. Benchmark runs
were made one at a time. The host was busier than when the 0.8.17 record
was made, so 0.8.17 was run again beside this build wherever the two are
compared; those are the 0.8.17 figures here.

**Opening a large store.** An online backup of a live store that 0.8.17
held in memory: 702,988 series, 3,965,046 raw chunks, 3,910,948 rollup
chunks, 3.1 GB. Time from process start until the metrics server answers
`/ready`, anonymous resident memory then, and the median of seven reads:

| | 0.8.17, index in memory | 0.8.17, `64MB` | 0.9.0 |
|---|---:|---:|---:|
| ready | 7.87 s | 0.70 s | 0.82 s |
| RssAnon at ready | 1,961 MiB | 77 MiB | 77 MiB |
| RssAnon after the reads | 2,001 MiB | 123 MiB | 123 MiB |
| one series, instant | 0.84 ms | 0.77 ms | 0.80 ms |
| one series, 24 h at 5 min | 0.88 ms | 0.70 ms | 0.85 ms |
| `count()` over 98,000 series | 67 ms | 104 ms | 103 ms |

Every answer and every total (`series`, points, raw and rollup chunks) is
the same in all three. The first start of 0.9.0 on that store built the two
chunk indexes and was ready in 3.92 s; the file did not grow, having free
pages.

**Maintenance cost.** `metrics_sparse_compaction_bench`, 547,763 series, 26
five-minute rounds, rollups on, the two runs back to back:

| | ingest + flush CPU | sweep CPU | writer CPU | longest step | final file |
|---|---:|---:|---:|---:|---:|
| 0.8.17, index in memory | 58.7 s | 117.4 s | 176.2 s | 605 ms | 492 MiB |
| 0.9.0 | 47.2 s | 136.6 s | 183.8 s | 233 ms | 578 MiB |

Both end with the same 1,881,884 raw chunks and 547,734 rollup chunks. The
larger file is the two chunk indexes. Before sweeps were planned in
stretches the same build's longest step was 1,251 ms and its sweep CPU
155.7 s.

**Reads beside writes.** The same benchmark, eight rounds, with four
readers each asking for one series over HTTP every 10 ms while the rounds
ingest and sweep:

| | reads | refused | retries | p50 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| 0.8.17, index in memory | 3,420 | 38 | 268,243 | 21 ms | 5,091 ms | 6,226 ms |
| 0.8.17, `64MB` | 3,425 | 0 | 101,064 | 23 ms | 1,048 ms | 3,512 ms |
| 0.9.0 | 18,698 | 0 | 0 | 0.9 ms | 2.5 ms | 134 ms |

Eighteen of the 18,698 took over 100 ms; where that time goes has not been
found (it is not the engine's writer gate, whose wait counter stays zero).

**A timeless-acct store.** A copy of a live store sampled every 10 s:
251,931 series, 472,513 raw chunks, 429 million points, created before
`index_cache` existed. The 33 `timeless_latest` reads behind one screen of
a moment (about 2,360 rows; each metric has about 16,000 series, 280 of
them live):

| | open and first read | frame, now | frame, 3 days back |
|---|---:|---:|---:|
| 0.8.17 | 1.81 s | 119 ms | 230 ms |
| 0.9.0 before labels were read only for series that answer | 0.07 s | 222 ms | 349 ms |
| 0.9.0 | 0.07 s | 56 ms | 154 ms |

The first writable open under 0.9.0 took 0.86 s and grew the file from 537
to 575 MiB.

**Write patterns.** 20,000 points over 20 series into a new table, WAL,
`synchronous = NORMAL`:

| | 0.8.17 | 0.9.0 | chunks before compaction, 0.9.0 |
|---|---:|---:|---:|
| a commit a point | 61,526 rows/s | 17,456 rows/s | 20,000 |
| a transaction a timestamp (20 points) | 375,785 rows/s | 128,293 rows/s | 20,000 |
| a transaction a minute (1,200 points) | 655,488 rows/s | 625,591 rows/s | 340 |

0.8.17 wrote 20 chunks in each case, having held the points in memory.
Compaction brings each 0.9.0 case to 20 chunks; the first two take 1,524
KiB until it does, against 372 KiB.

**Durability.** A writer was killed with `SIGKILL` five times, after 150 to
390 acknowledged 40-point statements each, with a compaction every seventh:
every acknowledged statement's points were present after reopening, none
was partly present, and `disk_points` equalled the rows.

**Compatibility.** A table written by 0.9.0 (`index_cache='unbounded'`,
rollups) was opened by the 0.8.17 extension, which read it with its index
on disk, kept both chunk indexes, wrote, compacted and rolled up; 0.9.0
then read the same rows and totals, and wrote to it.

## Pre-tag verification

Run on the release source after the bump, in one pass:

- `cargo check --workspace --locked` in the root, `servers/` and
  `tools/query-harness/`; `cargo fmt --check` for all eight workspaces
- root: build, 285 tests, clippy and rustdoc with warnings denied
- the extension under `--no-default-features --features embedded`: 104
  tests and clippy with warnings denied
- release builds of the extension, `dbhealth-ext` and the servers
- servers: 610 tests with `--include-ignored`, clippy, rustdoc
- `tests/cli.sh`; `tests/correctness.sh` for `r1 r2 r3 r4 r8 logs-rich`;
  `tests/dbhealth.sh`; `tests/crash.sh`
- query harness: 65 tests, clippy, contracts, oracle validation, SQL
  recipes (135 recipes, 173 statements) and docs

The same pass was run after each of the six changes that make up the
release, before the next was begun.

## Known limits

- A host that commits a metrics point, or a few, at a time writes a chunk a
  commit: about a quarter of the insert rate and four times the bytes until
  compaction. Group writes into transactions.
- Reads that touch very many series cost more than they did with the index
  in memory: a count over 98,000 series takes 103 ms against 67 ms.
- A read-only connection cannot open a table that an earlier version held
  in memory until a connection that can write has opened it once.
- The identity cache of an `unbounded` table grows with the number of
  series ingested, at about 48 bytes each.
- Two error messages in this tag carry a run of spaces in mid-sentence
  ("open it once ... with a connection that can write"). Corrected on
  `main`.
- Logs and traces are unchanged: their buffers, flush barrier and read
  permits are as in 0.8.17.

## Publication and downloaded binaries

[`v0.9.0` was published](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.9.0)
on 2026-10-12 at 01:42:49 UTC. `main` was pushed before the annotated tag,
which resolves to the release source above. The
[tag-triggered artifact workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/38192518664)
passed all three native builds, checksum verification, and publication.

All four permanent assets were downloaded after publication. Every archive
passed the outer checksum and its inner checksums (11 files each), and every
archive manifest reports version `0.9.0` at the release source commit. The
downloaded Linux x86-64 `timeless-metrics-api` binary was executed, and its
`--version` reported the same version and commit.

| Native target | Archive bytes | SHA-256 |
|---|---:|---|
| `x86_64-unknown-linux-gnu` | 16339898 | `e9b21482a165b05366fb490e300e80ff1fe64a84534f2fee7a6d382453b0991d` |
| `aarch64-unknown-linux-gnu` | 15460602 | `16e11bc74378e6fbce94ba6dbd8729894ebcc3bc382b717b6ca5200f87283d3c` |
| `aarch64-apple-darwin` | 14310826 | `6879a7ab99d8b581c9d0fd21a84175cce03d0642f6daa736afabfdeb4a1e1e54` |

The publication record was committed afterward; the immutable release tag
continues to identify the verified source commit above.
