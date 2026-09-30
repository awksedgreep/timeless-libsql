# Log scan and series-discovery fixes — 2026-09-30

Source `69fcf2f488e783bf0a622b57b04a6d1994719d47` addresses the rowset
materialization problem in [#77](https://github.com/awksedgreep/timeless-libsql/issues/77),
adds exact time-window series discovery for
[#88](https://github.com/awksedgreep/timeless-libsql/issues/88), and restores
bounded log paging with strict timestamp bounds for
[#89](https://github.com/awksedgreep/timeless-libsql/issues/89).

## Behavior and compatibility

Unordered log scans retain one decoded block plus their owned buffered snapshot.
Scans without a message predicate decode only required payload columns;
message-filtered scans retain their existing decoder/pruning path.
Metadata-only rich-log scans
omit the auxiliary string-pair projection unless a hidden-column predicate
needs it. SQLite metadata output borrows the entry's existing JSON instead of
retaining a second copy. The existing API metadata reductions therefore fold
rows as they arrive, preserving typed filters and textual group identities.
Ordered scans retain canonical equal-timestamp ordering; strict integer bounds
normalize to inclusive bounds before the bounded scan. Overflow and empty
ranges match no rows. Non-integer bounds keep SQLite's recheck semantics.

This is an additive execution capability, with no index or storage-format
change. The [guide](GUIDE.md) documents `streaming_projection_v1`, request-local
streaming/retained-row counters, and cumulative counters. Older extensions
continue to serve the existing SQL and API fallback; they do not advertise the
new execution capability. Work, group cardinality/bytes, response limits and
cancellation still apply. Diagnostic and nested-query fallback semantics are
unchanged. Writers are released after the stream owns its read snapshot.

Both metrics series routes accept inclusive Unix-seconds/RFC 3339 `start` and
`end`. Catalog extrema prove recent sample presence without chunk decoding.
Windows inside a series' extent use bounded public `timeless_latest` probes
to exclude historical gaps. Their shared storage budget conservatively reserves
the ambiguous series' catalog point counts. Neither route reads shadow tables.
See the [API reference](SERVER_API_REFERENCE.md) for bounds, errors and budgets.

## Direct SQL: large metadata fixture

Five fresh SQLite CLI processes per shape/build read the same 274,118-row,
37,068,800-byte database with approximately 1.1 KB metadata per row. The
filesystem cache was warm. Median process wall time and maximum peak RSS across
the five runs are shown below. These are synthetic measurements on an Intel
Core Ultra 9 185H, 22 logical CPUs, Linux 7.2.5; unrelated workstation processes
remained running. They are not measurements of the reporter's database.

| Query | Previous local extension, median ms | Candidate, median ms | Previous peak MiB | Candidate peak MiB |
|---|---:|---:|---:|---:|
| `count(*)` over the whole range | 986.12 | 29.97 | 1327.90 | 27.05 |
| Strict-bound newest 200 rows | 1003.96 | 16.30 | 1328.12 | 29.52 |
| Read every metadata row | 1113.83 | 583.80 | 1328.19 | 43.87 |

The [raw samples](evidence/2026-09-30_issue77_sql.json) record both binary
SHA-256 hashes. The previous local binary reports extension version 0.8.6 and
embedded build commit `eb2ed465584cb8ce5d5b6111b28e46f89e5f2bc1`; its exact
binary hash, rather than an inferred release archive identity, is the baseline.
The candidate is the clean packaged build identified below. Queries checked the
exact count and ordered paging timestamps; metadata output was sent to
`/dev/null`. Each sample used a fresh Python parent to time its `sqlite3` child
and read `resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss`, excluding
Python startup from the timed interval.

Fixture SQL, run once after loading the candidate into a fresh database:

```sql
CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service');
WITH RECURSIVE n(x) AS (
  VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<274117
)
INSERT INTO logs(ts,level,message,metadata)
SELECT x,'info','process exited',
       json_object('command','process '||x,'padding',printf('%01000d',x),
         'service',CASE WHEN x%2=0 THEN 'api' ELSE 'db' END,'status',x%4)
FROM n;
INSERT INTO logs(logs) VALUES('flush');
```

Measured queries, each in its own newly opened process:

```sql
SELECT count(*) FROM logs WHERE ts BETWEEN 0 AND 274117;
SELECT ts FROM logs WHERE ts>=0 AND ts<274118 ORDER BY ts DESC LIMIT 200;
SELECT metadata FROM logs WHERE ts BETWEEN 0 AND 274117;
```

The candidate count report processes all 34 blocks and 274,118 timestamps,
with 274,118 non-timestamp values read and at most 8,192 decoded rows retained.
It avoids message/metadata decoding; it does not claim zero payload reads or
the metadata-only shortcut of `timeless_log_count`.

## Repeated pinned HTTP comparison

The existing [competitive protocol](COMPETITIVE_BENCHMARK.md) ran both sizes
twice: 512 × 32 metric samples with 8,192 logs, and 2,048 × 32 metric samples
with 16,384 logs. Every warmup/measured response matched independent fixture
expectations, and every engine passed both complete-data restart checks.
Each shape has five warmups and fifty measured requests with rotating engine
order. Competitors remain Prometheus 3.13.2, VictoriaMetrics 1.148.0 and
VictoriaLogs 1.52.0, with the native immutable digests in the oracle manifest.
All four captures also pass field-sort and count-limit composition probes.

The clean, unpublished candidate archive SHA-256 is
`40d12e0b736f9c66abf92cab1e1b3e9cf64e04d23f8e02863a601b0172680407`.
Its source is `69fcf2f`; harness source is
`5e4eebbb8740799aa732309439ad747703bf8ac3`.
The native x86-64 runtime image is
`docker.io/library/archlinux@sha256:b21322c663be387c0ed9cbc7bbbfe18e41633ad4e7b7c77cfad45f128be20040`.
This host's binaries require glibc 2.44, so the older Debian example image
cannot run this local candidate. No emulation or competitor pin changes were
used. A harness pipe-backpressure deadlock exposed by Podman inspection was
fixed and regression-tested before these captures.

Latency, **p50 / p95 milliseconds**:

| Capture | Filtered count: Timeless | Filtered count: VictoriaLogs | Grouped count: Timeless | Grouped count: VictoriaLogs |
|---|---:|---:|---:|---:|
| [Small A](evidence/2026-09-30_issue77_small-a.json) | 6.668 / 8.040 | 0.924 / 1.525 | 6.489 / 7.210 | 1.102 / 1.678 |
| [Small B](evidence/2026-09-30_issue77_small-b.json) | 6.905 / 7.440 | 0.990 / 1.674 | 6.295 / 7.298 | 1.020 / 1.557 |
| [Larger A](evidence/2026-09-30_issue77_large-a.json) | 14.152 / 15.932 | 1.191 / 1.989 | 11.989 / 15.011 | 1.381 / 2.660 |
| [Larger B](evidence/2026-09-30_issue77_large-b.json) | 13.658 / 15.457 | 1.209 / 1.835 | 11.745 / 13.170 | 1.398 / 2.568 |

VictoriaLogs remains faster for these HTTP reductions. These x86-64 results
cannot establish a latency improvement against the earlier ARM64 captures.
The controlled before/after comparison above establishes the memory reduction.

Each filtered/grouped request still decodes all 8,192/16,384 metadata entries
in the two mixed-service blocks. It reads 16,384/32,768 logical value slots
(two per row), uses exactly one stream, and retains at most 7,168/14,336 rows
at once, the largest physical block. Filtered counts pass 2,048/4,096 matching
rows to the API; grouping passes all rows into incremental counters. Necessary
metadata scanning and API JSON parsing remain; there is no per-value index.

Whole-server resource snapshots after the complete workload:

| Capture | Timeless RSS / HWM, KiB | VictoriaLogs RSS / HWM, KiB | Timeless apparent bytes | VictoriaLogs apparent bytes |
|---|---:|---:|---:|---:|
| Small A | 36684 / 36684 | 33460 / 44172 | 753744 | 22444 |
| Small B | 37416 / 37416 | 39652 / 42760 | 753744 | 22444 |
| Larger A | 57612 / 57612 | 56416 / 58068 | 1413200 | 41631 |
| Larger B | 58740 / 58740 | 40800 / 55436 | 1413200 | 41631 |

These memory figures include the ordered full-row workloads, not just metadata
reductions. Disk figures include the complete volume after graceful restart.
The change trades full-rowset retention for block-wise decode and SQLite cursor
iteration; it adds no durable bytes or index. Ordered scans can still retain
many rows, and very wide physical blocks still require substantial decode work.
Metadata substring search suggested in the issue comments remains a separate
extension to the query surface; it is not introduced here.

## Validation

- Root workspace: 286 passed, with the existing production-scale rollup memory
  fixture ignored. All 594 server-workspace tests passed with ignored real
  extension integrations enabled. After the final cancellation/order changes,
  the 135 core unit tests and all 100 logs HTTP tests passed again.
- All CLI sections passed, including crash recovery, strict-bound SQL oracles,
  streaming projection, maintenance/publication, and reopen checks. Rich-log
  compatibility and R4 shared-engine correctness suites passed.
- Projection tests cover every supported log codec and all 16 payload masks.
  Streaming tests cover owned snapshots across prune, cancellation/reuse,
  empty/mixed/typed metadata, group/work limits, early close and single-use
  reports. Series tests cover recent/historical/fractional/RFC 3339 windows,
  gaps, repeated selectors, budgets, buffered data and reopen.
- Strict Clippy passed for both workspaces and the harness. All 65 harness
  tests, including the new subprocess-output regression, passed. The native
  packager verified identities/checksums and its isolated install/remove drill.

No release tag or publication was performed.
