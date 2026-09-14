# Competitive fixes — 2026-09-13

This records validation as the prioritized issues from the
[competitive review](2026-09-13_competitive_review.md) are completed.
Historical release measurements remain unchanged.

## #71: count aliases

[Issue #71](https://github.com/awksedgreep/timeless-libsql/issues/71) is implemented
by `84c1bf8c82d6bbb22c1eb734919c9cab99e87140`. A scalar count at the beginning
of a general pipeline now uses the same public native count primitive as
`stats count() as total`, then applies the remaining operations to the single
aggregate row. The requested alias and JSON numeric type are retained.
A preceding filter, limit, projection, or other cardinality/value-changing
operation retains the row path. Request-local diagnostic and nested-query
reports also retain their existing cursor-report path because the native count
TVF does not publish that report contract.

The native Linux candidate was packaged from clean commit `84c1bf8`, using
Rust 1.98.1 on the canonical checkout mounted into a native ARM64 build
container. It is an **unpublished candidate** with the existing workspace
version 0.8.4, not a newly tagged release. Bundle SHA-256:
`72529678d98ac37c2e467354193e8b186bb3528902a8fb65b80e82642a0413d1`.
The packager verified inner checksums, build identities, and install/remove.
The builder exited before measurements began.

Four captures repeat the same small/larger fixtures, pinned competitors,
container quotas, five warmups, fifty timed requests, and restart checks from
the [protocol](COMPETITIVE_BENCHMARK.md). The harness uses the same `84c1bf8`
source and now retains public storage-stat snapshots outside request timers.
All 6,800 timed responses matched independent expectations, and complete fixture
data survived both graceful restart checks on every engine in every capture.
These are warm serial HTTP measurements on the same shared development host.

Timeless count latency, **p50 / p95 milliseconds**:

| Capture | `as total` | `as n` |
|---|---:|---:|
| Small A | 0.255 / 0.293 | 0.261 / 0.321 |
| Small B | 0.258 / 0.309 | 0.246 / 0.291 |
| Larger A | 0.249 / 0.289 | 0.251 / 0.304 |
| Larger B | 0.244 / 0.326 | 0.260 / 0.341 |

The previous 31–33× small-fixture alias penalty is gone. Both aliases now take
about 0.25 ms on both fixture sizes. Small differences at this scale do not
establish a winner. More decisively, each alias in every capture performed
exactly **55 native counts, zero row queries, zero decoded entries, and zero
native-count payload bytes read**. Native count metadata accounted for all
8,192 or 16,384 fixture entries on each request. Filtered counts still use the
existing postfilter path and remain work for #74.

Validation: all **273 logs-package tests** passed with the real extension and
ignored integration tests enabled; the new regression compares exact work
counters and results across aliases in buffered, optimized, and reopened
states. It covers empty/filtered input, quoted names, subsequent transforms,
prior cardinality-changing operations, and response-byte limits. Strict logs
Clippy, all **64 harness tests**, strict harness Clippy, and query contracts
passed. No storage format or version was changed.

| Capture | SHA-256 |
|---|---|
| [Small A](evidence/2026-09-13_issue71_small_a.json) | `a2b4d33a2449c8614aa68e8196c5cd89b6c96c66d3c65b79b626ab41a065f6d7` |
| [Small B](evidence/2026-09-13_issue71_small_b.json) | `e10dea0389192071d003c518cced949b52484d9aaf77f82ba511dd2d1e2ba478` |
| [Larger A](evidence/2026-09-13_issue71_larger_a.json) | `0bf25339e7acea728372fc07ac89419bfa4371df72c925e902ef742393589832` |
| [Larger B](evidence/2026-09-13_issue71_larger_b.json) | `68db50a6b68d3a401c00a830ab866fc7911f72636a80c071e5a2934cc6a73de2` |

## #72: ordinary field sorting

[Issue #72](https://github.com/awksedgreep/timeless-libsql/issues/72) is implemented
in `ba143f1` and `624bdeceb7c51d11ad59222169953519c1a04b95`. Sorting accepts
explicit ordinary or aggregate fields, multiple keys, per-field direction,
and a global ascending/descending direction. For example:

```logsql
* | stats by (service) count() as n | sort by (n desc, service)
```

The shared bounded comparator preserves natural numeric ordering and retained
JSON types. Sort works on the current pipeline rows, so preceding projection
or limit and following pagination retain their meaning. Timestamp-only storage
queries keep their existing path. Timestamp sorting inside a pipeline now uses
value comparisons too: this fixes whole-second values being misplaced after
fractional values when query-time offsets produce different RFC3339 precisions.
An HTTP regression reproduced the failure before this correction and now passes.

All-field sorting and inline sort limit/offset/rank/partition clauses remain
unsupported. Use explicit fields and following `offset`/`limit` pipes. Name a
secondary key if equal primary keys must have the same order across engines.

All **1,510 pinned VictoriaLogs oracle cases passed**, including 12 new sorting
cases ([retained output](evidence/2026-09-13_issue72_oracle.txt)). All **275 logs
tests** passed with the real extension and ignored integrations enabled after
the timestamp correction. Strict logs Clippy, 64 harness tests, strict harness
Clippy, oracle manifest validation, and query contracts passed. The new tests
cover types, missing/empty input, directions, pagination, state/work/result
bounds, maintenance/reopen, and both competitive fixture sizes.

The final native Linux bundle is a clean, unpublished 0.8.4 candidate from
`624bdeceb7c51d11ad59222169953519c1a04b95`, SHA-256
`875561db2a649d20c46882b3308111b67c5cb54f69c2b06e539b285415c6cd34`.
It passed the same packager checks and native ARM64 build procedure as #71.
Four complete captures used `--require-field-sort`. All **6,800 timed responses**
and both full-data restart checks passed. Each untimed field-sort probe returned
HTTP 200 and the complete ordered `api`, `worker` result: counts 2,048/6,144 on
the small fixture and 4,096/12,288 on the larger fixture. The required gate was
also tested against the pre-sort bundle and [correctly rejected its HTTP 422](evidence/2026-09-13_issue72_gate_rejection.txt).
No new release was tagged.

| Capture | SHA-256 |
|---|---|
| [Small A](evidence/2026-09-13_issue72_small_a.json) | `bd673c8883c12ebdef50ee8799da968f70e8bcff7994c4e4676240ca671f583f` |
| [Small B](evidence/2026-09-13_issue72_small_b.json) | `8dc61939b0ab02cb5a28eedd4ddb3aef01bc653ccff6b4c2d7f9ef132214a4b6` |
| [Larger A](evidence/2026-09-13_issue72_larger_a.json) | `cd04617eab5ffc617e4e450572e52fe9bf134487abf87ab1d4a15108dee96444` |
| [Larger B](evidence/2026-09-13_issue72_larger_b.json) | `877813b6909dd29bf122a9b224a60dfc6fa3cfb54571dc8765c2b5af92c4a076` |

## #73: pagination after native count

[Issue #73](https://github.com/awksedgreep/timeless-libsql/issues/73) is implemented
in `f910c23be008d9eccdc5fd7de3672e785f51d9a7`. `limit`, `head`, and `offset`
after scalar count now apply to its aggregate result. The complete count is
computed first through native execution. Pagination before count retains the
input-row pipeline, so `limit 2 | stats count() as total` returns two while
`stats count() as total | limit 1` returns the complete count.
Bare `stats count()` keeps the established Timeless `total` column (upstream's
default is `count(*)`); explicit aliases and JSON numeric types are preserved.

All **277 logs-package tests** passed with the real extension and ignored
integrations enabled, including parser boundaries, native work counters,
empty/filtered input, zero/larger limits, aliases, bounds, optimize, and reopen.
All **1,525 pinned VictoriaLogs cases** passed, including 15 new pagination
cases ([retained output](evidence/2026-09-13_issue73_oracle.txt)). The final
harness's 64 tests and strict Clippy passed, as did strict logs Clippy, oracle
manifest validation, and query contracts.

The clean unpublished native Linux candidate uses source `f910c23`, bundle
SHA-256 `30961d6bc48ef8758ac3545f20dad7eee4c4631d0bf2f8cfaa3a145068fcb618`.
It passed the same packager and native ARM64 build checks as the earlier fixes.
Harness source `095ee4a` additionally records public work counters around each
untimed capability probe. Four captures require both `--require-field-sort`
and `--require-count-limit`; all **6,800 timed responses** and both complete-data
restart checks passed, along with both required capabilities.

In every capture, `stats count() as total | limit 1` returned HTTP 200 with
8,192 or 16,384, as appropriate. Each probe performed **one native count,
zero row queries, zero decoded entries, and zero native-count payload reads**;
metadata accounted for every fixture entry. The gate also
[rejected the pre-fix bundle](evidence/2026-09-13_issue73_gate_rejection.txt).
No release tag or storage-format change was made.

| Capture | SHA-256 |
|---|---|
| [Small A](evidence/2026-09-13_issue73_small_a.json) | `cf1443227e80a96d4bbe772623f342ee23632d6a4f2842529fad1bfcbfd14e5f` |
| [Small B](evidence/2026-09-13_issue73_small_b.json) | `b2964d1b9ddb91d3898b20a9e9579d4d79de53aa8d436606f007e5734d3d49d1` |
| [Larger A](evidence/2026-09-13_issue73_larger_a.json) | `0a04723d29d464085825a6f2e8a859bdc4c648943f93ce713d1d1c37b8558971` |
| [Larger B](evidence/2026-09-13_issue73_larger_b.json) | `541171d47fe4d4e9f85149a9ebb56f6a3115b684ac3427f46c02e58618c95cfd` |


## #74: bounded metadata reductions

[Issue #74](https://github.com/awksedgreep/timeless-libsql/issues/74) is implemented
in `423d13e200f3a322db4bebd07c8bf02068700468`. Eligible metadata-only grouped
counts now retain a bounded map of group keys and counters while consuming the
public cursor. Metadata-only filtered counts read only metadata across the
SQLite/API boundary, avoiding copies of timestamps, levels, and messages.
Exact JSON typing remains checked after index candidate selection. Ordinary
row transforms, built-in presentation fields, diagnostic reports, and other
predicates retain their existing execution paths. A newly exposed state-limit
classification gap is fixed: exhausted group-state bytes return HTTP 422 with
`max_response_bytes`, rather than an internal error.

The #73 baseline exposes the bottleneck. Per request, all three shapes decode
two mixed-service blocks containing 8,192/16,384 entries and read
666,700/1,333,324 payload bytes. Filtered counts return 2,048/4,096 cursor rows;
grouped counts return every entry to produce two totals. On capture A, filtered
`as total` spends 3.73/7.47 ms in storage materialization out of 4.73/9.44 ms API
query time. Grouped count spends 3.41/6.84 ms there out of 8.45/17.33 ms API time.
The other alias has the same physical work; every delta is retained in the raw
baseline and candidate captures.

Candidate public HTTP latency, **p50 / p95 milliseconds**:

| Capture | Shape | Timeless | VictoriaLogs |
|---|---|---:|---:|
| Small A | `filtered_count_as_total` | 4.941 / 5.576 | 0.461 / 0.755 |
| Small A | `filtered_count_as_n` | 5.048 / 5.675 | 0.467 / 0.843 |
| Small A | `grouped_count` | 6.202 / 6.435 | 0.622 / 1.098 |
| Small B | `filtered_count_as_total` | 4.915 / 5.927 | 0.453 / 0.926 |
| Small B | `filtered_count_as_n` | 5.115 / 6.376 | 0.450 / 0.812 |
| Small B | `grouped_count` | 6.127 / 6.570 | 0.737 / 1.159 |
| Larger A | `filtered_count_as_total` | 10.014 / 10.742 | 0.536 / 1.004 |
| Larger A | `filtered_count_as_n` | 9.804 / 11.490 | 0.634 / 0.940 |
| Larger A | `grouped_count` | 12.548 / 15.005 | 0.729 / 1.470 |
| Larger B | `filtered_count_as_total` | 9.272 / 10.471 | 0.567 / 1.164 |
| Larger B | `filtered_count_as_n` | 9.913 / 11.111 | 0.479 / 0.954 |
| Larger B | `grouped_count` | 13.263 / 14.484 | 0.866 / 1.526 |

Grouped-count p50 drops from the #73 small range 8.79–8.85 ms to 6.13–6.20 ms,
and the larger range 17.75–18.03 ms to 12.55–13.26 ms: approximately 26–31%
faster. Filtered-count differences are modest and overlap host variation;
they do not establish a substantial improvement or competitive parity.
Both aliases still receive the same typed filtering and work bounds.

Candidate mean public-counter time per request (capture A; counters cover
five warmups plus fifty timed requests):

| Fixture | Shape | API query ms | Storage materialization ms | Cursor rows |
|---|---|---:|---:|---:|
| Small | `filtered_count_as_total` | 4.590 | 3.754 | 2,048 |
| Small | `filtered_count_as_n` | 4.537 | 3.756 | 2,048 |
| Small | `grouped_count` | 5.793 | 3.407 | 8,192 |
| Larger | `filtered_count_as_total` | 9.173 | 7.501 | 4,096 |
| Larger | `filtered_count_as_n` | 9.437 | 7.702 | 4,096 |
| Larger | `grouped_count` | 12.805 | 7.745 | 16,384 |

Decoded entries, candidate blocks, and payload bytes are unchanged. The API
retains group counters rather than all input rows, but the extension still
materializes decoded log entries. Mixed-service blocks require metadata
inspection with the current indexes. The existing native count filter uses
string projections and cannot replace typed exact predicates. The bucket API
also lacks this bounded typed/nested grouping contract. Concrete follow-up
[#77](https://github.com/awksedgreep/timeless-libsql/issues/77) defines the public
reduction/projection work needed to remove that remaining materialization,
with compatibility, cancellation, memory, and diagnostic requirements. No new
index or storage format is justified by these measurements alone.

Timeless logs RSS after the complete workload is 31.9–32.0 MiB small and
50.7–52.4 MiB larger (baseline 33.2–33.5 and 52.1–53.7 MiB). These are whole-server
snapshots after all shapes, not isolated query peaks. Apparent data volume
bytes remain exactly 757,840/1,417,296; allocated sizes are retained in captures.
No storage format, index, version, or competitor fixture was changed.

All **279 logs-package tests** passed with the real extension and ignored
integrations enabled. New tests cover independently expected selective/absent
counts, typed strings versus numbers, missing/null/nested/array/object groups,
identity-projection fallback equivalence, built-in field fallbacks, pagination,
1,024 groups, cancellation, state/work/result/response ceilings, buffered and
persisted data, optimize/reopen, and both competitive fixture sizes. Strict
logs Clippy and query contracts passed. The pinned oracle fixtures are unchanged.

The clean unpublished native Linux 0.8.4 candidate and harness use `423d13e`;
bundle SHA-256 `2cc4d1e6f351bd5a4ba98c7ffc559ca86bbd1ab6f7f773c5cec40bdea1625449`.
The same native ARM64 packager checks, pinned images, quotas and benchmark
protocol apply. All **6,800 timed responses**, both complete-data restart checks,
and both required capability probes passed across four captures. No release
was tagged.

| Capture | SHA-256 |
|---|---|
| [Small A](evidence/2026-09-13_issue74_small_a.json) | `f75f453acaa60e8100d6897d75564df56fc755a5aca8e6c95edb58bbaf429281` |
| [Small B](evidence/2026-09-13_issue74_small_b.json) | `e2ada11c78352097f8a26b83eb6f9873191d8261a154653bec76881ebcb5fb4d` |
| [Larger A](evidence/2026-09-13_issue74_larger_a.json) | `6d1d1af58ea22c4d24a514a98296e3a683fe35b96288b5ebbd0f0ab25ced814a` |
| [Larger B](evidence/2026-09-13_issue74_larger_b.json) | `a77a9641eceb9f80136097d85680e49f3904f4123efd8f052e920eb5cfb00674` |
