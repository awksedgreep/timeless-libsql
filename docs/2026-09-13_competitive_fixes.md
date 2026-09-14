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
