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
