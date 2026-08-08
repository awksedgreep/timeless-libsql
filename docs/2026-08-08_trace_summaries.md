# Trace-level summary decision

Date: 2026-08-08  
Session: 6 of the [trace query enhancement plan](2026-08-07_trace_query_enhancement_plan.md)  
Measured source: `b34bd9622dd5eae6551d2f36432df648e2ff518d`

## Result

Session 6 is complete with an explicit no-storage verdict. Timeless did not
add a persisted trace-summary table, private server cache, completeness
heuristic, batch revision, or Jaeger semantic change.

Instead, direct SQLite/libSQL users receive an exact retained-snapshot summary
through ordinary public SQL. The final recipe performs one virtual-table scan,
reports physical and distinct span counts, error rows, checked envelope
bounds, missing/unique/ambiguous root state, and service cardinality, and
states completeness honestly as `unknown`.

The design boundary is checked in before implementation in the
[trace query matrix](2026-08-08_trace_query_matrix.md). The copyable statement
is in the [SQL API reference](SQL_API_REFERENCE.md#retained-trace-summaries).

## Why persisted summaries were rejected

The existing Jaeger search is intentionally span-oriented: it applies
service, operation, time, and duration predicates to spans, orders and limits
those spans, and only then groups the retained rows by trace ID. A whole-trace
summary cannot accelerate that operation without changing which spans and
traces are returned. Exact trace-ID lookup already uses the packed
`(trace_id, block_id)` index.

OTLP also has no trace-finalization marker, expected span count, or retry
identity. Under the retained append-only model:

- a child can arrive in any later request or block;
- the same `(trace_id, span_id)` can be retained more than once;
- root presence does not prove that all children arrived;
- a distributed trace has a service set, not one generally correct service;
- retention can remove roots while leaving later children, which is
  indistinguishable from source truncation without unbounded history.

An exact persisted accelerator would therefore need per-block trace
contributions so optimize and retention could add and subtract extrema, roots,
counts, and service sets transactionally. It would duplicate the existing
trace/block postings while still being unable to make completeness truthful.
There is no current query whose unchanged semantics justify that write, WAL,
index, migration, and maintenance cost.

## Retained behavior pinned through the real extension

The Rust `rich-traces` gate now creates one trace across separate public v2
batches and flushes, then verifies all of these states:

| event | retained result |
|---|---|
| first root | one row, one distinct span ID, unique root, completeness `unknown` |
| retry with the same IDs | two rows, one distinct span ID, ambiguous root; no silent deduplication |
| late child in another block | three rows, two distinct IDs, updated envelope/error count, completeness still `unknown` |
| transaction rollback | the attempted additional child is absent from rows and summary |
| optimize | aggregate values remain exact after reblocking |
| retention | both old root rows disappear while the later child remains; root state becomes `missing` |
| reopen | the retained partial snapshot is byte/value exact |

The SQL guards negative direct-SQL durations and signed-64-bit end/difference
overflow instead of coercing them. OTLP ingest already rejects end-before-start
and signed-storage overflow before admission.

## SQL scan refinement

The first readable recipe referenced one `retained` CTE from separate totals
and roots aggregates. SQLite executed the trace virtual table once for each
consumer. Conditional aggregate filters produce the same result in one scan.
Both shapes remain in the Rust release harness so the work reduction is
measured rather than inferred.

Two clean runs are retained:

- [primary JSON](evidence/2026-08-08_trace_summaries.json), SHA-256
  `0d494a4ebef8e18c73f755ff2a020ed2549c97d16cad0ce31904e651d1dc9a27`
- [repeat JSON](evidence/2026-08-08_trace_summaries_repeat.json), SHA-256
  `8041116667eca019f1c7d3ef90f7ae3a37b4f3299fb9767c6e3ef89c83254447`

The fixture is the established 131,072-span, sixteen authoritative
8,192-span-batch, 48-optimized-block rich-span v2 workload. Every candidate
ran after three warmups for twenty measured iterations in a fresh direct-SQL
process.

### Latency

Times are p50/p95/p99. Each range is primary–repeat.

| shape | two-scan control | published one-scan SQL | change |
|---|---:|---:|---:|
| exact 8-span trace | 0.540–0.602 / 0.575–0.625 / 0.586–0.637 ms | 0.337–0.368 / 0.359–0.385 / 0.420–0.495 ms | p50 37.5–38.9% faster; p95 37.6–38.4% faster |
| all 16,384 retained traces | 149.972–168.285 / 169.461–173.101 / 170.514–175.981 ms | 123.394–139.121 / 140.620–142.297 / 142.587–148.517 ms | p50 7.2–26.7% faster; p95 17.0–17.8% faster |

Results are byte-identical: one exact row is 366 JSON bytes; all 16,384
summaries are 5,980,161 bytes and account for all 131,072 retained spans.

### Public extension work per query

| work | exact control | exact one scan | broad control | broad one scan |
|---|---:|---:|---:|---:|
| virtual-table scans | 2 | 1 | 2 | 1 |
| candidate/payload blocks | 6 | 3 | 96 | 48 |
| decoded spans | 16,384 | 8,192 | 262,144 | 131,072 |
| payload bytes read | 95,260 | 47,630 | 1,531,074 | 765,537 |
| decoded physical columns | 33 | 24 | 528 | 384 |
| materialized values | 16,456 | 8,248 | 1,441,792 | 1,048,576 |

The direct-SQL process HWM is 37,016–38,672 KiB while running both control
and candidate shapes. It is a whole-process maximum, not memory attributed to
one query. No rich-span v2 field is decoded for the summary.

## Write and storage verdict

There is no summary write path, so its amplification is exactly zero. The two
release runs preserve the Session 5 fixture layout exactly:

- 48 compressed blocks and zero raw blocks;
- 131,072 durable spans and zero buffered spans;
- 765,537 logical payload bytes;
- 2,523,136 index bytes and 49,152 trace-index rows;
- 88,440,832 checkpointed database bytes and zero WAL/SHM bytes.

The runs admitted 168,971–173,974 spans/s and optimized in 489.8–513.1 ms.
Those values are retained as whole-run host variation; no extension, server,
storage, batch, codec, index, compression, transaction, retention, or
maintenance implementation changed in Session 6.

## Validation

- Rust query-harness unit and documentation-contract suites
- Rust `rich-traces` gate against the real extension
- complete extension core and virtual-table suites
- complete CLI crash, transaction, rollback, optimize, corruption, and reopen
  gates
- complete traces server OTLP, Jaeger, storage, cancellation, shutdown, and
  backup suites
- root, server, and query-harness Clippy with warnings denied
- two clean release-build A/B measurements with exact result/work assertions

The full CLI run also found and pinned two harness issues without changing
product behavior: the trace-plan proof now tests the stable trace-index bit
rather than an obsolete whole `idxNum`, and the SIGKILL gate now keeps exact
child ownership in Rust instead of delaying a shell kill against a numeric
PID.

## Verdict and future prerequisite

Ship the public one-scan SQL recipe and regressions. Reject persisted trace
summaries for the current data model and current Jaeger route.

Reconsider storage only for a separately versioned complete-trace API that
defines root/envelope/any-span filters, trace-level order and limit, duplicate
policy, completeness, and retention truncation. It must first demonstrate
representative demand beyond the current 0.36–0.39 ms exact p95 and
140.6–142.3 ms broad p95, then measure per-block contribution costs across
ingest, WAL, optimize, retention, migration, crash, and reopen.
