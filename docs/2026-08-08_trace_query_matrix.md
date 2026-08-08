# Trace query matrix

Date: 2026-08-08  
Session: 7 of the [trace query enhancement plan](2026-08-07_trace_query_enhancement_plan.md)
Status: Session 6 complete; Session 7 attribute prerequisite implementation
is complete and awaiting the final measured verdict. Persisted summaries were rejected; the measured
public SQL verdict is in
[`2026-08-08_trace_summaries.md`](2026-08-08_trace_summaries.md). No persisted
summary storage may be added until the `TSQ-06` prerequisites are satisfied.

## Purpose

This matrix separates queries over stored spans from queries over logical,
complete traces. The distinction matters because OTLP exports spans, not a
trace-finalization record. Spans for one trace may arrive in different
requests and blocks, after arbitrary delay, more than once, or after retention
has already removed an earlier part of the trace.

The current store is append-only. `(trace_id, span_id)` is not a uniqueness
constraint and the ingest API has no idempotency key. A repeated export is
therefore indistinguishable from two deliberately retained rows with the same
IDs. Any summary must describe that physical retained-row model unless a later
version first changes the write contract.

## Dispositions

| disposition | meaning |
|---|---|
| `shipped` | The current public extension or Rust API has an exact contract. |
| `sql` | Ordinary SQL over the public `timeless_traces` table is the honest surface; a new extension primitive is not justified. |
| `library` | The operation composes public rows in a higher-order Rust trace library or API. |
| `deferred` | A required storage identity or language/product contract does not exist yet. |
| `rejected` | The proposed persisted summary cannot serve the current query without changing its semantics. |

## Query vectors

| ID | vector | exact semantics | owner | summary value | disposition |
|---|---|---|---|---|---|
| `TSQ-01` | Exact trace-ID lookup | Return every retained row with the requested packed 16-byte trace ID, ordered by `(start_ts, span_id)`. Duplicate IDs remain duplicate rows. | extension + Rust API | None. The existing `(trace_id, block_id)` index is already the exact minimal candidate path. | shipped |
| `TSQ-02` | Existing Jaeger search | Apply service, operation, start, end, and duration predicates to individual spans; order matching spans newest first; limit spans; then group those rows by trace ID. Results may intentionally contain incomplete traces. | Rust traces API | A whole-trace summary cannot preserve span filtering, span limiting, or returned-row cardinality. | rejected |
| `TSQ-03` | Native dashboard span search | Filter, order, offset, and limit individual retained spans. | Rust traces API | None; it is not a trace-level operation. | shipped |
| `TSQ-04` | One retained-trace summary by ID | Aggregate the exact retained rows selected by `trace_id`: row count, error-row count, minimum start, maximum valid end, envelope duration, roots, and services. | SQL | The trace index already makes the source scan narrow; ordinary aggregates preserve the stored-row semantics. | sql |
| `TSQ-05` | Broad retained-trace summaries | Group the selected retained span snapshot by packed trace ID. Counts include repeated rows; roots and services are sets, not assumed scalars. | SQL | A persisted accelerator is possible only as per-block contributions, but no current API consumes it and completeness remains unknowable. | sql |
| `TSQ-06` | Complete-trace search | Select logical traces by root service/operation, trace start/end/duration, total spans, or any-error state; limit traces; then return all retained spans for each selected trace. | future versioned Rust API | This is the only vector that could justify persisted trace contributions, but it is not the current Jaeger contract. | deferred |
| `TSQ-07` | Trace completeness | Distinguish complete, still arriving, source-truncated, retry-duplicated, and retention-truncated traces. | future ingest/storage contract | OTLP supplies no trace-finalization marker, expected span count, or retry identity. Root presence does not prove completeness. | deferred |
| `TSQ-08` | Trace attribute search | Select retained spans by exact typed scalar equality on configured resource, scope, or span paths; a later API may compose those span sets into explicitly defined trace quantifiers. | extension prerequisite + future Rust query planner | The extension implements opt-in fixed-size block-negative filters plus exact row recheck for at most eight configured paths. Event/link predicates, non-equality operators, structural composition, and any/root/all trace quantifiers remain higher-order prerequisites; a scalar summary would lose type, scope, and quantifier semantics. | implemented; measured verdict pending |
| `TSQ-09` | Dependency/service graph | Derive parent/link edges across services from a retained snapshot. | Rust trace library | Requires graph composition and explicit missing-parent/link policy, not one scalar trace row. | library |
| `TSQ-10` | Trace statistics over a time window | Count retained traces or compute distributions from a precisely defined selected-span or selected-trace population. | SQL or future Rust API | Depends on whether the time predicate applies to roots, envelopes, or any span; those are different populations. | deferred |

## Honest retained-summary fields

These are the only meanings a public `TSQ-04`/`TSQ-05` SQL recipe may claim.
They describe the current retained snapshot, not source completeness.

| field | definition |
|---|---|
| `trace_id` | Exact packed 16-byte value used as the group key. |
| `span_rows` | `count(*)`; repeated `(trace_id, span_id)` rows count repeatedly. |
| `distinct_span_ids` | `count(DISTINCT span_id)` when the caller wants an additional diagnostic; it must not silently replace `span_rows`. |
| `error_rows` | Count of retained rows whose status is exactly `error`. |
| `start_ts` | Minimum retained span start timestamp. |
| `end_ts` | Maximum checked `start_ts + duration_ns` among rows with non-negative duration and no signed-64-bit overflow. Invalid direct-SQL durations are reported separately, not coerced. |
| `duration_ns` | `end_ts - start_ts` only when both values are valid and subtraction does not overflow. This is the retained envelope, not necessarily the root-span duration. |
| `root_count` | Count of retained rows whose `parent_span_id IS NULL`; repeated root rows count repeatedly. |
| `root_span_id`, `root_name`, `root_service` | Defined only when exactly one retained root row exists. Otherwise `NULL` plus an explicit `missing` or `ambiguous` root state. |
| `services` | Deterministic distinct set of retained service names. A distributed trace does not have one generally correct scalar service. |
| `completeness` | Always `unknown` under the current ingest contract. It must never be inferred from root presence or inactivity. |

## Edge-case contract

| case | required behavior |
|---|---|
| Cross-batch trace | Exact lookup and SQL aggregation include rows from every current buffer/block contribution. Batch boundaries are invisible. |
| Late child | A later committed child changes retained counts and envelope bounds. It does not transition completeness away from `unknown`. |
| Duplicate/retry | The append-only store preserves the additional row. Counts include it and root state may become ambiguous. No layer may silently deduplicate without a new write contract. |
| Partial source trace | Root or children may be absent. The retained snapshot is queryable, but completeness remains `unknown`. |
| Retention | Results describe rows remaining after whole-block retention. Without durable per-trace removal history, the query cannot distinguish retention truncation from source truncation. |
| Transaction/rollback | Rows and any future contribution metadata must publish and roll back together, including automatic 8,192-span flushes and savepoints. |
| Optimize | Reblocking must not change any retained-row aggregate. A persisted design would need to replace per-block contributions in the same host transaction as payload and index rows. |
| Crash/reopen | Only committed rows are visible. No summary may be reconstructed from a server-private cache. |

## Session 6 storage decision

Do not add a persisted trace-summary table in Session 6.

The only current broad consumer, `TSQ-02`, intentionally filters and limits
spans before grouping. A summary keyed once per trace cannot accelerate that
path without changing which rows and traces are returned. `TSQ-01` is already
served by the packed trace/block index, and `TSQ-04` needs only the few blocks
for one trace. `TSQ-05` has an ordinary public SQL expression but no current
product/API consumer whose measured workload would repay a second maintained
aggregate index.

An exact persisted implementation would also require per-block trace
contributions rather than one mutable row: optimize and retention replace or
delete whole blocks, and extrema/root fields cannot be subtracted from a
single accumulated row. That design would duplicate every `(trace_id,
block_id)` posting with counts, extrema, root candidates, and service sets,
while still being unable to make `completeness` truthful. No private server
table or heuristic timeout is an acceptable substitute.

Reconsider `TSQ-06` only after all of these prerequisites exist:

1. a versioned complete-trace search route with root/envelope/any-span filter
   semantics and trace-level limit/order defined independently of `TSQ-02`;
2. an explicit duplicate/retry policy or ingest idempotency identity;
3. an honest completeness contract, whether permanently `unknown` or backed
   by a new producer/finalization protocol;
4. explicit retention-truncation behavior;
5. representative query evidence showing that public SQL/current trace-index
   composition is materially too slow;
6. measured write, WAL, index, optimize, retention, migration, and storage
   costs for per-block contributions.

Until then, persisted summaries would be write amplification in search of a
query and would falsely suggest that Timeless knows when a trace is complete.
