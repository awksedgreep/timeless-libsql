# TraceQL prerequisite matrix

Date: 2026-08-08  
Session: 7 of the [trace query enhancement plan](2026-08-07_trace_query_enhancement_plan.md)  
Status: implementation complete; final measured verdict pending. This is a
storage-prerequisite matrix, not a claim of TraceQL syntax or compatibility.

## Boundary

The SQLite/libSQL extension may own generally useful, storage-aware candidate
pruning and exact row predicates. A later Rust traces API must own TraceQL
parsing, ASTs, scopes, structural operators, span-set composition, trace
quantifiers, result envelopes, limits, and cancellation. TraceQL text must
never enter the extension.

The current retained model stores spans, not finalized traces. No row in this
matrix may infer trace completeness, silently deduplicate repeated exports, or
turn a span match into a complete-trace result without an explicit higher-order
plan.

## Prerequisites

| ID | future vector | extension prerequisite | higher-order Rust work | Session 7 disposition |
|---|---|---|---|---|
| `TQP-01` | Exact typed span-attribute scalar equality | Bounded, opt-in exact-path candidate pruning plus exact per-span recheck | Parse TraceQL attribute syntax and compose the returned span set | implemented; measured verdict pending |
| `TQP-02` | Exact typed resource-attribute scalar equality | Same primitive with an explicit resource scope | Resolve TraceQL resource namespace and compose span/trace results | implemented; measured verdict pending |
| `TQP-03` | Exact typed instrumentation-scope scalar equality | Same primitive with an explicit scope scope | Resolve scope namespace and compose span/trace results | implemented; measured verdict pending |
| `TQP-04` | Missing versus JSON null versus empty string | Equality keeps all three states distinct; ordinary public JSON1 remains the existence/type surface | Define language operators and null/missing propagation | implemented; measured verdict pending |
| `TQP-05` | Boolean `and`, `or`, and `not` over attributes | Individual positive equality candidates only | Plan intersections/unions/complements and preserve exact row semantics | library |
| `TQP-06` | Any/root/all-span trace quantifiers | Exact span matches and existing packed trace IDs | Group by trace, identify roots, define empty/all behavior, fetch retained trace rows | library |
| `TQP-07` | Numeric/string range or regex predicates | No equality filter can honestly accelerate these | Define typed comparison/coercion and measure before proposing separate primitives | deferred |
| `TQP-08` | Event-attribute predicates | Events are ordered repeated arrays; no scalar path index is sufficient | Define event existential/quantified semantics and event identity | deferred |
| `TQP-09` | Link-attribute predicates | Links are ordered repeated arrays; no scalar path index is sufficient | Define link existential/quantified semantics and link identity | deferred |
| `TQP-10` | Structural parent/child/ancestor/descendant operators | Existing packed trace/span/parent IDs are lossless but only trace IDs are indexed | Build bounded per-trace graphs and define missing-parent behavior | library |
| `TQP-11` | Trace duration/count/error/root predicates | Public retained-summary SQL exists, but completeness is always unknown | Select a retained-trace population and define duplicate/retention policy | deferred by `TSQ-06`/`TSQ-07` |
| `TQP-12` | TraceQL parser, pipelines, aggregators, and response types | None; language syntax does not belong in SQLite | Pinned TraceQL oracle, parser, planner, evaluator, envelopes, limits, cancellation | library |

## Candidate gate

Session 7 implements only one primitive shared by `TQP-01` through
`TQP-04`: exact typed scalar equality on an allowlisted JSON Pointer in one of
the span, resource, or instrumentation-scope objects. The candidate is a
fixed-size per-block probabilistic negative filter. False positives are
permitted because every surviving row is rechecked exactly; false negatives
are not.

Ordinary posting lists are not the candidate. Low-cardinality attribute values
usually occur in every 8,192-span block and prune nothing, while
identifier-valued attributes can add nearly one posting row per retained span.
The fixed-size design is being measured because it bounds this cardinality
independently of user values.

The implementation ships only if the final evidence accounts for ingest CPU,
logical/physical storage, WAL/checkpoint behavior, optimize and retention,
narrow/wide query tails and work, and RSS HWM. Otherwise the implementation is
reverted and this matrix will record an explicit rejected disposition.
