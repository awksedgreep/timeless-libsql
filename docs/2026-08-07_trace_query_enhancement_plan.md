# Trace query enhancement plan

Date: 2026-08-07  
Branch: `feature/trace-duration-pruning`  
Baseline parent: `df9c9af` (`Add trace duration block pruning`)

## Objective

Improve trace-query speed and fidelity without weakening the retained storage
model or moving language semantics into the SQLite extension. Each session is
independently measurable, revertible, documented, committed, and pushable.
Correctness and bounded resource use remain release gates; a proposed
optimization is optional when its measured read benefit does not justify its
write, storage, or maintenance cost.

## Invariants

- Use only the public `timeless_traces` virtual table, batch input, maintenance,
  stats, and query contracts. Never read or write private shadow tables from a
  server or benchmark.
- Keep the extension's 8,192-span automatic batch boundary authoritative.
- Preserve exact IDs, parentage, timestamps, durations, status, attributes,
  events, resource data, instrumentation scope data, transactions, reopen
  behavior, and existing codecs.
- Keep Jaeger/dashboard/OTLP semantics in the Rust traces API. Extension
  primitives must remain generally useful to direct SQLite/libSQL users.
- Use Rust for benchmark and regression tooling. Do not add Python or shell
  performance drivers.
- Measure release builds from a clean, identified commit. Capture p50/p95/p99,
  result cardinality, response bytes, candidate/payload blocks, decoded spans,
  payload bytes, logical/physical storage, completed durable work, and RSS HWM.
- Add a regression for every defect found. Document and revert failed
  optimizations, then continue with independent sessions.
- Do not tag, publish, open a pull request, or merge to `main` as part of this
  plan.

## Session 1 — Reproducible broad-read baseline

Build a deterministic rich-span database in exactly 8,192-span public batch
inserts, optimize it through the public maintenance command, checkpoint, and
reopen it. Measure two deliberately different read costs:

1. broad Jaeger search over loopback HTTP, including a full-decode miss whose
   duration bounds overlap every block but no span matches; and
2. direct SQLite/libSQL execution of `timeless_trace_buckets` over the complete
   fixture.

Include narrow controls so future sessions can distinguish fixed HTTP and SQL
costs from decoded-work costs. Keep fixture generation outside measured query
intervals and run the direct-SQL phase in a fresh process so its HWM is not
polluted by fixture construction.

Exit criteria:

- the harness is Rust-only, deterministic, self-cleaning by default, and can
  retain a failed fixture only when explicitly requested;
- the extension and traces binary identities match a clean source commit;
- the fixture contains at least 16 authoritative batches and non-empty rich
  fields, survives optimize/checkpoint/reopen, and passes exact count/range
  checks;
- every measured query returns a stable expected cardinality and the evidence
  reports latency tails, extension work, bytes, storage, durability, and HWM;
- a versioned JSON evidence artifact and concise interpretation are checked in;
- focused tests, formatting, Clippy, and the existing rich-trace/duration gates
  pass.

No query implementation or optimization is allowed in this session.

Status: complete. Refined per-shape release-build evidence from source commit
`ba3df20eef88ba63b57d89e90d3713e723dc6adf` is recorded in
[`2026-08-07_trace_query_baseline.md`](2026-08-07_trace_query_baseline.md)
and the versioned
[`JSON artifact`](evidence/2026-08-07_trace_query_baseline.json).

## Session 2 — Projection-aware, late-materializing trace decoding

Teach the trace cursor to decode only the physical columns requested by SQLite.
Add predicate-first late materialization so rich columns are decoded only for
rows that survive storage predicates and bounds, with a minimal scalar path for
`timeless_trace_buckets`. Keep predicate, ordering, and rich-span semantics
identical. Do not split or revise an on-disk codec unless the baseline proves
projection/late materialization cannot be applied safely to the existing
columnar representation.

Exit criteria:

- direct SQL projection regressions cover every individual public column,
  mixed projections, predicates on unselected columns, `count(*)`, and rich
  full-row reads before/after optimize and reopen;
- bucket output is bit/exact-value identical to Session 1;
- broad Jaeger and bucket evidence shows the change in decoded fields/bytes,
  p50/p95/p99, and HWM with no storage or write regression;
- report the 4,096-span Jaeger response HWM independently: projection work
  must not claim its response-construction/serialization memory as a storage
  improvement, and any response-memory experiment remains a separate change;
- any new stats are public, bounded, documented, and transaction/reopen safe.

Status: complete. Predicate-first generation-2 projection, conservative legacy
fallback, direct-SQL/Rust API regressions, public work counters, and exact
release comparison are recorded in
[`2026-08-07_trace_query_projection.md`](2026-08-07_trace_query_projection.md)
and its versioned
[`JSON artifact`](evidence/2026-08-07_trace_query_projection.json).

## Session 3 — Exact percentile selection

Replace full duration sorting inside `timeless_trace_buckets` with exact
nearest-rank selection only if the Session 1/2 profiles show percentile work is
material. Preserve the documented exact p50/p95/p99 definition, including
duplicates and boundary cardinalities.

Exit criteria:

- randomized and adversarial direct-SQL tests match the existing sorted oracle
  bit-for-bit for empty, singleton, duplicate, ordered, reverse, and large
  buckets;
- memory remains bounded and cancellation is checked during selection;
- release evidence demonstrates a material bucket improvement or the
  implementation is reverted and the negative result is documented.

## Session 4 — Time-aware posting-list experiment

Measure whether service/name/kind/status postings should carry time bounds or a
time ordering. Prototype only after profiling shows repeated decompression of
time-disjoint candidates. Account for ingest CPU, index bytes, WAL growth,
optimize cost, and retention behavior—not only read latency.

Exit criteria:

- candidate-block reduction and latency improvement are material on both
  selective and broad time windows;
- the added write/storage/maintenance cost has an explicit acceptable verdict;
- crash, transaction, rollback, retention, old-format reopen, and corruption
  tests pass;
- otherwise revert the prototype and retain the evidence.

## Session 5 — Rich-span v2 fidelity

Design an additive public batch/codec revision for currently unretained OTLP
fields: links, trace state, trace flags, resource and scope schema URLs, and
dropped attribute/event/link counts. This is a fidelity change, not a private
server blob.

Exit criteria:

- the public schema and batch format are documented before implementation;
- old databases remain readable and capability negotiation is additive;
- JSON and protobuf OTLP ingest, direct SQL, Jaeger projection where applicable,
  backup/reopen, optimize, and mixed old/new batches preserve every field;
- memory, storage, and write/read regressions receive an explicit verdict.

## Session 6 — Trace-level summaries

Create a trace query matrix before adding storage. If complete-trace search
semantics justify it, add transactional trace summaries (root/service/start/end,
span/error counts, duration, and completeness state) through a public,
generally useful contract. A summary may span many blocks and therefore cannot
be inferred from one block header.

Exit criteria:

- cross-batch, late-child, duplicate/retry, and partial-trace behavior is
  explicitly defined and tested;
- summary publication is atomic with spans and remains exact after optimize,
  retention, crash, and reopen;
- Jaeger search either uses the summary without semantic change or the proposal
  is rejected and documented;
- write amplification and summary storage are measured alongside read gains.

## Session 7 — Attribute indexes and TraceQL prerequisites

Use the trace query matrix and observed workloads to select a bounded set of
attribute-index primitives, if any. Do not add arbitrary attribute indexing or
claim TraceQL support. Record which later TraceQL rows would be enabled by each
primitive and which require higher-order Rust planning.

Exit criteria:

- every index has an explicit field-selection/configuration contract and
  missing/null/type semantics;
- ingest, storage, WAL, retention, and query-tail costs are measured;
- direct SQLite/libSQL users receive a documented public benefit;
- the final report recommends the structure and ownership boundaries of a
  later TraceQL feature matrix.

## Completion

The plan is complete when every session has a shipped, rejected, or deferred
verdict with reproducible evidence; all accepted changes pass the complete
trace storage, transaction, crash, rich-fidelity, direct-SQL, OTLP, Jaeger,
packaging, and documentation gates; and the final report states exact gains,
tradeoffs, format compatibility, and remaining TraceQL prerequisites.
