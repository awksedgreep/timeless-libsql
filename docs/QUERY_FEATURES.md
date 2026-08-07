# Query feature maps

This is the coordination page for query work in `timeless-libsql`.
The feature maps are the source of truth for what the storage extension can do,
what the Rust signal APIs compose from those primitives, and what deliberately
belongs in a higher-order Timeless library.

- [PromQL feature matrix](PROMQL_FEATURE_MATRIX.md)
- [LogsQL feature matrix](LOGSQL_FEATURE_MATRIX.md)
- [SQL equivalents for query-language features](QUERY_SQL_EQUIVALENTS.md)
- [Query/storage findings](QUERY_STORAGE_FINDINGS.md)
- [Shipped-row conformance references](QUERY_TEST_REFERENCES.md)
- [Pinned semantic oracles](QUERY_ORACLES.md)
- [Query evidence protocol](QUERY_EVIDENCE.md)
- [Sequential implementation plan](2026-08-04_query_surface_implementation_plan.md)
- [SQL query cookbook](QUERIES.md)

The baseline represented by the first version of these maps is commit
`a72634e` on 2026-08-04. Exact source and container versions live in the
[oracle contract](QUERY_ORACLES.md). Upstream language references are
snapshots, not a claim that every upstream feature belongs in this project:

- [PromQL basics](https://prometheus.io/docs/prometheus/latest/querying/basics/),
  [operators](https://prometheus.io/docs/prometheus/latest/querying/operators/),
  and [functions](https://prometheus.io/docs/prometheus/latest/querying/functions/)
- [VictoriaLogs LogsQL](https://docs.victoriametrics.com/victorialogs/logsql/)
- [VictoriaMetrics MetricsQL](https://docs.victoriametrics.com/victoriametrics/metricsql/)

## Ownership boundary

Every matrix row has one primary target. A feature can use several layers, but
only one layer owns its public semantics.

| target | responsibility |
|---|---|
| `EXT` | A public SQLite/libSQL virtual table, scalar function, hidden-column contract, pruning rule, or packed result. Use this only when storage awareness materially avoids reads, decode, copies, or cross-boundary rows. |
| `API` | Parsing, AST validation, query planning, expression evaluation, result shaping, cancellation, and Prometheus/Victoria-compatible HTTP behavior in the Rust signal server. |
| `SQL` | Composition that ordinary SQLite already expresses well. Document and test the executable statement in the [SQL equivalents cookbook](QUERY_SQL_EQUIVALENTS.md) instead of adding a special extension API. The Rust API may generate this SQL. |
| `LIB` | Application behavior: saved queries, recording/alert rules, subscriptions, live tail, dashboards, scrape-target administration, token issuance, cluster policy, or UI state. These belong in `timeless_metrics`, `timeless_logs`, `timeless_traces`, TimelessUI, or another higher-order consumer. |
| `DEFER` | The feature requires a data type or storage identity that Timeless does not retain, is experimental upstream, or has not earned its implementation cost. The row must say why. |

PromQL and LogsQL syntax do **not** belong in the extension. The extension is a
general SQLite/libSQL storage and query accelerator. The Rust APIs own language
compatibility and lower expressions onto public SQL/extension primitives. A
direct SQLite user can use those same primitives without starting a Timeless
HTTP server.

## Status vocabulary

| status | meaning |
|---|---|
| `shipped` | Parses, executes, is documented, and has a semantic regression test against real extension storage. |
| `partial` | Some syntax or execution path exists, but the matrix names missing semantics. |
| `in progress` | A temporary working-branch state: the failing oracle exists and implementation is underway. It must become `shipped`, return to its prior state, or be documented as deferred before release. |
| `missing` | Not implemented in the Rust data plane. An implementation may already exist in an Elixir library and serve as a porting oracle. |
| `experimental` | Deliberately not shipped by default while upstream semantics or the product cost/benefit remains experimental. The row names the decision and reconsideration trigger. |
| `deferred` | Deliberately not scheduled until the recorded prerequisite or product decision changes. |
| `library` | Intentionally remains a higher-order library concern. |

`shipped` is intentionally strict. A parser test alone is not completion, and
an HTTP response assembled from a fake store is not storage parity.

## One-feature workflow

Query work proceeds one matrix row at a time. Closely related rows may share a
commit only when they are one grammar/evaluator primitive and cannot be tested
honestly in isolation.

1. Select one `missing` or `partial` row and change it to `in progress` in the
   working branch.
2. Add the smallest conformance fixture that distinguishes the required
   semantics, including empty, boundary, invalid, and cancellation cases.
3. Run it against the current Rust API and record the expected failure before
   implementation.
4. Implement at the row's target layer. Never add an extension primitive just
   to avoid writing an ordinary SQL expression.
5. Test direct SQLite/libSQL behavior for every new or changed `EXT` primitive.
   For every `SQL` target or foundation, add the equivalent parameterized
   statement to the [SQL equivalents cookbook](QUERY_SQL_EQUIVALENTS.md),
   state what language semantics still belong to the API, and execute the
   statement through the real extension with the Rust `timeless-query-harness`.
   If no honest public SQL equivalent exists, change the ownership
   classification.
6. Test the Rust parser, planner, evaluator, HTTP envelope, and reopen behavior
   for every `API` feature.
7. Compare PromQL with Prometheus/VictoriaMetrics or LogsQL with VictoriaLogs
   when an upstream semantic oracle exists. Record intentional differences.
8. Measure completed work, p50/p95/p99, result cardinality, bytes read/decoded,
   response bytes, cancellation, and RSS HWM on a representative narrow and
   wide fixture. Performance does not decide correctness, but it decides
   whether pushdown is justified.
9. Add the public example and error behavior to the relevant user guide.
10. Update the matrix row to `shipped` and append any surprising storage
    behavior to [the findings log](QUERY_STORAGE_FINDINGS.md) in the same
    commit.

If an approach fails, keep the regression fixture, document the result, revert
the failed implementation, and continue with an independent row.

## Delivery order

1. **Restore known product parity.** Port the established
   `timeless_metrics` PromQL behavior and the established `timeless_logs`
   LogsQL/DDNet grammar before adding novel compatibility.
2. **Complete storage-native cores.** Prioritize selectors, time bounds,
   exact/indexed filters, counters, aggregations, sorting, pagination, and
   discovery that can exploit existing indexes and packed frames.
3. **Complete language composition.** Add vector matching, label transforms,
   logical filters, pipelines, and response transformations in the Rust APIs.
4. **Optimize only from evidence.** Promote work into `EXT` only after query
   measurements show that SQL/API composition pays an avoidable storage or
   decode cost.
5. **Revisit deferred types and higher libraries.** Native histograms (which
   the capability document explicitly reports as unsupported), log stream
   identity, subscriptions, rules, and UI workflows are separate decisions
   after the storage/query contract is stable.

## Release gate for query work

A `timeless-libsql` release must publish the matrices as they were tested. CI
must reject a `shipped` row without its named conformance coverage and reject
server documentation that advertises a smaller or larger surface than the
matrix. A shipped row that names `SQL` must link its executable cookbook
recipe. Higher-order Timeless releases should link to the exact
`timeless-libsql` version's maps rather than restating a drifting query list.
