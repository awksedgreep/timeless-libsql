# Deferred and experimental work

This document is the discovery index for work deliberately not shipped in the
current `0.4.x` source line. It keeps future sessions from depending on old
chat context or searching long implementation narratives.

The owning feature matrix remains authoritative. This index does not replace
the exact semantics, upstream oracle behavior, SQL disposition, tests,
evidence, or prerequisites in those rows:

- [PromQL and MetricsQL feature matrix](PROMQL_FEATURE_MATRIX.md)
- [LogsQL feature matrix](LOGSQL_FEATURE_MATRIX.md)
- [Trace query matrix](2026-08-08_trace_query_matrix.md)
- [TraceQL prerequisite matrix](2026-08-08_traceql_prerequisite_matrix.md)
- [Query release report](QUERY_RELEASE_REPORT.md#experimental-and-deferred-dispositions)

## PromQL deferred rows

| ID | work | prerequisite or reason |
|---|---|---|
| `PQL-S10` | Step-relative ranges such as `[5i]` | Pinned Prometheus rejects the syntax. It is shipped separately as MetricsQL `MQL-09`, not part of stable PromQL. |
| `PQL-S17` | Prometheus stale-marker semantics | Add bit-preserving marker-capable ingress and marker-aware selector/range/window execution that excludes only the canonical stale marker while preserving ordinary NaNs. |
| `PQL-S22` | Native histogram sample type | Design versioned typed samples, batches, chunks, SQL and packed results, ingress, rollups, retention, and migration. Current storage explicitly advertises float64 only. |
| `PQL-O19` | Native-histogram trim operators `</` and `>/` | Requires the typed native-histogram model in `PQL-S22`; float/classic-histogram approximations are not equivalent. |
| `PQL-H04` | Value-producing native histogram functions | Float-input behavior is shipped. Producing `histogram_avg/count/sum/stddev/stdvar` values requires `PQL-S22`. |

## PromQL experimental rows

These rows are not silently enabled on stable routes. Each requires a
separately configured feature tier and a matching feature-enabled upstream
oracle before implementation can be called compatible.

| IDs | work | principal gate |
|---|---|---|
| `PQL-O17` | `limitk`, `limit_ratio` | Experimental Prometheus function gate plus bounded group state; full parity also depends on native histograms. |
| `PQL-O18` | Binary `fill`, `fill_left`, `fill_right` | Experimental binary-modifier gate and complete bounded vector matching. |
| `PQL-R07`, `PQL-R21`, `PQL-R22`, `PQL-R23` | `first_over_time`, `double_exponential_smoothing`, `mad_over_time`, and timestamp-of-range functions | Experimental-function tier and pinned oracle; some rows already have executable SQL foundations. |
| `PQL-F14` | `sort_by_label`, `sort_by_label_desc` | Experimental tier and exact natural multi-label ordering. |
| `PQL-F19`, `PQL-F20` | Query-context functions and `min_of`/`max_of` | Prometheus duration-expression feature; MetricsQL ownership remains separate where applicable. |
| `PQL-F21` | `info` | Experimental tier plus stale-marker and typed-histogram prerequisites. |
| `PQL-H03` | `histogram_quantiles` | Experimental Prometheus tier; VictoriaMetrics uses a separately tracked argument contract. |

## MetricsQL deferred rows

| ID | work | prerequisite or reason |
|---|---|---|
| `MQL-08` | `label_keep`, `label_map`, `quantiles`, `distinct`, `increase_pure`, `remove_resets`, `interpolate`, `keep_last_value`, `keep_next_value`, `drop_common_labels`, `rate_over_sum`, and `WITH` | Split the finite catalog into individually specified rows with immutable oracle cases, ownership, limits, and evidence. |
| `MQL-11` | `min_of`, `max_of` | The pinned VictoriaMetrics release rejects them. Use a later immutable oracle tier or define an explicitly Timeless-only contract. |

## LogsQL deferred rows

| ID | work | prerequisite or reason |
|---|---|---|
| `LQL-F35`, `LQL-F36` | Stream selectors and `_stream_id` filters | Design versioned tenant identity, ingestion-declared stream fields, canonical stream IDs, batches, indexes, codecs, and migration. Ordinary metadata is not stream identity. |
| `LQL-P50` | `stream_context` | Requires the same stored tenant-scoped stream identity plus bounded surrounding-row semantics. |
| `LQL-P10`, `LQL-P11` | `block_stats`, `blocks_count` | Define a stable public Timeless block-stat snapshot without leaking private paths, codecs, mutable shadow schemas, or fabricating VictoriaLogs processing-batch lineage. |
| `LQL-Q03` | Intra-query concurrency/parallel-reader options | Design fan-out within one query. A pool serving independent SQLite requests is not equivalent. |
| `LQL-Q06` | Partial responses | Requires multiple fenced storage owners, bounded deterministic merge, failure classification, and response-completeness metadata. One authoritative libSQL owner cannot return an honest partial success. |

## Trace query and TraceQL work

The current store retains spans, not finalized traces. It cannot infer
completeness from root presence, inactivity, or a block boundary.

| ID | work | prerequisite or owner |
|---|---|---|
| `TSQ-06` | Complete-trace search | Define a versioned trace-level route, root/envelope/any-span filters, trace-level order/limit, duplicate/retry policy, retention truncation, and honest completeness behavior before considering persisted contributions. |
| `TSQ-07` | Trace completeness | Add an explicit producer/finalization or retry-identity contract. OTLP does not provide one today. |
| `TSQ-10` | Trace statistics over a time window | Select an exact population: roots, retained envelopes, any matching span, or another explicitly named model. These are not interchangeable. |
| `TQP-05`, `TQP-06`, `TQP-10`, `TQP-12` | Boolean span sets, trace quantifiers, structural graph operators, and TraceQL parser/pipelines | Higher-order Rust trace library/API work over existing public rows and IDs; TraceQL text does not belong in SQLite. |
| `TQP-07` | Attribute range and regex predicates | Define typed comparison/coercion and measure the public-row implementation before proposing a new storage primitive. |
| `TQP-08`, `TQP-09` | Event and link attribute predicates | Define existential/quantified semantics and repeated event/link identity; a scalar attribute index is insufficient. |
| `TQP-11` | Trace duration/count/error/root predicates | Select a retained-trace population and duplicate/retention policy; completeness remains unknown under the current ingest contract. |

Already shipped prerequisites `TQP-01` through `TQP-04` provide opt-in exact
typed scalar equality for span, resource, and instrumentation-scope JSON
Pointers while preserving missing/null/empty distinctions. They are storage
primitives, not a claim that TraceQL is implemented.

## Other known follow-up work

| area | current state | next decision |
|---|---|---|
| Native release packaging | `v0.4.0` Linux candidates passed; macOS failed because the release tool linked against Apple's restricted system SQLite; no complete checksum set or GitHub Release exists. | Make the release tool independent of Apple's restricted SQLite, validate both macOS architectures, and use a new immutable tag. |
| Remaining Python benchmark utilities | The production extension/API/query/SQL/crash harnesses are Rust, but the exact remaining scripts are inventoried in [TESTING.md](../TESTING.md#tooling-migration-status). | Port one bounded harness at a time while retaining fixture, completed-work, percentile, storage, cleanup, and RSS contracts. |
| Competitive baselines | Current evidence measures correctness, regression, SQLite controls, public storage work, and endpoint tails. It does not claim VictoriaMetrics, VictoriaLogs, or Jaeger performance parity. | Add pinned, same-fixture competitive measurements when they become a priority; do not weaken current gates in the process. |
| Higher-order Elixir interfaces | The Rust servers own the data plane; the query release report contains the control-plane recommendations. | Inventory and redesign downstream Elixir interfaces in their own repositories and goal, without moving query/storage work back into Elixir. |

## Evidence that remains available

The 84-hour query implementation record is not stored only in README text:

- [QUERY_FEATURES.md](QUERY_FEATURES.md) defines ownership and the one-row
  implementation workflow.
- The PromQL/MetricsQL and LogsQL matrices retain all 216 stable row IDs and
  terminal dispositions.
- [QUERY_TEST_REFERENCES.md](QUERY_TEST_REFERENCES.md) maps every shipped row
  to an executable regression.
- [QUERY_SQL_EQUIVALENTS.md](QUERY_SQL_EQUIVALENTS.md) retains all honest
  direct-SQL foundations and explicit no-equivalent decisions.
- [QUERY_EVIDENCE.md](QUERY_EVIDENCE.md) and `docs/evidence/` retain measured
  result, latency, storage-work, response-byte, durability, and RSS evidence.
- [QUERY_STORAGE_FINDINGS.md](QUERY_STORAGE_FINDINGS.md) is append-only through
  `QSF-310`; failed optimizations and regressions remain visible.
- [The sequential plan](2026-08-04_query_surface_implementation_plan.md) and
  dated trace plans/reports preserve implementation order and exit criteria.
- Git history preserves earlier document revisions, but the files above are
  the maintained future-work contract and should be preferred over archaeology.

## Resuming a row

When work resumes, select the stable ID in its owning matrix, change it to
`in progress`, add the failing semantic regression first, implement through
the documented extension/API boundary, compare with the pinned oracle, update
SQL/docs/evidence/findings in the same commit, and mark it shipped only after
all applicable gates pass. Do not infer scope from this index alone.
