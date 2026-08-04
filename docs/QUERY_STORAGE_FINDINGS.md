# Query and storage findings

This append-only log records storage behavior discovered while implementing
the [PromQL](PROMQL_FEATURE_MATRIX.md) and
[LogsQL](LOGSQL_FEATURE_MATRIX.md) feature maps. It is not a substitute for a
regression test. Every resolved finding links to the test and the public
documentation that now pins the behavior.

## Baseline constraints

These are known constraints at the 2026-08-04 `a72634e` baseline. They are not
new defects, but query work must not accidentally obscure them.

| ID | behavior | consequence |
|---|---|---|
| `QSF-001` | Metric window kernels use `(T-window,T]`. | This matches PromQL range-vector boundaries, but callers must still implement lookback, staleness, counter extrapolation, and output-label rules. |
| `QSF-002` | `timeless_window(..., 'rate'|'increase')` is a storage kernel, not a complete PromQL evaluator. | The Rust API must apply PromQL reset, extrapolation, carry-in, sparse-series, and name-retention semantics rather than relabeling the native result as PromQL. |
| `QSF-003` | Metric storage contains float samples and timestamps but no native-histogram sample type. | Classic `_bucket` histograms are queryable as float series. Native-histogram-only operators and functions remain deferred until a typed storage design exists. |
| `QSF-004` | Stored metrics do not currently preserve an explicit Prometheus stale-marker contract. | Five-minute lookback can be implemented, but exact stale-marker parity needs a storage/ingest decision and must not be claimed implicitly. |
| `QSF-005` | Logs retain typed nested metadata, while the term index intentionally covers only selected low-cardinality keys. | Arbitrary metadata predicates are correct through bounded decode, but only indexed keys can promise posting-list pruning. Measurements decide whether another key earns indexing. |
| `QSF-006` | Message substring filtering and many metadata transformations require block decode. | The API can implement them correctly now; an extension index or new block metadata is justified only by measured workloads. |
| `QSF-007` | Timeless logs do not currently assign VictoriaLogs-compatible `_stream_id` identities. | Ordinary field filters fit now. `_stream_id`, stream-context, and stream-specific optimizations are deferred pending an explicit stream model. |

## Findings

Add one entry for every unexpected correctness, performance, durability,
memory, or planner behavior found during feature work.

| ID | date | matrix row | observation | expected behavior | evidence/test | disposition | status |
|---|---|---|---|---|---|---|---|
| `QSF-008` | 2026-08-04 | `LQL-F02` | Relative LogsQL time is computed from the process wall clock inside parsing; no injected evaluation clock reaches the real-extension HTTP contract. | The request owns one explicit evaluation instant so inclusive/exclusive boundaries and reopen fixtures are deterministic. | `workload_logsql_shapes_parse`; query-contract shipped-row audit | Downgraded to `partial`; Session 10 adds clock injection and a real-extension boundary regression. | open |
| `QSF-009` | 2026-08-04 | `LQL-F06` | The exact `error` baseline prunes to one error-family block but decodes 4,096 entries to return 1,024 because `error`, `critical`, `alert`, and `emergency` share a physical partition. | Exact severity fidelity is preserved even when the coarse partition needs decode. | `docs/evidence/2026-08-04_query_baseline.json`; `http_uses_the_established_8192_entry_buffer_without_request_flushes` | Accepted storage/write tradeoff. Consider finer severity metadata only after representative workloads prove a material read benefit without harming 8,192-entry batching/compression. | accepted |
| `QSF-010` | 2026-08-04 | `PQL-S06` | The public raw TVF uses inclusive read bounds, while a PromQL range selector uses `(T-W,T]`. | The API and direct SQL recipe must discard samples exactly on the left boundary without changing the extension contract. | `tests/cli.sh` section 33; `session_four_pins_promql_selector_window_errors_and_reopen` | Keep the general raw contract intact and apply the one-boundary predicate in SQL/Rust. No new extension primitive or format change is justified. | accepted |
| `QSF-011` | 2026-08-04 | `PQL-R01` | The existing Rust `avg_over_time` response retained `__name__`, despite the pinned Prometheus fixture already requiring range-function metric-name removal. | Range-function output labels omit the metric name. | `tests/query_oracles/prometheus/promql_smoke.yml`; `session_four_pins_promql_selector_window_errors_and_reopen` | Corrected both packed-window and raw fallback response paths and pinned the exact label set. | resolved |
| `QSF-012` | 2026-08-04 | `PQL-S06` | Metrics API accounting exposed returned frame/response bytes but the public extension stats had no candidate-chunk, payload-byte, or decode counters for packed raw reads. | Direct SQLite/libSQL users and API benchmarks can attribute selected series, candidate chunks, stored payload bytes, fully decoded points, buffered points considered, and returned points. | `tests/cli.sh` section 34; `docs/evidence/2026-08-04_session2_pql_s06.json` | Added cumulative, per-process `raw_batch_query_*` counters to `timeless_stats`; storage and frame formats are unchanged. | resolved |
| `QSF-013` | 2026-08-04 | `PQL-S06` | An incremental local release build retained the previous Git SHA because the common build script watched only an environment override, not the active Git ref. | Checked evidence must identify the exact source commit that produced every measured signal binary. | `test_evidence_rejects_a_stale_release_binary_identity`; Session 2 evidence harness | The build script now watches `HEAD`, its symbolic ref, and `packed-refs`; the evidence harness fails before workload setup when a binary SHA differs from `HEAD`. Release CI's explicit SHA override remains authoritative. | resolved |

Use stable IDs (`QSF-008`, `QSF-009`, ...). Do not delete resolved findings;
change their status and link the correcting regression.
