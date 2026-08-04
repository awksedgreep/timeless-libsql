# Query conformance test references

This index gives every matrix row marked `shipped` a concrete regression
reference. `tools/query_contracts.py` verifies that the row set is exact, the
file exists, and the named test symbol occurs in that file. A reference is not
permission to weaken the definition of shipped: the test must exercise the
real extension when the behavior reaches storage.

| ID | test path | test symbol | coverage |
|---|---|---|---|
| `PQL-S01` | `servers/crates/timeless-metrics-api/tests/storage_contract.rs` | `session_four_pins_promql_selector_window_errors_and_reopen` | real extension, HTTP, exact response, reopen |
| `PQL-S02` | `servers/crates/timeless-metrics-api/tests/storage_contract.rs` | `session_four_pins_promql_selector_window_errors_and_reopen` | real extension matchers, missing labels, errors |
| `PQL-S03` | `servers/crates/timeless-metrics-api/tests/storage_contract.rs` | `session_four_pins_promql_selector_window_errors_and_reopen` | duplicate matcher AND semantics |
| `PQL-S21` | `servers/crates/timeless-metrics-api/tests/storage_contract.rs` | `session_four_cancels_dropped_promql_requests_and_reuses_the_reader` | cancellation and reader reuse through real extension |
| `PQL-R01` | `servers/crates/timeless-metrics-api/tests/storage_contract.rs` | `session_four_pins_promql_selector_window_errors_and_reopen` | real extension window, boundaries, HTTP, reopen |
| `LQL-F01` | `servers/crates/timeless-logs-api/tests/api_e2e.rs` | `http_uses_the_established_8192_entry_buffer_without_request_flushes` | real extension query path and bounded row result |
| `LQL-F06` | `servers/crates/timeless-logs-api/tests/api_e2e.rs` | `http_uses_the_established_8192_entry_buffer_without_request_flushes` | exact severity through real rich blocks |
| `LQL-F07` | `servers/crates/timeless-logs-api/tests/api_e2e.rs` | `http_uses_the_established_8192_entry_buffer_without_request_flushes` | service index pruning through real extension |
| `LQL-P01` | `servers/crates/timeless-logs-api/tests/api_e2e.rs` | `http_uses_the_established_8192_entry_buffer_without_request_flushes` | bounded limit through API and extension |
| `LQL-S01` | `servers/crates/timeless-logs-api/tests/api_e2e.rs` | `http_uses_the_established_8192_entry_buffer_without_request_flushes` | scalar native count through real extension |

`LQL-F02` was downgraded to `partial` when this index exposed that its parser
test did not prove deterministic real-extension boundaries. See `QSF-008`.
