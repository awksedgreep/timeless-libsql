# Open issue work plan

This is the durable execution list for the open-issue review performed on
2026-09-10. GitHub remains the source of truth for discussion and closure;
this file records ordering, dependencies, verification, and work that cannot
be completed from the repository alone.

Statuses: `pending`, `in progress`, `externally pending`, `done`, `deferred`.

## Ordered work

| Order | Issue | Priority | Status | Exit condition |
|---:|---|---|---|---|
| 0 | [#50](https://github.com/awksedgreep/timeless-libsql/issues/50) metrics discovery writer-gate exhaustion | P0 | externally pending | Deploy a build containing `ef81a38` to the affected environment; observe at least one active scheduled sweep; record `compact_step_max_ns`, `api_read_errors`, discovery behavior, ingestion accounting, and container health; then close the issue. |
| 1 | [#51](https://github.com/awksedgreep/timeless-libsql/issues/51) LogsQL opaque internal errors | P0 | externally pending | The repository fix and local real-extension verification are complete. Re-run the reported requests against the affected deployment, retain the server-side detail for any remaining `query_execution` fault, and close when production behavior is confirmed. |
| 2 | [#49](https://github.com/awksedgreep/timeless-libsql/issues/49) LogsQL cancellation-counter flake | P1 | done | The `replace`/`replace_regexp` cancellation regression now reaches reader-owned work before asserting the counter; 20 consecutive focused runs and all 93 logs real-extension tests pass. |
| 3 | [#48](https://github.com/awksedgreep/timeless-libsql/issues/48) WriterGate pointer ABA | P1 | done | Writer/read ownership now uses a generation-stamped connection identity; the synthetic pointer-reuse regression proves the later connection cannot re-enter, read through, or release the leaked holder. |
| 4 | [#46](https://github.com/awksedgreep/timeless-libsql/issues/46) remaining API consistency | P1 | externally pending | Repository work is complete: native JSON error contract v1 is implemented and tested without changing Prometheus/MetricsQL, Jaeger, OTLP, or Victoria-compatible ingest shapes. Post the reconciliation evidence to the issue and close it. |
| 5 | [#52](https://github.com/awksedgreep/timeless-libsql/issues/52) metrics size-tiered compaction | P2 | pending | First establish the repeated-arrival growth curve and phase byte counters, then implement and verify byte-bounded, size-tiered raw conversion and compressed merges. |
| 6 | [#55](https://github.com/awksedgreep/timeless-libsql/issues/55) OTel exporter health | P3 | pending | Exporter state, drops, failures, queue pressure, safe configuration, auth/TLS, and bounded outage behavior are observable and tested. |
| 7 | [#53](https://github.com/awksedgreep/timeless-libsql/issues/53) logs/traces maintenance spans | P3 | pending | Add bounded maintenance summary spans and prove trace ingest/export cannot recurse. |
| 8 | [#54](https://github.com/awksedgreep/timeless-libsql/issues/54) request and contention tracing | P3 | pending | Add sampled, redacted, cardinality-bounded request spans with measured disabled/enabled overhead and nonblocking export. |

## Issue #51 execution checklist

- [x] Add a focused real-extension regression using representative rows for:
  - `_time:20m bootfile`;
  - `_msg:*`;
  - matching and non-matching `_msg:~\"(?i)...\"` patterns;
  - `_msg:bootfile` exhausting `max_work_rows` without a time bound;
  - unknown `_time`, `time`, `start`, and `end` form fields.
- [ ] Capture the original affected deployment's server-side error if any
  request still fails after deployment. The local real-extension regression
  reproduced the field-qualified regex semantic fault as an empty result, not
  the reported 500.
- [x] Preserve documented bare-word `_msg` word-filter semantics.
- [x] Keep wide queries explicitly work-bounded; do not invent an implicit
  time range.
- [x] Reject unsupported form parameters with a stable client envelope rather
  than ignoring them or returning `internal`.
- [x] Distinguish malformed query, unsupported capability, query execution,
  query limit, timeout, storage busy, and genuine internal faults without
  exposing SQLite paths, schemas, or customer data.
- [x] Verify the focused regression before and after optimize and reopen.
- [x] Run formatting, all 170 logs library tests, all 93 logs real-extension
  tests, and logs server clippy with warnings denied.
- [ ] Run the complete repository gates after #49 removes the known flaky
  cancellation assertion.

### 2026-09-10 progress

- Reproduced `_msg:~"(?i)bootfile"` returning no matching row on current
  `main`. The simple field parser duplicated the logical field compiler but
  omitted case-insensitive, regex, substring, and prefix branches, so those
  forms fell through to literal equality.
- Unified those field-qualified matcher branches with their typed predicate
  semantics and pinned matching/non-matching regexes plus bare words, `_msg:*`,
  work exhaustion, optimize, shutdown, and reopen.
- Made the POST form reject unknown time aliases explicitly. Added safe
  `storage_busy`/503 classification and a non-sensitive `query_execution`
  reason for unexpected executor failures.

## Issue #49 completion

- Gave the `replace` and `replace_regexp` cancellation cases 5 ms of
  pre-reader headroom, matching the existing `extract_regexp` disposition.
- Kept the 16,384-row expanding transforms, timeout response, storage
  cancellation counter, in-flight drain, and reader-reuse assertions intact.
- Passed the complete focused scenario 20 consecutive times, then passed all
  93 logs real-extension tests, logs clippy with warnings denied, formatting,
  and diff checks.

## Issue #48 completion

- Replaced raw-pointer writer and reader identity with `(address, generation)`,
  where each connection-lifetime registration receives a monotonic generation.
- Captured the identity in metrics, logs, and traces virtual tables and their
  cursors so teardown and read callbacks never re-resolve through a reused raw
  address. Eponymous query TVFs resolve the active registered identity.
- Made connection-pin and transactional DROP-pin cleanup generation-aware, so
  a late old scope cannot erase resources owned by the replacement connection.
- Added a direct synthetic ABA regression that leaves the old writer token in
  place, registers the same address with a new generation, and proves the new
  identity cannot re-enter, read through, or release the old holder.
- Passed all 51 extension unit tests, extension clippy with warnings denied,
  every functional section of `tests/cli.sh`, and all five crash-recovery
  iterations (the crash section was rerun outside the restricted sandbox so
  its intentional kill signal was permitted).

## Issue #46 residual audit

- [x] Confirm the first eleven original findings are implemented or resolved by
  an explicit documented product decision.
- [x] Confirm flush is POST-only in all three routers and correct stale GET
  entries in the authorization route-scope inventory test.
- [x] Confirm actionable `Retry-After` behavior: auth admission 429/503 derives
  it from the subject queue budget; retryable storage-read 503 responses use a
  one-second hint.
- [x] Keep `RateLimit-*` absent. The server enforces concurrent-request
  admission, not a quota over a time window, so those fields would advertise a
  limit that does not exist.
- [x] Make the logs `field_values` policy explicit and test it: the omitted
  1,000-value default clamps to `max_result_rows`; an explicit over-limit value
  fails instead of truncating silently.
- [x] Close an adjacent fail-open path found during reconciliation: invalid or
  overflowing native logs `start`/`end` values no longer become unbounded, and
  unknown `order` values no longer silently mean descending.
- [x] Verify the focused real-extension HTTP scenario, all 172 logs unit tests,
  the complete non-ignored server workspace, and common/logs clippy with
  warnings denied. The one signal-delivery lifecycle test blocked by the
  restricted sandbox passed when rerun with signal permission and the current
  release extension.
- [x] Define and test native JSON error contract v1. Timeless-native query,
  discovery, maintenance, and administration errors carry stable `error` and
  `reason` codes, with optional safe `message` detail; internal detail stays
  server-side. Prometheus/MetricsQL, Jaeger, OTLP, Victoria-compatible ingest,
  and Prometheus text exposition keep their established protocol shapes.
- [ ] After that contract is implemented, update the GitHub issue with the
  reconciliation evidence and close the umbrella. External issue mutation is
  intentionally not performed by this repository-only work session.

### 2026-09-10 error-contract completion

- Added shared native error constructors and operation-specific internal
  reasons for query, stats, flush, and backup failures across all three signal
  servers. Native administration JSON rejects with `invalid_json_body`,
  scrape-target validation is a safe 400, and overlapping backups use the
  stable `backup_in_progress` reason.
- Split traces reads by protocol so Jaeger retains its exact client, timeout,
  limit, and internal envelopes while native dashboard search, trace lookup,
  and tail use contract v1. OTLP remained on its collector contract.
- Split Prometheus label-name, label-value, and series aliases from native
  metrics discovery so their `status`/`errorType`/`error` contract is retained.
  Victoria-compatible metric/log ingestion and all three Prometheus text
  endpoints likewise keep their prior shapes.
- Passed common (34), logs (173), metrics (78 passed, 1 extension-only
  telemetry test skipped by the default suite), and traces (28) library tests;
  the complete non-ignored server package suites using the current release
  extension; all Jaeger (6), OTLP (4), and trace-tail (5) contracts; focused
  real-extension metrics lifecycle/discovery/timeout and logs backup tests;
  the signal lifecycle test with its required signal permission; formatting;
  and common/metrics/logs/traces clippy with warnings denied.

## Tracker reconciliation

- [ ] Confirm the observability-schema v1 acceptance checklist and close
  [#27](https://github.com/awksedgreep/timeless-libsql/issues/27) if the shipped
  v0.8.0 MVP satisfies it; update the stale #20--#26 scopes/checklists.
- [ ] Split the remaining maintainability items from
  [#45](https://github.com/awksedgreep/timeless-libsql/issues/45) and close the
  completed audit umbrella.
- [ ] Keep [#19](https://github.com/awksedgreep/timeless-libsql/issues/19)
  deferred until a concrete similarity-search use case exists.
- [ ] If the #28--#34 compression program is selected, begin with
  [#29](https://github.com/awksedgreep/timeless-libsql/issues/29), the pinned
  corpus and harness; do not select profiles before that evidence exists.
- [ ] Treat #35--#39 as product-selection work rather than implicitly queued
  implementation.

## Gate state at review

- Canonical checkout: clean `main` at `f471e66`.
- Latest successful dispatched production and query-contract gates: `2a6a3c9`
  (`v0.8.2`), before `ef81a38` and `f471e66`.
- No open pull requests, assignments, or milestones were present during the
  review.
