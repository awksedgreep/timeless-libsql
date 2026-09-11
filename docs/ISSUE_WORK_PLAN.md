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
| 4 | [#46](https://github.com/awksedgreep/timeless-libsql/issues/46) remaining API consistency | P1 | in progress | Reconcile the umbrella after #51; split any remaining cross-server envelope or `field_values` contract into focused leaf issues and close the completed audit. |
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
