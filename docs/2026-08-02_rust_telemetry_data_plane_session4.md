# Rust telemetry data plane — Session 4 result

Date: 2026-08-02

Branch: `release/rust-telemetry-data-plane`

Starting heads: extension `b39477146e73`; UI `53cd935e4caf`; metrics
`af44a943b219`; logs `344a32b81f18`; traces `49c7813eabe4`

Outcome: pass

Session 4 establishes Phoenix as the control plane and one authenticated,
bounded, independently supervised Rust process as the data-plane owner for
each signal. It does not yet activate the three owners in the default stack or
redirect every historical adapter. That atomic product-default change remains
Session 5, after this authentication and lifecycle boundary is available.

## Control-plane authority

`TimelessUI.TelemetryAuth` owns Ed25519 signing keys, subject policies, token
revocation, global auth version, and audit records in Phoenix/Ecto tables.
Signing-key private material is AES-256-GCM encrypted at rest. The public
verifier document contains only public keys, subject policy, limits, and
revocation state and is replaced atomically with mode `0600`.

The scoped administration API supports key rotation/revocation, policy
creation, auth-version bumps, short-lived token issuance, and token
revocation. Tokens default to five minutes and cannot exceed fifteen minutes.
Every mutation and issuance has an actor, subject, key/token identity, and
action audit record; no full token or private key is stored in audit data.

The controller endpoints live inside the existing browser scope with both
`:require_authenticated_user` and `:require_admin_user`. This placement is
intentional: Phoenix sessions and CSRF protection authenticate the human
administrator, `current_scope` supplies the actor to the context, and the
endpoints are not exposed as bearer-authenticated data-plane routes.

## Rust authorization and limits

The neutral `timeless-api-common` crate validates a compact Ed25519 bearer
claim before dispatch. The three release binaries require a public policy file
by default; unauthenticated mode must be explicitly configured. Liveness is
anonymous so the OS supervisor can distinguish a live-but-not-ready process,
while readiness, stats, reads, writes, and maintenance require signal-specific
scopes.

The verifier checks key id/signature/state and validity, issuer, audience,
issued/not-before/expiry times, maximum token lifetime, tenant, signal, global
and subject auth versions, token revocation, route scope, policy scope, and all
claim limits. Policy reload observes atomic same-size replacements by file
identity as well as size and modification time. Verified request state retains
only audit identity and claims; it never retains the bearer token.

The common middleware enforces:

- compressed request bytes and signal-specific bounded decompression bytes;
- response bytes;
- explicit query row limits, including LogsQL embedded `| limit` clauses;
- read/stats request deadlines;
- per-subject concurrent work and bounded queue wait; and
- exact positive policy maxima for request, decompression, response, rows,
  time, concurrency, and queue duration.

Failures return stable JSON status/code pairs. An admitted write is not
cancelled by the outer request-deadline middleware after it enters a storage
queue: cancelling the HTTP future would create an ambiguous result in which a
reported timeout could commit later. Storage admission and queue wait remain
bounded; Session 5 documents this durable-write response contract.

## Process ownership and observability

`TimelessUI.TelemetryDataPlane.Process` is the single neutral lifecycle seam.
The metrics, logs, and traces facades supply only signal identity. Before
spawning a Rust port, the owner requires the public auth policy and runs the
signal's Session 3 `ReleaseStartup.prepare/2` task. Migration failure remains
closed and inspectable, and an explicit retry reruns preparation; no process
can bypass the startup state machine or directly open the database.

Each signal has an independent registered owner. It binds loopback by default,
passes no bearer credential to the child environment, waits for the binary's
ready marker, merges read-only migration stats into Phoenix administration,
and supplies short-lived internal authorization headers to clients on demand.
Normal shutdown sends `SIGTERM`, waits for drain/reap, and uses a bounded kill
fallback. An abnormal child exit terminates the owner so its supervisor starts
a clean replacement. Target ownership remains fenced by the release startup
and binary owner leases; two owners cannot open one target.

Phoenix exposes status, retry, and auth refresh through cluster administration
without moving user/session/token/policy/configuration/UI state into Rust.
Recursive error sanitization and stderr redaction prevent credential-shaped
values from entering status or logs. Phoenix parameter filtering includes
token, authorization, secret, and private-key fields.

## Regression discovered and fixed

The traces binary originally treated its seven-day server default as an
explicit retention choice. A migrated target can carry another extension-owned
retention policy, so the server would reject or overwrite a valid cutover. The
production server now inherits persisted vtab retention unless an operator
explicitly configures a value. Direct storage callers retain exact enforcement.

Logs and traces fresh/candidate creation now pass their configured retention
to the public vtab constructor, and startup detection validates the persisted
value exactly. Regressions cover default persistence, custom persistence,
configuration drift, and the server's inherited-versus-explicit boundary. The
migrator still does not implement retention, compression, indexing, batching,
or block publication itself.

No failed optimization remains in the tree.

## Validation evidence

Passing release gates:

```bash
cd timeless-libsql/servers
cargo fmt --all --check
cargo check --workspace
TIMELESS_EXT_PATH=../../target/release/libtimeless_ext.so \
  TIMELESS_EXT_TEST_PATH=../../target/release/libtimeless_ext.so \
  cargo test --workspace -- --include-ignored
cargo build --release --workspace

cd ../../timeless_logs
mix format --check-formatted
mix compile --warnings-as-errors
mix test

cd ../timeless_traces
mix format --check-formatted
mix compile --warnings-as-errors
mix test

cd ../timeless_ui
mix precommit
```

Results:

- Rust common: 7 tests; logs: 12 unit/API contract tests; metrics: 22
  unit/storage contract tests; traces: 23 unit/Jaeger/OTLP/storage contract
  tests; all passed, including ignored real-extension contracts;
- metrics legacy/release repository: 489 tests passed with no Session 4 code
  change;
- logs: 227 tests passed;
- traces: 197 tests passed;
- UI: 88 tests passed and one optional real-binary integration test skipped;
  and
- Rust formatting/check/release build and every Elixir formatting,
  warnings-as-errors, whitespace, and precommit gate passed.

The Rust regressions pin missing/malformed credentials, expiry, future claims,
issuer/audience, wrong tenant/signal, key unknown/future/expired/revoked and
same-size rotation, token revocation, global/subject auth version, route and
policy scopes, signature/lifetime, request/decompression/response/query/time/
concurrency/queue limits, and credential non-disclosure. Phoenix regressions
pin key and policy administration, issuance/revocation/audit behavior, admin
authorization, all three independent lifecycle owners, fail-closed migration
and retry, auth preflight, normal drain/reap, and abnormal restart.

## Exit criterion

Pass. All specified identity, key, version, scope, and resource boundaries are
pinned; credential material is absent from stats and logs; and the three
processes independently prepare, start, drain, reap, and restart behind one
exclusive target owner. Session 5 can now switch adapters and defaults as one
declared compatibility change rather than exposing a half-owned runtime.
