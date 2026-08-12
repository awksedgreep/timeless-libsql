# Auth opt-in plan

Status: proposal, no code changed. Written against the current
`timeless-libsql` working tree and `timeless_stack` `3bb0894`.

This document specifies making the three Rust signal servers start **wide open
by default**, making authentication **opt-in at every level**, giving Rust and
SQLite users a **shipped path to generate keys and tokens** if they want auth,
fixing the **security defects** the current design leaves in place, and
updating **every document** that encodes the old design.

---

## 1. Audience priority — read this first

**The overwhelming majority of users will consume Timeless directly from
SQLite or from Rust. They are the primary audience and every default,
document, and error message must serve them first.**

Expected distribution:

| Audience | Share | How they consume it |
|---|---|---|
| **SQLite, directly** | Largest | `.load libtimeless_ext`, then SQL against the vtabs. No HTTP, no server, no auth. |
| **Rust, embedded** | Large | `timeless-ext` with `features = ["embedded"]` linked into their process, or `timeless-*-api` as a library calling `run(Config { .. })`. |
| **Standalone binaries** | Meaningful | Download `timeless-metrics-api`, run it, point Prometheus/OTLP at it. |
| **`timeless_stack`** | Smallest | Phoenix control plane managing the servers as children. |

`timeless_stack` is **secondary**. It is one deployment topology among
several, it has a maintainer who can configure it, and it already works. It
must not set the defaults for everyone else.

The current design inverts this. The servers were built as children of the
Phoenix control plane and inherited that context as their default posture:
mandatory token verification, with the only implementation of the key and
token machinery living inside `timeless_ui`, in Elixir. Everyone outside that
one topology is left holding a policy-file format with no tool to produce it.

This inversion is the actual defect. The specific fixes below follow from
correcting it.

### 1.1 Rules that follow

1. A SQLite user must never encounter authentication. There is no server in
   their path and nothing to configure. This is true today; keep it true.
2. A Rust embedder must get a working server from `Config::default()` with no
   auth reasoning required. This is true today; keep it true.
3. A standalone binary user must get a working server from `./timeless-*-api`
   with no arguments and no environment. **This is false today and is the
   central fix.**
4. Every one of the three must behave the same way. Divergence between the
   library and the binary is itself a defect.
5. `timeless_stack` opts into auth explicitly, as it already does. It keeps
   working with no changes.
6. Documentation is written for audiences 1–3 first. The Phoenix control plane
   is described as one deployment option, not as the frame.

---

## 2. The problem

`timeless-metrics-api`, `timeless-logs-api`, and `timeless-traces-api` do not
start with zero configuration. They exit with code 2 before binding a listener.

`servers/crates/timeless-api-common/src/auth.rs:67-85`:

```rust
pub fn required_from_env(signal: &str) -> Result<Self, String> {
    match std::env::var("TIMELESS_AUTH_MODE") {
        Ok(mode) if mode == "disabled" => return Ok(Self::disabled()),
        Ok(mode) if mode != "required" => {
            return Err(format!(
                "TIMELESS_AUTH_MODE must be required or disabled, got {mode:?}"
            ))
        }
        Ok(_) | Err(std::env::VarError::NotPresent) => {}   // unset falls through to REQUIRED
        Err(error) => return Err(format!("read TIMELESS_AUTH_MODE: {error}")),
    }
    let policy = std::env::var("TIMELESS_AUTH_POLICY_FILE").map_err(|_| {
        "TIMELESS_AUTH_POLICY_FILE is required unless TIMELESS_AUTH_MODE=disabled".to_owned()
    })?;
    ...
}
```

An unset `TIMELESS_AUTH_MODE` lands in the no-op arm and proceeds to demand a
policy file. Called from `timeless-metrics-api/src/main.rs:46`,
`timeless-logs-api/src/main.rs:46`, `timeless-traces-api/src/main.rs:43`, each
mapping the error to `ExitCode::from(2)`.

Satisfying it is not "set a token." `PolicyFile` (`auth.rs:145-176`) has no
serde defaults on any load-bearing field. An operator must hand-author JSON
containing `version`, `issuer`, `audience`, `tenant`, `minimum_auth_version`,
`max_token_seconds`, a seven-field `maximum_limits`, a `subjects` map where
each subject repeats its own seven-field `maximum_limits`, and a `keys` array
of Ed25519 public keys with `kid` / `not_before` / `expires_at`. They must then
mint Ed25519-signed JWS tokens carrying `iss`, `aud`, `sub`, `jti`, `tenant`,
`signal`, `scopes`, `auth_version`, `iat`, `nbf`, `exp`, and `limits`.

**No tooling ships to produce either artifact.** There is no keygen, no policy
scaffold, and no token minter anywhere in `tools/` or the workspace. The only
working implementation is Elixir, inside `timeless_ui`.

### 2.1 Current state by audience

| Audience | Status today |
|---|---|
| SQLite, directly | **Unaffected.** No HTTP server exists in `crates/timeless-ext`; verified by grep for `axum` / `AuthConfig` / `Authorization` — zero hits. See `docs/EMBEDDED_RUST.md`. |
| Rust, embedded | **Works.** `Config` fields are `pub`, `Default` sets `auth: AuthConfig::disabled()` (`metrics lib.rs:118`, `logs lib.rs:107`, `traces lib.rs:61`), and `AuthConfig::disabled()` is a public constructor (`auth.rs:46`). |
| Standalone binaries | **Broken.** Exit code 2, with an error naming a file format the user has no tool to produce. |
| `timeless_stack` | **Works.** Sets `auth_mode: :required` and `auth_policy_path` explicitly at `config/runtime.exs:104`, `:118`, `:132`. |

The single broken row is the one this plan fixes. Note that it is also the
on-ramp: a SQLite or Rust user who wants an HTTP endpoint reaches for the
binary, and that is where they hit the wall.

### 2.2 What is genuinely good, and stays

The verifier is careful work and none of it needs to be discarded: fail-closed
before listener bind, stable machine-readable error codes, 30-second
clock-skew tolerance, key and JTI revocation, scope containment, and policy
caching keyed on device+inode so a swapped file is noticed.

The Elixir provisioning is also well built. `timeless_ui` auto-generates the
keypair (`telemetry_auth.ex:50`), publishes the policy at mode 0600, mints
short-lived tokens, and refreshes them 60s before expiry. The private key never
leaves Phoenix. This plan does not touch it.

The problem is not the mechanism. It is that the mechanism is mandatory, and
that it is reachable only from Elixir.

### 2.3 Market context

Every comparable telemetry server ships open.

VictoriaMetrics single-node defaults to `-httpListenAddr=:8428` — all
interfaces — with no authentication. Ingest and query answer unauthenticated
on a fresh binary. `-httpAuth.username` / `-httpAuth.password` are optional and
empty by default. The cluster components (`vminsert` / `vmselect` /
`vmstorage`) have no auth at all and no flag to add it; the documentation says
plainly they must not be exposed to untrusted networks. Authentication is
`vmauth`, a **separate opt-in binary** in front of the cluster. The storage
tier stays dumb.

Prometheus, Loki, Tempo, and InfluxDB OSS follow the same posture.

Timeless today is two full steps more locked down than any of them: loopback
bind *and* mandatory token verification. Nobody evaluating this product will
expect that, and most will not get past the first `./timeless-metrics-api`.

---

## 3. Design principles

1. **Open by default, at every level.** Extension, library, and binary must
   agree.
2. **Auth is opt-in and explicit.** Never inferred, never conditional on bind
   address, never on by omission.
3. **Opt-in must be usable without Elixir.** If turning auth on requires
   implementing an Ed25519 JWS signer, it is not a feature.
4. **Open must not mean dangerous.** Endpoints that are hazardous when
   unauthenticated get fixed on their merits, not hidden behind a default.

### 3.1 Rejected alternative: loopback-aware default

An earlier idea was to disable auth on loopback and require it on non-loopback
binds, pairing with the existing `TIMELESS_ALLOW_NON_LOOPBACK` gate.

**Reject this.** In a container you must bind `0.0.0.0` or the port is
unreachable from the host, so every containerized user takes the non-loopback
branch and hits the auth wall. This is confirmed by the project's own
deployment: the stack `Dockerfile` sets `TIMELESS_TELEMETRY_BIND=0.0.0.0` and
the Elixir launcher then sets `TIMELESS_ALLOW_NON_LOOPBACK=1`. The rule would
gate the primary deployment path while appearing arbitrary to the user. It is
also why VictoriaMetrics binds `:8428` rather than loopback.

Behavior that changes based on bind address is surprising. Do not do it.

---

## 4. Part 1 — flip the binary default

### 4.1 `auth.rs`

Rename `required_from_env` to `from_env` (the old name becomes a lie) and
invert the unset case. Replace `auth.rs:67-85` with:

```rust
/// Reads the opt-in auth configuration for `signal`.
///
/// Auth is disabled unless `TIMELESS_AUTH_MODE=required` is set explicitly.
/// This matches the library `Config::default()` and every comparable
/// telemetry server. Enabling auth requires `TIMELESS_AUTH_POLICY_FILE`.
pub fn from_env(signal: &str) -> Result<Self, String> {
    match std::env::var("TIMELESS_AUTH_MODE") {
        Err(std::env::VarError::NotPresent) => return Ok(Self::disabled()),
        Ok(mode) if mode == "disabled" => return Ok(Self::disabled()),
        Ok(mode) if mode == "required" => {}
        Ok(mode) => {
            return Err(format!(
                "TIMELESS_AUTH_MODE must be required or disabled, got {mode:?}"
            ))
        }
        Err(error) => return Err(format!("read TIMELESS_AUTH_MODE: {error}")),
    }
    let policy = std::env::var("TIMELESS_AUTH_POLICY_FILE").map_err(|_| {
        "TIMELESS_AUTH_POLICY_FILE is required when TIMELESS_AUTH_MODE=required".to_owned()
    })?;
    let tenant = std::env::var("TIMELESS_TENANT").unwrap_or_else(|_| "default".to_owned());
    let config = Self::enforced(signal, tenant, policy);
    config.preflight()?;
    Ok(config)
}
```

Note the arm reordering: the explicit `NotPresent` early return is what makes
the new default readable. Keep `preflight()` — when a user *does* opt in, a bad
policy file must still fail closed before the listener binds.

The error string changes from "unless `TIMELESS_AUTH_MODE=disabled`" to "when
`TIMELESS_AUTH_MODE=required`", which now reads correctly.

### 4.2 Call sites

Three lines, mechanical:

- `servers/crates/timeless-metrics-api/src/main.rs:46` — `AuthConfig::from_env("metrics")`
- `servers/crates/timeless-logs-api/src/main.rs:46` — `AuthConfig::from_env("logs")`
- `servers/crates/timeless-traces-api/src/main.rs:43` — `AuthConfig::from_env("traces")`

No other changes in the mains. `usage_error` still handles a malformed
`TIMELESS_AUTH_MODE` or an unreadable policy file.

### 4.3 What this fixes for free

`Config::default()` already sets `auth: AuthConfig::disabled()` in all three
crates while the binaries required it. That divergence is a footgun: the same
`Config` type yields an open server for an embedder and a hard failure for an
operator, with the difference living in `main.rs` rather than in the type.
After the flip they agree.

### 4.4 Blast radius

Zero for `timeless_stack`. `runtime.exs:104`/`:118`/`:132` set
`auth_mode: :required` explicitly, which `process.ex:295` turns into an
explicit `TIMELESS_AUTH_MODE=required` in the child env. The default is never
consulted on that path. `config/dev.exs:42`/`:52`/`:62` likewise set
`auth_policy_path` explicitly.

Zero for SQLite and Rust embedders — they were already open.

The only behavior change is for standalone binary users, who today cannot start
the server at all.

---

## 5. Part 2 — unauthenticated health endpoints

Today `/live` is the sole exemption (`auth.rs:288-290`, exact path match — not
prefix or suffix, so it is not spoofable):

```rust
if request.uri().path() == "/live" {
    return next.run(request).await;
}
```

`/ready` and `/health` require `<signal>:stats` (`required_scope`,
`auth.rs:378-398`). A Kubernetes readiness probe, an ELB health check, or a
Docker `HEALTHCHECK` therefore needs a minted token — an adoption problem
independent of the default flip, and one that persists for anyone who opts in.

**Recommendation:** exempt all three fixed paths.

```rust
const UNAUTHENTICATED_PATHS: [&str; 3] = ["/live", "/ready", "/health"];

if UNAUTHENTICATED_PATHS.contains(&request.uri().path()) {
    return next.run(request).await;
}
```

Keep exact-match semantics. Do **not** exempt the `path.ends_with("/stats")`
family — signal stats expose series cardinality and storage internals and stay
behind `<signal>:stats`.

**Disclosure tradeoff to decide:** `/ready` reports startup and migration
state, capability and minimum-version negotiation, build identity, and storage
health. Exempting it publishes exact build identity to anyone who can reach the
port. Prometheus (`/-/healthy`, `/-/ready`) and VictoriaMetrics accept this.
If you would rather not, the alternative is a terse unauthenticated response
(`{"status":"ready"}`) with the detailed report kept behind `<signal>:stats` —
more code, and the terse form is what probes actually consume. Recommend the
terse split if you have appetite for it; plain exemption if you want this
merged quickly.

---

## 6. Part 3 — make opt-in usable without Elixir

Flipping the default without this leaves auth technically present and
practically unreachable for the primary audience. Two changes, in order of
value.

### 6.1 Give `ClaimLimits` defaults (high value, small change)

`ClaimLimits` (`auth.rs:99-107`) has seven mandatory fields and no `Default`.
A policy file must spell them out once for `maximum_limits` and again for every
subject. This is the single largest contributor to the hand-authoring burden.

Add serde defaults matching the documented shipped maxima
(`docs/SERVER_API_REFERENCE.md` limits table):

```rust
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ClaimLimits {
    pub max_request_bytes: usize,
    pub max_decompressed_bytes: usize,
    pub max_response_bytes: usize,
    pub max_query_rows: usize,
    pub max_request_ms: u64,
    pub max_concurrent_requests: usize,
    pub max_queue_ms: u64,
}

impl Default for ClaimLimits {
    fn default() -> Self {
        Self {
            max_request_bytes: 10 * 1024 * 1024,      // 10 MiB
            max_decompressed_bytes: 10 * 1024 * 1024, // 10 MiB
            max_response_bytes: 16 * 1024 * 1024,     // 16 MiB
            max_query_rows: 100_000,
            max_request_ms: 30_000,                   // 30 s
            max_concurrent_requests: 64,
            max_queue_ms: 1_000,                      // 1 s
        }
    }
}
```

The existing containment rule is unchanged and still applies: a claim may lower
but never raise the configured maximum, and neither may exceed the server's
hard caps. Defaults remove transcription, not enforcement.

Consider the same for `PolicyFile::minimum_auth_version` (default `0`),
`max_token_seconds` (default e.g. `3600`), and `SubjectPolicy::auth_version`
(default `0`). Keep `version`, `issuer`, `audience`, `tenant`, `subjects`, and
`keys` mandatory — they are genuine decisions.

Verify `validate_policy` (`auth.rs:616-629`) still rejects a
structurally-empty policy afterward: `version != 1`, empty issuer/audience,
empty `keys`, empty `subjects`, non-positive `max_token_seconds`.

### 6.2 Ship a key and token CLI

Add `servers/crates/timeless-authctl` producing a `timeless-authctl` binary.
`ed25519-dalek` is already a dependency of `timeless-api-common` (`auth.rs:16`);
minting needs `SigningKey` and the `rand_core` feature alongside the existing
`Verifier` / `VerifyingKey` usage.

| Command | Behavior |
|---|---|
| `timeless-authctl keygen --out <dir>` | Generates an Ed25519 keypair. Writes the private key mode 0600, prints `kid` and base64url public key. |
| `timeless-authctl policy init --signal <s> --key <pub> --out <path>` | Scaffolds a complete valid policy-v1 JSON using `ClaimLimits::default()`, one subject with all four scopes. |
| `timeless-authctl policy add-subject --policy <path> --subject <s> --scopes <list>` | Adds or updates a subject in place. |
| `timeless-authctl token mint --key <priv> --policy <path> --subject <s> --signal <s> --ttl 1h` | Emits a signed compact JWS to stdout. |
| `timeless-authctl token inspect <token>` | Decodes and prints claims without verifying, for debugging. |

The verification path must not change — `authctl` produces inputs the existing
`AuthVerifier::verify` (`auth.rs:515-613`) already accepts. Add a round-trip
test that mints with `authctl` and verifies with `AuthVerifier` so the two
cannot drift.

A five-line quickstart in `docs/SERVER_API_REFERENCE.md` showing keygen →
policy init → mint → `curl -H "Authorization: Bearer …"` is what converts auth
from theoretically-available to actually-adopted by a Rust user.

### 6.3 Follow-up: cross-implementation conformance

Out of scope here, but record it. Once `authctl` exists, `TimelessUI.TelemetryAuth`
and the Rust minter are two independent implementations of one token format,
reconciled only by the shared verifier. A conformance fixture — fixed keypair,
fixed policy, and a token set both must verify identically — is cheap
insurance against a production break.

---

## 7. Part 4 — security defects

Independent of the default flip, but **S1 and S2 must land in the same
change**, because default-open raises their severity from
"authenticated-user disclosure" to "unauthenticated, internet-facing."

### S1 — `GET /api/v1/scrape/targets` returns stored credentials in cleartext

**Confirmed.** `scrape.rs:33-41`:

```rust
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ScrapeAuth {
    #[serde(default)]
    pub bearer: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}
```

`ScrapeAuth` is embedded in `ScrapeTarget` (`scrape.rs:28`), embedded in
`ScrapeTargetReport` (`scrape.rs:50`), returned whole by the handler
(`api.rs:292-294`):

```rust
async fn scrape_targets(State(storage): State<Storage>) -> Response {
    (StatusCode::OK, Json(storage.scrape_targets().await)).into_response()
}
```

No `skip_serializing` or redaction anywhere in the crate — verified by grep.
`required_scope` maps GET to `metrics:read`, the same scope an ordinary
dashboard query uses. Any read-scoped token can exfiltrate every scrape
target's bearer token and basic-auth password.

This reaches the Elixir deployment too: the runtime token carries all four
scopes (`telemetry_auth.ex:267`). Severity is lower there — loopback-bound,
Phoenix-issued — but the defect is identical.

**Fix.** Do *not* reach for `#[serde(skip_serializing)]` on the fields.
`ScrapeTarget` and `ScrapeAuth` derive both `Serialize` and `Deserialize`;
confirm first whether that `Serialize` is load-bearing for persistence or the
`PUT` round-trip, because a blanket skip could silently drop credentials on
write.

The safe approach is a distinct response type:

```rust
#[derive(Clone, Debug, Serialize)]
pub struct ScrapeAuthView {
    pub bearer_configured: bool,
    pub username: Option<String>,   // username is not secret; password never appears
    pub password_configured: bool,
}
```

Build `ScrapeTargetView` / `ScrapeTargetReportView` around it and return those
from the GET handler. Keep the storage and `PUT` types exactly as they are.
Booleans preserve the operational signal without disclosure. Add a test
asserting no secret value appears in the GET body.

### S2 — `PUT /api/v1/scrape/targets` is an unauthenticated SSRF once auth is off

`validate_set` (`scrape.rs:354-361`) checks only that `address` is non-empty:

```rust
if target.id <= 0 || target.job_name.trim().is_empty() || target.address.trim().is_empty() {
    return Err("scrape target id, job_name, and address are required".into());
}
```

`scrape.rs:230-239` accepts `http` or `https` and formats
`{scheme}://{address}{path}` with no address validation. A caller who can reach
the port can make the server issue outbound HTTP(S) requests to any address,
with attacker-chosen credentials attached, then read the response back via a
normal metrics query.

The obvious target is cloud instance metadata — `169.254.169.254` — but any
internal service reachable from the server is in scope. Today this needs a
`metrics:write` token; after the flip it needs nothing.

**Fix, applied regardless of auth mode:** resolve the host and validate the
resulting address.

- Deny `169.254.0.0/16` and `fe80::/10` (link-local, metadata).
- Deny `0.0.0.0` / `::`.
- Optionally deny RFC1918 and unique-local behind an opt-out
  (`TIMELESS_SCRAPE_ALLOW_PRIVATE=1`) — scraping private addresses is a
  legitimate and common case, so this should default to allowed.
- Guard DNS rebinding by validating the address actually connected to rather
  than only the pre-resolution hostname, or by pinning the resolved address.

Also review the interval floor alongside the existing `MAX_TARGETS = 10_000`
(`scrape.rs:14`) so an open server cannot be turned into a request amplifier.

### S3 — `POST /api/v1/backup` writes attacker-chosen paths

`BackupRequest` (`timeless-api-common/src/lib.rs:28-32`) carries a
caller-supplied `destination: PathBuf`, passed to `create_verified_backup`
(`lib.rs:83-108`).

Existing validation is decent — absolute path required, must name a file,
**must not already exist** (no overwrite), parent must exist and canonicalize.
This is not an arbitrary-overwrite primitive.

What remains: an unauthenticated caller can write a full copy of the telemetry
database anywhere the server process can write, repeatedly, under
attacker-chosen filenames. Disk exhaustion, plus staging data somewhere more
readable than the data directory.

**Fix.** Constrain destinations to a configured backup root
(`TIMELESS_BACKUP_DIR`), defaulting to a subdirectory of the data directory,
rejecting anything that canonicalizes outside it. Better than gating on auth,
because it is correct for authenticated callers too.

### S4 — sensitive endpoints need an independent opt-in gate

Following VictoriaMetrics' `-deleteAuthKey` / `-snapshotAuthKey` precedent, add
`TIMELESS_ADMIN_KEY`. When set, these routes additionally require it (header or
query parameter), independent of `TIMELESS_AUTH_MODE`:

- `PUT /api/v1/scrape/targets`
- `GET /api/v1/scrape/targets`
- `POST /api/v1/backup`
- `POST /api/v1/flush`, `/api/v1/optimize`

**Unset means open**, consistent with "wide open by default" and with VM. This
serves operators who want ingest and query open but administration closed,
without standing up policy and token machinery.

This is defence in depth, not the fix for S1–S3. Those must be fixed on their
merits; an unset admin key must not leave a credential-disclosure or SSRF
primitive exposed.

### S5 — library/binary divergence

Covered by Part 1. Recorded here so it appears in the security summary.

---

## 8. Part 5 — Elixir side

**No code changes required.** Verification only:

1. Confirm `config/runtime.exs:104`, `:118`, `:132` still set
   `auth_mode: :required`. The stack should keep full auth — its provisioning
   works and there is no reason to weaken it.
2. Confirm `config/dev.exs:42`, `:52`, `:62` unchanged.
3. `timeless_ui/lib/timeless_ui/telemetry_data_plane/process.ex:283` defaults
   `auth_mode` to `:required` for callers that omit it. **Recommend flipping to
   `:disabled`** for consistency with Rust, since the stack passes it
   explicitly and is unaffected. Matters for anyone embedding `timeless_ui`
   directly. Decide deliberately; either is defensible, but Rust and Elixir
   should not silently disagree.
4. `timeless_ui/lib/timeless_ui/application.ex:73-96` keys child startup off
   `auth_mode == :required`. If step 3 changes, re-read this — a plane
   defaulting to `:disabled` will no longer start the `Policy` GenServer or
   inject a token provider. Correct, but confirm by test.
5. Re-run the stack's data-plane integration tests. The contract is unchanged,
   so they should pass untouched. If they do not, the stack was relying on the
   default rather than its explicit config — worth knowing.
6. Apply S1 awareness to any stack UI rendering scrape targets. If a LiveView
   displays credential fields today, the new view type changes that response
   shape.

---

## 9. Documentation — complete inventory

Every document below encodes the old design and must be updated together. The
sweep has two halves: correcting the **stated default**, and correcting the
**framing** so the primary audience is served first (§1).

### 9.1 Automated gate — read before editing

`tools/query-harness/src/contracts.rs` enforces **bidirectional** consistency
between source and `docs/SERVER_API_REFERENCE.md`. Both directions fail the
build:

- `validate_public_server_environment` (`contracts.rs:659-689`) scans
  `servers/crates/timeless-api-common/src/auth.rs`, `.../src/lib.rs`, and the
  three `main.rs` files for `"TIMELESS_*"` string literals
  (`source_runtime_environment`, `contracts.rs:633-657`). Every variable found
  must have a row in the `public-server-environment` marked region
  (`docs/SERVER_API_REFERENCE.md:240-273`), **and** every documented row must
  be read by production source.
- The route inventory in the `public-server-routes` marked region
  (`docs/SERVER_API_REFERENCE.md:51-100`) must match registered axum routes
  (`contracts.rs:540`, `:592`).

Practical consequences for this plan:

- Adding `TIMELESS_ADMIN_KEY` (S4), `TIMELESS_BACKUP_DIR` (S3), or
  `TIMELESS_SCRAPE_ALLOW_PRIVATE` (S2) **requires** a matching row in the
  marked environment region or the gate fails.
- Renaming `required_from_env` → `from_env` does not change the variable set,
  so the env gate is unaffected by Part 1 alone.
- Adding routes for `authctl` is not applicable — it is a separate binary with
  no HTTP surface.

Run the gate before opening the PR.

### 9.2 `timeless-libsql` — normative documents, must change

| File / line | Change |
|---|---|
| `README.md:311-315` | "Release binaries require authentication unless `TIMELESS_AUTH_MODE=disabled` is explicitly set." **Now false.** Rewrite: binaries start open; set `TIMELESS_AUTH_MODE=required` to enable. Keep the `TIMELESS_ALLOW_NON_LOOPBACK` sentence — unchanged. |
| `docs/SERVER_API_REFERENCE.md:41-44` | The "binaries require auth / library defaults are test-only" paragraph. Replace: auth is opt-in at every level, library and binary agree. Delete the "not the release-binary default" caveat entirely. |
| `docs/SERVER_API_REFERENCE.md:53-99` (route table) | Add a note that the "Required scope" column applies **only when auth is enabled**. Otherwise the table reads as though scopes are always enforced. |
| `docs/SERVER_API_REFERENCE.md:245` | Env row: `TIMELESS_AUTH_MODE` default `required` → `disabled`; description → "set `required` to enable token verification." Inside the marked region — see §9.1. |
| `docs/SERVER_API_REFERENCE.md:246` | `TIMELESS_AUTH_POLICY_FILE` → "required when `TIMELESS_AUTH_MODE=required`." |
| `docs/SERVER_API_REFERENCE.md:240-273` | Add rows for any new variable from S2/S3/S4. Gate-enforced. |
| `docs/SERVER_API_REFERENCE.md:280-346` ("Authentication and admission") | Reframe as opt-in. Add the `authctl` quickstart (§6.2). Update the exempt-path list to include `/ready` and `/health` if Part 2 lands. The error-code inventory at `:323-340` stays accurate and needs no change. |
| `docs/COMPATIBILITY.md` | Record the behavior change on the compatibility line: a binary upgraded across this boundary stops requiring auth unless `TIMELESS_AUTH_MODE=required` is set. |
| `docs/UPGRADE.md:90` | "call its authenticated flush route" — now conditional. Reword to "flush route (authenticated only if you enabled auth)". |
| `docs/UPGRADE.md:150` | "Require `/live`, authenticated `/ready`, and signal stats to report…" — `/ready` is no longer authenticated if Part 2 lands. Update the checklist. |
| `docs/UPGRADE.md` (new section) | **The most important doc change.** An explicit upgrade warning with the one-line remediation. Anyone upgrading without reading it silently loses authentication. See §10. |
| `servers/README.md:9` | "The complete public route, configuration, authentication, lifecycle…" — still accurate, but confirm the pointer text does not imply auth is mandatory. |
| `servers/README.md:69` | "Authentication and production limits are added in…" — reword to reflect opt-in. |
| `servers/README.md` (defaults section) | Add auth posture next to the existing TCP defaults table: "Auth: disabled unless `TIMELESS_AUTH_MODE=required`." |
| `servers/crates/timeless-metrics-api/README.md:85` | Drop `TIMELESS_AUTH_MODE=disabled` from the run snippet — now redundant. |
| `servers/crates/timeless-metrics-api/README.md:93` | "A production launch supplies `TIMELESS_AUTH_POLICY_FILE` and…" — reword as optional hardening, not the expected path. |
| `servers/crates/timeless-logs-api/README.md:78` | Same as metrics `:85`. |
| `servers/crates/timeless-logs-api/README.md:86` | Same as metrics `:93`. |
| `servers/crates/timeless-traces-api/README.md:25` | Drop `TIMELESS_AUTH_MODE=disabled` from the `cargo run` line. |
| `servers/crates/timeless-traces-api/README.md:29-30` | Delete "only for an isolated local benchmark" — that caveat inverts after this change. Reword the production sentence as optional. |
| `docs/EMBEDDED_RUST.md` | No correction needed — it is already accurate. **Add** one sentence stating the embedded path has no HTTP surface and therefore no authentication, so a reader does not go looking. |
| `docs/GUIDE.md:761` | Lists "transactions, authentication, limits, maintenance, shutdown" as covered topics. Verify the pointer still resolves correctly after the API reference is reorganized. |
| `CHANGELOG.md` `[Unreleased]` | See §10. |

### 9.3 `timeless-libsql` — verify, likely no change

| File | Why it is on the list |
|---|---|
| `docs/RELEASING.md` | Pre-tag checklist. Confirm nothing asserts an auth default. Add the contract gate run if not already listed. |
| `TESTING.md` | Confirm the gate ordering still holds and add the new tests from §12. |
| `docs/DEFERRED_WORK.md` | If auth tooling was previously deferred here, close the entry. |
| `docs/SQLD.md`, `docs/QUERY_*.md`, `docs/*_FEATURE_MATRIX.md` | Auth mentions are incidental (mostly the word "authoritative" or query-surface context). Confirm by reading, change nothing unless a default is asserted. |
| `METRICS_API_POC_PLAN.md`, `FEATURE_PLAN.md`, `PLAN.md`, `REVIEW_FIX_PLAN.md`, `STANDALONE_QUERY_PLAN.md` | Planning documents. If any states auth-required as a goal, add a note pointing here rather than rewriting history. |

### 9.4 `timeless-libsql` — evidence documents, **do not rewrite**

`CHANGELOG.md:1-5` states that development-session documents are evidence, not
release history. These recorded what was true when written and should stay
that way:

- `docs/2026-08-02_rust_telemetry_data_plane_session1.md` … `session8.md`
  (notably `session4.md`, 21 auth references, and `session5.md:127`)
- `docs/2026-08-02_rust_telemetry_data_plane_release_plan.md`
- `docs/2026-08-04_query_surface_implementation_plan.md`
- `docs/2026-08-07_*`, `docs/2026-08-08_*`
- `docs/QUERY_RELEASE_REPORT.md`, `docs/QUERY_EVIDENCE.md`,
  `docs/QUERY_STORAGE_FINDINGS.md`, `docs/evidence/`

**Exception:** if any of these is linked from a normative document as current
guidance, either add a dated banner ("describes the pre-*N* auth default; see
`AUTH_OPT_IN_PLAN.md`") or repoint the link. Do not silently leave a reader
following stale instructions.

### 9.5 `timeless_stack` — normative documents, must change

| File / line | Change |
|---|---|
| `docs/telemetry_data_plane_compatibility.md:139` | "Authorization is required by default." **Now false for the binaries.** Rewrite: the servers default to open; the stack enables auth explicitly via `auth_mode: :required`. The limits table below it stays accurate. |
| `docs/telemetry_data_plane_compatibility.md:15` | "Phoenix … owns users, sessions, token issuance, authorization policy" — still true for the stack, but add that this is one topology, not the only supported one. |
| `docs/telemetry_data_plane_compatibility.md:136` | "authenticated telemetry administration API" — still accurate for the stack. Verify only. |
| `docs/telemetry_data_plane_operations.md:69-72` | "`/live` is deliberately unauthenticated … require a Phoenix-issued credential." Update if Part 2 lands — `/ready` and `/health` join `/live`. |
| `docs/telemetry_data_plane_operations.md:133,136,147,186` | "authenticated" readiness and query steps. Still correct for the stack (which enables auth), but confirm the runbook does not imply the servers mandate it. |
| `docs/telemetry_data_plane_release_handoff.md:38-39` | "Phoenix remains the control plane. It owns users, sessions, token issuance, authorization policy…" — add that the servers no longer require it. |
| `docs/telemetry_data_plane_release_handoff.md:97,117` | "wait for authenticated combined readiness", "Alert when authenticated readiness is false" — stack-specific and still correct. Verify only. |

### 9.6 `timeless_stack` — evidence documents, do not rewrite

- `docs/2026-08-02_rust_data_plane_boundary_corrections.md`
- `docs/2026-08-02_rust_data_plane_boundary_c0_baseline.md`
- `docs/2026-08-02_rust_data_plane_c5_gate.md`

Same exception as §9.4 applies.

### 9.7 Test harnesses — optional cleanup

These set `TIMELESS_AUTH_MODE=disabled` and become redundant. Leaving them is
harmless; removing them is tidy:

- `tools/query-harness/src/production.rs:328`
- `tools/query-harness/src/evidence.rs:1185`, `:1424`
- `tools/query-harness/src/trace_baseline.rs:171`
- `servers/crates/timeless-traces-api/tests/storage_contract.rs:503`

### 9.8 Framing pass

Beyond individual line corrections, `docs/SERVER_API_REFERENCE.md` and
`servers/README.md` are written from the Phoenix-control-plane perspective —
the frame that produced this problem. After the factual corrections, reread
both as a Rust or SQLite user with no Elixir in their stack:

- Does the quickstart work with no environment variables? It must.
- Is the first mention of auth an *option*, not a prerequisite?
- Is `timeless_stack` described as one deployment topology rather than the
  assumed context?
- Does `docs/EMBEDDED_RUST.md` get a prominent link from `README.md`, given it
  serves the largest audience?

---

## 10. Release notes

Auth-by-default entered at `CHANGELOG.md:196-199` in the `0.4.0` "Added" block.
Nothing since has revisited it.

This is a **behavior change** and must be unmissable. Draft entry:

```markdown
## [Unreleased]

### Changed

- The three signal server binaries now start with authentication **disabled**
  by default, matching the library `Config::default()` and every comparable
  telemetry server. Set `TIMELESS_AUTH_MODE=required` with
  `TIMELESS_AUTH_POLICY_FILE` to enable token verification. Previously an
  unset `TIMELESS_AUTH_MODE` required a policy file and the binary exited
  with code 2 without one.
  **Operators who relied on the previous default must now set
  `TIMELESS_AUTH_MODE=required` explicitly.** Deployments that already set it
  explicitly — including every `timeless_stack` deployment — are unaffected.
- `/ready` and `/health` no longer require a token, so container and load
  balancer probes work without minted credentials. `/live` was already exempt.

### Added

- `timeless-authctl`: Ed25519 keygen, policy scaffolding, and token minting,
  so enabling auth no longer requires implementing a JWS signer or running
  the Elixir control plane.
- `ClaimLimits` now has defaults, so a policy file need only state the limits
  it wants to lower.

### Fixed

- `GET /api/v1/scrape/targets` no longer returns stored scrape bearer tokens
  or basic-auth passwords. The response reports whether credentials are
  configured without disclosing them.
- Scrape targets can no longer be pointed at link-local addresses, closing a
  server-side request forgery path to cloud instance metadata.
- Backup destinations are constrained to a configured backup root.
```

`docs/UPGRADE.md` needs the same warning in prose with the one-line remediation
(`TIMELESS_AUTH_MODE=required`) prominent. Anyone who upgrades without reading
it silently loses authentication — that is the one genuine risk in this plan
and the documentation has to carry it.

---

## 11. Suggested commit order

Each step should build and pass tests independently.

1. **S1** — scrape credential redaction. Independent, highest severity. Merge
   first so nothing blocks it.
2. **S2** — scrape SSRF address validation.
3. **S3** — backup destination root constraint.
4. **Part 1** — the default flip, plus §9.2 normative doc sweep and CHANGELOG.
   Only after 1–3, so default-open never exists in a tree that still has the
   disclosure and SSRF primitives.
5. **Part 2** — health endpoint exemption.
6. **§6.1** — `ClaimLimits` defaults.
7. **§6.2** — `timeless-authctl`.
8. **S4** — `TIMELESS_ADMIN_KEY`.
9. **§9.5** — stack documentation sweep.
10. **Part 5** — Elixir default alignment, if you take it.

Steps 1–4 are the shippable minimum: the server starts open, and open is not
dangerous. Everything after makes opt-in pleasant rather than merely possible.

---

## 12. Test plan

New tests, matching the existing style at `auth.rs:727-735`:

- **Binary with no env at all binds its listener and serves an unauthenticated
  ingest and query.** This is the regression test for the whole document.
- `TIMELESS_AUTH_MODE=required` without `TIMELESS_AUTH_POLICY_FILE` still
  exits 2.
- `TIMELESS_AUTH_MODE=required` with a valid policy still returns 401
  `missing_credentials` on a tokenless request to every non-exempt route, and
  200 with a valid token. Existing coverage should carry over unchanged.
- `TIMELESS_AUTH_MODE=garbage` still exits 2.
- `/live`, `/ready`, `/health` return 200 with auth enabled and no token.
- A signal stats route still returns 401 with auth enabled and no token —
  proves the exemption did not widen.
- GET scrape targets with a configured bearer and password returns neither
  value anywhere in the body.
- PUT a scrape target at `169.254.169.254` is rejected.
- Backup to a path outside the backup root is rejected.
- `authctl` mint → `AuthVerifier::verify` round trip.
- A policy file omitting all optional limit fields loads and yields
  `ClaimLimits::default()`.

Run the full gate per `TESTING.md`, including the contract gate (§9.1) and:

```bash
cargo clippy --workspace --all-targets --manifest-path servers/Cargo.toml -- -D warnings
```

Then run the stack's data-plane integration suite to confirm §8's zero-impact
claim.

---

## 13. Summary

The servers are well engineered for the job they were designed for — being
children of a Phoenix control plane that provisions their PKI automatically.
The verifier is careful work and none of it needs to be discarded.

What needs to change is the assumption underneath it. Timeless is a SQLite
extension and a Rust library first, and a Phoenix-managed service last. The
default posture, the tooling, and the documentation all currently assume the
reverse, and the result is a binary that will not start for the people most
likely to try it.

After this plan: the binary starts open like VictoriaMetrics, the extension and
library and binary all agree, opting into auth takes four `authctl` commands
with no Elixir involved, health probes work, the credential-disclosure and SSRF
issues are closed on their own merits, and every document describes the product
the primary audience is actually holding.
