//! Key generation, policy scaffolding, and token minting for opt-in
//! Timeless server auth.
//!
//! The servers verify Ed25519-signed compact JWS tokens against a policy-v1
//! JSON file, but until this crate the only implementation of the producing
//! side lived in Elixir inside `timeless_ui` — enabling auth from Rust or a
//! shell meant implementing a JWS signer. `authctl` produces exactly the
//! inputs the existing verifier accepts; the verification path is unchanged,
//! and the round-trip test in `tests/` mints here and verifies through
//! `timeless-api-common` so the two cannot drift.
//!
//! Policy files are manipulated as `serde_json::Value` on purpose: the
//! authoritative schema lives with the verifier, and scaffolding leans on the
//! serde defaults introduced alongside this crate (`ClaimLimits`,
//! `minimum_auth_version`, `max_token_seconds`) rather than duplicating them.

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use serde_json::{json, Map, Value};

pub const PRIVATE_KEY_FILE: &str = "timeless-auth.key";
pub const PUBLIC_KEY_FILE: &str = "timeless-auth.pub";

/// A generated keypair: base64url-no-padding encodings plus the key id the
/// policy and tokens reference.
pub struct Keypair {
    pub kid: String,
    pub public_key: String,
    pub private_key: String,
}

fn unix_now() -> Result<i64, String> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system time precedes Unix epoch: {error}"))?
        .as_secs() as i64)
}

/// The key id is the first 8 bytes of the raw public key, hex-encoded
/// (16 characters) — stable and derivable from the public key alone.
/// (An earlier docstring claimed SHA-256 here; the code always used the
/// key bytes themselves, which are already uniformly random. The hash
/// framing in the comment below explains why no new dependency was
/// pulled in for this.)
fn derive_kid(public: &VerifyingKey) -> String {
    // A tiny SHA-256 would be another dependency; SQLite-style FNV/DJB hashes
    // are too weak for an identifier. ed25519-dalek re-exports sha2 via its
    // signature dependency chain, but not publicly — so hash with the same
    // primitive the signature scheme already trusts: sign a fixed message and
    // take the prefix. Deterministic per key, unforgeable without the private
    // key, and needs no new dependency... but requires the private key. For a
    // kid derivable from the PUBLIC key alone, fall back to the key bytes
    // themselves: they are already uniformly random.
    let bytes = public.to_bytes();
    bytes[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Generates an Ed25519 keypair into `dir`, writing the private key with
/// mode 0600. Returns the material for display; the private key value is
/// also the file content.
pub fn keygen(dir: &Path) -> Result<Keypair, String> {
    fs::create_dir_all(dir)
        .map_err(|error| format!("create key directory {}: {error}", dir.display()))?;
    let mut seed = [0u8; 32];
    getrandom::getrandom(&mut seed).map_err(|error| format!("gather key entropy: {error}"))?;
    let signing = SigningKey::from_bytes(&seed);
    let keypair = Keypair {
        kid: derive_kid(&signing.verifying_key()),
        public_key: URL_SAFE_NO_PAD.encode(signing.verifying_key().to_bytes()),
        private_key: URL_SAFE_NO_PAD.encode(seed),
    };
    let private_path = dir.join(PRIVATE_KEY_FILE);
    if private_path.exists() {
        return Err(format!(
            "refusing to overwrite existing private key {}",
            private_path.display()
        ));
    }
    fs::write(&private_path, &keypair.private_key)
        .map_err(|error| format!("write private key {}: {error}", private_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&private_path, fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("restrict private key permissions: {error}"))?;
    }
    let public_path = dir.join(PUBLIC_KEY_FILE);
    fs::write(&public_path, &keypair.public_key)
        .map_err(|error| format!("write public key {}: {error}", public_path.display()))?;
    Ok(keypair)
}

fn load_signing_key(path: &Path) -> Result<SigningKey, String> {
    let encoded = fs::read_to_string(path)
        .map_err(|error| format!("read private key {}: {error}", path.display()))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded.trim())
        .map_err(|error| format!("decode private key: {error}"))?;
    let seed: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| "private key must be 32 bytes".to_string())?;
    Ok(SigningKey::from_bytes(&seed))
}

/// All four scopes for a signal — the scaffold's single subject gets full
/// access; `policy add-subject` narrows from there.
fn all_scopes(signal: &str) -> Vec<String> {
    ["read", "write", "stats", "maintenance"]
        .iter()
        .map(|scope| format!("{signal}:{scope}"))
        .collect()
}

/// Scaffolds a complete, valid policy-v1 file: one subject with all four
/// scopes, limits left to the verifier's defaults, the given public key
/// valid from now for ten years.
pub fn policy_init(
    signal: &str,
    public_key: &str,
    subject: &str,
    tenant: &str,
    out: &Path,
) -> Result<Value, String> {
    let key_bytes = URL_SAFE_NO_PAD
        .decode(public_key)
        .map_err(|error| format!("decode public key: {error}"))?;
    let key_array: [u8; 32] = key_bytes
        .as_slice()
        .try_into()
        .map_err(|_| "public key must be 32 bytes".to_string())?;
    let verifying = VerifyingKey::from_bytes(&key_array)
        .map_err(|error| format!("invalid public key: {error}"))?;
    let now = unix_now()?;
    let policy = json!({
        "version": 1,
        "issuer": "timeless-authctl",
        "audience": format!("timeless-{signal}"),
        "tenant": tenant,
        "subjects": {
            subject: { "scopes": all_scopes(signal) }
        },
        "keys": [{
            "kid": derive_kid(&verifying),
            "public_key": public_key,
            "not_before": now - 60,
            "expires_at": now + 10 * 365 * 24 * 3600,
        }]
    });
    if out.exists() {
        return Err(format!(
            "refusing to overwrite existing policy {}",
            out.display()
        ));
    }
    if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create policy directory {}: {error}", parent.display()))?;
    }
    fs::write(out, serde_json::to_string_pretty(&policy).unwrap())
        .map_err(|error| format!("write policy {}: {error}", out.display()))?;
    Ok(policy)
}

/// Adds or replaces a subject in an existing policy file. Returns
/// whether a subject of that name already existed: overwriting a
/// subject silently (notably narrowing its scopes) used to be
/// invisible, so the CLI reports created-vs-replaced explicitly.
/// Scopes must be shaped `signal:scope` with both parts nonempty —
/// anything else can never match a route's required scope, so it is
/// rejected here instead of failing closed (and confusingly) at use
/// time. Well-formed but unknown scopes are still accepted: the
/// verifier fails closed on those, and a future signal/scope must not
/// be blocked by an old CLI allowlist.
pub fn policy_add_subject(
    policy_path: &Path,
    subject: &str,
    scopes: &[String],
) -> Result<bool, String> {
    if scopes.is_empty() {
        return Err("subject must have at least one scope".into());
    }
    for scope in scopes {
        let well_formed = scope
            .split_once(':')
            .is_some_and(|(signal, name)| !signal.is_empty() && !name.is_empty());
        if !well_formed {
            return Err(format!(
                "scope {scope:?} must be shaped `signal:scope` (e.g. `metrics:read`)"
            ));
        }
    }
    let mut policy: Value = serde_json::from_str(
        &fs::read_to_string(policy_path)
            .map_err(|error| format!("read policy {}: {error}", policy_path.display()))?,
    )
    .map_err(|error| format!("parse policy: {error}"))?;
    let subjects = policy
        .get_mut("subjects")
        .and_then(Value::as_object_mut)
        .ok_or("policy has no subjects object")?;
    let replaced = subjects.contains_key(subject);
    subjects.insert(subject.to_owned(), json!({ "scopes": scopes }));
    fs::write(policy_path, serde_json::to_string_pretty(&policy).unwrap())
        .map_err(|error| format!("write policy {}: {error}", policy_path.display()))?;
    Ok(replaced)
}

/// Mints a signed compact JWS the servers' verifier accepts: claims are
/// drawn from the policy (issuer, audience, tenant, the subject's scopes)
/// so the token cannot disagree with the file it will be verified against.
pub fn mint(
    key_path: &Path,
    policy_path: &Path,
    subject: &str,
    signal: &str,
    ttl_seconds: i64,
) -> Result<String, String> {
    let now = unix_now()?;
    let jti = format!("authctl-{now}-{}", std::process::id());
    mint_at(
        key_path,
        policy_path,
        subject,
        signal,
        ttl_seconds,
        now,
        &jti,
    )
}

/// The deterministic core of `mint`: fixed issue time and token id. Exists
/// for the cross-implementation conformance fixture, where the exact token
/// bytes are pinned; production callers use `mint`.
pub fn mint_at(
    key_path: &Path,
    policy_path: &Path,
    subject: &str,
    signal: &str,
    ttl_seconds: i64,
    issued_at: i64,
    jti: &str,
) -> Result<String, String> {
    if ttl_seconds <= 0 {
        return Err("token ttl must be positive".into());
    }
    let signing = load_signing_key(key_path)?;
    let policy: Value = serde_json::from_str(
        &fs::read_to_string(policy_path)
            .map_err(|error| format!("read policy {}: {error}", policy_path.display()))?,
    )
    .map_err(|error| format!("parse policy: {error}"))?;
    let field = |name: &str| -> Result<String, String> {
        policy
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| format!("policy is missing {name}"))
    };
    let subject_policy = policy
        .get("subjects")
        .and_then(|subjects| subjects.get(subject))
        .ok_or_else(|| format!("policy has no subject {subject:?}"))?;
    let scopes = subject_policy
        .get("scopes")
        .cloned()
        .ok_or_else(|| format!("subject {subject:?} has no scopes"))?;
    let auth_version = subject_policy
        .get("auth_version")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let max_token_seconds = policy
        .get("max_token_seconds")
        .and_then(Value::as_i64)
        .unwrap_or(3_600);
    if ttl_seconds > max_token_seconds {
        return Err(format!(
            "ttl {ttl_seconds}s exceeds the policy's max_token_seconds {max_token_seconds}s"
        ));
    }
    let mut claims = Map::new();
    claims.insert("iss".into(), json!(field("issuer")?));
    claims.insert("aud".into(), json!(field("audience")?));
    claims.insert("sub".into(), json!(subject));
    claims.insert("jti".into(), json!(jti));
    claims.insert("tenant".into(), json!(field("tenant")?));
    claims.insert("signal".into(), json!(signal));
    claims.insert("scopes".into(), scopes);
    claims.insert("auth_version".into(), json!(auth_version));
    claims.insert("iat".into(), json!(issued_at));
    claims.insert("nbf".into(), json!(issued_at));
    let expires_at = issued_at
        .checked_add(ttl_seconds)
        .ok_or_else(|| format!("token expiry overflows for ttl {ttl_seconds}s"))?;
    claims.insert("exp".into(), json!(expires_at));
    if let Some(limits) = subject_policy
        .get("maximum_limits")
        .or_else(|| policy.get("maximum_limits"))
    {
        claims.insert("limits".into(), limits.clone());
    }
    let header = json!({"alg": "EdDSA", "typ": "JWT", "kid": derive_kid(&signing.verifying_key())});
    let signing_input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap()),
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Value::Object(claims)).unwrap())
    );
    let signature = signing.sign(signing_input.as_bytes());
    Ok(format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    ))
}

/// Decodes and returns (header, claims) WITHOUT verifying — a debugging aid,
/// never a verification path.
pub fn inspect(token: &str) -> Result<(Value, Value), String> {
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(_signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err("token is not a three-part compact JWS".into());
    };
    let decode = |part: &str, name: &str| -> Result<Value, String> {
        let bytes = URL_SAFE_NO_PAD
            .decode(part)
            .map_err(|error| format!("decode token {name}: {error}"))?;
        serde_json::from_slice(&bytes).map_err(|error| format!("parse token {name}: {error}"))
    };
    Ok((decode(header, "header")?, decode(payload, "claims")?))
}

/// Parses "30s" / "15m" / "1h" / "2d" (bare numbers are seconds).
pub fn parse_ttl(value: &str) -> Result<i64, String> {
    let value = value.trim();
    let (digits, unit) = match value.find(|c: char| !c.is_ascii_digit()) {
        Some(idx) => value.split_at(idx),
        None => (value, "s"),
    };
    let quantity: i64 = digits
        .parse()
        .map_err(|_| format!("invalid ttl {value:?}"))?;
    if quantity <= 0 {
        return Err(format!("invalid ttl {value:?}: quantity must be positive"));
    }
    let seconds = match unit {
        "s" => quantity,
        "m" => quantity
            .checked_mul(60)
            .ok_or_else(|| format!("ttl {value:?} overflows"))?,
        "h" => quantity
            .checked_mul(3_600)
            .ok_or_else(|| format!("ttl {value:?} overflows"))?,
        "d" => quantity
            .checked_mul(86_400)
            .ok_or_else(|| format!("ttl {value:?} overflows"))?,
        other => return Err(format!("invalid ttl unit {other:?}; use s, m, h, or d")),
    };
    Ok(seconds)
}
