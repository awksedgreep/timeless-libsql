use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Notify;

const MAX_POLICY_BYTES: u64 = 1_048_576;
const MAX_TOKEN_BYTES: usize = 32_768;
const CLOCK_SKEW_SECONDS: i64 = 30;

#[derive(Clone)]
pub struct AuthConfig {
    verifier: Option<Arc<AuthVerifier>>,
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.verifier.as_ref() {
            None => formatter.write_str("AuthConfig::Disabled"),
            Some(verifier) => formatter
                .debug_struct("AuthConfig::Enforced")
                .field("signal", &verifier.signal)
                .field("tenant", &verifier.tenant)
                .field("policy_path", &verifier.policy_path)
                .finish(),
        }
    }
}

impl AuthConfig {
    pub fn disabled() -> Self {
        Self { verifier: None }
    }

    pub fn enforced(
        signal: impl Into<String>,
        tenant: impl Into<String>,
        policy_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            verifier: Some(Arc::new(AuthVerifier {
                signal: signal.into(),
                tenant: tenant.into(),
                policy_path: policy_path.into(),
                cache: Mutex::new(None),
                admission: Mutex::new(HashMap::new()),
                admission_notify: Notify::new(),
            })),
        }
    }

    pub fn required_from_env(signal: &str) -> Result<Self, String> {
        match std::env::var("TIMELESS_AUTH_MODE") {
            Ok(mode) if mode == "disabled" => return Ok(Self::disabled()),
            Ok(mode) if mode != "required" => {
                return Err(format!(
                    "TIMELESS_AUTH_MODE must be required or disabled, got {mode:?}"
                ))
            }
            Ok(_) | Err(std::env::VarError::NotPresent) => {}
            Err(error) => return Err(format!("read TIMELESS_AUTH_MODE: {error}")),
        }
        let policy = std::env::var("TIMELESS_AUTH_POLICY_FILE").map_err(|_| {
            "TIMELESS_AUTH_POLICY_FILE is required unless TIMELESS_AUTH_MODE=disabled".to_owned()
        })?;
        let tenant = std::env::var("TIMELESS_TENANT").unwrap_or_else(|_| "default".to_owned());
        let config = Self::enforced(signal, tenant, policy);
        config.preflight()?;
        Ok(config)
    }

    pub fn preflight(&self) -> Result<(), String> {
        match self.verifier.as_ref() {
            None => Ok(()),
            Some(verifier) => verifier
                .policy()
                .map(|_| ())
                .map_err(|error| format!("preflight data-plane authorization: {}", error.code)),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ClaimLimits {
    pub max_request_bytes: usize,
    pub max_decompressed_bytes: usize,
    pub max_response_bytes: usize,
    pub max_query_rows: usize,
    pub max_request_ms: u64,
    pub max_concurrent_requests: usize,
    pub max_queue_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct VerifiedClaims {
    pub sub: String,
    pub jti: String,
    pub tenant: String,
    pub signal: String,
    pub scopes: Vec<String>,
    pub auth_version: u64,
    pub limits: ClaimLimits,
}

#[derive(Clone, Debug, Deserialize)]
struct TokenClaims {
    iss: String,
    aud: String,
    sub: String,
    jti: String,
    tenant: String,
    signal: String,
    scopes: Vec<String>,
    auth_version: u64,
    iat: i64,
    nbf: i64,
    exp: i64,
    limits: ClaimLimits,
}

#[derive(Debug, Deserialize)]
struct TokenHeader {
    alg: String,
    kid: String,
    #[serde(default)]
    typ: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct PolicyFile {
    version: u64,
    issuer: String,
    audience: String,
    tenant: String,
    minimum_auth_version: u64,
    max_token_seconds: i64,
    maximum_limits: ClaimLimits,
    subjects: HashMap<String, SubjectPolicy>,
    keys: Vec<PolicyKey>,
    #[serde(default)]
    revoked_jtis: HashSet<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct SubjectPolicy {
    auth_version: u64,
    scopes: HashSet<String>,
    maximum_limits: ClaimLimits,
    #[serde(default = "default_enabled")]
    enabled: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct PolicyKey {
    kid: String,
    public_key: String,
    not_before: i64,
    expires_at: i64,
    #[serde(default)]
    revoked: bool,
}

#[derive(Clone)]
struct CachedPolicy {
    len: u64,
    modified: Option<SystemTime>,
    file_identity: FileIdentity,
    policy: Arc<PolicyFile>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
fn file_identity(metadata: &fs::Metadata) -> FileIdentity {
    use std::os::unix::fs::MetadataExt;

    FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(not(unix))]
fn file_identity(_metadata: &fs::Metadata) -> FileIdentity {
    FileIdentity {
        device: 0,
        inode: 0,
    }
}

struct AuthVerifier {
    signal: String,
    tenant: String,
    policy_path: PathBuf,
    cache: Mutex<Option<CachedPolicy>>,
    admission: Mutex<HashMap<String, usize>>,
    admission_notify: Notify,
}

struct AdmissionGuard {
    verifier: Arc<AuthVerifier>,
    subject: String,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        let mut admission = self
            .verifier
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(active) = admission.get_mut(&self.subject) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                admission.remove(&self.subject);
            }
        }
        drop(admission);
        self.verifier.admission_notify.notify_waiters();
    }
}

#[derive(Clone, Copy, Debug)]
struct AuthError {
    status: StatusCode,
    code: &'static str,
}

impl AuthError {
    const fn unauthorized(code: &'static str) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code,
        }
    }

    const fn forbidden(code: &'static str) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code,
        }
    }

    const fn limit(status: StatusCode, code: &'static str) -> Self {
        Self { status, code }
    }

    fn response(self) -> Response {
        (
            self.status,
            Json(json!({"error": "data_plane_authorization", "reason": self.code})),
        )
            .into_response()
    }
}

pub fn protect_router(router: Router, config: AuthConfig) -> Router {
    if config.verifier.is_none() {
        router
    } else {
        router.layer(middleware::from_fn_with_state(config, authorize))
    }
}

async fn authorize(State(config): State<AuthConfig>, request: Request, next: Next) -> Response {
    let Some(verifier) = config.verifier else {
        return next.run(request).await;
    };
    if request.uri().path() == "/live" {
        return next.run(request).await;
    }
    let scope = required_scope(&verifier.signal, request.method(), request.uri().path());
    let token = match bearer_token(request.headers()) {
        Ok(token) => token,
        Err(error) => return error.response(),
    };
    let claims = match verifier.verify(token, &scope) {
        Ok(claims) => claims,
        Err(error) => return error.response(),
    };
    if let Err(error) = enforce_query_limit(request.uri().query(), claims.limits.max_query_rows) {
        return error.response();
    }

    let _admission = match verifier.admit(&claims).await {
        Ok(admission) => admission,
        Err(error) => return error.response(),
    };

    let (mut parts, body) = request.into_parts();
    let body = match to_bytes(body, claims.limits.max_request_bytes).await {
        Ok(body) => body,
        Err(_) => {
            return AuthError::limit(StatusCode::PAYLOAD_TOO_LARGE, "request_bytes_exceeded")
                .response()
        }
    };
    if let Err(error) = enforce_query_limit_from_body(&body, claims.limits.max_query_rows) {
        return error.response();
    }
    parts.extensions.insert(claims.clone());
    let request = Request::from_parts(parts, Body::from(body));
    // A timed-out write may already be queued and can still become durable.
    // Never manufacture an ambiguous write result by cancelling its response
    // future; storage owns its bounded queue and completion timeout. Read and
    // stats work is cancellation-safe and receives the per-claim deadline.
    let response = if scope.ends_with(":read") || scope.ends_with(":stats") {
        match tokio::time::timeout(
            Duration::from_millis(claims.limits.max_request_ms),
            next.run(request),
        )
        .await
        {
            Ok(response) => response,
            Err(_) => {
                return AuthError::limit(StatusCode::GATEWAY_TIMEOUT, "request_time_exceeded")
                    .response();
            }
        }
    } else {
        next.run(request).await
    };
    let (parts, body) = response.into_parts();
    match to_bytes(body, claims.limits.max_response_bytes).await {
        Ok(body) => Response::from_parts(parts, Body::from(body)),
        Err(_) => {
            AuthError::limit(StatusCode::PAYLOAD_TOO_LARGE, "response_bytes_exceeded").response()
        }
    }
}

fn bearer_token(headers: &axum::http::HeaderMap) -> Result<&str, AuthError> {
    let value = headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| AuthError::unauthorized("missing_credentials"))?
        .to_str()
        .map_err(|_| AuthError::unauthorized("invalid_credentials"))?;
    let token = value
        .strip_prefix("Bearer ")
        .ok_or_else(|| AuthError::unauthorized("invalid_credentials"))?;
    if token.is_empty() || token.len() > MAX_TOKEN_BYTES {
        return Err(AuthError::unauthorized("invalid_credentials"));
    }
    Ok(token)
}

fn required_scope(signal: &str, method: &Method, path: &str) -> String {
    let operation = if path == "/ready" || path == "/health" || path.ends_with("/stats") {
        "stats"
    } else if path.ends_with("/flush") || path.ends_with("/optimize") {
        "maintenance"
    } else if *method == Method::GET || *method == Method::HEAD {
        "read"
    } else if path.contains("query")
        || path.contains("series")
        || path.contains("labels")
        || path.contains("search")
        || path.contains("services")
        || path.contains("operations")
        || path.contains("traces/")
    {
        "read"
    } else {
        "write"
    };
    format!("{signal}:{operation}")
}

fn enforce_query_limit(query: Option<&str>, maximum: usize) -> Result<(), AuthError> {
    let Some(query) = query else {
        return Ok(());
    };
    for (name, value) in form_urlencoded::parse(query.as_bytes()) {
        if matches!(name.as_ref(), "limit" | "max_rows" | "max_points") {
            let requested = value
                .parse::<usize>()
                .map_err(|_| AuthError::limit(StatusCode::BAD_REQUEST, "invalid_query_limit"))?;
            if requested > maximum {
                return Err(AuthError::limit(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "query_rows_exceeded",
                ));
            }
        } else if name == "query" {
            enforce_embedded_query_limit(&value, maximum)?;
        }
    }
    Ok(())
}

fn enforce_embedded_query_limit(query: &str, maximum: usize) -> Result<(), AuthError> {
    for segment in query.split('|') {
        let mut words = segment.split_ascii_whitespace();
        if words
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case("limit"))
        {
            let requested = words
                .next()
                .ok_or_else(|| AuthError::limit(StatusCode::BAD_REQUEST, "invalid_query_limit"))?
                .parse::<usize>()
                .map_err(|_| AuthError::limit(StatusCode::BAD_REQUEST, "invalid_query_limit"))?;
            if requested > maximum {
                return Err(AuthError::limit(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "query_rows_exceeded",
                ));
            }
        }
    }
    Ok(())
}

fn enforce_query_limit_from_body(body: &[u8], maximum: usize) -> Result<(), AuthError> {
    let Ok(body) = std::str::from_utf8(body) else {
        return Ok(());
    };
    enforce_query_limit(Some(body), maximum)
}

impl AuthVerifier {
    async fn admit(self: &Arc<Self>, claims: &VerifiedClaims) -> Result<AdmissionGuard, AuthError> {
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(claims.limits.max_queue_ms);
        loop {
            let notified = self.admission_notify.notified();
            {
                let mut admission = self.admission.lock().map_err(|_| {
                    AuthError::limit(StatusCode::SERVICE_UNAVAILABLE, "queue_unavailable")
                })?;
                let active = admission.entry(claims.sub.clone()).or_default();
                if *active < claims.limits.max_concurrent_requests {
                    *active += 1;
                    return Ok(AdmissionGuard {
                        verifier: self.clone(),
                        subject: claims.sub.clone(),
                    });
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(AuthError::limit(
                    StatusCode::TOO_MANY_REQUESTS,
                    "queue_wait_exceeded",
                ));
            }
        }
    }

    fn policy(&self) -> Result<Arc<PolicyFile>, AuthError> {
        let metadata = fs::metadata(&self.policy_path)
            .map_err(|_| AuthError::unauthorized("authorization_policy_unavailable"))?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_POLICY_BYTES {
            return Err(AuthError::unauthorized("authorization_policy_invalid"));
        }
        let modified = metadata.modified().ok();
        let file_identity = file_identity(&metadata);
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| AuthError::unauthorized("authorization_policy_unavailable"))?;
        if let Some(cached) = cache.as_ref() {
            if cached.len == metadata.len()
                && cached.modified == modified
                && cached.file_identity == file_identity
            {
                return Ok(cached.policy.clone());
            }
        }
        let bytes = fs::read(&self.policy_path)
            .map_err(|_| AuthError::unauthorized("authorization_policy_unavailable"))?;
        let policy: PolicyFile = serde_json::from_slice(&bytes)
            .map_err(|_| AuthError::unauthorized("authorization_policy_invalid"))?;
        validate_policy(&policy, &self.signal, &self.tenant)?;
        let policy = Arc::new(policy);
        *cache = Some(CachedPolicy {
            len: metadata.len(),
            modified,
            file_identity,
            policy: policy.clone(),
        });
        Ok(policy)
    }

    fn verify(&self, token: &str, required_scope: &str) -> Result<VerifiedClaims, AuthError> {
        let mut segments = token.split('.');
        let (Some(encoded_header), Some(encoded_claims), Some(encoded_signature), None) = (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ) else {
            return Err(AuthError::unauthorized("invalid_token"));
        };
        let header: TokenHeader = decode_json(encoded_header)?;
        if header.alg != "EdDSA" || header.typ.as_deref().is_some_and(|value| value != "JWT") {
            return Err(AuthError::unauthorized("invalid_token"));
        }
        let claims: TokenClaims = decode_json(encoded_claims)?;
        let policy = self.policy()?;
        let key = policy
            .keys
            .iter()
            .find(|key| key.kid == header.kid)
            .ok_or_else(|| AuthError::unauthorized("unknown_key"))?;
        let public_key = decode_fixed::<32>(&key.public_key)?;
        let signature = decode_fixed::<64>(encoded_signature)?;
        VerifyingKey::from_bytes(&public_key)
            .map_err(|_| AuthError::unauthorized("invalid_key"))?
            .verify(
                format!("{encoded_header}.{encoded_claims}").as_bytes(),
                &Signature::from_bytes(&signature),
            )
            .map_err(|_| AuthError::unauthorized("invalid_signature"))?;

        let now = unix_seconds()?;
        if claims.exp <= now {
            return Err(AuthError::unauthorized("expired_token"));
        }
        if claims.exp <= claims.iat
            || claims.exp.saturating_sub(claims.iat) > policy.max_token_seconds
        {
            return Err(AuthError::unauthorized("token_lifetime_exceeded"));
        }
        if claims.nbf > now.saturating_add(CLOCK_SKEW_SECONDS)
            || claims.iat > now.saturating_add(CLOCK_SKEW_SECONDS)
            || key.not_before > now.saturating_add(CLOCK_SKEW_SECONDS)
        {
            return Err(AuthError::unauthorized("token_not_yet_valid"));
        }
        if key.revoked || key.expires_at <= now {
            return Err(AuthError::unauthorized("revoked_key"));
        }
        if claims.iss != policy.issuer {
            return Err(AuthError::unauthorized("wrong_issuer"));
        }
        if claims.aud != policy.audience {
            return Err(AuthError::unauthorized("wrong_audience"));
        }
        if claims.tenant != self.tenant || claims.tenant != policy.tenant {
            return Err(AuthError::forbidden("wrong_tenant"));
        }
        if claims.signal != self.signal {
            return Err(AuthError::forbidden("wrong_signal"));
        }
        if claims.auth_version < policy.minimum_auth_version {
            return Err(AuthError::unauthorized("stale_auth_version"));
        }
        let subject = policy
            .subjects
            .get(&claims.sub)
            .ok_or_else(|| AuthError::unauthorized("unknown_subject"))?;
        if !subject.enabled || claims.auth_version != subject.auth_version {
            return Err(AuthError::unauthorized("stale_auth_version"));
        }
        if policy.revoked_jtis.contains(&claims.jti) {
            return Err(AuthError::unauthorized("revoked_token"));
        }
        if !claims.scopes.iter().any(|scope| scope == required_scope) {
            return Err(AuthError::forbidden("insufficient_scope"));
        }
        if claims
            .scopes
            .iter()
            .any(|scope| !subject.scopes.contains(scope))
        {
            return Err(AuthError::forbidden("scope_policy_exceeded"));
        }
        validate_limits(&claims.limits, &policy.maximum_limits)?;
        validate_limits(&claims.limits, &subject.maximum_limits)?;
        if claims.sub.is_empty() || claims.jti.is_empty() {
            return Err(AuthError::unauthorized("invalid_token"));
        }
        Ok(VerifiedClaims {
            sub: claims.sub,
            jti: claims.jti,
            tenant: claims.tenant,
            signal: claims.signal,
            scopes: claims.scopes,
            auth_version: claims.auth_version,
            limits: claims.limits,
        })
    }
}

fn validate_policy(policy: &PolicyFile, signal: &str, tenant: &str) -> Result<(), AuthError> {
    if policy.version != 1
        || policy.issuer.is_empty()
        || policy.audience.is_empty()
        || policy.tenant != tenant
        || !matches!(signal, "metrics" | "logs" | "traces")
        || policy.keys.is_empty()
        || policy.subjects.is_empty()
        || policy.max_token_seconds <= 0
    {
        return Err(AuthError::unauthorized("authorization_policy_invalid"));
    }
    validate_limits(&policy.maximum_limits, &policy.maximum_limits)
}

const fn default_enabled() -> bool {
    true
}

fn validate_limits(actual: &ClaimLimits, maximum: &ClaimLimits) -> Result<(), AuthError> {
    if actual.max_request_bytes == 0
        || actual.max_decompressed_bytes == 0
        || actual.max_response_bytes == 0
        || actual.max_query_rows == 0
        || actual.max_request_ms == 0
        || actual.max_concurrent_requests == 0
        || actual.max_queue_ms == 0
        || actual.max_request_bytes > maximum.max_request_bytes
        || actual.max_decompressed_bytes > maximum.max_decompressed_bytes
        || actual.max_response_bytes > maximum.max_response_bytes
        || actual.max_query_rows > maximum.max_query_rows
        || actual.max_request_ms > maximum.max_request_ms
        || actual.max_concurrent_requests > maximum.max_concurrent_requests
        || actual.max_queue_ms > maximum.max_queue_ms
    {
        return Err(AuthError::forbidden("claim_limits_exceeded"));
    }
    Ok(())
}

fn decode_json<T: for<'de> Deserialize<'de>>(encoded: &str) -> Result<T, AuthError> {
    if encoded.len() > MAX_TOKEN_BYTES {
        return Err(AuthError::unauthorized("invalid_token"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| AuthError::unauthorized("invalid_token"))?;
    serde_json::from_slice(&bytes).map_err(|_| AuthError::unauthorized("invalid_token"))
}

fn decode_fixed<const N: usize>(encoded: &str) -> Result<[u8; N], AuthError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| AuthError::unauthorized("invalid_token"))?;
    bytes
        .try_into()
        .map_err(|_| AuthError::unauthorized("invalid_token"))
}

fn unix_seconds() -> Result<i64, AuthError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .ok_or_else(|| AuthError::unauthorized("clock_unavailable"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::{get, post};
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use ed25519_dalek::{Signer, SigningKey};
    use serde_json::Value;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::Notify;
    use tower::ServiceExt;

    #[tokio::test]
    async fn every_claim_boundary_scope_and_limit_fails_closed() {
        let root = std::env::temp_dir().join(format!(
            "timeless-auth-{}-{}",
            std::process::id(),
            unix_seconds().unwrap()
        ));
        fs::create_dir_all(&root).unwrap();
        let policy_path = root.join("policy.json");
        let signing = SigningKey::from_bytes(&[7; 32]);
        write_policy(&policy_path, &signing, 1, &[]);
        let config = AuthConfig::enforced("metrics", "tenant-a", &policy_path);
        config.preflight().unwrap();
        let app = protect_router(
            Router::new()
                .route("/live", get(|| async { "live" }))
                .route("/ready", get(|| async { "ready" }))
                .route("/api/v1/query", get(|| async { "read" }))
                .route("/api/v1/import", post(|| async { "write" })),
            config,
        );

        assert_eq!(request(&app, "/live", None).await.0, StatusCode::OK);
        assert_case(
            &app,
            "/ready",
            None,
            StatusCode::UNAUTHORIZED,
            "missing_credentials",
        )
        .await;
        let now = unix_seconds().unwrap();
        let valid = claims(now);
        let read = token(&signing, valid.clone());
        assert_eq!(
            request(&app, "/api/v1/query", Some(&read)).await.0,
            StatusCode::OK
        );

        for (field, value, code, status) in [
            (
                "exp",
                json!(now - 1),
                "expired_token",
                StatusCode::UNAUTHORIZED,
            ),
            (
                "nbf",
                json!(now + 120),
                "token_not_yet_valid",
                StatusCode::UNAUTHORIZED,
            ),
            (
                "iat",
                json!(now + 120),
                "token_not_yet_valid",
                StatusCode::UNAUTHORIZED,
            ),
            (
                "aud",
                json!("wrong"),
                "wrong_audience",
                StatusCode::UNAUTHORIZED,
            ),
            (
                "tenant",
                json!("wrong"),
                "wrong_tenant",
                StatusCode::FORBIDDEN,
            ),
            (
                "signal",
                json!("logs"),
                "wrong_signal",
                StatusCode::FORBIDDEN,
            ),
            (
                "auth_version",
                json!(0),
                "stale_auth_version",
                StatusCode::UNAUTHORIZED,
            ),
            (
                "scopes",
                json!(["metrics:write"]),
                "insufficient_scope",
                StatusCode::FORBIDDEN,
            ),
        ] {
            let mut changed = valid.clone();
            changed[field] = value;
            assert_case(
                &app,
                "/api/v1/query",
                Some(&token(&signing, changed)),
                status,
                code,
            )
            .await;
        }

        let mut excessive = valid.clone();
        excessive["limits"]["max_query_rows"] = json!(1_001);
        assert_case(
            &app,
            "/api/v1/query",
            Some(&token(&signing, excessive)),
            StatusCode::FORBIDDEN,
            "claim_limits_exceeded",
        )
        .await;

        let mut write = valid.clone();
        write["scopes"] = json!(["metrics:write"]);
        let write = token(&signing, write);
        assert_eq!(
            request_method(&app, Method::POST, "/api/v1/import", Some(&write), "ok")
                .await
                .0,
            StatusCode::OK
        );
        assert_case(
            &app,
            "/api/v1/query?limit=1001",
            Some(&read),
            StatusCode::UNPROCESSABLE_ENTITY,
            "query_rows_exceeded",
        )
        .await;

        write_policy(&policy_path, &signing, 1, &["token-1"]);
        assert_case(
            &protect_router(
                Router::new().route("/api/v1/query", get(|| async { "read" })),
                AuthConfig::enforced("metrics", "tenant-a", &policy_path),
            ),
            "/api/v1/query",
            Some(&read),
            StatusCode::UNAUTHORIZED,
            "revoked_token",
        )
        .await;

        write_policy(&policy_path, &signing, 2, &[]);
        assert_case(
            &protect_router(
                Router::new().route("/api/v1/query", get(|| async { "read" })),
                AuthConfig::enforced("metrics", "tenant-a", &policy_path),
            ),
            "/api/v1/query",
            Some(&read),
            StatusCode::UNAUTHORIZED,
            "stale_auth_version",
        )
        .await;
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn key_policy_signature_and_lifetime_failures_are_distinct() {
        let root = test_root("key-policy");
        let policy_path = root.join("policy.json");
        let signing = SigningKey::from_bytes(&[11; 32]);
        let other = SigningKey::from_bytes(&[12; 32]);
        let now = unix_seconds().unwrap();
        let valid = claims(now);

        let cases = [
            (
                mutate_policy(&signing, |policy| {
                    policy["keys"][0]["revoked"] = json!(true);
                }),
                token(&signing, valid.clone()),
                "revoked_key",
                StatusCode::UNAUTHORIZED,
            ),
            (
                mutate_policy(&signing, |policy| {
                    policy["keys"][0]["not_before"] = json!(now + 120);
                }),
                token(&signing, valid.clone()),
                "token_not_yet_valid",
                StatusCode::UNAUTHORIZED,
            ),
            (
                mutate_policy(&signing, |policy| {
                    policy["subjects"]["user-1"]["enabled"] = json!(false);
                }),
                token(&signing, valid.clone()),
                "stale_auth_version",
                StatusCode::UNAUTHORIZED,
            ),
            (
                policy_document(&signing, 1, &[]),
                token(&other, valid.clone()),
                "invalid_signature",
                StatusCode::UNAUTHORIZED,
            ),
        ];

        for (policy, token, reason, status) in cases {
            write_document(&policy_path, &policy);
            let app = protected_read_app(&policy_path);
            assert_case(&app, "/api/v1/query", Some(&token), status, reason).await;
        }

        write_document(&policy_path, &policy_document(&signing, 1, &[]));
        let app = protected_read_app(&policy_path);

        let mut wrong_issuer = valid.clone();
        wrong_issuer["iss"] = json!("somewhere-else");
        assert_case(
            &app,
            "/api/v1/query",
            Some(&token(&signing, wrong_issuer)),
            StatusCode::UNAUTHORIZED,
            "wrong_issuer",
        )
        .await;

        let mut lifetime = valid.clone();
        lifetime["exp"] = json!(now + 901);
        assert_case(
            &app,
            "/api/v1/query",
            Some(&token(&signing, lifetime)),
            StatusCode::UNAUTHORIZED,
            "token_lifetime_exceeded",
        )
        .await;

        let mut policy_scope = valid.clone();
        policy_scope["scopes"] = json!(["metrics:read", "metrics:admin"]);
        assert_case(
            &app,
            "/api/v1/query",
            Some(&token(&signing, policy_scope)),
            StatusCode::FORBIDDEN,
            "scope_policy_exceeded",
        )
        .await;

        let unknown = token_with_kid(&signing, valid, "new-key");
        assert_case(
            &app,
            "/api/v1/query",
            Some(&unknown),
            StatusCode::UNAUTHORIZED,
            "unknown_key",
        )
        .await;

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn request_response_time_query_and_queue_limits_are_enforced() {
        let root = test_root("limits");
        let policy_path = root.join("policy.json");
        let signing = SigningKey::from_bytes(&[13; 32]);
        write_policy(&policy_path, &signing, 1, &[]);
        let now = unix_seconds().unwrap();

        let mut write_claims = claims(now);
        write_claims["scopes"] = json!(["metrics:write"]);
        write_claims["limits"]["max_request_bytes"] = json!(3);
        let write_token = token(&signing, write_claims);
        let write_app = protect_router(
            Router::new().route("/echo", post(|body: String| async move { body })),
            AuthConfig::enforced("metrics", "tenant-a", &policy_path),
        );
        let (status, body) = request_method(
            &write_app,
            Method::POST,
            "/echo",
            Some(&write_token),
            "four",
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body["reason"], "request_bytes_exceeded");
        assert!(!body.to_string().contains(&write_token));

        let mut response_claims = claims(now);
        response_claims["limits"]["max_response_bytes"] = json!(3);
        let response_token = token(&signing, response_claims);
        let response_app = protect_router(
            Router::new().route("/response", get(|| async { "four" })),
            AuthConfig::enforced("metrics", "tenant-a", &policy_path),
        );
        assert_case(
            &response_app,
            "/response",
            Some(&response_token),
            StatusCode::PAYLOAD_TOO_LARGE,
            "response_bytes_exceeded",
        )
        .await;

        let mut timeout_claims = claims(now);
        timeout_claims["limits"]["max_request_ms"] = json!(1);
        let timeout_token = token(&signing, timeout_claims);
        let timeout_app = protect_router(
            Router::new().route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    "late"
                }),
            ),
            AuthConfig::enforced("metrics", "tenant-a", &policy_path),
        );
        assert_case(
            &timeout_app,
            "/slow",
            Some(&timeout_token),
            StatusCode::GATEWAY_TIMEOUT,
            "request_time_exceeded",
        )
        .await;

        let query_token = token(&signing, claims(now));
        let query_app = protect_router(
            Router::new().route("/api/v1/query", post(|| async { "query" })),
            AuthConfig::enforced("metrics", "tenant-a", &policy_path),
        );
        let (status, body) = request_method(
            &query_app,
            Method::POST,
            "/api/v1/query",
            Some(&query_token),
            "limit=1001",
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["reason"], "query_rows_exceeded");
        let (status, body) = request_method(
            &query_app,
            Method::POST,
            "/api/v1/query",
            Some(&query_token),
            "query=level%3Aerror+%7C+limit+1001",
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["reason"], "query_rows_exceeded");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handler_entered = entered.clone();
        let handler_release = release.clone();
        let queue_app = protect_router(
            Router::new().route(
                "/hold",
                get(move || {
                    let entered = handler_entered.clone();
                    let release = handler_release.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        "done"
                    }
                }),
            ),
            AuthConfig::enforced("metrics", "tenant-a", &policy_path),
        );
        let mut queue_claims = claims(now);
        queue_claims["limits"]["max_concurrent_requests"] = json!(1);
        queue_claims["limits"]["max_queue_ms"] = json!(10);
        let queue_token = token(&signing, queue_claims);
        let first_app = queue_app.clone();
        let first_token = queue_token.clone();
        let first =
            tokio::spawn(async move { request(&first_app, "/hold", Some(&first_token)).await });
        entered.notified().await;
        assert_case(
            &queue_app,
            "/hold",
            Some(&queue_token),
            StatusCode::TOO_MANY_REQUESTS,
            "queue_wait_exceeded",
        )
        .await;
        release.notify_one();
        assert_eq!(first.await.unwrap().0, StatusCode::OK);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn atomic_same_size_policy_replacement_invalidates_the_cache() {
        let root = test_root("reload");
        let policy_path = root.join("policy.json");
        let temporary = root.join("policy.next.json");
        let signing = SigningKey::from_bytes(&[14; 32]);
        let now = unix_seconds().unwrap();
        let token = token(&signing, claims(now));

        write_policy(&policy_path, &signing, 1, &["token-2"]);
        let app = protected_read_app(&policy_path);
        assert_eq!(
            request(&app, "/api/v1/query", Some(&token)).await.0,
            StatusCode::OK
        );

        write_policy(&temporary, &signing, 1, &["token-1"]);
        assert_eq!(
            fs::metadata(&temporary).unwrap().len(),
            fs::metadata(&policy_path).unwrap().len()
        );
        fs::rename(&temporary, &policy_path).unwrap();
        assert_case(
            &app,
            "/api/v1/query",
            Some(&token),
            StatusCode::UNAUTHORIZED,
            "revoked_token",
        )
        .await;

        let _ = fs::remove_dir_all(root);
    }

    fn claims(now: i64) -> Value {
        json!({
            "iss": "timeless-control-plane", "aud": "timeless-data-plane",
            "sub": "user-1", "jti": "token-1", "tenant": "tenant-a",
            "signal": "metrics", "scopes": ["metrics:read", "metrics:stats"],
            "auth_version": 1, "iat": now, "nbf": now - 1, "exp": now + 300,
            "limits": {"max_request_bytes": 1024, "max_decompressed_bytes": 2048,
                       "max_response_bytes": 1024,
                       "max_query_rows": 1000, "max_request_ms": 1000,
                       "max_concurrent_requests": 2, "max_queue_ms": 100}
        })
    }

    fn write_policy(path: &Path, signing: &SigningKey, version: u64, revoked: &[&str]) {
        write_document(path, &policy_document(signing, version, revoked));
    }

    fn policy_document(signing: &SigningKey, version: u64, revoked: &[&str]) -> Value {
        json!({
            "version": 1, "issuer": "timeless-control-plane",
            "audience": "timeless-data-plane", "tenant": "tenant-a",
            "minimum_auth_version": version,
            "max_token_seconds": 900,
            "maximum_limits": {"max_request_bytes": 4096, "max_decompressed_bytes": 8192,
                               "max_response_bytes": 4096,
                               "max_query_rows": 1000, "max_request_ms": 5000,
                               "max_concurrent_requests": 8, "max_queue_ms": 1000},
            "subjects": {"user-1": {"auth_version": version,
                         "scopes": ["metrics:read", "metrics:write", "metrics:stats"],
                         "maximum_limits": {"max_request_bytes": 4096,
                                            "max_decompressed_bytes": 8192,
                                            "max_response_bytes": 4096,
                                            "max_query_rows": 1000,
                                            "max_request_ms": 5000,
                                            "max_concurrent_requests": 8,
                                            "max_queue_ms": 1000},
                         "enabled": true}},
            "keys": [{"kid": "key-1",
                          "public_key": URL_SAFE_NO_PAD.encode(signing.verifying_key().as_bytes()),
                          "not_before": 0, "expires_at": i64::MAX, "revoked": false}],
            "revoked_jtis": revoked
        })
    }

    fn mutate_policy(signing: &SigningKey, mutate: impl FnOnce(&mut Value)) -> Value {
        let mut policy = policy_document(signing, 1, &[]);
        mutate(&mut policy);
        policy
    }

    fn write_document(path: &Path, document: &Value) {
        fs::write(path, serde_json::to_vec(document).unwrap()).unwrap();
    }

    fn protected_read_app(policy_path: &Path) -> Router {
        protect_router(
            Router::new().route("/api/v1/query", get(|| async { "read" })),
            AuthConfig::enforced("metrics", "tenant-a", policy_path),
        )
    }

    fn test_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "timeless-auth-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn token(signing: &SigningKey, claims: Value) -> String {
        token_with_kid(signing, claims, "key-1")
    }

    fn token_with_kid(signing: &SigningKey, claims: Value, kid: &str) -> String {
        let header = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({"alg": "EdDSA", "kid": kid, "typ": "JWT"})).unwrap(),
        );
        let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        let input = format!("{header}.{claims}");
        let signature = signing.sign(input.as_bytes());
        format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
    }

    async fn request(app: &Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
        request_method(app, Method::GET, path, token, "").await
    }

    async fn request_method(
        app: &Router,
        method: Method,
        path: &str,
        token: Option<&str>,
        body: &str,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 16_384).await.unwrap();
        let body = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
        (status, body)
    }

    async fn assert_case(
        app: &Router,
        path: &str,
        token: Option<&str>,
        status: StatusCode,
        reason: &str,
    ) {
        let response = request(app, path, token).await;
        assert_eq!(response.0, status);
        assert_eq!(response.1["reason"], reason);
    }
}
