//! The drift guard: authctl mints, the servers' verifier verifies.
//!
//! `TimelessUI.TelemetryAuth` (Elixir) and authctl are two independent
//! producers of one token format, reconciled only by this verifier. If
//! either drifts, requests fail in production; this test pins the Rust
//! pair together end to end through the real middleware.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::Router;
use timeless_api_common::{protect_router, AuthConfig};
use tower::ServiceExt;

#[tokio::test]
async fn authctl_tokens_verify_through_the_server_middleware() {
    let dir = tempfile::tempdir().unwrap();
    let keypair = timeless_authctl::keygen(dir.path()).unwrap();
    let policy_path = dir.path().join("policy.json");
    timeless_authctl::policy_init(
        "metrics",
        &keypair.public_key,
        "default",
        "default",
        &policy_path,
    )
    .unwrap();
    timeless_authctl::policy_add_subject(&policy_path, "reader", &["metrics:read".to_owned()])
        .unwrap();

    let config = AuthConfig::enforced("metrics", "default", &policy_path);
    config
        .preflight()
        .expect("scaffolded policy must pass the verifier preflight");
    let app = protect_router(
        Router::new()
            .route("/api/v1/query", get(|| async { "read" }))
            .route(
                "/api/v1/flush",
                axum::routing::post(|| async { "maintenance" }),
            ),
        config,
    );

    let request = |token: Option<String>, path: &str, method: &str| {
        let mut builder = Request::builder().uri(path).method(method);
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::empty()).unwrap()
    };

    // Full-scope subject reaches both routes.
    let token = timeless_authctl::mint(
        &dir.path().join(timeless_authctl::PRIVATE_KEY_FILE),
        &policy_path,
        "default",
        "metrics",
        300,
    )
    .unwrap();
    let response = app
        .clone()
        .oneshot(request(Some(token.clone()), "/api/v1/query", "GET"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(request(Some(token), "/api/v1/flush", "POST"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // The narrowed subject reads but cannot run maintenance: scope
    // containment survives the round trip.
    let reader = timeless_authctl::mint(
        &dir.path().join(timeless_authctl::PRIVATE_KEY_FILE),
        &policy_path,
        "reader",
        "metrics",
        300,
    )
    .unwrap();
    let response = app
        .clone()
        .oneshot(request(Some(reader.clone()), "/api/v1/query", "GET"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(request(Some(reader), "/api/v1/flush", "POST"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // No token still fails closed.
    let response = app
        .oneshot(request(None, "/api/v1/query", "GET"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // inspect decodes what mint produced.
    let token = timeless_authctl::mint(
        &dir.path().join(timeless_authctl::PRIVATE_KEY_FILE),
        &policy_path,
        "default",
        "metrics",
        60,
    )
    .unwrap();
    let (header, claims) = timeless_authctl::inspect(&token).unwrap();
    assert_eq!(header["alg"], "EdDSA");
    assert_eq!(claims["sub"], "default");
    assert_eq!(claims["signal"], "metrics");
}

#[test]
fn ttl_parsing_and_policy_cap() {
    assert_eq!(timeless_authctl::parse_ttl("30s").unwrap(), 30);
    assert_eq!(timeless_authctl::parse_ttl("15m").unwrap(), 900);
    assert_eq!(timeless_authctl::parse_ttl("1h").unwrap(), 3_600);
    assert_eq!(timeless_authctl::parse_ttl("2d").unwrap(), 172_800);
    assert_eq!(timeless_authctl::parse_ttl("45").unwrap(), 45);
    assert!(timeless_authctl::parse_ttl("1w").is_err());

    let dir = tempfile::tempdir().unwrap();
    let keypair = timeless_authctl::keygen(dir.path()).unwrap();
    let policy_path = dir.path().join("policy.json");
    timeless_authctl::policy_init(
        "logs",
        &keypair.public_key,
        "default",
        "default",
        &policy_path,
    )
    .unwrap();
    // Scaffolded policies inherit the verifier's 3600s max_token_seconds
    // default; a longer ttl is refused at mint time, not at verify time.
    let error = timeless_authctl::mint(
        &dir.path().join(timeless_authctl::PRIVATE_KEY_FILE),
        &policy_path,
        "default",
        "logs",
        7_200,
    )
    .unwrap_err();
    assert!(error.contains("max_token_seconds"), "{error}");
}
