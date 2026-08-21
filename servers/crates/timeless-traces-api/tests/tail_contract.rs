//! Live tail (/select/timeless/api/spans/tail).
//!
//! The streaming twin of the dashboard search: admitted spans fan out to
//! subscribers through the storage tail hub, filtered server-side. These
//! tests read the response body incrementally -- the stream never ends on
//! its own, so each assertion reads exactly the frames it expects.

use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tempfile::TempDir;
use timeless_traces_api::{router, Storage};
use tower::ServiceExt;

#[tokio::test(flavor = "multi_thread")]
async fn tail_streams_matching_spans_and_filters_server_side() {
    let directory = TempDir::new().unwrap();
    let storage = Storage::start(
        directory.path().join("tail.db"),
        required_extension(),
        2,
        16,
        None,
    )
    .unwrap();
    let app = router(storage.clone());

    // Subscribe to error spans from one service.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/select/timeless/api/spans/tail?service=checkout&status=error")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/x-ndjson")
    );
    let mut body = response.into_body();

    // One matching span and two that miss on either half of the filter.
    let ingest = app
        .clone()
        .oneshot(ingest_request(&spans(&[
            ("checkout", "GET /orders", "STATUS_CODE_ERROR"),
            ("checkout", "GET /healthz", "STATUS_CODE_OK"),
            ("shipping", "GET /labels", "STATUS_CODE_ERROR"),
        ])))
        .await
        .unwrap();
    assert!(ingest.status().is_success());

    // Exactly the matching span arrives. If the non-matching ones were
    // merely being filtered somewhere downstream, the next frame would be
    // one of them rather than the second batch's span.
    let row = next_row(&mut body).await;
    assert_eq!(row["name"], "GET /orders");
    assert_eq!(row["service"], "checkout");
    assert_eq!(row["status"], "error");

    let ingest = app
        .clone()
        .oneshot(ingest_request(&spans(&[(
            "checkout",
            "POST /orders",
            "STATUS_CODE_ERROR",
        )])))
        .await
        .unwrap();
    assert!(ingest.status().is_success());

    let row = next_row(&mut body).await;
    assert_eq!(row["name"], "POST /orders");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unfiltered_tail_receives_every_span() {
    // The cleared-filter case: no filter means everything, not nothing.
    let directory = TempDir::new().unwrap();
    let storage = Storage::start(
        directory.path().join("tail-all.db"),
        required_extension(),
        2,
        16,
        None,
    )
    .unwrap();
    let app = router(storage.clone());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/select/timeless/api/spans/tail")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();

    let ingest = app
        .clone()
        .oneshot(ingest_request(&spans(&[
            ("checkout", "GET /orders", "STATUS_CODE_ERROR"),
            ("shipping", "GET /labels", "STATUS_CODE_OK"),
        ])))
        .await
        .unwrap();
    assert!(ingest.status().is_success());

    let mut names = vec![
        next_row(&mut body).await["name"]
            .as_str()
            .unwrap()
            .to_owned(),
        next_row(&mut body).await["name"]
            .as_str()
            .unwrap()
            .to_owned(),
    ];
    names.sort();
    assert_eq!(names, ["GET /labels", "GET /orders"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn tail_rejects_a_kind_or_status_outside_the_enumerated_set() {
    // Accepting these would stream nothing and look exactly like a system
    // with no matching spans.
    let directory = TempDir::new().unwrap();
    let storage = Storage::start(
        directory.path().join("tail-reject.db"),
        required_extension(),
        2,
        16,
        None,
    )
    .unwrap();
    let app = router(storage);

    for query in ["kind=srever", "status=failed"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/select/timeless/api/spans/tail?{query}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{query} should be rejected"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tail_pins_to_a_host_through_a_span_attribute() {
    // How a canvas trace element scopes itself to one host.
    let directory = TempDir::new().unwrap();
    let storage = Storage::start(
        directory.path().join("tail-attr.db"),
        required_extension(),
        2,
        16,
        None,
    )
    .unwrap();
    let app = router(storage.clone());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/select/timeless/api/spans/tail?attributes=%7B%22host.name%22%3A%22srv-a%22%7D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();

    let ingest = app
        .clone()
        .oneshot(ingest_request(
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[
                {"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"on other host","startTimeUnixNano":"1","endTimeUnixNano":"2","attributes":[{"key":"host.name","value":{"stringValue":"srv-b"}}]},
                {"traceId":"00000000000000000000000000000002","spanId":"0000000000000002","name":"on our host","startTimeUnixNano":"3","endTimeUnixNano":"4","attributes":[{"key":"host.name","value":{"stringValue":"srv-a"}}]}
            ]}]}]}"#,
        ))
        .await
        .unwrap();
    assert!(ingest.status().is_success());

    let row = next_row(&mut body).await;
    assert_eq!(row["name"], "on our host");
}

#[tokio::test(flavor = "multi_thread")]
async fn tail_rejects_attributes_that_are_not_a_json_object() {
    // Ignoring them would stream every span while appearing pinned to a host.
    let directory = TempDir::new().unwrap();
    let storage = Storage::start(
        directory.path().join("tail-attr-bad.db"),
        required_extension(),
        2,
        16,
        None,
    )
    .unwrap();
    let app = router(storage);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/select/timeless/api/spans/tail?attributes=srv-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// Reads frames until one carries a span, skipping keepalive newlines.
async fn next_row(body: &mut Body) -> Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), body.frame())
            .await
            .expect("tail delivered a frame before the timeout")
            .expect("tail stream is still open")
            .expect("tail frame reads");
        let bytes = frame.into_data().expect("tail frames carry data");
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        return serde_json::from_str(text).expect("tail lines are JSON");
    }
}

fn ingest_request(body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/insert/opentelemetry/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

/// One OTLP request carrying a span per entry, each under its own service.
fn spans(entries: &[(&str, &str, &str)]) -> String {
    let resource_spans: Vec<String> = entries
        .iter()
        .enumerate()
        .map(|(index, (service, name, status))| {
            let trace_id = format!("{:032x}", index + 1);
            let span_id = format!("{:016x}", index + 1);
            format!(
                r#"{{"resource":{{"attributes":[{{"key":"service.name","value":{{"stringValue":"{service}"}}}}]}},"scopeSpans":[{{"spans":[{{"traceId":"{trace_id}","spanId":"{span_id}","name":"{name}","kind":"SPAN_KIND_SERVER","startTimeUnixNano":"{start}","endTimeUnixNano":"{end}","status":{{"code":"{status}"}}}}]}}]}}"#,
                start = 1_000 + index,
                end = 2_000 + index,
            )
        })
        .collect();
    format!(r#"{{"resourceSpans":[{}]}}"#, resource_spans.join(","))
}

fn required_extension() -> PathBuf {
    let path = std::env::var_os("TIMELESS_EXT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../..")
                .join("target/debug/libtimeless_ext.so")
        });
    assert!(path.is_file(), "missing {}", path.display());
    path
}
