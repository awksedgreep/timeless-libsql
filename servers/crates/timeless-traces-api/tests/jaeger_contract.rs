use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use serde_json::Value;
use tempfile::TempDir;
use timeless_traces_api::{router, Storage};
use tower::ServiceExt;

const TRACE_ID: &str = "00112233445566778899aabbccddeeff";
const ROOT_ID: &str = "0102030405060708";
const CHILD_ID: &str = "1112131415161718";

#[tokio::test]
async fn session_zero_fixture_has_semantically_exact_jaeger_routes() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let storage =
        Storage::start(directory.path().join("jaeger.db"), extension, 2, 16, None).unwrap();
    let app = router(storage.clone());
    let fixture = std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../timeless_traces/test/fixtures/data_plane/rich_trace.otlp.json"),
    )
    .unwrap();
    assert_eq!(post_otlp(&app, &fixture).await.0, StatusCode::OK);
    storage.flush().await.unwrap();

    let services = get_json(&app, "/select/jaeger/api/services").await;
    assert_eq!(services.0, StatusCode::OK, "{}", services.1);
    assert_eq!(services.1["data"], serde_json::json!(["contract-svc"]));
    assert_envelope(&services.1, 1);

    let operations = get_json(&app, "/select/jaeger/api/services/contract-svc/operations").await;
    assert_eq!(operations.0, StatusCode::OK, "{}", operations.1);
    assert_eq!(
        operations.1["data"],
        serde_json::json!(["DB contract", "GET /contract"])
    );
    assert_envelope(&operations.1, 2);

    let detail = get_json(&app, &format!("/select/jaeger/api/traces/{TRACE_ID}")).await;
    assert_eq!(detail.0, StatusCode::OK, "{}", detail.1);
    assert_envelope(&detail.1, 1);
    let trace = &detail.1["data"][0];
    assert_eq!(trace["traceID"], TRACE_ID);
    assert_eq!(trace["spans"].as_array().unwrap().len(), 2);
    let process = trace["processes"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert_eq!(process["serviceName"], "contract-svc");
    assert_eq!(
        tag_map(&process["tags"]),
        serde_json::json!({
            "debug":["bool",false],
            "replica":["int64",7],
            "service.version":["string","1.2.3"]
        })
    );
    let spans = trace["spans"]
        .as_array()
        .unwrap()
        .iter()
        .map(|span| (span["spanID"].as_str().unwrap(), span))
        .collect::<std::collections::HashMap<_, _>>();
    let root = spans[ROOT_ID];
    assert_eq!(root["operationName"], "GET /contract");
    assert_eq!(root["startTime"], 1_700_000_000_000_000_i64);
    assert_eq!(root["duration"], 120_000);
    assert_eq!(root["references"], serde_json::json!([]));
    assert_eq!(
        tag_map(&root["tags"]),
        serde_json::json!({
            "http.method":["string","GET"],
            "http.status_code":["int64",503],
            "otel.status_code":["string","ERROR"],
            "otel.status_description":["string","contract failure"],
            "retryable":["bool",true],
            "score":["float64",0.75],
            "span.kind":["string","server"]
        })
    );
    assert_eq!(root["logs"][0]["timestamp"], 1_700_000_000_040_000_i64);
    assert_eq!(root["logs"][0]["fields"][0]["value"], "exception");
    let child = spans[CHILD_ID];
    assert_eq!(
        child["references"],
        serde_json::json!([{"refType":"CHILD_OF","traceID":TRACE_ID,"spanID":ROOT_ID}])
    );

    // Preserve the established compatibility boundary: operation filtering
    // and `limit` apply to spans before trace grouping.
    let search = get_json(
        &app,
        "/select/jaeger/api/traces?service=contract-svc&operation=GET%20%2Fcontract&limit=10",
    )
    .await;
    assert_eq!(search.1["data"][0]["spans"][0]["spanID"], ROOT_ID);
    assert_eq!(search.1["data"][0]["spans"].as_array().unwrap().len(), 1);

    let slow = get_json(
        &app,
        "/select/jaeger/api/traces?service=contract-svc&minDuration=100ms&maxDuration=2s&start=1699999999999000&end=1700000000001000&limit=10",
    )
    .await;
    assert_eq!(slow.1["data"][0]["spans"][0]["spanID"], ROOT_ID);
    let fast = get_json(
        &app,
        "/select/jaeger/api/traces?service=contract-svc&maxDuration=100ms&limit=10",
    )
    .await;
    assert_eq!(fast.1["data"][0]["spans"][0]["spanID"], CHILD_ID);

    let bad = get_json(&app, "/select/jaeger/api/traces?minDuration=oops").await;
    assert_eq!(bad.0, StatusCode::BAD_REQUEST);
    assert!(bad.1["error"].as_str().unwrap().contains("duration"));

    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.api_services_requests, 1);
    assert_eq!(stats.api_operations_requests, 1);
    assert_eq!(stats.api_trace_requests, 1);
    assert_eq!(stats.api_search_requests, 3);
    assert_eq!(stats.api_read_requests, 6);
    assert_eq!(stats.api_read_in_flight, 0);
    assert_eq!(stats.api_read_errors, 0);
    assert_eq!(stats.api_read_result_spans, 5);
    assert!(stats.api_read_response_bytes > 0);
    assert_eq!(stats.extension_query_count, 4);
    assert_eq!(stats.extension_query_candidate_blocks, 6);
    assert_eq!(stats.extension_query_payload_blocks_read, 6);
    assert_eq!(stats.extension_query_decoded_spans, 6);
    // Discovery is metadata-native and duration is now an exact engine
    // predicate, so only the five API-visible span rows cross the vtab.
    assert_eq!(stats.extension_query_matched_spans, 5);
    assert_eq!(stats.extension_query_returned_spans, 5);
    assert_eq!(stats.extension_query_bounded_count, 3);
    assert_eq!(stats.extension_query_bounded_requested_spans, 30);
    assert_eq!(stats.extension_query_bounded_max_spans, 10);
    assert_eq!(stats.extension_query_stable_location_snapshots, 4);
    assert_eq!(stats.extension_query_snapshot_payload_bytes, 0);
    assert_eq!(stats.extension_discovery_count, 2);
    assert_eq!(stats.extension_discovery_payload_bytes_read, 0);
    assert_eq!(stats.extension_discovery_decoded_spans, 0);
    assert!(stats.extension_query_payload_bytes_read > 0);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn trace_lookup_assembles_spans_split_across_extension_blocks() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let storage = Storage::start(
        directory.path().join("cross-block.db"),
        extension,
        1,
        8,
        None,
    )
    .unwrap();
    let app = router(storage.clone());
    let trace_id = [0xaa; 16];
    let root_id = [0x11; 8];
    let child_id = [0x22; 8];
    let mut first = (1..8_192)
        .map(|number| Span::minimal(number as u64))
        .collect::<Vec<_>>();
    first.push(Span::target(trace_id, root_id, [0; 8], "root", 100));
    let first_blob = rich_batch(&first);
    storage
        .submit_batch(first_blob.clone(), first.len(), first_blob.len())
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    let second = vec![Span::target(trace_id, child_id, root_id, "child", 200)];
    let second_blob = rich_batch(&second);
    storage
        .submit_batch(second_blob.clone(), second.len(), second_blob.len())
        .await
        .unwrap();
    storage.flush().await.unwrap();

    let trace_hex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let response = get_json(&app, &format!("/select/jaeger/api/traces/{trace_hex}")).await;
    let spans = response.1["data"][0]["spans"].as_array().unwrap();
    assert_eq!(
        spans
            .iter()
            .map(|span| span["operationName"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["root", "child"]
    );
    assert_eq!(spans[1]["references"][0]["spanID"], "1111111111111111");
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_query_cancels_and_the_same_reader_is_reusable() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let storage =
        Storage::start(directory.path().join("cancel.db"), extension, 1, 16, None).unwrap();
    let mut next = 1_u64;
    for _ in 0..16 {
        let spans = (0..8_192)
            .map(|_| {
                let span = Span::minimal(next);
                next += 1;
                span
            })
            .collect::<Vec<_>>();
        let batch = rich_batch(&spans);
        storage
            .submit_batch(batch.clone(), spans.len(), batch.len())
            .await
            .unwrap();
    }
    storage.barrier().await.unwrap();
    let app = router(storage.clone());
    let slow_app = app.clone();
    let task = tokio::spawn(async move {
        slow_app
            .oneshot(
                Request::get(
                    "/select/jaeger/api/traces?service=svc&minDuration=999999999s&limit=100",
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(2)).await;
    task.abort();
    let _ = task.await;

    let stats = tokio::time::timeout(Duration::from_secs(3), storage.stats())
        .await
        .expect("cancelled query kept the sole reader busy")
        .unwrap();
    assert_eq!(stats.api_read_cancelled, 1);
    assert_eq!(stats.api_read_in_flight, 0);
    assert_eq!(stats.extension_query_cancelled, 1);
    let fresh = tokio::time::timeout(
        Duration::from_secs(2),
        get_json(&app, "/select/jaeger/api/services/svc/operations"),
    )
    .await
    .expect("reader was not reusable after cancellation");
    assert_eq!(fresh.0, StatusCode::OK);
    assert_eq!(fresh.1["data"], serde_json::json!(["span"]));
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn broad_query_releases_publication_gate_before_decode_cpu() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let storage = Storage::start(
        directory.path().join("writer-fairness.db"),
        extension,
        2,
        32,
        None,
    )
    .unwrap();
    let mut next = 1_u64;
    for _ in 0..16 {
        let spans = (0..8_192)
            .map(|_| {
                let span = Span::minimal(next);
                next += 1;
                span
            })
            .collect::<Vec<_>>();
        let batch = rich_batch(&spans);
        storage
            .submit_batch(batch.clone(), spans.len(), batch.len())
            .await
            .unwrap();
    }
    storage.barrier().await.unwrap();
    let app = router(storage.clone());
    let slow_app = app.clone();
    let slow = tokio::spawn(async move {
        get_json(
            &slow_app,
            "/select/jaeger/api/traces?service=svc&minDuration=999999999s&limit=100",
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert!(
        !slow.is_finished(),
        "broad query did not overlap the writer"
    );

    let target = Span::target([0xbb; 16], [0x33; 8], [0; 8], "published", 999);
    let batch = rich_batch(std::slice::from_ref(&target));
    tokio::time::timeout(
        Duration::from_secs(2),
        storage.submit_batch(batch.clone(), 1, batch.len()),
    )
    .await
    .expect("query CPU blocked writer publication")
    .unwrap();
    assert!(
        !slow.is_finished(),
        "broad query finished before writer-fairness overlap was observed"
    );
    assert_eq!(slow.await.unwrap().0, StatusCode::OK);
    storage.flush().await.unwrap();
    let published = get_json(
        &app,
        "/select/jaeger/api/traces/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )
    .await;
    assert_eq!(published.0, StatusCode::OK, "{}", published.1);
    assert_eq!(
        published.1["data"][0]["spans"][0]["operationName"],
        "published"
    );
    storage.shutdown().await.unwrap();
}

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn post_otlp(app: &axum::Router, body: &[u8]) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::post("/insert/opentelemetry/v1/traces")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, bytes.to_vec())
}

fn assert_envelope(value: &Value, total: u64) {
    assert_eq!(value["errors"], Value::Null);
    assert_eq!(value["limit"], 0);
    assert_eq!(value["offset"], 0);
    assert_eq!(value["total"], total);
}

fn tag_map(tags: &Value) -> Value {
    Value::Object(
        tags.as_array()
            .unwrap()
            .iter()
            .map(|tag| {
                (
                    tag["key"].as_str().unwrap().to_owned(),
                    serde_json::json!([tag["type"], tag["value"]]),
                )
            })
            .collect(),
    )
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

#[derive(Clone)]
struct Span {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: [u8; 8],
    name: String,
    service: String,
    start_ts: i64,
}

impl Span {
    fn minimal(number: u64) -> Self {
        Self {
            trace_id: (number as u128).to_be_bytes(),
            span_id: number.to_be_bytes(),
            parent_span_id: [0; 8],
            name: "span".into(),
            service: "svc".into(),
            start_ts: 1_700_000_000_000_000_000_i64.saturating_add(number as i64),
        }
    }

    fn target(
        trace_id: [u8; 16],
        span_id: [u8; 8],
        parent_span_id: [u8; 8],
        name: &str,
        start_ts: i64,
    ) -> Self {
        Self {
            trace_id,
            span_id,
            parent_span_id,
            name: name.into(),
            service: "target".into(),
            start_ts,
        }
    }
}

fn rich_batch(spans: &[Span]) -> Vec<u8> {
    let mut out = vec![0x02, 0, 0, 0];
    out.extend_from_slice(&(spans.len() as u32).to_le_bytes());
    for span in spans {
        out.extend_from_slice(&span.trace_id);
    }
    for span in spans {
        out.extend_from_slice(&span.span_id);
    }
    for span in spans {
        out.extend_from_slice(&span.parent_span_id);
    }
    text_column(&mut out, spans.iter().map(|span| span.name.as_str()));
    text_column(&mut out, spans.iter().map(|span| span.service.as_str()));
    out.extend(std::iter::repeat_n(0, spans.len()));
    out.extend(std::iter::repeat_n(0, spans.len()));
    for span in spans {
        out.extend_from_slice(&span.start_ts.to_le_bytes());
    }
    for _ in spans {
        out.extend_from_slice(&1_i64.to_le_bytes());
    }
    for value in ["{}", "", "[]", "{}", "{}"] {
        text_column(&mut out, std::iter::repeat_n(value, spans.len()));
    }
    out
}

fn text_column<'a>(out: &mut Vec<u8>, values: impl Iterator<Item = &'a str>) {
    for value in values {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }
}
