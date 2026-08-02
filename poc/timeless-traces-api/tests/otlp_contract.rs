use std::path::{Path, PathBuf};

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use flate2::write::GzEncoder;
use flate2::Compression;
use prost::Message;
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;
use timeless_traces_api::{router, Storage, MAX_BODY_BYTES};
use tower::ServiceExt;

#[tokio::test]
async fn json_protobuf_and_gzip_preserve_the_exact_rich_fixture() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("wire-parity.db");
    let storage = Storage::start(database.clone(), extension.clone(), 2, 16, None).unwrap();
    let app = router(storage.clone());
    let json = rich_json_fixture();
    let protobuf = rich_protobuf_fixture();
    let gzip = gzip(&protobuf);

    for (body, content_type, encoding) in [
        (json.as_slice(), "application/json", None),
        (protobuf.as_slice(), "application/x-protobuf", None),
        (gzip.as_slice(), "application/x-protobuf", Some("gzip")),
    ] {
        let response = post(&app, body, content_type, encoding).await;
        assert_eq!(response.0, StatusCode::OK);
        assert_eq!(response.1, br#"{"partialSuccess":{}}"#);
    }

    let watermarks = storage.runtime_watermarks();
    assert_eq!(watermarks.admitted_requests, 3);
    assert_eq!(watermarks.completed_requests, 3);
    assert_eq!(watermarks.admitted_spans, 6);
    assert_eq!(watermarks.completed_spans, 6);
    assert_eq!(watermarks.failed_requests, 0);
    assert_eq!(watermarks.queued_requests, 0);
    assert_eq!(watermarks.in_flight_requests, 0);
    assert_eq!(
        watermarks.admitted_body_bytes,
        (json.len() + protobuf.len() + gzip.len()) as u64
    );
    assert_eq!(
        watermarks.completed_body_bytes,
        watermarks.admitted_body_bytes
    );

    let report = storage.flush().await.unwrap();
    assert_eq!(report.through_requests, 3);
    assert_eq!(report.through_spans, 6);
    assert_eq!(report.through_body_bytes, watermarks.admitted_body_bytes);
    assert_eq!(report.completed_requests, 3);
    assert_eq!(report.completed_spans, 6);

    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.api_ingest_requests, 3);
    assert_eq!(stats.api_rejected_requests, 0);
    assert_eq!(stats.admitted_requests, 3);
    assert_eq!(stats.completed_requests, 3);
    assert!(stats.api_parse_ns > 0);
    assert!(stats.api_wire_decode_ns > 0);
    assert!(stats.api_batch_encode_ns > 0);
    assert_eq!(
        stats.api_decompressed_body_bytes,
        (json.len() + protobuf.len() * 2) as u64
    );

    storage.shutdown().await.unwrap();
    drop(storage);
    assert_exact_rich_rows(&database, &extension, 6);
}

#[tokio::test]
async fn malformed_partial_and_oversized_inputs_are_atomic_pre_admission_rejections() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("validation.db");
    let storage = Storage::start(database, extension, 1, 4, None).unwrap();
    let app = router(storage.clone());

    for (body, content_type, encoding, expected) in [
        (
            b"not json".as_slice(),
            "application/json",
            None,
            StatusCode::BAD_REQUEST,
        ),
        (
            br#"{"data":"nope"}"#.as_slice(),
            "application/json",
            None,
            StatusCode::BAD_REQUEST,
        ),
        (
            b"not protobuf at all".as_slice(),
            "application/x-protobuf",
            None,
            StatusCode::BAD_REQUEST,
        ),
        (
            b"not gzip".as_slice(),
            "application/x-protobuf",
            Some("gzip"),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(post(&app, body, content_type, encoding).await.0, expected);
    }

    let mut partial: Value = serde_json::from_slice(&rich_json_fixture()).unwrap();
    partial["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "traceId": "invalid",
            "spanId": "0000000000000001",
            "startTimeUnixNano": "10",
            "endTimeUnixNano": "20"
        }));
    let partial = serde_json::to_vec(&partial).unwrap();
    assert_eq!(
        post(&app, &partial, "application/json", None).await.0,
        StatusCode::BAD_REQUEST
    );

    let reversed = br#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","startTimeUnixNano":"20","endTimeUnixNano":"10"}]}]}]}"#;
    assert_eq!(
        post(&app, reversed, "application/json", None).await.0,
        StatusCode::BAD_REQUEST
    );

    let oversized = vec![b'x'; MAX_BODY_BYTES + 1];
    assert_eq!(
        post(&app, &oversized, "application/json", None).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let decompression_bomb = gzip(&vec![0_u8; MAX_BODY_BYTES + 1]);
    assert!(decompression_bomb.len() < MAX_BODY_BYTES);
    assert_eq!(
        post(
            &app,
            &decompression_bomb,
            "application/x-protobuf",
            Some("gzip")
        )
        .await
        .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );

    let before_empty = storage.runtime_watermarks();
    assert_eq!(before_empty.admitted_requests, 0);
    assert_eq!(before_empty.completed_requests, 0);
    assert_eq!(before_empty.admitted_spans, 0);
    assert_eq!(before_empty.failed_requests, 0);

    // The established Elixir route treats every non-protobuf content type as
    // JSON. Preserve that behavior and accept an explicitly empty request.
    let empty = br#"{"resourceSpans":[]}"#;
    let accepted = post(&app, empty, "application/octet-stream", None).await;
    assert_eq!(accepted.0, StatusCode::OK);
    assert_eq!(accepted.1, br#"{"partialSuccess":{}}"#);
    let after = storage.stats().await.unwrap();
    assert_eq!(after.admitted_requests, 1);
    assert_eq!(after.completed_requests, 1);
    assert_eq!(after.admitted_spans, 0);
    assert_eq!(after.api_ingest_requests, 9);
    assert_eq!(after.api_rejected_requests, 8);
    assert_eq!(after.api_rejected_spans, 4); // partial request's 3 + reversed 1
    assert!(after.api_rejected_body_bytes >= partial.len() as u64);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn http_batches_leave_the_extension_8192_span_threshold_authoritative() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("threshold.db");
    let storage = Storage::start(database, extension, 1, 4, None).unwrap();
    let app = router(storage.clone());

    let first = minimal_json_request(1, 8_191);
    assert!(first.len() < MAX_BODY_BYTES);
    assert_eq!(
        post(&app, &first, "application/json", None).await.0,
        StatusCode::OK
    );
    let before = storage.stats().await.unwrap();
    assert_eq!(before.admitted_requests, 1);
    assert_eq!(before.completed_requests, 1);
    assert_eq!(before.buffered_spans, 8_191);
    assert_eq!(before.blocks, 0);

    let last = minimal_json_request(9_000, 1);
    assert_eq!(
        post(&app, &last, "application/json", None).await.0,
        StatusCode::OK
    );
    let threshold = storage.stats().await.unwrap();
    assert_eq!(threshold.admitted_requests, 2);
    assert_eq!(threshold.completed_requests, 2);
    assert_eq!(threshold.completed_spans, 8_192);
    assert_eq!(threshold.buffered_spans, 0);
    assert_eq!(threshold.blocks, 1);
    assert_eq!(threshold.raw_blocks, 1);
    storage.shutdown().await.unwrap();
}

async fn post(
    app: &axum::Router,
    body: &[u8],
    content_type: &str,
    encoding: Option<&str>,
) -> (StatusCode, Vec<u8>) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/insert/opentelemetry/v1/traces")
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, body.len().to_string());
    if let Some(encoding) = encoding {
        request = request.header(header::CONTENT_ENCODING, encoding);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_vec())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, body.to_vec())
}

fn rich_json_fixture() -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../timeless_traces/test/fixtures/data_plane/rich_trace.otlp.json");
    std::fs::read(path).unwrap()
}

fn rich_protobuf_fixture() -> Vec<u8> {
    use fixture_proto::any_value::Value as Any;
    let attr = |key: &str, value: Any| fixture_proto::KeyValue {
        key: key.into(),
        value: Some(fixture_proto::AnyValue { value: Some(value) }),
    };
    fixture_proto::ExportTraceServiceRequest {
        resource_spans: vec![fixture_proto::ResourceSpans {
            resource: Some(fixture_proto::Resource {
                attributes: vec![
                    attr("service.name", Any::StringValue("contract-svc".into())),
                    attr("service.version", Any::StringValue("1.2.3".into())),
                    attr("replica", Any::IntValue(7)),
                    attr("debug", Any::BoolValue(false)),
                ],
            }),
            scope_spans: vec![fixture_proto::ScopeSpans {
                scope: Some(fixture_proto::InstrumentationScope {
                    name: "contract-lib".into(),
                    version: "4.5.6".into(),
                }),
                spans: vec![
                    fixture_proto::Span {
                        trace_id: hex("00112233445566778899aabbccddeeff"),
                        span_id: hex("0102030405060708"),
                        parent_span_id: Vec::new(),
                        name: "GET /contract".into(),
                        kind: 2,
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_120_000_000,
                        attributes: vec![
                            attr("http.method", Any::StringValue("GET".into())),
                            attr("http.status_code", Any::IntValue(503)),
                            attr("retryable", Any::BoolValue(true)),
                            attr("score", Any::DoubleValue(0.75)),
                        ],
                        events: vec![fixture_proto::Event {
                            time_unix_nano: 1_700_000_000_040_000_000,
                            name: "exception".into(),
                            attributes: vec![
                                attr("exception.type", Any::StringValue("ContractError".into())),
                                attr("handled", Any::BoolValue(false)),
                            ],
                        }],
                        status: Some(fixture_proto::Status {
                            message: "contract failure".into(),
                            code: 2,
                        }),
                    },
                    fixture_proto::Span {
                        trace_id: hex("00112233445566778899aabbccddeeff"),
                        span_id: hex("1112131415161718"),
                        parent_span_id: hex("0102030405060708"),
                        name: "DB contract".into(),
                        kind: 3,
                        start_time_unix_nano: 1_700_000_000_020_000_000,
                        end_time_unix_nano: 1_700_000_000_080_000_000,
                        attributes: vec![
                            attr("db.system", Any::StringValue("libsql".into())),
                            attr("rows", Any::IntValue(3)),
                        ],
                        events: Vec::new(),
                        status: Some(fixture_proto::Status {
                            message: String::new(),
                            code: 0,
                        }),
                    },
                ],
            }],
        }],
    }
    .encode_to_vec()
}

fn assert_exact_rich_rows(database: &Path, extension: &Path, count: i64) {
    let conn = Connection::open(database).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(extension, None::<&str>).unwrap();
    }
    let actual: i64 = conn
        .query_row("SELECT COUNT(*) FROM traces", [], |row| row.get(0))
        .unwrap();
    assert_eq!(actual, count);
    let distinct: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM traces
              WHERE service='contract-svc'
                AND json_extract(resource,'$.replica')=7
                AND json_extract(instrumentation_scope,'$.name')='contract-lib'
                AND (name='DB contract' OR (
                    status_description='contract failure'
                    AND json_extract(attributes,'$.retryable')=1
                    AND json_extract(events,'$[0].name')='exception'))",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(distinct, count);
}

fn minimal_json_request(start: u64, count: usize) -> Vec<u8> {
    let spans = (start..start + count as u64)
        .map(|number| {
            serde_json::json!({
                "traceId": format!("{number:032x}"),
                "spanId": format!("{number:016x}"),
                "name": "threshold",
                "kind": 1,
                "startTimeUnixNano": (1_700_000_000_000_000_000_u64 + number).to_string(),
                "endTimeUnixNano": (1_700_000_000_000_000_001_u64 + number).to_string(),
                "status": {"code": 0},
                "attributes": [],
                "events": []
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&serde_json::json!({
        "resourceSpans": [{"scopeSpans": [{"spans": spans}]}]
    }))
    .unwrap()
}

fn gzip(body: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(body).unwrap();
    encoder.finish().unwrap()
}

fn required_extension() -> PathBuf {
    let path = std::env::var_os("TIMELESS_EXT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../../target/debug/libtimeless_ext.so"));
    assert!(path.is_file(), "missing {}", path.display());
    path
}

fn hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

mod fixture_proto {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ExportTraceServiceRequest {
        #[prost(message, repeated, tag = "1")]
        pub resource_spans: Vec<ResourceSpans>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ResourceSpans {
        #[prost(message, optional, tag = "1")]
        pub resource: Option<Resource>,
        #[prost(message, repeated, tag = "2")]
        pub scope_spans: Vec<ScopeSpans>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Resource {
        #[prost(message, repeated, tag = "1")]
        pub attributes: Vec<KeyValue>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ScopeSpans {
        #[prost(message, optional, tag = "1")]
        pub scope: Option<InstrumentationScope>,
        #[prost(message, repeated, tag = "2")]
        pub spans: Vec<Span>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct InstrumentationScope {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub version: String,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Span {
        #[prost(bytes = "vec", tag = "1")]
        pub trace_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub span_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "4")]
        pub parent_span_id: Vec<u8>,
        #[prost(string, tag = "5")]
        pub name: String,
        #[prost(int32, tag = "6")]
        pub kind: i32,
        #[prost(fixed64, tag = "7")]
        pub start_time_unix_nano: u64,
        #[prost(fixed64, tag = "8")]
        pub end_time_unix_nano: u64,
        #[prost(message, repeated, tag = "9")]
        pub attributes: Vec<KeyValue>,
        #[prost(message, repeated, tag = "11")]
        pub events: Vec<Event>,
        #[prost(message, optional, tag = "15")]
        pub status: Option<Status>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Event {
        #[prost(fixed64, tag = "1")]
        pub time_unix_nano: u64,
        #[prost(string, tag = "2")]
        pub name: String,
        #[prost(message, repeated, tag = "3")]
        pub attributes: Vec<KeyValue>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Status {
        #[prost(string, tag = "2")]
        pub message: String,
        #[prost(int32, tag = "3")]
        pub code: i32,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct KeyValue {
        #[prost(string, tag = "1")]
        pub key: String,
        #[prost(message, optional, tag = "2")]
        pub value: Option<AnyValue>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct AnyValue {
        #[prost(oneof = "any_value::Value", tags = "1, 2, 3, 4")]
        pub value: Option<any_value::Value>,
    }
    pub mod any_value {
        #[derive(Clone, PartialEq, prost::Oneof)]
        #[allow(clippy::enum_variant_names)]
        pub enum Value {
            #[prost(string, tag = "1")]
            StringValue(String),
            #[prost(bool, tag = "2")]
            BoolValue(bool),
            #[prost(int64, tag = "3")]
            IntValue(i64),
            #[prost(double, tag = "4")]
            DoubleValue(f64),
        }
    }
}
