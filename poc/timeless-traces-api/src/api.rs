use std::time::Instant;

use axum::body::{to_bytes, Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::otlp;
use crate::query::{ReadRequest, SearchParams};
use crate::{IngestTimings, Storage};

pub const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/live", get(liveness))
        .route("/ready", get(readiness))
        .route("/health", get(readiness))
        .route("/select/traces/stats", get(stats))
        .route("/select/jaeger/api/services", get(services))
        .route(
            "/select/jaeger/api/services/{service}/operations",
            get(operations),
        )
        .route("/select/jaeger/api/traces", get(search_traces))
        .route("/select/jaeger/api/traces/{trace_id}", get(trace_by_id))
        .route("/api/v1/flush", get(flush).post(flush))
        .route("/insert/opentelemetry/v1/traces", post(ingest_otlp))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(storage)
}

async fn liveness() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "alive"})))
}

async fn readiness(State(storage): State<Storage>) -> Response {
    if !storage.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not_ready", "reason": "shutting_down"})),
        )
            .into_response();
    }
    match storage.stats().await {
        Ok(stats) => (
            StatusCode::OK,
            Json(json!({
                "status": "ready",
                "capability": stats.capability,
                "module": stats.module,
                "blocks": stats.blocks,
                "spans": stats.total_spans,
                "disk_size": stats.bytes_on_disk,
                "index_size": stats.sqlite_index_bytes,
                "admitted_requests": stats.admitted_requests,
                "completed_requests": stats.completed_requests,
                "admitted_spans": stats.admitted_spans,
                "completed_spans": stats.completed_spans,
                "queued_requests": stats.queued_requests,
                "in_flight_requests": stats.in_flight_requests,
                "data_plane": {
                    "admitted_requests": stats.admitted_requests,
                    "admitted_spans": stats.admitted_spans,
                    "admitted_bytes": stats.admitted_body_bytes,
                    "completed_requests": stats.completed_requests,
                    "completed_spans": stats.completed_spans,
                    "failed_requests": stats.failed_requests,
                    "failed_spans": stats.failed_spans,
                    "rejected_requests": stats.api_rejected_requests,
                    "queued_requests": stats.queued_requests,
                    "queued_spans": stats.queued_spans,
                    "in_flight_requests": stats.in_flight_requests,
                    "in_flight_batches": stats.in_flight_requests,
                    "in_flight_spans": stats.in_flight_spans,
                    "oldest_queue_age_ms": stats.oldest_queued_ms
                }
            })),
        )
            .into_response(),
        Err(error) => server_error(StatusCode::SERVICE_UNAVAILABLE, error),
    }
}

async fn stats(State(storage): State<Storage>) -> Response {
    match storage.stats().await {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(error) => server_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

async fn services(State(storage): State<Storage>) -> Response {
    read_response(storage, ReadRequest::Services).await
}

async fn operations(State(storage): State<Storage>, Path(service): Path<String>) -> Response {
    read_response(storage, ReadRequest::Operations { service }).await
}

async fn trace_by_id(State(storage): State<Storage>, Path(trace_id): Path<String>) -> Response {
    read_response(storage, ReadRequest::Trace { trace_id }).await
}

async fn search_traces(
    State(storage): State<Storage>,
    Query(params): Query<SearchParams>,
) -> Response {
    match ReadRequest::search(params) {
        Ok(request) => read_response(storage, request).await,
        Err(error) => client_error(StatusCode::BAD_REQUEST, error),
    }
}

async fn read_response(storage: Storage, request: ReadRequest) -> Response {
    match storage.read(request).await {
        Ok(output) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            Bytes::from(output.body),
        )
            .into_response(),
        Err(error) => server_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

async fn flush(State(storage): State<Storage>) -> Response {
    let started = Instant::now();
    match storage.flush().await {
        Ok(mut report) => {
            report.api_request_ns = duration_ns(started.elapsed());
            let data_plane = json!({
                "admitted_requests": report.through_requests,
                "admitted_spans": report.through_spans,
                "admitted_bytes": report.through_body_bytes,
                "completed_requests": report.completed_requests,
                "completed_spans": report.completed_spans,
                "failed_requests": report.failed_requests,
                "failed_spans": report.failed_spans,
                "queued_requests": report.queued_requests,
                "queued_spans": report.queued_spans,
                "in_flight_requests": report.in_flight_requests,
                "in_flight_batches": report.in_flight_requests,
                "in_flight_spans": report.in_flight_spans,
                "oldest_queue_age_ms": 0
            });
            let mut body = serde_json::to_value(&report).unwrap_or_else(|_| json!({}));
            body.as_object_mut()
                .expect("FlushReport serializes as a JSON object")
                .insert("data_plane".into(), data_plane);
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(error) => server_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

async fn ingest_otlp(
    State(storage): State<Storage>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Response {
    let offered_bytes = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());
    let body = match to_bytes(request.into_body(), MAX_BODY_BYTES).await {
        Ok(body) => body,
        Err(_) => {
            storage.record_ingest_rejection(0, offered_bytes.unwrap_or(MAX_BODY_BYTES + 1));
            return client_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("request body exceeds {MAX_BODY_BYTES} bytes"),
            );
        }
    };
    let body_bytes = body.len();
    let protobuf = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("application/x-protobuf"));
    let gzip = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        == Some("gzip");

    let wire_started = Instant::now();
    let decoded = if protobuf && gzip {
        match otlp::gunzip_bounded(&body, MAX_BODY_BYTES) {
            Ok(decoded) => decoded,
            Err(error) if error.starts_with("decompressed protobuf exceeds") => {
                storage.record_ingest_rejection(0, body_bytes);
                return client_error(StatusCode::PAYLOAD_TOO_LARGE, error);
            }
            Err(error) => {
                storage.record_ingest_rejection(0, body_bytes);
                return client_error(StatusCode::BAD_REQUEST, error);
            }
        }
    } else {
        body.to_vec()
    };
    let wire_decode = wire_started.elapsed();

    let parse_started = Instant::now();
    let parsed = if protobuf {
        otlp::parse_protobuf(&decoded)
    } else {
        otlp::parse_json(&decoded)
    };
    let spans = match parsed {
        Ok(spans) => spans,
        Err(error) => {
            let rejected_spans = if protobuf {
                otlp::declared_protobuf_spans(&decoded)
            } else {
                otlp::declared_json_spans(&decoded)
            };
            storage.record_ingest_rejection(rejected_spans, body_bytes);
            return client_error(StatusCode::BAD_REQUEST, error);
        }
    };
    let parse = parse_started.elapsed();
    let span_count = spans.len();
    let encode_started = Instant::now();
    let batch = match otlp::encode_rich_batch(&spans) {
        Ok(batch) => batch,
        Err(error) => {
            storage.record_ingest_rejection(span_count, body_bytes);
            return server_error(StatusCode::INTERNAL_SERVER_ERROR, error);
        }
    };
    let batch_encode = encode_started.elapsed();
    let timings = IngestTimings {
        parse,
        wire_decode,
        batch_encode,
        decompressed_body_bytes: decoded.len(),
    };
    match storage
        .submit_otlp_batch(batch, span_count, body_bytes, timings)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            Bytes::from_static(br#"{"partialSuccess":{}}"#),
        )
            .into_response(),
        Err(error) => server_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn server_error(status: StatusCode, error: String) -> Response {
    (status, Json(json!({"status": "error", "error": error}))).into_response()
}

fn client_error(status: StatusCode, error: String) -> Response {
    (status, Json(json!({"error": error}))).into_response()
}
