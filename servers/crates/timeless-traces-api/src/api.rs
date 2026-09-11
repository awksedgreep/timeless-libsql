use std::time::Instant;

use axum::body::{to_bytes, Body, Bytes};
use axum::extract::rejection::{FormRejection, JsonRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Extension, Form, Path, Query, State};
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;
use timeless_api_common::{
    build_info, native_error, native_error_with_message, native_internal_error,
    server_build_identity, BackupRequest, Exposition, VerifiedClaims, PROMETHEUS_CONTENT_TYPE,
    RESULT_ROWS_HEADER,
};

use crate::otlp;
use crate::query::{DashboardSearchParams, ReadRequest, SearchParams};
use crate::tail::TailParams;
use crate::{IngestTimings, Storage, TracesQueryLimits};

pub const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

pub fn router(storage: Storage) -> Router {
    router_with_limits(storage, TracesQueryLimits::default())
}

pub fn router_with_limits(storage: Storage, limits: TracesQueryLimits) -> Router {
    limits
        .validate()
        .expect("traces router limits must be valid");
    Router::new()
        .route("/live", get(liveness))
        .route("/ready", get(readiness))
        .route("/health", get(health))
        .route("/metrics", get(self_metrics))
        .route("/select/traces/stats", get(stats))
        .route("/select/jaeger/api/services", get(services))
        .route(
            "/select/jaeger/api/services/{service}/operations",
            get(operations),
        )
        .route("/select/jaeger/api/traces", get(search_traces))
        .route("/select/jaeger/api/traces/{trace_id}", get(trace_by_id))
        .route("/select/timeless/api/spans", get(dashboard_search))
        .route(
            "/select/timeless/api/spans/tail",
            get(tail_get).post(tail_post),
        )
        .route(
            "/select/timeless/api/traces/{trace_id}",
            get(dashboard_trace),
        )
        .route("/api/v1/flush", post(flush))
        .route("/api/v1/backup", post(backup))
        .route("/insert/opentelemetry/v1/traces", post(ingest_otlp))
        .fallback(unsupported)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(Extension(limits))
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
    (
        StatusCode::OK,
        Json(json!({
            "status": "ready",
            "build": server_build_identity("traces")
        })),
    )
        .into_response()
}

async fn health(State(storage): State<Storage>) -> Response {
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
                "build": server_build_identity("traces"),
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
        Err(error) => native_server_error("stats_execution", error),
    }
}

/// Prometheus self-metrics: the `/health` operational stats in text
/// exposition format, on the plane's own port (the VictoriaMetrics-
/// family convention).
async fn self_metrics(State(storage): State<Storage>) -> Response {
    if !storage.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "# timeless-traces-api: shutting_down\n",
        )
            .into_response();
    }
    match storage.stats().await {
        Ok(stats) => {
            let clamp = |value: u64| value.min(i64::MAX as u64) as i64;
            let mut x = Exposition::new();
            build_info(&mut x, "traces");
            x.counter(
                "timeless_traces_admitted_spans_total",
                "Spans admitted into the write path.",
                clamp(stats.admitted_spans),
            );
            x.counter(
                "timeless_traces_completed_spans_total",
                "Spans durably written.",
                clamp(stats.completed_spans),
            );
            let (tail_subscribers, tail_sent, tail_dropped) = storage.tail_hub().stats();
            x.counter(
                "timeless_traces_tail_spans_sent_total",
                "Spans delivered to live-tail subscribers.",
                clamp(tail_sent),
            );
            x.counter(
                "timeless_traces_tail_spans_dropped_total",
                "Spans dropped for slow live-tail subscribers.",
                clamp(tail_dropped),
            );
            x.gauge(
                "timeless_traces_tail_active_subscribers",
                "Live-tail subscribers currently connected.",
                clamp(tail_subscribers),
            );
            x.counter(
                "timeless_traces_failed_spans_total",
                "Spans in failed requests.",
                clamp(stats.failed_spans),
            );
            x.counter(
                "timeless_traces_admitted_requests_total",
                "Ingest requests admitted into the write path.",
                clamp(stats.admitted_requests),
            );
            x.counter(
                "timeless_traces_completed_requests_total",
                "Ingest requests durably written.",
                clamp(stats.completed_requests),
            );
            x.counter(
                "timeless_traces_failed_requests_total",
                "Ingest requests that failed to write.",
                clamp(stats.failed_requests),
            );
            x.counter(
                "timeless_traces_rejected_requests_total",
                "Ingest requests rejected before admission.",
                clamp(stats.api_rejected_requests),
            );
            x.counter(
                "timeless_traces_admitted_bytes_total",
                "Request body bytes admitted into the write path.",
                clamp(stats.admitted_body_bytes),
            );
            x.gauge(
                "timeless_traces_spans",
                "Spans resident in storage.",
                stats.total_spans,
            );
            x.gauge(
                "timeless_traces_blocks",
                "Storage blocks (raw plus compressed).",
                stats.blocks,
            );
            x.gauge(
                "timeless_traces_disk_size_bytes",
                "Block payload bytes on disk.",
                stats.bytes_on_disk,
            );
            x.gauge(
                "timeless_traces_index_size_bytes",
                "SQLite index bytes on disk.",
                stats.sqlite_index_bytes,
            );
            x.gauge(
                "timeless_traces_storage_bytes",
                "Bytes of span block payload on disk — the stored side of a compression ratio; excludes indexes, WAL, and freelist.",
                stats.bytes_on_disk,
            );
            x.gauge(
                "timeless_traces_index_bytes",
                "SQLite index bytes on disk, reported beside storage bytes and never inside a compression ratio.",
                stats.sqlite_index_bytes,
            );
            x.gauge(
                "timeless_traces_wal_bytes",
                "SQLite write-ahead log size.",
                clamp(stats.database_wal_bytes),
            );
            x.counter(
                "timeless_traces_compression_input_bytes_total",
                "Raw span-block bytes fed to first-pass compression; persisted in the store's _meta, so it survives restarts. Recompression (merge, duration backfill) never accrues here.",
                stats.extension_compression_input_bytes_total,
            );
            x.counter(
                "timeless_traces_compression_output_bytes_total",
                "Compressed bytes standing in for those inputs (merges adjust this side only); persisted in the store's _meta, so it survives restarts.",
                stats.extension_compression_output_bytes_total,
            );
            x.counter(
                "timeless_traces_raw_ingested_bytes_total",
                "Raw ingested bytes: logical span bytes (ids, kind/status, timings, and all string fields) counted once when spans become durable, from the engine's persisted ingest_raw_bytes_total; monotonic under optimize and prune, survives restarts; buffered spans are not yet counted.",
                stats.raw_ingested_bytes_total,
            );
            x.gauge(
                "timeless_traces_buffered_spans",
                "Spans buffered in memory ahead of flush.",
                stats.buffered_spans,
            );
            x.gauge(
                "timeless_traces_queued_requests",
                "Ingest requests waiting in the write queue.",
                clamp(stats.queued_requests),
            );
            x.gauge(
                "timeless_traces_queued_spans",
                "Spans waiting in the write queue.",
                clamp(stats.queued_spans),
            );
            x.gauge(
                "timeless_traces_in_flight_requests",
                "Ingest requests currently being written.",
                clamp(stats.in_flight_requests),
            );
            x.gauge(
                "timeless_traces_in_flight_spans",
                "Spans currently being written.",
                clamp(stats.in_flight_spans),
            );
            x.gauge(
                "timeless_traces_oldest_queued_ms",
                "Age of the oldest queued request in milliseconds.",
                clamp(stats.oldest_queued_ms),
            );
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
                x.finish(),
            )
                .into_response()
        }
        Err(error) => compatibility_server_error(StatusCode::SERVICE_UNAVAILABLE, error),
    }
}

async fn stats(State(storage): State<Storage>) -> Response {
    match storage.stats().await {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(error) => native_server_error("stats_execution", error),
    }
}

async fn services(
    State(storage): State<Storage>,
    Extension(limits): Extension<TracesQueryLimits>,
) -> Response {
    read_response(storage, limits, ReadRequest::Services, ReadProtocol::Jaeger).await
}

async fn operations(
    State(storage): State<Storage>,
    Extension(limits): Extension<TracesQueryLimits>,
    Path(service): Path<String>,
) -> Response {
    read_response(
        storage,
        limits,
        ReadRequest::Operations { service },
        ReadProtocol::Jaeger,
    )
    .await
}

async fn trace_by_id(
    State(storage): State<Storage>,
    Extension(limits): Extension<TracesQueryLimits>,
    Path(trace_id): Path<String>,
) -> Response {
    read_response(
        storage,
        limits,
        ReadRequest::Trace { trace_id },
        ReadProtocol::Jaeger,
    )
    .await
}

async fn search_traces(
    State(storage): State<Storage>,
    Extension(limits): Extension<TracesQueryLimits>,
    params: Result<Query<SearchParams>, QueryRejection>,
) -> Response {
    let Query(params) = match params {
        Ok(params) => params,
        Err(_) => return unsupported_query_parameters(),
    };
    match ReadRequest::search(params) {
        Ok(request) => read_response(storage, limits, request, ReadProtocol::Jaeger).await,
        Err(error) => compatibility_client_error(StatusCode::BAD_REQUEST, error),
    }
}

async fn dashboard_trace(
    State(storage): State<Storage>,
    Extension(limits): Extension<TracesQueryLimits>,
    Path(trace_id): Path<String>,
) -> Response {
    read_response(
        storage,
        limits,
        ReadRequest::DashboardTrace { trace_id },
        ReadProtocol::Native,
    )
    .await
}

async fn dashboard_search(
    State(storage): State<Storage>,
    Extension(limits): Extension<TracesQueryLimits>,
    params: Result<Query<DashboardSearchParams>, QueryRejection>,
) -> Response {
    let Query(params) = match params {
        Ok(params) => params,
        Err(_) => return unsupported_query_parameters(),
    };
    match ReadRequest::dashboard_search(params) {
        Ok(request) => read_response(storage, limits, request, ReadProtocol::Native).await,
        Err(error) => native_query_error(error),
    }
}

#[derive(Clone, Copy)]
enum ReadProtocol {
    Jaeger,
    Native,
}

async fn read_response(
    storage: Storage,
    limits: TracesQueryLimits,
    request: ReadRequest,
    protocol: ReadProtocol,
) -> Response {
    // The reader actor has no notion of a deadline: bound every search
    // at the HTTP layer so one unbounded request cannot pin a reader
    // (and its SQLite connection) indefinitely.
    let output = match tokio::time::timeout(limits.deadline, storage.read(request)).await {
        Err(_) => return read_timeout(protocol, limits.deadline),
        Ok(Err(error)) => {
            return read_server_error(protocol, error);
        }
        Ok(Ok(output)) => output,
    };
    if output.body.len() > limits.max_response_bytes {
        return read_limit_error(protocol, limits.max_response_bytes);
    }
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE.as_str(), "application/json".to_owned()),
            (RESULT_ROWS_HEADER, output.rows.to_string()),
        ],
        Bytes::from(output.body),
    )
        .into_response()
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
        Err(error) => native_server_error("flush_execution", error),
    }
}

async fn backup(
    State(storage): State<Storage>,
    request: Result<Json<BackupRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match request {
        Ok(request) => request,
        Err(_) => {
            return native_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "invalid_json_body",
            )
        }
    };
    match storage.backup(request.destination).await {
        Ok(report) => (StatusCode::OK, Json(report)).into_response(),
        // A backup is already occupying the writer. That is a transient
        // conflict the caller can retry, not an internal fault, so it must
        // not be flattened into 500 by the generic error path.
        Err(error) if error == timeless_api_common::BACKUP_IN_PROGRESS => {
            native_error(StatusCode::CONFLICT, "conflict", "backup_in_progress")
        }
        Err(error) => native_server_error("backup_execution", error),
    }
}

async fn ingest_otlp(
    State(storage): State<Storage>,
    claims: Option<Extension<VerifiedClaims>>,
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
            return compatibility_client_error(
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
    let decompressed_limit = claims
        .as_ref()
        .map(|Extension(claims)| claims.limits.max_decompressed_bytes)
        .unwrap_or(MAX_BODY_BYTES)
        .min(MAX_BODY_BYTES);
    let decoded = if protobuf && gzip {
        match otlp::gunzip_bounded(&body, decompressed_limit) {
            Ok(decoded) => decoded,
            Err(error) if error.starts_with("decompressed protobuf exceeds") => {
                storage.record_ingest_rejection(0, body_bytes);
                return compatibility_client_error(StatusCode::PAYLOAD_TOO_LARGE, error);
            }
            Err(error) => {
                storage.record_ingest_rejection(0, body_bytes);
                return compatibility_client_error(StatusCode::BAD_REQUEST, error);
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
            return compatibility_client_error(StatusCode::BAD_REQUEST, error);
        }
    };
    let parse = parse_started.elapsed();
    let span_count = spans.len();
    let encode_started = Instant::now();
    let batch = match otlp::encode_rich_batch(&spans) {
        Ok(batch) => batch,
        Err(error) => {
            storage.record_ingest_rejection(span_count, body_bytes);
            return compatibility_server_error(StatusCode::INTERNAL_SERVER_ERROR, error);
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
        Ok(()) => {
            // Published only once the batch is durably accepted, so a live
            // subscriber never sees a span a search would not return. An idle
            // hub costs one atomic load.
            storage.tail_hub().publish(&spans);
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                Bytes::from_static(br#"{"partialSuccess":{}}"#),
            )
                .into_response()
        }
        Err(error) => compatibility_server_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

// -- Live tail (/select/timeless/api/spans/tail) -----------------------------
//
// The live-matchable subset of the dashboard search parameters, matched
// in-memory against every admitted span by the storage tail hub. No time
// bounds and no paging: the stream is already bounded by now. Slow consumers
// drop spans (counted in stats) rather than backpressuring ingest.

async fn tail_get(
    State(storage): State<Storage>,
    params: Result<Query<TailParams>, QueryRejection>,
) -> Response {
    let Ok(Query(params)) = params else {
        return unsupported_query_parameters();
    };
    tail(storage, params)
}

async fn tail_post(
    State(storage): State<Storage>,
    params: Result<Form<TailParams>, FormRejection>,
) -> Response {
    let Form(params) = match params {
        Ok(params) => params,
        Err(_) => return unsupported_query_parameters(),
    };
    tail(storage, params)
}

fn tail(storage: Storage, params: TailParams) -> Response {
    let filter = match params.into_filter() {
        Ok(filter) => filter,
        Err(error) => return native_query_error(error),
    };
    // An unfiltered tail is the whole firehose, which is a legitimate ask;
    // skipping the per-span match for it is just the cheaper way to serve it.
    let filter = if filter.is_empty() {
        None
    } else {
        Some(filter)
    };

    let subscription = storage.tail_hub().subscribe(filter);
    let id = crate::tail::TailHub::subscription_id(&subscription);
    // Heartbeat newlines keep idle connections alive through proxies; a full
    // buffer skips the beat rather than blocking, and a gone subscriber ends
    // the task.
    if let Some(heartbeat) = storage.tail_hub().heartbeat_sender(id) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                if heartbeat.is_closed() {
                    break;
                }
                let _ = heartbeat.try_send("\n".to_owned());
            }
        });
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .header("cache-control", "no-cache")
        .body(Body::from_stream(TailStream { subscription }))
        .expect("tail response builds")
}

/// Dropping the stream (client disconnect) drops the subscription, which
/// unsubscribes from the hub.
struct TailStream {
    subscription: crate::tail::TailSubscription,
}

impl futures_core::Stream for TailStream {
    type Item = Result<Bytes, std::convert::Infallible>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.subscription
            .receiver
            .poll_recv(context)
            .map(|line| line.map(|line| Ok(Bytes::from(line))))
    }
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn native_server_error(reason: &'static str, error: String) -> Response {
    native_internal_error("traces", reason, error)
}

fn native_query_error(error: String) -> Response {
    native_error_with_message(
        StatusCode::BAD_REQUEST,
        "invalid_query",
        "query_validation",
        error,
    )
}

fn read_server_error(protocol: ReadProtocol, error: String) -> Response {
    match protocol {
        ReadProtocol::Jaeger => {
            compatibility_server_error(StatusCode::INTERNAL_SERVER_ERROR, error)
        }
        ReadProtocol::Native => native_server_error("query_execution", error),
    }
}

fn read_timeout(protocol: ReadProtocol, deadline: std::time::Duration) -> Response {
    match protocol {
        ReadProtocol::Jaeger => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(json!({
                "status": "error",
                "error": format!(
                    "query exceeded the {}ms execution deadline",
                    deadline.as_millis()
                ),
            })),
        )
            .into_response(),
        ReadProtocol::Native => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(json!({
                "error": "timeout",
                "reason": "query_deadline",
                "deadline_ms": deadline.as_millis()
            })),
        )
            .into_response(),
    }
}

fn read_limit_error(protocol: ReadProtocol, limit: usize) -> Response {
    match protocol {
        ReadProtocol::Jaeger => compatibility_client_error(
            StatusCode::BAD_REQUEST,
            format!("response exceeds {limit} bytes"),
        ),
        ReadProtocol::Native => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "query_limit",
                "reason": "max_response_bytes",
                "limit": limit
            })),
        )
            .into_response(),
    }
}

/// Jaeger and OTLP retain their established compatibility envelopes.
fn compatibility_server_error(status: StatusCode, error: String) -> Response {
    eprintln!("timeless-traces-api: compatibility endpoint internal error: {error}");
    (
        status,
        Json(json!({"status": "error", "error": "internal"})),
    )
        .into_response()
}

fn compatibility_client_error(status: StatusCode, error: String) -> Response {
    (status, Json(json!({"error": error}))).into_response()
}

async fn unsupported() -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({"error": "unsupported_capability", "reason": "unsupported_route"})),
    )
        .into_response()
}

fn unsupported_query_parameters() -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({
            "error": "unsupported_capability",
            "reason": "unsupported_query_parameters"
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use serde_json::Value;

    async fn response_json(response: Response) -> Value {
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn native_read_errors_use_stable_codes() {
        let timeout = read_timeout(ReadProtocol::Native, std::time::Duration::from_millis(25));
        assert_eq!(timeout.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(
            response_json(timeout).await,
            json!({"error": "timeout", "reason": "query_deadline", "deadline_ms": 25})
        );

        let limit = read_limit_error(ReadProtocol::Native, 1_024);
        assert_eq!(limit.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            response_json(limit).await,
            json!({
                "error": "query_limit",
                "reason": "max_response_bytes",
                "limit": 1_024
            })
        );

        let internal = read_server_error(
            ReadProtocol::Native,
            "database /secret/customer.db is corrupt".into(),
        );
        assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response_json(internal).await,
            json!({"error": "internal", "reason": "query_execution"})
        );
    }

    #[tokio::test]
    async fn jaeger_read_errors_keep_the_compatibility_envelope() {
        let timeout = read_timeout(ReadProtocol::Jaeger, std::time::Duration::from_millis(25));
        assert_eq!(timeout.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(
            response_json(timeout).await,
            json!({
                "status": "error",
                "error": "query exceeded the 25ms execution deadline"
            })
        );

        let internal = read_server_error(ReadProtocol::Jaeger, "sensitive detail".into());
        assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response_json(internal).await,
            json!({"status": "error", "error": "internal"})
        );
    }
}
