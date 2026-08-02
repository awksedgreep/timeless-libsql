use std::time::Instant;

use axum::extract::{DefaultBodyLimit, Path, RawQuery, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use serde_json::json;

use crate::query::{self, Params, ReadRequest};
use crate::{victoria, Storage};

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/select/metrics/stats", get(stats))
        .route("/api/v1/flush", post(flush))
        .route("/api/v1/import", post(import_victoria))
        .route("/api/v1/import/prometheus", post(import_prometheus))
        .route("/api/v1/query", get(latest).post(latest))
        .route("/api/v1/export", get(export))
        .route("/api/v1/query_range", get(range).post(range))
        .route("/api/v1/labels", get(labels))
        .route("/api/v1/label/{name}/values", get(label_values))
        .route("/api/v1/series", get(series))
        .route("/prometheus/api/v1/labels", get(labels))
        .route("/prometheus/api/v1/label/{name}/values", get(label_values))
        .route("/prometheus/api/v1/series", get(prometheus_series))
        .route(
            "/prometheus/api/v1/query",
            get(prometheus_instant).post(prometheus_instant),
        )
        .route(
            "/prometheus/api/v1/query_range",
            get(prometheus_range).post(prometheus_range),
        )
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(storage)
}

async fn latest(
    State(storage): State<Storage>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    let params = params(query, &body);
    if params.get("query").is_some() {
        prometheus_read_route(storage, query::prometheus_instant_request(&params)).await
    } else {
        read_route(storage, query::latest_request(&params)).await
    }
}

async fn export(
    State(storage): State<Storage>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    read_route(storage, query::export_request(&params(query, &body))).await
}

async fn range(State(storage): State<Storage>, RawQuery(query): RawQuery, body: Bytes) -> Response {
    let params = params(query, &body);
    if params.get("query").is_some() {
        prometheus_read_route(storage, query::prometheus_range_request(&params)).await
    } else {
        read_route(storage, query::range_request(&params)).await
    }
}

async fn prometheus_instant(
    State(storage): State<Storage>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    prometheus_read_route(
        storage,
        query::prometheus_instant_request(&params(query, &body)),
    )
    .await
}

async fn prometheus_range(
    State(storage): State<Storage>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    prometheus_read_route(
        storage,
        query::prometheus_range_request(&params(query, &body)),
    )
    .await
}

async fn labels(
    State(storage): State<Storage>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    read_route(storage, query::labels_request(&params(query, &body))).await
}

async fn label_values(
    State(storage): State<Storage>,
    Path(name): Path<String>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    read_route(
        storage,
        query::label_values_request(&params(query, &body), name),
    )
    .await
}

async fn series(
    State(storage): State<Storage>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    read_route(storage, query::series_request(&params(query, &body), false)).await
}

async fn prometheus_series(
    State(storage): State<Storage>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    read_route(storage, query::series_request(&params(query, &body), true)).await
}

fn params(query: Option<String>, body: &[u8]) -> Params {
    Params::parse(query.as_deref(), body)
}

async fn read_route(storage: Storage, request: Result<ReadRequest, String>) -> Response {
    let request = match request {
        Ok(request) => request,
        Err(error) => return client_error(error),
    };
    match storage.read(request).await {
        Ok(output) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            Bytes::from(output.body),
        )
            .into_response(),
        Err(error) => server_error(error),
    }
}

async fn prometheus_read_route(storage: Storage, request: Result<ReadRequest, String>) -> Response {
    let request = match request {
        Ok(request) => request,
        Err(error) => return prometheus_error(StatusCode::BAD_REQUEST, "bad_data", error),
    };
    match storage.read(request).await {
        Ok(output) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            Bytes::from(output.body),
        )
            .into_response(),
        Err(error) => prometheus_error(StatusCode::UNPROCESSABLE_ENTITY, "execution", error),
    }
}

async fn import_victoria(State(storage): State<Storage>, body: Bytes) -> Response {
    let body_bytes = body.len();
    let parse_started = Instant::now();
    let batch = victoria::parse(&body);
    let parse_duration = parse_started.elapsed();
    let points = batch.point_count();
    let import_errors = batch.errors;
    let encode_started = Instant::now();
    let blob = match batch.encode() {
        Ok(blob) => blob,
        Err(error) => return server_error(error),
    };
    let encode_duration = encode_started.elapsed();
    match storage
        .submit_victoria_batch(
            blob,
            points,
            import_errors,
            body_bytes,
            parse_duration,
            encode_duration,
        )
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => server_error(error),
    }
}

async fn import_prometheus(State(storage): State<Storage>, body: Bytes) -> Response {
    match storage.submit_prometheus(body).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => server_error(error),
    }
}

async fn health(State(storage): State<Storage>) -> Response {
    match storage.stats().await {
        Ok(stats) => (
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "series": stats.series,
                "points": stats.total_points,
                "disk_points": stats.disk_points,
                "buffered_points": stats.buffered_points,
                "raw_tier_chunks": stats.raw_tier_chunks,
                "rollup_chunks": stats.rollup_chunks,
                "admitted_batches": stats.admitted_batches,
                "admitted_points": stats.admitted_points,
                "completed_batches": stats.completed_batches,
                "completed_points": stats.completed_points,
                "failed_batches": stats.failed_batches,
                "failed_points": stats.failed_points,
                "queued_batches": stats.queued_batches,
                "queued_points": stats.queued_points,
                "in_flight_batches": stats.in_flight_batches,
                "in_flight_points": stats.in_flight_points,
                "oldest_queued_ms": stats.oldest_queued_ms,
                "import_errors": stats.import_errors,
                "database_file_bytes": stats.database_file_bytes,
                "database_wal_bytes": stats.database_wal_bytes,
                "freelist_bytes": stats.freelist_bytes
            })),
        )
            .into_response(),
        Err(error) => server_error(error),
    }
}

async fn stats(State(storage): State<Storage>) -> Response {
    match storage.stats().await {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(error) => server_error(error),
    }
}

async fn flush(State(storage): State<Storage>) -> Response {
    let started = Instant::now();
    match storage.flush().await {
        Ok(mut report) => {
            report.api_request_ns = duration_ns(started.elapsed());
            (StatusCode::OK, Json(report)).into_response()
        }
        Err(error) => server_error(error),
    }
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn server_error(error: String) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"status": "error", "error": error})),
    )
        .into_response()
}

fn client_error(error: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"status": "error", "error": error})),
    )
        .into_response()
}

fn prometheus_error(status: StatusCode, error_type: &str, error: String) -> Response {
    (
        status,
        Json(json!({"status": "error", "errorType": error_type, "error": error})),
    )
        .into_response()
}
