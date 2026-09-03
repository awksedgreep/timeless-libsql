use std::time::Instant;

use axum::extract::{DefaultBodyLimit, Extension, Path, RawQuery, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use serde_json::json;
use timeless_api_common::{
    build_info, server_build_identity, BackupRequest, Exposition, PROMETHEUS_CONTENT_TYPE,
    RESULT_ROWS_HEADER,
};

use crate::query::{self, Params, ReadRequest};
use crate::{victoria, PromQueryLimits, ScrapeTargetSet, Storage};

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

pub fn router(storage: Storage) -> Router {
    router_with_limits(storage, PromQueryLimits::default())
}

pub fn router_with_limits(storage: Storage, limits: PromQueryLimits) -> Router {
    limits
        .validate()
        .expect("PromQL router limits must be valid");
    Router::new()
        .route("/live", get(liveness))
        .route("/ready", get(health))
        .route("/health", get(health))
        .route("/metrics", get(self_metrics))
        .route("/select/metrics/stats", get(stats))
        .route("/api/v1/flush", post(flush))
        .route("/api/v1/backup", post(backup))
        .route("/api/v1/import", post(import_victoria))
        .route("/api/v1/import/prometheus", post(import_prometheus))
        .route(
            "/api/v1/scrape/targets",
            get(scrape_targets).put(replace_scrape_targets),
        )
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
        .route(
            "/metricsql/api/v1/query",
            get(metricsql_instant).post(metricsql_instant),
        )
        .route(
            "/metricsql/api/v1/query_range",
            get(metricsql_range).post(metricsql_range),
        )
        .fallback(unsupported)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(Extension(limits))
        .with_state(storage)
}

async fn liveness() -> Response {
    (StatusCode::OK, Json(json!({"status": "alive"}))).into_response()
}

async fn latest(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    let params = params(query, &body);
    if params.get("query").is_some() {
        prometheus_read_route(storage, limits, query::prometheus_instant_request(&params)).await
    } else {
        read_route(storage, limits, query::latest_request(&params)).await
    }
}

async fn export(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    read_route(
        storage,
        limits,
        query::export_request(&params(query, &body)),
    )
    .await
}

async fn range(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    let params = params(query, &body);
    if params.get("query").is_some() {
        prometheus_read_route(storage, limits, query::prometheus_range_request(&params)).await
    } else {
        read_route(storage, limits, query::range_request(&params)).await
    }
}

async fn prometheus_instant(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    prometheus_read_route(
        storage,
        limits,
        query::prometheus_instant_request(&params(query, &body)),
    )
    .await
}

async fn prometheus_range(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    prometheus_read_route(
        storage,
        limits,
        query::prometheus_range_request(&params(query, &body)),
    )
    .await
}

async fn metricsql_instant(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    prometheus_read_route(
        storage,
        limits,
        query::metricsql_instant_request(&params(query, &body)),
    )
    .await
}

async fn metricsql_range(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    prometheus_read_route(
        storage,
        limits,
        query::metricsql_range_request(&params(query, &body)),
    )
    .await
}

async fn labels(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    read_route(
        storage,
        limits,
        query::labels_request(&params(query, &body)),
    )
    .await
}

async fn label_values(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    Path(name): Path<String>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    read_route(
        storage,
        limits,
        query::label_values_request(&params(query, &body), name),
    )
    .await
}

async fn series(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    read_route(
        storage,
        limits,
        query::series_request(&params(query, &body), false),
    )
    .await
}

async fn prometheus_series(
    State(storage): State<Storage>,
    Extension(limits): Extension<PromQueryLimits>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    read_route(
        storage,
        limits,
        query::series_request(&params(query, &body), true),
    )
    .await
}

fn params(query: Option<String>, body: &[u8]) -> Params {
    Params::parse(query.as_deref(), body)
}

async fn read_route(
    storage: Storage,
    limits: PromQueryLimits,
    request: Result<ReadRequest, String>,
) -> Response {
    // Native routes used to bypass PromQueryLimits entirely (no grid
    // enforcement, no deadline): apply both, exactly like the
    // Prometheus routes. `with_prometheus_limits` is a no-op for native
    // request shapes beyond validating the limits themselves.
    let request = match request.and_then(|request| request.with_prometheus_limits(limits)) {
        Ok(request) => request,
        Err(error) => return client_error(error),
    };
    match tokio::time::timeout(limits.deadline, storage.read(request)).await {
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(json!({
                "status": "error",
                "error": format!(
                    "query exceeded the {}ms execution deadline",
                    limits.deadline.as_millis()
                ),
            })),
        )
            .into_response(),
        Ok(Ok(output)) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE.as_str(), "application/json".to_owned()),
                (RESULT_ROWS_HEADER, output.rows.to_string()),
            ],
            Bytes::from(output.body),
        )
            .into_response(),
        Ok(Err(error)) => server_error(error),
    }
}

async fn prometheus_read_route(
    storage: Storage,
    limits: PromQueryLimits,
    request: Result<ReadRequest, String>,
) -> Response {
    let request = match request.and_then(|request| request.with_prometheus_limits(limits)) {
        Ok(request) => request,
        Err(error) => return prometheus_error(StatusCode::BAD_REQUEST, "bad_data", error),
    };
    match tokio::time::timeout(limits.deadline, storage.read(request)).await {
        Err(_) => prometheus_error(
            StatusCode::GATEWAY_TIMEOUT,
            "timeout",
            format!(
                "query exceeded the {}ms execution deadline",
                limits.deadline.as_millis()
            ),
        ),
        Ok(Ok(output)) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE.as_str(), "application/json".to_owned()),
                (RESULT_ROWS_HEADER, output.rows.to_string()),
            ],
            Bytes::from(output.body),
        )
            .into_response(),
        Ok(Err(error)) => prometheus_error(StatusCode::UNPROCESSABLE_ENTITY, "execution", error),
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

async fn scrape_targets(State(storage): State<Storage>) -> Response {
    // Serialize the redacted view, never the storage types: the stored
    // ScrapeAuth carries bearer tokens and passwords (see scrape.rs views).
    let report = storage.scrape_targets().await;
    (
        StatusCode::OK,
        Json(crate::scrape::ScrapeTargetSetReportView::from(&report)),
    )
        .into_response()
}

async fn replace_scrape_targets(
    State(storage): State<Storage>,
    Json(targets): Json<ScrapeTargetSet>,
) -> Response {
    match storage.replace_scrape_targets(targets).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => server_error(error),
    }
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
                "status": "ok",
                "build": server_build_identity("metrics"),
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

/// Prometheus self-metrics: the `/health` operational stats in text
/// exposition format, on the plane's own port (the VictoriaMetrics-
/// family convention), so any Prometheus-compatible scraper picks the
/// plane up with zero adapter code.
async fn self_metrics(State(storage): State<Storage>) -> Response {
    if !storage.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "# timeless-metrics-api: shutting_down\n",
        )
            .into_response();
    }
    match storage.stats().await {
        Ok(stats) => {
            let clamp = |value: u64| value.min(i64::MAX as u64) as i64;
            let mut x = Exposition::new();
            build_info(&mut x, "metrics");
            x.counter(
                "timeless_metrics_admitted_points_total",
                "Points admitted into the write path.",
                clamp(stats.admitted_points),
            );
            x.counter(
                "timeless_metrics_completed_points_total",
                "Points durably written.",
                clamp(stats.completed_points),
            );
            x.counter(
                "timeless_metrics_failed_points_total",
                "Points in failed batches.",
                clamp(stats.failed_points),
            );
            x.counter(
                "timeless_metrics_admitted_batches_total",
                "Batches admitted into the write path.",
                clamp(stats.admitted_batches),
            );
            x.counter(
                "timeless_metrics_completed_batches_total",
                "Batches durably written.",
                clamp(stats.completed_batches),
            );
            x.counter(
                "timeless_metrics_failed_batches_total",
                "Batches that failed to write.",
                clamp(stats.failed_batches),
            );
            x.counter(
                "timeless_metrics_import_errors_total",
                "Import requests rejected as unparseable.",
                clamp(stats.import_errors),
            );
            x.gauge(
                "timeless_metrics_series",
                "Distinct series tracked.",
                stats.series,
            );
            x.gauge(
                "timeless_metrics_disk_points",
                "Points resident in storage.",
                stats.disk_points,
            );
            x.gauge(
                "timeless_metrics_buffered_points",
                "Points buffered in memory ahead of flush.",
                stats.buffered_points,
            );
            x.gauge(
                "timeless_metrics_raw_tier_chunks",
                "Chunks in the raw retention tier.",
                stats.raw_tier_chunks,
            );
            x.gauge(
                "timeless_metrics_rollup_chunks",
                "Chunks in rollup tiers.",
                stats.rollup_chunks,
            );
            x.gauge(
                "timeless_metrics_storage_bytes",
                "Bytes of chunk payload on disk.",
                stats.bytes_on_disk,
            );
            x.gauge(
                "timeless_metrics_raw_ingested_bytes",
                "Raw bytes of all stored points at the standard 16 bytes per \
                 sample (8-byte timestamp + 8-byte value); the honest \
                 comparator for timeless_metrics_storage_bytes.",
                stats.raw_ingested_bytes,
            );
            x.gauge(
                "timeless_metrics_index_bytes",
                "SQLite index bytes beside the chunk payload; never part of \
                 a compression ratio.",
                stats.sqlite_index_bytes,
            );
            x.gauge(
                "timeless_metrics_queued_batches",
                "Batches waiting in the write queue.",
                clamp(stats.queued_batches),
            );
            x.gauge(
                "timeless_metrics_queued_points",
                "Points waiting in the write queue.",
                clamp(stats.queued_points),
            );
            x.gauge(
                "timeless_metrics_in_flight_batches",
                "Batches currently being written.",
                clamp(stats.in_flight_batches),
            );
            x.gauge(
                "timeless_metrics_in_flight_points",
                "Points currently being written.",
                clamp(stats.in_flight_points),
            );
            x.gauge(
                "timeless_metrics_oldest_queued_ms",
                "Age of the oldest queued batch in milliseconds.",
                clamp(stats.oldest_queued_ms),
            );
            x.gauge(
                "timeless_metrics_database_file_bytes",
                "Main SQLite database file size.",
                clamp(stats.database_file_bytes),
            );
            x.gauge(
                "timeless_metrics_wal_bytes",
                "SQLite write-ahead log size.",
                clamp(stats.database_wal_bytes),
            );
            x.gauge(
                "timeless_metrics_freelist_bytes",
                "Reusable free pages inside the database file.",
                stats.freelist_bytes,
            );
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
                x.finish(),
            )
                .into_response()
        }
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

async fn backup(State(storage): State<Storage>, Json(request): Json<BackupRequest>) -> Response {
    match storage.backup(request.destination).await {
        Ok(report) => (StatusCode::OK, Json(report)).into_response(),
        Err(error) => server_error(error),
    }
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn server_error(error: String) -> Response {
    // Storage and SQLite internals must not reach clients (table/TVF
    // names, file paths, busy-state detail): log server-side and return
    // a stable envelope. This crate has no tracing dependency; the
    // binary's stderr is the log sink.
    eprintln!("timeless-metrics-api: internal error: {error}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"status": "error", "error": "internal"})),
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

async fn unsupported() -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({"error": "unsupported_capability", "reason": "unsupported_route"})),
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
