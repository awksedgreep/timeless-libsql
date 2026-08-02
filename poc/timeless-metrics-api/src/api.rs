use std::time::Instant;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::Storage;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/select/metrics/stats", get(stats))
        .route("/api/v1/flush", post(flush))
        // Session 1 has no request bodies. Keep a finite global default so a
        // later route cannot accidentally begin with unbounded allocation.
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(storage)
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
