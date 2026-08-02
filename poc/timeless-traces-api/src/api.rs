use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::Storage;

pub const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/live", get(liveness))
        .route("/ready", get(readiness))
        .route("/health", get(readiness))
        .route("/select/traces/stats", get(stats))
        .route("/api/v1/flush", post(flush))
        // Session 2 deliberately reserves the established route while making
        // its absence explicit. Session 3 replaces this handler with OTLP.
        .route(
            "/insert/opentelemetry/v1/traces",
            post(otlp_not_implemented),
        )
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
                "admitted_requests": stats.admitted_requests,
                "completed_requests": stats.completed_requests,
                "admitted_spans": stats.admitted_spans,
                "completed_spans": stats.completed_spans,
                "queued_requests": stats.queued_requests,
                "in_flight_requests": stats.in_flight_requests
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

async fn flush(State(storage): State<Storage>) -> Response {
    let started = Instant::now();
    match storage.flush().await {
        Ok(mut report) => {
            report.api_request_ns = duration_ns(started.elapsed());
            (StatusCode::OK, Json(report)).into_response()
        }
        Err(error) => server_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

async fn otlp_not_implemented(_body: Bytes) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "status": "error",
            "error": "OTLP ingest is introduced in traces POC Session 3"
        })),
    )
        .into_response()
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn server_error(status: StatusCode, error: String) -> Response {
    (status, Json(json!({"status": "error", "error": error}))).into_response()
}
