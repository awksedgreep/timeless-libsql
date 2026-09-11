//! Stable error envelopes for Timeless-native HTTP routes.
//!
//! Compatibility surfaces keep their upstream protocols: Prometheus and
//! MetricsQL use `status`/`errorType`/`error`, Jaeger keeps its established
//! response body, OTLP keeps its collector contract, and Victoria-compatible
//! ingest keeps its existing shape. Timeless-native query, discovery,
//! administration, and maintenance failures expose machine-readable `error`
//! and `reason` codes; a safe human-readable `message` is optional. Internal
//! details are logged only.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

pub fn native_error(status: StatusCode, error: &'static str, reason: &'static str) -> Response {
    (status, Json(json!({"error": error, "reason": reason}))).into_response()
}

pub fn native_error_with_message(
    status: StatusCode,
    error: &'static str,
    reason: &'static str,
    message: String,
) -> Response {
    (
        status,
        Json(json!({
            "error": error,
            "reason": reason,
            "message": message
        })),
    )
        .into_response()
}

/// Log sensitive implementation detail and return only stable native codes.
pub fn native_internal_error(
    signal: &'static str,
    reason: &'static str,
    detail: String,
) -> Response {
    eprintln!("timeless-{signal}-api: internal error ({reason}): {detail}");
    native_error(StatusCode::INTERNAL_SERVER_ERROR, "internal", reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use serde_json::Value;

    async fn body(response: Response) -> Value {
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn native_envelope_requires_stable_error_and_reason_codes() {
        assert_eq!(
            body(native_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "temporarily_unavailable",
                "storage_busy"
            ))
            .await,
            json!({"error": "temporarily_unavailable", "reason": "storage_busy"})
        );
        assert_eq!(
            body(native_error_with_message(
                StatusCode::BAD_REQUEST,
                "invalid_query",
                "query_validation",
                "bad selector".into()
            ))
            .await,
            json!({
                "error": "invalid_query",
                "reason": "query_validation",
                "message": "bad selector"
            })
        );
    }

    #[tokio::test]
    async fn internal_envelope_never_returns_the_logged_detail() {
        let response = native_internal_error(
            "test",
            "query_execution",
            "/secret/customer.db is corrupt".into(),
        );
        assert_eq!(
            body(response).await,
            json!({"error": "internal", "reason": "query_execution"})
        );
    }
}
