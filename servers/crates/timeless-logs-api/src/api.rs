use std::time::Instant;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Form, Query, State};
use axum::http::{header, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use timeless_api_common::server_build_identity;

use crate::storage::{LogEntry, QueryRow, QuerySpec, TimestampUnit};
use crate::Storage;

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/live", get(liveness))
        .route("/ready", get(health))
        .route("/health", get(health))
        .route("/insert/jsonline", post(ingest))
        .route("/select/logsql/query", get(query_get).post(query_post))
        .route("/select/logsql/stats", get(stats))
        .route("/api/v1/flush", get(flush))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(storage)
}

async fn liveness() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "alive"})))
}

async fn health(State(storage): State<Storage>) -> impl IntoResponse {
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
                "build": server_build_identity("logs"),
                "blocks": stats.total_blocks,
                "entries": stats.total_entries,
                "disk_size": stats.disk_size,
                "buffered_entries": stats.buffered_entries,
                "queued_batches": stats.queued_batches,
                "queued_entries": stats.queued_entries,
                "oldest_queued_ms": stats.oldest_queued_ms,
                "admitted_entries": stats.admitted_entries,
                "completed_entries": stats.completed_entries
            })),
        )
            .into_response(),
        Err(error) => server_error(error),
    }
}

#[derive(Deserialize, Default)]
struct IngestParams {
    #[serde(rename = "_msg_field")]
    message_field: Option<String>,
    #[serde(rename = "_time_field")]
    time_field: Option<String>,
}

async fn ingest(
    State(storage): State<Storage>,
    Query(params): Query<IngestParams>,
    body: String,
) -> impl IntoResponse {
    let message_field = params.message_field.as_deref().unwrap_or("_msg");
    let time_field = params.time_field.as_deref().unwrap_or("_time");
    let parse_started = Instant::now();
    let (entries, errors) =
        parse_ndjson(&body, message_field, time_field, storage.timestamp_unit());
    storage.record_parse(parse_started.elapsed());
    match storage.ingest(entries).await {
        Ok(_count) if errors == 0 => StatusCode::NO_CONTENT.into_response(),
        Ok(count) => (
            StatusCode::OK,
            Json(json!({"entries": count, "errors": errors})),
        )
            .into_response(),
        Err(error) => server_error(error),
    }
}

#[derive(Deserialize, Default)]
struct GetQuery {
    level: Option<String>,
    message: Option<String>,
    service: Option<String>,
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    order: Option<String>,
}

async fn query_get(
    State(storage): State<Storage>,
    Query(query): Query<GetQuery>,
) -> impl IntoResponse {
    let spec = QuerySpec {
        level: query.level,
        service: query.service,
        message: query.message,
        ts_min: query
            .start
            .as_deref()
            .and_then(|value| parse_query_time(value, storage.timestamp_unit())),
        ts_max: query
            .end
            .as_deref()
            .and_then(|value| parse_query_time(value, storage.timestamp_unit())),
        limit: query.limit.unwrap_or(100),
        offset: query.offset.unwrap_or(0),
        descending: query.order.as_deref() != Some("asc"),
    };
    query_response(&storage, spec).await
}

#[derive(Deserialize)]
struct QueryForm {
    query: Option<String>,
}

async fn query_post(
    State(storage): State<Storage>,
    Form(form): Form<QueryForm>,
) -> impl IntoResponse {
    let (spec, count) = parse_logsql(
        form.query.as_deref().unwrap_or("*"),
        storage.timestamp_unit(),
    );
    if count {
        match storage.count(spec).await {
            Ok(total) => ndjson_response(format!("{}\n", json!({"total": total}))),
            Err(error) => server_error(error),
        }
    } else {
        query_response(&storage, spec).await
    }
}

async fn query_response(storage: &Storage, spec: QuerySpec) -> Response<Body> {
    match storage.query(spec).await {
        Ok(rows) => {
            let mut body = String::new();
            for row in rows {
                match response_row(row, storage.timestamp_unit()) {
                    Ok(line) => {
                        body.push_str(&line);
                        body.push('\n');
                    }
                    Err(error) => return server_error(error),
                }
            }
            ndjson_response(body)
        }
        Err(error) => server_error(error),
    }
}

async fn stats(State(storage): State<Storage>) -> impl IntoResponse {
    match storage.stats().await {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(error) => server_error(error),
    }
}

async fn flush(State(storage): State<Storage>) -> impl IntoResponse {
    match storage.flush().await {
        Ok(()) => (StatusCode::OK, Json(json!({"status": "ok"}))).into_response(),
        Err(error) => server_error(error),
    }
}

fn ndjson_response(body: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from(body))
        .unwrap()
}

fn server_error(error: String) -> Response<Body> {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": error})),
    )
        .into_response()
}

fn parse_ndjson(
    body: &str,
    message_field: &str,
    time_field: &str,
    timestamp_unit: TimestampUnit,
) -> (Vec<LogEntry>, usize) {
    let mut entries = Vec::new();
    let mut errors = 0;
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        match parse_line(line, message_field, time_field, timestamp_unit) {
            Ok(entry) => entries.push(entry),
            Err(()) => errors += 1,
        }
    }
    (entries, errors)
}

fn parse_line(
    line: &str,
    message_field: &str,
    time_field: &str,
    timestamp_unit: TimestampUnit,
) -> Result<LogEntry, ()> {
    let Value::Object(mut object) = serde_json::from_str::<Value>(line).map_err(|_| ())? else {
        return Err(());
    };
    let message = object
        .remove(message_field)
        .map(value_to_string)
        .unwrap_or_default();
    let ts = object
        .remove(time_field)
        .as_ref()
        .map(|value| parse_ingest_time(value, timestamp_unit))
        .unwrap_or_else(|| now(timestamp_unit));
    let severity = object
        .remove("level")
        .map(value_to_string)
        .map(|level| canonical_severity(&level))
        .unwrap_or("info");
    let level = level_number(severity);

    // Logs batch v1 preserves the canonical typed object. The extension
    // derives string projections only for equality/posting indexes.
    let metadata_json = serde_json::to_string(&object).map_err(|_| ())?;
    Ok(LogEntry {
        ts,
        level,
        severity: severity.into(),
        message,
        metadata_json,
    })
}

fn value_to_string(value: Value) -> String {
    match value {
        Value::String(value) => value,
        Value::Null => "null".into(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        value => serde_json::to_string(&value).unwrap_or_default(),
    }
}

fn level_number(level: &str) -> u8 {
    match level {
        "debug" => 0,
        "warning" => 2,
        "error" | "critical" | "alert" | "emergency" => 3,
        _ => 1,
    }
}

fn canonical_severity(level: &str) -> &'static str {
    match level {
        "debug" => "debug",
        "notice" => "notice",
        "warning" | "warn" => "warning",
        "error" => "error",
        "critical" => "critical",
        "alert" => "alert",
        "emergency" => "emergency",
        _ => "info",
    }
}

fn parse_ingest_time(value: &Value, timestamp_unit: TimestampUnit) -> i64 {
    match value {
        Value::Number(number) => number
            .as_i64()
            .map(|value| normalize_integer_time(value, timestamp_unit))
            .unwrap_or_else(|| now(timestamp_unit)),
        Value::String(value) => value
            .parse::<i64>()
            .map(|value| normalize_integer_time(value, timestamp_unit))
            .or_else(|_| {
                DateTime::parse_from_rfc3339(value)
                    .map(|dt| micros_to_native(dt.timestamp_micros(), timestamp_unit))
            })
            .unwrap_or_else(|_| now(timestamp_unit)),
        _ => now(timestamp_unit),
    }
}

fn normalize_integer_time(ts: i64, timestamp_unit: TimestampUnit) -> i64 {
    let magnitude = ts.unsigned_abs();
    let micros = if magnitude < 100_000_000_000 {
        ts.saturating_mul(1_000_000)
    } else if magnitude < 100_000_000_000_000 {
        ts.saturating_mul(1_000)
    } else if magnitude < 100_000_000_000_000_000 {
        ts
    } else {
        ts / 1_000
    };
    micros_to_native(micros, timestamp_unit)
}

fn parse_query_time(value: &str, timestamp_unit: TimestampUnit) -> Option<i64> {
    value
        .parse::<i64>()
        .ok()
        .map(|value| normalize_integer_time(value, timestamp_unit))
        .or_else(|| {
            DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|dt| micros_to_native(dt.timestamp_micros(), timestamp_unit))
        })
}

fn parse_logsql(query: &str, timestamp_unit: TimestampUnit) -> (QuerySpec, bool) {
    let mut spec = QuerySpec {
        limit: 100,
        descending: true,
        ..QuerySpec::default()
    };
    let mut count = false;
    let lower = query.to_ascii_lowercase();
    if lower.contains("stats count(") {
        count = true;
    }
    if let Some(level) = find_field(query, "level:") {
        spec.level = Some(level);
    }
    if let Some(service) = find_field(query, "service:") {
        spec.service = Some(service);
    }
    if let Some(window) = find_field(query, "_time:") {
        if let Some(duration_ms) = parse_duration_ms(&window) {
            spec.ts_min = Some(
                now(timestamp_unit)
                    .saturating_sub(duration_from_millis(duration_ms, timestamp_unit)),
            );
        }
    }
    if let Some(message) = first_quoted(query) {
        spec.message = Some(message);
    }
    if let Some(limit) = pipe_limit(query) {
        spec.limit = limit;
    }
    (spec, count)
}

fn find_field(query: &str, prefix: &str) -> Option<String> {
    query.split_whitespace().find_map(|token| {
        token
            .strip_prefix(prefix)
            .map(|value| value.trim_matches(['"', '\'', '|', ',']).to_string())
            .filter(|value| !value.is_empty())
    })
}

fn first_quoted(query: &str) -> Option<String> {
    let start = query.find('"')? + 1;
    let end = query[start..].find('"')? + start;
    Some(query[start..end].to_string())
}

fn pipe_limit(query: &str) -> Option<usize> {
    let tokens: Vec<&str> = query.split_whitespace().collect();
    tokens.windows(2).find_map(|pair| {
        if pair[0] == "limit" {
            pair[1].parse().ok()
        } else {
            None
        }
    })
}

fn parse_duration_ms(value: &str) -> Option<i64> {
    let split = value.find(|c: char| !c.is_ascii_digit())?;
    let count: i64 = value[..split].parse().ok()?;
    let unit = &value[split..];
    let multiplier = match unit {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    count.checked_mul(multiplier)
}

fn response_row(row: QueryRow, timestamp_unit: TimestampUnit) -> Result<String, String> {
    let metadata: Map<String, Value> = serde_json::from_str(&row.metadata_json)
        .map_err(|e| format!("decode stored metadata: {e}"))?;
    let mut object = metadata;
    object.insert(
        "_time".into(),
        Value::String(format_timestamp(row.ts, timestamp_unit)?),
    );
    object.insert("_msg".into(), Value::String(row.message));
    object.insert("level".into(), Value::String(row.level));
    serde_json::to_string(&Value::Object(object)).map_err(|e| format!("encode result row: {e}"))
}

fn format_timestamp(ts: i64, timestamp_unit: TimestampUnit) -> Result<String, String> {
    let (datetime, format) = match timestamp_unit {
        TimestampUnit::Milliseconds => (
            DateTime::<Utc>::from_timestamp_millis(ts),
            SecondsFormat::Millis,
        ),
        TimestampUnit::Microseconds => (
            DateTime::<Utc>::from_timestamp_micros(ts),
            SecondsFormat::Micros,
        ),
    };
    datetime
        .map(|dt| dt.to_rfc3339_opts(format, true))
        .ok_or_else(|| format!("timestamp {ts} is outside the RFC3339 range"))
}

fn now(timestamp_unit: TimestampUnit) -> i64 {
    micros_to_native(Utc::now().timestamp_micros(), timestamp_unit)
}

fn micros_to_native(micros: i64, timestamp_unit: TimestampUnit) -> i64 {
    match timestamp_unit {
        TimestampUnit::Milliseconds => micros / 1_000,
        TimestampUnit::Microseconds => micros,
    }
}

fn duration_from_millis(duration: i64, timestamp_unit: TimestampUnit) -> i64 {
    match timestamp_unit {
        TimestampUnit::Milliseconds => duration,
        TimestampUnit::Microseconds => duration.saturating_mul(1_000),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndjson_preserves_product_microseconds_severity_and_typed_metadata() {
        let body =
            "{\"_time\":1700000000,\"_msg\":\"hello\",\"level\":\"warn\",\"status\":500,\"context\":{\"retry\":true}}\ninvalid";
        let (entries, errors) = parse_ndjson(body, "_msg", "_time", TimestampUnit::Microseconds);
        assert_eq!(errors, 1);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].ts, 1_700_000_000_000_000);
        assert_eq!(entries[0].level, 2);
        assert_eq!(entries[0].severity, "warning");
        assert_eq!(
            entries[0].metadata_json,
            "{\"context\":{\"retry\":true},\"status\":500}"
        );
    }

    #[test]
    fn workload_logsql_shapes_parse() {
        let (spec, count) = parse_logsql(
            "_time:5m level:error | limit 100",
            TimestampUnit::Microseconds,
        );
        assert_eq!(spec.level.as_deref(), Some("error"));
        assert_eq!(spec.limit, 100);
        assert!(spec.ts_min.is_some());
        assert!(!count);

        let (spec, count) = parse_logsql(
            "_time:1h level:error | stats count(*)",
            TimestampUnit::Microseconds,
        );
        assert_eq!(spec.level.as_deref(), Some("error"));
        assert!(count);

        let (spec, _) = parse_logsql(
            "_time:15m \"timeout\" | limit 50",
            TimestampUnit::Microseconds,
        );
        assert_eq!(spec.message.as_deref(), Some("timeout"));
    }
}
