use std::collections::BTreeMap;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Form, Query, State};
use axum::http::{header, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::storage::{LogEntry, QueryRow, QuerySpec};
use crate::Storage;

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/insert/jsonline", post(ingest))
        .route("/select/logsql/query", get(query_get).post(query_post))
        .route("/select/logsql/stats", get(stats))
        .route("/api/v1/flush", get(flush))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(storage)
}

async fn health(State(storage): State<Storage>) -> impl IntoResponse {
    match storage.stats().await {
        Ok(stats) => (
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "blocks": stats.total_blocks,
                "entries": stats.total_entries,
                "disk_size": stats.disk_size,
                "queued_entries": stats.queued_entries
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
    let (entries, errors) = parse_ndjson(&body, message_field, time_field);
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
        ts_min: query.start.as_deref().and_then(parse_query_time),
        ts_max: query.end.as_deref().and_then(parse_query_time),
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
    let (spec, count) = parse_logsql(form.query.as_deref().unwrap_or("*"));
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
                match response_row(row) {
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

fn parse_ndjson(body: &str, message_field: &str, time_field: &str) -> (Vec<LogEntry>, usize) {
    let mut entries = Vec::new();
    let mut errors = 0;
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        match parse_line(line, message_field, time_field) {
            Ok(entry) => entries.push(entry),
            Err(()) => errors += 1,
        }
    }
    (entries, errors)
}

fn parse_line(line: &str, message_field: &str, time_field: &str) -> Result<LogEntry, ()> {
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
        .map(parse_ingest_time)
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    let level = object
        .remove("level")
        .map(value_to_string)
        .map(|level| level_number(&level))
        .unwrap_or(1);

    // The established extension metadata contract is a flat object of
    // strings. Preserve every producer field by stringifying scalar values;
    // nested JSON stays available as its compact JSON representation.
    let metadata: BTreeMap<String, String> = object
        .into_iter()
        .map(|(key, value)| (key, value_to_string(value)))
        .collect();
    let metadata_json = serde_json::to_string(&metadata).map_err(|_| ())?;
    Ok(LogEntry {
        ts,
        level,
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
        "warning" | "warn" => 2,
        "error" => 3,
        _ => 1,
    }
}

fn parse_ingest_time(value: &Value) -> i64 {
    match value {
        Value::Number(number) => number
            .as_i64()
            .map(normalize_integer_time)
            .unwrap_or_else(|| Utc::now().timestamp_millis()),
        Value::String(value) => value
            .parse::<i64>()
            .map(|seconds| seconds.saturating_mul(1_000))
            .or_else(|_| DateTime::parse_from_rfc3339(value).map(|dt| dt.timestamp_millis()))
            .unwrap_or_else(|_| Utc::now().timestamp_millis()),
        _ => Utc::now().timestamp_millis(),
    }
}

fn normalize_integer_time(ts: i64) -> i64 {
    let magnitude = ts.unsigned_abs();
    if magnitude < 100_000_000_000 {
        ts.saturating_mul(1_000)
    } else if magnitude < 100_000_000_000_000 {
        ts
    } else if magnitude < 100_000_000_000_000_000 {
        ts / 1_000
    } else {
        ts / 1_000_000
    }
}

fn parse_query_time(value: &str) -> Option<i64> {
    value
        .parse::<i64>()
        .ok()
        .map(normalize_integer_time)
        .or_else(|| {
            DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|dt| dt.timestamp_millis())
        })
}

fn parse_logsql(query: &str) -> (QuerySpec, bool) {
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
            spec.ts_min = Some(Utc::now().timestamp_millis().saturating_sub(duration_ms));
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

fn response_row(row: QueryRow) -> Result<String, String> {
    let metadata: Map<String, Value> = serde_json::from_str(&row.metadata_json)
        .map_err(|e| format!("decode stored metadata: {e}"))?;
    let mut object = metadata;
    object.insert("_time".into(), Value::String(format_timestamp(row.ts)?));
    object.insert("_msg".into(), Value::String(row.message));
    object.insert("level".into(), Value::String(row.level));
    serde_json::to_string(&Value::Object(object)).map_err(|e| format!("encode result row: {e}"))
}

fn format_timestamp(ts_ms: i64) -> Result<String, String> {
    DateTime::<Utc>::from_timestamp_millis(ts_ms)
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true))
        .ok_or_else(|| format!("timestamp {ts_ms} is outside the RFC3339 range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndjson_keeps_batches_and_stringifies_metadata_for_the_flat_contract() {
        let body =
            "{\"_time\":1700000000,\"_msg\":\"hello\",\"level\":\"warn\",\"status\":500}\ninvalid";
        let (entries, errors) = parse_ndjson(body, "_msg", "_time");
        assert_eq!(errors, 1);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].ts, 1_700_000_000_000);
        assert_eq!(entries[0].level, 2);
        assert_eq!(entries[0].metadata_json, "{\"status\":\"500\"}");
    }

    #[test]
    fn workload_logsql_shapes_parse() {
        let (spec, count) = parse_logsql("_time:5m level:error | limit 100");
        assert_eq!(spec.level.as_deref(), Some("error"));
        assert_eq!(spec.limit, 100);
        assert!(spec.ts_min.is_some());
        assert!(!count);

        let (spec, count) = parse_logsql("_time:1h level:error | stats count(*)");
        assert_eq!(spec.level.as_deref(), Some("error"));
        assert!(count);

        let (spec, _) = parse_logsql("_time:15m \"timeout\" | limit 50");
        assert_eq!(spec.message.as_deref(), Some("timeout"));
    }
}
