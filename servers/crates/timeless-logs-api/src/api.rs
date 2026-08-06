use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::rejection::QueryRejection;
use axum::extract::{DefaultBodyLimit, Extension, Form, Query, State};
use axum::http::{header, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use timeless_api_common::{server_build_identity, BackupRequest, RESULT_ROWS_HEADER};

use crate::logsql::{self, LogsqlError, LogsqlErrorKind, LogsqlOutput, LogsqlPlan};
use crate::pipeline::{self, PipelineLimits};
use crate::storage::{LogEntry, QuerySpec, TimestampUnit};
use crate::{LogsQueryLimits, Storage};

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

pub fn router(storage: Storage) -> Router {
    router_with_limits(storage, LogsQueryLimits::default())
}

pub fn router_with_limits(storage: Storage, limits: LogsQueryLimits) -> Router {
    limits
        .validate()
        .expect("LogsQL router limits must be valid");
    Router::new()
        .route("/live", get(liveness))
        .route("/ready", get(health))
        .route("/health", get(health))
        .route("/insert/jsonline", post(ingest))
        .route("/select/logsql/query", get(query_get).post(query_post))
        .route("/select/logsql/field_values", get(field_values))
        .route("/select/logsql/stats", get(stats))
        .route("/api/v1/flush", get(flush))
        .route("/api/v1/backup", post(backup))
        .fallback(unsupported)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(Extension(limits))
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
#[serde(deny_unknown_fields)]
struct GetQuery {
    level: Option<String>,
    message: Option<String>,
    service: Option<String>,
    host: Option<String>,
    path: Option<String>,
    status: Option<String>,
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    order: Option<String>,
}

async fn query_get(
    State(storage): State<Storage>,
    Extension(limits): Extension<LogsQueryLimits>,
    query: Result<Query<GetQuery>, QueryRejection>,
) -> Response<Body> {
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return client_error("unsupported_query_parameters"),
    };
    let limit_explicit = query.limit.is_some();
    let mut spec = get_query_spec(query, storage.timestamp_unit());
    if let Err((reason, limit)) = apply_query_limits(&mut spec, limit_explicit, limits) {
        return query_limit_error(reason, limit);
    }
    query_response(&storage, spec, limits, limits.deadline).await
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FieldValuesQuery {
    field: String,
    level: Option<String>,
    message: Option<String>,
    service: Option<String>,
    host: Option<String>,
    path: Option<String>,
    status: Option<String>,
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
}

async fn field_values(
    State(storage): State<Storage>,
    Extension(limits): Extension<LogsQueryLimits>,
    query: Result<Query<FieldValuesQuery>, QueryRejection>,
) -> Response<Body> {
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return client_error("unsupported_query_parameters"),
    };
    if !matches!(query.field.as_str(), "service" | "host" | "path" | "status") {
        return client_error("unsupported_log_field").into_response();
    }
    let limit = query.limit.unwrap_or(1_000).min(if query.limit.is_some() {
        usize::MAX
    } else {
        limits.max_result_rows
    });
    if limit > limits.max_result_rows {
        return query_limit_error("max_result_rows", limits.max_result_rows);
    }
    let spec = QuerySpec {
        level: query.level,
        service: query.service,
        metadata_eq: metadata_filters(query.host, query.path, query.status),
        metadata_exact: Vec::new(),
        message: query.message,
        message_phrase: None,
        predicate: None,
        ts_min: query
            .start
            .as_deref()
            .and_then(|value| parse_query_time(value, storage.timestamp_unit())),
        ts_max: query
            .end
            .as_deref()
            .and_then(|value| parse_query_time(value, storage.timestamp_unit())),
        limit,
        max_work_rows: limits.max_work_rows,
        ..QuerySpec::default()
    };
    match tokio::time::timeout(
        limits.deadline,
        storage.field_values(spec, query.field, limit),
    )
    .await
    {
        Err(_) => timeout_error(limits.deadline),
        Ok(Err(error)) => query_execution_error(error),
        Ok(Ok(values)) => {
            let row_count = values.len();
            match bounded_json(&json!({"values": values}), limits.max_response_bytes) {
                Ok(body) => {
                    storage.record_query_response_bytes(body.len());
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(RESULT_ROWS_HEADER, row_count)
                        .body(Body::from(body))
                        .unwrap()
                }
                Err(BoundedJsonError::Limit) => {
                    query_limit_error("max_response_bytes", limits.max_response_bytes)
                }
                Err(BoundedJsonError::Encode(error)) => server_error(error),
            }
        }
    }
}

#[derive(Deserialize)]
struct QueryForm {
    query: Option<String>,
}

struct QueryBackedResolution {
    remaining_work_rows: usize,
    remaining_state_bytes: usize,
    values: BTreeMap<String, Vec<String>>,
    limits: LogsQueryLimits,
}

type ResolveFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

fn resolve_query_backed_plan<'a>(
    storage: &'a Storage,
    plan: &'a mut LogsqlPlan,
    resolution: &'a mut QueryBackedResolution,
) -> ResolveFuture<'a> {
    Box::pin(async move {
        if let Some(predicate) = &mut plan.spec.predicate {
            resolve_query_backed_predicate(storage, predicate, resolution).await?;
        }
        for operation in &mut plan.pipeline {
            match operation {
                crate::logsql::PipelineOp::Filter(predicate) => {
                    resolve_query_backed_predicate(storage, predicate, resolution).await?;
                }
                crate::logsql::PipelineOp::Format(spec) => {
                    if let Some(predicate) = &mut spec.condition {
                        resolve_query_backed_predicate(storage, predicate, resolution).await?;
                    }
                }
                crate::logsql::PipelineOp::Replace(spec) => {
                    if let Some(predicate) = &mut spec.condition {
                        resolve_query_backed_predicate(storage, predicate, resolution).await?;
                    }
                }
                crate::logsql::PipelineOp::ReplaceRegexp(spec) => {
                    if let Some(predicate) = &mut spec.condition {
                        resolve_query_backed_predicate(storage, predicate, resolution).await?;
                    }
                }
                crate::logsql::PipelineOp::Extract(spec) => {
                    if let Some(predicate) = &mut spec.condition {
                        resolve_query_backed_predicate(storage, predicate, resolution).await?;
                    }
                }
                crate::logsql::PipelineOp::ExtractRegexp(spec) => {
                    if let Some(predicate) = &mut spec.condition {
                        resolve_query_backed_predicate(storage, predicate, resolution).await?;
                    }
                }
                crate::logsql::PipelineOp::UnpackJson(spec) => {
                    if let Some(predicate) = &mut spec.condition {
                        resolve_query_backed_predicate(storage, predicate, resolution).await?;
                    }
                }
                crate::logsql::PipelineOp::UnpackLogfmt(spec) => {
                    if let Some(predicate) = &mut spec.condition {
                        resolve_query_backed_predicate(storage, predicate, resolution).await?;
                    }
                }
                crate::logsql::PipelineOp::UnpackSyslog(spec) => {
                    if let Some(predicate) = &mut spec.condition {
                        resolve_query_backed_predicate(storage, predicate, resolution).await?;
                    }
                }
                _ => {}
            }
        }
        if resolution.remaining_work_rows == 0 {
            return Err(format!(
                "LogsQL query-backed composition exceeded max_work_rows={}",
                resolution.limits.max_work_rows
            ));
        }
        plan.spec.max_work_rows = plan.spec.max_work_rows.min(resolution.remaining_work_rows);
        Ok(())
    })
}

fn resolve_query_backed_predicate<'a>(
    storage: &'a Storage,
    predicate: &'a mut crate::LogPredicate,
    resolution: &'a mut QueryBackedResolution,
) -> ResolveFuture<'a> {
    Box::pin(async move {
        match predicate {
            crate::LogPredicate::And(predicates) | crate::LogPredicate::Or(predicates) => {
                for predicate in predicates {
                    resolve_query_backed_predicate(storage, predicate, resolution).await?;
                }
            }
            crate::LogPredicate::Not(predicate) => {
                resolve_query_backed_predicate(storage, predicate, resolution).await?;
            }
            crate::LogPredicate::QueryBackedTextualIn {
                field,
                query,
                output_path,
                cache_key,
            } => {
                let values =
                    resolve_query_backed_values(storage, query, output_path, cache_key, resolution)
                        .await?;
                *predicate = crate::LogPredicate::TextualIn {
                    field: field.clone(),
                    values,
                };
            }
            crate::LogPredicate::QueryBackedTextualContainsAll {
                field,
                query,
                output_path,
                cache_key,
            } => {
                let mut values =
                    resolve_query_backed_values(storage, query, output_path, cache_key, resolution)
                        .await?;
                values.retain(|value| !value.is_empty());
                *predicate = if values.is_empty() {
                    crate::LogPredicate::True
                } else {
                    crate::LogPredicate::TextualContainsAll {
                        field: field.clone(),
                        values,
                    }
                };
            }
            crate::LogPredicate::QueryBackedTextualContainsAny {
                field,
                query,
                output_path,
                cache_key,
            } => {
                let values =
                    resolve_query_backed_values(storage, query, output_path, cache_key, resolution)
                        .await?;
                *predicate = if values.iter().any(String::is_empty) {
                    crate::LogPredicate::True
                } else if values.is_empty() {
                    crate::LogPredicate::Or(Vec::new())
                } else {
                    crate::LogPredicate::TextualContainsAny {
                        field: field.clone(),
                        values,
                    }
                };
            }
            _ => {}
        }
        Ok(())
    })
}

async fn resolve_query_backed_values(
    storage: &Storage,
    query: &mut LogsqlPlan,
    output_path: &[String],
    cache_key: &str,
    resolution: &mut QueryBackedResolution,
) -> Result<Vec<String>, String> {
    if let Some(values) = resolution.values.get(cache_key) {
        let state_bytes = query_backed_values_state_bytes(values)?;
        if state_bytes > resolution.remaining_state_bytes {
            return Err(format!(
                "LogsQL query-backed lists exceed max_response_bytes={}",
                resolution.limits.max_response_bytes
            ));
        }
        resolution.remaining_state_bytes -= state_bytes;
        return Ok(values.clone());
    }
    resolve_query_backed_plan(storage, query, resolution).await?;
    if query.output != LogsqlOutput::Pipeline {
        return Err("LogsQL query-backed list did not compile to a row pipeline".into());
    }
    let limits = resolution.limits;
    let rate_window_seconds = rate_window_seconds(&query.spec, storage.timestamp_unit());
    let (rows, report) = storage
        .pipeline_with_report(
            query.spec.clone(),
            query.pipeline.clone(),
            None,
            rate_window_seconds,
            PipelineLimits {
                max_result_rows: limits.max_result_rows,
                max_state_items: resolution.remaining_work_rows,
                max_state_bytes: limits.max_response_bytes,
            },
        )
        .await?;
    let physical_work = usize::try_from(report.processed_entries)
        .unwrap_or(usize::MAX)
        .max(rows.len());
    if physical_work > resolution.remaining_work_rows {
        return Err(format!(
            "LogsQL query-backed composition exceeded max_work_rows={}",
            limits.max_work_rows
        ));
    }
    resolution.remaining_work_rows -= physical_work;

    let mut unique = BTreeSet::new();
    let mut state_bytes = 0usize;
    for (index, row) in rows.iter().enumerate() {
        if index % 1_024 == 0 {
            tokio::task::yield_now().await;
        }
        let value = crate::pipeline::projected_text(crate::pipeline::field_value(row, output_path));
        if unique.contains(value.as_ref()) {
            continue;
        }
        state_bytes = state_bytes
            .checked_add(value.len())
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or_else(|| "LogsQL query-backed list state size overflow".to_string())?;
        if state_bytes > resolution.remaining_state_bytes {
            return Err(format!(
                "LogsQL query-backed lists exceed max_response_bytes={}",
                limits.max_response_bytes
            ));
        }
        if unique.len() >= limits.max_result_rows {
            return Err(format!(
                "LogsQL query-backed list exceeds max_result_rows={}",
                limits.max_result_rows
            ));
        }
        unique.insert(value.into_owned());
    }
    let values = unique.into_iter().collect::<Vec<_>>();
    // The cache owns one copy and the resolved predicate owns another. Charge
    // both at the first use, then charge each later predicate copy on a cache
    // hit. This keeps repeated equivalent subqueries computationally cheap
    // without allowing their materialized values to evade the request bound.
    let cache_bytes = cache_key
        .len()
        .checked_add(64)
        .ok_or_else(|| "LogsQL query-backed list state size overflow".to_string())?;
    let retained_bytes = state_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(cache_bytes))
        .ok_or_else(|| "LogsQL query-backed list state size overflow".to_string())?;
    if retained_bytes > resolution.remaining_state_bytes {
        return Err(format!(
            "LogsQL query-backed lists exceed max_response_bytes={}",
            limits.max_response_bytes
        ));
    }
    resolution.remaining_state_bytes -= retained_bytes;
    resolution
        .values
        .insert(cache_key.to_owned(), values.clone());
    Ok(values)
}

fn query_backed_values_state_bytes(values: &[String]) -> Result<usize, String> {
    values.iter().try_fold(0usize, |bytes, value| {
        bytes
            .checked_add(value.len())
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or_else(|| "LogsQL query-backed list state size overflow".to_string())
    })
}

async fn query_post(
    State(storage): State<Storage>,
    Extension(limits): Extension<LogsQueryLimits>,
    Form(form): Form<QueryForm>,
) -> impl IntoResponse {
    let Some(query) = form.query.as_deref() else {
        return logsql_error(LogsqlError {
            kind: LogsqlErrorKind::Malformed,
            message: "LogsQL query parameter is required".into(),
        });
    };
    let deadline = tokio::time::Instant::now() + limits.deadline;
    let mut plan = match logsql::parse(query, storage.timestamp_unit()) {
        Ok(parsed) => parsed,
        Err(error) => return logsql_error(error),
    };
    if let Err((reason, limit)) = apply_plan_limits(&mut plan, limits) {
        return query_limit_error(reason, limit);
    }
    let mut resolution = QueryBackedResolution {
        remaining_work_rows: limits.max_work_rows,
        remaining_state_bytes: limits.max_response_bytes,
        values: BTreeMap::new(),
        limits,
    };
    match tokio::time::timeout_at(
        deadline,
        resolve_query_backed_plan(&storage, &mut plan, &mut resolution),
    )
    .await
    {
        Err(_) => return timeout_error(limits.deadline),
        Ok(Err(error)) => return query_execution_error(error),
        Ok(Ok(())) => {}
    }
    // The plan now owns the bounded static values. Release the request-local
    // deduplication cache before the outer scan to minimize peak overlap.
    drop(resolution);
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return timeout_error(limits.deadline);
    }
    let execution_limits = LogsQueryLimits {
        deadline: remaining,
        ..limits
    };
    if plan.output == LogsqlOutput::Count {
        match tokio::time::timeout(execution_limits.deadline, storage.count(plan.spec)).await {
            Err(_) => timeout_error(limits.deadline),
            Ok(Err(error)) => query_execution_error(error),
            Ok(Ok(total)) => {
                match bounded_json_line(
                    &json!({"total": total}),
                    execution_limits.max_response_bytes,
                ) {
                    Ok(body) => {
                        storage.record_query_response_bytes(body.len());
                        ndjson_response(body, 1)
                    }
                    Err(BoundedJsonError::Limit) => {
                        query_limit_error("max_response_bytes", execution_limits.max_response_bytes)
                    }
                    Err(BoundedJsonError::Encode(error)) => server_error(error),
                }
            }
        }
    } else if plan.output == LogsqlOutput::Pipeline {
        pipeline_response(&storage, plan, execution_limits, limits.deadline).await
    } else {
        query_response(&storage, plan.spec, execution_limits, limits.deadline).await
    }
}

async fn pipeline_response(
    storage: &Storage,
    plan: LogsqlPlan,
    limits: LogsQueryLimits,
    reported_deadline: Duration,
) -> Response<Body> {
    let rate_window_seconds = rate_window_seconds(&plan.spec, storage.timestamp_unit());
    let rows = match tokio::time::timeout(
        limits.deadline,
        storage.pipeline(
            plan.spec,
            plan.pipeline,
            plan.implicit_result_limit,
            rate_window_seconds,
            PipelineLimits {
                max_result_rows: limits.max_result_rows,
                max_state_items: limits.max_work_rows,
                max_state_bytes: limits.max_response_bytes,
            },
        ),
    )
    .await
    {
        Err(_) => return timeout_error(reported_deadline),
        Ok(Err(error)) => return query_execution_error(error),
        Ok(Ok(rows)) => rows,
    };
    let row_count = rows.len();
    let mut body = BoundedBuffer::new(limits.max_response_bytes);
    for row in rows {
        if serde_json::to_writer(&mut body, &row).is_err() || body.write_all(b"\n").is_err() {
            return if body.exceeded {
                query_limit_error("max_response_bytes", limits.max_response_bytes)
            } else {
                server_error("encode LogsQL pipeline response".into())
            };
        }
    }
    let body = body.into_inner();
    storage.record_query_response_bytes(body.len());
    ndjson_response(body, row_count)
}

fn rate_window_seconds(spec: &QuerySpec, timestamp_unit: TimestampUnit) -> Option<f64> {
    let (minimum, maximum) = (spec.ts_min?, spec.ts_max?);
    if maximum < minimum {
        return Some(0.0);
    }
    let native_width = maximum.checked_sub(minimum)?.checked_add(1)?;
    Some(match timestamp_unit {
        TimestampUnit::Milliseconds => native_width as f64 / 1_000.0,
        TimestampUnit::Microseconds => native_width as f64 / 1_000_000.0,
    })
}

async fn query_response(
    storage: &Storage,
    spec: QuerySpec,
    limits: LogsQueryLimits,
    reported_deadline: Duration,
) -> Response<Body> {
    match tokio::time::timeout(limits.deadline, storage.query(spec)).await {
        Err(_) => timeout_error(reported_deadline),
        Ok(Err(error)) => query_execution_error(error),
        Ok(Ok(rows)) => {
            let row_count = rows.len();
            let mut body = BoundedBuffer::new(limits.max_response_bytes);
            for row in rows {
                match pipeline::response_row(row, storage.timestamp_unit()).and_then(|value| {
                    serde_json::to_writer(&mut body, &value)
                        .map_err(|error| format!("encode result row: {error}"))?;
                    body.write_all(b"\n")
                        .map_err(|error| format!("encode result row: {error}"))
                }) {
                    Ok(()) => {}
                    Err(_) if body.exceeded => {
                        return query_limit_error("max_response_bytes", limits.max_response_bytes)
                    }
                    Err(error) => return server_error(error),
                }
            }
            let body = body.into_inner();
            storage.record_query_response_bytes(body.len());
            ndjson_response(body, row_count)
        }
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

async fn backup(
    State(storage): State<Storage>,
    Json(request): Json<BackupRequest>,
) -> impl IntoResponse {
    match storage.backup(request.destination).await {
        Ok(report) => (StatusCode::OK, Json(report)).into_response(),
        Err(error) => server_error(error),
    }
}

fn ndjson_response(body: Vec<u8>, rows: usize) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header(RESULT_ROWS_HEADER, rows)
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

fn client_error(code: &str) -> Response<Body> {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({"error": "unsupported_capability", "reason": code})),
    )
        .into_response()
}

fn logsql_error(error: LogsqlError) -> Response<Body> {
    match error.kind {
        LogsqlErrorKind::Malformed => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid_query",
                "reason": "malformed_logsql",
                "message": error.message
            })),
        )
            .into_response(),
        LogsqlErrorKind::Unsupported => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "unsupported_capability",
                "reason": "unsupported_logsql",
                "message": error.message
            })),
        )
            .into_response(),
    }
}

fn query_limit_error(reason: &str, limit: usize) -> Response<Body> {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({
            "error": "query_limit",
            "reason": reason,
            "limit": limit
        })),
    )
        .into_response()
}

fn timeout_error(reported_deadline: Duration) -> Response<Body> {
    (
        StatusCode::GATEWAY_TIMEOUT,
        Json(json!({
            "error": "timeout",
            "reason": "query_deadline",
            "deadline_ms": reported_deadline.as_millis()
        })),
    )
        .into_response()
}

fn query_execution_error(error: String) -> Response<Body> {
    if error.starts_with("LogsQL coalesce destination conflict:")
        || error.starts_with("LogsQL copy destination conflict:")
        || error.starts_with("LogsQL rename destination conflict:")
        || error.starts_with("LogsQL format destination conflict:")
        || error.starts_with("LogsQL math destination conflict:")
        || error.starts_with("LogsQL len destination conflict:")
        || error.starts_with("LogsQL hash destination conflict:")
        || error.starts_with("LogsQL json_array_len destination conflict:")
        || error.starts_with("LogsQL split destination conflict:")
        || error.starts_with("LogsQL extract destination conflict:")
        || error.starts_with("LogsQL extract_regexp destination conflict:")
        || error.starts_with("LogsQL pack_json destination conflict:")
        || error.starts_with("LogsQL pack_json field selection conflict:")
        || error.starts_with("LogsQL pack_logfmt destination conflict:")
        || error.starts_with("LogsQL unpack_json destination conflict:")
        || error.starts_with("LogsQL unpack_json field selection conflict:")
        || error.starts_with("LogsQL unpack_logfmt destination conflict:")
        || error.starts_with("LogsQL unpack_syslog destination conflict:")
        || error.starts_with("LogsQL unpack_words destination conflict:")
    {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "query_execution",
                "reason": "field_conflict",
                "message": error
            })),
        )
            .into_response();
    }
    if let Some(limit) = error
        .split("max_result_rows=")
        .nth(1)
        .and_then(|value| {
            value
                .split(|character: char| !character.is_ascii_digit())
                .next()
        })
        .and_then(|value| value.parse::<usize>().ok())
    {
        return query_limit_error("max_result_rows", limit);
    }
    if let Some(limit) = error
        .split("max_work_entries=")
        .nth(1)
        .and_then(|value| {
            value
                .split(|character: char| !character.is_ascii_digit())
                .next()
        })
        .and_then(|value| value.parse::<usize>().ok())
    {
        return query_limit_error("max_work_rows", limit);
    }
    if let Some(limit) = error
        .split("max_work_rows=")
        .nth(1)
        .and_then(|value| {
            value
                .split(|character: char| !character.is_ascii_digit())
                .next()
        })
        .and_then(|value| value.parse::<usize>().ok())
    {
        return query_limit_error("max_work_rows", limit);
    }
    if let Some(limit) = error
        .split("max_response_bytes=")
        .nth(1)
        .and_then(|value| {
            value
                .split(|character: char| !character.is_ascii_digit())
                .next()
        })
        .and_then(|value| value.parse::<usize>().ok())
    {
        return query_limit_error("max_response_bytes", limit);
    }
    server_error(error)
}

type QueryLimit = (&'static str, usize);

fn apply_plan_limits(plan: &mut LogsqlPlan, limits: LogsQueryLimits) -> Result<(), QueryLimit> {
    match plan.output {
        LogsqlOutput::Rows => {
            apply_query_limits(&mut plan.spec, plan.limit_explicit, limits)?;
        }
        LogsqlOutput::Count => plan.spec.max_work_rows = limits.max_work_rows,
        LogsqlOutput::Pipeline => {
            for operation in &plan.pipeline {
                match operation {
                    crate::logsql::PipelineOp::Limit(limit)
                    | crate::logsql::PipelineOp::FieldValues {
                        limit: Some(limit), ..
                    }
                    | crate::logsql::PipelineOp::First(crate::logsql::FirstSpec {
                        limit, ..
                    })
                    | crate::logsql::PipelineOp::Last(crate::logsql::FirstSpec { limit, .. })
                    | crate::logsql::PipelineOp::Top(crate::logsql::TopSpec { limit, .. })
                        if *limit > limits.max_result_rows =>
                    {
                        return Err(("max_result_rows", limits.max_result_rows));
                    }
                    crate::logsql::PipelineOp::Offset(offset) if *offset > limits.max_work_rows => {
                        return Err(("max_work_rows", limits.max_work_rows));
                    }
                    crate::logsql::PipelineOp::Stats(expressions) => {
                        for expression in expressions {
                            let Some(limit) = expression.limit else {
                                continue;
                            };
                            if matches!(
                                expression.kind,
                                crate::logsql::StatsKind::UniqValues
                                    | crate::logsql::StatsKind::Values
                            ) && limit > limits.max_result_rows
                            {
                                return Err(("max_result_rows", limits.max_result_rows));
                            }
                            if matches!(
                                expression.kind,
                                crate::logsql::StatsKind::CountUniq
                                    | crate::logsql::StatsKind::CountUniqHash
                            ) && limit > limits.max_work_rows
                            {
                                return Err(("max_work_rows", limits.max_work_rows));
                            }
                        }
                    }
                    _ => {}
                }
            }
            plan.spec.max_work_rows = limits.max_work_rows;
        }
    }
    if let Some(predicate) = &mut plan.spec.predicate {
        apply_query_backed_predicate_limits(predicate, limits)?;
    }
    for operation in &mut plan.pipeline {
        match operation {
            crate::logsql::PipelineOp::Filter(predicate) => {
                apply_query_backed_predicate_limits(predicate, limits)?;
            }
            crate::logsql::PipelineOp::Format(spec) => {
                if let Some(predicate) = &mut spec.condition {
                    apply_query_backed_predicate_limits(predicate, limits)?;
                }
            }
            crate::logsql::PipelineOp::Replace(spec) => {
                if let Some(predicate) = &mut spec.condition {
                    apply_query_backed_predicate_limits(predicate, limits)?;
                }
            }
            crate::logsql::PipelineOp::ReplaceRegexp(spec) => {
                if let Some(predicate) = &mut spec.condition {
                    apply_query_backed_predicate_limits(predicate, limits)?;
                }
            }
            crate::logsql::PipelineOp::Extract(spec) => {
                if let Some(predicate) = &mut spec.condition {
                    apply_query_backed_predicate_limits(predicate, limits)?;
                }
            }
            crate::logsql::PipelineOp::ExtractRegexp(spec) => {
                if let Some(predicate) = &mut spec.condition {
                    apply_query_backed_predicate_limits(predicate, limits)?;
                }
            }
            crate::logsql::PipelineOp::UnpackJson(spec) => {
                if let Some(predicate) = &mut spec.condition {
                    apply_query_backed_predicate_limits(predicate, limits)?;
                }
            }
            crate::logsql::PipelineOp::UnpackLogfmt(spec) => {
                if let Some(predicate) = &mut spec.condition {
                    apply_query_backed_predicate_limits(predicate, limits)?;
                }
            }
            crate::logsql::PipelineOp::UnpackSyslog(spec) => {
                if let Some(predicate) = &mut spec.condition {
                    apply_query_backed_predicate_limits(predicate, limits)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn apply_query_backed_predicate_limits(
    predicate: &mut crate::LogPredicate,
    limits: LogsQueryLimits,
) -> Result<(), QueryLimit> {
    match predicate {
        crate::LogPredicate::And(predicates) | crate::LogPredicate::Or(predicates) => {
            for predicate in predicates {
                apply_query_backed_predicate_limits(predicate, limits)?;
            }
        }
        crate::LogPredicate::Not(predicate) => {
            apply_query_backed_predicate_limits(predicate, limits)?;
        }
        crate::LogPredicate::QueryBackedTextualIn { query, .. }
        | crate::LogPredicate::QueryBackedTextualContainsAll { query, .. }
        | crate::LogPredicate::QueryBackedTextualContainsAny { query, .. } => {
            apply_plan_limits(query, limits)?;
        }
        _ => {}
    }
    Ok(())
}

fn apply_query_limits(
    spec: &mut QuerySpec,
    limit_explicit: bool,
    limits: LogsQueryLimits,
) -> Result<(), QueryLimit> {
    if spec.limit > limits.max_result_rows {
        if limit_explicit {
            return Err(("max_result_rows", limits.max_result_rows));
        }
        spec.limit = limits.max_result_rows;
    }
    let Some(window) = spec.offset.checked_add(spec.limit) else {
        return Err(("max_work_rows", limits.max_work_rows));
    };
    if window > limits.max_work_rows {
        return Err(("max_work_rows", limits.max_work_rows));
    }
    spec.max_work_rows = limits.max_work_rows;
    Ok(())
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedBuffer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(8 * 1024)),
            limit,
            exceeded: false,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("LogsQL response byte limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum BoundedJsonError {
    Limit,
    Encode(String),
}

fn bounded_json(value: &Value, limit: usize) -> Result<Vec<u8>, BoundedJsonError> {
    let mut body = BoundedBuffer::new(limit);
    if let Err(error) = serde_json::to_writer(&mut body, value) {
        return Err(if body.exceeded {
            BoundedJsonError::Limit
        } else {
            BoundedJsonError::Encode(format!("encode query response: {error}"))
        });
    }
    Ok(body.into_inner())
}

fn bounded_json_line(value: &Value, limit: usize) -> Result<Vec<u8>, BoundedJsonError> {
    let mut body = BoundedBuffer::new(limit);
    if let Err(error) = serde_json::to_writer(&mut body, value) {
        return Err(if body.exceeded {
            BoundedJsonError::Limit
        } else {
            BoundedJsonError::Encode(format!("encode query response: {error}"))
        });
    }
    if body.write_all(b"\n").is_err() {
        return Err(BoundedJsonError::Limit);
    }
    Ok(body.into_inner())
}

async fn unsupported() -> Response<Body> {
    client_error("unsupported_route")
}

fn get_query_spec(query: GetQuery, timestamp_unit: TimestampUnit) -> QuerySpec {
    QuerySpec {
        level: query.level,
        service: query.service,
        metadata_eq: metadata_filters(query.host, query.path, query.status),
        metadata_exact: Vec::new(),
        message: query.message,
        message_phrase: None,
        predicate: None,
        ts_min: query
            .start
            .as_deref()
            .and_then(|value| parse_query_time(value, timestamp_unit)),
        ts_max: query
            .end
            .as_deref()
            .and_then(|value| parse_query_time(value, timestamp_unit)),
        limit: query.limit.unwrap_or(100),
        offset: query.offset.unwrap_or(0),
        descending: query.order.as_deref() != Some("asc"),
        max_work_rows: QuerySpec::default().max_work_rows,
    }
}

fn metadata_filters(
    host: Option<String>,
    path: Option<String>,
    status: Option<String>,
) -> BTreeMap<String, String> {
    [("host", host), ("path", path), ("status", status)]
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key.to_owned(), value)))
        .collect()
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

fn now(timestamp_unit: TimestampUnit) -> i64 {
    micros_to_native(Utc::now().timestamp_micros(), timestamp_unit)
}

fn micros_to_native(micros: i64, timestamp_unit: TimestampUnit) -> i64 {
    match timestamp_unit {
        TimestampUnit::Milliseconds => micros / 1_000,
        TimestampUnit::Microseconds => micros,
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
}
