//! Standalone Timeless telemetry data plane.
//!
//! This first vertical slice implements the VictoriaLogs-compatible logs API.
//! The process shell and bounded database command queue are signal-neutral;
//! traces and metrics can add routes without changing the Phoenix boundary.

mod storage;

use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use serde_json::json;
use timeless_api::{now_ms, parse_logsql, parse_ndjson, render_ndjson_row, LogQuery, SortOrder};

use storage::{Database, StoredLog};

const DEFAULT_LISTEN: &str = "127.0.0.1:9428";
const DEFAULT_DATABASE: &str = "timeless.db";
const DEFAULT_EXTENSION: &str = "target/release/libtimeless_ext.so";
const DEFAULT_INDEX_KEYS: &str = "service,app,node";
const DEFAULT_QUEUE_CAPACITY: usize = 1_024;
const DEFAULT_READ_WORKERS: usize = 4;
const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_MAX_RESULT_ROWS: usize = 10_000;

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: SocketAddr,
    pub database: PathBuf,
    pub extension: PathBuf,
    pub index_keys: Vec<String>,
    pub bearer_token: Option<String>,
    pub queue_capacity: usize,
    pub read_workers: usize,
    pub max_body_bytes: usize,
    pub max_result_rows: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN
                .parse()
                .expect("valid default listen address"),
            database: PathBuf::from(DEFAULT_DATABASE),
            extension: PathBuf::from(DEFAULT_EXTENSION),
            index_keys: DEFAULT_INDEX_KEYS.split(',').map(str::to_owned).collect(),
            bearer_token: None,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            read_workers: DEFAULT_READ_WORKERS,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_result_rows: DEFAULT_MAX_RESULT_ROWS,
        }
    }
}

impl Config {
    /// Minimal dependency-free CLI for the POC. Environment variables provide
    /// the same knobs for Phoenix supervision and container deployments.
    pub fn from_env_args() -> Result<Self, String> {
        let mut config = Self::default();
        if let Ok(value) = env::var("TIMELESSD_LISTEN") {
            config.listen = parse_socket("TIMELESSD_LISTEN", &value)?;
        }
        if let Ok(value) = env::var("TIMELESSD_DATABASE") {
            config.database = value.into();
        }
        if let Ok(value) = env::var("TIMELESSD_EXTENSION") {
            config.extension = value.into();
        }
        if let Ok(value) = env::var("TIMELESSD_INDEX_KEYS") {
            config.index_keys = parse_index_keys(&value)?;
        }
        if let Ok(value) = env::var("TIMELESSD_BEARER_TOKEN") {
            config.bearer_token = (!value.is_empty()).then_some(value);
        }
        if let Ok(value) = env::var("TIMELESSD_READ_WORKERS") {
            config.read_workers = parse_positive("TIMELESSD_READ_WORKERS", &value)?;
        }

        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--listen" => {
                    let value = arguments.next().ok_or("--listen requires an address")?;
                    config.listen = parse_socket("--listen", &value)?;
                }
                "--db" => {
                    config.database = arguments.next().ok_or("--db requires a path")?.into();
                }
                "--extension" => {
                    config.extension = arguments
                        .next()
                        .ok_or("--extension requires a path")?
                        .into();
                }
                "--index-keys" => {
                    config.index_keys =
                        parse_index_keys(&arguments.next().ok_or("--index-keys requires a list")?)?;
                }
                "--bearer-token" => {
                    config.bearer_token =
                        Some(arguments.next().ok_or("--bearer-token requires a token")?);
                }
                "--read-workers" => {
                    let value = arguments.next().ok_or("--read-workers requires a count")?;
                    config.read_workers = parse_positive("--read-workers", &value)?;
                }
                "--help" | "-h" => {
                    return Err(format!(
                        "usage: timelessd [--listen {DEFAULT_LISTEN}] [--db PATH] \
                         [--extension PATH] \
                         [--index-keys service,app,node] [--read-workers 4] \
                         [--bearer-token TOKEN]"
                    ));
                }
                other => return Err(format!("unknown argument {other:?}")),
            }
        }
        Ok(config)
    }
}

fn parse_socket(name: &str, value: &str) -> Result<SocketAddr, String> {
    value
        .parse()
        .map_err(|error| format!("{name}: invalid socket address {value:?}: {error}"))
}

fn parse_index_keys(value: &str) -> Result<Vec<String>, String> {
    let keys: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect();
    for key in &keys {
        if !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(format!(
                "invalid index key {key:?}; use letters, digits, '_', '-', or '.'"
            ));
        }
    }
    Ok(keys)
}

fn parse_positive(name: &str, value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!(
            "{name}: expected a positive integer, got {value:?}"
        )),
    }
}

#[derive(Clone)]
struct AppState {
    database: Database,
    bearer_token: Option<String>,
    max_result_rows: usize,
}

pub async fn run(config: Config) -> Result<(), String> {
    run_until(config, shutdown_signal()).await
}

pub async fn run_until<F>(config: Config, shutdown: F) -> Result<(), String>
where
    F: Future<Output = ()> + Send + 'static,
{
    let database = Database::start(
        config.database.clone(),
        config.extension.clone(),
        config.index_keys,
        config.queue_capacity,
        config.read_workers,
    )?;
    let app = router(
        AppState {
            database: database.clone(),
            bearer_token: config.bearer_token,
            max_result_rows: config.max_result_rows,
        },
        config.max_body_bytes,
    );
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|error| format!("bind {}: {error}", config.listen))?;
    eprintln!(
        "timelessd listening on {} (database {}, extension {})",
        listener.local_addr().unwrap_or(config.listen),
        config.database.display(),
        config.extension.display()
    );
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|error| format!("HTTP server: {error}"));
    let flush_result = database.shutdown().await;
    result.and(flush_result)
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        eprintln!("timelessd: cannot install shutdown signal handler: {error}");
    }
}

fn router(state: AppState, max_body_bytes: usize) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/insert/jsonline", post(ingest))
        .route("/select/logsql/query", get(query_get).post(query_post))
        .route("/api/v1/flush", get(flush))
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Result<Response, ApiError> {
    let stats = state.database.stats().await?;
    Ok(json_response(
        StatusCode::OK,
        json!({
            "status": "ok",
            "blocks": stats.blocks,
            "entries": stats.entries,
            "disk_size": stats.disk_size,
        }),
    ))
}

async fn ingest(
    State(state): State<AppState>,
    Query(parameters): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    authorize(&state, &headers, parameters.get("token"))?;
    let message_field = parameters.get("_msg_field").map_or("_msg", String::as_str);
    let time_field = parameters
        .get("_time_field")
        .map_or("_time", String::as_str);
    let parsed = parse_ndjson(&body, message_field, time_field, now_ms());
    let entries = parsed.entries.len();
    let errors = parsed.errors;
    if entries > 0 {
        state.database.ingest(parsed.entries).await?;
    }
    if errors > 0 {
        Ok(json_response(
            StatusCode::OK,
            json!({"entries": entries, "errors": errors}),
        ))
    } else {
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

async fn query_get(
    State(state): State<AppState>,
    Query(parameters): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize(&state, &headers, parameters.get("token"))?;
    let mut query = LogQuery::default();
    if let Some(level) = parameters.get("level") {
        query.level =
            Some(timeless_api::Level::parse_filter(level).map_err(ApiError::bad_request)?);
    }
    query.message = parameters.get("message").cloned();
    query.since_ms = parameters
        .get("start")
        .and_then(|value| parse_query_time(value));
    query.until_ms = parameters
        .get("end")
        .and_then(|value| parse_query_time(value));
    query.limit = parse_usize(parameters.get("limit")).unwrap_or(query.limit);
    query.offset = parse_usize(parameters.get("offset")).unwrap_or(query.offset);
    query.order = match parameters.get("order").map(String::as_str) {
        Some("asc") => SortOrder::Asc,
        _ => SortOrder::Desc,
    };
    run_query(&state, query).await
}

async fn query_post(
    State(state): State<AppState>,
    Query(parameters): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    authorize(&state, &headers, parameters.get("token"))?;
    let query_text = url::form_urlencoded::parse(&body)
        .find_map(|(key, value)| (key == "query").then(|| value.into_owned()))
        .unwrap_or_else(|| "*".to_owned());
    let query = parse_logsql(&query_text, now_ms()).map_err(ApiError::bad_request)?;
    run_query(&state, query).await
}

async fn run_query(state: &AppState, mut query: LogQuery) -> Result<Response, ApiError> {
    query.limit = query.limit.min(state.max_result_rows);
    if query.count {
        let total = state.database.count(query).await?;
        return Ok(ndjson_response(json!({"total": total}).to_string()));
    }
    let rows = state.database.query(query).await?;
    let mut body = String::new();
    for StoredLog {
        ts_ms,
        level,
        message,
        metadata_json,
    } in rows
    {
        if !body.is_empty() {
            body.push('\n');
        }
        body.push_str(
            &render_ndjson_row(ts_ms, &level, &message, &metadata_json)
                .map_err(ApiError::internal)?,
        );
    }
    Ok(ndjson_response(body))
}

async fn flush(
    State(state): State<AppState>,
    Query(parameters): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize(&state, &headers, parameters.get("token"))?;
    state.database.flush().await?;
    Ok(json_response(StatusCode::OK, json!({"status": "ok"})))
}

async fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}

fn parse_query_time(value: &str) -> Option<i64> {
    chrono_like_rfc3339_ms(value).or_else(|| value.parse().ok())
}

fn chrono_like_rfc3339_ms(value: &str) -> Option<i64> {
    // Reuse the LogsQL parser's RFC3339 implementation without exposing a
    // second public timestamp parser.
    parse_logsql(&format!("_time:>={value}"), 0)
        .ok()
        .and_then(|query| query.since_ms)
}

fn parse_usize(value: Option<&String>) -> Option<usize> {
    value.and_then(|value| value.parse().ok())
}

fn authorize(
    state: &AppState,
    headers: &HeaderMap,
    query_token: Option<&String>,
) -> Result<(), ApiError> {
    let Some(expected) = state.bearer_token.as_deref() else {
        return Ok(());
    };
    let header_token = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);
    match header_token.or(query_token.map(String::as_str)) {
        None => Err(ApiError::status(StatusCode::UNAUTHORIZED, "unauthorized")),
        Some(actual) if constant_time_eq(actual.as_bytes(), expected.as_bytes()) => Ok(()),
        Some(_) => Err(ApiError::status(StatusCode::FORBIDDEN, "forbidden")),
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn json_response(status: StatusCode, value: serde_json::Value) -> Response {
    (
        status,
        [("content-type", "application/json")],
        value.to_string(),
    )
        .into_response()
}

fn ndjson_response(body: String) -> Response {
    (
        StatusCode::OK,
        [("content-type", "application/x-ndjson")],
        body,
    )
        .into_response()
}

fn empty_response(status: StatusCode) -> Response {
    (status, "").into_response()
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn status(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::status(StatusCode::BAD_REQUEST, message)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::status(StatusCode::INTERNAL_SERVER_ERROR, message)
    }
}

impl From<storage::Error> for ApiError {
    fn from(error: storage::Error) -> Self {
        match error {
            storage::Error::Overloaded => {
                Self::status(StatusCode::SERVICE_UNAVAILABLE, error.to_string())
            }
            storage::Error::Stopped => {
                Self::status(StatusCode::SERVICE_UNAVAILABLE, error.to_string())
            }
            storage::Error::Database(_) => Self::internal(error.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        json_response(self.status, json!({"error": self.message}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tempfile::TempDir;
    use tower::ServiceExt;

    fn extension_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("target/debug/libtimeless_ext.so")
    }

    fn test_app(token: Option<&str>) -> (Router, Database, TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::start(
            directory.path().join("logs.db"),
            extension_path(),
            vec!["service".into()],
            16,
            2,
        )
        .unwrap();
        let app = router(
            AppState {
                database: database.clone(),
                bearer_token: token.map(str::to_owned),
                max_result_rows: 100,
            },
            1_024,
        );
        (app, database, directory)
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn index_keys_reject_sql_syntax() {
        assert!(parse_index_keys("service,app").is_ok());
        assert!(parse_index_keys("service'); DROP TABLE logs;--").is_err());
    }

    #[test]
    fn token_comparison_is_length_and_content_sensitive() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secrex"));
        assert!(!constant_time_eq(b"secret", b"secret-long"));
    }

    #[tokio::test]
    async fn http_vertical_slice_ingests_queries_counts_and_reports_health() {
        let (app, database, _directory) = test_app(None);
        let ingest = app
            .clone()
            .oneshot(
                Request::post("/insert/jsonline")
                    .header("content-type", "application/x-ndjson")
                    .body(Body::from(
                        "{\"_msg\":\"hello\",\"level\":\"info\",\"service\":\"api\"}\n\
                         broken\n\
                         {\"_msg\":\"boom\",\"level\":\"error\",\"service\":\"worker\"}",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);
        assert_eq!(body_json(ingest).await, json!({"entries": 2, "errors": 1}));

        let query = app
            .clone()
            .oneshot(
                Request::post("/select/logsql/query")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from("query=level%3Aerror"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(query.status(), StatusCode::OK);
        assert_eq!(query.headers()["content-type"], "application/x-ndjson");
        let row = body_json(query).await;
        assert_eq!(row["_msg"], "boom");
        assert_eq!(row["service"], "worker");

        let count = app
            .clone()
            .oneshot(
                Request::post("/select/logsql/query")
                    .body(Body::from("query=*+%7C+stats+count%28%29+as+total"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(body_json(count).await, json!({"total": 2}));

        let health = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let health = body_json(health).await;
        assert_eq!(health["status"], "ok");
        assert_eq!(health["entries"], 2);
        assert!(health["blocks"].as_i64().unwrap() >= 1);
        database.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn health_skips_auth_but_data_routes_require_it() {
        let (app, database, _directory) = test_app(Some("secret"));
        let health = app
            .clone()
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let missing = app
            .clone()
            .oneshot(
                Request::get("/select/logsql/query")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

        let accepted = app
            .oneshot(
                Request::get("/select/logsql/query?token=secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        database.shutdown().await.unwrap();
    }
}
