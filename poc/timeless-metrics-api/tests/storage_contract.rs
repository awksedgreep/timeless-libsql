use std::path::{Path, PathBuf};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tempfile::TempDir;
use timeless_metrics_api::{router, Storage, DEFAULT_RAW_RETENTION};
use tower::ServiceExt;

/// This is intentionally an extension contract test, not an API-owned storage
/// implementation. Build `timeless_ext` first, then run ignored tests.
#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_one_pins_the_existing_storage_lifecycle() {
    let extension = extension_path();
    assert!(
        extension.is_file(),
        "build timeless_ext or set TIMELESS_EXT_PATH; missing {}",
        extension.display()
    );
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("metrics.db");
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        2,
        32,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());

    let owner_error = match Storage::start(
        database.clone(),
        extension.clone(),
        1,
        8,
        DEFAULT_RAW_RETENTION,
    ) {
        Ok(second) => {
            second.shutdown().await.unwrap();
            panic!("a second API owner unexpectedly acquired the database")
        }
        Err(error) => error,
    };
    assert!(owner_error.contains("already owned"), "{owner_error}");

    let health = get_json(&app, "/health").await;
    assert_eq!(health.0, StatusCode::OK);
    assert_eq!(health.1["status"], "ok");
    assert_eq!(health.1["points"], 0);

    storage
        .submit_named_batch(named_batch(4_095, 1_700_000_000), 4_095)
        .await
        .unwrap();
    storage.barrier().await.unwrap();
    let before_threshold = storage.stats().await.unwrap();
    assert_eq!(before_threshold.series, 1);
    assert_eq!(before_threshold.buffered_points, 4_095);
    assert_eq!(before_threshold.disk_points, 0);
    assert_eq!(before_threshold.raw_tier_chunks, 0);

    storage
        .submit_named_batch(named_batch(1, 1_700_004_095), 1)
        .await
        .unwrap();
    storage.barrier().await.unwrap();
    let at_threshold = storage.stats().await.unwrap();
    assert_eq!(at_threshold.buffered_points, 0);
    assert_eq!(at_threshold.disk_points, 4_096);
    assert_eq!(at_threshold.raw_tier_chunks, 1);
    assert_eq!(at_threshold.admitted_batches, 2);
    assert_eq!(at_threshold.admitted_points, 4_096);
    assert_eq!(at_threshold.completed_batches, 2);
    assert_eq!(at_threshold.completed_points, 4_096);
    assert_eq!(at_threshold.failed_batches, 0);
    assert_eq!(at_threshold.queued_batches, 0);
    assert_eq!(at_threshold.in_flight_batches, 0);
    assert_eq!(at_threshold.series_index_entries, 1);
    assert_eq!(
        at_threshold.raw_chunk_index_entries,
        at_threshold.raw_tier_chunks
    );
    assert!(at_threshold.sqlite_index_bytes > 0);

    storage
        .submit_named_batch(named_batch(10, 1_700_004_096), 10)
        .await
        .unwrap();
    let flush = post_json(&app, "/api/v1/flush").await;
    assert_eq!(flush.0, StatusCode::OK);
    assert_eq!(flush.1["status"], "ok");
    assert_eq!(flush.1["through_batches"], 3);
    assert_eq!(flush.1["through_points"], 4_106);
    assert_eq!(flush.1["completed_batches"], 3);
    assert_eq!(flush.1["completed_points"], 4_106);
    assert_eq!(flush.1["queued_batches"], 0);

    let flushed = get_json(&app, "/select/metrics/stats").await;
    assert_eq!(flushed.0, StatusCode::OK);
    assert_eq!(flushed.1["disk_points"], 4_106);
    assert_eq!(flushed.1["buffered_points"], 0);
    assert_eq!(flushed.1["raw_retention_seconds"], 604_800);
    assert_eq!(flushed.1["writer_connections"], 1);
    assert_eq!(flushed.1["reader_connections"], 2);
    assert_eq!(flushed.1["api_flush_count"], 1);
    assert!(flushed.1["database_file_bytes"].as_u64().unwrap() > 0);
    assert!(flushed.1["sqlite_page_bytes"].as_i64().unwrap() > 0);

    let missing_query = app
        .clone()
        .oneshot(
            Request::get("/api/v1/query?metric=not-implemented-until-session-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_query.status(), StatusCode::NOT_FOUND);

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let recovered = reopened.stats().await.unwrap();
    assert_eq!(recovered.series, 1);
    assert_eq!(recovered.disk_points, 4_106);
    assert_eq!(recovered.buffered_points, 0);
    assert_eq!(recovered.total_points, 4_106);
    assert_eq!(recovered.raw_tier_chunks, 2);
    assert_eq!(
        recovered.rollup_tiers.as_deref(),
        Some("3600:2592000,86400:31536000,2592000:0")
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_two_preserves_native_ingest_and_partial_success_contracts() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_two.db");
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        2,
        8,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let base_ms = 1_700_000_000_000_i64;
    let victoria = format!(
        concat!(
            "{{\"metric\":{{\"__name__\":\"contract_vm\",\"host\":\"edge\",\"env\":\"test\"}},",
            "\"values\":[1.5,2.5],\"timestamps\":[{},{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"contract_vm\",\"host\":\"edge\",\"env\":\"test\"}},",
            "\"values\":[3.5],\"timestamps\":[{}]}}\n",
            "{{\"metric\":"
        ),
        base_ms,
        base_ms + 1_000,
        base_ms + 2_000,
    );
    let prometheus = format!(
        concat!(
            "contract_prom{{host=\"edge\",env=\"test\"}} 4.5 {}\n",
            "contract_prom{{host=\"edge\",env=\"test\"}} NaN {}\n",
            "contract_prom{{host=\"edge\",env=\"test\"}} +Inf {}\n",
            "malformed line\n"
        ),
        base_ms,
        base_ms + 1_000,
        base_ms + 2_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", prometheus.as_bytes()).await);
    assert_no_content(post_body(&app, "/api/v1/import", br#"{"metric":"#).await);
    assert_no_content(
        post_body(
            &app,
            "/api/v1/import/prometheus",
            b"garbage one\ngarbage two\n",
        )
        .await,
    );
    let reserved_prometheus = b"\x01not an HTTP batch protocol";
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", reserved_prometheus).await);

    let oversized = vec![b'x'; 10 * 1024 * 1024 + 1];
    let too_large = post_body(&app, "/api/v1/import", &oversized).await;
    assert_eq!(too_large.0, StatusCode::PAYLOAD_TOO_LARGE);

    let flush = post_json(&app, "/api/v1/flush").await;
    assert_eq!(flush.0, StatusCode::OK);
    assert_eq!(flush.1["through_batches"], 5);
    assert_eq!(flush.1["through_points"], 4);
    assert_eq!(flush.1["completed_batches"], 5);
    assert_eq!(flush.1["completed_points"], 4);
    assert_eq!(flush.1["failed_batches"], 0);
    assert_eq!(flush.1["queued_batches"], 0);
    assert_eq!(flush.1["in_flight_batches"], 0);

    let health = get_json(&app, "/health").await;
    assert_eq!(health.1["points"], 4);
    assert_eq!(health.1["series"], 2);
    assert_eq!(health.1["import_errors"], 8);
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.admitted_batches, 5);
    assert_eq!(stats.admitted_points, 4);
    assert_eq!(stats.completed_batches, 5);
    assert_eq!(stats.completed_points, 4);
    assert_eq!(stats.import_errors, 8);
    assert_eq!(stats.api_ingest_requests, 5);
    assert_eq!(stats.api_victoria_requests, 2);
    assert_eq!(stats.api_prometheus_requests, 3);
    let expected_body_bytes = victoria.len()
        + prometheus.len()
        + br#"{"metric":"#.len()
        + b"garbage one\ngarbage two\n".len()
        + reserved_prometheus.len();
    assert_eq!(stats.admitted_body_bytes, expected_body_bytes as u64);
    assert_eq!(stats.completed_body_bytes, expected_body_bytes as u64);
    assert!(stats.api_parse_ns > 0);
    assert!(stats.api_batch_encode_ns > 0);
    assert_eq!(stats.extension_prometheus_ingest_batches, 2);
    assert_eq!(stats.extension_prometheus_ingest_points, 1);
    assert_eq!(stats.extension_prometheus_ingest_errors, 5);
    assert!(stats.extension_prometheus_ingest_total_ns > 0);
    assert_eq!(stats.queued_unknown_point_batches, 0);
    assert_eq!(stats.queued_body_bytes, 0);
    assert_eq!(stats.in_flight_body_bytes, 0);

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let rows = persisted_rows(&database, &extension);
    assert_eq!(
        rows,
        vec![
            ("contract_prom".into(), 1_700_000_000, 4.5),
            ("contract_vm".into(), 1_700_000_000, 1.5),
            ("contract_vm".into(), 1_700_000_001, 2.5),
            ("contract_vm".into(), 1_700_000_002, 3.5),
        ]
    );

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let recovered = reopened.stats().await.unwrap();
    assert_eq!(recovered.series, 2);
    assert_eq!(recovered.disk_points, 4);
    assert_eq!(recovered.buffered_points, 0);
    reopened.shutdown().await.unwrap();
}

async fn get_json(app: &axum::Router, path: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn post_json(app: &axum::Router, path: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::post(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn post_body(app: &axum::Router, path: &str, body: &[u8]) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(Request::post(path).body(Body::from(body.to_vec())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 11 * 1024 * 1024)
        .await
        .unwrap();
    (status, body.to_vec())
}

fn assert_no_content(response: (StatusCode, Vec<u8>)) {
    assert_eq!(response.0, StatusCode::NO_CONTENT);
    assert!(response.1.is_empty());
}

fn persisted_rows(database: &Path, extension: &Path) -> Vec<(String, i64, f64)> {
    let conn = rusqlite::Connection::open(database).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(extension, None::<&str>).unwrap();
    }
    conn.load_extension_disable().unwrap();
    let mut stmt = conn
        .prepare("SELECT name, ts, value FROM metrics ORDER BY name, ts, value")
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn extension_path() -> PathBuf {
    std::env::var_os("TIMELESS_EXT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("target/release/libtimeless_ext.so")
        })
}

fn named_batch(points: usize, first_timestamp: i64) -> Vec<u8> {
    let points_u32 = u32::try_from(points).unwrap();
    let name = b"session_one_metric";
    let mut blob = Vec::with_capacity(12 + name.len() + points * 20);
    blob.push(0x01);
    blob.push(0);
    blob.extend_from_slice(&0_u16.to_le_bytes());
    blob.extend_from_slice(&1_u32.to_le_bytes());
    blob.extend_from_slice(&points_u32.to_le_bytes());
    blob.extend_from_slice(&(name.len() as u32).to_le_bytes());
    blob.extend_from_slice(name);
    blob.extend_from_slice(&0_u32.to_le_bytes());
    for _ in 0..points {
        blob.extend_from_slice(&0_u32.to_le_bytes());
    }
    for offset in 0..points {
        blob.extend_from_slice(&(first_timestamp + offset as i64).to_le_bytes());
    }
    for offset in 0..points {
        blob.extend_from_slice(&(offset as f64).to_bits().to_le_bytes());
    }
    blob
}
