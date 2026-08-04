use std::path::{Path, PathBuf};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;
use timeless_metrics_api::{
    router, router_with_limits, PromQueryLimits, Storage, DEFAULT_RAW_RETENTION,
};
use tower::ServiceExt;

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn release_backup_is_ordered_verified_no_clobber_and_cold_reopenable() {
    let extension = extension_path();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("metrics.db");
    let backup = directory.path().join("backup-metrics.db");
    let storage = Storage::start(database, extension.clone(), 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    storage
        .submit_named_batch(named_batch(3, 1_700_000_000), 3)
        .await
        .unwrap();

    let report = storage.backup(backup.clone()).await.unwrap();
    assert_eq!(report.signal, "metrics");
    assert_eq!(report.destination, backup.to_string_lossy());
    assert_eq!(report.schema_version, 1);
    assert!(report.bytes > 0);
    assert!(report.pages > 0);
    let unchanged_bytes = std::fs::read(&backup).unwrap();
    let error = storage.backup(backup.clone()).await.unwrap_err();
    assert!(error.contains("refusing to overwrite"), "{error}");
    assert_eq!(std::fs::read(&backup).unwrap(), unchanged_bytes);
    let live_stats = storage.stats().await.unwrap();
    assert_eq!(live_stats.backup_count, 2);
    assert_eq!(live_stats.backup_errors, 1);
    assert_eq!(live_stats.checkpoint_count, 2);
    assert_eq!(live_stats.checkpoint_errors, 0);
    assert!(live_stats.backup_total_ns > 0);
    assert!(live_stats.checkpoint_total_ns > 0);

    let restored = Storage::start(backup, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let stats = restored.stats().await.unwrap();
    assert_eq!(stats.total_points, 3);
    assert_eq!(stats.buffered_points, 0);
    assert_eq!(stats.series, 1);
    restored.shutdown().await.unwrap();
    storage.shutdown().await.unwrap();
}

#[test]
#[ignore = "requires a built timeless_ext shared library"]
fn future_metrics_schema_fails_before_vtab_initialization() {
    let extension = extension_path();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("future-metrics.db");
    let conn = Connection::open(&database).unwrap();
    conn.execute_batch(
        "CREATE TABLE _timeless_schema_migrations(
           signal TEXT NOT NULL, version INTEGER NOT NULL,
           applied_at_unix INTEGER NOT NULL, server_version TEXT NOT NULL,
           extension_version TEXT NOT NULL, extension_data_abi INTEGER NOT NULL,
           PRIMARY KEY(signal, version));
         INSERT INTO _timeless_schema_migrations VALUES
           ('metrics',999,0,'future','future',1);",
    )
    .unwrap();
    drop(conn);
    let error = match Storage::start(database.clone(), extension, 1, 1, DEFAULT_RAW_RETENTION) {
        Ok(_) => panic!("future metrics database unexpectedly opened"),
        Err(error) => error,
    };
    assert!(error.contains("supports at most 1"), "{error}");
    let conn = Connection::open(database).unwrap();
    let created: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('metric_samples','metrics')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(created, 0, "downgrade refusal must precede vtab creation");
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn migrated_canonical_metrics_table_is_the_only_store_and_is_queried_in_place() {
    let extension = extension_path();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("migrated-metrics.db");
    let conn = open_with_extension(&database, &extension);
    conn.execute_batch(
        "CREATE VIRTUAL TABLE metric_samples USING timeless_metrics(
           rollups='3600s@2592000s,86400s@31536000s,2592000s@0');",
    )
    .unwrap();
    let batch = named_batch(2, 1_700_000_000);
    conn.execute(
        "INSERT INTO metric_samples(metric_samples) VALUES (?1)",
        [&batch],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO metric_samples(metric_samples) VALUES ('flush')",
        [],
    )
    .unwrap();
    drop(conn);

    let storage = Storage::start(database.clone(), extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.total_points, 2);
    assert_eq!(stats.series, 1);
    let app = router(storage.clone());
    let latest = get_json(&app, "/api/v1/query?metric=session_one_metric").await;
    assert_eq!(latest.0, StatusCode::OK);
    assert_eq!(latest.1["timestamp"], 1_700_000_001_i64);
    assert_eq!(latest.1["value"], 1.0);
    drop(app);
    storage.shutdown().await.unwrap();

    let conn = Connection::open(database).unwrap();
    let names: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_schema
              WHERE type='table' AND name IN ('metric_samples','metrics')
              ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, ["metric_samples"]);
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn single_poc_metrics_table_remains_compatible_without_copying_storage() {
    let extension = extension_path();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("poc-metrics.db");
    let conn = open_with_extension(&database, &extension);
    conn.execute_batch("CREATE VIRTUAL TABLE metrics USING timeless_metrics;")
        .unwrap();
    drop(conn);

    let storage = Storage::start(database.clone(), extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    storage
        .submit_named_batch(named_batch(1, 1_700_000_000), 1)
        .await
        .unwrap();
    storage.flush().await.unwrap();
    assert_eq!(storage.stats().await.unwrap().total_points, 1);
    storage.shutdown().await.unwrap();

    let conn = Connection::open(database).unwrap();
    let names: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_schema
              WHERE type='table' AND name IN ('metric_samples','metrics')
              ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, ["metrics"]);
}

#[test]
#[ignore = "requires a built timeless_ext shared library"]
fn ambiguous_metrics_virtual_tables_fail_closed() {
    let extension = extension_path();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("ambiguous-metrics.db");
    let conn = open_with_extension(&database, &extension);
    conn.execute_batch(
        "CREATE VIRTUAL TABLE metric_samples USING timeless_metrics;
         CREATE VIRTUAL TABLE metrics USING timeless_metrics;",
    )
    .unwrap();
    drop(conn);

    let error = match Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION) {
        Ok(_) => panic!("ambiguous metrics database unexpectedly opened"),
        Err(error) => error,
    };
    assert!(error.contains("both metric_samples and metrics"), "{error}");
    assert!(error.contains("refuse to choose"), "{error}");
}

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

    let empty_query = get_json(&app, "/api/v1/query?metric=missing").await;
    assert_eq!(empty_query.0, StatusCode::OK);
    assert_eq!(empty_query.1, serde_json::json!({"data": []}));

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

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_three_pins_mechanical_reads_discovery_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_three.db");
    let base = 1_700_000_000_i64;
    let base_ms = base * 1_000;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        2,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let victoria = format!(
        concat!(
            "{{\"metric\":{{\"__name__\":\"contract_vm\",\"host\":\"edge\",\"env\":\"test\"}},",
            "\"values\":[1.5,2.5,3.5],\"timestamps\":[{},{},{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"contract_vm\",\"host\":\"west\",\"env\":\"prod\"}},",
            "\"values\":[8.0],\"timestamps\":[{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"contract_sparse\",\"host\":\"edge\"}},",
            "\"values\":[9.0],\"timestamps\":[{}]}}"
        ),
        base_ms,
        base_ms + 1_000,
        base_ms + 2_000,
        base_ms + 1_000,
        base_ms,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let latest = get_json(&app, "/api/v1/query?metric=contract_vm&host=edge&env=test").await;
    assert_eq!(latest.0, StatusCode::OK);
    assert_eq!(
        latest.1,
        serde_json::json!({
            "labels": {"env": "test", "host": "edge"},
            "timestamp": base + 2,
            "value": 3.5
        })
    );

    let posted = post_form(
        &app,
        "/api/v1/query?metric=wrong&host=wrong",
        "metric=contract_vm&host=edge&env=test",
    )
    .await;
    assert_eq!(posted.0, StatusCode::OK);
    assert_eq!(posted.1, latest.1);

    let export = get_body(
        &app,
        &format!(
            "/api/v1/export?metric=contract_vm&host=edge&env=test&from={base}&to={}",
            base + 2
        ),
    )
    .await;
    assert_eq!(export.0, StatusCode::OK);
    assert_eq!(
        String::from_utf8(export.1).unwrap(),
        format!(
            concat!(
                "{{\"metric\":{{\"__name__\":\"contract_vm\",\"env\":\"test\",\"host\":\"edge\"}},",
                "\"timestamps\":[{},{},{}],\"values\":[1.5,2.5,3.5]}}"
            ),
            base_ms,
            base_ms + 1_000,
            base_ms + 2_000
        )
    );
    let empty_export = get_body(
        &app,
        &format!(
            "/api/v1/export?metric=contract_vm&host=missing&from={base}&to={}",
            base + 2
        ),
    )
    .await;
    assert_eq!(empty_export, (StatusCode::OK, Vec::new()));

    let range = get_json(
        &app,
        &format!(
            "/api/v1/query_range?metric=contract_vm&host=edge&env=test&from={base}&to={}&step=1&aggregate=avg",
            base + 2
        ),
    )
    .await;
    assert_eq!(range.0, StatusCode::OK);
    assert_eq!(
        range.1,
        serde_json::json!({
            "metric": "contract_vm",
            "series": [{
                "labels": {"env": "test", "host": "edge"},
                "data": [[base, 1.5], [base + 1, 2.5], [base + 2, 3.5]]
            }]
        })
    );
    let partial = get_json(
        &app,
        &format!(
            "/api/v1/query_range?metric=contract_vm&host=edge&env=test&from={base}&to={}&step=2&aggregate=avg",
            base + 2
        ),
    )
    .await;
    assert_eq!(
        partial.1,
        serde_json::json!({
            "metric": "contract_vm",
            "series": [{
                "labels": {"env": "test", "host": "edge"},
                "data": [[base, 2.0], [base + 2, 3.5]]
            }]
        })
    );

    assert_eq!(
        get_json(&app, "/api/v1/labels").await.1,
        serde_json::json!({"status": "success", "data": ["__name__", "env", "host"]})
    );
    assert_eq!(
        get_json(&app, "/api/v1/label/host/values?metric=contract_vm")
            .await
            .1,
        serde_json::json!({"status": "success", "data": ["edge", "west"]})
    );
    assert_eq!(
        get_json(&app, "/api/v1/label/__name__/values").await.1,
        serde_json::json!({"status": "success", "data": ["contract_sparse", "contract_vm"]})
    );
    assert_eq!(
        get_json(&app, "/api/v1/series?metric=contract_vm").await.1,
        serde_json::json!({
            "status": "success",
            "data": [
                {"labels": {"env": "prod", "host": "west"}},
                {"labels": {"env": "test", "host": "edge"}}
            ]
        })
    );

    let selector = "%7B__name__%3D~%22contract_.%2A%22%2Cenv%21%3D%22prod%22%2Chost%3D~%22edge%7Cwest%22%2Chost%21%3D%22west%22%7D";
    assert_eq!(
        get_json(
            &app,
            &format!("/prometheus/api/v1/series?match%5B%5D={selector}")
        )
        .await
        .1,
        serde_json::json!({
            "status": "success",
            "data": [
                {"__name__": "contract_sparse", "host": "edge"},
                {"__name__": "contract_vm", "env": "test", "host": "edge"}
            ]
        })
    );
    let absent_env = "%7B__name__%3D%22contract_sparse%22%2Cenv%3D%22%22%7D";
    assert_eq!(
        get_json(&app, &format!("/api/v1/series?match%5B%5D={absent_env}"))
            .await
            .1,
        serde_json::json!({
            "status": "success",
            "data": [{"__name__": "contract_sparse", "host": "edge"}]
        })
    );
    assert_eq!(
        get_json(&app, "/prometheus/api/v1/series").await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get_json(&app, "/api/v1/series?match%5B%5D=%7Bbad%3D~%22%5B%22%7D")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let promql = get_json(&app, "/api/v1/query?query=contract_vm").await;
    assert_eq!(promql.0, StatusCode::OK);
    assert_eq!(promql.1["data"]["resultType"], "vector");
    assert_eq!(promql.1["data"]["result"], serde_json::json!([]));

    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.api_read_errors, 0);
    assert_eq!(stats.api_latest_requests, 2);
    assert_eq!(stats.api_export_requests, 2);
    assert_eq!(stats.api_range_requests, 2);
    assert_eq!(stats.api_discovery_requests, 6);
    assert_eq!(stats.api_promql_requests, 1);
    assert_eq!(stats.api_read_requests, 13);
    assert!(stats.api_read_total_ns > 0);
    assert!(stats.api_read_frame_bytes > 0);
    assert!(stats.api_read_response_bytes > 0);
    assert!(stats.api_read_result_series > 0);
    assert!(stats.api_read_result_points > 0);

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        get_json(
            &reopened_app,
            "/api/v1/query?metric=contract_vm&host=edge&env=test"
        )
        .await
        .1,
        latest.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_four_pins_promql_selector_window_errors_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_four.db");
    let base = 1_700_000_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        2,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let victoria = format!(
        concat!(
            "{{\"metric\":{{\"__name__\":\"prom_cpu\",\"host\":\"a\",\"env\":\"prod\"}},",
            "\"values\":[10.0,20.0,30.0,50.0],\"timestamps\":[{},{},{},{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"prom_cpu\",\"host\":\"b\"}},",
            "\"values\":[80.0],\"timestamps\":[{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"prom_stale\",\"host\":\"excluded\"}},",
            "\"values\":[1.0],\"timestamps\":[{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"prom_stale\",\"host\":\"included\"}},",
            "\"values\":[2.0],\"timestamps\":[{}]}}"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 60) * 1_000,
        base * 1_000,
        (base - 300) * 1_000,
        (base - 299) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let selector = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query_range?query=prom_cpu%7Bhost%3D%22a%22%7D&start={base}&end={}&step=10",
            base + 20
        ),
    )
    .await;
    assert_eq!(selector.0, StatusCode::OK);
    assert_eq!(
        selector.1,
        serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{
                    "metric": {"__name__": "prom_cpu", "env": "prod", "host": "a"},
                    "values": [[base, "10"], [base + 10, "20"], [base + 20, "30"]]
                }]
            }
        })
    );

    let rfc3339 = get_json(
        &app,
        "/prometheus/api/v1/query_range?query=prom_cpu%7Bhost%3D%22a%22%7D&start=2023-11-14T22%3A13%3A20Z&end=1700000020.9&step=10s",
    )
    .await;
    assert_eq!(rfc3339.0, StatusCode::OK);
    assert_eq!(rfc3339.1, selector.1);

    let partial_grid = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query_range?query=prom_cpu%7Bhost%3D%22a%22%7D&start={base}&end={}&step=10",
            base + 15
        ),
    )
    .await;
    assert_eq!(
        partial_grid.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "10"], [base + 10, "20"]])
    );

    let average = get_json(
        &app,
        &format!(
            "/api/v1/query_range?query=avg_over_time%28prom_cpu%7Bhost%3D%22a%22%7D%5B20s%5D%29&start={base}&end={}&step=10",
            base + 20
        ),
    )
    .await;
    assert_eq!(average.0, StatusCode::OK);
    assert_eq!(
        average.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "10"], [base + 10, "15"], [base + 20, "25"]])
    );
    assert_eq!(
        average.1["data"]["result"][0]["metric"],
        serde_json::json!({"env": "prod", "host": "a"})
    );

    let range_vector = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=prom_cpu%7Bhost%3D%22a%22%7D%5B20s%5D&time={}",
            base + 20
        ),
    )
    .await;
    assert_eq!(range_vector.0, StatusCode::OK);
    assert_eq!(range_vector.1["data"]["resultType"], "matrix");
    assert_eq!(
        range_vector.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "prom_cpu", "env": "prod", "host": "a"},
            "values": [[base + 10, "20"], [base + 20, "30"]]
        }])
    );

    let range_vector_over_grid = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query_range?query=prom_cpu%5B20s%5D&start={base}&end={}&step=10",
            base + 20
        ),
    )
    .await;
    assert_eq!(range_vector_over_grid.0, StatusCode::BAD_REQUEST);
    assert_eq!(range_vector_over_grid.1["status"], "error");
    assert_eq!(range_vector_over_grid.1["errorType"], "bad_data");
    assert!(range_vector_over_grid.1["error"]
        .as_str()
        .unwrap()
        .contains("range vector"));

    let instant = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=prom_cpu%7Bhost%3D%22a%22%7D&time={}",
            base + 20
        ),
    )
    .await;
    assert_eq!(instant.0, StatusCode::OK);
    assert_eq!(instant.1["data"]["resultType"], "vector");
    assert_eq!(
        instant.1["data"]["result"][0]["value"],
        serde_json::json!([base + 20, "30"])
    );

    let stale = get_json(&app, &format!("/api/v1/query?query=prom_stale&time={base}")).await;
    assert_eq!(stale.0, StatusCode::OK);
    assert_eq!(stale.1["data"]["result"].as_array().unwrap().len(), 1);
    assert_eq!(stale.1["data"]["result"][0]["metric"]["host"], "included");

    let duplicate = get_json(
        &app,
        &format!(
            "/api/v1/query_range?query=prom_cpu%7Bhost%3D%22nope%22%2Chost%3D%22a%22%7D&start={base}&end={base}&step=10"
        ),
    )
    .await;
    assert_eq!(duplicate.0, StatusCode::OK);
    assert_eq!(duplicate.1["data"]["result"], serde_json::json!([]));

    let posted = post_form(
        &app,
        "/prometheus/api/v1/query_range?query=wrong&start=0&end=0&step=1",
        &format!("query=prom_cpu%7Bhost%3D%22a%22%7D&start={base}&end={base}&step=10"),
    )
    .await;
    assert_eq!(posted.0, StatusCode::OK);
    assert_eq!(posted.1["data"]["result"][0]["values"][0][1], "10");

    for path in [
        "/prometheus/api/v1/query_range?start=0&end=10&step=1",
        "/prometheus/api/v1/query_range?query=rate%28prom_cpu%5B1m%5D%29&start=0&end=10&step=1",
        "/prometheus/api/v1/query_range?query=prom_cpu%2Bother&start=0&end=10&step=1",
    ] {
        let error = get_json(&app, path).await;
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert_eq!(error.1["status"], "error");
        assert_eq!(error.1["errorType"], "bad_data");
        assert!(error.1["error"].as_str().unwrap().len() > 8);
    }

    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.api_promql_requests, 9);
    assert_eq!(stats.api_read_errors, 0);
    assert!(stats.api_read_frame_bytes > 0);
    assert!(stats.api_read_result_points >= 9);

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = get_json(
        &reopened_app,
        &format!(
            "/prometheus/api/v1/query?query=prom_cpu%7Bhost%3D%22a%22%7D&time={}",
            base + 20
        ),
    )
    .await;
    assert_eq!(recovered.1, instant.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_three_promql_nameless_selectors_expand_before_reads_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_three_nameless.db");
    let base = 1_700_100_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        2,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let victoria = format!(
        concat!(
            "{{\"metric\":{{\"__name__\":\"nameless_alpha\",\"job\":\"api\",\"host\":\"a\"}},",
            "\"values\":[1.0,2.0],\"timestamps\":[{},{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"nameless_beta\",\"job\":\"api\",\"host\":\"b\"}},",
            "\"values\":[3.0],\"timestamps\":[{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"nameless_gamma\",\"job\":\"db\"}},",
            "\"values\":[4.0],\"timestamps\":[{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"nameless_no_job\",\"host\":\"z\"}},",
            "\"values\":[5.0],\"timestamps\":[{}]}}"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let instant = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=%7Bjob%3D%22api%22%7D&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(instant.0, StatusCode::OK);
    assert_eq!(
        instant.1["data"]["result"],
        serde_json::json!([
            {
                "metric": {"__name__": "nameless_alpha", "host": "a", "job": "api"},
                "value": [base + 10, "2"]
            },
            {
                "metric": {"__name__": "nameless_beta", "host": "b", "job": "api"},
                "value": [base + 10, "3"]
            }
        ])
    );

    let missing_as_empty = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=%7Bjob%3D%22api%22%2Cregion%3D%22%22%7D&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(missing_as_empty.0, StatusCode::OK);
    assert_eq!(
        missing_as_empty.1["data"]["result"],
        instant.1["data"]["result"]
    );

    let range = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query_range?query=%7Bjob%3D%22api%22%7D&start={base}&end={}&step=10",
            base + 10
        ),
    )
    .await;
    assert_eq!(range.0, StatusCode::OK);
    assert_eq!(range.1["data"]["result"].as_array().unwrap().len(), 2);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "1"], [base + 10, "2"]])
    );

    let average = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=avg_over_time%28%7Bjob%3D%22api%22%7D%5B20s%5D%29&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(average.0, StatusCode::OK);
    assert_eq!(
        average.1["data"]["result"],
        serde_json::json!([
            {"metric": {"host": "a", "job": "api"}, "value": [base + 10, "1.5"]},
            {"metric": {"host": "b", "job": "api"}, "value": [base + 10, "3"]}
        ])
    );

    let empty_matching_only = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=%7Bjob%3D~%22.%2A%22%7D&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(empty_matching_only.0, StatusCode::BAD_REQUEST);
    assert_eq!(empty_matching_only.1["errorType"], "bad_data");
    assert!(empty_matching_only.1["error"]
        .as_str()
        .unwrap()
        .contains("at least one non-empty matcher"));

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_result_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = get_json(
        &limited,
        &format!(
            "/prometheus/api/v1/query?query=%7Bjob%3D%22api%22%7D&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum result-point limit of 1"));

    let catalog_limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 3,
            ..PromQueryLimits::default()
        },
    );
    let rejected = get_json(
        &catalog_limited,
        &format!(
            "/prometheus/api/v1/query?query=%7Bjob%3D%22api%22%7D&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum catalog-work limit of 3 series"));

    drop(catalog_limited);
    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = get_json(
        &reopened_app,
        &format!(
            "/prometheus/api/v1/query?query=%7Bjob%3D%22api%22%7D&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(recovered.1, instant.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_three_promql_metric_name_matchers_prune_before_reads_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_three_name_matchers.db");
    let base = 1_700_200_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        2,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let victoria = format!(
        concat!(
            "{{\"metric\":{{\"__name__\":\"name_alpha\",\"job\":\"api\"}},\"values\":[1.0],\"timestamps\":[{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"name_beta\",\"job\":\"api\"}},\"values\":[2.0],\"timestamps\":[{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"name_gamma\",\"job\":\"db\"}},\"values\":[3.0],\"timestamps\":[{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"other_delta\",\"job\":\"api\"}},\"values\":[4.0],\"timestamps\":[{}]}}"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let regex = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=%7B__name__%3D~%22name_.%2A%22%2Cjob%3D%22api%22%7D&time={base}"
        ),
    )
    .await;
    assert_eq!(regex.0, StatusCode::OK);
    assert_eq!(
        regex.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "name_alpha", "job": "api"}, "value": [base, "1"]},
            {"metric": {"__name__": "name_beta", "job": "api"}, "value": [base, "2"]}
        ])
    );

    let negative = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=%7B__name__%21%3D%22name_beta%22%2Cjob%3D%22api%22%7D&time={base}"
        ),
    )
    .await;
    assert_eq!(negative.0, StatusCode::OK);
    assert_eq!(
        negative.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "name_alpha", "job": "api"}, "value": [base, "1"]},
            {"metric": {"__name__": "other_delta", "job": "api"}, "value": [base, "4"]}
        ])
    );

    let duplicate = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=%7B__name__%3D~%22name_.%2A%22%2C__name__%21~%22name_beta%22%2Cjob%3D%22api%22%7D&time={base}"
        ),
    )
    .await;
    assert_eq!(duplicate.0, StatusCode::OK);
    assert_eq!(
        duplicate.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "name_alpha", "job": "api"}, "value": [base, "1"]}
        ])
    );

    let all_names = get_json(
        &app,
        &format!("/prometheus/api/v1/query?query=%7B__name__%21%3D%22%22%7D&time={base}"),
    )
    .await;
    assert_eq!(all_names.0, StatusCode::OK);
    assert_eq!(all_names.1["data"]["result"].as_array().unwrap().len(), 4);

    let matches_empty = get_json(
        &app,
        &format!("/prometheus/api/v1/query?query=%7B__name__%21%3D%22missing%22%7D&time={base}"),
    )
    .await;
    assert_eq!(matches_empty.0, StatusCode::BAD_REQUEST);
    assert!(matches_empty.1["error"]
        .as_str()
        .unwrap()
        .contains("at least one non-empty matcher"));

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = get_json(
        &reopened_app,
        &format!(
            "/prometheus/api/v1/query?query=%7B__name__%3D~%22name_.%2A%22%2Cjob%3D%22api%22%7D&time={base}"
        ),
    )
    .await;
    assert_eq!(recovered.1, regex.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_two_promql_scalar_literals_match_prometheus() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_two_promql_scalars.db");
    let storage = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let app = router(storage.clone());

    for (query, expected) in [
        ("1", "1"),
        ("1.5", "1.5"),
        ("NaN", "NaN"),
        ("Inf", "+Inf"),
        ("%2BInf", "+Inf"),
        ("-Inf", "-Inf"),
        ("0x_12", "18"),
        ("00_1_23_4.56_7_8", "1234.5678"),
        ("1e2_3", "100000000000000000000000"),
    ] {
        let response = get_json(&app, &format!("/api/v1/query?query={query}&time=2")).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1["data"]["resultType"], "scalar");
        assert_eq!(
            response.1["data"]["result"],
            serde_json::json!([2, expected])
        );
    }

    let range = get_json(&app, "/api/v1/query_range?query=NaN&start=0&end=2&step=1").await;
    assert_eq!(range.0, StatusCode::OK);
    assert_eq!(range.1["data"]["resultType"], "matrix");
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[0, "NaN"], [1, "NaN"], [2, "NaN"]]
        }])
    );

    for query in ["1__2", "1_", "1._2"] {
        let error = get_json(&app, &format!("/api/v1/query?query={query}&time=2")).await;
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert_eq!(error.1["errorType"], "bad_data");
    }

    drop(app);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_two_promql_duration_literals_preserve_milliseconds() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_two_promql_durations.db");
    let base = 1_700_000_000_i64;
    let storage = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let app = router(storage.clone());
    let victoria = format!(
        "{{\"metric\":{{\"__name__\":\"duration_metric\",\"host\":\"a\"}},\"values\":[10.0,20.0,30.0],\"timestamps\":[{},{},{}]}}\n",
        base * 1_000,
        (base + 1) * 1_000,
        (base + 2) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (query, expected) in [("1m30s", "90"), ("500ms", "0.5")] {
        let scalar = get_json(
            &app,
            &format!("/api/v1/query?query={query}&time={}", base + 1),
        )
        .await;
        assert_eq!(scalar.0, StatusCode::OK, "{query}: {}", scalar.1);
        assert_eq!(scalar.1["data"]["resultType"], "scalar");
        assert_eq!(
            scalar.1["data"]["result"],
            serde_json::json!([base + 1, expected])
        );
    }

    let range_vector = get_json(
        &app,
        &format!(
            "/api/v1/query?query=duration_metric%5B1500ms%5D&time={}",
            base + 1
        ),
    )
    .await;
    assert_eq!(range_vector.0, StatusCode::OK, "{}", range_vector.1);
    assert_eq!(
        range_vector.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "10"], [base + 1, "20"]])
    );

    let average = get_json(
        &app,
        &format!(
            "/api/v1/query?query=avg_over_time%28duration_metric%5B1s500ms%5D%29&time={}",
            base + 1
        ),
    )
    .await;
    assert_eq!(average.0, StatusCode::OK, "{}", average.1);
    assert_eq!(
        average.1["data"]["result"][0]["metric"],
        serde_json::json!({"host":"a"})
    );
    assert_eq!(
        average.1["data"]["result"][0]["value"],
        serde_json::json!([base + 1, "15"])
    );

    let half_second_grid = get_json(
        &app,
        &format!(
            "/api/v1/query_range?query=1&start={base}&end={}&step=500ms",
            base + 1
        ),
    )
    .await;
    assert_eq!(half_second_grid.0, StatusCode::OK, "{}", half_second_grid.1);
    assert_eq!(
        half_second_grid.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "1"], [base as f64 + 0.5, "1"], [base + 1, "1"]])
    );

    for path in [
        "/api/v1/query?query=duration_metric%5B1m1h%5D&time=2",
        "/api/v1/query_range?query=1&start=0&end=2&step=1m1h",
    ] {
        let error = get_json(&app, path).await;
        assert_eq!(error.0, StatusCode::BAD_REQUEST, "{path}: {}", error.1);
        assert_eq!(error.1["errorType"], "bad_data");
    }

    drop(app);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_two_promql_string_literals_match_prometheus() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_two_promql_strings.db");
    let base = 1_700_000_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        8,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());

    let escaped = get_json(
        &app,
        &format!("/api/v1/query?query=%22hello%5Cn%5C%22world%5C%22%22&time={base}.0005"),
    )
    .await;
    assert_eq!(escaped.0, StatusCode::OK, "{}", escaped.1);
    assert_eq!(escaped.1["data"]["resultType"], "string");
    assert_eq!(
        escaped.1["data"]["result"],
        serde_json::json!([base as f64 + 0.001, "hello\n\"world\""])
    );

    let raw = get_json(
        &app,
        &format!("/api/v1/query?query=%60hello%5Cnworld%60&time={base}"),
    )
    .await;
    assert_eq!(raw.0, StatusCode::OK, "{}", raw.1);
    assert_eq!(
        raw.1["data"]["result"],
        serde_json::json!([base, "hello\\nworld"])
    );

    let range = get_json(
        &app,
        "/api/v1/query_range?query=%22hello%22&start=0&end=1&step=500ms",
    )
    .await;
    assert_eq!(range.0, StatusCode::BAD_REQUEST, "{}", range.1);
    assert_eq!(
        range.1["error"],
        "invalid parameter \"query\": invalid expression type \"string\" for range query, must be Scalar or instant Vector"
    );

    let invalid = get_json(
        &app,
        &format!("/api/v1/query?query=%22bad%5Cq%22&time={base}"),
    )
    .await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    drop(app);
    storage.shutdown().await.unwrap();

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let response = get_json(
        &reopened_app,
        &format!("/api/v1/query?query=%22reopened%22&time={base}"),
    )
    .await;
    assert_eq!(response.0, StatusCode::OK, "{}", response.1);
    assert_eq!(
        response.1["data"]["result"],
        serde_json::json!([base, "reopened"])
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_two_promql_grid_and_lookback_match_prometheus() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_two_promql_grid.db");
    let base = 1_700_000_000_i64;
    let storage = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let app = router(storage.clone());
    let victoria = format!(
        "{{\"metric\":{{\"__name__\":\"grid_metric\",\"host\":\"a\"}},\"values\":[10.0],\"timestamps\":[{}]}}\n",
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let default_lookback = get_json(
        &app,
        &format!("/api/v1/query?query=grid_metric&time={}", base + 10),
    )
    .await;
    assert_eq!(default_lookback.0, StatusCode::OK, "{}", default_lookback.1);
    assert_eq!(
        default_lookback.1["data"]["result"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let excluded = get_json(
        &app,
        &format!(
            "/api/v1/query?query=grid_metric&time={}&lookback_delta=10s",
            base + 10
        ),
    )
    .await;
    assert_eq!(excluded.0, StatusCode::OK, "{}", excluded.1);
    assert_eq!(excluded.1["data"]["result"], serde_json::json!([]));

    let included = get_json(
        &app,
        &format!(
            "/api/v1/query?query=grid_metric&time={}&lookback_delta=10001ms",
            base + 10
        ),
    )
    .await;
    assert_eq!(included.0, StatusCode::OK, "{}", included.1);
    assert_eq!(included.1["data"]["result"].as_array().unwrap().len(), 1);

    let zero_uses_default = get_json(
        &app,
        &format!(
            "/api/v1/query?query=grid_metric&time={}&lookback_delta=0",
            base + 10
        ),
    )
    .await;
    assert_eq!(
        zero_uses_default.0,
        StatusCode::OK,
        "{}",
        zero_uses_default.1
    );
    assert_eq!(
        zero_uses_default.1["data"]["result"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let range = get_json(
        &app,
        &format!(
            "/api/v1/query_range?query=grid_metric&start={base}.5&end={}&step=500ms&lookback_delta=10s",
            base + 11
        ),
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([
            [base as f64 + 0.5, "10"],
            [base + 1, "10"],
            [base as f64 + 1.5, "10"],
            [base + 2, "10"],
            [base as f64 + 2.5, "10"],
            [base + 3, "10"],
            [base as f64 + 3.5, "10"],
            [base + 4, "10"],
            [base as f64 + 4.5, "10"],
            [base + 5, "10"],
            [base as f64 + 5.5, "10"],
            [base + 6, "10"],
            [base as f64 + 6.5, "10"],
            [base + 7, "10"],
            [base as f64 + 7.5, "10"],
            [base + 8, "10"],
            [base as f64 + 8.5, "10"],
            [base + 9, "10"],
            [base as f64 + 9.5, "10"]
        ])
    );

    let scalar_grid = get_json(
        &app,
        "/api/v1/query_range?query=1&start=0&end=1.1&step=500ms&lookback_delta=1s",
    )
    .await;
    assert_eq!(scalar_grid.0, StatusCode::OK, "{}", scalar_grid.1);
    assert_eq!(
        scalar_grid.1["data"]["result"][0]["values"],
        serde_json::json!([[0, "1"], [0.5, "1"], [1, "1"]])
    );

    let invalid = get_json(
        &app,
        "/api/v1/query?query=grid_metric&time=2&lookback_delta=1.5s",
    )
    .await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    drop(app);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_two_promql_value_types_match_prometheus() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_two_promql_types.db");
    let base = 1_700_000_000_i64;
    let storage = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let app = router(storage.clone());
    let victoria = format!(
        "{{\"metric\":{{\"__name__\":\"type_metric\",\"host\":\"a\"}},\"values\":[10.0],\"timestamps\":[{}]}}\n",
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (path, result_type, result_len) in [
        (
            format!("/api/v1/query?query=type_metric&time={base}"),
            "vector",
            1,
        ),
        (
            format!("/api/v1/query?query=missing_metric&time={base}"),
            "vector",
            0,
        ),
        (
            format!("/api/v1/query?query=type_metric%5B1s%5D&time={base}"),
            "matrix",
            1,
        ),
        (
            format!("/api/v1/query?query=missing_metric%5B1s%5D&time={base}"),
            "matrix",
            0,
        ),
    ] {
        let response = get_json(&app, &path).await;
        assert_eq!(response.0, StatusCode::OK, "{path}: {}", response.1);
        assert_eq!(response.1["data"]["resultType"], result_type);
        assert_eq!(
            response.1["data"]["result"].as_array().unwrap().len(),
            result_len
        );
    }

    let scalar = get_json(&app, &format!("/api/v1/query?query=1&time={base}")).await;
    assert_eq!(scalar.0, StatusCode::OK, "{}", scalar.1);
    assert_eq!(scalar.1["data"]["resultType"], "scalar");
    assert_eq!(scalar.1["data"]["result"], serde_json::json!([base, "1"]));

    let string = get_json(
        &app,
        &format!("/api/v1/query?query=%22value%22&time={base}"),
    )
    .await;
    assert_eq!(string.0, StatusCode::OK, "{}", string.1);
    assert_eq!(string.1["data"]["resultType"], "string");
    assert_eq!(
        string.1["data"]["result"],
        serde_json::json!([base, "value"])
    );

    let range_scalar = get_json(&app, "/api/v1/query_range?query=1&start=0&end=1&step=1").await;
    assert_eq!(range_scalar.0, StatusCode::OK, "{}", range_scalar.1);
    assert_eq!(range_scalar.1["data"]["resultType"], "matrix");

    drop(app);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_two_promql_errors_match_the_documented_contract() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_two_promql_errors.db");
    let storage = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let app = router(storage.clone());

    for (path, expected) in [
        (
            "/prometheus/api/v1/query",
            "invalid parameter \"query\": unknown position: parse error: no expression found in input",
        ),
        (
            "/prometheus/api/v1/query?query=up&time=bad",
            "invalid parameter \"time\": invalid time value for 'time': cannot parse \"bad\" to a valid timestamp",
        ),
        (
            "/prometheus/api/v1/query?query=up&lookback_delta=1.5s",
            "error parsing lookback delta duration: cannot parse \"1.5s\" to a valid duration",
        ),
        (
            "/prometheus/api/v1/query_range?query=1",
            "invalid parameter \"start\": cannot parse \"\" to a valid timestamp",
        ),
        (
            "/prometheus/api/v1/query_range?query=1&start=0",
            "invalid parameter \"end\": cannot parse \"\" to a valid timestamp",
        ),
        (
            "/prometheus/api/v1/query_range?query=1&start=0&end=1",
            "invalid parameter \"step\": cannot parse \"\" to a valid duration",
        ),
        (
            "/prometheus/api/v1/query_range?query=1&start=bad&end=1&step=1",
            "invalid parameter \"start\": cannot parse \"bad\" to a valid timestamp",
        ),
        (
            "/prometheus/api/v1/query_range?query=1&start=0&end=bad&step=1",
            "invalid parameter \"end\": cannot parse \"bad\" to a valid timestamp",
        ),
        (
            "/prometheus/api/v1/query_range?query=1&start=0&end=1&step=bad",
            "invalid parameter \"step\": cannot parse \"bad\" to a valid duration",
        ),
        (
            "/prometheus/api/v1/query_range?query=1&start=2&end=1&step=1",
            "invalid parameter \"end\": end timestamp must not be before start time",
        ),
        (
            "/prometheus/api/v1/query_range?query=up%5B1m%5D&start=0&end=1&step=1",
            "invalid parameter \"query\": invalid expression type \"range vector\" for range query, must be Scalar or instant Vector",
        ),
        (
            "/prometheus/api/v1/query_range?query=%22value%22&start=0&end=1&step=1",
            "invalid parameter \"query\": invalid expression type \"string\" for range query, must be Scalar or instant Vector",
        ),
        (
            "/prometheus/api/v1/query?query=rate%28up%5B5m%5D%29",
            "invalid parameter \"query\": unsupported PromQL expression (parsed as function call)",
        ),
        (
            "/prometheus/api/v1/query?query=up&extra=x",
            "invalid parameter \"extra\": unsupported query parameter",
        ),
    ] {
        let response = get_json(&app, path).await;
        assert_eq!(response.0, StatusCode::BAD_REQUEST, "{path}: {}", response.1);
        assert_eq!(response.1["status"], "error", "{path}");
        assert_eq!(response.1["errorType"], "bad_data", "{path}");
        assert_eq!(response.1["error"], expected, "{path}");
        assert_eq!(response.1.as_object().unwrap().len(), 3, "{path}");
    }

    let post = post_form(
        &app,
        "/prometheus/api/v1/query_range",
        "query=1&start=0&end=1&step=bad",
    )
    .await;
    assert_eq!(post.0, StatusCode::BAD_REQUEST, "{}", post.1);
    assert_eq!(
        post.1["error"],
        "invalid parameter \"step\": cannot parse \"bad\" to a valid duration"
    );

    drop(app);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_four_cancels_dropped_promql_requests_and_reuses_the_reader() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_four_cancel.db");
    let storage = Storage::start(database, extension, 1, 16, DEFAULT_RAW_RETENTION).unwrap();
    let app = router(storage.clone());
    let base = 1_700_000_000_i64;
    let mut victoria = String::new();
    for series in 0..4_000 {
        use std::fmt::Write;
        writeln!(
            victoria,
            "{{\"metric\":{{\"__name__\":\"cancel_metric\",\"host\":\"h{series}\"}},\"values\":[{series}.0],\"timestamps\":[{}]}}",
            base * 1_000
        )
        .unwrap();
    }
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let slow_app = app.clone();
    let request = format!(
        "/prometheus/api/v1/query_range?query=cancel_metric&start={base}&end={}&step=1",
        base + 10_999
    );
    let task = tokio::spawn(async move {
        slow_app
            .oneshot(Request::get(request).body(Body::empty()).unwrap())
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    task.abort();
    let _ = task.await;

    let stats = tokio::time::timeout(std::time::Duration::from_secs(2), storage.stats())
        .await
        .expect("cancelled query kept the sole reader busy")
        .unwrap();
    assert_eq!(stats.api_read_cancelled, 1);
    assert_eq!(stats.api_read_in_flight, 0);
    assert_eq!(stats.api_read_errors, 0);

    let fresh = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        get_json(
            &app,
            &format!(
                "/prometheus/api/v1/query?query=cancel_metric%7Bhost%3D%22h0%22%7D&time={base}"
            ),
        ),
    )
    .await
    .expect("reader did not recover after cancellation");
    assert_eq!(fresh.0, StatusCode::OK);
    assert_eq!(fresh.1["data"]["result"][0]["value"][1], "0");

    drop(app);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_two_promql_limits_bound_grid_work_results_response_and_deadline() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_two_promql_limits.db");
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let base = 1_700_000_000_i64;
    let victoria = format!(
        "{{\"metric\":{{\"__name__\":\"limit_metric\",\"host\":\"a\"}},\"values\":[1,2,3],\"timestamps\":[{},{},{}]}}\n",
        base * 1_000,
        (base + 1) * 1_000,
        (base + 2) * 1_000
    );
    let ingest_app = router(storage.clone());
    assert_no_content(post_body(&ingest_app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(
        post_json(&ingest_app, "/api/v1/flush").await.0,
        StatusCode::OK
    );

    let grid_app = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_points_per_series: 2,
            ..PromQueryLimits::default()
        },
    );
    let grid = get_json(
        &grid_app,
        &format!(
            "/prometheus/api/v1/query_range?query=1&start={base}&end={}&step=1",
            base + 2
        ),
    )
    .await;
    assert_eq!(grid.0, StatusCode::BAD_REQUEST, "{}", grid.1);
    assert_eq!(grid.1["errorType"], "bad_data");
    assert!(grid.1["error"].as_str().unwrap().contains("2 points"));

    let result_app = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_result_points: 2,
            ..PromQueryLimits::default()
        },
    );
    let result = get_json(
        &result_app,
        &format!(
            "/prometheus/api/v1/query_range?query=1&start={base}&end={}&step=1",
            base + 2
        ),
    )
    .await;
    assert_eq!(result.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", result.1);
    assert_eq!(result.1["errorType"], "execution");
    assert!(result.1["error"]
        .as_str()
        .unwrap()
        .contains("result-point limit of 2"));

    let work_app = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 2,
            ..PromQueryLimits::default()
        },
    );
    for query in [
        "limit_metric%5B10s%5D",
        "avg_over_time%28limit_metric%5B10s%5D%29",
    ] {
        let work = get_json(
            &work_app,
            &format!("/prometheus/api/v1/query?query={query}&time={}", base + 2),
        )
        .await;
        assert_eq!(work.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", work.1);
        assert_eq!(work.1["errorType"], "execution");
        assert!(work.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 2 exceeded"));
    }

    let response_app = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_response_bytes: 32,
            ..PromQueryLimits::default()
        },
    );
    let response = get_json(
        &response_app,
        "/prometheus/api/v1/query?query=%22a-long-string%22&time=1",
    )
    .await;
    assert_eq!(
        response.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        response.1
    );
    assert!(response.1["error"]
        .as_str()
        .unwrap()
        .contains("response-size limit of 32 bytes"));

    let mut deadline_fixture = String::new();
    for series in 0..2_000 {
        use std::fmt::Write;
        writeln!(
            deadline_fixture,
            "{{\"metric\":{{\"__name__\":\"deadline_metric\",\"host\":\"h{series}\"}},\"values\":[{series}],\"timestamps\":[{}]}}",
            base * 1_000
        )
        .unwrap();
    }
    assert_no_content(post_body(&ingest_app, "/api/v1/import", deadline_fixture.as_bytes()).await);
    assert_eq!(
        post_json(&ingest_app, "/api/v1/flush").await.0,
        StatusCode::OK
    );

    let deadline_app = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            deadline: std::time::Duration::from_millis(1),
            ..PromQueryLimits::default()
        },
    );
    let deadline = get_json(
        &deadline_app,
        &format!(
            "/prometheus/api/v1/query_range?query=deadline_metric&start={base}&end={}&step=1",
            base + 49
        ),
    )
    .await;
    assert_eq!(deadline.0, StatusCode::GATEWAY_TIMEOUT, "{}", deadline.1);
    assert_eq!(deadline.1["errorType"], "timeout");

    let recovered = get_json(
        &ingest_app,
        &format!(
            "/prometheus/api/v1/query?query=limit_metric&time={}",
            base + 2
        ),
    )
    .await;
    assert_eq!(recovered.0, StatusCode::OK, "{}", recovered.1);
    assert_eq!(recovered.1["data"]["result"][0]["value"][1], "3");

    drop((
        grid_app,
        result_app,
        work_app,
        response_app,
        deadline_app,
        ingest_app,
    ));
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let durable = get_json(
        &reopened_app,
        &format!(
            "/prometheus/api/v1/query?query=limit_metric&time={}",
            base + 2
        ),
    )
    .await;
    assert_eq!(durable.0, StatusCode::OK, "{}", durable.1);
    drop(reopened_app);
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

async fn get_body(app: &axum::Router, path: &str) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    (status, body.to_vec())
}

async fn post_form(app: &axum::Router, path: &str, body: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::post(path)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

fn assert_no_content(response: (StatusCode, Vec<u8>)) {
    assert_eq!(response.0, StatusCode::NO_CONTENT);
    assert!(response.1.is_empty());
}

fn persisted_rows(database: &Path, extension: &Path) -> Vec<(String, i64, f64)> {
    let conn = open_with_extension(database, extension);
    let mut stmt = conn
        .prepare("SELECT name, ts, value FROM metric_samples ORDER BY name, ts, value")
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn open_with_extension(database: &Path, extension: &Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(database).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(extension, None::<&str>).unwrap();
    }
    conn.load_extension_disable().unwrap();
    conn
}

fn extension_path() -> PathBuf {
    std::env::var_os("TIMELESS_EXT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("..")
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
