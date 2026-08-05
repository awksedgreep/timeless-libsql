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
    assert_eq!(flush.1["through_points"], 6);
    assert_eq!(flush.1["completed_batches"], 5);
    assert_eq!(flush.1["completed_points"], 6);
    assert_eq!(flush.1["failed_batches"], 0);
    assert_eq!(flush.1["queued_batches"], 0);
    assert_eq!(flush.1["in_flight_batches"], 0);

    let health = get_json(&app, "/health").await;
    assert_eq!(health.1["points"], 6);
    assert_eq!(health.1["series"], 2);
    assert_eq!(health.1["import_errors"], 6);
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.admitted_batches, 5);
    assert_eq!(stats.admitted_points, 6);
    assert_eq!(stats.completed_batches, 5);
    assert_eq!(stats.completed_points, 6);
    assert_eq!(stats.import_errors, 6);
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
    assert_eq!(stats.extension_prometheus_ingest_points, 3);
    assert_eq!(stats.extension_prometheus_ingest_errors, 3);
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
            ("contract_prom".into(), 1_700_000_000, Some(4.5)),
            ("contract_prom".into(), 1_700_000_001, None),
            ("contract_prom".into(), 1_700_000_002, Some(f64::INFINITY),),
            ("contract_vm".into(), 1_700_000_000, Some(1.5)),
            ("contract_vm".into(), 1_700_000_001, Some(2.5)),
            ("contract_vm".into(), 1_700_000_002, Some(3.5)),
        ]
    );

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let recovered = reopened.stats().await.unwrap();
    assert_eq!(recovered.series, 2);
    assert_eq!(recovered.disk_points, 6);
    assert_eq!(recovered.buffered_points, 0);
    let reopened_app = router(reopened.clone());
    let ieee = prom_query_range(
        &reopened_app,
        "contract_prom",
        1_700_000_000,
        1_700_000_002,
        1,
    )
    .await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"][0]["values"],
        serde_json::json!([
            [1_700_000_000, "4.5"],
            [1_700_000_001, "NaN"],
            [1_700_000_002, "+Inf"]
        ])
    );
    drop(reopened_app);
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
        "/prometheus/api/v1/query_range?query=rate%28prom_cpu%29&start=0&end=10&step=1",
    ] {
        let error = get_json(&app, path).await;
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert_eq!(error.1["status"], "error");
        assert_eq!(error.1["errorType"], "bad_data");
        assert!(error.1["error"].as_str().unwrap().len() > 8);
    }
    let sum = get_json(
        &app,
        "/prometheus/api/v1/query_range?query=sum%28prom_cpu%29&start=0&end=10&step=1",
    )
    .await;
    assert_eq!(sum.0, StatusCode::OK, "{}", sum.1);
    assert_eq!(sum.1["data"]["result"], serde_json::json!([]));

    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.api_promql_requests, 10);
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
async fn session_three_promql_temporal_modifiers_preserve_selection_and_output_time() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_three_temporal.db");
    let base = 1_700_300_000_i64;
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
            "{{\"metric\":{{\"__name__\":\"temporal_metric\",\"host\":\"a\"}},",
            "\"values\":[1,2,3,4,5,6,7],\"timestamps\":[{},{},{},{},{},{},{}]}}"
        ),
        (base - 30) * 1_000,
        (base - 20) * 1_000,
        (base - 10) * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let positive = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=temporal_metric%20offset%2020s&time={}",
            base + 30
        ),
    )
    .await;
    assert_eq!(positive.0, StatusCode::OK);
    assert_eq!(
        positive.1["data"]["result"][0]["value"],
        serde_json::json!([base + 30, "5"])
    );

    let negative = get_json(
        &app,
        &format!("/prometheus/api/v1/query?query=temporal_metric%20offset%20-20s&time={base}"),
    )
    .await;
    assert_eq!(negative.0, StatusCode::OK);
    assert_eq!(
        negative.1["data"]["result"][0]["value"],
        serde_json::json!([base, "6"])
    );

    let at_timestamp = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=temporal_metric%20%40%20{}&time={}",
            base + 10,
            base + 30
        ),
    )
    .await;
    assert_eq!(at_timestamp.0, StatusCode::OK);
    assert_eq!(
        at_timestamp.1["data"]["result"][0]["value"],
        serde_json::json!([base + 30, "5"])
    );

    let offset_then_at = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=temporal_metric%20offset%2010s%20%40%20{}&time={}",
            base + 20,
            base + 30
        ),
    )
    .await;
    assert_eq!(offset_then_at.0, StatusCode::OK);
    assert_eq!(
        offset_then_at.1["data"]["result"][0]["value"],
        serde_json::json!([base + 30, "5"])
    );

    let positive_range = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query_range?query=temporal_metric%20offset%2010s&start={base}&end={}&step=10",
            base + 30
        ),
    )
    .await;
    assert_eq!(positive_range.0, StatusCode::OK);
    assert_eq!(
        positive_range.1["data"]["result"][0]["values"],
        serde_json::json!([
            [base, "3"],
            [base + 10, "4"],
            [base + 20, "5"],
            [base + 30, "6"]
        ])
    );

    for (modifier, value) in [("start%28%29", "4"), ("end%28%29", "7")] {
        let fixed = get_json(
            &app,
            &format!(
                "/prometheus/api/v1/query_range?query=temporal_metric%20%40%20{modifier}&start={base}&end={}&step=10",
                base + 30
            ),
        )
        .await;
        assert_eq!(fixed.0, StatusCode::OK);
        assert_eq!(
            fixed.1["data"]["result"][0]["values"],
            serde_json::json!([
                [base, value],
                [base + 10, value],
                [base + 20, value],
                [base + 30, value]
            ])
        );
    }

    let range_root = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=temporal_metric%5B20s%5D%20%40%20{}&time={}",
            base + 10,
            base + 30
        ),
    )
    .await;
    assert_eq!(range_root.0, StatusCode::OK);
    assert_eq!(
        range_root.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "4"], [base + 10, "5"]])
    );

    let fixed_average = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query_range?query=avg_over_time%28temporal_metric%5B20s%5D%20%40%20end%28%29%20offset%2010s%29&start={base}&end={}&step=10",
            base + 30
        ),
    )
    .await;
    assert_eq!(fixed_average.0, StatusCode::OK);
    assert_eq!(
        fixed_average.1["data"]["result"][0]["values"],
        serde_json::json!([
            [base, "5.5"],
            [base + 10, "5.5"],
            [base + 20, "5.5"],
            [base + 30, "5.5"]
        ])
    );

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = get_json(
        &reopened_app,
        &format!(
            "/prometheus/api/v1/query?query=temporal_metric%20offset%2020s&time={}",
            base + 30
        ),
    )
    .await;
    assert_eq!(recovered.1, positive.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_three_promql_subqueries_align_bound_cancel_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_three_subqueries.db");
    // A minute-aligned base makes the pinned 15-second default-resolution
    // expectations readable without weakening global-alignment coverage.
    let base = 1_700_300_040_i64;
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
            "{{\"metric\":{{\"__name__\":\"subquery_metric\",\"host\":\"a\"}},",
            "\"values\":[1,2,3,4,5,6,7],\"timestamps\":[{},{},{},{},{},{},{}]}}"
        ),
        (base - 30) * 1_000,
        (base - 20) * 1_000,
        (base - 10) * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let aligned = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=subquery_metric%5B30s%3A10s%5D&time={}",
            base + 25
        ),
    )
    .await;
    assert_eq!(aligned.0, StatusCode::OK, "{}", aligned.1);
    assert_eq!(aligned.1["data"]["resultType"], "matrix");
    assert_eq!(
        aligned.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "4"], [base + 10, "5"], [base + 20, "6"]])
    );

    let default_resolution = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=subquery_metric%5B30s%3A%5D&time={}",
            base + 30
        ),
    )
    .await;
    assert_eq!(
        default_resolution.0,
        StatusCode::OK,
        "{}",
        default_resolution.1
    );
    assert_eq!(
        default_resolution.1["data"]["result"][0]["values"],
        serde_json::json!([[base + 15, "5"], [base + 30, "7"]])
    );

    let offset = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=subquery_metric%5B20s%3A10s%5D%20offset%2010s&time={}",
            base + 30
        ),
    )
    .await;
    assert_eq!(offset.0, StatusCode::OK, "{}", offset.1);
    assert_eq!(
        offset.1["data"]["result"][0]["values"],
        serde_json::json!([[base + 10, "5"], [base + 20, "6"]])
    );

    let fixed = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=subquery_metric%5B20s%3A10s%5D%20%40%20{}&time={}",
            base + 20,
            base + 30
        ),
    )
    .await;
    assert_eq!(fixed.0, StatusCode::OK, "{}", fixed.1);
    assert_eq!(fixed.1["data"]["result"], offset.1["data"]["result"]);

    let average = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=avg_over_time%28subquery_metric%5B20s%3A10s%5D%29&time={}",
            base + 30
        ),
    )
    .await;
    assert_eq!(average.0, StatusCode::OK, "{}", average.1);
    assert_eq!(
        average.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "a"},
            "value": [base + 30, "6.5"]
        }])
    );

    let average_range = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query_range?query=avg_over_time%28subquery_metric%5B20s%3A10s%5D%29&start={base}&end={}&step=10",
            base + 30
        ),
    )
    .await;
    assert_eq!(average_range.0, StatusCode::OK, "{}", average_range.1);
    assert_eq!(
        average_range.1["data"]["result"][0]["values"],
        serde_json::json!([
            [base, "3.5"],
            [base + 10, "4.5"],
            [base + 20, "5.5"],
            [base + 30, "6.5"]
        ])
    );

    for (anchor, value) in [("start%28%29", "3.5"), ("end%28%29", "6.5")] {
        let fixed_range = get_json(
            &app,
            &format!(
                "/prometheus/api/v1/query_range?query=avg_over_time%28subquery_metric%5B20s%3A10s%5D%20%40%20{anchor}%29&start={base}&end={}&step=10",
                base + 30
            ),
        )
        .await;
        assert_eq!(fixed_range.0, StatusCode::OK, "{}", fixed_range.1);
        assert_eq!(
            fixed_range.1["data"]["result"][0]["values"],
            serde_json::json!([
                [base, value],
                [base + 10, value],
                [base + 20, value],
                [base + 30, value]
            ])
        );
    }

    let nested_root = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=avg_over_time%28subquery_metric%5B20s%3A10s%5D%29%5B20s%3A10s%5D&time={}",
            base + 30
        ),
    )
    .await;
    assert_eq!(nested_root.0, StatusCode::OK, "{}", nested_root.1);
    assert_eq!(
        nested_root.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "a"},
            "values": [[base + 20, "5.5"], [base + 30, "6.5"]]
        }])
    );

    let nested_average = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=avg_over_time%28avg_over_time%28subquery_metric%5B20s%3A10s%5D%29%5B20s%3A10s%5D%29&time={}",
            base + 30
        ),
    )
    .await;
    assert_eq!(nested_average.0, StatusCode::OK, "{}", nested_average.1);
    assert_eq!(nested_average.1["data"]["result"][0]["value"][1], "6");
    let query_stats = storage.stats().await.unwrap();
    assert!(
        query_stats.api_promql_intermediate_points > 0,
        "subquery intermediate accounting stayed zero: {query_stats:?}"
    );

    let range_root = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query_range?query=subquery_metric%5B20s%3A10s%5D&start={base}&end={}&step=10",
            base + 30
        ),
    )
    .await;
    assert_eq!(range_root.0, StatusCode::BAD_REQUEST);
    assert_eq!(range_root.1["errorType"], "bad_data");
    assert!(range_root.1["error"]
        .as_str()
        .unwrap()
        .contains("range vector"));

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 2,
            ..PromQueryLimits::default()
        },
    );
    let rejected = get_json(
        &limited,
        &format!(
            "/prometheus/api/v1/query?query=avg_over_time%28subquery_metric%5B30s%3A10s%5D%29&time={}",
            base + 30
        ),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum intermediate-work limit of 2 points"));

    let nested_limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 7,
            ..PromQueryLimits::default()
        },
    );
    let nested_rejected = get_json(
        &nested_limited,
        &format!(
            "/prometheus/api/v1/query?query=avg_over_time%28avg_over_time%28subquery_metric%5B40s%3A10s%5D%29%5B40s%3A10s%5D%29&time={}",
            base + 30
        ),
    )
    .await;
    assert_eq!(nested_rejected.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(
        nested_rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("maximum intermediate-work limit of 7 points"),
        "{}",
        nested_rejected.1
    );

    drop(nested_limited);
    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = get_json(
        &reopened_app,
        &format!(
            "/prometheus/api/v1/query?query=subquery_metric%5B30s%3A10s%5D&time={}",
            base + 25
        ),
    )
    .await;
    assert_eq!(recovered.1, aligned.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_four_promql_unary_minus_preserves_types_labels_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_four_unary.db");
    let base = 1_700_400_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let victoria = format!(
        "{{\"metric\":{{\"__name__\":\"unary_metric\",\"host\":\"a\"}},\"values\":[1,-2],\"timestamps\":[{},{}]}}\n",
        base * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let instant = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=-unary_metric&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(instant.0, StatusCode::OK, "{}", instant.1);
    assert_eq!(instant.1["data"]["resultType"], "vector");
    assert_eq!(
        instant.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "a"},
            "value": [base + 10, "2"]
        }])
    );

    let range = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query_range?query=-unary_metric&start={base}&end={}&step=10",
            base + 10
        ),
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "-1"], [base + 10, "2"]])
    );

    let nested = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=-avg_over_time%28unary_metric%5B20s%5D%29&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(
        nested.1["data"]["result"][0]["metric"],
        serde_json::json!({"host": "a"})
    );
    assert_eq!(nested.1["data"]["result"][0]["value"][1], "0.5");

    let inside_subquery = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=avg_over_time%28%28-unary_metric%29%5B20s%3A10s%5D%29&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(inside_subquery.0, StatusCode::OK, "{}", inside_subquery.1);
    assert_eq!(
        inside_subquery.1["data"]["result"][0]["metric"],
        serde_json::json!({"host": "a"})
    );
    assert_eq!(inside_subquery.1["data"]["result"][0]["value"][1], "0.5");

    let double = get_json(
        &app,
        &format!(
            "/prometheus/api/v1/query?query=-%28-unary_metric%29&time={}",
            base + 10
        ),
    )
    .await;
    assert_eq!(double.0, StatusCode::OK, "{}", double.1);
    assert_eq!(double.1["data"]["result"][0]["value"][1], "-2");

    for (query, expected) in [
        ("-%281%29", "-1"),
        ("-%28NaN%29", "NaN"),
        ("-%28Inf%29", "-Inf"),
        ("-%28-Inf%29", "+Inf"),
    ] {
        let scalar = get_json(
            &app,
            &format!("/prometheus/api/v1/query?query={query}&time={base}"),
        )
        .await;
        assert_eq!(scalar.0, StatusCode::OK, "{query}: {}", scalar.1);
        assert_eq!(scalar.1["data"]["resultType"], "scalar");
        assert_eq!(scalar.1["data"]["result"][1], expected);
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = get_json(
        &limited,
        "/prometheus/api/v1/query_range?query=-%281%29&start=0&end=1&step=1",
    )
    .await;
    assert_eq!(rejected.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum intermediate-work limit of 1 points"));

    for (query, error) in [
        (
            "-%28%22text%22%29",
            "unary expression only allowed on expressions of type scalar or vector",
        ),
        (
            "-%28unary_metric%5B20s%5D%29",
            "unary expression only allowed on expressions of type scalar or vector",
        ),
    ] {
        let rejected_type = get_json(
            &app,
            &format!("/prometheus/api/v1/query?query={query}&time={base}"),
        )
        .await;
        assert_eq!(
            rejected_type.0,
            StatusCode::BAD_REQUEST,
            "{query}: {}",
            rejected_type.1
        );
        assert!(
            rejected_type.1["error"].as_str().unwrap().contains(error),
            "{query}: {}",
            rejected_type.1
        );
    }

    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = get_json(
        &reopened_app,
        &format!(
            "/prometheus/api/v1/query?query=-unary_metric&time={}",
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
async fn session_four_promql_arithmetic_and_one_to_one_match_oracle_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_four_arithmetic.db");
    let base = 1_700_410_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let mut victoria = String::new();
    for (metric, host, zone, first, second) in [
        ("arithmetic_lhs", "a", "east", 8.0, 10.0),
        ("arithmetic_lhs", "b", "west", 10.0, 12.0),
        ("arithmetic_lhs", "c", "north", 12.0, 14.0),
        ("arithmetic_rhs", "a", "east", 2.0, 4.0),
        ("arithmetic_rhs", "b", "west", 5.0, 7.0),
        ("arithmetic_rhs", "d", "south", 4.0, 6.0),
        ("arithmetic_rhs_duplicate", "a", "east", 3.0, 5.0),
    ] {
        use std::fmt::Write;
        writeln!(
            victoria,
            "{{\"metric\":{{\"__name__\":\"{metric}\",\"host\":\"{host}\",\"zone\":\"{zone}\"}},\"values\":[{first},{second}],\"timestamps\":[{},{}]}}",
            base * 1_000,
            (base + 10) * 1_000,
        )
        .unwrap();
    }
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (query, expected) in [
        ("1 + 2 * 3", "7"),
        ("5 - 8", "-3"),
        ("3 * 4", "12"),
        ("5 / 2", "2.5"),
        ("5 % 2", "1"),
        ("2 ^ 3", "8"),
        ("1 / 0", "+Inf"),
        ("0 / 0", "NaN"),
        ("5 % 0", "NaN"),
        ("(-1) ^ 0.5", "NaN"),
    ] {
        let scalar = prom_query(&app, query, base).await;
        assert_eq!(scalar.0, StatusCode::OK, "{query}: {}", scalar.1);
        assert_eq!(scalar.1["data"]["resultType"], "scalar", "{query}");
        assert_eq!(scalar.1["data"]["result"][1], expected, "{query}");
    }

    for (operator, expected) in [
        ("+", "14"),
        ("-", "6"),
        ("*", "40"),
        ("/", "2.5"),
        ("%", "2"),
        ("^", "10000"),
    ] {
        let query = format!("arithmetic_lhs{{host=\"a\"}} {operator} arithmetic_rhs{{host=\"a\"}}");
        let result = prom_query(&app, &query, base + 10).await;
        assert_eq!(result.0, StatusCode::OK, "{query}: {}", result.1);
        assert_eq!(
            result.1["data"]["result"],
            serde_json::json!([{
                "metric": {"host": "a", "zone": "east"},
                "value": [base + 10, expected]
            }]),
            "{query}"
        );
    }

    let scalar_left = prom_query(&app, "20 - arithmetic_lhs{host=\"a\"}", base + 10).await;
    assert_eq!(scalar_left.0, StatusCode::OK, "{}", scalar_left.1);
    assert_eq!(scalar_left.1["data"]["result"][0]["value"][1], "10");
    assert_eq!(
        scalar_left.1["data"]["result"][0]["metric"],
        serde_json::json!({"host": "a", "zone": "east"})
    );

    let divide_zero = prom_query(&app, "arithmetic_lhs{host=\"a\"} / 0", base + 10).await;
    assert_eq!(divide_zero.0, StatusCode::OK, "{}", divide_zero.1);
    assert_eq!(divide_zero.1["data"]["result"][0]["value"][1], "+Inf");

    let matched = prom_query(&app, "arithmetic_lhs + arithmetic_rhs", base + 10).await;
    assert_eq!(matched.0, StatusCode::OK, "{}", matched.1);
    assert_eq!(
        matched.1["data"]["result"],
        serde_json::json!([
            {"metric": {"host": "a", "zone": "east"}, "value": [base + 10, "14"]},
            {"metric": {"host": "b", "zone": "west"}, "value": [base + 10, "19"]}
        ])
    );

    let range =
        prom_query_range(&app, "arithmetic_lhs + arithmetic_rhs", base, base + 10, 10).await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([
            {"metric": {"host": "a", "zone": "east"}, "values": [[base, "10"], [base + 10, "14"]]},
            {"metric": {"host": "b", "zone": "west"}, "values": [[base, "15"], [base + 10, "19"]]}
        ])
    );

    let duplicate = prom_query(
        &app,
        "arithmetic_lhs + {__name__=~\"arithmetic_rhs(_duplicate)?\"}",
        base + 10,
    )
    .await;
    assert_eq!(
        duplicate.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        duplicate.1
    );
    assert_eq!(duplicate.1["errorType"], "execution");
    assert!(duplicate.1["error"]
        .as_str()
        .unwrap()
        .contains("many-to-many matching not allowed"));

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 3,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query_range(
        &limited,
        "arithmetic_lhs{host=\"a\"} + arithmetic_rhs{host=\"a\"}",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum intermediate-work limit of 3 points"));

    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = prom_query(&reopened_app, "arithmetic_lhs + arithmetic_rhs", base + 10).await;
    assert_eq!(recovered.1, matched.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_four_promql_comparisons_filter_bool_bound_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_four_comparisons.db");
    let base = 1_700_420_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let victoria = format!(
        concat!(
            "{{\"metric\":{{\"__name__\":\"comparison_lhs\",\"host\":\"a\",\"zone\":\"east\"}},\"values\":[1,3],\"timestamps\":[{},{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"comparison_lhs\",\"host\":\"b\",\"zone\":\"west\"}},\"values\":[4,2],\"timestamps\":[{},{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"comparison_rhs\",\"host\":\"a\",\"zone\":\"east\"}},\"values\":[2,3],\"timestamps\":[{},{}]}}\n",
            "{{\"metric\":{{\"__name__\":\"comparison_rhs\",\"host\":\"b\",\"zone\":\"west\"}},\"values\":[3,5],\"timestamps\":[{},{}]}}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (query, expected) in [
        ("1 < bool 2", "1"),
        ("2 < bool 1", "0"),
        ("NaN != bool NaN", "1"),
        ("Inf >= bool Inf", "1"),
    ] {
        let scalar = prom_query(&app, query, base).await;
        assert_eq!(scalar.0, StatusCode::OK, "{query}: {}", scalar.1);
        assert_eq!(scalar.1["data"]["resultType"], "scalar");
        assert_eq!(scalar.1["data"]["result"][1], expected, "{query}");
    }

    let scalar_without_bool = prom_query(&app, "1 < 2", base).await;
    assert_eq!(scalar_without_bool.0, StatusCode::BAD_REQUEST);
    assert_eq!(scalar_without_bool.1["errorType"], "bad_data");

    let filtered = prom_query(&app, "comparison_lhs{host=\"a\"} == 3", base + 10).await;
    assert_eq!(filtered.0, StatusCode::OK, "{}", filtered.1);
    assert_eq!(
        filtered.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "comparison_lhs", "host": "a", "zone": "east"},
            "value": [base + 10, "3"]
        }])
    );

    for operator in ["!=", ">", ">="] {
        let query = format!("comparison_lhs{{host=\"a\"}} {operator} 2");
        let result = prom_query(&app, &query, base + 10).await;
        assert_eq!(result.0, StatusCode::OK, "{query}: {}", result.1);
        assert_eq!(result.1["data"]["result"][0]["value"][1], "3");
        assert_eq!(
            result.1["data"]["result"][0]["metric"]["__name__"],
            "comparison_lhs"
        );
    }
    for operator in ["<", "<="] {
        let query = format!("comparison_lhs{{host=\"a\"}} {operator} 2");
        let result = prom_query(&app, &query, base + 10).await;
        assert_eq!(result.0, StatusCode::OK, "{query}: {}", result.1);
        assert_eq!(result.1["data"]["result"], serde_json::json!([]));
    }

    let bool_false = prom_query(&app, "comparison_lhs{host=\"a\"} < bool 3", base + 10).await;
    assert_eq!(bool_false.0, StatusCode::OK, "{}", bool_false.1);
    assert_eq!(
        bool_false.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "a", "zone": "east"},
            "value": [base + 10, "0"]
        }])
    );

    let scalar_left = prom_query(&app, "3 <= comparison_lhs{host=\"a\"}", base + 10).await;
    assert_eq!(scalar_left.0, StatusCode::OK, "{}", scalar_left.1);
    assert_eq!(scalar_left.1["data"]["result"][0]["value"][1], "3");
    assert_eq!(
        scalar_left.1["data"]["result"][0]["metric"]["__name__"],
        "comparison_lhs"
    );

    let vector_filter = prom_query(&app, "comparison_lhs == comparison_rhs", base + 10).await;
    assert_eq!(vector_filter.0, StatusCode::OK, "{}", vector_filter.1);
    assert_eq!(
        vector_filter.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "comparison_lhs", "host": "a", "zone": "east"},
            "value": [base + 10, "3"]
        }])
    );

    let vector_bool = prom_query(&app, "comparison_lhs > bool comparison_rhs", base + 10).await;
    assert_eq!(vector_bool.0, StatusCode::OK, "{}", vector_bool.1);
    assert_eq!(
        vector_bool.1["data"]["result"],
        serde_json::json!([
            {"metric": {"host": "a", "zone": "east"}, "value": [base + 10, "0"]},
            {"metric": {"host": "b", "zone": "west"}, "value": [base + 10, "0"]}
        ])
    );

    let sparse = prom_query_range(&app, "comparison_lhs > 2", base, base + 10, 10).await;
    assert_eq!(sparse.0, StatusCode::OK, "{}", sparse.1);
    assert_eq!(
        sparse.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "comparison_lhs", "host": "a", "zone": "east"}, "values": [[base + 10, "3"]]},
            {"metric": {"__name__": "comparison_lhs", "host": "b", "zone": "west"}, "values": [[base, "4"]]}
        ])
    );

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 3,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query_range(
        &limited,
        "comparison_lhs{host=\"a\"} == comparison_rhs{host=\"a\"}",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum intermediate-work limit of 3 points"));

    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = prom_query(&reopened_app, "comparison_lhs == comparison_rhs", base + 10).await;
    assert_eq!(recovered.1, vector_filter.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_four_promql_set_operators_are_many_to_many_stepwise_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_four_set_operators.db");
    let base = 1_700_430_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let mut victoria = String::new();
    for (metric, host, zone, first, second) in [
        ("set_lhs", "a", "east", 1.0, 10.0),
        ("set_lhs", "b", "west", 2.0, 20.0),
        ("set_lhs", "c", "north", 3.0, 30.0),
        ("set_lhs_alt", "a", "east", 100.0, 110.0),
        ("set_rhs", "a", "east", 4.0, 40.0),
        ("set_rhs", "b", "west", 5.0, 50.0),
        ("set_rhs_alt", "a", "east", 400.0, 410.0),
    ] {
        use std::fmt::Write;
        writeln!(
            victoria,
            "{{\"metric\":{{\"__name__\":\"{metric}\",\"host\":\"{host}\",\"zone\":\"{zone}\"}},\"values\":[{first},{second}],\"timestamps\":[{},{}]}}",
            base * 1_000,
            (base + 10) * 1_000,
        )
        .unwrap();
    }
    use std::fmt::Write;
    writeln!(
        victoria,
        "{{\"metric\":{{\"__name__\":\"set_rhs\",\"host\":\"d\",\"zone\":\"south\"}},\"values\":[60],\"timestamps\":[{}]}}",
        (base + 10) * 1_000,
    )
    .unwrap();
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let and = prom_query(&app, "set_lhs and set_rhs", base + 10).await;
    assert_eq!(and.0, StatusCode::OK, "{}", and.1);
    assert_eq!(
        and.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "set_lhs", "host": "a", "zone": "east"}, "value": [base + 10, "10"]},
            {"metric": {"__name__": "set_lhs", "host": "b", "zone": "west"}, "value": [base + 10, "20"]}
        ])
    );

    let unless = prom_query(&app, "set_lhs unless set_rhs", base + 10).await;
    assert_eq!(unless.0, StatusCode::OK, "{}", unless.1);
    assert_eq!(
        unless.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "set_lhs", "host": "c", "zone": "north"},
            "value": [base + 10, "30"]
        }])
    );

    let or = prom_query(&app, "set_lhs or set_rhs", base + 10).await;
    assert_eq!(or.0, StatusCode::OK, "{}", or.1);
    assert_eq!(
        or.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "set_lhs", "host": "a", "zone": "east"}, "value": [base + 10, "10"]},
            {"metric": {"__name__": "set_lhs", "host": "b", "zone": "west"}, "value": [base + 10, "20"]},
            {"metric": {"__name__": "set_lhs", "host": "c", "zone": "north"}, "value": [base + 10, "30"]},
            {"metric": {"__name__": "set_rhs", "host": "d", "zone": "south"}, "value": [base + 10, "60"]}
        ])
    );

    let many = prom_query(
        &app,
        "{__name__=~\"set_lhs(_alt)?\"} and {__name__=~\"set_rhs(_alt)?\"}",
        base + 10,
    )
    .await;
    assert_eq!(many.0, StatusCode::OK, "{}", many.1);
    assert_eq!(many.1["data"]["result"].as_array().unwrap().len(), 3);
    assert_eq!(
        many.1["data"]["result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|sample| sample["metric"]["__name__"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["set_lhs", "set_lhs", "set_lhs_alt"]
    );

    let range = prom_query_range(&app, "set_lhs or set_rhs", base, base + 10, 10).await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    let rhs_d = range.1["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|series| series["metric"]["host"] == "d")
        .unwrap();
    assert_eq!(rhs_d["metric"]["__name__"], "set_rhs");
    assert_eq!(rhs_d["values"], serde_json::json!([[base + 10, "60"]]));

    let invalid = prom_query(&app, "1 and set_lhs", base).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 3,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query_range(
        &limited,
        "set_lhs{host=\"a\"} and set_rhs{host=\"a\"}",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum intermediate-work limit of 3 points"));

    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = prom_query(&reopened_app, "set_lhs or set_rhs", base + 10).await;
    assert_eq!(recovered.1, or.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_four_promql_on_ignoring_match_labels_names_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_four_on_ignoring.db");
    let base = 1_700_440_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let mut victoria = String::new();
    for (metric, zone, first, second) in [
        ("match_lhs", "east", 8.0, 18.0),
        ("match_rhs", "west", 2.0, 12.0),
        ("match_rhs_duplicate", "north", 3.0, 13.0),
    ] {
        use std::fmt::Write;
        writeln!(
            victoria,
            "{{\"metric\":{{\"__name__\":\"{metric}\",\"host\":\"a\",\"shared\":\"x\",\"zone\":\"{zone}\"}},\"values\":[{first},{second}],\"timestamps\":[{},{}]}}",
            base * 1_000,
            (base + 10) * 1_000,
        )
        .unwrap();
    }
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let on = prom_query(&app, "match_lhs + on(host) match_rhs", base + 10).await;
    assert_eq!(on.0, StatusCode::OK, "{}", on.1);
    assert_eq!(
        on.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "a"},
            "value": [base + 10, "30"]
        }])
    );

    let ignoring = prom_query(&app, "match_lhs + ignoring(zone) match_rhs", base + 10).await;
    assert_eq!(ignoring.0, StatusCode::OK, "{}", ignoring.1);
    assert_eq!(
        ignoring.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "a", "shared": "x"},
            "value": [base + 10, "30"]
        }])
    );

    let comparison = prom_query(&app, "match_lhs == on(host) match_lhs", base + 10).await;
    assert_eq!(comparison.0, StatusCode::OK, "{}", comparison.1);
    assert_eq!(
        comparison.1["data"]["result"][0]["metric"],
        serde_json::json!({"host": "a"})
    );

    let set = prom_query(&app, "match_lhs and on(host) match_rhs", base + 10).await;
    assert_eq!(set.0, StatusCode::OK, "{}", set.1);
    assert_eq!(
        set.1["data"]["result"][0]["metric"],
        serde_json::json!({
            "__name__": "match_lhs", "host": "a", "shared": "x", "zone": "east"
        })
    );

    for query in [
        "match_lhs + on(missing) match_rhs",
        "match_lhs + on() match_rhs",
    ] {
        let empty_key = prom_query(&app, query, base + 10).await;
        assert_eq!(empty_key.0, StatusCode::OK, "{}", empty_key.1);
        assert_eq!(
            empty_key.1["data"]["result"][0]["metric"],
            serde_json::json!({})
        );
        assert_eq!(empty_key.1["data"]["result"][0]["value"][1], "30");
    }

    let duplicate = prom_query(
        &app,
        "match_lhs + on(host) {__name__=~\"match_rhs(_duplicate)?\"}",
        base + 10,
    )
    .await;
    assert_eq!(duplicate.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(duplicate.1["errorType"], "execution");
    assert!(duplicate.1["error"]
        .as_str()
        .unwrap()
        .contains("many-to-many matching not allowed"));

    let range = prom_query_range(
        &app,
        "match_lhs + ignoring(zone) match_rhs",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0],
        serde_json::json!({
            "metric": {"host": "a", "shared": "x"},
            "values": [[base, "10"], [base + 10, "30"]]
        })
    );

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 3,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query_range(
        &limited,
        "match_lhs + on(host) match_rhs",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(rejected.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum intermediate-work limit of 3 points"));

    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = prom_query(
        &reopened_app,
        "match_lhs + ignoring(zone) match_rhs",
        base + 10,
    )
    .await;
    assert_eq!(recovered.1, ignoring.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_four_promql_group_matching_direction_labels_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_four_group_matching.db");
    let base = 1_700_450_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let mut victoria = String::new();
    for (metric, labels, first, second) in [
        (
            "group_many_lhs",
            "\"host\":\"a\",\"owner\":\"old\",\"pod\":\"p1\",\"zone\":\"east\"",
            8.0,
            18.0,
        ),
        (
            "group_many_lhs",
            "\"host\":\"a\",\"owner\":\"old\",\"pod\":\"p2\",\"zone\":\"west\"",
            9.0,
            19.0,
        ),
        (
            "group_collision_many",
            "\"host\":\"a\",\"team\":\"red\"",
            4.0,
            14.0,
        ),
        (
            "group_collision_many",
            "\"host\":\"a\",\"team\":\"blue\"",
            5.0,
            15.0,
        ),
        (
            "group_one_rhs",
            "\"host\":\"a\",\"team\":\"core\"",
            2.0,
            12.0,
        ),
        (
            "group_one_rhs_duplicate",
            "\"host\":\"a\",\"team\":\"ops\"",
            3.0,
            13.0,
        ),
        (
            "group_one_lhs",
            "\"host\":\"a\",\"team\":\"core\"",
            8.0,
            18.0,
        ),
        (
            "group_one_lhs_duplicate",
            "\"host\":\"a\",\"team\":\"ops\"",
            9.0,
            19.0,
        ),
        (
            "group_many_rhs",
            "\"host\":\"a\",\"pod\":\"p1\",\"zone\":\"east\"",
            2.0,
            12.0,
        ),
        (
            "group_many_rhs",
            "\"host\":\"a\",\"pod\":\"p2\",\"zone\":\"west\"",
            3.0,
            13.0,
        ),
    ] {
        use std::fmt::Write;
        writeln!(
            victoria,
            "{{\"metric\":{{\"__name__\":\"{metric}\",{labels}}},\"values\":[{first},{second}],\"timestamps\":[{},{}]}}",
            base * 1_000,
            (base + 10) * 1_000,
        )
        .unwrap();
    }
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let group_left = prom_query(
        &app,
        "group_many_lhs + on(host) group_left(team) group_one_rhs",
        base + 10,
    )
    .await;
    assert_eq!(group_left.0, StatusCode::OK, "{}", group_left.1);
    assert_eq!(
        group_left.1["data"]["result"],
        serde_json::json!([
            {"metric": {"host": "a", "owner": "old", "pod": "p1", "team": "core", "zone": "east"}, "value": [base + 10, "30"]},
            {"metric": {"host": "a", "owner": "old", "pod": "p2", "team": "core", "zone": "west"}, "value": [base + 10, "31"]}
        ])
    );

    let missing_include = prom_query(
        &app,
        "group_many_lhs + on(host) group_left(owner) group_one_rhs",
        base + 10,
    )
    .await;
    assert_eq!(missing_include.0, StatusCode::OK, "{}", missing_include.1);
    for sample in missing_include.1["data"]["result"].as_array().unwrap() {
        assert!(sample["metric"].get("owner").is_none(), "{sample}");
    }

    let group_right = prom_query(
        &app,
        "group_one_lhs - on(host) group_right(team) group_many_rhs",
        base + 10,
    )
    .await;
    assert_eq!(group_right.0, StatusCode::OK, "{}", group_right.1);
    assert_eq!(
        group_right.1["data"]["result"],
        serde_json::json!([
            {"metric": {"host": "a", "pod": "p1", "team": "core", "zone": "east"}, "value": [base + 10, "6"]},
            {"metric": {"host": "a", "pod": "p2", "team": "core", "zone": "west"}, "value": [base + 10, "5"]}
        ])
    );

    let comparison = prom_query(
        &app,
        "group_one_lhs > on(host) group_right(team) group_many_rhs",
        base + 10,
    )
    .await;
    assert_eq!(comparison.0, StatusCode::OK, "{}", comparison.1);
    for sample in comparison.1["data"]["result"].as_array().unwrap() {
        assert_eq!(sample["metric"]["__name__"], "group_many_rhs");
        assert_eq!(sample["metric"]["team"], "core");
        assert_eq!(sample["value"][1], "18");
    }

    for query in [
        "group_many_lhs + on(host) group_left(team) {__name__=~\"group_one_rhs(_duplicate)?\"}",
        "{__name__=~\"group_one_lhs(_duplicate)?\"} - on(host) group_right(team) group_many_rhs",
    ] {
        let duplicate = prom_query(&app, query, base + 10).await;
        assert_eq!(duplicate.0, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(duplicate.1["errorType"], "execution");
        assert!(duplicate.1["error"]
            .as_str()
            .unwrap()
            .contains("many-to-many matching not allowed"));
    }

    let collision = prom_query(
        &app,
        "group_collision_many + on(host) group_left(team) group_one_rhs",
        base + 10,
    )
    .await;
    assert_eq!(collision.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(collision.1["errorType"], "execution");
    assert!(collision.1["error"]
        .as_str()
        .unwrap()
        .contains("grouping labels must ensure unique matches"));

    let range = prom_query_range(
        &app,
        "group_many_lhs + on(host) group_left(team) group_one_rhs",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "10"], [base + 10, "30"]])
    );

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 5,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query_range(
        &limited,
        "group_many_lhs + on(host) group_left(team) group_one_rhs",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(rejected.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum intermediate-work limit of 5 points"));

    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = prom_query(
        &reopened_app,
        "group_one_lhs - on(host) group_right(team) group_many_rhs",
        base + 10,
    )
    .await;
    assert_eq!(recovered.1, group_right.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_five_promql_sum_groups_labels_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_five_sum.db");
    let base = 1_700_500_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let victoria = [
        ("a", "east", 1.0, 3.0),
        ("b", "east", 2.0, 4.0),
        ("c", "west", 5.0, 6.0),
    ]
    .into_iter()
    .map(|(host, region, first, second)| {
        format!(
            "{{\"metric\":{{\"__name__\":\"aggregate_metric\",\"host\":\"{host}\",\"region\":\"{region}\"}},\"values\":[{first},{second}],\"timestamps\":[{},{}]}}\n",
            base * 1_000,
            (base + 10) * 1_000,
        )
    })
    .collect::<String>();
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    let ieee = format!(
        concat!(
            "aggregate_ieee{{case=\"nan\",host=\"a\"}} NaN {}\n",
            "aggregate_ieee{{case=\"nan\",host=\"b\"}} 1 {}\n",
            "aggregate_ieee{{case=\"positive\",host=\"a\"}} +Inf {}\n",
            "aggregate_ieee{{case=\"positive\",host=\"b\"}} 1 {}\n",
            "aggregate_ieee{{case=\"mixed\",host=\"a\"}} +Inf {}\n",
            "aggregate_ieee{{case=\"mixed\",host=\"b\"}} -Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", ieee.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let by = prom_query(&app, "sum by (region) (aggregate_metric)", base + 10).await;
    assert_eq!(by.0, StatusCode::OK, "{}", by.1);
    assert_eq!(
        by.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "value": [base + 10, "7"]},
            {"metric": {"region": "west"}, "value": [base + 10, "6"]}
        ])
    );

    let without = prom_query(
        &app,
        "sum without (__name__, host) (aggregate_metric)",
        base + 10,
    )
    .await;
    assert_eq!(without.0, StatusCode::OK, "{}", without.1);
    assert_eq!(without.1["data"]["result"], by.1["data"]["result"]);

    let empty = prom_query(&app, "sum(aggregate_metric)", base + 10).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(
        empty.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base + 10, "13"]}])
    );

    let missing = prom_query(&app, "sum by (missing) (aggregate_metric)", base + 10).await;
    assert_eq!(missing.0, StatusCode::OK, "{}", missing.1);
    assert_eq!(missing.1["data"]["result"], empty.1["data"]["result"]);

    let named = prom_query(&app, "sum by (__name__) (aggregate_metric)", base + 10).await;
    assert_eq!(named.0, StatusCode::OK, "{}", named.1);
    assert_eq!(
        named.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "aggregate_metric"},
            "value": [base + 10, "13"]
        }])
    );

    let ieee = prom_query(&app, "sum by (case) (aggregate_ieee)", base + 10).await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "mixed"}, "value": [base + 10, "NaN"]},
            {"metric": {"case": "nan"}, "value": [base + 10, "NaN"]},
            {"metric": {"case": "positive"}, "value": [base + 10, "+Inf"]}
        ])
    );

    let range = prom_query_range(
        &app,
        "sum by (region) (aggregate_metric)",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "values": [[base, "3"], [base + 10, "7"]]},
            {"metric": {"region": "west"}, "values": [[base, "5"], [base + 10, "6"]]}
        ])
    );

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 2,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "sum(aggregate_metric)", base + 10).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("raw batch work point limit 2 exceeded"),
        "{}",
        rejected.1
    );

    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let recovered = prom_query(
        &reopened_app,
        "sum by (region) (aggregate_metric)",
        base + 10,
    )
    .await;
    assert_eq!(recovered.1, by.1);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_five_promql_avg_is_compensated_grouped_and_reopenable() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_five_avg.db");
    let base = 1_700_510_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let victoria = [
        ("a", "east", 1.0, 3.0),
        ("b", "east", 2.0, 4.0),
        ("c", "west", 5.0, 6.0),
    ]
    .into_iter()
    .map(|(host, region, first, second)| {
        format!(
            "{{\"metric\":{{\"__name__\":\"aggregate_avg\",\"host\":\"{host}\",\"region\":\"{region}\"}},\"values\":[{first},{second}],\"timestamps\":[{},{}]}}\n",
            base * 1_000,
            (base + 10) * 1_000,
        )
    })
    .collect::<String>();
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    let edge = format!(
        concat!(
            "aggregate_avg_precision{{host=\"a\"}} 1e16 {}\n",
            "aggregate_avg_precision{{host=\"b\"}} 1 {}\n",
            "aggregate_avg_precision{{host=\"c\"}} -1e16 {}\n",
            "aggregate_avg_overflow{{host=\"a\"}} 1.7976931348623157e308 {}\n",
            "aggregate_avg_overflow{{host=\"b\"}} 1.7976931348623157e308 {}\n",
            "aggregate_avg_ieee{{case=\"nan\",host=\"a\"}} NaN {}\n",
            "aggregate_avg_ieee{{case=\"nan\",host=\"b\"}} 1 {}\n",
            "aggregate_avg_ieee{{case=\"positive\",host=\"a\"}} +Inf {}\n",
            "aggregate_avg_ieee{{case=\"positive\",host=\"b\"}} 1 {}\n",
            "aggregate_avg_ieee{{case=\"mixed\",host=\"a\"}} +Inf {}\n",
            "aggregate_avg_ieee{{case=\"mixed\",host=\"b\"}} -Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", edge.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let grouped = prom_query(&app, "avg by (region) (aggregate_avg)", base + 10).await;
    assert_eq!(grouped.0, StatusCode::OK, "{}", grouped.1);
    assert_eq!(
        grouped.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "value": [base + 10, "3.5"]},
            {"metric": {"region": "west"}, "value": [base + 10, "6"]}
        ])
    );
    let all = prom_query(&app, "avg(aggregate_avg)", base + 10).await;
    assert_eq!(all.0, StatusCode::OK, "{}", all.1);
    assert_eq!(all.1["data"]["result"][0]["value"][1], "4.333333333333333");

    let precision = prom_query(&app, "avg(aggregate_avg_precision)", base + 10).await;
    assert_eq!(precision.0, StatusCode::OK, "{}", precision.1);
    assert_eq!(
        precision.1["data"]["result"][0]["value"][1],
        "0.3333333333333333"
    );
    let overflow = prom_query(&app, "avg(aggregate_avg_overflow)", base + 10).await;
    assert_eq!(overflow.0, StatusCode::OK, "{}", overflow.1);
    assert_eq!(
        overflow.1["data"]["result"][0]["value"][1]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap()
            .to_bits(),
        f64::MAX.to_bits()
    );
    let ieee = prom_query(&app, "avg by (case) (aggregate_avg_ieee)", base + 10).await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "mixed"}, "value": [base + 10, "NaN"]},
            {"metric": {"case": "nan"}, "value": [base + 10, "NaN"]},
            {"metric": {"case": "positive"}, "value": [base + 10, "+Inf"]}
        ])
    );
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(&reopened_app, "avg by (region) (aggregate_avg)", base + 10)
            .await
            .1,
        grouped.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_five_promql_min_max_group_ieee_range_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_five_min_max.db");
    let base = 1_700_520_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let victoria = [
        ("a", "east", 1.0, 3.0),
        ("b", "east", 2.0, 4.0),
        ("c", "west", 5.0, 6.0),
    ]
    .into_iter()
    .map(|(host, region, first, second)| {
        format!(
            "{{\"metric\":{{\"__name__\":\"aggregate_min_max\",\"host\":\"{host}\",\"region\":\"{region}\"}},\"values\":[{first},{second}],\"timestamps\":[{},{}]}}\n",
            base * 1_000,
            (base + 10) * 1_000,
        )
    })
    .collect::<String>();
    assert_no_content(post_body(&app, "/api/v1/import", victoria.as_bytes()).await);
    let ieee = format!(
        concat!(
            "aggregate_min_max_ieee{{case=\"all_nan\",host=\"a\"}} NaN {}\n",
            "aggregate_min_max_ieee{{case=\"all_nan\",host=\"b\"}} NaN {}\n",
            "aggregate_min_max_ieee{{case=\"mixed\",host=\"a\"}} NaN {}\n",
            "aggregate_min_max_ieee{{case=\"mixed\",host=\"b\"}} 2 {}\n",
            "aggregate_min_max_ieee{{case=\"mixed\",host=\"c\"}} 4 {}\n",
            "aggregate_min_max_ieee{{case=\"infinite\",host=\"a\"}} -Inf {}\n",
            "aggregate_min_max_ieee{{case=\"infinite\",host=\"b\"}} +Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", ieee.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let min = prom_query(&app, "min by (region) (aggregate_min_max)", base + 10).await;
    assert_eq!(min.0, StatusCode::OK, "{}", min.1);
    assert_eq!(
        min.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "value": [base + 10, "3"]},
            {"metric": {"region": "west"}, "value": [base + 10, "6"]}
        ])
    );
    let max = prom_query(&app, "max by (region) (aggregate_min_max)", base + 10).await;
    assert_eq!(max.0, StatusCode::OK, "{}", max.1);
    assert_eq!(
        max.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "value": [base + 10, "4"]},
            {"metric": {"region": "west"}, "value": [base + 10, "6"]}
        ])
    );
    let min_ieee = prom_query(&app, "min by (case) (aggregate_min_max_ieee)", base + 10).await;
    assert_eq!(min_ieee.0, StatusCode::OK, "{}", min_ieee.1);
    assert_eq!(
        min_ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "all_nan"}, "value": [base + 10, "NaN"]},
            {"metric": {"case": "infinite"}, "value": [base + 10, "-Inf"]},
            {"metric": {"case": "mixed"}, "value": [base + 10, "2"]}
        ])
    );
    let max_ieee = prom_query(&app, "max by (case) (aggregate_min_max_ieee)", base + 10).await;
    assert_eq!(max_ieee.0, StatusCode::OK, "{}", max_ieee.1);
    assert_eq!(
        max_ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "all_nan"}, "value": [base + 10, "NaN"]},
            {"metric": {"case": "infinite"}, "value": [base + 10, "+Inf"]},
            {"metric": {"case": "mixed"}, "value": [base + 10, "4"]}
        ])
    );
    let range = prom_query_range(
        &app,
        "min by (region) (aggregate_min_max)",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "1"], [base + 10, "3"]])
    );

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "max by (region) (aggregate_min_max)",
            base + 10,
        )
        .await
        .1,
        max.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_five_promql_count_group_include_all_values_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_five_count_group.db");
    let base = 1_700_530_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let values = format!(
        concat!(
            "aggregate_count_group{{host=\"a\",region=\"east\"}} NaN {}\n",
            "aggregate_count_group{{host=\"b\",region=\"east\"}} +Inf {}\n",
            "aggregate_count_group{{host=\"c\",region=\"west\"}} -Inf {}\n",
            "aggregate_count_group{{host=\"a\",region=\"east\"}} 1 {}\n",
            "aggregate_count_group{{host=\"b\",region=\"east\"}} 2 {}\n",
            "aggregate_count_group{{host=\"c\",region=\"west\"}} 3 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", values.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let count = prom_query(&app, "count by (region) (aggregate_count_group)", base).await;
    assert_eq!(count.0, StatusCode::OK, "{}", count.1);
    assert_eq!(
        count.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "value": [base, "2"]},
            {"metric": {"region": "west"}, "value": [base, "1"]}
        ])
    );
    let group = prom_query(
        &app,
        "group without (__name__, host) (aggregate_count_group)",
        base,
    )
    .await;
    assert_eq!(group.0, StatusCode::OK, "{}", group.1);
    assert_eq!(
        group.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "value": [base, "1"]},
            {"metric": {"region": "west"}, "value": [base, "1"]}
        ])
    );
    let empty = prom_query(&app, "count(aggregate_count_group)", base).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(
        empty.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base, "3"]}])
    );
    let range = prom_query_range(
        &app,
        "count by (region) (aggregate_count_group)",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "2"], [base + 10, "2"]])
    );

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "group without (__name__, host) (aggregate_count_group)",
            base,
        )
        .await
        .1,
        group.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_five_promql_stddev_stdvar_are_population_grouped_and_reopenable() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_five_stddev_stdvar.db");
    let base = 1_700_540_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let values = format!(
        concat!(
            "aggregate_dispersion{{host=\"a\",region=\"east\"}} 3 {}\n",
            "aggregate_dispersion{{host=\"b\",region=\"east\"}} 4 {}\n",
            "aggregate_dispersion{{host=\"c\",region=\"west\"}} 7 {}\n",
            "aggregate_dispersion{{host=\"n1\",region=\"nan\"}} NaN {}\n",
            "aggregate_dispersion{{host=\"n2\",region=\"nan\"}} 1 {}\n",
            "aggregate_dispersion{{host=\"i1\",region=\"inf\"}} +Inf {}\n",
            "aggregate_dispersion{{host=\"i2\",region=\"inf\"}} +Inf {}\n",
            "aggregate_dispersion{{host=\"a\",region=\"east\"}} 5 {}\n",
            "aggregate_dispersion{{host=\"b\",region=\"east\"}} 9 {}\n",
            "aggregate_dispersion{{host=\"c\",region=\"west\"}} 8 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", values.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let variance = prom_query(&app, "stdvar by (region) (aggregate_dispersion)", base).await;
    assert_eq!(variance.0, StatusCode::OK, "{}", variance.1);
    assert_eq!(
        variance.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "value": [base, "0.25"]},
            {"metric": {"region": "inf"}, "value": [base, "NaN"]},
            {"metric": {"region": "nan"}, "value": [base, "NaN"]},
            {"metric": {"region": "west"}, "value": [base, "0"]}
        ])
    );
    let deviation = prom_query(
        &app,
        "stddev without (__name__, host) (aggregate_dispersion)",
        base,
    )
    .await;
    assert_eq!(deviation.0, StatusCode::OK, "{}", deviation.1);
    assert_eq!(
        deviation.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "value": [base, "0.5"]},
            {"metric": {"region": "inf"}, "value": [base, "NaN"]},
            {"metric": {"region": "nan"}, "value": [base, "NaN"]},
            {"metric": {"region": "west"}, "value": [base, "0"]}
        ])
    );
    let range = prom_query_range(
        &app,
        "stdvar by (region) (aggregate_dispersion{region=~\"east|west\"})",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "values": [[base, "0.25"], [base + 10, "4"]]},
            {"metric": {"region": "west"}, "values": [[base, "0"], [base + 10, "0"]]}
        ])
    );

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "stddev without (__name__, host) (aggregate_dispersion)",
            base,
        )
        .await
        .1,
        deviation.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_five_promql_topk_bottomk_rank_per_step_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_five_topk_bottomk.db");
    let base = 1_700_550_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let values = format!(
        concat!(
            "aggregate_rank{{host=\"a\",region=\"east\"}} 10 {}\n",
            "aggregate_rank{{host=\"b\",region=\"east\"}} 9 {}\n",
            "aggregate_rank{{host=\"c\",region=\"east\"}} NaN {}\n",
            "aggregate_rank{{host=\"d\",region=\"west\"}} 8 {}\n",
            "aggregate_rank{{host=\"e\",region=\"west\"}} 7 {}\n",
            "aggregate_rank{{host=\"a\",region=\"east\"}} 1 {}\n",
            "aggregate_rank{{host=\"b\",region=\"east\"}} 20 {}\n",
            "aggregate_rank{{host=\"d\",region=\"west\"}} 30 {}\n",
            "aggregate_rank{{host=\"e\",region=\"west\"}} 2 {}\n",
            "aggregate_rank_limit{{host=\"a\"}} 1 {}\n",
            "aggregate_rank_limit{{host=\"b\"}} 2 {}\n",
            "aggregate_rank_limit{{host=\"c\"}} 3 {}\n",
            "aggregate_rank_limit{{host=\"d\"}} 4 {}\n",
            "aggregate_rank_limit{{host=\"e\"}} 5 {}\n",
            "aggregate_rank_tie{{host=\"a\"}} 5 {}\n",
            "aggregate_rank_tie{{host=\"b\"}} 5 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", values.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let top = prom_query(&app, "topk by (region) (1 + 1, aggregate_rank)", base).await;
    assert_eq!(top.0, StatusCode::OK, "{}", top.1);
    assert_eq!(
        top.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "aggregate_rank", "host": "a", "region": "east"}, "value": [base, "10"]},
            {"metric": {"__name__": "aggregate_rank", "host": "b", "region": "east"}, "value": [base, "9"]},
            {"metric": {"__name__": "aggregate_rank", "host": "d", "region": "west"}, "value": [base, "8"]},
            {"metric": {"__name__": "aggregate_rank", "host": "e", "region": "west"}, "value": [base, "7"]}
        ])
    );
    let bottom = prom_query(&app, "bottomk by (region) (1, aggregate_rank)", base).await;
    assert_eq!(bottom.0, StatusCode::OK, "{}", bottom.1);
    assert_eq!(
        bottom.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "aggregate_rank", "host": "b", "region": "east"}, "value": [base, "9"]},
            {"metric": {"__name__": "aggregate_rank", "host": "e", "region": "west"}, "value": [base, "7"]}
        ])
    );
    assert_eq!(
        prom_query(&app, "topk(0, aggregate_rank)", base).await.1["data"]["result"],
        serde_json::json!([])
    );
    let nan = prom_query(&app, "topk(NaN, aggregate_rank)", base).await;
    assert_eq!(nan.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", nan.1);
    assert!(nan.1["error"].as_str().unwrap().contains("NaN"));
    let overflow = prom_query(&app, "topk(+Inf, aggregate_rank)", base).await;
    assert_eq!(
        overflow.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        overflow.1
    );
    assert!(overflow.1["error"]
        .as_str()
        .unwrap()
        .contains("Scalar value +Inf overflows int64"));
    let with_nan = prom_query(&app, "topk by (region) (3, aggregate_rank)", base).await;
    assert_eq!(with_nan.0, StatusCode::OK, "{}", with_nan.1);
    assert_eq!(
        with_nan.1["data"]["result"][2]["value"][1],
        serde_json::json!("NaN")
    );
    for operation in ["topk", "bottomk"] {
        let tied = prom_query(&app, &format!("{operation}(1, aggregate_rank_tie)"), base).await;
        assert_eq!(tied.0, StatusCode::OK, "{}: {}", operation, tied.1);
        assert_eq!(tied.1["data"]["result"].as_array().unwrap().len(), 1);
        assert_eq!(tied.1["data"]["result"][0]["metric"]["host"], "a");
        assert_eq!(tied.1["data"]["result"][0]["value"][1], "5");
    }

    let range = prom_query_range(
        &app,
        "topk by (region) (1, aggregate_rank)",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "aggregate_rank", "host": "a", "region": "east"}, "values": [[base, "10"]]},
            {"metric": {"__name__": "aggregate_rank", "host": "b", "region": "east"}, "values": [[base + 10, "20"]]},
            {"metric": {"__name__": "aggregate_rank", "host": "d", "region": "west"}, "values": [[base, "8"], [base + 10, "30"]]}
        ])
    );
    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 5,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "topk(1, aggregate_rank_limit)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("maximum intermediate-work limit of 5 points"),
        "{}",
        rejected.1
    );

    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "bottomk by (region) (1, aggregate_rank)",
            base
        )
        .await
        .1,
        bottom.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_five_promql_quantile_interpolates_per_step_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_five_quantile.db");
    let base = 1_700_560_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let values = format!(
        concat!(
            "aggregate_quantile{{host=\"a\",region=\"east\"}} 1 {}\n",
            "aggregate_quantile{{host=\"b\",region=\"east\"}} 2 {}\n",
            "aggregate_quantile{{host=\"c\",region=\"east\"}} 3 {}\n",
            "aggregate_quantile{{host=\"d\",region=\"east\"}} 4 {}\n",
            "aggregate_quantile{{host=\"w\",region=\"west\"}} 7 {}\n",
            "aggregate_quantile{{host=\"n1\",region=\"nan\"}} NaN {}\n",
            "aggregate_quantile{{host=\"n2\",region=\"nan\"}} 1 {}\n",
            "aggregate_quantile{{host=\"n3\",region=\"nan\"}} 3 {}\n",
            "aggregate_quantile{{host=\"a\",region=\"east\"}} 2 {}\n",
            "aggregate_quantile{{host=\"b\",region=\"east\"}} 4 {}\n",
            "aggregate_quantile{{host=\"c\",region=\"east\"}} 6 {}\n",
            "aggregate_quantile{{host=\"d\",region=\"east\"}} 8 {}\n",
            "aggregate_quantile_limit{{host=\"a\"}} 1 {}\n",
            "aggregate_quantile_limit{{host=\"b\"}} 2 {}\n",
            "aggregate_quantile_limit{{host=\"c\"}} 3 {}\n",
            "aggregate_quantile_limit{{host=\"d\"}} 4 {}\n",
            "aggregate_quantile_limit{{host=\"e\"}} 5 {}\n",
            "aggregate_quantile_zero{{host=\"a\"}} 0 {}\n",
            "aggregate_quantile_zero{{host=\"b\"}} -0 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", values.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let median = prom_query(
        &app,
        "quantile by (region) (1 / 2, aggregate_quantile{region!=\"nan\"})",
        base,
    )
    .await;
    assert_eq!(median.0, StatusCode::OK, "{}", median.1);
    assert_eq!(
        median.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "value": [base, "2.5"]},
            {"metric": {"region": "west"}, "value": [base, "7"]}
        ])
    );
    let ieee = prom_query(
        &app,
        "quantile by (region) (0.75, aggregate_quantile{region=\"nan\"})",
        base,
    )
    .await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(ieee.1["data"]["result"][0]["value"][1], "2");
    for (parameter, expected) in [("NaN", "NaN"), ("-1", "-Inf"), ("2", "+Inf")] {
        let response = prom_query(
            &app,
            &format!("quantile({parameter}, aggregate_quantile{{region=\"east\"}})"),
            base,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{}: {}", parameter, response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
        assert_eq!(
            response.1["warnings"],
            serde_json::json!([format!(
                "PromQL warning: quantile value should be between 0 and 1, got {parameter} (1:10)"
            )])
        );
    }
    for (parameter, expected) in [("0", "0"), ("1", "-0")] {
        let response = prom_query(
            &app,
            &format!("quantile({parameter}, aggregate_quantile_zero)"),
            base,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }
    let range = prom_query_range(
        &app,
        "quantile by (region) (0.5, aggregate_quantile{region=\"east\"})",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east"}, "values": [[base, "2.5"], [base + 10, "5"]]}
        ])
    );
    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 5,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "quantile(0.5, aggregate_quantile_limit)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum intermediate-work limit of 5 points"));

    drop(limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "quantile by (region) (1 / 2, aggregate_quantile{region!=\"nan\"})",
            base,
        )
        .await
        .1,
        median.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_five_promql_count_values_formats_groups_ranges_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_five_count_values.db");
    let base = 1_700_570_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let values = format!(
        concat!(
            "aggregate_values{{host=\"a\",region=\"east\",value=\"old\"}} 1 {}\n",
            "aggregate_values{{host=\"b\",region=\"east\",value=\"old\"}} 1 {}\n",
            "aggregate_values{{host=\"c\",region=\"east\",value=\"old\"}} 2 {}\n",
            "aggregate_values{{host=\"d\",region=\"west\",value=\"old\"}} -0 {}\n",
            "aggregate_values{{host=\"e\",region=\"west\",value=\"old\"}} +Inf {}\n",
            "aggregate_values{{host=\"f\",region=\"west\",value=\"old\"}} NaN {}\n",
            "aggregate_values{{host=\"g\",region=\"west\",value=\"old\"}} 1e-20 {}\n",
            "aggregate_values{{host=\"h\",region=\"west\",value=\"old\"}} 1e20 {}\n",
            "aggregate_values{{host=\"a\",region=\"east\",value=\"old\"}} 2 {}\n",
            "aggregate_values{{host=\"b\",region=\"east\",value=\"old\"}} 1 {}\n",
            "aggregate_values{{host=\"c\",region=\"east\",value=\"old\"}} 2 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", values.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let grouped = prom_query(
        &app,
        "count_values by (region) (\"value\", aggregate_values)",
        base,
    )
    .await;
    assert_eq!(grouped.0, StatusCode::OK, "{}", grouped.1);
    assert_eq!(
        grouped.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east", "value": "1"}, "value": [base, "2"]},
            {"metric": {"region": "east", "value": "2"}, "value": [base, "1"]},
            {"metric": {"region": "west", "value": "+Inf"}, "value": [base, "1"]},
            {"metric": {"region": "west", "value": "-0"}, "value": [base, "1"]},
            {"metric": {"region": "west", "value": "0.00000000000000000001"}, "value": [base, "1"]},
            {"metric": {"region": "west", "value": "100000000000000000000"}, "value": [base, "1"]},
            {"metric": {"region": "west", "value": "NaN"}, "value": [base, "1"]}
        ])
    );
    let result_limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_result_points: 4,
            ..PromQueryLimits::default()
        },
    );
    let too_many = prom_query(
        &result_limited,
        "count_values by (region) (\"value\", aggregate_values)",
        base,
    )
    .await;
    assert_eq!(
        too_many.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        too_many.1
    );
    assert!(too_many.1["error"]
        .as_str()
        .unwrap()
        .contains("maximum result-point limit of 4"));
    let without = prom_query(
        &app,
        "count_values without (__name__, host) (\"sample\", aggregate_values{region=\"east\"})",
        base,
    )
    .await;
    assert_eq!(without.0, StatusCode::OK, "{}", without.1);
    assert_eq!(
        without.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east", "sample": "1", "value": "old"}, "value": [base, "2"]},
            {"metric": {"region": "east", "sample": "2", "value": "old"}, "value": [base, "1"]}
        ])
    );
    let invalid = prom_query(&app, "count_values(\"\", aggregate_values)", base).await;
    assert_eq!(invalid.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", invalid.1);
    assert!(invalid.1["error"]
        .as_str()
        .unwrap()
        .contains("invalid label name"));
    let utf8 = prom_query(
        &app,
        "count_values(\"value label\", aggregate_values{host=\"a\"})",
        base,
    )
    .await;
    assert_eq!(utf8.0, StatusCode::OK, "{}", utf8.1);
    assert_eq!(utf8.1["data"]["result"][0]["metric"]["value label"], "1");
    for query in [
        "sum(aggregate_values{host=\"missing\"})",
        "avg(aggregate_values{host=\"missing\"})",
        "min(aggregate_values{host=\"missing\"})",
        "max(aggregate_values{host=\"missing\"})",
        "count(aggregate_values{host=\"missing\"})",
        "group(aggregate_values{host=\"missing\"})",
        "stddev(aggregate_values{host=\"missing\"})",
        "stdvar(aggregate_values{host=\"missing\"})",
        "topk(1, aggregate_values{host=\"missing\"})",
        "bottomk(1, aggregate_values{host=\"missing\"})",
        "quantile(0.5, aggregate_values{host=\"missing\"})",
        "count_values(\"value\", aggregate_values{host=\"missing\"})",
    ] {
        let empty = prom_query(&app, query, base).await;
        assert_eq!(empty.0, StatusCode::OK, "{}: {}", query, empty.1);
        assert_eq!(empty.1["data"]["result"], serde_json::json!([]));
    }
    for query in [
        "topk(\"wrong\", aggregate_values)",
        "count_values(1, aggregate_values)",
    ] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(
            invalid.0,
            StatusCode::BAD_REQUEST,
            "{}: {}",
            query,
            invalid.1
        );
    }

    let range = prom_query_range(
        &app,
        "count_values by (region) (\"value\", aggregate_values{region=\"east\"})",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([
            {"metric": {"region": "east", "value": "1"}, "values": [[base, "2"], [base + 10, "1"]]},
            {"metric": {"region": "east", "value": "2"}, "values": [[base, "1"], [base + 10, "2"]]}
        ])
    );

    drop(result_limited);
    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "count_values by (region) (\"value\", aggregate_values)",
            base,
        )
        .await
        .1,
        grouped.1
    );
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
            "/prometheus/api/v1/query?query=rate%28up%29",
            "invalid parameter \"query\": parse error: expected type matrix in call to function 'rate', got vector",
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
        "/prometheus/api/v1/query_range?query=avg_over_time%28cancel_metric%5B5m%3A1s%5D%29&start={base}&end={}&step=1",
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

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_avg_over_time_is_compensated_ieee_bounded_and_reopenable() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_six_avg_over_time.db");
    let base = 1_700_600_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_avg_precision 1e16 {}\n",
            "range_avg_precision 1 {}\n",
            "range_avg_precision -1e16 {}\n",
            "range_avg_overflow 1.7976931348623157e308 {}\n",
            "range_avg_overflow 1.7976931348623157e308 {}\n",
            "range_avg_ieee{{case=\"nan\"}} NaN {}\n",
            "range_avg_ieee{{case=\"nan\"}} 1 {}\n",
            "range_avg_ieee{{case=\"positive\"}} +Inf {}\n",
            "range_avg_ieee{{case=\"positive\"}} 1 {}\n",
            "range_avg_ieee{{case=\"mixed\"}} +Inf {}\n",
            "range_avg_ieee{{case=\"mixed\"}} -Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let direct = prom_query(&app, "avg_over_time(range_avg_precision[30s])", base + 30).await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "value": [base + 30, "0.3333333333333333"]
        }])
    );

    let subquery = prom_query(
        &app,
        "avg_over_time(range_avg_precision[30s:10s])",
        base + 30,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1, direct.1);

    let overflow = prom_query(&app, "avg_over_time(range_avg_overflow[20s])", base + 30).await;
    assert_eq!(overflow.0, StatusCode::OK, "{}", overflow.1);
    assert_eq!(
        overflow.1["data"]["result"][0]["value"][1]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap()
            .to_bits(),
        f64::MAX.to_bits()
    );

    let ieee = prom_query(&app, "avg_over_time(range_avg_ieee[20s])", base + 30).await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "mixed"}, "value": [base + 30, "NaN"]},
            {"metric": {"case": "nan"}, "value": [base + 30, "NaN"]},
            {"metric": {"case": "positive"}, "value": [base + 30, "+Inf"]}
        ])
    );

    let empty = prom_query(
        &app,
        "avg_over_time(range_avg_precision[30s])",
        base + 1_000,
    )
    .await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 2,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "avg_over_time(range_avg_precision[30s])",
        base + 30,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 2 exceeded"),
        "{}",
        rejected.1
    );
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.extension_window_batch_query_count, 5);
    assert_eq!(stats.extension_window_batch_query_series_considered, 7);
    assert_eq!(stats.extension_window_batch_query_candidate_chunks, 6);
    assert_eq!(stats.extension_window_batch_query_decoded_points, 14);
    assert_eq!(stats.extension_window_batch_query_returned_points, 5);
    assert!(stats.extension_window_batch_query_payload_bytes_read > 0);
    assert!(stats.extension_window_batch_query_total_ns > 0);

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "avg_over_time(range_avg_precision[30s])",
            base + 30,
        )
        .await
        .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_min_over_time_boundaries_ieee_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_six_min_over_time.db");
    let base = 1_700_610_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_min 5 {}\n",
            "range_min 3 {}\n",
            "range_min 4 {}\n",
            "range_min_ieee{{case=\"all_nan\"}} NaN {}\n",
            "range_min_ieee{{case=\"all_nan\"}} NaN {}\n",
            "range_min_ieee{{case=\"mixed\"}} NaN {}\n",
            "range_min_ieee{{case=\"mixed\"}} 2 {}\n",
            "range_min_ieee{{case=\"infinite\"}} -Inf {}\n",
            "range_min_ieee{{case=\"infinite\"}} +Inf {}\n",
            "range_min_ieee{{case=\"zero\"}} 0 {}\n",
            "range_min_ieee{{case=\"zero\"}} -0 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let direct = prom_query(&app, "min_over_time(range_min[20s])", base + 30).await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base + 30, "3"]}])
    );
    let subquery = prom_query(&app, "min_over_time(range_min[20s:10s])", base + 30).await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1, direct.1);
    let range = prom_query_range(
        &app,
        "min_over_time(range_min[20s])",
        base + 20,
        base + 40,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base + 20, "3"], [base + 30, "3"], [base + 40, "4"]]
        }])
    );
    let ieee = prom_query(&app, "min_over_time(range_min_ieee[20s])", base + 30).await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "all_nan"}, "value": [base + 30, "NaN"]},
            {"metric": {"case": "infinite"}, "value": [base + 30, "-Inf"]},
            {"metric": {"case": "mixed"}, "value": [base + 30, "2"]},
            {"metric": {"case": "zero"}, "value": [base + 30, "0"]}
        ])
    );
    let empty = prom_query(&app, "min_over_time(range_min[20s])", base + 1_000).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "min_over_time(range_min[20s])", base + 30).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(&reopened_app, "min_over_time(range_min[20s])", base + 30)
            .await
            .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_max_over_time_boundaries_ieee_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_six_max_over_time.db");
    let base = 1_700_620_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_max 5 {}\n",
            "range_max 3 {}\n",
            "range_max 4 {}\n",
            "range_max_ieee{{case=\"all_nan\"}} NaN {}\n",
            "range_max_ieee{{case=\"all_nan\"}} NaN {}\n",
            "range_max_ieee{{case=\"mixed\"}} NaN {}\n",
            "range_max_ieee{{case=\"mixed\"}} 2 {}\n",
            "range_max_ieee{{case=\"infinite\"}} -Inf {}\n",
            "range_max_ieee{{case=\"infinite\"}} +Inf {}\n",
            "range_max_ieee{{case=\"zero\"}} 0 {}\n",
            "range_max_ieee{{case=\"zero\"}} -0 {}\n",
            "range_max_ieee{{case=\"zero_reverse\"}} -0 {}\n",
            "range_max_ieee{{case=\"zero_reverse\"}} 0 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let direct = prom_query(&app, "max_over_time(range_max[20s])", base + 30).await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base + 30, "4"]}])
    );
    let subquery = prom_query(&app, "max_over_time(range_max[20s:10s])", base + 30).await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1, direct.1);
    let range = prom_query_range(
        &app,
        "max_over_time(range_max[20s])",
        base + 20,
        base + 40,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base + 20, "5"], [base + 30, "4"], [base + 40, "4"]]
        }])
    );
    let ieee = prom_query(&app, "max_over_time(range_max_ieee[20s])", base + 30).await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "all_nan"}, "value": [base + 30, "NaN"]},
            {"metric": {"case": "infinite"}, "value": [base + 30, "+Inf"]},
            {"metric": {"case": "mixed"}, "value": [base + 30, "2"]},
            {"metric": {"case": "zero"}, "value": [base + 30, "0"]},
            {"metric": {"case": "zero_reverse"}, "value": [base + 30, "-0"]}
        ])
    );
    let empty = prom_query(&app, "max_over_time(range_max[20s])", base + 1_000).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "max_over_time(range_max[20s])", base + 30).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(&reopened_app, "max_over_time(range_max[20s])", base + 30)
            .await
            .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_sum_over_time_is_compensated_ieee_bounded_and_reopenable() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_six_sum_over_time.db");
    let base = 1_700_630_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_sum 5 {}\n",
            "range_sum 3 {}\n",
            "range_sum 4 {}\n",
            "range_sum_precision 1e16 {}\n",
            "range_sum_precision 1 {}\n",
            "range_sum_precision -1e16 {}\n",
            "range_sum_overflow 1.7976931348623157e308 {}\n",
            "range_sum_overflow 1.7976931348623157e308 {}\n",
            "range_sum_ieee{{case=\"nan\"}} NaN {}\n",
            "range_sum_ieee{{case=\"nan\"}} 1 {}\n",
            "range_sum_ieee{{case=\"positive\"}} +Inf {}\n",
            "range_sum_ieee{{case=\"positive\"}} 1 {}\n",
            "range_sum_ieee{{case=\"mixed\"}} +Inf {}\n",
            "range_sum_ieee{{case=\"mixed\"}} -Inf {}\n",
            "range_sum_zero{{case=\"forward\"}} 0 {}\n",
            "range_sum_zero{{case=\"forward\"}} -0 {}\n",
            "range_sum_zero{{case=\"reverse\"}} -0 {}\n",
            "range_sum_zero{{case=\"reverse\"}} 0 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let direct = prom_query(&app, "sum_over_time(range_sum[20s])", base + 30).await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base + 30, "7"]}])
    );
    let subquery = prom_query(&app, "sum_over_time(range_sum[20s:10s])", base + 30).await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1, direct.1);
    let range = prom_query_range(
        &app,
        "sum_over_time(range_sum[20s])",
        base + 20,
        base + 40,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base + 20, "8"], [base + 30, "7"], [base + 40, "4"]]
        }])
    );
    let precision = prom_query(&app, "sum_over_time(range_sum_precision[30s])", base + 30).await;
    assert_eq!(precision.0, StatusCode::OK, "{}", precision.1);
    assert_eq!(precision.1["data"]["result"][0]["value"][1], "1");
    let overflow = prom_query(&app, "sum_over_time(range_sum_overflow[20s])", base + 30).await;
    assert_eq!(overflow.0, StatusCode::OK, "{}", overflow.1);
    assert_eq!(overflow.1["data"]["result"][0]["value"][1], "+Inf");
    let ieee = prom_query(&app, "sum_over_time(range_sum_ieee[20s])", base + 30).await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "mixed"}, "value": [base + 30, "NaN"]},
            {"metric": {"case": "nan"}, "value": [base + 30, "NaN"]},
            {"metric": {"case": "positive"}, "value": [base + 30, "+Inf"]}
        ])
    );
    let zero = prom_query(&app, "sum_over_time(range_sum_zero[20s])", base + 30).await;
    assert_eq!(zero.0, StatusCode::OK, "{}", zero.1);
    assert_eq!(
        zero.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "forward"}, "value": [base + 30, "0"]},
            {"metric": {"case": "reverse"}, "value": [base + 30, "0"]}
        ])
    );
    let empty = prom_query(&app, "sum_over_time(range_sum[20s])", base + 1_000).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "sum_over_time(range_sum[20s])", base + 30).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(&reopened_app, "sum_over_time(range_sum[20s])", base + 30)
            .await
            .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_count_over_time_includes_ieee_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_six_count_over_time.db");
    let base = 1_700_640_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_count 5 {}\n",
            "range_count 3 {}\n",
            "range_count 4 {}\n",
            "range_count_ieee{{case=\"nan\"}} NaN {}\n",
            "range_count_ieee{{case=\"nan\"}} 1 {}\n",
            "range_count_ieee{{case=\"infinite\"}} -Inf {}\n",
            "range_count_ieee{{case=\"infinite\"}} +Inf {}\n",
            "range_count_ieee{{case=\"zero\"}} 0 {}\n",
            "range_count_ieee{{case=\"zero\"}} -0 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let direct = prom_query(&app, "count_over_time(range_count[20s])", base + 30).await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base + 30, "2"]}])
    );
    let subquery = prom_query(&app, "count_over_time(range_count[20s:10s])", base + 30).await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1, direct.1);
    let range = prom_query_range(
        &app,
        "count_over_time(range_count[20s])",
        base + 20,
        base + 40,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base + 20, "2"], [base + 30, "2"], [base + 40, "1"]]
        }])
    );
    let ieee = prom_query(&app, "count_over_time(range_count_ieee[20s])", base + 30).await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "infinite"}, "value": [base + 30, "2"]},
            {"metric": {"case": "nan"}, "value": [base + 30, "2"]},
            {"metric": {"case": "zero"}, "value": [base + 30, "2"]}
        ])
    );
    let empty = prom_query(&app, "count_over_time(range_count[20s])", base + 1_000).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "count_over_time(range_count[20s])", base + 30).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "count_over_time(range_count[20s])",
            base + 30,
        )
        .await
        .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_last_over_time_preserves_name_ieee_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_six_last_over_time.db");
    let base = 1_700_650_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_last 5 {}\n",
            "range_last 3 {}\n",
            "range_last 4 {}\n",
            "range_last_ieee{{case=\"nan\"}} 1 {}\n",
            "range_last_ieee{{case=\"nan\"}} NaN {}\n",
            "range_last_ieee{{case=\"infinite\"}} -Inf {}\n",
            "range_last_ieee{{case=\"infinite\"}} +Inf {}\n",
            "range_last_ieee{{case=\"zero\"}} 0 {}\n",
            "range_last_ieee{{case=\"zero\"}} -0 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let direct = prom_query(&app, "last_over_time(range_last[20s])", base + 30).await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "range_last"},
            "value": [base + 30, "4"]
        }])
    );
    let subquery = prom_query(&app, "last_over_time(range_last[20s:10s])", base + 30).await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1, direct.1);
    let range = prom_query_range(
        &app,
        "last_over_time(range_last[20s])",
        base + 20,
        base + 40,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "range_last"},
            "values": [[base + 20, "3"], [base + 30, "4"], [base + 40, "4"]]
        }])
    );
    let ieee = prom_query(&app, "last_over_time(range_last_ieee[20s])", base + 30).await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "range_last_ieee", "case": "infinite"}, "value": [base + 30, "+Inf"]},
            {"metric": {"__name__": "range_last_ieee", "case": "nan"}, "value": [base + 30, "NaN"]},
            {"metric": {"__name__": "range_last_ieee", "case": "zero"}, "value": [base + 30, "-0"]}
        ])
    );
    let empty = prom_query(&app, "last_over_time(range_last[20s])", base + 1_000).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "last_over_time(range_last[20s])", base + 30).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(&reopened_app, "last_over_time(range_last[20s])", base + 30)
            .await
            .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_first_over_time_is_explicitly_experimental() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory
        .path()
        .join("session_six_first_over_time_experimental.db");
    let storage = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let app = router(storage.clone());

    let response = prom_query(&app, "first_over_time(vector(1)[1m:])", 2).await;
    assert_eq!(response.0, StatusCode::BAD_REQUEST, "{}", response.1);
    assert_eq!(response.1["status"], "error");
    assert_eq!(response.1["errorType"], "bad_data");
    assert_eq!(
        response.1["error"],
        "invalid parameter \"query\": first_over_time is experimental and is not enabled in the stable PromQL compatibility tier"
    );
    assert_eq!(response.1.as_object().unwrap().len(), 3);

    drop(app);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_present_over_time_tracks_presence_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_six_present_over_time.db");
    let base = 1_700_660_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_present 5 {}\n",
            "range_present 3 {}\n",
            "range_present 4 {}\n",
            "range_present_ieee{{case=\"nan\"}} NaN {}\n",
            "range_present_ieee{{case=\"infinite\"}} +Inf {}\n",
            "range_present_ieee{{case=\"zero\"}} -0 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let direct = prom_query(&app, "present_over_time(range_present[20s])", base + 30).await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base + 30, "1"]}])
    );
    let subquery = prom_query(&app, "present_over_time(range_present[20s:10s])", base + 30).await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1, direct.1);
    let range = prom_query_range(
        &app,
        "present_over_time(range_present[20s])",
        base + 20,
        base + 50,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base + 20, "1"], [base + 30, "1"], [base + 40, "1"]]
        }])
    );
    let ieee = prom_query(
        &app,
        "present_over_time(range_present_ieee[20s])",
        base + 30,
    )
    .await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "infinite"}, "value": [base + 30, "1"]},
            {"metric": {"case": "nan"}, "value": [base + 30, "1"]},
            {"metric": {"case": "zero"}, "value": [base + 30, "1"]}
        ])
    );
    let empty = prom_query(&app, "present_over_time(range_present[20s])", base + 1_000).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "present_over_time(range_present[20s])", base + 30).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "present_over_time(range_present[20s])",
            base + 30,
        )
        .await
        .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_quantile_over_time_interpolates_ieee_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_six_quantile_over_time.db");
    let base = 1_700_670_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_quantile 5 {}\n",
            "range_quantile 3 {}\n",
            "range_quantile 4 {}\n",
            "range_quantile_ieee{{case=\"mixed\"}} NaN {}\n",
            "range_quantile_ieee{{case=\"mixed\"}} 2 {}\n",
            "range_quantile_ieee{{case=\"infinite\"}} -Inf {}\n",
            "range_quantile_ieee{{case=\"infinite\"}} +Inf {}\n",
            "range_quantile_ieee{{case=\"zero\"}} 0 {}\n",
            "range_quantile_ieee{{case=\"zero\"}} -0 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let direct = prom_query(
        &app,
        "quantile_over_time(1 / 2, range_quantile[20s])",
        base + 30,
    )
    .await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base + 30, "3.5"]}])
    );
    let subquery = prom_query(
        &app,
        "quantile_over_time(0.5, range_quantile[20s:10s])",
        base + 30,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1, direct.1);
    let range = prom_query_range(
        &app,
        "quantile_over_time(0.5, range_quantile[20s])",
        base + 20,
        base + 40,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base + 20, "4"], [base + 30, "3.5"], [base + 40, "4"]]
        }])
    );

    for (parameter, case, expected) in [
        ("0", "mixed", "NaN"),
        ("1", "mixed", "2"),
        ("0.5", "infinite", "NaN"),
        ("0", "zero", "0"),
        ("1", "zero", "-0"),
    ] {
        let query =
            format!("quantile_over_time({parameter}, range_quantile_ieee{{case=\"{case}\"}}[20s])");
        let result = prom_query(&app, &query, base + 30).await;
        assert_eq!(result.0, StatusCode::OK, "{query}: {}", result.1);
        assert_eq!(
            result.1["data"]["result"][0]["value"][1], expected,
            "{query}"
        );
    }
    for (parameter, expected) in [("NaN", "NaN"), ("-1", "-Inf"), ("2", "+Inf")] {
        let query = format!("quantile_over_time({parameter}, range_quantile[20s])");
        let result = prom_query(&app, &query, base + 30).await;
        assert_eq!(result.0, StatusCode::OK, "{query}: {}", result.1);
        assert_eq!(
            result.1["data"]["result"][0]["value"][1], expected,
            "{query}"
        );
        assert_eq!(
            result.1["warnings"],
            serde_json::json!([format!(
                "PromQL warning: quantile value should be between 0 and 1, got {parameter} (1:20)"
            )])
        );
    }
    let empty = prom_query(
        &app,
        "quantile_over_time(0.5, range_quantile[20s])",
        base + 1_000,
    )
    .await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    for (query, expected) in [
        (
            "quantile_over_time(range_quantile[20s])",
            "invalid parameter \"query\": 1:1: parse error: expected 2 argument(s) in call to \"quantile_over_time\", got 1",
        ),
        (
            "quantile_over_time(range_quantile, range_quantile[20s])",
            "invalid parameter \"query\": 1:20: parse error: expected type scalar in call to function \"quantile_over_time\", got instant vector",
        ),
    ] {
        let invalid = prom_query(&app, query, base + 30).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data", "{query}");
        assert_eq!(invalid.1["error"], expected, "{query}");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "quantile_over_time(0.5, range_quantile[20s])",
        base + 30,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "quantile_over_time(1 / 2, range_quantile[20s])",
            base + 30,
        )
        .await
        .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_stddev_over_time_is_population_ieee_and_reopenable() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_six_stddev_over_time.db");
    let base = 1_700_680_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_stddev 5 {}\n",
            "range_stddev 3 {}\n",
            "range_stddev 4 {}\n",
            "range_stddev_wide 10000000000000000 {}\n",
            "range_stddev_wide 1 {}\n",
            "range_stddev_wide -10000000000000000 {}\n",
            "range_stddev_ieee{{case=\"nan\"}} NaN {}\n",
            "range_stddev_ieee{{case=\"nan\"}} 2 {}\n",
            "range_stddev_ieee{{case=\"infinite\"}} -Inf {}\n",
            "range_stddev_ieee{{case=\"infinite\"}} +Inf {}\n",
            "range_stddev_ieee{{case=\"zero\"}} 0 {}\n",
            "range_stddev_ieee{{case=\"zero\"}} -0 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let direct = prom_query(&app, "stddev_over_time(range_stddev[20s])", base + 30).await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base + 30, "0.5"]}])
    );
    let subquery = prom_query(&app, "stddev_over_time(range_stddev[20s:10s])", base + 30).await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1, direct.1);
    let range = prom_query_range(
        &app,
        "stddev_over_time(range_stddev[20s])",
        base + 20,
        base + 40,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base + 20, "1"], [base + 30, "0.5"], [base + 40, "0"]]
        }])
    );
    let wide = prom_query(&app, "stddev_over_time(range_stddev_wide[30s])", base + 30).await;
    assert_eq!(wide.0, StatusCode::OK, "{}", wide.1);
    assert_eq!(wide.1["data"]["result"][0]["value"][1], "8164965809277260");
    let ieee = prom_query(&app, "stddev_over_time(range_stddev_ieee[20s])", base + 30).await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "infinite"}, "value": [base + 30, "NaN"]},
            {"metric": {"case": "nan"}, "value": [base + 30, "NaN"]},
            {"metric": {"case": "zero"}, "value": [base + 30, "0"]}
        ])
    );
    let empty = prom_query(&app, "stddev_over_time(range_stddev[20s])", base + 1_000).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));
    let invalid = prom_query(&app, "stddev_over_time(range_stddev)", base + 30).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "stddev_over_time(range_stddev[20s])", base + 30).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "stddev_over_time(range_stddev[20s])",
            base + 30,
        )
        .await
        .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_six_promql_stdvar_over_time_is_population_ieee_and_reopenable() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_six_stdvar_over_time.db");
    let base = 1_700_690_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_stdvar 5 {}\n",
            "range_stdvar 3 {}\n",
            "range_stdvar 4 {}\n",
            "range_stdvar_wide 10000000000000000 {}\n",
            "range_stdvar_wide 1 {}\n",
            "range_stdvar_wide -10000000000000000 {}\n",
            "range_stdvar_ieee{{case=\"nan\"}} NaN {}\n",
            "range_stdvar_ieee{{case=\"nan\"}} 2 {}\n",
            "range_stdvar_ieee{{case=\"infinite\"}} -Inf {}\n",
            "range_stdvar_ieee{{case=\"infinite\"}} +Inf {}\n",
            "range_stdvar_ieee{{case=\"zero\"}} 0 {}\n",
            "range_stdvar_ieee{{case=\"zero\"}} -0 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let direct = prom_query(&app, "stdvar_over_time(range_stdvar[20s])", base + 30).await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base + 30, "0.25"]}])
    );
    let subquery = prom_query(&app, "stdvar_over_time(range_stdvar[20s:10s])", base + 30).await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1, direct.1);
    let range = prom_query_range(
        &app,
        "stdvar_over_time(range_stdvar[20s])",
        base + 20,
        base + 40,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base + 20, "1"], [base + 30, "0.25"], [base + 40, "0"]]
        }])
    );
    let wide = prom_query(&app, "stdvar_over_time(range_stdvar_wide[30s])", base + 30).await;
    assert_eq!(wide.0, StatusCode::OK, "{}", wide.1);
    assert_eq!(
        wide.1["data"]["result"][0]["value"][1],
        "6.666666666666666e+31"
    );
    let ieee = prom_query(&app, "stdvar_over_time(range_stdvar_ieee[20s])", base + 30).await;
    assert_eq!(ieee.0, StatusCode::OK, "{}", ieee.1);
    assert_eq!(
        ieee.1["data"]["result"],
        serde_json::json!([
            {"metric": {"case": "infinite"}, "value": [base + 30, "NaN"]},
            {"metric": {"case": "nan"}, "value": [base + 30, "NaN"]},
            {"metric": {"case": "zero"}, "value": [base + 30, "0"]}
        ])
    );
    let empty = prom_query(&app, "stdvar_over_time(range_stdvar[20s])", base + 1_000).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));
    let invalid = prom_query(&app, "stdvar_over_time(range_stdvar)", base + 30).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "stdvar_over_time(range_stdvar[20s])", base + 30).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "stdvar_over_time(range_stdvar[20s])",
            base + 30,
        )
        .await
        .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_seven_promql_rate_extrapolates_resets_bounds_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_seven_rate.db");
    let base = 1_700_700_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_rate{{case=\"steady\"}} 100 {}\n",
            "range_rate{{case=\"steady\"}} 300 {}\n",
            "range_rate{{case=\"steady\"}} 500 {}\n",
            "range_rate{{case=\"reset\"}} 100 {}\n",
            "range_rate{{case=\"reset\"}} 150 {}\n",
            "range_rate{{case=\"reset\"}} 20 {}\n",
            "range_rate{{case=\"sparse\"}} 100 {}\n",
            "range_rate{{case=\"sparse\"}} 200 {}\n",
            "range_rate{{case=\"zero\"}} 1 {}\n",
            "range_rate{{case=\"zero\"}} 101 {}\n",
            "range_rate{{case=\"singleton\"}} 5 {}\n",
            "range_rate{{case=\"nan\"}} NaN {}\n",
            "range_rate{{case=\"nan\"}} 2 {}\n",
            "range_rate{{case=\"pos_inf\"}} 1 {}\n",
            "range_rate{{case=\"pos_inf\"}} +Inf {}\n",
            "range_rate{{case=\"neg_inf\"}} 1 {}\n",
            "range_rate{{case=\"neg_inf\"}} -Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 30) * 1_000,
        (base + 40) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let steady = prom_query(&app, "rate(range_rate{case=\"steady\"}[60s])", base + 60).await;
    assert_eq!(steady.0, StatusCode::OK, "{}", steady.1);
    assert_eq!(
        steady.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 60, "10"]}])
    );
    assert_eq!(
        steady.1["infos"],
        serde_json::json!([
            "PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: \"range_rate\" (1:6)"
        ])
    );
    let posted_steady = post_form(
        &app,
        "/prometheus/api/v1/query",
        &form_urlencoded::Serializer::new(String::new())
            .append_pair("query", "rate(range_rate{case=\"steady\"}[60s])")
            .append_pair("time", &(base + 60).to_string())
            .finish(),
    )
    .await;
    assert_eq!(posted_steady, steady);

    for (case, window, at, expected) in [
        ("reset", 60, 60, "1.75"),
        ("sparse", 60, 60, "3.3333333333333335"),
        ("zero", 40, 40, "3.775"),
    ] {
        let response = prom_query(
            &app,
            &format!("rate(range_rate{{case=\"{case}\"}}[{window}s])"),
            base + at,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"],
            serde_json::json!([{
                "metric": {"case": case},
                "value": [base + at, expected]
            }])
        );
    }
    let boundary = prom_query(&app, "rate(range_rate{case=\"steady\"}[40s])", base + 50).await;
    assert_eq!(boundary.0, StatusCode::OK, "{}", boundary.1);
    assert_eq!(
        boundary.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 50, "10"]}])
    );
    let offset = prom_query(
        &app,
        "rate(range_rate{case=\"steady\"}[40s] offset 10s)",
        base + 60,
    )
    .await;
    assert_eq!(offset.0, StatusCode::OK, "{}", offset.1);
    assert_eq!(offset.1, steady.1);
    let subquery = prom_query(
        &app,
        "rate(range_rate{case=\"steady\"}[40s:10s])",
        base + 60,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(
        subquery.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "steady"},
            "value": [base + 60, "6.666666666666667"]
        }])
    );
    assert_eq!(
        subquery.1["infos"],
        serde_json::json!([
            "PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: \"range_rate\" (1:1)"
        ])
    );
    let nan = prom_query(&app, "rate(range_rate{case=\"nan\"}[60s])", base + 60).await;
    assert_eq!(nan.0, StatusCode::OK, "{}", nan.1);
    assert_eq!(nan.1["data"]["result"][0]["value"][1], "NaN");
    for (case, expected) in [("pos_inf", "+Inf"), ("neg_inf", "-Inf")] {
        let response = prom_query(
            &app,
            &format!("rate(range_rate{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }
    let singleton = prom_query(&app, "rate(range_rate{case=\"singleton\"}[60s])", base + 60).await;
    assert_eq!(singleton.0, StatusCode::OK, "{}", singleton.1);
    assert_eq!(singleton.1["data"]["result"], serde_json::json!([]));
    assert!(singleton.1.get("infos").is_none(), "{}", singleton.1);
    let range = prom_query_range(
        &app,
        "rate(range_rate{case=\"steady\"}[40s])",
        base + 50,
        base + 60,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "steady"},
            "values": [[base + 50, "10"], [base + 60, "10"]]
        }])
    );
    assert_eq!(range.1["infos"], steady.1["infos"]);
    let invalid = prom_query(&app, "rate(range_rate)", base + 60).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "rate(range_rate{case=\"steady\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "rate(range_rate{case=\"steady\"}[60s])",
            base + 60,
        )
        .await
        .1,
        steady.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_seven_promql_irate_uses_last_two_samples_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_seven_irate.db");
    let base = 1_700_710_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_irate{{case=\"steady\"}} 100 {}\n",
            "range_irate{{case=\"steady\"}} 300 {}\n",
            "range_irate{{case=\"steady\"}} 500 {}\n",
            "range_irate{{case=\"reset\"}} 100 {}\n",
            "range_irate{{case=\"reset\"}} 150 {}\n",
            "range_irate{{case=\"reset\"}} 20 {}\n",
            "range_irate{{case=\"sparse\"}} 100 {}\n",
            "range_irate{{case=\"sparse\"}} 200 {}\n",
            "range_irate{{case=\"singleton\"}} 5 {}\n",
            "range_irate{{case=\"nan\"}} NaN {}\n",
            "range_irate{{case=\"nan\"}} 2 {}\n",
            "range_irate{{case=\"pos_inf\"}} 1 {}\n",
            "range_irate{{case=\"pos_inf\"}} +Inf {}\n",
            "range_irate{{case=\"neg_inf\"}} 1 {}\n",
            "range_irate{{case=\"neg_inf\"}} -Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 30) * 1_000,
        (base + 40) * 1_000,
        (base + 50) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let steady = prom_query(&app, "irate(range_irate{case=\"steady\"}[60s])", base + 60).await;
    assert_eq!(steady.0, StatusCode::OK, "{}", steady.1);
    assert_eq!(
        steady.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 60, "10"]}])
    );
    for (case, expected) in [("reset", "1"), ("sparse", "10")] {
        let response = prom_query(
            &app,
            &format!("irate(range_irate{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"],
            serde_json::json!([{
                "metric": {"case": case},
                "value": [base + 60, expected]
            }])
        );
    }
    let boundary = prom_query(&app, "irate(range_irate{case=\"steady\"}[40s])", base + 50).await;
    assert_eq!(boundary.0, StatusCode::OK, "{}", boundary.1);
    assert_eq!(
        boundary.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 50, "10"]}])
    );
    let offset = prom_query(
        &app,
        "irate(range_irate{case=\"steady\"}[40s] offset 10s)",
        base + 60,
    )
    .await;
    assert_eq!(offset.0, StatusCode::OK, "{}", offset.1);
    assert_eq!(offset.1, steady.1);
    let subquery = prom_query(
        &app,
        "irate(range_irate{case=\"steady\"}[40s:10s])",
        base + 60,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(
        subquery.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "steady"},
            "value": [base + 60, "0"]
        }])
    );
    let nan = prom_query(&app, "irate(range_irate{case=\"nan\"}[60s])", base + 60).await;
    assert_eq!(nan.0, StatusCode::OK, "{}", nan.1);
    assert_eq!(nan.1["data"]["result"][0]["value"][1], "NaN");
    for (case, expected) in [("pos_inf", "+Inf"), ("neg_inf", "-Inf")] {
        let response = prom_query(
            &app,
            &format!("irate(range_irate{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }
    let singleton = prom_query(
        &app,
        "irate(range_irate{case=\"singleton\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(singleton.0, StatusCode::OK, "{}", singleton.1);
    assert_eq!(singleton.1["data"]["result"], serde_json::json!([]));
    let range = prom_query_range(
        &app,
        "irate(range_irate{case=\"steady\"}[40s])",
        base + 50,
        base + 60,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "steady"},
            "values": [[base + 50, "10"], [base + 60, "10"]]
        }])
    );
    let invalid = prom_query(&app, "irate(range_irate)", base + 60).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "irate(range_irate{case=\"steady\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "irate(range_irate{case=\"steady\"}[60s])",
            base + 60,
        )
        .await
        .1,
        steady.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_seven_promql_increase_extrapolates_without_rate_normalization() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_seven_increase.db");
    let base = 1_700_720_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_increase{{case=\"steady\"}} 100 {}\n",
            "range_increase{{case=\"steady\"}} 300 {}\n",
            "range_increase{{case=\"steady\"}} 500 {}\n",
            "range_increase{{case=\"reset\"}} 100 {}\n",
            "range_increase{{case=\"reset\"}} 150 {}\n",
            "range_increase{{case=\"reset\"}} 20 {}\n",
            "range_increase{{case=\"sparse\"}} 100 {}\n",
            "range_increase{{case=\"sparse\"}} 200 {}\n",
            "range_increase{{case=\"zero\"}} 1 {}\n",
            "range_increase{{case=\"zero\"}} 101 {}\n",
            "range_increase{{case=\"singleton\"}} 5 {}\n",
            "range_increase{{case=\"nan\"}} NaN {}\n",
            "range_increase{{case=\"nan\"}} 2 {}\n",
            "range_increase{{case=\"pos_inf\"}} 1 {}\n",
            "range_increase{{case=\"pos_inf\"}} +Inf {}\n",
            "range_increase{{case=\"neg_inf\"}} 1 {}\n",
            "range_increase{{case=\"neg_inf\"}} -Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 30) * 1_000,
        (base + 40) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let steady = prom_query(
        &app,
        "increase(range_increase{case=\"steady\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(steady.0, StatusCode::OK, "{}", steady.1);
    assert_eq!(
        steady.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 60, "600"]}])
    );
    for (case, window, at, expected) in [
        ("reset", 60, 60, "105"),
        ("sparse", 60, 60, "200"),
        ("zero", 40, 40, "151"),
    ] {
        let response = prom_query(
            &app,
            &format!("increase(range_increase{{case=\"{case}\"}}[{window}s])"),
            base + at,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"],
            serde_json::json!([{
                "metric": {"case": case},
                "value": [base + at, expected]
            }])
        );
    }
    let boundary = prom_query(
        &app,
        "increase(range_increase{case=\"steady\"}[40s])",
        base + 50,
    )
    .await;
    assert_eq!(boundary.0, StatusCode::OK, "{}", boundary.1);
    assert_eq!(
        boundary.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 50, "400"]}])
    );
    let offset = prom_query(
        &app,
        "increase(range_increase{case=\"steady\"}[40s] offset 10s)",
        base + 60,
    )
    .await;
    assert_eq!(offset.0, StatusCode::OK, "{}", offset.1);
    assert_eq!(
        offset.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 60, "400"]}])
    );
    let subquery = prom_query(
        &app,
        "increase(range_increase{case=\"steady\"}[40s:10s])",
        base + 60,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(
        subquery.1["data"]["result"][0]["value"],
        serde_json::json!([base + 60, "266.66666666666663"])
    );
    for (case, expected) in [("nan", "NaN"), ("pos_inf", "+Inf"), ("neg_inf", "-Inf")] {
        let response = prom_query(
            &app,
            &format!("increase(range_increase{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }
    let singleton = prom_query(
        &app,
        "increase(range_increase{case=\"singleton\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(singleton.0, StatusCode::OK, "{}", singleton.1);
    assert_eq!(singleton.1["data"]["result"], serde_json::json!([]));
    let range = prom_query_range(
        &app,
        "increase(range_increase{case=\"steady\"}[40s])",
        base + 50,
        base + 60,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "steady"},
            "values": [[base + 50, "400"], [base + 60, "400"]]
        }])
    );
    let invalid = prom_query(&app, "increase(range_increase)", base + 60).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "increase(range_increase{case=\"steady\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "increase(range_increase{case=\"steady\"}[60s])",
            base + 60,
        )
        .await
        .1,
        steady.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_seven_promql_delta_extrapolates_gauges_without_counter_correction() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_seven_delta.db");
    let base = 1_700_730_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_delta{{case=\"steady\"}} 100 {}\n",
            "range_delta{{case=\"steady\"}} 300 {}\n",
            "range_delta{{case=\"steady\"}} 500 {}\n",
            "range_delta{{case=\"decrease\"}} 100 {}\n",
            "range_delta{{case=\"decrease\"}} 150 {}\n",
            "range_delta{{case=\"decrease\"}} 20 {}\n",
            "range_delta{{case=\"sparse\"}} 100 {}\n",
            "range_delta{{case=\"sparse\"}} 200 {}\n",
            "range_delta{{case=\"zero\"}} 1 {}\n",
            "range_delta{{case=\"zero\"}} 101 {}\n",
            "range_delta{{case=\"singleton\"}} 5 {}\n",
            "range_delta{{case=\"nan\"}} NaN {}\n",
            "range_delta{{case=\"nan\"}} 2 {}\n",
            "range_delta{{case=\"pos_inf\"}} 1 {}\n",
            "range_delta{{case=\"pos_inf\"}} +Inf {}\n",
            "range_delta{{case=\"neg_inf\"}} 1 {}\n",
            "range_delta{{case=\"neg_inf\"}} -Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 30) * 1_000,
        (base + 40) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let steady = prom_query(&app, "delta(range_delta{case=\"steady\"}[60s])", base + 60).await;
    assert_eq!(steady.0, StatusCode::OK, "{}", steady.1);
    assert_eq!(
        steady.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 60, "600"]}])
    );
    for (case, window, at, expected) in [
        ("decrease", 60, 60, "-120"),
        ("sparse", 60, 60, "200"),
        ("zero", 40, 40, "200"),
    ] {
        let response = prom_query(
            &app,
            &format!("delta(range_delta{{case=\"{case}\"}}[{window}s])"),
            base + at,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"],
            serde_json::json!([{
                "metric": {"case": case},
                "value": [base + at, expected]
            }])
        );
    }
    let boundary = prom_query(&app, "delta(range_delta{case=\"steady\"}[40s])", base + 50).await;
    assert_eq!(boundary.0, StatusCode::OK, "{}", boundary.1);
    assert_eq!(
        boundary.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 50, "400"]}])
    );
    let offset = prom_query(
        &app,
        "delta(range_delta{case=\"steady\"}[40s] offset 10s)",
        base + 60,
    )
    .await;
    assert_eq!(offset.0, StatusCode::OK, "{}", offset.1);
    assert_eq!(
        offset.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 60, "400"]}])
    );
    let subquery = prom_query(
        &app,
        "delta(range_delta{case=\"steady\"}[40s:10s])",
        base + 60,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(
        subquery.1["data"]["result"][0]["value"],
        serde_json::json!([base + 60, "266.66666666666663"])
    );
    for (case, expected) in [("nan", "NaN"), ("pos_inf", "+Inf"), ("neg_inf", "-Inf")] {
        let response = prom_query(
            &app,
            &format!("delta(range_delta{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }
    let singleton = prom_query(
        &app,
        "delta(range_delta{case=\"singleton\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(singleton.0, StatusCode::OK, "{}", singleton.1);
    assert_eq!(singleton.1["data"]["result"], serde_json::json!([]));
    let range = prom_query_range(
        &app,
        "delta(range_delta{case=\"steady\"}[40s])",
        base + 50,
        base + 60,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "steady"},
            "values": [[base + 50, "400"], [base + 60, "400"]]
        }])
    );
    let invalid = prom_query(&app, "delta(range_delta)", base + 60).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "delta(range_delta{case=\"steady\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "delta(range_delta{case=\"steady\"}[60s])",
            base + 60,
        )
        .await
        .1,
        steady.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_seven_promql_idelta_uses_only_the_final_gauge_pair() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_seven_idelta.db");
    let base = 1_700_740_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_idelta{{case=\"steady\"}} 100 {}\n",
            "range_idelta{{case=\"steady\"}} 300 {}\n",
            "range_idelta{{case=\"steady\"}} 500 {}\n",
            "range_idelta{{case=\"decrease\"}} 100 {}\n",
            "range_idelta{{case=\"decrease\"}} 150 {}\n",
            "range_idelta{{case=\"decrease\"}} 20 {}\n",
            "range_idelta{{case=\"sparse\"}} 100 {}\n",
            "range_idelta{{case=\"sparse\"}} 200 {}\n",
            "range_idelta{{case=\"singleton\"}} 5 {}\n",
            "range_idelta{{case=\"nan\"}} NaN {}\n",
            "range_idelta{{case=\"nan\"}} 2 {}\n",
            "range_idelta{{case=\"pos_inf\"}} 1 {}\n",
            "range_idelta{{case=\"pos_inf\"}} +Inf {}\n",
            "range_idelta{{case=\"neg_inf\"}} 1 {}\n",
            "range_idelta{{case=\"neg_inf\"}} -Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 30) * 1_000,
        (base + 40) * 1_000,
        (base + 50) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let steady = prom_query(
        &app,
        "idelta(range_idelta{case=\"steady\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(steady.0, StatusCode::OK, "{}", steady.1);
    assert_eq!(
        steady.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 60, "200"]}])
    );
    for (case, expected) in [("decrease", "-130"), ("sparse", "100")] {
        let response = prom_query(
            &app,
            &format!("idelta(range_idelta{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"],
            serde_json::json!([{
                "metric": {"case": case},
                "value": [base + 60, expected]
            }])
        );
    }
    let boundary = prom_query(
        &app,
        "idelta(range_idelta{case=\"steady\"}[40s])",
        base + 50,
    )
    .await;
    assert_eq!(boundary.0, StatusCode::OK, "{}", boundary.1);
    assert_eq!(
        boundary.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 50, "200"]}])
    );
    let offset = prom_query(
        &app,
        "idelta(range_idelta{case=\"steady\"}[40s] offset 10s)",
        base + 60,
    )
    .await;
    assert_eq!(offset.0, StatusCode::OK, "{}", offset.1);
    assert_eq!(offset.1, steady.1);
    let subquery = prom_query(
        &app,
        "idelta(range_idelta{case=\"steady\"}[40s:10s])",
        base + 60,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(
        subquery.1["data"]["result"][0]["value"],
        serde_json::json!([base + 60, "0"])
    );
    for (case, expected) in [("nan", "NaN"), ("pos_inf", "+Inf"), ("neg_inf", "-Inf")] {
        let response = prom_query(
            &app,
            &format!("idelta(range_idelta{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }
    let singleton = prom_query(
        &app,
        "idelta(range_idelta{case=\"singleton\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(singleton.0, StatusCode::OK, "{}", singleton.1);
    assert_eq!(singleton.1["data"]["result"], serde_json::json!([]));
    let range = prom_query_range(
        &app,
        "idelta(range_idelta{case=\"steady\"}[40s])",
        base + 50,
        base + 60,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "steady"},
            "values": [[base + 50, "200"], [base + 60, "200"]]
        }])
    );
    let invalid = prom_query(&app, "idelta(range_idelta)", base + 60).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "idelta(range_idelta{case=\"steady\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "idelta(range_idelta{case=\"steady\"}[60s])",
            base + 60,
        )
        .await
        .1,
        steady.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_seven_promql_deriv_matches_centered_compensated_regression() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_seven_deriv.db");
    let base = 1_700_750_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_deriv{{case=\"steady\"}} 100 {}\n",
            "range_deriv{{case=\"steady\"}} 300 {}\n",
            "range_deriv{{case=\"steady\"}} 500 {}\n",
            "range_deriv{{case=\"decrease\"}} 100 {}\n",
            "range_deriv{{case=\"decrease\"}} 150 {}\n",
            "range_deriv{{case=\"decrease\"}} 20 {}\n",
            "range_deriv{{case=\"sparse\"}} 100 {}\n",
            "range_deriv{{case=\"sparse\"}} 200 {}\n",
            "range_deriv{{case=\"constant\"}} 7 {}\n",
            "range_deriv{{case=\"constant\"}} 7 {}\n",
            "range_deriv{{case=\"constant\"}} 7 {}\n",
            "range_deriv{{case=\"singleton\"}} 5 {}\n",
            "range_deriv{{case=\"nan\"}} NaN {}\n",
            "range_deriv{{case=\"nan\"}} 2 {}\n",
            "range_deriv{{case=\"pos_inf\"}} 1 {}\n",
            "range_deriv{{case=\"pos_inf\"}} +Inf {}\n",
            "range_deriv{{case=\"neg_inf\"}} 1 {}\n",
            "range_deriv{{case=\"neg_inf\"}} -Inf {}\n",
            "range_deriv{{case=\"constant_inf\"}} +Inf {}\n",
            "range_deriv{{case=\"constant_inf\"}} +Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 30) * 1_000,
        (base + 40) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 50) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let steady = prom_query(&app, "deriv(range_deriv{case=\"steady\"}[60s])", base + 60).await;
    assert_eq!(steady.0, StatusCode::OK, "{}", steady.1);
    assert_eq!(
        steady.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 60, "10"]}])
    );
    for (case, expected) in [("decrease", "-2"), ("sparse", "10"), ("constant", "0")] {
        let response = prom_query(
            &app,
            &format!("deriv(range_deriv{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
        assert_eq!(
            response.1["data"]["result"][0]["metric"]["__name__"],
            Value::Null
        );
    }
    let boundary = prom_query(&app, "deriv(range_deriv{case=\"steady\"}[40s])", base + 50).await;
    assert_eq!(boundary.0, StatusCode::OK, "{}", boundary.1);
    assert_eq!(boundary.1["data"]["result"][0]["value"][1], "10");
    let offset = prom_query(
        &app,
        "deriv(range_deriv{case=\"steady\"}[40s] offset 10s)",
        base + 60,
    )
    .await;
    assert_eq!(offset.0, StatusCode::OK, "{}", offset.1);
    assert_eq!(offset.1, steady.1);
    let subquery = prom_query(
        &app,
        "deriv(range_deriv{case=\"steady\"}[40s:10s])",
        base + 60,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1["data"]["result"][0]["value"][1], "8");
    for case in ["nan", "pos_inf", "neg_inf", "constant_inf"] {
        let response = prom_query(
            &app,
            &format!("deriv(range_deriv{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], "NaN");
    }
    let singleton = prom_query(
        &app,
        "deriv(range_deriv{case=\"singleton\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(singleton.0, StatusCode::OK, "{}", singleton.1);
    assert_eq!(singleton.1["data"]["result"], serde_json::json!([]));
    let range = prom_query_range(
        &app,
        "deriv(range_deriv{case=\"steady\"}[40s])",
        base + 50,
        base + 60,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "steady"},
            "values": [[base + 50, "10"], [base + 60, "10"]]
        }])
    );
    let invalid = prom_query(&app, "deriv(range_deriv)", base + 60).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "deriv(range_deriv{case=\"steady\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "deriv(range_deriv{case=\"steady\"}[60s])",
            base + 60,
        )
        .await
        .1,
        steady.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_seven_promql_predict_linear_anchors_at_evaluation_time() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_seven_predict_linear.db");
    let base = 1_700_760_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_predict{{case=\"steady\"}} 100 {}\n",
            "range_predict{{case=\"steady\"}} 300 {}\n",
            "range_predict{{case=\"steady\"}} 500 {}\n",
            "range_predict{{case=\"nonlinear\"}} 100 {}\n",
            "range_predict{{case=\"nonlinear\"}} 150 {}\n",
            "range_predict{{case=\"nonlinear\"}} 20 {}\n",
            "range_predict{{case=\"constant\"}} 7 {}\n",
            "range_predict{{case=\"constant\"}} 7 {}\n",
            "range_predict{{case=\"constant\"}} 7 {}\n",
            "range_predict{{case=\"constant_inf\"}} +Inf {}\n",
            "range_predict{{case=\"constant_inf\"}} +Inf {}\n",
            "range_predict{{case=\"singleton\"}} 5 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 50) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let steady = prom_query(
        &app,
        "predict_linear(range_predict{case=\"steady\"}[60s], 10)",
        base + 60,
    )
    .await;
    assert_eq!(steady.0, StatusCode::OK, "{}", steady.1);
    assert_eq!(
        steady.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 60, "700"]}])
    );
    for (case, horizon, expected) in [
        ("steady", "0", "600"),
        ("steady", "-10", "500"),
        ("nonlinear", "10", "10"),
        ("constant", "10", "7"),
    ] {
        let response = prom_query(
            &app,
            &format!("predict_linear(range_predict{{case=\"{case}\"}}[60s], {horizon})"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
        assert_eq!(
            response.1["data"]["result"][0]["metric"]["__name__"],
            Value::Null
        );
    }
    for query in [
        "predict_linear(range_predict{case=\"steady\"}[60s], 5 + 5)",
        "predict_linear(range_predict{case=\"steady\"}[60s] offset 10s, 10)",
        "predict_linear(range_predict{case=\"steady\"}[60s] @ end(), 10)",
    ] {
        let response = prom_query(&app, query, base + 60).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1, steady.1, "{query}");
    }
    let subquery = prom_query(
        &app,
        "predict_linear(range_predict{case=\"steady\"}[40s:10s], 10)",
        base + 60,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1["data"]["result"][0]["value"][1], "600");
    for (horizon, expected) in [("NaN", "NaN"), ("Inf", "+Inf"), ("-Inf", "-Inf")] {
        let response = prom_query(
            &app,
            &format!("predict_linear(range_predict{{case=\"steady\"}}[60s], {horizon})"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{horizon}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }
    let constant_inf = prom_query(
        &app,
        "predict_linear(range_predict{case=\"constant_inf\"}[60s], 10)",
        base + 60,
    )
    .await;
    assert_eq!(constant_inf.0, StatusCode::OK, "{}", constant_inf.1);
    assert_eq!(constant_inf.1["data"]["result"][0]["value"][1], "NaN");
    let singleton = prom_query(
        &app,
        "predict_linear(range_predict{case=\"singleton\"}[60s], 10)",
        base + 60,
    )
    .await;
    assert_eq!(singleton.0, StatusCode::OK, "{}", singleton.1);
    assert_eq!(singleton.1["data"]["result"], serde_json::json!([]));
    let range = prom_query_range(
        &app,
        "predict_linear(range_predict{case=\"steady\"}[40s], 10)",
        base + 50,
        base + 60,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "steady"},
            "values": [[base + 50, "600"], [base + 60, "700"]]
        }])
    );
    for query in [
        "predict_linear(range_predict, 10)",
        "predict_linear(range_predict[60s])",
        "predict_linear(range_predict[60s], range_predict)",
    ] {
        let invalid = prom_query(&app, query, base + 60).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "predict_linear(range_predict{case=\"steady\"}[60s], 10)",
        base + 60,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "predict_linear(range_predict{case=\"steady\"}[60s], 10)",
            base + 60,
        )
        .await
        .1,
        steady.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_seven_promql_changes_counts_float_transitions() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_seven_changes.db");
    let base = 1_700_770_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_changes{{case=\"steady\"}} 100 {}\n",
            "range_changes{{case=\"steady\"}} 300 {}\n",
            "range_changes{{case=\"steady\"}} 500 {}\n",
            "range_changes{{case=\"repeated\"}} 1 {}\n",
            "range_changes{{case=\"repeated\"}} 1 {}\n",
            "range_changes{{case=\"repeated\"}} 2 {}\n",
            "range_changes{{case=\"repeated\"}} 2 {}\n",
            "range_changes{{case=\"repeated\"}} 1 {}\n",
            "range_changes{{case=\"constant\"}} 7 {}\n",
            "range_changes{{case=\"constant\"}} 7 {}\n",
            "range_changes{{case=\"singleton\"}} 5 {}\n",
            "range_changes{{case=\"nan\"}} NaN {}\n",
            "range_changes{{case=\"nan\"}} 2 {}\n",
            "range_changes{{case=\"nan_repeat\"}} NaN {}\n",
            "range_changes{{case=\"nan_repeat\"}} NaN {}\n",
            "range_changes{{case=\"zero_sign\"}} 0 {}\n",
            "range_changes{{case=\"zero_sign\"}} -0 {}\n",
            "range_changes{{case=\"infinity\"}} 1 {}\n",
            "range_changes{{case=\"infinity\"}} +Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 40) * 1_000,
        (base + 50) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 50) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let steady = prom_query(
        &app,
        "changes(range_changes{case=\"steady\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(steady.0, StatusCode::OK, "{}", steady.1);
    assert_eq!(
        steady.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "steady"}, "value": [base + 60, "2"]}])
    );
    for (case, expected) in [
        ("repeated", "2"),
        ("constant", "0"),
        ("singleton", "0"),
        ("nan", "1"),
        ("nan_repeat", "0"),
        ("zero_sign", "0"),
        ("infinity", "1"),
    ] {
        let response = prom_query(
            &app,
            &format!("changes(range_changes{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
        assert_eq!(
            response.1["data"]["result"][0]["metric"]["__name__"],
            Value::Null
        );
    }
    let boundary = prom_query(
        &app,
        "changes(range_changes{case=\"steady\"}[40s])",
        base + 50,
    )
    .await;
    assert_eq!(boundary.0, StatusCode::OK, "{}", boundary.1);
    assert_eq!(boundary.1["data"]["result"][0]["value"][1], "1");
    let offset = prom_query(
        &app,
        "changes(range_changes{case=\"steady\"}[40s] offset 10s)",
        base + 60,
    )
    .await;
    assert_eq!(offset.0, StatusCode::OK, "{}", offset.1);
    assert_eq!(offset.1["data"]["result"][0]["value"][1], "1");
    let subquery = prom_query(
        &app,
        "changes(range_changes{case=\"steady\"}[40s:10s])",
        base + 60,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1["data"]["result"][0]["value"][1], "1");
    let range = prom_query_range(
        &app,
        "changes(range_changes{case=\"steady\"}[40s])",
        base + 50,
        base + 60,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "steady"},
            "values": [[base + 50, "1"], [base + 60, "1"]]
        }])
    );
    let invalid = prom_query(&app, "changes(range_changes)", base + 60).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "changes(range_changes{case=\"steady\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "changes(range_changes{case=\"steady\"}[60s])",
            base + 60,
        )
        .await
        .1,
        steady.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_seven_promql_resets_counts_strict_float_decreases() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_seven_resets.db");
    let base = 1_700_780_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "range_resets{{case=\"steady\"}} 100 {}\n",
            "range_resets{{case=\"steady\"}} 300 {}\n",
            "range_resets{{case=\"steady\"}} 500 {}\n",
            "range_resets{{case=\"reset\"}} 100 {}\n",
            "range_resets{{case=\"reset\"}} 150 {}\n",
            "range_resets{{case=\"reset\"}} 20 {}\n",
            "range_resets{{case=\"repeated\"}} 1 {}\n",
            "range_resets{{case=\"repeated\"}} 1 {}\n",
            "range_resets{{case=\"repeated\"}} 2 {}\n",
            "range_resets{{case=\"repeated\"}} 2 {}\n",
            "range_resets{{case=\"repeated\"}} 1 {}\n",
            "range_resets{{case=\"singleton\"}} 5 {}\n",
            "range_resets{{case=\"nan\"}} NaN {}\n",
            "range_resets{{case=\"nan\"}} 2 {}\n",
            "range_resets{{case=\"zero_sign\"}} 0 {}\n",
            "range_resets{{case=\"zero_sign\"}} -0 {}\n",
            "range_resets{{case=\"pos_inf\"}} 1 {}\n",
            "range_resets{{case=\"pos_inf\"}} +Inf {}\n",
            "range_resets{{case=\"inf_drop\"}} +Inf {}\n",
            "range_resets{{case=\"inf_drop\"}} 1 {}\n",
            "range_resets{{case=\"neg_inf\"}} 1 {}\n",
            "range_resets{{case=\"neg_inf\"}} -Inf {}\n"
        ),
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 30) * 1_000,
        (base + 50) * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 40) * 1_000,
        (base + 50) * 1_000,
        (base + 50) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
        (base + 20) * 1_000,
        (base + 40) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let reset = prom_query(&app, "resets(range_resets{case=\"reset\"}[60s])", base + 60).await;
    assert_eq!(reset.0, StatusCode::OK, "{}", reset.1);
    assert_eq!(
        reset.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "reset"}, "value": [base + 60, "1"]}])
    );
    for (case, expected) in [
        ("steady", "0"),
        ("repeated", "1"),
        ("singleton", "0"),
        ("nan", "0"),
        ("zero_sign", "0"),
        ("pos_inf", "0"),
        ("inf_drop", "1"),
        ("neg_inf", "1"),
    ] {
        let response = prom_query(
            &app,
            &format!("resets(range_resets{{case=\"{case}\"}}[60s])"),
            base + 60,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
        assert_eq!(
            response.1["data"]["result"][0]["metric"]["__name__"],
            Value::Null
        );
    }
    let boundary = prom_query(&app, "resets(range_resets{case=\"reset\"}[40s])", base + 50).await;
    assert_eq!(boundary.0, StatusCode::OK, "{}", boundary.1);
    assert_eq!(boundary.1["data"]["result"][0]["value"][1], "1");
    let offset = prom_query(
        &app,
        "resets(range_resets{case=\"reset\"}[40s] offset 10s)",
        base + 60,
    )
    .await;
    assert_eq!(offset.0, StatusCode::OK, "{}", offset.1);
    assert_eq!(offset.1, reset.1);
    let subquery = prom_query(
        &app,
        "resets(range_resets{case=\"reset\"}[40s:10s])",
        base + 60,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(subquery.1["data"]["result"][0]["value"][1], "1");
    let range = prom_query_range(
        &app,
        "resets(range_resets{case=\"reset\"}[40s])",
        base + 50,
        base + 60,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "reset"},
            "values": [[base + 50, "1"], [base + 60, "1"]]
        }])
    );
    let invalid = prom_query(&app, "resets(range_resets)", base + 60).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "resets(range_resets{case=\"reset\"}[60s])",
        base + 60,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 1 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "resets(range_resets{case=\"reset\"}[60s])",
            base + 60,
        )
        .await
        .1,
        reset.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_abs_transforms_float_vectors_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_abs.db");
    let base = 1_700_800_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "transform_abs{{case=\"positive\"}} 2 {}\n",
            "transform_abs{{case=\"negative\"}} -3 {}\n",
            "transform_abs{{case=\"negative\"}} -4 {}\n",
            "transform_abs{{case=\"negative_zero\"}} -0 {}\n",
            "transform_abs{{case=\"nan\"}} NaN {}\n",
            "transform_abs{{case=\"positive_inf\"}} +Inf {}\n",
            "transform_abs{{case=\"negative_inf\"}} -Inf {}\n"
        ),
        base * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (case, expected) in [
        ("positive", "2"),
        ("negative", "3"),
        ("negative_zero", "0"),
        ("nan", "NaN"),
        ("positive_inf", "+Inf"),
        ("negative_inf", "+Inf"),
    ] {
        let response = prom_query(
            &app,
            &format!("abs(transform_abs{{case=\"{case}\"}})"),
            base,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["resultType"], "vector");
        assert_eq!(
            response.1["data"]["result"][0]["metric"],
            serde_json::json!({"case": case})
        );
        assert_eq!(
            response.1["data"]["result"][0]["value"],
            serde_json::json!([base, expected])
        );
    }

    let range = prom_query_range(
        &app,
        "abs(transform_abs{case=\"negative\"})",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "negative"},
            "values": [[base, "3"], [base + 10, "4"]]
        }])
    );

    let nested = prom_query(
        &app,
        "avg_over_time(abs(transform_abs{case=\"negative\"})[20s:10s])",
        base + 10,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(
        nested.1["data"]["result"],
        serde_json::json!([{"metric": {"case": "negative"}, "value": [base + 10, "3.5"]}])
    );

    for query in ["abs(1)", "abs(transform_abs[1m])"] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "abs(transform_abs)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("work point limit 1 exceeded"));

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let reopened_result =
        prom_query(&reopened_app, "abs(transform_abs{case=\"negative\"})", base).await;
    assert_eq!(reopened_result.0, StatusCode::OK, "{}", reopened_result.1);
    assert_eq!(reopened_result.1["data"]["result"][0]["value"][1], "3");
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_rounding_functions_match_float_and_step_semantics() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_rounding.db");
    let base = 1_700_810_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "transform_round{{case=\"negative\"}} -1.6 {}\n",
            "transform_round{{case=\"negative\"}} -2.6 {}\n",
            "transform_round{{case=\"negative_tie\"}} -1.5 {}\n",
            "transform_round{{case=\"negative_zero\"}} -0 {}\n",
            "transform_round{{case=\"positive\"}} 1.6 {}\n",
            "transform_round{{case=\"positive_tie\"}} 1.5 {}\n",
            "transform_round{{case=\"nan\"}} NaN {}\n",
            "transform_round{{case=\"positive_inf\"}} +Inf {}\n",
            "transform_round{{case=\"negative_inf\"}} -Inf {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (function, case, expected) in [
        ("ceil", "negative", "-1"),
        ("floor", "negative", "-2"),
        ("round", "negative", "-2"),
        ("round", "negative_tie", "-1"),
        ("ceil", "positive", "2"),
        ("floor", "positive", "1"),
        ("round", "positive", "2"),
        ("round", "positive_tie", "2"),
        ("ceil", "negative_zero", "-0"),
        ("floor", "negative_zero", "-0"),
        ("round", "negative_zero", "0"),
        ("ceil", "nan", "NaN"),
        ("floor", "positive_inf", "+Inf"),
        ("round", "negative_inf", "-Inf"),
    ] {
        let response = prom_query(
            &app,
            &format!("{function}(transform_round{{case=\"{case}\"}})"),
            base,
        )
        .await;
        assert_eq!(
            response.0,
            StatusCode::OK,
            "{function}/{case}: {}",
            response.1
        );
        assert_eq!(
            response.1["data"]["result"][0]["metric"],
            serde_json::json!({"case": case})
        );
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }

    for (case, expected) in [("negative", "-1.5"), ("positive", "1.5")] {
        let response = prom_query(
            &app,
            &format!("round(transform_round{{case=\"{case}\"}}, 1 / 2)"),
            base,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{case}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }
    for (step, expected) in [
        ("0", "NaN"),
        ("-0", "NaN"),
        ("NaN", "NaN"),
        ("Inf", "NaN"),
        ("-0.5", "1.5"),
    ] {
        let response = prom_query(
            &app,
            &format!("round(transform_round{{case=\"positive\"}}, {step})"),
            base,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{step}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }

    let range = prom_query_range(
        &app,
        "ceil(transform_round{case=\"negative\"})",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "negative"},
            "values": [[base, "-1"], [base + 10, "-2"]]
        }])
    );
    let nested = prom_query(
        &app,
        "round(abs(transform_round{case=\"negative\"}), 0.5)",
        base,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["value"][1], "1.5");

    for query in [
        "ceil(1)",
        "floor(transform_round[1m])",
        "round(transform_round, transform_round)",
    ] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "round(transform_round, 0.5)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let reopened_result = prom_query(
        &reopened_app,
        "round(transform_round{case=\"negative_tie\"})",
        base,
    )
    .await;
    assert_eq!(reopened_result.0, StatusCode::OK, "{}", reopened_result.1);
    assert_eq!(reopened_result.1["data"]["result"][0]["value"][1], "-1");
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_clamp_functions_bound_ieee_vectors_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_clamp.db");
    let base = 1_700_820_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "transform_clamp{{case=\"below\"}} -2 {}\n",
            "transform_clamp{{case=\"below\"}} -3 {}\n",
            "transform_clamp{{case=\"inside\"}} 2 {}\n",
            "transform_clamp{{case=\"above\"}} 8 {}\n",
            "transform_clamp{{case=\"negative_zero\"}} -0 {}\n",
            "transform_clamp{{case=\"positive_zero\"}} 0 {}\n",
            "transform_clamp{{case=\"nan\"}} NaN {}\n",
            "transform_clamp{{case=\"positive_inf\"}} +Inf {}\n",
            "transform_clamp{{case=\"negative_inf\"}} -Inf {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (function, case, expected) in [
        ("clamp", "below", "0"),
        ("clamp", "inside", "2"),
        ("clamp", "above", "5"),
        ("clamp", "negative_zero", "0"),
        ("clamp", "nan", "NaN"),
        ("clamp", "positive_inf", "5"),
        ("clamp", "negative_inf", "0"),
        ("clamp_min", "below", "0"),
        ("clamp_min", "negative_zero", "0"),
        ("clamp_min", "positive_inf", "+Inf"),
        ("clamp_max", "above", "5"),
        ("clamp_max", "negative_inf", "-Inf"),
    ] {
        let expression = if function == "clamp" {
            format!("clamp(transform_clamp{{case=\"{case}\"}}, 0, 5)")
        } else if function == "clamp_min" {
            format!("clamp_min(transform_clamp{{case=\"{case}\"}}, 0)")
        } else {
            format!("clamp_max(transform_clamp{{case=\"{case}\"}}, 5)")
        };
        let response = prom_query(&app, &expression, base).await;
        assert_eq!(response.0, StatusCode::OK, "{expression}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"][0]["metric"],
            serde_json::json!({"case": case})
        );
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }

    let negative_zero = prom_query(
        &app,
        "clamp_max(transform_clamp{case=\"positive_zero\"}, -0)",
        base,
    )
    .await;
    assert_eq!(negative_zero.0, StatusCode::OK, "{}", negative_zero.1);
    assert_eq!(negative_zero.1["data"]["result"][0]["value"][1], "-0");

    let empty = prom_query(&app, "clamp(transform_clamp, 5, 0)", base).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    let range = prom_query_range(
        &app,
        "clamp(transform_clamp{case=\"below\"}, -2.5, -1)",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "below"},
            "values": [[base, "-2"], [base + 10, "-2.5"]]
        }])
    );
    let nested = prom_query(
        &app,
        "clamp_max(abs(transform_clamp{case=\"below\"}), 1 + 1)",
        base,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["value"][1], "2");

    for query in [
        "clamp(1, 0, 5)",
        "clamp_min(transform_clamp[1m], 0)",
        "clamp_max(transform_clamp, transform_clamp)",
    ] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 2,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "clamp(transform_clamp, 0, 5)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let reopened_result = prom_query(
        &reopened_app,
        "clamp(transform_clamp{case=\"above\"}, 0, 5)",
        base,
    )
    .await;
    assert_eq!(reopened_result.0, StatusCode::OK, "{}", reopened_result.1);
    assert_eq!(reopened_result.1["data"]["result"][0]["value"][1], "5");
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_math_transforms_match_domains_ieee_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_math.db");
    let base = 1_700_830_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "transform_math{{case=\"sqrt\"}} 4 {}\n",
            "transform_math{{case=\"sqrt\"}} 9 {}\n",
            "transform_math{{case=\"zero\"}} 0 {}\n",
            "transform_math{{case=\"negative_zero\"}} -0 {}\n",
            "transform_math{{case=\"one\"}} 1 {}\n",
            "transform_math{{case=\"eight\"}} 8 {}\n",
            "transform_math{{case=\"hundred\"}} 100 {}\n",
            "transform_math{{case=\"negative\"}} -4 {}\n",
            "transform_math{{case=\"nan\"}} NaN {}\n",
            "transform_math{{case=\"positive_inf\"}} +Inf {}\n",
            "transform_math{{case=\"negative_inf\"}} -Inf {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (expression, case, expected) in [
        ("sqrt(transform_math{case=\"sqrt\"})", "sqrt", "2"),
        ("exp(transform_math{case=\"zero\"})", "zero", "1"),
        ("ln(transform_math{case=\"one\"})", "one", "0"),
        ("log2(transform_math{case=\"eight\"})", "eight", "3"),
        ("log10(transform_math{case=\"hundred\"})", "hundred", "2"),
        ("sqrt(transform_math{case=\"negative\"})", "negative", "NaN"),
        (
            "sqrt(transform_math{case=\"negative_zero\"})",
            "negative_zero",
            "-0",
        ),
        (
            "sqrt(transform_math{case=\"positive_inf\"})",
            "positive_inf",
            "+Inf",
        ),
        (
            "sqrt(transform_math{case=\"negative_inf\"})",
            "negative_inf",
            "NaN",
        ),
        (
            "exp(transform_math{case=\"negative_inf\"})",
            "negative_inf",
            "0",
        ),
        ("ln(transform_math{case=\"zero\"})", "zero", "-Inf"),
        (
            "ln(transform_math{case=\"negative_zero\"})",
            "negative_zero",
            "-Inf",
        ),
        ("log2(transform_math{case=\"negative\"})", "negative", "NaN"),
        ("log10(transform_math{case=\"nan\"})", "nan", "NaN"),
    ] {
        let response = prom_query(&app, expression, base).await;
        assert_eq!(response.0, StatusCode::OK, "{expression}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"][0]["metric"],
            serde_json::json!({"case": case})
        );
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }

    let range = prom_query_range(
        &app,
        "sqrt(transform_math{case=\"sqrt\"})",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "sqrt"},
            "values": [[base, "2"], [base + 10, "3"]]
        }])
    );
    let nested = prom_query(&app, "ln(exp(transform_math{case=\"zero\"}))", base).await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["value"][1], "0");

    for query in [
        "sqrt(1)",
        "exp(transform_math[1m])",
        "ln(transform_math, 1)",
    ] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "sqrt(transform_math)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let reopened_result = prom_query(
        &reopened_app,
        "log10(transform_math{case=\"hundred\"})",
        base,
    )
    .await;
    assert_eq!(reopened_result.0, StatusCode::OK, "{}", reopened_result.1);
    assert_eq!(reopened_result.1["data"]["result"][0]["value"][1], "2");
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_sgn_preserves_ieee_signs_ranges_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_sgn.db");
    let base = 1_700_840_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "transform_sgn{{case=\"negative\"}} -2 {}\n",
            "transform_sgn{{case=\"negative\"}} -3 {}\n",
            "transform_sgn{{case=\"positive\"}} 2 {}\n",
            "transform_sgn{{case=\"negative_zero\"}} -0 {}\n",
            "transform_sgn{{case=\"positive_zero\"}} 0 {}\n",
            "transform_sgn{{case=\"nan\"}} NaN {}\n",
            "transform_sgn{{case=\"positive_inf\"}} +Inf {}\n",
            "transform_sgn{{case=\"negative_inf\"}} -Inf {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (case, expected) in [
        ("negative", "-1"),
        ("positive", "1"),
        ("negative_zero", "-0"),
        ("positive_zero", "0"),
        ("nan", "NaN"),
        ("positive_inf", "1"),
        ("negative_inf", "-1"),
    ] {
        let expression = format!("sgn(transform_sgn{{case=\"{case}\"}})");
        let response = prom_query(&app, &expression, base).await;
        assert_eq!(response.0, StatusCode::OK, "{expression}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"][0]["metric"],
            serde_json::json!({"case": case})
        );
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }

    let range = prom_query_range(
        &app,
        "sgn(transform_sgn{case=\"negative\"})",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "negative"},
            "values": [[base, "-1"], [base + 10, "-1"]]
        }])
    );
    let nested = prom_query(&app, "abs(sgn(transform_sgn{case=\"negative\"}))", base).await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["value"][1], "1");

    for query in ["sgn(1)", "sgn(transform_sgn[1m])", "sgn(transform_sgn, 1)"] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "sgn(transform_sgn)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let reopened_result = prom_query(
        &reopened_app,
        "sgn(transform_sgn{case=\"negative_inf\"})",
        base,
    )
    .await;
    assert_eq!(reopened_result.0, StatusCode::OK, "{}", reopened_result.1);
    assert_eq!(reopened_result.1["data"]["result"][0]["value"][1], "-1");
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_inverse_transforms_pin_domains_ieee_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_inverse.db");
    let base = 1_700_850_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "transform_inverse{{case=\"range\"}} 0 {}\n",
            "transform_inverse{{case=\"range\"}} 1 {}\n",
            "transform_inverse{{case=\"negative_one\"}} -1 {}\n",
            "transform_inverse{{case=\"negative_zero\"}} -0 {}\n",
            "transform_inverse{{case=\"zero\"}} 0 {}\n",
            "transform_inverse{{case=\"half\"}} 0.5 {}\n",
            "transform_inverse{{case=\"one\"}} 1 {}\n",
            "transform_inverse{{case=\"two\"}} 2 {}\n",
            "transform_inverse{{case=\"nan\"}} NaN {}\n",
            "transform_inverse{{case=\"positive_inf\"}} +Inf {}\n",
            "transform_inverse{{case=\"negative_inf\"}} -Inf {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (expression, case, expected) in [
        ("acos(transform_inverse{case=\"one\"})", "one", "0"),
        ("acosh(transform_inverse{case=\"one\"})", "one", "0"),
        (
            "asin(transform_inverse{case=\"one\"})",
            "one",
            "1.5707963267948966",
        ),
        (
            "asinh(transform_inverse{case=\"negative_zero\"})",
            "negative_zero",
            "-0",
        ),
        (
            "atan(transform_inverse{case=\"negative_inf\"})",
            "negative_inf",
            "-1.5707963267948966",
        ),
        ("atanh(transform_inverse{case=\"one\"})", "one", "+Inf"),
        ("acos(transform_inverse{case=\"two\"})", "two", "NaN"),
        ("acosh(transform_inverse{case=\"zero\"})", "zero", "NaN"),
        ("asin(transform_inverse{case=\"two\"})", "two", "NaN"),
        ("atanh(transform_inverse{case=\"two\"})", "two", "NaN"),
        (
            "asinh(transform_inverse{case=\"positive_inf\"})",
            "positive_inf",
            "+Inf",
        ),
        ("atan(transform_inverse{case=\"nan\"})", "nan", "NaN"),
    ] {
        let response = prom_query(&app, expression, base).await;
        assert_eq!(response.0, StatusCode::OK, "{expression}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"][0]["metric"],
            serde_json::json!({"case": case})
        );
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }

    let range = prom_query_range(
        &app,
        "asin(transform_inverse{case=\"range\"})",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "range"},
            "values": [[base, "0"], [base + 10, "1.5707963267948966"]]
        }])
    );
    let nested = prom_query(
        &app,
        "abs(asin(transform_inverse{case=\"negative_zero\"}))",
        base,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["value"][1], "0");

    for query in [
        "acos(1)",
        "asinh(transform_inverse[1m])",
        "atan(transform_inverse, 1)",
    ] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "atan(transform_inverse)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let reopened_result = prom_query(
        &reopened_app,
        "acosh(transform_inverse{case=\"one\"})",
        base,
    )
    .await;
    assert_eq!(reopened_result.0, StatusCode::OK, "{}", reopened_result.1);
    assert_eq!(reopened_result.1["data"]["result"][0]["value"][1], "0");
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_trig_transforms_pin_ieee_ranges_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_trig.db");
    let base = 1_700_860_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "transform_trig{{case=\"range\"}} 0 {}\n",
            "transform_trig{{case=\"range\"}} 1.5707963267948966 {}\n",
            "transform_trig{{case=\"negative_zero\"}} -0 {}\n",
            "transform_trig{{case=\"zero\"}} 0 {}\n",
            "transform_trig{{case=\"one\"}} 1 {}\n",
            "transform_trig{{case=\"nan\"}} NaN {}\n",
            "transform_trig{{case=\"positive_inf\"}} +Inf {}\n",
            "transform_trig{{case=\"negative_inf\"}} -Inf {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (expression, case, expected) in [
        ("cos(transform_trig{case=\"zero\"})", "zero", "1"),
        (
            "cosh(transform_trig{case=\"negative_zero\"})",
            "negative_zero",
            "1",
        ),
        (
            "sin(transform_trig{case=\"negative_zero\"})",
            "negative_zero",
            "-0",
        ),
        (
            "sinh(transform_trig{case=\"negative_zero\"})",
            "negative_zero",
            "-0",
        ),
        (
            "tan(transform_trig{case=\"negative_zero\"})",
            "negative_zero",
            "-0",
        ),
        (
            "tanh(transform_trig{case=\"positive_inf\"})",
            "positive_inf",
            "1",
        ),
        (
            "tanh(transform_trig{case=\"negative_inf\"})",
            "negative_inf",
            "-1",
        ),
        (
            "cos(transform_trig{case=\"positive_inf\"})",
            "positive_inf",
            "NaN",
        ),
        (
            "sin(transform_trig{case=\"negative_inf\"})",
            "negative_inf",
            "NaN",
        ),
        (
            "tan(transform_trig{case=\"positive_inf\"})",
            "positive_inf",
            "NaN",
        ),
        (
            "cosh(transform_trig{case=\"positive_inf\"})",
            "positive_inf",
            "+Inf",
        ),
        (
            "sinh(transform_trig{case=\"negative_inf\"})",
            "negative_inf",
            "-Inf",
        ),
        ("tanh(transform_trig{case=\"nan\"})", "nan", "NaN"),
    ] {
        let response = prom_query(&app, expression, base).await;
        assert_eq!(response.0, StatusCode::OK, "{expression}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"][0]["metric"],
            serde_json::json!({"case": case})
        );
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }

    let range = prom_query_range(
        &app,
        "sin(transform_trig{case=\"range\"})",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "range"},
            "values": [[base, "0"], [base + 10, "1"]]
        }])
    );
    let nested = prom_query(
        &app,
        "abs(sin(transform_trig{case=\"negative_zero\"}))",
        base,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["value"][1], "0");

    for query in [
        "cos(1)",
        "sinh(transform_trig[1m])",
        "tanh(transform_trig, 1)",
    ] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "sin(transform_trig)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let reopened_result = prom_query(
        &reopened_app,
        "tanh(transform_trig{case=\"positive_inf\"})",
        base,
    )
    .await;
    assert_eq!(reopened_result.0, StatusCode::OK, "{}", reopened_result.1);
    assert_eq!(reopened_result.1["data"]["result"][0]["value"][1], "1");
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_angle_transforms_and_pi_preserve_types_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_angles.db");
    let base = 1_700_870_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "transform_angle{{case=\"range\"}} 0 {}\n",
            "transform_angle{{case=\"range\"}} 3.141592653589793 {}\n",
            "transform_angle{{case=\"negative_zero\"}} -0 {}\n",
            "transform_angle{{case=\"degrees\"}} 180 {}\n",
            "transform_angle{{case=\"nan\"}} NaN {}\n",
            "transform_angle{{case=\"positive_inf\"}} +Inf {}\n",
            "transform_angle{{case=\"negative_inf\"}} -Inf {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (expression, case, expected) in [
        ("deg(transform_angle{case=\"range\"})", "range", "0"),
        (
            "rad(transform_angle{case=\"degrees\"})",
            "degrees",
            "3.141592653589793",
        ),
        (
            "deg(transform_angle{case=\"negative_zero\"})",
            "negative_zero",
            "-0",
        ),
        ("deg(transform_angle{case=\"nan\"})", "nan", "NaN"),
        (
            "rad(transform_angle{case=\"positive_inf\"})",
            "positive_inf",
            "+Inf",
        ),
        (
            "deg(transform_angle{case=\"negative_inf\"})",
            "negative_inf",
            "-Inf",
        ),
    ] {
        let response = prom_query(&app, expression, base).await;
        assert_eq!(response.0, StatusCode::OK, "{expression}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"][0]["metric"],
            serde_json::json!({"case": case})
        );
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
    }

    let scalar = prom_query(&app, "pi()", base).await;
    assert_eq!(scalar.0, StatusCode::OK, "{}", scalar.1);
    assert_eq!(scalar.1["data"]["resultType"], "scalar");
    assert_eq!(
        scalar.1["data"]["result"],
        serde_json::json!([base, "3.141592653589793"])
    );
    let scalar_range = prom_query_range(&app, "pi()", base, base + 10, 10).await;
    assert_eq!(scalar_range.0, StatusCode::OK, "{}", scalar_range.1);
    assert_eq!(scalar_range.1["data"]["resultType"], "matrix");
    assert_eq!(
        scalar_range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base, "3.141592653589793"], [base + 10, "3.141592653589793"]]
        }])
    );

    let range = prom_query_range(
        &app,
        "deg(transform_angle{case=\"range\"})",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "range"},
            "values": [[base, "0"], [base + 10, "180"]]
        }])
    );
    let nested = prom_query(
        &app,
        "abs(rad(deg(transform_angle{case=\"negative_zero\"})))",
        base,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["value"][1], "0");

    for query in ["deg(1)", "rad(transform_angle[1m])", "pi(1)"] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "deg(transform_angle)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let reopened_result = prom_query(&reopened_app, "pi()", base).await;
    assert_eq!(reopened_result.0, StatusCode::OK, "{}", reopened_result.1);
    assert_eq!(reopened_result.1["data"]["result"][1], "3.141592653589793");
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_label_replace_pins_regex_labels_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_label_replace.db");
    let base = 1_700_880_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "label_replace_metric{{case=\"capture\",service=\"api:west\",zone=\"old\"}} 1 {}\n",
            "label_replace_metric{{case=\"capture\",service=\"api:west\",zone=\"old\"}} 6 {}\n",
            "label_replace_metric{{case=\"missing\"}} 2 {}\n",
            "label_replace_metric{{case=\"empty\",service=\"\"}} 3 {}\n",
            "label_replace_metric{{case=\"unmatched\",service=\"api\"}} 4 {}\n",
            "label_replace_metric{{case=\"named\",service=\"west-api\"}} 5 {}\n",
            "label_replace_metric{{case=\"newline\",service=\"api\\nwest\"}} 7 {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let capture = prom_query(
        &app,
        "label_replace(label_replace_metric{case=\"capture\"}, \"zone\", \"$2-$1\", \"service\", \"([^:]+):([^:]+)\")",
        base,
    )
    .await;
    assert_eq!(capture.0, StatusCode::OK, "{}", capture.1);
    assert_eq!(
        capture.1["data"]["result"],
        serde_json::json!([{
            "metric": {
                "__name__": "label_replace_metric",
                "case": "capture",
                "service": "api:west",
                "zone": "west-api"
            },
            "value": [base, "1"]
        }])
    );

    for (case, source_regex, replacement, expected) in [
        (
            "missing",
            "^$",
            "missing",
            serde_json::json!({
                "__name__": "label_replace_metric", "case": "missing", "zone": "missing"
            }),
        ),
        (
            "empty",
            "^$",
            "empty",
            serde_json::json!({
                "__name__": "label_replace_metric", "case": "empty", "service": "", "zone": "empty"
            }),
        ),
        (
            "unmatched",
            "([^:]+):([^:]+)",
            "$2-$1",
            serde_json::json!({
                "__name__": "label_replace_metric", "case": "unmatched", "service": "api"
            }),
        ),
    ] {
        let expression = format!(
            "label_replace(label_replace_metric{{case=\"{case}\"}}, \"zone\", \"{replacement}\", \"service\", \"{source_regex}\")"
        );
        let response = prom_query(&app, &expression, base).await;
        assert_eq!(response.0, StatusCode::OK, "{expression}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["metric"], expected);
    }

    let deleted = prom_query(
        &app,
        "label_replace(label_replace_metric{case=\"capture\"}, \"zone\", \"\", \"service\", \".*\")",
        base,
    )
    .await;
    assert_eq!(deleted.0, StatusCode::OK, "{}", deleted.1);
    assert_eq!(
        deleted.1["data"]["result"][0]["metric"],
        serde_json::json!({
            "__name__": "label_replace_metric", "case": "capture", "service": "api:west"
        })
    );
    let renamed = prom_query(
        &app,
        "label_replace(label_replace_metric{case=\"capture\"}, \"__name__\", \"renamed_$1\", \"case\", \"(.*)\")",
        base,
    )
    .await;
    assert_eq!(renamed.0, StatusCode::OK, "{}", renamed.1);
    assert_eq!(
        renamed.1["data"]["result"][0]["metric"]["__name__"],
        "renamed_capture"
    );
    let from_name = prom_query(
        &app,
        "label_replace(label_replace_metric{case=\"capture\"}, \"original\", \"${1}_copy\", \"__name__\", \"(.*)\")",
        base,
    )
    .await;
    assert_eq!(from_name.0, StatusCode::OK, "{}", from_name.1);
    assert_eq!(
        from_name.1["data"]["result"][0]["metric"]["original"],
        "label_replace_metric_copy"
    );
    let replacement_escapes = prom_query(
        &app,
        "label_replace(label_replace_metric{case=\"capture\"}, \"expanded\", \"$$$1-${missing}-$\", \"service\", \"(.*)\")",
        base,
    )
    .await;
    assert_eq!(
        replacement_escapes.0,
        StatusCode::OK,
        "{}",
        replacement_escapes.1
    );
    assert_eq!(
        replacement_escapes.1["data"]["result"][0]["metric"]["expanded"],
        "$api:west--$"
    );
    let named = prom_query(
        &app,
        "label_replace(label_replace_metric{case=\"named\"}, \"region\", \"$region\", \"service\", \"(?P<region>[^-]+)-.*\")",
        base,
    )
    .await;
    assert_eq!(named.0, StatusCode::OK, "{}", named.1);
    assert_eq!(named.1["data"]["result"][0]["metric"]["region"], "west");
    let dot_all = prom_query(
        &app,
        "label_replace(label_replace_metric{case=\"newline\"}, \"region\", \"$1\", \"service\", \"api.(.*)\")",
        base,
    )
    .await;
    assert_eq!(dot_all.0, StatusCode::OK, "{}", dot_all.1);
    assert_eq!(dot_all.1["data"]["result"][0]["metric"]["region"], "west");
    let nested = prom_query(
        &app,
        "label_replace(label_replace(label_replace_metric{case=\"capture\"}, \"node\", \"$1\", \"service\", \"(.*)\"), \"region\", \"$1\", \"node\", \"[^:]+:(.*)\")",
        base,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["metric"]["region"], "west");
    let utf8_name = prom_query(
        &app,
        "label_replace(label_replace_metric{case=\"capture\"}, \"bad-name\", \"accepted\", \"service\", \".*\")",
        base,
    )
    .await;
    assert_eq!(utf8_name.0, StatusCode::OK, "{}", utf8_name.1);
    assert_eq!(
        utf8_name.1["data"]["result"][0]["metric"]["bad-name"],
        "accepted"
    );

    let range = prom_query_range(
        &app,
        "label_replace(label_replace_metric{case=\"capture\"}, \"zone\", \"$2-$1\", \"service\", \"([^:]+):([^:]+)\")",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0],
        serde_json::json!({
            "metric": {
                "__name__": "label_replace_metric",
                "case": "capture",
                "service": "api:west",
                "zone": "west-api"
            },
            "values": [[base, "1"], [base + 10, "6"]]
        })
    );

    for query in [
        "label_replace(1, \"zone\", \"x\", \"service\", \".*\")",
        "label_replace(label_replace_metric[1m], \"zone\", \"x\", \"service\", \".*\")",
    ] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }
    for query in [
        "label_replace(label_replace_metric, \"zone\", \"x\", \"service\", \"(\")",
        "label_replace(label_replace_metric, \"\", \"x\", \"service\", \".*\")",
    ] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(
            invalid.0,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{query}: {}",
            invalid.1
        );
        assert_eq!(invalid.1["errorType"], "execution");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "label_replace(label_replace_metric, \"zone\", \"x\", \"service\", \".*\")",
        base,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );

    let response_limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_response_bytes: 256,
            ..PromQueryLimits::default()
        },
    );
    let repeated_captures = "$1".repeat(100);
    let inflated = prom_query(
        &response_limited,
        &format!(
            "label_replace(label_replace_metric{{case=\"capture\"}}, \"expanded\", \"{repeated_captures}\", \"service\", \"(.*)\")"
        ),
        base,
    )
    .await;
    assert_eq!(
        inflated.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        inflated.1
    );
    assert!(
        inflated.1["error"]
            .as_str()
            .unwrap()
            .contains("response-size limit"),
        "{}",
        inflated.1
    );

    drop((response_limited, limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "label_replace(label_replace_metric{case=\"capture\"}, \"zone\", \"$2-$1\", \"service\", \"([^:]+):([^:]+)\")",
            base,
        )
        .await
        .1,
        capture.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_label_join_pins_sources_names_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_label_join.db");
    let base = 1_700_890_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "label_join_metric{{case=\"both\",service=\"api\",zone=\"west\",joined=\"old\"}} 1 {}\n",
            "label_join_metric{{case=\"both\",service=\"api\",zone=\"west\",joined=\"old\"}} 6 {}\n",
            "label_join_metric{{case=\"missing\",service=\"api\"}} 2 {}\n",
            "label_join_metric{{case=\"empty\",service=\"\",zone=\"\"}} 3 {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let joined = prom_query(
        &app,
        "label_join(label_join_metric{case=\"both\"}, \"joined\", \"/\", \"service\", \"zone\")",
        base,
    )
    .await;
    assert_eq!(joined.0, StatusCode::OK, "{}", joined.1);
    assert_eq!(
        joined.1["data"]["result"],
        serde_json::json!([{
            "metric": {
                "__name__": "label_join_metric",
                "case": "both",
                "joined": "api/west",
                "service": "api",
                "zone": "west"
            },
            "value": [base, "1"]
        }])
    );

    for (query, expected) in [
        (
            "label_join(label_join_metric{case=\"missing\"}, \"joined\", \"/\", \"service\", \"zone\")",
            serde_json::json!({
                "__name__": "label_join_metric", "case": "missing", "joined": "api/", "service": "api"
            }),
        ),
        (
            "label_join(label_join_metric{case=\"empty\"}, \"joined\", \"/\", \"service\", \"zone\")",
            serde_json::json!({
                "__name__": "label_join_metric", "case": "empty", "joined": "/", "service": "", "zone": ""
            }),
        ),
        (
            "label_join(label_join_metric{case=\"both\"}, \"joined\", \"/\", \"service\", \"service\")",
            serde_json::json!({
                "__name__": "label_join_metric", "case": "both", "joined": "api/api", "service": "api", "zone": "west"
            }),
        ),
        (
            "label_join(label_join_metric{case=\"both\"}, \"original\", \":\", \"__name__\", \"case\")",
            serde_json::json!({
                "__name__": "label_join_metric", "case": "both", "joined": "old", "original": "label_join_metric:both", "service": "api", "zone": "west"
            }),
        ),
        (
            "label_join(label_join_metric{case=\"both\"}, \"__name__\", \":\", \"case\", \"service\")",
            serde_json::json!({
                "__name__": "both:api", "case": "both", "joined": "old", "service": "api", "zone": "west"
            }),
        ),
        (
            "label_join(label_join_metric{case=\"both\"}, \"zone\", \":\", \"zone\", \"service\")",
            serde_json::json!({
                "__name__": "label_join_metric", "case": "both", "joined": "old", "service": "api", "zone": "west:api"
            }),
        ),
        (
            "label_join(label_join_metric{case=\"both\"}, \"joined\", \"\")",
            serde_json::json!({
                "__name__": "label_join_metric", "case": "both", "service": "api", "zone": "west"
            }),
        ),
        (
            "label_join(label_join_metric{case=\"both\"}, \"bad-name\", \"\", \"service\", \"zone\")",
            serde_json::json!({
                "__name__": "label_join_metric", "bad-name": "apiwest", "case": "both", "joined": "old", "service": "api", "zone": "west"
            }),
        ),
    ] {
        let response = prom_query(&app, query, base).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["metric"], expected);
    }

    let nested = prom_query(
        &app,
        "label_replace(label_join(label_join_metric{case=\"both\"}, \"node\", \"-\", \"service\", \"zone\"), \"region\", \"$1\", \"node\", \"[^-]+-(.*)\")",
        base,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["metric"]["region"], "west");

    let range = prom_query_range(
        &app,
        "label_join(label_join_metric{case=\"both\"}, \"joined\", \"/\", \"service\", \"zone\")",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0],
        serde_json::json!({
            "metric": {
                "__name__": "label_join_metric", "case": "both", "joined": "api/west", "service": "api", "zone": "west"
            },
            "values": [[base, "1"], [base + 10, "6"]]
        })
    );

    for query in [
        "label_join(1, \"joined\", \"/\", \"service\")",
        "label_join(label_join_metric[1m], \"joined\", \"/\", \"service\")",
        "label_join(label_join_metric, \"joined\", 1, \"service\")",
    ] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }
    let invalid = prom_query(
        &app,
        "label_join(label_join_metric, \"\", \"/\", \"service\")",
        base,
    )
    .await;
    assert_eq!(invalid.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "execution");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "label_join(label_join_metric, \"joined\", \"/\", \"service\", \"zone\")",
        base,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );

    let response_limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_response_bytes: 256,
            ..PromQueryLimits::default()
        },
    );
    let sources = (0..100)
        .map(|_| "\"service\"")
        .collect::<Vec<_>>()
        .join(",");
    let inflated = prom_query(
        &response_limited,
        &format!("label_join(label_join_metric{{case=\"both\"}}, \"joined\", \"/\", {sources})"),
        base,
    )
    .await;
    assert_eq!(
        inflated.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        inflated.1
    );
    assert!(
        inflated.1["error"]
            .as_str()
            .unwrap()
            .contains("response-size limit"),
        "{}",
        inflated.1
    );
    assert_eq!(
        prom_query(
            &response_limited,
            "label_join(label_join_metric{case=\"both\"}, \"joined\", \"/\", \"service\", \"zone\")",
            base,
        )
        .await
        .0,
        StatusCode::OK
    );

    drop((response_limited, limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "label_join(label_join_metric{case=\"both\"}, \"joined\", \"/\", \"service\", \"zone\")",
            base,
        )
        .await
        .1,
        joined.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_absent_derives_labels_per_step_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_absent.db");
    let base = 1_700_900_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "absent_metric{{case=\"present\",service=\"api\"}} 1 {}\n",
            "absent_metric{{case=\"late\",service=\"api\"}} NaN {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let present = prom_query(&app, "absent(absent_metric{case=\"present\"})", base).await;
    assert_eq!(present.0, StatusCode::OK, "{}", present.1);
    assert_eq!(present.1["data"]["result"], serde_json::json!([]));

    let missing = prom_query(
        &app,
        "absent(absent_metric{case=\"missing\",service=\"worker\",zone!=\"east\",host=~\"web.*\"})",
        base,
    )
    .await;
    assert_eq!(missing.0, StatusCode::OK, "{}", missing.1);
    assert_eq!(
        missing.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "missing", "service": "worker"},
            "value": [base, "1"]
        }])
    );

    for (query, expected) in [
        (
            "absent({__name__=\"does_not_exist\",service=\"api\"})",
            serde_json::json!({"service": "api"}),
        ),
        (
            "absent(absent_metric{case=~\"missing\"})",
            serde_json::json!({}),
        ),
        (
            "absent({__name__=\"does_not_exist\",service=\"\"})",
            serde_json::json!({}),
        ),
        (
            "absent((absent_metric{case=\"missing\",service=\"api\"}))",
            serde_json::json!({"case": "missing", "service": "api"}),
        ),
        (
            "absent(abs(absent_metric{case=\"missing\"}))",
            serde_json::json!({}),
        ),
        (
            "absent(absent_metric{case=\"missing\",case!=\"other\"})",
            serde_json::json!({}),
        ),
    ] {
        let response = prom_query(&app, query, base).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["metric"], expected);
        assert_eq!(
            response.1["data"]["result"][0]["value"],
            serde_json::json!([base, "1"])
        );
    }

    let nan_present = prom_query(&app, "absent(absent_metric{case=\"late\"})", base + 10).await;
    assert_eq!(nan_present.0, StatusCode::OK, "{}", nan_present.1);
    assert_eq!(nan_present.1["data"]["result"], serde_json::json!([]));

    let sparse = prom_query_range(
        &app,
        "absent(absent_metric{case=\"late\",service=\"api\"})",
        base,
        base + 20,
        10,
    )
    .await;
    assert_eq!(sparse.0, StatusCode::OK, "{}", sparse.1);
    assert_eq!(
        sparse.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "late", "service": "api"},
            "values": [[base, "1"]]
        }])
    );
    let absent_range = prom_query_range(
        &app,
        "absent(absent_metric{case=\"missing\",service=\"worker\"})",
        base,
        base + 20,
        10,
    )
    .await;
    assert_eq!(absent_range.0, StatusCode::OK, "{}", absent_range.1);
    assert_eq!(
        absent_range.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "1"], [base + 10, "1"], [base + 20, "1"]])
    );

    for query in ["absent(1)", "absent(absent_metric[1m])"] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 2,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query_range(
        &limited,
        "absent(absent_metric{case=\"missing\"})",
        base,
        base + 20,
        10,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("intermediate-work limit"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "absent(absent_metric{case=\"missing\",service=\"worker\"})",
            base,
        )
        .await
        .1,
        missing.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_absent_over_time_pins_windows_subqueries_limits_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_absent_over_time.db");
    let base = 1_700_910_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "absent_window{{case=\"boundary\",service=\"api\"}} 9 {}\n",
            "absent_window{{case=\"nan\",service=\"api\"}} NaN {}\n"
        ),
        base * 1_000,
        (base + 20) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let boundary = prom_query(
        &app,
        "absent_over_time(absent_window{case=\"boundary\",service=\"api\"}[10s])",
        base + 10,
    )
    .await;
    assert_eq!(boundary.0, StatusCode::OK, "{}", boundary.1);
    assert_eq!(
        boundary.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "boundary", "service": "api"},
            "value": [base + 10, "1"]
        }])
    );
    let included = prom_query(
        &app,
        "absent_over_time(absent_window{case=\"boundary\"}[10001ms])",
        base + 10,
    )
    .await;
    assert_eq!(included.0, StatusCode::OK, "{}", included.1);
    assert_eq!(included.1["data"]["result"], serde_json::json!([]));

    let missing = prom_query(
        &app,
        "absent_over_time(absent_window{case=\"missing\",service=\"worker\",zone!=\"east\",host=~\"web.*\"}[10s])",
        base + 20,
    )
    .await;
    assert_eq!(missing.0, StatusCode::OK, "{}", missing.1);
    assert_eq!(
        missing.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "missing", "service": "worker"},
            "value": [base + 20, "1"]
        }])
    );
    let nan_present = prom_query(
        &app,
        "absent_over_time(absent_window{case=\"nan\"}[10s])",
        base + 20,
    )
    .await;
    assert_eq!(nan_present.0, StatusCode::OK, "{}", nan_present.1);
    assert_eq!(nan_present.1["data"]["result"], serde_json::json!([]));

    let sparse = prom_query_range(
        &app,
        "absent_over_time(absent_window{case=\"boundary\"}[10s])",
        base,
        base + 30,
        10,
    )
    .await;
    assert_eq!(sparse.0, StatusCode::OK, "{}", sparse.1);
    assert_eq!(
        sparse.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "boundary"},
            "values": [[base + 10, "1"], [base + 20, "1"], [base + 30, "1"]]
        }])
    );

    let subquery = prom_query(
        &app,
        "absent_over_time((abs(absent_window{case=\"missing\"}))[20s:10s])",
        base + 20,
    )
    .await;
    assert_eq!(subquery.0, StatusCode::OK, "{}", subquery.1);
    assert_eq!(
        subquery.1["data"]["result"][0]["metric"],
        serde_json::json!({})
    );

    for query in ["absent_over_time(absent_window)", "absent_over_time(1)"] {
        let invalid = prom_query(&app, query, base + 20).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 2,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query_range(
        &limited,
        "absent_over_time(absent_window{case=\"missing\"}[10s])",
        base,
        base + 20,
        10,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("intermediate-work limit"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "absent_over_time(absent_window{case=\"missing\",service=\"worker\"}[10s])",
            base + 20,
        )
        .await
        .1,
        missing.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_sort_orders_ieee_instants_not_range_matrices_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_sort.db");
    let base = 1_700_920_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "sort_metric{{case=\"negative_inf\"}} -Inf {}\n",
            "sort_metric{{case=\"negative\"}} -4 {}\n",
            "sort_metric{{case=\"negative_zero\"}} -0 {}\n",
            "sort_metric{{case=\"positive\"}} 2 {}\n",
            "sort_metric{{case=\"positive_inf\"}} +Inf {}\n",
            "sort_metric{{case=\"nan\"}} NaN {}\n",
            "sort_range{{host=\"a\"}} 1 {}\n",
            "sort_range{{host=\"a\"}} 10 {}\n",
            "sort_range{{host=\"b\"}} 2 {}\n",
            "sort_range{{host=\"b\"}} 0 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let selector = "sort_metric{case=~\"negative|positive|nan|negative_inf|positive_inf\"}";
    let ascending = prom_query(&app, &format!("sort({selector})"), base).await;
    assert_eq!(ascending.0, StatusCode::OK, "{}", ascending.1);
    assert_eq!(
        ascending.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "sort_metric", "case": "negative_inf"}, "value": [base, "-Inf"]},
            {"metric": {"__name__": "sort_metric", "case": "negative"}, "value": [base, "-4"]},
            {"metric": {"__name__": "sort_metric", "case": "positive"}, "value": [base, "2"]},
            {"metric": {"__name__": "sort_metric", "case": "positive_inf"}, "value": [base, "+Inf"]},
            {"metric": {"__name__": "sort_metric", "case": "nan"}, "value": [base, "NaN"]}
        ])
    );
    let descending = prom_query(&app, &format!("sort_desc({selector})"), base).await;
    assert_eq!(descending.0, StatusCode::OK, "{}", descending.1);
    assert_eq!(
        descending.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "sort_metric", "case": "positive_inf"}, "value": [base, "+Inf"]},
            {"metric": {"__name__": "sort_metric", "case": "positive"}, "value": [base, "2"]},
            {"metric": {"__name__": "sort_metric", "case": "negative"}, "value": [base, "-4"]},
            {"metric": {"__name__": "sort_metric", "case": "negative_inf"}, "value": [base, "-Inf"]},
            {"metric": {"__name__": "sort_metric", "case": "nan"}, "value": [base, "NaN"]}
        ])
    );

    let negative_zero = prom_query(&app, "sort(sort_metric{case=\"negative_zero\"})", base).await;
    assert_eq!(negative_zero.0, StatusCode::OK, "{}", negative_zero.1);
    assert_eq!(negative_zero.1["data"]["result"][0]["value"][1], "-0");
    let nested = prom_query(&app, "sort(abs(sort_metric{case=\"negative\"}))", base).await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(
        nested.1["data"]["result"][0],
        serde_json::json!({"metric": {"case": "negative"}, "value": [base, "4"]})
    );
    let empty = prom_query(&app, "sort(sort_metric{case=\"missing\"})", base).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    let range = prom_query_range(&app, "sort_desc(sort_range)", base, base + 10, 10).await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([
            {"metric": {"__name__": "sort_range", "host": "a"}, "values": [[base, "1"], [base + 10, "10"]]},
            {"metric": {"__name__": "sort_range", "host": "b"}, "values": [[base, "2"], [base + 10, "0"]]}
        ])
    );
    assert_eq!(
        range.1["warnings"],
        serde_json::json!([
            "PromQL warning: sort is ineffective for range queries since results are always ordered by labels (1:1)"
        ])
    );
    let posted_range = post_form(
        &app,
        "/prometheus/api/v1/query_range",
        &form_urlencoded::Serializer::new(String::new())
            .append_pair("query", "sort_desc(sort_range)")
            .append_pair("start", &base.to_string())
            .append_pair("end", &(base + 10).to_string())
            .append_pair("step", "10")
            .finish(),
    )
    .await;
    assert_eq!(posted_range, range);

    for query in ["sort(1)", "sort_desc(sort_metric[1m])"] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }
    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 4,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, &format!("sort({selector})"), base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(&reopened_app, &format!("sort({selector})"), base)
            .await
            .1,
        ascending.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_scalar_vector_convert_types_cardinality_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_scalar_vector.db");
    let base = 1_700_930_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "scalar_metric{{case=\"single\"}} 7 {}\n",
            "scalar_metric{{case=\"single\"}} 8 {}\n",
            "scalar_metric{{case=\"single\"}} 9 {}\n",
            "scalar_metric{{case=\"multi\",host=\"a\"}} 1 {}\n",
            "scalar_metric{{case=\"multi\",host=\"b\"}} 2 {}\n",
            "scalar_metric{{case=\"nan\"}} NaN {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 20) * 1_000,
        (base + 20) * 1_000,
        (base + 20) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let single = prom_query(&app, "scalar(scalar_metric{case=\"single\"})", base + 20).await;
    assert_eq!(single.0, StatusCode::OK, "{}", single.1);
    assert_eq!(
        single.1,
        serde_json::json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [base + 20, "9"]}
        })
    );
    for (query, value) in [
        ("scalar(scalar_metric{case=\"missing\"})", "NaN"),
        ("scalar(scalar_metric{case=\"multi\"})", "NaN"),
        ("scalar(scalar_metric{case=\"nan\"})", "NaN"),
    ] {
        let response = prom_query(&app, query, base + 20).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1["data"]["resultType"], "scalar");
        assert_eq!(
            response.1["data"]["result"],
            serde_json::json!([base + 20, value])
        );
    }

    let scalar_range = prom_query_range(
        &app,
        "scalar(scalar_metric{case=\"single\"})",
        base,
        base + 20,
        10,
    )
    .await;
    assert_eq!(scalar_range.0, StatusCode::OK, "{}", scalar_range.1);
    assert_eq!(
        scalar_range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base, "7"], [base + 10, "8"], [base + 20, "9"]]
        }])
    );

    let vector = prom_query(&app, "vector(2 + 3)", base + 20).await;
    assert_eq!(vector.0, StatusCode::OK, "{}", vector.1);
    assert_eq!(
        vector.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base + 20, "5"]}])
    );
    let vector_range = prom_query_range(&app, "vector(2)", base, base + 20, 10).await;
    assert_eq!(vector_range.0, StatusCode::OK, "{}", vector_range.1);
    assert_eq!(
        vector_range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base, "2"], [base + 10, "2"], [base + 20, "2"]]
        }])
    );
    let nested = prom_query(&app, "scalar(abs(vector(-2)))", base + 20).await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(
        nested.1["data"]["result"],
        serde_json::json!([base + 20, "2"])
    );

    for query in ["scalar(1)", "vector(scalar_metric)"] {
        let invalid = prom_query(&app, query, base + 20).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }
    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 2,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query_range(
        &limited,
        "scalar(scalar_metric{case=\"missing\"})",
        base,
        base + 20,
        10,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("intermediate-work limit"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "scalar(scalar_metric{case=\"single\"})",
            base + 20,
        )
        .await
        .1,
        single.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_time_timestamp_pin_clock_provenance_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_time_timestamp.db");
    let base = 1_700_940_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "timestamp_metric{{case=\"direct\",host=\"a\"}} 7 {}\n",
            "timestamp_metric{{case=\"direct\",host=\"a\"}} 8 {}\n",
            "timestamp_metric{{case=\"direct\",host=\"a\"}} NaN {}\n",
            "timestamp_metric{{case=\"multi\",host=\"a\"}} 1 {}\n",
            "timestamp_metric{{case=\"multi\",host=\"b\"}} 2 {}\n"
        ),
        base * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 20) * 1_000,
        (base + 20) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let clock = prom_query(&app, "time()", base + 15).await;
    assert_eq!(clock.0, StatusCode::OK, "{}", clock.1);
    assert_eq!(
        clock.1,
        serde_json::json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [base + 15, (base + 15).to_string()]}
        })
    );

    let direct = prom_query(
        &app,
        "timestamp(timestamp_metric{case=\"direct\"})",
        base + 15,
    )
    .await;
    assert_eq!(direct.0, StatusCode::OK, "{}", direct.1);
    assert_eq!(
        direct.1["data"]["result"],
        serde_json::json!([{
            "metric": {"case": "direct", "host": "a"},
            "value": [base + 15, (base + 10).to_string()]
        }])
    );
    let offset = prom_query(
        &app,
        "timestamp(timestamp_metric{case=\"direct\"} offset 10s)",
        base + 20,
    )
    .await;
    assert_eq!(offset.0, StatusCode::OK, "{}", offset.1);
    assert_eq!(
        offset.1["data"]["result"][0]["value"][1],
        (base + 10).to_string()
    );
    let anchored = prom_query(
        &app,
        &format!(
            "timestamp(timestamp_metric{{case=\"direct\"}} @ {})",
            base + 10
        ),
        base + 20,
    )
    .await;
    assert_eq!(anchored.0, StatusCode::OK, "{}", anchored.1);
    assert_eq!(
        anchored.1["data"]["result"][0]["value"][1],
        (base + 10).to_string()
    );

    let composed = prom_query(
        &app,
        "timestamp(abs(timestamp_metric{case=\"direct\"}))",
        base + 15,
    )
    .await;
    assert_eq!(composed.0, StatusCode::OK, "{}", composed.1);
    assert_eq!(
        composed.1["data"]["result"][0]["value"][1],
        (base + 15).to_string()
    );
    assert!(composed.1["data"]["result"][0]["metric"]["__name__"].is_null());
    let aggregated = prom_query(
        &app,
        "timestamp(sum(timestamp_metric{case=\"multi\"}))",
        base + 20,
    )
    .await;
    assert_eq!(aggregated.0, StatusCode::OK, "{}", aggregated.1);
    assert_eq!(
        aggregated.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "value": [base + 20, (base + 20).to_string()]
        }])
    );

    let range = prom_query_range(
        &app,
        "timestamp(timestamp_metric{case=\"direct\"})",
        base + 5,
        base + 20,
        5,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([
            [base + 5, base.to_string()],
            [base + 10, (base + 10).to_string()],
            [base + 15, (base + 10).to_string()],
            [base + 20, (base + 20).to_string()]
        ])
    );

    for query in ["time(1)", "timestamp(1)"] {
        let invalid = prom_query(&app, query, base + 20).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }
    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "timestamp(timestamp_metric{case=\"multi\"})",
        base + 20,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("work point limit"));

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "timestamp(timestamp_metric{case=\"direct\"})",
            base + 15,
        )
        .await
        .1,
        direct.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_calendar_part_one_uses_utc_defaults_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_calendar_part_one.db");
    // 2024-01-02 00:04:05 UTC (Tuesday).
    let base = 1_704_153_845_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "calendar_metric{{case=\"date\",host=\"a\"}} 90061.9 {}\n",
            "calendar_metric{{case=\"nan\",host=\"a\"}} NaN {}\n",
            "calendar_metric{{case=\"infinity\",host=\"a\"}} +Inf {}\n",
            "calendar_metric{{case=\"multi\",host=\"a\"}} 1 {}\n",
            "calendar_metric{{case=\"multi\",host=\"b\"}} 2 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let expected = [
        ("minute(calendar_metric{case=\"date\"})", "1"),
        ("hour(calendar_metric{case=\"date\"})", "1"),
        ("day_of_week(calendar_metric{case=\"date\"})", "5"),
        ("day_of_month(calendar_metric{case=\"date\"})", "2"),
        ("hour(vector(-0.1))", "0"),
        ("hour(calendar_metric{case=\"nan\"})", "15"),
        ("day_of_month(calendar_metric{case=\"infinity\"})", "4"),
        ("hour(vector(1e20))", "15"),
        ("day_of_month(vector(-1e20))", "4"),
        ("day_of_month(vector(-9223372036854775808))", "4"),
    ];
    let mut date_response = Value::Null;
    for (query, value) in expected {
        let response = prom_query(&app, query, base).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"][0]["value"][1], value,
            "{query}"
        );
        assert!(response.1["data"]["result"][0]["metric"]["__name__"].is_null());
        if query.starts_with("minute(calendar_metric") {
            assert_eq!(
                response.1["data"]["result"][0]["metric"],
                serde_json::json!({"case": "date", "host": "a"})
            );
            date_response = response.1;
        }
    }

    for (query, value) in [
        ("minute()", "4"),
        ("hour()", "0"),
        ("day_of_week()", "2"),
        ("day_of_month()", "2"),
    ] {
        let response = prom_query(&app, query, base).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"][0]["metric"],
            serde_json::json!({})
        );
        assert_eq!(
            response.1["data"]["result"][0]["value"][1], value,
            "{query}"
        );
    }

    let range = prom_query_range(
        &app,
        "abs(day_of_week(calendar_metric{case=\"date\"}))",
        base,
        base + 10,
        5,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "5"], [base + 5, "5"], [base + 10, "5"]])
    );

    for query in ["minute(1)", "hour(vector(1), vector(2))"] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }
    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "minute(calendar_metric{case=\"multi\"})", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("work point limit"));

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "minute(calendar_metric{case=\"date\"})",
            base,
        )
        .await
        .1,
        date_response
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eight_promql_calendar_part_two_pins_leap_years_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eight_calendar_part_two.db");
    // 2024-01-02 00:04:05 UTC.
    let base = 1_704_153_845_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "calendar_two{{case=\"date\",host=\"a\"}} 90061.9 {}\n",
            "calendar_two{{case=\"leap\",host=\"a\"}} 1709208000 {}\n",
            "calendar_two{{case=\"multi\",host=\"a\"}} 1 {}\n",
            "calendar_two{{case=\"multi\",host=\"b\"}} 2 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let expected = [
        ("day_of_year(calendar_two{case=\"date\"})", "2"),
        ("day_of_year(calendar_two{case=\"leap\"})", "60"),
        ("days_in_month(calendar_two{case=\"leap\"})", "29"),
        ("month(calendar_two{case=\"date\"})", "1"),
        ("year(calendar_two{case=\"date\"})", "1970"),
        ("year(vector(-0.1))", "1970"),
        ("day_of_year(vector(NaN))", "339"),
        ("days_in_month(vector(+Inf))", "31"),
        ("month(vector(-Inf))", "12"),
        ("year(vector(-1e20))", "292277026596"),
    ];
    let mut date_response = Value::Null;
    for (query, value) in expected {
        let response = prom_query(&app, query, base).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"][0]["value"][1], value,
            "{query}"
        );
        assert!(response.1["data"]["result"][0]["metric"]["__name__"].is_null());
        if query.starts_with("year(calendar_two{case=\"date\"") {
            assert_eq!(
                response.1["data"]["result"][0]["metric"],
                serde_json::json!({"case": "date", "host": "a"})
            );
            date_response = response.1;
        }
    }

    for (query, value) in [
        ("day_of_year()", "2"),
        ("days_in_month()", "31"),
        ("month()", "1"),
        ("year()", "2024"),
    ] {
        let response = prom_query(&app, query, base).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(
            response.1["data"]["result"][0]["metric"],
            serde_json::json!({})
        );
        assert_eq!(
            response.1["data"]["result"][0]["value"][1], value,
            "{query}"
        );
    }

    let range = prom_query_range(
        &app,
        "abs(month(calendar_two{case=\"date\"}))",
        base,
        base + 10,
        5,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "1"], [base + 5, "1"], [base + 10, "1"]])
    );

    for query in ["year(1)", "month(vector(1), vector(2))"] {
        let invalid = prom_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }
    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "year(calendar_two{case=\"multi\"})", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("work point limit"));

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(&reopened_app, "year(calendar_two{case=\"date\"})", base)
            .await
            .1,
        date_response
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_nine_promql_classic_histogram_quantile_matches_oracle_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_nine_histogram_quantile.db");
    let base = 1_700_900_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let mut fixture = format!(
        concat!(
            "contract_hist_bucket{{host=\"a\",le=\"0.1\"}} 5 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"0.1\"}} 8 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"0.1\"}} 10 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"0.5\"}} 10 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"0.5\"}} 16 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"0.5\"}} 20 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"1\"}} 15 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"1\"}} 24 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"1\"}} 30 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"+Inf\"}} 20 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"+Inf\"}} 32 {}\n",
            "contract_hist_bucket{{host=\"a\",le=\"+Inf\"}} 40 {}\n",
            "contract_hist_bucket{{host=\"b\",le=\"0.1\"}} 3 {}\n",
            "contract_hist_bucket{{host=\"b\",le=\"0.5\"}} 6 {}\n",
            "contract_hist_bucket{{host=\"b\",le=\"1\"}} 9 {}\n",
            "contract_hist_bucket{{host=\"b\",le=\"+Inf\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"missing\",le=\"1\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"missing\",le=\"2\"}} 20 {}\n",
            "contract_hist_special_bucket{{case=\"negative\",le=\"-5\"}} 2 {}\n",
            "contract_hist_special_bucket{{case=\"negative\",le=\"-1\"}} 4 {}\n",
            "contract_hist_special_bucket{{case=\"negative\",le=\"+Inf\"}} 4 {}\n",
            "contract_hist_special_bucket{{case=\"positive\",le=\"10\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"positive\",le=\"+Inf\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"decrease\",le=\"1\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"decrease\",le=\"2\"}} 9 {}\n",
            "contract_hist_special_bucket{{case=\"decrease\",le=\"+Inf\"}} 20 {}\n",
            "contract_hist_special_bucket{{case=\"duplicate\",le=\"1\"}} 3 {}\n",
            "contract_hist_special_bucket{{case=\"duplicate\",le=\"1.0\"}} 4 {}\n",
            "contract_hist_special_bucket{{case=\"duplicate\",le=\"2\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"duplicate\",le=\"+Inf\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"malformed\",le=\"bogus\"}} 9 {}\n",
            "contract_hist_special_bucket{{case=\"malformed\",le=\"1\"}} 5 {}\n",
            "contract_hist_special_bucket{{case=\"malformed\",le=\"+Inf\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"absent\"}} 123 {}\n"
        ),
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 10) * 1_000,
        (base + 20) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
    );
    fixture.push_str(&format!(
        concat!(
            "contract_hist_special_bucket{{case=\"tiny\",le=\"1\"}} 1000000000000 {}\n",
            "contract_hist_special_bucket{{case=\"tiny\",le=\"2\"}} 1000000000000.5 {}\n",
            "contract_hist_special_bucket{{case=\"tiny\",le=\"3\"}} 2000000000000 {}\n",
            "contract_hist_special_bucket{{case=\"tiny\",le=\"+Inf\"}} 3000000000000 {}\n",
            "contract_hist_special_bucket{{case=\"only_inf\",le=\"+Inf\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"zero\",le=\"1\"}} 0 {}\n",
            "contract_hist_special_bucket{{case=\"zero\",le=\"+Inf\"}} 0 {}\n",
            "contract_hist_special_bucket{{case=\"nan_count\",le=\"1\"}} NaN {}\n",
            "contract_hist_special_bucket{{case=\"nan_count\",le=\"2\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"nan_count\",le=\"+Inf\"}} 20 {}\n",
            "contract_hist_special_bucket{{case=\"infinite_total\",le=\"1\"}} 10 {}\n",
            "contract_hist_special_bucket{{case=\"infinite_total\",le=\"+Inf\"}} +Inf {}\n",
            "contract_hist_special_bucket{{case=\"infinite_finite\",le=\"1\"}} +Inf {}\n",
            "contract_hist_special_bucket{{case=\"infinite_finite\",le=\"+Inf\"}} +Inf {}\n",
            "contract_hist_other_bucket{{host=\"a\",le=\"10\"}} 10 {}\n",
            "contract_hist_other_bucket{{host=\"a\",le=\"+Inf\"}} 10 {}\n"
        ),
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
        (base + 30) * 1_000,
    ));
    for index in 0..12 {
        fixture.push_str(&format!(
            "contract_hist_special_bucket{{case=\"many_malformed\",le=\"bad{index:02}\"}} {index} {}\n",
            (base + 30) * 1_000
        ));
    }
    for source in ["a", "b"] {
        fixture.push_str(&format!(
            "contract_hist_special_bucket{{case=\"duplicate_malformed\",le=\"duplicate\",source=\"{source}\"}} 1 {}\n",
            (base + 30) * 1_000
        ));
    }
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let grouped = prom_query(
        &app,
        "histogram_quantile(0.5, contract_hist_bucket)",
        base + 30,
    )
    .await;
    assert_eq!(grouped.0, StatusCode::OK, "{}", grouped.1);
    assert_eq!(
        grouped.1["data"]["result"],
        serde_json::json!([
            {"metric": {"host": "a"}, "value": [base + 30, "0.5"]},
            {"metric": {"host": "b"}, "value": [base + 30, "0.3666666666666667"]}
        ])
    );
    let collision = prom_query(
        &app,
        "histogram_quantile(0.5, {__name__=~\"contract_hist(_other)?_bucket\",host=\"a\"})",
        base + 30,
    )
    .await;
    assert_eq!(
        collision.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        collision.1
    );
    assert_eq!(collision.1["errorType"], "execution");
    assert!(collision.1["error"]
        .as_str()
        .unwrap()
        .contains("vector cannot contain metrics with the same labelset"));

    for (case, quantile, expected) in [
        ("missing", "0.5", "NaN"),
        ("negative", "0.5", "-5"),
        ("positive", "0.5", "5"),
        ("decrease", "0.4", "0.8"),
        ("tiny", "0.5", "2.5"),
        ("duplicate", "0.5", "0.7142857142857143"),
        ("malformed", "0.5", "1"),
        ("only_inf", "0.5", "NaN"),
        ("zero", "0.5", "NaN"),
        ("nan_count", "0.5", "NaN"),
        ("infinite_total", "0.5", "1"),
        ("infinite_finite", "0.5", "NaN"),
    ] {
        let query = format!(
            "histogram_quantile({quantile}, contract_hist_special_bucket{{case=\"{case}\"}})"
        );
        let response = prom_query(&app, &query, base + 30).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
        match case {
            "decrease" => assert_eq!(
                response.1["infos"],
                serde_json::json!([
                    "PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile), from buckets 2 to 2, with a max diff of 1, over 1 samples from 2023-11-25T08:13:50Z to 2023-11-25T08:13:50Z (1:25)"
                ])
            ),
            "malformed" => assert_eq!(
                response.1["warnings"],
                serde_json::json!([
                    "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"bogus\" (1:25)"
                ])
            ),
            _ => {
                assert!(response.1.get("warnings").is_none(), "{query}: {}", response.1);
                assert!(response.1.get("infos").is_none(), "{query}: {}", response.1);
            }
        }
    }
    let absent = prom_query(
        &app,
        "histogram_quantile(0.5, contract_hist_special_bucket{case=\"absent\"})",
        base + 30,
    )
    .await;
    assert_eq!(absent.0, StatusCode::OK, "{}", absent.1);
    assert_eq!(absent.1["data"]["result"], serde_json::json!([]));
    assert_eq!(
        absent.1["warnings"],
        serde_json::json!([
            "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"\" (1:25)"
        ])
    );

    let deduplicated = prom_query(
        &app,
        "histogram_quantile(0.5, contract_hist_special_bucket{case=\"duplicate_malformed\"})",
        base + 30,
    )
    .await;
    assert_eq!(deduplicated.0, StatusCode::OK, "{}", deduplicated.1);
    assert_eq!(deduplicated.1["data"]["result"], serde_json::json!([]));
    assert_eq!(
        deduplicated.1["warnings"],
        serde_json::json!([
            "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"duplicate\" (1:25)"
        ])
    );

    let capped = prom_query(
        &app,
        "histogram_quantile(0.5, contract_hist_special_bucket{case=\"many_malformed\"})",
        base + 30,
    )
    .await;
    assert_eq!(capped.0, StatusCode::OK, "{}", capped.1);
    assert_eq!(capped.1["data"]["result"], serde_json::json!([]));
    let mut expected_capped: Vec<String> = (0..10)
        .map(|index| {
            format!(
                "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"bad{index:02}\" (1:25)"
            )
        })
        .collect();
    expected_capped.push("2 more warning annotations omitted".into());
    assert_eq!(capped.1["warnings"], serde_json::json!(expected_capped));

    let range = prom_query_range(
        &app,
        "histogram_quantile(0.5, contract_hist_bucket{host=\"a\"})",
        base + 10,
        base + 30,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([[base + 10, "0.5"], [base + 20, "0.5"], [base + 30, "0.5"]])
    );
    let composed = prom_query(
        &app,
        "histogram_quantile(0.5, sum by (host, le) (rate(contract_hist_bucket[20s])))",
        base + 30,
    )
    .await;
    assert_eq!(composed.0, StatusCode::OK, "{}", composed.1);
    assert_eq!(
        composed.1["data"]["result"],
        serde_json::json!([{"metric": {"host": "a"}, "value": [base + 30, "0.5"]}])
    );
    let nested = prom_query(
        &app,
        "abs(histogram_quantile(0.5, contract_hist_bucket{host=\"a\"}))",
        base + 30,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["value"][1], "0.5");

    for (quantile, expected) in [("-1", "-Inf"), ("2", "+Inf"), ("NaN", "NaN")] {
        let query = format!("histogram_quantile({quantile}, contract_hist_bucket{{host=\"a\"}})");
        let response = prom_query(&app, &query, base + 30).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
        assert_eq!(
            response.1["warnings"],
            serde_json::json!([format!(
                "PromQL warning: quantile value should be between 0 and 1, got {quantile} (1:20)"
            )])
        );
    }
    let posted_invalid = post_form(
        &app,
        "/prometheus/api/v1/query",
        &form_urlencoded::Serializer::new(String::new())
            .append_pair(
                "query",
                "histogram_quantile(-1, contract_hist_bucket{host=\"a\"})",
            )
            .append_pair("time", &(base + 30).to_string())
            .finish(),
    )
    .await;
    assert_eq!(
        posted_invalid,
        prom_query(
            &app,
            "histogram_quantile(-1, contract_hist_bucket{host=\"a\"})",
            base + 30,
        )
        .await
    );
    for query in [
        "histogram_quantile(vector(0.5), contract_hist_bucket)",
        "histogram_quantile(0.5, 1)",
        "histogram_quantile(0.5)",
    ] {
        let invalid = prom_query(&app, query, base + 30).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "histogram_quantile(0.5, contract_hist_bucket{host=\"a\"})",
        base + 30,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("work point limit"));

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(
            &reopened_app,
            "histogram_quantile(0.5, contract_hist_bucket)",
            base + 30,
        )
        .await
        .1,
        grouped.1
    );
    assert_eq!(
        prom_query(
            &reopened_app,
            "histogram_quantile(0.5, contract_hist_special_bucket{case=\"duplicate_malformed\"})",
            base + 30,
        )
        .await,
        deduplicated
    );
    assert_eq!(
        prom_query(
            &reopened_app,
            "histogram_quantile(0.5, contract_hist_special_bucket{case=\"many_malformed\"})",
            base + 30,
        )
        .await,
        capped
    );
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

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eleven_promql_double_exponential_smoothing_is_explicitly_experimental() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory
        .path()
        .join("session_eleven_double_exponential_smoothing_experimental.db");
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        8,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let query = "double_exponential_smoothing(vector(1)[1m:], 0.5, 0.1)";
    let expected = serde_json::json!({
        "status": "error",
        "errorType": "bad_data",
        "error": "invalid parameter \"query\": double_exponential_smoothing is experimental and is not enabled in the stable PromQL compatibility tier"
    });

    let get = prom_query(&app, query, 2).await;
    assert_eq!(get.0, StatusCode::BAD_REQUEST, "{}", get.1);
    assert_eq!(get.1, expected);
    let post = post_form(
        &app,
        "/api/v1/query",
        "query=double_exponential_smoothing%28vector%281%29%5B1m%3A%5D%2C0.5%2C0.1%29&time=2",
    )
    .await;
    assert_eq!(post.0, StatusCode::BAD_REQUEST, "{}", post.1);
    assert_eq!(post.1, expected);

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(prom_query(&reopened_app, query, 2).await.1, expected);
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_fourteen_prometheus_rejects_metricsql_step_relative_durations() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_fourteen_metricsql_step.db");
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        8,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let query = "count_over_time(oracle_temporal[5i])";
    let expected = serde_json::json!({
        "status": "error",
        "errorType": "bad_data",
        "error": "invalid parameter \"query\": 1:33: parse error: bad number or duration syntax: \"5\""
    });

    let get = prom_query_range(&app, query, 0, 4, 1).await;
    assert_eq!(get.0, StatusCode::BAD_REQUEST, "{}", get.1);
    assert_eq!(get.1, expected);
    let post = post_form(
        &app,
        "/prometheus/api/v1/query_range",
        "query=count_over_time%28oracle_temporal%5B5i%5D%29&start=0&end=4&step=1",
    )
    .await;
    assert_eq!(post.0, StatusCode::BAD_REQUEST, "{}", post.1);
    assert_eq!(post.1, expected);

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query_range(&reopened_app, query, 0, 4, 1).await.1,
        expected
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_fourteen_quoted_utf8_names_ingest_query_catalog_compact_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_fourteen_quoted_utf8.db");
    let base = 1_701_400_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        8,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "{{\"oracle.metric/温度\",job=\"oracle\",\"node.name\"=\"東京\"}} 42 {}\n",
            "{{\"oracle.\\\"quoted\\\"\\\\温度\",job=\"oracle\",\"node.name\"=\"大阪\"}} 43 {}\n",
            "{{\"oracle_temporal\",job=\"oracle\"}} 7 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let cases = [
        (
            r#"{"oracle.metric/温度"}"#,
            serde_json::json!({
                "__name__": "oracle.metric/温度",
                "job": "oracle",
                "node.name": "東京"
            }),
            "42",
        ),
        (
            r#"{job="oracle","oracle.metric/温度","node.name"="東京"}"#,
            serde_json::json!({
                "__name__": "oracle.metric/温度",
                "job": "oracle",
                "node.name": "東京"
            }),
            "42",
        ),
        (
            r#"{"oracle.\"quoted\"\\温度"}"#,
            serde_json::json!({
                "__name__": "oracle.\"quoted\"\\温度",
                "job": "oracle",
                "node.name": "大阪"
            }),
            "43",
        ),
        (
            r#"{"oracle_temporal"}"#,
            serde_json::json!({"__name__": "oracle_temporal", "job": "oracle"}),
            "7",
        ),
    ];
    for (query, labels, value) in &cases {
        let response = prom_query(&app, query, base).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1["data"]["resultType"], "vector");
        assert_eq!(response.1["data"]["result"][0]["metric"], *labels);
        assert_eq!(
            response.1["data"]["result"][0]["value"],
            serde_json::json!([base, value])
        );
    }
    let range = prom_query_range(&app, r#"{"oracle.metric/温度"}"#, base, base, 1).await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "42"]])
    );

    let malformed = prom_query(&app, r#"{"unterminated}"#, base).await;
    assert_eq!(malformed.0, StatusCode::BAD_REQUEST, "{}", malformed.1);
    assert_eq!(malformed.1["errorType"], "bad_data");

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    let conn = open_with_extension(&database, &extension);
    conn.execute(
        "INSERT INTO metric_samples(metric_samples) VALUES ('compact')",
        [],
    )
    .unwrap();
    let catalog = conn
        .prepare(
            "SELECT name, labels, min_ts, max_ts, points
               FROM timeless_series('metric_samples')
              ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(catalog.len(), 3);
    assert_eq!(catalog[0].0, "oracle.\"quoted\"\\温度");
    assert_eq!(
        serde_json::from_str::<Value>(&catalog[0].1).unwrap(),
        serde_json::json!({"job": "oracle", "node.name": "大阪"})
    );
    assert_eq!(catalog[1].0, "oracle.metric/温度");
    assert_eq!(
        serde_json::from_str::<Value>(&catalog[1].1).unwrap(),
        serde_json::json!({"job": "oracle", "node.name": "東京"})
    );
    assert_eq!(catalog[2].0, "oracle_temporal");
    assert_eq!(
        serde_json::from_str::<Value>(&catalog[2].1).unwrap(),
        serde_json::json!({"job": "oracle"})
    );
    for (_, _, min_ts, max_ts, points) in &catalog {
        assert_eq!((*min_ts, *max_ts, *points), (base, base, 1));
    }
    drop(conn);

    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    for (query, labels, value) in &cases {
        let response = prom_query(&reopened_app, query, base).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["metric"], *labels);
        assert_eq!(
            response.1["data"]["result"][0]["value"],
            serde_json::json!([base, value])
        );
    }
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_fourteen_promql_line_comments_match_get_post_range_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_fourteen_line_comments.db");
    let base = 1_701_400_100_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        8,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!("oracle_temporal{{job=\"oracle\"}} 7 {}\n", base * 1_000);
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let selector = "# leading comment\noracle_temporal # trailing comment";
    let selected = prom_query(&app, selector, base).await;
    assert_eq!(selected.0, StatusCode::OK, "{}", selected.1);
    assert_eq!(
        selected.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "oracle_temporal", "job": "oracle"},
            "value": [base, "7"]
        }])
    );

    let binary = prom_query(&app, "oracle_temporal\n# between operands\n+ 1", base).await;
    assert_eq!(binary.0, StatusCode::OK, "{}", binary.1);
    assert_eq!(
        binary.1["data"]["result"],
        serde_json::json!([{"metric": {"job": "oracle"}, "value": [base, "8"]}])
    );

    let commented_call = concat!(
        "sort # rate(fake[5m]) and an unmatched ) are comments\n",
        "(\n",
        "  # sort(fake) is not a call or an argument\n",
        "  oracle_temporal\n",
        ")"
    );
    let sorted = prom_query(&app, commented_call, base).await;
    assert_eq!(sorted.0, StatusCode::OK, "{}", sorted.1);
    assert_eq!(sorted.1, selected.1);

    let hash_in_string = prom_query(&app, r#"oracle_temporal{job=~"oracle#|oracle"}"#, base).await;
    assert_eq!(hash_in_string.0, StatusCode::OK, "{}", hash_in_string.1);
    assert_eq!(hash_in_string.1, selected.1);

    let post_params = form_urlencoded::Serializer::new(String::new())
        .append_pair("query", commented_call)
        .append_pair("time", &base.to_string())
        .finish();
    let posted = post_form(&app, "/prometheus/api/v1/query", &post_params).await;
    assert_eq!(posted.0, StatusCode::OK, "{}", posted.1);
    assert_eq!(posted.1, selected.1);

    let range = prom_query_range(&app, selector, base, base, 1).await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "oracle_temporal", "job": "oracle"},
            "values": [[base, "7"]]
        }])
    );

    let comment_only = prom_query(&app, "# only a comment", base).await;
    assert_eq!(
        comment_only.0,
        StatusCode::BAD_REQUEST,
        "{}",
        comment_only.1
    );
    assert_eq!(
        comment_only.1,
        serde_json::json!({
            "status": "error",
            "errorType": "bad_data",
            "error": "invalid parameter \"query\": unknown position: parse error: no expression found in input"
        })
    );

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(&reopened_app, commented_call, base).await.1,
        selected.1
    );
    assert_eq!(
        prom_query(&reopened_app, "# only a comment", base).await.1,
        comment_only.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_fourteen_histogram_fraction_matches_classic_buckets_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory
        .path()
        .join("session_fourteen_histogram_fraction.db");
    let base = 1_701_400_200_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        32,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let mut fixture = String::new();
    for (bound, count) in [("0.1", 10.0), ("0.5", 20.0), ("1", 30.0), ("+Inf", 40.0)] {
        fixture.push_str(&format!(
            "fraction_bucket{{host=\"a\",le=\"{bound}\"}} {} {}\n",
            count,
            base * 1_000
        ));
        fixture.push_str(&format!(
            "fraction_bucket{{host=\"a\",le=\"{bound}\"}} {} {}\n",
            count * 2.0,
            (base + 10) * 1_000
        ));
    }
    for (bound, count) in [("0.1", 2.0), ("0.5", 4.0), ("1", 6.0), ("+Inf", 8.0)] {
        fixture.push_str(&format!(
            "fraction_other_bucket{{host=\"a\",le=\"{bound}\"}} {count} {}\n",
            (base + 10) * 1_000
        ));
    }
    for (case, buckets) in [
        ("missing_inf", vec![("1", 10.0), ("2", 20.0)]),
        ("zero_total", vec![("1", 0.0), ("+Inf", 0.0)]),
        (
            "duplicate_bound",
            vec![("1", 3.0), ("1.0", 4.0), ("2", 10.0), ("+Inf", 10.0)],
        ),
        (
            "malformed_bound",
            vec![("bogus", 9.0), ("1", 5.0), ("+Inf", 10.0)],
        ),
    ] {
        for (bound, count) in buckets {
            fixture.push_str(&format!(
                "fraction_special_bucket{{case=\"{case}\",le=\"{bound}\"}} {count} {}\n",
                (base + 10) * 1_000
            ));
        }
    }
    fixture.push_str(&format!(
        "fraction_special_bucket{{case=\"absent_bound\"}} 123 {}\n",
        (base + 10) * 1_000
    ));
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (query, expected) in [
        ("histogram_fraction(0.1, 0.5, fraction_bucket)", "0.25"),
        ("histogram_fraction(0.25, 0.75, fraction_bucket)", "0.28125"),
        ("histogram_fraction(-Inf, +Inf, fraction_bucket)", "1"),
        ("histogram_fraction(1, 0.5, fraction_bucket)", "0"),
        ("histogram_fraction(NaN, 1, fraction_bucket)", "NaN"),
        (
            "histogram_fraction(0, 1, fraction_special_bucket{case=\"missing_inf\"})",
            "NaN",
        ),
        (
            "histogram_fraction(0, 1, fraction_special_bucket{case=\"zero_total\"})",
            "NaN",
        ),
        (
            "histogram_fraction(0, 1, fraction_special_bucket{case=\"duplicate_bound\"})",
            "0.7",
        ),
    ] {
        let response = prom_query(&app, query, base + 10).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1["data"]["result"][0]["value"][1], expected);
        assert!(response.1["data"]["result"][0]["metric"]
            .get("__name__")
            .is_none());
    }

    let range = prom_query_range(
        &app,
        "histogram_fraction(0.1, 0.5, fraction_bucket)",
        base,
        base + 10,
        10,
    )
    .await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "a"},
            "values": [[base, "0.25"], [base + 10, "0.25"]]
        }])
    );
    let nested = prom_query(
        &app,
        "avg_over_time(histogram_fraction(0.1, 0.5, fraction_bucket)[10s:10s])",
        base + 10,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(nested.1["data"]["result"][0]["value"][1], "0.25");

    let warning_query = concat!(
        "histogram_fraction(0, 1, ",
        "fraction_special_bucket{case=~\"malformed_bound|absent_bound\"})"
    );
    let warnings = prom_query(&app, warning_query, base + 10).await;
    assert_eq!(warnings.0, StatusCode::OK, "{}", warnings.1);
    assert_eq!(warnings.1["data"]["result"][0]["value"][1], "0.5");
    assert_eq!(
        warnings.1["warnings"],
        serde_json::json!([
            "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"\" (1:26)",
            "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"bogus\" (1:26)"
        ])
    );

    let collision = prom_query(
        &app,
        "histogram_fraction(0, 1, {__name__=~\"fraction(_other)?_bucket\",host=\"a\"})",
        base + 10,
    )
    .await;
    assert_eq!(
        collision.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        collision.1
    );
    assert_eq!(collision.1["errorType"], "execution");
    assert!(collision.1["error"]
        .as_str()
        .unwrap()
        .contains("vector cannot contain metrics with the same labelset"));

    let post_params = form_urlencoded::Serializer::new(String::new())
        .append_pair("query", "histogram_fraction(0.1, 0.5, fraction_bucket)")
        .append_pair("time", &(base + 10).to_string())
        .finish();
    let posted = post_form(&app, "/prometheus/api/v1/query", &post_params).await;
    assert_eq!(posted.0, StatusCode::OK, "{}", posted.1);
    assert_eq!(posted.1["data"]["result"][0]["value"][1], "0.25");

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 5,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(
        &limited,
        "histogram_fraction(0.1, 0.5, fraction_bucket)",
        base + 10,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert_eq!(rejected.1["errorType"], "execution");
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap()
            .contains("work point limit 5 exceeded"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let conn = open_with_extension(&database, &extension);
    conn.execute(
        "INSERT INTO metric_samples(metric_samples) VALUES ('compact')",
        [],
    )
    .unwrap();
    drop(conn);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    let reopened_result = prom_query(
        &reopened_app,
        "histogram_fraction(0.25, 0.75, fraction_bucket)",
        base + 10,
    )
    .await;
    assert_eq!(reopened_result.0, StatusCode::OK, "{}", reopened_result.1);
    assert_eq!(
        reopened_result.1["data"]["result"][0]["value"][1],
        "0.28125"
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_fourteen_experimental_promql_functions_fail_stably_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory
        .path()
        .join("session_fourteen_experimental_functions.db");
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        8,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let cases = [
        ("start()", "function \"start\" is not enabled"),
        ("end()", "function \"end\" is not enabled"),
        (
            "start_timestamp()",
            "unknown function with name \"start_timestamp\"",
        ),
        ("step()", "function \"step\" is not enabled"),
        ("range()", "function \"range\" is not enabled"),
        ("min_of(1, 2)", "function \"min_of\" is not enabled"),
        ("max_of(1, 2)", "function \"max_of\" is not enabled"),
        (
            "histogram_quantiles(vector(1), \"quantile\", 0.5, 0.9)",
            "function \"histogram_quantiles\" is not enabled",
        ),
    ];
    for (query, diagnostic) in cases {
        let response = prom_query(&app, query, 2).await;
        assert_eq!(
            response.0,
            StatusCode::BAD_REQUEST,
            "{query}: {}",
            response.1
        );
        assert_eq!(
            response.1,
            serde_json::json!({
                "status": "error",
                "errorType": "bad_data",
                "error": format!("invalid parameter \"query\": 1:1: parse error: {diagnostic}")
            })
        );
    }

    let at_start = prom_query_range(&app, "missing_metric @ start()", 2, 4, 1).await;
    assert_eq!(at_start.0, StatusCode::OK, "{}", at_start.1);
    assert_eq!(at_start.1["data"]["result"], serde_json::json!([]));
    let at_end = prom_query_range(&app, "missing_metric @ end()", 2, 4, 1).await;
    assert_eq!(at_end.0, StatusCode::OK, "{}", at_end.1);
    assert_eq!(at_end.1["data"]["result"], serde_json::json!([]));

    let post_params = form_urlencoded::Serializer::new(String::new())
        .append_pair(
            "query",
            "histogram_quantiles(vector(1), \"quantile\", 0.5, 0.9)",
        )
        .append_pair("time", "2")
        .finish();
    let posted = post_form(&app, "/prometheus/api/v1/query", &post_params).await;
    assert_eq!(posted.0, StatusCode::BAD_REQUEST, "{}", posted.1);
    assert!(posted.1["error"]
        .as_str()
        .unwrap()
        .contains("function \"histogram_quantiles\" is not enabled"));

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    for (query, diagnostic) in cases {
        let response = prom_query(&reopened_app, query, 2).await;
        assert_eq!(
            response.0,
            StatusCode::BAD_REQUEST,
            "{query}: {}",
            response.1
        );
        assert!(response.1["error"].as_str().unwrap().contains(diagnostic));
    }
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_fifteen_metricsql_default_if_ifnot_match_victoriametrics_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_fifteen_metricsql_binary.db");
    let base = 1_785_901_638_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "mql_cpu{{host=\"a\",zone=\"east\"}} 1 {}\n",
            "mql_cpu{{host=\"a\",zone=\"east\"}} 3 {}\n",
            "mql_cpu{{host=\"b\",zone=\"west\"}} 2 {}\n",
            "mql_cpu{{host=\"b\",zone=\"west\"}} 4 {}\n",
            "mql_sparse{{host=\"a\"}} 10 {}\n",
            "mql_sparse{{host=\"a\"}} 30 {}\n",
            "mql_sparse{{host=\"b\"}} 20 {}\n",
            "mql_disjoint_left{{host=\"c\"}} 1 {}\n",
            "mql_disjoint_right{{host=\"c\"}} 2 {}\n"
        ),
        (base - 20) * 1_000,
        base * 1_000,
        (base - 10) * 1_000,
        base * 1_000,
        (base - 20) * 1_000,
        base * 1_000,
        (base - 10) * 1_000,
        (base - 600) * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let defaulted = mql_query_range(&app, "(mql_cpu > 2) default 0", base - 20, base, 10).await;
    assert_eq!(defaulted.0, StatusCode::OK, "{}", defaulted.1);
    assert_eq!(defaulted.1["data"]["resultType"], "matrix");
    assert_eq!(
        defaulted.1["data"]["result"],
        serde_json::json!([
            {
                "metric": {"__name__": "mql_cpu", "host": "a", "zone": "east"},
                "values": [[base - 20, "0"], [base - 10, "0"], [base, "3"]]
            },
            {
                "metric": {"__name__": "mql_cpu", "host": "b", "zone": "west"},
                "values": [[base - 20, "0"], [base - 10, "0"], [base, "4"]]
            }
        ])
    );
    let completely_filtered =
        mql_query_range(&app, "(mql_cpu > 5) default 0", base - 20, base, 10).await;
    assert_eq!(
        completely_filtered.1["data"]["result"],
        serde_json::json!([
            {
                "metric": {"__name__": "mql_cpu", "host": "a", "zone": "east"},
                "values": [[base - 20, "0"], [base - 10, "0"], [base, "0"]]
            },
            {
                "metric": {"__name__": "mql_cpu", "host": "b", "zone": "west"},
                "values": [[base - 20, "0"], [base - 10, "0"], [base, "0"]]
            }
        ])
    );
    let vector_filtered = mql_query_range(
        &app,
        "(mql_cpu > on(host) mql_sparse) default 0",
        base - 20,
        base,
        10,
    )
    .await;
    assert_eq!(vector_filtered.0, StatusCode::OK, "{}", vector_filtered.1);
    assert_eq!(
        vector_filtered.1["data"]["result"],
        serde_json::json!([
            {
                "metric": {"host": "a"},
                "values": [[base - 20, "0"], [base - 10, "0"], [base, "0"]]
            },
            {
                "metric": {"host": "b"},
                "values": [[base - 20, "0"], [base - 10, "0"], [base, "0"]]
            }
        ])
    );
    let disjoint = mql_query_range(
        &app,
        "(mql_disjoint_left > on(host) mql_disjoint_right) default 0",
        base - 600,
        base,
        600,
    )
    .await;
    assert_eq!(disjoint.0, StatusCode::OK, "{}", disjoint.1);
    assert_eq!(
        disjoint.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "c"},
            "values": [[base - 600, "0"], [base, "0"]]
        }])
    );

    let selector_default = mql_query_range(&app, "mql_cpu default 0", base - 20, base, 10).await;
    assert_eq!(selector_default.0, StatusCode::OK, "{}", selector_default.1);
    assert_eq!(
        selector_default.1["data"]["result"],
        serde_json::json!([
            {
                "metric": {"__name__": "mql_cpu", "host": "a", "zone": "east"},
                "values": [[base - 20, "1"], [base - 10, "1"], [base, "3"]]
            },
            {
                "metric": {"__name__": "mql_cpu", "host": "b", "zone": "west"},
                "values": [[base - 20, "0"], [base - 10, "2"], [base, "4"]]
            }
        ])
    );

    let matching = mql_query_range(
        &app,
        "(mql_cpu > 2) default on(host) mql_sparse",
        base - 20,
        base,
        10,
    )
    .await;
    assert_eq!(matching.0, StatusCode::OK, "{}", matching.1);
    assert_eq!(
        matching.1["data"]["result"],
        serde_json::json!([
            {
                "metric": {"__name__": "mql_cpu", "host": "a", "zone": "east"},
                "values": [[base - 20, "10"], [base - 10, "10"], [base, "3"]]
            },
            {
                "metric": {"__name__": "mql_cpu", "host": "b", "zone": "west"},
                "values": [[base - 10, "20"], [base, "4"]]
            }
        ])
    );

    let conditional =
        mql_query_range(&app, "mql_cpu if on(host) mql_sparse", base - 20, base, 10).await;
    assert_eq!(conditional.0, StatusCode::OK, "{}", conditional.1);
    assert_eq!(
        conditional.1["data"]["result"],
        serde_json::json!([
            {
                "metric": {"__name__": "mql_cpu", "host": "a", "zone": "east"},
                "values": [[base - 20, "1"], [base - 10, "1"], [base, "3"]]
            },
            {
                "metric": {"__name__": "mql_cpu", "host": "b", "zone": "west"},
                "values": [[base - 10, "2"], [base, "4"]]
            }
        ])
    );
    let conditional_join = mql_query_range(
        &app,
        "mql_cpu if on(host) group_left(zone) mql_sparse",
        base - 20,
        base,
        10,
    )
    .await;
    assert_eq!(conditional_join, conditional);
    let unmatched = mql_query_range(&app, "mql_cpu if mql_sparse", base - 20, base, 10).await;
    assert_eq!(unmatched.0, StatusCode::OK, "{}", unmatched.1);
    assert_eq!(unmatched.1["data"]["result"], serde_json::json!([]));
    let inverse = mql_query_range(
        &app,
        "mql_cpu ifnot on(host) mql_sparse",
        base - 20,
        base,
        10,
    )
    .await;
    assert_eq!(inverse.0, StatusCode::OK, "{}", inverse.1);
    assert_eq!(inverse.1["data"]["result"], serde_json::json!([]));
    let inverse_unmatched =
        mql_query_range(&app, "mql_cpu ifnot mql_sparse", base - 20, base, 10).await;
    assert_eq!(
        inverse_unmatched.0,
        StatusCode::OK,
        "{}",
        inverse_unmatched.1
    );
    assert_eq!(
        inverse_unmatched.1["data"]["result"],
        conditional.1["data"]["result"]
    );

    let missing = mql_query(&app, "mql_missing default 7", base).await;
    assert_eq!(missing.0, StatusCode::OK, "{}", missing.1);
    assert_eq!(missing.1["data"]["resultType"], "vector");
    assert_eq!(
        missing.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base, "7"]}])
    );
    assert_eq!(
        mql_query(&app, "1 default 2", base).await.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base, "1"]}])
    );
    assert_eq!(
        mql_query(&app, "1 ifnot 2", base).await.1["data"]["result"],
        serde_json::json!([])
    );

    let nested = mql_query_range(&app, "sum((mql_cpu > 2) default 0)", base - 20, base, 10).await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(
        nested.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [[base - 20, "0"], [base - 10, "0"], [base, "7"]]
        }])
    );
    let precedence =
        mql_query_range(&app, "(mql_cpu > 2) default 0 + 1", base - 20, base, 10).await;
    assert_eq!(precedence.0, StatusCode::OK, "{}", precedence.1);
    assert_eq!(precedence.1["data"]["result"][0]["values"][0][1], "1");
    assert_eq!(precedence.1["data"]["result"][0]["values"][2][1], "3");

    let stable_promql = prom_query(&app, "mql_cpu default 0", base).await;
    assert_eq!(
        stable_promql.0,
        StatusCode::BAD_REQUEST,
        "{}",
        stable_promql.1
    );
    assert_eq!(stable_promql.1["errorType"], "bad_data");
    for query in [
        "mql_cpu default",
        "mql_cpu default on(host",
        "mql_cpu default bool 0",
    ] {
        let invalid = mql_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }
    let posted = post_form(
        &app,
        "/metricsql/api/v1/query",
        &form_urlencoded::Serializer::new(String::new())
            .append_pair("query", "mql_missing default 7")
            .append_pair("time", &base.to_string())
            .finish(),
    )
    .await;
    assert_eq!(posted, missing);

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 3,
            ..PromQueryLimits::default()
        },
    );
    let rejected = mql_query_range(&limited, "(mql_cpu > 2) default 0", base - 20, base, 10).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert_eq!(rejected.1["errorType"], "execution");
    let limit_error = rejected.1["error"].as_str().unwrap();
    assert!(
        limit_error.contains("intermediate-work limit") || limit_error.contains("work point limit"),
        "{}",
        rejected.1
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 16, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        mql_query_range(
            &reopened_app,
            "(mql_cpu > 2) default 0",
            base - 20,
            base,
            10,
        )
        .await,
        defaulted
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_fifteen_metricsql_keep_metric_names_matches_victoriametrics_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory
        .path()
        .join("session_fifteen_metricsql_keep_names.db");
    let base = 1_785_901_638_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "mql_keep_a{{host=\"shared\"}} -2 {}\n",
            "mql_keep_b{{host=\"shared\"}} 3 {}\n",
            "mql_keep_counter 1 {}\n",
            "mql_keep_counter 2 {}\n",
            "mql_keep_counter 3 {}\n",
            "mql_keep_counter 4 {}\n",
            "mql_keep_counter 5 {}\n",
            "mql_keep_counter 6 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        (base - 5) * 1_000,
        (base - 4) * 1_000,
        (base - 3) * 1_000,
        (base - 2) * 1_000,
        (base - 1) * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let transform = mql_query(
        &app,
        "abs({__name__=~\"mql_keep_a|mql_keep_b\"}) keep_metric_names",
        base,
    )
    .await;
    assert_eq!(transform.0, StatusCode::OK, "{}", transform.1);
    assert_eq!(
        transform.1["data"]["result"],
        serde_json::json!([
            {
                "metric": {"__name__": "mql_keep_a", "host": "shared"},
                "value": [base, "2"]
            },
            {
                "metric": {"__name__": "mql_keep_b", "host": "shared"},
                "value": [base, "3"]
            }
        ])
    );

    let rollup = mql_query(&app, "rate(mql_keep_counter[5s]) keep_metric_names", base).await;
    assert_eq!(rollup.0, StatusCode::OK, "{}", rollup.1);
    assert_eq!(
        rollup.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "mql_keep_counter"},
            "value": [base, "1"]
        }])
    );

    let scalar_binary = mql_query(
        &app,
        "({__name__=~\"mql_keep_a|mql_keep_b\"} / 10) keep_metric_names",
        base,
    )
    .await;
    assert_eq!(scalar_binary.0, StatusCode::OK, "{}", scalar_binary.1);
    assert_eq!(
        scalar_binary.1["data"]["result"],
        serde_json::json!([
            {
                "metric": {"__name__": "mql_keep_a", "host": "shared"},
                "value": [base, "-0.2"]
            },
            {
                "metric": {"__name__": "mql_keep_b", "host": "shared"},
                "value": [base, "0.3"]
            }
        ])
    );
    let name_aware = mql_query(&app, "(mql_keep_a + mql_keep_b) keep_metric_names", base).await;
    assert_eq!(name_aware.0, StatusCode::OK, "{}", name_aware.1);
    assert_eq!(name_aware.1["data"]["result"], serde_json::json!([]));
    let explicit_on = mql_query(
        &app,
        "(mql_keep_a + on(host) mql_keep_b) keep_metric_names",
        base,
    )
    .await;
    assert_eq!(explicit_on.0, StatusCode::OK, "{}", explicit_on.1);
    assert_eq!(
        explicit_on.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "mql_keep_a", "host": "shared"},
            "value": [base, "1"]
        }])
    );
    let nested = mql_query(
        &app,
        "sum(abs({__name__=~\"mql_keep_a|mql_keep_b\"}) keep_metric_names)",
        base,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(
        nested.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base, "5"]}])
    );

    let posted = post_form(
        &app,
        "/metricsql/api/v1/query",
        &form_urlencoded::Serializer::new(String::new())
            .append_pair("query", "abs(mql_keep_a) keep_metric_names")
            .append_pair("time", &base.to_string())
            .finish(),
    )
    .await;
    assert_eq!(posted.0, StatusCode::OK, "{}", posted.1);
    assert_eq!(
        posted.1["data"]["result"][0]["metric"]["__name__"],
        "mql_keep_a"
    );

    let stable = prom_query(&app, "abs(mql_keep_a) keep_metric_names", base).await;
    assert_eq!(stable.0, StatusCode::BAD_REQUEST, "{}", stable.1);
    for query in [
        "mql_keep_a keep_metric_names",
        "sum(mql_keep_a) keep_metric_names",
        "-mql_keep_a keep_metric_names",
        "abs(mql_keep_a) keep_metric_names keep_metric_names",
    ] {
        let invalid = mql_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = mql_query(
        &limited,
        "abs({__name__=~\"mql_keep_a|mql_keep_b\"}) keep_metric_names",
        base,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert_eq!(rejected.1["errorType"], "execution");

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 16, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        mql_query(
            &reopened_app,
            "abs({__name__=~\"mql_keep_a|mql_keep_b\"}) keep_metric_names",
            base,
        )
        .await,
        transform
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_fifteen_metricsql_union_alias_match_victoriametrics_and_reopen() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_fifteen_metricsql_union.db");
    let base = 1_785_901_638_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "mql_union_a{{host=\"shared\"}} 2 {}\n",
            "mql_union_b{{host=\"shared\"}} 3 {}\n",
            "mql_union_c{{host=\"other\"}} 4 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    let expected_union = serde_json::json!([
        {
            "metric": {"__name__": "mql_union_a", "host": "shared"},
            "value": [base, "2"]
        },
        {
            "metric": {"__name__": "mql_union_b", "host": "shared"},
            "value": [base, "3"]
        }
    ]);
    let named = mql_query(&app, "union(mql_union_a, mql_union_b)", base).await;
    assert_eq!(named.0, StatusCode::OK, "{}", named.1);
    assert_eq!(named.1["data"]["resultType"], "vector");
    assert_eq!(named.1["data"]["result"], expected_union);
    let shorthand = mql_query(&app, "(mql_union_a, mql_union_b)", base).await;
    assert_eq!(shorthand, named);
    assert_eq!(
        mql_query(&app, "UNION(mql_union_a, mql_union_b)", base).await,
        named
    );
    assert_eq!(
        mql_query(&app, "union(mql_union_a)", base).await.1["data"]["result"],
        serde_json::json!([expected_union[0].clone()])
    );
    assert_eq!(
        mql_query(&app, "union(mql_union_a, mql_union_b,)", base).await,
        named
    );
    assert_eq!(
        mql_query(&app, "(mql_union_a, mql_union_b,)", base).await,
        named
    );
    assert_eq!(
        mql_query(&app, "(mql_union_a,)", base).await.1["data"]["result"],
        serde_json::json!([expected_union[0].clone()])
    );
    let empty = mql_query(&app, "union()", base).await;
    assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
    assert_eq!(empty.1["data"]["resultType"], "vector");
    assert_eq!(empty.1["data"]["result"], serde_json::json!([]));

    let aliased = mql_query(&app, "alias(mql_union_a, \"mql_alias\")", base).await;
    assert_eq!(aliased.0, StatusCode::OK, "{}", aliased.1);
    assert_eq!(
        aliased.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "mql_alias", "host": "shared"},
            "value": [base, "2"]
        }])
    );
    assert_eq!(
        mql_query(&app, "alias(mql_union_a, \"mql_alias\",)", base).await,
        aliased
    );
    let collision = mql_query(
        &app,
        concat!(
            "union(alias(mql_union_a, \"mql_collision\"), ",
            "alias(mql_union_b, \"mql_collision\"))"
        ),
        base,
    )
    .await;
    assert_eq!(collision.0, StatusCode::OK, "{}", collision.1);
    assert_eq!(
        collision.1["data"]["result"],
        serde_json::json!([{
            "metric": {"__name__": "mql_collision", "host": "shared"},
            "value": [base, "2"]
        }])
    );
    let direct_collision = mql_query(
        &app,
        "alias({__name__=~\"mql_union_a|mql_union_b\"}, \"mql_collision\")",
        base,
    )
    .await;
    assert_eq!(
        direct_collision.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        direct_collision.1
    );
    assert_eq!(direct_collision.1["errorType"], "execution");
    assert!(direct_collision.1["error"]
        .as_str()
        .unwrap()
        .contains("duplicate output timeseries"));
    let nameless = mql_query(&app, "alias(mql_union_a, \"\")", base).await;
    assert_eq!(nameless.0, StatusCode::OK, "{}", nameless.1);
    assert_eq!(
        nameless.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "shared"},
            "value": [base, "2"]
        }])
    );
    let scalars = mql_query(&app, "union(1, 2)", base).await;
    assert_eq!(scalars.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", scalars.1);
    assert_eq!(scalars.1["errorType"], "execution");
    assert!(scalars.1["error"]
        .as_str()
        .unwrap()
        .contains("duplicate output timeseries"));
    let nested = mql_query(
        &app,
        concat!(
            "sum(union(alias(mql_union_a, \"mql_alias_a\"), ",
            "alias(mql_union_b, \"mql_alias_b\")))"
        ),
        base,
    )
    .await;
    assert_eq!(nested.0, StatusCode::OK, "{}", nested.1);
    assert_eq!(
        nested.1["data"]["result"],
        serde_json::json!([{"metric": {}, "value": [base, "5"]}])
    );

    let range = mql_query_range(&app, "alias(mql_union_a, \"mql_alias\")", base, base, 1).await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(range.1["data"]["resultType"], "matrix");
    assert_eq!(
        range.1["data"]["result"][0]["values"],
        serde_json::json!([[base, "2"]])
    );
    let posted = post_form(
        &app,
        "/metricsql/api/v1/query",
        &form_urlencoded::Serializer::new(String::new())
            .append_pair("query", "union(mql_union_a, mql_union_b)")
            .append_pair("time", &base.to_string())
            .finish(),
    )
    .await;
    assert_eq!(posted, named);

    for query in [
        "union(mql_union_a, mql_union_b)",
        "(mql_union_a, mql_union_b)",
        "alias(mql_union_a, \"mql_alias\")",
    ] {
        let stable = prom_query(&app, query, base).await;
        assert_eq!(stable.0, StatusCode::BAD_REQUEST, "{query}: {}", stable.1);
        assert_eq!(stable.1["errorType"], "bad_data");
    }
    for query in [
        "alias(mql_union_a)",
        "alias(mql_union_a, 1)",
        "alias(mql_union_a, \"a\", \"b\")",
        "ALIAS(mql_union_a, \"mql_alias\")",
        "(mql_union_a,,mql_union_b)",
    ] {
        let invalid = mql_query(&app, query, base).await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{query}: {}", invalid.1);
        assert_eq!(invalid.1["errorType"], "bad_data");
    }

    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = mql_query(&limited, "union(mql_union_a, mql_union_b)", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert_eq!(rejected.1["errorType"], "execution");

    let response_limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_response_bytes: 350,
            ..PromQueryLimits::default()
        },
    );
    let long_alias = "x".repeat(200);
    let rejected = mql_query(
        &response_limited,
        &format!("alias({{__name__=~\"mql_union_a|mql_union_c\"}}, \"{long_alias}\")"),
        base,
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert_eq!(rejected.1["errorType"], "execution");
    assert!(rejected.1["error"]
        .as_str()
        .unwrap()
        .contains("response-size limit"));
    assert_eq!(
        mql_query(&response_limited, "union()", base).await.0,
        StatusCode::OK
    );

    drop((response_limited, limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 16, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        mql_query(&reopened_app, "union(mql_union_a, mql_union_b)", base).await,
        named
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn session_eleven_promql_atan2_matches_scalar_vector_ieee_and_reopens() {
    let extension = extension_path();
    assert!(extension.is_file(), "missing {}", extension.display());
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("session_eleven_atan2.db");
    let base = 1_701_000_000_i64;
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        16,
        DEFAULT_RAW_RETENTION,
    )
    .unwrap();
    let app = router(storage.clone());
    let fixture = format!(
        concat!(
            "atan_lhs{{host=\"a\",zone=\"east\"}} 8 {}\n",
            "atan_rhs{{host=\"a\",zone=\"east\"}} 2 {}\n",
            "atan_matching_lhs{{host=\"a\",shared=\"x\",zone=\"east\"}} 8 {}\n",
            "atan_matching_rhs{{host=\"a\",shared=\"x\",zone=\"west\"}} 2 {}\n",
            "atan_temporal 6 {}\n",
            "atan_temporal 7 {}\n"
        ),
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        base * 1_000,
        (base + 10) * 1_000,
    );
    assert_no_content(post_body(&app, "/api/v1/import/prometheus", fixture.as_bytes()).await);
    assert_eq!(post_json(&app, "/api/v1/flush").await.0, StatusCode::OK);

    for (query, expected) in [
        ("8 atan2 2", "1.3258176636680323"),
        ("NaN atan2 2", "NaN"),
        ("0 atan2 -1", "3.141592653589793"),
        ("-0 atan2 -1", "-3.141592653589793"),
    ] {
        let response = prom_query(&app, query, base).await;
        assert_eq!(response.0, StatusCode::OK, "{query}: {}", response.1);
        assert_eq!(response.1["data"]["resultType"], "scalar");
        assert_eq!(response.1["data"]["result"][1], expected);
    }

    let matched = prom_query(&app, "atan_lhs atan2 atan_rhs", base).await;
    assert_eq!(matched.0, StatusCode::OK, "{}", matched.1);
    assert_eq!(
        matched.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "a", "zone": "east"},
            "value": [base, "1.3258176636680323"]
        }])
    );
    let ignoring = prom_query(
        &app,
        "atan_matching_lhs atan2 ignoring(zone) atan_matching_rhs",
        base,
    )
    .await;
    assert_eq!(ignoring.0, StatusCode::OK, "{}", ignoring.1);
    assert_eq!(
        ignoring.1["data"]["result"],
        serde_json::json!([{
            "metric": {"host": "a", "shared": "x"},
            "value": [base, "1.3258176636680323"]
        }])
    );
    let scalar_left = prom_query(&app, "-0 atan2 atan_rhs", base).await;
    assert_eq!(scalar_left.0, StatusCode::OK, "{}", scalar_left.1);
    assert_eq!(scalar_left.1["data"]["result"][0]["value"][1], "-0");
    assert!(scalar_left.1["data"]["result"][0]["metric"]
        .get("__name__")
        .is_none());

    let range = prom_query_range(&app, "atan_temporal atan2 2", base, base + 10, 10).await;
    assert_eq!(range.0, StatusCode::OK, "{}", range.1);
    assert_eq!(
        range.1["data"]["result"],
        serde_json::json!([{
            "metric": {},
            "values": [
                [base, "1.2490457723982544"],
                [base + 10, "1.2924966677897851"]
            ]
        }])
    );

    let invalid = prom_query(&app, "atan_temporal[1m] atan2 2", base).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert_eq!(invalid.1["errorType"], "bad_data");
    let limited = router_with_limits(
        storage.clone(),
        PromQueryLimits {
            max_work_points: 1,
            ..PromQueryLimits::default()
        },
    );
    let rejected = prom_query(&limited, "atan_lhs atan2 atan_rhs", base).await;
    assert_eq!(
        rejected.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        rejected.1
    );
    assert_eq!(
        rejected.1,
        serde_json::json!({
            "status": "error",
            "errorType": "execution",
            "error": "query exceeded the maximum intermediate-work limit of 1 points"
        })
    );

    drop((limited, app));
    storage.shutdown().await.unwrap();
    drop(storage);
    let reopened = Storage::start(database, extension, 1, 8, DEFAULT_RAW_RETENTION).unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        prom_query(&reopened_app, "atan_lhs atan2 atan_rhs", base)
            .await
            .1,
        matched.1
    );
    drop(reopened_app);
    reopened.shutdown().await.unwrap();
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

async fn prom_query(app: &axum::Router, query: &str, time: i64) -> (StatusCode, Value) {
    let params = form_urlencoded::Serializer::new(String::new())
        .append_pair("query", query)
        .append_pair("time", &time.to_string())
        .finish();
    get_json(app, &format!("/prometheus/api/v1/query?{params}")).await
}

async fn prom_query_range(
    app: &axum::Router,
    query: &str,
    start: i64,
    end: i64,
    step: i64,
) -> (StatusCode, Value) {
    let params = form_urlencoded::Serializer::new(String::new())
        .append_pair("query", query)
        .append_pair("start", &start.to_string())
        .append_pair("end", &end.to_string())
        .append_pair("step", &step.to_string())
        .finish();
    get_json(app, &format!("/prometheus/api/v1/query_range?{params}")).await
}

async fn mql_query(app: &axum::Router, query: &str, time: i64) -> (StatusCode, Value) {
    let params = form_urlencoded::Serializer::new(String::new())
        .append_pair("query", query)
        .append_pair("time", &time.to_string())
        .finish();
    get_json(app, &format!("/metricsql/api/v1/query?{params}")).await
}

async fn mql_query_range(
    app: &axum::Router,
    query: &str,
    start: i64,
    end: i64,
    step: i64,
) -> (StatusCode, Value) {
    let params = form_urlencoded::Serializer::new(String::new())
        .append_pair("query", query)
        .append_pair("start", &start.to_string())
        .append_pair("end", &end.to_string())
        .append_pair("step", &step.to_string())
        .finish();
    get_json(app, &format!("/metricsql/api/v1/query_range?{params}")).await
}

fn assert_no_content(response: (StatusCode, Vec<u8>)) {
    assert_eq!(response.0, StatusCode::NO_CONTENT);
    assert!(response.1.is_empty());
}

fn persisted_rows(database: &Path, extension: &Path) -> Vec<(String, i64, Option<f64>)> {
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
