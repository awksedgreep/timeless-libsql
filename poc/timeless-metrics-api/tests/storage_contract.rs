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
                    "values": [[base, "10.0"], [base + 10, "20.0"], [base + 20, "30.0"]]
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
        serde_json::json!([[base, "10.0"], [base + 10, "20.0"]])
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
        serde_json::json!([[base, "10.0"], [base + 10, "15.0"], [base + 20, "25.0"]])
    );
    assert_eq!(
        average.1["data"]["result"][0]["metric"],
        serde_json::json!({"__name__": "prom_cpu", "env": "prod", "host": "a"})
    );

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
        serde_json::json!([base + 20, "30.0"])
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
    assert_eq!(posted.1["data"]["result"][0]["values"][0][1], "10.0");

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
    assert_eq!(stats.api_promql_requests, 8);
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
    assert_eq!(fresh.1["data"]["result"][0]["value"][1], "0.0");

    drop(app);
    storage.shutdown().await.unwrap();
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
