use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Datelike;
use rusqlite::Connection;
use std::time::Duration;
use timeless_logs_api::{
    parse_logsql_at, router, router_with_limits, LogEntry, LogsQueryLimits, LogsqlOutput, Storage,
    TimestampUnit,
};
use tower::ServiceExt;

async fn pipeline_rows(app: &axum::Router, query: &str) -> Vec<serde_json::Value> {
    let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{query}: {}",
        String::from_utf8_lossy(&body)
    );
    ndjson_values(&body)
}

fn numeric_pipeline_entries() -> Vec<LogEntry> {
    [
        ("numeric-missing", "a", None),
        ("numeric-null", "a", Some("null")),
        ("numeric-negative", "b", Some("-2")),
        ("numeric-zero", "b", Some("0")),
        ("numeric-two", "a", Some("2")),
        ("numeric-decimal", "a", Some("2.5")),
        ("numeric-string", "b", Some(r#""3""#)),
        ("numeric-ten", "b", Some("10")),
        ("numeric-huge", "a", Some("9007199254740993")),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (case, partition, n))| {
        let mut metadata = serde_json::json!({
            "case": case,
            "numeric_group": "numeric",
            "first_partition": partition,
            "nested": {"case": case},
        });
        if let Some(n) = n {
            metadata["n"] = serde_json::from_str(n).unwrap();
        }
        LogEntry {
            ts: 1_800_000_000_000_000 + index as i64,
            level: 1,
            severity: "info".into(),
            message: case.replace('-', " "),
            metadata_json: serde_json::to_string(&metadata).unwrap(),
        }
    })
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn release_backup_preserves_exact_logs_and_refuses_overwrite() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let backup = temp.path().join("backup-logs.db");
    let storage =
        Storage::start(temp.path().join("logs.db"), extension.clone().into(), 1, 8).unwrap();
    let app = router(storage.clone());
    assert_eq!(
        app.oneshot(ingest_request(make_lines(0, 16_384)))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );

    let report = storage.backup(backup.clone()).await.unwrap();
    assert_eq!(report.signal, "logs");
    assert!(report.bytes > 0);
    let original = std::fs::read(&backup).unwrap();
    assert!(storage
        .backup(backup.clone())
        .await
        .unwrap_err()
        .contains("refusing to overwrite"));
    assert_eq!(std::fs::read(&backup).unwrap(), original);
    let live_stats = storage.stats().await.unwrap();
    assert_eq!(live_stats.backup_count, 2);
    assert_eq!(live_stats.backup_errors, 1);
    assert_eq!(live_stats.checkpoint_count, 2);
    assert_eq!(live_stats.checkpoint_errors, 0);
    assert!(live_stats.backup_total_ns > 0);
    assert!(live_stats.checkpoint_total_ns > 0);
    assert!(live_stats.database_file_bytes > 0);
    assert_eq!(live_stats.writer_connections, 1);
    assert_eq!(live_stats.reader_connections, 1);
    assert_eq!(live_stats.command_queue_capacity_batches, 8);
    assert_eq!(
        live_stats.physical_database_bytes,
        live_stats.database_file_bytes
            + live_stats.database_wal_bytes
            + live_stats.database_shm_bytes
    );
    assert!(live_stats.sqlite_page_bytes > 0);
    assert!(live_stats.freelist_pages >= 0);
    assert!(live_stats.freelist_bytes >= 0);
    assert!(live_stats.freelist_bytes <= live_stats.sqlite_page_bytes);

    let restored = Storage::start(backup, extension.into(), 1, 8).unwrap();
    let stats = restored.stats().await.unwrap();
    assert_eq!(stats.total_entries, 16_384);
    assert_eq!(stats.buffered_entries, 0);
    assert_eq!(
        restored
            .query(timeless_logs_api::QuerySpec {
                limit: 20_000,
                ..Default::default()
            })
            .await
            .unwrap()
            .len(),
        16_384
    );
    restored.shutdown().await.unwrap();
    storage.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn http_uses_the_established_8192_entry_buffer_without_request_flushes() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::start(temp.path().join("logs.db"), extension.into(), 2, 32).unwrap();
    let app = router(storage.clone());

    // One HTTP request below the engine threshold remains in the extension
    // buffer. There is no host threshold and no request-boundary flush.
    let response = app
        .clone()
        .oneshot(ingest_request(make_lines(0, 100)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    storage.barrier().await.unwrap();
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.total_entries, 100);
    assert_eq!(stats.buffered_entries, 100);
    assert_eq!(stats.raw_blocks, 0);
    assert_eq!(stats.queued_entries, 0);
    assert_eq!(stats.admitted_entries, 100);
    assert_eq!(stats.completed_entries, 100);
    assert_eq!(stats.flush_count, 0);

    // Crossing exactly 8,192 lets the unmodified vtab perform its own
    // automatic level-partitioned raw flush.
    let response = app
        .clone()
        .oneshot(ingest_request(make_lines(100, 8_092)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    storage.barrier().await.unwrap();
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.total_entries, 8_192);
    assert_eq!(stats.buffered_entries, 0);
    assert_eq!(stats.raw_blocks, 4, "one raw block per level present");
    assert_eq!(stats.compressed_blocks, 0, "ingest must not optimize");
    assert_eq!(stats.admitted_entries, 8_192);
    assert_eq!(stats.completed_entries, 8_192);
    assert_eq!(stats.ingest_batch_count, 2);
    assert_eq!(stats.ingest_batch_entries, 8_192);
    assert!(stats.ingest_wire_decode_ns > 0);
    assert!(stats.ingest_normalize_ns > 0);
    assert!(stats.ingest_buffer_append_ns > 0);
    assert_eq!(stats.flush_count, 1);
    assert_eq!(stats.flush_entries, 8_192);
    assert!(stats.index_size > 0, "index_size is allocated bytes");
    assert!(stats.term_postings > 0);
    assert!(stats.flush_total_ns > 0);
    assert!(stats.flush_partition_ns > 0);
    assert!(stats.flush_encode_terms_ns > 0);
    assert!(stats.flush_store_ns > 0);

    // The API query path sees the exact batch after the engine-controlled
    // flush and pushes service+level into the existing hidden columns.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/select/logsql/query?level=error&service=api&limit=10000&order=asc")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        body.split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count(),
        410
    );
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.query_count, 1);
    assert_eq!(stats.query_candidate_blocks, 1);
    assert_eq!(stats.query_decoded_entries, 410);
    assert_eq!(stats.query_matched_entries, 410);
    assert_eq!(stats.query_returned_entries, 410);
    assert_eq!(stats.query_bounded_count, 1);
    assert_eq!(stats.query_bounded_requested_entries, 10_000);
    assert_eq!(stats.query_bounded_max_entries, 10_000);
    assert!(stats.query_total_ns > 0);
    assert!(stats.query_snapshot_ns > 0);
    assert!(stats.query_materialize_ns > 0);
    assert_eq!(stats.query_snapshot_payload_bytes, 0);
    assert_eq!(stats.query_snapshot_payload_max_bytes, 0);
    assert_eq!(stats.query_stable_location_snapshots, 1);
    assert!(stats.query_payload_bytes_read > 0);
    assert!(stats.read_permit_count > 0);
    assert_eq!(stats.waiting_writers, 0);

    // Dashboard grouping stays inside the public extension through the
    // bounded field-values TVF. Host equality is also an indexed API filter;
    // neither path scans a private shadow table or falls back to BEAM.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(
                    "/select/logsql/field_values?field=host&level=error&start=1700000000&end=1700010000&limit=10",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], br#"{"values":["host-api"]}"#);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/select/logsql/query?host=host-api&level=error&limit=10000&order=asc")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        body.split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count(),
        410
    );

    // Count is a scalar engine operation, not COUNT(*) over a materialized
    // vtab rowset. Rich log blocks retain the product's exact severities, so
    // an `error` predicate must decode its coarse error-family partition to
    // distinguish error from critical/alert/emergency.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/select/logsql/query")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=level%3Aerror+%7C+stats+count%28*%29"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"{\"total\":410}\n");
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.api_query_count, 4);
    assert_eq!(stats.query_count, 2, "native count is not a row query");
    assert_eq!(stats.native_count_count, 1);
    assert_eq!(stats.native_count_metadata_blocks, 0);
    assert_eq!(stats.native_count_metadata_entries, 0);
    assert_eq!(stats.native_count_decoded_blocks, 1);
    assert_eq!(stats.native_count_decoded_entries, 410);
    assert!(stats.native_count_payload_bytes_read > 0);

    // A syntactically valid but unsupported LogsQL pipeline must not be
    // silently reduced to a broader query or cross into another owner.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/select/logsql/query")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=level%3Aerror+%7C+block_stats"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({
            "error": "unsupported_capability",
            "reason": "unsupported_logsql",
            "message": "unsupported LogsQL pipeline \"block_stats\""
        })
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/select/logsql/query?regex=request")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({
            "error": "unsupported_capability",
            "reason": "unsupported_query_parameters"
        })
    );

    // The API maintenance tick asks the extension for its exact actionable
    // backlog, derives a bounded source-byte budget, and invokes the public
    // incremental optimize command. It does not reproduce compaction policy.
    assert_eq!(stats.optimize_pending_raw_blocks, 4);
    assert_eq!(stats.optimize_pending_raw_entries, 8_192);
    assert_eq!(stats.optimize_merge_ready_entries, 0);
    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.raw_blocks, 0);
    assert_eq!(stats.compressed_blocks, 4);
    assert_eq!(stats.optimize_count, 1);
    assert_eq!(stats.optimize_budgeted_count, 1);
    assert_eq!(stats.optimize_budget_entries, 8_192);
    assert_eq!(stats.optimize_raw_groups, 4);
    assert_eq!(stats.optimize_raw_blocks, 4);
    assert_eq!(stats.optimize_raw_entries, 8_192);
    assert!(stats.optimize_raw_input_bytes > 0);
    assert!(stats.optimize_raw_output_bytes > 0);
    assert!(stats.optimize_raw_total_ns > 0);
    assert_eq!(stats.optimize_merge_groups, 0);
    assert_eq!(stats.optimize_pending_raw_entries, 0);
    assert_eq!(stats.optimize_merge_ready_entries, 0);
    assert_eq!(stats.optimize_merge_deferred_blocks, 4);
    assert_eq!(stats.optimize_merge_deferred_entries, 8_192);
    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    assert_eq!(
        storage.stats().await.unwrap().optimize_count,
        1,
        "a timer wake-up must not optimize permanently deferred tails"
    );

    // Manual flush is an ordered durability barrier, not the ingest path.
    let response = app
        .clone()
        .oneshot(ingest_request(make_lines(8_192, 10)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/flush")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.total_entries, 8_202);
    assert_eq!(stats.buffered_entries, 0);

    storage.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_exact_rich_batch_stays_decodable_across_readers_and_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("rich-batch-decode.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        32,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    assert_eq!(
        app.oneshot(ingest_request(make_evidence_rich_lines(8_192)))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.total_entries, 8_192);
    assert_eq!(stats.buffered_entries, 0);
    assert_eq!(stats.raw_blocks, 4);
    assert_eq!(stats.compressed_blocks, 0);

    assert_evidence_rich_rows(&storage, 16).await;
    assert_eq!(
        pipeline_rows(
            &router(storage.clone()),
            r#"pattern_match_full("query contract event <N>") | sort by (_time) asc | limit 10000"#,
        )
        .await
        .len(),
        8_192
    );
    storage.shutdown().await.unwrap();

    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        2,
        32,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_evidence_rich_rows(&reopened, 16).await;
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"context.attempt:pattern_match_full("<N>") | sort by (_time) asc | limit 10000"#,
        )
        .await
        .len(),
        8_192
    );

    reopened.schedule_optimize().await.unwrap();
    reopened.barrier().await.unwrap();
    let stats = reopened.stats().await.unwrap();
    assert_eq!(stats.raw_blocks, 0);
    assert_eq!(stats.compressed_blocks, 4);
    assert_evidence_rich_rows(&reopened, 16).await;
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_query_stats_is_request_local_durable_and_direct_sql_visible() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("query-stats-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let body = [
        r#"{"_time":1,"_msg":"one","level":"info","case":"one","service":"api"}"#,
        r#"{"_time":2,"_msg":"two","level":"info","case":"two","service":"worker"}"#,
        r#"{"_time":3,"_msg":"three","level":"info","case":"three","service":"worker"}"#,
        r#"{"_time":4,"_msg":"four","level":"info","case":"four","service":"worker"}"#,
    ]
    .join("\n");
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.flush().await.unwrap();

    let full = pipeline_rows(&app, "* | query_stats").await;
    assert_eq!(full.len(), 1);
    let full = full[0].as_object().unwrap();
    assert_eq!(full.len(), 14);
    assert!(full.values().all(serde_json::Value::is_string));
    assert_eq!(full["BytesReadColumnsHeaders"], "0");
    assert_eq!(full["BytesReadColumnsHeaderIndexes"], "0");
    assert_eq!(full["BytesReadBloomFilters"], "0");
    assert_eq!(full["BytesReadTimestamps"], "0");
    assert_eq!(full["BytesReadBlockHeaders"], "0");
    assert_eq!(full["BytesProcessedUncompressedValues"], "0");
    assert_eq!(full["BlocksProcessed"], "1");
    assert_eq!(full["RowsProcessed"], "4");
    assert_eq!(full["RowsFound"], "4");
    assert_eq!(full["ValuesRead"], "12");
    assert_eq!(full["TimestampsRead"], "4");
    assert_eq!(full["BytesReadValues"], full["BytesReadTotal"]);
    assert!(
        full["BytesReadTotal"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
    assert!(
        full["QueryDurationNsecs"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"service:="api" | query_stats | keep RowsFound,RowsProcessed"#,
        )
        .await,
        [serde_json::json!({"RowsFound":"1","RowsProcessed":"4"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"="absent" | query_stats | keep RowsFound,RowsProcessed"#,
        )
        .await,
        [serde_json::json!({"RowsFound":"0","RowsProcessed":"4"})]
    );
    // Timeless eagerly executes the bounded API rowset today. The report is
    // actual work rather than a VictoriaLogs-style hypothetical early stop.
    assert_eq!(
        pipeline_rows(
            &app,
            "* | limit 1 | query_stats | keep RowsFound,RowsProcessed",
        )
        .await,
        [serde_json::json!({"RowsFound":"4","RowsProcessed":"4"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            "* | fields case | query_stats | keep RowsFound,ValuesRead,TimestampsRead",
        )
        .await,
        [serde_json::json!({
            "RowsFound":"4",
            "ValuesRead":"12",
            "TimestampsRead":"4"
        })]
    );

    let malformed = app
        .clone()
        .oneshot(logsql_request("* | query_stats extra"))
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(malformed.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error":"invalid_query",
            "reason":"malformed_logsql",
            "message":"LogsQL query_stats accepts no arguments"
        })
    );

    // Two reader connections execute different cardinalities concurrently;
    // every response must retain only its own report.
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..32 {
        let app = app.clone();
        tasks.spawn(async move {
            let (query, expected) = if index % 2 == 0 {
                (r#"service:="api" | query_stats | keep RowsFound"#, "1")
            } else {
                (r#"service:="worker" | query_stats | keep RowsFound"#, "3")
            };
            let rows = pipeline_rows(&app, query).await;
            assert_eq!(rows, [serde_json::json!({"RowsFound":expected})]);
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 2,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("* | query_stats"))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        pipeline_rows(&app, "* | query_stats | keep RowsFound").await,
        [serde_json::json!({"RowsFound":"4"})]
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    assert_eq!(
        pipeline_rows(&app, "* | query_stats | keep RowsFound").await,
        [serde_json::json!({"RowsFound":"4"})]
    );
    storage.shutdown().await.unwrap();

    // Fresh connection: no report. A consumed scan publishes exactly one;
    // rollback does not erase work, while a failed scan clears prior state.
    let conn = Connection::open(&database).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(&extension, None::<&str>).unwrap();
    }
    conn.load_extension_disable().unwrap();
    let take = || {
        conn.query_row(
            "SELECT processed_entries,matched_entries,payload_bytes_read \
               FROM timeless_log_query_stats('logs')",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
    };
    assert!(take().unwrap_err().to_string().contains("no unconsumed"));
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM logs WHERE service='api' AND max_work_entries=4",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        1
    );
    let (processed, matched, payload) = take().unwrap();
    assert_eq!((processed, matched), (4, 1));
    assert!(payload > 0);
    assert!(take().unwrap_err().to_string().contains("no unconsumed"));

    conn.execute_batch("BEGIN").unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM logs", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        4
    );
    conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(take().unwrap().0, 4);
    assert!(conn
        .query_row(
            "SELECT count(*) FROM logs WHERE max_work_entries=1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_err()
        .to_string()
        .contains("max_work_entries=1"));
    assert!(take().unwrap_err().to_string().contains("no unconsumed"));
    drop(conn);

    let reopened = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        pipeline_rows(&reopened_app, "* | query_stats | keep RowsFound").await,
        [serde_json::json!({"RowsFound":"4"})]
    );
    reopened.shutdown().await.unwrap();

    let corrupt = temp.path().join("query-stats-corrupt.db");
    std::fs::copy(&database, &corrupt).unwrap();
    let conn = Connection::open(corrupt).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(&extension, None::<&str>).unwrap();
    }
    conn.load_extension_disable().unwrap();
    conn.execute("UPDATE logs_blocks SET data=x'00'", [])
        .unwrap();
    assert!(conn
        .query_row("SELECT count(*) FROM logs", [], |row| row.get::<_, i64>(0))
        .is_err());
    assert!(conn
        .query_row(
            "SELECT processed_entries FROM timeless_log_query_stats('logs')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_err()
        .to_string()
        .contains("no unconsumed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_first_is_typed_partitioned_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("first-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage.ingest(numeric_pipeline_entries()).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let cases = |rows: &[serde_json::Value]| {
        rows.iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    let ascending = pipeline_rows(&app, r#"numeric_group:="numeric" | first 5 by (n, case)"#).await;
    assert_eq!(
        cases(&ascending),
        [
            "numeric-missing",
            "numeric-null",
            "numeric-negative",
            "numeric-zero",
            "numeric-two",
        ]
    );
    assert!(ascending[0].get("n").is_none());
    assert!(ascending[1]["n"].is_null());
    assert_eq!(ascending[2]["n"], -2);
    assert_eq!(ascending[4]["n"], 2);

    let descending = pipeline_rows(
        &app,
        r#"numeric_group:="numeric" | FIRST 3 (n DeSc, case) | fields case"#,
    )
    .await;
    assert_eq!(
        cases(&descending),
        ["numeric-huge", "numeric-ten", "numeric-string"]
    );

    let partitioned = pipeline_rows(
        &app,
        r#"numeric_group:="numeric" | first 2 by (n, case) partition by (first_partition) rank as position | fields case, first_partition, position"#,
    )
    .await;
    assert_eq!(
        partitioned,
        [
            serde_json::json!({"case":"numeric-missing","first_partition":"a","position":"1"}),
            serde_json::json!({"case":"numeric-null","first_partition":"a","position":"2"}),
            serde_json::json!({"case":"numeric-negative","first_partition":"b","position":"1"}),
            serde_json::json!({"case":"numeric-zero","first_partition":"b","position":"2"}),
        ]
    );
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"case:in(numeric-two, numeric-ten) | first | fields case"#,
            )
            .await
        ),
        ["numeric-two"]
    );
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"case:in(numeric-two, numeric-ten) | delete _time | first | fields case"#,
            )
            .await
        ),
        ["numeric-ten"]
    );
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"numeric_group:="numeric" | filter n:>0 | first 2 by (n desc, case) | fields case"#,
            )
            .await
        ),
        ["numeric-huge", "numeric-ten"]
    );
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"numeric_group:="numeric" | first 2 by (_time desc) | fields case"#,
            )
            .await
        ),
        ["numeric-huge", "numeric-ten"]
    );
    assert!(
        pipeline_rows(&app, r#"case:="first-missing" | first 3 by (case)"#,)
            .await
            .is_empty()
    );

    for malformed in [
        "* | first 0",
        "* | first nope",
        "* | first by",
        "* | first by (case*)",
        "* | first partition by",
        "* | first partition by (*)",
        "* | first rank as",
        "* | first by (case) trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 4,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"numeric_group:="numeric" | first 2 by (n, case)"#,
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let result_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 2,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"numeric_group:="numeric" | first 3 by (n, case)"#,
    ))
    .await
    .unwrap();
    assert_eq!(result_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let state_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_response_bytes: 64,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"numeric_group:="numeric" | first 2 by (n, case)"#,
    ))
    .await
    .unwrap();
    assert_eq!(state_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(state_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_response_bytes",
            "limit": 64
        })
    );
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"numeric_group:="numeric" | first 2 by (n, case) | fields case"#,
            )
            .await
        ),
        ["numeric-missing", "numeric-null"]
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"numeric_group:="numeric" | first 3 by (n desc, case) | fields case"#,
            )
            .await
        ),
        ["numeric-huge", "numeric-ten", "numeric-string"]
    );
    storage.shutdown().await.unwrap();

    let reopened = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        cases(
            &pipeline_rows(
                &router(reopened.clone()),
                r#"numeric_group:="numeric" | first 5 by (n, case) | fields case"#,
            )
            .await
        ),
        [
            "numeric-missing",
            "numeric-null",
            "numeric-negative",
            "numeric-zero",
            "numeric-two",
        ]
    );
    reopened.shutdown().await.unwrap();

    let conn = Connection::open(database).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(&extension, None::<&str>).unwrap();
    }
    conn.load_extension_disable().unwrap();
    let mut statement = conn
        .prepare(
            "SELECT json_extract(metadata, '$.case') \
               FROM logs \
              WHERE json_extract(metadata, '$.numeric_group') = 'numeric' \
              ORDER BY CAST(json_extract(metadata, '$.n') AS REAL), \
                       json_extract(metadata, '$.case') \
              LIMIT 5",
        )
        .unwrap();
    let sql_cases = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        sql_cases,
        [
            "numeric-missing",
            "numeric-null",
            "numeric-negative",
            "numeric-zero",
            "numeric-two",
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_last_inverts_first_with_same_bounds_and_durability() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("last-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let rows = [
        ("numeric-missing", "a", None),
        ("numeric-null", "a", Some("null")),
        ("numeric-negative", "b", Some("-2")),
        ("numeric-zero", "b", Some("0")),
        ("numeric-two", "a", Some("2")),
        ("numeric-decimal", "a", Some("2.5")),
        ("numeric-string", "b", Some(r#""3""#)),
        ("numeric-ten", "b", Some("10")),
        ("numeric-huge", "a", Some("9007199254740993")),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (case, partition, n))| {
        let mut metadata = serde_json::json!({
            "case": case,
            "numeric_group": "numeric",
            "first_partition": partition,
            "nested": {"case": case},
        });
        if let Some(n) = n {
            metadata["n"] = serde_json::from_str(n).unwrap();
        }
        LogEntry {
            ts: 1_800_000_000_000_000 + index as i64,
            level: 1,
            severity: "info".into(),
            message: case.replace('-', " "),
            metadata_json: serde_json::to_string(&metadata).unwrap(),
        }
    })
    .collect();
    storage.ingest(rows).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let cases = |rows: &[serde_json::Value]| {
        rows.iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    let descending = pipeline_rows(&app, r#"numeric_group:="numeric" | last 5 by (n, case)"#).await;
    assert_eq!(
        cases(&descending),
        [
            "numeric-huge",
            "numeric-ten",
            "numeric-string",
            "numeric-decimal",
            "numeric-two",
        ]
    );
    assert_eq!(descending[0]["n"], 9_007_199_254_740_993u64);
    assert_eq!(descending[2]["n"], "3");
    assert_eq!(descending[3]["n"], 2.5);

    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"numeric_group:="numeric" | LAST 3 (case DeSc) | fields case"#,
            )
            .await
        ),
        ["numeric-decimal", "numeric-huge", "numeric-missing"]
    );

    let partitioned = pipeline_rows(
        &app,
        r#"numeric_group:="numeric" | last 2 by (n, case) partition by (first_partition) rank as position | fields case, first_partition, position"#,
    )
    .await;
    assert_eq!(
        partitioned,
        [
            serde_json::json!({"case":"numeric-huge","first_partition":"a","position":"1"}),
            serde_json::json!({"case":"numeric-decimal","first_partition":"a","position":"2"}),
            serde_json::json!({"case":"numeric-ten","first_partition":"b","position":"1"}),
            serde_json::json!({"case":"numeric-string","first_partition":"b","position":"2"}),
        ]
    );
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"case:in(numeric-two, numeric-ten) | last | fields case"#,
            )
            .await
        ),
        ["numeric-ten"]
    );
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"case:in(numeric-two, numeric-ten) | delete _time | last | fields case"#,
            )
            .await
        ),
        ["numeric-two"]
    );
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"numeric_group:="numeric" | filter n:>0 | last 2 by (n, case) | fields case"#,
            )
            .await
        ),
        ["numeric-huge", "numeric-ten"]
    );
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"numeric_group:="numeric" | last 2 by (_time) | fields case"#,
            )
            .await
        ),
        ["numeric-huge", "numeric-ten"]
    );
    assert!(
        pipeline_rows(&app, r#"case:="last-missing" | last 3 by (case)"#)
            .await
            .is_empty()
    );

    for malformed in [
        "* | last 0",
        "* | last nope",
        "* | last by",
        "* | last by (case*)",
        "* | last partition by",
        "* | last partition by (*)",
        "* | last rank as",
        "* | last by (case) trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 4,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"numeric_group:="numeric" | last 2 by (n, case)"#,
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let result_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 2,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"numeric_group:="numeric" | last 3 by (n, case)"#,
    ))
    .await
    .unwrap();
    assert_eq!(result_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let state_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_response_bytes: 64,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"numeric_group:="numeric" | last 2 by (n, case)"#,
    ))
    .await
    .unwrap();
    assert_eq!(state_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(state_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_response_bytes",
            "limit": 64
        })
    );
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"numeric_group:="numeric" | last 2 by (n, case) | fields case"#,
            )
            .await
        ),
        ["numeric-huge", "numeric-ten"]
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    assert_eq!(
        cases(
            &pipeline_rows(
                &app,
                r#"numeric_group:="numeric" | last 3 by (n, case) | fields case"#,
            )
            .await
        ),
        ["numeric-huge", "numeric-ten", "numeric-string"]
    );
    storage.shutdown().await.unwrap();

    let reopened = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        cases(
            &pipeline_rows(
                &router(reopened.clone()),
                r#"numeric_group:="numeric" | last 5 by (n, case) | fields case"#,
            )
            .await
        ),
        [
            "numeric-huge",
            "numeric-ten",
            "numeric-string",
            "numeric-decimal",
            "numeric-two",
        ]
    );
    reopened.shutdown().await.unwrap();

    let conn = Connection::open(database).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(&extension, None::<&str>).unwrap();
    }
    conn.load_extension_disable().unwrap();
    let mut statement = conn
        .prepare(
            "SELECT json_extract(metadata, '$.case') \
               FROM logs \
              WHERE json_extract(metadata, '$.numeric_group') = 'numeric' \
              ORDER BY CAST(json_extract(metadata, '$.n') AS REAL) DESC, \
                       json_extract(metadata, '$.case') DESC \
              LIMIT 5",
        )
        .unwrap();
    let sql_cases = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        sql_cases,
        [
            "numeric-huge",
            "numeric-ten",
            "numeric-string",
            "numeric-decimal",
            "numeric-two",
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_top_counts_textual_groups_with_bounds_and_durability() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("top-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let rows = [
        ("numeric-missing", "a", None),
        ("numeric-null", "a", Some("null")),
        ("numeric-negative", "b", Some("-2")),
        ("numeric-zero", "b", Some("0")),
        ("numeric-two", "a", Some("2")),
        ("numeric-decimal", "a", Some("2.5")),
        ("numeric-string", "b", Some(r#""3""#)),
        ("numeric-ten", "b", Some("10")),
        ("numeric-huge", "a", Some("9007199254740993")),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (case, partition, n))| {
        let mut metadata = serde_json::json!({
            "case": case,
            "numeric_group": "numeric",
            "first_partition": partition,
            "nested": {"case": case},
        });
        if let Some(n) = n {
            metadata["n"] = serde_json::from_str(n).unwrap();
        }
        LogEntry {
            ts: 1_800_000_000_000_000 + index as i64,
            level: 1,
            severity: "info".into(),
            message: case.replace('-', " "),
            metadata_json: serde_json::to_string(&metadata).unwrap(),
        }
    })
    .collect();
    storage.ingest(rows).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | top by (first_partition)"#,
        )
        .await,
        [
            serde_json::json!({"first_partition":"a","hits":"5"}),
            serde_json::json!({"first_partition":"b","hits":"4"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | TOP 1 BY (first_partition) HITS AS total RANK AS position"#,
        )
        .await,
        [serde_json::json!({"first_partition":"a","total":"5","position":"1"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | top 1 by (first_partition) hits as hits rank as position"#,
        )
        .await,
        [serde_json::json!({"first_partition":"a","hits":"5","position":"1"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | top 5 numeric_group, first_partition"#,
        )
        .await,
        [
            serde_json::json!({"numeric_group":"numeric","first_partition":"a","hits":"5"}),
            serde_json::json!({"numeric_group":"numeric","first_partition":"b","hits":"4"}),
        ]
    );
    let values = pipeline_rows(&app, r#"numeric_group:="numeric" | top 8 by (n)"#).await;
    assert_eq!(
        values,
        [
            serde_json::json!({"hits":"2"}),
            serde_json::json!({"n":"-2","hits":"1"}),
            serde_json::json!({"n":"0","hits":"1"}),
            serde_json::json!({"n":"10","hits":"1"}),
            serde_json::json!({"n":"2","hits":"1"}),
            serde_json::json!({"n":"2.5","hits":"1"}),
            serde_json::json!({"n":"3","hits":"1"}),
            serde_json::json!({"n":"9007199254740993","hits":"1"}),
        ]
    );
    assert!(values[0].get("n").is_none());
    assert!(values.iter().all(|row| row["hits"].is_string()));
    assert_eq!(
        pipeline_rows(&app, r#"numeric_group:="numeric" | top by (hits)"#).await,
        [serde_json::json!({"hitss":"9"})]
    );
    assert_eq!(
        pipeline_rows(&app, r#"numeric_group:="numeric" | top by (rank) rank"#,).await,
        [serde_json::json!({"hits":"9","ranks":"1"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | filter first_partition:="a" | top by (first_partition) hits as count"#,
        )
        .await,
        [serde_json::json!({"first_partition":"a","count":"5"})]
    );
    assert!(
        pipeline_rows(&app, r#"case:="top-missing" | top 3 by (case)"#)
            .await
            .is_empty()
    );

    for malformed in [
        "* | top",
        "* | top 0 by (case)",
        "* | top nope by (case)",
        "* | top by",
        "* | top by (case*)",
        "* | top by (case) hits",
        "* | top by (case) rank as",
        "* | top by (case) trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 4,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | top by (n)"#,
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 2,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | top 3 by (n)"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 64,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | top 2 by (n)"#,
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason);
    }
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | top 1 by (first_partition)"#,
        )
        .await,
        [serde_json::json!({"first_partition":"a","hits":"5"})],
        "the reader remains reusable after every top limit rejection"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | top by (first_partition)"#,
        )
        .await,
        [
            serde_json::json!({"first_partition":"a","hits":"5"}),
            serde_json::json!({"first_partition":"b","hits":"4"}),
        ]
    );
    storage.shutdown().await.unwrap();

    let reopened = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"numeric_group:="numeric" | top 2 by (first_partition) rank"#,
        )
        .await,
        [
            serde_json::json!({"first_partition":"a","hits":"5","rank":"1"}),
            serde_json::json!({"first_partition":"b","hits":"4","rank":"2"}),
        ]
    );
    reopened.shutdown().await.unwrap();

    let conn = Connection::open(database).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(&extension, None::<&str>).unwrap();
    }
    conn.load_extension_disable().unwrap();
    let mut statement = conn
        .prepare(
            "SELECT json_extract(metadata, '$.first_partition') AS partition, \
                    count(*) AS hits \
               FROM logs \
              WHERE json_extract(metadata, '$.numeric_group') = 'numeric' \
              GROUP BY partition \
              ORDER BY hits DESC, partition ASC \
              LIMIT 10",
        )
        .unwrap();
    let sql_groups = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(sql_groups, [("a".into(), 5), ("b".into(), 4)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_uniq_is_textual_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("uniq-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage.ingest(numeric_pipeline_entries()).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | uniq by (first_partition) with hits"#,
        )
        .await,
        [
            serde_json::json!({"first_partition":"a","hits":"5"}),
            serde_json::json!({"first_partition":"b","hits":"4"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | uniq numeric_group, first_partition hits"#,
        )
        .await,
        [
            serde_json::json!({"numeric_group":"numeric","first_partition":"a","hits":"5"}),
            serde_json::json!({"numeric_group":"numeric","first_partition":"b","hits":"4"}),
        ]
    );
    let values = pipeline_rows(&app, r#"numeric_group:="numeric" | uniq by (n) hits"#).await;
    assert_eq!(values.len(), 8);
    assert_eq!(
        values.iter().find(|row| row.get("n").is_none()),
        Some(&serde_json::json!({"hits":"2"}))
    );
    assert!(values.iter().all(|row| row["hits"].is_string()));
    assert!(values.contains(&serde_json::json!({"n":"9007199254740993","hits":"1"})));
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | uniq by (n) filter 2 with hits"#,
        )
        .await,
        [
            serde_json::json!({"n":"-2","hits":"1"}),
            serde_json::json!({"n":"2","hits":"1"}),
            serde_json::json!({"n":"2.5","hits":"1"}),
            serde_json::json!({"n":"9007199254740993","hits":"1"}),
        ]
    );
    assert_eq!(
        pipeline_rows(&app, r#"numeric_group:="numeric" | uniq by (hits) hits"#).await,
        [serde_json::json!({"hitss":"9"})]
    );
    let overflow = pipeline_rows(
        &app,
        r#"numeric_group:="numeric" | uniq by (n) with hits limit 2"#,
    )
    .await;
    assert_eq!(overflow.len(), 2);
    assert!(overflow.iter().all(|row| row["hits"] == "0"));
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | filter first_partition:="a" | uniq by (first_partition) hits"#,
        )
        .await,
        [serde_json::json!({"first_partition":"a","hits":"5"})]
    );
    assert!(
        pipeline_rows(&app, r#"case:="uniq-missing" | uniq by (case) hits"#)
            .await
            .is_empty()
    );

    for malformed in [
        "* | uniq",
        "* | uniq hits",
        "* | uniq by",
        "* | uniq by ()",
        "* | uniq by (case*)",
        "* | uniq case level",
        "* | uniq by (case) filter",
        "* | uniq by (case, level) filter x",
        "* | uniq by (case) with",
        "* | uniq by (case) limit",
        "* | uniq by (case) limit nope",
        "* | uniq by (case) with hits trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 4,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | uniq by (n)"#,
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 2,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | uniq by (n) limit 3"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 64,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | uniq by (n)"#,
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason);
    }
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | uniq by (first_partition) hits"#,
        )
        .await,
        [
            serde_json::json!({"first_partition":"a","hits":"5"}),
            serde_json::json!({"first_partition":"b","hits":"4"}),
        ],
        "the reader remains reusable after uniq limit rejections"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"numeric_group:="numeric" | UNIQ BY (first_partition) WITH HITS LIMIT 2"#,
        )
        .await,
        [
            serde_json::json!({"first_partition":"a","hits":"5"}),
            serde_json::json!({"first_partition":"b","hits":"4"}),
        ]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_facets_are_flattened_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("facets-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let mut entries = numeric_pipeline_entries();
    for (index, (case, probe)) in [
        ("state-missing", None),
        ("state-null", Some(serde_json::Value::Null)),
        ("state-empty", Some(serde_json::json!(""))),
        ("state-string", Some(serde_json::json!("value"))),
        ("state-zero", Some(serde_json::json!(0))),
        ("state-false", Some(serde_json::json!(false))),
    ]
    .into_iter()
    .enumerate()
    {
        let mut metadata = serde_json::json!({"case":case,"state_group":"state"});
        if let Some(probe) = probe {
            metadata["probe"] = probe;
        }
        entries.push(LogEntry {
            ts: 1_800_000_000_001_000 + index as i64,
            level: 1,
            severity: "info".into(),
            message: case.replace('-', " "),
            metadata_json: serde_json::to_string(&metadata).unwrap(),
        });
    }
    storage.ingest(entries).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | fields first_partition, n | facets 2"#,
        )
        .await,
        [
            serde_json::json!({"field_name":"first_partition","field_value":"a","hits":"5"}),
            serde_json::json!({"field_name":"first_partition","field_value":"b","hits":"4"}),
            serde_json::json!({"field_name":"n","field_value":"-2","hits":"1"}),
            serde_json::json!({"field_name":"n","field_value":"0","hits":"1"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | fields numeric_group, first_partition | facets 1 keep_const_fields"#,
        )
        .await,
        [
            serde_json::json!({"field_name":"first_partition","field_value":"a","hits":"5"}),
            serde_json::json!({"field_name":"numeric_group","field_value":"numeric","hits":"9"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | fields first_partition, n | facets max_values_per_field 2"#,
        )
        .await,
        [
            serde_json::json!({"field_name":"first_partition","field_value":"a","hits":"5"}),
            serde_json::json!({"field_name":"first_partition","field_value":"b","hits":"4"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"state_group:="state" | fields case, probe | facets max_value_len 5"#,
        )
        .await,
        [
            serde_json::json!({"field_name":"probe","field_value":"0","hits":"1"}),
            serde_json::json!({"field_name":"probe","field_value":"false","hits":"1"}),
            serde_json::json!({"field_name":"probe","field_value":"value","hits":"1"}),
        ]
    );
    let nested = pipeline_rows(
        &app,
        r#"case:in(numeric-two,numeric-ten) | fields nested.case | facets keep_const_fields"#,
    )
    .await;
    assert_eq!(
        nested,
        [
            serde_json::json!({"field_name":"nested.case","field_value":"numeric-ten","hits":"1"}),
            serde_json::json!({"field_name":"nested.case","field_value":"numeric-two","hits":"1"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"numeric_group:="numeric" | fields first_partition | facets 1.9"#,
        )
        .await,
        [serde_json::json!({"field_name":"first_partition","field_value":"a","hits":"5"})]
    );
    assert!(pipeline_rows(&app, r#"case:="facets-missing" | facets"#)
        .await
        .is_empty());

    for malformed in [
        "* | facets 0",
        "* | facets -1",
        "* | facets nope",
        "* | facets max_values_per_field",
        "* | facets max_values_per_field 0",
        "* | facets max_values_per_field nope",
        "* | facets max_value_len",
        "* | facets max_value_len 0",
        "* | facets max_value_len nope",
        "* | facets keep_const_fields trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 4,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | facets"#,
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 2,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | fields first_partition, n | facets"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 64,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | fields first_partition | facets"#,
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason);
    }
    assert_eq!(
        pipeline_rows(&app, r#"case:="numeric-two" | fields nested, n"#,).await,
        [serde_json::json!({"n":2,"nested":{"case":"numeric-two"}})],
        "faceting must not mutate retained rich metadata and the reader remains reusable"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"numeric_group:="numeric" | fields numeric_group, first_partition | FACETS 1 KEEP_CONST_FIELDS"#,
        )
        .await,
        [
            serde_json::json!({"field_name":"first_partition","field_value":"a","hits":"5"}),
            serde_json::json!({"field_name":"numeric_group","field_value":"numeric","hits":"9"}),
        ]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_coalesce_is_textual_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("coalesce-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let mut entries = numeric_pipeline_entries();
    for (index, (case, probe)) in [
        ("state-missing", None),
        ("state-null", Some(serde_json::Value::Null)),
        ("state-empty", Some(serde_json::json!(""))),
        ("state-string", Some(serde_json::json!("value"))),
        ("state-zero", Some(serde_json::json!(0))),
        ("state-false", Some(serde_json::json!(false))),
        ("state-array", Some(serde_json::json!([1, "x"]))),
        ("state-object", Some(serde_json::json!({"child":"nested"}))),
    ]
    .into_iter()
    .enumerate()
    {
        let mut metadata = serde_json::json!({"case":case,"state_group":"state"});
        if let Some(probe) = probe {
            metadata["probe"] = probe;
        }
        entries.push(LogEntry {
            ts: 1_800_000_000_001_000 + index as i64,
            level: 1,
            severity: "info".into(),
            message: case.replace('-', " "),
            metadata_json: serde_json::to_string(&metadata).unwrap(),
        });
    }
    storage.ingest(entries).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"state_group:="state" | coalesce(probe, case) as selected | fields case, selected"#,
        )
        .await,
        [
            serde_json::json!({"case":"state-missing","selected":"state-missing"}),
            serde_json::json!({"case":"state-null","selected":"state-null"}),
            serde_json::json!({"case":"state-empty","selected":"state-empty"}),
            serde_json::json!({"case":"state-string","selected":"value"}),
            serde_json::json!({"case":"state-zero","selected":"0"}),
            serde_json::json!({"case":"state-false","selected":"false"}),
            serde_json::json!({"case":"state-array","selected":"[1,\"x\"]"}),
            serde_json::json!({"case":"state-object","selected":"state-object"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="state-missing" | coalesce(probe) default "fallback value" | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({"case":"state-missing","_msg":"fallback value"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="numeric-two" | coalesce(nested*, case) as selected | fields case, selected"#,
        )
        .await,
        [serde_json::json!({"case":"numeric-two","selected":"numeric-two"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="numeric-two" | fields n | coalesce(*) as selected"#,
        )
        .await,
        [serde_json::json!({"n":2,"selected":"2"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="numeric-two" | coalesce(n, n*, case) as selected | fields case, selected"#,
        )
        .await,
        [serde_json::json!({"case":"numeric-two","selected":"2"})]
    );
    assert!(pipeline_rows(
        &app,
        r#"case:="coalesce-missing" | coalesce(case) as selected"#,
    )
    .await
    .is_empty());

    for malformed in [
        "* | coalesce",
        "* | coalesce a, b",
        "* | coalesce()",
        "* | coalesce(,a)",
        "* | coalesce(a,,b)",
        "* | coalesce(a",
        "* | coalesce(a) default",
        "* | coalesce(a) default count() as result",
        "* | coalesce(a) as",
        "* | coalesce(a) as result*",
        "* | coalesce(a) trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 4,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | coalesce(n, case) as selected | limit 9"#,
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 4,
                ..LogsQueryLimits::default()
            },
            r#"numeric_group:="numeric" | coalesce(n, case) as selected | limit 9"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 64,
                ..LogsQueryLimits::default()
            },
            r#"case:="numeric-two" | coalesce(n, case) as selected"#,
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason);
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="state-string" | coalesce(case) as probe.child"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict_body = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict_body["error"], "query_execution");
    assert_eq!(conflict_body["reason"], "field_conflict");
    assert!(conflict_body["message"]
        .as_str()
        .unwrap()
        .contains("conflicts with a scalar field"));
    assert_eq!(
        pipeline_rows(&app, r#"case:="numeric-two" | fields nested, n"#).await,
        [serde_json::json!({"n":2,"nested":{"case":"numeric-two"}})],
        "coalesce must not mutate rich source fields and the reader remains reusable"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="numeric-two" | COALESCE(n, case,) DEFAULT fallback AS selected | fields case, selected"#,
        )
        .await,
        [serde_json::json!({"case":"numeric-two","selected":"2"})]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_copy_is_typed_sequential_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("copy-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let entries = [
        LogEntry {
            ts: 1_800_000_000_000_001,
            level: 1,
            severity: "info".into(),
            message: "copy rich".into(),
            metadata_json: serde_json::json!({
                "case":"copy-rich",
                "copy_group":"copy",
                "probe":2,
                "flag":false,
                "nested":{"a":"one","b":[1,"x"]},
                "null_value":null,
                "empty_value":""
            })
            .to_string(),
        },
        LogEntry {
            ts: 1_800_000_000_000_002,
            level: 1,
            severity: "info".into(),
            message: "copy missing".into(),
            metadata_json: serde_json::json!({
                "case":"copy-missing",
                "copy_group":"copy"
            })
            .to_string(),
        },
    ];
    storage.ingest(entries.into()).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | copy probe as selected, flag flag_copy, nested.b as array_copy, null_value as null_copy, empty_value as empty_copy, missing as missing_copy | fields case, probe, selected, flag_copy, array_copy, null_copy, empty_copy, missing_copy"#,
        )
        .await,
        [serde_json::json!({
            "case":"copy-rich",
            "probe":2,
            "selected":2,
            "flag_copy":false,
            "array_copy":[1,"x"],
            "null_copy":null,
            "empty_copy":"",
            "missing_copy":""
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | CP probe first, first AS second, case selected, probe selected | fields case, first, second, selected"#,
        )
        .await,
        [serde_json::json!({"case":"copy-rich","first":2,"second":2,"selected":2})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | copy nested.* as copied.* | fields case, copied.a, copied.b"#,
        )
        .await,
        [serde_json::json!({"case":"copy-rich","copied":{"a":"one","b":[1,"x"]}})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | copy nested.* as selected | fields case, selected"#,
        )
        .await,
        [serde_json::json!({"case":"copy-rich","selected":[1,"x"]})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | copy nested as object_parent_copy, absent* as copied* | fields case, object_parent_copy"#,
        )
        .await,
        [serde_json::json!({"case":"copy-rich","object_parent_copy":""})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | copy probe as probe, case as saved, probe as case, saved as probe | fields case, probe"#,
        )
        .await,
        [serde_json::json!({"case":2,"probe":"copy-rich"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | copy * as * | fields case, probe, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"copy-rich",
            "probe":2,
            "nested":{"a":"one","b":[1,"x"]}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | copy * as copied.* | fields copied.case, copied.probe, copied.nested"#,
        )
        .await,
        [serde_json::json!({
            "copied":{
                "case":"copy-rich",
                "probe":2,
                "nested":{"a":"one","b":[1,"x"]}
            }
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | copy case* as * | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({"case":"copy-rich","_msg":"copy rich"})]
    );
    assert_eq!(
        pipeline_rows(&app, r#"case:="copy-rich" | fields case | copy case* as *"#,).await,
        [serde_json::json!({"case":"copy-rich","":"copy-rich"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | copy probe* as copied*, copied* as chained* | fields case, copied, chained"#,
        )
        .await,
        [serde_json::json!({"case":"copy-rich","copied":2,"chained":2})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | copy probe as selected* | fields case, "selected*""#,
        )
        .await,
        [serde_json::json!({"case":"copy-rich","selected*":2})]
    );
    assert!(pipeline_rows(
        &app,
        r#"case:="copy-does-not-exist" | copy case as selected"#,
    )
    .await
    .is_empty());

    for malformed in [
        "* | copy",
        "* | copy source",
        "* | copy source as",
        "* | copy , source as destination",
        "* | copy source as destination,",
        "* | copy source as destination trailing",
        "* | copy source * as destination",
        "* | copy source as destination *",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for query in [
        r#"case:="copy-rich" | copy case as probe.child"#,
        r#"case:="copy-rich" | copy probe as nested"#,
    ] {
        let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["error"], "query_execution");
        assert_eq!(body["reason"], "field_conflict");
    }

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"copy_group:="copy" | copy case as selected | limit 2"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 64,
                ..LogsQueryLimits::default()
            },
            r#"case:="copy-rich" | copy * as copied*"#,
            "max_response_bytes",
        ),
        (
            LogsQueryLimits {
                max_work_rows: 8,
                ..LogsQueryLimits::default()
            },
            r#"case:="copy-rich" | copy probe as a, probe as b, probe as c, probe as d, probe as e, probe as f, probe as g, probe as h, probe as i"#,
            "max_work_rows",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason);
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="copy-rich" | fields case, probe, nested, null_value, empty_value"#,
        )
        .await,
        [serde_json::json!({
            "case":"copy-rich",
            "probe":2,
            "nested":{"a":"one","b":[1,"x"]},
            "null_value":null,
            "empty_value":""
        })],
        "copy must not mutate rich source fields and the reader remains reusable"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="copy-rich" | COPY nested.* AS copied.* | fields case, copied.a, copied.b"#,
        )
        .await,
        [serde_json::json!({"case":"copy-rich","copied":{"a":"one","b":[1,"x"]}})]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_rename_is_typed_sequential_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("rename-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let entries = [
        LogEntry {
            ts: 1_800_000_000_000_001,
            level: 1,
            severity: "info".into(),
            message: "rename rich".into(),
            metadata_json: serde_json::json!({
                "case":"rename-rich",
                "rename_group":"rename",
                "probe":2,
                "flag":false,
                "nested":{"a":"one","b":[1,"x"]},
                "null_value":null,
                "empty_value":""
            })
            .to_string(),
        },
        LogEntry {
            ts: 1_800_000_000_000_002,
            level: 1,
            severity: "info".into(),
            message: "rename missing".into(),
            metadata_json: serde_json::json!({
                "case":"rename-missing",
                "rename_group":"rename"
            })
            .to_string(),
        },
    ];
    storage.ingest(entries.into()).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | rename probe as selected, flag flag_moved, nested.b as array_moved, null_value as null_moved, empty_value as empty_moved, missing as missing_moved | fields case, probe, flag, nested.b, null_value, empty_value, selected, flag_moved, array_moved, null_moved, empty_moved, missing_moved"#,
        )
        .await,
        [serde_json::json!({
            "case":"rename-rich",
            "selected":2,
            "flag_moved":false,
            "array_moved":[1,"x"],
            "null_moved":null,
            "empty_moved":"",
            "missing_moved":""
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | MV case saved, probe AS case, saved probe | fields case, probe, saved"#,
        )
        .await,
        [serde_json::json!({"case":2,"probe":"rename-rich"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | rename probe first, probe second | fields case, probe, first, second"#,
        )
        .await,
        [serde_json::json!({"case":"rename-rich","first":2,"second":""})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | rename nested.* as moved.* | fields case, nested.a, nested.b, moved.a, moved.b"#,
        )
        .await,
        [serde_json::json!({"case":"rename-rich","moved":{"a":"one","b":[1,"x"]}})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | rename nested.* as selected | fields case, nested.a, nested.b, selected"#,
        )
        .await,
        [serde_json::json!({"case":"rename-rich","selected":[1,"x"]})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | rename nested as object_parent, absent* as moved* | fields case, nested, object_parent"#,
        )
        .await,
        [serde_json::json!({
            "case":"rename-rich",
            "nested":{"a":"one","b":[1,"x"]},
            "object_parent":""
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | rename * as * | fields case, probe, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"rename-rich",
            "probe":2,
            "nested":{"a":"one","b":[1,"x"]}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | rename * as moved.* | fields case, probe, moved.case, moved.probe, moved.nested"#,
        )
        .await,
        [serde_json::json!({
            "moved":{
                "case":"rename-rich",
                "probe":2,
                "nested":{"a":"one","b":[1,"x"]}
            }
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | fields case | rename case* as *"#,
        )
        .await,
        [serde_json::json!({"":"rename-rich"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | rename case* as * | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({"_msg":"rename rich"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | rename probe* as moved*, moved* as chained* | fields case, probe, moved, chained"#,
        )
        .await,
        [serde_json::json!({"case":"rename-rich","chained":2})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | rename probe as selected* | fields case, probe, "selected*""#,
        )
        .await,
        [serde_json::json!({"case":"rename-rich","selected*":2})]
    );
    assert!(pipeline_rows(
        &app,
        r#"case:="rename-does-not-exist" | rename case as selected"#,
    )
    .await
    .is_empty());

    for malformed in [
        "* | rename",
        "* | mv",
        "* | rename source",
        "* | rename source as",
        "* | rename , source as destination",
        "* | rename source as destination,",
        "* | rename source as destination trailing",
        "* | rename source * as destination",
        "* | rename source as destination *",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for query in [
        r#"case:="rename-rich" | rename case as probe.child"#,
        r#"case:="rename-rich" | rename probe as nested"#,
    ] {
        let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["error"], "query_execution");
        assert_eq!(body["reason"], "field_conflict");
        assert!(body["message"].as_str().unwrap().contains("LogsQL rename"));
    }

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"rename_group:="rename" | rename case as selected | limit 2"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 64,
                ..LogsQueryLimits::default()
            },
            r#"case:="rename-rich" | rename * as moved*"#,
            "max_response_bytes",
        ),
        (
            LogsQueryLimits {
                max_work_rows: 8,
                ..LogsQueryLimits::default()
            },
            r#"case:="rename-rich" | rename probe as a, probe as b, probe as c, probe as d, probe as e, probe as f, probe as g, probe as h, probe as i"#,
            "max_work_rows",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason);
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="rename-rich" | fields case, probe, nested, null_value, empty_value"#,
        )
        .await,
        [serde_json::json!({
            "case":"rename-rich",
            "probe":2,
            "nested":{"a":"one","b":[1,"x"]},
            "null_value":null,
            "empty_value":""
        })],
        "rename must not mutate stored rich source fields and the reader remains reusable"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="rename-rich" | RENAME nested.* AS moved.* | fields case, nested.a, nested.b, moved.a, moved.b"#,
        )
        .await,
        [serde_json::json!({"case":"rename-rich","moved":{"a":"one","b":[1,"x"]}})]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_format_is_complete_bounded_rich_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("format-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let entries = [
        LogEntry {
            ts: 1_800_000_000_000_001,
            level: 1,
            severity: "info".into(),
            message: "format rich".into(),
            metadata_json: serde_json::json!({
                "case":"format-rich",
                "format_group":"format",
                "host":"aцC",
                "lower":"aBП",
                "unicode_edge":"ßİ",
                "url":"a b+ц",
                "encoded_url":"a+b%2B%D1%86",
                "hex":"D099D0A6D0A3D09A",
                "b64":"YdGGQw==",
                "duration":"1h5m35s",
                "duration_ns":"210123456789",
                "duration_min":"-9223372036854775808",
                "unix_seconds_fraction":"1717328141.12",
                "unix_millis":"1717328141123",
                "unix_micros":"1717328141123456",
                "unix_ns":"1717328141123456789",
                "unix_scientific":"1.717328141123456789e18",
                "unix_plus":"+1717328141",
                "unix_negative":"-1717328141.123",
                "number":"1234",
                "hex_number":"00000000000004D2",
                "ipv4":"1234567890",
                "probe":2,
                "flag":false,
                "nested":{"a":"one","b":[1,"x"]},
                "null_value":null,
                "empty_value":"",
                "result":"original"
            })
            .to_string(),
        },
        LogEntry {
            ts: 1_800_000_000_000_002,
            level: 1,
            severity: "info".into(),
            message: "format missing".into(),
            metadata_json: serde_json::json!({
                "case":"format-missing",
                "format_group":"format"
            })
            .to_string(),
        },
    ];
    storage.ingest(entries.into()).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="format-rich" | format '&lt;<uc:host>&gt;|<lc:lower>|<q:_msg>|<urlencode:url>|<urldecode:encoded_url>|<hexdecode:hex>|<base64decode:b64>|<duration_seconds:duration>|<duration:duration_ns>|<time:unix_ns>|<hexnumencode:number>|<hexnumdecode:hex_number>|<ipv4:ipv4>|<probe>|<flag>|<nested.b>|<null_value><_><*><>' as rendered | fields case, rendered"#,
        )
        .await,
        [serde_json::json!({
            "case":"format-rich",
            "rendered":"<AЦC>|abп|\"format rich\"|a+b%2B%D1%86|a b+ц|ЙЦУК|aцC|3935|3m30.123456789s|2024-06-02T11:35:41.123456789Z|00000000000004D2|1234|73.150.2.210|2|false|[1,\"x\"]|"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="format-rich" | format '<time:unix_seconds_fraction>|<time:unix_millis>|<time:unix_micros>|<time:unix_scientific>|<time:unix_plus>|<time:unix_negative>|<uc:unicode_edge>|<lc:unicode_edge>|<duration:duration_min>' as rendered | fields case, rendered"#,
        )
        .await,
        [serde_json::json!({
            "case":"format-rich",
            "rendered":"2024-06-02T11:35:41.12Z|2024-06-02T11:35:41.123Z|2024-06-02T11:35:41.123456Z|2024-06-02T11:35:41.123456789Z|2024-06-02T11:35:41Z|1915-08-01T12:24:18.877Z|ßİ|ßi|-"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"format_group:="format" | format if (host:*) 'matched <host>' as result | fields case, result"#,
        )
        .await,
        [
            serde_json::json!({"case":"format-rich","result":"matched aцC"}),
            serde_json::json!({"case":"format-missing"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="format-rich" | format 'replacement' as result keep_original_fields | fields case, result"#,
        )
        .await,
        [serde_json::json!({"case":"format-rich","result":"original"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="format-missing" | format 'replacement' as result keep_original_fields | fields case, result"#,
        )
        .await,
        [serde_json::json!({"case":"format-missing","result":"replacement"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="format-rich" | format '<missing>' as result skip_empty_results | fields case, result"#,
        )
        .await,
        [serde_json::json!({"case":"format-rich","result":"original"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="format-missing" | format '<missing>' as result | fields case, result"#,
        )
        .await,
        [serde_json::json!({"case":"format-missing","result":""})],
        "Timeless keeps an explicit empty formatted destination distinct from a missing rich field"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="format-rich" | format '<probe>' as probe keep_original_fields | fields case, probe"#,
        )
        .await,
        [serde_json::json!({"case":"format-rich","probe":2})],
        "preservation modes retain the original rich type"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="format-rich" | format if () 'always' as rendered | fields case, rendered"#,
        )
        .await,
        [serde_json::json!({"case":"format-rich","rendered":"always"})]
    );

    for malformed in [
        "* | format",
        "* | format if",
        "* | format if (host:*)",
        r#"* | format "<unterminated""#,
        r#"* | format "<field*>""#,
        "* | format value as",
        "* | format value as result*",
        "* | format value keep_original_fields skip_empty_results",
        "* | format value trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="format-rich" | format 'replacement' as nested"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["error"], "query_execution");
    assert_eq!(conflict["reason"], "field_conflict");
    assert!(conflict["message"]
        .as_str()
        .unwrap()
        .contains("LogsQL format destination conflict"));

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"format_group:="format" | format '<case>' as rendered | limit 2"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 64,
                ..LogsQueryLimits::default()
            },
            r#"case:="format-rich" | format '<urlencode:url><urlencode:url><urlencode:url>' as rendered"#,
            "max_response_bytes",
        ),
        (
            LogsQueryLimits {
                max_work_rows: 2,
                ..LogsQueryLimits::default()
            },
            r#"case:="format-rich" | format '<host><lower><url>' as rendered"#,
            "max_work_rows",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{query}"
        );
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{query}: {body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="format-rich" | fields case, probe, flag, nested, null_value, empty_value, result"#,
        )
        .await,
        [serde_json::json!({
            "case":"format-rich",
            "probe":2,
            "flag":false,
            "nested":{"a":"one","b":[1,"x"]},
            "null_value":null,
            "empty_value":"",
            "result":"original"
        })],
        "format must not mutate stored rich source fields and the reader remains reusable"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="format-rich" | FORMAT '<uc:host>|<nested.b>' AS rendered | fields case, rendered"#,
        )
        .await,
        [serde_json::json!({"case":"format-rich","rendered":"AЦC|[1,\"x\"]"})]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_math_is_sequential_typed_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("math-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "math rich".into(),
                    metadata_json: serde_json::json!({
                        "case":"math-rich",
                        "a":"2",
                        "b":"3",
                        "negative":-2,
                        "bad":"nope",
                        "empty":"",
                        "duration":"10m5s",
                        "bytes":"1.5KiB",
                        "timestamp":"2024-05-30T01:02:03Z",
                        "ipv4":"123.45.67.89",
                        "math abs":"7",
                        "math left field":"2",
                        "result":"original",
                        "nested":{"value":"4"},
                        "source_array":[1,2],
                        "source_null":null
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: r#"["second"]"#.into(),
                    metadata_json: serde_json::json!({
                        "case":"json-array-concat-second",
                        "array_concat_group":"json-array-concat",
                        "native_array":["second"]
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="math-rich" | eval a + 1 first, first * b as second, second + nested.value as total | fields case, first, second, total, source_array, source_null"#,
        )
        .await,
        [serde_json::json!({
            "case":"math-rich",
            "first":"3",
            "second":"9",
            "total":"13",
            "source_array":[1,2],
            "source_null":null
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="math-rich" | math a + b * 4 as precedence, (a + b) * 4 wrapped, 2 ^ 3 ^ 2 power, abs(-b) absolute, ceil(2.1) ceiling, floor(2.9) floored, round(2.5) rounded, exp(1) exponential, ln(exp(1)) logarithm, max(a, bad, b) maximum, min(b, bad, a) minimum | fields case, precedence, wrapped, power, absolute, ceiling, floored, rounded, exponential, logarithm, maximum, minimum"#,
        )
        .await,
        [serde_json::json!({
            "case":"math-rich",
            "precedence":"14",
            "wrapped":"20",
            "power":"64",
            "absolute":"3",
            "ceiling":"3",
            "floored":"2",
            "rounded":"3",
            "exponential":"2.718281828459045",
            "logarithm":"1",
            "maximum":"3",
            "minimum":"2"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="math-rich" | math round(3.14159, 0.01) rounded, round(-3.14159, 0.01) negative_rounded, -5 % 2 signed_remainder, 5.5 % 2 fractional_remainder, a ^ b pow, 7 & 3 band, 4 or 1 bor, 6 xor 3 bxor, bad default 5 fallback | fields case, rounded, negative_rounded, signed_remainder, fractional_remainder, pow, band, bor, bxor, fallback"#,
        )
        .await,
        [serde_json::json!({
            "case":"math-rich",
            "rounded":"3.14",
            "negative_rounded":"-3.14",
            "signed_remainder":"-1",
            "fractional_remainder":"1.5",
            "pow":"8",
            "band":"3",
            "bor":"5",
            "bxor":"5",
            "fallback":"5"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="math-rich" | math duration + 10e9 duration_result, bytes + 512B bytes_result, timestamp + 10e9 time_result, ipv4 + 1000 ip_result, empty default 1 empty_result, source_null default 2 null_result, missing default 3 missing_result, source_array default 4 array_result, "math abs" + "math left field" + nested.value quoted_total | fields case, duration_result, bytes_result, time_result, ip_result, empty_result, null_result, missing_result, array_result, quoted_total"#,
        )
        .await,
        [serde_json::json!({
            "case":"math-rich",
            "duration_result":"615000000000",
            "bytes_result":"2048",
            "time_result":"1717030933000000000",
            "ip_result":"2066564929",
            "empty_result":"1",
            "null_result":"2",
            "missing_result":"3",
            "array_result":"4",
            "quoted_total":"13"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="math-rich" | math '2024-05-30T01:02:03Z' + 10e9 quoted_time, '123.45.67.89' + 1000 quoted_ip, -1.5K scaled, -45ms negative_duration, max(a, b,) maximum, round(3.14,) rounded | fields case, quoted_time, quoted_ip, scaled, negative_duration, maximum, rounded"#,
        )
        .await,
        [serde_json::json!({
            "case":"math-rich",
            "quoted_time":"1717030933000000000",
            "quoted_ip":"2066564929",
            "scaled":"-1500",
            "negative_duration":"-45000000",
            "maximum":"3",
            "rounded":"3"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="math-rich" | math a / b default 10 | fields case, "a / b default 10""#,
        )
        .await,
        [serde_json::json!({
            "case":"math-rich",
            "a / b default 10":"0.6666666666666666"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="math-rich" | math 1e20 huge, 0.000000001 tiny, 1 / 0 positive_inf, -1 / 0 negative_inf, 0 / 0 not_a_number, (1 / 0) default 7 inf_default, a + as result, a* as adjacent_result, negative & 3 negative_and, (1 / 0) & 1 inf_and, 18446744073709551616 & 1 overflow_and, -1 or 1 negative_or | fields case, huge, tiny, positive_inf, negative_inf, not_a_number, inf_default, result, adjacent_result, negative_and, inf_and, overflow_and, negative_or"#,
        )
        .await,
        [serde_json::json!({
            "case":"math-rich",
            "huge":"100000000000000000000",
            "tiny":"0.000000001",
            "positive_inf":"+Inf",
            "negative_inf":"-Inf",
            "not_a_number":"NaN",
            "inf_default":"+Inf",
            "result":"NaN",
            "adjacent_result":"NaN",
            "negative_and":"2",
            "inf_and":"0",
            "overflow_and":"0",
            "negative_or":"18446744073709552000"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="math-rich" | math duration + 10e9 duration_result | format '<duration:duration_result>' as rendered | fields case, rendered"#,
        )
        .await,
        [serde_json::json!({"case":"math-rich","rendered":"10m15s"})]
    );

    let volatile = pipeline_rows(
        &app,
        r#"case:="math-rich" | math now() clock, floor(rand()) bucket | fields case, clock, bucket"#,
    )
    .await;
    let clock = volatile[0]["clock"]
        .as_str()
        .unwrap()
        .parse::<f64>()
        .unwrap();
    assert!(clock > 1_000_000_000_000_000_000.0, "{volatile:?}");
    assert_eq!(volatile[0]["bucket"], "0");

    for malformed in [
        "* | math",
        "* | eval",
        "* | math * as result",
        "* | math (a + b as result",
        "* | math source as result*",
        "* | math source as",
        "* | math a as x,, b as y",
        "* | math a as x,",
        "* | math abs(a, b) as x",
        "* | math abs() as x",
        "* | math min(a) as x",
        "* | math max() as x",
        "* | math max(a) as x",
        "* | math round() as x",
        "* | math round(a, b, c) as x",
        "* | math rand(1) as x",
        "* | math now(1) as x",
        "* | math 1e-9 as x",
        "* | math abs value as x",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="math-rich" | math a + b as nested"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict");
    assert!(conflict["message"]
        .as_str()
        .unwrap()
        .contains("LogsQL math destination conflict"));

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 2,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(
                r#"case:="math-rich" | math a + b as selected | fields selected"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="math-rich" | fields case, a, b, negative, bad, nested, source_array, source_null, result"#,
        )
        .await,
        [serde_json::json!({
            "case":"math-rich",
            "a":"2",
            "b":"3",
            "negative":-2,
            "bad":"nope",
            "nested":{"value":"4"},
            "source_array":[1,2],
            "source_null":null,
            "result":"original"
        })],
        "math must not mutate durable rich source values"
    );
    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="math-rich" | EVAL ROUND(ABS(-2.5)) AS result_value | fields case, result_value"#,
        )
        .await,
        [serde_json::json!({"case":"math-rich","result_value":"3"})]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_len_counts_textual_bytes_and_preserves_rich_sources() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("len-pipe-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "hello \"world\"\nnext".into(),
                    metadata_json: serde_json::json!({
                        "case":"len-rich",
                        "len_group":"len",
                        "unicode":"ßİ",
                        "number":9007199254740993u64,
                        "flag":false,
                        "empty":"",
                        "null_value":null,
                        "array":["x",1],
                        "nested":{"child":"nested-gone"},
                        "result":"original"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "missing".into(),
                    metadata_json: serde_json::json!({
                        "case":"len-missing",
                        "len_group":"len"
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="len-rich" | len(unicode) byte_len | len(number) number_len | len(flag) flag_len | len(empty) empty_len | len(null_value) null_len | len(missing) missing_len | len(array) array_len | len(nested) parent_len | len(nested.child) leaf_len | len(_time) time_len | fields case, byte_len, number_len, flag_len, empty_len, null_len, missing_len, array_len, parent_len, leaf_len, time_len"#,
        )
        .await,
        [serde_json::json!({
            "case":"len-rich",
            "byte_len":"4",
            "number_len":"16",
            "flag_len":"5",
            "empty_len":"0",
            "null_len":"0",
            "missing_len":"0",
            "array_len":"7",
            "parent_len":"0",
            "leaf_len":"11",
            "time_len":"27"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="len-rich" | len("") as message_len | len(unicode) as "" | fields case, message_len, _msg"#,
        )
        .await,
        [serde_json::json!({"case":"len-rich","message_len":"18","_msg":"4"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="len-rich" | LEN unicode AS unicode | len(unicode) second | len(result) as | fields case, unicode, second, _msg"#,
        )
        .await,
        [serde_json::json!({
            "case":"len-rich",
            "unicode":"4",
            "second":"1",
            "_msg":"8"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="len-missing" | len(missing) as computed.value | fields case, computed.value"#,
        )
        .await,
        [serde_json::json!({"case":"len-missing","computed":{"value":"0"}})]
    );

    for malformed in [
        "* | len",
        "* | len(",
        "* | len()",
        "* | len(source",
        "* | len(source, other)",
        "* | len(*)",
        "* | len(source*)",
        "* | len(source) as result*",
        "* | len(source) result trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="len-rich" | len(unicode) as nested"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["error"], "query_execution");
    assert_eq!(conflict["reason"], "field_conflict");
    assert!(conflict["message"]
        .as_str()
        .unwrap()
        .contains("LogsQL len destination conflict"));

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"len_group:="len" | len(_msg) result | limit 2"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 1,
                ..LogsQueryLimits::default()
            },
            r#"case:="len-rich" | len(unicode) result | fields result"#,
            "max_response_bytes",
        ),
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"case:="len-rich" | len(array) result | fields result"#,
            "max_work_rows",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{query}"
        );
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{query}: {body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="len-rich" | fields case, unicode, number, flag, empty, null_value, array, nested, result"#,
        )
        .await,
        [serde_json::json!({
            "case":"len-rich",
            "unicode":"ßİ",
            "number":9007199254740993u64,
            "flag":false,
            "empty":"",
            "null_value":null,
            "array":["x",1],
            "nested":{"child":"nested-gone"},
            "result":"original"
        })],
        "len must not mutate durable rich source values and the reader remains reusable"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="len-rich" | LEN ( unicode ) AS byte_len | fields case, byte_len, unicode, array, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"len-rich",
            "byte_len":"4",
            "unicode":"ßİ",
            "array":["x",1],
            "nested":{"child":"nested-gone"}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_hash_matches_xxhash53_and_preserves_rich_sources() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("hash-pipe-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "abc".into(),
                    metadata_json: serde_json::json!({
                        "case":"hash-rich",
                        "hash_group":"hash",
                        "source":"abcde",
                        "empty":"",
                        "null_value":null,
                        "number":9007199254740993u64,
                        "flag":false,
                        "array":["x",1],
                        "nested":{"child":"leaf"},
                        "result":"original"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "missing".into(),
                    metadata_json: serde_json::json!({
                        "case":"hash-missing",
                        "hash_group":"hash"
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="hash-rich" | hash(source) source_hash | hash(empty) empty_hash | hash(null_value) null_hash | hash(missing) missing_hash | hash(number) number_hash | hash(flag) flag_hash | hash(array) array_hash | hash(nested) parent_hash | hash(nested.child) child_hash | fields case, source_hash, empty_hash, null_hash, missing_hash, number_hash, flag_hash, array_hash, parent_hash, child_hash"#,
        )
        .await,
        [serde_json::json!({
            "case":"hash-rich",
            "source_hash":"957726378018795",
            "empty_hash":"1929880503118233",
            "null_hash":"1929880503118233",
            "missing_hash":"1929880503118233",
            "number_hash":"2140486140596034",
            "flag_hash":"8894828964231802",
            "array_hash":"7056501881616813",
            "parent_hash":"1929880503118233",
            "child_hash":"6957220181421943"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="hash-rich" | HASH ( source ) AS source | math source + 1 as adjusted | hash("") as message_hash | fields case, source, adjusted, message_hash"#,
        )
        .await,
        [serde_json::json!({
            "case":"hash-rich",
            "source":"957726378018795",
            "adjusted":"957726378018796",
            "message_hash":"7930733036767641"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="hash-missing" | hash(missing) as computed.value | fields case, computed.value"#,
        )
        .await,
        [serde_json::json!({
            "case":"hash-missing",
            "computed":{"value":"1929880503118233"}
        })]
    );

    for malformed in [
        "* | hash",
        "* | hash(",
        "* | hash()",
        "* | hash(source",
        "* | hash(source, other)",
        "* | hash(*)",
        "* | hash(source*)",
        "* | hash(source) as result*",
        "* | hash(source) result trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="hash-rich" | hash(source) as nested"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["error"], "query_execution");
    assert_eq!(conflict["reason"], "field_conflict");
    assert!(conflict["message"]
        .as_str()
        .unwrap()
        .contains("LogsQL hash destination conflict"));

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"hash_group:="hash" | hash(_msg) result | limit 2"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 1,
                ..LogsQueryLimits::default()
            },
            r#"case:="hash-rich" | hash(source) result | fields result"#,
            "max_response_bytes",
        ),
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"case:="hash-rich" | hash(array) result | fields result"#,
            "max_work_rows",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{query}"
        );
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{query}: {body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="hash-rich" | fields case, source, empty, null_value, number, flag, array, nested, result"#,
        )
        .await,
        [serde_json::json!({
            "case":"hash-rich",
            "source":"abcde",
            "empty":"",
            "null_value":null,
            "number":9007199254740993u64,
            "flag":false,
            "array":["x",1],
            "nested":{"child":"leaf"},
            "result":"original"
        })],
        "hash must not mutate durable rich source values and the reader remains reusable"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="hash-rich" | hash(source) as source_hash | fields case, source_hash, source, array, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"hash-rich",
            "source_hash":"957726378018795",
            "source":"abcde",
            "array":["x",1],
            "nested":{"child":"leaf"}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_collapse_nums_is_exact_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("collapse-nums-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "request 123".into(),
                    metadata_json: serde_json::json!({
                        "case":"collapse-rich",
                        "collapse_group":"collapse",
                        "kind":"admin",
                        "text":"a b c d e f ad be:eac,dead beef ab",
                        "unicode":"a1foo2bar34 ЫВА123bar45.78",
                        "duration":"0x1234 0XFEAD12 123ms 2us 3h5m6s43ms43μs324ns",
                        "pretty":"35.191.193.225:51648 - 2edfed59-3e98-4073-bbb2-28d321ca71a7 - - [2024/12/08 15:21:02] 10.71.20.32 GET /foo 200",
                        "number":9007199254740993u64,
                        "flag":false,
                        "array":["item",1,false],
                        "empty":"",
                        "null_value":null,
                        "nested":{"child":"leaf123"},
                        "conditional":"value 456",
                        "skipped":"value 789",
                        "result":"original"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 3,
                    severity: "emergency".into(),
                    message: "no numbers".into(),
                    metadata_json: serde_json::json!({
                        "case":"collapse-missing",
                        "collapse_group":"collapse",
                        "flag":false,
                        "null_value":null,
                        "nested":{"child":"unchanged"}
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="collapse-rich" | collapse_nums at text | collapse_nums at unicode | collapse_nums at duration | collapse_nums at pretty prettify | collapse_nums at number | collapse_nums at flag | collapse_nums at array | collapse_nums at empty | collapse_nums at null_value | collapse_nums at nested | collapse_nums at missing | collapse_nums if (kind:=admin) at conditional | collapse_nums if (kind:=other) at skipped | fields case, text, unicode, duration, pretty, number, flag, array, empty, null_value, nested, missing, conditional, skipped"#,
        )
        .await,
        [serde_json::json!({
            "case":"collapse-rich",
            "text":"a b c d e f ad be:eac,<N> <N> ab",
            "unicode":"a1foo2bar34 ЫВА123bar45.<N>",
            "duration":"0x<N> 0X<N> <N>ms <N>us <N>h<N>m<N>s<N>ms<N>μs<N>ns",
            "pretty":"<IP4>:<N> - <UUID> - - [<DATETIME>] <IP4> GET /foo <N>",
            "number":"<N>",
            "flag":false,
            "array":r#"["item",<N>,false]"#,
            "empty":"",
            "null_value":null,
            "nested":{"child":"leaf123"},
            "conditional":"value <N>",
            "skipped":"value 789"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="collapse-rich" | COLLAPSE_NUMS | collapse_nums at _msg prettify | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({"case":"collapse-rich","_msg":"request <N>"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="collapse-missing" | collapse_nums if () at missing | fields case, missing, flag, null_value, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"collapse-missing",
            "flag":false,
            "null_value":null,
            "nested":{"child":"unchanged"}
        })],
        "no-op projections preserve missing, null, bool, and rich-object states"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="collapse-rich" | collapse_nums at conditional | format '<conditional>' as rendered | fields case, conditional, rendered"#,
        )
        .await,
        [serde_json::json!({
            "case":"collapse-rich",
            "conditional":"value <N>",
            "rendered":"value <N>"
        })]
    );

    for malformed in [
        "* | collapse_nums foo",
        "* | collapse_nums()",
        "* | collapse_nums at",
        "* | collapse_nums at source*",
        "* | collapse_nums if",
        "* | collapse_nums if (kind:=admin",
        "* | collapse_nums prettify at source",
        "* | collapse_nums prettify if ()",
        "* | collapse_nums at source trailing",
        "* | collapse_nums.at source",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"collapse_group:="collapse" | collapse_nums | limit 2"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 1,
                ..LogsQueryLimits::default()
            },
            r#"case:="collapse-rich" | collapse_nums at text | fields text"#,
            "max_response_bytes",
        ),
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"case:="collapse-rich" | collapse_nums at array | fields array"#,
            "max_work_rows",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{query}"
        );
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{query}: {body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="collapse-rich" | fields case, _msg, text, unicode, duration, pretty, number, flag, array, empty, null_value, nested, conditional, skipped, result"#,
        )
        .await,
        [serde_json::json!({
            "case":"collapse-rich",
            "_msg":"request 123",
            "text":"a b c d e f ad be:eac,dead beef ab",
            "unicode":"a1foo2bar34 ЫВА123bar45.78",
            "duration":"0x1234 0XFEAD12 123ms 2us 3h5m6s43ms43μs324ns",
            "pretty":"35.191.193.225:51648 - 2edfed59-3e98-4073-bbb2-28d321ca71a7 - - [2024/12/08 15:21:02] 10.71.20.32 GET /foo 200",
            "number":9007199254740993u64,
            "flag":false,
            "array":["item",1,false],
            "empty":"",
            "null_value":null,
            "nested":{"child":"leaf123"},
            "conditional":"value 456",
            "skipped":"value 789",
            "result":"original"
        })],
        "collapse_nums must not mutate durable rich source values and the reader remains reusable"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="collapse-rich" | collapse_nums at pretty prettify | fields case, pretty, array, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"collapse-rich",
            "pretty":"<IP4>:<N> - <UUID> - - [<DATETIME>] <IP4> GET /foo <N>",
            "array":["item",1,false],
            "nested":{"child":"leaf123"}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_decolorize_is_exact_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("decolorize-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let invalid = "a\u{1b}[\u{1}b\u{1b}[abc\u{1b}[".to_owned();
    let osc = "\u{1b}]0;title\u{7}tail".to_owned();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "\u{1b}[mfoo\u{1b}[1;31mERROR bar\u{1b}[10;5H".into(),
                    metadata_json: serde_json::json!({
                        "case":"decolorize-rich",
                        "decolorize_group":"decolorize",
                        "source field":"left\u{1b}[2Jright",
                        "classes":"A\u{1b}[?25lB\u{1b}[1;2$zC",
                        "invalid":invalid,
                        "osc":osc,
                        "plain":"unchanged",
                        "number":9007199254740993u64,
                        "flag":false,
                        "array":["\u{1b}[31m",1],
                        "empty":"",
                        "null_value":null,
                        "object":{"child":"retained"},
                        "nested":{"source":"nested \u{1b}[32mgreen\u{1b}[0m"},
                        "many":"\u{1b}[m\u{1b}[m"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 3,
                    severity: "emergency".into(),
                    message: "plain".into(),
                    metadata_json: serde_json::json!({
                        "case":"decolorize-noop",
                        "decolorize_group":"decolorize",
                        "flag":false,
                        "null_value":null,
                        "object":{"child":"unchanged"}
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="decolorize-rich" | decolorize | DECOLORIZE "source field" | decolorize classes | decolorize invalid | decolorize osc | decolorize plain | decolorize number | decolorize flag | decolorize array | decolorize empty | decolorize null_value | decolorize object | decolorize missing | decolorize nested.source | format '<nested.source>' as rendered | fields case, _msg, "source field", classes, invalid, osc, plain, number, flag, array, empty, null_value, object, missing, nested, rendered"#,
        )
        .await,
        [serde_json::json!({
            "case":"decolorize-rich",
            "_msg":"fooERROR bar",
            "source field":"leftright",
            "classes":"ABC",
            "invalid":format!("a\u{1}bbc"),
            "osc":format!("\u{1b}]0;title\u{7}tail"),
            "plain":"unchanged",
            "number":9007199254740993u64,
            "flag":false,
            "array":["\u{1b}[31m",1],
            "empty":"",
            "null_value":null,
            "object":{"child":"retained"},
            "nested":{"source":"nested green"},
            "rendered":"nested green"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="decolorize-noop" | decolorize "" | decolorize missing | decolorize flag | decolorize null_value | decolorize object | fields case, _msg, missing, flag, null_value, object"#,
        )
        .await,
        [serde_json::json!({
            "case":"decolorize-noop",
            "_msg":"plain",
            "flag":false,
            "null_value":null,
            "object":{"child":"unchanged"}
        })],
        "no-op projections preserve missing, null, bool, and rich-object states"
    );

    for malformed in [
        "* | decolorize *",
        "* | decolorize source*",
        "* | decolorize (source)",
        "* | decolorize source, other",
        "* | decolorize source trailing",
        "* | decolorize.source",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"decolorize_group:="decolorize" | decolorize | limit 2"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 1,
                ..LogsQueryLimits::default()
            },
            r#"case:="decolorize-rich" | decolorize | fields _msg"#,
            "max_response_bytes",
        ),
        (
            LogsQueryLimits {
                max_work_rows: 2,
                ..LogsQueryLimits::default()
            },
            r#"case:="decolorize-rich" | decolorize many | fields many"#,
            "max_work_rows",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{query}"
        );
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{query}: {body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="decolorize-rich" | fields case, _msg, "source field", classes, invalid, osc, plain, number, flag, array, empty, null_value, object, nested, many"#,
        )
        .await,
        [serde_json::json!({
            "case":"decolorize-rich",
            "_msg":"\u{1b}[mfoo\u{1b}[1;31mERROR bar\u{1b}[10;5H",
            "source field":"left\u{1b}[2Jright",
            "classes":"A\u{1b}[?25lB\u{1b}[1;2$zC",
            "invalid":invalid,
            "osc":osc,
            "plain":"unchanged",
            "number":9007199254740993u64,
            "flag":false,
            "array":["\u{1b}[31m",1],
            "empty":"",
            "null_value":null,
            "object":{"child":"retained"},
            "nested":{"source":"nested \u{1b}[32mgreen\u{1b}[0m"},
            "many":"\u{1b}[m\u{1b}[m"
        })],
        "decolorize must not mutate durable rich source values after failures"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="decolorize-rich" | decolorize | decolorize nested.source | fields case, _msg, nested, array, object"#,
        )
        .await,
        [serde_json::json!({
            "case":"decolorize-rich",
            "_msg":"fooERROR bar",
            "nested":{"source":"nested green"},
            "array":["\u{1b}[31m",1],
            "object":{"child":"retained"}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_split_is_exact_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("split-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: ",foo,bar,,baz,".into(),
                    metadata_json: serde_json::json!({
                        "case":"split-rich",
                        "split_group":"split",
                        "unicode":"Шzч",
                        "multi":"йцуквенгшвевоы",
                        "none":"foobar",
                        "empty":"",
                        "number":9007199254740993u64,
                        "flag":false,
                        "array":["x",1],
                        "null_value":null,
                        "object":{"child":"retained"},
                        "escape":"<'\"\n\\",
                        "sequence":"a,b,c",
                        "many":",,",
                        "nested":{"source":"left::right","sibling":"retained"},
                        "result":"original"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 3,
                    severity: "emergency".into(),
                    message: "plain".into(),
                    metadata_json: serde_json::json!({
                        "case":"split-missing",
                        "split_group":"split",
                        "flag":false,
                        "null_value":null,
                        "object":{"child":"unchanged"}
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="split-rich" | split "," | SPLIT "" FrOm unicode As unicode_parts | split ве multi multi_parts | split aaaa none none_parts | split "," empty empty_parts | split "" empty empty_runes | split "," number number_parts | split "," flag flag_parts | split "," array array_parts | split "," null_value null_parts | split "," missing missing_parts | split "" object object_parts | split "" escape escape_parts | split "::" nested.source nested.parts | split "," sequence parts | json_array_len(parts) as part_count | fields case, _msg, unicode_parts, multi_parts, none_parts, empty_parts, empty_runes, number_parts, flag_parts, array_parts, null_parts, missing_parts, object_parts, escape_parts, nested, sequence, parts, part_count, number, flag, array, object"#,
        )
        .await,
        [serde_json::json!({
            "case":"split-rich",
            "_msg":r#"["","foo","bar","","baz",""]"#,
            "unicode_parts":r#"["Ш","z","ч"]"#,
            "multi_parts":r#"["йцук","нгш","воы"]"#,
            "none_parts":r#"["foobar"]"#,
            "empty_parts":r#"[""]"#,
            "empty_runes":"[]",
            "number_parts":r#"["9007199254740993"]"#,
            "flag_parts":r#"["false"]"#,
            "array_parts":r#"["[\"x\"","1]"]"#,
            "null_parts":r#"[""]"#,
            "missing_parts":r#"[""]"#,
            "object_parts":"[]",
            "escape_parts":r#"["\u003c","\u0027","\"","\n","\\"]"#,
            "nested":{
                "source":"left::right",
                "sibling":"retained",
                "parts":r#"["left","right"]"#
            },
            "sequence":"a,b,c",
            "parts":r#"["a","b","c"]"#,
            "part_count":"3",
            "number":9007199254740993u64,
            "flag":false,
            "array":["x",1],
            "object":{"child":"retained"}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="split-rich" | split "::" from nested.source | fields case, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"split-rich",
            "nested":{
                "source":r#"["left","right"]"#,
                "sibling":"retained"
            }
        })],
        "omitting as overwrites the selected current-row source"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="split-missing" | split "," missing parts | split "" absent runes | fields case, parts, runes, flag, null_value, object"#,
        )
        .await,
        [serde_json::json!({
            "case":"split-missing",
            "parts":r#"[""]"#,
            "runes":"[]",
            "flag":false,
            "null_value":null,
            "object":{"child":"unchanged"}
        })]
    );

    for malformed in [
        "* | split",
        "* | split as result",
        r#"* | split " " as *"#,
        r#"* | split " " as"#,
        r#"* | split " " from"#,
        r#"* | split " " from *"#,
        r#"* | split " " from source*"#,
        "* | split separator source, result",
        "* | split separator, result",
        r#"* | split(",")"#,
        r#"* | split.source from input as output"#,
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="split-rich" | split "," from _msg as object"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["error"], "query_execution");
    assert_eq!(conflict["reason"], "field_conflict");
    assert!(conflict["message"]
        .as_str()
        .unwrap()
        .contains("LogsQL split destination conflict"));

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"split_group:="split" | split "," | limit 2"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 1,
                ..LogsQueryLimits::default()
            },
            r#"case:="split-rich" | split "," many parts | fields parts"#,
            "max_response_bytes",
        ),
        (
            LogsQueryLimits {
                max_work_rows: 3,
                ..LogsQueryLimits::default()
            },
            r#"case:="split-rich" | split "," many parts | fields parts"#,
            "max_work_rows",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{query}"
        );
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{query}: {body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="split-rich" | fields case, _msg, unicode, multi, none, empty, number, flag, array, null_value, object, escape, sequence, many, nested, result"#,
        )
        .await,
        [serde_json::json!({
            "case":"split-rich",
            "_msg":",foo,bar,,baz,",
            "unicode":"Шzч",
            "multi":"йцуквенгшвевоы",
            "none":"foobar",
            "empty":"",
            "number":9007199254740993u64,
            "flag":false,
            "array":["x",1],
            "null_value":null,
            "object":{"child":"retained"},
            "escape":"<'\"\n\\",
            "sequence":"a,b,c",
            "many":",,",
            "nested":{"source":"left::right","sibling":"retained"},
            "result":"original"
        })],
        "split must not mutate durable rich source values after failures"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="split-rich" | split "," | split "" unicode unicode_parts | split "::" nested.source nested.parts | fields case, _msg, unicode_parts, nested, array, object"#,
        )
        .await,
        [serde_json::json!({
            "case":"split-rich",
            "_msg":r#"["","foo","bar","","baz",""]"#,
            "unicode_parts":r#"["Ш","z","ч"]"#,
            "nested":{
                "source":"left::right",
                "sibling":"retained",
                "parts":r#"["left","right"]"#
            },
            "array":["x",1],
            "object":{"child":"retained"}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_drop_empty_fields_is_typed_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("drop-empty-fields-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "rich".into(),
                    metadata_json: serde_json::json!({
                        "case":"drop-empty-rich",
                        "drop_empty_group":"drop-empty",
                        "empty":"",
                        "null_value":null,
                        "zero":0,
                        "flag":false,
                        "array":[],
                        "nested":{
                            "empty":"",
                            "null_value":null,
                            "keep":"yes",
                            "deeper":{"empty":"", "keep":0},
                            "empty_parent":{"only":""}
                        }
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "all empty after projection".into(),
                    metadata_json: serde_json::json!({
                        "case":"drop-empty-all",
                        "drop_empty_group":"drop-empty",
                        "empty":"",
                        "null_value":null
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="drop-empty-rich" | fields case, empty, null_value, zero, flag, array, nested | DrOp_EmPtY_FiElDs"#,
        )
        .await,
        [serde_json::json!({
            "case":"drop-empty-rich",
            "zero":0,
            "flag":false,
            "array":[],
            "nested":{"keep":"yes", "deeper":{"keep":0}}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="drop-empty-all" | fields empty, null_value | drop_empty_fields | stats count() as rows"#,
        )
        .await,
        [serde_json::json!({"rows":0})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="drop-empty-rich" | fields case | format "" as transient | drop_empty_fields | field_names"#,
        )
        .await,
        [serde_json::json!({"name":"case", "hits":1})]
    );

    for malformed in [
        "* | drop_empty_fields()",
        "* | drop_empty_fields field",
        "* | drop_empty_fields.extra",
        "* | drop_empty_fields as",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 2,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="drop-empty-rich" | fields case, nested | drop_empty_fields"#,
    ))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(limited.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(limited["reason"], "max_work_rows", "{limited}");

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="drop-empty-rich" | fields case, empty, null_value, zero, flag, array, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"drop-empty-rich",
            "empty":"",
            "null_value":null,
            "zero":0,
            "flag":false,
            "array":[],
            "nested":{
                "empty":"",
                "null_value":null,
                "keep":"yes",
                "deeper":{"empty":"", "keep":0},
                "empty_parent":{"only":""}
            }
        })],
        "drop_empty_fields must not mutate durable rich source values and the reader remains reusable"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="drop-empty-rich" | fields case, empty, null_value, zero, flag, array, nested | drop_empty_fields"#,
        )
        .await,
        [serde_json::json!({
            "case":"drop-empty-rich",
            "zero":0,
            "flag":false,
            "array":[],
            "nested":{"keep":"yes", "deeper":{"keep":0}}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_replace_is_literal_typed_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("replace-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "secret_secret_ß".into(),
                    metadata_json: serde_json::json!({
                        "case":"replace-admin",
                        "replace_group":"replace",
                        "kind":"admin",
                        "password":"secret secret",
                        "text":"a_a_ß",
                        "number":101,
                        "flag":false,
                        "array":["a",1],
                        "null_value":null,
                        "nested":{"value":"a_a", "keep":1}
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "public".into(),
                    metadata_json: serde_json::json!({
                        "case":"replace-user",
                        "replace_group":"replace",
                        "kind":"user",
                        "password":"secret secret"
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-admin" | replace ("secret", "***") | replace ("ß", "ss") | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({"case":"replace-admin", "_msg":"***_***_ss"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-admin" | RePlAcE ("_", "-") at text limit 1 | fields case, text"#,
        )
        .await,
        [serde_json::json!({"case":"replace-admin", "text":"a-a_ß"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"replace_group:=replace | replace if (kind:=admin) ("secret", "***") at password | fields case, password"#,
        )
        .await,
        [
            serde_json::json!({"case":"replace-admin", "password":"*** ***"}),
            serde_json::json!({"case":"replace-user", "password":"secret secret"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-admin" | replace ("1", "x") at number | replace ("_", ".") at nested.value | fields case, number, flag, array, null_value, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-admin",
            "number":"x0x",
            "flag":false,
            "array":["a",1],
            "null_value":null,
            "nested":{"value":"a.a", "keep":1}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-admin" | replace if () ("a", "z") at array | replace ("", "ignored") at flag | replace ("x", "y") at nested | replace ("x", "y") at missing | fields case, array, flag, nested, missing"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-admin",
            "array":"[\"z\",1]",
            "flag":false,
            "nested":{"value":"a_a", "keep":1}
        })],
        "an actual array replacement uses its compact textual projection, while empty-old, object-parent, and missing targets remain native no-ops"
    );

    for malformed in [
        "* | replace(foo,bar)",
        "* | replace (foo)",
        "* | replace (foo, bar) at *",
        "* | replace (foo, bar) limit N",
        "* | replace (foo, bar) trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="replace-admin" | replace ("secret", "***")"#,
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let work_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(work_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(work_limited["reason"], "max_work_rows", "{work_limited}");

    let response_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_response_bytes: 8,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="replace-admin" | replace ("secret", "replacement-expands")"#,
    ))
    .await
    .unwrap();
    assert_eq!(response_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let response_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(response_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response_limited["reason"], "max_response_bytes",
        "{response_limited}"
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-admin" | fields case, _msg, text, number, flag, array, null_value, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-admin",
            "_msg":"secret_secret_ß",
            "text":"a_a_ß",
            "number":101,
            "flag":false,
            "array":["a",1],
            "null_value":null,
            "nested":{"value":"a_a", "keep":1}
        })],
        "replace must not mutate durable rich source values and the reader remains reusable"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="replace-admin" | replace ("_", "-") at text limit 1 | replace if (kind:=admin) ("secret", "***") | fields case, _msg, text, number, array, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-admin",
            "_msg":"***_***_ß",
            "text":"a-a_ß",
            "number":101,
            "array":["a",1],
            "nested":{"value":"a_a", "keep":1}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_replace_regexp_matches_re2_captures_and_durability() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("replace-regexp-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "foo a\n b bar / foo".into(),
                    metadata_json: serde_json::json!({
                        "case":"replace-regexp-admin",
                        "replace_regexp_group":"replace-regexp",
                        "kind":"admin",
                        "host":"aцC",
                        "password":"secret secret",
                        "text":"hello",
                        "number":101,
                        "flag":false,
                        "array":["prod",1],
                        "null_value":null,
                        "nested":{"value":"a_a", "keep":1}
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "public".into(),
                    metadata_json: serde_json::json!({
                        "case":"replace-regexp-user",
                        "replace_regexp_group":"replace-regexp",
                        "kind":"user",
                        "password":"secret secret"
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-regexp-admin" | replace_regexp ("foo(.+?)bar", "capture=$1") | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-regexp-admin",
            "_msg":"capture= a\n b  / foo"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-regexp-admin" | replace_regexp ("(?-s)foo(.+?)bar", "nope") | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-regexp-admin",
            "_msg":"foo a\n b bar / foo"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-regexp-admin" | RePlAcE_ReGeXp ("(?P<lead>a)(?P<rest>.+)", "${rest}-${lead}") at host | replace_regexp ("", X) at nested.value limit 2 | fields case, host, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-regexp-admin",
            "host":"цC-a",
            "nested":{"value":"XaX_a", "keep":1}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-regexp-admin" | replace_regexp ("h(e)", "$$-$9-$1x-${1}x") at text | fields case, text"#,
        )
        .await,
        [serde_json::json!({"case":"replace-regexp-admin", "text":"$---exllo"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-regexp-admin" | replace_regexp (l, L) at text limit 0 | replace_regexp ("^|$", X) at host | replace_regexp (X, Z) at host limit 1 | fields case, text, host"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-regexp-admin",
            "text":"heLLo",
            "host":"ZaцCX"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"replace_regexp_group:=replace-regexp | replace_regexp if (kind:=admin) (secret, "***") at password limit 1 | fields case, password"#,
        )
        .await,
        [
            serde_json::json!({
                "case":"replace-regexp-admin",
                "password":"*** secret"
            }),
            serde_json::json!({
                "case":"replace-regexp-user",
                "password":"secret secret"
            }),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-regexp-admin" | replace_regexp ("1", x) at number | replace_regexp (missing, ignored) at flag | replace_regexp (prod, stage) at array | replace_regexp ("_", ".") at nested.value | fields case, number, flag, array, null_value, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-regexp-admin",
            "number":"x0x",
            "flag":false,
            "array":"[\"stage\",1]",
            "null_value":null,
            "nested":{"value":"a.a", "keep":1}
        })]
    );

    for malformed in [
        "* | replace_regexp(foo,bar)",
        "* | replace_regexp (foo)",
        r#"* | replace_regexp ("foo[", bar)"#,
        r#"* | replace_regexp ("(a)\\1", bar)"#,
        r#"* | replace_regexp ("a(?=b)", bar)"#,
        "* | replace_regexp (foo, bar) at *",
        "* | replace_regexp (foo, bar) limit N",
        "* | replace_regexp (foo, bar) trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="replace-regexp-admin" | replace_regexp ("[a-z]", expanded)"#,
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let work_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(work_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(work_limited["reason"], "max_work_rows", "{work_limited}");

    let response_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_response_bytes: 8,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="replace-regexp-admin" | replace_regexp (".", replacement-expands)"#,
    ))
    .await
    .unwrap();
    assert_eq!(response_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let response_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(response_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response_limited["reason"], "max_response_bytes",
        "{response_limited}"
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="replace-regexp-admin" | fields case, _msg, host, password, text, number, flag, array, null_value, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-regexp-admin",
            "_msg":"foo a\n b bar / foo",
            "host":"aцC",
            "password":"secret secret",
            "text":"hello",
            "number":101,
            "flag":false,
            "array":["prod",1],
            "null_value":null,
            "nested":{"value":"a_a", "keep":1}
        })],
        "replace_regexp must not mutate durable rich source values after failures"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="replace-regexp-admin" | replace_regexp ("[/ ]", "-") limit 2 | replace_regexp ("(?P<lead>a)(?P<rest>.+)", "${rest}-${lead}") at host | fields case, _msg, host, number, array, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"replace-regexp-admin",
            "_msg":"foo-a\n-b bar / foo",
            "host":"цC-a",
            "number":101,
            "array":["prod",1],
            "nested":{"value":"a_a", "keep":1}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_extract_is_literal_typed_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("extract-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: r#"prefix kind=admin id="a\"b" tail suffix"#.into(),
                    metadata_json: serde_json::json!({
                        "case":"extract-admin",
                        "extract_group":"extract",
                        "kind":"admin",
                        "result":"original",
                        "empty":"",
                        "number":101,
                        "flag":false,
                        "array":["prod",1],
                        "null_value":null,
                        "source_html":"left < right",
                        "source_partial":r#"begin "captured without-delimiter""#,
                        "nested":{"value":"old", "keep":1}
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "public".into(),
                    metadata_json: serde_json::json!({
                        "case":"extract-user",
                        "extract_group":"extract",
                        "kind":"user",
                        "result":"original"
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-admin" | extract 'kind=<parsed_kind> id=<parsed_id> tail' | fields case, parsed_kind, parsed_id"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-admin",
            "parsed_kind":"admin",
            "parsed_id":"a\"b"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-admin" | extract 'kind=<_> id=<plain:raw_id> tail' | fields case, raw_id"#,
        )
        .await,
        [serde_json::json!({"case":"extract-admin", "raw_id":r#""a\"b""#})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-admin" | extract '<left> &lt; <right>' from source_html | fields case, left, right"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-admin",
            "left":"left",
            "right":"right"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-admin" | extract 'begin <captured> delimiter=<missing>' from source_partial | fields case, captured, missing"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-admin",
            "captured":"captured without-delimiter",
            "missing":""
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"extract_group:=extract | extract if (kind:=admin) 'kind=<parsed> id=<_>' | fields case, parsed"#,
        )
        .await,
        [
            serde_json::json!({"case":"extract-admin", "parsed":"admin"}),
            serde_json::json!({"case":"extract-user"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-admin" | extract 'missing=<result>' keep_original_fields | extract 'missing=<empty>' skip_empty_results | extract '<number_text>' from number | fields case, result, empty, number, number_text, flag, array, null_value, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-admin",
            "result":"original",
            "empty":"",
            "number":101,
            "number_text":"101",
            "flag":false,
            "array":["prod",1],
            "null_value":null,
            "nested":{"value":"old", "keep":1}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-admin" | extract 'kind=<nested.value> id=<copy>' | extract '<copied_again>' from copy | fields case, nested, copy, copied_again"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-admin",
            "nested":{"value":"admin", "keep":1},
            "copy":"a\"b",
            "copied_again":"a\"b"
        })]
    );

    for malformed in [
        "* | extract",
        "* | extract keep_original_fields",
        r#"* | extract 'literal-only'"#,
        r#"* | extract '<left><right>'"#,
        r#"* | extract '<field*>'"#,
        r#"* | extract '<field>' from *"#,
        r#"* | extract 'foo=<bar>' from x*"#,
        r#"* | extract '<*>foo<_>bar'"#,
        "* | extract if (x:y)",
        r#"* | extract '<field>' keep_original_fields skip_empty_results"#,
        r#"* | extract '<field>' trailing"#,
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="extract-admin" | extract 'kind=<nested>'"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"extract_group:=extract | extract '<captured>'"#,
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let work_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(work_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(work_limited["reason"], "max_work_rows", "{work_limited}");

    let response_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_response_bytes: 8,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="extract-admin" | extract '<captured>'"#,
    ))
    .await
    .unwrap();
    assert_eq!(response_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let response_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(response_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response_limited["reason"], "max_response_bytes",
        "{response_limited}"
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-admin" | fields case, _msg, kind, result, empty, number, flag, array, null_value, source_html, source_partial, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-admin",
            "_msg":r#"prefix kind=admin id="a\"b" tail suffix"#,
            "kind":"admin",
            "result":"original",
            "empty":"",
            "number":101,
            "flag":false,
            "array":["prod",1],
            "null_value":null,
            "source_html":"left < right",
            "source_partial":r#"begin "captured without-delimiter""#,
            "nested":{"value":"old", "keep":1}
        })],
        "extract must not mutate durable rich source values after failures"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="extract-admin" | extract 'kind=<parsed_kind> id=<parsed_id> tail' | fields case, parsed_kind, parsed_id, number, array, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-admin",
            "parsed_kind":"admin",
            "parsed_id":"a\"b",
            "number":101,
            "array":["prod",1],
            "nested":{"value":"old", "keep":1}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_extract_regexp_is_first_match_typed_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("extract-regexp-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "prefix user=Alice id=42\nnext user=Later id=99".into(),
                    metadata_json: serde_json::json!({
                        "case":"extract-regexp-admin",
                        "extract_regexp_group":"extract-regexp",
                        "kind":"admin",
                        "result":"original",
                        "empty":"",
                        "number":101,
                        "flag":false,
                        "array":["prod",1],
                        "null_value":null,
                        "optional_source":"kind=admin",
                        "nested":{"value":"source=xy", "keep":1}
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "public".into(),
                    metadata_json: serde_json::json!({
                        "case":"extract-regexp-user",
                        "extract_regexp_group":"extract-regexp",
                        "kind":"user",
                        "result":"original"
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-regexp-admin" | extract_regexp 'user=(?P<user>[A-Za-z]+) id=([0-9]+)(?P<id_tail>.*)' | extract_regexp '(?P<id>[0-9]+)' from id_tail | fields case, user, id"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-regexp-admin",
            "user":"Alice",
            "id":"99"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-regexp-admin" | extract_regexp '(?P<whole>user=(?P<first>[A-Za-z]+) id=(?P<id>[0-9]+).*(?P<tail>next))' | fields case, whole, first, id, tail"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-regexp-admin",
            "whole":"user=Alice id=42\nnext",
            "first":"Alice",
            "id":"42",
            "tail":"next"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-regexp-admin" | ExTrAcT_ReGeXp '(?-s)prefix (?<first_line>.+)' SkIp_EmPtY_ReSuLtS | fields case, first_line"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-regexp-admin",
            "first_line":"user=Alice id=42"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"extract_regexp_group:=extract-regexp | extract_regexp if (kind:=admin) 'user=(?P<parsed>[A-Za-z]+)' | fields case, parsed"#,
        )
        .await,
        [
            serde_json::json!({"case":"extract-regexp-admin", "parsed":"Alice"}),
            serde_json::json!({"case":"extract-regexp-user"}),
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-regexp-admin" | extract_regexp 'number=(?P<number>.+)' from optional_source keep_original_fields | extract_regexp 'missing=(?P<array>.+)' from optional_source skip_empty_results | extract_regexp '^(?P<number_text>.*)$' from number | extract_regexp '^(?P<array_text>.*)$' from array | extract_regexp 'missing=(?P<null_value>.+)' from optional_source | fields case, number, number_text, flag, array, array_text, null_value, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-regexp-admin",
            "number":101,
            "number_text":"101",
            "flag":false,
            "array":["prod",1],
            "array_text":"[\"prod\",1]",
            "null_value":"",
            "nested":{"value":"source=xy", "keep":1}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-regexp-admin" | extract_regexp 'source=(?P<nested_value>.+)' from nested.value | extract_regexp '(?P<copied>.+)' from nested_value | extract_regexp 'kind=(?P<optional_kind>[a-z]+)(?: id=(?P<optional_id>[0-9]+))?' from optional_source | fields case, nested_value, copied, optional_kind, optional_id"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-regexp-admin",
            "nested_value":"xy",
            "copied":"xy",
            "optional_kind":"admin",
            "optional_id":""
        })]
    );

    for malformed in [
        "* | extract_regexp",
        "* | extract_regexp keep_original_fields",
        r#"* | extract_regexp '(anonymous-only)'"#,
        r#"* | extract_regexp '(?P<field>[)'"#,
        r#"* | extract_regexp '(?P<field>(a)\2)'"#,
        r#"* | extract_regexp '(?P<field>a(?=b))'"#,
        r#"* | extract_regexp '(?P<field>.*)' from *"#,
        r#"* | extract_regexp '(?P<field>.*)' from x*"#,
        "* | extract_regexp if (x:y)",
        r#"* | extract_regexp '(?P<field>.*)' keep_original_fields skip_empty_results"#,
        r#"* | extract_regexp '(?P<field>.*)' trailing"#,
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="extract-regexp-admin" | extract_regexp 'user=(?P<nested>.+)'"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"extract_regexp_group:=extract-regexp | extract_regexp '(?P<captured>.+)'"#,
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let work_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(work_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(work_limited["reason"], "max_work_rows", "{work_limited}");

    let response_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_response_bytes: 8,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="extract-regexp-admin" | extract_regexp '(?P<captured>.+)'"#,
    ))
    .await
    .unwrap();
    assert_eq!(response_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let response_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(response_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response_limited["reason"], "max_response_bytes",
        "{response_limited}"
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="extract-regexp-admin" | fields case, _msg, kind, result, empty, number, flag, array, null_value, optional_source, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-regexp-admin",
            "_msg":"prefix user=Alice id=42\nnext user=Later id=99",
            "kind":"admin",
            "result":"original",
            "empty":"",
            "number":101,
            "flag":false,
            "array":["prod",1],
            "null_value":null,
            "optional_source":"kind=admin",
            "nested":{"value":"source=xy", "keep":1}
        })],
        "extract_regexp must not mutate durable rich source values after failures"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="extract-regexp-admin" | extract_regexp 'user=(?P<user>[A-Za-z]+) id=(?P<id>[0-9]+)' | extract_regexp 'source=(?P<nested_value>.+)' from nested.value | fields case, user, id, nested_value, number, array, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"extract-regexp-admin",
            "user":"Alice",
            "id":"42",
            "nested_value":"xy",
            "number":101,
            "array":["prod",1],
            "nested":{"value":"source=xy", "keep":1}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_pack_json_is_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("pack-json-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 3,
                    severity: "warning".into(),
                    message: "original message".into(),
                    metadata_json: serde_json::json!({
                        "case":"pack-json-admin",
                        "pack_json_group":"pack-json",
                        "number":101,
                        "zero":0,
                        "flag":false,
                        "empty":"",
                        "null_value":null,
                        "array":["prod",1],
                        "nested":{"keep":"yes", "drop":"no"},
                        "empty_object":{},
                        "old_destination":"old"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "second".into(),
                    metadata_json: serde_json::json!({
                        "case":"pack-json-user",
                        "pack_json_group":"pack-json",
                        "number":202
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let rows = pipeline_rows(
        &app,
        r#"case:="pack-json-admin" | pack_json fields (case, number, zero, flag, empty, null_value, array, nested.keep, empty_object, missing) as packed | fields case, packed"#,
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["case"], "pack-json-admin");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(rows[0]["packed"].as_str().unwrap()).unwrap(),
        serde_json::json!({
            "case":"pack-json-admin",
            "number":101,
            "zero":0,
            "flag":false,
            "empty":"",
            "null_value":null,
            "array":["prod",1],
            "nested":{"keep":"yes"},
            "empty_object":{}
        })
    );

    let rows = pipeline_rows(
        &app,
        r#"case:="pack-json-admin" | fields case, _msg, level, _time | pack_json | fields case, _msg"#,
    )
    .await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(rows[0]["_msg"].as_str().unwrap()).unwrap(),
        serde_json::json!({
            "case":"pack-json-admin",
            "_msg":"original message",
            "level":"warning",
            "_time":"2027-01-15T08:00:00.000001Z"
        })
    );

    let rows = pipeline_rows(
        &app,
        r#"case:="pack-json-admin" | PaCk_JsOn FiElDs ("nested."*, flag, missing) As "packed field" | fields case, "packed field""#,
    )
    .await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(rows[0]["packed field"].as_str().unwrap())
            .unwrap(),
        serde_json::json!({
            "flag":false,
            "nested":{"drop":"no", "keep":"yes"}
        })
    );

    let rows = pipeline_rows(
        &app,
        r#"case:="pack-json-admin" | fields case, number | pack_json fields (missing, *,) packed | len(packed) as packed_len | fields case, packed, packed_len"#,
    )
    .await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(rows[0]["packed"].as_str().unwrap()).unwrap(),
        serde_json::json!({"case":"pack-json-admin", "number":101})
    );
    assert_eq!(
        rows[0]["packed_len"],
        rows[0]["packed"].as_str().unwrap().len().to_string()
    );

    let rows = pipeline_rows(
        &app,
        r#"case:="pack-json-admin" | pack_json fields (old_destination) as old_destination | fields case, old_destination"#,
    )
    .await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(rows[0]["old_destination"].as_str().unwrap())
            .unwrap(),
        serde_json::json!({"old_destination":"old"})
    );

    for malformed in [
        "* | pack_json foo bar",
        "* | pack_json fields",
        "* | pack_json fields case",
        "* | pack_json fields (case number)",
        "* | pack_json as *",
        "* | pack_json as x*",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="pack-json-admin" | pack_json as number.child"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");
    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="pack-json-admin" | pack_json as packed"#,
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let work_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(work_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(work_limited["reason"], "max_work_rows", "{work_limited}");

    let response_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_response_bytes: 8,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="pack-json-admin" | pack_json as packed"#,
    ))
    .await
    .unwrap();
    assert_eq!(response_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let response_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(response_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response_limited["reason"], "max_response_bytes",
        "{response_limited}"
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="pack-json-admin" | fields case, _msg, number, zero, flag, empty, null_value, array, nested, empty_object, old_destination"#,
        )
        .await,
        [serde_json::json!({
            "case":"pack-json-admin",
            "_msg":"original message",
            "number":101,
            "zero":0,
            "flag":false,
            "empty":"",
            "null_value":null,
            "array":["prod",1],
            "nested":{"keep":"yes", "drop":"no"},
            "empty_object":{},
            "old_destination":"old"
        })],
        "pack_json must not mutate durable rich source values after failures"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let rows = pipeline_rows(
        &router(reopened.clone()),
        r#"case:="pack-json-admin" | pack_json fields (case, number, flag, null_value, array, nested) as packed | fields case, packed"#,
    )
    .await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(rows[0]["packed"].as_str().unwrap()).unwrap(),
        serde_json::json!({
            "case":"pack-json-admin",
            "number":101,
            "flag":false,
            "null_value":null,
            "array":["prod",1],
            "nested":{"keep":"yes", "drop":"no"}
        })
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_pack_logfmt_is_exact_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("pack-logfmt-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 3,
                    severity: "warning".into(),
                    message: "original message".into(),
                    metadata_json: serde_json::json!({
                        "case":"pack-logfmt",
                        "pack_logfmt_group":"pack-logfmt",
                        "number":101,
                        "zero":0,
                        "flag":false,
                        "empty":"",
                        "null_value":null,
                        "array":["prod",1],
                        "nested":{"keep":"yes", "drop":"no"},
                        "spaced field":"two words",
                        "escape":"line one\n\"two\"\\tail<'",
                        "old_destination":"old"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "second message".into(),
                    metadata_json: serde_json::json!({
                        "case":"pack-logfmt-secondary",
                        "pack_logfmt_group":"pack-logfmt",
                        "number":202
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="pack-logfmt" | pack_logfmt fields (case, number, zero, flag, empty, null_value, array, nested.*, "spaced field", escape, missing) as packed | fields case, packed"#,
        )
        .await,
        [serde_json::json!({
            "case":"pack-logfmt",
            "packed":r#"array="[\"prod\",1]" case=pack-logfmt empty= escape="line one\n\"two\"\\tail\u003c\u0027" flag=false missing= nested.drop=no nested.keep=yes null_value= number=101 spaced field="two words" zero=0"#
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="pack-logfmt" | fields case, _msg, level, _time | pack_logfmt | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({
            "case":"pack-logfmt",
            "_msg":r#"_msg="original message" _time=2027-01-15T08:00:00.000001Z case=pack-logfmt level=warning"#
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="pack-logfmt" | PaCk_LoGfMt FiElDs ("nested."*, flag, missing) As "packed field" | fields case, "packed field""#,
        )
        .await,
        [serde_json::json!({
            "case":"pack-logfmt",
            "packed field":"flag=false missing= nested.drop=no nested.keep=yes"
        })]
    );

    let rows = pipeline_rows(
        &app,
        r#"case:="pack-logfmt" | fields case, number | pack_logfmt fields (missing, *,) packed | len(packed) as packed_len | fields case, packed, packed_len"#,
    )
    .await;
    assert_eq!(rows[0]["packed"], "case=pack-logfmt number=101");
    assert_eq!(
        rows[0]["packed_len"],
        rows[0]["packed"].as_str().unwrap().len().to_string()
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="pack-logfmt" | pack_logfmt fields (old_destination) as old_destination | fields case, old_destination"#,
        )
        .await,
        [serde_json::json!({
            "case":"pack-logfmt",
            "old_destination":"old_destination=old"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="pack-logfmt" | pack_logfmt fields (nested) as packed | fields case, packed"#,
        )
        .await,
        [serde_json::json!({"case":"pack-logfmt", "packed":"nested="})]
    );

    for malformed in [
        "* | pack_logfmt foo bar",
        "* | pack_logfmt fields",
        "* | pack_logfmt fields case",
        "* | pack_logfmt fields (case number)",
        "* | pack_logfmt fields (, case)",
        "* | pack_logfmt as *",
        "* | pack_logfmt as x*",
        "* | pack_logfmt.extra",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="pack-logfmt" | pack_logfmt as nested"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(
                r#"pack_logfmt_group:="pack-logfmt" | pack_logfmt as packed | limit 10000"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="pack-logfmt" | fields case, _msg, number, zero, flag, empty, null_value, array, nested, "spaced field", escape, old_destination"#,
        )
        .await,
        [serde_json::json!({
            "case":"pack-logfmt",
            "_msg":"original message",
            "number":101,
            "zero":0,
            "flag":false,
            "empty":"",
            "null_value":null,
            "array":["prod",1],
            "nested":{"keep":"yes", "drop":"no"},
            "spaced field":"two words",
            "escape":"line one\n\"two\"\\tail<'",
            "old_destination":"old"
        })],
        "pack_logfmt must not mutate durable rich source values after failures"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="pack-logfmt" | pack_logfmt fields (case, number, flag, null_value, array, nested.*) as packed | fields case, packed"#,
        )
        .await,
        [serde_json::json!({
            "case":"pack-logfmt",
            "packed":r#"array="[\"prod\",1]" case=pack-logfmt flag=false nested.drop=no nested.keep=yes null_value= number=101"#
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_unpack_json_is_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("unpack-json-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let json_source = r#" {"decoded":"yes","zero":0,"flag":false,"empty":"","null_value":null,"array":["prod",1],"nested":{"keep":"yes","number":2},"empty_object":{},"json_source":"replaced","nonstandard":NaN} "#;
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 3,
                    severity: "warning".into(),
                    message: "unpack source".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-json-admin",
                        "unpack_json_group":"unpack-json",
                        "kind":"admin",
                        "json_source":json_source,
                        "overwrite_source":r#"{"result":"new","empty":"","null_value":null,"overwrite_source":"replaced"}"#,
                        "malformed_source":"{broken",
                        "scalar_source":"[1,2]",
                        "native_source":{"native":true,"nested":{"value":3}},
                        "literal_source":r#"{"literal.key":1,"nested":{"key":2}}"#,
                        "conflict_source":r#"{"nested":"scalar"}"#,
                        "decoded":"original",
                        "empty":"existing-empty",
                        "null_value":"existing-null",
                        "nested":{"sibling":"retained"},
                        "result":["native"]
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "condition miss".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-json-user",
                        "unpack_json_group":"unpack-json",
                        "kind":"user",
                        "json_source":r#"{"decoded":"changed"}"#,
                        "decoded":"untouched"
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-json-admin" | unpack_json from json_source fields (decoded, zero, flag, empty, null_value, array, nested.*, empty_object, json_source, nonstandard, missing) | fields case, decoded, zero, flag, empty, null_value, array, nested, empty_object, json_source, nonstandard, missing"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-json-admin",
            "decoded":"yes",
            "zero":0,
            "flag":false,
            "empty":"",
            "null_value":null,
            "array":["prod",1],
            "nested":{"sibling":"retained", "keep":"yes", "number":2},
            "empty_object":{},
            "json_source":"replaced",
            "nonstandard":"NaN",
            "missing":""
        })]
    );

    let prefixed = pipeline_rows(
        &app,
        r#"case:="unpack-json-admin" | UnPaCk_JsOn FrOm json_source PrEsErVe_KeYs (nested) ReSuLt_PrEfIx "decoded_prefix." | fields case, decoded_prefix"#,
    )
    .await;
    assert_eq!(prefixed[0]["decoded_prefix"]["zero"], serde_json::json!(0));
    assert_eq!(
        prefixed[0]["decoded_prefix"]["nested"],
        serde_json::json!({"keep":"yes", "number":2})
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-json-admin" | unpack_json from overwrite_source keep_original_fields | fields case, result, empty, null_value, overwrite_source"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-json-admin",
            "result":["native"],
            "empty":"existing-empty",
            "null_value":"existing-null",
            "overwrite_source":r#"{"result":"new","empty":"","null_value":null,"overwrite_source":"replaced"}"#
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-json-admin" | unpack_json from overwrite_source skip_empty_results | fields case, result, empty, null_value, overwrite_source"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-json-admin",
            "result":"new",
            "empty":"existing-empty",
            "null_value":"existing-null",
            "overwrite_source":"replaced"
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-json-user" | unpack_json if (kind:=admin) from json_source fields (decoded) | fields case, decoded"#,
        )
        .await,
        [serde_json::json!({"case":"unpack-json-user", "decoded":"untouched"})]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-json-admin" | unpack_json from native_source fields (native, nested.value) | fields case, native, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-json-admin",
            "native":true,
            "nested":{"sibling":"retained", "value":3}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-json-admin" | unpack_json from malformed_source fields (missing) result_prefix malformed_ | fields case, malformed_missing"#,
        )
        .await,
        [serde_json::json!({"case":"unpack-json-admin", "malformed_missing":""})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-json-admin" | unpack_json from scalar_source fields (missing) | fields case, scalar_source"#,
        )
        .await,
        [serde_json::json!({"case":"unpack-json-admin", "scalar_source":"[1,2]"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-json-admin" | unpack_json from literal_source fields ("literal.key", nested.key) result_prefix decoded_literal. | fields case, decoded_literal"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-json-admin",
            "decoded_literal":{"literal.key":1, "nested":{"key":2}}
        })]
    );

    let round_trip = pipeline_rows(
        &app,
        r#"case:="unpack-json-admin" | pack_json fields (case, null_value, nested.sibling, result, native_source) as packed | unpack_json from packed result_prefix roundtrip. | fields case, roundtrip"#,
    )
    .await;
    assert_eq!(
        round_trip[0]["roundtrip"],
        serde_json::json!({
            "case":"unpack-json-admin",
            "null_value":"existing-null",
            "nested":{"sibling":"retained"},
            "result":["native"],
            "native_source":{"native":true,"nested":{"value":3}}
        })
    );

    for malformed in [
        "* | unpack_json if",
        "* | unpack_json from",
        "* | unpack_json from *",
        "* | unpack_json fields",
        "* | unpack_json fields (case missing)",
        "* | unpack_json preserve_keys (*)",
        "* | unpack_json result_prefix",
        "* | unpack_json keep_original_fields skip_empty_results",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="unpack-json-admin" | unpack_json from conflict_source"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="unpack-json-admin" | unpack_json from json_source"#,
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let work_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(work_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(work_limited["reason"], "max_work_rows", "{work_limited}");

    let response_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_response_bytes: 8,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="unpack-json-admin" | unpack_json from json_source"#,
    ))
    .await
    .unwrap();
    assert_eq!(response_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let response_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(response_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response_limited["reason"], "max_response_bytes",
        "{response_limited}"
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-json-admin" | fields case, json_source, overwrite_source, malformed_source, scalar_source, native_source, literal_source, conflict_source, decoded, empty, null_value, nested, result"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-json-admin",
            "json_source":json_source,
            "overwrite_source":r#"{"result":"new","empty":"","null_value":null,"overwrite_source":"replaced"}"#,
            "malformed_source":"{broken",
            "scalar_source":"[1,2]",
            "native_source":{"native":true,"nested":{"value":3}},
            "literal_source":r#"{"literal.key":1,"nested":{"key":2}}"#,
            "conflict_source":r#"{"nested":"scalar"}"#,
            "decoded":"original",
            "empty":"existing-empty",
            "null_value":"existing-null",
            "nested":{"sibling":"retained"},
            "result":["native"]
        })],
        "unpack_json must not mutate durable rich source values after failures"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="unpack-json-admin" | unpack_json from json_source fields (decoded, zero, flag, null_value, array, nested.*, nonstandard) result_prefix reopened. | fields case, reopened"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-json-admin",
            "reopened":{
                "decoded":"yes",
                "zero":0,
                "flag":false,
                "null_value":null,
                "array":["prod",1],
                "nested":{"keep":"yes", "number":2},
                "nonstandard":"NaN"
            }
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_unpack_logfmt_is_exact_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("unpack-logfmt-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let source = r#"foo=bar quoted="x y=z" empty= lone duplicate=first duplicate=second nested.keep=yes escaped="line\nnext\t\u263a" source=replaced"#;
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 3,
                    severity: "warning".into(),
                    message: "default=yes quoted=\"message value\"".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-logfmt-admin",
                        "unpack_logfmt_group":"unpack-logfmt",
                        "kind":"admin",
                        "source":source,
                        "malformed_source":r#"malformed="unterminated next=still-parsed"#,
                        "conflict_source":"conflict_parent.value=changed",
                        "conflict_parent":"scalar",
                        "foo":"original",
                        "empty":"original-empty",
                        "duplicate":"original-duplicate",
                        "nested":{"sibling":"retained"},
                        "result":["native"]
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "default=changed".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-logfmt-user",
                        "unpack_logfmt_group":"unpack-logfmt",
                        "kind":"user",
                        "source":"foo=changed",
                        "foo":"untouched"
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-logfmt-admin" | unpack_logfmt from source fields (foo, quoted, empty, lone, duplicate, nested.*, escaped, source, missing) | fields case, foo, quoted, empty, lone, duplicate, nested, escaped, source, missing"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-logfmt-admin",
            "foo":"bar",
            "quoted":"x y=z",
            "empty":"",
            "lone":"",
            "duplicate":"second",
            "nested":{"sibling":"retained", "keep":"yes"},
            "escaped":"line\nnext\t☺",
            "source":"replaced",
            "missing":""
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-logfmt-admin" | UnPaCk_LoGfMt FrOm source FiElDs (foo, missing, "nested."*) ReSuLt_PrEfIx "decoded." | fields case, decoded"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-logfmt-admin",
            "decoded":{"foo":"bar", "missing":"", "nested":{"keep":"yes"}}
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-logfmt-admin" | unpack_logfmt from source keep_original_fields | fields case, foo, empty, duplicate, quoted, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-logfmt-admin",
            "foo":"original",
            "empty":"original-empty",
            "duplicate":"original-duplicate",
            "quoted":"x y=z",
            "nested":{"sibling":"retained", "keep":"yes"}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-logfmt-admin" | unpack_logfmt from source skip_empty_results | fields case, foo, empty, lone, duplicate"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-logfmt-admin",
            "foo":"bar",
            "empty":"original-empty",
            "duplicate":"second"
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-logfmt-user" | unpack_logfmt if (kind:=admin) from source | fields case, foo"#,
        )
        .await,
        [serde_json::json!({"case":"unpack-logfmt-user", "foo":"untouched"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-logfmt-admin" | unpack_logfmt if (kind:in(case:="unpack-logfmt-admin" | fields kind)) from source fields (foo) result_prefix query_backed. | fields case, query_backed.foo"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-logfmt-admin",
            "query_backed":{"foo":"bar"}
        })],
        "query-backed conditions must resolve for unpack_logfmt"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-logfmt-admin" | unpack_logfmt from malformed_source | fields case, malformed, next"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-logfmt-admin",
            "malformed":"\"unterminated",
            "next":"still-parsed"
        })]
    );

    let round_trip = pipeline_rows(
        &app,
        r#"case:="unpack-logfmt-admin" | fields case, nested.sibling, result | pack_logfmt fields (case, nested.*, result) as packed | unpack_logfmt from packed result_prefix roundtrip. | fields case, roundtrip"#,
    )
    .await;
    assert_eq!(
        round_trip[0]["roundtrip"],
        serde_json::json!({
            "case":"unpack-logfmt-admin",
            "nested":{"sibling":"retained"},
            "result":r#"["native"]"#
        })
    );

    for malformed in [
        "* | unpack_logfmt if",
        "* | unpack_logfmt from",
        "* | unpack_logfmt from *",
        "* | unpack_logfmt fields",
        "* | unpack_logfmt fields (case missing)",
        "* | unpack_logfmt result_prefix",
        "* | unpack_logfmt keep_original_fields skip_empty_results",
        "* | unpack_logfmt.extra",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="unpack-logfmt-admin" | unpack_logfmt from conflict_source"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(
                r#"unpack_logfmt_group:="unpack-logfmt" | unpack_logfmt from source | limit 10000"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-logfmt-admin" | fields case, source, malformed_source, conflict_source, conflict_parent, foo, empty, duplicate, nested, result"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-logfmt-admin",
            "source":source,
            "malformed_source":r#"malformed="unterminated next=still-parsed"#,
            "conflict_source":"conflict_parent.value=changed",
            "conflict_parent":"scalar",
            "foo":"original",
            "empty":"original-empty",
            "duplicate":"original-duplicate",
            "nested":{"sibling":"retained"},
            "result":["native"]
        })],
        "unpack_logfmt must not mutate durable rich source values after failures"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="unpack-logfmt-admin" | unpack_logfmt from source fields (foo, duplicate, nested.*, escaped) result_prefix reopened. | fields case, reopened"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-logfmt-admin",
            "reopened":{
                "foo":"bar",
                "duplicate":"second",
                "nested":{"keep":"yes"},
                "escaped":"line\nnext\t☺"
            }
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_unpack_syslog_is_exact_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("unpack-syslog-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: r#"<165>1 2023-06-03T17:42:32.123456789Z host.example app 123 ID47 [meta keep="yes"] test message  "#.into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-syslog-rfc5424",
                        "unpack_syslog_group":"unpack-syslog",
                        "kind":"admin",
                        "app_name":"original",
                        "nested":{"sibling":"retained"}
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "Jan  1 00:00:00 classic-host classic[9]: classic message".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-syslog-rfc3164",
                        "unpack_syslog_group":"unpack-syslog"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_003,
                    level: 3,
                    severity: "warning".into(),
                    message: r#"Sep 29 08:26:10 cef-host CEF:1|Security|product|1.0|100|name\|value|10|src=10.0.0.1 msg=two words"#.into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-syslog-cef",
                        "unpack_syslog_group":"unpack-syslog"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_004,
                    level: 3,
                    severity: "warning".into(),
                    message: r#"Jun  3 12:08:33 cee-host app[1]: @cee: {"number":123,"flag":false,"null_value":null,"nested":{"leaf":7},"array":[1,"x"],"message":"cee message"}"#.into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-syslog-cee",
                        "unpack_syslog_group":"unpack-syslog",
                        "nested":{"sibling":"retained"}
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_005,
                    level: 1,
                    severity: "info".into(),
                    message: "custom source".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-syslog-custom",
                        "unpack_syslog_group":"unpack-syslog",
                        "source":"\t\n  <46>2025-01-23T12:15:23.965512+01:00 iso-host rsyslogd: start  "
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_006,
                    level: 3,
                    severity: "warning".into(),
                    message: r#"<165>1 2023-06-03T17:42:00Z host app 1 ID [conflict value="changed"] tail"#.into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-syslog-conflict",
                        "unpack_syslog_group":"unpack-syslog",
                        "conflict":"scalar"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_007,
                    level: 1,
                    severity: "info".into(),
                    message: "dictionary".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-syslog-dictionary",
                        "role":"syslog-dictionary",
                        "lookup":"admin"
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-syslog-rfc5424" | unpack_syslog | fields case, priority, facility_keyword, level, facility, severity, format, timestamp, hostname, app_name, proc_id, msg_id, meta, message, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-rfc5424",
            "priority":"165",
            "facility_keyword":"local4",
            "level":"notice",
            "facility":"20",
            "severity":"5",
            "format":"rfc5424",
            "timestamp":"2023-06-03T17:42:32.123456789Z",
            "hostname":"host.example",
            "app_name":"app",
            "proc_id":"123",
            "msg_id":"ID47",
            "meta":{"keep":"yes"},
            "message":"test message  ",
            "nested":{"sibling":"retained"}
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-syslog-rfc5424" | unpack_syslog keep_original_fields | fields case, app_name, hostname, meta"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-rfc5424",
            "app_name":"original",
            "hostname":"host.example",
            "meta":{"keep":"yes"}
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-syslog-custom" | UnPaCk_SySlOg FrOm source OfFsEt -8h ReSuLt_PrEfIx "decoded." | fields case, source, decoded"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-custom",
            "source":"\t\n  <46>2025-01-23T12:15:23.965512+01:00 iso-host rsyslogd: start  ",
            "decoded":{
                "priority":"46",
                "facility_keyword":"syslog",
                "level":"info",
                "facility":"5",
                "severity":"6",
                "format":"rfc3164",
                "timestamp":"2025-01-23T11:15:23.965512Z",
                "hostname":"iso-host",
                "app_name":"rsyslogd",
                "message":"start  "
            }
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-syslog-rfc5424" | unpack_syslog if (kind:=user) | fields case, app_name, priority"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-rfc5424",
            "app_name":"original"
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-syslog-rfc5424" | unpack_syslog if (kind:in(role:="syslog-dictionary" | fields lookup)) result_prefix selected. | fields case, selected.app_name"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-rfc5424",
            "selected":{"app_name":"app"}
        })],
        "query-backed conditions must resolve for unpack transforms"
    );

    let current_year = chrono::Utc::now().year();
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-syslog-rfc3164" | unpack_syslog offset 0s | fields case, format, timestamp, hostname, app_name, proc_id, message"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-rfc3164",
            "format":"rfc3164",
            "timestamp":format!("{current_year}-01-01T00:00:00Z"),
            "hostname":"classic-host",
            "app_name":"classic",
            "proc_id":"9",
            "message":"classic message"
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-syslog-cef" | unpack_syslog offset 0s | fields case, app_name, cef"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-cef",
            "app_name":"CEF",
            "cef":{
                "version":"1",
                "device_vendor":"Security",
                "device_product":"product",
                "device_version":"1.0",
                "device_event_class_id":"100",
                "name":"name|value",
                "severity":"10",
                "extension":{"src":"10.0.0.1", "msg":"two words"}
            }
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-syslog-cee" | unpack_syslog offset 0s | fields case, number, flag, null_value, nested, array, message"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-cee",
            "number":"123",
            "flag":"false",
            "nested":{"sibling":"retained", "leaf":"7"},
            "array":r#"[1,"x"]"#,
            "message":"cee message"
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-syslog-rfc5424" | unpack_syslog from absent | fields case, app_name"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-rfc5424",
            "app_name":"original"
        })]
    );

    for malformed in [
        "* | unpack_syslog if",
        "* | unpack_syslog from",
        "* | unpack_syslog from *",
        "* | unpack_syslog offset",
        "* | unpack_syslog offset invalid",
        "* | unpack_syslog result_prefix",
        "* | unpack_syslog keep_original_fields trailing",
        "* | unpack_syslog.extra",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="unpack-syslog-conflict" | unpack_syslog"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(
                r#"unpack_syslog_group:="unpack-syslog" | unpack_syslog | limit 10000"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-syslog-conflict" | fields case, conflict, _msg"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-conflict",
            "conflict":"scalar",
            "_msg":r#"<165>1 2023-06-03T17:42:00Z host app 1 ID [conflict value="changed"] tail"#
        })],
        "failed unpack_syslog must not mutate durable source values"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="unpack-syslog-rfc5424" | unpack_syslog result_prefix reopened. | fields case, reopened"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-syslog-rfc5424",
            "reopened":{
                "priority":"165",
                "facility_keyword":"local4",
                "level":"notice",
                "facility":"20",
                "severity":"5",
                "format":"rfc5424",
                "timestamp":"2023-06-03T17:42:32.123456789Z",
                "hostname":"host.example",
                "app_name":"app",
                "proc_id":"123",
                "msg_id":"ID47",
                "meta":{"keep":"yes"},
                "message":"test message  "
            }
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_unpack_words_is_exact_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("unpack-words-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "  foo,bar foo тест45_2 Ⅳz ²q e\u{301} १२  ".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-words-message",
                        "unpack_words_group":"unpack-words",
                        "source":"alpha alpha,βeta 42_7 Ⅳz ²q e\u{301} १२",
                        "words":"original",
                        "nested":{"sibling":"retained"}
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "info".into(),
                    message: "!#$%,".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-words-punctuation",
                        "unpack_words_group":"unpack-words",
                        "source":""
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_003,
                    level: 3,
                    severity: "warning".into(),
                    message: "missing source".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-words-missing",
                        "unpack_words_group":"unpack-words"
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_004,
                    level: 3,
                    severity: "warning".into(),
                    message: "rich source".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-words-rich",
                        "unpack_words_group":"unpack-words",
                        "numeric_source":123,
                        "array_source":["left","right"],
                        "object_source":{"leaf":"value"}
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_005,
                    level: 3,
                    severity: "warning".into(),
                    message: "destination conflict".into(),
                    metadata_json: serde_json::json!({
                        "case":"unpack-words-conflict",
                        "unpack_words_group":"unpack-words",
                        "source":"one two",
                        "nested":"scalar"
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    assert_eq!(
        pipeline_rows(&app, r#"case:="unpack-words-message" z q | fields case"#,).await,
        [serde_json::json!({"case":"unpack-words-message"})],
        "Letter_Number and Other_Number must remain word separators"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-words-message" | unpack_words | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-words-message",
            "_msg":r#"["foo","bar","foo","тест45_2","z","q","e","१२"]"#
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-words-message" | UnPaCk_WoRdS FrOm source As "word list" DrOp_DuPlIcAtEs | fields case, source, "word list", nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-words-message",
            "source":"alpha alpha,βeta 42_7 Ⅳz ²q e\u{301} १२",
            "word list":r#"["alpha","βeta","42_7","z","q","e","१२"]"#,
            "nested":{"sibling":"retained"}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-words-message" | unpack_words from source drop_duplicates | fields case, source"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-words-message",
            "source":r#"["alpha","βeta","42_7","z","q","e","१२"]"#
        })],
        "in-place unpack must snapshot the source before replacement"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-words-missing" | unpack_words from absent as words | fields case, words"#,
        )
        .await,
        [serde_json::json!({"case":"unpack-words-missing", "words":"[]"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-words-punctuation" | unpack_words as words | fields case, words"#,
        )
        .await,
        [serde_json::json!({"case":"unpack-words-punctuation", "words":"[]"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-words-rich" | unpack_words numeric_source numeric_words | unpack_words array_source array_words | unpack_words object_source object_words | fields case, numeric_words, array_words, object_words"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-words-rich",
            "numeric_words":r#"["123"]"#,
            "array_words":r#"["left","right"]"#,
            "object_words":"[]"
        })]
    );

    for malformed in [
        "* | unpack_words as",
        "* | unpack_words as *",
        "* | unpack_words from",
        "* | unpack_words from *",
        "* | unpack_words from source*",
        "* | unpack_words source destination trailing",
        "* | unpack_words drop_duplicates source",
        "* | unpack_words source, destination",
        "* | unpack_words.extra",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="unpack-words-conflict" | unpack_words source nested.words"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(
                r#"unpack_words_group:="unpack-words" | unpack_words as words | limit 10000"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unpack-words-conflict" | fields case, source, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-words-conflict",
            "source":"one two",
            "nested":"scalar"
        })],
        "failed unpack_words must not mutate durable source values"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="unpack-words-message" | unpack_words source reopened.words drop_duplicates | fields case, reopened"#,
        )
        .await,
        [serde_json::json!({
            "case":"unpack-words-message",
            "reopened":{"words":r#"["alpha","βeta","42_7","z","q","e","१२"]"#}
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_json_array_concat_is_exact_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("json-array-concat-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [LogEntry {
                ts: 1_800_000_000_000_001,
                level: 1,
                severity: "info".into(),
                message: r#"["message",1]"#.into(),
                metadata_json: serde_json::json!({
                    "case":"json-array-concat",
                    "array_concat_group":"json-array-concat",
                    "native_array":[null,"",0,false,[1],{"x":1}],
                    "text_array":" \t[\"a\",2,true,{\"x\":1},[null],NaN] \r\n",
                    "token_array":"[{\"z\":1, \"a\":\"\\u0061\"},1.00,-0,1e3,[NaN]]",
                    "empty_array":[],
                    "malformed_array":"[1,",
                    "scalar":"not-an-array",
                    "null_value":null,
                    "number":42,
                    "object":{"child":"value"},
                    "nested":{"array":[1,2,3],"sibling":"retained"},
                    "result":"original",
                    "left field":["quoted", "path"]
                })
                .to_string(),
            }]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let query = r#"case:="json-array-concat" | json_array_concat "," from native_array as native_joined | json_array_concat "|" text_array text_joined | json_array_concat ";" token_array token_joined | json_array_concat "," from empty_array as empty_joined | json_array_concat "," malformed_array malformed_joined | json_array_concat "," scalar scalar_joined | json_array_concat "," null_value null_joined | json_array_concat "," number number_joined | json_array_concat "," object object_joined | json_array_concat "," missing missing_joined | json_array_concat ";" nested.array nested.joined | json_array_concat "\n" from "left field" as lines | fields case, native_joined, text_joined, token_joined, empty_joined, malformed_joined, scalar_joined, null_joined, number_joined, object_joined, missing_joined, nested, lines"#;
    assert_eq!(
        pipeline_rows(&app, query).await,
        [serde_json::json!({
            "case":"json-array-concat",
            "native_joined":r#"null,,0,false,[1],{"x":1}"#,
            "text_joined":r#"a|2|true|{"x":1}|[null]|NaN"#,
            "token_joined":r#"{"z":1,"a":"\u0061"};1.00;-0;1e3;[NaN]"#,
            "empty_joined":"",
            "malformed_joined":"",
            "scalar_joined":"",
            "null_joined":"",
            "number_joined":"",
            "object_joined":"",
            "missing_joined":"",
            "nested":{"array":[1,2,3],"sibling":"retained","joined":"1;2;3"},
            "lines":"quoted\npath"
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="json-array-concat" | JsOn_ArRaY_CoNcAt | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({"case":"json-array-concat", "_msg":"message1"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="json-array-concat" | json_array_concat "," native_array result | fields case, native_array, result"#,
        )
        .await,
        [serde_json::json!({
            "case":"json-array-concat",
            "native_array":[null,"",0,false,[1],{"x":1}],
            "result":r#"null,,0,false,[1],{"x":1}"#
        })],
        "bare source and destination must preserve the native source"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="json-array-concat" | json_array_concat "," from native_array | fields case, native_array"#,
        )
        .await,
        [serde_json::json!({
            "case":"json-array-concat",
            "native_array":r#"null,,0,false,[1],{"x":1}"#
        })],
        "in-place concatenation must snapshot the source before replacement"
    );

    for malformed in [
        "* | json_array_concat from",
        r#"* | json_array_concat "," from *"#,
        r#"* | json_array_concat "," from source*"#,
        r#"* | json_array_concat "," from source as"#,
        r#"* | json_array_concat "," from source as *"#,
        r#"* | json_array_concat "," from source as result*"#,
        r#"* | json_array_concat "," from source result trailing"#,
        "* | json_array_concat.extra",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="json-array-concat" | json_array_concat "," native_array nested"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(
                r#"array_concat_group:="json-array-concat" | json_array_concat "," native_array joined | limit 10000"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="json-array-concat" | fields case, native_array, text_array, token_array, empty_array, malformed_array, scalar, null_value, number, object, nested, result, "left field""#,
        )
        .await,
        [serde_json::json!({
            "case":"json-array-concat",
            "native_array":[null,"",0,false,[1],{"x":1}],
            "text_array":" \t[\"a\",2,true,{\"x\":1},[null],NaN] \r\n",
            "token_array":"[{\"z\":1, \"a\":\"\\u0061\"},1.00,-0,1e3,[NaN]]",
            "empty_array":[],
            "malformed_array":"[1,",
            "scalar":"not-an-array",
            "null_value":null,
            "number":42,
            "object":{"child":"value"},
            "nested":{"array":[1,2,3],"sibling":"retained"},
            "result":"original",
            "left field":["quoted", "path"]
        })],
        "request-local concatenation must not mutate durable rich values"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="json-array-concat" | json_array_concat "," native_array reopened.joined | fields case, reopened, native_array"#,
        )
        .await,
        [serde_json::json!({
            "case":"json-array-concat",
            "reopened":{"joined":r#"null,,0,false,[1],{"x":1}"#},
            "native_array":[null,"",0,false,[1],{"x":1}]
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_json_array_len_is_typed_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("json-array-len-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [LogEntry {
                ts: 1_800_000_000_000_001,
                level: 1,
                severity: "info".into(),
                message: "array lengths".into(),
                metadata_json: serde_json::json!({
                    "case":"json-array-len",
                    "array_len_group":"json-array-len",
                    "native_array":[null,"",0,false,[1],{"x":1}],
                    "text_array":" \t[\"a\",2,true,{\"x\":1},[null],NaN] \r\n",
                    "empty_array":[],
                    "malformed_array":"[1,",
                    "scalar":"not-an-array",
                    "null_value":null,
                    "number":42,
                    "object":{"child":"value"},
                    "nested":{"array":[1,2,3],"sibling":"retained"},
                    "result":"original",
                    "left field":["quoted", "path"]
                })
                .to_string(),
            }]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let query = r#"case:="json-array-len" | json_array_len(native_array) as native_len | json_array_len text_array text_len | json_array_len(empty_array) empty_len | json_array_len(malformed_array) malformed_len | json_array_len(scalar) scalar_len | json_array_len(null_value) null_len | json_array_len(number) number_len | json_array_len(object) object_len | json_array_len(missing) missing_len | json_array_len(nested.array) nested_len | json_array_len("left field") as "quoted length" | fields case, native_len, text_len, empty_len, malformed_len, scalar_len, null_len, number_len, object_len, missing_len, nested_len, "quoted length""#;
    assert_eq!(
        pipeline_rows(&app, query).await,
        [serde_json::json!({
            "case":"json-array-len",
            "native_len":"6",
            "text_len":"6",
            "empty_len":"0",
            "malformed_len":"0",
            "scalar_len":"0",
            "null_len":"0",
            "number_len":"0",
            "object_len":"0",
            "missing_len":"0",
            "nested_len":"3",
            "quoted length":"2"
        })]
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="json-array-len" | JsOn_ArRaY_LeN(native_array) | fields case, _msg"#,
        )
        .await,
        [serde_json::json!({"case":"json-array-len", "_msg":"6"})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="json-array-len" | json_array_len(native_array) as result | json_array_len(native_array) as second | fields case, result, second"#,
        )
        .await,
        [serde_json::json!({"case":"json-array-len", "result":"6", "second":"6"})]
    );

    for malformed in [
        "* | json_array_len",
        "* | json_array_len()",
        "* | json_array_len(source, other)",
        "* | json_array_len(*)",
        "* | json_array_len(source*)",
        "* | json_array_len(source) result trailing",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="json-array-len" | json_array_len(native_array) as nested"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="json-array-len" | json_array_len(text_array) as length"#,
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let work_limited = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(work_limited.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(work_limited["reason"], "max_work_rows", "{work_limited}");

    let response_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_response_bytes: 8,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:="json-array-len" | json_array_len(native_array) as length"#,
    ))
    .await
    .unwrap();
    assert_eq!(response_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="json-array-len" | fields case, native_array, text_array, empty_array, malformed_array, scalar, null_value, number, object, nested, result, "left field""#,
        )
        .await,
        [serde_json::json!({
            "case":"json-array-len",
            "native_array":[null,"",0,false,[1],{"x":1}],
            "text_array":" \t[\"a\",2,true,{\"x\":1},[null],NaN] \r\n",
            "empty_array":[],
            "malformed_array":"[1,",
            "scalar":"not-an-array",
            "null_value":null,
            "number":42,
            "object":{"child":"value"},
            "nested":{"array":[1,2,3],"sibling":"retained"},
            "result":"original",
            "left field":["quoted", "path"]
        })],
        "json_array_len must not mutate durable rich source values"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(&router(reopened.clone()), query).await.len(),
        1
    );
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="json-array-len" | json_array_len(native_array) as length | fields case, length, native_array"#,
        )
        .await,
        [serde_json::json!({
            "case":"json-array-len",
            "length":"6",
            "native_array":[null,"",0,false,[1],{"x":1}]
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_unroll_is_exact_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("unroll-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                LogEntry {
                    ts: 1_800_000_000_000_001,
                    level: 1,
                    severity: "info".into(),
                    message: "expand arrays".into(),
                    metadata_json: serde_json::json!({
                        "case":"unroll-expand",
                        "unroll_group":"unroll",
                        "native_array":[null,"",0,false,[1],{"x":1}],
                        "text_array":" [\"a\",1.00,-0,1e3,{\"z\":1, \"a\":\"\\u0061\"},[NaN]] ",
                        "short_array":["x","y"],
                        "empty_array":[],
                        "malformed_array":"[1,",
                        "scalar":"not-an-array",
                        "nested":{"array":[1,2,3],"sibling":"retained"},
                        "left field":["quoted","path"]
                    })
                    .to_string(),
                },
                LogEntry {
                    ts: 1_800_000_000_000_002,
                    level: 1,
                    severity: "notice".into(),
                    message: "skip arrays".into(),
                    metadata_json: serde_json::json!({
                        "case":"unroll-skip",
                        "unroll_group":"unroll",
                        "native_array":["kept",2],
                        "nested":{"sibling":"skip-retained"}
                    })
                    .to_string(),
                },
            ]
            .into(),
        )
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let query = r#"case:="unroll-expand" | unroll by (native_array, text_array, short_array, missing) | fields case, native_array, text_array, short_array, missing, nested | limit 10000"#;
    let rows = pipeline_rows(&app, query).await;
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0]["native_array"], "null");
    assert_eq!(rows[0]["text_array"], "a");
    assert_eq!(rows[0]["short_array"], "x");
    assert_eq!(rows[0]["missing"], "");
    assert_eq!(rows[1]["native_array"], "");
    assert_eq!(rows[1]["text_array"], "1.00");
    assert_eq!(rows[1]["short_array"], "y");
    assert_eq!(rows[2]["native_array"], "0");
    assert_eq!(rows[2]["text_array"], "-0");
    assert_eq!(rows[3]["native_array"], "false");
    assert_eq!(rows[3]["text_array"], "1e3");
    assert_eq!(rows[4]["native_array"], "[1]");
    assert_eq!(rows[4]["text_array"], r#"{"z":1,"a":"a"}"#);
    assert_eq!(rows[5]["native_array"], r#"{"x":1}"#);
    assert_eq!(rows[5]["text_array"], "[NaN]");
    assert!(rows
        .iter()
        .all(|row| row["nested"]["sibling"] == "retained"));

    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unroll-expand" | unroll (empty_array, malformed_array, scalar, missing) | fields case, empty_array, malformed_array, scalar, missing"#,
        )
        .await,
        [serde_json::json!({
            "case":"unroll-expand",
            "empty_array":"",
            "malformed_array":"",
            "scalar":"",
            "missing":""
        })],
        "all empty or invalid arrays must still emit one row"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unroll-skip" | unroll if (case:="unroll-expand") native_array | fields case, native_array, nested"#,
        )
        .await,
        [serde_json::json!({
            "case":"unroll-skip",
            "native_array":["kept",2],
            "nested":{"sibling":"skip-retained"}
        })],
        "a false condition must preserve the complete rich row"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unroll-expand" | UnRoLl by ("left field", nested.array) | fields case, "left field", nested | limit 10000"#,
        )
        .await
        .len(),
        3,
        "quoted and dotted fields must use the longest array"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="unroll-expand" | unroll if (case:in(case:="unroll-expand" | fields case)) native_array | fields case, native_array | limit 10000"#,
        )
        .await
        .len(),
        6,
        "query-backed conditions must resolve under the parent request limits"
    );

    for malformed in [
        "* | unroll",
        "* | unroll by",
        "* | unroll ()",
        "* | unroll *",
        "* | unroll source*",
        "* | unroll (source,",
        "* | unroll source,",
        "* | unroll (source) trailing",
        "* | unroll (source) if (case:=x)",
        "* | unroll.extra",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="unroll-expand" | unroll (nested, nested.array)"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"case:="unroll-expand" | unroll scalar.child"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 5,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"unroll_group:="unroll" | fields case, native_array, text_array, short_array, empty_array, malformed_array, scalar, nested, "left field""#,
        )
        .await,
        [
            serde_json::json!({
                "case":"unroll-expand",
                "native_array":[null,"",0,false,[1],{"x":1}],
                "text_array":" [\"a\",1.00,-0,1e3,{\"z\":1, \"a\":\"\\u0061\"},[NaN]] ",
                "short_array":["x","y"],
                "empty_array":[],
                "malformed_array":"[1,",
                "scalar":"not-an-array",
                "nested":{"array":[1,2,3],"sibling":"retained"},
                "left field":["quoted","path"]
            }),
            serde_json::json!({
                "case":"unroll-skip",
                "native_array":["kept",2],
                "nested":{"sibling":"skip-retained"}
            })
        ],
        "request-local expansion must not mutate durable rich values"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        pipeline_rows(
            &router(reopened.clone()),
            r#"case:="unroll-expand" | unroll nested.array | fields case, nested | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"unroll-expand","nested":{"array":"1","sibling":"retained"}}),
            serde_json::json!({"case":"unroll-expand","nested":{"array":"2","sibling":"retained"}}),
            serde_json::json!({"case":"unroll-expand","nested":{"array":"3","sibling":"retained"}})
        ]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_join_is_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("join-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let entries = [
        (
            1,
            "left-a",
            serde_json::json!({
                "join_group":"left", "case":"a", "region":"us", "left":"kept",
                "collision":"left", "empty":"", "null_value":null,
                "nested":{"sibling":true}
            }),
        ),
        (
            2,
            "left-b",
            serde_json::json!({
                "join_group":"left", "case":"b", "region":"eu", "left":"only-b"
            }),
        ),
        (
            3,
            "left-number",
            serde_json::json!({
                "join_group":"left", "case":1, "left":"numeric"
            }),
        ),
        (
            4,
            "left-missing",
            serde_json::json!({
                "join_group":"left", "left":"missing-key"
            }),
        ),
        (
            5,
            "left-unmatched",
            serde_json::json!({
                "join_group":"left", "case":"unmatched", "left":"only"
            }),
        ),
        (
            6,
            "left-conflict",
            serde_json::json!({
                "join_group":"conflict", "case":"conflict", "nested":"scalar"
            }),
        ),
        (
            11,
            "right-a-first",
            serde_json::json!({
                "join_group":"right", "case":"a", "region":"us",
                "typed":[1,false,null,{"value":2}], "collision":"right",
                "empty":"filled", "null_value":"filled-null", "rank":"first"
            }),
        ),
        (
            12,
            "right-a-second",
            serde_json::json!({
                "join_group":"right", "case":"a", "region":"us", "rank":"second"
            }),
        ),
        (
            13,
            "right-b",
            serde_json::json!({
                "join_group":"right", "case":"b", "region":"eu"
            }),
        ),
        (
            14,
            "right-number",
            serde_json::json!({
                "join_group":"right", "case":"1", "numeric":true
            }),
        ),
        (
            15,
            "right-missing",
            serde_json::json!({
                "join_group":"right", "missing_fill":{"native":false}
            }),
        ),
    ]
    .into_iter()
    .map(|(offset, message, metadata)| LogEntry {
        ts: 1_800_000_000_000_000 + offset,
        level: 1,
        severity: "info".into(),
        message: message.into(),
        metadata_json: metadata.to_string(),
    })
    .collect();
    storage.ingest(entries).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let query = r#"join_group:="left" | sort by (_time) asc | join by (case) (join_group:="right" | sort by (_time) asc | fields case, typed, collision, empty, null_value, rank, numeric, missing_fill) | fields case, left, collision, empty, null_value, typed, rank, numeric, missing_fill, nested | limit 10000"#;
    let rows = pipeline_rows(&app, query).await;
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0]["case"], "a");
    assert_eq!(rows[0]["left"], "kept");
    assert_eq!(rows[0]["collision"], "left");
    assert_eq!(rows[0]["empty"], "filled");
    assert_eq!(rows[0]["null_value"], "filled-null");
    assert_eq!(
        rows[0]["typed"],
        serde_json::json!([1, false, null, {"value":2}])
    );
    assert_eq!(rows[0]["rank"], "first");
    assert_eq!(rows[0]["nested"]["sibling"], true);
    assert_eq!(rows[1]["rank"], "second");
    assert!(rows[1].get("typed").is_none());
    assert_eq!(rows[2], serde_json::json!({"case":"b","left":"only-b"}));
    assert_eq!(
        rows[3],
        serde_json::json!({"case":1,"left":"numeric","numeric":true})
    );
    assert_eq!(rows[4]["left"], "missing-key");
    assert_eq!(rows[4]["missing_fill"], serde_json::json!({"native":false}));
    assert_eq!(
        rows[5],
        serde_json::json!({"case":"unmatched","left":"only"})
    );

    let inner = pipeline_rows(&app, &format!("{query} | join by (case) rows() inner")).await;
    assert!(
        inner.is_empty(),
        "empty inline inner join must drop every row"
    );
    let inner = pipeline_rows(
        &app,
        r#"join_group:="left" | sort by (_time) asc | join by (case) (join_group:="right" | fields case, rank, numeric, missing_fill) inner | fields case, left, rank, numeric, missing_fill | limit 10000"#,
    )
    .await;
    assert_eq!(inner.len(), 5);
    assert!(inner.iter().all(|row| row["case"] != "unmatched"));

    assert_eq!(
        pipeline_rows(
            &app,
            r#"join_group:="left" case:="a" | join by (case, region) (join_group:="right" | sort by (_time) asc | fields case, region, rank) inner prefix joined. | fields case, region, joined | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a","region":"us","joined":{"rank":"first"}}),
            serde_json::json!({"case":"a","region":"us","joined":{"rank":"second"}})
        ],
        "multiple keys, inner mode, duplicate matches, and rich prefixes must compose"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"join_group:="left" case:="a" | join by (case) rows({"case":"a","inline":"yes","nested.value":"7"}) prefix right. | fields case, left, right"#,
        )
        .await,
        [serde_json::json!({
            "case":"a", "left":"kept",
            "right":{"inline":"yes","nested":{"value":"7"}}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"join_group:="left" case:="b" | join by (case) (join_group:="right" case:="b" | stats count() as total | format 'b' as case) | fields case, left, total"#,
        )
        .await,
        [serde_json::json!({"case":"b","left":"only-b","total":1})],
        "query-backed join sources must support complete nested pipelines"
    );

    for malformed in [
        "* | join",
        "* | join by () (*)",
        "* | join by (*) (*)",
        "* | join by (case)",
        "* | join by (case) ()",
        "* | join by (case) rows({case})",
        "* | join by (case) rows({,})",
        "* | join by (case) (*) prefix",
        "* | join by (case) (*) inner prefix right. inner",
        "* | join.extra by (case) (*)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let conflict = app
        .clone()
        .oneshot(logsql_request(
            r#"join_group:="conflict" | join by (case) rows({case:"conflict",nested.child:"x"})"#,
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let conflict = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(conflict.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(conflict["reason"], "field_conflict", "{conflict}");

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 5,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }
    assert_eq!(
        pipeline_rows(
            &app,
            r#"join_group:="left" | sort by (_time) asc | fields case, region, left, collision, empty, null_value, nested | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({
                "case":"a", "region":"us", "left":"kept", "collision":"left",
                "empty":"", "null_value":null, "nested":{"sibling":true}
            }),
            serde_json::json!({"case":"b","region":"eu","left":"only-b"}),
            serde_json::json!({"case":1,"left":"numeric"}),
            serde_json::json!({"left":"missing-key"}),
            serde_json::json!({"case":"unmatched","left":"only"})
        ],
        "request-local join composition must not mutate durable rich rows"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(pipeline_rows(&router(reopened.clone()), query).await, rows);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_union_is_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("union-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let entries = [
        (
            1,
            "left-a",
            serde_json::json!({
                "union_group":"left", "case":"left-a",
                "typed":[1,false,null], "nested":{"side":"left"}
            }),
        ),
        (
            2,
            "left-duplicate",
            serde_json::json!({"union_group":"left", "case":"duplicate"}),
        ),
        (
            11,
            "right-a",
            serde_json::json!({
                "union_group":"right", "case":"right-a",
                "typed":{"object":{"value":2}}, "nested":{"side":"right"}
            }),
        ),
        (
            12,
            "right-duplicate",
            serde_json::json!({"union_group":"right", "case":"duplicate"}),
        ),
    ]
    .into_iter()
    .map(|(offset, message, metadata)| LogEntry {
        ts: 1_800_000_000_000_000 + offset,
        level: 1,
        severity: "info".into(),
        message: message.into(),
        metadata_json: metadata.to_string(),
    })
    .collect();
    storage.ingest(entries).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let query = r#"union_group:="left" | sort by (_time) asc | fields case, typed, nested | union (union_group:="right" | sort by (_time) asc | fields case, typed, nested) | limit 10000"#;
    let rows = pipeline_rows(&app, query).await;
    assert_eq!(
        rows,
        [
            serde_json::json!({
                "case":"left-a", "typed":[1,false,null],
                "nested":{"side":"left"}
            }),
            serde_json::json!({"case":"duplicate"}),
            serde_json::json!({
                "case":"right-a", "typed":{"object":{"value":2}},
                "nested":{"side":"right"}
            }),
            serde_json::json!({"case":"duplicate"})
        ],
        "union must preserve complete rich rows, duplicates, and deterministic left/source order"
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"union_group:="left" case:="left-a" | fields case, nested | UnIoN RoWs({case=inline flag=true nested.value=7},{}) | fields case, nested, flag"#,
        )
        .await,
        [
            serde_json::json!({"case":"left-a","nested":{"side":"left"}}),
            serde_json::json!({"case":"inline","nested":{"value":"7"},"flag":"true"})
        ],
        "inline rows are textual and empty inline rows carry no result fields"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"union_group:="left" case:="left-a" | fields case | union (union_group:="right" case:="right-a" | fields case | union rows({case:nested})) | fields case"#,
        )
        .await,
        [
            serde_json::json!({"case":"left-a"}),
            serde_json::json!({"case":"right-a"}),
            serde_json::json!({"case":"nested"})
        ],
        "nested query-backed unions must append depth-first"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"union_group:="left" | union (union_group:="right") | stats count() as rows"#,
        )
        .await,
        [serde_json::json!({"rows":4})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"union_group:="left" case:="left-a" | fields case | union (union_group:="right" | stats count() as rows) | fields case, rows"#,
        )
        .await,
        [
            serde_json::json!({"case":"left-a"}),
            serde_json::json!({"rows":2})
        ],
        "query-backed statistics must append as ordinary rows"
    );

    for malformed in [
        "* | union",
        "* | union()",
        "* | union(foo | count)",
        "* | union (foo) trailing",
        "* | union rows",
        "* | union rows(",
        "* | union rows({case})",
        "* | union rows({,})",
        "* | union rows({case:[]})",
        "* | union.extra (*)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 3,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }
    let inherited_limit = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 3,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"union_group:="left" | union (union_group:="right" | limit 10000)"#,
    ))
    .await
    .unwrap();
    assert_eq!(inherited_limit.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let inherited_limit = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(inherited_limit.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(inherited_limit["reason"], "max_result_rows");

    assert_eq!(
        pipeline_rows(
            &app,
            r#"union_group:in("left","right") | sort by (_time) asc | fields case, typed, nested | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({
                "case":"left-a", "typed":[1,false,null],
                "nested":{"side":"left"}
            }),
            serde_json::json!({"case":"duplicate"}),
            serde_json::json!({
                "case":"right-a", "typed":{"object":{"value":2}},
                "nested":{"side":"right"}
            }),
            serde_json::json!({"case":"duplicate"})
        ],
        "request-local union composition must not mutate durable rows"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(pipeline_rows(&router(reopened.clone()), query).await, rows);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_running_stats_are_rich_bounded_chronological_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("running-stats-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let base = 1_800_000_000_000_000_i64;
    let entries = [
        (
            4,
            "b-four",
            serde_json::json!({
                "running_group":"running", "case":"b-four", "group":"b",
                "n":2, "source":{"nested":[1,false,null]},
                "prefix_values":{"direct":1,"nested":{"value":"2"}}
            }),
        ),
        (
            2,
            "a-two",
            serde_json::json!({
                "running_group":"running", "case":"a-two", "group":"a",
                "n":"2.5", "source":null
            }),
        ),
        (
            3,
            "b-three",
            serde_json::json!({
                "running_group":"running", "case":"b-three", "group":"b",
                "n":"10", "source":[1,false,null]
            }),
        ),
        (
            1,
            "a-one",
            serde_json::json!({
                "running_group":"running", "case":"a-one", "group":"a", "n":-2
            }),
        ),
    ]
    .into_iter()
    .map(|(offset, message, metadata)| LogEntry {
        ts: base + offset,
        level: 1,
        severity: "info".into(),
        message: message.into(),
        metadata_json: metadata.to_string(),
    })
    .collect();
    storage.ingest(entries).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let query = r#"running_group:="running" | running_stats by (group) count() as running_count, count(source) as source_count, sum(n) as running_sum, min(n) as running_min, max(n) as running_max, first(source) offset 1 as first_source, last(source) offset 1 as previous_source | fields case, running_count, source_count, running_sum, running_min, running_max, first_source, previous_source | limit 10000"#;
    let rows = pipeline_rows(&app, query).await;
    assert_eq!(
        rows,
        [
            serde_json::json!({
                "case":"a-one", "running_count":1, "source_count":0,
                "running_sum":-2.0, "running_min":-2, "running_max":-2,
                "first_source":"", "previous_source":""
            }),
            serde_json::json!({
                "case":"a-two", "running_count":2, "source_count":0,
                "running_sum":0.5, "running_min":-2, "running_max":"2.5",
                "first_source":null, "previous_source":""
            }),
            serde_json::json!({
                "case":"b-three", "running_count":1, "source_count":1,
                "running_sum":10.0, "running_min":"10", "running_max":"10",
                "first_source":"", "previous_source":""
            }),
            serde_json::json!({
                "case":"b-four", "running_count":2, "source_count":2,
                "running_sum":12.0, "running_min":2, "running_max":"10",
                "first_source":{"nested":[1,false,null]},
                "previous_source":[1,false,null]
            })
        ],
        "running_stats must use numeric time, independent groups, typed results, and rich source values"
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"running_group:="running" | running_stats count(), sum(n) | fields case, "count(*)", "sum(n)" | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","count(*)":1,"sum(n)":-2.0}),
            serde_json::json!({"case":"a-two","count(*)":2,"sum(n)":0.5}),
            serde_json::json!({"case":"b-three","count(*)":3,"sum(n)":10.5}),
            serde_json::json!({"case":"b-four","count(*)":4,"sum(n)":12.5})
        ],
        "omitted result names must be canonical and usable by later projections"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"running_group:="running" | running_stats count() as running | filter running:>2 | fields case, running | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"b-three","running":3}),
            serde_json::json!({"case":"b-four","running":4})
        ],
        "generated numeric fields must feed subsequent pipes"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"running_group:="running" | running_stats count() as source | fields case, source | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","source":1}),
            serde_json::json!({"case":"a-two","source":2}),
            serde_json::json!({"case":"b-three","source":3}),
            serde_json::json!({"case":"b-four","source":4})
        ],
        "running results deliberately overwrite an existing destination"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"running_group:="running" | running_stats sum(prefix_values.*) as nested_sum, count(prefix_values.nested.*) as nested_count | fields case, nested_sum, nested_count | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","nested_sum":"NaN","nested_count":0}),
            serde_json::json!({"case":"a-two","nested_sum":"NaN","nested_count":0}),
            serde_json::json!({"case":"b-three","nested_sum":"NaN","nested_count":0}),
            serde_json::json!({"case":"b-four","nested_sum":3.0,"nested_count":1})
        ],
        "running prefix selectors must traverse retained nested fields"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"running_group:="running" | math 0 / 0 as nan_value, 1 / 0 as positive_value, -1 / 0 as negative_value | running_stats sum(never_present) as missing_sum, sum(nan_value) as nan_sum, sum(positive_value) as positive_sum, sum(negative_value) as negative_sum | fields case, missing_sum, nan_sum, positive_sum, negative_sum | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","missing_sum":"NaN","nan_sum":"NaN","positive_sum":"NaN","negative_sum":"NaN"}),
            serde_json::json!({"case":"a-two","missing_sum":"NaN","nan_sum":"NaN","positive_sum":"NaN","negative_sum":"NaN"}),
            serde_json::json!({"case":"b-three","missing_sum":"NaN","nan_sum":"NaN","positive_sum":"NaN","negative_sum":"NaN"}),
            serde_json::json!({"case":"b-four","missing_sum":"NaN","nan_sum":"NaN","positive_sum":"NaN","negative_sum":"NaN"})
        ],
        "running sums must expose initial NaN and skip textual NaN or infinity"
    );

    for malformed in [
        "* | running_stats",
        "* | running_stats by",
        "* | running_stats foo()",
        "* | running_stats count",
        "* | running_stats by (*) count()",
        "* | running_stats by (host*) count()",
        "* | running_stats count() as x*",
        "* | running_stats sum() x, count() x",
        "* | running_stats first() as x",
        "* | running_stats first(a*) as x",
        "* | running_stats first(a, b) as x",
        "* | running_stats last(a) offset -1 as x",
        "* | running_stats first(a) offset nope as x",
        "* | running_stats count() as x trailing",
        "* | running_stats count() as x,,sum(n) as y",
        "* | running_stats sum(n",
        "* | running_stats.extra count()",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 3,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"running_group:="running" | sort by (_time) asc | fields case, source, n | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","n":-2}),
            serde_json::json!({"case":"a-two","source":null,"n":"2.5"}),
            serde_json::json!({"case":"b-three","source":[1,false,null],"n":"10"}),
            serde_json::json!({"case":"b-four","source":{"nested":[1,false,null]},"n":2})
        ],
        "running_stats must never mutate stored rows"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(pipeline_rows(&router(reopened.clone()), query).await, rows);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_total_stats_are_rich_bounded_chronological_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("total-stats-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        2,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let base = 1_800_100_000_000_000_i64;
    let entries = [
        (
            4,
            "b-four",
            serde_json::json!({
                "total_group":"total", "case":"b-four", "group":"b",
                "n":2, "source":{"nested":[1,false,null]},
                "prefix_values":{"direct":1,"nested":{"value":"2"}}
            }),
        ),
        (
            2,
            "a-two",
            serde_json::json!({
                "total_group":"total", "case":"a-two", "group":"a",
                "n":"2.5", "source":null
            }),
        ),
        (
            3,
            "b-three",
            serde_json::json!({
                "total_group":"total", "case":"b-three", "group":"b",
                "n":"10", "source":[1,false,null]
            }),
        ),
        (
            1,
            "a-one",
            serde_json::json!({
                "total_group":"total", "case":"a-one", "group":"a", "n":-2
            }),
        ),
    ]
    .into_iter()
    .map(|(offset, message, metadata)| LogEntry {
        ts: base + offset,
        level: 1,
        severity: "info".into(),
        message: message.into(),
        metadata_json: metadata.to_string(),
    })
    .collect();
    storage.ingest(entries).await.unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let query = r#"total_group:="total" | total_stats by (group) count() as total_count, count(source) as source_count, sum(n) as total_sum, min(n) as total_min, max(n) as total_max, first(source) offset 1 as first_source, last(source) offset 1 as previous_source | fields case, total_count, source_count, total_sum, total_min, total_max, first_source, previous_source | limit 10000"#;
    let rows = pipeline_rows(&app, query).await;
    assert_eq!(
        rows,
        [
            serde_json::json!({
                "case":"a-one", "total_count":2, "source_count":0,
                "total_sum":0.5, "total_min":-2, "total_max":"2.5",
                "first_source":null, "previous_source":""
            }),
            serde_json::json!({
                "case":"a-two", "total_count":2, "source_count":0,
                "total_sum":0.5, "total_min":-2, "total_max":"2.5",
                "first_source":null, "previous_source":""
            }),
            serde_json::json!({
                "case":"b-three", "total_count":2, "source_count":2,
                "total_sum":12.0, "total_min":2, "total_max":"10",
                "first_source":{"nested":[1,false,null]},
                "previous_source":[1,false,null]
            }),
            serde_json::json!({
                "case":"b-four", "total_count":2, "source_count":2,
                "total_sum":12.0, "total_min":2, "total_max":"10",
                "first_source":{"nested":[1,false,null]},
                "previous_source":[1,false,null]
            })
        ],
        "total_stats must compute complete typed group state before writing every row"
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"total_group:="total" | total_stats count(), sum(n) | fields case, "count(*)", "sum(n)" | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","count(*)":4,"sum(n)":12.5}),
            serde_json::json!({"case":"a-two","count(*)":4,"sum(n)":12.5}),
            serde_json::json!({"case":"b-three","count(*)":4,"sum(n)":12.5}),
            serde_json::json!({"case":"b-four","count(*)":4,"sum(n)":12.5})
        ],
        "omitted total result names must be canonical and visible to projection"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"total_group:="total" | total_stats count() as total | filter total:>3 | fields case, total | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","total":4}),
            serde_json::json!({"case":"a-two","total":4}),
            serde_json::json!({"case":"b-three","total":4}),
            serde_json::json!({"case":"b-four","total":4})
        ],
        "final totals must feed subsequent pipes"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"total_group:="total" | total_stats count() as source | fields case, source | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","source":4}),
            serde_json::json!({"case":"a-two","source":4}),
            serde_json::json!({"case":"b-three","source":4}),
            serde_json::json!({"case":"b-four","source":4})
        ],
        "total results deliberately overwrite an existing destination"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"total_group:="total" | total_stats sum(prefix_values.*) as nested_sum, count(prefix_values.nested.*) as nested_count | fields case, nested_sum, nested_count | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","nested_sum":3.0,"nested_count":1}),
            serde_json::json!({"case":"a-two","nested_sum":3.0,"nested_count":1}),
            serde_json::json!({"case":"b-three","nested_sum":3.0,"nested_count":1}),
            serde_json::json!({"case":"b-four","nested_sum":3.0,"nested_count":1})
        ],
        "total prefix selectors must traverse retained nested fields"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"total_group:="total" | math 0 / 0 as nan_value, 1 / 0 as positive_value, -1 / 0 as negative_value | total_stats sum(never_present) as missing_sum, sum(nan_value) as nan_sum, sum(positive_value) as positive_sum, sum(negative_value) as negative_sum | fields case, missing_sum, nan_sum, positive_sum, negative_sum | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","missing_sum":"NaN","nan_sum":"NaN","positive_sum":"NaN","negative_sum":"NaN"}),
            serde_json::json!({"case":"a-two","missing_sum":"NaN","nan_sum":"NaN","positive_sum":"NaN","negative_sum":"NaN"}),
            serde_json::json!({"case":"b-three","missing_sum":"NaN","nan_sum":"NaN","positive_sum":"NaN","negative_sum":"NaN"}),
            serde_json::json!({"case":"b-four","missing_sum":"NaN","nan_sum":"NaN","positive_sum":"NaN","negative_sum":"NaN"})
        ],
        "total sums must expose initial NaN and skip textual NaN or infinity"
    );

    for malformed in [
        "* | total_stats",
        "* | total_stats by",
        "* | total_stats foo()",
        "* | total_stats count",
        "* | total_stats by (*) count()",
        "* | total_stats by (host*) count()",
        "* | total_stats count() as x*",
        "* | total_stats sum() x, count() x",
        "* | total_stats first() as x",
        "* | total_stats first(a*) as x",
        "* | total_stats first(a, b) as x",
        "* | total_stats last(a) offset -1 as x",
        "* | total_stats first(a) offset nope as x",
        "* | total_stats count() as x trailing",
        "* | total_stats count() as x,,sum(n) as y",
        "* | total_stats sum(n",
        "* | total_stats.extra count()",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 3,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }

    assert_eq!(
        pipeline_rows(
            &app,
            r#"total_group:="total" | sort by (_time) asc | fields case, source, n | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"a-one","n":-2}),
            serde_json::json!({"case":"a-two","source":null,"n":"2.5"}),
            serde_json::json!({"case":"b-three","source":[1,false,null],"n":"10"}),
            serde_json::json!({"case":"b-four","source":{"nested":[1,false,null]},"n":2})
        ],
        "total_stats must never mutate stored rows"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(pipeline_rows(&router(reopened.clone()), query).await, rows);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_time_add_is_rich_bounded_composable_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("time-add-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let base = 1_800_200_000_000_000_i64;
    storage
        .ingest(vec![
            LogEntry {
                ts: base + 1,
                level: 1,
                severity: "info".into(),
                message: "valid".into(),
                metadata_json: serde_json::json!({
                    "time_add_group":"time-add", "case":"valid",
                    "stamp":"2024-05-30T01:02:03.123456789+02:30",
                    "nested":{"stamp":"2024-05-30 01:02:03.000000100Z"}
                })
                .to_string(),
            },
            LogEntry {
                ts: base + 2,
                level: 2,
                severity: "notice".into(),
                message: "rich".into(),
                metadata_json: serde_json::json!({
                    "time_add_group":"time-add", "case":"rich",
                    "stamp":123, "nested":{"stamp":null},
                    "array":[1,false,null], "object":{"keep":"yes"}
                })
                .to_string(),
            },
            LogEntry {
                ts: base + 3,
                level: 3,
                severity: "warning".into(),
                message: "missing".into(),
                metadata_json: serde_json::json!({
                    "time_add_group":"time-add", "case":"missing"
                })
                .to_string(),
            },
        ])
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let query = r#"time_add_group:="time-add" | sort by (_time) asc | time_add 500ns at stamp | time_add 1h at nested.stamp | fields case, stamp, nested, array, object | limit 10000"#;
    let rows = pipeline_rows(&app, query).await;
    assert_eq!(
        rows,
        [
            serde_json::json!({
                "case":"valid",
                "stamp":"2024-05-29T22:32:03.123457289Z",
                "nested":{"stamp":"2024-05-30T02:02:03.0000001Z"}
            }),
            serde_json::json!({
                "case":"rich", "stamp":123, "nested":{"stamp":null},
                "array":[1,false,null], "object":{"keep":"yes"}
            }),
            serde_json::json!({"case":"missing"})
        ]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"time_add_group:="time-add" | sort by (_time) asc | time_add 1ns | fields case, _time | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({"case":"valid","_time":"2027-01-17T15:33:20.000001001Z"}),
            serde_json::json!({"case":"rich","_time":"2027-01-17T15:33:20.000002001Z"}),
            serde_json::json!({"case":"missing","_time":"2027-01-17T15:33:20.000003001Z"})
        ],
        "default time_add must retain nanosecond response precision beyond stored microseconds"
    );

    for malformed in [
        "* | time_add",
        "* | time_add at stamp",
        "* | time_add 1d at",
        "* | time_add 1hour",
        "* | time_add +1h",
        "* | time_add 1h at stamp trailing",
        "* | time_add 1h at stamp*",
        "* | time_add.extra 1h",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let response = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(query))
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(body["reason"], "max_work_rows", "{body}");

    assert_eq!(
        pipeline_rows(
            &app,
            r#"time_add_group:="time-add" | sort by (_time) asc | fields case, stamp, nested, array, object | limit 10000"#,
        )
        .await,
        [
            serde_json::json!({
                "case":"valid", "stamp":"2024-05-30T01:02:03.123456789+02:30",
                "nested":{"stamp":"2024-05-30 01:02:03.000000100Z"}
            }),
            serde_json::json!({
                "case":"rich", "stamp":123, "nested":{"stamp":null},
                "array":[1,false,null], "object":{"keep":"yes"}
            }),
            serde_json::json!({"case":"missing"})
        ],
        "time_add must never mutate stored rows"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(pipeline_rows(&router(reopened.clone()), query).await, rows);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_generate_sequence_is_scan_free_bounded_composable_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("generate-sequence-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(vec![LogEntry {
            ts: 1_800_300_000_000_001,
            level: 3,
            severity: "error".into(),
            message: "durable source".into(),
            metadata_json: serde_json::json!({
                "sequence_group":"source", "case":"source",
                "nested":{"keep":[1,false,null]}
            })
            .to_string(),
        }])
        .await
        .unwrap();
    storage.flush().await.unwrap();
    let app = router(storage.clone());

    let before = storage.stats().await.unwrap().query_count;
    let query = r#"sequence_group:=does-not-exist | generate_sequence 3"#;
    let rows = pipeline_rows(&app, query).await;
    assert_eq!(
        rows,
        [
            serde_json::json!({"_msg":"0"}),
            serde_json::json!({"_msg":"1"}),
            serde_json::json!({"_msg":"2"})
        ],
        "generate_sequence must not depend on matching durable input"
    );
    assert_eq!(
        storage.stats().await.unwrap().query_count,
        before,
        "an input-independent sequence must not scan the public logs table"
    );

    assert_eq!(
        pipeline_rows(
            &app,
            r#"sequence_group:=source | limit 1 | generate_sequence 2 | generate_sequence 3 | math (_msg + 10) as value | fields _msg, value"#,
        )
        .await,
        [
            serde_json::json!({"_msg":"0","value":"10"}),
            serde_json::json!({"_msg":"1","value":"11"}),
            serde_json::json!({"_msg":"2","value":"12"})
        ],
        "the last sequence replaces all prior rows and feeds later pipes"
    );
    assert_eq!(
        pipeline_rows(&app, r#"* | generate_sequence "3.9" | filter _msg:="1""#).await,
        [serde_json::json!({"_msg":"1"})],
        "the upstream parser accepts quoted fractional N and truncates it"
    );
    assert_eq!(
        pipeline_rows(&app, "* | generate_sequence 1_0 | offset 8").await,
        [
            serde_json::json!({"_msg":"8"}),
            serde_json::json!({"_msg":"9"})
        ]
    );
    assert_eq!(
        pipeline_rows(&app, "* | generate_sequence 3ns").await,
        [
            serde_json::json!({"_msg":"0"}),
            serde_json::json!({"_msg":"1"}),
            serde_json::json!({"_msg":"2"})
        ],
        "generate_sequence shares VictoriaLogs numeric duration parsing"
    );

    for malformed in [
        "* | generate_sequence",
        "* | generate_sequence 0",
        "* | generate_sequence 0.9",
        "* | generate_sequence -1",
        "* | generate_sequence +3",
        "* | generate_sequence nope",
        "* | generate_sequence 3 trailing",
        "* | generate_sequence.3",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 2,
                ..LogsQueryLimits::default()
            },
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 2,
                ..LogsQueryLimits::default()
            },
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_response_bytes: 8,
                ..LogsQueryLimits::default()
            },
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{body}");
    }

    let source_query = r#"sequence_group:=source | fields case, nested, _msg, level | limit 10000"#;
    let source_rows = pipeline_rows(&app, source_query).await;
    assert_eq!(
        source_rows,
        [serde_json::json!({
            "case":"source", "nested":{"keep":[1,false,null]},
            "_msg":"durable source", "level":"error"
        })],
        "sequence generation must not mutate durable source rows"
    );

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(pipeline_rows(&reopened_app, query).await, rows);
    assert_eq!(
        pipeline_rows(&reopened_app, source_query).await,
        source_rows
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_ten_relative_logsql_pins_inclusive_lower_exclusive_upper_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("relative-logsql.db");
    let query_now = 1_800_000_000_123_456;
    let lower = query_now - 300_000_000;
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [lower - 1, lower, query_now - 1, query_now]
                .into_iter()
                .map(|ts| LogEntry {
                    ts,
                    level: 1,
                    severity: "info".into(),
                    message: format!("edge-{ts}"),
                    metadata_json: "{\"service\":\"clock\"}".into(),
                })
                .collect(),
        )
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    let mut plan = parse_logsql_at("_time:5m", TimestampUnit::Microseconds, query_now).unwrap();
    assert_eq!(plan.output, LogsqlOutput::Rows);
    plan.spec.descending = false;
    plan.spec.limit = 10;
    let rows = storage.query(plan.spec.clone()).await.unwrap();
    assert_eq!(
        rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
        vec![lower, query_now - 1]
    );
    let mut empty = parse_logsql_at("_time:0s", TimestampUnit::Microseconds, query_now).unwrap();
    empty.spec.descending = false;
    empty.spec.limit = 10;
    assert!(storage.query(empty.spec.clone()).await.unwrap().is_empty());

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        reopened
            .query(plan.spec)
            .await
            .unwrap()
            .iter()
            .map(|row| row.ts)
            .collect::<Vec<_>>(),
        vec![lower, query_now - 1]
    );
    assert!(reopened.query(empty.spec).await.unwrap().is_empty());
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_ten_absolute_logsql_preserves_range_and_comparison_edges_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("absolute-logsql.db");
    let base = 1_800_000_000_000_000;
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [0, 1, 2, 3, 4, 5, 1_000_000, 2_000_000, 3_000_000]
                .into_iter()
                .map(|offset| LogEntry {
                    ts: base + offset,
                    level: 1,
                    severity: "info".into(),
                    message: format!("absolute-{offset}"),
                    metadata_json: "{\"service\":\"clock\"}".into(),
                })
                .collect(),
        )
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    let plans = [
        (
            "_time:[2027-01-15T08:00:00.000001Z, 2027-01-15T08:00:00.000004Z)",
            vec![base + 1, base + 2, base + 3],
        ),
        (
            "_time:(2027-01-15T08:00:00.000001Z, 2027-01-15T08:00:00.000004Z]",
            vec![base + 2, base + 3, base + 4],
        ),
        (
            "_time:>=2027-01-15T08:00:00.000001Z _time:<2027-01-15T08:00:00.000004Z",
            vec![base + 1, base + 2, base + 3],
        ),
        (
            "_time:>2027-01-15T08:00:00.000001Z _time:<=2027-01-15T08:00:00.000004Z",
            vec![base + 2, base + 3, base + 4],
        ),
        (
            "_time:[1800000001,1800000003)",
            vec![base + 1_000_000, base + 2_000_000],
        ),
        (
            "_time:[1800000001000,1800000003000)",
            vec![base + 1_000_000, base + 2_000_000],
        ),
        (
            "_time:[1800000001000000,1800000003000000)",
            vec![base + 1_000_000, base + 2_000_000],
        ),
        (
            "_time:[1800000001000000000,1800000003000000000)",
            vec![base + 1_000_000, base + 2_000_000],
        ),
    ];
    for (query, expected) in &plans {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 10;
        assert_eq!(
            storage
                .query(plan.spec)
                .await
                .unwrap()
                .iter()
                .map(|row| row.ts)
                .collect::<Vec<_>>(),
            *expected
        );
    }

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in plans {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 10;
        assert_eq!(
            reopened
                .query(plan.spec)
                .await
                .unwrap()
                .iter()
                .map(|row| row.ts)
                .collect::<Vec<_>>(),
            expected
        );
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_ten_typed_metadata_equality_distinguishes_nested_missing_null_empty_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("typed-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let body = [
        r#"{"_time":1800000000000001,"_msg":"typed-match","level":"info","app":"my app","status":"500","nested":{"ok":true,"count":2,"none":null,"empty":""}}"#,
        r#"{"_time":1800000000000002,"_msg":"bool-string","level":"info","app":"my app","status":"500","nested":{"ok":"true","count":2,"none":null,"empty":""}}"#,
        r#"{"_time":1800000000000003,"_msg":"number-string","level":"info","app":"my app","status":"500","nested":{"ok":true,"count":"2","none":null,"empty":""}}"#,
        r#"{"_time":1800000000000004,"_msg":"null-missing","level":"info","app":"my app","status":"500","nested":{"ok":true,"count":2,"empty":""}}"#,
        r#"{"_time":1800000000000005,"_msg":"empty-null","level":"info","app":"my app","status":"500","nested":{"ok":true,"count":2,"none":null,"empty":null}}"#,
        r#"{"_time":1800000000000006,"_msg":"numeric-status","level":"info","app":"my app","status":500,"nested":{"ok":true,"count":2,"none":null,"empty":""}}"#,
    ]
    .join("\n");
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    let exact = r#"app:"my app" status:500 nested.ok:=true nested.count:=2 nested.none:=null nested.empty:"" | limit 10"#;
    let response = app.clone().oneshot(logsql_request(exact)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let rows = ndjson_values(&body);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["_msg"], "typed-match");
    assert_eq!(rows[0]["nested"]["ok"], true);
    assert_eq!(rows[0]["nested"]["count"], 2);
    assert!(rows[0]["nested"]["none"].is_null());
    assert_eq!(rows[0]["nested"]["empty"], "");

    let response = app
        .clone()
        .oneshot(logsql_request("status:=500"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let rows = ndjson_values(&body);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["_msg"], "numeric-status");

    let response = app
        .clone()
        .oneshot(logsql_request(
            "nested.none:=null nested.empty:\"\" | stats count()",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
        b"{\"total\":4}\n"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let response = router(reopened.clone())
        .oneshot(logsql_request(exact))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(ndjson_values(&body)[0]["_msg"], "typed-match");
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_pattern_match_filters_match_victorialogs_and_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("pattern-match-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(vec![
            LogEntry {
                ts: 1_800_000_000_000_001,
                level: 1,
                severity: "info".into(),
                message: "prefix user_id=123, ip=45.67.89.12, time=2025-10-20T23:32:12Z suffix"
                    .into(),
                metadata_json: r#"{"service":"pattern","code":"job-42"}"#.into(),
            },
            LogEntry {
                ts: 1_800_000_000_000_002,
                level: 1,
                severity: "info".into(),
                message: "alpha".into(),
                metadata_json: r#"{"service":"pattern","code":"other"}"#.into(),
            },
            LogEntry {
                ts: 1_800_000_000_000_003,
                level: 1,
                severity: "info".into(),
                message: "a x nope a x 12 y".into(),
                metadata_json: r#"{"service":"pattern","n":42,"flag":true,"empty":"","nullish":null,"list":[1,"x"],"nested":{"value":true},"uuid":"2edfed59-3e98-4073-bbb2-28d321ca71a7","date":"2025/10/20","time":"10:20:30,123","word":"\"hello world\"","nd":"१२","no":"²","nl":"Ⅳ","mark":"é"}"#.into(),
            },
        ])
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    let queries = [
        (
            r#"pattern_match("user_id=<N>, ip=<IP4>, time=<DATETIME>")"#,
            vec![1_800_000_000_000_001],
        ),
        (
            r#"pattern_match_prefix("prefix <W>")"#,
            vec![1_800_000_000_000_001],
        ),
        (
            r#"pattern_match_suffix("suffix")"#,
            vec![1_800_000_000_000_001],
        ),
        (r#"pattern_match_full("<W>")"#, vec![1_800_000_000_000_002]),
        (
            r#"code:pattern_match_full("job-<N>")"#,
            vec![1_800_000_000_000_001],
        ),
        (
            r#"code:PaTtErN_MaTcH_FuLl("job-<N>")"#,
            vec![1_800_000_000_000_001],
        ),
        (r#"pattern_match("a x <N> y")"#, vec![1_800_000_000_000_003]),
        (
            r#"n:pattern_match_full("<N>")"#,
            vec![1_800_000_000_000_003],
        ),
        (
            r#"flag:pattern_match_full("true")"#,
            vec![1_800_000_000_000_003],
        ),
        (
            r#"list:pattern_match_full(`[1,"x"]`)"#,
            vec![1_800_000_000_000_003],
        ),
        (
            r#"nested:pattern_match_full(`{"value":true}`)"#,
            vec![1_800_000_000_000_003],
        ),
        (
            r#"empty:pattern_match_full("")"#,
            vec![
                1_800_000_000_000_001,
                1_800_000_000_000_002,
                1_800_000_000_000_003,
            ],
        ),
        (
            r#"nullish:pattern_match_full("")"#,
            vec![
                1_800_000_000_000_001,
                1_800_000_000_000_002,
                1_800_000_000_000_003,
            ],
        ),
        (
            r#"uuid:pattern_match_full("<UUID>")"#,
            vec![1_800_000_000_000_003],
        ),
        (
            r#"date:pattern_match_full("<DATE>")"#,
            vec![1_800_000_000_000_003],
        ),
        (
            r#"time:pattern_match_full("<TIME>")"#,
            vec![1_800_000_000_000_003],
        ),
        (
            r#"word:pattern_match_full("<W>")"#,
            vec![1_800_000_000_000_003],
        ),
        (
            r#"nd:pattern_match_full("<W>")"#,
            vec![1_800_000_000_000_003],
        ),
        (r#"no:pattern_match_full("<W>")"#, vec![]),
        (r#"nl:pattern_match_full("<W>")"#, vec![]),
        (r#"mark:pattern_match_full("<W>")"#, vec![]),
    ];
    for (query, expected) in &queries {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 10;
        assert_eq!(
            storage
                .query(plan.spec)
                .await
                .unwrap()
                .iter()
                .map(|row| row.ts)
                .collect::<Vec<_>>(),
            *expected,
            "{query}"
        );
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(&app, r#"* | filter n:pattern_match_full("<N>") | limit 10"#,)
            .await
            .len(),
        1
    );
    let malformed = app
        .clone()
        .oneshot(logsql_request("pattern_match()"))
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(malformed.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "malformed_logsql"
    );
    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(r#"pattern_match("a x <N> y") | limit 10"#))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        pipeline_rows(&app, r#"pattern_match_full("alpha") | limit 10"#)
            .await
            .len(),
        1
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 10;
        assert_eq!(
            reopened
                .query(plan.spec)
                .await
                .unwrap()
                .iter()
                .map(|row| row.ts)
                .collect::<Vec<_>>(),
            expected,
            "{query}"
        );
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_exact_prefix_matches_rich_fields_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("exact-prefix-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(vec![
            LogEntry {
                ts: 1_810_000_000_000_001,
                level: 1,
                severity: "info".into(),
                message: "Processing request 42".into(),
                metadata_json: r#"{"kind":"processor","attempt":42,"flag":true,"items":[1,"x"],"nested":{"value":true},"empty":"","nullish":null,"unicode":"१२"}"#.into(),
            },
            LogEntry {
                ts: 1_810_000_000_000_002,
                level: 1,
                severity: "info".into(),
                message: "start: Processing request".into(),
                metadata_json: r#"{"kind":"other"}"#.into(),
            },
            LogEntry {
                ts: 1_810_000_000_000_003,
                level: 1,
                severity: "info".into(),
                message: "processing".into(),
                metadata_json: "{}".into(),
            },
        ])
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    let queries = [
        (r#"="Processing request"*"#, vec![1_810_000_000_000_001]),
        (
            r#"exact("Processing request 42")"#,
            vec![1_810_000_000_000_001],
        ),
        (r#"=processing"#, vec![1_810_000_000_000_003]),
        (r#"exact(Processing*)"#, vec![1_810_000_000_000_001]),
        (r#"kind:="process"*"#, vec![1_810_000_000_000_001]),
        (r#"attempt:="4"*"#, vec![1_810_000_000_000_001]),
        (r#"flag:="tr"*"#, vec![1_810_000_000_000_001]),
        (r#"items:=`[1,"`*"#, vec![1_810_000_000_000_001]),
        (r#"nested:=`{"value":`*"#, vec![1_810_000_000_000_001]),
        (
            r#"missing:=""*"#,
            vec![
                1_810_000_000_000_001,
                1_810_000_000_000_002,
                1_810_000_000_000_003,
            ],
        ),
        (
            r#"nullish:=""*"#,
            vec![
                1_810_000_000_000_001,
                1_810_000_000_000_002,
                1_810_000_000_000_003,
            ],
        ),
        (r#"unicode:="१"*"#, vec![1_810_000_000_000_001]),
        (
            r#"="Processing"* AND NOT ="processing"*"#,
            vec![1_810_000_000_000_001],
        ),
    ];
    for (query, expected) in &queries {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 10;
        assert_eq!(
            storage
                .query(plan.spec)
                .await
                .unwrap()
                .iter()
                .map(|row| row.ts)
                .collect::<Vec<_>>(),
            *expected,
            "{query}"
        );
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(&app, r#"* | filter attempt:="4"* | limit 10"#)
            .await
            .len(),
        1
    );
    for malformed in ["exact()", "exact(foo, bar)"] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "malformed_logsql",
            "{malformed}"
        );
    }
    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(r#"="Processing"* | limit 10"#))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        pipeline_rows(&app, r#"="Processing"* | limit 10"#)
            .await
            .len(),
        1
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 10;
        assert_eq!(
            reopened
                .query(plan.spec)
                .await
                .unwrap()
                .iter()
                .map(|row| row.ts)
                .collect::<Vec<_>>(),
            expected,
            "reopened: {query}"
        );
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_multi_exact_matches_rich_fields_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("multi-exact-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(vec![
            LogEntry {
                ts: 1_811_000_000_000_001,
                level: 1,
                severity: "info".into(),
                message: "missing".into(),
                metadata_json:
                    r#"{"case":"missing","service":"api gateway","unicode":"éalpha beta"}"#.into(),
            },
            LogEntry {
                ts: 1_811_000_000_000_002,
                level: 1,
                severity: "info".into(),
                message: "null".into(),
                metadata_json: r#"{"case":"null","probe":null,"nested":{"leaf":null}}"#.into(),
            },
            LogEntry {
                ts: 1_811_000_000_000_003,
                level: 1,
                severity: "info".into(),
                message: "empty".into(),
                metadata_json:
                    r#"{"case":"empty","probe":"","nested":{"leaf":""},"tags":[]}"#
                        .into(),
            },
            LogEntry {
                ts: 1_811_000_000_000_004,
                level: 1,
                severity: "info".into(),
                message: "string".into(),
                metadata_json:
                    r#"{"case":"string","probe":"value","nested":{"leaf":"value"},"tags":["prod",""]}"#
                        .into(),
            },
            LogEntry {
                ts: 1_811_000_000_000_005,
                level: 1,
                severity: "info".into(),
                message: "zero".into(),
                metadata_json:
                    r#"{"case":"zero","probe":0,"nested":{"leaf":0},"tags":[123,true,false,null,"123"]}"#
                        .into(),
            },
            LogEntry {
                ts: 1_811_000_000_000_006,
                level: 1,
                severity: "info".into(),
                message: "false".into(),
                metadata_json:
                    r#"{"case":"false","probe":false,"nested":{"leaf":false},"tags":[{"a":"b"},["a"],"leaf"]}"#
                        .into(),
            },
            LogEntry {
                ts: 1_811_000_000_000_007,
                level: 1,
                severity: "info".into(),
                message: "array".into(),
                metadata_json:
                    r#"{"case":"array","probe":[1,"x"],"nested":{"leaf":[1,"x"]},"tags":["a\"b","a\nb","a\/b","a\u0062","*"]}"#
                        .into(),
            },
            LogEntry {
                ts: 1_811_000_000_000_008,
                level: 1,
                severity: "info".into(),
                message: "object".into(),
                metadata_json:
                    r#"{"case":"object","probe":{"ok":true},"nested":{"leaf":{"ok":true}},"tags":{"key":"prod"}}"#.into(),
            },
        ])
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    async fn timestamps(storage: &Storage, query: &str) -> Vec<i64> {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 100;
        storage
            .query(plan.spec)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.ts)
            .collect()
    }

    let queries = [
        (
            r#"in(missing, string)"#,
            vec![1_811_000_000_000_001, 1_811_000_000_000_004],
        ),
        (
            r#"case:in(missing, string, missing,)"#,
            vec![1_811_000_000_000_001, 1_811_000_000_000_004],
        ),
        (
            r#"probe:in("", 0, false, value)"#,
            vec![
                1_811_000_000_000_001,
                1_811_000_000_000_002,
                1_811_000_000_000_003,
                1_811_000_000_000_004,
                1_811_000_000_000_005,
                1_811_000_000_000_006,
            ],
        ),
        (
            r#"probe:in(`[1,"x"]`, `{"ok":true}`)"#,
            vec![1_811_000_000_000_007, 1_811_000_000_000_008],
        ),
        (
            r#"nested.leaf:in("", 0, false, value, `[1,"x"]`, `{"ok":true}`)"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (r#"case:in("*", string)"#, vec![1_811_000_000_000_004]),
        (
            r#"never:in(nope, *)"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (
            r#"never:contains_any(*)"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (
            r#"never:contains_all(*)"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (
            r#"never:contains_any(nope, *)"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (
            r#"never:contains_all(*, nope)"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (
            r#"service:in(*)"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (
            r#"level:contains_all(*)"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (r#"contains_all(missing)"#, vec![1_811_000_000_000_001]),
        (r#"case:contains_all(miss, ing)"#, Vec::new()),
        (r#"probe:contains_all(value)"#, vec![1_811_000_000_000_004]),
        (r#"probe:contains_all(1, x)"#, vec![1_811_000_000_000_007]),
        (
            r#"probe:contains_all(ok, true)"#,
            vec![1_811_000_000_000_008],
        ),
        (
            r#"nested.leaf:contains_all(ok, true)"#,
            vec![1_811_000_000_000_008],
        ),
        (
            r#"never:contains_all()"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (
            r#"never:contains_all("")"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (r#"never:contains_all(value)"#, Vec::new()),
        (r#"case:contains_all("*")"#, Vec::new()),
        (
            r#"case:contains_all("", missing, missing,)"#,
            vec![1_811_000_000_000_001],
        ),
        (
            r#"level:contains_all(info)"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (
            r#"service:contains_all(api, gateway)"#,
            vec![1_811_000_000_000_001],
        ),
        (
            r#"unicode:contains_all(éalpha, beta)"#,
            vec![1_811_000_000_000_001],
        ),
        (r#"unicode:contains_all(alpha)"#, Vec::new()),
        (
            r#"contains_any(missing, object)"#,
            vec![1_811_000_000_000_001, 1_811_000_000_000_008],
        ),
        (
            r#"case:contains_any(missing, object, missing,)"#,
            vec![1_811_000_000_000_001, 1_811_000_000_000_008],
        ),
        (
            r#"probe:contains_any(value, false)"#,
            vec![1_811_000_000_000_004, 1_811_000_000_000_006],
        ),
        (
            r#"probe:contains_any(1, ok)"#,
            vec![1_811_000_000_000_007, 1_811_000_000_000_008],
        ),
        (
            r#"nested.leaf:contains_any(1, ok)"#,
            vec![1_811_000_000_000_007, 1_811_000_000_000_008],
        ),
        (r#"never:contains_any()"#, Vec::new()),
        (
            r#"never:contains_any("")"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (
            r#"never:contains_any(value, "")"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (r#"never:contains_any(value)"#, Vec::new()),
        (r#"case:contains_any("*")"#, Vec::new()),
        (
            r#"level:contains_any(debug, info)"#,
            (1_811_000_000_000_001..=1_811_000_000_000_008).collect(),
        ),
        (
            r#"service:contains_any(gateway, absent)"#,
            vec![1_811_000_000_000_001],
        ),
        (
            r#"unicode:contains_any(alpha, beta)"#,
            vec![1_811_000_000_000_001],
        ),
        (r#"unicode:contains_any(alpha)"#, Vec::new()),
        (
            r#"tags:json_array_contains_any(prod, absent)"#,
            vec![1_811_000_000_000_004],
        ),
        (
            r#"tags:json_array_contains_any(123)"#,
            vec![1_811_000_000_000_005],
        ),
        (
            r#"tags:json_array_contains_any(true, false, null)"#,
            vec![1_811_000_000_000_005],
        ),
        (
            r#"tags:json_array_contains_any("")"#,
            vec![1_811_000_000_000_004],
        ),
        (
            r#"tags:json_array_contains_any(leaf)"#,
            vec![1_811_000_000_000_006],
        ),
        (
            r#"tags:json_array_contains_any(`{"a":"b"}`, `["a"]`)"#,
            Vec::new(),
        ),
        (
            r#"tags:json_array_contains_any("a\"b", "a\nb", a/b, ab, "*")"#,
            vec![1_811_000_000_000_007],
        ),
        (r#"tags:json_array_contains_any()"#, Vec::new()),
        (r#"level:json_array_contains_any(info)"#, Vec::new()),
        (r#"service:json_array_contains_any(api)"#, Vec::new()),
        (r#"case:in()"#, Vec::new()),
        (
            r#"case:in(missing, string) AND NOT case:in(string)"#,
            vec![1_811_000_000_000_001],
        ),
        (
            r#"case:in(missing, string) AND NOT never:contains_all(*)"#,
            Vec::new(),
        ),
    ];
    for (query, expected) in &queries {
        assert_eq!(timestamps(&storage, query).await, *expected, "{query}");
    }

    let app = router(storage.clone());
    let mut pipeline_cases = pipeline_rows(
        &app,
        r#"* | filter probe:in(false, `[1,"x"]`) | fields case | limit 100"#,
    )
    .await
    .into_iter()
    .map(|row| row["case"].as_str().unwrap().to_owned())
    .collect::<Vec<_>>();
    pipeline_cases.sort();
    assert_eq!(pipeline_cases, ["array", "false"]);

    let mut noop_pipeline_cases = pipeline_rows(
        &app,
        r#"* | filter never:contains_any(*) | fields case | limit 100"#,
    )
    .await
    .into_iter()
    .map(|row| row["case"].as_str().unwrap().to_owned())
    .collect::<Vec<_>>();
    noop_pipeline_cases.sort();
    assert_eq!(
        noop_pipeline_cases,
        ["array", "empty", "false", "missing", "null", "object", "string", "zero"]
    );

    let mut contains_all_pipeline_cases = pipeline_rows(
        &app,
        r#"* | filter probe:contains_all(1, x) | fields case | limit 100"#,
    )
    .await
    .into_iter()
    .map(|row| row["case"].as_str().unwrap().to_owned())
    .collect::<Vec<_>>();
    contains_all_pipeline_cases.sort();
    assert_eq!(contains_all_pipeline_cases, ["array"]);

    let mut contains_any_pipeline_cases = pipeline_rows(
        &app,
        r#"* | filter probe:contains_any(false, ok) | fields case | limit 100"#,
    )
    .await
    .into_iter()
    .map(|row| row["case"].as_str().unwrap().to_owned())
    .collect::<Vec<_>>();
    contains_any_pipeline_cases.sort();
    assert_eq!(contains_any_pipeline_cases, ["false", "object"]);

    let mut json_array_pipeline_cases = pipeline_rows(
        &app,
        r#"* | filter tags:json_array_contains_any(false, leaf) | fields case | limit 100"#,
    )
    .await
    .into_iter()
    .map(|row| row["case"].as_str().unwrap().to_owned())
    .collect::<Vec<_>>();
    json_array_pipeline_cases.sort();
    assert_eq!(json_array_pipeline_cases, ["false", "zero"]);

    for malformed in [
        "in",
        "in(",
        "in(,value)",
        "in(value other)",
        "in(value*)",
        "contains_any",
        "contains_any(",
        "contains_any(,value)",
        "contains_any(value other)",
        "contains_any(value*)",
        "contains_any(* value)",
        "tags:json_array_contains_any(",
        "tags:json_array_contains_any(,prod)",
        "tags:json_array_contains_any(prod other)",
        "tags:json_array_contains_any(*)",
        "contains_all(,*)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "malformed_logsql",
            "{malformed}"
        );
    }
    let query_backed_cases = pipeline_rows(
        &app,
        "case:in(case:string | fields case) | fields case | limit 100",
    )
    .await
    .into_iter()
    .map(|row| row["case"].as_str().unwrap().to_owned())
    .collect::<Vec<_>>();
    assert_eq!(query_backed_cases, ["string"]);

    for query_backed in [
        "case:contains_all(missing | fields case)",
        "case:contains_any(missing | fields case)",
    ] {
        let query = format!("{query_backed} | fields case | limit 100");
        let cases = pipeline_rows(&app, &query)
            .await
            .into_iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(cases, ["missing"], "{query_backed}");
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("case:in(missing, string) | limit 100"))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        timestamps(&storage, "case:in(missing, string)").await.len(),
        2
    );

    let limited_contains_all = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("case:contains_all(missing) | limit 100"))
    .await
    .unwrap();
    assert_eq!(
        limited_contains_all.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited_contains_all.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(timestamps(&storage, "contains_all(missing)").await.len(), 1);

    let limited_contains_any = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "case:contains_any(missing, object) | limit 100",
    ))
    .await
    .unwrap();
    assert_eq!(
        limited_contains_any.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited_contains_any.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        timestamps(&storage, "contains_any(missing, object)")
            .await
            .len(),
        2
    );

    let limited_json_array = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "tags:json_array_contains_any(prod, leaf) | limit 100",
    ))
    .await
    .unwrap();
    assert_eq!(
        limited_json_array.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited_json_array.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        timestamps(&storage, "tags:json_array_contains_any(prod, leaf)")
            .await
            .len(),
        2
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        assert_eq!(
            timestamps(&reopened, query).await,
            expected,
            "reopened: {query}"
        );
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_ipv4_range_matches_retained_strings_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("ipv4-range-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(vec![
            LogEntry {
                ts: 1_812_000_000_000_001,
                level: 1,
                severity: "info".into(),
                message: "missing".into(),
                metadata_json: r#"{"case":"missing"}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_002,
                level: 1,
                severity: "info".into(),
                message: "null".into(),
                metadata_json: r#"{"case":"null","ip":null}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_003,
                level: 1,
                severity: "info".into(),
                message: "numeric".into(),
                metadata_json: r#"{"case":"numeric","ip":167772161}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_004,
                level: 1,
                severity: "info".into(),
                message: "network".into(),
                metadata_json: r#"{"case":"network","ip":"10.0.0.0"}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_005,
                level: 1,
                severity: "info".into(),
                message: "low".into(),
                metadata_json: r#"{"case":"low","ip":"10.0.0.1"}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_006,
                level: 1,
                severity: "info".into(),
                message: "leading zeros".into(),
                metadata_json: r#"{"case":"leading-zero","ip":"010.000.000.002"}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_007,
                level: 1,
                severity: "info".into(),
                message: "broadcast".into(),
                metadata_json: r#"{"case":"broadcast","ip":"10.0.0.255"}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_008,
                level: 1,
                severity: "info".into(),
                message: "outside".into(),
                metadata_json: r#"{"case":"outside","ip":"10.0.1.0"}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_009,
                level: 1,
                severity: "info".into(),
                message: "invalid".into(),
                metadata_json: r#"{"case":"invalid","ip":"10.0.0.256"}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_010,
                level: 1,
                severity: "info".into(),
                message: "embedded".into(),
                metadata_json: r#"{"case":"embedded","ip":"before 10.0.0.1"}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_011,
                level: 1,
                severity: "info".into(),
                message: "nested".into(),
                metadata_json: r#"{"case":"nested","nested":{"ip":"10.0.0.3"}}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_012,
                level: 1,
                severity: "info".into(),
                message: "service".into(),
                metadata_json: r#"{"case":"service","service":"10.0.0.4"}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_013,
                level: 1,
                severity: "info".into(),
                message: "10.0.0.42".into(),
                metadata_json: r#"{"case":"message"}"#.into(),
            },
            LogEntry {
                ts: 1_812_000_000_000_014,
                level: 1,
                severity: "info".into(),
                message: "before 10.0.0.42".into(),
                metadata_json: r#"{"case":"message-embedded"}"#.into(),
            },
        ])
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    async fn cases(storage: &Storage, query: &str) -> Vec<String> {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 100;
        storage
            .query(plan.spec)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.metadata_json).unwrap()["case"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    let queries = [
        (
            "ip:ipv4_range(10.0.0.0/24)",
            vec!["network", "low", "leading-zero", "broadcast"],
        ),
        (
            "ip:ipv4_range(10.0.0.1, 10.0.1.0)",
            vec!["low", "leading-zero", "broadcast", "outside"],
        ),
        ("ip:ipv4_range(10.0.0.1)", vec!["low"]),
        (
            "ip:IpV4_RaNgE(0.0.0.0/0)",
            vec!["network", "low", "leading-zero", "broadcast", "outside"],
        ),
        ("ip:ipv4_range(10.0.1.0, 10.0.0.0)", Vec::new()),
        ("nested.ip:ipv4_range(10.0.0.3)", vec!["nested"]),
        ("service:ipv4_range(10.0.0.4)", vec!["service"]),
        ("ipv4_range(10.0.0.42)", vec!["message"]),
        (
            "ip:ipv4_range(10.0.0.0/24) AND NOT ip:ipv4_range(10.0.0.128/25)",
            vec!["network", "low", "leading-zero"],
        ),
    ];
    for (query, expected) in &queries {
        assert_eq!(cases(&storage, query).await, *expected, "{query}");
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(
            &app,
            "* | filter ip:ipv4_range(10.0.0.0/24) | fields case | limit 100",
        )
        .await
        .into_iter()
        .map(|row| row["case"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>(),
        ["network", "low", "leading-zero", "broadcast"]
    );

    for malformed in [
        "ipv4_range(",
        "ipv4_range()",
        "ipv4_range(10.0.0.256)",
        "ipv4_range(10.0.0.1/33)",
        "ipv4_range(10.0.0.1, 10.0.0)",
        "ipv4_range(10.0.0.1, 10.0.0.2, 10.0.0.3)",
        "ipv4_range(10.0.0.1 10.0.0.2)",
        "ipv4_range(10.0.0.1*)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "malformed_logsql",
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("ip:ipv4_range(10.0.0.0/24) | limit 100"))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        cases(&storage, "ip:ipv4_range(10.0.0.0/24)").await.len(),
        4,
        "the reader must remain reusable after a bounded-work rejection"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        assert_eq!(cases(&reopened, query).await, expected, "reopened: {query}");
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_ipv6_range_matches_retained_strings_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("ipv6-range-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(vec![
            LogEntry {
                ts: 1_813_000_000_000_001,
                level: 1,
                severity: "info".into(),
                message: "missing".into(),
                metadata_json: r#"{"case":"missing"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_002,
                level: 1,
                severity: "info".into(),
                message: "null".into(),
                metadata_json: r#"{"case":"null","ip":null}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_003,
                level: 1,
                severity: "info".into(),
                message: "numeric".into(),
                metadata_json: r#"{"case":"numeric","ip":6}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_004,
                level: 1,
                severity: "info".into(),
                message: "network".into(),
                metadata_json: r#"{"case":"network","ip":"2001:db8::"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_005,
                level: 1,
                severity: "info".into(),
                message: "low".into(),
                metadata_json: r#"{"case":"low","ip":"2001:db8::1"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_006,
                level: 1,
                severity: "info".into(),
                message: "uppercase".into(),
                metadata_json: r#"{"case":"uppercase","ip":"2001:DB8::2"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_007,
                level: 1,
                severity: "info".into(),
                message: "upper".into(),
                metadata_json: r#"{"case":"upper","ip":"2001:db8::ffff"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_008,
                level: 1,
                severity: "info".into(),
                message: "outside".into(),
                metadata_json: r#"{"case":"outside","ip":"2001:db8:0:1::"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_009,
                level: 1,
                severity: "info".into(),
                message: "invalid".into(),
                metadata_json: r#"{"case":"invalid","ip":"2001:db8:::1"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_010,
                level: 1,
                severity: "info".into(),
                message: "embedded".into(),
                metadata_json: r#"{"case":"embedded","ip":"before 2001:db8::1"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_011,
                level: 1,
                severity: "info".into(),
                message: "nested".into(),
                metadata_json: r#"{"case":"nested","nested":{"ip":"2001:db8::3"}}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_012,
                level: 1,
                severity: "info".into(),
                message: "service".into(),
                metadata_json: r#"{"case":"service","service":"2001:db8::4"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_013,
                level: 1,
                severity: "info".into(),
                message: "2001:db8::42".into(),
                metadata_json: r#"{"case":"message"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_014,
                level: 1,
                severity: "info".into(),
                message: "[2001:db8::42]:443".into(),
                metadata_json: r#"{"case":"message-embedded"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_015,
                level: 1,
                severity: "info".into(),
                message: "mapped low".into(),
                metadata_json: r#"{"case":"mapped-low","ip":"1.2.3.4"}"#.into(),
            },
            LogEntry {
                ts: 1_813_000_000_000_016,
                level: 1,
                severity: "info".into(),
                message: "mapped outside".into(),
                metadata_json: r#"{"case":"mapped-outside","ip":"9.0.0.0"}"#.into(),
            },
        ])
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    async fn cases(storage: &Storage, query: &str) -> Vec<String> {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 100;
        storage
            .query(plan.spec)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.metadata_json).unwrap()["case"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    let queries = [
        (
            "ip:ipv6_range(2001:db8::/112)",
            vec!["network", "low", "uppercase", "upper"],
        ),
        (
            "ip:ipv6_range(2001:db8::34/112,)",
            vec!["network", "low", "uppercase", "upper"],
        ),
        (
            "ip:ipv6_range(2001:db8::1, 2001:db8:0:1::)",
            vec!["low", "uppercase", "upper", "outside"],
        ),
        ("ip:ipv6_range(2001:DB8::1)", vec!["low"]),
        (
            "ip:IpV6_RaNgE(::/0)",
            vec![
                "network",
                "low",
                "uppercase",
                "upper",
                "outside",
                "mapped-low",
                "mapped-outside",
            ],
        ),
        ("ip:ipv6_range(2001:db8:0:1::, 2001:db8::)", Vec::new()),
        ("nested.ip:ipv6_range(2001:db8::3)", vec!["nested"]),
        ("service:ipv6_range(2001:db8::4)", vec!["service"]),
        ("ipv6_range(2001:db8::42)", vec!["message"]),
        ("ip:ipv6_range(1.2.3.4, 8.0.0.0)", vec!["mapped-low"]),
        ("ip:ipv6_range(1.2.3.99/120)", vec!["mapped-low"]),
        (
            "ip:ipv6_range(2001:db8::/112) AND NOT ip:ipv6_range(2001:db8::8000/113)",
            vec!["network", "low", "uppercase"],
        ),
    ];
    for (query, expected) in &queries {
        assert_eq!(cases(&storage, query).await, *expected, "{query}");
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(
            &app,
            "* | filter ip:ipv6_range(2001:db8::/112) | fields case | limit 100",
        )
        .await
        .into_iter()
        .map(|row| row["case"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>(),
        ["network", "low", "uppercase", "upper"]
    );

    for malformed in [
        "ipv6_range(",
        "ipv6_range()",
        "ipv6_range(2001:db8:::1)",
        "ipv6_range(2001:db8::1/129)",
        "ipv6_range(2001:db8::1, 2001:db8::gg)",
        "ipv6_range(::1, ::2, ::3)",
        "ipv6_range(::1 ::2)",
        "ipv6_range(::1*)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "malformed_logsql",
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("ip:ipv6_range(2001:db8::/112) | limit 100"))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        cases(&storage, "ip:ipv6_range(2001:db8::/112)").await.len(),
        4,
        "the reader must remain reusable after a bounded-work rejection"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        assert_eq!(cases(&reopened, query).await, expected, "reopened: {query}");
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_string_range_matches_rich_fields_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("string-range-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(vec![
            LogEntry {
                ts: 1_814_000_000_000_001,
                level: 1,
                severity: "info".into(),
                message: "missing".into(),
                metadata_json: r#"{"case":"missing"}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_002,
                level: 1,
                severity: "info".into(),
                message: "null".into(),
                metadata_json: r#"{"case":"null","probe":null}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_003,
                level: 1,
                severity: "info".into(),
                message: "empty".into(),
                metadata_json: r#"{"case":"empty","probe":""}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_004,
                level: 1,
                severity: "info".into(),
                message: "lower".into(),
                metadata_json: r#"{"case":"lower","probe":"alpha"}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_005,
                level: 1,
                severity: "info".into(),
                message: "inside".into(),
                metadata_json: r#"{"case":"inside","probe":"alpha2"}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_006,
                level: 1,
                severity: "info".into(),
                message: "upper".into(),
                metadata_json: r#"{"case":"upper","probe":"beta"}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_007,
                level: 1,
                severity: "info".into(),
                message: "case".into(),
                metadata_json: r#"{"case":"case","probe":"Alpha"}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_008,
                level: 1,
                severity: "info".into(),
                message: "unicode low".into(),
                metadata_json: r#"{"case":"unicode-low","probe":"éclair"}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_009,
                level: 1,
                severity: "info".into(),
                message: "unicode upper".into(),
                metadata_json: r#"{"case":"unicode-upper","probe":"ê"}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_010,
                level: 1,
                severity: "info".into(),
                message: "numeric".into(),
                metadata_json: r#"{"case":"numeric","probe":123}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_011,
                level: 1,
                severity: "info".into(),
                message: "boolean".into(),
                metadata_json: r#"{"case":"boolean","probe":true}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_012,
                level: 1,
                severity: "info".into(),
                message: "array".into(),
                metadata_json: r#"{"case":"array","probe":["alpha"]}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_013,
                level: 1,
                severity: "info".into(),
                message: "object".into(),
                metadata_json: r#"{"case":"object","probe":{"key":"alpha"}}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_014,
                level: 1,
                severity: "info".into(),
                message: "nested".into(),
                metadata_json: r#"{"case":"nested","nested":{"probe":"alpha3"}}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_015,
                level: 1,
                severity: "info".into(),
                message: "service".into(),
                metadata_json: r#"{"case":"service","service":"alpha4"}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_016,
                level: 1,
                severity: "info".into(),
                message: "middle".into(),
                metadata_json: r#"{"case":"message"}"#.into(),
            },
            LogEntry {
                ts: 1_814_000_000_000_017,
                level: 1,
                severity: "info".into(),
                message: "zulu".into(),
                metadata_json: r#"{"case":"message-outside"}"#.into(),
            },
        ])
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    async fn cases(storage: &Storage, query: &str) -> Vec<String> {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 100;
        storage
            .query(plan.spec)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.metadata_json).unwrap()["case"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    let queries = [
        ("probe:string_range(alpha, beta)", vec!["lower", "inside"]),
        ("probe:string_range(alpha, alpha2)", vec!["lower"]),
        (
            r#"probe:string_range("", b)"#,
            vec![
                "missing",
                "null",
                "empty",
                "lower",
                "inside",
                "case",
                "numeric",
                "array",
                "nested",
                "service",
                "message",
                "message-outside",
            ],
        ),
        ("probe:string_range(A, B)", vec!["case"]),
        (r#"probe:string_range("é", "ê")"#, vec!["unicode-low"]),
        ("probe:string_range(100, 200)", vec!["numeric"]),
        ("probe:string_range(true, truez)", vec!["boolean"]),
        (r#"probe:string_range("[", "\\")"#, vec!["array"]),
        (r#"probe:string_range("{", "|")"#, vec!["object"]),
        ("probe:string_range(alpha, alpha)", Vec::new()),
        ("probe:string_range(z, a)", Vec::new()),
        ("nested.probe:string_range(alpha, beta)", vec!["nested"]),
        ("service:string_range(alpha, beta)", vec!["service"]),
        ("string_range(l, n)", vec!["missing", "lower", "message"]),
        (
            "probe:string_range(alpha, beta) AND NOT probe:string_range(alpha2, beta)",
            vec!["lower"],
        ),
    ];
    for (query, expected) in &queries {
        assert_eq!(cases(&storage, query).await, *expected, "{query}");
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(
            &app,
            "* | filter probe:string_range(alpha, beta) | fields case | limit 100",
        )
        .await
        .into_iter()
        .map(|row| row["case"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>(),
        ["lower", "inside"]
    );

    for malformed in [
        "string_range(",
        "string_range()",
        "string_range(alpha)",
        "string_range(alpha, beta, gamma)",
        "string_range(alpha beta)",
        "string_range(alpha*, beta)",
        "string_range(, beta)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "malformed_logsql",
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "probe:string_range(alpha, beta) | limit 100",
    ))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        cases(&storage, "probe:string_range(alpha, beta)")
            .await
            .len(),
        2,
        "the reader must remain reusable after a bounded-work rejection"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        assert_eq!(cases(&reopened, query).await, expected, "reopened: {query}");
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_len_range_matches_codepoints_rich_fields_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("len-range-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let entry = |offset: i64, message: &str, metadata_json: &str| LogEntry {
        ts: 1_815_000_000_000_000 + offset,
        level: 1,
        severity: "info".into(),
        message: message.into(),
        metadata_json: metadata_json.into(),
    };
    storage
        .ingest(vec![
            entry(1, "missing", r#"{"case":"missing"}"#),
            entry(2, "null", r#"{"case":"null","probe":null}"#),
            entry(3, "empty", r#"{"case":"empty","probe":""}"#),
            entry(4, "lower", r#"{"case":"lower","probe":"alpha"}"#),
            entry(5, "inside", r#"{"case":"inside","probe":"alpha2"}"#),
            entry(6, "upper", r#"{"case":"upper","probe":"beta"}"#),
            entry(7, "case", r#"{"case":"case","probe":"Alpha"}"#),
            entry(
                8,
                "unicode low",
                r#"{"case":"unicode-low","probe":"éclair"}"#,
            ),
            entry(
                9,
                "unicode upper",
                r#"{"case":"unicode-upper","probe":"ê"}"#,
            ),
            entry(10, "numeric", r#"{"case":"numeric","probe":123}"#),
            entry(11, "boolean", r#"{"case":"boolean","probe":true}"#),
            entry(12, "array", r#"{"case":"array","probe":["alpha"]}"#),
            entry(13, "object", r#"{"case":"object","probe":{"key":"alpha"}}"#),
            entry(
                14,
                "nested",
                r#"{"case":"nested","nested":{"probe":"alpha3"}}"#,
            ),
            entry(15, "service", r#"{"case":"service","service":"alpha4"}"#),
            entry(16, "middle", r#"{"case":"message"}"#),
            entry(17, "zulu", r#"{"case":"message-outside"}"#),
        ])
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    async fn cases(storage: &Storage, query: &str) -> Vec<String> {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 100;
        storage
            .query(plan.spec)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.metadata_json).unwrap()["case"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    let queries = [
        (
            "probe:len_range(5, 6)",
            vec!["lower", "inside", "case", "unicode-low"],
        ),
        (
            r#"probe:LeN_RaNgE("5", 0b110,)"#,
            vec!["lower", "inside", "case", "unicode-low"],
        ),
        (
            "probe:len_range(0x5, 6B)",
            vec!["lower", "inside", "case", "unicode-low"],
        ),
        ("probe:len_range(1, 1)", vec!["unicode-upper"]),
        (
            "probe:len_range(0, 0)",
            vec![
                "missing",
                "null",
                "empty",
                "nested",
                "service",
                "message",
                "message-outside",
            ],
        ),
        ("probe:len_range(3, 4)", vec!["upper", "numeric", "boolean"]),
        ("probe:len_range(9, 9)", vec!["array"]),
        ("probe:len_range(15, 15)", vec!["object"]),
        ("nested.probe:len_range(6, 6)", vec!["nested"]),
        ("service:len_range(6, 6)", vec!["service"]),
        (
            "len_range(6, 6)",
            vec!["inside", "object", "nested", "message"],
        ),
        (
            "probe:len_range(5, 6) AND NOT probe:len_range(6, 6)",
            vec!["lower", "case"],
        ),
        ("probe:len_range(6, 5)", Vec::new()),
    ];
    for (query, expected) in &queries {
        assert_eq!(cases(&storage, query).await, *expected, "{query}");
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(
            &app,
            "* | filter probe:len_range(5, 6) | fields case | limit 100",
        )
        .await
        .into_iter()
        .map(|row| row["case"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>(),
        ["lower", "inside", "case", "unicode-low"]
    );

    for malformed in [
        "len_range(",
        "len_range()",
        "len_range(1)",
        "len_range(1, 2, 3)",
        "len_range(foo, bar)",
        "len_range(-1, 2)",
        "len_range(1.2, 3.4)",
        "len_range(1, 2",
        "len_range(1 2)",
        "len_range(1,,2)",
        "len_range(08, 9)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "malformed_logsql",
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("probe:len_range(5, 6) | limit 100"))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        cases(&storage, "probe:len_range(5, 6)").await.len(),
        4,
        "the reader must remain reusable after a bounded-work rejection"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        assert_eq!(cases(&reopened, query).await, expected, "reopened: {query}");
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_field_comparisons_match_numeric_lexical_and_rich_rows_after_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("field-comparison-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let entry = |offset: i64, message: &str, metadata_json: &str| LogEntry {
        ts: 1_816_000_000_000_000 + offset,
        level: 1,
        severity: "info".into(),
        message: message.into(),
        metadata_json: metadata_json.into(),
    };
    storage
        .ingest(vec![
            entry(1, "missing", r#"{"case":"missing"}"#),
            entry(
                2,
                "nulls",
                r#"{"case":"nulls","left":null,"right":null}"#,
            ),
            entry(
                3,
                "empty",
                r#"{"case":"empty","left":"","right":""}"#,
            ),
            entry(
                4,
                "left only",
                r#"{"case":"left-only","left":"x"}"#,
            ),
            entry(
                5,
                "right only",
                r#"{"case":"right-only","right":"x"}"#,
            ),
            entry(
                6,
                "equal text",
                r#"{"case":"equal-text","left":"alpha","right":"alpha"}"#,
            ),
            entry(
                7,
                "lexical less",
                r#"{"case":"lexical-less","left":"bar","right":"foo"}"#,
            ),
            entry(
                8,
                "lexical greater",
                r#"{"case":"lexical-greater","left":"foo","right":"bar"}"#,
            ),
            entry(
                9,
                "numeric less",
                r#"{"case":"numeric-less","left":2,"right":"10"}"#,
            ),
            entry(
                10,
                "numeric greater",
                r#"{"case":"numeric-greater","left":"10","right":2}"#,
            ),
            entry(
                11,
                "numeric equal",
                r#"{"case":"numeric-equal","left":2,"right":"2"}"#,
            ),
            entry(
                12,
                "duration less",
                r#"{"case":"duration-less","left":"500ms","right":"1s"}"#,
            ),
            entry(
                13,
                "duration greater",
                r#"{"case":"duration-greater","left":"1s","right":"500ms"}"#,
            ),
            entry(
                14,
                "bytes less",
                r#"{"case":"bytes-less","left":"1000B","right":"1KiB"}"#,
            ),
            entry(
                15,
                "timestamp less",
                r#"{"case":"timestamp-less","left":"2026-01-01T00:00:00Z","right":"2026-01-01T00:00:01Z"}"#,
            ),
            entry(
                16,
                "ipv4 less",
                r#"{"case":"ipv4-less","left":"10.0.0.2","right":"10.0.0.10"}"#,
            ),
            entry(
                17,
                "fallback less",
                r#"{"case":"fallback-less","left":"10x","right":"2"}"#,
            ),
            entry(
                18,
                "boolean equal",
                r#"{"case":"boolean-equal","left":true,"right":"true"}"#,
            ),
            entry(
                19,
                "array equal",
                r#"{"case":"array-equal","left":[1],"right":"[1]"}"#,
            ),
            entry(
                20,
                "object equal",
                r#"{"case":"object-equal","left":{"k":"v"},"right":"{\"k\":\"v\"}"}"#,
            ),
            entry(
                21,
                "echo",
                r#"{"case":"message-equal","left":"z","right":"echo"}"#,
            ),
            entry(
                22,
                "service equal",
                r#"{"case":"service-equal","left":"z","right":"api","service":"api"}"#,
            ),
            entry(
                23,
                "nested less",
                r#"{"case":"nested-less","left":"z","right":"a","nested":{"left":2,"right":"10"}}"#,
            ),
            entry(
                24,
                "integer precision less",
                r#"{"case":"integer-precision-less","left":9007199254740992,"right":9007199254740993}"#,
            ),
        ])
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    async fn cases(storage: &Storage, query: &str) -> Vec<String> {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 100;
        storage
            .query(plan.spec)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.metadata_json).unwrap()["case"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    let equality = vec![
        "missing",
        "nulls",
        "empty",
        "equal-text",
        "numeric-equal",
        "boolean-equal",
        "array-equal",
        "object-equal",
    ];
    let less = vec![
        "right-only",
        "lexical-less",
        "numeric-less",
        "duration-less",
        "bytes-less",
        "timestamp-less",
        "ipv4-less",
        "fallback-less",
        "integer-precision-less",
    ];
    let mut less_or_equal = equality.clone();
    less_or_equal.extend(less.iter().copied());
    less_or_equal.sort_by_key(|case| {
        [
            "missing",
            "nulls",
            "empty",
            "right-only",
            "equal-text",
            "lexical-less",
            "numeric-less",
            "numeric-equal",
            "duration-less",
            "bytes-less",
            "timestamp-less",
            "ipv4-less",
            "fallback-less",
            "boolean-equal",
            "array-equal",
            "object-equal",
            "integer-precision-less",
        ]
        .iter()
        .position(|candidate| candidate == case)
        .unwrap()
    });
    let all = vec![
        "missing",
        "nulls",
        "empty",
        "left-only",
        "right-only",
        "equal-text",
        "lexical-less",
        "lexical-greater",
        "numeric-less",
        "numeric-greater",
        "numeric-equal",
        "duration-less",
        "duration-greater",
        "bytes-less",
        "timestamp-less",
        "ipv4-less",
        "fallback-less",
        "boolean-equal",
        "array-equal",
        "object-equal",
        "message-equal",
        "service-equal",
        "nested-less",
        "integer-precision-less",
    ];
    let queries = [
        ("left:eq_field(right)", equality),
        ("left:le_field(right)", less_or_equal),
        ("left:lt_field(right)", less),
        ("left:lt_field(left)", Vec::new()),
        ("left:le_field(left)", all),
        (
            "case:=\"timestamp-less\" left:lt_field(_time)",
            vec!["timestamp-less"],
        ),
        ("eq_field(right)", vec!["message-equal"]),
        (
            "service:eq_field(right)",
            vec!["missing", "nulls", "empty", "left-only", "service-equal"],
        ),
        ("nested.left:lt_field(nested.right)", vec!["nested-less"]),
        (
            "left:le_field(right) AND NOT left:eq_field(right)",
            vec![
                "right-only",
                "lexical-less",
                "numeric-less",
                "duration-less",
                "bytes-less",
                "timestamp-less",
                "ipv4-less",
                "fallback-less",
                "integer-precision-less",
            ],
        ),
    ];
    for (query, expected) in &queries {
        assert_eq!(cases(&storage, query).await, *expected, "{query}");
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(
            &app,
            "* | filter left:lt_field(right) | fields case | limit 100",
        )
        .await
        .into_iter()
        .map(|row| row["case"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>(),
        [
            "right-only",
            "lexical-less",
            "numeric-less",
            "duration-less",
            "bytes-less",
            "timestamp-less",
            "ipv4-less",
            "fallback-less",
            "integer-precision-less",
        ]
    );

    for malformed in [
        "eq_field(",
        "eq_field()",
        "eq_field(left, right)",
        "eq_field(left right)",
        "eq_field(*)",
        "le_field()",
        "le_field(left, right)",
        "lt_field()",
        "lt_field(left right)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "malformed_logsql",
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("left:le_field(right) | limit 100"))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        cases(&storage, "left:lt_field(right)").await,
        [
            "right-only",
            "lexical-less",
            "numeric-less",
            "duration-less",
            "bytes-less",
            "timestamp-less",
            "ipv4-less",
            "fallback-less",
            "integer-precision-less",
        ],
        "the reader must remain reusable after a bounded-work rejection"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        assert_eq!(cases(&reopened, query).await, expected, "reopened: {query}");
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_field_prefixes_expand_existing_rich_fields_and_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("field-prefix-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let entry = |offset: i64, message: &str, level: u8, metadata_json: &str| LogEntry {
        ts: 1_817_000_000_000_000 + offset,
        level,
        severity: if level == 3 { "error" } else { "info" }.into(),
        message: message.into(),
        metadata_json: metadata_json.into(),
    };
    storage
        .ingest(vec![
            entry(
                1,
                "plain",
                1,
                r#"{"case":"cross","cmp_left":"bar","cmp_right":"foo"}"#,
            ),
            entry(2, "plain", 1, r#"{"case":"alpha-left","cmp_left":"alpha"}"#),
            entry(
                3,
                "plain",
                1,
                r#"{"case":"alpha-right","cmp_right":"alpha"}"#,
            ),
            entry(4, "plain", 1, r#"{"case":"foo-left","cmp_left":"foo"}"#),
            entry(
                5,
                "plain",
                1,
                r#"{"case":"quoted-prefix","foo:bar:value":"needle"}"#,
            ),
            entry(
                6,
                "plain",
                1,
                r#"{"case":"nested","nested":{"ip":"alpha","number":2},"tags":["alpha"]}"#,
            ),
            entry(7, "plain", 1, r#"{"case":"null","nullable_one":null}"#),
            entry(8, "messageword alpha", 1, r#"{"case":"message"}"#),
            entry(9, "plain", 3, r#"{"case":"level"}"#),
            entry(
                10,
                "plain",
                1,
                r#"{"case":"numeric","numeric_u64":18446744073709551615}"#,
            ),
            entry(11, "plain", 1, r#"{"case":"none","other":"omega"}"#),
        ])
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    async fn cases(storage: &Storage, query: &str) -> Vec<String> {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 100;
        storage
            .query(plan.spec)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.metadata_json).unwrap()["case"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    let queries = [
        ("cmp_*:alpha", vec!["alpha-left", "alpha-right"]),
        ("cmp_l*:foo", vec!["foo-left"]),
        ("cmp_z*:alpha", Vec::new()),
        ("\"cmp_\"*:foo", vec!["cross", "foo-left"]),
        (
            "*:alpha",
            vec!["alpha-left", "alpha-right", "nested", "message"],
        ),
        (
            "\"\"*:alpha",
            vec!["alpha-left", "alpha-right", "nested", "message"],
        ),
        ("cmp_*:(bar AND foo)", vec!["cross"]),
        (
            "cmp_*:(alpha OR foo)",
            vec!["cross", "alpha-left", "alpha-right", "foo-left"],
        ),
        ("cmp_*:exact(alpha)", vec!["alpha-left", "alpha-right"]),
        ("cmp_*:string_range(bar, baz)", vec!["cross"]),
        ("nested.*:alpha", vec!["nested"]),
        ("tag*:value_type(array)", vec!["nested"]),
        ("numeric*:value_type(uint64)", vec!["numeric"]),
        ("\"foo:bar:\"*:needle", vec!["quoted-prefix"]),
        ("_*:messageword", vec!["message"]),
        (
            "\"_time\"*:value_type(string)",
            vec![
                "cross",
                "alpha-left",
                "alpha-right",
                "foo-left",
                "quoted-prefix",
                "nested",
                "null",
                "message",
                "level",
                "numeric",
                "none",
            ],
        ),
        ("level*:exact(error)", vec!["level"]),
        ("nullable*:exact(\"\")", vec!["null"]),
        (
            "NOT cmp_*:alpha",
            vec![
                "cross",
                "foo-left",
                "quoted-prefix",
                "nested",
                "null",
                "message",
                "level",
                "numeric",
                "none",
            ],
        ),
    ];
    for (query, expected) in &queries {
        assert_eq!(cases(&storage, query).await, *expected, "{query}");
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(&app, "* | filter cmp_*:foo | fields case | limit 100",)
            .await
            .into_iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        ["cross", "foo-left"]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            "* | fields cmp_left, case | filter cmp_*:bar | fields case | limit 100",
        )
        .await
        .into_iter()
        .map(|row| row["case"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>(),
        ["cross"]
    );

    for malformed in [
        "cmp_*:",
        "cmp_*:()",
        "cmp_*:(alpha",
        "cmp_**:alpha",
        "\"cmp_*:alpha",
        "cmp_*:eq_field(cmp_right)",
        "cmp_*:le_field(cmp_right)",
        "cmp_*:lt_field(cmp_right)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "malformed_logsql",
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("cmp_*:alpha | limit 100"))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        cases(&storage, "cmp_*:alpha").await,
        ["alpha-left", "alpha-right"],
        "the reader must remain reusable after a bounded-work rejection"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        assert_eq!(cases(&reopened, query).await, expected, "reopened: {query}");
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_day_ranges_use_explicit_utc_offsets_and_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("day-range-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                (1_800_007_199_999_999, "day-before"),
                (1_800_007_200_000_000, "day-start"),
                (1_800_010_800_123_456, "day-middle"),
                (1_800_014_400_000_000, "day-end"),
                (1_800_014_400_000_001, "day-after"),
            ]
            .into_iter()
            .map(|(ts, case)| LogEntry {
                ts,
                level: 1,
                severity: "info".into(),
                message: "clock fixture".into(),
                metadata_json: format!(r#"{{"case":"{case}","day_group":"day"}}"#),
            })
            .collect(),
        )
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    async fn cases(storage: &Storage, query: &str) -> Vec<String> {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 100;
        storage
            .query(plan.spec)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.metadata_json).unwrap()["case"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    let all = vec![
        "day-before",
        "day-start",
        "day-middle",
        "day-end",
        "day-after",
    ];
    let queries = [
        (
            "_time:day_range[10:00, 12:00] offset 0h",
            vec!["day-start", "day-middle", "day-end"],
        ),
        (
            "_time:day_range[10:00, 12:00]",
            vec!["day-start", "day-middle", "day-end"],
        ),
        (
            "_time:day_range(10:00, 12:00) offset 0h",
            vec!["day-middle"],
        ),
        (
            "_time:day_range[10:00, 12:00) offset 0h",
            vec!["day-start", "day-middle"],
        ),
        (
            "_time:day_range(10:00, 12:00] offset 0h",
            vec!["day-middle", "day-end"],
        ),
        (
            "_time:DAY_RANGE[1000, 1200] offset 0h",
            vec!["day-start", "day-middle", "day-end"],
        ),
        (
            "_time:day_range[12:00, 14:00] offset 2h",
            vec!["day-start", "day-middle", "day-end"],
        ),
        (
            "_time:day_range[08:00, 10:00] offset -2h",
            vec!["day-start", "day-middle", "day-end"],
        ),
        (
            "_time:day_range[11:30, 13:30] offset 1h30m",
            vec!["day-start", "day-middle", "day-end"],
        ),
        (
            "_time:day_range[10:60, 12:00] offset 0h",
            vec!["day-middle", "day-end"],
        ),
        ("_time:day_range[00:00, 24:00] offset 0h", all.clone()),
        ("_time:day_range[00:00, 00:00) offset 0h", all),
        ("_time:day_range[10:00, 10:00] offset 0h", vec!["day-start"]),
        ("_time:day_range[12:00, 10:00] offset 0h", Vec::new()),
    ];
    for (query, expected) in &queries {
        assert_eq!(cases(&storage, query).await, *expected, "{query}");
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(
            &app,
            "_time:day_range[10:00, 12:00] offset 0h | filter day_group:=\"day\" | fields case",
        )
        .await
        .into_iter()
        .map(|row| row["case"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>(),
        ["day-start", "day-middle", "day-end"]
    );
    assert!(
        pipeline_rows(
            &app,
            "* | fields _msg | filter _time:day_range[10:00, 12:00] offset 0h",
        )
        .await
        .is_empty(),
        "pipeline day_range must observe the current projected row"
    );

    for malformed in [
        "_time:day_range",
        "_time:day_range[foo, 12:00]",
        "_time:day_range[10:00, bar]",
        "_time:day_range[25:00, 26:00]",
        "_time:day_range[10:61, 12:00]",
        "_time:day_range[10:00, 12:00",
        "_time:day_range[10:00, 12:00] offset",
        "_time:day_range[10:00, 12:00] offset nope",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "malformed_logsql",
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "_time:day_range[10:00, 12:00] offset 0h | limit 100",
    ))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        cases(&storage, "_time:day_range[10:00, 12:00]").await,
        ["day-start", "day-middle", "day-end"],
        "the reader must remain reusable after a bounded-work rejection"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        assert_eq!(cases(&reopened, query).await, expected, "reopened: {query}");
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_week_ranges_use_explicit_utc_offsets_and_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("week-range-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    const SUNDAY_US: i64 = 1_798_934_400_000_000;
    const HOUR_US: i64 = 3_600_000_000;
    const DAY_US: i64 = 24 * HOUR_US;
    storage
        .ingest(
            [
                (SUNDAY_US + 12 * HOUR_US, "week-sun"),
                (SUNDAY_US + 23 * HOUR_US + HOUR_US / 2, "week-sun-late"),
                (SUNDAY_US + DAY_US + HOUR_US / 2, "week-mon-early"),
                (SUNDAY_US + DAY_US + 12 * HOUR_US, "week-mon"),
                (SUNDAY_US + 2 * DAY_US + 12 * HOUR_US, "week-tue"),
                (SUNDAY_US + 3 * DAY_US + 12 * HOUR_US, "week-wed"),
                (SUNDAY_US + 4 * DAY_US + 12 * HOUR_US, "week-thu"),
                (SUNDAY_US + 5 * DAY_US + 12 * HOUR_US, "week-fri"),
                (SUNDAY_US + 6 * DAY_US + 12 * HOUR_US, "week-sat"),
            ]
            .into_iter()
            .map(|(ts, case)| LogEntry {
                ts,
                level: 1,
                severity: "info".into(),
                message: "week fixture".into(),
                metadata_json: format!(r#"{{"case":"{case}","week_group":"week"}}"#),
            })
            .collect(),
        )
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    async fn cases(storage: &Storage, query: &str) -> Vec<String> {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 100;
        storage
            .query(plan.spec)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.metadata_json).unwrap()["case"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    let all = vec![
        "week-sun",
        "week-sun-late",
        "week-mon-early",
        "week-mon",
        "week-tue",
        "week-wed",
        "week-thu",
        "week-fri",
        "week-sat",
    ];
    let weekdays = vec![
        "week-mon-early",
        "week-mon",
        "week-tue",
        "week-wed",
        "week-thu",
        "week-fri",
    ];
    let queries = [
        ("_time:week_range[Mon, Fri]", weekdays.clone()),
        (
            "_time:week_range(Sun, Sat] offset 0h",
            vec![
                "week-mon-early",
                "week-mon",
                "week-tue",
                "week-wed",
                "week-thu",
                "week-fri",
                "week-sat",
            ],
        ),
        (
            "_time:week_range[Sun, Sat) offset 0h",
            vec![
                "week-sun",
                "week-sun-late",
                "week-mon-early",
                "week-mon",
                "week-tue",
                "week-wed",
                "week-thu",
                "week-fri",
            ],
        ),
        ("_time:week_range(Sun, Sat) offset 0h", weekdays.clone()),
        ("_time:week_range[Sun, Sat] offset 0h", all.clone()),
        ("_time:week_range[Fri, Mon] offset 0h", Vec::new()),
        ("_time:week_range[Sun, Sun) offset 0h", all.clone()),
        ("_time:week_range(Sat, Sun) offset 0h", all),
        (
            "_time:week_range[Mon, Mon] offset 0h",
            vec!["week-mon-early", "week-mon"],
        ),
        ("_time:week_range[Mon, Mon) offset 0h", Vec::new()),
        ("_time:WEEK_RANGE[Monday, Friday] offset 0h", weekdays),
        (
            "_time:week_range[Mon, Mon] offset 1h30m",
            vec!["week-sun-late", "week-mon-early", "week-mon"],
        ),
        (
            "_time:week_range[Sun, Sun] offset -1h",
            vec!["week-sun", "week-sun-late", "week-mon-early"],
        ),
    ];
    for (query, expected) in &queries {
        assert_eq!(cases(&storage, query).await, *expected, "{query}");
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(
            &app,
            "_time:week_range[Mon, Fri] offset 0h | filter week_group:=\"week\" | fields case",
        )
        .await
        .into_iter()
        .map(|row| row["case"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>(),
        [
            "week-mon-early",
            "week-mon",
            "week-tue",
            "week-wed",
            "week-thu",
            "week-fri",
        ]
    );
    assert!(
        pipeline_rows(
            &app,
            "* | fields _msg | filter _time:week_range[Mon, Fri] offset 0h",
        )
        .await
        .is_empty(),
        "pipeline week_range must observe the current projected row"
    );

    for malformed in [
        "_time:week_range",
        "_time:week_range[foo, Fri]",
        "_time:week_range[Mon, bar]",
        "_time:week_range[mom, Wed]",
        "_time:week_range[Mon Fri]",
        "_time:week_range[Mon, Fri",
        "_time:week_range[Mon, Fri] offset",
        "_time:week_range[Mon, Fri] offset nope",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "malformed_logsql",
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "_time:week_range[Mon, Fri] offset 0h | limit 100",
    ))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(
        cases(&storage, "_time:week_range[Mon, Fri]").await,
        [
            "week-mon-early",
            "week-mon",
            "week-tue",
            "week-wed",
            "week-thu",
            "week-fri",
        ],
        "the reader must remain reusable after a bounded-work rejection"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        assert_eq!(cases(&reopened, query).await, expected, "reopened: {query}");
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_sixteen_comments_multiline_semicolons_and_locations_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("comment-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(
            [
                (1_800_000_000_000_000, "word-exact", "alpha", r#"{}"#),
                (1_800_000_000_000_001, "word-case", "ALPHA", r#"{}"#),
                (
                    1_800_000_000_000_002,
                    "word-inside",
                    "before alpha after",
                    r#"{}"#,
                ),
                (
                    1_800_000_000_000_003,
                    "comment-hash",
                    "hash#inside",
                    r#"{"comment_group":"comments","hash#field":"hash#value"}"#,
                ),
            ]
            .into_iter()
            .map(|(ts, case, message, extra)| {
                let mut metadata = serde_json::from_str::<serde_json::Value>(extra).unwrap();
                metadata["case"] = serde_json::Value::String(case.into());
                LogEntry {
                    ts,
                    level: 1,
                    severity: "info".into(),
                    message: message.into(),
                    metadata_json: serde_json::to_string(&metadata).unwrap(),
                }
            })
            .collect(),
        )
        .await
        .unwrap();
    storage.barrier().await.unwrap();

    async fn cases(storage: &Storage, query: &str) -> Vec<String> {
        let mut plan = parse_logsql_at(query, TimestampUnit::Microseconds, 0).unwrap();
        plan.spec.descending = false;
        plan.spec.limit = 100;
        storage
            .query(plan.spec)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                serde_json::from_str::<serde_json::Value>(&row.metadata_json).unwrap()["case"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    let queries = [
        ("case:=\"word-exact\" # ignored", vec!["word-exact"]),
        ("# leading\ncase:=\"word-exact\"", vec!["word-exact"]),
        ("case:=\"word-exact\"#attached", vec!["word-exact"]),
        (
            "(case:=\"word-exact\" OR\n case:=\"word-case\")",
            vec!["word-exact", "word-case"],
        ),
        (
            "comment_group:=\"comments\" \"hash#inside\"",
            vec!["comment-hash"],
        ),
        (
            "comment_group:=\"comments\" 'hash#inside'",
            vec!["comment-hash"],
        ),
        (
            "comment_group:=\"comments\" `hash#inside`",
            vec!["comment-hash"],
        ),
        ("\"hash#field\":=\"hash#value\"", vec!["comment-hash"]),
        ("\"hash#inside\"# trailing", vec!["comment-hash"]),
        ("# windows\r\ncase:=\"word-exact\"", vec!["word-exact"]),
        ("case:=\"word-exact\";", vec!["word-exact"]),
        (
            "case:=\"word-exact\"; # terminal before comment",
            vec!["word-exact"],
        ),
        ("alpha\n~\"before\"", vec!["word-inside"]),
    ];
    for (query, expected) in &queries {
        assert_eq!(cases(&storage, query).await, *expected, "{query:?}");
    }

    let app = router(storage.clone());
    assert_eq!(
        pipeline_rows(
            &app,
            "case:=\"word-exact\" |\n # projected below\n fields case",
        )
        .await,
        [serde_json::json!({"case": "word-exact"})]
    );

    for (query, location) in [
        ("# first line\n  \"hash#inside", "line 2, column 3"),
        (
            "case:=\"word-exact\";\n  case:=\"word-case\"",
            "line 1, column 19",
        ),
    ] {
        let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{query:?}");
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], "malformed_logsql", "{query:?}");
        assert!(
            body["message"].as_str().unwrap().contains(location),
            "{query:?}: {body}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("alpha # bounded parser\n| limit 100"))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        cases(&storage, "case:=\"word-exact\";").await,
        ["word-exact"]
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    for (query, expected) in queries {
        assert_eq!(
            cases(&reopened, query).await,
            expected,
            "reopened: {query:?}"
        );
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_delete_pipe_preserves_rich_rows_limits_and_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("delete-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    storage
        .ingest(vec![
            LogEntry {
                ts: 1_800_000_000_000_000,
                level: 3,
                severity: "warning".into(),
                message: "delete message".into(),
                metadata_json: serde_json::json!({
                    "case": "delete-row",
                    "delete_group": "delete",
                    "keep": "kept",
                    "drop_exact": "gone",
                    "drop_prefix_a": "one",
                    "drop_prefix_b": "two",
                    "Drop_prefix_caps": "caps",
                    "delete,weird": "comma",
                    "delete|pipe": "pipe",
                    "drop,prefix.one": "quoted-prefix",
                    "star*literal": "literal-star",
                    "nested": {"drop": "nested-gone", "keep": "nested-kept"},
                    "array": ["x", 1],
                    "null_value": null,
                    "empty_value": ""
                })
                .to_string(),
            },
            LogEntry {
                ts: 1_800_000_000_000_001,
                level: 1,
                severity: "info".into(),
                message: "other row".into(),
                metadata_json: r#"{"case":"delete-other","delete_group":"other"}"#.into(),
            },
        ])
        .await
        .unwrap();
    storage.barrier().await.unwrap();
    let app = router(storage.clone());

    let queries = [
        (
            "delete_group:=\"delete\" | delete drop_exact, drop_prefix_a | fields case, keep, drop_exact, drop_prefix_a, drop_prefix_b",
            vec![serde_json::json!({"case":"delete-row","keep":"kept","drop_prefix_b":"two"})],
        ),
        (
            "delete_group:=\"delete\" | drop drop_exact | fields case, drop_exact",
            vec![serde_json::json!({"case":"delete-row"})],
        ),
        (
            "delete_group:=\"delete\" | del drop_exact | fields case, drop_exact",
            vec![serde_json::json!({"case":"delete-row"})],
        ),
        (
            "delete_group:=\"delete\" | rm drop_exact | fields case, drop_exact",
            vec![serde_json::json!({"case":"delete-row"})],
        ),
        (
            "delete_group:=\"delete\" | DELETE drop_exact | fields case, drop_exact",
            vec![serde_json::json!({"case":"delete-row"})],
        ),
        (
            "delete_group:=\"delete\" | delete drop_prefix* | fields case, drop_prefix_a, drop_prefix_b, Drop_prefix_caps",
            vec![serde_json::json!({"case":"delete-row","Drop_prefix_caps":"caps"})],
        ),
        (
            "delete_group:=\"delete\" | delete \"delete,weird\", \"delete|pipe\", \"star*literal\" | fields case, \"delete,weird\", \"delete|pipe\", \"star*literal\"",
            vec![serde_json::json!({"case":"delete-row"})],
        ),
        (
            "delete_group:=\"delete\" | delete \"drop,prefix.\"* | fields case, \"drop,prefix.one\", keep",
            vec![serde_json::json!({"case":"delete-row","keep":"kept"})],
        ),
        (
            "delete_group:=\"delete\" | delete absent | fields case, keep",
            vec![serde_json::json!({"case":"delete-row","keep":"kept"})],
        ),
        (
            "delete_group:=\"delete\" | delete _msg, _time, level | fields case, _msg, _time, level",
            vec![serde_json::json!({"case":"delete-row"})],
        ),
        (
            "delete_group:=\"delete\" | delete *",
            Vec::new(),
        ),
        (
            "delete_group:=\"delete\" | fields case, keep, drop_exact | delete drop_exact",
            vec![serde_json::json!({"case":"delete-row","keep":"kept"})],
        ),
        (
            "delete_group:=\"delete\" | delete drop_exact | filter drop_exact:* | fields case",
            Vec::new(),
        ),
        (
            "delete_group:=\"delete\" | delete drop_exact, drop_exact | fields case, drop_exact",
            vec![serde_json::json!({"case":"delete-row"})],
        ),
        (
            "delete_group:=\"delete\" | delete \"\" | fields case, _msg",
            vec![serde_json::json!({"case":"delete-row"})],
        ),
        (
            "delete_group:=\"delete\" | delete nested.drop | fields case, nested",
            vec![serde_json::json!({"case":"delete-row","nested":{"keep":"nested-kept"}})],
        ),
        (
            "delete_group:=\"delete\" | delete nested.d* | fields case, nested",
            vec![serde_json::json!({"case":"delete-row","nested":{"keep":"nested-kept"}})],
        ),
        (
            "delete_group:=\"delete\" | delete nested | fields case, nested",
            vec![serde_json::json!({"case":"delete-row"})],
        ),
        (
            "delete_group:=\"delete\" | delete array, null_value, empty_value | fields case, array, null_value, empty_value",
            vec![serde_json::json!({"case":"delete-row"})],
        ),
    ];
    for (query, expected) in &queries {
        assert_eq!(pipeline_rows(&app, query).await, *expected, "{query:?}");
    }

    for malformed in [
        "* | delete",
        "* | delete drop_exact,",
        "* | delete , drop_exact",
        "* | delete drop_exact,,keep",
        "* | delete drop_exact keep",
        "* | delete drop_prefix *",
        "* | delete *drop_prefix",
        "* | delete drop*prefix",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed:?}");
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], "malformed_logsql", "{malformed:?}: {body}");
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 100,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "delete_group:=\"delete\" | delete drop_exact | fields case",
    ))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        pipeline_rows(
            &app,
            "delete_group:=\"delete\" | delete drop_exact | fields case"
        )
        .await,
        [serde_json::json!({"case":"delete-row"})]
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let reopened_app = router(reopened.clone());
    for (query, expected) in queries {
        assert_eq!(
            pipeline_rows(&reopened_app, query).await,
            expected,
            "reopened: {query:?}"
        );
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_ten_quoted_phrase_matches_victorialogs_case_and_bytes_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("phrase-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let body = [
        r#"{"_time":1800000000000001,"_msg":"ssh: login fail","level":"info","case":"exact"}"#,
        r#"{"_time":1800000000000002,"_msg":"prefix ssh: login fail suffix","level":"info","case":"inside"}"#,
        r#"{"_time":1800000000000003,"_msg":"SSH: login fail","level":"info","case":"case"}"#,
        r#"{"_time":1800000000000004,"_msg":"ssh:  login fail","level":"info","case":"space"}"#,
        r#"{"_time":1800000000000005,"_msg":"ssh: login failed","level":"info","case":"suffix"}"#,
        r#"{"_time":1800000000000006,"_msg":"xssh: login fail","level":"info","case":"prefix"}"#,
        r#"{"_time":1800000000000007,"_msg":"x_ssh: login fail","level":"info","case":"underscore"}"#,
        r#"{"_time":1800000000000008,"_msg":"éssh: login fail","level":"info","case":"unicode-letter"}"#,
        r#"{"_time":1800000000000009,"_msg":"(ssh: login fail)!","level":"info","case":"punctuation"}"#,
        r#"{"_time":1800000000000010,"_msg":"ssh: login fail—next","level":"info","case":"unicode-punctuation"}"#,
    ]
    .join("\n");
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    let query = r#""ssh: login fail" | limit 10"#;
    let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let mut cases = ndjson_values(&body)
        .into_iter()
        .map(|row| row["case"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    cases.sort();
    assert_eq!(
        cases,
        ["exact", "inside", "punctuation", "unicode-punctuation"]
    );

    let prefix_query = r#""ssh: login fai"* | limit 10"#;
    let response = app
        .clone()
        .oneshot(logsql_request(prefix_query))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut prefix_cases =
        ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
            .into_iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
    prefix_cases.sort();
    assert_eq!(
        prefix_cases,
        [
            "exact",
            "inside",
            "punctuation",
            "suffix",
            "unicode-punctuation"
        ]
    );

    let insensitive_query = r#"i("SSH: LOGIN FAIL") | limit 10"#;
    let response = app
        .clone()
        .oneshot(logsql_request(insensitive_query))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut insensitive_cases =
        ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
            .into_iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
    insensitive_cases.sort();
    assert_eq!(
        insensitive_cases,
        [
            "case",
            "exact",
            "inside",
            "punctuation",
            "unicode-punctuation"
        ]
    );

    let response = app
        .clone()
        .oneshot(logsql_request(r#""ssh: login fail" | stats count()"#))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
        b"{\"total\":4}\n"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let response = router(reopened.clone())
        .oneshot(logsql_request(query))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).len(),
        4
    );
    let response = router(reopened.clone())
        .oneshot(logsql_request(prefix_query))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).len(),
        5
    );
    let response = router(reopened.clone())
        .oneshot(logsql_request(insensitive_query))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).len(),
        5
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_sequence_filters_are_ordered_rich_bounded_and_reopenable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("sequence-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let mut body = String::new();
    for index in 0..8_192_u64 {
        let (message, case, n, payload) = match index {
            0 => (
                "ssh: login fail",
                "exact",
                Some(serde_json::json!(2.5)),
                Some(serde_json::json!({"ready": true, "stage": "ssh login fail"})),
            ),
            1 => ("prefix ssh: login fail suffix", "inside", None, None),
            2 => ("ssh: login failed", "suffix", None, None),
            3 => ("fail before login before ssh", "reverse", None, None),
            4 => ("ssh before ssh", "repeated", None, None),
            5 => ("éssh login fail", "unicode-prefix", None, None),
            6 => ("alpha then beta", "unicode", None, None),
            _ => ("request sequence filler", "filler", None, None),
        };
        let mut row = serde_json::json!({
            "_time": 1_812_000_000_000_000_i64 + index as i64,
            "_msg": message,
            "level": "info",
            "case": case,
        });
        if let Some(n) = n {
            row["n"] = n;
        }
        if let Some(payload) = payload {
            row["payload"] = payload;
        }
        body.push_str(&row.to_string());
        body.push('\n');
    }
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn cases(app: &axum::Router, query: &str) -> Vec<String> {
        let mut cases = pipeline_rows(app, &format!("{query} | fields case | limit 10000"))
            .await
            .into_iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        cases.sort();
        cases
    }

    assert_eq!(
        cases(&app, "seq(ssh, login, fail)").await,
        ["exact", "inside"]
    );
    assert_eq!(cases(&app, "seq(fail, login, ssh)").await, ["reverse"]);
    assert_eq!(cases(&app, "seq(ssh, ssh)").await, ["repeated"]);
    assert_eq!(cases(&app, "payload.stage:seq(ssh, fail)").await, ["exact"]);
    assert_eq!(cases(&app, "n:seq(2, .5)").await, ["exact"]);
    assert_eq!(cases(&app, "payload:seq(ready, true)").await, ["exact"]);
    assert_eq!(
        cases(
            &app,
            "* | copy _msg as current | filter current:seq(ssh, fail)"
        )
        .await,
        ["exact", "inside"]
    );
    assert_eq!(
        cases(&app, "seq(ssh, fail) AND NOT case:=\"inside\"").await,
        ["exact"]
    );

    let no_op = app
        .clone()
        .oneshot(logsql_request("never_present:seq() | stats count()"))
        .await
        .unwrap();
    assert_eq!(no_op.status(), StatusCode::OK);
    assert_eq!(
        &to_bytes(no_op.into_body(), usize::MAX).await.unwrap()[..],
        b"{\"total\":8192}\n"
    );

    for malformed in [
        "seq(,alpha)",
        "seq(alpha,,beta)",
        "seq(alpha beta)",
        "seq(alpha*)",
        "seq(*)",
        "seq(alpha",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10_000,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("seq(request, absent) | limit 10000"))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(work_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );

    let cancelled_before = storage.stats().await.unwrap().api_query_cancelled;
    let timed_out = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10_000,
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("seq(request, absent) | limit 10000"))
    .await
    .unwrap();
    assert_eq!(timed_out.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before);
    assert_eq!(stats.api_query_in_flight, 0);
    assert_eq!(
        cases(&app, "seq(ssh, login, fail)").await,
        ["exact", "inside"]
    );

    storage.schedule_optimize().await.unwrap();
    assert_eq!(
        cases(&app, "seq(ssh, login, fail)").await,
        ["exact", "inside"]
    );
    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();

    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        cases(&reopened_app, "seq(ssh, login, fail)").await,
        ["exact", "inside"]
    );
    assert_eq!(
        cases(&reopened_app, "payload:seq(ready, true)").await,
        ["exact"]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_twelve_logsql_word_filter_matches_unicode_oracle_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("word-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let body = [
        r#"{"_time":1800000000000001,"_msg":"alpha","level":"info","case":"exact"}"#,
        r#"{"_time":1800000000000002,"_msg":"before alpha after","level":"info","case":"inside"}"#,
        r#"{"_time":1800000000000003,"_msg":"ALPHA","level":"info","case":"case"}"#,
        r#"{"_time":1800000000000004,"_msg":"alphas","level":"info","case":"suffix"}"#,
        r#"{"_time":1800000000000005,"_msg":"xalpha","level":"info","case":"prefix"}"#,
        r#"{"_time":1800000000000006,"_msg":"x_alpha","level":"info","case":"underscore"}"#,
        r#"{"_time":1800000000000007,"_msg":"(alpha)!","level":"info","case":"punctuation"}"#,
        r#"{"_time":1800000000000008,"_msg":"éalpha","level":"info","case":"unicode-boundary"}"#,
        r#"{"_time":1800000000000009,"_msg":"prefix тест45 suffix","level":"info","case":"unicode"}"#,
        r#"{"_time":1800000000000010,"_msg":"CAFÉ","level":"info","case":"unicode-case"}"#,
    ]
    .join("\n");
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn cases(app: &axum::Router, query: &str) -> Vec<String> {
        let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        let mut cases = ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
            .into_iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        cases.sort();
        cases
    }

    assert_eq!(
        cases(&app, "alpha | limit 100").await,
        ["exact", "inside", "punctuation"]
    );
    assert_eq!(
        cases(&app, "alph* | limit 100").await,
        ["exact", "inside", "punctuation", "suffix"]
    );
    assert_eq!(
        cases(&app, "*pha* | limit 100").await,
        [
            "exact",
            "inside",
            "prefix",
            "punctuation",
            "suffix",
            "underscore",
            "unicode-boundary"
        ]
    );
    assert_eq!(
        cases(&app, r#"~"alp(ha|ine)" | limit 100"#).await,
        [
            "exact",
            "inside",
            "prefix",
            "punctuation",
            "suffix",
            "underscore",
            "unicode-boundary"
        ]
    );
    assert_eq!(
        cases(&app, r#"~"(?i)^alpha$" | limit 100"#).await,
        ["case", "exact"]
    );
    let invalid = app
        .clone()
        .oneshot(logsql_request(r#"~"(""#))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(invalid.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "malformed_logsql"
    );
    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 1,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(r#"~"before" | limit 1"#))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(work_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );
    assert_eq!(cases(&app, "тест45 | limit 100").await, ["unicode"]);
    assert_eq!(
        cases(&app, "i(alpha) | limit 100").await,
        ["case", "exact", "inside", "punctuation"]
    );
    assert_eq!(
        cases(&app, "i(alph*) | limit 100").await,
        ["case", "exact", "inside", "punctuation", "suffix"]
    );
    assert_eq!(cases(&app, "i(café) | limit 100").await, ["unicode-case"]);
    assert_eq!(cases(&app, r#"="alpha" | limit 100"#).await, ["exact"]);
    assert_eq!(cases(&app, r#"case:="exact" | limit 100"#).await, ["exact"]);
    assert_eq!(
        cases(&app, r#"alpha AND ~"before" | limit 100"#).await,
        ["inside"]
    );
    assert_eq!(
        cases(&app, r#"="alpha" OR ="ALPHA" | limit 100"#).await,
        ["case", "exact"]
    );
    assert_eq!(
        cases(&app, r#"alpha AND NOT ~"before" | limit 100"#).await,
        ["exact", "punctuation"]
    );
    assert_eq!(
        cases(&app, r#"(="alpha" OR ="ALPHA") AND ~"ALPHA" | limit 100"#).await,
        ["case"]
    );
    assert_eq!(
        cases(&app, r#"case:(="exact" OR ="case") | limit 100"#).await,
        ["case", "exact"]
    );
    let count = app
        .clone()
        .oneshot(logsql_request("alpha | stats count()"))
        .await
        .unwrap();
    assert_eq!(count.status(), StatusCode::OK);
    assert_eq!(
        &to_bytes(count.into_body(), usize::MAX).await.unwrap()[..],
        b"{\"total\":3}\n"
    );

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(reopened.clone());
    assert_eq!(
        cases(&app, "alpha | limit 100").await,
        ["exact", "inside", "punctuation"]
    );
    assert_eq!(
        cases(&app, "alph* | limit 100").await,
        ["exact", "inside", "punctuation", "suffix"]
    );
    assert_eq!(
        cases(&app, "*pha* | limit 100").await,
        [
            "exact",
            "inside",
            "prefix",
            "punctuation",
            "suffix",
            "underscore",
            "unicode-boundary"
        ]
    );
    assert_eq!(
        cases(&app, r#"~"alp(ha|ine)" | limit 100"#).await,
        [
            "exact",
            "inside",
            "prefix",
            "punctuation",
            "suffix",
            "underscore",
            "unicode-boundary"
        ]
    );
    assert_eq!(
        cases(&app, "i(alpha) | limit 100").await,
        ["case", "exact", "inside", "punctuation"]
    );
    assert_eq!(cases(&app, "i(café) | limit 100").await, ["unicode-case"]);
    assert_eq!(cases(&app, r#"="alpha" | limit 100"#).await, ["exact"]);
    assert_eq!(
        cases(&app, r#"="alpha" OR ="ALPHA" | limit 100"#).await,
        ["case", "exact"]
    );
    assert_eq!(
        cases(&app, r#"case:(="exact" OR ="case") | limit 100"#).await,
        ["case", "exact"]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_twelve_empty_any_and_typed_presence_remain_distinct_after_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("presence-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let body = [
        r#"{"_time":1800000000000001,"_msg":"missing","level":"info","case":"missing","state_group":"state"}"#,
        r#"{"_time":1800000000000002,"_msg":"null","level":"info","case":"null","state_group":"state","probe":null,"nested":{"leaf":null}}"#,
        r#"{"_time":1800000000000003,"_msg":"empty","level":"info","case":"empty","state_group":"state","probe":"","nested":{"leaf":""}}"#,
        r#"{"_time":1800000000000004,"_msg":"string","level":"info","case":"string","state_group":"state","probe":"value","nested":{"leaf":"value"}}"#,
        r#"{"_time":1800000000000005,"_msg":"zero","level":"info","case":"zero","state_group":"state","probe":0,"nested":{"leaf":0}}"#,
        r#"{"_time":1800000000000006,"_msg":"false","level":"info","case":"false","state_group":"state","probe":false,"nested":{"leaf":false}}"#,
        r#"{"_time":1800000000000007,"_msg":"array","level":"info","case":"array","state_group":"state","probe":[],"nested":{"leaf":[]}}"#,
        r#"{"_time":1800000000000008,"_msg":"object","level":"info","case":"object","state_group":"state","probe":{},"nested":{"leaf":{}}}"#,
    ]
    .join("\n");
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn cases(app: &axum::Router, query: &str) -> Vec<String> {
        let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        let mut cases = ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
            .into_iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        cases.sort();
        cases
    }

    let compatible_empty = r#"state_group:="state" probe:("") | limit 100"#;
    let any_value = r#"state_group:="state" probe:* | limit 100"#;
    assert_eq!(
        cases(&app, compatible_empty).await,
        ["empty", "missing", "null"]
    );
    assert_eq!(
        cases(&app, r#"state_group:="state" probe:"" | limit 100"#).await,
        ["empty"]
    );
    assert_eq!(
        cases(&app, r#"state_group:="state" probe:=null | limit 100"#).await,
        ["null"]
    );
    assert_eq!(
        cases(&app, any_value).await,
        ["array", "false", "object", "string", "zero"]
    );
    let field_values = app
        .clone()
        .oneshot(logsql_request(
            r#"state_group:="state" | field_values probe"#,
        ))
        .await
        .unwrap();
    assert_eq!(field_values.status(), StatusCode::OK);
    assert_eq!(
        ndjson_values(
            &to_bytes(field_values.into_body(), usize::MAX)
                .await
                .unwrap()
        ),
        [
            serde_json::json!({"hits": 1}),
            serde_json::json!({"hits": 1, "probe": null}),
            serde_json::json!({"hits": 1, "probe": false}),
            serde_json::json!({"hits": 1, "probe": 0}),
            serde_json::json!({"hits": 1, "probe": ""}),
            serde_json::json!({"hits": 1, "probe": "value"}),
            serde_json::json!({"hits": 1, "probe": []}),
            serde_json::json!({"hits": 1, "probe": {}}),
        ]
    );
    assert_eq!(
        pipeline_rows(&app, r#"state_group:="state" | field_values probe limit 0"#).await,
        [
            serde_json::json!({"hits": 1}),
            serde_json::json!({"hits": 1, "probe": null}),
            serde_json::json!({"hits": 1, "probe": false}),
            serde_json::json!({"hits": 1, "probe": 0}),
            serde_json::json!({"hits": 1, "probe": ""}),
            serde_json::json!({"hits": 1, "probe": "value"}),
            serde_json::json!({"hits": 1, "probe": []}),
            serde_json::json!({"hits": 1, "probe": {}}),
        ]
    );
    let field_names = app
        .clone()
        .oneshot(logsql_request(r#"state_group:="state" | field_names"#))
        .await
        .unwrap();
    assert_eq!(field_names.status(), StatusCode::OK);
    assert_eq!(
        ndjson_values(&to_bytes(field_names.into_body(), usize::MAX).await.unwrap()),
        [
            serde_json::json!({"hits": 8, "name": "_msg"}),
            serde_json::json!({"hits": 8, "name": "_time"}),
            serde_json::json!({"hits": 8, "name": "case"}),
            serde_json::json!({"hits": 8, "name": "level"}),
            serde_json::json!({"hits": 7, "name": "nested"}),
            serde_json::json!({"hits": 7, "name": "probe"}),
            serde_json::json!({"hits": 8, "name": "state_group"}),
        ]
    );

    let projected = app
        .clone()
        .oneshot(logsql_request(
            r#"state_group:="state" | sort by (_time) asc | fields case, probe, nested.leaf | limit 100"#,
        ))
        .await
        .unwrap();
    assert_eq!(projected.status(), StatusCode::OK);
    assert_eq!(
        ndjson_values(&to_bytes(projected.into_body(), usize::MAX).await.unwrap()),
        [
            serde_json::json!({"case": "missing"}),
            serde_json::json!({"case": "null", "probe": null, "nested": {"leaf": null}}),
            serde_json::json!({"case": "empty", "probe": "", "nested": {"leaf": ""}}),
            serde_json::json!({"case": "string", "probe": "value", "nested": {"leaf": "value"}}),
            serde_json::json!({"case": "zero", "probe": 0, "nested": {"leaf": 0}}),
            serde_json::json!({"case": "false", "probe": false, "nested": {"leaf": false}}),
            serde_json::json!({"case": "array", "probe": [], "nested": {"leaf": []}}),
            serde_json::json!({"case": "object", "probe": {}, "nested": {"leaf": {}}}),
        ]
    );

    let filtered_projection = app
        .clone()
        .oneshot(logsql_request(
            r#"state_group:="state" | fields case, probe | filter probe:* | limit 100"#,
        ))
        .await
        .unwrap();
    assert_eq!(filtered_projection.status(), StatusCode::OK);
    let mut filtered_cases = ndjson_values(
        &to_bytes(filtered_projection.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .into_iter()
    .map(|row| row["case"].as_str().unwrap().to_owned())
    .collect::<Vec<_>>();
    filtered_cases.sort();
    assert_eq!(
        filtered_cases,
        ["array", "false", "object", "string", "zero"]
    );

    let aggregate = app
        .clone()
        .oneshot(logsql_request(
            r#"state_group:="state" | stats count(probe) as present, count_empty(probe) as empty, count_uniq(probe) as exact, count_uniq_hash(probe) as hashed, uniq_values(probe) as unique, values(probe) as all_values"#,
        ))
        .await
        .unwrap();
    assert_eq!(aggregate.status(), StatusCode::OK);
    assert_eq!(
        ndjson_values(&to_bytes(aggregate.into_body(), usize::MAX).await.unwrap()),
        [serde_json::json!({
            "present": 5,
            "empty": 3,
            "exact": 5,
            "hashed": 5,
            "unique": [false, 0, "value", [], {}],
            "all_values": {
                "items": [null, "", "value", 0, false, [], {}],
                "missing": 1
            }
        })]
    );

    let limited_values = app
        .clone()
        .oneshot(logsql_request(
            r#"state_group:="state" | stats values(probe) limit 3 as first_three"#,
        ))
        .await
        .unwrap();
    assert_eq!(limited_values.status(), StatusCode::OK);
    assert_eq!(
        ndjson_values(
            &to_bytes(limited_values.into_body(), usize::MAX)
                .await
                .unwrap()
        ),
        [serde_json::json!({
            "first_three": {"items": [null, ""], "missing": 1}
        })]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"state_group:="state" | stats uniq_values(probe) limit 0 as unique, values(probe) limit 0 as all_values"#
        )
        .await,
        [serde_json::json!({
            "unique": [false, 0, "value", [], {}],
            "all_values": {
                "items": [null, "", "value", 0, false, [], {}],
                "missing": 1
            }
        })]
    );
    assert_eq!(
        cases(
            &app,
            r#"state_group:="state" probe:value_type(string) | limit 100"#
        )
        .await,
        ["empty", "string"]
    );
    assert_eq!(
        cases(
            &app,
            r#"state_group:="state" probe:value_type(bool) | limit 100"#
        )
        .await,
        ["false"]
    );
    assert_eq!(
        cases(
            &app,
            r#"state_group:="state" probe:value_type(null) | limit 100"#
        )
        .await,
        ["null"]
    );
    assert_eq!(
        cases(
            &app,
            r#"state_group:="state" probe:value_type(array) | limit 100"#
        )
        .await,
        ["array"]
    );
    assert_eq!(
        cases(
            &app,
            r#"state_group:="state" probe:value_type(object) | limit 100"#
        )
        .await,
        ["object"]
    );
    let physical = app
        .clone()
        .oneshot(logsql_request("probe:value_type(const)"))
        .await
        .unwrap();
    assert_eq!(physical.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        cases(
            &app,
            r#"state_group:="state" AND (probe:="" OR probe:=null) | limit 100"#
        )
        .await,
        ["empty", "null"]
    );
    assert_eq!(
        cases(&app, r#"state_group:="state" nested.leaf:("") | limit 100"#).await,
        ["empty", "missing", "null"]
    );
    let count = app
        .clone()
        .oneshot(logsql_request(
            r#"state_group:="state" probe:* | stats count()"#,
        ))
        .await
        .unwrap();
    assert_eq!(count.status(), StatusCode::OK);
    assert_eq!(
        &to_bytes(count.into_body(), usize::MAX).await.unwrap()[..],
        b"{\"total\":5}\n"
    );

    storage.flush().await.unwrap();
    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(reopened.clone());
    assert_eq!(
        cases(&app, compatible_empty).await,
        ["empty", "missing", "null"]
    );
    assert_eq!(
        cases(&app, any_value).await,
        ["array", "false", "object", "string", "zero"]
    );
    assert_eq!(
        cases(
            &app,
            r#"state_group:="state" probe:value_type(null) | limit 100"#
        )
        .await,
        ["null"]
    );
    assert_eq!(
        pipeline_rows(&app, r#"state_group:="state" | field_values probe limit 0"#)
            .await
            .len(),
        8
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"state_group:="state" | fields case, nested.leaf | filter nested.leaf:* | limit 100"#
        )
        .await
        .len(),
        5
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"state_group:="state" | stats count(probe) as present, count_empty(probe) as empty, count_uniq(probe) as exact, values(probe) limit 0 as all_values"#
        )
        .await,
        [serde_json::json!({
            "present": 5,
            "empty": 3,
            "exact": 5,
            "all_values": {
                "items": [null, "", "value", 0, false, [], {}],
                "missing": 1
            }
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_twelve_numeric_filters_keep_types_and_integer_precision_after_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("numeric-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let body = [
        r#"{"_time":1800000000000001,"_msg":"missing","level":"info","case":"missing","numeric_group":"numeric"}"#,
        r#"{"_time":1800000000000002,"_msg":"null","level":"info","case":"null","numeric_group":"numeric","n":null}"#,
        r#"{"_time":1800000000000003,"_msg":"negative","level":"info","case":"negative","numeric_group":"numeric","n":-2}"#,
        r#"{"_time":1800000000000004,"_msg":"zero","level":"info","case":"zero","numeric_group":"numeric","n":0}"#,
        r#"{"_time":1800000000000005,"_msg":"two","level":"info","case":"two","numeric_group":"numeric","n":2}"#,
        r#"{"_time":1800000000000006,"_msg":"decimal","level":"info","case":"decimal","numeric_group":"numeric","n":2.5}"#,
        r#"{"_time":1800000000000007,"_msg":"string","level":"info","case":"string","numeric_group":"numeric","n":"3"}"#,
        r#"{"_time":1800000000000008,"_msg":"ten","level":"info","case":"ten","numeric_group":"numeric","n":10}"#,
        r#"{"_time":1800000000000009,"_msg":"huge","level":"info","case":"huge","numeric_group":"numeric","n":9007199254740993}"#,
    ]
    .join("\n");
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn cases(app: &axum::Router, query: &str) -> Vec<String> {
        let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        let mut cases = ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
            .into_iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        cases.sort();
        cases
    }

    let greater = r#"numeric_group:="numeric" n:>2 | limit 100"#;
    let between = r#"numeric_group:="numeric" n:>=2 n:<10 | limit 100"#;
    assert_eq!(cases(&app, greater).await, ["decimal", "huge", "ten"]);
    assert_eq!(cases(&app, between).await, ["decimal", "two"]);
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:value_type(uint64) | limit 100"#
        )
        .await,
        ["huge", "ten", "two", "zero"]
    );
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:value_type(int64) | limit 100"#
        )
        .await,
        ["negative"]
    );
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:value_type(float64) | limit 100"#
        )
        .await,
        ["decimal"]
    );
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:value_type(string) | limit 100"#
        )
        .await,
        ["string"]
    );
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:value_type(number) | limit 100"#
        )
        .await,
        ["decimal", "huge", "negative", "ten", "two", "zero"]
    );
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" AND (n:<0 OR n:>9) | limit 100"#
        )
        .await,
        ["huge", "negative", "ten"]
    );
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:range(2, 10) | limit 100"#
        )
        .await,
        ["decimal"]
    );
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:range[2, 10) | limit 100"#
        )
        .await,
        ["decimal", "two"]
    );
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:range(2, 10] | limit 100"#
        )
        .await,
        ["decimal", "ten"]
    );
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:range[2, 10] | limit 100"#
        )
        .await,
        ["decimal", "ten", "two"]
    );
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:>9007199254740992 | limit 100"#
        )
        .await,
        ["huge"]
    );

    let numeric_stats = app
        .clone()
        .oneshot(logsql_request(
            r#"numeric_group:="numeric" | stats sum(n) as sum, avg(n) as avg, min(n) as min, max(n) as max, median(n) as median"#,
        ))
        .await
        .unwrap();
    assert_eq!(numeric_stats.status(), StatusCode::OK);
    let numeric_stats = ndjson_values(
        &to_bytes(numeric_stats.into_body(), usize::MAX)
            .await
            .unwrap(),
    );
    assert_eq!(numeric_stats.len(), 1);
    assert_eq!(numeric_stats[0]["min"], serde_json::json!(-2));
    assert_eq!(
        numeric_stats[0]["max"],
        serde_json::json!(9_007_199_254_740_993u64)
    );
    assert_eq!(numeric_stats[0]["median"], serde_json::json!(2.25));
    assert_eq!(
        numeric_stats[0]["sum"].as_f64().unwrap(),
        9_007_199_254_741_004.0
    );
    assert_eq!(
        numeric_stats[0]["avg"].as_f64().unwrap(),
        1_501_199_875_790_167.2
    );

    let rates = app
        .clone()
        .oneshot(logsql_request(
            r#"numeric_group:="numeric" _time:[1800000000000000,1800000002000000) | stats rate() as rate, rate_sum(n) as rate_sum"#,
        ))
        .await
        .unwrap();
    assert_eq!(rates.status(), StatusCode::OK);
    let rates = ndjson_values(&to_bytes(rates.into_body(), usize::MAX).await.unwrap());
    assert_eq!(rates.len(), 1);
    assert_eq!(rates[0]["rate"], serde_json::json!(4.5));
    assert_eq!(
        rates[0]["rate_sum"].as_f64().unwrap(),
        4_503_599_627_370_502.0
    );

    storage.flush().await.unwrap();
    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(reopened.clone());
    assert_eq!(cases(&app, greater).await, ["decimal", "huge", "ten"]);
    assert_eq!(cases(&app, between).await, ["decimal", "two"]);
    assert_eq!(
        cases(
            &app,
            r#"numeric_group:="numeric" n:value_type(uint64) | limit 100"#
        )
        .await,
        ["huge", "ten", "two", "zero"]
    );
    let numeric_stats = pipeline_rows(
        &app,
        r#"numeric_group:="numeric" | stats sum(n) as sum, avg(n) as avg, min(n) as min, max(n) as max, median(n) as median"#,
    )
    .await;
    assert_eq!(numeric_stats.len(), 1);
    assert_eq!(numeric_stats[0]["min"], serde_json::json!(-2));
    assert_eq!(
        numeric_stats[0]["max"],
        serde_json::json!(9_007_199_254_740_993u64)
    );
    assert_eq!(numeric_stats[0]["median"], serde_json::json!(2.25));
    assert_eq!(
        numeric_stats[0]["sum"].as_f64().unwrap(),
        9_007_199_254_741_004.0
    );
    assert_eq!(
        numeric_stats[0]["avg"].as_f64().unwrap(),
        1_501_199_875_790_167.2
    );
    let rates = pipeline_rows(
        &app,
        r#"numeric_group:="numeric" _time:[1800000000000000,1800000002000000) | stats rate() as rate, rate_sum(n) as rate_sum"#,
    )
    .await;
    assert_eq!(
        rates,
        [serde_json::json!({
            "rate": 4.5,
            "rate_sum": 4_503_599_627_370_502.0
        })]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_twelve_safe_logical_conjunct_prunes_before_bounded_decode() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::start_with_timestamp_unit(
        temp.path().join("logical-pruning.db"),
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let ingest_app = router(storage.clone());
    assert_eq!(
        ingest_app
            .oneshot(ingest_request(make_lines(0, 8_192)))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    let before = storage.stats().await.unwrap();
    let app = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 500,
            max_work_rows: 500,
            ..LogsQueryLimits::default()
        },
    );
    let response = app
        .oneshot(logsql_request(
            r#"service:="api" AND (request OR ="never") | limit 500"#,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).len(),
        410
    );
    let after = storage.stats().await.unwrap();
    assert_eq!(
        after.query_decoded_entries - before.query_decoded_entries,
        410,
        "the indexed service conjunct must prune before the Rust OR evaluator"
    );
    assert_eq!(
        after.query_candidate_blocks - before.query_candidate_blocks,
        1,
        "only the service=api/level=error block should reach bounded decode"
    );
    storage.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_ten_quoted_literals_and_field_identifiers_match_oracle_and_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("quoted-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let body = [
        r#"{"_time":1800000000000001,"_msg":"line one\nline two\t\"quoted\"\\slash","level":"info","case":"double"}"#,
        r#"{"_time":1800000000000002,"_msg":"single'quote A λ","level":"info","case":"single"}"#,
        r#"{"_time":1800000000000003,"_msg":"raw\\n\"double\"'single\\slash","level":"info","case":"raw"}"#,
        r#"{"_time":1800000000000004,"_msg":"left|right","level":"info","case":"pipe"}"#,
        r#"{"_time":1800000000000005,"_msg":"quoted field","level":"info","case":"field","log:level":"error"}"#,
    ]
    .join("\n");
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    let cases = [
        (r#""line one\nline two\t\"quoted\"\\slash""#, "double"),
        (r#"'single\'quote \x41 \u03bb'"#, "single"),
        (r#"`raw\n"double"'single\slash`"#, "raw"),
        (r#""left|right""#, "pipe"),
        (r#""log:level":="error""#, "field"),
    ];
    for (query, expected) in cases {
        let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        let rows = ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap());
        assert_eq!(rows.len(), 1, "{query}");
        assert_eq!(rows[0]["case"], expected, "{query}");
    }

    for malformed in [r#""bad\q""#, r#"'bad\q'"#, r#""bad\uD800""#] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(reopened.clone());
    for (query, expected) in cases {
        let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        let rows = ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap());
        assert_eq!(rows.len(), 1, "{query}");
        assert_eq!(rows[0]["case"], expected, "{query}");
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_ten_logsql_sort_offset_limit_count_survive_optimize_and_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("pagination-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let body = [
        r#"{"_time":90,"_msg":"older","level":"info","page_group":"page"}"#,
        r#"{"_time":100,"_msg":"tie-b","level":"info","page_group":"page"}"#,
        r#"{"_time":100,"_msg":"tie-a","level":"info","page_group":"page"}"#,
        r#"{"_time":110,"_msg":"newer","level":"info","page_group":"page"}"#,
    ]
    .join("\n");
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn messages(app: &axum::Router, query: &str) -> Vec<String> {
        let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
            .into_iter()
            .map(|row| row["_msg"].as_str().unwrap().to_owned())
            .collect()
    }

    let asc = "page_group:page | sort by (_time) asc | offset 1 | limit 3";
    let desc = "page_group:page | order by (_time) desc | skip 1 | head 2";
    assert_eq!(messages(&app, asc).await, ["tie-a", "tie-b", "newer"]);
    assert_eq!(messages(&app, desc).await, ["tie-a", "tie-b"]);
    assert!(
        messages(&app, "page_group:page | sort by (_time) | limit 0")
            .await
            .is_empty()
    );

    let response = app
        .clone()
        .oneshot(logsql_request("level:info | stats count() as total"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
        b"{\"total\":4}\n"
    );

    storage.flush().await.unwrap();
    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    assert_eq!(messages(&app, asc).await, ["tie-a", "tie-b", "newer"]);
    assert_eq!(messages(&app, desc).await, ["tie-a", "tie-b"]);

    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(reopened.clone());
    assert_eq!(messages(&app, asc).await, ["tie-a", "tie-b", "newer"]);
    assert_eq!(messages(&app, desc).await, ["tie-a", "tie-b"]);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_thirteen_median_state_is_bounded_and_reader_remains_reusable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::start_with_timestamp_unit(
        temp.path().join("median-limit-logsql.db"),
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        router(storage.clone())
            .oneshot(ingest_request(
                r#"{"_time":1,"_msg":"wide median","level":"info","a":1,"b":2,"c":3}"#.to_owned(),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    let app = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    );
    let limited = app
        .clone()
        .oneshot(logsql_request("_time:[1,2) | stats median(a,b,c) as value"))
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_work_rows",
            "limit": 1
        })
    );
    assert_eq!(
        pipeline_rows(&app, "_time:[1,2) | stats count() as total").await,
        [serde_json::json!({"total": 1})]
    );
    storage.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_quantile_and_stddev_match_retained_semantics_and_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("quantile-stddev-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let body = [
        r#"{"_time":1800000000000001,"_msg":"s07 one","level":"info","stats_group":"s07","q":"1","n":0}"#,
        r#"{"_time":1800000000000002,"_msg":"s07 ten","level":"info","stats_group":"s07","q":"10","n":2}"#,
        r#"{"_time":1800000000000003,"_msg":"s07 two","level":"info","stats_group":"s07","q":"2","n":4}"#,
        r#"{"_time":1800000000000004,"_msg":"s07 alpha ten","level":"info","stats_group":"s07","q":"a10","n":6}"#,
        r#"{"_time":1800000000000005,"_msg":"s07 alpha two","level":"info","stats_group":"s07","q":"a2","n":"8"}"#,
        r#"{"_time":1800000000000006,"_msg":"s07 null","level":"info","stats_group":"s07","q":null,"n":null}"#,
        r#"{"_time":1800000000000007,"_msg":"s07 missing","level":"info","stats_group":"s07"}"#,
    ]
    .join("\n");
    assert_eq!(
        router(storage.clone())
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn assert_stats(app: &axum::Router) {
        let rows = pipeline_rows(
            app,
            r#"stats_group:="s07" | stats quantile(0, q) as minimum, quantile(0.5, q) as p50, quantile(1, q) as maximum, stddev(n) as sigma"#,
        )
        .await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["minimum"], "");
        assert_eq!(rows[0]["p50"], "2");
        assert_eq!(rows[0]["maximum"], "a10");
        assert!((rows[0]["sigma"].as_f64().unwrap() - 5.0_f64.sqrt()).abs() < 1e-12);

        assert_eq!(
            pipeline_rows(
                app,
                r#"stats_group:="s07" | fields q | stats QuAnTiLe(0.5) as p50"#,
            )
            .await,
            [serde_json::json!({"p50": "10"})]
        );
        assert_eq!(
            pipeline_rows(
                app,
                r#"stats_group:="s07" | stats quantile(0.5, never_present) as p50"#,
            )
            .await,
            [serde_json::json!({"p50": ""})]
        );
        assert_eq!(
            pipeline_rows(
                app,
                r#"stats_group:="s07" | stats StDdEv(never_present) as sigma"#,
            )
            .await,
            [serde_json::json!({"sigma": null})]
        );
        assert_eq!(
            pipeline_rows(app, r#"stats_group:="s07" | stats quantile(0.5, q) median"#,).await,
            [serde_json::json!({"median": "2"})]
        );
    }

    let app = router(storage.clone());
    assert_stats(&app).await;
    for malformed in [
        r#"stats_group:="s07" | stats quantile()"#,
        r#"stats_group:="s07" | stats quantile(word, q)"#,
        r#"stats_group:="s07" | stats quantile(-0.1, q)"#,
        r#"stats_group:="s07" | stats quantile(1.1, q)"#,
        r#"stats_group:="s07" | stats quantile(0.5, q) tail extra"#,
        r#"stats_group:="s07" | stats stddev"#,
    ] {
        assert_eq!(
            app.clone()
                .oneshot(logsql_request(malformed))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST,
            "{malformed}"
        );
    }

    storage.flush().await.unwrap();
    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_stats(&router(reopened.clone())).await;
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_quantile_state_is_bounded_and_reader_remains_reusable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::start_with_timestamp_unit(
        temp.path().join("quantile-limit-logsql.db"),
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        router(storage.clone())
            .oneshot(ingest_request(
                r#"{"_time":1,"_msg":"wide quantile","level":"info","a":1,"b":"2","c":"3"}"#
                    .to_owned(),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    let app = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    );
    let limited = app
        .clone()
        .oneshot(logsql_request(
            "_time:[1,2) | stats quantile(0.5, a, b, c) as value",
        ))
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_work_rows",
            "limit": 1
        })
    );
    assert_eq!(
        pipeline_rows(&app, "_time:[1,2) | stats stddev(a) as sigma").await,
        [serde_json::json!({"sigma": 0.0})]
    );
    let stddev_limited = app
        .clone()
        .oneshot(logsql_request("_time:[1,2) | stats stddev(a, b) as sigma"))
        .await
        .unwrap();
    assert_eq!(stddev_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(stddev_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_work_rows",
            "limit": 1
        })
    );
    assert_eq!(
        pipeline_rows(&app, "_time:[1,2) | stats count() as total").await,
        [serde_json::json!({"total": 1})]
    );
    storage.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_sum_len_counts_utf8_text_and_reopens() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("sum-len-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let body = [
        r#"{"_time":1,"_msg":"one","level":"info","stats_group":"s09","text":"é","n":12,"flag":true,"nested":{"leaf":"λ"}}"#,
        r#"{"_time":2,"_msg":"two","level":"info","stats_group":"s09","text":"","n":-3,"flag":false,"nested":null}"#,
        r#"{"_time":3,"_msg":"three","level":"info","stats_group":"s09","text":null}"#,
        r#"{"_time":4,"_msg":"four","level":"info","stats_group":"s09"}"#,
    ]
    .join("\n");
    assert_eq!(
        router(storage.clone())
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn assert_stats(app: &axum::Router) {
        assert_eq!(
            pipeline_rows(
                app,
                r#"stats_group:="s09" | stats sum_len(text) as text_bytes, sum_len(n, flag) as scalar_bytes, sum_len(nested) as object_bytes, sum_len(never_present) as missing_bytes"#,
            )
            .await,
            [serde_json::json!({
                "text_bytes": 2,
                "scalar_bytes": 13,
                "object_bytes": 13,
                "missing_bytes": 0
            })]
        );
        assert_eq!(
            pipeline_rows(
                app,
                r#"stats_group:="s09" | fields text, n | stats SuM_LeN() as bytes, sum_len(text*) as text_bytes"#,
            )
            .await,
            [serde_json::json!({"bytes": 6, "text_bytes": 2})]
        );
    }

    let app = router(storage.clone());
    assert_stats(&app).await;
    for malformed in [
        r#"stats_group:="s09" | stats sum_len"#,
        r#"stats_group:="s09" | stats sum_len(text n)"#,
        r#"stats_group:="s09" | stats sum_len(text) limit 2"#,
    ] {
        assert_eq!(
            app.clone()
                .oneshot(logsql_request(malformed))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST,
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "_time:[1,2) | stats sum_len(text, n) as bytes",
    ))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_work_rows",
            "limit": 1
        })
    );
    assert_eq!(
        pipeline_rows(&app, r#"stats_group:="s09" | stats count() as total"#).await,
        [serde_json::json!({"total": 4})]
    );

    storage.flush().await.unwrap();
    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_stats(&router(reopened.clone())).await;
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_any_and_field_extrema_preserve_rich_rows_and_reopen() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("any-field-extrema-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let body = [
        r#"{"_time":1,"_msg":"missing","level":"info","stats_group":"s10","payload":"missing-key"}"#,
        r#"{"_time":2,"_msg":"null","level":"info","stats_group":"s10","key":null,"any_src":null,"payload":"null-key"}"#,
        r#"{"_time":3,"_msg":"empty","level":"info","stats_group":"s10","key":"","any_src":"","payload":"empty-key"}"#,
        r#"{"_time":4,"_msg":"ten","level":"info","stats_group":"s10","key":10,"any_src":false,"payload":{"rank":"ten"}}"#,
        r#"{"_time":5,"_msg":"two","level":"info","stats_group":"s10","key":2,"any_src":"later","payload":[1,"λ"],"null_target":null}"#,
        r#"{"_time":6,"_msg":"tie","level":"info","stats_group":"s10","key":2,"any_src":true,"payload":"tie-later"}"#,
    ]
    .join("\n");
    assert_eq!(
        router(storage.clone())
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn assert_stats(app: &axum::Router) {
        assert_eq!(
            pipeline_rows(
                app,
                r#"stats_group:="s10" | stats AnY(any_src) as selected, field_min(key, payload) as minimum_payload, field_max(key, payload) as maximum_payload, field_min(key, never_present) as missing_target, field_min(key, null_target) as null_target"#,
            )
            .await,
            [serde_json::json!({
                "selected": false,
                "minimum_payload": [1, "λ"],
                "maximum_payload": {"rank": "ten"},
                "missing_target": "",
                "null_target": null
            })]
        );
        assert_eq!(
            pipeline_rows(
                app,
                r#"stats_group:="s10" | stats any(never_present) as selected, field_min(never_present, payload) as minimum_payload, field_max(never_present, payload) as maximum_payload"#,
            )
            .await,
            [serde_json::json!({
                "selected": "",
                "minimum_payload": "",
                "maximum_payload": ""
            })]
        );
    }

    let app = router(storage.clone());
    assert_stats(&app).await;
    for malformed in [
        r#"stats_group:="s10" | stats any"#,
        r#"stats_group:="s10" | stats any()"#,
        r#"stats_group:="s10" | stats any(left, right)"#,
        r#"stats_group:="s10" | stats field_min(key)"#,
        r#"stats_group:="s10" | stats field_max(key, payload, extra)"#,
        r#"stats_group:="s10" | stats field_min(key*, payload)"#,
    ] {
        assert_eq!(
            app.clone()
                .oneshot(logsql_request(malformed))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST,
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "_time:[1,2] | stats any(any_src) as selected",
    ))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_work_rows",
            "limit": 1
        })
    );
    assert_eq!(
        pipeline_rows(&app, r#"stats_group:="s10" | stats count() as total"#).await,
        [serde_json::json!({"total": 6})]
    );

    storage.flush().await.unwrap();
    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_stats(&router(reopened.clone())).await;
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_seventeen_row_selection_stats_are_rich_bounded_and_durable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("row-selection-stats-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let body = [
        r#"{"_time":1,"_msg":"missing","level":"info","stats_group":"s11"}"#,
        r#"{"_time":2,"_msg":"null","level":"info","stats_group":"s11","key":null,"any_src":null,"payload":null}"#,
        r#"{"_time":3,"_msg":"empty","level":"info","stats_group":"s11","key":"","any_src":"","payload":""}"#,
        r#"{"_time":4,"_msg":"ten","level":"info","stats_group":"s11","key":10,"any_src":false,"payload":{"rank":"ten"}}"#,
        r#"{"_time":5,"_msg":"two","level":"info","stats_group":"s11","key":2,"any_src":"later","payload":[1,"λ"],"null_target":null}"#,
        r#"{"_time":6,"_msg":"tie","level":"info","stats_group":"s11","key":2,"any_src":true,"payload":"tie-later"}"#,
    ]
    .join("\n");
    assert_eq!(
        router(storage.clone())
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn assert_stats(app: &axum::Router) {
        assert_eq!(
            pipeline_rows(
                app,
                r#"stats_group:="s11" | fields key, any_src, payload, null_target | stats RoW_AnY(any_src, payload) as any_row, row_min(key) as minimum_row, row_max(key, payload, null_target) as maximum_row, row_min(never_present) as empty_row"#,
            )
            .await,
            [serde_json::json!({
                "any_row": {"any_src": false, "payload": {"rank": "ten"}},
                "minimum_row": {"key": 2, "any_src": "later", "payload": [1, "λ"], "null_target": null},
                "maximum_row": {"payload": {"rank": "ten"}},
                "empty_row": {}
            })]
        );
        assert_eq!(
            pipeline_rows(
                app,
                r#"stats_group:="s11" | fields any_src, payload | stats row_any() as all_row, row_any(payload*) as prefix_row, row_any(payload.r*) nested_prefix_row"#,
            )
            .await,
            [serde_json::json!({
                "all_row": {"any_src": false, "payload": {"rank": "ten"}},
                "prefix_row": {"payload": {"rank": "ten"}},
                "nested_prefix_row": {"payload": {"rank": "ten"}}
            })]
        );
    }

    let app = router(storage.clone());
    assert_stats(&app).await;
    for malformed in [
        r#"stats_group:="s11" | stats row_any"#,
        r#"stats_group:="s11" | stats row_any(left right)"#,
        r#"stats_group:="s11" | stats row_any(left**)"#,
        r#"stats_group:="s11" | stats row_min()"#,
        r#"stats_group:="s11" | stats row_min(key*, payload)"#,
        r#"stats_group:="s11" | stats row_max(*, payload)"#,
        r#"stats_group:="s11" | stats row_max(key, payload) tail extra"#,
    ] {
        assert_eq!(
            app.clone()
                .oneshot(logsql_request(malformed))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST,
            "{malformed}"
        );
    }

    let limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"stats_group:="s11" | fields any_src, payload | stats row_any() as selected"#,
    ))
    .await
    .unwrap();
    assert_eq!(limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(limited.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_work_rows",
            "limit": 1
        })
    );
    assert_eq!(
        pipeline_rows(&app, r#"stats_group:="s11" | stats count() as total"#).await,
        [serde_json::json!({"total": 6})]
    );

    storage.flush().await.unwrap();
    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_stats(&router(reopened.clone())).await;
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_ten_logsql_limits_cancel_errors_and_direct_sql_reuse_the_reader() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("limits-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let ingest_app = router(storage.clone());
    assert_eq!(
        ingest_app
            .oneshot(ingest_request(make_lines(0, 16_384)))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    let errors_before = storage.stats().await.unwrap().api_query_count;
    let default_app = router(storage.clone());
    let malformed = default_app
        .clone()
        .oneshot(logsql_request("level:"))
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(malformed.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "invalid_query",
            "reason": "malformed_logsql",
            "message": "LogsQL level: term requires a value"
        })
    );
    for (query, message) in [
        (
            "* | block_stats",
            "unsupported LogsQL pipeline \"block_stats\"",
        ),
        (
            "* | blocks_count",
            "unsupported LogsQL pipeline \"blocks_count\"",
        ),
    ] {
        let unsupported = default_app
            .clone()
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(unsupported.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(unsupported.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap(),
            serde_json::json!({
                "error": "unsupported_capability",
                "reason": "unsupported_logsql",
                "message": message
            })
        );
    }
    let missing = default_app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/select/logsql/query")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        storage.stats().await.unwrap().api_query_count,
        errors_before,
        "parser failures must not reach storage"
    );

    let result_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 2,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("* | limit 3"))
    .await
    .unwrap();
    assert_eq!(result_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(result_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_result_rows",
            "limit": 2
        })
    );

    let pipeline_result_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 2,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("* | field_names"))
    .await
    .unwrap();
    assert_eq!(
        pipeline_result_limited.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(pipeline_result_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_result_rows",
            "limit": 2
        })
    );

    let stats_result_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 2,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("* | stats values(host) limit 3 as hosts"))
    .await
    .unwrap();
    assert_eq!(
        stats_result_limited.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(stats_result_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_result_rows",
            "limit": 2
        })
    );

    let discovery_app = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 200,
            max_work_rows: 20_000,
            ..LogsQueryLimits::default()
        },
    );
    let discovered = pipeline_rows(&discovery_app, "* | field_values _msg limit 128").await;
    assert_eq!(discovered.len(), 128);
    assert!(
        discovered.iter().all(|row| row["hits"] == 0),
        "crossing the explicit cardinality limit makes retained hit counts unknown"
    );

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 1,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("* | limit 1"))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(work_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_work_rows",
            "limit": 1
        })
    );

    let response_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_response_bytes: 32,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("* | limit 1"))
    .await
    .unwrap();
    assert_eq!(response_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_response_bytes",
            "limit": 32
        })
    );

    let deadline_app = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    );
    let timed_out = deadline_app
        .oneshot(logsql_request(r#""request" | limit 100000"#))
        .await
        .unwrap();
    assert_eq!(timed_out.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(timed_out.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "timeout",
            "reason": "query_deadline",
            "deadline_ms": 1
        })
    );
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > 0 && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(
        stats.api_query_cancelled > 0,
        "timeout did not cancel storage"
    );
    assert_eq!(stats.api_query_in_flight, 0);
    assert!(stats.api_query_errors > 0);
    let reused = default_app
        .clone()
        .oneshot(logsql_request("level:error | limit 1"))
        .await
        .unwrap();
    assert_eq!(reused.status(), StatusCode::OK);

    let cancelled_before = storage.stats().await.unwrap().api_query_cancelled;
    let pipeline_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | fields _msg | filter *request* | stats count_uniq(_msg) as unique",
    ))
    .await
    .unwrap();
    assert_eq!(pipeline_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_pipeline_cancel = default_app
        .clone()
        .oneshot(logsql_request("level:error | stats count() as total"))
        .await
        .unwrap();
    assert_eq!(reused_after_pipeline_cancel.status(), StatusCode::OK);

    let cancelled_before_running_stats = storage.stats().await.unwrap().api_query_cancelled;
    let running_stats_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | running_stats by (service) count() as running | fields running | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(running_stats_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_running_stats
            && stats.api_query_in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_running_stats);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_running_stats_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            "level:error | running_stats count() as running | fields running | limit 1",
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_running_stats_cancel.status(), StatusCode::OK);

    let cancelled_before_total_stats = storage.stats().await.unwrap().api_query_cancelled;
    let total_stats_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | total_stats by (service) count() as total | fields total | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(total_stats_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_total_stats
            && stats.api_query_in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_total_stats);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_total_stats_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            "level:error | total_stats count() as total | fields total | limit 1",
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_total_stats_cancel.status(), StatusCode::OK);

    let cancelled_before_time_add = storage.stats().await.unwrap().api_query_cancelled;
    let time_add_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | time_add 1ns | fields _time | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(time_add_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_time_add && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_time_add);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_time_add_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            "level:error | time_add 1ns | fields _time | limit 1",
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_time_add_cancel.status(), StatusCode::OK);

    let cancelled_before_uniq = storage.stats().await.unwrap().api_query_cancelled;
    let uniq_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request("* | uniq by (_msg) with hits limit 10000"))
    .await
    .unwrap();
    assert_eq!(uniq_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_uniq && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_uniq);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_uniq_cancel = default_app
        .clone()
        .oneshot(logsql_request("level:error | uniq by (level) with hits"))
        .await
        .unwrap();
    assert_eq!(reused_after_uniq_cancel.status(), StatusCode::OK);

    let cancelled_before_facets = storage.stats().await.unwrap().api_query_cancelled;
    let facets_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | fields _msg, context | facets max_values_per_field 20000",
    ))
    .await
    .unwrap();
    assert_eq!(facets_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_facets && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_facets);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_facets_cancel = default_app
        .clone()
        .oneshot(logsql_request("level:error | facets 1"))
        .await
        .unwrap();
    assert_eq!(reused_after_facets_cancel.status(), StatusCode::OK);

    let cancelled_before_coalesce = storage.stats().await.unwrap().api_query_cancelled;
    let coalesce_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | coalesce(_msg) as selected | fields selected | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(coalesce_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_coalesce && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_coalesce);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_coalesce_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            "level:error | coalesce(_msg) as selected | fields selected | limit 1",
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_coalesce_cancel.status(), StatusCode::OK);

    let cancelled_before_copy = storage.stats().await.unwrap().api_query_cancelled;
    let copy_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | copy * as copied* | fields copied* | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(copy_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_copy && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_copy);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_copy_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            "level:error | copy _msg as selected | fields selected | limit 1",
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_copy_cancel.status(), StatusCode::OK);

    let cancelled_before_rename = storage.stats().await.unwrap().api_query_cancelled;
    let rename_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | rename * as moved* | fields moved* | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(rename_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_rename && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_rename);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_rename_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            "level:error | rename _msg as selected | fields selected | limit 1",
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_rename_cancel.status(), StatusCode::OK);

    let cancelled_before_format = storage.stats().await.unwrap().api_query_cancelled;
    let format_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | format '<urlencode:_msg><hexencode:_msg>' as selected | fields selected | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(format_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_format && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_format);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_format_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            "level:error | format '<_msg>' as selected | fields selected | limit 1",
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_format_cancel.status(), StatusCode::OK);

    let cancelled_before_math = storage.stats().await.unwrap().api_query_cancelled;
    let math_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | math ln(exp(1)) + round(3.14159, 0.01) + _time as selected | fields selected | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(math_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_math && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_math);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_math_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            "level:error | math 1 + 1 as selected | fields selected | limit 1",
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_math_cancel.status(), StatusCode::OK);

    let cancelled_before_replace = storage.stats().await.unwrap().api_query_cancelled;
    let replace_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "* | replace (e, replacement-expands) | fields _msg | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(replace_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_replace && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_replace);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_replace_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            "level:error | replace (e, E) | fields _msg | limit 1",
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_replace_cancel.status(), StatusCode::OK);

    let cancelled_before_replace_regexp = storage.stats().await.unwrap().api_query_cancelled;
    let replace_regexp_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"* | replace_regexp ("[a-z]", replacement-expands) | fields _msg | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(replace_regexp_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_replace_regexp
            && stats.api_query_in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_replace_regexp);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_replace_regexp_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            r#"level:error | replace_regexp ("e", E) | fields _msg | limit 1"#,
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_replace_regexp_cancel.status(), StatusCode::OK);

    let cancelled_before_pack_json = storage.stats().await.unwrap().api_query_cancelled;
    let pack_json_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"* | pack_json as selected | fields selected | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(pack_json_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_pack_json && stats.api_query_in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_pack_json);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_pack_json_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            r#"level:error | pack_json fields (_msg, level) as selected | fields selected | limit 1"#,
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_pack_json_cancel.status(), StatusCode::OK);

    let cancelled_before_unpack_json = storage.stats().await.unwrap().api_query_cancelled;
    let unpack_json_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"* | pack_json fields (_msg, level, service) as packed | unpack_json from packed result_prefix selected. | fields selected | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(unpack_json_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_unpack_json
            && stats.api_query_in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_unpack_json);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_unpack_json_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            r#"level:error | pack_json fields (_msg, level) as packed | unpack_json from packed result_prefix selected. | fields selected | limit 1"#,
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_unpack_json_cancel.status(), StatusCode::OK);

    let cancelled_before_unpack_logfmt = storage.stats().await.unwrap().api_query_cancelled;
    let unpack_logfmt_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"* | pack_logfmt fields (_msg, level, service) as packed | unpack_logfmt from packed result_prefix selected. | fields selected | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(unpack_logfmt_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_unpack_logfmt
            && stats.api_query_in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_unpack_logfmt);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_unpack_logfmt_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            r#"level:error | pack_logfmt fields (_msg, level) as packed | unpack_logfmt from packed result_prefix selected. | fields selected | limit 1"#,
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_unpack_logfmt_cancel.status(), StatusCode::OK);

    let cancelled_before_unpack_syslog = storage.stats().await.unwrap().api_query_cancelled;
    let unpack_syslog_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"* | unpack_syslog result_prefix selected. | fields selected | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(unpack_syslog_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_unpack_syslog
            && stats.api_query_in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_unpack_syslog);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_unpack_syslog_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            r#"level:error | unpack_syslog result_prefix selected. | fields selected | limit 1"#,
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_unpack_syslog_cancel.status(), StatusCode::OK);

    let cancelled_before_unpack_words = storage.stats().await.unwrap().api_query_cancelled;
    let unpack_words_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"* | unpack_words as selected drop_duplicates | fields selected | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(unpack_words_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_unpack_words
            && stats.api_query_in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_unpack_words);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_unpack_words_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            r#"level:error | unpack_words as selected | fields selected | limit 1"#,
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_unpack_words_cancel.status(), StatusCode::OK);

    let cancelled_before_json_array_concat = storage.stats().await.unwrap().api_query_cancelled;
    let json_array_concat_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"* | format '[\"a\",{\"z\":1},[NaN]]' as selected | json_array_concat "," selected joined | fields joined | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(
        json_array_concat_timeout.status(),
        StatusCode::GATEWAY_TIMEOUT
    );
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_json_array_concat
            && stats.api_query_in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_json_array_concat);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_json_array_concat_cancel = default_app
        .clone()
        .oneshot(logsql_request(
            r#"level:error | format '[\"a\",{\"z\":1},[NaN]]' as selected | json_array_concat "," selected joined | fields joined | limit 1"#,
        ))
        .await
        .unwrap();
    assert_eq!(
        reused_after_json_array_concat_cancel.status(),
        StatusCode::OK
    );

    let cancelled_before_extract_regexp = storage.stats().await.unwrap().api_query_cancelled;
    let extract_regexp_timeout = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"* | extract_regexp "(?P<selected>.+)" | fields selected | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(extract_regexp_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before_extract_regexp
            && stats.api_query_in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before_extract_regexp);
    assert_eq!(stats.api_query_in_flight, 0);
    let reused_after_extract_regexp_cancel = default_app
        .oneshot(logsql_request(
            r#"level:error | extract_regexp "(?P<selected>e.+)" | fields selected | limit 1"#,
        ))
        .await
        .unwrap();
    assert_eq!(reused_after_extract_regexp_cancel.status(), StatusCode::OK);

    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();

    let conn = Connection::open(&database).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(&extension, None::<&str>).unwrap();
    }
    conn.load_extension_disable().unwrap();
    let collision = conn
        .execute(
            "CREATE VIRTUAL TABLE work_collision USING \
             timeless_logs(index_keys='max_work_entries')",
            [],
        )
        .unwrap_err();
    assert!(
        collision
            .to_string()
            .contains("collides with a built-in column name"),
        "{collision}"
    );
    let row_error = conn
        .query_row(
            "SELECT message FROM logs WHERE max_work_entries=1 ORDER BY ts LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap_err();
    assert!(row_error.to_string().contains("max_work_entries=1"));
    let count_error = conn
        .query_row(
            "SELECT n FROM timeless_log_count('logs',NULL,'request',NULL,NULL,1)",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_err();
    assert!(count_error.to_string().contains("max_work_entries=1"));
    let values_error = conn
        .query_row(
            "SELECT value FROM timeless_log_values('logs','service',NULL,NULL,NULL,NULL,10,1)",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap_err();
    assert!(values_error.to_string().contains("max_work_entries=1"));

    conn.progress_handler(1, Some(|| true)).unwrap();
    let interrupted = conn
        .query_row(
            "SELECT message FROM logs WHERE max_work_entries=100000 ORDER BY ts LIMIT 100000",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap_err();
    assert!(interrupted.to_string().contains("interrupt"));
    conn.progress_handler(0, None::<fn() -> bool>).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM logs WHERE max_work_entries=100000",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        16_384
    );
}

#[test]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
fn future_logs_schema_fails_before_vtab_initialization() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("future-logs.db");
    let conn = Connection::open(&database).unwrap();
    conn.execute_batch(
        "CREATE TABLE _timeless_schema_migrations(
           signal TEXT NOT NULL, version INTEGER NOT NULL,
           applied_at_unix INTEGER NOT NULL, server_version TEXT NOT NULL,
           extension_version TEXT NOT NULL, extension_data_abi INTEGER NOT NULL,
           PRIMARY KEY(signal, version));
         INSERT INTO _timeless_schema_migrations VALUES
           ('logs',999,0,'future','future',1);",
    )
    .unwrap();
    drop(conn);

    let error = match Storage::start_with_timestamp_unit(
        database.clone(),
        extension.into(),
        1,
        1,
        TimestampUnit::Microseconds,
    ) {
        Ok(_) => panic!("future logs database unexpectedly opened"),
        Err(error) => error,
    };
    assert!(error.contains("supports at most 1"), "{error}");
    let conn = Connection::open(database).unwrap();
    let created: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name='logs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(created, 0, "downgrade refusal must precede vtab creation");
}

fn ingest_request(body: String) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/insert/jsonline")
        .header("content-type", "application/x-ndjson")
        .body(Body::from(body))
        .unwrap()
}

fn logsql_request(query: &str) -> Request<Body> {
    let mut encoded = String::from("query=");
    for byte in query.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            b' ' => encoded.push('+'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    Request::builder()
        .method("POST")
        .uri("/select/logsql/query")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(encoded))
        .unwrap()
}

fn ndjson_values(body: &[u8]) -> Vec<serde_json::Value> {
    body.split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

async fn assert_evidence_rich_rows(storage: &Storage, passes: usize) {
    const BASE_TS: i64 = 1_800_000_000_000_000;
    const SEVERITIES: [&str; 8] = [
        "debug",
        "info",
        "notice",
        "warning",
        "error",
        "critical",
        "alert",
        "emergency",
    ];
    for pass in 0..passes {
        let rows = storage
            .query(timeless_logs_api::QuerySpec {
                limit: 10_000,
                max_work_rows: 10_000,
                ..Default::default()
            })
            .await
            .unwrap_or_else(|error| panic!("rich batch decode pass {pass}: {error}"));
        assert_eq!(rows.len(), 8_192, "rich batch decode pass {pass}");
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(row.ts, BASE_TS + index as i64, "pass {pass}, row {index}");
            assert_eq!(
                row.level,
                SEVERITIES[index % SEVERITIES.len()],
                "pass {pass}, row {index}"
            );
            assert_eq!(
                row.message,
                format!("query contract event {index}"),
                "pass {pass}, row {index}"
            );
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&row.metadata_json).unwrap(),
                serde_json::json!({
                    "context": {"attempt": index % 5, "retry": index % 3 == 0},
                    "host": format!("h{:02}", index % 64),
                    "service": if index % 4 == 0 { "api" } else { "worker" },
                    "status": if index % 8 == 4 { 500 } else { 200 },
                }),
                "pass {pass}, row {index}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_query_backed_lists_are_rich_cumulative_cached_and_reopenable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("query-backed-list-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let mut body = String::new();
    for index in 0..8_192_u64 {
        let mut row = serde_json::json!({
            "_time": 1_813_000_000_000_000_i64 + index as i64,
            "_msg": "request filler",
            "level": "info",
            "case": format!("filler-{index}"),
            "role": "filler",
        });
        match index {
            0 => {
                row["case"] = serde_json::json!("source-alpha");
                row["role"] = serde_json::json!("dictionary");
                row["lookup"] = serde_json::json!("alpha");
                row["_msg"] = serde_json::json!("dictionary alpha");
            }
            1 => {
                row["case"] = serde_json::json!("source-beta");
                row["role"] = serde_json::json!("dictionary");
                row["lookup"] = serde_json::json!("beta");
                row["_msg"] = serde_json::json!("dictionary beta");
            }
            2 => {
                row["case"] = serde_json::json!("source-rich");
                row["role"] = serde_json::json!("rich-dictionary");
                row["lookup_value"] = serde_json::json!(2.5);
                row["nested"] = serde_json::json!({"lookup": 2.5});
                row["_msg"] = serde_json::json!("dictionary rich");
            }
            3 => {
                row["case"] = serde_json::json!("target-alpha");
                row["target_group"] = serde_json::json!("target");
                row["_msg"] = serde_json::json!("alpha");
            }
            4 => {
                row["case"] = serde_json::json!("target-beta");
                row["target_group"] = serde_json::json!("target");
                row["_msg"] = serde_json::json!("before beta after");
            }
            5 => {
                row["case"] = serde_json::json!("target-both");
                row["target_group"] = serde_json::json!("target");
                row["_msg"] = serde_json::json!("alpha before beta");
            }
            6 => {
                row["case"] = serde_json::json!("target-rich");
                row["target_group"] = serde_json::json!("target");
                row["n"] = serde_json::json!(2.5);
            }
            7 => {
                row["case"] = serde_json::json!("source-empty");
                row["role"] = serde_json::json!("empty-dictionary");
            }
            8 => {
                row["case"] = serde_json::json!("target-missing");
                row["target_group"] = serde_json::json!("empty");
            }
            9 => {
                row["case"] = serde_json::json!("target-empty");
                row["target_group"] = serde_json::json!("empty");
                row["probe"] = serde_json::json!("");
            }
            10 => {
                row["case"] = serde_json::json!("target-null");
                row["target_group"] = serde_json::json!("empty");
                row["probe"] = serde_json::Value::Null;
            }
            _ => {}
        }
        body.push_str(&row.to_string());
        body.push('\n');
    }
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn cases(app: &axum::Router, query: &str) -> Vec<String> {
        let mut cases = pipeline_rows(app, &format!("{query} | fields case | limit 10000"))
            .await
            .into_iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        cases.sort();
        cases
    }

    assert_eq!(
        cases(
            &app,
            r#"target_group:="target" AND _msg:in(role:="dictionary" | fields lookup)"#,
        )
        .await,
        ["target-alpha"]
    );
    assert_eq!(
        cases(
            &app,
            r#"target_group:="target" AND _msg:contains_any(role:="dictionary" | fields lookup)"#,
        )
        .await,
        ["target-alpha", "target-beta", "target-both"]
    );
    assert_eq!(
        cases(
            &app,
            r#"target_group:="target" AND _msg:contains_all(role:="dictionary" | fields lookup)"#,
        )
        .await,
        ["target-both"]
    );
    assert_eq!(
        cases(
            &app,
            r#"n:in(role:="rich-dictionary" | fields lookup_value)"#
        )
        .await,
        ["target-rich"]
    );
    assert_eq!(
        cases(
            &app,
            r#"n:in(role:="rich-dictionary" | fields nested.lookup)"#
        )
        .await,
        ["target-rich"]
    );
    assert_eq!(
        cases(
            &app,
            r#"n:in(role:="rich-dictionary" | uniq nested.lookup)"#
        )
        .await,
        ["target-rich"]
    );
    assert_eq!(
        cases(
            &app,
            r#"target_group:="empty" AND probe:in(role:="empty-dictionary" | fields missing_value)"#,
        )
        .await,
        ["target-empty", "target-missing", "target-null"]
    );
    assert!(cases(&app, r#"case:in(role:="absent" | fields lookup)"#)
        .await
        .is_empty());
    assert!(
        cases(&app, r#"case:contains_any(role:="absent" | fields lookup)"#)
            .await
            .is_empty()
    );
    assert_eq!(
        cases(
            &app,
            r#"target_group:="target" AND case:contains_all(role:="absent" | fields lookup)"#,
        )
        .await,
        ["target-alpha", "target-beta", "target-both", "target-rich"]
    );
    assert_eq!(
        cases(
            &app,
            r#"case:in(target_group:="target" AND _msg:in(role:="dictionary" | fields lookup) | fields case)"#,
        )
        .await,
        ["target-alpha"]
    );
    assert_eq!(
        cases(
            &app,
            r#"* | copy case as selected | filter selected:in(role:="dictionary" | fields case)"#,
        )
        .await,
        ["source-alpha", "source-beta"]
    );
    assert_eq!(
        cases(
            &app,
            r#"* | format if (case:in(role:="dictionary" | fields case)) hit as result | filter result:=hit"#,
        )
        .await,
        ["source-alpha", "source-beta"]
    );

    let before_cached = storage.stats().await.unwrap().api_query_count;
    assert_eq!(
        cases(
            &app,
            r#"case:in(role:="dictionary" | fields case) OR case:in(role:="dictionary" | fields case)"#,
        )
        .await,
        ["source-alpha", "source-beta"]
    );
    assert_eq!(
        storage.stats().await.unwrap().api_query_count - before_cached,
        2,
        "one cached list scan plus one outer scan"
    );

    for malformed in [
        r#"in(role:="dictionary" | limit 1)"#,
        r#"in(role:="dictionary" | fields case,lookup)"#,
        r#"in(role:="dictionary" | fields *)"#,
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10_000,
            max_work_rows: 100,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:in(role:="dictionary" | fields case) | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(work_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );

    let result_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"case:in(role:="dictionary" | fields case) | limit 1"#,
    ))
    .await
    .unwrap();
    assert_eq!(result_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(result_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap()["reason"],
        "max_result_rows"
    );

    let state_bounded = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10_000,
            max_response_bytes: 500,
            ..LogsQueryLimits::default()
        },
    );
    assert_eq!(
        cases(
            &state_bounded,
            r#"case:in(role:="dictionary" | fields case)"#,
        )
        .await,
        ["source-alpha", "source-beta"]
    );
    let state_limited = state_bounded
        .oneshot(logsql_request(
            r#"case:in(role:="dictionary" | fields case) OR case:in(role:="dictionary" | fields case) | fields case | limit 10000"#,
        ))
        .await
        .unwrap();
    assert_eq!(state_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(state_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "error": "query_limit",
            "reason": "max_response_bytes",
            "limit": 500
        })
    );

    let cancelled_before = storage.stats().await.unwrap().api_query_cancelled;
    let timed_out = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10_000,
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "case:in(seq(request, absent) | fields case) | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(timed_out.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before);
    assert_eq!(stats.api_query_in_flight, 0);
    assert_eq!(
        cases(
            &app,
            r#"target_group:="target" AND _msg:in(role:="dictionary" | fields lookup)"#,
        )
        .await,
        ["target-alpha"]
    );

    storage.schedule_optimize().await.unwrap();
    assert_eq!(
        cases(
            &app,
            r#"target_group:="target" AND _msg:in(role:="dictionary" | fields lookup)"#,
        )
        .await,
        ["target-alpha"]
    );
    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();

    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        cases(
            &router(reopened.clone()),
            r#"target_group:="target" AND _msg:in(role:="dictionary" | fields lookup)"#,
        )
        .await,
        ["target-alpha"]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_common_case_filters_are_unicode_rich_bounded_and_reopenable() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("common-case-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let mut body = String::new();
    let severities = [
        "debug",
        "info",
        "notice",
        "warning",
        "error",
        "critical",
        "alert",
        "emergency",
    ];
    for index in 0..8_192_u64 {
        let mut row = serde_json::json!({
            "_time": 1_814_000_000_000_000_i64 + index as i64,
            "_msg": format!("RequestEvent filler {index}"),
            "level": severities[index as usize % severities.len()],
            "case": format!("filler-{index}"),
            "common_group": "filler",
        });
        if index > 24 {
            body.push_str(&row.to_string());
            body.push('\n');
            continue;
        }
        let (case, value) = match index {
            0 => ("original", Some(serde_json::json!("VictoriaMetrics"))),
            1 => ("lower", Some(serde_json::json!("victoriametrics"))),
            2 => ("upper", Some(serde_json::json!("VICTORIAMETRICS"))),
            3 => ("lower-first", Some(serde_json::json!("victoriaMetrics"))),
            4 => ("lower-second", Some(serde_json::json!("Victoriametrics"))),
            5 => ("invalid-mixed", Some(serde_json::json!("VIctoriaMetrics"))),
            6 => (
                "boundary",
                Some(serde_json::json!("before victoriaMetrics after")),
            ),
            7 => (
                "attached",
                Some(serde_json::json!("beforevictoriaMetricsafter")),
            ),
            8 => ("punctuation", Some(serde_json::json!("(VictoriaMetrics)!"))),
            9 => ("dot-original", Some(serde_json::json!("İx"))),
            10 => ("dot-lower", Some(serde_json::json!("ix"))),
            11 => ("dot-upper", Some(serde_json::json!("İX"))),
            12 => ("dot-decomposed", Some(serde_json::json!("i̇x"))),
            13 => ("title-original", Some(serde_json::json!("ǅx"))),
            14 => ("title-upper", Some(serde_json::json!("ǄX"))),
            15 => ("title-lower", Some(serde_json::json!("ǆx"))),
            16 => ("literal-star", Some(serde_json::json!("*"))),
            17 => ("quoted-comma", Some(serde_json::json!("Error,Retry"))),
            18 => ("state-missing", None),
            19 => ("state-null", Some(serde_json::Value::Null)),
            20 => ("state-empty", Some(serde_json::json!(""))),
            21 => ("rich-number", Some(serde_json::json!(2.5))),
            22 => {
                row["common_object"] = serde_json::json!({"stage": "VictoriaMetrics"});
                row["common_array"] = serde_json::json!(["VictoriaMetrics", 2.5, true]);
                ("rich-nested", Some(serde_json::json!("not-common")))
            }
            23 => {
                row["common_extra"] = serde_json::json!("VictoriaMetrics");
                ("prefix-only", Some(serde_json::json!("not-common")))
            }
            24 => {
                row["_msg"] = serde_json::json!("MessageCase");
                ("message-exact", Some(serde_json::json!("not-common")))
            }
            _ => unreachable!("indices above 24 continue before special-row selection"),
        };
        row["case"] = serde_json::json!(case);
        row["common_group"] = serde_json::json!(if index <= 17 {
            "common"
        } else if index <= 20 {
            "state"
        } else {
            "rich"
        });
        if let Some(value) = value {
            row["common_value"] = value;
        }
        body.push_str(&row.to_string());
        body.push('\n');
    }
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(body))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    async fn cases(app: &axum::Router, query: &str) -> Vec<String> {
        let mut cases = pipeline_rows(app, &format!("{query} | fields case | limit 10000"))
            .await
            .into_iter()
            .map(|row| row["case"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        cases.sort();
        cases
    }

    let exact = ["lower", "lower-first", "lower-second", "original", "upper"];
    assert_eq!(
        cases(
            &app,
            r#"common_group:="common" common_value:equals_common_case(VictoriaMetrics)"#,
        )
        .await,
        exact
    );
    assert_eq!(
        cases(
            &app,
            r#"common_group:="common" common_value:contains_common_case(VictoriaMetrics)"#,
        )
        .await,
        [
            "boundary",
            "lower",
            "lower-first",
            "lower-second",
            "original",
            "punctuation",
            "upper",
        ]
    );
    assert_eq!(
        cases(&app, r#"common_value:equals_common_case(İx)"#).await,
        ["dot-lower", "dot-original", "dot-upper"]
    );
    assert_eq!(
        cases(&app, r#"common_value:equals_common_case(ǅx)"#).await,
        ["title-original", "title-upper"]
    );
    assert_eq!(
        cases(&app, r#"common_value:equals_common_case("*")"#).await,
        ["literal-star"]
    );
    assert_eq!(
        cases(&app, r#"common_value:equals_common_case("Error,Retry")"#,).await,
        ["quoted-comma"]
    );
    assert_eq!(
        cases(
            &app,
            r#"common_group:="state" common_value:equals_common_case("")"#,
        )
        .await,
        ["state-empty", "state-missing", "state-null"]
    );
    assert_eq!(
        cases(
            &app,
            r#"common_group:="state" common_value:contains_common_case("")"#,
        )
        .await,
        ["state-empty", "state-missing", "state-null"]
    );
    assert_eq!(
        cases(&app, r#"common_value:equals_common_case("2.5")"#).await,
        ["rich-number"]
    );
    assert_eq!(
        cases(
            &app,
            r#"common_object.stage:equals_common_case(VictoriaMetrics)"#,
        )
        .await,
        ["rich-nested"]
    );
    assert_eq!(
        cases(
            &app,
            r#"common_array:contains_common_case(VictoriaMetrics)"#,
        )
        .await,
        ["rich-nested"]
    );
    assert_eq!(
        cases(
            &app,
            r#"case:="prefix-only" AND common*:equals_common_case(VictoriaMetrics)"#,
        )
        .await,
        ["prefix-only"]
    );
    assert_eq!(
        cases(&app, "equals_common_case(MessageCase)").await,
        ["message-exact"]
    );
    assert_eq!(
        cases(
            &app,
            r#"* | copy common_value as current | filter current:contains_common_case(VictoriaMetrics)"#,
        )
        .await,
        [
            "boundary",
            "lower",
            "lower-first",
            "lower-second",
            "original",
            "punctuation",
            "upper",
        ]
    );

    for malformed in [
        "equals_common_case",
        "equals_common_case(*)",
        "equals_common_case(,Error)",
        "contains_common_case(Error,,Retry)",
        "equals_common_case(ABCDEFGHIJK)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    let expansion_limited = app
        .clone()
        .oneshot(logsql_request(
            "equals_common_case(ABCDEFGHIJ0,ABCDEFGHIJ1,ABCDEFGHIJ2,ABCDEFGHIJ3,ABCDEFGHIJ4,ABCDEFGHIJ5,ABCDEFGHIJ6,ABCDEFGHIJ7,ABCDEFGHIJ8)",
        ))
        .await
        .unwrap();
    assert_eq!(expansion_limited.status(), StatusCode::BAD_REQUEST);

    let work_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10_000,
            max_work_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "contains_common_case(RequestEventAbsent) | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(work_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(work_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap()["reason"],
        "max_work_rows"
    );

    let result_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 1,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"common_value:equals_common_case(VictoriaMetrics) | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(result_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(result_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap()["reason"],
        "max_result_rows"
    );

    let response_limited = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10_000,
            max_response_bytes: 80,
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"common_value:equals_common_case(VictoriaMetrics) | fields case | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(response_limited.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response_limited.into_body(), usize::MAX)
                .await
                .unwrap()
        )
        .unwrap()["reason"],
        "max_response_bytes"
    );

    let cancelled_before = storage.stats().await.unwrap().api_query_cancelled;
    let timed_out = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10_000,
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        "contains_common_case(RequestEventAbsent) | limit 10000",
    ))
    .await
    .unwrap();
    assert_eq!(timed_out.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before);
    assert_eq!(stats.api_query_in_flight, 0);
    assert_eq!(
        cases(
            &app,
            r#"common_group:="common" common_value:equals_common_case(VictoriaMetrics)"#,
        )
        .await,
        exact
    );

    storage.schedule_optimize().await.unwrap();
    assert_eq!(
        cases(
            &app,
            r#"common_group:="common" common_value:equals_common_case(VictoriaMetrics)"#,
        )
        .await,
        exact
    );
    storage.flush().await.unwrap();
    storage.shutdown().await.unwrap();

    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        cases(
            &reopened_app,
            r#"common_group:="common" common_value:equals_common_case(VictoriaMetrics)"#,
        )
        .await,
        exact
    );
    assert_eq!(
        cases(
            &reopened_app,
            r#"common_object.stage:equals_common_case(VictoriaMetrics)"#,
        )
        .await,
        ["rich-nested"]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn session_eighteen_sample_is_random_bounded_durable_and_row_preserving() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("sample-logsql.db");
    let storage = Storage::start_with_timestamp_unit(
        database.clone(),
        extension.clone().into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let app = router(storage.clone());
    let severities = [
        "debug",
        "info",
        "notice",
        "warning",
        "error",
        "critical",
        "alert",
        "emergency",
    ];
    let body = (0..8_192)
        .map(|index| {
            serde_json::json!({
                "_time": 1_815_000_000_000_000_i64 + index,
                "_msg": format!("sample row {index}"),
                "level": severities[index as usize % severities.len()],
                "case": format!("row-{index}"),
                "sample_group": "sample",
                "nested": {"index": index, "even": index % 2 == 0},
                "array": [index, true, null, "value"],
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        app.clone()
            .oneshot(ingest_request(format!("{body}\n")))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    storage.barrier().await.unwrap();

    assert_eq!(
        pipeline_rows(
            &app,
            r#"sample_group:="sample" | sample 1 | stats count() as total"#,
        )
        .await,
        [serde_json::json!({"total": 8192})]
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"case:="row-0" | sample 1 | fields case, level, nested, array"#,
        )
        .await,
        [serde_json::json!({
            "case": "row-0",
            "level": "debug",
            "nested": {"index": 0, "even": true},
            "array": [0, true, null, "value"],
        })],
        "sample must preserve all selected rich values without rewriting storage"
    );

    let selected = pipeline_rows(
        &app,
        r#"sample_group:="sample" | sample 4 | fields case | limit 10000"#,
    )
    .await;
    assert!(
        (1_500..=2_600).contains(&selected.len()),
        "{}",
        selected.len()
    );
    let selected_indices = selected
        .iter()
        .map(|row| {
            row["case"]
                .as_str()
                .unwrap()
                .strip_prefix("row-")
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(selected_indices.windows(2).all(|pair| pair[0] < pair[1]));

    let projected = pipeline_rows(
        &app,
        r#"sample_group:="sample" | fields case, nested | sample 4 | limit 10000"#,
    )
    .await;
    assert!(
        (1_500..=2_600).contains(&projected.len()),
        "{}",
        projected.len()
    );
    assert!(projected.iter().all(|row| {
        let Some(object) = row.as_object() else {
            return false;
        };
        object.len() == 2 && object.contains_key("case") && object.contains_key("nested")
    }));
    let projected_indices = projected
        .iter()
        .map(|row| {
            row["case"]
                .as_str()
                .unwrap()
                .strip_prefix("row-")
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(projected_indices.windows(2).all(|pair| pair[0] < pair[1]));

    let mut sampled_total = 0_u64;
    let mut sampled_counts = std::collections::BTreeSet::new();
    for _ in 0..24 {
        let rows = pipeline_rows(
            &app,
            r#"sample_group:="sample" | sample 4 | stats count() as total"#,
        )
        .await;
        let count = rows[0]["total"].as_u64().unwrap();
        sampled_total += count;
        sampled_counts.insert(count);
    }
    assert!(
        (41_000..=57_000).contains(&sampled_total),
        "sample 4 selected {sampled_total} of 196608 rows"
    );
    assert!(
        sampled_counts.len() > 4,
        "sample must draw request-local random selections: {sampled_counts:?}"
    );
    assert_eq!(
        pipeline_rows(
            &app,
            r#"sample_group:="sample" | sample inf | stats count() as total"#,
        )
        .await,
        [serde_json::json!({"total": 0})]
    );

    for malformed in [
        "* | sample",
        "* | sample 0",
        "* | sample -1",
        "* | sample nope",
        "* | sample 08",
        "* | sample 1.5",
        "* | sample 1 2",
        "* | sample(2)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(malformed))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{malformed}");
    }

    for (limits, query, reason) in [
        (
            LogsQueryLimits {
                max_work_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"sample_group:="sample" | sample 4 | stats count() as total"#,
            "max_work_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 1,
                ..LogsQueryLimits::default()
            },
            r#"sample_group:="sample" | sample 1 | limit 2"#,
            "max_result_rows",
        ),
        (
            LogsQueryLimits {
                max_result_rows: 10_000,
                max_response_bytes: 64,
                ..LogsQueryLimits::default()
            },
            r#"sample_group:="sample" | sample 1 | fields case | limit 10000"#,
            "max_response_bytes",
        ),
    ] {
        let response = router_with_limits(storage.clone(), limits)
            .oneshot(logsql_request(query))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{query}"
        );
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["reason"], reason, "{query}: {body}");
    }

    let cancelled_before = storage.stats().await.unwrap().api_query_cancelled;
    let timed_out = router_with_limits(
        storage.clone(),
        LogsQueryLimits {
            max_result_rows: 10_000,
            deadline: Duration::from_millis(1),
            ..LogsQueryLimits::default()
        },
    )
    .oneshot(logsql_request(
        r#"sample_group:="sample" | sample 2 | fields case | limit 10000"#,
    ))
    .await
    .unwrap();
    assert_eq!(timed_out.status(), StatusCode::GATEWAY_TIMEOUT);
    for _ in 0..100 {
        let stats = storage.stats().await.unwrap();
        if stats.api_query_cancelled > cancelled_before && stats.api_query_in_flight == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stats = storage.stats().await.unwrap();
    assert!(stats.api_query_cancelled > cancelled_before);
    assert_eq!(stats.api_query_in_flight, 0);

    storage.schedule_optimize().await.unwrap();
    storage.barrier().await.unwrap();
    assert_eq!(
        pipeline_rows(
            &app,
            r#"sample_group:="sample" | sample 1 | stats count() as total"#,
        )
        .await,
        [serde_json::json!({"total": 8192})]
    );
    storage.flush().await.unwrap();

    storage.shutdown().await.unwrap();
    let reopened = Storage::start_with_timestamp_unit(
        database,
        extension.into(),
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    let reopened_app = router(reopened.clone());
    assert_eq!(
        pipeline_rows(
            &reopened_app,
            r#"sample_group:="sample" | sample 1 | stats count() as total"#,
        )
        .await,
        [serde_json::json!({"total": 8192})]
    );
    assert_eq!(
        pipeline_rows(
            &reopened_app,
            r#"case:="row-8191" | sample 1 | fields case, level, nested, array"#,
        )
        .await,
        [serde_json::json!({
            "case": "row-8191",
            "level": "emergency",
            "nested": {"index": 8191, "even": false},
            "array": [8191, true, null, "value"],
        })]
    );
    reopened.shutdown().await.unwrap();
}

fn make_evidence_rich_lines(count: usize) -> String {
    const BASE_TS: i64 = 1_800_000_000_000_000;
    const SEVERITIES: [&str; 8] = [
        "debug",
        "info",
        "notice",
        "warning",
        "error",
        "critical",
        "alert",
        "emergency",
    ];
    let mut body = String::with_capacity(count * 180);
    for index in 0..count {
        body.push_str(
            &serde_json::json!({
                "_time": BASE_TS + index as i64,
                "_msg": format!("query contract event {index}"),
                "level": SEVERITIES[index % SEVERITIES.len()],
                "service": if index % 4 == 0 { "api" } else { "worker" },
                "host": format!("h{:02}", index % 64),
                "status": if index % 8 == 4 { 500 } else { 200 },
                "context": {"retry": index % 3 == 0, "attempt": index % 5},
            })
            .to_string(),
        );
        body.push('\n');
    }
    body
}

fn make_lines(start: usize, count: usize) -> String {
    let mut body = String::with_capacity(count * 100);
    for i in start..start + count {
        let (level, service) = if i % 20 == 0 {
            ("error", "api")
        } else {
            (
                ["debug", "info", "warning"][i % 3],
                ["web", "worker", "billing"][i % 3],
            )
        };
        body.push_str(&format!(
            "{{\"_time\":{},\"_msg\":\"request {i}\",\"level\":\"{level}\",\"service\":\"{service}\",\"host\":\"host-{service}\",\"status\":\"200\"}}\n",
            1_700_000_000 + i
        ));
    }
    body
}
