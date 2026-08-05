use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use rusqlite::Connection;
use std::time::Duration;
use timeless_logs_api::{
    parse_logsql_at, router, router_with_limits, LogEntry, LogsQueryLimits, LogsqlOutput, Storage,
    TimestampUnit,
};
use tower::ServiceExt;

async fn pipeline_rows(app: &axum::Router, query: &str) -> Vec<serde_json::Value> {
    let response = app.clone().oneshot(logsql_request(query)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{query}");
    ndjson_values(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
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
                .body(Body::from("query=level%3Aerror+%7C+unpack_json"))
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
            "message": "unsupported LogsQL pipeline \"unpack_json\""
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
    let subquery = app
        .clone()
        .oneshot(logsql_request("case:in(value | fields case)"))
        .await
        .unwrap();
    assert_eq!(subquery.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(subquery.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap()["reason"],
        "unsupported_logsql"
    );

    for unsupported in [
        "case:contains_all(missing | fields case)",
        "case:contains_any(missing | fields case)",
    ] {
        let response = app
            .clone()
            .oneshot(logsql_request(unsupported))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{unsupported}"
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX).await.unwrap()
            )
            .unwrap()["reason"],
            "unsupported_logsql",
            "{unsupported}"
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
    let unsupported = default_app
        .clone()
        .oneshot(logsql_request("* | unpack_json"))
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
            "message": "unsupported LogsQL pipeline \"unpack_json\""
        })
    );
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
        .oneshot(logsql_request("level:error | stats count() as total"))
        .await
        .unwrap();
    assert_eq!(reused_after_pipeline_cancel.status(), StatusCode::OK);

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
