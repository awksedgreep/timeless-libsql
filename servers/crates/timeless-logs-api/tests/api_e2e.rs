use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use rusqlite::Connection;
use std::time::Duration;
use timeless_logs_api::{
    parse_logsql_at, router, router_with_limits, LogEntry, LogsQueryLimits, LogsqlOutput, Storage,
    TimestampUnit,
};
use tower::ServiceExt;

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
    reopened.shutdown().await.unwrap();
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
        .oneshot(logsql_request("level:error | limit 1"))
        .await
        .unwrap();
    assert_eq!(reused.status(), StatusCode::OK);

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
