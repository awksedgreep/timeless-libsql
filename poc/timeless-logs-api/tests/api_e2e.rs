use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use timeless_logs_api::{router, Storage};
use tower::ServiceExt;

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
    assert_eq!(stats.query_returned_entries, 410);
    assert!(stats.query_total_ns > 0);
    assert!(stats.query_snapshot_ns > 0);
    assert!(stats.query_materialize_ns > 0);
    assert_eq!(stats.query_snapshot_payload_bytes, 0);
    assert_eq!(stats.query_snapshot_payload_max_bytes, 0);
    assert_eq!(stats.query_stable_location_snapshots, 1);
    assert!(stats.query_payload_bytes_read > 0);
    assert!(stats.read_permit_count > 0);
    assert_eq!(stats.waiting_writers, 0);

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

fn ingest_request(body: String) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/insert/jsonline")
        .header("content-type", "application/x-ndjson")
        .body(Body::from(body))
        .unwrap()
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
            "{{\"_time\":{},\"_msg\":\"request {i}\",\"level\":\"{level}\",\"service\":\"{service}\",\"status\":\"200\"}}\n",
            1_700_000_000 + i
        ));
    }
    body
}
