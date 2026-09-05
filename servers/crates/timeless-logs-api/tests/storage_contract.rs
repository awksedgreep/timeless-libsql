//! Storage reporting contract (COMPRESSION_REPORTING_PLAN.md Phase 2): the
//! `/metrics` exposition and the stats JSON must publish the honest storage
//! split — data-block payload, index, WAL, freelist as separate series — and
//! the compression byte counters, all reconciling exactly with what the
//! engine's public `timeless_stats('logs')` reports on the same database.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;
use timeless_logs_api::{router, Storage};
use tower::ServiceExt;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn exposition_and_stats_json_reconcile_with_engine_timeless_stats() {
    let extension = std::env::var("TIMELESS_EXT_TEST_PATH")
        .expect("TIMELESS_EXT_TEST_PATH must point at libtimeless_ext");
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("logs.db");
    let storage = Storage::start(database.clone(), extension.clone().into(), 1, 8).unwrap();
    let app = router(storage.clone());

    // Seed past the extension's 8,192-entry buffer so blocks reach disk,
    // then take the ordered durability barrier.
    let response = app
        .clone()
        .oneshot(ingest_request(make_lines(0, 16_384)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = app
        .clone()
        .oneshot(Request::post("/api/v1/flush").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Drain the optimize backlog so first-pass compression accrues into the
    // persistent compression totals. No timers run in a test router, so the
    // database is quiescent afterwards.
    for _ in 0..32 {
        storage.schedule_optimize().await.unwrap();
        storage.barrier().await.unwrap();
        let stats = storage.stats().await.unwrap();
        if stats.optimize_pending_raw_entries == 0 && stats.compression_input_bytes_total > 0 {
            break;
        }
    }

    // /metrics: the honest storage series exist, each under its own family.
    let response = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    for family in [
        "# TYPE timeless_logs_storage_bytes gauge",
        "# TYPE timeless_logs_index_bytes gauge",
        "# TYPE timeless_logs_wal_bytes gauge",
        "# TYPE timeless_logs_freelist_bytes gauge",
        "# TYPE timeless_logs_compression_input_bytes_total counter",
        "# TYPE timeless_logs_compression_output_bytes_total counter",
        "# TYPE timeless_logs_raw_ingested_bytes_total counter",
    ] {
        assert!(text.contains(family), "missing {family} in:\n{text}");
    }
    let storage_bytes = series_value(&text, "timeless_logs_storage_bytes");
    let index_bytes = series_value(&text, "timeless_logs_index_bytes");
    let input_total = series_value(&text, "timeless_logs_compression_input_bytes_total");
    let output_total = series_value(&text, "timeless_logs_compression_output_bytes_total");
    let raw_ingested = series_value(&text, "timeless_logs_raw_ingested_bytes_total");
    assert!(storage_bytes > 0, "storage_bytes={storage_bytes}");
    assert!(index_bytes > 0, "index_bytes={index_bytes}");
    assert!(input_total > 0, "input_total={input_total}");
    assert!(output_total > 0, "output_total={output_total}");
    assert!(
        output_total <= input_total,
        "compression must not inflate: {output_total} > {input_total}"
    );
    // Raw ingested is the engine's persisted logical-row-bytes counter;
    // every seeded entry is on disk here, so it must be positive, and it
    // must reconcile exactly with the engine below.
    assert!(raw_ingested > 0, "raw_ingested={raw_ingested}");
    // The stored side of a compression ratio is data-block payload only:
    // it never includes the index, and both sit strictly inside the page
    // space of the whole database file.
    let disk_size = series_value(&text, "timeless_logs_disk_size_bytes");
    assert!(storage_bytes + index_bytes <= disk_size);

    // The stats JSON publishes the same figures.
    let response = app
        .clone()
        .oneshot(
            Request::get("/select/logsql/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["total_bytes"], storage_bytes);
    assert_eq!(json["index_size"], index_bytes);
    assert_eq!(json["compression_input_bytes_total"], input_total);
    assert_eq!(json["compression_output_bytes_total"], output_total);
    assert_eq!(json["raw_ingested_bytes_total"], raw_ingested);

    storage.shutdown().await.unwrap();

    // Ground truth: SELECT-ing the engine's public stats on the same
    // database must agree exactly with what the server exported.
    let engine = engine_stats(&database, Path::new(&extension));
    assert_eq!(engine["bytes_on_disk"], storage_bytes);
    assert_eq!(engine["index_bytes"], index_bytes);
    assert_eq!(engine["compression_input_bytes_total"], input_total);
    assert_eq!(engine["compression_output_bytes_total"], output_total);
    assert_eq!(engine["ingest_raw_bytes_total"], raw_ingested);
}

fn series_value(text: &str, name: &str) -> i64 {
    let prefix = format!("{name} ");
    text.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .unwrap_or_else(|| panic!("missing {name} in:\n{text}"))
        .parse()
        .unwrap()
}

fn engine_stats(database: &Path, extension: &Path) -> HashMap<String, i64> {
    let conn = Connection::open(database).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(extension, None::<&str>).unwrap();
    }
    conn.load_extension_disable().unwrap();
    let mut stmt = conn
        .prepare("SELECT key, CAST(value AS INTEGER) FROM timeless_stats('logs')")
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .unwrap();
    rows.filter_map(|row| {
        let (key, value) = row.unwrap();
        value.map(|value| (key, value))
    })
    .collect()
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
        let (level, service) = (
            ["debug", "info", "warning"][i % 3],
            ["web", "worker", "billing"][i % 3],
        );
        body.push_str(&format!(
            "{{\"_time\":{},\"_msg\":\"request {i}\",\"level\":\"{level}\",\"service\":\"{service}\",\"host\":\"host-{service}\",\"status\":\"200\"}}\n",
            1_700_000_000 + i
        ));
    }
    body
}
