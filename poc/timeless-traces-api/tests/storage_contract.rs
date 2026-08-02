use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;
use timeless_traces_api::{router, Storage, DEFAULT_RETENTION, MAX_BODY_BYTES, TRACE_CAPABILITY};
use tower::ServiceExt;

/// This test intentionally runs the server shell only against the built
/// extension. There is no fake store and no server-owned block/index path.
#[tokio::test]
async fn session_two_owns_lifecycle_durability_and_cold_reopen() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("traces.db");
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        2,
        8,
        Some(DEFAULT_RETENTION),
    )
    .unwrap();
    let app = router(storage.clone());

    let owner_error = match Storage::start(
        database.clone(),
        extension.clone(),
        1,
        1,
        Some(DEFAULT_RETENTION),
    ) {
        Ok(second) => {
            second.shutdown().await.unwrap();
            panic!("a second process unexpectedly acquired the owner lease")
        }
        Err(error) => error,
    };
    assert!(owner_error.contains("already owned"), "{owner_error}");

    assert_eq!(get_json(&app, "/live").await.0, StatusCode::OK);
    let ready = get_json(&app, "/ready").await;
    assert_eq!(ready.0, StatusCode::OK);
    assert_eq!(ready.1["status"], "ready");
    assert_eq!(ready.1["capability"], TRACE_CAPABILITY);
    assert_eq!(ready.1["module"], "timeless_traces");

    // Invalid input and oversized bodies never reach storage admission.
    let absent_otlp = post_body(&app, "/insert/opentelemetry/v1/traces", b"{}").await;
    assert_eq!(absent_otlp.0, StatusCode::BAD_REQUEST);
    let oversized = vec![0_u8; MAX_BODY_BYTES + 1];
    let rejected = post_body(&app, "/insert/opentelemetry/v1/traces", &oversized).await;
    assert_eq!(rejected.0, StatusCode::PAYLOAD_TOO_LARGE);
    let empty = storage.runtime_watermarks();
    assert_eq!(empty.admitted_requests, 0);
    assert_eq!(empty.admitted_spans, 0);
    assert_eq!(empty.admitted_body_bytes, 0);

    let spans = vec![RichSpan::fixture()];
    let batch = rich_batch(&spans);
    storage
        .submit_batch(batch, spans.len(), 4_321)
        .await
        .unwrap();
    let flush = post_json(&app, "/api/v1/flush").await;
    assert_eq!(flush.0, StatusCode::OK);
    assert_eq!(flush.1["through_requests"], 1);
    assert_eq!(flush.1["through_spans"], 1);
    assert_eq!(flush.1["through_body_bytes"], 4_321);
    assert_eq!(flush.1["data_plane"]["admitted_bytes"], 4_321);
    assert_eq!(flush.1["completed_requests"], 1);
    assert_eq!(flush.1["completed_spans"], 1);
    assert_eq!(flush.1["completed_body_bytes"], 4_321);
    assert_eq!(flush.1["queued_requests"], 0);
    assert_eq!(flush.1["in_flight_requests"], 0);

    let stats = get_json(&app, "/select/traces/stats").await;
    assert_eq!(stats.0, StatusCode::OK);
    assert_eq!(stats.1["buffered_spans"], 0);
    assert_eq!(stats.1["blocks"], 1);
    assert_eq!(stats.1["raw_blocks"], 1);
    assert_eq!(stats.1["reader_connections"], 2);
    assert_eq!(stats.1["writer_connections"], 1);
    assert_eq!(stats.1["admitted_requests"], 1);
    assert_eq!(stats.1["completed_requests"], 1);
    assert_eq!(stats.1["failed_requests"], 0);
    assert!(stats.1["database_file_bytes"].as_u64().unwrap() > 0);

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);
    assert_eq!(file_size(&suffix_path(&database, "-wal")), 0);

    assert_fixture_persisted(&database, &extension);
    let reopened = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        2,
        Some(DEFAULT_RETENTION),
    )
    .unwrap();
    let cold = reopened.stats().await.unwrap();
    assert_eq!(cold.capability, TRACE_CAPABILITY);
    assert_eq!(cold.blocks, 1);
    assert_eq!(cold.buffered_spans, 0);
    assert_eq!(
        cold.admitted_requests, 0,
        "process watermarks reset honestly"
    );
    reopened.shutdown().await.unwrap();

    // Existing files are fenced against accidental retention drift.
    let mismatch = match Storage::start(database, extension, 1, 2, None) {
        Ok(unexpected) => {
            unexpected.shutdown().await.unwrap();
            panic!("retention mismatch unexpectedly started")
        }
        Err(error) => error,
    };
    assert!(mismatch.contains("retention mismatch"), "{mismatch}");
}

#[tokio::test]
async fn bounded_queue_backpressures_without_over_admitting() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("saturation.db");
    let storage = Storage::start(database.clone(), extension, 1, 1, None).unwrap();

    // Hold SQLite's write lock. The first 8,192-span request becomes the one
    // in-flight writer command, the second occupies the sole queue slot, and
    // a third admission must remain pending without changing watermarks.
    let blocker = Connection::open(&database).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let first_storage = storage.clone();
    let first = tokio::spawn(async move {
        let spans = minimal_spans(8_192);
        let body_bytes = rich_batch(&spans).len();
        first_storage
            .submit_batch(rich_batch(&spans), spans.len(), body_bytes)
            .await
    });
    first.await.unwrap().unwrap();
    wait_for_in_flight(&storage, 1).await;

    let one = vec![RichSpan::minimal(9_000)];
    let one_batch = rich_batch(&one);
    storage
        .submit_batch(one_batch.clone(), 1, one_batch.len())
        .await
        .unwrap();
    let saturated = storage.runtime_watermarks();
    assert_eq!(saturated.admitted_requests, 2);
    assert_eq!(saturated.in_flight_requests, 1);
    assert_eq!(saturated.queued_requests, 1);
    assert_eq!(saturated.queued_spans, 1);
    assert_eq!(saturated.queued_body_bytes, one_batch.len() as u64);

    let mut third = Box::pin(storage.submit_batch(one_batch.clone(), 1, one_batch.len()));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut third)
            .await
            .is_err(),
        "a saturated one-request queue admitted a third request"
    );
    drop(third);
    let still_saturated = storage.runtime_watermarks();
    assert_eq!(still_saturated.admitted_requests, 2);
    assert_eq!(still_saturated.queued_requests, 1);

    blocker.execute_batch("COMMIT").unwrap();
    storage.barrier().await.unwrap();
    let drained = storage.runtime_watermarks();
    assert_eq!(drained.completed_requests, 2);
    assert_eq!(drained.completed_spans, 8_193);
    assert_eq!(drained.queued_requests, 0);
    assert_eq!(drained.in_flight_requests, 0);
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_rejects_a_non_extension_traces_table_before_readiness() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("wrong-schema.db");
    Connection::open(&database)
        .unwrap()
        .execute_batch("CREATE TABLE traces(not_the_extension TEXT)")
        .unwrap();
    let error = match Storage::start(database, extension, 1, 1, None) {
        Ok(unexpected) => {
            unexpected.shutdown().await.unwrap();
            panic!("incompatible table unexpectedly reached readiness")
        }
        Err(error) => error,
    };
    assert!(
        error.contains("incompatible timeless_traces extension"),
        "{error}"
    );
    assert!(error.contains(TRACE_CAPABILITY), "{error}");
}

#[cfg(unix)]
#[tokio::test]
async fn binary_sigterm_drains_and_kill9_preserves_the_flushed_prefix() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("signals.db");
    let seed = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        2,
        Some(DEFAULT_RETENTION),
    )
    .unwrap();
    let fixture = vec![RichSpan::fixture()];
    let blob = rich_batch(&fixture);
    seed.submit_batch(blob.clone(), 1, blob.len())
        .await
        .unwrap();
    seed.flush().await.unwrap();
    seed.shutdown().await.unwrap();
    drop(seed);

    let mut killed = spawn_server(&extension, &database);
    wait_for_tcp(killed.1).await;
    signal(killed.0.id(), "KILL");
    assert!(!killed.0.wait().unwrap().success());
    assert_fixture_persisted(&database, &extension);

    // The OS released the owner lease, startup recovers the same extension
    // database, and SIGTERM follows the graceful flush/checkpoint path.
    let mut terminated = spawn_server(&extension, &database);
    wait_for_tcp(terminated.1).await;
    signal(terminated.0.id(), "TERM");
    assert!(terminated.0.wait().unwrap().success());
    assert_eq!(file_size(&suffix_path(&database, "-wal")), 0);
    assert_fixture_persisted(&database, &extension);
}

async fn wait_for_in_flight(storage: &Storage, expected: u64) {
    for _ in 0..500 {
        if storage.runtime_watermarks().in_flight_requests == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("writer did not reach {expected} in-flight request(s)");
}

#[cfg(unix)]
fn spawn_server(extension: &Path, database: &Path) -> (std::process::Child, std::net::SocketAddr) {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);
    let child = Command::new(env!("CARGO_BIN_EXE_timeless-traces-api"))
        .arg(extension)
        .arg(database)
        .arg(address.to_string())
        .env("TIMELESS_TRACES_FLUSH_INTERVAL_SECS", "3600")
        .env("TIMELESS_TRACES_OPTIMIZE_INTERVAL_SECS", "3600")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    (child, address)
}

#[cfg(unix)]
async fn wait_for_tcp(address: std::net::SocketAddr) {
    for _ in 0..500 {
        if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("server did not listen on {address}");
}

#[cfg(unix)]
fn signal(pid: u32, name: &str) {
    let status = Command::new("kill")
        .arg(format!("-{name}"))
        .arg(pid.to_string())
        .status()
        .unwrap();
    assert!(status.success(), "failed to send SIG{name} to {pid}");
}

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn post_json(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn post_body(app: &axum::Router, uri: &str, body: &[u8]) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .body(Body::from(body.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, bytes.to_vec())
}

fn required_extension() -> PathBuf {
    let path = std::env::var_os("TIMELESS_EXT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../../target/debug/libtimeless_ext.so"));
    assert!(
        path.is_file(),
        "build timeless_ext or set TIMELESS_EXT_PATH; missing {}",
        path.display()
    );
    path
}

fn assert_fixture_persisted(database: &Path, extension: &Path) {
    let conn = Connection::open(database).unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(extension, None::<&str>).unwrap();
    }
    let row = conn
        .query_row(
            "SELECT hex(trace_id),hex(span_id),parent_span_id,name,service,kind,status,
                    start_ts,duration_ns,attributes,status_description,events,resource,
                    instrumentation_scope FROM traces",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, String>(13)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(row.0, "00112233445566778899AABBCCDDEEFF");
    assert_eq!(row.1, "0102030405060708");
    assert_eq!(row.2, None);
    assert_eq!(row.3, "GET /contract");
    assert_eq!(row.4, "contract-svc");
    assert_eq!(row.5, "server");
    assert_eq!(row.6, "error");
    assert_eq!(row.7, 1_700_000_000_000_000_000);
    assert_eq!(row.8, 120_000_000);
    assert_eq!(
        serde_json::from_str::<Value>(&row.9).unwrap()["retryable"],
        true
    );
    assert_eq!(row.10, "contract failure");
    assert_eq!(
        serde_json::from_str::<Value>(&row.11).unwrap()[0]["name"],
        "exception"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&row.12).unwrap()["replica"],
        7
    );
    assert_eq!(
        serde_json::from_str::<Value>(&row.13).unwrap()["name"],
        "contract-lib"
    );
}

#[derive(Clone)]
struct RichSpan {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: [u8; 8],
    name: String,
    service: String,
    kind: u8,
    status: u8,
    start_ts: i64,
    duration_ns: i64,
    attributes: String,
    status_description: String,
    events: String,
    resource: String,
    scope: String,
}

impl RichSpan {
    fn fixture() -> Self {
        Self {
            trace_id: hex_16("00112233445566778899aabbccddeeff"),
            span_id: hex_8("0102030405060708"),
            parent_span_id: [0; 8],
            name: "GET /contract".into(),
            service: "explicit-must-not-win".into(),
            kind: 1,
            status: 2,
            start_ts: 1_700_000_000_000_000_000,
            duration_ns: 120_000_000,
            attributes: r#"{"http.status_code":503,"retryable":true,"service.name":"contract-svc"}"#.into(),
            status_description: "contract failure".into(),
            events: r#"[{"attributes":{"handled":false},"name":"exception","timestamp":1700000000040000000}]"#.into(),
            resource: r#"{"replica":7,"service.name":"contract-svc"}"#.into(),
            scope: r#"{"name":"contract-lib","version":"4.5.6"}"#.into(),
        }
    }

    fn minimal(number: u64) -> Self {
        Self {
            trace_id: (number as u128).to_be_bytes(),
            span_id: number.to_be_bytes(),
            parent_span_id: [0; 8],
            name: "span".into(),
            service: "svc".into(),
            kind: 0,
            status: 0,
            start_ts: 1_700_000_000_000_000_000_i64.saturating_add(number as i64),
            duration_ns: 1,
            attributes: "{}".into(),
            status_description: String::new(),
            events: "[]".into(),
            resource: "{}".into(),
            scope: "{}".into(),
        }
    }
}

fn minimal_spans(count: usize) -> Vec<RichSpan> {
    (1..=count)
        .map(|number| RichSpan::minimal(number as u64))
        .collect()
}

fn rich_batch(spans: &[RichSpan]) -> Vec<u8> {
    let mut out = vec![0x02, 0, 0, 0];
    out.extend_from_slice(&(spans.len() as u32).to_le_bytes());
    for span in spans {
        out.extend_from_slice(&span.trace_id);
    }
    for span in spans {
        out.extend_from_slice(&span.span_id);
    }
    for span in spans {
        out.extend_from_slice(&span.parent_span_id);
    }
    text_column(&mut out, spans.iter().map(|span| span.name.as_str()));
    text_column(&mut out, spans.iter().map(|span| span.service.as_str()));
    out.extend(spans.iter().map(|span| span.kind));
    out.extend(spans.iter().map(|span| span.status));
    for span in spans {
        out.extend_from_slice(&span.start_ts.to_le_bytes());
    }
    for span in spans {
        out.extend_from_slice(&span.duration_ns.to_le_bytes());
    }
    text_column(&mut out, spans.iter().map(|span| span.attributes.as_str()));
    text_column(
        &mut out,
        spans.iter().map(|span| span.status_description.as_str()),
    );
    text_column(&mut out, spans.iter().map(|span| span.events.as_str()));
    text_column(&mut out, spans.iter().map(|span| span.resource.as_str()));
    text_column(&mut out, spans.iter().map(|span| span.scope.as_str()));
    out
}

fn text_column<'a>(out: &mut Vec<u8>, values: impl Iterator<Item = &'a str>) {
    for value in values {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }
}

fn hex_16(value: &str) -> [u8; 16] {
    let mut out = [0_u8; 16];
    decode_hex(value, &mut out);
    out
}

fn hex_8(value: &str) -> [u8; 8] {
    let mut out = [0_u8; 8];
    decode_hex(value, &mut out);
    out
}

fn decode_hex(value: &str, out: &mut [u8]) {
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).unwrap();
        out[index] = u8::from_str_radix(pair, 16).unwrap();
    }
}

fn suffix_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}
