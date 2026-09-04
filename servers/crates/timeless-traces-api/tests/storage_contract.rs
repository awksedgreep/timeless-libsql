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

#[test]
fn direct_sql_mixed_batches_optimize_rollback_and_reopen_preserve_v2() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("direct-v2.db");
    let conn = Connection::open(&database).unwrap();
    load_extension(&conn, &extension);
    conn.execute_batch("CREATE VIRTUAL TABLE traces USING timeless_traces;")
        .unwrap();
    conn.execute_batch(
        r#"INSERT INTO traces(
             trace_id,span_id,name,service,start_ts,duration_ns,attributes,
             status_description,events,resource,instrumentation_scope,links,
             trace_state,trace_flags,dropped_attributes_count,dropped_events_count,
             dropped_links_count,resource_schema_url,scope_schema_url,
             resource_dropped_attributes_count,scope_dropped_attributes_count)
           VALUES(
             'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbb','direct','svc',10,20,
             '{"typed":true}','ok','[{"name":"event","dropped_attributes_count":1}]',
             '{"service.name":"svc"}','{"name":"scope"}',
             '[{"trace_id":"00112233445566778899aabbccddeeff","span_id":"0102030405060708","trace_state":"x=y","attributes":{},"dropped_attributes_count":2,"flags":3}]',
             'vendor=direct',4294967295,4,5,6,'resource-schema','scope-schema',7,8);"#,
    )
    .unwrap();

    let v0 = RichSpan::minimal(20);
    let mut v1 = RichSpan::minimal(21);
    v1.name = "legacy-v1".into();
    v1.status_description = "retained by v1".into();
    let v2 = RichSpan::fixture();
    conn.execute(
        "INSERT INTO traces(traces) VALUES (?1)",
        [legacy_batch(&[v0], 0x01)],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO traces(traces) VALUES (?1)",
        [legacy_batch(&[v1], 0x02)],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO traces(traces) VALUES (?1)",
        [rich_batch(&[v2])],
    )
    .unwrap();
    conn.execute_batch("INSERT INTO traces(traces) VALUES ('flush');")
        .unwrap();

    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM traces WHERE links='[]' AND trace_state='' AND trace_flags=0
              AND dropped_attributes_count=0 AND dropped_events_count=0
              AND dropped_links_count=0 AND resource_schema_url=''
              AND scope_schema_url='' AND resource_dropped_attributes_count=0
              AND scope_dropped_attributes_count=0",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        2,
        "v0 and v1 batches receive exact v2 defaults"
    );
    assert_eq!(
        conn.query_row(
            "SELECT trace_flags FROM traces WHERE name='direct'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        i64::from(u32::MAX)
    );

    let before: i64 = conn
        .query_row("SELECT COUNT(*) FROM traces", [], |row| row.get(0))
        .unwrap();
    let mut malformed = rich_batch(&[RichSpan::fixture()]);
    malformed.pop();
    assert!(conn
        .execute("INSERT INTO traces(traces) VALUES (?1)", [malformed])
        .is_err());
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM traces", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        before,
        "malformed v2 batch is all-or-nothing"
    );
    conn.execute_batch(
        "BEGIN;
         INSERT INTO traces(trace_id,span_id,name,service,start_ts,trace_flags)
           VALUES('cccccccccccccccccccccccccccccccc','dddddddddddddddd','rollback','svc',30,9);
         ROLLBACK;",
    )
    .unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM traces WHERE name='rollback'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        0
    );
    assert!(conn
        .execute_batch(
            "INSERT INTO traces(trace_id,span_id,name,service,start_ts,trace_flags)
               VALUES('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','ffffffffffffffff','bad','svc',40,-1);"
        )
        .is_err());
    conn.execute_batch("INSERT INTO traces(traces) VALUES ('optimize');")
        .unwrap();
    drop(conn);

    let reopened = Connection::open(&database).unwrap();
    load_extension(&reopened, &extension);
    assert_eq!(
        reopened
            .query_row(
                "SELECT trace_state,trace_flags,dropped_attributes_count,
                        resource_schema_url,scope_dropped_attributes_count
                   FROM traces WHERE name='direct'",
                [],
                |row| Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?
                )),
            )
            .unwrap(),
        (
            "vendor=direct".into(),
            i64::from(u32::MAX),
            4,
            "resource-schema".into(),
            8
        )
    );
}

#[tokio::test]
async fn release_backup_preserves_rich_spans_and_is_no_clobber() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let backup = directory.path().join("backups/backup-traces.db");
    let storage = Storage::start(
        directory.path().join("traces.db"),
        extension.clone(),
        1,
        8,
        Some(DEFAULT_RETENTION),
    )
    .unwrap();
    let spans = vec![RichSpan::fixture()];
    storage
        .submit_batch(rich_batch(&spans), spans.len(), 1_234)
        .await
        .unwrap();

    let report = storage.backup(backup.clone()).await.unwrap();
    assert_eq!(report.signal, "traces");
    assert_eq!(report.schema_version, 1);
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
    assert!(live_stats.sqlite_index_bytes > 0);

    assert_fixture_persisted(&backup, &extension);
    let restored = Storage::start(backup, extension, 1, 8, Some(DEFAULT_RETENTION)).unwrap();
    let restored_stats = restored.stats().await.unwrap();
    assert_eq!(restored_stats.total_spans, 1);
    assert!(restored_stats.sqlite_index_bytes > 0);
    restored.shutdown().await.unwrap();
    storage.shutdown().await.unwrap();
}

#[test]
fn future_traces_schema_fails_before_vtab_initialization() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("future-traces.db");
    let conn = Connection::open(&database).unwrap();
    conn.execute_batch(
        "CREATE TABLE _timeless_schema_migrations(
           signal TEXT NOT NULL, version INTEGER NOT NULL,
           applied_at_unix INTEGER NOT NULL, server_version TEXT NOT NULL,
           extension_version TEXT NOT NULL, extension_data_abi INTEGER NOT NULL,
           PRIMARY KEY(signal, version));
         INSERT INTO _timeless_schema_migrations VALUES
           ('traces',999,0,'future','future',1);",
    )
    .unwrap();
    drop(conn);
    let error = match Storage::start(database.clone(), extension, 1, 1, Some(DEFAULT_RETENTION)) {
        Ok(_) => panic!("future traces database unexpectedly opened"),
        Err(error) => error,
    };
    assert!(error.contains("supports at most 1"), "{error}");
    let conn = Connection::open(database).unwrap();
    let created: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name='traces'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(created, 0, "downgrade refusal must precede vtab creation");
}

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

    // The production binary inherits the migration-persisted policy unless
    // the operator explicitly configured a retention override.
    let inherited = Storage::start_with_retention_policy(
        database.clone(),
        extension.clone(),
        1,
        2,
        None,
        false,
    )
    .unwrap();
    assert_eq!(
        inherited.stats().await.unwrap().retention_nanoseconds,
        Some(DEFAULT_RETENTION.as_nanos() as i64)
    );
    inherited.shutdown().await.unwrap();

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
async fn bytes_gate_blocks_admission_when_queue_bytes_are_exhausted() {
    // M5 regression: the queue is bounded by PAYLOAD bytes, not just
    // batch count. A 64 KiB gate fills with one large in-flight batch;
    // further admissions must block on the gate even though channel
    // slots remain free. The oversized-batch clamp lets the first batch
    // through at all.
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let storage = Storage::start_with_queue_bytes_and_retention_policy(
        directory.path().join("gate.db"),
        extension,
        1,
        8,
        None,
        false,
        64 * 1024,
    )
    .unwrap();

    let blocker = Connection::open(directory.path().join("gate.db")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    let spans = minimal_spans(8_192);
    let blob = rich_batch(&spans);
    let (span_count, body_bytes) = (spans.len(), blob.len());
    assert!(blob.len() > 64 * 1024, "fixture must exceed the gate");
    let first = tokio::spawn({
        let storage = storage.clone();
        async move { storage.submit_batch(blob, span_count, body_bytes).await }
    });
    wait_for_in_flight(&storage, 1).await;

    let one = vec![RichSpan::minimal(9_000)];
    let one_batch = rich_batch(&one);
    let mut third = Box::pin(storage.submit_batch(one_batch.clone(), 1, one_batch.len()));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut third)
            .await
            .is_err(),
        "a full bytes gate must block further admissions"
    );
    drop(third);

    blocker.execute_batch("COMMIT").unwrap();
    first.await.unwrap().unwrap();
    storage.shutdown().await.unwrap();
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
    // submit_batch now waits for the writer to APPLY the batch (issue #47
    // M5: the bytes gate must span the queue). The writer dequeues and
    // blocks on the held write lock, so the spawned future stays pending
    // until COMMIT below.
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
    // The held batch applied once the lock was released; its admission
    // reply carried the result back to the spawned task.
    first.await.unwrap().expect("first batch must apply after commit");
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

async fn get_text(app: &axum::Router, uri: &str) -> String {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// The single sample line `name value` for an exposition family.
fn sample(text: &str, name: &str) -> i64 {
    text.lines()
        .find_map(|line| {
            line.strip_prefix(name)
                .and_then(|rest| rest.strip_prefix(' '))
        })
        .unwrap_or_else(|| panic!("missing sample for {name} in:\n{text}"))
        .parse()
        .unwrap()
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
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../..")
                .join("target/debug/libtimeless_ext.so")
        });
    assert!(
        path.is_file(),
        "build timeless_ext or set TIMELESS_EXT_PATH; missing {}",
        path.display()
    );
    path
}

fn load_extension(conn: &Connection, extension: &Path) {
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(extension, None::<&str>).unwrap();
    }
    conn.load_extension_disable().unwrap();
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
    let fidelity = conn
        .query_row(
            "SELECT links,trace_state,trace_flags,dropped_attributes_count,
                    dropped_events_count,dropped_links_count,resource_schema_url,
                    scope_schema_url,resource_dropped_attributes_count,
                    scope_dropped_attributes_count FROM traces",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&fidelity.0).unwrap()[0]["trace_id"],
        "ffeeddccbbaa99887766554433221100"
    );
    assert_eq!(fidelity.1, "vendor=contract");
    assert_eq!(fidelity.2, i64::from(u32::MAX));
    assert_eq!((fidelity.3, fidelity.4, fidelity.5), (3, 4, 5));
    assert_eq!(fidelity.6, "https://example.test/resource/1");
    assert_eq!(fidelity.7, "https://example.test/scope/2");
    assert_eq!((fidelity.8, fidelity.9), (6, 7));
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
    links: String,
    trace_state: String,
    trace_flags: u32,
    dropped_attributes_count: u32,
    dropped_events_count: u32,
    dropped_links_count: u32,
    resource_schema_url: String,
    scope_schema_url: String,
    resource_dropped_attributes_count: u32,
    scope_dropped_attributes_count: u32,
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
            events: r#"[{"attributes":{"handled":false},"dropped_attributes_count":8,"name":"exception","timestamp":1700000000040000000}]"#.into(),
            resource: r#"{"replica":7,"service.name":"contract-svc"}"#.into(),
            scope: r#"{"name":"contract-lib","version":"4.5.6"}"#.into(),
            links: r#"[{"attributes":{"reason":"retry"},"dropped_attributes_count":9,"flags":257,"span_id":"8877665544332211","trace_id":"ffeeddccbbaa99887766554433221100","trace_state":"link=state"}]"#.into(),
            trace_state: "vendor=contract".into(),
            trace_flags: u32::MAX,
            dropped_attributes_count: 3,
            dropped_events_count: 4,
            dropped_links_count: 5,
            resource_schema_url: "https://example.test/resource/1".into(),
            scope_schema_url: "https://example.test/scope/2".into(),
            resource_dropped_attributes_count: 6,
            scope_dropped_attributes_count: 7,
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
            links: "[]".into(),
            trace_state: String::new(),
            trace_flags: 0,
            dropped_attributes_count: 0,
            dropped_events_count: 0,
            dropped_links_count: 0,
            resource_schema_url: String::new(),
            scope_schema_url: String::new(),
            resource_dropped_attributes_count: 0,
            scope_dropped_attributes_count: 0,
        }
    }
}

fn minimal_spans(count: usize) -> Vec<RichSpan> {
    (1..=count)
        .map(|number| RichSpan::minimal(number as u64))
        .collect()
}

fn rich_batch(spans: &[RichSpan]) -> Vec<u8> {
    let mut out = vec![0x03, 0, 0, 0];
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
    text_column(&mut out, spans.iter().map(|span| span.links.as_str()));
    text_column(&mut out, spans.iter().map(|span| span.trace_state.as_str()));
    u32_column(&mut out, spans.iter().map(|span| span.trace_flags));
    u32_column(
        &mut out,
        spans.iter().map(|span| span.dropped_attributes_count),
    );
    u32_column(&mut out, spans.iter().map(|span| span.dropped_events_count));
    u32_column(&mut out, spans.iter().map(|span| span.dropped_links_count));
    text_column(
        &mut out,
        spans.iter().map(|span| span.resource_schema_url.as_str()),
    );
    text_column(
        &mut out,
        spans.iter().map(|span| span.scope_schema_url.as_str()),
    );
    u32_column(
        &mut out,
        spans
            .iter()
            .map(|span| span.resource_dropped_attributes_count),
    );
    u32_column(
        &mut out,
        spans.iter().map(|span| span.scope_dropped_attributes_count),
    );
    out
}

fn legacy_batch(spans: &[RichSpan], version: u8) -> Vec<u8> {
    assert!(matches!(version, 0x01 | 0x02));
    let mut out = vec![version, 0, 0, 0];
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
    if version == 0x02 {
        text_column(
            &mut out,
            spans.iter().map(|span| span.status_description.as_str()),
        );
        text_column(&mut out, spans.iter().map(|span| span.events.as_str()));
        text_column(&mut out, spans.iter().map(|span| span.resource.as_str()));
        text_column(&mut out, spans.iter().map(|span| span.scope.as_str()));
    }
    out
}

fn text_column<'a>(out: &mut Vec<u8>, values: impl Iterator<Item = &'a str>) {
    for value in values {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }
}

fn u32_column(out: &mut Vec<u8>, values: impl Iterator<Item = u32>) {
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
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
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
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

#[tokio::test]
#[ignore = "requires a built timeless_ext shared library"]
async fn self_metrics_exposes_prometheus_families() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let storage = Storage::start(
        directory.path().join("traces.db"),
        extension,
        1,
        8,
        Some(DEFAULT_RETENTION),
    )
    .unwrap();
    let app = router(storage.clone());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap(),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("timeless_build_info{name=\"timeless-traces-api\",version=\""),
        "missing build info: {text}"
    );
    for family in [
        "# TYPE timeless_traces_admitted_spans_total counter",
        "# TYPE timeless_traces_rejected_requests_total counter",
        "# TYPE timeless_traces_admitted_bytes_total counter",
        "# TYPE timeless_traces_spans gauge",
        "# TYPE timeless_traces_disk_size_bytes gauge",
        "# TYPE timeless_traces_storage_bytes gauge",
        "# TYPE timeless_traces_index_bytes gauge",
        "# TYPE timeless_traces_wal_bytes gauge",
        "# TYPE timeless_traces_compression_input_bytes_total counter",
        "# TYPE timeless_traces_compression_output_bytes_total counter",
        "# TYPE timeless_traces_raw_ingested_bytes_total counter",
        "# TYPE timeless_traces_oldest_queued_ms gauge",
    ] {
        assert!(text.contains(family), "missing {family} in:\n{text}");
    }
    storage.shutdown().await.unwrap();
}

/// COMPRESSION_REPORTING_PLAN Phase 2: the exposition's storage series
/// must reconcile exactly with the engine's `timeless_stats` on the same
/// database, and the compression ratio inputs never blend index, WAL,
/// freelist, or whole-file bytes.
#[tokio::test]
async fn metrics_exposition_reconciles_storage_series_with_timeless_stats() {
    let extension = required_extension();
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("honest-storage.db");
    let storage = Storage::start(
        database.clone(),
        extension.clone(),
        1,
        8,
        Some(DEFAULT_RETENTION),
    )
    .unwrap();
    let app = router(storage.clone());

    let spans = minimal_spans(4_096);
    let batch = rich_batch(&spans);
    storage
        .submit_batch(batch.clone(), spans.len(), batch.len())
        .await
        .unwrap();
    storage.flush().await.unwrap();
    storage.schedule_optimize().await.unwrap();

    let text = get_text(&app, "/metrics").await;
    let storage_bytes = sample(&text, "timeless_traces_storage_bytes");
    let index_bytes = sample(&text, "timeless_traces_index_bytes");
    let wal_bytes = sample(&text, "timeless_traces_wal_bytes");
    let compression_input = sample(&text, "timeless_traces_compression_input_bytes_total");
    let compression_output = sample(&text, "timeless_traces_compression_output_bytes_total");
    let raw_ingested = sample(&text, "timeless_traces_raw_ingested_bytes_total");
    assert!(storage_bytes > 0, "optimize left no block payload bytes");
    assert!(index_bytes > 0, "trace shadow indexes must have bytes");
    assert!(wal_bytes >= 0);
    assert!(
        compression_input > 0 && compression_output > 0,
        "optimize must persist first-pass compression totals"
    );
    // Raw ingested is the engine's persisted logical-span-bytes counter;
    // all seeded spans are on disk here, so it is positive, and it must
    // reconcile exactly with the engine at the bottom of this test.
    assert!(raw_ingested > 0, "raw_ingested={raw_ingested}");

    let stats = get_json(&app, "/select/traces/stats").await;
    assert_eq!(stats.0, StatusCode::OK);
    assert_eq!(stats.1["bytes_on_disk"].as_i64().unwrap(), storage_bytes);
    assert_eq!(stats.1["sqlite_index_bytes"].as_i64().unwrap(), index_bytes);
    assert_eq!(
        stats.1["extension_compression_input_bytes_total"]
            .as_i64()
            .unwrap(),
        compression_input
    );
    assert_eq!(
        stats.1["extension_compression_output_bytes_total"]
            .as_i64()
            .unwrap(),
        compression_output
    );
    assert_eq!(
        stats.1["raw_ingested_bytes_total"].as_i64().unwrap(),
        raw_ingested
    );
    assert!(stats.1["database_wal_bytes"].as_i64().is_some());

    drop(app);
    storage.shutdown().await.unwrap();
    drop(storage);

    // Ground truth: the engine's own timeless_stats on the same database.
    let conn = Connection::open(&database).unwrap();
    load_extension(&conn, &extension);
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM traces", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 4_096);
    let engine = |key: &str| -> i64 {
        conn.query_row(
            "SELECT value FROM timeless_stats('traces') WHERE key=?1",
            [key],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(engine("bytes_on_disk"), storage_bytes);
    assert_eq!(engine("index_bytes"), index_bytes);
    assert_eq!(engine("compression_input_bytes_total"), compression_input);
    assert_eq!(engine("compression_output_bytes_total"), compression_output);
    assert_eq!(engine("ingest_raw_bytes_total"), raw_ingested);
}
