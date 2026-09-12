//! Issue #53: one bounded summary span per scheduled traces optimize
//! sweep, and the recursion guard: a traces server exporting into its own
//! OTLP ingest stores exactly the sweep spans and nothing about ingesting
//! them.

use std::path::PathBuf;

use tempfile::TempDir;
use timeless_traces_api::{router, OtelExportState, OtelTelemetry, OtelTracesConfig, Storage};

fn extension_path() -> PathBuf {
    std::env::var_os("TIMELESS_EXT_TEST_PATH")
        .or_else(|| std::env::var_os("TIMELESS_EXT_PATH"))
        .map(PathBuf::from)
        .expect("set TIMELESS_EXT_TEST_PATH to the built timeless_ext shared library")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn self_export_stores_sweep_spans_without_recursing() {
    let extension = extension_path();
    let directory = TempDir::new().unwrap();
    let storage =
        Storage::start(directory.path().join("traces.db"), extension, 1, 8, None).unwrap();
    assert_eq!(
        storage.stats().await.unwrap().otel_traces.state,
        OtelExportState::Disabled
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/insert/opentelemetry/v1/traces",
        listener.local_addr().unwrap()
    );
    let app = router(storage.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    let config = OtelTracesConfig {
        endpoint: Some(endpoint),
        ..OtelTracesConfig::default()
    };
    let telemetry =
        tokio::task::spawn_blocking(move || OtelTelemetry::initialize(&config, "traces"))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    storage.attach_otel_health(telemetry.health());
    let maintenance = telemetry.maintenance("traces");
    for _ in 0..3 {
        storage
            .schedule_optimize_with_telemetry(Some(&maintenance))
            .await
            .unwrap();
    }
    // Flushing delivers the three sweep spans into this same store through
    // the public OTLP route. That ingest, and the writer work it causes, is
    // not instrumented: if it were, the spans it produced would show up as
    // enqueued (while the exporter was live) or dropped (after shutdown).
    tokio::task::spawn_blocking(move || telemetry.shutdown())
        .await
        .unwrap()
        .unwrap();
    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.admitted_spans, 3, "{stats:?}");
    assert_eq!(stats.completed_spans, 3, "{stats:?}");
    assert_eq!(stats.failed_spans, 0, "{stats:?}");
    let otel = stats.otel_traces.clone();
    assert_eq!(otel.state, OtelExportState::Healthy, "{otel:?}");
    assert_eq!(otel.enqueued_spans, 3, "{otel:?}");
    assert_eq!(otel.exported_spans, 3, "{otel:?}");
    assert_eq!(otel.dropped_spans, 0, "{otel:?}");

    // Later untraced sweeps (which now optimize the stored sweep spans)
    // add nothing either.
    for _ in 0..2 {
        storage.schedule_optimize().await.unwrap();
    }
    let after = storage.stats().await.unwrap();
    assert_eq!(after.admitted_spans, 3, "{after:?}");
    assert_eq!(after.otel_traces.enqueued_spans, 3, "{after:?}");
    assert_eq!(after.otel_traces.dropped_spans, 0, "{after:?}");

    server.abort();
    let _ = server.await;
    storage.shutdown().await.unwrap();
}
