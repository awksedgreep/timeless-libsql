//! Issue #53: one bounded summary span per scheduled logs optimize sweep,
//! exported through the shared bounded exporter into a real
//! `timeless-traces-api`, with health visible in `stats()`.

use std::path::PathBuf;
use std::time::Duration;

use tempfile::TempDir;
use timeless_logs_api::{OtelExportState, OtelTelemetry, OtelTracesConfig, Storage, TimestampUnit};

fn extension_path() -> PathBuf {
    std::env::var_os("TIMELESS_EXT_TEST_PATH")
        .or_else(|| std::env::var_os("TIMELESS_EXT_PATH"))
        .map(PathBuf::from)
        .expect("set TIMELESS_EXT_TEST_PATH to the built timeless_ext shared library")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TIMELESS_EXT_TEST_PATH pointing at libtimeless_ext"]
async fn scheduled_optimize_spans_round_trip_into_timeless_traces() {
    let extension = extension_path();
    let directory = TempDir::new().unwrap();
    let traces = timeless_traces_api::Storage::start(
        directory.path().join("self-traces.db"),
        extension.clone(),
        1,
        8,
        Some(Duration::from_secs(24 * 60 * 60)),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/insert/opentelemetry/v1/traces",
        listener.local_addr().unwrap()
    );
    let traces_app = timeless_traces_api::router(traces.clone());
    let server = tokio::spawn(async move { axum::serve(listener, traces_app).await });

    let logs = Storage::start_with_timestamp_unit(
        directory.path().join("logs.db"),
        extension,
        1,
        8,
        TimestampUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        logs.stats().await.unwrap().otel_traces.state,
        OtelExportState::Disabled
    );
    let config = OtelTracesConfig {
        endpoint: Some(endpoint),
        ..OtelTracesConfig::default()
    };
    let telemetry = tokio::task::spawn_blocking(move || OtelTelemetry::initialize(&config, "logs"))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    logs.attach_otel_health(telemetry.health());
    let maintenance = telemetry.maintenance("logs");
    for _ in 0..3 {
        logs.schedule_optimize_with_telemetry(Some(&maintenance))
            .await
            .unwrap();
    }
    // Untraced sweeps emit nothing.
    logs.schedule_optimize().await.unwrap();
    tokio::task::spawn_blocking(move || telemetry.shutdown())
        .await
        .unwrap()
        .unwrap();

    let received = traces.stats().await.unwrap();
    assert_eq!(received.admitted_spans, 3, "{received:?}");
    assert_eq!(received.completed_spans, 3, "{received:?}");
    assert_eq!(received.failed_spans, 0, "{received:?}");
    let otel = logs.stats().await.unwrap().otel_traces;
    assert_eq!(otel.state, OtelExportState::Healthy, "{otel:?}");
    assert_eq!(otel.enqueued_spans, 3, "{otel:?}");
    assert_eq!(otel.exported_spans, 3, "{otel:?}");
    assert_eq!(otel.dropped_spans, 0, "{otel:?}");

    logs.shutdown().await.unwrap();
    server.abort();
    let _ = server.await;
    traces.shutdown().await.unwrap();
}
