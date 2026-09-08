//! Narrow first-party OpenTelemetry export for metrics maintenance.
//!
//! This initial slice intentionally emits only one span per scheduled
//! compact/rollup sweep, plus events for unusually slow committed steps. It
//! does not trace HTTP requests, individual chunks, samples, or the exporter
//! itself. The bounded batch processor drops spans instead of backpressuring
//! the data plane when its queue is full.

use std::time::Duration;

use opentelemetry::trace::{Span as _, SpanKind, Status, Tracer as _, TracerProvider as _};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider, Span};
use opentelemetry_sdk::Resource;

const EXPORT_TIMEOUT: Duration = Duration::from_secs(2);
const EXPORT_DELAY: Duration = Duration::from_secs(1);
const MAX_QUEUE_SPANS: usize = 256;
const MAX_EXPORT_BATCH_SPANS: usize = 64;
const SLOW_STEP: Duration = Duration::from_millis(50);

/// Owns the exporter lifecycle. The provider is not installed globally: only
/// the metrics compaction scheduler receives its tracer in this first slice.
pub(crate) struct Telemetry {
    provider: SdkTracerProvider,
    compaction: CompactionTelemetry,
}

impl Telemetry {
    pub(crate) fn initialize(endpoint: Option<&str>) -> Result<Option<Self>, String> {
        validate_endpoint(endpoint)?;
        let Some(endpoint) = endpoint else {
            return Ok(None);
        };
        let endpoint = endpoint.trim();
        if endpoint.is_empty() {
            return Err("metrics OTel traces endpoint must not be empty".into());
        }

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(endpoint)
            .with_timeout(EXPORT_TIMEOUT)
            .build()
            .map_err(|error| format!("initialize metrics OTLP/HTTP exporter: {error}"))?;
        let processor = BatchSpanProcessor::builder(exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(MAX_QUEUE_SPANS)
                    .with_max_export_batch_size(MAX_EXPORT_BATCH_SPANS)
                    .with_scheduled_delay(EXPORT_DELAY)
                    .build(),
            )
            .build();
        let identity = timeless_api_common::server_build_identity("metrics");
        let identity_string = |key: &str| {
            identity
                .get(key)
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned()
        };
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .with_resource(
                Resource::builder()
                    .with_service_name("timeless-metrics-api")
                    .with_attributes([
                        KeyValue::new("service.version", identity_string("version")),
                        KeyValue::new("timeless.build.commit", identity_string("commit")),
                        KeyValue::new("timeless.build.target", identity_string("target")),
                        KeyValue::new("timeless.build.profile", identity_string("profile")),
                    ])
                    .build(),
            )
            .build();
        let tracer = provider.tracer("timeless-metrics-api");
        Ok(Some(Self {
            provider,
            compaction: CompactionTelemetry { tracer },
        }))
    }

    pub(crate) fn compaction(&self) -> CompactionTelemetry {
        self.compaction.clone()
    }

    pub(crate) fn shutdown(self) -> Result<(), String> {
        self.provider
            .shutdown_with_timeout(EXPORT_TIMEOUT)
            .map_err(|error| format!("flush metrics OpenTelemetry traces: {error}"))
    }
}

pub(crate) fn validate_endpoint(endpoint: Option<&str>) -> Result<(), String> {
    let Some(endpoint) = endpoint else {
        return Ok(());
    };
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err("metrics OTel traces endpoint must not be empty".into());
    }
    let parsed = reqwest::Url::parse(endpoint)
        .map_err(|error| format!("invalid metrics OTel traces endpoint {endpoint:?}: {error}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err("metrics OTel traces endpoint must use http or https".into());
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("metrics OTel traces endpoint must not contain credentials".into());
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct CompactionTelemetry {
    tracer: opentelemetry_sdk::trace::SdkTracer,
}

impl CompactionTelemetry {
    pub(crate) fn start(
        &self,
        raw_series_budget: usize,
        rollup_group_budget: usize,
        reader_admission_pause: Duration,
    ) -> CompactionTrace {
        let mut span = self
            .tracer
            .span_builder("timeless.metrics.compaction")
            .with_kind(SpanKind::Internal)
            .start(&self.tracer);
        span.set_attributes([
            KeyValue::new("timeless.signal", "metrics"),
            KeyValue::new("timeless.maintenance.operation", "compact_rollup"),
            KeyValue::new(
                "timeless.compaction.raw_series_budget",
                bounded_i64(raw_series_budget as u64),
            ),
            KeyValue::new(
                "timeless.compaction.rollup_group_budget",
                bounded_i64(rollup_group_budget as u64),
            ),
            KeyValue::new(
                "timeless.compaction.reader_admission_pause_ms",
                bounded_i64(reader_admission_pause.as_millis() as u64),
            ),
        ]);
        CompactionTrace {
            span,
            slow_steps: 0,
        }
    }
}

pub(crate) struct CompactionTrace {
    span: Span,
    slow_steps: u64,
}

impl CompactionTrace {
    pub(crate) fn record_step(&mut self, step: u64, elapsed_ns: u64, continues: bool) {
        if elapsed_ns < duration_ns(SLOW_STEP) {
            return;
        }
        self.slow_steps = self.slow_steps.saturating_add(1);
        self.span.add_event(
            "timeless.metrics.compaction.slow_step",
            vec![
                KeyValue::new("timeless.compaction.step", bounded_i64(step)),
                KeyValue::new(
                    "timeless.compaction.step.elapsed_ns",
                    bounded_i64(elapsed_ns),
                ),
                KeyValue::new("timeless.compaction.step.continues", continues),
            ],
        );
    }

    pub(crate) fn finish(
        mut self,
        steps: u64,
        total_ns: u64,
        max_step_ns: u64,
        read_retries: u64,
        result: &Result<(), String>,
    ) {
        self.span.set_attributes([
            KeyValue::new("timeless.compaction.steps", bounded_i64(steps)),
            KeyValue::new("timeless.compaction.step_total_ns", bounded_i64(total_ns)),
            KeyValue::new("timeless.compaction.max_step_ns", bounded_i64(max_step_ns)),
            KeyValue::new(
                "timeless.compaction.slow_steps",
                bounded_i64(self.slow_steps),
            ),
            KeyValue::new(
                "timeless.compaction.api_read_retries",
                bounded_i64(read_retries),
            ),
        ]);
        match result {
            Ok(()) => {
                self.span
                    .set_attribute(KeyValue::new("timeless.compaction.result", "ok"));
                self.span.set_status(Status::Ok);
            }
            Err(error) => {
                self.span
                    .set_attribute(KeyValue::new("timeless.compaction.result", "error"));
                self.span
                    .set_attribute(KeyValue::new("error.message", error.clone()));
                self.span.set_status(Status::error(error.clone()));
            }
        }
        self.span.end();
    }
}

fn bounded_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_sdk::trace::InMemorySpanExporter;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[test]
    fn optional_endpoint_is_disabled_by_default_and_requires_http() {
        assert_eq!(validate_endpoint(None), Ok(()));
        assert!(validate_endpoint(Some("http://127.0.0.1:19449/v1/traces")).is_ok());
        assert_eq!(
            validate_endpoint(Some("file:///tmp/traces")).unwrap_err(),
            "metrics OTel traces endpoint must use http or https"
        );
        assert_eq!(
            validate_endpoint(Some("http://secret@example.test/v1/traces")).unwrap_err(),
            "metrics OTel traces endpoint must not contain credentials"
        );
    }

    #[test]
    fn compaction_trace_is_one_summary_span_with_only_slow_step_events() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let telemetry = CompactionTelemetry {
            tracer: provider.tracer("test"),
        };

        let mut trace = telemetry.start(64, 64, Duration::from_millis(10));
        trace.record_step(1, duration_ns(Duration::from_millis(49)), true);
        trace.record_step(2, duration_ns(Duration::from_millis(50)), true);
        trace.finish(
            3,
            duration_ns(Duration::from_millis(80)),
            duration_ns(Duration::from_millis(50)),
            7,
            &Ok(()),
        );

        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "timeless.metrics.compaction");
        assert_eq!(spans[0].events.len(), 1);
        assert_eq!(
            spans[0].events[0].name,
            "timeless.metrics.compaction.slow_step"
        );
        assert!(spans[0]
            .attributes
            .contains(&KeyValue::new("timeless.compaction.steps", 3_i64)));
        assert!(spans[0].attributes.contains(&KeyValue::new(
            "timeless.compaction.api_read_retries",
            7_i64
        )));
        assert_eq!(spans[0].status, Status::Ok);
    }

    #[test]
    fn compaction_trace_records_error_status_without_panicking() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let telemetry = CompactionTelemetry {
            tracer: provider.tracer("test"),
        };

        telemetry.start(64, 64, Duration::from_millis(10)).finish(
            1,
            10,
            10,
            0,
            &Err("writer unavailable".into()),
        );

        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert!(matches!(spans[0].status, Status::Error { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires a built timeless_ext shared library"]
    async fn scheduled_compaction_span_round_trips_into_timeless_traces() {
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

        let metrics = crate::Storage::start(
            directory.path().join("metrics.db"),
            extension,
            1,
            8,
            crate::DEFAULT_RAW_RETENTION,
        )
        .unwrap();
        let telemetry = Telemetry::initialize(Some(&endpoint)).unwrap().unwrap();
        let compaction = telemetry.compaction();
        metrics
            .schedule_compact_with_telemetry(Some(&compaction))
            .await
            .unwrap();
        tokio::task::spawn_blocking(move || telemetry.shutdown())
            .await
            .unwrap()
            .unwrap();

        let stats = traces.stats().await.unwrap();
        assert_eq!(stats.admitted_spans, 1, "{stats:?}");
        assert_eq!(stats.completed_spans, 1, "{stats:?}");
        assert_eq!(stats.failed_spans, 0, "{stats:?}");

        metrics.shutdown().await.unwrap();
        server.abort();
        let _ = server.await;
        traces.shutdown().await.unwrap();
    }

    fn extension_path() -> PathBuf {
        std::env::var_os("TIMELESS_EXT_TEST_PATH")
            .or_else(|| std::env::var_os("TIMELESS_EXT_PATH"))
            .map(PathBuf::from)
            .expect("set TIMELESS_EXT_TEST_PATH to the built timeless_ext shared library")
    }
}
