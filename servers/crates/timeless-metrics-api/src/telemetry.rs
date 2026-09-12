//! Metrics-specific OpenTelemetry vocabulary: one span per scheduled
//! compact/rollup sweep on the shared bounded exporter
//! (`timeless_api_common::otel`). Nothing per request, sample, or chunk.

use std::time::Duration;

use opentelemetry_sdk::trace::SdkTracer;
use timeless_api_common::otel::{bounded_i64, KeyValue, MaintenanceTelemetry, SweepTrace};

#[derive(Clone)]
pub(crate) struct CompactionTelemetry {
    maintenance: MaintenanceTelemetry,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CompactionWork {
    pub(crate) raw_steps: u64,
    pub(crate) raw_chunks: u64,
    pub(crate) raw_points: u64,
    pub(crate) raw_input_bytes: u64,
    pub(crate) raw_output_bytes: u64,
    pub(crate) raw_total_ns: u64,
    pub(crate) merge_steps: u64,
    pub(crate) merge_chunks: u64,
    pub(crate) merge_points: u64,
    pub(crate) merge_input_bytes: u64,
    pub(crate) merge_output_bytes: u64,
    pub(crate) merge_total_ns: u64,
}

impl CompactionWork {
    pub(crate) fn delta_from(self, before: Self) -> Self {
        Self {
            raw_steps: self.raw_steps.saturating_sub(before.raw_steps),
            raw_chunks: self.raw_chunks.saturating_sub(before.raw_chunks),
            raw_points: self.raw_points.saturating_sub(before.raw_points),
            raw_input_bytes: self.raw_input_bytes.saturating_sub(before.raw_input_bytes),
            raw_output_bytes: self
                .raw_output_bytes
                .saturating_sub(before.raw_output_bytes),
            raw_total_ns: self.raw_total_ns.saturating_sub(before.raw_total_ns),
            merge_steps: self.merge_steps.saturating_sub(before.merge_steps),
            merge_chunks: self.merge_chunks.saturating_sub(before.merge_chunks),
            merge_points: self.merge_points.saturating_sub(before.merge_points),
            merge_input_bytes: self
                .merge_input_bytes
                .saturating_sub(before.merge_input_bytes),
            merge_output_bytes: self
                .merge_output_bytes
                .saturating_sub(before.merge_output_bytes),
            merge_total_ns: self.merge_total_ns.saturating_sub(before.merge_total_ns),
        }
    }

    pub(crate) fn add(&mut self, step: Self) {
        self.raw_steps = self.raw_steps.saturating_add(step.raw_steps);
        self.raw_chunks = self.raw_chunks.saturating_add(step.raw_chunks);
        self.raw_points = self.raw_points.saturating_add(step.raw_points);
        self.raw_input_bytes = self.raw_input_bytes.saturating_add(step.raw_input_bytes);
        self.raw_output_bytes = self.raw_output_bytes.saturating_add(step.raw_output_bytes);
        self.raw_total_ns = self.raw_total_ns.saturating_add(step.raw_total_ns);
        self.merge_steps = self.merge_steps.saturating_add(step.merge_steps);
        self.merge_chunks = self.merge_chunks.saturating_add(step.merge_chunks);
        self.merge_points = self.merge_points.saturating_add(step.merge_points);
        self.merge_input_bytes = self
            .merge_input_bytes
            .saturating_add(step.merge_input_bytes);
        self.merge_output_bytes = self
            .merge_output_bytes
            .saturating_add(step.merge_output_bytes);
        self.merge_total_ns = self.merge_total_ns.saturating_add(step.merge_total_ns);
    }
}

impl CompactionTelemetry {
    pub(crate) fn new(tracer: SdkTracer) -> Self {
        Self {
            maintenance: MaintenanceTelemetry::new(tracer, "metrics"),
        }
    }

    pub(crate) fn start(
        &self,
        metrics_series_budget: usize,
        metrics_point_budget: usize,
        metrics_byte_budget: u64,
        rollup_group_budget: usize,
        reader_admission_pause: Duration,
    ) -> CompactionTrace {
        CompactionTrace {
            trace: self.maintenance.start(
                "compaction",
                vec![
                    KeyValue::new("timeless.maintenance.operation", "compact_rollup"),
                    KeyValue::new(
                        "timeless.compaction.metrics_series_budget",
                        bounded_i64(metrics_series_budget as u64),
                    ),
                    KeyValue::new(
                        "timeless.compaction.metrics_input_point_budget",
                        bounded_i64(metrics_point_budget as u64),
                    ),
                    KeyValue::new(
                        "timeless.compaction.metrics_input_byte_budget",
                        bounded_i64(metrics_byte_budget),
                    ),
                    KeyValue::new(
                        "timeless.compaction.rollup_group_budget",
                        bounded_i64(rollup_group_budget as u64),
                    ),
                    KeyValue::new(
                        "timeless.compaction.reader_admission_pause_ms",
                        bounded_i64(reader_admission_pause.as_millis() as u64),
                    ),
                ],
            ),
        }
    }
}

pub(crate) struct CompactionTrace {
    trace: SweepTrace,
}

impl CompactionTrace {
    pub(crate) fn record_step(
        &mut self,
        step: u64,
        elapsed_ns: u64,
        continues: bool,
        work: CompactionWork,
    ) {
        self.trace.record_step(
            step,
            elapsed_ns,
            continues,
            vec![
                KeyValue::new(
                    "timeless.compaction.step.raw_input_bytes",
                    bounded_i64(work.raw_input_bytes),
                ),
                KeyValue::new(
                    "timeless.compaction.step.merge_input_bytes",
                    bounded_i64(work.merge_input_bytes),
                ),
            ],
        );
    }

    pub(crate) fn finish(
        self,
        steps: u64,
        total_ns: u64,
        max_step_ns: u64,
        read_retries: u64,
        work: CompactionWork,
        result: &Result<(), String>,
    ) {
        let attributes = vec![
            KeyValue::new("timeless.compaction.steps", bounded_i64(steps)),
            KeyValue::new("timeless.compaction.step_total_ns", bounded_i64(total_ns)),
            KeyValue::new("timeless.compaction.max_step_ns", bounded_i64(max_step_ns)),
            KeyValue::new(
                "timeless.compaction.api_read_retries",
                bounded_i64(read_retries),
            ),
            KeyValue::new("timeless.compaction.raw.steps", bounded_i64(work.raw_steps)),
            KeyValue::new(
                "timeless.compaction.raw.chunks",
                bounded_i64(work.raw_chunks),
            ),
            KeyValue::new(
                "timeless.compaction.raw.points",
                bounded_i64(work.raw_points),
            ),
            KeyValue::new(
                "timeless.compaction.raw.input_bytes",
                bounded_i64(work.raw_input_bytes),
            ),
            KeyValue::new(
                "timeless.compaction.raw.output_bytes",
                bounded_i64(work.raw_output_bytes),
            ),
            KeyValue::new(
                "timeless.compaction.raw.elapsed_ns",
                bounded_i64(work.raw_total_ns),
            ),
            KeyValue::new(
                "timeless.compaction.merge.steps",
                bounded_i64(work.merge_steps),
            ),
            KeyValue::new(
                "timeless.compaction.merge.chunks",
                bounded_i64(work.merge_chunks),
            ),
            KeyValue::new(
                "timeless.compaction.merge.points",
                bounded_i64(work.merge_points),
            ),
            KeyValue::new(
                "timeless.compaction.merge.input_bytes",
                bounded_i64(work.merge_input_bytes),
            ),
            KeyValue::new(
                "timeless.compaction.merge.output_bytes",
                bounded_i64(work.merge_output_bytes),
            ),
            KeyValue::new(
                "timeless.compaction.merge.elapsed_ns",
                bounded_i64(work.merge_total_ns),
            ),
        ];
        self.trace.finish(attributes, result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{Status, TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use std::path::PathBuf;
    use std::time::Instant;
    use tempfile::TempDir;
    use timeless_api_common::otel::{
        duration_ns, OtelExportState, OtelTelemetry, OtelTracesConfig,
    };

    fn config(endpoint: &str) -> OtelTracesConfig {
        OtelTracesConfig {
            endpoint: Some(endpoint.into()),
            ..OtelTracesConfig::default()
        }
    }

    #[test]
    fn compaction_trace_is_one_summary_span_with_only_slow_step_events() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let telemetry = CompactionTelemetry::new(provider.tracer("test"));

        let mut trace = telemetry.start(
            64,
            256 * 1024,
            4 * 1024 * 1024,
            64,
            Duration::from_millis(10),
        );
        let work = CompactionWork {
            raw_steps: 1,
            raw_chunks: 2,
            raw_points: 1024,
            raw_input_bytes: 16_384,
            raw_output_bytes: 2048,
            raw_total_ns: 20,
            merge_steps: 1,
            merge_chunks: 2,
            merge_points: 16_384,
            merge_input_bytes: 4096,
            merge_output_bytes: 3072,
            merge_total_ns: 30,
        };
        trace.record_step(
            1,
            duration_ns(Duration::from_millis(49)),
            true,
            CompactionWork::default(),
        );
        trace.record_step(2, duration_ns(Duration::from_millis(50)), true, work);
        trace.finish(
            3,
            duration_ns(Duration::from_millis(80)),
            duration_ns(Duration::from_millis(50)),
            7,
            work,
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
        assert!(spans[0].attributes.contains(&KeyValue::new(
            "timeless.compaction.raw.input_bytes",
            16_384_i64
        )));
        assert!(spans[0].attributes.contains(&KeyValue::new(
            "timeless.compaction.merge.input_bytes",
            4096_i64
        )));
        assert_eq!(spans[0].status, Status::Ok);
    }

    #[test]
    fn compaction_trace_records_error_status_without_panicking() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let telemetry = CompactionTelemetry::new(provider.tracer("test"));

        telemetry
            .start(
                64,
                256 * 1024,
                4 * 1024 * 1024,
                64,
                Duration::from_millis(10),
            )
            .finish(
                1,
                10,
                10,
                0,
                CompactionWork::default(),
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
        let telemetry = OtelTelemetry::initialize(&config(&endpoint), "metrics")
            .unwrap()
            .unwrap();
        metrics.attach_otel_health(telemetry.health());
        let compaction = CompactionTelemetry::new(telemetry.tracer());
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
        let otel = metrics.stats().await.unwrap().otel_traces;
        assert_eq!(otel.state, OtelExportState::Healthy, "{otel:?}");
        assert_eq!(otel.exported_spans, 1, "{otel:?}");

        metrics.shutdown().await.unwrap();
        server.abort();
        let _ = server.await;
        traces.shutdown().await.unwrap();
    }

    /// Issue #55 outage contract against the real storage path: a collector
    /// that refuses connections makes every sweep's export fail, the failure
    /// is visible in `stats().otel_traces`, nothing accumulates beyond the
    /// queue bound, and scheduled compaction keeps completing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires a built timeless_ext shared library"]
    async fn collector_outage_is_visible_in_stats_and_leaves_maintenance_alone() {
        let extension = extension_path();
        let directory = TempDir::new().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
        drop(listener);

        let metrics = crate::Storage::start(
            directory.path().join("metrics.db"),
            extension,
            1,
            8,
            crate::DEFAULT_RAW_RETENTION,
        )
        .unwrap();
        assert_eq!(
            metrics.stats().await.unwrap().otel_traces.state,
            OtelExportState::Disabled
        );
        let config = OtelTracesConfig {
            queue_spans: 4,
            batch_spans: 1,
            export_delay: Duration::from_millis(20),
            export_timeout: Duration::from_millis(500),
            ..config(&endpoint)
        };
        let telemetry =
            tokio::task::spawn_blocking(move || OtelTelemetry::initialize(&config, "metrics"))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        metrics.attach_otel_health(telemetry.health());
        let compaction = CompactionTelemetry::new(telemetry.tracer());
        let mut sweep_costs = Vec::new();
        for _ in 0..8 {
            let started = Instant::now();
            metrics
                .schedule_compact_with_telemetry(Some(&compaction))
                .await
                .unwrap();
            sweep_costs.push(started.elapsed());
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while metrics.stats().await.unwrap().otel_traces.export_attempts < 8
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let stats = metrics.stats().await.unwrap();
        let otel = stats.otel_traces.clone();
        assert_eq!(otel.state, OtelExportState::Failing, "{otel:?}");
        assert!(otel.enabled);
        assert_eq!(otel.enqueued_spans + otel.dropped_spans, 8, "{otel:?}");
        assert!(otel.export_failures >= 1, "{otel:?}");
        assert_eq!(otel.export_successes, 0, "{otel:?}");
        assert!(otel.queue_high_water_spans <= 4, "{otel:?}");
        assert!(otel.last_error.is_some(), "{otel:?}");
        assert_eq!(stats.compact_count, 8, "{stats:?}");
        assert_eq!(stats.compact_errors, 0, "{stats:?}");
        assert!(
            sweep_costs
                .iter()
                .all(|cost| *cost < Duration::from_secs(1)),
            "{sweep_costs:?}"
        );

        let _ = tokio::task::spawn_blocking(move || telemetry.shutdown())
            .await
            .unwrap();
        let drained = metrics.stats().await.unwrap().otel_traces;
        assert_eq!(drained.queued_spans, 0, "{drained:?}");
        metrics.shutdown().await.unwrap();
    }

    fn extension_path() -> PathBuf {
        std::env::var_os("TIMELESS_EXT_TEST_PATH")
            .or_else(|| std::env::var_os("TIMELESS_EXT_PATH"))
            .map(PathBuf::from)
            .expect("set TIMELESS_EXT_TEST_PATH to the built timeless_ext shared library")
    }
}
