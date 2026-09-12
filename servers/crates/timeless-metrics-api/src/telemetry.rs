//! Narrow first-party OpenTelemetry export for metrics maintenance.
//!
//! This slice intentionally emits only one span per scheduled compact/rollup
//! sweep, plus events for unusually slow committed steps. It does not trace
//! HTTP requests, individual chunks, samples, or the exporter itself.
//!
//! Export is best effort and bounded (issue #55):
//!
//! - spans queue in a fixed-capacity buffer owned by [`BoundedBatchProcessor`];
//!   a full queue drops the newest span and counts it, never blocking the
//!   maintenance thread that ended the span;
//! - one worker thread exports batches through the app's own `reqwest`
//!   client, so TLS roots, request headers, and timeouts are configured here
//!   rather than by a second HTTP stack;
//! - every attempt, success, failure, drop, and queue depth is counted in
//!   [`ExporterHealth`], which the stats API and Prometheus self-metrics
//!   render, so operators can distinguish disabled, starting, healthy,
//!   dropping, and failing export without exporting telemetry about
//!   telemetry.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use opentelemetry::trace::{Span as _, SpanKind, Status, Tracer as _, TracerProvider as _};
use opentelemetry::{Context, KeyValue};
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{
    Sampler, SdkTracerProvider, Span, SpanData, SpanExporter, SpanProcessor,
};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};

pub const DEFAULT_QUEUE_SPANS: usize = 256;
pub const DEFAULT_BATCH_SPANS: usize = 64;
pub const DEFAULT_EXPORT_DELAY: Duration = Duration::from_secs(1);
pub const DEFAULT_EXPORT_TIMEOUT: Duration = Duration::from_secs(2);
pub const MAX_QUEUE_SPANS: usize = 65_536;
pub const MAX_EXPORT_DELAY: Duration = Duration::from_secs(60);
pub const MAX_EXPORT_TIMEOUT: Duration = Duration::from_secs(60);
const SLOW_STEP: Duration = Duration::from_millis(50);
/// Upper bound on the retained text of the last export error.
const LAST_ERROR_MAX_CHARS: usize = 256;
/// Request headers the exporter owns; a configured duplicate would either
/// be silently overwritten or corrupt the OTLP request, so refuse it.
const RESERVED_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "host",
    "transfer-encoding",
];

// ---------------------------------------------------------------------------
// Operator configuration
// ---------------------------------------------------------------------------

/// Operator configuration for the metrics OTel trace exporter. `endpoint`
/// unset means export is disabled and nothing else here is used.
#[derive(Clone, Debug)]
pub struct OtelTracesConfig {
    /// Full OTLP/HTTP traces endpoint (`http://` or `https://`, no
    /// credentials in the URL).
    pub endpoint: Option<String>,
    /// Extra request headers, typically authentication. Values are treated
    /// as secrets: they are never logged or echoed in errors.
    pub headers: OtelHeaders,
    /// PEM file with the CA certificate(s) that sign the collector's TLS
    /// certificate. Only meaningful for an `https://` endpoint; unset uses
    /// the built-in WebPKI roots.
    pub ca_certificate: Option<PathBuf>,
    /// Fraction of sweeps traced, `0.0..=1.0`. Sampling is decided when the
    /// span starts, so an unsampled sweep costs no attributes or queueing.
    pub sample_ratio: f64,
    /// Capacity of the drop-on-full span queue.
    pub queue_spans: usize,
    /// Maximum spans per export request.
    pub batch_spans: usize,
    /// Longest a queued span waits before an export is attempted.
    pub export_delay: Duration,
    /// Per-request HTTP timeout, also the shutdown flush budget.
    pub export_timeout: Duration,
}

impl Default for OtelTracesConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            headers: OtelHeaders::default(),
            ca_certificate: None,
            sample_ratio: 1.0,
            queue_spans: DEFAULT_QUEUE_SPANS,
            batch_spans: DEFAULT_BATCH_SPANS,
            export_delay: DEFAULT_EXPORT_DELAY,
            export_timeout: DEFAULT_EXPORT_TIMEOUT,
        }
    }
}

impl OtelTracesConfig {
    pub fn enabled(&self) -> bool {
        self.endpoint.is_some()
    }

    /// Fail fast on anything that would silently misbehave later. Error
    /// text names the offending setting and never includes header values.
    pub fn validate(&self) -> Result<(), String> {
        validate_endpoint(self.endpoint.as_deref())?;
        let endpoint = match self.endpoint.as_deref().map(str::trim) {
            Some(endpoint) => endpoint,
            None => {
                if !self.headers.is_empty() {
                    return Err(
                        "metrics OTel traces headers require an OTel traces endpoint".into(),
                    );
                }
                if self.ca_certificate.is_some() {
                    return Err(
                        "metrics OTel traces CA certificate requires an OTel traces endpoint"
                            .into(),
                    );
                }
                return Ok(());
            }
        };
        if !self.sample_ratio.is_finite() || !(0.0..=1.0).contains(&self.sample_ratio) {
            return Err(format!(
                "metrics OTel traces sample ratio must be between 0.0 and 1.0, got {}",
                self.sample_ratio
            ));
        }
        if self.queue_spans == 0 || self.queue_spans > MAX_QUEUE_SPANS {
            return Err(format!(
                "metrics OTel traces queue must hold between 1 and {MAX_QUEUE_SPANS} spans, got {}",
                self.queue_spans
            ));
        }
        if self.batch_spans == 0 || self.batch_spans > self.queue_spans {
            return Err(format!(
                "metrics OTel traces export batch must be between 1 and the queue size {} spans, got {}",
                self.queue_spans, self.batch_spans
            ));
        }
        if self.export_delay.is_zero() || self.export_delay > MAX_EXPORT_DELAY {
            return Err(format!(
                "metrics OTel traces export delay must be between 1 ms and {} ms, got {} ms",
                MAX_EXPORT_DELAY.as_millis(),
                self.export_delay.as_millis()
            ));
        }
        if self.export_timeout.is_zero() || self.export_timeout > MAX_EXPORT_TIMEOUT {
            return Err(format!(
                "metrics OTel traces export timeout must be between 1 ms and {} ms, got {} ms",
                MAX_EXPORT_TIMEOUT.as_millis(),
                self.export_timeout.as_millis()
            ));
        }
        if let Some(path) = &self.ca_certificate {
            if !endpoint.starts_with("https://") {
                return Err(
                    "metrics OTel traces CA certificate requires an https:// endpoint".into(),
                );
            }
            load_ca_certificates(path)?;
        }
        Ok(())
    }
}

fn load_ca_certificates(path: &std::path::Path) -> Result<Vec<reqwest::Certificate>, String> {
    let pem = std::fs::read(path).map_err(|error| {
        format!(
            "read metrics OTel traces CA certificate {}: {error}",
            path.display()
        )
    })?;
    let certificates = reqwest::Certificate::from_pem_bundle(&pem).map_err(|error| {
        format!(
            "parse metrics OTel traces CA certificate {}: {error}",
            path.display()
        )
    })?;
    if certificates.is_empty() {
        return Err(format!(
            "metrics OTel traces CA certificate {} contains no PEM certificates",
            path.display()
        ));
    }
    Ok(certificates)
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

/// Extra OTLP request headers. Values are secrets by assumption: `Debug`
/// prints names only, and parse errors never quote a value.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct OtelHeaders(Vec<(String, String)>);

impl OtelHeaders {
    /// Parse the OTLP convention: comma- or newline-separated `name=value`
    /// entries. Values may be percent-encoded; the exporter decodes them.
    pub fn parse(input: &str) -> Result<Self, String> {
        let mut headers = Vec::new();
        for (index, entry) in input.split([',', '\n', '\r']).map(str::trim).enumerate() {
            if entry.is_empty() {
                continue;
            }
            let Some((name, value)) = entry.split_once('=') else {
                return Err(format!(
                    "metrics OTel traces header entry {} is not name=value",
                    index + 1
                ));
            };
            let name = name.trim();
            let value = value.trim();
            if name.is_empty() {
                return Err(format!(
                    "metrics OTel traces header entry {} has an empty name",
                    index + 1
                ));
            }
            if http::header::HeaderName::from_bytes(name.as_bytes()).is_err() {
                return Err(format!(
                    "metrics OTel traces header name {name:?} is not a valid HTTP header name"
                ));
            }
            if RESERVED_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
                return Err(format!(
                    "metrics OTel traces header {name:?} is set by the exporter and cannot be overridden"
                ));
            }
            if value.is_empty() {
                return Err(format!(
                    "metrics OTel traces header {name:?} has an empty value"
                ));
            }
            if http::header::HeaderValue::from_str(value).is_err() {
                return Err(format!(
                    "metrics OTel traces header {name:?} has a value that is not a valid HTTP header value"
                ));
            }
            headers.push((name.to_ascii_lowercase(), value.to_owned()));
        }
        Ok(Self(headers))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn names(&self) -> Vec<&str> {
        self.0.iter().map(|(name, _)| name.as_str()).collect()
    }

    fn to_map(&self) -> HashMap<String, String> {
        self.0.iter().cloned().collect()
    }
}

impl fmt::Debug for OtelHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|(name, _)| format!("{name}=<redacted>")))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Exporter health: exact accounting the stats API renders
// ---------------------------------------------------------------------------

/// Coarse operator-facing export state, derived from the counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtelExportState {
    /// No endpoint configured; nothing is queued or exported.
    #[default]
    Disabled,
    /// Enabled, but no export has been attempted and nothing dropped yet.
    Starting,
    /// The most recent export succeeded and no span was dropped since.
    Healthy,
    /// Exports succeed, but the queue overflowed since the last success:
    /// the collector is slower than the span rate.
    Dropping,
    /// The most recent export failed or timed out.
    Failing,
}

impl OtelExportState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Starting => "starting",
            Self::Healthy => "healthy",
            Self::Dropping => "dropping",
            Self::Failing => "failing",
        }
    }
}

/// Point-in-time exporter health, serialized inside `StorageStats`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct OtelTracesStats {
    pub state: OtelExportState,
    pub enabled: bool,
    pub sample_ratio: f64,
    pub queue_capacity_spans: u64,
    pub batch_spans: u64,
    pub export_delay_ms: u64,
    pub export_timeout_ms: u64,
    /// Spans waiting in the queue right now.
    pub queued_spans: u64,
    pub queue_high_water_spans: u64,
    pub enqueued_spans: u64,
    /// Spans discarded because the queue was full or export had shut down.
    pub dropped_spans: u64,
    pub dropped_since_last_success: u64,
    pub export_attempts: u64,
    pub export_successes: u64,
    pub export_failures: u64,
    pub exported_spans: u64,
    pub failed_spans: u64,
    pub last_success_unix_ms: Option<u64>,
    pub last_failure_unix_ms: Option<u64>,
    /// Bounded text of the most recent export error; retained after a later
    /// success so `last_failure_unix_ms` explains when it happened.
    pub last_error: Option<String>,
}

const ATTEMPT_NONE: u8 = 0;
const ATTEMPT_OK: u8 = 1;
const ATTEMPT_FAILED: u8 = 2;

/// Shared counters behind [`OtelTracesStats`]. Updated from the processor
/// (`on_end`, drops) and the export worker; read by the stats path.
#[derive(Debug)]
pub struct ExporterHealth {
    sample_ratio: f64,
    queue_capacity: u64,
    batch_spans: u64,
    export_delay: Duration,
    export_timeout: Duration,
    queued: AtomicU64,
    high_water: AtomicU64,
    enqueued: AtomicU64,
    dropped: AtomicU64,
    dropped_since_success: AtomicU64,
    attempts: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    exported_spans: AtomicU64,
    failed_spans: AtomicU64,
    last_success_ms: AtomicU64,
    last_failure_ms: AtomicU64,
    last_attempt: AtomicU8,
    last_error: Mutex<Option<String>>,
}

impl ExporterHealth {
    fn new(config: &OtelTracesConfig) -> Self {
        Self {
            sample_ratio: config.sample_ratio,
            queue_capacity: config.queue_spans as u64,
            batch_spans: config.batch_spans as u64,
            export_delay: config.export_delay,
            export_timeout: config.export_timeout,
            queued: AtomicU64::new(0),
            high_water: AtomicU64::new(0),
            enqueued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            dropped_since_success: AtomicU64::new(0),
            attempts: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            exported_spans: AtomicU64::new(0),
            failed_spans: AtomicU64::new(0),
            last_success_ms: AtomicU64::new(0),
            last_failure_ms: AtomicU64::new(0),
            last_attempt: AtomicU8::new(ATTEMPT_NONE),
            last_error: Mutex::new(None),
        }
    }

    fn record_enqueued(&self, depth: usize) {
        self.enqueued.fetch_add(1, Ordering::Relaxed);
        self.set_queued(depth);
    }

    fn set_queued(&self, depth: usize) {
        let depth = depth as u64;
        self.queued.store(depth, Ordering::Relaxed);
        self.high_water.fetch_max(depth, Ordering::Relaxed);
    }

    fn record_dropped(&self, spans: u64) {
        if spans == 0 {
            return;
        }
        self.dropped.fetch_add(spans, Ordering::Relaxed);
        self.dropped_since_success
            .fetch_add(spans, Ordering::Relaxed);
    }

    fn record_success(&self, spans: u64) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.successes.fetch_add(1, Ordering::Relaxed);
        self.exported_spans.fetch_add(spans, Ordering::Relaxed);
        self.last_success_ms.store(unix_ms(), Ordering::Relaxed);
        self.dropped_since_success.store(0, Ordering::Relaxed);
        self.last_attempt.store(ATTEMPT_OK, Ordering::Relaxed);
    }

    fn record_failure(&self, spans: u64, error: &OTelSdkError) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.failures.fetch_add(1, Ordering::Relaxed);
        self.failed_spans.fetch_add(spans, Ordering::Relaxed);
        self.last_failure_ms.store(unix_ms(), Ordering::Relaxed);
        self.last_attempt.store(ATTEMPT_FAILED, Ordering::Relaxed);
        let text: String = error
            .to_string()
            .chars()
            .take(LAST_ERROR_MAX_CHARS)
            .collect();
        *lock(&self.last_error) = Some(text);
    }

    pub fn snapshot(&self) -> OtelTracesStats {
        let dropped_since_success = self.dropped_since_success.load(Ordering::Relaxed);
        let state = match self.last_attempt.load(Ordering::Relaxed) {
            ATTEMPT_FAILED => OtelExportState::Failing,
            _ if dropped_since_success > 0 => OtelExportState::Dropping,
            ATTEMPT_OK => OtelExportState::Healthy,
            _ => OtelExportState::Starting,
        };
        let stamp = |value: u64| (value > 0).then_some(value);
        OtelTracesStats {
            state,
            enabled: true,
            sample_ratio: self.sample_ratio,
            queue_capacity_spans: self.queue_capacity,
            batch_spans: self.batch_spans,
            export_delay_ms: self.export_delay.as_millis() as u64,
            export_timeout_ms: self.export_timeout.as_millis() as u64,
            queued_spans: self.queued.load(Ordering::Relaxed),
            queue_high_water_spans: self.high_water.load(Ordering::Relaxed),
            enqueued_spans: self.enqueued.load(Ordering::Relaxed),
            dropped_spans: self.dropped.load(Ordering::Relaxed),
            dropped_since_last_success: dropped_since_success,
            export_attempts: self.attempts.load(Ordering::Relaxed),
            export_successes: self.successes.load(Ordering::Relaxed),
            export_failures: self.failures.load(Ordering::Relaxed),
            exported_spans: self.exported_spans.load(Ordering::Relaxed),
            failed_spans: self.failed_spans.load(Ordering::Relaxed),
            last_success_unix_ms: stamp(self.last_success_ms.load(Ordering::Relaxed)),
            last_failure_unix_ms: stamp(self.last_failure_ms.load(Ordering::Relaxed)),
            last_error: lock(&self.last_error).clone(),
        }
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
        .max(1)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Bounded batch processor with exact accounting
// ---------------------------------------------------------------------------

/// Counts every export outcome of the wrapped exporter.
struct AccountingExporter<E> {
    inner: Mutex<E>,
    health: Arc<ExporterHealth>,
}

impl<E: SpanExporter> AccountingExporter<E> {
    fn export_blocking(&self, batch: Vec<SpanData>) {
        let spans = batch.len() as u64;
        let inner = lock(&self.inner);
        match futures_executor::block_on(inner.export(batch)) {
            Ok(()) => self.health.record_success(spans),
            Err(error) => self.health.record_failure(spans, &error),
        }
    }
}

struct ProcessorState {
    queue: VecDeque<SpanData>,
    shutdown: bool,
    finished: bool,
    flush_requested: u64,
    flush_served: u64,
}

struct ProcessorShared<E> {
    state: Mutex<ProcessorState>,
    /// Wakes the worker: batch ready, flush, or shutdown.
    wake: Condvar,
    /// Wakes callers waiting on a flush or on shutdown completion.
    done: Condvar,
    health: Arc<ExporterHealth>,
    exporter: AccountingExporter<E>,
    capacity: usize,
    batch_spans: usize,
    delay: Duration,
}

/// Drop-on-full batching span processor with one export worker thread.
///
/// Unlike the SDK's batch processor it exposes exact drop, queue, and export
/// counters, and its shutdown returns within the caller's timeout even when
/// the exporter hangs (remaining spans are counted as dropped).
pub(crate) struct BoundedBatchProcessor<E> {
    shared: Arc<ProcessorShared<E>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl<E: SpanExporter + 'static> BoundedBatchProcessor<E> {
    pub(crate) fn new(exporter: E, config: &OtelTracesConfig, health: Arc<ExporterHealth>) -> Self {
        let shared = Arc::new(ProcessorShared {
            state: Mutex::new(ProcessorState {
                queue: VecDeque::with_capacity(config.queue_spans.min(1024)),
                shutdown: false,
                finished: false,
                flush_requested: 0,
                flush_served: 0,
            }),
            wake: Condvar::new(),
            done: Condvar::new(),
            health: Arc::clone(&health),
            exporter: AccountingExporter {
                inner: Mutex::new(exporter),
                health,
            },
            capacity: config.queue_spans,
            batch_spans: config.batch_spans,
            delay: config.export_delay,
        });
        let worker_shared = Arc::clone(&shared);
        let worker = thread::Builder::new()
            .name("timeless-otel-export".into())
            .spawn(move || run_worker(worker_shared))
            .ok();
        Self {
            shared,
            worker: Mutex::new(worker),
        }
    }
}

fn run_worker<E: SpanExporter>(shared: Arc<ProcessorShared<E>>) {
    let mut last_export = Instant::now();
    loop {
        let (batch, flush_generation) = {
            let mut state = lock(&shared.state);
            loop {
                let due = last_export.elapsed() >= shared.delay;
                let flush_pending = state.flush_requested > state.flush_served;
                if state.shutdown
                    || flush_pending
                    || state.queue.len() >= shared.batch_spans
                    || (due && !state.queue.is_empty())
                {
                    break;
                }
                if due {
                    last_export = Instant::now();
                }
                let wait = shared.delay.saturating_sub(last_export.elapsed());
                state = shared
                    .wake
                    .wait_timeout(state, wait)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
            if state.queue.is_empty() {
                state.flush_served = state.flush_requested;
                if state.shutdown {
                    state.finished = true;
                    shared.done.notify_all();
                    return;
                }
                shared.done.notify_all();
                continue;
            }
            let take = state.queue.len().min(shared.batch_spans);
            let batch: Vec<SpanData> = state.queue.drain(..take).collect();
            shared.health.set_queued(state.queue.len());
            (batch, state.flush_requested)
        };
        shared.exporter.export_blocking(batch);
        last_export = Instant::now();
        let mut state = lock(&shared.state);
        if state.queue.is_empty() && state.flush_served < flush_generation {
            state.flush_served = flush_generation;
            shared.done.notify_all();
        }
    }
}

impl<E> fmt::Debug for BoundedBatchProcessor<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedBatchProcessor")
            .field("capacity", &self.shared.capacity)
            .field("batch_spans", &self.shared.batch_spans)
            .finish()
    }
}

impl<E: SpanExporter + 'static> SpanProcessor for BoundedBatchProcessor<E> {
    fn on_start(&self, _span: &mut Span, _cx: &Context) {}

    fn on_end(&self, span: SpanData) {
        if !span.span_context.is_sampled() {
            return;
        }
        let mut state = lock(&self.shared.state);
        if state.shutdown || state.queue.len() >= self.shared.capacity {
            drop(state);
            self.shared.health.record_dropped(1);
            return;
        }
        state.queue.push_back(span);
        let depth = state.queue.len();
        self.shared.health.record_enqueued(depth);
        if depth >= self.shared.batch_spans {
            self.shared.wake.notify_one();
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        let timeout = self.shared.health.export_timeout;
        let deadline = Instant::now() + timeout;
        let mut state = lock(&self.shared.state);
        if state.shutdown {
            return Err(OTelSdkError::AlreadyShutdown);
        }
        state.flush_requested += 1;
        let generation = state.flush_requested;
        self.shared.wake.notify_one();
        while state.flush_served < generation {
            let now = Instant::now();
            if now >= deadline {
                return Err(OTelSdkError::Timeout(timeout));
            }
            state = self
                .shared
                .done
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        Ok(())
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        let deadline = Instant::now() + timeout;
        let mut state = lock(&self.shared.state);
        if state.shutdown {
            return Err(OTelSdkError::AlreadyShutdown);
        }
        state.shutdown = true;
        self.shared.wake.notify_all();
        while !state.finished {
            let now = Instant::now();
            if now >= deadline {
                // The worker is stuck in an export the HTTP timeout bounds;
                // do not wait for it. Whatever is still queued is lost.
                let left = state.queue.len() as u64;
                state.queue.clear();
                self.shared.health.set_queued(0);
                self.shared.health.record_dropped(left);
                return Err(OTelSdkError::Timeout(timeout));
            }
            state = self
                .shared
                .done
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        drop(state);
        if let Some(worker) = lock(&self.worker).take() {
            let _ = worker.join();
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        lock(&self.shared.exporter.inner).shutdown_with_timeout(remaining)
    }

    fn set_resource(&mut self, resource: &Resource) {
        lock(&self.shared.exporter.inner).set_resource(resource);
    }
}

// ---------------------------------------------------------------------------
// HTTP transport on the app's reqwest
// ---------------------------------------------------------------------------

/// OTLP transport over the crate's own blocking `reqwest` client: the
/// export worker is a plain thread, so a blocking send is the simplest
/// correct choice, and TLS roots/timeouts are configured in one place.
#[derive(Debug)]
struct BlockingHttpClient {
    client: reqwest::blocking::Client,
}

impl BlockingHttpClient {
    fn build(config: &OtelTracesConfig) -> Result<Self, String> {
        let certificates = match &config.ca_certificate {
            Some(path) => load_ca_certificates(path)?,
            None => Vec::new(),
        };
        let timeout = config.export_timeout;
        // A blocking reqwest client owns a runtime thread; build it off any
        // async context the caller may be in.
        let client = thread::spawn(move || {
            let mut builder = reqwest::blocking::Client::builder()
                .timeout(timeout)
                .connect_timeout(timeout)
                .no_proxy();
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
            builder.build()
        })
        .join()
        .map_err(|_| "metrics OTel traces HTTP client thread panicked".to_string())?
        .map_err(|error| format!("build metrics OTel traces HTTP client: {error}"))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl HttpClient for BlockingHttpClient {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let (parts, body) = request.into_parts();
        let url = reqwest::Url::parse(&parts.uri.to_string())?;
        let response = self
            .client
            .request(parts.method, url)
            .headers(parts.headers)
            .body(body)
            .send()?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.bytes()?;
        let mut out = Response::builder().status(status).body(body)?;
        *out.headers_mut() = headers;
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Owns the exporter lifecycle. The provider is not installed globally: only
/// the metrics compaction scheduler receives its tracer.
pub(crate) struct Telemetry {
    provider: SdkTracerProvider,
    compaction: CompactionTelemetry,
    health: Arc<ExporterHealth>,
    export_timeout: Duration,
}

impl Telemetry {
    pub(crate) fn initialize(config: &OtelTracesConfig) -> Result<Option<Self>, String> {
        config.validate()?;
        let Some(endpoint) = config.endpoint.as_deref().map(str::trim) else {
            return Ok(None);
        };
        let client = BlockingHttpClient::build(config)?;
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(endpoint)
            .with_timeout(config.export_timeout)
            .with_http_client(client)
            .with_headers(config.headers.to_map())
            .build()
            .map_err(|error| format!("initialize metrics OTLP/HTTP exporter: {error}"))?;
        let health = Arc::new(ExporterHealth::new(config));
        let processor = BoundedBatchProcessor::new(exporter, config, Arc::clone(&health));
        let identity = timeless_api_common::server_build_identity("metrics");
        let identity_string = |key: &str| {
            identity
                .get(key)
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned()
        };
        let mut builder = SdkTracerProvider::builder()
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
            );
        if config.sample_ratio < 1.0 {
            builder = builder.with_sampler(Sampler::TraceIdRatioBased(config.sample_ratio));
        }
        let provider = builder.build();
        let tracer = provider.tracer("timeless-metrics-api");
        Ok(Some(Self {
            provider,
            compaction: CompactionTelemetry { tracer },
            health,
            export_timeout: config.export_timeout,
        }))
    }

    pub(crate) fn compaction(&self) -> CompactionTelemetry {
        self.compaction.clone()
    }

    pub(crate) fn health(&self) -> Arc<ExporterHealth> {
        Arc::clone(&self.health)
    }

    /// Flush what is queued, then release the exporter. The budget is twice
    /// the export timeout: one in-flight request may need the full timeout
    /// to fail, and the last sweep's span deserves one more attempt. Call
    /// off the async runtime: the transport blocks.
    pub(crate) fn shutdown(self) -> Result<(), String> {
        self.provider
            .shutdown_with_timeout(self.export_timeout * 2)
            .map_err(|error| format!("flush metrics OpenTelemetry traces: {error}"))
    }
}

#[derive(Clone)]
pub(crate) struct CompactionTelemetry {
    tracer: opentelemetry_sdk::trace::SdkTracer,
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
    pub(crate) fn start(
        &self,
        metrics_series_budget: usize,
        metrics_point_budget: usize,
        metrics_byte_budget: u64,
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
    pub(crate) fn record_step(
        &mut self,
        step: u64,
        elapsed_ns: u64,
        continues: bool,
        work: CompactionWork,
    ) {
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
        mut self,
        steps: u64,
        total_ns: u64,
        max_step_ns: u64,
        read_retries: u64,
        work: CompactionWork,
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
    use std::sync::atomic::AtomicBool;
    use tempfile::TempDir;

    fn config(endpoint: &str) -> OtelTracesConfig {
        OtelTracesConfig {
            endpoint: Some(endpoint.into()),
            ..OtelTracesConfig::default()
        }
    }

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
        assert!(OtelTracesConfig::default().validate().is_ok());
        assert!(Telemetry::initialize(&OtelTracesConfig::default())
            .unwrap()
            .is_none());
    }

    #[test]
    fn headers_parse_and_never_reveal_values() {
        let headers =
            OtelHeaders::parse("Authorization=Bearer%20s3cr3t, X-Tenant = acme\napi-key=k")
                .unwrap();
        assert_eq!(headers.names(), ["authorization", "x-tenant", "api-key"]);
        let debug = format!("{headers:?}");
        assert!(debug.contains("authorization=<redacted>"), "{debug}");
        assert!(
            !debug.contains("s3cr3t") && !debug.contains("acme"),
            "{debug}"
        );

        let cases = [
            ("token-only-s3cr3t", "entry 1 is not name=value"),
            ("=s3cr3t", "entry 1 has an empty name"),
            ("bad name=s3cr3t", "not a valid HTTP header name"),
            ("Content-Type=s3cr3t", "set by the exporter"),
            ("x-empty=", "has an empty value"),
            ("x-ctl=s3\u{7}cr3t", "not a valid HTTP header value"),
        ];
        for (input, expected) in cases {
            let error = OtelHeaders::parse(input).unwrap_err();
            assert!(error.contains(expected), "{input:?}: {error}");
            assert!(
                !error.contains("s3cr3t"),
                "{input:?} leaked a value: {error}"
            );
        }
        assert!(OtelHeaders::parse(" , ,").unwrap().is_empty());
    }

    #[test]
    fn config_validation_names_the_offending_bound() {
        let endpoint = "http://127.0.0.1:1/v1/traces";
        let expect = |config: OtelTracesConfig, needle: &str| {
            let error = config.validate().unwrap_err();
            assert!(error.contains(needle), "{error}");
        };
        expect(
            OtelTracesConfig {
                headers: OtelHeaders::parse("a=b").unwrap(),
                ..OtelTracesConfig::default()
            },
            "headers require an OTel traces endpoint",
        );
        expect(
            OtelTracesConfig {
                ca_certificate: Some(PathBuf::from("/nonexistent.pem")),
                ..OtelTracesConfig::default()
            },
            "CA certificate requires an OTel traces endpoint",
        );
        expect(
            OtelTracesConfig {
                sample_ratio: 1.5,
                ..config(endpoint)
            },
            "sample ratio must be between 0.0 and 1.0",
        );
        expect(
            OtelTracesConfig {
                sample_ratio: f64::NAN,
                ..config(endpoint)
            },
            "sample ratio must be between 0.0 and 1.0",
        );
        expect(
            OtelTracesConfig {
                queue_spans: MAX_QUEUE_SPANS + 1,
                ..config(endpoint)
            },
            "queue must hold between 1 and 65536 spans",
        );
        expect(
            OtelTracesConfig {
                queue_spans: 8,
                batch_spans: 9,
                ..config(endpoint)
            },
            "export batch must be between 1 and the queue size 8",
        );
        expect(
            OtelTracesConfig {
                export_delay: Duration::ZERO,
                ..config(endpoint)
            },
            "export delay must be between 1 ms and 60000 ms",
        );
        expect(
            OtelTracesConfig {
                export_timeout: MAX_EXPORT_TIMEOUT + Duration::from_millis(1),
                ..config(endpoint)
            },
            "export timeout must be between 1 ms and 60000 ms",
        );
        expect(
            OtelTracesConfig {
                ca_certificate: Some(PathBuf::from("/nonexistent.pem")),
                ..config(endpoint)
            },
            "CA certificate requires an https:// endpoint",
        );
        expect(
            OtelTracesConfig {
                ca_certificate: Some(PathBuf::from("/nonexistent.pem")),
                ..config("https://collector.example.test/v1/traces")
            },
            "read metrics OTel traces CA certificate",
        );
        let directory = TempDir::new().unwrap();
        let not_pem = directory.path().join("ca.pem");
        std::fs::write(&not_pem, "not a certificate").unwrap();
        expect(
            OtelTracesConfig {
                ca_certificate: Some(not_pem),
                ..config("https://collector.example.test/v1/traces")
            },
            "contains no PEM certificates",
        );
        assert!(config(endpoint).validate().is_ok());
    }

    /// Test exporter: records batch sizes, can fail on demand, and can block
    /// until released so the queue fills behind it.
    #[derive(Clone, Debug, Default)]
    struct ScriptedExporter {
        batches: Arc<Mutex<Vec<usize>>>,
        fail: Arc<AtomicBool>,
        blocked: Arc<(Mutex<bool>, Condvar)>,
    }

    impl ScriptedExporter {
        fn block(&self) {
            *lock(&self.blocked.0) = true;
        }

        fn release(&self) {
            *lock(&self.blocked.0) = false;
            self.blocked.1.notify_all();
        }
    }

    impl SpanExporter for ScriptedExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            let mut blocked = lock(&self.blocked.0);
            while *blocked {
                blocked = self
                    .blocked
                    .1
                    .wait(blocked)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            drop(blocked);
            lock(&self.batches).push(batch.len());
            if self.fail.load(Ordering::Relaxed) {
                Err(OTelSdkError::InternalFailure(
                    "collector said 503 Service Unavailable".into(),
                ))
            } else {
                Ok(())
            }
        }
    }

    fn provider_with(
        exporter: ScriptedExporter,
        config: &OtelTracesConfig,
    ) -> (SdkTracerProvider, Arc<ExporterHealth>) {
        let health = Arc::new(ExporterHealth::new(config));
        let processor = BoundedBatchProcessor::new(exporter, config, Arc::clone(&health));
        (
            SdkTracerProvider::builder()
                .with_span_processor(processor)
                .build(),
            health,
        )
    }

    fn emit(provider: &SdkTracerProvider, count: usize) {
        let tracer = provider.tracer("test");
        for _ in 0..count {
            tracer.span_builder("sweep").start(&tracer).end();
        }
    }

    #[test]
    fn full_queue_drops_newest_spans_and_reports_pressure_then_recovers() {
        let config = OtelTracesConfig {
            queue_spans: 4,
            batch_spans: 2,
            export_delay: Duration::from_secs(30),
            export_timeout: Duration::from_secs(5),
            ..config("http://127.0.0.1:1/v1/traces")
        };
        let exporter = ScriptedExporter::default();
        exporter.block();
        let (provider, health) = provider_with(exporter.clone(), &config);

        // Ending spans never blocks: the first batch is taken by the worker
        // (stuck in the blocked exporter), the queue holds four, the rest drop.
        let started = Instant::now();
        emit(&provider, 20);
        assert!(started.elapsed() < Duration::from_secs(1));
        // Let the worker pick up its batch before reading the queue.
        let deadline = Instant::now() + Duration::from_secs(5);
        while health.snapshot().enqueued_spans + health.snapshot().dropped_spans < 20
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(5));
        }
        let pressured = health.snapshot();
        assert_eq!(pressured.state, OtelExportState::Dropping, "{pressured:?}");
        assert!(pressured.queued_spans <= 4, "{pressured:?}");
        assert!(pressured.queue_high_water_spans <= 4, "{pressured:?}");
        assert!(pressured.dropped_spans >= 14, "{pressured:?}");
        assert_eq!(
            pressured.enqueued_spans + pressured.dropped_spans,
            20,
            "{pressured:?}"
        );
        assert_eq!(pressured.export_attempts, 0, "{pressured:?}");

        exporter.release();
        provider.force_flush().unwrap();
        let recovered = health.snapshot();
        assert_eq!(recovered.state, OtelExportState::Healthy, "{recovered:?}");
        assert_eq!(recovered.queued_spans, 0, "{recovered:?}");
        assert_eq!(recovered.dropped_since_last_success, 0, "{recovered:?}");
        assert_eq!(recovered.dropped_spans, pressured.dropped_spans);
        assert_eq!(
            recovered.exported_spans, recovered.enqueued_spans,
            "{recovered:?}"
        );
        assert!(recovered.export_successes >= 1, "{recovered:?}");
        assert!(lock(&exporter.batches).iter().all(|size| *size <= 2));
        assert!(recovered.last_success_unix_ms.is_some());
        assert_eq!(recovered.last_error, None);
        provider.shutdown().unwrap();
    }

    #[test]
    fn export_failures_are_counted_and_state_recovers_on_success() {
        let config = OtelTracesConfig {
            queue_spans: 8,
            batch_spans: 8,
            export_delay: Duration::from_millis(20),
            export_timeout: Duration::from_secs(5),
            ..config("http://127.0.0.1:1/v1/traces")
        };
        let exporter = ScriptedExporter::default();
        exporter.fail.store(true, Ordering::Relaxed);
        let (provider, health) = provider_with(exporter.clone(), &config);
        assert_eq!(health.snapshot().state, OtelExportState::Starting);

        emit(&provider, 3);
        provider.force_flush().unwrap();
        let failing = health.snapshot();
        assert_eq!(failing.state, OtelExportState::Failing, "{failing:?}");
        assert_eq!(failing.export_attempts, 1);
        assert_eq!(failing.export_failures, 1);
        assert_eq!(failing.failed_spans, 3);
        assert_eq!(failing.exported_spans, 0);
        assert_eq!(failing.queued_spans, 0);
        assert_eq!(failing.dropped_spans, 0);
        assert!(failing.last_failure_unix_ms.is_some());
        assert!(
            failing
                .last_error
                .as_deref()
                .unwrap()
                .contains("503 Service Unavailable"),
            "{failing:?}"
        );

        // The scheduled delay exports without an explicit flush.
        exporter.fail.store(false, Ordering::Relaxed);
        emit(&provider, 2);
        let deadline = Instant::now() + Duration::from_secs(5);
        while health.snapshot().export_successes == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        let healthy = health.snapshot();
        assert_eq!(healthy.state, OtelExportState::Healthy, "{healthy:?}");
        assert_eq!(healthy.export_attempts, 2);
        assert_eq!(healthy.exported_spans, 2);
        assert_eq!(healthy.failed_spans, 3);
        // The last error stays visible; its timestamp dates it.
        assert!(healthy.last_error.is_some());
        assert!(healthy.last_success_unix_ms >= healthy.last_failure_unix_ms);
        provider.shutdown().unwrap();
    }

    #[test]
    fn shutdown_is_bounded_when_the_exporter_hangs() {
        let config = OtelTracesConfig {
            queue_spans: 16,
            batch_spans: 4,
            export_delay: Duration::from_secs(30),
            export_timeout: Duration::from_millis(200),
            ..config("http://127.0.0.1:1/v1/traces")
        };
        let exporter = ScriptedExporter::default();
        exporter.block();
        let (provider, health) = provider_with(exporter.clone(), &config);
        emit(&provider, 10);
        // Worker takes one batch of four and hangs; six stay queued.
        let deadline = Instant::now() + Duration::from_secs(5);
        while health.snapshot().queued_spans != 6 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(health.snapshot().queued_spans, 6);

        let started = Instant::now();
        let result = provider.shutdown_with_timeout(Duration::from_millis(200));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        // The provider wraps processor errors; the timeout must be visible.
        let error = result.expect_err("shutdown with a hung exporter must not report success");
        assert!(format!("{error:?}").contains("Timeout(200ms)"), "{error:?}");
        let after = health.snapshot();
        assert_eq!(after.queued_spans, 0, "{after:?}");
        assert_eq!(after.dropped_spans, 6, "{after:?}");
        // Spans ended after shutdown are dropped, not queued.
        emit(&provider, 1);
        assert_eq!(health.snapshot().dropped_spans, 6);
        exporter.release();
    }

    #[test]
    fn sample_ratio_zero_traces_nothing_and_costs_no_queue() {
        let config = OtelTracesConfig {
            sample_ratio: 0.0,
            export_timeout: Duration::from_secs(1),
            ..config("http://127.0.0.1:1/v1/traces")
        };
        let health = Arc::new(ExporterHealth::new(&config));
        let exporter = ScriptedExporter::default();
        let processor = BoundedBatchProcessor::new(exporter.clone(), &config, Arc::clone(&health));
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .with_sampler(Sampler::TraceIdRatioBased(config.sample_ratio))
            .build();
        emit(&provider, 50);
        provider.force_flush().unwrap();
        let snapshot = health.snapshot();
        assert_eq!(snapshot.enqueued_spans, 0, "{snapshot:?}");
        assert_eq!(snapshot.export_attempts, 0, "{snapshot:?}");
        assert!(lock(&exporter.batches).is_empty());
        provider.shutdown().unwrap();
    }

    /// Loopback OTLP receiver that records request headers and either
    /// answers 200, refuses, or stalls, so the real transport is exercised
    /// without the extension.
    struct Receiver {
        endpoint: String,
        seen_headers: Arc<Mutex<Vec<(String, String)>>>,
        requests: Arc<AtomicU64>,
        server: tokio::task::JoinHandle<()>,
    }

    async fn receiver(stall: bool) -> Receiver {
        use axum::{extract::State, http::HeaderMap, routing::post, Router};
        #[derive(Clone)]
        struct Seen {
            headers: Arc<Mutex<Vec<(String, String)>>>,
            requests: Arc<AtomicU64>,
            stall: bool,
        }
        async fn traces(State(seen): State<Seen>, headers: HeaderMap) -> axum::http::StatusCode {
            seen.requests.fetch_add(1, Ordering::Relaxed);
            {
                // Scoped so the guard is provably gone before the await.
                let mut recorded = lock(&seen.headers);
                for (name, value) in &headers {
                    recorded.push((
                        name.as_str().to_owned(),
                        value.to_str().unwrap_or("<binary>").to_owned(),
                    ));
                }
            }
            if seen.stall {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            axum::http::StatusCode::OK
        }
        let seen = Seen {
            headers: Arc::new(Mutex::new(Vec::new())),
            requests: Arc::new(AtomicU64::new(0)),
            stall,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/v1/traces", post(traces))
            .with_state(seen.clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Receiver {
            endpoint,
            seen_headers: seen.headers,
            requests: seen.requests,
            server,
        }
    }

    async fn run_one_sweep_and_shutdown(
        config: OtelTracesConfig,
    ) -> (Arc<ExporterHealth>, OtelTracesStats, Duration) {
        let telemetry = tokio::task::spawn_blocking(move || Telemetry::initialize(&config))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let health = telemetry.health();
        let compaction = telemetry.compaction();
        let started = Instant::now();
        let trace = compaction.start(
            64,
            256 * 1024,
            4 * 1024 * 1024,
            64,
            Duration::from_millis(10),
        );
        trace.finish(1, 10, 10, 0, CompactionWork::default(), &Ok(()));
        let span_cost = started.elapsed();
        tokio::task::spawn_blocking(move || {
            // Shutdown returns Err on timeout; the health snapshot is the
            // contract under test, not the flush result.
            let _ = telemetry.shutdown();
        })
        .await
        .unwrap();
        (Arc::clone(&health), health.snapshot(), span_cost)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_transport_delivers_configured_headers_and_reports_healthy() {
        let receiver = receiver(false).await;
        let config = OtelTracesConfig {
            headers: OtelHeaders::parse("Authorization=Bearer%20t0ken,X-Tenant=acme").unwrap(),
            export_timeout: Duration::from_secs(2),
            ..config(&receiver.endpoint)
        };
        let (_, snapshot, _) = run_one_sweep_and_shutdown(config).await;
        assert_eq!(snapshot.state, OtelExportState::Healthy, "{snapshot:?}");
        assert_eq!(snapshot.export_successes, 1, "{snapshot:?}");
        assert_eq!(snapshot.exported_spans, 1, "{snapshot:?}");
        assert_eq!(snapshot.dropped_spans, 0, "{snapshot:?}");
        assert_eq!(receiver.requests.load(Ordering::Relaxed), 1);
        let seen = lock(&receiver.seen_headers).clone();
        assert!(
            seen.contains(&("authorization".into(), "Bearer t0ken".into())),
            "{seen:?}"
        );
        assert!(
            seen.contains(&("x-tenant".into(), "acme".into())),
            "{seen:?}"
        );
        assert!(
            seen.contains(&("content-type".into(), "application/x-protobuf".into())),
            "{seen:?}"
        );
        receiver.server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refused_connection_is_a_counted_failure_that_never_slows_the_span() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
        drop(listener);
        let config = OtelTracesConfig {
            export_timeout: Duration::from_secs(2),
            ..config(&endpoint)
        };
        let (_, snapshot, span_cost) = run_one_sweep_and_shutdown(config).await;
        assert!(span_cost < Duration::from_millis(250), "{span_cost:?}");
        assert_eq!(snapshot.state, OtelExportState::Failing, "{snapshot:?}");
        assert_eq!(snapshot.export_failures, 1, "{snapshot:?}");
        assert_eq!(snapshot.failed_spans, 1, "{snapshot:?}");
        assert_eq!(snapshot.queued_spans, 0, "{snapshot:?}");
        assert!(snapshot.last_error.is_some(), "{snapshot:?}");
        assert!(snapshot.last_failure_unix_ms.is_some(), "{snapshot:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_collector_hits_the_export_timeout_not_the_data_plane() {
        let receiver = receiver(true).await;
        let config = OtelTracesConfig {
            export_timeout: Duration::from_millis(300),
            ..config(&receiver.endpoint)
        };
        let started = Instant::now();
        let (health, at_shutdown, span_cost) = run_one_sweep_and_shutdown(config).await;
        let total = started.elapsed();
        assert!(span_cost < Duration::from_millis(250), "{span_cost:?}");
        // Shutdown waits at most twice the export timeout for the stalled
        // request; it must not wait for the collector.
        assert!(total < Duration::from_secs(3), "{total:?}");
        assert_eq!(at_shutdown.queued_spans, 0, "{at_shutdown:?}");
        // The stalled request itself fails once the HTTP timeout fires.
        let deadline = Instant::now() + Duration::from_secs(5);
        while health.snapshot().export_attempts == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let snapshot = health.snapshot();
        assert_eq!(snapshot.state, OtelExportState::Failing, "{snapshot:?}");
        assert_eq!(snapshot.export_failures, 1, "{snapshot:?}");
        assert_eq!(snapshot.queued_spans, 0, "{snapshot:?}");
        assert_eq!(receiver.requests.load(Ordering::Relaxed), 1);
        receiver.server.abort();
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
        let telemetry = CompactionTelemetry {
            tracer: provider.tracer("test"),
        };

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
        let telemetry = Telemetry::initialize(&config(&endpoint)).unwrap().unwrap();
        metrics.attach_otel_health(telemetry.health());
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
        let telemetry = tokio::task::spawn_blocking(move || Telemetry::initialize(&config))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        metrics.attach_otel_health(telemetry.health());
        let compaction = telemetry.compaction();
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
