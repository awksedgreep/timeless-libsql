//! Streaming drivers: turn the fleet generators into a sequence of Tier 2
//! blobs pushed through a caller-supplied sink. The CLI's sink executes a
//! prepared INSERT and prints `\r` progress; the loadable extension's sink
//! does the same on the calling connection. Nothing here touches SQLite.

use crate::blobs::{encode_logs_blob, encode_metrics_blob, encode_spans_blob, LogEntry, SpanEntry};
use crate::fleet::{
    generate_log, generate_trace, Config, Incident, Rng, SeriesSpec, SeriesState, TraceReservoir,
};

pub const BLOB_LOGS: usize = 50_000;
pub const BLOB_SPANS: usize = 50_000;
pub const BLOB_METRIC_POINTS: usize = 250_000;

/// Live/follow pacing shared by the CLI and the extension.
pub const LIVE_LOG_RATE: usize = 1_500; // entries per second
pub const LIVE_TRACE_RATE: usize = 40; // traces per second

/// Receives each encoded blob plus the running item total; returns an
/// error string to abort the drive.
pub type Sink<'a> = &'a mut dyn FnMut(&[u8], usize) -> Result<(), String>;

/// What a drive produced. `raw_bytes` is the size of the logical rows as
/// the public surface returns them — for a metric sample 16 bytes
/// (ts + value; series identity is the amortized catalog, not the row),
/// for a log entry ts + level + message + metadata, for a span the ids,
/// kind/status, timings, and every string field. This is the honest
/// numerator for compression claims: the data itself, not our batch
/// encoding, not indexes, not SQLite pages.
#[derive(Clone, Copy, Default)]
pub struct DriveTotals {
    pub items: usize,
    pub raw_bytes: u64,
}

const RAW_METRIC_SAMPLE: u64 = 16; // 8 ts + 8 value
const RAW_LOG_FIXED: u64 = 9; // 8 ts + 1 level
const RAW_SPAN_FIXED: u64 = 50; // 16+8+8 ids, 1+1 kind/status, 8+8 timings

// ---------------------------------------------------------------------------
// Profiles
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub struct ProfileSpec {
    pub services: usize,
    pub pods: usize,
    pub paths: usize,
    pub minutes: u64,
    pub step_secs: u64,
    pub logs: usize,
    pub traces: usize,
}

pub fn profile(name: &str) -> Option<ProfileSpec> {
    Some(match name {
        "small" => ProfileSpec {
            services: 6,
            pods: 10,
            paths: 8,
            minutes: 30,
            step_secs: 15,
            logs: 200_000,
            traces: 20_000,
        },
        "medium" => ProfileSpec {
            services: 12,
            pods: 30,
            paths: 12,
            minutes: 60,
            step_secs: 15,
            logs: 2_000_000,
            traces: 100_000,
        },
        "large" => ProfileSpec {
            services: 25,
            pods: 100,
            paths: 12,
            minutes: 60,
            step_secs: 30,
            logs: 5_000_000,
            traces: 300_000,
        },
        _ => return None,
    })
}

impl ProfileSpec {
    /// Anchor a config to "now" (unix millis), aligned to the scrape step.
    pub fn config(&self, seed: u64, now_ms: i64) -> Config {
        Config {
            seed,
            services: self.services,
            pods: self.pods,
            paths: self.paths,
            minutes: self.minutes,
            step_secs: self.step_secs,
            logs: self.logs,
            traces: self.traces,
            end_ms: now_ms - now_ms.rem_euclid((self.step_secs * 1000) as i64),
        }
    }
}

// ---------------------------------------------------------------------------
// Drivers
// ---------------------------------------------------------------------------

fn span_raw_bytes(spans: &[SpanEntry]) -> u64 {
    spans
        .iter()
        .map(|s| {
            RAW_SPAN_FIXED
                + (s.name.len()
                    + s.service.len()
                    + s.attributes.len()
                    + s.status_message.len()
                    + s.events.len()
                    + s.resource.len()
                    + s.scope.len()) as u64
        })
        .sum()
}

/// Generate `n_traces` whole traces spread over [start_ms, end_ms) and
/// push them as rich span blobs.
pub fn drive_traces(
    cfg: &Config,
    incident: &Incident,
    rng: &mut Rng,
    start_ms: i64,
    end_ms: i64,
    n_traces: usize,
    reservoir: &mut TraceReservoir,
    sink: Sink,
) -> Result<DriveTotals, String> {
    let start_ns = start_ms * 1_000_000;
    let window_ns = (end_ms - start_ms).max(1) * 1_000_000;
    let step_ns = (window_ns / n_traces.max(1) as i64).max(1);

    let mut spans: Vec<SpanEntry> = Vec::with_capacity(BLOB_SPANS + 32);
    let mut totals = DriveTotals::default();
    for t in 0..n_traces {
        let ts = start_ns + t as i64 * step_ns + rng.below(step_ns as u64) as i64;
        generate_trace(rng, cfg, incident, ts, reservoir, &mut spans);
        if spans.len() >= BLOB_SPANS {
            totals.items += spans.len();
            totals.raw_bytes += span_raw_bytes(&spans);
            sink(&encode_spans_blob(&spans), totals.items)?;
            spans.clear();
        }
    }
    if !spans.is_empty() {
        totals.items += spans.len();
        totals.raw_bytes += span_raw_bytes(&spans);
        sink(&encode_spans_blob(&spans), totals.items)?;
    }
    Ok(totals)
}

fn log_raw_bytes(entries: &[LogEntry]) -> u64 {
    entries
        .iter()
        .map(|e| RAW_LOG_FIXED + (e.message.len() + e.metadata.len()) as u64)
        .sum()
}

/// Generate `n_logs` entries spread evenly over [start_ms, end_ms) and
/// push them as log blobs.
pub fn drive_logs(
    cfg: &Config,
    incident: &Incident,
    rng: &mut Rng,
    start_ms: i64,
    end_ms: i64,
    n_logs: usize,
    reservoir: &TraceReservoir,
    sink: Sink,
) -> Result<DriveTotals, String> {
    let window = (end_ms - start_ms).max(1);
    let mut buf: Vec<LogEntry> = Vec::with_capacity(BLOB_LOGS);
    let mut totals = DriveTotals::default();
    for i in 0..n_logs {
        let ts = start_ms + (window as i128 * i as i128 / n_logs.max(1) as i128) as i64;
        buf.push(generate_log(rng, cfg, incident, reservoir, ts));
        if buf.len() >= BLOB_LOGS {
            totals.items += buf.len();
            totals.raw_bytes += log_raw_bytes(&buf);
            sink(&encode_logs_blob(&buf), totals.items)?;
            buf.clear();
        }
    }
    if !buf.is_empty() {
        totals.items += buf.len();
        totals.raw_bytes += log_raw_bytes(&buf);
        sink(&encode_logs_blob(&buf), totals.items)?;
    }
    Ok(totals)
}

/// Advance every series `n_steps` scrapes starting at absolute step
/// `first_step` (timestamps come from `ts_of(step)`), pushing metric
/// blobs grouped so each blob holds ~BLOB_METRIC_POINTS samples.
/// `states` must be positioned at `first_step` (see `warm_states`).
pub fn drive_metrics(
    cfg: &Config,
    incident: &Incident,
    catalog: &[SeriesSpec],
    states: &mut [SeriesState],
    first_step: usize,
    n_steps: usize,
    ts_of: &dyn Fn(usize) -> i64,
    sink: Sink,
) -> Result<DriveTotals, String> {
    let group = (BLOB_METRIC_POINTS / n_steps.max(1)).max(1);
    let mut totals = DriveTotals::default();
    let mut points: Vec<(u32, i64, f64)> = Vec::with_capacity(group.min(catalog.len()) * n_steps);
    for (chunk, schunk) in catalog.chunks(group).zip(states.chunks_mut(group)) {
        let table: Vec<(String, String)> = chunk
            .iter()
            .map(|s| (s.name.to_string(), s.labels.clone()))
            .collect();
        points.clear();
        for (local, (spec, state)) in chunk.iter().zip(schunk.iter_mut()).enumerate() {
            for i in first_step..first_step + n_steps {
                let ts = ts_of(i);
                points.push((local as u32, ts, state.value(spec, cfg, incident, i, ts)));
            }
        }
        totals.items += points.len();
        totals.raw_bytes += points.len() as u64 * RAW_METRIC_SAMPLE;
        sink(&encode_metrics_blob(&table, &points), totals.items)?;
    }
    Ok(totals)
}

// ---------------------------------------------------------------------------
// Storage report. The numbers come from two places, both defensible:
// raw bytes are counted at generation time (the logical rows as the public
// surface returns them), and stored/index bytes come from the engine's own
// public `timeless_stats` counters (`bytes_on_disk`, `index_bytes`) — data
// payload only, never SQLite pages, never the WAL, never free space.
// ---------------------------------------------------------------------------

pub struct SignalReport {
    /// e.g. "metrics"
    pub label: &'static str,
    /// e.g. "samples"
    pub unit: &'static str,
    /// e.g. "sample" (for the B/<unit> figure)
    pub per: &'static str,
    pub items: u64,
    pub raw_bytes: u64,
    pub payload_bytes: u64,
    pub index_bytes: u64,
}

pub fn fmt_count_u64(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

pub fn fmt_mb(bytes: u64) -> String {
    let mb = bytes as f64 / 1e6;
    if mb >= 100.0 {
        format!("{mb:.0} MB")
    } else if mb >= 1.0 {
        format!("{mb:.1} MB")
    } else {
        format!("{mb:.2} MB")
    }
}

/// Render the compression/storage table. `file_bytes`/`free_bytes` are the
/// checkpointed main database file and its reclaimable freelist.
pub fn format_report(signals: &[SignalReport], file_bytes: u64, free_bytes: u64) -> String {
    let mut out = String::from(
        "storage (raw = logical rows as queried; stored/index = engine block bytes on disk):\n",
    );
    let (mut raw_t, mut payload_t, mut index_t) = (0u64, 0u64, 0u64);
    for s in signals {
        raw_t += s.raw_bytes;
        payload_t += s.payload_bytes;
        index_t += s.index_bytes;
        let ratio = s.raw_bytes as f64 / s.payload_bytes.max(1) as f64;
        let per = s.payload_bytes as f64 / s.items.max(1) as f64;
        out.push_str(&format!(
            "  {:<8} {:>13} {:<8} raw {:>9} -> stored {:>9}  ({:>5.1}x, {:.1} B/{})  index {}\n",
            s.label,
            fmt_count_u64(s.items),
            s.unit,
            fmt_mb(s.raw_bytes),
            fmt_mb(s.payload_bytes),
            ratio,
            per,
            s.per,
            fmt_mb(s.index_bytes),
        ));
    }
    out.push_str(&format!(
        "  total    raw {} -> stored {} ({:.1}x); indexes {}\n",
        fmt_mb(raw_t),
        fmt_mb(payload_t),
        raw_t as f64 / payload_t.max(1) as f64,
        fmt_mb(index_t),
    ));
    out.push_str(&format!(
        "  file     {} (data + indexes + btree overhead{})",
        fmt_mb(file_bytes.saturating_sub(free_bytes)),
        if free_bytes > 0 {
            format!(
                "; +{} reclaimable free — run VACUUM; to return it to the OS",
                fmt_mb(free_bytes)
            )
        } else {
            String::new()
        },
    ));
    out
}

/// Fresh per-series states advanced through `steps` seeded scrapes, so a
/// continuation (live mode, `tick`, `follow`) extends each walk and
/// counter instead of restarting it.
pub fn warm_states(
    catalog: &[SeriesSpec],
    cfg: &Config,
    incident: &Incident,
    steps: usize,
) -> Vec<SeriesState> {
    let step_ms = (cfg.step_secs * 1000) as i64;
    catalog
        .iter()
        .map(|spec| {
            let mut state = SeriesState::new(spec);
            for i in 0..steps {
                state.value(spec, cfg, incident, i, cfg.start_ms() + i as i64 * step_ms);
            }
            state
        })
        .collect()
}
