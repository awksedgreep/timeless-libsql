//! High-cardinality sparse compaction harness for issue #119.
//!
//! `metrics_compaction_bench` measures 320 series × 1,024 points: few
//! series, dense arrivals. The fleet workload behind #118 is the opposite
//! shape — hundreds of thousands of series, one sample per series per
//! five-minute slot — and it is that shape whose per-transaction and
//! per-series costs dominate. This harness replays it through `Storage`, so
//! every compaction step pays what it pays in the server: the writer queue,
//! the autocommit `compact-step` insert, `xBegin`, the commit, and the pause.
//!
//! Each round is one scrape slot: SERIES points at one aligned timestamp,
//! submitted in IMPORT_BATCH_POINTS batches like the fleet importer, then a
//! flush and one compaction sweep. The rollup ladder is the stack's, and the
//! data starts on an hour boundary, so a run of ROUNDS >= 24 closes an hour
//! and rollup steps do real work. Series identity, labels, and values are
//! synthetic and deterministic.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use serde::Serialize;
use timeless_metrics_api::{Storage, DEFAULT_RAW_RETENTION};

const DEFAULT_SERIES: usize = 550_000;
const DEFAULT_ROUNDS: usize = 26;
const SLOT_SECS: i64 = 300;
const IMPORT_BATCH_POINTS: usize = 20_000;
/// The rollup ladder `timeless_stack` configures (`config/runtime.exs`).
const STACK_ROLLUPS: &str = "1h@30d,1d@365d,30d@forever";
/// Fleet metric names: interface and Wi-Fi counters dominate the catalog.
const METRICS: [&str; 10] = [
    "fleet_if_in_octets",
    "fleet_if_out_octets",
    "fleet_if_in_errors",
    "fleet_if_out_errors",
    "fleet_if_info",
    "fleet_if_admin_status",
    "fleet_if_oper_status",
    "fleet_wifi_radio_packets_sent",
    "fleet_wifi_radio_packets_received",
    "fleet_device_reachable",
];
/// Interfaces per device; with METRICS.len() names this sets device count.
const INTERFACES: usize = 8;

#[derive(Serialize)]
struct RoundSample {
    round: usize,
    slot_ts: i64,
    ingest_ms: f64,
    ingest_cpu_ms: f64,
    flush_ms: f64,
    flush_cpu_ms: f64,
    flush_write_bytes: u64,
    sweep_ms: f64,
    sweep_cpu_ms: f64,
    sweep_write_bytes: u64,
    sweep_steps: u64,
    sweep_step_mean_ms: f64,
    sweep_step_high_water_ms: f64,
    sweep_yields: u64,
    raw_steps: i64,
    raw_chunks: i64,
    merge_steps: i64,
    merge_chunks: i64,
    plans: i64,
    raw_tier_chunks: i64,
    rollup_chunks: i64,
    disk_points: i64,
    points_per_raw_chunk: f64,
    database_file_bytes: u64,
    rss_kib: u64,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    series: usize,
    rounds: usize,
    slot_seconds: i64,
    import_batch_points: usize,
    rollups: &'static str,
    total_points: usize,
    samples: Vec<RoundSample>,
}

fn series_identity(series: usize) -> (&'static str, String) {
    let metric = METRICS[series % METRICS.len()];
    let interface = (series / METRICS.len()) % INTERFACES;
    let device = series / (METRICS.len() * INTERFACES);
    // A locally administered MAC derived from the device ordinal.
    let mac = format!(
        "02:00:{:02x}:{:02x}:{:02x}:{:02x}",
        (device >> 24) & 0xff,
        (device >> 16) & 0xff,
        (device >> 8) & 0xff,
        device & 0xff
    );
    (
        metric,
        format!("{{\"cm_mac\":\"{mac}\",\"interface_id\":\"{interface}\"}}"),
    )
}

/// One named batch: series `start..end`, one point each at `slot_ts`.
fn encode_batch(start: usize, end: usize, round: usize, slot_ts: i64) -> Vec<u8> {
    let count = end - start;
    let mut blob = Vec::with_capacity(12 + count * (96 + 20));
    blob.push(0x01);
    blob.push(0);
    blob.extend_from_slice(&0u16.to_le_bytes());
    blob.extend_from_slice(&(count as u32).to_le_bytes());
    blob.extend_from_slice(&(count as u32).to_le_bytes());
    for series in start..end {
        let (name, labels) = series_identity(series);
        blob.extend_from_slice(&(name.len() as u32).to_le_bytes());
        blob.extend_from_slice(name.as_bytes());
        blob.extend_from_slice(&(labels.len() as u32).to_le_bytes());
        blob.extend_from_slice(labels.as_bytes());
    }
    for index in 0..count {
        blob.extend_from_slice(&(index as u32).to_le_bytes());
    }
    for _ in 0..count {
        blob.extend_from_slice(&slot_ts.to_le_bytes());
    }
    for series in start..end {
        // Counters that advance by a series-specific rate, with small noise.
        let rate = 1_000.0 + (series % 997) as f64;
        let value = rate * round as f64 + ((series * 31 + round * 7) % 13) as f64;
        blob.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    blob
}

fn memory_kib() -> u64 {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

/// Process CPU (user + system) in milliseconds. The harness drives one
/// phase at a time, so a phase's delta is the writer's work plus the small
/// cost of the harness awaiting it.
fn process_cpu_ms() -> f64 {
    let Ok(stat) = fs::read_to_string("/proc/self/stat") else {
        return 0.0;
    };
    // Fields after the parenthesised command name; utime and stime are the
    // 14th and 15th fields overall.
    let Some(rest) = stat.rsplit_once(')').map(|(_, rest)| rest) else {
        return 0.0;
    };
    let fields: Vec<&str> = rest.split_ascii_whitespace().collect();
    let ticks: f64 = fields
        .get(11..13)
        .map(|f| f.iter().filter_map(|v| v.parse::<f64>().ok()).sum())
        .unwrap_or(0.0);
    // USER_HZ is 100 on every Linux target this harness runs on.
    ticks * 10.0
}

/// Bytes this process caused to be written to storage (`/proc/self/io`).
fn write_bytes() -> u64 {
    fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|io| {
            io.lines()
                .find(|line| line.starts_with("write_bytes:"))
                .and_then(|line| line.split_ascii_whitespace().nth(1))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0)
}

fn delta(after: i64, before: i64) -> i64 {
    after.saturating_sub(before)
}

async fn run(
    extension: PathBuf,
    database: PathBuf,
    series: usize,
    rounds: usize,
) -> Result<(), String> {
    if database.exists() {
        return Err(format!(
            "refusing to overwrite benchmark database {}",
            database.display()
        ));
    }
    // Slots end before now, so every sweep's wall-clock cutoff covers them,
    // and start on an hour boundary so round 24 onward closes an hour.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("clock: {error}"))?
        .as_secs() as i64;
    let span = rounds as i64 * SLOT_SECS;
    let base_ts = (now - span - 3_600).div_euclid(3_600) * 3_600;

    let storage = Storage::start_with_queue_bytes_and_rollups(
        database,
        extension,
        2,
        128,
        DEFAULT_RAW_RETENTION,
        Storage::DEFAULT_QUEUE_BYTES,
        Some(STACK_ROLLUPS),
    )?;
    let mut samples = Vec::with_capacity(rounds);

    for round in 0..rounds {
        let slot_ts = base_ts + round as i64 * SLOT_SECS;
        let batches: Vec<(Vec<u8>, usize)> = (0..series)
            .step_by(IMPORT_BATCH_POINTS)
            .map(|start| {
                let end = (start + IMPORT_BATCH_POINTS).min(series);
                (encode_batch(start, end, round, slot_ts), end - start)
            })
            .collect();

        let cpu = process_cpu_ms();
        let started = Instant::now();
        for (blob, points) in batches {
            storage.submit_named_batch(blob, points).await?;
        }
        storage.barrier().await?;
        let ingest_ms = started.elapsed().as_secs_f64() * 1000.0;
        let ingest_cpu_ms = process_cpu_ms() - cpu;

        let cpu = process_cpu_ms();
        let written = write_bytes();
        let started = Instant::now();
        storage.flush().await?;
        let flush_ms = started.elapsed().as_secs_f64() * 1000.0;
        let flush_cpu_ms = process_cpu_ms() - cpu;
        let flush_write_bytes = write_bytes().saturating_sub(written);

        let before = storage.stats().await?;
        let cpu = process_cpu_ms();
        let written = write_bytes();
        let started = Instant::now();
        storage.schedule_compact().await?;
        let sweep_ms = started.elapsed().as_secs_f64() * 1000.0;
        let sweep_cpu_ms = process_cpu_ms() - cpu;
        let sweep_write_bytes = write_bytes().saturating_sub(written);
        let after = storage.stats().await?;

        let sweep_steps = after
            .compact_step_count
            .saturating_sub(before.compact_step_count);
        let sweep_ns = after
            .compact_total_ns
            .saturating_sub(before.compact_total_ns);
        let sample = RoundSample {
            round,
            slot_ts,
            ingest_ms,
            ingest_cpu_ms,
            flush_ms,
            flush_cpu_ms,
            flush_write_bytes,
            sweep_ms,
            sweep_cpu_ms,
            sweep_write_bytes,
            sweep_steps,
            sweep_step_mean_ms: if sweep_steps == 0 {
                0.0
            } else {
                sweep_ns as f64 / sweep_steps as f64 / 1_000_000.0
            },
            sweep_step_high_water_ms: after.compact_step_max_ns as f64 / 1_000_000.0,
            sweep_yields: after
                .compact_yield_count
                .saturating_sub(before.compact_yield_count),
            raw_steps: delta(
                after.extension_compaction_raw_steps,
                before.extension_compaction_raw_steps,
            ),
            raw_chunks: delta(
                after.extension_compaction_raw_chunks,
                before.extension_compaction_raw_chunks,
            ),
            merge_steps: delta(
                after.extension_compaction_merge_steps,
                before.extension_compaction_merge_steps,
            ),
            merge_chunks: delta(
                after.extension_compaction_merge_chunks,
                before.extension_compaction_merge_chunks,
            ),
            plans: delta(
                after.extension_compaction_plans,
                before.extension_compaction_plans,
            ),
            raw_tier_chunks: after.raw_tier_chunks,
            rollup_chunks: after.rollup_chunks,
            disk_points: after.disk_points,
            points_per_raw_chunk: if after.raw_tier_chunks == 0 {
                0.0
            } else {
                after.disk_points as f64 / after.raw_tier_chunks as f64
            },
            database_file_bytes: after.database_file_bytes,
            rss_kib: memory_kib(),
        };
        eprintln!(
            "round {round:>3}: sweep {:>9.1} ms ({:>9.1} ms cpu, {:>6} steps, {:>7.2} ms/step), \
             flush {:>7.1} ms, raw chunks {:>9}, rollup chunks {:>8}, wrote {:>6.1} MiB",
            sample.sweep_ms,
            sample.sweep_cpu_ms,
            sample.sweep_steps,
            sample.sweep_step_mean_ms,
            sample.flush_ms,
            sample.raw_tier_chunks,
            sample.rollup_chunks,
            (sample.flush_write_bytes + sample.sweep_write_bytes) as f64 / 1_048_576.0,
        );
        samples.push(sample);
    }

    let total_points = series.saturating_mul(rounds);
    let final_stats = storage.stats().await?;
    if final_stats.disk_points != total_points as i64
        || final_stats.buffered_points != 0
        || final_stats.queued_points != 0
    {
        return Err(format!(
            "durability mismatch: expected {total_points} disk points and no buffered/queued work, got {} disk / {} buffered / {} queued",
            final_stats.disk_points, final_stats.buffered_points, final_stats.queued_points
        ));
    }
    storage.shutdown().await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&Report {
            schema: "timeless.metrics.sparse-compaction-benchmark.v1",
            series,
            rounds,
            slot_seconds: SLOT_SECS,
            import_batch_points: IMPORT_BATCH_POINTS,
            rollups: STACK_ROLLUPS,
            total_points,
            samples,
        })
        .map_err(|error| format!("serialize report: {error}"))?
    );
    Ok(())
}

fn parse_positive(value: Option<String>, default: usize, name: &str) -> Result<usize, String> {
    let parsed = value
        .map(|value| {
            value
                .parse()
                .map_err(|_| format!("{name} must be positive"))
        })
        .transpose()?
        .unwrap_or(default);
    (parsed > 0)
        .then_some(parsed)
        .ok_or_else(|| format!("{name} must be positive"))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let extension = args.next().map(PathBuf::from);
    let database = args.next().map(PathBuf::from);
    let series = parse_positive(args.next(), DEFAULT_SERIES, "SERIES");
    let rounds = parse_positive(args.next(), DEFAULT_ROUNDS, "ROUNDS");
    let result = match (extension, database, series, rounds) {
        (Some(extension), Some(database), Ok(series), Ok(rounds)) if args.next().is_none() => {
            run(extension, database, series, rounds).await
        }
        (_, _, Err(error), _) | (_, _, _, Err(error)) => Err(error),
        _ => Err(
            "usage: metrics_sparse_compaction_bench EXTENSION DATABASE [SERIES] [ROUNDS]".into(),
        ),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("metrics sparse compaction benchmark: {error}");
            ExitCode::FAILURE
        }
    }
}
