//! Production-shaped repeated-arrival harness for issue #52.
//!
//! The driver builds one fixed 320-series × 1,024-point batch before starting
//! a fresh worker process. The worker reuses that payload shape after every
//! completed compact sweep, shifting timestamps before the ingest clock starts
//! so each round is a true append while payload synthesis and pacing stay out
//! of maintenance timing. The real release extension runs through `Storage`.

use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::Instant;

use serde::Serialize;
use timeless_metrics_api::{Storage, DEFAULT_RAW_RETENTION};

const SERIES: usize = 320;
const POINTS_PER_SERIES: usize = 1024;
const DEFAULT_ROUNDS: usize = 32;
const BASE_TS: i64 = 1_700_000_000;
const SAMPLE_INTERVAL_SECS: i64 = 15;

#[derive(Serialize)]
struct RoundSample {
    round: usize,
    ingest_ms: f64,
    flush_ms: f64,
    active_sweep_ms: f64,
    maintenance_steps: u64,
    maintenance_step_mean_ms: f64,
    maintenance_step_high_water_ms: f64,
    raw_steps: i64,
    raw_points: i64,
    raw_input_bytes: i64,
    raw_output_bytes: i64,
    merge_steps: i64,
    merge_points: i64,
    merge_input_bytes: i64,
    merge_output_bytes: i64,
    chunks: i64,
    bytes_on_disk: i64,
    rss_kib: u64,
    hwm_kib: u64,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    series: usize,
    points_per_arrival: usize,
    rounds: usize,
    total_points: usize,
    samples: Vec<RoundSample>,
}

fn metric_name(series: usize) -> String {
    format!("production_metric_{:02}", series % 10)
}

fn labels(series: usize) -> String {
    const REGIONS: [&str; 4] = ["us-east", "us-west", "eu-west", "ap-south"];
    format!(
        "{{\"env\":\"prod\",\"host\":\"host-{series:03}\",\"region\":\"{}\"}}",
        REGIONS[series % REGIONS.len()]
    )
}

fn encode_fixed_batch() -> Vec<u8> {
    let points = SERIES * POINTS_PER_SERIES;
    let mut blob = Vec::with_capacity(points * 20 + SERIES * 96 + 12);
    blob.push(0x01);
    blob.push(0);
    blob.extend_from_slice(&0u16.to_le_bytes());
    blob.extend_from_slice(&(SERIES as u32).to_le_bytes());
    blob.extend_from_slice(&(points as u32).to_le_bytes());
    for series in 0..SERIES {
        let name = metric_name(series);
        let labels = labels(series);
        blob.extend_from_slice(&(name.len() as u32).to_le_bytes());
        blob.extend_from_slice(name.as_bytes());
        blob.extend_from_slice(&(labels.len() as u32).to_le_bytes());
        blob.extend_from_slice(labels.as_bytes());
    }
    for _ in 0..POINTS_PER_SERIES {
        for series in 0..SERIES {
            blob.extend_from_slice(&(series as u32).to_le_bytes());
        }
    }
    for point in 0..POINTS_PER_SERIES {
        for _ in 0..SERIES {
            blob.extend_from_slice(&(BASE_TS + point as i64 * SAMPLE_INTERVAL_SECS).to_le_bytes());
        }
    }
    for point in 0..POINTS_PER_SERIES {
        for series in 0..SERIES {
            let phase = (series as f64 * 0.17) + (point as f64 * 0.013);
            let value = 50.0 + phase.sin() * 40.0 + ((point * 37 + series) % 100) as f64 / 100.0;
            blob.extend_from_slice(&value.to_bits().to_le_bytes());
        }
    }
    blob
}

fn timestamp_offset() -> usize {
    let catalog_bytes: usize = (0..SERIES)
        .map(|series| 8 + metric_name(series).len() + labels(series).len())
        .sum();
    12 + catalog_bytes + SERIES * POINTS_PER_SERIES * 4
}

fn shift_batch_timestamps(batch: &mut [u8], round: usize) -> Result<(), String> {
    let timestamp_bytes = SERIES
        .checked_mul(POINTS_PER_SERIES)
        .and_then(|points| points.checked_mul(8))
        .ok_or_else(|| "timestamp section length overflow".to_string())?;
    let start = timestamp_offset();
    let end = start
        .checked_add(timestamp_bytes)
        .filter(|end| *end <= batch.len())
        .ok_or_else(|| "prebuilt payload has a truncated timestamp section".to_string())?;
    let append_seconds = i64::try_from(round.saturating_sub(1))
        .ok()
        .and_then(|round| round.checked_mul(POINTS_PER_SERIES as i64))
        .and_then(|points| points.checked_mul(SAMPLE_INTERVAL_SECS))
        .ok_or_else(|| "round timestamp offset overflow".to_string())?;
    let (timestamps, remainder) = batch[start..end].as_chunks_mut::<8>();
    debug_assert!(remainder.is_empty());
    for timestamp in timestamps {
        let base = i64::from_le_bytes(*timestamp);
        let shifted = base
            .checked_add(append_seconds)
            .ok_or_else(|| "appended timestamp overflow".to_string())?;
        timestamp.copy_from_slice(&shifted.to_le_bytes());
    }
    Ok(())
}

fn memory_kib() -> (u64, u64) {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return (0, 0);
    };
    let mut rss = 0;
    let mut hwm = 0;
    for line in status.lines() {
        let value = line
            .split_ascii_whitespace()
            .nth(1)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        if line.starts_with("VmRSS:") {
            rss = value;
        } else if line.starts_with("VmHWM:") {
            hwm = value;
        }
    }
    (rss, hwm)
}

fn delta(after: i64, before: i64) -> i64 {
    after.saturating_sub(before)
}

async fn worker(
    extension: PathBuf,
    database: PathBuf,
    payload: PathBuf,
    rounds: usize,
) -> Result<(), String> {
    let fixed_batch = fs::read(&payload)
        .map_err(|error| format!("read prebuilt payload {}: {error}", payload.display()))?;
    let points_per_arrival = SERIES * POINTS_PER_SERIES;
    let storage = Storage::start(database, extension, 2, 128, DEFAULT_RAW_RETENTION)?;
    let mut samples = Vec::with_capacity(rounds);

    for round in 1..=rounds {
        let mut batch = fixed_batch.clone();
        shift_batch_timestamps(&mut batch, round)?;
        let started = Instant::now();
        storage
            .submit_named_batch(batch, points_per_arrival)
            .await?;
        let ingest_ms = started.elapsed().as_secs_f64() * 1000.0;

        let started = Instant::now();
        storage.flush().await?;
        let flush_ms = started.elapsed().as_secs_f64() * 1000.0;

        let before = storage.stats().await?;
        let started = Instant::now();
        storage.schedule_compact().await?;
        let active_sweep_ms = started.elapsed().as_secs_f64() * 1000.0;
        let after = storage.stats().await?;
        let maintenance_steps = after
            .compact_step_count
            .saturating_sub(before.compact_step_count);
        let maintenance_ns = after
            .compact_total_ns
            .saturating_sub(before.compact_total_ns);
        let (rss_kib, hwm_kib) = memory_kib();
        samples.push(RoundSample {
            round,
            ingest_ms,
            flush_ms,
            active_sweep_ms,
            maintenance_steps,
            maintenance_step_mean_ms: if maintenance_steps == 0 {
                0.0
            } else {
                maintenance_ns as f64 / maintenance_steps as f64 / 1_000_000.0
            },
            maintenance_step_high_water_ms: after.compact_step_max_ns as f64 / 1_000_000.0,
            raw_steps: delta(
                after.extension_compaction_raw_steps,
                before.extension_compaction_raw_steps,
            ),
            raw_points: delta(
                after.extension_compaction_raw_points,
                before.extension_compaction_raw_points,
            ),
            raw_input_bytes: delta(
                after.extension_compaction_raw_input_bytes,
                before.extension_compaction_raw_input_bytes,
            ),
            raw_output_bytes: delta(
                after.extension_compaction_raw_output_bytes,
                before.extension_compaction_raw_output_bytes,
            ),
            merge_steps: delta(
                after.extension_compaction_merge_steps,
                before.extension_compaction_merge_steps,
            ),
            merge_points: delta(
                after.extension_compaction_merge_points,
                before.extension_compaction_merge_points,
            ),
            merge_input_bytes: delta(
                after.extension_compaction_merge_input_bytes,
                before.extension_compaction_merge_input_bytes,
            ),
            merge_output_bytes: delta(
                after.extension_compaction_merge_output_bytes,
                before.extension_compaction_merge_output_bytes,
            ),
            chunks: after.raw_tier_chunks,
            bytes_on_disk: after.bytes_on_disk,
            rss_kib,
            hwm_kib,
        });
    }

    let final_stats = storage.stats().await?;
    let total_points = rounds.saturating_mul(points_per_arrival);
    if final_stats.disk_points != total_points as i64
        || final_stats.buffered_points != 0
        || final_stats.queued_points != 0
    {
        return Err(format!(
            "durability mismatch: expected {total_points} disk points and no buffered/queued work, got {final_stats:?}"
        ));
    }
    storage.shutdown().await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&Report {
            schema: "timeless.metrics.compaction-benchmark.v1",
            series: SERIES,
            points_per_arrival,
            rounds,
            total_points,
            samples,
        })
        .map_err(|error| format!("serialize report: {error}"))?
    );
    Ok(())
}

fn prepare_payload(database: &Path) -> Result<PathBuf, String> {
    let parent = database.parent().unwrap_or_else(|| Path::new("."));
    let payload = parent.join(format!(
        ".timeless-metrics-compaction-{}.payload",
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&payload)
        .map_err(|error| format!("create payload {}: {error}", payload.display()))?;
    file.write_all(&encode_fixed_batch())
        .map_err(|error| format!("write payload {}: {error}", payload.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync payload {}: {error}", payload.display()))?;
    Ok(payload)
}

fn driver(extension: PathBuf, database: PathBuf, rounds: usize) -> Result<(), String> {
    if database.exists() {
        return Err(format!(
            "refusing to overwrite benchmark database {}",
            database.display()
        ));
    }
    let payload = prepare_payload(&database)?;
    let executable = env::current_exe().map_err(|error| format!("resolve executable: {error}"))?;
    let status = Command::new(executable)
        .arg("--worker")
        .arg(&extension)
        .arg(&database)
        .arg(&payload)
        .arg(rounds.to_string())
        .status()
        .map_err(|error| format!("start benchmark worker: {error}"));
    let status = status?;
    fs::remove_file(&payload)
        .map_err(|error| format!("remove payload {}: {error}", payload.display()))?;
    if !status.success() {
        return Err(format!(
            "benchmark worker failed; database retained at {}",
            database.display()
        ));
    }
    Ok(())
}

fn parse_rounds(value: Option<String>) -> Result<usize, String> {
    let rounds = value
        .map(|value| value.parse().map_err(|_| "ROUNDS must be positive"))
        .transpose()?
        .unwrap_or(DEFAULT_ROUNDS);
    (rounds > 0)
        .then_some(rounds)
        .ok_or_else(|| "ROUNDS must be positive".into())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let result = if args.next().as_deref() == Some("--worker") {
        let extension = args.next().map(PathBuf::from);
        let database = args.next().map(PathBuf::from);
        let payload = args.next().map(PathBuf::from);
        let rounds = parse_rounds(args.next());
        match (extension, database, payload, rounds) {
            (Some(extension), Some(database), Some(payload), Ok(rounds))
                if args.next().is_none() =>
            {
                worker(extension, database, payload, rounds).await
            }
            (_, _, _, Err(error)) => Err(error),
            _ => Err("worker usage: --worker EXTENSION DATABASE PAYLOAD ROUNDS".into()),
        }
    } else {
        let mut args = env::args().skip(1);
        let extension = args.next().map(PathBuf::from);
        let database = args.next().map(PathBuf::from);
        let rounds = parse_rounds(args.next());
        match (extension, database, rounds) {
            (Some(extension), Some(database), Ok(rounds)) if args.next().is_none() => {
                driver(extension, database, rounds)
            }
            (_, _, Err(error)) => Err(error),
            _ => Err("usage: metrics_compaction_bench EXTENSION DATABASE [ROUNDS]".into()),
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("metrics compaction benchmark: {error}");
            ExitCode::FAILURE
        }
    }
}
