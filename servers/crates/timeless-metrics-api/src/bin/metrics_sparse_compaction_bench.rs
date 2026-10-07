//! High-cardinality sparse compaction harness for issue #119.
//!
//! `metrics_compaction_bench` measures 320 series × 1,024 points: few
//! series, dense arrivals. The fleet workload behind #118 is the opposite
//! shape — hundreds of thousands of series, at most one sample per series
//! per five-minute slot — and it is that shape whose per-transaction and
//! per-series costs dominate. This harness replays it through `Storage`, so
//! every compaction step pays what it pays in the server: the writer queue,
//! the autocommit `compact-step` insert, `xBegin`, the commit, and the pause.
//!
//! The catalog is a synthetic copy of the live fleet measured on 2026-10-07
//! (5,659 gateways, ~550k series): per gateway, interface counters and
//! status, an `if_info` series with descriptive labels, Wi-Fi radio counters,
//! and device gauges, plus rare radio-error and fleet-wide series. A series
//! reports in a slot with its family's measured probability (interface
//! series ~82%, so ~445k points per slot), and a slice of the interface
//! counters retires after the first few slots the way a label change left
//! ~39k dead series in the live store. Values follow each family's kind:
//! advancing counters, near-constant status gauges, and noisy levels.
//!
//! Each round is one scrape slot, sent gateway by gateway in
//! IMPORT_BATCH_POINTS batches like the fleet importer, then a flush and one
//! compaction sweep. The rollup ladder is the stack's, and the data starts on
//! an hour boundary, so round 24 onward closes an hour and rollup steps do
//! real work; the 1h tier first has four chunks to merge at round 60.
//! Everything is deterministic for a given gateway count.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use serde::Serialize;
use timeless_metrics_api::{Storage, DEFAULT_RAW_RETENTION};

/// Gateways in the live fleet; the catalog scales with this.
const DEFAULT_DEVICES: usize = 5_659;
const DEFAULT_ROUNDS: usize = 26;
const SLOT_SECS: i64 = 300;
const IMPORT_BATCH_POINTS: usize = 20_000;
/// The rollup ladder `timeless_stack` configures (`config/runtime.exs`).
const STACK_ROLLUPS: &str = "1h@30d,1d@365d,30d@forever";
/// Interfaces per gateway that carry counters and an `if_info` series, and
/// the extra info-only interfaces (live: ~16.0 counter and ~18.2 info series
/// per gateway).
const COUNTER_INTERFACES: usize = 16;
const INFO_ONLY_INTERFACES: usize = 2;
/// Counter series that stop reporting after RETIRE_AFTER_ROUNDS slots, in
/// per mille of interface counters (live: ~38.7k dead of ~511k+).
const RETIRED_PER_MILLE: u64 = 70;
const RETIRE_AFTER_ROUNDS: usize = 4;

#[derive(Clone, Copy)]
enum Value {
    /// Monotonic counter advancing by a series-specific rate per slot.
    Counter { rate: f64 },
    /// Mostly-zero counter with occasional increments.
    Sparse,
    /// A status gauge that rarely changes.
    Status,
    /// Constant 1 (info series, reachability).
    One,
    /// A noisy level around a base.
    Level { base: f64 },
}

struct Series {
    name: &'static str,
    labels: String,
    /// Probability of reporting in a slot, per mille.
    report_per_mille: u64,
    retires: bool,
    value: Value,
}

#[derive(Serialize)]
struct RoundSample {
    round: usize,
    slot_ts: i64,
    points: usize,
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
    devices: usize,
    series: usize,
    rounds: usize,
    slot_seconds: i64,
    import_batch_points: usize,
    rollups: &'static str,
    total_points: usize,
    samples: Vec<RoundSample>,
}

/// SplitMix64: a deterministic, well-mixed hash for per-series choices.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn mac(device: usize) -> String {
    let d = mix(device as u64 ^ 0x6d61_6373);
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        (d & 0xfe) | 0x02,
        (d >> 8) & 0xff,
        (d >> 16) & 0xff,
        device >> 16 & 0xff,
        device >> 8 & 0xff,
        device & 0xff
    )
}

/// The catalog in send order: gateway by gateway, then fleet-wide series.
fn fleet_catalog(devices: usize) -> Vec<Series> {
    const IF_COUNTERS: [&str; 4] = [
        "majordomo_if_in_octets",
        "majordomo_if_out_octets",
        "majordomo_if_in_errors",
        "majordomo_if_out_errors",
    ];
    const DEVICE_GAUGES: [&str; 3] = [
        "majordomo_device_reachable",
        "majordomo_wifi_contention_available",
        "majordomo_wifi_health_available",
    ];
    const RADIO_ERRORS: [&str; 3] = [
        "majordomo_wifi_radio_fcs_errors",
        "majordomo_wifi_radio_frames_retransmitted",
        "majordomo_wifi_radio_noise_dbm",
    ];
    const DEVICE_DETAIL: [&str; 9] = [
        "majordomo_device_inform_age_seconds",
        "majordomo_device_informing",
        "majordomo_device_max_rssi",
        "majordomo_device_neighbors",
        "majordomo_device_scanned",
        "majordomo_docsis_down_power_dbmv",
        "majordomo_docsis_down_snr_db",
        "majordomo_docsis_uncorrectables",
        "majordomo_docsis_up_power_dbmv",
    ];
    const FLEET: [&str; 6] = [
        "majordomo_fleet_adjacencies",
        "majordomo_fleet_changes",
        "majordomo_fleet_congested",
        "majordomo_fleet_cwmp_reachable",
        "majordomo_fleet_managed",
        "majordomo_fleet_scanned",
    ];
    const IF_TYPES: [&str; 4] = ["ethernetCsmacd", "ieee80211", "docsCableMaclayer", "bridge"];
    const ROLES: [&str; 4] = ["lan", "wan", "wifi", "management"];

    let mut catalog = Vec::with_capacity(devices * 100);
    for device in 0..devices {
        let cm = mac(device);
        for name in DEVICE_GAUGES {
            catalog.push(Series {
                name,
                labels: format!("{{\"cm_mac\":\"{cm}\"}}"),
                report_per_mille: 1000,
                retires: false,
                value: Value::One,
            });
        }
        for interface in 0..COUNTER_INTERFACES + INFO_ONLY_INTERFACES {
            let interface_id = format!("if:{}", 10_000 + interface);
            let if_type = IF_TYPES[interface % IF_TYPES.len()];
            let role = ROLES[(device + interface) % ROLES.len()];
            catalog.push(Series {
                name: "majordomo_if_info",
                labels: format!(
                    "{{\"alias\":\"{role}-port-{interface}\",\"cm_mac\":\"{cm}\",\
                     \"display_name\":\"{if_type} {interface} on {cm}\",\
                     \"if_type\":\"{if_type}\",\"interface_id\":\"{interface_id}\",\
                     \"mtu\":\"1500\",\"role\":\"{role}\",\"speed\":\"1000000000\"}}"
                ),
                report_per_mille: 830,
                retires: false,
                value: Value::One,
            });
            if interface >= COUNTER_INTERFACES {
                continue;
            }
            for (index, name) in IF_COUNTERS.into_iter().enumerate() {
                let key = mix((device * 64 + interface) as u64 * 4 + index as u64);
                catalog.push(Series {
                    name,
                    labels: format!("{{\"cm_mac\":\"{cm}\",\"interface_id\":\"{interface_id}\"}}"),
                    report_per_mille: 820,
                    retires: key % 1000 < RETIRED_PER_MILLE,
                    value: if index < 2 {
                        Value::Counter {
                            rate: 1.0e5 + (key % 50_000_000) as f64,
                        }
                    } else {
                        Value::Sparse
                    },
                });
            }
        }
        let radios = if device % 10 == 0 { 2 } else { 3 };
        for radio in 0..radios {
            let labels = format!("{{\"cm_mac\":\"{cm}\",\"interface_id\":\"radio:{radio}\"}}");
            for (name, value) in [
                (
                    "majordomo_wifi_radio_packets_sent",
                    Value::Counter { rate: 9_000.0 },
                ),
                (
                    "majordomo_wifi_radio_packets_received",
                    Value::Counter { rate: 12_000.0 },
                ),
                ("majordomo_if_admin_status", Value::Status),
                ("majordomo_if_oper_status", Value::Status),
            ] {
                catalog.push(Series {
                    name,
                    labels: labels.clone(),
                    report_per_mille: 640,
                    retires: false,
                    value,
                });
            }
            if device % 16 == 0 && radio == 0 {
                for name in RADIO_ERRORS {
                    catalog.push(Series {
                        name,
                        labels: labels.clone(),
                        report_per_mille: 950,
                        retires: false,
                        value: if name.ends_with("noise_dbm") {
                            Value::Level { base: -91.0 }
                        } else {
                            Value::Sparse
                        },
                    });
                }
            }
        }
        if device < 4 {
            for name in DEVICE_DETAIL {
                catalog.push(Series {
                    name,
                    labels: format!("{{\"cm_mac\":\"{cm}\"}}"),
                    report_per_mille: 1000,
                    retires: false,
                    value: Value::Level { base: 40.0 },
                });
            }
        }
    }
    for name in FLEET {
        catalog.push(Series {
            name,
            labels: "{}".into(),
            report_per_mille: 1000,
            retires: false,
            value: Value::Level { base: 4.0 },
        });
    }
    catalog
}

fn reports(ordinal: usize, series: &Series, round: usize) -> bool {
    if series.retires && round >= RETIRE_AFTER_ROUNDS {
        return false;
    }
    mix(((ordinal as u64) << 20) ^ round as u64) % 1000 < series.report_per_mille
}

fn value(ordinal: usize, series: &Series, round: usize) -> f64 {
    let noise = mix(((ordinal as u64) << 24) ^ (round as u64) ^ 0x7661_6c75);
    match series.value {
        Value::Counter { rate } => {
            (rate * round as f64 * (0.9 + (noise % 200) as f64 / 1000.0)).floor()
        }
        Value::Sparse => (round as u64 * (noise % 3) / 2) as f64,
        Value::Status => {
            if noise.is_multiple_of(500) {
                2.0
            } else {
                1.0
            }
        }
        Value::One => 1.0,
        Value::Level { base } => base + (noise % 41) as f64 / 10.0 - 2.0,
    }
}

/// One named batch: one point each for the catalog ordinals in `members`.
fn encode_batch(catalog: &[Series], members: &[usize], round: usize, slot_ts: i64) -> Vec<u8> {
    let count = members.len();
    let mut blob = Vec::with_capacity(12 + count * (128 + 20));
    blob.push(0x01);
    blob.push(0);
    blob.extend_from_slice(&0u16.to_le_bytes());
    blob.extend_from_slice(&(count as u32).to_le_bytes());
    blob.extend_from_slice(&(count as u32).to_le_bytes());
    for &ordinal in members {
        let series = &catalog[ordinal];
        blob.extend_from_slice(&(series.name.len() as u32).to_le_bytes());
        blob.extend_from_slice(series.name.as_bytes());
        blob.extend_from_slice(&(series.labels.len() as u32).to_le_bytes());
        blob.extend_from_slice(series.labels.as_bytes());
    }
    for index in 0..count {
        blob.extend_from_slice(&(index as u32).to_le_bytes());
    }
    for _ in 0..count {
        blob.extend_from_slice(&slot_ts.to_le_bytes());
    }
    for &ordinal in members {
        let v = value(ordinal, &catalog[ordinal], round);
        blob.extend_from_slice(&v.to_bits().to_le_bytes());
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
    devices: usize,
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
    let catalog = fleet_catalog(devices);
    eprintln!(
        "fleet: {devices} gateways, {} series, {rounds} rounds of {SLOT_SECS} s",
        catalog.len()
    );
    let mut total_points = 0_usize;

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
        let members: Vec<usize> = catalog
            .iter()
            .enumerate()
            .filter(|(ordinal, series)| reports(*ordinal, series, round))
            .map(|(ordinal, _)| ordinal)
            .collect();
        let points = members.len();
        total_points += points;
        let batches: Vec<(Vec<u8>, usize)> = members
            .chunks(IMPORT_BATCH_POINTS)
            .map(|chunk| (encode_batch(&catalog, chunk, round, slot_ts), chunk.len()))
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
            points,
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
            "round {round:>3}: {points:>6} pts, sweep {:>9.1} ms ({:>9.1} ms cpu, {:>6} steps, {:>7.2} ms/step), \
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
            devices,
            series: catalog.len(),
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
    let devices = parse_positive(args.next(), DEFAULT_DEVICES, "DEVICES");
    let rounds = parse_positive(args.next(), DEFAULT_ROUNDS, "ROUNDS");
    let result = match (extension, database, devices, rounds) {
        (Some(extension), Some(database), Ok(devices), Ok(rounds)) if args.next().is_none() => {
            run(extension, database, devices, rounds).await
        }
        (_, _, Err(error), _) | (_, _, _, Err(error)) => Err(error),
        _ => Err(
            "usage: metrics_sparse_compaction_bench EXTENSION DATABASE [DEVICES] [ROUNDS]".into(),
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
