use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use timeless_logs_storage_poc::{LogEntry, StorageConfig, StorageSnapshot, StorageWorker};

const BASE_TS: i64 = 1_700_000_000_000;

fn main() {
    if let Err(error) = run() {
        eprintln!("logs-lifecycle: FAIL: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let extension_path = env::args().nth(1).ok_or_else(|| {
        "usage: logs-lifecycle <path-to-libtimeless_ext.so> [database-path]".to_string()
    })?;
    if !Path::new(&extension_path).exists() {
        return Err(format!("extension does not exist: {extension_path}"));
    }
    let database_path = env::args()
        .nth(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/timeless_logs_storage_lifecycle.db"));
    scrub(&database_path);

    let config = StorageConfig {
        database_path: database_path.clone(),
        extension_path: PathBuf::from(extension_path),
        queue_batches: 32,
        queue_entries: 50_000,
        flush_entries: 1_000,
        flush_interval: Duration::from_millis(200),
        // Automatic maintenance is intentionally kept out of the way while
        // each transition is inspected. optimize_once uses the identical
        // bounded command the timer invokes.
        optimize_interval: Duration::from_secs(60),
        optimize_raw_entries: 500,
        optimize_max_raw_age: Duration::from_secs(60),
        optimize_entry_budget: 500,
    };

    let mut all = Vec::new();
    let worker = StorageWorker::start(config.clone())?;

    let low_volume = generate(0, 100);
    all.extend(low_volume.iter().cloned());
    assert_eq!(worker.enqueue(low_volume)?, 100);
    let buffered = worker.snapshot()?;
    check_counts("buffered", &buffered, &all)?;
    require(
        buffered.buffered_entries == 100,
        "low-volume batch was not buffered",
    )?;
    require(
        buffered.disk_entries == 0,
        "low-volume batch flushed per request",
    )?;
    require(
        buffered.raw_blocks == 0,
        "raw block exists before timer/threshold",
    )?;
    require(
        buffered.admitted_entries == 100,
        "admission credits were released before raw durability",
    )?;

    thread::sleep(Duration::from_millis(275));
    let timer_raw = worker.snapshot()?;
    check_counts("timer raw", &timer_raw, &all)?;
    require(
        timer_raw.buffered_entries == 0,
        "timer did not drain the buffer",
    )?;
    require(
        timer_raw.raw_entries == 100,
        "timer flush did not produce raw entries",
    )?;
    require(
        timer_raw.compressed_entries == 0,
        "timer flush compressed on the hot path",
    )?;
    require(
        timer_raw.admitted_entries == 0,
        "raw timer flush did not release admission credits",
    )?;

    for start in [100usize, 1_100, 2_100] {
        let batch = generate(start, 1_000);
        all.extend(batch.iter().cloned());
        assert_eq!(worker.enqueue(batch)?, 1_000);
        let threshold = worker.snapshot()?;
        check_counts("threshold raw", &threshold, &all)?;
        require(
            threshold.buffered_entries == 0,
            "aggregate threshold did not drain the extension buffer",
        )?;
        require(
            threshold.compressed_entries == 0,
            "ingest threshold compressed entries synchronously",
        )?;
    }

    let raw = worker.snapshot()?;
    let first_pass = worker.optimize_once()?;
    check_counts("first bounded optimize", &first_pass, &all)?;
    require(
        first_pass.raw_entries > 0 && first_pass.raw_entries < raw.raw_entries,
        "one bounded optimize pass did not reduce—but preserve—raw debt",
    )?;
    require(
        first_pass.compressed_entries > 0,
        "one bounded optimize pass wrote no compressed entries",
    )?;

    let mut optimized = first_pass;
    for _ in 0..16 {
        if optimized.raw_entries == 0 {
            break;
        }
        optimized = worker.optimize_once()?;
    }
    check_counts("compressed steady state", &optimized, &all)?;
    require(
        optimized.raw_entries == 0,
        "bounded passes did not drain raw debt",
    )?;
    require(
        optimized.compressed_entries == all.len() as i64,
        "compressed steady state has the wrong entry count",
    )?;
    require(
        optimized.codec5_blocks > 0,
        "steady-state blocks are not codec 5",
    )?;
    worker.shutdown()?;

    let reopened = StorageWorker::start(config.clone())?;
    let after_reopen = reopened.snapshot()?;
    check_counts("cold reopen", &after_reopen, &all)?;
    require(
        after_reopen.raw_entries == 0,
        "cold reopen changed codec state",
    )?;

    let shutdown_tail = generate(all.len(), 10);
    all.extend(shutdown_tail.iter().cloned());
    reopened.enqueue(shutdown_tail)?;
    let before_shutdown = reopened.snapshot()?;
    require(
        before_shutdown.buffered_entries == 10,
        "graceful-shutdown tail was not buffered first",
    )?;
    reopened.shutdown()?;

    let final_worker = StorageWorker::start(config.clone())?;
    let graceful = final_worker.snapshot()?;
    check_counts("graceful reopen", &graceful, &all)?;
    require(
        graceful.buffered_entries == 0,
        "reopen recovered an in-memory buffer",
    )?;
    require(
        graceful.raw_entries == 10,
        "graceful shutdown did not flush its tail as raw",
    )?;
    let mut final_state = final_worker.optimize_once()?;
    for _ in 0..16 {
        if final_state.raw_entries == 0 {
            break;
        }
        final_state = final_worker.optimize_once()?;
    }
    require(
        final_state.raw_entries == 0,
        "final bounded optimize left raw debt",
    )?;
    final_worker.shutdown()?;

    // Entry credits, not channel dequeue, are the backpressure boundary.
    // Fill all credits below the size threshold, then prove another producer
    // waits until the low-volume timer makes the first batch raw/durable.
    let backpressure_path = PathBuf::from(format!("{}.backpressure", database_path.display()));
    scrub(&backpressure_path);
    let mut backpressure_config = config.clone();
    backpressure_config.database_path = backpressure_path;
    backpressure_config.queue_entries = 100;
    backpressure_config.flush_entries = 1_000;
    backpressure_config.flush_interval = Duration::from_millis(300);
    let backpressure_worker = StorageWorker::start(backpressure_config)?;
    backpressure_worker.enqueue(generate(0, 100))?;
    let credits_full = backpressure_worker.snapshot()?;
    require(
        credits_full.admitted_entries == 100 && credits_full.buffered_entries == 100,
        "entry-credit backpressure fixture did not fill admission",
    )?;
    let blocked_at = Instant::now();
    backpressure_worker.enqueue(generate(100, 1))?;
    let blocked_for = blocked_at.elapsed();
    require(
        blocked_for >= Duration::from_millis(150),
        "producer admission did not wait for raw durability",
    )?;
    let credits_released = backpressure_worker.snapshot()?;
    require(
        credits_released.raw_entries == 100
            && credits_released.buffered_entries == 1
            && credits_released.admitted_entries == 1,
        "raw flush did not release exactly the durable admission credits",
    )?;
    backpressure_worker.shutdown()?;

    // Separate database: prove the worker's recurring maintenance timer
    // invokes the same bounded command on raw AGE, even below the entry-debt
    // threshold. The main lifecycle above used manual ticks so every mixed
    // state remained observable and deterministic.
    let automatic_path = PathBuf::from(format!("{}.automatic", database_path.display()));
    scrub(&automatic_path);
    let mut automatic_config = config;
    automatic_config.database_path = automatic_path;
    automatic_config.flush_entries = 100;
    automatic_config.flush_interval = Duration::from_secs(1);
    automatic_config.optimize_interval = Duration::from_millis(100);
    automatic_config.optimize_raw_entries = 10_000;
    automatic_config.optimize_max_raw_age = Duration::from_millis(50);
    let automatic_entries = generate(0, 100);
    let automatic_worker = StorageWorker::start(automatic_config)?;
    automatic_worker.enqueue(automatic_entries.clone())?;
    let automatic_raw = automatic_worker.snapshot()?;
    require(
        automatic_raw.raw_entries == 100 && automatic_raw.compressed_entries == 0,
        "automatic-maintenance fixture did not begin in raw state",
    )?;
    thread::sleep(Duration::from_millis(175));
    let automatic_compressed = automatic_worker.snapshot()?;
    check_counts(
        "age-triggered background optimize",
        &automatic_compressed,
        &automatic_entries,
    )?;
    require(
        automatic_compressed.raw_entries == 0 && automatic_compressed.compressed_entries == 100,
        "background raw-age maintenance did not reach compressed state",
    )?;
    automatic_worker.shutdown()?;

    println!("# timeless logs storage lifecycle\n");
    println!("| stage | admitted | buffered | raw entries | compressed entries | raw blocks | compressed blocks | total | raw bytes | compressed bytes |");
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for (label, snapshot) in [
        ("accepted / buffered", buffered),
        ("timer flush / raw", timer_raw),
        ("raw debt", raw),
        ("one bounded optimize", first_pass),
        ("compressed steady state", optimized),
        ("cold reopen", after_reopen),
        ("graceful shutdown + reopen", graceful),
        ("entry-credit backpressure", credits_released),
        ("background age optimize", automatic_compressed),
    ] {
        print_row(label, snapshot);
    }
    println!("\nPASS — no HTTP or auth involved; exact queries agree in every storage state.");
    Ok(())
}

fn generate(start: usize, count: usize) -> Vec<LogEntry> {
    (start..start + count)
        .map(|i| {
            let (level, level_name) = match i % 20 {
                0 => (3, "error"),
                1 | 2 => (2, "warning"),
                3..=5 => (0, "debug"),
                _ => (1, "info"),
            };
            let service = ["api", "web", "worker", "billing"][i % 4];
            let message = if level == 3 {
                format!("request {i} failed: upstream timeout")
            } else {
                format!("{level_name} request {i} completed")
            };
            LogEntry {
                ts: BASE_TS + i as i64 * 3,
                level,
                message,
                metadata_json: format!(
                    "{{\"path\":\"/items\",\"service\":\"{service}\",\"status\":\"{}\"}}",
                    if level == 3 { "500" } else { "200" }
                ),
            }
        })
        .collect()
}

fn check_counts(stage: &str, got: &StorageSnapshot, entries: &[LogEntry]) -> Result<(), String> {
    let errors = entries.iter().filter(|entry| entry.level == 3).count() as i64;
    let api_errors = entries
        .iter()
        .filter(|entry| entry.level == 3 && entry.metadata_json.contains("\"service\":\"api\""))
        .count() as i64;
    let timeouts = entries
        .iter()
        .filter(|entry| entry.message.contains("timeout"))
        .count() as i64;
    require(
        got.total_entries == entries.len() as i64,
        &format!("{stage}: total query count mismatch"),
    )?;
    require(
        got.error_entries == errors,
        &format!("{stage}: level query mismatch"),
    )?;
    require(
        got.api_error_entries == api_errors,
        &format!("{stage}: indexed service+level query mismatch"),
    )?;
    require(
        got.timeout_entries == timeouts,
        &format!("{stage}: LIKE query mismatch"),
    )?;
    require(
        got.buffered_entries + got.disk_entries == entries.len() as i64,
        &format!("{stage}: lifecycle entry accounting mismatch"),
    )
}

fn require(condition: bool, message: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(message.to_string())
    }
}

fn print_row(label: &str, s: StorageSnapshot) {
    println!(
        "| {label} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
        s.admitted_entries,
        s.buffered_entries,
        s.raw_entries,
        s.compressed_entries,
        s.raw_blocks,
        s.compressed_blocks,
        s.total_entries,
        s.raw_bytes,
        s.compressed_bytes
    );
}

fn scrub(path: &Path) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let candidate = PathBuf::from(format!("{}{suffix}", path.display()));
        let _ = fs::remove_file(candidate);
    }
}
