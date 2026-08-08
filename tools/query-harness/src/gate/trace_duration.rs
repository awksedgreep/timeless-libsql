use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Instant;

use anyhow::{bail, ensure, Context, Result};
use rusqlite::types::Value;
use rusqlite::{params, Connection, Statement};
use serde_json::json;

fn validate_table(table: &str) -> Result<()> {
    ensure!(!table.is_empty(), "trace table name must not be empty");
    ensure!(
        table
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "trace table name must contain only ASCII letters, digits, or underscore"
    );
    Ok(())
}

fn integer_stats(connection: &Connection, table: &str) -> Result<BTreeMap<String, i64>> {
    let mut statement = connection.prepare("SELECT key,value FROM timeless_stats(?1)")?;
    let rows = statement.query_map([table], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Value>(1)?))
    })?;
    let mut values = BTreeMap::new();
    for row in rows {
        let (key, value) = row?;
        if let Value::Integer(value) = value {
            values.insert(key, value);
        }
    }
    Ok(values)
}

fn stat(values: &BTreeMap<String, i64>, key: &str) -> Result<i64> {
    values
        .get(key)
        .copied()
        .with_context(|| format!("timeless_stats omitted integer key {key:?}"))
}

fn work_delta(
    before: &BTreeMap<String, i64>,
    after: &BTreeMap<String, i64>,
) -> BTreeMap<String, i64> {
    after
        .iter()
        .filter(|(key, _)| key.starts_with("query_"))
        .filter_map(|(key, value)| {
            before
                .get(key)
                .map(|old| (key.clone(), value.saturating_sub(*old)))
        })
        .collect()
}

fn physical_bytes(database: &Path) -> u64 {
    let mut total = fs::metadata(database).map_or(0, |metadata| metadata.len());
    let base = database.as_os_str().to_string_lossy();
    for suffix in ["-wal", "-shm"] {
        total = total.saturating_add(
            fs::metadata(format!("{base}{suffix}")).map_or(0, |metadata| metadata.len()),
        );
    }
    total
}

fn storage_files(database: &Path) -> serde_json::Value {
    let base = database.as_os_str().to_string_lossy();
    let main = fs::metadata(database).map_or(0, |metadata| metadata.len());
    let wal = fs::metadata(format!("{base}-wal")).map_or(0, |metadata| metadata.len());
    let shm = fs::metadata(format!("{base}-shm")).map_or(0, |metadata| metadata.len());
    json!({
        "main": main,
        "wal": wal,
        "shm": shm,
        "total": main.saturating_add(wal).saturating_add(shm),
    })
}

fn rss_hwm_kib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmHWM:")?;
        value.split_whitespace().next()?.parse().ok()
    })
}

fn consume_miss(
    statement: &mut Statement<'_>,
    service: &str,
    minimum_duration_ns: i64,
) -> Result<usize> {
    let mut rows = statement.query(params![service, minimum_duration_ns])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        // Force the complete public row projection if a supposedly impossible
        // threshold unexpectedly matches.
        for column in 0..14 {
            let _ = row.get_ref(column)?;
        }
        count += 1;
    }
    Ok(count)
}

fn percentile(sorted_ns: &[u128], percentile: usize) -> f64 {
    let index = (sorted_ns.len() - 1).saturating_mul(percentile) / 100;
    sorted_ns[index] as f64 / 1_000_000.0
}

fn measure(
    statement: &mut Statement<'_>,
    service: &str,
    minimum_duration_ns: i64,
    iterations: usize,
) -> Result<serde_json::Value> {
    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let rows = consume_miss(statement, service, minimum_duration_ns)?;
        ensure!(rows == 0, "duration evidence threshold matched {rows} rows");
        latencies.push(started.elapsed().as_nanos());
    }
    latencies.sort_unstable();
    let mean_ms = latencies.iter().sum::<u128>() as f64 / latencies.len() as f64 / 1_000_000.0;
    Ok(json!({
        "iterations": iterations,
        "rows": 0,
        "p50_ms": percentile(&latencies, 50),
        "p95_ms": percentile(&latencies, 95),
        "p99_ms": percentile(&latencies, 99),
        "mean_ms": mean_ms,
    }))
}

pub(super) struct Options<'a> {
    pub(super) extension: &'a Path,
    pub(super) database: &'a Path,
    pub(super) table: &'a str,
    pub(super) service: &'a str,
    pub(super) iterations: usize,
    pub(super) warmup: usize,
    pub(super) minimum_duration_ns: i64,
    pub(super) wal: bool,
}

pub(super) fn run(options: Options<'_>) -> Result<()> {
    let Options {
        extension,
        database,
        table,
        service,
        iterations,
        warmup,
        minimum_duration_ns,
        wal,
    } = options;
    validate_table(table)?;
    ensure!(iterations > 0, "--iterations must be positive");
    ensure!(database.is_file(), "database does not exist: {}", database.display());

    let connection = super::open(extension, database)?;
    if wal {
        let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        ensure!(mode.eq_ignore_ascii_case("wal"), "failed to enable WAL mode");
    }
    let query = format!(
        "SELECT trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,\
         duration_ns,attributes,status_description,events,resource,instrumentation_scope \
         FROM \"{table}\" WHERE service=?1 AND duration_ns>=?2 \
         ORDER BY start_ts DESC,span_id DESC LIMIT 20"
    );
    let command = format!("INSERT INTO \"{table}\"(\"{table}\") VALUES('optimize')");

    let initial = integer_stats(&connection, table)?;
    ensure!(stat(&initial, "total_spans")? > 0, "trace fixture is empty");
    ensure!(
        stat(&initial, "duration_unknown_blocks")? > 0,
        "trace fixture has no legacy duration metadata to backfill"
    );
    let physical_before = physical_bytes(database);
    let files_before = storage_files(database);
    let journal_mode: String =
        connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;

    let mut statement = connection.prepare(&query)?;
    for _ in 0..warmup {
        ensure!(
            consume_miss(&mut statement, service, minimum_duration_ns)? == 0,
            "duration evidence threshold is not a miss"
        );
    }
    let before_legacy = integer_stats(&connection, table)?;
    let legacy = measure(&mut statement, service, minimum_duration_ns, iterations)?;
    let after_legacy = integer_stats(&connection, table)?;
    let legacy_hwm_kib = rss_hwm_kib();
    drop(statement);

    let optimize_started = Instant::now();
    connection.execute(&command, [])?;
    let optimize_ms = optimize_started.elapsed().as_secs_f64() * 1_000.0;
    let after_optimize = integer_stats(&connection, table)?;
    ensure!(
        stat(&after_optimize, "duration_unknown_blocks")? == 0,
        "public optimize left legacy duration metadata behind"
    );
    ensure!(
        stat(&after_optimize, "duration_bounded_blocks")?
            == stat(&after_optimize, "blocks")?,
        "not every persisted trace block has duration bounds after optimize"
    );

    let mut statement = connection.prepare(&query)?;
    for _ in 0..warmup {
        ensure!(consume_miss(&mut statement, service, minimum_duration_ns)? == 0);
    }
    let before_bounded = integer_stats(&connection, table)?;
    let bounded = measure(&mut statement, service, minimum_duration_ns, iterations)?;
    let after_bounded = integer_stats(&connection, table)?;
    let bounded_hwm_kib = rss_hwm_kib();
    drop(statement);
    let files_after_optimize = storage_files(database);
    let checkpoint_started = Instant::now();
    let checkpoint: (i64, i64, i64) = connection.query_row(
        "PRAGMA wal_checkpoint(TRUNCATE)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let checkpoint_ms = checkpoint_started.elapsed().as_secs_f64() * 1_000.0;
    let files_after_checkpoint = storage_files(database);

    let report = json!({
        "database": database.display().to_string(),
        "table": table,
        "service": service,
        "minimum_duration_ns": minimum_duration_ns,
        "fixture": {
            "spans": stat(&after_optimize, "total_spans")?,
            "blocks_before": stat(&initial, "blocks")?,
            "unknown_blocks_before": stat(&initial, "duration_unknown_blocks")?,
            "blocks_after": stat(&after_optimize, "blocks")?,
            "bounded_blocks_after": stat(&after_optimize, "duration_bounded_blocks")?,
            "logical_payload_bytes": stat(&after_optimize, "bytes_on_disk")?,
            "physical_bytes_before": physical_before,
            "physical_bytes_after": physical_bytes(database),
            "journal_mode": journal_mode,
            "files_before": files_before,
            "files_after_optimize": files_after_optimize,
            "files_after_checkpoint": files_after_checkpoint,
            "checkpoint": {
                "busy": checkpoint.0,
                "log_frames": checkpoint.1,
                "checkpointed_frames": checkpoint.2,
                "elapsed_ms": checkpoint_ms,
            },
        },
        "legacy_decode_fallback": legacy,
        "legacy_work": work_delta(&before_legacy, &after_legacy),
        "optimized_duration_bounds": bounded,
        "bounded_work": work_delta(&before_bounded, &after_bounded),
        "optimize": {
            "elapsed_ms": optimize_ms,
            "backfill_blocks": stat(&after_optimize, "optimize_duration_backfill_blocks")?,
            "backfill_entries": stat(&after_optimize, "optimize_duration_backfill_entries")?,
            "backfill_input_bytes": stat(&after_optimize, "optimize_duration_backfill_input_bytes")?,
            "backfill_total_ns": stat(&after_optimize, "optimize_duration_backfill_total_ns")?,
        },
        "rss_hwm_kib": {
            "after_legacy": legacy_hwm_kib,
            "after_bounded": bounded_hwm_kib,
        }
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    if stat(&after_legacy, "query_candidate_blocks")?
        == stat(&before_legacy, "query_candidate_blocks")?
        || stat(&after_legacy, "query_decoded_spans")?
            == stat(&before_legacy, "query_decoded_spans")?
    {
        bail!("legacy duration evidence did not decode candidate blocks; check --service and fixture");
    }
    if stat(&after_bounded, "query_candidate_blocks")?
        != stat(&before_bounded, "query_candidate_blocks")?
        || stat(&after_bounded, "query_decoded_spans")?
            != stat(&before_bounded, "query_decoded_spans")?
    {
        bail!("duration-bounded miss still considered or decoded persisted blocks");
    }
    Ok(())
}
