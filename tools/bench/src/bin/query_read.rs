//! Direct SQLite-extension read benchmark used by QUERY_PERFORMANCE_PLAN.md.
//!
//!   cargo run --release --bin query-read -- EXT [--series N] [--points N] [--runs N]
//!
//! The output is comment-prefixed metadata followed by CSV. It deliberately
//! uses only the public loadable-extension SQL surface so every measurement is
//! representative of a direct SQLite/libSQL user, not an internal Rust call.

use std::env;
use std::fs;
use std::path::Path;
use std::time::Instant;

use rusqlite::{params, Connection, Statement};

const METRIC: &str = "query_hot_metric";
const BASE_TS: i64 = 1_700_000_000;

#[derive(Clone, Copy)]
struct Config {
    series: usize,
    points: usize,
    runs: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Outcome {
    series: usize,
    points: usize,
    bytes: usize,
    checksum: u64,
}

struct Stats {
    median_us: u128,
    p95_us: u128,
    min_us: u128,
    max_us: u128,
    runs: usize,
    outcome: Outcome,
}

fn main() {
    let (ext, config) = parse_args();
    assert!(Path::new(&ext).is_file(), "extension not found at {ext}");
    assert!(config.series > 0, "--series must be positive");
    assert!(config.points > 0, "--points must be positive");
    assert!(config.runs > 0, "--runs must be positive");

    let total_points = config
        .series
        .checked_mul(config.points)
        .expect("dataset point count overflow");
    assert!(
        u32::try_from(config.series).is_ok(),
        "series count exceeds batch format"
    );
    assert!(
        u32::try_from(total_points).is_ok(),
        "point count exceeds batch format"
    );

    let temporary = tempfile::Builder::new()
        .prefix("timeless-query-read-")
        .tempdir()
        .expect("create query-read scratch directory");
    let db_path = temporary
        .path()
        .join("query-read.db")
        .to_string_lossy()
        .into_owned();
    scrub(&db_path);

    let writer = open_with_ext(&db_path, &ext);
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout=5000;
             CREATE VIRTUAL TABLE metrics USING timeless_metrics(rollups='10@0');",
        )
        .expect("create metrics vtab");

    // Open and touch the reader before publication. This reproduces the
    // long-lived pooled-reader shape used by embedded hosts.
    let reader = open_with_ext(&db_path, &ext);
    let _: i64 = reader
        .query_row(
            "SELECT COUNT(*) FROM timeless_series('metrics')",
            [],
            |row| row.get(0),
        )
        .expect("prime reader catalog");

    let blob = encode_named_batch(config);
    let ingest_started = Instant::now();
    writer
        .execute_batch("BEGIN IMMEDIATE")
        .expect("begin ingest");
    writer
        .execute("INSERT INTO metrics(metrics) VALUES (?1)", params![blob])
        .expect("insert named batch");
    writer.execute_batch("COMMIT").expect("commit ingest");
    let ingest_us = ingest_started.elapsed().as_micros();

    let flush_started = Instant::now();
    writer
        .execute("INSERT INTO metrics(metrics) VALUES ('flush')", [])
        .expect("flush metrics");
    let flush_us = flush_started.elapsed().as_micros();

    let start = BASE_TS;
    let stop = BASE_TS + config.points as i64 - 1;
    let exact_filter = "{\"host\":\"device_1\"}";
    let narrow_filter = "{\"service\":\"svc_1\"}";
    let wide_filter = "{\"env\":\"prod\"}";
    let selective_regex_filter = "{\"host\":{\"re\":\"device_1\"}}";
    let selective_negative_filter = "{\"region\":{\"neq\":\"us-east\"}}";
    let raw_sql =
        "SELECT series_id, labels, points FROM timeless_raw_batches('metrics', ?1, ?2, ?3, ?4)";

    let mut exact_stmt = reader.prepare(raw_sql).expect("prepare exact raw batches");
    let first = measure(1, || {
        consume_raw(&mut exact_stmt, exact_filter, start, stop, false)
    });
    let exact_expected = Outcome {
        series: 1,
        points: config.points,
        bytes: first.outcome.bytes,
        checksum: first.outcome.checksum,
    };
    assert_eq!(
        first.outcome, exact_expected,
        "unexpected exact result cardinality"
    );

    // Empty metric: exercises the complete warm refresh fast path (TVF
    // dispatch, one authoritative generation SELECT, token comparison, and
    // empty candidate selection) without reading or decoding a chunk. This is
    // a conservative upper bound for Session 6's <=10us generation-check gate.
    let mut generation_stmt = reader
        .prepare(
            "SELECT COUNT(*) FROM timeless_raw_batches(
              'metrics', '__timeless_missing_metric__', NULL, 0, 0)",
        )
        .expect("prepare warm catalog generation check");
    let refresh_noop = measure(config.runs.max(100), || {
        consume_count(&mut generation_stmt, [])
    });
    assert_eq!(
        refresh_noop.outcome.points, 0,
        "missing metric unexpectedly returned rows"
    );
    // Isolate the actual one-row generation SELECT from TVF dispatch and
    // candidate planning. Session 6's warm <=10us gate applies to this check;
    // warm_refresh_noop is the conservative full empty-query measurement.
    let mut generation_select_stmt = reader
        .prepare(
            "SELECT (SELECT COALESCE(MAX(id), 0) FROM metrics_series) +
                    COALESCE((SELECT v FROM metrics_meta WHERE k = 'chunk_gen'), 0)",
        )
        .expect("prepare isolated catalog generation select");
    let generation_select = measure(config.runs.max(100), || {
        consume_count(&mut generation_select_stmt, [])
    });

    // Warm every shape before sampling. The first publication cost remains a
    // separate metric above.
    let _ = consume_raw(&mut exact_stmt, exact_filter, start, stop, false);
    let exact = measure(config.runs.max(100), || {
        consume_raw(&mut exact_stmt, exact_filter, start, stop, false)
    });

    let mut selective_regex_stmt = reader
        .prepare(raw_sql)
        .expect("prepare selective regex raw batches");
    let selective_regex = measure(config.runs, || {
        consume_raw(
            &mut selective_regex_stmt,
            selective_regex_filter,
            start,
            stop,
            false,
        )
    });
    assert_eq!(selective_regex.outcome.series, 1);
    assert_eq!(selective_regex.outcome.points, config.points);

    let mut selective_negative_stmt = reader
        .prepare(raw_sql)
        .expect("prepare selective negative raw batches");
    let selective_negative = measure(config.runs, || {
        consume_raw(
            &mut selective_negative_stmt,
            selective_negative_filter,
            start,
            stop,
            false,
        )
    });
    let negative_series = config.series.div_ceil(2);
    assert_eq!(selective_negative.outcome.series, negative_series);
    assert_eq!(
        selective_negative.outcome.points,
        negative_series * config.points
    );

    let mut selective_discovery_stmt = reader
        .prepare("SELECT COUNT(*) FROM timeless_series('metrics', ?1, ?2)")
        .expect("prepare selective series discovery");
    let selective_discovery = measure(config.runs.max(100), || {
        consume_count(
            &mut selective_discovery_stmt,
            params![METRIC, selective_regex_filter],
        )
    });
    assert_eq!(selective_discovery.outcome.points, 1);

    let mut selective_label_values_stmt = reader
        .prepare("SELECT COUNT(*) FROM timeless_label_values('metrics', ?1, 'service', ?2)")
        .expect("prepare selective label-value discovery");
    let selective_label_values = measure(config.runs.max(100), || {
        consume_count(
            &mut selective_label_values_stmt,
            params![METRIC, selective_regex_filter],
        )
    });
    assert_eq!(selective_label_values.outcome.points, 1);

    let mut narrow_stmt = reader.prepare(raw_sql).expect("prepare narrow raw batches");
    let narrow_warm = consume_raw(&mut narrow_stmt, narrow_filter, start, stop, false);
    let narrow = measure(config.runs, || {
        consume_raw(&mut narrow_stmt, narrow_filter, start, stop, false)
    });
    assert_eq!(
        narrow.outcome, narrow_warm,
        "narrow result changed between runs"
    );

    let mut wide_stmt = reader.prepare(raw_sql).expect("prepare wide raw batches");
    let wide_warm = consume_raw(&mut wide_stmt, wide_filter, start, stop, false);
    assert_eq!(
        wide_warm.series, config.series,
        "wide filter did not match every series"
    );
    assert_eq!(wide_warm.points, total_points, "wide result lost points");
    let wide = measure(config.runs, || {
        consume_raw(&mut wide_stmt, wide_filter, start, stop, false)
    });

    let raw_frame_sql = "SELECT frame FROM timeless_raw_frame('metrics', ?1, ?2, ?3, ?4)";
    let mut raw_frame_stmt = reader.prepare(raw_frame_sql).expect("prepare raw frame");
    let raw_frame_warm = consume_raw_frame(&mut raw_frame_stmt, wide_filter, start, stop);
    assert_eq!(
        (raw_frame_warm.series, raw_frame_warm.points),
        (config.series, total_points),
        "raw frame lost series or points"
    );
    let raw_frame = measure(config.runs, || {
        consume_raw_frame(&mut raw_frame_stmt, wide_filter, start, stop)
    });

    // Current scalar/latest fallback: transfer every packed raw point and
    // reduce in the host. Future TVFs are compared against these exact rows.
    let mut aggregate_stmt = reader.prepare(raw_sql).expect("prepare aggregate fallback");
    let aggregate_warm = consume_raw(&mut aggregate_stmt, wide_filter, start, stop, true);
    let aggregate_fallback = measure(config.runs, || {
        consume_raw(&mut aggregate_stmt, wide_filter, start, stop, true)
    });
    assert_eq!(
        aggregate_fallback.outcome, aggregate_warm,
        "aggregate fallback changed between runs"
    );

    let native_aggregate_sql = "SELECT series_id, labels, value FROM timeless_aggregate(
      'metrics', ?1, ?2, ?3, ?4, 'avg')";
    let mut native_aggregate_stmt = reader
        .prepare(native_aggregate_sql)
        .expect("prepare native aggregate");
    let native_aggregate_warm =
        consume_aggregate(&mut native_aggregate_stmt, wide_filter, start, stop);
    assert_eq!(
        native_aggregate_warm.series, config.series,
        "native aggregate lost matching series"
    );
    let native_aggregate = measure(config.runs, || {
        consume_aggregate(&mut native_aggregate_stmt, wide_filter, start, stop)
    });

    let aggregate_frame_sql = "SELECT frame FROM timeless_aggregate_frame(
      'metrics', ?1, ?2, ?3, ?4, 'avg')";
    let mut aggregate_frame_stmt = reader
        .prepare(aggregate_frame_sql)
        .expect("prepare aggregate frame");
    let aggregate_frame_warm =
        consume_aggregate_frame(&mut aggregate_frame_stmt, wide_filter, start, stop);
    assert_eq!(
        (
            aggregate_frame_warm.series,
            aggregate_frame_warm.points,
            aggregate_frame_warm.checksum,
        ),
        (
            native_aggregate_warm.series,
            native_aggregate_warm.points,
            native_aggregate_warm.checksum,
        ),
        "aggregate frame differs from native aggregate rows"
    );
    let aggregate_frame = measure(config.runs, || {
        consume_aggregate_frame(&mut aggregate_frame_stmt, wide_filter, start, stop)
    });

    let mut latest_stmt = reader.prepare(raw_sql).expect("prepare latest fallback");
    let latest_warm = consume_latest(&mut latest_stmt, wide_filter, start, stop);
    let latest_fallback = measure(config.runs, || {
        consume_latest(&mut latest_stmt, wide_filter, start, stop)
    });
    assert_eq!(
        latest_fallback.outcome, latest_warm,
        "latest fallback changed between runs"
    );

    let native_latest_sql = "SELECT series_id, labels, ts, value FROM timeless_latest(
      'metrics', ?1, ?2, ?3, ?4)";
    let mut native_latest_stmt = reader
        .prepare(native_latest_sql)
        .expect("prepare native latest");
    let native_latest_warm =
        consume_native_latest(&mut native_latest_stmt, wide_filter, start, stop);
    assert_eq!(
        (
            native_latest_warm.series,
            native_latest_warm.points,
            native_latest_warm.checksum,
        ),
        (latest_warm.series, latest_warm.points, latest_warm.checksum),
        "native latest differs from raw fallback"
    );
    let native_latest = measure(config.runs, || {
        consume_native_latest(&mut native_latest_stmt, wide_filter, start, stop)
    });

    let latest_frame_sql = "SELECT frame FROM timeless_latest_frame(
      'metrics', ?1, ?2, ?3, ?4)";
    let mut latest_frame_stmt = reader
        .prepare(latest_frame_sql)
        .expect("prepare latest frame");
    let latest_frame_warm = consume_latest_frame(&mut latest_frame_stmt, wide_filter, start, stop);
    assert_eq!(
        (
            latest_frame_warm.series,
            latest_frame_warm.points,
            latest_frame_warm.checksum,
        ),
        (
            native_latest_warm.series,
            native_latest_warm.points,
            native_latest_warm.checksum,
        ),
        "latest frame differs from native latest rows"
    );
    let latest_frame = measure(config.runs, || {
        consume_latest_frame(&mut latest_frame_stmt, wide_filter, start, stop)
    });

    let step = 10i64;
    let window = 10i64;
    let grid_sql = "SELECT COUNT(*) FROM timeless_grid(
      'metrics', ?1, ?2, ?3, ?4, ?5, ?6)";
    let mut grid_stmt = reader.prepare(grid_sql).expect("prepare grid kernel");
    let grid = measure(config.runs, || {
        consume_count(
            &mut grid_stmt,
            params![METRIC, wide_filter, start, stop, step, window],
        )
    });

    let window_sql = "SELECT COUNT(*) FROM timeless_window(
      'metrics', ?1, ?2, ?3, ?4, ?5, ?6, 'avg')";
    let mut window_stmt = reader.prepare(window_sql).expect("prepare window kernel");
    let window_stats = measure(config.runs, || {
        consume_count(
            &mut window_stmt,
            params![METRIC, wide_filter, start, stop, step, window],
        )
    });

    let window_batch_sql = "SELECT series_id, buckets FROM timeless_window_batches(
      'metrics', ?1, ?2, ?3, ?4, ?5, ?6, 'avg')";
    let mut window_batch_stmt = reader
        .prepare(window_batch_sql)
        .expect("prepare packed window kernel");
    let window_batch_warm = consume_window_batches(
        &mut window_batch_stmt,
        wide_filter,
        start,
        stop,
        step,
        window,
    );
    assert_eq!(
        window_batch_warm.series, config.series,
        "packed window lost matching series"
    );
    assert_eq!(
        window_batch_warm.points, window_stats.outcome.points,
        "packed and row window cardinality differ"
    );
    let window_batch = measure(config.runs, || {
        consume_window_batches(
            &mut window_batch_stmt,
            wide_filter,
            start,
            stop,
            step,
            window,
        )
    });

    writer
        .execute("INSERT INTO metrics(metrics) VALUES ('rollup')", [])
        .expect("build rollups");
    let rollup_sql = "SELECT COUNT(*) FROM timeless_rollup(
      'metrics', ?1, ?2, 10, ?3, ?4, 'avg')";
    let mut rollup_stmt = reader.prepare(rollup_sql).expect("prepare rollup query");
    let rollup = measure(config.runs, || {
        consume_count(&mut rollup_stmt, params![METRIC, wide_filter, start, stop])
    });

    let rollup_row_sql = "SELECT labels, ts, value FROM timeless_rollup(
      'metrics', ?1, ?2, 10, ?3, ?4, ?5)";
    let mut rollup_row_stmts: Vec<Statement<'_>> = (0..6)
        .map(|_| {
            reader
                .prepare(rollup_row_sql)
                .expect("prepare row rollup query")
        })
        .collect();
    let rollup_row_warm = consume_rollup_rows(&mut rollup_row_stmts, wide_filter, start, stop);
    assert_eq!(
        rollup_row_warm.points, rollup.outcome.points,
        "six row rollup calls lost buckets"
    );
    let rollup_rows = measure(config.runs, || {
        consume_rollup_rows(&mut rollup_row_stmts, wide_filter, start, stop)
    });

    let rollup_batch_sql = "SELECT series_id, buckets FROM timeless_rollup_batches(
      'metrics', ?1, ?2, 10, ?3, ?4)";
    let mut rollup_batch_stmt = reader
        .prepare(rollup_batch_sql)
        .expect("prepare packed rollup query");
    let rollup_batch_warm =
        consume_rollup_batches(&mut rollup_batch_stmt, wide_filter, start, stop);
    assert_eq!(
        rollup_batch_warm.points, rollup.outcome.points,
        "packed rollup call lost buckets"
    );
    let rollup_batches = measure(config.runs, || {
        consume_rollup_batches(&mut rollup_batch_stmt, wide_filter, start, stop)
    });
    let database_bytes = database_bytes(&db_path);

    println!("# benchmark=query-read");
    println!("# extension={ext}");
    println!("# sqlite={}", rusqlite::version());
    println!("# series={}", config.series);
    println!("# points_per_series={}", config.points);
    println!("# total_points={total_points}");
    println!("# runs={}", config.runs);
    println!("# ingest_us={ingest_us}");
    println!("# flush_us={flush_us}");
    println!("# database_bytes={database_bytes}");
    println!("metric,median_us,p95_us,min_us,max_us,runs,result_series,result_points,result_bytes");
    print_stats("first_exact_after_flush", &first);
    print_stats("catalog_generation_select", &generation_select);
    print_stats("warm_refresh_noop", &refresh_noop);
    print_stats("exact_raw_batches", &exact);
    print_stats("selective_regex_raw_batches", &selective_regex);
    print_stats("selective_negative_raw_batches", &selective_negative);
    print_stats("selective_series_discovery", &selective_discovery);
    print_stats("selective_label_values", &selective_label_values);
    print_stats("narrow_raw_batches", &narrow);
    print_stats("wide_raw_batches", &wide);
    print_stats("wide_raw_frame", &raw_frame);
    print_stats("scalar_aggregate_raw_fallback", &aggregate_fallback);
    print_stats("scalar_aggregate_native", &native_aggregate);
    print_stats("scalar_aggregate_frame", &aggregate_frame);
    print_stats("latest_raw_fallback", &latest_fallback);
    print_stats("latest_native", &native_latest);
    print_stats("latest_frame", &latest_frame);
    print_stats("grid_count", &grid);
    print_stats("window_avg_count", &window_stats);
    print_stats("window_avg_batches", &window_batch);
    print_stats("rollup_avg_count", &rollup);
    print_stats("rollup_six_row_aggregates", &rollup_rows);
    print_stats("rollup_all_batches", &rollup_batches);

    drop(rollup_batch_stmt);
    drop(rollup_row_stmts);
    drop(rollup_stmt);
    drop(window_batch_stmt);
    drop(window_stmt);
    drop(grid_stmt);
    drop(latest_frame_stmt);
    drop(native_latest_stmt);
    drop(latest_stmt);
    drop(aggregate_frame_stmt);
    drop(native_aggregate_stmt);
    drop(aggregate_stmt);
    drop(raw_frame_stmt);
    drop(wide_stmt);
    drop(narrow_stmt);
    drop(exact_stmt);
    drop(selective_regex_stmt);
    drop(selective_negative_stmt);
    drop(selective_discovery_stmt);
    drop(selective_label_values_stmt);
    drop(generation_select_stmt);
    drop(generation_stmt);
    drop(reader);
    drop(writer);
    scrub(&db_path);
}

fn parse_args() -> (String, Config) {
    let mut args = env::args().skip(1);
    let ext = args.next().unwrap_or_else(|| {
        eprintln!("usage: query-read EXT [--series N] [--points N] [--runs N]");
        std::process::exit(2);
    });
    let mut config = Config {
        series: 12_000,
        points: 60,
        runs: 20,
    };

    while let Some(flag) = args.next() {
        let value = args
            .next()
            .unwrap_or_else(|| panic!("{flag} requires an integer value"));
        let parsed: usize = value
            .parse()
            .unwrap_or_else(|_| panic!("{flag} expects an integer, got {value:?}"));
        match flag.as_str() {
            "--series" => config.series = parsed,
            "--points" => config.points = parsed,
            "--runs" => config.runs = parsed,
            _ => panic!("unknown argument {flag:?}"),
        }
    }
    (ext, config)
}

fn open_with_ext(path: &str, ext: &str) -> Connection {
    let conn = Connection::open(path).expect("open benchmark database");
    unsafe {
        conn.load_extension_enable()
            .expect("enable extension loading");
        conn.load_extension(ext, None::<&str>)
            .expect("load timeless extension");
    }
    conn.load_extension_disable()
        .expect("disable extension loading");
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .expect("set busy timeout");
    conn
}

fn scrub(path: &str) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = fs::remove_file(format!("{path}{suffix}"));
    }
}

fn database_bytes(path: &str) -> u64 {
    ["", "-wal", "-shm", "-journal"]
        .iter()
        .filter_map(|suffix| fs::metadata(format!("{path}{suffix}")).ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn labels_for(series: usize) -> String {
    let region = if series.is_multiple_of(2) {
        "us-east"
    } else {
        "us-west"
    };
    format!(
        "{{\"env\":\"prod\",\"host\":\"device_{series}\",\"region\":\"{region}\",\"service\":\"svc_{}\"}}",
        series % 64
    )
}

fn encode_named_batch(config: Config) -> Vec<u8> {
    let total_points = config.series * config.points;
    let labels: Vec<String> = (1..=config.series).map(labels_for).collect();
    let series_bytes: usize = labels
        .iter()
        .map(|labels| 4 + METRIC.len() + 4 + labels.len())
        .sum();
    let capacity = 12usize
        .checked_add(series_bytes)
        .and_then(|n| n.checked_add(total_points.checked_mul(20)?))
        .expect("batch allocation size overflow");
    let mut blob = Vec::with_capacity(capacity);

    blob.extend_from_slice(&[0x01, 0x00]);
    blob.extend_from_slice(&0u16.to_le_bytes());
    blob.extend_from_slice(&(config.series as u32).to_le_bytes());
    blob.extend_from_slice(&(total_points as u32).to_le_bytes());

    for labels in &labels {
        blob.extend_from_slice(&(METRIC.len() as u32).to_le_bytes());
        blob.extend_from_slice(METRIC.as_bytes());
        blob.extend_from_slice(&(labels.len() as u32).to_le_bytes());
        blob.extend_from_slice(labels.as_bytes());
    }
    for _point in 0..config.points {
        for series in 0..config.series {
            blob.extend_from_slice(&(series as u32).to_le_bytes());
        }
    }
    for point in 0..config.points {
        for _series in 0..config.series {
            blob.extend_from_slice(&(BASE_TS + point as i64).to_le_bytes());
        }
    }
    for point in 0..config.points {
        for series in 1..=config.series {
            let value = series as f64 * 0.001 + point as f64;
            blob.extend_from_slice(&value.to_bits().to_le_bytes());
        }
    }
    assert_eq!(blob.len(), capacity, "batch encoder size mismatch");
    blob
}

fn consume_raw(
    stmt: &mut Statement<'_>,
    filter: &str,
    start: i64,
    stop: i64,
    aggregate_values: bool,
) -> Outcome {
    let mut rows = stmt
        .query(params![METRIC, filter, start, stop])
        .expect("query raw batches");
    let mut series = 0usize;
    let mut points = 0usize;
    let mut bytes = 0usize;
    let mut checksum = 0u64;
    while let Some(row) = rows.next().expect("step raw batches") {
        let sid: i64 = row.get(0).expect("series id");
        let labels: String = row.get(1).expect("labels");
        let blob: Vec<u8> = row.get(2).expect("points blob");
        let count = validate_point_blob(&blob);
        series += 1;
        points += count;
        bytes += 8 + labels.len() + blob.len();
        checksum = checksum
            .wrapping_add(sid as u64)
            .wrapping_add(labels.len() as u64);
        if aggregate_values {
            let values = &blob[4 + count * 8..];
            let mut sum = 0.0;
            for value in values.chunks_exact(8) {
                sum += f64::from_bits(u64::from_le_bytes(value.try_into().unwrap()));
            }
            let average = if count == 0 { 0.0 } else { sum / count as f64 };
            checksum = checksum.wrapping_add(average.to_bits());
        } else {
            checksum = checksum.wrapping_add(blob.len() as u64);
        }
    }
    Outcome {
        series,
        points,
        bytes,
        checksum,
    }
}

fn consume_latest(stmt: &mut Statement<'_>, filter: &str, start: i64, stop: i64) -> Outcome {
    let mut rows = stmt
        .query(params![METRIC, filter, start, stop])
        .expect("query latest fallback");
    let mut series = 0usize;
    let mut points = 0usize;
    let mut bytes = 0usize;
    let mut checksum = 0u64;
    while let Some(row) = rows.next().expect("step latest fallback") {
        let sid: i64 = row.get(0).expect("series id");
        let labels: String = row.get(1).expect("labels");
        let blob: Vec<u8> = row.get(2).expect("points blob");
        let count = validate_point_blob(&blob);
        if count > 0 {
            let ts_offset = 4 + (count - 1) * 8;
            let val_offset = 4 + count * 8 + (count - 1) * 8;
            let ts = i64::from_le_bytes(blob[ts_offset..ts_offset + 8].try_into().unwrap());
            let value_bits =
                u64::from_le_bytes(blob[val_offset..val_offset + 8].try_into().unwrap());
            checksum = checksum
                .wrapping_add(sid as u64)
                .wrapping_add(ts as u64)
                .wrapping_add(value_bits);
            series += 1;
            points += 1;
            bytes += 8 + labels.len() + blob.len();
        }
    }
    Outcome {
        series,
        points,
        bytes,
        checksum,
    }
}

fn consume_native_latest(stmt: &mut Statement<'_>, filter: &str, start: i64, stop: i64) -> Outcome {
    let mut rows = stmt
        .query(params![METRIC, filter, start, stop])
        .expect("query native latest");
    let mut series = 0usize;
    let mut bytes = 0usize;
    let mut checksum = 0u64;
    while let Some(row) = rows.next().expect("step native latest") {
        let sid: i64 = row.get(0).expect("series id");
        let labels: String = row.get(1).expect("latest labels");
        let ts: i64 = row.get(2).expect("latest timestamp");
        let value: f64 = row.get(3).expect("latest value");
        checksum = checksum
            .wrapping_add(sid as u64)
            .wrapping_add(ts as u64)
            .wrapping_add(value.to_bits());
        series += 1;
        bytes += 24 + labels.len();
    }
    Outcome {
        series,
        points: series,
        bytes,
        checksum,
    }
}

fn consume_aggregate(stmt: &mut Statement<'_>, filter: &str, start: i64, stop: i64) -> Outcome {
    let mut rows = stmt
        .query(params![METRIC, filter, start, stop])
        .expect("query native aggregate");
    let mut series = 0usize;
    let mut bytes = 0usize;
    let mut checksum = 0u64;
    while let Some(row) = rows.next().expect("step native aggregate") {
        let sid: i64 = row.get(0).expect("series id");
        let labels: String = row.get(1).expect("labels");
        let value: f64 = row.get(2).expect("aggregate value");
        series += 1;
        bytes += 16 + labels.len();
        checksum = checksum
            .wrapping_add(sid as u64)
            .wrapping_add(value.to_bits());
    }
    Outcome {
        series,
        points: series,
        bytes,
        checksum,
    }
}

fn consume_aggregate_frame(
    stmt: &mut Statement<'_>,
    filter: &str,
    start: i64,
    stop: i64,
) -> Outcome {
    let blob: Vec<u8> = stmt
        .query_row(params![METRIC, filter, start, stop], |row| row.get(0))
        .expect("query aggregate frame");
    assert!(blob.len() >= 12, "truncated aggregate frame");
    assert_eq!(&blob[..4], b"TAF1", "unknown aggregate frame version");
    assert_eq!(blob[4], 0, "benchmark expected an avg aggregate frame");
    assert_eq!(&blob[5..8], &[0, 0, 0], "aggregate frame flags set");
    let series = u32::from_le_bytes(blob[8..12].try_into().unwrap()) as usize;
    let bitmap_bytes = series.checked_add(7).expect("bitmap size overflow") / 8;
    let expected = 12usize
        .checked_add(series.checked_mul(16).expect("aggregate columns overflow"))
        .and_then(|size| size.checked_add(bitmap_bytes))
        .expect("aggregate frame size overflow");
    assert_eq!(blob.len(), expected, "malformed aggregate frame");
    let bitmap_start = 12 + series * 8;
    let values_start = bitmap_start + bitmap_bytes;
    let mut checksum = 0u64;
    for index in 0..series {
        assert_ne!(
            blob[bitmap_start + index / 8] & (1 << (index % 8)),
            0,
            "benchmark aggregate unexpectedly returned NULL"
        );
        let id_start = 12 + index * 8;
        let value_start = values_start + index * 8;
        let series_id = i64::from_le_bytes(blob[id_start..id_start + 8].try_into().unwrap());
        let value_bits = u64::from_le_bytes(blob[value_start..value_start + 8].try_into().unwrap());
        checksum = checksum
            .wrapping_add(series_id as u64)
            .wrapping_add(value_bits);
    }
    Outcome {
        series,
        points: series,
        bytes: blob.len(),
        checksum,
    }
}

fn consume_latest_frame(stmt: &mut Statement<'_>, filter: &str, start: i64, stop: i64) -> Outcome {
    let blob: Vec<u8> = stmt
        .query_row(params![METRIC, filter, start, stop], |row| row.get(0))
        .expect("query latest frame");
    assert!(blob.len() >= 8, "truncated latest frame");
    assert_eq!(&blob[..4], b"TLF1", "unknown latest frame version");
    let series = u32::from_le_bytes(blob[4..8].try_into().unwrap()) as usize;
    let bitmap_bytes = series.checked_add(7).expect("bitmap size overflow") / 8;
    let expected = 8usize
        .checked_add(series.checked_mul(24).expect("latest columns overflow"))
        .and_then(|size| size.checked_add(bitmap_bytes))
        .expect("latest frame size overflow");
    assert_eq!(blob.len(), expected, "malformed latest frame");
    let timestamps_start = 8 + series * 8;
    let bitmap_start = timestamps_start + series * 8;
    let values_start = bitmap_start + bitmap_bytes;
    let mut checksum = 0u64;
    for index in 0..series {
        assert_ne!(
            blob[bitmap_start + index / 8] & (1 << (index % 8)),
            0,
            "benchmark latest unexpectedly returned NULL"
        );
        let id_start = 8 + index * 8;
        let timestamp_start = timestamps_start + index * 8;
        let value_start = values_start + index * 8;
        let series_id = i64::from_le_bytes(blob[id_start..id_start + 8].try_into().unwrap());
        let timestamp = i64::from_le_bytes(
            blob[timestamp_start..timestamp_start + 8]
                .try_into()
                .unwrap(),
        );
        let value_bits = u64::from_le_bytes(blob[value_start..value_start + 8].try_into().unwrap());
        checksum = checksum
            .wrapping_add(series_id as u64)
            .wrapping_add(timestamp as u64)
            .wrapping_add(value_bits);
    }
    Outcome {
        series,
        points: series,
        bytes: blob.len(),
        checksum,
    }
}

fn validate_point_blob(blob: &[u8]) -> usize {
    assert!(blob.len() >= 4, "truncated point blob");
    let count = u32::from_le_bytes(blob[0..4].try_into().unwrap()) as usize;
    let expected = 4usize
        .checked_add(count.checked_mul(16).expect("point blob size overflow"))
        .expect("point blob size overflow");
    assert_eq!(blob.len(), expected, "malformed point blob");
    count
}

fn consume_raw_frame(stmt: &mut Statement<'_>, filter: &str, start: i64, stop: i64) -> Outcome {
    let blob: Vec<u8> = stmt
        .query_row(params![METRIC, filter, start, stop], |row| row.get(0))
        .expect("query raw frame");
    assert!(blob.len() >= 16, "truncated raw frame");
    assert_eq!(&blob[..4], b"TRF1", "unknown raw frame version");
    let series = u32::from_le_bytes(blob[4..8].try_into().unwrap()) as usize;
    let points_u64 = u64::from_le_bytes(blob[8..16].try_into().unwrap());
    let points = usize::try_from(points_u64).expect("raw frame point count overflows host");
    let ids_bytes = series.checked_mul(8).expect("raw frame ids overflow");
    let counts_bytes = series.checked_mul(4).expect("raw frame counts overflow");
    let point_bytes = points.checked_mul(16).expect("raw frame points overflow");
    let expected = 16usize
        .checked_add(ids_bytes)
        .and_then(|size| size.checked_add(counts_bytes))
        .and_then(|size| size.checked_add(point_bytes))
        .expect("raw frame size overflow");
    assert_eq!(blob.len(), expected, "malformed raw frame");
    let counts_start = 16 + ids_bytes;
    let counted_points: usize = blob[counts_start..counts_start + counts_bytes]
        .chunks_exact(4)
        .map(|bytes| u32::from_le_bytes(bytes.try_into().unwrap()) as usize)
        .sum();
    assert_eq!(counted_points, points, "raw frame point counts differ");
    let checksum = blob.chunks(8).fold(0u64, |sum, bytes| {
        let mut word = [0u8; 8];
        word[..bytes.len()].copy_from_slice(bytes);
        sum.wrapping_add(u64::from_le_bytes(word))
    });
    Outcome {
        series,
        points,
        bytes: blob.len(),
        checksum,
    }
}

fn consume_window_batches(
    stmt: &mut Statement<'_>,
    filter: &str,
    start: i64,
    stop: i64,
    step: i64,
    window: i64,
) -> Outcome {
    let mut rows = stmt
        .query(params![METRIC, filter, start, stop, step, window])
        .expect("query packed window batches");
    let mut series = 0usize;
    let mut points = 0usize;
    let mut bytes = 0usize;
    let mut checksum = 0u64;
    while let Some(row) = rows.next().expect("step packed window batches") {
        let sid: i64 = row.get(0).expect("window series id");
        let blob: Vec<u8> = row.get(1).expect("window bucket blob");
        let count = validate_window_blob(&blob);
        series += 1;
        points += count;
        bytes += 8 + blob.len();
        checksum = checksum
            .wrapping_add(sid as u64)
            .wrapping_add(blob.len() as u64);
    }
    Outcome {
        series,
        points,
        bytes,
        checksum,
    }
}

fn validate_window_blob(blob: &[u8]) -> usize {
    assert!(blob.len() >= 8, "truncated packed window blob");
    assert_eq!(&blob[..4], b"TWB1", "unknown packed window version");
    let count = u32::from_le_bytes(blob[4..8].try_into().unwrap()) as usize;
    let bitmap_bytes = count.checked_add(7).expect("bitmap size overflow") / 8;
    let expected = 8usize
        .checked_add(count.checked_mul(16).expect("window column size overflow"))
        .and_then(|size| size.checked_add(bitmap_bytes))
        .expect("window blob size overflow");
    assert_eq!(blob.len(), expected, "malformed packed window blob");
    count
}

fn consume_rollup_rows(
    stmts: &mut [Statement<'_>],
    filter: &str,
    start: i64,
    stop: i64,
) -> Outcome {
    const AGGS: [&str; 6] = ["avg", "min", "max", "count", "sum", "last"];
    assert_eq!(stmts.len(), AGGS.len());
    let mut values = 0usize;
    let mut bytes = 0usize;
    let mut checksum = 0u64;
    for (stmt, agg) in stmts.iter_mut().zip(AGGS) {
        let mut rows = stmt
            .query(params![METRIC, filter, start, stop, agg])
            .expect("query row rollups");
        while let Some(row) = rows.next().expect("step row rollups") {
            let labels: String = row.get(0).expect("rollup labels");
            let timestamp: i64 = row.get(1).expect("rollup timestamp");
            let value: f64 = row.get(2).expect("rollup value");
            values += 1;
            bytes += labels.len() + 16;
            checksum = checksum
                .wrapping_add(labels.len() as u64)
                .wrapping_add(timestamp as u64)
                .wrapping_add(value.to_bits());
        }
    }
    assert_eq!(
        values % AGGS.len(),
        0,
        "rollup aggregate cardinality differs"
    );
    Outcome {
        series: 0,
        points: values / AGGS.len(),
        bytes,
        checksum,
    }
}

fn consume_rollup_batches(
    stmt: &mut Statement<'_>,
    filter: &str,
    start: i64,
    stop: i64,
) -> Outcome {
    let mut rows = stmt
        .query(params![METRIC, filter, start, stop])
        .expect("query packed rollups");
    let mut series = 0usize;
    let mut points = 0usize;
    let mut bytes = 0usize;
    let mut checksum = 0u64;
    while let Some(row) = rows.next().expect("step packed rollups") {
        let series_id: i64 = row.get(0).expect("rollup series id");
        let blob: Vec<u8> = row.get(1).expect("rollup bucket blob");
        let count = validate_rollup_blob(&blob);
        series += 1;
        points += count;
        bytes += 8 + blob.len();
        checksum = checksum.wrapping_add(series_id as u64);
        for word in blob[8..].chunks_exact(8) {
            checksum = checksum.wrapping_add(u64::from_le_bytes(word.try_into().unwrap()));
        }
    }
    Outcome {
        series,
        points,
        bytes,
        checksum,
    }
}

fn validate_rollup_blob(blob: &[u8]) -> usize {
    assert!(blob.len() >= 8, "truncated packed rollup blob");
    assert_eq!(&blob[..4], b"TRB1", "unknown packed rollup version");
    let count = u32::from_le_bytes(blob[4..8].try_into().unwrap()) as usize;
    let expected = 8usize
        .checked_add(count.checked_mul(64).expect("rollup column size overflow"))
        .expect("rollup blob size overflow");
    assert_eq!(blob.len(), expected, "malformed packed rollup blob");
    count
}

fn consume_count<P>(stmt: &mut Statement<'_>, params: P) -> Outcome
where
    P: rusqlite::Params,
{
    let count: i64 = stmt
        .query_row(params, |row| row.get(0))
        .expect("count kernel rows");
    Outcome {
        series: 0,
        points: count as usize,
        bytes: 8,
        checksum: count as u64,
    }
}

fn measure(mut runs: usize, mut operation: impl FnMut() -> Outcome) -> Stats {
    runs = runs.max(1);
    let mut samples = Vec::with_capacity(runs);
    let mut expected = None;
    for _ in 0..runs {
        let started = Instant::now();
        let outcome = operation();
        let elapsed = started.elapsed().as_micros();
        if let Some(expected) = expected {
            assert_eq!(outcome, expected, "query result changed between timed runs");
        } else {
            expected = Some(outcome);
        }
        samples.push(elapsed);
    }
    samples.sort_unstable();
    let p95_index = ((samples.len() * 95).div_ceil(100)).saturating_sub(1);
    Stats {
        median_us: samples[samples.len() / 2],
        p95_us: samples[p95_index],
        min_us: samples[0],
        max_us: *samples.last().unwrap(),
        runs,
        outcome: expected.unwrap(),
    }
}

fn print_stats(metric: &str, stats: &Stats) {
    println!(
        "{metric},{},{},{},{},{},{},{},{}",
        stats.median_us,
        stats.p95_us,
        stats.min_us,
        stats.max_us,
        stats.runs,
        stats.outcome.series,
        stats.outcome.points,
        stats.outcome.bytes
    );
}
