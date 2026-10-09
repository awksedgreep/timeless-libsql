//! Series-index cost at fleet scale (#132 phase 0).
//!
//! Builds a fleet-shaped metrics catalog through the public SQL surface and
//! measures what an on-disk index behind a bounded cache must match: a full
//! five-minute cycle that re-resolves every existing series, selector and
//! discovery reads, on-disk bytes, and the open time and resident memory of
//! a fresh process. Labels are generated on the fly, so the harness's own
//! memory does not inflate the process RSS it reports.
//!
//!   query-read EXT --index-cache --series N [--runs N]
//!   query-read EXT --index-cache-open DB      (child: open + first read)

use super::{open_with_ext, BASE_TS};
use rusqlite::Connection;
use std::process::Command;
use std::time::Instant;

/// Interface counters and the info metric, as the Wi-Fi poll writes them.
const METRICS: [&str; 5] = [
    "fleet_if_info",
    "fleet_if_in_octets",
    "fleet_if_out_octets",
    "fleet_if_in_errors",
    "fleet_if_out_errors",
];
const INTERFACES: usize = 19;
const SERIES_PER_GATEWAY: usize = METRICS.len() * INTERFACES;
const BATCH_POINTS: usize = 20_000;
const CYCLE_SECS: i64 = 300;
const ROLES: [&str; 8] = [
    "wan", "lan", "ssid", "radio", "moca", "voice", "mgmt", "other",
];

fn mac(gateway: usize) -> String {
    let bytes = (gateway as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes();
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
    )
}

/// The (metric, sorted label pairs) of series `index`: eight labels, ~270
/// bytes, high-cardinality `cm_mac`, low-cardinality the rest. Keys are in
/// canonical (sorted) order.
pub(super) fn series_pairs(index: usize) -> (&'static str, Vec<(&'static str, String)>) {
    let gateway = index / SERIES_PER_GATEWAY;
    let within = index % SERIES_PER_GATEWAY;
    let metric = METRICS[within / INTERFACES];
    let interface = within % INTERFACES;
    let role = ROLES[interface % ROLES.len()];
    let name = format!("if{interface}");
    let if_index = interface + 1;
    let pairs = vec![
        ("cm_mac", mac(gateway)),
        (
            "counter_source",
            if interface < 12 { "if_mib" } else { "clab_wifi_radio_stats" }.to_string(),
        ),
        ("display_name", format!("{role} · {name} (ifIndex {if_index})")),
        ("if", name),
        ("if_index", if_index.to_string()),
        ("if_type", [6, 71, 127, 129, 142, 236, 1, 24][interface % 8].to_string()),
        ("interface_id", format!("if:{if_index}")),
        ("role", role.to_string()),
    ];
    (metric, pairs)
}

/// The (metric, labels JSON) of series `index`.
fn series(index: usize) -> (&'static str, String) {
    let (metric, pairs) = series_pairs(index);
    let body: Vec<String> = pairs
        .iter()
        .map(|(key, value)| format!("{}:{}", json(key), json(value)))
        .collect();
    (metric, format!("{{{}}}", body.join(",")))
}

fn json(text: &str) -> String {
    serde_json_escape(text)
}

fn serde_json_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub(super) const SERIES_PER_GATEWAY_PUB: usize = SERIES_PER_GATEWAY;
pub(super) const METRICS_PUB: [&str; 5] = METRICS;
pub(super) fn mac_pub(gateway: usize) -> String {
    mac(gateway)
}

/// One cycle: a point for every series at `ts`, in import-sized batches,
/// each its own transaction. Returns wall time in milliseconds.
fn cycle(conn: &Connection, series_count: usize, ts: i64, value: f64) -> f64 {
    let started = Instant::now();
    let mut start = 0;
    while start < series_count {
        let end = (start + BATCH_POINTS).min(series_count);
        let blob = encode(start..end, ts, value);
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        conn.execute("INSERT INTO metrics(metrics) VALUES (?1)", [blob])
            .unwrap();
        conn.execute_batch("COMMIT").unwrap();
        start = end;
    }
    started.elapsed().as_secs_f64() * 1_000.0
}

/// A named batch: one point for each series in `range`.
fn encode(range: std::ops::Range<usize>, ts: i64, value: f64) -> Vec<u8> {
    let count = range.len();
    let mut out = Vec::with_capacity(12 + count * 320);
    out.extend_from_slice(&[1, 0, 0, 0]);
    out.extend_from_slice(&(count as u32).to_le_bytes());
    out.extend_from_slice(&(count as u32).to_le_bytes());
    for index in range {
        let (metric, labels) = series(index);
        for text in [metric, labels.as_str()] {
            out.extend_from_slice(&(text.len() as u32).to_le_bytes());
            out.extend_from_slice(text.as_bytes());
        }
    }
    for i in 0..count {
        out.extend_from_slice(&(i as u32).to_le_bytes());
    }
    for _ in 0..count {
        out.extend_from_slice(&ts.to_le_bytes());
    }
    for i in 0..count {
        out.extend_from_slice(&(value + i as f64).to_le_bytes());
    }
    out
}

fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| {
            line.strip_prefix(field)?
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .ok()
        })
        .unwrap_or(0)
}

fn count(conn: &Connection, sql: &str, args: &[&dyn rusqlite::ToSql]) -> usize {
    let mut stmt = conn.prepare_cached(sql).unwrap();
    let mut rows = stmt.query(args).unwrap();
    let mut n = 0;
    while rows.next().unwrap().is_some() {
        n += 1;
    }
    n
}

fn timed(runs: usize, mut read: impl FnMut() -> usize) -> (f64, f64, usize) {
    let expected = read();
    for _ in 0..3 {
        assert_eq!(read(), expected);
    }
    let mut samples: Vec<f64> = (0..runs)
        .map(|_| {
            let started = Instant::now();
            assert_eq!(read(), expected, "read result changed between runs");
            started.elapsed().as_secs_f64() * 1_000_000.0
        })
        .collect();
    samples.sort_by(f64::total_cmp);
    (
        samples[samples.len() / 2],
        samples[(samples.len() * 95).div_ceil(100) - 1],
        expected,
    )
}

/// The reads the index serves, each with its expected result size.
fn reads(conn: &Connection, series_count: usize, runs: usize) {
    // The bounded form is what servers call; the unbounded form intersects
    // the in-memory posting lists (find_series) instead of scanning the
    // metric's series, so both are measured.
    const SERIES: &str =
        "SELECT series_id, labels FROM timeless_series('metrics', ?1, ?2, 1000000000, 1000000000000)";
    const SERIES_POSTINGS: &str =
        "SELECT series_id, labels FROM timeless_series('metrics', ?1, ?2)";
    let gateways = series_count / SERIES_PER_GATEWAY;
    let one_mac = format!(r#"{{"cm_mac":"{}"}}"#, mac(gateways / 2));
    let cases: Vec<(&str, Box<dyn Fn() -> usize + '_>)> = vec![
        (
            "select_exact_name",
            Box::new(|| count(conn, SERIES, &[&METRICS[1], &None::<String>])),
        ),
        (
            "select_name_and_mac",
            Box::new(|| count(conn, SERIES, &[&METRICS[1], &one_mac])),
        ),
        (
            "select_name_and_regex_role",
            Box::new(|| {
                count(
                    conn,
                    SERIES,
                    &[&METRICS[1], &r#"{"role":{"re":"wan|ssid"}}"#],
                )
            }),
        ),
        (
            "select_exact_name_unbounded",
            Box::new(|| count(conn, SERIES_POSTINGS, &[&METRICS[1], &None::<String>])),
        ),
        (
            "select_name_and_mac_unbounded",
            Box::new(|| count(conn, SERIES_POSTINGS, &[&METRICS[1], &one_mac])),
        ),
        (
            "discover_label_names",
            Box::new(|| count(conn, "SELECT name FROM timeless_label_names('metrics')", &[])),
        ),
        (
            "discover_metric_names",
            Box::new(|| {
                count(
                    conn,
                    "SELECT value FROM timeless_label_values('metrics', NULL, '__name__')",
                    &[],
                )
            }),
        ),
        (
            "discover_mac_values",
            Box::new(|| {
                count(
                    conn,
                    "SELECT value FROM timeless_label_values('metrics', NULL, 'cm_mac')",
                    &[],
                )
            }),
        ),
    ];
    for (name, read) in cases {
        let (median, p95, rows) = timed(runs, read);
        println!("{series_count},read,{name},{median:.0},{p95:.0},{rows}");
    }
}

pub(super) fn run(ext: &str, series_count: usize, runs: usize) {
    assert!(
        series_count >= SERIES_PER_GATEWAY,
        "--series must be at least one gateway ({SERIES_PER_GATEWAY})"
    );
    let series_count = series_count / SERIES_PER_GATEWAY * SERIES_PER_GATEWAY;
    println!(
        "# index-cache: series={series_count}, gateways={}, labels=8, batch_points={BATCH_POINTS}",
        series_count / SERIES_PER_GATEWAY
    );
    println!("# extension={ext}, sqlite={}", rusqlite::version());
    println!("series,kind,measure,median_or_value,p95,rows");
    let temporary = tempfile::Builder::new()
        .prefix("timeless-index-cache-")
        .tempdir()
        .unwrap();
    let db = temporary.path().join("metrics.db");
    let path = db.to_str().unwrap();
    let rss_start = status_kib("VmRSS:");
    {
        let conn = open_with_ext(path, ext);
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
             CREATE VIRTUAL TABLE metrics USING timeless_metrics;",
        )
        .unwrap();
        let create = cycle(&conn, series_count, BASE_TS, 0.0);
        println!("{series_count},ingest,create_cycle_ms,{create:.0},,{series_count}");
        conn.execute_batch("INSERT INTO metrics(metrics) VALUES ('flush')")
            .unwrap();
        let mut steady: Vec<f64> = (1..=3)
            .map(|n| {
                let ms = cycle(&conn, series_count, BASE_TS + n * CYCLE_SECS, n as f64);
                conn.execute_batch("INSERT INTO metrics(metrics) VALUES ('flush')")
                    .unwrap();
                ms
            })
            .collect();
        steady.sort_by(f64::total_cmp);
        println!(
            "{series_count},ingest,steady_cycle_ms,{:.0},{:.0},{series_count}",
            steady[1], steady[2]
        );
        println!(
            "{series_count},ingest,steady_ns_per_series,{:.0},,{series_count}",
            steady[1] * 1_000_000.0 / series_count as f64
        );
        reads(&conn, series_count, runs);
        let rss = status_kib("VmRSS:").saturating_sub(rss_start);
        println!("{series_count},memory,writer_rss_delta_kib,{rss},,");
        println!(
            "{series_count},memory,writer_rss_bytes_per_series,{},,",
            rss * 1024 / series_count as u64
        );
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    let disk = super::database_bytes(path);
    println!("{series_count},disk,database_bytes,{disk},,");
    println!(
        "{series_count},disk,bytes_per_series,{},,",
        disk / series_count as u64
    );

    // A fresh process: what opening the store costs and keeps resident.
    let output = Command::new(std::env::current_exe().unwrap())
        .args([ext, "--index-cache-open", path])
        .output()
        .expect("spawn open child");
    assert!(
        output.status.success(),
        "open child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        println!("{series_count},{line}");
    }

    // The on-disk design over the same store, in its own fresh process.
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--index-disk",
            path,
            &series_count.to_string(),
            &runs.to_string(),
        ])
        .output()
        .expect("spawn disk-index child");
    assert!(
        output.status.success(),
        "disk-index child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        println!("{series_count},{line}");
    }
}

/// Child mode: open an existing store, force the engine to load with one
/// read, and report the time and the memory that load keeps resident.
pub(super) fn open_only(ext: &str, path: &str) {
    let before = status_kib("VmRSS:");
    let started = Instant::now();
    let conn = open_with_ext(path, ext);
    let names = count(
        &conn,
        "SELECT value FROM timeless_label_values('metrics', NULL, '__name__')",
        &[],
    );
    assert_eq!(names, METRICS.len());
    let ms = started.elapsed().as_secs_f64() * 1_000.0;
    println!("open,open_to_first_read_ms,{ms:.0},,");
    println!(
        "open,open_rss_delta_kib,{},,",
        status_kib("VmRSS:").saturating_sub(before)
    );
    println!("open,open_hwm_kib,{},,", status_kib("VmHWM:"));
}
