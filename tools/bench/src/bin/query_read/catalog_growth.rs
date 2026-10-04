//! Fixed-cardinality selectors in growing, interleaved catalogs (#116).
//! Uses the same public SQL path as PromQL raw reads, with no concurrent
//! writer or catalog refresh in the timed region.

use super::{
    measure, open_with_ext, raw_frame_outcome, summarize, Config, Outcome, Stats, BASE_TS, METRIC,
};
use rusqlite::{params, Connection, Statement};
use std::time::Instant;

const SELECTED: usize = 161;
const BACKGROUND: &str = "unrelated_metric";
const FILTER: &str = r#"{"node":"test-node"}"#;

struct Series {
    metric: &'static str,
    labels: String,
    value: f64,
    half: bool,
}

pub(super) fn run(ext: &str, compare_ext: Option<&str>, config: Config) {
    assert!(
        config.series > SELECTED,
        "catalog must exceed {SELECTED} series"
    );
    let points = config.series.checked_mul(config.points).unwrap();
    assert!(u32::try_from(points).is_ok(), "batch point count overflow");
    println!(
        "# catalog-growth: selected={SELECTED}, points_per_series={}, warmup=5, runs={}",
        config.points, config.runs
    );
    println!("# extension={ext}, sqlite={}", rusqlite::version());
    if let Some(compare_ext) = compare_ext {
        println!("# comparison_extension={compare_ext}; paired reads alternate first reader on each iteration");
    }
    println!("catalog_series,layout,query,median_us,p95_us,min_us,max_us,runs,result_series,result_points,result_bytes,checksum");

    for interleaved in [true, false] {
        let layout = if interleaved {
            "interleaved"
        } else {
            "adjacent"
        };
        let temporary = tempfile::Builder::new()
            .prefix("timeless-catalog-growth-")
            .tempdir()
            .unwrap();
        let db = temporary.path().join("metrics.db");
        let conn = open_with_ext(db.to_str().unwrap(), ext);
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
             CREATE VIRTUAL TABLE metrics USING timeless_metrics;",
        )
        .unwrap();
        let selected_positions: Vec<usize> = (0..SELECTED)
            .map(|i| {
                if interleaved {
                    i * (config.series - 1) / (SELECTED - 1)
                } else {
                    i
                }
            })
            .collect();
        let mut selected = 0;
        let fixture: Vec<Series> = (0..config.series)
            .map(|i| {
                let target = selected_positions.get(selected) == Some(&i);
                let ordinal = if target { selected } else { i };
                let half = ordinal.is_multiple_of(2);
                if target {
                    selected += 1;
                }
                Series {
                    metric: if target { METRIC } else { BACKGROUND },
                    labels: format!(
                        r#"{{"band":"{}","item":"{ordinal}","node":"test-node"}}"#,
                        if half { "half" } else { "other" }
                    ),
                    value: ordinal as f64 + 0.5,
                    half,
                }
            })
            .collect();
        assert_eq!(selected, SELECTED);
        let blob = encode(&fixture, config.points);
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        conn.execute("INSERT INTO metrics(metrics) VALUES (?1)", [blob])
            .unwrap();
        conn.execute_batch("COMMIT; INSERT INTO metrics(metrics) VALUES ('flush')")
            .unwrap();
        // Close the writer and warm an independent reader before measuring.
        drop(conn);
        let conn = open_with_ext(db.to_str().unwrap(), ext);
        let comparison = compare_ext.map(|ext| open_with_ext(db.to_str().unwrap(), ext));
        let background: Vec<usize> = (0..config.series)
            .filter(|&i| fixture[i].metric == BACKGROUND)
            .collect();
        let half: Vec<usize> = background
            .iter()
            .copied()
            .filter(|&i| fixture[i].half)
            .collect();
        let first = vec![selected_positions[0]];
        let shapes = [
            ("fixed_161_raw", METRIC, FILTER, &selected_positions),
            ("broad_raw", BACKGROUND, FILTER, &background),
            (
                "half_raw",
                BACKGROUND,
                r#"{"node":"test-node","band":"half"}"#,
                &half,
            ),
            (
                "single_raw",
                METRIC,
                r#"{"node":"test-node","item":"0"}"#,
                &first,
            ),
        ];
        for (name, metric, filter, positions) in shapes {
            let mut stmt = conn
                .prepare("SELECT frame FROM timeless_raw_frame('metrics',?1,?2,?3,?4,?5)")
                .unwrap();
            let mut query = || raw_query(&mut stmt, metric, filter, config.points);
            let expected = query();
            validate(&expected, positions, &fixture, config.points);
            if let Some(comparison) = &comparison {
                let mut other_stmt = comparison
                    .prepare("SELECT frame FROM timeless_raw_frame('metrics',?1,?2,?3,?4,?5)")
                    .unwrap();
                let mut other_query = || raw_query(&mut other_stmt, metric, filter, config.points);
                assert_eq!(
                    other_query(),
                    expected,
                    "extensions returned different frames"
                );
                let stats = measure_pair(
                    config.runs,
                    || raw_frame_outcome(&query()),
                    || raw_frame_outcome(&other_query()),
                );
                print(config.series, layout, &format!("primary_{name}"), &stats[0]);
                print(
                    config.series,
                    layout,
                    &format!("comparison_{name}"),
                    &stats[1],
                );
                continue;
            }
            for _ in 0..5 {
                assert_eq!(query(), expected);
            }
            let stats = measure(config.runs, || raw_frame_outcome(&query()));
            assert_eq!(stats.outcome, raw_frame_outcome(&expected));
            print(config.series, layout, name, &stats);
        }

        // The catalog stage is measured independently: it should not acquire
        // the raw reader's dependence on unrelated chunk-index entries.
        let catalog = || catalog_outcome(&conn);
        for _ in 0..5 {
            assert_eq!(catalog().series, SELECTED);
        }
        if let Some(comparison) = &comparison {
            let stats = measure_pair(config.runs, catalog, || catalog_outcome(comparison));
            print(
                config.series,
                layout,
                "primary_fixed_161_catalog",
                &stats[0],
            );
            print(
                config.series,
                layout,
                "comparison_fixed_161_catalog",
                &stats[1],
            );
        } else {
            print(
                config.series,
                layout,
                "fixed_161_catalog",
                &measure(config.runs, catalog),
            );
        }
    }
}

fn raw_query(stmt: &mut Statement<'_>, metric: &str, filter: &str, points: usize) -> Vec<u8> {
    stmt.query_row(
        params![
            metric,
            filter,
            BASE_TS - 30,
            BASE_TS + points as i64 - 1,
            i64::MAX
        ],
        |row| row.get(0),
    )
    .unwrap()
}

fn measure_pair(
    runs: usize,
    mut primary: impl FnMut() -> Outcome,
    mut comparison: impl FnMut() -> Outcome,
) -> [Stats; 2] {
    let expected = primary();
    let mut samples = [Vec::with_capacity(runs), Vec::with_capacity(runs)];
    for iteration in 0..runs + 5 {
        // Both readers use the exact same persisted fixture. Alternating
        // first reader distributes cache and frequency effects between them.
        for reader in [iteration % 2, (iteration + 1) % 2] {
            let started = Instant::now();
            let outcome = if reader == 0 { primary() } else { comparison() };
            let elapsed = started.elapsed().as_micros();
            assert_eq!(outcome, expected, "paired query result mismatch");
            if iteration >= 5 {
                samples[reader].push(elapsed);
            }
        }
    }
    samples.map(|samples| summarize(samples, expected))
}

fn print(total: usize, layout: &str, query: &str, stats: &super::Stats) {
    println!(
        "{total},{layout},{query},{},{},{},{},{},{},{},{},{:016x}",
        stats.median_us,
        stats.p95_us,
        stats.min_us,
        stats.max_us,
        stats.runs,
        stats.outcome.series,
        stats.outcome.points,
        stats.outcome.bytes,
        stats.outcome.checksum
    );
}

fn catalog_outcome(conn: &Connection) -> Outcome {
    let mut stmt = conn.prepare_cached(
        "SELECT series_id,labels FROM timeless_series('metrics',?1,?2,1000000,268435456) ORDER BY labels,series_id",
    ).unwrap();
    let mut rows = stmt.query(params![METRIC, FILTER]).unwrap();
    let mut result = Outcome {
        series: 0,
        points: 0,
        bytes: 0,
        checksum: 0,
    };
    while let Some(row) = rows.next().unwrap() {
        let id: i64 = row.get(0).unwrap();
        let labels: String = row.get(1).unwrap();
        result.series += 1;
        result.bytes += 8 + labels.len();
        result.checksum = result.checksum.wrapping_add(id as u64);
        for byte in labels.bytes() {
            result.checksum = result.checksum.rotate_left(5) ^ byte as u64;
        }
    }
    result
}

fn encode(fixture: &[Series], points: usize) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&[1, 0, 0, 0]);
    out.extend_from_slice(&(fixture.len() as u32).to_le_bytes());
    out.extend_from_slice(&((fixture.len() * points) as u32).to_le_bytes());
    for series in fixture {
        for text in [series.metric, &series.labels] {
            out.extend_from_slice(&(text.len() as u32).to_le_bytes());
            out.extend_from_slice(text.as_bytes());
        }
    }
    for _ in 0..points {
        for i in 0..fixture.len() {
            out.extend_from_slice(&(i as u32).to_le_bytes());
        }
    }
    for point in 0..points {
        for _ in fixture {
            out.extend_from_slice(&(BASE_TS + point as i64).to_le_bytes());
        }
    }
    for point in 0..points {
        for series in fixture {
            out.extend_from_slice(&(series.value + point as f64).to_le_bytes());
        }
    }
    out
}

fn validate(blob: &[u8], positions: &[usize], fixture: &[Series], points: usize) {
    let outcome = raw_frame_outcome(blob);
    assert_eq!(outcome.series, positions.len());
    assert_eq!(outcome.points, positions.len() * points);
    let timestamps = 16 + positions.len() * 12;
    let values = timestamps + outcome.points * 8;
    let word = |at| u64::from_le_bytes(blob[at..at + 8].try_into().unwrap());
    for (i, &position) in positions.iter().enumerate() {
        assert_eq!(word(16 + i * 8), position as u64 + 1, "durable series ID");
        let at = 16 + positions.len() * 8 + i * 4;
        assert_eq!(
            u32::from_le_bytes(blob[at..at + 4].try_into().unwrap()) as usize,
            points
        );
        for point in 0..points {
            let offset = (i * points + point) * 8;
            assert_eq!(word(timestamps + offset), (BASE_TS + point as i64) as u64);
            assert_eq!(
                word(values + offset),
                (fixture[position].value + point as f64).to_bits()
            );
        }
    }
}
