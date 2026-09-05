//! bench-cardinality: series-cardinality sweep with memory tracking.
//!
//!   bench-cardinality <path-to-libtimeless_ext.so> [--series 1000,10000,100000]
//!
//! The 1M-point hero bench (main.rs) holds cardinality at 1,000 series
//! and scales points. This tool does the opposite — the edge/IoT-fleet
//! question: what happens to ingest, flush, recovery, catalog and query
//! latency, file size, and above all MEMORY as the series count climbs
//! toward 100k (points per series held at 100).
//!
//! Memory methodology: the interesting number is the READER's footprint
//! (a core-side replica opening a shipped file), which the ingesting
//! process would mask — its RSS peaks on dataset/blob buffers and never
//! shrinks back (allocators don't return pages). So all read-side
//! phases run in a SPAWNED FRESH PROCESS (`--probe` mode) whose RSS
//! starts near zero: baseline → engine recovery + first catalog → warm
//! catalog → label discovery → equality grid → regex-matcher grid →
//! window rate. RSS is sampled from `ps -o rss=` after every phase on
//! both sides. Values are synthesized statelessly per (series, step) so
//! the driver never holds the dataset in memory either.

use std::env;
use std::fs;
use std::process::Command;
use std::time::Instant;

use rusqlite::{params, Connection};

const N_METRICS: usize = 10;
const METRIC_NAMES: [&str; N_METRICS] = [
    "cpu.usage",
    "cpu.system",
    "mem.used",
    "mem.cached",
    "disk.io",
    "disk.read",
    "net.rx",
    "net.tx",
    "load.avg",
    "temp.cpu",
];

const PTS_PER_SERIES: usize = 100;
const STEP_MS: i64 = 10_000;
const BASE_TS: i64 = 1_700_000_000_000;

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn unit(h: u64) -> f64 {
    (h >> 11) as f64 / (1u64 << 53) as f64
}

/// Stateless point synthesis: sine base + per-series phase + jitter, so
/// any (series, step) is computable without carrying walk state — the
/// driver streams blobs instead of materializing 10M points. Compresses
/// like a periodic gauge (the honest middle of the hero bench's kinds).
fn point(series: usize, i: usize) -> (i64, f64) {
    let jitter = (splitmix((series as u64) << 32 | i as u64) % 1000) as i64;
    let ts = BASE_TS + (i as i64) * STEP_MS + jitter;
    let phase = unit(splitmix(series as u64 ^ 0xC0FFEE)) * std::f64::consts::TAU;
    let noise = (unit(splitmix((series as u64) << 20 ^ i as u64)) - 0.5) * 2.0;
    let v = 50.0 + 40.0 * ((i as f64) * 0.05 + phase).sin() + noise;
    (ts, v)
}

fn series_name(series: usize) -> &'static str {
    METRIC_NAMES[series % N_METRICS]
}

fn series_labels(series: usize) -> String {
    format!("{{\"host\":\"host-{:05}\"}}", series / N_METRICS)
}

/// Batch blob v0 for steps [lo, hi) of ALL series (time-major), full
/// series table included (self-contained, like the hero bench's blobs).
fn encode_blob(n_series: usize, lo: usize, hi: usize) -> Vec<u8> {
    let n_points = (hi - lo) * n_series;
    let mut out = Vec::with_capacity(n_series * 48 + n_points * 20 + 64);
    out.push(0x01);
    out.push(0x00);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(n_series as u32).to_le_bytes());
    out.extend_from_slice(&(n_points as u32).to_le_bytes());
    for s in 0..n_series {
        let name = series_name(s).as_bytes();
        let labels = series_labels(s);
        out.extend_from_slice(&(name.len() as u32).to_le_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&(labels.len() as u32).to_le_bytes());
        out.extend_from_slice(labels.as_bytes());
    }
    for _i in lo..hi {
        for s in 0..n_series {
            out.extend_from_slice(&(s as u32).to_le_bytes());
        }
    }
    for i in lo..hi {
        for s in 0..n_series {
            out.extend_from_slice(&point(s, i).0.to_le_bytes());
        }
    }
    for i in lo..hi {
        for s in 0..n_series {
            out.extend_from_slice(&point(s, i).1.to_le_bytes());
        }
    }
    out
}

/// Resident set size of this process in KB, via ps (macOS + Linux).
fn rss_kb() -> u64 {
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("run ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

fn fmt_mb(kb: u64) -> String {
    format!("{:.1}", kb as f64 / 1024.0)
}

fn open_with_ext(path: &str, ext: &str) -> Connection {
    let conn = Connection::open(path).expect("open db");
    unsafe {
        conn.load_extension_enable().expect("enable ext loading");
        conn.load_extension(ext, None::<&str>)
            .expect("load extension");
    }
    conn.load_extension_disable().expect("disable ext loading");
    conn
}

// ---------------------------------------------------------------------------
// Probe mode: the fresh-process reader
// ---------------------------------------------------------------------------

/// Phases print machine-readable lines the driver parses:
///   probe|<phase>|<millis>|<rss_kb>|<detail>
///
/// Two regimes, measured in this order:
///
///   unpinned_*: a TVF-ONLY connection — the process registry holds
///     engines by Weak reference and eponymous TVF vtabs die at
///     statement end, so NOTHING keeps a strong Arc between queries:
///     every statement rebuilds the engine (full recovery). This is
///     what a dashboard reader that never touches the base vtab
///     experiences today.
///
///   pinned_*: after one query against the base vtab `m` — the created
///     vtab's connection holds the engine Arc for the connection
///     lifetime, so TVF queries hit the generation fast path.
fn run_probe(db: &str, ext: &str, n_series: usize) {
    let emit = |phase: &str, ms: f64, detail: String| {
        println!("probe|{phase}|{ms:.1}|{}|{detail}", rss_kb());
    };
    emit("baseline", 0.0, String::new());

    let conn = open_with_ext(db, ext);
    emit("open", 0.0, String::new());

    let stop = BASE_TS + (PTS_PER_SERIES as i64 - 1) * STEP_MS;

    // ── Regime 1: TVF-only (engine rebuilt per statement) ──
    let t = Instant::now();
    let series: i64 = conn
        .query_row("SELECT COUNT(*) FROM timeless_series('m')", [], |r| {
            r.get(0)
        })
        .expect("first catalog");
    emit(
        "unpinned_catalog",
        t.elapsed().as_secs_f64() * 1e3,
        format!("{series} series"),
    );
    assert_eq!(series as usize, n_series, "catalog series count");

    let t = Instant::now();
    let rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM timeless_grid('m', 'cpu.usage',
               '{\"host\":\"host-00000\"}', ?1, ?2, 60000, 20000)",
            params![BASE_TS, stop],
            |r| r.get(0),
        )
        .expect("unpinned eq grid");
    emit(
        "unpinned_grid_eq",
        t.elapsed().as_secs_f64() * 1e3,
        format!("{rows} rows"),
    );

    // ── Pin: one pushdown-narrow query against the base vtab. Its
    // connection keeps the engine Arc alive from here on. ──
    let t = Instant::now();
    let _: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM (SELECT ts FROM m
               WHERE name = 'cpu.usage' AND ts <= ?1 LIMIT 1)",
            params![BASE_TS],
            |r| r.get(0),
        )
        .expect("pin vtab");
    emit(
        "pin_base_vtab",
        t.elapsed().as_secs_f64() * 1e3,
        String::new(),
    );

    // ── Regime 2: pinned (generation fast path) ──
    let t = Instant::now();
    let _: i64 = conn
        .query_row("SELECT COUNT(*) FROM timeless_series('m')", [], |r| {
            r.get(0)
        })
        .unwrap();
    emit(
        "pinned_catalog",
        t.elapsed().as_secs_f64() * 1e3,
        String::new(),
    );

    let t = Instant::now();
    let hosts: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM timeless_label_values('m', 'cpu.usage', 'host')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    emit(
        "pinned_label_values",
        t.elapsed().as_secs_f64() * 1e3,
        format!("{hosts} hosts"),
    );
    assert_eq!(hosts as usize, n_series / N_METRICS, "host count");

    // Equality filter: one host, one metric — the index-pushdown path.
    let t = Instant::now();
    let rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM timeless_grid('m', 'cpu.usage',
               '{\"host\":\"host-00000\"}', ?1, ?2, 60000, 20000)",
            params![BASE_TS, stop],
            |r| r.get(0),
        )
        .expect("eq grid");
    emit(
        "pinned_grid_eq",
        t.elapsed().as_secs_f64() * 1e3,
        format!("{rows} rows"),
    );

    // Regex matcher: anchored scan over EVERY candidate series of the
    // metric (n_series/10) — the F8 per-series cost at cardinality.
    let t = Instant::now();
    let rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM timeless_grid('m', 'cpu.usage',
               '{\"host\":{\"re\":\"host-000[0-4][0-9]\"}}', ?1, ?2, 60000, 20000)",
            params![BASE_TS, stop],
            |r| r.get(0),
        )
        .expect("regex grid");
    emit(
        "pinned_grid_re",
        t.elapsed().as_secs_f64() * 1e3,
        format!("{rows} rows"),
    );

    // The heavy dashboard shape: 5-min rate for one metric across ALL
    // hosts (n_series/10 series decoded + folded).
    let t = Instant::now();
    let rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM timeless_window('m', 'cpu.usage', NULL,
               ?1, ?2, 60000, 300000, 'rate')",
            params![BASE_TS, stop],
            |r| r.get(0),
        )
        .expect("window rate");
    emit(
        "pinned_window_rate",
        t.elapsed().as_secs_f64() * 1e3,
        format!("{rows} rows"),
    );

    emit("final", 0.0, String::new());
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

struct ProbeLine {
    phase: String,
    ms: f64,
    rss_kb: u64,
    detail: String,
}

fn run_one(n_series: usize, ext: &str, dir: &str) -> Vec<String> {
    let path = format!("{dir}/card_{n_series}.db");
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = fs::remove_file(format!("{path}{suffix}"));
    }
    let n_points = n_series * PTS_PER_SERIES;
    println!("== {n_series} series x {PTS_PER_SERIES} pts = {n_points} points ==");
    println!("  driver rss before ingest: {} MB", fmt_mb(rss_kb()));

    let conn = open_with_ext(&path, ext);
    conn.execute_batch("CREATE VIRTUAL TABLE m USING timeless_metrics;")
        .expect("create vtab");

    // ~1M points per blob, whole steps, at least one step per blob.
    // Synthesis happens per blob outside the clock — only the INSERTs
    // are timed, matching the hero bench (which pre-generates).
    let steps_per_blob = (1_000_000 / n_series).clamp(1, PTS_PER_SERIES);
    let mut ingest_s = 0.0f64;
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn.prepare("INSERT INTO m(m) VALUES (?1)").unwrap();
        let mut lo = 0usize;
        while lo < PTS_PER_SERIES {
            let hi = (lo + steps_per_blob).min(PTS_PER_SERIES);
            let blob = encode_blob(n_series, lo, hi);
            let t0 = Instant::now();
            stmt.execute(params![blob]).expect("ingest blob");
            ingest_s += t0.elapsed().as_secs_f64();
            lo = hi;
        }
    }
    let t0 = Instant::now();
    conn.execute_batch("COMMIT").unwrap();
    ingest_s += t0.elapsed().as_secs_f64();
    let rss_ingest = rss_kb();

    let t0 = Instant::now();
    conn.execute("INSERT INTO m(m) VALUES ('flush')", [])
        .expect("flush");
    let flush_s = t0.elapsed().as_secs_f64();
    let rss_flush = rss_kb();
    drop(conn);
    let file_bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    println!(
        "  ingest {} pts in {:.2}s ({:.2}M pts/s), rss after {} MB",
        n_points,
        ingest_s,
        n_points as f64 / ingest_s / 1e6,
        fmt_mb(rss_ingest)
    );
    println!(
        "  flush {:.2}s ({:.0}k series/s), rss after {} MB",
        flush_s,
        n_series as f64 / flush_s / 1e3,
        fmt_mb(rss_flush)
    );
    println!(
        "  file {:.1} MB ({:.2} bytes/pt)",
        file_bytes as f64 / 1e6,
        file_bytes as f64 / n_points as f64
    );

    // Fresh-process read side.
    let exe = env::current_exe().expect("current exe");
    let out = Command::new(exe)
        .args(["--probe", &path, ext, &n_series.to_string()])
        .output()
        .expect("spawn probe");
    if !out.status.success() {
        panic!(
            "probe failed for {n_series} series:\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let lines: Vec<ProbeLine> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| {
            let mut f = l.strip_prefix("probe|")?.splitn(4, '|');
            Some(ProbeLine {
                phase: f.next()?.to_string(),
                ms: f.next()?.parse().ok()?,
                rss_kb: f.next()?.parse().ok()?,
                detail: f.next().unwrap_or("").to_string(),
            })
        })
        .collect();
    let get = |phase: &str| -> &ProbeLine {
        lines
            .iter()
            .find(|p| p.phase == phase)
            .unwrap_or_else(|| panic!("probe phase {phase} missing"))
    };

    println!("  reader (fresh process):");
    for p in &lines {
        let detail = if p.detail.is_empty() {
            String::new()
        } else {
            format!("  [{}]", p.detail)
        };
        println!(
            "    {:<24} {:>9.1} ms   rss {:>7} MB{}",
            p.phase,
            p.ms,
            fmt_mb(p.rss_kb),
            detail
        );
    }

    // One row for the final cross-cardinality table.
    vec![
        format!("{n_series}"),
        format!("{:.2}M", n_points as f64 / ingest_s / 1e6),
        format!("{:.2}", flush_s),
        format!("{:.1}", file_bytes as f64 / 1e6),
        format!("{:.2}", file_bytes as f64 / n_points as f64),
        format!("{:.0}", get("unpinned_catalog").ms),
        format!("{:.0}", get("unpinned_grid_eq").ms),
        format!("{:.1}", get("pinned_catalog").ms),
        format!("{:.1}", get("pinned_label_values").ms),
        format!("{:.2}", get("pinned_grid_eq").ms),
        format!("{:.1}", get("pinned_grid_re").ms),
        format!("{:.0}", get("pinned_window_rate").ms),
        fmt_mb(get("unpinned_catalog").rss_kb),
        fmt_mb(get("final").rss_kb),
    ]
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--probe") {
        let (db, ext, n) = (&args[1], &args[2], args[3].parse().expect("n_series"));
        run_probe(db, ext, n);
        return;
    }

    let ext = args
        .first()
        .unwrap_or_else(|| {
            eprintln!("usage: bench-cardinality <libtimeless_ext.so> [--series 1000,10000,100000]");
            std::process::exit(2);
        })
        .clone();
    let series: Vec<usize> = args
        .iter()
        .position(|a| a == "--series")
        .and_then(|i| args.get(i + 1))
        .map(|s| {
            s.split(',')
                .map(|n| n.parse().expect("series count"))
                .collect()
        })
        .unwrap_or_else(|| vec![1_000, 10_000, 100_000]);

    let dir = format!(
        "{}/tl_bench_card_{}",
        env::temp_dir().display(),
        std::process::id()
    );
    fs::create_dir_all(&dir).expect("create scratch dir");

    println!("# bench-cardinality — {PTS_PER_SERIES} pts/series, tier2 blob ingest\n");
    let mut rows = Vec::new();
    for &n in &series {
        rows.push(run_one(n, &ext, &dir));
        println!();
    }

    println!("| series | ingest | flush s | file MB | B/pt | unpinned catalog ms | unpinned grid ms | catalog ms | label_values ms | grid eq ms | grid re ms | rate-all ms | rss@recovery MB | rss final MB |");
    println!("|--------|--------|---------|---------|------|---------------------|------------------|------------|-----------------|------------|------------|-------------|-----------------|--------------|");
    for r in rows {
        println!("| {} |", r.join(" | "));
    }
    println!("\ndbs left in {dir} for inspection.");
}
