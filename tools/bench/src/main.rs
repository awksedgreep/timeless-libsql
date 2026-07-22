//! bench: the Session 4 hero benchmark (PLAN.md "Session 4").
//!
//!   bench <path-to-libtimeless_ext.so>
//!
//! Three-way comparison over the SAME deterministic 1M-point TSBS-inspired
//! workload (100 hosts x 10 metrics x 1000 points), against temp dbs in
//! /tmp:
//!
//!   plain  - ordinary SQLite table, prepared INSERT loop (the baseline)
//!   tier1  - vtab row-at-a-time SQL: INSERT INTO metrics(name,ts,value,labels)
//!   tier2  - vtab batch blobs (format v0): INSERT INTO metrics(metrics) VALUES(?)
//!
//! plus query timings and a bit-exact correctness spot check on the tier2
//! db. Prints one markdown table — that table IS the artifact.
//!
//! No dependencies beyond rusqlite: the PRNG is a hand-rolled xorshift64*
//! and timing is std::time::Instant. That is fine for a tool binary (we
//! are measuring milliseconds, not nanoseconds).

use std::env;
use std::fs;
use std::path::Path;
use std::time::Instant;

use rusqlite::{params, Connection};

// ---------------------------------------------------------------------------
// Workload shape (PLAN.md: TSBS-style; deterministic so runs are comparable)
// ---------------------------------------------------------------------------

const N_HOSTS: usize = 100;
const N_METRICS: usize = 10;
const N_SERIES: usize = N_HOSTS * N_METRICS; // 1000
const PTS_PER_SERIES: usize = 1000;
const N_POINTS: usize = N_SERIES * PTS_PER_SERIES; // 1_000_000

/// Scrape interval: 10s in MILLISECONDS (timestamps are i64 millis here;
/// the engine treats ts as an opaque i64, so the unit is the producer's
/// choice — millis is what real collectors emit).
const STEP_MS: i64 = 10_000;
const BASE_TS: i64 = 1_700_000_000_000; // 2023-11-14T22:13:20Z in millis

/// Tier 2 batching: 10 blobs x 100k points.
const BLOB_POINTS: usize = 100_000;

/// How each of the 10 metrics behaves over time — this is what makes the
/// workload "TSBS-inspired" rather than white noise (compressors care!).
#[derive(Clone, Copy)]
enum Kind {
    /// 0-100 bounded random walk (cpu-style percentages).
    Walk,
    /// Slowly growing level with jitter (memory usage).
    Grow,
    /// Monotonic counter with random positive increments (bytes, ops).
    Counter,
    /// Sine wave + noise (periodic gauges: load, temperature).
    Sine,
}

const METRICS: [(&str, Kind); N_METRICS] = [
    ("cpu.usage", Kind::Walk),
    ("cpu.system", Kind::Walk),
    ("mem.used", Kind::Grow),
    ("mem.cached", Kind::Grow),
    ("disk.io", Kind::Counter),
    ("disk.read", Kind::Counter),
    ("net.rx", Kind::Counter),
    ("net.tx", Kind::Counter),
    ("load.avg", Kind::Sine),
    ("temp.cpu", Kind::Sine),
];

// ---------------------------------------------------------------------------
// PRNG: xorshift64* — 3 shifts + 1 multiply, passes BigCrush, zero deps
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    /// splitmix64 on the seed so that consecutive series numbers still
    /// produce wildly different streams (raw xorshift is sensitive to
    /// similar seeds; splitmix is the standard fix).
    fn new(seed: u64) -> Self {
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Rng((z ^ (z >> 31)) | 1) // xorshift state must be non-zero
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform f64 in [0, 1): take the top 53 bits (a f64 mantissa's worth).
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ---------------------------------------------------------------------------
// Dataset: generated once, reused by all three benchmarks + verification
// ---------------------------------------------------------------------------

/// Struct-of-arrays per series: ~16MB total, cheap to keep around, and
/// keeping it around is what makes the correctness spot check trivial.
struct Dataset {
    /// series id = host * N_METRICS + metric
    ts: Vec<Vec<i64>>,
    val: Vec<Vec<f64>>,
    /// per-series metric name (borrowed from METRICS).
    name: Vec<&'static str>,
    /// per-series labels, pre-rendered to the vtab's canonical JSON.
    labels: Vec<String>,
}

fn generate() -> Dataset {
    let mut ts = Vec::with_capacity(N_SERIES);
    let mut val = Vec::with_capacity(N_SERIES);
    let mut name = Vec::with_capacity(N_SERIES);
    let mut labels = Vec::with_capacity(N_SERIES);

    for s in 0..N_SERIES {
        let host = s / N_METRICS;
        let (metric_name, kind) = METRICS[s % N_METRICS];
        // One deterministic stream per series: same seed → same points,
        // every run, so the spot check can compare bit-exact values.
        let mut rng = Rng::new(0xC0FFEE ^ (s as u64));

        let mut sts = Vec::with_capacity(PTS_PER_SERIES);
        let mut svals = Vec::with_capacity(PTS_PER_SERIES);

        // Walk/Grow/Counter carry state point-to-point.
        let mut level: f64 = match kind {
            Kind::Walk => 50.0,
            Kind::Grow => 2.0e9 + rng.next_f64() * 1.0e9, // ~2-3 GB used
            Kind::Counter => 0.0,
            Kind::Sine => 0.0, // stateless
        };
        let phase = rng.next_f64() * std::f64::consts::TAU;

        for i in 0..PTS_PER_SERIES {
            // 10s cadence with 0-999ms of scrape jitter, like a real
            // collector that never fires exactly on the tick.
            let jitter = (rng.next_u64() % 1000) as i64;
            sts.push(BASE_TS + (i as i64) * STEP_MS + jitter);

            let v = match kind {
                Kind::Walk => {
                    level += (rng.next_f64() - 0.5) * 5.0;
                    level = level.clamp(0.0, 100.0);
                    level
                }
                Kind::Grow => {
                    // Mostly grows, occasionally dips (GC, cache eviction).
                    level += (rng.next_f64() - 0.2) * 1.0e6;
                    level
                }
                Kind::Counter => {
                    level += rng.next_f64() * 1.0e5; // monotonic
                    level
                }
                Kind::Sine => {
                    50.0 + 40.0 * ((i as f64) * 0.05 + phase).sin()
                        + (rng.next_f64() - 0.5) * 2.0
                }
            };
            svals.push(v);
        }

        ts.push(sts);
        val.push(svals);
        name.push(metric_name);
        labels.push(format!("{{\"host\":\"host-{host:03}\"}}"));
    }

    Dataset { ts, val, name, labels }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Remove a db and its WAL/SHM sidecars so every run starts cold.
fn scrub(path: &str) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = fs::remove_file(format!("{path}{suffix}"));
    }
}

/// Size of the FINAL db file. Callers must have dropped the Connection
/// first: closing folds the WAL back into the main file and deletes the
/// sidecars, so this is the honest on-disk footprint.
fn db_bytes(path: &str) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn open_with_ext(path: &str, ext: &str) -> Connection {
    let conn = Connection::open(path).expect("open db");
    // Loading arbitrary .so files is inherently unsafe — that is exactly
    // what this tool exists to do, with the .so the user pointed us at.
    unsafe {
        conn.load_extension_enable().expect("enable ext loading");
        conn.load_extension(ext, None::<&str>).expect("load extension");
    }
    conn.load_extension_disable().expect("disable ext loading");
    conn
}

fn fmt_rate(points: usize, secs: f64) -> String {
    format!("{:.2}M pts/s", points as f64 / secs / 1.0e6)
}

fn fmt_bytes(b: u64) -> String {
    format!("{b} ({:.1} MB)", b as f64 / 1.0e6)
}

// ---------------------------------------------------------------------------
// Batch blob format v0 ENCODER (the decoder lives in the extension;
// PLAN.md is the canonical spec — little-endian throughout)
// ---------------------------------------------------------------------------

/// Encode points [lo, hi) of every series (time-major order) into one v0
/// blob. Every blob carries the full 1000-entry series table (~26KB) —
/// negligible next to 100k * 20 bytes of point data, and it keeps blobs
/// self-contained (any blob can be replayed against an empty table).
fn encode_blob(data: &Dataset, lo: usize, hi: usize) -> Vec<u8> {
    let n_points = (hi - lo) * N_SERIES;
    let mut out = Vec::with_capacity(64 * 1024 + n_points * 20);

    // header
    out.push(0x01); // version
    out.push(0x00); // flags
    out.extend_from_slice(&0u16.to_le_bytes()); // reserved
    out.extend_from_slice(&(N_SERIES as u32).to_le_bytes());
    out.extend_from_slice(&(n_points as u32).to_le_bytes());

    // series table: { u32 name_len, name, u32 labels_len, labels-JSON }
    for s in 0..N_SERIES {
        let name = data.name[s].as_bytes();
        let labels = data.labels[s].as_bytes();
        out.extend_from_slice(&(name.len() as u32).to_le_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&(labels.len() as u32).to_le_bytes());
        out.extend_from_slice(labels);
    }

    // three columnar sections, same time-major point order in each.
    for i in lo..hi {
        for s in 0..N_SERIES {
            let _ = i;
            out.extend_from_slice(&(s as u32).to_le_bytes());
        }
    }
    for i in lo..hi {
        for s in 0..N_SERIES {
            out.extend_from_slice(&data.ts[s][i].to_le_bytes());
        }
    }
    for i in lo..hi {
        for s in 0..N_SERIES {
            out.extend_from_slice(&data.val[s][i].to_le_bytes());
        }
    }
    out
}

// ---------------------------------------------------------------------------
// The three ingest benchmarks
// ---------------------------------------------------------------------------

struct IngestResult {
    label: &'static str,
    insert_secs: f64,
    flush_ms: Option<f64>,
    compact_ms: Option<f64>,
    file_bytes: u64,
}

/// Baseline: plain SQLite table, one transaction, prepared-statement loop.
fn bench_plain(data: &Dataset, path: &str) -> IngestResult {
    scrub(path);
    let conn = Connection::open(path).expect("open plain db");
    conn.execute_batch(
        "CREATE TABLE metrics(name TEXT, ts INTEGER, value REAL, labels TEXT);",
    )
    .expect("create plain table");

    let t0 = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO metrics(name, ts, value, labels) VALUES (?1, ?2, ?3, ?4)")
            .expect("prepare");
        // Time-major (scrape order): step 0 for all series, then step 1...
        for i in 0..PTS_PER_SERIES {
            for s in 0..N_SERIES {
                stmt.execute(params![
                    data.name[s],
                    data.ts[s][i],
                    data.val[s][i],
                    data.labels[s]
                ])
                .expect("insert row");
            }
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    let insert_secs = t0.elapsed().as_secs_f64();

    drop(conn); // close → WAL folded in, honest file size
    IngestResult {
        label: "plain",
        insert_secs,
        flush_ms: None,
        compact_ms: None,
        file_bytes: db_bytes(path),
    }
}

/// Tier 1: the same rows through the vtab's row-at-a-time SQL interface.
fn bench_tier1(data: &Dataset, path: &str, ext: &str) -> IngestResult {
    scrub(path);
    let conn = open_with_ext(path, ext);
    conn.execute_batch("CREATE VIRTUAL TABLE metrics USING timeless_metrics;")
        .expect("create vtab");

    let t0 = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO metrics(name, ts, value, labels) VALUES (?1, ?2, ?3, ?4)")
            .expect("prepare");
        for i in 0..PTS_PER_SERIES {
            for s in 0..N_SERIES {
                stmt.execute(params![
                    data.name[s],
                    data.ts[s][i],
                    data.val[s][i],
                    data.labels[s]
                ])
                .expect("vtab insert");
            }
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    let insert_secs = t0.elapsed().as_secs_f64(); // insert only, per spec

    let tf = Instant::now();
    conn.execute("INSERT INTO metrics(metrics) VALUES ('flush')", [])
        .expect("flush");
    let flush_ms = tf.elapsed().as_secs_f64() * 1e3;

    let tc = Instant::now();
    conn.execute("INSERT INTO metrics(metrics) VALUES ('compact')", [])
        .expect("compact");
    let compact_ms = tc.elapsed().as_secs_f64() * 1e3;

    drop(conn);
    IngestResult {
        label: "tier1",
        insert_secs,
        flush_ms: Some(flush_ms),
        compact_ms: Some(compact_ms),
        file_bytes: db_bytes(path),
    }
}

/// Tier 2: the same points as 10 pre-encoded 100k-point v0 blobs.
/// Encoding happens BEFORE the clock starts — the measurement is the
/// INSERT statements only (decode + resolve + buffer inside the vtab),
/// which is what a collector's hot path would see.
fn bench_tier2(data: &Dataset, path: &str, ext: &str) -> IngestResult {
    scrub(path);

    let blobs: Vec<Vec<u8>> = (0..N_POINTS / BLOB_POINTS)
        .map(|b| {
            let steps = BLOB_POINTS / N_SERIES; // 100 time steps per blob
            encode_blob(data, b * steps, (b + 1) * steps)
        })
        .collect();

    let conn = open_with_ext(path, ext);
    conn.execute_batch("CREATE VIRTUAL TABLE metrics USING timeless_metrics;")
        .expect("create vtab");

    let t0 = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO metrics(metrics) VALUES (?1)")
            .expect("prepare ingest");
        for blob in &blobs {
            stmt.execute(params![blob]).expect("tier2 ingest");
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    let insert_secs = t0.elapsed().as_secs_f64();

    let tf = Instant::now();
    conn.execute("INSERT INTO metrics(metrics) VALUES ('flush')", [])
        .expect("flush");
    let flush_ms = tf.elapsed().as_secs_f64() * 1e3;

    let tc = Instant::now();
    conn.execute("INSERT INTO metrics(metrics) VALUES ('compact')", [])
        .expect("compact");
    let compact_ms = tc.elapsed().as_secs_f64() * 1e3;

    drop(conn);
    IngestResult {
        label: "tier2",
        insert_secs,
        flush_ms: Some(flush_ms),
        compact_ms: Some(compact_ms),
        file_bytes: db_bytes(path),
    }
}

// ---------------------------------------------------------------------------
// Queries + correctness on the tier2 db
// ---------------------------------------------------------------------------

fn query_checks(data: &Dataset, path: &str, ext: &str) {
    let conn = open_with_ext(path, ext);

    // count(*) — correctness first: every point must have survived
    // flush + compact + reopen.
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM metrics", [], |r| r.get(0))
        .expect("count");
    println!(
        "- count(*) after reopen: {n} ({})",
        if n as usize == N_POINTS { "OK" } else { "MISMATCH — EXPECTED 1000000" }
    );

    // Name + ts-range query (the pushdown path: name EQ + ts GE/LE reach
    // best_index; the engine prunes chunks by series + ts_min/ts_max).
    let lo = BASE_TS + 300 * STEP_MS;
    let hi = BASE_TS + 400 * STEP_MS;
    let tq = Instant::now();
    let (cnt, avg): (i64, f64) = conn
        .query_row(
            "SELECT COUNT(*), AVG(value) FROM metrics
             WHERE name = 'cpu.usage' AND ts >= ?1 AND ts <= ?2",
            params![lo, hi],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("range query");
    println!(
        "- name+range query (cpu.usage, 100 steps, 100 hosts): {cnt} rows, avg {avg:.2}, {:.1} ms",
        tq.elapsed().as_secs_f64() * 1e3
    );

    // Full-scan count, timed (decompresses every chunk of every series).
    let ts = Instant::now();
    let n2: i64 = conn
        .query_row("SELECT COUNT(*) FROM metrics", [], |r| r.get(0))
        .expect("full scan");
    println!(
        "- full-scan count(*): {n2} rows, {:.1} ms",
        ts.elapsed().as_secs_f64() * 1e3
    );

    // Bit-exact spot check: 3 deterministic (series, offset) points. The
    // dataset is still in memory, so "expected" is the exact f64 we
    // generated; anything but to_bits() equality is a lossy pipeline.
    println!("- correctness spot check (bit-exact f64):");
    for &(s, i) in &[(7usize, 0usize), (423, 500), (999, 999)] {
        let want_ts = data.ts[s][i];
        let want = data.val[s][i];
        let got: f64 = conn
            .query_row(
                // name + exact ts + labels: jitter can collide across
                // hosts of one metric, so labels pin the series down.
                "SELECT value FROM metrics WHERE name = ?1 AND ts = ?2 AND labels = ?3",
                params![data.name[s], want_ts, data.labels[s]],
                |r| r.get(0),
            )
            .expect("spot check row missing");
        let ok = got.to_bits() == want.to_bits();
        println!(
            "    series {s} ({} {}) offset {i}: got {got:?}, want {want:?} -> {}",
            data.name[s],
            data.labels[s],
            if ok { "OK" } else { "FAIL (bits differ)" }
        );
        assert!(ok, "bit-exact verification failed for series {s} offset {i}");
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let ext = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: bench <path-to-libtimeless_ext.so>");
        std::process::exit(2);
    });
    assert!(
        Path::new(&ext).exists(),
        "extension not found at {ext} (build with: cargo build -p timeless-ext --release)"
    );

    println!(
        "# timeless bench — {} hosts x {} metrics x {} pts = {} points\n",
        N_HOSTS, N_METRICS, PTS_PER_SERIES, N_POINTS
    );

    let tg = Instant::now();
    let data = generate();
    println!("- generated workload in {:.1} ms", tg.elapsed().as_secs_f64() * 1e3);

    let plain = bench_plain(&data, "/tmp/tl_bench_plain.db");
    println!("- plain baseline done ({:.2}s insert)", plain.insert_secs);
    let tier1 = bench_tier1(&data, "/tmp/tl_bench_tier1.db", &ext);
    println!("- tier1 done ({:.2}s insert)", tier1.insert_secs);
    let tier2 = bench_tier2(&data, "/tmp/tl_bench_tier2.db", &ext);
    println!("- tier2 done ({:.3}s ingest)", tier2.insert_secs);
    println!();

    // ── The markdown table ───────────────────────────────────────────
    println!("| path  | ingest rate | file bytes | bytes/point | size vs plain |");
    println!("|-------|-------------|------------|-------------|---------------|");
    for r in [&plain, &tier1, &tier2] {
        println!(
            "| {} | {} | {} | {:.3} | {:.1}x smaller |",
            r.label,
            fmt_rate(N_POINTS, r.insert_secs),
            fmt_bytes(r.file_bytes),
            r.file_bytes as f64 / N_POINTS as f64,
            plain.file_bytes as f64 / r.file_bytes as f64,
        );
    }
    println!();
    for r in [&tier1, &tier2] {
        println!(
            "- {}: flush {:.1} ms, compact {:.1} ms",
            r.label,
            r.flush_ms.unwrap_or(0.0),
            r.compact_ms.unwrap_or(0.0)
        );
    }
    println!();

    query_checks(&data, "/tmp/tl_bench_tier2.db", &ext);

    println!("\ndone. dbs left in /tmp/tl_bench_{{plain,tier1,tier2}}.db for inspection.");
}
