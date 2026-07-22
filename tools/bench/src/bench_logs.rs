//! bench-logs: the Session 5 logs benchmark (PLAN.md "Session 5").
//!
//!   bench-logs <path-to-libtimeless_ext.so>
//!
//! Two-way comparison over the SAME deterministic 1M-entry realistic
//! log workload, against temp dbs in /tmp:
//!
//!   plain - ordinary SQLite table (ts INTEGER, level TEXT,
//!           message TEXT, metadata TEXT), prepared INSERT loop
//!   vtab  - timeless_logs(index_keys='service,path,status'):
//!           row INSERTs → 'flush' → 'optimize'
//!
//! plus query timings on both after a cold reopen. Prints one markdown
//! table — that table IS the artifact. The PLAN target is a ≥10x
//! smaller file than the plain table; the table reports whatever the
//! honest number is.
//!
//! Workload shape ("realistic", not white noise — compressors and the
//! term index both care):
//!   - 10 services, 20 paths, 6 statuses (low-cardinality metadata,
//!     exactly what index_keys is for)
//!   - level mix 70% info / 15% debug / 10% warning / 5% error
//!   - templated messages with variable ids/durations (repetitive
//!     structure, unique values — the log-compression sweet spot);
//!     a subset of warning/error messages contain "timeout" for the
//!     LIKE benchmark
//!   - timestamps in unix millis, ~3ms cadence with jitter (≈50 min of
//!     traffic, so the vtab's 1h merge-span cap never fragments it)

mod datasets;

use std::env;
use std::fs;
use std::path::Path;
use std::time::Instant;

use rusqlite::{params, Connection};

// The workload lives in the shared datasets module (Session 7 codec
// bake-off refactor) so bench-codec measures the exact bytes this
// benchmark ingests. Aliased to the original local names to keep the
// benchmark body diff-minimal against the recorded Session 5 runs.
use datasets::{
    generate_logs as generate, LogRecord as Entry, LOG_BASE_TS as BASE_TS,
    LOG_ENTRIES as N_ENTRIES, LOG_PATHS as PATHS, LOG_STATUSES as STATUSES,
    LOG_STEP_MS as STEP_MS, SERVICES,
};

// ---------------------------------------------------------------------------
// Helpers (same shapes as main.rs)
// ---------------------------------------------------------------------------

fn scrub(path: &str) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = fs::remove_file(format!("{path}{suffix}"));
    }
}

fn db_bytes(path: &str) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn open_with_ext(path: &str, ext: &str) -> Connection {
    let conn = Connection::open(path).expect("open db");
    unsafe {
        conn.load_extension_enable().expect("enable ext loading");
        conn.load_extension(ext, None::<&str>).expect("load extension");
    }
    conn.load_extension_disable().expect("disable ext loading");
    conn
}

fn fmt_rate(entries: usize, secs: f64) -> String {
    format!("{:.2}M entries/s", entries as f64 / secs / 1.0e6)
}

fn fmt_bytes(b: u64) -> String {
    format!("{b} ({:.1} MB)", b as f64 / 1.0e6)
}

// ---------------------------------------------------------------------------
// Ingest benchmarks
// ---------------------------------------------------------------------------

struct IngestResult {
    label: &'static str,
    insert_secs: f64,
    flush_ms: Option<f64>,
    optimize_ms: Option<f64>,
    file_bytes: u64,
}

fn bench_plain(data: &[Entry], path: &str) -> IngestResult {
    scrub(path);
    let conn = Connection::open(path).expect("open plain db");
    conn.execute_batch(
        "CREATE TABLE logs(ts INTEGER, level TEXT, message TEXT, metadata TEXT);",
    )
    .expect("create plain table");

    let t0 = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO logs(ts, level, message, metadata) VALUES (?1, ?2, ?3, ?4)")
            .expect("prepare");
        for e in data {
            stmt.execute(params![e.ts, e.level, e.message, e.metadata])
                .expect("insert row");
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    let insert_secs = t0.elapsed().as_secs_f64();

    drop(conn); // close → WAL folded in, honest file size
    IngestResult {
        label: "plain",
        insert_secs,
        flush_ms: None,
        optimize_ms: None,
        file_bytes: db_bytes(path),
    }
}

fn bench_vtab(data: &[Entry], path: &str, ext: &str) -> IngestResult {
    scrub(path);
    let conn = open_with_ext(path, ext);
    // PLAN.md "Pruning & retention": incremental auto-vacuum, set
    // BEFORE the database grows. The vtab attempts this in xCreate too,
    // but by then the CREATE VIRTUAL TABLE statement has already
    // allocated pages, so the in-extension attempt is a silent no-op —
    // a deployment that wants its file to shrink after 'optimize' (raw
    // blocks are deleted wholesale there) sets the pragma at db
    // creation, exactly like this.
    conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;")
        .expect("set auto_vacuum");
    conn.execute_batch(
        "CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service,path,status');",
    )
    .expect("create logs vtab");

    let t0 = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO logs(ts, level, message, metadata) VALUES (?1, ?2, ?3, ?4)")
            .expect("prepare");
        for e in data {
            stmt.execute(params![e.ts, e.level, e.message, e.metadata])
                .expect("vtab insert");
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    // NOTE: auto-flush fires inside the loop every 8192 entries, so the
    // insert time already contains ~120 raw-block encodes + writes —
    // that is the honest Tier 1 ingest cost, not buffer-only speed.
    let insert_secs = t0.elapsed().as_secs_f64();

    let tf = Instant::now();
    conn.execute("INSERT INTO logs(logs) VALUES ('flush')", [])
        .expect("flush");
    let flush_ms = tf.elapsed().as_secs_f64() * 1e3;

    let to = Instant::now();
    conn.execute("INSERT INTO logs(logs) VALUES ('optimize')", [])
        .expect("optimize");
    let optimize_ms = to.elapsed().as_secs_f64() * 1e3;

    // Maintenance step (PLAN.md): 'optimize' deleted ~120 raw blocks'
    // worth of pages; incremental_vacuum returns them to the OS so the
    // file size reflects the compressed data, not the transient raw
    // tier's high-water mark. Never a full VACUUM (whole-file rewrite).
    let tv = Instant::now();
    {
        // Stepped explicitly: incremental_vacuum does its work during
        // sqlite3_step, and execute_batch() proved unreliable for it
        // (returned Ok without freeing anything); driving the rows
        // cursor to completion is unambiguous.
        let mut stmt = conn
            .prepare("PRAGMA incremental_vacuum;")
            .expect("prepare incremental_vacuum");
        let mut rows = stmt.query([]).expect("run incremental_vacuum");
        while rows.next().expect("step incremental_vacuum").is_some() {}
    }
    println!(
        "- incremental_vacuum after optimize: {:.1} ms",
        tv.elapsed().as_secs_f64() * 1e3
    );

    drop(conn);
    IngestResult {
        label: "vtab",
        insert_secs,
        flush_ms: Some(flush_ms),
        optimize_ms: Some(optimize_ms),
        file_bytes: db_bytes(path),
    }
}

// ---------------------------------------------------------------------------
// Queries (cold reopen on both dbs; same logical question each time)
// ---------------------------------------------------------------------------

fn time_count(conn: &Connection, label: &str, sql: &str, params: &[&dyn rusqlite::ToSql]) -> i64 {
    let t = Instant::now();
    let n: i64 = conn.query_row(sql, params, |r| r.get(0)).expect(label);
    println!("- {label}: {n} rows, {:.1} ms", t.elapsed().as_secs_f64() * 1e3);
    n
}

fn query_bench(plain_path: &str, vtab_path: &str, ext: &str) {
    // Middle-third ts window for the range query.
    let lo = BASE_TS + (N_ENTRIES as i64) * STEP_MS / 3;
    let hi = BASE_TS + 2 * (N_ENTRIES as i64) * STEP_MS / 3;

    println!("\n## Query timings (cold reopen)\n");

    println!("plain table:");
    let plain = Connection::open(plain_path).expect("reopen plain");
    let p1 = time_count(&plain, "level=error count", "SELECT COUNT(*) FROM logs WHERE level='error'", &[]);
    let p2 = time_count(
        &plain,
        "service+level+range",
        "SELECT COUNT(*) FROM logs WHERE metadata LIKE '%\"service\":\"api\"%' \
         AND level='error' AND ts >= ?1 AND ts <= ?2",
        &[&lo, &hi],
    );
    let p3 = time_count(
        &plain,
        "message LIKE '%timeout%'",
        "SELECT COUNT(*) FROM logs WHERE message LIKE '%timeout%'",
        &[],
    );
    drop(plain);

    println!("logs vtab:");
    let vtab = open_with_ext(vtab_path, ext);
    let v0 = time_count(&vtab, "count(*) after reopen", "SELECT COUNT(*) FROM logs", &[]);
    assert_eq!(v0 as usize, N_ENTRIES, "vtab lost entries across reopen!");
    let v1 = time_count(&vtab, "level=error count", "SELECT COUNT(*) FROM logs WHERE level='error'", &[]);
    let v2 = time_count(
        &vtab,
        "service+level+range (pushdown)",
        "SELECT COUNT(*) FROM logs WHERE service='api' AND level='error' \
         AND ts >= ?1 AND ts <= ?2",
        &[&lo, &hi],
    );
    let v3 = time_count(
        &vtab,
        "message LIKE '%timeout%'",
        "SELECT COUNT(*) FROM logs WHERE message LIKE '%timeout%'",
        &[],
    );
    drop(vtab);

    // Cross-check: both stores must agree on every count (the plain
    // table is the oracle).
    assert_eq!(p1, v1, "level=error counts disagree");
    assert_eq!(p2, v2, "service+level+range counts disagree");
    assert_eq!(p3, v3, "timeout LIKE counts disagree");
    println!("- correctness: all three counts match the plain-table oracle");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let ext = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: bench-logs <path-to-libtimeless_ext.so>");
        std::process::exit(2);
    });
    assert!(
        Path::new(&ext).exists(),
        "extension not found at {ext} (build with: cargo build -p timeless-ext --release)"
    );

    println!(
        "# timeless bench-logs — {N_ENTRIES} entries, {} services x {} paths x {} statuses\n",
        SERVICES.len(),
        PATHS.len(),
        STATUSES.len()
    );

    let tg = Instant::now();
    let data = generate();
    println!("- generated workload in {:.1} ms", tg.elapsed().as_secs_f64() * 1e3);

    let plain = bench_plain(&data, "/tmp/tl_bench_logs_plain.db");
    println!("- plain baseline done ({:.2}s insert)", plain.insert_secs);
    let vtab = bench_vtab(&data, "/tmp/tl_bench_logs_vtab.db", &ext);
    println!("- vtab done ({:.2}s insert)", vtab.insert_secs);
    println!();

    println!("| path  | ingest rate | file bytes | bytes/entry | size vs plain |");
    println!("|-------|-------------|------------|-------------|---------------|");
    for r in [&plain, &vtab] {
        println!(
            "| {} | {} | {} | {:.2} | {:.1}x smaller |",
            r.label,
            fmt_rate(N_ENTRIES, r.insert_secs),
            fmt_bytes(r.file_bytes),
            r.file_bytes as f64 / N_ENTRIES as f64,
            plain.file_bytes as f64 / r.file_bytes as f64,
        );
    }
    println!();
    println!(
        "- vtab: flush {:.1} ms, optimize {:.1} ms",
        vtab.flush_ms.unwrap_or(0.0),
        vtab.optimize_ms.unwrap_or(0.0)
    );

    query_bench("/tmp/tl_bench_logs_plain.db", "/tmp/tl_bench_logs_vtab.db", &ext);

    println!("\ndone. dbs left in /tmp/tl_bench_logs_{{plain,vtab}}.db for inspection.");
}
