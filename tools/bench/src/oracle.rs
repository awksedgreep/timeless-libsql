//! oracle: plain-table oracle property test (hardening session, Job 2).
//!
//!   oracle <path-to-libtimeless_ext.so> [seed seed ...]
//!
//! One database, six tables: the three timeless vtabs (metrics, logs,
//! traces) and three ORDINARY SQLite tables mirroring them row for row.
//! A seeded PRNG generates a randomized operation sequence (~50k ops):
//! inserts across varied series/labels/levels/statuses, 'flush' /
//! 'optimize' / 'compact' commands at random points, range/name/level/
//! service/trace_id queries, occasional prune (mirrored by DELETE), and
//! occasional explicit transactions that ROLLBACK on both sides.
//!
//! THE INVARIANT: after every query op, the vtab's result set must
//! equal the plain table's exactly (order-insensitive — both sides are
//! canonicalized to sorted row-strings before comparison). The plain
//! table is trivially correct, so any divergence is a timeless bug: in
//! buffering, flushing, compaction, codecs, pushdown, the term index,
//! the trace index, or the R5 transaction journal.
//!
//! On mismatch: panic printing the SEED and OP INDEX so the exact run
//! replays with `oracle <ext> <seed>`.
//!
//! Design notes (the "why" behind the generator's shape):
//!  - No new deps: the PRNG is splitmix64 (public-domain construction),
//!    deterministic across platforms.
//!  - Values are generated from PRNG bits but always FINITE: pco and
//!    plain REAL columns both round-trip any finite f64 bit-exactly,
//!    and results compare by to_bits(), so floats are exact, not fuzzy.
//!  - Metric timestamps are NON-DECREASING per series, with occasional
//!    DUPLICATES — including across flush boundaries. The old
//!    strictly-increasing workaround (chunk index keyed (series,
//!    min_ts); duplicate-min_ts chunks shadowed each other — see
//!    the chunk-index shadowing fix (2026-07-22, see git history)) is gone: the key now
//!    carries a chunk_seq, and the engine treats duplicate-ts points
//!    as DISTINCT points, exactly like the plain mirror table's rows,
//!    so the invariant holds with duplicates. Logs/traces always got
//!    duplicate timestamps freely.
//!  - Prune is generated as PRUNE-ALL (flush first, cutoff above every
//!    stored ts, mirror = DELETE everything). Partial prune is not
//!    row-mirrorable by design: it deletes whole chunks/blocks below
//!    the cutoff, and a chunk STRADDLING the cutoff keeps all its rows
//!    — the plain-table mirror would have to know block boundaries.
//!    Prune-all still exercises the delete path, index cleanup and
//!    subsequent reuse of the tables. (Partial-prune CONTRACT tests
//!    live in cli.sh sections 4/11/16, where boundaries are staged.)
//!  - Explicit-txn ops mirror both sides inside ONE SQLite transaction
//!    on ONE connection, so ROLLBACK reverts the plain tables and (via
//!    the R5 journal) the vtabs identically. Some txns COMMIT instead,
//!    so both outcomes are exercised. A random 'flush' inside the txn
//!    exercises intra-txn chunk/block-row rollback.

use std::env;
use std::time::Instant;

use rusqlite::{params, Connection};

// ---------------------------------------------------------------------------
// PRNG: splitmix64 — tiny, seedable, deterministic everywhere.
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    /// Uniform in [0, n).
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    /// A finite, exactly-representable value with two decimals of
    /// texture — enough variety for compression paths, zero float fuzz.
    fn value(&mut self) -> f64 {
        (self.below(2_000_000) as f64 - 1_000_000.0) / 100.0
    }
}

// ---------------------------------------------------------------------------
// Workload vocabulary (small pools so queries actually hit data).
// ---------------------------------------------------------------------------

const METRIC_NAMES: [&str; 6] = ["cpu", "mem", "disk", "net", "load", "temp"];
/// Label sets are CANONICAL JSON (sorted keys) because that is what the
/// vtab returns; the plain mirror stores the identical string.
const LABEL_SETS: [&str; 4] = [
    "{}",
    r#"{"host":"a"}"#,
    r#"{"host":"b","zone":"eu"}"#,
    r#"{"host":"c","iface":"eth0","zone":"us"}"#,
];
const LEVELS: [&str; 4] = ["debug", "info", "warning", "error"];
const SERVICES: [&str; 4] = ["api", "web", "db", "cache"];
const KINDS: [&str; 5] = ["internal", "server", "client", "producer", "consumer"];
const STATUSES: [&str; 3] = ["unset", "ok", "error"];
const SPAN_NAMES: [&str; 4] = ["GET /x", "db.query", "cache.get", "publish"];
/// Trace ids drawn from a pool of 48 so trace_id lookups hit multiple
/// spans across multiple blocks.
const N_TRACE_IDS: u64 = 48;

fn trace_id_hex(n: u64) -> String {
    format!("{:032x}", n + 1) // never all-zero
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

fn open_db(path: &str, ext: &str) -> Connection {
    let conn = Connection::open(path).expect("open db");
    unsafe {
        conn.load_extension_enable().expect("enable ext loading");
        conn.load_extension(ext, None::<&str>).expect("load ext");
        conn.load_extension_disable().expect("disable ext loading");
    }
    conn.execute_batch(
        r#"
        CREATE VIRTUAL TABLE metrics USING timeless_metrics;
        CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service');
        CREATE VIRTUAL TABLE traces USING timeless_traces;
        CREATE TABLE plain_metrics(name TEXT, ts INTEGER, value REAL, labels TEXT);
        CREATE TABLE plain_logs(ts INTEGER, level TEXT, message TEXT, metadata TEXT, service TEXT);
        CREATE TABLE plain_traces(trace_id BLOB, span_id BLOB, parent_span_id BLOB,
                                  name TEXT, service TEXT, kind TEXT, status TEXT,
                                  start_ts INTEGER, duration_ns INTEGER, attributes TEXT);
        "#,
    )
    .expect("create tables");
    conn
}

// ---------------------------------------------------------------------------
// Row canonicalization: every result row becomes one string. Floats by
// bit pattern (exactness is the contract), blobs as hex, NULL spelled
// out. Sorting the strings makes comparison order-insensitive.
// ---------------------------------------------------------------------------

fn canon_rows(conn: &Connection, sql: &str, params: &[&dyn rusqlite::ToSql]) -> Vec<String> {
    let mut stmt = conn.prepare_cached(sql).expect("prepare");
    let ncols = stmt.column_count();
    let mut rows = stmt.query(params).expect("query");
    let mut out = Vec::new();
    while let Some(row) = rows.next().expect("row") {
        let mut s = String::new();
        for i in 0..ncols {
            use rusqlite::types::ValueRef::*;
            match row.get_ref(i).expect("col") {
                Null => s.push_str("N"),
                Integer(v) => s.push_str(&v.to_string()),
                Real(v) => s.push_str(&format!("f{:016x}", v.to_bits())),
                Text(t) => {
                    s.push('t');
                    s.push_str(&String::from_utf8_lossy(t));
                }
                Blob(b) => {
                    s.push('b');
                    for byte in b {
                        s.push_str(&format!("{byte:02x}"));
                    }
                }
            }
            s.push('|');
        }
        out.push(s);
    }
    out.sort();
    out
}

/// Compare vtab vs plain for one query pair; panic with replay info on
/// mismatch.
#[allow(clippy::too_many_arguments)]
fn check(
    conn: &Connection,
    seed: u64,
    op: usize,
    what: &str,
    vtab_sql: &str,
    plain_sql: &str,
    params: &[&dyn rusqlite::ToSql],
) {
    let got = canon_rows(conn, vtab_sql, params);
    let want = canon_rows(conn, plain_sql, params);
    if got != want {
        eprintln!("ORACLE MISMATCH  seed={seed}  op={op}  query={what}");
        eprintln!("  vtab : {} rows   plain: {} rows", got.len(), want.len());
        for g in got.iter().filter(|g| !want.contains(g)).take(5) {
            eprintln!("  only in vtab : {g}");
        }
        for w in want.iter().filter(|w| !got.contains(w)).take(5) {
            eprintln!("  only in plain: {w}");
        }
        eprintln!("replay: oracle <ext.so> {seed}");
        panic!("oracle mismatch at seed={seed} op={op} ({what})");
    }
}

// ---------------------------------------------------------------------------
// The op sequence
// ---------------------------------------------------------------------------

const N_OPS: usize = 50_000;

fn run_seed(ext: &str, seed: u64) {
    let path = format!(
        "{}/timeless_oracle_{}_{}.db",
        std::env::temp_dir().display(),
        std::process::id(),
        seed
    );
    let _ = std::fs::remove_file(&path);
    let conn = open_db(&path, ext);
    let mut rng = Rng::new(seed);

    // Per-series non-decreasing metric timestamps (see header).
    let mut metric_ts: Vec<i64> = vec![1_700_000_000; METRIC_NAMES.len() * LABEL_SETS.len()];
    let mut log_seq: u64 = 0; // message uniqueness counter
    let mut span_seq: u64 = 0; // span_id uniqueness counter
    let mut queries = 0usize;
    let mut txns = 0usize;
    let mut prunes = 0usize;

    let t0 = Instant::now();
    let mut op = 0usize;
    while op < N_OPS {
        let roll = rng.below(100);
        if roll < 85 {
            // ── insert one row into a random signal + its mirror ────
            insert_one(
                &conn,
                &mut rng,
                &mut metric_ts,
                &mut log_seq,
                &mut span_seq,
            );
        } else if roll < 90 {
            // ── maintenance command on a random vtab. The mirror is
            //    untouched: flush/optimize/compact must be INVISIBLE to
            //    queries — that is exactly what the oracle checks.
            let cmd_sql = match rng.below(4) {
                0 => "INSERT INTO metrics(metrics) VALUES ('flush')",
                1 => "INSERT INTO metrics(metrics) VALUES ('compact')",
                2 => "INSERT INTO logs(logs) VALUES ('flush')",
                _ => "INSERT INTO traces(traces) VALUES ('flush')",
            };
            conn.execute(cmd_sql, []).expect("command");
            // optimize occasionally, after flushes exist to chew on.
            if rng.below(3) == 0 {
                conn.execute("INSERT INTO logs(logs) VALUES ('optimize')", [])
                    .expect("optimize logs");
                conn.execute("INSERT INTO traces(traces) VALUES ('optimize')", [])
                    .expect("optimize traces");
            }
        } else if roll < 96 {
            // ── query + compare ─────────────────────────────────────
            queries += 1;
            run_query(&conn, &mut rng, seed, op);
        } else if roll < 98 {
            // ── explicit transaction, mirrored on both sides, ending
            //    in ROLLBACK (2 of 3) or COMMIT (1 of 3) ──────────────
            txns += 1;
            let commit = rng.below(3) == 0;
            conn.execute_batch("BEGIN").expect("begin");
            let n = 1 + rng.below(20);
            for _ in 0..n {
                insert_one(
                    &conn,
                    &mut rng,
                    &mut metric_ts,
                    &mut log_seq,
                    &mut span_seq,
                );
                op += 1;
            }
            // Half the transactions flush INSIDE — the R5 case where
            // chunk/block rows are born mid-txn and must die with it.
            if rng.below(2) == 0 {
                conn.execute("INSERT INTO metrics(metrics) VALUES ('flush')", [])
                    .expect("flush in txn");
                conn.execute("INSERT INTO logs(logs) VALUES ('flush')", [])
                    .expect("flush in txn");
                conn.execute("INSERT INTO traces(traces) VALUES ('flush')", [])
                    .expect("flush in txn");
            }
            conn.execute_batch(if commit { "COMMIT" } else { "ROLLBACK" })
                .expect("end txn");
            if !commit {
                // The mirrors rolled back too, but metric_ts marched
                // on — harmless (timestamps stay non-decreasing; gaps
                // are fine). Immediately cross-check all three.
                run_all_full_checks(&conn, seed, op);
            }
        } else if rng.below(16) != 0 {
            // Prune slot, 15/16 of the time demoted to another insert:
            // full prunes are cheap to exercise but each one resets the
            // tables, and a run that prunes every ~50 ops never grows
            // deep enough to make optimize/compact merge REAL block
            // populations. ~60 prune-alls per 50k ops keeps both.
            insert_one(
                &conn,
                &mut rng,
                &mut metric_ts,
                &mut log_seq,
                &mut span_seq,
            );
        } else {
            // ── prune-all, mirrored by DELETE (see header) ──────────
            prunes += 1;
            conn.execute_batch(
                "INSERT INTO metrics(metrics) VALUES ('flush');
                 INSERT INTO logs(logs) VALUES ('flush');
                 INSERT INTO traces(traces) VALUES ('flush');",
            )
            .expect("flush before prune");
            // Cutoff above every ts each generator can have produced —
            // PER SIGNAL, because the units differ: metrics are epoch
            // seconds (~1.7e9), logs milliseconds (~1.7e12), traces
            // nanoseconds (~1.7e18). One shared cutoff was this test's
            // own first bug: 9e9 pruned metrics but left log/trace
            // blocks alive while the mirrors were emptied.
            conn.execute_batch(
                "INSERT INTO metrics(metrics) VALUES ('prune:9000000000');
                 INSERT INTO logs(logs) VALUES ('prune:9000000000000');
                 INSERT INTO traces(traces) VALUES ('prune:9000000000000000000');
                 DELETE FROM plain_metrics;
                 DELETE FROM plain_logs;
                 DELETE FROM plain_traces;",
            )
            .expect("prune-all");
            run_all_full_checks(&conn, seed, op);
        }
        op += 1;
    }

    // Final: flush everything and compare complete contents.
    conn.execute_batch(
        "INSERT INTO metrics(metrics) VALUES ('flush');
         INSERT INTO logs(logs) VALUES ('flush');
         INSERT INTO traces(traces) VALUES ('flush');
         INSERT INTO logs(logs) VALUES ('optimize');
         INSERT INTO traces(traces) VALUES ('optimize');
         INSERT INTO metrics(metrics) VALUES ('compact');",
    )
    .expect("final flush");
    run_all_full_checks(&conn, seed, N_OPS);

    // Reopen from disk (fresh connection = xConnect recovery) and
    // compare once more: recovery must reproduce the same world.
    drop(conn);
    let conn = open_db_existing(&path, ext);
    run_all_full_checks(&conn, seed, N_OPS + 1);
    drop(conn);
    let _ = std::fs::remove_file(&path);

    println!(
        "seed {seed}: {N_OPS} ops OK ({queries} query checks, {txns} txns, {prunes} prune-alls) in {:.1}s",
        t0.elapsed().as_secs_f64()
    );
}

fn open_db_existing(path: &str, ext: &str) -> Connection {
    let conn = Connection::open(path).expect("reopen db");
    unsafe {
        conn.load_extension_enable().expect("enable ext loading");
        conn.load_extension(ext, None::<&str>).expect("load ext");
        conn.load_extension_disable().expect("disable ext loading");
    }
    conn
}

/// One mirrored insert into a random signal.
fn insert_one(
    conn: &Connection,
    rng: &mut Rng,
    metric_ts: &mut [i64],
    log_seq: &mut u64,
    span_seq: &mut u64,
) {
    match rng.below(3) {
        0 => {
            let name_i = rng.below(METRIC_NAMES.len() as u64) as usize;
            let label_i = rng.below(LABEL_SETS.len() as u64) as usize;
            let series = name_i * LABEL_SETS.len() + label_i;
            // Non-decreasing per series; a 0 step (~1 in 50) produces a
            // duplicate timestamp, sometimes straddling a flush — the
            // exact duplicate-min_ts-chunk shape the widened chunk key
            // fixed (see module header).
            metric_ts[series] += rng.below(50) as i64;
            let (name, labels, ts, val) = (
                METRIC_NAMES[name_i],
                LABEL_SETS[label_i],
                metric_ts[series],
                rng.value(),
            );
            conn.execute(
                "INSERT INTO metrics(name, ts, value, labels) VALUES (?1, ?2, ?3, ?4)",
                params![name, ts, val, labels],
            )
            .expect("vtab metric insert");
            conn.execute(
                "INSERT INTO plain_metrics(name, ts, value, labels) VALUES (?1, ?2, ?3, ?4)",
                params![name, ts, val, labels],
            )
            .expect("plain metric insert");
        }
        1 => {
            *log_seq += 1;
            let level = LEVELS[rng.below(4) as usize];
            let service = SERVICES[rng.below(4) as usize];
            let ts = 1_700_000_000_000i64 + rng.below(3_600_000) as i64; // dupes allowed
            let message = format!("event-{log_seq} in {service}");
            // Canonical metadata: the vtab returns sorted flat JSON, so
            // the mirror stores the identical canonical string.
            let metadata = format!(r#"{{"service":"{service}"}}"#);
            conn.execute(
                "INSERT INTO logs(ts, level, message, metadata) VALUES (?1, ?2, ?3, ?4)",
                params![ts, level, message, metadata],
            )
            .expect("vtab log insert");
            conn.execute(
                "INSERT INTO plain_logs(ts, level, message, metadata, service) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![ts, level, message, metadata, service],
            )
            .expect("plain log insert");
        }
        _ => {
            *span_seq += 1;
            let tid = trace_id_hex(rng.below(N_TRACE_IDS));
            let tid_blob: Vec<u8> = (0..16)
                .map(|i| u8::from_str_radix(&tid[i * 2..i * 2 + 2], 16).unwrap())
                .collect();
            let span_id: Vec<u8> = span_seq.to_be_bytes().to_vec();
            let parent: Option<Vec<u8>> = if rng.below(2) == 0 {
                Some((*span_seq / 2).max(1).to_be_bytes().to_vec())
            } else {
                None
            };
            let name = SPAN_NAMES[rng.below(4) as usize];
            let service = SERVICES[rng.below(4) as usize];
            let kind = KINDS[rng.below(5) as usize];
            let status = STATUSES[rng.below(3) as usize];
            let start_ts = 1_700_000_000_000_000_000i64 + rng.below(3_600_000_000) as i64;
            let duration = rng.below(1_000_000_000) as i64;
            let attrs = if rng.below(3) == 0 {
                format!(r#"{{"peer":"{service}"}}"#) // canonical (1 key)
            } else {
                "{}".to_string()
            };
            conn.execute(
                "INSERT INTO traces(trace_id, span_id, parent_span_id, name, service, kind, status, start_ts, duration_ns, attributes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![tid_blob, span_id, parent, name, service, kind, status, start_ts, duration, attrs],
            )
            .expect("vtab span insert");
            conn.execute(
                "INSERT INTO plain_traces(trace_id, span_id, parent_span_id, name, service, kind, status, start_ts, duration_ns, attributes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![tid_blob, span_id, parent, name, service, kind, status, start_ts, duration, attrs],
            )
            .expect("plain span insert");
        }
    }
}

/// One randomized query pair, compared. Every query family exercises a
/// different pushdown plan (name-eq, ts-range, level/term, trace-index).
fn run_query(conn: &Connection, rng: &mut Rng, seed: u64, op: usize) {
    match rng.below(8) {
        0 => {
            // metrics: name equality (the pushdown plan).
            let name = METRIC_NAMES[rng.below(6) as usize];
            check(
                conn, seed, op, "metrics name",
                "SELECT name, ts, value, labels FROM metrics WHERE name = ?1",
                "SELECT name, ts, value, labels FROM plain_metrics WHERE name = ?1",
                &[&name],
            );
        }
        1 => {
            // metrics: name + ts range.
            let name = METRIC_NAMES[rng.below(6) as usize];
            let lo = 1_700_000_000i64 + rng.below(400_000) as i64;
            let hi = lo + rng.below(400_000) as i64;
            check(
                conn, seed, op, "metrics name+range",
                "SELECT name, ts, value, labels FROM metrics WHERE name = ?1 AND ts >= ?2 AND ts <= ?3",
                "SELECT name, ts, value, labels FROM plain_metrics WHERE name = ?1 AND ts >= ?2 AND ts <= ?3",
                &[&name, &lo, &hi],
            );
        }
        2 => {
            // metrics: full scan (no pushdown at all).
            check(
                conn, seed, op, "metrics full",
                "SELECT name, ts, value, labels FROM metrics",
                "SELECT name, ts, value, labels FROM plain_metrics",
                &[],
            );
        }
        3 => {
            // logs: level equality (posting-list plan).
            let level = LEVELS[rng.below(4) as usize];
            check(
                conn, seed, op, "logs level",
                "SELECT ts, level, message, metadata FROM logs WHERE level = ?1",
                "SELECT ts, level, message, metadata FROM plain_logs WHERE level = ?1",
                &[&level],
            );
        }
        4 => {
            // logs: service (indexed hidden column) + ts range.
            let service = SERVICES[rng.below(4) as usize];
            let lo = 1_700_000_000_000i64 + rng.below(1_800_000) as i64;
            let hi = lo + rng.below(1_800_000) as i64;
            check(
                conn, seed, op, "logs service+range",
                "SELECT ts, level, message, metadata FROM logs WHERE service = ?1 AND ts >= ?2 AND ts <= ?3",
                "SELECT ts, level, message, metadata FROM plain_logs WHERE service = ?1 AND ts >= ?2 AND ts <= ?3",
                &[&service, &lo, &hi],
            );
        }
        5 => {
            // traces: trace_id point lookup (the hero pushdown).
            let tid = trace_id_hex(rng.below(N_TRACE_IDS));
            let tid_blob: Vec<u8> = (0..16)
                .map(|i| u8::from_str_radix(&tid[i * 2..i * 2 + 2], 16).unwrap())
                .collect();
            check(
                conn, seed, op, "traces trace_id",
                "SELECT hex(trace_id), hex(span_id), name, service, kind, status, start_ts, duration_ns, attributes FROM traces WHERE trace_id = ?1",
                "SELECT hex(trace_id), hex(span_id), name, service, kind, status, start_ts, duration_ns, attributes FROM plain_traces WHERE trace_id = ?1",
                &[&tid_blob],
            );
        }
        6 => {
            // traces: status (partition dimension) + service.
            let status = STATUSES[rng.below(3) as usize];
            let service = SERVICES[rng.below(4) as usize];
            check(
                conn, seed, op, "traces status+service",
                "SELECT hex(trace_id), hex(span_id), name, start_ts FROM traces WHERE status = ?1 AND service = ?2",
                "SELECT hex(trace_id), hex(span_id), name, start_ts FROM plain_traces WHERE status = ?1 AND service = ?2",
                &[&status, &service],
            );
        }
        _ => {
            // traces: start_ts range only.
            let lo = 1_700_000_000_000_000_000i64 + rng.below(1_800_000_000) as i64;
            let hi = lo + rng.below(1_800_000_000) as i64;
            check(
                conn, seed, op, "traces range",
                "SELECT hex(span_id), name, status, start_ts FROM traces WHERE start_ts >= ?1 AND start_ts <= ?2",
                "SELECT hex(span_id), name, status, start_ts FROM plain_traces WHERE start_ts >= ?1 AND start_ts <= ?2",
                &[&lo, &hi],
            );
        }
    }
}

/// Full-content comparison of all three signals (used after rollback,
/// prune-all, the final flush, and the reopen).
fn run_all_full_checks(conn: &Connection, seed: u64, op: usize) {
    check(
        conn, seed, op, "metrics full-check",
        "SELECT name, ts, value, labels FROM metrics",
        "SELECT name, ts, value, labels FROM plain_metrics",
        &[],
    );
    check(
        conn, seed, op, "logs full-check",
        "SELECT ts, level, message, metadata FROM logs",
        "SELECT ts, level, message, metadata FROM plain_logs",
        &[],
    );
    check(
        conn, seed, op, "traces full-check",
        "SELECT hex(trace_id), hex(span_id), CASE WHEN parent_span_id IS NULL THEN 'N' ELSE hex(parent_span_id) END, name, service, kind, status, start_ts, duration_ns, attributes FROM traces",
        "SELECT hex(trace_id), hex(span_id), CASE WHEN parent_span_id IS NULL THEN 'N' ELSE hex(parent_span_id) END, name, service, kind, status, start_ts, duration_ns, attributes FROM plain_traces",
        &[],
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: oracle <path-to-libtimeless_ext.so> [seed ...]");
        std::process::exit(2);
    }
    let ext = &args[1];
    // Three fixed default seeds (CI determinism); pass explicit seeds
    // to replay a reported failure.
    let seeds: Vec<u64> = if args.len() > 2 {
        args[2..]
            .iter()
            .map(|s| s.parse().expect("seed must be a u64"))
            .collect()
    } else {
        vec![0xA11CE, 0xB0B5EED, 0xC0FFEE]
    };
    let t0 = Instant::now();
    for &seed in &seeds {
        run_seed(ext, seed);
    }
    println!(
        "oracle: {} seed(s) passed in {:.1}s",
        seeds.len(),
        t0.elapsed().as_secs_f64()
    );
}
