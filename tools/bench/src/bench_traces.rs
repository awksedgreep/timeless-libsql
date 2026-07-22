//! bench-traces: the Session 6 traces benchmark (PLAN.md "Session 6").
//!
//!   bench-traces <path-to-libtimeless_ext.so>
//!
//! Two-way comparison over the SAME deterministic ~1M-span workload
//! (100k traces × ~10 spans each), against temp dbs in /tmp:
//!
//!   plain - ordinary SQLite table with the SAME columns AND an index
//!           on trace_id. The index is deliberate — "look a trace up
//!           by id" is what people build today, so the plain baseline
//!           gets the tool they would actually reach for (a fair
//!           fight, unlike an unindexed strawman). Ingest pays for the
//!           index maintenance, exactly like a live deployment would.
//!   vtab  - timeless_traces: row INSERTs → 'flush' → 'optimize'.
//!
//! plus query timings on both after a cold reopen. Prints one markdown
//! table — that table IS the artifact. The PLAN target is ~10x smaller
//! than the plain table; the table reports whatever the honest number
//! is.
//!
//! Workload shape ("realistic", not white noise):
//!   - 100k traces; span count per trace 5..=20 skewed low (~80% small
//!     call chains, ~20% fan-outs) → ~1M spans total
//!   - 10 services, 30 operation names; root span is kind=server, the
//!     rest client/internal/producer/consumer with a call-chain parent
//!     structure (parent = a random earlier span of the trace)
//!   - 5% ERROR traces: root + one random child get status=error and
//!     http.status 500/503; everything else ok (80%) or unset
//!   - start_ts in unix NANOSECONDS, one trace every ~30ms (+jitter) →
//!     ~50 min of traffic (inside the vtab's 1h merge-span cap);
//!     spans start within their root's duration
//!   - durations log-normal-ish (exp of a sum of uniforms) with
//!     per-kind scale: servers ~50ms, clients ~10ms, internal ~1ms
//!   - attributes: {http.method, http.status} on every span, status
//!     correlated with error-ness — canonical sorted flat JSON, same
//!     bytes both stores
//!
//! Correctness gates (assert, not report): all query COUNTs match the
//! plain-table oracle; 3 random spans come back BIT-EXACT through the
//! vtab (every column compared against the generated truth, blob ids
//! included); one full trace's span SET is equal in both stores.

use std::env;
use std::fs;
use std::path::Path;
use std::time::Instant;

use rusqlite::{params, Connection};

const N_TRACES: usize = 100_000;
const BASE_TS: i64 = 1_700_000_000_000_000_000; // unix ns
const TRACE_STEP_NS: i64 = 30_000_000; // one trace every ~30ms

const SERVICES: [&str; 10] = [
    "api", "web", "auth", "billing", "search", "ingest", "worker", "gateway", "cache", "notify",
];
const NAMES: [&str; 30] = [
    "GET /",
    "GET /products",
    "GET /products/detail",
    "GET /cart",
    "POST /checkout",
    "POST /login",
    "POST /signup",
    "GET /api/v1/users",
    "GET /api/v1/orders",
    "POST /api/v1/orders",
    "db.query users",
    "db.query orders",
    "db.query products",
    "db.insert orders",
    "db.update inventory",
    "cache.get",
    "cache.set",
    "cache.del",
    "auth.verify_token",
    "auth.refresh",
    "billing.charge",
    "billing.invoice",
    "search.query",
    "search.index",
    "queue.publish",
    "queue.consume",
    "notify.email",
    "notify.push",
    "http.call inventory",
    "http.call shipping",
];
const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];

// ---------------------------------------------------------------------------
// PRNG: xorshift64* (same as main.rs — deterministic, zero deps)
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Rng((z ^ (z >> 31)) | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn bytes<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        for chunk in out.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
        out
    }

    /// Log-normal-ish duration: scale × exp(N(0, ~0.7)). Sum of three
    /// uniforms approximates a normal well enough for a benchmark
    /// (Irwin–Hall); the exp gives the heavy right tail real latency
    /// distributions have.
    fn duration(&mut self, scale_ns: f64) -> i64 {
        let z = (self.unit() + self.unit() + self.unit()) - 1.5; // ~N(0, 0.5)
        (scale_ns * (z * 1.4).exp()) as i64
    }
}

// ---------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------

struct Span {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    name: &'static str,
    service: &'static str,
    kind: &'static str,
    status: &'static str,
    start_ts: i64,
    duration_ns: i64,
    /// Canonical sorted flat JSON (http.method < http.status), matching
    /// what the vtab emits, so plain-vs-vtab rows are byte-comparable.
    attributes: String,
}

fn attrs(method: &str, status: &str) -> String {
    format!("{{\"http.method\":\"{method}\",\"http.status\":\"{status}\"}}")
}

fn generate() -> Vec<Span> {
    let mut rng = Rng::new(0x7247_CE5); // "TRACES", squint
    let mut out = Vec::with_capacity(N_TRACES * 10);
    for t in 0..N_TRACES {
        let trace_id: [u8; 16] = rng.bytes();
        let is_error = rng.below(20) == 0; // 5% error traces
        // Span count 5..=20, skewed low: 80% short chains (5..=11),
        // 20% fan-outs (12..=20) → mean ≈ 10.
        let n_spans = if rng.below(10) < 8 {
            5 + rng.below(7)
        } else {
            12 + rng.below(9)
        } as usize;

        let trace_start = BASE_TS + t as i64 * TRACE_STEP_NS + rng.below(10_000_000) as i64;
        let root_dur = rng.duration(50_000_000.0).max(1_000_000); // ~50ms
        let error_child = 1 + rng.below((n_spans - 1) as u64) as usize;

        let mut span_ids: Vec<[u8; 8]> = Vec::with_capacity(n_spans);
        for i in 0..n_spans {
            let span_id: [u8; 8] = rng.bytes();
            let root = i == 0;
            let this_error = is_error && (root || i == error_child);
            let kind = if root {
                "server"
            } else {
                ["internal", "client", "producer", "consumer"][rng.below(4) as usize]
            };
            let status = if this_error {
                "error"
            } else if rng.below(5) == 0 {
                "unset"
            } else {
                "ok"
            };
            let http_status = if this_error {
                if rng.below(2) == 0 { "500" } else { "503" }
            } else {
                "200"
            };
            let method = METHODS[rng.below(METHODS.len() as u64) as usize];
            let scale = match kind {
                "server" => 50_000_000.0,
                "client" => 10_000_000.0,
                _ => 1_000_000.0,
            };
            out.push(Span {
                trace_id,
                span_id,
                // Call-chain: parent is a random earlier span.
                parent_span_id: (!root).then(|| span_ids[rng.below(i as u64) as usize]),
                name: NAMES[rng.below(NAMES.len() as u64) as usize],
                service: SERVICES[rng.below(SERVICES.len() as u64) as usize],
                kind,
                status,
                start_ts: if root {
                    trace_start
                } else {
                    trace_start + rng.below(root_dur as u64) as i64
                },
                duration_ns: if root { root_dur } else { rng.duration(scale).max(1_000) },
                attributes: attrs(method, http_status),
            });
            span_ids.push(span_id);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Helpers (same shapes as bench_logs.rs)
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

fn fmt_rate(spans: usize, secs: f64) -> String {
    format!("{:.2}M spans/s", spans as f64 / secs / 1.0e6)
}

fn fmt_bytes(b: u64) -> String {
    format!("{b} ({:.1} MB)", b as f64 / 1.0e6)
}

const INSERT_SQL: &str = "INSERT INTO spans(trace_id, span_id, parent_span_id, name, service, \
     kind, status, start_ts, duration_ns, attributes) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)";

fn insert_all(conn: &Connection, data: &[Span], sql: &str) {
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn.prepare(sql).expect("prepare insert");
        for s in data {
            stmt.execute(params![
                &s.trace_id[..],
                &s.span_id[..],
                s.parent_span_id.as_ref().map(|p| &p[..]),
                s.name,
                s.service,
                s.kind,
                s.status,
                s.start_ts,
                s.duration_ns,
                s.attributes,
            ])
            .expect("insert span");
        }
    }
    conn.execute_batch("COMMIT").unwrap();
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

fn bench_plain(data: &[Span], path: &str) -> IngestResult {
    scrub(path);
    let conn = Connection::open(path).expect("open plain db");
    // The index exists BEFORE ingest — that is how a real deployment
    // runs (lookups must work at all times), so the insert loop pays
    // the honest per-row index maintenance cost.
    conn.execute_batch(
        "CREATE TABLE spans(trace_id BLOB, span_id BLOB, parent_span_id BLOB, \
         name TEXT, service TEXT, kind TEXT, status TEXT, \
         start_ts INTEGER, duration_ns INTEGER, attributes TEXT); \
         CREATE INDEX spans_trace ON spans(trace_id);",
    )
    .expect("create plain table");

    let t0 = Instant::now();
    insert_all(&conn, data, INSERT_SQL);
    let insert_secs = t0.elapsed().as_secs_f64();

    drop(conn); // close → WAL folded in, honest file size
    IngestResult {
        label: "plain+idx",
        insert_secs,
        flush_ms: None,
        optimize_ms: None,
        file_bytes: db_bytes(path),
    }
}

fn bench_vtab(data: &[Span], path: &str, ext: &str) -> IngestResult {
    scrub(path);
    let conn = open_with_ext(path, ext);
    // Incremental auto-vacuum BEFORE the db grows (see bench_logs.rs
    // for why the in-extension pragma attempt is too late) so the file
    // shrinks back after 'optimize' deletes the raw tier.
    conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;")
        .expect("set auto_vacuum");
    conn.execute_batch("CREATE VIRTUAL TABLE spans USING timeless_traces;")
        .expect("create traces vtab");

    let t0 = Instant::now();
    insert_all(&conn, data, INSERT_SQL);
    // NOTE: auto-flush fires every 8192 spans, so insert time already
    // contains ~120 status-partitioned raw-block encodes + writes —
    // honest Tier 1 cost, not buffer-only speed.
    let insert_secs = t0.elapsed().as_secs_f64();

    let tf = Instant::now();
    conn.execute("INSERT INTO spans(spans) VALUES ('flush')", [])
        .expect("flush");
    let flush_ms = tf.elapsed().as_secs_f64() * 1e3;

    let to = Instant::now();
    conn.execute("INSERT INTO spans(spans) VALUES ('optimize')", [])
        .expect("optimize");
    let optimize_ms = to.elapsed().as_secs_f64() * 1e3;

    // Return the raw tier's pages to the OS (stepped explicitly — see
    // bench_logs.rs on why execute_batch is unreliable here).
    let tv = Instant::now();
    {
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
// Queries (cold reopen; same logical question to both stores)
// ---------------------------------------------------------------------------

fn time_count(conn: &Connection, label: &str, sql: &str, params: &[&dyn rusqlite::ToSql]) -> i64 {
    let t = Instant::now();
    let n: i64 = conn.query_row(sql, params, |r| r.get(0)).expect(label);
    println!("- {label}: {n} rows, {:.1} ms", t.elapsed().as_secs_f64() * 1e3);
    n
}

/// Average latency of a COUNT-per-trace point lookup over `ids`.
/// Returns (avg_ms, total_rows) — total rows doubles as a cross-check.
fn trace_lookup_avg(conn: &Connection, ids: &[[u8; 16]]) -> (f64, i64) {
    let mut stmt = conn
        .prepare("SELECT COUNT(*) FROM spans WHERE trace_id = ?1")
        .expect("prepare trace lookup");
    let mut total = 0i64;
    let t = Instant::now();
    for id in ids {
        total += stmt
            .query_row(params![&id[..]], |r| r.get::<_, i64>(0))
            .expect("trace lookup");
    }
    (t.elapsed().as_secs_f64() * 1e3 / ids.len() as f64, total)
}

/// All spans of one trace as sorted, comparable tuples.
type SpanRow = (
    Vec<u8>,
    Vec<u8>,
    Option<Vec<u8>>,
    String,
    String,
    String,
    String,
    i64,
    i64,
    String,
);

fn fetch_trace(conn: &Connection, id: &[u8; 16]) -> Vec<SpanRow> {
    let mut stmt = conn
        .prepare(
            "SELECT trace_id, span_id, parent_span_id, name, service, kind, status, \
             start_ts, duration_ns, attributes FROM spans WHERE trace_id = ?1",
        )
        .expect("prepare fetch_trace");
    let mut rows: Vec<SpanRow> = stmt
        .query_map(params![&id[..]], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
                r.get(9)?,
            ))
        })
        .expect("fetch_trace")
        .collect::<Result<_, _>>()
        .expect("fetch_trace rows");
    rows.sort();
    rows
}

fn query_bench(data: &[Span], plain_path: &str, vtab_path: &str, ext: &str) {
    let n_spans = data.len();
    // 100 deterministic "random" trace ids for the point-lookup average
    // (every ~937th trace's root span — spread across the whole file).
    let ids: Vec<[u8; 16]> = data
        .iter()
        .filter(|s| s.parent_span_id.is_none())
        .step_by(937)
        .take(100)
        .map(|s| s.trace_id)
        .collect();
    assert_eq!(ids.len(), 100);
    // Middle-third ts window for the range query.
    let lo = BASE_TS + (N_TRACES as i64) * TRACE_STEP_NS / 3;
    let hi = BASE_TS + 2 * (N_TRACES as i64) * TRACE_STEP_NS / 3;

    println!("\n## Query timings (cold reopen)\n");

    println!("plain table (indexed trace_id):");
    let plain = Connection::open(plain_path).expect("reopen plain");
    let (p_avg, p_rows) = trace_lookup_avg(&plain, &ids);
    println!("- trace_id point lookup: {p_avg:.3} ms avg over 100 traces ({p_rows} spans)");
    let p1 = time_count(&plain, "status='error' count", "SELECT COUNT(*) FROM spans WHERE status='error'", &[]);
    let p2 = time_count(
        &plain,
        "service+range count",
        "SELECT COUNT(*) FROM spans WHERE service='api' AND start_ts >= ?1 AND start_ts <= ?2",
        &[&lo, &hi],
    );
    drop(plain);

    println!("traces vtab:");
    let vtab = open_with_ext(vtab_path, ext);
    let v0 = time_count(&vtab, "count(*) after reopen", "SELECT COUNT(*) FROM spans", &[]);
    assert_eq!(v0 as usize, n_spans, "vtab lost spans across reopen!");
    let (v_avg, v_rows) = trace_lookup_avg(&vtab, &ids);
    println!("- trace_id point lookup: {v_avg:.3} ms avg over 100 traces ({v_rows} spans)");
    let v1 = time_count(&vtab, "status='error' count", "SELECT COUNT(*) FROM spans WHERE status='error'", &[]);
    let v2 = time_count(
        &vtab,
        "service+range count (pushdown)",
        "SELECT COUNT(*) FROM spans WHERE service='api' AND start_ts >= ?1 AND start_ts <= ?2",
        &[&lo, &hi],
    );

    // Cross-checks: the plain table is the oracle.
    assert_eq!(p_rows, v_rows, "trace point-lookup span totals disagree");
    assert_eq!(p1, v1, "status='error' counts disagree");
    assert_eq!(p2, v2, "service+range counts disagree");
    println!("- correctness: lookup totals + both counts match the plain-table oracle");

    // ── Bit-exactness: 3 random spans, every column vs generated truth
    // (the generator IS the ground truth; the plain table only proved
    // itself equal via the counts above). Fetch by trace, match on
    // span_id, compare the lot.
    for &i in &[123_457usize, n_spans / 2, n_spans - 7] {
        let want = &data[i];
        let rows = fetch_trace(&vtab, &want.trace_id);
        let got = rows
            .iter()
            .find(|r| r.1 == want.span_id)
            .unwrap_or_else(|| panic!("span {i}: not found via vtab trace lookup"));
        assert_eq!(got.0, want.trace_id.to_vec(), "span {i}: trace_id");
        assert_eq!(
            got.2,
            want.parent_span_id.map(|p| p.to_vec()),
            "span {i}: parent_span_id"
        );
        assert_eq!(got.3, want.name, "span {i}: name");
        assert_eq!(got.4, want.service, "span {i}: service");
        assert_eq!(got.5, want.kind, "span {i}: kind");
        assert_eq!(got.6, want.status, "span {i}: status");
        assert_eq!(got.7, want.start_ts, "span {i}: start_ts");
        assert_eq!(got.8, want.duration_ns, "span {i}: duration_ns");
        assert_eq!(got.9, want.attributes, "span {i}: attributes");
    }
    println!("- correctness: 3 random spans bit-exact through the vtab (all 10 columns)");

    // ── Full-trace span-set equality, plain vs vtab, one trace.
    let probe = ids[42];
    let plain = Connection::open(plain_path).expect("reopen plain for trace set");
    let plain_set = fetch_trace(&plain, &probe);
    let vtab_set = fetch_trace(&vtab, &probe);
    assert!(!plain_set.is_empty());
    assert_eq!(
        plain_set, vtab_set,
        "full-trace span sets differ between plain and vtab"
    );
    println!(
        "- correctness: full trace ({} spans) identical span set in both stores",
        plain_set.len()
    );
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let ext = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: bench-traces <path-to-libtimeless_ext.so>");
        std::process::exit(2);
    });
    assert!(
        Path::new(&ext).exists(),
        "extension not found at {ext} (build with: cargo build -p timeless-ext --release)"
    );

    let tg = Instant::now();
    let data = generate();
    println!(
        "# timeless bench-traces — {} spans in {N_TRACES} traces, {} services x {} names\n",
        data.len(),
        SERVICES.len(),
        NAMES.len()
    );
    println!("- generated workload in {:.1} ms", tg.elapsed().as_secs_f64() * 1e3);

    let plain = bench_plain(&data, "/tmp/tl_bench_traces_plain.db");
    println!("- plain baseline done ({:.2}s insert)", plain.insert_secs);
    let vtab = bench_vtab(&data, "/tmp/tl_bench_traces_vtab.db", &ext);
    println!("- vtab done ({:.2}s insert)", vtab.insert_secs);
    println!();

    println!("| path  | ingest rate | file bytes | bytes/span | size vs plain |");
    println!("|-------|-------------|------------|------------|---------------|");
    for r in [&plain, &vtab] {
        println!(
            "| {} | {} | {} | {:.2} | {:.1}x smaller |",
            r.label,
            fmt_rate(data.len(), r.insert_secs),
            fmt_bytes(r.file_bytes),
            r.file_bytes as f64 / data.len() as f64,
            plain.file_bytes as f64 / r.file_bytes as f64,
        );
    }
    println!();
    println!(
        "- vtab: flush {:.1} ms, optimize {:.1} ms",
        vtab.flush_ms.unwrap_or(0.0),
        vtab.optimize_ms.unwrap_or(0.0)
    );

    query_bench(
        &data,
        "/tmp/tl_bench_traces_plain.db",
        "/tmp/tl_bench_traces_vtab.db",
        &ext,
    );

    println!("\ndone. dbs left in /tmp/tl_bench_traces_{{plain,vtab}}.db for inspection.");
}
