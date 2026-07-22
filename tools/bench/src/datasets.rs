//! Shared deterministic dataset generators for the bench binaries
//! (bench-logs, bench-traces, bench-codec). Extracted verbatim from
//! bench_logs.rs / bench_traces.rs in Session 7 so the codec bake-off
//! measures THE SAME bytes the end-to-end benchmarks ingest — a codec
//! verdict on a different dataset would be worthless.
//!
//! DO NOT reorder rng calls in the generators: the workloads are
//! deterministic by construction, and every recorded PLAN.md/RESULTS.md
//! number assumes these exact sequences. The only additions over the
//! originals are DERIVED fields (level_num, kind_num, status_num) that
//! consume no randomness — bench-codec needs the numeric forms to feed
//! timeless-core's encoders directly, without re-parsing strings.

// Each bench binary uses a different slice of this module; the unused
// remainder in any one binary is expected, not dead weight.
#![allow(dead_code)]

// ---------------------------------------------------------------------------
// PRNG: xorshift64* (same as main.rs — deterministic, zero deps)
// ---------------------------------------------------------------------------

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Rng((z ^ (z >> 31)) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn bytes<const N: usize>(&mut self) -> [u8; N] {
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
    pub fn duration(&mut self, scale_ns: f64) -> i64 {
        let z = (self.unit() + self.unit() + self.unit()) - 1.5; // ~N(0, 0.5)
        (scale_ns * (z * 1.4).exp()) as i64
    }
}

/// Shared by both workloads: the same 10 services.
pub const SERVICES: [&str; 10] = [
    "api", "web", "auth", "billing", "search", "ingest", "worker", "gateway", "cache", "notify",
];

// ---------------------------------------------------------------------------
// Logs dataset (the Session 5 bench-logs workload, verbatim)
// ---------------------------------------------------------------------------

pub const LOG_ENTRIES: usize = 1_000_000;
pub const LOG_BASE_TS: i64 = 1_700_000_000_000; // unix millis
pub const LOG_STEP_MS: i64 = 3;

pub const LOG_PATHS: [&str; 20] = [
    "/", "/login", "/logout", "/signup", "/checkout", "/cart", "/products", "/products/detail",
    "/search", "/api/v1/users", "/api/v1/orders", "/api/v1/items", "/health", "/metrics",
    "/admin", "/settings", "/profile", "/invoices", "/reports", "/webhooks",
];
pub const LOG_STATUSES: [&str; 6] = ["200", "201", "204", "404", "500", "503"];

pub struct LogRecord {
    pub ts: i64,
    pub level: &'static str,
    /// Derived: 0=debug 1=info 2=warning 3=error (timeless-core's byte).
    pub level_num: u8,
    pub message: String,
    /// Canonical sorted flat JSON (path < service < status), matching
    /// what the vtab emits, so plain-vs-vtab rows are byte-comparable.
    pub metadata: String,
    /// The raw metadata dimensions (bench-codec builds LogEntry pairs
    /// from these instead of re-parsing the JSON).
    pub path: &'static str,
    pub service: &'static str,
    pub status: &'static str,
}

pub fn generate_logs() -> Vec<LogRecord> {
    let mut rng = Rng::new(0x1065); // "LOGS", squint harder
    let mut out = Vec::with_capacity(LOG_ENTRIES);
    let mut ts = LOG_BASE_TS;
    for i in 0..LOG_ENTRIES {
        ts += LOG_STEP_MS + rng.below(3) as i64 - 1; // 2-4ms cadence jitter
        let service = SERVICES[rng.below(SERVICES.len() as u64) as usize];
        let path = LOG_PATHS[rng.below(LOG_PATHS.len() as u64) as usize];

        // Level mix: 70/15/10/5 info/debug/warning/error.
        let roll = rng.below(100);
        let (level, level_num) = if roll < 70 {
            ("info", 1u8)
        } else if roll < 85 {
            ("debug", 0)
        } else if roll < 95 {
            ("warning", 2)
        } else {
            ("error", 3)
        };
        // Server-ish status distribution, correlated with level.
        let status = match level {
            "error" => LOG_STATUSES[3 + rng.below(3) as usize], // 404/500/503
            _ => LOG_STATUSES[rng.below(3) as usize],           // 2xx
        };

        let dur = 1 + rng.below(2000);
        let id = rng.below(1_000_000);
        let message = match level {
            "info" => format!("GET {path} completed in {dur}ms status={status}"),
            "debug" => format!("cache lookup key=user:{id} shard={} hit=true", id % 16),
            "warning" => {
                if i % 3 == 0 {
                    format!("upstream timeout after {dur}ms retrying request {id}")
                } else {
                    format!("slow query took {dur}ms on shard {}", id % 16)
                }
            }
            _ => {
                if i % 2 == 0 {
                    format!("request {id} failed: connect timeout to {service}-backend")
                } else {
                    format!("request {id} failed: internal error (status {status})")
                }
            }
        };
        out.push(LogRecord {
            ts,
            level,
            level_num,
            message,
            metadata: format!(
                "{{\"path\":\"{path}\",\"service\":\"{service}\",\"status\":\"{status}\"}}"
            ),
            path,
            service,
            status,
        });
    }
    out
}

// ---------------------------------------------------------------------------
// Traces dataset (the Session 6 bench-traces workload, verbatim)
// ---------------------------------------------------------------------------

pub const N_TRACES: usize = 100_000;
pub const TRACE_BASE_TS: i64 = 1_700_000_000_000_000_000; // unix ns
pub const TRACE_STEP_NS: i64 = 30_000_000; // one trace every ~30ms

pub const TRACE_NAMES: [&str; 30] = [
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
pub const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];

pub struct SpanRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: &'static str,
    pub service: &'static str,
    pub kind: &'static str,
    /// Derived: 0=internal 1=server 2=client 3=producer 4=consumer.
    pub kind_num: u8,
    pub status: &'static str,
    /// Derived: 0=unset 1=ok 2=error.
    pub status_num: u8,
    pub start_ts: i64,
    pub duration_ns: i64,
    /// Canonical sorted flat JSON (http.method < http.status), matching
    /// what the vtab emits, so plain-vs-vtab rows are byte-comparable.
    pub attributes: String,
    /// The raw attribute dimensions (bench-codec builds SpanEntry pairs
    /// from these instead of re-parsing the JSON).
    pub http_method: &'static str,
    pub http_status: &'static str,
}

fn attrs(method: &str, status: &str) -> String {
    format!("{{\"http.method\":\"{method}\",\"http.status\":\"{status}\"}}")
}

pub fn generate_traces() -> Vec<SpanRecord> {
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

        let trace_start = TRACE_BASE_TS + t as i64 * TRACE_STEP_NS + rng.below(10_000_000) as i64;
        let root_dur = rng.duration(50_000_000.0).max(1_000_000); // ~50ms
        let error_child = 1 + rng.below((n_spans - 1) as u64) as usize;

        let mut span_ids: Vec<[u8; 8]> = Vec::with_capacity(n_spans);
        for i in 0..n_spans {
            let span_id: [u8; 8] = rng.bytes();
            let root = i == 0;
            let this_error = is_error && (root || i == error_child);
            let (kind, kind_num) = if root {
                ("server", 1u8)
            } else {
                [("internal", 0u8), ("client", 2), ("producer", 3), ("consumer", 4)]
                    [rng.below(4) as usize]
            };
            let (status, status_num) = if this_error {
                ("error", 2u8)
            } else if rng.below(5) == 0 {
                ("unset", 0)
            } else {
                ("ok", 1)
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
            out.push(SpanRecord {
                trace_id,
                span_id,
                // Call-chain: parent is a random earlier span.
                parent_span_id: (!root).then(|| span_ids[rng.below(i as u64) as usize]),
                name: TRACE_NAMES[rng.below(TRACE_NAMES.len() as u64) as usize],
                service: SERVICES[rng.below(SERVICES.len() as u64) as usize],
                kind,
                kind_num,
                status,
                status_num,
                start_ts: if root {
                    trace_start
                } else {
                    trace_start + rng.below(root_dur as u64) as i64
                },
                duration_ns: if root { root_dur } else { rng.duration(scale).max(1_000) },
                attributes: attrs(method, http_status),
                http_method: method,
                http_status,
            });
            span_ids.push(span_id);
        }
    }
    out
}
