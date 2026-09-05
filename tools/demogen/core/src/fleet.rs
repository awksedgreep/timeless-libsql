//! Deterministic synthetic fleet: services × pods, each pod exporting a
//! Prometheus-style metric catalog, plus correlated request logs and
//! traces. Everything is a pure function of the seed and the time window,
//! so a demo can be re-run and produce the same shapes.
//!
//! The workload deliberately includes one INCIDENT: for the middle third
//! of the seeded window, one service's cpu climbs, its HTTP 500 counters
//! jump, its latency sum inflates, its logs turn error-heavy, and its
//! traces slow down and fail — so a screencast has something to find.

use crate::blobs::{LogEntry, SpanEntry};

// ---------------------------------------------------------------------------
// PRNG: xorshift64* (same as tools/bench — deterministic, zero deps)
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

    pub fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + self.unit() * (hi - lo)
    }

    pub fn bytes<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        for chunk in out.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
        out
    }

    /// Log-normal-ish duration: scale × exp(N(0, ~0.7)) (Irwin–Hall
    /// approximation, heavy right tail like real latencies).
    pub fn duration(&mut self, scale_ns: f64) -> i64 {
        let z = (self.unit() + self.unit() + self.unit()) - 1.5;
        (scale_ns * (z * 1.4).exp()) as i64
    }
}

// ---------------------------------------------------------------------------
// Name pools
// ---------------------------------------------------------------------------

pub const SERVICE_POOL: [&str; 25] = [
    "api",
    "web",
    "auth",
    "billing",
    "search",
    "ingest",
    "worker",
    "gateway",
    "cache",
    "notify",
    "orders",
    "inventory",
    "shipping",
    "payments",
    "users",
    "catalog",
    "email",
    "push",
    "analytics",
    "export",
    "scheduler",
    "webhooks",
    "media",
    "sessions",
    "audit",
];

pub const PATH_POOL: [&str; 20] = [
    "/",
    "/login",
    "/logout",
    "/signup",
    "/checkout",
    "/cart",
    "/products",
    "/products/detail",
    "/search",
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v1/items",
    "/health",
    "/metrics",
    "/admin",
    "/settings",
    "/profile",
    "/invoices",
    "/reports",
    "/webhooks",
];

pub const ZONES: [&str; 6] = [
    "us-east-1a",
    "us-east-1b",
    "us-west-2a",
    "us-west-2b",
    "eu-west-1a",
    "eu-west-1b",
];

pub const SPAN_NAMES: [&str; 30] = [
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
// Configuration and the incident window
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Config {
    pub seed: u64,
    pub services: usize,
    pub pods: usize,
    /// HTTP paths exported per pod (drives most of the cardinality).
    pub paths: usize,
    pub minutes: u64,
    pub step_secs: u64,
    pub logs: usize,
    pub traces: usize,
    /// End of the seeded window, unix millis (usually "now").
    pub end_ms: i64,
}

impl Config {
    pub fn start_ms(&self) -> i64 {
        self.end_ms - (self.minutes * 60_000) as i64
    }

    pub fn steps(&self) -> usize {
        (self.minutes * 60 / self.step_secs) as usize
    }

    pub fn service_name(&self, idx: usize) -> String {
        let base = SERVICE_POOL[idx % SERVICE_POOL.len()];
        if idx < SERVICE_POOL.len() {
            base.to_string()
        } else {
            format!("{base}-{}", idx / SERVICE_POOL.len() + 2)
        }
    }

    pub fn incident(&self) -> Incident {
        let span = self.end_ms - self.start_ms();
        Incident {
            service: 2 % self.services, // "auth" in the default pool
            start_ms: self.start_ms() + span / 3,
            end_ms: self.start_ms() + span * 2 / 3,
        }
    }
}

pub struct Incident {
    pub service: usize,
    pub start_ms: i64,
    pub end_ms: i64,
}

impl Incident {
    pub fn active(&self, ts_ms: i64) -> bool {
        ts_ms >= self.start_ms && ts_ms < self.end_ms
    }
}

// ---------------------------------------------------------------------------
// Metric series catalog
// ---------------------------------------------------------------------------

/// Emitted values are quantized the way real collectors quantize them
/// (percentages to 0.1, byte gauges to pages, counters to integers) —
/// full-precision float noise is something no real exporter emits, and it
/// would misrepresent how the codec behaves on production data.
#[derive(Clone, Copy)]
pub enum Behavior {
    /// Bounded random walk (cpu, load, gc pause); emits 0.1 steps.
    Walk { base: f64, span: f64, max: f64 },
    /// Slowly growing level with jitter (memory); emits `quant` multiples.
    Grow {
        base: f64,
        slope_per_sec: f64,
        quant: f64,
    },
    /// Monotonic integer counter with jittered per-second rate.
    Counter { rate_per_sec: f64 },
    /// Diurnal-ish sinusoid with noise; emits `quant` multiples.
    Gauge { base: f64, amp: f64, quant: f64 },
}

/// How the incident bends this series while the window is active.
#[derive(Clone, Copy, PartialEq)]
pub enum Role {
    None,
    CpuSpike,
    ErrorCounter,
    LatencySum,
}

pub struct SeriesSpec {
    pub name: &'static str,
    /// Canonical sorted flat JSON labels.
    pub labels: String,
    pub behavior: Behavior,
    pub role: Role,
    pub in_incident_service: bool,
    pub seed: u64,
}

/// Flat JSON object with keys in sorted order (matching what the vtab
/// emits, so demo filters behave predictably).
fn labels_json(pairs: &mut Vec<(&str, String)>) -> String {
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let body: Vec<String> = pairs
        .iter()
        .map(|(k, v)| format!("\"{k}\":\"{v}\""))
        .collect();
    format!("{{{}}}", body.join(","))
}

/// Build the full series catalog for the fleet. Cardinality per pod is
/// 14 + 7 × paths (98 series with the default 12 paths).
pub fn build_catalog(cfg: &Config) -> Vec<SeriesSpec> {
    let incident = cfg.incident();
    let mut out = Vec::new();
    let mut salt: u64 = 0;
    for svc_idx in 0..cfg.services {
        let svc = cfg.service_name(svc_idx);
        let hot = svc_idx == incident.service;
        for pod in 0..cfg.pods {
            let mut rng = Rng::new(cfg.seed ^ ((svc_idx as u64) << 32) ^ pod as u64);
            let instance = format!("{svc}-{pod:03}");
            let zone = ZONES[rng.below(ZONES.len() as u64) as usize];
            let base = |extra: &mut Vec<(&str, String)>| {
                let mut pairs = vec![
                    ("env", "prod".to_string()),
                    ("instance", instance.clone()),
                    ("service", svc.clone()),
                    ("zone", zone.to_string()),
                ];
                pairs.append(extra);
                labels_json(&mut pairs)
            };
            let mut push = |name: &'static str,
                            extra: &mut Vec<(&str, String)>,
                            behavior: Behavior,
                            role: Role,
                            salt: &mut u64| {
                *salt += 1;
                out.push(SeriesSpec {
                    name,
                    labels: base(extra),
                    behavior,
                    role,
                    in_incident_service: hot,
                    seed: cfg.seed ^ 0xC0FF_EE00 ^ *salt,
                });
            };

            // System metrics: 10 series per pod.
            for (mode, b, s, m) in [
                ("user", rng.range(20.0, 50.0), 25.0, 100.0),
                ("system", rng.range(4.0, 12.0), 6.0, 100.0),
                ("iowait", rng.range(0.5, 3.0), 2.0, 100.0),
            ] {
                push(
                    "cpu_usage_percent",
                    &mut vec![("mode", mode.to_string())],
                    Behavior::Walk {
                        base: b,
                        span: s,
                        max: m,
                    },
                    if mode == "user" {
                        Role::CpuSpike
                    } else {
                        Role::None
                    },
                    &mut salt,
                );
            }
            push(
                "memory_used_bytes",
                &mut vec![],
                Behavior::Grow {
                    base: rng.range(2.0e9, 6.0e9),
                    slope_per_sec: rng.range(1.0e3, 4.0e4),
                    quant: 4096.0, // page-granular, like a real RSS gauge
                },
                Role::None,
                &mut salt,
            );
            push(
                "memory_cached_bytes",
                &mut vec![],
                Behavior::Gauge {
                    base: rng.range(0.8e9, 1.6e9),
                    amp: 2.0e8,
                    quant: 4096.0,
                },
                Role::None,
                &mut salt,
            );
            for (name, rate) in [
                ("disk_read_bytes_total", rng.range(0.5e6, 4.0e6)),
                ("disk_written_bytes_total", rng.range(1.0e6, 8.0e6)),
                ("net_rx_bytes_total", rng.range(2.0e6, 12.0e6)),
                ("net_tx_bytes_total", rng.range(1.0e6, 9.0e6)),
            ] {
                push(
                    name,
                    &mut vec![],
                    Behavior::Counter { rate_per_sec: rate },
                    Role::None,
                    &mut salt,
                );
            }
            push(
                "load_average_1m",
                &mut vec![],
                Behavior::Walk {
                    base: rng.range(0.5, 3.0),
                    span: 2.0,
                    max: 32.0,
                },
                Role::None,
                &mut salt,
            );
            push(
                "open_file_descriptors",
                &mut vec![],
                Behavior::Gauge {
                    base: rng.range(200.0, 500.0),
                    amp: 60.0,
                    quant: 1.0,
                },
                Role::None,
                &mut salt,
            );

            // HTTP metrics: 7 series per path per pod.
            for p in 0..cfg.paths {
                let path = PATH_POOL[p % PATH_POOL.len()];
                let req_rate = rng.range(0.5, 30.0);
                let latency_ms = rng.range(3.0, 120.0);
                for (status, mult, role) in [
                    ("200", 1.0, Role::None),
                    ("204", 0.05, Role::None),
                    ("301", 0.02, Role::None),
                    ("404", 0.06, Role::None),
                    ("500", 0.004, Role::ErrorCounter),
                ] {
                    push(
                        "http_requests_total",
                        &mut vec![("path", path.to_string()), ("status", status.to_string())],
                        Behavior::Counter {
                            rate_per_sec: req_rate * mult,
                        },
                        role,
                        &mut salt,
                    );
                }
                push(
                    "http_request_duration_ms_sum",
                    &mut vec![("path", path.to_string())],
                    Behavior::Counter {
                        rate_per_sec: req_rate * 1.13 * latency_ms,
                    },
                    Role::LatencySum,
                    &mut salt,
                );
                push(
                    "http_request_duration_ms_count",
                    &mut vec![("path", path.to_string())],
                    Behavior::Counter {
                        rate_per_sec: req_rate * 1.13,
                    },
                    Role::None,
                    &mut salt,
                );
            }

            // App odds and ends: 4 series per pod.
            push(
                "queue_depth",
                &mut vec![],
                Behavior::Gauge {
                    base: rng.range(2.0, 20.0),
                    amp: 10.0,
                    quant: 1.0,
                },
                Role::None,
                &mut salt,
            );
            push(
                "cache_hits_total",
                &mut vec![],
                Behavior::Counter {
                    rate_per_sec: rng.range(100.0, 900.0),
                },
                Role::None,
                &mut salt,
            );
            push(
                "cache_misses_total",
                &mut vec![],
                Behavior::Counter {
                    rate_per_sec: rng.range(5.0, 80.0),
                },
                Role::None,
                &mut salt,
            );
            push(
                "gc_pause_ms",
                &mut vec![],
                Behavior::Walk {
                    base: rng.range(1.0, 8.0),
                    span: 6.0,
                    max: 500.0,
                },
                Role::None,
                &mut salt,
            );
        }
    }
    out
}

/// Per-series generator state. Seeding walks it step by step through the
/// window; live mode warms it through the seeded steps once and then keeps
/// advancing it, so appended samples continue each walk and counter
/// instead of restarting them.
pub struct SeriesState {
    rng: Rng,
    level: f64,
    cum: f64,
}

impl SeriesState {
    pub fn new(spec: &SeriesSpec) -> Self {
        let mut rng = Rng::new(spec.seed);
        let level = match spec.behavior {
            Behavior::Walk { base, .. } => base + rng.range(-2.0, 2.0),
            _ => 0.0,
        };
        SeriesState {
            rng,
            level,
            cum: 0.0,
        }
    }

    /// Value for step `i` at wall time `ts` (unix millis).
    pub fn value(
        &mut self,
        spec: &SeriesSpec,
        cfg: &Config,
        incident: &Incident,
        i: usize,
        ts: i64,
    ) -> f64 {
        let hot = spec.in_incident_service && incident.active(ts);
        match spec.behavior {
            Behavior::Walk { span, max, .. } => {
                self.level += (self.rng.unit() - 0.5) * span * 0.3;
                self.level = self.level.clamp(0.0, max);
                let v = if hot && spec.role == Role::CpuSpike {
                    (self.level + 40.0).min(max * 0.98)
                } else {
                    self.level
                };
                (v * 10.0).round() / 10.0
            }
            Behavior::Grow {
                base,
                slope_per_sec,
                quant,
            } => {
                let v = base
                    + slope_per_sec * (i as f64 * cfg.step_secs as f64)
                    + self.rng.range(-0.002, 0.002) * base;
                (v / quant).round() * quant
            }
            Behavior::Counter { rate_per_sec } => {
                let boost = match spec.role {
                    Role::ErrorCounter if hot => 150.0,
                    Role::LatencySum if hot => 4.0,
                    _ => 1.0,
                };
                // Integer increments keep the cumulative value exactly
                // integral, like every real byte/request counter.
                self.cum +=
                    (rate_per_sec * boost * cfg.step_secs as f64 * (0.5 + self.rng.unit())).round();
                self.cum
            }
            Behavior::Gauge { base, amp, quant } => {
                let phase = i as f64 * cfg.step_secs as f64 / 3600.0 * std::f64::consts::TAU;
                let v = (base + amp * phase.sin() + self.rng.range(-0.05, 0.05) * base).max(0.0);
                (v / quant).round() * quant
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Traces (generated BEFORE logs so error logs can reference real traces)
// ---------------------------------------------------------------------------

/// Small sample of error traces kept aside so log metadata can carry real,
/// queryable trace ids.
pub struct TraceReservoir {
    /// (service index, lower-hex trace id, root start unix millis)
    pub entries: Vec<(usize, String, i64)>,
    cap: usize,
}

impl TraceReservoir {
    pub fn new(cap: usize) -> Self {
        TraceReservoir {
            entries: Vec::new(),
            cap,
        }
    }

    fn offer(&mut self, rng: &mut Rng, service: usize, trace_id: &[u8; 16], ts_ms: i64) {
        let hex: String = trace_id.iter().map(|b| format!("{b:02x}")).collect();
        if self.entries.len() < self.cap {
            self.entries.push((service, hex, ts_ms));
        } else {
            let slot = rng.below(self.cap as u64) as usize;
            self.entries[slot] = (service, hex, ts_ms);
        }
    }

    pub fn pick(&self, rng: &mut Rng, service: usize) -> Option<&(usize, String, i64)> {
        if self.entries.is_empty() {
            return None;
        }
        // A handful of probes is plenty for demo correlation.
        for _ in 0..8 {
            let e = &self.entries[rng.below(self.entries.len() as u64) as usize];
            if e.0 == service {
                return Some(e);
            }
        }
        None
    }
}

fn span_attrs(method: &str, status: u16) -> String {
    format!(r#"{{"http.method":"{method}","http.status":{status},"sampled":true}}"#)
}

/// Generate one whole trace (5–20 spans) starting at `trace_start` nanos.
pub fn generate_trace(
    rng: &mut Rng,
    cfg: &Config,
    incident: &Incident,
    trace_start_ns: i64,
    reservoir: &mut TraceReservoir,
    out: &mut Vec<SpanEntry>,
) {
    let root_service = rng.below(cfg.services as u64) as usize;
    let ts_ms = trace_start_ns / 1_000_000;
    let hot = root_service == incident.service && incident.active(ts_ms);
    let trace_id: [u8; 16] = rng.bytes();
    let is_error = if hot {
        rng.below(10) < 3
    } else {
        rng.below(20) == 0
    };
    if is_error {
        reservoir.offer(rng, root_service, &trace_id, ts_ms);
    }
    // 80% short chains (5..=11), 20% fan-outs (12..=20) → mean ≈ 10.
    let n_spans = if rng.below(10) < 8 {
        5 + rng.below(7)
    } else {
        12 + rng.below(9)
    } as usize;
    let dur_scale = if hot { 200_000_000.0 } else { 50_000_000.0 };
    let root_dur = rng.duration(dur_scale).max(1_000_000);
    let error_child = 1 + rng.below((n_spans - 1) as u64) as usize;

    let mut span_ids: Vec<[u8; 8]> = Vec::with_capacity(n_spans);
    for i in 0..n_spans {
        let span_id: [u8; 8] = rng.bytes();
        let root = i == 0;
        let this_error = is_error && (root || i == error_child);
        let service_idx = if root || rng.below(10) < 6 {
            root_service
        } else {
            rng.below(cfg.services as u64) as usize
        };
        let service = cfg.service_name(service_idx);
        let (kind_num, scale) = if root {
            (1u8, dur_scale) // server
        } else {
            let (k, s) = [(0u8, 1.0e6), (2, 1.0e7), (3, 2.0e6), (4, 2.0e6)][rng.below(4) as usize];
            (k, s)
        };
        let status_num = if this_error {
            2
        } else if rng.below(5) == 0 {
            0
        } else {
            1
        };
        let http_status: u16 = if this_error {
            if rng.below(2) == 0 {
                500
            } else {
                503
            }
        } else {
            200
        };
        let method = METHODS[rng.below(METHODS.len() as u64) as usize];
        let name = if root {
            SPAN_NAMES[rng.below(10) as usize] // endpoint-shaped names
        } else {
            SPAN_NAMES[rng.below(SPAN_NAMES.len() as u64) as usize]
        };
        let start_ts = if root {
            trace_start_ns
        } else {
            trace_start_ns + rng.below(root_dur as u64) as i64
        };
        out.push(SpanEntry {
            trace_id,
            span_id,
            parent_span_id: if root {
                [0u8; 8]
            } else {
                span_ids[rng.below(i as u64) as usize]
            },
            name,
            service: service.clone(),
            kind_num,
            status_num,
            start_ts,
            duration_ns: if root {
                root_dur
            } else {
                rng.duration(scale).max(1_000)
            },
            attributes: span_attrs(method, http_status),
            status_message: if this_error {
                format!("upstream {service} returned {http_status}")
            } else {
                String::new()
            },
            events: if this_error {
                format!(
                    r#"[{{"attributes":{{"escaped":false}},"name":"exception","timestamp":{}}}]"#,
                    start_ts + 1_000
                )
            } else {
                "[]".to_string()
            },
            resource: format!(
                r#"{{"deployment.environment":"production","service.name":"{service}"}}"#
            ),
            scope: r#"{"attributes":{},"name":"timeless-demogen","version":"0.1"}"#.to_string(),
        });
        span_ids.push(span_id);
    }
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

const LOG_STATUS_OK: [&str; 3] = ["200", "201", "204"];
const LOG_STATUS_BAD: [&str; 3] = ["404", "500", "503"];

/// Generate one log entry at `ts` millis.
pub fn generate_log(
    rng: &mut Rng,
    cfg: &Config,
    incident: &Incident,
    reservoir: &TraceReservoir,
    ts: i64,
) -> LogEntry {
    let hot_window = incident.active(ts);
    // During the incident, over-sample the hot service.
    let service_idx = if hot_window && rng.below(10) < 4 {
        incident.service
    } else {
        rng.below(cfg.services as u64) as usize
    };
    let service = cfg.service_name(service_idx);
    let hot = hot_window && service_idx == incident.service;
    let path = PATH_POOL[rng.below(cfg.paths.min(PATH_POOL.len()) as u64) as usize];

    // Level mix: 70/15/10/5 info/debug/warning/error; the incident flips
    // the hot service toward errors.
    let roll = rng.below(100);
    let (level, level_num) = if hot && roll < 45 {
        ("error", 3u8)
    } else if hot && roll < 70 {
        ("warning", 2)
    } else if roll < 70 {
        ("info", 1)
    } else if roll < 85 {
        ("debug", 0)
    } else if roll < 95 {
        ("warning", 2)
    } else {
        ("error", 3)
    };
    let status = match level {
        "error" => LOG_STATUS_BAD[rng.below(3) as usize],
        "warning" => {
            if rng.below(3) == 0 {
                LOG_STATUS_BAD[0]
            } else {
                LOG_STATUS_OK[rng.below(3) as usize]
            }
        }
        _ => LOG_STATUS_OK[rng.below(3) as usize],
    };

    let dur = 1 + rng.below(if hot { 8_000 } else { 2_000 });
    let id = rng.below(1_000_000);
    let message = match level {
        "info" => format!("GET {path} completed in {dur}ms status={status}"),
        "debug" => format!("cache lookup key=user:{id} shard={} hit=true", id % 16),
        "warning" => {
            if rng.below(3) == 0 {
                format!("upstream timeout after {dur}ms retrying request {id}")
            } else {
                format!("slow query took {dur}ms on shard {}", id % 16)
            }
        }
        _ => {
            if rng.below(2) == 0 {
                format!("request {id} failed: connect timeout to {service}-backend")
            } else {
                format!("request {id} failed: internal error (status {status})")
            }
        }
    };

    // Errors carry a real trace id from the reservoir about half the time,
    // so a screencast can pivot log → trace.
    let metadata = if level_num == 3 && rng.below(2) == 0 {
        if let Some((_, trace_hex, _)) = reservoir.pick(rng, service_idx) {
            format!(
                "{{\"path\":\"{path}\",\"service\":\"{service}\",\"status\":\"{status}\",\"trace_id\":\"{trace_hex}\"}}"
            )
        } else {
            format!("{{\"path\":\"{path}\",\"service\":\"{service}\",\"status\":\"{status}\"}}")
        }
    } else {
        format!("{{\"path\":\"{path}\",\"service\":\"{service}\",\"status\":\"{status}\"}}")
    };

    LogEntry {
        ts,
        level_num,
        message,
        metadata,
    }
}
