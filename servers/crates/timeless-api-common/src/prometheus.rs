//! Prometheus text exposition (version 0.0.4) for the data planes'
//! `/metrics` endpoints. Everything served is already collected for
//! `/health` — this module is a formatting layer, not instrumentation.
//!
//! Conventions: every series is prefixed `timeless_<signal>_`, counters
//! carry the `_total` suffix, and each plane emits one
//! `timeless_build_info` info-metric whose labels carry the identity
//! that `/health` reports under `build` (the version-drift tell).

/// The exposition content type Prometheus scrapers negotiate.
pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// A text-exposition document under construction. Metric families are
/// written in call order; each value emits its `# HELP` / `# TYPE`
/// header inline (one series per family here, so no grouping pass is
/// needed).
#[derive(Default)]
pub struct Exposition {
    buf: String,
}

impl Exposition {
    pub fn new() -> Self {
        Self::default()
    }

    fn header(&mut self, name: &str, help: &str, kind: &str) {
        self.buf.push_str("# HELP ");
        self.buf.push_str(name);
        self.buf.push(' ');
        self.buf.push_str(help);
        self.buf.push_str("\n# TYPE ");
        self.buf.push_str(name);
        self.buf.push(' ');
        self.buf.push_str(kind);
        self.buf.push('\n');
    }

    /// A monotonically non-decreasing series. Callers pass the name
    /// WITH its `_total` suffix so the reader greps for exactly what
    /// scrapes expose. Negative inputs clamp to zero: Prometheus
    /// counters must not go negative, and the stats these mirror are
    /// non-negative by construction.
    pub fn counter(&mut self, name: &str, help: &str, value: i64) {
        self.header(name, help, "counter");
        self.buf.push_str(name);
        self.buf.push(' ');
        self.buf.push_str(itoa(value.max(0)).as_str());
        self.buf.push('\n');
    }

    /// An instantaneous value.
    pub fn gauge(&mut self, name: &str, help: &str, value: i64) {
        self.header(name, help, "gauge");
        self.buf.push_str(name);
        self.buf.push(' ');
        self.buf.push_str(itoa(value).as_str());
        self.buf.push('\n');
    }

    /// The `name{labels...} 1` info-metric idiom (as `gauge`, matching
    /// the `*_build_info` convention across the Prometheus ecosystem).
    pub fn info(&mut self, name: &str, help: &str, labels: &[(&str, &str)]) {
        self.header(name, help, "gauge");
        self.buf.push_str(name);
        self.buf.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                self.buf.push(',');
            }
            self.buf.push_str(key);
            self.buf.push_str("=\"");
            for ch in value.chars() {
                match ch {
                    '\\' => self.buf.push_str("\\\\"),
                    '"' => self.buf.push_str("\\\""),
                    '\n' => self.buf.push_str("\\n"),
                    other => self.buf.push(other),
                }
            }
            self.buf.push('"');
        }
        self.buf.push_str("} 1\n");
    }

    /// A histogram of durations, in the exposition's base unit of seconds.
    /// `upper_bounds_ns` are the finite bucket bounds, ascending;
    /// `counts` holds one observation count per bound plus a final one for
    /// everything above the last bound (not cumulative: this writes the
    /// running totals `le` buckets carry). `name` is the family name
    /// without a suffix, conventionally ending in `_seconds`.
    pub fn duration_histogram(
        &mut self,
        name: &str,
        help: &str,
        upper_bounds_ns: &[u64],
        counts: &[u64],
        sum_ns: u64,
    ) {
        debug_assert_eq!(counts.len(), upper_bounds_ns.len() + 1);
        self.header(name, help, "histogram");
        let mut running = 0_u64;
        for (bound, count) in upper_bounds_ns.iter().zip(counts) {
            running = running.saturating_add(*count);
            self.buf.push_str(name);
            self.buf.push_str("_bucket{le=\"");
            self.buf.push_str(&seconds(*bound));
            self.buf.push_str("\"} ");
            self.buf.push_str(&running.to_string());
            self.buf.push('\n');
        }
        let total = counts
            .iter()
            .fold(0_u64, |total, count| total.saturating_add(*count));
        self.buf.push_str(name);
        self.buf.push_str("_bucket{le=\"+Inf\"} ");
        self.buf.push_str(&total.to_string());
        self.buf.push('\n');
        self.buf.push_str(name);
        self.buf.push_str("_sum ");
        self.buf.push_str(&seconds(sum_ns));
        self.buf.push('\n');
        self.buf.push_str(name);
        self.buf.push_str("_count ");
        self.buf.push_str(&total.to_string());
        self.buf.push('\n');
    }

    pub fn finish(self) -> String {
        self.buf
    }
}

/// Nanoseconds as decimal seconds, without float rounding or exponents.
fn seconds(ns: u64) -> String {
    let text = format!("{}.{:09}", ns / 1_000_000_000, ns % 1_000_000_000);
    let trimmed = text.trim_end_matches('0');
    trimmed.strip_suffix('.').unwrap_or(trimmed).to_string()
}

fn itoa(value: i64) -> String {
    value.to_string()
}

/// The plane's `timeless_build_info` metric, carrying the same identity
/// `/health` reports under `build`.
pub fn build_info(exposition: &mut Exposition, signal: &str) {
    let identity = crate::server_build_identity(signal);
    let field = |key: &str| {
        identity
            .get(key)
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_owned()
    };
    let (name, version, commit, target, profile) = (
        field("name"),
        field("version"),
        field("commit"),
        field("target"),
        field("profile"),
    );
    exposition.info(
        "timeless_build_info",
        "Build identity of this data plane (constant 1).",
        &[
            ("name", &name),
            ("version", &version),
            ("commit", &commit),
            ("target", &target),
            ("profile", &profile),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposition_formats_families() {
        let mut exposition = Exposition::new();
        exposition.counter("t_things_total", "Things seen.", 42);
        exposition.gauge("t_depth", "Queue depth.", -3);
        exposition.info("t_info", "Identity.", &[("v", "1.2\"x\\y\n")]);
        let text = exposition.finish();
        assert!(text.contains("# HELP t_things_total Things seen.\n# TYPE t_things_total counter\nt_things_total 42\n"));
        assert!(text.contains("# TYPE t_depth gauge\nt_depth -3\n"));
        assert!(text.contains("t_info{v=\"1.2\\\"x\\\\y\\n\"} 1\n"));
    }

    #[test]
    fn a_duration_histogram_is_cumulative_and_in_seconds() {
        let mut exposition = Exposition::new();
        exposition.duration_histogram(
            "t_step_seconds",
            "Step time.",
            &[10_000_000, 1_000_000_000],
            &[3, 2, 1],
            4_250_000_000,
        );
        assert_eq!(
            exposition.finish(),
            "# HELP t_step_seconds Step time.\n# TYPE t_step_seconds histogram\n\
             t_step_seconds_bucket{le=\"0.01\"} 3\n\
             t_step_seconds_bucket{le=\"1\"} 5\n\
             t_step_seconds_bucket{le=\"+Inf\"} 6\n\
             t_step_seconds_sum 4.25\n\
             t_step_seconds_count 6\n"
        );
    }

    #[test]
    fn counters_never_go_negative() {
        let mut exposition = Exposition::new();
        exposition.counter("t_total", "Clamped.", -7);
        assert!(exposition.finish().contains("t_total 0\n"));
    }

    #[test]
    fn build_info_carries_identity() {
        let mut exposition = Exposition::new();
        build_info(&mut exposition, "logs");
        let text = exposition.finish();
        assert!(text.contains("timeless_build_info{name=\"timeless-logs-api\",version=\""));
        assert!(text.contains("} 1\n"));
    }
}
