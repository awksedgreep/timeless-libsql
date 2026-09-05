//! Which signal tables a demo database has, and what they are called.
//!
//! Both front ends — the loadable extension and the CLI — let a caller name
//! the tables to fill instead of assuming `metrics`/`logs`/`spans`. The
//! decision logic is identical, so it lives here; only the SQL that reads
//! `sqlite_schema` differs, because the two crates link rusqlite with
//! mutually exclusive features.
//!
//! This module stays dependency-free like the rest of `core`: it is string
//! handling over SQL text a caller has already fetched.

/// The three signals, identified by the vtab module that implements them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Signal {
    Metrics,
    Logs,
    Traces,
}

impl Signal {
    /// The signal a `CREATE VIRTUAL TABLE` statement declares, if any.
    pub fn from_module(module: &str) -> Option<Self> {
        match module {
            "timeless_metrics" => Some(Self::Metrics),
            "timeless_logs" => Some(Self::Logs),
            "timeless_traces" => Some(Self::Traces),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Logs => "logs",
            Self::Traces => "traces",
        }
    }
}

/// The module name in `CREATE VIRTUAL TABLE x USING <module>(...)`.
///
/// Parsed rather than matched with LIKE so `timeless_metrics` cannot be
/// confused with a longer module that merely starts the same way, and so an
/// ordinary table is reported as "not a virtual table" rather than silently
/// skipped.
pub fn module_of(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let start = upper.find(" USING ")? + " USING ".len();
    let rest = &sql[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(rest.len());
    Some(rest[..end].to_ascii_lowercase())
}

/// Resolved target tables: at most one per signal, `None` meaning this
/// database has no table for that signal and none will be generated.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tables {
    pub metrics: Option<String>,
    pub logs: Option<String>,
    pub spans: Option<String>,
}

impl Tables {
    pub fn any(&self) -> bool {
        self.metrics.is_some() || self.logs.is_some() || self.spans.is_some()
    }

    /// The demo defaults, used only when a database declares no timeless
    /// vtables of its own and none were named.
    pub fn defaults() -> Self {
        Self {
            metrics: Some("metrics".into()),
            logs: Some("logs".into()),
            spans: Some("spans".into()),
        }
    }

    /// Every resolved table name, in signal order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        [&self.metrics, &self.logs, &self.spans]
            .into_iter()
            .flatten()
            .map(String::as_str)
    }

    pub fn slot(&mut self, signal: Signal) -> &mut Option<String> {
        match signal {
            Signal::Metrics => &mut self.metrics,
            Signal::Logs => &mut self.logs,
            Signal::Traces => &mut self.spans,
        }
    }

    /// `metrics (my_metrics), traces` — what is about to be filled, with the
    /// name shown only when it is not the one a reader would assume.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        for (signal, name) in [
            (Signal::Metrics, &self.metrics),
            (Signal::Logs, &self.logs),
            (Signal::Traces, &self.spans),
        ] {
            let Some(name) = name else { continue };
            let assumed = match signal {
                Signal::Traces => "spans",
                other => other.label(),
            };
            if name == assumed {
                parts.push(signal.label().to_string());
            } else {
                parts.push(format!("{} ({name})", signal.label()));
            }
        }
        parts.join(", ")
    }

    pub fn serialize(&self) -> String {
        let slot = |s: &Option<String>| s.clone().unwrap_or_else(|| "-".into());
        format!(
            "{} {} {}",
            slot(&self.metrics),
            slot(&self.logs),
            slot(&self.spans)
        )
    }

    pub fn parse(row: &str) -> Self {
        let mut it = row.split_whitespace();
        let mut slot = || match it.next() {
            None | Some("-") => None,
            Some(name) => Some(name.to_string()),
        };
        Self {
            metrics: slot(),
            logs: slot(),
            spans: slot(),
        }
    }

    /// Claim `name` for the signal its `sql` declares.
    ///
    /// The caller supplies the schema text; this enforces the rules that are
    /// the same for both front ends — must be a virtual table, must be a
    /// timeless module, at most one table per signal.
    pub fn claim(&mut self, name: &str, sql: &str) -> Result<(), String> {
        let module = module_of(sql).ok_or_else(|| format!("'{name}' is not a virtual table"))?;
        let signal = Signal::from_module(&module)
            .ok_or_else(|| format!("'{name}' is a {module} table, not a timeless signal table"))?;
        if let Some(taken) = self.slot(signal).replace(name.to_string()) {
            return Err(format!(
                "'{taken}' and '{name}' are both {} — name at most one table per signal",
                signal.label()
            ));
        }
        Ok(())
    }
}

/// The message used when a target already holds data.
///
/// These tables are append-only, so synthetic telemetry mixed into real data
/// cannot be taken back out; both front ends refuse rather than warn.
pub fn populated_error(name: &str) -> String {
    format!(
        "'{name}' already contains data — demogen will not mix synthetic telemetry \
         into an existing table, and these tables are append-only. Seed into an \
         empty table or a scratch database file."
    )
}

/// The message used when a named target does not exist.
pub fn missing_error(name: &str) -> String {
    format!(
        "no table named '{name}' in this database — create it first, \
         e.g. CREATE VIRTUAL TABLE {name} USING timeless_metrics;"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_is_parsed_not_prefix_matched() {
        assert_eq!(
            module_of("CREATE VIRTUAL TABLE a USING timeless_metrics").as_deref(),
            Some("timeless_metrics")
        );
        assert_eq!(
            module_of("CREATE VIRTUAL TABLE a USING timeless_traces(retention='1d')").as_deref(),
            Some("timeless_traces")
        );
        // A longer module is not mistaken for the one it starts with.
        assert_eq!(
            Signal::from_module(
                &module_of("CREATE VIRTUAL TABLE a USING timeless_metrics_v2").unwrap()
            ),
            None
        );
        assert_eq!(module_of("CREATE TABLE plain(x)"), None);
    }

    #[test]
    fn one_table_per_signal() {
        let mut t = Tables::default();
        t.claim("a", "CREATE VIRTUAL TABLE a USING timeless_metrics")
            .unwrap();
        let err = t
            .claim("b", "CREATE VIRTUAL TABLE b USING timeless_metrics")
            .unwrap_err();
        assert!(err.contains("at most one table per signal"), "{err}");
    }

    #[test]
    fn ordinary_tables_and_foreign_modules_are_rejected() {
        let mut t = Tables::default();
        assert!(t
            .claim("p", "CREATE TABLE p(x)")
            .unwrap_err()
            .contains("not a virtual table"));
        assert!(t
            .claim("f", "CREATE VIRTUAL TABLE f USING fts5(body)")
            .unwrap_err()
            .contains("not a timeless signal table"));
    }

    #[test]
    fn round_trips_through_state_including_absent_signals() {
        let mut t = Tables::default();
        t.claim(
            "app_logs",
            "CREATE VIRTUAL TABLE app_logs USING timeless_logs",
        )
        .unwrap();
        assert_eq!(t.serialize(), "- app_logs -");
        assert_eq!(Tables::parse(&t.serialize()), t);
        assert_eq!(t.describe(), "logs (app_logs)");
        assert_eq!(Tables::defaults().describe(), "metrics, logs, traces");
    }
}
