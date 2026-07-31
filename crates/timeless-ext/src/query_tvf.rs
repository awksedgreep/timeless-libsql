//! Q2 reduction-kernel table-valued functions (PLAN.md "Query interface
//! tiers & the PromQL layering contract").
//!
//! Two eponymous virtual tables expose the engine's semantics-free
//! reduction kernels to SQL — the opt-in accelerators behind the Q1
//! waist. They are purely ADDITIVE: the raw-scan contract of the
//! `timeless_metrics` vtab is untouched, and a caller that does not
//! probe for these modules loses nothing but speed.
//!
//!   -- Q2(a): last sample per grid point, lookback (t-lookback, t]
//!   SELECT labels, ts, value FROM timeless_grid(
//!     'metrics',          -- vtab name, or 'schema.table'
//!     'cpu_usage',        -- metric
//!     '{"host":"pvm1"}',  -- label filter (NULL/'{}' = all); see below
//!     :start, :stop, :step, :lookback);
//!
//! F8 — the filter argument accepts matcher objects per key alongside
//! plain equality strings:
//!
//!   '{"host": {"re": "web-.*"}, "env": {"neq": "dev"}}'
//!   -- plain "v" = equality | {"neq": v} | {"re": pat} | {"nre": pat}
//!
//! Regexes use the Rust `regex` crate (RE2 family: no backrefs, no
//! lookaround) and are FULLY ANCHORED — the pattern must match the
//! whole label value, PromQL-style. A label absent from a series
//! matches as "" (the pinned waist rule): neq-of-anything-nonempty
//! matches, re must accept "" to match. Matchers filter the candidate
//! series list before any chunk reads; equality keys still push down
//! into the registry index.
//!
//! Label discovery for UI builders:
//!
//!   SELECT value FROM timeless_label_values('metrics', 'cpu_usage', 'host');
//!
//! F9 — both kernel TVFs take an optional trailing fill argument:
//! 'none' (default, sparse) or 'null' (dense: every grid point emitted
//! per matched series, value NULL where the window/lookback is empty).
//! Presentation mechanics only — a series with NO points on the grid
//! stays entirely absent either way (query_multi's omission rule).
//!
//!   -- Q2(b): sliding-window aggregate, window (t-window, t]
//!   SELECT labels, ts, value FROM timeless_window(
//!     'metrics', 'cpu_usage', NULL,
//!     :start, :stop, :step, :window, 'avg');   -- sum|min|max|count|avg
//!
//! Rows are (labels TEXT — canonical JSON, ts INTEGER — grid point,
//! value REAL), ordered by series then grid ts. Grid points with no
//! sample in range produce no row; staleness/NaN policy stays above the
//! waist. Timestamp unit is whatever the underlying table stores
//! (epoch seconds for timeless_metrics) — the kernels are unit-agnostic.
//!
//! The engine is resolved through the same process registry as the
//! metrics vtab (metrics_vtab::shared_engine_for), so TVF queries see
//! exactly what vtab queries see — including buffered points — and a
//! fresh connection recovers the engine the same way xConnect would.

use std::borrow::Cow;
use std::ffi::{c_int, CStr};
use std::marker::PhantomData;
use std::sync::Arc;

use rusqlite::ffi;
use rusqlite::vtab::{
    Context, Filters, IndexConstraintOp, IndexInfo, Module, VTab, VTabConfig, VTabConnection,
    VTabCursor,
};
use rusqlite::{Connection, Error, Result};
use timeless_core::{AggFn, Engine, Labels};

use crate::flatjson::{labels_to_json, parse_labels_json, parse_matchers_json, MatcherSpec};
use crate::logs_vtab::LogsTab;
use crate::metrics_vtab::MetricsTab;
use crate::traces_vtab::TracesTab;
use crate::shared::{self, DbGuard, SharedEngine};

fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

/// F8: one compiled non-equality matcher. Equality keys never appear
/// here — they push down into the registry's label index instead.
#[derive(Debug)]
enum LabelMatcher {
    Neq(String),
    Re(regex::Regex),
    Nre(regex::Regex),
}

impl LabelMatcher {
    /// Absent label = "" (the pinned waist rule, extended to re/nre).
    fn matches(&self, labels: &Labels, key: &str) -> bool {
        let val = labels.get(key).map(String::as_str).unwrap_or("");
        match self {
            LabelMatcher::Neq(v) => val != v,
            LabelMatcher::Re(re) => re.is_match(val),
            LabelMatcher::Nre(re) => !re.is_match(val),
        }
    }
}

fn matchers_pass(labels: &Labels, matchers: &[(String, LabelMatcher)]) -> bool {
    matchers.iter().all(|(k, m)| m.matches(labels, k))
}

/// Fully anchored, PromQL-style: the pattern must match the WHOLE label
/// value. `web-.*` means "starts with web-", `.*web.*` means contains.
fn compile_anchored(module: &str, key: &str, pat: &str) -> Result<regex::Regex> {
    regex::Regex::new(&format!("^(?:{pat})$")).map_err(|e| {
        module_err(format!(
            "{module}: filter: invalid regex {pat:?} for label {key:?}: {e}"
        ))
    })
}

/// Split a filter's matchers into the equality set (pushed into the
/// registry index) and compiled non-equality matchers (applied to the
/// candidate list). Duplicate keys keep JSON-object semantics for
/// equality (last wins, via Labels insert) while non-eq matchers all
/// apply (AND).
fn compile_filter(module: &str, txt: &str) -> Result<(Labels, Vec<(String, LabelMatcher)>)> {
    let mut eq = Labels::new();
    let mut rest: Vec<(String, LabelMatcher)> = Vec::new();
    for (key, spec) in
        parse_matchers_json(txt).map_err(|e| module_err(format!("{module}: filter: {e}")))?
    {
        match spec {
            MatcherSpec::Eq(v) => {
                eq.insert(key, v);
            }
            MatcherSpec::Neq(v) => rest.push((key, LabelMatcher::Neq(v))),
            MatcherSpec::Re(p) => {
                let re = compile_anchored(module, &key, &p)?;
                rest.push((key, LabelMatcher::Re(re)));
            }
            MatcherSpec::Nre(p) => {
                let re = compile_anchored(module, &key, &p)?;
                rest.push((key, LabelMatcher::Nre(re)));
            }
        }
    }
    Ok((eq, rest))
}

/// F7: the full timeless_window vocabulary. Classic folds, counter
/// kernels (delta/increase/rate — raw window folds, NOT PromQL: no
/// extrapolation, no staleness), exact nearest-rank percentiles (pNN),
/// and explicit-parameter trimmed means (tavg:N). Definitions pinned
/// in FEATURE_PLAN.md F7.
fn parse_window_op(name: Option<&str>) -> Result<timeless_core::WindowOp> {
    use timeless_core::WindowOp;
    let name = name.unwrap_or("<missing>");
    Ok(match name {
        "sum" => WindowOp::Agg(AggFn::Sum),
        "min" => WindowOp::Agg(AggFn::Min),
        "max" => WindowOp::Agg(AggFn::Max),
        "count" => WindowOp::Agg(AggFn::Count),
        "avg" => WindowOp::Agg(AggFn::Avg),
        "delta" => WindowOp::Delta,
        "increase" => WindowOp::Increase,
        "rate" => WindowOp::Rate,
        _ => {
            if let Some(q) = name.strip_prefix('p') {
                let q: f64 = q.parse().map_err(|_| {
                    module_err(format!("timeless_window: bad percentile {name:?}"))
                })?;
                if !(q > 0.0 && q <= 100.0) {
                    return Err(module_err(format!(
                        "timeless_window: percentile must be in (0, 100], got {name:?}"
                    )));
                }
                return Ok(WindowOp::Percentile(q));
            }
            if let Some(q) = name.strip_prefix("tavg:") {
                let q: f64 = q.parse().map_err(|_| {
                    module_err(format!("timeless_window: bad trim fraction {name:?}"))
                })?;
                if !(0.0..50.0).contains(&q) {
                    return Err(module_err(format!(
                        "timeless_window: trim fraction must be in [0, 50), got {name:?}"
                    )));
                }
                return Ok(WindowOp::TrimmedMean(q));
            }
            return Err(module_err(format!(
                "timeless_window: unknown agg {name:?}; expected one of: sum, min, max, \
                 count, avg, delta, increase, rate, pNN (e.g. p95), tavg:N"
            )));
        }
    })
}

/// Register the TVF modules on a freshly-loaded connection.
pub(crate) fn register(db: &Connection) -> Result<()> {
    const GRID: Module<GridTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_grid", &GRID, None::<()>)?;
    const WINDOW: Module<WindowTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_window", &WINDOW, None::<()>)?;
    const SERIES: Module<SeriesTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_series", &SERIES, None::<()>)?;
    const STATS: Module<StatsTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_stats", &STATS, None::<()>)?;
    const ROLLUP: Module<RollupTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_rollup", &ROLLUP, None::<()>)?;
    const LOG_BUCKETS: Module<LogBucketsTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_log_buckets", &LOG_BUCKETS, None::<()>)?;
    const TRACE_BUCKETS: Module<TraceBucketsTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_trace_buckets", &TRACE_BUCKETS, None::<()>)?;
    const LABEL_VALUES: Module<LabelValuesTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_label_values", &LABEL_VALUES, None::<()>)
}

// Output columns (both modules).
const COL_LABELS: c_int = 0;
// Hidden argument columns start here; canonical order = function-call
// argument order.
const COL_FIRST_ARG: c_int = 3;

#[cfg(test)]
mod matcher_semantics_tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs.iter().map(|&(k, v)| (k.into(), v.into())).collect()
    }

    #[test]
    fn regex_is_fully_anchored() {
        let (eq, m) = compile_filter("t", r#"{"host": {"re": "web-.*"}}"#).unwrap();
        assert!(eq.is_empty());
        assert!(matchers_pass(&labels(&[("host", "web-1")]), &m));
        // Substring match must NOT pass: anchoring is the contract.
        assert!(!matchers_pass(&labels(&[("host", "xweb-1")]), &m));
        let (_, m) = compile_filter("t", r#"{"host": {"re": "eb-"}}"#).unwrap();
        assert!(!matchers_pass(&labels(&[("host", "web-1")]), &m));
    }

    #[test]
    fn absent_label_is_empty_string() {
        // neq of a non-empty value matches a series missing the label.
        let (_, m) = compile_filter("t", r#"{"env": {"neq": "prod"}}"#).unwrap();
        assert!(matchers_pass(&labels(&[("host", "a")]), &m));
        assert!(!matchers_pass(&labels(&[("env", "prod")]), &m));
        // re must accept "" to match an absent label.
        let (_, m) = compile_filter("t", r#"{"env": {"re": "prod|"}}"#).unwrap();
        assert!(matchers_pass(&labels(&[]), &m));
        let (_, m) = compile_filter("t", r#"{"env": {"re": "prod"}}"#).unwrap();
        assert!(!matchers_pass(&labels(&[]), &m));
        // nre of ".+" = "label absent or empty".
        let (_, m) = compile_filter("t", r#"{"env": {"nre": ".+"}}"#).unwrap();
        assert!(matchers_pass(&labels(&[]), &m));
        assert!(!matchers_pass(&labels(&[("env", "dev")]), &m));
    }

    #[test]
    fn eq_splits_from_matchers_and_ands() {
        let (eq, m) = compile_filter(
            "t",
            r#"{"host": "web-1", "env": {"neq": "dev"}, "dc": {"re": "us-.*"}}"#,
        )
        .unwrap();
        assert_eq!(eq.len(), 1);
        assert_eq!(eq.get("host").map(String::as_str), Some("web-1"));
        assert_eq!(m.len(), 2);
        assert!(matchers_pass(&labels(&[("env", "prod"), ("dc", "us-east")]), &m));
        assert!(!matchers_pass(&labels(&[("env", "dev"), ("dc", "us-east")]), &m));
        assert!(!matchers_pass(&labels(&[("env", "prod"), ("dc", "eu-1")]), &m));
    }

    #[test]
    fn invalid_regex_names_pattern_and_label() {
        let err = compile_filter("t", r#"{"host": {"re": "["}}"#).unwrap_err().to_string();
        assert!(err.contains("invalid regex") && err.contains("host"), "{err}");
    }
}

/// Everything one TVF scan needs, decoded from the pushed constraints.
pub(crate) struct KernelArgs {
    database: String,
    table: String,
    metric: String,
    filter: Labels,
    /// F8: non-equality matchers, applied to the candidate series list.
    matchers: Vec<(String, LabelMatcher)>,
    start: i64,
    stop: i64,
    step: i64,
    /// lookback (grid) or window (window) or resolution (rollup).
    width: i64,
    /// Raw agg argument; each module parses its own vocabulary.
    agg_name: Option<String>,
    /// F9: fill='null' — emit EVERY grid point per matched series, value
    /// NULL where the window is empty. Presentation only, zero
    /// semantics: the kernels still decide which points have values.
    fill: bool,
}

/// Decode the hidden-column EQ args per the best_index bitmask.
/// `names` lists the module's arg columns in canonical order; `required`
/// is a parallel mask of which must be present.
fn decode_args(
    module: &str,
    names: &[&str],
    required_mask: c_int,
    idx_num: c_int,
    args: &Filters<'_>,
) -> Result<KernelArgs> {
    let missing: Vec<&str> = names
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            let bit = 1 << *i;
            (required_mask & bit) != 0 && (idx_num & bit) == 0
        })
        .map(|(_, n)| *n)
        .collect();
    if !missing.is_empty() {
        return Err(module_err(format!(
            "{module}: missing required argument(s): {} — call as {module}({})",
            missing.join(", "),
            names.join(", ")
        )));
    }

    // argv slots were assigned in canonical (declaration) order over the
    // provided args; map them back BY NAME so modules may declare any
    // argument layout (grid/window/rollup differ).
    let mut slot_of: Vec<Option<usize>> = vec![None; names.len()];
    let mut slot = 0usize;
    for (i, s_of) in slot_of.iter_mut().enumerate() {
        if idx_num & (1 << i) != 0 {
            *s_of = Some(slot);
            slot += 1;
        }
    }
    let find = |name: &str| -> Option<usize> {
        names.iter().position(|n| *n == name).and_then(|i| slot_of[i])
    };

    let get_text = |s: usize, what: &str| -> Result<String> {
        let v: Option<String> = args.get(s)?;
        v.ok_or_else(|| module_err(format!("{module}: {what} must not be NULL")))
    };
    let get_int = |s: usize, what: &str| -> Result<i64> {
        let v: Option<i64> = args.get(s)?;
        v.ok_or_else(|| module_err(format!("{module}: {what} must not be NULL")))
    };

    let tbl_slot = find("tbl").expect("tbl is required by every module");
    let metric_slot = find("metric").expect("metric is required by every module");
    let filter_slot = find("filter");
    let start_slot = find("start").expect("start is required by every module");
    let stop_slot = find("stop").expect("stop is required by every module");
    let step_slot = find("step");
    let width_slot = find("lookback")
        .or_else(|| find("window"))
        .or_else(|| find("resolution"))
        .expect("every module declares a width-family argument");
    let agg_slot = find("agg");

    let spec = get_text(tbl_slot, "tbl")?;
    // 'schema.table' selects an attached schema; plain 'table' = main.
    // (A MAIN-schema table name containing a literal dot needs the vtab
    // spelled 'main.<name>'.)
    let (database, table) = match spec.split_once('.') {
        Some((schema, tbl)) => (schema.to_owned(), tbl.to_owned()),
        None => ("main".to_owned(), spec),
    };

    let (filter, matchers) = match filter_slot {
        None => (Labels::new(), Vec::new()),
        Some(s) => match args.get::<Option<String>>(s)? {
            None => (Labels::new(), Vec::new()), // NULL filter = no filter
            Some(txt) if txt.is_empty() => (Labels::new(), Vec::new()),
            Some(txt) => compile_filter(module, &txt)?,
        },
    };

    let agg_name = match agg_slot {
        None => None,
        Some(s) => Some(get_text(s, "agg")?),
    };

    // fill is optional everywhere it exists; NULL = default = 'none'.
    let fill = match find("fill") {
        None => false,
        Some(s) => match args.get::<Option<String>>(s)?.as_deref() {
            None | Some("none") => false,
            Some("null") => true,
            Some(other) => {
                return Err(module_err(format!(
                    "{module}: fill must be 'none' or 'null', got {other:?}"
                )))
            }
        },
    };

    Ok(KernelArgs {
        database,
        table,
        metric: get_text(metric_slot, "metric")?,
        filter,
        matchers,
        start: get_int(start_slot, "start")?,
        stop: get_int(stop_slot, "stop")?,
        step: match step_slot {
            Some(slot) => get_int(slot, "step")?,
            None => 0, // module has no step argument (rollup)
        },
        width: get_int(width_slot, "width")?,
        agg_name,
        fill,
    })
}

/// Shared best_index for both modules: collect EQ constraints on the
/// hidden arg columns into a bitmask, assign argv slots in canonical
/// order, defer required-arg checking to filter (clearer errors than a
/// bare "no query solution").
fn best_index_args(info: &mut IndexInfo, first_arg: c_int, n_args: c_int) -> Result<bool> {
    let mut idx_num: c_int = 0;
    let mut unusable: c_int = 0;
    let mut slots: Vec<Option<usize>> = vec![None; n_args as usize];
    for (i, constraint) in info.constraints().enumerate() {
        let col = constraint.column();
        if col < first_arg || col >= first_arg + n_args {
            continue;
        }
        let bit = 1 << (col - first_arg);
        if !constraint.is_usable() {
            unusable |= bit;
        } else if constraint.operator() == IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ
            && slots[(col - first_arg) as usize].is_none()
        {
            idx_num |= bit;
            slots[(col - first_arg) as usize] = Some(i);
        }
    }
    // An arg constrained only unusably: reject this plan so the planner
    // finds one where the constraint is usable (the series.c pattern).
    if unusable & !idx_num != 0 {
        return Ok(false);
    }
    let mut n_arg = 0;
    for slot in slots.iter().flatten() {
        n_arg += 1;
        let mut usage = info.constraint_usage(*slot);
        usage.set_argv_index(n_arg);
        usage.set_omit(true);
    }
    info.set_estimated_cost(1000.0);
    info.set_estimated_rows(1000);
    info.set_idx_num(idx_num);
    Ok(true)
}

/// Resolve the engine and run one kernel scan into materialized rows.
fn run_kernel(
    db: *mut ffi::sqlite3,
    ka: &KernelArgs,
    kernel: impl Fn(&Engine, i64) -> Result<Vec<(i64, f64)>>,
) -> Result<Vec<(String, i64, Option<f64>)>> {
    let _bind = DbGuard::bind(db);
    let shared: Arc<SharedEngine<Engine>> =
        MetricsTab::shared_engine_for(db, &ka.database, &ka.table)?;
    shared.engine.refresh_authoritative_state().map_err(module_err)?;

    // Candidate snapshot, then sequential per-series kernels — the
    // rayon-free discipline every vtab callback must follow (see
    // collect_metric in metrics_vtab.rs).
    let candidates: Vec<(i64, Labels)> = {
        let reg = shared.engine.series_read();
        reg.find_series(&ka.metric, &ka.filter)
            .into_iter()
            .filter_map(|sid| reg.info_for(sid).map(|info| (sid, info.labels.clone())))
            // F8: non-eq matchers cut the candidate list here, BEFORE
            // any chunk reads — the regex cost is per-series, not
            // per-point.
            .filter(|(_, labels)| matchers_pass(labels, &ka.matchers))
            .collect()
    };

    let mut rows = Vec::new();
    for (sid, labels) in candidates {
        let points = kernel(&shared.engine, sid)?;
        // Per-series absence rule (matches query_multi's omission): a
        // series with NO points on the grid emits nothing, fill or not.
        if points.is_empty() {
            continue;
        }
        let labels_json = labels_to_json(&labels);
        if ka.fill {
            // Dense emission: every grid point, NULL where the kernel
            // had no row. The kernel already validated step > 0 and the
            // grid-length cap, so this walk is bounded.
            let mut it = points.iter().peekable();
            let mut t = ka.start;
            while t <= ka.stop {
                let v = match it.peek() {
                    Some(&&(pts, pv)) if pts == t => {
                        it.next();
                        Some(pv)
                    }
                    _ => None,
                };
                rows.push((labels_json.clone(), t, v));
                match t.checked_add(ka.step) {
                    Some(next) => t = next,
                    None => break,
                }
            }
        } else {
            for (ts, value) in points {
                rows.push((labels_json.clone(), ts, Some(value)));
            }
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// timeless_grid
// ---------------------------------------------------------------------------

const GRID_ARGS: &[&str] =
    &["tbl", "metric", "filter", "start", "stop", "step", "lookback", "fill"];
// All required except filter (bit 2) and fill (bit 7).
const GRID_REQUIRED: c_int = 0b0111_1011;

#[repr(C)]
pub(crate) struct GridTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for GridTab {
    type Aux = ();
    type Cursor = KernelCursor<'vtab, GridTab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let handle = unsafe { db.handle() };
        db.config(VTabConfig::Innocuous)?;
        Ok((
            Cow::Borrowed(c"CREATE TABLE x(labels TEXT, ts INTEGER, value REAL, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, start HIDDEN, \
                            stop HIDDEN, step HIDDEN, lookback HIDDEN, fill HIDDEN)"),
            GridTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, COL_FIRST_ARG, GRID_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<KernelCursor<'vtab, GridTab>> {
        Ok(KernelCursor::new(self.db))
    }
}

impl KernelVTab for GridTab {
    const MODULE: &'static str = "timeless_grid";
    const ARGS: &'static [&'static str] = GRID_ARGS;
    const REQUIRED: c_int = GRID_REQUIRED;

    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<(String, i64, Option<f64>)>> {
        run_kernel(db, ka, |engine, sid| {
            engine
                .query_grid_last_by_id(sid, ka.start, ka.stop, ka.step, ka.width)
                .map_err(module_err)
        })
    }
}

// ---------------------------------------------------------------------------
// timeless_window
// ---------------------------------------------------------------------------

const WINDOW_ARGS: &[&str] = &[
    "tbl", "metric", "filter", "start", "stop", "step", "window", "agg", "fill",
];
// All required except filter (bit 2) and fill (bit 8).
const WINDOW_REQUIRED: c_int = 0b0_1111_1011;

#[repr(C)]
pub(crate) struct WindowTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for WindowTab {
    type Aux = ();
    type Cursor = KernelCursor<'vtab, WindowTab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let handle = unsafe { db.handle() };
        db.config(VTabConfig::Innocuous)?;
        Ok((
            Cow::Borrowed(c"CREATE TABLE x(labels TEXT, ts INTEGER, value REAL, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, start HIDDEN, \
                            stop HIDDEN, step HIDDEN, window HIDDEN, agg HIDDEN, \
                            fill HIDDEN)"),
            WindowTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, COL_FIRST_ARG, WINDOW_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<KernelCursor<'vtab, WindowTab>> {
        Ok(KernelCursor::new(self.db))
    }
}

impl KernelVTab for WindowTab {
    const MODULE: &'static str = "timeless_window";
    const ARGS: &'static [&'static str] = WINDOW_ARGS;
    const REQUIRED: c_int = WINDOW_REQUIRED;

    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<(String, i64, Option<f64>)>> {
        let op = parse_window_op(ka.agg_name.as_deref())?;
        run_kernel(db, ka, |engine, sid| {
            engine
                .query_window_op_by_id(sid, ka.start, ka.stop, ka.step, ka.width, op)
                .map_err(module_err)
        })
    }
}

// ---------------------------------------------------------------------------
// Shared cursor
// ---------------------------------------------------------------------------

/// Per-module constants + kernel dispatch, so one cursor serves both.
pub(crate) trait KernelVTab {
    const MODULE: &'static str;
    const ARGS: &'static [&'static str];
    const REQUIRED: c_int;
    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<(String, i64, Option<f64>)>>;
}

#[repr(C)]
pub(crate) struct KernelCursor<'vtab, T: KernelVTab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<(String, i64, Option<f64>)>,
    pos: usize,
    phantom: PhantomData<&'vtab T>,
}

impl<T: KernelVTab> KernelCursor<'_, T> {
    fn new(db: *mut ffi::sqlite3) -> Self {
        KernelCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        }
    }
}

unsafe impl<T: KernelVTab> VTabCursor for KernelCursor<'_, T> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        // A NULL in tbl/metric/int args is an error (decode_args); NULL
        // filter means "no filter" and is handled there too.
        let ka = decode_args(T::MODULE, T::ARGS, T::REQUIRED, idx_num, args)?;
        self.rows = T::run(self.db, &ka)?;
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        let (labels, ts, value) = &self.rows[self.pos];
        match col {
            COL_LABELS => ctx.set_result(labels),
            1 => ctx.set_result(ts),
            2 => match value {
                Some(v) => ctx.set_result(v),
                None => ctx.set_result(&rusqlite::types::Null),
            },
            // Hidden arg columns are omitted from output by set_omit;
            // selecting them explicitly yields NULL (args are echoed in
            // the query text anyway).
            _ => {
                ctx.set_result(&rusqlite::types::Null)?;
                Ok(())
            }
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

// ---------------------------------------------------------------------------
// F3: timeless_rollup — read one explicit rollup tier
// ---------------------------------------------------------------------------

const ROLLUP_ARGS: &[&str] = &[
    "tbl", "metric", "filter", "resolution", "start", "stop", "agg",
];
// All required except filter (bit 2).
const ROLLUP_REQUIRED: c_int = 0b111_1011;

/// timeless_rollup('metrics', 'cpu', NULL, 300, :t0, :t1, 'avg') — rows
/// (labels, bucket_ts, value) from the SETTLED buckets of one declared
/// tier. avg = sum/count at read; 'last' returns the bucket's last
/// sample value. Explicitly-tier reads only: no silent substitution for
/// raw, no raw tail merge (the raw table answers recent windows).
#[repr(C)]
pub(crate) struct RollupTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for RollupTab {
    type Aux = ();
    type Cursor = KernelCursor<'vtab, RollupTab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let handle = unsafe { db.handle() };
        db.config(VTabConfig::Innocuous)?;
        Ok((
            Cow::Borrowed(c"CREATE TABLE x(labels TEXT, ts INTEGER, value REAL, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, resolution HIDDEN, \
                            start HIDDEN, stop HIDDEN, agg HIDDEN)"),
            RollupTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, COL_FIRST_ARG, ROLLUP_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<KernelCursor<'vtab, RollupTab>> {
        Ok(KernelCursor::new(self.db))
    }
}

#[derive(Clone, Copy)]
enum RollupAgg {
    Avg,
    Sum,
    Min,
    Max,
    Count,
    Last,
}

impl KernelVTab for RollupTab {
    const MODULE: &'static str = "timeless_rollup";
    const ARGS: &'static [&'static str] = ROLLUP_ARGS;
    const REQUIRED: c_int = ROLLUP_REQUIRED;

    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<(String, i64, Option<f64>)>> {
        // KernelArgs reuse: width carries `resolution`; agg arrives as a
        // string in ka.agg_name (rollup vocabulary is larger than AggFn).
        let agg = match ka.agg_name.as_deref() {
            Some("avg") => RollupAgg::Avg,
            Some("sum") => RollupAgg::Sum,
            Some("min") => RollupAgg::Min,
            Some("max") => RollupAgg::Max,
            Some("count") => RollupAgg::Count,
            Some("last") => RollupAgg::Last,
            other => {
                return Err(module_err(format!(
                    "timeless_rollup: unknown agg {:?}; expected one of: avg, sum, min, max, count, last",
                    other.unwrap_or("<missing>")
                )))
            }
        };
        run_kernel(db, ka, |engine, sid| {
            let buckets = engine
                .query_rollup_by_id(sid, ka.width, ka.start, ka.stop)
                .map_err(module_err)?;
            Ok(buckets
                .into_iter()
                .map(|b| {
                    let value = match agg {
                        RollupAgg::Avg => b.sum / b.count as f64,
                        RollupAgg::Sum => b.sum,
                        RollupAgg::Min => b.min,
                        RollupAgg::Max => b.max,
                        RollupAgg::Count => b.count as f64,
                        RollupAgg::Last => b.last_val,
                    };
                    (b.bucket_ts, value)
                })
                .collect())
        })
    }
}

// ---------------------------------------------------------------------------
// F4: timeless_log_buckets / timeless_trace_buckets (FEATURE_PLAN.md)
// ---------------------------------------------------------------------------
//
// Histograms bin FORWARD: buckets are closed-open [start + k*step,
// start + k*step + step) aligned to `start` — deliberately different
// from the metrics grid kernels, which sample BACKWARD over (t-w, t].
// Both conventions are documented where they live.

/// Map provided args (bitmask) back to argv slots by name, with the
/// missing-required check. Shared by the F4 bucket TVFs.
fn named_slots(
    module: &str,
    names: &[&str],
    required_mask: c_int,
    idx_num: c_int,
) -> Result<Vec<Option<usize>>> {
    let missing: Vec<&str> = names
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            let bit = 1 << *i;
            (required_mask & bit) != 0 && (idx_num & bit) == 0
        })
        .map(|(_, n)| *n)
        .collect();
    if !missing.is_empty() {
        return Err(module_err(format!(
            "{module}: missing required argument(s): {} — call as {module}({})",
            missing.join(", "),
            names.join(", ")
        )));
    }
    let mut slot_of: Vec<Option<usize>> = vec![None; names.len()];
    let mut slot = 0usize;
    for (i, s_of) in slot_of.iter_mut().enumerate() {
        if idx_num & (1 << i) != 0 {
            *s_of = Some(slot);
            slot += 1;
        }
    }
    Ok(slot_of)
}

/// timeless_log_buckets('logs', 'level'|<index key>, filter_json, start,
/// stop, step) → (bucket_ts, group_key, n). The filter JSON holds
/// index-key equalities; a "level" key filters by level. Entries missing
/// the group key land in group ''.
const LOG_BUCKETS_ARGS: &[&str] = &["tbl", "group_by", "filter", "start", "stop", "step"];
const LOG_BUCKETS_REQUIRED: c_int = 0b11_1011; // all but filter
const LOG_BUCKETS_FIRST_ARG: c_int = 3;

#[repr(C)]
pub(crate) struct LogBucketsTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for LogBucketsTab {
    type Aux = ();
    type Cursor = LogBucketsCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let handle = unsafe { db.handle() };
        db.config(VTabConfig::Innocuous)?;
        Ok((
            Cow::Borrowed(c"CREATE TABLE x(bucket_ts INTEGER, group_key TEXT, n INTEGER, \
                            tbl HIDDEN, group_by HIDDEN, filter HIDDEN, start HIDDEN, \
                            stop HIDDEN, step HIDDEN)"),
            LogBucketsTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, LOG_BUCKETS_FIRST_ARG, LOG_BUCKETS_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<LogBucketsCursor<'vtab>> {
        Ok(LogBucketsCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct LogBucketsCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<(i64, String, u64)>,
    pos: usize,
    phantom: PhantomData<&'vtab LogBucketsTab>,
}

unsafe impl VTabCursor for LogBucketsCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_log_buckets";
        let slots = named_slots(M, LOG_BUCKETS_ARGS, LOG_BUCKETS_REQUIRED, idx_num)?;
        let text = |i: usize, what: &str| -> Result<String> {
            let v: Option<String> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let int = |i: usize, what: &str| -> Result<i64> {
            let v: Option<i64> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let (database, table) = split_spec(&text(0, "tbl")?);
        let group_by = text(1, "group_by")?;
        let filter_json: Option<String> = match slots[2] {
            None => None,
            Some(s) => args.get(s)?,
        };
        let (start, stop, step) = (int(3, "start")?, int(4, "stop")?, int(5, "step")?);

        let mut level = None;
        let mut metadata_eq: Vec<(String, String)> = Vec::new();
        if let Some(txt) = filter_json.filter(|t| !t.is_empty()) {
            for (k, v) in parse_labels_json(&txt)
                .map_err(|e| module_err(format!("{M}: filter: {e}")))?
            {
                if k == "level" {
                    level = Some(
                        timeless_core::level_from_name(&v).map_err(module_err)?,
                    );
                } else {
                    metadata_eq.push((k, v));
                }
            }
        }

        let _bind = DbGuard::bind(self.db);
        let shared = LogsTab::shared_engine_for(self.db, &database, &table)?;
        let q = timeless_core::LogQuery {
            ts_min: start,
            ts_max: stop,
            level,
            metadata_eq,
            message_contains: None,
            message_like_prune: None,
        };
        self.rows = shared
            .engine
            .bucket_counts(&q, &group_by, step)
            .map_err(module_err)?;
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        let (bucket_ts, group, n) = &self.rows[self.pos];
        match col {
            0 => ctx.set_result(bucket_ts),
            1 => ctx.set_result(group),
            2 => ctx.set_result(&(*n as i64)),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

/// timeless_trace_buckets('traces', service_filter, start, stop, step)
/// → (bucket_ts, service, spans, errors, dur_sum, dur_min, dur_max).
const TRACE_BUCKETS_ARGS: &[&str] = &["tbl", "service_filter", "start", "stop", "step"];
const TRACE_BUCKETS_REQUIRED: c_int = 0b1_1101; // all but service_filter
const TRACE_BUCKETS_FIRST_ARG: c_int = 10; // F7 added the three dur_pNN columns

#[repr(C)]
pub(crate) struct TraceBucketsTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for TraceBucketsTab {
    type Aux = ();
    type Cursor = TraceBucketsCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let handle = unsafe { db.handle() };
        db.config(VTabConfig::Innocuous)?;
        Ok((
            Cow::Borrowed(c"CREATE TABLE x(bucket_ts INTEGER, service TEXT, spans INTEGER, \
                            errors INTEGER, dur_sum INTEGER, dur_min INTEGER, dur_max INTEGER, \
                            dur_p50 INTEGER, dur_p95 INTEGER, dur_p99 INTEGER, \
                            tbl HIDDEN, service_filter HIDDEN, start HIDDEN, stop HIDDEN, \
                            step HIDDEN)"),
            TraceBucketsTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(
            info,
            TRACE_BUCKETS_FIRST_ARG,
            TRACE_BUCKETS_ARGS.len() as c_int,
        )
    }

    fn open(&mut self) -> Result<TraceBucketsCursor<'vtab>> {
        Ok(TraceBucketsCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct TraceBucketsCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<timeless_core::TraceBucketStat>,
    pos: usize,
    phantom: PhantomData<&'vtab TraceBucketsTab>,
}

unsafe impl VTabCursor for TraceBucketsCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_trace_buckets";
        let slots = named_slots(M, TRACE_BUCKETS_ARGS, TRACE_BUCKETS_REQUIRED, idx_num)?;
        let int = |i: usize, what: &str| -> Result<i64> {
            let v: Option<i64> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let spec: Option<String> = args.get(slots[0].unwrap())?;
        let spec = spec.ok_or_else(|| module_err(format!("{M}: tbl must not be NULL")))?;
        let (database, table) = split_spec(&spec);
        let service: Option<String> = match slots[1] {
            None => None,
            Some(s) => args.get::<Option<String>>(s)?.filter(|v| !v.is_empty()),
        };
        let (start, stop, step) = (int(2, "start")?, int(3, "stop")?, int(4, "step")?);

        let _bind = DbGuard::bind(self.db);
        let shared = TracesTab::shared_engine_for(self.db, &database, &table)?;
        let q = timeless_core::SpanQuery {
            ts_min: start,
            ts_max: stop,
            trace_id: None,
            service,
            kind: None,
            status: None,
            name: None,
        };
        self.rows = shared.engine.bucket_stats(&q, step).map_err(module_err)?;
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        let b = &self.rows[self.pos];
        match col {
            0 => ctx.set_result(&b.bucket_ts),
            1 => ctx.set_result(&b.service),
            2 => ctx.set_result(&(b.spans as i64)),
            3 => ctx.set_result(&(b.errors as i64)),
            4 => ctx.set_result(&b.dur_sum),
            5 => ctx.set_result(&b.dur_min),
            6 => ctx.set_result(&b.dur_max),
            7 => ctx.set_result(&b.dur_p50),
            8 => ctx.set_result(&b.dur_p95),
            9 => ctx.set_result(&b.dur_p99),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

// ---------------------------------------------------------------------------
// F1: timeless_series / timeless_stats (FEATURE_PLAN.md)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TimelessModule {
    Metrics,
    Logs,
    Traces,
}

impl TimelessModule {
    fn name(self) -> &'static str {
        match self {
            TimelessModule::Metrics => "timeless_metrics",
            TimelessModule::Logs => "timeless_logs",
            TimelessModule::Traces => "timeless_traces",
        }
    }
}

/// Which timeless module owns `table`, by shadow-table probe:
/// `_chunks` → metrics, `_trace_blocks` → traces, `_blocks` without
/// `_trace_blocks` → logs. One sqlite_schema query on the caller's
/// connection (DbGuard must be bound).
fn detect_module(database: &str, table: &str) -> Result<TimelessModule> {
    let conn = shared::current_conn().map_err(module_err)?;
    let sql = format!(
        "SELECT name FROM {} WHERE type = 'table' AND name IN (?1, ?2, ?3)",
        crate::sql_ident::qualified(database, "sqlite_schema")
    );
    let chunks = format!("{table}_chunks");
    let trace_blocks = format!("{table}_trace_blocks");
    let blocks = format!("{table}_blocks");
    let mut stmt = conn.prepare(&sql)?;
    let found: Vec<String> = stmt
        .query_map(rusqlite::params![chunks, trace_blocks, blocks], |r| r.get(0))?
        .collect::<std::result::Result<_, _>>()?;
    if found.iter().any(|n| n == &chunks) {
        Ok(TimelessModule::Metrics)
    } else if found.iter().any(|n| n == &trace_blocks) {
        Ok(TimelessModule::Traces)
    } else if found.iter().any(|n| n == &blocks) {
        Ok(TimelessModule::Logs)
    } else {
        Err(module_err(format!(
            "no such table: {table} is not a timeless virtual table in schema {database:?} \
             (no shadow tables found)"
        )))
    }
}

fn split_spec(spec: &str) -> (String, String) {
    match spec.split_once('.') {
        Some((schema, tbl)) => (schema.to_owned(), tbl.to_owned()),
        None => ("main".to_owned(), spec.to_owned()),
    }
}

/// Shared single-arg (tbl) best_index for the catalog/stats TVFs.
fn best_index_tbl(info: &mut IndexInfo, tbl_col: c_int) -> Result<bool> {
    let mut idx_num = 0;
    let mut slot = None;
    let mut unusable = false;
    for (i, constraint) in info.constraints().enumerate() {
        if constraint.column() != tbl_col {
            continue;
        }
        if !constraint.is_usable() {
            unusable = true;
        } else if constraint.operator() == IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ
            && slot.is_none()
        {
            idx_num = 1;
            slot = Some(i);
        }
    }
    if unusable && idx_num == 0 {
        return Ok(false);
    }
    if let Some(i) = slot {
        let mut usage = info.constraint_usage(i);
        usage.set_argv_index(1);
        usage.set_omit(true);
    }
    info.set_estimated_cost(100.0);
    info.set_estimated_rows(100);
    info.set_idx_num(idx_num);
    Ok(true)
}

fn require_tbl(module: &str, idx_num: c_int, args: &Filters<'_>) -> Result<(String, String)> {
    if idx_num != 1 {
        return Err(module_err(format!(
            "{module}: missing required argument tbl — call as {module}('<table>' | '<schema>.<table>')"
        )));
    }
    let spec: Option<String> = args.get(0)?;
    let spec = spec.ok_or_else(|| module_err(format!("{module}: tbl must not be NULL")))?;
    Ok(split_spec(&spec))
}

/// timeless_series('metrics') — the series catalog, from the in-memory
/// registry + chunk index only (no chunk decompression).
#[repr(C)]
pub(crate) struct SeriesTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

const SERIES_COL_TBL: c_int = 8;

unsafe impl<'vtab> VTab<'vtab> for SeriesTab {
    type Aux = ();
    type Cursor = SeriesCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let handle = unsafe { db.handle() };
        db.config(VTabConfig::Innocuous)?;
        Ok((
            Cow::Borrowed(c"CREATE TABLE x(name TEXT, labels TEXT, series_id INTEGER, \
                            min_ts INTEGER, max_ts INTEGER, points INTEGER, \
                            chunks INTEGER, buffered INTEGER, tbl HIDDEN)"),
            SeriesTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_tbl(info, SERIES_COL_TBL)
    }

    fn open(&mut self) -> Result<SeriesCursor<'vtab>> {
        Ok(SeriesCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct SeriesCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<timeless_core::SeriesOverview>,
    pos: usize,
    phantom: PhantomData<&'vtab SeriesTab>,
}

unsafe impl VTabCursor for SeriesCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        let (database, table) = require_tbl("timeless_series", idx_num, args)?;
        let _bind = DbGuard::bind(self.db);
        let module = detect_module(&database, &table)?;
        if module != TimelessModule::Metrics {
            return Err(module_err(format!(
                "timeless_series: {table} is a {} table; the series catalog exists for \
                 timeless_metrics only (timeless_stats works for every module)",
                module.name()
            )));
        }
        let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
        shared.engine.refresh_authoritative_state().map_err(module_err)?;
        self.rows = shared.engine.series_overview();
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        let row = &self.rows[self.pos];
        match col {
            0 => ctx.set_result(&row.name),
            1 => ctx.set_result(&labels_to_json(&row.labels)),
            2 => ctx.set_result(&row.series_id),
            3 => ctx.set_result(&row.min_ts),
            4 => ctx.set_result(&row.max_ts),
            5 => ctx.set_result(&(row.disk_points as i64 + row.buffered as i64)),
            6 => ctx.set_result(&(row.chunks as i64)),
            7 => ctx.set_result(&(row.buffered as i64)),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

/// timeless_label_values('metrics', 'cpu_usage', 'host') — sorted
/// distinct values of one label key across a metric's series, from the
/// in-memory registry only (no chunk reads). F8's discovery half: the
/// dropdown-population query for UI builders.
#[repr(C)]
pub(crate) struct LabelValuesTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

const LABEL_VALUES_FIRST_ARG: c_int = 1;
const LABEL_VALUES_ARGS: &[&str] = &["tbl", "metric", "key"];

unsafe impl<'vtab> VTab<'vtab> for LabelValuesTab {
    type Aux = ();
    type Cursor = LabelValuesCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let handle = unsafe { db.handle() };
        db.config(VTabConfig::Innocuous)?;
        Ok((
            Cow::Borrowed(c"CREATE TABLE x(value TEXT, tbl HIDDEN, metric HIDDEN, key HIDDEN)"),
            LabelValuesTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, LABEL_VALUES_FIRST_ARG, LABEL_VALUES_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<LabelValuesCursor<'vtab>> {
        Ok(LabelValuesCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct LabelValuesCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<String>,
    pos: usize,
    phantom: PhantomData<&'vtab LabelValuesTab>,
}

unsafe impl VTabCursor for LabelValuesCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_label_values";
        if idx_num != 0b111 {
            return Err(module_err(format!(
                "{M}: missing required argument(s) — call as {M}({})",
                LABEL_VALUES_ARGS.join(", ")
            )));
        }
        let get = |s: usize, what: &str| -> Result<String> {
            let v: Option<String> = args.get(s)?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let (database, table) = split_spec(&get(0, "tbl")?);
        let metric = get(1, "metric")?;
        let key = get(2, "key")?;

        let _bind = DbGuard::bind(self.db);
        let module = detect_module(&database, &table)?;
        if module != TimelessModule::Metrics {
            return Err(module_err(format!(
                "{M}: {table} is a {} table; label discovery exists for \
                 timeless_metrics only",
                module.name()
            )));
        }
        let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
        shared.engine.refresh_authoritative_state().map_err(module_err)?;
        self.rows = shared.engine.series_read().label_values(&metric, &key);
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        match col {
            0 => ctx.set_result(&self.rows[self.pos]),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

/// timeless_stats('t') — k/v health rows for any timeless table.
/// Engine-view counters come from the shared in-process engine; byte
/// sizes come from SQL over the shadow tables on the calling connection
/// (always-current, works before any engine exists elsewhere).
#[repr(C)]
pub(crate) struct StatsTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

const STATS_COL_TBL: c_int = 2;

unsafe impl<'vtab> VTab<'vtab> for StatsTab {
    type Aux = ();
    type Cursor = StatsCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let handle = unsafe { db.handle() };
        db.config(VTabConfig::Innocuous)?;
        Ok((
            Cow::Borrowed(c"CREATE TABLE x(key TEXT, value, tbl HIDDEN)"),
            StatsTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_tbl(info, STATS_COL_TBL)
    }

    fn open(&mut self) -> Result<StatsCursor<'vtab>> {
        Ok(StatsCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct StatsCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<(&'static str, rusqlite::types::Value)>,
    pos: usize,
    phantom: PhantomData<&'vtab StatsTab>,
}

fn sum_blob_bytes(database: &str, table: &str, suffix: &str, column: &str) -> Result<i64> {
    let conn = shared::current_conn().map_err(module_err)?;
    let sql = format!(
        "SELECT COALESCE(SUM(length({column})), 0) FROM {}",
        crate::sql_ident::qualified_shadow(database, table, suffix)
    );
    Ok(conn.query_row(&sql, [], |r| r.get(0))?)
}

fn count_rows(database: &str, table: &str, suffix: &str) -> Result<i64> {
    let conn = shared::current_conn().map_err(module_err)?;
    let sql = format!(
        "SELECT COUNT(*) FROM {}",
        crate::sql_ident::qualified_shadow(database, table, suffix)
    );
    Ok(conn.query_row(&sql, [], |r| r.get(0))?)
}

unsafe impl VTabCursor for StatsCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        use rusqlite::types::Value;
        let (database, table) = require_tbl("timeless_stats", idx_num, args)?;
        let _bind = DbGuard::bind(self.db);
        let module = detect_module(&database, &table)?;

        let opt_ts = |v: Option<i64>| v.map_or(Value::Null, Value::Integer);
        let mut rows: Vec<(&'static str, Value)> =
            vec![("module", Value::Text(module.name().to_owned()))];
        {
            // F2 retention (native ts units), NULL when unset.
            let conn = shared::current_conn().map_err(module_err)?;
            let retention = crate::shadow_meta::load_retention(&conn, &database, &table)
                .map_err(module_err)?;
            rows.push(("retention", opt_ts(retention)));
        }
        match module {
            TimelessModule::Metrics => {
                let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
                shared.engine.refresh_authoritative_state().map_err(module_err)?;
                let info = shared.engine.info();
                rows.extend([
                    ("series", Value::Integer(info.series_count as i64)),
                    ("chunks", Value::Integer(info.chunk_count as i64)),
                    ("disk_points", Value::Integer(info.disk_points as i64)),
                    ("buffered_points", Value::Integer(info.buffered_points as i64)),
                    ("bytes_on_disk", Value::Integer(info.total_bytes as i64)),
                    ("bytes_per_point", Value::Real(info.bytes_per_point)),
                    ("buffer_memory", Value::Integer(info.buffer_memory as i64)),
                    ("ts_min", opt_ts(info.oldest_ts)),
                    ("ts_max", opt_ts(info.newest_ts)),
                ]);
                // F3 ladder visibility: the declared tiers (native spec)
                // and how many rollup chunks exist across them.
                let conn = shared::current_conn().map_err(module_err)?;
                let tiers = crate::shadow_meta::load_meta_text(&conn, &database, &table, "rollups")
                    .map_err(module_err)?;
                rows.push((
                    "rollup_tiers",
                    tiers.map_or(Value::Null, Value::Text),
                ));
                let sql = format!(
                    "SELECT COUNT(*) FROM {} WHERE resolution > 0",
                    crate::sql_ident::qualified_shadow(&database, &table, "chunks")
                );
                rows.push((
                    "rollup_chunks",
                    Value::Integer(conn.query_row(&sql, [], |r| r.get(0))?),
                ));
            }
            TimelessModule::Logs => {
                let shared = LogsTab::shared_engine_for(self.db, &database, &table)?;
                let (blocks, raw_blocks, buffered) = shared.engine.stats();
                let (ts_min, ts_max) = shared.engine.ts_range();
                rows.extend([
                    ("blocks", Value::Integer(blocks as i64)),
                    ("raw_blocks", Value::Integer(raw_blocks as i64)),
                    ("buffered_entries", Value::Integer(buffered as i64)),
                    (
                        "bytes_on_disk",
                        Value::Integer(sum_blob_bytes(&database, &table, "blocks", "data")?),
                    ),
                    ("terms", Value::Integer(count_rows(&database, &table, "terms")?)),
                    ("ts_min", opt_ts(ts_min)),
                    ("ts_max", opt_ts(ts_max)),
                ]);
            }
            TimelessModule::Traces => {
                let shared = TracesTab::shared_engine_for(self.db, &database, &table)?;
                let (blocks, raw_blocks, buffered) = shared.engine.stats();
                let (ts_min, ts_max) = shared.engine.ts_range();
                rows.extend([
                    ("blocks", Value::Integer(blocks as i64)),
                    ("raw_blocks", Value::Integer(raw_blocks as i64)),
                    ("buffered_spans", Value::Integer(buffered as i64)),
                    (
                        "bytes_on_disk",
                        Value::Integer(sum_blob_bytes(&database, &table, "blocks", "data")?),
                    ),
                    ("terms", Value::Integer(count_rows(&database, &table, "terms")?)),
                    (
                        "trace_index_rows",
                        Value::Integer(count_rows(&database, &table, "trace_blocks")?),
                    ),
                    ("ts_min", opt_ts(ts_min)),
                    ("ts_max", opt_ts(ts_max)),
                ]);
            }
        }
        self.rows = rows;
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        let (key, value) = &self.rows[self.pos];
        match col {
            0 => ctx.set_result(key),
            1 => ctx.set_result(value),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}
