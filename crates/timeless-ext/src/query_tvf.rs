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
//!   SELECT labels FROM timeless_series(
//!     'metrics', 'cpu_usage', '{"host":{"re":"web-.*"}}');
//!   SELECT value FROM timeless_label_values(
//!     'metrics', 'cpu_usage', 'host', '{"env":{"neq":"dev"}}');
//!
//! The optional discovery filters use the same matcher contract and are
//! applied before catalog rows cross SQLite. The original one-argument
//! timeless_series and three-argument timeless_label_values calls remain
//! unchanged.
//!
//! The raw narrow waist is available without scanning unrelated series:
//!
//!   SELECT series_id, labels, ts, value FROM timeless_raw(
//!     'metrics', 'cpu_usage', '{"host":"pvm1"}', :start, :stop);
//!
//! Embedded hosts can amortize host-language/SQLite row crossings with the
//! series-batch form. `points` is u32 LE count, then i64 LE timestamps and
//! f64-bit LE values:
//!
//!   SELECT series_id, labels, points FROM timeless_raw_batches(
//!     'metrics', 'cpu_usage', '{"host":"pvm1"}', :start, :stop);
//!
//! Very wide fanout queries can cross SQLite only once with the frame form:
//!
//!   SELECT frame FROM timeless_raw_frame(
//!     'metrics', 'cpu_usage', NULL, :start, :stop, :max_work_points);
//!
//! `frame` is `TRF1`, u32 LE series count, u64 LE total point count, then
//! columnar series IDs, per-series point counts, timestamps, and f64 value
//! bits. Empty series are omitted and each point slice retains raw query order.
//! The optional positive limit is inclusive and rejects conservative chunk +
//! buffer work before persisted payload reads. Omit it for the original call.
//!
//! Scalar reductions stay below the SQLite/host materialization boundary:
//!
//!   SELECT series_id, labels, value FROM timeless_aggregate(
//!     'metrics', 'cpu_usage', '{"env":"prod"}', :start, :stop, 'avg');
//!
//! Supported operations are avg, sum, min, max, and count. Bounds are
//! inclusive, empty series emit no row, and count is returned as a SQLite
//! INTEGER. Sum/avg use persisted per-chunk sums for fully covered chunks and
//! decode only boundary chunks; consequently their documented accumulation
//! order is chunk-local then chunk-index order, not a flat SQL SUM scan.
//!
//! Latest-point selection also stays below the materialization boundary:
//!
//!   SELECT series_id, labels, ts, value FROM timeless_latest(
//!     'metrics', 'cpu_usage', '{"env":"prod"}', :start, :stop);
//!
//! Bounds are inclusive and empty series emit no row. The greatest timestamp
//! wins; duplicate maximum timestamps retain the first point in stable raw
//! engine order. Candidate chunks are searched newest-first.
//!
//! Logs expose an exact scalar-count waist as a one-row TVF. `filter` is a
//! flat JSON object whose `level` member selects severity and whose other
//! members are metadata equalities; `message_contains` is a case-insensitive
//! substring. Missing bounds default to the complete i64 range:
//!
//!   SELECT n FROM timeless_log_count(
//!     'logs', '{"level":"error","service":"api"}', 'timeout', :start, :stop,
//!     :max_work_entries);
//!
//! Fully covered unfiltered or level-pure blocks contribute persisted entry
//! counts without payload reads. All other blocks decode one at a time, so
//! exact counts never materialize a database-sized rowset.
//!
//! Bounded field discovery uses the identical exact predicate and returns the
//! lexicographically first distinct values while decoding at most one block
//! and retaining at most `limit + 1` strings:
//!
//!   SELECT value FROM timeless_log_values(
//!     'logs', 'host', '{"level":"error"}', NULL, :start, :stop, 1000,
//!     :max_work_entries);
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
//! `avg` uses compensated summation plus an overflow-safe incremental mean;
//! the other folds retain their documented IEEE behavior.
//!
//! Embedded hosts can request one versioned bucket blob per series instead of
//! crossing the SQLite boundary once per grid point:
//!
//!   SELECT series_id, labels, buckets FROM timeless_window_batches(
//!     'metrics', 'cpu_usage', NULL,
//!     :start, :stop, :step, :window, 'avg', NULL, :max_work_points);
//!
//! `buckets` is `TWB1`, u32 LE count, i64 LE timestamps, a packed validity
//! bitmap (one bit per timestamp, low bit first), then f64-bit LE values. The
//! bitmap preserves `fill='null'`; sparse calls naturally contain only set
//! bits. Unknown magic/version and malformed lengths must be rejected by the
//! decoder rather than guessed.
//! The optional positive `max_work_points` is inclusive and rejects before
//! chunk reads when either conservative input points or possible grid output
//! exceed the bound. Omit both trailing arguments for the original call.
//!
//! Stored rollups have an analogous all-aggregates batch form:
//!
//!   SELECT series_id, labels, buckets FROM timeless_rollup_batches(
//!     'metrics', 'cpu_usage', NULL, 300, :start, :stop);
//!
//! `buckets` is `TRB1`, u32 LE count, then eight 8-byte LE columns:
//! bucket timestamp (i64), exact count (u64), avg bits, sum bits, min bits,
//! max bits, last-sample timestamp (i64), and last-value bits. `avg` remains
//! derived at read time; the other fields preserve the stored rollup contract.
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
use std::collections::HashSet;
use std::ffi::{c_int, CStr};
use std::marker::PhantomData;
use std::sync::Arc;

use rusqlite::ffi;
use rusqlite::types::Value;
use rusqlite::vtab::{
    Context, Filters, IndexConstraintOp, IndexInfo, Module, VTab, VTabConfig, VTabConnection,
    VTabCursor,
};
use rusqlite::{Connection, Error, Result};
use timeless_core::{AggFn, Engine, Labels, LogQuery};

use crate::flatjson::{labels_to_json, parse_labels_json, parse_matchers_json, MatcherSpec};
use crate::logs_vtab::{LogsTab, MERGE_TARGET_ENTRIES as LOG_MERGE_TARGET_ENTRIES};
use crate::metrics_vtab::MetricsTab;
use crate::query_frame::{encode_aggregate_frame, encode_latest_frame};
use crate::query_report::LogQueryReportState;
use crate::shared::{self, DbGuard, SharedEngine};
use crate::sql_value::integer_affinity;
use crate::traces_vtab::TracesTab;

fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

fn positive_work_limit(module: &str, args: &Filters<'_>, slot: usize) -> Result<u64> {
    let value = integer_affinity(args.get::<Value>(slot)?).ok_or_else(|| {
        module_err(format!(
            "{module}: max_work_points must be a positive INTEGER"
        ))
    })?;
    if value <= 0 {
        return Err(module_err(format!(
            "{module}: max_work_points must be positive, got {value}"
        )));
    }
    Ok(value as u64)
}

fn read_permit<'a, E>(
    shared: &'a SharedEngine<E>,
    db: *mut ffi::sqlite3,
    table: &str,
) -> Result<shared::ReadPermit<'a>> {
    shared
        .write_gate
        .acquire_read(db as usize, table)
        .map_err(module_err)
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
fn parse_window_op(module: &str, name: Option<&str>) -> Result<timeless_core::WindowOp> {
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
                let q: f64 = q
                    .parse()
                    .map_err(|_| module_err(format!("{module}: bad percentile {name:?}")))?;
                if !(q > 0.0 && q <= 100.0) {
                    return Err(module_err(format!(
                        "{module}: percentile must be in (0, 100], got {name:?}"
                    )));
                }
                return Ok(WindowOp::Percentile(q));
            }
            if let Some(q) = name.strip_prefix("tavg:") {
                let q: f64 = q
                    .parse()
                    .map_err(|_| module_err(format!("{module}: bad trim fraction {name:?}")))?;
                if !(0.0..50.0).contains(&q) {
                    return Err(module_err(format!(
                        "{module}: trim fraction must be in [0, 50), got {name:?}"
                    )));
                }
                return Ok(WindowOp::TrimmedMean(q));
            }
            return Err(module_err(format!(
                "{module}: unknown agg {name:?}; expected one of: sum, min, max, \
                 count, avg, delta, increase, rate, pNN (e.g. p95), tavg:N"
            )));
        }
    })
}

/// Register the TVF modules on a freshly-loaded connection.
pub(crate) fn register(db: &Connection, query_reports: Arc<LogQueryReportState>) -> Result<()> {
    const GRID: Module<GridTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_grid", &GRID, None::<()>)?;
    const WINDOW: Module<WindowTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_window", &WINDOW, None::<()>)?;
    const WINDOW_BATCHES: Module<WindowBatchTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_window_batches", &WINDOW_BATCHES, None::<()>)?;
    const SERIES: Module<SeriesTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_series", &SERIES, None::<()>)?;
    const STATS: Module<StatsTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_stats", &STATS, None::<()>)?;
    const ROLLUP: Module<RollupTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_rollup", &ROLLUP, None::<()>)?;
    const ROLLUP_BATCHES: Module<RollupBatchTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_rollup_batches", &ROLLUP_BATCHES, None::<()>)?;
    const LOG_BUCKETS: Module<LogBucketsTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_log_buckets", &LOG_BUCKETS, None::<()>)?;
    const LOG_COUNT: Module<LogCountTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_log_count", &LOG_COUNT, None::<()>)?;
    const LOG_VALUES: Module<LogValuesTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_log_values", &LOG_VALUES, None::<()>)?;
    const LOG_QUERY_STATS: Module<LogQueryStatsTab> = Module::eponymous_only_module();
    db.create_module(
        c"timeless_log_query_stats",
        &LOG_QUERY_STATS,
        Some(query_reports),
    )?;
    const TRACE_DISCOVERY: Module<TraceDiscoveryTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_trace_services", &TRACE_DISCOVERY, None::<()>)?;
    db.create_module(c"timeless_trace_operations", &TRACE_DISCOVERY, None::<()>)?;
    const TRACE_BUCKETS: Module<TraceBucketsTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_trace_buckets", &TRACE_BUCKETS, None::<()>)?;
    const LABEL_VALUES: Module<LabelValuesTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_label_values", &LABEL_VALUES, None::<()>)?;
    const RAW: Module<RawTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_raw", &RAW, None::<()>)?;
    const RAW_BATCHES: Module<RawBatchTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_raw_batches", &RAW_BATCHES, None::<()>)?;
    const RAW_FRAME: Module<RawFrameTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_raw_frame", &RAW_FRAME, None::<()>)?;
    const AGGREGATE: Module<AggregateTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_aggregate", &AGGREGATE, None::<()>)?;
    const AGGREGATE_FRAME: Module<AggregateFrameTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_aggregate_frame", &AGGREGATE_FRAME, None::<()>)?;
    const LATEST: Module<LatestTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_latest", &LATEST, None::<()>)?;
    const LATEST_FRAME: Module<LatestFrameTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_latest_frame", &LATEST_FRAME, None::<()>)
}

/// `timeless_log_query_stats('logs')` consumes the request-owned report from
/// the immediately preceding successful `timeless_logs` scan on this SQLite
/// connection. It is deliberately separate from cumulative
/// `timeless_stats('logs')`: concurrent readers cannot contaminate these
/// values, and a failed/cancelled/new scan clears the prior report.
#[repr(C)]
pub(crate) struct LogQueryStatsTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
    reports: Arc<LogQueryReportState>,
}

const LOG_QUERY_STATS_TBL: c_int = 16;

unsafe impl<'vtab> VTab<'vtab> for LogQueryStatsTab {
    type Aux = Arc<LogQueryReportState>;
    type Cursor = LogQueryStatsCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let reports = aux.cloned().ok_or_else(|| {
            module_err("timeless_log_query_stats: missing connection report state".into())
        })?;
        let handle = unsafe { db.handle() };
        db.config(VTabConfig::Innocuous)?;
        Ok((
            Cow::Borrowed(
                c"CREATE TABLE x(\
                query_total_ns INTEGER,\
                query_snapshot_ns INTEGER,\
                query_materialize_ns INTEGER,\
                snapshot_payload_bytes INTEGER,\
                payload_bytes_read INTEGER,\
                candidate_blocks INTEGER,\
                processed_blocks INTEGER,\
                blocks_skipped_by_bound INTEGER,\
                buffered_entries_processed INTEGER,\
                decoded_entries INTEGER,\
                processed_entries INTEGER,\
                matched_entries INTEGER,\
                returned_entries INTEGER,\
                values_read INTEGER,\
                timestamps_read INTEGER,\
                stable_location_snapshot INTEGER,\
                tbl HIDDEN)",
            ),
            LogQueryStatsTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
                reports,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_tbl(info, LOG_QUERY_STATS_TBL)
    }

    fn open(&mut self) -> Result<Self::Cursor> {
        Ok(LogQueryStatsCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            reports: Arc::clone(&self.reports),
            report: None,
            done: false,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct LogQueryStatsCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    reports: Arc<LogQueryReportState>,
    report: Option<timeless_core::LogQueryExecutionReport>,
    done: bool,
    phantom: PhantomData<&'vtab LogQueryStatsTab>,
}

fn report_i64(field: &str, value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        module_err(format!(
            "timeless_log_query_stats: {field} exceeds SQLite INTEGER range"
        ))
    })
}

unsafe impl VTabCursor for LogQueryStatsCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        let (database, table) = require_tbl("timeless_log_query_stats", idx_num, args)?;
        let _bind = DbGuard::bind(self.db);
        if !matches!(detect_module(&database, &table)?, TimelessModule::Logs) {
            return Err(module_err(format!(
                "timeless_log_query_stats: {database}.{table} is not a timeless_logs table"
            )));
        }
        self.report = Some(self.reports.take(&database, &table).ok_or_else(|| {
            module_err(format!(
                "timeless_log_query_stats: no unconsumed successful query report for \
                 {database}.{table} on this connection; fully consume a timeless_logs \
                 SELECT and call timeless_log_query_stats immediately afterward"
            ))
        })?);
        self.done = false;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.done = true;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.done || self.report.is_none()
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        let report = self
            .report
            .as_ref()
            .ok_or_else(|| module_err("timeless_log_query_stats: cursor has no report".into()))?;
        let value = match col {
            0 => report_i64("query_total_ns", report.query_total_ns)?,
            1 => report_i64("query_snapshot_ns", report.query_snapshot_ns)?,
            2 => report_i64("query_materialize_ns", report.query_materialize_ns)?,
            3 => report_i64("snapshot_payload_bytes", report.snapshot_payload_bytes)?,
            4 => report_i64("payload_bytes_read", report.payload_bytes_read)?,
            5 => report_i64("candidate_blocks", report.candidate_blocks)?,
            6 => report_i64("processed_blocks", report.processed_blocks)?,
            7 => report_i64("blocks_skipped_by_bound", report.blocks_skipped_by_bound)?,
            8 => report_i64(
                "buffered_entries_processed",
                report.buffered_entries_processed,
            )?,
            9 => report_i64("decoded_entries", report.decoded_entries)?,
            10 => report_i64("processed_entries", report.processed_entries)?,
            11 => report_i64("matched_entries", report.matched_entries)?,
            12 => report_i64("returned_entries", report.returned_entries)?,
            13 => report_i64("values_read", report.values_read)?,
            14 => report_i64("timestamps_read", report.timestamps_read)?,
            15 => i64::from(report.stable_location_snapshot),
            _ => {
                return ctx.set_result(&rusqlite::types::Null);
            }
        };
        ctx.set_result(&value)
    }

    fn rowid(&self) -> Result<i64> {
        Ok(1)
    }
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
        assert!(matchers_pass(
            &labels(&[("env", "prod"), ("dc", "us-east")]),
            &m
        ));
        assert!(!matchers_pass(
            &labels(&[("env", "dev"), ("dc", "us-east")]),
            &m
        ));
        assert!(!matchers_pass(
            &labels(&[("env", "prod"), ("dc", "eu-1")]),
            &m
        ));
    }

    #[test]
    fn invalid_regex_names_pattern_and_label() {
        let err = compile_filter("t", r#"{"host": {"re": "["}}"#)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("invalid regex") && err.contains("host"),
            "{err}"
        );
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
    /// Optional relational series-handle constraint supplied by SQLite.
    series_selection: SeriesSelection,
    /// Optional inclusive cap on input and materialized work points for the
    /// packed batch surfaces that support bounded execution.
    max_work_points: Option<u64>,
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
        names
            .iter()
            .position(|n| *n == name)
            .and_then(|i| slot_of[i])
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

    let max_work_points = match find("max_work_points") {
        None => None,
        Some(slot) => Some(positive_work_limit(module, args, slot)?),
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
        series_selection: decode_series_selection(idx_num, names.len(), args)?,
        max_work_points,
    })
}

/// Shared best_index for both modules: collect EQ constraints on the
/// hidden arg columns into a bitmask, assign argv slots in canonical
/// order, defer required-arg checking to filter (clearer errors than a
/// bare "no query solution").
fn best_index_args(info: &mut IndexInfo, first_arg: c_int, n_args: c_int) -> Result<bool> {
    best_index_args_with_series_id(info, first_arg, n_args, None)
}

// Keep the selected-series marker outside every hidden-argument mask. The
// largest metrics TVF currently has nine arguments, so bit 30 leaves ample
// room for additive arguments without changing the public planner encoding.
const PLAN_SERIES_ID_EQ: c_int = 1 << 30;

/// Hidden-argument planning plus an optional equality constraint on a visible
/// (or explicitly selectable hidden) `series_id` column. Hidden arguments are
/// always assigned first in canonical order and the series id is appended, so
/// the existing argument decoders remain backward-compatible.
fn best_index_args_with_series_id(
    info: &mut IndexInfo,
    first_arg: c_int,
    n_args: c_int,
    series_id_col: Option<c_int>,
) -> Result<bool> {
    let mut idx_num: c_int = 0;
    let mut unusable: c_int = 0;
    let mut slots: Vec<Option<usize>> = vec![None; n_args as usize];
    let mut series_slot: Option<usize> = None;
    for (i, constraint) in info.constraints().enumerate() {
        let col = constraint.column();
        if Some(col) == series_id_col
            && constraint.operator() == IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ
        {
            // An unusable visible/output-column constraint can simply be
            // evaluated by SQLite after a broad scan. Rejecting that plan
            // creates a dependency cycle when two series-aware virtual
            // tables are joined on series_id: either side must remain a
            // valid outer loop before the other side can use the handle.
            if constraint.is_usable() && series_slot.is_none() {
                series_slot = Some(i);
            }
            continue;
        }
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
    if let Some(slot) = series_slot {
        n_arg += 1;
        let mut usage = info.constraint_usage(slot);
        usage.set_argv_index(n_arg);
        usage.set_omit(true);
        idx_num |= PLAN_SERIES_ID_EQ;
        info.set_estimated_cost(10.0);
        info.set_estimated_rows(10);
    } else {
        info.set_estimated_cost(1000.0);
        info.set_estimated_rows(1000);
    }
    info.set_idx_num(idx_num);
    Ok(true)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SeriesSelection {
    All,
    Empty,
    Id(i64),
}

type KernelRow = (i64, String, i64, Option<f64>);

/// Decode the series-id argv appended by `best_index_args_with_series_id`.
/// SQL `series_id = NULL` is an empty predicate, not an extension error.
fn decode_series_selection(
    idx_num: c_int,
    n_args: usize,
    args: &Filters<'_>,
) -> Result<SeriesSelection> {
    if idx_num & PLAN_SERIES_ID_EQ == 0 {
        return Ok(SeriesSelection::All);
    }
    let arg_mask = if n_args == 0 { 0 } else { (1u32 << n_args) - 1 };
    let slot = ((idx_num as u32) & arg_mask).count_ones() as usize;
    match integer_affinity(args.get::<Value>(slot)?) {
        Some(series_id) => Ok(SeriesSelection::Id(series_id)),
        None => Ok(SeriesSelection::Empty),
    }
}

/// Select catalog candidates without enumerating a metric when SQLite has
/// supplied an exact durable series handle. The ID predicate intersects with
/// metric and matcher arguments; it never bypasses their public semantics.
fn metric_candidates(
    engine: &Engine,
    metric: &str,
    eq: &Labels,
    matchers: &[(String, LabelMatcher)],
    selection: SeriesSelection,
) -> Vec<(i64, Labels)> {
    let reg = engine.series_read();
    match selection {
        SeriesSelection::Empty => Vec::new(),
        SeriesSelection::Id(sid) => reg
            .info_for(sid)
            .filter(|info| info.metric_name == metric)
            .filter(|info| {
                eq.iter()
                    .all(|(key, value)| info.labels.get(key) == Some(value))
            })
            .filter(|info| matchers_pass(&info.labels, matchers))
            .map(|info| vec![(sid, info.labels.clone())])
            .unwrap_or_default(),
        SeriesSelection::All => reg
            .find_series(metric, eq)
            .into_iter()
            .filter_map(|sid| reg.info_for(sid).map(|info| (sid, info.labels.clone())))
            .filter(|(_, labels)| matchers_pass(labels, matchers))
            .collect(),
    }
}

/// Frame TVFs attach catalog data by ID and therefore need no label clones.
fn metric_candidate_ids(
    engine: &Engine,
    metric: &str,
    eq: &Labels,
    matchers: &[(String, LabelMatcher)],
) -> Vec<i64> {
    let reg = engine.series_read();
    reg.find_series(metric, eq)
        .into_iter()
        .filter(|series_id| {
            reg.info_for(*series_id)
                .is_some_and(|info| matchers_pass(&info.labels, matchers))
        })
        .collect()
}

/// Resolve the engine and run one kernel scan into materialized rows.
fn run_kernel(
    db: *mut ffi::sqlite3,
    ka: &KernelArgs,
    kernel: impl Fn(&Engine, i64) -> Result<Vec<(i64, f64)>>,
) -> Result<Vec<KernelRow>> {
    let _bind = DbGuard::bind(db);
    let shared: Arc<SharedEngine<Engine>> =
        MetricsTab::shared_engine_for(db, &ka.database, &ka.table)?;
    let _read = read_permit(&shared, db, &ka.table)?;
    shared
        .engine
        .refresh_authoritative_state()
        .map_err(module_err)?;

    // Candidate snapshot, then sequential per-series kernels — the
    // rayon-free discipline every vtab callback must follow (see
    // collect_metric in metrics_vtab.rs).
    let candidates = metric_candidates(
        &shared.engine,
        &ka.metric,
        &ka.filter,
        &ka.matchers,
        ka.series_selection,
    );

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
                rows.push((sid, labels_json.clone(), t, v));
                match t.checked_add(ka.step) {
                    Some(next) => t = next,
                    None => break,
                }
            }
        } else {
            for (ts, value) in points {
                rows.push((sid, labels_json.clone(), ts, Some(value)));
            }
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// timeless_grid
// ---------------------------------------------------------------------------

const GRID_ARGS: &[&str] = &[
    "tbl", "metric", "filter", "start", "stop", "step", "lookback", "fill",
];
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
            Cow::Borrowed(
                c"CREATE TABLE x(labels TEXT, ts INTEGER, value REAL, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, start HIDDEN, \
                            stop HIDDEN, step HIDDEN, lookback HIDDEN, fill HIDDEN, \
                            series_id HIDDEN)",
            ),
            GridTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args_with_series_id(
            info,
            COL_FIRST_ARG,
            GRID_ARGS.len() as c_int,
            Some(COL_FIRST_ARG + GRID_ARGS.len() as c_int),
        )
    }

    fn open(&mut self) -> Result<KernelCursor<'vtab, GridTab>> {
        Ok(KernelCursor::new(self.db))
    }
}

impl KernelVTab for GridTab {
    const MODULE: &'static str = "timeless_grid";
    const ARGS: &'static [&'static str] = GRID_ARGS;
    const REQUIRED: c_int = GRID_REQUIRED;
    const SERIES_ID_COL: c_int = COL_FIRST_ARG + GRID_ARGS.len() as c_int;

    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<KernelRow>> {
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
const WINDOW_BATCH_ARGS: &[&str] = &[
    "tbl",
    "metric",
    "filter",
    "start",
    "stop",
    "step",
    "window",
    "agg",
    "fill",
    "max_work_points",
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
            Cow::Borrowed(
                c"CREATE TABLE x(labels TEXT, ts INTEGER, value REAL, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, start HIDDEN, \
                            stop HIDDEN, step HIDDEN, window HIDDEN, agg HIDDEN, \
                            fill HIDDEN, series_id HIDDEN)",
            ),
            WindowTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args_with_series_id(
            info,
            COL_FIRST_ARG,
            WINDOW_ARGS.len() as c_int,
            Some(COL_FIRST_ARG + WINDOW_ARGS.len() as c_int),
        )
    }

    fn open(&mut self) -> Result<KernelCursor<'vtab, WindowTab>> {
        Ok(KernelCursor::new(self.db))
    }
}

impl KernelVTab for WindowTab {
    const MODULE: &'static str = "timeless_window";
    const ARGS: &'static [&'static str] = WINDOW_ARGS;
    const REQUIRED: c_int = WINDOW_REQUIRED;
    const SERIES_ID_COL: c_int = COL_FIRST_ARG + WINDOW_ARGS.len() as c_int;

    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<KernelRow>> {
        let op = parse_window_op(Self::MODULE, ka.agg_name.as_deref())?;
        run_kernel(db, ka, |engine, sid| {
            engine
                .query_window_op_by_id(sid, ka.start, ka.stop, ka.step, ka.width, op)
                .map_err(module_err)
        })
    }
}

// ---------------------------------------------------------------------------
// timeless_window_batches — one packed grid/window blob per matched series
// ---------------------------------------------------------------------------

const WINDOW_BATCH_MAGIC: &[u8; 4] = b"TWB1";

#[repr(C)]
pub(crate) struct WindowBatchTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for WindowBatchTab {
    type Aux = ();
    type Cursor = WindowBatchCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(series_id INTEGER, labels TEXT, buckets BLOB, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, start HIDDEN, \
                            stop HIDDEN, step HIDDEN, window HIDDEN, agg HIDDEN, fill HIDDEN, \
                            max_work_points HIDDEN)",
            ),
            WindowBatchTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args_with_series_id(info, 3, WINDOW_BATCH_ARGS.len() as c_int, Some(0))
    }

    fn open(&mut self) -> Result<WindowBatchCursor<'vtab>> {
        Ok(WindowBatchCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct WindowBatchCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<(i64, Labels, Vec<u8>)>,
    pos: usize,
    phantom: PhantomData<&'vtab WindowBatchTab>,
}

unsafe impl VTabCursor for WindowBatchCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_window_batches";
        let ka = decode_args(M, WINDOW_BATCH_ARGS, WINDOW_REQUIRED, idx_num, args)?;
        let op = parse_window_op(M, ka.agg_name.as_deref())?;

        let _bind = DbGuard::bind(self.db);
        let shared: Arc<SharedEngine<Engine>> =
            MetricsTab::shared_engine_for(self.db, &ka.database, &ka.table)?;
        let _read = read_permit(&shared, self.db, &ka.table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;

        let candidates = metric_candidates(
            &shared.engine,
            &ka.metric,
            &ka.filter,
            &ka.matchers,
            ka.series_selection,
        );

        let series_ids: Vec<i64> = candidates.iter().map(|(sid, _)| *sid).collect();
        let batch = match ka.max_work_points {
            Some(limit) => shared.engine.query_window_op_batch_by_id_limited(
                &series_ids,
                ka.start,
                ka.stop,
                ka.step,
                ka.width,
                op,
                limit,
            ),
            None => shared.engine.query_window_op_batch_by_id(
                &series_ids,
                ka.start,
                ka.stop,
                ka.step,
                ka.width,
                op,
            ),
        }
        .map_err(module_err)?;

        let mut rows = Vec::new();
        for ((sid, labels), (result_sid, points)) in candidates.into_iter().zip(batch) {
            debug_assert_eq!(sid, result_sid);
            if points.is_empty() {
                continue;
            }

            let packed = encode_window_points(&points, &ka)?;
            rows.push((sid, labels, packed));
        }
        rows.sort_by(|a, b| (&a.1, a.0).cmp(&(&b.1, b.0)));
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
        let (sid, labels, buckets) = &self.rows[self.pos];
        match col {
            0 => ctx.set_result(sid),
            1 => {
                let json = labels_to_json(labels);
                ctx.set_result(&json)
            }
            2 => ctx.set_result(buckets),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

fn encode_window_points(points: &[(i64, f64)], ka: &KernelArgs) -> Result<Vec<u8>> {
    let count = if ka.fill {
        let span = (ka.stop as i128) - (ka.start as i128);
        usize::try_from(span / ka.step as i128 + 1)
            .map_err(|_| module_err("timeless_window_batches: grid is too large".into()))?
    } else {
        points.len()
    };
    let n: u32 = count
        .try_into()
        .map_err(|_| module_err("timeless_window_batches: too many buckets".into()))?;
    let bitmap_bytes = count
        .checked_add(7)
        .ok_or_else(|| module_err("timeless_window_batches: bitmap size overflow".into()))?
        / 8;
    let column_bytes = count
        .checked_mul(8)
        .ok_or_else(|| module_err("timeless_window_batches: column size overflow".into()))?;
    let capacity = 8usize
        .checked_add(column_bytes)
        .and_then(|v| v.checked_add(bitmap_bytes))
        .and_then(|v| v.checked_add(column_bytes))
        .ok_or_else(|| module_err("timeless_window_batches: blob size overflow".into()))?;

    let mut timestamps = Vec::with_capacity(column_bytes);
    let mut bitmap = vec![0u8; bitmap_bytes];
    let mut values = Vec::with_capacity(column_bytes);

    if ka.fill {
        let mut point_index = 0usize;
        let mut t = ka.start;
        for index in 0..count {
            timestamps.extend_from_slice(&t.to_le_bytes());
            if point_index < points.len() && points[point_index].0 == t {
                bitmap[index / 8] |= 1 << (index % 8);
                values.extend_from_slice(&points[point_index].1.to_bits().to_le_bytes());
                point_index += 1;
            } else {
                values.extend_from_slice(&0u64.to_le_bytes());
            }
            if index + 1 < count {
                t = t.checked_add(ka.step).ok_or_else(|| {
                    module_err("timeless_window_batches: grid timestamp overflow".into())
                })?;
            }
        }
    } else {
        for (index, (timestamp, _)) in points.iter().enumerate() {
            timestamps.extend_from_slice(&timestamp.to_le_bytes());
            bitmap[index / 8] |= 1 << (index % 8);
        }
        for (_, value) in points {
            values.extend_from_slice(&value.to_bits().to_le_bytes());
        }
    }

    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(WINDOW_BATCH_MAGIC);
    out.extend_from_slice(&n.to_le_bytes());
    out.extend_from_slice(&timestamps);
    out.extend_from_slice(&bitmap);
    out.extend_from_slice(&values);
    Ok(out)
}

#[cfg(test)]
mod window_batch_tests {
    use super::*;

    fn args(fill: bool) -> KernelArgs {
        KernelArgs {
            database: "main".into(),
            table: "metrics".into(),
            metric: "cpu".into(),
            filter: Labels::new(),
            matchers: Vec::new(),
            start: 10,
            stop: 30,
            step: 10,
            width: 10,
            agg_name: Some("avg".into()),
            fill,
            series_selection: SeriesSelection::All,
            max_work_points: None,
        }
    }

    #[test]
    fn sparse_window_blob_is_versioned_columnar_and_bit_exact() {
        let blob = encode_window_points(&[(10, 1.5), (30, -2.25)], &args(false)).unwrap();
        assert_eq!(&blob[..4], b"TWB1");
        assert_eq!(u32::from_le_bytes(blob[4..8].try_into().unwrap()), 2);
        assert_eq!(i64::from_le_bytes(blob[8..16].try_into().unwrap()), 10);
        assert_eq!(i64::from_le_bytes(blob[16..24].try_into().unwrap()), 30);
        assert_eq!(blob[24], 0b0000_0011);
        assert_eq!(f64::from_le_bytes(blob[25..33].try_into().unwrap()), 1.5);
        assert_eq!(f64::from_le_bytes(blob[33..41].try_into().unwrap()), -2.25);
        assert_eq!(blob.len(), 41);
    }

    #[test]
    fn dense_window_blob_marks_null_grid_points_in_the_bitmap() {
        let blob = encode_window_points(&[(10, 1.5), (30, -2.25)], &args(true)).unwrap();
        assert_eq!(u32::from_le_bytes(blob[4..8].try_into().unwrap()), 3);
        assert_eq!(i64::from_le_bytes(blob[8..16].try_into().unwrap()), 10);
        assert_eq!(i64::from_le_bytes(blob[16..24].try_into().unwrap()), 20);
        assert_eq!(i64::from_le_bytes(blob[24..32].try_into().unwrap()), 30);
        assert_eq!(blob[32], 0b0000_0101);
        assert_eq!(f64::from_le_bytes(blob[33..41].try_into().unwrap()), 1.5);
        assert_eq!(f64::from_le_bytes(blob[41..49].try_into().unwrap()), 0.0);
        assert_eq!(f64::from_le_bytes(blob[49..57].try_into().unwrap()), -2.25);
        assert_eq!(blob.len(), 57);
    }
}

// ---------------------------------------------------------------------------
// timeless_aggregate — one chunk-aware scalar reduction per matched series
// ---------------------------------------------------------------------------

const AGGREGATE_ARGS: &[&str] = &["tbl", "metric", "filter", "start", "stop", "agg"];
const AGGREGATE_REQUIRED: c_int = 0b11_1011; // all except filter
const AGGREGATE_FIRST_ARG: c_int = 3;

#[derive(Clone, Copy)]
enum AggregateValue {
    Real(f64),
    Integer(i64),
}

fn parse_scalar_aggregate(module: &str, name: &str) -> Result<AggFn> {
    match name {
        "avg" => Ok(AggFn::Avg),
        "sum" => Ok(AggFn::Sum),
        "min" => Ok(AggFn::Min),
        "max" => Ok(AggFn::Max),
        "count" => Ok(AggFn::Count),
        other => Err(module_err(format!(
            "{module}: unknown agg {other:?}; expected one of: avg, sum, min, max, count"
        ))),
    }
}

#[repr(C)]
pub(crate) struct AggregateTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for AggregateTab {
    type Aux = ();
    type Cursor = AggregateCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(series_id INTEGER, labels TEXT, value, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, start HIDDEN, \
                            stop HIDDEN, agg HIDDEN)",
            ),
            AggregateTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args_with_series_id(
            info,
            AGGREGATE_FIRST_ARG,
            AGGREGATE_ARGS.len() as c_int,
            Some(0),
        )
    }

    fn open(&mut self) -> Result<AggregateCursor<'vtab>> {
        Ok(AggregateCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct AggregateCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<(i64, Labels, AggregateValue)>,
    pos: usize,
    phantom: PhantomData<&'vtab AggregateTab>,
}

unsafe impl VTabCursor for AggregateCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_aggregate";
        let slots = named_slots(M, AGGREGATE_ARGS, AGGREGATE_REQUIRED, idx_num)?;
        let text = |i: usize, what: &str| -> Result<String> {
            let v: Option<String> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let int = |i: usize, what: &str| -> Result<i64> {
            let v: Option<i64> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };

        let (database, table) = split_spec(&text(0, "tbl")?);
        let metric = text(1, "metric")?;
        let filter_text: Option<String> = match slots[2] {
            None => None,
            Some(slot) => args.get(slot)?,
        };
        let (eq, matchers) = match filter_text.as_deref() {
            None | Some("") => (Labels::new(), Vec::new()),
            Some(txt) => compile_filter(M, txt)?,
        };
        let (start, stop) = (int(3, "start")?, int(4, "stop")?);
        let selection = decode_series_selection(idx_num, AGGREGATE_ARGS.len(), args)?;
        let agg = parse_scalar_aggregate(M, &text(5, "agg")?)?;
        if start > stop {
            self.rows.clear();
            self.pos = 0;
            return Ok(());
        }

        let _bind = DbGuard::bind(self.db);
        let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
        let _read = read_permit(&shared, self.db, &table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;
        let candidates = metric_candidates(&shared.engine, &metric, &eq, &matchers, selection);
        let series_ids: Vec<i64> = candidates.iter().map(|(series_id, _)| *series_id).collect();
        let batch = shared
            .engine
            .query_aggregate_summary_batch_by_id(&series_ids, start, stop)
            .map_err(module_err)?;
        let mut rows = Vec::new();
        for ((sid, labels), (result_sid, summary)) in candidates.into_iter().zip(batch) {
            debug_assert_eq!(sid, result_sid);
            let Some(summary) = summary else {
                continue;
            };
            let value =
                if agg == AggFn::Count {
                    AggregateValue::Integer(i64::try_from(summary.count()).map_err(|_| {
                        module_err(format!("{M}: count exceeds SQLite INTEGER range"))
                    })?)
                } else {
                    AggregateValue::Real(summary.value(agg))
                };
            rows.push((sid, labels, value));
        }
        rows.sort_by(|a, b| (&a.1, a.0).cmp(&(&b.1, b.0)));
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
        let (sid, labels, value) = &self.rows[self.pos];
        match col {
            0 => ctx.set_result(sid),
            1 => {
                let json = labels_to_json(labels);
                ctx.set_result(&json)
            }
            2 => match value {
                AggregateValue::Real(value) => ctx.set_result(value),
                AggregateValue::Integer(value) => ctx.set_result(value),
            },
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

// ---------------------------------------------------------------------------
// timeless_aggregate_frame — one packed scalar value per non-empty series
// ---------------------------------------------------------------------------

#[repr(C)]
pub(crate) struct AggregateFrameTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for AggregateFrameTab {
    type Aux = ();
    type Cursor = AggregateFrameCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(frame BLOB, tbl HIDDEN, metric HIDDEN, filter HIDDEN, \
                            start HIDDEN, stop HIDDEN, agg HIDDEN)",
            ),
            AggregateFrameTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, 1, AGGREGATE_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<AggregateFrameCursor<'vtab>> {
        Ok(AggregateFrameCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct AggregateFrameCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<Vec<u8>>,
    pos: usize,
    phantom: PhantomData<&'vtab AggregateFrameTab>,
}

unsafe impl VTabCursor for AggregateFrameCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_aggregate_frame";
        let slots = named_slots(M, AGGREGATE_ARGS, AGGREGATE_REQUIRED, idx_num)?;
        let text = |index: usize, what: &str| -> Result<String> {
            let value: Option<String> = args.get(slots[index].unwrap())?;
            value.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let integer = |index: usize, what: &str| -> Result<i64> {
            let value: Option<i64> = args.get(slots[index].unwrap())?;
            value.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let (database, table) = split_spec(&text(0, "tbl")?);
        let metric = text(1, "metric")?;
        let filter_text: Option<String> = match slots[2] {
            None => None,
            Some(slot) => args.get(slot)?,
        };
        let (eq, matchers) = match filter_text.as_deref() {
            None | Some("") => (Labels::new(), Vec::new()),
            Some(filter) => compile_filter(M, filter)?,
        };
        let (start, stop) = (integer(3, "start")?, integer(4, "stop")?);
        let aggregate = parse_scalar_aggregate(M, &text(5, "agg")?)?;
        if start > stop {
            self.rows.clear();
            self.pos = 0;
            return Ok(());
        }

        let _bind = DbGuard::bind(self.db);
        let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
        let _read = read_permit(&shared, self.db, &table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;
        let series_ids = metric_candidate_ids(&shared.engine, &metric, &eq, &matchers);
        let batch = shared
            .engine
            .query_aggregate_summary_batch_by_id(&series_ids, start, stop)
            .map_err(module_err)?;
        let frame = encode_aggregate_frame(&batch, aggregate).map_err(module_err)?;
        self.rows = if frame.is_empty() {
            Vec::new()
        } else {
            vec![frame]
        };
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

// ---------------------------------------------------------------------------
// timeless_latest — one newest point per matched series
// ---------------------------------------------------------------------------

const LATEST_ARGS: &[&str] = &["tbl", "metric", "filter", "start", "stop"];
const LATEST_REQUIRED: c_int = 0b1_1011; // all except filter
const LATEST_FIRST_ARG: c_int = 4;

#[repr(C)]
pub(crate) struct LatestTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for LatestTab {
    type Aux = ();
    type Cursor = LatestCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(series_id INTEGER, labels TEXT, ts INTEGER, value REAL, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, start HIDDEN, stop HIDDEN)",
            ),
            LatestTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args_with_series_id(info, LATEST_FIRST_ARG, LATEST_ARGS.len() as c_int, Some(0))
    }

    fn open(&mut self) -> Result<LatestCursor<'vtab>> {
        Ok(LatestCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct LatestCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<(i64, Labels, i64, f64)>,
    pos: usize,
    phantom: PhantomData<&'vtab LatestTab>,
}

unsafe impl VTabCursor for LatestCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_latest";
        let slots = named_slots(M, LATEST_ARGS, LATEST_REQUIRED, idx_num)?;
        let text = |i: usize, what: &str| -> Result<String> {
            let v: Option<String> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let int = |i: usize, what: &str| -> Result<i64> {
            let v: Option<i64> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };

        let (database, table) = split_spec(&text(0, "tbl")?);
        let metric = text(1, "metric")?;
        let filter_text: Option<String> = match slots[2] {
            None => None,
            Some(slot) => args.get(slot)?,
        };
        let (eq, matchers) = match filter_text.as_deref() {
            None | Some("") => (Labels::new(), Vec::new()),
            Some(txt) => compile_filter(M, txt)?,
        };
        let (start, stop) = (int(3, "start")?, int(4, "stop")?);
        let selection = decode_series_selection(idx_num, LATEST_ARGS.len(), args)?;
        if start > stop {
            self.rows.clear();
            self.pos = 0;
            return Ok(());
        }

        let _bind = DbGuard::bind(self.db);
        let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
        let _read = read_permit(&shared, self.db, &table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;
        let candidates = metric_candidates(&shared.engine, &metric, &eq, &matchers, selection);
        let series_ids: Vec<i64> = candidates.iter().map(|(series_id, _)| *series_id).collect();
        let batch = shared
            .engine
            .query_latest_batch_by_id(&series_ids, start, stop)
            .map_err(module_err)?;
        let mut rows = Vec::new();
        for ((sid, labels), (result_sid, point)) in candidates.into_iter().zip(batch) {
            debug_assert_eq!(sid, result_sid);
            let Some((ts, value)) = point else {
                continue;
            };
            rows.push((sid, labels, ts, value));
        }
        rows.sort_by(|a, b| (&a.1, a.0).cmp(&(&b.1, b.0)));
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
        let (sid, labels, ts, value) = &self.rows[self.pos];
        match col {
            0 => ctx.set_result(sid),
            1 => {
                let json = labels_to_json(labels);
                ctx.set_result(&json)
            }
            2 => ctx.set_result(ts),
            3 => ctx.set_result(value),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

// ---------------------------------------------------------------------------
// timeless_latest_frame — one packed newest point per non-empty series
// ---------------------------------------------------------------------------

#[repr(C)]
pub(crate) struct LatestFrameTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for LatestFrameTab {
    type Aux = ();
    type Cursor = LatestFrameCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(frame BLOB, tbl HIDDEN, metric HIDDEN, filter HIDDEN, \
                            start HIDDEN, stop HIDDEN)",
            ),
            LatestFrameTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, 1, LATEST_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<LatestFrameCursor<'vtab>> {
        Ok(LatestFrameCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct LatestFrameCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<Vec<u8>>,
    pos: usize,
    phantom: PhantomData<&'vtab LatestFrameTab>,
}

unsafe impl VTabCursor for LatestFrameCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_latest_frame";
        let slots = named_slots(M, LATEST_ARGS, LATEST_REQUIRED, idx_num)?;
        let text = |index: usize, what: &str| -> Result<String> {
            let value: Option<String> = args.get(slots[index].unwrap())?;
            value.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let integer = |index: usize, what: &str| -> Result<i64> {
            let value: Option<i64> = args.get(slots[index].unwrap())?;
            value.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let (database, table) = split_spec(&text(0, "tbl")?);
        let metric = text(1, "metric")?;
        let filter_text: Option<String> = match slots[2] {
            None => None,
            Some(slot) => args.get(slot)?,
        };
        let (eq, matchers) = match filter_text.as_deref() {
            None | Some("") => (Labels::new(), Vec::new()),
            Some(filter) => compile_filter(M, filter)?,
        };
        let (start, stop) = (integer(3, "start")?, integer(4, "stop")?);
        if start > stop {
            self.rows.clear();
            self.pos = 0;
            return Ok(());
        }

        let _bind = DbGuard::bind(self.db);
        let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
        let _read = read_permit(&shared, self.db, &table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;
        let series_ids = metric_candidate_ids(&shared.engine, &metric, &eq, &matchers);
        let batch = shared
            .engine
            .query_latest_batch_by_id(&series_ids, start, stop)
            .map_err(module_err)?;
        let frame = encode_latest_frame(&batch).map_err(module_err)?;
        self.rows = if frame.is_empty() {
            Vec::new()
        } else {
            vec![frame]
        };
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

// ---------------------------------------------------------------------------
// timeless_raw — the matcher-aware Q1 narrow waist
// ---------------------------------------------------------------------------

const RAW_ARGS: &[&str] = &["tbl", "metric", "filter", "start", "stop"];
const RAW_REQUIRED: c_int = 0b1_1011; // all except filter
const RAW_FIRST_ARG: c_int = 4;
const RAW_FRAME_ARGS: &[&str] = &[
    "tbl",
    "metric",
    "filter",
    "start",
    "stop",
    "max_work_points",
];

#[repr(C)]
pub(crate) struct RawTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for RawTab {
    type Aux = ();
    type Cursor = RawCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(series_id INTEGER, labels TEXT, ts INTEGER, value REAL, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, start HIDDEN, stop HIDDEN)",
            ),
            RawTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args_with_series_id(info, RAW_FIRST_ARG, RAW_ARGS.len() as c_int, Some(0))
    }

    fn open(&mut self) -> Result<RawCursor<'vtab>> {
        Ok(RawCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct RawCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<(i64, String, i64, f64)>,
    pos: usize,
    phantom: PhantomData<&'vtab RawTab>,
}

unsafe impl VTabCursor for RawCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_raw";
        let slots = named_slots(M, RAW_ARGS, RAW_REQUIRED, idx_num)?;
        let text = |i: usize, what: &str| -> Result<String> {
            let v: Option<String> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let int = |i: usize, what: &str| -> Result<i64> {
            let v: Option<i64> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let (database, table) = split_spec(&text(0, "tbl")?);
        let metric = text(1, "metric")?;
        let filter_text: Option<String> = match slots[2] {
            None => None,
            Some(slot) => args.get(slot)?,
        };
        let (eq, matchers) = match filter_text.as_deref() {
            None | Some("") => (Labels::new(), Vec::new()),
            Some(txt) => compile_filter(M, txt)?,
        };
        let (start, stop) = (int(3, "start")?, int(4, "stop")?);
        let selection = decode_series_selection(idx_num, RAW_ARGS.len(), args)?;
        if start > stop {
            self.rows.clear();
            self.pos = 0;
            return Ok(());
        }

        let _bind = DbGuard::bind(self.db);
        let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
        let _read = read_permit(&shared, self.db, &table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;
        let candidates = metric_candidates(&shared.engine, &metric, &eq, &matchers, selection);

        let mut rows = Vec::new();
        for (sid, labels) in candidates {
            let labels_json = labels_to_json(&labels);
            for (ts, value) in shared
                .engine
                .query_range_by_id(sid, start, stop)
                .map_err(module_err)?
            {
                rows.push((sid, labels_json.clone(), ts, value));
            }
        }
        rows.sort_by(|a, b| (&a.1, a.2, a.0).cmp(&(&b.1, b.2, b.0)));
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
        let (sid, labels, ts, value) = &self.rows[self.pos];
        match col {
            0 => ctx.set_result(sid),
            1 => ctx.set_result(labels),
            2 => ctx.set_result(ts),
            3 => ctx.set_result(value),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

// ---------------------------------------------------------------------------
// timeless_raw_batches — one packed point blob per matched series
// ---------------------------------------------------------------------------

#[repr(C)]
pub(crate) struct RawBatchTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for RawBatchTab {
    type Aux = ();
    type Cursor = RawBatchCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(series_id INTEGER, labels TEXT, points BLOB, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, start HIDDEN, stop HIDDEN)",
            ),
            RawBatchTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args_with_series_id(info, 3, RAW_ARGS.len() as c_int, Some(0))
    }

    fn open(&mut self) -> Result<RawBatchCursor<'vtab>> {
        Ok(RawBatchCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct RawBatchCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<(i64, Labels, Vec<u8>)>,
    pos: usize,
    phantom: PhantomData<&'vtab RawBatchTab>,
}

unsafe impl VTabCursor for RawBatchCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_raw_batches";
        let slots = named_slots(M, RAW_ARGS, RAW_REQUIRED, idx_num)?;
        let text = |i: usize, what: &str| -> Result<String> {
            let v: Option<String> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let int = |i: usize, what: &str| -> Result<i64> {
            let v: Option<i64> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let (database, table) = split_spec(&text(0, "tbl")?);
        let metric = text(1, "metric")?;
        let filter_text: Option<String> = match slots[2] {
            None => None,
            Some(slot) => args.get(slot)?,
        };
        let (eq, matchers) = match filter_text.as_deref() {
            None | Some("") => (Labels::new(), Vec::new()),
            Some(txt) => compile_filter(M, txt)?,
        };
        let (start, stop) = (int(3, "start")?, int(4, "stop")?);
        let selection = decode_series_selection(idx_num, RAW_ARGS.len(), args)?;
        if start > stop {
            self.rows.clear();
            self.pos = 0;
            return Ok(());
        }

        let _bind = DbGuard::bind(self.db);
        let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
        let _read = read_permit(&shared, self.db, &table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;
        let candidates = metric_candidates(&shared.engine, &metric, &eq, &matchers, selection);

        let series_ids: Vec<i64> = candidates.iter().map(|(sid, _)| *sid).collect();
        let batch = shared
            .engine
            .query_range_batch_by_id(&series_ids, start, stop)
            .map_err(module_err)?;

        let mut rows = Vec::new();
        for ((sid, labels), (result_sid, points)) in candidates.into_iter().zip(batch) {
            debug_assert_eq!(sid, result_sid);
            if !points.is_empty() {
                rows.push((sid, labels, encode_raw_points(&points)?));
            }
        }
        rows.sort_by(|a, b| (&a.1, a.0).cmp(&(&b.1, b.0)));
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
        let (sid, labels, points) = &self.rows[self.pos];
        match col {
            0 => ctx.set_result(sid),
            1 => {
                let json = labels_to_json(labels);
                ctx.set_result(&json)
            }
            2 => ctx.set_result(points),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

fn encode_raw_points(points: &[(i64, f64)]) -> Result<Vec<u8>> {
    let n: u32 = points
        .len()
        .try_into()
        .map_err(|_| module_err("timeless_raw_batches: too many points for one series".into()))?;
    let capacity = 4usize
        .checked_add(points.len().checked_mul(16).ok_or_else(|| {
            module_err("timeless_raw_batches: point blob size overflows this host".into())
        })?)
        .ok_or_else(|| module_err("timeless_raw_batches: point blob size overflow".into()))?;
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(&n.to_le_bytes());
    for (ts, _) in points {
        out.extend_from_slice(&ts.to_le_bytes());
    }
    for (_, value) in points {
        out.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// timeless_raw_frame — one packed blob for every matched non-empty series
// ---------------------------------------------------------------------------

const RAW_FRAME_MAGIC: &[u8; 4] = b"TRF1";

#[repr(C)]
pub(crate) struct RawFrameTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for RawFrameTab {
    type Aux = ();
    type Cursor = RawFrameCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(frame BLOB, tbl HIDDEN, metric HIDDEN, filter HIDDEN, \
                            start HIDDEN, stop HIDDEN, max_work_points HIDDEN)",
            ),
            RawFrameTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, 1, RAW_FRAME_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<RawFrameCursor<'vtab>> {
        Ok(RawFrameCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct RawFrameCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<Vec<u8>>,
    pos: usize,
    phantom: PhantomData<&'vtab RawFrameTab>,
}

unsafe impl VTabCursor for RawFrameCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_raw_frame";
        let slots = named_slots(M, RAW_FRAME_ARGS, RAW_REQUIRED, idx_num)?;
        let text = |i: usize, what: &str| -> Result<String> {
            let v: Option<String> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let int = |i: usize, what: &str| -> Result<i64> {
            let v: Option<i64> = args.get(slots[i].unwrap())?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let (database, table) = split_spec(&text(0, "tbl")?);
        let metric = text(1, "metric")?;
        let filter_text: Option<String> = match slots[2] {
            None => None,
            Some(slot) => args.get(slot)?,
        };
        let (eq, matchers) = match filter_text.as_deref() {
            None | Some("") => (Labels::new(), Vec::new()),
            Some(txt) => compile_filter(M, txt)?,
        };
        let (start, stop) = (int(3, "start")?, int(4, "stop")?);
        let max_work_points = match slots[5] {
            None => None,
            Some(slot) => Some(positive_work_limit(M, args, slot)?),
        };
        if start > stop {
            self.rows.clear();
            self.pos = 0;
            return Ok(());
        }

        let _bind = DbGuard::bind(self.db);
        let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
        let _read = read_permit(&shared, self.db, &table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;
        let series_ids: Vec<i64> = {
            let reg = shared.engine.series_read();
            reg.find_series(&metric, &eq)
                .into_iter()
                .filter(|sid| {
                    reg.info_for(*sid)
                        .is_some_and(|info| matchers_pass(&info.labels, &matchers))
                })
                .collect()
        };

        let batch = match max_work_points {
            Some(limit) => {
                shared
                    .engine
                    .query_range_batch_by_id_limited(&series_ids, start, stop, limit)
            }
            None => shared
                .engine
                .query_range_batch_by_id(&series_ids, start, stop),
        }
        .map_err(module_err)?;
        let frame = encode_raw_frame(&batch)?;
        self.rows = if frame.is_empty() {
            Vec::new()
        } else {
            vec![frame]
        };
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

fn encode_raw_frame(batch: &[(i64, Vec<(i64, f64)>)]) -> Result<Vec<u8>> {
    let non_empty: Vec<_> = batch
        .iter()
        .filter(|(_, points)| !points.is_empty())
        .collect();
    if non_empty.is_empty() {
        return Ok(Vec::new());
    }

    let series_count: u32 = non_empty
        .len()
        .try_into()
        .map_err(|_| module_err("timeless_raw_frame: too many series".into()))?;
    let total_points = non_empty.iter().try_fold(0usize, |total, (_, points)| {
        let _: u32 = points
            .len()
            .try_into()
            .map_err(|_| module_err("timeless_raw_frame: too many points in one series".into()))?;
        total
            .checked_add(points.len())
            .ok_or_else(|| module_err("timeless_raw_frame: total point count overflow".into()))
    })?;
    let total_points_u64: u64 = total_points
        .try_into()
        .map_err(|_| module_err("timeless_raw_frame: total point count overflow".into()))?;
    let series_bytes = non_empty
        .len()
        .checked_mul(12)
        .ok_or_else(|| module_err("timeless_raw_frame: series columns overflow".into()))?;
    let point_bytes = total_points
        .checked_mul(16)
        .ok_or_else(|| module_err("timeless_raw_frame: point columns overflow".into()))?;
    let capacity = 16usize
        .checked_add(series_bytes)
        .and_then(|size| size.checked_add(point_bytes))
        .ok_or_else(|| module_err("timeless_raw_frame: blob size overflow".into()))?;

    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(RAW_FRAME_MAGIC);
    out.extend_from_slice(&series_count.to_le_bytes());
    out.extend_from_slice(&total_points_u64.to_le_bytes());
    for (series_id, _) in &non_empty {
        out.extend_from_slice(&series_id.to_le_bytes());
    }
    for (_, points) in &non_empty {
        let count = u32::try_from(points.len()).expect("point count checked above");
        out.extend_from_slice(&count.to_le_bytes());
    }
    for (_, points) in &non_empty {
        for (timestamp, _) in points.iter() {
            out.extend_from_slice(&timestamp.to_le_bytes());
        }
    }
    for (_, points) in &non_empty {
        for (_, value) in points.iter() {
            out.extend_from_slice(&value.to_bits().to_le_bytes());
        }
    }
    debug_assert_eq!(out.len(), capacity);
    Ok(out)
}

#[cfg(test)]
mod raw_frame_tests {
    use super::*;

    #[test]
    fn frame_is_versioned_columnar_omits_empty_series_and_preserves_bits() {
        let nan = f64::from_bits(0x7ff8_0000_0000_0042);
        let frame = encode_raw_frame(&[
            (9, vec![(10, 1.5), (20, nan)]),
            (10, Vec::new()),
            (12, vec![(30, -2.25)]),
        ])
        .unwrap();

        assert_eq!(&frame[..4], b"TRF1");
        assert_eq!(u32::from_le_bytes(frame[4..8].try_into().unwrap()), 2);
        assert_eq!(u64::from_le_bytes(frame[8..16].try_into().unwrap()), 3);
        assert_eq!(i64::from_le_bytes(frame[16..24].try_into().unwrap()), 9);
        assert_eq!(i64::from_le_bytes(frame[24..32].try_into().unwrap()), 12);
        assert_eq!(u32::from_le_bytes(frame[32..36].try_into().unwrap()), 2);
        assert_eq!(u32::from_le_bytes(frame[36..40].try_into().unwrap()), 1);
        assert_eq!(i64::from_le_bytes(frame[40..48].try_into().unwrap()), 10);
        assert_eq!(i64::from_le_bytes(frame[48..56].try_into().unwrap()), 20);
        assert_eq!(i64::from_le_bytes(frame[56..64].try_into().unwrap()), 30);
        assert_eq!(
            u64::from_le_bytes(frame[64..72].try_into().unwrap()),
            1.5f64.to_bits()
        );
        assert_eq!(
            u64::from_le_bytes(frame[72..80].try_into().unwrap()),
            nan.to_bits()
        );
        assert_eq!(
            u64::from_le_bytes(frame[80..88].try_into().unwrap()),
            (-2.25f64).to_bits()
        );
        assert_eq!(frame.len(), 88);
    }

    #[test]
    fn frame_emits_no_row_payload_when_every_series_is_empty() {
        assert!(encode_raw_frame(&[(1, Vec::new())]).unwrap().is_empty());
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
    const SERIES_ID_COL: c_int;
    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<KernelRow>>;
}

#[repr(C)]
pub(crate) struct KernelCursor<'vtab, T: KernelVTab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<KernelRow>,
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
        let (series_id, labels, ts, value) = &self.rows[self.pos];
        match col {
            COL_LABELS => ctx.set_result(labels),
            1 => ctx.set_result(ts),
            2 => match value {
                Some(v) => ctx.set_result(v),
                None => ctx.set_result(&rusqlite::types::Null),
            },
            col if col == T::SERIES_ID_COL => ctx.set_result(series_id),
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
    "tbl",
    "metric",
    "filter",
    "resolution",
    "start",
    "stop",
    "agg",
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
            Cow::Borrowed(
                c"CREATE TABLE x(labels TEXT, ts INTEGER, value REAL, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, resolution HIDDEN, \
                            start HIDDEN, stop HIDDEN, agg HIDDEN, series_id HIDDEN)",
            ),
            RollupTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args_with_series_id(
            info,
            COL_FIRST_ARG,
            ROLLUP_ARGS.len() as c_int,
            Some(COL_FIRST_ARG + ROLLUP_ARGS.len() as c_int),
        )
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
    const SERIES_ID_COL: c_int = COL_FIRST_ARG + ROLLUP_ARGS.len() as c_int;

    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<KernelRow>> {
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
// timeless_rollup_batches — all rollup aggregates in one blob per series
// ---------------------------------------------------------------------------

const ROLLUP_BATCH_ARGS: &[&str] = &["tbl", "metric", "filter", "resolution", "start", "stop"];
const ROLLUP_BATCH_REQUIRED: c_int = 0b11_1011; // all except filter
const ROLLUP_BATCH_MAGIC: &[u8; 4] = b"TRB1";

#[repr(C)]
pub(crate) struct RollupBatchTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for RollupBatchTab {
    type Aux = ();
    type Cursor = RollupBatchCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(series_id INTEGER, labels TEXT, buckets BLOB, \
                            tbl HIDDEN, metric HIDDEN, filter HIDDEN, resolution HIDDEN, \
                            start HIDDEN, stop HIDDEN)",
            ),
            RollupBatchTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args_with_series_id(info, 3, ROLLUP_BATCH_ARGS.len() as c_int, Some(0))
    }

    fn open(&mut self) -> Result<RollupBatchCursor<'vtab>> {
        Ok(RollupBatchCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct RollupBatchCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<(i64, Labels, Vec<u8>)>,
    pos: usize,
    phantom: PhantomData<&'vtab RollupBatchTab>,
}

unsafe impl VTabCursor for RollupBatchCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_rollup_batches";
        let ka = decode_args(M, ROLLUP_BATCH_ARGS, ROLLUP_BATCH_REQUIRED, idx_num, args)?;

        let _bind = DbGuard::bind(self.db);
        let shared: Arc<SharedEngine<Engine>> =
            MetricsTab::shared_engine_for(self.db, &ka.database, &ka.table)?;
        let _read = read_permit(&shared, self.db, &ka.table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;

        let candidates = metric_candidates(
            &shared.engine,
            &ka.metric,
            &ka.filter,
            &ka.matchers,
            ka.series_selection,
        );
        let series_ids: Vec<i64> = candidates.iter().map(|(sid, _)| *sid).collect();
        let batch = shared
            .engine
            .query_rollup_batch_by_id(&series_ids, ka.width, ka.start, ka.stop)
            .map_err(module_err)?;

        let mut rows = Vec::new();
        for ((series_id, labels), (result_id, buckets)) in candidates.into_iter().zip(batch) {
            debug_assert_eq!(series_id, result_id);
            if buckets.is_empty() {
                continue;
            }
            rows.push((series_id, labels, encode_rollup_buckets(&buckets)?));
        }
        rows.sort_by(|a, b| (&a.1, a.0).cmp(&(&b.1, b.0)));
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
        let (series_id, labels, buckets) = &self.rows[self.pos];
        match col {
            0 => ctx.set_result(series_id),
            1 => {
                let json = labels_to_json(labels);
                ctx.set_result(&json)
            }
            2 => ctx.set_result(buckets),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

fn encode_rollup_buckets(buckets: &[timeless_core::RollupBucket]) -> Result<Vec<u8>> {
    let count = buckets.len();
    let n: u32 = count
        .try_into()
        .map_err(|_| module_err("timeless_rollup_batches: too many buckets".into()))?;
    let column_bytes = count
        .checked_mul(8)
        .ok_or_else(|| module_err("timeless_rollup_batches: column size overflow".into()))?;
    let capacity = 8usize
        .checked_add(
            column_bytes
                .checked_mul(8)
                .ok_or_else(|| module_err("timeless_rollup_batches: blob size overflow".into()))?,
        )
        .ok_or_else(|| module_err("timeless_rollup_batches: blob size overflow".into()))?;
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(ROLLUP_BATCH_MAGIC);
    out.extend_from_slice(&n.to_le_bytes());
    for bucket in buckets {
        out.extend_from_slice(&bucket.bucket_ts.to_le_bytes());
    }
    for bucket in buckets {
        out.extend_from_slice(&bucket.count.to_le_bytes());
    }
    for bucket in buckets {
        out.extend_from_slice(&(bucket.sum / bucket.count as f64).to_bits().to_le_bytes());
    }
    for bucket in buckets {
        out.extend_from_slice(&bucket.sum.to_bits().to_le_bytes());
    }
    for bucket in buckets {
        out.extend_from_slice(&bucket.min.to_bits().to_le_bytes());
    }
    for bucket in buckets {
        out.extend_from_slice(&bucket.max.to_bits().to_le_bytes());
    }
    for bucket in buckets {
        out.extend_from_slice(&bucket.last_ts.to_le_bytes());
    }
    for bucket in buckets {
        out.extend_from_slice(&bucket.last_val.to_bits().to_le_bytes());
    }
    Ok(out)
}

#[cfg(test)]
mod rollup_batch_tests {
    use super::*;
    use timeless_core::RollupBucket;

    #[test]
    fn blob_is_versioned_columnar_and_preserves_counts_and_float_bits() {
        let buckets = vec![
            RollupBucket {
                bucket_ts: -300,
                count: (1u64 << 53) + 7,
                sum: f64::from_bits(0x7ff8_0000_0000_0042),
                min: -0.0,
                max: f64::INFINITY,
                last_ts: -1,
                last_val: f64::from_bits(0x8000_0000_0000_0001),
            },
            RollupBucket {
                bucket_ts: 0,
                count: 2,
                sum: 3.0,
                min: 1.0,
                max: 2.0,
                last_ts: 9,
                last_val: 2.0,
            },
        ];
        let blob = encode_rollup_buckets(&buckets).unwrap();
        assert_eq!(&blob[..4], b"TRB1");
        assert_eq!(u32::from_le_bytes(blob[4..8].try_into().unwrap()), 2);
        let col = |index: usize| 8 + index * 16;
        let u64_at = |base: usize, index: usize| {
            u64::from_le_bytes(
                blob[base + index * 8..base + index * 8 + 8]
                    .try_into()
                    .unwrap(),
            )
        };
        assert_eq!(u64_at(col(0), 0) as i64, -300);
        assert_eq!(u64_at(col(1), 0), (1u64 << 53) + 7);
        assert_eq!(u64_at(col(3), 0), buckets[0].sum.to_bits());
        assert_eq!(u64_at(col(4), 0), buckets[0].min.to_bits());
        assert_eq!(u64_at(col(5), 0), buckets[0].max.to_bits());
        assert_eq!(u64_at(col(6), 0) as i64, -1);
        assert_eq!(u64_at(col(7), 0), buckets[0].last_val.to_bits());
        assert_eq!(blob.len(), 8 + 2 * 8 * 8);
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

/// Metadata-native trace discovery. These public TVFs avoid decoding every
/// span merely to enumerate the low-cardinality service/operation catalog:
///
///   SELECT value FROM timeless_trace_services('traces');
///   SELECT value FROM timeless_trace_operations('traces', 'checkout');
#[derive(Clone, Copy)]
enum TraceDiscoveryKind {
    Services,
    Operations,
}

#[repr(C)]
pub(crate) struct TraceDiscoveryTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
    kind: TraceDiscoveryKind,
}

unsafe impl<'vtab> VTab<'vtab> for TraceDiscoveryTab {
    type Aux = ();
    type Cursor = TraceDiscoveryCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        _args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let handle = unsafe { db.handle() };
        db.config(VTabConfig::Innocuous)?;
        let kind = if module_name == b"timeless_trace_operations" {
            TraceDiscoveryKind::Operations
        } else {
            TraceDiscoveryKind::Services
        };
        let schema = match kind {
            TraceDiscoveryKind::Services => c"CREATE TABLE x(value TEXT, tbl HIDDEN)",
            TraceDiscoveryKind::Operations => {
                c"CREATE TABLE x(value TEXT, tbl HIDDEN, service HIDDEN)"
            }
        };
        Ok((
            Cow::Borrowed(schema),
            TraceDiscoveryTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
                kind,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        let count = match self.kind {
            TraceDiscoveryKind::Services => 1,
            TraceDiscoveryKind::Operations => 2,
        };
        best_index_args(info, 1, count)
    }

    fn open(&mut self) -> Result<Self::Cursor> {
        Ok(TraceDiscoveryCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            kind: self.kind,
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct TraceDiscoveryCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    kind: TraceDiscoveryKind,
    rows: Vec<String>,
    pos: usize,
    phantom: PhantomData<&'vtab TraceDiscoveryTab>,
}

unsafe impl VTabCursor for TraceDiscoveryCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        let (module, names, required) = match self.kind {
            TraceDiscoveryKind::Services => ("timeless_trace_services", &["tbl"][..], 0b1),
            TraceDiscoveryKind::Operations => {
                ("timeless_trace_operations", &["tbl", "service"][..], 0b11)
            }
        };
        let slots = named_slots(module, names, required, idx_num)?;
        let table_spec: Option<String> = args.get(slots[0].unwrap())?;
        let table_spec =
            table_spec.ok_or_else(|| module_err(format!("{module}: tbl must not be NULL")))?;
        let service = if matches!(self.kind, TraceDiscoveryKind::Operations) {
            let value: Option<String> = args.get(slots[1].unwrap())?;
            Some(value.ok_or_else(|| module_err(format!("{module}: service must not be NULL")))?)
        } else {
            None
        };
        let (database, table) = split_spec(&table_spec);
        let _bind = DbGuard::bind(self.db);
        let shared = TracesTab::shared_engine_for(self.db, &database, &table)?;
        let _read = read_permit(&shared, self.db, &table)?;
        self.rows = match service {
            Some(service) => shared
                .engine
                .discover_operations(&service)
                .map_err(module_err)?,
            None => shared.engine.discover_services().map_err(module_err)?,
        };
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

/// timeless_log_count('logs', filter_json, message_contains, start, stop,
///                    max_work_entries)
/// → one INTEGER row. Only `tbl` is required; optional bounds default to the
/// full i64 range. Filter JSON uses `level` plus metadata equalities, matching
/// timeless_log_buckets.
const LOG_COUNT_ARGS: &[&str] = &[
    "tbl",
    "filter",
    "message_contains",
    "start",
    "stop",
    "max_work_entries",
];
const LOG_COUNT_REQUIRED: c_int = 0b00001;
const LOG_COUNT_FIRST_ARG: c_int = 1;

#[repr(C)]
pub(crate) struct LogCountTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for LogCountTab {
    type Aux = ();
    type Cursor = LogCountCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(n INTEGER, tbl HIDDEN, filter HIDDEN, \
                                 message_contains HIDDEN, start HIDDEN, stop HIDDEN, \
                                 max_work_entries HIDDEN)",
            ),
            LogCountTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, LOG_COUNT_FIRST_ARG, LOG_COUNT_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<LogCountCursor<'vtab>> {
        Ok(LogCountCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            value: 0,
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct LogCountCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    value: i64,
    pos: usize,
    phantom: PhantomData<&'vtab LogCountTab>,
}

unsafe impl VTabCursor for LogCountCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_log_count";
        let slots = named_slots(M, LOG_COUNT_ARGS, LOG_COUNT_REQUIRED, idx_num)?;
        let table_spec: Option<String> = args.get(slots[0].unwrap())?;
        let table_spec =
            table_spec.ok_or_else(|| module_err(format!("{M}: tbl must not be NULL")))?;
        let (database, table) = split_spec(&table_spec);
        let filter_json: Option<String> = match slots[1] {
            Some(slot) => args.get(slot)?,
            None => None,
        };
        let message_contains: Option<String> = match slots[2] {
            Some(slot) => args.get(slot)?,
            None => None,
        };
        let ts_min: i64 = match slots[3] {
            Some(slot) => args.get::<Option<i64>>(slot)?.unwrap_or(i64::MIN),
            None => i64::MIN,
        };
        let ts_max: i64 = match slots[4] {
            Some(slot) => args.get::<Option<i64>>(slot)?.unwrap_or(i64::MAX),
            None => i64::MAX,
        };
        let max_work_entries = optional_positive_usize(M, "max_work_entries", slots[5], args)?;

        let mut level = None;
        let mut severity = None;
        let mut metadata_eq = Vec::new();
        if let Some(text) = filter_json.filter(|text| !text.is_empty()) {
            for (key, value) in parse_labels_json(&text)
                .map_err(|error| module_err(format!("{M}: filter: {error}")))?
            {
                if key == "level" {
                    let canonical =
                        timeless_core::canonical_severity(&value).map_err(module_err)?;
                    level = Some(timeless_core::level_from_name(canonical).map_err(module_err)?);
                    severity = Some(canonical.to_owned());
                } else {
                    metadata_eq.push((key, value));
                }
            }
        }

        let _bind = DbGuard::bind(self.db);
        let shared = LogsTab::shared_engine_for(self.db, &database, &table)?;
        let read = read_permit(&shared, self.db, &table)?;
        let count = shared
            .engine
            .count_with_work_limit_after_snapshot(
                &LogQuery {
                    ts_min,
                    ts_max,
                    level,
                    severity,
                    metadata_eq,
                    message_contains,
                    message_like_prune: None,
                },
                max_work_entries,
                move || drop(read),
            )
            .map_err(module_err)?;
        self.value = i64::try_from(count)
            .map_err(|_| module_err(format!("{M}: count exceeds SQLite INTEGER range")))?;
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos > 0
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        match col {
            0 => ctx.set_result(&self.value),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(0)
    }
}

/// timeless_log_values('logs', key, filter_json, message_contains,
///                     start, stop, limit, max_work_entries)
/// → bounded distinct TEXT rows in lexical order. `tbl` and `key` are
/// required. The remaining arguments share timeless_log_count semantics;
/// limit defaults to 1,000 and is capped at 100,000.
const LOG_VALUES_ARGS: &[&str] = &[
    "tbl",
    "key",
    "filter",
    "message_contains",
    "start",
    "stop",
    "max_values",
    "max_work_entries",
];
const LOG_VALUES_REQUIRED: c_int = 0b0000011;
const LOG_VALUES_FIRST_ARG: c_int = 1;

fn optional_positive_usize(
    module: &str,
    name: &str,
    slot: Option<usize>,
    args: &Filters<'_>,
) -> Result<Option<usize>> {
    let Some(slot) = slot else {
        return Ok(None);
    };
    let value: Option<i64> = args.get(slot)?;
    let value = value.ok_or_else(|| {
        module_err(format!(
            "{module}: {name} must not be NULL and must be positive"
        ))
    })?;
    if value <= 0 {
        return Err(module_err(format!("{module}: {name} must be positive")));
    }
    usize::try_from(value).map(Some).map_err(|_| {
        module_err(format!(
            "{module}: {name} {value} exceeds this platform's usize"
        ))
    })
}

#[repr(C)]
pub(crate) struct LogValuesTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

unsafe impl<'vtab> VTab<'vtab> for LogValuesTab {
    type Aux = ();
    type Cursor = LogValuesCursor<'vtab>;

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
            Cow::Borrowed(
                c"CREATE TABLE x(value TEXT, tbl HIDDEN, key HIDDEN, filter HIDDEN, \
                                  message_contains HIDDEN, start HIDDEN, stop HIDDEN, \
                                  max_values HIDDEN, max_work_entries HIDDEN)",
            ),
            LogValuesTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, LOG_VALUES_FIRST_ARG, LOG_VALUES_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<LogValuesCursor<'vtab>> {
        Ok(LogValuesCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            values: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

#[repr(C)]
pub(crate) struct LogValuesCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    values: Vec<String>,
    pos: usize,
    phantom: PhantomData<&'vtab LogValuesTab>,
}

unsafe impl VTabCursor for LogValuesCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        const M: &str = "timeless_log_values";
        let slots = named_slots(M, LOG_VALUES_ARGS, LOG_VALUES_REQUIRED, idx_num)?;
        let required_text = |slot: Option<usize>, name: &str| -> Result<String> {
            let value: Option<String> = args.get(slot.expect("required slot validated"))?;
            value
                .filter(|value| !value.is_empty())
                .ok_or_else(|| module_err(format!("{M}: {name} must not be NULL or empty")))
        };
        let table_spec = required_text(slots[0], "tbl")?;
        let key = required_text(slots[1], "key")?;
        let (database, table) = split_spec(&table_spec);
        let filter_json: Option<String> = match slots[2] {
            Some(slot) => args.get(slot)?,
            None => None,
        };
        let message_contains: Option<String> = match slots[3] {
            Some(slot) => args.get(slot)?,
            None => None,
        };
        let ts_min = match slots[4] {
            Some(slot) => args.get::<Option<i64>>(slot)?.unwrap_or(i64::MIN),
            None => i64::MIN,
        };
        let ts_max = match slots[5] {
            Some(slot) => args.get::<Option<i64>>(slot)?.unwrap_or(i64::MAX),
            None => i64::MAX,
        };
        let limit = match slots[6] {
            Some(slot) => args.get::<Option<i64>>(slot)?.unwrap_or(1_000),
            None => 1_000,
        };
        if !(0..=100_000).contains(&limit) {
            return Err(module_err(format!(
                "{M}: limit must be between 0 and 100000"
            )));
        }
        let max_work_entries = optional_positive_usize(M, "max_work_entries", slots[7], args)?;

        let mut level = None;
        let mut severity = None;
        let mut metadata_eq = Vec::new();
        if let Some(text) = filter_json.filter(|text| !text.is_empty()) {
            for (filter_key, value) in parse_labels_json(&text)
                .map_err(|error| module_err(format!("{M}: filter: {error}")))?
            {
                if filter_key == "level" {
                    let canonical =
                        timeless_core::canonical_severity(&value).map_err(module_err)?;
                    level = Some(timeless_core::level_from_name(canonical).map_err(module_err)?);
                    severity = Some(canonical.to_owned());
                } else {
                    metadata_eq.push((filter_key, value));
                }
            }
        }

        let _bind = DbGuard::bind(self.db);
        let shared = LogsTab::shared_engine_for(self.db, &database, &table)?;
        let read = read_permit(&shared, self.db, &table)?;
        self.values = shared
            .engine
            .field_values_with_work_limit_after_snapshot(
                &LogQuery {
                    ts_min,
                    ts_max,
                    level,
                    severity,
                    metadata_eq,
                    message_contains,
                    message_like_prune: None,
                },
                &key,
                limit as usize,
                max_work_entries,
                move || drop(read),
            )
            .map_err(module_err)?;
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.values.len()
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        match col {
            0 => ctx.set_result(&self.values[self.pos]),
            _ => ctx.set_result(&rusqlite::types::Null),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.pos as i64)
    }
}

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
            Cow::Borrowed(
                c"CREATE TABLE x(bucket_ts INTEGER, group_key TEXT, n INTEGER, \
                            tbl HIDDEN, group_by HIDDEN, filter HIDDEN, start HIDDEN, \
                            stop HIDDEN, step HIDDEN)",
            ),
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
        let mut severity = None;
        let mut metadata_eq: Vec<(String, String)> = Vec::new();
        if let Some(txt) = filter_json.filter(|t| !t.is_empty()) {
            for (k, v) in
                parse_labels_json(&txt).map_err(|e| module_err(format!("{M}: filter: {e}")))?
            {
                if k == "level" {
                    let canonical = timeless_core::canonical_severity(&v).map_err(module_err)?;
                    level = Some(timeless_core::level_from_name(canonical).map_err(module_err)?);
                    severity = Some(canonical.to_owned());
                } else {
                    metadata_eq.push((k, v));
                }
            }
        }

        let _bind = DbGuard::bind(self.db);
        let shared = LogsTab::shared_engine_for(self.db, &database, &table)?;
        let _read = read_permit(&shared, self.db, &table)?;
        let q = timeless_core::LogQuery {
            ts_min: start,
            ts_max: stop,
            level,
            severity,
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
            Cow::Borrowed(
                c"CREATE TABLE x(bucket_ts INTEGER, service TEXT, spans INTEGER, \
                            errors INTEGER, dur_sum INTEGER, dur_min INTEGER, dur_max INTEGER, \
                            dur_p50 INTEGER, dur_p95 INTEGER, dur_p99 INTEGER, \
                            tbl HIDDEN, service_filter HIDDEN, start HIDDEN, stop HIDDEN, \
                            step HIDDEN)",
            ),
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
        let read = read_permit(&shared, self.db, &table)?;
        let q = timeless_core::SpanQuery {
            ts_min: start,
            ts_max: stop,
            trace_id: None,
            service,
            kind: None,
            status: None,
            name: None,
        };
        self.rows = shared
            .engine
            .bucket_stats_after_snapshot(&q, step, move || drop(read))
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
        .query_map(rusqlite::params![chunks, trace_blocks, blocks], |r| {
            r.get(0)
        })?
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

/// timeless_series('metrics' [, metric [, filter]]) — the series catalog,
/// from the in-memory registry + chunk index only (no chunk decompression).
#[repr(C)]
pub(crate) struct SeriesTab {
    base: ffi::sqlite3_vtab,
    db: *mut ffi::sqlite3,
}

const SERIES_FIRST_ARG: c_int = 8;
const SERIES_ARGS: &[&str] = &["tbl", "metric", "filter"];
const SERIES_REQUIRED: c_int = 0b001;

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
            Cow::Borrowed(
                c"CREATE TABLE x(name TEXT, labels TEXT, series_id INTEGER, \
                            min_ts INTEGER, max_ts INTEGER, points INTEGER, \
                            chunks INTEGER, buffered INTEGER, tbl HIDDEN, \
                            metric HIDDEN, filter HIDDEN)",
            ),
            SeriesTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args_with_series_id(info, SERIES_FIRST_ARG, SERIES_ARGS.len() as c_int, Some(2))
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
        const M: &str = "timeless_series";
        let slots = named_slots(M, SERIES_ARGS, SERIES_REQUIRED, idx_num)?;
        let get = |slot: usize, what: &str| -> Result<String> {
            let value: Option<String> = args.get(slot)?;
            value.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let (database, table) = split_spec(&get(slots[0].unwrap(), "tbl")?);
        let metric: Option<String> = match slots[1] {
            Some(slot) => args.get(slot)?,
            None => None,
        };
        let filter_text: Option<String> = match slots[2] {
            Some(slot) => args.get(slot)?,
            None => None,
        };
        if metric.is_none() && filter_text.as_deref().is_some_and(|text| !text.is_empty()) {
            return Err(module_err(format!(
                "{M}: filter requires metric — call as {M}(tbl, metric, filter)"
            )));
        }
        let (eq, matchers) = match filter_text.as_deref() {
            None | Some("") => (Labels::new(), Vec::new()),
            Some(text) => compile_filter(M, text)?,
        };
        let selection = decode_series_selection(idx_num, SERIES_ARGS.len(), args)?;
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
        let _read = read_permit(&shared, self.db, &table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;
        self.rows = match (metric, selection) {
            (_, SeriesSelection::Empty) => Vec::new(),
            (metric, SeriesSelection::Id(series_id)) => {
                let matches = {
                    let reg = shared.engine.series_read();
                    reg.info_for(series_id).is_some_and(|info| {
                        metric
                            .as_deref()
                            .is_none_or(|metric| info.metric_name == metric)
                            && eq
                                .iter()
                                .all(|(key, value)| info.labels.get(key) == Some(value))
                            && matchers_pass(&info.labels, &matchers)
                    })
                };
                if matches {
                    shared.engine.series_overview_by_ids(&[series_id])
                } else {
                    Vec::new()
                }
            }
            (Some(metric), SeriesSelection::All) => {
                let series_ids: Vec<i64> = {
                    let reg = shared.engine.series_read();
                    reg.find_series(&metric, &eq)
                        .into_iter()
                        .filter(|series_id| {
                            reg.info_for(*series_id)
                                .is_some_and(|info| matchers_pass(&info.labels, &matchers))
                        })
                        .collect()
                };
                shared.engine.series_overview_by_ids(&series_ids)
            }
            (None, SeriesSelection::All) => shared.engine.series_overview(),
        };
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
const LABEL_VALUES_ARGS: &[&str] = &["tbl", "metric", "key", "filter"];
const LABEL_VALUES_REQUIRED: c_int = 0b0111;

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
            Cow::Borrowed(
                c"CREATE TABLE x(value TEXT, tbl HIDDEN, metric HIDDEN, key HIDDEN, \
                                 filter HIDDEN)",
            ),
            LabelValuesTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(
            info,
            LABEL_VALUES_FIRST_ARG,
            LABEL_VALUES_ARGS.len() as c_int,
        )
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
        let slots = named_slots(M, LABEL_VALUES_ARGS, LABEL_VALUES_REQUIRED, idx_num)?;
        let get = |s: usize, what: &str| -> Result<String> {
            let v: Option<String> = args.get(s)?;
            v.ok_or_else(|| module_err(format!("{M}: {what} must not be NULL")))
        };
        let (database, table) = split_spec(&get(slots[0].unwrap(), "tbl")?);
        let metric = get(slots[1].unwrap(), "metric")?;
        let key = get(slots[2].unwrap(), "key")?;
        let filter_text: Option<String> = match slots[3] {
            Some(slot) => args.get(slot)?,
            None => None,
        };

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
        let _read = read_permit(&shared, self.db, &table)?;
        shared
            .engine
            .refresh_authoritative_state()
            .map_err(module_err)?;
        self.rows = match filter_text.as_deref() {
            None | Some("") => shared.engine.series_read().label_values(&metric, &key),
            Some(text) => {
                let (eq, matchers) = compile_filter(M, text)?;
                let reg = shared.engine.series_read();
                let mut values = HashSet::new();
                for series_id in reg.find_series(&metric, &eq) {
                    let Some(info) = reg.info_for(series_id) else {
                        continue;
                    };
                    if matchers_pass(&info.labels, &matchers) {
                        if let Some(value) = info.labels.get(&key) {
                            values.insert(value.clone());
                        }
                    }
                }
                let mut values: Vec<String> = values.into_iter().collect();
                values.sort();
                values
            }
        };
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
    conn.query_row(&sql, [], |r| r.get(0))
}

fn count_rows(database: &str, table: &str, suffix: &str) -> Result<i64> {
    let conn = shared::current_conn().map_err(module_err)?;
    let sql = format!(
        "SELECT COUNT(*) FROM {}",
        crate::sql_ident::qualified_shadow(database, table, suffix)
    );
    conn.query_row(&sql, [], |r| r.get(0))
}

struct LogStorageSummary {
    disk_entries: i64,
    bytes_on_disk: i64,
    raw_bytes: i64,
    optimize_source_entries: i64,
    optimize_source_bytes: i64,
}

fn log_storage_summary(database: &str, table: &str) -> Result<LogStorageSummary> {
    let conn = shared::current_conn().map_err(module_err)?;
    let blocks = crate::sql_ident::qualified_shadow(database, table, "blocks");
    let sql = format!(
        "SELECT COALESCE(SUM(entry_count), 0),
                COALESCE(SUM(length(data)), 0),
                COALESCE(SUM(CASE WHEN codec IN (1, 6) THEN length(data) ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN codec IN (1, 6) OR entry_count < {LOG_MERGE_TARGET_ENTRIES}
                                  THEN entry_count ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN codec IN (1, 6) OR entry_count < {LOG_MERGE_TARGET_ENTRIES}
                                  THEN length(data) ELSE 0 END), 0)
           FROM {blocks}"
    );
    conn.query_row(&sql, [], |row| {
        Ok(LogStorageSummary {
            disk_entries: row.get(0)?,
            bytes_on_disk: row.get(1)?,
            raw_bytes: row.get(2)?,
            optimize_source_entries: row.get(3)?,
            optimize_source_bytes: row.get(4)?,
        })
    })
}

fn log_index_bytes(database: &str, table: &str) -> Option<i64> {
    let conn = shared::current_conn().ok()?;
    let dbstat = crate::sql_ident::qualified(database, "dbstat");
    let sql = format!(
        "SELECT COALESCE(SUM(pgsize), 0) FROM {dbstat}
          WHERE name IN (?1, ?2, ?3, ?4)"
    );
    conn.query_row(
        &sql,
        [
            crate::sql_ident::shadow_object(table, "terms"),
            crate::sql_ident::shadow_object(table, "blocks_ts"),
            crate::sql_ident::shadow_object(table, "meta"),
            format!(
                "sqlite_autoindex_{}_1",
                crate::sql_ident::shadow_object(table, "meta")
            ),
        ],
        |row| row.get(0),
    )
    .ok()
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
            let retention =
                crate::shadow_meta::load_retention(&conn, &database, &table).map_err(module_err)?;
            rows.push(("retention", opt_ts(retention)));
        }
        match module {
            TimelessModule::Metrics => {
                let shared = MetricsTab::shared_engine_for(self.db, &database, &table)?;
                let _read = read_permit(&shared, self.db, &table)?;
                shared
                    .engine
                    .refresh_authoritative_state()
                    .map_err(module_err)?;
                let info = shared.engine.info();
                rows.extend([
                    ("series", Value::Integer(info.series_count as i64)),
                    ("chunks", Value::Integer(info.chunk_count as i64)),
                    ("disk_points", Value::Integer(info.disk_points as i64)),
                    (
                        "buffered_points",
                        Value::Integer(info.buffered_points as i64),
                    ),
                    ("bytes_on_disk", Value::Integer(info.total_bytes as i64)),
                    ("bytes_per_point", Value::Real(info.bytes_per_point)),
                    ("buffer_memory", Value::Integer(info.buffer_memory as i64)),
                    ("ts_min", opt_ts(info.oldest_ts)),
                    ("ts_max", opt_ts(info.newest_ts)),
                    (
                        "prometheus_ingest_batches",
                        Value::Integer(info.prometheus_ingest_batches as i64),
                    ),
                    (
                        "prometheus_ingest_points",
                        Value::Integer(info.prometheus_ingest_points as i64),
                    ),
                    (
                        "prometheus_ingest_errors",
                        Value::Integer(info.prometheus_ingest_errors as i64),
                    ),
                    (
                        "prometheus_ingest_total_ns",
                        Value::Integer(info.prometheus_ingest_total_ns as i64),
                    ),
                    (
                        "raw_batch_query_count",
                        Value::Integer(info.raw_batch_query_count as i64),
                    ),
                    (
                        "raw_batch_query_total_ns",
                        Value::Integer(info.raw_batch_query_total_ns as i64),
                    ),
                    (
                        "raw_batch_query_series_considered",
                        Value::Integer(info.raw_batch_query_series_considered as i64),
                    ),
                    (
                        "raw_batch_query_candidate_chunks",
                        Value::Integer(info.raw_batch_query_candidate_chunks as i64),
                    ),
                    (
                        "raw_batch_query_payload_bytes_read",
                        Value::Integer(info.raw_batch_query_payload_bytes_read as i64),
                    ),
                    (
                        "raw_batch_query_decoded_points",
                        Value::Integer(info.raw_batch_query_decoded_points as i64),
                    ),
                    (
                        "raw_batch_query_buffered_points_considered",
                        Value::Integer(info.raw_batch_query_buffered_points_considered as i64),
                    ),
                    (
                        "raw_batch_query_returned_points",
                        Value::Integer(info.raw_batch_query_returned_points as i64),
                    ),
                    (
                        "window_batch_query_count",
                        Value::Integer(info.window_batch_query_count as i64),
                    ),
                    (
                        "window_batch_query_total_ns",
                        Value::Integer(info.window_batch_query_total_ns as i64),
                    ),
                    (
                        "window_batch_query_series_considered",
                        Value::Integer(info.window_batch_query_series_considered as i64),
                    ),
                    (
                        "window_batch_query_candidate_chunks",
                        Value::Integer(info.window_batch_query_candidate_chunks as i64),
                    ),
                    (
                        "window_batch_query_payload_bytes_read",
                        Value::Integer(info.window_batch_query_payload_bytes_read as i64),
                    ),
                    (
                        "window_batch_query_decoded_points",
                        Value::Integer(info.window_batch_query_decoded_points as i64),
                    ),
                    (
                        "window_batch_query_buffered_points_considered",
                        Value::Integer(info.window_batch_query_buffered_points_considered as i64),
                    ),
                    (
                        "window_batch_query_returned_points",
                        Value::Integer(info.window_batch_query_returned_points as i64),
                    ),
                ]);
                // F3 ladder visibility: the declared tiers (native spec)
                // and how many rollup chunks exist across them.
                let conn = shared::current_conn().map_err(module_err)?;
                let tiers = crate::shadow_meta::load_meta_text(&conn, &database, &table, "rollups")
                    .map_err(module_err)?;
                rows.push(("rollup_tiers", tiers.map_or(Value::Null, Value::Text)));
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
                let _read = read_permit(&shared, self.db, &table)?;
                let (blocks, raw_blocks, buffered) = shared.engine.stats();
                let (ts_min, ts_max) = shared.engine.ts_range();
                let profile = shared.engine.profile();
                let optimize_backlog = shared.engine.optimize_backlog();
                let gate = shared.write_gate.profile();
                let storage = log_storage_summary(&database, &table)?;
                let timestamp_unit = {
                    let conn = shared::current_conn().map_err(module_err)?;
                    crate::shadow_meta::load_meta_text(&conn, &database, &table, "timestamp_unit")
                        .map_err(module_err)?
                };
                rows.extend([
                    (
                        "timestamp_unit",
                        timestamp_unit.map_or(Value::Null, Value::Text),
                    ),
                    ("blocks", Value::Integer(blocks as i64)),
                    ("raw_blocks", Value::Integer(raw_blocks as i64)),
                    (
                        "compressed_blocks",
                        Value::Integer(blocks.saturating_sub(raw_blocks) as i64),
                    ),
                    ("buffered_entries", Value::Integer(buffered as i64)),
                    ("disk_entries", Value::Integer(storage.disk_entries)),
                    (
                        "total_entries",
                        Value::Integer(storage.disk_entries.saturating_add(buffered as i64)),
                    ),
                    ("bytes_on_disk", Value::Integer(storage.bytes_on_disk)),
                    ("raw_bytes", Value::Integer(storage.raw_bytes)),
                    (
                        "compressed_bytes",
                        Value::Integer(storage.bytes_on_disk.saturating_sub(storage.raw_bytes)),
                    ),
                    (
                        "terms",
                        Value::Integer(count_rows(&database, &table, "terms")?),
                    ),
                    (
                        "index_bytes",
                        log_index_bytes(&database, &table).map_or(Value::Null, Value::Integer),
                    ),
                    ("ts_min", opt_ts(ts_min)),
                    ("ts_max", opt_ts(ts_max)),
                    (
                        "optimize_source_entries",
                        Value::Integer(storage.optimize_source_entries),
                    ),
                    (
                        "optimize_source_bytes",
                        Value::Integer(storage.optimize_source_bytes),
                    ),
                    (
                        "ingest_batch_count",
                        Value::Integer(profile.ingest_batch_count as i64),
                    ),
                    (
                        "ingest_batch_entries",
                        Value::Integer(profile.ingest_batch_entries as i64),
                    ),
                    (
                        "ingest_wire_decode_ns",
                        Value::Integer(profile.ingest_wire_decode_ns as i64),
                    ),
                    (
                        "ingest_normalize_ns",
                        Value::Integer(profile.ingest_normalize_ns as i64),
                    ),
                    (
                        "ingest_buffer_append_ns",
                        Value::Integer(profile.ingest_buffer_append_ns as i64),
                    ),
                    ("flush_count", Value::Integer(profile.flush_count as i64)),
                    (
                        "flush_entries",
                        Value::Integer(profile.flush_entries as i64),
                    ),
                    (
                        "flush_total_ns",
                        Value::Integer(profile.flush_total_ns as i64),
                    ),
                    (
                        "flush_partition_ns",
                        Value::Integer(profile.flush_partition_ns as i64),
                    ),
                    (
                        "flush_encode_terms_ns",
                        Value::Integer(profile.flush_encode_terms_ns as i64),
                    ),
                    (
                        "flush_store_ns",
                        Value::Integer(profile.flush_store_ns as i64),
                    ),
                    ("query_count", Value::Integer(profile.query_count as i64)),
                    (
                        "query_total_ns",
                        Value::Integer(profile.query_total_ns as i64),
                    ),
                    (
                        "query_snapshot_ns",
                        Value::Integer(profile.query_snapshot_ns as i64),
                    ),
                    (
                        "query_materialize_ns",
                        Value::Integer(profile.query_materialize_ns as i64),
                    ),
                    (
                        "query_snapshot_payload_bytes",
                        Value::Integer(profile.query_snapshot_payload_bytes as i64),
                    ),
                    (
                        "query_snapshot_payload_max_bytes",
                        Value::Integer(profile.query_snapshot_payload_max_bytes as i64),
                    ),
                    (
                        "query_snapshot_buffered_entries",
                        Value::Integer(profile.query_snapshot_buffered_entries as i64),
                    ),
                    (
                        "query_stable_location_snapshots",
                        Value::Integer(profile.query_stable_location_snapshots as i64),
                    ),
                    (
                        "query_payload_bytes_read",
                        Value::Integer(profile.query_payload_bytes_read as i64),
                    ),
                    (
                        "query_candidate_blocks",
                        Value::Integer(profile.query_candidate_blocks as i64),
                    ),
                    (
                        "query_decoded_entries",
                        Value::Integer(profile.query_decoded_entries as i64),
                    ),
                    (
                        "query_matched_entries",
                        Value::Integer(profile.query_matched_entries as i64),
                    ),
                    (
                        "query_returned_entries",
                        Value::Integer(profile.query_returned_entries as i64),
                    ),
                    (
                        "query_bounded_count",
                        Value::Integer(profile.query_bounded_count as i64),
                    ),
                    (
                        "query_bounded_requested_entries",
                        Value::Integer(profile.query_bounded_requested_entries as i64),
                    ),
                    (
                        "query_bounded_max_entries",
                        Value::Integer(profile.query_bounded_max_entries as i64),
                    ),
                    (
                        "query_blocks_skipped_by_bound",
                        Value::Integer(profile.query_blocks_skipped_by_bound as i64),
                    ),
                    (
                        "native_count_count",
                        Value::Integer(profile.native_count_count as i64),
                    ),
                    (
                        "native_count_total_ns",
                        Value::Integer(profile.native_count_total_ns as i64),
                    ),
                    (
                        "native_count_snapshot_ns",
                        Value::Integer(profile.native_count_snapshot_ns as i64),
                    ),
                    (
                        "native_count_payload_bytes_read",
                        Value::Integer(profile.native_count_payload_bytes_read as i64),
                    ),
                    (
                        "native_count_metadata_blocks",
                        Value::Integer(profile.native_count_metadata_blocks as i64),
                    ),
                    (
                        "native_count_metadata_entries",
                        Value::Integer(profile.native_count_metadata_entries as i64),
                    ),
                    (
                        "native_count_decoded_blocks",
                        Value::Integer(profile.native_count_decoded_blocks as i64),
                    ),
                    (
                        "native_count_decoded_entries",
                        Value::Integer(profile.native_count_decoded_entries as i64),
                    ),
                    (
                        "optimize_count",
                        Value::Integer(profile.optimize_count as i64),
                    ),
                    (
                        "optimize_total_ns",
                        Value::Integer(profile.optimize_total_ns as i64),
                    ),
                    (
                        "optimize_blocks_removed",
                        Value::Integer(profile.optimize_blocks_removed as i64),
                    ),
                    (
                        "optimize_blocks_written",
                        Value::Integer(profile.optimize_blocks_written as i64),
                    ),
                    (
                        "optimize_budgeted_count",
                        Value::Integer(profile.optimize_budgeted_count as i64),
                    ),
                    (
                        "optimize_budget_entries",
                        Value::Integer(profile.optimize_budget_entries as i64),
                    ),
                    (
                        "optimize_budget_limited_count",
                        Value::Integer(profile.optimize_budget_limited_count as i64),
                    ),
                    (
                        "optimize_raw_groups",
                        Value::Integer(profile.optimize_raw_groups as i64),
                    ),
                    (
                        "optimize_raw_blocks",
                        Value::Integer(profile.optimize_raw_blocks as i64),
                    ),
                    (
                        "optimize_raw_entries",
                        Value::Integer(profile.optimize_raw_entries as i64),
                    ),
                    (
                        "optimize_raw_input_bytes",
                        Value::Integer(profile.optimize_raw_input_bytes as i64),
                    ),
                    (
                        "optimize_raw_output_bytes",
                        Value::Integer(profile.optimize_raw_output_bytes as i64),
                    ),
                    (
                        "optimize_raw_total_ns",
                        Value::Integer(profile.optimize_raw_total_ns as i64),
                    ),
                    (
                        "optimize_merge_groups",
                        Value::Integer(profile.optimize_merge_groups as i64),
                    ),
                    (
                        "optimize_merge_blocks",
                        Value::Integer(profile.optimize_merge_blocks as i64),
                    ),
                    (
                        "optimize_merge_entries",
                        Value::Integer(profile.optimize_merge_entries as i64),
                    ),
                    (
                        "optimize_merge_input_bytes",
                        Value::Integer(profile.optimize_merge_input_bytes as i64),
                    ),
                    (
                        "optimize_merge_output_bytes",
                        Value::Integer(profile.optimize_merge_output_bytes as i64),
                    ),
                    (
                        "optimize_merge_total_ns",
                        Value::Integer(profile.optimize_merge_total_ns as i64),
                    ),
                    (
                        "optimize_pending_raw_blocks",
                        Value::Integer(optimize_backlog.raw_blocks as i64),
                    ),
                    (
                        "optimize_pending_raw_entries",
                        Value::Integer(optimize_backlog.raw_entries as i64),
                    ),
                    (
                        "optimize_merge_ready_groups",
                        Value::Integer(optimize_backlog.merge_ready_groups as i64),
                    ),
                    (
                        "optimize_merge_ready_blocks",
                        Value::Integer(optimize_backlog.merge_ready_blocks as i64),
                    ),
                    (
                        "optimize_merge_ready_entries",
                        Value::Integer(optimize_backlog.merge_ready_entries as i64),
                    ),
                    (
                        "optimize_merge_deferred_blocks",
                        Value::Integer(optimize_backlog.merge_deferred_blocks as i64),
                    ),
                    (
                        "optimize_merge_deferred_entries",
                        Value::Integer(optimize_backlog.merge_deferred_entries as i64),
                    ),
                    (
                        "read_permit_count",
                        Value::Integer(gate.read_permit_count as i64),
                    ),
                    (
                        "read_permit_hold_ns",
                        Value::Integer(gate.read_permit_hold_ns as i64),
                    ),
                    ("read_conflicts", Value::Integer(gate.read_conflicts as i64)),
                    (
                        "read_barge_rejections",
                        Value::Integer(gate.read_barge_rejections as i64),
                    ),
                    (
                        "waiting_writers",
                        Value::Integer(gate.waiting_writers as i64),
                    ),
                    (
                        "writer_wait_count",
                        Value::Integer(gate.writer_wait_count as i64),
                    ),
                    ("writer_wait_ns", Value::Integer(gate.writer_wait_ns as i64)),
                    (
                        "writer_timeouts",
                        Value::Integer(gate.writer_timeouts as i64),
                    ),
                ]);
            }
            TimelessModule::Traces => {
                let shared = TracesTab::shared_engine_for(self.db, &database, &table)?;
                let _read = read_permit(&shared, self.db, &table)?;
                let (blocks, raw_blocks, buffered) = shared.engine.stats();
                let (disk_spans, counted_buffered) = shared.engine.span_counts();
                let query = shared.engine.query_profile();
                let optimize = shared.engine.optimize_profile();
                let optimize_backlog = shared.engine.optimize_backlog();
                let gate = shared.write_gate.profile();
                debug_assert_eq!(buffered as u64, counted_buffered);
                let (ts_min, ts_max) = shared.engine.ts_range();
                rows.extend([
                    ("blocks", Value::Integer(blocks as i64)),
                    ("raw_blocks", Value::Integer(raw_blocks as i64)),
                    ("buffered_spans", Value::Integer(buffered as i64)),
                    ("disk_spans", Value::Integer(disk_spans as i64)),
                    (
                        "total_spans",
                        Value::Integer(disk_spans.saturating_add(counted_buffered) as i64),
                    ),
                    (
                        "bytes_on_disk",
                        Value::Integer(sum_blob_bytes(&database, &table, "blocks", "data")?),
                    ),
                    (
                        "terms",
                        Value::Integer(count_rows(&database, &table, "terms")?),
                    ),
                    (
                        "trace_index_rows",
                        Value::Integer(count_rows(&database, &table, "trace_blocks")?),
                    ),
                    ("ts_min", opt_ts(ts_min)),
                    ("ts_max", opt_ts(ts_max)),
                    ("query_count", Value::Integer(query.query_count as i64)),
                    (
                        "query_cancelled",
                        Value::Integer(query.query_cancelled as i64),
                    ),
                    (
                        "query_total_ns",
                        Value::Integer(query.query_total_ns as i64),
                    ),
                    (
                        "query_candidate_blocks",
                        Value::Integer(query.query_candidate_blocks as i64),
                    ),
                    (
                        "query_payload_blocks_read",
                        Value::Integer(query.query_payload_blocks_read as i64),
                    ),
                    (
                        "query_payload_bytes_read",
                        Value::Integer(query.query_payload_bytes_read as i64),
                    ),
                    (
                        "query_decoded_spans",
                        Value::Integer(query.query_decoded_spans as i64),
                    ),
                    (
                        "query_buffered_spans_examined",
                        Value::Integer(query.query_buffered_spans_examined as i64),
                    ),
                    (
                        "query_matched_spans",
                        Value::Integer(query.query_matched_spans as i64),
                    ),
                    (
                        "query_returned_spans",
                        Value::Integer(query.query_returned_spans as i64),
                    ),
                    (
                        "query_snapshot_ns",
                        Value::Integer(query.query_snapshot_ns as i64),
                    ),
                    (
                        "query_snapshot_payload_bytes",
                        Value::Integer(query.query_snapshot_payload_bytes as i64),
                    ),
                    (
                        "query_snapshot_payload_max_bytes",
                        Value::Integer(query.query_snapshot_payload_max_bytes as i64),
                    ),
                    (
                        "query_stable_location_snapshots",
                        Value::Integer(query.query_stable_location_snapshots as i64),
                    ),
                    (
                        "query_bounded_count",
                        Value::Integer(query.query_bounded_count as i64),
                    ),
                    (
                        "query_bounded_requested_spans",
                        Value::Integer(query.query_bounded_requested_spans as i64),
                    ),
                    (
                        "query_bounded_max_spans",
                        Value::Integer(query.query_bounded_max_spans as i64),
                    ),
                    (
                        "query_blocks_skipped_by_bound",
                        Value::Integer(query.query_blocks_skipped_by_bound as i64),
                    ),
                    (
                        "discovery_count",
                        Value::Integer(query.discovery_count as i64),
                    ),
                    (
                        "discovery_total_ns",
                        Value::Integer(query.discovery_total_ns as i64),
                    ),
                    (
                        "discovery_payload_bytes_read",
                        Value::Integer(query.discovery_payload_bytes_read as i64),
                    ),
                    (
                        "discovery_decoded_spans",
                        Value::Integer(query.discovery_decoded_spans as i64),
                    ),
                    (
                        "optimize_count",
                        Value::Integer(optimize.optimize_count as i64),
                    ),
                    (
                        "optimize_total_ns",
                        Value::Integer(optimize.optimize_total_ns as i64),
                    ),
                    (
                        "optimize_blocks_removed",
                        Value::Integer(optimize.optimize_blocks_removed as i64),
                    ),
                    (
                        "optimize_blocks_written",
                        Value::Integer(optimize.optimize_blocks_written as i64),
                    ),
                    (
                        "optimize_budgeted_count",
                        Value::Integer(optimize.optimize_budgeted_count as i64),
                    ),
                    (
                        "optimize_budget_entries",
                        Value::Integer(optimize.optimize_budget_entries as i64),
                    ),
                    (
                        "optimize_budget_limited_count",
                        Value::Integer(optimize.optimize_budget_limited_count as i64),
                    ),
                    (
                        "optimize_raw_groups",
                        Value::Integer(optimize.optimize_raw_groups as i64),
                    ),
                    (
                        "optimize_raw_blocks",
                        Value::Integer(optimize.optimize_raw_blocks as i64),
                    ),
                    (
                        "optimize_raw_entries",
                        Value::Integer(optimize.optimize_raw_entries as i64),
                    ),
                    (
                        "optimize_raw_input_bytes",
                        Value::Integer(optimize.optimize_raw_input_bytes as i64),
                    ),
                    (
                        "optimize_raw_output_bytes",
                        Value::Integer(optimize.optimize_raw_output_bytes as i64),
                    ),
                    (
                        "optimize_raw_total_ns",
                        Value::Integer(optimize.optimize_raw_total_ns as i64),
                    ),
                    (
                        "optimize_merge_groups",
                        Value::Integer(optimize.optimize_merge_groups as i64),
                    ),
                    (
                        "optimize_merge_blocks",
                        Value::Integer(optimize.optimize_merge_blocks as i64),
                    ),
                    (
                        "optimize_merge_entries",
                        Value::Integer(optimize.optimize_merge_entries as i64),
                    ),
                    (
                        "optimize_merge_input_bytes",
                        Value::Integer(optimize.optimize_merge_input_bytes as i64),
                    ),
                    (
                        "optimize_merge_output_bytes",
                        Value::Integer(optimize.optimize_merge_output_bytes as i64),
                    ),
                    (
                        "optimize_merge_total_ns",
                        Value::Integer(optimize.optimize_merge_total_ns as i64),
                    ),
                    (
                        "optimize_pending_raw_blocks",
                        Value::Integer(optimize_backlog.raw_blocks as i64),
                    ),
                    (
                        "optimize_pending_raw_entries",
                        Value::Integer(optimize_backlog.raw_entries as i64),
                    ),
                    (
                        "optimize_merge_ready_groups",
                        Value::Integer(optimize_backlog.merge_ready_groups as i64),
                    ),
                    (
                        "optimize_merge_ready_blocks",
                        Value::Integer(optimize_backlog.merge_ready_blocks as i64),
                    ),
                    (
                        "optimize_merge_ready_entries",
                        Value::Integer(optimize_backlog.merge_ready_entries as i64),
                    ),
                    (
                        "optimize_merge_deferred_blocks",
                        Value::Integer(optimize_backlog.merge_deferred_blocks as i64),
                    ),
                    (
                        "optimize_merge_deferred_entries",
                        Value::Integer(optimize_backlog.merge_deferred_entries as i64),
                    ),
                    (
                        "read_permit_count",
                        Value::Integer(gate.read_permit_count as i64),
                    ),
                    (
                        "read_permit_hold_ns",
                        Value::Integer(gate.read_permit_hold_ns as i64),
                    ),
                    ("read_conflicts", Value::Integer(gate.read_conflicts as i64)),
                    (
                        "read_barge_rejections",
                        Value::Integer(gate.read_barge_rejections as i64),
                    ),
                    (
                        "waiting_writers",
                        Value::Integer(gate.waiting_writers as i64),
                    ),
                    (
                        "writer_wait_count",
                        Value::Integer(gate.writer_wait_count as i64),
                    ),
                    ("writer_wait_ns", Value::Integer(gate.writer_wait_ns as i64)),
                    (
                        "writer_timeouts",
                        Value::Integer(gate.writer_timeouts as i64),
                    ),
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
