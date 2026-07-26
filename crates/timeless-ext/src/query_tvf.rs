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
//!     '{"host":"pvm1"}',  -- label equality filter (NULL/'{}' = all)
//!     :start, :stop, :step, :lookback);
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

use crate::flatjson::{labels_to_json, parse_labels_json};
use crate::metrics_vtab::MetricsTab;
use crate::shared::{DbGuard, SharedEngine};

fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

/// Register both TVF modules on a freshly-loaded connection.
pub(crate) fn register(db: &Connection) -> Result<()> {
    const GRID: Module<GridTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_grid", &GRID, None::<()>)?;
    const WINDOW: Module<WindowTab> = Module::eponymous_only_module();
    db.create_module(c"timeless_window", &WINDOW, None::<()>)
}

// Output columns (both modules).
const COL_LABELS: c_int = 0;
// Hidden argument columns start here; canonical order = function-call
// argument order.
const COL_FIRST_ARG: c_int = 3;

/// Everything one TVF scan needs, decoded from the pushed constraints.
pub(crate) struct KernelArgs {
    database: String,
    table: String,
    metric: String,
    filter: Labels,
    start: i64,
    stop: i64,
    step: i64,
    /// lookback (grid) or window (window).
    width: i64,
    agg: Option<AggFn>,
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
    has_agg: bool,
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

    // argv slots were assigned in canonical order over the provided args.
    let mut slot = 0usize;
    let mut take = |i: usize| -> Option<usize> {
        if idx_num & (1 << i) != 0 {
            let s = slot;
            slot += 1;
            Some(s)
        } else {
            None
        }
    };

    let get_text = |s: usize, what: &str| -> Result<String> {
        let v: Option<String> = args.get(s)?;
        v.ok_or_else(|| module_err(format!("{module}: {what} must not be NULL")))
    };
    let get_int = |s: usize, what: &str| -> Result<i64> {
        let v: Option<i64> = args.get(s)?;
        v.ok_or_else(|| module_err(format!("{module}: {what} must not be NULL")))
    };

    let tbl_slot = take(0).unwrap();
    let metric_slot = take(1).unwrap();
    let filter_slot = take(2);
    let start_slot = take(3).unwrap();
    let stop_slot = take(4).unwrap();
    let step_slot = take(5).unwrap();
    let width_slot = take(6).unwrap();
    let agg_slot = if has_agg { take(7) } else { None };

    let spec = get_text(tbl_slot, names[0])?;
    // 'schema.table' selects an attached schema; plain 'table' = main.
    // (A MAIN-schema table name containing a literal dot needs the vtab
    // spelled 'main.<name>'.)
    let (database, table) = match spec.split_once('.') {
        Some((schema, tbl)) => (schema.to_owned(), tbl.to_owned()),
        None => ("main".to_owned(), spec),
    };

    let filter: Labels = match filter_slot {
        None => Labels::new(),
        Some(s) => match args.get::<Option<String>>(s)? {
            None => Labels::new(), // NULL filter = no filter
            Some(txt) if txt.is_empty() => Labels::new(),
            Some(txt) => parse_labels_json(&txt)
                .map_err(|e| module_err(format!("{module}: filter: {e}")))?
                .into_iter()
                .collect(),
        },
    };

    let agg = match agg_slot {
        None => None,
        Some(s) => {
            let name = get_text(s, "agg")?;
            Some(match name.as_str() {
                "sum" => AggFn::Sum,
                "min" => AggFn::Min,
                "max" => AggFn::Max,
                "count" => AggFn::Count,
                "avg" => AggFn::Avg,
                other => {
                    return Err(module_err(format!(
                        "{module}: unknown agg {other:?}; expected one of: sum, min, max, count, avg"
                    )))
                }
            })
        }
    };

    Ok(KernelArgs {
        database,
        table,
        metric: get_text(metric_slot, names[1])?,
        filter,
        start: get_int(start_slot, names[3])?,
        stop: get_int(stop_slot, names[4])?,
        step: get_int(step_slot, names[5])?,
        width: get_int(width_slot, names[6])?,
        agg,
    })
}

/// Shared best_index for both modules: collect EQ constraints on the
/// hidden arg columns into a bitmask, assign argv slots in canonical
/// order, defer required-arg checking to filter (clearer errors than a
/// bare "no query solution").
fn best_index_args(info: &mut IndexInfo, n_args: c_int) -> Result<bool> {
    let mut idx_num: c_int = 0;
    let mut unusable: c_int = 0;
    let mut slots: Vec<Option<usize>> = vec![None; n_args as usize];
    for (i, constraint) in info.constraints().enumerate() {
        let col = constraint.column();
        if col < COL_FIRST_ARG || col >= COL_FIRST_ARG + n_args {
            continue;
        }
        let bit = 1 << (col - COL_FIRST_ARG);
        if !constraint.is_usable() {
            unusable |= bit;
        } else if constraint.operator() == IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ
            && slots[(col - COL_FIRST_ARG) as usize].is_none()
        {
            idx_num |= bit;
            slots[(col - COL_FIRST_ARG) as usize] = Some(i);
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
) -> Result<Vec<(String, i64, f64)>> {
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
            .collect()
    };

    let mut rows = Vec::new();
    for (sid, labels) in candidates {
        let points = kernel(&shared.engine, sid)?;
        if points.is_empty() {
            continue;
        }
        let labels_json = labels_to_json(&labels);
        for (ts, value) in points {
            rows.push((labels_json.clone(), ts, value));
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// timeless_grid
// ---------------------------------------------------------------------------

const GRID_ARGS: &[&str] = &["tbl", "metric", "filter", "start", "stop", "step", "lookback"];
// All required except filter (bit 2).
const GRID_REQUIRED: c_int = 0b111_1011;

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
                            stop HIDDEN, step HIDDEN, lookback HIDDEN)"),
            GridTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, GRID_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<KernelCursor<'vtab, GridTab>> {
        Ok(KernelCursor::new(self.db))
    }
}

impl KernelVTab for GridTab {
    const MODULE: &'static str = "timeless_grid";
    const ARGS: &'static [&'static str] = GRID_ARGS;
    const REQUIRED: c_int = GRID_REQUIRED;
    const HAS_AGG: bool = false;

    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<(String, i64, f64)>> {
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
    "tbl", "metric", "filter", "start", "stop", "step", "window", "agg",
];
// All required except filter (bit 2).
const WINDOW_REQUIRED: c_int = 0b1111_1011;

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
                            stop HIDDEN, step HIDDEN, window HIDDEN, agg HIDDEN)"),
            WindowTab {
                base: ffi::sqlite3_vtab::default(),
                db: handle,
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        best_index_args(info, WINDOW_ARGS.len() as c_int)
    }

    fn open(&mut self) -> Result<KernelCursor<'vtab, WindowTab>> {
        Ok(KernelCursor::new(self.db))
    }
}

impl KernelVTab for WindowTab {
    const MODULE: &'static str = "timeless_window";
    const ARGS: &'static [&'static str] = WINDOW_ARGS;
    const REQUIRED: c_int = WINDOW_REQUIRED;
    const HAS_AGG: bool = true;

    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<(String, i64, f64)>> {
        let agg = ka.agg.expect("agg is a required window argument");
        run_kernel(db, ka, |engine, sid| {
            engine
                .query_window_agg_by_id(sid, ka.start, ka.stop, ka.step, ka.width, agg)
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
    const HAS_AGG: bool;
    fn run(db: *mut ffi::sqlite3, ka: &KernelArgs) -> Result<Vec<(String, i64, f64)>>;
}

#[repr(C)]
pub(crate) struct KernelCursor<'vtab, T: KernelVTab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    rows: Vec<(String, i64, f64)>,
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
        let ka = decode_args(T::MODULE, T::ARGS, T::REQUIRED, idx_num, args, T::HAS_AGG)?;
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
            2 => ctx.set_result(value),
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
