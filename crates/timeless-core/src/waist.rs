//! THE NARROW WAIST (PLAN.md "Query interface tiers & the PromQL
//! layering contract") — the pinned API the timeless_metrics engine
//! swap binds against. The entire PromQL/MetricsQL evaluator consumes
//! storage through exactly two calls:
//!
//!   query_multi(metric, matchers, from, to) → [(labels, [(ts, value)])]
//!   list_metrics() → [metric names]
//!
//! Everything above — grid evaluation, lookback, rate, vector matching,
//! histogram_quantile, MetricsQL trivia — is storage-agnostic and stays
//! above this line. Any engine serving this waist is, by construction,
//! certifiable by the vm_diff conformance suite without the suite
//! knowing which engine is underneath.
//!
//! MATCHER POLICY (the layering rule applied):
//!   - `=`  (Eq)  pushes down into the registry (find_series).
//!   - `!=` (Neq) is MECHANICAL string inequality — evaluated here.
//!     An absent label matches `!=` (Prometheus semantics for a
//!     non-empty RHS are the CALLER's concern; this layer compares the
//!     label's value-or-absence against the string, exactly as
//!     documented on Matcher::Neq).
//!   - `=~` / `!~` are NOT implemented at this layer. Regex DIALECT is
//!     caller semantics (it must match whatever the evaluator's users
//!     wrote), and semantics never live below the waist. Callers
//!     evaluate regex matchers above the waist via list_series() and
//!     then fetch with query_multi_ids() — both provided here so the
//!     escape hatch is part of the pinned contract, not improvisation.
//!
//! All reads are SEQUENTIAL per series (safe from NIF schedulers and
//! vtab callbacks alike). Callers that want parallel reads over fs
//! stores can use Engine::query_range_labeled directly — that path is
//! rayon-parallel and NOT part of the waist.

use crate::engine::{Engine, Labels};

/// One label matcher, waist subset. See the module docs for why regex
/// matchers are deliberately absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Matcher {
    /// label == value; pushed down into the series registry.
    Eq { key: String, value: String },
    /// label != value, where an ABSENT label compares as the empty
    /// string (so `k != "v"` matches series without `k`, and
    /// `k != ""` matches exactly the series that HAVE `k`). This is
    /// mechanical string comparison — Prometheus-compatible for the
    /// common cases and precisely specified for the rest.
    Neq { key: String, value: String },
}

/// The waist, call one: every series of `metric` whose labels satisfy
/// ALL matchers, with every sample in `[from, to]` inclusive, in
/// ascending ts order. Series with zero samples in range are omitted.
pub fn query_multi(
    engine: &Engine,
    metric: &str,
    matchers: &[Matcher],
    from: i64,
    to: i64,
) -> Result<Vec<(Labels, Vec<(i64, f64)>)>, String> {
    let candidates = matching_series(engine, metric, matchers);
    let mut out = Vec::new();
    for (sid, labels) in candidates {
        let points = engine.query_range_by_id(sid, from, to)?;
        if !points.is_empty() {
            out.push((labels, points));
        }
    }
    Ok(out)
}

/// The waist, call two: every metric name known to the engine, sorted.
pub fn list_metrics(engine: &Engine) -> Vec<String> {
    engine.series_read().list_metrics()
}

/// Above-waist matcher support: enumerate `metric`'s series with their
/// labels so the CALLER can evaluate regex (or any other) matchers,
/// then fetch the survivors with query_multi_ids.
pub fn list_series(engine: &Engine, metric: &str) -> Vec<(i64, Labels)> {
    let reg = engine.series_read();
    reg.find_series(metric, &Labels::new())
        .into_iter()
        .filter_map(|sid| reg.info_for(sid).map(|info| (sid, info.labels.clone())))
        .collect()
}

/// Fetch specific series (from list_series, post-caller-filtering) with
/// their labels. Unknown ids are silently omitted (they may have been
/// pruned between enumeration and fetch — the caller sees a consistent
/// subset, never an error, matching query_multi's omission of empty
/// series).
pub fn query_multi_ids(
    engine: &Engine,
    series_ids: &[i64],
    from: i64,
    to: i64,
) -> Result<Vec<(Labels, Vec<(i64, f64)>)>, String> {
    let mut out = Vec::new();
    for &sid in series_ids {
        let labels = match engine.series_read().info_for(sid) {
            Some(info) => info.labels.clone(),
            None => continue,
        };
        let points = engine.query_range_by_id(sid, from, to)?;
        if !points.is_empty() {
            out.push((labels, points));
        }
    }
    Ok(out)
}

fn matching_series(engine: &Engine, metric: &str, matchers: &[Matcher]) -> Vec<(i64, Labels)> {
    // Eq matchers push down as the registry's equality filter.
    let mut eq = Labels::new();
    for m in matchers {
        if let Matcher::Eq { key, value } = m {
            eq.insert(key.clone(), value.clone());
        }
    }
    let reg = engine.series_read();
    reg.find_series(metric, &eq)
        .into_iter()
        .filter_map(|sid| reg.info_for(sid).map(|info| (sid, info.labels.clone())))
        .filter(|(_, labels)| {
            matchers.iter().all(|m| match m {
                Matcher::Eq { .. } => true, // already applied
                Matcher::Neq { key, value } => {
                    labels.get(key).map(String::as_str).unwrap_or("") != value
                }
            })
        })
        .collect()
}
