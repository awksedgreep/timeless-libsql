//! M1a: the pinned waist (FEATURE_PLAN.md). query_multi/list_metrics
//! shapes, Eq/Neq matcher semantics vs naive evaluation, and the
//! above-waist escape hatch (list_series + query_multi_ids).

use std::collections::HashMap;

use timeless_core::waist::{self, Matcher};
use timeless_core::{Engine, Labels};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("timeless_m1_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn setup(name: &str) -> (Engine, Vec<(i64, Labels)>) {
    let engine = Engine::new(temp_dir(name), 100_000, 0, 3, 64 << 20, false).unwrap();
    let mut series = Vec::new();
    for (host, env) in [
        (Some("a"), Some("prod")),
        (Some("a"), Some("dev")),
        (Some("b"), Some("prod")),
        (Some("b"), None), // env absent — the Neq edge case
    ] {
        let mut labels: HashMap<String, String> = HashMap::new();
        if let Some(h) = host {
            labels.insert("host".into(), h.into());
        }
        if let Some(e) = env {
            labels.insert("env".into(), e.into());
        }
        let sid = engine.resolve_cached("cpu", &labels).unwrap();
        for i in 0..50 {
            engine.write_point(sid, 1000 + i, (sid * 1000 + i) as f64 * 0.5);
        }
        series.push((sid, labels.into_iter().collect::<Labels>()));
    }
    // A second metric, and a series with no points in the query range.
    let other = engine.resolve_cached("mem", &HashMap::new()).unwrap();
    engine.write_point(other, 1000, 1.0);
    let empty_range = engine
        .resolve_cached(
            "cpu",
            &[("host".to_string(), "z".to_string())].into_iter().collect(),
        )
        .unwrap();
    engine.write_point(empty_range, 99_999, 1.0); // outside [1000, 2000]
    engine.flush_all().unwrap();
    (engine, series)
}

fn naive(
    series: &[(i64, Labels)],
    engine: &Engine,
    matchers: &[Matcher],
    from: i64,
    to: i64,
) -> Vec<(Labels, Vec<(i64, f64)>)> {
    let mut out = Vec::new();
    for (sid, labels) in series {
        let ok = matchers.iter().all(|m| match m {
            Matcher::Eq { key, value } => labels.get(key) == Some(value),
            Matcher::Neq { key, value } => {
                labels.get(key).map(String::as_str).unwrap_or("") != value
            }
        });
        if !ok {
            continue;
        }
        let points = engine.query_range_by_id(*sid, from, to).unwrap();
        if !points.is_empty() {
            out.push((labels.clone(), points));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn eq(key: &str, value: &str) -> Matcher {
    Matcher::Eq {
        key: key.into(),
        value: value.into(),
    }
}

fn neq(key: &str, value: &str) -> Matcher {
    Matcher::Neq {
        key: key.into(),
        value: value.into(),
    }
}

#[test]
fn query_multi_matches_naive() {
    let (engine, series) = setup("naive");
    let cases: Vec<Vec<Matcher>> = vec![
        vec![],
        vec![eq("host", "a")],
        vec![eq("host", "a"), eq("env", "prod")],
        vec![neq("env", "prod")],          // matches dev AND absent-env
        vec![neq("env", "")],              // matches exactly series WITH env
        vec![eq("host", "b"), neq("env", "prod")], // the absent-env series
        vec![eq("host", "nope")],          // no matches
    ];
    for matchers in cases {
        let mut got = waist::query_multi(&engine, "cpu", &matchers, 1000, 2000).unwrap();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        let want = naive(&series, &engine, &matchers, 1000, 2000);
        assert_eq!(got.len(), want.len(), "matchers {matchers:?}: series count");
        for (g, w) in got.iter().zip(&want) {
            assert_eq!(g.0, w.0, "matchers {matchers:?}: labels");
            assert_eq!(g.1.len(), w.1.len(), "matchers {matchers:?}: points");
            for (gp, wp) in g.1.iter().zip(&w.1) {
                assert_eq!(gp.0, wp.0);
                assert_eq!(gp.1.to_bits(), wp.1.to_bits(), "bit-exact values");
            }
        }
    }
}

#[test]
fn waist_shapes_and_escape_hatch() {
    let (engine, _series) = setup("shapes");

    // list_metrics: sorted, both metrics present.
    assert_eq!(waist::list_metrics(&engine), vec!["cpu", "mem"]);

    // The empty-in-range series is OMITTED, not returned empty.
    let all = waist::query_multi(&engine, "cpu", &[], 1000, 2000).unwrap();
    assert_eq!(all.len(), 4, "series with no in-range points omitted");
    assert!(all.iter().all(|(_, pts)| !pts.is_empty()));

    // Escape hatch: enumerate, filter above the waist (here: a fake
    // "regex" that keeps host=a), fetch by ids — identical to the
    // matcher path.
    let enumerated = waist::list_series(&engine, "cpu");
    assert_eq!(enumerated.len(), 5, "list_series shows ALL series (even empty-in-range)");
    let ids: Vec<i64> = enumerated
        .iter()
        .filter(|(_, l)| l.get("host").map(String::as_str) == Some("a"))
        .map(|(sid, _)| *sid)
        .collect();
    let via_ids = waist::query_multi_ids(&engine, &ids, 1000, 2000).unwrap();
    let via_matcher = waist::query_multi(&engine, "cpu", &[eq("host", "a")], 1000, 2000).unwrap();
    assert_eq!(via_ids.len(), via_matcher.len());

    // Unknown ids are silently omitted.
    let ghost = waist::query_multi_ids(&engine, &[999_999], 1000, 2000).unwrap();
    assert!(ghost.is_empty());
}
