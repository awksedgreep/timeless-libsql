//! Locks in the ingest_prometheus semantics the timeless_metrics vtab
//! depends on (metrics_vtab.rs `ingest_prometheus_text`):
//!
//! 1. Samples WITHOUT a timestamp receive default_ts VERBATIM — the vtab
//!    passes wall-clock EPOCH SECONDS, so the substituted value must not
//!    be scaled or otherwise touched.
//! 2. Explicit timestamps > 1e12 (the Prometheus wire unit: epoch
//!    MILLISECONDS) are normalized to SECONDS (/1000). This is the fact
//!    that forces the vtab's "default_ts is seconds" unit decision.
//! 3. NaN and infinities are valid float-series samples and retain their IEEE
//!    values; malformed non-comment lines are COUNTED as errors but never
//!    abort the body — partial success is the contract.
//! 4. Comments / HELP / TYPE / blank lines are free: neither samples nor
//!    errors.
//! 5. Prometheus label-value `\\n`, `\\\"`, and `\\\\` escapes are decoded
//!    before series identity is resolved.
//!
//! If any of these change, the vtab's Prometheus path (and cli.sh
//! section 18) breaks — fail here first, with a readable message.

use std::collections::HashMap;
use timeless_core::Engine;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("timeless_core_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn prometheus_ingest_semantics() {
    let dir = temp_dir("prom_ingest");
    let engine = Engine::new(dir.clone(), 1000, 0, 8, 64 * 1024 * 1024, false).unwrap();

    // One body exercising every rule at once — the same shape cli.sh
    // section 18 feeds through the vtab.
    let body = b"# HELP http_requests_total Total HTTP requests.\n\
                 # TYPE http_requests_total counter\n\
                 http_requests_total 1027\n\
                 node_temp_celsius{sensor=\"cpu0\",host=\"pvm1\"} 42.5 1753000000123\n\
                 this line is definitely not prometheus !!!\n\
                 bad_metric NaN\n\
                 positive_inf Inf\n\
                 negative_inf -Inf\n";

    // default_ts is deterministic here (no wall clock in a unit test).
    let default_ts: i64 = 1_800_000_000;
    let (count, errors) = engine.ingest_prometheus(body, default_ts).unwrap();
    assert_eq!(count, 5, "all five valid floats ingest; comments are free");
    assert_eq!(errors, 1, "only the malformed line counts as an error");

    // Rule 1: no-timestamp sample carries default_ts VERBATIM (seconds).
    let sid = engine
        .resolve_cached("http_requests_total", &HashMap::new())
        .unwrap();
    let rows = engine.query_range_by_id(sid, 0, i64::MAX - 1).unwrap();
    assert_eq!(
        rows,
        vec![(default_ts, 1027.0)],
        "absent prom timestamp must become default_ts untouched"
    );

    // Rule 2: explicit ms timestamp normalized to seconds — so seconds
    // is the only default unit that keeps one body consistent.
    let labels: HashMap<String, String> = [
        ("sensor".to_string(), "cpu0".to_string()),
        ("host".to_string(), "pvm1".to_string()),
    ]
    .into_iter()
    .collect();
    let sid = engine.resolve_cached("node_temp_celsius", &labels).unwrap();
    let rows = engine.query_range_by_id(sid, 0, i64::MAX - 1).unwrap();
    assert_eq!(
        rows,
        vec![(1_753_000_000, 42.5)],
        "explicit 1753000000123 ms must be stored as 1753000000 s"
    );

    let nan = engine
        .resolve_cached("bad_metric", &HashMap::new())
        .unwrap();
    let nan_rows = engine.query_range_by_id(nan, 0, i64::MAX - 1).unwrap();
    assert_eq!(nan_rows.len(), 1);
    assert!(nan_rows[0].1.is_nan());
    assert_ne!(
        nan_rows[0].1.to_bits(),
        0x7ff0_0000_0000_0002,
        "text exposition NaN is an ordinary float NaN, not an implicit Prometheus stale marker"
    );
    for (name, expected) in [
        ("positive_inf", f64::INFINITY),
        ("negative_inf", f64::NEG_INFINITY),
    ] {
        let sid = engine.resolve_cached(name, &HashMap::new()).unwrap();
        assert_eq!(
            engine.query_range_by_id(sid, 0, i64::MAX - 1).unwrap(),
            vec![(default_ts, expected)]
        );
    }

    let escaped_body = b"escaped_labels{path=\"a\\\"b\",note=\"x\\\\y\",line=\"a\\nb\"} 7 1800000000000\r\ncrlf_metric 9\r\n";
    let (count, errors) = engine.ingest_prometheus(escaped_body, default_ts).unwrap();
    assert_eq!((count, errors), (2, 0));
    let escaped: HashMap<String, String> = [
        ("path".to_string(), "a\"b".to_string()),
        ("note".to_string(), r#"x\y"#.to_string()),
        ("line".to_string(), "a\nb".to_string()),
    ]
    .into_iter()
    .collect();
    let sid = engine.resolve_cached("escaped_labels", &escaped).unwrap();
    assert_eq!(
        engine.query_range_by_id(sid, 0, i64::MAX - 1).unwrap(),
        vec![(1_800_000_000, 7.0)]
    );
    let sid = engine
        .resolve_cached("crlf_metric", &HashMap::new())
        .unwrap();
    assert_eq!(
        engine.query_range_by_id(sid, 0, i64::MAX - 1).unwrap(),
        vec![(default_ts, 9.0)]
    );

    // Rule 3 corollary: an all-garbage body yields (0, N) — the vtab
    // turns exactly that shape into its "0 samples ingested" error.
    let (count, errors) = engine
        .ingest_prometheus(b"garbage one\ngarbage two\n", default_ts)
        .unwrap();
    assert_eq!((count, errors), (0, 2));

    let info = engine.info();
    assert_eq!(info.prometheus_ingest_batches, 3);
    assert_eq!(info.prometheus_ingest_points, 7);
    assert_eq!(info.prometheus_ingest_errors, 3);
    assert!(info.prometheus_ingest_total_ns > 0);

    engine.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
