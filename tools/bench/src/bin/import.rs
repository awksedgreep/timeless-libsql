//! import: migrate an existing timeless FsStore data directory into a
//! timeless_metrics vtab (M1b, FEATURE_PLAN.md) — the storage half of
//! the timeless_metrics engine swap.
//!
//!   import <fs-data-dir> <path-to-extension> <output.db> [table]
//!   import --selftest <path-to-extension> <scratch-dir>
//!
//! Strategy: recover the source through the fs engine (registry + chunk
//! index, exactly as a restart would), then REPLAY every series through
//! the extension's Tier 2 batch-blob path — the proven write pipeline,
//! so catalog identity, validation, generation bumps and durability all
//! come for free instead of being reimplemented against shadow tables.
//!
//! VERIFICATION IS NOT OPTIONAL: after flush, every point of every
//! series is read back through the vtab and compared BIT-exact
//! ((ts, f64 bits) multisets per series, labels matched by canonical
//! JSON). The tool fails loudly on the first mismatch — an import that
//! cannot prove itself did not happen.
//!
//! Empty series (registered names with zero stored points) are NOT
//! migrated — they carry no data and the target allocates identity on
//! first write.

use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::time::Instant;

use rusqlite::{params, Connection};
use timeless_core::Engine;

type Labels = BTreeMap<String, String>;

/// Mirror of the extension's canonical label JSON (flatjson.rs): sorted
/// keys (BTreeMap iteration), minimal escaping. The selftest's hostile
/// labels exist to prove this mirror exact — if the encodings ever
/// drift, verification fails on the labels filter.
fn labels_to_json(labels: &Labels) -> String {
    fn esc(out: &mut String, s: &str) {
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                c => out.push(c),
            }
        }
    }
    let mut out = String::from("{");
    let mut first = true;
    for (k, v) in labels {
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        esc(&mut out, k);
        out.push_str("\":\"");
        esc(&mut out, v);
        out.push('"');
    }
    out.push('}');
    out
}

/// One source series ready to ship.
struct SourceSeries {
    name: String,
    labels_json: String,
    points: Vec<(i64, f64)>,
}

/// Encode one metrics batch blob v0 (PLAN.md spec) for a group of
/// series. Values round-trip by BITS.
fn encode_blob(group: &[&SourceSeries]) -> Vec<u8> {
    let n_points: usize = group.iter().map(|s| s.points.len()).sum();
    let mut out = Vec::with_capacity(64 + n_points * 20);
    out.push(0x01);
    out.push(0x00);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(group.len() as u32).to_le_bytes());
    out.extend_from_slice(&(n_points as u32).to_le_bytes());
    for s in group {
        out.extend_from_slice(&(s.name.len() as u32).to_le_bytes());
        out.extend_from_slice(s.name.as_bytes());
        // '{}' → empty labels field (the blob's "no labels" spelling).
        let labels = if s.labels_json == "{}" {
            ""
        } else {
            &s.labels_json
        };
        out.extend_from_slice(&(labels.len() as u32).to_le_bytes());
        out.extend_from_slice(labels.as_bytes());
    }
    for (idx, s) in group.iter().enumerate() {
        for _ in 0..s.points.len() {
            out.extend_from_slice(&(idx as u32).to_le_bytes());
        }
    }
    for s in group {
        for &(ts, _) in &s.points {
            out.extend_from_slice(&ts.to_le_bytes());
        }
    }
    for s in group {
        for &(_, val) in &s.points {
            out.extend_from_slice(&val.to_bits().to_le_bytes());
        }
    }
    out
}

fn run_import(data_dir: &Path, ext: &str, db_path: &str, table: &str) -> Result<(), String> {
    let t0 = Instant::now();
    let engine = Engine::new(data_dir.to_path_buf(), usize::MAX, 0, 3, 512 << 20, false)
        .map_err(|e| format!("failed to open source data dir: {e}"))?;

    let overview = engine.series_overview();
    let mut series: Vec<SourceSeries> = Vec::new();
    let mut total_points = 0usize;
    let mut skipped_empty = 0usize;
    for row in &overview {
        let points = engine
            .query_range_by_id(row.series_id, i64::MIN, i64::MAX)
            .map_err(|e| format!("failed to read series {}: {e}", row.series_id))?;
        if points.is_empty() {
            skipped_empty += 1;
            continue;
        }
        total_points += points.len();
        series.push(SourceSeries {
            name: row.name.clone(),
            labels_json: labels_to_json(&row.labels),
            points,
        });
    }
    println!(
        "- source: {} series, {total_points} points ({skipped_empty} empty series skipped)",
        series.len()
    );

    // Ship in blobs of ~100k points (or 10k series entries, whichever
    // first — both far below the format's u32 limits).
    let conn = Connection::open(db_path).map_err(|e| format!("open {db_path}: {e}"))?;
    unsafe {
        conn.load_extension_enable().map_err(|e| e.to_string())?;
        conn.load_extension(ext, None::<&str>)
            .map_err(|e| format!("load extension: {e}"))?;
    }
    conn.load_extension_disable().map_err(|e| e.to_string())?;
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE IF NOT EXISTS \"{}\" USING timeless_metrics;",
        table.replace('"', "\"\"")
    ))
    .map_err(|e| format!("create vtab: {e}"))?;

    let quoted = format!("\"{}\"", table.replace('"', "\"\""));
    let insert_sql = format!("INSERT INTO {quoted}({quoted}) VALUES (?1)");
    let mut shipped = 0usize;
    conn.execute_batch("BEGIN").map_err(|e| e.to_string())?;
    {
        let mut stmt = conn
            .prepare(&insert_sql)
            .map_err(|e| format!("prepare ingest: {e}"))?;
        let mut group: Vec<&SourceSeries> = Vec::new();
        let mut group_points = 0usize;
        let mut flush_group =
            |group: &mut Vec<&SourceSeries>, group_points: &mut usize| -> Result<(), String> {
                if group.is_empty() {
                    return Ok(());
                }
                let blob = encode_blob(group);
                stmt.execute(params![blob])
                    .map_err(|e| format!("blob ingest: {e}"))?;
                shipped += group.len();
                group.clear();
                *group_points = 0;
                Ok(())
            };
        for s in &series {
            if group_points + s.points.len() > 100_000 || group.len() >= 10_000 {
                flush_group(&mut group, &mut group_points)?;
            }
            group.push(s);
            group_points += s.points.len();
        }
        flush_group(&mut group, &mut group_points)?;
    }
    conn.execute_batch("COMMIT").map_err(|e| e.to_string())?;
    conn.execute(&insert_sql, params!["flush"])
        .map_err(|e| format!("flush: {e}"))?;
    println!("- shipped {shipped} series via Tier 2 blobs + flush");

    // ── The non-optional part: verify EVERY point ───────────────────
    // Bit-exact for every finite value. NaN is a documented SQLite
    // surface limitation: REAL cannot represent NaN, so SQL reads show
    // NULL — storage keeps the exact bits (the chunk payload round-
    // trips them; engine/waist reads return real NaN). Verification
    // therefore matches NaN samples positionally against NULLs and
    // everything else by bits.
    let mut verify_stmt = conn
        .prepare(&format!(
            "SELECT ts, value FROM {quoted} WHERE name = ?1 AND labels = ?2"
        ))
        .map_err(|e| format!("prepare verify: {e}"))?;
    for s in &series {
        let mut got: Vec<(i64, Option<u64>)> = verify_stmt
            .query_map(params![s.name, s.labels_json], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<f64>>(1)?.map(f64::to_bits),
                ))
            })
            .map_err(|e| format!("verify query: {e}"))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("verify row: {e}"))?;
        let mut want: Vec<(i64, Option<u64>)> = s
            .points
            .iter()
            .map(|&(ts, v)| (ts, (!v.is_nan()).then(|| v.to_bits())))
            .collect();
        got.sort_unstable();
        want.sort_unstable();
        if got != want {
            return Err(format!(
                "VERIFICATION FAILED for {} {}: {} points in source, {} in target \
                 (first divergence near {:?})",
                s.name,
                s.labels_json,
                want.len(),
                got.len(),
                want.iter().zip(&got).find(|(a, b)| a != b)
            ));
        }
    }
    drop(verify_stmt);
    drop(conn);
    let bytes = std::fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    println!(
        "- VERIFIED bit-exact: {} series, {total_points} points → {db_path} ({bytes} bytes) in {:.2}s",
        series.len(),
        t0.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Deterministic hostile fixture + full import cycle. The fixture
/// exists to break sloppy importers: escaped/quoted labels, NaN with a
/// payload, negative and far-future timestamps, duplicate timestamps,
/// a multi-blob-sized series, and an empty-label series.
fn selftest(ext: &str, scratch: &Path) -> Result<(), String> {
    let data_dir = scratch.join("src_data");
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;
    let db_path = scratch.join("imported.db");
    let _ = std::fs::remove_file(&db_path);

    {
        let engine = Engine::new(data_dir.clone(), usize::MAX, 0, 3, 256 << 20, false)
            .map_err(|e| e.to_string())?;
        let mk = |pairs: &[(&str, &str)]| -> std::collections::HashMap<String, String> {
            pairs
                .iter()
                .map(|&(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        // Hostile labels: quotes, backslashes, newline, tab, unicode.
        let hostile = [
            mk(&[("host", "a\"b"), ("pa\\th", "c\nd")]),
            mk(&[("tab", "x\ty"), ("uni", "μ-héllo")]),
            mk(&[]),
        ];
        for (i, labels) in hostile.iter().enumerate() {
            let sid = engine
                .resolve_cached("hostile.metric", labels)
                .map_err(|e| e.to_string())?;
            engine.write_point(
                sid,
                -1_000 - i as i64,
                f64::from_bits(0x7ff8_dead_beef_0001),
            ); // NaN payload
            engine.write_point(sid, 0, -0.0);
            engine.write_point(sid, 5, 5.0);
            engine.write_point(sid, 5, 6.0); // duplicate ts
            engine.write_point(sid, 4_102_444_800, f64::MAX); // far future
        }
        // A multi-blob series (150k points → 2+ blobs).
        let big = engine
            .resolve_cached("big.metric", &mk(&[("host", "big")]))
            .map_err(|e| e.to_string())?;
        for i in 0..150_000i64 {
            engine.write_point(big, i * 3, (i as f64) * 0.25);
        }
        // An empty series: registered, never written — must be skipped.
        engine
            .resolve_cached("empty.metric", &mk(&[("host", "ghost")]))
            .map_err(|e| e.to_string())?;
        engine.flush_all().map_err(|e| e.to_string())?;
        engine.shutdown().map_err(|e| e.to_string())?;
    }

    run_import(&data_dir, ext, db_path.to_str().unwrap(), "metrics")?;

    // Belt and braces: independent SQL spot checks on the result.
    let conn = Connection::open(&db_path).map_err(|e| e.to_string())?;
    unsafe {
        conn.load_extension_enable().map_err(|e| e.to_string())?;
        conn.load_extension(ext, None::<&str>)
            .map_err(|e| e.to_string())?;
    }
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM metrics", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    let expect = 3 * 5 + 150_000;
    if n as usize != expect {
        return Err(format!("selftest: expected {expect} rows, got {n}"));
    }
    let cat: i64 = conn
        .query_row("SELECT COUNT(*) FROM timeless_series('metrics')", [], |r| {
            r.get(0)
        })
        .map_err(|e| e.to_string())?;
    if cat != 4 {
        return Err(format!("selftest: expected 4 catalog series, got {cat}"));
    }
    println!("- selftest OK: {n} rows, {cat} series, hostile labels round-tripped");
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let result = match args.get(1).map(String::as_str) {
        Some("--selftest") => {
            let (ext, scratch) = (args.get(2), args.get(3));
            match (ext, scratch) {
                (Some(ext), Some(scratch)) => selftest(ext, Path::new(scratch)),
                _ => Err("usage: import --selftest <extension> <scratch-dir>".into()),
            }
        }
        Some(dir) if args.len() >= 4 => run_import(
            Path::new(dir),
            &args[2],
            &args[3],
            args.get(4).map(String::as_str).unwrap_or("metrics"),
        ),
        _ => Err(
            "usage: import <fs-data-dir> <extension> <output.db> [table]\n       \
             import --selftest <extension> <scratch-dir>"
                .into(),
        ),
    };
    if let Err(e) = result {
        eprintln!("import: {e}");
        std::process::exit(1);
    }
}
