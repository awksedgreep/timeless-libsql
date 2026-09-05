//! codec8-bench: Phase-3 measurement for CODEC_RICH_TEMPLATE (8) —
//! CLP_PLAN.md. Where clp-vet PROJECTED the win by composing encoders
//! in-memory, this drives the REAL `encode_block`/`decode_block` on
//! rich entries built from the same corpora, codec 7 vs codec 8:
//! whole-block bytes/entry, encode/decode throughput, per-block
//! codec-byte outcomes (how often the fallback fired), and an exactness
//! assert on every block.
//!
//! Usage: codec8-bench <file.log|file.jsonl>...
//! (same corpora as clp-vet; RESULTS.md methodology — run twice, quote
//! the second run)

use std::time::Instant;

use timeless_core::blocks::{decode_block, encode_block, CODEC_RICH_COLUMNAR, CODEC_RICH_TEMPLATE};
use timeless_core::LogEntry;

const GROUP: usize = 8192; // the engine's merge_target_entries
const ZSTD_LEVEL: i32 = 7; // the engine's optimize() default

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: codec8-bench <file.log|file.jsonl>...");
        std::process::exit(2);
    }
    println!("| corpus | entries | c7 B/e | c8 B/e | ratio | c8 blocks | enc7 MB/s | enc8 MB/s | dec7 MB/s | dec8 MB/s |");
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for arg in &args {
        run(arg);
    }
}

fn run(path: &str) {
    let loaded = if path.ends_with(".jsonl") {
        load_journal_json(path)
    } else {
        load_plain(path)
    };
    // Canonicalize through one codec-7 round-trip: the decoder derives
    // the flat metadata pairs from the rich envelope, and the exactness
    // asserts below must compare canonical forms.
    let entries: Vec<LogEntry> = loaded
        .chunks(GROUP)
        .flat_map(|g| {
            let (bytes, _) = encode_block(g, CODEC_RICH_COLUMNAR, 1).unwrap();
            decode_block(&bytes).unwrap()
        })
        .collect();
    let name = std::path::Path::new(path)
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let raw_bytes: usize = entries.iter().map(|e| e.message.len()).sum();

    let mut totals = [0usize; 2]; // [codec7, codec8-requested]
    let mut enc_secs = [0f64; 2];
    let mut dec_secs = [0f64; 2];
    let mut c8_blocks = 0usize;
    let mut blocks = 0usize;
    for group in entries.chunks(GROUP) {
        blocks += 1;
        for (i, codec) in [CODEC_RICH_COLUMNAR, CODEC_RICH_TEMPLATE]
            .into_iter()
            .enumerate()
        {
            let t0 = Instant::now();
            let (bytes, meta) = encode_block(group, codec, ZSTD_LEVEL).unwrap();
            enc_secs[i] += t0.elapsed().as_secs_f64();
            totals[i] += bytes.len();
            if i == 1 && meta.codec == CODEC_RICH_TEMPLATE {
                c8_blocks += 1;
            }
            let t1 = Instant::now();
            let back = decode_block(&bytes).unwrap();
            dec_secs[i] += t1.elapsed().as_secs_f64();
            assert_eq!(back, group, "{name}: codec {codec} round-trip mismatch");
        }
    }

    let n = entries.len() as f64;
    let mbs = |secs: f64| raw_bytes as f64 / 1e6 / secs;
    println!(
        "| {name} | {} | {:.1} | {:.1} | {:.2}x | {c8_blocks}/{blocks} | {:.0} | {:.0} | {:.0} | {:.0} |",
        entries.len(),
        totals[0] as f64 / n,
        totals[1] as f64 / n,
        totals[0] as f64 / totals[1] as f64,
        mbs(enc_secs[0]),
        mbs(enc_secs[1]),
        mbs(dec_secs[0]),
        mbs(dec_secs[1]),
    );
}

/// Plain lines become rich entries with synthetic timestamps (1ms
/// cadence) and a fixed severity — the message column is the variable
/// under test; ts/level/envelope are identical for both codecs.
fn load_plain(path: &str) -> Vec<LogEntry> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let strip_prefix = path.ends_with("journal_ours.log");
    text.lines()
        .enumerate()
        .map(|(i, l)| {
            let msg = if strip_prefix {
                let mut it = l.splitn(3, ' ');
                match (it.next(), it.next(), it.next()) {
                    (Some(_), Some(_), Some(rest)) => rest.to_string(),
                    _ => l.to_string(),
                }
            } else {
                l.to_string()
            };
            LogEntry {
                ts: 1_785_600_000_000 + i as i64,
                level: 1,
                severity: Some("info".into()),
                message: msg,
                metadata: Vec::new(),
                metadata_json: Some("{}".into()),
            }
        })
        .collect()
}

/// journalctl -o json: real ts (µs), PRIORITY mapped onto the 4-level
/// scheme, the priority name as severity, and the remaining fields as
/// the canonical envelope — the rich four-column shape end to end.
fn load_journal_json(path: &str) -> Vec<LogEntry> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut out = Vec::new();
    for line in text.lines() {
        let v: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let obj = match v.as_object() {
            Some(o) => o,
            None => continue,
        };
        let msg = match obj.get("MESSAGE").and_then(|m| m.as_str()) {
            Some(m) => m.to_string(),
            None => continue,
        };
        let ts: i64 = obj
            .get("__REALTIME_TIMESTAMP")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let prio: u8 = obj
            .get("PRIORITY")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(6);
        let (level, severity) = match prio {
            0 => (3, "emergency"),
            1 => (3, "alert"),
            2 => (3, "critical"),
            3 => (3, "error"),
            4 => (2, "warning"),
            5 => (1, "notice"),
            6 => (1, "info"),
            _ => (0, "debug"),
        };
        let rest: std::collections::BTreeMap<&String, &serde_json::Value> = obj
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "MESSAGE" | "__REALTIME_TIMESTAMP" | "PRIORITY"))
            .collect();
        out.push(LogEntry {
            ts,
            level,
            severity: Some(severity.into()),
            message: msg,
            metadata: Vec::new(),
            metadata_json: Some(serde_json::to_string(&rest).unwrap()),
        });
    }
    // encode_block wants ts-sorted input (the engine sorts at flush).
    out.sort_by_key(|e| e.ts);
    out
}
