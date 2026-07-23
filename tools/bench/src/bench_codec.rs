//! bench-codec: the codec bake-off (PLAN.md "Codec strategy").
//! Session 7 ran codec 2 (zstd columnar) vs codec 4 (adaptive columnar
//! v1); Session 8 re-aims it at codec 4 vs codec 5 ("adaptive columnar
//! v2" — codec 4 plus per-key SHREDDED metadata/attributes), because 4
//! already beat 2 and the metadata/attributes column — which moved
//! 0.0% in Session 7 — is exactly what codec 5 attacks. The
//! metadata/attributes rows of the per-column tables are the headline.
//!
//!   bench-codec            (no arguments — no SQLite in the loop)
//!
//! Both codecs run over the EXACT datasets bench-logs and bench-traces
//! ingest (shared `datasets` module — same PRNG, same bytes).
//!
//! Methodology: the 1M-entry logs workload and the ~1M-span traces
//! workload are cut into 8192-entry level/status-partitioned groups,
//! mirroring what the engines' flush+optimize actually encode
//! (level/status-PURE blocks of ~merge_target_entries): per partition
//! (level for logs, status for spans), ts-ordered entries are chunked
//! into 8192-entry groups. Each group is encoded BOTH ways, decoded
//! back, and verified EXACT — a codec that wins on size but loses a
//! bit is not a codec, it's a bug.
//!
//! Reported per dataset:
//!   - per-column stored bytes for one representative group (the
//!     largest — an info/ok group) and summed over all groups,
//!   - total encoded bytes + ratio vs codec 4,
//!   - encode/decode throughput (MB/s over raw column bytes, and
//!     entries/s),
//!   - which strategy actually won each adaptive column (including
//!     shredded-vs-legacy for the pairs columns),
//!   - a verdict line against the Session 8 decision rule: codec 5
//!     becomes/stays the optimize() default only if total bytes
//!     improve ≥3% over codec 4 on either dataset (query-time
//!     regressions are checked by the end-to-end benches, not here).

mod datasets;

use std::time::Instant;

use timeless_core::blocks::{
    decode_block, encode_block, CODEC_COLUMNAR, CODEC_COLUMNAR_V2, CODEC_RAW,
};
use timeless_core::spans::{decode_span_block, encode_span_block};
use timeless_core::{LogEntry, SpanEntry};

const GROUP_ENTRIES: usize = 8192; // the engines' merge_target_entries
const ZSTD_LEVEL: i32 = 7; // the engines' zstd level

// ---------------------------------------------------------------------------
// Container header parsing (for per-column accounting). Layout is
// documented in blocks/codec.rs and spans/codec.rs: byte 1 = codec,
// bytes 22.. = n_cols u32 column lengths, then the columns.
// ---------------------------------------------------------------------------

/// Stored length of each column in an encoded block payload.
fn column_lens(bytes: &[u8], n_cols: usize) -> Vec<usize> {
    (0..n_cols)
        .map(|i| {
            let off = 22 + i * 4;
            u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize
        })
        .collect()
}

/// First byte of each column payload — for TYPED columns that is the
/// timeless-codec encoding id (strategy tag); for a codec-5
/// metadata/attributes column it is the pairs strategy byte
/// (0 = legacy, 1 = shredded). Meaningless for the unframed zstd
/// columns (parent ids, codec-4 pairs); the caller knows which
/// indexes to ask about.
fn column_strategy_ids(bytes: &[u8], n_cols: usize) -> Vec<u8> {
    let lens = column_lens(bytes, n_cols);
    let mut off = 22 + n_cols * 4;
    let mut ids = Vec::with_capacity(n_cols);
    for len in lens {
        ids.push(if len > 0 { bytes[off] } else { 0 });
        off += len;
    }
    ids
}

/// Pairs-column strategy byte (codec 5 metadata/attributes) to name.
fn pairs_strategy_name(id: u8) -> &'static str {
    match id {
        0 => "legacy",
        1 => "shredded",
        _ => "?",
    }
}

fn strategy_name(id: u8) -> &'static str {
    match id {
        timeless_codec_ids::I64_DELTA_PCO => "delta+pco",
        timeless_codec_ids::I64_DELTA_ZSTD => "delta+zstd",
        timeless_codec_ids::STR_DICT => "dictionary",
        timeless_codec_ids::STR_ZSTD => "concat+zstd",
        timeless_codec_ids::U8_RLE => "rle",
        timeless_codec_ids::U8_ZSTD => "zstd",
        timeless_codec_ids::FIXED_ZSTD => "zstd",
        _ => "?",
    }
}

/// The timeless-codec encoding ids, mirrored as literals: bench is a
/// detached workspace and pulling the whole codec crate in just for
/// seven constants isn't worth a second path dependency (timeless-core
/// re-exports the codec behavior we exercise; the ids are on-disk
/// stable by contract).
mod timeless_codec_ids {
    pub const I64_DELTA_PCO: u8 = 1;
    pub const I64_DELTA_ZSTD: u8 = 2;
    pub const STR_DICT: u8 = 5;
    pub const STR_ZSTD: u8 = 6;
    pub const U8_RLE: u8 = 7;
    pub const U8_ZSTD: u8 = 8;
    pub const FIXED_ZSTD: u8 = 9;
}

// ---------------------------------------------------------------------------
// Group construction: mirror the engines' flush/optimize partitioning.
// ---------------------------------------------------------------------------

/// Partition `items` by `part(item)`, sort each partition by `ts`, cut
/// into GROUP_ENTRIES chunks. This reproduces the steady-state block
/// population after optimize(): partition-pure, ts-ordered, ~8192
/// entries each.
fn partition_groups<T: Clone>(
    items: &[T],
    part: impl Fn(&T) -> u8,
    ts: impl Fn(&T) -> i64,
    n_parts: u8,
) -> Vec<Vec<T>> {
    let mut groups = Vec::new();
    for p in 0..n_parts {
        let mut bucket: Vec<T> = items.iter().filter(|e| part(e) == p).cloned().collect();
        bucket.sort_by_key(|e| ts(e));
        for chunk in bucket.chunks(GROUP_ENTRIES) {
            groups.push(chunk.to_vec());
        }
    }
    groups
}

// ---------------------------------------------------------------------------
// Measurement plumbing shared by both datasets.
// ---------------------------------------------------------------------------

struct CodecStats {
    total_bytes: usize,
    per_column: Vec<usize>,
    encode_secs: f64,
    decode_secs: f64,
}

impl CodecStats {
    fn new(n_cols: usize) -> Self {
        CodecStats {
            total_bytes: 0,
            per_column: vec![0; n_cols],
            encode_secs: 0.0,
            decode_secs: 0.0,
        }
    }
}

fn fmt_mb(b: usize) -> String {
    format!("{:.2} MB", b as f64 / 1.0e6)
}

fn report(
    label: &str,
    col_names: &[&str],
    rep_idx: usize,
    rep_len: usize,
    rep_v1: &[usize],
    rep_v2: &[usize],
    s_v1: &CodecStats,
    s_v2: &CodecStats,
    raw_bytes: usize,
    n_entries: usize,
    wins: &[(usize, Vec<(&'static str, usize)>)],
) {
    println!("\n### {label}: representative group #{rep_idx} ({rep_len} entries), per-column bytes\n");
    println!("| column | codec 4 | codec 5 | delta |");
    println!("|--------|---------|---------|-------|");
    for (i, name) in col_names.iter().enumerate() {
        println!(
            "| {name} | {} | {} | {:+.1}% |",
            rep_v1[i],
            rep_v2[i],
            (rep_v2[i] as f64 / rep_v1[i].max(1) as f64 - 1.0) * 100.0
        );
    }
    println!(
        "| **group total** | **{}** | **{}** | **{:+.1}%** |",
        rep_v1.iter().sum::<usize>(),
        rep_v2.iter().sum::<usize>(),
        (rep_v2.iter().sum::<usize>() as f64 / rep_v1.iter().sum::<usize>() as f64 - 1.0) * 100.0
    );

    println!("\n### {label}: all groups, per-column totals\n");
    println!("| column | codec 4 | codec 5 | delta | codec-5 strategy (groups) |");
    println!("|--------|---------|---------|-------|---------------------------|");
    for (i, name) in col_names.iter().enumerate() {
        let strat = wins
            .iter()
            .find(|(idx, _)| *idx == i)
            .map(|(_, counts)| {
                counts
                    .iter()
                    .map(|(name, cnt)| format!("{name} x{cnt}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "zstd (fixed)".into());
        println!(
            "| {name} | {} | {} | {:+.1}% | {strat} |",
            s_v1.per_column[i],
            s_v2.per_column[i],
            (s_v2.per_column[i] as f64 / s_v1.per_column[i].max(1) as f64 - 1.0) * 100.0
        );
    }
    println!(
        "| **total** | **{}** | **{}** | **{:+.1}%** | |",
        fmt_mb(s_v1.total_bytes),
        fmt_mb(s_v2.total_bytes),
        (s_v2.total_bytes as f64 / s_v1.total_bytes as f64 - 1.0) * 100.0
    );

    let mbs = raw_bytes as f64 / 1.0e6;
    println!("\n### {label}: throughput (raw column bytes: {})\n", fmt_mb(raw_bytes));
    println!("| codec | encode | decode |");
    println!("|-------|--------|--------|");
    for (name, s) in [("codec 4", s_v1), ("codec 5", s_v2)] {
        println!(
            "| {name} | {:.0} MB/s ({:.2}M entries/s) | {:.0} MB/s ({:.2}M entries/s) |",
            mbs / s.encode_secs,
            n_entries as f64 / s.encode_secs / 1.0e6,
            mbs / s.decode_secs,
            n_entries as f64 / s.decode_secs / 1.0e6,
        );
    }
}

/// Track which strategy won a typed column, per group (by NAME — the
/// pairs columns use a different id namespace than the typed columns,
/// so callers translate before tallying).
fn tally(wins: &mut Vec<(&'static str, usize)>, name: &'static str) {
    match wins.iter_mut().find(|(w, _)| *w == name) {
        Some((_, c)) => *c += 1,
        None => wins.push((name, 1)),
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    println!("# timeless bench-codec — codec 4 (adaptive columnar v1) vs codec 5 (v2: shredded metadata/attributes)");
    println!("\ngroups mirror engine flush/optimize: partition-pure, ts-ordered, ≤{GROUP_ENTRIES} entries");

    // ══ Logs ═══════════════════════════════════════════════════════
    let t = Instant::now();
    let logs = datasets::generate_logs();
    let entries: Vec<LogEntry> = logs
        .iter()
        .map(|r| LogEntry {
            ts: r.ts,
            level: r.level_num,
            message: r.message.clone(),
            // Canonical sorted pair order (path < service < status) —
            // exactly what the vtab stores after its JSON parse.
            metadata: vec![
                ("path".into(), r.path.into()),
                ("service".into(), r.service.into()),
                ("status".into(), r.status.into()),
            ],
        })
        .collect();
    drop(logs);
    println!(
        "\n- generated {} log entries in {:.1} ms",
        entries.len(),
        t.elapsed().as_secs_f64() * 1e3
    );

    let groups = partition_groups(&entries, |e| e.level, |e| e.ts, 4);
    drop(entries);
    let n_entries: usize = groups.iter().map(|g| g.len()).sum();
    println!("- {} level-pure groups", groups.len());

    const LOG_COLS: usize = 4;
    let log_col_names = ["ts", "level", "message", "metadata"];
    let mut s_v1 = CodecStats::new(LOG_COLS);
    let mut s_v2 = CodecStats::new(LOG_COLS);
    let mut raw_bytes = 0usize;
    // Strategy tallies: ts(0), level(1), message(2) are typed columns;
    // metadata(3) reports shredded-vs-legacy (codec 5's pairs byte).
    let mut ts_wins: Vec<(&'static str, usize)> = Vec::new();
    let mut lvl_wins: Vec<(&'static str, usize)> = Vec::new();
    let mut msg_wins: Vec<(&'static str, usize)> = Vec::new();
    let mut meta_wins: Vec<(&'static str, usize)> = Vec::new();

    // Representative group = the largest (a full 8192-entry info group).
    let rep_idx = (0..groups.len()).max_by_key(|&i| groups[i].len()).unwrap();
    let (mut rep_v1, mut rep_v2) = (vec![0usize; LOG_COLS], vec![0usize; LOG_COLS]);
    let mut rep_len = 0usize;

    for (gi, g) in groups.iter().enumerate() {
        // Raw baseline (codec 1 payload minus header) — the honest
        // "bytes in" figure for MB/s.
        let (raw, _) = encode_block(g, CODEC_RAW, ZSTD_LEVEL).unwrap();
        raw_bytes += raw.len() - 38;

        let t = Instant::now();
        let (b_v1, _) = encode_block(g, CODEC_COLUMNAR, ZSTD_LEVEL).unwrap();
        s_v1.encode_secs += t.elapsed().as_secs_f64();
        let t = Instant::now();
        let (b_v2, _) = encode_block(g, CODEC_COLUMNAR_V2, ZSTD_LEVEL).unwrap();
        s_v2.encode_secs += t.elapsed().as_secs_f64();

        let t = Instant::now();
        let d_v1 = decode_block(&b_v1).unwrap();
        s_v1.decode_secs += t.elapsed().as_secs_f64();
        let t = Instant::now();
        let d_v2 = decode_block(&b_v2).unwrap();
        s_v2.decode_secs += t.elapsed().as_secs_f64();
        assert_eq!(&d_v1, g, "codec 4 must round-trip exactly");
        assert_eq!(&d_v2, g, "codec 5 must round-trip exactly");

        s_v1.total_bytes += b_v1.len();
        s_v2.total_bytes += b_v2.len();
        let l_v1 = column_lens(&b_v1, LOG_COLS);
        let l_v2 = column_lens(&b_v2, LOG_COLS);
        for i in 0..LOG_COLS {
            s_v1.per_column[i] += l_v1[i];
            s_v2.per_column[i] += l_v2[i];
        }
        let ids = column_strategy_ids(&b_v2, LOG_COLS);
        tally(&mut ts_wins, strategy_name(ids[0]));
        tally(&mut lvl_wins, strategy_name(ids[1]));
        tally(&mut msg_wins, strategy_name(ids[2]));
        tally(&mut meta_wins, pairs_strategy_name(ids[3]));

        if gi == rep_idx {
            rep_v1 = l_v1;
            rep_v2 = l_v2;
            rep_len = g.len();
        }
    }

    let log_wins = vec![(0usize, ts_wins), (1, lvl_wins), (2, msg_wins), (3, meta_wins)];
    report(
        "logs", &log_col_names, rep_idx, rep_len, &rep_v1, &rep_v2, &s_v1, &s_v2, raw_bytes, n_entries,
        &log_wins,
    );
    let logs_improve = 1.0 - s_v2.total_bytes as f64 / s_v1.total_bytes as f64;

    // ══ Traces ═════════════════════════════════════════════════════
    let t = Instant::now();
    let recs = datasets::generate_traces();
    let spans: Vec<SpanEntry> = recs
        .iter()
        .map(|r| SpanEntry {
            trace_id: r.trace_id,
            span_id: r.span_id,
            parent_span_id: r.parent_span_id,
            name: r.name.into(),
            service: r.service.into(),
            kind: r.kind_num,
            status: r.status_num,
            start_ts: r.start_ts,
            duration_ns: r.duration_ns,
            // Canonical sorted pair order (http.method < http.status).
            attributes: vec![
                ("http.method".into(), r.http_method.into()),
                ("http.status".into(), r.http_status.into()),
            ],
        })
        .collect();
    let n_spans = spans.len();
    drop(recs);
    println!(
        "\n- generated {} spans in {:.1} ms",
        n_spans,
        t.elapsed().as_secs_f64() * 1e3
    );

    let groups = partition_groups(&spans, |s| s.status, |s| s.start_ts, 3);
    drop(spans);
    println!("- {} status-pure groups", groups.len());

    const SPAN_COLS: usize = 10;
    let span_col_names = [
        "trace_id", "span_id", "parent_id", "name", "service", "kind", "status", "start_ts",
        "duration", "attributes",
    ];
    let mut s_v1 = CodecStats::new(SPAN_COLS);
    let mut s_v2 = CodecStats::new(SPAN_COLS);
    let mut raw_bytes = 0usize;
    // Typed columns: name(3), service(4), kind(5), status(6),
    // start_ts(7), duration(8); attributes(9) reports codec 5's
    // shredded-vs-legacy pairs byte. Ids (0,1) are always fixed+zstd.
    let mut wins: Vec<(usize, Vec<(&'static str, usize)>)> =
        [3usize, 4, 5, 6, 7, 8, 9].iter().map(|&i| (i, Vec::new())).collect();

    let rep_idx = (0..groups.len()).max_by_key(|&i| groups[i].len()).unwrap();
    let (mut rep_v1, mut rep_v2) = (vec![0usize; SPAN_COLS], vec![0usize; SPAN_COLS]);
    let mut rep_len = 0usize;

    for (gi, g) in groups.iter().enumerate() {
        let (raw, _) = encode_span_block(g, CODEC_RAW, ZSTD_LEVEL).unwrap();
        raw_bytes += raw.len() - 62;

        let t = Instant::now();
        let (b_v1, _) = encode_span_block(g, CODEC_COLUMNAR, ZSTD_LEVEL).unwrap();
        s_v1.encode_secs += t.elapsed().as_secs_f64();
        let t = Instant::now();
        let (b_v2, _) = encode_span_block(g, CODEC_COLUMNAR_V2, ZSTD_LEVEL).unwrap();
        s_v2.encode_secs += t.elapsed().as_secs_f64();

        let t = Instant::now();
        let d_v1 = decode_span_block(&b_v1).unwrap();
        s_v1.decode_secs += t.elapsed().as_secs_f64();
        let t = Instant::now();
        let d_v2 = decode_span_block(&b_v2).unwrap();
        s_v2.decode_secs += t.elapsed().as_secs_f64();
        assert_eq!(&d_v1, g, "codec 4 must round-trip exactly");
        assert_eq!(&d_v2, g, "codec 5 must round-trip exactly");

        s_v1.total_bytes += b_v1.len();
        s_v2.total_bytes += b_v2.len();
        let l_v1 = column_lens(&b_v1, SPAN_COLS);
        let l_v2 = column_lens(&b_v2, SPAN_COLS);
        for i in 0..SPAN_COLS {
            s_v1.per_column[i] += l_v1[i];
            s_v2.per_column[i] += l_v2[i];
        }
        let ids = column_strategy_ids(&b_v2, SPAN_COLS);
        for (idx, tallies) in wins.iter_mut() {
            let name = if *idx == 9 {
                pairs_strategy_name(ids[*idx])
            } else {
                strategy_name(ids[*idx])
            };
            tally(tallies, name);
        }

        if gi == rep_idx {
            rep_v1 = l_v1;
            rep_v2 = l_v2;
            rep_len = g.len();
        }
    }

    report(
        "traces", &span_col_names, rep_idx, rep_len, &rep_v1, &rep_v2, &s_v1, &s_v2, raw_bytes, n_spans,
        &wins,
    );
    let traces_improve = 1.0 - s_v2.total_bytes as f64 / s_v1.total_bytes as f64;

    // ══ Verdict ════════════════════════════════════════════════════
    println!("\n## Verdict\n");
    println!(
        "- logs: codec 5 total is {:.2}% {} than codec 4",
        logs_improve.abs() * 100.0,
        if logs_improve >= 0.0 { "smaller" } else { "LARGER" }
    );
    println!(
        "- traces: codec 5 total is {:.2}% {} than codec 4",
        traces_improve.abs() * 100.0,
        if traces_improve >= 0.0 { "smaller" } else { "LARGER" }
    );
    let pass = logs_improve >= 0.03 || traces_improve >= 0.03;
    println!(
        "- decision rule (≥3% on either dataset): {}",
        if pass {
            "PASS — codec 5 stays the optimize() default (confirm no >20% query regression in bench-logs/bench-traces)"
        } else {
            "FAIL — revert optimize() to codec 4 (decode keeps speaking 5 either way)"
        }
    );
}
