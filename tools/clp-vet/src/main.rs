//! Phase-0 measurement harness for CLP-style template compression
//! (CLP_PLAN.md). No engine changes: corpora run through the tokenizer
//! and the PROJECTED codec-8 message column is sized by feeding the
//! outputs through the real timeless-codec encoders in-memory, against
//! the codec-7 baseline (`encode_str` over whole messages).
//!
//! Per corpus, per 8192-entry block (the engine's optimize group size),
//! and per tokenizer mode (digit-run split vs whole-token — see
//! tokenizer.rs, neither dominates):
//!   baseline   = encode_str(messages)
//!   v1         = encode_i64(template ids) + encode_str(template dict)
//!              + num vars (u8 width col + i64 value col) + str vars,
//!                streams in MESSAGE order
//!   v2-lite    = same columns, var streams reordered by (template,
//!                slot) so homogeneous values are adjacent
//!   v2-full    = one encoded column PER (template, slot) group
//!   fallback   = min(baseline, all candidates) — the per-block gate a
//!                real codec 8 would apply (variant byte per block)
//! and v1 + v2-lite are decoded back and asserted bit-exact per line.
//!
//! Usage: clp-vet <corpus...>   where a corpus is a *.log path, a
//! *.jsonl path (journalctl -o json; also sizes the whole rich block),
//! or gen:unique-ids / gen:random-text (deterministic adversarial).
//!
//! NOTE: this tool keeps its own copy of the tokenizer (with per-mode
//! instrumentation and shape stats the production code doesn't need).
//! The shipping implementation is `timeless-core/src/blocks/template.rs`
//! (codec 8); change THAT one, and use this harness to re-measure.

mod tokenizer;

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use timeless_codec::{
    decode_i64, decode_str, decode_u8, encode_i64, encode_str, encode_u8, zstd_compress,
};
use tokenizer::{
    classify_shape, detokenize, slot_kinds, tokenize, tokenize_whole, Var, VarKind, VarShape,
};

const BLOCK: usize = 8192;
const ZSTD_LEVEL: i32 = 7; // the engine's optimize() default

#[derive(Default)]
struct CorpusStats {
    entries: usize,
    msg_bytes: usize,
    blocks: usize,
    base_bytes: usize,
    // Per-mode totals: [0] = digit-run split, [1] = whole-token.
    v1_bytes: [usize; 2],
    v2lite_bytes: [usize; 2],
    v2full_bytes: [usize; 2],
    fallback_bytes: usize,
    win_base: usize,  // blocks where codec 7 kept the message column
    win_split: usize, // blocks won by the digit-run tokenizer
    win_whole: usize, // blocks won by the whole-token tokenizer
    // Structure stats, recorded from the digit-run mode.
    templates_per_block: usize,
    hit_msgs: usize,
    vars: usize,
    num_vars: usize,
    shapes: HashMap<VarShape, usize>,
    tok_secs: f64,
    base_enc_secs: f64,
    cand_enc_secs: f64,
    // jsonl only: bytes shared by both sides (ts + level + envelope columns)
    fixed_bytes: usize,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: clp-vet <file.log|file.jsonl|gen:unique-ids|gen:random-text>...");
        std::process::exit(2);
    }

    let mut rows = Vec::new();
    for arg in &args {
        let (name, corpus) = load_corpus(arg);
        eprintln!(
            "== {name}: {} entries, {:.1} MB of message text",
            corpus.messages.len(),
            corpus.messages.iter().map(String::len).sum::<usize>() as f64 / 1e6
        );
        let stats = measure(&corpus);
        report(&name, &stats);
        rows.push((name, stats));
    }
    summary_table(&rows);
}

struct Corpus {
    messages: Vec<String>,
    /// jsonl only: (ts µs, priority) per entry + canonical envelope JSON.
    rich: Option<(Vec<i64>, Vec<u8>, Vec<String>)>,
}

fn load_corpus(arg: &str) -> (String, Corpus) {
    match arg {
        "gen:unique-ids" => (
            "adv-unique-ids".into(),
            Corpus {
                messages: gen_unique_ids(100_000),
                rich: None,
            },
        ),
        "gen:random-text" => (
            "adv-random-text".into(),
            Corpus {
                messages: gen_random_text(100_000),
                rich: None,
            },
        ),
        path if path.ends_with(".jsonl") => {
            (format!("{}-rich", stem(path)), load_journal_json(path))
        }
        path => (stem(path), load_plain(path)),
    }
}

fn stem(path: &str) -> String {
    std::path::Path::new(path)
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

fn load_plain(path: &str) -> Corpus {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    // journal short-iso lines carry "<ts> <host> unit[pid]: msg"; the
    // engine stores ts separately, so strip the first two fields there.
    // Loghub sets are measured as full lines (their timestamps become
    // ordinary variables — conservative for the candidate).
    let strip_prefix = path.ends_with("journal_ours.log");
    let messages = text
        .lines()
        .map(|l| {
            if strip_prefix {
                let mut it = l.splitn(3, ' ');
                match (it.next(), it.next(), it.next()) {
                    (Some(_), Some(_), Some(rest)) => rest.to_string(),
                    _ => l.to_string(),
                }
            } else {
                l.to_string()
            }
        })
        .collect();
    Corpus {
        messages,
        rich: None,
    }
}

fn load_journal_json(path: &str) -> Corpus {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut messages = Vec::new();
    let mut ts = Vec::new();
    let mut levels = Vec::new();
    let mut envelopes = Vec::new();
    let mut skipped = 0usize;
    for line in text.lines() {
        let v: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let obj = match v.as_object() {
            Some(o) => o,
            None => {
                skipped += 1;
                continue;
            }
        };
        // Non-UTF8 journal messages arrive as byte arrays — skip those.
        let msg = match obj.get("MESSAGE").and_then(|m| m.as_str()) {
            Some(m) => m.to_string(),
            None => {
                skipped += 1;
                continue;
            }
        };
        let t: i64 = obj
            .get("__REALTIME_TIMESTAMP")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let p: u8 = obj
            .get("PRIORITY")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(6);
        // Canonical envelope: every other field, key-sorted, compact.
        let rest: BTreeMap<&String, &serde_json::Value> = obj
            .iter()
            .filter(|(k, _)| {
                !matches!(k.as_str(), "MESSAGE" | "__REALTIME_TIMESTAMP" | "PRIORITY")
            })
            .collect();
        envelopes.push(serde_json::to_string(&rest).unwrap());
        messages.push(msg);
        ts.push(t);
        levels.push(p);
    }
    if skipped > 0 {
        eprintln!("   (journal jsonl: skipped {skipped} non-string-MESSAGE lines)");
    }
    Corpus {
        messages,
        rich: Some((ts, levels, envelopes)),
    }
}

// ---------------------------------------------------------------------------
// Deterministic adversarial corpora (seeded xorshift — no external files,
// same bytes every run).
// ---------------------------------------------------------------------------

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const HEX: &[u8] = b"0123456789abcdef";

/// One stable template, every variable unique + high-entropy: the
/// template dict stays tiny but the variable stream is incompressible.
fn gen_unique_ids(n: usize) -> Vec<String> {
    let mut rng = Rng(0xc15c0dec0de);
    (0..n)
        .map(|_| {
            let uuid: String = (0..32)
                .map(|i| {
                    let c = HEX[rng.below(16) as usize] as char;
                    if matches!(i, 8 | 12 | 16 | 20) {
                        format!("-{c}")
                    } else {
                        c.to_string()
                    }
                })
                .collect();
            let sess: String = (0..16).map(|_| HEX[rng.below(16) as usize] as char).collect();
            format!(
                "request {uuid} from session {sess} completed in {} ms",
                rng.below(100_000)
            )
        })
        .collect()
}

/// Near-unique TEMPLATES: random word soup, so the template dictionary
/// balloons and the per-block fallback must fire.
fn gen_random_text(n: usize) -> Vec<String> {
    let mut rng = Rng(0xbadc0ffee);
    (0..n)
        .map(|_| {
            let words = 6 + rng.below(10);
            (0..words)
                .map(|_| {
                    if rng.below(5) == 0 {
                        rng.below(1_000_000).to_string()
                    } else {
                        let len = 3 + rng.below(7);
                        (0..len)
                            .map(|_| (b'a' + rng.below(26) as u8) as char)
                            .collect()
                    }
                })
                .collect::<Vec<String>>()
                .join(" ")
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

fn measure(corpus: &Corpus) -> CorpusStats {
    let msgs = &corpus.messages;
    let mut s = CorpusStats {
        entries: msgs.len(),
        msg_bytes: msgs.iter().map(String::len).sum(),
        ..Default::default()
    };

    // Tokenizer-only throughput pass (the optimize() regression risk).
    // Timed on the digit-run mode; whole-token mode does strictly less.
    let t0 = Instant::now();
    let mut sink = 0usize;
    for m in msgs {
        let (t, v) = tokenize(m);
        sink += t.len() + v.len();
    }
    std::hint::black_box(sink);
    s.tok_secs = t0.elapsed().as_secs_f64();

    for (bi, block) in msgs.chunks(BLOCK).enumerate() {
        measure_block(block, &mut s);
        // jsonl: size the columns both sides share, once per block.
        if let Some((ts, levels, envelopes)) = &corpus.rich {
            let lo = bi * BLOCK;
            let hi = (lo + block.len()).min(ts.len());
            let ts_col = encode_i64(&ts[lo..hi], ZSTD_LEVEL).unwrap();
            let lv_col = encode_u8(&levels[lo..hi], ZSTD_LEVEL).unwrap();
            let mut env_raw = Vec::new();
            for e in &envelopes[lo..hi] {
                env_raw.extend_from_slice(&(e.len() as u32).to_le_bytes());
                env_raw.extend_from_slice(e.as_bytes());
            }
            let env_col = zstd_compress(&env_raw, ZSTD_LEVEL).unwrap();
            s.fixed_bytes += ts_col.encoded_len() + lv_col.encoded_len() + env_col.len();
        }
    }
    s
}

fn num_streams<'a>(vars: impl Iterator<Item = &'a Var<'a>>) -> (Vec<u8>, Vec<i64>, Vec<&'a str>) {
    let mut widths = Vec::new();
    let mut values = Vec::new();
    let mut strs = Vec::new();
    for v in vars {
        match v.kind {
            VarKind::Num => {
                widths.push(v.text.len() as u8);
                values.push(v.text.parse().unwrap());
            }
            VarKind::Str => strs.push(v.text),
        }
    }
    (widths, values, strs)
}

struct ModeSizes {
    v1: usize,
    v2lite: usize,
    v2full: usize,
    n_templates: usize,
    hit_msgs: usize,
}

fn measure_block(block: &[String], s: &mut CorpusStats) {
    let n = block.len();
    s.blocks += 1;

    // Baseline: exactly what codec 7 does to the message column.
    let t0 = Instant::now();
    let base = encode_str(block.iter().map(String::as_str), n, ZSTD_LEVEL).unwrap();
    s.base_enc_secs += t0.elapsed().as_secs_f64();
    let base_len = base.encoded_len();
    s.base_bytes += base_len;

    let split = measure_mode(block, true, Some(s));
    let whole = measure_mode(block, false, None);

    s.v1_bytes[0] += split.v1;
    s.v2lite_bytes[0] += split.v2lite;
    s.v2full_bytes[0] += split.v2full;
    s.v1_bytes[1] += whole.v1;
    s.v2lite_bytes[1] += whole.v2lite;
    s.v2full_bytes[1] += whole.v2full;
    s.templates_per_block += split.n_templates;
    s.hit_msgs += split.hit_msgs;

    // The per-block gate a real codec 8 would apply: try both tokenizer
    // modes (v1 + v2-lite layouts — v2-full lost everywhere), keep the
    // best, fall back to codec 7 if it still wins.
    let best_split = split.v1.min(split.v2lite);
    let best_whole = whole.v1.min(whole.v2lite);
    let best = best_split.min(best_whole);
    if best < base_len {
        s.fallback_bytes += best;
        if best_split <= best_whole {
            s.win_split += 1;
        } else {
            s.win_whole += 1;
        }
    } else {
        s.fallback_bytes += base_len;
        s.win_base += 1;
    }
}

/// Measure one tokenizer mode over one block: build all three layouts,
/// round-trip v1 and v2-lite through the DECODED columns bit-exact, and
/// return the projected sizes.
fn measure_mode(block: &[String], split_runs: bool, mut stats: Option<&mut CorpusStats>) -> ModeSizes {
    let n = block.len();
    let t1 = Instant::now();
    let mut dict_map: HashMap<String, i64> = HashMap::new();
    let mut dict: Vec<String> = Vec::new();
    let mut ids: Vec<i64> = Vec::with_capacity(n);
    let mut msg_vars: Vec<Vec<Var>> = Vec::with_capacity(n);
    for m in block {
        let (template, vars) = if split_runs {
            tokenize(m)
        } else {
            tokenize_whole(m)
        };
        if let Some(s) = stats.as_deref_mut() {
            for v in &vars {
                s.vars += 1;
                *s.shapes.entry(classify_shape(v)).or_default() += 1;
                if v.kind == VarKind::Num {
                    s.num_vars += 1;
                }
            }
        }
        let id = match dict_map.get(&template) {
            Some(&id) => id,
            None => {
                let id = dict.len() as i64;
                dict.push(template.clone());
                dict_map.insert(template, id);
                id
            }
        };
        ids.push(id);
        msg_vars.push(vars);
    }

    let ids_col = encode_i64(&ids, ZSTD_LEVEL).unwrap();
    let dict_col = encode_str(dict.iter().map(String::as_str), dict.len(), ZSTD_LEVEL).unwrap();
    let shared = ids_col.encoded_len() + dict_col.encoded_len();

    // v1: var streams in message order.
    let (w1, vals1, str1) = num_streams(msg_vars.iter().flatten());
    let w1_col = encode_u8(&w1, ZSTD_LEVEL).unwrap();
    let vals1_col = encode_i64(&vals1, ZSTD_LEVEL).unwrap();
    let str1_col = encode_str(str1.iter().copied(), str1.len(), ZSTD_LEVEL).unwrap();
    // +20: a real codec 8 must frame the five sub-column lengths.
    let v1_len =
        shared + w1_col.encoded_len() + vals1_col.encoded_len() + str1_col.encoded_len() + 20;
    if let Some(s) = stats.as_deref_mut() {
        s.cand_enc_secs += t1.elapsed().as_secs_f64();
    }

    // v2: group variables by (template id, slot). Same template slot ==
    // same rule kind and usually the same real-world field (pid, port,
    // path...), so grouped values are homogeneous — the codec-5
    // shredding insight applied to message variables. Decode needs no
    // permutation: the id column replays messages in order and each
    // message consumes the next value of each of its (template, slot)
    // groups.
    let mut groups: BTreeMap<(i64, u16), Vec<Var>> = BTreeMap::new();
    for (mi, &id) in ids.iter().enumerate() {
        for (slot, v) in msg_vars[mi].iter().enumerate() {
            groups.entry((id, slot as u16)).or_default().push(*v);
        }
    }
    // v2-lite: ONE set of typed columns, values laid out group by group
    // (no per-group framing; pco/zstd see homogeneous runs back to back).
    // v2-full: every group encoded as its own column (+4 bytes framing).
    let mut w2: Vec<u8> = Vec::with_capacity(w1.len());
    let mut vals2: Vec<i64> = Vec::with_capacity(vals1.len());
    let mut str2: Vec<&str> = Vec::with_capacity(str1.len());
    let mut v2full = 0usize;
    for vs in groups.values() {
        let (gw, gv, gs) = num_streams(vs.iter());
        match vs[0].kind {
            VarKind::Num => {
                v2full += encode_u8(&gw, ZSTD_LEVEL).unwrap().encoded_len()
                    + encode_i64(&gv, ZSTD_LEVEL).unwrap().encoded_len()
                    + 4;
            }
            VarKind::Str => {
                v2full += encode_str(gs.iter().copied(), gs.len(), ZSTD_LEVEL)
                    .unwrap()
                    .encoded_len()
                    + 4;
            }
        }
        w2.extend_from_slice(&gw);
        vals2.extend_from_slice(&gv);
        str2.extend_from_slice(&gs);
    }
    let w2_col = encode_u8(&w2, ZSTD_LEVEL).unwrap();
    let vals2_col = encode_i64(&vals2, ZSTD_LEVEL).unwrap();
    let str2_col = encode_str(str2.iter().copied(), str2.len(), ZSTD_LEVEL).unwrap();
    let v2lite_len =
        shared + w2_col.encoded_len() + vals2_col.encoded_len() + str2_col.encoded_len() + 20;
    let v2full_len = shared + v2full + 20;

    let mut counts: HashMap<i64, usize> = HashMap::new();
    for &id in &ids {
        *counts.entry(id).or_default() += 1;
    }
    let hit_msgs = ids.iter().filter(|id| counts[id] >= 2).count();

    // Round-trip v1 THROUGH THE DECODED COLUMNS, asserted bit-exact for
    // every message — the Phase-0 correctness backbone.
    let ids_dec = decode_i64(&ids_col.to_bytes(), n).unwrap();
    let dict_dec = decode_str(&dict_col.to_bytes(), dict.len()).unwrap();
    {
        let w_dec = decode_u8(&w1_col.to_bytes(), w1.len()).unwrap();
        let v_dec = decode_i64(&vals1_col.to_bytes(), vals1.len()).unwrap();
        let s_dec = decode_str(&str1_col.to_bytes(), str1.len()).unwrap();
        let (mut nc, mut sc) = (0usize, 0usize);
        for (m, &id) in block.iter().zip(&ids_dec) {
            let back = detokenize(&dict_dec[id as usize], &w_dec, &v_dec, &mut nc, &s_dec, &mut sc)
                .unwrap();
            assert_eq!(&back, m, "v1 round-trip mismatch");
        }
        assert_eq!(nc, v_dec.len(), "unconsumed num vars");
        assert_eq!(sc, s_dec.len(), "unconsumed string vars");
    }

    // Round-trip v2-lite: rebuild group extents from the id column +
    // template slot kinds alone, then splice per message.
    {
        let w_dec = decode_u8(&w2_col.to_bytes(), w2.len()).unwrap();
        let v_dec = decode_i64(&vals2_col.to_bytes(), vals2.len()).unwrap();
        let s_dec = decode_str(&str2_col.to_bytes(), str2.len()).unwrap();
        let kinds_by_id: Vec<Vec<VarKind>> = dict_dec.iter().map(|t| slot_kinds(t)).collect();
        let mut sizes: BTreeMap<(i64, u16), usize> = BTreeMap::new();
        for &id in &ids_dec {
            for slot in 0..kinds_by_id[id as usize].len() {
                *sizes.entry((id, slot as u16)).or_default() += 1;
            }
        }
        // Group cursors: offset of each group inside its typed columns.
        let mut cursors: HashMap<(i64, u16), usize> = HashMap::new();
        let (mut num_off, mut str_off) = (0usize, 0usize);
        for (&(id, slot), &count) in &sizes {
            match kinds_by_id[id as usize][slot as usize] {
                VarKind::Num => {
                    cursors.insert((id, slot), num_off);
                    num_off += count;
                }
                VarKind::Str => {
                    cursors.insert((id, slot), str_off);
                    str_off += count;
                }
            }
        }
        for (m, &id) in block.iter().zip(&ids_dec) {
            let template = &dict_dec[id as usize];
            let kinds = &kinds_by_id[id as usize];
            let mut widths_m: Vec<u8> = Vec::new();
            let mut vals_m: Vec<i64> = Vec::new();
            let mut strs_m: Vec<String> = Vec::new();
            for (slot, kind) in kinds.iter().enumerate() {
                let c = cursors.get_mut(&(id, slot as u16)).unwrap();
                match kind {
                    VarKind::Num => {
                        widths_m.push(w_dec[*c]);
                        vals_m.push(v_dec[*c]);
                    }
                    VarKind::Str => strs_m.push(s_dec[*c].clone()),
                }
                *c += 1;
            }
            let (mut nc, mut sc) = (0usize, 0usize);
            let back =
                detokenize(template, &widths_m, &vals_m, &mut nc, &strs_m, &mut sc).unwrap();
            assert_eq!(&back, m, "v2-lite round-trip mismatch");
        }
    }

    ModeSizes {
        v1: v1_len,
        v2lite: v2lite_len,
        v2full: v2full_len,
        n_templates: dict.len(),
        hit_msgs,
    }
}

fn report(name: &str, s: &CorpusStats) {
    let be = |b: usize| b as f64 / s.entries as f64;
    let x = |b: usize| s.base_bytes as f64 / b as f64;
    println!("\n### {name}");
    println!(
        "entries {} | blocks {} | msg bytes {:.2} MB | baseline {:.1} B/e",
        s.entries,
        s.blocks,
        s.msg_bytes as f64 / 1e6,
        be(s.base_bytes)
    );
    println!(
        "  digit-run:   v1 {:.1} B/e (x{:.2}) | v2-lite {:.1} B/e (x{:.2}) | v2-full {:.1} B/e (x{:.2})",
        be(s.v1_bytes[0]),
        x(s.v1_bytes[0]),
        be(s.v2lite_bytes[0]),
        x(s.v2lite_bytes[0]),
        be(s.v2full_bytes[0]),
        x(s.v2full_bytes[0])
    );
    println!(
        "  whole-token: v1 {:.1} B/e (x{:.2}) | v2-lite {:.1} B/e (x{:.2}) | v2-full {:.1} B/e (x{:.2})",
        be(s.v1_bytes[1]),
        x(s.v1_bytes[1]),
        be(s.v2lite_bytes[1]),
        x(s.v2lite_bytes[1]),
        be(s.v2full_bytes[1]),
        x(s.v2full_bytes[1])
    );
    println!(
        "  per-block best: {:.1} B/e (x{:.2}) — wins: split {} | whole {} | codec-7 fallback {}",
        be(s.fallback_bytes),
        x(s.fallback_bytes),
        s.win_split,
        s.win_whole,
        s.win_base
    );
    if s.fixed_bytes > 0 {
        let whole_base = s.fixed_bytes + s.base_bytes;
        let whole_cand = s.fixed_bytes + s.fallback_bytes;
        println!(
            "  whole rich block: {:.1} -> {:.1} B/e (x{:.3}; envelope+ts+level = {:.1} B/e)",
            be(whole_base),
            be(whole_cand),
            whole_base as f64 / whole_cand as f64,
            be(s.fixed_bytes)
        );
    }
    println!(
        "templates: {:.0} avg/block | hit rate {:.1}% | vars/msg {:.2} | num-vars {:.1}%  (digit-run mode)",
        s.templates_per_block as f64 / s.blocks as f64,
        100.0 * s.hit_msgs as f64 / s.entries as f64,
        s.vars as f64 / s.entries as f64,
        100.0 * s.num_vars as f64 / s.vars.max(1) as f64
    );
    let mut shapes: Vec<_> = s.shapes.iter().collect();
    shapes.sort();
    let shape_str: Vec<String> = shapes
        .iter()
        .map(|(k, v)| format!("{k:?} {:.1}%", 100.0 * **v as f64 / s.vars.max(1) as f64))
        .collect();
    println!("  var shapes: {}", shape_str.join(" | "));
    println!(
        "tokenizer: {:.0} MB/s, {:.2} M entries/s | encode: baseline {:.0} ms, tokenize+v1 {:.0} ms",
        s.msg_bytes as f64 / 1e6 / s.tok_secs,
        s.entries as f64 / 1e6 / s.tok_secs,
        s.base_enc_secs * 1e3,
        s.cand_enc_secs * 1e3
    );
}

fn summary_table(rows: &[(String, CorpusStats)]) {
    println!("\n\n| corpus | entries | base B/e | best B/e | ratio | v2lite-split | v2lite-whole | wins s/w/base | tok MB/s | hit% | vars/msg | num-var% |");
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    for (name, s) in rows {
        println!(
            "| {name} | {} | {:.1} | {:.1} | {:.2}x | {:.1} | {:.1} | {}/{}/{} | {:.0} | {:.1}% | {:.2} | {:.1}% |",
            s.entries,
            s.base_bytes as f64 / s.entries as f64,
            s.fallback_bytes as f64 / s.entries as f64,
            s.base_bytes as f64 / s.fallback_bytes as f64,
            s.v2lite_bytes[0] as f64 / s.entries as f64,
            s.v2lite_bytes[1] as f64 / s.entries as f64,
            s.win_split,
            s.win_whole,
            s.win_base,
            s.msg_bytes as f64 / 1e6 / s.tok_secs,
            100.0 * s.hit_msgs as f64 / s.entries as f64,
            s.vars as f64 / s.entries as f64,
            100.0 * s.num_vars as f64 / s.vars.max(1) as f64
        );
    }
}
