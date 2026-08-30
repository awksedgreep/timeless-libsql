//! CLP-style template compression for log message columns (CLP_PLAN.md).
//!
//! A message like `user 4821 logged in from 10.0.0.5` splits into a
//! reusable TEMPLATE (`user ␑d logged in from ␑d.␑d.␑d.␑d`) plus typed
//! VARIABLES. Templates dedup into a per-block dictionary; numeric
//! variables ride the same pco/zstd typed columns as everything else.
//! Phase 0 (tools/clp-vet) measured the design in here against the
//! codec-7 baseline before any of it was built — see the plan's
//! decision log for the numbers and the layout rationale.
//!
//! The tokenizer is CLP's core heuristic — digits are variables, words
//! are template text — as deterministic, stateless rules: no training,
//! no similarity tree, no persisted model. Two variable kinds:
//!
//!   Num  a run of decimal DIGITS, stored as (u8 width, i64 value).
//!        Width restores zero padding exactly, so "07" and "11" share
//!        one template slot ("2017-07-01" and "2017-11-23" produce the
//!        SAME template) while values ride the pco-friendly i64 column.
//!        Signs and separators stay in the template, so values are
//!        always non-negative.
//!   Str  a whole token kept verbatim: hex-ish runs and UUIDs (their
//!        digit/letter skeleton differs per instance — splitting them
//!        would mint a fresh template per id), digit runs too long for
//!        i64 / wider than 255, and (in whole-token mode) any other
//!        digit-bearing token.
//!
//! Two tokenizer MODES, because Phase 0 showed neither dominates:
//! digit-run (shred digit runs inside tokens — wins on id/size-heavy
//! blocks) vs whole-token (keep each digit-bearing token as one
//! variable — wins on timestamp-heavy blocks, which dictionary-dedup).
//! [`template::encode_template_str`] trial-encodes both and keeps the smaller,
//! the same measure-don't-guess pick every encoder in this crate makes.
//!
//! Variable layout is "v2-lite": streams ordered by (template, slot) so
//! each slot's values sit adjacent (a slot is usually one real-world
//! field — pid, port, size), but encoded as ONE column per type — no
//! per-group framing. No permutation is stored: decode replays the
//! template-id column in message order and pops each message's next
//! value from its (template, slot) group cursor; group extents are
//! derivable from the id column + per-template slot kinds alone.
//!
//! LOSSLESS BY CONSTRUCTION: a Str variable is the exact original
//! substring; a Num variable re-renders as its value zero-padded to its
//! recorded width, byte-identical to the digits it came from. The
//! round-trip is asserted per-line by tests over every corpus the
//! Phase-0 harness runs.

use std::collections::{BTreeMap, HashMap};

use timeless_codec::{
    decode_i64, decode_str, decode_u8, encode_i64, encode_str, encode_u8, Reader,
};

/// Placeholder sentinel inside templates. 0x11 (DC1) never appears in
/// real log text; when it does, it escapes as a doubled sentinel.
const SENTINEL: u8 = 0x11;
/// Sentinel + 'd': next variable is (width, value) from the num columns.
const KIND_NUM: u8 = b'd';
/// Sentinel + 's': next variable comes from the string column.
const KIND_STR: u8 = b's';

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum VarKind {
    Num,
    Str,
}

#[derive(Clone, Copy, Debug)]
struct Var<'a> {
    kind: VarKind,
    text: &'a str,
}

/// Characters that can be part of a variable token. Broad on purpose:
/// '.' ':' '-' '/' '@' '+' glue IPv4:port, ISO timestamps, UUIDs,
/// paths and block ids into ONE token instead of shredding them into
/// many. A token only produces variables if it contains a digit, so
/// words, plain paths and "key:" prefixes stay template text.
#[inline]
fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-' | b'+' | b'/' | b'@')
}

/// Tokens whose digit-run skeleton is UNSTABLE across instances — pure
/// hex runs and UUIDs — are kept whole as Str variables in both modes.
fn is_hex_like(tok: &str) -> bool {
    let body = tok.strip_prefix("0x").unwrap_or(tok);
    if body.len() >= 4
        && body.bytes().all(|b| b.is_ascii_hexdigit())
        && body.bytes().any(|b| b.is_ascii_alphabetic())
    {
        return true;
    }
    if tok.len() >= 4 && tok.starts_with("0x") {
        return true;
    }
    // UUID: 8-4-4-4-12 hex groups.
    let groups: Vec<&str> = tok.split('-').collect();
    groups.len() == 5
        && [8, 4, 4, 4, 12]
            .iter()
            .zip(&groups)
            .all(|(n, g)| g.len() == *n && g.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// A digit run rides the typed num columns when its value fits an i64
/// and its width fits the u8 width column. (A 19-digit HDFS-style block
/// id fits; a 20-digit overflow or a 300-zero pathology stays a string.)
fn num_run_ok(run: &str) -> bool {
    run.len() <= 255 && run.parse::<i64>().is_ok()
}

/// Split `msg` into a template (placeholders inline) and the ordered
/// variable list. Deterministic and stateless.
fn tokenize(msg: &str, split_runs: bool) -> (String, Vec<Var<'_>>) {
    let bytes = msg.as_bytes();
    let mut template = String::with_capacity(msg.len());
    let mut vars = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == SENTINEL {
            template.push(SENTINEL as char);
            template.push(SENTINEL as char);
            i += 1;
        } else if is_token_byte(b) {
            let start = i;
            while i < bytes.len() && is_token_byte(bytes[i]) {
                i += 1;
            }
            let tok = &msg[start..i];
            if !tok.bytes().any(|c| c.is_ascii_digit()) {
                template.push_str(tok);
            } else if is_hex_like(tok) || (!split_runs && !tok.bytes().all(|c| c.is_ascii_digit()))
            {
                push_var(&mut template, &mut vars, VarKind::Str, tok);
            } else if !split_runs {
                // Whole-token mode, pure digit run: the width column
                // restores padding, so it can still ride the num columns.
                let kind = if num_run_ok(tok) {
                    VarKind::Num
                } else {
                    VarKind::Str
                };
                push_var(&mut template, &mut vars, kind, tok);
            } else {
                // Digit-run mode: runs of digits become variables, the
                // rest of the token is stable skeleton in the template.
                let tb = tok.as_bytes();
                let mut j = 0;
                while j < tb.len() {
                    if tb[j].is_ascii_digit() {
                        let rs = j;
                        while j < tb.len() && tb[j].is_ascii_digit() {
                            j += 1;
                        }
                        let run = &tok[rs..j];
                        let kind = if num_run_ok(run) {
                            VarKind::Num
                        } else {
                            VarKind::Str
                        };
                        push_var(&mut template, &mut vars, kind, run);
                    } else {
                        template.push(tb[j] as char);
                        j += 1;
                    }
                }
            }
        } else {
            // Non-token byte: copy the whole (possibly multi-byte) char
            // verbatim. Non-ASCII never starts a token, so char
            // boundaries are respected by construction.
            let ch_len = utf8_char_len(b);
            let end = (i + ch_len).min(bytes.len());
            template.push_str(&msg[i..end]);
            i = end;
        }
    }
    (template, vars)
}

fn push_var<'a>(template: &mut String, vars: &mut Vec<Var<'a>>, kind: VarKind, text: &'a str) {
    template.push(SENTINEL as char);
    template.push(match kind {
        VarKind::Num => KIND_NUM,
        VarKind::Str => KIND_STR,
    } as char);
    vars.push(Var { kind, text });
}

#[inline]
fn utf8_char_len(lead: u8) -> usize {
    match lead {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

/// Per-slot kinds in placeholder order. A template fully determines the
/// kind of every slot — decode rebuilds group sizes from this + the id
/// column alone.
fn slot_kinds(template: &str) -> Vec<VarKind> {
    let bytes = template.as_bytes();
    let mut kinds = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == SENTINEL {
            match bytes.get(i + 1) {
                Some(&KIND_NUM) => kinds.push(VarKind::Num),
                Some(&KIND_STR) => kinds.push(VarKind::Str),
                _ => {}
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    kinds
}

/// Exact inverse of [`tokenize`]: splice variables back into the
/// template, consuming one (width, value) per Num slot and one string
/// per Str slot from the supplied per-message lists.
fn detokenize(
    template: &str,
    widths: &[u8],
    values: &[i64],
    strs: &[&str],
) -> Result<String, String> {
    let bytes = template.as_bytes();
    let mut out = String::with_capacity(template.len() * 2);
    let (mut nc, mut sc) = (0usize, 0usize);
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == SENTINEL {
            let tag = *bytes
                .get(i + 1)
                .ok_or("template column: dangling sentinel in template")?;
            match tag {
                SENTINEL => out.push(SENTINEL as char),
                KIND_NUM => {
                    let w = *widths
                        .get(nc)
                        .ok_or("template column: width stream exhausted")?
                        as usize;
                    let v = *values
                        .get(nc)
                        .ok_or("template column: value stream exhausted")?;
                    nc += 1;
                    push_padded(&mut out, v, w);
                }
                KIND_STR => {
                    let v = strs
                        .get(sc)
                        .ok_or("template column: string stream exhausted")?;
                    sc += 1;
                    out.push_str(v);
                }
                other => return Err(format!("template column: unknown placeholder tag {other}")),
            }
            i += 2;
        } else {
            let ch_len = utf8_char_len(bytes[i]);
            let end = (i + ch_len).min(bytes.len());
            out.push_str(&template[i..end]);
            i = end;
        }
    }
    if nc != widths.len() || sc != strs.len() {
        return Err("template column: unconsumed variables for message".into());
    }
    Ok(out)
}

/// Append `v` zero-padded to `w` digits — `format!("{v:0w$}")` without
/// the per-variable allocation (decode-path hot spot: millions of num
/// vars per block batch). Encode only stores non-negative values (signs
/// live in the template), so the negative branch exists purely to keep
/// corrupt input on the slow-but-correct path.
fn push_padded(out: &mut String, v: i64, w: usize) {
    if v < 0 {
        out.push_str(&format!("{v:0w$}"));
        return;
    }
    let mut buf = [0u8; 20];
    let mut x = v as u64;
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (x % 10) as u8;
        x /= 10;
        if x == 0 {
            break;
        }
    }
    for _ in (buf.len() - i)..w {
        out.push('0');
    }
    // The buffer holds only ASCII digits.
    out.push_str(std::str::from_utf8(&buf[i..]).unwrap());
}

// ---------------------------------------------------------------------------
// Column encode/decode
//
// Layout (all integers little-endian):
//   0   1   tokenizer mode that won (0 digit-run, 1 whole-token) —
//           informational; decode is mode-agnostic (templates are
//           self-describing)
//   1   4   u32 dict_n   distinct templates
//   5   4   u32 num_n    total Num variables
//   9   4   u32 str_n    total Str variables
//   13  5×4 u32 stored length of each sub-column
//   33  —   sub-columns back to back:
//             template ids   encode_i64, message order
//             template dict  encode_str, first-seen order
//             num widths     encode_u8,  (template, slot) group order
//             num values     encode_i64, (template, slot) group order
//             str values     encode_str, (template, slot) group order
// ---------------------------------------------------------------------------

const TPL_HEADER_LEN: usize = 33;

struct Tokenized<'a> {
    ids: Vec<i64>,
    dict: Vec<String>,
    msg_vars: Vec<Vec<Var<'a>>>,
}

fn tokenize_block<'a>(msgs: &[&'a str], split_runs: bool) -> Tokenized<'a> {
    let mut dict_map: HashMap<String, i64> = HashMap::new();
    let mut dict: Vec<String> = Vec::new();
    let mut ids: Vec<i64> = Vec::with_capacity(msgs.len());
    let mut msg_vars: Vec<Vec<Var<'a>>> = Vec::with_capacity(msgs.len());
    for m in msgs {
        let (template, vars) = tokenize(m, split_runs);
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
    Tokenized {
        ids,
        dict,
        msg_vars,
    }
}

fn encode_mode(msgs: &[&str], split_runs: bool, zstd_level: i32) -> Result<Vec<u8>, String> {
    let tk = tokenize_block(msgs, split_runs);

    // Group variables by (template id, slot); concatenate group by
    // group into single typed streams (the v2-lite layout).
    let mut groups: BTreeMap<(i64, u32), Vec<Var>> = BTreeMap::new();
    for (mi, &id) in tk.ids.iter().enumerate() {
        for (slot, v) in tk.msg_vars[mi].iter().enumerate() {
            groups.entry((id, slot as u32)).or_default().push(*v);
        }
    }
    let mut widths: Vec<u8> = Vec::new();
    let mut values: Vec<i64> = Vec::new();
    let mut strs: Vec<&str> = Vec::new();
    for vs in groups.values() {
        for v in vs {
            match v.kind {
                VarKind::Num => {
                    widths.push(v.text.len() as u8);
                    // num_run_ok guaranteed the parse at tokenize time.
                    values.push(v.text.parse().map_err(|_| {
                        "template column: internal error: unparseable num var".to_string()
                    })?);
                }
                VarKind::Str => strs.push(v.text),
            }
        }
    }

    let ids_col = encode_i64(&tk.ids, zstd_level)?.to_bytes();
    let dict_col = encode_str(
        tk.dict.iter().map(String::as_str),
        tk.dict.len(),
        zstd_level,
    )?
    .to_bytes();
    let w_col = encode_u8(&widths, zstd_level)?.to_bytes();
    let v_col = encode_i64(&values, zstd_level)?.to_bytes();
    let s_col = encode_str(strs.iter().copied(), strs.len(), zstd_level)?.to_bytes();

    for (what, count) in [
        ("templates", tk.dict.len()),
        ("num vars", values.len()),
        ("str vars", strs.len()),
    ] {
        if count > u32::MAX as usize {
            return Err(format!("template column: {what} count exceeds u32"));
        }
    }
    let cols = [&ids_col, &dict_col, &w_col, &v_col, &s_col];
    let mut out = Vec::with_capacity(TPL_HEADER_LEN + cols.iter().map(|c| c.len()).sum::<usize>());
    out.push(if split_runs { 0 } else { 1 });
    out.extend_from_slice(&(tk.dict.len() as u32).to_le_bytes());
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    out.extend_from_slice(&(strs.len() as u32).to_le_bytes());
    for c in cols {
        if c.len() > u32::MAX as usize {
            return Err("template column: sub-column exceeds u32::MAX bytes".into());
        }
        out.extend_from_slice(&(c.len() as u32).to_le_bytes());
    }
    for c in cols {
        out.extend_from_slice(c);
    }
    Ok(out)
}

/// Encode a message column as templates + typed variable columns. Both
/// tokenizer modes are trial-encoded and the smaller wins — the caller
/// (codec 8) then compares the result against plain [`encode_str`] and
/// falls back to codec 7 for the whole block if templates lose.
pub fn encode_template_str(msgs: &[&str], zstd_level: i32) -> Result<Vec<u8>, String> {
    let split = encode_mode(msgs, true, zstd_level)?;
    let whole = encode_mode(msgs, false, zstd_level)?;
    Ok(if split.len() <= whole.len() {
        split
    } else {
        whole
    })
}

/// One (template, slot) variable group's extent in its typed stream:
/// Num groups index into the widths/values columns, Str groups into
/// the string column.
struct Group {
    kind: VarKind,
    start: usize,
    len: usize,
}

/// Rebuild group extents: count each (template, slot)'s occurrences in
/// message order, then assign stream offsets in sorted group order —
/// the exact inverse of the encoder's BTreeMap walk. Returns the dense
/// per-template base table (group of (id, slot) sits at base[id] +
/// slot — no hashing in per-variable hot loops) plus the groups in
/// that order.
fn group_layout(
    ids: &[i64],
    kinds_by_id: &[Vec<VarKind>],
    num_n: usize,
    str_n: usize,
) -> Result<(Vec<usize>, Vec<Group>), String> {
    let mut sizes: BTreeMap<(i64, u32), usize> = BTreeMap::new();
    for &id in ids {
        for slot in 0..kinds_by_id[id as usize].len() {
            *sizes.entry((id, slot as u32)).or_default() += 1;
        }
    }
    let mut base: Vec<usize> = vec![usize::MAX; kinds_by_id.len()];
    let mut groups: Vec<Group> = Vec::with_capacity(sizes.len());
    let (mut num_off, mut str_off) = (0usize, 0usize);
    for (&(id, slot), &count) in &sizes {
        if base[id as usize] == usize::MAX {
            base[id as usize] = groups.len();
        }
        let kind = kinds_by_id[id as usize][slot as usize];
        let start = match kind {
            VarKind::Num => {
                let start = num_off;
                num_off += count;
                start
            }
            VarKind::Str => {
                let start = str_off;
                str_off += count;
                start
            }
        };
        debug_assert_eq!(groups.len(), base[id as usize] + slot as usize);
        groups.push(Group {
            kind,
            start,
            len: count,
        });
    }
    if num_off != num_n || str_off != str_n {
        return Err(format!(
            "template column: slot totals ({num_off} num, {str_off} str) disagree with header ({num_n}, {str_n})"
        ));
    }
    Ok((base, groups))
}

/// Decode a template-encoded message column back to exactly `n`
/// messages, bit-exact. Validates everything it reads — corrupt input
/// is an error naming the field, never a panic.
pub fn decode_template_str(bytes: &[u8], n: usize) -> Result<Vec<String>, String> {
    let mut r = Reader::new(bytes);
    let _mode = r.u8("template column mode")?;
    let dict_n = r.u32("template dict count")? as usize;
    let num_n = r.u32("template num-var count")? as usize;
    let str_n = r.u32("template str-var count")? as usize;
    let lens = [
        r.u32("template ids length")? as usize,
        r.u32("template dict length")? as usize,
        r.u32("template widths length")? as usize,
        r.u32("template values length")? as usize,
        r.u32("template strings length")? as usize,
    ];
    let names = [
        "template ids column",
        "template dict column",
        "template widths column",
        "template values column",
        "template strings column",
    ];
    let mut stored: Vec<&[u8]> = Vec::with_capacity(5);
    for (i, len) in lens.iter().enumerate() {
        stored.push(r.take(*len, names[i])?);
    }
    if r.remaining() != 0 {
        return Err(format!(
            "template column: {} trailing byte(s) after last sub-column",
            r.remaining()
        ));
    }

    let ids = decode_i64(stored[0], n)?;
    let dict = decode_str(stored[1], dict_n)?;
    let widths = decode_u8(stored[2], num_n)?;
    let values = decode_i64(stored[3], num_n)?;
    let strs = decode_str(stored[4], str_n)?;

    let kinds_by_id: Vec<Vec<VarKind>> = dict.iter().map(|t| slot_kinds(t)).collect();
    for (i, &id) in ids.iter().enumerate() {
        if id < 0 || id as usize >= dict_n {
            return Err(format!(
                "template column: entry {i} references template {id} of {dict_n}"
            ));
        }
    }

    let (base, groups) = group_layout(&ids, &kinds_by_id, num_n, str_n)?;
    let mut cursors: Vec<usize> = groups.iter().map(|g| g.start).collect();

    let mut out = Vec::with_capacity(n);
    let mut widths_m: Vec<u8> = Vec::new();
    let mut vals_m: Vec<i64> = Vec::new();
    let mut strs_m: Vec<&str> = Vec::new();
    for &id in &ids {
        let template = &dict[id as usize];
        let kinds = &kinds_by_id[id as usize];
        widths_m.clear();
        vals_m.clear();
        strs_m.clear();
        for (slot, kind) in kinds.iter().enumerate() {
            let c = cursors
                .get_mut(base[id as usize] + slot)
                .ok_or("template column: missing group cursor")?;
            match kind {
                VarKind::Num => {
                    widths_m.push(widths[*c]);
                    vals_m.push(values[*c]);
                }
                VarKind::Str => strs_m.push(strs[*c].as_str()),
            }
            *c += 1;
        }
        out.push(detokenize(template, &widths_m, &vals_m, &strs_m)?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// CLP-dictionary feasibility (issue #2)
//
// A template column already contains everything needed to prove a
// substring ABSENT from every message in the block, without decoding a
// single entry: the template dictionary holds all static text, and the
// variable columns hold everything else. The proof rests on one
// structural invariant of the tokenizer: TEMPLATE TEXT NEVER CONTAINS
// AN ASCII DIGIT (every digit ends up inside a variable) and EVERY
// VARIABLE CONTAINS AT LEAST ONE DIGIT (only digit-bearing tokens
// become variables).
//
// Consequences for a needle split at its ASCII digit runs:
//   - a digit-free needle fragment can overlap AT MOST a digit-free
//     suffix of one Str variable, then contiguous template text, then a
//     digit-free prefix of another Str variable — any variable strictly
//     inside the fragment's span would contribute a digit;
//   - an inner digit run of the needle (bounded by non-digits) is a
//     maximal digit run of the message, so it must equal either a Num
//     variable's rendering (value zero-padded to width) or a maximal
//     digit run inside a Str variable.
//
// The check is deliberately one-sided: `false` is a PROOF the needle
// matches nothing in the block; `true` just means "decode and look".
// Anything uncertain (non-ASCII needles, empty needles, pathological
// lengths, unknown layouts) answers `true`.
// ---------------------------------------------------------------------------

/// Fragments longer than this skip the split search and answer
/// feasible — the split search is quadratic in fragment length and a
/// needle this long is not a realistic interactive filter.
const FEASIBILITY_MAX_FRAGMENT: usize = 256;

/// ASCII-case-insensitive `haystack.contains(needle)`, mirroring the
/// engine's `message_contains_case_insensitive` ASCII fast path.
fn contains_ci(haystack: &str, needle: &str) -> bool {
    let n = needle.as_bytes();
    if n.is_empty() {
        return true;
    }
    haystack
        .as_bytes()
        .windows(n.len())
        .any(|w| w.eq_ignore_ascii_case(n))
}

fn ends_with_ci(haystack: &str, suffix: &str) -> bool {
    let (h, s) = (haystack.as_bytes(), suffix.as_bytes());
    h.len() >= s.len() && h[h.len() - s.len()..].eq_ignore_ascii_case(s)
}

fn starts_with_ci(haystack: &str, prefix: &str) -> bool {
    let (h, p) = (haystack.as_bytes(), prefix.as_bytes());
    h.len() >= p.len() && h[..p.len()].eq_ignore_ascii_case(p)
}

/// The placeholder-free text runs of a template, doubled sentinels
/// unescaped back to the literal byte. These are exactly the maximal
/// contiguous stretches of rendered text that come from the template.
fn template_segments(template: &str, out: &mut Vec<String>) {
    let bytes = template.as_bytes();
    let mut seg = String::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == SENTINEL {
            match bytes.get(i + 1) {
                Some(&SENTINEL) => {
                    seg.push(SENTINEL as char);
                    i += 2;
                }
                Some(&KIND_NUM) | Some(&KIND_STR) => {
                    if !seg.is_empty() {
                        out.push(std::mem::take(&mut seg));
                    }
                    i += 2;
                }
                _ => {
                    // Dangling sentinel: keep it as text — permissive.
                    seg.push(SENTINEL as char);
                    i += 1;
                }
            }
        } else {
            let ch_len = utf8_char_len(bytes[i]);
            let end = (i + ch_len).min(bytes.len());
            seg.push_str(&template[i..end]);
            i = end;
        }
    }
    if !seg.is_empty() {
        out.push(seg);
    }
}

/// `run` occurs in `text` as a MAXIMAL ASCII digit run (non-digit or
/// string boundary on both sides). Digits have no case, so this is an
/// exact byte search.
fn has_maximal_digit_run(text: &str, run: &str) -> bool {
    let t = text.as_bytes();
    let r = run.as_bytes();
    if t.len() < r.len() {
        return false;
    }
    for start in 0..=t.len() - r.len() {
        if &t[start..start + r.len()] != r {
            continue;
        }
        let left_ok = start == 0 || !t[start - 1].is_ascii_digit();
        let right_ok = start + r.len() == t.len() || !t[start + r.len()].is_ascii_digit();
        if left_ok && right_ok {
            return true;
        }
    }
    false
}

/// One digit-free fragment of the needle against the block's
/// dictionaries: feasible iff the fragment fits wholly inside one Str
/// variable, or splits as (suffix of a Str variable) + (substring of
/// one template segment) + (prefix of a Str variable), any part empty.
fn fragment_feasible(frag: &str, segments: &[String], strs: &[&str]) -> bool {
    if frag.is_empty() {
        return true;
    }
    if frag.len() > FEASIBILITY_MAX_FRAGMENT {
        return true;
    }
    if strs.iter().any(|v| contains_ci(v, frag)) {
        return true;
    }
    // Valid prefix cuts: i = 0 (no variable on the left) or frag[..i]
    // is the digit-free tail of some Str variable.
    let cut_is: Vec<usize> = std::iter::once(0)
        .chain((1..=frag.len()).filter(|&i| {
            frag.is_char_boundary(i) && strs.iter().any(|v| ends_with_ci(v, &frag[..i]))
        }))
        .collect();
    let cut_js: Vec<usize> = (0..frag.len())
        .filter(|&j| frag.is_char_boundary(j) && strs.iter().any(|v| starts_with_ci(v, &frag[j..])))
        .chain(std::iter::once(frag.len()))
        .collect();
    for &i in &cut_is {
        for &j in cut_js.iter().filter(|&&j| j >= i) {
            let middle = &frag[i..j];
            if middle.is_empty() || segments.iter().any(|s| contains_ci(s, middle)) {
                return true;
            }
        }
    }
    false
}

/// Can `needle` (engine `message_contains` semantics: case-insensitive
/// substring) possibly occur in any message of this template-encoded
/// column? Decodes only the dictionary and variable columns — never
/// the per-message id column, never a full message.
///
/// `Ok(false)` is a proof of absence. `Ok(true)` means "cannot rule it
/// out". `Err` means the column bytes did not parse — callers should
/// fall through to a full decode, which will report the corruption.
pub fn column_may_contain(bytes: &[u8], needle: &str) -> Result<bool, String> {
    if needle.is_empty() || !needle.is_ascii() {
        return Ok(true);
    }
    let mut r = Reader::new(bytes);
    let mode = r.u8("template column mode")?;
    let dict_n = r.u32("template dict count")? as usize;
    let num_n = r.u32("template num-var count")? as usize;
    let str_n = r.u32("template str-var count")? as usize;
    let lens = [
        r.u32("template ids length")? as usize,
        r.u32("template dict length")? as usize,
        r.u32("template widths length")? as usize,
        r.u32("template values length")? as usize,
        r.u32("template strings length")? as usize,
    ];
    let _ids = r.take(lens[0], "template ids column")?;
    let dict_bytes = r.take(lens[1], "template dict column")?;
    let widths_bytes = r.take(lens[2], "template widths column")?;
    let values_bytes = r.take(lens[3], "template values column")?;
    let strs_bytes = r.take(lens[4], "template strings column")?;

    let dict = decode_str(dict_bytes, dict_n)?;
    let strs_owned = decode_str(strs_bytes, str_n)?;
    // Duplicate variable values are common (the same id repeats across
    // entries); the searches below only need the distinct set.
    let strs: Vec<&str> = {
        let mut set: std::collections::HashSet<&str> =
            std::collections::HashSet::with_capacity(strs_owned.len());
        strs_owned.iter().for_each(|s| {
            set.insert(s.as_str());
        });
        set.into_iter().collect()
    };
    let mut segments: Vec<String> = Vec::new();
    for t in &dict {
        template_segments(t, &mut segments);
    }

    // Token-confined needle, whole-token mode: the strongest rule this
    // structure admits. Every byte of the needle is a token byte, so a
    // match can never span a token boundary — it lives inside ONE token
    // of some message. That token contains a digit (the needle has
    // one), so in whole-token mode it became a variable; and it also
    // contains a non-digit token byte (the needle has one), so it is a
    // Str variable, never a Num rendering. The needle keeps its FULL
    // literal selectivity — `10.52.89.122` is looked up as itself, not
    // as four independent digit runs.
    let nb = needle.as_bytes();
    if mode == 1
        && nb.iter().all(|&b| is_token_byte(b))
        && nb.iter().any(u8::is_ascii_digit)
        && nb.iter().any(|b| !b.is_ascii_digit())
    {
        return Ok(strs.iter().any(|v| contains_ci(v, needle)));
    }

    // Inner digit runs need the num columns; decode them once, up
    // front, so corruption surfaces as Err (decode-and-report), never
    // as a false proof of absence.
    let has_inner_run = {
        let first_non_digit = nb.iter().position(|b| !b.is_ascii_digit());
        let last_non_digit = nb.iter().rposition(|b| !b.is_ascii_digit());
        match (first_non_digit, last_non_digit) {
            (Some(first), Some(last)) => nb[first..=last].iter().any(u8::is_ascii_digit),
            _ => false,
        }
    };
    let num_pairs: std::collections::HashSet<(u8, i64)> = if has_inner_run {
        let widths = decode_u8(widths_bytes, num_n)?;
        let values = decode_i64(values_bytes, num_n)?;
        widths.iter().copied().zip(values).collect()
    } else {
        std::collections::HashSet::new()
    };

    // Every fragment and every inner digit run must be explainable.
    let mut i = 0;
    while i < nb.len() {
        if nb[i].is_ascii_digit() {
            let start = i;
            while i < nb.len() && nb[i].is_ascii_digit() {
                i += 1;
            }
            // Only INNER runs are maximal runs of the message; edge
            // runs may continue past the needle boundary.
            if start == 0 || i == nb.len() {
                continue;
            }
            let run = &needle[start..i];
            let num_ok = run.len() <= 19
                && run
                    .parse::<i64>()
                    .ok()
                    .is_some_and(|v| num_pairs.contains(&(run.len() as u8, v)));
            if !num_ok && !strs.iter().any(|s| has_maximal_digit_run(s, run)) {
                return Ok(false);
            }
        } else {
            let start = i;
            while i < nb.len() && !nb[i].is_ascii_digit() {
                i += 1;
            }
            if !fragment_feasible(&needle[start..i], &segments, &strs) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Sub-block row skipping (issue #2, phase 2)
//
// Block-level feasibility answers "could ANY message here contain the
// needle". This section answers it PER TEMPLATE, with each template's
// own variable groups (a message rendered from template t draws its
// variables exclusively from t's (template, slot) groups, so
// group-local checks are sound and strictly tighter than block-level
// ones). Rows of infeasible templates advance the decode cursors
// without materializing a single string; candidate rows detokenize and
// are kept only when the rendered message actually contains the
// needle. Query work becomes proportional to candidate rows, not
// window size.
// ---------------------------------------------------------------------------

/// The needle split once, reused across every template's check.
struct NeedleParts<'a> {
    /// Maximal digit-free fragments.
    fragments: Vec<&'a str>,
    /// Maximal digit runs bounded by non-digits on both sides.
    inner_runs: Vec<&'a str>,
    /// All token bytes, at least one digit, at least one non-digit:
    /// can never span a token boundary, so in whole-token mode it must
    /// sit inside one Str variable.
    token_confined_mixed: bool,
}

fn needle_parts(needle: &str) -> NeedleParts<'_> {
    let nb = needle.as_bytes();
    let mut fragments = Vec::new();
    let mut inner_runs = Vec::new();
    let mut i = 0;
    while i < nb.len() {
        let start = i;
        if nb[i].is_ascii_digit() {
            while i < nb.len() && nb[i].is_ascii_digit() {
                i += 1;
            }
            if start > 0 && i < nb.len() {
                inner_runs.push(&needle[start..i]);
            }
        } else {
            while i < nb.len() && !nb[i].is_ascii_digit() {
                i += 1;
            }
            fragments.push(&needle[start..i]);
        }
    }
    NeedleParts {
        fragments,
        inner_runs,
        token_confined_mixed: !nb.is_empty()
            && nb.iter().all(|&b| is_token_byte(b))
            && nb.iter().any(u8::is_ascii_digit)
            && nb.iter().any(|b| !b.is_ascii_digit()),
    }
}

/// The block-level feasibility rules, template-locally: `segments` are
/// THIS template's placeholder-free runs, `t_strs`/`t_nums` its own
/// groups' variables. `false` proves no message of this template can
/// contain the needle.
fn template_feasible(
    needle: &str,
    parts: &NeedleParts,
    whole_token_mode: bool,
    segments: &[String],
    t_strs: &[&str],
    t_nums: &[(u8, i64)],
) -> bool {
    if whole_token_mode && parts.token_confined_mixed {
        return t_strs.iter().any(|v| contains_ci(v, needle));
    }
    for frag in &parts.fragments {
        if !fragment_feasible(frag, segments, t_strs) {
            return false;
        }
    }
    for run in &parts.inner_runs {
        let num_ok = run.len() <= 19
            && run.parse::<i64>().ok().is_some_and(|v| {
                t_nums
                    .iter()
                    .any(|&(w, val)| w as usize == run.len() && val == v)
            });
        if !num_ok && !t_strs.iter().any(|s| has_maximal_digit_run(s, run)) {
            return false;
        }
    }
    true
}

/// The rows of a filtered column decode: message-order indices plus
/// the rendered messages that matched, and how many rows had to be
/// detokenized to find them (the decode work actually done).
pub(crate) struct FilteredMessages {
    pub(crate) rows: Vec<(usize, String)>,
    pub(crate) candidate_rows: u64,
}

/// Decode ONLY the messages that contain `needle` (ASCII,
/// case-insensitive substring — the engine's `message_contains`
/// semantics). The needle must be non-empty ASCII; callers gate.
///
/// Correctness contract, enforced by the equivalence tests: the result
/// is EXACTLY `decode_template_str(..)` filtered to matching rows,
/// with original row indices.
pub(crate) fn decode_template_str_filtered(
    bytes: &[u8],
    n: usize,
    needle: &str,
) -> Result<FilteredMessages, String> {
    if needle.is_empty() || !needle.is_ascii() {
        return Err("template column: filtered decode requires a non-empty ASCII needle".into());
    }
    let mut r = Reader::new(bytes);
    let mode = r.u8("template column mode")?;
    let dict_n = r.u32("template dict count")? as usize;
    let num_n = r.u32("template num-var count")? as usize;
    let str_n = r.u32("template str-var count")? as usize;
    let lens = [
        r.u32("template ids length")? as usize,
        r.u32("template dict length")? as usize,
        r.u32("template widths length")? as usize,
        r.u32("template values length")? as usize,
        r.u32("template strings length")? as usize,
    ];
    let names = [
        "template ids column",
        "template dict column",
        "template widths column",
        "template values column",
        "template strings column",
    ];
    let mut stored: Vec<&[u8]> = Vec::with_capacity(5);
    for (i, len) in lens.iter().enumerate() {
        stored.push(r.take(*len, names[i])?);
    }
    if r.remaining() != 0 {
        return Err(format!(
            "template column: {} trailing byte(s) after last sub-column",
            r.remaining()
        ));
    }

    let ids = decode_i64(stored[0], n)?;
    let dict = decode_str(stored[1], dict_n)?;
    let widths = decode_u8(stored[2], num_n)?;
    let values = decode_i64(stored[3], num_n)?;
    let strs = decode_str(stored[4], str_n)?;

    let kinds_by_id: Vec<Vec<VarKind>> = dict.iter().map(|t| slot_kinds(t)).collect();
    for (i, &id) in ids.iter().enumerate() {
        if id < 0 || id as usize >= dict_n {
            return Err(format!(
                "template column: entry {i} references template {id} of {dict_n}"
            ));
        }
    }
    let (base, groups) = group_layout(&ids, &kinds_by_id, num_n, str_n)?;

    let parts = needle_parts(needle);
    let whole_token_mode = mode == 1;
    let mut segments: Vec<String> = Vec::new();
    let feasible: Vec<bool> = (0..dict_n)
        .map(|t| {
            let kinds = &kinds_by_id[t];
            segments.clear();
            template_segments(&dict[t], &mut segments);
            if kinds.is_empty() {
                // Variable-free template: the message IS the template
                // text (there are no groups, so base[t] is unset).
                return template_feasible(needle, &parts, whole_token_mode, &segments, &[], &[]);
            }
            // A slotted template that never occurs has no groups and no
            // rows that could consult this entry.
            if base[t] == usize::MAX {
                return false;
            }
            let mut t_strs: Vec<&str> = Vec::new();
            let mut t_nums: Vec<(u8, i64)> = Vec::new();
            for slot in 0..kinds.len() {
                let g = &groups[base[t] + slot];
                match g.kind {
                    VarKind::Str => {
                        t_strs.extend(strs[g.start..g.start + g.len].iter().map(String::as_str))
                    }
                    VarKind::Num => t_nums.extend(
                        widths[g.start..g.start + g.len]
                            .iter()
                            .copied()
                            .zip(values[g.start..g.start + g.len].iter().copied()),
                    ),
                }
            }
            t_strs.sort_unstable();
            t_strs.dedup();
            template_feasible(
                needle,
                &parts,
                whole_token_mode,
                &segments,
                &t_strs,
                &t_nums,
            )
        })
        .collect();

    let mut cursors: Vec<usize> = groups.iter().map(|g| g.start).collect();
    let mut rows: Vec<(usize, String)> = Vec::new();
    let mut candidate_rows = 0u64;
    let mut widths_m: Vec<u8> = Vec::new();
    let mut vals_m: Vec<i64> = Vec::new();
    let mut strs_m: Vec<&str> = Vec::new();
    for (row, &id) in ids.iter().enumerate() {
        let t = id as usize;
        let kinds = &kinds_by_id[t];
        if !feasible[t] {
            // Skip: advance this row's group cursors, build nothing.
            for slot in 0..kinds.len() {
                let c = cursors
                    .get_mut(base[t] + slot)
                    .ok_or("template column: missing group cursor")?;
                *c += 1;
            }
            continue;
        }
        candidate_rows += 1;
        widths_m.clear();
        vals_m.clear();
        strs_m.clear();
        for (slot, kind) in kinds.iter().enumerate() {
            let c = cursors
                .get_mut(base[t] + slot)
                .ok_or("template column: missing group cursor")?;
            match kind {
                VarKind::Num => {
                    widths_m.push(widths[*c]);
                    vals_m.push(values[*c]);
                }
                VarKind::Str => strs_m.push(strs[*c].as_str()),
            }
            *c += 1;
        }
        let message = detokenize(&dict[t], &widths_m, &vals_m, &strs_m)?;
        if contains_ci(&message, needle) {
            rows.push((row, message));
        }
    }
    Ok(FilteredMessages {
        rows,
        candidate_rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column_roundtrip(msgs: &[&str]) {
        let enc = encode_template_str(msgs, 3).unwrap();
        let dec = decode_template_str(&enc, msgs.len()).unwrap();
        assert_eq!(dec, msgs);
    }

    fn tokenizer_roundtrip(msg: &str) {
        for split in [true, false] {
            let (template, vars) = tokenize(msg, split);
            let mut widths = Vec::new();
            let mut values = Vec::new();
            let mut strs = Vec::new();
            for v in &vars {
                match v.kind {
                    VarKind::Num => {
                        widths.push(v.text.len() as u8);
                        values.push(v.text.parse().unwrap());
                    }
                    VarKind::Str => strs.push(v.text),
                }
            }
            let back = detokenize(&template, &widths, &values, &strs).unwrap();
            assert_eq!(back, msg, "mode split={split}, template {template:?}");
            let kinds = slot_kinds(&template);
            assert_eq!(kinds.len(), vars.len());
            for (k, v) in kinds.iter().zip(&vars) {
                assert_eq!(*k, v.kind);
            }
        }
    }

    const TRICKY: &[&str] = &[
        "user 4821 logged in from 10.0.0.5",
        "user 04821 logged in from 10.0.0.5:8080",
        "big -9223372036854775808 and 9223372036854775807",
        "overflow 99999999999999999999 stays a string",
        "small -42, -0, 007, 0, 00",
        "latency 3.14ms for /api/v2/users/91",
        "blk_-3544583377289625738 replicated to 10.251.31.5:50010",
        "2017-07-01T12:00:00.123+02:00 job_1445144423722_0020 done",
        "uuid 550e8400-e29b-41d4-a716-446655440000 hex 0xdeadbeef raw d3adbeef",
        "unicode: héllo wörld 42 — em—dash and 日本語 123",
        "",
        "no digits at all, just words.",
        "   leading and trailing   ",
        "a\nmulti-line\nstack trace\n  at foo.bar(Baz.java:123)",
        "tab\tseparated\t99",
    ];

    #[test]
    fn tokenizer_roundtrips_exactly() {
        for msg in TRICKY {
            tokenizer_roundtrip(msg);
        }
        let evil = format!("bad {}byte 42 and doubled {}{} end", '\x11', '\x11', '\x11');
        tokenizer_roundtrip(&evil);
        tokenizer_roundtrip("x\x11123\x11y");
    }

    #[test]
    fn column_roundtrips_exactly() {
        column_roundtrip(TRICKY);
        // Single message, and a block of identical messages.
        column_roundtrip(&["only one line, 42"]);
        column_roundtrip(&["same line 7"; 100]);
    }

    #[test]
    fn templated_corpus_beats_encode_str() {
        // The whole point: similar-but-not-identical lines.
        let msgs: Vec<String> = (0..4096)
            .map(|i| {
                format!(
                    "user {} logged in from 10.0.{}.{} in {}ms",
                    1000 + i,
                    i % 256,
                    (i * 7) % 256,
                    i % 900
                )
            })
            .collect();
        let refs: Vec<&str> = msgs.iter().map(String::as_str).collect();
        let tpl = encode_template_str(&refs, 7).unwrap();
        let base = encode_str(refs.iter().copied(), refs.len(), 7)
            .unwrap()
            .encoded_len();
        assert!(
            tpl.len() * 3 < base * 2,
            "expected ≥1.5x win, got {} vs {base}",
            tpl.len()
        );
        let dec = decode_template_str(&tpl, refs.len()).unwrap();
        assert_eq!(dec, refs);
    }

    #[test]
    fn zero_padding_shares_a_template() {
        let (t1, _) = tokenize("2017-07-01 12:07:33", true);
        let (t2, _) = tokenize("2017-11-23 04:00:09", true);
        assert_eq!(t1, t2);
        let (t3, _) = tokenize("id 550e8400 done", true);
        let (t4, _) = tokenize("id a1b2c3d4 done", true);
        assert_eq!(t3, t4);
    }

    #[test]
    fn deterministic_pseudo_random_roundtrip() {
        // Seeded xorshift fuzz: printable-ish soup including sentinels,
        // unicode and digit runs. Same seed every run — reproducible.
        let mut state = 0x9e3779b97f4a7c15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let alphabet: Vec<char> = ('!'..='~')
            .chain([' ', '\t', '\n', '\x11', 'é', '本', '—', '🦀'])
            .collect();
        let mut block: Vec<String> = Vec::new();
        for _ in 0..2000 {
            let len = (next() % 80) as usize;
            let msg: String = (0..len)
                .map(|_| alphabet[(next() as usize) % alphabet.len()])
                .collect();
            tokenizer_roundtrip(&msg);
            block.push(msg);
        }
        let refs: Vec<&str> = block.iter().map(String::as_str).collect();
        column_roundtrip(&refs);
    }

    // ── CLP-dictionary feasibility ───────────────────────────────────

    /// Case-insensitive contains, mirroring the engine's matcher — the
    /// oracle the feasibility check must never contradict.
    fn oracle_contains(message: &str, needle: &str) -> bool {
        let n = needle.as_bytes();
        needle.is_empty()
            || message
                .as_bytes()
                .windows(n.len())
                .any(|w| w.eq_ignore_ascii_case(n))
    }

    /// Every substring of every encoded message must stay feasible —
    /// a false negative here is a wrong query result in production.
    fn assert_no_false_negatives(msgs: &[&str]) {
        let enc = encode_template_str(msgs, 3).unwrap();
        for m in msgs {
            let len = m.len();
            for start in 0..len {
                if !m.is_char_boundary(start) {
                    continue;
                }
                for end in start + 1..=len {
                    if !m.is_char_boundary(end) {
                        continue;
                    }
                    let needle = &m[start..end];
                    assert!(
                        column_may_contain(&enc, needle).unwrap(),
                        "false negative: needle {needle:?} from message {m:?}"
                    );
                    let upper = needle.to_uppercase();
                    if upper.is_ascii() {
                        assert!(
                            column_may_contain(&enc, &upper).unwrap(),
                            "false negative (case): needle {upper:?} from message {m:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn feasibility_no_false_negatives_tricky_corpus() {
        assert_no_false_negatives(TRICKY);
    }

    #[test]
    fn feasibility_no_false_negatives_templated_corpus() {
        let msgs: Vec<String> = (0..64)
            .map(|i| {
                format!(
                    "user {} logged in from 10.0.{}.{} in {}ms via 0xdead{:04x}",
                    1000 + i,
                    i % 256,
                    (i * 7) % 256,
                    i % 900,
                    i
                )
            })
            .collect();
        let refs: Vec<&str> = msgs.iter().map(String::as_str).collect();
        assert_no_false_negatives(&refs);
    }

    #[test]
    fn feasibility_prunes_absent_needles() {
        let msgs: Vec<String> = (0..256)
            .map(|i| format!("DHCP NAK - MAC:00:1a:{:02x} on port {}", i, 8000 + i))
            .collect();
        let refs: Vec<&str> = msgs.iter().map(String::as_str).collect();
        let enc = encode_template_str(&refs, 3).unwrap();
        // Absent word.
        assert!(!column_may_contain(&enc, "PROVISIONING").unwrap());
        // Present words, any case.
        assert!(column_may_contain(&enc, "dhcp nak").unwrap());
        // Inner digit run absent as a maximal run everywhere: ports are
        // 8000..8255, so 9999 appears nowhere.
        assert!(!column_may_contain(&enc, "port 9999 ").unwrap());
        // Present inner run.
        assert!(column_may_contain(&enc, "port 8017 ").unwrap());
        // A run that only occurs as a NON-maximal run must not match:
        // "port 801 " needs digit-boundary 801, but every 801 is
        // followed by another digit.
        assert!(!column_may_contain(&enc, "port 801 ").unwrap());
        // Edge digit runs stay permissive (may continue past the
        // needle), so a pure-digit needle never prunes.
        assert!(column_may_contain(&enc, "99999").unwrap());
    }

    #[test]
    fn feasibility_handles_seam_spanning_needles() {
        // Whole-token mode: "abc42def" is one Str variable. Needles
        // spanning template↔variable seams must stay feasible.
        let msgs = ["session abc42def opened", "session abc42def closed"];
        let enc = encode_template_str(&msgs, 3).unwrap();
        for needle in [
            "session abc",  // template + var prefix
            "def opened",   // var suffix + template
            "n abc42def o", // template + whole var + template
            "SESSION ABC42DEF CLOSED",
        ] {
            assert!(
                column_may_contain(&enc, needle).unwrap(),
                "seam needle {needle:?} must stay feasible"
            );
        }
        assert!(!column_may_contain(&enc, "session xyz").unwrap());
    }

    #[test]
    fn feasibility_fuzz_against_decode_oracle() {
        // Seeded xorshift corpus + needles: wherever the check says
        // "absent", the decoded messages must agree.
        let mut state = 0x2545f4914f6cdd1du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let words = [
            "connect", "refused", "timeout", "retry", "lease", "offer", "expired", "renewed",
        ];
        let mut msgs: Vec<String> = Vec::new();
        for _ in 0..512 {
            let w1 = words[(next() as usize) % words.len()];
            let w2 = words[(next() as usize) % words.len()];
            msgs.push(format!(
                "{w1} {w2} id {} from 10.{}.{}.{} tok {:x}",
                next() % 100000,
                next() % 256,
                next() % 256,
                next() % 256,
                next() % 0xffffff
            ));
        }
        let refs: Vec<&str> = msgs.iter().map(String::as_str).collect();
        let enc = encode_template_str(&refs, 3).unwrap();
        let decoded = decode_template_str(&enc, refs.len()).unwrap();

        let mut checked_absent = 0u32;
        for _ in 0..2000 {
            // Mix of real substrings and perturbed ones.
            let src = &decoded[(next() as usize) % decoded.len()];
            let len = src.len();
            let a = (next() as usize) % len;
            let b = (a + 1 + (next() as usize) % (len - a)).min(len);
            let mut needle = src[a..b].to_string();
            if next() % 2 == 0 {
                // Perturb: flip a byte to make absence likely.
                let idx = (next() as usize) % needle.len();
                let mut bytes = needle.into_bytes();
                bytes[idx] = b'a' + (next() % 26) as u8;
                needle = String::from_utf8(bytes).unwrap_or_default();
            }
            if needle.is_empty() || !needle.is_ascii() {
                continue;
            }
            let feasible = column_may_contain(&enc, &needle).unwrap();
            let truly_present = decoded.iter().any(|m| oracle_contains(m, &needle));
            if truly_present {
                assert!(feasible, "false negative for needle {needle:?}");
            }
            if !feasible {
                checked_absent += 1;
            }
        }
        // The fuzz must actually exercise pruning, not vacuously pass.
        assert!(checked_absent > 50, "only {checked_absent} pruned needles");
    }

    /// Filtered decode must be EXACTLY full decode + filter: same rows,
    /// same indices, same rendered strings.
    fn assert_filtered_equivalence(msgs: &[&str], needles: &[&str]) {
        let enc = encode_template_str(msgs, 3).unwrap();
        let full = decode_template_str(&enc, msgs.len()).unwrap();
        for needle in needles {
            if needle.is_empty() || !needle.is_ascii() {
                continue;
            }
            let expected: Vec<(usize, String)> = full
                .iter()
                .enumerate()
                .filter(|(_, m)| oracle_contains(m, needle))
                .map(|(i, m)| (i, m.clone()))
                .collect();
            let filtered = decode_template_str_filtered(&enc, msgs.len(), needle).unwrap();
            assert_eq!(
                filtered.rows, expected,
                "filtered decode diverged for needle {needle:?}"
            );
            assert!(filtered.candidate_rows <= msgs.len() as u64);
        }
    }

    #[test]
    fn filtered_decode_equals_full_decode_filter() {
        assert_filtered_equivalence(
            TRICKY,
            &[
                "user",
                "USER",
                "logged",
                "10.0.0.5",
                "0.5",
                "42",
                "no digits",
                "absent-x",
                "blk_-3544583377289625738",
                "Baz.java:123",
                "é 42",
            ],
        );
        // Two message families sharing a block: per-template skipping
        // must not disturb the group-cursor walk for kept rows.
        let msgs: Vec<String> = (0..512)
            .map(|i| {
                if i % 2 == 0 {
                    format!("alpha request {} from 10.1.{}.9 done", i, i % 128)
                } else {
                    format!("beta lease {} renewed after {}s", 9000 + i, i % 300)
                }
            })
            .collect();
        let refs: Vec<&str> = msgs.iter().map(String::as_str).collect();
        assert_filtered_equivalence(
            &refs,
            &[
                "alpha",
                "beta",
                "renewed",
                "ALPHA REQUEST",
                "10.1.44.9",
                "absent",
            ],
        );
        // And skipping must actually happen: "alpha" rows are half the
        // block, so candidates must be well under n.
        let enc = encode_template_str(&refs, 3).unwrap();
        let filtered = decode_template_str_filtered(&enc, refs.len(), "alpha").unwrap();
        assert_eq!(filtered.rows.len(), 256);
        assert!(
            filtered.candidate_rows < 512,
            "expected per-template skipping, candidates={}",
            filtered.candidate_rows
        );
    }

    #[test]
    fn filtered_decode_fuzz_equivalence() {
        let mut state = 0x853c49e6748fea9bu64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let words = ["lease", "offer", "nak", "renew", "expire", "bind", "probe"];
        let mut msgs: Vec<String> = Vec::new();
        for _ in 0..400 {
            let w1 = words[(next() as usize) % words.len()];
            let w2 = words[(next() as usize) % words.len()];
            msgs.push(format!(
                "{w1} {w2} id {} at 172.{}.{}.{} tag {:x}",
                next() % 10000,
                next() % 32,
                next() % 256,
                next() % 256,
                next() % 0xfffff
            ));
        }
        let refs: Vec<&str> = msgs.iter().map(String::as_str).collect();
        let enc = encode_template_str(&refs, 3).unwrap();
        let full = decode_template_str(&enc, refs.len()).unwrap();
        for _ in 0..600 {
            let src = &full[(next() as usize) % full.len()];
            let len = src.len();
            let a = (next() as usize) % len;
            let b = (a + 1 + (next() as usize) % (len - a)).min(len);
            let mut needle = src[a..b].to_string();
            if next() % 2 == 0 {
                let idx = (next() as usize) % needle.len();
                let mut bytes = needle.into_bytes();
                bytes[idx] = b'a' + (next() % 26) as u8;
                needle = String::from_utf8(bytes).unwrap_or_default();
            }
            if needle.is_empty() {
                continue;
            }
            let expected: Vec<(usize, String)> = full
                .iter()
                .enumerate()
                .filter(|(_, m)| oracle_contains(m, &needle))
                .map(|(i, m)| (i, m.clone()))
                .collect();
            let filtered = decode_template_str_filtered(&enc, refs.len(), &needle).unwrap();
            assert_eq!(
                filtered.rows, expected,
                "fuzz divergence for needle {needle:?}"
            );
        }
    }

    #[test]
    fn corrupt_input_errors_not_panics() {
        let msgs = ["a 1", "b 2", "c 3"];
        let enc = encode_template_str(&msgs, 3).unwrap();
        // Truncations at every length must error, never panic — for the
        // decoder AND the feasibility check.
        for cut in 0..enc.len() {
            let _ = decode_template_str(&enc[..cut], msgs.len());
            let _ = column_may_contain(&enc[..cut], "a 1");
            let _ = decode_template_str_filtered(&enc[..cut], msgs.len(), "a 1");
        }
        // Wrong n.
        assert!(decode_template_str(&enc, 4).is_err());
        // Garbage.
        assert!(decode_template_str(&[0xff; 40], 3).is_err());
    }
}
