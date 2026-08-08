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
//! [`encode_template_str`] trial-encodes both and keeps the smaller,
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

    // Rebuild group extents: count each (template, slot)'s occurrences
    // in message order, then assign stream offsets in sorted group
    // order — the exact inverse of the encoder's BTreeMap walk.
    let mut sizes: BTreeMap<(i64, u32), usize> = BTreeMap::new();
    for &id in &ids {
        for slot in 0..kinds_by_id[id as usize].len() {
            *sizes.entry((id, slot as u32)).or_default() += 1;
        }
    }
    // Dense cursor table: `sizes` iterates sorted by (id, slot) and a
    // template's slots are all present whenever the template occurs, so
    // group g of template `id`, slot `s` sits at base[id] + s — no
    // hashing in the per-variable hot loop below.
    let mut base: Vec<usize> = vec![usize::MAX; dict_n];
    let mut cursors: Vec<usize> = Vec::with_capacity(sizes.len());
    let (mut num_off, mut str_off) = (0usize, 0usize);
    for (&(id, slot), &count) in &sizes {
        if base[id as usize] == usize::MAX {
            base[id as usize] = cursors.len();
        }
        match kinds_by_id[id as usize][slot as usize] {
            VarKind::Num => {
                cursors.push(num_off);
                num_off += count;
            }
            VarKind::Str => {
                cursors.push(str_off);
                str_off += count;
            }
        }
        debug_assert_eq!(cursors.len() - 1, base[id as usize] + slot as usize);
    }
    if num_off != num_n || str_off != str_n {
        return Err(format!(
            "template column: slot totals ({num_off} num, {str_off} str) disagree with header ({num_n}, {str_n})"
        ));
    }

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

    #[test]
    fn corrupt_input_errors_not_panics() {
        let msgs = ["a 1", "b 2", "c 3"];
        let enc = encode_template_str(&msgs, 3).unwrap();
        // Truncations at every length must error, never panic.
        for cut in 0..enc.len() {
            let _ = decode_template_str(&enc[..cut], msgs.len());
        }
        // Wrong n.
        assert!(decode_template_str(&enc, 4).is_err());
        // Garbage.
        assert!(decode_template_str(&[0xff; 40], 3).is_err());
    }
}
