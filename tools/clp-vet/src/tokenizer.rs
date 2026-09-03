//! CLP-style deterministic, stateless rule tokenizer (CLP_PLAN.md Phase 1
//! spec, built in Phase 0 because the harness needs it to measure anything).
//!
//! The core heuristic is CLP's: digits are variables, words are template
//! text. No training, no similarity tree, no persisted state — the same
//! message always produces the same (template, vars) split.
//!
//! Two variable kinds:
//!
//!   Num  a run of decimal DIGITS inside a token, stored as
//!        (width, i64 value). Width restores zero padding exactly, so
//!        "07" and "11" share one template slot ("2017-07-01" and
//!        "2017-11-23" produce the SAME template) while the values ride
//!        the pco-friendly i64 column. Any sign or separator stays in
//!        the template, so values are always non-negative.
//!   Str  a whole token kept verbatim: hex-ish runs and UUIDs, whose
//!        digit/letter skeleton differs per instance and would explode
//!        the template dictionary if digit runs were split out; plus
//!        digit runs too long for an i64 or wider than 255.
//!
//! LOSSLESS BY CONSTRUCTION: a Str variable is the exact original
//! substring; a Num variable re-renders as its value zero-padded to its
//! recorded width, which is byte-identical to the digits it came from.

/// Placeholder sentinel inside templates. 0x11 (DC1) never appears in
/// real log text; when it does appear in a message we escape it as a
/// doubled sentinel, so reconstruction is still exact.
pub const SENTINEL: u8 = 0x11;
/// Sentinel + 'd': next variable is (width, value) from the num streams.
pub const KIND_NUM: u8 = b'd';
/// Sentinel + 's': next variable comes from the string stream.
pub const KIND_STR: u8 = b's';

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VarKind {
    Num,
    Str,
}

#[derive(Clone, Copy, Debug)]
pub struct Var<'a> {
    pub kind: VarKind,
    pub text: &'a str,
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
/// hex runs and UUIDs — are kept whole as Str variables. Splitting
/// "550e8400" vs "a1b2c3d4" into digit runs would mint a fresh template
/// per id.
fn is_hex_like(tok: &str) -> bool {
    let body = tok.strip_prefix("0x").unwrap_or(tok);
    if body.len() >= 4
        && body.bytes().all(|b| b.is_ascii_hexdigit())
        && body.bytes().any(|b| b.is_ascii_alphabetic())
    {
        return true;
    }
    if tok.len() >= 4 && tok.strip_prefix("0x").is_some() {
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

/// A digit run rides the typed num column when its value fits an i64
/// and its width fits the u8 width column. (A 19-digit HDFS block id
/// fits; a 20-digit overflow or a 300-zero pathology stays a string.)
fn num_run_ok(run: &str) -> bool {
    run.len() <= 255 && run.parse::<i64>().is_ok()
}

/// Split `msg` into a template (placeholders inline) and the ordered
/// variable list. Deterministic and stateless. Digit-run mode
/// ([`tokenize`]) shreds runs of digits inside tokens; whole-token mode
/// ([`tokenize_whole`]) keeps each digit-bearing token as ONE variable
/// (pure digit runs still ride the num columns via the width trick).
/// Neither dominates — timestamp-heavy corpora favor whole tokens
/// (they dictionary-dedup), id/size-heavy corpora favor digit runs —
/// so a codec-8 block would try both and keep the smaller, exactly
/// like every other adaptive pick in timeless-codec.
pub fn tokenize(msg: &str) -> (String, Vec<Var<'_>>) {
    tokenize_mode(msg, true)
}

pub fn tokenize_whole(msg: &str) -> (String, Vec<Var<'_>>) {
    tokenize_mode(msg, false)
}

fn tokenize_mode(msg: &str, split_runs: bool) -> (String, Vec<Var<'_>>) {
    let bytes = msg.as_bytes();
    let mut template = String::with_capacity(msg.len());
    let mut vars = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == SENTINEL {
            // Literal sentinel in the source: escape as doubled sentinel.
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
                template.push(SENTINEL as char);
                template.push(KIND_STR as char);
                vars.push(Var {
                    kind: VarKind::Str,
                    text: tok,
                });
            } else if !split_runs {
                // Whole-token mode, pure digit run: the width column
                // restores padding, so it can still ride the num columns.
                let kind = if num_run_ok(tok) {
                    VarKind::Num
                } else {
                    VarKind::Str
                };
                template.push(SENTINEL as char);
                template.push(match kind {
                    VarKind::Num => KIND_NUM,
                    VarKind::Str => KIND_STR,
                } as char);
                vars.push(Var { kind, text: tok });
            } else {
                // Split the token into digit runs (variables) and
                // everything else (stable skeleton, stays in template).
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
                        template.push(SENTINEL as char);
                        template.push(match kind {
                            VarKind::Num => KIND_NUM,
                            VarKind::Str => KIND_STR,
                        } as char);
                        vars.push(Var { kind, text: run });
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

#[inline]
fn utf8_char_len(lead: u8) -> usize {
    match lead {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

/// Exact inverse of [`tokenize`]: splice variables back into the
/// template. Streams are consumed in placeholder order; cursors advance
/// past what this message used (block decode walks shared streams).
pub fn detokenize(
    template: &str,
    widths: &[u8],
    values: &[i64],
    num_cursor: &mut usize,
    strs: &[String],
    str_cursor: &mut usize,
) -> Result<String, String> {
    let bytes = template.as_bytes();
    let mut out = String::with_capacity(template.len() * 2);
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == SENTINEL {
            let tag = *bytes
                .get(i + 1)
                .ok_or("detokenize: dangling sentinel at end of template")?;
            match tag {
                SENTINEL => out.push(SENTINEL as char),
                KIND_NUM => {
                    let w = *widths
                        .get(*num_cursor)
                        .ok_or("detokenize: width stream exhausted")?
                        as usize;
                    let v = values
                        .get(*num_cursor)
                        .ok_or("detokenize: value stream exhausted")?;
                    *num_cursor += 1;
                    out.push_str(&format!("{v:0w$}"));
                }
                KIND_STR => {
                    let v = strs
                        .get(*str_cursor)
                        .ok_or("detokenize: string stream exhausted")?;
                    *str_cursor += 1;
                    out.push_str(v);
                }
                other => return Err(format!("detokenize: unknown placeholder tag {other}")),
            }
            i += 2;
        } else {
            let ch_len = utf8_char_len(bytes[i]);
            let end = (i + ch_len).min(bytes.len());
            out.push_str(&template[i..end]);
            i = end;
        }
    }
    Ok(out)
}

/// Per-slot kinds in placeholder order. A template fully determines the
/// kind of every slot — the v2 grouped layout leans on this to rebuild
/// group sizes from the id column alone.
pub fn slot_kinds(template: &str) -> Vec<VarKind> {
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

// ---------------------------------------------------------------------------
// Variable shape classification — STATS ONLY. Feeds the Phase-0
// "% variables numeric / pco-eligible" number; never affects encoding
// or correctness.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub enum VarShape {
    /// Digit run riding the typed (width, value) num columns.
    Num,
    /// Hex run / UUID kept whole.
    Hex,
    /// Digit run too long for i64 — string fallback.
    BigNum,
    /// Anything else in the string stream.
    Other,
}

pub fn classify_shape(v: &Var) -> VarShape {
    match v.kind {
        VarKind::Num => VarShape::Num,
        VarKind::Str => {
            if v.text.bytes().all(|b| b.is_ascii_digit()) {
                VarShape::BigNum
            } else if is_hex_like(v.text) {
                VarShape::Hex
            } else {
                VarShape::Other
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: &str) {
        roundtrip_mode(msg, true);
        roundtrip_mode(msg, false);
    }

    fn roundtrip_mode(msg: &str, split: bool) {
        let (template, vars) = tokenize_mode(msg, split);
        let mut widths = Vec::new();
        let mut values = Vec::new();
        let mut strs = Vec::new();
        for v in &vars {
            match v.kind {
                VarKind::Num => {
                    widths.push(v.text.len() as u8);
                    values.push(v.text.parse().unwrap());
                }
                VarKind::Str => strs.push(v.text.to_string()),
            }
        }
        let (mut nc, mut sc) = (0, 0);
        let back = detokenize(&template, &widths, &values, &mut nc, &strs, &mut sc).unwrap();
        assert_eq!(back, msg, "template was {template:?}");
        assert_eq!(nc, values.len());
        assert_eq!(sc, strs.len());
        let kinds = slot_kinds(&template);
        assert_eq!(kinds.len(), vars.len());
        for (k, v) in kinds.iter().zip(&vars) {
            assert_eq!(*k, v.kind);
        }
    }

    #[test]
    fn roundtrips_exactly() {
        for msg in [
            "user 4821 logged in from 10.0.0.5",
            "user 04821 logged in from 10.0.0.5:8080",
            "big -9223372036854775808 and 9223372036854775807",
            "overflow 99999999999999999999 stays a string",
            "small -42, -0, 007, 0, 00",
            "latency 3.14ms for /api/v2/users/91",
            "blk_-3544583377289625738 replicated to 10.251.31.5:50010",
            "2017-07-01T12:00:00.123+02:00 job_1445144423722_0020 done",
            "uuid 550e8400-e29b-41d4-a716-446655440000 hex 0xdeadbeef raw deadbeef42",
            "unicode: héllo wörld 42 — em—dash and 日本語 123",
            "",
            "no digits at all, just words.",
            "   leading and trailing   ",
            "a\nmulti-line\nstack trace\n  at foo.bar(Baz.java:123)",
            "tab\tseparated\t99",
        ] {
            roundtrip(msg);
        }
    }

    #[test]
    fn zero_padding_shares_a_template() {
        // The whole point of the width column: padded and unpadded
        // digit runs land in the SAME template.
        let (t1, _) = tokenize("2017-07-01 12:07:33");
        let (t2, _) = tokenize("2017-11-23 04:00:09");
        assert_eq!(t1, t2);
        // ...while hex ids of differing digit layout also share one.
        let (t3, _) = tokenize("id 550e8400 done");
        let (t4, _) = tokenize("id a1b2c3d4 done");
        assert_eq!(t3, t4);
    }

    #[test]
    fn sentinel_in_source_is_escaped() {
        let evil = format!("bad {}byte 42 and doubled {}{} end", '\x11', '\x11', '\x11');
        roundtrip(&evil);
        roundtrip("x\x11123\x11y");
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
        for _ in 0..2000 {
            let len = (next() % 80) as usize;
            let msg: String = (0..len)
                .map(|_| alphabet[(next() as usize) % alphabet.len()])
                .collect();
            roundtrip(&msg);
        }
    }

    #[test]
    fn num_run_gate() {
        assert!(num_run_ok("0"));
        assert!(num_run_ok("007"));
        assert!(num_run_ok("3544583377289625738")); // 19-digit block id
        assert!(!num_run_ok("9999999999999999999")); // i64 overflow
        assert!(!num_run_ok(&"0".repeat(256))); // width > u8
    }

    #[test]
    fn shapes() {
        let (_, vars) =
            tokenize("x 42 d3adbeef 99999999999999999999 blk_-35445833772896257380000000000000");
        let shapes: Vec<VarShape> = vars.iter().map(classify_shape).collect();
        assert_eq!(
            shapes,
            vec![
                VarShape::Num,
                VarShape::Hex,
                VarShape::BigNum,
                VarShape::BigNum
            ]
        );
    }
}
