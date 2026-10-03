//! Flat JSON objects (`{"key":"value", ...}`) <-> string maps/pairs,
//! WITHOUT serde. Shared by the metrics vtab (labels) and the logs vtab
//! (metadata) — one parser means the two tables can never disagree
//! about what a flat JSON object means.
//!
//! A whole serde dependency for `{"key":"value"}` objects would be the
//! heaviest crate in the extension. Instead: a tiny hand parser.
//!
//! KNOWN LIMITS (deliberate — reject rather than misparse):
//!   - values must be strings: numbers, booleans, null, nested objects
//!     and arrays are errors ("flat JSON object of string values" only);
//!   - \uXXXX escapes cover the Basic Multilingual Plane only —
//!     surrogate pairs (emoji etc. written as 😀) are
//!     rejected; literal UTF-8 in the string works fine;
//!   - duplicate keys: last one wins (like most JSON parsers).

use std::borrow::Cow;
use std::collections::HashMap;

use timeless_core::Labels;

/// Serialize labels back to a canonical JSON string: keys in BTreeMap
/// (sorted) order, minimal escaping. Canonical form means equal label
/// sets always render byte-identical, so it is safe to compare/GROUP BY.
pub(crate) fn labels_to_json(labels: &Labels) -> String {
    pairs_to_json_iter(
        labels.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        labels.len(),
    )
}

/// Same canonical serialization for a SORTED slice of (key, value)
/// pairs — the logs engine's metadata shape.
pub(crate) fn pairs_to_json(pairs: &[(String, String)]) -> String {
    pairs_to_json_iter(
        pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        pairs.len(),
    )
}

fn pairs_to_json_iter<'a>(pairs: impl Iterator<Item = (&'a str, &'a str)>, len: usize) -> String {
    let mut out = String::with_capacity(2 + len * 16);
    out.push('{');
    let mut first = true;
    for (k, v) in pairs {
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        json_escape_into(&mut out, k);
        out.push_str("\":\"");
        json_escape_into(&mut out, v);
        out.push('"');
    }
    out.push('}');
    out
}

fn json_escape_into(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
}

/// Byte-cursor over the input; the parse functions below advance it.
/// No copy of the input is made: structural scanning is byte-wise (safe
/// because UTF-8 continuation bytes never collide with ASCII syntax),
/// and `parse_string` borrows the slice directly unless escapes force an
/// owned copy. The old `Vec<char>` copy cost 4x the input on every
/// labels/filter parse.
struct JsonCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> JsonCursor<'a> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, want: u8) -> Result<(), String> {
        match self.bump() {
            Some(c) if c == want => Ok(()),
            Some(c) => Err(format!(
                "labels JSON: expected '{}', found '{}'",
                want as char, c as char
            )),
            None => Err(format!(
                "labels JSON: expected '{}', found end of input",
                want as char
            )),
        }
    }

    /// Parse a JSON string (cursor on the opening quote). Borrows the
    /// input slice when it holds no escapes; otherwise decodes escapes
    /// into a fresh `String`. Invalid UTF-8 is rejected either way.
    fn parse_string(&mut self) -> Result<Cow<'a, str>, String> {
        self.expect(b'"')?;
        let start = self.pos;
        // Fast scan for the terminator or the first escape.
        let mut end = start;
        let mut escaped = false;
        while let Some(c) = self.bytes.get(end) {
            match c {
                b'"' => break,
                b'\\' => {
                    escaped = true;
                    break;
                }
                _ => end += 1,
            }
        }
        if !escaped {
            let raw = self
                .bytes
                .get(start..end)
                .ok_or_else(|| "labels JSON: unterminated string".to_string())?;
            if self.bytes.get(end) != Some(&b'"') {
                return Err("labels JSON: unterminated string".into());
            }
            let s = std::str::from_utf8(raw)
                .map_err(|_| "labels JSON: invalid UTF-8 in string".to_string())?;
            self.pos = end + 1;
            return Ok(Cow::Borrowed(s));
        }
        // Slow path: escapes present — decode into an owned string.
        let mut out = String::new();
        // Re-validate the pre-escape run as part of the owned copy.
        let head = self
            .bytes
            .get(start..end)
            .ok_or_else(|| "labels JSON: unterminated string".to_string())?;
        out.push_str(
            std::str::from_utf8(head)
                .map_err(|_| "labels JSON: invalid UTF-8 in string".to_string())?,
        );
        self.pos = end;
        loop {
            match self.bump() {
                None => return Err("labels JSON: unterminated string".into()),
                Some(b'"') => return Ok(Cow::Owned(out)),
                Some(b'\\') => match self.bump() {
                    Some(b'"') => out.push('"'),
                    Some(b'\\') => out.push('\\'),
                    Some(b'/') => out.push('/'),
                    Some(b'b') => out.push('\u{0008}'),
                    Some(b'f') => out.push('\u{000C}'),
                    Some(b'n') => out.push('\n'),
                    Some(b'r') => out.push('\r'),
                    Some(b't') => out.push('\t'),
                    Some(b'u') => {
                        let mut code: u32 = 0;
                        for _ in 0..4 {
                            let d = self
                                .bump()
                                .and_then(|c| (c as char).to_digit(16))
                                .ok_or_else(|| "labels JSON: \\u needs 4 hex digits".to_string())?;
                            code = code * 16 + d;
                        }
                        // Surrogate halves are not valid chars on their
                        // own; pairing them is more parser than labels
                        // deserve. Use literal UTF-8 instead.
                        let c = char::from_u32(code).ok_or_else(|| {
                            format!(
                                "labels JSON: \\u{code:04x} is a surrogate half; \
                                 surrogate pairs unsupported, use literal UTF-8"
                            )
                        })?;
                        out.push(c);
                    }
                    Some(c) => return Err(format!("labels JSON: bad escape '\\{}'", c as char)),
                    None => return Err("labels JSON: unterminated escape".into()),
                },
                Some(c) => {
                    // Raw byte of a (possibly multi-byte) run: accumulate
                    // bytes and validate UTF-8 at the next boundary.
                    // Collect the maximal raw run first for one check.
                    let run_start = self.pos - 1;
                    let mut run_end = self.pos;
                    while let Some(b) = self.bytes.get(run_end) {
                        if *b == b'"' || *b == b'\\' {
                            break;
                        }
                        run_end += 1;
                    }
                    let raw = self
                        .bytes
                        .get(run_start..run_end)
                        .ok_or_else(|| "labels JSON: unterminated string".to_string())?;
                    out.push_str(
                        std::str::from_utf8(raw)
                            .map_err(|_| "labels JSON: invalid UTF-8 in string".to_string())?,
                    );
                    self.pos = run_end;
                    let _ = c;
                }
            }
        }
    }
}

/// Parse a FLAT JSON object of string keys and string values into a map.
pub(crate) fn parse_labels_json(input: &str) -> Result<HashMap<String, String>, String> {
    let mut cur = JsonCursor {
        bytes: input.as_bytes(),
        pos: 0,
    };
    let mut out = HashMap::new();

    cur.skip_ws();
    cur.expect(b'{')?;
    cur.skip_ws();
    if cur.peek() == Some(b'}') {
        cur.bump();
    } else {
        loop {
            cur.skip_ws();
            let key = cur.parse_string()?;
            cur.skip_ws();
            cur.expect(b':')?;
            cur.skip_ws();
            match cur.peek() {
                Some(b'"') => {
                    let val = cur.parse_string()?;
                    out.insert(key.into_owned(), val.into_owned());
                }
                Some(c @ (b'{' | b'[')) => {
                    return Err(format!(
                        "labels must be a FLAT JSON object of string values; \
                         found nested '{}' at key {key:?}",
                        c as char
                    ));
                }
                Some(c) => {
                    return Err(format!(
                        "labels values must be JSON strings; found '{}' at key {key:?} \
                         (numbers/booleans/null are not supported)",
                        c as char
                    ));
                }
                None => return Err("labels JSON: unexpected end of input".into()),
            }
            cur.skip_ws();
            match cur.bump() {
                Some(b',') => continue,
                Some(b'}') => break,
                Some(c) => {
                    return Err(format!(
                        "labels JSON: expected ',' or '}}', found '{}'",
                        c as char
                    ))
                }
                None => return Err("labels JSON: unexpected end of input".into()),
            }
        }
    }
    cur.skip_ws();
    if cur.pos != cur.bytes.len() {
        return Err("labels JSON: trailing characters after object".into());
    }
    Ok(out)
}

/// One TVF filter matcher, parsed but not compiled — regex compilation
/// (and the `regex` dependency) lives in query_tvf.rs. Plain string
/// values stay equality, so every pre-F8 filter parses identically.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MatcherSpec {
    Eq(String),
    Neq(String),
    Re(String),
    Nre(String),
}

/// Parse a TVF filter object: values are either plain strings (eq) or
/// single-operator objects `{"neq"|"re"|"nre": "..."}`. This is the ONE
/// place filter JSON grows beyond flat — vtab labels themselves stay on
/// `parse_labels_json` and remain strictly flat.
pub(crate) fn parse_matchers_json(input: &str) -> Result<Vec<(String, MatcherSpec)>, String> {
    let mut cur = JsonCursor {
        bytes: input.as_bytes(),
        pos: 0,
    };
    let mut out: Vec<(String, MatcherSpec)> = Vec::new();

    cur.skip_ws();
    cur.expect(b'{')?;
    cur.skip_ws();
    if cur.peek() == Some(b'}') {
        cur.bump();
    } else {
        loop {
            cur.skip_ws();
            let key = cur.parse_string()?;
            cur.skip_ws();
            cur.expect(b':')?;
            cur.skip_ws();
            let spec = match cur.peek() {
                Some(b'"') => MatcherSpec::Eq(cur.parse_string()?.into_owned()),
                Some(b'{') => {
                    cur.bump();
                    cur.skip_ws();
                    let op = cur.parse_string()?;
                    cur.skip_ws();
                    cur.expect(b':')?;
                    cur.skip_ws();
                    let val = match cur.peek() {
                        Some(b'"') => cur.parse_string()?.into_owned(),
                        _ => {
                            return Err(format!(
                                "filter: operator value for {op:?} at key {key:?} \
                                 must be a JSON string"
                            ))
                        }
                    };
                    cur.skip_ws();
                    match cur.bump() {
                        Some(b'}') => {}
                        _ => {
                            return Err(format!(
                                "filter: matcher object at key {key:?} must hold exactly \
                                 one operator ({{\"neq\"|\"re\"|\"nre\": \"...\"}})"
                            ))
                        }
                    }
                    match op.as_ref() as &str {
                        "neq" => MatcherSpec::Neq(val),
                        "re" => MatcherSpec::Re(val),
                        "nre" => MatcherSpec::Nre(val),
                        other => {
                            return Err(format!(
                                "filter: unknown operator {other:?} at key {key:?}; \
                                 valid operators: neq, re, nre (plain string = eq)"
                            ))
                        }
                    }
                }
                Some(c) => {
                    return Err(format!(
                        "filter values must be strings or matcher objects; found '{}' \
                         at key {key:?}",
                        c as char
                    ))
                }
                None => return Err("filter JSON: unexpected end of input".into()),
            };
            out.push((key.into_owned(), spec));
            cur.skip_ws();
            match cur.bump() {
                Some(b',') => continue,
                Some(b'}') => break,
                Some(c) => {
                    return Err(format!(
                        "filter JSON: expected ',' or '}}', found '{}'",
                        c as char
                    ))
                }
                None => return Err("filter JSON: unexpected end of input".into()),
            }
        }
    }
    cur.skip_ws();
    if cur.pos != cur.bytes.len() {
        return Err("filter JSON: trailing characters after object".into());
    }
    Ok(out)
}

#[cfg(test)]
mod matcher_tests {
    use super::*;

    #[test]
    fn plain_strings_stay_equality() {
        let m = parse_matchers_json(r#"{"host":"pvm1","env":"prod"}"#).unwrap();
        assert_eq!(
            m,
            vec![
                ("host".into(), MatcherSpec::Eq("pvm1".into())),
                ("env".into(), MatcherSpec::Eq("prod".into())),
            ]
        );
    }

    #[test]
    fn operators_parse() {
        let m =
            parse_matchers_json(r#"{ "a": {"neq": "x"}, "b": {"re": "w.*"}, "c": {"nre": ""} }"#)
                .unwrap();
        assert_eq!(
            m,
            vec![
                ("a".into(), MatcherSpec::Neq("x".into())),
                ("b".into(), MatcherSpec::Re("w.*".into())),
                ("c".into(), MatcherSpec::Nre(String::new())),
            ]
        );
    }

    #[test]
    fn empty_object_is_empty() {
        assert!(parse_matchers_json("{}").unwrap().is_empty());
        assert!(parse_matchers_json(" { } ").unwrap().is_empty());
    }

    #[test]
    fn rejects_reject_loudly() {
        for (input, needle) in [
            (r#"{"a": {"like": "x"}}"#, "unknown operator"),
            (r#"{"a": {"re": "x", "neq": "y"}}"#, "exactly one operator"),
            (r#"{"a": {"re": 5}}"#, "must be a JSON string"),
            (r#"{"a": 5}"#, "strings or matcher objects"),
            (r#"{"a": {}}"#, ""),
            (r#"{"a": ["x"]}"#, ""),
        ] {
            let err = parse_matchers_json(input).unwrap_err();
            assert!(
                err.contains(needle),
                "{input}: error {err:?} should mention {needle:?}"
            );
        }
    }
}
