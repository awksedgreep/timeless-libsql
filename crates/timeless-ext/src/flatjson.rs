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

use std::collections::HashMap;

use timeless_core::Labels;

/// Serialize labels back to a canonical JSON string: keys in BTreeMap
/// (sorted) order, minimal escaping. Canonical form means equal label
/// sets always render byte-identical, so it is safe to compare/GROUP BY.
pub(crate) fn labels_to_json(labels: &Labels) -> String {
    pairs_to_json_iter(labels.iter().map(|(k, v)| (k.as_str(), v.as_str())), labels.len())
}

/// Same canonical serialization for a SORTED slice of (key, value)
/// pairs — the logs engine's metadata shape.
pub(crate) fn pairs_to_json(pairs: &[(String, String)]) -> String {
    pairs_to_json_iter(pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())), pairs.len())
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

/// Character-cursor over the input; the parse functions below advance it.
struct JsonCursor {
    chars: Vec<char>,
    pos: usize,
}

impl JsonCursor {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, want: char) -> Result<(), String> {
        match self.bump() {
            Some(c) if c == want => Ok(()),
            Some(c) => Err(format!("labels JSON: expected '{want}', found '{c}'")),
            None => Err(format!("labels JSON: expected '{want}', found end of input")),
        }
    }

    /// Parse a JSON string (cursor on the opening quote).
    fn parse_string(&mut self) -> Result<String, String> {
        self.expect('"')?;
        let mut out = String::new();
        loop {
            match self.bump() {
                None => return Err("labels JSON: unterminated string".into()),
                Some('"') => return Ok(out),
                Some('\\') => match self.bump() {
                    Some('"') => out.push('"'),
                    Some('\\') => out.push('\\'),
                    Some('/') => out.push('/'),
                    Some('b') => out.push('\u{0008}'),
                    Some('f') => out.push('\u{000C}'),
                    Some('n') => out.push('\n'),
                    Some('r') => out.push('\r'),
                    Some('t') => out.push('\t'),
                    Some('u') => {
                        let mut code: u32 = 0;
                        for _ in 0..4 {
                            let d = self
                                .bump()
                                .and_then(|c| c.to_digit(16))
                                .ok_or_else(|| {
                                    "labels JSON: \\u needs 4 hex digits".to_string()
                                })?;
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
                    Some(c) => return Err(format!("labels JSON: bad escape '\\{c}'")),
                    None => return Err("labels JSON: unterminated escape".into()),
                },
                Some(c) => out.push(c),
            }
        }
    }
}

/// Parse a FLAT JSON object of string keys and string values into a map.
pub(crate) fn parse_labels_json(input: &str) -> Result<HashMap<String, String>, String> {
    let mut cur = JsonCursor {
        chars: input.chars().collect(),
        pos: 0,
    };
    let mut out = HashMap::new();

    cur.skip_ws();
    cur.expect('{')?;
    cur.skip_ws();
    if cur.peek() == Some('}') {
        cur.bump();
    } else {
        loop {
            cur.skip_ws();
            let key = cur.parse_string()?;
            cur.skip_ws();
            cur.expect(':')?;
            cur.skip_ws();
            match cur.peek() {
                Some('"') => {
                    let val = cur.parse_string()?;
                    out.insert(key, val);
                }
                Some(c @ ('{' | '[')) => {
                    return Err(format!(
                        "labels must be a FLAT JSON object of string values; \
                         found nested '{c}' at key {key:?}"
                    ));
                }
                Some(c) => {
                    return Err(format!(
                        "labels values must be JSON strings; found '{c}' at key {key:?} \
                         (numbers/booleans/null are not supported)"
                    ));
                }
                None => return Err("labels JSON: unexpected end of input".into()),
            }
            cur.skip_ws();
            match cur.bump() {
                Some(',') => continue,
                Some('}') => break,
                Some(c) => {
                    return Err(format!("labels JSON: expected ',' or '}}', found '{c}'"))
                }
                None => return Err("labels JSON: unexpected end of input".into()),
            }
        }
    }
    cur.skip_ws();
    if cur.pos != cur.chars.len() {
        return Err("labels JSON: trailing characters after object".into());
    }
    Ok(out)
}
