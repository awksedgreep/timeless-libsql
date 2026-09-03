//! Prometheus exposition text-format parser (pure functions, no engine
//! state — extracted from `engine.rs` so the 5,000-line `Engine` impl
//! stays query-focused).
//!
//! Mirrors c_src/prometheus_nif.cpp semantics: entries are
//! (name, [(label_key, label_value)], value, timestamp), timestamp 0 when
//! absent, IEEE float values preserved, malformed non-comment lines counted
//! as errors.

/// Parse a Prometheus sample value. Non-finite IEEE values are valid
/// Prometheus samples and the Rust/libSQL data plane preserves their bits.
fn parse_prom_value(bytes: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse().ok()
}

fn take_prom_quoted(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    if bytes.first() != Some(&b'"') {
        return None;
    }
    let mut index = 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' if index + 1 < bytes.len() => index += 2,
            b'"' => return Some((&bytes[1..index], &bytes[index + 1..])),
            _ => index += 1,
        }
    }
    None
}

fn find_prom_label_close(line: &[u8], open: usize) -> Option<usize> {
    let mut quoted = false;
    let mut index = open + 1;
    while index < line.len() {
        match line[index] {
            b'\\' if quoted && index + 1 < line.len() => index += 2,
            b'"' => {
                quoted = !quoted;
                index += 1;
            }
            b'}' if !quoted => return Some(index),
            _ => index += 1,
        }
    }
    None
}

fn trim_prom_separators(mut bytes: &[u8]) -> &[u8] {
    while bytes
        .first()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b','))
    {
        bytes = &bytes[1..];
    }
    bytes
}

/// Parse the inside of a `{key="val",key2="val2"}` label block into `out`.
/// The scanner keeps escaped bytes borrowed here. `resolve_entry` decodes the
/// three Prometheus exposition escapes only on the uncommon escaped-identity
/// path, preserving zero-allocation resolution for ordinary identities.
fn parse_prom_labels_into<'a>(mut s: &'a [u8], out: &mut Vec<(&'a [u8], &'a [u8])>) -> bool {
    loop {
        s = trim_prom_separators(s);
        if s.is_empty() {
            return true;
        }

        let (key, rest) = if s[0] == b'"' {
            let Some((key, rest)) = take_prom_quoted(s) else {
                return false;
            };
            (key, rest)
        } else {
            let Some(eq) = s.iter().position(|&b| b == b'=') else {
                return false;
            };
            let mut key = &s[..eq];
            while let [rest @ .., b' ' | b'\t'] = key {
                key = rest;
            }
            (key, &s[eq..])
        };
        if key.is_empty() {
            return false;
        }
        s = rest;
        while s.first().is_some_and(|byte| matches!(byte, b' ' | b'\t')) {
            s = &s[1..];
        }
        let Some((&b'=', rest)) = s.split_first() else {
            return false;
        };
        s = rest;
        while s.first().is_some_and(|byte| matches!(byte, b' ' | b'\t')) {
            s = &s[1..];
        }
        let Some((value, rest)) = take_prom_quoted(s) else {
            return false;
        };
        out.push((key, value));
        s = rest;
    }
}

/// Decode the three Prometheus exposition escapes (`\n`, `\\`, `\"`);
/// unknown escapes are preserved verbatim (existing ingest did not
/// reject them; tightening that is a separate compatibility decision).
pub(crate) fn unescape_prom_label_value(value: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index] != b'\\' || index + 1 >= value.len() {
            output.push(value[index]);
            index += 1;
            continue;
        }
        let escaped = value[index + 1];
        match escaped {
            b'n' => output.push(b'\n'),
            b'\\' | b'"' => output.push(escaped),
            _ => {
                // Preserve unknown escapes exactly. Existing partial-success
                // ingest did not reject them; tightening malformed-line
                // policy is a separate compatibility decision.
                output.push(b'\\');
                output.push(escaped);
            }
        }
        index += 2;
    }
    output
}

/// Parse one exposition line. Labels land in the caller's scratch buffer;
/// returns (name, value, timestamp) on success. Returns None for comments,
/// blanks, and malformed lines — the caller decides which count as errors.
fn parse_prom_line_into<'a>(
    line: &'a [u8],
    labels: &mut Vec<(&'a [u8], &'a [u8])>,
) -> Option<(&'a [u8], f64, i64)> {
    let line = line.trim_ascii();
    if line.is_empty() || line[0] == b'#' {
        return None;
    }

    let (name, rest) = if line[0] == b'{' {
        let close = find_prom_label_close(line, 0)?;
        let inside = line[1..close].trim_ascii();
        let (name, remaining) = take_prom_quoted(inside)?;
        if name.is_empty() {
            return None;
        }
        let remaining = remaining.trim_ascii();
        if !remaining.is_empty() {
            let remaining = remaining.strip_prefix(b",")?;
            if !parse_prom_labels_into(remaining, labels) {
                return None;
            }
        }
        (name, &line[close + 1..])
    } else {
        let name_end = line
            .iter()
            .position(|&b| b == b'{' || b == b' ' || b == b'\t')?;
        if name_end == 0 {
            return None;
        }
        let name = &line[..name_end];
        let rest = if line[name_end] == b'{' {
            let close = find_prom_label_close(line, name_end)?;
            if !parse_prom_labels_into(&line[name_end + 1..close], labels) {
                return None;
            }
            &line[close + 1..]
        } else {
            &line[name_end..]
        };
        (name, rest)
    };

    let mut fields = rest
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|f| !f.is_empty());
    let value = parse_prom_value(fields.next()?)?;
    let timestamp = fields
        .next()
        .and_then(|f| std::str::from_utf8(f).ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    Some((name, value, timestamp))
}

/// Streaming parse: invokes `sink` once per valid sample with borrowed
/// views into `data`. One scratch label buffer is reused across all lines,
/// so steady-state parsing performs zero heap allocations. Returns
/// (entry_count, error_count).
pub(crate) fn parse_prom_body_visit<'a, F>(data: &'a [u8], mut sink: F) -> (usize, usize)
where
    F: FnMut(&'a [u8], &[(&'a [u8], &'a [u8])], f64, i64),
{
    let mut labels: Vec<(&[u8], &[u8])> = Vec::with_capacity(16);
    let mut count = 0;
    let mut errors = 0;

    for line in data.split(|&b| b == b'\n') {
        labels.clear();
        match parse_prom_line_into(line, &mut labels) {
            Some((name, value, timestamp)) => {
                count += 1;
                sink(name, &labels, value, timestamp);
            }
            None => {
                let t = line.trim_ascii();
                if !t.is_empty() && t[0] != b'#' {
                    errors += 1;
                }
            }
        }
    }
    (count, errors)
}
