//! Strict planning for the LogsQL surface owned by the Rust logs API.
//!
//! Language syntax stays out of the SQLite extension.  This module turns a
//! supported query into the public [`QuerySpec`] storage contract and never
//! silently drops a term or pipe it does not understand.

use std::fmt;

use chrono::{DateTime, Utc};

use serde_json::Value;

use crate::{MetadataExact, QuerySpec, TimestampUnit};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogsqlOutput {
    Rows,
    Count,
}

#[derive(Clone, Debug)]
pub struct LogsqlPlan {
    pub spec: QuerySpec,
    pub output: LogsqlOutput,
    /// Distinguishes an explicit `limit`/`head` from the API default so a
    /// tighter server policy can lower only the default without rewriting a
    /// caller's request.
    pub limit_explicit: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogsqlErrorKind {
    Malformed,
    Unsupported,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogsqlError {
    pub kind: LogsqlErrorKind,
    pub message: String,
}

impl LogsqlError {
    fn malformed(message: impl Into<String>) -> Self {
        Self {
            kind: LogsqlErrorKind::Malformed,
            message: message.into(),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            kind: LogsqlErrorKind::Unsupported,
            message: message.into(),
        }
    }
}

impl fmt::Display for LogsqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LogsqlError {}

pub fn parse(query: &str, timestamp_unit: TimestampUnit) -> Result<LogsqlPlan, LogsqlError> {
    parse_at(query, timestamp_unit, now(timestamp_unit))
}

/// Parse with one request-owned evaluation instant.
///
/// This is public so embedded users and deterministic semantic tests can use
/// the same planner as the HTTP route without substituting another parser.
pub fn parse_at(
    query: &str,
    timestamp_unit: TimestampUnit,
    query_now: i64,
) -> Result<LogsqlPlan, LogsqlError> {
    let mut spec = QuerySpec {
        limit: 100,
        descending: true,
        ..QuerySpec::default()
    };
    let mut output = LogsqlOutput::Rows;
    let mut limit_explicit = false;
    let mut pipeline_stage = 0u8;
    let mut segments = pipeline_segments(query)?.into_iter();
    let base = segments.next().unwrap_or_default().trim();
    if base.is_empty() {
        return Err(LogsqlError::malformed("LogsQL query is empty"));
    }
    for term in logsql_terms(base)? {
        match term {
            LogsqlTerm::Token(token) if token == "*" => {}
            LogsqlTerm::Token(token) if token.starts_with("level:") => {
                let level = required_logsql_value(&token, "level:")?;
                if !matches!(
                    level.as_str(),
                    "debug"
                        | "info"
                        | "notice"
                        | "warning"
                        | "error"
                        | "critical"
                        | "alert"
                        | "emergency"
                ) {
                    return Err(LogsqlError::malformed(format!(
                        "unsupported LogsQL level {level:?}"
                    )));
                }
                spec.level = Some(level);
            }
            LogsqlTerm::Token(token)
                if token.starts_with("service:") && !token.starts_with("service:=") =>
            {
                spec.service = Some(required_logsql_value(&token, "service:")?);
            }
            LogsqlTerm::Token(token) if token.starts_with("_time:") => {
                let window = required_logsql_value(&token, "_time:")?;
                apply_time_filter(&mut spec, &window, timestamp_unit, query_now)?;
            }
            LogsqlTerm::Message(message) if spec.message_phrase.is_none() => {
                spec.message_phrase = Some(message);
            }
            LogsqlTerm::Message(_) => {
                return Err(LogsqlError::unsupported(
                    "multiple LogsQL message terms are not supported",
                ))
            }
            LogsqlTerm::Token(token) if token.contains(':') => {
                apply_metadata_filter(&mut spec, &token)?;
            }
            LogsqlTerm::Token(token) => {
                return Err(LogsqlError::unsupported(format!(
                    "unsupported LogsQL term {token:?}"
                )))
            }
        }
    }
    for segment in segments {
        let segment = segment.trim();
        let words: Vec<&str> = segment.split_whitespace().collect();
        match words.as_slice() {
            [command @ ("limit" | "head")] => {
                advance_pipeline(&mut pipeline_stage, 3, command)?;
                spec.limit = 10;
                limit_explicit = true;
            }
            [command @ ("limit" | "head"), value] => {
                advance_pipeline(&mut pipeline_stage, 3, command)?;
                spec.limit = parse_pipeline_usize(command, value)?;
                limit_explicit = true;
            }
            [command @ ("offset" | "skip"), value] => {
                advance_pipeline(&mut pipeline_stage, 2, command)?;
                spec.offset = parse_pipeline_usize(command, value)?;
            }
            _ if is_sort_pipe(segment) => {
                advance_pipeline(&mut pipeline_stage, 1, "sort")?;
                spec.descending = parse_time_sort(segment)?;
            }
            ["stats", function @ ("count(*)" | "count()")] if output == LogsqlOutput::Rows => {
                advance_count_pipeline(&mut pipeline_stage)?;
                let _ = function;
                output = LogsqlOutput::Count;
            }
            ["stats", function @ ("count(*)" | "count()"), "as", "total"]
            | ["stats", function @ ("count(*)" | "count()"), "total"]
                if output == LogsqlOutput::Rows =>
            {
                advance_count_pipeline(&mut pipeline_stage)?;
                let _ = function;
                output = LogsqlOutput::Count;
            }
            [] => return Err(LogsqlError::malformed("empty LogsQL pipeline")),
            _ => {
                return Err(LogsqlError::unsupported(format!(
                    "unsupported LogsQL pipeline {segment:?}"
                )))
            }
        }
    }
    Ok(LogsqlPlan {
        spec,
        output,
        limit_explicit,
    })
}

fn pipeline_segments(input: &str) -> Result<Vec<&str>, LogsqlError> {
    let mut segments = Vec::new();
    let mut start = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (index, character) in input.char_indices() {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' && delimiter != '`' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        if matches!(character, '"' | '\'' | '`') {
            quote = Some(character);
        } else if character == '|' {
            segments.push(&input[start..index]);
            start = index + character.len_utf8();
        }
    }
    if quote.is_some() {
        return Err(LogsqlError::malformed("unterminated LogsQL quoted string"));
    }
    segments.push(&input[start..]);
    Ok(segments)
}

fn parse_pipeline_usize(command: &str, value: &str) -> Result<usize, LogsqlError> {
    value
        .parse::<usize>()
        .map_err(|_| LogsqlError::malformed(format!("invalid LogsQL {command} value {value:?}")))
}

fn advance_pipeline(stage: &mut u8, next: u8, name: &str) -> Result<(), LogsqlError> {
    if next <= *stage {
        return Err(LogsqlError::unsupported(format!(
            "duplicate or out-of-order LogsQL {name} pipeline"
        )));
    }
    *stage = next;
    Ok(())
}

fn advance_count_pipeline(stage: &mut u8) -> Result<(), LogsqlError> {
    if *stage >= 2 {
        return Err(LogsqlError::unsupported(
            "P0 LogsQL count cannot follow offset or limit because those pipes change count semantics",
        ));
    }
    *stage = 4;
    Ok(())
}

fn is_sort_pipe(segment: &str) -> bool {
    segment.starts_with("sort ") || segment.starts_with("order ")
}

fn parse_time_sort(segment: &str) -> Result<bool, LogsqlError> {
    match segment {
        "sort by (_time)"
        | "sort by (_time) asc"
        | "sort (_time)"
        | "sort (_time) asc"
        | "order by (_time)"
        | "order by (_time) asc" => Ok(false),
        "sort by (_time) desc" | "sort (_time) desc" | "order by (_time) desc" => Ok(true),
        "sort by (_time asc)" | "sort (_time asc)" | "order by (_time asc)" => Ok(false),
        "sort by (_time desc)" | "sort (_time desc)" | "order by (_time desc)" => Ok(true),
        _ => Err(LogsqlError::unsupported(format!(
            "P0 LogsQL sorting supports only _time asc/desc, not {segment:?}"
        ))),
    }
}

enum LogsqlTerm {
    Token(String),
    Message(String),
}

fn logsql_terms(input: &str) -> Result<Vec<LogsqlTerm>, LogsqlError> {
    let mut raw_terms = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut time_range = false;
    for character in input.chars() {
        if let Some(delimiter) = quote {
            current.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' && delimiter != '`' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        if matches!(character, '"' | '\'' | '`') {
            quote = Some(character);
            current.push(character);
            continue;
        }
        current.push(character);
        if current.starts_with("_time:[") || current.starts_with("_time:(") {
            time_range = true;
        }
        if time_range && matches!(character, ']' | ')') {
            time_range = false;
        }
        if character.is_whitespace() && !time_range {
            current.pop();
            if !current.is_empty() {
                raw_terms.push(std::mem::take(&mut current));
            }
        }
    }
    if quote.is_some() {
        return Err(LogsqlError::malformed("unterminated LogsQL quoted string"));
    }
    if time_range {
        return Err(LogsqlError::malformed("unterminated LogsQL time range"));
    }
    if !current.is_empty() {
        raw_terms.push(current);
    }

    let mut terms = Vec::with_capacity(raw_terms.len());
    for raw in raw_terms {
        if let Some((message, consumed)) = parse_quoted_prefix(&raw)? {
            if consumed == raw.len() {
                terms.push(LogsqlTerm::Message(message));
                continue;
            }
        }
        terms.push(LogsqlTerm::Token(raw));
    }
    Ok(terms)
}

fn required_logsql_value(token: &str, prefix: &str) -> Result<String, LogsqlError> {
    let value = token
        .strip_prefix(prefix)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LogsqlError::malformed(format!("LogsQL {prefix} term requires a value")))?;
    Ok(quoted_value(value)?.unwrap_or_else(|| value.to_owned()))
}

fn apply_metadata_filter(spec: &mut QuerySpec, token: &str) -> Result<(), LogsqlError> {
    let Some((operator, typed)) = metadata_operator(token) else {
        return Err(LogsqlError::unsupported(format!(
            "unsupported LogsQL term {token:?}"
        )));
    };
    let operator_width = if typed { 2 } else { 1 };
    let field = &token[..operator];
    let value = &token[operator + operator_width..];
    let path = parse_field_path(field)?;
    let expected = parse_metadata_value(value, typed)?;

    // Reuse declared posting-list columns as a sound candidate prefilter.
    // The typed predicate remains in metadata_exact, so a JSON number `500`
    // never aliases the string `"500"` after decoding.
    if path.len() == 1 {
        let key = &path[0];
        if matches!(key.as_str(), "service" | "host" | "path" | "status") {
            if let Value::String(value) = &expected {
                if key == "service" {
                    spec.service = Some(value.clone());
                } else {
                    spec.metadata_eq.insert(key.clone(), value.clone());
                }
            }
        }
    }
    spec.metadata_exact.push(MetadataExact { path, expected });
    Ok(())
}

fn metadata_operator(token: &str) -> Option<(usize, bool)> {
    let mut quote = None;
    let mut escaped = false;
    for (index, character) in token.char_indices() {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' && delimiter != '`' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        if matches!(character, '"' | '\'' | '`') {
            quote = Some(character);
        } else if character == ':' {
            return Some((index, token[index + 1..].starts_with('=')));
        }
    }
    None
}

fn parse_field_path(field: &str) -> Result<Vec<String>, LogsqlError> {
    if field.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL metadata filter requires a field name",
        ));
    }
    if let Some(field) = quoted_value(field)? {
        if field.is_empty() {
            return Err(LogsqlError::malformed(
                "LogsQL metadata filter requires a non-empty field name",
            ));
        }
        return Ok(vec![field]);
    }
    let path = field
        .split('.')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if path.iter().any(|segment| {
        segment.is_empty()
            || segment.chars().any(|character| {
                character.is_whitespace() || matches!(character, ':' | '"' | '\'' | '`')
            })
    }) {
        return Err(LogsqlError::malformed(format!(
            "invalid LogsQL metadata field path {field:?}"
        )));
    }
    Ok(path)
}

fn parse_metadata_value(value: &str, typed: bool) -> Result<Value, LogsqlError> {
    if value.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL metadata filter requires a value; quote an empty string as \"\"",
        ));
    }
    if let Some(value) = quoted_value(value)? {
        return Ok(Value::String(value));
    }
    if !typed {
        return Ok(Value::String(value.to_owned()));
    }
    match value {
        "null" => Ok(Value::Null),
        "true" => Ok(Value::Bool(true)),
        "false" => Ok(Value::Bool(false)),
        _ => match serde_json::from_str::<Value>(value) {
            Ok(value @ Value::Number(_)) => Ok(value),
            Ok(Value::Array(_) | Value::Object(_)) => Err(LogsqlError::unsupported(
                "P0 typed LogsQL equality supports JSON primitives; select a nested leaf for arrays or objects",
            )),
            Ok(Value::String(value)) => Ok(Value::String(value)),
            Ok(Value::Null | Value::Bool(_)) => unreachable!("handled above"),
            Err(_) => Ok(Value::String(value.to_owned())),
        },
    }
}

fn quoted_value(value: &str) -> Result<Option<String>, LogsqlError> {
    let Some((decoded, consumed)) = parse_quoted_prefix(value)? else {
        return Ok(None);
    };
    if consumed != value.len() {
        return Err(LogsqlError::malformed(format!(
            "unexpected characters after LogsQL quoted value {value:?}"
        )));
    }
    Ok(Some(decoded))
}

fn parse_quoted_prefix(value: &str) -> Result<Option<(String, usize)>, LogsqlError> {
    let Some(delimiter) = value.chars().next() else {
        return Ok(None);
    };
    if !matches!(delimiter, '"' | '\'' | '`') {
        return Ok(None);
    }
    let mut escaped = false;
    for (relative, character) in value[delimiter.len_utf8()..].char_indices() {
        let index = delimiter.len_utf8() + relative;
        if delimiter != '`' {
            if escaped {
                escaped = false;
                continue;
            }
            if character == '\\' {
                escaped = true;
                continue;
            }
        }
        if character == delimiter {
            let inner = &value[delimiter.len_utf8()..index];
            let decoded = if delimiter == '`' {
                inner.to_owned()
            } else {
                decode_quoted_escapes(inner)?
            };
            return Ok(Some((decoded, index + delimiter.len_utf8())));
        }
    }
    Err(LogsqlError::malformed(format!(
        "unterminated LogsQL quoted value {value:?}"
    )))
}

fn decode_quoted_escapes(value: &str) -> Result<String, LogsqlError> {
    let mut decoded = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            let character = value[index..]
                .chars()
                .next()
                .expect("index remains on a UTF-8 boundary");
            let width = character.len_utf8();
            decoded.extend_from_slice(&bytes[index..index + width]);
            index += width;
            continue;
        }
        index += 1;
        let escape = *bytes.get(index).ok_or_else(|| {
            LogsqlError::malformed("LogsQL quoted value ends with an incomplete escape")
        })?;
        index += 1;
        match escape {
            b'a' => decoded.push(0x07),
            b'b' => decoded.push(0x08),
            b'f' => decoded.push(0x0c),
            b'n' => decoded.push(b'\n'),
            b'r' => decoded.push(b'\r'),
            b't' => decoded.push(b'\t'),
            b'v' => decoded.push(0x0b),
            b'\\' => decoded.push(b'\\'),
            b'\'' => decoded.push(b'\''),
            b'"' => decoded.push(b'"'),
            b'x' => {
                let value = decode_digits(bytes, &mut index, 2, 16)?;
                decoded.push(value as u8);
            }
            b'u' => {
                let value = decode_digits(bytes, &mut index, 4, 16)?;
                push_unicode_escape(&mut decoded, value)?;
            }
            b'U' => {
                let value = decode_digits(bytes, &mut index, 8, 16)?;
                push_unicode_escape(&mut decoded, value)?;
            }
            b'0'..=b'7' => {
                index -= 1;
                let value = decode_digits(bytes, &mut index, 3, 8)?;
                if value > u8::MAX as u32 {
                    return Err(LogsqlError::malformed(
                        "LogsQL octal escape exceeds one byte",
                    ));
                }
                decoded.push(value as u8);
            }
            _ => {
                return Err(LogsqlError::malformed(format!(
                    "invalid LogsQL quoted escape \\{}",
                    char::from(escape)
                )))
            }
        }
    }
    String::from_utf8(decoded).map_err(|_| {
        LogsqlError::unsupported(
            "LogsQL byte escape decodes to non-UTF-8 data, which the retained log model cannot store",
        )
    })
}

fn decode_digits(
    bytes: &[u8],
    index: &mut usize,
    count: usize,
    radix: u32,
) -> Result<u32, LogsqlError> {
    let end = index
        .checked_add(count)
        .ok_or_else(|| LogsqlError::malformed("LogsQL quoted numeric escape length overflows"))?;
    let digits = bytes
        .get(*index..end)
        .ok_or_else(|| LogsqlError::malformed("incomplete LogsQL quoted numeric escape"))?;
    let mut value = 0u32;
    for digit in digits {
        let digit = char::from(*digit).to_digit(radix).ok_or_else(|| {
            LogsqlError::malformed("invalid digit in LogsQL quoted numeric escape")
        })?;
        value = value * radix + digit;
    }
    *index = end;
    Ok(value)
}

fn push_unicode_escape(decoded: &mut Vec<u8>, value: u32) -> Result<(), LogsqlError> {
    let character = char::from_u32(value).ok_or_else(|| {
        LogsqlError::malformed(format!("invalid LogsQL Unicode escape U+{value:04X}"))
    })?;
    let mut buffer = [0; 4];
    decoded.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
    Ok(())
}

fn parse_duration_ms(value: &str) -> Option<i64> {
    let split = value.find(|character: char| !character.is_ascii_digit())?;
    let count: i64 = value[..split].parse().ok()?;
    let multiplier = match &value[split..] {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    count.checked_mul(multiplier)
}

fn apply_time_filter(
    spec: &mut QuerySpec,
    value: &str,
    timestamp_unit: TimestampUnit,
    query_now: i64,
) -> Result<(), LogsqlError> {
    if matches!(value.as_bytes().first(), Some(b'[' | b'(')) {
        let start_inclusive = value.starts_with('[');
        let end_inclusive = value.ends_with(']');
        if !matches!(value.as_bytes().last(), Some(b']' | b')')) {
            return Err(LogsqlError::malformed(format!(
                "unterminated LogsQL time range {value:?}"
            )));
        }
        let inner = &value[1..value.len() - 1];
        let (start, end) = inner.split_once(',').ok_or_else(|| {
            LogsqlError::malformed(format!("LogsQL time range requires two bounds: {value:?}"))
        })?;
        let mut start = parse_absolute_time(start.trim(), timestamp_unit)?;
        let mut end = parse_absolute_time(end.trim(), timestamp_unit)?;
        if !start_inclusive {
            start = start.checked_add(1).ok_or_else(|| {
                LogsqlError::malformed("exclusive LogsQL lower time bound overflows")
            })?;
        }
        if !end_inclusive {
            end = end.checked_sub(1).ok_or_else(|| {
                LogsqlError::malformed("exclusive LogsQL upper time bound underflows")
            })?;
        }
        tighten_time_bounds(spec, Some(start), Some(end))?;
        return Ok(());
    }

    for (operator, lower, inclusive) in [
        (">=", true, true),
        ("<=", false, true),
        (">", true, false),
        ("<", false, false),
    ] {
        let Some(bound) = value.strip_prefix(operator) else {
            continue;
        };
        let mut bound = parse_absolute_time(bound.trim(), timestamp_unit)?;
        if !inclusive {
            bound = if lower {
                bound.checked_add(1).ok_or_else(|| {
                    LogsqlError::malformed("exclusive LogsQL lower time bound overflows")
                })?
            } else {
                bound.checked_sub(1).ok_or_else(|| {
                    LogsqlError::malformed("exclusive LogsQL upper time bound underflows")
                })?
            };
        }
        return tighten_time_bounds(spec, lower.then_some(bound), (!lower).then_some(bound));
    }

    let duration_ms = parse_duration_ms(value).ok_or_else(|| {
        LogsqlError::malformed(format!("unsupported LogsQL time window {value:?}"))
    })?;
    let upper = query_now
        .checked_sub(1)
        .ok_or_else(|| LogsqlError::malformed("relative LogsQL upper time bound underflows"))?;
    if duration_ms == 0 {
        // The upstream interval is [now, now), which is valid but empty.
        // Preserve that distinction from a contradictory user-supplied
        // absolute range: inclusive storage bounds execute this as an empty,
        // bounded scan.
        spec.ts_min = Some(query_now);
        spec.ts_max = Some(upper);
        return Ok(());
    }
    tighten_time_bounds(
        spec,
        Some(query_now.saturating_sub(duration_from_millis(duration_ms, timestamp_unit))),
        Some(upper),
    )
}

fn parse_absolute_time(value: &str, timestamp_unit: TimestampUnit) -> Result<i64, LogsqlError> {
    if value.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL absolute time bound must not be empty",
        ));
    }
    if let Ok(value) = value.parse::<i64>() {
        return Ok(normalize_integer_time(value, timestamp_unit));
    }
    DateTime::parse_from_rfc3339(value)
        .map(|datetime| micros_to_native(datetime.timestamp_micros(), timestamp_unit))
        .map_err(|error| {
            LogsqlError::malformed(format!("invalid LogsQL absolute time {value:?}: {error}"))
        })
}

fn normalize_integer_time(timestamp: i64, timestamp_unit: TimestampUnit) -> i64 {
    let magnitude = timestamp.unsigned_abs();
    let micros = if magnitude < 100_000_000_000 {
        timestamp.saturating_mul(1_000_000)
    } else if magnitude < 100_000_000_000_000 {
        timestamp.saturating_mul(1_000)
    } else if magnitude < 100_000_000_000_000_000 {
        timestamp
    } else {
        timestamp / 1_000
    };
    micros_to_native(micros, timestamp_unit)
}

fn tighten_time_bounds(
    spec: &mut QuerySpec,
    lower: Option<i64>,
    upper: Option<i64>,
) -> Result<(), LogsqlError> {
    if let Some(lower) = lower {
        spec.ts_min = Some(spec.ts_min.map_or(lower, |current| current.max(lower)));
    }
    if let Some(upper) = upper {
        spec.ts_max = Some(spec.ts_max.map_or(upper, |current| current.min(upper)));
    }
    if matches!((spec.ts_min, spec.ts_max), (Some(lower), Some(upper)) if lower > upper) {
        return Err(LogsqlError::malformed(
            "LogsQL time range has no representable timestamps",
        ));
    }
    Ok(())
}

fn now(timestamp_unit: TimestampUnit) -> i64 {
    micros_to_native(Utc::now().timestamp_micros(), timestamp_unit)
}

fn micros_to_native(micros: i64, timestamp_unit: TimestampUnit) -> i64 {
    match timestamp_unit {
        TimestampUnit::Milliseconds => micros / 1_000,
        TimestampUnit::Microseconds => micros,
    }
}

fn duration_from_millis(duration: i64, timestamp_unit: TimestampUnit) -> i64 {
    match timestamp_unit {
        TimestampUnit::Milliseconds => duration,
        TimestampUnit::Microseconds => duration.saturating_mul(1_000),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn workload_shapes_preserve_the_existing_strict_subset() {
        let plan = parse(
            "_time:5m level:error | limit 100",
            TimestampUnit::Microseconds,
        )
        .unwrap();
        assert_eq!(plan.spec.level.as_deref(), Some("error"));
        assert_eq!(plan.spec.limit, 100);
        assert!(plan.spec.ts_min.is_some());
        assert_eq!(plan.output, LogsqlOutput::Rows);

        let plan = parse(
            "_time:1h level:error | stats count(*)",
            TimestampUnit::Microseconds,
        )
        .unwrap();
        assert_eq!(plan.spec.level.as_deref(), Some("error"));
        assert_eq!(plan.output, LogsqlOutput::Count);

        let plan = parse(
            "_time:15m \"timeout\" | limit 50",
            TimestampUnit::Microseconds,
        )
        .unwrap();
        assert_eq!(plan.spec.message_phrase.as_deref(), Some("timeout"));

        for unsupported in [
            "level:error | unpack_json",
            "level:error or level:critical",
            "_time:5q",
            "level:made-up",
        ] {
            assert!(
                parse(unsupported, TimestampUnit::Microseconds).is_err(),
                "{unsupported:?} silently broadened"
            );
        }
    }

    #[test]
    fn relative_time_uses_the_injected_query_clock_exactly() {
        let plan = parse_at(
            "_time:5m",
            TimestampUnit::Microseconds,
            1_800_000_000_123_456,
        )
        .unwrap();
        assert_eq!(plan.spec.ts_min, Some(1_799_999_700_123_456));
        assert_eq!(plan.spec.ts_max, Some(1_800_000_000_123_455));
        assert_eq!(plan.output, LogsqlOutput::Rows);

        let empty = parse_at(
            "_time:0s",
            TimestampUnit::Microseconds,
            1_800_000_000_123_456,
        )
        .unwrap();
        assert_eq!(empty.spec.ts_min, Some(1_800_000_000_123_456));
        assert_eq!(empty.spec.ts_max, Some(1_800_000_000_123_455));
    }

    #[test]
    fn integer_absolute_times_accept_unix_seconds_millis_micros_and_nanos() {
        for (value, expected) in [
            ("1800000001", 1_800_000_001_000_000),
            ("1800000001000", 1_800_000_001_000_000),
            ("1800000001000000", 1_800_000_001_000_000),
            ("1800000001000000000", 1_800_000_001_000_000),
        ] {
            let plan =
                parse_at(&format!("_time:>={value}"), TimestampUnit::Microseconds, 0).unwrap();
            assert_eq!(plan.spec.ts_min, Some(expected), "{value}");
        }

        let milliseconds =
            parse_at("_time:>=1800000001000000", TimestampUnit::Milliseconds, 0).unwrap();
        assert_eq!(milliseconds.spec.ts_min, Some(1_800_000_001_000));
    }

    #[test]
    fn absolute_ranges_preserve_open_and_closed_microsecond_edges() {
        let closed_open = parse_at(
            "_time:[2027-01-15T08:00:00.000001Z, 2027-01-15T08:00:00.000004Z)",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(closed_open.spec.ts_min, Some(1_800_000_000_000_001));
        assert_eq!(closed_open.spec.ts_max, Some(1_800_000_000_000_003));

        let open_closed = parse_at(
            "_time:(2027-01-15T08:00:00.000001Z, 2027-01-15T08:00:00.000004Z]",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(open_closed.spec.ts_min, Some(1_800_000_000_000_002));
        assert_eq!(open_closed.spec.ts_max, Some(1_800_000_000_000_004));

        for malformed in [
            "_time:[2027-01-15T08:00:00Z,broken)",
            "_time:[2027-01-15T08:00:01Z,2027-01-15T08:00:00Z]",
            "_time:[2027-01-15T08:00:00Z,2027-01-15T08:00:01Z",
        ] {
            assert!(
                parse_at(malformed, TimestampUnit::Microseconds, 0).is_err(),
                "{malformed:?} was accepted"
            );
        }
    }

    #[test]
    fn comparison_time_bounds_intersect_without_losing_exclusivity() {
        let plan = parse_at(
            "_time:>2027-01-15T08:00:00.000001Z _time:<=2027-01-15T08:00:00.000004Z",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(plan.spec.ts_min, Some(1_800_000_000_000_002));
        assert_eq!(plan.spec.ts_max, Some(1_800_000_000_000_004));

        let plan = parse_at(
            "_time:>=2027-01-15T08:00:00.000001Z _time:<2027-01-15T08:00:00.000004Z",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(plan.spec.ts_min, Some(1_800_000_000_000_001));
        assert_eq!(plan.spec.ts_max, Some(1_800_000_000_000_003));

        assert!(parse_at(
            "_time:>=2027-01-15T08:00:01Z _time:<2027-01-15T08:00:00Z",
            TimestampUnit::Microseconds,
            0,
        )
        .is_err());
    }

    #[test]
    fn arbitrary_metadata_equality_keeps_paths_and_json_primitive_types() {
        let plan = parse_at(
            r#"app:"my app" nested.ok:=true nested.count:=2 nested.none:=null nested.empty:"""#,
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(
            plan.spec.metadata_exact,
            vec![
                crate::MetadataExact {
                    path: vec!["app".into()],
                    expected: json!("my app"),
                },
                crate::MetadataExact {
                    path: vec!["nested".into(), "ok".into()],
                    expected: json!(true),
                },
                crate::MetadataExact {
                    path: vec!["nested".into(), "count".into()],
                    expected: json!(2),
                },
                crate::MetadataExact {
                    path: vec!["nested".into(), "none".into()],
                    expected: json!(null),
                },
                crate::MetadataExact {
                    path: vec!["nested".into(), "empty".into()],
                    expected: json!(""),
                },
            ]
        );
        for malformed in ["nested..ok:true", "nested.ok:=", ":value"] {
            assert!(
                parse_at(malformed, TimestampUnit::Microseconds, 0).is_err(),
                "{malformed:?} was accepted"
            );
        }
    }

    #[test]
    fn quoted_message_is_a_case_sensitive_phrase_not_legacy_contains() {
        let plan = parse_at(r#""ssh: login fail""#, TimestampUnit::Microseconds, 0).unwrap();
        assert_eq!(plan.spec.message_phrase.as_deref(), Some("ssh: login fail"));
        assert_eq!(plan.spec.message, None);
    }

    #[test]
    fn logs_ql_literals_decode_escapes_and_quoted_field_identifiers() {
        let double = parse_at(
            r#""line one\nline two\t\"quoted\"\\slash""#,
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(
            double.spec.message_phrase.as_deref(),
            Some("line one\nline two\t\"quoted\"\\slash")
        );

        let single = parse_at(
            r#"'single\'quote \x41 \u03bb'"#,
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(
            single.spec.message_phrase.as_deref(),
            Some("single'quote A λ")
        );

        let raw = parse_at(
            r#"`raw\n"double"'single\slash`"#,
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(
            raw.spec.message_phrase.as_deref(),
            Some(r#"raw\n"double"'single\slash"#)
        );

        let pipe = parse_at(r#""left|right""#, TimestampUnit::Microseconds, 0).unwrap();
        assert_eq!(pipe.spec.message_phrase.as_deref(), Some("left|right"));

        let field = parse_at(r#""log:level":="error""#, TimestampUnit::Microseconds, 0).unwrap();
        assert_eq!(
            field.spec.metadata_exact,
            vec![crate::MetadataExact {
                path: vec!["log:level".into()],
                expected: json!("error"),
            }]
        );

        for malformed in [r#""bad\q""#, r#"'bad\q'"#, r#""bad\uD800""#] {
            assert!(
                parse_at(malformed, TimestampUnit::Microseconds, 0).is_err(),
                "{malformed:?} was accepted"
            );
        }
    }

    #[test]
    fn logs_ql_pagination_sort_and_count_pipes_are_strict() {
        let asc = parse_at(
            "* | sort by (_time) asc | offset 2 | limit 3",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert!(!asc.spec.descending);
        assert_eq!(asc.spec.offset, 2);
        assert_eq!(asc.spec.limit, 3);
        assert_eq!(asc.output, LogsqlOutput::Rows);

        let aliases = parse_at(
            "* | order by (_time) desc | skip 4 | head 5",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert!(aliases.spec.descending);
        assert_eq!(aliases.spec.offset, 4);
        assert_eq!(aliases.spec.limit, 5);

        let default_head = parse_at("* | head", TimestampUnit::Microseconds, 0).unwrap();
        assert_eq!(default_head.spec.limit, 10);

        let count = parse_at(
            "level:error | stats count() as total",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(count.output, LogsqlOutput::Count);

        for invalid in [
            "* | offset -1",
            "* | sort by (service) asc",
            "* | sort by (_time) sideways",
            "* | limit 2 | offset 1",
            "* | stats count() as other",
        ] {
            assert!(
                parse_at(invalid, TimestampUnit::Microseconds, 0).is_err(),
                "{invalid:?} was accepted"
            );
        }
    }
}
