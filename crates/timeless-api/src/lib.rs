//! Signal-level wire parsing shared by the standalone daemon and Rust hosts.
//!
//! Storage remains in `timeless-ext`; this crate translates public protocol
//! shapes into the extension's deliberately small SQL/blob interface.

use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Level {
    Debug,
    Info,
    Warning,
    Error,
}

impl Level {
    pub fn parse_ingest(value: Option<&str>) -> Self {
        match value {
            Some("debug") => Self::Debug,
            Some("warning" | "warn") => Self::Warning,
            Some("error") => Self::Error,
            _ => Self::Info,
        }
    }

    pub fn parse_filter(value: &str) -> Result<Self, String> {
        match value {
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warning" => Ok(Self::Warning),
            "error" => Ok(Self::Error),
            other => Err(format!(
                "unknown log level {other:?}; expected debug, info, warning, or error"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }

    pub fn as_byte(self) -> u8 {
        match self {
            Self::Debug => 0,
            Self::Info => 1,
            Self::Warning => 2,
            Self::Error => 3,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogRecord {
    /// Epoch milliseconds: the native unit of the `timeless_logs` vtab.
    pub ts_ms: i64,
    pub level: Level,
    pub message: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct IngestBatch {
    pub entries: Vec<LogRecord>,
    pub errors: usize,
}

/// Parse VictoriaLogs-style NDJSON. A malformed line is isolated from its
/// neighbors, matching the existing Elixir HTTP behavior.
pub fn parse_ndjson(
    body: &[u8],
    message_field: &str,
    time_field: &str,
    now_ms: i64,
) -> IngestBatch {
    let mut batch = IngestBatch::default();
    for raw in body.split(|byte| *byte == b'\n') {
        let raw = trim_ascii(raw);
        if raw.is_empty() {
            continue;
        }
        match parse_line(raw, message_field, time_field, now_ms) {
            Ok(entry) => batch.entries.push(entry),
            Err(()) => batch.errors += 1,
        }
    }
    batch
}

fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

fn parse_line(
    raw: &[u8],
    message_field: &str,
    time_field: &str,
    now_ms: i64,
) -> Result<LogRecord, ()> {
    let Value::Object(mut object) = serde_json::from_slice(raw).map_err(|_| ())? else {
        return Err(());
    };
    let message = object
        .remove(message_field)
        .map(value_to_text)
        .unwrap_or_default();
    let ts_ms = object
        .remove(time_field)
        .as_ref()
        .and_then(parse_ingest_timestamp)
        .unwrap_or(now_ms);
    let level = Level::parse_ingest(object.remove("level").as_ref().and_then(Value::as_str));
    let metadata = object
        .into_iter()
        .map(|(key, value)| (key, value_to_text(value)))
        .collect();
    Ok(LogRecord {
        ts_ms,
        level,
        message,
        metadata,
    })
}

fn value_to_text(value: Value) -> String {
    match value {
        Value::String(value) => value,
        other => serde_json::to_string(&other).unwrap_or_default(),
    }
}

fn parse_ingest_timestamp(value: &Value) -> Option<i64> {
    match value {
        Value::Number(value) => value.as_i64().map(timestamp_number_to_ms),
        Value::String(value) => DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|time| time.timestamp_millis())
            // The Elixir endpoint treats a numeric string as epoch seconds.
            .or_else(|| value.parse::<i64>().ok().and_then(|n| n.checked_mul(1_000))),
        _ => None,
    }
}

fn timestamp_number_to_ms(value: i64) -> i64 {
    let magnitude = value.unsigned_abs();
    if magnitude < 100_000_000_000 {
        value.saturating_mul(1_000)
    } else if magnitude < 100_000_000_000_000 {
        value
    } else if magnitude < 100_000_000_000_000_000 {
        value / 1_000
    } else {
        value / 1_000_000
    }
}

/// Encode the extension's logs batch-v0 blob. Keeping this encoder outside
/// the daemon makes the efficient ingest path available to other Rust hosts.
pub fn encode_logs_batch(entries: &[LogRecord]) -> Vec<u8> {
    let estimated_strings: usize = entries
        .iter()
        .map(|entry| entry.message.len() + entry.metadata.len() * 24)
        .sum();
    let mut output = Vec::with_capacity(8 + entries.len() * 17 + estimated_strings);
    output.push(0x01);
    output.push(0x00);
    output.extend_from_slice(&0_u16.to_le_bytes());
    output.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        output.extend_from_slice(&entry.ts_ms.to_le_bytes());
    }
    for entry in entries {
        output.push(entry.level.as_byte());
    }
    for entry in entries {
        push_string(&mut output, &entry.message);
    }
    for entry in entries {
        let json = serde_json::to_string(&entry.metadata).expect("string metadata is JSON-safe");
        push_string(&mut output, &json);
    }
    output
}

fn push_string(output: &mut Vec<u8>, value: &str) {
    output.extend_from_slice(&(value.len() as u32).to_le_bytes());
    output.extend_from_slice(value.as_bytes());
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogQuery {
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    pub level: Option<Level>,
    pub message: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub limit: usize,
    pub offset: usize,
    pub order: SortOrder,
    pub count: bool,
}

impl Default for LogQuery {
    fn default() -> Self {
        Self {
            since_ms: None,
            until_ms: None,
            level: None,
            message: None,
            metadata: BTreeMap::new(),
            limit: 100,
            offset: 0,
            order: SortOrder::Desc,
            count: false,
        }
    }
}

/// Parse the LogsQL subset currently accepted by `timeless_logs`.
pub fn parse_logsql(input: &str, now_ms: i64) -> Result<LogQuery, String> {
    let input = input.trim();
    let mut sections = input.split(" | ");
    let filters = sections.next().unwrap_or_default().trim();
    let mut query = LogQuery::default();

    for pipe in sections {
        let pipe = pipe.trim();
        if pipe.starts_with("sort by") {
            query.order = if pipe.ends_with("asc") {
                SortOrder::Asc
            } else {
                SortOrder::Desc
            };
        } else if let Some(value) = pipe.strip_prefix("limit ") {
            if let Ok(value) = value.trim().parse::<usize>() {
                query.limit = value;
            }
        } else if let Some(value) = pipe.strip_prefix("offset ") {
            if let Ok(value) = value.trim().parse::<usize>() {
                query.offset = value;
            }
        } else if pipe.starts_with("stats count(") {
            query.count = true;
        }
    }

    if filters.is_empty() || filters == "*" {
        return Ok(query);
    }
    for token in tokenize(filters) {
        if let Some(value) = token.strip_prefix("_time:") {
            parse_time_filter(value, now_ms, &mut query);
        } else if let Some(value) = token.strip_prefix("level:") {
            query.level = Some(Level::parse_filter(unquote(value))?);
        } else if token.starts_with('"') {
            query.message = Some(unquote(&token).to_owned());
        } else if let Some((field, value)) = token.split_once(':') {
            query
                .metadata
                .insert(field.to_owned(), unquote(value).to_owned());
        }
    }
    Ok(query)
}

fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut bracketed = false;
    for character in input.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        match character {
            '\\' if quoted => {
                current.push(character);
                escaped = true;
            }
            '"' => {
                quoted = !quoted;
                current.push(character);
            }
            '[' if !quoted => {
                bracketed = true;
                current.push(character);
            }
            ')' | ']' if !quoted && bracketed => {
                bracketed = false;
                current.push(character);
            }
            character if character.is_whitespace() && !quoted && !bracketed => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(character),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
}

fn parse_time_filter(value: &str, now_ms: i64, query: &mut LogQuery) {
    if let Some(range) = value.strip_prefix('[') {
        let range = range.trim_end_matches([')', ']']);
        if let Some((start, end)) = range.split_once(',') {
            query.since_ms = parse_rfc3339_ms(start.trim());
            query.until_ms = parse_rfc3339_ms(end.trim());
        }
    } else if let Some(value) = value.strip_prefix(">=") {
        query.since_ms = parse_rfc3339_ms(value);
    } else if let Some(value) = value.strip_prefix('<') {
        query.until_ms = parse_rfc3339_ms(value);
    } else if let Some(duration_ms) = parse_duration_ms(value) {
        query.since_ms = Some(now_ms.saturating_sub(duration_ms));
    }
}

fn parse_rfc3339_ms(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|time| time.timestamp_millis())
}

fn parse_duration_ms(value: &str) -> Option<i64> {
    let split = value.find(|character: char| !character.is_ascii_digit())?;
    let amount = value[..split].parse::<i64>().ok()?;
    let multiplier = match &value[split..] {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    amount.checked_mul(multiplier)
}

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

pub fn format_timestamp(ts_ms: i64) -> Result<String, String> {
    DateTime::<Utc>::from_timestamp_millis(ts_ms)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Micros, true))
        .ok_or_else(|| format!("timestamp {ts_ms}ms is outside RFC3339 range"))
}

pub fn render_ndjson_row(
    ts_ms: i64,
    level: &str,
    message: &str,
    metadata_json: &str,
) -> Result<String, String> {
    let metadata: BTreeMap<String, String> =
        serde_json::from_str(metadata_json).map_err(|error| error.to_string())?;
    let mut object = Map::new();
    object.insert("_time".into(), Value::String(format_timestamp(ts_ms)?));
    object.insert("_msg".into(), Value::String(message.to_owned()));
    object.insert("level".into(), Value::String(level.to_owned()));
    for (key, value) in metadata {
        object.insert(key, Value::String(value));
    }
    serde_json::to_string(&Value::Object(object)).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndjson_isolates_errors_and_normalizes_timestamps() {
        let body = br#"{"_msg":"one","_time":1700000000,"level":"warn","service":"api"}
broken
{"body":"two","ts":"2024-06-15T12:00:00Z","level":"error"}"#;
        let parsed = parse_ndjson(body, "_msg", "_time", 99);
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.errors, 1);
        assert_eq!(parsed.entries[0].ts_ms, 1_700_000_000_000);
        assert_eq!(parsed.entries[0].level, Level::Warning);
        assert_eq!(parsed.entries[0].metadata["service"], "api");
        assert_eq!(parsed.entries[1].message, "");
        assert_eq!(parsed.entries[1].ts_ms, 99);
    }

    #[test]
    fn custom_fields_are_removed_from_metadata() {
        let parsed = parse_ndjson(
            br#"{"body":"two","ts":"2024-06-15T12:00:00Z","level":"error"}"#,
            "body",
            "ts",
            0,
        );
        let entry = &parsed.entries[0];
        assert_eq!(entry.message, "two");
        assert_eq!(entry.ts_ms, 1_718_452_800_000);
        assert!(entry.metadata.is_empty());
    }

    #[test]
    fn logs_batch_v0_has_expected_column_layout() {
        let entry = LogRecord {
            ts_ms: 42,
            level: Level::Error,
            message: "boom".into(),
            metadata: BTreeMap::from([("service".into(), "api".into())]),
        };
        let blob = encode_logs_batch(&[entry]);
        assert_eq!(&blob[..8], &[1, 0, 0, 0, 1, 0, 0, 0]);
        assert_eq!(i64::from_le_bytes(blob[8..16].try_into().unwrap()), 42);
        assert_eq!(blob[16], 3);
    }

    #[test]
    fn parses_ddnet_logsql_shape() {
        let query = parse_logsql(
            "_time:1h level:error service:api \"timeout\" | sort by (_time) asc | offset 2 | limit 50",
            7_200_000,
        )
        .unwrap();
        assert_eq!(query.since_ms, Some(3_600_000));
        assert_eq!(query.level, Some(Level::Error));
        assert_eq!(query.metadata["service"], "api");
        assert_eq!(query.message.as_deref(), Some("timeout"));
        assert_eq!(query.order, SortOrder::Asc);
        assert_eq!(query.offset, 2);
        assert_eq!(query.limit, 50);
    }

    #[test]
    fn renders_elixir_compatible_timestamp_precision() {
        let row =
            render_ndjson_row(1_700_000_000_000, "info", "hello", "{\"app\":\"test\"}").unwrap();
        let value: Value = serde_json::from_str(&row).unwrap();
        assert_eq!(value["_time"], "2023-11-14T22:13:20.000000Z");
        assert_eq!(value["app"], "test");
    }
}
