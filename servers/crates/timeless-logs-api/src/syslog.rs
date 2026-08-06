//! Bounded RFC3164/RFC5424 parsing for the API-owned LogsQL `unpack_syslog`
//! pipe. Syslog syntax remains outside the SQLite extension: direct database
//! users retain ordinary SQL over stored rows, while the Rust signal API owns
//! query-language parsing and response composition.

use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{
    DateTime, Datelike, Days, Local, LocalResult, NaiveDate, NaiveDateTime, SecondsFormat,
    TimeZone, Timelike, Utc,
};
use serde_json::Value;

pub(crate) struct ParseRequest<'a> {
    pub source: &'a str,
    pub offset_seconds: Option<i64>,
    pub current_year: i32,
    pub query_now_seconds: i64,
    pub state_bytes: &'a mut usize,
    pub work_items: &'a mut usize,
    pub max_state_bytes: usize,
    pub max_work_items: usize,
    pub cancelled: &'a AtomicBool,
}

pub(crate) fn parse(request: ParseRequest<'_>) -> Result<Vec<(String, String)>, String> {
    let mut parser = Parser {
        fields: Vec::new(),
        offset_seconds: request.offset_seconds,
        current_year: request.current_year,
        query_now_seconds: request.query_now_seconds,
        state_bytes: request.state_bytes,
        work_items: request.work_items,
        max_state_bytes: request.max_state_bytes,
        max_work_items: request.max_work_items,
        cancelled: request.cancelled,
    };
    parser.parse(request.source)?;
    Ok(parser.fields)
}

struct Parser<'a> {
    fields: Vec<(String, String)>,
    offset_seconds: Option<i64>,
    current_year: i32,
    query_now_seconds: i64,
    state_bytes: &'a mut usize,
    work_items: &'a mut usize,
    max_state_bytes: usize,
    max_work_items: usize,
    cancelled: &'a AtomicBool,
}

impl Parser<'_> {
    fn parse(&mut self, source: &str) -> Result<(), String> {
        self.ensure_active()?;
        if source.is_empty() {
            return Ok(());
        }
        if !source.starts_with('<') {
            return self.parse_no_header(source);
        }

        let Some(close) = source[1..].find('>').map(|index| index + 1) else {
            return Ok(());
        };
        let priority = &source[1..close];
        self.add_field("priority", priority)?;
        let Ok(priority) = priority.parse::<u64>() else {
            return Ok(());
        };
        let facility = priority / 8;
        let severity = priority % 8;
        self.add_field("facility_keyword", facility_keyword(facility))?;
        self.add_field("level", severity_level(severity))?;
        self.add_field("facility", &facility.to_string())?;
        self.add_field("severity", &severity.to_string())?;
        self.parse_no_header(&source[close + 1..])
    }

    fn parse_no_header(&mut self, source: &str) -> Result<(), String> {
        if source.is_empty() {
            return Ok(());
        }
        if let Some(rest) = source.strip_prefix("1 ") {
            self.parse_rfc5424(rest)
        } else {
            self.parse_rfc3164(source)
        }
    }

    fn parse_rfc5424(&mut self, mut source: &str) -> Result<(), String> {
        self.add_field("format", "rfc5424")?;
        if source.is_empty() {
            return Ok(());
        }
        for field in ["timestamp", "hostname", "app_name", "proc_id", "msg_id"] {
            let Some(space) = source.find(' ') else {
                self.add_field(field, source)?;
                return Ok(());
            };
            self.add_field(field, &source[..space])?;
            source = &source[space + 1..];
        }

        let Some(message) = self.parse_rfc5424_structured_data(source)? else {
            return Ok(());
        };
        self.add_message(message)
    }

    fn parse_rfc5424_structured_data<'a>(
        &mut self,
        mut source: &'a str,
    ) -> Result<Option<&'a str>, String> {
        if let Some(message) = source.strip_prefix("- ") {
            return Ok(Some(message));
        }
        if source.starts_with("@cee:") {
            return Ok(Some(source));
        }
        loop {
            let Some(tail) = self.parse_rfc5424_structured_line(source)? else {
                return Ok(None);
            };
            source = tail;
            if let Some(message) = source.strip_prefix(' ') {
                return Ok(Some(message));
            }
        }
    }

    fn parse_rfc5424_structured_line<'a>(
        &mut self,
        source: &'a str,
    ) -> Result<Option<&'a str>, String> {
        let Some(mut rest) = source.strip_prefix('[') else {
            return Ok(None);
        };
        let Some(id_end) = rest.find([' ', ']']) else {
            return Ok(None);
        };
        let mut structured_id = &rest[..id_end];
        rest = &rest[id_end..];
        if let Some(equals) = structured_id.find('=') {
            self.add_field(&structured_id[..equals], &structured_id[equals + 1..])?;
            structured_id = "";
        }

        let bytes = rest.as_bytes();
        let mut cursor = 0usize;
        while cursor < bytes.len() && (bytes[cursor] != b']' || is_backslash_escaped(bytes, cursor))
        {
            self.check_scan(cursor)?;
            if bytes[cursor] == b' ' {
                cursor += 1;
                continue;
            }
            let Some(relative_equals) = rest[cursor..].find('=') else {
                return Ok(None);
            };
            cursor += relative_equals + 1;
            if cursor >= bytes.len() {
                return Ok(None);
            }
            if bytes[cursor] == b'"' {
                cursor += 1;
                let mut valid = false;
                while cursor < bytes.len() {
                    self.check_scan(cursor)?;
                    if bytes[cursor] == b'"' && !is_backslash_escaped(bytes, cursor) {
                        valid = true;
                        cursor += 1;
                        break;
                    }
                    cursor += 1;
                }
                if !valid {
                    return Ok(None);
                }
            } else {
                let Some(relative_end) = rest[cursor..].find([' ', ']']) else {
                    return Ok(None);
                };
                cursor += relative_end;
            }
        }
        if cursor == bytes.len() {
            return Ok(None);
        }

        let parameters = rest[..cursor].trim().replace(r"\]", "]");
        self.parse_structured_parameters(structured_id, &parameters)?;
        Ok(Some(&rest[cursor + 1..]))
    }

    fn parse_structured_parameters(
        &mut self,
        structured_id: &str,
        mut source: &str,
    ) -> Result<(), String> {
        let mut parsed = 0usize;
        loop {
            self.check_scan(parsed)?;
            let Some(separator) = source.find(['=', ' ']) else {
                return self.add_structured_field(structured_id, source.trim(), "");
            };
            parsed = parsed.saturating_add(separator).saturating_add(1);
            let name = source[..separator].trim();
            let delimiter = source.as_bytes()[separator];
            source = &source[separator + 1..];
            if delimiter == b' ' {
                self.add_structured_field(structured_id, name, "")?;
                continue;
            }
            if source.is_empty() {
                return self.add_structured_field(structured_id, name, "");
            }
            if let Some((value, consumed)) = decode_quoted_prefix(source, self.cancelled)? {
                self.add_structured_field(structured_id, name, &value)?;
                source = &source[consumed..];
                if source.is_empty() {
                    return Ok(());
                }
                if !source.starts_with(' ') {
                    return Ok(());
                }
                source = &source[1..];
                parsed = parsed.saturating_add(consumed).saturating_add(1);
                continue;
            }
            let Some(space) = source.find(' ') else {
                return self.add_structured_field(structured_id, name, source);
            };
            self.add_structured_field(structured_id, name, &source[..space])?;
            source = &source[space + 1..];
            parsed = parsed.saturating_add(space).saturating_add(1);
        }
    }

    fn add_structured_field(
        &mut self,
        structured_id: &str,
        name: &str,
        value: &str,
    ) -> Result<(), String> {
        if name.is_empty() && value.is_empty() {
            if !structured_id.is_empty() {
                return self.add_field(structured_id, "");
            }
            return Ok(());
        }
        if structured_id.is_empty() {
            return self.add_field(name, value);
        }
        self.add_field(&format!("{structured_id}.{name}"), value)
    }

    fn parse_rfc3164(&mut self, mut source: &str) -> Result<(), String> {
        self.add_field("format", "rfc3164")?;
        const CLASSIC_LENGTH: usize = 15;
        if source.len() < CLASSIC_LENGTH {
            return self.add_message(source);
        }

        let consumed = if source.as_bytes().get(10) != Some(&b'T') {
            let Some(stamp) = source.get(..CLASSIC_LENGTH) else {
                return self.add_message(source);
            };
            let Some(timestamp) = self.parse_classic_timestamp(stamp, self.current_year) else {
                return self.add_message(source);
            };
            self.add_field("timestamp", &format_utc(timestamp))?;
            CLASSIC_LENGTH
        } else {
            let Some(space) = source.find(' ') else {
                return self.add_message(source);
            };
            let Some(timestamp) = parse_rfc3339_utc(&source[..space]) else {
                return self.add_message(source);
            };
            self.add_field("timestamp", &format_utc(timestamp))?;
            space
        };
        source = &source[consumed..];
        if !source.starts_with(' ') {
            if !source.is_empty() {
                self.add_message(source)?;
            }
            return Ok(());
        }
        source = &source[1..];

        if let Some(space) = source.find(' ') {
            let candidate = &source[..space];
            if !candidate.contains([':', '[']) {
                self.add_field("hostname", candidate)?;
                source = &source[space + 1..];
            }
        } else if !source.contains([':', '[']) {
            self.add_field("hostname", source)?;
            return Ok(());
        }

        let Some(tag_end) = source.find(['[', ':', ' ']) else {
            self.add_field("app_name", source)?;
            return Ok(());
        };
        let app_name = &source[..tag_end];
        self.add_field("app_name", app_name)?;
        source = &source[tag_end..];
        if source.is_empty() {
            return Ok(());
        }
        if let Some(after_open) = source.strip_prefix('[') {
            let Some(close) = after_open.find(']') else {
                return Ok(());
            };
            self.add_field("proc_id", &after_open[..close])?;
            source = &after_open[close + 1..];
        }
        source = source.strip_prefix(':').unwrap_or(source);
        source = source.strip_prefix(' ').unwrap_or(source);
        if source.is_empty() {
            return Ok(());
        }
        if app_name == "CEF" {
            let checkpoint = self.checkpoint();
            if self.parse_cef(source)? {
                return Ok(());
            }
            self.restore(checkpoint);
        }
        self.add_message(source)
    }

    fn parse_classic_timestamp(&self, stamp: &str, year: i32) -> Option<DateTime<Utc>> {
        // Go's `time.Parse(time.Stamp, ...)` uses year zero, which is a leap
        // year, and `time.Date` then normalizes an impossible Feb 29 into
        // March 1 when assigning a non-leap current or previous year. Parse
        // against a known leap year and reproduce that normalization instead
        // of rejecting a valid RFC3164 lexical timestamp.
        let parsed =
            NaiveDateTime::parse_from_str(&format!("2000 {stamp}"), "%Y %b %e %H:%M:%S").ok()?;
        let naive = with_normalized_year(parsed, year)?;
        let timestamp = self.resolve_classic_timezone(naive)?;
        if timestamp.timestamp().saturating_sub(86_400) <= self.query_now_seconds {
            return Some(timestamp);
        }
        let previous_year = year.checked_sub(1)?;
        let adjusted = with_normalized_year(parsed, previous_year)?;
        self.resolve_classic_timezone(adjusted)
    }

    fn resolve_classic_timezone(&self, naive: NaiveDateTime) -> Option<DateTime<Utc>> {
        if let Some(offset_seconds) = self.offset_seconds {
            let utc_seconds = naive.and_utc().timestamp().checked_sub(offset_seconds)?;
            return DateTime::<Utc>::from_timestamp(utc_seconds, naive.nanosecond());
        }
        match Local.from_local_datetime(&naive) {
            LocalResult::Single(timestamp) => Some(timestamp.with_timezone(&Utc)),
            LocalResult::Ambiguous(earlier, _) => Some(earlier.with_timezone(&Utc)),
            LocalResult::None => None,
        }
    }

    fn add_message(&mut self, source: &str) -> Result<(), String> {
        if let Some(cef) = source.strip_prefix("CEF:") {
            let checkpoint = self.checkpoint();
            if self.parse_cef(cef)? {
                return Ok(());
            }
            self.restore(checkpoint);
        } else if let Some(cee) = source.strip_prefix("@cee:") {
            let checkpoint = self.checkpoint();
            if self.parse_cee(cee)? {
                return Ok(());
            }
            self.restore(checkpoint);
        }
        self.add_field("message", source)
    }

    fn parse_cee(&mut self, source: &str) -> Result<bool, String> {
        let Ok(value) = serde_json::from_str::<Value>(source) else {
            return Ok(false);
        };
        let Value::Object(object) = value else {
            return Ok(false);
        };
        self.flatten_cee_object("", &object, 0)?;
        Ok(true)
    }

    fn flatten_cee_object(
        &mut self,
        prefix: &str,
        object: &serde_json::Map<String, Value>,
        depth: usize,
    ) -> Result<(), String> {
        if depth > 128 {
            return Err("LogsQL unpack_syslog CEE nesting exceeds 128 levels".into());
        }
        for (position, (name, value)) in object.iter().enumerate() {
            self.check_scan(position)?;
            let field_name = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}.{name}")
            };
            match value {
                Value::Null => {}
                Value::Object(nested) => {
                    self.flatten_cee_object(&field_name, nested, depth + 1)?;
                }
                Value::String(value) => {
                    self.add_field(
                        if field_name.is_empty() {
                            "_msg"
                        } else {
                            &field_name
                        },
                        value,
                    )?;
                }
                Value::Bool(value) => {
                    self.add_field(
                        if field_name.is_empty() {
                            "_msg"
                        } else {
                            &field_name
                        },
                        if *value { "true" } else { "false" },
                    )?;
                }
                Value::Number(value) => {
                    self.add_field(
                        if field_name.is_empty() {
                            "_msg"
                        } else {
                            &field_name
                        },
                        &value.to_string(),
                    )?;
                }
                Value::Array(value) => {
                    let encoded = serde_json::to_string(value).map_err(|error| {
                        format!("encode LogsQL unpack_syslog CEE array: {error}")
                    })?;
                    self.add_field(
                        if field_name.is_empty() {
                            "_msg"
                        } else {
                            &field_name
                        },
                        &encoded,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn parse_cef(&mut self, mut source: &str) -> Result<bool, String> {
        for name in [
            "cef.version",
            "cef.device_vendor",
            "cef.device_product",
            "cef.device_version",
            "cef.device_event_class_id",
            "cef.name",
            "cef.severity",
        ] {
            let Some(separator) = next_unescaped(source, b'|') else {
                return Ok(false);
            };
            self.add_field(name, &unescape_cef(&source[..separator]))?;
            source = &source[separator + 1..];
        }
        self.parse_cef_extension(source)
    }

    fn parse_cef_extension(&mut self, mut source: &str) -> Result<bool, String> {
        if source.is_empty() {
            return Ok(true);
        }
        loop {
            let Some(equals) = next_unescaped(source, b'=') else {
                return Ok(false);
            };
            let name = format!("cef.extension.{}", unescape_cef(&source[..equals]));
            source = &source[equals + 1..];
            let Some(next_equals) = next_unescaped(source, b'=') else {
                self.add_field(&name, source)?;
                return Ok(true);
            };
            let Some(space) = source[..next_equals].rfind(' ') else {
                return Ok(false);
            };
            self.add_field(&name, &unescape_cef(&source[..space]))?;
            source = &source[space + 1..];
        }
    }

    fn add_field(&mut self, name: &str, value: &str) -> Result<(), String> {
        self.ensure_active()?;
        *self.work_items = self
            .work_items
            .checked_add(1)
            .ok_or_else(|| "LogsQL unpack_syslog work item count overflow".to_string())?;
        if *self.work_items > self.max_work_items {
            return Err(format!(
                "LogsQL unpack_syslog exceeded max_work_rows={}",
                self.max_work_items
            ));
        }
        *self.state_bytes = self
            .state_bytes
            .checked_add(size_of::<(String, String)>())
            .and_then(|bytes| bytes.checked_add(name.len()))
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or_else(|| "LogsQL unpack_syslog state size overflow".to_string())?;
        if *self.state_bytes > self.max_state_bytes {
            return Err(format!(
                "LogsQL unpack_syslog exceeded max_response_bytes={}",
                self.max_state_bytes
            ));
        }
        self.fields.push((name.to_owned(), value.to_owned()));
        Ok(())
    }

    fn checkpoint(&self) -> (usize, usize, usize) {
        (self.fields.len(), *self.state_bytes, *self.work_items)
    }

    fn restore(&mut self, checkpoint: (usize, usize, usize)) {
        self.fields.truncate(checkpoint.0);
        *self.state_bytes = checkpoint.1;
        *self.work_items = checkpoint.2;
    }

    fn check_scan(&self, position: usize) -> Result<(), String> {
        if position & 0xff == 0 {
            self.ensure_active()?;
        }
        Ok(())
    }

    fn ensure_active(&self) -> Result<(), String> {
        if self.cancelled.load(Ordering::Relaxed) {
            Err("LogsQL pipeline cancelled".into())
        } else {
            Ok(())
        }
    }
}

fn severity_level(severity: u64) -> &'static str {
    match severity {
        0 => "emerg",
        1 => "alert",
        2 => "critical",
        3 => "error",
        4 => "warning",
        5 => "notice",
        6 => "info",
        7 => "debug",
        _ => "unknown",
    }
}

fn facility_keyword(facility: u64) -> &'static str {
    match facility {
        0 => "kern",
        1 => "user",
        2 => "mail",
        3 => "daemon",
        4 => "auth",
        5 => "syslog",
        6 => "lpr",
        7 => "news",
        8 => "uucp",
        9 => "cron",
        10 => "authpriv",
        11 => "ftp",
        12 => "ntp",
        13 => "security",
        14 => "console",
        15 => "solaris-cron",
        16 => "local0",
        17 => "local1",
        18 => "local2",
        19 => "local3",
        20 => "local4",
        21 => "local5",
        22 => "local6",
        23 => "local7",
        _ => "unknown",
    }
}

fn is_backslash_escaped(source: &[u8], position: usize) -> bool {
    position > 0 && source[position - 1] == b'\\'
}

fn decode_quoted_prefix(
    source: &str,
    cancelled: &AtomicBool,
) -> Result<Option<(String, usize)>, String> {
    if !source.starts_with('"') {
        return Ok(None);
    }
    let bytes = source.as_bytes();
    let mut cursor = 1usize;
    while cursor < bytes.len() {
        if cursor & 0xff == 0 && cancelled.load(Ordering::Relaxed) {
            return Err("LogsQL pipeline cancelled".into());
        }
        if bytes[cursor] == b'"' && !is_backslash_escaped(bytes, cursor) {
            let consumed = cursor + 1;
            let Ok(value) = serde_json::from_str::<String>(&source[..consumed]) else {
                return Ok(None);
            };
            return Ok(Some((value, consumed)));
        }
        cursor += 1;
    }
    Ok(None)
}

fn parse_rfc3339_utc(source: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(source)
        .or_else(|_| DateTime::parse_from_str(source, "%Y-%m-%dT%H:%M:%S%.f%z"))
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn with_normalized_year(timestamp: NaiveDateTime, year: i32) -> Option<NaiveDateTime> {
    NaiveDate::from_ymd_opt(year, timestamp.month(), 1)?
        .checked_add_days(Days::new(u64::from(timestamp.day() - 1)))?
        .and_hms_nano_opt(
            timestamp.hour(),
            timestamp.minute(),
            timestamp.second(),
            timestamp.nanosecond(),
        )
}

fn format_utc(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn next_unescaped(source: &str, needle: u8) -> Option<usize> {
    let bytes = source.as_bytes();
    for (position, byte) in bytes.iter().copied().enumerate() {
        if byte != needle {
            continue;
        }
        let mut backslashes = 0usize;
        let mut previous = position;
        while previous > 0 && bytes[previous - 1] == b'\\' {
            backslashes += 1;
            previous -= 1;
        }
        if backslashes.is_multiple_of(2) {
            return Some(position);
        }
    }
    None
}

fn unescape_cef(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if bytes[cursor] != b'\\' {
            output.push(bytes[cursor]);
            cursor += 1;
            continue;
        }
        cursor += 1;
        let Some(escaped) = bytes.get(cursor).copied() else {
            output.push(b'\\');
            break;
        };
        match escaped {
            b'n' => output.push(b'\n'),
            b'r' => output.push(b'\r'),
            value => output.push(value),
        }
        cursor += 1;
    }
    String::from_utf8(output).expect("CEF unescaping preserves UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_at(source: &str, offset_seconds: Option<i64>) -> Vec<(String, String)> {
        let cancelled = AtomicBool::new(false);
        let mut state_bytes = 0;
        let mut work_items = 0;
        parse(ParseRequest {
            source,
            offset_seconds,
            current_year: 2024,
            query_now_seconds: 1_719_836_800,
            state_bytes: &mut state_bytes,
            work_items: &mut work_items,
            max_state_bytes: 1_000_000,
            max_work_items: 10_000,
            cancelled: &cancelled,
        })
        .unwrap()
    }

    fn value<'a>(fields: &'a [(String, String)], name: &str) -> Option<&'a str> {
        fields
            .iter()
            .rev()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn parses_rfc3164_rfc5424_structured_data_cef_and_cee() {
        let classic = parse_at("Jun  3 12:08:33 host app[1]: Starting caches", Some(0));
        assert_eq!(value(&classic, "format"), Some("rfc3164"));
        assert_eq!(value(&classic, "timestamp"), Some("2024-06-03T12:08:33Z"));
        assert_eq!(value(&classic, "hostname"), Some("host"));
        assert_eq!(value(&classic, "app_name"), Some("app"));
        assert_eq!(value(&classic, "proc_id"), Some("1"));
        assert_eq!(value(&classic, "message"), Some("Starting caches"));

        let modern = parse_at(
            r#"<165>1 2023-06-03T17:42:00Z host app 123 ID47 [foo@123 iut="3" event="Application = \] ok"][abc=def][bare@9] tail  "#,
            Some(0),
        );
        assert_eq!(value(&modern, "priority"), Some("165"));
        assert_eq!(value(&modern, "facility_keyword"), Some("local4"));
        assert_eq!(value(&modern, "level"), Some("notice"));
        assert_eq!(value(&modern, "foo@123.iut"), Some("3"));
        assert_eq!(value(&modern, "foo@123.event"), Some("Application = ] ok"));
        assert_eq!(value(&modern, "abc"), Some("def"));
        assert_eq!(value(&modern, "bare@9"), Some(""));
        assert_eq!(value(&modern, "message"), Some("tail  "));

        let cef = parse_at(
            r#"Sep 29 08:26:10 host CEF:1|Security|product|1.0|100|name\|value|10|src=10.0.0.1 msg=two words"#,
            Some(0),
        );
        assert_eq!(value(&cef, "cef.name"), Some("name|value"));
        assert_eq!(value(&cef, "cef.extension.src"), Some("10.0.0.1"));
        assert_eq!(value(&cef, "cef.extension.msg"), Some("two words"));

        let cee = parse_at(
            r#"Jun  3 12:08:33 host app[1]: @cee: {"number":123,"flag":false,"null_value":null,"nested":{"leaf":7},"array":[1,"x"]}"#,
            Some(0),
        );
        assert_eq!(value(&cee, "number"), Some("123"));
        assert_eq!(value(&cee, "flag"), Some("false"));
        assert_eq!(value(&cee, "null_value"), None);
        assert_eq!(value(&cee, "nested.leaf"), Some("7"));
        assert_eq!(value(&cee, "array"), Some(r#"[1,"x"]"#));
    }

    #[test]
    fn preserves_partial_and_invalid_upstream_behavior() {
        let invalid_priority = parse_at("<abc>1 now host app 1 ID - tail", Some(0));
        assert_eq!(invalid_priority, [("priority".into(), "abc".into())]);

        let arbitrary = parse_at("plain text  ", Some(0));
        assert_eq!(value(&arbitrary, "format"), Some("rfc3164"));
        assert_eq!(value(&arbitrary, "message"), Some("plain text  "));

        let invalid_cef = parse_at("Sep 29 08:26:10 host CEF:1|Security|too-short", Some(0));
        assert_eq!(value(&invalid_cef, "message"), Some("1|Security|too-short"));
        assert_eq!(value(&invalid_cef, "cef.version"), None);
    }

    #[test]
    fn matches_pinned_timestamp_header_and_special_message_edges() {
        let offset = parse_at("Jun  3 12:08:33 host app: value", Some(19_800));
        assert_eq!(value(&offset, "timestamp"), Some("2024-06-03T06:38:33Z"));

        let previous_year = parse_at("Dec 20 12:42:20 host app: value", Some(0));
        assert_eq!(
            value(&previous_year, "timestamp"),
            Some("2023-12-20T12:42:20Z")
        );

        let iso = parse_at(
            "2025-01-23T12:15:23.965512-0500 host app: value",
            Some(19_800),
        );
        assert_eq!(
            value(&iso, "timestamp"),
            Some("2025-01-23T17:15:23.965512Z"),
            "RFC3339 timestamps ignore the RFC3164 offset option"
        );

        let missing_hostname =
            parse_at("Jun  3 12:08:33 sshd-session[14308]: disconnected", Some(0));
        assert_eq!(value(&missing_hostname, "hostname"), None);
        assert_eq!(value(&missing_hostname, "app_name"), Some("sshd-session"));
        assert_eq!(value(&missing_hostname, "proc_id"), Some("14308"));

        let lexical_timestamp =
            parse_at("<6>1 2021-09-14T14:06:26-0500 host - - - - tail", Some(0));
        assert_eq!(
            value(&lexical_timestamp, "timestamp"),
            Some("2021-09-14T14:06:26-0500")
        );
        assert_eq!(value(&lexical_timestamp, "app_name"), Some("-"));
        assert_eq!(value(&lexical_timestamp, "message"), Some("tail"));

        let unknown_facility = parse_at("<999>plain", Some(0));
        assert_eq!(
            value(&unknown_facility, "facility_keyword"),
            Some("unknown")
        );
        assert_eq!(value(&unknown_facility, "level"), Some("debug"));
        assert_eq!(value(&unknown_facility, "facility"), Some("124"));
        assert_eq!(value(&unknown_facility, "severity"), Some("7"));

        let direct_cef = parse_at(
            "<6>CEF:0|Vendor|Product|1|event|name|3|src=10.0.0.1",
            Some(0),
        );
        assert_eq!(value(&direct_cef, "format"), Some("rfc3164"));
        assert_eq!(value(&direct_cef, "cef.version"), Some("0"));
        assert_eq!(value(&direct_cef, "cef.extension.src"), Some("10.0.0.1"));
        assert_eq!(value(&direct_cef, "message"), None);

        let invalid_cee = parse_at("Jun  3 12:08:33 host app: @cee: not-json", Some(0));
        assert_eq!(value(&invalid_cee, "message"), Some("@cee: not-json"));

        assert!(parse_at("", Some(0)).is_empty());
        assert!(parse_at("<missing-close", Some(0)).is_empty());
    }

    #[test]
    fn normalizes_classic_leap_day_like_go_time_date() {
        let cancelled = AtomicBool::new(false);
        let mut state_bytes = 0;
        let mut work_items = 0;
        let fields = parse(ParseRequest {
            source: "Feb 29 12:34:56 host app: leap",
            offset_seconds: Some(0),
            current_year: 2023,
            query_now_seconds: 1_677_628_800,
            state_bytes: &mut state_bytes,
            work_items: &mut work_items,
            max_state_bytes: 1_000_000,
            max_work_items: 10_000,
            cancelled: &cancelled,
        })
        .unwrap();
        assert_eq!(value(&fields, "timestamp"), Some("2023-03-01T12:34:56Z"));

        let mut state_bytes = 0;
        let mut work_items = 0;
        let fields = parse(ParseRequest {
            source: "Feb 29 12:34:56 host app: leap",
            offset_seconds: Some(0),
            current_year: 2024,
            query_now_seconds: 1_704_067_200,
            state_bytes: &mut state_bytes,
            work_items: &mut work_items,
            max_state_bytes: 1_000_000,
            max_work_items: 10_000,
            cancelled: &cancelled,
        })
        .unwrap();
        assert_eq!(value(&fields, "timestamp"), Some("2023-03-01T12:34:56Z"));
    }

    #[test]
    fn parsing_observes_state_work_and_cancellation_limits() {
        let source = "<165>1 2023-06-03T17:42:00Z host app 123 ID - tail";
        let cancelled = AtomicBool::new(false);

        let mut state_bytes = 0;
        let mut work_items = 0;
        let state_error = parse(ParseRequest {
            source,
            offset_seconds: Some(0),
            current_year: 2024,
            query_now_seconds: 1_719_836_800,
            state_bytes: &mut state_bytes,
            work_items: &mut work_items,
            max_state_bytes: 1,
            max_work_items: 10_000,
            cancelled: &cancelled,
        })
        .unwrap_err();
        assert!(
            state_error.contains("max_response_bytes=1"),
            "{state_error}"
        );

        let mut state_bytes = 0;
        let mut work_items = 0;
        let work_error = parse(ParseRequest {
            source,
            offset_seconds: Some(0),
            current_year: 2024,
            query_now_seconds: 1_719_836_800,
            state_bytes: &mut state_bytes,
            work_items: &mut work_items,
            max_state_bytes: 1_000_000,
            max_work_items: 1,
            cancelled: &cancelled,
        })
        .unwrap_err();
        assert!(work_error.contains("max_work_rows=1"), "{work_error}");

        cancelled.store(true, Ordering::Release);
        let mut state_bytes = 0;
        let mut work_items = 0;
        assert_eq!(
            parse(ParseRequest {
                source,
                offset_seconds: Some(0),
                current_year: 2024,
                query_now_seconds: 1_719_836_800,
                state_bytes: &mut state_bytes,
                work_items: &mut work_items,
                max_state_bytes: 1_000_000,
                max_work_items: 10_000,
                cancelled: &cancelled,
            })
            .unwrap_err(),
            "LogsQL pipeline cancelled"
        );
    }
}
