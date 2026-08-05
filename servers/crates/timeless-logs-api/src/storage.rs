use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use fs2::FileExt;
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value as JsonValue;
use timeless_api_common::{
    acquire_database_lease, apply_schema_ledger, checkpoint_wal, create_verified_backup,
    preflight_database, preflight_extension, require_current_schema, require_query_surface,
    BackupReport, DataPlaneSpec,
};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::logsql::PipelineOp;
use crate::pipeline::{self, PipelineLimits};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampUnit {
    Milliseconds,
    Microseconds,
}

impl TimestampUnit {
    fn sql_name(self) -> &'static str {
        match self {
            Self::Milliseconds => "ms",
            Self::Microseconds => "us",
        }
    }
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub ts: i64,
    pub level: u8,
    pub severity: String,
    pub message: String,
    pub metadata_json: String,
}

#[derive(Clone, Debug)]
pub struct QuerySpec {
    pub level: Option<String>,
    pub service: Option<String>,
    pub metadata_eq: BTreeMap<String, String>,
    pub metadata_exact: Vec<MetadataExact>,
    pub message: Option<String>,
    pub message_phrase: Option<String>,
    /// Bounded API-owned row predicate for LogsQL behavior that has no honest
    /// storage pushdown. Language syntax never enters the extension.
    pub predicate: Option<LogPredicate>,
    pub ts_min: Option<i64>,
    pub ts_max: Option<i64>,
    pub limit: usize,
    pub offset: usize,
    pub descending: bool,
    /// Hard upper bound on entries examined by the public extension query.
    /// This is work, not output cardinality; `LIMIT 1` cannot conceal an
    /// unbounded decode.
    pub max_work_rows: usize,
}

impl Default for QuerySpec {
    fn default() -> Self {
        Self {
            level: None,
            service: None,
            metadata_eq: BTreeMap::new(),
            metadata_exact: Vec::new(),
            message: None,
            message_phrase: None,
            predicate: None,
            ts_min: None,
            ts_max: None,
            limit: 0,
            offset: 0,
            descending: false,
            max_work_rows: 100_000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogField {
    Message,
    Level,
    Metadata(Vec<String>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NumericOp {
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

impl NumericOp {
    fn matches(self, ordering: CmpOrdering) -> bool {
        match self {
            Self::Greater => ordering == CmpOrdering::Greater,
            Self::GreaterOrEqual => ordering != CmpOrdering::Less,
            Self::Less => ordering == CmpOrdering::Less,
            Self::LessOrEqual => ordering != CmpOrdering::Greater,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueTypeKind {
    String,
    Uint64,
    Int64,
    Float64,
    Bool,
    Null,
    Array,
    Object,
    Number,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PatternMatchMode {
    Any,
    Full,
    Prefix,
    Suffix,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PatternPlaceholder {
    Number,
    Uuid,
    Ipv4,
    Time,
    Date,
    DateTime,
    Word,
}

/// Compiled VictoriaLogs-compatible pattern used by API-owned row predicates.
///
/// This matcher operates only after a bounded public-table read. It is not an
/// extension contract and does not expose LogsQL syntax to SQLite.
#[derive(Clone, Debug)]
pub struct PatternMatcher {
    mode: PatternMatchMode,
    separators: Vec<Vec<u8>>,
    placeholders: Vec<PatternPlaceholder>,
}

impl PatternMatcher {
    pub fn new(pattern: &str, mode: PatternMatchMode) -> Self {
        let mut separators = Vec::new();
        let mut placeholders = Vec::new();
        let mut separator = Vec::new();
        let bytes = pattern.as_bytes();
        let mut offset = 0usize;
        while offset < bytes.len() {
            let Some(relative_start) = bytes[offset..].iter().position(|byte| *byte == b'<') else {
                separator.extend_from_slice(&bytes[offset..]);
                break;
            };
            let start = offset + relative_start;
            separator.extend_from_slice(&bytes[offset..start]);
            let Some(relative_end) = bytes[start..].iter().position(|byte| *byte == b'>') else {
                separator.extend_from_slice(&bytes[start..]);
                break;
            };
            let end = start + relative_end + 1;
            let placeholder = match &bytes[start..end] {
                b"<N>" => Some(PatternPlaceholder::Number),
                b"<UUID>" => Some(PatternPlaceholder::Uuid),
                b"<IP4>" => Some(PatternPlaceholder::Ipv4),
                b"<TIME>" => Some(PatternPlaceholder::Time),
                b"<DATE>" => Some(PatternPlaceholder::Date),
                b"<DATETIME>" => Some(PatternPlaceholder::DateTime),
                b"<W>" => Some(PatternPlaceholder::Word),
                _ => None,
            };
            if let Some(placeholder) = placeholder {
                separators.push(std::mem::take(&mut separator));
                placeholders.push(placeholder);
            } else {
                separator.extend_from_slice(&bytes[start..end]);
            }
            offset = end;
        }
        separators.push(separator);
        Self {
            mode,
            separators,
            placeholders,
        }
    }

    pub fn matches(&self, value: &str) -> bool {
        let bytes = value.as_bytes();
        match self.mode {
            PatternMatchMode::Any => self.index_start_end(bytes, 0).is_some(),
            PatternMatchMode::Full => self.index_end(bytes, 0) == Some(bytes.len()),
            PatternMatchMode::Prefix => self.index_end(bytes, 0).is_some(),
            PatternMatchMode::Suffix => {
                if self.is_empty() {
                    return true;
                }
                if !bytes.ends_with(self.separators.last().expect("one separator")) {
                    return false;
                }
                let mut offset = 0usize;
                while offset <= bytes.len() {
                    let Some((start, end)) = self.index_start_end(bytes, offset) else {
                        return false;
                    };
                    if end == bytes.len() {
                        return true;
                    }
                    offset = start.saturating_add(1);
                }
                false
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.separators.len() == 1 && self.separators[0].is_empty()
    }

    fn index_start_end(&self, value: &[u8], mut offset: usize) -> Option<(usize, usize)> {
        while offset <= value.len() {
            let start = self.index_start(value, offset)?;
            if let Some(end) = self.index_end(value, start) {
                return Some((start, end));
            }
            offset = start.saturating_add(1);
        }
        None
    }

    fn index_start(&self, value: &[u8], offset: usize) -> Option<usize> {
        let first = &self.separators[0];
        if !first.is_empty() {
            return find_bytes(value, first, offset);
        }
        let Some(first_placeholder) = self.placeholders.first() else {
            return Some(0);
        };
        match first_placeholder {
            PatternPlaceholder::Word => index_word_start(value, offset),
            _ => index_number_start(value, offset),
        }
    }

    fn index_end(&self, value: &[u8], mut offset: usize) -> Option<usize> {
        for (index, separator) in self.separators.iter().enumerate() {
            if !value.get(offset..)?.starts_with(separator) {
                return None;
            }
            offset = offset.checked_add(separator.len())?;
            let Some(placeholder) = self.placeholders.get(index) else {
                return Some(offset);
            };
            offset = placeholder.index_end(value, offset)?;
        }
        Some(offset)
    }
}

impl PatternPlaceholder {
    fn index_end(self, value: &[u8], offset: usize) -> Option<usize> {
        match self {
            Self::Number => index_placeholder_number_end(value, offset),
            Self::Uuid => index_generic_placeholder_end(value, offset, 5, b'-'),
            Self::Ipv4 => index_generic_placeholder_end(value, offset, 4, b'.'),
            Self::Time => index_time_end(value, offset),
            Self::Date => index_date_end(value, offset),
            Self::DateTime => index_datetime_end(value, offset),
            Self::Word => index_word_end(value, offset),
        }
    }
}

fn find_bytes(value: &[u8], needle: &[u8], offset: usize) -> Option<usize> {
    if offset > value.len() {
        return None;
    }
    if needle.is_empty() {
        return Some(offset);
    }
    value[offset..]
        .windows(needle.len())
        .position(|candidate| candidate == needle)
        .map(|relative| offset + relative)
}

fn is_ascii_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn is_hex(byte: u8) -> bool {
    byte.is_ascii_hexdigit()
}

fn index_number_start(value: &[u8], mut offset: usize) -> Option<usize> {
    while offset < value.len() {
        if !is_hex(value[offset]) {
            offset += 1;
            continue;
        }
        if offset == 0
            || !is_ascii_token(value[offset - 1])
            || is_special_number_start(value[offset - 1])
        {
            return Some(offset);
        }
        offset += 1;
    }
    None
}

fn index_number_end(value: &[u8], mut offset: usize) -> usize {
    while offset < value.len() && is_hex(value[offset]) {
        offset += 1;
    }
    offset
}

fn index_placeholder_number_end(value: &[u8], start: usize) -> Option<usize> {
    let end = index_number_end(value, start);
    if end < value.len() && is_ascii_token(value[end]) && !is_special_number_end(value[end]) {
        return None;
    }
    let number = value.get(start..end)?;
    if number.is_empty() {
        return None;
    }
    let has_hex_letter = number
        .iter()
        .any(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_digit());
    if has_hex_letter && (number.len() < 4 || number.len() % 2 == 1) {
        return None;
    }
    Some(end)
}

fn is_special_number_start(byte: u8) -> bool {
    matches!(byte, b'_' | b'T' | b'X' | b'x' | b'v' | b's' | b'h' | b'm')
}

fn is_special_number_end(byte: u8) -> bool {
    matches!(byte, b'_' | b'T' | b'Z' | b's' | b'm' | b'h' | b'u' | b'n')
}

fn index_generic_placeholder_end(
    value: &[u8],
    start: usize,
    count: usize,
    separator: u8,
) -> Option<usize> {
    let mut end = index_placeholder_number_end(value, start)?;
    for _ in 1..count {
        if value.get(end).copied()? != separator {
            return None;
        }
        end = index_placeholder_number_end(value, end + 1)?;
    }
    Some(end)
}

fn index_time_end(value: &[u8], start: usize) -> Option<usize> {
    let end = index_generic_placeholder_end(value, start, 3, b':')?;
    if matches!(value.get(end), Some(b'.' | b',')) {
        if let Some(fraction_end) = index_placeholder_number_end(value, end + 1) {
            return Some(fraction_end);
        }
    }
    Some(end)
}

fn index_date_end(value: &[u8], start: usize) -> Option<usize> {
    index_generic_placeholder_end(value, start, 3, b'-')
        .or_else(|| index_generic_placeholder_end(value, start, 3, b'/'))
}

fn index_datetime_end(value: &[u8], start: usize) -> Option<usize> {
    let date_end = index_date_end(value, start)?;
    if !matches!(value.get(date_end), Some(b'T' | b' ')) {
        return None;
    }
    let time_end = index_time_end(value, date_end + 1)?;
    match value.get(time_end) {
        Some(b'Z') => Some(time_end + 1),
        Some(b'+' | b'-') => index_generic_placeholder_end(value, time_end + 1, 2, b':'),
        _ => Some(time_end),
    }
}

fn index_word_start(value: &[u8], mut offset: usize) -> Option<usize> {
    while offset < value.len() && std::str::from_utf8(&value[offset..]).is_err() {
        offset += 1;
    }
    let value = std::str::from_utf8(value.get(offset..)?).ok()?;
    pattern_token_start_regex()
        .find(value)
        .map(|matched| offset + matched.start())
}

fn index_word_end(value: &[u8], start: usize) -> Option<usize> {
    if start >= value.len() {
        return None;
    }
    if matches!(value[start], b'\'' | b'"' | b'`') {
        return index_quoted_word_end(value, start);
    }
    let value = std::str::from_utf8(value.get(start..)?).ok()?;
    Some(
        pattern_token_end_regex()
            .find(value)
            .map_or(start + value.len(), |matched| start + matched.start()),
    )
}

fn utf8_character_at(value: &[u8], offset: usize) -> Option<char> {
    std::str::from_utf8(value.get(offset..)?)
        .ok()?
        .chars()
        .next()
}

fn pattern_token_start_regex() -> &'static regex::Regex {
    // VictoriaLogs uses Go's unicode.IsLetter || unicode.IsDigit rather than
    // the wider Unicode Alphabetic or Number properties. In particular,
    // Letter_Number (Nl), Other_Number (No), and combining marks are not word
    // runes, while every Decimal_Number (Nd) remains valid.
    static TOKEN_START: OnceLock<regex::Regex> = OnceLock::new();
    TOKEN_START.get_or_init(|| {
        regex::Regex::new(r"[\p{L}\p{Nd}_]")
            .expect("VictoriaLogs token-rune regex is a build-time constant")
    })
}

fn pattern_token_end_regex() -> &'static regex::Regex {
    static TOKEN_END: OnceLock<regex::Regex> = OnceLock::new();
    TOKEN_END.get_or_init(|| {
        regex::Regex::new(r"[^\p{L}\p{Nd}_]")
            .expect("VictoriaLogs token-rune regex is a build-time constant")
    })
}

fn index_quoted_word_end(value: &[u8], start: usize) -> Option<usize> {
    let delimiter = *value.get(start)?;
    if delimiter == b'`' {
        return value[start + 1..]
            .iter()
            .position(|byte| *byte == b'`')
            .map(|relative| start + relative + 2);
    }
    let mut offset = start + 1;
    while offset < value.len() {
        match value[offset] {
            byte if byte == delimiter => return Some(offset + 1),
            b'\n' | b'\r' => return None,
            b'\\' => {
                offset += 1;
                let escape = *value.get(offset)?;
                offset += 1;
                match escape {
                    b'a' | b'b' | b'f' | b'n' | b'r' | b't' | b'v' | b'\\' | b'\'' | b'"' => {}
                    b'x' => validate_quoted_digits(value, &mut offset, 2, 16, false)?,
                    b'u' => validate_quoted_digits(value, &mut offset, 4, 16, true)?,
                    b'U' => validate_quoted_digits(value, &mut offset, 8, 16, true)?,
                    b'0'..=b'7' => {
                        offset -= 1;
                        validate_quoted_digits(value, &mut offset, 3, 8, false)?;
                    }
                    _ => return None,
                }
            }
            _ => {
                let character = utf8_character_at(value, offset)?;
                offset += character.len_utf8();
            }
        }
    }
    None
}

fn validate_quoted_digits(
    value: &[u8],
    offset: &mut usize,
    count: usize,
    radix: u32,
    unicode: bool,
) -> Option<()> {
    let end = offset.checked_add(count)?;
    let digits = value.get(*offset..end)?;
    let mut decoded = 0u32;
    for digit in digits {
        decoded = decoded.checked_mul(radix)? + char::from(*digit).to_digit(radix)?;
    }
    if unicode && char::from_u32(decoded).is_none() {
        return None;
    }
    if radix == 8 && decoded > u8::MAX as u32 {
        return None;
    }
    *offset = end;
    Some(())
}

#[derive(Clone, Debug)]
pub enum LogPredicate {
    True,
    And(Vec<LogPredicate>),
    Or(Vec<LogPredicate>),
    Not(Box<LogPredicate>),
    Word {
        field: LogField,
        value: String,
        case_insensitive: bool,
    },
    Phrase {
        field: LogField,
        value: String,
        case_insensitive: bool,
    },
    Prefix {
        field: LogField,
        value: String,
        phrase: bool,
        case_insensitive: bool,
    },
    Substring {
        field: LogField,
        value: String,
        case_insensitive: bool,
    },
    Exact {
        field: LogField,
        value: String,
    },
    /// VictoriaLogs `=value`/`exact(value)` semantics over the public textual
    /// projection of a retained field. This remains distinct from Timeless's
    /// typed exact predicate, which compares JSON types without stringifying.
    TextualExact {
        field: LogField,
        value: String,
    },
    /// Case-sensitive, start-anchored VictoriaLogs `="prefix"*` semantics.
    ExactPrefix {
        field: LogField,
        value: String,
    },
    TypedExact {
        field: LogField,
        value: JsonValue,
    },
    Empty {
        field: LogField,
    },
    AnyValue {
        field: LogField,
    },
    Numeric {
        field: LogField,
        operator: NumericOp,
        value: serde_json::Number,
    },
    ValueType {
        field: LogField,
        kind: ValueTypeKind,
    },
    Timestamp {
        minimum: Option<i64>,
        maximum: Option<i64>,
    },
    Regex {
        field: LogField,
        regex: regex::Regex,
    },
    PatternMatch {
        field: LogField,
        matcher: PatternMatcher,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetadataExact {
    pub path: Vec<String>,
    pub expected: JsonValue,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryRow {
    pub ts: i64,
    pub level: String,
    pub message: String,
    pub metadata_json: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StorageStats {
    pub total_blocks: i64,
    pub total_entries: i64,
    pub total_bytes: i64,
    pub disk_size: i64,
    pub index_size: i64,
    pub database_file_bytes: u64,
    pub database_wal_bytes: u64,
    pub database_shm_bytes: u64,
    pub physical_database_bytes: u64,
    pub sqlite_page_bytes: i64,
    pub freelist_pages: i64,
    pub freelist_bytes: i64,
    pub writer_connections: usize,
    pub reader_connections: usize,
    pub command_queue_capacity_batches: usize,
    pub term_postings: i64,
    pub oldest_timestamp: Option<i64>,
    pub newest_timestamp: Option<i64>,
    pub raw_blocks: i64,
    pub raw_bytes: i64,
    pub compressed_blocks: i64,
    pub compressed_bytes: i64,
    pub buffered_entries: i64,
    pub queued_batches: i64,
    pub queued_entries: i64,
    pub oldest_queued_ms: i64,
    pub admitted_batches: i64,
    pub admitted_entries: i64,
    pub completed_batches: i64,
    pub completed_entries: i64,
    pub api_parse_ns: i64,
    pub api_batch_encode_ns: i64,
    pub api_sqlite_insert_ns: i64,
    pub api_queue_wait_ns: i64,
    pub api_queue_wait_max_ns: i64,
    pub api_query_count: i64,
    pub api_query_ns: i64,
    pub api_query_in_flight: i64,
    pub api_query_cancelled: i64,
    pub api_query_errors: i64,
    pub api_query_result_rows: i64,
    pub api_query_response_bytes: i64,
    pub ingest_batch_count: i64,
    pub ingest_batch_entries: i64,
    pub ingest_wire_decode_ns: i64,
    pub ingest_normalize_ns: i64,
    pub ingest_buffer_append_ns: i64,
    pub flush_count: i64,
    pub flush_entries: i64,
    pub flush_total_ns: i64,
    pub flush_partition_ns: i64,
    pub flush_encode_terms_ns: i64,
    pub flush_store_ns: i64,
    pub query_count: i64,
    pub query_total_ns: i64,
    pub query_snapshot_ns: i64,
    pub query_materialize_ns: i64,
    pub query_snapshot_payload_bytes: i64,
    pub query_snapshot_payload_max_bytes: i64,
    pub query_snapshot_buffered_entries: i64,
    pub query_stable_location_snapshots: i64,
    pub query_payload_bytes_read: i64,
    pub query_candidate_blocks: i64,
    pub query_decoded_entries: i64,
    pub query_matched_entries: i64,
    pub query_returned_entries: i64,
    pub query_bounded_count: i64,
    pub query_bounded_requested_entries: i64,
    pub query_bounded_max_entries: i64,
    pub query_blocks_skipped_by_bound: i64,
    pub native_count_count: i64,
    pub native_count_total_ns: i64,
    pub native_count_snapshot_ns: i64,
    pub native_count_payload_bytes_read: i64,
    pub native_count_metadata_blocks: i64,
    pub native_count_metadata_entries: i64,
    pub native_count_decoded_blocks: i64,
    pub native_count_decoded_entries: i64,
    pub optimize_count: i64,
    pub optimize_total_ns: i64,
    pub optimize_blocks_removed: i64,
    pub optimize_blocks_written: i64,
    pub optimize_budgeted_count: i64,
    pub optimize_budget_entries: i64,
    pub optimize_budget_limited_count: i64,
    pub optimize_raw_groups: i64,
    pub optimize_raw_blocks: i64,
    pub optimize_raw_entries: i64,
    pub optimize_raw_input_bytes: i64,
    pub optimize_raw_output_bytes: i64,
    pub optimize_raw_total_ns: i64,
    pub optimize_merge_groups: i64,
    pub optimize_merge_blocks: i64,
    pub optimize_merge_entries: i64,
    pub optimize_merge_input_bytes: i64,
    pub optimize_merge_output_bytes: i64,
    pub optimize_merge_total_ns: i64,
    pub optimize_pending_raw_blocks: i64,
    pub optimize_pending_raw_entries: i64,
    pub optimize_merge_ready_groups: i64,
    pub optimize_merge_ready_blocks: i64,
    pub optimize_merge_ready_entries: i64,
    pub optimize_merge_deferred_blocks: i64,
    pub optimize_merge_deferred_entries: i64,
    pub read_permit_count: i64,
    pub read_permit_hold_ns: i64,
    pub read_conflicts: i64,
    pub read_barge_rejections: i64,
    pub waiting_writers: i64,
    pub writer_wait_count: i64,
    pub writer_wait_ns: i64,
    pub writer_timeouts: i64,
    pub checkpoint_count: i64,
    pub checkpoint_total_ns: i64,
    pub checkpoint_errors: i64,
    pub backup_count: i64,
    pub backup_total_ns: i64,
    pub backup_errors: i64,
    pub last_error: Option<String>,
}

#[derive(Default)]
struct QueueProfile {
    pending: VecDeque<(Instant, usize)>,
    admitted_batches: u64,
    admitted_entries: u64,
    completed_batches: u64,
    completed_entries: u64,
    parse_ns: u64,
    batch_encode_ns: u64,
    sqlite_insert_ns: u64,
    queue_wait_ns: u64,
    queue_wait_max_ns: u64,
    query_count: u64,
    query_ns: u64,
    query_in_flight: u64,
    query_cancelled: u64,
    query_errors: u64,
    query_result_rows: u64,
    query_response_bytes: u64,
    checkpoint_count: u64,
    checkpoint_total_ns: u64,
    checkpoint_errors: u64,
    backup_count: u64,
    backup_total_ns: u64,
    backup_errors: u64,
    last_error: Option<String>,
}

enum WriteCommand {
    Ingest(Vec<LogEntry>),
    Flush(Option<oneshot::Sender<Result<(), String>>>),
    Optimize,
    Barrier(oneshot::Sender<()>),
    Backup {
        destination: PathBuf,
        reply: oneshot::Sender<Result<BackupReport, String>>,
    },
    Shutdown(oneshot::Sender<Result<(), String>>),
}

enum ReadCommand {
    Query {
        spec: QuerySpec,
        cancelled: Arc<AtomicBool>,
        reply: oneshot::Sender<Result<Vec<QueryRow>, String>>,
    },
    Pipeline {
        spec: QuerySpec,
        operations: Vec<PipelineOp>,
        implicit_result_limit: Option<usize>,
        rate_window_seconds: Option<f64>,
        timestamp_unit: TimestampUnit,
        limits: PipelineLimits,
        cancelled: Arc<AtomicBool>,
        reply: oneshot::Sender<Result<Vec<JsonValue>, String>>,
    },
    Count {
        spec: QuerySpec,
        cancelled: Arc<AtomicBool>,
        reply: oneshot::Sender<Result<i64, String>>,
    },
    FieldValues {
        spec: QuerySpec,
        key: String,
        limit: usize,
        cancelled: Arc<AtomicBool>,
        reply: oneshot::Sender<Result<Vec<String>, String>>,
    },
    Stats(oneshot::Sender<Result<StorageStats, String>>),
    Shutdown,
}

// Optimize remains extension-owned. The API timer is only a maintenance
// wake-up; this byte target turns the extension's current actionable backlog
// into a bounded entry budget without adding a host-side flush/block policy.
const OPTIMIZE_SOURCE_BYTE_BUDGET: u64 = 32 * 1024 * 1024;
const OPTIMIZE_TARGET_ENTRIES: usize = 8192;
const MAX_BACKUP_OPTIMIZE_STEPS: usize = 1_000_000;

struct StorageInner {
    writer: mpsc::Sender<WriteCommand>,
    readers: Vec<mpsc::Sender<ReadCommand>>,
    next_reader: AtomicUsize,
    profile: Arc<StdMutex<QueueProfile>>,
    joins: Mutex<Vec<JoinHandle<Result<(), String>>>>,
    admission: Mutex<()>,
    lease: StdMutex<Option<File>>,
    shutting_down: AtomicBool,
    timestamp_unit: TimestampUnit,
    database_path: PathBuf,
    queue_capacity: usize,
}

#[derive(Clone)]
pub struct Storage(Arc<StorageInner>);

impl Storage {
    pub fn start(
        database_path: PathBuf,
        extension_path: PathBuf,
        reader_connections: usize,
        queue_batches: usize,
    ) -> Result<Self, String> {
        Self::start_with_timestamp_unit(
            database_path,
            extension_path,
            reader_connections,
            queue_batches,
            TimestampUnit::Milliseconds,
        )
    }

    pub fn start_with_timestamp_unit(
        database_path: PathBuf,
        extension_path: PathBuf,
        reader_connections: usize,
        queue_batches: usize,
        timestamp_unit: TimestampUnit,
    ) -> Result<Self, String> {
        if reader_connections == 0 {
            return Err("reader_connections must be positive".into());
        }
        if queue_batches == 0 {
            return Err("command_queue_batches must be positive".into());
        }
        if let Some(parent) = database_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!("create database directory {}: {error}", parent.display())
            })?;
        }
        let lease = acquire_database_lease(&database_path, "logs")?;
        let (writer_tx, writer_rx) = mpsc::channel(queue_batches);
        let (ready_tx, ready_rx) = std_mpsc::channel();
        let profile = Arc::new(StdMutex::new(QueueProfile::default()));
        let writer_profile = Arc::clone(&profile);
        let writer_db = database_path.clone();
        let writer_ext = extension_path.clone();
        let writer_join = thread::Builder::new()
            .name("timeless-logs-writer".into())
            .spawn(move || {
                writer_main(
                    writer_db,
                    writer_ext,
                    writer_rx,
                    ready_tx,
                    writer_profile,
                    timestamp_unit,
                )
            })
            .map_err(|e| format!("spawn SQLite writer: {e}"))?;
        ready_rx
            .recv()
            .map_err(|_| "SQLite writer exited during startup".to_string())??;

        let mut readers = Vec::with_capacity(reader_connections);
        let mut joins = vec![writer_join];
        for number in 0..reader_connections {
            let (reader_tx, reader_rx) = mpsc::channel(queue_batches);
            let (ready_tx, ready_rx) = std_mpsc::channel();
            let reader_db = database_path.clone();
            let reader_ext = extension_path.clone();
            let reader_profile = Arc::clone(&profile);
            let join = thread::Builder::new()
                .name(format!("timeless-logs-reader-{number}"))
                .spawn(move || {
                    reader_main(reader_db, reader_ext, reader_rx, ready_tx, reader_profile)
                })
                .map_err(|e| format!("spawn SQLite reader {number}: {e}"))?;
            ready_rx
                .recv()
                .map_err(|_| format!("SQLite reader {number} exited during startup"))??;
            readers.push(reader_tx);
            joins.push(join);
        }

        Ok(Storage(Arc::new(StorageInner {
            writer: writer_tx,
            readers,
            next_reader: AtomicUsize::new(0),
            profile,
            joins: Mutex::new(joins),
            admission: Mutex::new(()),
            lease: StdMutex::new(Some(lease)),
            shutting_down: AtomicBool::new(false),
            timestamp_unit,
            database_path,
            queue_capacity: queue_batches,
        })))
    }

    pub fn timestamp_unit(&self) -> TimestampUnit {
        self.0.timestamp_unit
    }

    pub fn is_ready(&self) -> bool {
        !self.0.shutting_down.load(Ordering::Acquire)
    }

    pub async fn ingest(&self, entries: Vec<LogEntry>) -> Result<usize, String> {
        let _admission = self.0.admission.lock().await;
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err("logs data plane is shutting down".into());
        }
        let count = entries.len();
        let permit = self
            .0
            .writer
            .reserve()
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        {
            let mut profile = profile_lock(&self.0.profile);
            profile.pending.push_back((Instant::now(), count));
            profile.admitted_batches = profile.admitted_batches.saturating_add(1);
            profile.admitted_entries = profile.admitted_entries.saturating_add(count as u64);
        }
        permit.send(WriteCommand::Ingest(entries));
        Ok(count)
    }

    pub fn record_parse(&self, duration: Duration) {
        let mut profile = profile_lock(&self.0.profile);
        profile.parse_ns = profile.parse_ns.saturating_add(duration_ns(duration));
    }

    pub async fn schedule_flush(&self) -> Result<(), String> {
        self.0
            .writer
            .send(WriteCommand::Flush(None))
            .await
            .map_err(|_| "SQLite writer is not running".to_string())
    }

    pub async fn flush(&self) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Flush(Some(reply_tx)))
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before flush completed".to_string())?
    }

    pub async fn schedule_optimize(&self) -> Result<(), String> {
        self.0
            .writer
            .send(WriteCommand::Optimize)
            .await
            .map_err(|_| "SQLite writer is not running".to_string())
    }

    /// Ordered API test/administration barrier. It changes no storage state;
    /// the reply only proves all previously admitted batches reached the
    /// established extension ingest path.
    pub async fn barrier(&self) -> Result<(), String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Barrier(reply_tx))
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before barrier".to_string())
    }

    pub async fn backup(&self, destination: PathBuf) -> Result<BackupReport, String> {
        let _ordered = self.0.admission.lock().await;
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err("logs API is shutting down; backup is closed".into());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Backup {
                destination,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "SQLite writer is not running".to_string())?;
        drop(_ordered);
        reply_rx
            .await
            .map_err(|_| "SQLite writer stopped before backup completed".to_string())?
    }

    pub async fn query(&self, spec: QuerySpec) -> Result<Vec<QueryRow>, String> {
        validate_query_spec(&spec)?;
        let (cancelled, mut cancellation) = self.begin_read();
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .reader()
            .send(ReadCommand::Query {
                spec,
                cancelled,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            cancellation.disarm();
            record_read_return(&self.0.profile);
            return Err("SQLite reader is not running".into());
        }
        cancellation.handoff_to_reader();
        let result = match reply_rx.await {
            Ok(result) => result,
            Err(_) => {
                cancellation.disarm();
                record_read_return(&self.0.profile);
                return Err("SQLite reader stopped before query completed".into());
            }
        };
        cancellation.disarm();
        result
    }

    /// Return the complete bounded base rowset for ordered API-owned query
    /// transforms. The extension still owns every storage read and enforces
    /// `max_work_entries`; this method only prevents an API pipeline from
    /// mistaking a truncated rowset for a complete aggregate input.
    pub(crate) async fn pipeline(
        &self,
        spec: QuerySpec,
        operations: Vec<PipelineOp>,
        implicit_result_limit: Option<usize>,
        rate_window_seconds: Option<f64>,
        limits: PipelineLimits,
    ) -> Result<Vec<JsonValue>, String> {
        validate_work_limit(&spec)?;
        let (cancelled, mut cancellation) = self.begin_read();
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .reader()
            .send(ReadCommand::Pipeline {
                spec,
                operations,
                implicit_result_limit,
                rate_window_seconds,
                timestamp_unit: self.timestamp_unit(),
                limits,
                cancelled,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            cancellation.disarm();
            record_read_return(&self.0.profile);
            return Err("SQLite reader is not running".into());
        }
        cancellation.handoff_to_reader();
        let result = match reply_rx.await {
            Ok(result) => result,
            Err(_) => {
                cancellation.disarm();
                record_read_return(&self.0.profile);
                return Err("SQLite reader stopped before LogsQL pipeline completed".into());
            }
        };
        cancellation.disarm();
        result
    }

    pub async fn count(&self, spec: QuerySpec) -> Result<i64, String> {
        validate_work_limit(&spec)?;
        let (cancelled, mut cancellation) = self.begin_read();
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .reader()
            .send(ReadCommand::Count {
                spec,
                cancelled,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            cancellation.disarm();
            record_read_return(&self.0.profile);
            return Err("SQLite reader is not running".into());
        }
        cancellation.handoff_to_reader();
        let result = match reply_rx.await {
            Ok(result) => result,
            Err(_) => {
                cancellation.disarm();
                record_read_return(&self.0.profile);
                return Err("SQLite reader stopped before count completed".into());
            }
        };
        cancellation.disarm();
        result
    }

    pub async fn field_values(
        &self,
        spec: QuerySpec,
        key: String,
        limit: usize,
    ) -> Result<Vec<String>, String> {
        validate_work_limit(&spec)?;
        let (cancelled, mut cancellation) = self.begin_read();
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .reader()
            .send(ReadCommand::FieldValues {
                spec,
                key,
                limit,
                cancelled,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            cancellation.disarm();
            record_read_return(&self.0.profile);
            return Err("SQLite reader is not running".into());
        }
        cancellation.handoff_to_reader();
        let result = match reply_rx.await {
            Ok(result) => result,
            Err(_) => {
                cancellation.disarm();
                record_read_return(&self.0.profile);
                return Err("SQLite reader stopped before field discovery completed".into());
            }
        };
        cancellation.disarm();
        result
    }

    pub(crate) fn record_query_response_bytes(&self, bytes: usize) {
        let mut profile = profile_lock(&self.0.profile);
        profile.query_response_bytes = profile.query_response_bytes.saturating_add(bytes as u64);
    }

    pub async fn stats(&self) -> Result<StorageStats, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader()
            .send(ReadCommand::Stats(reply_tx))
            .await
            .map_err(|_| "SQLite reader is not running".to_string())?;
        let mut stats = reply_rx
            .await
            .map_err(|_| "SQLite reader stopped before stats completed".to_string())??;
        let profile = profile_lock(&self.0.profile);
        stats.queued_batches = profile.pending.len() as i64;
        stats.queued_entries = profile.pending.iter().map(|(_, count)| *count as i64).sum();
        stats.oldest_queued_ms = profile
            .pending
            .front()
            .map(|(queued_at, _)| queued_at.elapsed().as_millis() as i64)
            .unwrap_or(0);
        stats.admitted_batches = profile.admitted_batches as i64;
        stats.admitted_entries = profile.admitted_entries as i64;
        stats.completed_batches = profile.completed_batches as i64;
        stats.completed_entries = profile.completed_entries as i64;
        stats.api_parse_ns = profile.parse_ns as i64;
        stats.api_batch_encode_ns = profile.batch_encode_ns as i64;
        stats.api_sqlite_insert_ns = profile.sqlite_insert_ns as i64;
        stats.api_queue_wait_ns = profile.queue_wait_ns as i64;
        stats.api_queue_wait_max_ns = profile.queue_wait_max_ns as i64;
        stats.api_query_count = profile.query_count as i64;
        stats.api_query_ns = profile.query_ns as i64;
        stats.api_query_in_flight = profile.query_in_flight as i64;
        stats.api_query_cancelled = profile.query_cancelled as i64;
        stats.api_query_errors = profile.query_errors as i64;
        stats.api_query_result_rows = profile.query_result_rows as i64;
        stats.api_query_response_bytes = profile.query_response_bytes as i64;
        stats.checkpoint_count = profile.checkpoint_count as i64;
        stats.checkpoint_total_ns = profile.checkpoint_total_ns as i64;
        stats.checkpoint_errors = profile.checkpoint_errors as i64;
        stats.backup_count = profile.backup_count as i64;
        stats.backup_total_ns = profile.backup_total_ns as i64;
        stats.backup_errors = profile.backup_errors as i64;
        stats.last_error.clone_from(&profile.last_error);
        stats.writer_connections = 1;
        stats.reader_connections = self.0.readers.len();
        stats.command_queue_capacity_batches = self.0.queue_capacity;
        let (file, wal, shm) = database_file_sizes(&self.0.database_path);
        stats.database_file_bytes = file;
        stats.database_wal_bytes = wal;
        stats.database_shm_bytes = shm;
        stats.physical_database_bytes = file.saturating_add(wal).saturating_add(shm);
        Ok(stats)
    }

    fn reader(&self) -> &mpsc::Sender<ReadCommand> {
        let number = self.0.next_reader.fetch_add(1, Ordering::Relaxed);
        &self.0.readers[number % self.0.readers.len()]
    }

    fn begin_read(&self) -> (Arc<AtomicBool>, ReadCancellation) {
        let cancelled = Arc::new(AtomicBool::new(false));
        {
            let mut profile = profile_lock(&self.0.profile);
            profile.query_in_flight = profile.query_in_flight.saturating_add(1);
        }
        (
            Arc::clone(&cancelled),
            ReadCancellation {
                cancelled,
                profile: Arc::clone(&self.0.profile),
                reader_owned: false,
                armed: true,
            },
        )
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        if self.0.shutting_down.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let _admission = self.0.admission.lock().await;
        for reader in &self.0.readers {
            let _ = reader.send(ReadCommand::Shutdown).await;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let writer_result = match self.0.writer.send(WriteCommand::Shutdown(reply_tx)).await {
            Ok(()) => reply_rx
                .await
                .map_err(|_| "SQLite writer stopped during shutdown".to_string())?,
            Err(_) => Err("SQLite writer is not running".into()),
        };
        let joins = {
            let mut guard = self.0.joins.lock().await;
            std::mem::take(&mut *guard)
        };
        for join in joins {
            join.join()
                .map_err(|_| "SQLite API worker panicked".to_string())??;
        }
        if let Some(file) = self
            .0
            .lease
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            FileExt::unlock(&file)
                .map_err(|error| format!("release database owner lease: {error}"))?;
        }
        writer_result
    }
}

struct ReadCancellation {
    cancelled: Arc<AtomicBool>,
    profile: Arc<StdMutex<QueueProfile>>,
    reader_owned: bool,
    armed: bool,
}

impl ReadCancellation {
    fn handoff_to_reader(&mut self) {
        self.reader_owned = true;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReadCancellation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.cancelled.store(true, Ordering::Release);
        let mut profile = profile_lock(&self.profile);
        profile.query_cancelled = profile.query_cancelled.saturating_add(1);
        if !self.reader_owned {
            profile.query_in_flight = profile.query_in_flight.saturating_sub(1);
        }
    }
}

fn writer_main(
    database_path: PathBuf,
    extension_path: PathBuf,
    mut commands: mpsc::Receiver<WriteCommand>,
    ready: std_mpsc::Sender<Result<(), String>>,
    profile: Arc<StdMutex<QueueProfile>>,
    timestamp_unit: TimestampUnit,
) -> Result<(), String> {
    let conn = match open_connection(&database_path, &extension_path, Some(timestamp_unit)) {
        Ok(conn) => {
            let _ = ready.send(Ok(()));
            conn
        }
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    while let Some(command) = commands.blocking_recv() {
        match command {
            WriteCommand::Ingest(entries) => {
                let count = entries.len();
                record_queue_start(&profile);
                let result = insert_batch(&conn, &entries, &profile);
                record_queue_completion(&profile, count, result.is_ok());
                result?;
            }
            WriteCommand::Flush(reply) => {
                let result = conn
                    .execute("INSERT INTO logs(logs) VALUES ('flush')", [])
                    .map(|_| ())
                    .map_err(|e| format!("flush logs: {e}"));
                if let Some(reply) = reply {
                    let _ = reply.send(result.clone());
                }
                result?;
            }
            WriteCommand::Optimize => {
                optimize_backlog(&conn)?;
            }
            WriteCommand::Barrier(reply) => {
                let _ = reply.send(());
            }
            WriteCommand::Backup { destination, reply } => {
                let started = Instant::now();
                let result = (|| {
                    conn.execute("INSERT INTO logs(logs) VALUES ('flush')", [])
                        .map_err(|error| format!("flush logs for backup: {error}"))?;
                    optimize_all_backlog(&conn)?;
                    let checkpoint_started = Instant::now();
                    let checkpoint = checkpoint_wal(&conn, "logs");
                    record_checkpoint(&profile, checkpoint_started.elapsed(), &checkpoint);
                    let checkpoint = checkpoint?;
                    create_verified_backup(&conn, &destination, "logs", checkpoint)
                })();
                record_backup(&profile, started.elapsed(), &result);
                let _ = reply.send(result);
            }
            WriteCommand::Shutdown(reply) => {
                let flush = conn
                    .execute("INSERT INTO logs(logs) VALUES ('flush')", [])
                    .map(|_| ())
                    .map_err(|e| format!("graceful logs flush: {e}"));
                let checkpoint_started = Instant::now();
                let checkpoint = checkpoint_wal(&conn, "logs").map(|_| ());
                record_checkpoint(&profile, checkpoint_started.elapsed(), &checkpoint);
                let result = match (flush, checkpoint) {
                    (Err(error), _) | (Ok(()), Err(error)) => Err(error),
                    (Ok(()), Ok(())) => Ok(()),
                };
                let _ = reply.send(result.clone());
                return result;
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OptimizeBacklog {
    pending_raw_blocks: u64,
    pending_raw_entries: u64,
    merge_ready_groups: u64,
    merge_ready_blocks: u64,
    merge_ready_entries: u64,
}

impl OptimizeBacklog {
    fn actionable_entries(self) -> u64 {
        self.pending_raw_entries
            .saturating_add(self.merge_ready_entries)
    }
}

fn optimize_made_progress(before: OptimizeBacklog, after: OptimizeBacklog) -> bool {
    after.actionable_entries() == 0 || after != before
}

fn optimize_backlog_state(conn: &Connection) -> Result<OptimizeBacklog, String> {
    let stats = stat_values(conn)?;
    let stat = |key: &str| stats.get(key).copied().unwrap_or(0).max(0) as u64;
    Ok(OptimizeBacklog {
        pending_raw_blocks: stat("optimize_pending_raw_blocks"),
        pending_raw_entries: stat("optimize_pending_raw_entries"),
        merge_ready_groups: stat("optimize_merge_ready_groups"),
        merge_ready_blocks: stat("optimize_merge_ready_blocks"),
        merge_ready_entries: stat("optimize_merge_ready_entries"),
    })
}

fn optimize_backlog(conn: &Connection) -> Result<(), String> {
    let actionable_entries = optimize_backlog_state(conn)?.actionable_entries();
    if actionable_entries == 0 {
        return Ok(());
    }
    optimize_backlog_with_actionable(conn, actionable_entries)
}

fn optimize_backlog_with_actionable(
    conn: &Connection,
    actionable_entries: u64,
) -> Result<(), String> {
    // The extension owns block layout and publishes this source sample through
    // its public stats TVF. The server turns it into a maintenance budget
    // without inspecting private block tables or duplicating block policy.
    let stats = stat_values(conn)?;
    let sample_entries = stats.get("optimize_source_entries").copied().unwrap_or(0);
    let sample_bytes = stats.get("optimize_source_bytes").copied().unwrap_or(0);
    let budget = optimize_entry_budget(
        actionable_entries,
        sample_entries.max(0) as u64,
        sample_bytes.max(0) as u64,
    );
    conn.execute(
        "INSERT INTO logs(logs) VALUES (?1)",
        [format!("optimize:{budget}")],
    )
    .map_err(|error| format!("optimize logs with {budget}-entry budget: {error}"))?;
    Ok(())
}

fn optimize_all_backlog(conn: &Connection) -> Result<(), String> {
    for step in 0..MAX_BACKUP_OPTIMIZE_STEPS {
        let before = optimize_backlog_state(conn)?;
        let actionable = before.actionable_entries();
        if actionable == 0 {
            return Ok(());
        }
        optimize_backlog_with_actionable(conn, actionable)?;
        let after = optimize_backlog_state(conn)?;
        if after.actionable_entries() == 0 {
            return Ok(());
        }
        if !optimize_made_progress(before, after) {
            return Err(format!(
                "logs optimize backlog made no progress at step {step}: {after:?}"
            ));
        }
    }
    Err(format!(
        "logs optimize backlog exceeded {MAX_BACKUP_OPTIMIZE_STEPS} steps"
    ))
}

fn optimize_entry_budget(actionable_entries: u64, sample_entries: u64, sample_bytes: u64) -> usize {
    if actionable_entries == 0 {
        return 0;
    }
    let target_entries = OPTIMIZE_TARGET_ENTRIES as u64;
    if sample_entries == 0 || sample_bytes == 0 {
        return usize::try_from(actionable_entries.min(target_entries)).unwrap_or(usize::MAX);
    }
    let estimated = (u128::from(OPTIMIZE_SOURCE_BYTE_BUDGET)
        .saturating_mul(u128::from(sample_entries))
        .saturating_add(u128::from(sample_bytes - 1))
        / u128::from(sample_bytes))
    .min(u128::from(u64::MAX)) as u64;
    usize::try_from(actionable_entries.min(estimated.max(target_entries))).unwrap_or(usize::MAX)
}

fn reader_main(
    database_path: PathBuf,
    extension_path: PathBuf,
    mut commands: mpsc::Receiver<ReadCommand>,
    ready: std_mpsc::Sender<Result<(), String>>,
    profile: Arc<StdMutex<QueueProfile>>,
) -> Result<(), String> {
    let conn = match open_connection(&database_path, &extension_path, None) {
        Ok(conn) => {
            let _ = ready.send(Ok(()));
            conn
        }
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    while let Some(command) = commands.blocking_recv() {
        match command {
            ReadCommand::Query {
                spec,
                cancelled,
                reply,
            } => {
                let started = Instant::now();
                let result = cancellable_read(&conn, &cancelled, || {
                    query_rows(&conn, &spec, cancelled.as_ref())
                });
                let rows = result.as_ref().map_or(0, Vec::len);
                record_query(&profile, started.elapsed(), result.is_err(), rows);
                let _ = reply.send(result);
            }
            ReadCommand::Pipeline {
                spec,
                operations,
                implicit_result_limit,
                rate_window_seconds,
                timestamp_unit,
                limits,
                cancelled,
                reply,
            } => {
                let started = Instant::now();
                let result = cancellable_read(&conn, &cancelled, || {
                    let rows = query_pipeline_rows(&conn, &spec, cancelled.as_ref())?;
                    pipeline::execute_query_rows(
                        rows,
                        &operations,
                        implicit_result_limit,
                        rate_window_seconds,
                        timestamp_unit,
                        limits,
                        cancelled.as_ref(),
                    )
                });
                let rows = result.as_ref().map_or(0, Vec::len);
                record_query(&profile, started.elapsed(), result.is_err(), rows);
                let _ = reply.send(result);
            }
            ReadCommand::Count {
                spec,
                cancelled,
                reply,
            } => {
                let started = Instant::now();
                let result = cancellable_read(&conn, &cancelled, || {
                    query_count(&conn, &spec, cancelled.as_ref())
                });
                let rows = usize::from(result.is_ok());
                record_query(&profile, started.elapsed(), result.is_err(), rows);
                let _ = reply.send(result);
            }
            ReadCommand::FieldValues {
                spec,
                key,
                limit,
                cancelled,
                reply,
            } => {
                let started = Instant::now();
                let result = cancellable_read(&conn, &cancelled, || {
                    query_field_values(&conn, &spec, &key, limit)
                });
                let rows = result.as_ref().map_or(0, Vec::len);
                record_query(&profile, started.elapsed(), result.is_err(), rows);
                let _ = reply.send(result);
            }
            ReadCommand::Stats(reply) => {
                let _ = reply.send(retry_read(|| storage_stats(&conn)));
            }
            ReadCommand::Shutdown => return Ok(()),
        }
    }
    Ok(())
}

fn open_connection(
    path: &Path,
    extension: &Path,
    initialize: Option<TimestampUnit>,
) -> Result<Connection, String> {
    let conn = Connection::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    unsafe {
        conn.load_extension_enable()
            .map_err(|e| format!("enable extension loading: {e}"))?;
        conn.load_extension(extension, None::<&str>)
            .map_err(|e| format!("load {}: {e}", extension.display()))?;
    }
    conn.load_extension_disable()
        .map_err(|e| format!("disable extension loading: {e}"))?;
    let spec = DataPlaneSpec {
        signal: "logs",
        required_batch: "rich-v1",
    };
    let capabilities = preflight_extension(&conn, spec)?;
    for surface in ["timeless_logs", "timeless_log_count", "timeless_log_values"] {
        require_query_surface(&capabilities, surface, "max_work_entries")?;
    }
    preflight_database(&conn, spec.signal)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|e| format!("set busy timeout: {e}"))?;
    if let Some(timestamp_unit) = initialize {
        conn.execute_batch(&format!(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA auto_vacuum = INCREMENTAL;
             CREATE VIRTUAL TABLE IF NOT EXISTS logs USING timeless_logs(
               index_keys='service,path,status,host', timestamp_unit='{}');",
            timestamp_unit.sql_name()
        ))
        .map_err(|e| format!("initialize logs database: {e}"))?;
        apply_schema_ledger(&conn, spec, &capabilities)?;
        let stored = stat_text(&conn, "timestamp_unit")?;
        if stored.as_deref() != Some(timestamp_unit.sql_name()) {
            return Err(format!(
                "logs timestamp capability mismatch: binary requested {}, database stores {}",
                timestamp_unit.sql_name(),
                stored.as_deref().unwrap_or("<missing>")
            ));
        }
    } else {
        require_current_schema(&conn, spec.signal)?;
    }
    Ok(conn)
}

/// The extension deliberately reports a retryable conflict when a reader
/// reaches a shared engine during the writer's short virtual-table
/// transaction. HTTP callers should wait behind that publication boundary,
/// not receive a spurious 500. This is API scheduling only; it does not alter
/// the engine, its buffer, or its transactions.
fn retry_read<T>(mut operation: impl FnMut() -> Result<T, String>) -> Result<T, String> {
    retry_read_with_cancellation(&mut operation, None)
}

fn retry_read_with_cancellation<T>(
    operation: &mut impl FnMut() -> Result<T, String>,
    cancelled: Option<&AtomicBool>,
) -> Result<T, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if cancelled.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
            return Err("logs query cancelled".into());
        }
        match operation() {
            Ok(value) => return Ok(value),
            Err(error)
                if std::time::Instant::now() < deadline
                    && (error.contains("active write transaction")
                        || error.contains("pending writer transaction")
                        || error.contains("database is locked")
                        || error.contains("database is busy")) =>
            {
                if cancelled.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
                    return Err("logs query cancelled".into());
                }
                thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => return Err(error),
        }
    }
}

fn cancellable_read<T>(
    conn: &Connection,
    cancelled: &Arc<AtomicBool>,
    mut operation: impl FnMut() -> Result<T, String>,
) -> Result<T, String> {
    let progress_cancelled = Arc::clone(cancelled);
    conn.progress_handler(
        1_000,
        Some(move || progress_cancelled.load(Ordering::Acquire)),
    )
    .map_err(|error| format!("install log query cancellation handler: {error}"))?;
    let result = retry_read_with_cancellation(&mut operation, Some(cancelled));
    let cleared = conn.progress_handler(0, None::<fn() -> bool>);
    if cancelled.load(Ordering::Acquire) {
        return Err("logs query cancelled".into());
    }
    match (result, cleared) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(format!("clear log query cancellation handler: {error}")),
    }
}

fn profile_lock(profile: &StdMutex<QueueProfile>) -> std::sync::MutexGuard<'_, QueueProfile> {
    profile.lock().unwrap_or_else(|error| error.into_inner())
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn elapsed_ns(started: Instant) -> u64 {
    duration_ns(started.elapsed())
}

fn record_queue_start(profile: &StdMutex<QueueProfile>) {
    let mut profile = profile_lock(profile);
    if let Some((queued_at, _)) = profile.pending.front() {
        let wait_ns = elapsed_ns(*queued_at);
        profile.queue_wait_ns = profile.queue_wait_ns.saturating_add(wait_ns);
        profile.queue_wait_max_ns = profile.queue_wait_max_ns.max(wait_ns);
    }
}

fn record_queue_completion(profile: &StdMutex<QueueProfile>, count: usize, success: bool) {
    let mut profile = profile_lock(profile);
    let queued = profile.pending.pop_front();
    debug_assert_eq!(queued.map(|(_, queued_count)| queued_count), Some(count));
    if success {
        profile.completed_batches = profile.completed_batches.saturating_add(1);
        profile.completed_entries = profile.completed_entries.saturating_add(count as u64);
    }
}

fn record_query(
    profile: &StdMutex<QueueProfile>,
    duration: Duration,
    error: bool,
    result_rows: usize,
) {
    let mut profile = profile_lock(profile);
    profile.query_count = profile.query_count.saturating_add(1);
    profile.query_ns = profile.query_ns.saturating_add(duration_ns(duration));
    profile.query_in_flight = profile.query_in_flight.saturating_sub(1);
    profile.query_errors = profile.query_errors.saturating_add(u64::from(error));
    profile.query_result_rows = profile.query_result_rows.saturating_add(result_rows as u64);
}

fn record_read_return(profile: &StdMutex<QueueProfile>) {
    let mut profile = profile_lock(profile);
    profile.query_in_flight = profile.query_in_flight.saturating_sub(1);
}

fn insert_batch(
    conn: &Connection,
    entries: &[LogEntry],
    profile: &StdMutex<QueueProfile>,
) -> Result<(), String> {
    if entries.is_empty() {
        return Ok(());
    }
    let encode_started = Instant::now();
    let blob = encode_batch(entries)?;
    let encode_ns = elapsed_ns(encode_started);
    let insert_started = Instant::now();
    let result = conn
        .execute("INSERT INTO logs(logs) VALUES (?1)", params![blob])
        .map(|_| ())
        .map_err(|e| format!("insert logs batch: {e}"));
    let insert_ns = elapsed_ns(insert_started);
    let mut profile = profile_lock(profile);
    profile.batch_encode_ns = profile.batch_encode_ns.saturating_add(encode_ns);
    profile.sqlite_insert_ns = profile.sqlite_insert_ns.saturating_add(insert_ns);
    result
}

fn encode_batch(entries: &[LogEntry]) -> Result<Vec<u8>, String> {
    let count = u32::try_from(entries.len()).map_err(|_| "log batch exceeds u32::MAX entries")?;
    let mut out = Vec::with_capacity(8 + entries.len() * 80);
    out.push(0x02);
    out.push(0x00);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    for entry in entries {
        out.extend_from_slice(&entry.ts.to_le_bytes());
    }
    for entry in entries {
        if entry.level > 3 {
            return Err(format!("invalid log level {}", entry.level));
        }
        push_string(&mut out, &entry.severity)?;
    }
    for entry in entries {
        push_string(&mut out, &entry.message)?;
    }
    for entry in entries {
        push_string(&mut out, &entry.metadata_json)?;
    }
    Ok(out)
}

fn push_string(out: &mut Vec<u8>, value: &str) -> Result<(), String> {
    let len = u32::try_from(value.len()).map_err(|_| "log string exceeds u32::MAX bytes")?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn validate_work_limit(spec: &QuerySpec) -> Result<(), String> {
    if spec.max_work_rows == 0 {
        return Err("max_work_rows must be positive".into());
    }
    Ok(())
}

fn validate_query_spec(spec: &QuerySpec) -> Result<(), String> {
    validate_work_limit(spec)?;
    if spec.limit > 100_000 {
        return Err("log query limit exceeds the extension maximum of 100000 rows".into());
    }
    let window = spec
        .offset
        .checked_add(spec.limit)
        .ok_or_else(|| "log query offset plus limit overflows usize".to_string())?;
    if window > spec.max_work_rows {
        return Err(format!(
            "log query offset plus limit exceeds max_work_rows={}",
            spec.max_work_rows
        ));
    }
    Ok(())
}

fn query_parts(spec: &QuerySpec) -> Result<(String, Vec<SqlValue>), String> {
    let mut clauses = Vec::new();
    let mut values = Vec::new();
    if let Some(level) = &spec.level {
        clauses.push("level = ?");
        values.push(SqlValue::Text(level.clone()));
    }
    if let Some(service) = &spec.service {
        clauses.push("service = ?");
        values.push(SqlValue::Text(service.clone()));
    }
    for (key, value) in &spec.metadata_eq {
        if !matches!(key.as_str(), "service" | "host" | "path" | "status") {
            return Err(format!("unsupported indexed log metadata field {key:?}"));
        }
        clauses.push(match key.as_str() {
            "service" => "service = ?",
            "host" => "host = ?",
            "path" => "path = ?",
            "status" => "status = ?",
            _ => unreachable!("metadata field validated above"),
        });
        values.push(SqlValue::Text(value.clone()));
    }
    if let Some(message) = &spec.message {
        clauses.push("message_contains = ?");
        values.push(SqlValue::Text(message.clone()));
    }
    if let Some(ts_min) = spec.ts_min {
        clauses.push("ts >= ?");
        values.push(SqlValue::Integer(ts_min));
    }
    if let Some(ts_max) = spec.ts_max {
        clauses.push("ts <= ?");
        values.push(SqlValue::Integer(ts_max));
    }
    clauses.push("max_work_entries = ?");
    values.push(SqlValue::Integer(
        i64::try_from(spec.max_work_rows)
            .map_err(|_| "max_work_rows exceeds SQLite INTEGER range".to_string())?,
    ));
    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    Ok((where_sql, values))
}

fn query_rows(
    conn: &Connection,
    spec: &QuerySpec,
    cancelled: &AtomicBool,
) -> Result<Vec<QueryRow>, String> {
    if spec.limit == 0 {
        return Ok(Vec::new());
    }
    if has_api_postfilter(spec) {
        return query_rows_with_postfilters(conn, spec, cancelled);
    }
    let (where_sql, mut values) = query_parts(spec)?;
    let order = if spec.descending { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT ts, level, message, metadata FROM logs{where_sql} \
         ORDER BY ts {order} LIMIT ? OFFSET ?"
    );
    values.push(SqlValue::Integer(
        i64::try_from(spec.limit.max(1))
            .map_err(|_| "log query limit exceeds SQLite INTEGER range".to_string())?,
    ));
    values.push(SqlValue::Integer(spec.offset as i64));
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("prepare query: {e}"))?;
    let rows = stmt
        .query_map(params_from_iter(values), |row| {
            Ok(QueryRow {
                ts: row.get(0)?,
                level: row.get(1)?,
                message: row.get(2)?,
                metadata_json: row.get(3)?,
            })
        })
        .map_err(|e| format!("query logs: {e}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read log row: {e}"))
}

fn query_rows_with_postfilters(
    conn: &Connection,
    spec: &QuerySpec,
    cancelled: &AtomicBool,
) -> Result<Vec<QueryRow>, String> {
    if spec.limit == 0 {
        return Ok(Vec::new());
    }
    let (where_sql, mut values) = query_parts(spec)?;
    let order = if spec.descending { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT ts, level, message, metadata FROM logs{where_sql} \
         ORDER BY ts {order} LIMIT ?"
    );
    values.push(SqlValue::Integer(
        i64::try_from(spec.max_work_rows.saturating_add(1)).unwrap_or(i64::MAX),
    ));
    let mut statement = conn
        .prepare(&sql)
        .map_err(|error| format!("prepare LogsQL post-filter query: {error}"))?;
    let mut rows = statement
        .query(params_from_iter(values))
        .map_err(|error| format!("query LogsQL post-filter candidates: {error}"))?;
    let limit = spec.limit;
    let mut considered = 0usize;
    let mut matched = 0usize;
    let mut output = Vec::new();
    loop {
        ensure_query_active(cancelled)?;
        let Some(row) = rows
            .next()
            .map_err(|error| format!("read LogsQL post-filter candidate: {error}"))?
        else {
            break;
        };
        considered = considered.saturating_add(1);
        if considered > spec.max_work_rows {
            return Err(format!(
                "LogsQL post-filter exceeded max_work_rows={}",
                spec.max_work_rows
            ));
        }
        let level: String = row
            .get(1)
            .map_err(|error| format!("read post-filtered log level: {error}"))?;
        let message: String = row
            .get(2)
            .map_err(|error| format!("read post-filtered log message: {error}"))?;
        let metadata_json: String = row
            .get(3)
            .map_err(|error| format!("read post-filtered log metadata JSON: {error}"))?;
        let timestamp: i64 = row
            .get(0)
            .map_err(|error| format!("read post-filtered log timestamp: {error}"))?;
        if api_postfilters_match(timestamp, &message, &level, &metadata_json, spec, cancelled)? {
            if matched < spec.offset {
                matched += 1;
                continue;
            }
            output.push(QueryRow {
                ts: timestamp,
                level,
                message,
                metadata_json,
            });
            if output.len() == limit {
                return Ok(output);
            }
        }
    }
    Ok(output)
}

fn query_pipeline_rows(
    conn: &Connection,
    spec: &QuerySpec,
    cancelled: &AtomicBool,
) -> Result<Vec<QueryRow>, String> {
    let mut scan = spec.clone();
    scan.offset = 0;
    scan.limit = spec
        .max_work_rows
        .checked_add(1)
        .ok_or_else(|| "LogsQL max_work_rows overflows internal sentinel limit".to_string())?;
    let rows = query_rows(conn, &scan, cancelled)?;
    if rows.len() > spec.max_work_rows {
        return Err(format!(
            "LogsQL pipeline exceeded max_work_rows={}",
            spec.max_work_rows
        ));
    }
    Ok(rows)
}

fn query_count(conn: &Connection, spec: &QuerySpec, cancelled: &AtomicBool) -> Result<i64, String> {
    if has_api_postfilter(spec) {
        return query_count_with_postfilters(conn, spec, cancelled);
    }
    let mut filter = BTreeMap::new();
    if let Some(level) = &spec.level {
        filter.insert("level", level);
    }
    if let Some(service) = &spec.service {
        filter.insert("service", service);
    }
    for (key, value) in &spec.metadata_eq {
        filter.insert(key, value);
    }
    let filter_json = if filter.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&filter)
                .map_err(|error| format!("encode native count filter: {error}"))?,
        )
    };
    conn.query_row(
        "SELECT n FROM timeless_log_count('logs', ?1, ?2, ?3, ?4, ?5)",
        params![
            filter_json,
            spec.message.as_deref(),
            spec.ts_min.unwrap_or(i64::MIN),
            spec.ts_max.unwrap_or(i64::MAX),
            i64::try_from(spec.max_work_rows)
                .map_err(|_| "max_work_rows exceeds SQLite INTEGER range".to_string())?
        ],
        |row| row.get(0),
    )
    .map_err(|e| format!("count logs: {e}"))
}

fn query_count_with_postfilters(
    conn: &Connection,
    spec: &QuerySpec,
    cancelled: &AtomicBool,
) -> Result<i64, String> {
    let (where_sql, mut values) = query_parts(spec)?;
    let sql =
        format!("SELECT ts, level, message, metadata FROM logs{where_sql} ORDER BY ts ASC LIMIT ?");
    values.push(SqlValue::Integer(
        i64::try_from(spec.max_work_rows.saturating_add(1)).unwrap_or(i64::MAX),
    ));
    let mut statement = conn
        .prepare(&sql)
        .map_err(|error| format!("prepare LogsQL post-filter count: {error}"))?;
    let mut rows = statement
        .query(params_from_iter(values))
        .map_err(|error| format!("query LogsQL post-filter count: {error}"))?;
    let mut considered = 0usize;
    let mut total = 0i64;
    loop {
        ensure_query_active(cancelled)?;
        let Some(row) = rows
            .next()
            .map_err(|error| format!("read LogsQL post-filter count row: {error}"))?
        else {
            break;
        };
        considered = considered.saturating_add(1);
        if considered > spec.max_work_rows {
            return Err(format!(
                "LogsQL post-filter count exceeded max_work_rows={}",
                spec.max_work_rows
            ));
        }
        let timestamp: i64 = row
            .get(0)
            .map_err(|error| format!("read post-filter count timestamp: {error}"))?;
        let level: String = row
            .get(1)
            .map_err(|error| format!("read post-filter count level: {error}"))?;
        let message: String = row
            .get(2)
            .map_err(|error| format!("read post-filter count message: {error}"))?;
        let metadata_json: String = row
            .get(3)
            .map_err(|error| format!("read post-filter count metadata: {error}"))?;
        if api_postfilters_match(timestamp, &message, &level, &metadata_json, spec, cancelled)? {
            total = total.saturating_add(1);
        }
    }
    Ok(total)
}

fn has_api_postfilter(spec: &QuerySpec) -> bool {
    spec.message_phrase.is_some() || !spec.metadata_exact.is_empty() || spec.predicate.is_some()
}

fn api_postfilters_match(
    timestamp: i64,
    message: &str,
    level: &str,
    metadata_json: &str,
    spec: &QuerySpec,
    cancelled: &AtomicBool,
) -> Result<bool, String> {
    ensure_query_active(cancelled)?;
    if spec
        .message_phrase
        .as_deref()
        .is_some_and(|phrase| !logsql_phrase_matches(message, phrase))
    {
        return Ok(false);
    }
    let needs_metadata = !spec.metadata_exact.is_empty()
        || spec
            .predicate
            .as_ref()
            .is_some_and(predicate_references_metadata);
    let metadata = needs_metadata
        .then(|| {
            serde_json::from_str(metadata_json)
                .map_err(|error| format!("decode stored typed log metadata: {error}"))
        })
        .transpose()?;
    if !metadata_exact_matches(metadata.as_ref(), &spec.metadata_exact) {
        return Ok(false);
    }
    match spec.predicate.as_ref() {
        None => Ok(true),
        Some(predicate) => log_predicate_matches(
            predicate,
            timestamp,
            message,
            level,
            metadata.as_ref(),
            cancelled,
        ),
    }
}

fn log_predicate_matches(
    predicate: &LogPredicate,
    timestamp: i64,
    message: &str,
    level: &str,
    metadata: Option<&JsonValue>,
    cancelled: &AtomicBool,
) -> Result<bool, String> {
    ensure_query_active(cancelled)?;
    match predicate {
        LogPredicate::True => Ok(true),
        LogPredicate::And(predicates) => {
            for predicate in predicates {
                if !log_predicate_matches(
                    predicate, timestamp, message, level, metadata, cancelled,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        LogPredicate::Or(predicates) => {
            for predicate in predicates {
                if log_predicate_matches(predicate, timestamp, message, level, metadata, cancelled)?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        LogPredicate::Not(predicate) => Ok(!log_predicate_matches(
            predicate, timestamp, message, level, metadata, cancelled,
        )?),
        LogPredicate::Word {
            field,
            value,
            case_insensitive,
        } => Ok(
            log_field_text(field, message, level, metadata).is_some_and(|text| {
                if *case_insensitive {
                    logsql_word_matches(&text.to_lowercase(), value)
                } else {
                    logsql_word_matches(text, value)
                }
            }),
        ),
        LogPredicate::Phrase {
            field,
            value,
            case_insensitive,
        } => Ok(
            log_field_text(field, message, level, metadata).is_some_and(|text| {
                if *case_insensitive {
                    logsql_phrase_matches(&text.to_lowercase(), value)
                } else {
                    logsql_phrase_matches(text, value)
                }
            }),
        ),
        LogPredicate::Prefix {
            field,
            value,
            phrase,
            case_insensitive,
        } => Ok(
            log_field_text(field, message, level, metadata).is_some_and(|text| {
                if *case_insensitive {
                    logsql_prefix_matches(&text.to_lowercase(), value, *phrase)
                } else {
                    logsql_prefix_matches(text, value, *phrase)
                }
            }),
        ),
        LogPredicate::Substring {
            field,
            value,
            case_insensitive,
        } => Ok(
            log_field_text(field, message, level, metadata).is_some_and(|text| {
                if *case_insensitive {
                    text.to_lowercase().contains(value)
                } else {
                    text.contains(value)
                }
            }),
        ),
        LogPredicate::Exact { field, value } => {
            Ok(log_field_text(field, message, level, metadata).is_some_and(|text| text == value))
        }
        LogPredicate::TextualExact { field, value } => Ok(log_field_projected_matches(
            field,
            message,
            level,
            metadata,
            |text| text == value,
        )),
        LogPredicate::ExactPrefix { field, value } => Ok(log_field_projected_matches(
            field,
            message,
            level,
            metadata,
            |text| text.starts_with(value),
        )),
        LogPredicate::TypedExact { field, value } => {
            Ok(log_field_value(field, message, level, metadata)
                .is_some_and(|actual| actual.equals_json(value)))
        }
        LogPredicate::Empty { field } => Ok(
            log_field_value(field, message, level, metadata).is_none_or(LogFieldValue::is_empty)
        ),
        LogPredicate::AnyValue { field } => Ok(log_field_value(field, message, level, metadata)
            .is_some_and(LogFieldValue::is_nonempty)),
        LogPredicate::Numeric {
            field,
            operator,
            value,
        } => Ok(log_field_value(field, message, level, metadata)
            .and_then(LogFieldValue::number)
            .and_then(|actual| compare_json_numbers(actual, value))
            .is_some_and(|ordering| operator.matches(ordering))),
        LogPredicate::ValueType { field, kind } => {
            Ok(log_field_value(field, message, level, metadata)
                .is_some_and(|value| value.is_type(*kind)))
        }
        LogPredicate::Timestamp { minimum, maximum } => Ok(minimum
            .is_none_or(|minimum| timestamp >= minimum)
            && maximum.is_none_or(|maximum| timestamp <= maximum)),
        LogPredicate::Regex { field, regex } => {
            let matched = log_field_text(field, message, level, metadata)
                .is_some_and(|text| regex.is_match(text));
            ensure_query_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::PatternMatch { field, matcher } => {
            let matched = log_field_pattern_matches(field, message, level, metadata, matcher);
            ensure_query_active(cancelled)?;
            Ok(matched)
        }
    }
}

fn predicate_references_metadata(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::True | LogPredicate::Timestamp { .. } => false,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            predicates.iter().any(predicate_references_metadata)
        }
        LogPredicate::Not(predicate) => predicate_references_metadata(predicate),
        LogPredicate::Word { field, .. }
        | LogPredicate::Phrase { field, .. }
        | LogPredicate::Prefix { field, .. }
        | LogPredicate::Substring { field, .. }
        | LogPredicate::Exact { field, .. }
        | LogPredicate::TextualExact { field, .. }
        | LogPredicate::ExactPrefix { field, .. }
        | LogPredicate::TypedExact { field, .. }
        | LogPredicate::Empty { field }
        | LogPredicate::AnyValue { field }
        | LogPredicate::Numeric { field, .. }
        | LogPredicate::ValueType { field, .. }
        | LogPredicate::Regex { field, .. }
        | LogPredicate::PatternMatch { field, .. } => matches!(field, LogField::Metadata(_)),
    }
}

#[derive(Clone, Copy)]
enum LogFieldValue<'a> {
    Text(&'a str),
    Json(&'a JsonValue),
}

impl<'a> LogFieldValue<'a> {
    fn is_empty(self) -> bool {
        match self {
            Self::Text(value) => value.is_empty(),
            Self::Json(JsonValue::Null) => true,
            Self::Json(JsonValue::String(value)) => value.is_empty(),
            Self::Json(_) => false,
        }
    }

    fn is_nonempty(self) -> bool {
        !self.is_empty()
    }

    fn number(self) -> Option<&'a serde_json::Number> {
        match self {
            Self::Json(JsonValue::Number(value)) => Some(value),
            Self::Text(_) | Self::Json(_) => None,
        }
    }

    fn equals_json(self, expected: &JsonValue) -> bool {
        match self {
            Self::Text(actual) => expected.as_str().is_some_and(|expected| actual == expected),
            Self::Json(actual) => json_values_equal(actual, expected),
        }
    }

    fn is_type(self, kind: ValueTypeKind) -> bool {
        match (self, kind) {
            (Self::Text(_), ValueTypeKind::String)
            | (Self::Json(JsonValue::String(_)), ValueTypeKind::String)
            | (Self::Json(JsonValue::Bool(_)), ValueTypeKind::Bool)
            | (Self::Json(JsonValue::Null), ValueTypeKind::Null)
            | (Self::Json(JsonValue::Array(_)), ValueTypeKind::Array)
            | (Self::Json(JsonValue::Object(_)), ValueTypeKind::Object)
            | (Self::Json(JsonValue::Number(_)), ValueTypeKind::Number) => true,
            (Self::Json(JsonValue::Number(value)), ValueTypeKind::Uint64) => {
                value.as_u64().is_some()
            }
            (Self::Json(JsonValue::Number(value)), ValueTypeKind::Int64) => {
                value.as_i64().is_some() && value.as_u64().is_none()
            }
            (Self::Json(JsonValue::Number(value)), ValueTypeKind::Float64) => {
                value.as_i64().is_none() && value.as_u64().is_none() && value.as_f64().is_some()
            }
            _ => false,
        }
    }
}

fn compare_json_numbers(
    left: &serde_json::Number,
    right: &serde_json::Number,
) -> Option<CmpOrdering> {
    match (exact_integer(left), exact_integer(right)) {
        (Some(left), Some(right)) => Some(left.cmp(&right)),
        (Some(left), None) => compare_i128_to_f64(left, right.as_f64()?),
        (None, Some(right)) => compare_i128_to_f64(right, left.as_f64()?).map(CmpOrdering::reverse),
        (None, None) => left.as_f64()?.partial_cmp(&right.as_f64()?),
    }
}

fn exact_integer(value: &serde_json::Number) -> Option<i128> {
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
}

fn compare_i128_to_f64(integer: i128, float: f64) -> Option<CmpOrdering> {
    if !float.is_finite() {
        return None;
    }
    if integer < 0 {
        if float >= 0.0 {
            return Some(CmpOrdering::Less);
        }
        return compare_u128_to_positive_f64(integer.unsigned_abs(), -float)
            .map(CmpOrdering::reverse);
    }
    if float < 0.0 {
        return Some(CmpOrdering::Greater);
    }
    compare_u128_to_positive_f64(integer as u128, float)
}

fn compare_u128_to_positive_f64(integer: u128, float: f64) -> Option<CmpOrdering> {
    debug_assert!(float.is_finite() && float >= 0.0);
    if float == 0.0 {
        return Some(integer.cmp(&0));
    }
    let bits = float.to_bits();
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1u64 << 52) - 1);
    let (significand, shift) = if exponent_bits == 0 {
        (u128::from(fraction), -1074)
    } else {
        (
            u128::from((1u64 << 52) | fraction),
            exponent_bits - 1023 - 52,
        )
    };
    if shift >= 0 {
        let shift = u32::try_from(shift).ok()?;
        let Some(float_integer) = significand.checked_shl(shift) else {
            return Some(CmpOrdering::Less);
        };
        return Some(integer.cmp(&float_integer));
    }
    let right_shift = u32::try_from(-shift).ok()?;
    if right_shift >= 128 {
        return Some(if integer == 0 {
            CmpOrdering::Less
        } else {
            CmpOrdering::Greater
        });
    }
    let whole = significand >> right_shift;
    match integer.cmp(&whole) {
        CmpOrdering::Equal => {
            let mask = (1u128 << right_shift) - 1;
            Some(if significand & mask == 0 {
                CmpOrdering::Equal
            } else {
                CmpOrdering::Less
            })
        }
        ordering => Some(ordering),
    }
}

fn log_field_value<'a>(
    field: &LogField,
    message: &'a str,
    level: &'a str,
    metadata: Option<&'a JsonValue>,
) -> Option<LogFieldValue<'a>> {
    match field {
        LogField::Message => Some(LogFieldValue::Text(message)),
        LogField::Level => Some(LogFieldValue::Text(level)),
        LogField::Metadata(path) => Some(LogFieldValue::Json(metadata_path(metadata?, path)?)),
    }
}

fn ensure_query_active(cancelled: &AtomicBool) -> Result<(), String> {
    if cancelled.load(Ordering::Acquire) {
        Err("logs query cancelled".into())
    } else {
        Ok(())
    }
}

fn log_field_text<'a>(
    field: &LogField,
    message: &'a str,
    level: &'a str,
    metadata: Option<&'a JsonValue>,
) -> Option<&'a str> {
    match field {
        LogField::Message => Some(message),
        LogField::Level => Some(level),
        LogField::Metadata(path) => metadata_path(metadata?, path)?.as_str(),
    }
}

fn log_field_pattern_matches(
    field: &LogField,
    message: &str,
    level: &str,
    metadata: Option<&JsonValue>,
    matcher: &PatternMatcher,
) -> bool {
    log_field_projected_matches(field, message, level, metadata, |text| {
        matcher.matches(text)
    })
}

fn log_field_projected_matches(
    field: &LogField,
    message: &str,
    level: &str,
    metadata: Option<&JsonValue>,
    predicate: impl FnOnce(&str) -> bool,
) -> bool {
    match field {
        LogField::Message => predicate(message),
        LogField::Level => predicate(level),
        LogField::Metadata(path) => match metadata.and_then(|value| metadata_path(value, path)) {
            None | Some(JsonValue::Null) => predicate(""),
            Some(JsonValue::String(value)) => predicate(value),
            Some(value) => predicate(&value.to_string()),
        },
    }
}

fn logsql_word_matches(value: &str, expected: &str) -> bool {
    value
        .split(|character: char| !logsql_word_char(character))
        .any(|word| word == expected)
}

fn logsql_prefix_matches(value: &str, prefix: &str, phrase: bool) -> bool {
    if prefix.is_empty() {
        return false;
    }
    if !phrase {
        return value
            .split(|character: char| !logsql_word_char(character))
            .any(|word| word.starts_with(prefix));
    }
    let require_start_boundary = prefix.chars().next().is_some_and(logsql_word_char);
    value.match_indices(prefix).any(|(start, _)| {
        !require_start_boundary
            || start == 0
            || value[..start]
                .chars()
                .next_back()
                .is_none_or(|character| !logsql_word_char(character))
    })
}

/// VictoriaLogs phrases preserve every byte between the quotes, while word
/// characters at either edge must begin/end a LogsQL word. Punctuation at an
/// edge has no additional boundary requirement.
fn logsql_phrase_matches(message: &str, phrase: &str) -> bool {
    if phrase.is_empty() {
        return false;
    }
    let require_start_boundary = phrase.chars().next().is_some_and(logsql_word_char);
    let require_end_boundary = phrase.chars().next_back().is_some_and(logsql_word_char);

    message.match_indices(phrase).any(|(start, matched)| {
        let start_ok = !require_start_boundary
            || start == 0
            || message[..start]
                .chars()
                .next_back()
                .is_none_or(|character| !logsql_word_char(character));
        let end = start + matched.len();
        let end_ok = !require_end_boundary
            || end == message.len()
            || message[end..]
                .chars()
                .next()
                .is_none_or(|character| !logsql_word_char(character));
        start_ok && end_ok
    })
}

fn logsql_word_char(character: char) -> bool {
    character == '_' || character.is_alphanumeric()
}

fn metadata_exact_matches(metadata: Option<&JsonValue>, predicates: &[MetadataExact]) -> bool {
    if predicates.is_empty() {
        return true;
    }
    let Some(metadata) = metadata else {
        return false;
    };
    for predicate in predicates {
        let Some(actual) = metadata_path(metadata, &predicate.path) else {
            return false;
        };
        if !json_values_equal(actual, &predicate.expected) {
            return false;
        }
    }
    true
}

fn metadata_path<'a>(metadata: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
    let mut value = metadata;
    for segment in path {
        value = match value {
            JsonValue::Object(object) => object.get(segment)?,
            JsonValue::Array(array) => array.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(value)
}

fn json_values_equal(left: &JsonValue, right: &JsonValue) -> bool {
    match (left, right) {
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            match (left.as_i64(), right.as_i64()) {
                (Some(left), Some(right)) => left == right,
                _ => match (left.as_u64(), right.as_u64()) {
                    (Some(left), Some(right)) => left == right,
                    _ => left.as_f64() == right.as_f64(),
                },
            }
        }
        _ => left == right,
    }
}

fn query_field_values(
    conn: &Connection,
    spec: &QuerySpec,
    key: &str,
    limit: usize,
) -> Result<Vec<String>, String> {
    if !matches!(key, "service" | "host" | "path" | "status") {
        return Err(format!("unsupported indexed log field {key:?}"));
    }
    let mut filter = BTreeMap::new();
    if let Some(level) = &spec.level {
        filter.insert("level", level);
    }
    if let Some(service) = &spec.service {
        filter.insert("service", service);
    }
    for (filter_key, value) in &spec.metadata_eq {
        filter.insert(filter_key, value);
    }
    let filter_json = if filter.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&filter)
                .map_err(|error| format!("encode field-values filter: {error}"))?,
        )
    };
    let mut statement = conn
        .prepare("SELECT value FROM timeless_log_values('logs', ?1, ?2, ?3, ?4, ?5, ?6, ?7)")
        .map_err(|error| format!("prepare log field-values query: {error}"))?;
    let values = statement
        .query_map(
            params![
                key,
                filter_json,
                spec.message.as_deref(),
                spec.ts_min,
                spec.ts_max,
                i64::try_from(limit).unwrap_or(i64::MAX),
                i64::try_from(spec.max_work_rows)
                    .map_err(|_| "max_work_rows exceeds SQLite INTEGER range".to_string())?
            ],
            |row| row.get(0),
        )
        .map_err(|error| format!("query log field values: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read log field value: {error}"))?;
    Ok(values)
}

fn storage_stats(conn: &Connection) -> Result<StorageStats, String> {
    let engine = stat_values(conn)?;
    let stat = |key: &str| engine.get(key).copied().unwrap_or(0);
    let buffered = stat("buffered_entries");
    let blocks = stat("blocks");
    let disk_entries = stat("disk_entries");
    let bytes = stat("bytes_on_disk");
    let raw_blocks = stat("raw_blocks");
    let raw_bytes = stat("raw_bytes");
    let oldest = engine.get("ts_min").copied();
    let newest = engine.get("ts_max").copied();
    let (page_count, page_size, freelist_pages): (i64, i64, i64) = conn
        .query_row(
            "SELECT (SELECT page_count FROM pragma_page_count),
                    (SELECT page_size FROM pragma_page_size),
                    (SELECT freelist_count FROM pragma_freelist_count)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|e| format!("read database size: {e}"))?;
    let page_bytes = page_count.saturating_mul(page_size);
    let index_bytes = stat("index_bytes");
    let term_postings = stat("terms");
    Ok(StorageStats {
        total_blocks: blocks,
        total_entries: disk_entries + buffered,
        total_bytes: bytes,
        disk_size: page_bytes,
        index_size: index_bytes,
        database_file_bytes: 0,
        database_wal_bytes: 0,
        database_shm_bytes: 0,
        physical_database_bytes: 0,
        sqlite_page_bytes: page_bytes,
        freelist_pages,
        freelist_bytes: freelist_pages.saturating_mul(page_size),
        writer_connections: 0,
        reader_connections: 0,
        command_queue_capacity_batches: 0,
        term_postings,
        oldest_timestamp: oldest,
        newest_timestamp: newest,
        raw_blocks,
        raw_bytes,
        compressed_blocks: blocks.saturating_sub(raw_blocks),
        compressed_bytes: bytes.saturating_sub(raw_bytes),
        buffered_entries: buffered,
        queued_batches: 0,
        queued_entries: 0,
        oldest_queued_ms: 0,
        admitted_batches: 0,
        admitted_entries: 0,
        completed_batches: 0,
        completed_entries: 0,
        api_parse_ns: 0,
        api_batch_encode_ns: 0,
        api_sqlite_insert_ns: 0,
        api_queue_wait_ns: 0,
        api_queue_wait_max_ns: 0,
        api_query_count: 0,
        api_query_ns: 0,
        api_query_in_flight: 0,
        api_query_cancelled: 0,
        api_query_errors: 0,
        api_query_result_rows: 0,
        api_query_response_bytes: 0,
        ingest_batch_count: stat("ingest_batch_count"),
        ingest_batch_entries: stat("ingest_batch_entries"),
        ingest_wire_decode_ns: stat("ingest_wire_decode_ns"),
        ingest_normalize_ns: stat("ingest_normalize_ns"),
        ingest_buffer_append_ns: stat("ingest_buffer_append_ns"),
        flush_count: stat("flush_count"),
        flush_entries: stat("flush_entries"),
        flush_total_ns: stat("flush_total_ns"),
        flush_partition_ns: stat("flush_partition_ns"),
        flush_encode_terms_ns: stat("flush_encode_terms_ns"),
        flush_store_ns: stat("flush_store_ns"),
        query_count: stat("query_count"),
        query_total_ns: stat("query_total_ns"),
        query_snapshot_ns: stat("query_snapshot_ns"),
        query_materialize_ns: stat("query_materialize_ns"),
        query_snapshot_payload_bytes: stat("query_snapshot_payload_bytes"),
        query_snapshot_payload_max_bytes: stat("query_snapshot_payload_max_bytes"),
        query_snapshot_buffered_entries: stat("query_snapshot_buffered_entries"),
        query_stable_location_snapshots: stat("query_stable_location_snapshots"),
        query_payload_bytes_read: stat("query_payload_bytes_read"),
        query_candidate_blocks: stat("query_candidate_blocks"),
        query_decoded_entries: stat("query_decoded_entries"),
        query_matched_entries: stat("query_matched_entries"),
        query_returned_entries: stat("query_returned_entries"),
        query_bounded_count: stat("query_bounded_count"),
        query_bounded_requested_entries: stat("query_bounded_requested_entries"),
        query_bounded_max_entries: stat("query_bounded_max_entries"),
        query_blocks_skipped_by_bound: stat("query_blocks_skipped_by_bound"),
        native_count_count: stat("native_count_count"),
        native_count_total_ns: stat("native_count_total_ns"),
        native_count_snapshot_ns: stat("native_count_snapshot_ns"),
        native_count_payload_bytes_read: stat("native_count_payload_bytes_read"),
        native_count_metadata_blocks: stat("native_count_metadata_blocks"),
        native_count_metadata_entries: stat("native_count_metadata_entries"),
        native_count_decoded_blocks: stat("native_count_decoded_blocks"),
        native_count_decoded_entries: stat("native_count_decoded_entries"),
        optimize_count: stat("optimize_count"),
        optimize_total_ns: stat("optimize_total_ns"),
        optimize_blocks_removed: stat("optimize_blocks_removed"),
        optimize_blocks_written: stat("optimize_blocks_written"),
        optimize_budgeted_count: stat("optimize_budgeted_count"),
        optimize_budget_entries: stat("optimize_budget_entries"),
        optimize_budget_limited_count: stat("optimize_budget_limited_count"),
        optimize_raw_groups: stat("optimize_raw_groups"),
        optimize_raw_blocks: stat("optimize_raw_blocks"),
        optimize_raw_entries: stat("optimize_raw_entries"),
        optimize_raw_input_bytes: stat("optimize_raw_input_bytes"),
        optimize_raw_output_bytes: stat("optimize_raw_output_bytes"),
        optimize_raw_total_ns: stat("optimize_raw_total_ns"),
        optimize_merge_groups: stat("optimize_merge_groups"),
        optimize_merge_blocks: stat("optimize_merge_blocks"),
        optimize_merge_entries: stat("optimize_merge_entries"),
        optimize_merge_input_bytes: stat("optimize_merge_input_bytes"),
        optimize_merge_output_bytes: stat("optimize_merge_output_bytes"),
        optimize_merge_total_ns: stat("optimize_merge_total_ns"),
        optimize_pending_raw_blocks: stat("optimize_pending_raw_blocks"),
        optimize_pending_raw_entries: stat("optimize_pending_raw_entries"),
        optimize_merge_ready_groups: stat("optimize_merge_ready_groups"),
        optimize_merge_ready_blocks: stat("optimize_merge_ready_blocks"),
        optimize_merge_ready_entries: stat("optimize_merge_ready_entries"),
        optimize_merge_deferred_blocks: stat("optimize_merge_deferred_blocks"),
        optimize_merge_deferred_entries: stat("optimize_merge_deferred_entries"),
        read_permit_count: stat("read_permit_count"),
        read_permit_hold_ns: stat("read_permit_hold_ns"),
        read_conflicts: stat("read_conflicts"),
        read_barge_rejections: stat("read_barge_rejections"),
        waiting_writers: stat("waiting_writers"),
        writer_wait_count: stat("writer_wait_count"),
        writer_wait_ns: stat("writer_wait_ns"),
        writer_timeouts: stat("writer_timeouts"),
        checkpoint_count: 0,
        checkpoint_total_ns: 0,
        checkpoint_errors: 0,
        backup_count: 0,
        backup_total_ns: 0,
        backup_errors: 0,
        last_error: None,
    })
}

fn record_checkpoint<T>(
    profile: &StdMutex<QueueProfile>,
    duration: Duration,
    result: &Result<T, String>,
) {
    let mut profile = profile_lock(profile);
    profile.checkpoint_count = profile.checkpoint_count.saturating_add(1);
    profile.checkpoint_total_ns = profile
        .checkpoint_total_ns
        .saturating_add(duration_ns(duration));
    if let Err(error) = result {
        profile.checkpoint_errors = profile.checkpoint_errors.saturating_add(1);
        profile.last_error = Some(error.clone());
    }
}

fn record_backup(
    profile: &StdMutex<QueueProfile>,
    duration: Duration,
    result: &Result<BackupReport, String>,
) {
    let mut profile = profile_lock(profile);
    profile.backup_count = profile.backup_count.saturating_add(1);
    profile.backup_total_ns = profile
        .backup_total_ns
        .saturating_add(duration_ns(duration));
    if let Err(error) = result {
        profile.backup_errors = profile.backup_errors.saturating_add(1);
        profile.last_error = Some(error.clone());
    }
}

fn database_file_sizes(database_path: &Path) -> (u64, u64, u64) {
    (
        file_size(database_path),
        file_size(&suffix_path(database_path, "-wal")),
        file_size(&suffix_path(database_path, "-shm")),
    )
}

fn suffix_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path)
        .map(|value| value.len())
        .unwrap_or(0)
}

fn stat_values(conn: &Connection) -> Result<HashMap<String, i64>, String> {
    let mut stmt = conn
        .prepare("SELECT key, CAST(value AS INTEGER) FROM timeless_stats('logs')")
        .map_err(|e| format!("prepare timeless_stats: {e}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .map_err(|e| format!("read timeless_stats: {e}"))?;
    let mut values = HashMap::new();
    for row in rows {
        let (key, value) = row.map_err(|e| format!("collect timeless_stats: {e}"))?;
        if let Some(value) = value {
            values.insert(key, value);
        }
    }
    Ok(values)
}

fn stat_text(conn: &Connection, key: &str) -> Result<Option<String>, String> {
    conn.query_row(
        "SELECT CAST(value AS TEXT) FROM timeless_stats('logs') WHERE key = ?1",
        [key],
        |row| row.get(0),
    )
    .optional()
    .map_err(|error| format!("read public log stat {key:?}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn owner_lease_is_exclusive_and_recoverable() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("logs.db");
        let first = acquire_database_lease(&database, "logs").unwrap();
        let error = acquire_database_lease(&database, "logs").unwrap_err();
        assert!(error.contains("already owned"), "{error}");
        FileExt::unlock(&first).unwrap();
        acquire_database_lease(&database, "logs").unwrap();
    }

    #[test]
    fn cancellation_accounting_transfers_only_after_reader_queue_admission() {
        let profile = Arc::new(StdMutex::new(QueueProfile {
            query_in_flight: 1,
            ..QueueProfile::default()
        }));
        let before_queue = Arc::new(AtomicBool::new(false));
        drop(ReadCancellation {
            cancelled: Arc::clone(&before_queue),
            profile: Arc::clone(&profile),
            reader_owned: false,
            armed: true,
        });
        assert!(before_queue.load(Ordering::Acquire));
        assert_eq!(profile_lock(&profile).query_in_flight, 0);
        assert_eq!(profile_lock(&profile).query_cancelled, 1);

        {
            let mut values = profile_lock(&profile);
            values.query_in_flight = 1;
        }
        let after_queue = Arc::new(AtomicBool::new(false));
        let mut cancellation = ReadCancellation {
            cancelled: Arc::clone(&after_queue),
            profile: Arc::clone(&profile),
            reader_owned: false,
            armed: true,
        };
        cancellation.handoff_to_reader();
        drop(cancellation);
        assert!(after_queue.load(Ordering::Acquire));
        assert_eq!(profile_lock(&profile).query_in_flight, 1);
        assert_eq!(profile_lock(&profile).query_cancelled, 2);

        record_read_return(&profile);
        assert_eq!(profile_lock(&profile).query_in_flight, 0);
    }

    #[test]
    fn decoded_log_predicates_observe_query_cancellation() {
        let cancelled = AtomicBool::new(true);
        for predicate in [
            LogPredicate::Regex {
                field: LogField::Message,
                regex: regex::Regex::new("request").unwrap(),
            },
            LogPredicate::PatternMatch {
                field: LogField::Message,
                matcher: PatternMatcher::new("request <N>", PatternMatchMode::Any),
            },
        ] {
            assert_eq!(
                log_predicate_matches(&predicate, 0, "request 42", "info", None, &cancelled)
                    .unwrap_err(),
                "logs query cancelled"
            );
        }
    }

    #[test]
    fn pattern_matcher_pins_victorialogs_anchors_placeholders_and_progress() {
        let matcher = |pattern, mode| PatternMatcher::new(pattern, mode);
        let any = matcher("x <N> y", PatternMatchMode::Any);
        assert!(any.matches("before x 10 y after"));
        assert!(any.matches("a x nope a x 12 y"));
        assert!(!any.matches("before x nope y after"));

        let full = matcher("x <N> y", PatternMatchMode::Full);
        assert!(full.matches("x 10 y"));
        assert!(!full.matches("x 10 y after"));
        let prefix = matcher("x <N> y", PatternMatchMode::Prefix);
        assert!(prefix.matches("x 10 y after"));
        assert!(!prefix.matches("before x 10 y"));
        let suffix = matcher("x <N> y", PatternMatchMode::Suffix);
        assert!(suffix.matches("before x 10 y"));
        assert!(!suffix.matches("x 10 y after"));

        for matching in ["job-123", "job-12abcdEF"] {
            assert!(matcher("job-<N>", PatternMatchMode::Full).matches(matching));
        }
        assert!(!matcher("job-<N>", PatternMatchMode::Full).matches("job-be"));
        assert!(matcher("id=<UUID>", PatternMatchMode::Full)
            .matches("id=2edfed59-3e98-4073-bbb2-28d321ca71a7"));
        assert!(matcher("ip=<IP4>", PatternMatchMode::Full).matches("ip=123.45.67.89"));
        assert!(matcher("time=<TIME>", PatternMatchMode::Full).matches("time=10:20:30,123"));
        assert!(matcher("date=<DATE>", PatternMatchMode::Full).matches("date=2025/10/20"));
        assert!(matcher("at=<DATETIME>", PatternMatchMode::Full)
            .matches("at=2025-10-20T08:09:11.123+01:30"));
        assert!(matcher("word=<W>", PatternMatchMode::Full).matches("word=привет_45"));
        assert!(matcher("word=<W>", PatternMatchMode::Full).matches("word=\"hello world\""));
        assert!(matcher("word=<W>", PatternMatchMode::Full).matches("word='hello\\' world'"));
        assert!(matcher("word=<W>", PatternMatchMode::Full).matches("word=१२"));
        assert!(!matcher("word=<W>", PatternMatchMode::Full).matches("word=²"));
        assert!(!matcher("word=<W>", PatternMatchMode::Full).matches("word=Ⅳ"));
        assert!(!matcher("word=<W>", PatternMatchMode::Full).matches("word=e\u{301}"));
        assert!(matcher("value=<BOGUS>", PatternMatchMode::Full).matches("value=<BOGUS>"));

        assert!(matcher("", PatternMatchMode::Any).matches("anything"));
        assert!(matcher("", PatternMatchMode::Prefix).matches("anything"));
        assert!(matcher("", PatternMatchMode::Suffix).matches("anything"));
        assert!(matcher("", PatternMatchMode::Full).matches(""));
        assert!(!matcher("", PatternMatchMode::Full).matches("anything"));
    }

    #[test]
    fn pattern_predicate_textually_matches_retained_types_without_changing_them() {
        let matcher = PatternMatcher::new("<N>", PatternMatchMode::Full);
        let predicate = LogPredicate::PatternMatch {
            field: LogField::Metadata(vec!["n".into()]),
            matcher,
        };
        let metadata = serde_json::json!({"n": 42, "nested": {"value": true}});
        let cancelled = AtomicBool::new(false);
        assert!(log_predicate_matches(
            &predicate,
            0,
            "message",
            "info",
            Some(&metadata),
            &cancelled,
        )
        .unwrap());
        assert_eq!(metadata["n"], 42);

        let missing = LogPredicate::PatternMatch {
            field: LogField::Metadata(vec!["missing".into()]),
            matcher: PatternMatcher::new("", PatternMatchMode::Full),
        };
        assert!(
            log_predicate_matches(&missing, 0, "message", "info", Some(&metadata), &cancelled,)
                .unwrap()
        );
    }

    #[test]
    fn exact_prefix_textually_matches_rich_values_missing_and_null() {
        let metadata = serde_json::json!({
            "n": 42,
            "flag": true,
            "list": [1, "x"],
            "nested": {"value": true},
            "nullish": null,
            "unicode": "१२"
        });
        let cancelled = AtomicBool::new(false);
        for (path, prefix) in [
            ("n", "4"),
            ("flag", "tr"),
            ("list", "[1,"),
            ("nested", r#"{"value":"#),
            ("unicode", "१"),
            ("nullish", ""),
            ("missing", ""),
        ] {
            let predicate = LogPredicate::ExactPrefix {
                field: LogField::Metadata(vec![path.into()]),
                value: prefix.into(),
            };
            assert!(
                log_predicate_matches(
                    &predicate,
                    0,
                    "message",
                    "info",
                    Some(&metadata),
                    &cancelled,
                )
                .unwrap(),
                "{path}: {prefix:?}"
            );
        }
        assert_eq!(metadata["n"], 42);
        assert_eq!(metadata["flag"], true);
        assert!(metadata["list"].is_array());
        assert!(metadata["nested"].is_object());
        assert!(metadata["nullish"].is_null());
    }

    #[test]
    fn typed_numeric_comparison_keeps_integer_bits_and_fractional_ordering() {
        let number = |value: &str| {
            serde_json::from_str::<JsonValue>(value)
                .unwrap()
                .as_number()
                .unwrap()
                .clone()
        };
        assert_eq!(
            compare_json_numbers(&number("9007199254740993"), &number("9007199254740992")),
            Some(CmpOrdering::Greater)
        );
        assert_eq!(
            compare_json_numbers(&number("2"), &number("2.5")),
            Some(CmpOrdering::Less)
        );
        assert_eq!(
            compare_json_numbers(&number("-3"), &number("-2.5")),
            Some(CmpOrdering::Less)
        );
        assert_eq!(
            compare_json_numbers(&number("10"), &number("10.0")),
            Some(CmpOrdering::Equal)
        );
    }

    #[test]
    fn batch_encoding_uses_rich_v1_header_and_exact_severity() {
        let entries = vec![LogEntry {
            ts: 42,
            level: 3,
            severity: "critical".into(),
            message: "boom".into(),
            metadata_json: "{\"service\":\"api\"}".into(),
        }];
        let blob = encode_batch(&entries).unwrap();
        assert_eq!(&blob[..4], &[2, 0, 0, 0]);
        assert_eq!(&blob[4..8], &1u32.to_le_bytes());
        assert_eq!(&blob[8..16], &42i64.to_le_bytes());
        assert_eq!(&blob[16..20], &8u32.to_le_bytes());
        assert_eq!(&blob[20..28], b"critical");
    }

    #[test]
    fn pending_writer_conflicts_are_retried_instead_of_becoming_http_errors() {
        let attempts = Cell::new(0);
        let value = retry_read(|| {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                Err("table \"logs\" read is blocked by a pending writer transaction — retry, as for SQLITE_BUSY".to_string())
            } else {
                Ok(42)
            }
        })
        .unwrap();

        assert_eq!(value, 42);
        assert_eq!(attempts.get(), 2);
    }

    #[test]
    fn optimize_budget_tracks_source_bytes_and_one_complete_group() {
        assert_eq!(optimize_entry_budget(0, 0, 0), 0);
        assert_eq!(optimize_entry_budget(4_000, 4_000, 1024), 4_000);
        assert_eq!(optimize_entry_budget(100_000, 100_000, 64 << 20), 50_000);
        assert_eq!(
            optimize_entry_budget(100_000, 100_000, 1024 << 20),
            OPTIMIZE_TARGET_ENTRIES
        );
        assert_eq!(
            optimize_entry_budget(100_000, 0, 0),
            OPTIMIZE_TARGET_ENTRIES
        );
    }

    #[test]
    fn optimize_progress_accepts_raw_to_merge_phase_expansion() {
        let raw = OptimizeBacklog {
            pending_raw_blocks: 1,
            pending_raw_entries: 1_280,
            merge_ready_groups: 0,
            merge_ready_blocks: 0,
            merge_ready_entries: 0,
        };
        let merge = OptimizeBacklog {
            pending_raw_blocks: 0,
            pending_raw_entries: 0,
            merge_ready_groups: 1,
            merge_ready_blocks: 4,
            merge_ready_entries: 4_698,
        };

        assert_ne!(raw, merge);
        assert!(merge.actionable_entries() > raw.actionable_entries());
        assert!(optimize_made_progress(raw, merge));
        assert!(!optimize_made_progress(raw, raw));
    }

    #[test]
    fn bundled_sqlite_exposes_page_accounting_for_compatible_index_bytes() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE postings(term TEXT PRIMARY KEY)", [])
            .unwrap();
        conn.execute("INSERT INTO postings VALUES ('level:error')", [])
            .unwrap();
        let bytes: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(pgsize), 0) FROM dbstat WHERE name = 'postings'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(bytes > 0);
    }

    #[test]
    fn logsql_phrase_preserves_bytes_and_unicode_word_boundaries() {
        for matching in [
            "ssh: login fail",
            "prefix ssh: login fail suffix",
            "(ssh: login fail)!",
            "ssh: login fail—next",
        ] {
            assert!(logsql_phrase_matches(matching, "ssh: login fail"));
        }
        for non_matching in [
            "SSH: login fail",
            "ssh:  login fail",
            "ssh: login failed",
            "xssh: login fail",
            "x_ssh: login fail",
            "éssh: login fail",
        ] {
            assert!(!logsql_phrase_matches(non_matching, "ssh: login fail"));
        }
        assert!(logsql_phrase_matches("xssh: login failed", ": login"));
        assert!(logsql_phrase_matches("тест45 done", "тест45"));
        assert!(!logsql_phrase_matches("xтест45 done", "тест45"));
        assert!(!logsql_phrase_matches("тест45x done", "тест45"));
        assert!(!logsql_phrase_matches("anything", ""));
    }
}
