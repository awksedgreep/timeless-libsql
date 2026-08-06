//! Bounded LogsQL row transforms owned by the Rust logs API.
//!
//! This module deliberately receives only rows returned by the public logs
//! virtual table. It contains no extension syntax and cannot access storage
//! shadow tables.

use std::borrow::Cow;
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::Instant;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Number, Value};

use crate::logsql::{
    logsql_field_comparison, logsql_sort_comparison, parse_ipv4_address, parse_ipv6_address,
    CoalesceSpec, CopySpec, FacetsSpec, FirstSpec, PipelineField, PipelineOp, RenameSpec,
    StatsExpression, StatsKind, TopSpec, UniqSpec,
};
use crate::storage::{day_range_matches, week_range_matches, LogQueryExecutionReport, QueryRow};
use crate::{LogField, LogPredicate, NumericOp, TimestampUnit, ValueTypeKind};

#[derive(Clone, Copy)]
pub(crate) struct PipelineLimits {
    pub max_result_rows: usize,
    pub max_state_items: usize,
    pub max_state_bytes: usize,
}

pub(crate) struct PipelineExecution<'a> {
    pub report: LogQueryExecutionReport,
    pub operations: &'a [PipelineOp],
    pub implicit_result_limit: Option<usize>,
    pub rate_window_seconds: Option<f64>,
    pub timestamp_unit: TimestampUnit,
    pub limits: PipelineLimits,
    pub cancelled: &'a AtomicBool,
    pub query_started: Instant,
}

pub(crate) fn execute_query_rows(
    rows: Vec<QueryRow>,
    mut execution: PipelineExecution<'_>,
) -> Result<Vec<Value>, String> {
    if matches!(execution.operations.first(), Some(PipelineOp::QueryStats)) {
        ensure_active(execution.cancelled)?;
        let rows = vec![Value::Object(query_stats(
            execution.report,
            execution
                .query_started
                .elapsed()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        ))];
        execution.operations = &execution.operations[1..];
        return execute(rows, execution);
    }
    let rows = rows
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            check_periodically(execution.cancelled, index)?;
            response_row(row, execution.timestamp_unit)
        })
        .collect::<Result<Vec<_>, _>>()?;
    execute(rows, execution)
}

pub(crate) fn response_row(row: QueryRow, timestamp_unit: TimestampUnit) -> Result<Value, String> {
    let metadata: Map<String, Value> = serde_json::from_str(&row.metadata_json)
        .map_err(|error| format!("decode stored metadata: {error}"))?;
    let mut object = metadata;
    object.insert(
        "_time".into(),
        Value::String(format_timestamp(row.ts, timestamp_unit)?),
    );
    object.insert("_msg".into(), Value::String(row.message));
    object.insert("level".into(), Value::String(row.level));
    Ok(Value::Object(object))
}

pub(crate) fn format_timestamp(ts: i64, timestamp_unit: TimestampUnit) -> Result<String, String> {
    let (datetime, format) = match timestamp_unit {
        TimestampUnit::Milliseconds => (
            DateTime::<Utc>::from_timestamp_millis(ts),
            SecondsFormat::Millis,
        ),
        TimestampUnit::Microseconds => (
            DateTime::<Utc>::from_timestamp_micros(ts),
            SecondsFormat::Micros,
        ),
    };
    datetime
        .map(|datetime| datetime.to_rfc3339_opts(format, true))
        .ok_or_else(|| format!("timestamp {ts} is outside the RFC3339 range"))
}

pub(crate) fn execute(
    mut rows: Vec<Value>,
    execution: PipelineExecution<'_>,
) -> Result<Vec<Value>, String> {
    for operation in execution.operations {
        ensure_active(execution.cancelled)?;
        rows = match operation {
            PipelineOp::SortTime { descending } => {
                rows.sort_by(|left, right| {
                    let ordering = left
                        .get("_time")
                        .and_then(Value::as_str)
                        .cmp(&right.get("_time").and_then(Value::as_str));
                    if *descending {
                        ordering.reverse()
                    } else {
                        ordering
                    }
                });
                rows
            }
            PipelineOp::Offset(offset) => {
                if *offset >= rows.len() {
                    Vec::new()
                } else {
                    rows.into_iter().skip(*offset).collect()
                }
            }
            PipelineOp::Limit(limit) => {
                rows.truncate(*limit);
                rows
            }
            PipelineOp::FieldValues {
                field,
                filter,
                limit,
            } => field_values(
                &rows,
                field,
                filter.as_deref(),
                *limit,
                execution.limits.max_result_rows,
                execution.cancelled,
            )?,
            PipelineOp::FieldNames {
                filter,
                result_name,
            } => field_names(
                &rows,
                filter.as_deref(),
                result_name,
                execution.limits.max_result_rows,
                execution.cancelled,
            )?,
            PipelineOp::Project(fields) => project(rows, fields, execution.cancelled)?,
            PipelineOp::Delete(fields) => delete_fields(rows, fields, execution.cancelled)?,
            PipelineOp::Filter(predicate) => filter(
                rows,
                predicate,
                execution.timestamp_unit,
                execution.cancelled,
            )?,
            PipelineOp::Stats(expressions) => vec![Value::Object(stats(
                &rows,
                expressions,
                execution.rate_window_seconds,
                execution.limits,
                execution.cancelled,
            )?)],
            PipelineOp::QueryStats => vec![Value::Object(query_stats(
                execution.report,
                execution
                    .query_started
                    .elapsed()
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
            ))],
            PipelineOp::First(spec) => first(rows, spec, execution.limits, execution.cancelled)?,
            PipelineOp::Last(spec) => last(rows, spec, execution.limits, execution.cancelled)?,
            PipelineOp::Top(spec) => top(rows, spec, execution.limits, execution.cancelled)?,
            PipelineOp::Uniq(spec) => uniq(rows, spec, execution.limits, execution.cancelled)?,
            PipelineOp::Facets(spec) => facets(rows, spec, execution.limits, execution.cancelled)?,
            PipelineOp::Coalesce(spec) => {
                coalesce(rows, spec, execution.limits, execution.cancelled)?
            }
            PipelineOp::Copy(spec) => {
                copy_fields(rows, spec, execution.limits, execution.cancelled)?
            }
            PipelineOp::Rename(spec) => {
                rename_fields(rows, spec, execution.limits, execution.cancelled)?
            }
        };
    }
    if let Some(limit) = execution.implicit_result_limit {
        rows.truncate(limit.min(execution.limits.max_result_rows));
    }
    if rows.len() > execution.limits.max_result_rows {
        return Err(format!(
            "LogsQL pipeline result exceeds max_result_rows={}",
            execution.limits.max_result_rows
        ));
    }
    ensure_active(execution.cancelled)?;
    Ok(rows)
}

fn query_stats(report: LogQueryExecutionReport, query_duration_ns: u64) -> Map<String, Value> {
    let mut result = Map::new();
    let mut insert = |name: &str, value: u64| {
        result.insert(name.to_owned(), Value::String(value.to_string()));
    };

    // Timeless codecs read one indivisible encoded payload. There are no
    // separately addressable VictoriaLogs column headers, header indexes,
    // blooms, timestamp streams, or block-header files to attribute here.
    insert("BytesReadColumnsHeaders", 0);
    insert("BytesReadColumnsHeaderIndexes", 0);
    insert("BytesReadBloomFilters", 0);
    insert("BytesReadValues", report.payload_bytes_read);
    insert("BytesReadTimestamps", 0);
    insert("BytesReadBlockHeaders", 0);
    insert("BytesReadTotal", report.payload_bytes_read);
    insert("BlocksProcessed", report.processed_blocks);
    insert("RowsProcessed", report.processed_entries);
    insert("RowsFound", report.matched_entries);
    insert("ValuesRead", report.values_read);
    insert("TimestampsRead", report.timestamps_read);
    // The current block report measures bytes at the encoded-payload seam;
    // it does not reserialize every decoded rich value merely to approximate
    // VictoriaLogs' columnar uncompressed-byte counter.
    insert("BytesProcessedUncompressedValues", 0);
    insert("QueryDurationNsecs", query_duration_ns);
    result
}

enum FirstSortKeys<'a> {
    Explicit(Vec<Vec<Cow<'a, str>>>),
    AllFields(Vec<String>),
}

fn first(
    rows: Vec<Value>,
    spec: &FirstSpec,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    first_last(rows, spec, limits, cancelled, false, "first")
}

fn last(
    rows: Vec<Value>,
    spec: &FirstSpec,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    first_last(rows, spec, limits, cancelled, true, "last")
}

fn first_last(
    rows: Vec<Value>,
    spec: &FirstSpec,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
    reverse: bool,
    operation: &str,
) -> Result<Vec<Value>, String> {
    if rows.len() > limits.max_state_items {
        return Err(format!(
            "LogsQL {operation} exceeds max_work_rows={}",
            limits.max_state_items
        ));
    }
    ensure_active(cancelled)?;
    let mut state_bytes = rows
        .len()
        .checked_mul(size_of::<usize>())
        .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
    ensure_first_state_bytes(state_bytes, limits.max_state_bytes, operation)?;

    let sort_keys = if spec.by_fields.is_empty() {
        let mut keys = Vec::with_capacity(rows.len());
        state_bytes = state_bytes
            .checked_add(
                rows.len()
                    .checked_mul(size_of::<String>())
                    .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?,
            )
            .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
        for (index, row) in rows.iter().enumerate() {
            check_periodically(cancelled, index)?;
            let key = all_fields_sort_key(row, operation)?;
            state_bytes = state_bytes
                .checked_add(key.len())
                .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
            ensure_first_state_bytes(state_bytes, limits.max_state_bytes, operation)?;
            keys.push(key);
        }
        FirstSortKeys::AllFields(keys)
    } else {
        let projection_count = rows
            .len()
            .checked_mul(spec.by_fields.len())
            .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
        state_bytes = state_bytes
            .checked_add(
                rows.len()
                    .checked_mul(size_of::<Vec<Cow<'_, str>>>())
                    .and_then(|bytes| {
                        projection_count
                            .checked_mul(size_of::<Cow<'_, str>>())
                            .and_then(|projections| bytes.checked_add(projections))
                    })
                    .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?,
            )
            .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
        ensure_first_state_bytes(state_bytes, limits.max_state_bytes, operation)?;
        let mut keys = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            check_periodically(cancelled, index)?;
            let mut key = Vec::with_capacity(spec.by_fields.len());
            for field in &spec.by_fields {
                let value = match &field.field {
                    PipelineField::Exact { path, .. } => field_value(row, path),
                    PipelineField::Prefix { .. } | PipelineField::All => {
                        return Err(format!("LogsQL {operation} sort field is not exact"))
                    }
                };
                let value = projected_text(value);
                if let Cow::Owned(value) = &value {
                    state_bytes = state_bytes
                        .checked_add(value.len())
                        .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
                    ensure_first_state_bytes(state_bytes, limits.max_state_bytes, operation)?;
                }
                key.push(value);
            }
            keys.push(key);
        }
        FirstSortKeys::Explicit(keys)
    };

    let mut groups = BTreeMap::<Vec<u8>, Vec<usize>>::new();
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        let key = partition_key(row, &spec.partition_by, operation)?;
        match groups.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                state_bytes = state_bytes
                    .checked_add(entry.key().len())
                    .and_then(|bytes| bytes.checked_add(size_of::<Vec<usize>>()))
                    .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
                ensure_first_state_bytes(state_bytes, limits.max_state_bytes, operation)?;
                entry.insert(vec![index]);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().push(index);
            }
        }
    }

    let selected_count = groups.values().try_fold(0usize, |total, indices| {
        total
            .checked_add(indices.len().min(spec.limit))
            .ok_or_else(|| format!("LogsQL {operation} result size overflow"))
    })?;
    if selected_count > limits.max_result_rows {
        return Err(format!(
            "LogsQL {operation} exceeds max_result_rows={}",
            limits.max_result_rows
        ));
    }
    state_bytes = state_bytes
        .checked_add(
            selected_count
                .checked_mul(size_of::<(usize, usize)>())
                .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?,
        )
        .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
    ensure_first_state_bytes(state_bytes, limits.max_state_bytes, operation)?;
    let mut selected = Vec::<(usize, usize)>::with_capacity(selected_count);
    for indices in groups.values_mut() {
        ensure_active(cancelled)?;
        let comparisons = Cell::new(0usize);
        let cancelled_during_sort = Cell::new(false);
        indices.sort_unstable_by(|left, right| {
            let count = comparisons.get().wrapping_add(1);
            comparisons.set(count);
            if count & 255 == 0 && cancelled.load(AtomicOrdering::Acquire) {
                cancelled_during_sort.set(true);
                return left.cmp(right);
            }
            first_row_comparison(*left, *right, &sort_keys, spec, reverse)
                .then_with(|| left.cmp(right))
        });
        if cancelled_during_sort.get() {
            return Err("LogsQL pipeline cancelled".into());
        }
        let keep = indices.len().min(spec.limit);
        selected.extend(
            indices[..keep]
                .iter()
                .enumerate()
                .map(|(rank, index)| (*index, rank + 1)),
        );
    }
    ensure_active(cancelled)?;
    drop(groups);
    drop(sort_keys);

    let mut rows = rows.into_iter().map(Some).collect::<Vec<_>>();
    selected
        .into_iter()
        .enumerate()
        .map(|(position, (index, rank))| {
            check_periodically(cancelled, position)?;
            let mut row = rows[index]
                .take()
                .ok_or_else(|| format!("LogsQL {operation} selected a row twice"))?;
            if let Some(PipelineField::Exact { path, .. }) = &spec.rank_field {
                let object = row
                    .as_object_mut()
                    .ok_or_else(|| "LogsQL pipeline row is not a JSON object".to_string())?;
                insert_path(object, path, Value::String(rank.to_string()))?;
            }
            Ok(row)
        })
        .collect()
}

fn ensure_first_state_bytes(used: usize, limit: usize, operation: &str) -> Result<(), String> {
    if used > limit {
        Err(format!(
            "LogsQL {operation} exceeds max_response_bytes={limit} state budget"
        ))
    } else {
        Ok(())
    }
}

fn top(
    rows: Vec<Value>,
    spec: &TopSpec,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    if spec.limit > limits.max_result_rows {
        return Err(format!(
            "LogsQL top exceeds max_result_rows={}",
            limits.max_result_rows
        ));
    }
    if rows.len() > limits.max_state_items {
        return Err(format!(
            "LogsQL top exceeds max_work_rows={}",
            limits.max_state_items
        ));
    }
    ensure_active(cancelled)?;
    let mut state_bytes = size_of::<BTreeMap<Vec<String>, u64>>();
    let mut groups = BTreeMap::<Vec<String>, u64>::new();
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        let mut key = Vec::with_capacity(spec.by_fields.len());
        for field in &spec.by_fields {
            let PipelineField::Exact { path, .. } = field else {
                return Err("LogsQL top by field is not exact".into());
            };
            key.push(projected_text(field_value(row, path)).into_owned());
        }
        if let Some(hits) = groups.get_mut(&key) {
            *hits = hits.saturating_add(1);
            continue;
        }
        if groups.len() == limits.max_state_items {
            return Err(format!(
                "LogsQL top state exceeds max_work_rows={}",
                limits.max_state_items
            ));
        }
        let key_bytes = key.iter().try_fold(0usize, |total, value| {
            total
                .checked_add(value.len())
                .ok_or_else(|| "LogsQL top state size overflow".to_string())
        })?;
        state_bytes = state_bytes
            .checked_add(size_of::<Vec<String>>())
            .and_then(|bytes| {
                spec.by_fields
                    .len()
                    .checked_mul(size_of::<String>())
                    .and_then(|strings| bytes.checked_add(strings))
            })
            .and_then(|bytes| bytes.checked_add(size_of::<u64>()))
            .and_then(|bytes| bytes.checked_add(key_bytes))
            .ok_or_else(|| "LogsQL top state size overflow".to_string())?;
        ensure_first_state_bytes(state_bytes, limits.max_state_bytes, "top")?;
        groups.insert(key, 1);
    }

    let group_count = groups.len();
    state_bytes = state_bytes
        .checked_add(
            group_count
                .checked_mul(size_of::<(Vec<String>, u64)>())
                .ok_or_else(|| "LogsQL top state size overflow".to_string())?,
        )
        .ok_or_else(|| "LogsQL top state size overflow".to_string())?;
    ensure_first_state_bytes(state_bytes, limits.max_state_bytes, "top")?;
    let mut groups = groups.into_iter().collect::<Vec<_>>();
    let comparisons = Cell::new(0usize);
    let cancelled_during_sort = Cell::new(false);
    groups.sort_by(|(left_key, left_hits), (right_key, right_hits)| {
        let count = comparisons.get().wrapping_add(1);
        comparisons.set(count);
        if count & 255 == 0 && cancelled.load(AtomicOrdering::Acquire) {
            cancelled_during_sort.set(true);
            return left_key.cmp(right_key);
        }
        right_hits
            .cmp(left_hits)
            .then_with(|| left_key.cmp(right_key))
    });
    if cancelled_during_sort.get() {
        return Err("LogsQL pipeline cancelled".into());
    }
    groups.truncate(spec.limit);

    groups
        .into_iter()
        .enumerate()
        .map(|(index, (values, hits))| {
            check_periodically(cancelled, index)?;
            let mut row = Map::new();
            for (field, value) in spec.by_fields.iter().zip(values) {
                let PipelineField::Exact { name, .. } = field else {
                    return Err("LogsQL top by field is not exact".into());
                };
                // VictoriaLogs' stream JSON omits empty fields. Missing, null,
                // and empty values still share the counted empty-text group.
                if !value.is_empty() {
                    row.insert(name.clone(), Value::String(value));
                }
            }
            row.insert(spec.hits_field.clone(), Value::String(hits.to_string()));
            if let Some(rank_field) = &spec.rank_field {
                row.insert(rank_field.clone(), Value::String((index + 1).to_string()));
            }
            Ok(Value::Object(row))
        })
        .collect()
}

fn uniq(
    rows: Vec<Value>,
    spec: &UniqSpec,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    let language_limit = spec.limit.filter(|limit| *limit > 0);
    if language_limit.is_some_and(|limit| limit > limits.max_result_rows) {
        return Err(format!(
            "LogsQL uniq exceeds max_result_rows={}",
            limits.max_result_rows
        ));
    }
    if rows.len() > limits.max_state_items {
        return Err(format!(
            "LogsQL uniq exceeds max_work_rows={}",
            limits.max_state_items
        ));
    }
    ensure_active(cancelled)?;
    let mut state_bytes = size_of::<BTreeMap<Vec<String>, u64>>();
    let mut groups = BTreeMap::<Vec<String>, u64>::new();
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        let mut key = Vec::with_capacity(spec.by_fields.len());
        for field in &spec.by_fields {
            let PipelineField::Exact { path, .. } = field else {
                return Err("LogsQL uniq by field is not exact".into());
            };
            key.push(projected_text(field_value(row, path)).into_owned());
        }
        if spec
            .filter
            .as_deref()
            .is_some_and(|filter| !filter.is_empty() && !key[0].contains(filter))
        {
            continue;
        }
        if let Some(hits) = groups.get_mut(&key) {
            *hits = hits.saturating_add(1);
            continue;
        }
        if groups.len() == limits.max_state_items {
            return Err(format!(
                "LogsQL uniq state exceeds max_work_rows={}",
                limits.max_state_items
            ));
        }
        let key_bytes = key.iter().try_fold(0usize, |total, value| {
            total
                .checked_add(value.len())
                .ok_or_else(|| "LogsQL uniq state size overflow".to_string())
        })?;
        state_bytes = state_bytes
            .checked_add(size_of::<Vec<String>>())
            .and_then(|bytes| {
                spec.by_fields
                    .len()
                    .checked_mul(size_of::<String>())
                    .and_then(|strings| bytes.checked_add(strings))
            })
            .and_then(|bytes| bytes.checked_add(size_of::<u64>()))
            .and_then(|bytes| bytes.checked_add(key_bytes))
            .ok_or_else(|| "LogsQL uniq state size overflow".to_string())?;
        ensure_first_state_bytes(state_bytes, limits.max_state_bytes, "uniq")?;
        groups.insert(key, 1);
    }

    if language_limit.is_none() && groups.len() > limits.max_result_rows {
        return Err(format!(
            "LogsQL uniq exceeds max_result_rows={}",
            limits.max_result_rows
        ));
    }
    let overflow = language_limit.is_some_and(|limit| groups.len() > limit);
    let take = language_limit.unwrap_or(groups.len());
    groups
        .into_iter()
        .take(take)
        .enumerate()
        .map(|(index, (values, hits))| {
            check_periodically(cancelled, index)?;
            let mut row = Map::new();
            for (field, value) in spec.by_fields.iter().zip(values) {
                let PipelineField::Exact { name, .. } = field else {
                    return Err("LogsQL uniq by field is not exact".into());
                };
                // VictoriaLogs' stream JSON omits empty fields while keeping
                // missing/null/empty in one textual uniqueness group.
                if !value.is_empty() {
                    row.insert(name.clone(), Value::String(value));
                }
            }
            if let Some(hits_field) = &spec.hits_field {
                let hits = if overflow { 0 } else { hits };
                row.insert(hits_field.clone(), Value::String(hits.to_string()));
            }
            Ok(Value::Object(row))
        })
        .collect()
}

#[derive(Default)]
struct FacetFieldState {
    values: BTreeMap<String, u64>,
    ignored: bool,
}

fn facets(
    rows: Vec<Value>,
    spec: &FacetsSpec,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    if rows.len() > limits.max_state_items {
        return Err(format!(
            "LogsQL facets exceeds max_work_rows={}",
            limits.max_state_items
        ));
    }
    ensure_active(cancelled)?;

    let rows_total = rows.len() as u64;
    let mut fields = BTreeMap::<String, FacetFieldState>::new();
    let mut group_slots = 0usize;
    let mut state_bytes = size_of::<BTreeMap<String, FacetFieldState>>();
    let mut visits = 0usize;
    for (row_index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, row_index)?;
        let object = row
            .as_object()
            .ok_or_else(|| "LogsQL facets input row is not a JSON object".to_string())?;
        let mut path = String::new();
        let mut seen_fields = BTreeSet::new();
        let mut seen_bytes = size_of::<BTreeSet<String>>();
        for (name, value) in object {
            let original_len = path.len();
            path.push_str(name);
            visit_facet_leaves(
                value,
                &mut path,
                cancelled,
                &mut visits,
                &mut |field_name, value| {
                    if !seen_fields.insert(field_name.to_owned()) {
                        // A retained literal dotted key can collide with a
                        // recursively flattened path. VictoriaLogs has one
                        // flattened column in this situation; count it once.
                        return Ok(());
                    }
                    seen_bytes = seen_bytes
                        .checked_add(size_of::<String>())
                        .and_then(|bytes| bytes.checked_add(field_name.len()))
                        .ok_or_else(|| "LogsQL facets row state size overflow".to_string())?;
                    let peak_bytes = state_bytes
                        .checked_add(seen_bytes)
                        .ok_or_else(|| "LogsQL facets state size overflow".to_string())?;
                    ensure_first_state_bytes(peak_bytes, limits.max_state_bytes, "facets")?;
                    update_facet_state(
                        &mut fields,
                        &mut group_slots,
                        &mut state_bytes,
                        field_name,
                        value,
                        spec,
                        limits,
                    )
                },
            )?;
            path.truncate(original_len);
        }
    }

    let mut output = Vec::new();
    let mut output_state_bytes = size_of::<Vec<Value>>();
    for (field_index, (field_name, field_state)) in fields.into_iter().enumerate() {
        check_periodically(cancelled, field_index)?;
        if field_state.ignored || field_state.values.is_empty() {
            continue;
        }
        if !spec.keep_const_fields
            && field_state.values.len() == 1
            && field_state.values.values().next() == Some(&rows_total)
        {
            continue;
        }

        state_bytes = state_bytes
            .checked_add(
                field_state
                    .values
                    .len()
                    .checked_mul(size_of::<(String, u64)>())
                    .ok_or_else(|| "LogsQL facets sort state size overflow".to_string())?,
            )
            .ok_or_else(|| "LogsQL facets sort state size overflow".to_string())?;
        ensure_first_state_bytes(state_bytes, limits.max_state_bytes, "facets")?;
        let mut values = field_state.values.into_iter().collect::<Vec<_>>();
        let comparisons = Cell::new(0usize);
        let cancelled_during_sort = Cell::new(false);
        values.sort_by(|(left_value, left_hits), (right_value, right_hits)| {
            let count = comparisons.get().wrapping_add(1);
            comparisons.set(count);
            if count & 255 == 0 && cancelled.load(AtomicOrdering::Acquire) {
                cancelled_during_sort.set(true);
                return left_value.cmp(right_value);
            }
            right_hits
                .cmp(left_hits)
                .then_with(|| left_value.as_bytes().cmp(right_value.as_bytes()))
        });
        if cancelled_during_sort.get() {
            return Err("LogsQL pipeline cancelled".into());
        }
        values.truncate(spec.limit);
        if output.len().saturating_add(values.len()) > limits.max_result_rows {
            return Err(format!(
                "LogsQL facets exceeds max_result_rows={}",
                limits.max_result_rows
            ));
        }
        for (value_index, (field_value, hits)) in values.into_iter().enumerate() {
            check_periodically(cancelled, value_index)?;
            let hits = hits.to_string();
            output_state_bytes = output_state_bytes
                .checked_add(size_of::<Value>())
                .and_then(|bytes| bytes.checked_add(3 * size_of::<String>()))
                .and_then(|bytes| bytes.checked_add(field_name.len()))
                .and_then(|bytes| bytes.checked_add(field_value.len()))
                .and_then(|bytes| bytes.checked_add(hits.len()))
                .ok_or_else(|| "LogsQL facets output state size overflow".to_string())?;
            ensure_first_state_bytes(
                state_bytes
                    .checked_add(output_state_bytes)
                    .ok_or_else(|| "LogsQL facets output state size overflow".to_string())?,
                limits.max_state_bytes,
                "facets",
            )?;
            output.push(Value::Object(Map::from_iter([
                ("field_name".into(), Value::String(field_name.clone())),
                ("field_value".into(), Value::String(field_value)),
                ("hits".into(), Value::String(hits)),
            ])));
        }
    }
    ensure_active(cancelled)?;
    Ok(output)
}

fn visit_facet_leaves(
    value: &Value,
    path: &mut String,
    cancelled: &AtomicBool,
    visits: &mut usize,
    callback: &mut impl FnMut(&str, &Value) -> Result<(), String>,
) -> Result<(), String> {
    *visits = visits.saturating_add(1);
    check_periodically(cancelled, *visits)?;
    let Value::Object(object) = value else {
        return callback(path, value);
    };
    for (name, child) in object {
        let original_len = path.len();
        if !path.is_empty() {
            path.push('.');
        }
        path.push_str(name);
        visit_facet_leaves(child, path, cancelled, visits, callback)?;
        path.truncate(original_len);
    }
    Ok(())
}

fn update_facet_state(
    fields: &mut BTreeMap<String, FacetFieldState>,
    group_slots: &mut usize,
    state_bytes: &mut usize,
    field_name: &str,
    value: &Value,
    spec: &FacetsSpec,
    limits: PipelineLimits,
) -> Result<(), String> {
    let value = projected_text(Some(value));
    if value.is_empty() {
        return Ok(());
    }
    if !fields.contains_key(field_name) {
        if fields.len() == limits.max_state_items {
            return Err(format!(
                "LogsQL facets field state exceeds max_work_rows={}",
                limits.max_state_items
            ));
        }
        *state_bytes = state_bytes
            .checked_add(size_of::<String>())
            .and_then(|bytes| bytes.checked_add(size_of::<FacetFieldState>()))
            .and_then(|bytes| bytes.checked_add(field_name.len()))
            .ok_or_else(|| "LogsQL facets state size overflow".to_string())?;
        ensure_first_state_bytes(*state_bytes, limits.max_state_bytes, "facets")?;
        fields.insert(field_name.to_owned(), FacetFieldState::default());
    }
    let field = fields
        .get_mut(field_name)
        .expect("facets field inserted above");
    if field.ignored {
        return Ok(());
    }
    if value.len() > spec.max_value_len {
        field.values.clear();
        field.ignored = true;
        return Ok(());
    }
    if let Some(hits) = field.values.get_mut(value.as_ref()) {
        *hits = hits.saturating_add(1);
        return Ok(());
    }
    if field.values.len() == spec.max_values_per_field {
        field.values.clear();
        field.ignored = true;
        return Ok(());
    }
    if *group_slots == limits.max_state_items {
        return Err(format!(
            "LogsQL facets value state exceeds max_work_rows={}",
            limits.max_state_items
        ));
    }
    *group_slots = group_slots.saturating_add(1);
    *state_bytes = state_bytes
        .checked_add(size_of::<String>())
        .and_then(|bytes| bytes.checked_add(size_of::<u64>()))
        .and_then(|bytes| bytes.checked_add(value.len()))
        .ok_or_else(|| "LogsQL facets state size overflow".to_string())?;
    ensure_first_state_bytes(*state_bytes, limits.max_state_bytes, "facets")?;
    field.values.insert(value.into_owned(), 1);
    Ok(())
}

fn coalesce(
    rows: Vec<Value>,
    spec: &CoalesceSpec,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    let PipelineField::Exact {
        path: destination_path,
        name: destination_name,
    } = &spec.destination
    else {
        return Err("LogsQL coalesce destination is not exact".into());
    };
    rows.into_iter()
        .enumerate()
        .map(|(row_index, mut row)| {
            check_periodically(cancelled, row_index)?;
            let mut seen = BTreeSet::new();
            let mut state_bytes = size_of::<BTreeSet<String>>();
            let mut visits = 0usize;
            let mut selected = None;
            for source in &spec.sources {
                ensure_active(cancelled)?;
                selected = match source {
                    PipelineField::Exact { path, name } => {
                        if remember_coalesce_field(
                            &mut seen,
                            &mut state_bytes,
                            name,
                            limits.max_state_bytes,
                        )? {
                            coalesce_exact_value(&row, path)
                                .filter(|value| !matches!(value, Value::Object(_)))
                                .map(|value| projected_text(Some(value)))
                                .filter(|value| !value.is_empty())
                                .map(Cow::into_owned)
                        } else {
                            None
                        }
                    }
                    PipelineField::Prefix { prefix } => first_coalesce_leaf(
                        &row,
                        &mut String::new(),
                        Some(prefix),
                        &mut seen,
                        &mut state_bytes,
                        limits.max_state_bytes,
                        cancelled,
                        &mut visits,
                    )?,
                    PipelineField::All => first_coalesce_leaf(
                        &row,
                        &mut String::new(),
                        None,
                        &mut seen,
                        &mut state_bytes,
                        limits.max_state_bytes,
                        cancelled,
                        &mut visits,
                    )?,
                };
                if selected.is_some() {
                    break;
                }
            }
            let selected = selected.unwrap_or_else(|| spec.default_value.clone());
            state_bytes = state_bytes
                .checked_add(selected.len())
                .and_then(|bytes| bytes.checked_add(destination_name.len()))
                .ok_or_else(|| "LogsQL coalesce state size overflow".to_string())?;
            ensure_first_state_bytes(state_bytes, limits.max_state_bytes, "coalesce")?;
            let object = row
                .as_object_mut()
                .ok_or_else(|| "LogsQL coalesce input row is not a JSON object".to_string())?;
            insert_path(object, destination_path, Value::String(selected))
                .map_err(|error| format!("LogsQL coalesce destination conflict: {error}"))?;
            Ok(row)
        })
        .collect()
}

fn coalesce_exact_value<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut current = value;
    for segment in path {
        current = match current {
            Value::Object(object) => object.get(segment)?,
            // VictoriaLogs retains arrays as one atomic textual column; a
            // dotted source never invents array-index columns.
            Value::Null
            | Value::Bool(_)
            | Value::Number(_)
            | Value::String(_)
            | Value::Array(_) => return None,
        };
    }
    Some(current)
}

#[allow(clippy::too_many_arguments)]
fn first_coalesce_leaf(
    value: &Value,
    path: &mut String,
    prefix: Option<&str>,
    seen: &mut BTreeSet<String>,
    state_bytes: &mut usize,
    max_state_bytes: usize,
    cancelled: &AtomicBool,
    visits: &mut usize,
) -> Result<Option<String>, String> {
    *visits = visits.saturating_add(1);
    check_periodically(cancelled, *visits)?;
    if let Value::Object(object) = value {
        for (name, child) in object {
            let original_len = path.len();
            if !path.is_empty() {
                path.push('.');
            }
            path.push_str(name);
            if let Some(value) = first_coalesce_leaf(
                child,
                path,
                prefix,
                seen,
                state_bytes,
                max_state_bytes,
                cancelled,
                visits,
            )? {
                return Ok(Some(value));
            }
            path.truncate(original_len);
        }
        return Ok(None);
    }
    if prefix.is_some_and(|prefix| !path.starts_with(prefix)) {
        return Ok(None);
    }
    if !remember_coalesce_field(seen, state_bytes, path, max_state_bytes)? {
        return Ok(None);
    }
    let value = projected_text(Some(value));
    Ok((!value.is_empty()).then(|| value.into_owned()))
}

fn remember_coalesce_field(
    seen: &mut BTreeSet<String>,
    state_bytes: &mut usize,
    name: &str,
    max_state_bytes: usize,
) -> Result<bool, String> {
    if seen.contains(name) {
        return Ok(false);
    }
    *state_bytes = state_bytes
        .checked_add(size_of::<String>())
        .and_then(|bytes| bytes.checked_add(name.len()))
        .ok_or_else(|| "LogsQL coalesce state size overflow".to_string())?;
    ensure_first_state_bytes(*state_bytes, max_state_bytes, "coalesce")?;
    seen.insert(name.to_owned());
    Ok(true)
}

fn copy_fields(
    rows: Vec<Value>,
    spec: &CopySpec,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    rows.into_iter()
        .enumerate()
        .map(|(row_index, mut row)| {
            check_periodically(cancelled, row_index)?;
            let mut work_items = 0usize;
            for (source, destination) in &spec.pairs {
                ensure_active(cancelled)?;
                let mut copies = Vec::new();
                let mut state_bytes = size_of::<Vec<(String, Value)>>();
                match source {
                    PipelineField::Exact { path, name } => {
                        charge_copy_work(&mut work_items, limits.max_state_items)?;
                        let value = coalesce_exact_value(&row, path)
                            .filter(|value| !matches!(value, Value::Object(_)))
                            .cloned()
                            .unwrap_or_else(|| Value::String(String::new()));
                        charge_copy_string(
                            name,
                            &mut state_bytes,
                            limits.max_state_bytes,
                        )?;
                        charge_copy_value(
                            &value,
                            &mut state_bytes,
                            limits.max_state_bytes,
                            cancelled,
                            &mut work_items,
                            limits.max_state_items,
                        )?;
                        copies.push((name.clone(), value));
                    }
                    PipelineField::Prefix { prefix } => collect_copy_leaves(
                        &row,
                        &mut String::new(),
                        Some(prefix),
                        &mut copies,
                        &mut state_bytes,
                        limits,
                        cancelled,
                        &mut work_items,
                    )?,
                    PipelineField::All => collect_copy_leaves(
                        &row,
                        &mut String::new(),
                        None,
                        &mut copies,
                        &mut state_bytes,
                        limits,
                        cancelled,
                        &mut work_items,
                    )?,
                }

                for (copy_index, (source_name, value)) in copies.into_iter().enumerate() {
                    check_periodically(cancelled, copy_index)?;
                    let (destination_name, destination_path) =
                        copy_destination(source, destination, &source_name)?;
                    charge_copy_string(
                        &destination_name,
                        &mut state_bytes,
                        limits.max_state_bytes,
                    )?;
                    let object = row.as_object_mut().ok_or_else(|| {
                        "LogsQL copy input row is not a JSON object".to_string()
                    })?;
                    if copy_destination_replaces_object(object, &destination_path) {
                        return Err(format!(
                            "LogsQL copy destination conflict: field {destination_name:?} would replace a retained object"
                        ));
                    }
                    insert_path(object, &destination_path, value)
                        .map_err(|error| format!("LogsQL copy destination conflict: {error}"))?;
                }
            }
            Ok(row)
        })
        .collect()
}

#[derive(Debug)]
struct RenameMove {
    source_name: String,
    source_path: Option<Vec<String>>,
    destination_name: String,
    destination_path: Vec<String>,
    value: Value,
}

fn rename_fields(
    rows: Vec<Value>,
    spec: &RenameSpec,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    rows.into_iter()
        .enumerate()
        .map(|(row_index, mut row)| {
            check_periodically(cancelled, row_index)?;
            let mut work_items = 0usize;
            for (source, destination) in &spec.pairs {
                ensure_active(cancelled)?;
                let mut moves = Vec::new();
                let mut state_bytes = size_of::<Vec<RenameMove>>();
                match source {
                    PipelineField::Exact { path, name } => {
                        charge_rename_work(&mut work_items, limits.max_state_items)?;
                        let source_value = coalesce_exact_value(&row, path)
                            .filter(|value| !matches!(value, Value::Object(_)));
                        let source_path = source_value.map(|_| path.clone());
                        let value = source_value
                            .cloned()
                            .unwrap_or_else(|| Value::String(String::new()));
                        charge_rename_move(
                            name,
                            source_path.as_deref(),
                            &value,
                            &mut state_bytes,
                            limits,
                            cancelled,
                            &mut work_items,
                        )?;
                        moves.push(RenameMove {
                            source_name: name.clone(),
                            source_path,
                            destination_name: String::new(),
                            destination_path: Vec::new(),
                            value,
                        });
                    }
                    PipelineField::Prefix { prefix } => collect_rename_leaves(
                        &row,
                        &mut String::new(),
                        &mut Vec::new(),
                        Some(prefix),
                        &mut moves,
                        &mut state_bytes,
                        limits,
                        cancelled,
                        &mut work_items,
                    )?,
                    PipelineField::All => collect_rename_leaves(
                        &row,
                        &mut String::new(),
                        &mut Vec::new(),
                        None,
                        &mut moves,
                        &mut state_bytes,
                        limits,
                        cancelled,
                        &mut work_items,
                    )?,
                }

                for (move_index, field) in moves.iter_mut().enumerate() {
                    check_periodically(cancelled, move_index)?;
                    let (destination_name, destination_path) =
                        copy_destination(source, destination, &field.source_name)?;
                    charge_rename_string(
                        &destination_name,
                        &mut state_bytes,
                        limits.max_state_bytes,
                    )?;
                    charge_rename_path(
                        &destination_path,
                        &mut state_bytes,
                        limits.max_state_bytes,
                    )?;
                    field.destination_name = destination_name;
                    field.destination_path = destination_path;
                }

                let object = row
                    .as_object_mut()
                    .ok_or_else(|| "LogsQL rename input row is not a JSON object".to_string())?;
                for field in &moves {
                    ensure_active(cancelled)?;
                    if let Some(path) = &field.source_path {
                        delete_exact_path(object, path, cancelled)?;
                    }
                }
                for (move_index, field) in moves.into_iter().enumerate() {
                    check_periodically(cancelled, move_index)?;
                    if copy_destination_replaces_object(object, &field.destination_path) {
                        return Err(format!(
                            "LogsQL rename destination conflict: field {:?} would replace a retained object",
                            field.destination_name
                        ));
                    }
                    insert_path(object, &field.destination_path, field.value)
                        .map_err(|error| format!("LogsQL rename destination conflict: {error}"))?;
                }
            }
            Ok(row)
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn collect_rename_leaves(
    value: &Value,
    flattened_path: &mut String,
    source_path: &mut Vec<String>,
    prefix: Option<&str>,
    output: &mut Vec<RenameMove>,
    state_bytes: &mut usize,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
    work_items: &mut usize,
) -> Result<(), String> {
    charge_rename_work(work_items, limits.max_state_items)?;
    check_periodically(cancelled, *work_items)?;
    if let Value::Object(object) = value {
        for (name, child) in object {
            let original_len = flattened_path.len();
            if !flattened_path.is_empty() {
                flattened_path.push('.');
            }
            flattened_path.push_str(name);
            source_path.push(name.clone());
            collect_rename_leaves(
                child,
                flattened_path,
                source_path,
                prefix,
                output,
                state_bytes,
                limits,
                cancelled,
                work_items,
            )?;
            source_path.pop();
            flattened_path.truncate(original_len);
        }
        return Ok(());
    }
    if prefix.is_some_and(|prefix| !flattened_path.starts_with(prefix)) {
        return Ok(());
    }
    charge_rename_move(
        flattened_path,
        Some(source_path),
        value,
        state_bytes,
        limits,
        cancelled,
        work_items,
    )?;
    output.push(RenameMove {
        source_name: flattened_path.clone(),
        source_path: Some(source_path.clone()),
        destination_name: String::new(),
        destination_path: Vec::new(),
        value: value.clone(),
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn charge_rename_move(
    source_name: &str,
    source_path: Option<&[String]>,
    value: &Value,
    state_bytes: &mut usize,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
    work_items: &mut usize,
) -> Result<(), String> {
    *state_bytes = state_bytes
        .checked_add(size_of::<RenameMove>())
        .ok_or_else(|| "LogsQL rename state size overflow".to_string())?;
    ensure_first_state_bytes(*state_bytes, limits.max_state_bytes, "rename")?;
    charge_rename_string(source_name, state_bytes, limits.max_state_bytes)?;
    if let Some(path) = source_path {
        charge_rename_path(path, state_bytes, limits.max_state_bytes)?;
    }
    charge_rename_value(
        value,
        state_bytes,
        limits.max_state_bytes,
        cancelled,
        work_items,
        limits.max_state_items,
    )
}

fn charge_rename_path(path: &[String], used: &mut usize, limit: usize) -> Result<(), String> {
    *used = used
        .checked_add(size_of::<Vec<String>>())
        .ok_or_else(|| "LogsQL rename state size overflow".to_string())?;
    ensure_first_state_bytes(*used, limit, "rename")?;
    for segment in path {
        charge_rename_string(segment, used, limit)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_copy_leaves(
    value: &Value,
    path: &mut String,
    prefix: Option<&str>,
    output: &mut Vec<(String, Value)>,
    state_bytes: &mut usize,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
    work_items: &mut usize,
) -> Result<(), String> {
    charge_copy_work(work_items, limits.max_state_items)?;
    check_periodically(cancelled, *work_items)?;
    if let Value::Object(object) = value {
        for (name, child) in object {
            let original_len = path.len();
            if !path.is_empty() {
                path.push('.');
            }
            path.push_str(name);
            collect_copy_leaves(
                child,
                path,
                prefix,
                output,
                state_bytes,
                limits,
                cancelled,
                work_items,
            )?;
            path.truncate(original_len);
        }
        return Ok(());
    }
    if prefix.is_some_and(|prefix| !path.starts_with(prefix)) {
        return Ok(());
    }
    charge_copy_string(path, state_bytes, limits.max_state_bytes)?;
    charge_copy_value(
        value,
        state_bytes,
        limits.max_state_bytes,
        cancelled,
        work_items,
        limits.max_state_items,
    )?;
    output.push((path.clone(), value.clone()));
    Ok(())
}

fn copy_destination(
    source: &PipelineField,
    destination: &PipelineField,
    source_name: &str,
) -> Result<(String, Vec<String>), String> {
    if matches!(source, PipelineField::Exact { .. }) {
        return Ok(match destination {
            PipelineField::Exact { path, name } => (name.clone(), path.clone()),
            // VictoriaLogs accepts these unusual forms. With an exact source,
            // the destination filter is the literal destination column name;
            // prefix substitution only occurs for a wildcard source.
            PipelineField::Prefix { prefix } => {
                let name = format!("{prefix}*");
                (name.clone(), vec![name])
            }
            PipelineField::All => ("*".into(), vec!["*".into()]),
        });
    }

    let suffix = match source {
        PipelineField::Prefix { prefix } => source_name.strip_prefix(prefix).ok_or_else(|| {
            format!("LogsQL copy source field {source_name:?} does not match prefix {prefix:?}")
        })?,
        PipelineField::All => source_name,
        PipelineField::Exact { .. } => unreachable!("handled above"),
    };
    match destination {
        PipelineField::Exact { path, name } => Ok((name.clone(), path.clone())),
        PipelineField::Prefix { prefix } => {
            let name = format!("{prefix}{suffix}");
            Ok(copy_flattened_destination(name))
        }
        PipelineField::All => Ok(copy_flattened_destination(suffix.to_owned())),
    }
}

fn copy_flattened_destination(name: String) -> (String, Vec<String>) {
    // Unlike an exact parsed empty field, a wildcard substitution may produce
    // a literal empty destination name. VictoriaLogs keeps that generated
    // column distinct from the canonical message field.
    let path = name.split('.').map(str::to_owned).collect();
    (name, path)
}

fn copy_destination_replaces_object(object: &Map<String, Value>, path: &[String]) -> bool {
    let Some((last, parents)) = path.split_last() else {
        return false;
    };
    let mut current = object;
    for parent in parents {
        let Some(Value::Object(child)) = current.get(parent) else {
            return false;
        };
        current = child;
    }
    matches!(current.get(last), Some(Value::Object(_)))
}

fn charge_copy_work(used: &mut usize, limit: usize) -> Result<(), String> {
    charge_transfer_work(used, limit, "copy")
}

fn charge_rename_work(used: &mut usize, limit: usize) -> Result<(), String> {
    charge_transfer_work(used, limit, "rename")
}

fn charge_transfer_work(used: &mut usize, limit: usize, operation: &str) -> Result<(), String> {
    *used = used
        .checked_add(1)
        .ok_or_else(|| format!("LogsQL {operation} work size overflow"))?;
    if *used > limit {
        return Err(format!(
            "LogsQL {operation} traversal exceeds max_work_rows={limit}"
        ));
    }
    Ok(())
}

fn charge_copy_string(value: &str, used: &mut usize, limit: usize) -> Result<(), String> {
    charge_transfer_string(value, used, limit, "copy")
}

fn charge_rename_string(value: &str, used: &mut usize, limit: usize) -> Result<(), String> {
    charge_transfer_string(value, used, limit, "rename")
}

fn charge_transfer_string(
    value: &str,
    used: &mut usize,
    limit: usize,
    operation: &str,
) -> Result<(), String> {
    *used = used
        .checked_add(size_of::<String>())
        .and_then(|bytes| bytes.checked_add(value.len()))
        .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
    ensure_first_state_bytes(*used, limit, operation)
}

fn charge_copy_value(
    value: &Value,
    used: &mut usize,
    limit: usize,
    cancelled: &AtomicBool,
    work_items: &mut usize,
    max_work_items: usize,
) -> Result<(), String> {
    charge_transfer_value(
        value,
        used,
        limit,
        cancelled,
        work_items,
        max_work_items,
        "copy",
    )
}

#[allow(clippy::too_many_arguments)]
fn charge_rename_value(
    value: &Value,
    used: &mut usize,
    limit: usize,
    cancelled: &AtomicBool,
    work_items: &mut usize,
    max_work_items: usize,
) -> Result<(), String> {
    charge_transfer_value(
        value,
        used,
        limit,
        cancelled,
        work_items,
        max_work_items,
        "rename",
    )
}

#[allow(clippy::too_many_arguments)]
fn charge_transfer_value(
    value: &Value,
    used: &mut usize,
    limit: usize,
    cancelled: &AtomicBool,
    work_items: &mut usize,
    max_work_items: usize,
    operation: &str,
) -> Result<(), String> {
    ensure_active(cancelled)?;
    *used = used
        .checked_add(size_of::<Value>())
        .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
    ensure_first_state_bytes(*used, limit, operation)?;
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => Ok(()),
        Value::String(value) => {
            *used = used
                .checked_add(value.len())
                .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
            ensure_first_state_bytes(*used, limit, operation)
        }
        Value::Array(values) => {
            *used = used
                .checked_add(size_of::<Vec<Value>>())
                .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
            ensure_first_state_bytes(*used, limit, operation)?;
            for value in values {
                charge_transfer_work(work_items, max_work_items, operation)?;
                charge_transfer_value(
                    value,
                    used,
                    limit,
                    cancelled,
                    work_items,
                    max_work_items,
                    operation,
                )?;
            }
            Ok(())
        }
        Value::Object(object) => {
            *used = used
                .checked_add(size_of::<Map<String, Value>>())
                .ok_or_else(|| format!("LogsQL {operation} state size overflow"))?;
            ensure_first_state_bytes(*used, limit, operation)?;
            for (name, value) in object {
                charge_transfer_work(work_items, max_work_items, operation)?;
                charge_transfer_string(name, used, limit, operation)?;
                charge_transfer_value(
                    value,
                    used,
                    limit,
                    cancelled,
                    work_items,
                    max_work_items,
                    operation,
                )?;
            }
            Ok(())
        }
    }
}

fn first_row_comparison(
    left: usize,
    right: usize,
    keys: &FirstSortKeys<'_>,
    spec: &FirstSpec,
    reverse: bool,
) -> Ordering {
    let ordering = match keys {
        FirstSortKeys::AllFields(keys) => logsql_sort_comparison(&keys[left], &keys[right]),
        FirstSortKeys::Explicit(keys) => {
            let mut result = Ordering::Equal;
            for (index, field) in spec.by_fields.iter().enumerate() {
                let mut ordering =
                    logsql_sort_comparison(keys[left][index].as_ref(), keys[right][index].as_ref());
                if field.descending {
                    ordering = ordering.reverse();
                }
                if !ordering.is_eq() {
                    result = ordering;
                    break;
                }
            }
            result
        }
    };
    if reverse {
        ordering.reverse()
    } else {
        ordering
    }
}

fn partition_key(
    row: &Value,
    fields: &[PipelineField],
    operation: &str,
) -> Result<Vec<u8>, String> {
    let mut key = Vec::new();
    for field in fields {
        let PipelineField::Exact { path, .. } = field else {
            return Err(format!("LogsQL {operation} partition field is not exact"));
        };
        let value = projected_text(field_value(row, path));
        append_varuint(&mut key, value.len() as u64);
        key.extend_from_slice(value.as_bytes());
    }
    Ok(key)
}

fn append_varuint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn all_fields_sort_key(row: &Value, operation: &str) -> Result<String, String> {
    let object = row
        .as_object()
        .ok_or_else(|| "LogsQL pipeline row is not a JSON object".to_string())?;
    let mut key = String::new();
    if let Some(value) = object.get("_time") {
        append_all_fields_sort_value(&mut key, "_time", value, operation)?;
    }
    for (name, value) in object {
        if name != "_time" {
            append_all_fields_sort_value(&mut key, name, value, operation)?;
        }
    }
    Ok(key)
}

fn append_all_fields_sort_value(
    output: &mut String,
    name: &str,
    value: &Value,
    operation: &str,
) -> Result<(), String> {
    output.push_str(
        &serde_json::to_string(name)
            .map_err(|error| format!("encode LogsQL {operation} field name: {error}"))?,
    );
    output.push(':');
    let value = projected_text(Some(value));
    output.push_str(
        &serde_json::to_string(value.as_ref())
            .map_err(|error| format!("encode LogsQL {operation} field value: {error}"))?,
    );
    output.push(',');
    Ok(())
}

fn projected_text(value: Option<&Value>) -> Cow<'_, str> {
    match value {
        None | Some(Value::Null) => Cow::Borrowed(""),
        Some(Value::String(value)) => Cow::Borrowed(value),
        Some(value) => Cow::Owned(serde_json::to_string(value).unwrap_or_default()),
    }
}

fn field_values(
    rows: &[Value],
    field: &PipelineField,
    filter: Option<&str>,
    explicit_limit: Option<usize>,
    max_result_rows: usize,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    let PipelineField::Exact { path, name } = field else {
        return Err("LogsQL field_values requires one exact field".into());
    };
    // VictoriaLogs treats `limit 0` as an omitted operator limit.
    let explicit_limit = explicit_limit.filter(|limit| *limit > 0);
    let capacity = explicit_limit.unwrap_or(max_result_rows);
    let mut values: BTreeMap<String, (Option<Value>, u64)> = BTreeMap::new();
    let mut overflow = false;
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        let value = field_value(row, path).cloned();
        if filter.is_some_and(|filter| !display_value(value.as_ref()).contains(filter)) {
            continue;
        }
        let key = presence_key(value.as_ref());
        if let Some((_, hits)) = values.get_mut(&key) {
            *hits = hits.saturating_add(1);
            continue;
        }
        if values.len() < capacity {
            values.insert(key, (value, 1));
            continue;
        }
        overflow = true;
        if explicit_limit.is_none() {
            return Err(format!(
                "LogsQL field_values exceeds max_result_rows={max_result_rows}"
            ));
        }
        let Some(largest) = values.keys().next_back().cloned() else {
            continue;
        };
        if key < largest {
            values.remove(&largest);
            values.insert(key, (value, 1));
        }
    }
    Ok(values
        .into_values()
        .map(|(value, hits)| {
            let mut row = Map::new();
            if let Some(value) = value {
                row.insert(name.clone(), value);
            }
            // VictoriaLogs cannot know exact hits after its cardinality limit
            // is crossed. Preserve that explicit contract while selecting a
            // deterministic typed subset.
            row.insert("hits".into(), Value::from(if overflow { 0 } else { hits }));
            Value::Object(row)
        })
        .collect())
}

fn field_names(
    rows: &[Value],
    filter: Option<&str>,
    result_name: &str,
    max_result_rows: usize,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    let mut names = BTreeMap::<String, u64>::new();
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        let object = row
            .as_object()
            .ok_or_else(|| "LogsQL pipeline row is not a JSON object".to_string())?;
        for name in object.keys() {
            if filter.is_some_and(|filter| !name.contains(filter)) {
                continue;
            }
            if !names.contains_key(name) && names.len() == max_result_rows {
                return Err(format!(
                    "LogsQL field_names exceeds max_result_rows={max_result_rows}"
                ));
            }
            let hits = names.entry(name.clone()).or_default();
            *hits = hits.saturating_add(1);
        }
    }
    let hits_name = if result_name == "hits" {
        "hits_2"
    } else {
        "hits"
    };
    Ok(names
        .into_iter()
        .map(|(name, hits)| {
            Value::Object(Map::from_iter([
                (result_name.to_owned(), Value::String(name)),
                (hits_name.to_owned(), Value::from(hits)),
            ]))
        })
        .collect())
}

fn project(
    rows: Vec<Value>,
    fields: &[PipelineField],
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| {
            check_periodically(cancelled, index)?;
            let source = row
                .as_object()
                .ok_or_else(|| "LogsQL pipeline row is not a JSON object".to_string())?;
            let mut projected = Map::new();
            for field in fields {
                match field {
                    PipelineField::All => {
                        for (name, value) in source {
                            projected.insert(name.clone(), value.clone());
                        }
                    }
                    PipelineField::Prefix { prefix } => {
                        for (name, value) in source {
                            if name.starts_with(prefix) {
                                projected.insert(name.clone(), value.clone());
                            }
                        }
                    }
                    PipelineField::Exact { path, .. } => {
                        if let Some(value) = field_value(&row, path) {
                            insert_path(&mut projected, path, value.clone())?;
                        }
                    }
                }
            }
            Ok(Value::Object(projected))
        })
        .collect()
}

fn delete_fields(
    rows: Vec<Value>,
    fields: &[PipelineField],
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    let mut output = Vec::with_capacity(rows.len());
    for (index, mut row) in rows.into_iter().enumerate() {
        check_periodically(cancelled, index)?;
        let object = row
            .as_object_mut()
            .ok_or_else(|| "LogsQL pipeline row is not a JSON object".to_string())?;
        for field in fields {
            ensure_active(cancelled)?;
            match field {
                PipelineField::All => object.clear(),
                PipelineField::Exact { path, .. } => {
                    delete_exact_path(object, path, cancelled)?;
                }
                PipelineField::Prefix { prefix } => {
                    delete_field_prefix(object, prefix, cancelled)?;
                }
            }
            if object.is_empty() {
                break;
            }
        }
        // VictoriaLogs omits a result row after every field has been deleted.
        if !object.is_empty() {
            output.push(row);
        }
    }
    Ok(output)
}

fn delete_exact_path(
    object: &mut Map<String, Value>,
    path: &[String],
    cancelled: &AtomicBool,
) -> Result<bool, String> {
    ensure_active(cancelled)?;
    let Some((first, tail)) = path.split_first() else {
        return Ok(false);
    };
    if tail.is_empty() {
        return Ok(object.remove(first).is_some());
    }
    let (removed, prune_parent) = match object.get_mut(first) {
        Some(Value::Object(child)) => {
            let removed = delete_exact_path(child, tail, cancelled)?;
            (removed, removed && child.is_empty())
        }
        // Retained arrays and scalars are atomic fields. Deletion never shifts
        // array element indexes or mutates stored rich values.
        Some(_) | None => (false, false),
    };
    if prune_parent {
        object.remove(first);
    }
    Ok(removed)
}

fn delete_field_prefix(
    object: &mut Map<String, Value>,
    prefix: &str,
    cancelled: &AtomicBool,
) -> Result<(), String> {
    let mut path = String::new();
    let mut visits = 0usize;
    let mut was_cancelled = false;
    object.retain(|name, value| {
        if was_cancelled {
            return true;
        }
        let original_len = path.len();
        path.push_str(name);
        let keep = retain_nonmatching_prefix(
            value,
            &mut path,
            prefix,
            cancelled,
            &mut visits,
            &mut was_cancelled,
        );
        path.truncate(original_len);
        keep
    });
    if was_cancelled {
        return ensure_active(cancelled);
    }
    Ok(())
}

fn retain_nonmatching_prefix(
    value: &mut Value,
    path: &mut String,
    prefix: &str,
    cancelled: &AtomicBool,
    visits: &mut usize,
    was_cancelled: &mut bool,
) -> bool {
    *visits = visits.saturating_add(1);
    if *visits & 0x3f == 0 && cancelled.load(AtomicOrdering::Relaxed) {
        *was_cancelled = true;
        return true;
    }
    if path.starts_with(prefix) {
        return false;
    }
    let Value::Object(object) = value else {
        return true;
    };
    let had_children = !object.is_empty();
    object.retain(|name, child| {
        if *was_cancelled {
            return true;
        }
        let original_len = path.len();
        if !path.is_empty() {
            path.push('.');
        }
        path.push_str(name);
        let keep = retain_nonmatching_prefix(child, path, prefix, cancelled, visits, was_cancelled);
        path.truncate(original_len);
        keep
    });
    !(had_children && object.is_empty())
}

fn filter(
    rows: Vec<Value>,
    predicate: &LogPredicate,
    timestamp_unit: TimestampUnit,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    let mut output = Vec::with_capacity(rows.len());
    for (index, row) in rows.into_iter().enumerate() {
        check_periodically(cancelled, index)?;
        if predicate_matches(predicate, &row, timestamp_unit, cancelled)? {
            output.push(row);
        }
    }
    Ok(output)
}

fn stats(
    rows: &[Value],
    expressions: &[StatsExpression],
    rate_window_seconds: Option<f64>,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Map<String, Value>, String> {
    let mut result = Map::new();
    for expression in expressions {
        ensure_active(cancelled)?;
        let value = match expression.kind {
            StatsKind::Count => Value::from(count(rows, &expression.fields, false, cancelled)?),
            StatsKind::CountEmpty => Value::from(count(rows, &expression.fields, true, cancelled)?),
            StatsKind::CountUniq => Value::from(count_uniq(
                rows,
                &expression.fields,
                expression.limit,
                limits.max_state_items,
                false,
                cancelled,
            )?),
            StatsKind::CountUniqHash => Value::from(count_uniq(
                rows,
                &expression.fields,
                expression.limit,
                limits.max_state_items,
                true,
                cancelled,
            )?),
            StatsKind::UniqValues => uniq_values(
                rows,
                &expression.fields,
                expression.limit,
                limits.max_result_rows,
                cancelled,
            )?,
            StatsKind::Values => values(
                rows,
                &expression.fields,
                expression.limit,
                limits.max_result_rows,
                cancelled,
            )?,
            StatsKind::Sum => numeric_sum(rows, &expression.fields, cancelled)?.0,
            StatsKind::Avg => {
                let (sum, count) = numeric_sum(rows, &expression.fields, cancelled)?;
                finite_number(sum.as_f64().unwrap_or(0.0) / count as f64)
            }
            StatsKind::Min => numeric_extreme(rows, &expression.fields, false, cancelled)?,
            StatsKind::Max => numeric_extreme(rows, &expression.fields, true, cancelled)?,
            StatsKind::Median => {
                median(rows, &expression.fields, limits.max_state_items, cancelled)?
            }
            StatsKind::Rate => {
                let value = rate_window_seconds
                    .filter(|duration| *duration > 0.0)
                    .map_or(rows.len() as f64, |duration| rows.len() as f64 / duration);
                finite_number(value)
            }
            StatsKind::RateSum => {
                let (sum, _) = numeric_sum(rows, &expression.fields, cancelled)?;
                let value = rate_window_seconds
                    .filter(|duration| *duration > 0.0)
                    .map_or(sum.as_f64().unwrap_or(0.0), |duration| {
                        sum.as_f64().unwrap_or(0.0) / duration
                    });
                finite_number(value)
            }
        };
        result.insert(expression.alias.clone(), value);
    }
    Ok(result)
}

fn count(
    rows: &[Value],
    fields: &[PipelineField],
    empty: bool,
    cancelled: &AtomicBool,
) -> Result<u64, String> {
    if !empty && matches!(fields, [PipelineField::All]) {
        return Ok(rows.len() as u64);
    }
    let mut total = 0u64;
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        let selected = selected_values(row, fields);
        let matches = if empty {
            selected.iter().all(|value| value.is_none_or(is_empty))
        } else {
            selected.iter().any(|value| value.is_some_and(is_nonempty))
        };
        if matches {
            total = total.saturating_add(1);
        }
    }
    Ok(total)
}

fn count_uniq(
    rows: &[Value],
    fields: &[PipelineField],
    explicit_limit: Option<usize>,
    max_state_items: usize,
    hashed: bool,
    cancelled: &AtomicBool,
) -> Result<u64, String> {
    let explicit_limit = explicit_limit.filter(|limit| *limit > 0);
    let capacity = explicit_limit.unwrap_or(max_state_items);
    let mut exact = BTreeSet::new();
    let mut hashes = BTreeSet::new();
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        let selected = selected_values(row, fields);
        if selected.iter().all(|value| value.is_none_or(is_empty)) {
            continue;
        }
        let key = tuple_key(&selected);
        let inserted = if hashed {
            hashes.insert(fnv1a64(key.as_bytes()))
        } else {
            exact.insert(key)
        };
        let count = if hashed { hashes.len() } else { exact.len() };
        if inserted && count > capacity {
            if explicit_limit.is_some() {
                return Ok(capacity as u64);
            }
            return Err(format!(
                "LogsQL unique state exceeds max_work_rows={max_state_items}"
            ));
        }
    }
    Ok(if hashed { hashes.len() } else { exact.len() } as u64)
}

fn uniq_values(
    rows: &[Value],
    fields: &[PipelineField],
    explicit_limit: Option<usize>,
    max_result_rows: usize,
    cancelled: &AtomicBool,
) -> Result<Value, String> {
    let explicit_limit = explicit_limit.filter(|limit| *limit > 0);
    let capacity = explicit_limit.unwrap_or(max_result_rows);
    let mut values = BTreeMap::<String, Value>::new();
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        for value in selected_values(row, fields).into_iter().flatten() {
            if is_empty(value) {
                continue;
            }
            let key = value_key(value);
            if values.contains_key(&key) {
                continue;
            }
            if values.len() == capacity {
                if explicit_limit.is_none() {
                    return Err(format!(
                        "LogsQL uniq_values exceeds max_result_rows={max_result_rows}"
                    ));
                }
                let largest = values.keys().next_back().cloned().unwrap();
                if key >= largest {
                    continue;
                }
                values.remove(&largest);
            }
            values.insert(key, value.clone());
        }
    }
    Ok(Value::Array(values.into_values().collect()))
}

fn values(
    rows: &[Value],
    fields: &[PipelineField],
    explicit_limit: Option<usize>,
    max_result_rows: usize,
    cancelled: &AtomicBool,
) -> Result<Value, String> {
    let explicit_limit = explicit_limit.filter(|limit| *limit > 0);
    let capacity = explicit_limit.unwrap_or(max_result_rows);
    let mut items = Vec::new();
    let mut missing = 0u64;
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        for value in selected_values(row, fields) {
            let represented = items
                .len()
                .saturating_add(usize::try_from(missing).unwrap_or(usize::MAX));
            if represented >= capacity {
                if explicit_limit.is_none() {
                    return Err(format!(
                        "LogsQL values exceeds max_result_rows={max_result_rows}"
                    ));
                }
                return Ok(values_envelope(items, missing));
            }
            match value {
                Some(value) => items.push(value.clone()),
                None => missing = missing.saturating_add(1),
            }
        }
    }
    Ok(values_envelope(items, missing))
}

fn values_envelope(items: Vec<Value>, missing: u64) -> Value {
    Value::Object(Map::from_iter([
        ("items".into(), Value::Array(items)),
        ("missing".into(), Value::from(missing)),
    ]))
}

fn numeric_sum(
    rows: &[Value],
    fields: &[PipelineField],
    cancelled: &AtomicBool,
) -> Result<(Value, usize), String> {
    let mut integer_sum = 0i128;
    let mut float_sum = 0.0f64;
    let mut float_mode = false;
    let mut count = 0usize;
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        for number in selected_values(row, fields)
            .into_iter()
            .flatten()
            .filter_map(Value::as_number)
        {
            count += 1;
            if let Some(integer) = exact_integer(number) {
                if float_mode {
                    float_sum += integer as f64;
                } else {
                    integer_sum = integer_sum.saturating_add(integer);
                }
            } else if let Some(value) = number.as_f64() {
                if !float_mode {
                    float_sum = integer_sum as f64;
                    float_mode = true;
                }
                float_sum += value;
            }
        }
    }
    let value = if float_mode {
        finite_number(float_sum)
    } else if let Ok(value) = i64::try_from(integer_sum) {
        Value::from(value)
    } else if let Ok(value) = u64::try_from(integer_sum) {
        Value::from(value)
    } else {
        finite_number(integer_sum as f64)
    };
    Ok((value, count))
}

fn numeric_extreme(
    rows: &[Value],
    fields: &[PipelineField],
    maximum: bool,
    cancelled: &AtomicBool,
) -> Result<Value, String> {
    let mut selected: Option<Number> = None;
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        for number in selected_values(row, fields)
            .into_iter()
            .flatten()
            .filter_map(Value::as_number)
        {
            let replace = selected.as_ref().is_none_or(|current| {
                compare_numbers(number, current).is_some_and(|ordering| {
                    if maximum {
                        ordering == Ordering::Greater
                    } else {
                        ordering == Ordering::Less
                    }
                })
            });
            if replace {
                selected = Some(number.clone());
            }
        }
    }
    Ok(selected.map_or(Value::Null, Value::Number))
}

fn median(
    rows: &[Value],
    fields: &[PipelineField],
    max_state_items: usize,
    cancelled: &AtomicBool,
) -> Result<Value, String> {
    let mut values = Vec::<Number>::new();
    for (index, row) in rows.iter().enumerate() {
        check_periodically(cancelled, index)?;
        for number in selected_values(row, fields)
            .into_iter()
            .flatten()
            .filter_map(Value::as_number)
        {
            if values.len() == max_state_items {
                return Err(format!(
                    "LogsQL median state exceeds max_work_rows={max_state_items}"
                ));
            }
            values.push(number.clone());
        }
    }
    if values.is_empty() {
        return Ok(Value::Null);
    }
    values.sort_by(|left, right| compare_numbers(left, right).unwrap_or(Ordering::Equal));
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        return Ok(Value::Number(values[middle].clone()));
    }
    let left = values[middle - 1].as_f64().unwrap_or(0.0);
    let right = values[middle].as_f64().unwrap_or(0.0);
    let sum = left + right;
    Ok(finite_number(if sum.is_finite() {
        sum / 2.0
    } else {
        left / 2.0 + right / 2.0
    }))
}

fn finite_number(value: f64) -> Value {
    Number::from_f64(value).map_or(Value::Null, Value::Number)
}

fn selected_values<'a>(row: &'a Value, fields: &[PipelineField]) -> Vec<Option<&'a Value>> {
    let Some(object) = row.as_object() else {
        return Vec::new();
    };
    let mut output = Vec::new();
    for field in fields {
        match field {
            PipelineField::Exact { path, .. } => output.push(field_value(row, path)),
            PipelineField::Prefix { prefix } => output.extend(
                object
                    .iter()
                    .filter(|(name, _)| name.starts_with(prefix))
                    .map(|(_, value)| Some(value)),
            ),
            PipelineField::All => output.extend(object.values().map(Some)),
        }
    }
    output
}

fn field_value<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut current = value;
    for segment in path {
        current = match current {
            Value::Object(object) => object.get(segment)?,
            Value::Array(array) => array.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

fn insert_path(
    object: &mut Map<String, Value>,
    path: &[String],
    value: Value,
) -> Result<(), String> {
    let Some((last, parents)) = path.split_last() else {
        return Err("LogsQL projected field path is empty".into());
    };
    let mut current = object;
    for parent in parents {
        let entry = current
            .entry(parent.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        current = entry.as_object_mut().ok_or_else(|| {
            format!("LogsQL projection path {parent:?} conflicts with a scalar field")
        })?;
    }
    current.insert(last.clone(), value);
    Ok(())
}

fn predicate_matches(
    predicate: &LogPredicate,
    row: &Value,
    timestamp_unit: TimestampUnit,
    cancelled: &AtomicBool,
) -> Result<bool, String> {
    ensure_active(cancelled)?;
    if let Some(prefix) = predicate_field_prefix(predicate) {
        let Some(object) = row.as_object() else {
            return Ok(false);
        };
        return row_prefix_predicate_matches(
            object,
            &mut Vec::new(),
            prefix,
            predicate,
            row,
            timestamp_unit,
            cancelled,
        );
    }
    predicate_matches_resolved(predicate, row, timestamp_unit, cancelled, None)
}

fn predicate_matches_resolved(
    predicate: &LogPredicate,
    row: &Value,
    timestamp_unit: TimestampUnit,
    cancelled: &AtomicBool,
    field_override: Option<&LogField>,
) -> Result<bool, String> {
    ensure_active(cancelled)?;
    macro_rules! resolved_field {
        ($field:expr) => {
            resolved_log_field($field, field_override)
        };
    }
    match predicate {
        LogPredicate::True => Ok(true),
        LogPredicate::And(predicates) => {
            for predicate in predicates {
                if !predicate_matches(predicate, row, timestamp_unit, cancelled)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        LogPredicate::Or(predicates) => {
            for predicate in predicates {
                if predicate_matches(predicate, row, timestamp_unit, cancelled)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        LogPredicate::Not(predicate) => Ok(!predicate_matches(
            predicate,
            row,
            timestamp_unit,
            cancelled,
        )?),
        LogPredicate::Word {
            field,
            value,
            case_insensitive,
        } => Ok(field_text(row, resolved_field!(field)).is_some_and(|text| {
            if *case_insensitive {
                word_matches(&text.to_lowercase(), value)
            } else {
                word_matches(text, value)
            }
        })),
        LogPredicate::Phrase {
            field,
            value,
            case_insensitive,
        } => Ok(field_text(row, resolved_field!(field)).is_some_and(|text| {
            if *case_insensitive {
                phrase_matches(&text.to_lowercase(), value)
            } else {
                phrase_matches(text, value)
            }
        })),
        LogPredicate::Prefix {
            field,
            value,
            phrase,
            case_insensitive,
        } => Ok(field_text(row, resolved_field!(field)).is_some_and(|text| {
            if *case_insensitive {
                prefix_matches(&text.to_lowercase(), value, *phrase)
            } else {
                prefix_matches(text, value, *phrase)
            }
        })),
        LogPredicate::Substring {
            field,
            value,
            case_insensitive,
        } => Ok(field_text(row, resolved_field!(field)).is_some_and(|text| {
            if *case_insensitive {
                text.to_lowercase().contains(value)
            } else {
                text.contains(value)
            }
        })),
        LogPredicate::Exact { field, value } => {
            Ok(field_text(row, resolved_field!(field)).is_some_and(|text| text == value))
        }
        LogPredicate::TextualExact { field, value } => Ok(projected_field_matches(
            row,
            resolved_field!(field),
            |text| text == value,
        )),
        LogPredicate::TextualIn { field, values } => Ok(projected_field_matches(
            row,
            resolved_field!(field),
            |text| {
                values
                    .binary_search_by(|candidate| candidate.as_str().cmp(text))
                    .is_ok()
            },
        )),
        LogPredicate::TextualContainsAll { field, values } => {
            let matched = projected_field_matches(row, resolved_field!(field), |text| {
                values.iter().all(|phrase| phrase_matches(text, phrase))
            });
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::TextualContainsAny { field, values } => {
            let matched = projected_field_matches(row, resolved_field!(field), |text| {
                values.iter().any(|phrase| phrase_matches(text, phrase))
            });
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::JsonArrayContainsAny { field, values } => {
            let matched = field_json(row, resolved_field!(field))
                .and_then(Value::as_array)
                .is_some_and(|array| {
                    array
                        .iter()
                        .any(|value| json_array_primitive_in(values, value))
                });
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::Ipv4Range {
            field,
            minimum,
            maximum,
        } => {
            let matched = minimum <= maximum
                && field_text(row, resolved_field!(field))
                    .and_then(parse_ipv4_address)
                    .is_some_and(|address| address >= *minimum && address <= *maximum);
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::Ipv6Range {
            field,
            minimum,
            maximum,
        } => {
            let matched = minimum <= maximum
                && field_text(row, resolved_field!(field))
                    .and_then(parse_ipv6_address)
                    .is_some_and(|address| address >= *minimum && address <= *maximum);
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::StringRange {
            field,
            minimum,
            maximum,
        } => {
            let matched = minimum <= maximum
                && projected_field_matches(row, resolved_field!(field), |text| {
                    text >= minimum.as_str() && text < maximum.as_str()
                });
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::LenRange {
            field,
            minimum,
            maximum,
        } => {
            let matched = minimum <= maximum
                && projected_field_matches(row, resolved_field!(field), |text| {
                    let length = u64::try_from(text.chars().count()).unwrap_or(u64::MAX);
                    length >= *minimum && length <= *maximum
                });
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::FieldCompare {
            left,
            right,
            operator,
        } => {
            let exact_numeric = (!matches!(operator, crate::FieldCompareOp::Equal))
                .then(|| {
                    let left = field_json(row, left)?.as_number()?;
                    let right = field_json(row, right)?.as_number()?;
                    compare_numbers(left, right).map(|ordering| operator.matches(ordering))
                })
                .flatten();
            let matched = exact_numeric.unwrap_or_else(|| {
                projected_field_pair_matches(row, left, right, |left, right| {
                    logsql_field_comparison(left, right, *operator)
                })
            });
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::ExactPrefix { field, value } => Ok(projected_field_matches(
            row,
            resolved_field!(field),
            |text| text.starts_with(value),
        )),
        LogPredicate::TypedExact { field, value } => {
            Ok(field_json(row, resolved_field!(field))
                .is_some_and(|actual| json_equal(actual, value)))
        }
        LogPredicate::Empty { field } => {
            Ok(field_json(row, resolved_field!(field)).is_none_or(is_empty))
        }
        LogPredicate::AnyValue { field } => {
            Ok(field_json(row, resolved_field!(field)).is_some_and(is_nonempty))
        }
        LogPredicate::Numeric {
            field,
            operator,
            value,
        } => Ok(field_json(row, resolved_field!(field))
            .and_then(Value::as_number)
            .and_then(|actual| compare_numbers(actual, value))
            .is_some_and(|ordering| numeric_op_matches(*operator, ordering))),
        LogPredicate::ValueType { field, kind } => Ok(field_json(row, resolved_field!(field))
            .is_some_and(|value| value_is_type(value, *kind))),
        LogPredicate::Timestamp { minimum, maximum } => {
            let timestamp = row
                .get("_time")
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| match timestamp_unit {
                    TimestampUnit::Milliseconds => value.timestamp_millis(),
                    TimestampUnit::Microseconds => value.timestamp_micros(),
                });
            Ok(timestamp.is_some_and(|timestamp| {
                minimum.is_none_or(|minimum| timestamp >= minimum)
                    && maximum.is_none_or(|maximum| timestamp <= maximum)
            }))
        }
        LogPredicate::DayRange {
            start_ns,
            end_ns,
            start_inclusive,
            end_inclusive,
            offset_ns,
        } => {
            let timestamp = row
                .get("_time")
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| match timestamp_unit {
                    TimestampUnit::Milliseconds => value.timestamp_millis(),
                    TimestampUnit::Microseconds => value.timestamp_micros(),
                });
            Ok(timestamp.is_some_and(|timestamp| {
                day_range_matches(
                    timestamp,
                    timestamp_unit,
                    *start_ns,
                    *end_ns,
                    *start_inclusive,
                    *end_inclusive,
                    *offset_ns,
                )
            }))
        }
        LogPredicate::WeekRange {
            start_day,
            end_day,
            offset_ns,
        } => {
            let timestamp = row
                .get("_time")
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| match timestamp_unit {
                    TimestampUnit::Milliseconds => value.timestamp_millis(),
                    TimestampUnit::Microseconds => value.timestamp_micros(),
                });
            Ok(timestamp.is_some_and(|timestamp| {
                week_range_matches(timestamp, timestamp_unit, *start_day, *end_day, *offset_ns)
            }))
        }
        LogPredicate::Regex { field, regex } => {
            let matched =
                field_text(row, resolved_field!(field)).is_some_and(|text| regex.is_match(text));
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::PatternMatch { field, matcher } => {
            let matched =
                projected_field_matches(row, resolved_field!(field), |text| matcher.matches(text));
            ensure_active(cancelled)?;
            Ok(matched)
        }
    }
}

fn predicate_field_prefix(predicate: &LogPredicate) -> Option<&str> {
    let field = match predicate {
        LogPredicate::Word { field, .. }
        | LogPredicate::Phrase { field, .. }
        | LogPredicate::Prefix { field, .. }
        | LogPredicate::Substring { field, .. }
        | LogPredicate::Exact { field, .. }
        | LogPredicate::TextualExact { field, .. }
        | LogPredicate::TextualIn { field, .. }
        | LogPredicate::TextualContainsAll { field, .. }
        | LogPredicate::TextualContainsAny { field, .. }
        | LogPredicate::JsonArrayContainsAny { field, .. }
        | LogPredicate::Ipv4Range { field, .. }
        | LogPredicate::Ipv6Range { field, .. }
        | LogPredicate::StringRange { field, .. }
        | LogPredicate::LenRange { field, .. }
        | LogPredicate::ExactPrefix { field, .. }
        | LogPredicate::TypedExact { field, .. }
        | LogPredicate::Empty { field }
        | LogPredicate::AnyValue { field }
        | LogPredicate::Numeric { field, .. }
        | LogPredicate::ValueType { field, .. }
        | LogPredicate::Regex { field, .. }
        | LogPredicate::PatternMatch { field, .. } => field,
        LogPredicate::True
        | LogPredicate::And(_)
        | LogPredicate::Or(_)
        | LogPredicate::Not(_)
        | LogPredicate::FieldCompare { .. }
        | LogPredicate::Timestamp { .. }
        | LogPredicate::DayRange { .. }
        | LogPredicate::WeekRange { .. } => return None,
    };
    match field {
        LogField::FieldPrefix(prefix) => Some(prefix),
        LogField::Message | LogField::Level | LogField::Time | LogField::Metadata(_) => None,
    }
}

fn resolved_log_field<'a>(
    field: &'a LogField,
    field_override: Option<&'a LogField>,
) -> &'a LogField {
    match field {
        LogField::FieldPrefix(_) => {
            field_override.expect("field-prefix predicates are resolved before evaluation")
        }
        LogField::Message | LogField::Level | LogField::Time | LogField::Metadata(_) => field,
    }
}

#[allow(clippy::too_many_arguments)]
fn row_prefix_predicate_matches(
    object: &Map<String, Value>,
    path: &mut Vec<String>,
    prefix: &str,
    predicate: &LogPredicate,
    row: &Value,
    timestamp_unit: TimestampUnit,
    cancelled: &AtomicBool,
) -> Result<bool, String> {
    for (name, value) in object {
        ensure_active(cancelled)?;
        path.push(name.clone());
        let matched = match value {
            Value::Object(child) => row_prefix_predicate_matches(
                child,
                path,
                prefix,
                predicate,
                row,
                timestamp_unit,
                cancelled,
            )?,
            Value::Null
            | Value::Bool(_)
            | Value::Number(_)
            | Value::String(_)
            | Value::Array(_) => {
                let canonical = path.join(".");
                if !canonical.starts_with(prefix) {
                    false
                } else {
                    let field = match path.as_slice() {
                        [field] if field == "_msg" => LogField::Message,
                        [field] if field == "_time" => LogField::Time,
                        [field] if field == "level" => LogField::Level,
                        _ => LogField::Metadata(path.clone()),
                    };
                    predicate_matches_resolved(
                        predicate,
                        row,
                        timestamp_unit,
                        cancelled,
                        Some(&field),
                    )?
                }
            }
        };
        path.pop();
        if matched {
            return Ok(true);
        }
    }
    Ok(false)
}

fn projected_field_matches(
    row: &Value,
    field: &LogField,
    predicate: impl FnOnce(&str) -> bool,
) -> bool {
    match field_json(row, field) {
        None | Some(Value::Null) => predicate(""),
        Some(Value::String(value)) => predicate(value),
        Some(value) => predicate(&value.to_string()),
    }
}

fn projected_field_pair_matches(
    row: &Value,
    left: &LogField,
    right: &LogField,
    predicate: impl FnOnce(&str, &str) -> bool,
) -> bool {
    let mut predicate = Some(predicate);
    projected_field_matches(row, left, |left| {
        projected_field_matches(row, right, |right| predicate.take().unwrap()(left, right))
    })
}

fn field_json<'a>(row: &'a Value, field: &LogField) -> Option<&'a Value> {
    match field {
        LogField::Message => row.get("_msg"),
        LogField::Level => row.get("level"),
        LogField::Time => row.get("_time"),
        LogField::Metadata(path) => field_value(row, path),
        LogField::FieldPrefix(_) => None,
    }
}

fn field_text<'a>(row: &'a Value, field: &LogField) -> Option<&'a str> {
    field_json(row, field).and_then(Value::as_str)
}

fn is_empty(value: &Value) -> bool {
    matches!(value, Value::Null) || value.as_str().is_some_and(str::is_empty)
}

fn is_nonempty(value: &Value) -> bool {
    !is_empty(value)
}

fn value_is_type(value: &Value, kind: ValueTypeKind) -> bool {
    match (value, kind) {
        (Value::String(_), ValueTypeKind::String)
        | (Value::Bool(_), ValueTypeKind::Bool)
        | (Value::Null, ValueTypeKind::Null)
        | (Value::Array(_), ValueTypeKind::Array)
        | (Value::Object(_), ValueTypeKind::Object)
        | (Value::Number(_), ValueTypeKind::Number) => true,
        (Value::Number(value), ValueTypeKind::Uint64) => value.as_u64().is_some(),
        (Value::Number(value), ValueTypeKind::Int64) => {
            value.as_i64().is_some() && value.as_u64().is_none()
        }
        (Value::Number(value), ValueTypeKind::Float64) => {
            value.as_i64().is_none() && value.as_u64().is_none() && value.as_f64().is_some()
        }
        _ => false,
    }
}

fn numeric_op_matches(operator: NumericOp, ordering: Ordering) -> bool {
    match operator {
        NumericOp::Greater => ordering == Ordering::Greater,
        NumericOp::GreaterOrEqual => ordering != Ordering::Less,
        NumericOp::Less => ordering == Ordering::Less,
        NumericOp::LessOrEqual => ordering != Ordering::Greater,
    }
}

fn compare_numbers(left: &Number, right: &Number) -> Option<Ordering> {
    match (exact_integer(left), exact_integer(right)) {
        (Some(left), Some(right)) => Some(left.cmp(&right)),
        (Some(left), None) => compare_i128_to_f64(left, right.as_f64()?),
        (None, Some(right)) => compare_i128_to_f64(right, left.as_f64()?).map(Ordering::reverse),
        (None, None) => left.as_f64()?.partial_cmp(&right.as_f64()?),
    }
}

fn exact_integer(value: &Number) -> Option<i128> {
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
}

fn compare_i128_to_f64(integer: i128, float: f64) -> Option<Ordering> {
    if !float.is_finite() {
        return None;
    }
    if integer < 0 {
        if float >= 0.0 {
            return Some(Ordering::Less);
        }
        return compare_u128_to_positive_f64(integer.unsigned_abs(), -float).map(Ordering::reverse);
    }
    if float < 0.0 {
        return Some(Ordering::Greater);
    }
    compare_u128_to_positive_f64(integer as u128, float)
}

fn compare_u128_to_positive_f64(integer: u128, float: f64) -> Option<Ordering> {
    if !float.is_finite() || float < 0.0 {
        return None;
    }
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
            return Some(Ordering::Less);
        };
        return Some(integer.cmp(&float_integer));
    }
    let right_shift = u32::try_from(-shift).ok()?;
    if right_shift >= 128 {
        return Some(if integer == 0 {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }
    let whole = significand >> right_shift;
    match integer.cmp(&whole) {
        Ordering::Equal => {
            let mask = (1u128 << right_shift) - 1;
            Some(if significand & mask == 0 {
                Ordering::Equal
            } else {
                Ordering::Less
            })
        }
        ordering => Some(ordering),
    }
}

fn json_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => {
            compare_numbers(left, right) == Some(Ordering::Equal)
        }
        _ => left == right,
    }
}

fn presence_key(value: Option<&Value>) -> String {
    value.map_or_else(|| "0:".into(), value_key)
}

fn value_key(value: &Value) -> String {
    let tag = match value {
        Value::Null => '1',
        Value::Bool(_) => '2',
        Value::Number(_) => '3',
        Value::String(_) => '4',
        Value::Array(_) => '5',
        Value::Object(_) => '6',
    };
    format!("{tag}:{}", serde_json::to_string(value).unwrap_or_default())
}

fn tuple_key(values: &[Option<&Value>]) -> String {
    let mut output = String::new();
    for value in values {
        let key = presence_key(*value);
        output.push_str(&key.len().to_string());
        output.push(':');
        output.push_str(&key);
    }
    output
}

fn display_value(value: Option<&Value>) -> String {
    match value {
        None => String::new(),
        Some(Value::String(value)) => value.clone(),
        Some(value) => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn word_matches(value: &str, expected: &str) -> bool {
    value
        .split(|character: char| !word_character(character))
        .any(|word| word == expected)
}

fn prefix_matches(value: &str, prefix: &str, phrase: bool) -> bool {
    if prefix.is_empty() {
        return false;
    }
    if !phrase {
        return value
            .split(|character: char| !word_character(character))
            .any(|word| word.starts_with(prefix));
    }
    let require_start_boundary = prefix.chars().next().is_some_and(word_character);
    value.match_indices(prefix).any(|(start, _)| {
        !require_start_boundary
            || start == 0
            || value[..start]
                .chars()
                .next_back()
                .is_none_or(|character| !word_character(character))
    })
}

fn phrase_matches(value: &str, phrase: &str) -> bool {
    if phrase.is_empty() {
        return false;
    }
    let require_start_boundary = phrase.chars().next().is_some_and(word_character);
    let require_end_boundary = phrase.chars().next_back().is_some_and(word_character);
    value.match_indices(phrase).any(|(start, matched)| {
        let start_ok = !require_start_boundary
            || start == 0
            || value[..start]
                .chars()
                .next_back()
                .is_none_or(|character| !word_character(character));
        let end = start + matched.len();
        let end_ok = !require_end_boundary
            || end == value.len()
            || value[end..]
                .chars()
                .next()
                .is_none_or(|character| !word_character(character));
        start_ok && end_ok
    })
}

fn word_character(character: char) -> bool {
    character == '_' || character.is_alphanumeric()
}

fn check_periodically(cancelled: &AtomicBool, index: usize) -> Result<(), String> {
    if index & 255 == 0 {
        ensure_active(cancelled)
    } else {
        Ok(())
    }
}

fn ensure_active(cancelled: &AtomicBool) -> Result<(), String> {
    if cancelled.load(AtomicOrdering::Acquire) {
        Err("LogsQL pipeline cancelled".into())
    } else {
        Ok(())
    }
}

fn json_array_primitive_in(values: &[String], value: &Value) -> bool {
    let matches = |candidate: &str| {
        values
            .binary_search_by(|value| value.as_str().cmp(candidate))
            .is_ok()
    };
    match value {
        Value::Null => matches("null"),
        Value::Bool(true) => matches("true"),
        Value::Bool(false) => matches("false"),
        Value::Number(value) => matches(&value.to_string()),
        Value::String(value) => matches(value),
        Value::Array(_) | Value::Object(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn typed_keys_keep_missing_null_empty_and_numbers_distinct() {
        assert!(presence_key(None) < presence_key(Some(&Value::Null)));
        assert_ne!(value_key(&json!(0)), value_key(&json!("0")));
        assert_ne!(value_key(&json!(false)), value_key(&json!("false")));
        assert_ne!(value_key(&json!([])), value_key(&json!({})));
    }

    #[test]
    fn integer_comparison_does_not_cross_the_f64_precision_boundary() {
        let exact = Number::from(9_007_199_254_740_993u64);
        let rounded = Number::from_f64(9_007_199_254_740_992.0).unwrap();
        assert_eq!(compare_numbers(&exact, &rounded), Some(Ordering::Greater));
    }

    #[test]
    fn contains_predicates_observe_pipeline_cancellation() {
        let row = json!({"_msg": "request 42"});
        let cancelled = AtomicBool::new(true);
        for predicate in [
            LogPredicate::Word {
                field: LogField::FieldPrefix(String::new()),
                value: "request".into(),
                case_insensitive: false,
            },
            LogPredicate::TextualContainsAll {
                field: LogField::Message,
                values: vec!["request".into(), "42".into()],
            },
            LogPredicate::TextualContainsAny {
                field: LogField::Message,
                values: vec!["other".into(), "request".into()],
            },
            LogPredicate::JsonArrayContainsAny {
                field: LogField::Metadata(vec!["tags".into()]),
                values: vec!["other".into(), "request".into()],
            },
            LogPredicate::Ipv4Range {
                field: LogField::Message,
                minimum: 0,
                maximum: u32::MAX,
            },
            LogPredicate::Ipv6Range {
                field: LogField::Message,
                minimum: [0; 16],
                maximum: [u8::MAX; 16],
            },
            LogPredicate::StringRange {
                field: LogField::Message,
                minimum: String::new(),
                maximum: "z".into(),
            },
            LogPredicate::LenRange {
                field: LogField::Message,
                minimum: 0,
                maximum: u64::MAX,
            },
            LogPredicate::FieldCompare {
                left: LogField::Message,
                right: LogField::Level,
                operator: crate::FieldCompareOp::LessOrEqual,
            },
            LogPredicate::DayRange {
                start_ns: 0,
                end_ns: 86_400_000_000_000 - 1,
                start_inclusive: true,
                end_inclusive: true,
                offset_ns: 0,
            },
            LogPredicate::WeekRange {
                start_day: 0,
                end_day: 6,
                offset_ns: 0,
            },
        ] {
            assert_eq!(
                predicate_matches(&predicate, &row, TimestampUnit::Microseconds, &cancelled)
                    .unwrap_err(),
                "LogsQL pipeline cancelled"
            );
        }
    }

    #[test]
    fn delete_prefix_observes_cancellation_during_recursive_walk() {
        let mut object = Map::new();
        for index in 0..128 {
            object.insert(format!("drop.{index:03}"), json!(index));
        }
        let cancelled = AtomicBool::new(true);
        assert_eq!(
            delete_field_prefix(&mut object, "drop.", &cancelled).unwrap_err(),
            "LogsQL pipeline cancelled"
        );
    }

    #[test]
    fn median_is_state_bounded_and_avoids_finite_midpoint_overflow() {
        let field = PipelineField::Exact {
            path: vec!["n".into()],
            name: "n".into(),
        };
        let cancelled = AtomicBool::new(false);
        let rows = [json!({"n": f64::MAX}), json!({"n": f64::MAX})];
        assert_eq!(
            median(&rows, std::slice::from_ref(&field), 2, &cancelled).unwrap(),
            json!(f64::MAX)
        );
        assert!(median(&rows, &[field], 1, &cancelled)
            .unwrap_err()
            .contains("max_work_rows=1"));
    }

    #[test]
    fn query_stats_maps_one_request_without_global_counter_deltas() {
        let result = query_stats(
            LogQueryExecutionReport {
                payload_bytes_read: 321,
                processed_blocks: 2,
                processed_entries: 9,
                matched_entries: 4,
                values_read: 27,
                timestamps_read: 9,
                ..LogQueryExecutionReport::default()
            },
            777,
        );
        assert_eq!(result.len(), 14);
        assert_eq!(result["BytesReadColumnsHeaders"], json!("0"));
        assert_eq!(result["BytesReadValues"], json!("321"));
        assert_eq!(result["BytesReadTotal"], json!("321"));
        assert_eq!(result["BlocksProcessed"], json!("2"));
        assert_eq!(result["RowsProcessed"], json!("9"));
        assert_eq!(result["RowsFound"], json!("4"));
        assert_eq!(result["ValuesRead"], json!("27"));
        assert_eq!(result["TimestampsRead"], json!("9"));
        assert_eq!(result["BytesProcessedUncompressedValues"], json!("0"));
        assert_eq!(result["QueryDurationNsecs"], json!("777"));
        assert!(result.values().all(Value::is_string));
    }

    #[test]
    fn query_stats_first_does_not_materialize_discarded_response_rows() {
        let cancelled = AtomicBool::new(false);
        let rows = execute_query_rows(
            vec![QueryRow {
                ts: i64::MAX,
                level: "info".to_owned(),
                message: "not rendered".to_owned(),
                metadata_json: "not decoded".to_owned(),
            }],
            PipelineExecution {
                report: LogQueryExecutionReport {
                    processed_entries: 1,
                    matched_entries: 1,
                    values_read: 3,
                    timestamps_read: 1,
                    ..LogQueryExecutionReport::default()
                },
                operations: &[PipelineOp::QueryStats],
                implicit_result_limit: None,
                rate_window_seconds: None,
                timestamp_unit: TimestampUnit::Microseconds,
                limits: PipelineLimits {
                    max_result_rows: 10,
                    max_state_items: 10,
                    max_state_bytes: 10_000,
                },
                cancelled: &cancelled,
                query_started: Instant::now(),
            },
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["RowsFound"], json!("1"));
        assert_eq!(rows[0]["ValuesRead"], json!("3"));
    }

    #[test]
    fn first_uses_victorialogs_natural_order_and_resets_partition_rank() {
        let exact = |name: &str| PipelineField::Exact {
            path: vec![name.to_owned()],
            name: name.to_owned(),
        };
        let rows = vec![
            json!({"case":"missing","group":"a"}),
            json!({"case":"null","group":"a","n":null}),
            json!({"case":"two","group":"a","n":2}),
            json!({"case":"huge","group":"a","n":9007199254740993u64}),
            json!({"case":"negative","group":"b","n":-2}),
            json!({"case":"zero","group":"b","n":0}),
            json!({"case":"text","group":"b","n":"3"}),
        ];
        let cancelled = AtomicBool::new(false);
        let result = first(
            rows,
            &FirstSpec {
                limit: 2,
                by_fields: vec![
                    crate::logsql::PipelineSortField {
                        field: exact("n"),
                        descending: false,
                    },
                    crate::logsql::PipelineSortField {
                        field: exact("case"),
                        descending: false,
                    },
                ],
                partition_by: vec![exact("group")],
                rank_field: Some(exact("position")),
            },
            PipelineLimits {
                max_result_rows: 10,
                max_state_items: 10,
                max_state_bytes: 10_000,
            },
            &cancelled,
        )
        .unwrap();
        assert_eq!(
            result,
            [
                json!({"case":"missing","group":"a","position":"1"}),
                json!({"case":"null","group":"a","n":null,"position":"2"}),
                json!({"case":"negative","group":"b","n":-2,"position":"1"}),
                json!({"case":"zero","group":"b","n":0,"position":"2"}),
            ]
        );
    }

    #[test]
    fn last_reverses_first_order_and_resets_partition_rank() {
        let exact = |name: &str| PipelineField::Exact {
            path: vec![name.to_owned()],
            name: name.to_owned(),
        };
        let rows = vec![
            json!({"case":"missing","group":"a"}),
            json!({"case":"null","group":"a","n":null}),
            json!({"case":"two","group":"a","n":2}),
            json!({"case":"huge","group":"a","n":9007199254740993u64}),
            json!({"case":"negative","group":"b","n":-2}),
            json!({"case":"zero","group":"b","n":0}),
            json!({"case":"text","group":"b","n":"3"}),
        ];
        let cancelled = AtomicBool::new(false);
        let result = last(
            rows.clone(),
            &FirstSpec {
                limit: 2,
                by_fields: vec![
                    crate::logsql::PipelineSortField {
                        field: exact("n"),
                        descending: false,
                    },
                    crate::logsql::PipelineSortField {
                        field: exact("case"),
                        descending: false,
                    },
                ],
                partition_by: vec![exact("group")],
                rank_field: Some(exact("position")),
            },
            PipelineLimits {
                max_result_rows: 10,
                max_state_items: 10,
                max_state_bytes: 10_000,
            },
            &cancelled,
        )
        .unwrap();
        assert_eq!(
            result,
            [
                json!({"case":"huge","group":"a","n":9007199254740993u64,"position":"1"}),
                json!({"case":"two","group":"a","n":2,"position":"2"}),
                json!({"case":"text","group":"b","n":"3","position":"1"}),
                json!({"case":"zero","group":"b","n":0,"position":"2"}),
            ]
        );

        let descending = last(
            rows,
            &FirstSpec {
                limit: 2,
                by_fields: vec![crate::logsql::PipelineSortField {
                    field: exact("case"),
                    descending: true,
                }],
                partition_by: Vec::new(),
                rank_field: None,
            },
            PipelineLimits {
                max_result_rows: 10,
                max_state_items: 10,
                max_state_bytes: 10_000,
            },
            &cancelled,
        )
        .unwrap();
        assert_eq!(
            descending,
            [
                json!({"case":"huge","group":"a","n":9007199254740993u64}),
                json!({"case":"missing","group":"a"})
            ]
        );
    }

    #[test]
    fn top_counts_textual_groups_orders_ties_and_bounds_state() {
        let exact = |name: &str| PipelineField::Exact {
            path: vec![name.to_owned()],
            name: name.to_owned(),
        };
        let rows = vec![
            json!({"group":"b","value":2}),
            json!({"group":"a","value":"2"}),
            json!({"group":"a","value":null}),
            json!({"group":"a"}),
            json!({"group":"b","value":10}),
            json!({"group":"b","value":10}),
        ];
        let spec = TopSpec {
            limit: 10,
            by_fields: vec![exact("group"), exact("value")],
            hits_field: "total".into(),
            rank_field: Some("position".into()),
        };
        let cancelled = AtomicBool::new(false);
        let limits = PipelineLimits {
            max_result_rows: 10,
            max_state_items: 10,
            max_state_bytes: 10_000,
        };
        assert_eq!(
            top(rows.clone(), &spec, limits, &cancelled).unwrap(),
            [
                json!({"group":"a","total":"2","position":"1"}),
                json!({"group":"b","value":"10","total":"2","position":"2"}),
                json!({"group":"a","value":"2","total":"1","position":"3"}),
                json!({"group":"b","value":"2","total":"1","position":"4"}),
            ]
        );

        assert!(top(
            rows.clone(),
            &spec,
            PipelineLimits {
                max_state_items: 3,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err()
        .contains("max_work_rows=3"));
        assert!(top(
            rows.clone(),
            &TopSpec {
                limit: 3,
                ..spec.clone()
            },
            PipelineLimits {
                max_result_rows: 2,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err()
        .contains("max_result_rows=2"));
        assert!(top(
            rows,
            &spec,
            PipelineLimits {
                max_state_bytes: 1,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err()
        .contains("max_response_bytes=1"));

        cancelled.store(true, AtomicOrdering::Release);
        assert_eq!(
            top(Vec::new(), &spec, limits, &cancelled).unwrap_err(),
            "LogsQL pipeline cancelled"
        );
    }

    #[test]
    fn uniq_groups_textually_filters_resets_overflow_hits_and_bounds_state() {
        let exact = |name: &str| PipelineField::Exact {
            path: vec![name.to_owned()],
            name: name.to_owned(),
        };
        let rows = vec![
            json!({"group":"b","value":2}),
            json!({"group":"a","value":"2"}),
            json!({"group":"a","value":null}),
            json!({"group":"a"}),
            json!({"group":"b","value":10}),
            json!({"group":"b","value":10}),
        ];
        let spec = UniqSpec {
            by_fields: vec![exact("group"), exact("value")],
            filter: None,
            hits_field: Some("total".into()),
            limit: None,
        };
        let cancelled = AtomicBool::new(false);
        let limits = PipelineLimits {
            max_result_rows: 10,
            max_state_items: 10,
            max_state_bytes: 10_000,
        };
        assert_eq!(
            uniq(rows.clone(), &spec, limits, &cancelled).unwrap(),
            [
                json!({"group":"a","total":"2"}),
                json!({"group":"a","value":"2","total":"1"}),
                json!({"group":"b","value":"10","total":"2"}),
                json!({"group":"b","value":"2","total":"1"}),
            ]
        );

        let overflow = uniq(
            rows.clone(),
            &UniqSpec {
                limit: Some(2),
                ..spec.clone()
            },
            limits,
            &cancelled,
        )
        .unwrap();
        assert_eq!(overflow.len(), 2);
        assert!(overflow.iter().all(|row| row["total"] == "0"));

        assert_eq!(
            uniq(
                rows.clone(),
                &UniqSpec {
                    by_fields: vec![exact("value")],
                    filter: Some("2".into()),
                    hits_field: None,
                    limit: Some(0),
                },
                limits,
                &cancelled,
            )
            .unwrap(),
            [json!({"value":"2"})]
        );
        assert!(uniq(
            rows.clone(),
            &spec,
            PipelineLimits {
                max_state_items: 3,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err()
        .contains("max_work_rows=3"));
        assert!(uniq(
            rows.clone(),
            &UniqSpec {
                limit: Some(3),
                ..spec.clone()
            },
            PipelineLimits {
                max_result_rows: 2,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err()
        .contains("max_result_rows=2"));
        assert!(uniq(
            rows,
            &spec,
            PipelineLimits {
                max_state_bytes: 1,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err()
        .contains("max_response_bytes=1"));

        cancelled.store(true, AtomicOrdering::Release);
        assert_eq!(
            uniq(Vec::new(), &spec, limits, &cancelled).unwrap_err(),
            "LogsQL pipeline cancelled"
        );
    }

    #[test]
    fn facets_flattens_counts_filters_and_bounds_state() {
        let rows = vec![
            json!({
                "constant":"same",
                "group":"a",
                "probe":null,
                "nested":{"leaf":"x"},
                "long":"short"
            }),
            json!({
                "constant":"same",
                "group":"a",
                "probe":"",
                "nested":{"leaf":"x"},
                "long":"too-long"
            }),
            json!({"constant":"same","group":"b","probe":0}),
        ];
        let spec = FacetsSpec {
            limit: 1,
            max_values_per_field: 2,
            max_value_len: 5,
            keep_const_fields: false,
        };
        let cancelled = AtomicBool::new(false);
        let limits = PipelineLimits {
            max_result_rows: 10,
            max_state_items: 10,
            max_state_bytes: 10_000,
        };
        assert_eq!(
            facets(rows.clone(), &spec, limits, &cancelled).unwrap(),
            [
                json!({"field_name":"group","field_value":"a","hits":"2"}),
                json!({"field_name":"nested.leaf","field_value":"x","hits":"2"}),
                json!({"field_name":"probe","field_value":"0","hits":"1"}),
            ]
        );
        assert!(facets(
            rows.clone(),
            &FacetsSpec {
                keep_const_fields: true,
                ..spec.clone()
            },
            limits,
            &cancelled,
        )
        .unwrap()
        .contains(&json!({"field_name":"constant","field_value":"same","hits":"3"})));

        assert!(facets(
            rows.clone(),
            &spec,
            PipelineLimits {
                max_state_items: 2,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err()
        .contains("max_work_rows=2"));
        assert!(facets(
            rows.clone(),
            &spec,
            PipelineLimits {
                max_result_rows: 2,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err()
        .contains("max_result_rows=2"));
        assert!(facets(
            rows,
            &spec,
            PipelineLimits {
                max_state_bytes: 1,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err()
        .contains("max_response_bytes=1"));

        cancelled.store(true, AtomicOrdering::Release);
        assert_eq!(
            facets(Vec::new(), &spec, limits, &cancelled).unwrap_err(),
            "LogsQL pipeline cancelled"
        );
    }

    #[test]
    fn coalesce_uses_textual_flattened_sources_and_bounds_temporary_state() {
        let exact = |name: &str| PipelineField::Exact {
            path: name.split('.').map(str::to_owned).collect(),
            name: name.to_owned(),
        };
        let spec = CoalesceSpec {
            sources: vec![
                exact("probe"),
                PipelineField::Prefix {
                    prefix: "nested.".into(),
                },
                exact("fallback"),
            ],
            destination: exact("selected"),
            default_value: "default".into(),
        };
        let rows = vec![
            json!({"probe":null,"nested":{"a":"","b":"nested"},"fallback":"last"}),
            json!({"probe":0,"fallback":"last"}),
            json!({"probe":false,"fallback":"last"}),
            json!({"probe":[1,"x"],"fallback":"last"}),
            json!({"probe":{"child":"object-is-flattened"},"fallback":"last"}),
            json!({"probe":"","fallback":""}),
        ];
        let limits = PipelineLimits {
            max_result_rows: 10,
            max_state_items: 10,
            max_state_bytes: 10_000,
        };
        let cancelled = AtomicBool::new(false);
        assert_eq!(
            coalesce(rows.clone(), &spec, limits, &cancelled).unwrap(),
            [
                json!({"probe":null,"nested":{"a":"","b":"nested"},"fallback":"last","selected":"nested"}),
                json!({"probe":0,"fallback":"last","selected":"0"}),
                json!({"probe":false,"fallback":"last","selected":"false"}),
                json!({"probe":[1,"x"],"fallback":"last","selected":"[1,\"x\"]"}),
                json!({"probe":{"child":"object-is-flattened"},"fallback":"last","selected":"last"}),
                json!({"probe":"","fallback":"","selected":"default"}),
            ]
        );
        assert!(coalesce(
            rows.clone(),
            &spec,
            PipelineLimits {
                max_state_bytes: 1,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err()
        .contains("max_response_bytes=1"));

        let conflict = CoalesceSpec {
            destination: exact("probe.child"),
            ..spec.clone()
        };
        assert!(coalesce(
            vec![json!({"probe":"scalar"})],
            &conflict,
            limits,
            &cancelled,
        )
        .unwrap_err()
        .contains("conflicts with a scalar field"));

        cancelled.store(true, AtomicOrdering::Release);
        assert_eq!(
            coalesce(rows, &spec, limits, &cancelled).unwrap_err(),
            "LogsQL pipeline cancelled"
        );
    }

    #[test]
    fn copy_preserves_rich_values_sequences_prefixes_and_bounds_temporary_state() {
        let exact = |name: &str| PipelineField::Exact {
            path: name.split('.').map(str::to_owned).collect(),
            name: name.to_owned(),
        };
        let prefix = |prefix: &str| PipelineField::Prefix {
            prefix: prefix.to_owned(),
        };
        let spec = CopySpec {
            pairs: vec![
                (exact("probe"), exact("selected")),
                (exact("selected"), exact("chained")),
                (prefix("nested."), prefix("copied.")),
                (exact("missing"), exact("absent")),
            ],
        };
        let rows = vec![json!({
            "probe": 2,
            "nested": {"a": "one", "b": [1, "x"]},
            "null_value": null,
            "empty_value": ""
        })];
        let limits = PipelineLimits {
            max_result_rows: 10,
            max_state_items: 100,
            max_state_bytes: 10_000,
        };
        let cancelled = AtomicBool::new(false);
        assert_eq!(
            copy_fields(rows.clone(), &spec, limits, &cancelled).unwrap(),
            [json!({
                "probe": 2,
                "selected": 2,
                "chained": 2,
                "nested": {"a": "one", "b": [1, "x"]},
                "copied": {"a": "one", "b": [1, "x"]},
                "null_value": null,
                "empty_value": "",
                "absent": ""
            })]
        );

        assert_eq!(
            copy_fields(
                rows.clone(),
                &CopySpec {
                    pairs: vec![(prefix("nested."), exact("last"))],
                },
                limits,
                &cancelled,
            )
            .unwrap(),
            [json!({
                "probe": 2,
                "nested": {"a": "one", "b": [1, "x"]},
                "null_value": null,
                "empty_value": "",
                "last": [1, "x"]
            })]
        );
        assert_eq!(
            copy_fields(
                rows.clone(),
                &CopySpec {
                    pairs: vec![(PipelineField::All, PipelineField::All)],
                },
                limits,
                &cancelled,
            )
            .unwrap(),
            rows
        );
        assert_eq!(
            copy_fields(
                vec![json!({"case":"copy","probe":2})],
                &CopySpec {
                    pairs: vec![
                        (prefix("case"), PipelineField::All),
                        (prefix("probe"), prefix("copied")),
                        (prefix("copied"), prefix("chained")),
                    ],
                },
                limits,
                &cancelled,
            )
            .unwrap(),
            [json!({
                "case":"copy",
                "probe":2,
                "":"copy",
                "copied":2,
                "chained":2
            })]
        );
        assert_eq!(
            copy_fields(
                rows.clone(),
                &CopySpec {
                    pairs: vec![(exact("probe"), prefix("literal"))],
                },
                limits,
                &cancelled,
            )
            .unwrap()[0]["literal*"],
            2
        );

        let state_error = copy_fields(
            rows.clone(),
            &spec,
            PipelineLimits {
                max_state_bytes: 1,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err();
        assert!(state_error.contains("LogsQL copy"), "{state_error}");
        assert!(
            state_error.contains("max_response_bytes=1"),
            "{state_error}"
        );
        let work_error = copy_fields(
            rows.clone(),
            &CopySpec {
                pairs: vec![(PipelineField::All, prefix("copied."))],
            },
            PipelineLimits {
                max_state_items: 1,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err();
        assert!(work_error.contains("LogsQL copy"), "{work_error}");
        assert!(work_error.contains("max_work_rows=1"), "{work_error}");

        for destination in [exact("probe.child"), exact("nested")] {
            let error = copy_fields(
                rows.clone(),
                &CopySpec {
                    pairs: vec![(exact("probe"), destination)],
                },
                limits,
                &cancelled,
            )
            .unwrap_err();
            assert!(error.contains("destination conflict"), "{error}");
        }

        cancelled.store(true, AtomicOrdering::Release);
        assert_eq!(
            copy_fields(rows, &spec, limits, &cancelled).unwrap_err(),
            "LogsQL pipeline cancelled"
        );
    }

    #[test]
    fn rename_moves_rich_values_sequentially_and_bounds_temporary_state() {
        let exact = |name: &str| PipelineField::Exact {
            path: name.split('.').map(str::to_owned).collect(),
            name: name.to_owned(),
        };
        let prefix = |prefix: &str| PipelineField::Prefix {
            prefix: prefix.to_owned(),
        };
        let rows = vec![json!({
            "probe": 2,
            "flag": false,
            "nested": {"a": "one", "b": [1, "x"]},
            "null_value": null,
            "empty_value": ""
        })];
        let limits = PipelineLimits {
            max_result_rows: 10,
            max_state_items: 100,
            max_state_bytes: 10_000,
        };
        let cancelled = AtomicBool::new(false);
        let spec = RenameSpec {
            pairs: vec![
                (exact("probe"), exact("selected")),
                (exact("selected"), exact("chained")),
                (prefix("nested."), prefix("moved.")),
                (exact("missing"), exact("absent")),
            ],
        };
        assert_eq!(
            rename_fields(rows.clone(), &spec, limits, &cancelled).unwrap(),
            [json!({
                "chained": 2,
                "flag": false,
                "moved": {"a": "one", "b": [1, "x"]},
                "null_value": null,
                "empty_value": "",
                "absent": ""
            })]
        );

        assert_eq!(
            rename_fields(
                rows.clone(),
                &RenameSpec {
                    pairs: vec![(exact("nested"), exact("parent"))],
                },
                limits,
                &cancelled,
            )
            .unwrap(),
            [json!({
                "probe": 2,
                "flag": false,
                "nested": {"a": "one", "b": [1, "x"]},
                "null_value": null,
                "empty_value": "",
                "parent": ""
            })]
        );
        assert_eq!(
            rename_fields(
                vec![json!({"a": 1, "b": 2})],
                &RenameSpec {
                    pairs: vec![
                        (exact("a"), exact("temporary")),
                        (exact("b"), exact("a")),
                        (exact("temporary"), exact("b")),
                    ],
                },
                limits,
                &cancelled,
            )
            .unwrap(),
            [json!({"a": 2, "b": 1})]
        );
        assert_eq!(
            rename_fields(
                vec![json!({"a": 1})],
                &RenameSpec {
                    pairs: vec![(exact("a"), exact("b")), (exact("a"), exact("c"))],
                },
                limits,
                &cancelled,
            )
            .unwrap(),
            [json!({"b": 1, "c": ""})]
        );
        assert_eq!(
            rename_fields(
                rows.clone(),
                &RenameSpec {
                    pairs: vec![(prefix("nested."), exact("last"))],
                },
                limits,
                &cancelled,
            )
            .unwrap(),
            [json!({
                "probe": 2,
                "flag": false,
                "null_value": null,
                "empty_value": "",
                "last": [1, "x"]
            })]
        );
        assert_eq!(
            rename_fields(
                rows.clone(),
                &RenameSpec {
                    pairs: vec![(PipelineField::All, PipelineField::All)],
                },
                limits,
                &cancelled,
            )
            .unwrap(),
            rows
        );
        assert_eq!(
            rename_fields(
                vec![json!({"case": "rename", "probe": 2})],
                &RenameSpec {
                    pairs: vec![(prefix("case"), PipelineField::All)],
                },
                limits,
                &cancelled,
            )
            .unwrap(),
            [json!({"": "rename", "probe": 2})]
        );
        assert_eq!(
            rename_fields(
                rows.clone(),
                &RenameSpec {
                    pairs: vec![(exact("probe"), prefix("literal"))],
                },
                limits,
                &cancelled,
            )
            .unwrap()[0]["literal*"],
            2
        );

        let state_error = rename_fields(
            rows.clone(),
            &spec,
            PipelineLimits {
                max_state_bytes: 1,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err();
        assert!(state_error.contains("LogsQL rename"), "{state_error}");
        assert!(
            state_error.contains("max_response_bytes=1"),
            "{state_error}"
        );
        let work_error = rename_fields(
            rows.clone(),
            &RenameSpec {
                pairs: vec![(PipelineField::All, prefix("moved."))],
            },
            PipelineLimits {
                max_state_items: 1,
                ..limits
            },
            &cancelled,
        )
        .unwrap_err();
        assert!(work_error.contains("LogsQL rename"), "{work_error}");
        assert!(work_error.contains("max_work_rows=1"), "{work_error}");

        for destination in [exact("probe.child"), exact("nested")] {
            let error = rename_fields(
                vec![json!({"case": "rename", "probe": 2, "nested": {"a": 1}})],
                &RenameSpec {
                    pairs: vec![(exact("case"), destination)],
                },
                limits,
                &cancelled,
            )
            .unwrap_err();
            assert!(
                error.contains("LogsQL rename destination conflict"),
                "{error}"
            );
        }

        cancelled.store(true, AtomicOrdering::Release);
        assert_eq!(
            rename_fields(rows, &spec, limits, &cancelled).unwrap_err(),
            "LogsQL pipeline cancelled"
        );
    }

    #[test]
    fn first_observes_cancellation_and_state_and_result_bounds() {
        let exact = |name: &str| PipelineField::Exact {
            path: vec![name.to_owned()],
            name: name.to_owned(),
        };
        let spec = FirstSpec {
            limit: 2,
            by_fields: vec![crate::logsql::PipelineSortField {
                field: exact("n"),
                descending: false,
            }],
            partition_by: vec![exact("group")],
            rank_field: None,
        };
        let rows = vec![
            json!({"group":"a","n":1}),
            json!({"group":"a","n":2}),
            json!({"group":"b","n":1}),
            json!({"group":"b","n":2}),
        ];
        let limits = PipelineLimits {
            max_result_rows: 10,
            max_state_items: 10,
            max_state_bytes: 10_000,
        };
        assert_eq!(
            first(rows.clone(), &spec, limits, &AtomicBool::new(true)).unwrap_err(),
            "LogsQL pipeline cancelled"
        );
        let state_error = first(
            rows.clone(),
            &spec,
            PipelineLimits {
                max_state_bytes: 1,
                ..limits
            },
            &AtomicBool::new(false),
        )
        .unwrap_err();
        assert!(
            state_error.contains("max_response_bytes=1"),
            "{state_error}"
        );
        let result_error = first(
            rows,
            &spec,
            PipelineLimits {
                max_result_rows: 3,
                ..limits
            },
            &AtomicBool::new(false),
        )
        .unwrap_err();
        assert!(result_error.contains("max_result_rows=3"), "{result_error}");
    }
}
