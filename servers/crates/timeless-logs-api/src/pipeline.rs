//! Bounded LogsQL row transforms owned by the Rust logs API.
//!
//! This module deliberately receives only rows returned by the public logs
//! virtual table. It contains no extension syntax and cannot access storage
//! shadow tables.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Number, Value};

use crate::logsql::{parse_ipv4_address, PipelineField, PipelineOp, StatsExpression, StatsKind};
use crate::storage::QueryRow;
use crate::{LogField, LogPredicate, NumericOp, TimestampUnit, ValueTypeKind};

#[derive(Clone, Copy)]
pub(crate) struct PipelineLimits {
    pub max_result_rows: usize,
    pub max_state_items: usize,
}

pub(crate) fn execute_query_rows(
    rows: Vec<QueryRow>,
    operations: &[PipelineOp],
    implicit_result_limit: Option<usize>,
    rate_window_seconds: Option<f64>,
    timestamp_unit: TimestampUnit,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    let rows = rows
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            check_periodically(cancelled, index)?;
            response_row(row, timestamp_unit)
        })
        .collect::<Result<Vec<_>, _>>()?;
    execute(
        rows,
        operations,
        implicit_result_limit,
        rate_window_seconds,
        timestamp_unit,
        limits,
        cancelled,
    )
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

fn format_timestamp(ts: i64, timestamp_unit: TimestampUnit) -> Result<String, String> {
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
    operations: &[PipelineOp],
    implicit_result_limit: Option<usize>,
    rate_window_seconds: Option<f64>,
    timestamp_unit: TimestampUnit,
    limits: PipelineLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, String> {
    for operation in operations {
        ensure_active(cancelled)?;
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
                limits.max_result_rows,
                cancelled,
            )?,
            PipelineOp::FieldNames {
                filter,
                result_name,
            } => field_names(
                &rows,
                filter.as_deref(),
                result_name,
                limits.max_result_rows,
                cancelled,
            )?,
            PipelineOp::Project(fields) => project(rows, fields, cancelled)?,
            PipelineOp::Filter(predicate) => filter(rows, predicate, timestamp_unit, cancelled)?,
            PipelineOp::Stats(expressions) => vec![Value::Object(stats(
                &rows,
                expressions,
                rate_window_seconds,
                limits,
                cancelled,
            )?)],
        };
    }
    if let Some(limit) = implicit_result_limit {
        rows.truncate(limit.min(limits.max_result_rows));
    }
    if rows.len() > limits.max_result_rows {
        return Err(format!(
            "LogsQL pipeline result exceeds max_result_rows={}",
            limits.max_result_rows
        ));
    }
    ensure_active(cancelled)?;
    Ok(rows)
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
        } => Ok(field_text(row, field).is_some_and(|text| {
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
        } => Ok(field_text(row, field).is_some_and(|text| {
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
        } => Ok(field_text(row, field).is_some_and(|text| {
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
        } => Ok(field_text(row, field).is_some_and(|text| {
            if *case_insensitive {
                text.to_lowercase().contains(value)
            } else {
                text.contains(value)
            }
        })),
        LogPredicate::Exact { field, value } => {
            Ok(field_text(row, field).is_some_and(|text| text == value))
        }
        LogPredicate::TextualExact { field, value } => {
            Ok(projected_field_matches(row, field, |text| text == value))
        }
        LogPredicate::TextualIn { field, values } => {
            Ok(projected_field_matches(row, field, |text| {
                values
                    .binary_search_by(|candidate| candidate.as_str().cmp(text))
                    .is_ok()
            }))
        }
        LogPredicate::TextualContainsAll { field, values } => {
            let matched = projected_field_matches(row, field, |text| {
                values.iter().all(|phrase| phrase_matches(text, phrase))
            });
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::TextualContainsAny { field, values } => {
            let matched = projected_field_matches(row, field, |text| {
                values.iter().any(|phrase| phrase_matches(text, phrase))
            });
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::JsonArrayContainsAny { field, values } => {
            let matched = field_json(row, field)
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
                && field_text(row, field)
                    .and_then(parse_ipv4_address)
                    .is_some_and(|address| address >= *minimum && address <= *maximum);
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::ExactPrefix { field, value } => {
            Ok(projected_field_matches(row, field, |text| {
                text.starts_with(value)
            }))
        }
        LogPredicate::TypedExact { field, value } => {
            Ok(field_json(row, field).is_some_and(|actual| json_equal(actual, value)))
        }
        LogPredicate::Empty { field } => Ok(field_json(row, field).is_none_or(is_empty)),
        LogPredicate::AnyValue { field } => Ok(field_json(row, field).is_some_and(is_nonempty)),
        LogPredicate::Numeric {
            field,
            operator,
            value,
        } => Ok(field_json(row, field)
            .and_then(Value::as_number)
            .and_then(|actual| compare_numbers(actual, value))
            .is_some_and(|ordering| numeric_op_matches(*operator, ordering))),
        LogPredicate::ValueType { field, kind } => {
            Ok(field_json(row, field).is_some_and(|value| value_is_type(value, *kind)))
        }
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
        LogPredicate::Regex { field, regex } => {
            let matched = field_text(row, field).is_some_and(|text| regex.is_match(text));
            ensure_active(cancelled)?;
            Ok(matched)
        }
        LogPredicate::PatternMatch { field, matcher } => {
            let matched = projected_field_matches(row, field, |text| matcher.matches(text));
            ensure_active(cancelled)?;
            Ok(matched)
        }
    }
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

fn field_json<'a>(row: &'a Value, field: &LogField) -> Option<&'a Value> {
    match field {
        LogField::Message => row.get("_msg"),
        LogField::Level => row.get("level"),
        LogField::Metadata(path) => field_value(row, path),
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
        ] {
            assert_eq!(
                predicate_matches(&predicate, &row, TimestampUnit::Microseconds, &cancelled)
                    .unwrap_err(),
                "LogsQL pipeline cancelled"
            );
        }
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
}
