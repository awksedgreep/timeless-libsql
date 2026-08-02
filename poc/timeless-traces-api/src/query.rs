use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection};
use serde::Deserialize;
use serde_json::{json, Map, Value};

const DEFAULT_LIMIT: i64 = 100;

#[derive(Clone, Copy)]
pub(crate) enum ReadKind {
    Services,
    Operations,
    Trace,
    Search,
}

#[derive(Clone)]
pub(crate) enum ReadRequest {
    Services,
    Operations { service: String },
    Trace { trace_id: String },
    Search(SearchQuery),
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SearchParams {
    pub service: Option<String>,
    pub operation: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<String>,
    pub min_duration: Option<String>,
    pub max_duration: Option<String>,
}

#[derive(Clone)]
pub(crate) struct SearchQuery {
    service: Option<String>,
    operation: Option<String>,
    start_ns: Option<i64>,
    end_ns: Option<i64>,
    limit: i64,
    min_duration_ns: Option<i64>,
    max_duration_ns: Option<i64>,
}

pub(crate) struct ReadOutput {
    pub body: Vec<u8>,
    pub traces: u64,
    pub spans: u64,
}

impl ReadRequest {
    pub(crate) fn search(params: SearchParams) -> Result<Self, String> {
        let start_ns = parse_optional_integer(params.start.as_deref())
            .and_then(|value| value.checked_mul(1_000));
        let end_ns = parse_optional_integer(params.end.as_deref())
            .and_then(|value| value.checked_mul(1_000));
        let limit = parse_optional_integer(params.limit.as_deref()).unwrap_or(DEFAULT_LIMIT);
        if limit < 0 {
            return Err("limit must be non-negative".into());
        }
        let min_duration_ns = params
            .min_duration
            .as_deref()
            .map(parse_duration)
            .transpose()?;
        let max_duration_ns = params
            .max_duration
            .as_deref()
            .map(parse_duration)
            .transpose()?;
        Ok(Self::Search(SearchQuery {
            service: params.service,
            operation: params.operation,
            start_ns,
            end_ns,
            limit,
            min_duration_ns,
            max_duration_ns,
        }))
    }

    pub(crate) fn kind(&self) -> ReadKind {
        match self {
            Self::Services => ReadKind::Services,
            Self::Operations { .. } => ReadKind::Operations,
            Self::Trace { .. } => ReadKind::Trace,
            Self::Search(_) => ReadKind::Search,
        }
    }
}

pub(crate) fn execute(
    conn: &Connection,
    request: ReadRequest,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    match request {
        ReadRequest::Services => string_list(
            conn,
            "SELECT value FROM timeless_trace_services('traces') ORDER BY value",
            [],
            cancelled,
        ),
        ReadRequest::Operations { service } => string_list(
            conn,
            "SELECT value FROM timeless_trace_operations('traces', ?1) ORDER BY value",
            [service],
            cancelled,
        ),
        ReadRequest::Trace { trace_id } => {
            let spans = query_spans(
                conn,
                "SELECT trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,\
                        duration_ns,attributes,status_description,events,resource,instrumentation_scope \
                   FROM traces WHERE trace_id=?1 ORDER BY start_ts,span_id",
                vec![SqlValue::Text(trace_id.clone())],
                cancelled,
            )?;
            let trace = jaeger_trace(&trace_id, &spans, cancelled)?;
            envelope(vec![trace], 1, 1, spans.len() as u64)
        }
        ReadRequest::Search(search) => execute_search(conn, &search, cancelled),
    }
}

fn string_list<I>(
    conn: &Connection,
    sql: &str,
    params: I,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String>
where
    I: rusqlite::Params,
{
    let mut statement = conn
        .prepare(sql)
        .map_err(|error| format!("prepare Jaeger discovery query: {error}"))?;
    let mut rows = statement
        .query(params)
        .map_err(|error| format!("execute Jaeger discovery query: {error}"))?;
    let mut values = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("read Jaeger discovery row: {error}"))?
    {
        check_cancelled(cancelled)?;
        values.push(
            row.get::<_, String>(0)
                .map_err(|error| format!("decode Jaeger discovery row: {error}"))?,
        );
    }
    let total = values.len() as u64;
    envelope(values, total, 0, 0)
}

fn execute_search(
    conn: &Connection,
    search: &SearchQuery,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let mut sql = String::from(
        "SELECT trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,\
                duration_ns,attributes,status_description,events,resource,instrumentation_scope \
           FROM traces",
    );
    let mut values = Vec::new();
    {
        let mut add = |clause: &str, value: SqlValue| {
            sql.push_str(if values.is_empty() {
                " WHERE "
            } else {
                " AND "
            });
            sql.push_str(clause);
            values.push(value);
        };
        if let Some(value) = &search.service {
            add("service=?", SqlValue::Text(value.clone()));
        }
        if let Some(value) = &search.operation {
            add("name=?", SqlValue::Text(value.clone()));
        }
        if let Some(value) = search.start_ns {
            add("start_ts>=?", SqlValue::Integer(value));
        }
        if let Some(value) = search.end_ns {
            add("start_ts<=?", SqlValue::Integer(value));
        }
        if let Some(value) = search.min_duration_ns {
            add("duration_ns>=?", SqlValue::Integer(value));
        }
        if let Some(value) = search.max_duration_ns {
            add("duration_ns<=?", SqlValue::Integer(value));
        }
    }
    // Compatibility contract: newest spans first, then apply the span limit,
    // then group. This can return an incomplete trace and is intentionally
    // not relabeled as a Jaeger trace limit.
    sql.push_str(" ORDER BY start_ts DESC,span_id DESC LIMIT ?");
    values.push(SqlValue::Integer(search.limit));
    let spans = query_spans(conn, &sql, values, cancelled)?;
    let mut grouped: BTreeMap<String, Vec<SpanRow>> = BTreeMap::new();
    for span in spans {
        check_cancelled(cancelled)?;
        grouped.entry(span.trace_id.clone()).or_default().push(span);
    }
    let span_count = grouped.values().map(Vec::len).sum::<usize>() as u64;
    let mut traces = Vec::with_capacity(grouped.len());
    for (trace_id, mut spans) in grouped {
        spans.sort_by(|a, b| (a.start_ts, &a.span_id).cmp(&(b.start_ts, &b.span_id)));
        traces.push(jaeger_trace(&trace_id, &spans, cancelled)?);
    }
    let trace_count = traces.len() as u64;
    envelope(traces, trace_count, trace_count, span_count)
}

#[derive(Clone)]
struct SpanRow {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    name: String,
    service: String,
    kind: String,
    status: String,
    start_ts: i64,
    duration_ns: i64,
    attributes: Map<String, Value>,
    status_description: String,
    events: Vec<Value>,
    resource: Map<String, Value>,
    #[allow(dead_code)]
    instrumentation_scope: Map<String, Value>,
}

fn query_spans(
    conn: &Connection,
    sql: &str,
    values: Vec<SqlValue>,
    cancelled: &AtomicBool,
) -> Result<Vec<SpanRow>, String> {
    let mut statement = conn
        .prepare(sql)
        .map_err(|error| format!("prepare Jaeger spans query: {error}"))?;
    let mut rows = statement
        .query(params_from_iter(values))
        .map_err(|error| format!("execute Jaeger spans query: {error}"))?;
    let mut output = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("read Jaeger span row: {error}"))?
    {
        check_cancelled(cancelled)?;
        macro_rules! column {
            ($index:expr) => {
                row.get($index)
                    .map_err(|error| format!("decode Jaeger span column {}: {error}", $index))?
            };
        }
        let attributes = json_object(column!(9), "attributes")?;
        let events = json_array(column!(11), "events")?;
        let resource = json_object(column!(12), "resource")?;
        let instrumentation_scope = json_object(column!(13), "instrumentation_scope")?;
        let parent_span_id: Option<Vec<u8>> = column!(2);
        output.push(SpanRow {
            trace_id: hex_blob(column!(0), 16, "trace_id")?,
            span_id: hex_blob(column!(1), 8, "span_id")?,
            parent_span_id: parent_span_id
                .map(|value| hex_bytes(&value, 8, "parent_span_id"))
                .transpose()?,
            name: column!(3),
            service: column!(4),
            kind: column!(5),
            status: column!(6),
            start_ts: column!(7),
            duration_ns: column!(8),
            attributes,
            status_description: column!(10),
            events,
            resource,
            instrumentation_scope,
        });
    }
    Ok(output)
}

fn jaeger_trace(
    trace_id: &str,
    spans: &[SpanRow],
    cancelled: &AtomicBool,
) -> Result<Value, String> {
    let mut service_resources: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
    for span in spans {
        check_cancelled(cancelled)?;
        let resource = service_resources.entry(span.service.clone()).or_default();
        resource.extend(span.resource.clone());
    }
    let mut process_ids = BTreeMap::new();
    let mut processes = Map::new();
    for (index, (service, resource)) in service_resources.into_iter().enumerate() {
        check_cancelled(cancelled)?;
        let id = format!("p{}", index + 1);
        process_ids.insert(service.clone(), id.clone());
        let tags = resource
            .into_iter()
            .filter(|(key, _)| key != "service.name")
            .map(|(key, value)| jaeger_tag(key, value))
            .collect::<Vec<_>>();
        processes.insert(id, json!({"serviceName": service, "tags": tags}));
    }
    let mut rendered = Vec::with_capacity(spans.len());
    for span in spans {
        check_cancelled(cancelled)?;
        let references = span
            .parent_span_id
            .as_ref()
            .map_or_else(Vec::new, |parent| {
                vec![json!({"refType":"CHILD_OF", "traceID":trace_id, "spanID":parent})]
            });
        let mut tags = vec![
            json!({"key":"span.kind", "type":"string", "value":span.kind}),
            json!({"key":"otel.status_code", "type":"string", "value":span.status.to_uppercase()}),
        ];
        if !span.status_description.is_empty() {
            tags.push(json!({"key":"otel.status_description", "type":"string", "value":span.status_description}));
        }
        tags.extend(
            span.attributes
                .clone()
                .into_iter()
                .map(|(key, value)| jaeger_tag(key, value)),
        );
        let logs = span
            .events
            .iter()
            .map(jaeger_log)
            .collect::<Result<Vec<_>, _>>()?;
        rendered.push(json!({
            "traceID": trace_id,
            "spanID": span.span_id,
            "operationName": span.name,
            "references": references,
            "startTime": span.start_ts / 1_000,
            "duration": span.duration_ns / 1_000,
            "tags": tags,
            "logs": logs,
            "processID": process_ids.get(&span.service).map(String::as_str).unwrap_or("p1"),
            "warnings": Value::Null
        }));
    }
    Ok(json!({
        "traceID": trace_id,
        "spans": rendered,
        "processes": processes,
        "warnings": Value::Null
    }))
}

fn jaeger_log(event: &Value) -> Result<Value, String> {
    let event = event
        .as_object()
        .ok_or_else(|| "stored event is not a JSON object".to_string())?;
    let name = event.get("name").and_then(Value::as_str).unwrap_or("");
    let timestamp = event.get("timestamp").and_then(Value::as_i64).unwrap_or(0) / 1_000;
    let mut fields = vec![json!({"key":"event", "type":"string", "value":name})];
    if let Some(attributes) = event.get("attributes").and_then(Value::as_object) {
        fields.extend(
            attributes
                .clone()
                .into_iter()
                .map(|(key, value)| jaeger_tag(key, value)),
        );
    }
    Ok(json!({"timestamp":timestamp, "fields":fields}))
}

fn jaeger_tag(key: String, value: Value) -> Value {
    match value {
        Value::String(value) => json!({"key":key, "type":"string", "value":value}),
        Value::Bool(value) => json!({"key":key, "type":"bool", "value":value}),
        Value::Number(value) if value.is_i64() || value.is_u64() => {
            json!({"key":key, "type":"int64", "value":value})
        }
        Value::Number(value) => json!({"key":key, "type":"float64", "value":value}),
        value => json!({"key":key, "type":"string", "value":value.to_string()}),
    }
}

fn envelope(
    data: impl serde::Serialize,
    total: u64,
    traces: u64,
    spans: u64,
) -> Result<ReadOutput, String> {
    let data =
        serde_json::to_value(data).map_err(|error| format!("encode Jaeger data: {error}"))?;
    let body = serde_json::to_vec(&json!({
        "data": data,
        "errors": Value::Null,
        "limit": 0,
        "offset": 0,
        "total": total
    }))
    .map_err(|error| format!("encode Jaeger response: {error}"))?;
    Ok(ReadOutput {
        body,
        traces,
        spans,
    })
}

fn parse_duration(value: &str) -> Result<i64, String> {
    for (suffix, multiplier) in [("ms", 1_000_000), ("us", 1_000), ("s", 1_000_000_000)] {
        if let Some(number) = value.strip_suffix(suffix) {
            return parse_integer_prefix(number)
                .and_then(|number| number.checked_mul(multiplier))
                .ok_or_else(|| format!("invalid Jaeger duration {value:?}"));
        }
    }
    parse_integer_prefix(value).ok_or_else(|| format!("invalid Jaeger duration {value:?}"))
}

fn parse_optional_integer(value: Option<&str>) -> Option<i64> {
    value.and_then(parse_integer_prefix)
}

fn parse_integer_prefix(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    let start = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let digits = bytes[start..]
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    (digits > 0)
        .then(|| value[..start + digits].parse().ok())
        .flatten()
}

fn json_object(text: String, field: &str) -> Result<Map<String, Value>, String> {
    serde_json::from_str::<Value>(&text)
        .map_err(|error| format!("decode stored {field}: {error}"))?
        .as_object()
        .cloned()
        .ok_or_else(|| format!("stored {field} is not a JSON object"))
}

fn json_array(text: String, field: &str) -> Result<Vec<Value>, String> {
    serde_json::from_str::<Value>(&text)
        .map_err(|error| format!("decode stored {field}: {error}"))?
        .as_array()
        .cloned()
        .ok_or_else(|| format!("stored {field} is not a JSON array"))
}

fn hex_blob(value: Vec<u8>, width: usize, field: &str) -> Result<String, String> {
    hex_bytes(&value, width, field)
}

fn hex_bytes(value: &[u8], width: usize, field: &str) -> Result<String, String> {
    if value.len() != width {
        return Err(format!(
            "stored {field} has {} bytes, expected {width}",
            value.len()
        ));
    }
    let mut out = String::with_capacity(width * 2);
    for byte in value {
        use std::fmt::Write;
        write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(out)
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), String> {
    if cancelled.load(Ordering::Acquire) {
        Err("query cancelled".into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn established_duration_and_partial_integer_rules_are_pinned() {
        assert_eq!(parse_duration("100us").unwrap(), 100_000);
        assert_eq!(parse_duration("2ms").unwrap(), 2_000_000);
        assert_eq!(parse_duration("3s").unwrap(), 3_000_000_000);
        assert_eq!(parse_duration("42").unwrap(), 42);
        assert_eq!(parse_optional_integer(Some("17junk")), Some(17));
        assert!(parse_duration("oops").is_err());
    }
}
