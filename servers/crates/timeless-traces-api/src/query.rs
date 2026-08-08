use std::collections::{BTreeMap, BTreeSet};
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
    DashboardTrace { trace_id: String },
    DashboardSearch(DashboardSearchQuery),
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchParams {
    pub service: Option<String>,
    pub operation: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<String>,
    pub min_duration: Option<String>,
    pub max_duration: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub(crate) struct DashboardSearchParams {
    pub name: Option<String>,
    pub service: Option<String>,
    pub kind: Option<String>,
    pub status: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<String>,
    pub offset: Option<String>,
    pub order: Option<String>,
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

#[derive(Clone)]
pub(crate) struct DashboardSearchQuery {
    name: Option<String>,
    service: Option<String>,
    kind: Option<String>,
    status: Option<String>,
    since_ns: Option<i64>,
    until_ns: Option<i64>,
    limit: i64,
    offset: i64,
    descending: bool,
}

pub(crate) struct ReadOutput {
    pub body: Vec<u8>,
    pub traces: u64,
    pub spans: u64,
    pub rows: u64,
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
            Self::Trace { .. } | Self::DashboardTrace { .. } => ReadKind::Trace,
            Self::Search(_) | Self::DashboardSearch(_) => ReadKind::Search,
        }
    }

    pub(crate) fn dashboard_search(params: DashboardSearchParams) -> Result<Self, String> {
        let parse_exact = |field: &str, value: Option<&str>, default: i64| {
            value.map_or(Ok(default), |value| {
                value
                    .parse::<i64>()
                    .map_err(|_| format!("invalid dashboard {field} {value:?}"))
            })
        };
        let limit = parse_exact("limit", params.limit.as_deref(), DEFAULT_LIMIT)?;
        let offset = parse_exact("offset", params.offset.as_deref(), 0)?;
        if !(1..=100).contains(&limit) {
            return Err("dashboard limit must be between 1 and 100".into());
        }
        if offset < 0 {
            return Err("dashboard offset must be non-negative".into());
        }
        let descending = match params.order.as_deref().unwrap_or("desc") {
            "desc" => true,
            "asc" => false,
            value => return Err(format!("invalid dashboard order {value:?}")),
        };
        let kind = nonempty(params.kind);
        if kind.as_deref().is_some_and(|value| {
            !matches!(
                value,
                "internal" | "server" | "client" | "producer" | "consumer"
            )
        }) {
            return Err("invalid dashboard span kind".into());
        }
        let status = nonempty(params.status);
        if status
            .as_deref()
            .is_some_and(|value| !matches!(value, "unset" | "ok" | "error"))
        {
            return Err("invalid dashboard span status".into());
        }
        let since_ns = optional_exact_integer("since", params.since.as_deref())?;
        let until_ns = optional_exact_integer("until", params.until.as_deref())?;
        Ok(Self::DashboardSearch(DashboardSearchQuery {
            name: nonempty(params.name),
            service: nonempty(params.service),
            kind,
            status,
            since_ns,
            until_ns,
            limit,
            offset,
            descending,
        }))
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
                        duration_ns,attributes,status_description,events,resource,instrumentation_scope,links,trace_state,trace_flags,dropped_attributes_count,dropped_events_count,dropped_links_count,resource_schema_url,scope_schema_url,resource_dropped_attributes_count,scope_dropped_attributes_count \
                   FROM traces WHERE trace_id=?1 ORDER BY start_ts,span_id",
                vec![SqlValue::Text(trace_id.clone())],
                cancelled,
            )?;
            let trace = jaeger_trace(&trace_id, &spans, cancelled)?;
            envelope(vec![trace], 1, 1, spans.len() as u64)
        }
        ReadRequest::Search(search) => execute_search(conn, &search, cancelled),
        ReadRequest::DashboardTrace { trace_id } => {
            let spans = query_spans(
                conn,
                "SELECT trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,\
                        duration_ns,attributes,status_description,events,resource,instrumentation_scope,links,trace_state,trace_flags,dropped_attributes_count,dropped_events_count,dropped_links_count,resource_schema_url,scope_schema_url,resource_dropped_attributes_count,scope_dropped_attributes_count \
                   FROM traces WHERE trace_id=?1 ORDER BY start_ts,span_id",
                vec![SqlValue::Text(trace_id)],
                cancelled,
            )?;
            dashboard_trace_envelope(spans)
        }
        ReadRequest::DashboardSearch(search) => execute_dashboard_search(conn, &search, cancelled),
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
                duration_ns,attributes,status_description,events,resource,instrumentation_scope,links,trace_state,trace_flags,dropped_attributes_count,dropped_events_count,dropped_links_count,resource_schema_url,scope_schema_url,resource_dropped_attributes_count,scope_dropped_attributes_count \
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

fn execute_dashboard_search(
    conn: &Connection,
    search: &DashboardSearchQuery,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let mut sql = String::from(
        "SELECT trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,\
                duration_ns,attributes,status_description,events,resource,instrumentation_scope,links,trace_state,trace_flags,dropped_attributes_count,dropped_events_count,dropped_links_count,resource_schema_url,scope_schema_url,resource_dropped_attributes_count,scope_dropped_attributes_count \
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
        if let Some(value) = &search.kind {
            add("kind=?", SqlValue::Text(value.clone()));
        }
        if let Some(value) = &search.status {
            add("status=?", SqlValue::Text(value.clone()));
        }
        if let Some(value) = search.since_ns {
            add("start_ts>=?", SqlValue::Integer(value));
        }
        if let Some(value) = search.until_ns {
            add("start_ts<=?", SqlValue::Integer(value));
        }
    }
    if search.descending {
        sql.push_str(" ORDER BY start_ts DESC,span_id DESC");
    } else {
        sql.push_str(" ORDER BY start_ts ASC,span_id ASC");
    }

    // The native product's name filter also searches string-valued span
    // attributes. JSON text LIKE would produce false positives, so keep this
    // predicate in a bounded host loop. The public vtab cursor streams blocks
    // and the loop stops after OFFSET + LIMIT + 1 exact matches.
    let needed = search.offset.saturating_add(search.limit).saturating_add(1);
    if search.name.is_none() {
        sql.push_str(" LIMIT ? OFFSET ?");
        values.push(SqlValue::Integer(search.limit.saturating_add(1)));
        values.push(SqlValue::Integer(search.offset));
    }
    let mut statement = conn
        .prepare(&sql)
        .map_err(|error| format!("prepare dashboard spans query: {error}"))?;
    let mut rows = statement
        .query(params_from_iter(values))
        .map_err(|error| format!("execute dashboard spans query: {error}"))?;
    let mut matched = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("read dashboard span row: {error}"))?
    {
        check_cancelled(cancelled)?;
        let span = decode_span_row(row)?;
        if search
            .name
            .as_deref()
            .is_none_or(|pattern| dashboard_name_matches(&span, pattern))
        {
            matched.push(span);
            if search.name.is_some() && matched.len() as i64 >= needed {
                break;
            }
        }
    }

    let (page, has_more) = if search.name.is_some() {
        let page = matched
            .into_iter()
            .skip(search.offset as usize)
            .collect::<Vec<_>>();
        let has_more = page.len() > search.limit as usize;
        (
            page.into_iter().take(search.limit as usize).collect(),
            has_more,
        )
    } else {
        let has_more = matched.len() > search.limit as usize;
        (
            matched.into_iter().take(search.limit as usize).collect(),
            has_more,
        )
    };
    dashboard_search_envelope(page, search.limit, search.offset, has_more)
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
    links: Vec<Value>,
    trace_state: String,
    trace_flags: i64,
    dropped_attributes_count: i64,
    dropped_events_count: i64,
    dropped_links_count: i64,
    resource_schema_url: String,
    scope_schema_url: String,
    resource_dropped_attributes_count: i64,
    scope_dropped_attributes_count: i64,
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
        output.push(decode_span_row(row)?);
    }
    Ok(output)
}

fn decode_span_row(row: &rusqlite::Row<'_>) -> Result<SpanRow, String> {
    macro_rules! column {
        ($index:expr) => {
            row.get($index)
                .map_err(|error| format!("decode traces span column {}: {error}", $index))?
        };
    }
    let attributes = json_object(column!(9), "attributes")?;
    let events = json_array(column!(11), "events")?;
    let resource = json_object(column!(12), "resource")?;
    let instrumentation_scope = json_object(column!(13), "instrumentation_scope")?;
    let links = json_array(column!(14), "links")?;
    let parent_span_id: Option<Vec<u8>> = column!(2);
    let service: String = column!(4);
    Ok(SpanRow {
        trace_id: hex_blob(column!(0), 16, "trace_id")?,
        span_id: hex_blob(column!(1), 8, "span_id")?,
        parent_span_id: parent_span_id
            .map(|value| hex_bytes(&value, 8, "parent_span_id"))
            .transpose()?,
        name: column!(3),
        service: if service.is_empty() {
            "unknown".to_owned()
        } else {
            service
        },
        kind: column!(5),
        status: column!(6),
        start_ts: column!(7),
        duration_ns: column!(8),
        attributes,
        status_description: column!(10),
        events,
        resource,
        instrumentation_scope,
        links,
        trace_state: column!(15),
        trace_flags: column!(16),
        dropped_attributes_count: column!(17),
        dropped_events_count: column!(18),
        dropped_links_count: column!(19),
        resource_schema_url: column!(20),
        scope_schema_url: column!(21),
        resource_dropped_attributes_count: column!(22),
        scope_dropped_attributes_count: column!(23),
    })
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
        let mut references = span
            .parent_span_id
            .as_ref()
            .map_or_else(Vec::new, |parent| {
                vec![json!({"refType":"CHILD_OF", "traceID":trace_id, "spanID":parent})]
            });
        for link in &span.links {
            let link = link
                .as_object()
                .ok_or_else(|| "stored link is not a JSON object".to_string())?;
            let link_trace_id = stored_link_id(link, "trace_id", 32)?;
            let link_span_id = stored_link_id(link, "span_id", 16)?;
            references.push(json!({
                "refType": "FOLLOWS_FROM",
                "traceID": link_trace_id,
                "spanID": link_span_id
            }));
        }
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

fn stored_link_id<'a>(
    link: &'a Map<String, Value>,
    key: &str,
    length: usize,
) -> Result<&'a str, String> {
    let value = link
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("stored link {key} is not a string"))?;
    if value.len() != length || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!(
            "stored link {key} must be a {length}-character hexadecimal string"
        ));
    }
    Ok(value)
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

fn dashboard_trace_envelope(spans: Vec<SpanRow>) -> Result<ReadOutput, String> {
    let span_count = spans.len() as u64;
    let traces = u64::from(!spans.is_empty());
    let spans = spans
        .into_iter()
        .map(dashboard_span)
        .collect::<Result<Vec<_>, _>>()?;
    let body = serde_json::to_vec(&json!({"spans": spans}))
        .map_err(|error| format!("encode dashboard trace response: {error}"))?;
    Ok(ReadOutput {
        body,
        traces,
        spans: span_count,
        rows: span_count,
    })
}

fn dashboard_search_envelope(
    spans: Vec<SpanRow>,
    limit: i64,
    offset: i64,
    has_more: bool,
) -> Result<ReadOutput, String> {
    let span_count = spans.len() as u64;
    let trace_count = spans
        .iter()
        .map(|span| span.trace_id.as_str())
        .collect::<BTreeSet<_>>()
        .len() as u64;
    let total = offset
        .saturating_add(spans.len() as i64)
        .saturating_add(i64::from(has_more));
    let entries = spans
        .into_iter()
        .map(dashboard_span)
        .collect::<Result<Vec<_>, _>>()?;
    let body = serde_json::to_vec(&json!({
        "entries": entries,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": has_more
    }))
    .map_err(|error| format!("encode dashboard search response: {error}"))?;
    Ok(ReadOutput {
        body,
        traces: trace_count,
        spans: span_count,
        rows: span_count,
    })
}

fn dashboard_span(span: SpanRow) -> Result<Value, String> {
    let end_time = span
        .start_ts
        .checked_add(span.duration_ns)
        .ok_or_else(|| "stored span end timestamp overflows i64".to_string())?;
    let status_message = if span.status_description.is_empty() {
        Value::Null
    } else {
        Value::String(span.status_description)
    };
    Ok(json!({
        "trace_id": span.trace_id,
        "span_id": span.span_id,
        "parent_span_id": span.parent_span_id,
        "name": span.name,
        "kind": span.kind,
        "start_time": span.start_ts,
        "end_time": end_time,
        "duration_ns": span.duration_ns,
        "status": span.status,
        "status_message": status_message,
        "attributes": span.attributes,
        "events": span.events,
        "resource": span.resource,
        "instrumentation_scope": span.instrumentation_scope,
        "links": span.links,
        "trace_state": span.trace_state,
        "trace_flags": span.trace_flags,
        "dropped_attributes_count": span.dropped_attributes_count,
        "dropped_events_count": span.dropped_events_count,
        "dropped_links_count": span.dropped_links_count,
        "resource_schema_url": span.resource_schema_url,
        "scope_schema_url": span.scope_schema_url,
        "resource_dropped_attributes_count": span.resource_dropped_attributes_count,
        "scope_dropped_attributes_count": span.scope_dropped_attributes_count
    }))
}

fn dashboard_name_matches(span: &SpanRow, pattern: &str) -> bool {
    let pattern = pattern.to_lowercase();
    span.name.to_lowercase().contains(&pattern)
        || span.attributes.values().any(|value| {
            value
                .as_str()
                .is_some_and(|value| value.to_lowercase().contains(&pattern))
        })
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
        rows: total,
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

fn optional_exact_integer(field: &str, value: Option<&str>) -> Result<Option<i64>, String> {
    value
        .map(|value| {
            value
                .parse::<i64>()
                .map_err(|_| format!("invalid dashboard {field} {value:?}"))
        })
        .transpose()
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
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
