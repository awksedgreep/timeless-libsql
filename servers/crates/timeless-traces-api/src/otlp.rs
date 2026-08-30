use std::collections::BTreeMap;
use std::io::Read;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use flate2::read::GzDecoder;
use prost::Message;
use serde_json::{Map, Number, Value};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Span {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    name: String,
    kind: u8,
    status: u8,
    status_description: String,
    start_ts: i64,
    duration_ns: i64,
    attributes: String,
    events: String,
    resource: String,
    scope: String,
    links: String,
    trace_state: String,
    trace_flags: u32,
    dropped_attributes_count: u32,
    dropped_events_count: u32,
    dropped_links_count: u32,
    resource_schema_url: String,
    scope_schema_url: String,
    resource_dropped_attributes_count: u32,
    scope_dropped_attributes_count: u32,
}

struct ResourceContext {
    attributes: Map<String, Value>,
    schema_url: String,
    dropped_attributes_count: u32,
}

struct ScopeContext {
    value: Map<String, Value>,
    schema_url: String,
    dropped_attributes_count: u32,
}

pub(crate) fn parse_json(body: &[u8]) -> Result<Vec<Span>, String> {
    let root: Value = serde_json::from_slice(body).map_err(|_| "invalid JSON".to_owned())?;
    let root = object(&root, "request")?;
    let resource_spans = root
        .get("resourceSpans")
        .ok_or_else(|| "missing resourceSpans field".to_owned())?;
    let resource_spans = array(resource_spans, "resourceSpans")?;
    let mut out = Vec::new();
    for (resource_index, resource_spans) in resource_spans.iter().enumerate() {
        let resource_spans = object(resource_spans, &format!("resourceSpans[{resource_index}]"))?;
        let resource_value = match resource_spans.get("resource") {
            None | Some(Value::Null) => (Map::new(), 0),
            Some(resource) => {
                let resource = object(resource, "resource")?;
                (
                    json_attributes(resource.get("attributes"), "resource.attributes")?,
                    json_u32(
                        resource.get("droppedAttributesCount"),
                        "resource.droppedAttributesCount",
                    )?,
                )
            }
        };
        let resource = ResourceContext {
            attributes: resource_value.0,
            schema_url: optional_string(
                resource_spans.get("schemaUrl"),
                "resourceSpans.schemaUrl",
            )?
            .unwrap_or_default(),
            dropped_attributes_count: resource_value.1,
        };
        let scope_spans = match resource_spans.get("scopeSpans") {
            None | Some(Value::Null) => &[][..],
            Some(value) => array(value, "scopeSpans")?,
        };
        for (scope_index, scope_spans) in scope_spans.iter().enumerate() {
            let scope_spans = object(scope_spans, &format!("scopeSpans[{scope_index}]"))?;
            let (scope_value, scope_dropped_attributes_count) =
                json_scope(scope_spans.get("scope"))?;
            let scope = ScopeContext {
                value: scope_value,
                schema_url: optional_string(scope_spans.get("schemaUrl"), "scopeSpans.schemaUrl")?
                    .unwrap_or_default(),
                dropped_attributes_count: scope_dropped_attributes_count,
            };
            let spans = match scope_spans.get("spans") {
                None | Some(Value::Null) => &[][..],
                Some(value) => array(value, "spans")?,
            };
            for (span_index, span) in spans.iter().enumerate() {
                out.push(json_span(
                    span,
                    &resource,
                    &scope,
                    &format!("spans[{span_index}]"),
                )?);
            }
        }
    }
    Ok(out)
}

pub(crate) fn parse_protobuf(body: &[u8]) -> Result<Vec<Span>, String> {
    let request = proto::ExportTraceServiceRequest::decode(body)
        .map_err(|_| "invalid protobuf".to_owned())?;
    let mut out = Vec::new();
    for (resource_index, resource_spans) in request.resource_spans.into_iter().enumerate() {
        let resource_value = resource_spans.resource.unwrap_or_default();
        let resource = ResourceContext {
            attributes: protobuf_attributes(
                resource_value.attributes,
                &format!("resourceSpans[{resource_index}].resource.attributes"),
            )?,
            schema_url: resource_spans.schema_url,
            dropped_attributes_count: resource_value.dropped_attributes_count,
        };
        for (scope_index, scope_spans) in resource_spans.scope_spans.into_iter().enumerate() {
            let (scope_value, scope_dropped_attributes_count) = protobuf_scope(scope_spans.scope)?;
            let scope = ScopeContext {
                value: scope_value,
                schema_url: scope_spans.schema_url,
                dropped_attributes_count: scope_dropped_attributes_count,
            };
            for (span_index, span) in scope_spans.spans.into_iter().enumerate() {
                out.push(protobuf_span(
                    span,
                    &resource,
                    &scope,
                    &format!("resourceSpans[{resource_index}].scopeSpans[{scope_index}].spans[{span_index}]"),
                )?);
            }
        }
    }
    Ok(out)
}

pub(crate) fn declared_json_spans(body: &[u8]) -> usize {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|root| root.get("resourceSpans").and_then(Value::as_array).cloned())
        .map(|resources| {
            resources
                .iter()
                .filter_map(Value::as_object)
                .filter_map(|resource| resource.get("scopeSpans"))
                .filter_map(Value::as_array)
                .flat_map(|scopes| scopes.iter())
                .filter_map(Value::as_object)
                .filter_map(|scope| scope.get("spans"))
                .filter_map(Value::as_array)
                .map(Vec::len)
                .sum()
        })
        .unwrap_or(0)
}

pub(crate) fn declared_protobuf_spans(body: &[u8]) -> usize {
    proto::ExportTraceServiceRequest::decode(body)
        .map(|request| {
            request
                .resource_spans
                .iter()
                .flat_map(|resource| resource.scope_spans.iter())
                .map(|scope| scope.spans.len())
                .sum()
        })
        .unwrap_or(0)
}

pub(crate) fn gunzip_bounded(body: &[u8], limit: usize) -> Result<Vec<u8>, String> {
    let maximum = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    let mut decoder = GzDecoder::new(body).take(maximum);
    let mut decoded = Vec::with_capacity(body.len().min(limit));
    decoder
        .read_to_end(&mut decoded)
        .map_err(|_| "invalid protobuf".to_owned())?;
    if decoded.len() > limit {
        return Err(format!("decompressed protobuf exceeds {limit} bytes"));
    }
    Ok(decoded)
}

pub(crate) fn encode_rich_batch(spans: &[Span]) -> Result<Vec<u8>, String> {
    let count = u32::try_from(spans.len())
        .map_err(|_| "OTLP request contains more than u32::MAX spans".to_owned())?;
    let mut out = Vec::with_capacity(8 + spans.len().saturating_mul(128));
    out.extend_from_slice(&[0x03, 0, 0, 0]);
    out.extend_from_slice(&count.to_le_bytes());
    for span in spans {
        out.extend_from_slice(&span.trace_id);
    }
    for span in spans {
        out.extend_from_slice(&span.span_id);
    }
    for span in spans {
        out.extend_from_slice(&span.parent_span_id.unwrap_or([0; 8]));
    }
    text_column(&mut out, spans.iter().map(|span| span.name.as_str()))?;
    // The extension applies span-attribute/resource service precedence. This
    // fallback matches the product for spans without service.name.
    text_column(&mut out, spans.iter().map(|_| "unknown"))?;
    out.extend(spans.iter().map(|span| span.kind));
    out.extend(spans.iter().map(|span| span.status));
    for span in spans {
        out.extend_from_slice(&span.start_ts.to_le_bytes());
    }
    for span in spans {
        out.extend_from_slice(&span.duration_ns.to_le_bytes());
    }
    text_column(&mut out, spans.iter().map(|span| span.attributes.as_str()))?;
    text_column(
        &mut out,
        spans.iter().map(|span| span.status_description.as_str()),
    )?;
    text_column(&mut out, spans.iter().map(|span| span.events.as_str()))?;
    text_column(&mut out, spans.iter().map(|span| span.resource.as_str()))?;
    text_column(&mut out, spans.iter().map(|span| span.scope.as_str()))?;
    text_column(&mut out, spans.iter().map(|span| span.links.as_str()))?;
    text_column(&mut out, spans.iter().map(|span| span.trace_state.as_str()))?;
    u32_column(&mut out, spans.iter().map(|span| span.trace_flags));
    u32_column(
        &mut out,
        spans.iter().map(|span| span.dropped_attributes_count),
    );
    u32_column(&mut out, spans.iter().map(|span| span.dropped_events_count));
    u32_column(&mut out, spans.iter().map(|span| span.dropped_links_count));
    text_column(
        &mut out,
        spans.iter().map(|span| span.resource_schema_url.as_str()),
    )?;
    text_column(
        &mut out,
        spans.iter().map(|span| span.scope_schema_url.as_str()),
    )?;
    u32_column(
        &mut out,
        spans
            .iter()
            .map(|span| span.resource_dropped_attributes_count),
    );
    u32_column(
        &mut out,
        spans.iter().map(|span| span.scope_dropped_attributes_count),
    );
    Ok(out)
}

fn json_span(
    value: &Value,
    resource: &ResourceContext,
    scope: &ScopeContext,
    context: &str,
) -> Result<Span, String> {
    let span = object(value, context)?;
    let trace_id = json_id::<16>(span.get("traceId"), "traceId")?;
    let span_id = json_id::<8>(span.get("spanId"), "spanId")?;
    let parent_span_id = match span.get("parentSpanId") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if value.is_empty() => None,
        value => Some(json_id::<8>(value, "parentSpanId")?),
    };
    let start = json_time(span.get("startTimeUnixNano"), "startTimeUnixNano")?;
    let end = json_time(span.get("endTimeUnixNano"), "endTimeUnixNano")?;
    if end < start {
        return Err(format!(
            "{context}: endTimeUnixNano precedes startTimeUnixNano"
        ));
    }
    let duration = end
        .checked_sub(start)
        .ok_or_else(|| format!("{context}: span duration overflows signed 64-bit storage"))?;
    let name = optional_string(span.get("name"), "name")?.unwrap_or_default();
    let kind = json_kind(span.get("kind"));
    let (status, status_description) = json_status(span.get("status"))?;
    let attributes = json_attributes(span.get("attributes"), "span.attributes")?;
    let events = json_events(span.get("events"))?;
    let links = json_links(span.get("links"))?;
    Ok(Span {
        trace_id,
        span_id,
        parent_span_id,
        name,
        kind,
        status,
        status_description,
        start_ts: start,
        duration_ns: duration,
        attributes: canonical_object(attributes)?,
        events: canonical_array(events)?,
        resource: canonical_object(resource.attributes.clone())?,
        scope: canonical_object(scope.value.clone())?,
        links: canonical_array(links)?,
        trace_state: optional_string(span.get("traceState"), "traceState")?.unwrap_or_default(),
        trace_flags: json_u32(span.get("flags"), "flags")?,
        dropped_attributes_count: json_u32(
            span.get("droppedAttributesCount"),
            "droppedAttributesCount",
        )?,
        dropped_events_count: json_u32(span.get("droppedEventsCount"), "droppedEventsCount")?,
        dropped_links_count: json_u32(span.get("droppedLinksCount"), "droppedLinksCount")?,
        resource_schema_url: resource.schema_url.clone(),
        scope_schema_url: scope.schema_url.clone(),
        resource_dropped_attributes_count: resource.dropped_attributes_count,
        scope_dropped_attributes_count: scope.dropped_attributes_count,
    })
}

fn protobuf_span(
    span: proto::Span,
    resource: &ResourceContext,
    scope: &ScopeContext,
    context: &str,
) -> Result<Span, String> {
    let trace_id = bytes_id::<16>(&span.trace_id, "trace_id")?;
    let span_id = bytes_id::<8>(&span.span_id, "span_id")?;
    let parent_span_id = if span.parent_span_id.is_empty() {
        None
    } else {
        Some(bytes_id::<8>(&span.parent_span_id, "parent_span_id")?)
    };
    let start = native_time(span.start_time_unix_nano, "start_time_unix_nano")?;
    let end = native_time(span.end_time_unix_nano, "end_time_unix_nano")?;
    if end < start {
        return Err(format!(
            "{context}: end_time_unix_nano precedes start_time_unix_nano"
        ));
    }
    let duration = end
        .checked_sub(start)
        .ok_or_else(|| format!("{context}: span duration overflows signed 64-bit storage"))?;
    let attributes = protobuf_attributes(span.attributes, "span.attributes")?;
    let events = span
        .events
        .into_iter()
        .enumerate()
        .map(|(index, event)| protobuf_event(event, index))
        .collect::<Result<Vec<_>, _>>()?;
    let links = span
        .links
        .into_iter()
        .enumerate()
        .map(|(index, link)| protobuf_link(link, index))
        .collect::<Result<Vec<_>, _>>()?;
    let (status, status_description) = span.status.map_or((0, String::new()), |status| {
        (protobuf_status(status.code), status.message)
    });
    Ok(Span {
        trace_id,
        span_id,
        parent_span_id,
        name: span.name,
        kind: protobuf_kind(span.kind),
        status,
        status_description,
        start_ts: start,
        duration_ns: duration,
        attributes: canonical_object(attributes)?,
        events: canonical_array(events)?,
        resource: canonical_object(resource.attributes.clone())?,
        scope: canonical_object(scope.value.clone())?,
        links: canonical_array(links)?,
        trace_state: span.trace_state,
        trace_flags: span.flags,
        dropped_attributes_count: span.dropped_attributes_count,
        dropped_events_count: span.dropped_events_count,
        dropped_links_count: span.dropped_links_count,
        resource_schema_url: resource.schema_url.clone(),
        scope_schema_url: scope.schema_url.clone(),
        resource_dropped_attributes_count: resource.dropped_attributes_count,
        scope_dropped_attributes_count: scope.dropped_attributes_count,
    })
}

fn json_id<const N: usize>(value: Option<&Value>, name: &str) -> Result<[u8; N], String> {
    let Some(Value::String(value)) = value else {
        return Err(format!("{name} must be a {}-character hex string", N * 2));
    };
    if value.len() != N * 2 {
        return Err(format!("{name} must be a {}-character hex string", N * 2));
    }
    let mut out = [0_u8; N];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let pair = std::str::from_utf8(pair).unwrap();
        out[index] = u8::from_str_radix(pair, 16)
            .map_err(|_| format!("{name} must be a {}-character hex string", N * 2))?;
    }
    Ok(out)
}

fn bytes_id<const N: usize>(value: &[u8], name: &str) -> Result<[u8; N], String> {
    value
        .try_into()
        .map_err(|_| format!("{name} must contain exactly {N} bytes"))
}

/// The dashboard query surface's row shape (`query::dashboard_span`), built
/// from an ingest-side span instead of a stored one, so a live subscriber and
/// a search return the same fields for the same span.
///
/// `service` is carried as well, which the stored surface leaves implicit in
/// the resource attributes: the tail matches filters against this row rather
/// than against SQL, so the field a filter names has to be present on it.
///
/// Attribute bundles travel as JSON text and are parsed back here. Text that
/// does not parse yields no row rather than a row that misstates the span --
/// the span itself is still stored normally.
pub(crate) fn tail_row(span: &Span) -> Option<Value> {
    let end_time = span.start_ts.checked_add(span.duration_ns)?;
    let resource: Value = serde_json::from_str(&span.resource).ok()?;
    let service = resource
        .get("service.name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();

    let mut row = Map::new();
    row.insert("trace_id".into(), Value::String(hex_id(&span.trace_id)));
    row.insert("span_id".into(), Value::String(hex_id(&span.span_id)));
    row.insert(
        "parent_span_id".into(),
        match &span.parent_span_id {
            Some(id) => Value::String(hex_id(id)),
            None => Value::Null,
        },
    );
    row.insert("name".into(), Value::String(span.name.clone()));
    row.insert("service".into(), Value::String(service));
    row.insert("kind".into(), Value::String(kind_name(span.kind).into()));
    row.insert(
        "status".into(),
        Value::String(status_name(span.status).into()),
    );
    row.insert("start_time".into(), Value::Number(span.start_ts.into()));
    row.insert("end_time".into(), Value::Number(end_time.into()));
    row.insert("duration_ns".into(), Value::Number(span.duration_ns.into()));
    row.insert(
        "status_message".into(),
        if span.status_description.is_empty() {
            Value::Null
        } else {
            Value::String(span.status_description.clone())
        },
    );
    row.insert(
        "attributes".into(),
        serde_json::from_str(&span.attributes).ok()?,
    );
    row.insert("events".into(), serde_json::from_str(&span.events).ok()?);
    row.insert("resource".into(), resource);
    row.insert(
        "instrumentation_scope".into(),
        serde_json::from_str(&span.scope).ok()?,
    );
    row.insert("links".into(), serde_json::from_str(&span.links).ok()?);
    row.insert(
        "trace_state".into(),
        Value::String(span.trace_state.clone()),
    );
    row.insert("trace_flags".into(), Value::Number(span.trace_flags.into()));
    row.insert(
        "dropped_attributes_count".into(),
        Value::Number(span.dropped_attributes_count.into()),
    );
    row.insert(
        "dropped_events_count".into(),
        Value::Number(span.dropped_events_count.into()),
    );
    row.insert(
        "dropped_links_count".into(),
        Value::Number(span.dropped_links_count.into()),
    );
    row.insert(
        "resource_schema_url".into(),
        Value::String(span.resource_schema_url.clone()),
    );
    row.insert(
        "scope_schema_url".into(),
        Value::String(span.scope_schema_url.clone()),
    );
    row.insert(
        "resource_dropped_attributes_count".into(),
        Value::Number(span.resource_dropped_attributes_count.into()),
    );
    row.insert(
        "scope_dropped_attributes_count".into(),
        Value::Number(span.scope_dropped_attributes_count.into()),
    );
    Some(Value::Object(row))
}

/// The wire encoding is numeric; the query surface exposes these names, and a
/// tail filter is written against the names.
fn kind_name(kind: u8) -> &'static str {
    match kind {
        1 => "server",
        2 => "client",
        3 => "producer",
        4 => "consumer",
        _ => "internal",
    }
}

fn status_name(status: u8) -> &'static str {
    match status {
        1 => "ok",
        2 => "error",
        _ => "unset",
    }
}

fn hex_id<const N: usize>(value: &[u8; N]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(N * 2);
    for byte in value {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn json_u32(value: Option<&Value>, name: &str) -> Result<u32, String> {
    match value {
        None | Some(Value::Null) => Ok(0),
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| format!("{name} must be an unsigned 32-bit integer")),
        Some(Value::String(value)) => value
            .parse::<u32>()
            .map_err(|_| format!("{name} must be an unsigned 32-bit integer")),
        _ => Err(format!("{name} must be an unsigned 32-bit integer")),
    }
}

fn json_time(value: Option<&Value>, name: &str) -> Result<i64, String> {
    let native = match value {
        Some(Value::String(value)) => value
            .parse::<u64>()
            .map_err(|_| format!("{name} must be an unsigned nanosecond integer"))?,
        Some(Value::Number(value)) => value
            .as_u64()
            .ok_or_else(|| format!("{name} must be an unsigned nanosecond integer"))?,
        _ => 0,
    };
    native_time(native, name)
}

fn native_time(value: u64, name: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("{name} exceeds the signed 64-bit storage range"))
}

fn json_kind(value: Option<&Value>) -> u8 {
    match value {
        Some(Value::Number(value)) => match value.as_i64() {
            Some(2) => 1,
            Some(3) => 2,
            Some(4) => 3,
            Some(5) => 4,
            _ => 0,
        },
        Some(Value::String(value)) => match value.as_str() {
            "SPAN_KIND_SERVER" => 1,
            "SPAN_KIND_CLIENT" => 2,
            "SPAN_KIND_PRODUCER" => 3,
            "SPAN_KIND_CONSUMER" => 4,
            _ => 0,
        },
        _ => 0,
    }
}

fn protobuf_kind(value: i32) -> u8 {
    match value {
        2 => 1,
        3 => 2,
        4 => 3,
        5 => 4,
        _ => 0,
    }
}

fn json_status(value: Option<&Value>) -> Result<(u8, String), String> {
    let Some(value) = value else {
        return Ok((0, String::new()));
    };
    let status = object(value, "status")?;
    let code = match status.get("code") {
        Some(Value::Number(value)) => match value.as_i64() {
            Some(1) => 1,
            Some(2) => 2,
            _ => 0,
        },
        Some(Value::String(value)) => match value.as_str() {
            "STATUS_CODE_OK" => 1,
            "STATUS_CODE_ERROR" => 2,
            _ => 0,
        },
        _ => 0,
    };
    let description = optional_string(status.get("message"), "status.message")?.unwrap_or_default();
    Ok((code, description))
}

fn protobuf_status(value: i32) -> u8 {
    match value {
        1 => 1,
        2 => 2,
        _ => 0,
    }
}

fn json_attributes(value: Option<&Value>, context: &str) -> Result<Map<String, Value>, String> {
    let values = match value {
        None | Some(Value::Null) => return Ok(Map::new()),
        Some(value) => array(value, context)?,
    };
    let mut out = BTreeMap::new();
    for (index, value) in values.iter().enumerate() {
        let value = object(value, &format!("{context}[{index}]"))?;
        let key = optional_string(value.get("key"), "attribute key")?
            .ok_or_else(|| format!("{context}[{index}] is missing key"))?;
        let any = value
            .get("value")
            .ok_or_else(|| format!("{context}[{index}] is missing value"))?;
        out.insert(key, json_any_value(any, context)?);
    }
    Ok(out.into_iter().collect())
}

fn json_any_value(value: &Value, context: &str) -> Result<Value, String> {
    let value = object(value, context)?;
    for key in [
        "stringValue",
        "boolValue",
        "intValue",
        "doubleValue",
        "bytesValue",
    ] {
        if let Some(value) = value.get(key) {
            return Ok(value.clone());
        }
    }
    if let Some(value) = value.get("arrayValue") {
        let value = object(value, "arrayValue")?;
        let values = match value.get("values") {
            None | Some(Value::Null) => &[][..],
            Some(value) => array(value, "arrayValue.values")?,
        };
        return values
            .iter()
            .map(|value| json_any_value(value, "arrayValue"))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array);
    }
    if let Some(value) = value.get("kvlistValue") {
        let value = object(value, "kvlistValue")?;
        return json_attributes(value.get("values"), "kvlistValue.values").map(Value::Object);
    }
    Err(format!("{context} contains an unsupported AnyValue"))
}

fn protobuf_attributes(
    values: Vec<proto::KeyValue>,
    context: &str,
) -> Result<Map<String, Value>, String> {
    let mut out = BTreeMap::new();
    for (index, value) in values.into_iter().enumerate() {
        let any = value
            .value
            .ok_or_else(|| format!("{context}[{index}] is missing value"))?;
        out.insert(value.key, protobuf_any_value(any)?);
    }
    Ok(out.into_iter().collect())
}

fn protobuf_any_value(value: proto::AnyValue) -> Result<Value, String> {
    use proto::any_value::Value as Any;
    match value.value {
        Some(Any::StringValue(value)) => Ok(Value::String(value)),
        Some(Any::BoolValue(value)) => Ok(Value::Bool(value)),
        Some(Any::IntValue(value)) => Ok(Value::Number(value.into())),
        Some(Any::DoubleValue(value)) => Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| "protobuf double AnyValue must be finite".to_owned()),
        Some(Any::BytesValue(value)) => Ok(Value::String(BASE64.encode(value))),
        Some(Any::ArrayValue(value)) => value
            .values
            .into_iter()
            .map(protobuf_any_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Some(Any::KvlistValue(value)) => {
            protobuf_attributes(value.values, "kvlistValue.values").map(Value::Object)
        }
        None => Err("protobuf AnyValue is empty".to_owned()),
    }
}

fn json_events(value: Option<&Value>) -> Result<Vec<Value>, String> {
    let values = match value {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(value) => array(value, "events")?,
    };
    values
        .iter()
        .enumerate()
        .map(|(index, event)| {
            let event = object(event, &format!("events[{index}]"))?;
            let mut out = Map::new();
            out.insert(
                "name".into(),
                Value::String(
                    optional_string(event.get("name"), "event.name")?.unwrap_or_default(),
                ),
            );
            out.insert(
                "timestamp".into(),
                Value::Number(json_time(event.get("timeUnixNano"), "event.timeUnixNano")?.into()),
            );
            out.insert(
                "attributes".into(),
                Value::Object(json_attributes(
                    event.get("attributes"),
                    "event.attributes",
                )?),
            );
            out.insert(
                "dropped_attributes_count".into(),
                Value::Number(
                    json_u32(
                        event.get("droppedAttributesCount"),
                        "event.droppedAttributesCount",
                    )?
                    .into(),
                ),
            );
            Ok(Value::Object(out))
        })
        .collect()
}

fn protobuf_event(event: proto::Event, index: usize) -> Result<Value, String> {
    let mut out = Map::new();
    out.insert("name".into(), Value::String(event.name));
    out.insert(
        "timestamp".into(),
        Value::Number(native_time(event.time_unix_nano, "event.time_unix_nano")?.into()),
    );
    out.insert(
        "attributes".into(),
        Value::Object(protobuf_attributes(
            event.attributes,
            &format!("events[{index}].attributes"),
        )?),
    );
    out.insert(
        "dropped_attributes_count".into(),
        Value::Number(event.dropped_attributes_count.into()),
    );
    Ok(Value::Object(out))
}

fn json_links(value: Option<&Value>) -> Result<Vec<Value>, String> {
    let values = match value {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(value) => array(value, "links")?,
    };
    values
        .iter()
        .enumerate()
        .map(|(index, link)| {
            let link = object(link, &format!("links[{index}]"))?;
            let mut out = Map::new();
            out.insert(
                "trace_id".into(),
                Value::String(hex_id(&json_id::<16>(link.get("traceId"), "link.traceId")?)),
            );
            out.insert(
                "span_id".into(),
                Value::String(hex_id(&json_id::<8>(link.get("spanId"), "link.spanId")?)),
            );
            out.insert(
                "trace_state".into(),
                Value::String(
                    optional_string(link.get("traceState"), "link.traceState")?.unwrap_or_default(),
                ),
            );
            out.insert(
                "attributes".into(),
                Value::Object(json_attributes(link.get("attributes"), "link.attributes")?),
            );
            out.insert(
                "dropped_attributes_count".into(),
                Value::Number(
                    json_u32(
                        link.get("droppedAttributesCount"),
                        "link.droppedAttributesCount",
                    )?
                    .into(),
                ),
            );
            out.insert(
                "flags".into(),
                Value::Number(json_u32(link.get("flags"), "link.flags")?.into()),
            );
            Ok(Value::Object(out))
        })
        .collect()
}

fn protobuf_link(link: proto::Link, index: usize) -> Result<Value, String> {
    let mut out = Map::new();
    out.insert(
        "trace_id".into(),
        Value::String(hex_id(&bytes_id::<16>(&link.trace_id, "link.trace_id")?)),
    );
    out.insert(
        "span_id".into(),
        Value::String(hex_id(&bytes_id::<8>(&link.span_id, "link.span_id")?)),
    );
    out.insert("trace_state".into(), Value::String(link.trace_state));
    out.insert(
        "attributes".into(),
        Value::Object(protobuf_attributes(
            link.attributes,
            &format!("links[{index}].attributes"),
        )?),
    );
    out.insert(
        "dropped_attributes_count".into(),
        Value::Number(link.dropped_attributes_count.into()),
    );
    out.insert("flags".into(), Value::Number(link.flags.into()));
    Ok(Value::Object(out))
}

fn json_scope(value: Option<&Value>) -> Result<(Map<String, Value>, u32), String> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok((Map::new(), 0));
    };
    let scope = object(value, "scope")?;
    let mut out = Map::new();
    out.insert(
        "name".into(),
        Value::String(optional_string(scope.get("name"), "scope.name")?.unwrap_or_default()),
    );
    out.insert(
        "version".into(),
        optional_string(scope.get("version"), "scope.version")?
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    out.insert(
        "attributes".into(),
        Value::Object(json_attributes(
            scope.get("attributes"),
            "scope.attributes",
        )?),
    );
    Ok((
        out,
        json_u32(
            scope.get("droppedAttributesCount"),
            "scope.droppedAttributesCount",
        )?,
    ))
}

fn protobuf_scope(
    value: Option<proto::InstrumentationScope>,
) -> Result<(Map<String, Value>, u32), String> {
    let Some(scope) = value else {
        return Ok((Map::new(), 0));
    };
    let mut out = Map::new();
    out.insert("name".into(), Value::String(scope.name));
    out.insert(
        "version".into(),
        if scope.version.is_empty() {
            Value::Null
        } else {
            Value::String(scope.version)
        },
    );
    out.insert(
        "attributes".into(),
        Value::Object(protobuf_attributes(scope.attributes, "scope.attributes")?),
    );
    Ok((out, scope.dropped_attributes_count))
}

fn optional_string(value: Option<&Value>, name: &str) -> Result<Option<String>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(format!("{name} must be a string")),
    }
}

fn object<'a>(value: &'a Value, name: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{name} must be an object"))
}

fn array<'a>(value: &'a Value, name: &str) -> Result<&'a [Value], String> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| format!("{name} must be an array"))
}

fn canonical_object(value: Map<String, Value>) -> Result<String, String> {
    serde_json::to_string(&Value::Object(value)).map_err(|error| format!("encode object: {error}"))
}

fn canonical_array(value: Vec<Value>) -> Result<String, String> {
    serde_json::to_string(&Value::Array(value)).map_err(|error| format!("encode array: {error}"))
}

fn text_column<'a>(out: &mut Vec<u8>, values: impl Iterator<Item = &'a str>) -> Result<(), String> {
    for value in values {
        let length = u32::try_from(value.len())
            .map_err(|_| "rich batch string exceeds u32::MAX bytes".to_owned())?;
        out.extend_from_slice(&length.to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    Ok(())
}

fn u32_column(out: &mut Vec<u8>, values: impl Iterator<Item = u32>) {
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
}

pub(crate) mod proto {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ExportTraceServiceRequest {
        #[prost(message, repeated, tag = "1")]
        pub resource_spans: Vec<ResourceSpans>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ResourceSpans {
        #[prost(message, optional, tag = "1")]
        pub resource: Option<Resource>,
        #[prost(message, repeated, tag = "2")]
        pub scope_spans: Vec<ScopeSpans>,
        #[prost(string, tag = "3")]
        pub schema_url: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Resource {
        #[prost(message, repeated, tag = "1")]
        pub attributes: Vec<KeyValue>,
        #[prost(uint32, tag = "2")]
        pub dropped_attributes_count: u32,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ScopeSpans {
        #[prost(message, optional, tag = "1")]
        pub scope: Option<InstrumentationScope>,
        #[prost(message, repeated, tag = "2")]
        pub spans: Vec<Span>,
        #[prost(string, tag = "3")]
        pub schema_url: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct InstrumentationScope {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub version: String,
        #[prost(message, repeated, tag = "3")]
        pub attributes: Vec<KeyValue>,
        #[prost(uint32, tag = "4")]
        pub dropped_attributes_count: u32,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Span {
        #[prost(bytes = "vec", tag = "1")]
        pub trace_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub span_id: Vec<u8>,
        #[prost(string, tag = "3")]
        pub trace_state: String,
        #[prost(bytes = "vec", tag = "4")]
        pub parent_span_id: Vec<u8>,
        #[prost(string, tag = "5")]
        pub name: String,
        #[prost(enumeration = "SpanKind", tag = "6")]
        pub kind: i32,
        #[prost(fixed64, tag = "7")]
        pub start_time_unix_nano: u64,
        #[prost(fixed64, tag = "8")]
        pub end_time_unix_nano: u64,
        #[prost(message, repeated, tag = "9")]
        pub attributes: Vec<KeyValue>,
        #[prost(uint32, tag = "10")]
        pub dropped_attributes_count: u32,
        #[prost(message, repeated, tag = "11")]
        pub events: Vec<Event>,
        #[prost(uint32, tag = "12")]
        pub dropped_events_count: u32,
        #[prost(message, repeated, tag = "13")]
        pub links: Vec<Link>,
        #[prost(uint32, tag = "14")]
        pub dropped_links_count: u32,
        #[prost(message, optional, tag = "15")]
        pub status: Option<Status>,
        #[prost(fixed32, tag = "16")]
        pub flags: u32,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Event {
        #[prost(fixed64, tag = "1")]
        pub time_unix_nano: u64,
        #[prost(string, tag = "2")]
        pub name: String,
        #[prost(message, repeated, tag = "3")]
        pub attributes: Vec<KeyValue>,
        #[prost(uint32, tag = "4")]
        pub dropped_attributes_count: u32,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Link {
        #[prost(bytes = "vec", tag = "1")]
        pub trace_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub span_id: Vec<u8>,
        #[prost(string, tag = "3")]
        pub trace_state: String,
        #[prost(message, repeated, tag = "4")]
        pub attributes: Vec<KeyValue>,
        #[prost(uint32, tag = "5")]
        pub dropped_attributes_count: u32,
        #[prost(fixed32, tag = "6")]
        pub flags: u32,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Status {
        #[prost(string, tag = "2")]
        pub message: String,
        #[prost(enumeration = "StatusCode", tag = "3")]
        pub code: i32,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct KeyValue {
        #[prost(string, tag = "1")]
        pub key: String,
        #[prost(message, optional, tag = "2")]
        pub value: Option<AnyValue>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct AnyValue {
        #[prost(oneof = "any_value::Value", tags = "1, 2, 3, 4, 5, 6, 7")]
        pub value: Option<any_value::Value>,
    }

    pub mod any_value {
        #[derive(Clone, PartialEq, prost::Oneof)]
        #[allow(clippy::enum_variant_names)]
        pub enum Value {
            #[prost(string, tag = "1")]
            StringValue(String),
            #[prost(bool, tag = "2")]
            BoolValue(bool),
            #[prost(int64, tag = "3")]
            IntValue(i64),
            #[prost(double, tag = "4")]
            DoubleValue(f64),
            #[prost(message, tag = "5")]
            ArrayValue(super::ArrayValue),
            #[prost(message, tag = "6")]
            KvlistValue(super::KeyValueList),
            #[prost(bytes, tag = "7")]
            BytesValue(Vec<u8>),
        }
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ArrayValue {
        #[prost(message, repeated, tag = "1")]
        pub values: Vec<AnyValue>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct KeyValueList {
        #[prost(message, repeated, tag = "1")]
        pub values: Vec<KeyValue>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
    #[repr(i32)]
    pub enum SpanKind {
        Unspecified = 0,
        Internal = 1,
        Server = 2,
        Client = 3,
        Producer = 4,
        Consumer = 5,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
    #[repr(i32)]
    pub enum StatusCode {
        Unset = 0,
        Ok = 1,
        Error = 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_parser_preserves_nested_typed_values() {
        let body = br#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","startTimeUnixNano":"10","endTimeUnixNano":"20","attributes":[{"key":"nested","value":{"kvlistValue":{"values":[{"key":"ok","value":{"boolValue":true}}]}}},{"key":"array","value":{"arrayValue":{"values":[{"intValue":"7"},{"doubleValue":1.25}]}}}]}]}]}]}"#;
        let spans = parse_json(body).unwrap();
        assert_eq!(spans.len(), 1);
        let attrs: Value = serde_json::from_str(&spans[0].attributes).unwrap();
        assert_eq!(attrs["nested"]["ok"], true);
        assert_eq!(attrs["array"], serde_json::json!(["7", 1.25]));
    }

    #[test]
    fn invalid_or_reversed_identity_and_time_are_rejected_before_batching() {
        let bad_id = br#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"bad","spanId":"0000000000000001"}]}]}]}"#;
        assert!(parse_json(bad_id).unwrap_err().contains("traceId"));
        let reversed = br#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","startTimeUnixNano":"20","endTimeUnixNano":"10"}]}]}]}"#;
        assert!(parse_json(reversed).unwrap_err().contains("precedes"));
    }

    #[test]
    fn tail_row_carries_the_dashboard_span_shape() {
        // The fields query::dashboard_span emits, plus `service`, which the
        // stored surface keeps in a column and the tail needs on the row to
        // filter against. Keep these in step: a subscriber and a search are
        // supposed to describe the same span the same way.
        let body = br#"{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeSpans":[{"spans":[{"traceId":"0000000000000000000000000000000a","spanId":"000000000000000b","parentSpanId":"000000000000000c","name":"GET /orders","kind":"SPAN_KIND_SERVER","startTimeUnixNano":"100","endTimeUnixNano":"250","status":{"code":"STATUS_CODE_ERROR","message":"upstream timeout"},"attributes":[{"key":"http.route","value":{"stringValue":"/orders"}}]}]}]}]}"#;
        let spans = parse_json(body).unwrap();
        let row = tail_row(&spans[0]).unwrap();

        assert_eq!(row["trace_id"], "0000000000000000000000000000000a");
        assert_eq!(row["span_id"], "000000000000000b");
        assert_eq!(row["parent_span_id"], "000000000000000c");
        assert_eq!(row["name"], "GET /orders");
        assert_eq!(row["service"], "checkout");
        assert_eq!(row["kind"], "server");
        assert_eq!(row["status"], "error");
        assert_eq!(row["status_message"], "upstream timeout");
        assert_eq!(row["start_time"], 100);
        // Stored spans carry a duration; the row carries the end time the
        // dashboard computes from it, so both surfaces agree.
        assert_eq!(row["end_time"], 250);
        assert_eq!(row["duration_ns"], 150);
        assert_eq!(row["attributes"]["http.route"], "/orders");
        assert_eq!(row["resource"]["service.name"], "checkout");

        for field in [
            "events",
            "links",
            "instrumentation_scope",
            "trace_state",
            "trace_flags",
            "dropped_attributes_count",
            "dropped_events_count",
            "dropped_links_count",
            "resource_schema_url",
            "scope_schema_url",
            "resource_dropped_attributes_count",
            "scope_dropped_attributes_count",
        ] {
            assert!(row.get(field).is_some(), "row is missing {field}");
        }
    }

    #[test]
    fn tail_row_reports_an_absent_parent_and_an_empty_status_message_as_null() {
        let body = br#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0000000000000000000000000000000a","spanId":"000000000000000b","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}"#;
        let row = tail_row(&parse_json(body).unwrap()[0]).unwrap();

        assert_eq!(row["parent_span_id"], Value::Null);
        assert_eq!(row["status_message"], Value::Null);
        // Absent, not "unknown": the defaults the OTLP spec assigns.
        assert_eq!(row["kind"], "internal");
        assert_eq!(row["status"], "unset");
        assert_eq!(row["service"], "");
    }

    #[test]
    fn all_kind_status_and_bytes_mappings_are_pinned() {
        for (wire, stored) in [(1, 0), (2, 1), (3, 2), (4, 3), (5, 4), (99, 0)] {
            assert_eq!(json_kind(Some(&Value::Number(wire.into()))), stored);
            assert_eq!(protobuf_kind(wire), stored);
        }
        for (wire, stored) in [
            ("SPAN_KIND_INTERNAL", 0),
            ("SPAN_KIND_SERVER", 1),
            ("SPAN_KIND_CLIENT", 2),
            ("SPAN_KIND_PRODUCER", 3),
            ("SPAN_KIND_CONSUMER", 4),
        ] {
            assert_eq!(json_kind(Some(&Value::String(wire.into()))), stored);
        }
        for (wire, stored) in [(0, 0), (1, 1), (2, 2), (99, 0)] {
            assert_eq!(protobuf_status(wire), stored);
            assert_eq!(
                json_status(Some(&serde_json::json!({"code": wire})))
                    .unwrap()
                    .0,
                stored
            );
        }
        assert_eq!(
            protobuf_any_value(proto::AnyValue {
                value: Some(proto::any_value::Value::BytesValue(vec![0xfb, 0xff])),
            })
            .unwrap(),
            Value::String("+/8=".into())
        );
    }
}
