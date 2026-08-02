//! Canonical typed JSON at the public OTel/SQLite boundary.
//!
//! Metrics labels and log metadata intentionally remain flat string
//! maps. Traces cannot share that parser: AnyValue permits booleans,
//! numbers, nulls, arrays, and nested key/value lists. These helpers
//! validate the expected top-level shape and serialize through
//! `serde_json`, whose default map representation is key-sorted. The
//! result is stable JSON text without any loss of value types.

use serde_json::Value;

fn canonical(text: Option<&str>, field: &str, object: bool) -> Result<String, String> {
    let default = if object { "{}" } else { "[]" };
    let text = match text {
        Some(text) if !text.trim().is_empty() => text,
        _ => default,
    };
    let value: Value =
        serde_json::from_str(text).map_err(|error| format!("{field}: invalid JSON: {error}"))?;
    let valid = if object {
        value.is_object()
    } else {
        value.is_array()
    };
    if !valid {
        return Err(format!(
            "{field}: expected a JSON {}, got {}",
            if object { "object" } else { "array" },
            json_type(&value)
        ));
    }
    serde_json::to_string(&value).map_err(|error| format!("{field}: {error}"))
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub(crate) fn object(text: Option<&str>, field: &str) -> Result<String, String> {
    canonical(text, field, true)
}

pub(crate) fn array(text: Option<&str>, field: &str) -> Result<String, String> {
    canonical(text, field, false)
}

fn service_from(json: &str) -> Option<String> {
    let value: Value = serde_json::from_str(json).ok()?;
    value.get("service.name")?.as_str().map(str::to_owned)
}

/// Match timeless_traces product semantics: a span attribute wins,
/// then its resource, then the compatibility `service` column. An
/// empty/non-string JSON service value is not an indexable name.
pub(crate) fn derive_service(
    attributes: &str,
    resource: &str,
    explicit: Option<String>,
) -> Result<String, String> {
    service_from(attributes)
        .or_else(|| service_from(resource))
        .or(explicit)
        .filter(|service| !service.is_empty())
        .ok_or_else(|| {
            "service is required: supply service TEXT or string service.name in attributes/resource"
                .into()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_typed_nested_values_and_sorts_keys() {
        assert_eq!(
            object(Some(r#"{"z":null,"a":[true,3.5,{"x":"v"}]}"#), "attributes").unwrap(),
            r#"{"a":[true,3.5,{"x":"v"}],"z":null}"#
        );
        assert!(object(Some("[]"), "attributes").is_err());
        assert!(array(Some("{}"), "events").is_err());
    }

    #[test]
    fn service_precedence_matches_product() {
        assert_eq!(
            derive_service(
                r#"{"service.name":"span"}"#,
                r#"{"service.name":"resource"}"#,
                Some("explicit".into())
            )
            .unwrap(),
            "span"
        );
    }
}
