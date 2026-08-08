//! Bounded trace-attribute equality primitives.
//!
//! This module deliberately knows nothing about TraceQL. It defines an
//! allowlisted JSON-Pointer field identity, an exact typed-scalar predicate,
//! and a fixed-size per-block negative filter. A filter may return false
//! positives; the engine always rechecks surviving spans exactly.

use std::collections::BTreeSet;

use serde_json::{Map, Value};

use super::SpanEntry;

pub const MAX_SPAN_ATTRIBUTE_INDEXES: usize = 8;
pub const MAX_SPAN_ATTRIBUTE_PATH_BYTES: usize = 256;
pub const SPAN_ATTRIBUTE_BLOOM_BYTES: usize = 4096;
pub const SPAN_ATTRIBUTE_BLOOM_HASHES: u64 = 4;
pub const SPAN_ATTRIBUTE_BLOOM_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpanAttributeScope {
    Span,
    Resource,
    InstrumentationScope,
}

impl SpanAttributeScope {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Span => "span",
            Self::Resource => "resource",
            Self::InstrumentationScope => "scope",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "span" => Ok(Self::Span),
            "resource" => Ok(Self::Resource),
            "scope" => Ok(Self::InstrumentationScope),
            other => Err(format!(
                "unknown trace attribute scope {other:?}; expected 'span', 'resource', or 'scope'"
            )),
        }
    }

    fn json(self, entry: &SpanEntry) -> &str {
        match self {
            Self::Span => entry.attributes.as_ref(),
            Self::Resource => entry.resource.as_ref(),
            Self::InstrumentationScope => entry.instrumentation_scope.as_ref(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SpanAttributeIndex {
    scope: SpanAttributeScope,
    path: String,
}

impl SpanAttributeIndex {
    pub fn new(scope: SpanAttributeScope, path: impl Into<String>) -> Result<Self, String> {
        let path = path.into();
        validate_pointer(&path)?;
        Ok(Self { scope, path })
    }

    pub const fn scope(&self) -> SpanAttributeScope {
        self.scope
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    fn from_object(object: &Map<String, Value>, context: &str) -> Result<Self, String> {
        reject_unknown_keys(object, &["scope", "path"], context)?;
        if object.len() != 2 {
            return Err(format!(
                "{context} requires exactly string keys 'scope' and 'path'"
            ));
        }
        let scope = object
            .get("scope")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{context}.scope must be a string"))?;
        let path = object
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{context}.path must be a string"))?;
        Self::new(SpanAttributeScope::parse(scope)?, path)
    }

    fn to_value(&self) -> Value {
        let mut object = Map::new();
        object.insert("path".into(), Value::String(self.path.clone()));
        object.insert("scope".into(), Value::String(self.scope.name().into()));
        Value::Object(object)
    }
}

pub fn parse_span_attribute_indexes(encoded: &str) -> Result<Vec<SpanAttributeIndex>, String> {
    let value: Value = serde_json::from_str(encoded)
        .map_err(|error| format!("attribute_indexes must be a JSON array: {error}"))?;
    let array = value
        .as_array()
        .ok_or_else(|| "attribute_indexes must be a JSON array".to_owned())?;
    if array.len() > MAX_SPAN_ATTRIBUTE_INDEXES {
        return Err(format!(
            "attribute_indexes contains {} fields; at most {MAX_SPAN_ATTRIBUTE_INDEXES} are supported",
            array.len()
        ));
    }
    let mut unique = BTreeSet::new();
    for (position, value) in array.iter().enumerate() {
        let object = value.as_object().ok_or_else(|| {
            format!("attribute_indexes[{position}] must be an object with scope and path")
        })?;
        let index =
            SpanAttributeIndex::from_object(object, &format!("attribute_indexes[{position}]"))?;
        if !unique.insert(index.clone()) {
            return Err(format!(
                "duplicate trace attribute index {}:{}",
                index.scope.name(),
                index.path
            ));
        }
    }
    Ok(unique.into_iter().collect())
}

pub fn encode_span_attribute_indexes(indexes: &[SpanAttributeIndex]) -> String {
    Value::Array(indexes.iter().map(SpanAttributeIndex::to_value).collect()).to_string()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanAttributeFilter {
    index: SpanAttributeIndex,
    scalar: Value,
    scalar_json: String,
}

impl SpanAttributeFilter {
    pub fn parse(encoded: &str) -> Result<Self, String> {
        let value: Value = serde_json::from_str(encoded)
            .map_err(|error| format!("attribute_filter must be a JSON object: {error}"))?;
        let object = value
            .as_object()
            .ok_or_else(|| "attribute_filter must be a JSON object".to_owned())?;
        reject_unknown_keys(object, &["scope", "path", "value"], "attribute_filter")?;
        if object.len() != 3 || !object.contains_key("value") {
            return Err(
                "attribute_filter requires exactly keys 'scope', 'path', and 'value'".into(),
            );
        }
        let index = SpanAttributeIndex::from_object_without_value(object)?;
        let scalar = object.get("value").expect("checked above");
        if !is_scalar(scalar) {
            return Err(
                "attribute_filter.value must be a JSON scalar, not an array or object".into(),
            );
        }
        Ok(Self {
            index,
            scalar: scalar.clone(),
            scalar_json: scalar.to_string(),
        })
    }

    pub fn index(&self) -> &SpanAttributeIndex {
        &self.index
    }

    pub fn scalar_json(&self) -> &str {
        &self.scalar_json
    }

    pub fn matches_entry(&self, entry: &SpanEntry) -> Result<bool, String> {
        self.matches_json(self.index.scope.json(entry))
    }

    pub fn matches_json(&self, encoded: &str) -> Result<bool, String> {
        let root: Value = serde_json::from_str(encoded).map_err(|error| {
            format!(
                "stored {} attributes are invalid JSON: {error}",
                self.index.scope.name()
            )
        })?;
        let Some(value) = root.pointer(&self.index.path) else {
            return Ok(false);
        };
        Ok(is_scalar(value) && value == &self.scalar)
    }
}

impl SpanAttributeIndex {
    fn from_object_without_value(object: &Map<String, Value>) -> Result<Self, String> {
        let scope = object
            .get("scope")
            .and_then(Value::as_str)
            .ok_or_else(|| "attribute_filter.scope must be a string".to_owned())?;
        let path = object
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| "attribute_filter.path must be a string".to_owned())?;
        Self::new(SpanAttributeScope::parse(scope)?, path)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanAttributeBloom {
    pub index: SpanAttributeIndex,
    pub bits: Vec<u8>,
}

impl SpanAttributeBloom {
    fn empty(index: SpanAttributeIndex) -> Self {
        Self {
            index,
            bits: vec![0; SPAN_ATTRIBUTE_BLOOM_BYTES],
        }
    }

    fn insert(&mut self, scalar_json: &str) {
        for bit in bloom_positions(scalar_json.as_bytes()) {
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    pub fn might_contain(&self, scalar_json: &str) -> Result<bool, String> {
        validate_span_attribute_bloom(&self.bits)?;
        Ok(bloom_positions(scalar_json.as_bytes())
            .into_iter()
            .all(|bit| self.bits[bit / 8] & (1 << (bit % 8)) != 0))
    }
}

pub fn build_span_attribute_blooms(
    entries: &[SpanEntry],
    indexes: &[SpanAttributeIndex],
) -> Result<Vec<SpanAttributeBloom>, String> {
    let mut blooms = indexes
        .iter()
        .cloned()
        .map(SpanAttributeBloom::empty)
        .collect::<Vec<_>>();
    let needs_span = indexes
        .iter()
        .any(|index| index.scope == SpanAttributeScope::Span);
    let needs_resource = indexes
        .iter()
        .any(|index| index.scope == SpanAttributeScope::Resource);
    let needs_scope = indexes
        .iter()
        .any(|index| index.scope == SpanAttributeScope::InstrumentationScope);

    for entry in entries {
        let span = needs_span
            .then(|| parse_object(entry.attributes.as_ref(), "span"))
            .transpose()?;
        let resource = needs_resource
            .then(|| parse_object(entry.resource.as_ref(), "resource"))
            .transpose()?;
        let scope = needs_scope
            .then(|| parse_object(entry.instrumentation_scope.as_ref(), "scope"))
            .transpose()?;
        for bloom in &mut blooms {
            let root = match bloom.index.scope {
                SpanAttributeScope::Span => span.as_ref().expect("parsed when configured"),
                SpanAttributeScope::Resource => resource.as_ref().expect("parsed when configured"),
                SpanAttributeScope::InstrumentationScope => {
                    scope.as_ref().expect("parsed when configured")
                }
            };
            if let Some(value) = root
                .pointer(&bloom.index.path)
                .filter(|value| is_scalar(value))
            {
                bloom.insert(&value.to_string());
            }
        }
    }
    Ok(blooms)
}

pub fn validate_span_attribute_bloom(bits: &[u8]) -> Result<(), String> {
    if bits.len() != SPAN_ATTRIBUTE_BLOOM_BYTES {
        return Err(format!(
            "trace attribute bloom is {} bytes; expected {SPAN_ATTRIBUTE_BLOOM_BYTES}",
            bits.len()
        ));
    }
    Ok(())
}

pub fn span_attribute_bloom_checksum(bits: &[u8]) -> [u8; 8] {
    stable_hash(bits, 0xd6e8_feb8_6659_fd93).to_le_bytes()
}

fn bloom_positions(bytes: &[u8]) -> [usize; SPAN_ATTRIBUTE_BLOOM_HASHES as usize] {
    let first = stable_hash(bytes, 0x243f_6a88_85a3_08d3);
    let second = stable_hash(bytes, 0x1319_8a2e_0370_7344) | 1;
    let bits = (SPAN_ATTRIBUTE_BLOOM_BYTES * 8) as u64;
    let mut positions = [0; SPAN_ATTRIBUTE_BLOOM_HASHES as usize];
    for (index, position) in positions.iter_mut().enumerate() {
        *position = first
            .wrapping_add((index as u64).wrapping_mul(second))
            .wrapping_add((index as u64).wrapping_mul(index as u64))
            .rem_euclid(bits) as usize;
    }
    positions
}

fn stable_hash(bytes: &[u8], seed: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ seed;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^ (hash >> 33)
}

fn parse_object(encoded: &str, scope: &str) -> Result<Value, String> {
    let value: Value = serde_json::from_str(encoded)
        .map_err(|error| format!("{scope} attributes are invalid JSON: {error}"))?;
    if !value.is_object() {
        return Err(format!("{scope} attributes must be a JSON object"));
    }
    Ok(value)
}

fn is_scalar(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

fn validate_pointer(path: &str) -> Result<(), String> {
    if path.is_empty() || !path.starts_with('/') {
        return Err("trace attribute path must be a non-empty RFC 6901 JSON Pointer".into());
    }
    if path.len() > MAX_SPAN_ATTRIBUTE_PATH_BYTES {
        return Err(format!(
            "trace attribute path is {} bytes; maximum is {MAX_SPAN_ATTRIBUTE_PATH_BYTES}",
            path.len()
        ));
    }
    let bytes = path.as_bytes();
    let mut position = 0;
    while position < bytes.len() {
        if bytes[position] == b'~' {
            let Some(next) = bytes.get(position + 1) else {
                return Err(
                    "trace attribute JSON Pointer ends with an incomplete '~' escape".into(),
                );
            };
            if !matches!(*next, b'0' | b'1') {
                return Err(format!(
                    "trace attribute JSON Pointer contains invalid escape '~{}'",
                    char::from(*next)
                ));
            }
            position += 2;
        } else {
            position += 1;
        }
    }
    Ok(())
}

fn reject_unknown_keys(
    object: &Map<String, Value>,
    allowed: &[&str],
    context: &str,
) -> Result<(), String> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!("{context} contains unknown key {key:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    fn entry() -> SpanEntry {
        SpanEntry {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "op".into(),
            service: "svc".into(),
            kind: 0,
            status: 0,
            status_description: Cow::Borrowed(""),
            start_ts: 1,
            duration_ns: 2,
            attributes: Cow::Borrowed(
                r#"{"empty":"","integer":1,"null":null,"real":1.0,"string":"1","nested":{"a/b":true}}"#,
            ),
            events: Cow::Borrowed("[]"),
            resource: Cow::Borrowed(r#"{"deployment.environment":"test"}"#),
            instrumentation_scope: Cow::Borrowed(r#"{"attributes":{"debug":false}}"#),
            links: Cow::Borrowed("[]"),
            trace_state: Cow::Borrowed(""),
            trace_flags: 0,
            dropped_attributes_count: 0,
            dropped_events_count: 0,
            dropped_links_count: 0,
            resource_schema_url: Cow::Borrowed(""),
            scope_schema_url: Cow::Borrowed(""),
            resource_dropped_attributes_count: 0,
            scope_dropped_attributes_count: 0,
        }
    }

    #[test]
    fn configuration_is_bounded_canonical_and_strict() {
        let indexes = parse_span_attribute_indexes(
            r#"[{"scope":"scope","path":"/attributes/debug"},{"scope":"span","path":"/nested/a~1b"}]"#,
        )
        .unwrap();
        assert_eq!(
            encode_span_attribute_indexes(&indexes),
            r#"[{"path":"/nested/a~1b","scope":"span"},{"path":"/attributes/debug","scope":"scope"}]"#
        );
        assert!(parse_span_attribute_indexes(r#"[{"scope":"event","path":"/x"}]"#).is_err());
        assert!(parse_span_attribute_indexes(r#"[{"scope":"span","path":"x"}]"#).is_err());
        assert!(parse_span_attribute_indexes(r#"[{"scope":"span","path":"/~2"}]"#).is_err());
        assert!(
            parse_span_attribute_indexes(r#"[{"scope":"span","path":"/x","extra":1}]"#).is_err()
        );
    }

    #[test]
    fn exact_filter_distinguishes_missing_null_empty_and_types() {
        let entry = entry();
        for (value, expected) in [
            (r#"{"scope":"span","path":"/missing","value":null}"#, false),
            (r#"{"scope":"span","path":"/null","value":null}"#, true),
            (r#"{"scope":"span","path":"/empty","value":""}"#, true),
            (r#"{"scope":"span","path":"/integer","value":1}"#, true),
            (r#"{"scope":"span","path":"/integer","value":1.0}"#, false),
            (r#"{"scope":"span","path":"/string","value":"1"}"#, true),
            (
                r#"{"scope":"span","path":"/nested/a~1b","value":true}"#,
                true,
            ),
            (
                r#"{"scope":"resource","path":"/deployment.environment","value":"test"}"#,
                true,
            ),
            (
                r#"{"scope":"scope","path":"/attributes/debug","value":false}"#,
                true,
            ),
        ] {
            assert_eq!(
                SpanAttributeFilter::parse(value)
                    .unwrap()
                    .matches_entry(&entry)
                    .unwrap(),
                expected,
                "{value}"
            );
        }
        assert!(
            SpanAttributeFilter::parse(r#"{"scope":"span","path":"/integer","value":[1]}"#)
                .is_err()
        );
    }

    #[test]
    fn bloom_has_no_false_negatives_and_validates_shape() {
        let indexes = parse_span_attribute_indexes(
            r#"[{"scope":"span","path":"/integer"},{"scope":"resource","path":"/deployment.environment"}]"#,
        )
        .unwrap();
        let blooms = build_span_attribute_blooms(&[entry()], &indexes).unwrap();
        assert_eq!(blooms.len(), 2);
        assert!(blooms[0].might_contain("1").unwrap());
        assert!(blooms[1].might_contain(r#""test""#).unwrap());
        assert_ne!(span_attribute_bloom_checksum(&blooms[0].bits), [0; 8]);
        assert!(validate_span_attribute_bloom(&blooms[0].bits[..10]).is_err());
    }
}
