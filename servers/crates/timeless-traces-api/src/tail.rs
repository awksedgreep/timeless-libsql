//! Live tail: streaming of admitted spans.
//!
//! Subscribers register a filter; the OTLP insert route -- the single
//! admission path -- publishes each accepted batch to the hub once storage
//! has taken it. Fan-out is in-memory only: nothing touches the extension,
//! and a subscriber's row shape is exactly the dashboard query surface's
//! (`otlp::tail_row`), so a live span and a searched span read alike.
//!
//! Filtering happens here rather than at the subscriber. A tail with no
//! filter on a busy service is the whole span firehose, and making each
//! client discard what it did not want would spend that bandwidth to reach
//! the same result.
//!
//! Slow consumers never backpressure ingest: each subscriber owns a bounded
//! channel and spans that do not fit are dropped and counted, the standard
//! tail contract. Serialization happens once per span per batch, and only
//! when at least one subscriber exists -- an idle hub costs one atomic load
//! per publish call.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::otlp::{self, Span};

/// Spans buffered per subscriber before drops begin. Sized for bursts, not
/// for durable delivery -- tail is a live view, the store is the record.
pub(crate) const SUBSCRIBER_BUFFER: usize = 1_024;

/// The live-matchable subset of the dashboard's search parameters. Time
/// bounds and paging are deliberately absent: a live stream is already
/// bounded by now, and there is no page to skip to.
///
/// `service`, `kind` and `status` match exactly, as their SQL counterparts
/// do. `name` reproduces `query::dashboard_name_matches` -- a case-insensitive
/// substring of the span name or of any string-valued attribute -- so the
/// same filter text selects the same spans live as it does in a search.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SpanFilter {
    pub service: Option<String>,
    pub name: Option<String>,
    pub kind: Option<String>,
    pub status: Option<String>,
    /// Span attributes that must all be present with these exact values.
    /// Distinct from `name`, which searches attribute values as substrings:
    /// this selects a specific attribute by key, which is what pinning a
    /// stream to one host needs.
    pub attributes: BTreeMap<String, String>,
}

impl SpanFilter {
    pub(crate) fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    fn matches(&self, row: &Value) -> bool {
        let field = |key: &str| row.get(key).and_then(Value::as_str).unwrap_or_default();

        if let Some(service) = &self.service {
            if field("service") != service {
                return false;
            }
        }
        if let Some(kind) = &self.kind {
            if field("kind") != kind {
                return false;
            }
        }
        if let Some(status) = &self.status {
            if field("status") != status {
                return false;
            }
        }
        if !self.attributes.is_empty() {
            let actual = row.get("attributes").and_then(Value::as_object);
            for (key, expected) in &self.attributes {
                let matched = actual
                    .and_then(|attributes| attributes.get(key))
                    .is_some_and(|value| &scalar_text(value) == expected);
                if !matched {
                    return false;
                }
            }
        }
        if let Some(pattern) = &self.name {
            let pattern = pattern.to_lowercase();
            let name_matches = field("name").to_lowercase().contains(&pattern);
            let attribute_matches = || {
                row.get("attributes")
                    .and_then(Value::as_object)
                    .is_some_and(|attributes| {
                        attributes.values().any(|value| {
                            value
                                .as_str()
                                .is_some_and(|value| value.to_lowercase().contains(&pattern))
                        })
                    })
            };
            if !name_matches && !attribute_matches() {
                return false;
            }
        }
        true
    }
}

/// Tail query parameters, named as the dashboard search names them so one
/// filter can be moved between a search and a live tail unchanged.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub(crate) struct TailParams {
    pub name: Option<String>,
    pub service: Option<String>,
    pub kind: Option<String>,
    pub status: Option<String>,
    /// A JSON object of attribute keys to required values, e.g.
    /// `{"host.name":"srv1"}`. One parameter rather than a repeated one
    /// because attribute keys are dotted and arbitrary, and inventing a
    /// prefix convention for them invites collisions with real parameters.
    pub attributes: Option<String>,
}

impl TailParams {
    /// Rejects a kind or status outside the enumerated set. A tail that
    /// quietly accepted `kind=srever` would stream nothing and look exactly
    /// like a system with no matching spans.
    pub(crate) fn into_filter(self) -> Result<SpanFilter, String> {
        let nonempty = |value: Option<String>| value.filter(|value| !value.is_empty());

        let kind = nonempty(self.kind);
        if kind.as_deref().is_some_and(|value| {
            !matches!(
                value,
                "internal" | "server" | "client" | "producer" | "consumer"
            )
        }) {
            return Err("invalid dashboard span kind".into());
        }
        let status = nonempty(self.status);
        if status
            .as_deref()
            .is_some_and(|value| !matches!(value, "unset" | "ok" | "error"))
        {
            return Err("invalid dashboard span status".into());
        }
        let attributes = match nonempty(self.attributes) {
            None => BTreeMap::new(),
            Some(text) => parse_attributes(&text)?,
        };

        Ok(SpanFilter {
            service: nonempty(self.service),
            name: nonempty(self.name),
            kind,
            status,
            attributes,
        })
    }
}

/// Attribute values are compared as text. A string compares as itself;
/// numbers and booleans compare as they render, so `{"http.status_code":"500"}`
/// selects the span whether the exporter sent 500 or "500". Containers have no
/// sensible scalar form and match nothing.
fn scalar_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(_) | Value::Bool(_) => value.to_string(),
        _ => String::new(),
    }
}

/// Rejected rather than ignored: a filter that silently did nothing would
/// stream every span while appearing to be pinned to one host.
fn parse_attributes(text: &str) -> Result<BTreeMap<String, String>, String> {
    let parsed: Value = serde_json::from_str(text)
        .map_err(|_| "attributes must be a JSON object of string values".to_owned())?;
    let Some(object) = parsed.as_object() else {
        return Err("attributes must be a JSON object of string values".into());
    };
    object
        .iter()
        .map(|(key, value)| match value {
            Value::String(text) => Ok((key.clone(), text.clone())),
            Value::Number(_) | Value::Bool(_) => Ok((key.clone(), value.to_string())),
            _ => Err(format!(
                "attribute {key} must be a string, number or boolean"
            )),
        })
        .collect()
}

pub(crate) struct TailHub {
    subscribers: Mutex<Vec<Subscriber>>,
    active: AtomicUsize,
    next_id: AtomicU64,
    sent: AtomicU64,
    dropped: AtomicU64,
}

struct Subscriber {
    id: u64,
    filter: Option<SpanFilter>,
    sender: mpsc::Sender<String>,
}

pub(crate) struct TailSubscription {
    pub(crate) receiver: mpsc::Receiver<String>,
    id: u64,
    hub: Arc<TailHub>,
}

impl Drop for TailSubscription {
    fn drop(&mut self) {
        self.hub.unsubscribe(self.id);
    }
}

impl TailHub {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            subscribers: Mutex::new(Vec::new()),
            active: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            sent: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        })
    }

    pub(crate) fn subscribe(self: &Arc<Self>, filter: Option<SpanFilter>) -> TailSubscription {
        let (sender, receiver) = mpsc::channel(SUBSCRIBER_BUFFER);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut subscribers = self.subscribers.lock().expect("tail hub lock");
        subscribers.push(Subscriber { id, filter, sender });
        self.active.store(subscribers.len(), Ordering::Relaxed);
        drop(subscribers);
        TailSubscription {
            receiver,
            id,
            hub: Arc::clone(self),
        }
    }

    fn unsubscribe(&self, id: u64) {
        let mut subscribers = self.subscribers.lock().expect("tail hub lock");
        subscribers.retain(|subscriber| subscriber.id != id);
        self.active.store(subscribers.len(), Ordering::Relaxed);
    }

    /// A clone of a live subscriber's sender, for per-connection heartbeats.
    pub(crate) fn heartbeat_sender(&self, id: u64) -> Option<mpsc::Sender<String>> {
        let subscribers = self.subscribers.lock().expect("tail hub lock");
        subscribers
            .iter()
            .find(|subscriber| subscriber.id == id)
            .map(|subscriber| subscriber.sender.clone())
    }

    pub(crate) fn subscription_id(subscription: &TailSubscription) -> u64 {
        subscription.id
    }

    pub(crate) fn stats(&self) -> (u64, u64, u64) {
        (
            self.active.load(Ordering::Relaxed) as u64,
            self.sent.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed),
        )
    }

    /// Publish an admitted batch. Rows serialize lazily: the JSON line for a
    /// span is built at most once per batch regardless of subscriber count.
    pub(crate) fn publish(&self, spans: &[Span]) {
        if self.active.load(Ordering::Relaxed) == 0 {
            return;
        }
        let subscribers = self.subscribers.lock().expect("tail hub lock");
        if subscribers.is_empty() {
            return;
        }
        for span in spans {
            let Some(row) = otlp::tail_row(span) else {
                continue;
            };
            let mut line: Option<String> = None;
            for subscriber in subscribers.iter() {
                let matched = match &subscriber.filter {
                    None => true,
                    Some(filter) => filter.matches(&row),
                };
                if !matched {
                    continue;
                }
                let line = line.get_or_insert_with(|| {
                    let mut text = row.to_string();
                    text.push('\n');
                    text
                });
                match subscriber.sender.try_send(line.clone()) {
                    Ok(()) => {
                        self.sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row() -> Value {
        json!({
            "name": "GET /orders",
            "service": "checkout",
            "kind": "server",
            "status": "error",
            "attributes": {"http.route": "/orders", "http.status_code": 500},
        })
    }

    #[test]
    fn an_empty_filter_matches_everything() {
        let filter = SpanFilter::default();
        assert!(filter.is_empty());
        assert!(filter.matches(&row()));
    }

    #[test]
    fn service_kind_and_status_match_exactly() {
        let matching = SpanFilter {
            service: Some("checkout".into()),
            kind: Some("server".into()),
            status: Some("error".into()),
            ..SpanFilter::default()
        };
        assert!(matching.matches(&row()));

        // Exact, as the SQL counterpart is: a prefix is a different service.
        let prefix = SpanFilter {
            service: Some("check".into()),
            ..SpanFilter::default()
        };
        assert!(!prefix.matches(&row()));

        let other_kind = SpanFilter {
            kind: Some("client".into()),
            ..SpanFilter::default()
        };
        assert!(!other_kind.matches(&row()));
    }

    #[test]
    fn every_filter_must_match() {
        // Right service, wrong status: AND, not OR.
        let filter = SpanFilter {
            service: Some("checkout".into()),
            status: Some("ok".into()),
            ..SpanFilter::default()
        };
        assert!(!filter.matches(&row()));
    }

    #[test]
    fn name_matches_a_case_insensitive_substring() {
        for pattern in ["orders", "ORDERS", "GET /orders"] {
            let filter = SpanFilter {
                name: Some(pattern.into()),
                ..SpanFilter::default()
            };
            assert!(filter.matches(&row()), "{pattern} should match");
        }

        let filter = SpanFilter {
            name: Some("payments".into()),
            ..SpanFilter::default()
        };
        assert!(!filter.matches(&row()));
    }

    #[test]
    fn name_also_searches_string_valued_attributes() {
        // Mirrors query::dashboard_name_matches, so the same text selects the
        // same spans live as it does in a search.
        let filter = SpanFilter {
            name: Some("/orders".into()),
            ..SpanFilter::default()
        };
        assert!(filter.matches(&row()));

        // Non-string attributes are not stringified and searched: matching
        // "500" against http.status_code would also match a duration or an ID.
        let filter = SpanFilter {
            name: Some("500".into()),
            ..SpanFilter::default()
        };
        assert!(!filter.matches(&row()));
    }

    #[test]
    fn a_row_missing_the_filtered_field_does_not_match() {
        let filter = SpanFilter {
            service: Some("checkout".into()),
            ..SpanFilter::default()
        };
        assert!(!filter.matches(&json!({"name": "orphan"})));
    }

    #[test]
    fn attributes_match_a_specific_key_exactly() {
        let filter = TailParams {
            attributes: Some(r#"{"http.route":"/orders"}"#.into()),
            ..TailParams::default()
        }
        .into_filter()
        .unwrap();
        assert!(!filter.is_empty());
        assert!(filter.matches(&row()));

        // Keyed and exact, unlike `name`, which searches attribute values as
        // substrings: this is what pinning a stream to one host needs.
        let partial = TailParams {
            attributes: Some(r#"{"http.route":"/order"}"#.into()),
            ..TailParams::default()
        }
        .into_filter()
        .unwrap();
        assert!(!partial.matches(&row()));

        let wrong_key = TailParams {
            attributes: Some(r#"{"http.path":"/orders"}"#.into()),
            ..TailParams::default()
        }
        .into_filter()
        .unwrap();
        assert!(!wrong_key.matches(&row()));
    }

    #[test]
    fn every_named_attribute_must_match() {
        let filter = TailParams {
            attributes: Some(r#"{"http.route":"/orders","http.status_code":"404"}"#.into()),
            ..TailParams::default()
        }
        .into_filter()
        .unwrap();
        assert!(!filter.matches(&row()));
    }

    #[test]
    fn attribute_values_compare_as_text_whatever_the_exporter_sent() {
        // http.status_code is a JSON number on the row; a filter is text.
        let filter = TailParams {
            attributes: Some(r#"{"http.status_code":"500"}"#.into()),
            ..TailParams::default()
        }
        .into_filter()
        .unwrap();
        assert!(filter.matches(&row()));
    }

    #[test]
    fn malformed_attributes_are_rejected_rather_than_ignored() {
        // Ignoring them would stream every span while appearing pinned.
        for bad in [r#"not json"#, r#"["a"]"#, r#"{"k":{"nested":1}}"#] {
            let params = TailParams {
                attributes: Some(bad.into()),
                ..TailParams::default()
            };
            assert!(params.into_filter().is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn params_reject_a_kind_or_status_outside_the_enumerated_set() {
        let bad_kind = TailParams {
            kind: Some("srever".into()),
            ..TailParams::default()
        };
        assert!(bad_kind.into_filter().is_err());

        let bad_status = TailParams {
            status: Some("failed".into()),
            ..TailParams::default()
        };
        assert!(bad_status.into_filter().is_err());
    }

    #[test]
    fn params_treat_blank_values_as_absent() {
        // A cleared field in a UI submits as empty, and means "no filter" --
        // not "match the empty string", which would match nothing at all.
        let params = TailParams {
            service: Some(String::new()),
            name: Some(String::new()),
            kind: Some(String::new()),
            status: Some(String::new()),
            attributes: Some(String::new()),
        };
        assert!(params.into_filter().unwrap().is_empty());
    }

    #[test]
    fn params_carry_every_field_through() {
        let filter = TailParams {
            name: Some("orders".into()),
            service: Some("checkout".into()),
            kind: Some("server".into()),
            status: Some("error".into()),
            attributes: Some(r#"{"http.route":"/orders"}"#.into()),
        }
        .into_filter()
        .unwrap();

        assert!(!filter.is_empty());
        assert!(filter.matches(&row()));
    }
}
