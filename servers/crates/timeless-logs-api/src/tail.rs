//! Live tail: VictoriaLogs-compatible streaming of admitted log entries.
//!
//! Subscribers register a compiled LogsQL predicate; `Storage::ingest` — the
//! single admission choke point every insert route funnels through —
//! publishes each admitted batch to the hub. Fan-out is in-memory only:
//! nothing touches the extension, and a subscriber's row shape is exactly
//! the query surface's (`response_row`): metadata fields at the top level
//! plus `_time`, `_msg`, and `level`.
//!
//! Slow consumers never backpressure ingest: each subscriber owns a bounded
//! channel and entries that do not fit are dropped and counted, the standard
//! tail contract. Serialization happens once per entry per batch, and only
//! when at least one subscriber exists — an idle hub costs one atomic load
//! per ingest call.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::pipeline::format_timestamp;
use crate::storage::LogEntry;
use crate::{LogPredicate, TimestampUnit};

/// Entries buffered per subscriber before drops begin. Sized for bursts, not
/// for durable delivery — tail is a live view, the store is the record.
pub(crate) const SUBSCRIBER_BUFFER: usize = 1_024;

static NEVER_CANCELLED: AtomicBool = AtomicBool::new(false);

pub(crate) struct TailHub {
    subscribers: Mutex<Vec<Subscriber>>,
    active: AtomicUsize,
    next_id: AtomicU64,
    sent: AtomicU64,
    dropped: AtomicU64,
}

struct Subscriber {
    id: u64,
    predicate: Option<LogPredicate>,
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

    pub(crate) fn subscribe(self: &Arc<Self>, predicate: Option<LogPredicate>) -> TailSubscription {
        let (sender, receiver) = mpsc::channel(SUBSCRIBER_BUFFER);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut subscribers = self.subscribers.lock().expect("tail hub lock");
        subscribers.push(Subscriber {
            id,
            predicate,
            sender,
        });
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

    /// Publish an admitted batch. Rows serialize lazily: the JSON line for an
    /// entry is built at most once per batch regardless of subscriber count.
    pub(crate) fn publish(&self, entries: &[LogEntry], timestamp_unit: TimestampUnit) {
        if self.active.load(Ordering::Relaxed) == 0 {
            return;
        }
        let subscribers = self.subscribers.lock().expect("tail hub lock");
        if subscribers.is_empty() {
            return;
        }
        for entry in entries {
            let Some(row) = tail_row(entry, timestamp_unit) else {
                continue;
            };
            let mut line: Option<String> = None;
            for subscriber in subscribers.iter() {
                let matched = match &subscriber.predicate {
                    None => true,
                    Some(predicate) => crate::pipeline::predicate_matches(
                        predicate,
                        &row,
                        timestamp_unit,
                        &NEVER_CANCELLED,
                    )
                    .unwrap_or(false),
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

/// The query surface's row shape (`pipeline::response_row`), built from an
/// ingest-side entry. Metadata that fails to decode yields no row rather
/// than a malformed one — the entry itself is still stored normally.
fn tail_row(entry: &LogEntry, timestamp_unit: TimestampUnit) -> Option<Value> {
    let metadata: Map<String, Value> = serde_json::from_str(&entry.metadata_json).ok()?;
    let mut object = metadata;
    object.insert(
        "_time".into(),
        Value::String(format_timestamp(entry.ts, timestamp_unit).ok()?),
    );
    object.insert("_msg".into(), Value::String(entry.message.clone()));
    object.insert("level".into(), Value::String(entry.severity.clone()));
    Some(Value::Object(object))
}
