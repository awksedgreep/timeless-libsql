//! Tier 2 batch-blob encoders for all three signals. These are the same
//! wire formats the bench binaries ingest through (metrics blob v0, logs
//! blob v0, traces rich blob v2) — the decoders live in the extension and
//! PLAN.md is the canonical spec. Little-endian throughout.

fn put_text(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
}

// ---------------------------------------------------------------------------
// Metrics blob v0: header, series table, then three columnar sections
// (series index u32, ts i64, value f64) in the same point order.
// ---------------------------------------------------------------------------

/// `series` is (name, canonical-labels-JSON); `points` is (series index
/// into that table, ts, value). Blobs are self-contained: every blob
/// carries the table for exactly the series its points reference.
pub fn encode_metrics_blob(series: &[(String, String)], points: &[(u32, i64, f64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + series.len() * 96 + points.len() * 20);
    out.push(0x01); // version
    out.push(0x00); // flags
    out.extend_from_slice(&0u16.to_le_bytes()); // reserved
    out.extend_from_slice(&(series.len() as u32).to_le_bytes());
    out.extend_from_slice(&(points.len() as u32).to_le_bytes());
    for (name, labels) in series {
        put_text(&mut out, name);
        put_text(&mut out, labels);
    }
    for (idx, _, _) in points {
        out.extend_from_slice(&idx.to_le_bytes());
    }
    for (_, ts, _) in points {
        out.extend_from_slice(&ts.to_le_bytes());
    }
    for (_, _, val) in points {
        out.extend_from_slice(&val.to_le_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// Logs blob v0: header, then columnar ts / level / message / metadata.
// ---------------------------------------------------------------------------

pub struct LogEntry {
    pub ts: i64, // unix millis
    /// 0=debug 1=info 2=warning 3=error (timeless-core's byte).
    pub level_num: u8,
    pub message: String,
    /// Canonical sorted flat JSON.
    pub metadata: String,
}

pub fn encode_logs_blob(data: &[LogEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + data.len() * 96);
    out.push(0x01);
    out.push(0x00);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    for e in data {
        out.extend_from_slice(&e.ts.to_le_bytes());
    }
    for e in data {
        out.push(e.level_num);
    }
    for e in data {
        put_text(&mut out, &e.message);
    }
    for e in data {
        put_text(&mut out, &e.metadata);
    }
    out
}

// ---------------------------------------------------------------------------
// Traces rich blob v2: header, then columnar ids / names / services /
// kinds / statuses / timings / attributes, followed by the rich sections
// (status message, events, resource, scope).
// ---------------------------------------------------------------------------

pub struct SpanEntry {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    /// All-zero means "no parent" on the wire.
    pub parent_span_id: [u8; 8],
    pub name: &'static str,
    pub service: String,
    /// 0=internal 1=server 2=client 3=producer 4=consumer.
    pub kind_num: u8,
    /// 0=unset 1=ok 2=error.
    pub status_num: u8,
    pub start_ts: i64, // unix nanos
    pub duration_ns: i64,
    pub attributes: String,
    pub status_message: String,
    pub events: String,
    pub resource: String,
    pub scope: String,
}

pub fn encode_spans_blob(data: &[SpanEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + data.len() * 256);
    out.push(0x02); // rich
    out.push(0x00);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    for e in data {
        out.extend_from_slice(&e.trace_id);
    }
    for e in data {
        out.extend_from_slice(&e.span_id);
    }
    for e in data {
        out.extend_from_slice(&e.parent_span_id);
    }
    for e in data {
        put_text(&mut out, e.name);
    }
    for e in data {
        put_text(&mut out, &e.service);
    }
    for e in data {
        out.push(e.kind_num);
    }
    for e in data {
        out.push(e.status_num);
    }
    for e in data {
        out.extend_from_slice(&e.start_ts.to_le_bytes());
    }
    for e in data {
        out.extend_from_slice(&e.duration_ns.to_le_bytes());
    }
    for e in data {
        put_text(&mut out, &e.attributes);
    }
    for e in data {
        put_text(&mut out, &e.status_message);
    }
    for e in data {
        put_text(&mut out, &e.events);
    }
    for e in data {
        put_text(&mut out, &e.resource);
    }
    for e in data {
        put_text(&mut out, &e.scope);
    }
    out
}
