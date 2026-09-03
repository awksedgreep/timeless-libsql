//! Columnar span block codec — the traces sibling of blocks/codec.rs
//! (read that header first; the container idea, the RAW/ZSTD/COLUMNAR
//! codec byte semantics are identical, and the zstd helpers + bounds-
//! checked Reader come from the shared timeless-codec crate).
//!
//! Generation 3 has twenty-four columns — spans are wider than log lines, and
//! the columnar split is where the compression comes from: each column
//! is long runs of SIMILAR data (all the 16-byte trace ids together,
//! all the u8 kinds together...), which every codec rewards far more
//! than interleaved span structs would:
//!
//!   col  1  trace_ids   16 bytes fixed per span, packed binary
//!   col  2  span_ids    8 bytes fixed per span
//!   col  3  parent_ids  1 presence byte (0|1), then 8 bytes IF present
//!                       (root spans pay 1 byte, not 9 zeros)
//!   col  4  names       UTF-8 strings (u16-len-prefixed in codecs 1/2)
//!   col  5  services    UTF-8 strings (ditto)
//!   col  6  kinds       one u8 per span (0..=4)
//!   col  7  statuses    one u8 per span (0..=2)
//!   col  8  start_ts    i64 per span
//!   col  9  durations   i64 per span
//!   col 10  attributes  canonical typed JSON object
//!   col 11  status_description
//!   col 12  events      canonical typed JSON array
//!   col 13  resource    canonical typed JSON object
//!   col 14  instrumentation_scope canonical typed JSON object
//!   col 15  links       canonical typed JSON array
//!   col 16  trace_state UTF-8 text
//!   col 17  trace_flags u32
//!   col 18  dropped_attributes_count u32
//!   col 19  dropped_events_count u32
//!   col 20  dropped_links_count u32
//!   col 21  resource_schema_url UTF-8 text
//!   col 22  scope_schema_url UTF-8 text
//!   col 23  resource_dropped_attributes_count u32
//!   col 24  scope_dropped_attributes_count u32
//!
//! Codec map (same ids as logs — the constants ARE the logs constants):
//!   CODEC_RAW      (1) — everything uncompressed; the flush format.
//!   CODEC_ZSTD     (2) — start_ts delta-encoded, every column zstd'd.
//!                        The Session 6 format; still decodable, no
//!                        longer written by optimize().
//!   codec 3 reserved for OpenZL, untouched.
//!   CODEC_COLUMNAR_V2 (5) — the optimized adaptive columnar codec.
//!                        In generation 2, each JSON document is an
//!                        ordinary adaptive string column: shredding
//!                        typed/nested JSON into string pairs would be
//!                        lossy. Generation-1 codec-5 pair shredding
//!                        remains decode-compatible forever.
//!   CODEC_COLUMNAR (4) — "adaptive columnar v1", per-column typed
//!                        encoders from timeless-codec (the Session 7
//!                        format; still decodable, no longer written
//!                        by optimize()):
//!             start_ts/durations → encode_i64 (delta+pco vs delta+zstd)
//!             kinds/statuses     → encode_u8  (RLE vs zstd; status-pure
//!                                  blocks collapse to one RLE pair)
//!             names/services     → encode_str (services are ~10
//!                                  distinct values → the dictionary
//!                                  strategy fires; names are a bounded
//!                                  set too)
//!             trace/span ids     → encode_fixed_bytes (zstd only —
//!                                  random ids are irreducible; see
//!                                  that encoder's doc for why there's
//!                                  no byte-plane transpose tonight)
//!             parent_ids         → presence-byte serialization + zstd
//!                                  (NOT encode_fixed_bytes: the column
//!                                  is variable-width by construction —
//!                                  splitting it into a presence u8
//!                                  column + packed ids is a format
//!                                  revision for another night)
//!             rich JSON/text     → adaptive string encoding
//!
//! Name/service lengths are u16 (a >64KB operation name is nonsense and
//! rejected, same policy as metadata keys in logs); attribute values
//! get u32 like log metadata values (they can legitimately be long).
//!
//! Container layout (all integers little-endian, identical for all
//! codecs — only the column payloads differ):
//!
//!   offset  size   field
//!   0       1      format version (0x03 for new writes; 0x01/0x02 readable)
//!   1       1      codec (1, 2, 4 or 5; 3 reserved for OpenZL)
//!   2       4      u32 entry_count
//!   6       8      i64 ts_min   (min start_ts)
//!   14      8      i64 ts_max   (max start_ts)
//!   22      24×4   u32 stored length of each generation-3 column
//!   118     —      the 24 columns, back to back
//!
//! decode_span_block() is the exact inverse and validates everything —
//! a truncated or corrupt block is an error naming the field, never a
//! panic or garbage spans.

use timeless_codec::{
    check_block_range, check_entry_count, decode_fixed_bytes, decode_i64, decode_str,
    decode_str_selected, decode_u8, encode_fixed_bytes, encode_i64, encode_str, encode_u8,
    zstd_compress, zstd_decompress, Reader,
};

pub use crate::blocks::codec::{CODEC_COLUMNAR, CODEC_COLUMNAR_V2, CODEC_RAW, CODEC_ZSTD};

use crate::blocks::codec::decode_pairs_column;

use super::{BlockMeta, SpanEntry};

const FORMAT_VERSION_V1: u8 = 1;
const FORMAT_VERSION_V2: u8 = 2;
const FORMAT_VERSION: u8 = 3;
const V1_N_COLUMNS: usize = 10;
const V2_N_COLUMNS: usize = 14;
const N_COLUMNS: usize = 24;
const HEADER_LEN: usize = 22 + N_COLUMNS * 4; // 118

const COLUMN_NAMES: [&str; N_COLUMNS] = [
    "trace_id column",
    "span_id column",
    "parent_id column",
    "name column",
    "service column",
    "kind column",
    "status column",
    "start_ts column",
    "duration column",
    "attributes column",
    "status_description column",
    "events column",
    "resource column",
    "instrumentation_scope column",
    "links column",
    "trace_state column",
    "trace_flags column",
    "dropped_attributes_count column",
    "dropped_events_count column",
    "dropped_links_count column",
    "resource_schema_url column",
    "scope_schema_url column",
    "resource_dropped_attributes_count column",
    "scope_dropped_attributes_count column",
];

const V1_COLUMN_NAMES: [&str; V1_N_COLUMNS] = [
    "trace_id column",
    "span_id column",
    "parent_id column",
    "name column",
    "service column",
    "kind column",
    "status column",
    "start_ts column",
    "duration column",
    "attributes column",
];

/// Physical/output span columns requested by a query. The original fourteen
/// public columns map directly. The ten additive rich-span-v2 columns share
/// one fidelity bit because SQLite's signed 32-bit `idxNum` cannot carry 24
/// projection bits above the nine predicate bits. They remain independent
/// physical/SQL columns; selecting any one late-materializes that group.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpanColumnMask(u16);

impl SpanColumnMask {
    pub const TRACE_ID: Self = Self(1 << 0);
    pub const SPAN_ID: Self = Self(1 << 1);
    pub const PARENT_SPAN_ID: Self = Self(1 << 2);
    pub const NAME: Self = Self(1 << 3);
    pub const SERVICE: Self = Self(1 << 4);
    pub const KIND: Self = Self(1 << 5);
    pub const STATUS: Self = Self(1 << 6);
    pub const START_TS: Self = Self(1 << 7);
    pub const DURATION_NS: Self = Self(1 << 8);
    pub const ATTRIBUTES: Self = Self(1 << 9);
    pub const STATUS_DESCRIPTION: Self = Self(1 << 10);
    pub const EVENTS: Self = Self(1 << 11);
    pub const RESOURCE: Self = Self(1 << 12);
    pub const INSTRUMENTATION_SCOPE: Self = Self(1 << 13);
    pub const FIDELITY_V2: Self = Self(1 << 14);
    pub const RICH: Self = Self(
        Self::ATTRIBUTES.0
            | Self::STATUS_DESCRIPTION.0
            | Self::EVENTS.0
            | Self::RESOURCE.0
            | Self::INSTRUMENTATION_SCOPE.0
            | Self::FIDELITY_V2.0,
    );
    pub const ALL: Self = Self((1 << 15) - 1);

    pub const fn from_bits(bits: u16) -> Self {
        Self(bits & Self::ALL.0)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Convert SQLite's visible-column bitmap into the compact projection
    /// vocabulary carried through `idxNum`.
    pub const fn from_col_used(bits: u64) -> Self {
        let original = bits & ((1_u64 << V2_N_COLUMNS) - 1);
        let fidelity = bits & (((1_u64 << N_COLUMNS) - 1) ^ ((1_u64 << V2_N_COLUMNS) - 1));
        Self::from_bits(
            original as u16
                | if fidelity != 0 {
                    Self::FIDELITY_V2.0
                } else {
                    0
                },
        )
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    const fn column(self, index: usize) -> bool {
        if index < V2_N_COLUMNS {
            self.0 & (1 << index) != 0
        } else {
            self.contains(Self::FIDELITY_V2)
        }
    }
}

/// Work performed below the row boundary by one projected block decode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpanDecodeProfile {
    pub columns: u64,
    pub column_bytes: u64,
    pub materialized_values: u64,
    pub materialized_rich_values: u64,
    pub examined_spans: u64,
}

impl SpanDecodeProfile {
    pub(crate) fn add(&mut self, other: Self) {
        self.columns = self.columns.saturating_add(other.columns);
        self.column_bytes = self.column_bytes.saturating_add(other.column_bytes);
        self.materialized_values = self
            .materialized_values
            .saturating_add(other.materialized_values);
        self.materialized_rich_values = self
            .materialized_rich_values
            .saturating_add(other.materialized_rich_values);
        self.examined_spans = self.examined_spans.saturating_add(other.examined_spans);
    }
}

/// Borrowed predicate view over only the columns requested by the engine.
pub(crate) struct SpanPredicateRow<'a> {
    pub trace_id: &'a [u8; 16],
    pub name: &'a str,
    pub service: &'a str,
    pub kind: u8,
    pub status: u8,
    pub start_ts: i64,
    pub duration_ns: i64,
    pub attributes: &'a str,
    pub resource: &'a str,
    pub instrumentation_scope: &'a str,
}

fn known_codec(codec: u8) -> bool {
    codec == CODEC_RAW
        || codec == CODEC_ZSTD
        || codec == CODEC_COLUMNAR
        || codec == CODEC_COLUMNAR_V2
}

/// Encode `entries` into one span block payload. Entries should already
/// be sorted by start_ts (the engine sorts at flush); the codec doesn't
/// REQUIRE it (deltas may be negative — see the negative-ts round-trip
/// test) but sorted input compresses better.
pub fn encode_span_block(
    entries: &[SpanEntry],
    codec: u8,
    zstd_level: i32,
) -> Result<(Vec<u8>, BlockMeta), String> {
    if entries.is_empty() {
        return Err("encode_span_block: refusing to encode an empty block".into());
    }
    if !known_codec(codec) {
        return Err(format!("encode_span_block: unknown codec {codec}"));
    }

    let mut ts_min = i64::MAX;
    let mut ts_max = i64::MIN;
    for e in entries {
        if e.kind > 4 {
            return Err(format!(
                "encode_span_block: span has invalid kind {} (must be 0..=4)",
                e.kind
            ));
        }
        if e.status > 2 {
            return Err(format!(
                "encode_span_block: span has invalid status {} (must be 0..=2)",
                e.status
            ));
        }
        ts_min = ts_min.min(e.start_ts);
        ts_max = ts_max.max(e.start_ts);
    }

    let n = entries.len();
    // The container's entry count is a u32 prefix (and BlockMeta carries
    // it as u32): reject over-wide blocks here so every `n as u32` below
    // is guarded by construction, never a silent wrap.
    if n > u32::MAX as usize {
        return Err("encode_span_block: entry count exceeds u32::MAX".into());
    }

    // ── Raw column material shared by every codec ────────────────────
    let mut col_trace = Vec::with_capacity(n * 16);
    let mut col_span = Vec::with_capacity(n * 8);
    let mut col_parent = Vec::with_capacity(n); // + 8/present
    let mut col_kind = Vec::with_capacity(n);
    let mut col_status = Vec::with_capacity(n);
    for e in entries {
        col_trace.extend_from_slice(&e.trace_id);
        col_span.extend_from_slice(&e.span_id);
        match &e.parent_span_id {
            Some(p) => {
                col_parent.push(1);
                col_parent.extend_from_slice(p);
            }
            None => col_parent.push(0),
        }
        col_kind.push(e.kind);
        col_status.push(e.status);

        // Name/service length policy is enforced in both branches below
        // (codec 4 hands strs to encode_str, which has its own u32
        // guard; the u16 rejection here documents OUR policy).
        for (label, s) in [("name", &e.name), ("service", &e.service)] {
            if s.len() > u16::MAX as usize {
                return Err(format!("encode_span_block: {label} longer than 64KB"));
            }
        }
        for (label, s) in [
            ("attributes", &e.attributes),
            ("status_description", &e.status_description),
            ("events", &e.events),
            ("resource", &e.resource),
            ("instrumentation_scope", &e.instrumentation_scope),
            ("links", &e.links),
            ("trace_state", &e.trace_state),
            ("resource_schema_url", &e.resource_schema_url),
            ("scope_schema_url", &e.scope_schema_url),
        ] {
            if s.len() > u32::MAX as usize {
                return Err(format!("encode_span_block: {label} exceeds u32::MAX bytes"));
            }
        }
    }

    let columns: Vec<Vec<u8>> = if codec == CODEC_COLUMNAR || codec == CODEC_COLUMNAR_V2 {
        // ── Codecs 4/5: typed encoders per column ───────────────────
        let starts: Vec<i64> = entries.iter().map(|e| e.start_ts).collect();
        let durs: Vec<i64> = entries.iter().map(|e| e.duration_ns).collect();
        let trace_flags: Vec<i64> = entries.iter().map(|e| i64::from(e.trace_flags)).collect();
        let dropped_attributes: Vec<i64> = entries
            .iter()
            .map(|e| i64::from(e.dropped_attributes_count))
            .collect();
        let dropped_events: Vec<i64> = entries
            .iter()
            .map(|e| i64::from(e.dropped_events_count))
            .collect();
        let dropped_links: Vec<i64> = entries
            .iter()
            .map(|e| i64::from(e.dropped_links_count))
            .collect();
        let resource_dropped_attributes: Vec<i64> = entries
            .iter()
            .map(|e| i64::from(e.resource_dropped_attributes_count))
            .collect();
        let scope_dropped_attributes: Vec<i64> = entries
            .iter()
            .map(|e| i64::from(e.scope_dropped_attributes_count))
            .collect();
        vec![
            encode_fixed_bytes(&col_trace, 16, zstd_level)?.to_bytes()?,
            encode_fixed_bytes(&col_span, 8, zstd_level)?.to_bytes()?,
            // Parent ids: variable-width presence serialization, plain
            // zstd (unframed, byte-identical to the codec-2 column).
            zstd_compress(&col_parent, zstd_level)?,
            encode_str(entries.iter().map(|e| e.name.as_str()), n, zstd_level)?.to_bytes()?,
            encode_str(entries.iter().map(|e| e.service.as_str()), n, zstd_level)?.to_bytes()?,
            encode_u8(&col_kind, zstd_level)?.to_bytes()?,
            encode_u8(&col_status, zstd_level)?.to_bytes()?,
            // Both i64 columns delta inside encode_i64; durations don't
            // trend but the adaptive pick just falls back to whichever
            // strategy handles their magnitude-similarity best.
            encode_i64(&starts, zstd_level)?.to_bytes()?,
            encode_i64(&durs, zstd_level)?.to_bytes()?,
            encode_str(entries.iter().map(|e| e.attributes.as_ref()), n, zstd_level)?.to_bytes()?,
            encode_str(
                entries.iter().map(|e| e.status_description.as_ref()),
                n,
                zstd_level,
            )?
            .to_bytes()?,
            encode_str(entries.iter().map(|e| e.events.as_ref()), n, zstd_level)?.to_bytes()?,
            encode_str(entries.iter().map(|e| e.resource.as_ref()), n, zstd_level)?.to_bytes()?,
            encode_str(
                entries.iter().map(|e| e.instrumentation_scope.as_ref()),
                n,
                zstd_level,
            )?
            .to_bytes()?,
            encode_str(entries.iter().map(|e| e.links.as_ref()), n, zstd_level)?.to_bytes()?,
            encode_str(
                entries.iter().map(|e| e.trace_state.as_ref()),
                n,
                zstd_level,
            )?
            .to_bytes()?,
            encode_i64(&trace_flags, zstd_level)?.to_bytes()?,
            encode_i64(&dropped_attributes, zstd_level)?.to_bytes()?,
            encode_i64(&dropped_events, zstd_level)?.to_bytes()?,
            encode_i64(&dropped_links, zstd_level)?.to_bytes()?,
            encode_str(
                entries.iter().map(|e| e.resource_schema_url.as_ref()),
                n,
                zstd_level,
            )?
            .to_bytes()?,
            encode_str(
                entries.iter().map(|e| e.scope_schema_url.as_ref()),
                n,
                zstd_level,
            )?
            .to_bytes()?,
            encode_i64(&resource_dropped_attributes, zstd_level)?.to_bytes()?,
            encode_i64(&scope_dropped_attributes, zstd_level)?.to_bytes()?,
        ]
    } else {
        // ── Codecs 1/2 — the Session 6 formats, byte-for-byte ────────
        let mut col_name = Vec::new();
        let mut col_svc = Vec::new();
        let mut col_ts = Vec::with_capacity(n * 8);
        let mut col_dur = Vec::with_capacity(n * 8);
        let mut col_attr = Vec::new();
        let mut col_status_description = Vec::new();
        let mut col_events = Vec::new();
        let mut col_resource = Vec::new();
        let mut col_scope = Vec::new();
        let mut col_links = Vec::new();
        let mut col_trace_state = Vec::new();
        let mut col_trace_flags = Vec::with_capacity(n * 4);
        let mut col_dropped_attributes = Vec::with_capacity(n * 4);
        let mut col_dropped_events = Vec::with_capacity(n * 4);
        let mut col_dropped_links = Vec::with_capacity(n * 4);
        let mut col_resource_schema_url = Vec::new();
        let mut col_scope_schema_url = Vec::new();
        let mut col_resource_dropped_attributes = Vec::with_capacity(n * 4);
        let mut col_scope_dropped_attributes = Vec::with_capacity(n * 4);
        let mut prev_ts = 0i64;
        for e in entries {
            for (s, col) in [(&e.name, &mut col_name), (&e.service, &mut col_svc)] {
                let b = s.as_bytes();
                col.extend_from_slice(&(b.len() as u16).to_le_bytes());
                col.extend_from_slice(b);
            }
            // start_ts: RAW stores absolutes, ZSTD stores deltas (first
            // absolute, then differences) — same scheme as the logs ts
            // column and for the same reason: steady traffic makes
            // deltas small repeated numbers, much better zstd food.
            if codec == CODEC_RAW {
                col_ts.extend_from_slice(&e.start_ts.to_le_bytes());
            } else {
                col_ts.extend_from_slice(&e.start_ts.wrapping_sub(prev_ts).to_le_bytes());
                prev_ts = e.start_ts;
            }
            col_dur.extend_from_slice(&e.duration_ns.to_le_bytes());
            for (s, col) in [
                (&e.attributes, &mut col_attr),
                (&e.status_description, &mut col_status_description),
                (&e.events, &mut col_events),
                (&e.resource, &mut col_resource),
                (&e.instrumentation_scope, &mut col_scope),
                (&e.links, &mut col_links),
                (&e.trace_state, &mut col_trace_state),
                (&e.resource_schema_url, &mut col_resource_schema_url),
                (&e.scope_schema_url, &mut col_scope_schema_url),
            ] {
                col.extend_from_slice(&(s.len() as u32).to_le_bytes());
                col.extend_from_slice(s.as_bytes());
            }
            col_trace_flags.extend_from_slice(&e.trace_flags.to_le_bytes());
            col_dropped_attributes.extend_from_slice(&e.dropped_attributes_count.to_le_bytes());
            col_dropped_events.extend_from_slice(&e.dropped_events_count.to_le_bytes());
            col_dropped_links.extend_from_slice(&e.dropped_links_count.to_le_bytes());
            col_resource_dropped_attributes
                .extend_from_slice(&e.resource_dropped_attributes_count.to_le_bytes());
            col_scope_dropped_attributes
                .extend_from_slice(&e.scope_dropped_attributes_count.to_le_bytes());
        }

        let raw_cols: [Vec<u8>; N_COLUMNS] = [
            col_trace,
            col_span,
            col_parent,
            col_name,
            col_svc,
            col_kind,
            col_status,
            col_ts,
            col_dur,
            col_attr,
            col_status_description,
            col_events,
            col_resource,
            col_scope,
            col_links,
            col_trace_state,
            col_trace_flags,
            col_dropped_attributes,
            col_dropped_events,
            col_dropped_links,
            col_resource_schema_url,
            col_scope_schema_url,
            col_resource_dropped_attributes,
            col_scope_dropped_attributes,
        ];
        if codec == CODEC_ZSTD {
            raw_cols
                .iter()
                .map(|c| zstd_compress(c, zstd_level))
                .collect::<Result<_, _>>()?
        } else {
            raw_cols.into_iter().collect()
        }
    };

    // ── Assemble container ───────────────────────────────────────────
    let total: usize = HEADER_LEN + columns.iter().map(|c| c.len()).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    out.push(FORMAT_VERSION);
    out.push(codec);
    out.extend_from_slice(&(n as u32).to_le_bytes());
    out.extend_from_slice(&ts_min.to_le_bytes());
    out.extend_from_slice(&ts_max.to_le_bytes());
    for c in &columns {
        if c.len() > u32::MAX as usize {
            return Err("encode_span_block: column exceeds u32::MAX bytes".into());
        }
        out.extend_from_slice(&(c.len() as u32).to_le_bytes());
    }
    for c in &columns {
        out.extend_from_slice(c);
    }

    let meta = BlockMeta {
        ts_min,
        ts_max,
        entry_count: n as u32,
        codec,
    };
    Ok((out, meta))
}

enum ProjectedColumn {
    TraceIds(Vec<[u8; 16]>),
    SpanIds(Vec<[u8; 8]>),
    Parents(Vec<Option<[u8; 8]>>),
    Strings(Vec<String>),
    Bytes(Vec<u8>),
    Integers(Vec<i64>),
    Unsigned(Vec<u32>),
}

impl ProjectedColumn {
    fn assign(&self, column: usize, source: usize, entry: &mut SpanEntry) {
        match (column, self) {
            (0, Self::TraceIds(values)) => entry.trace_id = values[source],
            (1, Self::SpanIds(values)) => entry.span_id = values[source],
            (2, Self::Parents(values)) => entry.parent_span_id = values[source],
            (3, Self::Strings(values)) => entry.name.clone_from(&values[source]),
            (4, Self::Strings(values)) => entry.service.clone_from(&values[source]),
            (5, Self::Bytes(values)) => entry.kind = values[source],
            (6, Self::Bytes(values)) => entry.status = values[source],
            (7, Self::Integers(values)) => entry.start_ts = values[source],
            (8, Self::Integers(values)) => entry.duration_ns = values[source],
            (9, Self::Strings(values)) => entry.attributes = values[source].clone().into(),
            (10, Self::Strings(values)) => entry.status_description = values[source].clone().into(),
            (11, Self::Strings(values)) => entry.events = values[source].clone().into(),
            (12, Self::Strings(values)) => entry.resource = values[source].clone().into(),
            (13, Self::Strings(values)) => {
                entry.instrumentation_scope = values[source].clone().into()
            }
            (14, Self::Strings(values)) => entry.links = values[source].clone().into(),
            (15, Self::Strings(values)) => entry.trace_state = values[source].clone().into(),
            (16, Self::Unsigned(values)) => entry.trace_flags = values[source],
            (17, Self::Unsigned(values)) => entry.dropped_attributes_count = values[source],
            (18, Self::Unsigned(values)) => entry.dropped_events_count = values[source],
            (19, Self::Unsigned(values)) => entry.dropped_links_count = values[source],
            (20, Self::Strings(values)) => {
                entry.resource_schema_url = values[source].clone().into()
            }
            (21, Self::Strings(values)) => entry.scope_schema_url = values[source].clone().into(),
            (22, Self::Unsigned(values)) => {
                entry.resource_dropped_attributes_count = values[source]
            }
            (23, Self::Unsigned(values)) => entry.scope_dropped_attributes_count = values[source],
            _ => unreachable!("projected span column type mismatch"),
        }
    }
}

fn empty_projected_span() -> SpanEntry {
    SpanEntry {
        trace_id: [0; 16],
        span_id: [0; 8],
        parent_span_id: None,
        name: String::new(),
        service: String::new(),
        kind: 0,
        status: 0,
        status_description: "".into(),
        start_ts: 0,
        duration_ns: 0,
        attributes: "{}".into(),
        events: "[]".into(),
        resource: "{}".into(),
        instrumentation_scope: "{}".into(),
        links: "[]".into(),
        trace_state: "".into(),
        trace_flags: 0,
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        resource_schema_url: "".into(),
        scope_schema_url: "".into(),
        resource_dropped_attributes_count: 0,
        scope_dropped_attributes_count: 0,
    }
}

fn selected_fixed<const WIDTH: usize>(flat: Vec<u8>, selected: &[usize]) -> Vec<[u8; WIDTH]> {
    selected
        .iter()
        .map(|index| {
            let start = index * WIDTH;
            flat[start..start + WIDTH].try_into().unwrap()
        })
        .collect()
}

fn selected_parents(
    raw: &[u8],
    n: usize,
    selected: &[usize],
) -> Result<Vec<Option<[u8; 8]>>, String> {
    let parents = parse_parents(raw, n)?;
    Ok(selected.iter().map(|index| parents[*index]).collect())
}

fn decode_columnar_column(
    stored: &[&[u8]],
    n: usize,
    column: usize,
    selected: &[usize],
) -> Result<ProjectedColumn, String> {
    match column {
        0 => Ok(ProjectedColumn::TraceIds(selected_fixed(
            decode_fixed_bytes(stored[0], n, 16)?,
            selected,
        ))),
        1 => Ok(ProjectedColumn::SpanIds(selected_fixed(
            decode_fixed_bytes(stored[1], n, 8)?,
            selected,
        ))),
        2 => {
            let raw = zstd_decompress(stored[2], COLUMN_NAMES[2])?;
            Ok(ProjectedColumn::Parents(selected_parents(
                &raw, n, selected,
            )?))
        }
        3 | 4 | 9..=15 | 20 | 21 => Ok(ProjectedColumn::Strings(decode_str_selected(
            stored[column],
            n,
            selected,
        )?)),
        5 | 6 => {
            let values = decode_u8(stored[column], n)?;
            if column == 5 {
                validate_kinds_statuses(&values, &[])?;
            } else {
                validate_kinds_statuses(&[], &values)?;
            }
            Ok(ProjectedColumn::Bytes(
                selected.iter().map(|index| values[*index]).collect(),
            ))
        }
        7 | 8 => {
            let values = decode_i64(stored[column], n)?;
            Ok(ProjectedColumn::Integers(
                selected.iter().map(|index| values[*index]).collect(),
            ))
        }
        16..=19 | 22 | 23 => {
            let values = decode_i64(stored[column], n)?;
            let values = values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    u32::try_from(value).map_err(|_| {
                        format!(
                            "span block: span {index}: {} value {value} is outside uint32",
                            COLUMN_NAMES[column]
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ProjectedColumn::Unsigned(
                selected.iter().map(|index| values[*index]).collect(),
            ))
        }
        _ => unreachable!("span projection column out of range"),
    }
}

fn parse_columnar(bytes: &[u8]) -> Result<Option<(usize, usize, Vec<&[u8]>)>, String> {
    let mut reader = Reader::new(bytes);
    let version = reader.u8("format version")?;
    let physical_columns = match version {
        FORMAT_VERSION_V2 => V2_N_COLUMNS,
        FORMAT_VERSION => N_COLUMNS,
        _ => return Ok(None),
    };
    let codec = reader.u8("codec")?;
    if !known_codec(codec) {
        return Err(format!("span block: unknown codec {codec}"));
    }
    if codec != CODEC_COLUMNAR && codec != CODEC_COLUMNAR_V2 {
        return Ok(None);
    }
    let n = reader.u32("entry_count")? as usize;
    // Ceiling only: the projected path returns slices, but every
    // downstream sized decode keys off this count.
    check_entry_count(n, "span block")?;
    let _ts_min = reader.i64("ts_min")?;
    let _ts_max = reader.i64("ts_max")?;
    let mut lengths = vec![0usize; physical_columns];
    for (index, length) in lengths.iter_mut().enumerate() {
        *length = reader.u32(COLUMN_NAMES[index])? as usize;
    }
    let mut stored = Vec::with_capacity(physical_columns);
    for (index, length) in lengths.iter().enumerate() {
        stored.push(reader.take(*length, COLUMN_NAMES[index])?);
    }
    if reader.remaining() != 0 {
        return Err(format!(
            "span block: {} trailing byte(s) after last column (corrupt header?)",
            reader.remaining()
        ));
    }
    Ok(Some((n, physical_columns, stored)))
}

fn projected_row<'a>(columns: &'a [Option<ProjectedColumn>], row: usize) -> SpanPredicateRow<'a> {
    static ZERO_TRACE: [u8; 16] = [0; 16];
    let trace_id = match &columns[0] {
        Some(ProjectedColumn::TraceIds(values)) => &values[row],
        _ => &ZERO_TRACE,
    };
    let string = |column| match &columns[column] {
        Some(ProjectedColumn::Strings(values)) => values[row].as_str(),
        _ => "",
    };
    let byte = |column| match &columns[column] {
        Some(ProjectedColumn::Bytes(values)) => values[row],
        _ => 0,
    };
    let integer = |column| match &columns[column] {
        Some(ProjectedColumn::Integers(values)) => values[row],
        _ => 0,
    };
    SpanPredicateRow {
        trace_id,
        name: string(3),
        service: string(4),
        kind: byte(5),
        status: byte(6),
        start_ts: integer(7),
        duration_ns: integer(8),
        attributes: string(9),
        resource: string(12),
        instrumentation_scope: string(13),
    }
}

fn entry_predicate_row(entry: &SpanEntry) -> SpanPredicateRow<'_> {
    SpanPredicateRow {
        trace_id: &entry.trace_id,
        name: &entry.name,
        service: &entry.service,
        kind: entry.kind,
        status: entry.status,
        start_ts: entry.start_ts,
        duration_ns: entry.duration_ns,
        attributes: entry.attributes.as_ref(),
        resource: entry.resource.as_ref(),
        instrumentation_scope: entry.instrumentation_scope.as_ref(),
    }
}

fn clear_unprojected(entry: &mut SpanEntry, mask: SpanColumnMask) {
    let defaults = empty_projected_span();
    if !mask.column(0) {
        entry.trace_id = defaults.trace_id;
    }
    if !mask.column(1) {
        entry.span_id = defaults.span_id;
    }
    if !mask.column(2) {
        entry.parent_span_id = None;
    }
    if !mask.column(3) {
        entry.name.clear();
    }
    if !mask.column(4) {
        entry.service.clear();
    }
    if !mask.column(5) {
        entry.kind = 0;
    }
    if !mask.column(6) {
        entry.status = 0;
    }
    if !mask.column(7) {
        entry.start_ts = 0;
    }
    if !mask.column(8) {
        entry.duration_ns = 0;
    }
    if !mask.column(9) {
        entry.attributes = defaults.attributes;
    }
    if !mask.column(10) {
        entry.status_description = defaults.status_description;
    }
    if !mask.column(11) {
        entry.events = defaults.events;
    }
    if !mask.column(12) {
        entry.resource = defaults.resource;
    }
    if !mask.column(13) {
        entry.instrumentation_scope = defaults.instrumentation_scope;
    }
    if !mask.contains(SpanColumnMask::FIDELITY_V2) {
        entry.links = defaults.links;
        entry.trace_state = defaults.trace_state;
        entry.trace_flags = 0;
        entry.dropped_attributes_count = 0;
        entry.dropped_events_count = 0;
        entry.dropped_links_count = 0;
        entry.resource_schema_url = defaults.resource_schema_url;
        entry.scope_schema_url = defaults.scope_schema_url;
        entry.resource_dropped_attributes_count = 0;
        entry.scope_dropped_attributes_count = 0;
    }
}

/// Predicate-first projected block decode. Generation-2/3 adaptive columnar
/// blocks decode only predicate columns first, then materialize requested
/// columns for matching rows. Older readable formats retain the exact full
/// decoder as a conservative compatibility fallback.
pub(crate) fn decode_span_block_projected<F>(
    bytes: &[u8],
    predicate_mask: SpanColumnMask,
    output_mask: SpanColumnMask,
    mut predicate: F,
) -> Result<(Vec<SpanEntry>, SpanDecodeProfile), String>
where
    F: FnMut(SpanPredicateRow<'_>) -> Result<bool, String>,
{
    let Some((n, physical_columns, stored)) = parse_columnar(bytes)? else {
        let entries = decode_span_block(bytes)?;
        let examined = entries.len() as u64;
        let materialized = predicate_mask.union(output_mask);
        let mut selected = Vec::new();
        for mut entry in entries {
            if predicate(entry_predicate_row(&entry))? {
                clear_unprojected(&mut entry, materialized);
                selected.push(entry);
            }
        }
        let physical_columns = match bytes.first().copied() {
            Some(FORMAT_VERSION_V1) => V1_N_COLUMNS,
            Some(FORMAT_VERSION_V2) => V2_N_COLUMNS,
            _ => N_COLUMNS,
        };
        return Ok((
            selected,
            SpanDecodeProfile {
                columns: physical_columns as u64,
                column_bytes: bytes.len() as u64,
                materialized_values: examined.saturating_mul(physical_columns as u64),
                materialized_rich_values: examined
                    .saturating_mul(physical_columns.saturating_sub(9) as u64),
                examined_spans: examined,
            },
        ));
    };

    let all_rows = (0..n).collect::<Vec<_>>();
    let mut profile = SpanDecodeProfile {
        examined_spans: n as u64,
        ..SpanDecodeProfile::default()
    };
    let mut columns = (0..N_COLUMNS).map(|_| None).collect::<Vec<_>>();
    for (column, slot) in columns.iter_mut().take(physical_columns).enumerate() {
        if predicate_mask.column(column) {
            *slot = Some(decode_columnar_column(&stored, n, column, &all_rows)?);
            profile.columns += 1;
            profile.column_bytes = profile
                .column_bytes
                .saturating_add(stored[column].len() as u64);
            profile.materialized_values = profile.materialized_values.saturating_add(n as u64);
            if SpanColumnMask::RICH.column(column) {
                profile.materialized_rich_values =
                    profile.materialized_rich_values.saturating_add(n as u64);
            }
        }
    }
    let mut selected = Vec::new();
    for row in 0..n {
        if predicate(projected_row(&columns, row))? {
            selected.push(row);
        }
    }
    if selected.is_empty() {
        return Ok((Vec::new(), profile));
    }

    let materialized = predicate_mask.union(output_mask);
    let mut entries = (0..selected.len())
        .map(|_| empty_projected_span())
        .collect::<Vec<_>>();
    for column in 0..N_COLUMNS {
        if !materialized.column(column) {
            continue;
        }
        if column >= physical_columns {
            continue;
        }
        if let Some(values) = &columns[column] {
            for (target, source) in selected.iter().copied().enumerate() {
                values.assign(column, source, &mut entries[target]);
            }
            continue;
        }
        let values = decode_columnar_column(&stored, n, column, &selected)?;
        profile.columns += 1;
        profile.column_bytes = profile
            .column_bytes
            .saturating_add(stored[column].len() as u64);
        profile.materialized_values = profile
            .materialized_values
            .saturating_add(selected.len() as u64);
        if SpanColumnMask::RICH.column(column) {
            profile.materialized_rich_values = profile
                .materialized_rich_values
                .saturating_add(selected.len() as u64);
        }
        for (target, entry) in entries.iter_mut().enumerate() {
            values.assign(column, target, entry);
        }
    }
    Ok((entries, profile))
}

/// Decode a span block payload back into spans, in stored order.
/// Generation 1 (the original ten-column/string-attribute layout) and
/// generation 2 (the first fourteen rich columns) are permanently readable.
/// Generation 3 appends complete rich-span-v2 fidelity.
pub fn decode_span_block(bytes: &[u8]) -> Result<Vec<SpanEntry>, String> {
    match bytes.first().copied() {
        Some(FORMAT_VERSION_V1) => decode_span_block_v1(bytes),
        Some(FORMAT_VERSION_V2 | FORMAT_VERSION) => decode_span_block_v2_v3(bytes),
        Some(version) => Err(format!(
            "span block: unsupported format version {version} (this build speaks 1, 2, and 3)"
        )),
        None => Err("span block: missing format version".into()),
    }
}

fn validate_kinds_statuses(kinds: &[u8], statuses: &[u8]) -> Result<(), String> {
    for (i, &kind) in kinds.iter().enumerate() {
        if kind > 4 {
            return Err(format!("span block: span {i} has invalid kind byte {kind}"));
        }
    }
    for (i, &status) in statuses.iter().enumerate() {
        if status > 2 {
            return Err(format!(
                "span block: span {i} has invalid status byte {status}"
            ));
        }
    }
    Ok(())
}

fn parse_len_strings(
    raw: &[u8],
    n: usize,
    label: &'static str,
    wide: bool,
) -> Result<Vec<String>, String> {
    let mut out = Vec::with_capacity(n);
    let mut r = Reader::new(raw);
    for i in 0..n {
        let len = if wide {
            r.u32(label)? as usize
        } else {
            r.u16(label)? as usize
        };
        let bytes = r.take(len, label)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| format!("span block: span {i}: {label} is not valid UTF-8"))?;
        out.push(value.to_owned());
    }
    if r.remaining() != 0 {
        return Err(format!("span block: trailing bytes in {label}"));
    }
    Ok(out)
}

fn decode_adaptive_u32(stored: &[u8], n: usize, label: &'static str) -> Result<Vec<u32>, String> {
    decode_i64(stored, n)?
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            u32::try_from(value).map_err(|_| {
                format!("span block: span {index}: {label} value {value} is outside uint32")
            })
        })
        .collect()
}

fn decode_raw_u32(raw: &[u8]) -> Vec<u32> {
    raw.as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| u32::from_le_bytes(*chunk))
        .collect()
}

fn decode_span_block_v2_v3(bytes: &[u8]) -> Result<Vec<SpanEntry>, String> {
    let mut r = Reader::new(bytes);
    let version = r.u8("format version")?;
    debug_assert!(version == FORMAT_VERSION_V2 || version == FORMAT_VERSION);
    let physical_columns = if version == FORMAT_VERSION_V2 {
        V2_N_COLUMNS
    } else {
        N_COLUMNS
    };
    let codec = r.u8("codec")?;
    if !known_codec(codec) {
        return Err(format!("span block: unknown codec {codec}"));
    }
    let n = r.u32("entry_count")? as usize;
    check_entry_count(n, "span block")?;
    let (ts_min, ts_max) = (r.i64("ts_min")?, r.i64("ts_max")?);
    let mut lens = vec![0usize; physical_columns];
    for (i, len) in lens.iter_mut().enumerate() {
        *len = r.u32(COLUMN_NAMES[i])? as usize;
    }
    let mut stored = Vec::with_capacity(physical_columns);
    for (i, len) in lens.iter().enumerate() {
        stored.push(r.take(*len, COLUMN_NAMES[i])?);
    }
    if r.remaining() != 0 {
        return Err(format!(
            "span block: {} trailing byte(s) after last column (corrupt header?)",
            r.remaining()
        ));
    }

    let (
        trace_flat,
        span_flat,
        parents,
        names,
        services,
        kinds,
        statuses,
        timestamps,
        durations,
        attributes,
        status_descriptions,
        events,
        resources,
        scopes,
    ) = if codec == CODEC_COLUMNAR || codec == CODEC_COLUMNAR_V2 {
        let trace_flat = decode_fixed_bytes(stored[0], n, 16)?;
        let span_flat = decode_fixed_bytes(stored[1], n, 8)?;
        let parent_raw = zstd_decompress(stored[2], COLUMN_NAMES[2])?;
        let parents = parse_parents(&parent_raw, n)?;
        let names = decode_str(stored[3], n)?;
        let services = decode_str(stored[4], n)?;
        let kinds = decode_u8(stored[5], n)?;
        let statuses = decode_u8(stored[6], n)?;
        let timestamps = decode_i64(stored[7], n)?;
        let durations = decode_i64(stored[8], n)?;
        let attributes = decode_str(stored[9], n)?;
        let status_descriptions = decode_str(stored[10], n)?;
        let events = decode_str(stored[11], n)?;
        let resources = decode_str(stored[12], n)?;
        let scopes = decode_str(stored[13], n)?;
        (
            trace_flat,
            span_flat,
            parents,
            names,
            services,
            kinds,
            statuses,
            timestamps,
            durations,
            attributes,
            status_descriptions,
            events,
            resources,
            scopes,
        )
    } else {
        let cols: Vec<Vec<u8>> = if codec == CODEC_ZSTD {
            stored
                .iter()
                .enumerate()
                .map(|(i, c)| zstd_decompress(c, COLUMN_NAMES[i]))
                .collect::<Result<_, _>>()?
        } else {
            stored.iter().map(|c| c.to_vec()).collect()
        };
        for (idx, want) in [
            (0usize, n * 16),
            (1, n * 8),
            (5, n),
            (6, n),
            (7, n * 8),
            (8, n * 8),
        ] {
            if cols[idx].len() != want {
                return Err(format!(
                    "span block: {} is {} bytes, expected {want} for {n} spans",
                    COLUMN_NAMES[idx],
                    cols[idx].len()
                ));
            }
        }
        let mut timestamps = Vec::with_capacity(n);
        let mut previous = 0i64;
        for chunk in cols[7].as_chunks::<8>().0 {
            let value = i64::from_le_bytes(*chunk);
            if codec == CODEC_ZSTD {
                previous = previous.wrapping_add(value);
                timestamps.push(previous);
            } else {
                timestamps.push(value);
            }
        }
        let durations = cols[8]
            .as_chunks::<8>()
            .0
            .iter()
            .map(|c| i64::from_le_bytes(*c))
            .collect();
        (
            cols[0].clone(),
            cols[1].clone(),
            parse_parents(&cols[2], n)?,
            parse_len_strings(&cols[3], n, COLUMN_NAMES[3], false)?,
            parse_len_strings(&cols[4], n, COLUMN_NAMES[4], false)?,
            cols[5].clone(),
            cols[6].clone(),
            timestamps,
            durations,
            parse_len_strings(&cols[9], n, COLUMN_NAMES[9], true)?,
            parse_len_strings(&cols[10], n, COLUMN_NAMES[10], true)?,
            parse_len_strings(&cols[11], n, COLUMN_NAMES[11], true)?,
            parse_len_strings(&cols[12], n, COLUMN_NAMES[12], true)?,
            parse_len_strings(&cols[13], n, COLUMN_NAMES[13], true)?,
        )
    };

    validate_kinds_statuses(&kinds, &statuses)?;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(SpanEntry {
            trace_id: trace_flat[i * 16..(i + 1) * 16].try_into().unwrap(),
            span_id: span_flat[i * 8..(i + 1) * 8].try_into().unwrap(),
            parent_span_id: parents[i],
            name: names[i].clone(),
            service: services[i].clone(),
            kind: kinds[i],
            status: statuses[i],
            status_description: status_descriptions[i].clone().into(),
            start_ts: timestamps[i],
            duration_ns: durations[i],
            attributes: attributes[i].clone().into(),
            events: events[i].clone().into(),
            resource: resources[i].clone().into(),
            instrumentation_scope: scopes[i].clone().into(),
            links: "[]".into(),
            trace_state: "".into(),
            trace_flags: 0,
            dropped_attributes_count: 0,
            dropped_events_count: 0,
            dropped_links_count: 0,
            resource_schema_url: "".into(),
            scope_schema_url: "".into(),
            resource_dropped_attributes_count: 0,
            scope_dropped_attributes_count: 0,
        });
    }
    if version == FORMAT_VERSION {
        let (
            links,
            trace_states,
            trace_flags,
            dropped_attributes,
            dropped_events,
            dropped_links,
            resource_schema_urls,
            scope_schema_urls,
            resource_dropped_attributes,
            scope_dropped_attributes,
        ) = if codec == CODEC_COLUMNAR || codec == CODEC_COLUMNAR_V2 {
            (
                decode_str(stored[14], n)?,
                decode_str(stored[15], n)?,
                decode_adaptive_u32(stored[16], n, COLUMN_NAMES[16])?,
                decode_adaptive_u32(stored[17], n, COLUMN_NAMES[17])?,
                decode_adaptive_u32(stored[18], n, COLUMN_NAMES[18])?,
                decode_adaptive_u32(stored[19], n, COLUMN_NAMES[19])?,
                decode_str(stored[20], n)?,
                decode_str(stored[21], n)?,
                decode_adaptive_u32(stored[22], n, COLUMN_NAMES[22])?,
                decode_adaptive_u32(stored[23], n, COLUMN_NAMES[23])?,
            )
        } else {
            let cols = stored[14..]
                .iter()
                .enumerate()
                .map(|(offset, column)| {
                    if codec == CODEC_ZSTD {
                        zstd_decompress(column, COLUMN_NAMES[14 + offset])
                    } else {
                        Ok(column.to_vec())
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            for index in [2_usize, 3, 4, 5, 8, 9] {
                if cols[index].len() != n * 4 {
                    return Err(format!(
                        "span block: {} is {} bytes, expected {} for {n} spans",
                        COLUMN_NAMES[14 + index],
                        cols[index].len(),
                        n * 4
                    ));
                }
            }
            (
                parse_len_strings(&cols[0], n, COLUMN_NAMES[14], true)?,
                parse_len_strings(&cols[1], n, COLUMN_NAMES[15], true)?,
                decode_raw_u32(&cols[2]),
                decode_raw_u32(&cols[3]),
                decode_raw_u32(&cols[4]),
                decode_raw_u32(&cols[5]),
                parse_len_strings(&cols[6], n, COLUMN_NAMES[20], true)?,
                parse_len_strings(&cols[7], n, COLUMN_NAMES[21], true)?,
                decode_raw_u32(&cols[8]),
                decode_raw_u32(&cols[9]),
            )
        };
        for (index, entry) in out.iter_mut().enumerate() {
            entry.links = links[index].clone().into();
            entry.trace_state = trace_states[index].clone().into();
            entry.trace_flags = trace_flags[index];
            entry.dropped_attributes_count = dropped_attributes[index];
            entry.dropped_events_count = dropped_events[index];
            entry.dropped_links_count = dropped_links[index];
            entry.resource_schema_url = resource_schema_urls[index].clone().into();
            entry.scope_schema_url = scope_schema_urls[index].clone().into();
            entry.resource_dropped_attributes_count = resource_dropped_attributes[index];
            entry.scope_dropped_attributes_count = scope_dropped_attributes[index];
        }
    }
    // The header's range claims feed pruning elsewhere: prove them
    // against the payload before returning spans.
    check_block_range("span block", n, ts_min, ts_max, &timestamps)?;
    Ok(out)
}

fn decode_span_block_v1(bytes: &[u8]) -> Result<Vec<SpanEntry>, String> {
    let mut r = Reader::new(bytes);
    let version = r.u8("format version")?;
    debug_assert_eq!(version, FORMAT_VERSION_V1);
    let codec = r.u8("codec")?;
    if !known_codec(codec) {
        return Err(format!("span block: unknown codec {codec}"));
    }
    let n = r.u32("entry_count")? as usize;
    check_entry_count(n, "span block")?;
    let (ts_min, ts_max) = (r.i64("ts_min")?, r.i64("ts_max")?);
    let mut lens = [0usize; V1_N_COLUMNS];
    for (i, len) in lens.iter_mut().enumerate() {
        *len = r.u32(V1_COLUMN_NAMES[i])? as usize;
    }

    let mut stored: Vec<&[u8]> = Vec::with_capacity(V1_N_COLUMNS);
    for (i, len) in lens.iter().enumerate() {
        stored.push(r.take(*len, V1_COLUMN_NAMES[i])?);
    }
    if r.remaining() != 0 {
        return Err(format!(
            "span block: {} trailing byte(s) after last column (corrupt header?)",
            r.remaining()
        ));
    }

    // ── Codecs 4/5: typed column decoders ────────────────────────────
    if codec == CODEC_COLUMNAR || codec == CODEC_COLUMNAR_V2 {
        let trace_flat = decode_fixed_bytes(stored[0], n, 16)?;
        let span_flat = decode_fixed_bytes(stored[1], n, 8)?;
        let parent_raw = zstd_decompress(stored[2], V1_COLUMN_NAMES[2])?;
        let parents = parse_parents(&parent_raw, n)?;
        let names = decode_str(stored[3], n)?;
        let services = decode_str(stored[4], n)?;
        let kinds = decode_u8(stored[5], n)?;
        let statuses = decode_u8(stored[6], n)?;
        for (i, &k) in kinds.iter().enumerate() {
            if k > 4 {
                return Err(format!("span block: span {i} has invalid kind byte {k}"));
            }
        }
        for (i, &s) in statuses.iter().enumerate() {
            if s > 2 {
                return Err(format!("span block: span {i} has invalid status byte {s}"));
            }
        }
        let timestamps = decode_i64(stored[7], n)?;
        let durations = decode_i64(stored[8], n)?;
        let attrs = if codec == CODEC_COLUMNAR_V2 {
            decode_pairs_column(stored[9], n, "attribute", parse_attributes)?
        } else {
            let attr_raw = zstd_decompress(stored[9], V1_COLUMN_NAMES[9])?;
            parse_attributes(&attr_raw, n)?
        };

        let mut out = Vec::with_capacity(n);
        let mut name_it = names.into_iter();
        let mut svc_it = services.into_iter();
        let mut attr_it = attrs.into_iter();
        for i in 0..n {
            out.push(SpanEntry {
                trace_id: <[u8; 16]>::try_from(&trace_flat[i * 16..(i + 1) * 16]).unwrap(),
                span_id: <[u8; 8]>::try_from(&span_flat[i * 8..(i + 1) * 8]).unwrap(),
                parent_span_id: parents[i],
                name: name_it.next().unwrap(),
                service: svc_it.next().unwrap(),
                kind: kinds[i],
                status: statuses[i],
                status_description: "".into(),
                start_ts: timestamps[i],
                duration_ns: durations[i],
                attributes: legacy_pairs_to_json(&attr_it.next().unwrap()).into(),
                events: "[]".into(),
                resource: "{}".into(),
                instrumentation_scope: "{}".into(),
                links: "[]".into(),
                trace_state: "".into(),
                trace_flags: 0,
                dropped_attributes_count: 0,
                dropped_events_count: 0,
                dropped_links_count: 0,
                resource_schema_url: "".into(),
                scope_schema_url: "".into(),
                resource_dropped_attributes_count: 0,
                scope_dropped_attributes_count: 0,
            });
        }
        check_block_range("span block", n, ts_min, ts_max, &timestamps)?;
        return Ok(out);
    }

    // ── Codecs 1/2 — the Session 6 decode path, byte-for-byte ────────
    let cols: Vec<Vec<u8>> = if codec == CODEC_ZSTD {
        stored
            .iter()
            .enumerate()
            .map(|(i, c)| zstd_decompress(c, V1_COLUMN_NAMES[i]))
            .collect::<Result<_, _>>()?
    } else {
        stored.iter().map(|c| c.to_vec()).collect()
    };

    // ── Fixed-width columns: validate lengths up front ───────────────
    for (idx, want) in [
        (0usize, n * 16),
        (1, n * 8),
        (5, n),
        (6, n),
        (7, n * 8),
        (8, n * 8),
    ] {
        if cols[idx].len() != want {
            return Err(format!(
                "span block: {} is {} bytes, expected {want} for {n} spans",
                V1_COLUMN_NAMES[idx],
                cols[idx].len()
            ));
        }
    }
    for (i, &k) in cols[5].iter().enumerate() {
        if k > 4 {
            return Err(format!("span block: span {i} has invalid kind byte {k}"));
        }
    }
    for (i, &s) in cols[6].iter().enumerate() {
        if s > 2 {
            return Err(format!("span block: span {i} has invalid status byte {s}"));
        }
    }

    let mut timestamps = Vec::with_capacity(n);
    if codec == CODEC_ZSTD {
        let mut prev = 0i64;
        for c in cols[7].as_chunks::<8>().0 {
            prev = prev.wrapping_add(i64::from_le_bytes(*c));
            timestamps.push(prev);
        }
    } else {
        for c in cols[7].as_chunks::<8>().0 {
            timestamps.push(i64::from_le_bytes(*c));
        }
    }
    let durations: Vec<i64> = cols[8]
        .as_chunks::<8>()
        .0
        .iter()
        .map(|c| i64::from_le_bytes(*c))
        .collect();

    // ── Variable columns: parents, names, services, attributes ───────
    let parents = parse_parents(&cols[2], n)?;

    let read_strings = |col: usize| -> Result<Vec<String>, String> {
        let mut out = Vec::with_capacity(n);
        let mut sr = Reader::new(&cols[col]);
        for i in 0..n {
            let len = sr.u16(V1_COLUMN_NAMES[col])? as usize;
            let b = sr.take(len, V1_COLUMN_NAMES[col])?;
            let s = std::str::from_utf8(b).map_err(|_| {
                format!(
                    "span block: span {i}: {} is not valid UTF-8",
                    V1_COLUMN_NAMES[col]
                )
            })?;
            out.push(s.to_owned());
        }
        if sr.remaining() != 0 {
            return Err(format!(
                "span block: trailing bytes in {}",
                V1_COLUMN_NAMES[col]
            ));
        }
        Ok(out)
    };
    let names = read_strings(3)?;
    let services = read_strings(4)?;

    let attrs = parse_attributes(&cols[9], n)?;

    // ── Zip the columns back into spans ──────────────────────────────
    let mut out = Vec::with_capacity(n);
    let mut name_it = names.into_iter();
    let mut svc_it = services.into_iter();
    let mut attr_it = attrs.into_iter();
    for i in 0..n {
        out.push(SpanEntry {
            trace_id: <[u8; 16]>::try_from(&cols[0][i * 16..(i + 1) * 16]).unwrap(),
            span_id: <[u8; 8]>::try_from(&cols[1][i * 8..(i + 1) * 8]).unwrap(),
            parent_span_id: parents[i],
            name: name_it.next().unwrap(),
            service: svc_it.next().unwrap(),
            kind: cols[5][i],
            status: cols[6][i],
            status_description: "".into(),
            start_ts: timestamps[i],
            duration_ns: durations[i],
            attributes: legacy_pairs_to_json(&attr_it.next().unwrap()).into(),
            events: "[]".into(),
            resource: "{}".into(),
            instrumentation_scope: "{}".into(),
            links: "[]".into(),
            trace_state: "".into(),
            trace_flags: 0,
            dropped_attributes_count: 0,
            dropped_events_count: 0,
            dropped_links_count: 0,
            resource_schema_url: "".into(),
            scope_schema_url: "".into(),
            resource_dropped_attributes_count: 0,
            scope_dropped_attributes_count: 0,
        });
    }
    check_block_range("span block", n, ts_min, ts_max, &timestamps)?;
    Ok(out)
}

/// Parse the parent-id presence serialization (1 presence byte, then 8
/// id bytes if present) — shared by every decode path.
fn parse_parents(raw: &[u8], n: usize) -> Result<Vec<Option<[u8; 8]>>, String> {
    let mut parents = Vec::with_capacity(n);
    let mut pr = Reader::new(raw);
    for i in 0..n {
        match pr.u8("parent presence byte")? {
            0 => parents.push(None),
            1 => {
                let b = pr.take(8, "parent span id")?;
                parents.push(Some(<[u8; 8]>::try_from(b).unwrap()));
            }
            other => {
                return Err(format!(
                    "span block: span {i} has invalid parent presence byte {other}"
                ))
            }
        }
    }
    if pr.remaining() != 0 {
        return Err("span block: trailing bytes in parent_id column".into());
    }
    Ok(parents)
}

/// Parse the attribute pair serialization — shared by every decode path
/// (byte-identical layout to logs metadata).
fn parse_attributes(raw: &[u8], n: usize) -> Result<Vec<Vec<(String, String)>>, String> {
    let mut attrs = Vec::with_capacity(n);
    let mut ar = Reader::new(raw);
    for i in 0..n {
        let pairs = ar.u16("attribute pair count")? as usize;
        let mut a = Vec::with_capacity(pairs);
        for _ in 0..pairs {
            let klen = ar.u16("attribute key length")? as usize;
            let kb = ar.take(klen, "attribute key")?;
            let k = std::str::from_utf8(kb)
                .map_err(|_| format!("span block: span {i}: attribute key is not valid UTF-8"))?;
            let vlen = ar.u32("attribute value length")? as usize;
            let vb = ar.take(vlen, "attribute value")?;
            let v = std::str::from_utf8(vb)
                .map_err(|_| format!("span block: span {i}: attribute value is not valid UTF-8"))?;
            a.push((k.to_owned(), v.to_owned()));
        }
        attrs.push(a);
    }
    if ar.remaining() != 0 {
        return Err("span block: trailing bytes in attributes column".into());
    }
    Ok(attrs)
}

fn legacy_pairs_to_json(pairs: &[(String, String)]) -> String {
    fn quoted(out: &mut String, value: &str) {
        out.push('"');
        for ch in value.chars() {
            match ch {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\u{08}' => out.push_str("\\b"),
                '\u{0c}' => out.push_str("\\f"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                ch if ch <= '\u{1f}' => {
                    use std::fmt::Write;
                    let _ = write!(out, "\\u{:04x}", ch as u32);
                }
                ch => out.push(ch),
            }
        }
        out.push('"');
    }

    let mut out = String::from("{");
    for (i, (key, value)) in pairs.iter().enumerate() {
        if i != 0 {
            out.push(',');
        }
        quoted(&mut out, key);
        out.push(':');
        quoted(&mut out, value);
    }
    out.push('}');
    out
}
