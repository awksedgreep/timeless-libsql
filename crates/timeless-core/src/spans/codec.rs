//! Columnar span block codec — the traces sibling of blocks/codec.rs
//! (read that header first; the container idea, the RAW/ZSTD two-tier
//! split and the codec byte semantics are identical, and the zstd
//! helpers + bounds-checked Reader are literally shared from there).
//!
//! Ten columns instead of four — spans are wider than log lines, and
//! the columnar split is where the compression comes from: each column
//! is long runs of SIMILAR data (all the 16-byte trace ids together,
//! all the u8 kinds together...), which zstd rewards far more than
//! interleaved span structs would:
//!
//!   col  1  trace_ids   16 bytes fixed per span, packed binary
//!   col  2  span_ids    8 bytes fixed per span
//!   col  3  parent_ids  1 presence byte (0|1), then 8 bytes IF present
//!                       (root spans pay 1 byte, not 9 zeros)
//!   col  4  names       u16-len-prefixed UTF-8, concatenated
//!   col  5  services    u16-len-prefixed UTF-8, concatenated
//!   col  6  kinds       one u8 per span (0..=4)
//!   col  7  statuses    one u8 per span (0..=2)
//!   col  8  start_ts    i64 LE; delta-encoded before zstd (spans in a
//!                       block are start_ts-sorted → tiny deltas)
//!   col  9  durations   i64 LE (no delta: durations don't trend, but
//!                       similar magnitudes still zstd well)
//!   col 10  attributes  per span: u16 pair count, then per pair
//!                       u16 key-len + key + u32 val-len + value
//!                       (byte-identical layout to the logs metadata
//!                       column — flat sorted string pairs both places)
//!
//! Name/service lengths are u16 (a >64KB operation name is nonsense and
//! rejected, same policy as metadata keys in logs); attribute values
//! get u32 like log metadata values (they can legitimately be long).
//!
//! Container layout (all integers little-endian):
//!
//!   offset  size   field
//!   0       1      format version (0x01)
//!   1       1      codec (1=raw 2=zstd; 3 reserved for OpenZL)
//!   2       4      u32 entry_count
//!   6       8      i64 ts_min   (min start_ts)
//!   14      8      i64 ts_max   (max start_ts)
//!   22      10×4   u32 stored length of each of the 10 columns
//!   62      —      the 10 columns, back to back
//!
//! decode_span_block() is the exact inverse and validates everything —
//! a truncated or corrupt block is an error naming the field, never a
//! panic or garbage spans.

use crate::blocks::codec::{zstd_compress, zstd_decompress, Reader};
pub use crate::blocks::codec::{CODEC_RAW, CODEC_ZSTD};

use super::{BlockMeta, SpanEntry};

const FORMAT_VERSION: u8 = 1;
const N_COLUMNS: usize = 10;
const HEADER_LEN: usize = 22 + N_COLUMNS * 4; // 62

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
];

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
    if codec != CODEC_RAW && codec != CODEC_ZSTD {
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

    // ── Build the ten columns, uncompressed ─────────────────────────
    let mut col_trace = Vec::with_capacity(n * 16);
    let mut col_span = Vec::with_capacity(n * 8);
    let mut col_parent = Vec::with_capacity(n); // + 8/present
    let mut col_name = Vec::new();
    let mut col_svc = Vec::new();
    let mut col_kind = Vec::with_capacity(n);
    let mut col_status = Vec::with_capacity(n);
    let mut col_ts = Vec::with_capacity(n * 8);
    let mut col_dur = Vec::with_capacity(n * 8);
    let mut col_attr = Vec::new();

    let mut prev_ts = 0i64;
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
        for (label, s, col) in [("name", &e.name, &mut col_name), ("service", &e.service, &mut col_svc)] {
            let b = s.as_bytes();
            if b.len() > u16::MAX as usize {
                return Err(format!("encode_span_block: {label} longer than 64KB"));
            }
            col.extend_from_slice(&(b.len() as u16).to_le_bytes());
            col.extend_from_slice(b);
        }
        col_kind.push(e.kind);
        col_status.push(e.status);
        // start_ts: RAW stores absolutes, ZSTD stores deltas (first
        // absolute, then differences) — same scheme as the logs ts
        // column and for the same reason: steady traffic makes deltas
        // small repeated numbers, much better zstd food.
        if codec == CODEC_RAW {
            col_ts.extend_from_slice(&e.start_ts.to_le_bytes());
        } else {
            col_ts.extend_from_slice(&e.start_ts.wrapping_sub(prev_ts).to_le_bytes());
            prev_ts = e.start_ts;
        }
        col_dur.extend_from_slice(&e.duration_ns.to_le_bytes());

        if e.attributes.len() > u16::MAX as usize {
            return Err("encode_span_block: more than 65535 attributes in one span".into());
        }
        col_attr.extend_from_slice(&(e.attributes.len() as u16).to_le_bytes());
        for (k, v) in &e.attributes {
            let (kb, vb) = (k.as_bytes(), v.as_bytes());
            if kb.len() > u16::MAX as usize {
                return Err(format!("encode_span_block: attribute key {k:?} longer than 64KB"));
            }
            if vb.len() > u32::MAX as usize {
                return Err(format!("encode_span_block: attribute value for {k:?} too long"));
            }
            col_attr.extend_from_slice(&(kb.len() as u16).to_le_bytes());
            col_attr.extend_from_slice(kb);
            col_attr.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            col_attr.extend_from_slice(vb);
        }
    }

    // ── Compress (codec 2) or store as-is (codec 1) ──────────────────
    let raw_cols: [Vec<u8>; N_COLUMNS] = [
        col_trace, col_span, col_parent, col_name, col_svc, col_kind, col_status, col_ts,
        col_dur, col_attr,
    ];
    let columns: Vec<Vec<u8>> = if codec == CODEC_ZSTD {
        raw_cols
            .iter()
            .map(|c| zstd_compress(c, zstd_level))
            .collect::<Result<_, _>>()?
    } else {
        raw_cols.into_iter().collect()
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

/// Decode a span block payload back into spans, in stored order.
pub fn decode_span_block(bytes: &[u8]) -> Result<Vec<SpanEntry>, String> {
    let mut r = Reader::new(bytes);
    let version = r.u8("format version")?;
    if version != FORMAT_VERSION {
        return Err(format!(
            "span block: unsupported format version {version} (this build speaks {FORMAT_VERSION})"
        ));
    }
    let codec = r.u8("codec")?;
    if codec != CODEC_RAW && codec != CODEC_ZSTD {
        return Err(format!("span block: unknown codec {codec}"));
    }
    let n = r.u32("entry_count")? as usize;
    let _ts_min = r.i64("ts_min")?;
    let _ts_max = r.i64("ts_max")?;
    let mut lens = [0usize; N_COLUMNS];
    for (i, len) in lens.iter_mut().enumerate() {
        *len = r.u32(COLUMN_NAMES[i])? as usize;
    }

    let mut stored: Vec<&[u8]> = Vec::with_capacity(N_COLUMNS);
    for (i, len) in lens.iter().enumerate() {
        stored.push(r.take(*len, COLUMN_NAMES[i])?);
    }
    if r.remaining() != 0 {
        return Err(format!(
            "span block: {} trailing byte(s) after last column (corrupt header?)",
            r.remaining()
        ));
    }

    let cols: Vec<Vec<u8>> = if codec == CODEC_ZSTD {
        stored
            .iter()
            .enumerate()
            .map(|(i, c)| zstd_decompress(c, COLUMN_NAMES[i]))
            .collect::<Result<_, _>>()?
    } else {
        stored.iter().map(|c| c.to_vec()).collect()
    };

    // ── Fixed-width columns: validate lengths up front ───────────────
    for (idx, want) in [(0usize, n * 16), (1, n * 8), (5, n), (6, n), (7, n * 8), (8, n * 8)] {
        if cols[idx].len() != want {
            return Err(format!(
                "span block: {} is {} bytes, expected {want} for {n} spans",
                COLUMN_NAMES[idx],
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
        for c in cols[7].chunks_exact(8) {
            prev = prev.wrapping_add(i64::from_le_bytes(c.try_into().unwrap()));
            timestamps.push(prev);
        }
    } else {
        for c in cols[7].chunks_exact(8) {
            timestamps.push(i64::from_le_bytes(c.try_into().unwrap()));
        }
    }
    let durations: Vec<i64> = cols[8]
        .chunks_exact(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect();

    // ── Variable columns: parents, names, services, attributes ───────
    let mut parents = Vec::with_capacity(n);
    let mut pr = Reader::new(&cols[2]);
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

    let read_strings = |col: usize| -> Result<Vec<String>, String> {
        let mut out = Vec::with_capacity(n);
        let mut sr = Reader::new(&cols[col]);
        for i in 0..n {
            let len = sr.u16(COLUMN_NAMES[col])? as usize;
            let b = sr.take(len, COLUMN_NAMES[col])?;
            let s = std::str::from_utf8(b).map_err(|_| {
                format!("span block: span {i}: {} is not valid UTF-8", COLUMN_NAMES[col])
            })?;
            out.push(s.to_owned());
        }
        if sr.remaining() != 0 {
            return Err(format!("span block: trailing bytes in {}", COLUMN_NAMES[col]));
        }
        Ok(out)
    };
    let names = read_strings(3)?;
    let services = read_strings(4)?;

    let mut attrs = Vec::with_capacity(n);
    let mut ar = Reader::new(&cols[9]);
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
            start_ts: timestamps[i],
            duration_ns: durations[i],
            attributes: attr_it.next().unwrap(),
        });
    }
    Ok(out)
}
