//! Columnar span block codec — the traces sibling of blocks/codec.rs
//! (read that header first; the container idea, the RAW/ZSTD/COLUMNAR
//! codec byte semantics are identical, and the zstd helpers + bounds-
//! checked Reader come from the shared timeless-codec crate).
//!
//! Generation 2 has fourteen columns — spans are wider than log lines, and
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
//!   0       1      format version (0x02 for new writes; 0x01 readable)
//!   1       1      codec (1, 2, 4 or 5; 3 reserved for OpenZL)
//!   2       4      u32 entry_count
//!   6       8      i64 ts_min   (min start_ts)
//!   14      8      i64 ts_max   (max start_ts)
//!   22      14×4   u32 stored length of each generation-2 column
//!   78      —      the 14 columns, back to back
//!
//! decode_span_block() is the exact inverse and validates everything —
//! a truncated or corrupt block is an error naming the field, never a
//! panic or garbage spans.

use timeless_codec::{
    decode_fixed_bytes, decode_i64, decode_str, decode_u8, encode_fixed_bytes, encode_i64,
    encode_str, encode_u8, zstd_compress, zstd_decompress, Reader,
};

pub use crate::blocks::codec::{CODEC_COLUMNAR, CODEC_COLUMNAR_V2, CODEC_RAW, CODEC_ZSTD};

use crate::blocks::codec::decode_pairs_column;

use super::{BlockMeta, SpanEntry};

const FORMAT_VERSION_V1: u8 = 1;
const FORMAT_VERSION: u8 = 2;
const V1_N_COLUMNS: usize = 10;
const N_COLUMNS: usize = 14;
const HEADER_LEN: usize = 22 + N_COLUMNS * 4; // 78

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
        vec![
            encode_fixed_bytes(&col_trace, 16, zstd_level)?.to_bytes(),
            encode_fixed_bytes(&col_span, 8, zstd_level)?.to_bytes(),
            // Parent ids: variable-width presence serialization, plain
            // zstd (unframed, byte-identical to the codec-2 column).
            zstd_compress(&col_parent, zstd_level)?,
            encode_str(entries.iter().map(|e| e.name.as_str()), n, zstd_level)?.to_bytes(),
            encode_str(entries.iter().map(|e| e.service.as_str()), n, zstd_level)?.to_bytes(),
            encode_u8(&col_kind, zstd_level)?.to_bytes(),
            encode_u8(&col_status, zstd_level)?.to_bytes(),
            // Both i64 columns delta inside encode_i64; durations don't
            // trend but the adaptive pick just falls back to whichever
            // strategy handles their magnitude-similarity best.
            encode_i64(&starts, zstd_level)?.to_bytes(),
            encode_i64(&durs, zstd_level)?.to_bytes(),
            encode_str(entries.iter().map(|e| e.attributes.as_ref()), n, zstd_level)?.to_bytes(),
            encode_str(
                entries.iter().map(|e| e.status_description.as_ref()),
                n,
                zstd_level,
            )?
            .to_bytes(),
            encode_str(entries.iter().map(|e| e.events.as_ref()), n, zstd_level)?.to_bytes(),
            encode_str(entries.iter().map(|e| e.resource.as_ref()), n, zstd_level)?.to_bytes(),
            encode_str(
                entries.iter().map(|e| e.instrumentation_scope.as_ref()),
                n,
                zstd_level,
            )?
            .to_bytes(),
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
            ] {
                col.extend_from_slice(&(s.len() as u32).to_le_bytes());
                col.extend_from_slice(s.as_bytes());
            }
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

/// Decode a span block payload back into spans, in stored order.
/// Generation 1 (the original ten-column/string-attribute layout) is
/// permanently readable. Generation 2 stores all rich OTel fields and
/// typed JSON text in fourteen independent columns.
pub fn decode_span_block(bytes: &[u8]) -> Result<Vec<SpanEntry>, String> {
    match bytes.first().copied() {
        Some(FORMAT_VERSION_V1) => decode_span_block_v1(bytes),
        Some(FORMAT_VERSION) => decode_span_block_v2(bytes),
        Some(version) => Err(format!(
            "span block: unsupported format version {version} (this build speaks 1 and 2)"
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

fn decode_span_block_v2(bytes: &[u8]) -> Result<Vec<SpanEntry>, String> {
    let mut r = Reader::new(bytes);
    let version = r.u8("format version")?;
    debug_assert_eq!(version, FORMAT_VERSION);
    let codec = r.u8("codec")?;
    if !known_codec(codec) {
        return Err(format!("span block: unknown codec {codec}"));
    }
    let n = r.u32("entry_count")? as usize;
    let _ts_min = r.i64("ts_min")?;
    let _ts_max = r.i64("ts_max")?;
    let mut lens = [0usize; N_COLUMNS];
    for (i, len) in lens.iter_mut().enumerate() {
        *len = r.u32(COLUMN_NAMES[i])? as usize;
    }
    let mut stored = Vec::with_capacity(N_COLUMNS);
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
        for chunk in cols[7].chunks_exact(8) {
            let value = i64::from_le_bytes(chunk.try_into().unwrap());
            if codec == CODEC_ZSTD {
                previous = previous.wrapping_add(value);
                timestamps.push(previous);
            } else {
                timestamps.push(value);
            }
        }
        let durations = cols[8]
            .chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
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
        });
    }
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
    let _ts_min = r.i64("ts_min")?;
    let _ts_max = r.i64("ts_max")?;
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
            });
        }
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
        });
    }
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
