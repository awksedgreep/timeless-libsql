//! Columnar block codec, ported from timeless_logs storage.md and
//! adapted from Erlang terms to explicit little-endian framing.
//!
//! The big compression win is the COLUMNAR SPLIT (PLAN.md "Codec
//! strategy"): instead of compressing interleaved entries, we split
//! them into four columns and compress each independently, so the
//! compressor sees long runs of similar data:
//!
//!   column 1  timestamps   i64 per entry
//!   column 2  levels       one u8 per entry (mostly "info" → ~free)
//!   column 3  messages     UTF-8 strings
//!   column 4  metadata     per entry: u16 pair count, then per pair
//!                          u16 key-len + key + u32 val-len + value
//!
//! Three codecs share one container:
//!   CODEC_RAW      (1) — columns stored uncompressed, no delta. This
//!                        is the low-latency flush format (write fast
//!                        now, compress later — the two-tier design).
//!   CODEC_ZSTD     (2) — timestamps delta-encoded, then EVERY column
//!                        independently zstd-compressed. The Session 5
//!                        format; still fully decodable (existing dbs
//!                        keep working) but no longer written by
//!                        optimize().
//!   codec 3 is reserved for OpenZL (never assigned here; the codec
//!   byte in the header + `_blocks.codec` column means all formats
//!   coexist in one table and any bake-off needs no migration).
//!   CODEC_COLUMNAR (4) — "adaptive columnar v1": each column goes
//!                        through the timeless-codec TYPED ENCODERS,
//!                        which pick a strategy per column by
//!                        measurement (ts → delta+pco vs delta+zstd,
//!                        levels → RLE vs zstd, messages → dictionary
//!                        vs concat+zstd). The winning strategy id is
//!                        framed inside the column, so decode never
//!                        guesses. The Session 7 format; still fully
//!                        decodable but no longer written by
//!                        optimize().
//!   CODEC_COLUMNAR_V2 (5) — "adaptive columnar v2": identical to
//!                        codec 4 EXCEPT the metadata column, which is
//!                        SHREDDED into per-key typed columns (see
//!                        encode_pairs_column below). This is what
//!                        optimize() writes since the Session 8
//!                        shredding bake-off.
//!   CODEC_RICH_TEMPLATE (8) — identical to codec 7 EXCEPT the message
//!                        column, which is CLP-style template-compressed
//!                        (blocks/template.rs, CLP_PLAN.md): template
//!                        ids + template dictionary + typed variable
//!                        columns. encode_block measures it against the
//!                        codec-7 message column and emits codec 7 when
//!                        templates lose, so requesting 8 never costs
//!                        bytes.
//!
//! Codec-5 metadata note: codec 4 kept TODAY'S pair serialization
//! (below) compressed with plain zstd — same bytes as codec 2, and it
//! moved 0.0% in the Session 7 bake-off while dominating the block
//! (~29.6KB of a ~44KB 8192-entry logs group). Codec 5 shreds the
//! column per KEY: each distinct key gets a presence bitmap plus a
//! DENSE encode_str column of its values, so a "status" column
//! dictionary-encodes instead of re-paying `status` + the value
//! serialization per entry. Blocks with pathological key sets fall
//! back to the legacy bytes verbatim (strategy byte 0) — see the
//! SHRED_MAX_KEYS rationale.
//!
//! Container layout (all integers little-endian, IDENTICAL for all
//! codecs — only the column payloads differ):
//!
//!   offset  size  field
//!   0       1     format version (0x01)
//!   1       1     codec (1, 2, 4, 5, 6, 7 or 8)
//!   2       4     u32 entry_count
//!   6       8     i64 ts_min
//!   14      8     i64 ts_max
//!   22      4×4   u32 stored length of each of the 4 columns
//!   38      —     the 4 columns, back to back
//!
//! decode_block() is the exact inverse and validates everything it
//! reads — a truncated or corrupt block is an error naming the field,
//! never a panic or garbage entries.
//!
//! The zstd helpers and the bounds-checked Reader used to live here
//! (pub(crate), shared with spans/codec.rs); they moved to the
//! timeless-codec crate — one copy, three consumers.

use std::collections::BTreeSet;

use timeless_codec::{
    bitmap_len, decode_bitmap, decode_i64, decode_str, decode_u8, encode_bitmap, encode_i64,
    encode_str, encode_u8, zstd_compress, zstd_decompress, Reader,
};

use super::{template, BlockMeta, LogEntry};

pub const CODEC_RAW: u8 = 1;
pub const CODEC_ZSTD: u8 = 2;
/// Codec 3 stays reserved for OpenZL — see the module header.
pub const CODEC_COLUMNAR: u8 = 4;
/// Adaptive columnar v2: codec 4 + shredded metadata/attributes.
pub const CODEC_COLUMNAR_V2: u8 = 5;
/// Rich logs raw format: the fourth column retains exact severity and typed
/// canonical JSON. It is intentionally a new codec so older extensions fail
/// loudly instead of flattening new data.
pub const CODEC_RICH_RAW: u8 = 6;
/// Rich logs compressed format. Timestamp/level/message use the established
/// typed encoders; the rich envelope is independently zstd-compressed.
pub const CODEC_RICH_COLUMNAR: u8 = 7;
/// Rich logs, CLP-style template-compressed message column (CLP_PLAN.md):
/// identical to codec 7 except the message column stores template ids +
/// a template dictionary + typed variable columns (see blocks/template.rs).
/// encode_block PROJECTS BOTH message encodings and silently emits codec 7
/// when templates lose, so no block is ever larger than codec 7 — callers
/// request 8 and read the winner off `BlockMeta::codec`.
pub const CODEC_RICH_TEMPLATE: u8 = 8;

const FORMAT_VERSION: u8 = 1;
const HEADER_LEN: usize = 38;

fn known_codec(codec: u8) -> bool {
    codec == CODEC_RAW
        || codec == CODEC_ZSTD
        || codec == CODEC_COLUMNAR
        || codec == CODEC_COLUMNAR_V2
        || codec == CODEC_RICH_RAW
        || codec == CODEC_RICH_COLUMNAR
        || codec == CODEC_RICH_TEMPLATE
}

pub fn is_raw_codec(codec: u8) -> bool {
    codec == CODEC_RAW || codec == CODEC_RICH_RAW
}

/// Encode `entries` into one block payload. Entries should already be
/// sorted by ts (the engine sorts at flush); the codec doesn't REQUIRE
/// it (deltas may be negative) but sorted input compresses better and
/// keeps ts_min/ts_max cheap to trust.
///
/// `zstd_level` is consulted for every codec except RAW. Level 7
/// is the engine's default: measurably better ratio than the zstd
/// crate's default (3) at a throughput still far above ingest rates.
pub fn encode_block(
    entries: &[LogEntry],
    codec: u8,
    zstd_level: i32,
) -> Result<(Vec<u8>, BlockMeta), String> {
    if entries.is_empty() {
        return Err("encode_block: refusing to encode an empty block".into());
    }
    if !known_codec(codec) {
        return Err(format!("encode_block: unknown codec {codec}"));
    }

    let n = entries.len();
    let rich_codec = matches!(
        codec,
        CODEC_RICH_RAW | CODEC_RICH_COLUMNAR | CODEC_RICH_TEMPLATE
    );
    if !rich_codec && entries.iter().any(LogEntry::is_rich) {
        return Err(format!(
            "encode_block: rich log entry requires codec {CODEC_RICH_RAW}, {CODEC_RICH_COLUMNAR} or {CODEC_RICH_TEMPLATE}"
        ));
    }
    let mut ts_min = i64::MAX;
    let mut ts_max = i64::MIN;
    for e in entries {
        if e.level > 3 {
            return Err(format!(
                "encode_block: entry has invalid level {} (must be 0..=3)",
                e.level
            ));
        }
        ts_min = ts_min.min(e.ts);
        ts_max = ts_max.max(e.ts);
    }

    // Column 2 raw form (one byte per entry) and column 4 raw form
    // (the pair serialization) are shared by every codec.
    let col_lvl_raw: Vec<u8> = entries.iter().map(|e| e.level).collect();
    let col_meta_raw = if rich_codec {
        serialize_rich_metadata(entries)?
    } else {
        serialize_metadata(entries)?
    };

    // Codec 8 may downgrade itself to 7 below (per-block template
    // fallback); the container byte and BlockMeta record what was
    // actually written.
    let mut codec = codec;
    let columns: [Vec<u8>; 4] = match codec {
        CODEC_COLUMNAR | CODEC_COLUMNAR_V2 | CODEC_RICH_COLUMNAR | CODEC_RICH_TEMPLATE => {
            // Codecs 4/5: typed column encoders pick their own strategy
            // (and record it in the column frame). The ts delta pass
            // lives INSIDE encode_i64 now; we hand it absolutes. The
            // ONLY difference between 4 and 5 is the metadata column,
            // and the only difference between 7 and 8 is the message
            // column.
            let ts_values: Vec<i64> = entries.iter().map(|e| e.ts).collect();
            let col_meta = if codec == CODEC_COLUMNAR_V2 {
                // Codec 5: metadata shredded into per-key columns
                // (with a verbatim-legacy fallback inside).
                let pairs: Vec<&[(String, String)]> =
                    entries.iter().map(|e| e.metadata.as_slice()).collect();
                encode_pairs_column(&pairs, &col_meta_raw, zstd_level)?
            } else {
                // Codecs 4/7/8: today's serialization + zstd, UNFRAMED
                // (byte-identical to the codec-2 column).
                zstd_compress(&col_meta_raw, zstd_level)?
            };
            let col_msg_str =
                encode_str(entries.iter().map(|e| e.message.as_str()), n, zstd_level)?.to_bytes();
            let col_msg = if codec == CODEC_RICH_TEMPLATE {
                // Codec 8: template-compress the message column, but
                // MEASURE against the codec-7 encoding — if templates
                // lose (near-unique lines, high-entropy blobs), emit a
                // codec-7 block instead. No block is ever larger than
                // codec 7 (CLP_PLAN.md, the mandatory per-block gate).
                let msgs: Vec<&str> = entries.iter().map(|e| e.message.as_str()).collect();
                let tpl = template::encode_template_str(&msgs, zstd_level)?;
                if tpl.len() < col_msg_str.len() {
                    tpl
                } else {
                    codec = CODEC_RICH_COLUMNAR;
                    col_msg_str
                }
            } else {
                col_msg_str
            };
            [
                encode_i64(&ts_values, zstd_level)?.to_bytes(),
                encode_u8(&col_lvl_raw, zstd_level)?.to_bytes(),
                col_msg,
                col_meta,
            ]
        }
        _ => {
            // Codecs 1/2 — the Session 5 formats, byte-for-byte.
            // Column 1: RAW stores absolutes; ZSTD stores deltas
            // (first value absolute, then successive differences)
            // because the deltas of steady traffic are small repeated
            // numbers — much better zstd food than large monotonically-
            // shifting absolutes.
            let mut col_ts = Vec::with_capacity(n * 8);
            if is_raw_codec(codec) {
                for e in entries {
                    col_ts.extend_from_slice(&e.ts.to_le_bytes());
                }
            } else {
                let mut prev = 0i64;
                for e in entries {
                    col_ts.extend_from_slice(&e.ts.wrapping_sub(prev).to_le_bytes());
                    prev = e.ts;
                }
            }

            // Column 3: messages, u32-len-prefixed UTF-8 concatenated.
            let mut col_msg = Vec::new();
            for e in entries {
                let b = e.message.as_bytes();
                if b.len() > u32::MAX as usize {
                    return Err("encode_block: message longer than u32::MAX bytes".into());
                }
                col_msg.extend_from_slice(&(b.len() as u32).to_le_bytes());
                col_msg.extend_from_slice(b);
            }

            if codec == CODEC_ZSTD {
                [
                    zstd_compress(&col_ts, zstd_level)?,
                    zstd_compress(&col_lvl_raw, zstd_level)?,
                    zstd_compress(&col_msg, zstd_level)?,
                    zstd_compress(&col_meta_raw, zstd_level)?,
                ]
            } else {
                [col_ts, col_lvl_raw, col_msg, col_meta_raw]
            }
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
            return Err("encode_block: column exceeds u32::MAX bytes".into());
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

/// Decode a block payload back into entries, in stored order. Speaks
/// every codec ever written — existing databases must stay decodable
/// forever, whatever optimize() currently emits.
pub fn decode_block(bytes: &[u8]) -> Result<Vec<LogEntry>, String> {
    let mut r = Reader::new(bytes);
    let version = r.u8("format version")?;
    if version != FORMAT_VERSION {
        return Err(format!(
            "block: unsupported format version {version} (this build speaks {FORMAT_VERSION})"
        ));
    }
    let codec = r.u8("codec")?;
    if !known_codec(codec) {
        return Err(format!("block: unknown codec {codec}"));
    }
    let n = r.u32("entry_count")? as usize;
    let _ts_min = r.i64("ts_min")?;
    let _ts_max = r.i64("ts_max")?;
    let lens = [
        r.u32("ts column length")? as usize,
        r.u32("level column length")? as usize,
        r.u32("message column length")? as usize,
        r.u32("metadata column length")? as usize,
    ];

    let mut stored: Vec<&[u8]> = Vec::with_capacity(4);
    for (i, len) in lens.iter().enumerate() {
        stored.push(r.take(*len, COLUMN_NAMES[i])?);
    }
    if r.remaining() != 0 {
        return Err(format!(
            "block: {} trailing byte(s) after last column (corrupt header?)",
            r.remaining()
        ));
    }

    // ── Codecs 4/5/7/8: typed column decoders ────────────────────────
    if matches!(
        codec,
        CODEC_COLUMNAR | CODEC_COLUMNAR_V2 | CODEC_RICH_COLUMNAR | CODEC_RICH_TEMPLATE
    ) {
        let timestamps = decode_i64(stored[0], n)?;
        let levels = decode_u8(stored[1], n)?;
        for (i, &lvl) in levels.iter().enumerate() {
            if lvl > 3 {
                return Err(format!("block: entry {i} has invalid level byte {lvl}"));
            }
        }
        let messages = if codec == CODEC_RICH_TEMPLATE {
            template::decode_template_str(stored[2], n)?
        } else {
            decode_str(stored[2], n)?
        };
        let rich = matches!(codec, CODEC_RICH_COLUMNAR | CODEC_RICH_TEMPLATE);
        let rich_metadatas = if rich {
            let raw = zstd_decompress(stored[3], "rich metadata column")?;
            Some(parse_rich_metadata(&raw, n)?)
        } else {
            None
        };
        let metadatas = if codec == CODEC_COLUMNAR_V2 {
            decode_pairs_column(stored[3], n, "metadata", parse_metadata)?
        } else if rich {
            Vec::new()
        } else {
            let meta_raw = zstd_decompress(stored[3], "metadata column")?;
            parse_metadata(&meta_raw, n)?
        };

        let mut out = Vec::with_capacity(n);
        let mut msg_it = messages.into_iter();
        let mut md_it = metadatas.into_iter();
        let mut rich_it = rich_metadatas.into_iter().flatten();
        for i in 0..n {
            let (metadata, severity, metadata_json) = if rich {
                let rich = rich_it.next().unwrap();
                (rich.metadata, Some(rich.severity), Some(rich.metadata_json))
            } else {
                (md_it.next().unwrap(), None, None)
            };
            out.push(LogEntry {
                ts: timestamps[i],
                level: levels[i],
                severity,
                message: msg_it.next().unwrap(),
                metadata,
                metadata_json,
            });
        }
        return Ok(out);
    }

    // ── Codecs 1/2 — the Session 5 decode path, byte-for-byte ────────
    decode_block_legacy(codec, n, stored)
}

/// Sound pre-decode gate for `message_contains` (issue #2): can this
/// block possibly hold a message containing `needle` (case-insensitive
/// substring)? Only codec-8 blocks carry the CLP dictionaries the
/// proof needs — every other codec answers `true` (decode and filter,
/// exactly as before). `Ok(false)` is a proof of absence for the whole
/// block; `Err` means the payload didn't parse, and callers should
/// fall through to `decode_block` so corruption is reported by the
/// path that owns that responsibility.
pub fn block_message_feasible(bytes: &[u8], needle: &str) -> Result<bool, String> {
    if needle.is_empty() || !needle.is_ascii() {
        return Ok(true);
    }
    let mut r = Reader::new(bytes);
    let version = r.u8("format version")?;
    if version != FORMAT_VERSION {
        return Err(format!(
            "block: unsupported format version {version} (this build speaks {FORMAT_VERSION})"
        ));
    }
    let codec = r.u8("codec")?;
    if codec != CODEC_RICH_TEMPLATE {
        return Ok(true);
    }
    let _n = r.u32("entry_count")?;
    let _ts_min = r.i64("ts_min")?;
    let _ts_max = r.i64("ts_max")?;
    let lens = [
        r.u32("ts column length")? as usize,
        r.u32("level column length")? as usize,
        r.u32("message column length")? as usize,
        r.u32("metadata column length")? as usize,
    ];
    let _ts = r.take(lens[0], COLUMN_NAMES[0])?;
    let _lvl = r.take(lens[1], COLUMN_NAMES[1])?;
    let msg = r.take(lens[2], COLUMN_NAMES[2])?;
    template::column_may_contain(msg, needle)
}

fn decode_block_legacy(codec: u8, n: usize, stored: Vec<&[u8]>) -> Result<Vec<LogEntry>, String> {
    // Decompress columns for codec 2. `Cow`-style: raw columns borrow,
    // zstd columns own — a Vec<u8> per column either way keeps it simple
    // (blocks are a few hundred KB at most).
    let cols: Vec<Vec<u8>> = if codec == CODEC_ZSTD {
        stored
            .iter()
            .enumerate()
            .map(|(i, c)| zstd_decompress(c, COLUMN_NAMES[i]))
            .collect::<Result<_, _>>()?
    } else {
        stored.iter().map(|c| c.to_vec()).collect()
    };

    // ── Column 1: timestamps ─────────────────────────────────────────
    if cols[0].len() != n * 8 {
        return Err(format!(
            "block: ts column is {} bytes, expected {} for {n} entries",
            cols[0].len(),
            n * 8
        ));
    }
    let mut timestamps = Vec::with_capacity(n);
    if codec == CODEC_ZSTD {
        let mut prev = 0i64;
        for c in cols[0].chunks_exact(8) {
            prev = prev.wrapping_add(i64::from_le_bytes(c.try_into().unwrap()));
            timestamps.push(prev);
        }
    } else {
        for c in cols[0].chunks_exact(8) {
            timestamps.push(i64::from_le_bytes(c.try_into().unwrap()));
        }
    }

    // ── Column 2: levels ─────────────────────────────────────────────
    if cols[1].len() != n {
        return Err(format!(
            "block: level column is {} bytes, expected {n}",
            cols[1].len()
        ));
    }
    for (i, &lvl) in cols[1].iter().enumerate() {
        if lvl > 3 {
            return Err(format!("block: entry {i} has invalid level byte {lvl}"));
        }
    }

    // ── Column 3: messages ───────────────────────────────────────────
    let mut messages = Vec::with_capacity(n);
    let mut mr = Reader::new(&cols[2]);
    for i in 0..n {
        let len = mr.u32("message length")? as usize;
        let b = mr.take(len, "message bytes")?;
        let s = std::str::from_utf8(b)
            .map_err(|_| format!("block: entry {i}: message is not valid UTF-8"))?;
        messages.push(s.to_owned());
    }
    if mr.remaining() != 0 {
        return Err("block: trailing bytes in message column".into());
    }

    // ── Column 4: metadata ───────────────────────────────────────────
    let rich = codec == CODEC_RICH_RAW;
    let rich_metadatas = if rich {
        Some(parse_rich_metadata(&cols[3], n)?)
    } else {
        None
    };
    let metadatas = if rich {
        Vec::new()
    } else {
        parse_metadata(&cols[3], n)?
    };

    // ── Zip the columns back into entries ────────────────────────────
    let mut out = Vec::with_capacity(n);
    let mut msg_it = messages.into_iter();
    let mut md_it = metadatas.into_iter();
    let mut rich_it = rich_metadatas.into_iter().flatten();
    for i in 0..n {
        let (metadata, severity, metadata_json) = if rich {
            let rich = rich_it.next().unwrap();
            (rich.metadata, Some(rich.severity), Some(rich.metadata_json))
        } else {
            (md_it.next().unwrap(), None, None)
        };
        out.push(LogEntry {
            ts: timestamps[i],
            level: cols[1][i],
            severity,
            message: msg_it.next().unwrap(),
            metadata,
            metadata_json,
        });
    }
    Ok(out)
}

const COLUMN_NAMES: [&str; 4] = [
    "ts column",
    "level column",
    "message column",
    "metadata column",
];

/// The metadata pair serialization — u16 pair count, then per pair u16
/// key length (keys are short identifiers; >64KB keys are rejected as
/// nonsense) and u32 value length (values can be long). Shared by every
/// codec: 1 stores it raw, 2 and 4 zstd it.
fn serialize_metadata(entries: &[LogEntry]) -> Result<Vec<u8>, String> {
    let mut col_meta = Vec::new();
    for e in entries {
        if e.metadata.len() > u16::MAX as usize {
            return Err("encode_block: more than 65535 metadata pairs in one entry".into());
        }
        col_meta.extend_from_slice(&(e.metadata.len() as u16).to_le_bytes());
        for (k, v) in &e.metadata {
            let (kb, vb) = (k.as_bytes(), v.as_bytes());
            if kb.len() > u16::MAX as usize {
                return Err(format!("encode_block: metadata key {k:?} longer than 64KB"));
            }
            if vb.len() > u32::MAX as usize {
                return Err(format!("encode_block: metadata value for {k:?} too long"));
            }
            col_meta.extend_from_slice(&(kb.len() as u16).to_le_bytes());
            col_meta.extend_from_slice(kb);
            col_meta.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            col_meta.extend_from_slice(vb);
        }
    }
    Ok(col_meta)
}

struct RichMetadata {
    severity: String,
    metadata: Vec<(String, String)>,
    metadata_json: String,
}

fn serialize_rich_metadata(entries: &[LogEntry]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for entry in entries {
        let severity = entry.severity_name().as_bytes();
        if severity.len() > u16::MAX as usize {
            return Err("encode_block: severity exceeds u16::MAX bytes".into());
        }
        let metadata_json = match &entry.metadata_json {
            Some(json) => canonical_metadata_json(json)?,
            None => pairs_metadata_json(&entry.metadata)?,
        };
        if metadata_json.len() > u32::MAX as usize {
            return Err("encode_block: metadata JSON exceeds u32::MAX bytes".into());
        }
        out.extend_from_slice(&(severity.len() as u16).to_le_bytes());
        out.extend_from_slice(severity);
        out.extend_from_slice(&(metadata_json.len() as u32).to_le_bytes());
        out.extend_from_slice(metadata_json.as_bytes());
    }
    Ok(out)
}

fn parse_rich_metadata(bytes: &[u8], n: usize) -> Result<Vec<RichMetadata>, String> {
    let mut reader = Reader::new(bytes);
    let mut out = Vec::with_capacity(n);
    for index in 0..n {
        let severity_len = reader.u16("severity length")? as usize;
        let severity = std::str::from_utf8(reader.take(severity_len, "severity bytes")?)
            .map_err(|_| format!("block: entry {index}: severity is not valid UTF-8"))?
            .to_owned();
        super::canonical_severity(&severity)
            .map_err(|error| format!("block: entry {index}: {error}"))?;
        let json_len = reader.u32("metadata JSON length")? as usize;
        let json = std::str::from_utf8(reader.take(json_len, "metadata JSON bytes")?)
            .map_err(|_| format!("block: entry {index}: metadata JSON is not UTF-8"))?;
        let metadata_json = canonical_metadata_json(json)
            .map_err(|error| format!("block: entry {index}: {error}"))?;
        let metadata = metadata_pairs_from_json(&metadata_json)?;
        out.push(RichMetadata {
            severity,
            metadata,
            metadata_json,
        });
    }
    if reader.remaining() != 0 {
        return Err("block: trailing bytes in rich metadata column".into());
    }
    Ok(out)
}

fn canonical_metadata_json(json: &str) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|error| format!("metadata JSON: {error}"))?;
    let serde_json::Value::Object(object) = value else {
        return Err("metadata JSON must be an object".into());
    };
    let sorted: std::collections::BTreeMap<String, serde_json::Value> =
        object.into_iter().collect();
    serde_json::to_string(&sorted).map_err(|error| format!("metadata JSON encode: {error}"))
}

fn pairs_metadata_json(metadata: &[(String, String)]) -> Result<String, String> {
    let object: std::collections::BTreeMap<String, serde_json::Value> = metadata
        .iter()
        .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
        .collect();
    serde_json::to_string(&object).map_err(|error| format!("metadata JSON encode: {error}"))
}

fn metadata_pairs_from_json(json: &str) -> Result<Vec<(String, String)>, String> {
    let serde_json::Value::Object(object) = serde_json::from_str::<serde_json::Value>(json)
        .map_err(|error| format!("metadata JSON: {error}"))?
    else {
        return Err("metadata JSON must be an object".into());
    };
    Ok(object
        .into_iter()
        .map(|(key, value)| {
            let value = match value {
                serde_json::Value::String(value) => value,
                other => serde_json::to_string(&other).unwrap_or_default(),
            };
            (key, value)
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Codec-5 pair-column shredding — shared by the logs metadata column
// and (via pub(crate)) the spans attributes column, which store the
// same shape: per entry, a canonically SORTED, key-DEDUPED list of
// (String, String) pairs. That canonical form is enforced upstream at
// ingest, NOT here: blocks/engine.rs push() does
// `entry.metadata.sort_by(..); entry.metadata.dedup_by(..)` and
// spans/engine.rs push() does the same for attributes — so by the time
// a block is encoded, duplicate keys per entry are impossible and the
// pair order is deterministic. encode_pairs_column still VERIFIES the
// invariant (strictly increasing keys per entry) and falls back to the
// legacy bytes if a direct encode_block() caller hands it something
// else, because the shredded form can only reproduce canonical input
// bit-exactly.
// ---------------------------------------------------------------------------

/// SHREDDED vs LEGACY cap: shred only if the block's distinct key set
/// is ≤ 64 keys. Rationale: every shredded key pays a FIXED overhead
/// regardless of how many entries actually carry it — a ceil(n/8)-byte
/// presence bitmap (1KB at the 8192-entry merge target) plus the
/// ~14-byte frame+zstd floor of an encode_str column. Sane telemetry
/// schemas (OTel semantic conventions, app log fields) are dozens of
/// keys, comfortably under 64. A key-EXPLOSION block — request ids or
/// user ids misused as keys, approaching one distinct key per entry —
/// would pay that fixed cost thousands of times over (8192 keys ≈ 8MB
/// of bitmaps for a block whose legacy form is tens of KB), so those
/// blocks must not shred. 64 keeps the worst shredded overhead bounded
/// (~66KB/block) while never kicking in on real schemas.
pub(crate) const SHRED_MAX_KEYS: usize = 64;

/// Strategy byte values for the codec-5 pairs column. LEGACY means the
/// rest of the column is byte-identical to the codec-2/4 column (the
/// pair serialization above, zstd'd) — pathological blocks never
/// regress vs codec 4, they just pay 1 extra byte.
pub(crate) const PAIRS_LEGACY: u8 = 0;
pub(crate) const PAIRS_SHREDDED: u8 = 1;

/// Encode a pairs column (logs metadata / span attributes) the codec-5
/// way. `pairs` is one slice of canonical (sorted, deduped) key/value
/// pairs per entry; `legacy_raw` is the codec-2/4 serialization of the
/// same entries (already built by both callers), used verbatim when we
/// don't shred.
///
/// SHREDDED layout (after the strategy byte):
///
///   u16               n_keys (≤ SHRED_MAX_KEYS)
///   per key           u16 key_len + key bytes   (sorted, ascending)
///   per key, in the   ceil(n_entries/8) bytes   presence bitmap
///   same sorted       framed encode_str column  DENSE values — only
///   order                                       the entries whose
///                                               bitmap bit is set, in
///                                               entry order
///
/// Two deliberate choices worth defending:
///
///   - Values go through encode_str, so each key's value list gets the
///     adaptive dictionary/concat pick on ITS OWN distribution — a
///     "status" key with 8 distinct values dictionary-encodes even
///     when it shares the block with a high-cardinality "path" key.
///   - The shredded region is NOT additionally zstd'd as one blob: the
///     values (the bulk) are already compressed per key, so an outer
///     pass would mostly re-chew compressed bytes for nothing, and the
///     only incompressible leftovers — the keys table and the bitmaps
///     — are tiny (≤64 keys, ≤1KB/key of bitmap). Keeping them plain
///     also means decode can walk the layout with zero extra
///     allocations.
pub(crate) fn encode_pairs_column(
    pairs: &[&[(String, String)]],
    legacy_raw: &[u8],
    zstd_level: i32,
) -> Result<Vec<u8>, String> {
    // Pass 1: collect the distinct key set and verify the canonical
    // invariant (strictly increasing keys per entry ⇒ sorted AND no
    // duplicates). BTreeSet gives us the sorted key table for free.
    let mut keys: BTreeSet<&str> = BTreeSet::new();
    let mut canonical = true;
    'outer: for entry in pairs {
        for w in entry.windows(2) {
            if w[0].0 >= w[1].0 {
                canonical = false;
                break 'outer;
            }
        }
        for (k, _) in entry.iter() {
            keys.insert(k.as_str());
        }
    }

    if !canonical || keys.len() > SHRED_MAX_KEYS {
        // LEGACY: strategy byte + the codec-2/4 bytes verbatim.
        let compressed = zstd_compress(legacy_raw, zstd_level)?;
        let mut out = Vec::with_capacity(1 + compressed.len());
        out.push(PAIRS_LEGACY);
        out.extend_from_slice(&compressed);
        return Ok(out);
    }

    let n = pairs.len();
    let mut out = Vec::new();
    out.push(PAIRS_SHREDDED);

    // Keys table: count + len-prefixed sorted keys. Key length is u16,
    // same policy as serialize_metadata (a >64KB key is nonsense).
    out.extend_from_slice(&(keys.len() as u16).to_le_bytes());
    for k in &keys {
        if k.len() > u16::MAX as usize {
            return Err(format!("encode_pairs_column: key {k:?} longer than 64KB"));
        }
        out.extend_from_slice(&(k.len() as u16).to_le_bytes());
        out.extend_from_slice(k.as_bytes());
    }

    // Per key: presence bitmap + dense value column. binary_search is
    // valid because we just verified every entry is sorted by key.
    for k in &keys {
        let mut present = Vec::with_capacity(n);
        let mut dense: Vec<&str> = Vec::new();
        for entry in pairs {
            match entry.binary_search_by(|(ek, _)| ek.as_str().cmp(k)) {
                Ok(i) => {
                    present.push(true);
                    dense.push(entry[i].1.as_str());
                }
                Err(_) => present.push(false),
            }
        }
        out.extend_from_slice(&encode_bitmap(&present));
        let n_dense = dense.len();
        out.extend_from_slice(&encode_str(dense, n_dense, zstd_level)?.to_bytes());
    }
    Ok(out)
}

/// Decode a codec-5 pairs column back to one pair list per entry —
/// bit-identical to the (canonical) input. `what` names the column in
/// errors ("metadata"/"attribute"); `parse_legacy` is the caller's
/// legacy pair parser (parse_metadata / parse_attributes), so LEGACY
/// blocks keep their existing error vocabulary.
pub(crate) fn decode_pairs_column(
    bytes: &[u8],
    n: usize,
    what: &str,
    parse_legacy: impl Fn(&[u8], usize) -> Result<Vec<Vec<(String, String)>>, String>,
) -> Result<Vec<Vec<(String, String)>>, String> {
    let mut r = Reader::new(bytes);
    let strategy = r.u8(&format!("{what} strategy byte"))?;
    match strategy {
        PAIRS_LEGACY => {
            let raw = zstd_decompress(r.take(r.remaining(), what)?, &format!("{what} column"))?;
            parse_legacy(&raw, n)
        }
        PAIRS_SHREDDED => {
            let n_keys = r.u16(&format!("{what} key count"))? as usize;
            if n_keys > SHRED_MAX_KEYS {
                return Err(format!(
                    "{what} column: {n_keys} shredded keys exceeds the cap {SHRED_MAX_KEYS}"
                ));
            }
            // Keys table. Must be strictly ascending — the encoder
            // writes a BTreeSet — so we validate that too: order is
            // what lets decode emit each entry's pairs pre-sorted.
            let mut keys: Vec<String> = Vec::with_capacity(n_keys);
            for i in 0..n_keys {
                let klen = r.u16(&format!("{what} key length"))? as usize;
                let kb = r.take(klen, &format!("{what} key bytes"))?;
                let k = std::str::from_utf8(kb)
                    .map_err(|_| format!("{what} column: key {i} is not valid UTF-8"))?;
                if let Some(prev) = keys.last() {
                    if prev.as_str() >= k {
                        return Err(format!("{what} column: keys table is not strictly sorted"));
                    }
                }
                keys.push(k.to_owned());
            }

            let mut out: Vec<Vec<(String, String)>> = vec![Vec::new(); n];
            for key in &keys {
                let bm = r.take(bitmap_len(n), &format!("{what} presence bitmap"))?;
                let present =
                    decode_bitmap(bm, n).map_err(|e| format!("{what} column, key {key:?}: {e}"))?;
                let n_present = present.iter().filter(|&&b| b).count();
                let frame = r.framed_column(&format!("{what} value column"))?;
                let values = decode_str(frame, n_present)
                    .map_err(|e| format!("{what} column, key {key:?}: {e}"))?;
                let mut vi = values.into_iter();
                for (entry, &p) in out.iter_mut().zip(&present) {
                    if p {
                        entry.push((key.clone(), vi.next().unwrap()));
                    }
                }
            }
            if r.remaining() != 0 {
                return Err(format!("{what} column: trailing bytes after last key"));
            }
            Ok(out)
        }
        other => Err(format!("{what} column: unknown strategy byte {other}")),
    }
}

/// Exact inverse of [`serialize_metadata`], shared by every decode path.
fn parse_metadata(raw: &[u8], n: usize) -> Result<Vec<Vec<(String, String)>>, String> {
    let mut metadatas = Vec::with_capacity(n);
    let mut tr = Reader::new(raw);
    for i in 0..n {
        let pairs = tr.u16("metadata pair count")? as usize;
        let mut md = Vec::with_capacity(pairs);
        for _ in 0..pairs {
            let klen = tr.u16("metadata key length")? as usize;
            let kb = tr.take(klen, "metadata key")?;
            let k = std::str::from_utf8(kb)
                .map_err(|_| format!("block: entry {i}: metadata key is not valid UTF-8"))?;
            let vlen = tr.u32("metadata value length")? as usize;
            let vb = tr.take(vlen, "metadata value")?;
            let v = std::str::from_utf8(vb)
                .map_err(|_| format!("block: entry {i}: metadata value is not valid UTF-8"))?;
            md.push((k.to_owned(), v.to_owned()));
        }
        metadatas.push(md);
    }
    if tr.remaining() != 0 {
        return Err("block: trailing bytes in metadata column".into());
    }
    Ok(metadatas)
}
