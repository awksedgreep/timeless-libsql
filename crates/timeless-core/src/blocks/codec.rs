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
//!                        guesses. This is what optimize() writes since
//!                        the Session 7 bake-off.
//!
//! Codec-4 metadata note: the metadata column keeps TODAY'S pair
//! serialization (below) compressed with plain zstd — same bytes as
//! codec 2. Splitting metadata into PER-KEY typed columns (a "status"
//! column would dictionary-encode beautifully) is future work: it
//! needs a key manifest in the block and interacts with the term
//! index, so it's a format revision of its own, not a tonight job.
//!
//! Container layout (all integers little-endian, IDENTICAL for all
//! codecs — only the column payloads differ):
//!
//!   offset  size  field
//!   0       1     format version (0x01)
//!   1       1     codec (1, 2 or 4)
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

use timeless_codec::{
    decode_i64, decode_str, decode_u8, encode_i64, encode_str, encode_u8, zstd_compress,
    zstd_decompress, Reader,
};

use super::{BlockMeta, LogEntry};

pub const CODEC_RAW: u8 = 1;
pub const CODEC_ZSTD: u8 = 2;
/// Codec 3 stays reserved for OpenZL — see the module header.
pub const CODEC_COLUMNAR: u8 = 4;

const FORMAT_VERSION: u8 = 1;
const HEADER_LEN: usize = 38;

fn known_codec(codec: u8) -> bool {
    codec == CODEC_RAW || codec == CODEC_ZSTD || codec == CODEC_COLUMNAR
}

/// Encode `entries` into one block payload. Entries should already be
/// sorted by ts (the engine sorts at flush); the codec doesn't REQUIRE
/// it (deltas may be negative) but sorted input compresses better and
/// keeps ts_min/ts_max cheap to trust.
///
/// `zstd_level` is consulted for CODEC_ZSTD and CODEC_COLUMNAR. Level 7
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
    let col_meta_raw = serialize_metadata(entries)?;

    let columns: [Vec<u8>; 4] = match codec {
        CODEC_COLUMNAR => {
            // Codec 4: typed column encoders pick their own strategy
            // (and record it in the column frame). The ts delta pass
            // lives INSIDE encode_i64 now; we hand it absolutes.
            let ts_values: Vec<i64> = entries.iter().map(|e| e.ts).collect();
            [
                encode_i64(&ts_values, zstd_level)?.to_bytes(),
                encode_u8(&col_lvl_raw, zstd_level)?.to_bytes(),
                encode_str(entries.iter().map(|e| e.message.as_str()), n, zstd_level)?
                    .to_bytes(),
                // Metadata: today's serialization + zstd, UNFRAMED
                // (byte-identical to the codec-2 column) — see the
                // module header for why per-key columns are future
                // work.
                zstd_compress(&col_meta_raw, zstd_level)?,
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
            if codec == CODEC_RAW {
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
/// every codec ever written (1, 2 and 4) — existing databases must
/// stay decodable forever, whatever optimize() currently emits.
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

    // ── Codec 4: typed column decoders ───────────────────────────────
    if codec == CODEC_COLUMNAR {
        let timestamps = decode_i64(stored[0], n)?;
        let levels = decode_u8(stored[1], n)?;
        for (i, &lvl) in levels.iter().enumerate() {
            if lvl > 3 {
                return Err(format!("block: entry {i} has invalid level byte {lvl}"));
            }
        }
        let messages = decode_str(stored[2], n)?;
        let meta_raw = zstd_decompress(stored[3], "metadata column")?;
        let metadatas = parse_metadata(&meta_raw, n)?;

        let mut out = Vec::with_capacity(n);
        let mut msg_it = messages.into_iter();
        let mut md_it = metadatas.into_iter();
        for i in 0..n {
            out.push(LogEntry {
                ts: timestamps[i],
                level: levels[i],
                message: msg_it.next().unwrap(),
                metadata: md_it.next().unwrap(),
            });
        }
        return Ok(out);
    }

    // ── Codecs 1/2 — the Session 5 decode path, byte-for-byte ────────
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
    let metadatas = parse_metadata(&cols[3], n)?;

    // ── Zip the columns back into entries ────────────────────────────
    let mut out = Vec::with_capacity(n);
    let mut msg_it = messages.into_iter();
    let mut md_it = metadatas.into_iter();
    for i in 0..n {
        out.push(LogEntry {
            ts: timestamps[i],
            level: cols[1][i],
            message: msg_it.next().unwrap(),
            metadata: md_it.next().unwrap(),
        });
    }
    Ok(out)
}

const COLUMN_NAMES: [&str; 4] = ["ts column", "level column", "message column", "metadata column"];

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
