//! timeless-codec: typed column encoders with adaptive strategy
//! selection — the pure-Rust block codec behind the timeless log/trace
//! stores (PLAN.md "Codec strategy", DECIDED 2026-07-22).
//!
//! # The API guardrail (read this before adding anything)
//!
//! The public unit of this crate is the TYPED COLUMN ENCODER:
//! i64 / f64 / string / u8 / fixed-width-bytes columns in, framed
//! compressed bytes out. There is deliberately NO LogEntry, SpanEntry,
//! row, record or schema type in here. Logs are 4 fixed columns, spans
//! are 10, a future generic table is any STRICT schema — all of them
//! are compositions of these five encoders, assembled by THEIR code,
//! not ours. That composition (which column is which type, container
//! headers, entry counts) lives in the caller (timeless-core's
//! blocks/codec.rs and spans/codec.rs). Keeping rows out of this crate
//! is what makes it publishable and reusable; scope name is
//! positioning, not a technical ceiling.
//!
//! # How a column is chosen (adaptive selection)
//!
//! Every encoder knows a small menu of strategies and picks by
//! MEASURING, not guessing: encode a bounded sample with each
//! candidate, compare projected sizes, then encode the full column
//! with the winner. The extra sample pass is cheap (samples are capped
//! at 64Ki values / the strategies are fast) and buys us robustness:
//! ms-jitter timestamps love delta+pco, but a pathological column
//! (random i64s, say) silently falls back to zstd instead of
//! ballooning. The chosen strategy is recorded in the wire format, so
//! decode never guesses.
//!
//! Strategy menu (see the per-encoder docs for the rationale):
//!   i64          delta+pco  vs  delta+zstd         (sampled pick)
//!   f64          pco        vs  zstd of LE bytes   (sampled pick)
//!   str          dictionary vs  concat+zstd        (distinct-ratio pick)
//!   u8           RLE        vs  zstd               (full encode, tiny)
//!   fixed bytes  zstd only                         (irreducible ids)
//!
//! NOT in the menu, by owner decision: FSST (prior results poor, and
//! our access pattern decompresses whole blocks so FSST's random-access
//! edge is never collected while its ratio deficit vs concatenated
//! zstd is always paid). Codec id 3 in the CALLERS' container header
//! stays reserved for OpenZL and is untouched by this crate.
//!
//! # Wire format
//!
//! One encoded column = `[u8 encoding_id][u32 LE payload_len][payload]`.
//! The encoding_id constants below are crate-public and ON-DISK STABLE:
//! never renumber them, only append. The payload_len is validated
//! against the enclosing buffer on decode (bounds-checked `Reader`),
//! so a corrupt column is an error naming the field, never a panic.
//!
//! # Exactness contract
//!
//! Every encoder round-trips BIT-EXACTLY: i64::MIN/MAX, negative and
//! unsorted values, unicode strings, and every f64 NaN bit pattern
//! (verified via to_bits in the tests — pco's float handling is
//! lossless over the raw bits, not the numeric value).

use std::collections::{BTreeMap, HashSet};

// ---------------------------------------------------------------------------
// Encoding ids — crate-public, on-disk stable. Grouped by column type;
// an id only ever appears in columns of its type, but the numbering is
// globally unique anyway so a mismatched decode fails loudly instead
// of misinterpreting a payload.
// ---------------------------------------------------------------------------

/// i64: wrapping delta (first value absolute), then pco.
pub const ENC_I64_DELTA_PCO: u8 = 1;
/// i64: wrapping delta, values as LE bytes, then zstd.
pub const ENC_I64_DELTA_ZSTD: u8 = 2;
/// f64: pco directly over the floats (bit-exact, NaNs included).
pub const ENC_F64_PCO: u8 = 3;
/// f64: values as LE bit patterns, then zstd.
pub const ENC_F64_ZSTD: u8 = 4;
/// str: sorted-unique dictionary (zstd) + u32 codes (RLE, then zstd).
pub const ENC_STR_DICT: u8 = 5;
/// str: u32-len-prefixed UTF-8 concatenated, then zstd. This is the
/// logs codec-2 message-column format, moved here VERBATIM as a
/// strategy (same bytes a codec-2 block would hold, minus the frame).
pub const ENC_STR_ZSTD: u8 = 6;
/// u8: run-length encoding, (u32 run_len, u8 value) pairs.
pub const ENC_U8_RLE: u8 = 7;
/// u8: plain zstd.
pub const ENC_U8_ZSTD: u8 = 8;
/// fixed-width byte groups: plain zstd.
pub const ENC_FIXED_ZSTD: u8 = 9;

/// Adaptive-selection sample cap: strategies are auditioned on the
/// first min(len, 65536) values. 64Ki is enough for the size ranking
/// to be stable (both pco and zstd have converged well before that)
/// while keeping the double-encode cost bounded for huge columns.
const SAMPLE_LEN: usize = 65536;

/// Frame overhead per column: 1 byte encoding id + 4 bytes payload len.
const FRAME_LEN: usize = 5;

// ---------------------------------------------------------------------------
// ColumnEnc — one encoded column, strategy tag + payload.
// ---------------------------------------------------------------------------

/// One encoded column. `encoding` is the winning strategy's id (one of
/// the `ENC_*` constants); `payload` is that strategy's output. Callers
/// serialize with [`ColumnEnc::to_bytes`] and get the framed wire form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnEnc {
    pub encoding: u8,
    pub payload: Vec<u8>,
}

impl ColumnEnc {
    /// Serialize to the wire form: `[u8 encoding_id][u32 LE len][payload]`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(FRAME_LEN + self.payload.len());
        out.push(self.encoding);
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    /// Size on the wire (frame + payload) — what adaptive selection
    /// compares, so the 5-byte frame is charged to every candidate
    /// equally and never tips a decision.
    pub fn encoded_len(&self) -> usize {
        FRAME_LEN + self.payload.len()
    }
}

/// Parse a framed column: returns (encoding_id, payload). The whole of
/// `bytes` must be exactly one frame — trailing bytes are corruption
/// (the caller's container header said this slice IS the column).
pub fn read_column_frame<'a>(bytes: &'a [u8], what: &str) -> Result<(u8, &'a [u8]), String> {
    let mut r = Reader::new(bytes);
    let enc = r.u8(what)?;
    let len = r.u32(what)? as usize;
    let payload = r.take(len, what)?;
    if r.remaining() != 0 {
        return Err(format!(
            "{what}: {} trailing byte(s) after column payload",
            r.remaining()
        ));
    }
    Ok((enc, payload))
}

// ---------------------------------------------------------------------------
// Shared primitives: zstd helpers + bounds-checked Reader.
// Moved here from timeless-core's blocks/codec.rs (they were pub(crate)
// and shared with spans/codec.rs; now both codecs import THIS crate and
// the duplicated copies are gone).
// ---------------------------------------------------------------------------

pub fn zstd_compress(data: &[u8], level: i32) -> Result<Vec<u8>, String> {
    zstd::bulk::compress(data, level).map_err(|e| format!("zstd compress failed: {e}"))
}

pub fn zstd_decompress(data: &[u8], what: &str) -> Result<Vec<u8>, String> {
    // decompress() needs a capacity hint; decode_all streams instead and
    // handles any size without us trusting an attacker-controlled header
    // field for the allocation.
    zstd::stream::decode_all(data).map_err(|e| format!("zstd decompress of {what} failed: {e}"))
}

/// Bounds-checked byte reader: every read names what it was reading, so
/// corruption errors point at the exact field — never a panic, never a
/// silent short read. (Same pattern as the vtab's BatchReader.)
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8], String> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| format!("length overflow reading {what}"))?;
        if end > self.buf.len() {
            return Err(format!(
                "truncated: need {n} byte(s) for {what} at offset {}, only {} remain",
                self.pos,
                self.remaining()
            ));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    pub fn u8(&mut self, what: &str) -> Result<u8, String> {
        Ok(self.take(1, what)?[0])
    }

    pub fn u16(&mut self, what: &str) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2, what)?.try_into().unwrap()))
    }

    pub fn u32(&mut self, what: &str) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4, what)?.try_into().unwrap()))
    }

    pub fn i64(&mut self, what: &str) -> Result<i64, String> {
        Ok(i64::from_le_bytes(self.take(8, what)?.try_into().unwrap()))
    }
}

// ---------------------------------------------------------------------------
// pco helpers (private): default ChunkConfig, the same knob the metrics
// engine uses. pco's default level (8) is already in its sweet spot;
// exposing another tuning surface here would just be decision fatigue.
// ---------------------------------------------------------------------------

fn pco_compress<T: pco::data_types::Number>(nums: &[T]) -> Result<Vec<u8>, String> {
    pco::standalone::simple_compress(nums, &pco::ChunkConfig::default())
        .map_err(|e| format!("pco compress failed: {e}"))
}

fn pco_decompress<T: pco::data_types::Number>(bytes: &[u8], what: &str) -> Result<Vec<T>, String> {
    pco::standalone::simple_decompress(bytes).map_err(|e| format!("pco decompress of {what} failed: {e}"))
}

// ---------------------------------------------------------------------------
// i64 columns (timestamps, durations, any integer column)
// ---------------------------------------------------------------------------

/// Encode an i64 column. Both candidate strategies share the DELTA
/// pre-pass (first value absolute, then wrapping differences): sorted
/// timestamp columns become tiny repetitive numbers, and even unsorted
/// columns lose their common magnitude. On top of the deltas:
///
///   delta+pco   pco models the delta distribution directly (bit
///               packing + binning) — usually the winner on ms-jitter
///               timestamps and similar "small numbers with structure";
///   delta+zstd  the deltas as LE bytes through zstd — wins when the
///               deltas are highly REPETITIVE rather than merely small
///               (fixed cadence), and the safe fallback for hostile
///               distributions.
///
/// Adaptive pick: both candidates encode a min(len, 64Ki) sample, the
/// smaller projected size encodes the full column. When the sample IS
/// the full column (the common ≤8192-entry block case) the winning
/// sample encoding is reused as-is — no second pass.
pub fn encode_i64(values: &[i64], zstd_level: i32) -> Result<ColumnEnc, String> {
    // Empty column: an empty zstd payload is the canonical form (pco
    // would also work, but one canonical empty keeps tests and diffs
    // deterministic).
    if values.is_empty() {
        return Ok(ColumnEnc {
            encoding: ENC_I64_DELTA_ZSTD,
            payload: zstd_compress(&[], zstd_level)?,
        });
    }

    // Delta pre-pass. Wrapping arithmetic: i64::MIN/MAX neighbors must
    // round-trip, and wrapping_add on decode is the exact inverse.
    let mut deltas = Vec::with_capacity(values.len());
    let mut prev = 0i64;
    for &v in values {
        deltas.push(v.wrapping_sub(prev));
        prev = v;
    }

    let sample = &deltas[..deltas.len().min(SAMPLE_LEN)];
    let sample_is_all = sample.len() == deltas.len();

    let pco_sample = pco_compress(sample)?;
    let zstd_sample = zstd_compress(&i64s_to_le_bytes(sample), zstd_level)?;

    // Ties go to zstd: decode is faster and the dependency is already
    // paid for by every other column.
    if pco_sample.len() < zstd_sample.len() {
        let payload = if sample_is_all { pco_sample } else { pco_compress(&deltas)? };
        Ok(ColumnEnc { encoding: ENC_I64_DELTA_PCO, payload })
    } else {
        let payload = if sample_is_all {
            zstd_sample
        } else {
            zstd_compress(&i64s_to_le_bytes(&deltas), zstd_level)?
        };
        Ok(ColumnEnc { encoding: ENC_I64_DELTA_ZSTD, payload })
    }
}

/// Decode an i64 column (framed bytes) back to exactly `n` values.
pub fn decode_i64(bytes: &[u8], n: usize) -> Result<Vec<i64>, String> {
    let (enc, payload) = read_column_frame(bytes, "i64 column")?;
    let deltas: Vec<i64> = match enc {
        ENC_I64_DELTA_PCO => pco_decompress(payload, "i64 column")?,
        ENC_I64_DELTA_ZSTD => {
            let raw = zstd_decompress(payload, "i64 column")?;
            if raw.len() % 8 != 0 {
                return Err(format!("i64 column: {} bytes is not a multiple of 8", raw.len()));
            }
            raw.chunks_exact(8)
                .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
                .collect()
        }
        other => return Err(format!("i64 column: unknown encoding id {other}")),
    };
    if deltas.len() != n {
        return Err(format!("i64 column: decoded {} values, expected {n}", deltas.len()));
    }
    // Invert the delta pre-pass.
    let mut out = Vec::with_capacity(n);
    let mut prev = 0i64;
    for d in deltas {
        prev = prev.wrapping_add(d);
        out.push(prev);
    }
    Ok(out)
}

fn i64s_to_le_bytes(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 8);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// f64 columns
// ---------------------------------------------------------------------------

/// Encode an f64 column: pco directly over the floats vs zstd over the
/// LE bit patterns, adaptive pick over a min(len, 64Ki) sample exactly
/// like [`encode_i64`]. No delta pre-pass — floats don't delta cleanly
/// (pco's internal float decomposition does the equivalent job better).
///
/// BIT-EXACTNESS: both strategies preserve every bit pattern including
/// every flavor of NaN — pco's float path is a total-order bijection
/// over the raw bits, and the zstd path never even interprets them.
/// The tests verify via to_bits, not ==.
pub fn encode_f64(values: &[f64], zstd_level: i32) -> Result<ColumnEnc, String> {
    if values.is_empty() {
        return Ok(ColumnEnc {
            encoding: ENC_F64_ZSTD,
            payload: zstd_compress(&[], zstd_level)?,
        });
    }
    let sample = &values[..values.len().min(SAMPLE_LEN)];
    let sample_is_all = sample.len() == values.len();

    let pco_sample = pco_compress(sample)?;
    let zstd_sample = zstd_compress(&f64s_to_le_bytes(sample), zstd_level)?;

    if pco_sample.len() < zstd_sample.len() {
        let payload = if sample_is_all { pco_sample } else { pco_compress(values)? };
        Ok(ColumnEnc { encoding: ENC_F64_PCO, payload })
    } else {
        let payload = if sample_is_all {
            zstd_sample
        } else {
            zstd_compress(&f64s_to_le_bytes(values), zstd_level)?
        };
        Ok(ColumnEnc { encoding: ENC_F64_ZSTD, payload })
    }
}

/// Decode an f64 column (framed bytes) back to exactly `n` values.
pub fn decode_f64(bytes: &[u8], n: usize) -> Result<Vec<f64>, String> {
    let (enc, payload) = read_column_frame(bytes, "f64 column")?;
    let out: Vec<f64> = match enc {
        ENC_F64_PCO => pco_decompress(payload, "f64 column")?,
        ENC_F64_ZSTD => {
            let raw = zstd_decompress(payload, "f64 column")?;
            if raw.len() % 8 != 0 {
                return Err(format!("f64 column: {} bytes is not a multiple of 8", raw.len()));
            }
            raw.chunks_exact(8)
                .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
                .collect()
        }
        other => return Err(format!("f64 column: unknown encoding id {other}")),
    };
    if out.len() != n {
        return Err(format!("f64 column: decoded {} values, expected {n}", out.len()));
    }
    Ok(out)
}

fn f64s_to_le_bytes(values: &[f64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 8);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// String columns
// ---------------------------------------------------------------------------

/// Dictionary threshold: distinct/count ≤ 1/8 on the sample switches to
/// dictionary encoding. Below that ratio each string repeats ≥8x on
/// average, so paying for the string once (in the dict) plus a small
/// RLE'd code per occurrence beats re-feeding zstd the repetitions —
/// service names, http methods, operation names all sit FAR below 1/8.
/// Above it (messages with unique ids baked in), the dictionary is
/// nearly as large as the data and concat+zstd wins.
const DICT_MAX_RATIO_NUM: usize = 1; // distinct * 8 <= count
const DICT_MAX_RATIO_DEN: usize = 8;

/// Encode a string column from any iterator of &str (the callers hold
/// entries, not string arrays — an iterator avoids materializing an
/// intermediate Vec<String>). `n` is the expected count and is
/// validated: a mismatch is a caller bug worth failing loudly on.
///
/// Strategy pick by DISTINCT RATIO on a min(n, 64Ki) sample (a cheap
/// HashSet pass — no trial encodes needed; the ratio predicts the
/// winner reliably because the two strategies degenerate in opposite
/// directions):
///   ratio ≤ 1/8  → ENC_STR_DICT: sorted unique table (zstd), u32
///                  codes per row, codes RLE'd then zstd. Sorted rows
///                  (the engines sort by level/status, and runs of one
///                  service arrive together) make the RLE collapse.
///   otherwise    → ENC_STR_ZSTD: u32-len-prefixed concat, zstd — the
///                  codec-2 message format verbatim.
pub fn encode_str<'a, I>(strs: I, n: usize, zstd_level: i32) -> Result<ColumnEnc, String>
where
    I: IntoIterator<Item = &'a str>,
{
    let strs: Vec<&str> = strs.into_iter().collect();
    if strs.len() != n {
        return Err(format!(
            "encode_str: iterator yielded {} strings, caller said {n}",
            strs.len()
        ));
    }
    if n == 0 {
        return Ok(ColumnEnc {
            encoding: ENC_STR_ZSTD,
            payload: zstd_compress(&[], zstd_level)?,
        });
    }

    // Distinct ratio over the sample.
    let sample = &strs[..n.min(SAMPLE_LEN)];
    let distinct: HashSet<&str> = sample.iter().copied().collect();
    let dict_worthy = distinct.len() * DICT_MAX_RATIO_DEN <= sample.len() * DICT_MAX_RATIO_NUM;

    if dict_worthy {
        // ── Dictionary strategy ─────────────────────────────────────
        // Sorted unique table over the FULL column (the sample only
        // chose the strategy; correctness needs every string). BTreeMap
        // gives sorted-unique + code assignment in one structure.
        let mut table: BTreeMap<&str, u32> = BTreeMap::new();
        for s in &strs {
            table.entry(s).or_insert(0);
        }
        if table.len() > u32::MAX as usize {
            return Err("encode_str: more than u32::MAX distinct strings".into());
        }
        for (i, v) in table.values_mut().enumerate() {
            *v = i as u32;
        }

        // Dict blob: u32-len-prefixed sorted unique strings, zstd'd.
        // (Sorted order groups shared prefixes — zstd likes that.)
        let mut dict_blob = Vec::new();
        for s in table.keys() {
            if s.len() > u32::MAX as usize {
                return Err("encode_str: string longer than u32::MAX bytes".into());
            }
            dict_blob.extend_from_slice(&(s.len() as u32).to_le_bytes());
            dict_blob.extend_from_slice(s.as_bytes());
        }
        let dict_zstd = zstd_compress(&dict_blob, zstd_level)?;

        // Codes, RLE'd: (u32 run_len, u32 code) pairs, then zstd. The
        // RLE handles the sorted-input case (whole blocks of one
        // service = one pair); zstd mops up whatever repetition the
        // RLE missed on shuffled input.
        let mut rle = Vec::new();
        let mut iter = strs.iter().map(|s| table[s]);
        let mut cur = iter.next().unwrap(); // n > 0 checked above
        let mut run: u32 = 1;
        for code in iter {
            if code == cur && run < u32::MAX {
                run += 1;
            } else {
                rle.extend_from_slice(&run.to_le_bytes());
                rle.extend_from_slice(&cur.to_le_bytes());
                cur = code;
                run = 1;
            }
        }
        rle.extend_from_slice(&run.to_le_bytes());
        rle.extend_from_slice(&cur.to_le_bytes());
        let codes_zstd = zstd_compress(&rle, zstd_level)?;

        // Payload: [u32 dict_count][u32 dict_zstd_len][dict_zstd][codes_zstd].
        // codes_zstd runs to the end of the payload — its length is
        // implied by the frame, no second length field needed.
        let mut payload =
            Vec::with_capacity(8 + dict_zstd.len() + codes_zstd.len());
        payload.extend_from_slice(&(table.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(dict_zstd.len() as u32).to_le_bytes());
        payload.extend_from_slice(&dict_zstd);
        payload.extend_from_slice(&codes_zstd);
        Ok(ColumnEnc { encoding: ENC_STR_DICT, payload })
    } else {
        // ── Concat strategy (codec-2 message format, verbatim) ──────
        let mut concat = Vec::new();
        for s in &strs {
            if s.len() > u32::MAX as usize {
                return Err("encode_str: string longer than u32::MAX bytes".into());
            }
            concat.extend_from_slice(&(s.len() as u32).to_le_bytes());
            concat.extend_from_slice(s.as_bytes());
        }
        Ok(ColumnEnc {
            encoding: ENC_STR_ZSTD,
            payload: zstd_compress(&concat, zstd_level)?,
        })
    }
}

/// Decode a string column (framed bytes) back to exactly `n` strings.
pub fn decode_str(bytes: &[u8], n: usize) -> Result<Vec<String>, String> {
    let (enc, payload) = read_column_frame(bytes, "string column")?;
    match enc {
        ENC_STR_ZSTD => {
            let raw = zstd_decompress(payload, "string column")?;
            let mut r = Reader::new(&raw);
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let len = r.u32("string length")? as usize;
                let b = r.take(len, "string bytes")?;
                let s = std::str::from_utf8(b)
                    .map_err(|_| format!("string column: value {i} is not valid UTF-8"))?;
                out.push(s.to_owned());
            }
            if r.remaining() != 0 {
                return Err("string column: trailing bytes after last string".into());
            }
            Ok(out)
        }
        ENC_STR_DICT => {
            let mut r = Reader::new(payload);
            let dict_count = r.u32("dict count")? as usize;
            let dict_zstd_len = r.u32("dict zstd length")? as usize;
            let dict_zstd = r.take(dict_zstd_len, "dict bytes")?;
            let codes_zstd = r.take(r.remaining(), "code bytes")?;

            // Dictionary table.
            let dict_raw = zstd_decompress(dict_zstd, "string dictionary")?;
            let mut dr = Reader::new(&dict_raw);
            let mut dict: Vec<String> = Vec::with_capacity(dict_count);
            for i in 0..dict_count {
                let len = dr.u32("dict entry length")? as usize;
                let b = dr.take(len, "dict entry bytes")?;
                let s = std::str::from_utf8(b)
                    .map_err(|_| format!("string column: dict entry {i} is not valid UTF-8"))?;
                dict.push(s.to_owned());
            }
            if dr.remaining() != 0 {
                return Err("string column: trailing bytes in dictionary".into());
            }

            // RLE codes. Total is validated INCREMENTALLY against n so a
            // corrupt run length can't drive a huge allocation.
            let codes_raw = zstd_decompress(codes_zstd, "string codes")?;
            if codes_raw.len() % 8 != 0 {
                return Err("string column: RLE stream is not (u32,u32) pairs".into());
            }
            let mut out = Vec::with_capacity(n);
            for pair in codes_raw.chunks_exact(8) {
                let run = u32::from_le_bytes(pair[0..4].try_into().unwrap()) as usize;
                let code = u32::from_le_bytes(pair[4..8].try_into().unwrap()) as usize;
                if code >= dict.len() {
                    return Err(format!(
                        "string column: code {code} out of range (dict has {})",
                        dict.len()
                    ));
                }
                if run == 0 || out.len() + run > n {
                    return Err(format!(
                        "string column: RLE runs sum past expected count {n}"
                    ));
                }
                for _ in 0..run {
                    out.push(dict[code].clone());
                }
            }
            if out.len() != n {
                return Err(format!(
                    "string column: RLE expanded to {} values, expected {n}",
                    out.len()
                ));
            }
            Ok(out)
        }
        other => Err(format!("string column: unknown encoding id {other}")),
    }
}

// ---------------------------------------------------------------------------
// u8 columns (levels, kinds, statuses — near-constant after the
// engines' level/status partitioning, so RLE usually collapses them
// to a handful of bytes)
// ---------------------------------------------------------------------------

/// Encode a u8 column: RLE vs zstd, adaptive. These columns are at most
/// a few KB (one byte per entry), so BOTH strategies encode the full
/// column and the smaller wins — no sampling machinery needed at this
/// size. After level/status-partitioned flushes the column is one
/// constant value, i.e. a single 5-byte RLE pair, which even zstd's
/// header can't beat.
pub fn encode_u8(values: &[u8], zstd_level: i32) -> Result<ColumnEnc, String> {
    if values.is_empty() {
        return Ok(ColumnEnc {
            encoding: ENC_U8_ZSTD,
            payload: zstd_compress(&[], zstd_level)?,
        });
    }
    // RLE: (u32 run_len, u8 value) pairs.
    let mut rle = Vec::new();
    let mut cur = values[0];
    let mut run: u32 = 1;
    for &v in &values[1..] {
        if v == cur && run < u32::MAX {
            run += 1;
        } else {
            rle.extend_from_slice(&run.to_le_bytes());
            rle.push(cur);
            cur = v;
            run = 1;
        }
    }
    rle.extend_from_slice(&run.to_le_bytes());
    rle.push(cur);

    let zstd_payload = zstd_compress(values, zstd_level)?;
    if rle.len() <= zstd_payload.len() {
        Ok(ColumnEnc { encoding: ENC_U8_RLE, payload: rle })
    } else {
        Ok(ColumnEnc { encoding: ENC_U8_ZSTD, payload: zstd_payload })
    }
}

/// Decode a u8 column (framed bytes) back to exactly `n` values.
pub fn decode_u8(bytes: &[u8], n: usize) -> Result<Vec<u8>, String> {
    let (enc, payload) = read_column_frame(bytes, "u8 column")?;
    match enc {
        ENC_U8_RLE => {
            if payload.len() % 5 != 0 {
                return Err("u8 column: RLE stream is not (u32,u8) pairs".into());
            }
            let mut out = Vec::with_capacity(n);
            for pair in payload.chunks_exact(5) {
                let run = u32::from_le_bytes(pair[0..4].try_into().unwrap()) as usize;
                let val = pair[4];
                // Incremental cap: corrupt run lengths must not allocate.
                if run == 0 || out.len() + run > n {
                    return Err(format!("u8 column: RLE runs sum past expected count {n}"));
                }
                out.resize(out.len() + run, val);
            }
            if out.len() != n {
                return Err(format!(
                    "u8 column: RLE expanded to {} values, expected {n}",
                    out.len()
                ));
            }
            Ok(out)
        }
        ENC_U8_ZSTD => {
            let raw = zstd_decompress(payload, "u8 column")?;
            if raw.len() != n {
                return Err(format!("u8 column: {} bytes, expected {n}", raw.len()));
            }
            Ok(raw)
        }
        other => Err(format!("u8 column: unknown encoding id {other}")),
    }
}

// ---------------------------------------------------------------------------
// Fixed-width byte columns (trace ids, span ids)
// ---------------------------------------------------------------------------

/// Encode a column of fixed-width byte values, passed as one flat
/// buffer (`data.len()` must be a multiple of `width`). zstd only —
/// trace/span ids are random bytes, i.e. IRREDUCIBLE: no transform
/// creates structure that isn't there, so the only honest menu entry
/// is zstd catching accidental repetition (e.g. many spans of one
/// trace sharing a block → repeated 16-byte trace ids).
///
/// Why no byte-plane TRANSPOSE tonight: transposing id bytes (all
/// byte-0s together, then all byte-1s...) only helps when bytes at the
/// same position correlate across values — true for counters/UUIDv7,
/// FALSE for the random ids OTel mandates, where it just shuffles
/// incompressible bytes at CPU cost. If sequential-id workloads show
/// up, add ENC_FIXED_TRANSPOSE_ZSTD as a new id and let a sample pick.
pub fn encode_fixed_bytes(data: &[u8], width: usize, zstd_level: i32) -> Result<ColumnEnc, String> {
    if width == 0 {
        return Err("encode_fixed_bytes: width must be > 0".into());
    }
    if data.len() % width != 0 {
        return Err(format!(
            "encode_fixed_bytes: {} bytes is not a multiple of width {width}",
            data.len()
        ));
    }
    Ok(ColumnEnc {
        encoding: ENC_FIXED_ZSTD,
        payload: zstd_compress(data, zstd_level)?,
    })
}

/// Decode a fixed-width byte column (framed bytes) back to the flat
/// buffer of exactly `n * width` bytes.
pub fn decode_fixed_bytes(bytes: &[u8], n: usize, width: usize) -> Result<Vec<u8>, String> {
    let (enc, payload) = read_column_frame(bytes, "fixed-bytes column")?;
    match enc {
        ENC_FIXED_ZSTD => {
            let raw = zstd_decompress(payload, "fixed-bytes column")?;
            if raw.len() != n * width {
                return Err(format!(
                    "fixed-bytes column: {} bytes, expected {} ({n} x {width})",
                    raw.len(),
                    n * width
                ));
            }
            Ok(raw)
        }
        other => Err(format!("fixed-bytes column: unknown encoding id {other}")),
    }
}

// ---------------------------------------------------------------------------
// Tests: exactness per encoder — empty, single, all-identical,
// all-distinct, negatives, unsorted, i64::MIN/MAX edges, unicode,
// NaN-bit-pattern f64 preservation. Every round-trip is BIT-exact.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const LVL: i32 = 7; // the engines' zstd level

    fn rt_i64(values: &[i64]) {
        let enc = encode_i64(values, LVL).unwrap();
        let back = decode_i64(&enc.to_bytes(), values.len()).unwrap();
        assert_eq!(back, values, "i64 round-trip (encoding {})", enc.encoding);
    }

    #[test]
    fn i64_edges() {
        rt_i64(&[]);
        rt_i64(&[42]);
        rt_i64(&[7; 1000]); // all identical
        rt_i64(&(0..1000).collect::<Vec<i64>>()); // all distinct, sorted
        rt_i64(&[-5, -1, -1000, 3, 0, -7]); // negatives, unsorted
        rt_i64(&[i64::MIN, i64::MAX, 0, i64::MIN + 1, i64::MAX - 1, -1]); // extremes (wrapping deltas)
        // ms-jitter-ish timestamps (what the ts columns actually look like)
        let mut ts = Vec::new();
        let mut t = 1_700_000_000_000i64;
        for i in 0..5000 {
            t += 3 + (i % 3) - 1;
            ts.push(t);
        }
        rt_i64(&ts);
    }

    #[test]
    fn i64_large_column_exceeds_sample() {
        // > SAMPLE_LEN values: the winner re-encodes the FULL column;
        // prove the full path (not just the reused-sample path) is exact.
        let values: Vec<i64> = (0..70_000).map(|i| 1_000_000 + i * 3 + (i % 7)).collect();
        rt_i64(&values);
    }

    fn rt_f64_bits(values: &[f64]) {
        let enc = encode_f64(values, LVL).unwrap();
        let back = decode_f64(&enc.to_bytes(), values.len()).unwrap();
        assert_eq!(back.len(), values.len());
        for (i, (a, b)) in values.iter().zip(&back).enumerate() {
            // Bit-exact, not ==: NaN != NaN and -0.0 == 0.0 would both
            // lie to us here.
            assert_eq!(
                a.to_bits(),
                b.to_bits(),
                "f64 value {i} not bit-exact (encoding {})",
                enc.encoding
            );
        }
    }

    #[test]
    fn f64_edges() {
        rt_f64_bits(&[]);
        rt_f64_bits(&[3.25]);
        rt_f64_bits(&[1.5; 512]); // all identical
        rt_f64_bits(&(0..1000).map(|i| i as f64 * 0.1).collect::<Vec<_>>());
        rt_f64_bits(&[-1.5, 7.25, -0.0, 0.0, f64::MIN, f64::MAX, f64::INFINITY, f64::NEG_INFINITY]);
    }

    #[test]
    fn f64_nan_bit_patterns_preserved() {
        // Standard NaN, a payload-carrying quiet NaN, and a signaling-
        // style pattern: all must survive BIT-exactly through both
        // strategies (force each by column shape: repetitive → either
        // may win; assert on bits regardless of winner, then force the
        // zstd path with a tiny column and the pco path with a smooth
        // one salted with NaNs).
        let quiet = f64::from_bits(0x7FF8_0000_0000_0001);
        let payload = f64::from_bits(0x7FF8_DEAD_BEEF_CAFE);
        let negnan = f64::from_bits(0xFFF8_0000_0000_0042);
        rt_f64_bits(&[f64::NAN, quiet, payload, negnan]);
        let mut smooth: Vec<f64> = (0..4096).map(|i| i as f64).collect();
        smooth[7] = quiet;
        smooth[100] = payload;
        smooth[4000] = negnan;
        rt_f64_bits(&smooth);
    }

    fn rt_str(values: &[&str], expect_encoding: Option<u8>) {
        let enc = encode_str(values.iter().copied(), values.len(), LVL).unwrap();
        if let Some(want) = expect_encoding {
            assert_eq!(enc.encoding, want, "strategy pick for {values:?}");
        }
        let back = decode_str(&enc.to_bytes(), values.len()).unwrap();
        assert_eq!(back, values, "str round-trip (encoding {})", enc.encoding);
    }

    #[test]
    fn str_edges() {
        rt_str(&[], None);
        rt_str(&["solo"], None);
        rt_str(&["", "", ""], None); // empty strings are values too
        // Unicode: multi-byte, combining, RTL, emoji.
        rt_str(&["héllo wörld", "日本語のログ", "🚀🔥", "مرحبا", "a\u{0301}"], None);
    }

    #[test]
    fn str_dictionary_fires_on_low_cardinality() {
        // 3 distinct over 3000 rows = ratio 0.001 → dictionary.
        let services: Vec<&str> = (0..3000)
            .map(|i| ["api", "web", "auth"][i % 3])
            .collect();
        rt_str(&services, Some(ENC_STR_DICT));
    }

    #[test]
    fn str_concat_fires_on_high_cardinality() {
        // All distinct → ratio 1.0 → concat+zstd (codec-2 format).
        let owned: Vec<String> = (0..500).map(|i| format!("request {i} failed")).collect();
        let msgs: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        rt_str(&msgs, Some(ENC_STR_ZSTD));
    }

    #[test]
    fn str_count_mismatch_is_an_error() {
        assert!(encode_str(["a", "b"].into_iter(), 3, LVL).is_err());
    }

    fn rt_u8(values: &[u8]) {
        let enc = encode_u8(values, LVL).unwrap();
        let back = decode_u8(&enc.to_bytes(), values.len()).unwrap();
        assert_eq!(back, values, "u8 round-trip (encoding {})", enc.encoding);
    }

    #[test]
    fn u8_edges() {
        rt_u8(&[]);
        rt_u8(&[3]);
        rt_u8(&[1; 8192]); // the post-partitioning constant column
        rt_u8(&(0..=255).collect::<Vec<u8>>()); // all distinct
        rt_u8(&[0, 2, 1, 1, 3, 0, 0, 0, 2]); // unsorted mix
    }

    #[test]
    fn u8_constant_column_is_tiny() {
        // The whole point of RLE here: a level-pure block's level
        // column must collapse to one (u32, u8) pair.
        let enc = encode_u8(&[2u8; 8192], LVL).unwrap();
        assert_eq!(enc.encoding, ENC_U8_RLE);
        assert_eq!(enc.payload.len(), 5);
    }

    #[test]
    fn fixed_bytes_round_trip() {
        // Empty, one value, repeated ids (compressible), random-ish ids.
        for (data, width) in [
            (vec![], 16usize),
            (vec![0xAB; 16], 16),
            ([[7u8; 16], [7u8; 16], [9u8; 16]].concat(), 16),
            ((0..25u8).flat_map(|i| [i, i ^ 0x5A, 0, 255, i, 1, 2, 3]).collect::<Vec<u8>>(), 8),
        ] {
            let n = data.len() / width;
            let enc = encode_fixed_bytes(&data, width, LVL).unwrap();
            let back = decode_fixed_bytes(&enc.to_bytes(), n, width).unwrap();
            assert_eq!(back, data);
        }
    }

    #[test]
    fn fixed_bytes_rejects_misaligned() {
        assert!(encode_fixed_bytes(&[1, 2, 3], 2, LVL).is_err());
        assert!(encode_fixed_bytes(&[1, 2, 3], 0, LVL).is_err());
    }

    #[test]
    fn corrupt_frames_error_not_panic() {
        // Truncated frame, unknown encoding id, wrong count, garbage
        // payload: all must be Err with a field name, never a panic.
        assert!(decode_i64(&[], 0).is_err());
        assert!(decode_i64(&[99, 0, 0, 0, 0], 1).is_err()); // unknown id
        let enc = encode_i64(&[1, 2, 3], LVL).unwrap().to_bytes();
        assert!(decode_i64(&enc, 4).is_err()); // wrong n
        assert!(decode_i64(&enc[..enc.len() - 1], 3).is_err()); // truncated
        let mut garbage = enc.clone();
        let last = garbage.len() - 1;
        garbage[last] ^= 0xFF;
        let _ = decode_i64(&garbage, 3); // any Result is fine; no panic
        assert!(decode_str(&[ENC_STR_DICT, 1, 0, 0, 0, 7], 1).is_err());
        assert!(decode_u8(&[ENC_U8_RLE, 5, 0, 0, 0, 255, 255, 255, 255, 7], 3).is_err()); // run > n
    }
}
