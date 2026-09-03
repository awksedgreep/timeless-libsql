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
use std::mem::size_of;

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

fn validate_minimum_encoded_len(
    count: usize,
    width: usize,
    available: usize,
    what: &str,
) -> Result<(), String> {
    let minimum = count
        .checked_mul(width)
        .ok_or_else(|| format!("{what}: count overflows minimum encoded length"))?;
    if minimum > available {
        return Err(format!(
            "{what}: {count} entries require at least {minimum} bytes, but only {available} available"
        ));
    }
    Ok(())
}

fn reserve_decoded<T>(out: &mut Vec<T>, count: usize, what: &str) -> Result<(), String> {
    out.try_reserve(count)
        .map_err(|_| format!("{what}: cannot allocate {count} decoded entries"))
}

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
    /// Rejects payloads over u32::MAX instead of wrapping the length
    /// prefix (a wrapped prefix would decode as a different, shorter
    /// column — silent corruption, not a clean error).
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(FRAME_LEN + self.payload.len());
        out.push(self.encoding);
        out.extend_from_slice(&u32_len(self.payload.len(), "column payload")?.to_le_bytes());
        out.extend_from_slice(&self.payload);
        Ok(out)
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

/// Ceiling for entry counts read from untrusted container headers
/// (block `entry_count`, chunk `point_count`). Flush/merge thresholds
/// keep honest blocks at ≤8192 entries, so anything past 2^20 is
/// corruption, not a workload — and every downstream `Vec::with_capacity`
/// or caller-sized buffer keys off this count. Checked at the container
/// layer (blocks/spans/chunks), never in the column decoders, which
/// stay agnostic to engine sizing.
pub const MAX_BLOCK_ENTRIES: usize = 1 << 20;

/// Absolute ceiling for any single variable-width decompression
/// (string/dictionary/codes/metadata blobs whose exact output size is
/// not knowable before decode). 64 MiB matches the rollup payload cap:
/// at the engines' ≤8192-entry blocks a legitimate column never
/// approaches it, while a corrupt row can no longer turn kilobytes of
/// compressed input into gigabytes of output. Fixed-width columns use
/// exact `n * width` caps instead — see each decoder.
pub const DECOMPRESS_MAX_BYTES: usize = 64 << 20;

/// Bounded zstd decompression: output past `cap` bytes is an error,
/// never an allocation. `zstd::bulk::decompress` pre-sizes to
/// `min(frame_hint, cap)`, so a bomb cannot even drive the initial
/// allocation — the failure happens before the host feels it.
pub fn zstd_decompress_capped(data: &[u8], what: &str, cap: usize) -> Result<Vec<u8>, String> {
    zstd::bulk::decompress(data, cap).map_err(|e| format!("zstd decompress of {what} failed: {e}"))
}

/// Variable-width column decompression, bounded at
/// [`DECOMPRESS_MAX_BYTES`]. All block/span metadata, message, and
/// shredded-column paths route through here, so no corrupt row can
/// turn kilobytes of stored bytes into gigabytes of output. Callers
/// with an exact expected size (numeric columns) use
/// [`zstd_decompress_capped`] with `n * width` instead.
pub fn zstd_decompress(data: &[u8], what: &str) -> Result<Vec<u8>, String> {
    zstd_decompress_capped(data, what, DECOMPRESS_MAX_BYTES)
}

/// Exact output-size cap for fixed-width columns: `n` values of
/// `width` bytes. Checked so a corrupt count is an error before any
/// allocation, on every pointer width.
fn exact_cap(n: usize, width: usize, what: &str) -> Result<usize, String> {
    n.checked_mul(width)
        .ok_or_else(|| format!("{what}: count {n} x width {width} overflows usize"))
}

/// Reject absurd entry counts read from untrusted container headers
/// (block `entry_count`, chunk `point_count`). Checked at the
/// container layer, before any `Vec::with_capacity(n)`, caller-sized
/// pco buffer, or exact `n * width` cap runs — a corrupt count must
/// fail here, never drive an allocation.
pub fn check_entry_count(n: usize, what: &str) -> Result<(), String> {
    if n > MAX_BLOCK_ENTRIES {
        return Err(format!(
            "{what}: entry count {n} exceeds limit {MAX_BLOCK_ENTRIES}"
        ));
    }
    Ok(())
}

/// Verify a fully decoded container against its header range claims.
/// Pruning elsewhere trusts header metadata, so a corrupt header that
/// disagrees with its own payload must be an error — never silently
/// wrong query results. (A row corrupt enough to be skipped by pruning
/// never reaches this check; see the block/span codec headers for that
/// residual.)
pub fn check_block_range(
    what: &str,
    n: usize,
    ts_min: i64,
    ts_max: i64,
    timestamps: &[i64],
) -> Result<(), String> {
    if timestamps.len() != n {
        return Err(format!(
            "{what}: decoded {} entries, header claims {n}",
            timestamps.len()
        ));
    }
    let (lo, hi) = timestamps
        .iter()
        .fold((i64::MAX, i64::MIN), |(a, b), &t| (a.min(t), b.max(t)));
    if lo != ts_min || hi != ts_max {
        return Err(format!(
            "{what}: payload range [{lo}, {hi}] disagrees with header [{ts_min}, {ts_max}]"
        ));
    }
    Ok(())
}

/// Checked `usize -> u32` length prefix. Every on-wire length field in
/// this crate is u32 LE; a bare `as u32` would silently wrap past 4 GiB
/// and the decoder would then mis-parse the frame. All encoders route
/// through here so over-wide inputs are a clean `Err`, never a wrap.
pub fn u32_len(len: usize, what: &str) -> Result<u32, String> {
    u32::try_from(len).map_err(|_| format!("{what}: {len} byte(s) exceeds u32::MAX"))
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

    /// Consume ONE framed column (`[u8 id][u32 LE len][payload]`) from
    /// the stream and return the WHOLE frame slice, header included —
    /// ready to hand to `decode_str`/`decode_i64`/... which expect the
    /// framed form. This exists for containers that pack SEVERAL framed
    /// columns back to back with no outer length table (e.g. the
    /// shredded metadata layout: one `encode_str` column per key):
    /// [`read_column_frame`] insists the slice is exactly one frame, so
    /// sequential consumers need this cursor-advancing variant instead.
    pub fn framed_column(&mut self, what: &str) -> Result<&'a [u8], String> {
        let start = self.pos;
        let _encoding = self.u8(what)?;
        let len = self.u32(what)? as usize;
        self.take(len, what)?;
        Ok(&self.buf[start..self.pos])
    }
}

// ---------------------------------------------------------------------------
// Presence bitmaps — tiny, but they are WIRE FORMAT, so they live here
// next to the other on-disk-stable primitives (and get the same testing
// discipline). Used by callers that shred sparse per-entry key/value
// pairs into per-key columns: one bit per entry says "this entry has a
// value in the dense column".
//
// Layout: ceil(n/8) bytes, bit i of the stream = byte i/8, bit i%8
// (LSB-first — bit 0 is the LOWEST bit of byte 0). Trailing pad bits in
// the last byte MUST be zero and decode validates that: a canonical
// encoding means byte-identical re-encodes, and a set pad bit is a
// corruption signal we would otherwise silently swallow.
// ---------------------------------------------------------------------------

/// Bytes needed for an `n`-bit presence bitmap: ceil(n / 8).
pub fn bitmap_len(n: usize) -> usize {
    n.div_ceil(8)
}

/// Pack bools into an LSB-first bitmap (see the layout note above).
pub fn encode_bitmap(bits: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; bitmap_len(bits.len())];
    for (i, &b) in bits.iter().enumerate() {
        if b {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}

/// Unpack an `n`-bit LSB-first bitmap. Validates the byte count AND
/// that trailing pad bits are zero (canonical form — see above).
pub fn decode_bitmap(bytes: &[u8], n: usize) -> Result<Vec<bool>, String> {
    if bytes.len() != bitmap_len(n) {
        return Err(format!(
            "bitmap: {} byte(s), expected {} for {n} bits",
            bytes.len(),
            bitmap_len(n)
        ));
    }
    // Pad-bit check: mask off the n%8 valid bits of the last byte;
    // anything left set is not a bitmap we wrote.
    if !n.is_multiple_of(8) {
        let last = bytes[bytes.len() - 1];
        if last & !((1u8 << (n % 8)) - 1) != 0 {
            return Err("bitmap: nonzero padding bits in final byte".into());
        }
    }
    Ok((0..n).map(|i| bytes[i / 8] & (1 << (i % 8)) != 0).collect())
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

/// Bounded pco decompression into a caller-sized destination.
pub fn pco_decompress_capped<T: pco::data_types::Number>(
    bytes: &[u8],
    n: usize,
    what: &str,
) -> Result<Vec<T>, String> {
    // Bounded by construction: the destination is sized from the
    // caller-known count, never from the payload's own header claim
    // (which `simple_decompress` would trust for its allocation — a
    // corrupt header alone could OOM the host). A payload holding more
    // or fewer values than `n` fails the progress check below.
    let mut out = vec![T::default(); n];
    let progress = pco::standalone::simple_decompress_into(bytes, &mut out)
        .map_err(|e| format!("pco decompress of {what} failed: {e}"))?;
    if !progress.finished || progress.n_processed != n {
        return Err(format!(
            "pco decompress of {what}: stream holds {} values, expected {n}",
            progress.n_processed
        ));
    }
    Ok(out)
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
        let payload = if sample_is_all {
            pco_sample
        } else {
            pco_compress(&deltas)?
        };
        Ok(ColumnEnc {
            encoding: ENC_I64_DELTA_PCO,
            payload,
        })
    } else {
        let payload = if sample_is_all {
            zstd_sample
        } else {
            zstd_compress(&i64s_to_le_bytes(&deltas), zstd_level)?
        };
        Ok(ColumnEnc {
            encoding: ENC_I64_DELTA_ZSTD,
            payload,
        })
    }
}

/// Decode an i64 column (framed bytes) back to exactly `n` values.
pub fn decode_i64(bytes: &[u8], n: usize) -> Result<Vec<i64>, String> {
    let (enc, payload) = read_column_frame(bytes, "i64 column")?;
    let deltas: Vec<i64> = match enc {
        ENC_I64_DELTA_PCO => pco_decompress_capped(payload, n, "i64 column")?,
        ENC_I64_DELTA_ZSTD => {
            let raw =
                zstd_decompress_capped(payload, "i64 column", exact_cap(n, 8, "i64 column")?)?;
            if raw.len() % 8 != 0 {
                return Err(format!(
                    "i64 column: {} bytes is not a multiple of 8",
                    raw.len()
                ));
            }
            raw.as_chunks::<8>()
                .0
                .iter()
                .map(|c| i64::from_le_bytes(*c))
                .collect()
        }
        other => return Err(format!("i64 column: unknown encoding id {other}")),
    };
    if deltas.len() != n {
        return Err(format!(
            "i64 column: decoded {} values, expected {n}",
            deltas.len()
        ));
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
        let payload = if sample_is_all {
            pco_sample
        } else {
            pco_compress(values)?
        };
        Ok(ColumnEnc {
            encoding: ENC_F64_PCO,
            payload,
        })
    } else {
        let payload = if sample_is_all {
            zstd_sample
        } else {
            zstd_compress(&f64s_to_le_bytes(values), zstd_level)?
        };
        Ok(ColumnEnc {
            encoding: ENC_F64_ZSTD,
            payload,
        })
    }
}

/// Decode an f64 column (framed bytes) back to exactly `n` values.
pub fn decode_f64(bytes: &[u8], n: usize) -> Result<Vec<f64>, String> {
    let (enc, payload) = read_column_frame(bytes, "f64 column")?;
    let out: Vec<f64> = match enc {
        ENC_F64_PCO => pco_decompress_capped(payload, n, "f64 column")?,
        ENC_F64_ZSTD => {
            let raw =
                zstd_decompress_capped(payload, "f64 column", exact_cap(n, 8, "f64 column")?)?;
            if raw.len() % 8 != 0 {
                return Err(format!(
                    "f64 column: {} bytes is not a multiple of 8",
                    raw.len()
                ));
            }
            raw.as_chunks::<8>()
                .0
                .iter()
                .map(|c| f64::from_le_bytes(*c))
                .collect()
        }
        other => return Err(format!("f64 column: unknown encoding id {other}")),
    };
    if out.len() != n {
        return Err(format!(
            "f64 column: decoded {} values, expected {n}",
            out.len()
        ));
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
/// intermediate `Vec<String>`). `n` is the expected count and is
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
        let mut payload = Vec::with_capacity(8 + dict_zstd.len() + codes_zstd.len());
        payload.extend_from_slice(
            &u32_len(table.len(), "encode_str: dict entry count")?.to_le_bytes(),
        );
        payload
            .extend_from_slice(&u32_len(dict_zstd.len(), "encode_str: dictionary")?.to_le_bytes());
        payload.extend_from_slice(&dict_zstd);
        payload.extend_from_slice(&codes_zstd);
        Ok(ColumnEnc {
            encoding: ENC_STR_DICT,
            payload,
        })
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
            let raw = zstd_decompress_capped(payload, "string column", DECOMPRESS_MAX_BYTES)?;
            let mut r = Reader::new(&raw);
            validate_minimum_encoded_len(n, size_of::<u32>(), raw.len(), "string column")?;
            let mut out = Vec::new();
            reserve_decoded(&mut out, n, "string column")?;
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
            let dict_raw =
                zstd_decompress_capped(dict_zstd, "string dictionary", DECOMPRESS_MAX_BYTES)?;
            let mut dr = Reader::new(&dict_raw);
            validate_minimum_encoded_len(
                dict_count,
                size_of::<u32>(),
                dict_raw.len(),
                "string dictionary",
            )?;
            let mut dict: Vec<String> = Vec::new();
            reserve_decoded(&mut dict, dict_count, "string dictionary")?;
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
            let codes_raw =
                zstd_decompress_capped(codes_zstd, "string codes", DECOMPRESS_MAX_BYTES)?;
            if codes_raw.len() % 8 != 0 {
                return Err("string column: RLE stream is not (u32,u32) pairs".into());
            }
            let mut expanded = 0usize;
            for pair in codes_raw.as_chunks::<8>().0 {
                let run = u32::from_le_bytes(pair[0..4].try_into().unwrap()) as usize;
                let code = u32::from_le_bytes(pair[4..8].try_into().unwrap()) as usize;
                if code >= dict.len() {
                    return Err(format!(
                        "string column: code {code} out of range (dict has {})",
                        dict.len()
                    ));
                }
                if run == 0 || expanded.checked_add(run).is_none_or(|total| total > n) {
                    return Err(format!(
                        "string column: RLE runs sum past expected count {n}"
                    ));
                }
                expanded += run;
            }
            if expanded != n {
                return Err(format!(
                    "string column: RLE expanded to {expanded} values, expected {n}"
                ));
            }
            let mut out = Vec::new();
            reserve_decoded(&mut out, n, "string column")?;
            for pair in codes_raw.as_chunks::<8>().0 {
                let run = u32::from_le_bytes(pair[0..4].try_into().unwrap()) as usize;
                let code = u32::from_le_bytes(pair[4..8].try_into().unwrap()) as usize;
                for _ in 0..run {
                    out.push(dict[code].clone());
                }
            }
            Ok(out)
        }
        other => Err(format!("string column: unknown encoding id {other}")),
    }
}

/// Decode only the requested zero-based rows from a string column.
///
/// `selected` must be strictly increasing and each index must be below `n`.
/// The complete encoded stream is still validated, but unselected strings are
/// never materialized. This is the late-materialization path for callers that
/// first evaluate predicates from cheaper physical columns.
pub fn decode_str_selected(
    bytes: &[u8],
    n: usize,
    selected: &[usize],
) -> Result<Vec<String>, String> {
    if selected.windows(2).any(|pair| pair[0] >= pair[1])
        || selected.last().is_some_and(|index| *index >= n)
    {
        return Err("string column: selected rows must be strictly increasing and in range".into());
    }
    let (enc, payload) = read_column_frame(bytes, "string column")?;
    let mut out = Vec::with_capacity(selected.len());
    match enc {
        ENC_STR_ZSTD => {
            let raw = zstd_decompress_capped(payload, "string column", DECOMPRESS_MAX_BYTES)?;
            let mut reader = Reader::new(&raw);
            let mut selected_pos = 0usize;
            for row in 0..n {
                let len = reader.u32("string length")? as usize;
                let value = reader.take(len, "string bytes")?;
                let value = std::str::from_utf8(value)
                    .map_err(|_| format!("string column: value {row} is not valid UTF-8"))?;
                if selected.get(selected_pos) == Some(&row) {
                    out.push(value.to_owned());
                    selected_pos += 1;
                }
            }
            if reader.remaining() != 0 {
                return Err("string column: trailing bytes after last string".into());
            }
        }
        ENC_STR_DICT => {
            let mut reader = Reader::new(payload);
            let dict_count = reader.u32("dict count")? as usize;
            let dict_zstd_len = reader.u32("dict zstd length")? as usize;
            let dict_zstd = reader.take(dict_zstd_len, "dict bytes")?;
            let codes_zstd = reader.take(reader.remaining(), "code bytes")?;

            let dict_raw =
                zstd_decompress_capped(dict_zstd, "string dictionary", DECOMPRESS_MAX_BYTES)?;
            let mut dict_reader = Reader::new(&dict_raw);
            validate_minimum_encoded_len(
                dict_count,
                size_of::<u32>(),
                dict_raw.len(),
                "string dictionary",
            )?;
            let mut dict = Vec::new();
            reserve_decoded(&mut dict, dict_count, "string dictionary")?;
            for index in 0..dict_count {
                let len = dict_reader.u32("dict entry length")? as usize;
                let value = dict_reader.take(len, "dict entry bytes")?;
                let value = std::str::from_utf8(value)
                    .map_err(|_| format!("string column: dict entry {index} is not valid UTF-8"))?;
                dict.push(value.to_owned());
            }
            if dict_reader.remaining() != 0 {
                return Err("string column: trailing bytes in dictionary".into());
            }

            let codes_raw =
                zstd_decompress_capped(codes_zstd, "string codes", DECOMPRESS_MAX_BYTES)?;
            if codes_raw.len() % 8 != 0 {
                return Err("string column: RLE stream is not (u32,u32) pairs".into());
            }
            let mut row = 0usize;
            let mut selected_pos = 0usize;
            for pair in codes_raw.as_chunks::<8>().0 {
                let run = u32::from_le_bytes(pair[0..4].try_into().unwrap()) as usize;
                let code = u32::from_le_bytes(pair[4..8].try_into().unwrap()) as usize;
                if code >= dict.len() {
                    return Err(format!(
                        "string column: code {code} out of range (dict has {})",
                        dict.len()
                    ));
                }
                if run == 0 || row.checked_add(run).is_none_or(|end| end > n) {
                    return Err(format!(
                        "string column: RLE runs sum past expected count {n}"
                    ));
                }
                let end = row + run;
                while selected.get(selected_pos).is_some_and(|index| *index < end) {
                    debug_assert!(selected[selected_pos] >= row);
                    out.push(dict[code].clone());
                    selected_pos += 1;
                }
                row = end;
            }
            if row != n {
                return Err(format!(
                    "string column: RLE expanded to {row} values, expected {n}"
                ));
            }
        }
        other => return Err(format!("string column: unknown encoding id {other}")),
    }
    debug_assert_eq!(out.len(), selected.len());
    Ok(out)
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
        Ok(ColumnEnc {
            encoding: ENC_U8_RLE,
            payload: rle,
        })
    } else {
        Ok(ColumnEnc {
            encoding: ENC_U8_ZSTD,
            payload: zstd_payload,
        })
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
            for pair in payload.as_chunks::<5>().0 {
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
            let raw = zstd_decompress_capped(payload, "u8 column", n)?;
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
    if !data.len().is_multiple_of(width) {
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
            let want = exact_cap(n, width, "fixed-bytes column")?;
            let raw = zstd_decompress_capped(payload, "fixed-bytes column", want)?;
            if raw.len() != want {
                return Err(format!(
                    "fixed-bytes column: {} bytes, expected {} ({n} x {width})",
                    raw.len(),
                    want
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
        let back = decode_i64(&enc.to_bytes().unwrap(), values.len()).unwrap();
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
        let back = decode_f64(&enc.to_bytes().unwrap(), values.len()).unwrap();
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
        rt_f64_bits(&[
            -1.5,
            7.25,
            -0.0,
            0.0,
            f64::MIN,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ]);
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
        let back = decode_str(&enc.to_bytes().unwrap(), values.len()).unwrap();
        assert_eq!(back, values, "str round-trip (encoding {})", enc.encoding);
    }

    #[test]
    fn str_edges() {
        rt_str(&[], None);
        rt_str(&["solo"], None);
        rt_str(&["", "", ""], None); // empty strings are values too
                                     // Unicode: multi-byte, combining, RTL, emoji.
        rt_str(
            &["héllo wörld", "日本語のログ", "🚀🔥", "مرحبا", "a\u{0301}"],
            None,
        );
    }

    #[test]
    fn str_dictionary_fires_on_low_cardinality() {
        // 3 distinct over 3000 rows = ratio 0.001 → dictionary.
        let services: Vec<&str> = (0..3000).map(|i| ["api", "web", "auth"][i % 3]).collect();
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
    fn selected_string_decode_validates_all_rows_and_materializes_only_selection() {
        for values in [
            (0..3000)
                .map(|index| ["api", "web", "auth"][index % 3].to_owned())
                .collect::<Vec<_>>(),
            (0..500)
                .map(|index| format!("request {index} failed"))
                .collect::<Vec<_>>(),
        ] {
            let encoded = encode_str(values.iter().map(String::as_str), values.len(), LVL)
                .unwrap()
                .to_bytes()
                .unwrap();
            let selected = [0, 7, values.len() / 2, values.len() - 1];
            let decoded = decode_str_selected(&encoded, values.len(), &selected).unwrap();
            assert_eq!(
                decoded,
                selected
                    .iter()
                    .map(|index| values[*index].clone())
                    .collect::<Vec<_>>()
            );
            assert!(decode_str_selected(&encoded, values.len(), &[7, 7]).is_err());
            assert!(decode_str_selected(&encoded, values.len(), &[values.len()]).is_err());
        }
    }

    #[test]
    fn str_count_mismatch_is_an_error() {
        assert!(encode_str(["a", "b"].into_iter(), 3, LVL).is_err());
    }

    #[test]
    fn string_decoders_reject_unrepresentable_counts_before_allocation() {
        let empty_zstd = zstd_compress(&[], LVL).unwrap();

        let mut dict_payload = Vec::new();
        dict_payload.extend_from_slice(&u32::MAX.to_le_bytes());
        dict_payload.extend_from_slice(&(empty_zstd.len() as u32).to_le_bytes());
        dict_payload.extend_from_slice(&empty_zstd);
        dict_payload.extend_from_slice(&empty_zstd);
        let dict_frame = ColumnEnc {
            encoding: ENC_STR_DICT,
            payload: dict_payload,
        }
        .to_bytes();

        for error in [
            decode_str(&dict_frame, 0).unwrap_err(),
            decode_str_selected(&dict_frame, 0, &[]).unwrap_err(),
        ] {
            assert!(error.contains("string dictionary"), "{error}");
            assert!(
                error.contains("count overflows minimum encoded length")
                    || error.contains("4294967295 entries require at least"),
                "{error}"
            );
        }

        let concat_frame = ColumnEnc {
            encoding: ENC_STR_ZSTD,
            payload: empty_zstd,
        }
        .to_bytes();
        let error = decode_str(&concat_frame, u32::MAX as usize).unwrap_err();
        assert!(error.contains("string column"), "{error}");
        assert!(
            error.contains("count overflows minimum encoded length")
                || error.contains("4294967295 entries require at least"),
            "{error}"
        );
    }

    fn rt_u8(values: &[u8]) {
        let enc = encode_u8(values, LVL).unwrap();
        let back = decode_u8(&enc.to_bytes().unwrap(), values.len()).unwrap();
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
            (
                (0..25u8)
                    .flat_map(|i| [i, i ^ 0x5A, 0, 255, i, 1, 2, 3])
                    .collect::<Vec<u8>>(),
                8,
            ),
        ] {
            let n = data.len() / width;
            let enc = encode_fixed_bytes(&data, width, LVL).unwrap();
            let back = decode_fixed_bytes(&enc.to_bytes().unwrap(), n, width).unwrap();
            assert_eq!(back, data);
        }
    }

    #[test]
    fn fixed_bytes_rejects_misaligned() {
        assert!(encode_fixed_bytes(&[1, 2, 3], 2, LVL).is_err());
        assert!(encode_fixed_bytes(&[1, 2, 3], 0, LVL).is_err());
    }

    #[test]
    fn bitmap_round_trips() {
        for n in [0usize, 1, 7, 8, 9, 63, 64, 65, 8192] {
            // Deterministic mixed pattern (not all-true/all-false).
            let bits: Vec<bool> = (0..n).map(|i| (i * 7 + 3) % 5 < 2).collect();
            let enc = encode_bitmap(&bits);
            assert_eq!(enc.len(), bitmap_len(n));
            assert_eq!(decode_bitmap(&enc, n).unwrap(), bits, "n = {n}");
        }
        assert_eq!(encode_bitmap(&[]), Vec::<u8>::new());
        assert_eq!(decode_bitmap(&[], 0).unwrap(), Vec::<bool>::new());
    }

    #[test]
    fn bitmap_rejects_bad_lengths_and_pad_bits() {
        assert!(decode_bitmap(&[0, 0], 17).is_err()); // too short for 17 bits
        assert!(decode_bitmap(&[0], 9).is_err()); // too short
        assert!(decode_bitmap(&[0, 0, 0], 9).is_err()); // too long
        assert!(decode_bitmap(&[0xFF], 3).is_err()); // pad bits set
        assert!(decode_bitmap(&[0x07], 3).is_ok()); // exactly the 3 valid bits
    }

    #[test]
    fn reader_framed_column_walks_back_to_back_frames() {
        // Two encode_str frames packed with no outer length table —
        // the shredded-metadata consumption pattern.
        let a = encode_str(["x", "y"], 2, LVL).unwrap().to_bytes().unwrap();
        let b = encode_str(["z"], 1, LVL).unwrap().to_bytes().unwrap();
        let mut buf = a.clone();
        buf.extend_from_slice(&b);
        let mut r = Reader::new(&buf);
        let fa = r.framed_column("frame a").unwrap();
        let fb = r.framed_column("frame b").unwrap();
        assert_eq!(fa, &a[..]);
        assert_eq!(fb, &b[..]);
        assert_eq!(r.remaining(), 0);
        assert_eq!(decode_str(fa, 2).unwrap(), vec!["x", "y"]);
        assert_eq!(decode_str(fb, 1).unwrap(), vec!["z"]);
        // Truncated frame: error, not panic.
        let mut rt = Reader::new(&buf[..buf.len() - 1]);
        rt.framed_column("frame a").unwrap();
        assert!(rt.framed_column("frame b").is_err());
    }

    #[test]
    fn corrupt_frames_error_not_panic() {
        // Truncated frame, unknown encoding id, wrong count, garbage
        // payload: all must be Err with a field name, never a panic.
        assert!(decode_i64(&[], 0).is_err());
        assert!(decode_i64(&[99, 0, 0, 0, 0], 1).is_err()); // unknown id
        let enc = encode_i64(&[1, 2, 3], LVL).unwrap().to_bytes().unwrap();
        assert!(decode_i64(&enc, 4).is_err()); // wrong n
        assert!(decode_i64(&enc[..enc.len() - 1], 3).is_err()); // truncated
        let mut garbage = enc.clone();
        let last = garbage.len() - 1;
        garbage[last] ^= 0xFF;
        let _ = decode_i64(&garbage, 3); // any Result is fine; no panic
        assert!(decode_str(&[ENC_STR_DICT, 1, 0, 0, 0, 7], 1).is_err());
        assert!(decode_u8(&[ENC_U8_RLE, 5, 0, 0, 0, 255, 255, 255, 255, 7], 3).is_err());
        // run > n
    }

    #[test]
    fn width_prefixes_reject_over_wide_lengths() {
        // Regression tests for the truncating-cast class (issue #40):
        // over-wide lengths must be a clean Err, never a wrapped prefix.
        // None of these allocate the over-wide size.
        assert!(u32_len(u32::MAX as usize, "x").is_ok());
        assert!(u32_len(u32::MAX as usize + 1, "x").is_err());
        assert!(u32_len(usize::MAX, "x").is_err());
        // A payload that cannot be framed is an error, not a wrap.
        let big = ColumnEnc {
            encoding: ENC_FIXED_ZSTD,
            payload: vec![0u8; 16],
        };
        assert!(big.to_bytes().is_ok());
        // decode_fixed_bytes with an overflowing n * width: Err, not a
        // debug panic (overflow) or release wrap (wrong comparison).
        let enc = encode_fixed_bytes(&[7u8; 16], 16, LVL)
            .unwrap()
            .to_bytes()
            .unwrap();
        assert!(decode_fixed_bytes(&enc, usize::MAX, 16).is_err());
        assert!(decode_fixed_bytes(&enc, usize::MAX, usize::MAX).is_err());
        // ... while a merely wrong (non-overflowing) count is still the
        // plain length-mismatch error.
        assert!(decode_fixed_bytes(&enc, 2, 16).is_err());
    }

    #[test]
    fn decompression_caps_reject_bombs() {
        // Regression tests for the decompression-bomb class (issue #41).
        // Every case below must be a clean Err with no giant allocation;
        // the old unbounded paths would allocate the full output (or, for
        // pco, trust the payload's own element count for sizing).

        // Exact cap: 25 ids (200 bytes) decoded with room for 3 → the
        // bulk bound fails at 24 bytes, before any 200-byte material.
        let data: Vec<u8> = (0..25u8)
            .flat_map(|i| [i, i ^ 0x5A, 0, 255, i, 1, 2, 3])
            .collect();
        let enc = encode_fixed_bytes(&data, 8, LVL)
            .unwrap()
            .to_bytes()
            .unwrap();
        let err = decode_fixed_bytes(&enc, 3, 8).expect_err("over-cap content must fail");
        assert!(err.contains("failed"), "unexpected error: {err}");

        // Absolute cap: well-formed strings totaling past 64 MiB.
        // Framed by hand (not via encode_str) so no strategy heuristic
        // is involved: 17M one-byte strings, ~85 MB raw.
        let mut raw = Vec::new();
        for _ in 0..17_000_000 {
            raw.extend_from_slice(&1u32.to_le_bytes());
            raw.push(b'a');
        }
        let payload = zstd_compress(&raw, LVL).unwrap();
        drop(raw);
        let frame = ColumnEnc {
            encoding: ENC_STR_ZSTD,
            payload,
        }
        .to_bytes()
        .unwrap();
        let err = decode_str(&frame, 17_000_000).expect_err("over-cap column must fail");
        assert!(err.contains("failed"), "unexpected error: {err}");

        // Entry-count ceiling: the column layer stays agnostic, but the
        // container helper pins the contract.
        assert!(check_entry_count(MAX_BLOCK_ENTRIES, "t").is_ok());
        assert!(check_entry_count(MAX_BLOCK_ENTRIES + 1, "t").is_err());

        // Bounded pco: exact round-trip, short/long/garbage payloads.
        let nums: Vec<i64> = (0..50).collect();
        let comp = pco::standalone::simple_compress(&nums, &pco::ChunkConfig::default()).unwrap();
        assert_eq!(pco_decompress_capped::<i64>(&comp, 50, "t").unwrap(), nums);
        assert!(pco_decompress_capped::<i64>(&comp, 49, "t").is_err());
        assert!(pco_decompress_capped::<i64>(&comp, 51, "t").is_err());
        assert!(pco_decompress_capped::<i64>(&[0u8; 8], 4, "t").is_err());
        assert!(pco_decompress_capped::<i64>(&[], 0, "t").is_err());
    }
}
