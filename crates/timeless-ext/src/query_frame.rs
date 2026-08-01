//! Versioned public result envelopes for high-cardinality metrics queries.
//!
//! These frames are query transport only. They never appear in shadow tables
//! and are not replication-visible. Row-oriented TVFs remain the ordinary SQL
//! interface; frames let embedded and remote hosts cross SQLite once.

use timeless_core::{AggFn, AggregateSummary};

pub const AGGREGATE_FRAME_MAGIC: &[u8; 4] = b"TAF1";
pub const LATEST_FRAME_MAGIC: &[u8; 4] = b"TLF1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum AggregateFrameKind {
    Avg = 0,
    Sum = 1,
    Min = 2,
    Max = 3,
    Count = 4,
}

impl AggregateFrameKind {
    fn from_byte(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(Self::Avg),
            1 => Ok(Self::Sum),
            2 => Ok(Self::Min),
            3 => Ok(Self::Max),
            4 => Ok(Self::Count),
            other => Err(format!("TAF1: unknown aggregate kind {other}")),
        }
    }

    fn agg(self) -> AggFn {
        match self {
            Self::Avg => AggFn::Avg,
            Self::Sum => AggFn::Sum,
            Self::Min => AggFn::Min,
            Self::Max => AggFn::Max,
            Self::Count => AggFn::Count,
        }
    }
}

impl From<AggFn> for AggregateFrameKind {
    fn from(value: AggFn) -> Self {
        match value {
            AggFn::Avg => Self::Avg,
            AggFn::Sum => Self::Sum,
            AggFn::Min => Self::Min,
            AggFn::Max => Self::Max,
            AggFn::Count => Self::Count,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AggregateFrameValue {
    Null,
    Real(f64),
    Integer(i64),
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateFrame {
    pub kind: AggregateFrameKind,
    pub rows: Vec<(i64, AggregateFrameValue)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LatestFrame {
    pub rows: Vec<(i64, i64, Option<f64>)>,
}

pub(crate) fn encode_aggregate_frame(
    batch: &[(i64, Option<AggregateSummary>)],
    aggregate: AggFn,
) -> Result<Vec<u8>, String> {
    let rows: Vec<_> = batch
        .iter()
        .filter_map(|(series_id, summary)| summary.map(|summary| (*series_id, summary)))
        .collect();
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let count: u32 = rows
        .len()
        .try_into()
        .map_err(|_| "TAF1: too many series".to_string())?;
    let bitmap_len = bitmap_len(rows.len())?;
    let columns_len = rows
        .len()
        .checked_mul(16)
        .ok_or_else(|| "TAF1: column size overflow".to_string())?;
    let capacity = 12usize
        .checked_add(columns_len)
        .and_then(|size| size.checked_add(bitmap_len))
        .ok_or_else(|| "TAF1: frame size overflow".to_string())?;

    let kind = AggregateFrameKind::from(aggregate);
    let mut validity = vec![0u8; bitmap_len];
    let mut words = Vec::with_capacity(rows.len() * 8);
    for (index, (_, summary)) in rows.iter().enumerate() {
        if kind == AggregateFrameKind::Count {
            let count = i64::try_from(summary.count())
                .map_err(|_| "TAF1: count exceeds SQLite INTEGER range".to_string())?;
            set_valid(&mut validity, index);
            words.extend_from_slice(&count.to_le_bytes());
        } else {
            let value = summary.value(kind.agg());
            if value.is_nan() {
                words.extend_from_slice(&0u64.to_le_bytes());
            } else {
                set_valid(&mut validity, index);
                words.extend_from_slice(&value.to_bits().to_le_bytes());
            }
        }
    }

    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(AGGREGATE_FRAME_MAGIC);
    out.push(kind as u8);
    out.push(0); // flags
    out.extend_from_slice(&0u16.to_le_bytes()); // reserved
    out.extend_from_slice(&count.to_le_bytes());
    for (series_id, _) in &rows {
        out.extend_from_slice(&series_id.to_le_bytes());
    }
    out.extend_from_slice(&validity);
    out.extend_from_slice(&words);
    debug_assert_eq!(out.len(), capacity);
    Ok(out)
}

pub(crate) fn encode_latest_frame(batch: &[(i64, Option<(i64, f64)>)]) -> Result<Vec<u8>, String> {
    let rows: Vec<_> = batch
        .iter()
        .filter_map(|(series_id, point)| point.map(|(ts, value)| (*series_id, ts, value)))
        .collect();
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let count: u32 = rows
        .len()
        .try_into()
        .map_err(|_| "TLF1: too many series".to_string())?;
    let bitmap_len = bitmap_len(rows.len())?;
    let columns_len = rows
        .len()
        .checked_mul(24)
        .ok_or_else(|| "TLF1: column size overflow".to_string())?;
    let capacity = 8usize
        .checked_add(columns_len)
        .and_then(|size| size.checked_add(bitmap_len))
        .ok_or_else(|| "TLF1: frame size overflow".to_string())?;
    let mut validity = vec![0u8; bitmap_len];
    let mut words = Vec::with_capacity(rows.len() * 8);
    for (index, (_, _, value)) in rows.iter().enumerate() {
        if value.is_nan() {
            words.extend_from_slice(&0u64.to_le_bytes());
        } else {
            set_valid(&mut validity, index);
            words.extend_from_slice(&value.to_bits().to_le_bytes());
        }
    }

    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(LATEST_FRAME_MAGIC);
    out.extend_from_slice(&count.to_le_bytes());
    for (series_id, _, _) in &rows {
        out.extend_from_slice(&series_id.to_le_bytes());
    }
    for (_, timestamp, _) in &rows {
        out.extend_from_slice(&timestamp.to_le_bytes());
    }
    out.extend_from_slice(&validity);
    out.extend_from_slice(&words);
    debug_assert_eq!(out.len(), capacity);
    Ok(out)
}

pub fn decode_aggregate_frame(bytes: &[u8]) -> Result<AggregateFrame, String> {
    if bytes.len() < 12 {
        return Err("TAF1: truncated header".into());
    }
    if &bytes[..4] != AGGREGATE_FRAME_MAGIC {
        return Err("TAF1: unknown magic/version".into());
    }
    let kind = AggregateFrameKind::from_byte(bytes[4])?;
    if bytes[5] != 0 {
        return Err(format!("TAF1: unknown flags 0x{:02x}", bytes[5]));
    }
    if bytes[6..8] != [0, 0] {
        return Err("TAF1: reserved bits must be zero".into());
    }
    let count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let bitmap_len = bitmap_len(count)?;
    let columns_len = count
        .checked_mul(16)
        .ok_or_else(|| "TAF1: column size overflow".to_string())?;
    let expected = 12usize
        .checked_add(columns_len)
        .and_then(|size| size.checked_add(bitmap_len))
        .ok_or_else(|| "TAF1: frame size overflow".to_string())?;
    if bytes.len() != expected {
        return Err(format!(
            "TAF1: {} bytes, expected {expected} for {count} series",
            bytes.len()
        ));
    }

    let ids_start = 12;
    let bitmap_start = ids_start + count * 8;
    let words_start = bitmap_start + bitmap_len;
    validate_bitmap(&bytes[bitmap_start..words_start], count, "TAF1")?;
    let mut rows = Vec::with_capacity(count);
    for index in 0..count {
        let series_id = i64_at(bytes, ids_start + index * 8);
        let valid = bit(&bytes[bitmap_start..words_start], index);
        let word = u64_at(bytes, words_start + index * 8);
        let value = if !valid {
            if word != 0 {
                return Err(format!("TAF1: invalid value {index} has a nonzero word"));
            }
            if kind == AggregateFrameKind::Count {
                return Err(format!("TAF1: count value {index} must not be NULL"));
            }
            AggregateFrameValue::Null
        } else if kind == AggregateFrameKind::Count {
            if word > i64::MAX as u64 {
                return Err(format!("TAF1: count value {index} exceeds SQLite INTEGER"));
            }
            AggregateFrameValue::Integer(word as i64)
        } else {
            let value = f64::from_bits(word);
            if value.is_nan() {
                return Err(format!("TAF1: valid value {index} must not be NaN"));
            }
            AggregateFrameValue::Real(value)
        };
        rows.push((series_id, value));
    }
    Ok(AggregateFrame { kind, rows })
}

pub fn decode_latest_frame(bytes: &[u8]) -> Result<LatestFrame, String> {
    if bytes.len() < 8 {
        return Err("TLF1: truncated header".into());
    }
    if &bytes[..4] != LATEST_FRAME_MAGIC {
        return Err("TLF1: unknown magic/version".into());
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let bitmap_len = bitmap_len(count)?;
    let columns_len = count
        .checked_mul(24)
        .ok_or_else(|| "TLF1: column size overflow".to_string())?;
    let expected = 8usize
        .checked_add(columns_len)
        .and_then(|size| size.checked_add(bitmap_len))
        .ok_or_else(|| "TLF1: frame size overflow".to_string())?;
    if bytes.len() != expected {
        return Err(format!(
            "TLF1: {} bytes, expected {expected} for {count} series",
            bytes.len()
        ));
    }

    let ids_start = 8;
    let timestamps_start = ids_start + count * 8;
    let bitmap_start = timestamps_start + count * 8;
    let words_start = bitmap_start + bitmap_len;
    validate_bitmap(&bytes[bitmap_start..words_start], count, "TLF1")?;
    let mut rows = Vec::with_capacity(count);
    for index in 0..count {
        let series_id = i64_at(bytes, ids_start + index * 8);
        let timestamp = i64_at(bytes, timestamps_start + index * 8);
        let valid = bit(&bytes[bitmap_start..words_start], index);
        let word = u64_at(bytes, words_start + index * 8);
        let value = if valid {
            let value = f64::from_bits(word);
            if value.is_nan() {
                return Err(format!("TLF1: valid value {index} must not be NaN"));
            }
            Some(value)
        } else {
            if word != 0 {
                return Err(format!("TLF1: invalid value {index} has a nonzero word"));
            }
            None
        };
        rows.push((series_id, timestamp, value));
    }
    Ok(LatestFrame { rows })
}

fn bitmap_len(count: usize) -> Result<usize, String> {
    count
        .checked_add(7)
        .map(|value| value / 8)
        .ok_or_else(|| "frame bitmap size overflow".to_string())
}

fn set_valid(bitmap: &mut [u8], index: usize) {
    bitmap[index / 8] |= 1 << (index % 8);
}

fn bit(bitmap: &[u8], index: usize) -> bool {
    bitmap[index / 8] & (1 << (index % 8)) != 0
}

fn validate_bitmap(bitmap: &[u8], count: usize, name: &str) -> Result<(), String> {
    let used_bits = count & 7;
    if used_bits != 0 && bitmap.last().copied().unwrap_or(0) & !((1 << used_bits) - 1) != 0 {
        return Err(format!("{name}: nonzero bitmap padding bits"));
    }
    Ok(())
}

fn i64_at(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_decoder_pins_exact_layout_and_nulls() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"TAF1");
        bytes.extend_from_slice(&[AggregateFrameKind::Avg as u8, 0, 0, 0]);
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&7i64.to_le_bytes());
        bytes.extend_from_slice(&9i64.to_le_bytes());
        bytes.push(0b0000_0001);
        bytes.extend_from_slice(&1.5f64.to_bits().to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        let frame = decode_aggregate_frame(&bytes).unwrap();
        assert_eq!(frame.kind, AggregateFrameKind::Avg);
        assert_eq!(
            frame.rows,
            vec![
                (7, AggregateFrameValue::Real(1.5)),
                (9, AggregateFrameValue::Null)
            ]
        );
        assert_eq!(bytes.len(), 45);
    }

    #[test]
    fn latest_decoder_pins_exact_layout_and_nulls() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"TLF1");
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&7i64.to_le_bytes());
        bytes.extend_from_slice(&9i64.to_le_bytes());
        bytes.extend_from_slice(&100i64.to_le_bytes());
        bytes.extend_from_slice(&200i64.to_le_bytes());
        bytes.push(0b0000_0001);
        bytes.extend_from_slice(&1.5f64.to_bits().to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        let frame = decode_latest_frame(&bytes).unwrap();
        assert_eq!(frame.rows, vec![(7, 100, Some(1.5)), (9, 200, None)]);
        assert_eq!(bytes.len(), 57);
    }

    #[test]
    fn decoders_reject_versions_lengths_flags_padding_and_noncanonical_nulls() {
        assert!(decode_aggregate_frame(b"TAF2").is_err());
        assert!(decode_latest_frame(b"TLF2").is_err());

        let mut aggregate = Vec::new();
        aggregate.extend_from_slice(b"TAF1");
        aggregate.extend_from_slice(&[0, 1, 0, 0]);
        aggregate.extend_from_slice(&0u32.to_le_bytes());
        assert!(decode_aggregate_frame(&aggregate).is_err());
        aggregate[5] = 0;
        aggregate[8..12].copy_from_slice(&1u32.to_le_bytes());
        assert!(decode_aggregate_frame(&aggregate).is_err());

        let mut latest = Vec::new();
        latest.extend_from_slice(b"TLF1");
        latest.extend_from_slice(&1u32.to_le_bytes());
        latest.extend_from_slice(&7i64.to_le_bytes());
        latest.extend_from_slice(&100i64.to_le_bytes());
        latest.push(0b1000_0001); // nonzero padding
        latest.extend_from_slice(&1.5f64.to_bits().to_le_bytes());
        assert!(decode_latest_frame(&latest).is_err());
        latest[24] = 0;
        assert!(decode_latest_frame(&latest).is_err()); // nonzero word for NULL
    }
}
