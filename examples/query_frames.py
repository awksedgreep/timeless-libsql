"""Strict dependency-free decoders for timeless-libsql TAF1/TLF1 frames.

Copy these functions into a Python SQLite/libSQL client, or use them as a
reference for another host language. Unknown versions and non-canonical frames
raise ValueError; callers must not guess a future layout.
"""

from __future__ import annotations

import math
import struct
from enum import IntEnum


class AggregateKind(IntEnum):
    AVG = 0
    SUM = 1
    MIN = 2
    MAX = 3
    COUNT = 4


def _bitmap_ok(bitmap: bytes, count: int, name: str) -> None:
    if count % 8 and bitmap[-1] & ~((1 << (count % 8)) - 1):
        raise ValueError(f"{name}: nonzero bitmap padding bits")


def decode_aggregate_frame(
    blob: bytes,
) -> tuple[AggregateKind, list[tuple[int, float | int | None]]]:
    """Decode TAF1 into (aggregate kind, [(series_id, value), ...])."""

    if len(blob) < 12 or blob[:4] != b"TAF1":
        raise ValueError("TAF1: truncated or unknown magic/version")
    kind_byte, flags, reserved, count = struct.unpack_from("<BBHI", blob, 4)
    try:
        kind = AggregateKind(kind_byte)
    except ValueError as error:
        raise ValueError(f"TAF1: unknown aggregate kind {kind_byte}") from error
    if flags or reserved:
        raise ValueError("TAF1: flags and reserved bits must be zero")
    bitmap_len = (count + 7) // 8
    expected = 12 + count * 16 + bitmap_len
    if len(blob) != expected:
        raise ValueError(f"TAF1: {len(blob)} bytes, expected {expected}")

    ids = struct.unpack_from(f"<{count}q", blob, 12)
    bitmap_at = 12 + count * 8
    bitmap = blob[bitmap_at : bitmap_at + bitmap_len]
    _bitmap_ok(bitmap, count, "TAF1")
    words = struct.unpack_from(f"<{count}Q", blob, bitmap_at + bitmap_len)
    rows: list[tuple[int, float | int | None]] = []
    for index, (series_id, word) in enumerate(zip(ids, words)):
        valid = bool(bitmap[index // 8] & (1 << (index % 8)))
        if not valid:
            if word or kind == AggregateKind.COUNT:
                raise ValueError(f"TAF1: non-canonical NULL at value {index}")
            value: float | int | None = None
        elif kind == AggregateKind.COUNT:
            if word > (1 << 63) - 1:
                raise ValueError(f"TAF1: count {index} exceeds SQLite INTEGER")
            value = word
        else:
            value = struct.unpack("<d", struct.pack("<Q", word))[0]
            if math.isnan(value):
                raise ValueError(f"TAF1: valid value {index} must not be NaN")
        rows.append((series_id, value))
    return kind, rows


def decode_latest_frame(blob: bytes) -> list[tuple[int, int, float | None]]:
    """Decode TLF1 into [(series_id, timestamp, value), ...]."""

    if len(blob) < 8 or blob[:4] != b"TLF1":
        raise ValueError("TLF1: truncated or unknown magic/version")
    (count,) = struct.unpack_from("<I", blob, 4)
    bitmap_len = (count + 7) // 8
    expected = 8 + count * 24 + bitmap_len
    if len(blob) != expected:
        raise ValueError(f"TLF1: {len(blob)} bytes, expected {expected}")

    ids = struct.unpack_from(f"<{count}q", blob, 8)
    timestamps_at = 8 + count * 8
    timestamps = struct.unpack_from(f"<{count}q", blob, timestamps_at)
    bitmap_at = timestamps_at + count * 8
    bitmap = blob[bitmap_at : bitmap_at + bitmap_len]
    _bitmap_ok(bitmap, count, "TLF1")
    words = struct.unpack_from(f"<{count}Q", blob, bitmap_at + bitmap_len)
    rows: list[tuple[int, int, float | None]] = []
    for index, (series_id, timestamp, word) in enumerate(zip(ids, timestamps, words)):
        valid = bool(bitmap[index // 8] & (1 << (index % 8)))
        if valid:
            value = struct.unpack("<d", struct.pack("<Q", word))[0]
            if math.isnan(value):
                raise ValueError(f"TLF1: valid value {index} must not be NaN")
        else:
            if word:
                raise ValueError(f"TLF1: non-canonical NULL at value {index}")
            value = None
        rows.append((series_id, timestamp, value))
    return rows
