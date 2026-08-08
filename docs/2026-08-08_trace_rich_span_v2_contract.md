# Rich-span v2 public fidelity contract

Status: implemented by Session 5. Validation and measured verdict are in
[`2026-08-08_trace_rich_span_v2.md`](2026-08-08_trace_rich_span_v2.md).

## Purpose

The existing `rich-span-v1` (`0x02`) path preserves span attributes, status
descriptions, events, resource attributes, and instrumentation scope, but it
drops OTLP links, trace state and flags, resource/scope schema URLs, and all
dropped-value counts. Rich-span v2 adds those values at the public
SQLite/libSQL boundary. They are ordinary columns and an additive public
batch revision, never a server-private payload or shadow-table contract.

## Additive `timeless_traces` columns

The following visible columns are appended after `instrumentation_scope` and
before the hidden command column:

| Column | SQLite type | Default for legacy data and omitted inserts |
|---|---|---|
| `links` | TEXT | `[]` |
| `trace_state` | TEXT | empty string |
| `trace_flags` | INTEGER | `0` |
| `dropped_attributes_count` | INTEGER | `0` |
| `dropped_events_count` | INTEGER | `0` |
| `dropped_links_count` | INTEGER | `0` |
| `resource_schema_url` | TEXT | empty string |
| `scope_schema_url` | TEXT | empty string |
| `resource_dropped_attributes_count` | INTEGER | `0` |
| `scope_dropped_attributes_count` | INTEGER | `0` |

The five count columns and `trace_flags` are lossless OTLP `uint32` values.
SQL inserts must reject negative values, non-integers, and values greater than
`4294967295`. `links` is canonical typed JSON and must be an array.

Each stored link is an object with `trace_id` (32 lowercase hexadecimal
characters), `span_id` (16 lowercase hexadecimal characters), `trace_state`
(string), `attributes` (typed JSON object), `dropped_attributes_count`
(`uint32`), and `flags` (`uint32`). Event objects retain their existing
`name`, `timestamp`, and `attributes` members and add
`dropped_attributes_count`. Instrumentation-scope attributes remain inside
the public `instrumentation_scope` object.

## `rich-span-v2` batch (`0x03`)

The common header and complete `rich-span-v1` body are byte-for-byte the
prefix of v2. After the final `instrumentation_scope` value, v2 appends these
columns in order:

1. `links[n]`: `u32 byte_length`, then UTF-8 JSON array
2. `trace_state[n]`: `u32 byte_length`, then UTF-8 text
3. `trace_flags[n]`: little-endian `u32`
4. `dropped_attributes_count[n]`: little-endian `u32`
5. `dropped_events_count[n]`: little-endian `u32`
6. `dropped_links_count[n]`: little-endian `u32`
7. `resource_schema_url[n]`: `u32 byte_length`, then UTF-8 text
8. `scope_schema_url[n]`: `u32 byte_length`, then UTF-8 text
9. `resource_dropped_attributes_count[n]`: little-endian `u32`
10. `scope_dropped_attributes_count[n]`: little-endian `u32`

The header remains `version:u8`, `flags:u8=0`, `reserved:u16=0`, followed by
`n_spans:u32`. Empty link JSON means `[]`. Every malformed value rejects the
entire batch before any span is buffered. `span-v0` (`0x01`) and
`rich-span-v1` (`0x02`) remain accepted and receive the defaults above.
Unknown versions remain explicit errors.

## Storage and compatibility

The on-disk codec revision is additive and backward-readable. New code reads
generation-1 and generation-2 blocks with the documented defaults; new
blocks retain all v2 values. Optimize may rewrite a mixed old/new database
without changing either legacy defaults or v2 values. No public server reads
private shadow tables, and no legacy block is rewritten merely to add empty
fields.

`timeless_capabilities()` advertises `rich-span-v2` separately. A traces
server requiring v2 must fail its capability handshake against an older
extension instead of silently falling back to v1.

## Signal API projection

OTLP/HTTP JSON and OTLP/protobuf ingest map every listed field into v2.
Direct SQL exposes the same values. Jaeger responses project links as
`FOLLOWS_FROM` references in addition to the parent `CHILD_OF` reference;
Jaeger has no native representation for trace state, flags, schema URLs, or
dropped-value counts, so those remain available through SQL and native OTLP
surfaces without invented Jaeger tags.

## Required evidence

Session 5 is complete only when tests cover direct row inserts, all three
batch versions, mixed blocks, JSON and protobuf OTLP, Jaeger link projection,
flush, optimize, transaction rollback, backup/reopen, malformed all-or-
nothing batches, and legacy database reopen. The final report must state
write throughput, representative narrow and full-fidelity read latency,
logical/physical storage, and RSS high-water mark relative to the Session 4
baseline.
