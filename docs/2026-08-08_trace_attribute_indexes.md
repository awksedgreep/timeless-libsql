# Bounded trace attribute equality

Date: 2026-08-08

Session: 7 of the [trace query enhancement plan](2026-08-07_trace_query_enhancement_plan.md)
Measured source: `5b96413fca408dd77b8fc46c12c2cead963abad1`

## Result

Session 7 ships an opt-in direct SQLite/libSQL primitive for exact typed
scalar equality on at most eight configured trace fields. It uses one fixed
4,096-byte, four-hash negative filter per configured field and persisted
block, then rechecks every surviving span exactly. The implementation does
not parse TraceQL, infer trace completeness, index arbitrary fields, or claim
event/link, comparison, regex, structural, or trace-quantifier semantics.

This is deliberately opt-in. High-cardinality values can avoid nearly every
block decode; a low-cardinality value present in every block cannot prune and
pays the exact Rust JSON recheck plus the fixed write/storage cost. The
measured evidence supports shipping the primitive for selective identifiers,
not enabling it by default.

The complete public contract and JSON1 control are in the
[SQL API reference](SQL_API_REFERENCE.md#bounded-typed-attribute-equality).
The [TraceQL prerequisite matrix](2026-08-08_traceql_prerequisite_matrix.md)
keeps language and storage ownership separate.

## Public contract

Tables declare an immutable JSON allowlist at creation:

```sql
CREATE VIRTUAL TABLE traces USING timeless_traces(
  attribute_indexes='[
    {"scope":"span","path":"/http.method"},
    {"scope":"resource","path":"/deployment.environment"}
  ]'
);
```

Each field has a `span`, `resource`, or `scope` scope and a non-empty RFC 6901
JSON Pointer of at most 256 UTF-8 bytes. Query `attribute_filter` with exactly
one configured field and one JSON scalar. Missing, null, empty text, strings,
integers, reals, and booleans remain distinct. Stored arrays/objects do not
match; composite operands, unknown keys/scopes, malformed JSON, and
unconfigured paths fail explicitly.

The extension processes Bloom metadata in fixed 256-block chunks. Missing
metadata always takes the conservative exact-decode path. Bad versions,
lengths, and checksums fail closed. Buffer rows are exact before a Bloom row
exists. Flush, automatic 8,192-span flush, optimize, retention, rollback,
savepoints, crash recovery, and reopen keep the metadata atomic with block
lifecycle. No public code reads a private shadow table.

## Evidence

The current-build A/B pairs use the established deterministic rich-span-v2
fixture: sixteen public 8,192-span batches, 131,072 durable spans, sixteen
one-hour boxes, 48 status-partitioned optimized blocks, three warmups, and
twenty measured queries. The unindexed and two-field indexed fixtures were
built in separate fresh processes at the same commit.

- [unindexed primary](evidence/2026-08-08_trace_attributes_unindexed_control.json),
  SHA-256 `43d9262e7a76081974791f55a9848fcc6bc52ee460dbf648f54a82501ef3fe03`
- [unindexed repeat](evidence/2026-08-08_trace_attributes_unindexed_control_repeat.json),
  SHA-256 `a2be6448265c6f47324831b948b49ef65aaf960888c6c5acbad142cd238cd57d`
- [indexed primary](evidence/2026-08-08_trace_attributes.json), SHA-256
  `2f2fe363766b9446dba315a170b46314d6f963c02c2b063984fc0ee68392acca`
- [indexed repeat](evidence/2026-08-08_trace_attributes_repeat.json), SHA-256
  `e5993bce3a3de244f65f18f79373899c32a6c9b6531b42fcafbbdb4285e5403e`
- [pre-implementation JSON1 baseline](evidence/2026-08-08_trace_attributes_baseline.json),
  SHA-256 `131075e4fdaff691df91faae9b676d7f94a4ea1b89c8a38a4118fb55745a3afc`

Every control and candidate returns identical cardinality and result bytes.
The high-cardinality target occurs in one status-pure block. The
low-cardinality Boolean is present in every span and therefore every block.

### Query p50/p95

Each cell is primary / repeat in milliseconds. Percentage and speedup use
p95 within the same indexed fixture, avoiding cross-build timing attribution.

| shape | public JSON1 control | configured filter | p95 verdict |
|---|---:|---:|---:|
| high-cardinality, one box | 4.460/4.564 / 5.125/5.241 | 1.480/1.526 / 1.663/1.692 | 66.6–67.7% lower; 3.0–3.1× faster |
| high-cardinality, all boxes | 70.850/72.759 / 80.410/83.821 | 1.796/1.887 / 2.018/2.066 | 97.4–97.5% lower; 38.6–40.6× faster |
| low-cardinality, one box | 3.391/3.455 / 3.955/4.151 | 5.021/5.100 / 4.982/5.647 | 36.0–47.6% higher |
| low-cardinality, all boxes | 55.368/69.655 / 64.386/66.688 | 94.919/95.971 / 79.277/94.316 | 37.8–41.4% higher |

### Public extension work per query

The work is identical in both indexed runs.

| shape | JSON1 candidate blocks / decoded spans | configured candidate blocks / decoded spans | returned spans |
|---|---:|---:|---:|
| high-cardinality, one box | 3 / 8,192 | 1 / 2,731 | 1 |
| high-cardinality, all boxes | 48 / 131,072 | 1 / 2,731 | 1 |
| low-cardinality, one box | 3 / 8,192 | 3 / 8,192 | 8,192 |
| low-cardinality, all boxes | 48 / 131,072 | 48 / 131,072 | 131,072 |

The measured high-cardinality values produced no Bloom false-positive block.
False positives remain a permitted performance outcome; exact row recheck
prevents a result error.

## Write, WAL, storage, and memory cost

Two configured fields create 96 Bloom rows after optimize, exactly
393,216 logical Bloom bytes. Compared with an unindexed table at the same
commit:

| measurement | unindexed primary / repeat | indexed primary / repeat | indexed cost |
|---|---:|---:|---:|
| durable ingest | 189,282 / 180,563 spans/s | 157,336 / 162,211 spans/s | 10.2–16.9% lower throughput |
| optimize | 458.802 / 447.727 ms | 599.919 / 563.279 ms | 25.8–30.8% higher |
| checkpointed database | 88,444,928 bytes | 89,341,952 bytes | +897,024 bytes, 1.01% |
| physical index allocation | 2,527,232 bytes | 2,981,888 bytes | +454,656 bytes, 18.0% |
| WAL file before final checkpoint | 88,954,952 bytes | 89,861,352 bytes | +906,400 bytes, 1.02% |
| builder-process HWM | 48,592 / 47,788 KiB | 46,944 / 46,772 KiB | no increase |
| direct-SQL process HWM | 38,400 / 37,244 KiB | 36,300 / 37,448 KiB | -5.5% to +0.5%; noise-level |

The WAL is zero after the explicit `TRUNCATE` checkpoint in all four runs.
Logical compressed span payload remains exactly 765,537 bytes, every fixture
reopens with all 131,072 spans, and rich-span fidelity is unchanged. The CPU
cost comes from parsing configured JSON paths and constructing filters at
flush/optimize; it is not charged to tables that omit `attribute_indexes`.

## Regressions and compatibility

The maintained Rust gates pin:

- strict, bounded, canonical allowlist parsing and persisted configuration;
- buffered, flushed, optimized, retained, rolled-back, crash-recovered, and
  reopened exact equality;
- missing/null/empty/string/integer/real/boolean distinctions and stored
  container non-matches;
- span/resource/scope paths and explicit event/link rejection;
- invalid filter/configuration errors and query-only hidden-input writes;
- fixed-chunk metadata reads across 257 blocks with one exact survivor;
- missing-row conservative fallback plus version, length, and checksum
  corruption handling;
- public capability and stats negotiation in the extension and traces server;
- cold opening of an older table with neither attribute metadata key nor
  side table; and
- rich-span, projection, transaction, optimize, retention, crash, and reopen
  behavior through the real extension.

The data ABI remains 1. The new hidden input, capability member, stats rows,
metadata table, and server stats fields are additive. Tables created without
an allowlist retain their prior write path and reject the new input. A missing
per-block row never causes a false negative. Changing an allowlist requires a
side-by-side public row/batch migration; startup arguments never overwrite
persisted table configuration.

## TraceQL ownership recommendation

A later TraceQL matrix should separate four layers:

1. `EXT`: this exact configured scalar-equality candidate primitive and the
   existing packed trace/span/parent IDs;
2. `SQL`: JSON1 existence/type/container/non-equality expressions and exact
   retained-snapshot aggregates where ordinary SQL is honest;
3. `LIB`: Rust span-set Boolean composition, trace grouping, root/any/all
   quantifiers, bounded graph traversal, and missing-parent policy; and
4. `API`: pinned TraceQL grammar/oracle compatibility, pipelines,
   aggregators, result envelopes, limits, deadlines, and cancellation.

Event/link quantifiers, numeric/string range or regex acceleration,
complete-trace predicates, retry identity, completeness, and
retention-truncation semantics remain deferred until their data and query
contracts exist. TraceQL text must not enter the SQLite extension.

## Verdict

Ship the bounded attribute primitive as opt-in infrastructure for direct
SQLite/libSQL users and a later Rust TraceQL planner. Recommend it only for
fields expected to exclude most blocks. Keep JSON1 as the correct default for
unconfigured or low-cardinality fields, and do not enable any attribute path
automatically.
