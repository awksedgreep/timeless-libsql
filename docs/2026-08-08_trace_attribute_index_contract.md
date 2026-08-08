# Trace attribute equality contract

Date: 2026-08-08  
Session: 7 of the [trace query enhancement plan](2026-08-07_trace_query_enhancement_plan.md)  
Status: shipped as opt-in infrastructure

## Scope

The feature is a direct SQLite/libSQL primitive for exact typed scalar
equality. It does not parse TraceQL, return logical traces, infer completeness,
or index arbitrary attributes automatically.

At table creation, users opt in with a JSON array of exact fields:

```sql
CREATE VIRTUAL TABLE traces USING timeless_traces(
  attribute_indexes='[
    {"scope":"span","path":"/http.method"},
    {"scope":"resource","path":"/deployment.environment"},
    {"scope":"scope","path":"/attributes/debug"}
  ]'
);
```

The proposed contract accepts at most eight unique fields. `scope` is exactly
`span`, `resource`, or `scope`; `path` is a non-empty RFC 6901 JSON Pointer of
at most 256 UTF-8 bytes with only valid `~0` and `~1` escapes. Configuration is
canonicalized, persisted in the virtual table's metadata, and loaded from the
database on every connection. Replayed `CREATE` arguments never override
persisted configuration.

Events and links are deliberately excluded. They are repeated arrays with
their own quantifier and identity semantics, so treating them as one scalar
object would create a misleading prerequisite for TraceQL.

## Query shape

The feature adds one hidden input, `attribute_filter`, to the existing
`timeless_traces` table. Its value is a JSON object containing exactly one
configured scope/path and one scalar value:

```sql
SELECT lower(hex(trace_id)), lower(hex(span_id)), start_ts
FROM traces
WHERE attribute_filter =
  '{"scope":"span","path":"/http.method","value":"GET"}'
ORDER BY start_ts, span_id;
```

The filter may be combined with existing trace ID, service, name, kind,
status, timestamp, duration, order, limit, and offset constraints. The
extension performs the attribute comparison row-exactly; no SQLite recheck of
the hidden JSON input is needed.

## Type, missing, and null semantics

Only JSON scalar values are valid query operands: null, boolean, string, and
JSON number. Arrays and objects fail explicitly. Equality compares canonical
typed JSON scalar encodings, so these are distinct:

- missing path;
- JSON `null`;
- empty string `""`;
- string `"1"`;
- integer `1`;
- real `1.0`;
- boolean `true`.

A missing path never matches. JSON null matches only a present JSON null.
Arrays and objects stored at the selected path never match this scalar
primitive. JSON1 remains the public SQL surface for existence, type,
container, and non-equality predicates. Invalid configuration, an unconfigured
query path, malformed filter JSON, unknown keys/scopes, and composite operands
all fail explicitly.

## Storage behavior

Each configured field receives one fixed-size filter per persisted block. The
filter stores typed scalar hashes only; missing and container values add no
bits. A query uses it only to reject a block that cannot contain the requested
typed scalar, then decodes surviving blocks and rechecks every span exactly.

Legacy blocks without a filter row always take the exact decode fallback.
Buffer rows are checked exactly. Flush, automatic 8,192-span flush, optimize,
retention, transactions, rollback, savepoints, and block replacement must
publish or remove filter rows atomically with their payload block. Filter
version, size, or checksum corruption must fail closed rather than prune data.

Version 1 uses exactly 4,096 bytes and four stable hashes per configured
field/block. Metadata lookups process at most 256 candidate blocks per SQLite
statement and discard that Bloom working set before reading the next chunk;
the complete candidate/result vectors remain subject to the ordinary trace
query contract. Public `timeless_stats` rows report
`attribute_index_fields`, `attribute_bloom_rows`, and
`attribute_bloom_bytes`; `index_bytes` includes their physical b-tree.

The allowlist is immutable table metadata. Replayed creation arguments do not
change it, and a table created without an allowlist rejects
`attribute_filter`. Changing fields requires a side-by-side table populated
through the public row or batch surface.

## Ordinary SQL equivalent

For a span attribute, public JSON1 provides the exact decoded control:

```sql
SELECT lower(hex(trace_id)), lower(hex(span_id)), start_ts
FROM traces
WHERE start_ts >= :start_ns
  AND start_ts <= :stop_ns
  AND json_type(attributes, :json1_path) = :json_type
  AND attributes -> :json1_path = json(:scalar_json)
ORDER BY start_ts, span_id;
```

Use SQLite JSON-path syntax for `:json1_path`, the exact JSON1 type name for
`:json_type`, and an encoded scalar for `:scalar_json`. This preserves the
same missing/null/empty/number-type distinctions but cannot prune a block
before its public JSON column is decoded. Resource and scope controls replace
`attributes` with `resource` or `instrumentation_scope`.

The measured cost/benefit and exact recommendation are in the
[Session 7 report](2026-08-08_trace_attribute_indexes.md).
