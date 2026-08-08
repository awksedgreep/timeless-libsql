# Trace rich-span v2 fidelity

Date: 2026-08-08  
Implementation commit: `813a84cc13402f4a37ede2004c3a825c273d1f46`  
Contract: [`2026-08-08_trace_rich_span_v2_contract.md`](2026-08-08_trace_rich_span_v2_contract.md)

## Result

Session 5 is complete. `timeless_traces` now preserves OTLP links, trace
state/flags, resource and scope schema URLs, span/resource/scope dropped
attribute counts, dropped event/link counts, per-event dropped-attribute
counts, complete link values, and instrumentation-scope attributes. The
values are public SQL columns and public `rich-span-v2` (`0x03`) batch fields,
not a private server blob.

JSON OTLP, protobuf OTLP, direct SQL, dashboard responses, and the Rust traces
API use the same representation. Jaeger emits stored links as `FOLLOWS_FROM`
references and retains the parent as `CHILD_OF`; fields with no native Jaeger
representation are not fabricated as tags.

## Compatibility and storage

- Block generation 3 appends ten independent physical columns to the fourteen
  generation-2 columns. Generation 1 and 2 remain readable for raw, zstd, and
  both adaptive codecs and receive exact empty/zero defaults.
- Batch `0x01` and `0x02` remain accepted. Batch `0x03` keeps the complete v1
  body as its byte-for-byte prefix.
- Selecting any v2 fidelity column late-materializes the ten-column group.
  This keeps the projection mask inside SQLite's signed 32-bit `idxNum` while
  leaving each value separately typed and visible in SQL. Queries that match
  no span do not materialize the group.
- `timeless_capabilities()` advertises `rich-span-v2`, fidelity version 2, and
  the exact field inventory. The Rust traces server requires that capability
  and fails against an older extension instead of dropping fields.
- Optimize preserves mixed old/new batches; backup, reopen, transactions,
  rollback, malformed-batch atomicity, corruption failure, and the 8,192-span
  authoritative flush boundary are pinned through the real extension.

## Release evidence

The maintained Rust-only harness used the same 131,072-span, sixteen-batch,
48-block workload as Session 4. The v2 fixture deliberately makes every span
carry schema URLs, flags, dropped counts, and an event count; one eighth of
spans carry a complete link. This is a feature-cost comparison, not an
empty-column microbenchmark. Each candidate shape ran in an isolated release
process for 3 warmups and 20 measured iterations. Two complete candidate runs
are retained:

- [`primary JSON`](evidence/2026-08-08_trace_rich_span_v2.json)
- [`repeat JSON`](evidence/2026-08-08_trace_rich_span_v2_repeat.json)

The comparison baseline is the unchanged Session 4
[`posting-window evidence`](evidence/2026-08-07_trace_query_posting_windows.json).

### Write and storage

| measurement | Session 4 v1 fixture | v2 primary | v2 repeat | verdict |
|---|---:|---:|---:|---|
| public batch bytes | 66,817,644 | 86,953,580 | 86,953,580 | +30.1%; expected complete wire data |
| durable spans/s | 217,111 | 176,193 | 191,136 | 12.0–18.9% slower; accepted |
| optimize time | 394.4 ms | 507.9 ms | 462.9 ms | 17.4–28.8% slower; accepted |
| raw block bytes before optimize | 64,855,180 | 84,993,036 | 84,993,036 | +31.0% |
| optimized block payload bytes | 666,844 | 765,537 | 765,537 | +14.8% |
| live block payload + index bytes | 3,189,980 | 3,288,673 | 3,288,673 | +3.1% |
| active database bytes after optimize | 3,235,840 | 3,330,048 | 3,330,048 | +2.9% |

The allocated database file is 88,440,832 bytes versus 68,165,632 bytes
because incremental auto-vacuum retains pages freed when the larger raw
blocks are replaced. Candidate freelist bytes are 85,110,784, leaving only
3,330,048 active bytes. This is reclaimable allocation, not live v2 data;
automatic vacuum policy was not changed in a fidelity session.

### Read latency and memory

Times are p50/p95/p99. Candidate values show the primary–repeat range.

| shape | Session 4 | rich-span v2 | verdict |
|---|---:|---:|---|
| exact 8-span Jaeger trace | 2.382 / 2.562 / 2.627 ms | 2.711–2.719 / 2.867–3.055 / 2.894–3.788 ms | stable p50 +14%; accepted for complete projection |
| broad decode miss | 7.676 / 7.789 / 7.861 ms | 7.078–7.749 / 7.678–8.025 / 8.096–8.381 ms | effectively flat; no fidelity materialization |
| 4,096-span Jaeger result | 158.451 / 163.310 / 168.009 ms | 154.154–154.822 / 165.356–169.096 / 167.745–181.118 ms | p50 improved; p95 +1.3–3.5% |
| full-range bucket SQL | 19.547 / 20.421 / 20.459 ms | 20.435–23.244 / 20.987–28.073 / 21.128–30.624 ms | p50 +4.5–18.9%; reads 14.8% larger block blobs |
| one-box bucket SQL | 1.268 / 1.507 / 2.292 ms | 1.492–1.658 / 1.554–1.738 / 1.578–1.803 ms | p50 +17.6–30.7%; absolute tail remains under 1.9 ms |

The 4,096-span response grew only 1.31% (3,874,979 to 3,925,667 bytes) and
its isolated HWM fell from 178,764 KiB to 159,784–160,128 KiB. Exact/miss HWM
also fell slightly. Direct-SQL HWM moved from 15,048 KiB to
15,388–16,008 KiB (+2.3–6.4%), still bounded and well below the response
process.

Work counters explain the latency honestly. Miss and bucket queries decode
the same columns as Session 4 but read 14.8% larger SQLite block blobs. A full
fidelity result decodes the ten-column v2 group after predicate selection;
the broad Jaeger result materializes 15 rich values per returned span instead
of five. Candidate blocks, returned spans, cardinality, timestamps,
durations, and query results remain exact.

## Validation

- `cargo test -p timeless-core`
- `cargo test -p timeless-ext`
- complete `tests/cli.sh`, including crash/transaction/reopen gates
- complete `timeless-traces-api` unit, OTLP, Jaeger, storage, shutdown, backup,
  queue, cancellation, and publication tests against the real extension
- Rust `rich-traces` gate covering row insert, batches v0/v1/v2, every
  individual public-column projection, mixed projections, optimize,
  corruption, transactions, rollback, and reopen
- root and server workspace Clippy with warnings denied
- Rust query-harness Clippy with warnings denied

## Verdict

Ship the fidelity change. It makes writes materially heavier because it
stores materially more information, but retains 176k–191k durable spans/s on
this fixture. Live optimized storage grows only about 3%, broad read tails are
close to the previous engine, memory remains bounded, and the small exact
trace cost is appropriate for returning complete data rather than silently
discarding it. No private storage path or server fallback was introduced.
