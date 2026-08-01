# Standalone query API characterization — 2026-08-01

This run characterizes the additive durable-series and aggregate/latest frame
APIs. It is diagnostic data, not a release promise or a Rust-engine parity
gate.

## Revisions and host

- Starting `timeless-libsql` revision: `650fe13dc8ee2766c2d9d2aa60586b3091b71435`
- Working branch: `feat/standalone-query-api` (dirty implementation tree)
- Linux `7.1.3-arch1-2`, x86-64
- Intel Core Ultra 9 185H, 22 logical CPUs
- rusqlite-bundled SQLite `3.53.2`
- Fixture: 12,000 series, 60 points per series, 720,000 total points
- Samples: 20 timed runs (100 for very narrow paths)

Command:

```sh
cargo run --release --manifest-path tools/bench/Cargo.toml \
  --bin query-read -- target/release/libtimeless_ext.so \
  --series 12000 --points 60 --runs 20
```

## New whole-result frames

| query | median | p95 | SQLite rows | logical series | returned bytes |
|---|---:|---:|---:|---:|---:|
| `timeless_aggregate` rows (`avg`) | 20.906 ms | 21.636 ms | 12,000 | 12,000 | 1,067,015 |
| `timeless_aggregate_frame` (`TAF1`) | **4.452 ms** | **4.994 ms** | 1 | 12,000 | **193,512** |
| `timeless_latest` rows | 21.240 ms | 22.287 ms | 12,000 | 12,000 | 1,163,015 |
| `timeless_latest_frame` (`TLF1`) | **4.438 ms** | **5.040 ms** | 1 | 12,000 | **289,508** |

The benchmark decodes both frames and compares series IDs and value/timestamp
bits with the row TVFs before sampling. On this full-chunk fixture, aggregate
and latest rows and frames use persisted metadata and read zero raw chunk
payloads. The raw fallbacks read 12,000 payloads and return 12,539,015 bytes.

## Selection and existing query context

| query | median | p95 | selected series | logical points | returned bytes | fixture payload chunks |
|---|---:|---:|---:|---:|---:|---:|
| exact raw batch | 34 us | 43 us | 1 | 60 | 1,041 | 1 |
| selective regex raw batch | 3.203 ms | 4.239 ms | 1 | 60 | 1,041 | 1 |
| narrow raw batch | 1.004 ms | 1.221 ms | 188 | 11,280 | 196,284 | 188 |
| wide raw batches | 66.244 ms | 67.200 ms | 12,000 | 720,000 | 12,539,015 | 12,000 |
| wide raw frame | 55.979 ms | 56.977 ms | 12,000 | 720,000 | 11,664,016 | 12,000 |
| aggregate raw fallback | 67.337 ms | 73.153 ms | 12,000 | 720,000 | 12,539,015 | 12,000 |
| latest raw fallback | 66.803 ms | 67.984 ms | 12,000 | 12,000 | 12,539,015 | 12,000 |

The fixture performs one flush, so each selected raw series has exactly one raw
chunk. Payload-read counts above are therefore deterministic for the raw
paths. The CLI regression additionally corrupts an unrelated series payload:
an ID-selected query succeeds while the broad query reaches the corruption,
pinning the no-unrelated-chunk-read behavior without adding benchmark-only
instrumentation to the public extension.

## Other run metadata

- Initial named-batch ingest: 108.023 ms
- Flush: 319.332 ms
- Database plus WAL/SHM: 10,138,576 bytes
- First exact read after publication: 175 us
- Warm exact raw batch: 34 us median
- Warm empty-query refresh: 19 us median, 23 us p95

