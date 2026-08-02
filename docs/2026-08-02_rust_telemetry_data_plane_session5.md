# Rust telemetry data plane release promotion — Session 5

Date: 2026-08-02
Branch: `release/rust-telemetry-data-plane` in every affected repository

## Outcome

Session 5 switches application adapters and production defaults to the three
signal-specific Rust/libSQL owners after startup detection or migration. A
fresh release and migrated metrics/logs/traces fixture used only the Rust
processes; no embedded store or Rocket listener started. Unsupported routes,
parameters, query syntax, dashboard live tails, text metrics, and scraper
administration now fail explicitly instead of broadening a query or crossing
to a legacy owner.

The time-limited legacy selector was drilled offline against a copied retained
source. The release stopped all Rust owners first, started only the legacy OTP
owners with the required acknowledgement, returned the expected semantic
fixture, stopped cleanly, and left the retained source manifest byte-identical.

## Default owner and adapter cutover

- `timeless_metrics`, `timeless_logs`, and `timeless_traces` gained explicit
  embedded/external ownership modes. External mode starts no database owner,
  maintenance worker, or Rocket listener.
- `timeless_ui` gained complete-response clients for all three signals, a
  bounded logs producer buffer, a logger handler that preserves recursive
  typed metadata, a Victoria-compatible metrics writer, and an OTLP traces
  client. Normal reverse supervision shutdown drains the logs producer before
  its Rust owner.
- `timeless_stack` now defaults to `TIMELESS_DATA_PLANE=rust`, configures all
  three libraries as external owners, supervises the signal binaries through
  the neutral UI lifecycle seam, routes Canvas and OpenTelemetry through the
  Rust clients, and retains only an acknowledged offline legacy selector.
- The metrics, logs, and traces dashboards route historical reads through
  configurable data-plane sources. Unsupported live-tail subscriptions are
  visible errors.
- The exact declared surface, limits, no-fallback rule, ownership boundary,
  and rollback window are frozen in
  `timeless_stack/docs/telemetry_data_plane_compatibility.md`.

## Fresh and migrated release drill

The deterministic legacy fixture was generated only through the public legacy
APIs. It contained:

- one metric series with bit-exact `1.5` and negative zero samples;
- ordered notice and critical logs with typed/nested metadata; and
- a rich root/child trace with attributes, events, resources, instrumentation
  scope, status description, and exact parent relationship.

The actual Stack release was then started with Rust mode and the real
extension/binaries. All three startup states reached `completed_cutover` and
readiness. Process inspection found the three Rust owners and no embedded
telemetry owner. The target databases returned:

- metrics `[{1785698560000, 1.5}, {1785698561000, -0.0}]` from the canonical
  `metric_samples` table, with no parallel `metrics` table;
- both logs in exact source order with notice/critical severity and typed
  metadata; and
- both rich spans with exact IDs, relationship, timestamps, status, typed
  attributes, events, resources, and scope.

Graceful release stop drained and reaped all children. The retained legacy
source aggregate manifest before and after every final drill was:

```text
d5911bc7356ca3915df898bb56c0e6b1ed3af089ba15766304cd423cb840abc5d9a
```

## Offline rollback drill

The retained legacy directories were copied to an isolated rollback root and
the release started with:

```text
TIMELESS_DATA_PLANE=legacy
TIMELESS_LEGACY_ROLLBACK_ACK=retain-legacy-until-0.9.0
```

No Rust signal process started. Metrics returned the exact two float values,
logs returned the same ordered messages and typed metadata, and traces
returned the complete rich root/child fixture. The old legacy query envelope
coarsens notice to info and critical to error; that is a documented legacy
surface behavior, not mutation of the retained source. Shutdown was clean and
the retained source manifest remained identical.

There is no automatic or per-request fallback. Writes after cutover exist only
in the libSQL target, so selecting legacy mode creates a divergent timeline;
Session 6 owns the supported backup, rollback, and re-upgrade procedure.

## Regressions found and pinned

1. The migration target correctly used the canonical `metric_samples` table,
   but the server originally hard-coded the POC table name `metrics` and
   created an empty parallel store. Startup now selects one closed, validated
   table enum: fresh databases create `metric_samples`, POC-only `metrics`
   remains readable in place, and a dual-table database fails as ambiguous.
2. The LogsQL compatibility parser silently ignored unknown terms and
   pipelines. It now accepts only the frozen grammar and returns
   `422 unsupported_capability`; metrics, logs, and traces also reject unknown
   parameters and routes rather than broadening a request.
3. Same-size, same-mtime policy rewrites could reuse a stale Rust auth cache.
   The cache fingerprint now includes content identity and its regression
   rotates policy without changing file size.
4. `TimelessTraces.Span.from_map/1` crashed when a caller supplied an already
   normalized `%Span{}`. The conversion is now idempotent and pinned with a
   complete rich span.
5. Legacy logs/traces SQLite indexes contain absolute block paths. Starting a
   copied rollback store originally followed those paths back into the source,
   allowing retention in the copy to remove source blocks. Startup now
   preflights every indexed path, transactionally rebases it only when the
   corresponding regular file exists in the current data directory, rejects
   unsafe/missing external targets, and never follows or deletes the old path.
   Both libraries pin copy/query/retention while proving the original block
   survives.
6. The UI logs buffer previously allowed an unbounded configured batch. It is
   capped at the extension's authoritative 8,192 entries, traps exits, and
   drains on termination; tests pin the memory and shutdown boundaries.
7. Poller batches containing one text sample could discard otherwise valid
   numeric metrics. Numeric samples now proceed and the unsupported text count
   is reported explicitly.

## Fixed baseline rerun

All HTTP measurements used release artifacts, two readers, fresh databases,
the established deterministic workloads, and `TIMELESS_AUTH_MODE=disabled`
only so the numbers remain directly comparable with the POC baselines.
Authenticated request semantics and limits are covered separately by the
Session 4 gates. Maintenance was deferred for raw write/read matrices unless
stated; every write run ended with an ordered flush and zero queued/in-flight
work.

### Metrics

The mixed workload used 4,000 series, four writers, 1,000 points/request, two
query workers, seed 42001, and the fixed six-step 100-to-3.125 ms ramp.

| measurement | POC Session 6 | release Session 5 | change |
|---|---:|---:|---:|
| completed durable points/s | 869.9K | 856.5K | -1.5% |
| write p95 | 413 us | 436 us | +5.6% |
| mixed query p95 | 8.52 ms | 10.05 ms | +18.0% |
| mixed query p99 | 14.35 ms | 17.27 ms | +20.3% |
| process HWM | 181,080 KiB | 180,616 KiB | -0.3% |
| final drain | 704.32 ms | 710.90 ms | +0.9% |

All 7.8 million points completed, 4,000 series were visible, there were zero
write/query errors, and the final queue/in-flight gauges were zero.

The separately seeded 400,000-point fixture retained exact response sizes.
Selected p95s were 670 us exact latest, 606 us exact range, 671 us raw export,
9.25 ms label names, 2.36 ms 100-series selector range, and 2.74 ms
100-series `avg_over_time`. Read-process HWM was 55,088 KiB.

### Logs

The fixed workload used four writers, 500 entries/request, two query workers,
seed 42, and the six-step 100-to-3.125 ms ramp.

| measurement | flat POC v0 | rich release v1 | change |
|---|---:|---:|---:|
| final-step completed entries/s | 470.2K | 316.0K | -32.8% |
| admitted and durably drained / (offer + drain) | queue-free | 315.7K | explicit |
| write p99 | 1.66 ms | 25.61 ms | saturated rich queue |
| query p99 | 260.51 ms | 378.33 ms | +45.2% |
| process HWM | 62,340 KiB | 130,816 KiB | +109.9% |
| final drain | 15.88 ms | 63.20 ms | +298.0% |

The release completed all 2,651,500 entries with zero HTTP/query/storage
errors and zero final queue. The no-query control completed 353.5K entries/s
during the final offered window and 349.4K admitted/durable entries/s including
its 121.14 ms drain. The untouched POC head, rebuilt and run back-to-back on
the same host, completed 474.2K entries/s with no final queue. This confirms a
real release-format tradeoff rather than host noise or the strict parser.

The principal attribution is required fidelity. Rich v1 retains eight exact
severities, epoch microseconds, and canonical typed/nested JSON; the POC v0
flattened metadata and coarsened severity. In the no-query controls, rich v1
flush/encode work used 4.27 seconds for 2.791 million entries versus 2.69
seconds for 3.123 million flat entries, and logical raw bytes/entry increased
from about 152 to 162. Reverting that format would violate the release parity
contract. The regression is accepted honestly for Session 5 and remains a
candidate for a future fidelity-preserving codec optimization.

### Traces

The fixed ingest used 16 writers x 100 requests x 500 rich spans. The read
process was restarted over the 800,000-span fixture after public optimize had
converted all 95 raw blocks to 95 compressed blocks.

| measurement | POC Session 7 | release Session 5 | change |
|---|---:|---:|---:|
| completed durable spans/s | 178.5K | 171.1K | -4.1% |
| exact trace p95 | 4.63 ms | 5.58 ms | +20.5% |
| service fan-out p95 | 32.90 ms | 31.20 ms | -5.2% |
| duration-miss p95 | 362.43 ms | 394.53 ms | +8.9% |
| write HWM | 271,264 KiB | 258,532 KiB | -4.7% |
| read HWM | 66,732 KiB | 66,312 KiB | -0.6% |

All 800,000 spans completed, failed/queued/in-flight counts were zero, the
barrier drained in 1.71 ms, and the read oracle returned the identical 1,140
traces, 1,300 spans, and 751,270 response bytes. The decode-bound duration
miss remains the explicit tail tradeoff from the POC.

## Validation

The final Session 5 gate includes:

```text
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
TIMELESS_EXT_PATH=... cargo test --workspace --manifest-path servers/Cargo.toml
mix format --check-formatted
mix test / mix precommit in every affected Elixir repository
actual fresh/migrated release start, query oracle, drain, and child-reap drill
actual acknowledged legacy rollback query oracle and source-manifest check
```

The gate passed:

- the core Rust workspace and doc tests, plus strict all-target Clippy;
- 68 signal-server/common tests including every ignored real-extension
  contract, all server doc tests, strict all-target Clippy, and the rich-log
  SQL oracle;
- `timeless_metrics`: 491 tests;
- `timeless_logs`: 230 tests;
- `timeless_traces`: 201 tests;
- `timeless_ui`: 105 tests passed, one intentionally skipped;
- metrics/logs/traces dashboards: 31 / 6 / 13 tests; and
- `timeless_stack`: 37 tests against the final Git dependency graph.

## Exit verdict

The functional exit criterion is met: fresh and migrated fixtures use only
Rust/libSQL by default, declared surfaces return exact storage/response
semantics, unsupported behavior fails explicitly, and the offline rollback
drill leaves the retained source byte-identical. Session 5 preserves the logs
rich-fidelity throughput regression rather than hiding it. Session 6 must
complete coordinated backup/artifacts/install/upgrade/rollback before this
branch can receive a release verdict.
