# Rust telemetry data plane release promotion — Session 7

Date: 2026-08-02
Branch: `release/rust-telemetry-data-plane` in every affected repository

## Outcome

Session 7 adds a completion-aware production gate around the three real
signal-specific Rust binaries and the real `timeless-libsql` extension. It
drives public HTTP writes and reads, measures only work acknowledged by a
durable barrier, reopens every database cold, and rejects empty query shapes,
partial responses, count drift, queue residue, excessive WAL/RSS, or failed
maintenance. The short gate and all independent migration/control/package
gates pass. The authoritative release soak for clean build identity
`bab775035785b78e0d9879b7d871bbd938e92991` also passed. Its complete checked
evidence is
[`evidence/2026-08-02_release_gate_bab7750.json`](evidence/2026-08-02_release_gate_bab7750.json)
(SHA-256
`8d3472a3b7843b759e1a1fda830c2668b89da5c96ffa1025aa2069c10158d11d`).

Release mode runs metrics, logs, and traces concurrently for 160 minutes.
That supplies at least two continuous hours per signal and eight aggregate
signal-hours, matching the checked-in exit criterion without confusing eight
aggregate signal-hours with eight wall-clock hours.

## Authoritative 160-minute result

The exact-build run completed 9,602.37 seconds, or 8.00197 aggregate
signal-hours, with `verdict: passed`, no failures, and no workload errors. All
12 scheduled fault events passed. Final public barriers matched every accepted
record, returned `status: ok`, and left zero queued or in-flight work.

| signal | durable records | durable records/s | write p50 / p95 / p99 ms | query p99 range ms | RSS HWM KiB | WAL HWM bytes |
|---|---:|---:|---:|---:|---:|---:|
| metrics | 2,457,664 | 255.94 | 0.710 / 0.989 / 1.226 | 1.685–2.950 | 20,568 | 28,648,400 |
| logs | 1,859,904 | 193.69 | 0.516 / 1.244 / 1.580 | 5.551–3,746.335 | 62,228 | 27,810,032 |
| traces | 2,457,600 | 255.94 | 1.499 / 1.927 / 2.181 | 2.167–10.555 | 206,236 | 164,342,560 |

Logs completed below the offered 256 entries/s because its single cyclic query
worker spent most of its time in exact discovery and native scalar count while
the data set grew. This is completed durable work, not admission. It caused no
queue residue or loss and stayed well inside the ten-second query ceiling.

Every query shape was exercised thousands of times and returned a nonzero
result:

| signal / shape | requests | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|
| metrics / exact latest | 9,600 | 1.188 | 1.663 | 1.962 |
| metrics / narrow range | 9,600 | 1.903 | 2.571 | 2.950 |
| metrics / wide range | 9,600 | 1.847 | 2.495 | 2.874 |
| metrics / scalar average | 9,600 | 1.880 | 2.532 | 2.931 |
| metrics / discovery | 9,600 | 1.142 | 1.456 | 1.685 |
| logs / exact | 4,506 | 44.129 | 53.318 | 57.053 |
| logs / narrow | 4,506 | 1.379 | 3.913 | 5.551 |
| logs / wide | 4,506 | 4.654 | 18.633 | 21.520 |
| logs / scalar count | 4,506 | 328.183 | 824.752 | 930.625 |
| logs / discovery | 4,506 | 1,354.331 | 3,403.415 | 3,746.335 |
| traces / exact trace | 9,600 | 6.695 | 9.540 | 10.555 |
| traces / narrow search | 9,600 | 2.121 | 3.031 | 3.572 |
| traces / wide search | 9,600 | 3.593 | 5.723 | 7.131 |
| traces / operations | 9,600 | 1.968 | 3.007 | 3.574 |
| traces / services | 9,600 | 1.316 | 1.835 | 2.167 |

Final logical/physical storage was 54,055/8,745,112 bytes for metrics,
3,942,934/33,122,544 bytes for logs, and 21,581,068/219,114,272 bytes for
traces. Physical amplification was 3.56, 17.81, and 89.16 bytes per durable
record respectively. SQLite freelist watermarks, page bytes, and physical
high-water are reported separately; logical optimize is not mislabeled as a
full-file vacuum.

Production maintenance crossed 959 scheduled metric flushes and 30 compacts,
321 log optimizes, and 9,607 scheduled trace flushes plus 318 optimizes. Each
signal recorded three successful checkpoints and three successful backups;
the one backup error per signal is the required no-clobber refusal. Trace read
contention caused 127 bounded retries, all successful. No maintenance error,
writer timeout, rejected write, partial response, or final queue residue was
recorded.

Five process generations were measured because the fault schedule includes
two graceful and two SIGKILL restart cycles. Metrics peaked at 20 MiB, logs at
61 MiB, and traces at 201 MiB, far below their 512/512/768 MiB limits. No
generation lived for two continuous hours, so the checked long-generation RSS
slope value is correctly inapplicable (`0.0`) rather than a claim that memory
never grows. Diagnostic short-generation trace slopes were positive
(58.9–94.7 MiB/hour in the longer generations) as the retained trace and term
indexes grew. This is preserved as a named release limitation and must be
judged with live-set cardinality and HWM until retention reaches steady state.

## What the gate covers

- mixed 4 Hz ingestion and 5 Hz query traffic per signal using 64-record
  public HTTP batches;
- exact/latest, narrow, wide, scalar, discovery, Jaeger, LogsQL, and native
  dashboard query shapes with response-byte and result-row high-water marks;
- completion-aware flush/barrier accounting, final drain, cold reopen, and
  semantic sentinels;
- recurring graceful and SIGKILL restart/reap cycles, slow clients,
  disconnected clients, and cancellation storms;
- online backup overlapping ingestion, no-clobber refusal, checksums, and
  exact cold verification;
- corrupt and read-only storage, bind conflicts, descriptor pressure, and a
  real 1 MiB file-size/disk-I/O failure with exact durable-prefix recovery;
- production maintenance intervals, optimize/rollup/retention counters,
  SQLite page/freelist accounting, WAL/checkpoint behavior, logical/physical
  storage, queues, request/result watermarks, RSS/HWM, and per-process RSS
  slope after warmup; and
- live token rotation/revocation plus Phoenix control-policy disconnect and
  reconnect through a real metrics release binary.

## Two-minute CI-equivalent result

The corrected 120-second gate passed with no gate failures and all 12 fault
events successful. Each signal admitted and durably recovered 30,784 records,
or 256.53 records/s. No query shape was empty.

| signal | write p50 / p95 / p99 ms | query p99 range ms | RSS HWM KiB | WAL HWM bytes |
|---|---:|---:|---:|---:|
| metrics | 1.01 / 2.28 / 2.72 | 1.89–2.66 | 12,788 | 2,165,888 |
| logs | 1.24 / 1.83 / 2.04 | 3.30–61.33 | 45,268 | 4,000,552 |
| traces | 1.46 / 2.12 / 2.34 | 1.33–10.47 | 58,620 | 9,992,504 |

The largest individual ingest bodies were 1,760 bytes for metrics, 12,656
bytes for logs, and 36,651 bytes for traces. The largest query responses were
783 bytes, 216,750 bytes, and 81,574 bytes respectively. These are per-request
watermarks, not cumulative counters.

Disk-full recovery proved exact durable prefixes rather than equating HTTP
acceptance with durability:

| signal | accepted before failure | durable/recovered | surfaced failure |
|---|---:|---:|---|
| metrics | 69,632 | 65,536 | flush HTTP 500 / disk I/O error |
| logs | 6,144 | 4,096 | flush HTTP 500 / disk I/O error |
| traces | 2,048 | 1,024 | flush HTTP 500 / disk I/O error |

## Automatic migration under concurrent production load

Each product's real legacy reader fed the public extension batch/SQL
interface while the production gate was active. Sources remained immutable,
WALs ended at zero, candidates reopened cold, and exact identity digests (plus
the trace relationship digest) matched.

| signal | records | durable rate/s | public write ms | maintenance ms | migration RSS delta | candidate physical bytes |
|---|---:|---:|---:|---:|---:|---:|
| metrics | 8,195 | 149,027.4 | 5.02 | compact 1.67 + rollup 0.11 | 9,809,920 B | 475,136 |
| logs | 8,193 | 40,639.0 | 40.19 | optimize 25.45 | 29,040,640 B | 1,212,416 |
| traces | 8,193 | 16,557.5 | 186.89 | optimize 45.79 | 70,524,928 B | 4,440,064 |

The rates measure completed durable migration work, not decoded input or
queued requests. Metrics preserved exact float bits and rollups; logs
preserved timestamp/order/severity/typed metadata; traces preserved IDs,
parent relationships, nanosecond times, status descriptions, events,
resources, instrumentation scope, and typed attributes.

## Regressions found and pinned

1. Event timestamps were initially in 2033, so exact-latest queries could
   return empty while still reporting HTTP success. The workload now advances
   from 2026-08-02 at its real 256-record/s event-time rate, and every query
   shape must return a nonzero result high-water mark.
2. A cumulative API response-byte counter was mislabeled as a memory HWM.
   The gate now records exact per-request ingest, response, and result-row
   watermarks.
3. Restarts could hide growth by fitting one slope across unrelated PIDs.
   RSS slopes are computed per process generation and the release limit is
   enforced for every generation alive at least two hours.
4. Backup assumed actionable optimize entry counts decrease monotonically.
   A valid raw-to-merge planner transition grew trace work from 1,280 to 4,698
   entries. Logs/traces now compare structural planner state, allow phase
   expansion, reject an unchanged state, and retain a one-million-step hard
   bound. Unit regressions and overlap backup/cold parity pass.
5. Jaeger and OTLP contracts reached into a sibling `timeless_traces`
   checkout and failed on clean CI machines. Their rich-span fixture is now
   repository-local and byte-identical.
6. Focused trace startup tests still referenced helpers formerly owned by a
   test module. All startup/migration/legacy fixtures now use the shared test
   support module and pass independently.
7. A cross-repository migration command could accidentally load a historical
   preview extension. The release drill now supplies the exact candidate
   extension to all three migration suites.
8. The artifact matrix used retired `macos-13` and deprecated `macos-14`
   runner labels. It now uses the supported pinned `macos-15-intel` and
   `macos-15` runners.

## Independent release gates

- all root and server Rust unit/contract tests and strict all-target Clippy;
- all real-extension ignored metrics/logs contracts plus trace storage,
  OTLP, and Jaeger contracts;
- 14 focused trace startup/migration/legacy tests after fixture isolation;
- five real token/control tests and 13 focused lifecycle/controller tests;
- 24 focused UI owner/client/application tests and 37 Stack tests;
- byte-identical local x86-64 archives, checksum/SBOM/license verification,
  install/uninstall preservation, and warning-as-error production release
  compilation; and
- GitHub's four native artifact builders plus the clean short production gate.

## Exit verdict

Pass. The authoritative exact-build soak, shorter fault gate, parity,
migration-under-load, control-plane, packaging, and clean-CI prerequisites all
pass. Durable counts are exact; every query shape is populated and below the
declared p99 ceiling; faults recover as declared; queues drain; and RSS/WAL
remain below their HWM bounds. Trace live-index RSS growth and the slower log
discovery/scalar tails are retained explicitly for Session 8's named-limitation
verdict rather than averaged away.
