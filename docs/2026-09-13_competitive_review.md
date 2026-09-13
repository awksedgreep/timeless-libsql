# Oracle and competitive review — 2026-09-13

Timeless 0.8.4 retains the tested query behavior and has a substantially smaller
metrics process footprint on these fixtures. Prometheus leads the metric query
latencies, while VictoriaLogs leads most log workloads. Timeless's native
unfiltered count is close to VictoriaLogs; changing its output alias selects a
much slower execution path. Two ordinary LogsQL compositions remain unsupported.

These results support positioning Timeless as an embedded SQLite/libSQL telemetry
engine with useful compatible query surfaces. They do not establish a universal
speed advantage or complete upstream language compatibility.

## What ran

- Live immutable oracle suites: **549/549 Prometheus**, **196/196 VictoriaMetrics**,
  **1,498/1,498 VictoriaLogs**, plus Prometheus rule smoke and all version probes.
- Timeless metrics/logs suites: **457 passed, zero failed or ignored**, with the
  real release extension and the ignored integration tests explicitly included.
- Benchmark harness: **64 tests passed**, strict Clippy passed for all targets.
- Competitive captures: **four complete runs, 6,800 measured requests**. Every
  response matched independently generated fixture expectations. Every engine
  retained the full dataset across both required graceful restart checks.

The [oracle record](evidence/2026-09-13_oracle_revalidation.json) links the retained
raw upstream case logs and fixture hashes. Live oracle checks validate the
upstream fixtures; the separate Timeless regressions exercise our implementation.
Case counts include rejection cases and are not a count of supported features.

The declared matrices still contain 74 shipped PromQL rows, 10 MetricsQL rows,
and 107 LogsQL rows, with 11 experimental and 14 deferred rows across the 216-row
inventory. These are scoped implementation contracts, not percentages of all
competitor capabilities. The sorting row has been clarified to mean timestamp
sorting. The new composition probes show why passing the existing corpus alone
cannot establish complete parity.

## Provenance and method

Timeless used the **published 0.8.4 Linux ARM64 bundle**, source
`3453ec77436fde6eb09f1a603ce976227203e6eb`. Its outer archive SHA-256 is
`db5ef515a995f701b1b10f991768da3ed0224b0b7b194ea303c347b654d4145f`.
The archive and all inner checksums were verified. Server build identities match
the bundle manifest. Local real-extension tests used
`ee6c9dd2d7b5af45395ee06f5cbc44f9dfeb256e`, whose production code is identical to
the release; the intervening changes were release documentation.

The measured harness source is `b6115d27061a8bbdc94139634d91ce92ed31e6ca`.
Competitors retain the existing pins: **Prometheus 3.13.2**, **VictoriaMetrics
1.148.0**, **VictoriaLogs 1.52.0**. Native ARM64 child digests were recorded in
the [manifest](../tests/query_oracles/manifest.json); no upstream version or
semantic fixture was moved. This evaluates the pinned baseline, not the latest
upstream release lines.

All measured engines ran natively in the same ARM64 Linux Podman VM, configured
with 12 CPUs and 32 GiB RAM. Each container had the same four-CPU/4-GiB ceiling
and its own disposable volume. The macOS client used the same loopback port
forwarding path. One pre-existing idle PostgreSQL container was left running;
this was a shared development host, not an isolated benchmark machine.

The small fixture has **512 series × 32 samples and 8,192 logs**. The larger
fixture has **2,048 series × 32 samples and 16,384 logs**. Each size ran twice.
All metric labels, timestamps and samples are identical across engines in a
capture; both log engines ingest identical NDJSON. Five warmups precede fifty
measured requests per shape per engine, with engine order rotating each round.
Prometheus self-scraping is disabled. Engine caches and maintenance retain their
defaults. Timings cover HTTP through the complete body, excluding client-side
JSON decoding and validation.

Count columns normalize decimal strings versus JSON numbers; retained log
fields retain their types. Metric series and unsorted aggregate groups compare
by identity, while timestamps and ordered rows remain exact. See the
[reproduction protocol](COMPETITIVE_BENCHMARK.md) for the full contract.

## Small fixture: p50 / p95 milliseconds

| Query | Timeless | Prometheus | VictoriaMetrics |
|---|---:|---:|---:|
| Exact selector | 0.296 / 0.365 | 0.198 / 0.250 | 0.196 / 0.268 |
| Wide selector | 2.398 / 2.963 | 1.575 / 2.181 | 2.157 / 3.203 |
| Sum | 1.542 / 1.879 | 0.692 / 0.921 | 0.774 / 1.349 |
| Grouped sum | 1.447 / 1.590 | 0.672 / 0.784 | 0.870 / 1.108 |
| Rate over 60 seconds | 1.984 / 2.727 | 1.274 / 1.987 | 1.745 / 2.989 |
| Full range | 3.380 / 4.514 | 2.530 / 3.229 | 3.390 / 5.013 |

| Query | Timeless | VictoriaLogs |
|---|---:|---:|
| Count, `as total` | 0.241 / 0.363 | 0.244 / 0.449 |
| Count, `as n` | 7.985 / 8.502 | 0.373 / 0.659 |
| Filtered count, `as total` | 4.545 / 5.118 | 0.425 / 0.895 |
| Filtered count, `as n` | 5.308 / 5.663 | 0.412 / 0.797 |
| Grouped count | 8.315 / 8.451 | 0.568 / 1.073 |
| Exact message | 5.743 / 6.292 | 0.496 / 0.746 |
| Host-filtered rows | 0.959 / 1.320 | 0.599 / 1.263 |
| Ordered full rows | 14.794 / 15.790 | 6.590 / 8.170 |

The second small run preserves the main findings. Timeless metric sums/grouped
sums are about 2.1–2.2 times Prometheus's median. Timeless's full range is
3.38–3.88 ms versus VictoriaMetrics's 3.39–3.49 ms and Prometheus's 2.53–2.72 ms.
The native unfiltered count is 0.241–0.258 ms versus VictoriaLogs's
0.244–0.246 ms, too close to justify a winner on this host.

The `as n` count is 7.95–7.98 ms: **31–33 times the Timeless `as total` count**.
The parser explicitly selects its public native count path for `as total`;
other aliases enter the general row pipeline. Filtered native count still takes
4.45–4.55 ms versus VictoriaLogs's 0.37–0.43 ms. Exact-message lookup takes
5.58–5.74 ms versus 0.47–0.50 ms, and ordered full rows take 14.45–14.79 ms
versus 6.59–6.75 ms. The scalar reduction and message-filter paths deserve
attention even though the shared results are correct.

## Larger fixture: range of p50 milliseconds across both runs

| Query | Timeless | Prometheus | VictoriaMetrics |
|---|---:|---:|---:|
| Exact selector | 0.276–0.324 | 0.191–0.221 | 0.172–0.204 |
| Wide selector | 4.706–5.552 | 3.053–3.668 | 3.675–4.273 |
| Sum | 5.022–5.346 | 1.997–2.003 | 1.734–1.844 |
| Grouped sum | 5.130–5.617 | 2.048–2.122 | 1.816–1.971 |
| Rate over 60 seconds | 4.746–4.833 | 3.021–3.066 | 3.923–4.094 |
| Full range | 11.679–12.178 | 8.994–9.086 | 10.049–10.079 |

| Query | Timeless | VictoriaLogs |
|---|---:|---:|
| Count, `as total` | 0.203–0.258 | 0.191–0.243 |
| Count, `as n` | 16.027–22.227 | 0.367–0.624 |
| Filtered count, `as total` | 8.750–8.822 | 0.469–0.528 |
| Filtered count, `as n` | 9.829–13.850 | 0.499–0.753 |
| Grouped count | 17.001–17.868 | 0.717–0.737 |
| Exact message | 11.147–11.952 | 0.661–0.667 |
| Host-filtered rows | 1.491–1.793 | 0.731–0.959 |
| Ordered full rows | 28.695–36.793 | 11.179–15.002 |

The relative gaps persist as these fixtures grow. Some large-query medians vary
materially between repetitions, especially full log materialization and aliased
count. The raw samples and tails remain available; these runs do not establish
linear scaling, a throughput ceiling, or statistical significance for small
sub-millisecond differences.

## Resource footprint

| Engine | Small RSS, MiB | Larger RSS, MiB | Small allocated data, KiB | Larger allocated data, KiB |
|---|---:|---:|---:|---:|
| timeless metrics | 15.8–16.3 | 32.8–33.0 | 596.0 | 1716.0 |
| Prometheus | 84.7–85.6 | 92.9–94.6 | 148.0 | 436.0 |
| VictoriaMetrics | 49.2–50.7 | 62.5–64.5 | 2112.0–2152.0 | 3564.0–3572.0 |
| timeless logs | 31.1–31.9 | 52.2–54.5 | 744.0 | 1388.0 |
| VictoriaLogs | 23.9–33.6 | 32.7–43.8 | 100.0 | 116.0 |

RSS is the server process after the read/probe phase, measured inside Linux.
It excludes page cache and VM overhead; ingestion high-water memory is not
measured. The JSON also retains RSS HWM. The table's disk figures cover the
whole data volume **after the final graceful restart**, including indexes,
metadata and WAL state. The JSON separately records startup, post-query,
apparent-byte and allocated-byte snapshots. No compaction or VACUUM was forced.

Timeless has a clear metrics RSS advantage here. It does not have a universal
storage-size advantage: its metric data volume exceeds Prometheus's on these
small fixtures, and its log data volume exceeds VictoriaLogs's. Fixed metadata,
restart/checkpoint behavior and this flat string-field fixture matter; these
figures are not production compression ratios or richer-model equivalence.

## Compatibility findings and next work

1. **Make count execution independent of output alias.** Preserve the native
   count path for arbitrary aliases and supported following operations. The
   measured `as total` versus `as n` difference makes this the first performance
   target. Until then, an unfiltered terminal `stats count() as total` is the
   measured fast form; filtered counts still need profiling.
2. **Support ordinary field sorting.** `stats by (service) count() as n | sort
   by (service)` returns HTTP 422 from Timeless and the correctly ordered result
   from VictoriaLogs. The previously broad matrix wording has been narrowed.
3. **Allow a terminal limit after native count.** `stats count() as total |
   limit 1` also returns HTTP 422 from Timeless and HTTP 200 from VictoriaLogs.
   The fast path should preserve ordinary pipeline composition. Both failures
   are retained under `signals.logs.capability_probes` in every capture.
4. **Profile filtered/grouped log reductions, exact-message search, and metric
   sum/grouped-sum planning.** Those gaps persist across both fixture sizes.
   Use storage-work counters to distinguish avoidable materialization from
   required scanning before choosing an index or storage-format change.

Native histogram samples and stale-marker ingestion remain PromQL data-model
gaps. Stored log stream identity, query-internal parallel execution and partial
multi-owner responses remain separate architectural work. TraceQL/Jaeger
competition, sustained ingestion, concurrent clients, high cardinality,
multi-node behavior and cold-cache performance were not measured here.

## Evidence and validation limits

| Capture | SHA-256 |
|---|---|
| [Small A](evidence/2026-09-13_competitive_small_a.json) | `12b3b11824747ad0b9651f0fa66fe3ce4009433eeb73b4a2f49b95288183d67c` |
| [Small B](evidence/2026-09-13_competitive_small_b.json) | `ddce871ba95a01d901fd920864123965092af59cd57670f5149e32d904551c9b` |
| [Larger A](evidence/2026-09-13_competitive_larger_a.json) | `e1dceb4e212a04c6f433807a94f2c09ed3080b3f30f3aa29a01e517372d39777` |
| [Larger B](evidence/2026-09-13_competitive_larger_b.json) | `4c18f17b15bd6dc09dfb15919f1aa4780309694b1f28d2c254d27a3794cb77ab` |

The original broad `evidence` command was attempted on macOS and failed because
its RSS reader requires Linux `/proc`; it produced no complete baseline JSON.
The competitive captures above are complete native-Linux process measurements.
Docker Hub later rate-limited repeated registry requests during harness setup;
final runs used already-downloaded, digest-verified native images with implicit
pulls disabled. Setup failures and exploratory timings are not included in the
four final captures. Harness containers and volumes were removed after use.

The archived [August release report](QUERY_RELEASE_REPORT.md) remains unchanged.
Its historical host and workload differ, so this review does not claim a
controlled before/after performance change since August.
