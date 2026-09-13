# Same-workload competitive benchmark

The Rust query harness compares a published Timeless Linux bundle with the
immutable Prometheus, VictoriaMetrics and VictoriaLogs releases in
[`tests/query_oracles/manifest.json`](../tests/query_oracles/manifest.json).
This is a bounded public-HTTP read benchmark. Live semantic oracle verification
remains a separate gate described in [QUERY_ORACLES.md](QUERY_ORACLES.md).

## Reproduce

Use a clean canonical checkout, a running Podman Linux runtime, and an extracted
published Linux bundle whose architecture matches that runtime. Verify the
downloaded archive against the release's outer `SHA256SUMS` first. The harness
also verifies every inner bundle checksum, the clean artifact manifest, and
the runtime server build identities. Supply an immutable Debian base image
with `sh`, `cat` and GNU `du` for the published glibc binaries and measurements.
The same image runs measurement helpers for every engine.

```sh
cargo run --release --manifest-path tools/query-harness/Cargo.toml --locked -- \
  competitive \
  --runtime podman \
  --bundle /absolute/path/to/extracted-linux-bundle \
  --base-image docker.io/library/debian@sha256:79abd3da4379967c72ac0c0e2dc9ac2204143b92224fada931ef855b8559df87 \
  --output /absolute/path/to/competitive.json
```

The above base image was selected on the ARM64 test host. The runner rejects
an image or bundle whose architecture differs from the runtime; it never
silently benchmarks emulation. All containers have four CPU and 4 GiB memory
limits, open local authentication, independent disposable data volumes, and
loopback-only published ports. Only the containers and volumes owned by this
invocation are removed. Existing workloads and downloaded images are retained.
Run on an otherwise quiet host and repeat the capture before drawing conclusions.

## Workload and correctness

Defaults are 512 metric series with 32 ten-second samples each and 8,192 flat
string-field logs. Sizes are explicit options (`--metric-series`,
`--metric-points`, `--log-entries`). The report records the evaluation clock
and SHA-256 of each deterministic fixture. Metric labels and values are
identical across products; Prometheus receives Remote Write while Timeless and
VictoriaMetrics receive Victoria JSON. Both logs servers receive identical
NDJSON bytes. Prometheus self-scraping is disabled with an empty configuration.

Each engine is gracefully stopped and restarted after ingestion. The harness
then verifies every metric sample through the full range query, or every
projected log row through the ordered full scan. Readiness and row counts alone
do not satisfy this check. Timeless also executes its explicit flush endpoint.
After timing, the runner repeats the restart and complete-data check.

The six metric queries cover an exact selector, wide selector, sum, grouped
sum, rate and wide range. The eight log queries cover count, filtered count,
grouped count, exact message, indexed rows and ordered full rows. Expected
count results use both `as total` (Timeless's native count path) and `as n`
(its general pipeline) to expose alias-sensitive execution costs. Expected
results are generated independently from the fixture, not copied from a
competitor response. Every response, including warmups, must match those
expectations. Metric series order and numeric spelling may differ; labels,
result types, timestamps, sample order, log order and projected fields must
match. Only floating sample values have a relative/absolute `1e-9` tolerance.
The declared aggregate count columns `n` and `total` are normalized from a decimal string
or an unsigned JSON number: VictoriaLogs and Timeless use different wire
encodings for this numeric result. Retained log field types are not coerced.
Unsorted aggregate groups compare by group identity; explicitly sorted retained
log rows preserve order. A separate untimed `sort by (service)` probe records
the capability gap: Timeless supports timestamp sorting, while VictoriaLogs
also sorts the grouped result by an ordinary field. Failed capability probes
are retained alongside the timings rather than counted as successful queries.
Missing rows, wrong timestamps, non-finite samples and query diagnostics fail.

## Timing and resource accounting

Each shape has five warmups and fifty measured requests per engine. Requests
are serial and rotate which engine runs first each round. Recorded latency
covers request execution through the complete HTTP body. Request construction,
JSON decoding and correctness checking are outside the timer. Raw nanosecond
samples, nearest-rank p50/p95/p99, minima/maxima and response sizes are retained.
Fifty samples give only a coarse tail estimate; use `--iterations` for longer
runs. The client crosses the same VM port-forwarding path for every engine.

Engine caching defaults remain enabled. These are warmed repeated-query
measurements, including any upstream result cache; they do not isolate engine
execution time or cold reads. Equal container quotas are resource ceilings,
not a promise that internal thread counts or indexing architectures match.

The report records startup, post-query and post-restart data-volume apparent
and allocated bytes. These include WAL, indexes, metadata and preallocation.
They exclude application binaries, image layers and the container VM. No
forced compaction or SQLite VACUUM changes the normal storage lifecycle.
PID 1 RSS and HWM come from Linux `/proc` after timing and exclude page cache;
HWM starts at the ingestion restart, not at initial ingestion. Container image
IDs, selected digests, reported versions, server arguments, runtime details
and the published artifact manifest accompany the measurements.
The base image records its supplied immutable reference and native image ID;
the three oracle images additionally verify their selected platform digests
from the manifest. Cached images of the matching architecture are reused.

Admission times use different wire formats and are **not durable ingestion
throughput**. Small-fixture allocated bytes are **not compression ratios**.
This benchmark does not establish sustained write rates, cold-cache latency,
multi-client scalability, high-cardinality limits, multi-node availability,
native histogram support, rich typed log parity, or trace/Jaeger performance.
