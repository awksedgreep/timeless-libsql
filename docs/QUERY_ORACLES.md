# Query semantic oracles

This document pins the external implementations used to judge query-language
compatibility. Query tests must never use a moving image tag, a floating source
branch, or an unrecorded local installation. The pins below are the Session 0
baseline selected on 2026-08-04.

## Pinned versions

| language | role | upstream release | source commit | immutable multi-platform image | Linux amd64 image |
|---|---|---|---|---|---|
| PromQL | primary PromQL oracle | Prometheus `v3.13.2` | `bb5dff00cf8fdfbf5c65e0531aa835fa238a43a2` | `docker.io/prom/prometheus@sha256:508729e0e2d18e11fd742a5a5ca70e557b940a93948c3c95fd0123a6fd538b69` | `sha256:1147c92841726a6fef55fe6124491d6f85480f8de204f7d420304ca5bbd0a8f7` |
| MetricsQL | MetricsQL-only compatibility oracle | VictoriaMetrics community `v1.148.0` | `d94a85a4059b22fd238a0d2516bcb3e9bfb54587` | `docker.io/victoriametrics/victoria-metrics@sha256:407013e902f9a0ba1d4b2d4c077c47bbaf917c893c52ff39b19efe83a654afda` | `sha256:62f3b30fd73e16cc3a2909e3a2339499f8f8c597c77b851aeaf3a95b0a419001` |
| LogsQL | LogsQL oracle | VictoriaLogs `v1.52.0` | `46a54c976fa3d404396050e8a5ee6c5b0320efc5` | `docker.io/victoriametrics/victoria-logs@sha256:47b820890d64c4575a2a0a46415dcd8a4fd59a0f1fcd6a377693d7aea639442e` | `sha256:8f2140dca110705916751b9cdf57c2309555b6f1cf2707be1ee1a774c8c1e1f9` |

The VictoriaMetrics `v1.136.14` release visible when these pins were selected
is an enterprise LTS patch line and has no matching community image. It is not
the MetricsQL oracle. `v1.148.0` is the newest community release available at
the baseline date.

Source tag objects are also recorded so a future audit can distinguish an
annotated tag from its peeled commit:

| release | annotated tag object |
|---|---|
| Prometheus `v3.13.2` | `d08db18ac8e5eb1e30f941446ef954a44f510986` |
| VictoriaMetrics `v1.148.0` | `8509388b22920ec1e62949f54f11d30feb6c7170` |
| VictoriaLogs `v1.52.0` | `b753d73a38e3a779b35dc82e5f7d0e2bed5ec6fb` |

## Compatibility policy

Prometheus decides stable PromQL behavior. VictoriaMetrics is consulted for
PromQL differential coverage, but a disagreement does not silently redefine a
PromQL row. The affected row must record whether Timeless follows Prometheus,
intentionally offers a separately named MetricsQL behavior, or defers the
construct. MetricsQL rows are implemented only after the applicable stable
PromQL rows pass.

VictoriaLogs decides LogsQL behavior where its public language defines the
construct. The earlier `TimelessLogs.LogsQL` and DDNet-oriented tests decide
which P0 compatibility behaviors must be restored, but their silent-ignore
behavior is explicitly excluded: malformed and unsupported syntax must fail.

An oracle result is evidence, not a substitute for a Timeless regression. Each
shipped row must exercise the real `timeless-libsql` extension and compare the
applicable values, timestamps, labels or fields, types, ordering, result type,
and error classification. An upstream bug or intentional divergence is pinned
as a fixture and explained in the matrix row.

## Reproduction contract

Oracle harnesses must:

1. start the image by the immutable multi-platform digest above;
2. record the selected platform manifest and reported build version;
3. load a deterministic fixture with an explicit evaluation clock;
4. wait for durable ingestion before querying;
5. serialize requests, expected results, and normalized responses into the
   repository test fixture;
6. distinguish unordered language results from promised deterministic order;
7. preserve `NaN`, infinities, signed zero, missing/null/empty fields, and
   timestamp units rather than normalizing them away; and
8. stop and remove only the harness-owned container and temporary data.

Network access is needed only when deliberately refreshing an oracle fixture.
Normal CI runs the checked-in fixture against the real Timeless extension and
does not depend on an external service. Updating any pin is its own reviewed
query session: record upstream release notes, regenerate the affected fixtures,
run every prior oracle regression, and update the matrices in the same commit.

Validate the machine-readable pins without network access:

```bash
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- \
  oracle validate
```

When deliberately refreshing oracle evidence, probe the three immutable
containers and execute the baseline Prometheus fixture:

```bash
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle probe
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle prometheus-smoke
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle prometheus-api
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle victoria-metrics-api
cargo run --quiet --manifest-path tools/query-harness/Cargo.toml --locked -- oracle victoria-logs-api
```

The manifest is
[`tests/query_oracles/manifest.json`](../tests/query_oracles/manifest.json).
The rule smoke fixture pins selector and `avg_over_time` sample semantics. The
row-addressed API fixture pins result types and exact HTTP error envelopes; the
refresh command starts and removes only its uniquely named temporary
Prometheus container. For lookback semantics, the harness encodes one
dependency-free protobuf/raw-Snappy Remote Write sample, waits for its `204`
durability response, then tests the exact boundary, millisecond inclusion, and
zero/default behavior against that real series. Later sessions extend these
fixtures before implementation. Multi-element vector fixtures compare by
complete label/sample identity with `result_order: "unordered"` by default.
They use `result_order: "ordered"` only for an operator such as an instant
`topk` whose output order is part of the language contract.

The VictoriaLogs API fixture ingests deterministic messages into uniquely
named streams around one second-aligned server-owned evaluation instant, waits
until every row is query-visible, and pins normalized case identities plus
exact status and content-type classifications. It covers relative time,
RFC3339 open/closed and comparison bounds, integer Unix seconds, milliseconds,
microseconds, and nanoseconds, deterministic explicit sorting/pagination,
case-sensitive phrase bytes, Unicode word boundaries, literal decoding,
quoted field identifiers, and malformed query envelopes without treating
VictoriaLogs' unspecified default row order as a contract. Time placeholders
are resolved by the Rust harness after the container starts, so the checked
fixture remains deterministic without relying on expired absolute timestamps.

The VictoriaMetrics API fixture Remote Writes a deterministic one-second
series, then evaluates MetricsQL-only cases with explicit range-query steps.
It pins `Ni` lookbehind and subquery durations against the request step,
including millisecond steps, and records the exact Prometheus rejection of the
same syntax separately. This oracle is used only for rows explicitly assigned
to the MetricsQL compatibility tier. The first fixture contains five success
cases and one explicit syntax error for `MQL-09`.

Session 14 also pins Prometheus 3 quoted UTF-8 metric and label names, comments
and source positions, and classic-bucket `histogram_fraction` grouping and
interpolation. Its classification cases prove that query-context
`start`/`end`/`step`/`range` and `min_of`/`max_of` require
`promql-duration-expr`, vector-first `histogram_quantiles` requires
`promql-experimental-functions`, and `start_timestamp` is unknown to
Prometheus. These are explicit experimental or MetricsQL rows; the stable
Timeless endpoint must not enable them merely because the parser recognizes a
similar construct.

The following Timeless compatibility choices intentionally differ from the
pinned VictoriaLogs wire/storage model and are asserted on both sides rather
than hidden:

- VictoriaLogs returns parser and unsupported-pipe failures as HTTP 400
  `text/plain`. Timeless returns stable JSON, using HTTP 400
  `invalid_query/malformed_logsql` for malformed input and HTTP 422
  `unsupported_capability/unsupported_logsql` for syntax it recognizes but
  does not implement. This preserves the product requirement that unsupported
  behavior never silently broadens or falls back.
- VictoriaLogs serializes `stats count() as total` as a JSON string. The
  established Timeless/DDNet contract returns the exact count as a JSON number;
  direct SQLite/libSQL users receive the same INTEGER from
  `timeless_log_count`.
- VictoriaLogs flattens stored field values to strings for discovery,
  projection, and statistics, collapses missing/null/empty states, coerces
  numeric-looking strings, and may round large integers through binary64.
  Timeless instead preserves its retained rich JSON types, exact integer
  identity, and missing/null/empty distinctions. The matrix records the exact
  result envelope for each affected row.
- VictoriaLogs exposes synthetic `_stream` and `_stream_id` fields.
  Timeless does not invent them because the retained storage model has no
  declared stream identity; stream filters and mutations remain deferred.
- VictoriaLogs `values` emits a flattened string. Timeless emits the lossless
  JSON object `{\"items\":[...],\"missing\":N}`, which is the only supported
  response shape that distinguishes an absent field from a stored null while
  preserving array/object values.

`QSF-063` and `QSF-076` through `QSF-080` record these selected compatibility
behaviors. The fixture now contains 57 row-query cases, six error cases, and
twelve statistics/pipeline cases (75 total). Phrase, escape, identifier,
filtering, ordering, cardinality, pipeline-order, limit-zero, and rate-window
semantics remain exact to the pinned oracle where the retained Timeless
storage model applies.
