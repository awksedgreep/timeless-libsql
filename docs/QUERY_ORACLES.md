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
python3 tools/query_oracles.py validate
```

When deliberately refreshing oracle evidence, probe the three immutable
containers and execute the baseline Prometheus fixture:

```bash
python3 tools/query_oracles.py probe
python3 tools/query_oracles.py prometheus-smoke
python3 tools/query_oracles.py prometheus-api
```

The manifest is
[`tests/query_oracles/manifest.json`](../tests/query_oracles/manifest.json).
The rule smoke fixture pins selector and `avg_over_time` sample semantics. The
row-addressed API fixture pins result types and exact HTTP error envelopes; the
refresh command starts and removes only its uniquely named temporary
Prometheus container. Later sessions extend these fixtures before
implementation.
