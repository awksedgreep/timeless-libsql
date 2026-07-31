# The query cookbook

Recipes for the query surface: the raw vtabs, the kernel TVFs
(`timeless_grid`, `timeless_window`, `timeless_rollup`), the bucket
TVFs, and the catalog TVFs. **Every recipe here is executed by
`tests/cli.sh` §33** against a fixed dataset with hand-verified
expected output — if a recipe rots, the suite fails.

Conventions used throughout:

- `ts` is in the table's native unit (`timeless_metrics` = epoch
  **seconds**, logs = ms, traces = ns). The kernels are unit-agnostic:
  `step`, `lookback`, and `window` are in the same unit as `ts`.
- All kernel windows are half-open **`(t − width, t]`**: a sample
  exactly at `t` counts, a sample exactly at `t − width` does not.
- Results are **sparse by default** — grid points with no sample in
  the window produce no row. See [Gap-fill](#gap-fill) to change that.
- Counter kernels are **NOT PromQL**: no extrapolation, no lookback
  defaults, no staleness inference. Exact-percentile kernels are the
  flip side: raw samples are kept, so `p95` is exact, not an
  `le`-bucket estimate.

## The dashboard patterns, one per TVF

```sql
-- instant-selector shape: last sample per grid point, per series
SELECT labels, ts, value
  FROM timeless_grid('metrics', 'cpu_usage', NULL, :t0, :t1, 60, 90);

-- range-vector shape: sliding-window op per grid point
--   folds sum|min|max|count|avg, counters delta|increase|rate,
--   exact percentiles pNN, trimmed mean tavg:N
SELECT labels, ts, value
  FROM timeless_window('metrics', 'requests_total', NULL, :t0, :t1, 60, 300, 'rate');

-- pre-aggregated tier read (declared via rollups='60@0' on the vtab)
SELECT labels, ts, value
  FROM timeless_rollup('metrics', 'cpu_usage', NULL, 60, :t0, :t1, 'avg');

-- label filters: plain string = equality; {"neq"|"re"|"nre": ...} match
-- against the whole value (anchored); absent label matches as ""
SELECT labels, ts, value FROM timeless_grid('metrics', 'cpu_usage',
  '{"host": {"re": "web-.*"}, "env": {"neq": "dev"}}', :t0, :t1, 60, 90);

-- discovery: what metrics/series/labels exist? (no chunk reads)
SELECT * FROM timeless_series('metrics');
SELECT value FROM timeless_label_values('metrics', 'cpu_usage', 'host');
SELECT * FROM timeless_stats('metrics');

-- logs/traces frequency + latency dashboards
SELECT bucket_ts, group_key, n
  FROM timeless_log_buckets('logs', 'level', NULL, :t0, :t1, 60000);
SELECT bucket_ts, service, n, dur_p50, dur_p95, dur_p99
  FROM timeless_trace_buckets('traces', NULL, :t0, :t1, 60000000000);
```

## Gap-fill

Charting libraries want dense grids. Two ways to get one:

**Native (preferred):** the optional trailing `fill` argument on
`timeless_grid` and `timeless_window` — `'none'` (default) or
`'null'`:

```sql
-- every grid point emitted per matched series; value is NULL where
-- the lookback window is empty
SELECT labels, ts, value
  FROM timeless_grid('metrics', 'cpu_usage', NULL, :t0, :t1, 60, 90, 'null');
```

The per-series absence rule still holds: a series with **no** points on
the grid at all emits nothing, filled or not (matching the waist's
`query_multi` omission rule). Gap-fill is presentation mechanics only —
which points *have* values is decided by the same kernel either way.

**Portable SQL alternative** (single series; also useful for
right-edge padding beyond the data):

```sql
SELECT gs.value AS ts, g.value
  FROM generate_series(:t0, :t1, 60) gs
  LEFT JOIN timeless_grid('metrics', 'cpu_usage', '{"host":"web-1"}',
                          :t0, :t1, 60, 90) g
    ON g.ts = gs.value;
```

## Reset-corrected counter rate in pure SQL

When you need counter math over the **raw vtab** (e.g. a range that
mixes filters the kernels don't express), the standard reset-adjustment
rule in window functions:

```sql
WITH s AS (
  SELECT ts, value,
         LAG(value) OVER (PARTITION BY labels ORDER BY ts) AS prev
    FROM metrics
   WHERE name = 'requests_total' AND ts > :t0 AND ts <= :t1
)
SELECT SUM(CASE WHEN prev IS NULL      THEN 0            -- first sample: no step
                 WHEN value >= prev     THEN value - prev -- monotone step
                 ELSE value END) AS increase              -- reset: counter restarted
  FROM s;
-- rate = increase / (:t1 - :t0)
```

This computes exactly what `timeless_window(..., 'increase')` computes
over the window `(:t0, :t1]` — §33 asserts the two agree. **Prefer the
kernel** when the shape fits: it decompresses once in the engine and
ships grid points, not raw samples, and it's the form that stays fast
over sqld/HTTP.

## Top-k per bucket

"Top 2 hosts by average CPU per minute" — `ROW_NUMBER` over a bucketed
aggregate (works on the raw vtab; substitute a `timeless_window` call
as the inner query for big ranges):

```sql
WITH b AS (
  SELECT labels, (ts / 60) * 60 AS bucket_ts, AVG(value) AS v
    FROM metrics
   WHERE name = 'cpu_usage' AND ts >= :t0 AND ts <= :t1
   GROUP BY labels, bucket_ts
),
r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY bucket_ts ORDER BY v DESC) AS rn
    FROM b
)
SELECT bucket_ts, labels, v FROM r WHERE rn <= 2
 ORDER BY bucket_ts, rn;
```

## Cross-metric joins

Error ratio = two kernel calls joined on `(labels, ts)` — grids from
the same `(start, stop, step)` land on identical grid points, which is
what makes this join safe:

```sql
SELECT e.ts, e.labels, e.value / r.value AS error_ratio
  FROM timeless_grid('metrics', 'errors_total',   NULL, :t0, :t1, 60, 90) e
  JOIN timeless_grid('metrics', 'requests_total', NULL, :t0, :t1, 60, 90) r
    ON r.labels = e.labels AND r.ts = e.ts;
```

(`labels` is canonical JSON — sorted keys, minimal escaping — so string
equality is label-set equality.)

## Outlier exclusion, explicitly

The engine never decides what an outlier is; you say so in SQL. Three
escalating options:

**Trimmed mean (kernel):** drop a fixed fraction from each tail —
`timeless_window(..., 'tavg:5')`.

**IQR fences (Tukey):** quartiles from the exact-percentile kernel, cut
raw samples outside `[q1 − 1.5·IQR, q3 + 1.5·IQR]`:

```sql
WITH fences AS (
  SELECT (SELECT value FROM timeless_window('metrics', 'latency', NULL,
                                            :t1, :t1, 1, :t1 - :t0, 'p25')) AS q1,
         (SELECT value FROM timeless_window('metrics', 'latency', NULL,
                                            :t1, :t1, 1, :t1 - :t0, 'p75')) AS q3
)
SELECT AVG(value) AS robust_avg
  FROM metrics, fences
 WHERE name = 'latency' AND ts > :t0 AND ts <= :t1
   AND value BETWEEN q1 - 1.5 * (q3 - q1) AND q3 + 1.5 * (q3 - q1);
```

**σ-based (2-sigma):** population stddev in plain SQL:

```sql
WITH stats AS (
  SELECT AVG(value) AS mu,
         sqrt(AVG(value * value) - AVG(value) * AVG(value)) AS sigma
    FROM metrics WHERE name = 'latency' AND ts > :t0 AND ts <= :t1
)
SELECT AVG(value) AS robust_avg
  FROM metrics, stats
 WHERE name = 'latency' AND ts > :t0 AND ts <= :t1
   AND ABS(value - mu) <= 2 * sigma;
```

Caveat worth knowing: with tiny samples a single huge outlier inflates
σ enough to mask itself — IQR fences and `tavg:N` are the sturdier
tools there.
