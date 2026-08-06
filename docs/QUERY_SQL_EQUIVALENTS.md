# SQL equivalents for query-language features

This cookbook shows how a direct SQLite/libSQL user can execute the storage
and composition work behind PromQL and LogsQL query vectors. It accompanies
the [PromQL](PROMQL_FEATURE_MATRIX.md) and
[LogsQL](LOGSQL_FEATURE_MATRIX.md) matrices.

An `SQL` foundation in a matrix is not complete documentation until this file
contains an executable, parameterized statement for it. If no honest SQL
equivalent exists, the row must target `API`, `EXT`, `LIB`, or `DEFER` instead
of hand-waving at SQL.

## Contract for every recipe

Each recipe must:

1. use only public virtual tables, TVFs, scalar functions, and ordinary
   SQLite/libSQL features—never shadow tables;
2. include required table setup and named parameter units;
3. state whether it is semantically exact, an execution foundation whose API
   still owes language/result shaping, or an intentionally different SQL
   operation;
4. state ordering, bounds, missing-value, and type behavior;
5. name its matrix row IDs and its executable regression in the Rust
   `timeless-query-harness` or an extension-backed server contract; and
6. show `EXPLAIN QUERY PLAN` or measured counters when a claim depends on
   pushdown rather than ordinary row filtering.

The Rust API still owns Prometheus/Victoria HTTP envelopes, language errors,
lookback/staleness policy, output label/name rules, pipeline semantics,
resource limits, and cancellation. SQL recipes let embedded users reach the
same stored data and mechanical reductions without running that API.

PromQL scalar literals are intentionally not labeled `SQL`. A finite value can
of course be bound with `SELECT CAST(:value AS REAL)`, but SQLite commonly
normalizes IEEE NaN to SQL NULL and does not define Prometheus's `"NaN"`,
`"+Inf"`, and `"-Inf"` response strings or evaluation timestamps. Claiming
that statement as an exact `PQL-S11` recipe would be misleading; those
language/value-envelope semantics belong to the Rust API.

## Recipe index

| recipe | matrix rows | state | semantic class |
|---|---|---|---|
| [`SQL-PROM-001`](#sql-prom-001-instant-selector) | `PQL-S01`, `PQL-S02` | current | exact storage selection; API shapes PromQL output |
| [`SQL-PROM-002`](#sql-prom-002-avg_over_time) | `PQL-S06`, `PQL-R01` | current | exact float-window reduction |
| [`SQL-PROM-003`](#sql-prom-003-cross-series-sum-by-label) | `PQL-O09` | current foundation | exact bounded cross-series sum; API owns grouping syntax, labels, IEEE strings, limits, and envelopes |
| [`SQL-PROM-004`](#sql-prom-004-vector-arithmetic-with-label-matching) | `PQL-O02`, `PQL-O05` | current foundation | vector/scalar arithmetic and exact-label joins; API owns language, cardinality, labels, IEEE strings, and envelopes |
| [`SQL-PROM-005`](#sql-prom-005-top-k-per-evaluation-step) | `PQL-O14` | current foundation | per-step top/bottom ranking; API owns language, grouping modifiers, original labels, parameter errors, limits, and envelopes |
| [`SQL-PROM-006`](#sql-prom-006-range-selector) | `PQL-S06` | current | exact root range-vector storage selection; API shapes the matrix |
| [`SQL-PROM-007`](#sql-prom-007-bounded-packed-storage-work) | `PQL-S20` | current foundation | exact pre-decode work bounds; API owns language/result/deadline limits |
| [`SQL-PROM-008`](#sql-prom-008-temporal-selector-modifiers) | `PQL-S07`, `PQL-S08` | current foundation | exact shifted/fixed lookup time; API owns parser and outer query context |
| [`SQL-PROM-009`](#sql-prom-009-aligned-selector-subquery) | `PQL-S09` | current foundation | exact open-left global subquery grid for a stored selector; API owns arbitrary inner expressions and range consumption |
| [`SQL-PROM-010`](#sql-prom-010-unary-minus) | `PQL-O01` | current foundation | exact numeric negation over a bounded public grid; API owns types, envelopes, metric-name policy, limits, and cancellation |
| [`SQL-PROM-011`](#sql-prom-011-comparison-filter-and-bool) | `PQL-O03` | current foundation | exact SQLite predicate/CASE over public grids; API owns AST types, name policy, matching, limits, and envelopes |
| [`SQL-PROM-012`](#sql-prom-012-set-membership) | `PQL-O04` | current foundation | exact step-local many-to-many membership over public grids; API owns language, names, bounds, limits, and envelopes |
| [`SQL-PROM-013`](#sql-prom-013-on-and-ignoring-label-matching) | `PQL-O06` | current foundation | explicit JSON-label projection/equality over public grids; API owns AST/cardinality/name/error semantics |
| [`SQL-PROM-014`](#sql-prom-014-group_left-and-group_right) | `PQL-O07` | current foundation | explicit many/one grid join and label copy; API owns uniqueness failures, name/value direction, limits, and envelopes |
| [`SQL-PROM-015`](#sql-prom-015-cross-series-average-by-label) | `PQL-O10` | current foundation | bounded cross-series average; API owns compensated arithmetic, grouping syntax, labels, limits, and envelopes |
| [`SQL-PROM-016`](#sql-prom-016-cross-series-minimum-and-maximum) | `PQL-O11` | current foundation | bounded cross-series extrema; API owns all-NaN behavior, grouping syntax, labels, limits, and envelopes |
| [`SQL-PROM-017`](#sql-prom-017-cross-series-count-and-group) | `PQL-O12` | current foundation | bounded cross-series row count/presence; API owns grouping syntax, labels, limits, and envelopes |
| [`SQL-PROM-018`](#sql-prom-018-cross-series-population-variance-and-standard-deviation) | `PQL-O13` | current foundation | bounded population second moment; API owns Welford/IEEE arithmetic, language, labels, limits, and envelopes |
| [`SQL-PROM-019`](#sql-prom-019-cross-series-quantile) | `PQL-O15` | current foundation | bounded finite-value linear interpolation; API owns language, raw-NaN rank, parameters, labels, limits, and envelopes |
| [`SQL-PROM-020`](#sql-prom-020-count-series-by-sample-value) | `PQL-O16` | current foundation | exact bounded grouping by raw SQL numeric value; API owns Prometheus label formatting, grouping syntax, raw NaN, limits, and envelopes |
| [`SQL-PROM-021`](#sql-prom-021-min_over_time) | `PQL-R02` | current | exact float-window minimum |
| [`SQL-PROM-022`](#sql-prom-022-max_over_time) | `PQL-R03` | current | exact float-window maximum |
| [`SQL-PROM-023`](#sql-prom-023-sum_over_time) | `PQL-R04` | current | compensated float-window sum |
| [`SQL-PROM-024`](#sql-prom-024-count_over_time) | `PQL-R05` | current | exact float-sample window count |
| [`SQL-PROM-025`](#sql-prom-025-last_over_time) | `PQL-R06` | current | exact last stored float in each window |
| [`SQL-PROM-026`](#sql-prom-026-present_over_time) | `PQL-R08` | current | exact non-empty float-window presence |
| [`SQL-PROM-027`](#sql-prom-027-quantile_over_time) | `PQL-R09` | current foundation | exact finite-value linear interpolation per float window; API owns raw IEEE edge semantics |
| [`SQL-PROM-028`](#sql-prom-028-stddev_over_time-and-stdvar_over_time) | `PQL-R10`, `PQL-R11` | current foundation | finite-value population Welford deviation or variance per float window; API owns raw IEEE edge semantics |
| [`SQL-PROM-029`](#sql-prom-029-rate) | `PQL-R12` | current foundation | finite float-counter reset correction and Prometheus edge extrapolation; API owns language, special values, labels, limits, and envelopes |
| [`SQL-PROM-030`](#sql-prom-030-irate) | `PQL-R13` | current foundation | finite float-counter last-two-sample reset substitution and actual-interval rate; API owns language, special values, labels, limits, and envelopes |
| [`SQL-PROM-031`](#sql-prom-031-increase) | `PQL-R14` | current foundation | finite float-counter reset correction and Prometheus edge extrapolation without per-second normalization; API owns language, special values, labels, limits, and envelopes |
| [`SQL-PROM-032`](#sql-prom-032-delta) | `PQL-R15` | current foundation | finite float-gauge difference with Prometheus edge extrapolation and no counter correction; API owns language, special values, labels, limits, and envelopes |
| [`SQL-PROM-033`](#sql-prom-033-idelta) | `PQL-R16` | current foundation | finite float-gauge last-two-sample difference without extrapolation or time normalization; API owns language, special values, labels, limits, and envelopes |
| [`SQL-PROM-034`](#sql-prom-034-deriv) | `PQL-R17` | current foundation | finite float-gauge least-squares slope over timestamp-centered public raw rows; API owns compensated/IEEE arithmetic, language, labels, limits, and envelopes |
| [`SQL-PROM-035`](#sql-prom-035-predict_linear) | `PQL-R18` | current foundation | finite float-gauge least-squares forecast relative to each evaluation timestamp; API owns scalar-expression and compensated/IEEE semantics, labels, limits, and envelopes |
| [`SQL-PROM-036`](#sql-prom-036-changes) | `PQL-R19` | current | exact ordered float-transition count, including row-projected NaN, infinity, signed zero, and singleton semantics; API owns language, labels, limits, and envelopes |
| [`SQL-PROM-037`](#sql-prom-037-resets) | `PQL-R20` | current | exact ordered strict float-decrease count, including row-projected NaN, infinity, signed zero, and singleton semantics; API owns language, labels, limits, and envelopes |
| [`SQL-PROM-038`](#sql-prom-038-abs) | `PQL-F01` | current foundation | exact bounded absolute value for finite floats, infinities, and signed zero; API owns packed-NaN fidelity, language, names, limits, and envelopes |
| [`SQL-PROM-039`](#sql-prom-039-ceil-floor-and-round) | `PQL-F02` | current foundation | exact bounded IEEE ceiling/floor and Prometheus nearest-multiple arithmetic for row-visible values; API owns packed-NaN fidelity, scalar ASTs, names, limits, and envelopes |
| [`SQL-PROM-040`](#sql-prom-040-clamp-clamp_min-and-clamp_max) | `PQL-F03` | current foundation | bounded finite-value clamping and inverted-bound omission; API owns packed IEEE fidelity, scalar ASTs, names, limits, and envelopes |
| [`SQL-PROM-041`](#sql-prom-041-sqrt-exp-ln-log2-and-log10) | `PQL-F04` | current foundation | bounded SQLite math transforms for valid row-visible domains; API owns packed IEEE/domain results, names, limits, and envelopes |
| [`SQL-PROM-042`](#sql-prom-042-sgn) | `PQL-F05` | current foundation | exact bounded sign mapping for every row-visible finite value, infinity, and signed zero; API owns packed NaN, names, limits, and envelopes |
| [`SQL-PROM-043`](#sql-prom-043-inverse-trigonometric-and-hyperbolic-functions) | `PQL-F06` | current foundation | bounded SQLite inverse math transforms over valid row-visible domains; API owns packed IEEE/domain results, names, limits, and envelopes |
| [`SQL-PROM-044`](#sql-prom-044-trigonometric-and-hyperbolic-functions) | `PQL-F07` | current foundation | bounded SQLite trigonometric and hyperbolic transforms over valid row-visible domains; API owns packed IEEE/domain results, names, limits, and envelopes |
| [`SQL-PROM-045`](#sql-prom-045-deg-rad-and-pi) | `PQL-F08` | current foundation | bounded degree/radian conversion plus scalar pi through standard SQLite math; API owns packed NaN, names, types, limits, and envelopes |
| [`SQL-PROM-046`](#sql-prom-046-label_join) | `PQL-F10` | current foundation | ordered arbitrary-arity label joining over public JSON labels; API owns language parsing, names, limits, cancellation, and envelopes |
| [`SQL-PROM-047`](#sql-prom-047-absent) | `PQL-F11` | current foundation | exact step-local absence over a bounded public grid; API owns selector-derived labels, AST composition, limits, cancellation, and envelopes |
| [`SQL-PROM-048`](#sql-prom-048-absent_over_time) | `PQL-F12` | current foundation | exact step-local absence over public raw windows; API owns selector-derived labels and subquery composition |
| [`SQL-PROM-049`](#sql-prom-049-sort-and-sort_desc) | `PQL-F13` | current foundation | exact row-visible instant ordering; API owns packed NaN fidelity and range response ordering |
| [`SQL-PROM-050`](#sql-prom-050-scalar-and-vector) | `PQL-F15` | current foundation | exact per-step cardinality and nameless-vector composition; API owns packed NaN and result types |
| [`SQL-PROM-051`](#sql-prom-051-time-and-timestamp) | `PQL-F16` | current foundation | exact evaluation grid and stored timestamps; API owns AST sample provenance and envelopes |
| [`SQL-PROM-052`](#sql-prom-052-minute-hour-day_of_week-and-day_of_month) | `PQL-F17` | current foundation | UTC extraction for SQLite-representable Unix seconds; API owns packed IEEE and full-domain behavior |
| [`SQL-PROM-053`](#sql-prom-053-day_of_year-days_in_month-month-and-year) | `PQL-F18` | current foundation | UTC calendar extraction for SQLite-representable Unix seconds; API owns packed IEEE and full-domain behavior |
| [`SQL-PROM-054`](#sql-prom-054-histogram_quantile-over-classic-buckets) | `PQL-H01` | current foundation | bounded classic-bucket grouping, monotonic correction, and linear interpolation; API owns strict bound parsing, tolerance, annotations, IEEE, names, limits, and envelopes |
| [`SQL-PROM-055`](#sql-prom-055-atan2) | `PQL-O08` | current foundation | bounded SQLite `atan2(Y,X)` over scalar/vector or label-matched vectors; API owns Go-compatible last-bit rounding, types, names, matching errors, limits, and envelopes |
| [`SQL-PROM-056`](#sql-prom-056-histogram_fraction-over-classic-buckets) | `PQL-H02` | current foundation | bounded classic-bucket grouping and linear CDF interpolation; API owns strict bounds, scalar ASTs, IEEE values, names, limits, cancellation, and envelopes |
| [`SQL-MQL-001`](#sql-mql-001-default-if-and-ifnot) | `MQL-01` | current foundation | bounded gap filling and step-local label membership; API owns MetricsQL syntax, implicit scalar vectors, full label/name policy, limits, cancellation, and envelopes |
| [`SQL-MQL-002`](#sql-mql-002-keep_metric_names) | `MQL-02` | current foundation | carry the public metric-name column through ordinary SQL transforms; API owns modifier grammar, operation eligibility, name-aware matching, collisions, limits, cancellation, and envelopes |
| [`SQL-MQL-003`](#sql-mql-003-union-and-alias) | `MQL-03` | current foundation | public-grid `UNION ALL`, explicit metric-name projection, and first-branch labelset precedence; API owns grammar, scalar-vector conversion, duplicate-output errors, limits, cancellation, and envelopes |
| [`SQL-MQL-004`](#sql-mql-004-label_set-and-label_del) | `MQL-04` | current foundation | ordinary JSON label projection and separate metric-name projection; API owns transform grammar, scalar vectorization, collisions, limits, cancellation, and envelopes |
| [`SQL-MQL-005`](#sql-mql-005-default_rollup-and-window-less-rollups) | `MQL-05` | current foundation | automatic finite-series last-sample window and step-sized public window reductions; API owns packed stale/NaN fidelity, carry-in/reset semantics, language, limits, cancellation, and envelopes |
| [`SQL-MQL-006`](#sql-mql-006-range-aggregates) | `MQL-06` | current foundation | slot-indexed full-grid average, minimum, maximum, or sum over a bounded public input grid; API owns arbitrary expression composition, implicit windows, duplicate outputs, limits, cancellation, and envelopes |
| [`SQL-MQL-007`](#sql-mql-007-running-aggregates) | `MQL-07` | current foundation | slot-indexed cumulative average, minimum, maximum, or sum over a bounded public input grid; API owns arbitrary expression composition, packed missing/NaN semantics, collisions, limits, cancellation, and envelopes |
| [`SQL-MQL-009`](#sql-mql-009-request-step-relative-durations) | `MQL-09` | current foundation | exact request-step multiplication for public windows, subquery timing, and signed offsets; API owns MetricsQL duration grammar, millisecond composition, saturation, limits, cancellation, and envelopes |
| [`SQL-MQL-010`](#sql-mql-010-query-context-values) | `MQL-10` | current foundation | exact request start, end, and step projection in seconds over a bounded evaluation grid; API owns MetricsQL grammar, scalar/vector composition, limits, cancellation, and envelopes |
| [`SQL-MQL-012`](#sql-mql-012-histogram_quantiles) | `MQL-12` | current foundation | bounded multi-quantile evaluation over public cumulative classic buckets with VictoriaMetrics missing-`+Inf`, monotonic-repair, and interpolation rules; API owns MetricsQL/vmrange grammar, exact float labels and IEEE bits, collisions, limits, cancellation, and envelopes |
| [`SQL-LOG-001`](#sql-log-001-bounded-filter-sort-and-pagination) | `LQL-F01`, `LQL-F02`, `LQL-F06`, `LQL-F07`, `LQL-P01`, `LQL-P02`, `LQL-P03` | current foundation | exact row query for declared index keys |
| [`SQL-LOG-002`](#sql-log-002-message-substring) | `LQL-F12` | current foundation | exact Timeless case-insensitive substring, not LogsQL word or phrase semantics |
| [`SQL-LOG-003`](#sql-log-003-exact-count) | `LQL-P09`, `LQL-S01` | current | exact scalar count without row materialization |
| [`SQL-LOG-004`](#sql-log-004-distinct-field-values) | `LQL-P04`, `LQL-S03`, `LQL-S04` | current foundation | bounded lexical values; aggregate syntax remains API work |
| [`SQL-LOG-005`](#sql-log-005-arbitrary-metadata-equality) | `LQL-F05` | current | exact typed decoded fallback for a non-indexed field; `json_type` distinguishes missing, null, and empty |
| [`SQL-LOG-006`](#sql-log-006-counts-by-field-and-time-bucket) | `LQL-P09`, `LQL-S05`, `LQL-S08` | current foundation | storage bucket vector or ordinary SQL grouping |
| [`SQL-LOG-007`](#sql-log-007-case-sensitive-message-substring) | `LQL-F12` | current foundation | exact case-sensitive literal substring over bounded public rows |
| [`SQL-LOG-008`](#sql-log-008-exact-empty-and-any-value-predicates) | `LQL-F15`, `LQL-F18`, `LQL-F19` | current foundation | full-message exactness and explicit missing/null/empty/non-empty typed JSON states |
| [`SQL-LOG-009`](#sql-log-009-boolean-composition) | `LQL-F31` | current foundation | ordinary SQL `AND`/`OR`/`NOT` precedence over SQL-expressible public-row atoms |
| [`SQL-LOG-010`](#sql-log-010-field-names-and-typed-projection) | `LQL-P05`, `LQL-P06`, `LQL-Q02` | current foundation | bounded top-level field discovery and typed projection over public metadata JSON |
| [`SQL-LOG-011`](#sql-log-011-current-row-filter-and-empty-counts) | `LQL-P08`, `LQL-S02` | current foundation | bounded current-row any-value filter plus exact missing/null/empty counts |
| [`SQL-LOG-012`](#sql-log-012-typed-unique-values-and-counts) | `LQL-P04`, `LQL-S03`, `LQL-S04` | current foundation | type-tagged exact unique counts, values, hits, and ordered presence states |
| [`SQL-LOG-013`](#sql-log-013-numeric-aggregates-median-and-rates) | `LQL-S05`, `LQL-S06`, `LQL-S08` | current foundation | numeric-only ordinary SQL aggregates, median, and explicit-window rates |
| [`SQL-LOG-014`](#sql-log-014-exact-prefix) | `LQL-F16` | current foundation | exact case-sensitive start-of-message and retained-text-field prefixes; API owns rich-value textual projection |
| [`SQL-LOG-015`](#sql-log-015-static-multi-exact-membership) | `LQL-F17` | current foundation | case-sensitive message and retained-text membership with one bound parameter per value; API owns rich-value projection and static-list grammar |
| [`SQL-LOG-016`](#sql-log-016-field-no-op) | `LQL-F20` | current foundation | exact field-independent true predicate; API owns wildcard-function grammar and composition |
| [`SQL-LOG-017`](#sql-log-017-json-array-primitive-membership) | `LQL-F23` | current foundation | exact primitive membership in a retained JSON array through public JSON1 rows; API owns function grammar, composition, and semantic-JSON compatibility policy |
| [`SQL-LOG-018`](#sql-log-018-ipv4-range-over-retained-strings) | `LQL-F25` | current foundation | exact whole-string IPv4 membership between inclusive packed address bounds; API owns address/CIDR grammar, composition, limits, cancellation, and envelopes |
| [`SQL-LOG-019`](#sql-log-019-bytewise-string-range-over-retained-text) | `LQL-F27` | current foundation | lower-inclusive/upper-exclusive bytewise range over retained text plus missing/null-as-empty; API owns rich-value projection, grammar, composition, limits, cancellation, and envelopes |
| [`SQL-LOG-020`](#sql-log-020-unicode-codepoint-length-range-over-retained-text) | `LQL-F28` | current foundation | inclusive Unicode-codepoint length over retained text plus missing/null-as-empty; API owns rich-value projection, bound grammar, composition, limits, cancellation, and envelopes |
| [`SQL-LOG-021`](#sql-log-021-same-row-textual-field-comparison) | `LQL-F30` | current foundation | exact textual equality and explicit bytewise fallback ordering over two public-row fields; API owns VictoriaLogs math-value detection, `_time` rendering, grammar, composition, limits, cancellation, and envelopes |
| [`SQL-LOG-022`](#sql-log-022-prefix-selected-field-set) | `LQL-F32` | current foundation | row-local exact-string matching over canonical special fields and recursively flattened metadata leaf names selected by a literal prefix; API owns LogsQL filter semantics, rich projection, grammar, limits, cancellation, and envelopes |
| [`SQL-LOG-023`](#sql-log-023-utc-day-range-with-explicit-offset) | `LQL-F33` | current foundation | exact UTC time-of-day bracket filtering with an explicit fixed offset over native public timestamps; API owns LogsQL clock/duration grammar, deterministic default timezone, composition, limits, cancellation, and envelopes |
| [`SQL-LOG-024`](#sql-log-024-utc-week-range-with-explicit-offset) | `LQL-F34` | current foundation | exact Sunday-through-Saturday UTC weekday filtering with an explicit fixed offset over native public timestamps; API owns LogsQL weekday/bracket/duration grammar, deterministic default timezone, composition, limits, cancellation, and envelopes |
| [`SQL-LOG-025`](#sql-log-025-delete-exact-retained-metadata-fields) | `LQL-P07` | current foundation | exact deletion of retained metadata paths with public JSON1; API owns aliases, prefixes, special fields, empty-parent/row pruning, composition, limits, cancellation, and envelopes |
| [`SQL-LOG-026`](#sql-log-026-request-local-log-query-statistics) | `LQL-P12` | current foundation | single-use report for the immediately preceding successful public log scan on the same SQLite connection; API owns LogsQL syntax, complete logical predicates, pipeline duration, result strings, limits, cancellation, and envelopes |
| [`SQL-LOG-027`](#sql-log-027-first-numeric-rows-per-partition) | `LQL-P13` | current foundation | bounded numeric top-k per textual partition through a window rank; API owns LogsQL natural coercions, current-schema all-field order, rich paths, grammar, limits, cancellation, and envelopes |
| [`SQL-LOG-028`](#sql-log-028-last-numeric-rows-per-partition) | `LQL-P14` | current foundation | bounded reverse numeric top-k per textual partition through a window rank; API owns LogsQL direction inversion, natural coercions, current-schema all-field order, rich paths, grammar, limits, cancellation, and envelopes |
| [`SQL-LOG-029`](#sql-log-029-top-values-by-hit-count) | `LQL-P15` | current foundation | bounded frequency groups and deterministic string hits/rank over one public JSON path; API owns multi-field/current-row grammar, collision naming, limits, cancellation, and envelopes |
| [`SQL-LOG-030`](#sql-log-030-unique-textual-values) | `LQL-P16` | current foundation | bounded unique textual groups, optional case-sensitive filtering, deterministic limiting, and optional hits over one public JSON path; API owns multi-field/current-row grammar, omitted empty fields, collision naming, strict limits, cancellation, and envelopes |
| [`SQL-LOG-031`](#sql-log-031-bounded-facets-over-public-log-fields) | `LQL-P18` | current foundation | recursive rich-field flattening, textual nonempty frequencies, per-field limits, constant/high-cardinality/long-value exclusion, and deterministic ordering; API owns grammar, pipeline composition, hard state limits, cancellation, and envelopes |
| [`SQL-LOG-032`](#sql-log-032-first-nonempty-textual-log-field) | `LQL-P19` | current foundation | bounded first-nonempty textual selection across three exact public metadata paths with a default; API owns exact/all/prefix current-row expansion, destination mutation, conflicts, limits, cancellation, and envelopes |
| [`SQL-LOG-033`](#sql-log-033-copy-one-exact-retained-metadata-field) | `LQL-P20` | current foundation | typed copy of one exact retained metadata path to one exact top-level destination with missing/object-parent compatibility behavior; API owns pair grammar, flattened prefixes, sequential composition, nested destination conflicts, limits, cancellation, and envelopes |
| [`SQL-LOG-034`](#sql-log-034-rename-one-exact-top-level-retained-metadata-field) | `LQL-P21` | current foundation | typed move of one exact top-level retained metadata field to one exact top-level destination, including source removal and missing/object-parent compatibility behavior; API owns flattened prefixes, sequential composition, nested-parent pruning/conflicts, limits, cancellation, and envelopes |
| [`SQL-LOG-035`](#sql-log-035-format-two-exact-retained-metadata-fields) | `LQL-P22` | current foundation | bounded `printf` interpolation of two exact public metadata paths with explicit rich-value textual projection; API owns LogsQL patterns, codecs, conditions, destination mutation/preservation, limits, cancellation, and envelopes |
| [`SQL-LOG-036`](#sql-log-036-arithmetic-over-exact-retained-numeric-fields) | `LQL-P23` | current foundation | bounded row-local arithmetic over two exact typed numeric metadata paths; API owns LogsQL math grammar, broader coercions, functions, sequential destinations, fixed float rendering, limits, cancellation, and envelopes |
| [`SQL-LOG-037`](#sql-log-037-utf-8-byte-length-of-one-exact-retained-field) | `LQL-P24` | current foundation | UTF-8/compact-JSON byte length for one exact public metadata path with explicit missing/null/object-parent behavior; API owns LogsQL grammar, canonical fields, sequential destinations, rich conflicts, limits, cancellation, and envelopes |
| [`SQL-LOG-038`](#sql-log-038-drop-one-empty-retained-metadata-field) | `LQL-P28` | current foundation | typed removal of one exact public metadata path when it is JSON null or an empty string; API owns dynamic current-row traversal, recursive empty-parent/row pruning, canonical fields, limits, cancellation, and envelopes |
| [`SQL-LOG-039`](#sql-log-039-literal-replacement-in-one-exact-retained-field) | `LQL-P29` | current foundation | all-occurrence literal replacement over one exact public metadata path's textual projection; API owns LogsQL grammar, conditional/limited/sequential mutation, rich-value preservation, limits, cancellation, and envelopes |
| [`SQL-LOG-040`](#sql-log-040-two-literal-delimited-fields-from-one-exact-retained-field) | `LQL-P32` | current foundation | two unquoted captures from one exact public metadata path using fixed literal prefix/middle/suffix delimiters; API owns LogsQL patterns, quoted-string decoding, conditions, current-row mutation/preservation, limits, cancellation, and envelopes |
| [`SQL-LOG-041`](#sql-log-041-pack-selected-rich-metadata-fields-as-json) | `LQL-P34` | current foundation | bounded packing of a fixed list of exact public metadata paths into one typed nested JSON object; API owns current-row/canonical fields, prefix selection, destination mutation, limits, cancellation, and envelopes |
| [`SQL-LOG-042`](#sql-log-042-unpack-selected-rich-fields-from-a-json-object) | `LQL-P36` | current foundation | bounded unpacking of fixed exact paths from one public native-object or JSON-text metadata field into a typed nested JSON object; API owns LogsQL grammar, dynamic selection, current-row mutation/preservation, limits, cancellation, and envelopes |
| [`SQL-LOG-043`](#sql-log-043-top-level-json-array-length) | `LQL-P41` | current foundation | bounded top-level element count for one exact public native-array or JSON-array-text metadata path; API owns LogsQL grammar, current-row mutation, bare-`NaN` compatibility, limits, cancellation, and envelopes |
| [`SQL-LOG-044`](#sql-log-044-upper-step-numeric-quantile-and-population-standard-deviation) | `LQL-S07` | current foundation | bounded upper-step quantile and one-pass population deviation for one exact finite native-number path; API owns textual natural order, rich projection, grammar, state limits, cancellation, and envelopes |
| [`SQL-LOG-045`](#sql-log-045-summed-utf-8-byte-length-of-one-exact-field) | `LQL-S09` | current foundation | summed UTF-8/compact-JSON byte length for one exact public metadata path; API owns dynamic field selection, canonical fields, checked unsigned overflow, limits, cancellation, and envelopes |
| [`SQL-LOG-046`](#sql-log-046-deterministic-any-and-numeric-companion-field-extrema) | `LQL-S10` | current foundation | deterministic first nonempty rich JSON value plus finite native-number companion-field minima/maxima over exact public metadata paths; API owns complete natural ordering, canonical fields, typed results, state limits, cancellation, and envelopes |
| [`SQL-LOG-047`](#sql-log-047-deterministic-rich-row-selection-and-numeric-row-extrema) | `LQL-S11` | current foundation | deterministic first qualifying rich row plus finite native-number row minima/maxima for two fixed exact public metadata paths; API owns dynamic selectors, complete natural ordering, canonical fields, state limits, cancellation, and envelopes |
| [`SQL-LOG-048`](#sql-log-048-query-backed-exact-membership) | `LQL-F38` | current foundation | bounded two-scan exact membership for retained strings through public rows; API owns subquery grammar, rich projection, phrase variants, cumulative limits, cancellation, caching, and envelopes |
| [`SQL-LOG-049`](#sql-log-049-bounded-random-log-sample) | `LQL-P17` | current foundation | bounded independent `1/N` random selection over public log rows; API owns LogsQL unsigned grammar, exponential-gap compatibility, limits, cancellation, and envelopes |

`current` means the public SQL surface exists now. `reference` means the SQL
is executable now but the corresponding PromQL/LogsQL parser/evaluator row is
still correctly marked `missing`.

`LQL-F40` intentionally has no SQL recipe. Comments, multiline layout, and an
optional terminal semicolon are LogsQL source grammar owned by the Rust API;
direct SQLite/libSQL users already write ordinary parameterized SQL and do not
send LogsQL syntax to the extension. Each selected storage operation still
uses the applicable public-row recipe above. Claiming a storage SQL equivalent
for source preprocessing would conflate the two languages.

`LQL-F37` intentionally has no SQL recipe. `seq(...)` is an ordered,
non-overlapping phrase search with Unicode letter/digit/underscore boundary
rules. Core SQLite has no portable predicate for those Unicode categories, and
an `instr()` chain would silently accept attached word characters. Direct SQL
users may deliberately write such a byte-substring chain when that weaker
contract is wanted, but it is not an honest LogsQL equivalent. The matrix
therefore keeps this row `ROWS`/`API`, not `SQL` or `EXT`.

`LQL-P25` intentionally has no SQL recipe. VictoriaLogs `hash` is specifically
seed-zero xxHash64 masked to 53 bits and rendered as an exact decimal integer.
Core SQLite and libSQL expose no portable xxHash64 scalar, and an expression
assembled from signed SQL integers cannot preserve the required unsigned
wrapping arithmetic honestly. An application may register its own xxHash64
UDF and apply it to bounded public `logs` rows, but that is not a
`timeless-libsql` public SQL contract. The Rust logs API performs the bounded
row-local transform after the already-required public scan and decode. Exact
evidence must demonstrate a material avoidable storage or row-crossing cost
before this project adds an extension scalar solely for this pipe.

`LQL-P26` intentionally has no SQL recipe. VictoriaLogs `collapse_nums` uses
an exact ASCII token-boundary scanner, special version/time/duration/underscore
boundaries, even-length hexadecimal heuristics, and an ordered UUID/IPv4/time/
date/datetime prettification pass. Core SQLite and libSQL provide neither that
tokenizer nor an equivalent regular-expression replacement scalar. A recursive
character CTE or `replace()` chain would be substantially different and would
silently misclassify valid inputs, so neither is published as an equivalent.
Applications can compose a deliberately narrower transformation over bounded
public `logs` rows, but complete LogsQL semantics remain bounded Rust API work
after the required public scan and decode. Exact evidence must show avoidable
storage or row-crossing cost before adding a general extension primitive.

`LQL-F41` intentionally has no complete SQL recipe. Direct users can expand a
known phrase in their application and bind the resulting strings to an `IN`
predicate for `equals_common_case`, but core SQLite `upper()`/`lower()` do not
implement VictoriaLogs' Go-simple Unicode mapping. The
`contains_common_case` half also requires the same Unicode-category phrase
boundaries as `contains_any`; `LIKE`, `GLOB`, and `instr()` are not equivalent.
Because those operations happen after a required public-row decode, moving
them into an extension primitive would not avoid storage reads, decode,
allocation, copies, or row crossing. The row remains `ROWS`/`API`, not `SQL`
or `EXT`.

## Setup and parameter conventions

Examples assume the extension has been loaded and these tables exist:

```sql
CREATE VIRTUAL TABLE metrics USING timeless_metrics;
CREATE VIRTUAL TABLE logs USING timeless_logs(
  index_keys='service,host,path,status'
);
```

Metric timestamps, steps, windows, and lookback values are seconds. The
general-purpose `timeless_logs` table uses epoch milliseconds. Release signal
servers may create a table with a different declared timestamp capability;
direct users must bind times in the table's reported native unit.

Named parameters use SQLite notation (`:metric`, `:start`, and so on). Bind
numbers as integers/reals rather than interpolating query text.

## PromQL foundations and equivalents

### SQL-PROM-001: instant selector

At evaluation timestamp `:at`, return the newest sample in
`(:at - :lookback, :at]` for each matching series:

```sql
SELECT labels, ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :at, :at,
  1, :lookback
)
ORDER BY labels;
```

`:filter_json` uses a plain string for equality and an operator object for
the other matcher forms:

```json
{
  "host": {"re": "web-.*"},
  "env": {"neq": "dev"},
  "zone": {"nre": "test-.+"},
  "service": "api"
}
```

Regexes are fully anchored and an absent label is compared as the empty
string. The Rust API remains responsible for parsing PromQL, expanding
multi-metric selectors, preserving duplicate matcher AND semantics, and
formatting the vector response. Direct regression: `tests/cli.sh` sections 22
and 35.

Prometheus 3 quoted UTF-8 metric names are syntax only at the API boundary.
Bind their decoded value to `:metric` as ordinary SQLite TEXT—for example,
`{"oracle.metric/温度","node.name"="東京"}` binds
`:metric = 'oracle.metric/温度'` and includes `{"node.name":"東京"}` in
`:filter_json`. A metric name is data, never a SQL identifier, so it must not
be interpolated or identifier-quoted into this statement.

For a nameless selector, first enumerate distinct `name` values from
`timeless_series('metrics')`, applying its optional matcher-aware arguments
where possible, and execute this statement once per selected name. That
catalog/read loop is the honest public SQL composition: SQLite cannot bind a
table-valued function's hidden metric input from a correlated row on every
supported extension host. The Rust API keeps the loop bounded and does not
read unrelated metric payloads. Regex and negative `__name__` matchers are
applied to the catalog's `name` column before executing the per-name statement;
duplicate name predicates use ordinary SQL `AND` composition.

### SQL-PROM-002: `avg_over_time`

Evaluate `avg_over_time(metric{...}[:window])` on the exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'avg'
)
ORDER BY labels, ts;
```

The window is `(T-window,T]`, matching PromQL range boundaries. Set
`:start = :end` for an instant evaluation. This recipe is exact for stored
float samples: the public window kernel uses compensated summation and switches
to an incremental mean before a finite average would overflow. It preserves
Prometheus `NaN`, positive/negative infinity, and signed-zero float behavior;
the API still owns language parsing, metric-name removal, timestamp units,
limits, and result envelopes. Native histogram samples are not stored. Direct
regression: `tests/cli.sh` sections 22, 33, 35, and 45.

### SQL-PROM-021: `min_over_time`

Evaluate `min_over_time(metric{...}[:window])` on an exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'min'
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. Each reduction uses the exact
open-left, closed-right interval `(T-window,T]`; empty windows emit no row.
Samples are visited in timestamp order. An incoming NaN does not replace a
numeric minimum, a leading NaN is replaced by the first ordered value, an
all-NaN window remains NaN in the extension's packed IEEE bits, and equal
signed zeros retain the first sample. Some SQLite hosts or language bindings
normalize a NaN REAL to SQL NULL when projecting or binding it; use
`timeless_window_batches` when exact non-finite bits must cross that boundary.
The Rust API owns PromQL parsing, metric-name removal, outer evaluation
timestamps, subquery composition, limits, cancellation, IEEE response strings,
and result envelopes. Native histogram samples are not stored. Direct
regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_six_promql_min_over_time_boundaries_ieee_limits_and_reopen`.

### SQL-PROM-022: `max_over_time`

Evaluate `max_over_time(metric{...}[:window])` on an exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'max'
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. Each reduction uses
`(T-window,T]`; empty windows emit no row. Samples are visited in timestamp
order. An incoming NaN does not replace a numeric maximum, a leading NaN is
replaced by the first ordered value, an all-NaN window remains NaN in packed
IEEE bits, and equal signed zeros retain the first sample. SQLite hosts or
bindings may normalize a NaN REAL to SQL NULL; use
`timeless_window_batches` to preserve exact non-finite bits across that
boundary. The Rust API owns PromQL parsing, metric-name removal, outer
timestamps, subquery composition, limits, cancellation, IEEE response strings,
and result envelopes. Native histograms are not stored. Direct regression:
`tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_six_promql_max_over_time_boundaries_ieee_limits_and_reopen`.

### SQL-PROM-023: `sum_over_time`

Evaluate `sum_over_time(metric{...}[:window])` on an exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'sum'
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. Each reduction consumes stored
float samples in `(T-window,T]`; empty windows emit no row. The public kernel
uses Prometheus-compatible compensated addition, so cancellation-prone finite
inputs retain their low-order result. A finite overflow remains infinity,
mixed infinities become NaN, and NaN propagates. SQLite hosts or bindings may
normalize a NaN REAL to SQL NULL; `timeless_window_batches` preserves the
exact IEEE bits. The Rust API owns PromQL parsing, metric-name removal, outer
timestamps, subquery composition, limits, cancellation, IEEE response strings,
and result envelopes. Native histograms are not stored. Direct regression:
`tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_six_promql_sum_over_time_is_compensated_ieee_bounded_and_reopenable`.

### SQL-PROM-024: `count_over_time`

Evaluate `count_over_time(metric{...}[:window])` on an exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'count'
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. Each result counts every stored
float sample in `(T-window,T]`, including NaN, either infinity, and either
signed zero. Empty windows emit no row rather than zero. The row TVF returns
the count as a SQLite REAL because every window operation shares one numeric
schema; counts remain exact while bounded by the public work limit. The Rust
API owns PromQL parsing, metric-name removal, outer timestamps, subqueries,
limits, cancellation, string formatting, and result envelopes. Native
histograms are not stored. Direct regression: `tests/cli.sh` sections 22 and
45; HTTP/oracle/reopen regression:
`session_six_promql_count_over_time_includes_ieee_limits_and_reopen`.

### SQL-PROM-025: `last_over_time`

Evaluate `last_over_time(metric{...}[:window])` on an exact range-query grid:

```sql
SELECT labels, ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. `timeless_grid` selects the last
stored float in `(T-window,T]`; empty windows emit no row. Values—including
NaN, infinities, and signed zero—are returned unchanged. Duplicate timestamps
follow the extension's stable engine order; direct users that admit duplicates
must treat that order as part of their ingest contract because Prometheus
storage normally has one sample per series/timestamp. The Rust API owns
PromQL parsing, multi-metric expansion, outer timestamps, subquery composition,
limits, cancellation, IEEE response strings, and envelopes. Unlike most
PromQL range functions, pinned Prometheus `last_over_time` preserves the input
metric name; direct SQL already returns `:metric` separately from canonical
labels. Native histograms are not stored. Direct regression: `tests/cli.sh`
section 45; HTTP/oracle/reopen regression:
`session_six_promql_last_over_time_preserves_name_ieee_limits_and_reopen`.

### SQL-PROM-026: `present_over_time`

Map every non-empty stored float window to `1` on an exact range-query grid:

```sql
SELECT labels, ts, CAST(value > 0 AS REAL) AS value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window,
  'count'
)
ORDER BY labels, ts;
```

Metric timestamps and all bound parameters are integer seconds. Set
`:start = :end` for an instant evaluation. The public count kernel emits one
positive count for each series/window containing at least one stored float in
`(T-window,T]`, including NaN, either infinity, and either signed zero; empty
windows emit no row. Casting the positive comparison to REAL therefore yields
exactly `1.0` for every returned row without adding a presence-specific
extension primitive. The Rust API owns PromQL parsing, metric-name removal,
outer millisecond timestamps, subquery composition, limits, cancellation, and
result envelopes. Native histogram samples are not stored. Direct regression:
`tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_six_promql_present_over_time_tracks_presence_limits_and_reopen`.

### SQL-PROM-027: `quantile_over_time`

For a finite `:q` in `[0,1]` and finite sample values, rank each series and
evaluation window independently and linearly interpolate at `q * (N - 1)`:

```sql
WITH RECURSIVE evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    raw.value
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
  WHERE raw.value IS NOT NULL
), ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY series_id, ts ORDER BY value
    ) - 1 AS value_index,
    COUNT(*) OVER (PARTITION BY series_id, ts) AS value_count
  FROM selected
), positions AS (
  SELECT DISTINCT
    series_id,
    labels,
    ts,
    value_count,
    CAST(:q AS REAL) * (value_count - 1) AS rank
  FROM ranked
), bounds AS (
  SELECT
    *,
    CAST(rank AS INTEGER) AS lower_index,
    MIN(CAST(rank AS INTEGER) + 1, value_count - 1) AS upper_index
  FROM positions
)
SELECT
  bounds.labels,
  bounds.ts,
  lower.value * (1.0 - (bounds.rank - bounds.lower_index))
    + upper.value * (bounds.rank - bounds.lower_index) AS value
FROM bounds
JOIN ranked AS lower
  ON lower.series_id = bounds.series_id
 AND lower.ts = bounds.ts
 AND lower.value_index = bounds.lower_index
JOIN ranked AS upper
  ON upper.series_id = bounds.series_id
 AND upper.ts = bounds.ts
 AND upper.value_index = bounds.upper_index
ORDER BY bounds.labels, bounds.ts;
```

Metric timestamps, `:start`, `:end`, `:step`, and `:window` are integer
seconds; `:q` is REAL. Output grid bounds are inclusive, while every sample
window is exactly `(T-window,T]`. Empty windows emit no row, and output is
ordered by canonical labels then timestamp. The explicit raw join is required:
the public `pXX` window vocabulary is a fixed nearest-rank storage statistic,
not PromQL's scalar-parameter linear interpolation.

This SQL is exact for finite values. SQLite exposes a packed IEEE NaN as SQL
NULL and does not promise Prometheus's stable signed-zero tie order. The Rust
API reads the public packed raw frame and owns raw-NaN-low ranking, signed-zero
order, infinities, `q = NaN`, out-of-range `q` mapping to infinities, scalar
expression evaluation, metric-name removal, millisecond outer timestamps,
subqueries, limits, cancellation, and envelopes. This is an honest public SQL
foundation rather than an IEEE-parity claim. Direct regression:
`tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_six_promql_quantile_over_time_interpolates_ieee_and_reopens`.

### SQL-PROM-028: `stddev_over_time` and `stdvar_over_time`

For finite samples, apply Welford's population second-moment update to every
series and evaluation window through a recursive CTE:

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    ROW_NUMBER() OVER (
      PARTITION BY raw.series_id, evaluation.ts
      ORDER BY raw.ts
    ) AS sample_number,
    COUNT(*) OVER (
      PARTITION BY raw.series_id, evaluation.ts
    ) AS sample_count,
    raw.value
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
  WHERE raw.value IS NOT NULL
), moments(
  series_id, labels, ts, sample_number, sample_count, n, mean, m2
) AS (
  SELECT
    series_id, labels, ts, sample_number, sample_count,
    1.0, value, 0.0
  FROM selected
  WHERE sample_number = 1

  UNION ALL

  SELECT
    next.series_id,
    next.labels,
    next.ts,
    next.sample_number,
    next.sample_count,
    moments.n + 1.0,
    moments.mean + (next.value - moments.mean) / (moments.n + 1.0),
    moments.m2
      + (next.value - moments.mean)
      * (
          next.value
          - (
              moments.mean
              + (next.value - moments.mean) / (moments.n + 1.0)
            )
        )
  FROM moments
  JOIN selected AS next
    ON next.series_id = moments.series_id
   AND next.ts = moments.ts
   AND next.sample_number = moments.sample_number + 1
)
SELECT
  labels,
  ts,
  CASE WHEN :variance_only THEN m2 / n ELSE SQRT(m2 / n) END AS value
FROM moments
WHERE sample_number = sample_count
ORDER BY labels, ts;
```

Metric timestamps, `:start`, `:end`, `:step`, and `:window` are integer
seconds. Bind `:variance_only` to true for `stdvar_over_time` or false for
`stddev_over_time`. Output grid bounds are inclusive and sample windows are
exactly `(T-window,T]`. A singleton returns `0.0`; an empty window emits no row.
Canonical labels/timestamp ordering is deterministic. Samples with duplicate
timestamps require a caller-defined ingest order, as Prometheus normally
admits only one sample per series/timestamp.

The statement is exact for finite SQLite REAL inputs and avoids the unstable
`AVG(x*x)-AVG(x)*AVG(x)` cancellation form. SQLite projects packed NaN as SQL
NULL, so the Rust API reads public packed raw frames and owns raw NaN,
infinity, signed-zero, metric-name removal, millisecond outer timestamps,
subqueries, limits, cancellation, and Prometheus envelopes. Native histogram
samples are not stored. Direct regression: `tests/cli.sh` section 45;
HTTP/oracle/reopen regression:
`session_six_promql_stddev_over_time_is_population_ieee_and_reopenable` and
`session_six_promql_stdvar_over_time_is_population_ieee_and_reopenable`.

### SQL-PROM-029: `rate`

Prometheus `rate` is not merely reset-adjusted increase divided by the range.
For every `(T-window,T]` float-counter slice, compute reset correction, estimate
the sample interval, extrapolate sufficiently close range edges, clamp the left
edge to the counter's estimated zero point, and finally normalize per second:

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    raw.ts AS sample_ts,
    raw.value,
    LAG(raw.value) OVER (
      PARTITION BY raw.series_id, evaluation.ts ORDER BY raw.ts
    ) AS previous_value,
    ROW_NUMBER() OVER (
      PARTITION BY raw.series_id, evaluation.ts ORDER BY raw.ts
    ) AS sample_number,
    COUNT(*) OVER (
      PARTITION BY raw.series_id, evaluation.ts
    ) AS sample_count
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
), folded AS (
  SELECT
    series_id,
    labels,
    ts,
    MAX(sample_count) AS sample_count,
    MIN(sample_ts) AS first_ts,
    MAX(sample_ts) AS last_ts,
    MAX(CASE WHEN sample_number = 1 THEN value END) AS first_value,
    MAX(
      CASE WHEN sample_number = sample_count THEN value END
    ) AS last_value,
    SUM(
      CASE
        WHEN sample_number > 1 AND value < previous_value
          THEN previous_value
        ELSE 0.0
      END
    ) AS reset_correction
  FROM selected
  GROUP BY series_id, labels, ts
  HAVING MAX(sample_count) >= 2 AND MAX(sample_ts) > MIN(sample_ts)
), intervals AS (
  SELECT
    *,
    last_value - first_value + reset_correction AS counter_delta,
    (last_ts - first_ts) * 1.0 AS sampled_interval,
    (last_ts - first_ts) * 1.0 / (sample_count - 1) AS average_interval
  FROM folded
), edges AS (
  SELECT
    *,
    CASE
      WHEN first_ts - (ts - :window) >= average_interval * 1.1
        THEN average_interval / 2.0
      ELSE first_ts - (ts - :window)
    END AS start_duration,
    CASE
      WHEN ts - last_ts >= average_interval * 1.1
        THEN average_interval / 2.0
      ELSE ts - last_ts
    END AS end_duration
  FROM intervals
)
SELECT
  labels,
  ts,
  counter_delta
    * (
        sampled_interval
        + CASE
            WHEN counter_delta > 0 AND first_value >= 0
              THEN MIN(
                start_duration,
                sampled_interval * first_value / counter_delta
              )
            ELSE start_duration
          END
        + end_duration
      )
    / sampled_interval
    / :window AS value
FROM edges
ORDER BY labels, ts;
```

Metric timestamps, `:start`, `:end`, `:step`, and `:window` are integer
seconds, so the result is per second. `:filter_json` is the public matcher JSON
accepted by `timeless_raw`, or NULL. Output grid bounds are inclusive; sample
windows are exactly `(T-window,T]`. Fewer than two samples and zero-duration
sample pairs emit no row. Canonical label/timestamp ordering is deterministic.
Prometheus normally admits only one float sample per series/timestamp; callers
that insert duplicates directly must define an additional stable ordering.

This executable recipe is exact for finite float counters. It intentionally
does not use `timeless_window(..., 'rate')`: that public kernel is a general
mechanical reset fold divided by the full window and does not implement
Prometheus edge extrapolation or the zero-point clamp. The Rust API uses one
bounded public packed raw read and owns PromQL parsing, metric-name removal,
modifier and subquery evaluation, NaN/infinity strings, limits, cancellation,
and HTTP envelopes. Native histograms and counter start timestamps are not
stored. Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen
regression: `session_seven_promql_rate_extrapolates_resets_bounds_and_reopens`.

### SQL-PROM-030: `irate`

For every `(T-window,T]` float-counter slice, select its final two samples,
substitute the last value for a counter reset, and divide by their actual
timestamp interval:

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), ranked AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    raw.ts AS sample_ts,
    raw.value,
    ROW_NUMBER() OVER (
      PARTITION BY raw.series_id, evaluation.ts ORDER BY raw.ts DESC
    ) AS recency
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
), final_pair AS (
  SELECT
    series_id,
    labels,
    ts,
    MAX(CASE WHEN recency = 1 THEN sample_ts END) AS last_ts,
    MAX(CASE WHEN recency = 1 THEN value END) AS last_value,
    MAX(CASE WHEN recency = 2 THEN sample_ts END) AS previous_ts,
    MAX(CASE WHEN recency = 2 THEN value END) AS previous_value
  FROM ranked
  WHERE recency <= 2
  GROUP BY series_id, labels, ts
)
SELECT
  labels,
  ts,
  CASE
    WHEN last_value < previous_value THEN last_value
    ELSE last_value - previous_value
  END / (last_ts - previous_ts) AS value
FROM final_pair
WHERE previous_ts IS NOT NULL AND last_ts > previous_ts
ORDER BY labels, ts;
```

Metric timestamps, `:start`, `:end`, `:step`, and `:window` are integer
seconds, so the result is per second. `:filter_json` is the public matcher JSON
accepted by `timeless_raw`, or NULL. Output grid bounds are inclusive; sample
windows are exactly `(T-window,T]`. Fewer than two samples and a final pair
with a zero timestamp interval emit no row. Canonical label/timestamp ordering
is deterministic. Prometheus normally admits only one float sample per
series/timestamp; callers that insert duplicates directly must define an
additional stable ordering.

This executable recipe is exact for finite float counters. The Rust API uses
one bounded public packed raw read and owns PromQL parsing, metric-name
removal, modifier and subquery evaluation, NaN/infinity strings, limits,
cancellation, and HTTP envelopes. Native histograms are not stored. Direct
regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_seven_promql_irate_uses_last_two_samples_and_reopens`.

### SQL-PROM-031: `increase`

Prometheus `increase` shares `rate`'s reset correction, sample-interval
estimate, edge extrapolation, and left zero-point clamp, but returns the
estimated increase over the range rather than normalizing per second:

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    raw.ts AS sample_ts,
    raw.value,
    LAG(raw.value) OVER (
      PARTITION BY raw.series_id, evaluation.ts ORDER BY raw.ts
    ) AS previous_value,
    ROW_NUMBER() OVER (
      PARTITION BY raw.series_id, evaluation.ts ORDER BY raw.ts
    ) AS sample_number,
    COUNT(*) OVER (
      PARTITION BY raw.series_id, evaluation.ts
    ) AS sample_count
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
), folded AS (
  SELECT
    series_id,
    labels,
    ts,
    MAX(sample_count) AS sample_count,
    MIN(sample_ts) AS first_ts,
    MAX(sample_ts) AS last_ts,
    MAX(CASE WHEN sample_number = 1 THEN value END) AS first_value,
    MAX(
      CASE WHEN sample_number = sample_count THEN value END
    ) AS last_value,
    SUM(
      CASE
        WHEN sample_number > 1 AND value < previous_value
          THEN previous_value
        ELSE 0.0
      END
    ) AS reset_correction
  FROM selected
  GROUP BY series_id, labels, ts
  HAVING MAX(sample_count) >= 2 AND MAX(sample_ts) > MIN(sample_ts)
), intervals AS (
  SELECT
    *,
    last_value - first_value + reset_correction AS counter_delta,
    (last_ts - first_ts) * 1.0 AS sampled_interval,
    (last_ts - first_ts) * 1.0 / (sample_count - 1) AS average_interval
  FROM folded
), edges AS (
  SELECT
    *,
    CASE
      WHEN first_ts - (ts - :window) >= average_interval * 1.1
        THEN average_interval / 2.0
      ELSE first_ts - (ts - :window)
    END AS start_duration,
    CASE
      WHEN ts - last_ts >= average_interval * 1.1
        THEN average_interval / 2.0
      ELSE ts - last_ts
    END AS end_duration
  FROM intervals
)
SELECT
  labels,
  ts,
  counter_delta
    * (
        sampled_interval
        + CASE
            WHEN counter_delta > 0 AND first_value >= 0
              THEN MIN(
                start_duration,
                sampled_interval * first_value / counter_delta
              )
            ELSE start_duration
          END
        + end_duration
      )
    / sampled_interval AS value
FROM edges
ORDER BY labels, ts;
```

Metric timestamps, `:start`, `:end`, `:step`, and `:window` are integer
seconds. `:filter_json` is the public matcher JSON accepted by `timeless_raw`,
or NULL. Output grid bounds are inclusive; sample windows are exactly
`(T-window,T]`. Fewer than two samples and zero-duration sample pairs emit no
row. Canonical label/timestamp ordering is deterministic. Prometheus normally
admits only one float sample per series/timestamp; callers that insert
duplicates directly must define an additional stable ordering.

This executable recipe is exact for finite float counters. It intentionally
does not use `timeless_window(..., 'increase')`: that public kernel is a
general mechanical reset fold and does not implement Prometheus edge
extrapolation or the zero-point clamp. The Rust API uses one bounded public
packed raw read and owns PromQL parsing, metric-name removal, modifier and
subquery evaluation, NaN/infinity strings, limits, cancellation, and HTTP
envelopes. Native histograms and counter start timestamps are not stored.
Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_seven_promql_increase_extrapolates_without_rate_normalization`.

### SQL-PROM-032: `delta`

Prometheus `delta` extrapolates the difference between the first and last
float-gauge samples to the range edges. Gauge decreases remain negative; no
counter reset correction or zero-point clamp is applied:

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    raw.ts AS sample_ts,
    raw.value,
    ROW_NUMBER() OVER (
      PARTITION BY raw.series_id, evaluation.ts ORDER BY raw.ts
    ) AS sample_number,
    COUNT(*) OVER (
      PARTITION BY raw.series_id, evaluation.ts
    ) AS sample_count
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
), folded AS (
  SELECT
    series_id,
    labels,
    ts,
    MAX(sample_count) AS sample_count,
    MIN(sample_ts) AS first_ts,
    MAX(sample_ts) AS last_ts,
    MAX(CASE WHEN sample_number = 1 THEN value END) AS first_value,
    MAX(
      CASE WHEN sample_number = sample_count THEN value END
    ) AS last_value
  FROM selected
  GROUP BY series_id, labels, ts
  HAVING MAX(sample_count) >= 2 AND MAX(sample_ts) > MIN(sample_ts)
), intervals AS (
  SELECT
    *,
    last_value - first_value AS gauge_delta,
    (last_ts - first_ts) * 1.0 AS sampled_interval,
    (last_ts - first_ts) * 1.0 / (sample_count - 1) AS average_interval
  FROM folded
), edges AS (
  SELECT
    *,
    CASE
      WHEN first_ts - (ts - :window) >= average_interval * 1.1
        THEN average_interval / 2.0
      ELSE first_ts - (ts - :window)
    END AS start_duration,
    CASE
      WHEN ts - last_ts >= average_interval * 1.1
        THEN average_interval / 2.0
      ELSE ts - last_ts
    END AS end_duration
  FROM intervals
)
SELECT
  labels,
  ts,
  gauge_delta
    * (sampled_interval + start_duration + end_duration)
    / sampled_interval AS value
FROM edges
ORDER BY labels, ts;
```

Metric timestamps, `:start`, `:end`, `:step`, and `:window` are integer
seconds. `:filter_json` is the public matcher JSON accepted by `timeless_raw`,
or NULL. Output grid bounds are inclusive; sample windows are exactly
`(T-window,T]`. Fewer than two samples and zero-duration sample pairs emit no
row. Canonical label/timestamp ordering is deterministic. Prometheus normally
admits only one float sample per series/timestamp; callers that insert
duplicates directly must define an additional stable ordering.

This executable recipe is exact for finite float gauges. It intentionally
does not use `timeless_window(..., 'delta')`: that public kernel is a general
mechanical last-minus-first fold and does not extrapolate Prometheus range
edges. The Rust API uses one bounded public packed raw read and owns PromQL
parsing, metric-name removal, modifier and subquery evaluation, NaN/infinity
strings, limits, cancellation, and HTTP envelopes. Native histograms are not
stored. Direct regression: `tests/cli.sh` section 45;
HTTP/oracle/reopen regression:
`session_seven_promql_delta_extrapolates_gauges_without_counter_correction`.

### SQL-PROM-033: `idelta`

For every `(T-window,T]` float-gauge slice, select its final two samples and
subtract the earlier value from the later value. Do not extrapolate or divide
by their timestamp interval:

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), ranked AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    raw.ts AS sample_ts,
    raw.value,
    ROW_NUMBER() OVER (
      PARTITION BY raw.series_id, evaluation.ts ORDER BY raw.ts DESC
    ) AS recency
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
), final_pair AS (
  SELECT
    series_id,
    labels,
    ts,
    MAX(CASE WHEN recency = 1 THEN sample_ts END) AS last_ts,
    MAX(CASE WHEN recency = 1 THEN value END) AS last_value,
    MAX(CASE WHEN recency = 2 THEN sample_ts END) AS previous_ts,
    MAX(CASE WHEN recency = 2 THEN value END) AS previous_value
  FROM ranked
  WHERE recency <= 2
  GROUP BY series_id, labels, ts
)
SELECT
  labels,
  ts,
  last_value - previous_value AS value
FROM final_pair
WHERE previous_ts IS NOT NULL AND last_ts > previous_ts
ORDER BY labels, ts;
```

Metric timestamps, `:start`, `:end`, `:step`, and `:window` are integer
seconds. `:filter_json` is the public matcher JSON accepted by `timeless_raw`,
or NULL. Output grid bounds are inclusive; sample windows are exactly
`(T-window,T]`. Fewer than two samples and a final pair with a zero timestamp
interval emit no row. Canonical label/timestamp ordering is deterministic.
Prometheus normally admits only one float sample per series/timestamp; callers
that insert duplicates directly must define an additional stable ordering.

This executable recipe is exact for finite float gauges. The Rust API uses one
bounded public packed raw read and owns PromQL parsing, metric-name removal,
modifier and subquery evaluation, NaN/infinity strings, limits, cancellation,
and HTTP envelopes. Native histograms are not stored. Direct regression:
`tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_seven_promql_idelta_uses_only_the_final_gauge_pair`.

### SQL-PROM-034: `deriv`

Compute a least-squares slope per second for every float-gauge slice in the
exact PromQL interval `(T-window,T]`. Center each timestamp on the first sample
before forming products; converting absolute epoch timestamps to floating
point first needlessly loses precision:

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    raw.ts AS sample_ts,
    raw.value,
    MIN(raw.ts) OVER (
      PARTITION BY raw.series_id, evaluation.ts
    ) AS first_sample_ts
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
), centered AS (
  SELECT
    series_id,
    labels,
    ts,
    (sample_ts - first_sample_ts) * 1.0 AS x,
    value AS y
  FROM selected
), statistics AS (
  SELECT
    series_id,
    labels,
    ts,
    COUNT(*) * 1.0 AS n,
    MIN(y) AS min_y,
    MAX(y) AS max_y,
    SUM(x) AS sum_x,
    SUM(y) AS sum_y,
    SUM(x * y) AS sum_xy,
    SUM(x * x) AS sum_x2
  FROM centered
  GROUP BY series_id, labels, ts
)
SELECT
  labels,
  ts,
  CASE
    WHEN min_y = max_y THEN 0.0
    ELSE (sum_xy - sum_x * sum_y / n)
         / (sum_x2 - sum_x * sum_x / n)
  END AS value
FROM statistics
WHERE n >= 2
ORDER BY labels, ts;
```

Metric timestamps, `:start`, `:end`, `:step`, and `:window` are integer
seconds. `:filter_json` is the public matcher JSON accepted by `timeless_raw`,
or NULL. Output grid bounds are inclusive and each sample window is exactly
`(T-window,T]`. Fewer than two samples emit no row. Constant finite values
return `0`; canonical label/timestamp ordering is deterministic. Prometheus
normally admits one float sample per series/timestamp. If direct SQL inserts
distinct values at one timestamp, the zero time variance makes this finite
recipe return SQL NULL.

This statement is the ordinary-SQL foundation for finite values within
SQLite's aggregate precision. Prometheus uses compensated sums for `x`, `y`,
`x*y`, and `x*x`; it also returns `NaN` for constant infinities and other IEEE
indeterminacies. The Rust API performs that exact arithmetic over one bounded
public packed raw read and owns PromQL parsing, metric-name removal, modifier
and subquery evaluation, value strings, limits, cancellation, and HTTP
envelopes. No extension-specific regression opcode is justified. Direct
regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_seven_promql_deriv_matches_centered_compensated_regression`.

### SQL-PROM-035: `predict_linear`

Fit each float-gauge slice in `(T-window,T]` and evaluate the line
`:horizon` seconds after `T`. Centering samples on the evaluation timestamp is
essential: selector `offset` and `@` change which samples enter the range, but
the PromQL forecast origin remains the outer evaluation timestamp.

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    (raw.ts - evaluation.ts) * 1.0 AS x,
    raw.value AS y
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
), statistics AS (
  SELECT
    series_id,
    labels,
    ts,
    COUNT(*) * 1.0 AS n,
    MIN(y) AS min_y,
    MAX(y) AS max_y,
    SUM(x) AS sum_x,
    SUM(y) AS sum_y,
    SUM(x * y) AS sum_xy,
    SUM(x * x) AS sum_x2
  FROM selected
  GROUP BY series_id, labels, ts
), coefficients AS (
  SELECT
    *,
    CASE
      WHEN min_y = max_y THEN 0.0
      ELSE (sum_xy - sum_x * sum_y / n)
           / (sum_x2 - sum_x * sum_x / n)
    END AS slope
  FROM statistics
  WHERE n >= 2
)
SELECT
  labels,
  ts,
  slope * :horizon + (sum_y / n - slope * sum_x / n) AS value
FROM coefficients
ORDER BY labels, ts;
```

Metric timestamps, `:start`, `:end`, `:step`, `:window`, and `:horizon` are
seconds; the first five are integer storage/evaluation values and `:horizon`
is a SQLite numeric parameter. `:filter_json` is the public matcher JSON
accepted by `timeless_raw`, or NULL. Grid bounds are inclusive and sample
windows are exactly `(T-window,T]`. Fewer than two samples emit no row. A
constant finite series predicts that constant. Canonical label/timestamp
ordering is deterministic. Distinct values at one directly inserted duplicate
timestamp make the zero-variance finite recipe return SQL NULL.

This statement is the ordinary-SQL foundation for finite values within
SQLite's aggregate precision. The Rust API evaluates an arbitrary shipped
scalar expression for the horizon at each outer step, uses four compensated
sums, preserves Prometheus NaN/infinity behavior, and owns syntax,
metric-name removal, modifiers, subqueries, limits, cancellation, and HTTP
envelopes. It performs one bounded public packed raw read; no forecast-specific
extension primitive is justified. Direct regression: `tests/cli.sh` section
45; HTTP/oracle/reopen regression:
`session_seven_promql_predict_linear_anchors_at_evaluation_time`.

### SQL-PROM-036: `changes`

Count adjacent value transitions in every `(T-window,T]` float slice. A
repeated NaN is unchanged, NaN-to-number or number-to-NaN is one change, and
SQLite numeric equality correctly treats `+0` and `-0` as equal:

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    raw.ts AS sample_ts,
    raw.value
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
), sequenced AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY series_id, ts ORDER BY sample_ts
    ) AS sample_number,
    LAG(value) OVER (
      PARTITION BY series_id, ts ORDER BY sample_ts
    ) AS previous_value
  FROM selected
)
SELECT
  labels,
  ts,
  SUM(
    CASE
      WHEN sample_number = 1 THEN 0
      WHEN (value IS NULL) <> (previous_value IS NULL) THEN 1
      WHEN value != previous_value THEN 1
      ELSE 0
    END
  ) AS value
FROM sequenced
GROUP BY series_id, labels, ts
ORDER BY labels, ts;
```

Metric timestamps, `:start`, `:end`, `:step`, and `:window` are integer
seconds. `:filter_json` is the public matcher JSON accepted by `timeless_raw`,
or NULL. Grid bounds are inclusive; windows are exactly `(T-window,T]`;
canonical label/timestamp ordering is deterministic. A singleton emits `0`.
Prometheus normally admits one float sample per series/timestamp; direct
duplicates require an additional stable tie order.

`timeless_raw` preserves IEEE bits internally and exposes a stored NaN as SQL
NULL on ordinary SQLite row projection. Metric samples themselves are never
missing, so the explicit NULL comparison above implements Prometheus's
repeated-NaN rule without conflating it with absent samples. Infinities compare
as numeric values and signed zeros compare equal. The Rust API uses the
bit-exact packed raw surface, scans once without allocating another value
vector, and owns PromQL syntax, metric-name removal, modifiers, subqueries,
limits, cancellation, and HTTP envelopes. No transition-specific extension
primitive is justified. Direct regression: `tests/cli.sh` section 45;
HTTP/oracle/reopen regression:
`session_seven_promql_changes_counts_float_transitions`.

### SQL-PROM-037: `resets`

Count adjacent strict decreases in every float-counter slice in
`(T-window,T]`:

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    raw.ts AS sample_ts,
    raw.value
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
), sequenced AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY series_id, ts ORDER BY sample_ts
    ) AS sample_number,
    LAG(value) OVER (
      PARTITION BY series_id, ts ORDER BY sample_ts
    ) AS previous_value
  FROM selected
)
SELECT
  labels,
  ts,
  SUM(
    CASE
      WHEN sample_number > 1 AND value < previous_value THEN 1
      ELSE 0
    END
  ) AS value
FROM sequenced
GROUP BY series_id, labels, ts
ORDER BY labels, ts;
```

Metric timestamps, `:start`, `:end`, `:step`, and `:window` are integer
seconds. `:filter_json` is the public matcher JSON accepted by `timeless_raw`,
or NULL. Grid bounds are inclusive; windows are exactly `(T-window,T]`; a
singleton emits `0`; canonical label/timestamp ordering is deterministic.
Prometheus normally admits one float sample per series/timestamp; direct
duplicates require an additional stable tie order.

This recipe deliberately uses only IEEE `<`: equal values and either
signed-zero order are not resets; a NaN comparison is false; finite-to-`+Inf`
is not a reset; `+Inf`-to-finite and finite-to-`-Inf` are resets.
`timeless_raw` exposes stored NaN as SQL NULL on ordinary row projection, for
which the comparison is likewise not true. The Rust API scans the bit-exact
packed raw frame once without allocating another value vector and owns PromQL
syntax, metric-name removal, modifiers, subqueries, limits, cancellation, and
HTTP envelopes. The extension's mechanical reset-corrected increase/rate
kernels do not expose this count and are not relabeled; ordinary SQL is exact,
so no reset-count primitive is justified. Direct regression: `tests/cli.sh`
section 45; HTTP/oracle/reopen regression:
`session_seven_promql_resets_counts_strict_float_decreases`.

### SQL-PROM-038: `abs`

Apply SQLite's scalar `abs` to every sample selected on a bounded public grid:

```sql
SELECT labels, ts,
       CASE WHEN value = 0.0 THEN 0.0 ELSE abs(value) END AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

`:metric` is an exact metric name and `:filter_json` is public matcher JSON or
NULL. Timestamps, grid bounds, `:step`, and the open-left `:lookback` are in
the metric table's configured timestamp unit (integer seconds for the default
table). Canonical label JSON and timestamp ordering are deterministic. The
statement is exact for every finite float, explicitly converts either signed
zero to positive zero, and maps either infinity to `+Inf`. The `CASE` is
required because SQLite's built-in `abs(-0.0)` preserves the negative-zero
bits rather than producing Prometheus's positive-zero result.

SQLite represents a row-projected stored NaN as SQL NULL, so `abs(value)` also
returns NULL for that case. The Rust API reads the public bit-exact packed
frame and preserves Prometheus `NaN`; it also owns PromQL typing, metric-name
removal, nested expression composition, millisecond timestamps, cumulative
limits, cancellation, and vector/matrix HTTP envelopes. That remaining API
work is why this recipe is an honest SQL foundation rather than a claim that
ordinary row SQL alone provides the complete PromQL function. A specialized
extension primitive would add no storage pruning or decode benefit. Direct
regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_abs_transforms_float_vectors_and_reopens`.

### SQL-PROM-039: `ceil`, `floor`, and `round`

SQLite's public scalar math functions directly express bounded ceiling and
floor transforms:

```sql
SELECT labels, ts, ceil(value) AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

Substitute `floor(value)` for `ceil(value)`. To match Prometheus `round` and
its optional nearest-multiple argument, preserve the upstream operation order:

```sql
WITH
parameter(nearest) AS (
  SELECT COALESCE(CAST(:nearest AS REAL), 1.0)
), selected AS (
  SELECT labels, ts, value, 1.0 / nearest AS inverse
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  ), parameter
)
SELECT labels, ts,
       floor(value * inverse + 0.5) / inverse AS value
FROM selected
ORDER BY labels, ts;
```

Bind `:nearest` to NULL for the default `1`, or to a scalar step such as
`0.5`. The formula intentionally rounds ties upward, including `-1.5` to
`-1`; a zero, signed-zero, NaN, or infinite step follows IEEE arithmetic and
produces the same NaN cases as Prometheus. Negative steps are accepted and
retain the exact upstream operation order rather than being silently made
positive.

All timestamp and grid parameters use the metric table's configured unit;
the default table uses integer seconds. Grid bounds are inclusive, lookback is
open on the left, missing samples remain absent, labels are canonical JSON,
and ordering is deterministic. `ceil` and `floor` retain negative-zero bits;
default `round` maps negative zero to positive zero.

As in `SQL-PROM-038`, ordinary row projection represents a stored NaN as SQL
NULL. The Rust API reads the public bit-exact packed frame to retain NaN,
evaluates the optional scalar expression on each outer timestamp, removes the
metric name, and owns PromQL types, cumulative limits, cancellation, and HTTP
envelopes. These transforms do not justify an extension primitive: they add
no pruning and require no additional decode beyond the selected vector.
Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_rounding_functions_match_float_and_step_semantics`.

### SQL-PROM-040: `clamp`, `clamp_min`, and `clamp_max`

Bind the scalar lower and upper bounds through a one-row parameter CTE, then
bound every selected value. The final predicate is significant: Prometheus
returns an empty vector when the lower bound is greater than the upper bound.

```sql
WITH
parameters(minimum, maximum) AS (
  SELECT CAST(:minimum AS REAL), CAST(:maximum AS REAL)
), selected AS (
  SELECT labels, ts, value, minimum, maximum
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  ), parameters
)
SELECT labels, ts,
       CASE
         WHEN value <= minimum THEN minimum
         WHEN value >= maximum THEN maximum
         ELSE value
       END AS value
FROM selected
WHERE minimum <= maximum
ORDER BY labels, ts;
```

For `clamp_min(vector, minimum)`, omit `maximum` and project
`CASE WHEN value <= minimum THEN minimum ELSE value END`. For
`clamp_max(vector, maximum)`, project
`CASE WHEN value >= maximum THEN maximum ELSE value END`. Bounds, step, and
lookback use the metric table's configured timestamp unit (integer seconds for
the default table); grid bounds are inclusive and lookback is open on the
left. Missing grid samples remain absent, canonical label JSON is returned,
and the explicit ordering is deterministic.

This public-SQL recipe is exact for ordinary finite values and infinities.
SQLite row projection maps a stored NaN to SQL NULL and does not expose enough
information to reproduce Go's `math.Min`/`math.Max` choice between equal
positive and negative zero in every bound order. The Rust API therefore reads
the public bit-exact packed frame and implements those remaining Prometheus
semantics, evaluates each scalar bound expression on every outer timestamp,
removes `__name__`, and owns types, cumulative limits, cancellation, and HTTP
envelopes. No extension primitive is warranted: clamping cannot improve
storage pruning or avoid the already-required vector decode.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_clamp_functions_bound_ieee_vectors_and_reopen`.

### SQL-PROM-041: `sqrt`, `exp`, `ln`, `log2`, and `log10`

SQLite's public math functions directly transform every bounded row:

```sql
SELECT labels, ts, sqrt(value) AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

Substitute `exp(value)`, `ln(value)`, `log2(value)`, or `log10(value)` for
`sqrt(value)`. Bounds, step, and lookback use the metric table's configured
timestamp unit (integer seconds for the default table); grid bounds are
inclusive and lookback is open on the left. Missing grid samples remain
absent, labels are canonical JSON, and ordering is deterministic.

This recipe is exact over each SQLite function's valid row-visible domain.
SQLite returns SQL NULL for invalid square-root/logarithm domains and for
zero logarithms; a stored NaN is also row-projected as NULL. Those values do
not encode Prometheus's required distinction among `NaN`, `-Inf`, and an
absent sample. The Rust API reads the public bit-exact packed frame, applies
the IEEE operations directly, removes `__name__`, and owns PromQL typing,
limits, cancellation, nested composition, and HTTP envelopes. There is no
storage-read or decode saving from adding a specialized extension primitive:
direct SQLite/libSQL users already have these standard functions.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_math_transforms_match_domains_ieee_and_reopen`.

### SQL-PROM-042: `sgn`

Use an ordered `CASE` so both infinities map to a unit sign while either zero
falls through with its original sign bit:

```sql
SELECT labels, ts,
       CASE
         WHEN value > 0.0 THEN 1.0
         WHEN value < 0.0 THEN -1.0
         ELSE value
       END AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

Bounds, step, and lookback use the metric table's configured timestamp unit
(integer seconds for the default table); grid bounds are inclusive and
lookback is open on the left. Missing samples remain absent, canonical label
JSON is returned, and ordering is deterministic. This statement is exact for
finite values, both infinities, and both signed zeros visible through rows.

A stored NaN is row-projected as SQL NULL, so ordinary row SQL returns NULL
rather than Prometheus `NaN`. The Rust API reads the public bit-exact packed
frame and owns that final fidelity distinction together with metric-name
removal, types, nesting, limits, cancellation, and HTTP envelopes. A new
extension primitive would add no pruning or decode benefit.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_sgn_preserves_ieee_signs_ranges_and_reopen`.

### SQL-PROM-043: inverse trigonometric and hyperbolic functions

SQLite's public math functions directly express the bounded transforms:

```sql
SELECT labels, ts, acos(value) AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

Substitute `acosh(value)`, `asin(value)`, `asinh(value)`, `atan(value)`, or
`atanh(value)`. Bounds, step, and lookback use the metric table's configured
timestamp unit (integer seconds for the default table); grid bounds are
inclusive and lookback is open on the left. Missing samples remain absent,
canonical label JSON is returned, and ordering is deterministic.

This recipe is exact over each SQLite function's valid row-visible domain.
SQLite reports invalid domains as SQL NULL, and a stored NaN is also
row-projected as NULL. That cannot represent Prometheus's distinctions among
`NaN`, endpoint `+Inf`/`-Inf`, and an absent sample. The Rust API reads the
public bit-exact packed frame and owns those IEEE/domain results, signed zero,
metric-name removal, types, nesting, limits, cancellation, and HTTP envelopes.
The standard functions already serve direct SQLite/libSQL users; a specialized
extension primitive would save neither a read nor a decode.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_inverse_transforms_pin_domains_ieee_and_reopen`.

### SQL-PROM-044: trigonometric and hyperbolic functions

SQLite's public math functions directly express the bounded transforms:

```sql
SELECT labels, ts, cos(value) AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

Substitute `cosh(value)`, `sin(value)`, `sinh(value)`, `tan(value)`, or
`tanh(value)`. Bounds, step, and lookback use the metric table's configured
timestamp unit (integer seconds for the default table); grid bounds are
inclusive and lookback is open on the left. Missing samples remain absent,
canonical label JSON is returned, and ordering is deterministic.

This recipe is exact over each SQLite function's valid row-visible domain.
SQLite represents invalid trigonometric infinity inputs and stored NaN as SQL
NULL, which cannot distinguish Prometheus `NaN` from an absent/NULL value. The
Rust API reads the public bit-exact packed frame and owns those IEEE results,
hyperbolic infinities, signed zero, metric-name removal, types, nesting,
limits, cancellation, and HTTP envelopes. Standard SQLite math already
benefits direct users; a specialized extension primitive would not save a
storage read or decode.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_trig_transforms_pin_ieee_ranges_and_reopen`.

### SQL-PROM-045: `deg`, `rad`, and `pi`

Preserve Prometheus's degree/radian operation order over each bounded row:

```sql
SELECT labels, ts, value * 180.0 / pi() AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

For `rad(vector)`, project `value * pi() / 180.0`. Bounds, step, and lookback
use the metric table's configured timestamp unit (integer seconds for the
default table); grid bounds are inclusive and lookback is open on the left.
Missing samples remain absent, canonical label JSON is returned, and ordering
is deterministic.

The scalar `pi()` has no storage input. A direct SQL caller that needs an
evaluation timestamp can bind it explicitly:

```sql
WITH evaluation(ts) AS (SELECT :at)
SELECT ts, pi() AS value
FROM evaluation;
```

Use a recursive evaluation CTE for a range grid. The Rust API owns PromQL's
scalar instant and matrix range envelopes, exact outer timestamps, and
expression typing. For `deg`/`rad`, it also reads the public bit-exact packed
frame so stored NaN remains Prometheus `NaN` rather than row-projected SQL
NULL, removes `__name__`, and enforces limits and cancellation. Standard
SQLite math already provides every direct-user operation here; no extension
primitive is justified.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_angle_transforms_and_pi_preserve_types_and_reopen`.

### SQL-PROM-046: `label_join`

Bind the ordered source-label names as a JSON array. This statement handles
arbitrary arity, missing labels, overwrite/removal, and `__name__` without
constructing unsafe dynamic JSON paths:

```sql
WITH selected AS (
  SELECT :metric AS name, labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
), joined AS (
  SELECT selected.*,
         COALESCE((
           SELECT group_concat(component, :separator)
           FROM (
             SELECT CASE
                      WHEN source.value = '__name__' THEN selected.name
                      ELSE COALESCE((
                        SELECT label.value
                        FROM json_each(selected.labels) AS label
                        WHERE label.key = source.value
                      ), '')
                    END AS component
             FROM json_each(:source_labels_json) AS source
             ORDER BY CAST(source.key AS INTEGER)
           )
         ), '') AS joined_value
  FROM selected
)
SELECT
  CASE WHEN :destination = '__name__'
       THEN NULLIF(joined_value, '')
       ELSE name
  END AS name,
  CASE WHEN :destination = '__name__' THEN labels
       ELSE COALESCE((
         SELECT json_group_object(label_key, label_value)
         FROM (
           SELECT existing.key AS label_key, existing.value AS label_value
           FROM json_each(joined.labels) AS existing
           WHERE existing.key <> :destination
           UNION ALL
           SELECT :destination, joined_value
           WHERE joined_value <> ''
           ORDER BY label_key
         )
       ), '{}')
  END AS labels,
  ts,
  value
FROM joined
ORDER BY labels, ts;
```

`:source_labels_json` is an ordered JSON array such as
`["service","zone"]`; `[]` is valid. Every missing source contributes the
empty string, duplicate sources are retained, and separators occur between
every source position. Sources are read from the original label set before
the destination is changed. An empty joined value removes the destination;
for `__name__`, SQL `NULL` represents a nameless output series. Rebuilding the
ordinary JSON object by key also permits Prometheus 3's nonempty UTF-8 label
names without dynamic JSON-path quoting.

Grid bounds are inclusive, lookback is open-left, and timestamps use the
metric table's configured unit (integer seconds for the default table). The
statement preserves sample values and timestamps and emits deterministic
label-key and row ordering. The Rust API owns PromQL parsing and types,
response assembly, exact range-series identity, cumulative work and response
limits, cancellation, and error envelopes. This is ordinary public SQLite
composition over one bounded extension read; no label-specific extension
primitive is justified.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_label_join_pins_sources_names_limits_and_reopen`.

### SQL-PROM-047: `absent`

Generate the requested evaluation grid and retain only timestamps with no
selected sample from any series:

```sql
WITH RECURSIVE evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step
  FROM evaluation
  WHERE ts + :step <= :end
), present AS (
  SELECT DISTINCT ts
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT :output_labels_json AS labels,
       evaluation.ts,
       1.0 AS value
FROM evaluation
LEFT JOIN present USING (ts)
WHERE present.ts IS NULL
ORDER BY evaluation.ts;
```

`:start`, `:end`, `:step`, and `:lookback` use the metric table's configured
timestamp unit (integer seconds for the default table). Bounds are inclusive,
lookback is open-left, and `:step` must be positive. A stored NaN still emits a
grid row—its row-visible `value` may be SQL NULL, but that timestamp is
correctly present because this statement never filters on the value. If no
series or samples match, every evaluation timestamp returns `1.0`.

For a direct selector, bind `:output_labels_json` to the canonical object
containing each unique, nonempty equality matcher except `__name__`; regex,
negative, empty, and duplicate matcher names contribute no output label. Bind
`{}` for a composed input expression. That label derivation and arbitrary AST
composition remain Rust API responsibilities; direct SQL callers already
know the bounded selection they are testing and can bind the intended output
object explicitly. The API also owns parser types, sparse matrix assembly,
cumulative grid work, result/response limits, cancellation, and Prometheus
error envelopes. No absence-specific extension primitive is warranted.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_absent_derives_labels_per_step_and_reopens`.

### SQL-PROM-048: `absent_over_time`

Generate the evaluation grid, read the complete bounded raw interval once,
and retain only steps whose open-left, closed-right window contains no sample
from any selected series:

```sql
WITH RECURSIVE evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step
  FROM evaluation
  WHERE ts + :step <= :end
), present AS (
  SELECT DISTINCT evaluation.ts
  FROM evaluation
  JOIN timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :window, :end
  ) AS raw
    ON raw.ts > evaluation.ts - :window
   AND raw.ts <= evaluation.ts
)
SELECT :output_labels_json AS labels,
       evaluation.ts,
       1.0 AS value
FROM evaluation
LEFT JOIN present USING (ts)
WHERE present.ts IS NULL
ORDER BY evaluation.ts;
```

`:start`, `:end`, `:step`, and `:window` use the metric table's configured
timestamp unit (integer seconds for the default table); `:step` and `:window`
must be positive. The evaluation bounds are inclusive, while each sample
window is exactly `(evaluation.ts - :window, evaluation.ts]`. The query tests
row existence rather than a projected SQL value, so every stored float class,
including NaN, counts as present. It emits one `1.0` at each absent step and
deterministically orders those timestamps.

For a direct selector, bind `:output_labels_json` to the canonical object
containing each unique, nonempty equality matcher except `__name__`; exclude
regex, negative, empty, and duplicate matcher names. The Rust API derives that
object, composes subqueries using `{}`, assembles sparse Prometheus vector or
matrix envelopes, and enforces parser types, cumulative work/response limits,
cancellation, and error semantics. Direct SQLite/libSQL callers already own
the bounded selector and can bind the intended labels explicitly. The public
raw read is complete and efficient enough, so no absence-specific extension
primitive is justified.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_absent_over_time_pins_windows_subqueries_limits_and_reopen`.

### SQL-PROM-049: `sort` and `sort_desc`

For an instant-vector selection, order finite and infinite samples in the
requested direction, place row-projected NaN last in both directions, and use
canonical labels as a deterministic tie-break:

```sql
SELECT labels, ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :at, :at, 1, :lookback
)
ORDER BY value IS NULL,
         CASE WHEN :descending = 0 THEN value END ASC,
         CASE WHEN :descending <> 0 THEN value END DESC,
         labels;
```

`:at` and `:lookback` use the metric table's configured timestamp unit
(integer seconds for the default table), and lookback is open-left. Bind
`:descending` to zero for `sort` and nonzero for `sort_desc`. The public grid
preserves labels, metric identity, timestamp, and ordinary REAL values. SQLite
projects a stored NaN as SQL NULL; because metric inserts cannot store an
ordinary SQL NULL and this is a sparse grid, `value IS NULL` identifies that
stored IEEE class for ordering. The Rust API reads the public bit-exact packed
frame so its response preserves `NaN` rather than exposing SQL NULL. Numeric
ties, including signed zeros, use label order because PromQL does not promise
an order within an equal-value group.

Prometheus sorting affects only instant vectors. A range-query endpoint must
return its matrix in canonical label-set order regardless of the value order
at any individual step:

```sql
SELECT labels, ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

The Rust API owns PromQL parsing and types, nested expression composition,
instant-versus-range response semantics, exact float strings, cumulative work
and response limits, and cancellation. Both forms use one bounded public read;
there is no storage-read or row-crossing evidence for a sort-specific
extension primitive.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_sort_orders_ieee_instants_not_range_matrices_and_reopens`.

### SQL-PROM-050: `scalar` and `vector`

Convert a bounded instant vector to one scalar value per evaluation step. An
exactly one-sample step retains its value; zero or multiple samples become SQL
NULL, the row-projected equivalent of PromQL NaN:

```sql
WITH RECURSIVE evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step
  FROM evaluation
  WHERE ts + :step <= :end
), selected AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT evaluation.ts,
       CASE WHEN COUNT(selected.ts) = 1
            THEN MAX(selected.value)
            ELSE NULL
       END AS value
FROM evaluation
LEFT JOIN selected USING (ts)
GROUP BY evaluation.ts
ORDER BY evaluation.ts;
```

`:start`, `:end`, `:step`, and `:lookback` use the metric table's configured
timestamp unit (integer seconds for the default table); the grid is inclusive
and lookback is open-left. `COUNT(selected.ts)` counts every actual grid row,
including a stored NaN whose row-visible value is NULL. `MAX(value)` returns
NULL for that one stored NaN, correctly retaining the SQL projection of the
PromQL result. The Rust API reads packed float bits and therefore emits the
distinguishable Prometheus `NaN` string for empty, multiple, or stored-NaN
cases.

Convert a scalar expression to a nameless instant vector by attaching the
empty label set to every evaluation timestamp:

```sql
WITH RECURSIVE evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step
  FROM evaluation
  WHERE ts + :step <= :end
)
SELECT '{}' AS labels, ts, :scalar_value AS value
FROM evaluation
ORDER BY ts;
```

Direct callers can replace `:scalar_value` with an ordinary scalar SQL
expression or a joined scalar CTE. The Rust API owns PromQL expression typing,
nested AST composition, exact scalar/vector instant and range envelopes,
float-string formatting, per-step cardinality, cumulative work/response
limits, and cancellation. These conversions operate on already-bounded
results and need no extension primitive.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_scalar_vector_convert_types_cardinality_and_reopen`.

### SQL-PROM-051: `time` and `timestamp`

`time()` is the evaluation clock expressed as Unix seconds. For a default
second-native metric table, generate it directly from the requested grid:

```sql
WITH RECURSIVE evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step
  FROM evaluation
  WHERE ts + :step <= :end
)
SELECT ts, CAST(ts AS REAL) AS value
FROM evaluation
ORDER BY ts;
```

`:start`, `:end`, and `:step` are inclusive integer seconds here. The Rust API
uses a millisecond evaluation clock and divides by `1000.0`, preserving
fractional-second range steps and Prometheus scalar versus range-matrix
envelopes.

For `timestamp(direct_selector)`, retain the latest selected stored sample in
each open-left lookback window, emit its storage timestamp as the value, and
keep the outer evaluation timestamp as the response timestamp:

```sql
WITH RECURSIVE evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step
  FROM evaluation
  WHERE ts + :step <= :end
), samples AS (
  SELECT labels, ts
  FROM timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :lookback, :end
  )
), candidates AS (
  SELECT samples.labels,
         evaluation.ts AS response_ts,
         samples.ts AS sample_ts,
         ROW_NUMBER() OVER (
           PARTITION BY samples.labels, evaluation.ts
           ORDER BY samples.ts DESC
         ) AS rank
  FROM evaluation
  JOIN samples
    ON samples.ts <= evaluation.ts
   AND samples.ts > evaluation.ts - :lookback
)
SELECT labels,
       response_ts AS ts,
       CAST(sample_ts AS REAL) AS value
FROM candidates
WHERE rank = 1
ORDER BY labels, response_ts;
```

All parameters use the table's configured timestamp unit (integer seconds for
the default table). Labels exclude `__name__`, as required by `timestamp`.
Duplicate values at the same latest timestamp have the same timestamp result,
so their internal tie order is immaterial. Direct callers implement `offset`
or a fixed `@` anchor by shifting or replacing the selection-side evaluation
timestamp while retaining `response_ts`.

Prometheus distinguishes direct selector provenance from newly composed
samples. A direct selector—including `offset` and `@`—reports the selected
stored sample timestamp. Unary, value/label/sort functions, binary operators,
aggregations, and range functions create samples at the outer evaluation time;
`timestamp` over those expressions therefore returns that evaluation time.
The Rust AST owns that distinction, parser types, composition, exact output
timestamps and float strings, limits, cancellation, and response envelopes.
The public raw scan already exposes every storage timestamp needed by direct
SQLite/libSQL users, so no timestamp-specific extension primitive is needed.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_time_timestamp_pin_clock_provenance_and_reopen`.

### SQL-PROM-052: `minute`, `hour`, `day_of_week`, and `day_of_month`

Extract a UTC calendar component from the sample values of a bounded public
instant grid:

```sql
WITH selected AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT labels, ts,
       CASE :part
         WHEN 'minute' THEN CAST(strftime('%M', CAST(value AS INTEGER), 'unixepoch') AS INTEGER)
         WHEN 'hour' THEN CAST(strftime('%H', CAST(value AS INTEGER), 'unixepoch') AS INTEGER)
         WHEN 'day_of_week' THEN CAST(strftime('%w', CAST(value AS INTEGER), 'unixepoch') AS INTEGER)
         WHEN 'day_of_month' THEN CAST(strftime('%d', CAST(value AS INTEGER), 'unixepoch') AS INTEGER)
       END AS value
FROM selected
ORDER BY labels, ts;
```

`:start`, `:end`, `:step`, and `:lookback` use the metric table's timestamp
unit; each selected metric `value` is independently interpreted as Unix
seconds. `:part` must be one of the four names in the statement. Bounds are
inclusive and lookback is open-left. The inner integer cast implements
Prometheus's truncation toward zero for negative and positive fractions;
SQLite's `%w` already numbers Sunday as zero. Labels exclude the metric name
on this public table and ordering is canonical label then evaluation timestamp.

The zero-argument form uses the evaluation timestamp itself. For a
second-native grid it is:

```sql
SELECT '{}' AS labels, :evaluation_ts AS ts,
       CASE :part
         WHEN 'minute' THEN CAST(strftime('%M', CAST(:evaluation_ts AS REAL), 'unixepoch') AS INTEGER)
         WHEN 'hour' THEN CAST(strftime('%H', CAST(:evaluation_ts AS REAL), 'unixepoch') AS INTEGER)
         WHEN 'day_of_week' THEN CAST(strftime('%w', CAST(:evaluation_ts AS REAL), 'unixepoch') AS INTEGER)
         WHEN 'day_of_month' THEN CAST(strftime('%d', CAST(:evaluation_ts AS REAL), 'unixepoch') AS INTEGER)
       END AS value;
```

This ordinary SQL is an honest foundation, not a claim of complete PromQL
semantics. SQLite projects a stored NaN as NULL and its date functions return
NULL outside their supported calendar range. Prometheus instead truncates
finite fractional seconds toward zero and maps NaN, both infinities, and
out-of-range values to its maximum Unix-second calendar
(`292277026596-12-04 15:30:07 UTC`). The Rust API
reads packed float bits and implements that full-domain behavior, optional
argument defaulting, AST types, metric-name removal, exact result envelopes,
limits, and cancellation. No calendar-specific extension primitive or extra
storage read is justified.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_calendar_part_one_uses_utc_defaults_and_reopens`.

### SQL-PROM-053: `day_of_year`, `days_in_month`, `month`, and `year`

Use the same bounded public grid and integer-second conversion as
`SQL-PROM-052`, selecting the requested UTC field:

```sql
WITH selected AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT labels, ts,
       CASE :part
         WHEN 'day_of_year' THEN CAST(strftime('%j', CAST(value AS INTEGER), 'unixepoch') AS INTEGER)
         WHEN 'days_in_month' THEN CAST(strftime('%d', CAST(value AS INTEGER), 'unixepoch', 'start of month', '+1 month', '-1 day') AS INTEGER)
         WHEN 'month' THEN CAST(strftime('%m', CAST(value AS INTEGER), 'unixepoch') AS INTEGER)
         WHEN 'year' THEN CAST(strftime('%Y', CAST(value AS INTEGER), 'unixepoch') AS INTEGER)
       END AS value
FROM selected
ORDER BY labels, ts;
```

`:part` is one of the four names above. Metric values are Unix seconds;
`:start`, `:end`, `:step`, and `:lookback` remain in the metric table's native
timestamp unit. Bounds are inclusive, lookback is open-left, fractions
truncate toward zero, day-of-year and month are one-indexed, and Gregorian
leap-year rules determine February length. Rows are canonically ordered by
labels then evaluation timestamp.

For a zero-argument function, evaluate the same operation over the requested
clock with an empty label set:

```sql
WITH selected(labels, ts, value) AS (
  VALUES ('{}', :evaluation_ts, CAST(:evaluation_ts AS INTEGER))
)
SELECT labels, ts,
       CASE :part
         WHEN 'day_of_year' THEN CAST(strftime('%j', value, 'unixepoch') AS INTEGER)
         WHEN 'days_in_month' THEN CAST(strftime('%d', value, 'unixepoch', 'start of month', '+1 month', '-1 day') AS INTEGER)
         WHEN 'month' THEN CAST(strftime('%m', value, 'unixepoch') AS INTEGER)
         WHEN 'year' THEN CAST(strftime('%Y', value, 'unixepoch') AS INTEGER)
       END AS value
FROM selected;
```

SQLite's date functions still cover only their documented calendar range and
project stored NaN/out-of-range inputs as NULL. The Rust
API owns the pinned maximum-second sentinel, packed IEEE fidelity, optional
argument and AST typing, metric-name removal, millisecond evaluation grids,
limits, cancellation, and Prometheus envelopes. Existing public results
already contain every value required, so no extension primitive is justified.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_eight_promql_calendar_part_two_pins_leap_years_and_reopens`.

### SQL-PROM-054: `histogram_quantile` over classic buckets

For a classic histogram metric whose series carry cumulative counts in an
`le` label, this parameterized ordinary-SQL recipe computes one quantile per
remaining label set and evaluation step:

```sql
WITH selected AS (
  SELECT json_remove(labels, '$.le') AS labels, ts,
         json_extract(labels, '$.le') AS le, value AS count
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
), parsed AS (
  SELECT labels, ts,
         CASE WHEN le IN ('+Inf','Inf','+Infinity','Infinity')
              THEN 1 ELSE 0 END AS is_inf,
         CASE
           WHEN le IN ('+Inf','Inf','+Infinity','Infinity') THEN NULL
           WHEN le IN ('-Inf','-Infinity') THEN -1e999
           ELSE CAST(le AS REAL)
         END AS upper_bound,
         count
  FROM selected
  WHERE le IS NOT NULL
), coalesced AS (
  SELECT labels, ts, is_inf, upper_bound, SUM(count) AS count
  FROM parsed
  GROUP BY labels, ts, is_inf, upper_bound
), monotonic AS (
  SELECT labels, ts, is_inf, upper_bound,
         MAX(count) OVER (
           PARTITION BY labels, ts
           ORDER BY is_inf, upper_bound
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS count
  FROM coalesced
), positioned AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY labels, ts ORDER BY is_inf, upper_bound
         ) AS position,
         LAG(upper_bound) OVER (
           PARTITION BY labels, ts ORDER BY is_inf, upper_bound
         ) AS lower_bound,
         LAG(count) OVER (
           PARTITION BY labels, ts ORDER BY is_inf, upper_bound
         ) AS lower_count
  FROM monotonic
), stats AS (
  SELECT labels, ts,
         MAX(CASE WHEN is_inf = 1 THEN count END) AS total,
         COUNT(*) AS buckets,
         MAX(CASE WHEN is_inf = 0 THEN upper_bound END) AS last_finite
  FROM positioned
  GROUP BY labels, ts
), chosen AS (
  SELECT p.*,
         ROW_NUMBER() OVER (
           PARTITION BY p.labels, p.ts ORDER BY p.upper_bound
         ) AS choice
  FROM positioned AS p
  JOIN stats AS s USING (labels, ts)
  WHERE p.is_inf = 0 AND p.count >= :quantile * s.total
)
SELECT s.labels, s.ts,
       CASE
         WHEN s.total IS NULL OR s.total = 0 OR s.buckets < 2 THEN NULL
         WHEN :quantile < 0 THEN -1e999
         WHEN :quantile > 1 THEN 1e999
         WHEN c.choice IS NULL THEN s.last_finite
         WHEN c.position = 1 AND c.upper_bound <= 0 THEN c.upper_bound
         ELSE COALESCE(c.lower_bound, 0)
              + (c.upper_bound - COALESCE(c.lower_bound, 0))
              * ((:quantile * s.total - COALESCE(c.lower_count, 0))
                 / (c.count - COALESCE(c.lower_count, 0)))
       END AS value
FROM stats AS s
LEFT JOIN chosen AS c
  ON c.labels = s.labels AND c.ts = s.ts AND c.choice = 1
ORDER BY s.labels, s.ts;
```

`:metric` names one classic `*_bucket` family; `:filter_json` is a public
Timeless matcher object or NULL. `:start`, `:end`, `:step`, and `:lookback`
use the metric table's native timestamp unit, bounds are inclusive, and
lookback is open-left. `:quantile` is the scalar rank. Input label JSON must be
canonical and each participating series must have a valid numeric `le` or a
positive-infinity spelling shown above. Equal parsed bounds are summed,
output labels omit `le`, rows are ordered by labels then timestamp, the first
positive bucket interpolates from zero, and a rank in the infinite bucket
returns the largest finite bound. NULL represents the no-`+Inf`, fewer-than-two
buckets, or zero-total result that the API serializes as `NaN`.

This recipe is an honest direct-SQL foundation, not the PromQL parser. Its
running maximum provides ordinary monotonic correction, but SQL numeric casts
do not provide Prometheus's strict float-label parser, raw packed NaN fidelity,
or the `1e-12` relative-delta suppression rule. The Rust API supplies those
rules, keeps metric names as internal family discriminators before dropping
them from output, evaluates scalar ASTs per step, and owns warnings/infos,
limits, cancellation, and Prometheus envelopes. The bounded public grid
already crosses each selected bucket once, so no histogram-specific extension
primitive is justified.

Direct regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_nine_promql_classic_histogram_quantile_matches_oracle_and_reopens`.

### SQL-PROM-055: `atan2`

SQLite's ordinary two-argument math function uses the same operand orientation
as PromQL: `atan2(Y, X)`. Apply a scalar right operand to every selected
sample with:

```sql
SELECT labels, ts, atan2(value, CAST(:scalar AS REAL)) AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

Two stored vectors can be matched by canonical labels and evaluation time:

```sql
WITH lhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
), rhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT lhs.labels, lhs.ts, atan2(lhs.value, rhs.value) AS value
FROM lhs
JOIN rhs ON rhs.labels = lhs.labels AND rhs.ts = lhs.ts
ORDER BY lhs.labels, lhs.ts;
```

`:metric`, `:lhs_metric`, and `:rhs_metric` are exact stored metric names;
the filter parameters are public Timeless matcher objects or NULL. Metric
timestamps and `:start`, `:end`, `:step`, and `:lookback` are integer seconds.
Grid bounds are inclusive, sample lookback is `(T-lookback,T]`, unmatched and
empty inputs emit no row, and rows are ordered by canonical labels then time.
Values are SQLite REALs. The first statement demonstrates vector/scalar
direction; exchange the arguments for scalar/vector direction.

This is the bounded SQL execution foundation, not a byte-exact PromQL
implementation. SQLite delegates `atan2` to its platform math implementation,
which can differ from Go's Cephes-derived `math.Atan2` by one last-place bit
(for example, `atan2(8,2)`). The Rust API uses a deterministic Go-compatible
kernel and additionally owns scalar/scalar result types, `on`/`ignoring` and
group matching, cardinality failures, metric-name removal, packed NaN and
signed-zero strings, cumulative limits, cancellation, and HTTP envelopes. A
special extension primitive would not reduce storage reads or decoded points.

Executable regression: Rust SQL-equivalent harness `SQL-PROM-055` semantic
check; HTTP/oracle/reopen regression:
`session_eleven_promql_atan2_matches_scalar_vector_ieee_and_reopens`.

### SQL-PROM-056: `histogram_fraction` over classic buckets

For one classic histogram family, this parameterized ordinary-SQL recipe
estimates the fraction of observations between `:lower` and `:upper` at each
evaluation step:

```sql
WITH selected AS (
  SELECT json_remove(labels, '$.le') AS labels, ts,
         json_extract(labels, '$.le') AS le, value AS count
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
), parsed AS (
  SELECT labels, ts,
         CASE
           WHEN le IN ('+Inf','Inf','+Infinity','Infinity') THEN 1e999
           WHEN le IN ('-Inf','-Infinity') THEN -1e999
           ELSE CAST(le AS REAL)
         END AS upper_bound,
         count
  FROM selected
  WHERE le IS NOT NULL
), coalesced AS (
  SELECT labels, ts, upper_bound, SUM(count) AS count
  FROM parsed
  GROUP BY labels, ts, upper_bound
), ordered AS (
  SELECT *,
         LAG(upper_bound) OVER (
           PARTITION BY labels, ts ORDER BY upper_bound
         ) AS previous_bound,
         LAG(count) OVER (
           PARTITION BY labels, ts ORDER BY upper_bound
         ) AS previous_count
  FROM coalesced
), positioned AS (
  SELECT labels, ts, upper_bound, count,
         COALESCE(
           previous_bound,
           CASE WHEN upper_bound > 0 THEN 0.0 ELSE -1e999 END
         ) AS lower_bound,
         COALESCE(previous_count, 0.0) AS lower_count
  FROM ordered
), stats AS (
  SELECT labels, ts,
         MAX(CASE WHEN upper_bound = 1e999 THEN count END) AS total
  FROM positioned
  GROUP BY labels, ts
), bounds(kind, bound) AS (
  SELECT 'lower', CAST(:lower AS REAL)
  UNION ALL
  SELECT 'upper', CAST(:upper AS REAL)
), candidates AS (
  SELECT s.labels, s.ts, s.total, b.kind, b.bound,
         p.lower_bound, p.upper_bound, p.lower_count, p.count,
         ROW_NUMBER() OVER (
           PARTITION BY s.labels, s.ts, b.kind
           ORDER BY p.upper_bound
         ) AS choice
  FROM stats AS s
  CROSS JOIN bounds AS b
  JOIN positioned AS p USING (labels, ts)
  WHERE p.upper_bound >= b.bound
), evaluated AS (
  SELECT labels, ts, total, kind,
         CASE
           WHEN bound = -1e999 THEN 0.0
           WHEN bound = 1e999 THEN total
           WHEN bound <= lower_bound THEN lower_count
           WHEN lower_bound = -1e999 THEN count
           WHEN upper_bound = 1e999 THEN lower_count
           ELSE lower_count + (count - lower_count)
                * (bound - lower_bound) / (upper_bound - lower_bound)
         END AS rank
  FROM candidates
  WHERE choice = 1
), ranks AS (
  SELECT labels, ts, total,
         MAX(CASE WHEN kind = 'lower' THEN rank END) AS lower_rank,
         MAX(CASE WHEN kind = 'upper' THEN rank END) AS upper_rank
  FROM evaluated
  GROUP BY labels, ts, total
)
SELECT labels, ts,
       CASE
         WHEN total IS NULL OR total = 0 THEN NULL
         WHEN :lower >= :upper THEN 0.0
         ELSE (upper_rank - lower_rank) / total
       END AS value
FROM ranks
ORDER BY labels, ts;
```

`:metric` names one classic `*_bucket` family and `:filter_json` is a public
Timeless matcher object or NULL. Metric timestamps and `:start`, `:end`,
`:step`, and `:lookback` use the table's native unit; the public grid uses an
open-left lookback and inclusive evaluation grid. Bounds are SQLite REALs;
bind `-Inf`/`+Inf` where the host supports them. Equal parsed `le` bounds are
summed, output labels omit `le`, the first positive bucket begins at zero,
finite bounds inside a bucket interpolate linearly, and rows are deterministic
by labels then timestamp. NULL represents a missing `+Inf` bucket or zero
total, which the API serializes as `NaN`.

This is the bounded direct-SQL foundation, not the PromQL parser. Ordinary SQL
does not provide the API's strict OpenMetrics bound parser, packed NaN and
signed-zero behavior, arbitrary scalar-expression bounds, metric-family
collision checks, work limits, cancellation, or warning/result envelopes.
The Rust API supplies those semantics and retains metric names internally
until bucket families have been separated. The public grid already crosses
each selected bucket once, so a histogram-specific extension primitive would
not reduce storage reads or decoding.

Executable regression: Rust SQL-equivalent harness `SQL-PROM-056`; HTTP,
oracle, compact, and reopen regression:
`session_fourteen_histogram_fraction_matches_classic_buckets_and_reopens`.

### SQL-PROM-006: range selector

At instant evaluation timestamp `:at`, return every stored float sample in
the PromQL range-vector interval `(:at - :window, :at]`:

```sql
SELECT labels, ts, value
FROM timeless_raw(
  'metrics', :metric, :filter_json,
  :at - :window, :at
)
WHERE ts > :at - :window
  AND ts <= :at
ORDER BY labels, ts;
```

`:at` and `:window` are integer seconds for a default `timeless_metrics`
table. The explicit predicates turn the public raw surface's inclusive read
bounds into PromQL's open-left, closed-right interval. Values remain SQLite
REALs, including IEEE NaN/Inf where the SQLite build preserves them; labels
are canonical JSON. The Rust API owns parsing, matrix envelopes, value-string
formatting, limits, and rejection of a root range vector on a range query.
Direct regression: `tests/cli.sh` section 33 and the metrics API
`session_four_pins_promql_selector_window_errors_and_reopen` contract.

### SQL-PROM-007: bounded packed storage work

Direct SQLite/libSQL callers can cap the conservative number of stored or
buffered points inspected by packed raw and window reads:

```sql
SELECT frame
FROM timeless_raw_frame(
  'metrics', :metric, :filter_json,
  :start, :end, :max_work_points
);

SELECT series_id, labels, buckets
FROM timeless_window_batches(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :window, :aggregate,
  NULL, :max_work_points
)
ORDER BY labels, series_id;
```

All metric timestamps are integer seconds. Bounds are inclusive at the raw
storage surface; window samples use `(T-window,T]`. `:max_work_points` is a
positive SQLite INTEGER and is inclusive. The extension checks persisted
chunk point counts plus current buffer lengths before reading chunk payloads.
For a packed window it independently checks the conservative input count and
`matched series × grid points`, so neither decode nor packed output can exceed
the bound. An excess returns an error and no partial frame/rows. `NULL`, zero,
negative, and non-integer limits fail; omitting the trailing argument retains
the unbounded backward-compatible SQL call.

This is the storage foundation, not an SQL reimplementation of PromQL resource
semantics. The Rust metrics API additionally owns the fixed 11,000-point grid
ceiling, final result cardinality, serialized response bytes, cancellation,
deadline/error envelopes, and tighter auth-claim policy. Direct regressions:
`tests/cli.sh` sections 22, 34, and 45; core buffered/persisted coverage:
`write_flush_query_recover`; HTTP/reopen coverage:
`session_two_promql_limits_bound_grid_work_results_response_and_deadline`.

### SQL-PROM-008: temporal selector modifiers

For a selector with signed `offset`, shift the lookup grid by `:offset` and
restore the outer evaluation timestamp in the projection. Positive offsets
look into the past; negative offsets look into the future:

```sql
SELECT labels, ts + :offset AS ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start - :offset, :end - :offset, :step, :lookback
)
ORDER BY labels, ts;
```

For `@` forms, first resolve `:anchor` to the numeric timestamp, query once at
`:anchor - :offset`, and cross that value with the outer evaluation grid:

```sql
WITH RECURSIVE evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), selected AS (
  SELECT labels, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :anchor - :offset, :anchor - :offset, 1, :lookback
  )
)
SELECT selected.labels, evaluation.ts, selected.value
FROM selected CROSS JOIN evaluation
ORDER BY selected.labels, evaluation.ts;
```

All parameters use the metric table's native timestamp unit (seconds for the
default table). `:offset` is signed; bind zero when absent. For numeric `@`,
`:anchor` is that timestamp. For `@ start()` and `@ end()`, bind `:start` and
`:end`, respectively. The order is always anchor first, offset second. Output
timestamps are the outer grid, while lookback and the open-left boundary are
relative to lookup time. Missing series remain sparse. The API owns parsing,
millisecond conversion, overflow errors, range-vector raw timestamps,
function label policy, limits, cancellation, and Prometheus envelopes. Direct
regression: `tests/cli.sh` section 45; HTTP/oracle/reopen regression:
`session_three_promql_temporal_modifiers_preserve_selection_and_output_time`.

### SQL-PROM-009: aligned selector subquery

For a selector subquery `metric{...}[:window::resolution]` at `:at`, derive
the globally aligned evaluation grid and run the ordinary public selector
surface on it:

```sql
WITH timing AS (
  SELECT
    :at - :offset AS effective_at,
    :window AS window,
    :resolution AS resolution
), bounds AS (
  SELECT
    effective_at - window AS lower,
    effective_at,
    resolution,
    ((effective_at - window) % resolution + resolution) % resolution AS lower_mod,
    (effective_at % resolution + resolution) % resolution AS upper_mod
  FROM timing
), aligned AS (
  SELECT
    lower - lower_mod + resolution AS first_ts,
    effective_at - upper_mod AS last_ts,
    resolution
  FROM bounds
)
SELECT labels, ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  (SELECT first_ts FROM aligned),
  (SELECT last_ts FROM aligned),
  (SELECT resolution FROM aligned),
  :lookback
)
WHERE (SELECT first_ts <= last_ts FROM aligned)
ORDER BY labels, ts;
```

All parameters use the metric table's native unit: integer seconds for the
default table. `:window`, `:resolution`, and `:lookback` must be positive;
`:offset` is signed and defaults to zero. The normalized modulo expressions
preserve Euclidean/global alignment for pre-epoch timestamps. Adding one
resolution after the aligned floor makes the range open on the left, so this
returns evaluations strictly inside
`(:at - :offset - :window, :at - :offset]`. `timeless_grid` supplies the
newest stored float in each evaluation point's own open-left lookback window;
missing evaluations remain sparse. Labels are canonical JSON and ordering is
deterministic by labels then timestamp.

This is exact storage work for a selector subquery. The Rust API still owns
PromQL parsing, a configurable default resolution when the colon has no
duration, `@ start()`/`@ end()` outer context, arbitrary/nested instant-vector
inner expressions, matrix envelopes, function consumption, label/name policy,
millisecond conversion, intermediate/result limits, cancellation, and
deadline errors. Direct executable regression: `tests/cli.sh` section 45;
HTTP/oracle/reopen regression:
`session_three_promql_subqueries_align_bound_cancel_and_reopen`.

### SQL-PROM-010: unary minus

For the vector expression `-metric{...}`, negate values from the ordinary
bounded selector grid:

```sql
SELECT labels, ts, -value AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

All timestamp parameters use the virtual table's native unit (integer seconds
for the default metric table). `:start` and `:end` are inclusive output-grid
bounds, `:step` and `:lookback` are positive, and `:filter_json` is either
`NULL` or the documented matcher object. `timeless_grid` returns canonical
label JSON without a metric-name field, sparse rows when no sample exists in
the open-left `(T-lookback,T]` window, and deterministic label/timestamp
ordering. SQLite's unary numeric operator preserves ordinary finite values and
infinities supported by the host build; portable SQL must not claim
Prometheus's exact `NaN` string behavior.

This statement is the exact stored-vector arithmetic foundation. The Rust API
owns PromQL parsing, scalar versus vector types, removal of `__name__`,
millisecond evaluation, `NaN`/`+Inf`/`-Inf` response strings, nested AST
composition, intermediate/result/response limits, cancellation, and the
Prometheus envelope. Direct executable regression: `tests/cli.sh` section 33;
HTTP/oracle/reopen regression:
`session_four_promql_unary_minus_preserves_types_labels_limits_and_reopen`.

### SQL-PROM-011: comparison filter and `bool`

For a vector/scalar filter such as `metric > :threshold`, ordinary SQL returns
the original sample value only when the predicate is true:

```sql
SELECT labels, ts, value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
WHERE value > CAST(:threshold AS REAL)
ORDER BY labels, ts;
```

The `bool` form retains every selected grid sample and maps the predicate to a
floating `0` or `1`:

```sql
SELECT labels, ts,
       CAST(value > CAST(:threshold AS REAL) AS REAL) AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

Substitute `=`, `!=`, `>`, `<`, `>=`, or `<=` as required (`==` in PromQL is
SQLite `=`). Timestamp units, inclusive grid bounds, open-left lookback,
sparse missing samples, canonical labels, and deterministic ordering are the
same as `SQL-PROM-004`. A vector/vector comparison uses the exact-label public
grid join from that recipe and moves the predicate into `WHERE` or `CASE`.

The Rust API additionally enforces scalar/scalar `bool`, performs
per-timestamp PromQL vector matching/cardinality checks, preserves the
vector's original value and metric name for a filter, removes the metric name
for `bool`, renders IEEE results, bounds cumulative work, checks cancellation,
and writes Prometheus envelopes. Direct regression: `tests/cli.sh` section 33;
HTTP/oracle/reopen regression:
`session_four_promql_comparisons_filter_bool_bound_and_reopen`.

### SQL-PROM-012: set membership

PromQL set operators compare label signatures independently at every
evaluation timestamp and ignore sample values while deciding membership. For
two exact metric selectors, public grids make that operation ordinary SQL:

```sql
WITH
lhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
),
rhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT labels, ts, value
FROM lhs
WHERE EXISTS (
  SELECT 1 FROM rhs
  WHERE rhs.labels = lhs.labels AND rhs.ts = lhs.ts
)
ORDER BY labels, ts;
```

That is `lhs and rhs`. Change `EXISTS` to `NOT EXISTS` for `unless`. The
left-preferred union for `or` is:

```sql
WITH
lhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
),
rhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT labels, ts, value FROM lhs
UNION ALL
SELECT rhs.labels, rhs.ts, rhs.value
FROM rhs
WHERE NOT EXISTS (
  SELECT 1 FROM lhs
  WHERE lhs.labels = rhs.labels AND lhs.ts = rhs.ts
)
ORDER BY labels, ts;
```

All time parameters use the metric table's native unit. Grid bounds are
inclusive, each lookup window is open on the left, absent samples do not
participate, values may be any SQLite REAL, and canonical label JSON makes
equality exact and ordering deterministic. Set membership is many-to-many:
every left series in a matching group survives `and`, every such series is
removed by `unless`, and `or` keeps all left series while suppressing matching
right series. For a side containing multiple metric names, `UNION ALL` the
bounded public grids and carry a literal `name` column so the chosen side's
name can be projected.

The Rust API owns PromQL parsing, nameless/multi-name planning, per-step
membership, source metric-name preservation, millisecond timestamps,
intermediate/result/response limits, cancellation, and Prometheus envelopes.
Direct executable regression: `tests/cli.sh` section 33; HTTP/oracle/reopen
regression:
`session_four_promql_set_operators_are_many_to_many_stepwise_and_reopen`.

### SQL-PROM-013: `on` and `ignoring` label matching

For `lhs + on(host) rhs`, treat an absent matching label as the empty string,
join by that value and timestamp, and project only the matching label:

```sql
WITH
lhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
),
rhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object(
    'host', COALESCE(json_extract(lhs.labels, '$.host'), '')
  ) AS labels,
  lhs.ts,
  lhs.value + rhs.value AS value
FROM lhs
JOIN rhs
  ON rhs.ts = lhs.ts
 AND COALESCE(json_extract(rhs.labels, '$.host'), '') =
     COALESCE(json_extract(lhs.labels, '$.host'), '')
ORDER BY labels, lhs.ts;
```

For `lhs + ignoring(zone) rhs`, remove the ignored key before comparing and
projecting the left labels:

```sql
WITH
lhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
),
rhs AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_remove(lhs.labels, '$.zone') AS labels,
  lhs.ts,
  lhs.value + rhs.value AS value
FROM lhs
JOIN rhs
  ON rhs.ts = lhs.ts
 AND json_remove(rhs.labels, '$.zone') =
     json_remove(lhs.labels, '$.zone')
ORDER BY labels, lhs.ts;
```

Pass more JSON paths to `json_remove` for a multi-label `ignoring` list. For
multi-label `on`, compare each named `COALESCE(json_extract(...), '')` term
and construct the projected canonical label object in lexical key order.
`on()` has no label terms, so every series at a timestamp belongs to one match
group; one-to-one language operations must reject a group with duplicates.

All time parameters use the table's native unit; grids have inclusive output
bounds and open-left lookback windows. Missing samples do not participate.
For arithmetic and `bool`, the metric name is absent; a comparison filter with
`on` also projects only the named labels, while `ignoring` removes only the
listed labels. Set operators use the modified comparison signature but retain
the full contributing labelset. The Rust API enforces those per-operator name
rules, one-to-one cardinality errors, AST validation, bounded work,
cancellation, and Prometheus envelopes. Direct executable regression:
`tests/cli.sh` section 33; HTTP/oracle/reopen regression:
`session_four_promql_on_ignoring_match_labels_names_limits_and_reopen`.

### SQL-PROM-014: `group_left` and `group_right`

For `many + on(service) group_left(team) one`, join a many-side grid to a
one-side grid by the explicit key and copy `team` from the unique right side:

```sql
WITH
many AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :many_metric, :many_filter,
    :start, :end, :step, :lookback
  )
),
one AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :one_metric, :one_filter,
    :start, :end, :step, :lookback
  )
)
SELECT
  CASE
    WHEN COALESCE(json_extract(one.labels, '$.team'), '') = ''
      THEN json_remove(many.labels, '$.team')
    ELSE json_set(
      many.labels, '$.team', json_extract(one.labels, '$.team')
    )
  END AS labels,
  many.ts,
  many.value + one.value AS value
FROM many
JOIN one
  ON one.ts = many.ts
 AND COALESCE(json_extract(one.labels, '$.service'), '') =
     COALESCE(json_extract(many.labels, '$.service'), '')
ORDER BY labels, many.ts;
```

Before executing, the direct caller must prove that the one side is unique at
each match key and timestamp:

```sql
WITH one AS (
  SELECT labels, ts
  FROM timeless_grid(
    'metrics', :one_metric, :one_filter,
    :start, :end, :step, :lookback
  )
)
SELECT
  COALESCE(json_extract(labels, '$.service'), '') AS match_service,
  ts,
  COUNT(*) AS matches
FROM one
GROUP BY match_service, ts
HAVING COUNT(*) > 1;
```

Any returned row is a cardinality error, not permission to execute a Cartesian
join. `group_right` swaps which side supplies the base labelset and which side
must be unique, but never swaps the language operation. For example,
`one - on(service) group_right(team) many` projects from `many`, copies `team`
from `one`, and still calculates `one.value - many.value`.

Time units, inclusive grid bounds, open-left lookback, missing sample behavior,
and canonical label ordering follow `SQL-PROM-013`. A missing or empty included
label removes that label from the result. Included labels that collapse two
many-side results to the same labelset at one timestamp are also an execution
error. Arithmetic removes the metric name. A `group_right` comparison filter
uses the right metric identity but retains the original left value; the Rust
API owns that directionality, per-step uniqueness errors, result splitting,
limits, cancellation, and envelopes. Direct executable regression:
`tests/cli.sh` section 33; HTTP/oracle/reopen regression:
`session_four_promql_group_matching_direction_labels_limits_and_reopen`.

### SQL-PROM-003: cross-series sum by label

Equivalent mechanical reduction for `sum by (service) (metric)`:

```sql
WITH selected AS (
  SELECT
    ts,
    json_extract(labels, '$.service') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object('service', service) AS labels,
  ts,
  SUM(value) AS value
FROM selected
GROUP BY service, ts
ORDER BY service, ts;
```

`:metric` is text; `:filter_json` is a JSON matcher object or SQL `NULL`; and
`:start`, `:end`, `:step`, and `:lookback` use the metrics table's epoch-second
unit. Grid and lookback bounds retain the public `timeless_grid` contract.
SQLite groups a missing `service` and JSON null together here, so callers that
need PromQL's absent-as-empty grouping should normalize with
`COALESCE(json_extract(labels, '$.service'), '')`. Output is ordered by group
and timestamp; values remain SQLite numeric values.

This is the exact bounded numeric reduction foundation. The Rust API adds
`by`/`without` parsing, empty/missing and `__name__` output-label policy,
portable IEEE strings, millisecond evaluation timestamps, resource limits,
cancellation, and Prometheus result/error envelopes. The statement executes
in `tests/cli.sh` section 33; the real-extension/API contract is
`session_five_promql_sum_groups_labels_limits_and_reopen`.

### SQL-PROM-015: cross-series average by label

Equivalent ordinary-SQL reduction for `avg by (service) (metric)`:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object('service', service) AS labels,
  ts,
  AVG(value) AS value
FROM selected
GROUP BY service, ts
ORDER BY service, ts;
```

`:metric` is text, `:filter_json` is a matcher object or SQL `NULL`, and all
four temporal parameters use epoch seconds. The public grid has inclusive
output bounds and open-left lookback. Missing `service` is normalized to the
empty grouping value; rows are ordered by group and timestamp. SQLite returns
numeric `AVG` values using its own accumulation order.

The Rust API uses Prometheus 3.13.2's compensated direct mean and switches to
an incremental compensated mean when the running sum overflows. It also owns
`by`/`without`, output labels, millisecond timestamps, IEEE strings, limits,
cancellation, and envelopes. Therefore ordinary `AVG` is the copyable SQL
foundation but is not advertised as bit-identical for adversarial
cancellation/overflow inputs. This parameterized recipe executes in
`tests/cli.sh` section 45; the exact API contract is
`session_five_promql_avg_is_compensated_grouped_and_reopenable`.

### SQL-PROM-016: cross-series minimum and maximum

Use ordinary SQLite extrema over the public bounded grid:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object('service', service) AS labels,
  ts,
  MIN(value) AS min_value,
  MAX(value) AS max_value
FROM selected
GROUP BY service, ts
ORDER BY service, ts;
```

Parameter types, timestamp units, bounds, missing-label normalization, and
ordering are the same as `SQL-PROM-015`. SQLite ignores SQL NULL in `MIN` and
`MAX`; an IEEE NaN projected through an ordinary SQLite REAL is NULL. The Rust
API reads raw value bits from `TRF1`, ignores NaN when a numeric/infinite value
exists, and returns `NaN` for an all-NaN group exactly like Prometheus. It also
owns language, labels, limits, cancellation, and envelopes. The statement is
executed in `tests/cli.sh` section 45 and the exact API contract is
`session_five_promql_min_max_group_ieee_range_and_reopen`.

### SQL-PROM-017: cross-series count and group

Count every selected series at each evaluation timestamp and derive PromQL's
`group` presence value from the same public bounded grid:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object('service', service) AS labels,
  ts,
  COUNT(*) AS count_value,
  1 AS group_value
FROM selected
GROUP BY service, ts
ORDER BY service, ts;
```

`:metric` and `:filter_json` are text and a JSON matcher object (or SQL
`NULL`); `:start`, `:end`, `:step`, and `:lookback` are epoch seconds. The
public grid's output bounds are inclusive and its lookback is open-left.
Missing `service` is normalized to the empty grouping value. `COUNT(*)`
counts every selected sample, including values whose raw IEEE representation
is NaN or infinity; unlike `COUNT(value)`, it does not discard a NaN exposed
to SQLite as NULL. A non-empty group always yields the integer presence value
one, and an empty input yields no row.

The Rust API supplies `by`/`without`, `__name__` policy, Prometheus float
strings, millisecond timestamps, limits, cancellation, and response/error
envelopes. This parameterized statement executes in `tests/cli.sh` section
45; the exact API contract is
`session_five_promql_count_group_include_all_values_and_reopen`.

### SQL-PROM-018: cross-series population variance and standard deviation

This ordinary-SQL second-moment recipe is the copyable public-grid equivalent
for finite, well-scaled inputs:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
), moments AS (
  SELECT
    service,
    ts,
    AVG(value) AS mean,
    AVG(value * value) AS second_moment
  FROM selected
  GROUP BY service, ts
)
SELECT
  json_object('service', service) AS labels,
  ts,
  MAX(second_moment - mean * mean, 0.0) AS stdvar_value,
  SQRT(MAX(second_moment - mean * mean, 0.0)) AS stddev_value
FROM moments
ORDER BY service, ts;
```

Parameters, epoch-second units, inclusive grid bounds, open-left lookback,
missing-label normalization, and ordering match `SQL-PROM-017`. This computes
population variance (division by `N`), so a singleton is zero and empty input
has no row. `MAX(..., 0.0)` removes a small negative caused by ordinary
floating-point roundoff.

This formula can lose precision through cancellation or overflow while
squaring. SQLite also exposes stored NaN as SQL NULL. The Rust API therefore
uses Prometheus 3.13.2's one-pass Welford update over the raw packed float bits
and returns NaN when any group member is NaN or infinite. It also owns
`by`/`without`, labels, millisecond timestamps, limits, cancellation, and
envelopes. The SQL is an honest efficient foundation for normal finite data,
not a claim of bit-identical adversarial arithmetic. It executes in
`tests/cli.sh` section 45; the exact API contract is
`session_five_promql_stddev_stdvar_are_population_grouped_and_reopenable`.

### SQL-PROM-019: cross-series quantile

For a finite `:q` between zero and one and finite sample values, window
functions provide the same `q * (N - 1)` linear interpolation over each
service and evaluation timestamp:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
  WHERE value IS NOT NULL
), ranked AS (
  SELECT
    service,
    ts,
    value,
    ROW_NUMBER() OVER (
      PARTITION BY service, ts ORDER BY value
    ) - 1 AS value_index,
    COUNT(*) OVER (PARTITION BY service, ts) AS value_count
  FROM selected
), positions AS (
  SELECT DISTINCT
    service,
    ts,
    value_count,
    CAST(:q AS REAL) * (value_count - 1) AS rank
  FROM ranked
), bounds AS (
  SELECT
    *,
    CAST(rank AS INTEGER) AS lower_index,
    MIN(CAST(rank AS INTEGER) + 1, value_count - 1) AS upper_index
  FROM positions
)
SELECT
  json_object('service', bounds.service) AS labels,
  bounds.ts,
  lower.value * (1.0 - (bounds.rank - bounds.lower_index))
    + upper.value * (bounds.rank - bounds.lower_index) AS value
FROM bounds
JOIN ranked AS lower
  ON lower.service = bounds.service
 AND lower.ts = bounds.ts
 AND lower.value_index = bounds.lower_index
JOIN ranked AS upper
  ON upper.service = bounds.service
 AND upper.ts = bounds.ts
 AND upper.value_index = bounds.upper_index
ORDER BY bounds.service, bounds.ts;
```

`:metric`, `:filter_json`, and the epoch-second temporal parameters have the
same types and bounds as `SQL-PROM-018`; `:q` is REAL in `[0,1]`. Missing
`service` is normalized to empty, empty input emits no row, and ordering is
deterministic. A singleton returns its value.

The finite restriction is intentional. SQLite exposes a packed IEEE NaN as
SQL NULL, whereas PromQL sorts raw NaN before numeric samples and lets it
participate in the interpolation rank. PromQL also maps `q < 0` to `-Inf`,
`q > 1` to `+Inf`, and `q = NaN` to NaN. The Rust API operates on packed raw
bits and owns those edge rules, scalar parameter expressions, `by`/`without`,
labels, limits, cancellation, and envelopes. This executable SQL is the
public direct-user foundation for normal finite data, not a false IEEE-parity
claim. It runs in `tests/cli.sh` section 45; the exact API contract is
`session_five_promql_quantile_interpolates_per_step_and_reopens`.

### SQL-PROM-020: count series by sample value

Direct SQLite/libSQL users can group bounded grid rows by their numeric value
without materializing another storage representation:

```sql
WITH selected AS (
  SELECT
    ts,
    COALESCE(json_extract(labels, '$.service'), '') AS service,
    value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  json_object('service', service) AS grouping_labels,
  ts,
  value AS sample_value,
  COUNT(*) AS value_count
FROM selected
GROUP BY service, ts, value
ORDER BY service, ts, value;
```

`:metric`, `:filter_json`, and all epoch-second temporal parameters follow
`SQL-PROM-019`; output bounds are inclusive and lookback is open-left.
Missing `service` is normalized to empty. Output is one row per distinct SQL
numeric value per group and timestamp, with an integer count. Empty input
emits no row.

PromQL's `count_values("label", vector)` turns `sample_value` into a label
name/value pair. The Rust API implements Go-compatible fixed shortest float
text—including `-0`, `+Inf`, `-Inf`, NaN, and exponent expansion—using packed
raw float bits; it overwrites an existing label of the same name before
applying `by`/`without`. SQLite exposes raw NaN as NULL, so the SQL statement
is an honest numeric grouping foundation rather than an IEEE-label-formatting
claim. The API also owns UTF-8 label-name validation, per-step range assembly,
cardinality limits, cancellation, and envelopes. This recipe executes in
`tests/cli.sh` section 45; the exact API contract is
`session_five_promql_count_values_formats_groups_ranges_and_reopens`.

### SQL-PROM-004: vector arithmetic with label matching

For vector/scalar arithmetic, apply the ordinary SQLite operator to the public
grid (substitute the required arithmetic operator; `pow(value, :scalar)` is
the `^` form):

```sql
SELECT labels, ts, value * CAST(:scalar AS REAL) AS value
FROM timeless_grid(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :lookback
)
ORDER BY labels, ts;
```

For default one-to-one vector matching, canonical `labels` equality is exactly
PromQL's all-labels-except-`__name__` signature because public metric rows
carry the name separately. This parameterized statement shows every numeric
operation over `errors` and `requests`:

```sql
WITH
errors AS (
  SELECT ts, labels, value
  FROM timeless_grid(
    'metrics', 'errors_total', :error_filter,
    :start, :end, :step, :lookback
  )
),
requests AS (
  SELECT ts, labels, value
  FROM timeless_grid(
    'metrics', 'requests_total', :request_filter,
    :start, :end, :step, :lookback
  )
)
SELECT
  e.labels,
  e.ts,
  e.value + r.value AS add_value,
  e.value - r.value AS subtract_value,
  e.value * r.value AS multiply_value,
  e.value / r.value AS divide_value,
  e.value % r.value AS modulo_value,
  pow(e.value, r.value) AS power_value
FROM errors AS e
JOIN requests AS r
  ON r.ts = e.ts AND r.labels = e.labels
ORDER BY e.labels, e.ts;
```

All timestamps use the metric table's native unit; bounds are inclusive output
grid bounds, lookup windows are open on the left, unmatched rows are omitted,
and canonical label JSON plus timestamp ordering is deterministic. SQLite
ordinary arithmetic is the intended direct-user surface. The Rust API adds
parser precedence, scalar/vector result typing, per-timestamp one-to-one
cardinality validation, metric-name policy, millisecond timestamps, portable
IEEE strings, bounded cumulative work, cancellation, and Prometheus envelopes.
`on`, `ignoring`, and group matching remain separate matrix rows. The exact
six-operation public-grid join executes in `tests/cli.sh` section 33.

### SQL-PROM-005: top-k per evaluation step

For `topk(:k, metric)`, rank every bounded evaluation timestamp independently:

```sql
WITH selected AS (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY ts ORDER BY value DESC, labels
  ) AS rank
  FROM selected
)
SELECT labels, ts, value
FROM ranked
WHERE rank <= :k
ORDER BY ts, value DESC, labels;
```

`:metric` and `:filter_json` are text and a matcher object (or SQL `NULL`);
`:start`, `:end`, `:step`, and `:lookback` are epoch seconds; and `:k` is a
non-negative integer. Grid bounds are inclusive and lookback is open-left.
Each timestamp is ranked separately. Labels are the original canonical JSON,
not the grouping labels. The label tie-breaker makes direct SQL deterministic.
For `bottomk`, change `DESC` to `ASC` in both `ORDER BY` clauses.

To emulate `by (service)`, add
`COALESCE(json_extract(labels, '$.service'), '') AS service` to `selected` and
partition by `service, ts`; a `without` modifier requires projecting a
canonical JSON object with the excluded labels removed. The Rust API owns
that general label projection, scalar parameter expressions and per-step
integer truncation, NaN/overflow errors, Prometheus's rule that NaN ranks
after numeric values for both directions, within-group instant rank ordering
(group order is unspecified), sparse range
series assembly, limits, cancellation, and envelopes. This statement executes
in `tests/cli.sh` section 45; the exact API contract is
`session_five_promql_topk_bottomk_rank_per_step_and_reopen`.

## MetricsQL foundations and equivalents

### SQL-MQL-001: `default`, `if`, and `ifnot`

The public grid retains a row for every discovered series at every evaluation
step where lookback finds a value. A SQL `CASE` can preserve series identity
while a comparison filters every value, then `COALESCE` implements a scalar
`default` broadcast:

```sql
WITH lhs AS MATERIALIZED (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
), filtered AS (
  SELECT labels, ts,
         CASE WHEN value > :threshold THEN value END AS value
  FROM lhs
)
SELECT labels, ts, COALESCE(value, :default_value) AS value
FROM filtered
ORDER BY labels, ts;
```

For `if on(host)`, retain a left sample only when a right sample with the same
evaluation timestamp and projected label exists:

```sql
WITH lhs AS MATERIALIZED (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
), rhs AS MATERIALIZED (
  SELECT labels, ts
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT lhs.labels, lhs.ts, lhs.value
FROM lhs
WHERE EXISTS (
  SELECT 1
  FROM rhs
  WHERE rhs.ts = lhs.ts
    AND COALESCE(json_extract(rhs.labels, '$.host'), '')
        = COALESCE(json_extract(lhs.labels, '$.host'), '')
)
ORDER BY lhs.labels, lhs.ts;
```

`ifnot on(host)` is the corresponding step-local anti-membership operation:

```sql
WITH lhs AS MATERIALIZED (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :lhs_metric, :lhs_filter,
    :start, :end, :step, :lookback
  )
), rhs AS MATERIALIZED (
  SELECT labels, ts
  FROM timeless_grid(
    'metrics', :rhs_metric, :rhs_filter,
    :start, :end, :step, :lookback
  )
)
SELECT lhs.labels, lhs.ts, lhs.value
FROM lhs
WHERE NOT EXISTS (
  SELECT 1
  FROM rhs
  WHERE rhs.ts = lhs.ts
    AND COALESCE(json_extract(rhs.labels, '$.host'), '')
        = COALESCE(json_extract(lhs.labels, '$.host'), '')
)
ORDER BY lhs.labels, lhs.ts;
```

All times use the metrics table's native unit; `:start` and `:end` are
inclusive evaluation bounds, `:step` is positive, and lookback is open-left
and closed-right. `:lhs_filter` and `:rhs_filter` are public matcher JSON or
NULL. The explicit `COALESCE` makes missing labels compare as empty strings.
Replace the projected `$.host` path with every label named by `on(...)`; for
`ignoring(...)`, compare every retained label instead. SQL NULL represents a
missing step, not a stored float value.

These statements are exact mechanical foundations, not a claim that SQL
parses MetricsQL. The Rust API owns operator precedence, implicit scalar
vectors, scalar fallback for every left labelset, metric-name matching policy,
many-series collision behavior, all-NaN identity preservation, response
types, limits, cancellation, and errors. No extension primitive is warranted:
the public grid has already performed the bounded storage read and decode.

Executable regression: Rust SQL-equivalent harness `SQL-MQL-001`; pinned
VictoriaMetrics and real-extension HTTP/reopen regression:
`session_fifteen_metricsql_default_if_ifnot_match_victoriametrics_and_reopen`.

### SQL-MQL-002: `keep_metric_names`

The public grid accepts one explicit metric name at a time and returns its
labels separately. A direct SQLite/libSQL user preserves that identity simply
by carrying the bound name through the transform instead of discarding it:

```sql
WITH input(name, labels, ts, value) AS MATERIALIZED (
  SELECT :metric, labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT name, labels, ts, abs(value) AS value
FROM input
ORDER BY name, labels, ts;
```

`:metric` is the exact stored metric name. `:filter_json` is public matcher
JSON or NULL. All timestamps use the metrics table's native unit; `:start`
and `:end` are inclusive evaluation bounds, `:step` is positive, and
`:lookback` is open-left and closed-right. `name` and `labels` remain SQL TEXT,
`ts` remains INTEGER, and the transform result is REAL (or NULL under
SQLite's ordinary numeric rules). Ordering is deterministic by the complete
identity and timestamp. A missing input sample produces no grid row; this
statement never fabricates a metric name for a scalar or absent result.

For a default binary match, include `lhs.name = rhs.name` beside the desired
label comparisons. For `on(host)`, compare only the projected `host` values
and still select `lhs.name`; this is the pinned VictoriaMetrics exception in
which explicit `on(...)` controls matching while the modifier preserves the
left result name. `ignoring(...)` retains the metric name in the ordinary
comparison key unless it is explicitly projected away by API semantics.

This is an exact direct-SQL identity technique, not a claim that SQLite parses
the MetricsQL modifier. The Rust MetricsQL API owns which function and binary
nodes may carry it, nested AST composition, duplicate-labelset detection,
metric-name-aware default matching, response shaping, limits, cancellation,
and diagnostics. No extension primitive is warranted because the name is
already a public input/output identity and preserving it performs no
additional storage read or decode.

Executable regression: Rust SQL-equivalent harness `SQL-MQL-002`; pinned
VictoriaMetrics and real-extension HTTP/reopen regression:
`session_fifteen_metricsql_keep_metric_names_matches_victoriametrics_and_reopens`.

### SQL-MQL-003: `union` and `alias`

For exact metric inputs, ordinary `UNION ALL` composes bounded public grids.
Carry the metric name beside the public labels because MetricsQL labelset
identity includes `__name__`:

```sql
WITH candidates(branch, name, labels, ts, value) AS MATERIALIZED (
  SELECT 0, :first_metric, labels, ts, value
  FROM timeless_grid(
    'metrics', :first_metric, NULL,
    :start, :end, :step, :lookback
  )
  UNION ALL
  SELECT 1, :second_metric, labels, ts, value
  FROM timeless_grid(
    'metrics', :second_metric, NULL,
    :start, :end, :step, :lookback
  )
), winners(name, labels, branch) AS (
  SELECT name, labels, min(branch)
  FROM candidates
  GROUP BY name, labels
)
SELECT candidates.name, candidates.labels, candidates.ts, candidates.value
FROM candidates
JOIN winners USING (name, labels, branch)
ORDER BY candidates.name, candidates.labels, candidates.ts;
```

`alias(q, "name")` is direct metric-name projection. The stored metric name is
already supplied separately to the public grid, so no JSON rewrite or
extension function is required:

```sql
SELECT :alias_name AS name, labels, ts, value
FROM timeless_grid(
  'metrics', :alias_metric, NULL,
  :start, :end, :step, :lookback
)
ORDER BY name, labels, ts;
```

If aliasing makes two union branches identical, choose the lowest branch once
for the complete `(name, labels)` identity. This matches MetricsQL `union`'s
first-argument precedence; it does not merge disjoint timestamps or values:

```sql
WITH candidates(branch, name, labels, ts, value) AS MATERIALIZED (
  SELECT 0, :collision_alias, labels, ts, value
  FROM timeless_grid(
    'metrics', :collision_first_metric, NULL,
    :start, :end, :step, :lookback
  )
  UNION ALL
  SELECT 1, :collision_alias, labels, ts, value
  FROM timeless_grid(
    'metrics', :collision_second_metric, NULL,
    :start, :end, :step, :lookback
  )
), winners(name, labels, branch) AS (
  SELECT name, labels, min(branch)
  FROM candidates
  GROUP BY name, labels
)
SELECT candidates.name, candidates.labels, candidates.ts, candidates.value
FROM candidates
JOIN winners USING (name, labels, branch)
ORDER BY candidates.name, candidates.labels, candidates.ts;
```

All timestamps use the metrics table's native unit. `:start` and `:end` are
inclusive evaluation bounds, `:step` is positive, and `:lookback` is
open-left and closed-right. `name` and `labels` are TEXT, `ts` is INTEGER, and
`value` is REAL or NULL under the public grid contract. The recipes order by
the complete output identity and timestamp; MetricsQL does not promise that
HTTP union results preserve argument order.

An empty alias removes `__name__`; a direct SQL caller represents that by
omitting or discarding the projected `name` column. A bare `alias` over an
input that collapses multiple series to the same output identity is not the
same as a union: pinned VictoriaMetrics rejects it as duplicate output rather
than choosing one. SQL clients can detect that condition with `GROUP BY name,
labels HAVING count(*) > 1` over distinct source identities. The Rust API owns
that failure, zero/single/multiple union grammar, shorthand `(q1, q2)`,
trailing commas, scalar-to-vector behavior, nested AST composition, response
types, limits, cancellation, and diagnostics.

These statements use only public extension surfaces. `union` evaluates each
requested public grid once, so a new extension primitive would not eliminate
any storage read or decode. The Rust evaluator keeps only the first bounded
output for each complete labelset and charges every child result to the
existing intermediate-work limit.

Executable regression: Rust SQL-equivalent harness `SQL-MQL-003`; pinned
VictoriaMetrics and real-extension HTTP/reopen regression:
`session_fifteen_metricsql_union_alias_match_victoriametrics_and_reopen`.

### SQL-MQL-004: `label_set` and `label_del`

The public grid returns stored labels as JSON TEXT and the metric name as the
separately bound identity. One `label_set` pair is an ordinary JSON projection;
an empty value deletes the destination, including a missing destination as a
no-op. Treat `__name__` as the separate name column rather than inserting it
into the public labels object:

```sql
WITH input(name, labels, ts, value) AS MATERIALIZED (
  SELECT :metric, labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  CASE WHEN :label_name = '__name__'
       THEN NULLIF(:label_value, '')
       ELSE name
  END AS name,
  CASE WHEN :label_name = '__name__' THEN labels
       WHEN :label_value = '' THEN json_remove(labels, :label_path)
       ELSE json_set(labels, :label_path, :label_value)
  END AS labels,
  ts,
  value
FROM input
ORDER BY name, labels, ts;
```

`label_del` uses the same identity split:

```sql
WITH input(name, labels, ts, value) AS MATERIALIZED (
  SELECT :metric, labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
)
SELECT
  CASE WHEN :delete_label = '__name__' THEN NULL ELSE name END AS name,
  CASE WHEN :delete_label = '__name__' THEN labels
       ELSE json_remove(labels, :delete_path)
  END AS labels,
  ts,
  value
FROM input
ORDER BY name, labels, ts;
```

Bind `:label_name` or `:delete_label` as the decoded label name. For a
non-name label, bind `:label_path` or `:delete_path` as the corresponding valid
SQLite JSON path; for example, label `environment` uses `$.environment`.
`:label_value` is TEXT. Repeating the projection CTE once per argument pair,
in source order, implements multiple pairs and makes the last duplicate
destination win. Repeating `json_remove` implements multiple deletions;
missing paths remain no-ops. A direct caller must quote non-identifier label
names correctly when constructing a JSON path rather than concatenating
untrusted text into SQL.

All timestamps use the metrics table's native unit. `:start` and `:end` are
inclusive evaluation bounds, `:step` is positive, and `:lookback` is open-left
and closed-right. `name` is nullable TEXT after name deletion, `labels` is
JSON TEXT, `ts` is INTEGER, and `value` is REAL or NULL under the public-grid
contract. Ordering is deterministic by the complete output identity and
timestamp.

Pinned VictoriaMetrics rejects a bare transformation that collapses multiple
source identities to the same complete output labelset. Direct SQL users must
check the projected `(name, labels)` identities—retaining source identity in a
CTE and using `GROUP BY name, labels HAVING count(DISTINCT source_identity) >
1`—before returning the result. The Rust API owns that error, scalar-to-vector
conversion, case-insensitive function grammar, trailing commas,
`keep_metric_names`, nested expression composition, response types, limits,
cancellation, and diagnostics.

These statements use only public extension surfaces and perform no additional
storage read beyond their input grid. Label strings cloned across output
series are charged incrementally to the Rust response limit. No extension
primitive or storage-format change is warranted.

Executable regression: Rust SQL-equivalent harness `SQL-MQL-004`; pinned
VictoriaMetrics and real-extension HTTP/reopen regression:
`session_fifteen_metricsql_label_set_del_match_victoriametrics_and_reopen`.

### SQL-MQL-005: `default_rollup` and window-less rollups

For one exact metric, ordinary SQL can reproduce the finite-series storage
work behind automatic `default_rollup`. The following statement reads a
bounded public raw interval, estimates each series' scrape interval from the
interpolated 0.6 quantile of its last 20 positive intervals, applies the
VictoriaMetrics jitter inflation, caps the artificial window with
`:max_lookback` when it is positive, and selects the newest sample in the
open-left, closed-right window:

```sql
WITH RECURSIVE
evaluation(ts) AS (
  SELECT :start
  UNION ALL
  SELECT ts + :step FROM evaluation WHERE ts + :step <= :end
), raw AS MATERIALIZED (
  SELECT series_id, labels, ts, value
  FROM timeless_raw(
    'metrics', :metric, :filter_json,
    :start - :history - :step, :end
  )
), all_intervals AS (
  SELECT
    series_id,
    ts,
    ts - lag(ts) OVER (PARTITION BY series_id ORDER BY ts) AS sample_interval
  FROM raw
), recent_intervals AS (
  SELECT
    series_id,
    sample_interval,
    row_number() OVER (PARTITION BY series_id ORDER BY ts DESC) AS recency
  FROM all_intervals
  WHERE sample_interval > 0
), ordered_intervals AS (
  SELECT
    series_id,
    sample_interval,
    row_number() OVER (
      PARTITION BY series_id ORDER BY sample_interval
    ) - 1 AS interval_index,
    count(*) OVER (PARTITION BY series_id) AS interval_count
  FROM recent_intervals
  WHERE recency <= 20
), quantile_targets AS (
  SELECT
    series_id,
    interval_count,
    0.6 * (interval_count - 1) AS quantile_index
  FROM ordered_intervals
  GROUP BY series_id, interval_count
), scrape_quantiles AS (
  SELECT
    target.series_id,
    CAST(
      max(CASE
        WHEN interval_index = CAST(quantile_index AS INTEGER)
        THEN sample_interval
      END) * (1.0 - (quantile_index - CAST(quantile_index AS INTEGER)))
      + max(CASE
          WHEN interval_index = min(
            CAST(quantile_index AS INTEGER) + 1,
            interval_count - 1
          )
          THEN sample_interval
        END) * (quantile_index - CAST(quantile_index AS INTEGER))
      AS INTEGER
    ) AS scrape_interval
  FROM ordered_intervals AS ordered
  JOIN quantile_targets AS target USING (series_id, interval_count)
  GROUP BY target.series_id
), series AS (
  SELECT DISTINCT series_id FROM raw
), scrape_intervals AS (
  SELECT
    series.series_id,
    CASE
      WHEN :start = :end THEN :step
      ELSE coalesce(nullif(scrape_quantiles.scrape_interval, 0), :step)
    END AS scrape_interval
  FROM series
  LEFT JOIN scrape_quantiles USING (series_id)
), inflated AS (
  SELECT
    series_id,
    CASE
      WHEN scrape_interval <= 2 THEN scrape_interval * 5
      WHEN scrape_interval <= 4 THEN scrape_interval * 3
      WHEN scrape_interval <= 8 THEN scrape_interval * 2
      WHEN scrape_interval <= 16 THEN scrape_interval + scrape_interval / 2
      WHEN scrape_interval <= 32 THEN scrape_interval + scrape_interval / 4
      ELSE scrape_interval + scrape_interval / 8
    END AS max_previous_interval
  FROM scrape_intervals
), windows AS (
  SELECT
    series_id,
    CASE
      WHEN :max_lookback > 0 THEN min(
        max(:step, min(max_previous_interval, :max_lookback)),
        :max_lookback
      )
      ELSE max(:step, max_previous_interval)
    END AS window
  FROM inflated
), candidates AS (
  SELECT
    raw.series_id,
    raw.labels,
    evaluation.ts,
    raw.value,
    row_number() OVER (
      PARTITION BY raw.series_id, evaluation.ts ORDER BY raw.ts DESC
    ) AS recency
  FROM evaluation
  JOIN raw ON raw.ts <= evaluation.ts
  JOIN windows USING (series_id)
  WHERE raw.ts > evaluation.ts - windows.window
)
SELECT :metric AS name, labels, ts, value
FROM candidates
WHERE recency = 1
ORDER BY name, labels, ts;
```

`:history` is the maximum bounded interval from which scrape cadence may be
inferred; bind `300` for the release API's five-minute silence history.
`:max_lookback = 0` means no request cap. The recipe assumes the default
seconds table, so the jitter thresholds `2`, `4`, `8`, `16`, and `32` are
seconds; scale every time and threshold together for another declared metric
timestamp unit. `:start` and `:end` are inclusive evaluation bounds and
`:step` is positive. Canonical labels and the projected metric name are TEXT,
timestamps are INTEGER, and deterministic ordering uses complete identity and
time.

Ordinary window-less statistical rollups use the request step as their exact
window. The public window TVF performs the bounded decode and reduction:

```sql
SELECT :metric AS name, labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :step, :step,
  :aggregate
)
ORDER BY name, labels, ts;
```

Bind `:aggregate` to `avg`, `min`, `max`, `sum`, or `count`. Direct users keep
the projected name for `avg`, `min`, and `max`, and drop it for `sum` and
`count`; `last` is the equivalent public-grid operation and retains the name.
`present_over_time` maps any positive `count` result to `1`. The existing
`SQL-PROM-028`, `SQL-PROM-030`, and `SQL-PROM-034` recipes show the ordinary
SQL foundations for population deviation/variance, final-pair differences,
and regression. Every window is `(T-step,T]`; empty windows emit no row.

These are executable storage foundations, not a claim that SQLite parses
MetricsQL. Row-projected SQLite cannot distinguish the exact Prometheus stale
NaN bit pattern from an ordinary NaN normalized to SQL NULL. Window-less
`rate`, `irate`, `increase`, `delta`, `idelta`, `changes`, and `resets` also
need the bounded previous sample, VictoriaMetrics reset correction and
precision threshold, and `max_lookback` policy. The Rust MetricsQL API reads
the public packed raw frame once to preserve those distinctions and owns
implicit syntax, scalar/subquery composition, metric-name policy, duplicate
outputs, cumulative limits, cancellation, HTTP result types, and errors. No
new extension primitive is justified: all storage rows and reductions already
come from public raw/window surfaces, and the remaining work is language and
cross-window composition.

Executable regression: Rust SQL-equivalent harness `SQL-MQL-005`; pinned
VictoriaMetrics and real-extension HTTP/reopen regression:
`session_fifteen_metricsql_default_and_windowless_rollups_match_victoriametrics_and_reopen`.

### SQL-MQL-006: range aggregates

`range_avg`, `range_min`, `range_max`, and `range_sum` reduce each complete
input evaluation grid to one value and repeat that value at every requested
timestamp. The following ordinary recursive SQL applies those mechanics to
one exact metric selected through the public `timeless_grid` TVF:

```sql
WITH RECURSIVE
evaluation(slot, ts) AS (
  SELECT 0, :start
  UNION ALL
  SELECT slot + 1, ts + :step
  FROM evaluation
  WHERE ts + :step <= :end
), selected AS MATERIALIZED (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
), identities AS (
  SELECT DISTINCT labels FROM selected
), input_grid AS (
  SELECT identities.labels, evaluation.slot, evaluation.ts, selected.value
  FROM identities
  CROSS JOIN evaluation
  LEFT JOIN selected
    ON selected.labels = identities.labels
   AND selected.ts = evaluation.ts
), first_slots AS (
  SELECT labels, min(slot) AS first_slot
  FROM input_grid
  WHERE value IS NOT NULL
  GROUP BY labels
), running(labels, slot, value) AS (
  SELECT input_grid.labels, input_grid.slot, input_grid.value
  FROM input_grid
  JOIN first_slots
    ON first_slots.labels = input_grid.labels
   AND first_slots.first_slot = input_grid.slot
  UNION ALL
  SELECT
    input_grid.labels,
    input_grid.slot,
    CASE
      WHEN input_grid.value IS NULL THEN running.value
      WHEN :aggregate = 'avg' THEN
        running.value + (input_grid.value - running.value)
          / (input_grid.slot - first_slots.first_slot + 1.0)
      WHEN :aggregate = 'min' THEN
        CASE WHEN running.value < input_grid.value
          THEN running.value ELSE input_grid.value END
      WHEN :aggregate = 'max' THEN
        CASE WHEN running.value > input_grid.value
          THEN running.value ELSE input_grid.value END
      WHEN :aggregate = 'sum' THEN running.value + input_grid.value
    END
  FROM running
  JOIN input_grid
    ON input_grid.labels = running.labels
   AND input_grid.slot = running.slot + 1
  JOIN first_slots USING (labels)
), final_values AS (
  SELECT
    first_slots.labels,
    (
      SELECT candidate.value
      FROM running AS candidate
      WHERE candidate.labels = first_slots.labels
        AND candidate.value IS NOT NULL
      ORDER BY candidate.slot DESC
      LIMIT 1
    ) AS value
  FROM first_slots
)
SELECT NULL AS name, final_values.labels, evaluation.ts, final_values.value
FROM final_values
CROSS JOIN evaluation
WHERE :aggregate IN ('avg', 'min', 'max', 'sum')
  AND final_values.value IS NOT NULL
ORDER BY final_values.labels, evaluation.ts;
```

Bind `:aggregate` to `avg`, `min`, `max`, or `sum`. `:start` and `:end` are
inclusive timestamps in the metric table's declared unit, `:step` is a
positive interval in that unit, and `:lookback` is the public selector
lookback. Bind `:filter_json` to canonical label equality JSON or NULL. The
output name is deliberately SQL NULL because pinned VictoriaMetrics removes
the metric group for every `range_*` function—even when the expression has a
trailing `keep_metric_names` modifier. Labels are canonical JSON TEXT,
timestamps are INTEGER, values are REAL, and the final order is complete
label identity followed by timestamp.

The recursive state skips leading SQL NULL values, counts every missing grid
slot in `range_avg`'s denominator after the first value, carries the running
value through later gaps, uses ordinary binary64 addition, chooses the later
operand for equal minima/maxima, selects the last non-NULL running value, and
fills that value across leading and trailing output timestamps. This is the
exact row-visible transformation once `selected` represents the input
instant-vector grid. SQLite projects packed NaN as SQL NULL; that is compatible
with these four functions' treatment of NaN as missing, but direct SQL does
not expose the original NaN payload bits. The API additionally owns arbitrary
inner expressions and scalars, automatic per-series selector windows,
post-name-drop duplicate detection, `keep_metric_names` grammar, result/work/
response limits, cancellation, float string formatting, and instant/range
HTTP envelopes. A direct query spanning multiple metric names must retain the
name while forming identities and explicitly reject collisions after setting
the output name to NULL.

The statement materializes only the bounded public input grid. It never reads
a shadow table and adds no extension primitive; range aggregation performs no
additional storage selection or decode beyond its child expression.

Executable regression: Rust SQL-equivalent harness `SQL-MQL-006`; pinned
VictoriaMetrics and real-extension HTTP/reopen regression:
`session_fifteen_metricsql_range_aggregates_match_victoriametrics_and_reopen`.

### SQL-MQL-007: running aggregates

`running_avg`, `running_min`, `running_max`, and `running_sum` emit the
cumulative reduction at each timestamp instead of repeating only the final
value. The following recursive statement applies the row-visible mechanics to
one exact metric through the public `timeless_grid` TVF:

```sql
WITH RECURSIVE
evaluation(slot, ts) AS (
  SELECT 0, :start
  UNION ALL
  SELECT slot + 1, ts + :step
  FROM evaluation
  WHERE ts + :step <= :end
), selected AS MATERIALIZED (
  SELECT labels, ts, value
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
), identities AS (
  SELECT DISTINCT labels FROM selected
), input_grid AS (
  SELECT identities.labels, evaluation.slot, evaluation.ts, selected.value
  FROM identities
  CROSS JOIN evaluation
  LEFT JOIN selected
    ON selected.labels = identities.labels
   AND selected.ts = evaluation.ts
), first_slots AS (
  SELECT labels, min(slot) AS first_slot
  FROM input_grid
  WHERE value IS NOT NULL
  GROUP BY labels
), running(labels, slot, value) AS (
  SELECT input_grid.labels, input_grid.slot, input_grid.value
  FROM input_grid
  JOIN first_slots
    ON first_slots.labels = input_grid.labels
   AND first_slots.first_slot = input_grid.slot
  UNION ALL
  SELECT
    input_grid.labels,
    input_grid.slot,
    CASE
      WHEN input_grid.value IS NULL THEN running.value
      WHEN :aggregate = 'avg' THEN
        running.value + (input_grid.value - running.value)
          / (input_grid.slot - first_slots.first_slot + 1.0)
      WHEN :aggregate = 'min' THEN
        CASE WHEN running.value < input_grid.value
          THEN running.value ELSE input_grid.value END
      WHEN :aggregate = 'max' THEN
        CASE WHEN running.value > input_grid.value
          THEN running.value ELSE input_grid.value END
      WHEN :aggregate = 'sum' THEN running.value + input_grid.value
    END
  FROM running
  JOIN input_grid
    ON input_grid.labels = running.labels
   AND input_grid.slot = running.slot + 1
  JOIN first_slots USING (labels)
)
SELECT NULL AS name, running.labels, input_grid.ts, running.value
FROM running
JOIN input_grid
  ON input_grid.labels = running.labels
 AND input_grid.slot = running.slot
WHERE :aggregate IN ('avg', 'min', 'max', 'sum')
  AND running.value IS NOT NULL
ORDER BY running.labels, input_grid.ts;
```

Bind the same parameters and units as `SQL-MQL-006`: inclusive `:start` and
`:end`, positive `:step`, selector `:lookback`, exact `:metric`, canonical
label-equality `:filter_json` or NULL, and one of `avg`, `min`, `max`, or `sum`
for `:aggregate`. Output names are SQL NULL because every running function
removes the metric name even under `keep_metric_names`; labels are canonical
JSON TEXT, timestamps INTEGER, and values REAL ordered by label identity and
timestamp.

Leading SQL NULL slots emit no row. After the first value, a missing slot
carries the prior running value and still advances the average's slot-index
denominator. Average uses VictoriaMetrics's incremental update, sum uses
ordinary binary64 addition, and equal extrema choose the later operand. A
computed SQL NULL/NaN row is omitted. SQLite row projection cannot retain an
original packed NaN payload, while the Rust API treats stored ordinary/stale
NaNs through the documented packed contract before applying these same
running rules. The API also owns arbitrary scalar/vector expressions,
implicit per-series windows, post-name-drop duplicate detection, limits,
cancellation, float rendering, and HTTP envelopes.

The statement materializes one bounded public input grid and performs no
second storage query or private-table access. Its mechanics differ from
`SQL-MQL-006` only at output: each non-NULL recursive state is emitted instead
of selecting the final state and filling the complete grid.

Executable regression: Rust SQL-equivalent harness `SQL-MQL-007`; pinned
VictoriaMetrics and real-extension HTTP/reopen regression:
`session_fifteen_metricsql_running_aggregates_match_victoriametrics_and_reopen`.

### SQL-MQL-009: request-step-relative durations

MetricsQL resolves each `Ni` component to `N * request_step` before executing
the expression. Direct SQLite/libSQL users can perform the same arithmetic in
the arguments to an existing public window. For
`count_over_time(metric[:multiple i])` on the default seconds table:

```sql
SELECT NULL AS name, labels, ts, value
FROM timeless_window(
  'metrics', :metric, :filter_json,
  :start, :end, :request_step,
  CASE
    WHEN CAST(:multiple * :request_step AS INTEGER) = 0
      THEN :request_step
    ELSE CAST(:multiple * :request_step AS INTEGER)
  END,
  'count'
)
ORDER BY labels, ts;
```

`:request_step` is a positive INTEGER in the table's declared timestamp unit
and `:multiple` is a non-negative INTEGER or REAL. Multiplication uses SQLite
REAL arithmetic and `CAST(... AS INTEGER)` truncates toward zero, matching the
millisecond result of VictoriaMetrics duration composition for ordinary
finite values. A resolved zero window becomes one request step, matching the
pinned ordinary-reduction `0i` rule. The public window is open on the left and closed on
the right, output-grid bounds are inclusive, empty windows emit no row, the
metric name is SQL NULL for `count_over_time`, and rows are deterministic by
canonical labels then timestamp. Substitute any documented public aggregate
for `count` and apply its documented metric-name policy. Adaptive zero-window
`rate`, `irate`, `deriv`, and `default_rollup`—including their direct and
subquery forms—use the cadence-inference recipe in `SQL-MQL-005`; the Rust API
selects that plan rather than pretending a fixed SQL window is equivalent.

For a selector offset such as `metric offset :multiple i`, shift lookup times
by the resolved signed duration and restore each outer timestamp:

```sql
WITH timing(offset) AS (
  SELECT CAST(:multiple * :request_step + :fixed_offset AS INTEGER)
)
SELECT :metric AS name, labels, ts + timing.offset AS ts, value
FROM timing,
     timeless_grid(
       'metrics', :metric, :filter_json,
       :start - timing.offset, :end - timing.offset,
       :request_step, :lookback
     )
ORDER BY name, labels, ts;
```

Bind a negative `:multiple` for negative offset and bind any ordinary duration
components already converted to the table unit as signed `:fixed_offset`.
Selection uses the shifted open-left lookback; output timestamps remain on the
original inclusive request grid. Missing selections remain sparse. For
`[Ni:Mi]` subqueries, bind `:window = N * :request_step` and
`:resolution = M * :request_step` in `SQL-PROM-009`; a resolved zero window or
resolution becomes `:request_step`. The default metrics table stores integer
seconds. To reproduce a millisecond request directly, construct the recursive
outer grid in milliseconds, multiply public raw `ts` values by 1,000, and use
the exact predicates from `SQL-PROM-006`; the release Rust API owns that
bounded millisecond composition and preserves its result timestamps.

These statements expose the direct-user mechanics but do not make SQLite a
MetricsQL parser. The Rust API owns decimal/compound/case-insensitive `i`
grammar, comment/string isolation, VictoriaMetrics float accumulation and
`int64` saturation, collision-free `0i` lowering, stable-PromQL isolation,
subquery AST composition, work/result/response limits, cancellation, and HTTP
errors. No private table or new extension primitive is involved.

Executable regression: Rust SQL-equivalent harness `SQL-MQL-009`; pinned
VictoriaMetrics and real-extension HTTP/reopen regression:
`session_fifteen_metricsql_step_relative_durations_match_victoriametrics_and_reopen`.

### SQL-MQL-010: query-context values

MetricsQL exposes the current request's start, end, and positive step as
floating-point Unix seconds. Direct SQLite/libSQL users can bind the same
request context and project it over an inclusive millisecond evaluation grid:

```sql
WITH RECURSIVE
request(start_ms, end_ms, step_ms) AS (
  VALUES (
    CAST(:start_ms AS INTEGER),
    CAST(:end_ms AS INTEGER),
    CAST(:step_ms AS INTEGER)
  )
),
evaluation(ts_ms) AS (
  SELECT start_ms
  FROM request
  WHERE start_ms <= end_ms AND step_ms > 0
  UNION ALL
  SELECT evaluation.ts_ms + request.step_ms
  FROM evaluation, request
  WHERE evaluation.ts_ms + request.step_ms <= request.end_ms
)
SELECT evaluation.ts_ms,
       request.start_ms / 1000.0 AS start_seconds,
       request.end_ms / 1000.0 AS end_seconds,
       request.step_ms / 1000.0 AS step_seconds
FROM evaluation, request
ORDER BY evaluation.ts_ms;
```

Bind `:start_ms`, `:end_ms`, and `:step_ms` in the Rust API's native
millisecond request unit. For an instant query, bind start and end to the same
evaluation timestamp; `step_ms` is still the positive request step. Negative
pre-epoch bounds and subsecond steps retain their exact integer-millisecond
inputs before conversion to binary64 seconds. A range with start after end or
a non-positive step emits no SQL rows; the Rust API rejects those request
parameters before planning.

The values are request metadata, so this statement intentionally performs no
storage read. Compose its bound values with any public metrics recipe when a
selector is also required; that selector retains its documented bounds,
ordering, missing-value, and metric-name behavior. The Rust API owns the
case-insensitive zero-argument `start()`/`end()`/`step()` grammar, scalar and
vector response types, expression composition, result/work/response limits,
cancellation, and HTTP errors. It rejects `start_timestamp()` and `range()` as
unsupported MetricsQL functions. Stable PromQL remains independently
feature-gated, and direct selector modifiers `@ start()` and `@ end()` retain
their established PromQL semantics. No extension syntax, private table, or
new storage primitive is involved.

Executable regression: Rust SQL-equivalent harness `SQL-MQL-010`; pinned
VictoriaMetrics and real-extension HTTP/reopen regression:
`session_fifteen_metricsql_query_context_matches_victoriametrics_and_reopens`.

### SQL-MQL-012: `histogram_quantiles`

For cumulative classic buckets with valid numeric `le` labels, one bounded
public grid can calculate multiple VictoriaMetrics-style quantiles without
rereading storage. This example binds two ranks and their already formatted
destination-label values:

```sql
WITH
quantiles(phi_label, quantile) AS (
  VALUES
    (CAST(:first_phi_label AS TEXT), CAST(:first_quantile AS REAL)),
    (CAST(:second_phi_label AS TEXT), CAST(:second_quantile AS REAL))
),
selected AS (
  SELECT json_remove(labels, '$.le') AS labels, ts,
         json_extract(labels, '$.le') AS le, value AS count
  FROM timeless_grid(
    'metrics', :metric, :filter_json,
    :start, :end, :step, :lookback
  )
),
parsed AS (
  SELECT labels, ts,
         CASE WHEN le IN ('+Inf','Inf') THEN 1 ELSE 0 END AS is_inf,
         CASE
           WHEN le IN ('+Inf','Inf') THEN NULL
           WHEN le = '-Inf' THEN -1e999
           ELSE CAST(le AS REAL)
         END AS upper_bound,
         count
  FROM selected
  WHERE le IS NOT NULL
),
coalesced AS (
  SELECT labels, ts, is_inf, upper_bound, SUM(count) AS count
  FROM parsed
  GROUP BY labels, ts, is_inf, upper_bound
),
monotonic AS (
  SELECT labels, ts, is_inf, upper_bound,
         MAX(COALESCE(count, 0.0)) OVER (
           PARTITION BY labels, ts
           ORDER BY is_inf, upper_bound
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS count
  FROM coalesced
),
positioned AS (
  SELECT *,
         LAG(upper_bound) OVER (
           PARTITION BY labels, ts ORDER BY is_inf, upper_bound
         ) AS lower_bound,
         LAG(count) OVER (
           PARTITION BY labels, ts ORDER BY is_inf, upper_bound
         ) AS lower_count
  FROM monotonic
),
stats AS (
  SELECT labels, ts, MAX(count) AS total,
         MAX(CASE WHEN is_inf = 0 THEN upper_bound END) AS last_finite
  FROM positioned
  GROUP BY labels, ts
),
chosen AS (
  SELECT p.*, q.phi_label, q.quantile,
         ROW_NUMBER() OVER (
           PARTITION BY p.labels, p.ts, q.phi_label
           ORDER BY p.upper_bound
         ) AS choice
  FROM positioned AS p
  JOIN stats AS s USING (labels, ts)
  CROSS JOIN quantiles AS q
  WHERE p.is_inf = 0 AND p.count > 0
    AND p.count >= q.quantile * s.total
),
evaluated AS (
  SELECT json_set(s.labels, :destination_path, q.phi_label) AS labels,
         s.ts,
         CASE
           WHEN s.total = 0 THEN NULL
           WHEN q.quantile < 0 THEN -1e999
           WHEN q.quantile > 1 THEN 1e999
           WHEN c.choice IS NULL THEN s.last_finite
           WHEN c.count = COALESCE(c.lower_count, 0)
             THEN COALESCE(c.lower_bound, 0)
           ELSE COALESCE(c.lower_bound, 0)
                + (c.upper_bound - COALESCE(c.lower_bound, 0))
                * ((q.quantile * s.total - COALESCE(c.lower_count, 0))
                   / (c.count - COALESCE(c.lower_count, 0)))
         END AS value
  FROM stats AS s
  CROSS JOIN quantiles AS q
  LEFT JOIN chosen AS c
    ON c.labels = s.labels AND c.ts = s.ts
   AND c.phi_label = q.phi_label AND c.choice = 1
)
SELECT labels, ts, value
FROM evaluated
WHERE value IS NOT NULL
ORDER BY labels, ts;
```

`:metric`, `:filter_json`, and the inclusive epoch-second grid parameters use
the same public contracts as `SQL-PROM-054`. Bind unique ranks in
`:first_quantile` and `:second_quantile`, their VictoriaMetrics `%g` spellings
in `:first_phi_label` and `:second_phi_label`, and a SQLite JSON path such as
`$.phi` in `:destination_path`. Add more `VALUES` rows for more ranks. The
statement removes `le`, replaces the destination label, merges numerically
equal bounds by sum, repairs a leading NULL count to zero and later decreases
with a running maximum, treats the last bucket count as the total even without
`+Inf`, interpolates from zero even when the first finite bound is negative,
returns the final finite bound for a rank landing in an infinite bucket, and
omits NULL results. Rows are deterministic by canonical output labels then
timestamp.

The finite row-visible restriction is deliberate. SQLite projects a stored
NaN count as NULL and cannot distinguish its payload, while the Rust API reads
the public packed frame and preserves Timeless's exact NaN bits before applying
VictoriaMetrics repair. The API also converts native `vmrange="low...high"`
buckets to cumulative `le` buckets, evaluates scalar rank ASTs per step,
formats their first values exactly (`1e+06`, `1e-05`, `NaN`, `+Inf`), mutates
empty or `__name__` destinations, omits computed NaN samples, rejects duplicate
output labelsets, and owns MetricsQL grammar, implicit rollups, work/result/
response limits, cancellation, and HTTP envelopes. Those language and packed
IEEE behaviors are not mislabeled as ordinary SQL. The bucket expression is
executed once for every rank, so a new extension primitive would not reduce a
public storage read or row crossing.

Executable regression: Rust SQL-equivalent harness `SQL-MQL-012`; pinned
VictoriaMetrics and real-extension HTTP/reopen regression:
`session_fifteen_metricsql_histogram_quantiles_match_victoriametrics_and_reopen`.

## LogsQL foundations and equivalents

### SQL-LOG-001: bounded filter, sort, and pagination

Use the virtual table's declared index-key columns for posting-list pruning:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND level = :level
  AND service = :service
  AND max_work_entries = :max_work_entries
ORDER BY ts DESC
LIMIT :limit OFFSET :offset;
```

For `_time:5m`, bind `:start_ms = :now_ms - 300000` and
`:end_ms = :now_ms`. Passing `:now_ms` makes evaluation deterministic. Exact
`ORDER BY ts ASC|DESC LIMIT/OFFSET` is consumed by the virtual table when the
remaining predicates allow it. Confirm with:

```sql
EXPLAIN QUERY PLAN
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms AND ts <= :end_ms
  AND level = :level AND service = :service
  AND max_work_entries = :max_work_entries
ORDER BY ts DESC
LIMIT :limit OFFSET :offset;
```

The `_ms` suffix in this legacy-default recipe names the unit selected when
the table was created; the release logs server creates `timestamp_unit='us'`
and binds epoch microseconds instead. Relative and closed bounds use `>=` and
`<=`. For `_time:[start,end)` in a microsecond table, bind
`:start_us = start` and `:end_us = end - 1`; `(start,end]` binds
`start + 1` and `end`. The Rust planner rejects overflow and an empty
intersection rather than widening it.

`service` is fast only when declared in `index_keys`. Other declared keys use
the same shape. `:max_work_entries` is a positive hard cap on entries examined,
independent of result cardinality. It is optional for backward-compatible
direct SQL, but the Rust server always binds it; a small `LIMIT` therefore
cannot conceal an unbounded live-buffer filter or block decode. Zero, negative,
NULL, and non-integer equality inputs fail explicitly. Omit the predicate for
the compatible unbounded form; because an unbound hidden input projects SQL
NULL, `max_work_entries IS NULL` selects that form rather than supplying a
guard.

### SQL-LOG-002: message substring

Use the hidden exact engine predicate to filter before rows cross SQLite:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND message_contains = :needle
  AND max_work_entries = :max_work_entries
ORDER BY ts DESC
LIMIT :limit OFFSET :offset;
```

This is Timeless's case-insensitive literal substring operation. It is not by
itself VictoriaLogs word, phrase, prefix, or regexp semantics. A shipped
VictoriaLogs phrase preserves exact case and bytes, then enforces a word
boundary when its first or last character is a Unicode letter, digit, or
underscore. Portable SQLite has no complete Unicode-category predicate for
that boundary, so `LQL-F08` honestly remains bounded `ROWS`/Rust API
composition and does not claim this SQL recipe. The matrix keeps the two
operations separate.

### SQL-LOG-003: exact count

Return one scalar without materializing matching log rows:

```sql
SELECT n
FROM timeless_log_count(
  'logs',
  :filter_json,
  :message_contains,
  :start_ms,
  :end_ms,
  :max_work_entries
);
```

Example `:filter_json`:

```json
{"level":"error","service":"api","status":"500"}
```

Only the table name is required; other arguments may be omitted. A positive
`:max_work_entries` bounds entries decoded for boundary or content predicates;
metadata-only contributions do not consume row-decode work. Fully covered
blocks can answer from metadata, while boundary and content predicates decode
as needed. A supplied NULL, zero, negative, or non-integer guard fails
explicitly; omitting the final
argument retains the compatible unbounded arity. Direct regression: the Rust SQL-equivalent harness and
`tests/cli.sh` section 43.

### SQL-LOG-004: distinct field values

Return bounded distinct values in lexical order:

```sql
SELECT value
FROM timeless_log_values(
  'logs',
  :field,
  :filter_json,
  :message_contains,
  :start_ms,
  :end_ms,
  :max_values,
  :max_work_entries
)
ORDER BY value;
```

The value-result default is 1,000 and its hard cap is 100,000.
`:max_work_entries` separately caps entries examined while discovering those
values. A supplied NULL, zero, negative, or non-integer guard fails explicitly;
omitting the final argument
retains the compatible unbounded arity. This is the direct SQL foundation for
field discovery and distinct-value query work.

### SQL-LOG-005: arbitrary metadata equality

When a key was not declared in `index_keys`, filter the returned typed JSON
with SQLite JSON functions:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND max_work_entries = :max_work_entries
  AND json_type(metadata, '$.deployment.region') = 'text'
  AND json_extract(metadata, '$.deployment.region') = :region
ORDER BY ts DESC
LIMIT :limit OFFSET :offset;
```

This is correct but requires decoding candidate rows. If measurements show a
stable, selective key is common, create a new table with that key declared in
`index_keys`; do not read or mutate shadow tables.

`json_extract` alone is insufficient for exact typed filters because SQLite
returns SQL `NULL` for both a missing path and a stored JSON null. Pair it with
`json_type`:

| stored value | exact SQL predicate |
|---|---|
| missing | `json_type(metadata, '$.nested.value') IS NULL` |
| JSON null | `json_type(metadata, '$.nested.value') = 'null'` |
| empty string | `json_type(...) = 'text' AND json_extract(...) = ''` |
| boolean true | `json_type(...) = 'true' AND json_extract(...) = 1` |
| number 2 | `json_type(...) IN ('integer','real') AND json_extract(...) = 2` |
| string `"2"` | `json_type(...) = 'text' AND json_extract(...) = '2'` |

The Rust P0 compatibility grammar keeps legacy `field:value` as exact string
equality. Its explicit `field:=value` form accepts JSON primitives, and dotted
field names select nested object leaves. Arrays/objects as whole comparison
values and VictoriaLogs word/phrase exactness remain separate matrix rows.

### SQL-LOG-006: counts by field and time bucket

For a declared indexed field, use the storage-aware bucket vector:

```sql
SELECT bucket_ts, group_key, n
FROM timeless_log_buckets(
  'logs', :group_key, :filter_json,
  :start_ms, :end_ms, :step_ms
)
ORDER BY bucket_ts, group_key;
```

The buckets are forward `[T,T+step)` intervals aligned to `:start_ms`; this is
not a PromQL-style trailing window. For arbitrary decoded numeric metadata,
ordinary SQL remains available:

```sql
SELECT
  ((ts - :start_ms) / :step_ms) * :step_ms + :start_ms AS bucket_ts,
  COUNT(*) AS n,
  AVG(CAST(json_extract(metadata, '$.duration_ms') AS REAL)) AS avg_duration
FROM logs
WHERE ts >= :start_ms AND ts <= :end_ms
GROUP BY bucket_ts
ORDER BY bucket_ts;
```

The second form is deliberately decode-heavy. Measurements determine whether
a future typed storage-aware aggregate earns a new `EXT` row.

### SQL-LOG-007: case-sensitive message substring

Use SQLite's binary `instr()` over bounded public rows for LogsQL `*text*`
semantics:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND max_work_entries = :max_work_entries
  AND instr(message, :needle) > 0
ORDER BY ts DESC
LIMIT :limit;
```

Bind timestamps in the table's declared unit (`us` for the release server;
the cookbook fixture uses the legacy `ms` default). `instr()` is
case-sensitive and searches literal bytes, so it matches VictoriaLogs
substring behavior for retained UTF-8 text. It is intentionally different
from the extension's storage-aware `message_contains` hidden input in
`SQL-LOG-002`, which is the established case-insensitive Timeless operation.
The SQL form decodes candidates before applying `instr`; the Rust API owns the
LogsQL parser, cancellation between decoded rows, and HTTP limits/envelopes.

### SQL-LOG-008: exact, empty, and any-value predicates

Full-message exact matching is ordinary equality:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND max_work_entries = :max_work_entries
  AND message = :exact_message
ORDER BY ts DESC
LIMIT :limit;
```

For a dynamic nested metadata path, an empty-value predicate explicitly
includes missing, JSON null, and the stored empty string:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND max_work_entries = :max_work_entries
  AND (
    json_type(metadata, :empty_path) IS NULL
    OR json_type(metadata, :empty_path) = 'null'
    OR (
      json_type(metadata, :empty_path) = 'text'
      AND json_extract(metadata, :empty_path) = ''
    )
  )
ORDER BY ts DESC
LIMIT :limit;
```

The complementary any-value predicate requires a present, non-null value and
excludes only the stored empty string. Typed zero, false, arrays, and objects
remain values:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND max_work_entries = :max_work_entries
  AND json_type(metadata, :any_path) IS NOT NULL
  AND json_type(metadata, :any_path) <> 'null'
  AND NOT (
    json_type(metadata, :any_path) = 'text'
    AND json_extract(metadata, :any_path) = ''
  )
ORDER BY ts DESC
LIMIT :limit;
```

For exact typed metadata equality, use the `json_type` plus `json_extract`
table in `SQL-LOG-005`. The Rust compatibility grammar deliberately keeps
legacy `field:""` as exact empty string and provides `field:("")` for the
VictoriaLogs-compatible empty predicate. `field:=null` and `field:=""` remain
exact retained-type predicates, so applications can distinguish every state.

### SQL-LOG-009: boolean composition

Direct SQLite/libSQL users compose SQL-expressible atoms with ordinary
`AND`, `OR`, `NOT`, and parentheses. This example pushes the indexed service
conjunct into the virtual table before applying decoded message/numeric logic:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND service = :service
  AND max_work_entries = :max_work_entries
  AND (
    message = :exact_message
    OR (
      json_type(metadata, '$.duration_ms') IN ('integer', 'real')
      AND json_extract(metadata, '$.duration_ms') > :duration_threshold
    )
  )
  AND NOT level = :excluded_level
ORDER BY ts DESC
LIMIT :limit;
```

SQLite precedence is `NOT`, then `AND`, then `OR`, matching the shipped
LogsQL composition row; parentheses override it. This recipe is an exact SQL
equivalent for its stated atoms. It does not claim that portable SQLite can
reproduce LogsQL's Unicode word boundaries, RE2-compatible regexps, Unicode
case folding, or the Rust API's exact full-domain `u64`/`f64` comparison. Use
the corresponding bounded Rust query surface for those atoms.

### SQL-LOG-010: field names and typed projection

Discover top-level public response fields without reading extension shadow
tables. `bounded` must stay materialized so JSON expansion cannot move ahead of
the public row/work limits:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
  ORDER BY ts
  LIMIT :limit
), field_rows(name) AS (
  SELECT '_time' FROM bounded
  UNION ALL SELECT '_msg' FROM bounded
  UNION ALL SELECT 'level' FROM bounded
  UNION ALL
  SELECT fields.key
  FROM bounded JOIN json_each(bounded.metadata) AS fields
)
SELECT name, COUNT(*) AS hits
FROM field_rows
GROUP BY name
ORDER BY name;

WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
  ORDER BY ts
  LIMIT :limit
), selected AS (
  SELECT
    bounded.ts,
    bounded.level,
    bounded.message,
    CASE WHEN fields.fullkey IS NULL THEN json('{}')
         ELSE json_object(
           :field,
           json(bounded.metadata -> fields.fullkey)
         )
    END AS projected_metadata
  FROM bounded
  LEFT JOIN json_each(bounded.metadata) AS fields
    ON fields.key = :field
)
SELECT ts, level, message, projected_metadata
FROM selected
ORDER BY ts;
```

The release server binds epoch microseconds; this executable cookbook fixture
uses the table default of milliseconds. `_time`, `_msg`, and `level` are real
public columns rather than metadata keys. The second statement preserves the
selected JSON type—including null, boolean, number, array, and object—and emits
`{}` when the field is missing. Nested projection uses the same approach with a
literal or safely constructed JSON path. The Rust API owns LogsQL wildcard
field syntax, nested-object reconstruction, response limits, and cancellation.

### SQL-LOG-011: current-row filter and empty counts

Apply an any-value filter at the current SQL step, then calculate the exact
complementary `count(field)` and `count_empty(field)` states:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
  ORDER BY ts
  LIMIT :limit
), states AS (
  SELECT bounded.*, fields.fullkey, fields.type,
         bounded.metadata -> fields.fullkey AS value_json
  FROM bounded
  LEFT JOIN json_each(bounded.metadata) AS fields
    ON fields.key = :field
)
SELECT ts, level, message, metadata
FROM states
WHERE fullkey IS NOT NULL
  AND type <> 'null'
  AND NOT (type = 'text' AND value_json = json_quote(''))
ORDER BY ts;

WITH bounded AS MATERIALIZED (
  SELECT ts, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
  ORDER BY ts
  LIMIT :limit
), states AS (
  SELECT fields.fullkey, fields.type,
         bounded.metadata -> fields.fullkey AS value_json
  FROM bounded
  LEFT JOIN json_each(bounded.metadata) AS fields
    ON fields.key = :field
)
SELECT
  SUM(fullkey IS NOT NULL
      AND type <> 'null'
      AND NOT (type = 'text' AND value_json = json_quote('')))
    AS count_field,
  SUM(fullkey IS NULL
      OR type = 'null'
      OR (type = 'text' AND value_json = json_quote('')))
    AS count_empty
FROM states;
```

These statements intentionally distinguish a missing field from stored JSON
null and from an empty string before applying the LogsQL empty/non-empty
classification. Zero, false, arrays, and objects are non-empty. More complex
`filter`/`where` expressions use ordinary SQL composition where possible; the
Rust API owns LogsQL parsing and filters after non-SQL pipeline transforms.

### SQL-LOG-012: typed unique values and counts

Use the full JSON representation plus its logical JSON type as the uniqueness
key. This prevents `0`, `"0"`, and `false` from aliasing:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
  ORDER BY ts
  LIMIT :limit
), states AS (
  SELECT fields.fullkey, fields.type,
         bounded.metadata -> fields.fullkey AS value_json
  FROM bounded
  LEFT JOIN json_each(bounded.metadata) AS fields
    ON fields.key = :field
), nonempty AS (
  SELECT type, value_json
  FROM states
  WHERE fullkey IS NOT NULL
    AND type <> 'null'
    AND NOT (type = 'text' AND value_json = json_quote(''))
)
SELECT COUNT(*) AS count_uniq
FROM (SELECT type, value_json FROM nonempty GROUP BY type, value_json);

WITH bounded AS MATERIALIZED (
  SELECT ts, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
  ORDER BY ts
  LIMIT :limit
), states AS (
  SELECT fields.fullkey, fields.type,
         bounded.metadata -> fields.fullkey AS value_json
  FROM bounded
  LEFT JOIN json_each(bounded.metadata) AS fields
    ON fields.key = :field
)
SELECT type, value_json, COUNT(*) AS hits
FROM states
WHERE fullkey IS NOT NULL
  AND type <> 'null'
  AND NOT (type = 'text' AND value_json = json_quote(''))
GROUP BY type, value_json
ORDER BY CASE type
  WHEN 'false' THEN 1 WHEN 'true' THEN 1
  WHEN 'integer' THEN 2 WHEN 'real' THEN 2
  WHEN 'text' THEN 3 WHEN 'array' THEN 4 WHEN 'object' THEN 5
  ELSE 6 END,
  value_json;

WITH bounded AS MATERIALIZED (
  SELECT ts, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
  ORDER BY ts
  LIMIT :limit
)
SELECT bounded.ts,
       fields.fullkey IS NOT NULL AS present,
       COALESCE(fields.type, 'missing') AS logical_type,
       CASE WHEN fields.fullkey IS NULL THEN NULL
            ELSE bounded.metadata -> fields.fullkey END AS value_json
FROM bounded
LEFT JOIN json_each(bounded.metadata) AS fields
  ON fields.key = :field
ORDER BY bounded.ts;
```

The first two statements implement exact typed `count_uniq` and
`uniq_values` foundations. The third is the lossless `values` foundation: its
`present` bit keeps missing distinct from JSON null. The Rust
`count_uniq_hash` compatibility function uses a bounded stable 64-bit hash set
and is therefore collision-approximate; ordinary SQL should prefer the exact
grouping above.

### SQL-LOG-013: numeric aggregates, median, and rates

Only retained JSON numbers participate. Numeric-looking strings, null,
missing, booleans, arrays, and objects are ignored:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
  ORDER BY ts
  LIMIT :limit
), numeric AS MATERIALIZED (
  SELECT CAST(fields.value AS REAL) AS value
  FROM bounded JOIN json_each(bounded.metadata) AS fields
    ON fields.key = :field
  WHERE fields.type IN ('integer', 'real')
), ordered AS (
  SELECT value,
         ROW_NUMBER() OVER (ORDER BY value) AS position,
         COUNT(*) OVER () AS n
  FROM numeric
)
SELECT
  SUM(value) AS sum_value,
  AVG(value) AS avg_value,
  MIN(value) AS min_value,
  MAX(value) AS max_value,
  (SELECT AVG(value)
   FROM ordered
   WHERE position IN ((n + 1) / 2, (n + 2) / 2)) AS median_value
FROM numeric;

WITH bounded AS MATERIALIZED (
  SELECT ts, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
  ORDER BY ts
  LIMIT :limit
), numeric AS (
  SELECT CAST(fields.value AS REAL) AS value
  FROM bounded JOIN json_each(bounded.metadata) AS fields
    ON fields.key = :field
  WHERE fields.type IN ('integer', 'real')
), window(seconds) AS (
  VALUES ((:end_ms - :start_ms + 1) / 1000.0)
)
SELECT
  (SELECT COUNT(*) FROM bounded) / seconds AS rate,
  (SELECT COALESCE(SUM(value), 0.0) FROM numeric) / seconds AS rate_sum
FROM window;
```

The `+ 1` converts inclusive native-unit storage bounds back into the semantic
window width. Use `1000000.0` for the release server's microsecond tables.
SQLite `REAL` arithmetic has binary64 precision; the Rust API preserves exact
integer values for `min`/`max`, then uses documented binary64 arithmetic for
mixed or fractional `sum`, `avg`, `median`, and rates. Empty `avg`, `min`,
`max`, and `median` results are JSON null in the Rust API because JSON has no
portable NaN literal.

### SQL-LOG-014: exact prefix

The default-field LogsQL query `="prefix"*` is an ordinary case-sensitive,
start-anchored comparison over the public `message` column:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND max_work_entries = :max_work_entries
  AND substr(message, 1, length(:message_prefix))
      = :message_prefix COLLATE BINARY
ORDER BY ts DESC
LIMIT :limit;
```

For a dynamic metadata path known to contain retained strings, use
`json_type` so missing and JSON null project to the same empty text used by an
exact-prefix predicate while stored strings retain their decoded bytes:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), projected AS (
  SELECT *,
    CASE
      WHEN json_type(metadata, :field_path) IS NULL THEN ''
      WHEN json_type(metadata, :field_path) = 'null' THEN ''
      WHEN json_type(metadata, :field_path) = 'text'
        THEN json_extract(metadata, :field_path)
      ELSE NULL
    END AS field_text
  FROM bounded
)
SELECT ts, level, message, metadata
FROM projected
WHERE substr(field_text, 1, length(:field_prefix))
      = :field_prefix COLLATE BINARY
ORDER BY ts DESC
LIMIT :limit;
```

The first statement is exact for the default message field, including an
empty prefix. The second is exact for retained strings plus the documented
missing/null empty projection. SQLite and libSQL do not promise the same
textual formatting as `serde_json` for every floating-point, array, and object
value, so this recipe does not pretend to cover those types. The Rust API
projects retained numbers, booleans, arrays, and objects to compact JSON only
for this predicate, preserves their stored types, applies the same rule to
`exact(value*)`, and owns LogsQL parsing, logical/pipeline composition,
cancellation, limits, and error envelopes. Both statements decode bounded
public rows; neither justifies an extension primitive.

### SQL-LOG-015: static multi-exact membership

For a known static value count, bind every candidate separately and use
ordinary binary `IN` membership over the public message column:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND max_work_entries = :max_work_entries
  AND message COLLATE BINARY IN (:message_value_1, :message_value_2)
ORDER BY ts DESC
LIMIT :limit;
```

If the selected value is a declared string-only `index_keys` column, use the
public hidden column directly. SQLite/libSQL can plan the `IN` values as
repeated equality scans over the existing posting index:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND max_work_entries = :max_work_entries
  AND host COLLATE BINARY IN (:indexed_value_1, :indexed_value_2)
ORDER BY ts DESC
LIMIT :limit;
```

For a dynamic metadata path known to contain retained strings, project
missing and JSON null to the same empty text used by LogsQL before applying
the bound membership list:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), projected AS (
  SELECT *,
    CASE
      WHEN json_type(metadata, :field_path) IS NULL THEN ''
      WHEN json_type(metadata, :field_path) = 'null' THEN ''
      WHEN json_type(metadata, :field_path) = 'text'
        THEN json_extract(metadata, :field_path)
      ELSE NULL
    END AS field_text
  FROM bounded
)
SELECT ts, level, message, metadata
FROM projected
WHERE field_text COLLATE BINARY IN (:field_value_1, :field_value_2)
ORDER BY ts DESC
LIMIT :limit;
```

Generate one placeholder per caller-supplied value and bind it; never splice
values into SQL text. An empty static list is the constant-false predicate
`0`, while the upstream standalone-wildcard form is a no-op and therefore
omits the membership predicate. Bounds use the table's configured timestamp
unit (milliseconds here), remain inclusive, and `max_work_entries` bounds the
public decode before API composition. Ordering is explicitly newest first.

These statements are exact for the message and retained strings, including
case and arbitrary UTF-8 bytes. SQLite/libSQL formatting of floating-point,
array, and object values is not a portable substitute for `serde_json`'s
compact projection. The Rust Logs API therefore parses the static `in()`
list, deduplicates it within the request's bounded query text, projects all
retained rich types without mutating storage, and owns `in()`, wildcard,
logical/pipeline, limit, cancellation, and error semantics. Subquery
membership is separately shipped by `LQL-F38` with the executable
[`SQL-LOG-048`](#sql-log-048-query-backed-exact-membership) foundation. The declared string-key
form reuses the existing public posting index; the other forms use bounded
public rows. No new extension primitive is required.

### SQL-LOG-016: field no-op

A LogsQL field no-op is the constant-true predicate. It does not inspect the
named field and must also match a row where that field is missing. In ordinary
SQL, omit the field predicate entirely; an explicit `1 = 1` makes the mapping
visible in a generated statement:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND max_work_entries = :max_work_entries
  AND 1 = 1
ORDER BY ts DESC
LIMIT :limit;
```

Bounds use the table's configured timestamp unit (milliseconds here), are
inclusive, and remain independently required even though the language atom is
a no-op. `max_work_entries` bounds decoded work, while `:limit` bounds output
after the true predicate. Missing, JSON null, empty, and non-empty values are
all irrelevant; the SQL must not add `json_type`, `json_extract`, or a hidden
index-column condition.

The Rust LogsQL API owns the case-insensitive `in(*)`, `contains_any(*)`, and
`contains_all(*)` function names, field scoping, the rule that any standalone
unquoted wildcard makes the whole static list a no-op, logical/pipeline
composition, malformed errors, and the explicit LQL-F21/LQL-F22/LQL-F38
boundaries. A quoted `"*"` remains an ordinary value and is not this recipe.
The extension already exposes the exact direct-user operation through ordinary
bounded row SQL, so no new primitive is warranted.

### SQL-LOG-017: JSON-array primitive membership

For a static two-value list, bind each candidate independently and expand only
the selected retained JSON array through SQLite/libSQL JSON1:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), candidates(value) AS (
  VALUES (:array_value_1), (:array_value_2)
)
SELECT ts, level, message, metadata
FROM bounded
WHERE json_type(metadata, :field_path) = 'array'
  AND EXISTS (
    SELECT 1
    FROM json_each(bounded.metadata, :field_path) AS element
    JOIN candidates
      ON CASE element.type
           WHEN 'text' THEN CAST(element.value AS TEXT)
           WHEN 'integer' THEN CAST(element.value AS TEXT)
           WHEN 'real' THEN CAST(element.value AS TEXT)
           WHEN 'true' THEN 'true'
           WHEN 'false' THEN 'false'
           WHEN 'null' THEN 'null'
         END COLLATE BINARY = candidates.value COLLATE BINARY
    WHERE element.type IN ('text', 'integer', 'real', 'true', 'false', 'null')
  )
ORDER BY ts DESC
LIMIT :limit;
```

Generate one placeholder per caller-supplied candidate and bind it; never
interpolate values. The empty-list form is the constant-false predicate:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND max_work_entries = :max_work_entries
  AND 0
ORDER BY ts DESC
LIMIT :limit;
```

`:field_path` is a valid SQLite JSON path such as `$.tags`. Bounds use the
table's configured timestamp unit (milliseconds here), are inclusive, and
`max_work_entries` bounds public decoding before JSON expansion. Output is
newest first, and `:limit` applies only after membership. A missing field,
JSON null, scalar, object, or empty array does not match. Only top-level array
strings, numbers, booleans, and null participate; nested arrays and objects
are ignored. String comparison is decoded, byte-exact, and case-sensitive.
Numbers use JSON1's textual projection, booleans use `true`/`false`, and null
uses `null`. A bound empty string matches only an actual empty string element,
and a bound `*` is literal.

This recipe deliberately follows Timeless's retained semantic JSON model:
JSON string escapes are decoded before comparison, so a stored `"a\u0062"`
element matches the candidate `ab`. VictoriaLogs currently has a raw-lexeme
shortcut for this filter that does not make that match; the Rust API records
the intentional compatibility distinction and otherwise owns case-insensitive
function parsing, static-list grammar, logical/pipeline composition, limits,
cancellation, and HTTP errors. Query-backed lists remain deferred `LQL-F38`.
The bounded public row and JSON1 implementation is sufficient, so no extension
primitive is warranted.

### SQL-LOG-018: IPv4 range over retained strings

Bind the inclusive IPv4 bounds as unsigned network-order integers. For
example, `10.0.0.0/24` becomes `:ipv4_min = 167772160` and
`:ipv4_max = 167772415`. This statement selects an arbitrary retained string
field using a bound JSON path and strictly parses four decimal octets:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), projected AS (
  SELECT *,
    CASE WHEN json_type(metadata, :field_path) = 'text'
      THEN json_extract(metadata, :field_path)
    END AS ipv4_text
  FROM bounded
)
SELECT ts, level, message, metadata
FROM projected
WHERE (
  SELECT CASE
    WHEN count(*) = 4
     AND min(length(CAST(octet.value AS TEXT)) BETWEEN 1 AND 3) = 1
     AND min(CAST(octet.value AS TEXT) NOT GLOB '*[^0-9]*') = 1
     AND max(CAST(octet.value AS INTEGER)) <= 255
    THEN sum(
      CAST(octet.value AS INTEGER)
      << ((3 - CAST(octet.key AS INTEGER)) * 8)
    )
  END
  FROM json_each(
    CASE
      WHEN length(ipv4_text) BETWEEN 7 AND 15
       AND ipv4_text NOT GLOB '*[^0-9.]*'
       AND length(ipv4_text) - length(replace(ipv4_text, '.', '')) = 3
      THEN '["' || replace(ipv4_text, '.', '","') || '"]'
      ELSE '[]'
    END
  ) AS octet
) BETWEEN :ipv4_min AND :ipv4_max
ORDER BY ts DESC
LIMIT :limit;
```

`:field_path` is a valid SQLite JSON path such as `$.client_ip`. To query the
public `message` column, use `message AS ipv4_text` in `projected`; to query
`level`, use `level AS ipv4_text`. Bounds use the table's configured timestamp
unit (milliseconds here), are inclusive, and `max_work_entries` bounds public
decode before parsing. The result limit applies after the address predicate,
and output is explicitly newest first.

The recipe accepts decimal octets with leading zeroes, but not signs, spaces,
hex, missing octets, values above 255, embedded addresses, missing fields,
JSON null, or non-string JSON values. An inverted packed range matches
nothing. The Rust API additionally parses the case-insensitive one-address,
one-CIDR, and two-address `ipv4_range(...)` forms, expands `/0` through `/32`,
rejects malformed input, and owns field scoping, logical/pipeline composition,
limits, cancellation, and HTTP errors. Ordinary public SQL already provides
the retained-string operation; no extension scalar or storage change is
justified.

### SQL-LOG-019: bytewise string range over retained text

Bind `:string_min` and `:string_max` as UTF-8 strings and `:field_path` as a
valid SQLite JSON path such as `$.deployment.region`. This statement implements
the retained-text foundation for `field:string_range(minimum, maximum)`:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), projected AS (
  SELECT *,
    CASE
      WHEN json_type(metadata, :field_path) = 'text'
        THEN json_extract(metadata, :field_path)
      WHEN json_type(metadata, :field_path) = 'null'
        OR json_type(metadata, :field_path) IS NULL
        THEN ''
    END AS range_text
  FROM bounded
)
SELECT ts, level, message, metadata
FROM projected
WHERE CAST(range_text AS BLOB) >= CAST(:string_min AS BLOB)
  AND CAST(range_text AS BLOB) < CAST(:string_max AS BLOB)
ORDER BY ts DESC
LIMIT :limit;
```

The lower bound is inclusive and the upper bound is exclusive. Equal or
inverted bounds therefore return no rows. Casting both sides to `BLOB` makes
the comparison an exact unsigned byte ordering over the retained UTF-8 bytes,
independent of connection collation. Bounds use the table's configured
timestamp unit (milliseconds here), both timestamp endpoints are inclusive,
`max_work_entries` bounds public decode, output is newest first, and `:limit`
is applied after the range predicate.

For a missing metadata path or JSON null, `range_text` is the empty string, as
in the pinned LogsQL predicate. An actual empty string is indistinguishable
only for this textual comparison. Non-string JSON numbers, booleans, arrays,
and objects deliberately project SQL NULL in this portable recipe and do not
match. To query the public message or level column, replace the `CASE`
expression with `message AS range_text` or `level AS range_text`.

The Rust API additionally parses quoted/unquoted bounds and trailing commas,
selects arbitrary nested fields, applies the same compact textual projection
to retained rich values without mutating them, composes logical/pipeline
expressions, and owns work/result/response/deadline limits, cancellation, and
HTTP errors. VictoriaLogs flattens nested objects before this predicate;
Timeless preserves those objects and therefore can project a selected parent
object as compact JSON. That retained-model distinction is explicit rather
than hidden in this string-only SQL recipe. Ordinary public rows already
provide the operation, so no extension primitive is warranted.

### SQL-LOG-020: Unicode codepoint length range over retained text

Bind `:length_min` and `:length_max` as non-negative integers and
`:field_path` as a valid SQLite JSON path such as `$.deployment.region`. This
statement implements the retained-text foundation for
`field:len_range(minimum, maximum)`:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), projected AS (
  SELECT *,
    CASE
      WHEN json_type(metadata, :field_path) = 'text'
        THEN json_extract(metadata, :field_path)
      WHEN json_type(metadata, :field_path) = 'null'
        OR json_type(metadata, :field_path) IS NULL
        THEN ''
    END AS length_text
  FROM bounded
)
SELECT ts, level, message, metadata
FROM projected
WHERE length(length_text) >= :length_min
  AND length(length_text) <= :length_max
ORDER BY ts DESC
LIMIT :limit;
```

SQLite `length(TEXT)` counts Unicode code points, not UTF-8 bytes, matching
VictoriaLogs' `utf8.RuneCountInString` behavior for valid retained JSON text.
Both bounds are inclusive; an inverted range returns no rows. Bounds use the
table's configured timestamp unit (milliseconds here), timestamp endpoints
are inclusive, `max_work_entries` bounds public decode, output is newest
first, and `:limit` is applied after the length predicate.

For a missing metadata path or JSON null, `length_text` is the empty string and
therefore has length zero. An actual empty string is indistinguishable only for
this predicate. Non-string JSON numbers, booleans, arrays, and objects project
SQL NULL and do not match in this portable retained-text recipe. To query the
public message or level column, replace the `CASE` expression with
`message AS length_text` or `level AS length_text`.

The Rust API additionally parses quoted and unquoted integer bounds, base-
prefixed and underscored integers, `inf`, byte-size and duration forms, and a
trailing comma. It compact-projects retained rich values without mutating
them, composes logical and pipeline expressions, and owns work/result/response
limits, cancellation, and HTTP errors. VictoriaLogs flattens nested objects;
Timeless preserves and can length-project a selected parent object. Public
SQLite rows already provide the retained-text operation, so no extension
primitive is warranted.

### SQL-LOG-021: same-row textual field comparison

Bind `:left_path` and `:right_path` as valid SQLite JSON paths, such as
`$.actual_duration` and `$.maximum_duration`. Bind `:comparison` to `eq`,
`le_text`, or `lt_text`. This statement projects both retained metadata fields
without changing their stored types, then performs exact textual equality or
an explicitly bytewise ordering comparison:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), projected AS (
  SELECT *,
    CASE json_type(metadata, :left_path)
      WHEN 'text' THEN json_extract(metadata, :left_path)
      WHEN 'true' THEN 'true'
      WHEN 'false' THEN 'false'
      WHEN 'integer' THEN metadata -> :left_path
      WHEN 'real' THEN metadata -> :left_path
      WHEN 'array' THEN json(json_extract(metadata, :left_path))
      WHEN 'object' THEN json(json_extract(metadata, :left_path))
      ELSE ''
    END AS left_text,
    CASE json_type(metadata, :right_path)
      WHEN 'text' THEN json_extract(metadata, :right_path)
      WHEN 'true' THEN 'true'
      WHEN 'false' THEN 'false'
      WHEN 'integer' THEN metadata -> :right_path
      WHEN 'real' THEN metadata -> :right_path
      WHEN 'array' THEN json(json_extract(metadata, :right_path))
      WHEN 'object' THEN json(json_extract(metadata, :right_path))
      ELSE ''
    END AS right_text
  FROM bounded
)
SELECT ts, level, message, metadata
FROM projected
WHERE CASE :comparison
  WHEN 'eq' THEN left_text = right_text COLLATE BINARY
  WHEN 'le_text' THEN CAST(left_text AS BLOB) <= CAST(right_text AS BLOB)
  WHEN 'lt_text' THEN CAST(left_text AS BLOB) < CAST(right_text AS BLOB)
  ELSE 0
END
ORDER BY ts DESC
LIMIT :limit;
```

`eq` is the complete retained-model SQL equivalent of
`left:eq_field(right)`: strings remain strings; numbers and booleans use their
compact JSON spelling; arrays and objects use compact JSON; and missing and
JSON null both project to the empty string. An actual empty string is therefore
equal to either state for this operator. To compare the public message or level
column, replace the corresponding `CASE` expression with `message AS
left_text`/`right_text` or `level AS left_text`/`right_text`.
The JSON `->` operator deliberately retains the exact numeric token instead of
letting `json_extract()` convert unsigned integers above `i64::MAX` to an
indistinguishable binary64 value.

`le_text` and `lt_text` expose the exact unsigned-byte fallback used by
`le_field` and `lt_field`, but are not claimed as complete LogsQL equivalents.
VictoriaLogs first attempts to parse *both* projections as math values:
decimal and base-zero numbers, durations, byte sizes, RFC3339 timestamps, and
IPv4 addresses compare numerically; byte ordering is used only when either
parse fails. The Rust API owns that language-specific branch, function grammar,
case-insensitive names, field aliases, `_time` rendering on the right,
logical/pipeline composition, limits, cancellation, and errors. `_time:` stays
reserved for time filters and therefore cannot be a comparison's left field.

The table's configured timestamp unit is milliseconds in this example, both
timestamp endpoints are inclusive, `max_work_entries` bounds public decode,
output is newest first, and `:limit` is applied after comparison. Both operands
already arrive in the same decoded public row, so an extension primitive would
not remove a block read, decode, allocation, copy, or row crossing.

### SQL-LOG-022: prefix-selected field set

Bind `:field_prefix` to the literal canonical field-name prefix without the
LogsQL trailing `*`, and bind `:exact_text` to a retained string. For example,
`deployment.*:exact(us-east)` becomes `deployment.` and `us-east`. An empty
prefix searches every existing field. This statement recursively flattens only
metadata objects, keeps arrays as leaf values, adds the public `_msg`, `_time`,
and `level` fields, and matches if any selected string or null-valued field has
the requested exact textual value:

```sql
WITH RECURSIVE
bounded AS MATERIALIZED (
  SELECT
    ROW_NUMBER() OVER (ORDER BY ts DESC) AS query_row,
    ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), metadata_fields(
  query_row, ts, level, message, metadata,
  field_name, field_value, field_type
) AS (
  SELECT
    bounded.query_row, bounded.ts, bounded.level,
    bounded.message, bounded.metadata,
    CAST(child.key AS TEXT), child.value, child.type
  FROM bounded
  JOIN json_each(bounded.metadata) AS child
  UNION ALL
  SELECT
    parent.query_row, parent.ts, parent.level,
    parent.message, parent.metadata,
    parent.field_name || '.' || CAST(child.key AS TEXT),
    child.value, child.type
  FROM metadata_fields AS parent
  JOIN json_each(parent.field_value) AS child
  WHERE parent.field_type = 'object'
), fields AS (
  SELECT query_row, ts, level, message, metadata,
         '_msg' AS field_name, message AS field_value, 'text' AS field_type
  FROM bounded
  UNION ALL
  SELECT query_row, ts, level, message, metadata,
         '_time', CAST(ts AS TEXT), 'text'
  FROM bounded
  UNION ALL
  SELECT query_row, ts, level, message, metadata,
         'level', level, 'text'
  FROM bounded
  UNION ALL
  SELECT query_row, ts, level, message, metadata,
         field_name, field_value, field_type
  FROM metadata_fields
  WHERE field_type <> 'object'
), matched AS (
  SELECT query_row, ts, level, message, metadata
  FROM fields
  WHERE substr(field_name, 1, length(:field_prefix)) =
        :field_prefix COLLATE BINARY
    AND CASE field_type
      WHEN 'text' THEN CAST(field_value AS TEXT)
      WHEN 'null' THEN ''
    END = :exact_text COLLATE BINARY
  GROUP BY query_row
)
SELECT ts, level, message, metadata
FROM matched
ORDER BY ts DESC, query_row
LIMIT :limit;
```

The prefix comparison is literal: `%` and `_` have no wildcard meaning, and a
quoted LogsQL prefix containing `:` is bound after quote decoding. A nested
object contributes dotted leaf names such as `deployment.region` but not the
object parent; an array remains one leaf. JSON null is an existing field and
projects to the empty string for this exact-text recipe. A missing prefix has
no candidate and therefore cannot match, even when `:exact_text` is empty.
`query_row` prevents one row matching multiple fields from being duplicated
without collapsing two otherwise identical stored log entries.

The direct `_time` value is the table's native integer timestamp. The Rust API
formats `_time` at the configured precision before applying a LogsQL filter.
It also supplies word, phrase, regexp, pattern, range, logical-group, rich JSON,
and pipeline-current-row semantics; recursively enumerates retained leaves
without allocating a row-wide field list; and owns parser errors, work/result/
response limits, cancellation, and HTTP envelopes. In particular, Timeless
rejects a wildcard left operand for `eq_field`, `le_field`, or `lt_field`:
VictoriaLogs currently treats that spelling as one literal nonexistent field,
which can accidentally match missing/empty projections rather than expanding
the prefix.

Both timestamp endpoints are inclusive and use milliseconds in this example.
`max_work_entries` bounds public extension decode, while recursive field work
is bounded by each already-decoded metadata value. The result limit is applied
after field matching. Ordinary SQL already receives the required public rows;
an extension primitive would not avoid a storage read, decode, allocation,
copy, or row crossing.

### SQL-LOG-023: UTC day range with explicit offset

Bind `:day_start_ns` and `:day_end_ns` to parsed time-of-day offsets in
nanoseconds, `:start_inclusive` and `:end_inclusive` to zero or one from the
written brackets, and `:offset_ns` to the fixed offset applied to UTC before
comparison. For a normal millisecond `timeless_logs` table, bind
`:timestamp_scale_ns = 1000000` and `:units_per_day = 86400000`; a microsecond
table uses `1000` and `86400000000`. This example applies the filter to an
inclusive native timestamp window:

```sql
WITH
arguments AS (
  SELECT
    :day_start_ns AS day_start_ns,
    CASE
      WHEN :end_inclusive = 0 AND :day_end_ns = 0
      THEN 86399999999999
      ELSE :day_end_ns
    END AS day_end_ns,
    :start_inclusive AS start_inclusive,
    CASE
      WHEN :end_inclusive = 0 AND :day_end_ns = 0 THEN 1
      ELSE :end_inclusive
    END AS end_inclusive
), bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), evaluated AS (
  SELECT
    bounded.*,
    (
      (ts % :units_per_day) * :timestamp_scale_ns
      + (:offset_ns % 86400000000000)
    ) % 86400000000000 AS day_offset_ns
  FROM bounded
)
SELECT ts, level, message, metadata
FROM evaluated
CROSS JOIN arguments
WHERE day_start_ns <= day_end_ns
  AND CASE start_inclusive
        WHEN 0 THEN day_offset_ns > day_start_ns
        ELSE day_offset_ns >= day_start_ns
      END
  AND CASE end_inclusive
        WHEN 0 THEN day_offset_ns < day_end_ns
        ELSE day_offset_ns <= day_end_ns
      END
ORDER BY ts DESC
LIMIT :limit;
```

The bounds are points within a day, not minute buckets: a closed `12:00` end
includes exactly 12:00 and excludes 12:00 plus one native tick. An open
midnight end follows VictoriaLogs' special normalization to the final
nanosecond of the day, so `[00:00,00:00)` selects the full day. Any other
inverted range is valid and empty; ranges do not wrap overnight. `24:00` is
clamped to the final nanosecond of the day. SQLite `%` retains the dividend's
sign, matching the source behavior for pre-epoch timestamps and negative
shifts. The offset is reduced before addition so ordinary fixed timezone
offsets cannot overflow the per-day calculation.

The Rust API accepts `HH:MM` and `HHMM` bounds, parses VictoriaLogs signed
compound duration offsets, and deliberately treats an omitted offset as UTC.
It does not read the process-local timezone, whose ambient value would make
the same query change across hosts or daylight-saving transitions. LogsQL
parsing, logical and current-row pipeline composition, work/result/response
limits, cancellation, and HTTP envelopes remain API behavior. The repeated
daily predicate cannot narrow an arbitrary absolute timestamp window by
itself. Ordinary SQL already receives the necessary public timestamp, so an
extension primitive would not avoid block reads, decode, allocation, copy, or
row crossing.

### SQL-LOG-024: UTC week range with explicit offset

Number Sunday through Saturday as zero through six. Bind `:week_start_day`
and `:week_end_day` after applying bracket normalization: increment an open
start modulo seven and decrement an open end modulo seven. Bind `:offset_ns`
to the fixed duration added to UTC before weekday selection. Millisecond
tables use `:timestamp_scale_ns = 1000000` and
`:units_per_day = 86400000`; microsecond tables use `1000` and
`86400000000`.

```sql
WITH
bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), split AS (
  SELECT
    bounded.*,
    ts / :units_per_day
      - CASE WHEN ts % :units_per_day < 0 THEN 1 ELSE 0 END
      AS epoch_day,
    ((ts % :units_per_day + :units_per_day) % :units_per_day)
      * :timestamp_scale_ns AS timestamp_day_ns,
    :offset_ns / 86400000000000
      - CASE WHEN :offset_ns % 86400000000000 < 0 THEN 1 ELSE 0 END
      AS offset_days,
    ((:offset_ns % 86400000000000) + 86400000000000)
      % 86400000000000 AS offset_day_ns
  FROM bounded
), evaluated AS (
  SELECT
    split.*,
    (
      (
        epoch_day + offset_days
        + (timestamp_day_ns + offset_day_ns) / 86400000000000
        + 4
      ) % 7 + 7
    ) % 7 AS utc_weekday
  FROM split
)
SELECT ts, level, message, metadata
FROM evaluated
WHERE :week_start_day <= :week_end_day
  AND utc_weekday >= :week_start_day
  AND utc_weekday <= :week_end_day
ORDER BY ts DESC
LIMIT :limit;
```

The linear interval never wraps from Saturday to Sunday; a normalized start
above the normalized end is valid and empty. Bracket normalization retains
VictoriaLogs' edge behavior: `[Sun,Sun)` becomes zero through six (the full
week), while `[Mon,Mon)` becomes one through zero (empty), and `(Sat,Sun)`
also becomes the full week. The Euclidean day calculation is explicit because
SQLite integer division truncates toward zero; the recipe therefore preserves
pre-epoch weekdays and signed multi-day offsets. Bounds and timestamps remain
in the table's native unit, the weekday bounds are inclusive after
normalization, and output order is descending timestamp order.

The Rust API accepts case-insensitive short and full English weekday names,
parses brackets and VictoriaLogs signed compound durations, and deliberately
uses UTC when the offset is omitted rather than reading mutable process-local
timezone state. It also owns logical and current-row pipeline composition,
errors, work/result/response limits, cancellation, and HTTP envelopes. A
weekly predicate cannot independently narrow an arbitrary absolute timestamp
window. The public row already contains the necessary timestamp, so an
extension primitive would not avoid block reads, decode, allocation, copy, or
row crossing.

### SQL-LOG-025: delete exact retained metadata fields

Bind `:delete_path_1` and `:delete_path_2` as SQLite JSON paths such as
`$.duration_ms`, `$.nested.drop`, or `$."field,with,punctuation"`. The
timestamp bounds use the table's native unit: milliseconds for a default
`timeless_logs` table or microseconds when it was created with
`timestamp_unit='us'`.

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), deleted AS (
  SELECT
    ts,
    level,
    message,
    json_remove(metadata, :delete_path_1, :delete_path_2) AS metadata
  FROM bounded
)
SELECT ts, level, message, metadata
FROM deleted
ORDER BY ts DESC
LIMIT :limit;
```

`json_remove` retains JSON types and treats a missing path as a no-op. Paths
are case-sensitive, the statement deletes both paths before ordering, and the
result is a read-only projection: stored metadata is unchanged. To omit the
public `message`, `level`, or `ts` columns, leave that column out of the final
`SELECT`; `_msg`, `level`, and `_time` are the Rust API's row names, with
`_time` formatted from native `ts`.

The Rust API additionally owns case-insensitive `delete`, `del`, `drop`, and
`rm` grammar; quoted field names; exact deletion of `_msg`, `_time`, and
`level`; recursive dotted rich-object paths; literal prefix deletion; ordered
pipeline composition; pruning empty parents and fully empty rows; strict
errors; work/result/response limits; cancellation; and HTTP envelopes.
SQLite's `json_remove` deliberately leaves an emptied parent object in place,
so this recipe is the honest exact-path storage foundation rather than a
claim of complete LogsQL pipeline equivalence. Prefix deletion and recursive
pruning happen over already bounded public rows in Rust and do not justify an
extension primitive.

### SQL-LOG-026: request-local log query statistics

Run these two statements on the same SQLite/libSQL connection. Fully consume
the first result before immediately executing the second. The first statement
uses the table's native timestamp unit and an inclusive interval:

```sql
SELECT ts, level, message, metadata
FROM logs
WHERE ts >= :start_ms
  AND ts <= :end_ms
  AND service = :service
  AND max_work_entries = :max_work_entries
ORDER BY ts;
```

The successful `timeless_logs` scan publishes one request-owned report. This
statement consumes it and presents the LogsQL-compatible field names and TEXT
value types:

```sql
SELECT
  CAST(0 AS TEXT) AS BytesReadColumnsHeaders,
  CAST(0 AS TEXT) AS BytesReadColumnsHeaderIndexes,
  CAST(0 AS TEXT) AS BytesReadBloomFilters,
  CAST(payload_bytes_read AS TEXT) AS BytesReadValues,
  CAST(0 AS TEXT) AS BytesReadTimestamps,
  CAST(0 AS TEXT) AS BytesReadBlockHeaders,
  CAST(payload_bytes_read AS TEXT) AS BytesReadTotal,
  CAST(processed_blocks AS TEXT) AS BlocksProcessed,
  CAST(processed_entries AS TEXT) AS RowsProcessed,
  CAST(matched_entries AS TEXT) AS RowsFound,
  CAST(values_read AS TEXT) AS ValuesRead,
  CAST(timestamps_read AS TEXT) AS TimestampsRead,
  CAST(0 AS TEXT) AS BytesProcessedUncompressedValues,
  CAST(query_total_ns AS TEXT) AS QueryDurationNsecs
FROM timeless_log_query_stats('logs');
```

`timeless_log_query_stats` has one hidden `tbl` input and sixteen visible
INTEGER columns: `query_total_ns`, `query_snapshot_ns`,
`query_materialize_ns`, `snapshot_payload_bytes`, `payload_bytes_read`,
`candidate_blocks`, `processed_blocks`, `blocks_skipped_by_bound`,
`buffered_entries_processed`, `decoded_entries`, `processed_entries`,
`matched_entries`, `returned_entries`, `values_read`, `timestamps_read`, and
`stable_location_snapshot` (zero or one). A plain table name selects `main`;
use `schema.table` for an attached database.

The report is scoped to one connection and table, is published only after a
successful scan, and is consumed exactly once. A new, failed, or cancelled
scan clears an older unconsumed report. A second read, a fresh connection, a
non-log table, or a call before the row statement fails explicitly. These
rules prevent pooled/concurrent readers from being approximated through
process-wide before/after counter deltas. The report describes work performed
even when the scan occurred inside a transaction that was later rolled back.

Timeless block codecs read one complete encoded payload containing timestamp,
severity, message, and the rich metadata envelope. They do not have separately
addressable VictoriaLogs column-header, header-index, bloom, timestamp, or
block-header files. The equivalent therefore attributes the complete encoded
payload to `BytesReadValues`, makes `BytesReadTotal` identical, and returns
zero for the unavailable component counters. `ValuesRead` is three logical
non-timestamp slots per processed Timeless row; `TimestampsRead` is one per
processed row. `BytesProcessedUncompressedValues` remains zero rather than
paying a query-time reserialization tax to invent an incomparable value.

The Rust LogsQL API substitutes the exact logical row count after its typed and
nested post-filters, measures duration through the `query_stats` pipeline
position, returns all fourteen values as strings, and allows later pipelines.
It currently executes the complete bounded API rowset eagerly, so a preceding
`limit` does not retroactively reduce physical work counters. VictoriaLogs can
cancel parallel storage workers after a limit; its reported work is scheduling
dependent. Both products report work actually performed. Language parsing,
result/response limits, cancellation, and HTTP errors remain API behavior.

### SQL-LOG-027: first numeric rows per partition

Bind `:sort_path`, `:tie_path`, and `:partition_path` as SQLite JSON paths.
The timestamp bounds use the table's native unit. This statement returns the
first `:first_count` rows in every textual partition and emits the rank as
TEXT, matching LogsQL's public rank value type:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), ranked AS (
  SELECT
    ts,
    level,
    message,
    metadata,
    COALESCE(CAST(json_extract(metadata, :partition_path) AS TEXT), '')
      AS partition_value,
    row_number() OVER (
      PARTITION BY
        COALESCE(CAST(json_extract(metadata, :partition_path) AS TEXT), '')
      ORDER BY
        CAST(json_extract(metadata, :sort_path) AS REAL),
        COALESCE(CAST(json_extract(metadata, :tie_path) AS TEXT), ''),
        ts,
        level,
        message,
        metadata
    ) AS partition_rank
  FROM bounded
)
SELECT
  ts,
  level,
  message,
  metadata,
  CAST(partition_rank AS TEXT) AS rank
FROM ranked
WHERE partition_rank <= :first_count
ORDER BY partition_value, partition_rank
LIMIT :max_result_rows;
```

This is exact for a sort path whose present values are JSON numbers or
numeric strings representable by SQLite `REAL`; missing and JSON null both
project to SQL NULL and sort before those values, and `:tie_path` makes equal
sort values deterministic. The partition projection maps missing and null to
the same empty string, ranks restart at one inside each partition, partitions
and rows are deterministic, and retained metadata types are unchanged.
`:first_count`, `:max_work_entries`, and `:max_result_rows` must be positive.

The Rust API implements the complete LogsQL operation. It compares exact
signed and unsigned integers before floating point, recognizes RFC3339 times,
durations and byte sizes, applies VictoriaLogs natural UTF-8 ordering, permits
per-field `asc`/`desc`, reads nested rich paths, and implements the no-`by`
form over the current pipeline row schema. It also owns quoted grammar,
partition-key framing, rank insertion, strict errors, state/work/result/
response limits, cancellation, and HTTP envelopes. SQLite has no built-in
VictoriaLogs natural collation, so arbitrary textual sort values are not
claimed by this numeric recipe. The bounded public rows already contain all
required values; no extension primitive or private table would save a block
read or decode.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_first_is_typed_partitioned_bounded_and_durable`.

### SQL-LOG-028: last numeric rows per partition

Bind `:sort_path`, `:tie_path`, and `:partition_path` as SQLite JSON paths.
The timestamp bounds use the table's native unit. This statement returns the
last `:last_count` rows in every textual partition in reverse order and emits
the rank as TEXT, matching LogsQL's public rank value type:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), ranked AS (
  SELECT
    ts,
    level,
    message,
    metadata,
    COALESCE(CAST(json_extract(metadata, :partition_path) AS TEXT), '')
      AS partition_value,
    row_number() OVER (
      PARTITION BY
        COALESCE(CAST(json_extract(metadata, :partition_path) AS TEXT), '')
      ORDER BY
        CAST(json_extract(metadata, :sort_path) AS REAL) DESC,
        COALESCE(CAST(json_extract(metadata, :tie_path) AS TEXT), '') DESC,
        ts DESC,
        level DESC,
        message DESC,
        metadata DESC
    ) AS partition_rank
  FROM bounded
)
SELECT
  ts,
  level,
  message,
  metadata,
  CAST(partition_rank AS TEXT) AS rank
FROM ranked
WHERE partition_rank <= :last_count
ORDER BY partition_value, partition_rank
LIMIT :max_result_rows;
```

This is exact for a sort path whose present values are JSON numbers or
numeric strings representable by SQLite `REAL`; missing and JSON null both
project to SQL NULL and follow present numeric values in descending order.
`:tie_path` makes equal sort values deterministic. The partition projection
maps missing and null to the same empty string, ranks restart at one inside
each partition, partitions and rows are deterministic, and retained metadata
types are unchanged. `:last_count`, `:max_work_entries`, and
`:max_result_rows` must be positive.

The Rust API implements the complete LogsQL operation by reversing the same
order as `first`; every per-field `desc` reverses again. It compares exact
signed and unsigned integers before floating point, recognizes RFC3339 times,
durations and byte sizes, applies VictoriaLogs natural UTF-8 ordering, reads
nested rich paths, and implements the no-`by` form over the current pipeline
row schema. It also owns quoted grammar, partition-key framing, rank insertion,
strict errors, state/work/result/response limits, cancellation, and HTTP
envelopes. SQLite has no built-in VictoriaLogs natural collation, so arbitrary
textual sort values are not claimed by this numeric recipe. The bounded public
rows already contain all required values; no extension primitive or private
table would save a block read or decode.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_last_inverts_first_with_same_bounds_and_durability`.

### SQL-LOG-029: top values by hit count

Bind `:group_path` as a SQLite JSON path and use the table's native timestamp
unit. This statement groups one retained public field by the LogsQL textual
projection, orders the most frequent value first with a deterministic bytewise
tie-break, and returns both hit count and one-based rank as TEXT:

```sql
WITH bounded AS MATERIALIZED (
  SELECT metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), projected AS (
  SELECT
    CASE
      WHEN json_type(metadata, :group_path) IS NULL
        OR json_type(metadata, :group_path) = 'null' THEN ''
      WHEN json_type(metadata, :group_path) = 'true' THEN 'true'
      WHEN json_type(metadata, :group_path) = 'false' THEN 'false'
      ELSE CAST(json_extract(metadata, :group_path) AS TEXT)
    END AS group_value
  FROM bounded
), grouped AS (
  SELECT group_value, count(*) AS hits
  FROM projected
  GROUP BY group_value
), ranked AS (
  SELECT
    group_value,
    hits,
    row_number() OVER (ORDER BY hits DESC, group_value ASC) AS position
  FROM grouped
)
SELECT
  group_value,
  CAST(hits AS TEXT) AS hits,
  CAST(position AS TEXT) AS rank
FROM ranked
WHERE position <= :top_count
ORDER BY position
LIMIT :max_result_rows;
```

Missing and JSON null share the empty-text group; strings remain unquoted;
numbers use their retained SQLite textual representation; booleans are
`true`/`false`; and retained arrays/objects use JSON text. Equal hit counts use
ascending SQLite byte order. `:top_count`, `:max_work_entries`, and
`:max_result_rows` must be positive. The recipe returns an explicit empty
`group_value` column; VictoriaLogs stream JSON and the Rust API omit that
field from the result object while still returning its hits and rank.

The Rust API owns case-insensitive `top`, optional `by`, parenthesized and bare
multi-field lists, default ten, `hits [as]`, `rank [as]`, collision-safe result
names, current-pipeline-row composition, strict errors, state/work/result/
response limits, cancellation, and HTTP envelopes. It groups multiple values
with unambiguous vector keys and preserves the stored rich source; summary
values are strings because that is the LogsQL result contract. All required
values already cross the public bounded row interface, so neither private
tables nor a new extension primitive would avoid storage work.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_top_counts_textual_groups_with_bounds_and_durability`.

### SQL-LOG-030: unique textual values

Bind `:group_path` as a SQLite JSON path and use the table's native timestamp
unit. `:filter_text` is an optional case-sensitive substring, `:uniq_limit = 0`
means no operator-specific limit, and `:with_hits` selects whether the hits
column is populated. This statement groups one retained public field using the
LogsQL textual projection and returns a deterministic bytewise subset:

```sql
WITH bounded AS MATERIALIZED (
  SELECT metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :max_work_entries
), projected AS (
  SELECT
    CASE
      WHEN json_type(metadata, :group_path) IS NULL
        OR json_type(metadata, :group_path) = 'null' THEN ''
      WHEN json_type(metadata, :group_path) = 'true' THEN 'true'
      WHEN json_type(metadata, :group_path) = 'false' THEN 'false'
      ELSE CAST(json_extract(metadata, :group_path) AS TEXT)
    END AS group_value
  FROM bounded
), grouped AS (
  SELECT group_value, count(*) AS hits
  FROM projected
  WHERE :filter_text = '' OR instr(group_value, :filter_text) > 0
  GROUP BY group_value
), ranked AS (
  SELECT
    group_value,
    hits,
    count(*) OVER () AS group_count,
    row_number() OVER (ORDER BY group_value ASC) AS position
  FROM grouped
)
SELECT
  group_value,
  CASE
    WHEN :with_hits = 0 THEN NULL
    WHEN :uniq_limit > 0 AND group_count > :uniq_limit THEN '0'
    ELSE CAST(hits AS TEXT)
  END AS hits
FROM ranked
WHERE :uniq_limit >= 0
  AND :max_result_rows > 0
  AND (
    (:uniq_limit = 0 AND group_count <= :max_result_rows)
    OR (
      :uniq_limit > 0
      AND :uniq_limit <= :max_result_rows
      AND position <= :uniq_limit
    )
  )
ORDER BY position
LIMIT :max_result_rows;
```

Missing, JSON null, and the empty string share the empty-text group. Strings
remain unquoted; numbers use their retained SQLite textual representation;
booleans are `true`/`false`; and retained arrays/objects use JSON text.
`instr` is a bytewise, case-sensitive substring test. The statement retains an
explicit empty `group_value` column and returns SQL `NULL` when
`:with_hits = 0`; the Rust API omits both fields from the stream JSON object.

`:uniq_limit` must be nonnegative; `:with_hits` must be zero or one; and
`:max_work_entries` and `:max_result_rows` must be positive. A positive unique
limit larger than `:max_result_rows`, or an unbounded result whose cardinality
exceeds `:max_result_rows`, deliberately returns no rows in direct SQL; the API
preflights the same conditions and returns its explicit HTTP 422 limit
envelope. When cardinality exceeds a positive unique limit, returned hits are
the string `"0"`, matching VictoriaLogs' unknown-hit contract.

VictoriaLogs does not promise which hash-map groups survive a positive limit
or their output order. Timeless deliberately selects the first N bytewise
group keys and emits them in that order so direct SQL and API callers receive
repeatable results. Multi-field `uniq` extends the `projected` and `GROUP BY`
lists with one column per exact public path. The Rust API owns that grammar,
current-pipeline-row composition, collision-safe hits naming, omission of
empty fields, structural multi-field keys, strict errors, state/work/result/
response limits, cancellation, and HTTP envelopes. The public bounded rows
already contain every required value, so a new extension primitive would not
avoid a storage read, decode, or row crossing.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_uniq_is_textual_bounded_and_durable`.

### SQL-LOG-031: bounded facets over public log fields

Bind `:start_ts` and `:end_ts` in the table's native timestamp unit and set
`:timestamp_units_per_second` to `1000` for millisecond tables or `1000000`
for microsecond tables. `:facets_limit`, `:max_values_per_field`, and
`:max_value_len` are positive values; direct callers normalize a positive
fraction by truncating it before binding, matching the pinned LogsQL grammar.
`:keep_const_fields` is zero or one. This statement uses only the public logs
virtual table plus SQLite JSON1 and window functions:

```sql
WITH RECURSIVE bounded AS MATERIALIZED (
  SELECT
    row_number() OVER (ORDER BY ts, level, message, metadata) AS row_id,
    ts,
    level,
    message,
    metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), raw_fields(row_id, field_name, raw_value, field_type) AS (
  SELECT b.row_id, CAST(j.key AS TEXT), j.value, j.type
  FROM bounded AS b, json_each(b.metadata) AS j
  WHERE j.key NOT IN ('_time', '_msg', 'level')

  UNION ALL

  SELECT
    f.row_id,
    f.field_name || '.' || CAST(j.key AS TEXT),
    j.value,
    j.type
  FROM raw_fields AS f, json_each(f.raw_value) AS j
  WHERE f.field_type = 'object'
), current_fields AS (
  SELECT
    row_id,
    field_name,
    CASE field_type
      WHEN 'null' THEN ''
      WHEN 'true' THEN 'true'
      WHEN 'false' THEN 'false'
      ELSE CAST(raw_value AS TEXT)
    END AS field_value
  FROM raw_fields
  WHERE field_type <> 'object'

  UNION ALL

  SELECT
    row_id,
    '_time',
    strftime(
      '%Y-%m-%dT%H:%M:%S',
      (
        ts
        - ((ts % :timestamp_units_per_second)
           + :timestamp_units_per_second)
          % :timestamp_units_per_second
      ) / :timestamp_units_per_second,
      'unixepoch'
    ) || CASE :timestamp_units_per_second
      WHEN 1000 THEN printf(
        '.%03dZ',
        ((ts % :timestamp_units_per_second)
         + :timestamp_units_per_second)
        % :timestamp_units_per_second
      )
      WHEN 1000000 THEN printf(
        '.%06dZ',
        ((ts % :timestamp_units_per_second)
         + :timestamp_units_per_second)
        % :timestamp_units_per_second
      )
    END
  FROM bounded

  UNION ALL

  SELECT row_id, 'level', CAST(level AS TEXT) FROM bounded

  UNION ALL

  SELECT row_id, '_msg', message FROM bounded
), nonempty AS (
  SELECT row_id, field_name, field_value
  FROM current_fields
  WHERE field_value <> ''
), grouped AS (
  SELECT field_name, field_value, count(*) AS hits
  FROM nonempty
  GROUP BY field_name, field_value
), ranked AS (
  SELECT
    field_name,
    field_value,
    hits,
    count(*) OVER (PARTITION BY field_name) AS unique_values,
    max(length(CAST(field_value AS BLOB)))
      OVER (PARTITION BY field_name) AS longest_value_bytes,
    row_number() OVER (
      PARTITION BY field_name
      ORDER BY hits DESC, field_value COLLATE BINARY ASC
    ) AS position
  FROM grouped
), selected AS (
  SELECT field_name, field_value, hits, position
  FROM ranked
  WHERE unique_values <= CAST(:max_values_per_field AS INTEGER)
    AND longest_value_bytes <= CAST(:max_value_len AS INTEGER)
    AND (
      :keep_const_fields = 1
      OR unique_values <> 1
      OR hits <> (SELECT count(*) FROM bounded)
    )
    AND position <= CAST(:facets_limit AS INTEGER)
), bounded_result AS (
  SELECT
    field_name,
    field_value,
    CAST(hits AS TEXT) AS hits,
    position,
    count(*) OVER () AS result_rows
  FROM selected
)
SELECT field_name, field_value, hits
FROM bounded_result
WHERE :timestamp_units_per_second IN (1000, 1000000)
  AND :facets_limit >= 1
  AND :max_values_per_field >= 1
  AND :max_value_len >= 1
  AND :keep_const_fields IN (0, 1)
  AND :max_work_entries > 0
  AND :max_result_rows > 0
  AND result_rows <= :max_result_rows
ORDER BY field_name COLLATE BINARY ASC, position ASC;
```

Objects are recursively exposed as dotted leaf names, while arrays remain one
atomic JSON-text value. Missing fields, JSON null, and empty strings do not
contribute facet values. Numbers and booleans use the same textual projection
as LogsQL. `length(CAST(field_value AS BLOB))` makes `max_value_len` a UTF-8
byte limit rather than a character limit. If any nonempty value for a field is
too long, or its distinct textual cardinality exceeds the configured maximum,
the entire field is excluded. A field is constant only when one nonempty value
appears in every selected row.

The explicit `_time` branch reproduces the Rust API's RFC3339 millisecond or
microsecond rendering, including pre-epoch Euclidean remainders. Top-level
metadata keys named `_time`, `_msg`, or `level` are excluded because the public
API replaces them with canonical values. SQLite's binary tie break makes the
direct result repeatable; VictoriaLogs only promises descending hits within a
field and does not define equal-hit order.

The statement returns no rows when the final cardinality exceeds
`:max_result_rows`; the Rust API instead returns an explicit HTTP 422 envelope.
The API also owns case-insensitive/reorderable/repeated modifier grammar,
current-pipeline composition, collision handling for flattened paths,
per-request row/item/byte limits, cancellation, and response encoding. Every
required value already crosses the public bounded row interface, so a new
extension primitive would not reduce storage reads, block decode, or row
crossing.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_facets_are_flattened_bounded_and_durable`.

### SQL-LOG-032: first nonempty textual log field

Bind three SQLite JSON paths, a textual default, native timestamp bounds, and
a positive public row-work limit. This ordinary statement selects the first
nonempty LogsQL-style textual value from three exact retained metadata paths:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), projected AS (
  SELECT
    ts,
    level,
    message,
    metadata,
    CASE
      WHEN json_type(metadata, :source_path_1) IS NULL
        OR json_type(metadata, :source_path_1) = 'null' THEN ''
      WHEN json_type(metadata, :source_path_1) = 'true' THEN 'true'
      WHEN json_type(metadata, :source_path_1) = 'false' THEN 'false'
      ELSE CAST(json_extract(metadata, :source_path_1) AS TEXT)
    END AS value_1,
    CASE
      WHEN json_type(metadata, :source_path_2) IS NULL
        OR json_type(metadata, :source_path_2) = 'null' THEN ''
      WHEN json_type(metadata, :source_path_2) = 'true' THEN 'true'
      WHEN json_type(metadata, :source_path_2) = 'false' THEN 'false'
      ELSE CAST(json_extract(metadata, :source_path_2) AS TEXT)
    END AS value_2,
    CASE
      WHEN json_type(metadata, :source_path_3) IS NULL
        OR json_type(metadata, :source_path_3) = 'null' THEN ''
      WHEN json_type(metadata, :source_path_3) = 'true' THEN 'true'
      WHEN json_type(metadata, :source_path_3) = 'false' THEN 'false'
      ELSE CAST(json_extract(metadata, :source_path_3) AS TEXT)
    END AS value_3
  FROM bounded
)
SELECT
  ts,
  level,
  message,
  metadata,
  COALESCE(
    NULLIF(value_1, ''),
    NULLIF(value_2, ''),
    NULLIF(value_3, ''),
    :default_value
  ) AS coalesced_value
FROM projected
WHERE :max_result_rows > 0
ORDER BY ts, level, message, metadata
LIMIT :max_result_rows;
```

Missing, JSON null, and the empty string are skipped. Strings remain
unquoted; numbers use SQLite's retained textual representation; booleans are
`true`/`false`; and retained arrays/objects use compact JSON text. The final
default is always textual and may itself be empty. Extend the repeated
projection and `NULLIF` list for additional exact fields, or substitute the
public `message`, `level`, or rendered `ts` columns when those canonical
fields are sources. Bounds use the table's native millisecond or microsecond
unit and are inclusive in this recipe.

The Rust API owns case-insensitive `coalesce(...)`, quoted names, trailing
comma, exact/all/prefix expansion across the current recursively flattened
row, duplicate-source suppression, destination default `_msg`, optional
`default` and `as`, current-pipeline composition, rich-path insertion, an
explicit field-conflict envelope, hard work/state/result/response limits,
cancellation, and HTTP encoding. Exact object sources are absent in the
flattened compatibility view, while arrays remain atomic textual values.
Timeless retains an explicitly empty destination string in its rich response;
VictoriaLogs stream JSON omits empty-valued columns during serialization.

All inputs already cross the public bounded log-row interface. Ordinary
`CASE`, `NULLIF`, and `COALESCE` are complete and efficient for exact direct
SQL use, so no new extension primitive or private shadow-table access is
justified.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_coalesce_is_textual_bounded_and_durable`.

### SQL-LOG-033: copy one exact retained metadata field

Bind one SQLite JSON source path, one exact top-level destination path, native
timestamp bounds, and positive row/result limits. This ordinary statement
copies one retained metadata value without changing its JSON type or removing
the source:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), copied AS (
  SELECT
    ts,
    level,
    message,
    json_set(
      metadata,
      :copy_destination_path,
      json(
        CASE
          WHEN json_type(metadata, :copy_source_path) IS NULL
            OR json_type(metadata, :copy_source_path) = 'object'
          THEN '""'
          ELSE metadata -> :copy_source_path
        END
      )
    ) AS copied_metadata
  FROM bounded
)
SELECT ts, level, message, copied_metadata
FROM copied
WHERE :max_result_rows > 0
ORDER BY ts, level, message, copied_metadata
LIMIT :max_result_rows;
```

`:copy_source_path` and `:copy_destination_path` use SQLite JSON-path syntax;
this recipe intentionally constrains the destination to one top-level path
such as `$.copied`. Strings, numbers, booleans, arrays, null, and empty strings
retain their JSON types. A missing exact source becomes an explicit empty
string. An exact object parent is absent from VictoriaLogs' flattened-column
view and therefore also becomes an empty string; copy an exact leaf or use a
prefix operation in the Rust API instead. The original source and every other
metadata value remain unchanged.

Repeat `json_set` in query order for multiple exact pairs. The Rust API owns
case-insensitive `copy`/`cp`, optional `as`, quoted fields, comma-separated
sequential pairs, exact/all/prefix flattened source expansion, prefix
substitution, deterministic last-write-wins behavior, canonical fields,
current-pipeline composition, and explicit nested rich-object conflict
errors. It also enforces work/state/result/response limits, cancellation, and
HTTP envelopes. Timeless retains explicit null, empty, and missing-copy values
in rich responses; VictoriaLogs stream JSON omits empty-valued columns.

Every source value already crosses the public bounded log-row interface.
JSON1 provides the complete direct exact-field operation, so no extension
primitive or private shadow-table access is justified.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_copy_is_typed_sequential_bounded_and_durable`.

### SQL-LOG-034: rename one exact top-level retained metadata field

Bind exact top-level SQLite JSON paths for one source and destination, native
timestamp bounds, and positive row/result limits. This ordinary statement
moves a retained metadata value without changing its JSON type:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), renamed AS (
  SELECT
    ts,
    level,
    message,
    CASE
      WHEN :rename_source_path = :rename_destination_path
        AND json_type(metadata, :rename_source_path) IS NOT NULL
      THEN metadata
      WHEN json_type(metadata, :rename_source_path) IS NULL
        OR json_type(metadata, :rename_source_path) = 'object'
      THEN json_set(metadata, :rename_destination_path, json('""'))
      ELSE json_remove(
        json_set(
          metadata,
          :rename_destination_path,
          json(metadata -> :rename_source_path)
        ),
        :rename_source_path
      )
    END AS renamed_metadata
  FROM bounded
)
SELECT ts, level, message, renamed_metadata
FROM renamed
WHERE :max_result_rows > 0
ORDER BY ts, level, message, renamed_metadata
LIMIT :max_result_rows;
```

`:rename_source_path` and `:rename_destination_path` use SQLite JSON-path
syntax and are intentionally constrained to exact top-level paths such as
`$.duration_ms` and `$.moved`. Strings, numbers, booleans, arrays, null, and
empty strings retain their JSON types. A present non-object source is removed,
an existing scalar destination is overwritten, and an identical leaf source
and destination is unchanged. A missing exact source produces an explicit
empty destination. An exact object parent is absent from VictoriaLogs'
flattened-column view, so it is retained while the distinct destination
becomes empty.

The Rust API rejects an exact object-parent self-rename or any destination
that would replace a retained object or descend through a scalar. Direct SQL
callers must likewise preflight `json_type(metadata,
:rename_destination_path) <> 'object'` when the destination is not known to be
absent or scalar. Use the API for nested sources because it recursively prunes
empty parents after removing a leaf; this portable top-level recipe avoids
claiming that plain `json_remove` provides that richer behavior.

Repeat the `CASE` expression in query order for multiple exact top-level
pairs. The Rust API owns case-insensitive `rename`/`mv`, optional `as`, quoted
fields, strict comma-separated sequential pairs, exact/all/prefix flattened
source snapshots, prefix substitution, deterministic last-write-wins
behavior, canonical fields, current-pipeline composition, recursive
rich-path deletion/insertion, and explicit conflict errors. It also enforces
work/state/result/response limits, cancellation, and HTTP envelopes. Timeless
retains explicit null, empty, and missing-rename values in rich responses;
VictoriaLogs stream JSON omits empty-valued columns.

Every source value already crosses the public bounded log-row interface.
JSON1 provides the complete direct top-level operation, so no extension
primitive or private shadow-table access is justified.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_rename_is_typed_sequential_bounded_and_durable`.

### SQL-LOG-035: format two exact retained metadata fields

Bind two exact SQLite JSON paths, a SQLite `printf` pattern containing two
`%s` placeholders, native timestamp bounds, and positive work/result limits.
This ordinary statement formats values read through the public log table:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), textual AS (
  SELECT
    ts,
    level,
    message,
    CASE
      WHEN json_type(metadata, :format_source_path_1) IS NULL
        OR json_type(metadata, :format_source_path_1) = 'null' THEN ''
      WHEN json_type(metadata, :format_source_path_1) = 'true' THEN 'true'
      WHEN json_type(metadata, :format_source_path_1) = 'false' THEN 'false'
      WHEN json_type(metadata, :format_source_path_1) = 'text'
        THEN json_extract(metadata, :format_source_path_1)
      ELSE json(metadata -> :format_source_path_1)
    END AS first_value,
    CASE
      WHEN json_type(metadata, :format_source_path_2) IS NULL
        OR json_type(metadata, :format_source_path_2) = 'null' THEN ''
      WHEN json_type(metadata, :format_source_path_2) = 'true' THEN 'true'
      WHEN json_type(metadata, :format_source_path_2) = 'false' THEN 'false'
      WHEN json_type(metadata, :format_source_path_2) = 'text'
        THEN json_extract(metadata, :format_source_path_2)
      ELSE json(metadata -> :format_source_path_2)
    END AS second_value
  FROM bounded
), formatted AS (
  SELECT
    ts,
    level,
    message,
    printf(:format_pattern, first_value, second_value) AS formatted_value
  FROM textual
)
SELECT ts, level, message, formatted_value
FROM formatted
WHERE :max_result_rows > 0
ORDER BY ts, level, message, formatted_value
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` are inclusive native table timestamp units. For a
microsecond table, bind microseconds. `:format_source_path_1` and
`:format_source_path_2` are exact SQLite JSON paths such as `$.host` and
`$.duration_ms`; `:format_pattern` is SQLite syntax such as
`host=%s duration_ms=%s`, not LogsQL placeholder syntax. Missing and explicit
null project to empty text, strings lose their JSON quotes, booleans use
lowercase JSON spelling, and numbers, arrays, and scalar JSON values use
SQLite's canonical JSON text. Retained object parents are available to direct
SQL even though VictoriaLogs exposes only flattened leaves. Results are
deterministic in ascending `ts`, `level`, `message`, and formatted-value order.

The complete `LQL-P22` API additionally owns case-insensitive `format`, its
optional `if (...)`, quoted/unquoted patterns, arbitrary `<field>`
interpolation, HTML-decoded literal prefixes, default `_msg` or exact `as`
destination, `keep_original_fields`, `skip_empty_results`, and the `uc`, `lc`,
`q`, URL, hex, Base64, numeric-hex, `time`, `duration`, `duration_seconds`, and
`ipv4` transformations. It evaluates over recursively retained rich current
rows, enforces work/state/result/response limits and cancellation, reports
field conflicts, and shapes HTTP errors. Compose additional `CASE` values and
`printf` placeholders in ordinary SQL when that is clearer than using the
language API.

Every selected value already crosses the bounded public log-row interface.
SQLite JSON1 and `printf` perform the direct operation without a new extension
primitive, private shadow-table access, or storage-format change.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_format_is_complete_bounded_rich_and_durable`.

### SQL-LOG-036: arithmetic over exact retained numeric fields

Bind two exact SQLite JSON paths, a numeric multiplier, inclusive native
timestamp bounds, and positive work/result limits. This ordinary statement
evaluates `first * multiplier + second` over public log rows:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), numeric_fields AS (
  SELECT
    ts,
    level,
    message,
    json_type(metadata, :math_source_path_1) AS first_type,
    json_extract(metadata, :math_source_path_1) AS first_value,
    json_type(metadata, :math_source_path_2) AS second_type,
    json_extract(metadata, :math_source_path_2) AS second_value
  FROM bounded
), calculated AS (
  SELECT
    ts,
    level,
    message,
    CASE
      WHEN first_type IN ('integer', 'real')
       AND second_type IN ('integer', 'real')
      THEN first_value * :math_multiplier + second_value
      ELSE NULL
    END AS calculated_value
  FROM numeric_fields
)
SELECT ts, level, message, calculated_value
FROM calculated
WHERE :max_result_rows > 0
ORDER BY ts, level, message, calculated_value
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` use the table's declared timestamp unit.
`:math_source_path_1` and `:math_source_path_2` are exact SQLite JSON paths,
such as `$.duration_ms` and `$.nested.count`; `:math_multiplier` is bound as a
number. SQLite JSON `integer` and `real` values participate. Missing, null,
string, boolean, array, or object inputs produce SQL `NULL`, an explicit
invalid-result sentinel rather than SQLite's misleading `CAST('bad' AS REAL)
= 0`. Results are deterministic in ascending timestamp, level, message, and
calculated-value order.

The complete `LQL-P23` API additionally accepts case-insensitive `math` and
`eval`, comma-separated sequential expressions, optional `as`, canonical
expression destinations, unary signs, parentheses, left-associative `^`,
`*`, `/`, `%`, `+`, `-`, `&`, `xor`, `or`, and NaN-only `default`. It supports
`abs`, `ceil`, `exp`, `floor`, `ln`, `max`, `min`, `now`, `rand`, and `round`;
coerces VictoriaLogs numbers, durations, byte sizes, RFC3339 timestamps, and
IPv4 addresses; and emits fixed shortest `NaN`/`+Inf`/`-Inf` float strings.
Invalid or rich values become `NaN`, not SQL `NULL`. The API owns this
language behavior, sequential current-row mutation, rich destination
conflicts, AST/work/state/result/response limits, cancellation, and HTTP
envelopes. Direct users should compose ordinary SQL expressions and `CASE`
branches when those operations are sufficient.

Every selected value already crosses the bounded public log-row interface.
SQLite JSON1 and arithmetic perform the direct operation without a new
extension primitive, private shadow-table access, or storage-format change.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_math_is_sequential_typed_and_durable`.

### SQL-LOG-037: UTF-8 byte length of one exact retained field

Bind one exact SQLite JSON path, inclusive native timestamp bounds, and
positive work/result limits. This ordinary statement returns the byte length
of the public field's VictoriaLogs-compatible textual projection:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), measured AS (
  SELECT
    ts,
    level,
    message,
    CASE
      WHEN json_type(metadata, :len_source_path) IS NULL THEN 0
      WHEN json_type(metadata, :len_source_path) IN ('null', 'object') THEN 0
      WHEN json_type(metadata, :len_source_path) = 'true' THEN 4
      WHEN json_type(metadata, :len_source_path) = 'false' THEN 5
      WHEN json_type(metadata, :len_source_path) = 'text' THEN length(
        CAST(json_extract(metadata, :len_source_path) AS BLOB)
      )
      ELSE length(CAST(json(metadata -> :len_source_path) AS BLOB))
    END AS byte_length
  FROM bounded
)
SELECT ts, level, message, byte_length
FROM measured
WHERE :max_result_rows > 0
ORDER BY ts, level, message, byte_length
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` use the table's declared timestamp unit, and
`:len_source_path` is one exact SQLite JSON path such as `$.host`,
`$.nested.count`, or `$.tags`. Casting text to `BLOB` before `length()` is
essential: SQLite `length(TEXT)` counts Unicode code points, while the LogsQL
`len` pipe counts UTF-8 bytes. Missing, explicit null, and a retained object
parent have length zero because VictoriaLogs exposes flattened leaves rather
than an object-parent column. Strings count their decoded UTF-8 bytes;
booleans count lowercase `true`/`false`; numbers and arrays count compact JSON
text. The SQL result is an integer. The API renders the result as a decimal
string, matching VictoriaLogs stream JSON.

The complete `LQL-P24` API also accepts case-insensitive `len`, optional
parentheses, optional `as`, empty quoted `_msg` aliases, canonical `_msg`,
`_time`, and `level` sources, exact nested current-row paths, a default `_msg`
destination, and sequential composition. It preserves retained rich source
types, rejects a destination that would replace an object or descend through
a scalar, and enforces work/state/result/response limits, cancellation, and
HTTP envelopes. Repeat this ordinary SQL expression in explicit CTE order
when direct users need several sequential calculations.

Every selected value already crosses the bounded public log-row interface.
SQLite JSON1 and `length(CAST(... AS BLOB))` provide the direct operation
without a new extension primitive, private shadow-table access, or storage-
format change.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_len_counts_textual_bytes_and_preserves_rich_sources`.

### SQL-LOG-038: drop one empty retained metadata field

Bind one exact SQLite JSON path, inclusive native timestamp bounds, and
positive work/result limits. This ordinary statement removes the selected
retained metadata field only when its typed value is JSON null or an empty
string:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), cleaned AS (
  SELECT
    ts,
    level,
    message,
    CASE
      WHEN json_type(metadata, :drop_empty_path) = 'null' THEN
        json_remove(metadata, :drop_empty_path)
      WHEN json_type(metadata, :drop_empty_path) = 'text'
       AND json_extract(metadata, :drop_empty_path) = '' THEN
        json_remove(metadata, :drop_empty_path)
      ELSE metadata
    END AS cleaned_metadata
  FROM bounded
)
SELECT ts, level, message, cleaned_metadata
FROM cleaned
WHERE :max_result_rows > 0
ORDER BY ts, level, message, cleaned_metadata
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` use the table's declared timestamp unit, and
`:drop_empty_path` is one exact SQLite JSON path such as `$.optional` or
`$.nested.empty`. Missing paths are unchanged. Numeric zero, boolean false,
arrays (including `[]`), objects, and nonempty strings retain their exact JSON
types and values. The statement reads and returns only public `logs` columns;
it does not mutate stored metadata. Repeat the `CASE` in explicit CTE order
for every known path in a fixed application schema. After removing the last
child of a known object, direct users may apply the same test to that parent
path to prune it.

The complete `LQL-P28` API dynamically visits every field in the current
pipeline row, including canonical `_msg`, `_time`, and `level`. It removes
empty strings and nulls, recursively prunes newly empty object parents, omits
a row when no fields remain, treats arrays as atomic nonempty fields, and
observes transformations from earlier pipeline stages. Its traversal has a
128-level nesting ceiling, a request work bound, periodic cancellation, final
result/response limits, and stable HTTP errors. Timeless preserves rich JSON
types while VictoriaLogs exposes flattened textual columns; both select the
same empty/nonempty field set.

Every selected value already crosses the bounded public log-row interface.
SQLite JSON1 provides the useful fixed-schema operation directly. Dynamic
field discovery and recursive current-row composition remain in the Rust API;
moving them into an extension primitive would not avoid storage reads, block
decode, or row crossing.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_drop_empty_fields_is_typed_bounded_and_durable`.

### SQL-LOG-039: literal replacement in one exact retained field

Bind one exact SQLite JSON path, old and new literal strings, an explicit
condition flag, inclusive native timestamp bounds, and positive work/result
limits. This ordinary statement applies SQLite's literal all-occurrence
`replace()` to the VictoriaLogs-compatible textual projection of that public
metadata field:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), textual AS (
  SELECT
    ts,
    level,
    message,
    CASE
      WHEN json_type(metadata, :replace_path) IS NULL THEN ''
      WHEN json_type(metadata, :replace_path) IN ('null', 'object') THEN ''
      WHEN json_type(metadata, :replace_path) = 'text' THEN
        json_extract(metadata, :replace_path)
      WHEN json_type(metadata, :replace_path) = 'true' THEN 'true'
      WHEN json_type(metadata, :replace_path) = 'false' THEN 'false'
      ELSE json(metadata -> :replace_path)
    END AS source_value
  FROM bounded
), replaced AS (
  SELECT
    ts,
    level,
    message,
    CASE
      WHEN :replace_enabled = 0 OR :replace_old = '' THEN source_value
      ELSE replace(source_value, :replace_old, :replace_new)
    END AS replaced_value
  FROM textual
)
SELECT ts, level, message, replaced_value
FROM replaced
WHERE :max_result_rows > 0
ORDER BY ts, level, message, replaced_value
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` use the table's declared timestamp unit.
`:replace_path` is one exact SQLite JSON path such as `$.host` or
`$.nested.value`; `:replace_old` and `:replace_new` are bound literal strings,
not regular expressions. `:replace_enabled` is zero to preserve the textual
source and nonzero to apply replacement. Missing, explicit null, and retained
object parents project to empty text. Strings lose JSON quotes; booleans use
lowercase JSON spelling; numbers and arrays use compact JSON. An empty old
substring is explicitly a no-op. The query is read-only and deterministically
orders by timestamp, level, message, and result.

The complete `LQL-P29` API additionally owns case-insensitive `replace`,
quoted and unquoted substring grammar, optional arbitrary `if (...)`, default
`_msg` or exact `at` targets, zero-as-unbounded or first-`N` replacement
limits, canonical and nested current-row paths, and sequential composition.
When no substring matches, Timeless preserves the retained native JSON value;
an actual replacement produces a string while leaving durable storage
unchanged. The API bounds projected arrays, matches, output bytes, work,
result rows, response bytes, and cancellation and shapes stable HTTP errors.
Direct users can add an ordinary SQL predicate or `CASE` for a condition and
use an explicit recursive CTE when first-`N` rather than all-occurrence
replacement is required.

Every selected value already crosses the bounded public log-row interface.
SQLite's core `replace()` performs the useful literal operation directly, so
no extension primitive, private shadow-table access, or storage-format change
is justified.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_replace_is_literal_typed_bounded_and_durable`.

### SQL-LOG-040: two literal-delimited fields from one exact retained field

Bind one exact SQLite JSON source path, fixed literal prefix, middle, and suffix
delimiters, inclusive native timestamp bounds, and positive work/result limits.
This ordinary statement implements the unquoted fixed-pattern foundation for
`prefix<first>middle<second>suffix` with SQLite `instr()` and `substr()`:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), textual AS (
  SELECT
    ts,
    level,
    message,
    CASE
      WHEN json_type(metadata, :extract_source_path) IS NULL THEN ''
      WHEN json_type(metadata, :extract_source_path) IN ('null', 'object') THEN ''
      WHEN json_type(metadata, :extract_source_path) = 'text' THEN
        json_extract(metadata, :extract_source_path)
      WHEN json_type(metadata, :extract_source_path) = 'true' THEN 'true'
      WHEN json_type(metadata, :extract_source_path) = 'false' THEN 'false'
      ELSE json(metadata -> :extract_source_path)
    END AS source_value
  FROM bounded
), prefix_positions AS (
  SELECT *, instr(source_value, :extract_prefix) AS prefix_position
  FROM textual
), first_positions AS (
  SELECT *,
         prefix_position + length(:extract_prefix) AS first_start,
         CASE
           WHEN prefix_position = 0 OR :extract_middle = '' THEN 0
           ELSE instr(
             substr(source_value, prefix_position + length(:extract_prefix)),
             :extract_middle
           )
         END AS middle_position
  FROM prefix_positions
), second_positions AS (
  SELECT *,
         first_start + middle_position - 1 + length(:extract_middle) AS second_start
  FROM first_positions
), extracted AS (
  SELECT *,
         CASE
           WHEN prefix_position = 0 OR middle_position = 0 THEN ''
           ELSE substr(source_value, first_start, middle_position - 1)
         END AS first_value,
         CASE
           WHEN prefix_position = 0 OR middle_position = 0 THEN ''
           WHEN :extract_suffix = '' THEN substr(source_value, second_start)
           WHEN instr(substr(source_value, second_start), :extract_suffix) = 0 THEN ''
           ELSE substr(
             source_value,
             second_start,
             instr(substr(source_value, second_start), :extract_suffix) - 1
           )
         END AS second_value
  FROM second_positions
)
SELECT ts, level, message, first_value, second_value
FROM extracted
WHERE :max_result_rows > 0
ORDER BY ts, level, message, first_value, second_value
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` use the table's declared timestamp unit.
`:extract_source_path` is one exact SQLite JSON path such as `$.client_ip` or
`$.nested.value`. Missing, explicit null, and retained object parents project
to empty text; strings lose JSON quotes; booleans use lowercase JSON spelling;
numbers and arrays use compact JSON. `:extract_middle` must be nonempty,
matching the language rule that adjacent captures require a delimiter. An
empty prefix anchors the first capture at the start; a nonempty prefix may
begin anywhere. A missing prefix or middle delimiter returns both captures as
empty strings. Once the middle delimiter matches, a missing nonempty suffix
keeps the first capture and returns an empty second capture. An empty suffix
captures the remaining text. The statement is read-only and deterministic.

The complete `LQL-P32` API additionally owns case-insensitive `extract`,
quoted/unquoted pattern grammar, HTML-decoded delimiters, any number of named
or anonymous placeholders, automatic Go-style double/single/raw quoted-string
decoding, per-field `plain:`, optional arbitrary `if (...)`, default `_msg` or
exact `from` sources, nested current-row destinations, sequential pipeline
composition, `keep_original_fields`, and `skip_empty_results`. Timeless keeps
explicit empty strings and native rich values distinguishable; it refuses to
replace a retained object with a scalar capture. The API bounds source
projection, pattern work, decoded captures, destination state, final result
rows, response bytes, and cancellation and shapes stable HTTP errors.

Every selected value already crosses the bounded public log-row interface.
SQLite core string functions provide the useful fixed-pattern operation
directly. General pattern parsing and request-local rich-row mutation remain
in the Rust API; moving them into an extension primitive would not avoid a
storage read, block decode, or row crossing.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_extract_is_literal_typed_bounded_and_durable`.

### SQL-LOG-041: pack selected rich metadata fields as JSON

Bind a fixed list of exact SQLite JSON paths, inclusive native timestamp
bounds, and positive work/result limits. This recursive statement builds one
compact JSON object per public log row while preserving strings, numbers,
booleans, arrays, objects, explicit nulls, empty strings, and nested paths:

```sql
WITH RECURSIVE
bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), selected(position, path) AS (
  VALUES
    (1, :pack_path_1),
    (2, :pack_path_2),
    (3, :pack_path_3),
    (4, :pack_path_4),
    (5, :pack_path_5),
    (6, :pack_path_6)
), packed(ts, level, message, metadata, position, packed_json) AS (
  SELECT ts, level, message, metadata, 0, json('{}')
  FROM bounded
  UNION ALL
  SELECT
    packed.ts,
    packed.level,
    packed.message,
    packed.metadata,
    selected.position,
    CASE
      WHEN json_type(packed.metadata, selected.path) IS NULL
      THEN packed.packed_json
      ELSE json_set(
        packed.packed_json,
        selected.path,
        packed.metadata -> selected.path
      )
    END
  FROM packed
  JOIN selected ON selected.position = packed.position + 1
)
SELECT ts, level, message, packed_json
FROM packed
WHERE position = (SELECT count(*) FROM selected)
  AND :max_result_rows > 0
ORDER BY ts, level, message, packed_json
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` use the table's declared timestamp unit. The six
`:pack_path_N` values are valid exact SQLite JSON paths such as `$.host`,
`$.nested.ok`, or `$.tags`; extend or shorten the `selected` values for the
application's fixed schema. `json_type(...) IS NULL` means the path is
missing and omits that key. The JSON type name `null` is non-NULL, so explicit
JSON null is packed. The `->` operator preserves JSON subtypes when
`json_set()` inserts booleans, numbers, arrays, objects, and nulls. Empty text
is retained. Nested paths are reconstructed. Output is compact JSON TEXT,
ordered deterministically with its source identity. The statement is
read-only; it never updates durable metadata.

The complete `LQL-P34` API additionally owns case-insensitive `pack_json`
grammar; default `_msg`, optional bare, `as`, quoted, and dotted exact
destinations; `fields (...)` exact/all/prefix selectors; canonical `_msg`,
`_time`, and `level`; recursively selected current-row fields; sequential
pipeline composition; and field-conflict, work, response, deadline, and
cancellation envelopes. An empty selector list or a list containing `*`
packs every current field. The API snapshots the row before overwriting its
destination and keeps other fields unchanged.

VictoriaLogs flattens every value to text, omits empty values, preserves
current column order, and can emit duplicate JSON keys for overlapping
selectors. Timeless deliberately returns one deterministic valid JSON object:
selector overlap is an idempotent set union, retained native types and
explicit empty/null values survive, and nested metadata remains nested. This
is the selected compatibility policy rather than a claim of byte-identical
output for richer Timeless rows.

Every packed value already crosses the bounded public `logs` row interface.
SQLite JSON1 provides the useful fixed-schema operation directly; moving
dynamic LogsQL grammar or current-row mutation into the extension would not
avoid a storage read, block decode, or row crossing. No extension primitive,
private shadow-table access, or storage-format change is warranted.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_pack_json_is_rich_bounded_and_durable`.

### SQL-LOG-042: unpack selected rich fields from a JSON object

Bind one exact SQLite JSON source path, a fixed list of exact paths relative to
that source, inclusive native timestamp bounds, and positive work/result
limits. This recursive statement accepts either a retained native object or a
string containing a valid JSON object and reconstructs one typed, nested JSON
object per public log row:

```sql
WITH RECURSIVE
bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), sources AS MATERIALIZED (
  SELECT
    ts,
    level,
    message,
    metadata,
    CASE
      WHEN json_type(metadata, :unpack_source_path) = 'object'
      THEN metadata -> :unpack_source_path
      WHEN json_type(metadata, :unpack_source_path) = 'text'
       AND json_valid(json_extract(metadata, :unpack_source_path))
       AND json_type(json_extract(metadata, :unpack_source_path)) = 'object'
      THEN json(json_extract(metadata, :unpack_source_path))
      ELSE NULL
    END AS source_json
  FROM bounded
), selected(position, path) AS (
  VALUES
    (1, :unpack_path_1),
    (2, :unpack_path_2),
    (3, :unpack_path_3),
    (4, :unpack_path_4),
    (5, :unpack_path_5)
), unpacked(ts, level, message, metadata, source_json, position, unpacked_json) AS (
  SELECT ts, level, message, metadata, source_json, 0, json('{}')
  FROM sources
  WHERE source_json IS NOT NULL
  UNION ALL
  SELECT
    unpacked.ts,
    unpacked.level,
    unpacked.message,
    unpacked.metadata,
    unpacked.source_json,
    selected.position,
    CASE
      WHEN json_type(unpacked.source_json, selected.path) IS NULL
      THEN json_set(unpacked.unpacked_json, selected.path, '')
      ELSE json_set(
        unpacked.unpacked_json,
        selected.path,
        unpacked.source_json -> selected.path
      )
    END
  FROM unpacked
  JOIN selected ON selected.position = unpacked.position + 1
)
SELECT ts, level, message, unpacked_json
FROM unpacked
WHERE position = (SELECT count(*) FROM selected)
  AND :max_result_rows > 0
ORDER BY ts, level, message, unpacked_json
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` use the table's declared timestamp unit.
`:unpack_source_path` is one exact SQLite JSON path such as `$.payload` or
`$.nested`. The source may be a retained object or JSON-object text with
surrounding whitespace. Other strings, arrays, scalars, missing paths, and
explicit nulls produce no unpacked row in this focused statement. A caller
that needs the language's no-op behavior can `LEFT JOIN` this result to the
bounded source identity and retain the original row when `unpacked_json` is
NULL.

The five `:unpack_path_N` values are valid exact paths relative to the source,
such as `$.ok`, `$.nested.value`, or `$.tags`; extend or shorten `selected` for
the application's fixed schema. A missing exact path becomes empty text.
Explicit JSON null remains null, an empty string remains empty text, and `->`
preserves native booleans, numbers, arrays, objects, and null when `json_set()`
reconstructs nesting. Output is compact JSON TEXT with deterministic source
ordering. The statement is read-only and never changes durable metadata.

The complete `LQL-P36` API additionally owns case-insensitive `unpack_json`
grammar; optional `if (...)`; default `_msg`, bare, `from`, quoted, and dotted
exact sources; exact, prefix, empty-list, and all-field selection;
`preserve_keys`; arbitrary `result_prefix`; current-row source snapshots;
sequential pipeline composition; `keep_original_fields`;
`skip_empty_results`; malformed-object compatibility; literal dotted keys;
and field-conflict, work, response, deadline, and cancellation envelopes. It
accepts the pinned VictoriaLogs bare `NaN` token as the string `"NaN"`.

VictoriaLogs flattens object leaves to textual columns, compact-serializes
arrays, textualizes numbers and booleans, and uses empty text for JSON null.
Timeless deliberately preserves native rich JSON types and reconstructs
nested objects so the retained storage model does not lose fidelity. The
upstream grammar and selection behavior remain pinned; this type policy is an
explicit compatibility boundary rather than a claim of identical rich-row
output.

Every source value already crosses the bounded public `logs` row interface.
SQLite JSON1 provides the useful fixed-schema parse/select/reconstruction
operation directly. Moving dynamic LogsQL grammar or request-local row
mutation into the extension would not avoid a storage read, block decode, or
row crossing. No extension primitive, private shadow-table access, or storage
format change is warranted.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_unpack_json_is_rich_bounded_and_durable`.

### SQL-LOG-043: top-level JSON array length

Bind one exact SQLite JSON source path, inclusive native timestamp bounds, and
positive work/result limits. This statement counts only the top-level elements
of either a retained native JSON array or a string containing a valid JSON
array:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), sources AS MATERIALIZED (
  SELECT
    ts,
    level,
    message,
    metadata,
    json_type(metadata, :json_array_source_path) AS source_type,
    json_extract(metadata, :json_array_source_path) AS source_value
  FROM bounded
), lengths AS (
  SELECT
    ts,
    level,
    message,
    metadata,
    CASE
      WHEN source_type = 'array'
      THEN json_array_length(metadata, :json_array_source_path)
      WHEN source_type = 'text'
      THEN CASE
        WHEN json_valid(source_value)
        THEN CASE
          WHEN json_type(source_value) = 'array'
          THEN json_array_length(source_value)
          ELSE 0
        END
        ELSE 0
      END
      ELSE 0
    END AS array_length
  FROM sources
)
SELECT
  ts,
  level,
  message,
  CAST(array_length AS TEXT) AS array_length
FROM lengths
WHERE :max_result_rows > 0
ORDER BY ts, level, message, array_length
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` use the table's declared timestamp unit.
`:json_array_source_path` is one exact SQLite JSON path such as `$.tags` or
`$.nested.array_text`. A native array or valid JSON-array string may contain
any JSON element types; nested arrays and objects each count as one top-level
element. Surrounding JSON whitespace is accepted. An empty array returns
`"0"`. Missing paths, explicit nulls, malformed JSON text, JSON scalar text,
and native scalar or object values also return `"0"`. The result is decimal
TEXT to match the LogsQL field result. The statement is read-only and leaves
the retained source unchanged.

The complete `LQL-P41` API additionally owns case-insensitive
`json_array_len` grammar; parenthesized and bare exact fields; quoted and
dotted paths; optional `as`; default `_msg`; sequential current-row mutation;
destination conflict protection; and work, response, deadline, cancellation,
and HTTP error envelopes. It accepts the pinned VictoriaLogs bare `NaN` token
inside JSON-array text and counts it as one element. SQLite JSON1 deliberately
does not claim that non-standard compatibility behavior in this ordinary SQL
recipe.

Every source value already crosses the bounded public `logs` row interface,
and SQLite JSON1 supplies the fixed-path count without decoding private block
state. Moving the LogsQL grammar or request-local destination mutation into
the extension would not avoid a storage read, block decode, or row crossing.
No extension primitive, private shadow-table access, or storage-format change
is warranted.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_json_array_len_is_typed_bounded_and_durable`.

### SQL-LOG-044: upper-step numeric quantile and population standard deviation

Bind one exact SQLite JSON path, a quantile in `[0,1]`, inclusive native
timestamp bounds, and positive work/result limits. This statement computes an
upper-step quantile and population standard deviation over finite native JSON
numbers from the public `logs` table:

```sql
WITH RECURSIVE bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), numeric_values AS MATERIALIZED (
  SELECT
    ROW_NUMBER() OVER (ORDER BY ts, level, message, metadata) - 1 AS sequence,
    CAST(json_extract(metadata, :stats_source_path) AS REAL) AS value
  FROM bounded
  WHERE json_type(metadata, :stats_source_path) IN ('integer', 'real')
    AND ABS(CAST(json_extract(metadata, :stats_source_path) AS REAL))
          <= 1.7976931348623157e308
), ranked AS MATERIALIZED (
  SELECT
    value,
    ROW_NUMBER() OVER (ORDER BY value, sequence) - 1 AS value_rank,
    COUNT(*) OVER () AS value_count
  FROM numeric_values
), welford(step, sample_count, mean, squared_deviations) AS (
  SELECT 0, 0, 0.0, 0.0
  UNION ALL
  SELECT
    w.step + 1,
    w.sample_count + 1,
    w.mean + (n.value - w.mean) / (w.sample_count + 1),
    w.squared_deviations
      + (n.value - w.mean)
      * (n.value - (w.mean + (n.value - w.mean) / (w.sample_count + 1)))
  FROM welford AS w
  JOIN numeric_values AS n ON n.sequence = w.step
), final AS (
  SELECT sample_count, mean, squared_deviations
  FROM welford
  ORDER BY step DESC
  LIMIT 1
)
SELECT
  sample_count,
  CASE
    WHEN sample_count = 0 OR :quantile < 0 OR :quantile > 1 THEN NULL
    ELSE (
      SELECT value
      FROM ranked
      WHERE value_rank = MIN(
        CAST(:quantile * value_count AS INTEGER),
        value_count - 1
      )
      LIMIT 1
    )
  END AS quantile_value,
  CASE
    WHEN sample_count = 0 THEN NULL
    ELSE SQRT(squared_deviations / sample_count)
  END AS population_stddev
FROM final
WHERE :max_result_rows > 0
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` use the table's declared timestamp unit.
`:stats_source_path` is an exact metadata path such as `$.duration_ms`.
Only JSON `integer` and `real` values participate; missing paths, explicit
nulls, strings (including numeric-looking strings), booleans, arrays, and
objects are ignored. `sample_count` zero returns SQL NULL for both statistics;
a singleton deviation is zero. Quantile rank is
`min(floor(phi * N), N - 1)`, so this is the same upper-step selection used by
LogsQL for a finite native-number domain. Welford's one-pass population state
divides by `N`, not `N - 1`. Ordering by the public row identity makes the
floating-point update deterministic. The statement is read-only.

The complete `LQL-S07` API additionally owns case-insensitive `quantile` and
`stddev` grammar; exact, prefix, and all-current-field selection; formatted
`_time`; compact textual projection of retained booleans, arrays, and objects;
VictoriaLogs signed/unsigned/timestamp/math/natural text ordering; exact empty
results; strict phi errors; stable aliases; hard work/state/response limits;
deadline cancellation; and HTTP envelopes. `stddev` deliberately follows the
established Timeless typed-statistics policy and never coerces a stored string
to a number.

VictoriaLogs uses randomized reservoir sampling above 10,000 quantile values.
Timeless instead returns an explicit limit error when exact deterministic
state would exceed `max_work_rows` or `max_response_bytes`. SQLite's ordinary
numeric ordering also cannot claim the full mixed textual comparator, and a
REAL cast cannot preserve integers beyond binary64 precision. This recipe is
therefore an honest finite native-number foundation, not a claim that core
SQLite reproduces every language-owned edge.

All selected values already cross the bounded public `logs` row interface.
Sorting and Welford composition do not avoid a block read or decode when moved
into an extension opcode, so no new extension primitive, private shadow-table
access, or storage-format change is warranted.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regressions:
`session_seventeen_quantile_and_stddev_match_retained_semantics_and_reopen`
and `session_seventeen_quantile_state_is_bounded_and_reader_remains_reusable`.

### SQL-LOG-045: summed UTF-8 byte length of one exact field

Bind one exact SQLite JSON path, inclusive native timestamp bounds, and
positive work/result limits. This read-only statement sums the UTF-8 byte
length of that public field's retained textual projection:

```sql
WITH bounded AS MATERIALIZED (
  SELECT metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), measured AS (
  SELECT CASE
    WHEN json_type(metadata, :sum_len_source_path) IS NULL THEN 0
    WHEN json_type(metadata, :sum_len_source_path) = 'null' THEN 0
    WHEN json_type(metadata, :sum_len_source_path) = 'true' THEN 4
    WHEN json_type(metadata, :sum_len_source_path) = 'false' THEN 5
    WHEN json_type(metadata, :sum_len_source_path) = 'text' THEN length(
      CAST(json_extract(metadata, :sum_len_source_path) AS BLOB)
    )
    ELSE length(CAST(json(metadata -> :sum_len_source_path) AS BLOB))
  END AS byte_length
  FROM bounded
)
SELECT COALESCE(SUM(byte_length), 0) AS sum_len
FROM measured
WHERE :max_result_rows > 0
LIMIT :max_result_rows;
```

`:start_ts` and `:end_ts` use the table's declared timestamp unit.
`:sum_len_source_path` is one exact SQLite JSON path such as `$.host`,
`$.nested.count`, or `$.tags`. Missing paths and explicit nulls contribute
zero. Strings contribute their decoded UTF-8 byte length, booleans contribute
the lowercase JSON spellings `true` or `false`, and numbers, arrays, and
objects contribute compact JSON bytes. Casting to `BLOB` before `length()` is
required because SQLite counts Unicode code points for `TEXT`, while LogsQL
`sum_len` counts bytes. Empty input returns integer zero. SQLite `SUM` is a
signed 64-bit integer aggregate and fails on overflow; keep
`:max_work_entries` and stored value sizes below that bound.

The complete `LQL-S09` API owns case-insensitive `sum_len(fields...)` grammar;
exact, prefix, and all-current-field selection; canonical `_time`, `_msg`, and
`level` fields; rich current-row composition; a checked unsigned 64-bit total;
strict work/result/response limits; cooperative deadline cancellation; and
HTTP envelopes. Empty parentheses select all current fields. Every selected
missing or null value contributes zero, while each selected traversal still
counts against `max_work_rows`. Timeless returns a native JSON integer under
its retained typed-statistics policy; VictoriaLogs returns the same value as
decimal text.

Dynamic prefix/all-field expansion and the canonical fields remain ordinary
bounded Rust language composition. Every selected row already crosses the
public `logs` interface, and SQLite JSON1 provides the exact-path reduction
without a new extension primitive, private shadow-table access, or storage-
format change.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_sum_len_counts_utf8_text_and_reopens`.

### SQL-LOG-046: deterministic any and numeric companion-field extrema

For deterministic `any`, bind one exact SQLite JSON path, inclusive native
timestamp bounds, and positive work/result limits. This read-only statement
returns the first nonempty retained value in explicit public-row order as a
JSON type plus lossless compact JSON text:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), candidates AS (
  SELECT
    ts,
    level,
    message,
    metadata,
    json_type(metadata, :any_source_path) AS value_type,
    json(metadata -> :any_source_path) AS value_json,
    CASE
      WHEN json_type(metadata, :any_source_path) IS NULL THEN ''
      WHEN json_type(metadata, :any_source_path) = 'null' THEN ''
      WHEN json_type(metadata, :any_source_path) = 'text' THEN
        json_extract(metadata, :any_source_path)
      WHEN json_type(metadata, :any_source_path) = 'true' THEN 'true'
      WHEN json_type(metadata, :any_source_path) = 'false' THEN 'false'
      ELSE json(metadata -> :any_source_path)
    END AS projected_text
  FROM bounded
)
SELECT value_type, value_json
FROM candidates
WHERE projected_text <> ''
  AND :max_result_rows > 0
ORDER BY ts, level, message, metadata
LIMIT MIN(:max_result_rows, 1);
```

`:any_source_path` is one exact SQLite JSON path such as `$.host` or
`$.payload`. Missing, explicit null, and empty strings do not qualify; zero,
false, numbers, arrays, and objects do. `value_type` distinguishes SQLite's
scalar representations, and `value_json` preserves strings, booleans,
numbers, arrays, objects, and null without flattening. Direct users may apply
`json_extract(value_json, '$')` when they specifically want a scalar SQL
value. Explicit ordering makes this deterministic instead of copying
VictoriaLogs' encoding-dependent `any` selection.

For `field_min` and `field_max` over a finite native-number comparison field,
bind exact comparison/result paths and the same bounds. This second statement
returns the companion result from the minimum and maximum rows:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), candidates AS MATERIALIZED (
  SELECT
    ts,
    level,
    message,
    metadata,
    CAST(json_extract(metadata, :extreme_source_path) AS REAL) AS source_value,
    json_type(metadata, :extreme_result_path) AS result_type,
    json(metadata -> :extreme_result_path) AS result_json
  FROM bounded
  WHERE json_type(metadata, :extreme_source_path) IN ('integer', 'real')
    AND ABS(CAST(json_extract(metadata, :extreme_source_path) AS REAL))
          <= 1.7976931348623157e308
), ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      ORDER BY source_value, ts, level, message, metadata
    ) AS minimum_rank,
    ROW_NUMBER() OVER (
      ORDER BY source_value DESC, ts, level, message, metadata
    ) AS maximum_rank
  FROM candidates
)
SELECT
  MAX(CASE WHEN minimum_rank = 1 THEN result_type END) AS minimum_type,
  MAX(CASE WHEN minimum_rank = 1 THEN result_json END) AS minimum_json,
  MAX(CASE WHEN maximum_rank = 1 THEN result_type END) AS maximum_type,
  MAX(CASE WHEN maximum_rank = 1 THEN result_json END) AS maximum_json
FROM ranked
WHERE :max_result_rows > 0
LIMIT :max_result_rows;
```

`:extreme_source_path` is the comparison path and `:extreme_result_path` is
the companion result path. Only native JSON integers/reals participate in
this SQL foundation; missing, null, strings, booleans, arrays, objects, and
nonfinite REAL casts are ignored. Ties select the first explicit public-row
identity. A missing result has SQL NULL type/JSON columns, while an explicit
JSON null has `minimum_type`/`maximum_type` equal to `null` and JSON text
`null`. Empty input returns one all-NULL aggregate row. JSON result text is
lossless; core SQLite's dynamic scalar types alone cannot distinguish every
retained JSON state.

The complete `LQL-S10` API owns case-insensitive `any`, `field_min`, and
`field_max` grammar; exact canonical or nested fields; deterministic current-
pipeline order for `any`; VictoriaLogs signed/unsigned/timestamp/math/natural
text ordering for extrema; native rich companion values; stable empty
results; strict work/state/result/response limits; deadline cancellation; and
HTTP envelopes. It does not narrow extrema to native numbers or cast integers
through binary64. These SQL statements are therefore honest public-row
foundations, not a claim that SQLite's default collation implements the full
LogsQL comparator.

Every candidate already crosses the bounded public `logs` interface. The
selection and companion lookup do not avoid a block read, decode, allocation,
or row crossing when moved into an extension opcode, so no new primitive,
private shadow-table access, or storage-format change is warranted.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_any_and_field_extrema_preserve_rich_rows_and_reopen`.

### SQL-LOG-047: deterministic rich row selection and numeric row extrema

For deterministic `row_any`, bind two exact SQLite JSON paths, inclusive
native timestamp bounds, and positive work/result limits. This read-only
statement finds the first public row where either selected value is nonempty,
then returns both existing values from that same row as one compact, typed
JSON object:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), typed AS (
  SELECT
    ts,
    level,
    message,
    metadata,
    json_type(metadata, :row_any_path_1) AS type_1,
    json_type(metadata, :row_any_path_2) AS type_2
  FROM bounded
), eligible AS (
  SELECT
    *,
    CASE
      WHEN type_1 IS NULL THEN 0
      WHEN type_1 = 'null' THEN 0
      WHEN type_1 = 'text' THEN json_extract(metadata, :row_any_path_1) <> ''
      ELSE 1
    END OR CASE
      WHEN type_2 IS NULL THEN 0
      WHEN type_2 = 'null' THEN 0
      WHEN type_2 = 'text' THEN json_extract(metadata, :row_any_path_2) <> ''
      ELSE 1
    END AS qualifies,
    CASE
      WHEN type_1 IS NULL THEN json('{}')
      ELSE json_set(
        json('{}'),
        :row_any_path_1,
        json(metadata -> :row_any_path_1)
      )
    END AS selected_1
  FROM typed
), selected AS (
  SELECT
    ts,
    level,
    message,
    metadata,
    CASE
      WHEN type_2 IS NULL THEN selected_1
      ELSE json_set(
        selected_1,
        :row_any_path_2,
        json(metadata -> :row_any_path_2)
      )
    END AS selected_json
  FROM eligible
  WHERE qualifies
)
SELECT selected_json
FROM selected
WHERE :max_result_rows > 0
ORDER BY ts, level, message, metadata
LIMIT MIN(:max_result_rows, 1);
```

`:row_any_path_1` and `:row_any_path_2` are exact paths such as `$.host`,
`$.nested.ok`, or `$.payload`. A selected missing path is omitted; selected
JSON null, empty string, false, zero, arrays, and objects retain their native
JSON types in the result. Missing, null, and empty strings do not make a row
qualify, while false, zero, arrays, and objects do. If no row qualifies, the
statement returns no row. Explicit public-row ordering makes the selection
stable instead of copying VictoriaLogs' merge-order-dependent `row_any`
choice.

For `row_min` and `row_max` over a finite native-number comparison field,
bind one exact comparison path, two exact result paths, and the same bounds.
This statement returns the two selected rich objects in one row:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
), typed AS (
  SELECT
    ts,
    level,
    message,
    metadata,
    CAST(json_extract(metadata, :row_extreme_source_path) AS REAL) AS source_value,
    json_type(metadata, :row_result_path_1) AS result_type_1,
    json_type(metadata, :row_result_path_2) AS result_type_2
  FROM bounded
  WHERE json_type(metadata, :row_extreme_source_path) IN ('integer', 'real')
    AND ABS(CAST(json_extract(metadata, :row_extreme_source_path) AS REAL))
          <= 1.7976931348623157e308
), selected_1 AS (
  SELECT
    *,
    CASE
      WHEN result_type_1 IS NULL THEN json('{}')
      ELSE json_set(
        json('{}'),
        :row_result_path_1,
        json(metadata -> :row_result_path_1)
      )
    END AS selected_json_1
  FROM typed
), candidates AS MATERIALIZED (
  SELECT
    ts,
    level,
    message,
    metadata,
    source_value,
    CASE
      WHEN result_type_2 IS NULL THEN selected_json_1
      ELSE json_set(
        selected_json_1,
        :row_result_path_2,
        json(metadata -> :row_result_path_2)
      )
    END AS selected_json
  FROM selected_1
)
SELECT
  COALESCE((
    SELECT selected_json
    FROM candidates
    ORDER BY source_value, ts, level, message, metadata
    LIMIT 1
  ), '{}') AS minimum_row_json,
  COALESCE((
    SELECT selected_json
    FROM candidates
    ORDER BY source_value DESC, ts, level, message, metadata
    LIMIT 1
  ), '{}') AS maximum_row_json
WHERE :max_result_rows > 0
LIMIT :max_result_rows;
```

`:row_extreme_source_path` is the comparison path. `:row_result_path_1` and
`:row_result_path_2` select the result object. Only native JSON integers/reals
participate in this SQL foundation; missing, null, strings, booleans, arrays,
objects, and nonfinite REAL casts are ignored. Selected missing paths are
omitted, while every existing selected value keeps its JSON type. Ties select
the first explicit public-row identity. Empty input returns one row containing
`{}` for both extrema.

The complete `LQL-S11` API owns case-insensitive `row_any`, `row_min`, and
`row_max` grammar; optional `as` and implicit result aliases; exact, prefix,
all-current-field, canonical, and nested selectors; deterministic current-
pipeline order; VictoriaLogs signed/unsigned/timestamp/math/natural text
ordering; strict first-tie behavior; native rich object results; empty `{}`
results; strict work/state/result/response limits; cooperative deadline
cancellation; and HTTP envelopes. It does not narrow extrema to native
numbers or cast integers through binary64. These statements are honest fixed-
path public-row foundations, not claims that ordinary SQLite implements the
dynamic language surface.

Every selected row already crosses the bounded public `logs` interface. Row
selection does not avoid a block read, decode, allocation, or row crossing
when moved into an extension opcode, so no new primitive, private shadow-table
access, or storage-format change is warranted.

Direct regression: `tests/cli.sh` section 45 and the Rust SQL harness;
HTTP/oracle/optimize/reopen regression:
`session_seventeen_row_selection_stats_are_rich_bounded_and_durable`.

### SQL-LOG-048: query-backed exact membership

Materialize one exact retained-string field from a bounded public subquery,
then apply its distinct values to a separately bounded public outer scan:

```sql
WITH query_values AS MATERIALIZED (
  SELECT DISTINCT
    CASE
      WHEN json_type(metadata, :subquery_output_path) IS NULL THEN ''
      WHEN json_type(metadata, :subquery_output_path) = 'null' THEN ''
      WHEN json_type(metadata, :subquery_output_path) = 'text'
        THEN json_extract(metadata, :subquery_output_path)
      ELSE NULL
    END AS value
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND level = :subquery_level
    AND max_work_entries = :subquery_max_work_entries
), outer_bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ms
    AND ts <= :end_ms
    AND max_work_entries = :outer_max_work_entries
), projected AS (
  SELECT
    ts,
    level,
    message,
    metadata,
    CASE
      WHEN json_type(metadata, :outer_field_path) IS NULL THEN ''
      WHEN json_type(metadata, :outer_field_path) = 'null' THEN ''
      WHEN json_type(metadata, :outer_field_path) = 'text'
        THEN json_extract(metadata, :outer_field_path)
      ELSE NULL
    END AS field_text
  FROM outer_bounded
)
SELECT ts, level, message, metadata
FROM projected
WHERE field_text COLLATE BINARY IN (
  SELECT value COLLATE BINARY
  FROM query_values
  WHERE value IS NOT NULL
)
ORDER BY ts DESC
LIMIT :limit;
```

For the copyable fixture, bind `:subquery_output_path` and
`:outer_field_path` to `$.deployment.region`, `:subquery_level` to `error`,
inclusive `:start_ms`/`:end_ms` to `1000`/`2000`, and positive work limits.
The statement returns the `ts=1000` row because the subquery produces the
single value `us-east`. Timestamps use the virtual table's configured native
unit (milliseconds in this fixture). Both scans are independently bounded;
the caller must ensure
`:subquery_max_work_entries + :outer_max_work_entries` does not exceed its
cumulative request budget. `ORDER BY ts DESC` and `LIMIT` make the returned
row order and cardinality explicit.

This statement is exact for retained strings. Missing fields and JSON null
project to the same empty string as LogsQL, while non-string rich values are
excluded instead of being formatted with SQLite-dependent rules. An empty
subquery therefore makes `IN` false. The Rust Logs API additionally
compact-projects numbers, booleans, arrays, and objects with the same
canonical serializer used by current-row predicates; requires the subquery
to end in exactly one `fields`/`keep` or `uniq` field; removes the ordinary
100-row response default; and deduplicates the complete bounded result.

`contains_any(subquery)` and `contains_all(subquery)` share this public
two-scan value-materialization foundation, but their complete case-sensitive
phrase and Unicode word-boundary behavior has no honest portable core-SQL
predicate. The Rust API owns those predicates, nested query-backed lists,
one request timestamp, eight-level nesting and 32-list parse limits,
request-local equivalent-subquery caching, cumulative work and state limits,
cooperative cancellation, error/result envelopes, and empty-list identities.
It executes each subquery and the outer query through the public `logs`
row/pipeline contract; it does not expose LogsQL to SQLite, open a nested
virtual-table cursor, or read a private shadow table.

Both sides already require their own bounded public scan, so moving this
composition into the extension would not eliminate a block read, decode,
allocation, or row crossing. No extension primitive or storage-format change
is warranted. Direct regression: `tests/cli.sh` section 45 and the Rust SQL
harness; HTTP/oracle/optimize/flush/reopen regression:
`session_eighteen_query_backed_lists_are_rich_cumulative_cached_and_reopenable`.

### SQL-LOG-049: bounded random log sample

Bind a positive signed 64-bit `:sample`, inclusive native timestamp bounds,
and positive work/result limits. This read-only statement returns an
independent random sample of bounded public log rows with probability `1/N`
per row:

```sql
WITH bounded AS MATERIALIZED (
  SELECT ts, level, message, metadata
  FROM logs
  WHERE ts >= :start_ts
    AND ts <= :end_ts
    AND max_work_entries = :max_work_entries
)
SELECT ts, level, message, metadata
FROM bounded
WHERE :sample > 0
  AND (random() % :sample) = 0
ORDER BY ts, level, message, metadata
LIMIT :max_result_rows;
```

For the copyable fixture, bind `:start_ts`/`:end_ts` to `1000`/`2000`,
`:max_work_entries` to `100000`, `:max_result_rows` to `100`, and `:sample`
to `1`; both fixture rows are returned in explicit ascending order. Values
greater than one select each bounded row independently with probability
approximately `1/:sample`. SQLite's signed remainder is safe here: negative
random values still have remainder zero exactly when divisible, without the
`abs(INT64_MIN)` overflow edge. A caller must reject `:sample <= 0`; the
defensive predicate returns no rows for such a binding. The virtual table's
configured timestamp unit is authoritative.

The complete `LQL-P17` API accepts VictoriaLogs positive unsigned syntax,
including quoted, base-zero, byte-size, duration, and infinite spellings. It
uses VictoriaLogs-compatible exponentially distributed skip lengths, keeps
selected rows and their native rich values unchanged, preserves input order,
composes at the current pipeline position, and enforces request work, result,
response, deadline, and cancellation limits. The SQL statement uses ordinary
SQLite independent Bernoulli draws instead; it implements the public `1/N`
random-sample contract but deliberately does not claim the same private RNG
sequence or gap distribution. SQLite parameters are signed, so the direct
recipe supports `1..=9223372036854775807`; larger Rust API values are valid
but practically select no bounded row.

Sampling happens after the required bounded public scan. Moving its language
syntax or RNG into the extension would not eliminate a block read, decode, or
row crossing for direct users, while ordinary SQL already supplies a bounded
random predicate. No extension primitive, private shadow-table access, or
storage-format change is warranted. Direct regression: `tests/cli.sh` section
45 and the Rust SQL harness; HTTP/oracle/optimize/flush/reopen regression:
`session_eighteen_sample_is_random_bounded_durable_and_row_preserving`.

## Adding the next recipe

When a matrix row uses `SQL` as its target or foundation:

1. add its stable row ID to the recipe index;
2. include an executable statement and setup;
3. describe exact and non-equivalent language behavior;
4. add the statement to the Rust `timeless-query-harness` with hand-verified
   semantic output; and
5. link the recipe from the matrix row before changing its status to
   `shipped`.

If the statement needs a private shadow table, application callback, or
undocumented decoder, it is not a valid SQL equivalent.
