use super::*;

#[derive(Clone, Debug)]
pub(crate) enum NativeRequest {
    Latest {
        metric: String,
        filter: FilterPlan,
        stop: i64,
    },
    Export {
        metric: Option<String>,
        filter: FilterPlan,
        selectors: Vec<Selector>,
        start: i64,
        stop: i64,
    },
    Range {
        metric: String,
        filter: FilterPlan,
        start: i64,
        stop: i64,
        step: i64,
        aggregate: Aggregate,
    },
    Labels {
        selectors: Vec<Selector>,
    },
    LabelValues {
        name: String,
        metric: Option<String>,
        selectors: Vec<Selector>,
    },
    Series {
        metric: Option<String>,
        selectors: Vec<Selector>,
        window: Option<(i64, i64)>,
    },
}

impl NativeRequest {
    pub(super) fn kind(&self) -> ReadKind {
        match self {
            Self::Latest { .. } => ReadKind::Latest,
            Self::Export { .. } => ReadKind::Export,
            Self::Range { .. } => ReadKind::Range,
            _ => ReadKind::Discovery,
        }
    }

    pub(super) fn validate(&self, limits: PromQueryLimits) -> Result<(), String> {
        if let Self::Range {
            start, stop, step, ..
        } = self
        {
            let count = grid_points(*start, *stop, *step)?;
            if count > limits.max_points_per_series as u128 {
                return Err(format!(
                    "query exceeded the maximum of {} points per series; increase step",
                    limits.max_points_per_series
                ));
            }
        }
        Ok(())
    }
}

fn grid_points(start: i64, stop: i64, step: i64) -> Result<u128, String> {
    if step <= 0 {
        return Err("step must be positive".into());
    }
    Ok(if stop < start {
        0
    } else {
        ((i128::from(stop) - i128::from(start)) / i128::from(step) + 1) as u128
    })
}

struct Body {
    bytes: Vec<u8>,
    limit: usize,
}

impl Body {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
    fn text(&mut self, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(format!(
                "query exceeded the maximum response-size limit of {} bytes",
                self.limit
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn json<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), String> {
        write_json_bounded(&mut self.bytes, value, self.limit)
    }
    fn comma(&mut self, index: usize) -> Result<(), String> {
        if index > 0 {
            self.text(b",")?;
        }
        Ok(())
    }
}

struct Context<'a> {
    conn: &'a Connection,
    features: QueryFeatures,
    limits: PromQueryLimits,
    cancelled: &'a AtomicBool,
}

impl Context<'_> {
    /// Catalog extrema are actual sample timestamps, so most recent-window
    /// discovery needs no payload reads. A window strictly inside a series'
    /// extent needs an exact existence probe (the extent may contain gaps).
    fn series_in_window(
        &self,
        catalog: Vec<SeriesMeta>,
        start: i64,
        stop: i64,
    ) -> Result<Vec<SeriesMeta>, String> {
        if start > stop {
            return Ok(Vec::new());
        }
        let mut overview = self
            .conn
            .prepare(&format!(
                "SELECT min_ts,max_ts,points FROM timeless_series('{}') WHERE series_id=?1",
                self.features.table.name()
            ))
            .map_err(|error| format!("prepare series window catalog: {error}"))?;
        let mut probe = None;
        let mut remaining = self.limits.max_storage_points as u64;
        let mut selected = Vec::new();
        for meta in catalog {
            self.check()?;
            let (min, max, points): (Option<i64>, Option<i64>, i64) = overview
                .query_row([meta.id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .map_err(|error| format!("read series window catalog: {error}"))?;
            let points = u64::try_from(points)
                .map_err(|_| "invalid negative series point count".to_string())?;
            let (Some(min), Some(max)) = (min, max) else {
                continue;
            };
            if max < start || min > stop {
                continue;
            }
            if min >= start || max <= stop {
                selected.push(meta);
                continue;
            }
            if !self.features.latest_work_limit {
                return Err("incompatible extension: bounded timeless_latest capability is required for historical series discovery".into());
            }
            // Conservatively reserve the series' full point count across all
            // probes. This bounds the whole request without racy cumulative
            // counter deltas or charging discarded samples to work points.
            if points > remaining {
                return Err(format!("series discovery storage point limit {} exceeded while checking historical gaps", self.limits.max_storage_points));
            }
            remaining -= points;
            if probe.is_none() {
                probe = Some(
                    self.conn
                        .prepare(&format!(
                    "SELECT 1 FROM timeless_latest('{}',?1,NULL,?2,?3,?4) WHERE series_id=?5",
                    self.features.table.name()
                ))
                        .map_err(|error| format!("prepare series window probe: {error}"))?,
                );
            }
            let exists = probe
                .as_mut()
                .unwrap()
                .query_row(
                    params![meta.metric, start, stop, points.max(1) as i64, meta.id],
                    |_| Ok(()),
                )
                .optional()
                .map_err(|error| format!("query series window probe: {error}"))?
                .is_some();
            if exists {
                selected.push(meta);
            }
        }
        Ok(selected)
    }

    fn check(&self) -> Result<(), String> {
        check_cancelled(self.cancelled)
    }
    fn result_points(&self, count: u128) -> Result<(), String> {
        if count > self.limits.max_result_points as u128 {
            return Err(format!(
                "query exceeded the maximum result-point limit of {}",
                self.limits.max_result_points
            ));
        }
        Ok(())
    }
    fn catalog(
        &self,
        metric: Option<&str>,
        filter: Option<&FilterPlan>,
    ) -> Result<Vec<SeriesMeta>, String> {
        if !self.features.catalog_work_limit {
            return Err(
                "incompatible extension: bounded timeless_series catalog capability is required"
                    .into(),
            );
        }
        let mut statement = self
            .conn
            .prepare(&format!(
                "SELECT series_id,name,labels FROM timeless_series('{}',?1,?2,?3,?4)",
                self.features.table.name()
            ))
            .map_err(|error| super::catalog_error("prepare bounded catalog", error))?;
        let mut rows = statement
            .query(params![
                metric,
                filter.map(|filter| &filter.pushdown_json),
                crate::catalog_limit_sql(self.limits.max_catalog_series),
                crate::catalog_limit_sql(self.limits.max_catalog_bytes)
            ])
            .map_err(|error| super::catalog_error("query bounded catalog", error))?;
        let mut catalog = Vec::new();
        let mut bytes = 0_usize;
        while let Some(row) = rows
            .next()
            .map_err(|error| super::catalog_error("read bounded catalog", error))?
        {
            self.check()?;
            // Borrow TEXT before allocating or decoding escaped label JSON.
            let metric = row
                .get_ref(1)
                .and_then(|v| v.as_str().map_err(Into::into))
                .map_err(|error| format!("read catalog metric: {error}"))?;
            let labels = row
                .get_ref(2)
                .and_then(|v| v.as_str().map_err(Into::into))
                .map_err(|error| format!("read catalog labels: {error}"))?;
            bytes = bytes
                .checked_add(metric.len())
                .and_then(|n| n.checked_add(labels.len()))
                .ok_or_else(|| "catalog byte accounting overflow".to_string())?;
            if self.limits.max_catalog_bytes != 0 && bytes > self.limits.max_catalog_bytes {
                return Err(format!(
                    "query exceeded the maximum catalog-size limit of {} bytes; \
                     raise TIMELESS_METRICS_PROMQL_MAX_CATALOG_BYTES (0 disables the \
                     ceiling), or narrow the metric selector",
                    self.limits.max_catalog_bytes
                ));
            }
            let decoded = decode_labels(labels)?;
            if filter.is_none_or(|filter| filter.matches(&decoded)) {
                catalog.push(SeriesMeta {
                    id: row
                        .get(0)
                        .map_err(|error| format!("read catalog id: {error}"))?,
                    metric: metric.to_owned(),
                    labels_json: labels.to_owned(),
                    labels: decoded,
                });
            }
        }
        catalog.sort_by(|a, b| {
            (&a.metric, &a.labels_json, a.id).cmp(&(&b.metric, &b.labels_json, b.id))
        });
        Ok(catalog)
    }

    fn selected(
        &self,
        metric: Option<&str>,
        selectors: &[Selector],
    ) -> Result<Vec<SeriesMeta>, String> {
        if selectors.is_empty() {
            return self.catalog(metric, None);
        }
        let (metric, filter) = if let [selector] = selectors {
            match &selector.metric {
                MetricSelection::Exact(metric) => (Some(metric.as_str()), Some(&selector.filter)),
                _ => (None, None),
            }
        } else {
            (None, None)
        };
        // Multiple selectors inspect one shared catalog, so repeated selectors
        // cannot multiply the per-request work allowance.
        let catalog = self.catalog(metric, filter)?;
        let mut selected = Vec::new();
        let mut evaluations = 0_usize;
        for meta in catalog {
            for selector in selectors {
                self.check()?;
                if evaluations == self.limits.max_work_points {
                    return Err(format!(
                        "query exceeded the maximum selector-work limit of {}",
                        self.limits.max_work_points
                    ));
                }
                evaluations += 1;
                let metric = match &selector.metric {
                    MetricSelection::Exact(name) => name == &meta.metric,
                    MetricSelection::Regex(regex) => regex.is_match(&meta.metric),
                    MetricSelection::Matchers(matchers) => matchers
                        .iter()
                        .all(|matcher| matcher.matches_value(&meta.metric)),
                    MetricSelection::All => true,
                };
                if metric && selector.filter.matches(&meta.labels) {
                    selected.push(meta);
                    break;
                }
            }
        }
        Ok(selected)
    }

    /// Stream the selected catalog through `visit` without collecting it.
    /// Discovery routes that only accumulate distinct names or values use this
    /// so a high-cardinality store never builds a `Vec<SeriesMeta>`. Matches
    /// [`Self::selected`]; rows are visited in the extension's
    /// `(metric_name, series_id)` order.
    fn for_each_selected(
        &self,
        metric: Option<&str>,
        selectors: &[Selector],
        mut visit: impl FnMut(SeriesMeta) -> Result<(), String>,
    ) -> Result<usize, String> {
        if !self.features.catalog_work_limit {
            return Err(
                "incompatible extension: bounded timeless_series catalog capability is required"
                    .into(),
            );
        }
        let (read_metric, read_filter) = if selectors.is_empty() {
            (metric, None)
        } else if let [selector] = selectors {
            match &selector.metric {
                MetricSelection::Exact(exact) => (Some(exact.as_str()), Some(&selector.filter)),
                _ => (None, None),
            }
        } else {
            (None, None)
        };
        let mut statement = self
            .conn
            .prepare(&format!(
                "SELECT series_id,name,labels FROM timeless_series('{}',?1,?2,?3,?4)",
                self.features.table.name()
            ))
            .map_err(|error| super::catalog_error("prepare bounded catalog", error))?;
        let mut rows = statement
            .query(params![
                read_metric,
                read_filter.map(|filter| &filter.pushdown_json),
                crate::catalog_limit_sql(self.limits.max_catalog_series),
                crate::catalog_limit_sql(self.limits.max_catalog_bytes)
            ])
            .map_err(|error| super::catalog_error("query bounded catalog", error))?;
        let mut bytes = 0_usize;
        let mut evaluations = 0_usize;
        let mut emitted = 0_usize;
        while let Some(row) = rows
            .next()
            .map_err(|error| super::catalog_error("read bounded catalog", error))?
        {
            self.check()?;
            let metric_name = row
                .get_ref(1)
                .and_then(|value| value.as_str().map_err(Into::into))
                .map_err(|error| format!("read catalog metric: {error}"))?;
            let labels_json = row
                .get_ref(2)
                .and_then(|value| value.as_str().map_err(Into::into))
                .map_err(|error| format!("read catalog labels: {error}"))?;
            bytes = bytes
                .checked_add(metric_name.len())
                .and_then(|total| total.checked_add(labels_json.len()))
                .ok_or_else(|| "catalog byte accounting overflow".to_string())?;
            if self.limits.max_catalog_bytes != 0 && bytes > self.limits.max_catalog_bytes {
                return Err(format!(
                    "query exceeded the maximum catalog-size limit of {} bytes; \
                     raise TIMELESS_METRICS_PROMQL_MAX_CATALOG_BYTES (0 disables the \
                     ceiling), or narrow the metric selector",
                    self.limits.max_catalog_bytes
                ));
            }
            let labels = decode_labels(labels_json)?;
            let matched = if selectors.is_empty() {
                read_filter.is_none_or(|filter| filter.matches(&labels))
            } else {
                let mut hit = false;
                for selector in selectors {
                    if evaluations == self.limits.max_work_points {
                        return Err(format!(
                            "query exceeded the maximum selector-work limit of {}",
                            self.limits.max_work_points
                        ));
                    }
                    evaluations += 1;
                    let metric_matches = match &selector.metric {
                        MetricSelection::Exact(name) => name == metric_name,
                        MetricSelection::Regex(regex) => regex.is_match(metric_name),
                        MetricSelection::Matchers(matchers) => matchers
                            .iter()
                            .all(|matcher| matcher.matches_value(metric_name)),
                        MetricSelection::All => true,
                    };
                    if metric_matches && selector.filter.matches(&labels) {
                        hit = true;
                        break;
                    }
                }
                hit
            };
            if matched {
                visit(SeriesMeta {
                    id: row
                        .get(0)
                        .map_err(|error| format!("read catalog id: {error}"))?,
                    metric: metric_name.to_owned(),
                    labels_json: labels_json.to_owned(),
                    labels,
                })?;
                emitted += 1;
            }
        }
        Ok(emitted)
    }

    fn output(
        &self,
        body: Body,
        series: usize,
        points: usize,
        rows: usize,
        frame_bytes: usize,
    ) -> Result<ReadOutput, String> {
        self.result_points(rows as u128)?;
        Ok(ReadOutput {
            sum_profile: InstantSumProfile::default(),
            body: body.bytes,
            frame_bytes,
            series: series as u64,
            points: points as u64,
            intermediate_points: 0,
            rows: rows as u64,
        })
    }

    /// Selector-less discovery from the extension's label index: the
    /// distinct names or values, without streaming the series catalog.
    fn label_index_rows(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<BTreeSet<String>, String> {
        let mut statement = self
            .conn
            .prepare(sql)
            .map_err(|error| super::catalog_error("prepare label discovery", error))?;
        let rows = statement
            .query_map(params, |row| row.get::<_, String>(0))
            .map_err(|error| super::catalog_error("read label discovery", error))?;
        let mut values = BTreeSet::new();
        for row in rows {
            self.check()?;
            values
                .insert(row.map_err(|error| super::catalog_error("read label discovery", error))?);
            self.result_points(values.len() as u128)?;
        }
        Ok(values)
    }

    fn strings(&self, values: BTreeSet<String>, series: usize) -> Result<ReadOutput, String> {
        self.result_points(values.len() as u128)?;
        let mut body = Body::new(self.limits.max_response_bytes);
        body.text(br#"{"status":"success","data":["#)?;
        for (index, value) in values.iter().enumerate() {
            self.check()?;
            body.comma(index)?;
            body.json(value)?;
        }
        body.text(b"]}")?;
        self.output(body, series, 0, values.len(), 0)
    }
}

pub(super) fn execute(
    conn: &Connection,
    features: QueryFeatures,
    request: NativeRequest,
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    request.validate(limits)?;
    let context = Context {
        conn,
        features,
        limits,
        cancelled,
    };
    match request {
        NativeRequest::Latest {
            metric,
            filter,
            stop,
        } => latest(&context, &metric, &filter, stop),
        NativeRequest::Export {
            metric,
            filter,
            selectors,
            start,
            stop,
        } => export(
            &context,
            metric.as_deref(),
            &filter,
            &selectors,
            start,
            stop,
        ),
        NativeRequest::Range {
            metric,
            filter,
            start,
            stop,
            step,
            aggregate,
        } => range(&context, &metric, &filter, start, stop, step, aggregate),
        NativeRequest::Labels { selectors }
            if selectors.is_empty() && context.features.label_index =>
        {
            let names = context.label_index_rows(
                &format!(
                    "SELECT name FROM timeless_label_names('{}')",
                    context.features.table.name()
                ),
                rusqlite::params![],
            )?;
            context.strings(names, 0)
        }
        NativeRequest::Labels { selectors } => {
            let mut names = BTreeSet::from(["__name__".to_string()]);
            let count = context.for_each_selected(None, &selectors, |meta| {
                for name in meta.labels.keys() {
                    names.insert(name.clone());
                    context.result_points(names.len() as u128)?;
                }
                Ok(())
            })?;
            context.strings(names, count)
        }
        NativeRequest::LabelValues {
            name,
            metric,
            selectors,
        } if selectors.is_empty() && context.features.label_index => {
            let values = context.label_index_rows(
                &format!(
                    "SELECT value FROM timeless_label_values('{}', ?1, ?2)",
                    context.features.table.name()
                ),
                rusqlite::params![metric, name],
            )?;
            context.strings(values, 0)
        }
        NativeRequest::LabelValues {
            name,
            metric,
            selectors,
        } => {
            let mut values = BTreeSet::new();
            let count = context.for_each_selected(metric.as_deref(), &selectors, |meta| {
                if let Some(value) = if name == "__name__" {
                    Some(&meta.metric)
                } else {
                    meta.labels.get(&name)
                } {
                    values.insert(value.clone());
                    context.result_points(values.len() as u128)?;
                }
                Ok(())
            })?;
            context.strings(values, if selectors.is_empty() { 0 } else { count })
        }
        NativeRequest::Series {
            metric,
            selectors,
            window,
        } => {
            let mut catalog = context.selected(metric.as_deref(), &selectors)?;
            if let Some((start, stop)) = window {
                catalog = context.series_in_window(catalog, start, stop)?;
            }
            context.result_points(catalog.len() as u128)?;
            let mut body = Body::new(limits.max_response_bytes);
            body.text(br#"{"status":"success","data":["#)?;
            for (index, meta) in catalog.iter().enumerate() {
                context.check()?;
                body.comma(index)?;
                if selectors.is_empty() {
                    body.text(br#"{"labels":"#)?;
                    body.text(meta.labels_json.as_bytes())?;
                    body.text(b"}")?;
                } else {
                    let mut labels = meta.labels.clone();
                    labels.insert("__name__".into(), meta.metric.clone());
                    body.json(&labels)?;
                }
            }
            body.text(b"]}")?;
            context.output(body, catalog.len(), 0, catalog.len(), 0)
        }
    }
}

fn latest(
    context: &Context<'_>,
    metric: &str,
    filter: &FilterPlan,
    stop: i64,
) -> Result<ReadOutput, String> {
    let catalog = context.catalog(Some(metric), Some(filter))?;
    if !context.features.latest_frame || !context.features.latest_frame_work_limit {
        return Err(
            "incompatible extension: bounded timeless_latest_frame capability is required".into(),
        );
    }
    let frame: Option<Vec<u8>> = context
        .conn
        .query_row(
            &format!(
                "SELECT frame FROM timeless_latest_frame('{}',?1,?2,0,?3,?4)",
                context.features.table.name()
            ),
            params![
                metric,
                filter.pushdown_json,
                stop,
                context.limits.max_storage_points as i64
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| format!("query bounded latest frame: {error}"))?;
    let frame_bytes = frame.as_ref().map_or(0, Vec::len);
    let mut rows = frame
        .map(|frame| decode_latest_frame(&frame))
        .transpose()?
        .unwrap_or_default();
    let by_id: HashMap<_, _> = catalog.iter().map(|meta| (meta.id, meta)).collect();
    rows.retain(|row| by_id.contains_key(&row.id));
    context.result_points(rows.len() as u128)?;
    rows.sort_by(|a, b| (&by_id[&a.id].labels_json, a.id).cmp(&(&by_id[&b.id].labels_json, b.id)));
    let mut body = Body::new(context.limits.max_response_bytes);
    if rows.len() != 1 {
        body.text(br#"{"data":["#)?;
    }
    for (index, row) in rows.iter().enumerate() {
        context.check()?;
        body.comma(index)?;
        body.json(&json!({ "labels": by_id[&row.id].labels, "timestamp": row.timestamp, "value": row.value }))?;
    }
    if rows.len() != 1 {
        body.text(b"]}")?;
    }
    context.output(body, rows.len(), rows.len(), rows.len(), frame_bytes)
}

/// Export raw samples as VictoriaMetrics JSON lines. `metric=` reads one
/// metric; `match[]` selectors (the Prometheus and VictoriaMetrics form) are
/// resolved against the bounded catalog first and read one metric at a time,
/// every selected series once however many selectors match it. Label
/// parameters narrow either form.
fn export(
    context: &Context<'_>,
    metric: Option<&str>,
    filter: &FilterPlan,
    selectors: &[Selector],
    start: i64,
    stop: i64,
) -> Result<ReadOutput, String> {
    let groups: Vec<(String, &FilterPlan, Vec<SeriesMeta>)> = if selectors.is_empty() {
        let metric = metric.ok_or_else(|| "missing required parameter: metric".to_string())?;
        vec![(
            metric.to_owned(),
            filter,
            context.catalog(Some(metric), Some(filter))?,
        )]
    } else {
        let mut by_metric: BTreeMap<String, Vec<SeriesMeta>> = BTreeMap::new();
        for meta in context.selected(None, selectors)? {
            if metric.is_some_and(|metric| metric != meta.metric) || !filter.matches(&meta.labels) {
                continue;
            }
            by_metric.entry(meta.metric.clone()).or_default().push(meta);
        }
        // One selector's own matchers narrow each read; with several, only
        // what they all share (the label parameters) can be pushed down.
        let pushdown = match selectors {
            [selector] => &selector.filter,
            _ => filter,
        };
        by_metric
            .into_iter()
            .map(|(metric, metas)| (metric, pushdown, metas))
            .collect()
    };
    let mut body = Body::new(context.limits.max_response_bytes);
    let mut points = 0;
    let mut emitted = 0;
    let mut frame_bytes = 0;
    for (metric, pushdown, catalog) in groups {
        // The storage-point allowance is the request's, not each metric's.
        let remaining = context.limits.max_storage_points.saturating_sub(points);
        let raw = raw_query(
            context.conn,
            context.features,
            &metric,
            pushdown,
            start,
            stop,
            Some(remaining),
        )?;
        frame_bytes += raw.frame_bytes;
        let by_id: HashMap<_, _> = raw
            .series
            .iter()
            .map(|series| (series.id, series))
            .collect();
        for meta in &catalog {
            context.check()?;
            let Some(series) = by_id.get(&meta.id) else {
                continue;
            };
            points += series.len();
            context.result_points(points as u128)?;
            if emitted > 0 {
                body.text(b"\n")?;
            }
            body.text(br#"{"metric":"#)?;
            let mut labels = meta.labels.clone();
            labels.insert("__name__".into(), metric.clone());
            body.json(&labels)?;
            body.text(br#","timestamps":["#)?;
            for index in 0..series.len() {
                context.check()?;
                body.comma(index)?;
                body.json(&(i128::from(series.timestamp(raw.frame.as_deref(), index)?) * 1_000))?;
            }
            body.text(br#"],"values":["#)?;
            for index in 0..series.len() {
                context.check()?;
                body.comma(index)?;
                body.json(&series.value(raw.frame.as_deref(), index)?)?;
            }
            body.text(b"]}")?;
            emitted += 1;
        }
    }
    context.output(body, emitted, points, points, frame_bytes)
}

fn range(
    context: &Context<'_>,
    metric: &str,
    filter: &FilterPlan,
    start: i64,
    stop: i64,
    step: i64,
    aggregate: Aggregate,
) -> Result<ReadOutput, String> {
    let catalog = context.catalog(Some(metric), Some(filter))?;
    let span = i128::from(stop) - i128::from(start) + 1;
    let use_window = context.features.window_batches
        && context.features.window_batch_work_limit
        && aggregate.native_name().is_some()
        && span > 0
        && span % i128::from(step) == 0;
    let mut body = Body::new(context.limits.max_response_bytes);
    body.text(br#"{"metric":"#)?;
    body.json(metric)?;
    body.text(br#","series":["#)?;
    let mut emitted = 0;
    let mut points = 0;
    let mut frame_bytes = 0;
    if use_window {
        let window_start = start
            .checked_add(step - 1)
            .ok_or_else(|| "range window start overflow".to_string())?;
        let mut statement = context.conn.prepare(&format!(
            "SELECT labels,buckets FROM timeless_window_batches('{}',?1,?2,?3,?4,?5,?5,?6,NULL,?7) ORDER BY labels,series_id", context.features.table.name()
        )).map_err(|error| format!("prepare bounded window batches: {error}"))?;
        let rows = statement
            .query_map(
                params![
                    metric,
                    filter.pushdown_json,
                    window_start,
                    stop,
                    step,
                    aggregate.native_name().unwrap(),
                    context.limits.max_storage_points as i64
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .map_err(|error| format!("query bounded window batches: {error}"))?;
        for row in rows {
            context.check()?;
            let (labels, buckets) =
                row.map_err(|error| format!("read bounded window batch: {error}"))?;
            if !filter.matches(&decode_labels(&labels)?) {
                continue;
            }
            let decoded = decode_window_batch(&buckets)?;
            points += decoded.len();
            context.result_points(points as u128)?;
            body.comma(emitted)?;
            body.text(br#"{"labels":"#)?;
            body.text(labels.as_bytes())?;
            body.text(br#","data":["#)?;
            for index in 0..decoded.len() {
                context.check()?;
                body.comma(index)?;
                body.text(b"[")?;
                body.json(&decoded.timestamp(index).saturating_sub(step - 1))?;
                body.text(b",")?;
                if aggregate == Aggregate::Count {
                    body.json(&decoded.value(index).map(|v| v as i64))?;
                } else {
                    body.json(&decoded.value(index))?;
                }
                body.text(b"]")?;
            }
            body.text(b"]}")?;
            emitted += 1;
            frame_bytes += buckets.len();
        }
    } else {
        let raw = raw_query(
            context.conn,
            context.features,
            metric,
            filter,
            start,
            stop,
            Some(context.limits.max_storage_points),
        )?;
        frame_bytes = raw.frame_bytes;
        let by_id: HashMap<_, _> = raw
            .series
            .iter()
            .map(|series| (series.id, series))
            .collect();
        for meta in &catalog {
            context.check()?;
            let Some(series) = by_id.get(&meta.id) else {
                continue;
            };
            let buckets = aggregate_raw(series, raw.frame.as_deref(), start, step, aggregate)?;
            points += buckets.len();
            context.result_points(points as u128)?;
            body.comma(emitted)?;
            body.text(br#"{"labels":"#)?;
            body.text(meta.labels_json.as_bytes())?;
            body.text(br#","data":["#)?;
            for (index, (timestamp, value)) in buckets.iter().enumerate() {
                context.check()?;
                body.comma(index)?;
                body.text(b"[")?;
                body.json(timestamp)?;
                body.text(b",")?;
                match value {
                    BucketValue::Integer(value) => body.json(value)?,
                    BucketValue::Real(value) => body.json(value)?,
                }
                body.text(b"]")?;
            }
            body.text(b"]}")?;
            emitted += 1;
        }
    }
    body.text(b"]}")?;
    context.output(body, emitted, points, points, frame_bytes)
}
