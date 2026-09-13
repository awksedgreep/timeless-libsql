use super::*;

#[derive(Clone, Debug)]
pub(crate) enum NativeRequest {
    Latest {
        metric: String,
        filter: FilterPlan,
        stop: i64,
    },
    Export {
        metric: String,
        filter: FilterPlan,
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
            .map_err(|error| format!("prepare bounded catalog: {error}"))?;
        let mut rows = statement
            .query(params![
                metric,
                filter.map(|filter| &filter.pushdown_json),
                self.limits.max_work_points as i64,
                self.limits.max_response_bytes as i64
            ])
            .map_err(|error| format!("query bounded catalog: {error}"))?;
        let mut catalog = Vec::new();
        let mut bytes = 0_usize;
        while let Some(row) = rows
            .next()
            .map_err(|error| format!("read bounded catalog: {error}"))?
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
            if bytes > self.limits.max_response_bytes {
                return Err(format!(
                    "query exceeded the maximum catalog-size limit of {} bytes",
                    self.limits.max_response_bytes
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
            body: body.bytes,
            frame_bytes,
            series: series as u64,
            points: points as u64,
            intermediate_points: 0,
            rows: rows as u64,
        })
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
            start,
            stop,
        } => export(&context, &metric, &filter, start, stop),
        NativeRequest::Range {
            metric,
            filter,
            start,
            stop,
            step,
            aggregate,
        } => range(&context, &metric, &filter, start, stop, step, aggregate),
        NativeRequest::Labels { selectors } => {
            let catalog = context.selected(None, &selectors)?;
            let mut names = BTreeSet::from(["__name__".to_string()]);
            for meta in &catalog {
                context.check()?;
                for name in meta.labels.keys() {
                    names.insert(name.clone());
                    context.result_points(names.len() as u128)?;
                }
            }
            context.strings(names, catalog.len())
        }
        NativeRequest::LabelValues {
            name,
            metric,
            selectors,
        } => {
            let catalog = context.selected(metric.as_deref(), &selectors)?;
            let mut values = BTreeSet::new();
            for meta in &catalog {
                context.check()?;
                if let Some(value) = if name == "__name__" {
                    Some(&meta.metric)
                } else {
                    meta.labels.get(&name)
                } {
                    values.insert(value.clone());
                    context.result_points(values.len() as u128)?;
                }
            }
            context.strings(
                values,
                if selectors.is_empty() {
                    0
                } else {
                    catalog.len()
                },
            )
        }
        NativeRequest::Series { metric, selectors } => {
            let catalog = context.selected(metric.as_deref(), &selectors)?;
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
                context.limits.max_work_points as i64
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

fn export(
    context: &Context<'_>,
    metric: &str,
    filter: &FilterPlan,
    start: i64,
    stop: i64,
) -> Result<ReadOutput, String> {
    let catalog = context.catalog(Some(metric), Some(filter))?;
    let raw = raw_query(
        context.conn,
        context.features,
        metric,
        filter,
        start,
        stop,
        Some(context.limits.max_work_points),
    )?;
    let by_id: HashMap<_, _> = raw
        .series
        .iter()
        .map(|series| (series.id, series))
        .collect();
    let mut body = Body::new(context.limits.max_response_bytes);
    let mut points = 0;
    let mut emitted = 0;
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
        labels.insert("__name__".into(), metric.to_owned());
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
    context.output(body, emitted, points, points, raw.frame_bytes)
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
                    context.limits.max_work_points as i64
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
            Some(context.limits.max_work_points),
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
