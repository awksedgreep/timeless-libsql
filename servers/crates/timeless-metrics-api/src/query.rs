use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use regex::Regex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::promql;
use crate::storage::MetricsTable;

const RESERVED_PARAMS: &[&str] = &[
    "metric",
    "metrics",
    "from",
    "to",
    "start",
    "end",
    "step",
    "aggregate",
    "width",
    "height",
    "label_key",
    "theme",
    "transform",
    "token",
    "forecast",
    "anomalies",
    "sensitivity",
    "horizon",
    "group_by",
    "cross_aggregate",
    "threshold_gt",
    "threshold_lt",
    "limit",
    "query",
    "time",
    "match[]",
    "match",
];

#[derive(Clone, Debug, Default)]
pub struct Params {
    pairs: Vec<(String, String)>,
}

impl Params {
    pub fn parse(query: Option<&str>, body: &[u8]) -> Self {
        let mut pairs = Vec::new();
        if let Some(query) = query {
            pairs.extend(
                form_urlencoded::parse(query.as_bytes())
                    .map(|(key, value)| (key.into_owned(), value.into_owned())),
            );
        }
        if !body.is_empty() {
            pairs.extend(
                form_urlencoded::parse(body)
                    .map(|(key, value)| (key.into_owned(), value.into_owned())),
            );
        }
        Self { pairs }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.pairs
            .iter()
            .rev()
            .find_map(|(candidate, value)| (candidate == key).then_some(value.as_str()))
    }

    pub fn all(&self, keys: &[&str]) -> Vec<String> {
        self.pairs
            .iter()
            .filter(|(key, _)| keys.contains(&key.as_str()))
            .map(|(_, value)| value.clone())
            .collect()
    }

    fn ensure_only(&self, allowed: &[&str]) -> Result<(), String> {
        let unknown = self
            .pairs
            .iter()
            .map(|(key, _)| key.as_str())
            .find(|key| !allowed.contains(key));
        match unknown {
            Some(key) => Err(format!("unsupported query parameter: {key}")),
            None => Ok(()),
        }
    }

    fn ensure_prometheus_only(&self, allowed: &[&str]) -> Result<(), String> {
        let unknown = self
            .pairs
            .iter()
            .map(|(key, _)| key.as_str())
            .find(|key| !allowed.contains(key));
        match unknown {
            Some(key) => Err(format!(
                "invalid parameter \"{key}\": unsupported query parameter"
            )),
            None => Ok(()),
        }
    }

    fn label_matchers(&self, extended: bool) -> Result<Vec<Matcher>, String> {
        let mut labels = BTreeMap::new();
        for (key, value) in &self.pairs {
            if RESERVED_PARAMS.contains(&key.as_str()) {
                continue;
            }
            labels.insert(key.clone(), value.clone());
        }
        labels
            .into_iter()
            .map(|(key, value)| {
                if extended && value.starts_with("=~") {
                    Matcher::new(key, MatcherOp::Regex, value[2..].to_string())
                } else {
                    Matcher::new(key, MatcherOp::Eq, value)
                }
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatcherOp {
    Eq,
    NotEq,
    Regex,
    NotRegex,
}

#[derive(Clone, Debug)]
struct Matcher {
    key: String,
    op: MatcherOp,
    value: String,
    regex: Option<Regex>,
}

impl Matcher {
    fn new(key: String, op: MatcherOp, value: String) -> Result<Self, String> {
        let regex = if matches!(op, MatcherOp::Regex | MatcherOp::NotRegex) {
            Some(
                Regex::new(&format!("^(?:{value})$"))
                    .map_err(|error| format!("invalid regex for {key}: {error}"))?,
            )
        } else {
            None
        };
        Ok(Self {
            key,
            op,
            value,
            regex,
        })
    }

    fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        let actual = labels.get(&self.key).map(String::as_str).unwrap_or("");
        match self.op {
            MatcherOp::Eq => actual == self.value,
            MatcherOp::NotEq => actual != self.value,
            MatcherOp::Regex => self
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(actual)),
            MatcherOp::NotRegex => self
                .regex
                .as_ref()
                .is_some_and(|regex| !regex.is_match(actual)),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FilterPlan {
    matchers: Vec<Matcher>,
    pushdown_json: String,
}

impl FilterPlan {
    fn new(matchers: Vec<Matcher>) -> Self {
        let mut pushed = HashSet::new();
        let mut object = Map::new();
        for matcher in &matchers {
            if matcher.key == "__name__" || !pushed.insert(matcher.key.clone()) {
                continue;
            }
            let value = match matcher.op {
                MatcherOp::Eq if matcher.value.is_empty() => json!({"re": ""}),
                MatcherOp::Eq => Value::String(matcher.value.clone()),
                MatcherOp::NotEq => json!({"neq": matcher.value}),
                MatcherOp::Regex => json!({"re": matcher.value}),
                MatcherOp::NotRegex => json!({"nre": matcher.value}),
            };
            object.insert(matcher.key.clone(), value);
        }
        Self {
            matchers,
            pushdown_json: Value::Object(object).to_string(),
        }
    }

    fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.matchers.iter().all(|matcher| matcher.matches(labels))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Aggregate {
    Avg,
    Min,
    Max,
    Sum,
    Count,
    Last,
    First,
    Rate,
}

impl Aggregate {
    fn parse(value: Option<&str>) -> Result<Self, String> {
        match value {
            None | Some("avg") => Ok(Self::Avg),
            Some("min") => Ok(Self::Min),
            Some("max") => Ok(Self::Max),
            Some("sum") => Ok(Self::Sum),
            Some("count") => Ok(Self::Count),
            Some("last") => Ok(Self::Last),
            Some("first") => Ok(Self::First),
            Some("rate") => Ok(Self::Rate),
            Some(value) => Err(format!("unsupported native aggregate: {value}")),
        }
    }

    fn native_name(self) -> Option<&'static str> {
        match self {
            Self::Avg => Some("avg"),
            Self::Min => Some("min"),
            Self::Max => Some("max"),
            Self::Sum => Some("sum"),
            Self::Count => Some("count"),
            Self::Last | Self::First | Self::Rate => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Selector {
    metric: MetricSelection,
    filter: FilterPlan,
}

#[derive(Clone, Debug)]
enum MetricSelection {
    Exact(String),
    Regex(Regex),
    All,
}

#[derive(Clone, Debug)]
pub(crate) enum ReadRequest {
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
    Prometheus {
        plan: PromPlan,
        start: i64,
        stop: i64,
        step: i64,
        instant: bool,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum PromPlan {
    Scalar(f64),
    String(String),
    Selector {
        metric: String,
        filter: FilterPlan,
        lookback: i64,
    },
    AvgOverTime {
        metric: String,
        filter: FilterPlan,
        window: i64,
    },
    RangeSelector {
        metric: String,
        filter: FilterPlan,
        window: i64,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ReadKind {
    Latest,
    Export,
    Range,
    Discovery,
    Promql,
}

impl ReadRequest {
    pub(crate) fn kind(&self) -> ReadKind {
        match self {
            Self::Latest { .. } => ReadKind::Latest,
            Self::Export { .. } => ReadKind::Export,
            Self::Range { .. } => ReadKind::Range,
            Self::Labels { .. } | Self::LabelValues { .. } | Self::Series { .. } => {
                ReadKind::Discovery
            }
            Self::Prometheus { .. } => ReadKind::Promql,
        }
    }
}

pub(crate) fn latest_request(params: &Params) -> Result<ReadRequest, String> {
    let metric = required_metric(params)?;
    Ok(ReadRequest::Latest {
        metric,
        filter: FilterPlan::new(params.label_matchers(false)?),
        stop: now_seconds(),
    })
}

pub(crate) fn export_request(params: &Params) -> Result<ReadRequest, String> {
    let metric = required_metric(params)?;
    let now = now_seconds();
    let start = parse_time(
        params.get("start"),
        parse_time(params.get("from"), now.saturating_sub(3_600)),
    );
    let stop = parse_time(params.get("end"), parse_time(params.get("to"), now));
    Ok(ReadRequest::Export {
        metric,
        filter: FilterPlan::new(params.label_matchers(false)?),
        start,
        stop,
    })
}

pub(crate) fn range_request(params: &Params) -> Result<ReadRequest, String> {
    for unsupported in [
        "metrics",
        "group_by",
        "cross_aggregate",
        "transform",
        "threshold_gt",
        "threshold_lt",
        "limit",
    ] {
        if params.get(unsupported).is_some() {
            return Err(format!(
                "{unsupported} is outside the Session 3 native range slice"
            ));
        }
    }
    let metric = required_metric(params)?;
    let now = now_seconds();
    let start = parse_time(
        params.get("start"),
        parse_time(params.get("from"), now.saturating_sub(3_600)),
    );
    let stop = parse_time(params.get("end"), parse_time(params.get("to"), now));
    let step = parse_integer(params.get("step")).unwrap_or(60);
    if step <= 0 {
        return Err("step must be positive".into());
    }
    Ok(ReadRequest::Range {
        metric,
        filter: FilterPlan::new(params.label_matchers(true)?),
        start,
        stop,
        step,
        aggregate: Aggregate::parse(params.get("aggregate"))?,
    })
}

pub(crate) fn prometheus_instant_request(params: &Params) -> Result<ReadRequest, String> {
    params.ensure_prometheus_only(&["query", "time", "lookback_delta"])?;
    let query = params.get("query").unwrap_or("");
    let time = match params.get("time") {
        Some(value) => parse_prom_time(Some(value), 0).map_err(|_| {
            format!(
                "invalid parameter \"time\": invalid time value for 'time': cannot parse \"{value}\" to a valid timestamp"
            )
        })?,
        None => now_millis(),
    };
    let lookback = parse_prom_request_lookback(params.get("lookback_delta"), 300_000)?;
    Ok(ReadRequest::Prometheus {
        plan: lower_promql(query, lookback)
            .map_err(|error| format!("invalid parameter \"query\": {error}"))?,
        start: time,
        stop: time,
        step: 1_000,
        instant: true,
    })
}

pub(crate) fn prometheus_range_request(params: &Params) -> Result<ReadRequest, String> {
    params.ensure_prometheus_only(&["query", "start", "end", "step", "lookback_delta"])?;
    let query = params.get("query").unwrap_or("");
    let start_input = params.get("start").unwrap_or("");
    let start = parse_prom_time(Some(start_input), 0).map_err(|_| {
        format!("invalid parameter \"start\": cannot parse \"{start_input}\" to a valid timestamp")
    })?;
    let stop_input = params.get("end").unwrap_or("");
    let stop = parse_prom_time(Some(stop_input), 0).map_err(|_| {
        format!("invalid parameter \"end\": cannot parse \"{stop_input}\" to a valid timestamp")
    })?;
    let step_input = params.get("step").unwrap_or("");
    let step = parse_prom_step(Some(step_input), 0).map_err(|_| {
        format!("invalid parameter \"step\": cannot parse \"{step_input}\" to a valid duration")
    })?;
    let lookback = parse_prom_request_lookback(params.get("lookback_delta"), 300_000)?;
    if stop < start {
        return Err(
            "invalid parameter \"end\": end timestamp must not be before start time".into(),
        );
    }
    if step <= 0 {
        return Err("step must be positive".into());
    }
    let grid_points = (i128::from(stop) - i128::from(start)) / i128::from(step) + 1;
    if grid_points > 11_000 {
        return Err(
            "exceeded maximum resolution of 11000 points per timeseries — decrease the query resolution (increase step)"
                .into(),
        );
    }
    let plan = lower_promql(query, lookback)
        .map_err(|error| format!("invalid parameter \"query\": {error}"))?;
    match plan {
        PromPlan::RangeSelector { .. } => {
            return Err("invalid parameter \"query\": invalid expression type \"range vector\" for range query, must be Scalar or instant Vector".into());
        }
        PromPlan::String(_) => {
            return Err("invalid parameter \"query\": invalid expression type \"string\" for range query, must be Scalar or instant Vector".into());
        }
        _ => {}
    }
    Ok(ReadRequest::Prometheus {
        plan,
        start,
        stop,
        step,
        instant: false,
    })
}

fn lower_promql(input: &str, lookback: i64) -> Result<PromPlan, String> {
    let parsed = promql::parse(input).map_err(|error| {
        if error == "no expression found in input" {
            "unknown position: parse error: no expression found in input".to_string()
        } else {
            format!("parse error: {error}")
        }
    })?;
    let lower_selector = |selector: promql::VectorSelector| -> Result<_, String> {
        if selector.offset.is_some() || selector.at.is_some() {
            return Err("PromQL modifiers are not shipped yet".into());
        }
        if !selector.matchers.or_matchers.is_empty() {
            return Err("PromQL OR matcher groups are not shipped yet".into());
        }
        let mut metric = selector.name;
        let mut matchers = Vec::with_capacity(selector.matchers.matchers.len());
        for matcher in selector.matchers.matchers {
            if matcher.name == "__name__" {
                if metric.is_some() {
                    return Err("metric name specified twice".into());
                }
                if !matches!(matcher.op, promql::MatchOp::Equal) {
                    return Err("non-exact __name__ matchers are not shipped yet".into());
                }
                metric = Some(matcher.value);
                continue;
            }
            let op = match matcher.op {
                promql::MatchOp::Equal => MatcherOp::Eq,
                promql::MatchOp::NotEqual => MatcherOp::NotEq,
                promql::MatchOp::Re(_) => MatcherOp::Regex,
                promql::MatchOp::NotRe(_) => MatcherOp::NotRegex,
            };
            matchers.push(Matcher::new(matcher.name, op, matcher.value)?);
        }
        let metric = metric.ok_or_else(|| "nameless selectors are not shipped yet".to_string())?;
        Ok((metric, FilterPlan::new(matchers)))
    };
    match parsed {
        promql::Expr::NumberLiteral(number) => Ok(PromPlan::Scalar(number.val)),
        promql::Expr::StringLiteral(string) => Ok(PromPlan::String(string.val)),
        promql::Expr::VectorSelector(selector) => {
            let (metric, filter) = lower_selector(selector)?;
            Ok(PromPlan::Selector {
                metric,
                filter,
                lookback,
            })
        }
        promql::Expr::MatrixSelector(selector) => {
            let window = i64::try_from(selector.range.as_millis())
                .map_err(|_| "PromQL range duration overflow".to_string())?;
            if window == 0 {
                return Err("PromQL range duration must be at least 1ms".into());
            }
            let (metric, filter) = lower_selector(selector.vs)?;
            Ok(PromPlan::RangeSelector {
                metric,
                filter,
                window,
            })
        }
        promql::Expr::Call(call) if call.func.name == "avg_over_time" => {
            let [argument] = call.args.args.as_slice() else {
                return Err("avg_over_time requires exactly one range vector".into());
            };
            let promql::Expr::MatrixSelector(selector) = argument.as_ref() else {
                return Err("avg_over_time requires a range vector".into());
            };
            let window = i64::try_from(selector.range.as_millis())
                .map_err(|_| "PromQL range duration overflow".to_string())?;
            if window == 0 {
                return Err("PromQL range duration must be at least 1ms".into());
            }
            let (metric, filter) = lower_selector(selector.vs.clone())?;
            Ok(PromPlan::AvgOverTime {
                metric,
                filter,
                window,
            })
        }
        other => Err(format!(
            "unsupported PromQL expression (parsed as {})",
            promql_expression_name(&other)
        )),
    }
}

fn promql_expression_name(expression: &promql::Expr) -> &'static str {
    match expression {
        promql::Expr::Aggregate(_) => "aggregation",
        promql::Expr::Unary(_) => "unary expression",
        promql::Expr::Binary(_) => "binary expression",
        promql::Expr::Paren(_) => "parenthesized expression",
        promql::Expr::Subquery(_) => "subquery",
        promql::Expr::NumberLiteral(_) => "scalar",
        promql::Expr::StringLiteral(_) => "string",
        promql::Expr::VectorSelector(_) => "instant vector",
        promql::Expr::MatrixSelector(_) => "range vector",
        promql::Expr::Call(_) => "function call",
        promql::Expr::Extension(_) => "extension",
    }
}

pub(crate) fn labels_request(params: &Params) -> Result<ReadRequest, String> {
    params.ensure_only(&["match[]", "match"])?;
    Ok(ReadRequest::Labels {
        selectors: parse_selectors(params)?,
    })
}

pub(crate) fn label_values_request(params: &Params, name: String) -> Result<ReadRequest, String> {
    params.ensure_only(&["metric", "match[]", "match"])?;
    Ok(ReadRequest::LabelValues {
        name,
        metric: params.get("metric").map(ToOwned::to_owned),
        selectors: parse_selectors(params)?,
    })
}

pub(crate) fn series_request(
    params: &Params,
    prometheus_alias: bool,
) -> Result<ReadRequest, String> {
    params.ensure_only(&["metric", "match[]", "match"])?;
    let selectors = parse_selectors(params)?;
    let metric = params.get("metric").map(ToOwned::to_owned);
    if prometheus_alias && selectors.is_empty() {
        return Err("missing required parameter: match[]".into());
    }
    if !prometheus_alias && selectors.is_empty() && metric.is_none() {
        return Err("missing required parameter: metric or match[]".into());
    }
    Ok(ReadRequest::Series { metric, selectors })
}

fn required_metric(params: &Params) -> Result<String, String> {
    params
        .get("metric")
        .map(ToOwned::to_owned)
        .ok_or_else(|| "missing required parameter: metric".into())
}

fn parse_selectors(params: &Params) -> Result<Vec<Selector>, String> {
    params
        .all(&["match[]", "match"])
        .iter()
        .map(|selector| parse_selector(selector))
        .collect()
}

fn parse_selector(input: &str) -> Result<Selector, String> {
    let mut parser = SelectorParser::new(input);
    parser.parse()
}

struct SelectorParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> SelectorParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse(&mut self) -> Result<Selector, String> {
        self.skip_ws();
        let metric = if self.peek() == Some(b'{') {
            None
        } else {
            Some(self.identifier(true)?)
        };
        self.skip_ws();
        let mut matchers = if self.peek() == Some(b'{') {
            self.matcher_list()?
        } else {
            Vec::new()
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(format!("unexpected selector input at byte {}", self.pos));
        }
        if metric.is_none() && matchers.is_empty() {
            return Err("selector must contain a metric or matcher".into());
        }

        let name_positions: Vec<_> = matchers
            .iter()
            .enumerate()
            .filter(|(_, matcher)| matcher.key == "__name__")
            .map(|(index, _)| index)
            .collect();
        if metric.is_some() && !name_positions.is_empty() {
            return Err("metric name specified twice".into());
        }
        if name_positions.len() > 1 {
            return Err("multiple __name__ matchers are not supported".into());
        }

        let metric = if let Some(metric) = metric {
            MetricSelection::Exact(metric)
        } else if let Some(index) = name_positions.first().copied() {
            let matcher = matchers.remove(index);
            match matcher.op {
                MatcherOp::Eq => MetricSelection::Exact(matcher.value),
                MatcherOp::Regex => MetricSelection::Regex(
                    matcher
                        .regex
                        .expect("regex matcher compiled during construction"),
                ),
                MatcherOp::NotEq | MatcherOp::NotRegex => {
                    return Err("negative __name__ matchers are not supported".into());
                }
            }
        } else {
            MetricSelection::All
        };
        Ok(Selector {
            metric,
            filter: FilterPlan::new(matchers),
        })
    }

    fn matcher_list(&mut self) -> Result<Vec<Matcher>, String> {
        self.expect(b'{')?;
        let mut matchers = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some(b'}') {
                self.pos += 1;
                return Ok(matchers);
            }
            let key = self.identifier(false)?;
            self.skip_ws();
            let op = self.operator()?;
            self.skip_ws();
            let value = self.quoted_string()?;
            matchers.push(Matcher::new(key, op, value)?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(matchers);
                }
                _ => return Err(format!("expected ',' or '}}' at byte {}", self.pos)),
            }
        }
    }

    fn identifier(&mut self, metric: bool) -> Result<String, String> {
        let start = self.pos;
        let first = self
            .peek()
            .ok_or_else(|| "expected identifier".to_string())?;
        if !first.is_ascii_alphabetic() && first != b'_' && !(metric && first == b':') {
            return Err(format!("expected identifier at byte {}", self.pos));
        }
        self.pos += 1;
        while let Some(byte) = self.peek() {
            if byte.is_ascii_alphanumeric() || byte == b'_' || (metric && byte == b':') {
                self.pos += 1;
            } else {
                break;
            }
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn operator(&mut self) -> Result<MatcherOp, String> {
        for (text, op) in [
            ("=~", MatcherOp::Regex),
            ("!~", MatcherOp::NotRegex),
            ("!=", MatcherOp::NotEq),
            ("=", MatcherOp::Eq),
        ] {
            if self.input[self.pos..].starts_with(text) {
                self.pos += text.len();
                return Ok(op);
            }
        }
        Err(format!("expected matcher operator at byte {}", self.pos))
    }

    fn quoted_string(&mut self) -> Result<String, String> {
        let quote = self
            .peek()
            .filter(|byte| matches!(byte, b'"' | b'\''))
            .ok_or_else(|| format!("expected quoted string at byte {}", self.pos))?;
        let start = self.pos;
        self.pos += 1;
        let mut escaped = false;
        while let Some(byte) = self.peek() {
            self.pos += 1;
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == quote {
                let encoded = &self.input[start..self.pos];
                if quote == b'"' {
                    return serde_json::from_str(encoded)
                        .map_err(|error| format!("invalid matcher string: {error}"));
                }
                return decode_single_quoted(&encoded[1..encoded.len() - 1]);
            }
        }
        Err("unterminated matcher string".into())
    }

    fn expect(&mut self, expected: u8) -> Result<(), String> {
        if self.peek() == Some(expected) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!(
                "expected '{}' at byte {}",
                expected as char, self.pos
            ))
        }
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.as_bytes().get(self.pos).copied()
    }
}

fn decode_single_quoted(input: &str) -> Result<String, String> {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => output.push('\n'),
            Some('r') => output.push('\r'),
            Some('t') => output.push('\t'),
            Some('\\') => output.push('\\'),
            Some('\'') => output.push('\''),
            Some(other) => output.push(other),
            None => return Err("unterminated matcher escape".into()),
        }
    }
    Ok(output)
}

fn parse_time(value: Option<&str>, default: i64) -> i64 {
    let Some(value) = value else {
        return default;
    };
    if value == "now" {
        return now_seconds();
    }
    if let Some(relative) = value.strip_prefix('-') {
        return now_seconds().saturating_sub(parse_duration(relative).unwrap_or(0));
    }
    parse_integer(Some(value)).unwrap_or(default)
}

fn parse_prom_time(value: Option<&str>, default: i64) -> Result<i64, String> {
    let Some(value) = value else {
        return Ok(default);
    };
    if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.timestamp_millis());
    }
    let seconds = value
        .parse::<f64>()
        .map_err(|_| format!("invalid timestamp: {value}"))?;
    let millis = seconds * 1_000.0;
    if !millis.is_finite() || millis < i64::MIN as f64 || millis > i64::MAX as f64 {
        return Err(format!("timestamp out of range: {value}"));
    }
    Ok(millis.round() as i64)
}

fn parse_prom_step(value: Option<&str>, default: i64) -> Result<i64, String> {
    let Some(value) = value else {
        return Ok(default);
    };
    let millis = parse_prom_duration_millis(value, "step")?;
    if millis <= 0 {
        return Err("step must be at least 1ms".into());
    }
    Ok(millis)
}

fn parse_prom_lookback(value: Option<&str>, default: i64) -> Result<i64, String> {
    let Some(value) = value else {
        return Ok(default);
    };
    let millis = parse_prom_duration_millis(value, "lookback delta")?;
    Ok(if millis == 0 { default } else { millis })
}

fn parse_prom_request_lookback(value: Option<&str>, default: i64) -> Result<i64, String> {
    parse_prom_lookback(value, default).map_err(|_| {
        format!(
            "error parsing lookback delta duration: cannot parse \"{}\" to a valid duration",
            value.unwrap_or("")
        )
    })
}

fn parse_prom_duration_millis(value: &str, parameter: &str) -> Result<i64, String> {
    if let Ok(seconds) = value.parse::<f64>() {
        let millis = seconds * 1_000.0;
        if !millis.is_finite() || millis < 0.0 || millis > i64::MAX as f64 {
            return Err(format!("invalid {parameter}: {value}"));
        }
        return Ok(millis.round() as i64);
    }
    let duration = match promql_parser::util::parse_duration(value) {
        Ok(duration) => duration,
        Err(error) if error == "duration must be greater than 0" => return Ok(0),
        Err(error) => return Err(format!("invalid {parameter}: {error}")),
    };
    i64::try_from(duration.as_millis()).map_err(|_| format!("{parameter} overflow"))
}

fn parse_duration(value: &str) -> Option<i64> {
    let digits = value.bytes().take_while(u8::is_ascii_digit).count();
    let number = value.get(..digits)?.parse::<i64>().ok()?;
    let multiplier = match value.get(digits..)? {
        "" | "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 604_800,
        _ => return None,
    };
    number.checked_mul(multiplier)
}

fn parse_integer(value: Option<&str>) -> Option<i64> {
    let value = value?;
    let bytes = value.as_bytes();
    let sign = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let digits = bytes[sign..]
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digits == 0 {
        return None;
    }
    value[..sign + digits].parse().ok()
}

fn now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct QueryFeatures {
    table: MetricsTable,
    latest_frame: bool,
    raw_frame: bool,
    window_batches: bool,
}

impl QueryFeatures {
    pub(crate) fn discover(conn: &Connection, table: MetricsTable) -> Result<Self, String> {
        let mut stmt = conn
            .prepare(
                "SELECT name FROM pragma_module_list
                  WHERE name IN ('timeless_latest_frame', 'timeless_raw_frame',
                                 'timeless_window_batches')",
            )
            .map_err(|error| format!("discover query modules: {error}"))?;
        let modules = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| format!("read query modules: {error}"))?
            .collect::<Result<HashSet<_>, _>>()
            .map_err(|error| format!("collect query modules: {error}"))?;
        Ok(Self {
            table,
            latest_frame: modules.contains("timeless_latest_frame"),
            raw_frame: modules.contains("timeless_raw_frame"),
            window_batches: modules.contains("timeless_window_batches"),
        })
    }
}

pub(crate) struct ReadOutput {
    pub body: Vec<u8>,
    pub frame_bytes: usize,
    pub series: u64,
    pub points: u64,
    pub rows: u64,
}

pub(crate) fn execute(
    conn: &Connection,
    features: QueryFeatures,
    request: ReadRequest,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    match request {
        ReadRequest::Latest {
            metric,
            filter,
            stop,
        } => execute_latest(conn, features, &metric, &filter, stop),
        ReadRequest::Export {
            metric,
            filter,
            start,
            stop,
        } => execute_export(conn, features, &metric, &filter, start, stop),
        ReadRequest::Range {
            metric,
            filter,
            start,
            stop,
            step,
            aggregate,
        } => execute_range(
            conn,
            features,
            RangeQuery {
                metric: &metric,
                filter: &filter,
                start,
                stop,
                step,
                aggregate,
            },
        ),
        ReadRequest::Labels { selectors } => execute_labels(conn, features.table, &selectors),
        ReadRequest::LabelValues {
            name,
            metric,
            selectors,
        } => execute_label_values(conn, features.table, &name, metric.as_deref(), &selectors),
        ReadRequest::Series { metric, selectors } => {
            execute_series(conn, features.table, metric.as_deref(), &selectors)
        }
        ReadRequest::Prometheus {
            plan,
            start,
            stop,
            step,
            instant,
        } => execute_prometheus(conn, features, &plan, start, stop, step, instant, cancelled),
    }
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), String> {
    if cancelled.load(Ordering::Acquire) {
        Err("query cancelled".into())
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct SeriesMeta {
    id: i64,
    metric: String,
    labels_json: String,
    labels: BTreeMap<String, String>,
}

fn catalog(
    conn: &Connection,
    table: MetricsTable,
    metric: &str,
    filter: &FilterPlan,
) -> Result<Vec<SeriesMeta>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT series_id, labels
               FROM timeless_series('{}', ?1, ?2)
              ORDER BY labels, series_id",
            table.name()
        ))
        .map_err(|error| format!("prepare series catalog: {error}"))?;
    let rows = stmt
        .query_map(params![metric, filter.pushdown_json], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| format!("query series catalog: {error}"))?;
    let mut output = Vec::new();
    for row in rows {
        let (id, labels_json) = row.map_err(|error| format!("read series catalog: {error}"))?;
        let labels = decode_labels(&labels_json)?;
        if filter.matches(&labels) {
            output.push(SeriesMeta {
                id,
                metric: metric.to_string(),
                labels_json,
                labels,
            });
        }
    }
    Ok(output)
}

fn catalog_all(conn: &Connection, table: MetricsTable) -> Result<Vec<SeriesMeta>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT series_id, name, labels
               FROM timeless_series('{}')
              ORDER BY name, labels, series_id",
            table.name()
        ))
        .map_err(|error| format!("prepare complete series catalog: {error}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|error| format!("query complete series catalog: {error}"))?;
    let mut output = Vec::new();
    for row in rows {
        let (id, metric, labels_json) =
            row.map_err(|error| format!("read complete series catalog: {error}"))?;
        output.push(SeriesMeta {
            id,
            metric,
            labels: decode_labels(&labels_json)?,
            labels_json,
        });
    }
    Ok(output)
}

fn all_metrics(conn: &Connection, table: MetricsTable) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT DISTINCT name FROM timeless_series('{}') ORDER BY name",
            table.name()
        ))
        .map_err(|error| format!("prepare metric discovery: {error}"))?;
    let metrics = stmt
        .query_map([], |row| row.get(0))
        .map_err(|error| format!("query metric discovery: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("collect metric discovery: {error}"))?;
    Ok(metrics)
}

fn decode_labels(value: &str) -> Result<BTreeMap<String, String>, String> {
    serde_json::from_str(value).map_err(|error| format!("decode canonical labels: {error}"))
}

fn execute_latest(
    conn: &Connection,
    features: QueryFeatures,
    metric: &str,
    filter: &FilterPlan,
    stop: i64,
) -> Result<ReadOutput, String> {
    let catalog = catalog(conn, features.table, metric, filter)?;
    let by_id: HashMap<_, _> = catalog.iter().map(|meta| (meta.id, meta)).collect();
    let (mut rows, frame_bytes) = if features.latest_frame {
        let frame: Option<Vec<u8>> = conn
            .query_row(
                &format!(
                    "SELECT frame FROM timeless_latest_frame('{}', ?1, ?2, 0, ?3)",
                    features.table.name()
                ),
                params![metric, filter.pushdown_json, stop],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| format!("query latest frame: {error}"))?;
        match frame {
            Some(frame) => {
                let frame_bytes = frame.len();
                (decode_latest_frame(&frame)?, frame_bytes)
            }
            None => (Vec::new(), 0),
        }
    } else {
        (latest_rows(conn, features.table, metric, filter, stop)?, 0)
    };
    rows.retain(|row| by_id.contains_key(&row.id));
    rows.sort_by(|left, right| {
        by_id[&left.id]
            .labels_json
            .cmp(&by_id[&right.id].labels_json)
            .then(left.id.cmp(&right.id))
    });
    let mut body = Vec::new();
    if rows.len() == 1 {
        write_latest_object(&mut body, by_id[&rows[0].id], &rows[0])?;
    } else {
        body.extend_from_slice(br#"{"data":["#);
        for (index, row) in rows.iter().enumerate() {
            comma(&mut body, index);
            write_latest_object(&mut body, by_id[&row.id], row)?;
        }
        body.extend_from_slice(b"]}");
    }
    Ok(ReadOutput {
        body,
        frame_bytes,
        series: rows.len() as u64,
        points: rows.len() as u64,
        rows: rows.len() as u64,
    })
}

#[derive(Clone, Copy, Debug)]
struct LatestRow {
    id: i64,
    timestamp: i64,
    value: Option<f64>,
}

fn latest_rows(
    conn: &Connection,
    table: MetricsTable,
    metric: &str,
    filter: &FilterPlan,
    stop: i64,
) -> Result<Vec<LatestRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT series_id, ts, value
               FROM timeless_latest('{}', ?1, ?2, 0, ?3)",
            table.name()
        ))
        .map_err(|error| format!("prepare latest rows: {error}"))?;
    let rows = stmt
        .query_map(params![metric, filter.pushdown_json, stop], |row| {
            Ok(LatestRow {
                id: row.get(0)?,
                timestamp: row.get(1)?,
                value: row.get(2)?,
            })
        })
        .map_err(|error| format!("query latest rows: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("collect latest rows: {error}"))?;
    Ok(rows)
}

fn write_latest_object(
    output: &mut Vec<u8>,
    meta: &SeriesMeta,
    row: &LatestRow,
) -> Result<(), String> {
    output.extend_from_slice(b"{\"labels\":");
    output.extend_from_slice(meta.labels_json.as_bytes());
    output.extend_from_slice(b",\"timestamp\":");
    write_json(output, &row.timestamp)?;
    output.extend_from_slice(b",\"value\":");
    write_optional_float(output, row.value)?;
    output.push(b'}');
    Ok(())
}

fn execute_export(
    conn: &Connection,
    features: QueryFeatures,
    metric: &str,
    filter: &FilterPlan,
    start: i64,
    stop: i64,
) -> Result<ReadOutput, String> {
    let catalog = catalog(conn, features.table, metric, filter)?;
    let raw = raw_query(conn, features, metric, filter, start, stop)?;
    let by_id: HashMap<_, _> = raw
        .series
        .iter()
        .map(|series| (series.id, series))
        .collect();
    let mut body = Vec::new();
    let mut emitted = 0_u64;
    let mut points = 0_u64;
    for meta in &catalog {
        let Some(series) = by_id.get(&meta.id) else {
            continue;
        };
        if emitted > 0 {
            body.push(b'\n');
        }
        body.extend_from_slice(b"{\"metric\":");
        let mut labels = meta.labels.clone();
        labels.insert("__name__".into(), metric.into());
        write_json(&mut body, &labels)?;
        body.extend_from_slice(b",\"timestamps\":[");
        for index in 0..series.len() {
            comma(&mut body, index);
            let millis =
                i128::from(series.timestamp(raw.frame.as_deref(), index)?).saturating_mul(1_000);
            body.extend_from_slice(millis.to_string().as_bytes());
        }
        body.extend_from_slice(b"],\"values\":[");
        for index in 0..series.len() {
            comma(&mut body, index);
            write_float(&mut body, series.value(raw.frame.as_deref(), index)?)?;
        }
        body.extend_from_slice(b"]}");
        emitted += 1;
        points = points.saturating_add(series.len() as u64);
    }
    Ok(ReadOutput {
        body,
        frame_bytes: raw.frame_bytes,
        series: emitted,
        points,
        rows: points,
    })
}

struct RawQuery {
    series: Vec<RawSeries>,
    frame: Option<Vec<u8>>,
    frame_bytes: usize,
}

struct RawSeries {
    id: i64,
    data: RawSeriesData,
}

enum RawSeriesData {
    Frame {
        timestamps_start: usize,
        values_start: usize,
        count: usize,
    },
    Owned {
        timestamps: Vec<i64>,
        values: Vec<f64>,
    },
}

impl RawSeries {
    fn len(&self) -> usize {
        match &self.data {
            RawSeriesData::Frame { count, .. } => *count,
            RawSeriesData::Owned { timestamps, .. } => timestamps.len(),
        }
    }

    fn timestamp(&self, frame: Option<&[u8]>, index: usize) -> Result<i64, String> {
        match &self.data {
            RawSeriesData::Frame {
                timestamps_start,
                count,
                ..
            } if index < *count => Ok(i64_at(
                frame.ok_or_else(|| "raw frame storage is missing".to_string())?,
                timestamps_start + index * 8,
            )),
            RawSeriesData::Owned { timestamps, .. } => timestamps
                .get(index)
                .copied()
                .ok_or_else(|| "raw row timestamp index out of bounds".into()),
            RawSeriesData::Frame { .. } => Err("raw frame timestamp index out of bounds".into()),
        }
    }

    fn value(&self, frame: Option<&[u8]>, index: usize) -> Result<f64, String> {
        match &self.data {
            RawSeriesData::Frame {
                values_start,
                count,
                ..
            } if index < *count => Ok(f64::from_bits(u64_at(
                frame.ok_or_else(|| "raw frame storage is missing".to_string())?,
                values_start + index * 8,
            ))),
            RawSeriesData::Owned { values, .. } => values
                .get(index)
                .copied()
                .ok_or_else(|| "raw row value index out of bounds".into()),
            RawSeriesData::Frame { .. } => Err("raw frame value index out of bounds".into()),
        }
    }
}

fn raw_query(
    conn: &Connection,
    features: QueryFeatures,
    metric: &str,
    filter: &FilterPlan,
    start: i64,
    stop: i64,
) -> Result<RawQuery, String> {
    if features.raw_frame {
        let frame: Option<Vec<u8>> = conn
            .query_row(
                &format!(
                    "SELECT frame FROM timeless_raw_frame('{}', ?1, ?2, ?3, ?4)",
                    features.table.name()
                ),
                params![metric, filter.pushdown_json, start, stop],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| format!("query raw frame: {error}"))?;
        return match frame {
            Some(frame) => {
                let frame_bytes = frame.len();
                Ok(RawQuery {
                    series: decode_raw_frame(&frame)?,
                    frame: Some(frame),
                    frame_bytes,
                })
            }
            None => Ok(RawQuery {
                series: Vec::new(),
                frame: None,
                frame_bytes: 0,
            }),
        };
    }

    let mut stmt = conn
        .prepare(&format!(
            "SELECT series_id, ts, value
               FROM timeless_raw('{}', ?1, ?2, ?3, ?4)
              ORDER BY series_id, ts",
            features.table.name()
        ))
        .map_err(|error| format!("prepare raw row fallback: {error}"))?;
    let mut rows = stmt
        .query(params![metric, filter.pushdown_json, start, stop])
        .map_err(|error| format!("query raw row fallback: {error}"))?;
    let mut series = Vec::<RawSeries>::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("read raw row fallback: {error}"))?
    {
        let id: i64 = row
            .get(0)
            .map_err(|error| format!("read raw series id: {error}"))?;
        if series.last().is_none_or(|current| current.id != id) {
            series.push(RawSeries {
                id,
                data: RawSeriesData::Owned {
                    timestamps: Vec::new(),
                    values: Vec::new(),
                },
            });
        }
        let current = series.last_mut().expect("series inserted above");
        let RawSeriesData::Owned { timestamps, values } = &mut current.data else {
            unreachable!("row fallback always uses owned columns")
        };
        timestamps.push(
            row.get(1)
                .map_err(|error| format!("read raw timestamp: {error}"))?,
        );
        values.push(
            row.get(2)
                .map_err(|error| format!("read raw value: {error}"))?,
        );
    }
    Ok(RawQuery {
        series,
        frame: None,
        frame_bytes: 0,
    })
}

#[derive(Clone, Copy)]
struct RangeQuery<'a> {
    metric: &'a str,
    filter: &'a FilterPlan,
    start: i64,
    stop: i64,
    step: i64,
    aggregate: Aggregate,
}

fn execute_range(
    conn: &Connection,
    features: QueryFeatures,
    query: RangeQuery<'_>,
) -> Result<ReadOutput, String> {
    let span = query.stop.saturating_sub(query.start).saturating_add(1);
    let native = features.window_batches
        && query.aggregate.native_name().is_some()
        && span > 0
        && span % query.step == 0
        && span / query.step <= 1_000_000;
    if native {
        return execute_native_range(conn, features.table, query);
    }
    execute_raw_range(conn, features, query)
}

fn execute_native_range(
    conn: &Connection,
    table: MetricsTable,
    query: RangeQuery<'_>,
) -> Result<ReadOutput, String> {
    let window_start = query
        .start
        .checked_add(query.step - 1)
        .ok_or_else(|| "range window start overflow".to_string())?;
    let mut stmt = conn
        .prepare(&format!(
            "SELECT series_id, labels, buckets
               FROM timeless_window_batches('{}', ?1, ?2, ?3, ?4, ?5, ?6, ?7)
              ORDER BY labels, series_id",
            table.name()
        ))
        .map_err(|error| format!("prepare window batches: {error}"))?;
    let rows = stmt
        .query_map(
            params![
                query.metric,
                query.filter.pushdown_json,
                window_start,
                query.stop,
                query.step,
                query.step,
                query
                    .aggregate
                    .native_name()
                    .expect("native aggregate checked")
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .map_err(|error| format!("query window batches: {error}"))?;
    let mut body = Vec::new();
    write_range_prefix(&mut body, query.metric)?;
    let mut emitted = 0_usize;
    let mut points = 0_u64;
    let mut frame_bytes = 0_usize;
    for row in rows {
        let (_id, labels_json, buckets) =
            row.map_err(|error| format!("read window batch: {error}"))?;
        let labels = decode_labels(&labels_json)?;
        if !query.filter.matches(&labels) {
            continue;
        }
        let decoded = decode_window_batch(&buckets)?;
        comma(&mut body, emitted);
        body.extend_from_slice(b"{\"labels\":");
        body.extend_from_slice(labels_json.as_bytes());
        body.extend_from_slice(b",\"data\":[");
        for index in 0..decoded.len() {
            comma(&mut body, index);
            body.push(b'[');
            write_json(
                &mut body,
                &decoded.timestamp(index).saturating_sub(query.step - 1),
            )?;
            body.push(b',');
            let value = decoded.value(index);
            if query.aggregate == Aggregate::Count {
                match value {
                    Some(value) => body.extend_from_slice((value as i64).to_string().as_bytes()),
                    None => body.extend_from_slice(b"null"),
                }
            } else {
                write_optional_float(&mut body, value)?;
            }
            body.push(b']');
        }
        body.extend_from_slice(b"]}");
        emitted += 1;
        points = points.saturating_add(decoded.len() as u64);
        frame_bytes = frame_bytes.saturating_add(buckets.len());
    }
    body.extend_from_slice(b"]}");
    Ok(ReadOutput {
        body,
        frame_bytes,
        series: emitted as u64,
        points,
        rows: points,
    })
}

fn execute_raw_range(
    conn: &Connection,
    features: QueryFeatures,
    query: RangeQuery<'_>,
) -> Result<ReadOutput, String> {
    let catalog = catalog(conn, features.table, query.metric, query.filter)?;
    let raw = raw_query(
        conn,
        features,
        query.metric,
        query.filter,
        query.start,
        query.stop,
    )?;
    let by_id: HashMap<_, _> = raw
        .series
        .iter()
        .map(|series| (series.id, series))
        .collect();
    let mut body = Vec::new();
    write_range_prefix(&mut body, query.metric)?;
    let mut emitted = 0_usize;
    let mut point_count = 0_u64;
    for meta in &catalog {
        let Some(series) = by_id.get(&meta.id) else {
            continue;
        };
        let buckets = aggregate_raw(
            series,
            raw.frame.as_deref(),
            query.start,
            query.step,
            query.aggregate,
        )?;
        comma(&mut body, emitted);
        body.extend_from_slice(b"{\"labels\":");
        body.extend_from_slice(meta.labels_json.as_bytes());
        body.extend_from_slice(b",\"data\":[");
        for (index, (timestamp, value)) in buckets.iter().enumerate() {
            comma(&mut body, index);
            body.push(b'[');
            write_json(&mut body, timestamp)?;
            body.push(b',');
            match value {
                BucketValue::Integer(value) => write_json(&mut body, value)?,
                BucketValue::Real(value) => write_float(&mut body, *value)?,
            }
            body.push(b']');
        }
        body.extend_from_slice(b"]}");
        emitted += 1;
        point_count = point_count.saturating_add(buckets.len() as u64);
    }
    body.extend_from_slice(b"]}");
    Ok(ReadOutput {
        body,
        frame_bytes: raw.frame_bytes,
        series: emitted as u64,
        points: point_count,
        rows: point_count,
    })
}

fn write_range_prefix(output: &mut Vec<u8>, metric: &str) -> Result<(), String> {
    output.extend_from_slice(b"{\"metric\":");
    write_json(output, &metric)?;
    output.extend_from_slice(b",\"series\":[");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus(
    conn: &Connection,
    features: QueryFeatures,
    plan: &PromPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    match plan {
        PromPlan::Scalar(value) => {
            execute_prometheus_scalar(*value, start, stop, step, instant, cancelled)
        }
        PromPlan::String(value) => execute_prometheus_string(value, start, instant),
        PromPlan::Selector {
            metric,
            filter,
            lookback,
        } => execute_prometheus_selector(
            conn, features, metric, filter, start, stop, step, *lookback, instant, cancelled,
        ),
        PromPlan::AvgOverTime {
            metric,
            filter,
            window,
        } if features.window_batches
            && [start, stop, step, *window]
                .into_iter()
                .all(|value| value % 1_000 == 0) =>
        {
            execute_prometheus_window(
                conn,
                features.table,
                metric,
                filter,
                start / 1_000,
                stop / 1_000,
                step / 1_000,
                *window / 1_000,
                instant,
                cancelled,
            )
        }
        PromPlan::AvgOverTime {
            metric,
            filter,
            window,
        } => execute_prometheus_avg_raw(
            conn, features, metric, filter, start, stop, step, *window, instant, cancelled,
        ),
        PromPlan::RangeSelector {
            metric,
            filter,
            window,
        } => execute_prometheus_range_selector(
            conn, features, metric, filter, stop, *window, cancelled,
        ),
    }
}

fn execute_prometheus_string(
    value: &str,
    timestamp: i64,
    instant: bool,
) -> Result<ReadOutput, String> {
    if !instant {
        return Err(
            "invalid expression type \"string\" for range query, must be Scalar or instant Vector"
                .into(),
        );
    }
    let mut body = Vec::new();
    body.extend_from_slice(br#"{"status":"success","data":{"resultType":"string","result":["#);
    write_prometheus_timestamp(&mut body, timestamp);
    body.push(b',');
    write_json(&mut body, value)?;
    body.extend_from_slice(b"]}}");
    Ok(ReadOutput {
        body,
        frame_bytes: 0,
        series: 0,
        points: 1,
        rows: 1,
    })
}

fn execute_prometheus_scalar(
    value: f64,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let mut body = Vec::new();
    let mut points = 0_u64;
    if instant {
        body.extend_from_slice(br#"{"status":"success","data":{"resultType":"scalar","result":"#);
        write_prometheus_sample(&mut body, start, value)?;
        body.extend_from_slice(b"}}");
        points = 1;
    } else {
        body.extend_from_slice(
            br#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":["#,
        );
        let mut timestamp = start;
        loop {
            check_cancelled(cancelled)?;
            comma(&mut body, points as usize);
            write_prometheus_sample(&mut body, timestamp, value)?;
            points += 1;
            if timestamp >= stop {
                break;
            }
            let Some(next) = timestamp.checked_add(step).filter(|next| *next <= stop) else {
                break;
            };
            timestamp = next;
        }
        body.extend_from_slice(b"]}]}}");
    }
    Ok(ReadOutput {
        body,
        frame_bytes: 0,
        series: u64::from(!instant),
        points,
        rows: points,
    })
}

fn execute_prometheus_range_selector(
    conn: &Connection,
    features: QueryFeatures,
    metric: &str,
    filter: &FilterPlan,
    evaluation_time: i64,
    window: i64,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let lower = evaluation_time.saturating_sub(window);
    let catalog = catalog(conn, features.table, metric, filter)?;
    let raw = raw_query(
        conn,
        features,
        metric,
        filter,
        storage_seconds_floor(lower),
        storage_seconds_floor(evaluation_time),
    )?;
    let by_id: HashMap<_, _> = raw
        .series
        .iter()
        .map(|series| (series.id, series))
        .collect();
    let mut body = br#"{"status":"success","data":{"resultType":"matrix","result":["#.to_vec();
    let mut emitted = 0_u64;
    let mut points = 0_u64;
    for meta in &catalog {
        check_cancelled(cancelled)?;
        let Some(series) = by_id.get(&meta.id) else {
            continue;
        };
        let item_start = body.len();
        comma(&mut body, emitted as usize);
        write_prometheus_item_prefix(&mut body, Some(metric), &meta.labels, false)?;
        let mut item_points = 0_u64;
        for index in 0..series.len() {
            check_cancelled(cancelled)?;
            let timestamp = seconds_to_millis(series.timestamp(raw.frame.as_deref(), index)?);
            if timestamp <= lower || timestamp > evaluation_time {
                continue;
            }
            comma(&mut body, item_points as usize);
            write_prometheus_sample(
                &mut body,
                timestamp,
                series.value(raw.frame.as_deref(), index)?,
            )?;
            item_points += 1;
        }
        if item_points == 0 {
            body.truncate(item_start);
        } else {
            write_prometheus_item_suffix(&mut body, false);
            emitted += 1;
            points = points.saturating_add(item_points);
        }
    }
    write_prometheus_suffix(&mut body);
    Ok(ReadOutput {
        body,
        frame_bytes: raw.frame_bytes,
        series: emitted,
        points,
        rows: points,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_selector(
    conn: &Connection,
    features: QueryFeatures,
    metric: &str,
    filter: &FilterPlan,
    start: i64,
    stop: i64,
    step: i64,
    lookback: i64,
    instant: bool,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let catalog = catalog(conn, features.table, metric, filter)?;
    let raw = raw_query(
        conn,
        features,
        metric,
        filter,
        storage_seconds_floor(start.saturating_sub(lookback)),
        storage_seconds_floor(stop),
    )?;
    let by_id: HashMap<_, _> = raw
        .series
        .iter()
        .map(|series| (series.id, series))
        .collect();
    let mut body = Vec::new();
    write_prometheus_prefix(&mut body, instant);
    let mut emitted = 0_usize;
    let mut points = 0_u64;
    for meta in &catalog {
        check_cancelled(cancelled)?;
        let Some(series) = by_id.get(&meta.id) else {
            continue;
        };
        let item_start = body.len();
        comma(&mut body, emitted);
        write_prometheus_item_prefix(&mut body, Some(metric), &meta.labels, instant)?;
        let mut lo = 0_usize;
        let mut hi = 0_usize;
        let mut item_points = 0_u64;
        let mut t = start;
        loop {
            check_cancelled(cancelled)?;
            while hi < series.len()
                && seconds_to_millis(series.timestamp(raw.frame.as_deref(), hi)?) <= t
            {
                hi += 1;
            }
            let lower = t.saturating_sub(lookback);
            while lo < hi && seconds_to_millis(series.timestamp(raw.frame.as_deref(), lo)?) <= lower
            {
                lo += 1;
            }
            if hi > lo {
                if !instant {
                    comma(&mut body, item_points as usize);
                }
                write_prometheus_sample(&mut body, t, series.value(raw.frame.as_deref(), hi - 1)?)?;
                item_points += 1;
            }
            if t >= stop {
                break;
            }
            let Some(next) = t.checked_add(step).filter(|next| *next <= stop) else {
                break;
            };
            t = next;
        }
        if item_points == 0 {
            body.truncate(item_start);
        } else {
            write_prometheus_item_suffix(&mut body, instant);
            emitted += 1;
            points = points.saturating_add(item_points);
        }
    }
    write_prometheus_suffix(&mut body);
    Ok(ReadOutput {
        body,
        frame_bytes: raw.frame_bytes,
        series: emitted as u64,
        points,
        rows: points,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_window(
    conn: &Connection,
    table: MetricsTable,
    metric: &str,
    filter: &FilterPlan,
    start: i64,
    stop: i64,
    step: i64,
    window: i64,
    instant: bool,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT labels, buckets
               FROM timeless_window_batches('{}', ?1, ?2, ?3, ?4, ?5, ?6, 'avg')
              ORDER BY labels, series_id",
            table.name()
        ))
        .map_err(|error| format!("prepare PromQL window batches: {error}"))?;
    let rows = stmt
        .query_map(
            params![metric, filter.pushdown_json, start, stop, step, window],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .map_err(|error| format!("query PromQL window batches: {error}"))?;
    let mut body = Vec::new();
    write_prometheus_prefix(&mut body, instant);
    let mut emitted = 0_usize;
    let mut points = 0_u64;
    let mut frame_bytes = 0_usize;
    for row in rows {
        check_cancelled(cancelled)?;
        let (labels_json, buckets) =
            row.map_err(|error| format!("read PromQL window batch: {error}"))?;
        frame_bytes = frame_bytes.saturating_add(buckets.len());
        let labels = decode_labels(&labels_json)?;
        if !filter.matches(&labels) {
            continue;
        }
        let decoded = decode_window_batch(&buckets)?;
        let item_start = body.len();
        comma(&mut body, emitted);
        write_prometheus_item_prefix(&mut body, None, &labels, instant)?;
        let mut item_points = 0_u64;
        for index in 0..decoded.len() {
            check_cancelled(cancelled)?;
            let Some(value) = decoded.value(index) else {
                continue;
            };
            if !instant {
                comma(&mut body, item_points as usize);
            }
            write_prometheus_sample(
                &mut body,
                seconds_to_millis(decoded.timestamp(index)),
                value,
            )?;
            item_points += 1;
        }
        if item_points == 0 {
            body.truncate(item_start);
        } else {
            write_prometheus_item_suffix(&mut body, instant);
            emitted += 1;
            points = points.saturating_add(item_points);
        }
    }
    write_prometheus_suffix(&mut body);
    Ok(ReadOutput {
        body,
        frame_bytes,
        series: emitted as u64,
        points,
        rows: points,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_avg_raw(
    conn: &Connection,
    features: QueryFeatures,
    metric: &str,
    filter: &FilterPlan,
    start: i64,
    stop: i64,
    step: i64,
    window: i64,
    instant: bool,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let catalog = catalog(conn, features.table, metric, filter)?;
    let raw = raw_query(
        conn,
        features,
        metric,
        filter,
        storage_seconds_floor(start.saturating_sub(window)),
        storage_seconds_floor(stop),
    )?;
    let by_id: HashMap<_, _> = raw
        .series
        .iter()
        .map(|series| (series.id, series))
        .collect();
    let mut body = Vec::new();
    write_prometheus_prefix(&mut body, instant);
    let mut emitted = 0_usize;
    let mut points = 0_u64;
    for meta in &catalog {
        check_cancelled(cancelled)?;
        let Some(series) = by_id.get(&meta.id) else {
            continue;
        };
        let item_start = body.len();
        comma(&mut body, emitted);
        write_prometheus_item_prefix(&mut body, None, &meta.labels, instant)?;
        let mut lo = 0_usize;
        let mut hi = 0_usize;
        let mut item_points = 0_u64;
        let mut t = start;
        loop {
            check_cancelled(cancelled)?;
            while hi < series.len()
                && seconds_to_millis(series.timestamp(raw.frame.as_deref(), hi)?) <= t
            {
                hi += 1;
            }
            let lower = t.saturating_sub(window);
            while lo < hi && seconds_to_millis(series.timestamp(raw.frame.as_deref(), lo)?) <= lower
            {
                lo += 1;
            }
            if hi > lo {
                let mut sum = 0.0;
                for index in lo..hi {
                    sum += series.value(raw.frame.as_deref(), index)?;
                }
                if !instant {
                    comma(&mut body, item_points as usize);
                }
                write_prometheus_sample(&mut body, t, sum / (hi - lo) as f64)?;
                item_points += 1;
            }
            if t >= stop {
                break;
            }
            let Some(next) = t.checked_add(step).filter(|next| *next <= stop) else {
                break;
            };
            t = next;
        }
        if item_points == 0 {
            body.truncate(item_start);
        } else {
            write_prometheus_item_suffix(&mut body, instant);
            emitted += 1;
            points = points.saturating_add(item_points);
        }
    }
    write_prometheus_suffix(&mut body);
    Ok(ReadOutput {
        body,
        frame_bytes: raw.frame_bytes,
        series: emitted as u64,
        points,
        rows: points,
    })
}

fn write_prometheus_prefix(output: &mut Vec<u8>, instant: bool) {
    output.extend_from_slice(if instant {
        br#"{"status":"success","data":{"resultType":"vector","result":["#
    } else {
        br#"{"status":"success","data":{"resultType":"matrix","result":["#
    });
}

fn write_prometheus_item_prefix(
    output: &mut Vec<u8>,
    metric: Option<&str>,
    labels: &BTreeMap<String, String>,
    instant: bool,
) -> Result<(), String> {
    let mut labels = labels.clone();
    if let Some(metric) = metric {
        labels.insert("__name__".into(), metric.into());
    }
    output.extend_from_slice(b"{\"metric\":");
    write_json(output, &labels)?;
    output.extend_from_slice(if instant {
        b",\"value\":"
    } else {
        b",\"values\":["
    });
    Ok(())
}

fn write_prometheus_item_suffix(output: &mut Vec<u8>, instant: bool) {
    if instant {
        output.push(b'}');
    } else {
        output.extend_from_slice(b"]}");
    }
}

fn write_prometheus_sample(output: &mut Vec<u8>, timestamp: i64, value: f64) -> Result<(), String> {
    output.push(b'[');
    write_prometheus_timestamp(output, timestamp);
    output.push(b',');
    write_json(output, &format_prometheus_value(value))?;
    output.push(b']');
    Ok(())
}

fn write_prometheus_timestamp(output: &mut Vec<u8>, timestamp_ms: i64) {
    let negative = timestamp_ms < 0;
    let absolute = i128::from(timestamp_ms).abs();
    let seconds = absolute / 1_000;
    let millis = absolute % 1_000;
    if negative {
        output.push(b'-');
    }
    output.extend_from_slice(seconds.to_string().as_bytes());
    if millis == 0 {
        return;
    }
    output.push(b'.');
    let mut fraction = format!("{millis:03}");
    while fraction.ends_with('0') {
        fraction.pop();
    }
    output.extend_from_slice(fraction.as_bytes());
}

fn storage_seconds_floor(timestamp_ms: i64) -> i64 {
    timestamp_ms.div_euclid(1_000)
}

fn seconds_to_millis(timestamp: i64) -> i64 {
    timestamp.saturating_mul(1_000)
}

fn write_prometheus_suffix(output: &mut Vec<u8>) {
    output.extend_from_slice(b"]}}");
}

fn format_prometheus_value(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value == f64::INFINITY {
        return "+Inf".into();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".into();
    }
    value.to_string()
}

enum BucketValue {
    Integer(i64),
    Real(f64),
}

fn aggregate_raw(
    series: &RawSeries,
    frame: Option<&[u8]>,
    start: i64,
    step: i64,
    aggregate: Aggregate,
) -> Result<Vec<(i64, BucketValue)>, String> {
    let mut points = Vec::with_capacity(series.len());
    for index in 0..series.len() {
        points.push((series.timestamp(frame, index)?, series.value(frame, index)?));
    }
    points.sort_by_key(|point| point.0);
    if aggregate == Aggregate::Rate {
        return Ok(aggregate_rate(&points, start, step));
    }
    let mut buckets: BTreeMap<i64, Vec<(i64, f64)>> = BTreeMap::new();
    for point in points {
        let bucket = start.saturating_add((point.0.saturating_sub(start) / step) * step);
        buckets.entry(bucket).or_default().push(point);
    }
    Ok(buckets
        .into_iter()
        .map(|(timestamp, points)| {
            let value = match aggregate {
                Aggregate::Count => BucketValue::Integer(points.len() as i64),
                Aggregate::Avg => BucketValue::Real(
                    points.iter().map(|point| point.1).sum::<f64>() / points.len() as f64,
                ),
                Aggregate::Sum => {
                    BucketValue::Real(points.iter().map(|point| point.1).sum::<f64>())
                }
                Aggregate::Min => BucketValue::Real(
                    points
                        .iter()
                        .map(|point| point.1)
                        .reduce(f64::min)
                        .unwrap_or(f64::NAN),
                ),
                Aggregate::Max => BucketValue::Real(
                    points
                        .iter()
                        .map(|point| point.1)
                        .reduce(f64::max)
                        .unwrap_or(f64::NAN),
                ),
                Aggregate::First => BucketValue::Real(
                    points
                        .iter()
                        .min_by_key(|point| point.0)
                        .map_or(f64::NAN, |p| p.1),
                ),
                Aggregate::Last => BucketValue::Real(points.last().map_or(f64::NAN, |last| {
                    points
                        .iter()
                        .find(|point| point.0 == last.0)
                        .map_or(f64::NAN, |point| point.1)
                })),
                Aggregate::Rate => unreachable!("rate handled above"),
            };
            (timestamp, value)
        })
        .collect())
}

fn aggregate_rate(points: &[(i64, f64)], start: i64, step: i64) -> Vec<(i64, BucketValue)> {
    let mut output = Vec::new();
    let mut carry = None;
    let mut index = 0;
    while index < points.len() {
        let bucket = start.saturating_add((points[index].0.saturating_sub(start) / step) * step);
        let begin = index;
        while index < points.len()
            && start.saturating_add((points[index].0.saturating_sub(start) / step) * step) == bucket
        {
            index += 1;
        }
        let mut previous = carry;
        let mut delta = 0.0;
        let mut elapsed = 0_i64;
        for point in &points[begin..index] {
            if let Some((prior_ts, prior_value)) = previous {
                if point.1 >= prior_value && point.0 > prior_ts {
                    delta += point.1 - prior_value;
                    elapsed = elapsed.saturating_add(point.0 - prior_ts);
                }
            }
            previous = Some(*point);
        }
        carry = points.get(index - 1).copied();
        if elapsed > 0 {
            output.push((bucket, BucketValue::Real(delta / elapsed as f64)));
        }
    }
    output
}

fn execute_labels(
    conn: &Connection,
    table: MetricsTable,
    selectors: &[Selector],
) -> Result<ReadOutput, String> {
    let series = selected_series(conn, table, selectors)?;
    let mut names = BTreeSet::new();
    names.insert("__name__".to_string());
    for meta in &series {
        names.extend(meta.labels.keys().cloned());
    }
    success_array(names, 0, series.len() as u64)
}

fn execute_label_values(
    conn: &Connection,
    table: MetricsTable,
    name: &str,
    metric: Option<&str>,
    selectors: &[Selector],
) -> Result<ReadOutput, String> {
    let values = if !selectors.is_empty() {
        let series = selected_series(conn, table, selectors)?;
        let values = series
            .iter()
            .filter_map(|meta| {
                if name == "__name__" {
                    Some(meta.metric.clone())
                } else {
                    meta.labels.get(name).cloned()
                }
            })
            .collect::<BTreeSet<_>>();
        return success_array(values, 0, series.len() as u64);
    } else if name == "__name__" && metric.is_none() {
        all_metrics(conn, table)?.into_iter().collect()
    } else if let Some(metric) = metric {
        direct_label_values(conn, table, metric, name)?
    } else {
        let mut values = BTreeSet::new();
        for metric in all_metrics(conn, table)? {
            values.extend(direct_label_values(conn, table, &metric, name)?);
        }
        values
    };
    success_array(values, 0, 0)
}

fn direct_label_values(
    conn: &Connection,
    table: MetricsTable,
    metric: &str,
    name: &str,
) -> Result<BTreeSet<String>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT value FROM timeless_label_values('{}', ?1, ?2) ORDER BY value",
            table.name()
        ))
        .map_err(|error| format!("prepare label values: {error}"))?;
    let values = stmt
        .query_map(params![metric, name], |row| row.get(0))
        .map_err(|error| format!("query label values: {error}"))?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| format!("collect label values: {error}"))?;
    Ok(values)
}

fn execute_series(
    conn: &Connection,
    table: MetricsTable,
    metric: Option<&str>,
    selectors: &[Selector],
) -> Result<ReadOutput, String> {
    let mut series = if selectors.is_empty() {
        catalog(
            conn,
            table,
            metric.expect("native series requires metric"),
            &FilterPlan::new(Vec::new()),
        )?
    } else {
        selected_series(conn, table, selectors)?
    };
    series.sort_by(|left, right| {
        left.metric
            .cmp(&right.metric)
            .then(left.labels_json.cmp(&right.labels_json))
            .then(left.id.cmp(&right.id))
    });
    let mut body = Vec::new();
    body.extend_from_slice(br#"{"status":"success","data":["#);
    for (index, meta) in series.iter().enumerate() {
        comma(&mut body, index);
        if selectors.is_empty() {
            body.extend_from_slice(b"{\"labels\":");
            body.extend_from_slice(meta.labels_json.as_bytes());
            body.push(b'}');
        } else {
            let mut labels = meta.labels.clone();
            labels.insert("__name__".into(), meta.metric.clone());
            write_json(&mut body, &labels)?;
        }
    }
    body.extend_from_slice(b"]}");
    Ok(ReadOutput {
        body,
        frame_bytes: 0,
        series: series.len() as u64,
        points: 0,
        rows: series.len() as u64,
    })
}

fn selected_series(
    conn: &Connection,
    table: MetricsTable,
    selectors: &[Selector],
) -> Result<Vec<SeriesMeta>, String> {
    if selectors.is_empty() {
        return catalog_all(conn, table);
    }
    let mut all_metric_names = None;
    let mut unique = BTreeMap::<(String, String), SeriesMeta>::new();
    for selector in selectors {
        if let MetricSelection::Exact(metric) = &selector.metric {
            for meta in catalog(conn, table, metric, &selector.filter)? {
                unique.insert((meta.metric.clone(), meta.labels_json.clone()), meta);
            }
            continue;
        }
        let metrics = match &all_metric_names {
            Some(metrics) => metrics,
            None => all_metric_names.insert(all_metrics(conn, table)?),
        };
        for metric in metrics {
            let selected = match &selector.metric {
                MetricSelection::Regex(regex) => regex.is_match(metric),
                MetricSelection::All => true,
                MetricSelection::Exact(_) => unreachable!("handled above"),
            };
            if selected {
                for meta in catalog(conn, table, metric, &selector.filter)? {
                    unique.insert((meta.metric.clone(), meta.labels_json.clone()), meta);
                }
            }
        }
    }
    Ok(unique.into_values().collect())
}

fn success_array<T: Serialize + Ord>(
    values: BTreeSet<T>,
    frame_bytes: usize,
    series: u64,
) -> Result<ReadOutput, String> {
    let rows = values.len() as u64;
    let body = serde_json::to_vec(&json!({"status": "success", "data": values}))
        .map_err(|error| format!("encode discovery response: {error}"))?;
    Ok(ReadOutput {
        body,
        frame_bytes,
        series,
        points: 0,
        rows,
    })
}

fn decode_latest_frame(bytes: &[u8]) -> Result<Vec<LatestRow>, String> {
    if bytes.len() < 8 || &bytes[..4] != b"TLF1" {
        return Err("timeless_latest_frame returned an unknown or truncated frame".into());
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().expect("four bytes")) as usize;
    let bitmap_len = count
        .checked_add(7)
        .map(|value| value / 8)
        .ok_or_else(|| "latest frame bitmap overflow".to_string())?;
    let expected = 8usize
        .checked_add(
            count
                .checked_mul(24)
                .ok_or_else(|| "latest frame column overflow".to_string())?,
        )
        .and_then(|size| size.checked_add(bitmap_len))
        .ok_or_else(|| "latest frame length overflow".to_string())?;
    if bytes.len() != expected {
        return Err(format!(
            "timeless_latest_frame returned {} bytes, expected {expected}",
            bytes.len()
        ));
    }
    let ids = 8;
    let timestamps = ids + count * 8;
    let bitmap = timestamps + count * 8;
    let values = bitmap + bitmap_len;
    validate_bitmap(&bytes[bitmap..values], count, "TLF1")?;
    let mut output = Vec::with_capacity(count);
    for index in 0..count {
        let valid = bit(&bytes[bitmap..values], index);
        let word = u64_at(bytes, values + index * 8);
        let value = if valid {
            let value = f64::from_bits(word);
            if value.is_nan() {
                return Err(format!("TLF1 valid value {index} is NaN"));
            }
            Some(value)
        } else {
            if word != 0 {
                return Err(format!("TLF1 null value {index} has nonzero bits"));
            }
            None
        };
        output.push(LatestRow {
            id: i64_at(bytes, ids + index * 8),
            timestamp: i64_at(bytes, timestamps + index * 8),
            value,
        });
    }
    Ok(output)
}

fn decode_raw_frame(bytes: &[u8]) -> Result<Vec<RawSeries>, String> {
    if bytes.len() < 16 || &bytes[..4] != b"TRF1" {
        return Err("timeless_raw_frame returned an unknown or truncated frame".into());
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().expect("four bytes")) as usize;
    let total_u64 = u64::from_le_bytes(bytes[8..16].try_into().expect("eight bytes"));
    let total = usize::try_from(total_u64).map_err(|_| "raw frame point count overflow")?;
    let expected = 16usize
        .checked_add(
            count
                .checked_mul(12)
                .ok_or_else(|| "raw frame series column overflow".to_string())?,
        )
        .and_then(|size| size.checked_add(total.checked_mul(16)?))
        .ok_or_else(|| "raw frame length overflow".to_string())?;
    if bytes.len() != expected {
        return Err(format!(
            "timeless_raw_frame returned {} bytes, expected {expected}",
            bytes.len()
        ));
    }
    let ids = 16;
    let counts = ids + count * 8;
    let timestamps = counts + count * 4;
    let values = timestamps + total * 8;
    let mut offset = 0_usize;
    let mut output = Vec::with_capacity(count);
    for index in 0..count {
        let points = u32::from_le_bytes(
            bytes[counts + index * 4..counts + index * 4 + 4]
                .try_into()
                .expect("four bytes"),
        ) as usize;
        let end = offset
            .checked_add(points)
            .filter(|end| *end <= total)
            .ok_or_else(|| "raw frame per-series counts exceed total".to_string())?;
        output.push(RawSeries {
            id: i64_at(bytes, ids + index * 8),
            data: RawSeriesData::Frame {
                timestamps_start: timestamps + offset * 8,
                values_start: values + offset * 8,
                count: points,
            },
        });
        offset = end;
    }
    if offset != total {
        return Err("raw frame per-series counts do not equal total".into());
    }
    Ok(output)
}

struct WindowBatch<'a> {
    bytes: &'a [u8],
    count: usize,
    timestamps_start: usize,
    bitmap_start: usize,
    values_start: usize,
}

impl WindowBatch<'_> {
    fn len(&self) -> usize {
        self.count
    }

    fn timestamp(&self, index: usize) -> i64 {
        debug_assert!(index < self.count);
        i64_at(self.bytes, self.timestamps_start + index * 8)
    }

    fn value(&self, index: usize) -> Option<f64> {
        debug_assert!(index < self.count);
        if bit(&self.bytes[self.bitmap_start..self.values_start], index) {
            Some(f64::from_bits(u64_at(
                self.bytes,
                self.values_start + index * 8,
            )))
        } else {
            None
        }
    }
}

fn decode_window_batch(bytes: &[u8]) -> Result<WindowBatch<'_>, String> {
    if bytes.len() < 8 || &bytes[..4] != b"TWB1" {
        return Err("timeless_window_batches returned an unknown or truncated frame".into());
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().expect("four bytes")) as usize;
    let bitmap_len = count
        .checked_add(7)
        .map(|value| value / 8)
        .ok_or_else(|| "window batch bitmap overflow".to_string())?;
    let expected = 8usize
        .checked_add(
            count
                .checked_mul(16)
                .ok_or_else(|| "window batch column overflow".to_string())?,
        )
        .and_then(|size| size.checked_add(bitmap_len))
        .ok_or_else(|| "window batch length overflow".to_string())?;
    if bytes.len() != expected {
        return Err(format!(
            "timeless_window_batches returned {} bytes, expected {expected}",
            bytes.len()
        ));
    }
    let timestamps_start = 8;
    let bitmap_start = timestamps_start + count * 8;
    let values_start = bitmap_start + bitmap_len;
    validate_bitmap(&bytes[bitmap_start..values_start], count, "TWB1")?;
    for index in 0..count {
        let word = u64_at(bytes, values_start + index * 8);
        if bit(&bytes[bitmap_start..values_start], index) {
            let value = f64::from_bits(word);
            if value.is_nan() {
                return Err(format!("TWB1 valid value {index} is NaN"));
            }
        } else {
            if word != 0 {
                return Err(format!("TWB1 null value {index} has nonzero bits"));
            }
        }
    }
    Ok(WindowBatch {
        bytes,
        count,
        timestamps_start,
        bitmap_start,
        values_start,
    })
}

fn validate_bitmap(bitmap: &[u8], count: usize, name: &str) -> Result<(), String> {
    let used = count & 7;
    if used != 0 && bitmap.last().copied().unwrap_or(0) & !((1 << used) - 1) != 0 {
        return Err(format!("{name} has nonzero bitmap padding bits"));
    }
    Ok(())
}

fn bit(bitmap: &[u8], index: usize) -> bool {
    bitmap[index / 8] & (1 << (index % 8)) != 0
}

fn i64_at(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("validated frame"),
    )
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("validated frame"),
    )
}

fn write_json<T: ?Sized + Serialize>(output: &mut Vec<u8>, value: &T) -> Result<(), String> {
    serde_json::to_writer(output, value).map_err(|error| format!("encode query response: {error}"))
}

fn write_optional_float(output: &mut Vec<u8>, value: Option<f64>) -> Result<(), String> {
    match value {
        Some(value) => write_float(output, value),
        None => {
            output.extend_from_slice(b"null");
            Ok(())
        }
    }
}

fn write_float(output: &mut Vec<u8>, value: f64) -> Result<(), String> {
    if value.is_finite() {
        write_json(output, &value)
    } else {
        output.extend_from_slice(b"null");
        Ok(())
    }
}

fn comma(output: &mut Vec<u8>, index: usize) {
    if index > 0 {
        output.push(b',');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merged_params_keep_repeated_selectors_and_body_wins() {
        let params = Params::parse(
            Some("metric=a&match%5B%5D=a%7Bx%3D%221%22%7D&metric=b"),
            b"metric=c&match%5B%5D=d%7By%3D~%22z.%2A%22%7D",
        );
        assert_eq!(params.get("metric"), Some("c"));
        assert_eq!(
            params.all(&["match[]"]),
            vec!["a{x=\"1\"}", "d{y=~\"z.*\"}"]
        );
    }

    #[test]
    fn unsupported_parameters_and_native_aggregates_fail_instead_of_broadening() {
        let instant = Params::parse(Some("query=cpu&silently_ignored=true"), b"");
        assert!(prometheus_instant_request(&instant).is_err());

        let labels = Params::parse(Some("start=1700000000"), b"");
        assert!(labels_request(&labels).is_err());

        let range = Params::parse(
            Some("metric=cpu&start=1&end=2&step=1&aggregate=median"),
            b"",
        );
        assert!(range_request(&range).is_err());
    }

    #[test]
    fn selector_supports_duplicates_missing_labels_and_metric_regex() {
        let selector =
            parse_selector(r#"{__name__=~"http_.*",env!="dev",host=~"web-.*",host!="web-9"}"#)
                .unwrap();
        assert!(matches!(selector.metric, MetricSelection::Regex(_)));
        let labels = BTreeMap::from([
            ("env".into(), "prod".into()),
            ("host".into(), "web-1".into()),
        ]);
        assert!(selector.filter.matches(&labels));
        assert!(!selector.filter.matches(&BTreeMap::new()));
    }

    #[test]
    fn frame_decoders_reject_unknown_and_inconsistent_envelopes() {
        assert!(decode_latest_frame(b"TLF0\0\0\0\0").is_err());
        let mut raw = Vec::new();
        raw.extend_from_slice(b"TRF1");
        raw.extend_from_slice(&1_u32.to_le_bytes());
        raw.extend_from_slice(&1_u64.to_le_bytes());
        raw.extend_from_slice(&9_i64.to_le_bytes());
        raw.extend_from_slice(&0_u32.to_le_bytes());
        raw.extend_from_slice(&10_i64.to_le_bytes());
        raw.extend_from_slice(&1.5_f64.to_bits().to_le_bytes());
        assert!(decode_raw_frame(&raw).is_err());
    }

    #[test]
    fn raw_first_and_last_keep_the_first_value_at_duplicate_extreme_timestamps() {
        let series = RawSeries {
            id: 1,
            data: RawSeriesData::Owned {
                timestamps: vec![10, 10, 20, 20],
                values: vec![1.0, 2.0, 3.0, 4.0],
            },
        };
        let first = aggregate_raw(&series, None, 0, 60, Aggregate::First).unwrap();
        let last = aggregate_raw(&series, None, 0, 60, Aggregate::Last).unwrap();
        assert!(matches!(first[0].1, BucketValue::Real(1.0)));
        assert!(matches!(last[0].1, BucketValue::Real(3.0)));
    }

    #[test]
    fn prometheus_time_and_duration_inputs_use_a_millisecond_clock() {
        assert_eq!(parse_prom_time(None, 42).unwrap(), 42);
        assert_eq!(parse_prom_time(Some("2.0005"), 0).unwrap(), 2_001);
        assert_eq!(
            parse_prom_time(Some("2023-11-14T22:13:20.125Z"), 0).unwrap(),
            1_700_000_000_125
        );
        assert!(parse_prom_time(Some("not-a-time"), 0).is_err());
        assert!(parse_prom_time(Some("NaN"), 0).is_err());

        assert_eq!(parse_prom_step(Some("1h30m250ms"), 0).unwrap(), 5_400_250);
        assert_eq!(parse_prom_step(Some("0.5"), 0).unwrap(), 500);
        assert!(parse_prom_step(Some("0"), 0).is_err());
        assert!(parse_prom_step(Some("-1"), 0).is_err());
        assert!(parse_prom_step(Some("NaN"), 0).is_err());
        assert!(parse_prom_step(Some("1m1h"), 0).is_err());

        assert_eq!(parse_prom_lookback(None, 300_000).unwrap(), 300_000);
        assert_eq!(parse_prom_lookback(Some("0"), 300_000).unwrap(), 300_000);
        assert_eq!(parse_prom_lookback(Some("0s"), 300_000).unwrap(), 300_000);
        assert_eq!(parse_prom_lookback(Some("1501ms"), 300_000).unwrap(), 1_501);
        assert!(parse_prom_lookback(Some("1.5s"), 300_000).is_err());
    }

    #[test]
    fn prometheus_timestamps_are_exact_seconds_with_optional_milliseconds() {
        for (timestamp, expected) in [
            (1_700_000_000_000, "1700000000"),
            (1_700_000_000_500, "1700000000.5"),
            (1_700_000_000_125, "1700000000.125"),
            (-500, "-0.5"),
            (i64::MIN, "-9223372036854775.808"),
        ] {
            let mut output = Vec::new();
            write_prometheus_timestamp(&mut output, timestamp);
            assert_eq!(String::from_utf8(output).unwrap(), expected);
        }
    }

    #[test]
    fn prometheus_grid_size_rejects_extreme_ranges_without_overflow() {
        let params = Params::parse(
            Some("query=1&start=-9223372036854775.808&end=9223372036854775.807&step=0.001"),
            b"",
        );
        let error = prometheus_range_request(&params).unwrap_err();
        assert!(error.contains("maximum resolution"), "{error}");
    }
}
