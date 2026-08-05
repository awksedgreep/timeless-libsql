use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use regex::Regex;
use rusqlite::{params, Connection, OptionalExtension};

use crate::PromQueryLimits;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
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
        self.matches_value(actual)
    }

    fn matches_value(&self, actual: &str) -> bool {
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
    Count,
    Sum,
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
    timing: SelectorTiming,
}

#[derive(Clone, Debug)]
enum MetricSelection {
    Exact(String),
    Regex(Regex),
    Matchers(Vec<Matcher>),
    All,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SelectorTiming {
    offset_ms: i64,
    at: Option<SelectorAt>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectorAt {
    Start,
    End,
    Timestamp(i64),
}

impl SelectorTiming {
    fn lower(
        offset: Option<promql::Offset>,
        at: Option<promql::AtModifier>,
    ) -> Result<Self, String> {
        let offset_ms = match offset {
            None => 0,
            Some(promql::Offset::Pos(duration)) => duration_millis_i64(duration, "offset")?,
            Some(promql::Offset::Neg(duration)) => duration_millis_i64(duration, "offset")?
                .checked_neg()
                .ok_or_else(|| "PromQL negative offset overflow".to_string())?,
        };
        let at = match at {
            None => None,
            Some(promql::AtModifier::Start) => Some(SelectorAt::Start),
            Some(promql::AtModifier::End) => Some(SelectorAt::End),
            Some(promql::AtModifier::At(timestamp)) => {
                Some(SelectorAt::Timestamp(system_time_millis(timestamp)?))
            }
        };
        Ok(Self { offset_ms, at })
    }

    fn is_default(self) -> bool {
        self == Self::default()
    }

    fn selection_time(self, outer: i64, query_start: i64, query_end: i64) -> Result<i64, String> {
        let anchor = match self.at {
            None => outer,
            Some(SelectorAt::Start) => query_start,
            Some(SelectorAt::End) => query_end,
            Some(SelectorAt::Timestamp(timestamp)) => timestamp,
        };
        anchor
            .checked_sub(self.offset_ms)
            .ok_or_else(|| "PromQL timestamp overflow while applying offset".to_string())
    }
}

fn duration_millis_i64(duration: std::time::Duration, name: &str) -> Result<i64, String> {
    i64::try_from(duration.as_millis()).map_err(|_| format!("PromQL {name} duration overflow"))
}

fn system_time_millis(timestamp: SystemTime) -> Result<i64, String> {
    match timestamp.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_millis_i64(duration, "@ timestamp"),
        Err(error) => duration_millis_i64(error.duration(), "@ timestamp")?
            .checked_neg()
            .ok_or_else(|| "PromQL negative @ timestamp overflow".to_string()),
    }
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
        query: String,
        plan: PromPlan,
        start: i64,
        stop: i64,
        step: i64,
        instant: bool,
        limits: PromQueryLimits,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum PromPlan {
    Scalar(f64),
    String(String),
    Unary(Box<PromPlan>),
    Function(PromFunctionPlan),
    LabelReplace(PromLabelReplacePlan),
    LabelJoin(PromLabelJoinPlan),
    Absent(PromAbsentPlan),
    Sort(PromSortPlan),
    Conversion(PromConversionPlan),
    Time,
    Timestamp(PromTimestampPlan),
    Calendar(PromCalendarPlan),
    HistogramQuantile(PromHistogramQuantilePlan),
    Binary(PromBinaryPlan),
    Aggregate(PromAggregatePlan),
    Selector { selector: Selector, lookback: i64 },
    RangeReduction(PromRangePlan),
    RangeSelector { selector: Selector, window: i64 },
    Subquery(SubqueryPlan),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromFunctionOp {
    Abs,
    Acos,
    Acosh,
    Asin,
    Asinh,
    Atan,
    Atanh,
    Ceil,
    Clamp,
    ClampMax,
    ClampMin,
    Cos,
    Cosh,
    Deg,
    Exp,
    Floor,
    Ln,
    Log2,
    Log10,
    Rad,
    Round,
    Sgn,
    Sin,
    Sinh,
    Sqrt,
    Tan,
    Tanh,
}

impl PromFunctionOp {
    fn apply(self, value: f64, parameters: &[f64]) -> Option<f64> {
        match self {
            Self::Abs => Some(value.abs()),
            Self::Acos => Some(value.acos()),
            Self::Acosh => Some(value.acosh()),
            Self::Asin => Some(value.asin()),
            Self::Asinh => Some(value.asinh()),
            Self::Atan => Some(value.atan()),
            Self::Atanh => Some(value.atanh()),
            Self::Ceil => Some(value.ceil()),
            Self::Clamp => {
                let minimum = parameters[0];
                let maximum = parameters[1];
                (minimum <= maximum)
                    .then(|| prometheus_math_max(minimum, prometheus_math_min(maximum, value)))
            }
            Self::ClampMax => Some(prometheus_math_min(parameters[0], value)),
            Self::ClampMin => Some(prometheus_math_max(parameters[0], value)),
            Self::Cos => Some(value.cos()),
            Self::Cosh => Some(value.cosh()),
            Self::Deg => Some(value * 180.0 / std::f64::consts::PI),
            Self::Exp => Some(value.exp()),
            Self::Floor => Some(value.floor()),
            Self::Ln => Some(value.ln()),
            Self::Log2 => Some(value.log2()),
            Self::Log10 => Some(value.log10()),
            Self::Rad => Some(value * std::f64::consts::PI / 180.0),
            Self::Round => {
                let inverse = 1.0 / parameters.first().copied().unwrap_or(1.0);
                Some((value * inverse + 0.5).floor() / inverse)
            }
            Self::Sgn => Some(if value > 0.0 {
                1.0
            } else if value < 0.0 {
                -1.0
            } else {
                value
            }),
            Self::Sin => Some(value.sin()),
            Self::Sinh => Some(value.sinh()),
            Self::Sqrt => Some(value.sqrt()),
            Self::Tan => Some(value.tan()),
            Self::Tanh => Some(value.tanh()),
        }
    }
}

fn prometheus_math_min(lhs: f64, rhs: f64) -> f64 {
    if lhs.is_nan() || rhs.is_nan() {
        f64::NAN
    } else if lhs == 0.0 && rhs == 0.0 {
        if lhs.is_sign_negative() || rhs.is_sign_negative() {
            -0.0
        } else {
            0.0
        }
    } else if lhs < rhs {
        lhs
    } else {
        rhs
    }
}

fn prometheus_math_max(lhs: f64, rhs: f64) -> f64 {
    if lhs.is_nan() || rhs.is_nan() {
        f64::NAN
    } else if lhs == 0.0 && rhs == 0.0 {
        if lhs.is_sign_positive() || rhs.is_sign_positive() {
            0.0
        } else {
            -0.0
        }
    } else if lhs > rhs {
        lhs
    } else {
        rhs
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PromFunctionPlan {
    op: PromFunctionOp,
    inner: Box<PromPlan>,
    parameters: Vec<PromPlan>,
}

#[derive(Clone, Debug)]
pub(crate) struct PromLabelReplacePlan {
    inner: Box<PromPlan>,
    destination: String,
    replacement: String,
    source: String,
    pattern: String,
}

#[derive(Clone, Debug)]
pub(crate) struct PromLabelJoinPlan {
    inner: Box<PromPlan>,
    destination: String,
    separator: String,
    sources: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct PromAbsentPlan {
    inner: Box<PromPlan>,
    labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub(crate) struct PromSortPlan {
    inner: Box<PromPlan>,
    descending: bool,
    source: Option<PromSourceCall>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromConversionKind {
    Scalar,
    Vector,
}

#[derive(Clone, Debug)]
pub(crate) struct PromConversionPlan {
    inner: Box<PromPlan>,
    kind: PromConversionKind,
}

#[derive(Clone, Debug)]
pub(crate) struct PromTimestampPlan {
    inner: Box<PromPlan>,
}

#[derive(Clone, Debug)]
pub(crate) struct PromHistogramQuantilePlan {
    quantile: Box<PromPlan>,
    inner: Box<PromPlan>,
    source: Option<PromSourceCall>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromCalendarOp {
    Minute,
    Hour,
    DayOfWeek,
    DayOfMonth,
    DayOfYear,
    DaysInMonth,
    Month,
    Year,
}

impl PromCalendarOp {
    fn apply(self, value: f64) -> f64 {
        // Prometheus converts every non-finite or out-of-i64-range input to
        // the same maximum Unix second before extracting a UTC calendar
        // component. In-range finite values are truncated toward zero.
        let seconds = if value.is_finite() && value > i64::MIN as f64 && value < -(i64::MIN as f64)
        {
            value as i64
        } else {
            i64::MAX
        };
        let second_of_day = seconds.rem_euclid(86_400);
        match self {
            Self::Minute => ((second_of_day / 60) % 60) as f64,
            Self::Hour => (second_of_day / 3_600) as f64,
            Self::DayOfWeek => (seconds.div_euclid(86_400) + 4).rem_euclid(7) as f64,
            Self::DayOfMonth => {
                let (_, _, day) = prometheus_utc_civil_date(seconds);
                day as f64
            }
            Self::DayOfYear => {
                let (year, month, day) = prometheus_utc_civil_date(seconds);
                prometheus_day_of_year(year, month, day) as f64
            }
            Self::DaysInMonth => {
                let (year, month, _) = prometheus_utc_civil_date(seconds);
                prometheus_days_in_month(year, month) as f64
            }
            Self::Month => {
                let (_, month, _) = prometheus_utc_civil_date(seconds);
                month as f64
            }
            Self::Year => {
                let (year, _, _) = prometheus_utc_civil_date(seconds);
                year as f64
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PromCalendarPlan {
    inner: Box<PromPlan>,
    op: PromCalendarOp,
}

fn prometheus_utc_civil_date(seconds: i64) -> (i128, u8, u8) {
    // Howard Hinnant's civil-from-days algorithm, evaluated with i128 so the
    // entire i64 Unix-second domain remains defined (including Prometheus's
    // non-finite sentinel at i64::MAX).
    let z = i128::from(seconds.div_euclid(86_400)) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month as u8, day as u8)
}

fn prometheus_is_leap_year(year: i128) -> bool {
    year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0)
}

fn prometheus_days_in_month(year: i128, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if prometheus_is_leap_year(year) => 29,
        2 => 28,
        _ => unreachable!("civil-date month is always 1 through 12"),
    }
}

fn prometheus_day_of_year(year: i128, month: u8, day: u8) -> u16 {
    const DAYS_BEFORE_MONTH: [u16; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    DAYS_BEFORE_MONTH[usize::from(month - 1)]
        + u16::from(day)
        + u16::from(month > 2 && prometheus_is_leap_year(year))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromRangeOp {
    Avg,
    Min,
    Max,
    Sum,
    Count,
    Present,
    Quantile,
    StdDev,
    StdVar,
    Rate,
    IRate,
    Increase,
    Delta,
    IDelta,
    Deriv,
    PredictLinear,
    Changes,
    Resets,
    Last,
}

impl PromRangeOp {
    fn name(self) -> &'static str {
        match self {
            Self::Avg => "avg_over_time",
            Self::Min => "min_over_time",
            Self::Max => "max_over_time",
            Self::Sum => "sum_over_time",
            Self::Count => "count_over_time",
            Self::Present => "present_over_time",
            Self::Quantile => "quantile_over_time",
            Self::StdDev => "stddev_over_time",
            Self::StdVar => "stdvar_over_time",
            Self::Rate => "rate",
            Self::IRate => "irate",
            Self::Increase => "increase",
            Self::Delta => "delta",
            Self::IDelta => "idelta",
            Self::Deriv => "deriv",
            Self::PredictLinear => "predict_linear",
            Self::Changes => "changes",
            Self::Resets => "resets",
            Self::Last => "last_over_time",
        }
    }

    fn native_name(self) -> Option<&'static str> {
        match self {
            Self::Avg => Some("avg"),
            Self::Min => Some("min"),
            Self::Max => Some("max"),
            Self::Sum => Some("sum"),
            Self::Count => Some("count"),
            Self::Present => Some("count"),
            Self::Quantile
            | Self::StdDev
            | Self::StdVar
            | Self::Rate
            | Self::IRate
            | Self::Increase
            | Self::Delta
            | Self::IDelta
            | Self::Deriv
            | Self::PredictLinear
            | Self::Changes
            | Self::Resets
            | Self::Last => None,
        }
    }

    fn aggregate_op(self) -> Option<PromAggregateOp> {
        match self {
            Self::Avg => Some(PromAggregateOp::Avg),
            Self::Min => Some(PromAggregateOp::Min),
            Self::Max => Some(PromAggregateOp::Max),
            Self::Sum => Some(PromAggregateOp::Sum),
            Self::Count => Some(PromAggregateOp::Count),
            Self::StdDev => Some(PromAggregateOp::StdDev),
            Self::StdVar => Some(PromAggregateOp::StdVar),
            Self::Present
            | Self::Quantile
            | Self::Rate
            | Self::IRate
            | Self::Increase
            | Self::Delta
            | Self::IDelta
            | Self::Deriv
            | Self::PredictLinear
            | Self::Changes
            | Self::Resets
            | Self::Last => None,
        }
    }

    fn retains_metric_name(self) -> bool {
        matches!(self, Self::Last)
    }
}

#[derive(Clone, Debug)]
enum PromRangeInput {
    Selector { selector: Selector, window: i64 },
    Subquery(SubqueryPlan),
}

#[derive(Clone, Debug)]
pub(crate) struct PromRangePlan {
    op: PromRangeOp,
    input: PromRangeInput,
    parameter: Option<Box<PromPlan>>,
    source: Option<PromSourceCall>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromValueType {
    Scalar,
    String,
    Vector,
    Matrix,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromAggregateOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Group,
    StdDev,
    StdVar,
    TopK,
    BottomK,
    Quantile,
    CountValues,
}

#[derive(Clone, Debug, Default)]
enum PromAggregateGrouping {
    #[default]
    All,
    By(BTreeSet<String>),
    Without(BTreeSet<String>),
}

#[derive(Clone, Debug)]
pub(crate) struct PromAggregatePlan {
    op: PromAggregateOp,
    inner: Box<PromPlan>,
    param: Option<Box<PromPlan>>,
    value_label: Option<String>,
    grouping: PromAggregateGrouping,
    source: Option<PromSourceCall>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct PromSourceCall {
    start: usize,
    arguments: Vec<usize>,
}

impl PromSourceCall {
    fn argument(&self, index: usize) -> usize {
        self.arguments.get(index).copied().unwrap_or(self.start)
    }
}

#[derive(Debug)]
enum PromAnnotation {
    Generic {
        raw: String,
        position: usize,
    },
    HistogramMonotonicity {
        raw: String,
        position: usize,
        min_timestamp_ms: i64,
        max_timestamp_ms: i64,
        min_bucket: f64,
        max_bucket: f64,
        max_diff: f64,
        samples: u64,
    },
}

impl PromAnnotation {
    fn render(&self, query: &str) -> String {
        match self {
            Self::Generic { raw, position } => {
                format!("{raw} ({})", promql_source_position(query, *position))
            }
            Self::HistogramMonotonicity {
                raw,
                position,
                min_timestamp_ms,
                max_timestamp_ms,
                min_bucket,
                max_bucket,
                max_diff,
                samples,
            } => format!(
                "{raw}, from buckets {} to {}, with a max diff of {}, over {samples} samples from {} to {} ({})",
                format_prometheus_annotation_float(*min_bucket),
                format_prometheus_annotation_float(*max_bucket),
                format_prometheus_annotation_float_precision_2(*max_diff),
                format_prometheus_annotation_timestamp(*min_timestamp_ms),
                format_prometheus_annotation_timestamp(*max_timestamp_ms),
                promql_source_position(query, *position),
            ),
        }
    }
}

#[derive(Default)]
struct PromAnnotations {
    warnings: BTreeMap<String, PromAnnotation>,
    infos: BTreeMap<String, PromAnnotation>,
}

impl PromAnnotations {
    fn warning(&mut self, raw: String, position: usize) {
        self.warnings
            .insert(raw.clone(), PromAnnotation::Generic { raw, position });
    }

    fn info(&mut self, raw: String, position: usize) {
        self.infos
            .insert(raw.clone(), PromAnnotation::Generic { raw, position });
    }

    fn invalid_quantile(&mut self, quantile: f64, position: usize) {
        self.warning(
            format!(
                "PromQL warning: quantile value should be between 0 and 1, got {}",
                format_prometheus_annotation_float(quantile)
            ),
            position,
        );
    }

    fn bad_bucket_label(&mut self, label: &str, position: usize) {
        self.warning(
            format!(
                "PromQL warning: bucket label \"le\" is missing or has a malformed value of {}",
                prometheus_annotation_quote(label)
            ),
            position,
        );
    }

    fn possible_non_counter(&mut self, metric: &str, position: usize) {
        self.info(
            format!(
                "PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: {}",
                prometheus_annotation_quote(metric)
            ),
            position,
        );
    }

    fn histogram_monotonicity(
        &mut self,
        position: usize,
        timestamp_ms: i64,
        min_bucket: f64,
        max_bucket: f64,
        max_diff: f64,
    ) {
        let raw = "PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile)".to_string();
        match self.infos.get_mut(&raw) {
            Some(PromAnnotation::HistogramMonotonicity {
                position: existing_position,
                min_timestamp_ms,
                max_timestamp_ms,
                min_bucket: existing_min_bucket,
                max_bucket: existing_max_bucket,
                max_diff: existing_max_diff,
                samples,
                ..
            }) => {
                *existing_position = position;
                *min_timestamp_ms = (*min_timestamp_ms).min(timestamp_ms);
                *max_timestamp_ms = (*max_timestamp_ms).max(timestamp_ms);
                *existing_min_bucket = existing_min_bucket.min(min_bucket);
                *existing_max_bucket = existing_max_bucket.max(max_bucket);
                *existing_max_diff = existing_max_diff.max(max_diff);
                *samples = samples.saturating_add(1);
            }
            _ => {
                self.infos.insert(
                    raw.clone(),
                    PromAnnotation::HistogramMonotonicity {
                        raw,
                        position,
                        min_timestamp_ms: timestamp_ms,
                        max_timestamp_ms: timestamp_ms,
                        min_bucket,
                        max_bucket,
                        max_diff,
                        samples: 1,
                    },
                );
            }
        }
    }

    fn append_to_success(
        &self,
        query: &str,
        output: &mut ReadOutput,
        limits: PromQueryLimits,
    ) -> Result<(), String> {
        if self.warnings.is_empty() && self.infos.is_empty() {
            return Ok(());
        }
        if output.body.pop() != Some(b'}') {
            return Err("Prometheus success envelope is missing its final object delimiter".into());
        }
        if !self.warnings.is_empty() {
            output.body.extend_from_slice(b",\"warnings\":");
            write_json(
                &mut output.body,
                &render_prometheus_annotations(&self.warnings, query, "warning"),
            )?;
        }
        if !self.infos.is_empty() {
            output.body.extend_from_slice(b",\"infos\":");
            write_json(
                &mut output.body,
                &render_prometheus_annotations(&self.infos, query, "info"),
            )?;
        }
        output.body.push(b'}');
        enforce_prometheus_output(&output.body, output.points, limits)
    }
}

fn render_prometheus_annotations(
    annotations: &BTreeMap<String, PromAnnotation>,
    query: &str,
    level: &str,
) -> Vec<String> {
    const MAX_ANNOTATIONS: usize = 10;
    let mut rendered: Vec<String> = annotations
        .values()
        .take(MAX_ANNOTATIONS)
        .map(|annotation| annotation.render(query))
        .collect();
    if annotations.len() > MAX_ANNOTATIONS {
        rendered.push(format!(
            "{} more {level} annotations omitted",
            annotations.len() - MAX_ANNOTATIONS
        ));
    }
    rendered
}

fn promql_source_position(query: &str, position: usize) -> String {
    if query.is_empty() {
        return "unknown position".into();
    }
    if position > query.len() || !query.is_char_boundary(position) {
        return "invalid position".into();
    }
    let prefix = &query[..position];
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let line_start = prefix.rfind('\n').map_or(0, |index| index + 1);
    format!("{line}:{}", position - line_start + 1)
}

fn prometheus_annotation_quote(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        match character {
            '\u{0007}' => output.push_str("\\a"),
            '\u{0008}' => output.push_str("\\b"),
            '\u{000c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{000b}' => output.push_str("\\v"),
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            character if character <= '\u{001f}' || character == '\u{007f}' => {
                output.push_str(&format!("\\x{:02x}", character as u32));
            }
            character if character.is_control() => {
                let value = character as u32;
                if value <= u16::MAX as u32 {
                    output.push_str(&format!("\\u{value:04x}"));
                } else {
                    output.push_str(&format!("\\U{value:08x}"));
                }
            }
            character => output.push(character),
        }
    }
    output.push('"');
    output
}

fn format_prometheus_annotation_float(value: f64) -> String {
    format_prometheus_value(value)
}

fn format_prometheus_annotation_float_precision_2(value: f64) -> String {
    if !value.is_finite() || value == 0.0 {
        return format_prometheus_annotation_float(value);
    }
    let exponent = value.abs().log10().floor() as i32;
    if !(-4..2).contains(&exponent) {
        let rendered = format!("{value:.1e}");
        let (mantissa, exponent) = rendered
            .split_once('e')
            .expect("scientific format includes exponent");
        let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');
        let exponent: i32 = exponent.parse().expect("scientific exponent is decimal");
        format!("{mantissa}e{exponent:+03}")
    } else {
        let decimals = (1 - exponent).max(0) as usize;
        let rendered = format!("{value:.decimals$}");
        if rendered.contains('.') {
            rendered
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        } else {
            rendered
        }
    }
}

fn format_prometheus_annotation_timestamp(timestamp_ms: i64) -> String {
    let seconds = timestamp_ms / 1_000;
    let (year, month, day) = prometheus_utc_civil_date(seconds);
    let second_of_day = seconds.rem_euclid(86_400);
    let hour = second_of_day / 3_600;
    let minute = second_of_day / 60 % 60;
    let second = second_of_day % 60;
    if year >= 0 {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
    } else {
        format!(
            "-{:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z",
            -year
        )
    }
}

fn prometheus_counter_name(metric: &str) -> bool {
    ["_total", "_sum", "_count", "_bucket"]
        .iter()
        .any(|suffix| metric.ends_with(suffix))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromBinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
    Atan2,
    Equal,
    NotEqual,
    Greater,
    Less,
    GreaterOrEqual,
    LessOrEqual,
    And,
    Or,
    Unless,
}

#[derive(Clone, Debug, Default)]
enum PromVectorMatching {
    #[default]
    Default,
    On(BTreeSet<String>),
    Ignoring(BTreeSet<String>),
}

#[derive(Clone, Debug, Default)]
enum PromVectorCardinality {
    #[default]
    OneToOne,
    ManyToOne(BTreeSet<String>),
    OneToMany(BTreeSet<String>),
    ManyToMany,
}

impl PromVectorMatching {
    fn key(&self, labels: &BTreeMap<String, String>) -> PromMatchingKey {
        match self {
            Self::Default => labels
                .iter()
                .filter(|(name, _)| name.as_str() != "__name__")
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            Self::On(names) => names
                .iter()
                .map(|name| (name.clone(), labels.get(name).cloned().unwrap_or_default()))
                .collect(),
            Self::Ignoring(names) => labels
                .iter()
                .filter(|(name, _)| name.as_str() != "__name__" && !names.contains(*name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        }
    }

    fn project_one_to_one_result(&self, labels: &mut BTreeMap<String, String>) {
        match self {
            Self::Default => {}
            Self::On(names) => labels.retain(|name, _| names.contains(name)),
            Self::Ignoring(names) => labels.retain(|name, _| !names.contains(name)),
        }
    }
}

impl PromBinaryOp {
    fn is_arithmetic(self) -> bool {
        matches!(
            self,
            Self::Add
                | Self::Subtract
                | Self::Multiply
                | Self::Divide
                | Self::Modulo
                | Self::Power
                | Self::Atan2
        )
    }

    fn is_set(self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Unless)
    }

    fn evaluate(self, lhs: f64, rhs: f64, filter_value: f64, return_bool: bool) -> Option<f64> {
        match self {
            Self::Add => Some(lhs + rhs),
            Self::Subtract => Some(lhs - rhs),
            Self::Multiply => Some(lhs * rhs),
            Self::Divide => Some(lhs / rhs),
            Self::Modulo => Some(lhs % rhs),
            Self::Power => Some(lhs.powf(rhs)),
            Self::Atan2 => Some(prometheus_atan2(lhs, rhs)),
            comparison => {
                let matches = match comparison {
                    Self::Equal => lhs == rhs,
                    Self::NotEqual => lhs != rhs,
                    Self::Greater => lhs > rhs,
                    Self::Less => lhs < rhs,
                    Self::GreaterOrEqual => lhs >= rhs,
                    Self::LessOrEqual => lhs <= rhs,
                    Self::And | Self::Or | Self::Unless => {
                        unreachable!("set operators are evaluated over vectors")
                    }
                    _ => unreachable!("arithmetic handled above"),
                };
                if return_bool {
                    Some(f64::from(matches))
                } else {
                    matches.then_some(filter_value)
                }
            }
        }
    }
}

// Go's math.Atan2 and the host C libm do not always round the last bit the
// same way. Prometheus evaluates this operator with Go's Cephes-derived
// implementation, so keep the arithmetic local and deterministic.
fn prometheus_atan2(y: f64, x: f64) -> f64 {
    use std::f64::consts::{FRAC_PI_2, FRAC_PI_4, PI};

    if y.is_nan() || x.is_nan() {
        return f64::NAN;
    }
    if y == 0.0 {
        if x >= 0.0 && !x.is_sign_negative() {
            return 0.0_f64.copysign(y);
        }
        return PI.copysign(y);
    }
    if x == 0.0 {
        return FRAC_PI_2.copysign(y);
    }
    if x.is_infinite() {
        if x.is_sign_positive() {
            return if y.is_infinite() {
                FRAC_PI_4.copysign(y)
            } else {
                0.0_f64.copysign(y)
            };
        }
        return if y.is_infinite() {
            (3.0 * FRAC_PI_4).copysign(y)
        } else {
            PI.copysign(y)
        };
    }
    if y.is_infinite() {
        return FRAC_PI_2.copysign(y);
    }

    let quotient = prometheus_atan(y / x);
    if x < 0.0 {
        if quotient <= 0.0 {
            quotient + PI
        } else {
            quotient - PI
        }
    } else {
        quotient
    }
}

fn prometheus_atan(value: f64) -> f64 {
    fn reduced(value: f64) -> f64 {
        const MORE_BITS: f64 = 6.123_233_995_736_766e-17;
        const TAN_3_PI_8: f64 = 2.414_213_562_373_095;
        if value <= 0.66 {
            return series(value);
        }
        if value > TAN_3_PI_8 {
            return std::f64::consts::FRAC_PI_2 - series(1.0 / value) + MORE_BITS;
        }
        std::f64::consts::FRAC_PI_4 + series((value - 1.0) / (value + 1.0)) + 0.5 * MORE_BITS
    }

    fn series(value: f64) -> f64 {
        // These are the exact f64-rounded forms of Go's longer Cephes
        // coefficient spellings. Keeping only significant binary64 digits
        // avoids implying precision that is not present in either runtime.
        const P0: f64 = -8.750_608_600_031_904e-1;
        const P1: f64 = -1.615_753_718_733_365_2e1;
        const P2: f64 = -7.500_855_792_314_705e1;
        const P3: f64 = -1.228_866_684_490_136_1e2;
        const P4: f64 = -6.485_021_904_942_025e1;
        const Q0: f64 = 2.485_846_490_142_306_2e1;
        const Q1: f64 = 1.650_270_098_316_988_5e2;
        const Q2: f64 = 4.328_810_604_912_902_7e2;
        const Q3: f64 = 4.853_903_996_359_137e2;
        const Q4: f64 = 1.945_506_571_482_614e2;
        let square = value * value;
        let correction = square
            * ((((P0 * square + P1) * square + P2) * square + P3) * square + P4)
            / (((((square + Q0) * square + Q1) * square + Q2) * square + Q3) * square + Q4);
        value * correction + value
    }

    if value == 0.0 {
        value
    } else if value > 0.0 {
        reduced(value)
    } else {
        -reduced(-value)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PromBinaryPlan {
    op: PromBinaryOp,
    lhs: Box<PromPlan>,
    rhs: Box<PromPlan>,
    return_bool: bool,
    matching: PromVectorMatching,
    cardinality: PromVectorCardinality,
}

impl PromPlan {
    fn value_type(&self) -> PromValueType {
        match self {
            Self::Scalar(_) => PromValueType::Scalar,
            Self::String(_) => PromValueType::String,
            Self::RangeSelector { .. } | Self::Subquery(_) => PromValueType::Matrix,
            Self::Unary(inner) => inner.value_type(),
            Self::Function(_) => PromValueType::Vector,
            Self::LabelReplace(_) => PromValueType::Vector,
            Self::LabelJoin(_) => PromValueType::Vector,
            Self::Absent(_) => PromValueType::Vector,
            Self::Sort(_) => PromValueType::Vector,
            Self::Conversion(conversion) => match conversion.kind {
                PromConversionKind::Scalar => PromValueType::Scalar,
                PromConversionKind::Vector => PromValueType::Vector,
            },
            Self::Time => PromValueType::Scalar,
            Self::Timestamp(_) => PromValueType::Vector,
            Self::Calendar(_) => PromValueType::Vector,
            Self::HistogramQuantile(_) => PromValueType::Vector,
            Self::Aggregate(_) => PromValueType::Vector,
            Self::Binary(binary) => {
                if binary.lhs.value_type() == PromValueType::Scalar
                    && binary.rhs.value_type() == PromValueType::Scalar
                {
                    PromValueType::Scalar
                } else {
                    PromValueType::Vector
                }
            }
            Self::Selector { .. } | Self::RangeReduction(_) => PromValueType::Vector,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SubqueryPlan {
    inner: Box<PromPlan>,
    window: i64,
    resolution: Option<i64>,
    timing: SelectorTiming,
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

    pub(crate) fn with_prometheus_limits(
        mut self,
        limits: PromQueryLimits,
    ) -> Result<Self, String> {
        limits.validate()?;
        if let Self::Prometheus {
            start,
            stop,
            step,
            instant,
            limits: request_limits,
            ..
        } = &mut self
        {
            if !*instant {
                enforce_prometheus_grid(*start, *stop, *step, limits.max_points_per_series)?;
            }
            *request_limits = limits;
        }
        Ok(self)
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
        query: query.to_owned(),
        plan: lower_promql(query, lookback)
            .map_err(|error| format!("invalid parameter \"query\": {error}"))?,
        start: time,
        stop: time,
        step: 1_000,
        instant: true,
        limits: PromQueryLimits::default(),
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
    enforce_prometheus_grid(
        start,
        stop,
        step,
        PromQueryLimits::default().max_points_per_series,
    )?;
    let plan = lower_promql(query, lookback)
        .map_err(|error| format!("invalid parameter \"query\": {error}"))?;
    match plan.value_type() {
        PromValueType::Matrix => {
            return Err("invalid parameter \"query\": invalid expression type \"range vector\" for range query, must be Scalar or instant Vector".into());
        }
        PromValueType::String => {
            return Err("invalid parameter \"query\": invalid expression type \"string\" for range query, must be Scalar or instant Vector".into());
        }
        _ => {}
    }
    Ok(ReadRequest::Prometheus {
        query: query.to_owned(),
        plan,
        start,
        stop,
        step,
        instant: false,
        limits: PromQueryLimits::default(),
    })
}

fn enforce_prometheus_grid(
    start: i64,
    stop: i64,
    step: i64,
    max_points_per_series: usize,
) -> Result<(), String> {
    let grid_points = (i128::from(stop) - i128::from(start)) / i128::from(step) + 1;
    if grid_points > max_points_per_series as i128 {
        return Err(format!(
            "exceeded maximum resolution of {max_points_per_series} points per timeseries — decrease the query resolution (increase step)"
        ));
    }
    Ok(())
}

fn lower_promql(input: &str, lookback: i64) -> Result<PromPlan, String> {
    let parsed = promql::parse(input).map_err(|error| {
        if error == "no expression found in input" {
            "unknown position: parse error: no expression found in input".to_string()
        } else if error
            == "expected 2 argument(s) in call to 'quantile_over_time', got 1"
        {
            "1:1: parse error: expected 2 argument(s) in call to \"quantile_over_time\", got 1"
                .to_string()
        } else if error
            == "expected type scalar in call to function 'quantile_over_time', got vector"
        {
            "1:20: parse error: expected type scalar in call to function \"quantile_over_time\", got instant vector"
                .to_string()
        } else {
            format!("parse error: {error}")
        }
    })?;
    let mut plan = lower_promql_expr(parsed, lookback, 0)?;
    attach_promql_source_positions(&mut plan, input)?;
    Ok(plan)
}

fn attach_promql_source_positions(plan: &mut PromPlan, input: &str) -> Result<(), String> {
    let mut calls = scan_promql_source_calls(input);
    attach_promql_plan_source_positions(plan, &mut calls)
}

fn attach_promql_plan_source_positions(
    plan: &mut PromPlan,
    calls: &mut BTreeMap<String, VecDeque<PromSourceCall>>,
) -> Result<(), String> {
    let take = |calls: &mut BTreeMap<String, VecDeque<PromSourceCall>>, name: &str| {
        calls.get_mut(name).and_then(VecDeque::pop_front)
    };
    match plan {
        PromPlan::Scalar(_) | PromPlan::String(_) | PromPlan::Time => {}
        PromPlan::Unary(inner) => attach_promql_plan_source_positions(inner, calls)?,
        PromPlan::Function(function) => {
            attach_promql_plan_source_positions(&mut function.inner, calls)?;
            for parameter in &mut function.parameters {
                attach_promql_plan_source_positions(parameter, calls)?;
            }
        }
        PromPlan::LabelReplace(label_replace) => {
            attach_promql_plan_source_positions(&mut label_replace.inner, calls)?;
        }
        PromPlan::LabelJoin(label_join) => {
            attach_promql_plan_source_positions(&mut label_join.inner, calls)?;
        }
        PromPlan::Absent(absent) => {
            attach_promql_plan_source_positions(&mut absent.inner, calls)?;
        }
        PromPlan::Sort(sort) => {
            let name = if sort.descending { "sort_desc" } else { "sort" };
            sort.source = Some(
                take(calls, name)
                    .ok_or_else(|| format!("PromQL source locator could not find {name} call"))?,
            );
            attach_promql_plan_source_positions(&mut sort.inner, calls)?;
        }
        PromPlan::Conversion(conversion) => {
            attach_promql_plan_source_positions(&mut conversion.inner, calls)?;
        }
        PromPlan::Timestamp(timestamp) => {
            attach_promql_plan_source_positions(&mut timestamp.inner, calls)?;
        }
        PromPlan::Calendar(calendar) => {
            attach_promql_plan_source_positions(&mut calendar.inner, calls)?;
        }
        PromPlan::HistogramQuantile(histogram) => {
            histogram.source = Some(take(calls, "histogram_quantile").ok_or_else(|| {
                "PromQL source locator could not find histogram_quantile call".to_string()
            })?);
            attach_promql_plan_source_positions(&mut histogram.quantile, calls)?;
            attach_promql_plan_source_positions(&mut histogram.inner, calls)?;
        }
        PromPlan::Binary(binary) => {
            attach_promql_plan_source_positions(&mut binary.lhs, calls)?;
            attach_promql_plan_source_positions(&mut binary.rhs, calls)?;
        }
        PromPlan::Aggregate(aggregate) => {
            if aggregate.op == PromAggregateOp::Quantile {
                aggregate.source = Some(take(calls, "quantile").ok_or_else(|| {
                    "PromQL source locator could not find quantile aggregation".to_string()
                })?);
            }
            if let Some(parameter) = &mut aggregate.param {
                attach_promql_plan_source_positions(parameter, calls)?;
            }
            attach_promql_plan_source_positions(&mut aggregate.inner, calls)?;
        }
        PromPlan::Selector { .. } | PromPlan::RangeSelector { .. } => {}
        PromPlan::RangeReduction(range) => {
            if matches!(
                range.op,
                PromRangeOp::Quantile | PromRangeOp::Rate | PromRangeOp::Increase
            ) {
                let name = range.op.name();
                range.source =
                    Some(take(calls, name).ok_or_else(|| {
                        format!("PromQL source locator could not find {name} call")
                    })?);
            }
            if let Some(parameter) = &mut range.parameter {
                attach_promql_plan_source_positions(parameter, calls)?;
            }
            if let PromRangeInput::Subquery(subquery) = &mut range.input {
                attach_promql_plan_source_positions(&mut subquery.inner, calls)?;
            }
        }
        PromPlan::Subquery(subquery) => {
            attach_promql_plan_source_positions(&mut subquery.inner, calls)?;
        }
    }
    Ok(())
}

fn scan_promql_source_calls(input: &str) -> BTreeMap<String, VecDeque<PromSourceCall>> {
    let bytes = input.as_bytes();
    let mut calls: BTreeMap<String, VecDeque<PromSourceCall>> = BTreeMap::new();
    let mut index = 0_usize;
    let mut quote = None;
    while index < bytes.len() {
        if let Some(delimiter) = quote {
            if delimiter == b'"' && bytes[index] == b'\\' {
                index = (index + 2).min(bytes.len());
                continue;
            }
            if bytes[index] == delimiter {
                quote = None;
            }
            index += 1;
            continue;
        }
        if matches!(bytes[index], b'"' | b'`') {
            quote = Some(bytes[index]);
            index += 1;
            continue;
        }
        if !is_promql_identifier_start(bytes[index]) {
            index += 1;
            continue;
        }
        let start = index;
        index += 1;
        while index < bytes.len() && is_promql_identifier_continue(bytes[index]) {
            index += 1;
        }
        let name = &input[start..index];
        let mut next = skip_promql_whitespace(bytes, index);
        if name == "quantile" {
            if let Some((modifier, after_modifier)) = promql_identifier_at(input, next) {
                if matches!(modifier, "by" | "without") {
                    next = skip_promql_whitespace(bytes, after_modifier);
                    if bytes.get(next) == Some(&b'(') {
                        if let Some(close) = matching_promql_delimiter(bytes, next, b'(', b')') {
                            next = skip_promql_whitespace(bytes, close + 1);
                        }
                    }
                }
            }
        }
        if bytes.get(next) != Some(&b'(') {
            continue;
        }
        if let Some(arguments) = promql_direct_argument_offsets(bytes, next) {
            calls
                .entry(name.to_owned())
                .or_default()
                .push_back(PromSourceCall { start, arguments });
        }
    }
    calls
}

fn is_promql_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || matches!(byte, b'_' | b':')
}

fn is_promql_identifier_continue(byte: u8) -> bool {
    is_promql_identifier_start(byte) || byte.is_ascii_digit()
}

fn skip_promql_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        index += 1;
    }
    index
}

fn promql_identifier_at(input: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = input.as_bytes();
    if !bytes
        .get(start)
        .copied()
        .is_some_and(is_promql_identifier_start)
    {
        return None;
    }
    let mut end = start + 1;
    while bytes
        .get(end)
        .copied()
        .is_some_and(is_promql_identifier_continue)
    {
        end += 1;
    }
    Some((&input[start..end], end))
}

fn matching_promql_delimiter(bytes: &[u8], start: usize, open: u8, close: u8) -> Option<usize> {
    let mut depth = 0_usize;
    let mut quote = None;
    let mut index = start;
    while index < bytes.len() {
        if let Some(delimiter) = quote {
            if delimiter == b'"' && bytes[index] == b'\\' {
                index = (index + 2).min(bytes.len());
                continue;
            }
            if bytes[index] == delimiter {
                quote = None;
            }
            index += 1;
            continue;
        }
        if matches!(bytes[index], b'"' | b'`') {
            quote = Some(bytes[index]);
        } else if bytes[index] == open {
            depth += 1;
        } else if bytes[index] == close {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(index);
            }
        }
        index += 1;
    }
    None
}

fn promql_direct_argument_offsets(bytes: &[u8], open: usize) -> Option<Vec<usize>> {
    let close = matching_promql_delimiter(bytes, open, b'(', b')')?;
    let first = skip_promql_whitespace(bytes, open + 1);
    if first == close {
        return Some(Vec::new());
    }
    let mut arguments = vec![first];
    let mut parens = 1_usize;
    let mut brackets = 0_usize;
    let mut braces = 0_usize;
    let mut quote = None;
    let mut index = open + 1;
    while index < close {
        if let Some(delimiter) = quote {
            if delimiter == b'"' && bytes[index] == b'\\' {
                index = (index + 2).min(close);
                continue;
            }
            if bytes[index] == delimiter {
                quote = None;
            }
            index += 1;
            continue;
        }
        match bytes[index] {
            b'"' | b'`' => quote = Some(bytes[index]),
            b'(' => parens += 1,
            b')' => parens -= 1,
            b'[' => brackets += 1,
            b']' => brackets = brackets.saturating_sub(1),
            b'{' => braces += 1,
            b'}' => braces = braces.saturating_sub(1),
            b',' if parens == 1 && brackets == 0 && braces == 0 => {
                let argument = skip_promql_whitespace(bytes, index + 1);
                if argument < close {
                    arguments.push(argument);
                }
            }
            _ => {}
        }
        index += 1;
    }
    Some(arguments)
}

const MAX_PROMQL_NESTING: usize = 16;

fn lower_promql_expr(
    parsed: promql::Expr,
    lookback: i64,
    depth: usize,
) -> Result<PromPlan, String> {
    if depth >= MAX_PROMQL_NESTING {
        return Err(format!(
            "PromQL expression exceeds the maximum nesting depth of {MAX_PROMQL_NESTING}"
        ));
    }
    match parsed {
        promql::Expr::NumberLiteral(number) => Ok(PromPlan::Scalar(number.val)),
        promql::Expr::StringLiteral(string) => Ok(PromPlan::String(string.val)),
        promql::Expr::VectorSelector(selector) => {
            let selector = lower_promql_selector(selector)?;
            Ok(PromPlan::Selector { selector, lookback })
        }
        promql::Expr::MatrixSelector(selector) => {
            let window = i64::try_from(selector.range.as_millis())
                .map_err(|_| "PromQL range duration overflow".to_string())?;
            if window == 0 {
                return Err("PromQL range duration must be at least 1ms".into());
            }
            let selector = lower_promql_selector(selector.vs)?;
            Ok(PromPlan::RangeSelector { selector, window })
        }
        promql::Expr::Paren(paren) => lower_promql_expr(*paren.expr, lookback, depth + 1),
        promql::Expr::Unary(unary) => {
            let inner = lower_promql_expr(*unary.expr, lookback, depth + 1)?;
            if !matches!(
                inner.value_type(),
                PromValueType::Scalar | PromValueType::Vector
            ) {
                return Err("PromQL unary minus requires a scalar or instant vector".into());
            }
            Ok(PromPlan::Unary(Box::new(inner)))
        }
        promql::Expr::Binary(binary) => lower_promql_binary(binary, lookback, depth + 1),
        promql::Expr::Aggregate(aggregate) => {
            lower_promql_aggregate(aggregate, lookback, depth + 1)
        }
        promql::Expr::Subquery(subquery) => Ok(PromPlan::Subquery(lower_promql_subquery(
            subquery,
            lookback,
            depth + 1,
        )?)),
        promql::Expr::Call(call) if call.func.name == "abs" => {
            let [argument] = call.args.args.as_slice() else {
                return Err("abs requires exactly one instant vector".into());
            };
            let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
            if inner.value_type() != PromValueType::Vector {
                return Err("abs requires an instant vector".into());
            }
            Ok(PromPlan::Function(PromFunctionPlan {
                op: PromFunctionOp::Abs,
                inner: Box::new(inner),
                parameters: Vec::new(),
            }))
        }
        promql::Expr::Call(call) if call.func.name == "pi" => {
            if !call.args.args.is_empty() {
                return Err("pi does not accept arguments".into());
            }
            Ok(PromPlan::Scalar(std::f64::consts::PI))
        }
        promql::Expr::Call(call)
            if matches!(
                call.func.name,
                "acos"
                    | "acosh"
                    | "asin"
                    | "asinh"
                    | "atan"
                    | "atanh"
                    | "ceil"
                    | "cos"
                    | "cosh"
                    | "deg"
                    | "exp"
                    | "floor"
                    | "ln"
                    | "log2"
                    | "log10"
                    | "rad"
                    | "round"
                    | "sgn"
                    | "sin"
                    | "sinh"
                    | "sqrt"
                    | "tan"
                    | "tanh"
            ) =>
        {
            let op = match call.func.name {
                "acos" => PromFunctionOp::Acos,
                "acosh" => PromFunctionOp::Acosh,
                "asin" => PromFunctionOp::Asin,
                "asinh" => PromFunctionOp::Asinh,
                "atan" => PromFunctionOp::Atan,
                "atanh" => PromFunctionOp::Atanh,
                "ceil" => PromFunctionOp::Ceil,
                "cos" => PromFunctionOp::Cos,
                "cosh" => PromFunctionOp::Cosh,
                "deg" => PromFunctionOp::Deg,
                "exp" => PromFunctionOp::Exp,
                "floor" => PromFunctionOp::Floor,
                "ln" => PromFunctionOp::Ln,
                "log2" => PromFunctionOp::Log2,
                "log10" => PromFunctionOp::Log10,
                "rad" => PromFunctionOp::Rad,
                "round" => PromFunctionOp::Round,
                "sgn" => PromFunctionOp::Sgn,
                "sin" => PromFunctionOp::Sin,
                "sinh" => PromFunctionOp::Sinh,
                "sqrt" => PromFunctionOp::Sqrt,
                "tan" => PromFunctionOp::Tan,
                "tanh" => PromFunctionOp::Tanh,
                _ => unreachable!("guarded numeric transform"),
            };
            let (argument, parameter) = match call.args.args.as_slice() {
                [argument] => (argument, None),
                [argument, parameter] if op == PromFunctionOp::Round => {
                    (argument, Some(parameter))
                }
                _ => {
                    return Err(format!(
                        "{} requires an instant vector{}",
                        call.func.name,
                        if op == PromFunctionOp::Round {
                            " and an optional scalar step"
                        } else {
                            ""
                        }
                    ));
                }
            };
            let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
            if inner.value_type() != PromValueType::Vector {
                return Err(format!("{} requires an instant vector", call.func.name));
            }
            let parameter = parameter
                .map(|parameter| {
                    let parameter =
                        lower_promql_expr((**parameter).clone(), lookback, depth + 1)?;
                    if parameter.value_type() != PromValueType::Scalar {
                        return Err("round step must be a scalar".to_string());
                    }
                    Ok(parameter)
                })
                .transpose()?;
            Ok(PromPlan::Function(PromFunctionPlan {
                op,
                inner: Box::new(inner),
                parameters: parameter.into_iter().collect(),
            }))
        }
        promql::Expr::Call(call)
            if matches!(call.func.name, "clamp" | "clamp_min" | "clamp_max") =>
        {
            let (op, argument, parameters) = match call.args.args.as_slice() {
                [argument, minimum, maximum] if call.func.name == "clamp" => (
                    PromFunctionOp::Clamp,
                    argument,
                    vec![minimum, maximum],
                ),
                [argument, maximum] if call.func.name == "clamp_max" => {
                    (PromFunctionOp::ClampMax, argument, vec![maximum])
                }
                [argument, minimum] if call.func.name == "clamp_min" => {
                    (PromFunctionOp::ClampMin, argument, vec![minimum])
                }
                _ => {
                    return Err(format!(
                        "{} requires an instant vector and {} scalar parameter{}",
                        call.func.name,
                        if call.func.name == "clamp" { "two" } else { "one" },
                        if call.func.name == "clamp" { "s" } else { "" }
                    ));
                }
            };
            let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
            if inner.value_type() != PromValueType::Vector {
                return Err(format!("{} requires an instant vector", call.func.name));
            }
            let parameters = parameters
                .into_iter()
                .map(|parameter| {
                    let parameter =
                        lower_promql_expr((**parameter).clone(), lookback, depth + 1)?;
                    if parameter.value_type() != PromValueType::Scalar {
                        return Err(format!("{} bounds must be scalars", call.func.name));
                    }
                    Ok(parameter)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PromPlan::Function(PromFunctionPlan {
                op,
                inner: Box::new(inner),
                parameters,
            }))
        }
        promql::Expr::Call(call) if call.func.name == "histogram_quantile" => {
            let [quantile, argument] = call.args.args.as_slice() else {
                return Err(
                    "histogram_quantile requires a scalar quantile and an instant vector".into(),
                );
            };
            let quantile = lower_promql_expr((**quantile).clone(), lookback, depth + 1)?;
            if quantile.value_type() != PromValueType::Scalar {
                return Err("histogram_quantile quantile must be a scalar".into());
            }
            let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
            if inner.value_type() != PromValueType::Vector {
                return Err("histogram_quantile buckets must be an instant vector".into());
            }
            Ok(PromPlan::HistogramQuantile(PromHistogramQuantilePlan {
                quantile: Box::new(quantile),
                inner: Box::new(inner),
                source: None,
            }))
        }
        promql::Expr::Call(call) if call.func.name == "label_replace" => {
            let [argument, destination, replacement, source, pattern] = call.args.args.as_slice()
            else {
                return Err(
                    "label_replace requires an instant vector and four string arguments".into(),
                );
            };
            let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
            if inner.value_type() != PromValueType::Vector {
                return Err("label_replace requires an instant vector".into());
            }
            Ok(PromPlan::LabelReplace(PromLabelReplacePlan {
                inner: Box::new(inner),
                destination: promql_string_argument(
                    destination,
                    "label_replace",
                    "destination label",
                )?,
                replacement: promql_string_argument(replacement, "label_replace", "replacement")?,
                source: promql_string_argument(source, "label_replace", "source label")?,
                pattern: promql_string_argument(
                    pattern,
                    "label_replace",
                    "regular expression",
                )?,
            }))
        }
        promql::Expr::Call(call) if call.func.name == "label_join" => {
            let [argument, destination, separator, sources @ ..] = call.args.args.as_slice()
            else {
                return Err(
                    "label_join requires an instant vector, destination label, separator, and optional source labels"
                        .into(),
                );
            };
            let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
            if inner.value_type() != PromValueType::Vector {
                return Err("label_join requires an instant vector".into());
            }
            let sources = sources
                .iter()
                .map(|source| promql_string_argument(source, "label_join", "source label"))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PromPlan::LabelJoin(PromLabelJoinPlan {
                inner: Box::new(inner),
                destination: promql_string_argument(
                    destination,
                    "label_join",
                    "destination label",
                )?,
                separator: promql_string_argument(separator, "label_join", "separator")?,
                sources,
            }))
        }
        promql::Expr::Call(call) if call.func.name == "absent" => {
            let [argument] = call.args.args.as_slice() else {
                return Err("absent requires one instant vector".into());
            };
            let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
            if inner.value_type() != PromValueType::Vector {
                return Err("absent requires an instant vector".into());
            }
            let labels = absent_output_labels(&inner);
            Ok(PromPlan::Absent(PromAbsentPlan {
                inner: Box::new(inner),
                labels,
            }))
        }
        promql::Expr::Call(call) if call.func.name == "absent_over_time" => {
            let [argument] = call.args.args.as_slice() else {
                return Err("absent_over_time requires exactly one range vector".into());
            };
            let (input, labels) = match argument.as_ref() {
                promql::Expr::MatrixSelector(selector) => {
                    let window = duration_millis_i64(selector.range, "range")?;
                    if window == 0 {
                        return Err("PromQL range duration must be at least 1ms".into());
                    }
                    let selector = lower_promql_selector(selector.vs.clone())?;
                    let labels = absent_selector_output_labels(&selector);
                    (PromRangeInput::Selector { selector, window }, labels)
                }
                promql::Expr::Subquery(subquery) => (
                    PromRangeInput::Subquery(lower_promql_subquery(
                        subquery.clone(),
                        lookback,
                        depth + 1,
                    )?),
                    BTreeMap::new(),
                ),
                _ => return Err("absent_over_time requires a range vector".into()),
            };
            let present = PromPlan::RangeReduction(PromRangePlan {
                op: PromRangeOp::Present,
                input,
                parameter: None,
                source: None,
            });
            Ok(PromPlan::Absent(PromAbsentPlan {
                inner: Box::new(present),
                labels,
            }))
        }
        promql::Expr::Call(call) if matches!(call.func.name, "sort" | "sort_desc") => {
            let [argument] = call.args.args.as_slice() else {
                return Err(format!("{} requires one instant vector", call.func.name));
            };
            let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
            if inner.value_type() != PromValueType::Vector {
                return Err(format!("{} requires an instant vector", call.func.name));
            }
            Ok(PromPlan::Sort(PromSortPlan {
                inner: Box::new(inner),
                descending: call.func.name == "sort_desc",
                source: None,
            }))
        }
        promql::Expr::Call(call) if matches!(call.func.name, "scalar" | "vector") => {
            let [argument] = call.args.args.as_slice() else {
                return Err(format!("{} requires exactly one argument", call.func.name));
            };
            let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
            let kind = if call.func.name == "scalar" {
                if inner.value_type() != PromValueType::Vector {
                    return Err("scalar requires an instant vector".into());
                }
                PromConversionKind::Scalar
            } else {
                if inner.value_type() != PromValueType::Scalar {
                    return Err("vector requires a scalar".into());
                }
                PromConversionKind::Vector
            };
            Ok(PromPlan::Conversion(PromConversionPlan {
                inner: Box::new(inner),
                kind,
            }))
        }
        promql::Expr::Call(call) if call.func.name == "time" => {
            if !call.args.args.is_empty() {
                return Err("time does not accept arguments".into());
            }
            Ok(PromPlan::Time)
        }
        promql::Expr::Call(call) if call.func.name == "timestamp" => {
            let [argument] = call.args.args.as_slice() else {
                return Err("timestamp requires exactly one instant vector".into());
            };
            let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
            if inner.value_type() != PromValueType::Vector {
                return Err("timestamp requires an instant vector".into());
            }
            Ok(PromPlan::Timestamp(PromTimestampPlan {
                inner: Box::new(inner),
            }))
        }
        promql::Expr::Call(call)
            if matches!(
                call.func.name,
                "minute"
                    | "hour"
                    | "day_of_week"
                    | "day_of_month"
                    | "day_of_year"
                    | "days_in_month"
                    | "month"
                    | "year"
            ) =>
        {
            let inner = match call.args.args.as_slice() {
                [] => PromPlan::Conversion(PromConversionPlan {
                    inner: Box::new(PromPlan::Time),
                    kind: PromConversionKind::Vector,
                }),
                [argument] => {
                    let inner = lower_promql_expr((**argument).clone(), lookback, depth + 1)?;
                    if inner.value_type() != PromValueType::Vector {
                        return Err(format!("{} requires an instant vector", call.func.name));
                    }
                    inner
                }
                _ => {
                    return Err(format!(
                        "{} accepts at most one instant vector",
                        call.func.name
                    ));
                }
            };
            let op = match call.func.name {
                "minute" => PromCalendarOp::Minute,
                "hour" => PromCalendarOp::Hour,
                "day_of_week" => PromCalendarOp::DayOfWeek,
                "day_of_month" => PromCalendarOp::DayOfMonth,
                "day_of_year" => PromCalendarOp::DayOfYear,
                "days_in_month" => PromCalendarOp::DaysInMonth,
                "month" => PromCalendarOp::Month,
                "year" => PromCalendarOp::Year,
                _ => unreachable!("guarded calendar function"),
            };
            Ok(PromPlan::Calendar(PromCalendarPlan {
                inner: Box::new(inner),
                op,
            }))
        }
        promql::Expr::Call(call) if call.func.name == "first_over_time" => Err(
            "first_over_time is experimental and is not enabled in the stable PromQL compatibility tier"
                .into(),
        ),
        promql::Expr::Call(call) if call.func.name == "double_exponential_smoothing" => Err(
            "double_exponential_smoothing is experimental and is not enabled in the stable PromQL compatibility tier"
                .into(),
        ),
        promql::Expr::Call(call) if call.func.name == "quantile_over_time" => {
            let [parameter, argument] = call.args.args.as_slice() else {
                return Err("quantile_over_time requires a scalar and a range vector".into());
            };
            let parameter = lower_promql_expr((**parameter).clone(), lookback, depth + 1)?;
            if parameter.value_type() != PromValueType::Scalar {
                return Err("quantile_over_time requires a scalar parameter".into());
            }
            let input = match argument.as_ref() {
                promql::Expr::MatrixSelector(selector) => {
                    let window = duration_millis_i64(selector.range, "range")?;
                    if window == 0 {
                        return Err("PromQL range duration must be at least 1ms".into());
                    }
                    let selector = lower_promql_selector(selector.vs.clone())?;
                    PromRangeInput::Selector { selector, window }
                }
                promql::Expr::Subquery(subquery) => PromRangeInput::Subquery(
                    lower_promql_subquery(subquery.clone(), lookback, depth + 1)?,
                ),
                _ => return Err("quantile_over_time requires a range vector".into()),
            };
            Ok(PromPlan::RangeReduction(PromRangePlan {
                op: PromRangeOp::Quantile,
                input,
                parameter: Some(Box::new(parameter)),
                source: None,
            }))
        }
        promql::Expr::Call(call) if call.func.name == "predict_linear" => {
            let [argument, parameter] = call.args.args.as_slice() else {
                return Err("predict_linear requires a range vector and a scalar horizon".into());
            };
            let parameter = lower_promql_expr((**parameter).clone(), lookback, depth + 1)?;
            if parameter.value_type() != PromValueType::Scalar {
                return Err("predict_linear requires a scalar horizon".into());
            }
            let input = match argument.as_ref() {
                promql::Expr::MatrixSelector(selector) => {
                    let window = duration_millis_i64(selector.range, "range")?;
                    if window == 0 {
                        return Err("PromQL range duration must be at least 1ms".into());
                    }
                    let selector = lower_promql_selector(selector.vs.clone())?;
                    PromRangeInput::Selector { selector, window }
                }
                promql::Expr::Subquery(subquery) => PromRangeInput::Subquery(
                    lower_promql_subquery(subquery.clone(), lookback, depth + 1)?,
                ),
                _ => return Err("predict_linear requires a range vector".into()),
            };
            Ok(PromPlan::RangeReduction(PromRangePlan {
                op: PromRangeOp::PredictLinear,
                input,
                parameter: Some(Box::new(parameter)),
                source: None,
            }))
        }
        promql::Expr::Call(call)
            if matches!(
                call.func.name,
                "avg_over_time"
                    | "min_over_time"
                    | "max_over_time"
                    | "sum_over_time"
                    | "count_over_time"
                    | "present_over_time"
                    | "stddev_over_time"
                    | "stdvar_over_time"
                    | "rate"
                    | "irate"
                    | "increase"
                    | "delta"
                    | "idelta"
                    | "deriv"
                    | "changes"
                    | "resets"
                    | "last_over_time"
            ) =>
        {
            let op = match call.func.name {
                "avg_over_time" => PromRangeOp::Avg,
                "min_over_time" => PromRangeOp::Min,
                "max_over_time" => PromRangeOp::Max,
                "sum_over_time" => PromRangeOp::Sum,
                "count_over_time" => PromRangeOp::Count,
                "present_over_time" => PromRangeOp::Present,
                "stddev_over_time" => PromRangeOp::StdDev,
                "stdvar_over_time" => PromRangeOp::StdVar,
                "rate" => PromRangeOp::Rate,
                "irate" => PromRangeOp::IRate,
                "increase" => PromRangeOp::Increase,
                "delta" => PromRangeOp::Delta,
                "idelta" => PromRangeOp::IDelta,
                "deriv" => PromRangeOp::Deriv,
                "changes" => PromRangeOp::Changes,
                "resets" => PromRangeOp::Resets,
                "last_over_time" => PromRangeOp::Last,
                _ => unreachable!("guarded range function"),
            };
            let [argument] = call.args.args.as_slice() else {
                return Err(format!("{} requires exactly one range vector", op.name()));
            };
            let input = match argument.as_ref() {
                promql::Expr::MatrixSelector(selector) => {
                    let window = duration_millis_i64(selector.range, "range")?;
                    if window == 0 {
                        return Err("PromQL range duration must be at least 1ms".into());
                    }
                    let selector = lower_promql_selector(selector.vs.clone())?;
                    PromRangeInput::Selector { selector, window }
                }
                promql::Expr::Subquery(subquery) => PromRangeInput::Subquery(
                    lower_promql_subquery(subquery.clone(), lookback, depth + 1)?,
                ),
                _ => return Err(format!("{} requires a range vector", op.name())),
            };
            Ok(PromPlan::RangeReduction(PromRangePlan {
                op,
                input,
                parameter: None,
                source: None,
            }))
        }
        other => Err(format!(
            "unsupported PromQL expression (parsed as {})",
            promql_expression_name(&other)
        )),
    }
}

fn promql_string_argument(
    argument: &promql::Expr,
    function: &str,
    name: &str,
) -> Result<String, String> {
    let promql::Expr::StringLiteral(value) = argument else {
        return Err(format!("{function} {name} must be a string literal"));
    };
    Ok(value.val.clone())
}

fn absent_output_labels(inner: &PromPlan) -> BTreeMap<String, String> {
    let PromPlan::Selector { selector, .. } = inner else {
        return BTreeMap::new();
    };
    absent_selector_output_labels(selector)
}

fn absent_selector_output_labels(selector: &Selector) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    let mut seen = HashSet::new();
    for matcher in &selector.filter.matchers {
        if !seen.insert(matcher.key.as_str()) {
            labels.remove(&matcher.key);
            continue;
        }
        if matcher.op == MatcherOp::Eq && !matcher.value.is_empty() {
            labels.insert(matcher.key.clone(), matcher.value.clone());
        }
    }
    labels
}

fn lower_promql_aggregate(
    aggregate: promql_parser::parser::AggregateExpr,
    lookback: i64,
    depth: usize,
) -> Result<PromPlan, String> {
    use promql_parser::parser::token;
    use promql_parser::parser::LabelModifier;

    let op = match aggregate.op.id() {
        token::T_SUM => PromAggregateOp::Sum,
        token::T_AVG => PromAggregateOp::Avg,
        token::T_MIN => PromAggregateOp::Min,
        token::T_MAX => PromAggregateOp::Max,
        token::T_COUNT => PromAggregateOp::Count,
        token::T_GROUP => PromAggregateOp::Group,
        token::T_STDDEV => PromAggregateOp::StdDev,
        token::T_STDVAR => PromAggregateOp::StdVar,
        token::T_TOPK => PromAggregateOp::TopK,
        token::T_BOTTOMK => PromAggregateOp::BottomK,
        token::T_QUANTILE => PromAggregateOp::Quantile,
        token::T_COUNT_VALUES => PromAggregateOp::CountValues,
        _ => {
            return Err(format!("unsupported PromQL aggregation {}", aggregate.op));
        }
    };
    let (param, value_label) = if op == PromAggregateOp::CountValues {
        let Some(param) = aggregate.param else {
            return Err("PromQL aggregation count_values requires a string parameter".into());
        };
        let promql::Expr::StringLiteral(label) = *param else {
            return Err("PromQL aggregation count_values requires a string parameter".into());
        };
        (None, Some(label.val))
    } else {
        let requires_param = matches!(
            op,
            PromAggregateOp::TopK | PromAggregateOp::BottomK | PromAggregateOp::Quantile
        );
        let param = match (requires_param, aggregate.param) {
            (true, Some(param)) => {
                let param = lower_promql_expr(*param, lookback, depth)?;
                if param.value_type() != PromValueType::Scalar {
                    return Err(format!(
                        "PromQL aggregation {} requires a scalar parameter",
                        aggregate.op
                    ));
                }
                Some(Box::new(param))
            }
            (true, None) => {
                return Err(format!(
                    "PromQL aggregation {} requires a scalar parameter",
                    aggregate.op
                ));
            }
            (false, Some(_)) => {
                return Err(format!(
                    "PromQL aggregation {} does not accept a parameter",
                    aggregate.op
                ));
            }
            (false, None) => None,
        };
        (param, None)
    };
    let grouping = match aggregate.modifier {
        None => PromAggregateGrouping::All,
        Some(LabelModifier::Include(labels)) => {
            PromAggregateGrouping::By(labels.labels.iter().cloned().collect())
        }
        Some(LabelModifier::Exclude(labels)) => {
            PromAggregateGrouping::Without(labels.labels.iter().cloned().collect())
        }
    };
    let inner = lower_promql_expr(*aggregate.expr, lookback, depth)?;
    if inner.value_type() != PromValueType::Vector {
        return Err(format!(
            "PromQL aggregation {} requires an instant vector",
            aggregate.op
        ));
    }
    Ok(PromPlan::Aggregate(PromAggregatePlan {
        op,
        inner: Box::new(inner),
        param,
        value_label,
        grouping,
        source: None,
    }))
}

fn lower_promql_binary(
    binary: promql_parser::parser::BinaryExpr,
    lookback: i64,
    depth: usize,
) -> Result<PromPlan, String> {
    use promql_parser::parser::token;
    use promql_parser::parser::LabelModifier;
    use promql_parser::parser::VectorMatchCardinality;

    let op = match binary.op.id() {
        token::T_ADD => PromBinaryOp::Add,
        token::T_SUB => PromBinaryOp::Subtract,
        token::T_MUL => PromBinaryOp::Multiply,
        token::T_DIV => PromBinaryOp::Divide,
        token::T_MOD => PromBinaryOp::Modulo,
        token::T_POW => PromBinaryOp::Power,
        token::T_ATAN2 => PromBinaryOp::Atan2,
        token::T_EQLC => PromBinaryOp::Equal,
        token::T_NEQ => PromBinaryOp::NotEqual,
        token::T_GTR => PromBinaryOp::Greater,
        token::T_LSS => PromBinaryOp::Less,
        token::T_GTE => PromBinaryOp::GreaterOrEqual,
        token::T_LTE => PromBinaryOp::LessOrEqual,
        token::T_LAND => PromBinaryOp::And,
        token::T_LOR => PromBinaryOp::Or,
        token::T_LUNLESS => PromBinaryOp::Unless,
        _ => {
            return Err(format!("unsupported PromQL binary operator {}", binary.op));
        }
    };
    let return_bool = binary
        .modifier
        .as_ref()
        .is_some_and(|modifier| modifier.return_bool);
    let matching = match binary
        .modifier
        .as_ref()
        .and_then(|modifier| modifier.matching.as_ref())
    {
        None => PromVectorMatching::Default,
        Some(LabelModifier::Include(labels)) => {
            PromVectorMatching::On(labels.labels.iter().cloned().collect())
        }
        Some(LabelModifier::Exclude(labels)) => {
            PromVectorMatching::Ignoring(labels.labels.iter().cloned().collect())
        }
    };
    let cardinality = match binary.modifier.as_ref().map(|modifier| &modifier.card) {
        None | Some(VectorMatchCardinality::OneToOne) => PromVectorCardinality::OneToOne,
        Some(VectorMatchCardinality::ManyToOne(labels)) => {
            PromVectorCardinality::ManyToOne(labels.labels.iter().cloned().collect())
        }
        Some(VectorMatchCardinality::OneToMany(labels)) => {
            PromVectorCardinality::OneToMany(labels.labels.iter().cloned().collect())
        }
        Some(VectorMatchCardinality::ManyToMany) => PromVectorCardinality::ManyToMany,
    };
    if let Some(modifier) = &binary.modifier {
        let expected_cardinality = if op.is_set() {
            matches!(cardinality, PromVectorCardinality::ManyToMany)
        } else {
            !matches!(cardinality, PromVectorCardinality::ManyToMany)
        };
        if !expected_cardinality
            || modifier.fill_values.lhs.is_some()
            || modifier.fill_values.rhs.is_some()
        {
            return Err("PromQL binary matching modifiers are not shipped yet".into());
        }
    }
    let lhs = lower_promql_expr(*binary.lhs, lookback, depth)?;
    let rhs = lower_promql_expr(*binary.rhs, lookback, depth)?;
    if op.is_set()
        && (lhs.value_type() != PromValueType::Vector || rhs.value_type() != PromValueType::Vector)
    {
        return Err("PromQL set operators require instant-vector operands".into());
    }
    for operand in [&lhs, &rhs] {
        if !matches!(
            operand.value_type(),
            PromValueType::Scalar | PromValueType::Vector
        ) {
            return Err(
                "PromQL binary expressions require scalar or instant-vector operands".into(),
            );
        }
    }
    if !op.is_arithmetic()
        && lhs.value_type() == PromValueType::Scalar
        && rhs.value_type() == PromValueType::Scalar
        && !return_bool
    {
        return Err("comparisons between scalars must use BOOL modifier".into());
    }
    Ok(PromPlan::Binary(PromBinaryPlan {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
        return_bool,
        matching,
        cardinality,
    }))
}

fn lower_promql_subquery(
    subquery: promql_parser::parser::SubqueryExpr,
    lookback: i64,
    depth: usize,
) -> Result<SubqueryPlan, String> {
    let window = duration_millis_i64(subquery.range, "subquery range")?;
    if window == 0 {
        return Err("PromQL subquery range must be at least 1ms".into());
    }
    let resolution = subquery
        .step
        .map(|step| duration_millis_i64(step, "subquery resolution"))
        .transpose()?;
    if resolution == Some(0) {
        return Err("PromQL subquery resolution must be at least 1ms".into());
    }
    let inner = lower_promql_expr(*subquery.expr, lookback, depth)?;
    if !matches!(
        inner,
        PromPlan::Selector { .. }
            | PromPlan::RangeReduction(_)
            | PromPlan::Unary(_)
            | PromPlan::Function(_)
            | PromPlan::LabelReplace(_)
            | PromPlan::LabelJoin(_)
            | PromPlan::Absent(_)
            | PromPlan::Sort(_)
            | PromPlan::Conversion(_)
            | PromPlan::Timestamp(_)
            | PromPlan::Calendar(_)
            | PromPlan::HistogramQuantile(_)
            | PromPlan::Binary(_)
    ) {
        return Err("PromQL subquery requires an instant-vector expression".into());
    }
    Ok(SubqueryPlan {
        inner: Box::new(inner),
        window,
        resolution,
        timing: SelectorTiming::lower(subquery.offset, subquery.at)?,
    })
}

fn lower_promql_selector(selector: promql::VectorSelector) -> Result<Selector, String> {
    let timing = SelectorTiming::lower(selector.offset, selector.at)?;
    if !selector.matchers.or_matchers.is_empty() {
        return Err("PromQL OR matcher groups are not shipped yet".into());
    }
    let metric = selector.name;
    let mut name_matchers = Vec::new();
    let mut matchers = Vec::with_capacity(selector.matchers.matchers.len());
    for matcher in selector.matchers.matchers {
        let op = match matcher.op {
            promql::MatchOp::Equal => MatcherOp::Eq,
            promql::MatchOp::NotEqual => MatcherOp::NotEq,
            promql::MatchOp::Re(_) => MatcherOp::Regex,
            promql::MatchOp::NotRe(_) => MatcherOp::NotRegex,
        };
        if matcher.name == "__name__" {
            if metric.is_some() {
                return Err("metric name specified twice".into());
            }
            name_matchers.push(Matcher::new(matcher.name, op, matcher.value)?);
            continue;
        }
        matchers.push(Matcher::new(matcher.name, op, matcher.value)?);
    }
    Ok(Selector {
        metric: match (metric, name_matchers.as_slice()) {
            (Some(metric), []) => MetricSelection::Exact(metric),
            (None, []) => MetricSelection::All,
            (None, [matcher]) if matcher.op == MatcherOp::Eq => {
                MetricSelection::Exact(matcher.value.clone())
            }
            (None, _) => MetricSelection::Matchers(name_matchers),
            (Some(_), _) => return Err("metric name specified twice".into()),
        },
        filter: FilterPlan::new(matchers),
        timing,
    })
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
            timing: SelectorTiming::default(),
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
    raw_frame_work_limit: bool,
    window_batch_work_limit: bool,
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
        let capability_document: String = conn
            .query_row("SELECT timeless_capabilities()", [], |row| row.get(0))
            .map_err(|error| format!("read extension query capabilities: {error}"))?;
        let capabilities: serde_json::Value = serde_json::from_str(&capability_document)
            .map_err(|error| format!("decode extension query capabilities: {error}"))?;
        let has_work_limit =
            |surface: &str| capabilities["query_surfaces"][surface]["max_work_points"] == true;
        Ok(Self {
            table,
            latest_frame: modules.contains("timeless_latest_frame"),
            raw_frame: modules.contains("timeless_raw_frame"),
            window_batches: modules.contains("timeless_window_batches"),
            raw_frame_work_limit: has_work_limit("timeless_raw_frame"),
            window_batch_work_limit: has_work_limit("timeless_window_batches"),
        })
    }
}

pub(crate) struct ReadOutput {
    pub body: Vec<u8>,
    pub frame_bytes: usize,
    pub series: u64,
    pub points: u64,
    /// Evaluator points materialized only to feed a parent AST node. Final
    /// result points are reported separately in `points`.
    pub intermediate_points: u64,
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
            query,
            plan,
            start,
            stop,
            step,
            instant,
            limits,
        } => {
            let mut annotations = PromAnnotations::default();
            let mut output = execute_prometheus(
                conn,
                features,
                &plan,
                start,
                stop,
                step,
                instant,
                start,
                stop,
                limits,
                &mut annotations,
                cancelled,
            )?;
            annotations.append_to_success(&query, &mut output, limits)?;
            Ok(output)
        }
    }
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), String> {
    if cancelled.load(Ordering::Acquire) {
        Err("query cancelled".into())
    } else {
        Ok(())
    }
}

fn admit_prometheus_point(current_points: u64, limits: PromQueryLimits) -> Result<(), String> {
    if current_points >= limits.max_result_points as u64 {
        Err(format!(
            "query exceeded the maximum result-point limit of {}",
            limits.max_result_points
        ))
    } else {
        Ok(())
    }
}

fn enforce_prometheus_output(
    body: &[u8],
    result_points: u64,
    limits: PromQueryLimits,
) -> Result<(), String> {
    if result_points > limits.max_result_points as u64 {
        return Err(format!(
            "query exceeded the maximum result-point limit of {}",
            limits.max_result_points
        ));
    }
    if body.len() > limits.max_response_bytes {
        return Err(format!(
            "query exceeded the maximum response-size limit of {} bytes",
            limits.max_response_bytes
        ));
    }
    Ok(())
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

fn prometheus_catalogs(
    conn: &Connection,
    table: MetricsTable,
    selector: &Selector,
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<(String, Vec<SeriesMeta>)>, String> {
    if let MetricSelection::Exact(metric) = &selector.metric {
        let catalog = catalog(conn, table, metric, &selector.filter)?;
        return Ok(if catalog.is_empty() {
            Vec::new()
        } else {
            vec![(metric.clone(), catalog)]
        });
    }

    let mut stmt = conn
        .prepare(&format!(
            "SELECT series_id, name, labels
               FROM timeless_series('{}')
              ORDER BY name, labels, series_id",
            table.name()
        ))
        .map_err(|error| format!("prepare PromQL complete series catalog: {error}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| format!("query PromQL complete series catalog: {error}"))?;
    let mut considered = 0_usize;
    let mut selected_bytes = 0_usize;
    let mut grouped = BTreeMap::<String, Vec<SeriesMeta>>::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("read PromQL complete series catalog: {error}"))?
    {
        check_cancelled(cancelled)?;
        considered = considered.saturating_add(1);
        if considered > limits.max_work_points {
            return Err(format!(
                "query exceeded the maximum catalog-work limit of {} series",
                limits.max_work_points
            ));
        }
        let id = row
            .get::<_, i64>(0)
            .map_err(|error| format!("read PromQL catalog series id: {error}"))?;
        let metric = row
            .get::<_, String>(1)
            .map_err(|error| format!("read PromQL catalog metric: {error}"))?;
        let selected_metric = match &selector.metric {
            MetricSelection::Regex(regex) => regex.is_match(&metric),
            MetricSelection::Matchers(matchers) => matchers
                .iter()
                .all(|matcher| matcher.matches_value(&metric)),
            MetricSelection::All => true,
            MetricSelection::Exact(_) => unreachable!("handled before complete scan"),
        };
        if !selected_metric {
            continue;
        }
        let labels_json = row
            .get::<_, String>(2)
            .map_err(|error| format!("read PromQL catalog labels: {error}"))?;
        let labels = decode_labels(&labels_json)?;
        if !selector.filter.matches(&labels) {
            continue;
        }
        selected_bytes = selected_bytes
            .checked_add(metric.len())
            .and_then(|bytes| bytes.checked_add(labels_json.len()))
            .ok_or_else(|| "PromQL catalog byte accounting overflow".to_string())?;
        if selected_bytes > limits.max_response_bytes {
            return Err(format!(
                "query exceeded the maximum catalog-size limit of {} bytes",
                limits.max_response_bytes
            ));
        }
        grouped.entry(metric.clone()).or_default().push(SeriesMeta {
            id,
            metric,
            labels_json,
            labels,
        });
    }
    Ok(grouped.into_iter().collect())
}

fn consume_prometheus_work(
    remaining: &mut usize,
    work_points: usize,
    limits: PromQueryLimits,
) -> Result<(), String> {
    *remaining = remaining.checked_sub(work_points).ok_or_else(|| {
        format!(
            "query exceeded the maximum storage-work limit of {} points",
            limits.max_work_points
        )
    })?;
    Ok(())
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
        intermediate_points: 0,
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
    let raw = raw_query(conn, features, metric, filter, start, stop, None)?;
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
        intermediate_points: 0,
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
    max_work_points: Option<usize>,
) -> Result<RawQuery, String> {
    let max_work_points = max_work_points
        .map(i64::try_from)
        .transpose()
        .map_err(|_| "PromQL max_work_points exceeds SQLite INTEGER range".to_string())?;
    if max_work_points.is_some() && (!features.raw_frame || !features.raw_frame_work_limit) {
        return Err(
            "incompatible extension: timeless_raw_frame max_work_points capability is required for bounded PromQL execution"
                .into(),
        );
    }
    if features.raw_frame {
        let frame: Option<Vec<u8>> = match max_work_points {
            Some(limit) => conn.query_row(
                &format!(
                    "SELECT frame FROM timeless_raw_frame('{}', ?1, ?2, ?3, ?4, ?5)",
                    features.table.name()
                ),
                params![metric, filter.pushdown_json, start, stop, limit],
                |row| row.get(0),
            ),
            None => conn.query_row(
                &format!(
                    "SELECT frame FROM timeless_raw_frame('{}', ?1, ?2, ?3, ?4)",
                    features.table.name()
                ),
                params![metric, filter.pushdown_json, start, stop],
                |row| row.get(0),
            ),
        }
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
    let mut observed_points = 0_usize;
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
        observed_points = observed_points.saturating_add(1);
        if let Some(limit) = max_work_points.filter(|limit| observed_points > *limit as usize) {
            return Err(format!(
                "raw batch work point limit {limit} exceeded (candidate points: {observed_points})"
            ));
        }
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
        intermediate_points: 0,
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
        None,
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
        intermediate_points: 0,
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
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    match plan {
        PromPlan::Scalar(value) => {
            execute_prometheus_scalar(*value, start, stop, step, instant, limits, cancelled)
        }
        PromPlan::String(value) => execute_prometheus_string(value, start, instant, limits),
        PromPlan::Unary(inner) => execute_prometheus_unary(
            conn,
            features,
            inner,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::Function(function) => execute_prometheus_function(
            conn,
            features,
            function,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::LabelReplace(label_replace) => execute_prometheus_label_replace(
            conn,
            features,
            label_replace,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::LabelJoin(label_join) => execute_prometheus_label_join(
            conn,
            features,
            label_join,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::Absent(absent) => execute_prometheus_absent(
            conn,
            features,
            absent,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::Sort(sort) => execute_prometheus_sort(
            conn,
            features,
            sort,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::Conversion(conversion) => execute_prometheus_conversion(
            conn,
            features,
            conversion,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::Time => execute_prometheus_time(start, stop, step, instant, limits, cancelled),
        PromPlan::Timestamp(timestamp) => execute_prometheus_timestamp(
            conn,
            features,
            timestamp,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::Calendar(calendar) => execute_prometheus_calendar(
            conn,
            features,
            calendar,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::HistogramQuantile(histogram) => execute_prometheus_histogram_quantile(
            conn,
            features,
            histogram,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::Binary(binary) => execute_prometheus_binary(
            conn,
            features,
            binary,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::Aggregate(aggregate) => execute_prometheus_aggregate(
            conn,
            features,
            aggregate,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::Selector { selector, lookback } => execute_prometheus_selector(
            conn,
            features,
            selector,
            start,
            stop,
            step,
            *lookback,
            instant,
            query_start,
            query_end,
            limits,
            cancelled,
        ),
        PromPlan::RangeReduction(range) => execute_prometheus_range_reduction_plan(
            conn,
            features,
            range,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
        PromPlan::RangeSelector { selector, window } => execute_prometheus_range_selector(
            conn,
            features,
            selector,
            stop,
            *window,
            query_start,
            query_end,
            limits,
            cancelled,
        ),
        PromPlan::Subquery(subquery) => execute_prometheus_subquery(
            conn,
            features,
            subquery,
            start,
            stop,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_range_reduction_plan(
    conn: &Connection,
    features: QueryFeatures,
    range: &PromRangePlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let (parameters, parameter_frame_bytes, parameter_intermediate_points) =
        if let Some(parameter) = &range.parameter {
            let output = execute_prometheus(
                conn,
                features,
                parameter,
                start,
                stop,
                step,
                instant,
                query_start,
                query_end,
                limits,
                annotations,
                cancelled,
            )?;
            let frame_bytes = output.frame_bytes;
            let intermediate_points = output.intermediate_points.saturating_add(output.points);
            let IntermediateValue::Scalar(points) = decode_prometheus_intermediate(
                &output.body,
                PromValueType::Scalar,
                instant,
                limits,
                cancelled,
            )?
            else {
                unreachable!("range parameter type was checked while lowering")
            };
            (Some(points), frame_bytes, intermediate_points)
        } else {
            (None, 0, 0)
        };
    enforce_intermediate_work(parameter_intermediate_points, limits)?;
    let annotation_position = range.source.as_ref().map_or(0, |source| {
        if matches!(&range.input, PromRangeInput::Subquery(_)) {
            source.start
        } else {
            source.argument(0)
        }
    });

    let mut output = match &range.input {
        PromRangeInput::Selector { selector, window }
            if features.window_batches
                && features.window_batch_work_limit
                && range.op.native_name().is_some()
                && matches!(selector.metric, MetricSelection::Exact(_))
                && selector.timing.is_default()
                && [start, stop, step, *window]
                    .into_iter()
                    .all(|value| value % 1_000 == 0) =>
        {
            let MetricSelection::Exact(metric) = &selector.metric else {
                unreachable!("exact metric required by window guard")
            };
            execute_prometheus_window(
                conn,
                features.table,
                metric,
                &selector.filter,
                start / 1_000,
                stop / 1_000,
                step / 1_000,
                *window / 1_000,
                range.op,
                instant,
                limits,
                cancelled,
            )?
        }
        PromRangeInput::Selector { selector, window } => execute_prometheus_range_raw(
            conn,
            features,
            selector,
            start,
            stop,
            step,
            *window,
            range.op,
            parameters.as_deref(),
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            annotation_position,
            cancelled,
        )?,
        PromRangeInput::Subquery(subquery) => execute_prometheus_range_subquery(
            conn,
            features,
            subquery,
            start,
            stop,
            step,
            range.op,
            parameters.as_deref(),
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            annotation_position,
            cancelled,
        )?,
    };
    if matches!(range.op, PromRangeOp::Rate | PromRangeOp::Increase)
        && output.points > 0
        && matches!(&range.input, PromRangeInput::Selector { selector, .. } if matches!(&selector.metric, MetricSelection::Exact(metric) if !prometheus_counter_name(metric)))
    {
        let PromRangeInput::Selector { selector, .. } = &range.input else {
            unreachable!("guarded selector")
        };
        let MetricSelection::Exact(metric) = &selector.metric else {
            unreachable!("guarded exact metric")
        };
        annotations.possible_non_counter(metric, annotation_position);
    }
    if range.op == PromRangeOp::Quantile && output.points > 0 {
        for &(_, quantile) in parameters.as_deref().unwrap_or_default() {
            if quantile.is_nan() || !(0.0..=1.0).contains(&quantile) {
                annotations.invalid_quantile(quantile, annotation_position);
            }
        }
    }
    output.frame_bytes = output.frame_bytes.saturating_add(parameter_frame_bytes);
    output.intermediate_points = output
        .intermediate_points
        .saturating_add(parameter_intermediate_points);
    enforce_intermediate_work(output.intermediate_points, limits)?;
    Ok(output)
}

#[derive(Debug)]
struct IntermediateSeries {
    labels: BTreeMap<String, String>,
    points: Vec<(i64, f64)>,
}

#[derive(Debug)]
enum IntermediateValue {
    Scalar(Vec<(i64, f64)>),
    Vector(Vec<IntermediateSeries>),
}

type PromMatchingKey = Vec<(String, String)>;
type PromStepGroups<'a> = BTreeMap<&'a PromMatchingKey, Vec<(usize, f64)>>;
type PromRankGroupKey = (BTreeMap<String, String>, i64);
type PromRankCandidates<'a> = BTreeMap<PromRankGroupKey, Vec<(&'a BTreeMap<String, String>, f64)>>;

impl IntermediateValue {
    fn points(&self) -> u64 {
        match self {
            Self::Scalar(points) => points.len() as u64,
            Self::Vector(series) => series.iter().map(|series| series.points.len() as u64).sum(),
        }
    }
}

impl PromAggregateGrouping {
    fn output_labels(&self, labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        match self {
            Self::All => BTreeMap::new(),
            Self::By(names) => labels
                .iter()
                .filter(|(name, value)| names.contains(*name) && !value.is_empty())
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            Self::Without(names) => labels
                .iter()
                .filter(|(name, value)| {
                    name.as_str() != "__name__" && !names.contains(*name) && !value.is_empty()
                })
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_aggregate(
    conn: &Connection,
    features: QueryFeatures,
    aggregate: &PromAggregatePlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    let child = execute_prometheus(
        conn,
        features,
        &aggregate.inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let mut intermediate_points = child.intermediate_points.saturating_add(child.points);
    let mut frame_bytes = child.frame_bytes;
    let value = decode_prometheus_intermediate(
        &child.body,
        PromValueType::Vector,
        instant,
        limits,
        cancelled,
    )?;
    let IntermediateValue::Vector(series) = value else {
        unreachable!("aggregation child type was checked while lowering")
    };
    let params = if let Some(param) = &aggregate.param {
        check_cancelled(cancelled)?;
        let output = execute_prometheus(
            conn,
            features,
            param,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        )?;
        intermediate_points = intermediate_points
            .saturating_add(output.intermediate_points)
            .saturating_add(output.points);
        frame_bytes = frame_bytes.saturating_add(output.frame_bytes);
        let IntermediateValue::Scalar(points) = decode_prometheus_intermediate(
            &output.body,
            PromValueType::Scalar,
            instant,
            limits,
            cancelled,
        )?
        else {
            unreachable!("aggregation parameter type was checked while lowering")
        };
        Some(points)
    } else {
        None
    };
    enforce_intermediate_work(intermediate_points, limits)?;
    if aggregate.op == PromAggregateOp::Quantile {
        let position = aggregate
            .source
            .as_ref()
            .map_or(0, |source| source.argument(0));
        let parameters = params.as_deref().unwrap_or_default();
        if parameters.iter().any(|(_, value)| value.is_nan()) {
            annotations.invalid_quantile(f64::NAN, position);
        } else {
            let maximum = parameters
                .iter()
                .map(|(_, value)| *value)
                .fold(f64::NEG_INFINITY, f64::max);
            let minimum = parameters
                .iter()
                .map(|(_, value)| *value)
                .fold(f64::INFINITY, f64::min);
            if maximum > 1.0 {
                annotations.invalid_quantile(maximum, position);
            }
            if minimum < 0.0 {
                annotations.invalid_quantile(minimum, position);
            }
        }
    }
    let series = if aggregate.op.is_ranked() {
        apply_prometheus_ranked(
            aggregate,
            series,
            params.as_deref().unwrap_or_default(),
            instant,
            cancelled,
        )?
    } else if aggregate.op == PromAggregateOp::Quantile {
        apply_prometheus_quantile(
            aggregate,
            series,
            params.as_deref().unwrap_or_default(),
            cancelled,
        )?
    } else if aggregate.op == PromAggregateOp::CountValues {
        apply_prometheus_count_values(aggregate, series, cancelled)?
    } else {
        apply_prometheus_aggregate(aggregate, series, cancelled)?
    };
    encode_prometheus_intermediate(
        IntermediateValue::Vector(series),
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

impl PromAggregateOp {
    fn is_ranked(self) -> bool {
        matches!(self, Self::TopK | Self::BottomK)
    }
}

fn apply_prometheus_ranked(
    aggregate: &PromAggregatePlan,
    series: Vec<IntermediateSeries>,
    params: &[(i64, f64)],
    instant: bool,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    if params.iter().any(|(_, value)| value.is_nan()) {
        return Err("Parameter value is NaN".into());
    }
    let max_param = params
        .iter()
        .map(|(_, value)| *value)
        .fold(f64::NEG_INFINITY, f64::max);
    if max_param < 1.0 {
        return Ok(Vec::new());
    }
    let min_param = params
        .iter()
        .map(|(_, value)| *value)
        .fold(f64::INFINITY, f64::min);
    if min_param <= i64::MIN as f64 {
        return Err(format!(
            "Scalar value {} underflows int64",
            format_prometheus_value(min_param)
        ));
    }
    if max_param >= i64::MAX as f64 {
        return Err(format!(
            "Scalar value {} overflows int64",
            format_prometheus_value(max_param)
        ));
    }
    let params: BTreeMap<i64, f64> = params.iter().copied().collect();
    let mut groups: PromRankCandidates<'_> = BTreeMap::new();
    for item in &series {
        check_cancelled(cancelled)?;
        let group_labels = aggregate.grouping.output_labels(&item.labels);
        for (timestamp, value) in &item.points {
            check_cancelled(cancelled)?;
            groups
                .entry((group_labels.clone(), *timestamp))
                .or_default()
                .push((&item.labels, *value));
        }
    }

    let mut selected = Vec::new();
    for ((_group, timestamp), mut values) in groups {
        check_cancelled(cancelled)?;
        let Some(param) = params.get(&timestamp) else {
            continue;
        };
        let k = (*param as i64).max(0) as usize;
        if k == 0 {
            continue;
        }
        values.sort_by(|(left_labels, left), (right_labels, right)| {
            prometheus_rank_order(aggregate.op, *left, *right)
                .then_with(|| left_labels.cmp(right_labels))
        });
        selected.extend(
            values
                .into_iter()
                .take(k)
                .map(|(labels, value)| (labels.clone(), timestamp, value)),
        );
    }

    if instant {
        return Ok(selected
            .into_iter()
            .map(|(labels, timestamp, value)| IntermediateSeries {
                labels,
                points: vec![(timestamp, value)],
            })
            .collect());
    }
    let mut output: BTreeMap<BTreeMap<String, String>, Vec<(i64, f64)>> = BTreeMap::new();
    for (labels, timestamp, value) in selected {
        output.entry(labels).or_default().push((timestamp, value));
    }
    Ok(output
        .into_iter()
        .map(|(labels, points)| IntermediateSeries { labels, points })
        .collect())
}

fn prometheus_rank_order(op: PromAggregateOp, left: f64, right: f64) -> std::cmp::Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) if op == PromAggregateOp::TopK => right.total_cmp(&left),
        (false, false) => left.total_cmp(&right),
    }
}

fn apply_prometheus_quantile(
    aggregate: &PromAggregatePlan,
    series: Vec<IntermediateSeries>,
    params: &[(i64, f64)],
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    let params: BTreeMap<i64, f64> = params.iter().copied().collect();
    let mut groups: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, Vec<f64>>> = BTreeMap::new();
    for item in series {
        check_cancelled(cancelled)?;
        let labels = aggregate.grouping.output_labels(&item.labels);
        let group = groups.entry(labels).or_default();
        for (timestamp, value) in item.points {
            check_cancelled(cancelled)?;
            group.entry(timestamp).or_default().push(value);
        }
    }
    Ok(groups
        .into_iter()
        .map(|(labels, points)| IntermediateSeries {
            labels,
            points: points
                .into_iter()
                .filter_map(|(timestamp, mut values)| {
                    let quantile = params.get(&timestamp)?;
                    Some((timestamp, prometheus_quantile(*quantile, &mut values)))
                })
                .collect(),
        })
        .collect())
}

fn prometheus_quantile(quantile: f64, values: &mut [f64]) -> f64 {
    if values.is_empty() || quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }
    values.sort_by(|left, right| match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => left
            .partial_cmp(right)
            .expect("non-NaN floats have a total numeric order"),
    });
    let rank = quantile * (values.len() as f64 - 1.0);
    let lower = rank.floor().max(0.0) as usize;
    let upper = (lower + 1).min(values.len() - 1);
    let weight = rank - rank.floor();
    values[lower] * (1.0 - weight) + values[upper] * weight
}

fn apply_prometheus_count_values(
    aggregate: &PromAggregatePlan,
    series: Vec<IntermediateSeries>,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    let value_label = aggregate
        .value_label
        .as_deref()
        .ok_or_else(|| "count_values is missing its value label".to_string())?;
    if value_label.is_empty() {
        return Err("invalid label name \"\"".into());
    }
    let mut groups: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, u64>> = BTreeMap::new();
    for item in series {
        check_cancelled(cancelled)?;
        for (timestamp, value) in item.points {
            check_cancelled(cancelled)?;
            let formatted = format_prometheus_label_value(value);
            let labels = match &aggregate.grouping {
                PromAggregateGrouping::All => {
                    BTreeMap::from([(value_label.to_string(), formatted)])
                }
                PromAggregateGrouping::By(names) => {
                    let mut labels: BTreeMap<_, _> = item
                        .labels
                        .iter()
                        .filter(|(name, value)| names.contains(*name) && !value.is_empty())
                        .map(|(name, value)| (name.clone(), value.clone()))
                        .collect();
                    labels.insert(value_label.to_string(), formatted);
                    labels
                }
                PromAggregateGrouping::Without(names) => {
                    let mut labels = item.labels.clone();
                    labels.insert(value_label.to_string(), formatted);
                    labels
                        .into_iter()
                        .filter(|(name, value)| {
                            name != "__name__" && !names.contains(name) && !value.is_empty()
                        })
                        .collect()
                }
            };
            *groups
                .entry(labels)
                .or_default()
                .entry(timestamp)
                .or_default() += 1;
        }
    }
    Ok(groups
        .into_iter()
        .map(|(labels, points)| IntermediateSeries {
            labels,
            points: points
                .into_iter()
                .map(|(timestamp, count)| (timestamp, count as f64))
                .collect(),
        })
        .collect())
}

fn format_prometheus_label_value(value: f64) -> String {
    if !value.is_finite() {
        return format_prometheus_value(value);
    }
    let rendered = value.to_string();
    let Some(exponent_at) = rendered.find(['e', 'E']) else {
        return rendered;
    };
    let (mantissa, exponent) = rendered.split_at(exponent_at);
    let exponent: i32 = exponent[1..]
        .parse()
        .expect("Rust float formatting emits a decimal exponent");
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |unsigned| ("-", unsigned));
    let decimal_at = mantissa.find('.').unwrap_or(mantissa.len());
    let digits = mantissa.replace('.', "");
    let output_decimal_at = decimal_at as i32 + exponent;
    let expanded = if output_decimal_at <= 0 {
        format!("0.{}{}", "0".repeat((-output_decimal_at) as usize), digits)
    } else if output_decimal_at as usize >= digits.len() {
        format!(
            "{}{}",
            digits,
            "0".repeat(output_decimal_at as usize - digits.len())
        )
    } else {
        let split = output_decimal_at as usize;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    format!("{sign}{expanded}")
}

fn apply_prometheus_aggregate(
    aggregate: &PromAggregatePlan,
    series: Vec<IntermediateSeries>,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    let mut groups: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, PromAggregateState>> =
        BTreeMap::new();
    for series in series {
        check_cancelled(cancelled)?;
        let labels = aggregate.grouping.output_labels(&series.labels);
        let group = groups.entry(labels).or_default();
        for (timestamp, value) in series.points {
            check_cancelled(cancelled)?;
            match group.entry(timestamp) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(PromAggregateState::new(aggregate.op, value));
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().add(aggregate.op, value);
                }
            }
        }
    }
    Ok(groups
        .into_iter()
        .map(|(labels, points)| IntermediateSeries {
            labels,
            points: points
                .into_iter()
                .map(|(timestamp, state)| (timestamp, state.finish(aggregate.op)))
                .collect(),
        })
        .collect())
}

#[derive(Clone, Copy, Debug)]
struct PromAggregateState {
    value: f64,
    compensation: f64,
    mean: f64,
    count: f64,
    incremental_mean: bool,
    variance: f64,
}

impl PromAggregateState {
    fn new(op: PromAggregateOp, value: f64) -> Self {
        Self {
            value,
            compensation: 0.0,
            mean: value,
            count: 1.0,
            incremental_mean: false,
            variance: if matches!(op, PromAggregateOp::StdDev | PromAggregateOp::StdVar)
                && !value.is_finite()
            {
                f64::NAN
            } else {
                0.0
            },
        }
    }

    fn add(&mut self, op: PromAggregateOp, value: f64) {
        match op {
            PromAggregateOp::Sum => {
                (self.value, self.compensation) =
                    prometheus_compensated_add(value, self.value, self.compensation);
            }
            PromAggregateOp::Avg => {
                self.count += 1.0;
                if !self.incremental_mean {
                    let (sum, compensation) =
                        prometheus_compensated_add(value, self.value, self.compensation);
                    if !sum.is_infinite() {
                        self.value = sum;
                        self.compensation = compensation;
                        return;
                    }
                    self.incremental_mean = true;
                    self.mean = self.value / (self.count - 1.0);
                    self.compensation /= self.count - 1.0;
                }
                let previous_weight = (self.count - 1.0) / self.count;
                (self.mean, self.compensation) = prometheus_compensated_add(
                    value / self.count,
                    previous_weight * self.mean,
                    previous_weight * self.compensation,
                );
            }
            PromAggregateOp::Min => {
                if self.value > value || self.value.is_nan() {
                    self.value = value;
                }
            }
            PromAggregateOp::Max => {
                if self.value < value || self.value.is_nan() {
                    self.value = value;
                }
            }
            PromAggregateOp::Count => self.count += 1.0,
            PromAggregateOp::Group => {}
            PromAggregateOp::StdDev | PromAggregateOp::StdVar => {
                self.count += 1.0;
                let delta = value - self.mean;
                self.mean += delta / self.count;
                self.variance += delta * (value - self.mean);
            }
            PromAggregateOp::TopK
            | PromAggregateOp::BottomK
            | PromAggregateOp::Quantile
            | PromAggregateOp::CountValues => {
                unreachable!("ranking aggregations do not use reduction state")
            }
        }
    }

    fn finish(self, op: PromAggregateOp) -> f64 {
        match op {
            PromAggregateOp::Sum => self.value + self.compensation,
            PromAggregateOp::Avg if self.incremental_mean => self.mean + self.compensation,
            PromAggregateOp::Avg => self.value / self.count + self.compensation / self.count,
            PromAggregateOp::Min | PromAggregateOp::Max => self.value,
            PromAggregateOp::Count => self.count,
            PromAggregateOp::Group => 1.0,
            PromAggregateOp::StdDev => (self.variance / self.count).sqrt(),
            PromAggregateOp::StdVar => self.variance / self.count,
            PromAggregateOp::TopK
            | PromAggregateOp::BottomK
            | PromAggregateOp::Quantile
            | PromAggregateOp::CountValues => {
                unreachable!("ranking aggregations do not use reduction state")
            }
        }
    }
}

fn prometheus_compensated_add(increment: f64, sum: f64, compensation: f64) -> (f64, f64) {
    let total = sum + increment;
    let compensation = if total.is_infinite() {
        0.0
    } else if sum.abs() >= increment.abs() {
        compensation + ((sum - total) + increment)
    } else {
        compensation + ((increment - total) + sum)
    };
    (total, compensation)
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_unary(
    conn: &Connection,
    features: QueryFeatures,
    inner: &PromPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    let value_type = inner.value_type();
    let child = execute_prometheus(
        conn,
        features,
        inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let intermediate_points = child.intermediate_points.saturating_add(child.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let frame_bytes = child.frame_bytes;
    let mut value =
        decode_prometheus_intermediate(&child.body, value_type, instant, limits, cancelled)?;
    match &mut value {
        IntermediateValue::Scalar(points) => {
            for (_, value) in points {
                check_cancelled(cancelled)?;
                *value = -*value;
            }
        }
        IntermediateValue::Vector(series) => {
            for series in series {
                check_cancelled(cancelled)?;
                // Prometheus removes the metric name for every unary expression,
                // including a double negation. Other labels remain unchanged.
                series.labels.remove("__name__");
                for (_, value) in &mut series.points {
                    check_cancelled(cancelled)?;
                    *value = -*value;
                }
            }
        }
    }
    encode_prometheus_intermediate(
        value,
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_function(
    conn: &Connection,
    features: QueryFeatures,
    function: &PromFunctionPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    let child = execute_prometheus(
        conn,
        features,
        &function.inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let mut intermediate_points = child.intermediate_points.saturating_add(child.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let mut frame_bytes = child.frame_bytes;
    let mut parameters = Vec::with_capacity(function.parameters.len());
    for parameter in &function.parameters {
        check_cancelled(cancelled)?;
        let output = execute_prometheus(
            conn,
            features,
            parameter,
            start,
            stop,
            step,
            instant,
            query_start,
            query_end,
            limits,
            annotations,
            cancelled,
        )?;
        intermediate_points = intermediate_points
            .saturating_add(output.intermediate_points)
            .saturating_add(output.points);
        enforce_intermediate_work(intermediate_points, limits)?;
        frame_bytes = frame_bytes.saturating_add(output.frame_bytes);
        let IntermediateValue::Scalar(points) = decode_prometheus_intermediate(
            &output.body,
            PromValueType::Scalar,
            instant,
            limits,
            cancelled,
        )?
        else {
            unreachable!("function parameter type was checked while lowering")
        };
        parameters.push(points.into_iter().collect::<BTreeMap<_, _>>());
    }
    let IntermediateValue::Vector(mut series) = decode_prometheus_intermediate(
        &child.body,
        PromValueType::Vector,
        instant,
        limits,
        cancelled,
    )?
    else {
        unreachable!("function input type was checked while lowering")
    };
    for item in &mut series {
        check_cancelled(cancelled)?;
        item.labels.remove("__name__");
        let mut values = Vec::with_capacity(parameters.len());
        let mut write_index = 0;
        for read_index in 0..item.points.len() {
            check_cancelled(cancelled)?;
            let (timestamp, value) = item.points[read_index];
            values.clear();
            for parameter in &parameters {
                values.push(parameter.get(&timestamp).copied().ok_or_else(|| {
                    "PromQL function parameter is missing an evaluation timestamp".to_string()
                })?);
            }
            if let Some(value) = function.op.apply(value, &values) {
                item.points[write_index] = (timestamp, value);
                write_index += 1;
            }
        }
        item.points.truncate(write_index);
    }
    series.retain(|item| !item.points.is_empty());
    encode_prometheus_intermediate(
        IntermediateValue::Vector(series),
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

#[derive(Clone, Copy, Debug)]
struct PromClassicBucket {
    upper_bound: f64,
    count: f64,
}

#[derive(Clone, Copy, Debug)]
struct PromHistogramRepair {
    min_bucket: f64,
    max_bucket: f64,
    max_diff: f64,
}

#[derive(Clone, Copy, Debug)]
struct PromClassicQuantile {
    value: f64,
    repair: Option<PromHistogramRepair>,
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_histogram_quantile(
    conn: &Connection,
    features: QueryFeatures,
    histogram: &PromHistogramQuantilePlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    let quantile = execute_prometheus(
        conn,
        features,
        &histogram.quantile,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let buckets = execute_prometheus(
        conn,
        features,
        &histogram.inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let frame_bytes = quantile.frame_bytes.saturating_add(buckets.frame_bytes);
    let intermediate_points = quantile
        .intermediate_points
        .saturating_add(quantile.points)
        .saturating_add(buckets.intermediate_points)
        .saturating_add(buckets.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let IntermediateValue::Scalar(quantiles) = decode_prometheus_intermediate(
        &quantile.body,
        PromValueType::Scalar,
        instant,
        limits,
        cancelled,
    )?
    else {
        unreachable!("histogram quantile type was checked while lowering")
    };
    let IntermediateValue::Vector(series) = decode_prometheus_intermediate(
        &buckets.body,
        PromValueType::Vector,
        instant,
        limits,
        cancelled,
    )?
    else {
        unreachable!("histogram bucket type was checked while lowering")
    };
    let quantile_position = histogram
        .source
        .as_ref()
        .map_or(0, |source| source.argument(0));
    let bucket_position = histogram
        .source
        .as_ref()
        .map_or(0, |source| source.argument(1));
    if !series.is_empty() {
        for &(_, quantile) in &quantiles {
            if quantile.is_nan() || !(0.0..=1.0).contains(&quantile) {
                annotations.invalid_quantile(quantile, quantile_position);
            }
        }
    }
    let quantiles: BTreeMap<i64, f64> = quantiles.into_iter().collect();
    let mut groups: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, Vec<PromClassicBucket>>> =
        BTreeMap::new();
    for item in series {
        check_cancelled(cancelled)?;
        let bucket_label = item.labels.get("le").map_or("", String::as_str);
        let upper_bound = match parse_prometheus_bucket_bound(bucket_label) {
            Ok(upper_bound) => upper_bound,
            Err(()) => {
                annotations.bad_bucket_label(bucket_label, bucket_position);
                continue;
            }
        };
        let mut labels = item.labels;
        labels.remove("le");
        let group = groups.entry(labels).or_default();
        for (timestamp, count) in item.points {
            check_cancelled(cancelled)?;
            group
                .entry(timestamp)
                .or_default()
                .push(PromClassicBucket { upper_bound, count });
        }
    }

    let mut output = Vec::with_capacity(groups.len());
    for (mut labels, steps) in groups {
        check_cancelled(cancelled)?;
        // The metric name separates bucket families while grouping but every
        // histogram_quantile result is nameless.
        labels.remove("__name__");
        let mut points = Vec::with_capacity(steps.len());
        for (timestamp, buckets) in steps {
            check_cancelled(cancelled)?;
            let quantile = quantiles.get(&timestamp).copied().ok_or_else(|| {
                "histogram_quantile scalar is missing an evaluation timestamp".to_string()
            })?;
            let quantile = prometheus_classic_bucket_quantile(quantile, buckets, cancelled)?;
            if let Some(repair) = quantile.repair {
                annotations.histogram_monotonicity(
                    bucket_position,
                    timestamp,
                    repair.min_bucket,
                    repair.max_bucket,
                    repair.max_diff,
                );
            }
            points.push((timestamp, quantile.value));
        }
        if !points.is_empty() {
            output.push(IntermediateSeries { labels, points });
        }
    }
    let output = normalize_prometheus_vector(output, cancelled)?;
    encode_prometheus_intermediate(
        IntermediateValue::Vector(output),
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

fn parse_prometheus_bucket_bound(value: &str) -> Result<f64, ()> {
    match value {
        "NaN" => Ok(f64::NAN),
        "+Inf" | "Inf" | "+Infinity" | "Infinity" => Ok(f64::INFINITY),
        "-Inf" | "-Infinity" => Ok(f64::NEG_INFINITY),
        _ if value
            .bytes()
            .any(|byte| byte.is_ascii_alphabetic() && !matches!(byte, b'e' | b'E')) =>
        {
            Err(())
        }
        _ => value.parse::<f64>().map_err(|_| ()),
    }
}

fn prometheus_almost_equal(lhs: f64, rhs: f64, tolerance: f64) -> bool {
    if lhs == rhs {
        return true;
    }
    let lhs_abs = lhs.abs();
    let rhs_abs = rhs.abs();
    let sum = lhs_abs + rhs_abs;
    let difference = (lhs - rhs).abs();
    if lhs == 0.0 || rhs == 0.0 || sum < f64::MIN_POSITIVE {
        difference < tolerance * f64::MIN_POSITIVE
    } else {
        difference / sum.min(f64::MAX) < tolerance
    }
}

fn prometheus_classic_bucket_quantile(
    quantile: f64,
    mut buckets: Vec<PromClassicBucket>,
    cancelled: &AtomicBool,
) -> Result<PromClassicQuantile, String> {
    let without_repair = |value| PromClassicQuantile {
        value,
        repair: None,
    };
    if quantile.is_nan() {
        return Ok(without_repair(f64::NAN));
    }
    if quantile < 0.0 {
        return Ok(without_repair(f64::NEG_INFINITY));
    }
    if quantile > 1.0 {
        return Ok(without_repair(f64::INFINITY));
    }
    buckets.sort_by(|lhs, rhs| {
        if lhs.upper_bound < rhs.upper_bound {
            std::cmp::Ordering::Less
        } else if rhs.upper_bound < lhs.upper_bound {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
    if !buckets
        .last()
        .is_some_and(|bucket| bucket.upper_bound == f64::INFINITY)
    {
        return Ok(without_repair(f64::NAN));
    }

    let mut coalesced: Vec<PromClassicBucket> = Vec::with_capacity(buckets.len());
    for bucket in buckets {
        check_cancelled(cancelled)?;
        if let Some(previous) = coalesced
            .last_mut()
            .filter(|previous| previous.upper_bound == bucket.upper_bound)
        {
            previous.count += bucket.count;
        } else {
            coalesced.push(bucket);
        }
    }
    let mut previous = coalesced.first().map_or(0.0, |bucket| bucket.count);
    let mut repair: Option<PromHistogramRepair> = None;
    for bucket in coalesced.iter_mut().skip(1) {
        check_cancelled(cancelled)?;
        if bucket.count == previous {
            continue;
        }
        if prometheus_almost_equal(bucket.count, previous, 1e-12) {
            bucket.count = previous;
            continue;
        }
        if bucket.count < previous {
            let difference = previous - bucket.count;
            match &mut repair {
                Some(repair) => {
                    repair.min_bucket = repair.min_bucket.min(bucket.upper_bound);
                    repair.max_bucket = repair.max_bucket.max(bucket.upper_bound);
                    repair.max_diff = repair.max_diff.max(difference);
                }
                None => {
                    repair = Some(PromHistogramRepair {
                        min_bucket: bucket.upper_bound,
                        max_bucket: bucket.upper_bound,
                        max_diff: difference,
                    });
                }
            }
            bucket.count = previous;
            continue;
        }
        previous = bucket.count;
    }
    if coalesced.len() < 2 {
        return Ok(PromClassicQuantile {
            value: f64::NAN,
            repair,
        });
    }
    let observations = coalesced.last().expect("nonempty buckets").count;
    if observations == 0.0 {
        return Ok(PromClassicQuantile {
            value: f64::NAN,
            repair,
        });
    }
    let mut rank = quantile * observations;
    let bucket_index =
        coalesced[..coalesced.len() - 1].partition_point(|bucket| bucket.count < rank);
    if bucket_index == coalesced.len() - 1 {
        return Ok(PromClassicQuantile {
            value: coalesced[bucket_index - 1].upper_bound,
            repair,
        });
    }
    let bucket = coalesced[bucket_index];
    if bucket_index == 0 && bucket.upper_bound <= 0.0 {
        return Ok(PromClassicQuantile {
            value: bucket.upper_bound,
            repair,
        });
    }
    let (lower_bound, lower_count) = if bucket_index == 0 {
        (0.0, 0.0)
    } else {
        let lower = coalesced[bucket_index - 1];
        (lower.upper_bound, lower.count)
    };
    rank -= lower_count;
    let bucket_count = bucket.count - lower_count;
    Ok(PromClassicQuantile {
        value: lower_bound + (bucket.upper_bound - lower_bound) * (rank / bucket_count),
        repair,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_label_replace(
    conn: &Connection,
    features: QueryFeatures,
    label_replace: &PromLabelReplacePlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    if label_replace.destination.is_empty() {
        return Err("invalid destination label name in label_replace(): \"\"".into());
    }
    let pattern = Regex::new(&format!("^(?s:{})$", label_replace.pattern))
        .map_err(|error| format!("invalid regular expression in label_replace(): {}", error))?;
    let child = execute_prometheus(
        conn,
        features,
        &label_replace.inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let intermediate_points = child.intermediate_points.saturating_add(child.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let frame_bytes = child.frame_bytes;
    let IntermediateValue::Vector(mut series) = decode_prometheus_intermediate(
        &child.body,
        PromValueType::Vector,
        instant,
        limits,
        cancelled,
    )?
    else {
        unreachable!("label_replace input type was checked while lowering")
    };
    let mut generated_label_bytes = 0_usize;
    for item in &mut series {
        check_cancelled(cancelled)?;
        let source = item
            .labels
            .get(&label_replace.source)
            .cloned()
            .unwrap_or_default();
        let Some(captures) = pattern.captures(&source) else {
            continue;
        };
        let remaining = limits
            .max_response_bytes
            .saturating_sub(generated_label_bytes);
        let replacement = expand_prometheus_replacement_bounded(
            &captures,
            &label_replace.replacement,
            remaining,
            limits,
        )?;
        if replacement.is_empty() {
            item.labels.remove(&label_replace.destination);
        } else {
            generated_label_bytes = generated_label_bytes
                .checked_add(label_replace.destination.len())
                .and_then(|bytes| bytes.checked_add(replacement.len()))
                .filter(|bytes| *bytes <= limits.max_response_bytes)
                .ok_or_else(|| prometheus_response_limit_error(limits))?;
            item.labels
                .insert(label_replace.destination.clone(), replacement);
        }
    }
    encode_prometheus_intermediate(
        IntermediateValue::Vector(series),
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_label_join(
    conn: &Connection,
    features: QueryFeatures,
    label_join: &PromLabelJoinPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    if label_join.destination.is_empty() {
        return Err("invalid destination label name in label_join(): \"\"".into());
    }
    let child = execute_prometheus(
        conn,
        features,
        &label_join.inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let intermediate_points = child.intermediate_points.saturating_add(child.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let frame_bytes = child.frame_bytes;
    let IntermediateValue::Vector(mut series) = decode_prometheus_intermediate(
        &child.body,
        PromValueType::Vector,
        instant,
        limits,
        cancelled,
    )?
    else {
        unreachable!("label_join input type was checked while lowering")
    };
    let mut generated_label_bytes = 0_usize;
    for item in &mut series {
        check_cancelled(cancelled)?;
        let mut joined = String::new();
        let remaining = limits
            .max_response_bytes
            .saturating_sub(generated_label_bytes);
        for (index, source) in label_join.sources.iter().enumerate() {
            if index > 0 {
                push_prometheus_label_fragment(
                    &mut joined,
                    &label_join.separator,
                    remaining,
                    limits,
                )?;
            }
            if let Some(value) = item.labels.get(source) {
                push_prometheus_label_fragment(&mut joined, value, remaining, limits)?;
            }
        }
        if joined.is_empty() {
            item.labels.remove(&label_join.destination);
        } else {
            generated_label_bytes = generated_label_bytes
                .checked_add(label_join.destination.len())
                .and_then(|bytes| bytes.checked_add(joined.len()))
                .filter(|bytes| *bytes <= limits.max_response_bytes)
                .ok_or_else(|| prometheus_response_limit_error(limits))?;
            item.labels.insert(label_join.destination.clone(), joined);
        }
    }
    encode_prometheus_intermediate(
        IntermediateValue::Vector(series),
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

fn expand_prometheus_replacement_bounded(
    captures: &regex::Captures<'_>,
    replacement: &str,
    limit: usize,
    limits: PromQueryLimits,
) -> Result<String, String> {
    let mut output = String::new();
    let mut remaining = replacement;
    while let Some(dollar) = remaining.find('$') {
        push_prometheus_label_fragment(&mut output, &remaining[..dollar], limit, limits)?;
        remaining = &remaining[dollar + 1..];
        if let Some(rest) = remaining.strip_prefix('$') {
            push_prometheus_label_fragment(&mut output, "$", limit, limits)?;
            remaining = rest;
            continue;
        }
        let (reference, rest) = if let Some(braced) = remaining.strip_prefix('{') {
            let Some(end) = braced.find('}') else {
                push_prometheus_label_fragment(&mut output, "$", limit, limits)?;
                continue;
            };
            (&braced[..end], &braced[end + 1..])
        } else {
            let end = remaining
                .bytes()
                .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                .count();
            if end == 0 {
                push_prometheus_label_fragment(&mut output, "$", limit, limits)?;
                continue;
            }
            (&remaining[..end], &remaining[end..])
        };
        remaining = rest;
        let captured =
            if !reference.is_empty() && reference.bytes().all(|byte| byte.is_ascii_digit()) {
                reference
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| captures.get(index))
            } else {
                captures.name(reference)
            };
        if let Some(captured) = captured {
            push_prometheus_label_fragment(&mut output, captured.as_str(), limit, limits)?;
        }
    }
    push_prometheus_label_fragment(&mut output, remaining, limit, limits)?;
    Ok(output)
}

fn push_prometheus_label_fragment(
    output: &mut String,
    fragment: &str,
    limit: usize,
    limits: PromQueryLimits,
) -> Result<(), String> {
    if output.len().saturating_add(fragment.len()) > limit {
        return Err(prometheus_response_limit_error(limits));
    }
    output.push_str(fragment);
    Ok(())
}

fn prometheus_response_limit_error(limits: PromQueryLimits) -> String {
    format!(
        "query exceeded the maximum response-size limit of {} bytes",
        limits.max_response_bytes
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_absent(
    conn: &Connection,
    features: QueryFeatures,
    absent: &PromAbsentPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    let child = execute_prometheus(
        conn,
        features,
        &absent.inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let frame_bytes = child.frame_bytes;
    let child_points = child.points;
    let mut intermediate_points = child.intermediate_points.saturating_add(child_points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let IntermediateValue::Vector(series) = decode_prometheus_intermediate(
        &child.body,
        PromValueType::Vector,
        instant,
        limits,
        cancelled,
    )?
    else {
        unreachable!("absent input type was checked while lowering")
    };
    let present: HashSet<i64> = series
        .iter()
        .flat_map(|item| item.points.iter().map(|(timestamp, _)| *timestamp))
        .collect();
    let mut points = Vec::new();
    let mut timestamp = start;
    loop {
        check_cancelled(cancelled)?;
        intermediate_points = intermediate_points.saturating_add(1);
        enforce_intermediate_work(intermediate_points, limits)?;
        if !present.contains(&timestamp) {
            points.push((timestamp, 1.0));
        }
        if timestamp >= stop {
            break;
        }
        let Some(next) = timestamp.checked_add(step).filter(|next| *next <= stop) else {
            break;
        };
        timestamp = next;
    }
    let output = if points.is_empty() {
        Vec::new()
    } else {
        vec![IntermediateSeries {
            labels: absent.labels.clone(),
            points,
        }]
    };
    encode_prometheus_intermediate(
        IntermediateValue::Vector(output),
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_sort(
    conn: &Connection,
    features: QueryFeatures,
    sort: &PromSortPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    if !instant {
        annotations.warning(
            "PromQL warning: sort is ineffective for range queries since results are always ordered by labels".into(),
            sort.source.as_ref().map_or(0, |source| source.start),
        );
    }
    let child = execute_prometheus(
        conn,
        features,
        &sort.inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let frame_bytes = child.frame_bytes;
    let intermediate_points = child.intermediate_points.saturating_add(child.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let IntermediateValue::Vector(mut series) = decode_prometheus_intermediate(
        &child.body,
        PromValueType::Vector,
        instant,
        limits,
        cancelled,
    )?
    else {
        unreachable!("sort input type was checked while lowering")
    };
    check_cancelled(cancelled)?;
    if instant {
        series.sort_by(|left, right| {
            let left_value = left.points[0].1;
            let right_value = right.points[0].1;
            prometheus_sort_order(sort.descending, left_value, right_value)
                .then_with(|| left.labels.cmp(&right.labels))
        });
    } else {
        // Prometheus's range-query result is a matrix whose series ordering is
        // always label-based; an instant-vector sort cannot reorder it per step.
        series.sort_by(|left, right| left.labels.cmp(&right.labels));
    }
    check_cancelled(cancelled)?;
    encode_prometheus_intermediate(
        IntermediateValue::Vector(series),
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

fn prometheus_sort_order(descending: bool, left: f64, right: f64) -> std::cmp::Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => {
            let order = left
                .partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal);
            if descending {
                order.reverse()
            } else {
                order
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_conversion(
    conn: &Connection,
    features: QueryFeatures,
    conversion: &PromConversionPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    let child_type = conversion.inner.value_type();
    let child = execute_prometheus(
        conn,
        features,
        &conversion.inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let frame_bytes = child.frame_bytes;
    let mut intermediate_points = child.intermediate_points.saturating_add(child.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let child =
        decode_prometheus_intermediate(&child.body, child_type, instant, limits, cancelled)?;
    let value = match (conversion.kind, child) {
        (PromConversionKind::Vector, IntermediateValue::Scalar(points)) => {
            IntermediateValue::Vector(vec![IntermediateSeries {
                labels: BTreeMap::new(),
                points,
            }])
        }
        (PromConversionKind::Scalar, IntermediateValue::Vector(series)) => {
            let mut samples: BTreeMap<i64, (usize, f64)> = BTreeMap::new();
            for item in series {
                check_cancelled(cancelled)?;
                for (timestamp, value) in item.points {
                    check_cancelled(cancelled)?;
                    let entry = samples.entry(timestamp).or_insert((0, value));
                    entry.0 = entry.0.saturating_add(1);
                    entry.1 = value;
                }
            }
            let mut points = Vec::new();
            let mut timestamp = start;
            loop {
                check_cancelled(cancelled)?;
                intermediate_points = intermediate_points.saturating_add(1);
                enforce_intermediate_work(intermediate_points, limits)?;
                let value = match samples.get(&timestamp) {
                    Some((1, value)) => *value,
                    _ => f64::NAN,
                };
                points.push((timestamp, value));
                if timestamp >= stop {
                    break;
                }
                let Some(next) = timestamp.checked_add(step).filter(|next| *next <= stop) else {
                    break;
                };
                timestamp = next;
            }
            IntermediateValue::Scalar(points)
        }
        _ => unreachable!("conversion input type was checked while lowering"),
    };
    encode_prometheus_intermediate(
        value,
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

fn execute_prometheus_time(
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let mut points = Vec::new();
    let mut timestamp = start;
    loop {
        check_cancelled(cancelled)?;
        points.push((timestamp, timestamp as f64 / 1_000.0));
        if timestamp >= stop {
            break;
        }
        let Some(next) = timestamp.checked_add(step).filter(|next| *next <= stop) else {
            break;
        };
        timestamp = next;
    }
    encode_prometheus_intermediate(
        IntermediateValue::Scalar(points),
        instant,
        0,
        0,
        limits,
        cancelled,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_timestamp(
    conn: &Connection,
    features: QueryFeatures,
    timestamp: &PromTimestampPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    if let PromPlan::Selector { selector, lookback } = timestamp.inner.as_ref() {
        let mut output = execute_prometheus_selector_value(
            conn,
            features,
            selector,
            start,
            stop,
            step,
            *lookback,
            instant,
            query_start,
            query_end,
            limits,
            cancelled,
            true,
        )?;
        output.intermediate_points = output.points;
        enforce_intermediate_work(output.intermediate_points, limits)?;
        return Ok(output);
    }

    check_cancelled(cancelled)?;
    let child = execute_prometheus(
        conn,
        features,
        &timestamp.inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let frame_bytes = child.frame_bytes;
    let intermediate_points = child.intermediate_points.saturating_add(child.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let IntermediateValue::Vector(mut series) = decode_prometheus_intermediate(
        &child.body,
        PromValueType::Vector,
        instant,
        limits,
        cancelled,
    )?
    else {
        unreachable!("timestamp input type was checked while lowering")
    };
    for item in &mut series {
        check_cancelled(cancelled)?;
        item.labels.remove("__name__");
        for (evaluation_time, value) in &mut item.points {
            check_cancelled(cancelled)?;
            *value = *evaluation_time as f64 / 1_000.0;
        }
    }
    encode_prometheus_intermediate(
        IntermediateValue::Vector(series),
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_calendar(
    conn: &Connection,
    features: QueryFeatures,
    calendar: &PromCalendarPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    let child = execute_prometheus(
        conn,
        features,
        &calendar.inner,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let frame_bytes = child.frame_bytes;
    let intermediate_points = child.intermediate_points.saturating_add(child.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let IntermediateValue::Vector(mut series) = decode_prometheus_intermediate(
        &child.body,
        PromValueType::Vector,
        instant,
        limits,
        cancelled,
    )?
    else {
        unreachable!("calendar input type was checked while lowering")
    };
    for item in &mut series {
        check_cancelled(cancelled)?;
        item.labels.remove("__name__");
        for (_, value) in &mut item.points {
            check_cancelled(cancelled)?;
            *value = calendar.op.apply(*value);
        }
    }
    encode_prometheus_intermediate(
        IntermediateValue::Vector(series),
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_binary(
    conn: &Connection,
    features: QueryFeatures,
    binary: &PromBinaryPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    let lhs_type = binary.lhs.value_type();
    let rhs_type = binary.rhs.value_type();
    let lhs = execute_prometheus(
        conn,
        features,
        &binary.lhs,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    check_cancelled(cancelled)?;
    let rhs = execute_prometheus(
        conn,
        features,
        &binary.rhs,
        start,
        stop,
        step,
        instant,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let intermediate_points = lhs
        .intermediate_points
        .saturating_add(lhs.points)
        .saturating_add(rhs.intermediate_points)
        .saturating_add(rhs.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let frame_bytes = lhs.frame_bytes.saturating_add(rhs.frame_bytes);
    let lhs = decode_prometheus_intermediate(&lhs.body, lhs_type, instant, limits, cancelled)?;
    let rhs = decode_prometheus_intermediate(&rhs.body, rhs_type, instant, limits, cancelled)?;
    let value = apply_prometheus_binary(
        binary.op,
        binary.return_bool,
        &binary.matching,
        &binary.cardinality,
        lhs,
        rhs,
        cancelled,
    )?;
    encode_prometheus_intermediate(
        value,
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

fn apply_prometheus_binary(
    op: PromBinaryOp,
    return_bool: bool,
    matching: &PromVectorMatching,
    cardinality: &PromVectorCardinality,
    lhs: IntermediateValue,
    rhs: IntermediateValue,
    cancelled: &AtomicBool,
) -> Result<IntermediateValue, String> {
    if op.is_set() {
        return match (lhs, rhs) {
            (IntermediateValue::Vector(lhs), IntermediateValue::Vector(rhs)) => Ok(
                IntermediateValue::Vector(apply_set_vectors(op, matching, lhs, rhs, cancelled)?),
            ),
            _ => Err("PromQL set operators require instant-vector operands".into()),
        };
    }
    match (lhs, rhs) {
        (IntermediateValue::Scalar(mut lhs), IntermediateValue::Scalar(rhs)) => {
            if lhs.len() != rhs.len() {
                return Err("PromQL scalar operands produced different evaluation grids".into());
            }
            for (lhs, rhs) in lhs.iter_mut().zip(rhs) {
                check_cancelled(cancelled)?;
                if lhs.0 != rhs.0 {
                    return Err("PromQL scalar operands produced different evaluation grids".into());
                }
                let Some(value) = op.evaluate(lhs.1, rhs.1, lhs.1, return_bool) else {
                    return Err("scalar comparison requires the BOOL modifier".into());
                };
                lhs.1 = value;
            }
            Ok(IntermediateValue::Scalar(lhs))
        }
        (IntermediateValue::Vector(vector), IntermediateValue::Scalar(scalar)) => {
            Ok(IntermediateValue::Vector(apply_scalar_to_vector(
                op,
                return_bool,
                vector,
                scalar,
                false,
                cancelled,
            )?))
        }
        (IntermediateValue::Scalar(scalar), IntermediateValue::Vector(vector)) => {
            Ok(IntermediateValue::Vector(apply_scalar_to_vector(
                op,
                return_bool,
                vector,
                scalar,
                true,
                cancelled,
            )?))
        }
        (IntermediateValue::Vector(lhs), IntermediateValue::Vector(rhs)) => {
            Ok(IntermediateValue::Vector(apply_vector_vectors(
                op,
                return_bool,
                matching,
                cardinality,
                lhs,
                rhs,
                cancelled,
            )?))
        }
    }
}

fn apply_set_vectors(
    op: PromBinaryOp,
    matching: &PromVectorMatching,
    lhs: Vec<IntermediateSeries>,
    rhs: Vec<IntermediateSeries>,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    debug_assert!(op.is_set());
    let lhs_keys: Vec<_> = lhs
        .iter()
        .map(|series| matching.key(&series.labels))
        .collect();
    let rhs_keys: Vec<_> = rhs
        .iter()
        .map(|series| matching.key(&series.labels))
        .collect();
    let lhs_by_timestamp = samples_by_timestamp(&lhs, cancelled)?;
    let rhs_by_timestamp = samples_by_timestamp(&rhs, cancelled)?;
    let timestamps: BTreeSet<_> = lhs_by_timestamp
        .keys()
        .chain(rhs_by_timestamp.keys())
        .copied()
        .collect();
    let mut lhs_output = vec![Vec::new(); lhs.len()];
    let mut rhs_output = vec![Vec::new(); rhs.len()];

    for timestamp in timestamps {
        check_cancelled(cancelled)?;
        let lhs_samples = lhs_by_timestamp
            .get(&timestamp)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let rhs_samples = rhs_by_timestamp
            .get(&timestamp)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let lhs_step_keys: BTreeSet<_> = lhs_samples
            .iter()
            .map(|(index, _)| &lhs_keys[*index])
            .collect();
        let rhs_step_keys: BTreeSet<_> = rhs_samples
            .iter()
            .map(|(index, _)| &rhs_keys[*index])
            .collect();

        match op {
            PromBinaryOp::And => {
                for (index, value) in lhs_samples {
                    check_cancelled(cancelled)?;
                    if rhs_step_keys.contains(&lhs_keys[*index]) {
                        lhs_output[*index].push((timestamp, *value));
                    }
                }
            }
            PromBinaryOp::Unless => {
                for (index, value) in lhs_samples {
                    check_cancelled(cancelled)?;
                    if !rhs_step_keys.contains(&lhs_keys[*index]) {
                        lhs_output[*index].push((timestamp, *value));
                    }
                }
            }
            PromBinaryOp::Or => {
                for (index, value) in lhs_samples {
                    check_cancelled(cancelled)?;
                    lhs_output[*index].push((timestamp, *value));
                }
                for (index, value) in rhs_samples {
                    check_cancelled(cancelled)?;
                    if !lhs_step_keys.contains(&rhs_keys[*index]) {
                        rhs_output[*index].push((timestamp, *value));
                    }
                }
            }
            _ => unreachable!("set operator checked above"),
        }
    }

    let output = lhs
        .into_iter()
        .zip(lhs_output)
        .chain(rhs.into_iter().zip(rhs_output))
        .filter_map(|(mut series, points)| {
            if points.is_empty() {
                None
            } else {
                series.points = points;
                Some(series)
            }
        })
        .collect();
    normalize_prometheus_vector(output, cancelled)
}

fn apply_scalar_to_vector(
    op: PromBinaryOp,
    return_bool: bool,
    mut vector: Vec<IntermediateSeries>,
    scalar: Vec<(i64, f64)>,
    scalar_is_lhs: bool,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    let scalar: BTreeMap<_, _> = scalar.into_iter().collect();
    for series in &mut vector {
        check_cancelled(cancelled)?;
        if op.is_arithmetic() || return_bool {
            series.labels.remove("__name__");
        }
        let mut output = Vec::with_capacity(series.points.len());
        for (timestamp, value) in series.points.drain(..) {
            check_cancelled(cancelled)?;
            let Some(scalar) = scalar.get(&timestamp) else {
                continue;
            };
            let value = if scalar_is_lhs {
                op.evaluate(*scalar, value, value, return_bool)
            } else {
                op.evaluate(value, *scalar, value, return_bool)
            };
            if let Some(value) = value {
                output.push((timestamp, value));
            }
        }
        series.points = output;
    }
    normalize_prometheus_vector(vector, cancelled)
}

fn samples_by_timestamp(
    series: &[IntermediateSeries],
    cancelled: &AtomicBool,
) -> Result<BTreeMap<i64, Vec<(usize, f64)>>, String> {
    let mut output: BTreeMap<i64, Vec<(usize, f64)>> = BTreeMap::new();
    for (index, series) in series.iter().enumerate() {
        for (timestamp, value) in &series.points {
            check_cancelled(cancelled)?;
            output.entry(*timestamp).or_default().push((index, *value));
        }
    }
    Ok(output)
}

fn duplicate_matching_error(key: &[(String, String)]) -> String {
    let labels: BTreeMap<_, _> = key.iter().cloned().collect();
    let labels = serde_json::to_string(&labels).unwrap_or_else(|_| "{}".into());
    format!(
        "found duplicate series for the match group {labels}; many-to-many matching not allowed: matching labels must be unique on one side"
    )
}

fn apply_vector_vectors(
    op: PromBinaryOp,
    return_bool: bool,
    matching: &PromVectorMatching,
    cardinality: &PromVectorCardinality,
    lhs: Vec<IntermediateSeries>,
    rhs: Vec<IntermediateSeries>,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    let lhs_keys: Vec<_> = lhs
        .iter()
        .map(|series| matching.key(&series.labels))
        .collect();
    let rhs_keys: Vec<_> = rhs
        .iter()
        .map(|series| matching.key(&series.labels))
        .collect();
    let lhs_by_timestamp = samples_by_timestamp(&lhs, cancelled)?;
    let rhs_by_timestamp = samples_by_timestamp(&rhs, cancelled)?;
    let mut output: BTreeMap<BTreeMap<String, String>, Vec<(i64, f64)>> = BTreeMap::new();

    for (timestamp, lhs_samples) in lhs_by_timestamp {
        check_cancelled(cancelled)?;
        let Some(rhs_samples) = rhs_by_timestamp.get(&timestamp) else {
            continue;
        };
        let mut lhs_groups: PromStepGroups<'_> = BTreeMap::new();
        for (index, value) in lhs_samples {
            check_cancelled(cancelled)?;
            lhs_groups
                .entry(&lhs_keys[index])
                .or_default()
                .push((index, value));
        }
        let mut rhs_groups: PromStepGroups<'_> = BTreeMap::new();
        for (index, value) in rhs_samples {
            check_cancelled(cancelled)?;
            rhs_groups
                .entry(&rhs_keys[*index])
                .or_default()
                .push((*index, *value));
        }
        for (key, lhs_group) in lhs_groups {
            check_cancelled(cancelled)?;
            let Some(rhs_group) = rhs_groups.get(key) else {
                continue;
            };
            let matches: Vec<(usize, usize, f64, f64)> = match cardinality {
                PromVectorCardinality::OneToOne => {
                    if lhs_group.len() != 1 || rhs_group.len() != 1 {
                        return Err(duplicate_matching_error(key));
                    }
                    vec![(
                        lhs_group[0].0,
                        rhs_group[0].0,
                        lhs_group[0].1,
                        rhs_group[0].1,
                    )]
                }
                PromVectorCardinality::ManyToOne(_) => {
                    if rhs_group.len() != 1 {
                        return Err(duplicate_matching_error(key));
                    }
                    lhs_group
                        .iter()
                        .map(|(lhs_index, lhs_value)| {
                            (*lhs_index, rhs_group[0].0, *lhs_value, rhs_group[0].1)
                        })
                        .collect()
                }
                PromVectorCardinality::OneToMany(_) => {
                    if lhs_group.len() != 1 {
                        return Err(duplicate_matching_error(key));
                    }
                    rhs_group
                        .iter()
                        .map(|(rhs_index, rhs_value)| {
                            (lhs_group[0].0, *rhs_index, lhs_group[0].1, *rhs_value)
                        })
                        .collect()
                }
                PromVectorCardinality::ManyToMany => {
                    unreachable!("set operators use their dedicated evaluator")
                }
            };
            let mut step_output_labels = BTreeSet::new();
            for (lhs_index, rhs_index, lhs_value, rhs_value) in matches {
                check_cancelled(cancelled)?;
                let Some(value) = op.evaluate(lhs_value, rhs_value, lhs_value, return_bool) else {
                    continue;
                };
                let (base_labels, one_labels) = match cardinality {
                    PromVectorCardinality::OneToMany(_) => {
                        (&rhs[rhs_index].labels, &lhs[lhs_index].labels)
                    }
                    _ => (&lhs[lhs_index].labels, &rhs[rhs_index].labels),
                };
                let labels = vector_result_labels(
                    base_labels,
                    one_labels,
                    op,
                    return_bool,
                    matching,
                    cardinality,
                );
                if !step_output_labels.insert(labels.clone()) {
                    return Err(
                        "multiple matches for labels: grouping labels must ensure unique matches"
                            .into(),
                    );
                }
                output.entry(labels).or_default().push((timestamp, value));
            }
        }
    }

    let output = output
        .into_iter()
        .map(|(labels, points)| IntermediateSeries { labels, points })
        .collect();
    normalize_prometheus_vector(output, cancelled)
}

fn vector_result_labels(
    base_labels: &BTreeMap<String, String>,
    one_labels: &BTreeMap<String, String>,
    op: PromBinaryOp,
    return_bool: bool,
    matching: &PromVectorMatching,
    cardinality: &PromVectorCardinality,
) -> BTreeMap<String, String> {
    let mut labels = base_labels.clone();
    if op.is_arithmetic() {
        labels.remove("__name__");
    }
    match cardinality {
        PromVectorCardinality::OneToOne => matching.project_one_to_one_result(&mut labels),
        PromVectorCardinality::ManyToOne(include) | PromVectorCardinality::OneToMany(include) => {
            for name in include {
                match one_labels.get(name).filter(|value| !value.is_empty()) {
                    Some(value) => {
                        labels.insert(name.clone(), value.clone());
                    }
                    None => {
                        labels.remove(name);
                    }
                }
            }
        }
        PromVectorCardinality::ManyToMany => {
            unreachable!("set operators retain contributing labels")
        }
    }
    if return_bool {
        labels.remove("__name__");
    }
    labels
}

fn normalize_prometheus_vector(
    series: Vec<IntermediateSeries>,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    let mut output: Vec<IntermediateSeries> = Vec::new();
    let mut positions: BTreeMap<BTreeMap<String, String>, usize> = BTreeMap::new();
    for mut series in series {
        check_cancelled(cancelled)?;
        if series.points.is_empty() {
            continue;
        }
        series.points.sort_unstable_by_key(|sample| sample.0);
        if let Some(index) = positions.get(&series.labels).copied() {
            let existing = &mut output[index];
            merge_prometheus_points(&mut existing.points, series.points, cancelled)?;
        } else {
            positions.insert(series.labels.clone(), output.len());
            output.push(series);
        }
    }
    Ok(output)
}

fn merge_prometheus_points(
    existing: &mut Vec<(i64, f64)>,
    incoming: Vec<(i64, f64)>,
    cancelled: &AtomicBool,
) -> Result<(), String> {
    let left = std::mem::take(existing);
    let mut merged = Vec::with_capacity(left.len().saturating_add(incoming.len()));
    let mut lhs = left.into_iter().peekable();
    let mut rhs = incoming.into_iter().peekable();
    while lhs.peek().is_some() || rhs.peek().is_some() {
        check_cancelled(cancelled)?;
        match (lhs.peek(), rhs.peek()) {
            (Some(left), Some(right)) if left.0 == right.0 => {
                return Err("vector cannot contain metrics with the same labelset".into());
            }
            (Some(left), Some(right)) if left.0 < right.0 => {
                merged.push(lhs.next().expect("peeked left point"));
            }
            (Some(_), Some(_)) => merged.push(rhs.next().expect("peeked right point")),
            (Some(_), None) => merged.push(lhs.next().expect("peeked left point")),
            (None, Some(_)) => merged.push(rhs.next().expect("peeked right point")),
            (None, None) => break,
        }
    }
    *existing = merged;
    Ok(())
}

fn enforce_intermediate_work(points: u64, limits: PromQueryLimits) -> Result<(), String> {
    if points > limits.max_work_points as u64 {
        return Err(format!(
            "query exceeded the maximum intermediate-work limit of {} points",
            limits.max_work_points
        ));
    }
    Ok(())
}

fn decode_prometheus_intermediate(
    body: &[u8],
    value_type: PromValueType,
    instant: bool,
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<IntermediateValue, String> {
    match (value_type, instant) {
        (PromValueType::Scalar, true) => {
            let document = decode_prometheus_document(body)?;
            if document["data"]["resultType"] != "scalar" {
                return Err("PromQL child expression did not produce a scalar".into());
            }
            Ok(IntermediateValue::Scalar(vec![decode_prometheus_sample(
                &document["data"]["result"],
                "scalar",
            )?]))
        }
        (PromValueType::Scalar, false) => {
            let mut matrix = decode_prometheus_matrix(body, limits, cancelled)?;
            if matrix.len() != 1 || !matrix[0].labels.is_empty() {
                return Err("PromQL scalar range child produced an invalid matrix".into());
            }
            Ok(IntermediateValue::Scalar(matrix.remove(0).points))
        }
        (PromValueType::Vector, true) => Ok(IntermediateValue::Vector(
            decode_prometheus_instant_vector(body, limits, cancelled)?,
        )),
        (PromValueType::Vector, false) => Ok(IntermediateValue::Vector(decode_prometheus_matrix(
            body, limits, cancelled,
        )?)),
        (PromValueType::String, _) => {
            Err("PromQL unary minus cannot evaluate a string expression".into())
        }
        (PromValueType::Matrix, _) => {
            Err("PromQL unary minus cannot evaluate a range-vector expression".into())
        }
    }
}

fn decode_prometheus_document(body: &[u8]) -> Result<Value, String> {
    serde_json::from_slice(body)
        .map_err(|error| format!("decode bounded PromQL child result: {error}"))
}

fn decode_prometheus_instant_vector(
    body: &[u8],
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    let document = decode_prometheus_document(body)?;
    if document["data"]["resultType"] != "vector" {
        return Err("PromQL child expression did not produce an instant vector".into());
    }
    let rows = document["data"]["result"]
        .as_array()
        .ok_or_else(|| "PromQL child vector result is not an array".to_string())?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        check_cancelled(cancelled)?;
        if output.len() >= limits.max_work_points {
            return Err(format!(
                "query exceeded the maximum intermediate-work limit of {} points",
                limits.max_work_points
            ));
        }
        let labels = serde_json::from_value::<BTreeMap<String, String>>(row["metric"].clone())
            .map_err(|error| format!("decode PromQL child labels: {error}"))?;
        let sample = decode_prometheus_sample(&row["value"], "instant vector")?;
        output.push(IntermediateSeries {
            labels,
            points: vec![sample],
        });
    }
    Ok(output)
}

fn decode_prometheus_sample(sample: &Value, context: &str) -> Result<(i64, f64), String> {
    let pair = sample
        .as_array()
        .filter(|pair| pair.len() == 2)
        .ok_or_else(|| format!("PromQL {context} sample is not a timestamp/value pair"))?;
    let timestamp_text = pair[0].to_string();
    let timestamp = parse_prom_time(Some(&timestamp_text), 0)
        .map_err(|error| format!("decode PromQL {context} timestamp: {error}"))?;
    let value = pair[1]
        .as_str()
        .ok_or_else(|| format!("PromQL {context} sample value is not a string"))?;
    Ok((timestamp, parse_prometheus_value(value)?))
}

fn encode_prometheus_intermediate(
    value: IntermediateValue,
    instant: bool,
    frame_bytes: usize,
    intermediate_points: u64,
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let expected_points = value.points();
    let mut body = Vec::new();
    let (series, points) = match value {
        IntermediateValue::Scalar(points) if instant => {
            let [sample] = points.as_slice() else {
                return Err("PromQL scalar child did not produce exactly one sample".into());
            };
            body.extend_from_slice(
                br#"{"status":"success","data":{"resultType":"scalar","result":"#,
            );
            enforce_prometheus_output(&body, 0, limits)?;
            admit_prometheus_point(0, limits)?;
            write_prometheus_scalar_sample(&mut body, sample.0, sample.1)?;
            body.extend_from_slice(b"}}");
            (0, 1)
        }
        IntermediateValue::Scalar(points) => {
            body.extend_from_slice(
                br#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":["#,
            );
            for (index, (timestamp, value)) in points.iter().enumerate() {
                check_cancelled(cancelled)?;
                admit_prometheus_point(index as u64, limits)?;
                comma(&mut body, index);
                write_prometheus_sample(&mut body, *timestamp, *value)?;
                enforce_prometheus_output(&body, index as u64 + 1, limits)?;
            }
            body.extend_from_slice(b"]}]}}");
            (1, points.len() as u64)
        }
        IntermediateValue::Vector(series) => {
            write_prometheus_prefix(&mut body, instant);
            let mut emitted = 0_u64;
            let mut points = 0_u64;
            for item in series {
                check_cancelled(cancelled)?;
                if instant && item.points.len() != 1 {
                    return Err(
                        "PromQL instant-vector child did not produce exactly one sample per series"
                            .into(),
                    );
                }
                if item.points.is_empty() {
                    continue;
                }
                comma(&mut body, emitted as usize);
                write_prometheus_item_prefix(&mut body, None, &item.labels, instant, limits)?;
                for (index, (timestamp, value)) in item.points.iter().enumerate() {
                    check_cancelled(cancelled)?;
                    admit_prometheus_point(points, limits)?;
                    if !instant {
                        comma(&mut body, index);
                    }
                    write_prometheus_sample(&mut body, *timestamp, *value)?;
                    points += 1;
                    enforce_prometheus_output(&body, points, limits)?;
                }
                write_prometheus_item_suffix(&mut body, instant);
                emitted += 1;
            }
            write_prometheus_suffix(&mut body);
            (emitted, points)
        }
    };
    debug_assert_eq!(points, expected_points);
    enforce_prometheus_output(&body, points, limits)?;
    Ok(ReadOutput {
        body,
        frame_bytes,
        series,
        points,
        intermediate_points,
        rows: points,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_subquery(
    conn: &Connection,
    features: QueryFeatures,
    subquery: &SubqueryPlan,
    start: i64,
    stop: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    if !instant {
        return Err("invalid expression type \"range vector\" for range query, must be Scalar or instant Vector".into());
    }
    let effective_start = subquery
        .timing
        .selection_time(start, query_start, query_end)?;
    let effective_stop = subquery
        .timing
        .selection_time(stop, query_start, query_end)?;
    let resolution = subquery_resolution(subquery, limits)?;
    let Some((inner_start, inner_stop, points_per_series)) = aligned_subquery_grid(
        effective_start.min(effective_stop),
        effective_start.max(effective_stop),
        subquery.window,
        resolution,
    )?
    else {
        return empty_prometheus_matrix(limits);
    };
    enforce_intermediate_grid(points_per_series, limits)?;
    check_cancelled(cancelled)?;
    execute_prometheus(
        conn,
        features,
        &subquery.inner,
        inner_start,
        inner_stop,
        resolution,
        false,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_range_subquery(
    conn: &Connection,
    features: QueryFeatures,
    subquery: &SubqueryPlan,
    start: i64,
    stop: i64,
    step: i64,
    op: PromRangeOp,
    parameters: Option<&[(i64, f64)]>,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    annotation_position: usize,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let effective_start = subquery
        .timing
        .selection_time(start, query_start, query_end)?;
    let effective_stop = subquery
        .timing
        .selection_time(stop, query_start, query_end)?;
    let resolution = subquery_resolution(subquery, limits)?;
    let Some((inner_start, inner_stop, points_per_series)) = aligned_subquery_grid(
        effective_start.min(effective_stop),
        effective_start.max(effective_stop),
        subquery.window,
        resolution,
    )?
    else {
        return empty_prometheus_vector_or_matrix(instant, limits);
    };
    enforce_intermediate_grid(points_per_series, limits)?;
    let inner = execute_prometheus(
        conn,
        features,
        &subquery.inner,
        inner_start,
        inner_stop,
        resolution,
        false,
        query_start,
        query_end,
        limits,
        annotations,
        cancelled,
    )?;
    let frame_bytes = inner.frame_bytes;
    let intermediate_points = inner.intermediate_points.saturating_add(inner.points);
    if intermediate_points > limits.max_work_points as u64 {
        return Err(format!(
            "query exceeded the maximum intermediate-work limit of {} points",
            limits.max_work_points
        ));
    }
    let intermediate = decode_prometheus_matrix(&inner.body, limits, cancelled)?;
    let parameters: BTreeMap<i64, f64> = parameters.unwrap_or_default().iter().copied().collect();
    let mut body = Vec::new();
    write_prometheus_prefix(&mut body, instant);
    enforce_prometheus_output(&body, 0, limits)?;
    let mut emitted = 0_usize;
    let mut result_points = 0_u64;

    for mut series in intermediate {
        check_cancelled(cancelled)?;
        let metric = series.labels.get("__name__").cloned();
        if !op.retains_metric_name() {
            series.labels.remove("__name__");
        }
        let item_start = body.len();
        comma(&mut body, emitted);
        write_prometheus_item_prefix(&mut body, None, &series.labels, instant, limits)?;
        let mut lo = 0_usize;
        let mut hi = 0_usize;
        let mut item_points = 0_u64;
        let mut outer = start;
        loop {
            check_cancelled(cancelled)?;
            let effective = subquery
                .timing
                .selection_time(outer, query_start, query_end)?;
            while hi < series.points.len() && series.points[hi].0 <= effective {
                hi += 1;
            }
            let lower = checked_timestamp_sub(effective, subquery.window, "subquery range")?;
            while lo < hi && series.points[lo].0 <= lower {
                lo += 1;
            }
            if hi > lo {
                let value = prometheus_range_reduction(
                    &series.points[lo..hi],
                    op,
                    parameters.get(&outer).copied(),
                    lower,
                    effective,
                    outer,
                    cancelled,
                )?;
                if let Some(value) = value {
                    admit_prometheus_point(result_points.saturating_add(item_points), limits)?;
                    if !instant {
                        comma(&mut body, item_points as usize);
                    }
                    write_prometheus_sample(&mut body, outer, value)?;
                    item_points += 1;
                    enforce_prometheus_output(
                        &body,
                        result_points.saturating_add(item_points),
                        limits,
                    )?;
                }
            }
            if outer >= stop {
                break;
            }
            let Some(next) = outer.checked_add(step).filter(|next| *next <= stop) else {
                break;
            };
            outer = next;
        }
        if item_points == 0 {
            body.truncate(item_start);
        } else {
            if matches!(op, PromRangeOp::Rate | PromRangeOp::Increase) {
                if let Some(metric) = metric
                    .as_deref()
                    .filter(|metric| !metric.is_empty() && !prometheus_counter_name(metric))
                {
                    annotations.possible_non_counter(metric, annotation_position);
                }
            }
            write_prometheus_item_suffix(&mut body, instant);
            emitted += 1;
            result_points = result_points.saturating_add(item_points);
        }
    }
    write_prometheus_suffix(&mut body);
    enforce_prometheus_output(&body, result_points, limits)?;
    Ok(ReadOutput {
        body,
        frame_bytes,
        series: emitted as u64,
        points: result_points,
        intermediate_points,
        rows: result_points,
    })
}

fn subquery_resolution(subquery: &SubqueryPlan, limits: PromQueryLimits) -> Result<i64, String> {
    match subquery.resolution {
        Some(resolution) => Ok(resolution),
        None => i64::try_from(limits.default_subquery_step.as_millis())
            .map_err(|_| "PromQL default subquery resolution overflow".to_string()),
    }
}

fn aligned_subquery_grid(
    effective_start: i64,
    effective_stop: i64,
    window: i64,
    resolution: i64,
) -> Result<Option<(i64, i64, usize)>, String> {
    if resolution <= 0 {
        return Err("PromQL subquery resolution must be positive".into());
    }
    let resolution = i128::from(resolution);
    let lower = i128::from(effective_start) - i128::from(window);
    // Range vectors are open on the left, so the first point is the first
    // globally aligned timestamp strictly greater than effective_start-range.
    let first = (lower.div_euclid(resolution) + 1) * resolution;
    let last = i128::from(effective_stop).div_euclid(resolution) * resolution;
    if first > last {
        return Ok(None);
    }
    let count = (last - first) / resolution + 1;
    let first =
        i64::try_from(first).map_err(|_| "PromQL subquery start timestamp overflow".to_string())?;
    let last =
        i64::try_from(last).map_err(|_| "PromQL subquery end timestamp overflow".to_string())?;
    let count =
        usize::try_from(count).map_err(|_| "PromQL subquery point count overflow".to_string())?;
    Ok(Some((first, last, count)))
}

fn enforce_intermediate_grid(
    points_per_series: usize,
    limits: PromQueryLimits,
) -> Result<(), String> {
    if points_per_series > limits.max_work_points {
        return Err(format!(
            "query exceeded the maximum intermediate-work limit of {} points",
            limits.max_work_points
        ));
    }
    Ok(())
}

fn decode_prometheus_matrix(
    body: &[u8],
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    let document: Value = serde_json::from_slice(body)
        .map_err(|error| format!("decode bounded subquery result: {error}"))?;
    if document["data"]["resultType"] != "matrix" {
        return Err("PromQL subquery inner expression did not produce an instant vector".into());
    }
    let rows = document["data"]["result"]
        .as_array()
        .ok_or_else(|| "PromQL subquery result is not a matrix array".to_string())?;
    let mut output = Vec::with_capacity(rows.len());
    let mut points = 0_usize;
    for row in rows {
        check_cancelled(cancelled)?;
        let labels = serde_json::from_value::<BTreeMap<String, String>>(row["metric"].clone())
            .map_err(|error| format!("decode PromQL subquery labels: {error}"))?;
        let values = row["values"]
            .as_array()
            .ok_or_else(|| "PromQL subquery series has no values array".to_string())?;
        let mut series_points = Vec::with_capacity(values.len());
        for sample in values {
            check_cancelled(cancelled)?;
            points = points.saturating_add(1);
            if points > limits.max_work_points {
                return Err(format!(
                    "query exceeded the maximum intermediate-work limit of {} points",
                    limits.max_work_points
                ));
            }
            let pair = sample
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or_else(|| {
                    "PromQL subquery sample is not a timestamp/value pair".to_string()
                })?;
            let timestamp_text = pair[0].to_string();
            let timestamp = parse_prom_time(Some(&timestamp_text), 0)
                .map_err(|error| format!("decode PromQL subquery timestamp: {error}"))?;
            let value = pair[1]
                .as_str()
                .ok_or_else(|| "PromQL subquery sample value is not a string".to_string())?;
            series_points.push((timestamp, parse_prometheus_value(value)?));
        }
        output.push(IntermediateSeries {
            labels,
            points: series_points,
        });
    }
    Ok(output)
}

fn parse_prometheus_value(value: &str) -> Result<f64, String> {
    match value {
        "NaN" => Ok(f64::NAN),
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ => value
            .parse()
            .map_err(|error| format!("decode PromQL subquery value {value:?}: {error}")),
    }
}

fn checked_timestamp_sub(timestamp: i64, duration: i64, name: &str) -> Result<i64, String> {
    timestamp
        .checked_sub(duration)
        .ok_or_else(|| format!("PromQL timestamp overflow while applying {name}"))
}

fn prometheus_range_reduction(
    points: &[(i64, f64)],
    op: PromRangeOp,
    parameter: Option<f64>,
    range_start: i64,
    range_end: i64,
    evaluation_time: i64,
    cancelled: &AtomicBool,
) -> Result<Option<f64>, String> {
    if matches!(op, PromRangeOp::Present) {
        return Ok(Some(1.0));
    }
    if matches!(op, PromRangeOp::Last) {
        return Ok(Some(points[points.len() - 1].1));
    }
    if matches!(op, PromRangeOp::Rate) {
        return prometheus_extrapolated_rate(points, range_start, range_end, true, true, cancelled);
    }
    if matches!(op, PromRangeOp::IRate) {
        return prometheus_instant_delta(points, true, cancelled);
    }
    if matches!(op, PromRangeOp::IDelta) {
        return prometheus_instant_delta(points, false, cancelled);
    }
    if matches!(op, PromRangeOp::Increase) {
        return prometheus_extrapolated_rate(
            points,
            range_start,
            range_end,
            true,
            false,
            cancelled,
        );
    }
    if matches!(op, PromRangeOp::Delta) {
        return prometheus_extrapolated_rate(
            points,
            range_start,
            range_end,
            false,
            false,
            cancelled,
        );
    }
    if matches!(op, PromRangeOp::Deriv) {
        return prometheus_linear_regression(points, points[0].0, cancelled)
            .map(|result| result.map(|(slope, _)| slope));
    }
    if matches!(op, PromRangeOp::PredictLinear) {
        let horizon =
            parameter.ok_or_else(|| "predict_linear is missing its scalar horizon".to_string())?;
        return prometheus_linear_regression(points, evaluation_time, cancelled)
            .map(|result| result.map(|(slope, intercept)| slope * horizon + intercept));
    }
    if matches!(op, PromRangeOp::Changes) {
        return prometheus_changes(points, cancelled).map(Some);
    }
    if matches!(op, PromRangeOp::Resets) {
        return prometheus_resets(points, cancelled).map(Some);
    }
    if matches!(op, PromRangeOp::Quantile) {
        let quantile = parameter
            .ok_or_else(|| "quantile_over_time is missing its scalar parameter".to_string())?;
        let mut values = Vec::with_capacity(points.len());
        for &(_, value) in points {
            check_cancelled(cancelled)?;
            values.push(value);
        }
        let value = prometheus_quantile(quantile, &mut values);
        check_cancelled(cancelled)?;
        return Ok(Some(value));
    }
    let aggregate = op
        .aggregate_op()
        .expect("non-positional range reduction has an aggregate state");
    let mut reduction = PromAggregateState::new(aggregate, points[0].1);
    for &(_, value) in &points[1..] {
        check_cancelled(cancelled)?;
        reduction.add(aggregate, value);
    }
    Ok(Some(reduction.finish(aggregate)))
}

fn prometheus_extrapolated_rate(
    points: &[(i64, f64)],
    range_start: i64,
    range_end: i64,
    is_counter: bool,
    is_rate: bool,
    cancelled: &AtomicBool,
) -> Result<Option<f64>, String> {
    if points.len() < 2 || range_end <= range_start {
        return Ok(None);
    }
    let first = points[0];
    let last = points[points.len() - 1];
    if last.0 <= first.0 {
        return Ok(None);
    }

    let mut result = last.1 - first.1;
    if is_counter {
        for pair in points.windows(2) {
            check_cancelled(cancelled)?;
            if pair[1].1 < pair[0].1 {
                result += pair[0].1;
            }
        }
    }

    let mut duration_to_start = (first.0 - range_start) as f64 / 1_000.0;
    let mut duration_to_end = (range_end - last.0) as f64 / 1_000.0;
    let sampled_interval = (last.0 - first.0) as f64 / 1_000.0;
    let average_interval = sampled_interval / (points.len() - 1) as f64;
    let extrapolation_threshold = average_interval * 1.1;
    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_interval / 2.0;
    }
    if is_counter && result > 0.0 && first.1 >= 0.0 {
        let duration_to_zero = sampled_interval * (first.1 / result);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_interval / 2.0;
    }

    let mut factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
    if is_rate {
        factor /= (range_end - range_start) as f64 / 1_000.0;
    }
    check_cancelled(cancelled)?;
    Ok(Some(result * factor))
}

fn prometheus_instant_delta(
    points: &[(i64, f64)],
    is_rate: bool,
    cancelled: &AtomicBool,
) -> Result<Option<f64>, String> {
    if points.len() < 2 {
        return Ok(None);
    }
    let previous = points[points.len() - 2];
    let last = points[points.len() - 1];
    let sampled_interval = last.0 - previous.0;
    if sampled_interval <= 0 {
        return Ok(None);
    }
    check_cancelled(cancelled)?;
    let mut result = if is_rate && last.1 < previous.1 {
        last.1
    } else {
        last.1 - previous.1
    };
    if is_rate {
        result /= sampled_interval as f64 / 1_000.0;
    }
    check_cancelled(cancelled)?;
    Ok(Some(result))
}

fn prometheus_linear_regression(
    points: &[(i64, f64)],
    intercept_time: i64,
    cancelled: &AtomicBool,
) -> Result<Option<(f64, f64)>, String> {
    if points.len() < 2 {
        return Ok(None);
    }

    let initial_y = points[0].1;
    let mut constant_y = true;
    let mut sum_x = 0.0;
    let mut compensation_x = 0.0;
    let mut sum_y = 0.0;
    let mut compensation_y = 0.0;
    let mut sum_xy = 0.0;
    let mut compensation_xy = 0.0;
    let mut sum_x2 = 0.0;
    let mut compensation_x2 = 0.0;

    for &(timestamp, value) in points {
        check_cancelled(cancelled)?;
        if value != initial_y {
            constant_y = false;
        }
        // Prometheus performs this subtraction in signed millisecond space
        // before converting to seconds.
        let x = timestamp.wrapping_sub(intercept_time) as f64 / 1_000.0;
        (sum_x, compensation_x) = prometheus_compensated_add(x, sum_x, compensation_x);
        (sum_y, compensation_y) = prometheus_compensated_add(value, sum_y, compensation_y);
        (sum_xy, compensation_xy) = prometheus_compensated_add(x * value, sum_xy, compensation_xy);
        (sum_x2, compensation_x2) = prometheus_compensated_add(x * x, sum_x2, compensation_x2);
    }

    if constant_y {
        return if initial_y.is_infinite() {
            Ok(Some((f64::NAN, f64::NAN)))
        } else {
            Ok(Some((0.0, initial_y)))
        };
    }

    sum_x += compensation_x;
    sum_y += compensation_y;
    sum_xy += compensation_xy;
    sum_x2 += compensation_x2;
    let count = points.len() as f64;
    let covariance_xy = sum_xy - sum_x * sum_y / count;
    let variance_x = sum_x2 - sum_x * sum_x / count;
    let slope = covariance_xy / variance_x;
    let intercept = sum_y / count - slope * sum_x / count;
    check_cancelled(cancelled)?;
    Ok(Some((slope, intercept)))
}

fn prometheus_changes(points: &[(i64, f64)], cancelled: &AtomicBool) -> Result<f64, String> {
    let mut changes = 0_u64;
    let mut previous = points[0].1;
    for &(_, current) in &points[1..] {
        check_cancelled(cancelled)?;
        if current != previous && !(current.is_nan() && previous.is_nan()) {
            changes += 1;
        }
        previous = current;
    }
    check_cancelled(cancelled)?;
    Ok(changes as f64)
}

fn prometheus_resets(points: &[(i64, f64)], cancelled: &AtomicBool) -> Result<f64, String> {
    let mut resets = 0_u64;
    let mut previous = points[0].1;
    for &(_, current) in &points[1..] {
        check_cancelled(cancelled)?;
        if current < previous {
            resets += 1;
        }
        previous = current;
    }
    check_cancelled(cancelled)?;
    Ok(resets as f64)
}

fn empty_prometheus_matrix(limits: PromQueryLimits) -> Result<ReadOutput, String> {
    let body = br#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#.to_vec();
    enforce_prometheus_output(&body, 0, limits)?;
    Ok(ReadOutput {
        body,
        frame_bytes: 0,
        series: 0,
        points: 0,
        intermediate_points: 0,
        rows: 0,
    })
}

fn empty_prometheus_vector_or_matrix(
    instant: bool,
    limits: PromQueryLimits,
) -> Result<ReadOutput, String> {
    let mut body = Vec::new();
    write_prometheus_prefix(&mut body, instant);
    write_prometheus_suffix(&mut body);
    enforce_prometheus_output(&body, 0, limits)?;
    Ok(ReadOutput {
        body,
        frame_bytes: 0,
        series: 0,
        points: 0,
        intermediate_points: 0,
        rows: 0,
    })
}

fn execute_prometheus_string(
    value: &str,
    timestamp: i64,
    instant: bool,
    limits: PromQueryLimits,
) -> Result<ReadOutput, String> {
    if !instant {
        return Err(
            "invalid expression type \"string\" for range query, must be Scalar or instant Vector"
                .into(),
        );
    }
    let mut body = Vec::new();
    body.extend_from_slice(br#"{"status":"success","data":{"resultType":"string","result":["#);
    enforce_prometheus_output(&body, 0, limits)?;
    write_prometheus_timestamp(&mut body, timestamp);
    body.push(b',');
    enforce_prometheus_output(&body, 0, limits)?;
    write_json_bounded(&mut body, value, limits.max_response_bytes)?;
    body.extend_from_slice(b"]}}");
    enforce_prometheus_output(&body, 1, limits)?;
    Ok(ReadOutput {
        body,
        frame_bytes: 0,
        series: 0,
        points: 1,
        intermediate_points: 0,
        rows: 1,
    })
}

fn execute_prometheus_scalar(
    value: f64,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let mut body = Vec::new();
    let mut points = 0_u64;
    if instant {
        body.extend_from_slice(br#"{"status":"success","data":{"resultType":"scalar","result":"#);
        enforce_prometheus_output(&body, points, limits)?;
        admit_prometheus_point(points, limits)?;
        write_prometheus_scalar_sample(&mut body, start, value)?;
        body.extend_from_slice(b"}}");
        points = 1;
        enforce_prometheus_output(&body, points, limits)?;
    } else {
        body.extend_from_slice(
            br#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":["#,
        );
        enforce_prometheus_output(&body, points, limits)?;
        let mut timestamp = start;
        loop {
            check_cancelled(cancelled)?;
            admit_prometheus_point(points, limits)?;
            comma(&mut body, points as usize);
            write_prometheus_sample(&mut body, timestamp, value)?;
            points += 1;
            enforce_prometheus_output(&body, points, limits)?;
            if timestamp >= stop {
                break;
            }
            let Some(next) = timestamp.checked_add(step).filter(|next| *next <= stop) else {
                break;
            };
            timestamp = next;
        }
        body.extend_from_slice(b"]}]}}");
        enforce_prometheus_output(&body, points, limits)?;
    }
    Ok(ReadOutput {
        body,
        frame_bytes: 0,
        series: u64::from(!instant),
        points,
        intermediate_points: 0,
        rows: points,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_range_selector(
    conn: &Connection,
    features: QueryFeatures,
    selector: &Selector,
    evaluation_time: i64,
    window: i64,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let selection_time = selector
        .timing
        .selection_time(evaluation_time, query_start, query_end)?;
    let lower = selection_time.saturating_sub(window);
    let catalogs = prometheus_catalogs(conn, features.table, selector, limits, cancelled)?;
    let mut body = br#"{"status":"success","data":{"resultType":"matrix","result":["#.to_vec();
    enforce_prometheus_output(&body, 0, limits)?;
    let mut emitted = 0_u64;
    let mut points = 0_u64;
    let mut frame_bytes = 0_usize;
    let mut remaining_work = limits.max_work_points;
    for (metric, catalog) in catalogs {
        check_cancelled(cancelled)?;
        if remaining_work == 0 {
            return Err(format!(
                "query exceeded the maximum storage-work limit of {} points",
                limits.max_work_points
            ));
        }
        let raw = raw_query(
            conn,
            features,
            &metric,
            &selector.filter,
            storage_seconds_floor(lower),
            storage_seconds_floor(selection_time),
            Some(remaining_work),
        )?;
        let work_points = raw.series.iter().map(RawSeries::len).sum();
        consume_prometheus_work(&mut remaining_work, work_points, limits)?;
        frame_bytes = frame_bytes.saturating_add(raw.frame_bytes);
        let by_id: HashMap<_, _> = raw
            .series
            .iter()
            .map(|series| (series.id, series))
            .collect();
        for meta in &catalog {
            check_cancelled(cancelled)?;
            let Some(series) = by_id.get(&meta.id) else {
                continue;
            };
            let item_start = body.len();
            comma(&mut body, emitted as usize);
            write_prometheus_item_prefix(&mut body, Some(&metric), &meta.labels, false, limits)?;
            enforce_prometheus_output(&body, points, limits)?;
            let mut item_points = 0_u64;
            for index in 0..series.len() {
                check_cancelled(cancelled)?;
                let timestamp = seconds_to_millis(series.timestamp(raw.frame.as_deref(), index)?);
                if timestamp <= lower || timestamp > selection_time {
                    continue;
                }
                admit_prometheus_point(points.saturating_add(item_points), limits)?;
                comma(&mut body, item_points as usize);
                write_prometheus_sample(
                    &mut body,
                    timestamp,
                    series.value(raw.frame.as_deref(), index)?,
                )?;
                item_points += 1;
                enforce_prometheus_output(&body, points.saturating_add(item_points), limits)?;
            }
            if item_points == 0 {
                body.truncate(item_start);
            } else {
                write_prometheus_item_suffix(&mut body, false);
                emitted += 1;
                points = points.saturating_add(item_points);
            }
        }
    }
    write_prometheus_suffix(&mut body);
    enforce_prometheus_output(&body, points, limits)?;
    Ok(ReadOutput {
        body,
        frame_bytes,
        series: emitted,
        points,
        intermediate_points: 0,
        rows: points,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_selector(
    conn: &Connection,
    features: QueryFeatures,
    selector: &Selector,
    start: i64,
    stop: i64,
    step: i64,
    lookback: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    execute_prometheus_selector_value(
        conn,
        features,
        selector,
        start,
        stop,
        step,
        lookback,
        instant,
        query_start,
        query_end,
        limits,
        cancelled,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_selector_value(
    conn: &Connection,
    features: QueryFeatures,
    selector: &Selector,
    start: i64,
    stop: i64,
    step: i64,
    lookback: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
    source_timestamp: bool,
) -> Result<ReadOutput, String> {
    let selection_start = selector
        .timing
        .selection_time(start, query_start, query_end)?;
    let selection_stop = selector
        .timing
        .selection_time(stop, query_start, query_end)?;
    let read_start = selection_start.min(selection_stop);
    let read_stop = selection_start.max(selection_stop);
    let catalogs = prometheus_catalogs(conn, features.table, selector, limits, cancelled)?;
    let mut body = Vec::new();
    write_prometheus_prefix(&mut body, instant);
    enforce_prometheus_output(&body, 0, limits)?;
    let mut emitted = 0_usize;
    let mut points = 0_u64;
    let mut frame_bytes = 0_usize;
    let mut remaining_work = limits.max_work_points;
    for (metric, catalog) in catalogs {
        check_cancelled(cancelled)?;
        if remaining_work == 0 {
            return Err(format!(
                "query exceeded the maximum storage-work limit of {} points",
                limits.max_work_points
            ));
        }
        let raw = raw_query(
            conn,
            features,
            &metric,
            &selector.filter,
            storage_seconds_floor(read_start.saturating_sub(lookback)),
            storage_seconds_floor(read_stop),
            Some(remaining_work),
        )?;
        let work_points = raw.series.iter().map(RawSeries::len).sum();
        consume_prometheus_work(&mut remaining_work, work_points, limits)?;
        frame_bytes = frame_bytes.saturating_add(raw.frame_bytes);
        let by_id: HashMap<_, _> = raw
            .series
            .iter()
            .map(|series| (series.id, series))
            .collect();
        for meta in &catalog {
            check_cancelled(cancelled)?;
            let Some(series) = by_id.get(&meta.id) else {
                continue;
            };
            let item_start = body.len();
            comma(&mut body, emitted);
            write_prometheus_item_prefix(
                &mut body,
                (!source_timestamp).then_some(metric.as_str()),
                &meta.labels,
                instant,
                limits,
            )?;
            enforce_prometheus_output(&body, points, limits)?;
            let mut lo = 0_usize;
            let mut hi = 0_usize;
            let mut item_points = 0_u64;
            let mut t = start;
            loop {
                check_cancelled(cancelled)?;
                let selection_time = selector.timing.selection_time(t, query_start, query_end)?;
                while hi < series.len()
                    && seconds_to_millis(series.timestamp(raw.frame.as_deref(), hi)?)
                        <= selection_time
                {
                    hi += 1;
                }
                let lower = selection_time.saturating_sub(lookback);
                while lo < hi
                    && seconds_to_millis(series.timestamp(raw.frame.as_deref(), lo)?) <= lower
                {
                    lo += 1;
                }
                if hi > lo {
                    let selected = hi - 1;
                    admit_prometheus_point(points.saturating_add(item_points), limits)?;
                    if !instant {
                        comma(&mut body, item_points as usize);
                    }
                    let value = if source_timestamp {
                        series.timestamp(raw.frame.as_deref(), selected)? as f64
                    } else {
                        series.value(raw.frame.as_deref(), selected)?
                    };
                    write_prometheus_sample(&mut body, t, value)?;
                    item_points += 1;
                    enforce_prometheus_output(&body, points.saturating_add(item_points), limits)?;
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
    }
    write_prometheus_suffix(&mut body);
    enforce_prometheus_output(&body, points, limits)?;
    Ok(ReadOutput {
        body,
        frame_bytes,
        series: emitted as u64,
        points,
        intermediate_points: 0,
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
    op: PromRangeOp,
    instant: bool,
    limits: PromQueryLimits,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let native_op = op
        .native_name()
        .ok_or_else(|| format!("{} has no public window kernel", op.name()))?;
    let max_work_points = i64::try_from(limits.max_work_points)
        .map_err(|_| "PromQL max_work_points exceeds SQLite INTEGER range".to_string())?;
    let mut stmt = conn
        .prepare(&format!(
            "SELECT labels, buckets
               FROM timeless_window_batches('{}', ?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8)
              ORDER BY labels, series_id",
            table.name()
        ))
        .map_err(|error| format!("prepare PromQL window batches: {error}"))?;
    let rows = stmt
        .query_map(
            params![
                metric,
                filter.pushdown_json,
                start,
                stop,
                step,
                window,
                native_op,
                max_work_points
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .map_err(|error| format!("query PromQL window batches: {error}"))?;
    let mut body = Vec::new();
    write_prometheus_prefix(&mut body, instant);
    enforce_prometheus_output(&body, 0, limits)?;
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
        write_prometheus_item_prefix(&mut body, None, &labels, instant, limits)?;
        enforce_prometheus_output(&body, points, limits)?;
        let mut item_points = 0_u64;
        for index in 0..decoded.len() {
            check_cancelled(cancelled)?;
            let Some(mut value) = decoded.value(index) else {
                continue;
            };
            if matches!(op, PromRangeOp::Present) {
                value = 1.0;
            }
            admit_prometheus_point(points.saturating_add(item_points), limits)?;
            if !instant {
                comma(&mut body, item_points as usize);
            }
            write_prometheus_sample(
                &mut body,
                seconds_to_millis(decoded.timestamp(index)),
                value,
            )?;
            item_points += 1;
            enforce_prometheus_output(&body, points.saturating_add(item_points), limits)?;
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
    enforce_prometheus_output(&body, points, limits)?;
    Ok(ReadOutput {
        body,
        frame_bytes,
        series: emitted as u64,
        points,
        intermediate_points: 0,
        rows: points,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_prometheus_range_raw(
    conn: &Connection,
    features: QueryFeatures,
    selector: &Selector,
    start: i64,
    stop: i64,
    step: i64,
    window: i64,
    op: PromRangeOp,
    parameters: Option<&[(i64, f64)]>,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    annotation_position: usize,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let selection_start = selector
        .timing
        .selection_time(start, query_start, query_end)?;
    let selection_stop = selector
        .timing
        .selection_time(stop, query_start, query_end)?;
    let read_start = selection_start.min(selection_stop);
    let read_stop = selection_start.max(selection_stop);
    let catalogs = prometheus_catalogs(conn, features.table, selector, limits, cancelled)?;
    let mut body = Vec::new();
    write_prometheus_prefix(&mut body, instant);
    enforce_prometheus_output(&body, 0, limits)?;
    let mut emitted = 0_usize;
    let mut points = 0_u64;
    let mut frame_bytes = 0_usize;
    let mut remaining_work = limits.max_work_points;
    let parameters: BTreeMap<i64, f64> = parameters.unwrap_or_default().iter().copied().collect();
    for (metric, catalog) in catalogs {
        check_cancelled(cancelled)?;
        if remaining_work == 0 {
            return Err(format!(
                "query exceeded the maximum storage-work limit of {} points",
                limits.max_work_points
            ));
        }
        let raw = raw_query(
            conn,
            features,
            &metric,
            &selector.filter,
            storage_seconds_floor(read_start.saturating_sub(window)),
            storage_seconds_floor(read_stop),
            Some(remaining_work),
        )?;
        let work_points = raw.series.iter().map(RawSeries::len).sum();
        consume_prometheus_work(&mut remaining_work, work_points, limits)?;
        frame_bytes = frame_bytes.saturating_add(raw.frame_bytes);
        let by_id: HashMap<_, _> = raw
            .series
            .iter()
            .map(|series| (series.id, series))
            .collect();
        for meta in &catalog {
            check_cancelled(cancelled)?;
            let Some(series) = by_id.get(&meta.id) else {
                continue;
            };
            let item_start = body.len();
            comma(&mut body, emitted);
            write_prometheus_item_prefix(
                &mut body,
                op.retains_metric_name().then_some(metric.as_str()),
                &meta.labels,
                instant,
                limits,
            )?;
            enforce_prometheus_output(&body, points, limits)?;
            let mut lo = 0_usize;
            let mut hi = 0_usize;
            let mut item_points = 0_u64;
            let mut t = start;
            loop {
                check_cancelled(cancelled)?;
                let selection_time = selector.timing.selection_time(t, query_start, query_end)?;
                while hi < series.len()
                    && seconds_to_millis(series.timestamp(raw.frame.as_deref(), hi)?)
                        <= selection_time
                {
                    hi += 1;
                }
                let lower = selection_time.saturating_sub(window);
                while lo < hi
                    && seconds_to_millis(series.timestamp(raw.frame.as_deref(), lo)?) <= lower
                {
                    lo += 1;
                }
                if hi > lo {
                    let value = if matches!(op, PromRangeOp::Present) {
                        Some(1.0)
                    } else if matches!(op, PromRangeOp::Quantile) {
                        let quantile = parameters.get(&t).copied().ok_or_else(|| {
                            "quantile_over_time is missing its scalar parameter".to_string()
                        })?;
                        let mut values = Vec::with_capacity(hi - lo);
                        for index in lo..hi {
                            check_cancelled(cancelled)?;
                            values.push(series.value(raw.frame.as_deref(), index)?);
                        }
                        let value = prometheus_quantile(quantile, &mut values);
                        check_cancelled(cancelled)?;
                        Some(value)
                    } else if matches!(
                        op,
                        PromRangeOp::Rate
                            | PromRangeOp::Increase
                            | PromRangeOp::Delta
                            | PromRangeOp::Deriv
                            | PromRangeOp::PredictLinear
                    ) {
                        let mut values = Vec::with_capacity(hi - lo);
                        for index in lo..hi {
                            check_cancelled(cancelled)?;
                            values.push((
                                seconds_to_millis(series.timestamp(raw.frame.as_deref(), index)?),
                                series.value(raw.frame.as_deref(), index)?,
                            ));
                        }
                        let value = if matches!(op, PromRangeOp::Deriv) {
                            prometheus_linear_regression(&values, values[0].0, cancelled)?
                                .map(|(slope, _)| slope)
                        } else if matches!(op, PromRangeOp::PredictLinear) {
                            let horizon = parameters.get(&t).copied().ok_or_else(|| {
                                "predict_linear is missing its scalar horizon".to_string()
                            })?;
                            prometheus_linear_regression(&values, t, cancelled)?
                                .map(|(slope, intercept)| slope * horizon + intercept)
                        } else {
                            prometheus_extrapolated_rate(
                                &values,
                                lower,
                                selection_time,
                                !matches!(op, PromRangeOp::Delta),
                                matches!(op, PromRangeOp::Rate),
                                cancelled,
                            )?
                        };
                        check_cancelled(cancelled)?;
                        value
                    } else if matches!(op, PromRangeOp::IRate | PromRangeOp::IDelta) {
                        if hi - lo < 2 {
                            None
                        } else {
                            let values = [
                                (
                                    seconds_to_millis(
                                        series.timestamp(raw.frame.as_deref(), hi - 2)?,
                                    ),
                                    series.value(raw.frame.as_deref(), hi - 2)?,
                                ),
                                (
                                    seconds_to_millis(
                                        series.timestamp(raw.frame.as_deref(), hi - 1)?,
                                    ),
                                    series.value(raw.frame.as_deref(), hi - 1)?,
                                ),
                            ];
                            prometheus_instant_delta(
                                &values,
                                matches!(op, PromRangeOp::IRate),
                                cancelled,
                            )?
                        }
                    } else if matches!(op, PromRangeOp::Changes) {
                        let mut changes = 0_u64;
                        let mut previous = series.value(raw.frame.as_deref(), lo)?;
                        for index in lo + 1..hi {
                            check_cancelled(cancelled)?;
                            let current = series.value(raw.frame.as_deref(), index)?;
                            if current != previous && !(current.is_nan() && previous.is_nan()) {
                                changes += 1;
                            }
                            previous = current;
                        }
                        Some(changes as f64)
                    } else if matches!(op, PromRangeOp::Resets) {
                        let mut resets = 0_u64;
                        let mut previous = series.value(raw.frame.as_deref(), lo)?;
                        for index in lo + 1..hi {
                            check_cancelled(cancelled)?;
                            let current = series.value(raw.frame.as_deref(), index)?;
                            if current < previous {
                                resets += 1;
                            }
                            previous = current;
                        }
                        Some(resets as f64)
                    } else if matches!(op, PromRangeOp::Last) {
                        Some(series.value(raw.frame.as_deref(), hi - 1)?)
                    } else {
                        let aggregate = op
                            .aggregate_op()
                            .expect("non-positional range reduction has an aggregate state");
                        let mut reduction = PromAggregateState::new(
                            aggregate,
                            series.value(raw.frame.as_deref(), lo)?,
                        );
                        for index in lo + 1..hi {
                            check_cancelled(cancelled)?;
                            reduction.add(aggregate, series.value(raw.frame.as_deref(), index)?);
                        }
                        Some(reduction.finish(aggregate))
                    };
                    if let Some(value) = value {
                        admit_prometheus_point(points.saturating_add(item_points), limits)?;
                        if !instant {
                            comma(&mut body, item_points as usize);
                        }
                        write_prometheus_sample(&mut body, t, value)?;
                        item_points += 1;
                        enforce_prometheus_output(
                            &body,
                            points.saturating_add(item_points),
                            limits,
                        )?;
                    }
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
                if matches!(op, PromRangeOp::Rate | PromRangeOp::Increase)
                    && !prometheus_counter_name(&metric)
                {
                    annotations.possible_non_counter(&metric, annotation_position);
                }
                write_prometheus_item_suffix(&mut body, instant);
                emitted += 1;
                points = points.saturating_add(item_points);
            }
        }
    }
    write_prometheus_suffix(&mut body);
    enforce_prometheus_output(&body, points, limits)?;
    Ok(ReadOutput {
        body,
        frame_bytes,
        series: emitted as u64,
        points,
        intermediate_points: 0,
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
    limits: PromQueryLimits,
) -> Result<(), String> {
    output.extend_from_slice(b"{\"metric\":");
    enforce_prometheus_output(output, 0, limits)?;
    write_json_bounded(
        output,
        &PrometheusLabels { metric, labels },
        limits.max_response_bytes,
    )?;
    output.extend_from_slice(if instant {
        b",\"value\":"
    } else {
        b",\"values\":["
    });
    enforce_prometheus_output(output, 0, limits)?;
    Ok(())
}

struct PrometheusLabels<'a> {
    metric: Option<&'a str>,
    labels: &'a BTreeMap<String, String>,
}

impl Serialize for PrometheusLabels<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut entries: Vec<(&str, &str)> = self
            .labels
            .iter()
            .filter(|(key, _)| self.metric.is_none() || key.as_str() != "__name__")
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        if let Some(metric) = self.metric {
            entries.push(("__name__", metric));
            entries.sort_unstable_by_key(|(key, _)| *key);
        }
        let mut map = serializer.serialize_map(Some(entries.len()))?;
        for (key, value) in entries {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
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

fn write_prometheus_scalar_sample(
    output: &mut Vec<u8>,
    timestamp: i64,
    value: f64,
) -> Result<(), String> {
    output.push(b'[');
    write_prometheus_timestamp(output, timestamp);
    output.push(b',');
    write_json(output, &format_prometheus_label_value(value))?;
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
    let absolute = value.abs();
    if absolute != 0.0 && !(1e-6..1e21).contains(&absolute) {
        let rendered = format!("{value:e}");
        let (mantissa, exponent) = rendered
            .split_once('e')
            .expect("Rust scientific float formatting includes an exponent");
        let exponent: i32 = exponent
            .parse()
            .expect("Rust scientific float formatting emits a decimal exponent");
        return format!("{mantissa}e{exponent:+03}");
    }
    format_prometheus_label_value(value)
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
        intermediate_points: 0,
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
                MetricSelection::Matchers(matchers) => {
                    matchers.iter().all(|matcher| matcher.matches_value(metric))
                }
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
        intermediate_points: 0,
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
        if !bit(&bytes[bitmap_start..values_start], index) && word != 0 {
            return Err(format!("TWB1 null value {index} has nonzero bits"));
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

struct BoundedVecWriter<'a> {
    output: &'a mut Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl std::io::Write for BoundedVecWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.output.len());
        if bytes.len() > remaining {
            self.exceeded = true;
            return Err(std::io::Error::other("response byte limit exceeded"));
        }
        self.output.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn write_json_bounded<T: ?Sized + Serialize>(
    output: &mut Vec<u8>,
    value: &T,
    limit: usize,
) -> Result<(), String> {
    let mut writer = BoundedVecWriter {
        output,
        limit,
        exceeded: false,
    };
    let result = serde_json::to_writer(&mut writer, value);
    if writer.exceeded {
        return Err(format!(
            "query exceeded the maximum response-size limit of {limit} bytes"
        ));
    }
    result.map_err(|error| format!("encode query response: {error}"))
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
    fn prometheus_atan2_matches_go_rounding_and_ieee_quadrants() {
        for (y, x, expected) in [
            (6.0, 2.0, 1.2490457723982544_f64),
            (7.0, 2.0, 1.2924966677897851_f64),
            (8.0, 2.0, 1.3258176636680323_f64),
        ] {
            assert_eq!(prometheus_atan2(y, x).to_bits(), expected.to_bits());
        }

        assert_eq!(prometheus_atan2(0.0, 1.0).to_bits(), 0.0_f64.to_bits());
        assert_eq!(prometheus_atan2(-0.0, 1.0).to_bits(), (-0.0_f64).to_bits());
        assert_eq!(
            prometheus_atan2(0.0, -1.0).to_bits(),
            std::f64::consts::PI.to_bits()
        );
        assert_eq!(
            prometheus_atan2(-0.0, -1.0).to_bits(),
            (-std::f64::consts::PI).to_bits()
        );
        assert_eq!(
            prometheus_atan2(f64::INFINITY, f64::NEG_INFINITY).to_bits(),
            (3.0 * std::f64::consts::FRAC_PI_4).to_bits()
        );
        assert!(prometheus_atan2(f64::NAN, 1.0).is_nan());
    }

    #[test]
    fn promql_annotation_source_locations_follow_nested_calls_and_skip_literals() {
        let query = "histogram_quantile(\n  -1,\n  rate(foo[5m:1m])\n)";
        let calls = scan_promql_source_calls(query);
        let histogram = calls["histogram_quantile"].front().unwrap();
        assert_eq!(promql_source_position(query, histogram.start), "1:1");
        assert_eq!(promql_source_position(query, histogram.argument(0)), "2:3");
        assert_eq!(promql_source_position(query, histogram.argument(1)), "3:3");
        let rate = calls["rate"].front().unwrap();
        assert_eq!(promql_source_position(query, rate.start), "3:3");
        assert_eq!(promql_source_position(query, rate.argument(0)), "3:8");

        let grouped = "quantile without (pod) (\n  NaN,\n  foo\n)";
        let quantile = scan_promql_source_calls(grouped)["quantile"]
            .front()
            .unwrap()
            .clone();
        assert_eq!(promql_source_position(grouped, quantile.start), "1:1");
        assert_eq!(promql_source_position(grouped, quantile.argument(0)), "2:3");
        assert_eq!(promql_source_position(grouped, quantile.argument(1)), "3:3");

        let quoted = r#"foo{label="sort(fake)",raw=`rate(fake[5m])`} + sort(real)"#;
        let calls = scan_promql_source_calls(quoted);
        assert_eq!(calls["sort"].len(), 1);
        assert!(!calls.contains_key("rate"));
        assert_eq!(
            &quoted[calls["sort"].front().unwrap().argument(0)..],
            "real)"
        );
    }

    #[test]
    fn prometheus_annotations_deduplicate_merge_cap_and_render_exactly() {
        let query = "histogram_quantile(\n  0.5,\n  buckets\n)";
        let bucket_position = scan_promql_source_calls(query)["histogram_quantile"]
            .front()
            .unwrap()
            .argument(1);
        let mut annotations = PromAnnotations::default();
        annotations.warning("duplicate".into(), 0);
        annotations.warning("duplicate".into(), bucket_position);
        annotations.histogram_monotonicity(bucket_position, 1_700_000_000_000, 2.0, 2.0, 1.0);
        annotations.histogram_monotonicity(bucket_position, 1_700_000_010_000, 1.0, 4.0, 10.0);
        assert_eq!(annotations.warnings.len(), 1);
        assert_eq!(
            render_prometheus_annotations(&annotations.warnings, query, "warning"),
            vec!["duplicate (3:3)"]
        );
        assert_eq!(
            render_prometheus_annotations(&annotations.infos, query, "info"),
            vec!["PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile), from buckets 1 to 4, with a max diff of 10, over 2 samples from 2023-11-14T22:13:20Z to 2023-11-14T22:13:30Z (3:3)"]
        );
        assert_eq!(format_prometheus_annotation_float_precision_2(10.0), "10");
        assert_eq!(
            format_prometheus_annotation_float_precision_2(100.0),
            "1e+02"
        );

        let mut capped = BTreeMap::new();
        for index in 0..12 {
            let raw = format!("warning {index:02}");
            capped.insert(raw.clone(), PromAnnotation::Generic { raw, position: 0 });
        }
        let rendered = render_prometheus_annotations(&capped, "x", "warning");
        assert_eq!(rendered.len(), 11);
        assert_eq!(
            rendered.last().unwrap(),
            "2 more warning annotations omitted"
        );

        let body = br#"{"status":"success","data":{"resultType":"vector","result":[]}}"#.to_vec();
        let original_len = body.len();
        let mut output = ReadOutput {
            body,
            frame_bytes: 0,
            series: 0,
            points: 0,
            intermediate_points: 0,
            rows: 0,
        };
        let error = annotations
            .append_to_success(
                query,
                &mut output,
                PromQueryLimits {
                    max_response_bytes: original_len,
                    ..PromQueryLimits::default()
                },
            )
            .unwrap_err();
        assert_eq!(
            error,
            format!("query exceeded the maximum response-size limit of {original_len} bytes")
        );
    }

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
    fn promql_nameless_selectors_lower_to_catalog_expansion() {
        let PromPlan::Selector { selector, lookback } =
            lower_promql(r#"{job="api",region=""}"#, 300_000).unwrap()
        else {
            panic!("nameless selector lowered to the wrong plan")
        };
        assert!(matches!(selector.metric, MetricSelection::All));
        assert_eq!(selector.filter.matchers.len(), 2);
        assert_eq!(lookback, 300_000);

        let error = lower_promql(r#"{job=~".*"}"#, 300_000).unwrap_err();
        assert!(error.contains("at least one non-empty matcher"), "{error}");
    }

    #[test]
    fn promql_metric_name_matchers_are_anchored_and_anded() {
        let PromPlan::Selector { selector, .. } = lower_promql(
            r#"{__name__=~"http_.+",__name__!~"http_internal_.+",job="api"}"#,
            300_000,
        )
        .unwrap() else {
            panic!("name-matcher selector lowered to the wrong plan")
        };
        let MetricSelection::Matchers(matchers) = selector.metric else {
            panic!("non-exact name matchers did not retain AND composition")
        };
        assert_eq!(matchers.len(), 2);
        assert!(matchers
            .iter()
            .all(|matcher| matcher.matches_value("http_requests")));
        assert!(!matchers
            .iter()
            .all(|matcher| matcher.matches_value("http_internal_debug")));
        assert!(!matchers
            .iter()
            .all(|matcher| matcher.matches_value("prefix_http_requests")));

        let error = lower_promql(r#"{__name__!="missing"}"#, 300_000).unwrap_err();
        assert!(error.contains("at least one non-empty matcher"), "{error}");
    }

    #[test]
    fn promql_temporal_modifiers_resolve_anchor_before_signed_offset() {
        let PromPlan::Selector { selector, .. } =
            lower_promql("cpu @ 50 offset 10s", 300_000).unwrap()
        else {
            panic!("temporal selector lowered to the wrong plan")
        };
        assert_eq!(selector.timing.offset_ms, 10_000);
        assert_eq!(selector.timing.at, Some(SelectorAt::Timestamp(50_000)));
        assert_eq!(
            selector.timing.selection_time(60_000, 0, 60_000).unwrap(),
            40_000
        );

        let PromPlan::Selector { selector, .. } = lower_promql("cpu offset -20s", 300_000).unwrap()
        else {
            panic!("negative-offset selector lowered to the wrong plan")
        };
        assert_eq!(selector.timing.offset_ms, -20_000);
        assert_eq!(
            selector.timing.selection_time(10_000, 0, 60_000).unwrap(),
            30_000
        );

        let PromPlan::Selector { selector, .. } = lower_promql("cpu @ -1", 300_000).unwrap() else {
            panic!("pre-epoch selector lowered to the wrong plan")
        };
        assert_eq!(selector.timing.at, Some(SelectorAt::Timestamp(-1_000)));
    }

    #[test]
    fn promql_subqueries_lower_and_align_over_the_complete_timestamp_domain() {
        let PromPlan::Subquery(subquery) = lower_promql("cpu[30s:10s] offset 5s", 300_000).unwrap()
        else {
            panic!("subquery lowered to the wrong plan")
        };
        assert_eq!(subquery.window, 30_000);
        assert_eq!(subquery.resolution, Some(10_000));
        assert_eq!(subquery.timing.offset_ms, 5_000);
        assert!(matches!(*subquery.inner, PromPlan::Selector { .. }));
        assert_eq!(
            aligned_subquery_grid(25_000, 25_000, 30_000, 10_000).unwrap(),
            Some((0, 20_000, 3))
        );
        assert_eq!(
            aligned_subquery_grid(-1, -1, 20, 10).unwrap(),
            Some((-20, -10, 2))
        );

        let PromPlan::RangeReduction(PromRangePlan {
            op: PromRangeOp::Avg,
            input: PromRangeInput::Subquery(average),
            parameter: None,
            ..
        }) = lower_promql("avg_over_time(cpu[30s:])", 300_000).unwrap()
        else {
            panic!("subquery range function lowered to the wrong plan")
        };
        assert_eq!(average.resolution, None);
        assert!(matches!(*average.inner, PromPlan::Selector { .. }));

        let PromPlan::RangeReduction(PromRangePlan {
            op: PromRangeOp::Avg,
            input: PromRangeInput::Subquery(nested),
            parameter: None,
            ..
        }) = lower_promql(
            "avg_over_time(avg_over_time(cpu[20s:10s])[20s:10s])",
            300_000,
        )
        .unwrap()
        else {
            panic!("nested subquery lowered to the wrong plan")
        };
        assert!(matches!(
            *nested.inner,
            PromPlan::RangeReduction(PromRangePlan {
                op: PromRangeOp::Avg,
                input: PromRangeInput::Subquery(_),
                parameter: None,
                ..
            })
        ));
        assert!(aligned_subquery_grid(0, 0, 10, 0).is_err());
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

        let mut window = Vec::new();
        window.extend_from_slice(b"TWB1");
        window.extend_from_slice(&1_u32.to_le_bytes());
        window.extend_from_slice(&10_i64.to_le_bytes());
        window.push(1);
        window.extend_from_slice(&f64::NAN.to_bits().to_le_bytes());
        let decoded = decode_window_batch(&window).unwrap();
        assert_eq!(decoded.value(0).unwrap().to_bits(), f64::NAN.to_bits());
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
    fn prometheus_quantile_keeps_input_order_for_signed_zero_ties() {
        let mut positive_then_negative = [0.0, -0.0];
        let low = prometheus_quantile(0.0, &mut positive_then_negative);
        let high = prometheus_quantile(1.0, &mut positive_then_negative);
        assert_eq!(low.to_bits(), 0.0_f64.to_bits());
        assert_eq!(high.to_bits(), (-0.0_f64).to_bits());

        let mut negative_then_positive = [-0.0, 0.0];
        let low = prometheus_quantile(0.0, &mut negative_then_positive);
        let high = prometheus_quantile(1.0, &mut negative_then_positive);
        // The interpolation expression adds a zero-weight +0 upper endpoint,
        // which normalizes this exact q=0 result just as Prometheus does.
        assert_eq!(low.to_bits(), 0.0_f64.to_bits());
        assert_eq!(high.to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn prometheus_rate_extrapolates_resets_sparse_edges_and_zero_point() {
        let cancelled = AtomicBool::new(false);
        for (points, range_end, expected) in [
            (
                vec![(10_000, 100.0), (30_000, 300.0), (50_000, 500.0)],
                60_000,
                10.0,
            ),
            (
                vec![(10_000, 100.0), (30_000, 150.0), (50_000, 20.0)],
                60_000,
                1.75,
            ),
            (vec![(30_000, 100.0), (40_000, 200.0)], 60_000, 10.0 / 3.0),
            (vec![(10_000, 1.0), (30_000, 101.0)], 40_000, 3.775),
        ] {
            let actual =
                prometheus_extrapolated_rate(&points, 0, range_end, true, true, &cancelled)
                    .unwrap()
                    .unwrap();
            assert_eq!(actual, expected);
        }

        assert_eq!(
            prometheus_extrapolated_rate(&[(50_000, 5.0)], 0, 60_000, true, true, &cancelled,)
                .unwrap(),
            None
        );
        assert!(prometheus_extrapolated_rate(
            &[(20_000, f64::NAN), (40_000, 2.0)],
            0,
            60_000,
            true,
            true,
            &cancelled,
        )
        .unwrap()
        .unwrap()
        .is_nan());

        let increase = prometheus_extrapolated_rate(
            &[(10_000, 100.0), (30_000, 150.0), (50_000, 20.0)],
            0,
            60_000,
            true,
            false,
            &cancelled,
        )
        .unwrap()
        .unwrap();
        assert_eq!(increase, 105.0);

        let delta = prometheus_extrapolated_rate(
            &[(10_000, 100.0), (30_000, 150.0), (50_000, 20.0)],
            0,
            60_000,
            false,
            false,
            &cancelled,
        )
        .unwrap()
        .unwrap();
        assert_eq!(delta, -120.0);
    }

    #[test]
    fn prometheus_irate_uses_only_the_last_two_samples_and_requires_nonzero_interval() {
        let cancelled = AtomicBool::new(false);
        for (points, expected) in [
            (
                vec![(10_000, 100.0), (30_000, 300.0), (50_000, 500.0)],
                Some(10.0),
            ),
            (
                vec![(10_000, 100.0), (30_000, 150.0), (50_000, 20.0)],
                Some(1.0),
            ),
            (vec![(30_000, 100.0), (40_000, 200.0)], Some(10.0)),
            (vec![(50_000, 5.0)], None),
            (vec![(50_000, 5.0), (50_000, 6.0)], None),
        ] {
            assert_eq!(
                prometheus_instant_delta(&points, true, &cancelled).unwrap(),
                expected
            );
        }
        assert!(
            prometheus_instant_delta(&[(20_000, f64::NAN), (40_000, 2.0)], true, &cancelled,)
                .unwrap()
                .unwrap()
                .is_nan()
        );

        assert_eq!(
            prometheus_instant_delta(
                &[(10_000, 100.0), (30_000, 150.0), (50_000, 20.0)],
                false,
                &cancelled,
            )
            .unwrap(),
            Some(-130.0)
        );
    }

    #[test]
    fn prometheus_linear_regression_is_centered_compensated_and_cancellable() {
        let cancelled = AtomicBool::new(false);
        for (points, expected) in [
            (
                vec![
                    (1_700_750_010_000, 100.0),
                    (1_700_750_030_000, 300.0),
                    (1_700_750_050_000, 500.0),
                ],
                10.0,
            ),
            (
                vec![
                    (1_700_750_010_000, 100.0),
                    (1_700_750_030_000, 150.0),
                    (1_700_750_050_000, 20.0),
                ],
                -2.0,
            ),
            (
                vec![
                    (1_700_750_010_000, 7.0),
                    (1_700_750_030_000, 7.0),
                    (1_700_750_050_000, 7.0),
                ],
                0.0,
            ),
        ] {
            let actual = prometheus_linear_regression(&points, points[0].0, &cancelled)
                .unwrap()
                .unwrap();
            assert_eq!(actual.0, expected);
        }

        let points = [
            (1_700_760_010_000, 100.0),
            (1_700_760_030_000, 300.0),
            (1_700_760_050_000, 500.0),
        ];
        let (slope, intercept) =
            prometheus_linear_regression(&points, 1_700_760_060_000, &cancelled)
                .unwrap()
                .unwrap();
        assert_eq!((slope, intercept), (10.0, 600.0));
        assert_eq!(slope * 10.0 + intercept, 700.0);

        assert_eq!(
            prometheus_linear_regression(&[(50_000, 5.0)], 50_000, &cancelled).unwrap(),
            None
        );
        for points in [
            vec![(20_000, f64::NAN), (40_000, 2.0)],
            vec![(20_000, 1.0), (40_000, f64::INFINITY)],
            vec![(20_000, f64::INFINITY), (40_000, f64::INFINITY)],
        ] {
            let regression = prometheus_linear_regression(&points, points[0].0, &cancelled)
                .unwrap()
                .unwrap();
            assert!(regression.0.is_nan());
        }

        cancelled.store(true, Ordering::Relaxed);
        assert!(
            prometheus_linear_regression(&[(10_000, 1.0), (20_000, 2.0)], 10_000, &cancelled,)
                .unwrap_err()
                .contains("cancelled")
        );
    }

    #[test]
    fn prometheus_changes_treats_repeated_nan_and_signed_zero_as_equal() {
        let cancelled = AtomicBool::new(false);
        for (points, expected) in [
            (
                vec![
                    (10_000, 1.0),
                    (20_000, 1.0),
                    (30_000, 2.0),
                    (40_000, 2.0),
                    (50_000, 1.0),
                ],
                2.0,
            ),
            (vec![(20_000, f64::NAN), (40_000, f64::NAN)], 0.0),
            (vec![(20_000, f64::NAN), (40_000, 2.0)], 1.0),
            (vec![(20_000, 0.0), (40_000, -0.0)], 0.0),
            (vec![(50_000, 5.0)], 0.0),
        ] {
            assert_eq!(prometheus_changes(&points, &cancelled).unwrap(), expected);
        }

        cancelled.store(true, Ordering::Relaxed);
        assert!(prometheus_changes(&[(10_000, 1.0)], &cancelled)
            .unwrap_err()
            .contains("cancelled"));
    }

    #[test]
    fn prometheus_resets_counts_only_strict_float_decreases() {
        let cancelled = AtomicBool::new(false);
        for (points, expected) in [
            (vec![(10_000, 100.0), (30_000, 150.0), (50_000, 20.0)], 1.0),
            (
                vec![
                    (10_000, 1.0),
                    (20_000, 1.0),
                    (30_000, 2.0),
                    (40_000, 2.0),
                    (50_000, 1.0),
                ],
                1.0,
            ),
            (vec![(20_000, f64::NAN), (40_000, 2.0)], 0.0),
            (vec![(20_000, 0.0), (40_000, -0.0)], 0.0),
            (vec![(20_000, 1.0), (40_000, f64::INFINITY)], 0.0),
            (vec![(20_000, f64::INFINITY), (40_000, 1.0)], 1.0),
            (vec![(20_000, 1.0), (40_000, f64::NEG_INFINITY)], 1.0),
            (vec![(50_000, 5.0)], 0.0),
        ] {
            assert_eq!(prometheus_resets(&points, &cancelled).unwrap(), expected);
        }

        cancelled.store(true, Ordering::Relaxed);
        assert!(prometheus_resets(&[(10_000, 1.0)], &cancelled)
            .unwrap_err()
            .contains("cancelled"));
    }

    #[test]
    fn prometheus_samples_match_the_pinned_json_float_thresholds() {
        for (value, expected) in [
            (0.0, "0"),
            (-0.0, "-0"),
            (1e-6, "0.000001"),
            (1e-7, "1e-07"),
            (999_999.0, "999999"),
            (1_000_000.0, "1000000"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (6.666_666_666_666_666e31, "6.666666666666666e+31"),
        ] {
            assert_eq!(format_prometheus_value(value), expected, "{value}");
        }

        assert_eq!(
            format_prometheus_label_value(1e23),
            "100000000000000000000000"
        );
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

    #[test]
    fn prometheus_calendar_components_cover_utc_and_non_finite_sentinel() {
        assert_eq!(prometheus_utc_civil_date(0), (1970, 1, 1));
        assert_eq!(prometheus_utc_civil_date(90_061), (1970, 1, 2));
        assert_eq!(
            prometheus_utc_civil_date(i64::MAX),
            (292_277_026_596, 12, 4)
        );
        assert_eq!(PromCalendarOp::Minute.apply(90_061.9), 1.0);
        assert_eq!(PromCalendarOp::Hour.apply(-0.1), 0.0);
        assert_eq!(PromCalendarOp::DayOfMonth.apply(i64::MIN as f64), 4.0);
        assert_eq!(PromCalendarOp::DayOfMonth.apply(-1e20), 4.0);
        assert_eq!(PromCalendarOp::DayOfYear.apply(90_061.9), 2.0);
        assert_eq!(PromCalendarOp::DayOfYear.apply(1_709_208_000.0), 60.0);
        assert_eq!(PromCalendarOp::DaysInMonth.apply(1_709_208_000.0), 29.0);
        assert_eq!(PromCalendarOp::Month.apply(90_061.9), 1.0);
        assert_eq!(PromCalendarOp::Year.apply(90_061.9), 1970.0);
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(PromCalendarOp::Minute.apply(value), 30.0);
            assert_eq!(PromCalendarOp::Hour.apply(value), 15.0);
            assert_eq!(PromCalendarOp::DayOfWeek.apply(value), 0.0);
            assert_eq!(PromCalendarOp::DayOfMonth.apply(value), 4.0);
            assert_eq!(PromCalendarOp::DayOfYear.apply(value), 339.0);
            assert_eq!(PromCalendarOp::DaysInMonth.apply(value), 31.0);
            assert_eq!(PromCalendarOp::Month.apply(value), 12.0);
            assert_eq!(PromCalendarOp::Year.apply(value), 292_277_026_596.0);
        }
    }

    #[test]
    fn prometheus_classic_histogram_quantile_corrects_precision_and_cancels() {
        let cancelled = AtomicBool::new(false);
        let value = prometheus_classic_bucket_quantile(
            0.5,
            vec![
                PromClassicBucket {
                    upper_bound: 1.0,
                    count: 1e12,
                },
                PromClassicBucket {
                    upper_bound: 2.0,
                    count: 1e12 + 0.5,
                },
                PromClassicBucket {
                    upper_bound: 3.0,
                    count: 2e12,
                },
                PromClassicBucket {
                    upper_bound: f64::INFINITY,
                    count: 3e12,
                },
            ],
            &cancelled,
        )
        .unwrap();
        assert_eq!(value.value, 2.5);
        assert!(value.repair.is_none());

        assert_eq!(parse_prometheus_bucket_bound("+Inf"), Ok(f64::INFINITY));
        assert_eq!(
            parse_prometheus_bucket_bound("-Infinity"),
            Ok(f64::NEG_INFINITY)
        );
        assert!(parse_prometheus_bucket_bound("bogus").is_err());
        assert!(parse_prometheus_bucket_bound("inf").is_err());

        cancelled.store(true, Ordering::Relaxed);
        let error = prometheus_classic_bucket_quantile(
            0.5,
            vec![
                PromClassicBucket {
                    upper_bound: 1.0,
                    count: 1.0,
                },
                PromClassicBucket {
                    upper_bound: f64::INFINITY,
                    count: 2.0,
                },
            ],
            &cancelled,
        )
        .unwrap_err();
        assert!(error.contains("cancelled"), "{error}");
    }
}
