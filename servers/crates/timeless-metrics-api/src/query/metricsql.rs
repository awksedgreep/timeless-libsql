//! MetricsQL-only syntax and evaluation.
//!
//! The stable PromQL parser remains unchanged. MetricsQL expressions are
//! accepted only by the explicitly named MetricsQL routes, lowered into the
//! same bounded Rust plan tree, and never sent into SQLite as query syntax.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::AtomicBool;

use rusqlite::Connection;

use super::*;

const MAX_METRICSQL_DEPTH: usize = 64;
const PLACEHOLDER_PREFIX: &str = "__timeless_metricsql_expr_";
const DEFAULT_MAX_SILENCE_INTERVAL_MS: i64 = 300_000;
const PROMETHEUS_STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BinaryOp {
    Default,
    If,
    IfNot,
}

#[derive(Clone, Debug)]
pub(crate) struct BinaryPlan {
    pub(super) op: BinaryOp,
    pub(super) lhs: Box<PromPlan>,
    pub(super) rhs: Box<PromPlan>,
    pub(super) matching: PromVectorMatching,
}

#[derive(Clone, Debug)]
pub(crate) struct UnionPlan {
    pub(super) inputs: Vec<PromPlan>,
}

#[derive(Clone, Debug)]
pub(crate) struct AliasPlan {
    pub(super) inner: Box<PromPlan>,
    pub(super) name: String,
}

#[derive(Clone, Debug)]
pub(crate) struct LabelPlan {
    pub(super) inner: Box<PromPlan>,
    pub(super) operation: LabelOperation,
}

#[derive(Clone, Debug)]
pub(super) enum LabelOperation {
    Set(Vec<(String, String)>),
    Del(Vec<String>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DynamicRollupOp {
    Default,
    Avg,
    Min,
    Max,
    Sum,
    Count,
    Present,
    StdDev,
    StdVar,
    Rate,
    IRate,
    Increase,
    Delta,
    IDelta,
    Deriv,
    Changes,
    Resets,
    First,
    Last,
    Timestamp,
}

impl DynamicRollupOp {
    fn adjusts_window(self) -> bool {
        matches!(
            self,
            Self::Default | Self::Rate | Self::IRate | Self::Deriv | Self::Timestamp
        )
    }

    fn needs_silence_history(self) -> bool {
        matches!(
            self,
            Self::Default
                | Self::Rate
                | Self::IRate
                | Self::Increase
                | Self::Delta
                | Self::IDelta
                | Self::Changes
                | Self::Resets
        )
    }

    fn removes_counter_resets(self) -> bool {
        matches!(self, Self::Rate | Self::IRate | Self::Increase)
    }

    fn retains_metric_name(self) -> bool {
        matches!(
            self,
            Self::Default | Self::Avg | Self::Min | Self::Max | Self::First | Self::Last
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DynamicRollupPlan {
    pub(super) op: DynamicRollupOp,
    pub(super) selector: Selector,
    pub(super) max_lookback: i64,
}

struct LowerContext<'a> {
    original: &'a str,
    lookback: i64,
    #[allow(dead_code)]
    step: i64,
    max_lookback: i64,
    next_placeholder: usize,
    placeholders: HashMap<String, PromPlan>,
}

#[cfg(test)]
fn lower(input: &str, lookback: i64, step: i64) -> Result<PromPlan, String> {
    lower_with_max_lookback(input, lookback, step, lookback)
}

pub(super) fn lower_with_max_lookback(
    input: &str,
    lookback: i64,
    step: i64,
    max_lookback: i64,
) -> Result<PromPlan, String> {
    let mut context = LowerContext {
        original: input,
        lookback,
        step,
        max_lookback,
        next_placeholder: 0,
        placeholders: HashMap::new(),
    };
    let mut plan = lower_expr(input, &mut context, 0)?;
    replace_placeholders(&mut plan, &mut context.placeholders)?;
    if !context.placeholders.is_empty() {
        return Err("internal MetricsQL placeholder was not consumed".into());
    }
    apply_implicit_rollups(&mut plan, max_lookback)?;
    Ok(plan)
}

pub(super) fn vectorize_scalar(plan: PromPlan) -> PromPlan {
    if plan.value_type() == PromValueType::Scalar {
        PromPlan::Conversion(PromConversionPlan {
            inner: Box::new(plan),
            kind: PromConversionKind::Vector,
        })
    } else {
        plan
    }
}

fn lower_expr(
    input: &str,
    context: &mut LowerContext<'_>,
    depth: usize,
) -> Result<PromPlan, String> {
    if depth > MAX_METRICSQL_DEPTH {
        return Err(format!(
            "MetricsQL expression nesting exceeds {MAX_METRICSQL_DEPTH} levels"
        ));
    }
    let input = input.trim();
    if input.is_empty() {
        return Err("no expression found in input".into());
    }
    if let Some(inner) = trailing_keep_metric_names(input)? {
        let plan = lower_expr(inner, context, depth + 1)?;
        if !supports_keep_metric_names(&plan, inner) {
            return Err(
                "MetricsQL keep_metric_names can be applied only to a function or binary operator"
                    .into(),
            );
        }
        return Ok(PromPlan::KeepMetricNames(Box::new(plan)));
    }
    if let Some(binary) = root_binary(input)? {
        let lhs = input[..binary.start].trim();
        let (matching, rhs) = parse_matching_prefix(&input[binary.end..])?;
        if lhs.is_empty() || rhs.trim().is_empty() {
            return Err(format!(
                "MetricsQL {} operator requires two operands",
                binary.op.name()
            ));
        }
        return Ok(PromPlan::MetricsBinary(BinaryPlan {
            op: binary.op,
            lhs: Box::new(lower_expr(lhs, context, depth + 1)?),
            rhs: Box::new(lower_expr(rhs, context, depth + 1)?),
            matching,
        }));
    }
    if let Some(plan) = lower_union_or_alias(input, context, depth + 1)? {
        return Ok(plan);
    }

    let rewritten = rewrite_nested(input, context, depth + 1)?;
    let mut plan = lower_promql(&rewritten, context.lookback)?;
    replace_placeholders(&mut plan, &mut context.placeholders)?;
    Ok(plan)
}

fn supports_keep_metric_names(plan: &PromPlan, input: &str) -> bool {
    match plan {
        PromPlan::Function(_)
        | PromPlan::LabelReplace(_)
        | PromPlan::LabelJoin(_)
        | PromPlan::Absent(_)
        | PromPlan::Sort(_)
        | PromPlan::Conversion(_)
        | PromPlan::Time
        | PromPlan::Timestamp(_)
        | PromPlan::Calendar(_)
        | PromPlan::HistogramQuantile(_)
        | PromPlan::HistogramFraction(_)
        | PromPlan::MetricsUnion(_)
        | PromPlan::MetricsLabels(_)
        | PromPlan::MetricsDynamicRollup(_)
        | PromPlan::MetricsBinary(_)
        | PromPlan::Binary(_)
        | PromPlan::RangeReduction(_) => true,
        // pi() lowers to a scalar, but it remains a function expression in
        // MetricsQL. Numeric literals must not acquire the modifier.
        PromPlan::Scalar(_) => looks_like_function_call(input),
        PromPlan::String(_)
        | PromPlan::KeepMetricNames(_)
        | PromPlan::MetricsAlias(_)
        | PromPlan::Unary(_)
        | PromPlan::Aggregate(_)
        | PromPlan::Selector { .. }
        | PromPlan::RangeSelector { .. }
        | PromPlan::Subquery(_) => false,
    }
}

fn lower_union_or_alias(
    input: &str,
    context: &mut LowerContext<'_>,
    depth: usize,
) -> Result<Option<PromPlan>, String> {
    let start = skip_space_and_comments(input, 0);
    if input.as_bytes().get(start) == Some(&b'(') {
        let close = matching_paren(input, start)?;
        if !has_code(&input[close + 1..]) {
            let had_comma = split_arguments(&input[start + 1..close])?.len() > 1;
            let arguments = metric_sql_arguments(&input[start + 1..close], "union")?;
            if arguments.len() > 1 {
                return lower_union_arguments(arguments, context, depth).map(Some);
            }
            if had_comma && arguments.len() == 1 {
                return lower_expr(arguments[0], context, depth + 1).map(Some);
            }
        }
    }

    let Some((name, name_end)) = read_identifier(input, start) else {
        return Ok(None);
    };
    let open = skip_space_and_comments(input, name_end);
    if input.as_bytes().get(open) != Some(&b'(') {
        return Ok(None);
    }
    let close = matching_paren(input, open)?;
    if has_code(&input[close + 1..]) {
        return Ok(None);
    }
    let normalized = name.to_ascii_lowercase();
    if normalized == "default_rollup" {
        let arguments = metric_sql_arguments(&input[open + 1..close], "default_rollup")?;
        return lower_default_rollup(arguments, context, depth).map(Some);
    }
    if let Some((dynamic, op)) = windowless_rollup_op(&normalized) {
        let arguments = metric_sql_arguments(&input[open + 1..close], &normalized)?;
        return lower_metricsql_rollup(&normalized, dynamic, op, arguments, context, depth)
            .map(Some);
    }
    let function = if normalized == "union" {
        "union"
    } else if name == "alias" {
        "alias"
    } else if name.eq_ignore_ascii_case("label_set") {
        "label_set"
    } else if name.eq_ignore_ascii_case("label_del") {
        "label_del"
    } else {
        return Ok(None);
    };
    let arguments = metric_sql_arguments(&input[open + 1..close], function)?;
    match function {
        "union" => lower_union_arguments(arguments, context, depth).map(Some),
        "alias" => {
            let [inner, name] = arguments.as_slice() else {
                return Err("MetricsQL alias requires an expression and a string name".into());
            };
            let inner = lower_expr(inner, context, depth + 1)?;
            if !matches!(
                inner.value_type(),
                PromValueType::Scalar | PromValueType::Vector
            ) {
                return Err("MetricsQL alias requires a scalar or instant vector".into());
            }
            let alias = lower_promql(name, context.lookback)?;
            let PromPlan::String(name) = alias else {
                return Err("MetricsQL alias name must be a string literal".into());
            };
            Ok(Some(PromPlan::MetricsAlias(AliasPlan {
                inner: Box::new(inner),
                name,
            })))
        }
        "label_set" => lower_label_set(arguments, context, depth).map(Some),
        "label_del" => lower_label_del(arguments, context, depth).map(Some),
        _ => unreachable!("guarded MetricsQL function"),
    }
}

fn windowless_rollup_op(name: &str) -> Option<(DynamicRollupOp, PromRangeOp)> {
    Some(match name {
        "avg_over_time" => (DynamicRollupOp::Avg, PromRangeOp::Avg),
        "min_over_time" => (DynamicRollupOp::Min, PromRangeOp::Min),
        "max_over_time" => (DynamicRollupOp::Max, PromRangeOp::Max),
        "sum_over_time" => (DynamicRollupOp::Sum, PromRangeOp::Sum),
        "count_over_time" => (DynamicRollupOp::Count, PromRangeOp::Count),
        "present_over_time" => (DynamicRollupOp::Present, PromRangeOp::Present),
        "stddev_over_time" => (DynamicRollupOp::StdDev, PromRangeOp::StdDev),
        "stdvar_over_time" => (DynamicRollupOp::StdVar, PromRangeOp::StdVar),
        "rate" => (DynamicRollupOp::Rate, PromRangeOp::Rate),
        "irate" => (DynamicRollupOp::IRate, PromRangeOp::IRate),
        "increase" => (DynamicRollupOp::Increase, PromRangeOp::Increase),
        "delta" => (DynamicRollupOp::Delta, PromRangeOp::Delta),
        "idelta" => (DynamicRollupOp::IDelta, PromRangeOp::IDelta),
        "deriv" => (DynamicRollupOp::Deriv, PromRangeOp::Deriv),
        "changes" => (DynamicRollupOp::Changes, PromRangeOp::Changes),
        "resets" => (DynamicRollupOp::Resets, PromRangeOp::Resets),
        "first_over_time" => (DynamicRollupOp::First, PromRangeOp::First),
        "last_over_time" => (DynamicRollupOp::Last, PromRangeOp::Last),
        _ => return None,
    })
}

fn lower_default_rollup(
    arguments: Vec<&str>,
    context: &mut LowerContext<'_>,
    depth: usize,
) -> Result<PromPlan, String> {
    let [argument] = arguments.as_slice() else {
        return Err(format!(
            "MetricsQL default_rollup requires exactly one argument; got {}",
            arguments.len()
        ));
    };
    let input = lower_expr(argument, context, depth + 1)?;
    match input {
        PromPlan::Selector { selector, .. } => {
            Ok(PromPlan::MetricsDynamicRollup(DynamicRollupPlan {
                op: DynamicRollupOp::Default,
                selector,
                max_lookback: context.max_lookback,
            }))
        }
        PromPlan::RangeSelector { selector, window } => {
            Ok(PromPlan::RangeReduction(PromRangePlan {
                op: PromRangeOp::Last,
                input: PromRangeInput::Selector { selector, window },
                parameter: None,
                source: None,
            }))
        }
        PromPlan::Subquery(subquery) => Ok(PromPlan::RangeReduction(PromRangePlan {
            op: PromRangeOp::Last,
            input: PromRangeInput::Subquery(subquery),
            parameter: None,
            source: None,
        })),
        plan if matches!(
            plan.value_type(),
            PromValueType::Scalar | PromValueType::Vector
        ) =>
        {
            Ok(vectorize_scalar(plan))
        }
        _ => {
            Err("MetricsQL default_rollup requires a scalar, selector, or vector expression".into())
        }
    }
}

fn lower_metricsql_rollup(
    function: &str,
    dynamic: DynamicRollupOp,
    op: PromRangeOp,
    arguments: Vec<&str>,
    context: &mut LowerContext<'_>,
    depth: usize,
) -> Result<PromPlan, String> {
    let [argument] = arguments.as_slice() else {
        return Err(format!(
            "MetricsQL {function} requires exactly one argument; got {}",
            arguments.len()
        ));
    };
    let input = lower_expr(argument, context, depth + 1)?;
    let plan = match input {
        PromPlan::Selector { selector, .. } => PromPlan::MetricsDynamicRollup(DynamicRollupPlan {
            op: dynamic,
            selector,
            max_lookback: context.max_lookback,
        }),
        PromPlan::RangeSelector { selector, window } => PromPlan::RangeReduction(PromRangePlan {
            op,
            input: PromRangeInput::Selector { selector, window },
            parameter: None,
            source: None,
        }),
        PromPlan::Subquery(subquery) => PromPlan::RangeReduction(PromRangePlan {
            op,
            input: PromRangeInput::Subquery(subquery),
            parameter: None,
            source: None,
        }),
        plan if matches!(
            plan.value_type(),
            PromValueType::Scalar | PromValueType::Vector
        ) =>
        {
            PromPlan::RangeReduction(PromRangePlan {
                op,
                input: PromRangeInput::Subquery(SubqueryPlan {
                    inner: Box::new(vectorize_scalar(plan)),
                    window: context.step,
                    resolution: Some(context.step),
                    timing: SelectorTiming::default(),
                }),
                parameter: None,
                source: None,
            })
        }
        _ => {
            return Err(format!(
                "MetricsQL {function} requires a scalar, selector, or vector expression"
            ))
        }
    };
    Ok(
        if matches!(op, PromRangeOp::Avg | PromRangeOp::Min | PromRangeOp::Max) {
            PromPlan::KeepMetricNames(Box::new(plan))
        } else {
            plan
        },
    )
}

fn lower_label_set(
    arguments: Vec<&str>,
    context: &mut LowerContext<'_>,
    depth: usize,
) -> Result<PromPlan, String> {
    let Some((inner, labels)) = arguments.split_first() else {
        return Err("MetricsQL label_set requires an expression".into());
    };
    if labels.len() % 2 != 0 {
        return Err(format!(
            "MetricsQL label_set requires label/value pairs; got {} string arguments",
            labels.len()
        ));
    }
    let inner = lower_label_input(inner, "label_set", context, depth)?;
    let mut pairs = Vec::with_capacity(labels.len() / 2);
    for pair in labels.chunks_exact(2) {
        pairs.push((
            lower_string_argument(pair[0], "label_set", context.lookback)?,
            lower_string_argument(pair[1], "label_set", context.lookback)?,
        ));
    }
    Ok(PromPlan::MetricsLabels(LabelPlan {
        inner: Box::new(inner),
        operation: LabelOperation::Set(pairs),
    }))
}

fn lower_label_del(
    arguments: Vec<&str>,
    context: &mut LowerContext<'_>,
    depth: usize,
) -> Result<PromPlan, String> {
    let Some((inner, labels)) = arguments.split_first() else {
        return Err("MetricsQL label_del requires an expression".into());
    };
    let inner = lower_label_input(inner, "label_del", context, depth)?;
    let labels = labels
        .iter()
        .map(|label| lower_string_argument(label, "label_del", context.lookback))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PromPlan::MetricsLabels(LabelPlan {
        inner: Box::new(inner),
        operation: LabelOperation::Del(labels),
    }))
}

fn lower_label_input(
    input: &str,
    function: &str,
    context: &mut LowerContext<'_>,
    depth: usize,
) -> Result<PromPlan, String> {
    let plan = lower_expr(input, context, depth + 1)?;
    if !matches!(
        plan.value_type(),
        PromValueType::Scalar | PromValueType::Vector
    ) {
        return Err(format!(
            "MetricsQL {function} requires a scalar or instant vector"
        ));
    }
    Ok(plan)
}

fn lower_string_argument(input: &str, function: &str, lookback: i64) -> Result<String, String> {
    match lower_promql(input, lookback)? {
        PromPlan::String(value) => Ok(value),
        _ => Err(format!(
            "MetricsQL {function} label names and values must be string literals"
        )),
    }
}

fn lower_union_arguments(
    arguments: Vec<&str>,
    context: &mut LowerContext<'_>,
    depth: usize,
) -> Result<PromPlan, String> {
    let inputs = arguments
        .into_iter()
        .map(|argument| {
            let plan = lower_expr(argument, context, depth + 1)?;
            if !matches!(
                plan.value_type(),
                PromValueType::Scalar | PromValueType::Vector
            ) {
                return Err("MetricsQL union requires scalar or instant-vector expressions".into());
            }
            Ok(plan)
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(PromPlan::MetricsUnion(UnionPlan { inputs }))
}

fn metric_sql_arguments<'a>(input: &'a str, name: &str) -> Result<Vec<&'a str>, String> {
    let mut arguments = split_arguments(input)?;
    if arguments.len() == 1 && !has_code(arguments[0]) {
        arguments.clear();
    } else if arguments.last().is_some_and(|argument| !has_code(argument)) {
        arguments.pop();
    }
    if arguments.iter().any(|argument| !has_code(argument)) {
        return Err(format!("MetricsQL {name} contains an empty argument"));
    }
    Ok(arguments)
}

fn looks_like_function_call(input: &str) -> bool {
    let at = skip_space_and_comments(input, 0);
    let Some((_, end)) = read_identifier(input, at) else {
        return false;
    };
    let open = skip_space_and_comments(input, end);
    input.as_bytes().get(open) == Some(&b'(')
        && matching_paren(input, open)
            .ok()
            .is_some_and(|close| !has_code(&input[close + 1..]))
}

fn is_metricsql_special_call(name: &str) -> bool {
    name.eq_ignore_ascii_case("union")
        || name == "alias"
        || name.eq_ignore_ascii_case("label_set")
        || name.eq_ignore_ascii_case("label_del")
        || name.eq_ignore_ascii_case("default_rollup")
        || windowless_rollup_op(&name.to_ascii_lowercase()).is_some()
}

fn trailing_keep_metric_names(input: &str) -> Result<Option<&str>, String> {
    let bytes = input.as_bytes();
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    let mut comment = false;
    let mut candidate = None;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if comment {
            if byte == b'\n' {
                comment = false;
            }
            index += 1;
            continue;
        }
        if let Some(delimiter) = quote {
            if delimiter != b'`' && escaped {
                escaped = false;
            } else if delimiter != b'`' && byte == b'\\' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'#' => {
                comment = true;
                index += 1;
            }
            b'"' | b'\'' | b'`' => {
                quote = Some(byte);
                index += 1;
            }
            b'(' => {
                parens += 1;
                index += 1;
            }
            b')' => {
                parens -= 1;
                if parens < 0 {
                    return Err("unexpected closing parenthesis in MetricsQL expression".into());
                }
                index += 1;
            }
            b'[' => {
                brackets += 1;
                index += 1;
            }
            b']' => {
                brackets -= 1;
                if brackets < 0 {
                    return Err("unexpected closing bracket in MetricsQL expression".into());
                }
                index += 1;
            }
            b'{' => {
                braces += 1;
                index += 1;
            }
            b'}' => {
                braces -= 1;
                if braces < 0 {
                    return Err("unexpected closing brace in MetricsQL expression".into());
                }
                index += 1;
            }
            _ if parens == 0 && brackets == 0 && braces == 0 && is_ident_start(byte) => {
                let start = index;
                index += 1;
                while index < bytes.len() && is_ident_continue(bytes[index]) {
                    index += 1;
                }
                if input[start..index].eq_ignore_ascii_case("keep_metric_names") {
                    candidate = Some((start, index));
                }
            }
            _ => index += 1,
        }
    }
    if quote.is_some() {
        return Err("unterminated string in MetricsQL expression".into());
    }
    if parens != 0 || brackets != 0 || braces != 0 {
        return Err("unbalanced delimiter in MetricsQL expression".into());
    }
    Ok(candidate
        .and_then(|(start, end)| (!has_code(&input[end..])).then(|| input[..start].trim_end())))
}

#[derive(Clone, Copy)]
struct RootBinary {
    op: BinaryOp,
    start: usize,
    end: usize,
    priority: i8,
}

impl BinaryOp {
    fn parse(word: &str) -> Option<(Self, i8)> {
        match word {
            "default" => Some((Self::Default, -1)),
            "if" => Some((Self::If, 0)),
            "ifnot" => Some((Self::IfNot, 0)),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::If => "if",
            Self::IfNot => "ifnot",
        }
    }
}

fn root_binary(input: &str) -> Result<Option<RootBinary>, String> {
    let bytes = input.as_bytes();
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    let mut comment = false;
    let mut candidate: Option<RootBinary> = None;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if comment {
            if byte == b'\n' {
                comment = false;
            }
            index += 1;
            continue;
        }
        if let Some(delimiter) = quote {
            if delimiter != b'`' && escaped {
                escaped = false;
            } else if delimiter != b'`' && byte == b'\\' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'#' => {
                comment = true;
                index += 1;
            }
            b'"' | b'\'' | b'`' => {
                quote = Some(byte);
                index += 1;
            }
            b'(' => {
                parens += 1;
                index += 1;
            }
            b')' => {
                parens -= 1;
                if parens < 0 {
                    return Err("unexpected closing parenthesis in MetricsQL expression".into());
                }
                index += 1;
            }
            b'[' => {
                brackets += 1;
                index += 1;
            }
            b']' => {
                brackets -= 1;
                if brackets < 0 {
                    return Err("unexpected closing bracket in MetricsQL expression".into());
                }
                index += 1;
            }
            b'{' => {
                braces += 1;
                index += 1;
            }
            b'}' => {
                braces -= 1;
                if braces < 0 {
                    return Err("unexpected closing brace in MetricsQL expression".into());
                }
                index += 1;
            }
            _ if parens == 0 && brackets == 0 && braces == 0 && is_ident_start(byte) => {
                let start = index;
                index += 1;
                while index < bytes.len() && is_ident_continue(bytes[index]) {
                    index += 1;
                }
                let word = input[start..index].to_ascii_lowercase();
                let Some((op, priority)) = BinaryOp::parse(&word) else {
                    continue;
                };
                if !has_code(&input[..start]) || !has_code(&input[index..]) {
                    continue;
                }
                let current = RootBinary {
                    op,
                    start,
                    end: index,
                    priority,
                };
                if candidate.is_none_or(|existing| {
                    priority < existing.priority
                        || priority == existing.priority && start > existing.start
                }) {
                    candidate = Some(current);
                }
            }
            _ => index += 1,
        }
    }
    if quote.is_some() {
        return Err("unterminated string in MetricsQL expression".into());
    }
    if parens != 0 || brackets != 0 || braces != 0 {
        return Err("unbalanced delimiter in MetricsQL expression".into());
    }
    Ok(candidate)
}

fn has_code(input: &str) -> bool {
    let mut comment = false;
    for byte in input.bytes() {
        if comment {
            if byte == b'\n' {
                comment = false;
            }
        } else if byte == b'#' {
            comment = true;
        } else if !byte.is_ascii_whitespace() {
            return true;
        }
    }
    false
}

fn parse_matching_prefix(input: &str) -> Result<(PromVectorMatching, &str), String> {
    let mut at = skip_space_and_comments(input, 0);
    let Some((word, word_end)) = read_identifier(input, at) else {
        return Ok((PromVectorMatching::Default, &input[at..]));
    };
    let matching_kind = match word.to_ascii_lowercase().as_str() {
        "on" => true,
        "ignoring" => false,
        _ => return Ok((PromVectorMatching::Default, &input[at..])),
    };
    at = skip_space_and_comments(input, word_end);
    if input.as_bytes().get(at) != Some(&b'(') {
        return Ok((PromVectorMatching::Default, input));
    }
    let close = matching_paren(input, at)?;
    let labels = parse_modifier_labels(&input[at + 1..close])?;
    at = skip_space_and_comments(input, close + 1);

    if let Some((join, join_end)) = read_identifier(input, at) {
        if matches!(
            join.to_ascii_lowercase().as_str(),
            "group_left" | "group_right"
        ) {
            at = skip_space_and_comments(input, join_end);
            if input.as_bytes().get(at) == Some(&b'(') {
                at = matching_paren(input, at)? + 1;
            }
            at = skip_space_and_comments(input, at);
        }
    }

    let matching = if matching_kind {
        PromVectorMatching::On(labels.into_iter().collect())
    } else {
        PromVectorMatching::Ignoring(labels.into_iter().collect())
    };
    Ok((matching, &input[at..]))
}

fn parse_modifier_labels(input: &str) -> Result<Vec<String>, String> {
    if input.trim().is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|label| {
            let label = label.trim();
            if label.is_empty()
                || !label
                    .as_bytes()
                    .first()
                    .is_some_and(|byte| is_ident_start(*byte))
                || !label.bytes().all(is_ident_continue)
            {
                Err(format!("invalid MetricsQL matching label {label:?}"))
            } else {
                Ok(label.to_owned())
            }
        })
        .collect()
}

fn rewrite_nested(
    input: &str,
    context: &mut LowerContext<'_>,
    depth: usize,
) -> Result<String, String> {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut copied = 0;
    let mut index = 0;
    let mut quote = None;
    let mut escaped = false;
    let mut comment = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if comment {
            if byte == b'\n' {
                comment = false;
            }
            index += 1;
            continue;
        }
        if let Some(delimiter) = quote {
            if delimiter != b'`' && escaped {
                escaped = false;
            } else if delimiter != b'`' && byte == b'\\' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'#' => {
                comment = true;
                index += 1;
            }
            b'"' | b'\'' | b'`' => {
                quote = Some(byte);
                index += 1;
            }
            b'(' => {
                let close = matching_paren(input, index)?;
                let inner = &input[index + 1..close];
                let identifier = previous_identifier(input, index);
                let identifier_name = identifier.map(|(_, value)| value.to_ascii_lowercase());
                let special_call =
                    identifier.is_some_and(|(_, name)| is_metricsql_special_call(name));
                if special_call || contains_custom_syntax(inner) {
                    let modifier = identifier_name.as_deref().is_some_and(|name| {
                        matches!(
                            name,
                            "on" | "ignoring" | "group_left" | "group_right" | "by" | "without"
                        )
                    });
                    if special_call {
                        let (identifier_start, _) = identifier.expect("special call identifier");
                        output.push_str(&input[copied..identifier_start]);
                        let plan =
                            lower_expr(&input[identifier_start..close + 1], context, depth + 1)?;
                        output.push_str(&store_placeholder(context, plan));
                    } else {
                        output.push_str(&input[copied..index]);
                    }
                    if special_call {
                        // The complete function call was replaced above.
                    } else if modifier {
                        output.push('(');
                        output.push_str(inner);
                        output.push(')');
                    } else if identifier_name.is_some() {
                        output.push('(');
                        for (argument_index, argument) in split_arguments(inner)?.iter().enumerate()
                        {
                            if argument_index > 0 {
                                output.push(',');
                            }
                            if contains_custom_syntax(argument) {
                                let plan = lower_expr(argument, context, depth + 1)?;
                                output.push_str(&store_placeholder(context, plan));
                            } else {
                                output.push_str(argument);
                            }
                        }
                        output.push(')');
                    } else {
                        let plan = lower_expr(inner, context, depth + 1)?;
                        output.push_str(&store_placeholder(context, plan));
                    }
                    copied = close + 1;
                }
                index = close + 1;
            }
            _ => index += 1,
        }
    }
    output.push_str(&input[copied..]);
    Ok(output)
}

fn contains_custom_syntax(input: &str) -> bool {
    contains_custom_word(input) || contains_parenthesized_union(input)
}

fn contains_parenthesized_union(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut quote = None;
    let mut escaped = false;
    let mut comment = false;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if comment {
            if byte == b'\n' {
                comment = false;
            }
            index += 1;
            continue;
        }
        if let Some(delimiter) = quote {
            if delimiter != b'`' && escaped {
                escaped = false;
            } else if delimiter != b'`' && byte == b'\\' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'#' => comment = true,
            b'"' | b'\'' | b'`' => quote = Some(byte),
            b'(' if previous_identifier(input, index).is_none() => {
                let Ok(close) = matching_paren(input, index) else {
                    return false;
                };
                if split_arguments(&input[index + 1..close])
                    .is_ok_and(|arguments| arguments.len() > 1)
                {
                    return true;
                }
            }
            _ => {}
        }
        index += 1;
    }
    false
}

fn contains_custom_word(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut quote = None;
    let mut escaped = false;
    let mut comment = false;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if comment {
            if byte == b'\n' {
                comment = false;
            }
            index += 1;
            continue;
        }
        if let Some(delimiter) = quote {
            if delimiter != b'`' && escaped {
                escaped = false;
            } else if delimiter != b'`' && byte == b'\\' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'#' => {
                comment = true;
                index += 1;
            }
            b'"' | b'\'' | b'`' => {
                quote = Some(byte);
                index += 1;
            }
            _ if is_ident_start(byte) => {
                let start = index;
                index += 1;
                while index < bytes.len() && is_ident_continue(bytes[index]) {
                    index += 1;
                }
                let word = &input[start..index];
                if BinaryOp::parse(&word.to_ascii_lowercase()).is_some()
                    || word.eq_ignore_ascii_case("keep_metric_names")
                    || word.eq_ignore_ascii_case("union")
                    || word == "alias"
                    || word.eq_ignore_ascii_case("label_set")
                    || word.eq_ignore_ascii_case("label_del")
                    || word.eq_ignore_ascii_case("default_rollup")
                    || windowless_rollup_op(&word.to_ascii_lowercase()).is_some()
                {
                    return true;
                }
            }
            _ => index += 1,
        }
    }
    false
}

fn apply_implicit_rollups(plan: &mut PromPlan, max_lookback: i64) -> Result<(), String> {
    if let PromPlan::Selector { selector, .. } = plan {
        *plan = PromPlan::MetricsDynamicRollup(DynamicRollupPlan {
            op: DynamicRollupOp::Default,
            selector: selector.clone(),
            max_lookback,
        });
        return Ok(());
    }
    if let PromPlan::Timestamp(timestamp) = plan {
        if let PromPlan::Selector { selector, .. } = timestamp.inner.as_ref() {
            *plan = PromPlan::MetricsDynamicRollup(DynamicRollupPlan {
                op: DynamicRollupOp::Timestamp,
                selector: selector.clone(),
                max_lookback,
            });
            return Ok(());
        }
    }
    match plan {
        PromPlan::Scalar(_)
        | PromPlan::String(_)
        | PromPlan::Time
        | PromPlan::MetricsDynamicRollup(_)
        | PromPlan::RangeSelector { .. } => {}
        PromPlan::KeepMetricNames(inner) | PromPlan::Unary(inner) => {
            apply_implicit_rollups(inner, max_lookback)?
        }
        PromPlan::Function(function) => {
            apply_implicit_rollups(&mut function.inner, max_lookback)?;
            for parameter in &mut function.parameters {
                apply_implicit_rollups(parameter, max_lookback)?;
            }
        }
        PromPlan::LabelReplace(plan) => apply_implicit_rollups(&mut plan.inner, max_lookback)?,
        PromPlan::LabelJoin(plan) => apply_implicit_rollups(&mut plan.inner, max_lookback)?,
        PromPlan::Absent(plan) => apply_implicit_rollups(&mut plan.inner, max_lookback)?,
        PromPlan::Sort(plan) => apply_implicit_rollups(&mut plan.inner, max_lookback)?,
        PromPlan::Conversion(plan) => apply_implicit_rollups(&mut plan.inner, max_lookback)?,
        PromPlan::Timestamp(plan) => apply_implicit_rollups(&mut plan.inner, max_lookback)?,
        PromPlan::Calendar(plan) => apply_implicit_rollups(&mut plan.inner, max_lookback)?,
        PromPlan::HistogramQuantile(plan) => {
            apply_implicit_rollups(&mut plan.quantile, max_lookback)?;
            apply_implicit_rollups(&mut plan.inner, max_lookback)?;
        }
        PromPlan::HistogramFraction(plan) => {
            apply_implicit_rollups(&mut plan.lower, max_lookback)?;
            apply_implicit_rollups(&mut plan.upper, max_lookback)?;
            apply_implicit_rollups(&mut plan.inner, max_lookback)?;
        }
        PromPlan::MetricsUnion(plan) => {
            for input in &mut plan.inputs {
                apply_implicit_rollups(input, max_lookback)?;
            }
        }
        PromPlan::MetricsAlias(plan) => apply_implicit_rollups(&mut plan.inner, max_lookback)?,
        PromPlan::MetricsLabels(plan) => apply_implicit_rollups(&mut plan.inner, max_lookback)?,
        PromPlan::MetricsBinary(plan) => {
            apply_implicit_rollups(&mut plan.lhs, max_lookback)?;
            apply_implicit_rollups(&mut plan.rhs, max_lookback)?;
        }
        PromPlan::Binary(plan) => {
            apply_implicit_rollups(&mut plan.lhs, max_lookback)?;
            apply_implicit_rollups(&mut plan.rhs, max_lookback)?;
        }
        PromPlan::Aggregate(plan) => {
            apply_implicit_rollups(&mut plan.inner, max_lookback)?;
            if let Some(parameter) = &mut plan.param {
                apply_implicit_rollups(parameter, max_lookback)?;
            }
        }
        PromPlan::RangeReduction(plan) => {
            if let Some(parameter) = &mut plan.parameter {
                apply_implicit_rollups(parameter, max_lookback)?;
            }
            if let PromRangeInput::Subquery(subquery) = &mut plan.input {
                apply_implicit_rollups(&mut subquery.inner, max_lookback)?;
            }
        }
        PromPlan::Subquery(plan) => apply_implicit_rollups(&mut plan.inner, max_lookback)?,
        PromPlan::Selector { .. } => unreachable!("selector handled before traversal"),
    }
    Ok(())
}

fn split_arguments(input: &str) -> Result<Vec<&str>, String> {
    let bytes = input.as_bytes();
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    let mut comment = false;
    let mut start = 0;
    let mut output = Vec::new();
    for (index, byte) in bytes.iter().copied().enumerate() {
        if comment {
            if byte == b'\n' {
                comment = false;
            }
            continue;
        }
        if let Some(delimiter) = quote {
            if delimiter != b'`' && escaped {
                escaped = false;
            } else if delimiter != b'`' && byte == b'\\' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            continue;
        }
        match byte {
            b'#' => comment = true,
            b'"' | b'\'' | b'`' => quote = Some(byte),
            b'(' => parens += 1,
            b')' => parens -= 1,
            b'[' => brackets += 1,
            b']' => brackets -= 1,
            b'{' => braces += 1,
            b'}' => braces -= 1,
            b',' if parens == 0 && brackets == 0 && braces == 0 => {
                output.push(&input[start..index]);
                start = index + 1;
            }
            _ => {}
        }
        if parens < 0 || brackets < 0 || braces < 0 {
            return Err("unbalanced MetricsQL function argument".into());
        }
    }
    if quote.is_some() || parens != 0 || brackets != 0 || braces != 0 {
        return Err("unbalanced MetricsQL function argument".into());
    }
    output.push(&input[start..]);
    Ok(output)
}

fn store_placeholder(context: &mut LowerContext<'_>, plan: PromPlan) -> String {
    loop {
        let name = format!("{PLACEHOLDER_PREFIX}{:08x}", context.next_placeholder);
        context.next_placeholder += 1;
        if context.original.contains(&name) || context.placeholders.contains_key(&name) {
            continue;
        }
        context.placeholders.insert(name.clone(), plan);
        return name;
    }
}

fn replace_placeholders(
    plan: &mut PromPlan,
    placeholders: &mut HashMap<String, PromPlan>,
) -> Result<(), String> {
    if let PromPlan::Selector { selector, .. } = plan {
        if let MetricSelection::Exact(metric) = &selector.metric {
            if let Some(replacement) = placeholders.remove(metric) {
                *plan = replacement;
                return Ok(());
            }
        }
    }
    match plan {
        PromPlan::Scalar(_) | PromPlan::String(_) | PromPlan::Time => {}
        PromPlan::KeepMetricNames(inner) | PromPlan::Unary(inner) => {
            replace_placeholders(inner, placeholders)?
        }
        PromPlan::Function(function) => {
            replace_placeholders(&mut function.inner, placeholders)?;
            for parameter in &mut function.parameters {
                replace_placeholders(parameter, placeholders)?;
            }
        }
        PromPlan::LabelReplace(plan) => replace_placeholders(&mut plan.inner, placeholders)?,
        PromPlan::LabelJoin(plan) => replace_placeholders(&mut plan.inner, placeholders)?,
        PromPlan::Absent(plan) => replace_placeholders(&mut plan.inner, placeholders)?,
        PromPlan::Sort(plan) => replace_placeholders(&mut plan.inner, placeholders)?,
        PromPlan::Conversion(plan) => replace_placeholders(&mut plan.inner, placeholders)?,
        PromPlan::Timestamp(plan) => replace_placeholders(&mut plan.inner, placeholders)?,
        PromPlan::Calendar(plan) => replace_placeholders(&mut plan.inner, placeholders)?,
        PromPlan::HistogramQuantile(plan) => {
            replace_placeholders(&mut plan.quantile, placeholders)?;
            replace_placeholders(&mut plan.inner, placeholders)?;
        }
        PromPlan::HistogramFraction(plan) => {
            replace_placeholders(&mut plan.lower, placeholders)?;
            replace_placeholders(&mut plan.upper, placeholders)?;
            replace_placeholders(&mut plan.inner, placeholders)?;
        }
        PromPlan::MetricsUnion(plan) => {
            for input in &mut plan.inputs {
                replace_placeholders(input, placeholders)?;
            }
        }
        PromPlan::MetricsAlias(plan) => replace_placeholders(&mut plan.inner, placeholders)?,
        PromPlan::MetricsLabels(plan) => replace_placeholders(&mut plan.inner, placeholders)?,
        PromPlan::MetricsDynamicRollup(_) => {}
        PromPlan::MetricsBinary(plan) => {
            replace_placeholders(&mut plan.lhs, placeholders)?;
            replace_placeholders(&mut plan.rhs, placeholders)?;
        }
        PromPlan::Binary(plan) => {
            replace_placeholders(&mut plan.lhs, placeholders)?;
            replace_placeholders(&mut plan.rhs, placeholders)?;
        }
        PromPlan::Aggregate(plan) => {
            replace_placeholders(&mut plan.inner, placeholders)?;
            if let Some(parameter) = &mut plan.param {
                replace_placeholders(parameter, placeholders)?;
            }
        }
        PromPlan::Selector { .. }
        | PromPlan::RangeSelector { .. }
        | PromPlan::RangeReduction(_) => {}
        PromPlan::Subquery(plan) => replace_placeholders(&mut plan.inner, placeholders)?,
    }
    Ok(())
}

fn previous_identifier(input: &str, before: usize) -> Option<(usize, &str)> {
    let bytes = input.as_bytes();
    let mut end = before;
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    let mut start = end;
    while start > 0 && is_ident_continue(bytes[start - 1]) {
        start -= 1;
    }
    (start < end).then_some((start, &input[start..end]))
}

fn read_identifier(input: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = input.as_bytes();
    if !bytes.get(start).is_some_and(|byte| is_ident_start(*byte)) {
        return None;
    }
    let mut end = start + 1;
    while end < bytes.len() && is_ident_continue(bytes[end]) {
        end += 1;
    }
    Some((&input[start..end], end))
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'.')
}

fn skip_space_and_comments(input: &str, mut at: usize) -> usize {
    let bytes = input.as_bytes();
    loop {
        while at < bytes.len() && bytes[at].is_ascii_whitespace() {
            at += 1;
        }
        if bytes.get(at) != Some(&b'#') {
            return at;
        }
        while at < bytes.len() && bytes[at] != b'\n' {
            at += 1;
        }
    }
}

fn matching_paren(input: &str, open: usize) -> Result<usize, String> {
    let bytes = input.as_bytes();
    if bytes.get(open) != Some(&b'(') {
        return Err("internal MetricsQL parenthesis scan did not start at '('".into());
    }
    let mut depth = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    let mut comment = false;
    for (index, byte) in bytes.iter().copied().enumerate().skip(open) {
        if comment {
            if byte == b'\n' {
                comment = false;
            }
            continue;
        }
        if let Some(delimiter) = quote {
            if delimiter != b'`' && escaped {
                escaped = false;
            } else if delimiter != b'`' && byte == b'\\' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            continue;
        }
        match byte {
            b'#' => comment = true,
            b'"' | b'\'' | b'`' => quote = Some(byte),
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => {}
        }
    }
    Err("unclosed parenthesis in MetricsQL expression".into())
}

fn metricsql_scrape_interval(timestamps: &[i64], default_interval: i64) -> i64 {
    if timestamps.len() < 2 {
        return default_interval;
    }
    let first = timestamps.len().saturating_sub(21);
    let mut intervals = timestamps[first..]
        .windows(2)
        .map(|pair| (pair[1] - pair[0]) as f64)
        .collect::<Vec<_>>();
    intervals.sort_by(|left, right| left.total_cmp(right));
    let rank = 0.6 * (intervals.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = (lower + 1).min(intervals.len() - 1);
    let weight = rank - rank.floor();
    let interval = (intervals[lower] * (1.0 - weight) + intervals[upper] * weight) as i64;
    if interval > 0 {
        interval
    } else {
        default_interval
    }
}

fn metricsql_max_previous_interval(scrape_interval: i64) -> i64 {
    if scrape_interval <= 2_000 {
        scrape_interval.saturating_mul(5)
    } else if scrape_interval <= 4_000 {
        scrape_interval.saturating_mul(3)
    } else if scrape_interval <= 8_000 {
        scrape_interval.saturating_mul(2)
    } else if scrape_interval <= 16_000 {
        scrape_interval.saturating_add(scrape_interval / 2)
    } else if scrape_interval <= 32_000 {
        scrape_interval.saturating_add(scrape_interval / 4)
    } else {
        scrape_interval.saturating_add(scrape_interval / 8)
    }
}

fn dynamic_rollup_window(
    op: DynamicRollupOp,
    timestamps: &[i64],
    step: i64,
    instant: bool,
    max_lookback: i64,
) -> (i64, i64) {
    let mut max_previous = if instant {
        step
    } else {
        metricsql_max_previous_interval(metricsql_scrape_interval(timestamps, step))
    };
    if max_lookback > 0 {
        max_previous = max_previous.min(max_lookback);
    }
    let mut window = if op.adjusts_window() {
        step.max(max_previous)
    } else {
        step
    };
    if op == DynamicRollupOp::Default && max_lookback > 0 {
        window = window.min(max_lookback);
    }
    (window, max_previous)
}

fn is_prometheus_stale_nan(value: f64) -> bool {
    value.to_bits() == PROMETHEUS_STALE_NAN_BITS
}

fn correct_metricsql_counter_resets(samples: &mut [(i64, f64)], max_staleness: i64) {
    let Some((_, first)) = samples.first().copied() else {
        return;
    };
    let mut correction = 0.0;
    let mut previous_timestamp = samples[0].0;
    let mut previous_value = first;
    let mut previous_adjusted = first;
    for (index, (timestamp, value)) in samples.iter_mut().enumerate() {
        if value.is_nan() {
            correction = 0.0;
            previous_timestamp = *timestamp;
            previous_value = *value;
            previous_adjusted = *value;
            continue;
        }
        let raw = *value;
        let delta = raw - previous_value;
        if delta < 0.0 {
            correction += if (-delta * 8.0) < previous_value {
                previous_value - raw
            } else {
                previous_value
            };
        }
        if index > 0 && max_staleness > 0 && *timestamp - previous_timestamp > max_staleness {
            correction = 0.0;
            previous_timestamp = *timestamp;
            previous_value = raw;
            previous_adjusted = raw;
            continue;
        }
        previous_timestamp = *timestamp;
        previous_value = raw;
        *value = raw + correction;
        if index > 0 && !previous_adjusted.is_nan() && *value < previous_adjusted {
            *value = previous_adjusted;
        }
        previous_adjusted = *value;
    }
}

fn previous_numeric_sample(
    samples: &[(i64, f64)],
    before: usize,
    lower: i64,
) -> Option<(i64, f64)> {
    for &(timestamp, value) in samples[..before].iter().rev() {
        if timestamp <= lower || value.is_nan() {
            return None;
        }
        return Some((timestamp, value));
    }
    None
}

fn real_previous_numeric_sample(
    samples: &[(i64, f64)],
    before: usize,
    current: i64,
    max_lookback: i64,
) -> Option<(i64, f64)> {
    let &(timestamp, value) = samples[..before].last()?;
    if value.is_nan() || max_lookback > 0 && current.saturating_sub(timestamp) >= max_lookback {
        None
    } else {
        Some((timestamp, value))
    }
}

fn real_next_numeric_sample(samples: &[(i64, f64)], after: usize) -> Option<(i64, f64)> {
    let &(timestamp, value) = samples.get(after)?;
    (!value.is_nan()).then_some((timestamp, value))
}

fn metricsql_numeric_window(samples: &[(i64, f64)]) -> Vec<(i64, f64)> {
    let start = samples
        .iter()
        .rposition(|(_, value)| is_prometheus_stale_nan(*value))
        .map_or(0, |index| index + 1);
    samples[start..]
        .iter()
        .copied()
        .filter(|(_, value)| !value.is_nan())
        .collect()
}

fn metricsql_aggregate(
    op: PromAggregateOp,
    samples: &[(i64, f64)],
    cancelled: &AtomicBool,
) -> Result<Option<f64>, String> {
    let Some((_, first)) = samples.first().copied() else {
        return Ok(None);
    };
    let mut reduction = PromAggregateState::new(op, first);
    for &(_, value) in &samples[1..] {
        check_cancelled(cancelled)?;
        reduction.add(op, value);
    }
    Ok(Some(reduction.finish(op)))
}

fn metricsql_delta(
    samples: &[(i64, f64)],
    previous: Option<(i64, f64)>,
    real_previous: Option<(i64, f64)>,
    real_next: Option<(i64, f64)>,
) -> Option<f64> {
    let Some((_, last)) = samples.last().copied() else {
        return previous.map(|_| 0.0);
    };
    let mut values = samples;
    let baseline = if let Some((_, previous)) = previous {
        previous
    } else if let Some((_, previous)) = real_previous {
        return Some(last - previous);
    } else {
        let first = samples[0].1;
        let delta = samples
            .get(1)
            .map(|(_, value)| *value - first)
            .or_else(|| real_next.map(|(_, value)| value - first))
            .unwrap_or(0.0);
        if first.abs() < 10.0 * (delta.abs() + 1.0) {
            0.0
        } else {
            values = &samples[1..];
            first
        }
    };
    Some(values.last().map_or(0.0, |(_, value)| *value - baseline))
}

fn metricsql_changed(previous: f64, current: f64) -> bool {
    current != previous && (current - previous).abs() >= 1e-12 * current.abs()
}

fn metricsql_dynamic_value(
    op: DynamicRollupOp,
    samples: &[(i64, f64)],
    lo: usize,
    hi: usize,
    lower: i64,
    max_previous: i64,
    max_lookback: i64,
    cancelled: &AtomicBool,
) -> Result<Option<f64>, String> {
    let values = &samples[lo..hi];
    if op == DynamicRollupOp::Default {
        return Ok(values
            .last()
            .filter(|(_, value)| !is_prometheus_stale_nan(*value))
            .map(|(_, value)| *value));
    }
    if op == DynamicRollupOp::Timestamp {
        return Ok(values
            .last()
            .filter(|(_, value)| !is_prometheus_stale_nan(*value))
            .map(|(timestamp, _)| *timestamp as f64 / 1_000.0));
    }
    if op == DynamicRollupOp::First {
        return Ok(values
            .first()
            .filter(|(_, value)| !is_prometheus_stale_nan(*value))
            .map(|(_, value)| *value));
    }
    if op == DynamicRollupOp::Last {
        return Ok(values
            .last()
            .filter(|(_, value)| !is_prometheus_stale_nan(*value))
            .map(|(_, value)| *value));
    }

    let numeric = metricsql_numeric_window(values);
    let previous = previous_numeric_sample(samples, lo, lower.saturating_sub(max_previous));
    let current = numeric.first().map_or(lower, |(timestamp, _)| *timestamp);
    let real_previous = real_previous_numeric_sample(samples, lo, current, max_lookback);
    let real_next = real_next_numeric_sample(samples, hi);
    match op {
        DynamicRollupOp::Avg => metricsql_aggregate(PromAggregateOp::Avg, &numeric, cancelled),
        DynamicRollupOp::Min => metricsql_aggregate(PromAggregateOp::Min, &numeric, cancelled),
        DynamicRollupOp::Max => metricsql_aggregate(PromAggregateOp::Max, &numeric, cancelled),
        DynamicRollupOp::Sum => metricsql_aggregate(PromAggregateOp::Sum, &numeric, cancelled),
        DynamicRollupOp::Count => Ok((!numeric.is_empty()).then_some(numeric.len() as f64)),
        DynamicRollupOp::Present => Ok((!numeric.is_empty()).then_some(1.0)),
        DynamicRollupOp::StdDev => {
            metricsql_aggregate(PromAggregateOp::StdDev, &numeric, cancelled)
        }
        DynamicRollupOp::StdVar => {
            metricsql_aggregate(PromAggregateOp::StdVar, &numeric, cancelled)
        }
        DynamicRollupOp::Deriv => {
            if numeric.len() == 1 {
                Ok(Some(0.0))
            } else if numeric.len() < 2 {
                Ok(None)
            } else {
                prometheus_linear_regression(&numeric, numeric[0].0, cancelled)
                    .map(|value| value.map(|(slope, _)| slope))
            }
        }
        DynamicRollupOp::Rate | DynamicRollupOp::IRate => {
            if op == DynamicRollupOp::IRate {
                let (start, end) = if numeric.len() >= 2 {
                    (numeric[numeric.len() - 2], numeric[numeric.len() - 1])
                } else if numeric.len() == 1 {
                    let Some(previous) = previous else {
                        return Ok(None);
                    };
                    (previous, numeric[0])
                } else {
                    return Ok(None);
                };
                let elapsed = end.0 - start.0;
                return Ok((elapsed > 0).then(|| (end.1 - start.1) / (elapsed as f64 / 1_000.0)));
            }
            let Some(end) = numeric.last().copied() else {
                return Ok(previous.map(|_| 0.0));
            };
            let start = previous.or_else(|| (numeric.len() >= 2).then_some(numeric[0]));
            let Some(start) = start else {
                return Ok(None);
            };
            let elapsed = end.0 - start.0;
            Ok((elapsed > 0).then(|| (end.1 - start.1) / (elapsed as f64 / 1_000.0)))
        }
        DynamicRollupOp::Increase | DynamicRollupOp::Delta => Ok(metricsql_delta(
            &numeric,
            previous,
            real_previous,
            real_next,
        )),
        DynamicRollupOp::IDelta => {
            let Some((_, last)) = numeric.last().copied() else {
                return Ok(previous.map(|_| 0.0));
            };
            let prior = numeric
                .get(numeric.len().saturating_sub(2))
                .filter(|_| numeric.len() >= 2)
                .copied()
                .or(previous);
            Ok(Some(prior.map_or(last, |(_, value)| last - value)))
        }
        DynamicRollupOp::Changes => {
            if numeric.is_empty() {
                return Ok(previous.map(|_| 0.0));
            }
            let mut count = 0_u64;
            let mut index = 0_usize;
            let mut prior = if let Some((_, value)) = previous.or(real_previous) {
                value
            } else {
                count = 1;
                index = 1;
                numeric[0].1
            };
            for &(_, value) in &numeric[index..] {
                check_cancelled(cancelled)?;
                if metricsql_changed(prior, value) {
                    count += 1;
                    prior = value;
                }
            }
            Ok(Some(count as f64))
        }
        DynamicRollupOp::Resets => {
            if numeric.is_empty() {
                return Ok(previous.map(|_| 0.0));
            }
            let (mut prior, start) = previous.map_or((numeric[0].1, 1), |(_, value)| (value, 0));
            let mut count = 0_u64;
            for &(_, value) in &numeric[start..] {
                check_cancelled(cancelled)?;
                if value < prior && metricsql_changed(prior, value) {
                    count += 1;
                }
                prior = value;
            }
            Ok(Some(count as f64))
        }
        DynamicRollupOp::Default
        | DynamicRollupOp::First
        | DynamicRollupOp::Last
        | DynamicRollupOp::Timestamp => unreachable!("handled before numeric rollup"),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_dynamic_rollup(
    conn: &Connection,
    features: QueryFeatures,
    plan: &DynamicRollupPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    keep_metric_names: bool,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    let selection_start = plan
        .selector
        .timing
        .selection_time(start, query_start, query_end)?;
    let selection_stop = plan
        .selector
        .timing
        .selection_time(stop, query_start, query_end)?;
    let read_start = selection_start.min(selection_stop);
    let read_stop = selection_start.max(selection_stop);
    let history = if plan.op.needs_silence_history() {
        DEFAULT_MAX_SILENCE_INTERVAL_MS
            .max(plan.max_lookback)
            .saturating_add(step)
    } else {
        step
    };
    let catalogs = prometheus_catalogs(conn, features.table, &plan.selector, limits, cancelled)?;
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
            &plan.selector.filter,
            storage_seconds_floor(read_start.saturating_sub(history)),
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
            let mut samples = Vec::with_capacity(series.len());
            for index in 0..series.len() {
                check_cancelled(cancelled)?;
                samples.push((
                    seconds_to_millis(series.timestamp(raw.frame.as_deref(), index)?),
                    series.value(raw.frame.as_deref(), index)?,
                ));
            }
            let timestamps = samples
                .iter()
                .map(|(timestamp, _)| *timestamp)
                .collect::<Vec<_>>();
            let (window, max_previous) =
                dynamic_rollup_window(plan.op, &timestamps, step, instant, plan.max_lookback);
            if plan.op.removes_counter_resets() {
                correct_metricsql_counter_resets(&mut samples, plan.max_lookback);
            }
            let item_start = body.len();
            comma(&mut body, emitted);
            let retain_name = keep_metric_names || plan.op.retains_metric_name();
            write_prometheus_item_prefix(
                &mut body,
                retain_name.then_some(metric.as_str()),
                &meta.labels,
                instant,
                limits,
            )?;
            enforce_prometheus_output(&body, points, limits)?;
            let mut lo = 0_usize;
            let mut hi = 0_usize;
            let mut item_points = 0_u64;
            let mut outer = start;
            loop {
                check_cancelled(cancelled)?;
                let selection_time =
                    plan.selector
                        .timing
                        .selection_time(outer, query_start, query_end)?;
                while hi < samples.len() && samples[hi].0 <= selection_time {
                    hi += 1;
                }
                let lower = selection_time.saturating_sub(window);
                while lo < hi && samples[lo].0 <= lower {
                    lo += 1;
                }
                if let Some(value) = metricsql_dynamic_value(
                    plan.op,
                    &samples,
                    lo,
                    hi,
                    lower,
                    max_previous,
                    plan.max_lookback,
                    cancelled,
                )? {
                    admit_prometheus_point(points.saturating_add(item_points), limits)?;
                    if !instant {
                        comma(&mut body, item_points as usize);
                    }
                    write_prometheus_sample(&mut body, outer, value)?;
                    item_points += 1;
                    enforce_prometheus_output(&body, points.saturating_add(item_points), limits)?;
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
pub(super) fn execute_union(
    conn: &Connection,
    features: QueryFeatures,
    union: &UnionPlan,
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
    let mut frame_bytes = 0_usize;
    let mut intermediate_points = 0_u64;
    let mut output = Vec::new();
    let mut labels_seen = BTreeSet::new();
    let mut retained_label_bytes = 0_usize;
    for input in &union.inputs {
        check_cancelled(cancelled)?;
        let input_type = input.value_type();
        let child = execute_prometheus(
            conn,
            features,
            input,
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
        frame_bytes = frame_bytes.saturating_add(child.frame_bytes);
        intermediate_points = intermediate_points
            .saturating_add(child.intermediate_points)
            .saturating_add(child.points);
        enforce_intermediate_work(intermediate_points, limits)?;
        let series = into_series(decode_prometheus_intermediate(
            &child.body,
            input_type,
            instant,
            limits,
            cancelled,
        )?);
        for series in series {
            check_cancelled(cancelled)?;
            if labels_seen.insert(series.labels.clone()) {
                charge_metricsql_labels(&mut retained_label_bytes, &series.labels, limits)?;
                output.push(series);
            } else if input_type == PromValueType::Scalar {
                return Err("duplicate output timeseries: {}".into());
            }
        }
    }
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
pub(super) fn execute_alias(
    conn: &Connection,
    features: QueryFeatures,
    alias: &AliasPlan,
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
    if alias.name.len() > limits.max_response_bytes {
        return Err(prometheus_response_limit_error(limits));
    }
    let input_type = alias.inner.value_type();
    let child = execute_prometheus(
        conn,
        features,
        &alias.inner,
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
    let mut series = into_series(decode_prometheus_intermediate(
        &child.body,
        input_type,
        instant,
        limits,
        cancelled,
    )?);
    let mut labels_seen = BTreeSet::new();
    let mut generated_label_bytes = 0_usize;
    for item in &mut series {
        check_cancelled(cancelled)?;
        if alias.name.is_empty() {
            item.labels.remove("__name__");
        } else {
            generated_label_bytes = generated_label_bytes
                .checked_add("__name__".len())
                .and_then(|bytes| bytes.checked_add(alias.name.len()))
                .filter(|bytes| *bytes <= limits.max_response_bytes)
                .ok_or_else(|| prometheus_response_limit_error(limits))?;
            item.labels.insert("__name__".into(), alias.name.clone());
        }
        if !labels_seen.insert(item.labels.clone()) {
            return Err("duplicate output timeseries: aliased labelset".into());
        }
    }
    encode_prometheus_intermediate(
        IntermediateValue::Vector(series),
        instant,
        child.frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_labels(
    conn: &Connection,
    features: QueryFeatures,
    plan: &LabelPlan,
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
    if let LabelOperation::Set(pairs) = &plan.operation {
        let minimum_generated_bytes = pairs
            .iter()
            .filter(|(_, value)| !value.is_empty())
            .try_fold(0_usize, |bytes, (name, value)| {
                bytes
                    .checked_add(name.len())
                    .and_then(|bytes| bytes.checked_add(value.len()))
            })
            .filter(|bytes| *bytes <= limits.max_response_bytes)
            .ok_or_else(|| prometheus_response_limit_error(limits))?;
        debug_assert!(minimum_generated_bytes <= limits.max_response_bytes);
    }
    let input_type = plan.inner.value_type();
    let child = execute_prometheus(
        conn,
        features,
        &plan.inner,
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
    let mut series = into_series(decode_prometheus_intermediate(
        &child.body,
        input_type,
        instant,
        limits,
        cancelled,
    )?);
    let mut labels_seen = BTreeSet::new();
    let mut generated_label_bytes = 0_usize;
    for item in &mut series {
        check_cancelled(cancelled)?;
        match &plan.operation {
            LabelOperation::Set(pairs) => {
                for (name, value) in pairs {
                    check_cancelled(cancelled)?;
                    if value.is_empty() {
                        item.labels.remove(name);
                    } else {
                        generated_label_bytes = generated_label_bytes
                            .checked_add(name.len())
                            .and_then(|bytes| bytes.checked_add(value.len()))
                            .filter(|bytes| *bytes <= limits.max_response_bytes)
                            .ok_or_else(|| prometheus_response_limit_error(limits))?;
                        item.labels.insert(name.clone(), value.clone());
                    }
                }
            }
            LabelOperation::Del(labels) => {
                for name in labels {
                    check_cancelled(cancelled)?;
                    item.labels.remove(name);
                }
            }
        }
        if !labels_seen.insert(item.labels.clone()) {
            return Err("duplicate output timeseries: label transformation".into());
        }
    }
    encode_prometheus_intermediate(
        IntermediateValue::Vector(series),
        instant,
        child.frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

fn charge_metricsql_labels(
    total: &mut usize,
    labels: &BTreeMap<String, String>,
    limits: PromQueryLimits,
) -> Result<(), String> {
    for (name, value) in labels {
        *total = total
            .checked_add(name.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .filter(|bytes| *bytes <= limits.max_response_bytes)
            .ok_or_else(|| prometheus_response_limit_error(limits))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_binary(
    conn: &Connection,
    features: QueryFeatures,
    binary: &BinaryPlan,
    start: i64,
    stop: i64,
    step: i64,
    instant: bool,
    query_start: i64,
    query_end: i64,
    limits: PromQueryLimits,
    annotations: &mut PromAnnotations,
    keep_metric_names: bool,
    cancelled: &AtomicBool,
) -> Result<ReadOutput, String> {
    check_cancelled(cancelled)?;
    let lhs_type = binary.lhs.value_type();
    let rhs_type = binary.rhs.value_type();
    let mut candidates = None;
    let lhs = if binary.op == BinaryOp::Default {
        match execute_comparison_with_candidates(
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
        )? {
            Some((output, comparison_candidates)) => {
                candidates = Some(comparison_candidates);
                output
            }
            None => execute_prometheus(
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
            )?,
        }
    } else {
        execute_prometheus(
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
        )?
    };
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
    let mut intermediate_points = lhs
        .intermediate_points
        .saturating_add(lhs.points)
        .saturating_add(rhs.intermediate_points)
        .saturating_add(rhs.points);
    let frame_bytes = lhs.frame_bytes.saturating_add(rhs.frame_bytes);
    let mut lhs = into_series(decode_prometheus_intermediate(
        &lhs.body, lhs_type, instant, limits, cancelled,
    )?);
    let rhs = into_series(decode_prometheus_intermediate(
        &rhs.body, rhs_type, instant, limits, cancelled,
    )?);
    if let Some(candidates) = candidates {
        intermediate_points = intermediate_points.saturating_add(candidates.work_points);
        lhs = merge_comparison_candidates(lhs, candidates.series, cancelled)?;
    }
    enforce_intermediate_work(intermediate_points, limits)?;
    let output = apply_binary(binary, lhs, rhs, keep_metric_names, cancelled)?;
    encode_prometheus_intermediate(
        IntermediateValue::Vector(output),
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )
}

struct CandidateSeries {
    series: Vec<IntermediateSeries>,
    work_points: u64,
}

#[allow(clippy::too_many_arguments)]
fn execute_comparison_with_candidates(
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
) -> Result<Option<(ReadOutput, CandidateSeries)>, String> {
    let PromPlan::Binary(comparison) = plan else {
        return Ok(None);
    };
    if comparison.return_bool || comparison.op.is_arithmetic() || comparison.op.is_set() {
        return Ok(None);
    }
    let lhs_type = comparison.lhs.value_type();
    let rhs_type = comparison.rhs.value_type();
    let lhs_output = execute_prometheus(
        conn,
        features,
        &comparison.lhs,
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
    let rhs_output = execute_prometheus(
        conn,
        features,
        &comparison.rhs,
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
    let intermediate_points = lhs_output
        .intermediate_points
        .saturating_add(lhs_output.points)
        .saturating_add(rhs_output.intermediate_points)
        .saturating_add(rhs_output.points);
    enforce_intermediate_work(intermediate_points, limits)?;
    let frame_bytes = lhs_output
        .frame_bytes
        .saturating_add(rhs_output.frame_bytes);
    let lhs =
        decode_prometheus_intermediate(&lhs_output.body, lhs_type, instant, limits, cancelled)?;
    let rhs =
        decode_prometheus_intermediate(&rhs_output.body, rhs_type, instant, limits, cancelled)?;
    let series = match (&lhs, &rhs) {
        (IntermediateValue::Vector(series), IntermediateValue::Scalar(_))
        | (IntermediateValue::Scalar(_), IntermediateValue::Vector(series)) => series
            .iter()
            .map(|series| IntermediateSeries {
                labels: series.labels.clone(),
                points: Vec::new(),
            })
            .collect(),
        (IntermediateValue::Vector(lhs), IntermediateValue::Vector(rhs)) => {
            vector_comparison_candidates(comparison, lhs, rhs, cancelled)?
        }
        _ => Vec::new(),
    };
    let work_points = series
        .iter()
        .map(|series| series.points.len().max(1) as u64)
        .sum();
    let value = apply_prometheus_binary(
        comparison.op,
        comparison.return_bool,
        &comparison.matching,
        &comparison.cardinality,
        lhs,
        rhs,
        false,
        cancelled,
    )?;
    let output = encode_prometheus_intermediate(
        value,
        instant,
        frame_bytes,
        intermediate_points,
        limits,
        cancelled,
    )?;
    Ok(Some((
        output,
        CandidateSeries {
            series,
            work_points,
        },
    )))
}

fn vector_comparison_candidates(
    comparison: &PromBinaryPlan,
    lhs: &[IntermediateSeries],
    rhs: &[IntermediateSeries],
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    let lhs_keys: Vec<_> = lhs
        .iter()
        .map(|series| comparison.matching.key(&series.labels, false))
        .collect();
    let rhs_keys: Vec<_> = rhs
        .iter()
        .map(|series| comparison.matching.key(&series.labels, false))
        .collect();
    let mut lhs_groups: BTreeMap<&PromMatchingKey, Vec<usize>> = BTreeMap::new();
    for (index, key) in lhs_keys.iter().enumerate() {
        check_cancelled(cancelled)?;
        lhs_groups.entry(key).or_default().push(index);
    }
    let mut rhs_groups: BTreeMap<&PromMatchingKey, Vec<usize>> = BTreeMap::new();
    for (index, key) in rhs_keys.iter().enumerate() {
        check_cancelled(cancelled)?;
        rhs_groups.entry(key).or_default().push(index);
    }
    let mut output = BTreeMap::new();
    for (key, lhs_group) in lhs_groups {
        check_cancelled(cancelled)?;
        let Some(rhs_group) = rhs_groups.get(key) else {
            continue;
        };
        let matches: Vec<(usize, usize)> = match &comparison.cardinality {
            PromVectorCardinality::OneToOne => {
                if lhs_group.len() != 1 || rhs_group.len() != 1 {
                    return Err(duplicate_matching_error(key));
                }
                vec![(lhs_group[0], rhs_group[0])]
            }
            PromVectorCardinality::ManyToOne(_) => {
                if rhs_group.len() != 1 {
                    return Err(duplicate_matching_error(key));
                }
                lhs_group
                    .iter()
                    .map(|lhs_index| (*lhs_index, rhs_group[0]))
                    .collect()
            }
            PromVectorCardinality::OneToMany(_) => {
                if lhs_group.len() != 1 {
                    return Err(duplicate_matching_error(key));
                }
                rhs_group
                    .iter()
                    .map(|rhs_index| (lhs_group[0], *rhs_index))
                    .collect()
            }
            PromVectorCardinality::ManyToMany => {
                return Err("comparison operators do not allow many-to-many matching".into())
            }
        };
        for (lhs_index, rhs_index) in matches {
            let (base_labels, one_labels) = match comparison.cardinality {
                PromVectorCardinality::OneToMany(_) => {
                    (&rhs[rhs_index].labels, &lhs[lhs_index].labels)
                }
                _ => (&lhs[lhs_index].labels, &rhs[rhs_index].labels),
            };
            let labels = vector_result_labels(
                base_labels,
                one_labels,
                comparison.op,
                false,
                &comparison.matching,
                &comparison.cardinality,
                false,
            );
            output.entry(labels).or_insert_with(Vec::new);
        }
    }
    Ok(output
        .into_iter()
        .map(|(labels, points)| IntermediateSeries { labels, points })
        .collect())
}

fn merge_comparison_candidates(
    filtered: Vec<IntermediateSeries>,
    candidates: Vec<IntermediateSeries>,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    let mut filtered: BTreeMap<_, _> = filtered
        .into_iter()
        .map(|series| (series.labels.clone(), series.points))
        .collect();
    let mut output = Vec::with_capacity(candidates.len().saturating_add(filtered.len()));
    for mut candidate in candidates {
        check_cancelled(cancelled)?;
        candidate.points = filtered.remove(&candidate.labels).unwrap_or_default();
        output.push(candidate);
    }
    output.extend(
        filtered
            .into_iter()
            .map(|(labels, points)| IntermediateSeries { labels, points }),
    );
    Ok(output)
}

fn into_series(value: IntermediateValue) -> Vec<IntermediateSeries> {
    match value {
        IntermediateValue::Scalar(points) => vec![IntermediateSeries {
            labels: BTreeMap::new(),
            points,
        }],
        IntermediateValue::Vector(series) => series,
    }
}

fn apply_binary(
    binary: &BinaryPlan,
    lhs: Vec<IntermediateSeries>,
    rhs: Vec<IntermediateSeries>,
    keep_metric_names: bool,
    cancelled: &AtomicBool,
) -> Result<Vec<IntermediateSeries>, String> {
    if binary.op == BinaryOp::Default && lhs.is_empty() {
        return normalize_prometheus_vector(strip_nan_points(rhs), cancelled);
    }

    let scalar_rhs = (rhs.len() == 1 && rhs[0].labels.is_empty()).then(|| {
        rhs[0]
            .points
            .iter()
            .filter(|(_, value)| !value.is_nan())
            .copied()
            .collect::<BTreeMap<_, _>>()
    });
    let mut right_by_key: BTreeMap<PromMatchingKey, BTreeMap<i64, f64>> = BTreeMap::new();
    for series in &rhs {
        check_cancelled(cancelled)?;
        let key = metricsql_matching_key(&binary.matching, &series.labels, keep_metric_names);
        let values = right_by_key.entry(key).or_default();
        for (timestamp, value) in &series.points {
            if !value.is_nan() {
                values.entry(*timestamp).or_insert(*value);
            }
        }
    }

    let mut output = Vec::with_capacity(lhs.len());
    for mut series in lhs {
        check_cancelled(cancelled)?;
        let key = metricsql_matching_key(&binary.matching, &series.labels, keep_metric_names);
        let right = right_by_key.get(&key).or(scalar_rhs.as_ref());
        let mut points: BTreeMap<i64, f64> = series
            .points
            .drain(..)
            .filter(|(_, value)| !value.is_nan())
            .collect();
        match binary.op {
            BinaryOp::Default => {
                if let Some(right) = right {
                    for (timestamp, value) in right {
                        points.entry(*timestamp).or_insert(*value);
                    }
                }
            }
            BinaryOp::If => {
                let Some(right) = right else {
                    continue;
                };
                points.retain(|timestamp, _| right.contains_key(timestamp));
            }
            BinaryOp::IfNot => {
                if let Some(right) = right {
                    points.retain(|timestamp, _| !right.contains_key(timestamp));
                }
            }
        }
        if !points.is_empty() {
            series.points = points.into_iter().collect();
            output.push(series);
        }
    }
    normalize_prometheus_vector(output, cancelled)
}

fn metricsql_matching_key(
    matching: &PromVectorMatching,
    labels: &BTreeMap<String, String>,
    keep_metric_names: bool,
) -> PromMatchingKey {
    matching.key(labels, keep_metric_names)
}

fn strip_nan_points(mut series: Vec<IntermediateSeries>) -> Vec<IntermediateSeries> {
    for item in &mut series {
        item.points.retain(|(_, value)| !value.is_nan());
    }
    series
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_binary_precedence_and_matching_are_separate_from_promql() {
        let plan = lower("(cpu > 2) default on(host) sparse + 1", 300_000, 10_000)
            .expect("MetricsQL plan");
        let PromPlan::MetricsBinary(binary) = plan else {
            panic!("MetricsQL binary plan expected")
        };
        assert_eq!(binary.op, BinaryOp::Default);
        assert!(matches!(binary.matching, PromVectorMatching::On(_)));
        assert!(matches!(*binary.lhs, PromPlan::Binary(_)));
        assert!(matches!(*binary.rhs, PromPlan::Binary(_)));
    }

    #[test]
    fn custom_binary_can_be_nested_in_standard_promql() {
        let plan =
            lower("sum((cpu > 2) default 0)", 300_000, 10_000).expect("nested MetricsQL plan");
        let PromPlan::Aggregate(aggregate) = plan else {
            panic!("aggregate expected")
        };
        assert!(matches!(*aggregate.inner, PromPlan::MetricsBinary(_)));
    }

    #[test]
    fn keywords_in_labels_strings_and_metric_names_are_not_operators() {
        assert!(matches!(
            lower(r#"default_metric{word="ifnot default"}"#, 300_000, 10_000).unwrap(),
            PromPlan::MetricsDynamicRollup(DynamicRollupPlan {
                op: DynamicRollupOp::Default,
                ..
            })
        ));
        assert!(lower("if + 1", 300_000, 10_000).is_ok());
    }

    #[test]
    fn keep_metric_names_wraps_functions_and_binary_operators() {
        let transform = lower("abs(cpu) keep_metric_names", 300_000, 10_000).unwrap();
        assert!(matches!(
            transform,
            PromPlan::KeepMetricNames(inner) if matches!(*inner, PromPlan::Function(_))
        ));

        let binary = lower("(cpu / 10) keep_metric_names", 300_000, 10_000).unwrap();
        assert!(matches!(
            binary,
            PromPlan::KeepMetricNames(inner) if matches!(*inner, PromPlan::Binary(_))
        ));

        let nested = lower(
            "sum(abs({__name__=~\"cpu|memory\"}) keep_metric_names)",
            300_000,
            10_000,
        )
        .unwrap();
        let PromPlan::Aggregate(aggregate) = nested else {
            panic!("aggregate expected")
        };
        assert!(matches!(*aggregate.inner, PromPlan::KeepMetricNames(_)));
    }

    #[test]
    fn keep_metric_names_rejects_non_function_targets_and_repetition() {
        for query in [
            "cpu keep_metric_names",
            "sum(cpu) keep_metric_names",
            "-cpu keep_metric_names",
            "abs(cpu) keep_metric_names keep_metric_names",
        ] {
            let error = lower(query, 300_000, 10_000).unwrap_err();
            assert!(
                error.contains("function or binary operator"),
                "{query}: {error}"
            );
        }
        assert!(matches!(
            lower(
                r#"abs(keep_metric_names_total{note="keep_metric_names"})"#,
                300_000,
                10_000,
            )
            .unwrap(),
            PromPlan::Function(_)
        ));
    }

    #[test]
    fn union_named_shorthand_nested_and_trailing_forms_lower_separately() {
        for query in [
            "union(cpu, memory)",
            "(cpu, memory)",
            "union(cpu, memory,)",
            "(cpu, memory,)",
            "UNION(cpu, memory)",
        ] {
            assert!(matches!(
                lower(query, 300_000, 10_000).unwrap(),
                PromPlan::MetricsUnion(UnionPlan { inputs }) if inputs.len() == 2
            ));
        }
        assert!(matches!(
            lower("union()", 300_000, 10_000).unwrap(),
            PromPlan::MetricsUnion(UnionPlan { inputs }) if inputs.is_empty()
        ));
        assert!(matches!(
            lower("union(cpu)", 300_000, 10_000).unwrap(),
            PromPlan::MetricsUnion(UnionPlan { inputs }) if inputs.len() == 1
        ));
        assert!(matches!(
            lower("(cpu,)", 300_000, 10_000).unwrap(),
            PromPlan::MetricsDynamicRollup(DynamicRollupPlan {
                op: DynamicRollupOp::Default,
                ..
            })
        ));
        let nested = lower(
            "sum(union(alias(cpu, \"renamed_cpu\"), alias(memory, \"renamed_memory\")))",
            300_000,
            10_000,
        )
        .unwrap();
        let PromPlan::Aggregate(aggregate) = nested else {
            panic!("aggregate expected")
        };
        let PromPlan::MetricsUnion(UnionPlan { inputs }) = *aggregate.inner else {
            panic!("nested union expected")
        };
        assert!(inputs
            .iter()
            .all(|input| matches!(input, PromPlan::MetricsAlias(_))));
    }

    #[test]
    fn alias_lowers_name_and_rejects_invalid_arguments() {
        let plan = lower("alias(cpu, \"renamed\")", 300_000, 10_000).unwrap();
        assert!(matches!(
            plan,
            PromPlan::MetricsAlias(AliasPlan { name, .. }) if name == "renamed"
        ));
        assert!(matches!(
            lower("alias(cpu, \"renamed\",)", 300_000, 10_000).unwrap(),
            PromPlan::MetricsAlias(AliasPlan { name, .. }) if name == "renamed"
        ));
        for query in [
            "alias(cpu)",
            "alias(cpu, 1)",
            "alias(cpu, \"a\", \"b\")",
            "(cpu,,memory)",
            "ALIAS(cpu, \"renamed\")",
        ] {
            assert!(lower(query, 300_000, 10_000).is_err(), "{query}");
        }
    }

    #[test]
    fn label_manipulation_grammar_matches_metricsql_transform_rules() {
        for query in [
            "label_set(cpu, \"environment\", \"production\", \"host\", \"rewritten\")",
            "label_set(cpu)",
            "label_set(1, \"kind\", \"scalar\")",
            "label_set(cpu, \"host\", \"rewritten\",)",
            "label_del(cpu, \"host\", \"missing\")",
            "label_del(cpu)",
            "LABEL_DEL(LABEL_SET(cpu, \"host\", \"rewritten\"), \"job\")",
            "label_del(label_set(cpu, \"host\", \"rewritten\"), \"job\") keep_metric_names",
            "sum(label_del(label_set(cpu, \"host\", \"rewritten\"), \"job\"))",
        ] {
            assert!(lower(query, 300_000, 10_000).is_ok(), "{query}");
        }
        for query in [
            "label_set(cpu, \"host\")",
            "label_set(cpu, \"host\", 1)",
            "label_del(cpu, 1)",
            "label_set()",
            "label_del()",
        ] {
            assert!(lower(query, 300_000, 10_000).is_err(), "{query}");
        }
    }

    #[test]
    fn default_rollup_and_windowless_rollups_match_metricsql_grammar() {
        for query in [
            "default_rollup(cpu)",
            "default_rollup(cpu[2s])",
            "default_rollup(1)",
            "DEFAULT_ROLLUP(cpu,)",
            "avg_over_time(cpu)",
            "min_over_time(cpu)",
            "max_over_time(cpu)",
            "sum_over_time(cpu)",
            "count_over_time(cpu)",
            "present_over_time(cpu)",
            "stddev_over_time(cpu)",
            "stdvar_over_time(cpu)",
            "FIRST_OVER_TIME(cpu,)",
            "last_over_time(cpu)",
            "rate(cpu)",
            "irate(cpu)",
            "increase(cpu)",
            "delta(cpu)",
            "idelta(cpu)",
            "deriv(cpu)",
            "changes(cpu)",
            "resets(cpu)",
            "timestamp(cpu)",
            "sum(default_rollup(cpu))",
        ] {
            assert!(lower(query, 300_000, 10_000).is_ok(), "{query}");
        }
        for query in [
            "default_rollup()",
            "default_rollup(cpu, 1)",
            "first_over_time()",
            "first_over_time(cpu, 1)",
            "avg_over_time()",
            "avg_over_time(cpu, 1)",
        ] {
            assert!(lower(query, 300_000, 10_000).is_err(), "{query}");
        }
    }
}
