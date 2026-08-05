//! MetricsQL-only syntax and evaluation.
//!
//! The stable PromQL parser remains unchanged. MetricsQL expressions are
//! accepted only by the explicitly named MetricsQL routes, lowered into the
//! same bounded Rust plan tree, and never sent into SQLite as query syntax.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicBool;

use rusqlite::Connection;

use super::*;

const MAX_METRICSQL_DEPTH: usize = 64;
const PLACEHOLDER_PREFIX: &str = "__timeless_metricsql_expr_";

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

struct LowerContext<'a> {
    original: &'a str,
    lookback: i64,
    #[allow(dead_code)]
    step: i64,
    next_placeholder: usize,
    placeholders: HashMap<String, PromPlan>,
}

pub(super) fn lower(input: &str, lookback: i64, step: i64) -> Result<PromPlan, String> {
    let mut context = LowerContext {
        original: input,
        lookback,
        step,
        next_placeholder: 0,
        placeholders: HashMap::new(),
    };
    let mut plan = lower_expr(input, &mut context, 0)?;
    replace_placeholders(&mut plan, &mut context.placeholders)?;
    if !context.placeholders.is_empty() {
        return Err("internal MetricsQL placeholder was not consumed".into());
    }
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

    let rewritten = rewrite_nested(input, context, depth + 1)?;
    let mut plan = lower_promql(&rewritten, context.lookback)?;
    replace_placeholders(&mut plan, &mut context.placeholders)?;
    Ok(plan)
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
                if contains_custom_word(inner) {
                    let identifier = previous_identifier(input, index)
                        .map(|(_, value)| value.to_ascii_lowercase());
                    let modifier = identifier.as_deref().is_some_and(|name| {
                        matches!(
                            name,
                            "on" | "ignoring" | "group_left" | "group_right" | "by" | "without"
                        )
                    });
                    output.push_str(&input[copied..index]);
                    if modifier {
                        output.push('(');
                        output.push_str(inner);
                        output.push(')');
                    } else if identifier.is_some() {
                        output.push('(');
                        for (argument_index, argument) in split_arguments(inner)?.iter().enumerate()
                        {
                            if argument_index > 0 {
                                output.push(',');
                            }
                            if contains_custom_word(argument) {
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
                if BinaryOp::parse(&input[start..index].to_ascii_lowercase()).is_some() {
                    return true;
                }
            }
            _ => index += 1,
        }
    }
    false
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
        PromPlan::Unary(inner) => replace_placeholders(inner, placeholders)?,
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
    let output = apply_binary(binary, lhs, rhs, cancelled)?;
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
        .map(|series| comparison.matching.key(&series.labels))
        .collect();
    let rhs_keys: Vec<_> = rhs
        .iter()
        .map(|series| comparison.matching.key(&series.labels))
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
        let key = metricsql_matching_key(&binary.matching, &series.labels);
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
        let key = metricsql_matching_key(&binary.matching, &series.labels);
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
) -> PromMatchingKey {
    let mut labels = labels.clone();
    labels.remove("__name__");
    matching.key(&labels)
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
            PromPlan::Selector { .. }
        ));
        assert!(lower("if + 1", 300_000, 10_000).is_ok());
    }
}
