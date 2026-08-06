//! Strict planning for the LogsQL surface owned by the Rust logs API.
//!
//! Language syntax stays out of the SQLite extension.  This module turns a
//! supported query into the public [`QuerySpec`] storage contract and never
//! silently drops a term or pipe it does not understand.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;
use std::net::IpAddr;

use chrono::{DateTime, Utc};
use regex::RegexBuilder;

use serde_json::Value;

use crate::{
    FieldCompareOp, LogField, LogPredicate, MetadataExact, NumericOp, PatternMatchMode,
    PatternMatcher, QuerySpec, TimestampUnit, ValueTypeKind,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogsqlOutput {
    Rows,
    Count,
    /// Ordered API-owned transforms over bounded rows from the public logs
    /// virtual table. LogsQL syntax and state never enter the extension.
    Pipeline,
}

#[derive(Clone, Debug)]
pub struct LogsqlPlan {
    pub spec: QuerySpec,
    pub output: LogsqlOutput,
    /// Distinguishes an explicit `limit`/`head` from the API default so a
    /// tighter server policy can lower only the default without rewriting a
    /// caller's request.
    pub limit_explicit: bool,
    pub(crate) pipeline: Vec<PipelineOp>,
    pub(crate) implicit_result_limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PipelineField {
    Exact { path: Vec<String>, name: String },
    Prefix { prefix: String },
    All,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StatsKind {
    Count,
    CountEmpty,
    CountUniq,
    CountUniqHash,
    UniqValues,
    Values,
    Sum,
    Avg,
    Min,
    Max,
    Median,
    Rate,
    RateSum,
}

fn parse_field_values_pipe(segment: &str) -> Result<PipelineOp, LogsqlError> {
    let rest = segment
        .strip_prefix("field_values")
        .expect("caller checked field_values prefix")
        .trim();
    let words = pipeline_words(rest)?;
    let Some(field) = words.first() else {
        return Err(LogsqlError::malformed(
            "LogsQL field_values requires a field name",
        ));
    };
    let field = parse_pipeline_field(field, false)?;
    let mut filter = None;
    let mut limit = None;
    let mut index = 1usize;
    while index < words.len() {
        match words[index].as_str() {
            "filter" if filter.is_none() => {
                let value = words.get(index + 1).ok_or_else(|| {
                    LogsqlError::malformed("LogsQL field_values filter requires a value")
                })?;
                filter = Some(quoted_value(value)?.unwrap_or_else(|| value.clone()));
                index += 2;
            }
            "limit" if limit.is_none() => {
                let value = words.get(index + 1).ok_or_else(|| {
                    LogsqlError::malformed("LogsQL field_values limit requires a value")
                })?;
                limit = Some(parse_pipeline_usize("field_values limit", value)?);
                index += 2;
            }
            token => {
                return Err(LogsqlError::malformed(format!(
                    "unexpected LogsQL field_values token {token:?}"
                )))
            }
        }
    }
    Ok(PipelineOp::FieldValues {
        field,
        filter,
        limit,
    })
}

fn parse_field_names_pipe(segment: &str) -> Result<PipelineOp, LogsqlError> {
    let rest = segment
        .strip_prefix("field_names")
        .expect("caller checked field_names prefix")
        .trim();
    let words = pipeline_words(rest)?;
    let mut filter = None;
    let mut result_name = "name".to_owned();
    let mut index = 0usize;
    if words.get(index).is_some_and(|word| word == "filter") {
        let value = words
            .get(index + 1)
            .ok_or_else(|| LogsqlError::malformed("LogsQL field_names filter requires a value"))?;
        filter = Some(quoted_value(value)?.unwrap_or_else(|| value.clone()));
        index += 2;
    }
    if words.get(index).is_some_and(|word| word == "as") {
        index += 1;
    }
    if let Some(alias) = words.get(index) {
        result_name = pipeline_field_name(&parse_pipeline_field(alias, false)?)?;
        index += 1;
    }
    if index != words.len() {
        return Err(LogsqlError::malformed(format!(
            "unexpected LogsQL field_names token {:?}",
            words[index]
        )));
    }
    Ok(PipelineOp::FieldNames {
        filter,
        result_name,
    })
}

fn parse_project_pipe(segment: &str) -> Result<PipelineOp, LogsqlError> {
    let rest = segment
        .strip_prefix("fields")
        .or_else(|| segment.strip_prefix("keep"))
        .expect("caller checked projection prefix")
        .trim();
    if rest.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL fields/keep requires at least one field",
        ));
    }
    let fields = split_top_level(rest, ',')?
        .into_iter()
        .map(|field| parse_pipeline_field(field.trim(), true))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PipelineOp::Project(fields))
}

fn is_delete_pipe(segment: &str) -> bool {
    segment
        .split_whitespace()
        .next()
        .is_some_and(is_delete_alias)
}

fn is_delete_alias(command: &str) -> bool {
    ["delete", "del", "drop", "rm"]
        .iter()
        .any(|alias| command.eq_ignore_ascii_case(alias))
}

fn parse_delete_pipe(segment: &str) -> Result<PipelineOp, LogsqlError> {
    let command_end = segment.find(char::is_whitespace).unwrap_or(segment.len());
    let command = &segment[..command_end];
    if !is_delete_alias(command) {
        return Err(LogsqlError::malformed(format!(
            "unsupported LogsQL delete alias {command:?}"
        )));
    }
    let fields = segment[command_end..].trim();
    if fields.is_empty() {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {command} requires at least one field"
        )));
    }
    let fields = split_top_level(fields, ',')?
        .into_iter()
        .map(|field| parse_delete_field(field.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PipelineOp::Delete(fields))
}

fn parse_delete_field(value: &str) -> Result<PipelineField, LogsqlError> {
    if value == "*" {
        return Ok(PipelineField::All);
    }
    if matches!(value.chars().next(), Some('"' | '\'' | '`')) {
        let (field, consumed) = parse_quoted_prefix(value)?.ok_or_else(|| {
            LogsqlError::malformed(format!("invalid quoted LogsQL delete field {value:?}"))
        })?;
        let field = if field.is_empty() {
            "_msg".to_owned()
        } else {
            field
        };
        return match &value[consumed..] {
            "" => Ok(PipelineField::Exact {
                path: vec![field.clone()],
                name: field,
            }),
            "*" => Ok(PipelineField::Prefix { prefix: field }),
            _ => Err(LogsqlError::malformed(format!(
                "unexpected text after quoted LogsQL delete field {value:?}"
            ))),
        };
    }
    if let Some(prefix) = value.strip_suffix('*') {
        if prefix.is_empty() || prefix.contains('*') || prefix.chars().any(char::is_whitespace) {
            return Err(LogsqlError::malformed(format!(
                "invalid LogsQL delete field prefix {value:?}"
            )));
        }
        return Ok(PipelineField::Prefix {
            prefix: prefix.to_owned(),
        });
    }
    parse_pipeline_field(value, false)
}

fn parse_filter_pipe(
    segment: &str,
    timestamp_unit: TimestampUnit,
    query_now: i64,
) -> Result<PipelineOp, LogsqlError> {
    let expression = segment
        .strip_prefix("filter")
        .or_else(|| segment.strip_prefix("where"))
        .expect("caller checked filter prefix")
        .trim();
    if expression.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL filter/where requires an expression",
        ));
    }
    let tokens = lex_logical_tokens(expression)?;
    let expression = LogicalParser::new(tokens).parse()?;
    Ok(PipelineOp::Filter(compile_logical_expression(
        &expression,
        None,
        timestamp_unit,
        query_now,
    )?))
}

fn parse_stats_pipe(segment: &str) -> Result<PipelineOp, LogsqlError> {
    let rest = segment
        .strip_prefix("stats")
        .expect("caller checked stats prefix")
        .trim();
    if rest.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL stats requires at least one function",
        ));
    }
    let expressions = split_top_level(rest, ',')?
        .into_iter()
        .map(|expression| parse_stats_expression(expression.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut aliases = std::collections::BTreeSet::new();
    for expression in &expressions {
        if !aliases.insert(expression.alias.clone()) {
            return Err(LogsqlError::malformed(format!(
                "duplicate LogsQL stats result name {:?}",
                expression.alias
            )));
        }
    }
    Ok(PipelineOp::Stats(expressions))
}

fn parse_stats_expression(expression: &str) -> Result<StatsExpression, LogsqlError> {
    let open = expression.find('(').ok_or_else(|| {
        LogsqlError::malformed(format!(
            "LogsQL stats function requires parentheses: {expression:?}"
        ))
    })?;
    let close = matching_parenthesis(expression, open)?;
    let function = expression[..open].trim();
    if function.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL stats function name is empty",
        ));
    }
    let args = &expression[open + 1..close];
    let kind = match function {
        "count" => StatsKind::Count,
        "count_empty" => StatsKind::CountEmpty,
        "count_uniq" => StatsKind::CountUniq,
        "count_uniq_hash" => StatsKind::CountUniqHash,
        "uniq_values" => StatsKind::UniqValues,
        "values" => StatsKind::Values,
        "sum" => StatsKind::Sum,
        "avg" => StatsKind::Avg,
        "min" => StatsKind::Min,
        "max" => StatsKind::Max,
        "median" => StatsKind::Median,
        "rate" => StatsKind::Rate,
        "rate_sum" => StatsKind::RateSum,
        _ => {
            return Err(LogsqlError::unsupported(format!(
                "unsupported LogsQL stats function {function:?}"
            )))
        }
    };
    let mut fields = if args.trim().is_empty() {
        Vec::new()
    } else {
        split_top_level(args, ',')?
            .into_iter()
            .map(|field| parse_pipeline_field(field.trim(), true))
            .collect::<Result<Vec<_>, _>>()?
    };
    match kind {
        StatsKind::Rate if !fields.is_empty() => {
            return Err(LogsqlError::malformed(
                "LogsQL rate() does not accept fields",
            ))
        }
        StatsKind::CountUniq | StatsKind::CountUniqHash if fields.is_empty() => {
            return Err(LogsqlError::malformed(format!(
                "LogsQL {function} requires at least one field"
            )))
        }
        StatsKind::CountUniq | StatsKind::CountUniqHash
            if fields
                .iter()
                .any(|field| !matches!(field, PipelineField::Exact { .. })) =>
        {
            return Err(LogsqlError::malformed(format!(
                "LogsQL {function} requires exact field names"
            )))
        }
        StatsKind::Rate => {}
        _ if fields.is_empty() => fields.push(PipelineField::All),
        _ => {}
    }

    let canonical = format!("{function}({})", args.trim());
    let words = pipeline_words(expression[close + 1..].trim())?;
    let mut limit = None;
    let mut alias = None;
    let mut index = 0usize;
    while index < words.len() {
        match words[index].as_str() {
            "limit" if limit.is_none() => {
                let value = words.get(index + 1).ok_or_else(|| {
                    LogsqlError::malformed(format!("LogsQL {function} limit requires a value"))
                })?;
                limit = Some(parse_pipeline_usize(function, value)?);
                index += 2;
            }
            "as" if alias.is_none() => {
                let value = words.get(index + 1).ok_or_else(|| {
                    LogsqlError::malformed(format!("LogsQL {function} alias requires a value"))
                })?;
                alias = Some(pipeline_field_name(&parse_pipeline_field(value, false)?)?);
                index += 2;
            }
            token => {
                return Err(LogsqlError::malformed(format!(
                    "unexpected LogsQL {function} token {token:?}"
                )))
            }
        }
    }
    if limit.is_some()
        && !matches!(
            kind,
            StatsKind::CountUniq
                | StatsKind::CountUniqHash
                | StatsKind::UniqValues
                | StatsKind::Values
        )
    {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {function} does not accept limit"
        )));
    }
    Ok(StatsExpression {
        kind,
        fields,
        alias: alias.unwrap_or(canonical),
        limit,
    })
}

fn parse_first_pipe(segment: &str) -> Result<PipelineOp, LogsqlError> {
    parse_first_last_spec(segment, "first").map(PipelineOp::First)
}

fn parse_last_pipe(segment: &str) -> Result<PipelineOp, LogsqlError> {
    parse_first_last_spec(segment, "last").map(PipelineOp::Last)
}

fn parse_top_pipe(segment: &str) -> Result<PipelineOp, LogsqlError> {
    let tokens = lex_first_pipe(segment, "top")?;
    let Some(command) = tokens.first() else {
        return Err(LogsqlError::malformed("LogsQL top pipe is empty"));
    };
    if !command.eq_ignore_ascii_case("top") {
        return Err(LogsqlError::malformed(format!(
            "expected LogsQL top pipe, not {command:?}"
        )));
    }

    let mut cursor = 1usize;
    let mut limit = 10usize;
    if tokens.get(cursor).is_some_and(|token| {
        token
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_digit() || matches!(character, '+' | '-'))
    }) {
        let value = &tokens[cursor];
        limit = value.parse::<usize>().map_err(|_| {
            LogsqlError::malformed(format!(
                "LogsQL top requires a positive integer limit, not {value:?}"
            ))
        })?;
        if limit == 0 {
            return Err(LogsqlError::malformed(
                "LogsQL top limit must be greater than zero",
            ));
        }
        cursor += 1;
    }

    if tokens
        .get(cursor)
        .is_some_and(|token| token.eq_ignore_ascii_case("by"))
    {
        cursor += 1;
    }
    let by_fields = parse_top_fields(&tokens, &mut cursor)?;
    if by_fields.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL top requires at least one exact field",
        ));
    }

    let mut hits_field = "hits".to_owned();
    let mut rank_field = None;
    while let Some(token) = tokens.get(cursor) {
        if token.eq_ignore_ascii_case("hits") {
            cursor += 1;
            if tokens
                .get(cursor)
                .is_some_and(|token| token.eq_ignore_ascii_case("as"))
            {
                cursor += 1;
            }
            let value = tokens.get(cursor).ok_or_else(|| {
                LogsqlError::malformed("LogsQL top hits requires a result field name")
            })?;
            if matches!(value.as_str(), "," | "(" | ")") {
                return Err(LogsqlError::malformed(
                    "LogsQL top hits requires a result field name",
                ));
            }
            hits_field = pipeline_field_name(&parse_first_exact_field(value, "hits", "top")?)?;
            cursor += 1;
            continue;
        }
        if token.eq_ignore_ascii_case("rank") {
            cursor += 1;
            let explicit_as = tokens
                .get(cursor)
                .is_some_and(|token| token.eq_ignore_ascii_case("as"));
            if explicit_as {
                cursor += 1;
            }
            let name = match tokens.get(cursor) {
                Some(value) if !matches!(value.as_str(), "," | "(" | ")") => {
                    cursor += 1;
                    pipeline_field_name(&parse_first_exact_field(value, "rank", "top")?)?
                }
                _ if explicit_as => {
                    return Err(LogsqlError::malformed(
                        "LogsQL top rank as requires a result field name",
                    ))
                }
                _ => "rank".to_owned(),
            };
            rank_field = Some(name);
            continue;
        }
        return Err(LogsqlError::malformed(format!(
            "unexpected LogsQL top token {token:?}"
        )));
    }

    hits_field = unique_top_result_name(hits_field, &by_fields);
    rank_field = rank_field.map(|name| unique_top_result_name(name, &by_fields));
    Ok(PipelineOp::Top(TopSpec {
        limit,
        by_fields,
        hits_field,
        rank_field,
    }))
}

fn parse_uniq_pipe(segment: &str) -> Result<PipelineOp, LogsqlError> {
    let tokens = lex_first_pipe(segment, "uniq")?;
    let Some(command) = tokens.first() else {
        return Err(LogsqlError::malformed("LogsQL uniq pipe is empty"));
    };
    if !command.eq_ignore_ascii_case("uniq") {
        return Err(LogsqlError::malformed(format!(
            "expected LogsQL uniq pipe, not {command:?}"
        )));
    }

    let mut cursor = 1usize;
    let explicit_by = tokens
        .get(cursor)
        .is_some_and(|token| token.eq_ignore_ascii_case("by"));
    if explicit_by {
        cursor += 1;
    }
    let by_fields = parse_uniq_fields(&tokens, &mut cursor)?;
    if by_fields.is_empty() {
        return Err(LogsqlError::malformed(if explicit_by {
            "LogsQL uniq requires fields after by"
        } else {
            "LogsQL uniq requires at least one exact field"
        }));
    }

    let mut filter = None;
    if tokens
        .get(cursor)
        .is_some_and(|token| token.eq_ignore_ascii_case("filter"))
    {
        cursor += 1;
        let value = tokens
            .get(cursor)
            .ok_or_else(|| LogsqlError::malformed("LogsQL uniq filter requires a substring"))?;
        if matches!(value.as_str(), "," | "(" | ")") {
            return Err(LogsqlError::malformed(
                "LogsQL uniq filter requires a substring",
            ));
        }
        let value = quoted_value(value)?.unwrap_or_else(|| value.clone());
        if by_fields.len() != 1 && !value.is_empty() {
            return Err(LogsqlError::malformed(
                "LogsQL uniq filter applies only to a single field",
            ));
        }
        filter = Some(value);
        cursor += 1;
    }

    if tokens
        .get(cursor)
        .is_some_and(|token| token.eq_ignore_ascii_case("with"))
    {
        cursor += 1;
        if tokens
            .get(cursor)
            .is_none_or(|token| !token.eq_ignore_ascii_case("hits"))
        {
            return Err(LogsqlError::malformed("LogsQL uniq with requires hits"));
        }
    }
    let hits_field = if tokens
        .get(cursor)
        .is_some_and(|token| token.eq_ignore_ascii_case("hits"))
    {
        cursor += 1;
        Some(unique_top_result_name("hits".into(), &by_fields))
    } else {
        None
    };

    let mut limit = None;
    if tokens
        .get(cursor)
        .is_some_and(|token| token.eq_ignore_ascii_case("limit"))
    {
        cursor += 1;
        let value = tokens
            .get(cursor)
            .ok_or_else(|| LogsqlError::malformed("LogsQL uniq limit requires a value"))?;
        limit = Some(parse_pipeline_usize("uniq limit", value)?);
        cursor += 1;
    }
    if let Some(token) = tokens.get(cursor) {
        return Err(LogsqlError::malformed(format!(
            "unexpected LogsQL uniq token {token:?}"
        )));
    }
    Ok(PipelineOp::Uniq(UniqSpec {
        by_fields,
        filter,
        hits_field,
        limit,
    }))
}

fn parse_uniq_fields(
    tokens: &[String],
    cursor: &mut usize,
) -> Result<Vec<PipelineField>, LogsqlError> {
    if tokens.get(*cursor).is_some_and(|token| token == "(") {
        return parse_first_fields(tokens, cursor, "by", "uniq");
    }
    let mut fields = Vec::new();
    while let Some(token) = tokens.get(*cursor) {
        if token.eq_ignore_ascii_case("filter")
            || token.eq_ignore_ascii_case("with")
            || token.eq_ignore_ascii_case("hits")
            || token.eq_ignore_ascii_case("limit")
        {
            break;
        }
        if matches!(token.as_str(), "," | "(" | ")") {
            return Err(LogsqlError::malformed(
                "LogsQL uniq requires a field after each comma",
            ));
        }
        fields.push(parse_first_exact_field(token, "by", "uniq")?);
        *cursor += 1;
        match tokens.get(*cursor).map(String::as_str) {
            Some(",") => {
                *cursor += 1;
                if tokens.get(*cursor).is_none_or(|token| {
                    token == ","
                        || token.eq_ignore_ascii_case("filter")
                        || token.eq_ignore_ascii_case("with")
                        || token.eq_ignore_ascii_case("hits")
                        || token.eq_ignore_ascii_case("limit")
                }) {
                    return Err(LogsqlError::malformed(
                        "LogsQL uniq requires a field after each comma",
                    ));
                }
            }
            Some(token)
                if token.eq_ignore_ascii_case("filter")
                    || token.eq_ignore_ascii_case("with")
                    || token.eq_ignore_ascii_case("hits")
                    || token.eq_ignore_ascii_case("limit") =>
            {
                break;
            }
            Some(token) => {
                return Err(LogsqlError::malformed(format!(
                    "unexpected LogsQL uniq field token {token:?}; expected ','"
                )))
            }
            None => break,
        }
    }
    Ok(fields)
}

fn parse_top_fields(
    tokens: &[String],
    cursor: &mut usize,
) -> Result<Vec<PipelineField>, LogsqlError> {
    if tokens.get(*cursor).is_some_and(|token| token == "(") {
        return parse_first_fields(tokens, cursor, "by", "top");
    }
    let mut fields = Vec::new();
    while let Some(token) = tokens.get(*cursor) {
        if token.eq_ignore_ascii_case("hits") || token.eq_ignore_ascii_case("rank") {
            break;
        }
        if matches!(token.as_str(), "," | "(" | ")") {
            return Err(LogsqlError::malformed(
                "LogsQL top requires a field after each comma",
            ));
        }
        fields.push(parse_first_exact_field(token, "by", "top")?);
        *cursor += 1;
        match tokens.get(*cursor).map(String::as_str) {
            Some(",") => {
                *cursor += 1;
                if tokens.get(*cursor).is_none_or(|token| {
                    token == ","
                        || token.eq_ignore_ascii_case("hits")
                        || token.eq_ignore_ascii_case("rank")
                }) {
                    return Err(LogsqlError::malformed(
                        "LogsQL top requires a field after each comma",
                    ));
                }
            }
            Some(token)
                if token.eq_ignore_ascii_case("hits") || token.eq_ignore_ascii_case("rank") =>
            {
                break;
            }
            Some(token) => {
                return Err(LogsqlError::malformed(format!(
                    "unexpected LogsQL top field token {token:?}; expected ','"
                )))
            }
            None => break,
        }
    }
    Ok(fields)
}

fn unique_top_result_name(mut name: String, by_fields: &[PipelineField]) -> String {
    while by_fields.iter().any(
        |field| matches!(field, PipelineField::Exact { name: field_name, .. } if field_name == &name),
    ) {
        name.push('s');
    }
    name
}

fn parse_first_last_spec(segment: &str, operation: &str) -> Result<FirstSpec, LogsqlError> {
    let tokens = lex_first_pipe(segment, operation)?;
    let Some(command) = tokens.first() else {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {operation} pipe is empty"
        )));
    };
    if !command.eq_ignore_ascii_case(operation) {
        return Err(LogsqlError::malformed(format!(
            "expected LogsQL {operation} pipe, not {command:?}"
        )));
    }
    let mut cursor = 1usize;
    let mut limit = 1usize;
    if tokens.get(cursor).is_some_and(|token| {
        token != "("
            && !token.eq_ignore_ascii_case("by")
            && !token.eq_ignore_ascii_case("partition")
            && !token.eq_ignore_ascii_case("rank")
    }) {
        let value = &tokens[cursor];
        limit = value.parse::<usize>().map_err(|_| {
            LogsqlError::malformed(format!(
                "LogsQL {operation} requires a positive integer limit, not {value:?}"
            ))
        })?;
        if limit == 0 {
            return Err(LogsqlError::malformed(format!(
                "LogsQL {operation} limit must be greater than zero"
            )));
        }
        cursor += 1;
    }

    let mut by_fields = Vec::new();
    if tokens
        .get(cursor)
        .is_some_and(|token| token.eq_ignore_ascii_case("by"))
    {
        cursor += 1;
        by_fields = parse_first_sort_fields(&tokens, &mut cursor, operation)?;
    } else if tokens.get(cursor).is_some_and(|token| token == "(") {
        by_fields = parse_first_sort_fields(&tokens, &mut cursor, operation)?;
    }

    let mut partition_by = Vec::new();
    if tokens
        .get(cursor)
        .is_some_and(|token| token.eq_ignore_ascii_case("partition"))
    {
        cursor += 1;
        if tokens
            .get(cursor)
            .is_some_and(|token| token.eq_ignore_ascii_case("by"))
        {
            cursor += 1;
        }
        partition_by = parse_first_fields(&tokens, &mut cursor, "partition", operation)?;
    }

    let mut rank_field = None;
    if tokens
        .get(cursor)
        .is_some_and(|token| token.eq_ignore_ascii_case("rank"))
    {
        cursor += 1;
        let explicit_as = tokens
            .get(cursor)
            .is_some_and(|token| token.eq_ignore_ascii_case("as"));
        if explicit_as {
            cursor += 1;
        }
        let field = match tokens.get(cursor) {
            Some(token) if token != "," && token != "(" && token != ")" => {
                cursor += 1;
                parse_first_exact_field(token, "rank", operation)?
            }
            _ if explicit_as => {
                return Err(LogsqlError::malformed(format!(
                    "LogsQL {operation} rank as requires a field name"
                )))
            }
            _ => parse_first_exact_field("rank", "rank", operation)?,
        };
        rank_field = Some(field);
    }

    if let Some(token) = tokens.get(cursor) {
        return Err(LogsqlError::malformed(format!(
            "unexpected LogsQL {operation} token {token:?}"
        )));
    }
    Ok(FirstSpec {
        limit,
        by_fields,
        partition_by,
        rank_field,
    })
}

fn is_first_pipe(segment: &str) -> bool {
    is_first_last_pipe(segment, "first")
}

fn is_last_pipe(segment: &str) -> bool {
    is_first_last_pipe(segment, "last")
}

fn is_top_pipe(segment: &str) -> bool {
    is_first_last_pipe(segment, "top")
}

fn is_uniq_pipe(segment: &str) -> bool {
    is_first_last_pipe(segment, "uniq")
}

fn is_first_last_pipe(segment: &str, operation: &str) -> bool {
    let Some(command) = segment.get(..operation.len()) else {
        return false;
    };
    command.eq_ignore_ascii_case(operation)
        && segment[operation.len()..]
            .chars()
            .next()
            .is_none_or(|character| character.is_whitespace() || character == '(')
}

fn lex_first_pipe(segment: &str, operation: &str) -> Result<Vec<String>, LogsqlError> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in segment.chars() {
        if let Some(delimiter) = quote {
            current.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' && delimiter != '`' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        match character {
            '"' | '\'' | '`' => {
                quote = Some(character);
                current.push(character);
            }
            '(' | ')' | ',' => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(character.to_string());
            }
            _ if character.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(character),
        }
    }
    if quote.is_some() {
        return Err(LogsqlError::malformed(format!(
            "unterminated quoted field in LogsQL {operation} pipe"
        )));
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    Ok(tokens)
}

fn parse_first_sort_fields(
    tokens: &[String],
    cursor: &mut usize,
    operation: &str,
) -> Result<Vec<PipelineSortField>, LogsqlError> {
    if tokens.get(*cursor).is_none_or(|token| token != "(") {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {operation} by requires parenthesized fields"
        )));
    }
    *cursor += 1;
    let mut fields = Vec::new();
    loop {
        match tokens.get(*cursor).map(String::as_str) {
            Some(")") => {
                *cursor += 1;
                return Ok(fields);
            }
            Some(",") | None => {
                return Err(LogsqlError::malformed(format!(
                    "LogsQL {operation} by requires a field after each comma"
                )))
            }
            Some(token) => {
                let field = parse_first_exact_field(token, "sort", operation)?;
                *cursor += 1;
                let descending = match tokens.get(*cursor).map(String::as_str) {
                    Some(direction) if direction.eq_ignore_ascii_case("desc") => {
                        *cursor += 1;
                        true
                    }
                    Some(direction) if direction.eq_ignore_ascii_case("asc") => {
                        *cursor += 1;
                        false
                    }
                    _ => false,
                };
                fields.push(PipelineSortField { field, descending });
                match tokens.get(*cursor).map(String::as_str) {
                    Some(")") => {
                        *cursor += 1;
                        return Ok(fields);
                    }
                    Some(",") => {
                        *cursor += 1;
                        if tokens.get(*cursor).is_some_and(|token| token == ")") {
                            *cursor += 1;
                            return Ok(fields);
                        }
                    }
                    Some(token) => {
                        return Err(LogsqlError::malformed(format!(
                            "unexpected LogsQL {operation} by token {token:?}; expected ',' or ')'"
                        )))
                    }
                    None => {
                        return Err(LogsqlError::malformed(format!(
                            "unterminated LogsQL {operation} by fields"
                        )))
                    }
                }
            }
        }
    }
}

fn parse_first_fields(
    tokens: &[String],
    cursor: &mut usize,
    clause: &str,
    operation: &str,
) -> Result<Vec<PipelineField>, LogsqlError> {
    if tokens.get(*cursor).is_none_or(|token| token != "(") {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {operation} {clause} requires parenthesized fields"
        )));
    }
    *cursor += 1;
    let mut fields = Vec::new();
    loop {
        match tokens.get(*cursor).map(String::as_str) {
            Some(")") => {
                *cursor += 1;
                return Ok(fields);
            }
            Some(",") | None => {
                return Err(LogsqlError::malformed(format!(
                    "LogsQL {operation} {clause} requires a field after each comma"
                )))
            }
            Some(token) => {
                fields.push(parse_first_exact_field(token, clause, operation)?);
                *cursor += 1;
                match tokens.get(*cursor).map(String::as_str) {
                    Some(")") => {
                        *cursor += 1;
                        return Ok(fields);
                    }
                    Some(",") => {
                        *cursor += 1;
                        if tokens.get(*cursor).is_some_and(|token| token == ")") {
                            *cursor += 1;
                            return Ok(fields);
                        }
                    }
                    Some(token) => {
                        return Err(LogsqlError::malformed(format!(
                            "unexpected LogsQL {operation} {clause} token {token:?}; expected ',' or ')'"
                        )))
                    }
                    None => {
                        return Err(LogsqlError::malformed(format!(
                            "unterminated LogsQL {operation} {clause} fields"
                        )))
                    }
                }
            }
        }
    }
}

fn parse_first_exact_field(
    value: &str,
    clause: &str,
    operation: &str,
) -> Result<PipelineField, LogsqlError> {
    match parse_pipeline_field(value, false)? {
        field @ PipelineField::Exact { .. } => Ok(field),
        PipelineField::Prefix { .. } | PipelineField::All => Err(LogsqlError::malformed(format!(
            "LogsQL {operation} {clause} requires an exact field"
        ))),
    }
}

fn parse_pipeline_field(value: &str, allow_wildcard: bool) -> Result<PipelineField, LogsqlError> {
    let value = strip_optional_field_parentheses(value)?;
    if allow_wildcard && value == "*" {
        return Ok(PipelineField::All);
    }
    if allow_wildcard && !matches!(value.chars().next(), Some('"' | '\'' | '`')) {
        if let Some(prefix) = value.strip_suffix('*') {
            if prefix.is_empty() || prefix.contains('*') {
                return Err(LogsqlError::malformed(format!(
                    "invalid LogsQL field prefix {value:?}"
                )));
            }
            return Ok(PipelineField::Prefix {
                prefix: prefix.to_owned(),
            });
        }
    }
    if value.contains('*') && !matches!(value.chars().next(), Some('"' | '\'' | '`')) {
        return Err(LogsqlError::malformed(format!(
            "wildcards are not allowed in LogsQL field {value:?}"
        )));
    }
    let path = parse_field_path(value)?;
    let name = if path.len() == 1 {
        path[0].clone()
    } else {
        path.join(".")
    };
    Ok(PipelineField::Exact { path, name })
}

fn pipeline_field_name(field: &PipelineField) -> Result<String, LogsqlError> {
    match field {
        PipelineField::Exact { name, .. } => Ok(name.clone()),
        PipelineField::Prefix { .. } | PipelineField::All => Err(LogsqlError::malformed(
            "LogsQL result names cannot contain wildcards",
        )),
    }
}

fn strip_optional_field_parentheses(value: &str) -> Result<&str, LogsqlError> {
    let value = value.trim();
    if let Some(inner) = value.strip_prefix('(') {
        let inner = inner.strip_suffix(')').ok_or_else(|| {
            LogsqlError::malformed("unterminated parenthesized LogsQL field name")
        })?;
        if inner.trim().is_empty() || split_top_level(inner, ',')?.len() != 1 {
            return Err(LogsqlError::malformed(
                "parenthesized LogsQL field name must contain exactly one field",
            ));
        }
        Ok(inner.trim())
    } else {
        Ok(value)
    }
}

fn matching_parenthesis(value: &str, open: usize) -> Result<usize, LogsqlError> {
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (relative, character) in value[open..].char_indices() {
        let index = open + relative;
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' && delimiter != '`' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        match character {
            '"' | '\'' | '`' => quote = Some(character),
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    LogsqlError::malformed("unmatched LogsQL stats closing parenthesis")
                })?;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => {}
        }
    }
    Err(LogsqlError::malformed("unterminated LogsQL stats function"))
}

fn pipeline_words(value: &str) -> Result<Vec<String>, LogsqlError> {
    split_top_level(value, ' ')
}

fn split_top_level(value: &str, delimiter: char) -> Result<Vec<String>, LogsqlError> {
    let mut output = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut parentheses = 0usize;
    let mut brackets = 0usize;
    for character in value.chars() {
        if let Some(quote_delimiter) = quote {
            current.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' && quote_delimiter != '`' {
                escaped = true;
            } else if character == quote_delimiter {
                quote = None;
            }
            continue;
        }
        match character {
            '"' | '\'' | '`' => {
                quote = Some(character);
                current.push(character);
            }
            '(' => {
                parentheses += 1;
                current.push(character);
            }
            ')' => {
                parentheses = parentheses.checked_sub(1).ok_or_else(|| {
                    LogsqlError::malformed("unmatched LogsQL closing parenthesis")
                })?;
                current.push(character);
            }
            '[' => {
                brackets += 1;
                current.push(character);
            }
            ']' => {
                brackets = brackets
                    .checked_sub(1)
                    .ok_or_else(|| LogsqlError::malformed("unmatched LogsQL closing bracket"))?;
                current.push(character);
            }
            _ if character == delimiter && parentheses == 0 && brackets == 0 => {
                if delimiter == ' ' {
                    if !current.trim().is_empty() {
                        output.push(current.trim().to_owned());
                        current.clear();
                    }
                } else {
                    output.push(current.trim().to_owned());
                    current.clear();
                }
            }
            _ => current.push(character),
        }
    }
    if quote.is_some() || parentheses != 0 || brackets != 0 {
        return Err(LogsqlError::malformed(
            "unterminated LogsQL pipeline expression",
        ));
    }
    if !current.trim().is_empty() || delimiter != ' ' {
        output.push(current.trim().to_owned());
    }
    if output.iter().any(String::is_empty) {
        return Err(LogsqlError::malformed(
            "empty item in LogsQL pipeline expression",
        ));
    }
    Ok(output)
}

fn contains_top_level(value: &str, needle: char) -> Result<bool, LogsqlError> {
    let mut quote = None;
    let mut escaped = false;
    let mut groups = 0usize;
    for character in value.chars() {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' && delimiter != '`' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        match character {
            '"' | '\'' | '`' => quote = Some(character),
            '(' | '[' => groups = groups.saturating_add(1),
            ')' | ']' => {
                groups = groups
                    .checked_sub(1)
                    .ok_or_else(|| LogsqlError::malformed("unmatched LogsQL closing group"))?
            }
            _ if character == needle && groups == 0 => return Ok(true),
            _ => {}
        }
    }
    if quote.is_some() || groups != 0 {
        return Err(LogsqlError::malformed(
            "unterminated LogsQL function expression",
        ));
    }
    Ok(false)
}

#[derive(Clone, Debug)]
pub(crate) struct StatsExpression {
    pub kind: StatsKind,
    pub fields: Vec<PipelineField>,
    pub alias: String,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PipelineSortField {
    pub field: PipelineField,
    pub descending: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FirstSpec {
    pub limit: usize,
    pub by_fields: Vec<PipelineSortField>,
    pub partition_by: Vec<PipelineField>,
    pub rank_field: Option<PipelineField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TopSpec {
    pub limit: usize,
    pub by_fields: Vec<PipelineField>,
    pub hits_field: String,
    pub rank_field: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UniqSpec {
    pub by_fields: Vec<PipelineField>,
    pub filter: Option<String>,
    pub hits_field: Option<String>,
    /// Zero and absence both mean unbounded at the language layer. The hard
    /// API result/state limits still apply.
    pub limit: Option<usize>,
}

#[derive(Clone, Debug)]
pub(crate) enum PipelineOp {
    SortTime {
        descending: bool,
    },
    Offset(usize),
    Limit(usize),
    FieldValues {
        field: PipelineField,
        filter: Option<String>,
        limit: Option<usize>,
    },
    FieldNames {
        filter: Option<String>,
        result_name: String,
    },
    Project(Vec<PipelineField>),
    Delete(Vec<PipelineField>),
    Filter(LogPredicate),
    Stats(Vec<StatsExpression>),
    QueryStats,
    First(FirstSpec),
    Last(FirstSpec),
    Top(TopSpec),
    Uniq(UniqSpec),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogsqlErrorKind {
    Malformed,
    Unsupported,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogsqlError {
    pub kind: LogsqlErrorKind,
    pub message: String,
}

impl LogsqlError {
    fn malformed(message: impl Into<String>) -> Self {
        Self {
            kind: LogsqlErrorKind::Malformed,
            message: message.into(),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            kind: LogsqlErrorKind::Unsupported,
            message: message.into(),
        }
    }
}

impl fmt::Display for LogsqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LogsqlError {}

pub fn parse(query: &str, timestamp_unit: TimestampUnit) -> Result<LogsqlPlan, LogsqlError> {
    parse_at(query, timestamp_unit, now(timestamp_unit))
}

/// Parse with one request-owned evaluation instant.
///
/// This is public so embedded users and deterministic semantic tests can use
/// the same planner as the HTTP route without substituting another parser.
pub fn parse_at(
    query: &str,
    timestamp_unit: TimestampUnit,
    query_now: i64,
) -> Result<LogsqlPlan, LogsqlError> {
    let prepared_query = prepare_query_layout(query)?;
    let query = prepared_query.as_ref();
    let mut spec = QuerySpec {
        limit: 100,
        descending: true,
        ..QuerySpec::default()
    };
    let mut output = LogsqlOutput::Rows;
    let mut limit_explicit = false;
    let mut pipeline_stage = 0u8;
    let mut pipeline = Vec::new();
    let mut has_session_thirteen_pipeline = false;
    let mut has_outer_limit = false;
    let mut segments = pipeline_segments(query)?.into_iter();
    let base = segments.next().unwrap_or_default().trim();
    if base.is_empty() {
        return Err(LogsqlError::malformed("LogsQL query is empty"));
    }
    let logical_tokens = lex_logical_tokens(base)?;
    let use_logical_parser = logical_tokens.iter().any(|token| {
        matches!(
            token,
            LogicalToken::And | LogicalToken::Or | LogicalToken::Not | LogicalToken::FieldGroup(_)
        )
    }) || matches!(logical_tokens.first(), Some(LogicalToken::LeftParen));
    if use_logical_parser {
        let expression = LogicalParser::new(logical_tokens).parse()?;
        let predicate = compile_logical_expression(&expression, None, timestamp_unit, query_now)?;
        apply_safe_logical_pushdowns(&predicate, &mut spec)?;
        spec.predicate = Some(predicate);
    } else {
        for term in logsql_terms(base)? {
            match term {
                LogsqlTerm::Token(token) if token == "*" => {}
                LogsqlTerm::Token(token)
                    if token.starts_with("level:")
                        && uses_legacy_exact_syntax(&token, "level:") =>
                {
                    let level = required_logsql_value(&token, "level:")?;
                    if !matches!(
                        level.as_str(),
                        "debug"
                            | "info"
                            | "notice"
                            | "warning"
                            | "error"
                            | "critical"
                            | "alert"
                            | "emergency"
                    ) {
                        return Err(LogsqlError::malformed(format!(
                            "unsupported LogsQL level {level:?}"
                        )));
                    }
                    spec.level = Some(level);
                }
                LogsqlTerm::Token(token)
                    if token.starts_with("service:")
                        && uses_legacy_exact_syntax(&token, "service:") =>
                {
                    spec.service = Some(required_logsql_value(&token, "service:")?);
                }
                LogsqlTerm::Token(token) if token.starts_with("_time:") => {
                    let window = required_logsql_value(&token, "_time:")?;
                    if let Some(predicate) = parse_repeating_time_range_filter(&window)? {
                        append_predicate(&mut spec, predicate);
                    } else {
                        apply_time_filter(&mut spec, &window, timestamp_unit, query_now)?;
                    }
                }
                LogsqlTerm::Message(message) if spec.message_phrase.is_none() => {
                    spec.message_phrase = Some(message);
                }
                LogsqlTerm::Message(_) => {
                    return Err(LogsqlError::unsupported(
                        "multiple LogsQL message terms are not supported",
                    ))
                }
                LogsqlTerm::Token(token) if metadata_operator(&token).is_some() => {
                    apply_metadata_filter(&mut spec, &token)?;
                }
                LogsqlTerm::Token(token) => {
                    if matches!(token.to_ascii_uppercase().as_str(), "AND" | "OR" | "NOT") {
                        return Err(LogsqlError::unsupported(format!(
                            "LogsQL logical operator {token:?} is not implemented yet"
                        )));
                    }
                    if let Some(exact) = parse_exact_filter(&token)? {
                        append_predicate(&mut spec, exact.predicate(LogField::Message));
                        continue;
                    }
                    if let Some(exact) = parse_multi_exact_filter(&token)? {
                        append_predicate(&mut spec, exact.predicate(LogField::Message));
                        continue;
                    }
                    if let Some(predicate) = parse_contains_filter(&token, LogField::Message)? {
                        append_predicate(&mut spec, predicate);
                        continue;
                    }
                    if let Some(predicate) =
                        parse_json_array_contains_any_filter(&token, LogField::Message)?
                    {
                        append_predicate(&mut spec, predicate);
                        continue;
                    }
                    if let Some(predicate) = parse_ipv4_range_filter(&token, LogField::Message)? {
                        append_predicate(&mut spec, predicate);
                        continue;
                    }
                    if let Some(predicate) = parse_ipv6_range_filter(&token, LogField::Message)? {
                        append_predicate(&mut spec, predicate);
                        continue;
                    }
                    if let Some(predicate) = parse_string_range_filter(&token, LogField::Message)? {
                        append_predicate(&mut spec, predicate);
                        continue;
                    }
                    if let Some(predicate) = parse_len_range_filter(&token, LogField::Message)? {
                        append_predicate(&mut spec, predicate);
                        continue;
                    }
                    if let Some(predicate) =
                        parse_field_comparison_filter(&token, LogField::Message)?
                    {
                        append_predicate(&mut spec, predicate);
                        continue;
                    }
                    if let Some(predicate) = parse_case_insensitive_filter(&token)? {
                        append_predicate(&mut spec, predicate);
                        continue;
                    }
                    if let Some(matcher) = parse_pattern_match_filter(&token)? {
                        append_predicate(
                            &mut spec,
                            LogPredicate::PatternMatch {
                                field: LogField::Message,
                                matcher,
                            },
                        );
                        continue;
                    }
                    if let Some(regex) = parse_regexp_filter(&token)? {
                        append_predicate(
                            &mut spec,
                            LogPredicate::Regex {
                                field: LogField::Message,
                                regex,
                            },
                        );
                        continue;
                    }
                    if let Some(value) = parse_substring_filter(&token)? {
                        append_predicate(
                            &mut spec,
                            LogPredicate::Substring {
                                field: LogField::Message,
                                value,
                                case_insensitive: false,
                            },
                        );
                        continue;
                    }
                    if let Some((value, phrase)) = parse_prefix_filter(&token)? {
                        append_predicate(
                            &mut spec,
                            LogPredicate::Prefix {
                                field: LogField::Message,
                                value,
                                phrase,
                                case_insensitive: false,
                            },
                        );
                        continue;
                    }
                    if token.is_empty() || !token.chars().all(logsql_word_char) {
                        return Err(LogsqlError::unsupported(format!(
                            "unsupported LogsQL term {token:?}"
                        )));
                    }
                    append_predicate(
                        &mut spec,
                        LogPredicate::Word {
                            field: LogField::Message,
                            value: token,
                            case_insensitive: false,
                        },
                    );
                }
            }
        }
    }
    for segment in segments {
        let segment = segment.trim();
        let words: Vec<&str> = segment.split_whitespace().collect();
        match words.as_slice() {
            [command @ ("limit" | "head")] => {
                advance_pipeline(&mut pipeline_stage, 3, command)?;
                spec.limit = 10;
                limit_explicit = true;
                has_outer_limit = true;
                pipeline.push(PipelineOp::Limit(10));
            }
            [command @ ("limit" | "head"), value] => {
                advance_pipeline(&mut pipeline_stage, 3, command)?;
                spec.limit = parse_pipeline_usize(command, value)?;
                limit_explicit = true;
                has_outer_limit = true;
                pipeline.push(PipelineOp::Limit(spec.limit));
            }
            [command @ ("offset" | "skip"), value] => {
                advance_pipeline(&mut pipeline_stage, 2, command)?;
                spec.offset = parse_pipeline_usize(command, value)?;
                pipeline.push(PipelineOp::Offset(spec.offset));
            }
            _ if is_sort_pipe(segment) => {
                advance_pipeline(&mut pipeline_stage, 1, "sort")?;
                spec.descending = parse_time_sort(segment)?;
                pipeline.push(PipelineOp::SortTime {
                    descending: spec.descending,
                });
            }
            ["stats", function @ ("count(*)" | "count()")] if output == LogsqlOutput::Rows => {
                advance_count_pipeline(&mut pipeline_stage)?;
                let _ = function;
                output = LogsqlOutput::Count;
                pipeline.push(PipelineOp::Stats(vec![StatsExpression {
                    kind: StatsKind::Count,
                    fields: vec![PipelineField::All],
                    alias: "total".into(),
                    limit: None,
                }]));
            }
            ["stats", function @ ("count(*)" | "count()"), "as", "total"]
            | ["stats", function @ ("count(*)" | "count()"), "total"]
                if output == LogsqlOutput::Rows =>
            {
                advance_count_pipeline(&mut pipeline_stage)?;
                let _ = function;
                output = LogsqlOutput::Count;
                pipeline.push(PipelineOp::Stats(vec![StatsExpression {
                    kind: StatsKind::Count,
                    fields: vec![PipelineField::All],
                    alias: "total".into(),
                    limit: None,
                }]));
            }
            _ if segment.starts_with("field_values ") => {
                pipeline.push(parse_field_values_pipe(segment)?);
                has_session_thirteen_pipeline = true;
            }
            _ if segment == "field_names" || segment.starts_with("field_names ") => {
                pipeline.push(parse_field_names_pipe(segment)?);
                has_session_thirteen_pipeline = true;
            }
            _ if segment.starts_with("fields ") || segment.starts_with("keep ") => {
                pipeline.push(parse_project_pipe(segment)?);
                has_session_thirteen_pipeline = true;
            }
            _ if is_delete_pipe(segment) => {
                pipeline.push(parse_delete_pipe(segment)?);
                has_session_thirteen_pipeline = true;
            }
            _ if segment.starts_with("filter ") || segment.starts_with("where ") => {
                pipeline.push(parse_filter_pipe(segment, timestamp_unit, query_now)?);
                has_session_thirteen_pipeline = true;
            }
            _ if segment.starts_with("stats ") => {
                pipeline.push(parse_stats_pipe(segment)?);
                has_session_thirteen_pipeline = true;
            }
            _ if words
                .first()
                .is_some_and(|word| word.eq_ignore_ascii_case("query_stats")) =>
            {
                if words.len() != 1 {
                    return Err(LogsqlError::malformed(
                        "LogsQL query_stats accepts no arguments",
                    ));
                }
                pipeline.push(PipelineOp::QueryStats);
                has_session_thirteen_pipeline = true;
            }
            _ if is_first_pipe(segment) => {
                pipeline.push(parse_first_pipe(segment)?);
                has_session_thirteen_pipeline = true;
            }
            _ if is_last_pipe(segment) => {
                pipeline.push(parse_last_pipe(segment)?);
                has_session_thirteen_pipeline = true;
            }
            _ if is_top_pipe(segment) => {
                pipeline.push(parse_top_pipe(segment)?);
                has_session_thirteen_pipeline = true;
            }
            _ if is_uniq_pipe(segment) => {
                pipeline.push(parse_uniq_pipe(segment)?);
                has_session_thirteen_pipeline = true;
            }
            [] => return Err(LogsqlError::malformed("empty LogsQL pipeline")),
            _ => {
                return Err(LogsqlError::unsupported(format!(
                    "unsupported LogsQL pipeline {segment:?}"
                )))
            }
        }
    }
    if has_session_thirteen_pipeline {
        output = LogsqlOutput::Pipeline;
        // Pipeline execution scans a separately bounded public rowset. The
        // ordered sort/offset/limit operations above are replayed by the API
        // and must not also be applied inside the storage cursor.
        spec.limit = 0;
        spec.offset = 0;
        spec.descending = false;
    } else {
        pipeline.clear();
    }
    let cardinality_owned_by_pipeline = pipeline.iter().any(|operation| {
        matches!(
            operation,
            PipelineOp::FieldValues { .. }
                | PipelineOp::FieldNames { .. }
                | PipelineOp::Stats(_)
                | PipelineOp::QueryStats
                | PipelineOp::First(_)
                | PipelineOp::Last(_)
                | PipelineOp::Top(_)
                | PipelineOp::Uniq(_)
        )
    });
    let implicit_result_limit =
        (output == LogsqlOutput::Pipeline && !has_outer_limit && !cardinality_owned_by_pipeline)
            .then_some(100);
    Ok(LogsqlPlan {
        spec,
        output,
        limit_explicit,
        pipeline,
        implicit_result_limit,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ParsedExactFilter {
    Exact(String),
    Prefix(String),
}

impl ParsedExactFilter {
    fn predicate(self, field: LogField) -> LogPredicate {
        match self {
            Self::Exact(value) => LogPredicate::TextualExact { field, value },
            Self::Prefix(value) => LogPredicate::ExactPrefix { field, value },
        }
    }
}

fn parse_exact_filter(token: &str) -> Result<Option<ParsedExactFilter>, LogsqlError> {
    if let Some(value) = token.strip_prefix('=') {
        if value.starts_with('=') {
            return Err(LogsqlError::malformed(
                "an unquoted LogsQL exact value cannot start with =",
            ));
        }
        return parse_exact_argument(value, "LogsQL exact filter").map(Some);
    }

    let Some(open) = token.find('(') else {
        return Ok(None);
    };
    if !token[..open].eq_ignore_ascii_case("exact") {
        return Ok(None);
    }
    let inner = token[open..]
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| LogsqlError::malformed("unterminated LogsQL exact() filter"))?;
    if inner.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL exact() requires exactly one value",
        ));
    }
    parse_exact_argument(inner, "LogsQL exact() filter").map(Some)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ParsedMultiExactFilter {
    Values(Vec<String>),
    Noop,
}

impl ParsedMultiExactFilter {
    fn predicate(self, field: LogField) -> LogPredicate {
        match self {
            Self::Values(values) => LogPredicate::TextualIn { field, values },
            Self::Noop => LogPredicate::True,
        }
    }
}

fn parse_multi_exact_filter(token: &str) -> Result<Option<ParsedMultiExactFilter>, LogsqlError> {
    let Some(parsed) = parse_static_value_list(token, "in")? else {
        return Ok(None);
    };
    Ok(Some(match parsed {
        ParsedStaticValueList::Values(values) => ParsedMultiExactFilter::Values(values),
        ParsedStaticValueList::Noop => ParsedMultiExactFilter::Noop,
    }))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ParsedStaticValueList {
    Values(Vec<String>),
    Noop,
}

fn parse_static_value_list(
    token: &str,
    function: &str,
) -> Result<Option<ParsedStaticValueList>, LogsqlError> {
    if token.eq_ignore_ascii_case(function) {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {function} filter requires parentheses"
        )));
    }
    let Some(open) = token.find('(') else {
        return Ok(None);
    };
    if !token[..open].eq_ignore_ascii_case(function) {
        return Ok(None);
    }
    let inner = token[open..]
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| {
            LogsqlError::malformed(format!("unterminated LogsQL {function}() filter"))
        })?;
    if contains_top_level(inner, '|')? {
        return Err(LogsqlError::unsupported(format!(
            "LogsQL {function}(subquery) is owned by deferred LQL-F38 and is not supported"
        )));
    }

    let inner = inner.trim();
    if inner.is_empty() {
        return Ok(Some(ParsedStaticValueList::Values(Vec::new())));
    }
    let inner = inner.strip_suffix(',').unwrap_or(inner).trim_end();
    if inner.is_empty() {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {function}() cannot start with an empty value"
        )));
    }

    let mut values = Vec::new();
    let mut wildcard = false;
    for argument in split_top_level(inner, ',')? {
        let argument = argument.trim();
        if argument == "*" {
            wildcard = true;
            continue;
        }
        let value = if let Some(value) = quoted_value(argument)? {
            value
        } else {
            if argument.is_empty()
                || argument.chars().any(|character| {
                    character.is_whitespace() || matches!(character, '*' | '(' | ')')
                })
            {
                return Err(LogsqlError::malformed(format!(
                    "invalid LogsQL {function}() value {argument:?}"
                )));
            }
            argument.to_owned()
        };
        values.push(value);
    }
    if wildcard {
        return Ok(Some(ParsedStaticValueList::Noop));
    }
    values.sort_unstable();
    values.dedup();
    Ok(Some(ParsedStaticValueList::Values(values)))
}

fn parse_contains_filter(
    token: &str,
    field: LogField,
) -> Result<Option<LogPredicate>, LogsqlError> {
    if let Some(parsed) = parse_static_value_list(token, "contains_all")? {
        return Ok(Some(match parsed {
            ParsedStaticValueList::Noop => LogPredicate::True,
            ParsedStaticValueList::Values(mut values) => {
                values.retain(|value| !value.is_empty());
                if values.is_empty() {
                    LogPredicate::True
                } else {
                    LogPredicate::TextualContainsAll { field, values }
                }
            }
        }));
    }
    if let Some(parsed) = parse_static_value_list(token, "contains_any")? {
        return Ok(Some(match parsed {
            ParsedStaticValueList::Noop => LogPredicate::True,
            ParsedStaticValueList::Values(values) => {
                if values.iter().any(String::is_empty) {
                    LogPredicate::True
                } else if values.is_empty() {
                    LogPredicate::Or(Vec::new())
                } else {
                    LogPredicate::TextualContainsAny { field, values }
                }
            }
        }));
    }
    Ok(None)
}

fn parse_json_array_contains_any_filter(
    token: &str,
    field: LogField,
) -> Result<Option<LogPredicate>, LogsqlError> {
    const FUNCTION: &str = "json_array_contains_any";

    // VictoriaLogs treats the bare function name as an ordinary word filter.
    // It becomes a function only when the case-insensitive name is followed by
    // an opening parenthesis.
    let Some(open) = token.find('(') else {
        return Ok(None);
    };
    if !token[..open].eq_ignore_ascii_case(FUNCTION) {
        return Ok(None);
    }
    let inner = token[open..]
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| {
            LogsqlError::malformed(format!("unterminated LogsQL {FUNCTION}() filter"))
        })?;
    if contains_top_level(inner, '|')? {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {FUNCTION}() accepts only a static value list"
        )));
    }

    let inner = inner.trim();
    if inner.is_empty() {
        return Ok(Some(LogPredicate::JsonArrayContainsAny {
            field,
            values: Vec::new(),
        }));
    }
    let inner = inner.strip_suffix(',').unwrap_or(inner).trim_end();
    if inner.is_empty() {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {FUNCTION}() cannot start with an empty value"
        )));
    }

    let mut values = Vec::new();
    for argument in split_top_level(inner, ',')? {
        let argument = argument.trim();
        let value = if let Some(value) = quoted_value(argument)? {
            value
        } else {
            if argument.is_empty()
                || argument.chars().any(|character| {
                    character.is_whitespace() || matches!(character, '*' | '(' | ')')
                })
            {
                return Err(LogsqlError::malformed(format!(
                    "invalid LogsQL {FUNCTION}() value {argument:?}"
                )));
            }
            argument.to_owned()
        };
        values.push(value);
    }
    values.sort_unstable();
    values.dedup();
    Ok(Some(LogPredicate::JsonArrayContainsAny { field, values }))
}

fn parse_ipv4_range_filter(
    token: &str,
    field: LogField,
) -> Result<Option<LogPredicate>, LogsqlError> {
    const FUNCTION: &str = "ipv4_range";

    // As in VictoriaLogs, the bare name is still an ordinary word. It is a
    // function only when the case-insensitive name is followed by `(`.
    let Some(open) = token.find('(') else {
        return Ok(None);
    };
    if !token[..open].eq_ignore_ascii_case(FUNCTION) {
        return Ok(None);
    }
    let close = matching_parenthesis(token, open)?;
    if close + 1 != token.len() {
        return Err(LogsqlError::malformed(format!(
            "unexpected text after LogsQL {FUNCTION}() filter"
        )));
    }
    let inner = token[open + 1..close].trim();
    let inner = inner.strip_suffix(',').unwrap_or(inner).trim_end();
    let arguments = if inner.is_empty() {
        Vec::new()
    } else {
        split_top_level(inner, ',')?
            .into_iter()
            .map(|argument| {
                let argument = argument.trim();
                if argument.is_empty() {
                    return Err(LogsqlError::malformed(format!(
                        "empty LogsQL {FUNCTION}() argument"
                    )));
                }
                Ok(quoted_value(argument)?.unwrap_or_else(|| argument.to_owned()))
            })
            .collect::<Result<Vec<_>, LogsqlError>>()?
    };

    let (minimum, maximum) = match arguments.as_slice() {
        [value] => parse_ipv4_or_cidr(value).ok_or_else(|| {
            LogsqlError::malformed(format!(
                "LogsQL {FUNCTION}() requires a valid IPv4 address or CIDR, not {value:?}"
            ))
        })?,
        [minimum, maximum] => {
            let minimum = parse_ipv4_address(minimum).ok_or_else(|| {
                LogsqlError::malformed(format!(
                    "LogsQL {FUNCTION}() has invalid lower IPv4 bound {minimum:?}"
                ))
            })?;
            let maximum = parse_ipv4_address(maximum).ok_or_else(|| {
                LogsqlError::malformed(format!(
                    "LogsQL {FUNCTION}() has invalid upper IPv4 bound {maximum:?}"
                ))
            })?;
            (minimum, maximum)
        }
        _ => {
            return Err(LogsqlError::malformed(format!(
                "LogsQL {FUNCTION}() accepts one address/CIDR or two address bounds"
            )))
        }
    };
    Ok(Some(LogPredicate::Ipv4Range {
        field,
        minimum,
        maximum,
    }))
}

pub(crate) fn parse_ipv4_address(value: &str) -> Option<u32> {
    if !(7..=15).contains(&value.len()) {
        return None;
    }
    let mut address = 0u32;
    let mut count = 0usize;
    for octet in value.split('.') {
        count += 1;
        if !(1..=3).contains(&octet.len()) || !octet.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        let octet = octet.parse::<u16>().ok()?;
        if octet > u16::from(u8::MAX) {
            return None;
        }
        address = (address << 8) | u32::from(octet);
    }
    (count == 4).then_some(address)
}

fn parse_ipv4_or_cidr(value: &str) -> Option<(u32, u32)> {
    let Some((address, prefix)) = value.split_once('/') else {
        let address = parse_ipv4_address(value)?;
        return Some((address, address));
    };
    if prefix.contains('/') {
        return None;
    }
    let address = parse_ipv4_address(address)?;
    let prefix = parse_ipv4_prefix(prefix)?;
    let host_mask = if prefix == 0 {
        u32::MAX
    } else {
        (1u32 << (32 - prefix)) - 1
    };
    Some((address & !host_mask, address | host_mask))
}

fn parse_ipv4_prefix(value: &str) -> Option<u32> {
    parse_ip_prefix(value, 32)
}

fn parse_ip_prefix(value: &str, maximum: u64) -> Option<u32> {
    if value.is_empty()
        || value.len() > "18_446_744_073_709_551_615".len()
        || (value.len() > 1 && value.starts_with('0'))
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'_')
    {
        return None;
    }
    let mut prefix = 0u64;
    for digit in value.bytes().filter(|byte| *byte != b'_') {
        prefix = prefix.checked_mul(10)?;
        prefix = prefix.checked_add(u64::from(digit - b'0'))?;
    }
    (prefix <= maximum).then_some(prefix as u32)
}

fn parse_ipv6_range_filter(
    token: &str,
    field: LogField,
) -> Result<Option<LogPredicate>, LogsqlError> {
    const FUNCTION: &str = "ipv6_range";

    let Some(open) = token.find('(') else {
        return Ok(None);
    };
    if !token[..open].eq_ignore_ascii_case(FUNCTION) {
        return Ok(None);
    }
    let close = matching_parenthesis(token, open)?;
    if close + 1 != token.len() {
        return Err(LogsqlError::malformed(format!(
            "unexpected text after LogsQL {FUNCTION}() filter"
        )));
    }
    let inner = token[open + 1..close].trim();
    let inner = inner.strip_suffix(',').unwrap_or(inner).trim_end();
    let arguments = if inner.is_empty() {
        Vec::new()
    } else {
        split_top_level(inner, ',')?
            .into_iter()
            .map(|argument| {
                let argument = argument.trim();
                if argument.is_empty() {
                    return Err(LogsqlError::malformed(format!(
                        "empty LogsQL {FUNCTION}() argument"
                    )));
                }
                Ok(quoted_value(argument)?.unwrap_or_else(|| argument.to_owned()))
            })
            .collect::<Result<Vec<_>, LogsqlError>>()?
    };

    let (minimum, maximum) = match arguments.as_slice() {
        [value] => parse_ipv6_or_cidr(value).ok_or_else(|| {
            LogsqlError::malformed(format!(
                "LogsQL {FUNCTION}() requires a valid IP address or IPv6 CIDR, not {value:?}"
            ))
        })?,
        [minimum, maximum] => {
            let minimum = parse_ipv6_address(minimum).ok_or_else(|| {
                LogsqlError::malformed(format!(
                    "LogsQL {FUNCTION}() has invalid lower IP bound {minimum:?}"
                ))
            })?;
            let maximum = parse_ipv6_address(maximum).ok_or_else(|| {
                LogsqlError::malformed(format!(
                    "LogsQL {FUNCTION}() has invalid upper IP bound {maximum:?}"
                ))
            })?;
            (minimum, maximum)
        }
        _ => {
            return Err(LogsqlError::malformed(format!(
                "LogsQL {FUNCTION}() accepts one address/CIDR or two address bounds"
            )))
        }
    };
    Ok(Some(LogPredicate::Ipv6Range {
        field,
        minimum,
        maximum,
    }))
}

pub(crate) fn parse_ipv6_address(value: &str) -> Option<[u8; 16]> {
    if !(2..=45).contains(&value.len()) {
        return None;
    }
    match value.parse::<IpAddr>().ok()? {
        IpAddr::V4(address) => Some(address.to_ipv6_mapped().octets()),
        IpAddr::V6(address) => Some(address.octets()),
    }
}

fn parse_ipv6_or_cidr(value: &str) -> Option<([u8; 16], [u8; 16])> {
    let Some((address, prefix)) = value.split_once('/') else {
        let address = parse_ipv6_address(value)?;
        return Some((address, address));
    };
    if prefix.contains('/') {
        return None;
    }
    let address = parse_ipv6_address(address)?;
    let prefix = parse_ip_prefix(prefix, 128)? as usize;
    let mut minimum = address;
    let mut maximum = address;
    let whole_bytes = prefix / 8;
    let remaining_bits = prefix % 8;
    let suffix_start = if remaining_bits == 0 {
        whole_bytes
    } else {
        let mask = u8::MAX << (8 - remaining_bits);
        minimum[whole_bytes] &= mask;
        maximum[whole_bytes] |= !mask;
        whole_bytes + 1
    };
    for index in suffix_start..16 {
        minimum[index] = 0;
        maximum[index] = u8::MAX;
    }
    Some((minimum, maximum))
}

fn parse_string_range_filter(
    token: &str,
    field: LogField,
) -> Result<Option<LogPredicate>, LogsqlError> {
    const FUNCTION: &str = "string_range";

    let Some(open) = token.find('(') else {
        return Ok(None);
    };
    if !token[..open].eq_ignore_ascii_case(FUNCTION) {
        return Ok(None);
    }
    let close = matching_parenthesis(token, open)?;
    if close + 1 != token.len() {
        return Err(LogsqlError::malformed(format!(
            "unexpected text after LogsQL {FUNCTION}() filter"
        )));
    }
    let inner = token[open + 1..close].trim();
    let inner = inner.strip_suffix(',').unwrap_or(inner).trim_end();
    let arguments = if inner.is_empty() {
        Vec::new()
    } else {
        split_top_level(inner, ',')?
            .into_iter()
            .map(|argument| {
                let argument = argument.trim();
                if argument.is_empty() {
                    return Err(LogsqlError::malformed(format!(
                        "empty LogsQL {FUNCTION}() argument"
                    )));
                }
                let quoted = quoted_value(argument)?;
                if quoted.is_none() && argument.contains('*') {
                    return Err(LogsqlError::malformed(format!(
                        "unquoted wildcard in LogsQL {FUNCTION}() argument"
                    )));
                }
                Ok(quoted.unwrap_or_else(|| argument.to_owned()))
            })
            .collect::<Result<Vec<_>, LogsqlError>>()?
    };
    let [minimum, maximum] = arguments.as_slice() else {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {FUNCTION}() requires exactly two bounds"
        )));
    };
    Ok(Some(LogPredicate::StringRange {
        field,
        minimum: minimum.clone(),
        maximum: maximum.clone(),
    }))
}

fn parse_len_range_filter(
    token: &str,
    field: LogField,
) -> Result<Option<LogPredicate>, LogsqlError> {
    const FUNCTION: &str = "len_range";

    let Some(open) = token.find('(') else {
        return Ok(None);
    };
    if !token[..open].eq_ignore_ascii_case(FUNCTION) {
        return Ok(None);
    }
    let close = matching_parenthesis(token, open)?;
    if close + 1 != token.len() {
        return Err(LogsqlError::malformed(format!(
            "unexpected text after LogsQL {FUNCTION}() filter"
        )));
    }
    let inner = token[open + 1..close].trim();
    let inner = inner.strip_suffix(',').unwrap_or(inner).trim_end();
    let arguments = if inner.is_empty() {
        Vec::new()
    } else {
        split_top_level(inner, ',')?
            .into_iter()
            .map(|argument| {
                let argument = argument.trim();
                if argument.is_empty() {
                    return Err(LogsqlError::malformed(format!(
                        "empty LogsQL {FUNCTION}() argument"
                    )));
                }
                Ok(quoted_value(argument)?.unwrap_or_else(|| argument.to_owned()))
            })
            .collect::<Result<Vec<_>, LogsqlError>>()?
    };
    let [minimum, maximum] = arguments.as_slice() else {
        return Err(LogsqlError::malformed(format!(
            "LogsQL {FUNCTION}() requires exactly two bounds"
        )));
    };
    let minimum = parse_len_range_bound(minimum).ok_or_else(|| {
        LogsqlError::malformed(format!(
            "invalid LogsQL {FUNCTION}() minimum bound {minimum:?}"
        ))
    })?;
    let maximum = parse_len_range_bound(maximum).ok_or_else(|| {
        LogsqlError::malformed(format!(
            "invalid LogsQL {FUNCTION}() maximum bound {maximum:?}"
        ))
    })?;
    Ok(Some(LogPredicate::LenRange {
        field,
        minimum,
        maximum,
    }))
}

fn parse_len_range_bound(value: &str) -> Option<u64> {
    if value.eq_ignore_ascii_case("inf") || value.eq_ignore_ascii_case("+inf") {
        return Some(u64::MAX);
    }
    if invalid_base_zero_integer(value) {
        return None;
    }
    parse_prefixed_u64(value)
        .or_else(|| parse_human_bytes(value))
        .or_else(|| parse_human_duration_ns(value))
}

fn parse_field_comparison_filter(
    token: &str,
    left: LogField,
) -> Result<Option<LogPredicate>, LogsqlError> {
    let Some(open) = token.find('(') else {
        return Ok(None);
    };
    let Some(operator) = ([
        ("eq_field", FieldCompareOp::Equal),
        ("le_field", FieldCompareOp::LessOrEqual),
        ("lt_field", FieldCompareOp::Less),
    ])
    .into_iter()
    .find_map(|(name, operator)| token[..open].eq_ignore_ascii_case(name).then_some(operator)) else {
        return Ok(None);
    };
    if matches!(left, LogField::FieldPrefix(_)) {
        return Err(LogsqlError::malformed(
            "LogsQL field comparisons require one concrete left-hand field; wildcard prefixes are ambiguous",
        ));
    }
    let close = matching_parenthesis(token, open)?;
    if close + 1 != token.len() {
        return Err(LogsqlError::malformed(
            "unexpected text after LogsQL field-comparison filter",
        ));
    }
    let inner = token[open + 1..close].trim();
    let inner = inner.strip_suffix(',').unwrap_or(inner).trim_end();
    let arguments = if inner.is_empty() {
        Vec::new()
    } else {
        split_top_level(inner, ',')?
    };
    let [right] = arguments.as_slice() else {
        return Err(LogsqlError::malformed(
            "LogsQL field-comparison filter requires exactly one field",
        ));
    };
    let right = right.trim();
    if right.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL field-comparison filter requires a non-empty field",
        ));
    }
    if quoted_value(right)?.is_none() && right.contains('*') {
        return Err(LogsqlError::malformed(
            "unquoted wildcard in LogsQL field-comparison field",
        ));
    }
    let right = parse_field_selector(right)?;
    if matches!(right, LogField::FieldPrefix(_)) {
        return Err(LogsqlError::malformed(
            "LogsQL field comparisons require one concrete right-hand field",
        ));
    }
    Ok(Some(LogPredicate::FieldCompare {
        left,
        right,
        operator,
    }))
}

pub(crate) fn logsql_field_comparison(left: &str, right: &str, operator: FieldCompareOp) -> bool {
    match operator {
        FieldCompareOp::Equal => left == right,
        FieldCompareOp::LessOrEqual | FieldCompareOp::Less => {
            let ordering = match (
                parse_logsql_math_number(left),
                parse_logsql_math_number(right),
            ) {
                (Some(left), Some(right)) => left.partial_cmp(&right),
                _ => Some(left.as_bytes().cmp(right.as_bytes())),
            };
            match (operator, ordering) {
                (FieldCompareOp::LessOrEqual, Some(ordering)) => !ordering.is_gt(),
                (FieldCompareOp::Less, Some(ordering)) => ordering.is_lt(),
                _ => false,
            }
        }
    }
}

/// VictoriaLogs' `sort`/`first` value order: exact signed integer, exact
/// unsigned integer, RFC3339 timestamp, general math value, then natural UTF-8
/// byte order. This is deliberately separate from field-comparison ordering,
/// which also recognizes IPv4 addresses before its bytewise fallback.
pub(crate) fn logsql_sort_comparison(left: &str, right: &str) -> Ordering {
    if left == right {
        return Ordering::Equal;
    }
    if let (Some(left), Some(right)) = (
        parse_victorialogs_sort_i64(left),
        parse_victorialogs_sort_i64(right),
    ) {
        return left.cmp(&right);
    }
    if let (Some(left), Some(right)) = (
        parse_victorialogs_uint(left),
        parse_victorialogs_uint(right),
    ) {
        return left.cmp(&right);
    }
    if let (Some(left), Some(right)) = (
        parse_victorialogs_timestamp(left),
        parse_victorialogs_timestamp(right),
    ) {
        return left.cmp(&right);
    }
    if let (Some(left), Some(right)) = (
        parse_victorialogs_sort_number(left),
        parse_victorialogs_sort_number(right),
    ) {
        if let Some(ordering) = left.partial_cmp(&right) {
            return ordering;
        }
    }
    natural_comparison(left.as_bytes(), right.as_bytes())
}

fn parse_victorialogs_sort_i64(value: &str) -> Option<i64> {
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map_or((false, value), |value| (true, value));
    let magnitude = parse_victorialogs_uint(unsigned)?;
    if negative {
        if magnitude == 1u64 << 63 {
            Some(i64::MIN)
        } else {
            i64::try_from(magnitude).ok().map(|value| -value)
        }
    } else {
        i64::try_from(magnitude).ok()
    }
}

fn parse_victorialogs_sort_number(value: &str) -> Option<f64> {
    parse_victorialogs_decimal(value)
        .or_else(|| parse_victorialogs_human_duration(value).map(|value| value as f64))
        .or_else(|| parse_victorialogs_human_bytes(value).map(|value| value as f64))
        .or_else(|| {
            is_likely_math_number(value)
                .then(|| parse_go_number(value))
                .flatten()
        })
        .filter(|value| !value.is_nan())
}

fn natural_comparison(mut left: &[u8], mut right: &[u8]) -> Ordering {
    loop {
        let common = left.len().min(right.len());
        let mut index = 0usize;
        while index < common {
            let left_byte = left[index];
            let right_byte = right[index];
            let left_digit = left_byte.is_ascii_digit();
            let right_digit = right_byte.is_ascii_digit();
            match (left_digit, right_digit) {
                (true, true) => break,
                (true, false) => return Ordering::Less,
                (false, true) => return Ordering::Greater,
                (false, false) if left_byte != right_byte => return left_byte.cmp(&right_byte),
                (false, false) => index += 1,
            }
        }
        left = &left[index..];
        right = &right[index..];
        if left.is_empty() || right.is_empty() {
            return left.len().cmp(&right.len());
        }

        let left_digits = left.iter().take_while(|byte| byte.is_ascii_digit()).count();
        let right_digits = right
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        let left_number = parse_natural_u64(&left[..left_digits]);
        let right_number = parse_natural_u64(&right[..right_digits]);
        let (Some(left_number), Some(right_number)) = (left_number, right_number) else {
            return left.cmp(right);
        };
        match left_number.cmp(&right_number) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
        match left_digits.cmp(&right_digits) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
        left = &left[left_digits..];
        right = &right[right_digits..];
    }
}

fn parse_natural_u64(digits: &[u8]) -> Option<u64> {
    digits.iter().try_fold(0u64, |value, digit| {
        value.checked_mul(10)?.checked_add(u64::from(*digit - b'0'))
    })
}

fn parse_logsql_math_number(value: &str) -> Option<f64> {
    parse_victorialogs_decimal(value)
        .or_else(|| parse_victorialogs_human_duration(value).map(|value| value as f64))
        .or_else(|| parse_victorialogs_human_bytes(value).map(|value| value as f64))
        .or_else(|| {
            is_likely_math_number(value)
                .then(|| parse_go_number(value))
                .flatten()
        })
        .or_else(|| parse_victorialogs_timestamp(value).map(|value| value as f64))
        .or_else(|| parse_victorialogs_ipv4(value).map(|value| value as f64))
        .filter(|value| !value.is_nan())
}

fn parse_victorialogs_decimal(value: &str) -> Option<f64> {
    const MAX_TEXT: usize = "-18_446_744_073_709_551_615".len();
    if value.is_empty() || value.len() > MAX_TEXT || value.starts_with('+') {
        return None;
    }
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    if unsigned.is_empty() {
        return None;
    }
    let mut parts = unsigned.split('.');
    let integer = parts.next()?;
    let fraction = parts.next();
    if parts.next().is_some() {
        return None;
    }
    parse_victorialogs_uint(integer)?;
    if let Some(fraction) = fraction {
        if fraction.is_empty() {
            return None;
        }
        let trimmed = fraction
            .char_indices()
            .find(|(index, character)| *character != '0' || *index + 1 == fraction.len())
            .map(|(index, _)| &fraction[index..])
            .unwrap_or(fraction);
        parse_victorialogs_uint(trimmed)?;
    }
    value.replace('_', "").parse::<f64>().ok()
}

fn parse_victorialogs_uint(value: &str) -> Option<u64> {
    if value.is_empty()
        || value.len() > "18_446_744_073_709_551_615".len()
        || (value.len() > 1 && value.starts_with('0'))
    {
        return None;
    }
    let mut result = 0u64;
    for byte in value.bytes() {
        if byte == b'_' {
            continue;
        }
        if !byte.is_ascii_digit() {
            return None;
        }
        result = result
            .checked_mul(10)?
            .checked_add(u64::from(byte - b'0'))?;
    }
    Some(result)
}

fn parse_victorialogs_human_duration(value: &str) -> Option<i64> {
    parse_victorialogs_human_segments(
        value,
        &[
            ("ms", 1_000_000),
            ("ns", 1),
            ("µs", 1_000),
            ("y", 365 * 24 * 3_600_000_000_000),
            ("w", 7 * 24 * 3_600_000_000_000),
            ("d", 24 * 3_600_000_000_000),
            ("h", 3_600_000_000_000),
            ("m", 60_000_000_000),
            ("s", 1_000_000_000),
        ],
        false,
    )
}

fn parse_victorialogs_human_bytes(value: &str) -> Option<i64> {
    parse_victorialogs_human_segments(
        value,
        &[
            ("TiB", 1_i64 << 40),
            ("GiB", 1_i64 << 30),
            ("MiB", 1_i64 << 20),
            ("KiB", 1_i64 << 10),
            ("Ti", 1_i64 << 40),
            ("Gi", 1_i64 << 30),
            ("Mi", 1_i64 << 20),
            ("Ki", 1_i64 << 10),
            ("TB", 1_000_000_000_000),
            ("GB", 1_000_000_000),
            ("MB", 1_000_000),
            ("KB", 1_000),
            ("T", 1_000_000_000_000),
            ("G", 1_000_000_000),
            ("M", 1_000_000),
            ("K", 1_000),
            ("B", 1),
        ],
        true,
    )
}

fn parse_victorialogs_human_segments(
    value: &str,
    units: &[(&str, i64)],
    allow_unsuffixed_integer: bool,
) -> Option<i64> {
    if value.is_empty() || value.starts_with('+') {
        return None;
    }
    let negative = value.starts_with('-');
    let mut remaining = value.strip_prefix('-').unwrap_or(value);
    if remaining.is_empty() {
        return None;
    }
    let mut total = 0i64;
    while !remaining.is_empty() {
        let (number, used) = parse_victorialogs_float_prefix(remaining)?;
        remaining = &remaining[used..];
        let Some((suffix, multiplier)) = units
            .iter()
            .find(|(suffix, _)| remaining.starts_with(*suffix))
            .copied()
        else {
            if allow_unsuffixed_integer && remaining.is_empty() && number.fract() == 0.0 {
                total = saturating_victorialogs_sum(total, number);
                break;
            }
            return None;
        };
        total = saturating_victorialogs_sum(total, number * multiplier as f64);
        remaining = &remaining[suffix.len()..];
    }
    Some(if negative { -total } else { total })
}

fn parse_victorialogs_float_prefix(value: &str) -> Option<(f64, usize)> {
    let mut used = 0usize;
    for (index, byte) in value.bytes().enumerate() {
        if byte.is_ascii_digit() || byte == b'.' || byte == b'_' {
            used = index + 1;
        } else {
            break;
        }
    }
    (used > 0)
        .then(|| parse_victorialogs_decimal(&value[..used]).map(|value| (value, used)))
        .flatten()
}

fn saturating_victorialogs_sum(total: i64, component: f64) -> i64 {
    if !component.is_finite() || component < 0.0 || component >= i64::MAX as f64 {
        return i64::MAX;
    }
    total.saturating_add(component.trunc() as i64)
}

fn is_likely_math_number(value: &str) -> bool {
    let value = value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value);
    if value.is_empty() {
        return false;
    }
    if value.eq_ignore_ascii_case("inf") {
        return true;
    }
    let value = value.strip_prefix('.').unwrap_or(value);
    !value.is_empty()
        && value.as_bytes()[0].is_ascii_digit()
        && value.matches('.').count() <= 1
        && !value.contains(':')
        && value.matches('-').count() <= 2
}

fn parse_go_number(value: &str) -> Option<f64> {
    let compact = value.replace('_', "");
    let floating = match compact.to_ascii_lowercase().as_str() {
        "inf" | "+inf" => Some(f64::INFINITY),
        "-inf" => Some(f64::NEG_INFINITY),
        "nan" | "+nan" | "-nan" => Some(f64::NAN),
        _ => compact.parse::<f64>().ok(),
    };
    floating.or_else(|| parse_go_base_zero_i64(&compact).map(|value| value as f64))
}

fn parse_go_base_zero_i64(value: &str) -> Option<i64> {
    let (negative, unsigned) = if let Some(value) = value.strip_prefix('-') {
        (true, value)
    } else {
        (false, value.strip_prefix('+').unwrap_or(value))
    };
    let (digits, radix) = if let Some(digits) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        (digits, 16)
    } else if let Some(digits) = unsigned
        .strip_prefix("0b")
        .or_else(|| unsigned.strip_prefix("0B"))
    {
        (digits, 2)
    } else if let Some(digits) = unsigned
        .strip_prefix("0o")
        .or_else(|| unsigned.strip_prefix("0O"))
    {
        (digits, 8)
    } else if unsigned.len() > 1 && unsigned.starts_with('0') {
        (&unsigned[1..], 8)
    } else {
        (unsigned, 10)
    };
    if digits.is_empty() {
        return None;
    }
    let magnitude = i128::from_str_radix(digits, radix).ok()?;
    let signed = if negative { -magnitude } else { magnitude };
    i64::try_from(signed).ok()
}

fn parse_victorialogs_timestamp(value: &str) -> Option<i64> {
    if value.len() < "2006-01-02T15:04:05".len() {
        return None;
    }
    let mut normalized = value.to_owned();
    if normalized.as_bytes().get(10) == Some(&b' ') {
        normalized.replace_range(10..11, "T");
    }
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(&normalized) {
        return timestamp.timestamp_nanos_opt();
    }
    if normalized.len() >= 5 {
        let offset = normalized.len() - 5;
        if matches!(normalized.as_bytes().get(offset), Some(b'+' | b'-'))
            && normalized.as_bytes()[offset + 1..]
                .iter()
                .all(u8::is_ascii_digit)
        {
            normalized.insert(offset + 3, ':');
            if let Ok(timestamp) = DateTime::parse_from_rfc3339(&normalized) {
                return timestamp.timestamp_nanos_opt();
            }
        }
    }
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(timestamp) = chrono::NaiveDateTime::parse_from_str(value, format) {
            return timestamp.and_utc().timestamp_nanos_opt();
        }
    }
    None
}

fn parse_victorialogs_ipv4(value: &str) -> Option<u32> {
    let octets = value
        .split('.')
        .map(|octet| {
            (!octet.is_empty()
                && octet.len() <= 3
                && octet.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| octet.parse::<u16>().ok())
            .flatten()
            .filter(|octet| *octet <= 255)
            .map(|octet| octet as u8)
        })
        .collect::<Option<Vec<_>>>()?;
    let [a, b, c, d] = octets.as_slice() else {
        return None;
    };
    Some(u32::from_be_bytes([*a, *b, *c, *d]))
}

fn invalid_base_zero_integer(value: &str) -> bool {
    let value = value.strip_prefix('+').unwrap_or(value);
    if !valid_numeric_underscores(value) {
        return false;
    }
    let compact = value.replace('_', "");
    compact.len() > 1
        && compact.starts_with('0')
        && compact.bytes().all(|byte| byte.is_ascii_digit())
        && compact.bytes().any(|byte| matches!(byte, b'8' | b'9'))
}

fn parse_prefixed_u64(value: &str) -> Option<u64> {
    let value = value.strip_prefix('+').unwrap_or(value);
    if value.is_empty() || value.starts_with('-') || !valid_numeric_underscores(value) {
        return None;
    }
    let compact = value.replace('_', "");
    let (digits, radix) = if let Some(digits) = compact
        .strip_prefix("0x")
        .or_else(|| compact.strip_prefix("0X"))
    {
        (digits, 16)
    } else if let Some(digits) = compact
        .strip_prefix("0b")
        .or_else(|| compact.strip_prefix("0B"))
    {
        (digits, 2)
    } else if let Some(digits) = compact
        .strip_prefix("0o")
        .or_else(|| compact.strip_prefix("0O"))
    {
        (digits, 8)
    } else if compact.len() > 1 && compact.starts_with('0') {
        (&compact[1..], 8)
    } else {
        (compact.as_str(), 10)
    };
    (!digits.is_empty())
        .then(|| u64::from_str_radix(digits, radix).ok())
        .flatten()
}

fn valid_numeric_underscores(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.first() == Some(&b'_') || bytes.last() == Some(&b'_') {
        return false;
    }
    bytes.windows(2).all(|pair| pair != b"__")
}

fn parse_human_bytes(value: &str) -> Option<u64> {
    parse_human_segments(
        value,
        &[
            ("TiB", 1_u64 << 40),
            ("GiB", 1_u64 << 30),
            ("MiB", 1_u64 << 20),
            ("KiB", 1_u64 << 10),
            ("Ti", 1_u64 << 40),
            ("Gi", 1_u64 << 30),
            ("Mi", 1_u64 << 20),
            ("Ki", 1_u64 << 10),
            ("TB", 1_000_000_000_000),
            ("GB", 1_000_000_000),
            ("MB", 1_000_000),
            ("KB", 1_000),
            ("T", 1_000_000_000_000),
            ("G", 1_000_000_000),
            ("M", 1_000_000),
            ("K", 1_000),
            ("B", 1),
        ],
        true,
    )
}

fn parse_human_duration_ns(value: &str) -> Option<u64> {
    parse_human_segments(
        value,
        &[
            ("ms", 1_000_000),
            ("ns", 1),
            ("µs", 1_000),
            ("y", 365 * 24 * 3_600_000_000_000),
            ("w", 7 * 24 * 3_600_000_000_000),
            ("d", 24 * 3_600_000_000_000),
            ("h", 3_600_000_000_000),
            ("m", 60_000_000_000),
            ("s", 1_000_000_000),
        ],
        false,
    )
}

fn parse_human_segments(
    value: &str,
    units: &[(&str, u64)],
    allow_unsuffixed_integer: bool,
) -> Option<u64> {
    if value.is_empty() {
        return None;
    }
    let negative = value.starts_with('-');
    let mut remaining = value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value);
    if remaining.is_empty() {
        return None;
    }
    let mut total = 0_u64;
    let mut saw_suffix = false;
    while !remaining.is_empty() {
        let (number, used) = parse_positive_float_prefix(remaining)?;
        remaining = &remaining[used..];
        let Some((suffix, multiplier)) = units
            .iter()
            .find(|(suffix, _)| remaining.starts_with(*suffix))
            .copied()
        else {
            if allow_unsuffixed_integer && remaining.is_empty() && number.fract() == 0.0 {
                total = saturating_human_sum(total, number);
                break;
            }
            return None;
        };
        saw_suffix = true;
        total = saturating_human_sum(total, number * multiplier as f64);
        remaining = &remaining[suffix.len()..];
    }
    if negative && total != 0 {
        None
    } else {
        (saw_suffix || allow_unsuffixed_integer).then_some(total)
    }
}

fn parse_positive_float_prefix(value: &str) -> Option<(f64, usize)> {
    let bytes = value.as_bytes();
    let mut index = 0;
    let mut digits = 0;
    while matches!(bytes.get(index), Some(b'0'..=b'9' | b'_')) {
        if bytes[index].is_ascii_digit() {
            digits += 1;
        }
        index += 1;
    }
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while matches!(bytes.get(index), Some(b'0'..=b'9' | b'_')) {
            if bytes[index].is_ascii_digit() {
                digits += 1;
            }
            index += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        let exponent = index;
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while matches!(bytes.get(index), Some(b'0'..=b'9' | b'_')) {
            index += 1;
        }
        if exponent_start == index {
            index = exponent;
        }
    }
    let token = &value[..index];
    if !valid_numeric_underscores(token) {
        return None;
    }
    let number = token.replace('_', "").parse::<f64>().ok()?;
    number.is_finite().then_some((number, index))
}

fn saturating_human_sum(total: u64, component: f64) -> u64 {
    const MAX: u64 = i64::MAX as u64;
    if !component.is_finite() || component >= MAX as f64 {
        return MAX;
    }
    total.saturating_add(component.trunc() as u64).min(MAX)
}

fn parse_exact_prefix_argument(value: &str) -> Result<Option<String>, LogsqlError> {
    if !value.ends_with('*') {
        return Ok(None);
    }
    match parse_exact_argument(value, "LogsQL exact-prefix filter")? {
        ParsedExactFilter::Prefix(value) => Ok(Some(value)),
        ParsedExactFilter::Exact(_) => Ok(None),
    }
}

fn parse_exact_argument(value: &str, context: &str) -> Result<ParsedExactFilter, LogsqlError> {
    if value.is_empty() {
        return Err(LogsqlError::malformed(format!(
            "{context} requires a value"
        )));
    }
    if value == "*" {
        return Ok(ParsedExactFilter::Prefix(String::new()));
    }
    if let Some((decoded, consumed)) = parse_quoted_prefix(value)? {
        return match &value[consumed..] {
            "" => Ok(ParsedExactFilter::Exact(decoded)),
            "*" => Ok(ParsedExactFilter::Prefix(decoded)),
            _ => Err(LogsqlError::malformed(format!(
                "unexpected characters after {context} value {value:?}"
            ))),
        };
    }
    if value
        .chars()
        .any(|character| character.is_whitespace() || matches!(character, ',' | '(' | ')'))
    {
        return Err(LogsqlError::malformed(format!(
            "{context} requires exactly one value"
        )));
    }
    if let Some(prefix) = value.strip_suffix('*') {
        return Ok(ParsedExactFilter::Prefix(prefix.to_owned()));
    }
    Ok(ParsedExactFilter::Exact(value.to_owned()))
}

fn parse_case_insensitive_filter(token: &str) -> Result<Option<LogPredicate>, LogsqlError> {
    let Some(inner) = token.strip_prefix("i(") else {
        return Ok(None);
    };
    let inner = inner
        .strip_suffix(')')
        .ok_or_else(|| LogsqlError::malformed("unterminated LogsQL case-insensitive filter"))?;
    if inner.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL case-insensitive filter requires a value",
        ));
    }
    if let Some(value) = parse_substring_filter(inner)? {
        return Ok(Some(LogPredicate::Substring {
            field: LogField::Message,
            value: value.to_lowercase(),
            case_insensitive: true,
        }));
    }
    if let Some((value, phrase)) = parse_prefix_filter(inner)? {
        return Ok(Some(LogPredicate::Prefix {
            field: LogField::Message,
            value: value.to_lowercase(),
            phrase,
            case_insensitive: true,
        }));
    }
    if let Some(value) = quoted_value(inner)? {
        return Ok(Some(LogPredicate::Phrase {
            field: LogField::Message,
            value: value.to_lowercase(),
            case_insensitive: true,
        }));
    }
    if !inner.chars().all(logsql_word_char) {
        return Err(LogsqlError::unsupported(format!(
            "unsupported LogsQL case-insensitive filter {token:?}"
        )));
    }
    Ok(Some(LogPredicate::Word {
        field: LogField::Message,
        value: inner.to_lowercase(),
        case_insensitive: true,
    }))
}

fn parse_regexp_filter(token: &str) -> Result<Option<regex::Regex>, LogsqlError> {
    let Some(value) = token.strip_prefix('~') else {
        return Ok(None);
    };
    let pattern = quoted_value(value)?.ok_or_else(|| {
        LogsqlError::malformed("LogsQL regexp filter requires a quoted pattern after ~")
    })?;
    RegexBuilder::new(&pattern)
        .size_limit(1 << 20)
        .build()
        .map(Some)
        .map_err(|error| LogsqlError::malformed(format!("invalid LogsQL regexp: {error}")))
}

fn parse_pattern_match_filter(token: &str) -> Result<Option<PatternMatcher>, LogsqlError> {
    let functions = [
        ("pattern_match", PatternMatchMode::Any),
        ("pattern_match_full", PatternMatchMode::Full),
        ("pattern_match_prefix", PatternMatchMode::Prefix),
        ("pattern_match_suffix", PatternMatchMode::Suffix),
    ];
    let Some(open) = token.find('(') else {
        return Ok(None);
    };
    let Some((name, mode)) = functions
        .into_iter()
        .find(|(name, _)| token[..open].eq_ignore_ascii_case(name))
    else {
        return Ok(None);
    };
    let inner = token[open..]
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| LogsqlError::malformed(format!("unterminated LogsQL {name} filter")))?;
    let pattern = match quoted_value(inner)? {
        Some(pattern) => pattern,
        None if inner.is_empty() => {
            return Err(LogsqlError::malformed(format!(
                "LogsQL {name} requires one pattern"
            )))
        }
        None if !is_unquoted_pattern_argument(inner) => {
            return Err(LogsqlError::malformed(format!(
                "LogsQL {name} requires exactly one pattern argument"
            )))
        }
        None => inner.to_owned(),
    };
    Ok(Some(PatternMatcher::new(&pattern, mode)))
}

fn is_unquoted_pattern_argument(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|character| {
            logsql_word_char(character) || matches!(character, '+' | '-' | '/' | ':' | '.' | '$')
        })
        && !(value.len() == 1
            && matches!(value.as_bytes()[0], b'+' | b'-' | b'/' | b':' | b'.' | b'$'))
}

fn parse_substring_filter(token: &str) -> Result<Option<String>, LogsqlError> {
    let Some(value) = token
        .strip_prefix('*')
        .and_then(|value| value.strip_suffix('*'))
    else {
        return Ok(None);
    };
    if value.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL substring filter requires a non-empty value",
        ));
    }
    Ok(Some(
        quoted_value(value)?.unwrap_or_else(|| value.to_owned()),
    ))
}

fn parse_prefix_filter(token: &str) -> Result<Option<(String, bool)>, LogsqlError> {
    let Some(value) = token.strip_suffix('*') else {
        return Ok(None);
    };
    if value.is_empty() || value.starts_with('*') {
        return Ok(None);
    }
    if let Some(value) = quoted_value(value)? {
        return Ok(Some((value, true)));
    }
    if !value.chars().all(logsql_word_char) {
        return Ok(None);
    }
    Ok(Some((value.to_owned(), false)))
}

fn append_predicate(spec: &mut QuerySpec, predicate: LogPredicate) {
    spec.predicate = Some(match spec.predicate.take() {
        None => predicate,
        Some(LogPredicate::And(mut predicates)) => {
            predicates.push(predicate);
            LogPredicate::And(predicates)
        }
        Some(existing) => LogPredicate::And(vec![existing, predicate]),
    });
}

fn logsql_word_char(character: char) -> bool {
    character == '_' || character.is_alphanumeric()
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LogicalToken {
    Atom(String),
    FieldGroup(String),
    And,
    Or,
    Not,
    LeftParen,
    RightParen,
}

fn lex_logical_tokens(input: &str) -> Result<Vec<LogicalToken>, LogsqlError> {
    let mut tokens = Vec::new();
    let mut index = 0usize;
    while index < input.len() {
        let Some(character) = input[index..].chars().next() else {
            break;
        };
        if character.is_whitespace() {
            index += character.len_utf8();
            continue;
        }
        if character == '(' {
            tokens.push(LogicalToken::LeftParen);
            index += 1;
            continue;
        }
        if character == ')' {
            tokens.push(LogicalToken::RightParen);
            index += 1;
            continue;
        }

        let start = index;
        let mut field_group = None;
        while index < input.len() {
            let character = input[index..]
                .chars()
                .next()
                .expect("index remains on a UTF-8 boundary");
            if character.is_whitespace() || character == ')' {
                break;
            }
            if matches!(character, '"' | '\'' | '`') {
                let (_, consumed) = parse_quoted_prefix(&input[index..])?
                    .expect("known quote delimiter starts a quoted value");
                index += consumed;
                continue;
            }
            if character == '(' {
                let prefix = &input[start..index];
                if prefix.ends_with(':') && prefix != "_time:" {
                    field_group = Some(prefix[..prefix.len() - 1].to_owned());
                    index += 1;
                    break;
                }
                index = if is_time_range_prefix(prefix) {
                    scan_time_range(input, index)?
                } else {
                    scan_balanced_parentheses(input, index)?
                };
                continue;
            }
            if character == '[' {
                let prefix = &input[start..index];
                if is_time_range_prefix(prefix) {
                    index = scan_time_range(input, index)?;
                    continue;
                }
            }
            index += character.len_utf8();
        }
        if let Some(field) = field_group {
            if field.is_empty() {
                return Err(LogsqlError::malformed(
                    "LogsQL field-scoped group requires a field name",
                ));
            }
            tokens.push(LogicalToken::FieldGroup(field));
            tokens.push(LogicalToken::LeftParen);
            continue;
        }
        if index == start {
            return Err(LogsqlError::malformed(format!(
                "unexpected LogsQL token near {:?}",
                &input[index..]
            )));
        }
        push_logical_atom(&mut tokens, &input[start..index])?;
    }
    combine_repeating_time_range_offset_tokens(tokens)
}

fn is_time_range_prefix(prefix: &str) -> bool {
    prefix == "_time:"
        || prefix.eq_ignore_ascii_case("range")
        || prefix.strip_prefix("_time:").is_some_and(|function| {
            function.eq_ignore_ascii_case("day_range")
                || function.eq_ignore_ascii_case("week_range")
        })
        || prefix
            .get(prefix.len().saturating_sub(6)..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(":range"))
}

fn repeating_time_range_name(atom: &str) -> Option<&str> {
    let value = atom.strip_prefix("_time:")?;
    ["day_range", "week_range"].into_iter().find(|function| {
        value
            .get(..function.len())
            .is_some_and(|name| name.eq_ignore_ascii_case(function))
    })
}

fn combine_repeating_time_range_offset_tokens(
    tokens: Vec<LogicalToken>,
) -> Result<Vec<LogicalToken>, LogsqlError> {
    let mut combined = Vec::with_capacity(tokens.len());
    let mut index = 0usize;
    while index < tokens.len() {
        let LogicalToken::Atom(atom) = &tokens[index] else {
            combined.push(tokens[index].clone());
            index += 1;
            continue;
        };
        if let Some(function) = repeating_time_range_name(atom) {
            if matches!(tokens.get(index + 1), Some(LogicalToken::Atom(offset)) if offset.eq_ignore_ascii_case("offset"))
            {
                let (duration, consumed) = match (tokens.get(index + 2), tokens.get(index + 3)) {
                    (Some(LogicalToken::Atom(duration)), _) => (duration.clone(), 3),
                    (Some(LogicalToken::Not), Some(LogicalToken::Atom(duration))) => {
                        (format!("-{duration}"), 4)
                    }
                    _ => {
                        return Err(LogsqlError::malformed(format!(
                            "LogsQL {function} offset requires one duration"
                        )))
                    }
                };
                combined.push(LogicalToken::Atom(format!("{atom} offset {duration}")));
                index += consumed;
                continue;
            }
        }
        combined.push(tokens[index].clone());
        index += 1;
    }
    Ok(combined)
}

fn scan_balanced_parentheses(input: &str, start: usize) -> Result<usize, LogsqlError> {
    let mut depth = 0usize;
    let mut index = start;
    while index < input.len() {
        let character = input[index..]
            .chars()
            .next()
            .expect("index remains on a UTF-8 boundary");
        if matches!(character, '"' | '\'' | '`') {
            let (_, consumed) = parse_quoted_prefix(&input[index..])?
                .expect("known quote delimiter starts a quoted value");
            index += consumed;
            continue;
        }
        if character == '(' {
            depth = depth.saturating_add(1);
        } else if character == ')' {
            depth = depth
                .checked_sub(1)
                .ok_or_else(|| LogsqlError::malformed("unmatched LogsQL closing parenthesis"))?;
            index += 1;
            if depth == 0 {
                return Ok(index);
            }
            continue;
        }
        index += character.len_utf8();
    }
    Err(LogsqlError::malformed(
        "unterminated LogsQL function arguments",
    ))
}

fn scan_time_range(input: &str, start: usize) -> Result<usize, LogsqlError> {
    for (relative, character) in input[start + 1..].char_indices() {
        if matches!(character, ']' | ')') {
            return Ok(start + 1 + relative + character.len_utf8());
        }
    }
    Err(LogsqlError::malformed("unterminated LogsQL time range"))
}

fn push_logical_atom(tokens: &mut Vec<LogicalToken>, atom: &str) -> Result<(), LogsqlError> {
    if atom.is_empty() {
        return Err(LogsqlError::malformed("empty LogsQL logical token"));
    }
    match atom {
        "AND" => tokens.push(LogicalToken::And),
        "OR" => tokens.push(LogicalToken::Or),
        "NOT" => tokens.push(LogicalToken::Not),
        _ if atom.starts_with('-') && atom.len() > 1 => {
            tokens.push(LogicalToken::Not);
            tokens.push(LogicalToken::Atom(atom[1..].to_owned()));
        }
        _ => tokens.push(LogicalToken::Atom(atom.to_owned())),
    }
    Ok(())
}

#[derive(Clone, Debug)]
enum LogicalExpression {
    Atom(String),
    Field(String, Box<LogicalExpression>),
    And(Vec<LogicalExpression>),
    Or(Vec<LogicalExpression>),
    Not(Box<LogicalExpression>),
}

struct LogicalParser {
    tokens: Vec<LogicalToken>,
    index: usize,
}

impl LogicalParser {
    fn new(tokens: Vec<LogicalToken>) -> Self {
        Self { tokens, index: 0 }
    }

    fn parse(mut self) -> Result<LogicalExpression, LogsqlError> {
        if self.tokens.is_empty() {
            return Err(LogsqlError::malformed("LogsQL query is empty"));
        }
        let expression = self.parse_or()?;
        if self.index != self.tokens.len() {
            return Err(LogsqlError::malformed(format!(
                "unexpected LogsQL logical token {:?}",
                self.tokens[self.index]
            )));
        }
        Ok(expression)
    }

    fn parse_or(&mut self) -> Result<LogicalExpression, LogsqlError> {
        let mut expressions = vec![self.parse_and()?];
        while self.consume(&LogicalToken::Or) {
            expressions.push(self.parse_and()?);
        }
        Ok(combine_logical_or(expressions))
    }

    fn parse_and(&mut self) -> Result<LogicalExpression, LogsqlError> {
        let mut expressions = vec![self.parse_unary()?];
        loop {
            if self.consume(&LogicalToken::And) || self.next_starts_expression() {
                expressions.push(self.parse_unary()?);
            } else {
                break;
            }
        }
        Ok(combine_logical_and(expressions))
    }

    fn parse_unary(&mut self) -> Result<LogicalExpression, LogsqlError> {
        if self.consume(&LogicalToken::Not) {
            return Ok(LogicalExpression::Not(Box::new(self.parse_unary()?)));
        }
        match self.tokens.get(self.index).cloned() {
            Some(LogicalToken::Atom(atom)) => {
                self.index += 1;
                Ok(LogicalExpression::Atom(atom))
            }
            Some(LogicalToken::LeftParen) => {
                self.index += 1;
                let expression = self.parse_or()?;
                self.expect(LogicalToken::RightParen)?;
                Ok(expression)
            }
            Some(LogicalToken::FieldGroup(field)) => {
                self.index += 1;
                self.expect(LogicalToken::LeftParen)?;
                let expression = self.parse_or()?;
                self.expect(LogicalToken::RightParen)?;
                Ok(LogicalExpression::Field(field, Box::new(expression)))
            }
            Some(token) => Err(LogsqlError::malformed(format!(
                "expected LogsQL filter, found {token:?}"
            ))),
            None => Err(LogsqlError::malformed(
                "LogsQL logical expression ends unexpectedly",
            )),
        }
    }

    fn consume(&mut self, expected: &LogicalToken) -> bool {
        if self.tokens.get(self.index) == Some(expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: LogicalToken) -> Result<(), LogsqlError> {
        if self.consume(&expected) {
            Ok(())
        } else {
            Err(LogsqlError::malformed(format!(
                "expected LogsQL logical token {expected:?}"
            )))
        }
    }

    fn next_starts_expression(&self) -> bool {
        matches!(
            self.tokens.get(self.index),
            Some(
                LogicalToken::Atom(_)
                    | LogicalToken::FieldGroup(_)
                    | LogicalToken::Not
                    | LogicalToken::LeftParen
            )
        )
    }
}

fn combine_logical_and(expressions: Vec<LogicalExpression>) -> LogicalExpression {
    if expressions.len() == 1 {
        expressions.into_iter().next().unwrap()
    } else {
        LogicalExpression::And(expressions)
    }
}

fn combine_logical_or(expressions: Vec<LogicalExpression>) -> LogicalExpression {
    if expressions.len() == 1 {
        expressions.into_iter().next().unwrap()
    } else {
        LogicalExpression::Or(expressions)
    }
}

fn compile_logical_expression(
    expression: &LogicalExpression,
    inherited_field: Option<&LogField>,
    timestamp_unit: TimestampUnit,
    query_now: i64,
) -> Result<LogPredicate, LogsqlError> {
    match expression {
        LogicalExpression::Atom(atom) => {
            compile_logical_atom(atom, inherited_field, timestamp_unit, query_now)
        }
        LogicalExpression::Field(field, expression) => {
            let field = parse_field_selector(field)?;
            compile_logical_expression(expression, Some(&field), timestamp_unit, query_now)
        }
        LogicalExpression::And(expressions) => expressions
            .iter()
            .map(|expression| {
                compile_logical_expression(expression, inherited_field, timestamp_unit, query_now)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(LogPredicate::And),
        LogicalExpression::Or(expressions) => expressions
            .iter()
            .map(|expression| {
                compile_logical_expression(expression, inherited_field, timestamp_unit, query_now)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(LogPredicate::Or),
        LogicalExpression::Not(expression) => Ok(LogPredicate::Not(Box::new(
            compile_logical_expression(expression, inherited_field, timestamp_unit, query_now)?,
        ))),
    }
}

fn compile_logical_atom(
    atom: &str,
    inherited_field: Option<&LogField>,
    timestamp_unit: TimestampUnit,
    query_now: i64,
) -> Result<LogPredicate, LogsqlError> {
    if atom == "*" {
        return Ok(LogPredicate::True);
    }
    if atom.starts_with("_time:") {
        let mut spec = QuerySpec::default();
        let value = required_logsql_value(atom, "_time:")?;
        if let Some(predicate) = parse_repeating_time_range_filter(&value)? {
            return Ok(predicate);
        }
        apply_time_filter(&mut spec, &value, timestamp_unit, query_now)?;
        return Ok(LogPredicate::Timestamp {
            minimum: spec.ts_min,
            maximum: spec.ts_max,
        });
    }
    if let Some((operator, typed)) = metadata_operator(atom) {
        let width = if typed { 2 } else { 1 };
        let field = parse_field_selector(&atom[..operator])?;
        let value = &atom[operator + width..];
        return compile_field_filter(&field, value, typed);
    }
    let field = inherited_field.cloned().unwrap_or(LogField::Message);
    compile_unqualified_filter(&field, atom)
}

fn compile_field_filter(
    field: &LogField,
    value: &str,
    typed: bool,
) -> Result<LogPredicate, LogsqlError> {
    if matches!(field, LogField::FieldPrefix(_)) && value.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL field prefix requires a non-empty filter",
        ));
    }
    if typed {
        if let Some(value) = parse_exact_prefix_argument(value)? {
            return Ok(LogPredicate::ExactPrefix {
                field: field.clone(),
                value,
            });
        }
        let expected = parse_metadata_value(value, true)?;
        return Ok(match (field, expected) {
            (LogField::Message | LogField::Level, Value::String(value)) => LogPredicate::Exact {
                field: field.clone(),
                value,
            },
            (LogField::Message | LogField::Level, _) => {
                return Err(LogsqlError::unsupported(
                    "typed exact matching on message or level requires a string",
                ))
            }
            (_, value) => LogPredicate::TypedExact {
                field: field.clone(),
                value,
            },
        });
    }
    if let Some(exact) = parse_exact_filter(value)? {
        return Ok(exact.predicate(field.clone()));
    }
    if let Some(exact) = parse_multi_exact_filter(value)? {
        return Ok(exact.predicate(field.clone()));
    }
    if let Some(predicate) = parse_contains_filter(value, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_json_array_contains_any_filter(value, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_ipv4_range_filter(value, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_ipv6_range_filter(value, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_string_range_filter(value, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_len_range_filter(value, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_field_comparison_filter(value, field.clone())? {
        return Ok(predicate);
    }
    if let Some(matcher) = parse_pattern_match_filter(value)? {
        return Ok(LogPredicate::PatternMatch {
            field: field.clone(),
            matcher,
        });
    }
    if value == "*" {
        return Ok(LogPredicate::AnyValue {
            field: field.clone(),
        });
    }
    if let Some(kind) = value_type_filter(value)? {
        return Ok(LogPredicate::ValueType {
            field: field.clone(),
            kind,
        });
    }
    if let Some(predicates) = numeric_filter_for_field(field, value)? {
        return Ok(if predicates.len() == 1 {
            predicates.into_iter().next().unwrap()
        } else {
            LogPredicate::And(predicates)
        });
    }
    let quoted = quoted_value(value)?;
    if matches!(field, LogField::FieldPrefix(_)) {
        if let Some(value) = quoted {
            return Ok(LogPredicate::Phrase {
                field: field.clone(),
                value,
                case_insensitive: false,
            });
        }
        if value.chars().all(logsql_word_char) {
            return Ok(LogPredicate::Word {
                field: field.clone(),
                value: value.to_owned(),
                case_insensitive: false,
            });
        }
    }
    let value = quoted.unwrap_or_else(|| value.to_owned());
    Ok(LogPredicate::Exact {
        field: field.clone(),
        value,
    })
}

fn compile_unqualified_filter(field: &LogField, atom: &str) -> Result<LogPredicate, LogsqlError> {
    if let Some(value) = quoted_value(atom)? {
        return Ok(if value.is_empty() && !matches!(field, LogField::Message) {
            LogPredicate::Empty {
                field: field.clone(),
            }
        } else {
            LogPredicate::Phrase {
                field: field.clone(),
                value,
                case_insensitive: false,
            }
        });
    }
    if let Some(exact) = parse_exact_filter(atom)? {
        return Ok(exact.predicate(field.clone()));
    }
    if let Some(exact) = parse_multi_exact_filter(atom)? {
        return Ok(exact.predicate(field.clone()));
    }
    if let Some(predicate) = parse_contains_filter(atom, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_json_array_contains_any_filter(atom, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_ipv4_range_filter(atom, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_ipv6_range_filter(atom, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_string_range_filter(atom, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_len_range_filter(atom, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_field_comparison_filter(atom, field.clone())? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_case_insensitive_filter(atom)? {
        return Ok(predicate_for_field(predicate, field));
    }
    if let Some(matcher) = parse_pattern_match_filter(atom)? {
        return Ok(LogPredicate::PatternMatch {
            field: field.clone(),
            matcher,
        });
    }
    if let Some(regex) = parse_regexp_filter(atom)? {
        return Ok(LogPredicate::Regex {
            field: field.clone(),
            regex,
        });
    }
    if let Some(value) = parse_substring_filter(atom)? {
        return Ok(LogPredicate::Substring {
            field: field.clone(),
            value,
            case_insensitive: false,
        });
    }
    if let Some((value, phrase)) = parse_prefix_filter(atom)? {
        return Ok(LogPredicate::Prefix {
            field: field.clone(),
            value,
            phrase,
            case_insensitive: false,
        });
    }
    if let Some(kind) = value_type_filter(atom)? {
        return Ok(LogPredicate::ValueType {
            field: field.clone(),
            kind,
        });
    }
    if let Some(predicates) = numeric_filter_for_field(field, atom)? {
        return Ok(if predicates.len() == 1 {
            predicates.into_iter().next().unwrap()
        } else {
            LogPredicate::And(predicates)
        });
    }
    if atom.is_empty() || !atom.chars().all(logsql_word_char) {
        return Err(LogsqlError::unsupported(format!(
            "unsupported LogsQL logical filter {atom:?}"
        )));
    }
    Ok(LogPredicate::Word {
        field: field.clone(),
        value: atom.to_owned(),
        case_insensitive: false,
    })
}

fn predicate_for_field(predicate: LogPredicate, field: &LogField) -> LogPredicate {
    match predicate {
        LogPredicate::Word {
            value,
            case_insensitive,
            ..
        } => LogPredicate::Word {
            field: field.clone(),
            value,
            case_insensitive,
        },
        LogPredicate::Phrase {
            value,
            case_insensitive,
            ..
        } => LogPredicate::Phrase {
            field: field.clone(),
            value,
            case_insensitive,
        },
        LogPredicate::Prefix {
            value,
            phrase,
            case_insensitive,
            ..
        } => LogPredicate::Prefix {
            field: field.clone(),
            value,
            phrase,
            case_insensitive,
        },
        LogPredicate::Substring {
            value,
            case_insensitive,
            ..
        } => LogPredicate::Substring {
            field: field.clone(),
            value,
            case_insensitive,
        },
        other => other,
    }
}

fn numeric_filter_for_field(
    field: &LogField,
    value: &str,
) -> Result<Option<Vec<LogPredicate>>, LogsqlError> {
    let comparison = [
        (">=", NumericOp::GreaterOrEqual),
        ("<=", NumericOp::LessOrEqual),
        (">", NumericOp::Greater),
        ("<", NumericOp::Less),
    ]
    .into_iter()
    .find_map(|(prefix, operator)| value.strip_prefix(prefix).map(|value| (operator, value)));
    if let Some((operator, value)) = comparison {
        return Ok(Some(vec![LogPredicate::Numeric {
            field: field.clone(),
            operator,
            value: parse_numeric_value(value)?,
        }]));
    }
    let Some((inner, lower_operator, upper_operator)) = numeric_range_shape(value)? else {
        return Ok(None);
    };
    let (lower, upper) = inner.split_once(',').ok_or_else(|| {
        LogsqlError::malformed("LogsQL numeric range requires lower and upper bounds")
    })?;
    if upper.contains(',') {
        return Err(LogsqlError::malformed(
            "LogsQL numeric range accepts exactly two bounds",
        ));
    }
    Ok(Some(vec![
        LogPredicate::Numeric {
            field: field.clone(),
            operator: lower_operator,
            value: parse_numeric_value(lower.trim())?,
        },
        LogPredicate::Numeric {
            field: field.clone(),
            operator: upper_operator,
            value: parse_numeric_value(upper.trim())?,
        },
    ]))
}

fn numeric_range_shape(value: &str) -> Result<Option<(&str, NumericOp, NumericOp)>, LogsqlError> {
    let Some((inner, lower_operator)) = value
        .strip_prefix("range(")
        .map(|inner| (inner, NumericOp::Greater))
        .or_else(|| {
            value
                .strip_prefix("range[")
                .map(|inner| (inner, NumericOp::GreaterOrEqual))
        })
    else {
        if value.starts_with("range") {
            return Err(LogsqlError::malformed(
                "LogsQL numeric range must start with range( or range[",
            ));
        }
        return Ok(None);
    };
    let (inner, upper_operator) = if let Some(inner) = inner.strip_suffix(')') {
        (inner, NumericOp::Less)
    } else if let Some(inner) = inner.strip_suffix(']') {
        (inner, NumericOp::LessOrEqual)
    } else {
        return Err(LogsqlError::malformed(
            "unterminated LogsQL numeric range filter",
        ));
    };
    Ok(Some((inner, lower_operator, upper_operator)))
}

fn apply_safe_logical_pushdowns(
    predicate: &LogPredicate,
    spec: &mut QuerySpec,
) -> Result<(), LogsqlError> {
    match predicate {
        LogPredicate::And(predicates) => {
            for predicate in predicates {
                apply_safe_logical_pushdowns(predicate, spec)?;
            }
        }
        LogPredicate::Timestamp { minimum, maximum } => {
            if let Some(minimum) = minimum {
                spec.ts_min = Some(spec.ts_min.map_or(*minimum, |value| value.max(*minimum)));
            }
            if let Some(maximum) = maximum {
                spec.ts_max = Some(spec.ts_max.map_or(*maximum, |value| value.min(*maximum)));
            }
        }
        LogPredicate::Exact { field, value }
        | LogPredicate::TypedExact {
            field,
            value: Value::String(value),
        } => apply_exact_pushdown(field, value, spec)?,
        _ => {}
    }
    Ok(())
}

fn apply_exact_pushdown(
    field: &LogField,
    value: &str,
    spec: &mut QuerySpec,
) -> Result<(), LogsqlError> {
    match field {
        LogField::Level => {
            if !matches!(
                value,
                "debug"
                    | "info"
                    | "notice"
                    | "warning"
                    | "error"
                    | "critical"
                    | "alert"
                    | "emergency"
            ) {
                return Err(LogsqlError::malformed(format!(
                    "unsupported LogsQL level {value:?}"
                )));
            }
            spec.level = Some(value.to_owned());
        }
        LogField::Metadata(path) if path.as_slice() == ["service"] => {
            spec.service = Some(value.to_owned());
        }
        LogField::Metadata(path) if matches!(path.as_slice(), [key] if matches!(key.as_str(), "host" | "path" | "status")) =>
        {
            spec.metadata_eq.insert(path[0].clone(), value.to_owned());
        }
        LogField::Message | LogField::Time | LogField::Metadata(_) | LogField::FieldPrefix(_) => {}
    }
    Ok(())
}

/// Remove VictoriaLogs-compatible comments and one optional terminal
/// semicolon without changing byte offsets or line boundaries.
///
/// The common path borrows the caller's query. A copy is made only when a
/// comment or terminal semicolon actually needs to be replaced. LogsQL syntax
/// remains API-owned; this normalized text is never sent to the extension.
fn prepare_query_layout(input: &str) -> Result<Cow<'_, str>, LogsqlError> {
    let mut comment_ranges = Vec::new();
    let mut semicolons = Vec::new();
    let mut quote = None;
    let mut escaped = false;
    let mut comment_start = None;
    let mut last_meaningful = None;

    for (index, character) in input.char_indices() {
        if let Some(start) = comment_start {
            if character == '\n' {
                comment_ranges.push((start, index));
                comment_start = None;
            }
            continue;
        }

        if let Some((delimiter, _)) = quote {
            last_meaningful = Some(index);
            if escaped {
                escaped = false;
            } else if character == '\\' && delimiter != '`' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }

        match character {
            '#' => comment_start = Some(index),
            '"' | '\'' | '`' => {
                quote = Some((character, index));
                last_meaningful = Some(index);
            }
            ';' => {
                semicolons.push(index);
                last_meaningful = Some(index);
            }
            character if !character.is_whitespace() => last_meaningful = Some(index),
            _ => {}
        }
    }

    if let Some(start) = comment_start {
        comment_ranges.push((start, input.len()));
    }
    if let Some((_, start)) = quote {
        let (line, column) = source_line_column(input, start);
        return Err(LogsqlError::malformed(format!(
            "unterminated LogsQL quoted string starting at line {line}, column {column}"
        )));
    }

    if semicolons.len() > 1
        || semicolons
            .first()
            .is_some_and(|offset| Some(*offset) != last_meaningful)
    {
        let offset = semicolons
            .iter()
            .copied()
            .find(|offset| Some(*offset) != last_meaningful)
            .unwrap_or(semicolons[0]);
        let (line, column) = source_line_column(input, offset);
        return Err(LogsqlError::malformed(format!(
            "unexpected LogsQL semicolon at line {line}, column {column}; only one terminal semicolon is allowed"
        )));
    }

    if comment_ranges.is_empty() && semicolons.is_empty() {
        return Ok(Cow::Borrowed(input));
    }

    let mut prepared = input.as_bytes().to_vec();
    for (start, end) in comment_ranges {
        prepared[start..end].fill(b' ');
    }
    if let Some(offset) = semicolons.first() {
        prepared[*offset] = b' ';
    }
    Ok(Cow::Owned(
        String::from_utf8(prepared).expect("ASCII replacement preserves valid UTF-8"),
    ))
}

fn source_line_column(source: &str, byte_offset: usize) -> (usize, usize) {
    let prefix = &source[..byte_offset];
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix
        .rsplit_once('\n')
        .map_or(prefix, |(_, suffix)| suffix)
        .chars()
        .count()
        + 1;
    (line, column)
}

fn pipeline_segments(input: &str) -> Result<Vec<&str>, LogsqlError> {
    let mut segments = Vec::new();
    let mut start = 0usize;
    let mut quote = None;
    let mut escaped = false;
    let mut groups = 0usize;
    for (index, character) in input.char_indices() {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' && delimiter != '`' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        if matches!(character, '"' | '\'' | '`') {
            quote = Some(character);
        } else if matches!(character, '(' | '[') {
            groups = groups.saturating_add(1);
        } else if matches!(character, ')' | ']') {
            groups = groups
                .checked_sub(1)
                .ok_or_else(|| LogsqlError::malformed("unmatched LogsQL closing group"))?;
        } else if character == '|' && groups == 0 {
            segments.push(&input[start..index]);
            start = index + character.len_utf8();
        }
    }
    if quote.is_some() {
        return Err(LogsqlError::malformed("unterminated LogsQL quoted string"));
    }
    if groups != 0 {
        return Err(LogsqlError::malformed(
            "unterminated LogsQL parenthesized expression",
        ));
    }
    segments.push(&input[start..]);
    Ok(segments)
}

fn parse_pipeline_usize(command: &str, value: &str) -> Result<usize, LogsqlError> {
    value
        .parse::<usize>()
        .map_err(|_| LogsqlError::malformed(format!("invalid LogsQL {command} value {value:?}")))
}

fn advance_pipeline(stage: &mut u8, next: u8, name: &str) -> Result<(), LogsqlError> {
    if next <= *stage {
        return Err(LogsqlError::unsupported(format!(
            "duplicate or out-of-order LogsQL {name} pipeline"
        )));
    }
    *stage = next;
    Ok(())
}

fn advance_count_pipeline(stage: &mut u8) -> Result<(), LogsqlError> {
    if *stage >= 2 {
        return Err(LogsqlError::unsupported(
            "P0 LogsQL count cannot follow offset or limit because those pipes change count semantics",
        ));
    }
    *stage = 4;
    Ok(())
}

fn is_sort_pipe(segment: &str) -> bool {
    segment.starts_with("sort ") || segment.starts_with("order ")
}

fn parse_time_sort(segment: &str) -> Result<bool, LogsqlError> {
    match segment {
        "sort by (_time)"
        | "sort by (_time) asc"
        | "sort (_time)"
        | "sort (_time) asc"
        | "order by (_time)"
        | "order by (_time) asc" => Ok(false),
        "sort by (_time) desc" | "sort (_time) desc" | "order by (_time) desc" => Ok(true),
        "sort by (_time asc)" | "sort (_time asc)" | "order by (_time asc)" => Ok(false),
        "sort by (_time desc)" | "sort (_time desc)" | "order by (_time desc)" => Ok(true),
        _ => Err(LogsqlError::unsupported(format!(
            "P0 LogsQL sorting supports only _time asc/desc, not {segment:?}"
        ))),
    }
}

enum LogsqlTerm {
    Token(String),
    Message(String),
}

fn logsql_terms(input: &str) -> Result<Vec<LogsqlTerm>, LogsqlError> {
    let mut raw_terms = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut time_range = false;
    let mut numeric_range = false;
    let mut parenthesis_depth = 0usize;
    for character in input.chars() {
        if let Some(delimiter) = quote {
            current.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' && delimiter != '`' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        if matches!(character, '"' | '\'' | '`') {
            quote = Some(character);
            current.push(character);
            continue;
        }
        current.push(character);
        if current.starts_with("_time:[") || current.starts_with("_time:(") {
            time_range = true;
        }
        if !time_range
            && matches!(character, '(' | '[')
            && current
                .get(..current.len() - 1)
                .is_some_and(is_time_range_prefix)
        {
            numeric_range = true;
        }
        if time_range && matches!(character, ']' | ')') {
            time_range = false;
        } else if numeric_range && matches!(character, ']' | ')') {
            numeric_range = false;
        } else if !time_range && !numeric_range && character == '(' {
            parenthesis_depth = parenthesis_depth.saturating_add(1);
        } else if !time_range && !numeric_range && character == ')' {
            parenthesis_depth = parenthesis_depth
                .checked_sub(1)
                .ok_or_else(|| LogsqlError::malformed("unmatched LogsQL closing parenthesis"))?;
        }
        if character.is_whitespace() && !time_range && !numeric_range && parenthesis_depth == 0 {
            current.pop();
            if !current.is_empty() {
                raw_terms.push(std::mem::take(&mut current));
            }
        }
    }
    if quote.is_some() {
        return Err(LogsqlError::malformed("unterminated LogsQL quoted string"));
    }
    if time_range {
        return Err(LogsqlError::malformed("unterminated LogsQL time range"));
    }
    if numeric_range {
        return Err(LogsqlError::malformed(
            "unterminated LogsQL numeric range filter",
        ));
    }
    if parenthesis_depth != 0 {
        return Err(LogsqlError::malformed(
            "unterminated LogsQL parenthesized expression",
        ));
    }
    if !current.is_empty() {
        raw_terms.push(current);
    }

    let raw_terms = combine_repeating_time_range_offset_terms(raw_terms)?;
    let mut terms = Vec::with_capacity(raw_terms.len());
    for raw in raw_terms {
        if let Some((message, consumed)) = parse_quoted_prefix(&raw)? {
            if consumed == raw.len() {
                terms.push(LogsqlTerm::Message(message));
                continue;
            }
        }
        terms.push(LogsqlTerm::Token(raw));
    }
    Ok(terms)
}

fn combine_repeating_time_range_offset_terms(
    raw_terms: Vec<String>,
) -> Result<Vec<String>, LogsqlError> {
    let mut combined = Vec::with_capacity(raw_terms.len());
    let mut index = 0usize;
    while index < raw_terms.len() {
        let term = &raw_terms[index];
        if let Some(function) = repeating_time_range_name(term) {
            if raw_terms
                .get(index + 1)
                .is_some_and(|value| value.eq_ignore_ascii_case("offset"))
            {
                let duration = raw_terms.get(index + 2).ok_or_else(|| {
                    LogsqlError::malformed(format!(
                        "LogsQL {function} offset requires one duration"
                    ))
                })?;
                combined.push(format!("{term} offset {duration}"));
                index += 3;
                continue;
            }
        }
        combined.push(term.clone());
        index += 1;
    }
    Ok(combined)
}

fn required_logsql_value(token: &str, prefix: &str) -> Result<String, LogsqlError> {
    let value = token
        .strip_prefix(prefix)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LogsqlError::malformed(format!("LogsQL {prefix} term requires a value")))?;
    Ok(quoted_value(value)?.unwrap_or_else(|| value.to_owned()))
}

fn uses_legacy_exact_syntax(token: &str, prefix: &str) -> bool {
    token.strip_prefix(prefix).is_some_and(|value| {
        value != "*"
            && !value.starts_with('=')
            && !value.starts_with('(')
            && !value.starts_with('>')
            && !value.starts_with('<')
            && !value.starts_with("range(")
            && !value.starts_with("range[")
            && ![
                "in",
                "contains_all",
                "contains_any",
                "json_array_contains_any",
                "ipv4_range",
                "ipv6_range",
                "string_range",
                "len_range",
                "eq_field",
                "le_field",
                "lt_field",
            ]
            .iter()
            .any(|function| is_named_filter_call(value, function))
    })
}

fn is_named_filter_call(value: &str, function: &str) -> bool {
    value
        .find('(')
        .is_some_and(|open| value[..open].eq_ignore_ascii_case(function))
}

fn apply_metadata_filter(spec: &mut QuerySpec, token: &str) -> Result<(), LogsqlError> {
    let Some((operator, typed)) = metadata_operator(token) else {
        return Err(LogsqlError::unsupported(format!(
            "unsupported LogsQL term {token:?}"
        )));
    };
    let operator_width = if typed { 2 } else { 1 };
    let field_text = &token[..operator];
    let value = &token[operator + operator_width..];
    let field = parse_field_selector(field_text)?;
    if matches!(field, LogField::FieldPrefix(_)) {
        append_predicate(spec, compile_field_filter(&field, value, typed)?);
        return Ok(());
    }
    let path = parse_field_path(field_text)?;
    if typed {
        if let Some(value) = parse_exact_prefix_argument(value)? {
            append_predicate(
                spec,
                LogPredicate::ExactPrefix {
                    field: log_field(&path),
                    value,
                },
            );
            return Ok(());
        }
    } else if let Some(exact) = parse_exact_filter(value)? {
        append_predicate(spec, exact.predicate(log_field(&path)));
        return Ok(());
    } else if let Some(exact) = parse_multi_exact_filter(value)? {
        append_predicate(spec, exact.predicate(log_field(&path)));
        return Ok(());
    } else if let Some(predicate) = parse_contains_filter(value, log_field(&path))? {
        append_predicate(spec, predicate);
        return Ok(());
    } else if let Some(predicate) = parse_json_array_contains_any_filter(value, log_field(&path))? {
        append_predicate(spec, predicate);
        return Ok(());
    } else if let Some(predicate) = parse_ipv4_range_filter(value, log_field(&path))? {
        append_predicate(spec, predicate);
        return Ok(());
    } else if let Some(predicate) = parse_ipv6_range_filter(value, log_field(&path))? {
        append_predicate(spec, predicate);
        return Ok(());
    } else if let Some(predicate) = parse_string_range_filter(value, log_field(&path))? {
        append_predicate(spec, predicate);
        return Ok(());
    } else if let Some(predicate) = parse_len_range_filter(value, log_field(&path))? {
        append_predicate(spec, predicate);
        return Ok(());
    } else if let Some(predicate) = parse_field_comparison_filter(value, log_field(&path))? {
        append_predicate(spec, predicate);
        return Ok(());
    }
    if !typed {
        if let Some(matcher) = parse_pattern_match_filter(value)? {
            append_predicate(
                spec,
                LogPredicate::PatternMatch {
                    field: log_field(&path),
                    matcher,
                },
            );
            return Ok(());
        }
        if let Some(kind) = value_type_filter(value)? {
            append_predicate(
                spec,
                LogPredicate::ValueType {
                    field: log_field(&path),
                    kind,
                },
            );
            return Ok(());
        }
        if let Some(predicates) = numeric_filter(&path, value)? {
            for predicate in predicates {
                append_predicate(spec, predicate);
            }
            return Ok(());
        }
    }
    if !typed && empty_group_value(value)? {
        append_predicate(
            spec,
            LogPredicate::Empty {
                field: log_field(&path),
            },
        );
        return Ok(());
    }
    if !typed && value == "*" {
        append_predicate(
            spec,
            LogPredicate::AnyValue {
                field: log_field(&path),
            },
        );
        return Ok(());
    }
    let expected = parse_metadata_value(value, typed)?;

    if path.as_slice() == ["level"] {
        let Value::String(level) = expected else {
            return Err(LogsqlError::unsupported(
                "LogsQL level exact matching requires a string value",
            ));
        };
        spec.level = Some(level);
        return Ok(());
    }
    if matches!(path.as_slice(), [field] if field == "_msg" || field == "message") {
        let Value::String(value) = expected else {
            return Err(LogsqlError::unsupported(
                "LogsQL message exact matching requires a string value",
            ));
        };
        append_predicate(
            spec,
            LogPredicate::Exact {
                field: LogField::Message,
                value,
            },
        );
        return Ok(());
    }

    // Reuse declared posting-list columns as a sound candidate prefilter.
    // The typed predicate remains in metadata_exact, so a JSON number `500`
    // never aliases the string `"500"` after decoding.
    if path.len() == 1 {
        let key = &path[0];
        if matches!(key.as_str(), "service" | "host" | "path" | "status") {
            if let Value::String(value) = &expected {
                if key == "service" {
                    spec.service = Some(value.clone());
                } else {
                    spec.metadata_eq.insert(key.clone(), value.clone());
                }
            }
        }
    }
    spec.metadata_exact.push(MetadataExact { path, expected });
    Ok(())
}

fn value_type_filter(value: &str) -> Result<Option<ValueTypeKind>, LogsqlError> {
    let Some(inner) = value.strip_prefix("value_type(") else {
        return Ok(None);
    };
    let inner = inner
        .strip_suffix(')')
        .ok_or_else(|| LogsqlError::malformed("unterminated LogsQL value_type filter"))?;
    let kind = match inner {
        "string" => ValueTypeKind::String,
        "uint64" => ValueTypeKind::Uint64,
        "int64" => ValueTypeKind::Int64,
        "float64" => ValueTypeKind::Float64,
        "bool" => ValueTypeKind::Bool,
        "null" => ValueTypeKind::Null,
        "array" => ValueTypeKind::Array,
        "object" => ValueTypeKind::Object,
        "number" => ValueTypeKind::Number,
        "const" | "dict" | "ipv4" | "iso8601" => {
            return Err(LogsqlError::unsupported(format!(
                "LogsQL value_type({inner}) is a VictoriaLogs physical encoding; Timeless exposes retained logical value types"
            )))
        }
        _ => {
            return Err(LogsqlError::malformed(format!(
                "unknown LogsQL logical value type {inner:?}"
            )))
        }
    };
    Ok(Some(kind))
}

fn numeric_filter(path: &[String], value: &str) -> Result<Option<Vec<LogPredicate>>, LogsqlError> {
    numeric_filter_for_field(&log_field(path), value)
}

fn parse_numeric_value(value: &str) -> Result<serde_json::Number, LogsqlError> {
    match serde_json::from_str::<Value>(value) {
        Ok(Value::Number(value)) => Ok(value),
        _ => Err(LogsqlError::malformed(format!(
            "LogsQL numeric filter requires a JSON number, not {value:?}"
        ))),
    }
}

fn empty_group_value(value: &str) -> Result<bool, LogsqlError> {
    let Some(inner) = value
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    else {
        return Ok(false);
    };
    Ok(quoted_value(inner)?.is_some_and(|value| value.is_empty()))
}

fn log_field(path: &[String]) -> LogField {
    match path {
        [field] if field == "_msg" || field == "message" => LogField::Message,
        [field] if field == "level" => LogField::Level,
        [field] if field == "_time" => LogField::Time,
        _ => LogField::Metadata(path.to_vec()),
    }
}

fn parse_field_selector(field: &str) -> Result<LogField, LogsqlError> {
    if field.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL metadata filter requires a field name",
        ));
    }
    if let Some((decoded, consumed)) = parse_quoted_prefix(field)? {
        let suffix = &field[consumed..];
        if suffix == "*" {
            return Ok(LogField::FieldPrefix(decoded));
        }
        if suffix.is_empty() {
            if decoded.is_empty() {
                return Err(LogsqlError::malformed(
                    "LogsQL metadata filter requires a non-empty field name",
                ));
            }
            return Ok(log_field(&[decoded]));
        }
        return Err(LogsqlError::malformed(format!(
            "unexpected characters after LogsQL quoted field {field:?}"
        )));
    }
    if let Some(prefix) = field.strip_suffix('*') {
        if prefix.contains('*')
            || prefix.chars().any(|character| {
                character.is_whitespace() || matches!(character, ':' | '"' | '\'' | '`')
            })
        {
            return Err(LogsqlError::malformed(format!(
                "invalid LogsQL field prefix {field:?}"
            )));
        }
        return Ok(LogField::FieldPrefix(prefix.to_owned()));
    }
    if field.contains('*') {
        return Err(LogsqlError::malformed(format!(
            "invalid LogsQL wildcard field {field:?}"
        )));
    }
    Ok(log_field(&parse_field_path(field)?))
}

fn metadata_operator(token: &str) -> Option<(usize, bool)> {
    let mut quote = None;
    let mut escaped = false;
    let mut parenthesis_depth = 0usize;
    for (index, character) in token.char_indices() {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' && delimiter != '`' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        if matches!(character, '"' | '\'' | '`') {
            quote = Some(character);
        } else if character == '(' {
            parenthesis_depth = parenthesis_depth.saturating_add(1);
        } else if character == ')' {
            parenthesis_depth = parenthesis_depth.saturating_sub(1);
        } else if character == ':' && parenthesis_depth == 0 {
            return Some((index, token[index + 1..].starts_with('=')));
        }
    }
    None
}

fn parse_field_path(field: &str) -> Result<Vec<String>, LogsqlError> {
    if field.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL metadata filter requires a field name",
        ));
    }
    if let Some(field) = quoted_value(field)? {
        if field.is_empty() {
            return Err(LogsqlError::malformed(
                "LogsQL metadata filter requires a non-empty field name",
            ));
        }
        return Ok(vec![field]);
    }
    let path = field
        .split('.')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if path.iter().any(|segment| {
        segment.is_empty()
            || segment.chars().any(|character| {
                character.is_whitespace()
                    || matches!(character, ':' | '"' | '\'' | '`')
                    || character == '*'
            })
    }) {
        return Err(LogsqlError::malformed(format!(
            "invalid LogsQL metadata field path {field:?}"
        )));
    }
    Ok(path)
}

fn parse_metadata_value(value: &str, typed: bool) -> Result<Value, LogsqlError> {
    if value.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL metadata filter requires a value; quote an empty string as \"\"",
        ));
    }
    if let Some(value) = quoted_value(value)? {
        return Ok(Value::String(value));
    }
    if !typed {
        return Ok(Value::String(value.to_owned()));
    }
    match value {
        "null" => Ok(Value::Null),
        "true" => Ok(Value::Bool(true)),
        "false" => Ok(Value::Bool(false)),
        _ => match serde_json::from_str::<Value>(value) {
            Ok(value @ Value::Number(_)) => Ok(value),
            Ok(Value::Array(_) | Value::Object(_)) => Err(LogsqlError::unsupported(
                "P0 typed LogsQL equality supports JSON primitives; select a nested leaf for arrays or objects",
            )),
            Ok(Value::String(value)) => Ok(Value::String(value)),
            Ok(Value::Null | Value::Bool(_)) => unreachable!("handled above"),
            Err(_) => Ok(Value::String(value.to_owned())),
        },
    }
}

fn quoted_value(value: &str) -> Result<Option<String>, LogsqlError> {
    let Some((decoded, consumed)) = parse_quoted_prefix(value)? else {
        return Ok(None);
    };
    if consumed != value.len() {
        return Err(LogsqlError::malformed(format!(
            "unexpected characters after LogsQL quoted value {value:?}"
        )));
    }
    Ok(Some(decoded))
}

fn parse_quoted_prefix(value: &str) -> Result<Option<(String, usize)>, LogsqlError> {
    let Some(delimiter) = value.chars().next() else {
        return Ok(None);
    };
    if !matches!(delimiter, '"' | '\'' | '`') {
        return Ok(None);
    }
    let mut escaped = false;
    for (relative, character) in value[delimiter.len_utf8()..].char_indices() {
        let index = delimiter.len_utf8() + relative;
        if delimiter != '`' {
            if escaped {
                escaped = false;
                continue;
            }
            if character == '\\' {
                escaped = true;
                continue;
            }
        }
        if character == delimiter {
            let inner = &value[delimiter.len_utf8()..index];
            let decoded = if delimiter == '`' {
                inner.to_owned()
            } else {
                decode_quoted_escapes(inner)?
            };
            return Ok(Some((decoded, index + delimiter.len_utf8())));
        }
    }
    Err(LogsqlError::malformed(format!(
        "unterminated LogsQL quoted value {value:?}"
    )))
}

fn decode_quoted_escapes(value: &str) -> Result<String, LogsqlError> {
    let mut decoded = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            let character = value[index..]
                .chars()
                .next()
                .expect("index remains on a UTF-8 boundary");
            let width = character.len_utf8();
            decoded.extend_from_slice(&bytes[index..index + width]);
            index += width;
            continue;
        }
        index += 1;
        let escape = *bytes.get(index).ok_or_else(|| {
            LogsqlError::malformed("LogsQL quoted value ends with an incomplete escape")
        })?;
        index += 1;
        match escape {
            b'a' => decoded.push(0x07),
            b'b' => decoded.push(0x08),
            b'f' => decoded.push(0x0c),
            b'n' => decoded.push(b'\n'),
            b'r' => decoded.push(b'\r'),
            b't' => decoded.push(b'\t'),
            b'v' => decoded.push(0x0b),
            b'\\' => decoded.push(b'\\'),
            b'\'' => decoded.push(b'\''),
            b'"' => decoded.push(b'"'),
            b'x' => {
                let value = decode_digits(bytes, &mut index, 2, 16)?;
                decoded.push(value as u8);
            }
            b'u' => {
                let value = decode_digits(bytes, &mut index, 4, 16)?;
                push_unicode_escape(&mut decoded, value)?;
            }
            b'U' => {
                let value = decode_digits(bytes, &mut index, 8, 16)?;
                push_unicode_escape(&mut decoded, value)?;
            }
            b'0'..=b'7' => {
                index -= 1;
                let value = decode_digits(bytes, &mut index, 3, 8)?;
                if value > u8::MAX as u32 {
                    return Err(LogsqlError::malformed(
                        "LogsQL octal escape exceeds one byte",
                    ));
                }
                decoded.push(value as u8);
            }
            _ => {
                return Err(LogsqlError::malformed(format!(
                    "invalid LogsQL quoted escape \\{}",
                    char::from(escape)
                )))
            }
        }
    }
    String::from_utf8(decoded).map_err(|_| {
        LogsqlError::unsupported(
            "LogsQL byte escape decodes to non-UTF-8 data, which the retained log model cannot store",
        )
    })
}

fn decode_digits(
    bytes: &[u8],
    index: &mut usize,
    count: usize,
    radix: u32,
) -> Result<u32, LogsqlError> {
    let end = index
        .checked_add(count)
        .ok_or_else(|| LogsqlError::malformed("LogsQL quoted numeric escape length overflows"))?;
    let digits = bytes
        .get(*index..end)
        .ok_or_else(|| LogsqlError::malformed("incomplete LogsQL quoted numeric escape"))?;
    let mut value = 0u32;
    for digit in digits {
        let digit = char::from(*digit).to_digit(radix).ok_or_else(|| {
            LogsqlError::malformed("invalid digit in LogsQL quoted numeric escape")
        })?;
        value = value * radix + digit;
    }
    *index = end;
    Ok(value)
}

fn push_unicode_escape(decoded: &mut Vec<u8>, value: u32) -> Result<(), LogsqlError> {
    let character = char::from_u32(value).ok_or_else(|| {
        LogsqlError::malformed(format!("invalid LogsQL Unicode escape U+{value:04X}"))
    })?;
    let mut buffer = [0; 4];
    decoded.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
    Ok(())
}

fn parse_duration_ms(value: &str) -> Option<i64> {
    let split = value.find(|character: char| !character.is_ascii_digit())?;
    let count: i64 = value[..split].parse().ok()?;
    let multiplier = match &value[split..] {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    count.checked_mul(multiplier)
}

fn parse_repeating_time_range_filter(value: &str) -> Result<Option<LogPredicate>, LogsqlError> {
    if let Some(predicate) = parse_day_range_filter(value)? {
        return Ok(Some(predicate));
    }
    parse_week_range_filter(value)
}

fn parse_repeating_time_range_offset(suffix: &str, function: &str) -> Result<i64, LogsqlError> {
    if suffix.is_empty() {
        // Deterministic compatibility choice: an omitted offset means UTC,
        // not the process's mutable local timezone.
        return Ok(0);
    }
    let mut words = suffix.split_whitespace();
    let keyword = words.next().unwrap_or_default();
    let duration = words.next().ok_or_else(|| {
        LogsqlError::malformed(format!("LogsQL {function} offset requires one duration"))
    })?;
    if !keyword.eq_ignore_ascii_case("offset") || words.next().is_some() {
        return Err(LogsqlError::malformed(format!(
            "unexpected text after LogsQL {function}: {suffix:?}"
        )));
    }
    parse_victorialogs_human_duration(duration).ok_or_else(|| {
        LogsqlError::malformed(format!("invalid LogsQL {function} offset {duration:?}"))
    })
}

fn parse_day_range_filter(value: &str) -> Result<Option<LogPredicate>, LogsqlError> {
    const FUNCTION: &str = "day_range";
    const NANOSECONDS_PER_MINUTE: i64 = 60_000_000_000;
    const NANOSECONDS_PER_HOUR: i64 = 60 * NANOSECONDS_PER_MINUTE;
    const NANOSECONDS_PER_DAY: i64 = 24 * NANOSECONDS_PER_HOUR;

    let Some(name) = value.get(..FUNCTION.len()) else {
        return Ok(None);
    };
    if !name.eq_ignore_ascii_case(FUNCTION) {
        return Ok(None);
    }
    let remainder = &value[FUNCTION.len()..];
    let Some(open) = remainder.chars().next() else {
        return Err(LogsqlError::malformed(
            "LogsQL day_range requires '[' or '(' and two bounds",
        ));
    };
    if !matches!(open, '[' | '(') {
        return Err(LogsqlError::malformed(
            "LogsQL day_range must start with '[' or '('",
        ));
    }
    let Some((close_relative, close)) = remainder[open.len_utf8()..]
        .char_indices()
        .find(|(_, character)| matches!(character, ']' | ')'))
    else {
        return Err(LogsqlError::malformed(
            "unterminated LogsQL day_range filter",
        ));
    };
    let close_index = open.len_utf8() + close_relative;
    let inner = &remainder[open.len_utf8()..close_index];
    let (start, end) = inner
        .split_once(',')
        .ok_or_else(|| LogsqlError::malformed("LogsQL day_range requires exactly two bounds"))?;
    if end.contains(',') {
        return Err(LogsqlError::malformed(
            "LogsQL day_range accepts exactly two bounds",
        ));
    }
    let start_ns = parse_day_clock_ns(start.trim()).ok_or_else(|| {
        LogsqlError::malformed(format!("invalid LogsQL day_range start {start:?}"))
    })?;
    let mut end_ns = parse_day_clock_ns(end.trim())
        .ok_or_else(|| LogsqlError::malformed(format!("invalid LogsQL day_range end {end:?}")))?;
    let start_inclusive = open == '[';
    let mut end_inclusive = close == ']';
    if !end_inclusive && end_ns == 0 {
        // VictoriaLogs subtracts one nanosecond from an open end, wrapping
        // midnight to the final nanosecond of the day.
        end_ns = NANOSECONDS_PER_DAY - 1;
        end_inclusive = true;
    }

    let suffix = remainder[close_index + close.len_utf8()..].trim();
    let offset_ns = parse_repeating_time_range_offset(suffix, FUNCTION)?;
    Ok(Some(LogPredicate::DayRange {
        start_ns,
        end_ns,
        start_inclusive,
        end_inclusive,
        offset_ns,
    }))
}

fn parse_week_range_filter(value: &str) -> Result<Option<LogPredicate>, LogsqlError> {
    const FUNCTION: &str = "week_range";

    let Some(name) = value.get(..FUNCTION.len()) else {
        return Ok(None);
    };
    if !name.eq_ignore_ascii_case(FUNCTION) {
        return Ok(None);
    }
    let remainder = &value[FUNCTION.len()..];
    let Some(open) = remainder.chars().next() else {
        return Err(LogsqlError::malformed(
            "LogsQL week_range requires '[' or '(' and two bounds",
        ));
    };
    if !matches!(open, '[' | '(') {
        return Err(LogsqlError::malformed(
            "LogsQL week_range must start with '[' or '('",
        ));
    }
    let Some((close_relative, close)) = remainder[open.len_utf8()..]
        .char_indices()
        .find(|(_, character)| matches!(character, ']' | ')'))
    else {
        return Err(LogsqlError::malformed(
            "unterminated LogsQL week_range filter",
        ));
    };
    let close_index = open.len_utf8() + close_relative;
    let inner = &remainder[open.len_utf8()..close_index];
    let (start, end) = inner
        .split_once(',')
        .ok_or_else(|| LogsqlError::malformed("LogsQL week_range requires exactly two bounds"))?;
    if end.contains(',') {
        return Err(LogsqlError::malformed(
            "LogsQL week_range accepts exactly two bounds",
        ));
    }
    let mut start_day = parse_weekday(start.trim()).ok_or_else(|| {
        LogsqlError::malformed(format!("invalid LogsQL week_range start {start:?}"))
    })?;
    let mut end_day = parse_weekday(end.trim())
        .ok_or_else(|| LogsqlError::malformed(format!("invalid LogsQL week_range end {end:?}")))?;
    if open == '(' {
        start_day = (start_day + 1) % 7;
    }
    if close == ')' {
        end_day = (end_day + 6) % 7;
    }
    let suffix = remainder[close_index + close.len_utf8()..].trim();
    let offset_ns = parse_repeating_time_range_offset(suffix, FUNCTION)?;
    Ok(Some(LogPredicate::WeekRange {
        start_day,
        end_day,
        offset_ns,
    }))
}

fn parse_weekday(value: &str) -> Option<u8> {
    match value.to_ascii_lowercase().as_str() {
        "sun" | "sunday" => Some(0),
        "mon" | "monday" => Some(1),
        "tue" | "tuesday" => Some(2),
        "wed" | "wednesday" => Some(3),
        "thu" | "thursday" => Some(4),
        "fri" | "friday" => Some(5),
        "sat" | "saturday" => Some(6),
        _ => None,
    }
}

fn parse_day_clock_ns(value: &str) -> Option<i64> {
    const NANOSECONDS_PER_MINUTE: i64 = 60_000_000_000;
    const NANOSECONDS_PER_HOUR: i64 = 60 * NANOSECONDS_PER_MINUTE;
    const NANOSECONDS_PER_DAY: i64 = 24 * NANOSECONDS_PER_HOUR;

    let (hours, minutes) = match value.as_bytes() {
        [hour_tens, hour_ones, b':', minute_tens, minute_ones] => {
            ((hour_tens, hour_ones), (minute_tens, minute_ones))
        }
        [hour_tens, hour_ones, minute_tens, minute_ones] => {
            ((hour_tens, hour_ones), (minute_tens, minute_ones))
        }
        _ => return None,
    };
    let decimal_pair = |(tens, ones): (&u8, &u8)| {
        tens.is_ascii_digit()
            .then_some(i64::from(*tens - b'0') * 10)
            .and_then(|value| {
                ones.is_ascii_digit()
                    .then_some(value + i64::from(*ones - b'0'))
            })
    };
    let hours = decimal_pair(hours)?;
    let minutes = decimal_pair(minutes)?;
    if hours > 24 || minutes > 60 {
        return None;
    }
    Some(
        (hours * NANOSECONDS_PER_HOUR + minutes * NANOSECONDS_PER_MINUTE)
            .min(NANOSECONDS_PER_DAY - 1),
    )
}

fn apply_time_filter(
    spec: &mut QuerySpec,
    value: &str,
    timestamp_unit: TimestampUnit,
    query_now: i64,
) -> Result<(), LogsqlError> {
    if matches!(value.as_bytes().first(), Some(b'[' | b'(')) {
        let start_inclusive = value.starts_with('[');
        let end_inclusive = value.ends_with(']');
        if !matches!(value.as_bytes().last(), Some(b']' | b')')) {
            return Err(LogsqlError::malformed(format!(
                "unterminated LogsQL time range {value:?}"
            )));
        }
        let inner = &value[1..value.len() - 1];
        let (start, end) = inner.split_once(',').ok_or_else(|| {
            LogsqlError::malformed(format!("LogsQL time range requires two bounds: {value:?}"))
        })?;
        let mut start = parse_absolute_time(start.trim(), timestamp_unit)?;
        let mut end = parse_absolute_time(end.trim(), timestamp_unit)?;
        if !start_inclusive {
            start = start.checked_add(1).ok_or_else(|| {
                LogsqlError::malformed("exclusive LogsQL lower time bound overflows")
            })?;
        }
        if !end_inclusive {
            end = end.checked_sub(1).ok_or_else(|| {
                LogsqlError::malformed("exclusive LogsQL upper time bound underflows")
            })?;
        }
        tighten_time_bounds(spec, Some(start), Some(end))?;
        return Ok(());
    }

    for (operator, lower, inclusive) in [
        (">=", true, true),
        ("<=", false, true),
        (">", true, false),
        ("<", false, false),
    ] {
        let Some(bound) = value.strip_prefix(operator) else {
            continue;
        };
        let mut bound = parse_absolute_time(bound.trim(), timestamp_unit)?;
        if !inclusive {
            bound = if lower {
                bound.checked_add(1).ok_or_else(|| {
                    LogsqlError::malformed("exclusive LogsQL lower time bound overflows")
                })?
            } else {
                bound.checked_sub(1).ok_or_else(|| {
                    LogsqlError::malformed("exclusive LogsQL upper time bound underflows")
                })?
            };
        }
        return tighten_time_bounds(spec, lower.then_some(bound), (!lower).then_some(bound));
    }

    let duration_ms = parse_duration_ms(value).ok_or_else(|| {
        LogsqlError::malformed(format!("unsupported LogsQL time window {value:?}"))
    })?;
    let upper = query_now
        .checked_sub(1)
        .ok_or_else(|| LogsqlError::malformed("relative LogsQL upper time bound underflows"))?;
    if duration_ms == 0 {
        // The upstream interval is [now, now), which is valid but empty.
        // Preserve that distinction from a contradictory user-supplied
        // absolute range: inclusive storage bounds execute this as an empty,
        // bounded scan.
        spec.ts_min = Some(query_now);
        spec.ts_max = Some(upper);
        return Ok(());
    }
    tighten_time_bounds(
        spec,
        Some(query_now.saturating_sub(duration_from_millis(duration_ms, timestamp_unit))),
        Some(upper),
    )
}

fn parse_absolute_time(value: &str, timestamp_unit: TimestampUnit) -> Result<i64, LogsqlError> {
    if value.is_empty() {
        return Err(LogsqlError::malformed(
            "LogsQL absolute time bound must not be empty",
        ));
    }
    if let Ok(value) = value.parse::<i64>() {
        return Ok(normalize_integer_time(value, timestamp_unit));
    }
    DateTime::parse_from_rfc3339(value)
        .map(|datetime| micros_to_native(datetime.timestamp_micros(), timestamp_unit))
        .map_err(|error| {
            LogsqlError::malformed(format!("invalid LogsQL absolute time {value:?}: {error}"))
        })
}

fn normalize_integer_time(timestamp: i64, timestamp_unit: TimestampUnit) -> i64 {
    let magnitude = timestamp.unsigned_abs();
    let micros = if magnitude < 100_000_000_000 {
        timestamp.saturating_mul(1_000_000)
    } else if magnitude < 100_000_000_000_000 {
        timestamp.saturating_mul(1_000)
    } else if magnitude < 100_000_000_000_000_000 {
        timestamp
    } else {
        timestamp / 1_000
    };
    micros_to_native(micros, timestamp_unit)
}

fn tighten_time_bounds(
    spec: &mut QuerySpec,
    lower: Option<i64>,
    upper: Option<i64>,
) -> Result<(), LogsqlError> {
    if let Some(lower) = lower {
        spec.ts_min = Some(spec.ts_min.map_or(lower, |current| current.max(lower)));
    }
    if let Some(upper) = upper {
        spec.ts_max = Some(spec.ts_max.map_or(upper, |current| current.min(upper)));
    }
    if matches!((spec.ts_min, spec.ts_max), (Some(lower), Some(upper)) if lower > upper) {
        return Err(LogsqlError::malformed(
            "LogsQL time range has no representable timestamps",
        ));
    }
    Ok(())
}

fn now(timestamp_unit: TimestampUnit) -> i64 {
    micros_to_native(Utc::now().timestamp_micros(), timestamp_unit)
}

fn micros_to_native(micros: i64, timestamp_unit: TimestampUnit) -> i64 {
    match timestamp_unit {
        TimestampUnit::Milliseconds => micros / 1_000,
        TimestampUnit::Microseconds => micros,
    }
}

fn duration_from_millis(duration: i64, timestamp_unit: TimestampUnit) -> i64 {
    match timestamp_unit {
        TimestampUnit::Milliseconds => duration,
        TimestampUnit::Microseconds => duration.saturating_mul(1_000),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn workload_shapes_preserve_the_existing_strict_subset() {
        let plan = parse(
            "_time:5m level:error | limit 100",
            TimestampUnit::Microseconds,
        )
        .unwrap();
        assert_eq!(plan.spec.level.as_deref(), Some("error"));
        assert_eq!(plan.spec.limit, 100);
        assert!(plan.spec.ts_min.is_some());
        assert_eq!(plan.output, LogsqlOutput::Rows);

        let plan = parse(
            "_time:1h level:error | stats count(*)",
            TimestampUnit::Microseconds,
        )
        .unwrap();
        assert_eq!(plan.spec.level.as_deref(), Some("error"));
        assert_eq!(plan.output, LogsqlOutput::Count);

        let plan = parse(
            "_time:15m \"timeout\" | limit 50",
            TimestampUnit::Microseconds,
        )
        .unwrap();
        assert_eq!(plan.spec.message_phrase.as_deref(), Some("timeout"));

        for unsupported in [
            "level:error | unpack_json",
            "* | block_stats",
            "* | blocks_count",
            "level:error or level:critical",
            "_time:5q",
            "level:made-up",
        ] {
            assert!(
                parse(unsupported, TimestampUnit::Microseconds).is_err(),
                "{unsupported:?} silently broadened"
            );
        }

        let block_stats = parse("* | block_stats", TimestampUnit::Microseconds).unwrap_err();
        assert_eq!(block_stats.kind, LogsqlErrorKind::Unsupported);
        assert_eq!(
            block_stats.message,
            "unsupported LogsQL pipeline \"block_stats\""
        );

        let blocks_count = parse("* | blocks_count", TimestampUnit::Microseconds).unwrap_err();
        assert_eq!(blocks_count.kind, LogsqlErrorKind::Unsupported);
        assert_eq!(
            blocks_count.message,
            "unsupported LogsQL pipeline \"blocks_count\""
        );
    }

    #[test]
    fn relative_time_uses_the_injected_query_clock_exactly() {
        let plan = parse_at(
            "_time:5m",
            TimestampUnit::Microseconds,
            1_800_000_000_123_456,
        )
        .unwrap();
        assert_eq!(plan.spec.ts_min, Some(1_799_999_700_123_456));
        assert_eq!(plan.spec.ts_max, Some(1_800_000_000_123_455));
        assert_eq!(plan.output, LogsqlOutput::Rows);

        let empty = parse_at(
            "_time:0s",
            TimestampUnit::Microseconds,
            1_800_000_000_123_456,
        )
        .unwrap();
        assert_eq!(empty.spec.ts_min, Some(1_800_000_000_123_456));
        assert_eq!(empty.spec.ts_max, Some(1_800_000_000_123_455));
    }

    #[test]
    fn integer_absolute_times_accept_unix_seconds_millis_micros_and_nanos() {
        for (value, expected) in [
            ("1800000001", 1_800_000_001_000_000),
            ("1800000001000", 1_800_000_001_000_000),
            ("1800000001000000", 1_800_000_001_000_000),
            ("1800000001000000000", 1_800_000_001_000_000),
        ] {
            let plan =
                parse_at(&format!("_time:>={value}"), TimestampUnit::Microseconds, 0).unwrap();
            assert_eq!(plan.spec.ts_min, Some(expected), "{value}");
        }

        let milliseconds =
            parse_at("_time:>=1800000001000000", TimestampUnit::Milliseconds, 0).unwrap();
        assert_eq!(milliseconds.spec.ts_min, Some(1_800_000_001_000));
    }

    #[test]
    fn absolute_ranges_preserve_open_and_closed_microsecond_edges() {
        let closed_open = parse_at(
            "_time:[2027-01-15T08:00:00.000001Z, 2027-01-15T08:00:00.000004Z)",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(closed_open.spec.ts_min, Some(1_800_000_000_000_001));
        assert_eq!(closed_open.spec.ts_max, Some(1_800_000_000_000_003));

        let open_closed = parse_at(
            "_time:(2027-01-15T08:00:00.000001Z, 2027-01-15T08:00:00.000004Z]",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(open_closed.spec.ts_min, Some(1_800_000_000_000_002));
        assert_eq!(open_closed.spec.ts_max, Some(1_800_000_000_000_004));

        for malformed in [
            "_time:[2027-01-15T08:00:00Z,broken)",
            "_time:[2027-01-15T08:00:01Z,2027-01-15T08:00:00Z]",
            "_time:[2027-01-15T08:00:00Z,2027-01-15T08:00:01Z",
        ] {
            assert!(
                parse_at(malformed, TimestampUnit::Microseconds, 0).is_err(),
                "{malformed:?} was accepted"
            );
        }
    }

    #[test]
    fn comparison_time_bounds_intersect_without_losing_exclusivity() {
        let plan = parse_at(
            "_time:>2027-01-15T08:00:00.000001Z _time:<=2027-01-15T08:00:00.000004Z",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(plan.spec.ts_min, Some(1_800_000_000_000_002));
        assert_eq!(plan.spec.ts_max, Some(1_800_000_000_000_004));

        let plan = parse_at(
            "_time:>=2027-01-15T08:00:00.000001Z _time:<2027-01-15T08:00:00.000004Z",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(plan.spec.ts_min, Some(1_800_000_000_000_001));
        assert_eq!(plan.spec.ts_max, Some(1_800_000_000_000_003));

        assert!(parse_at(
            "_time:>=2027-01-15T08:00:01Z _time:<2027-01-15T08:00:00Z",
            TimestampUnit::Microseconds,
            0,
        )
        .is_err());
    }

    #[test]
    fn arbitrary_metadata_equality_keeps_paths_and_json_primitive_types() {
        let plan = parse_at(
            r#"app:"my app" nested.ok:=true nested.count:=2 nested.none:=null nested.empty:"""#,
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(
            plan.spec.metadata_exact,
            vec![
                crate::MetadataExact {
                    path: vec!["app".into()],
                    expected: json!("my app"),
                },
                crate::MetadataExact {
                    path: vec!["nested".into(), "ok".into()],
                    expected: json!(true),
                },
                crate::MetadataExact {
                    path: vec!["nested".into(), "count".into()],
                    expected: json!(2),
                },
                crate::MetadataExact {
                    path: vec!["nested".into(), "none".into()],
                    expected: json!(null),
                },
                crate::MetadataExact {
                    path: vec!["nested".into(), "empty".into()],
                    expected: json!(""),
                },
            ]
        );
        for malformed in ["nested..ok:true", "nested.ok:=", ":value"] {
            assert!(
                parse_at(malformed, TimestampUnit::Microseconds, 0).is_err(),
                "{malformed:?} was accepted"
            );
        }
    }

    #[test]
    fn quoted_message_is_a_case_sensitive_phrase_not_legacy_contains() {
        let plan = parse_at(r#""ssh: login fail""#, TimestampUnit::Microseconds, 0).unwrap();
        assert_eq!(plan.spec.message_phrase.as_deref(), Some("ssh: login fail"));
        assert_eq!(plan.spec.message, None);
    }

    #[test]
    fn session_twelve_word_filter_preserves_case_and_unicode_boundaries() {
        assert!(parse_at("alpha", TimestampUnit::Microseconds, 0).is_ok());
        assert!(parse_at("тест45", TimestampUnit::Microseconds, 0).is_ok());
    }

    #[test]
    fn session_twelve_prefix_filter_accepts_words_and_quoted_phrases() {
        assert!(parse_at("alph*", TimestampUnit::Microseconds, 0).is_ok());
        let phrase = parse_at(r#""ssh: login fai"*"#, TimestampUnit::Microseconds, 0);
        assert!(phrase.is_ok(), "{phrase:?}");
    }

    #[test]
    fn session_twelve_case_insensitive_filters_cover_word_phrase_prefix_and_unicode() {
        for query in ["i(alpha)", r#"i("SSH: LOGIN FAIL")"#, "i(alph*)", "i(café)"] {
            let parsed = parse_at(query, TimestampUnit::Microseconds, 0);
            assert!(parsed.is_ok(), "{query}: {parsed:?}");
        }
    }

    #[test]
    fn session_sixteen_exact_filter_accepts_pinned_quoted_unquoted_and_function_forms() {
        assert!(parse_at(r#"="alpha""#, TimestampUnit::Microseconds, 0).is_ok());
        assert!(parse_at("=alpha", TimestampUnit::Microseconds, 0).is_ok());
        assert!(parse_at("exact(alpha)", TimestampUnit::Microseconds, 0).is_ok());
        assert!(parse_at("==alpha", TimestampUnit::Microseconds, 0).is_err());
    }

    #[test]
    fn session_twelve_empty_and_any_filters_preserve_legacy_exact_empty() {
        let legacy = parse_at(r#"probe:"""#, TimestampUnit::Microseconds, 0).unwrap();
        assert_eq!(
            legacy.spec.metadata_exact[0].expected,
            Value::String(String::new())
        );
        assert!(legacy.spec.predicate.is_none());

        let compatible = parse_at(r#"probe:("")"#, TimestampUnit::Microseconds, 0).unwrap();
        assert!(compatible.spec.metadata_exact.is_empty());
        assert!(compatible.spec.predicate.is_some());
        assert!(parse_at("probe:*", TimestampUnit::Microseconds, 0).is_ok());
        assert!(parse_at("nested.leaf:*", TimestampUnit::Microseconds, 0).is_ok());
    }

    #[test]
    fn session_twelve_numeric_filters_are_typed_and_ranges_are_open() {
        for query in [
            "n:>2",
            "n:>=2",
            "n:<10",
            "n:<=10",
            "n:range(2, 10)",
            "n:range[2, 10)",
            "n:range(2, 10]",
            "n:range[2, 10]",
        ] {
            let parsed = parse_at(query, TimestampUnit::Microseconds, 0);
            assert!(parsed.is_ok(), "{query}: {parsed:?}");
        }
        assert!(parse_at("n:>two", TimestampUnit::Microseconds, 0).is_err());
        assert!(parse_at("n:range(2)", TimestampUnit::Microseconds, 0).is_err());
    }

    #[test]
    fn session_twelve_value_type_uses_retained_logical_types() {
        for kind in [
            "string", "uint64", "int64", "float64", "bool", "null", "array", "object", "number",
        ] {
            let query = format!("field:value_type({kind})");
            let parsed = parse_at(&query, TimestampUnit::Microseconds, 0);
            assert!(parsed.is_ok(), "{query}: {parsed:?}");
        }
        let physical =
            parse_at("field:value_type(const)", TimestampUnit::Microseconds, 0).unwrap_err();
        assert_eq!(physical.kind, LogsqlErrorKind::Unsupported);
    }

    #[test]
    fn session_twelve_logical_parser_honors_precedence_and_safe_pushdowns() {
        for query in [
            r#"alpha AND ~"before""#,
            r#"="alpha" OR ="ALPHA""#,
            r#"alpha AND NOT ~"before""#,
            r#"(="alpha" OR ="ALPHA") AND ~"ALPHA""#,
            r#"case:(="word-exact" OR ="word-case")"#,
            r#"numeric_group:="numeric" AND (n:<0 OR n:>9)"#,
        ] {
            let parsed = parse_at(query, TimestampUnit::Microseconds, 0);
            assert!(parsed.is_ok(), "{query}: {parsed:?}");
        }
        let safe = parse_at(
            r#"service:="api" AND (alpha OR beta)"#,
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(safe.spec.service.as_deref(), Some("api"));

        let unsafe_branch =
            parse_at(r#"service:="api" OR alpha"#, TimestampUnit::Microseconds, 0).unwrap();
        assert_eq!(unsafe_branch.spec.service, None);
    }

    #[test]
    fn session_twelve_substring_filter_is_explicit_and_case_sensitive() {
        assert!(parse_at("*pha*", TimestampUnit::Microseconds, 0).is_ok());
    }

    #[test]
    fn session_twelve_regexp_filter_is_re2_compatible_and_strict() {
        assert!(parse_at(r#"~"alp(ha|ine)""#, TimestampUnit::Microseconds, 0).is_ok());
        assert!(parse_at(r#"~"(?i)^alpha$""#, TimestampUnit::Microseconds, 0).is_ok());
        assert!(parse_at(r#"~"(""#, TimestampUnit::Microseconds, 0).is_err());
    }

    #[test]
    fn session_sixteen_pattern_match_grammar_is_strict_and_composable() {
        for query in [
            r#"pattern_match("x <N> y")"#,
            r#"pattern_match_full("<UUID>")"#,
            r#"pattern_match_prefix("date=<DATE>")"#,
            r#"pattern_match_suffix("word=<W>")"#,
            r#"PaTtErN_MaTcH_FuLl("job-<N>")"#,
            r#"pattern_match_full(job-123)"#,
            r#"code:pattern_match_full("job-<N>")"#,
            r#"code:(pattern_match("job-<N>") OR pattern_match_full("other"))"#,
            r#"pattern_match("alpha") AND NOT pattern_match_suffix("beta")"#,
            r#"* | filter code:pattern_match_full("job-<N>")"#,
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0);
            assert!(plan.is_ok(), "{query}: {plan:?}");
        }

        for malformed in [
            "pattern_match()",
            r#"pattern_match("x", "y")"#,
            "pattern_match(x y)",
            r#"pattern_match("unterminated)"#,
            r#"pattern_match_full("x")junk"#,
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(
                error.kind,
                LogsqlErrorKind::Malformed,
                "{malformed}: {error}"
            );
        }
    }

    #[test]
    fn session_sixteen_exact_prefix_grammar_is_strict_and_composable() {
        for query in [
            r#"="alpha"*"#,
            r#"case:="pattern-"*"#,
            r#"probe:=""*"#,
            r#"=alpha"#,
            r#"exact(alpha*)"#,
            r#"ExAcT(alpha*)"#,
            r#"exact(*)"#,
            r#"field:exact(alpha*)"#,
            r#"exact(alpha)"#,
            r#"="alpha"* AND NOT ="alphas"*"#,
            r#"* | filter field:="prefix"*"#,
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0);
            assert!(plan.is_ok(), "{query}: {plan:?}");
        }

        for malformed in ["exact()", "exact(foo, bar)", "exact(foo *)"] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }
    }

    #[test]
    fn session_sixteen_multi_exact_grammar_is_strict_and_composable() {
        for query in [
            r#"in(alpha, "ssh: login fail")"#,
            r#"in("left|right")"#,
            r#"case:in(word-exact, "phrase-exact")"#,
            r#"case:In(word-exact, word-case)"#,
            r#"probe:in("", 0, false, value)"#,
            r#"case:in()"#,
            r#"missing:in(*)"#,
            r#"case:in(word-exact,)"#,
            r#"missing:in(alpha, *)"#,
            r#"case:in(word-exact, word-case) AND NOT case:in(word-case)"#,
            r#"* | filter case:in(word-exact, word-case)"#,
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0);
            assert!(plan.is_ok(), "{query}: {plan:?}");
        }

        for malformed in ["in", "in(", "in(,)", "in(alpha beta)", "in(alpha*)"] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }

        let subquery = parse_at(
            "case:in(word | fields case)",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap_err();
        assert_eq!(subquery.kind, LogsqlErrorKind::Unsupported);
        assert!(subquery.message.contains("subquery"));
    }

    #[test]
    fn session_sixteen_field_noop_grammar_is_strict_and_field_independent() {
        for query in [
            "never_present:contains_any(*)",
            "never_present:contains_all(*)",
            "never_present:CoNtAiNs_AnY(*)",
            "never_present:contains_any(alpha, *)",
            "never_present:contains_all(*, alpha)",
            "service:in(*)",
            "level:contains_all(*)",
            "probe:in(*)",
            "probe:contains_any(*) AND case:in(state-string)",
            "NOT probe:contains_all(*)",
            "* | filter never_present:contains_any(*)",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0);
            assert!(plan.is_ok(), "{query}: {plan:?}");
        }

        for malformed in ["contains_any", "contains_any(* alpha)", "contains_all(,*)"] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }
    }

    #[test]
    fn session_sixteen_contains_all_static_grammar_is_complete_and_strict() {
        for query in [
            "contains_all(alpha)",
            r#"contains_all(alpha, "before alpha")"#,
            "case:contains_all(word, exact)",
            r#"case:contains_all("", word, exact)"#,
            "case:contains_all(word, word, exact,)",
            "never_present:contains_all()",
            r#"never_present:contains_all("")"#,
            "level:contains_all(info)",
            "case:CoNtAiNs_AlL(word, exact)",
            "contains_all(alpha) AND NOT case:exact(word-inside)",
            "* | filter case:contains_all(word, exact)",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0);
            assert!(plan.is_ok(), "{query}: {plan:?}");
        }

        let subquery = parse_at(
            "case:contains_all(* | fields case)",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap_err();
        assert_eq!(subquery.kind, LogsqlErrorKind::Unsupported);
        assert!(subquery.message.contains("LQL-F38"), "{subquery}");

        for malformed in [
            "contains_all",
            "contains_all(",
            "contains_all(,alpha)",
            "contains_all(alpha beta)",
            "contains_all(alpha*)",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }
    }

    #[test]
    fn session_sixteen_contains_any_static_grammar_is_complete_and_strict() {
        for query in [
            "contains_any(alpha)",
            r#"contains_any(alpha, "before alpha")"#,
            "case:contains_any(word, exact)",
            r#"case:contains_any("", word, exact)"#,
            "case:contains_any(word, word, exact,)",
            "never_present:contains_any()",
            r#"never_present:contains_any("")"#,
            "level:contains_any(info)",
            "case:CoNtAiNs_AnY(word, exact)",
            "contains_any(alpha) AND NOT case:exact(word-inside)",
            "* | filter case:contains_any(word, exact)",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0);
            assert!(plan.is_ok(), "{query}: {plan:?}");
        }

        let subquery = parse_at(
            "case:contains_any(* | fields case)",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap_err();
        assert_eq!(subquery.kind, LogsqlErrorKind::Unsupported);
        assert!(subquery.message.contains("LQL-F38"), "{subquery}");

        for malformed in [
            "contains_any",
            "contains_any(",
            "contains_any(,alpha)",
            "contains_any(alpha beta)",
            "contains_any(alpha*)",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }
    }

    #[test]
    fn session_sixteen_json_array_contains_any_grammar_is_typed_and_strict() {
        for query in [
            "tags:json_array_contains_any(prod, dev)",
            r#"tags:json_array_contains_any("", 123, true, null)"#,
            r#"tags:json_array_contains_any("*")"#,
            "tags:json_array_contains_any()",
            "tags:json_array_contains_any(prod,)",
            "tags:JsOn_ArRaY_CoNtAiNs_AnY(prod)",
            "tags:json_array_contains_any(prod) OR tags:json_array_contains_any(dev)",
            "* | filter tags:json_array_contains_any(prod)",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0).unwrap();
            assert!(plan.spec.metadata_exact.is_empty(), "{query}: {plan:?}");
            assert!(
                plan.spec.predicate.is_some() || !plan.pipeline.is_empty(),
                "{query}: {plan:?}"
            );
        }

        for word_filter in ["json_array_contains_any", "tags:json_array_contains_any"] {
            assert!(
                parse_at(word_filter, TimestampUnit::Microseconds, 0).is_ok(),
                "{word_filter}"
            );
        }

        for malformed in [
            "tags:json_array_contains_any(",
            "tags:json_array_contains_any(,prod)",
            "tags:json_array_contains_any(prod dev)",
            "tags:json_array_contains_any(*)",
            "tags:json_array_contains_any(prod | fields tags)",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }
    }

    #[test]
    fn session_sixteen_ipv4_range_grammar_is_complete_and_strict() {
        assert_eq!(parse_ipv4_address("0.0.0.0"), Some(0));
        assert_eq!(parse_ipv4_address("255.255.255.255"), Some(u32::MAX));
        assert_eq!(parse_ipv4_address("010.000.000.001"), Some(0x0a00_0001));
        assert_eq!(parse_ipv4_address("10.0.0.256"), None);
        assert_eq!(parse_ipv4_address("before 10.0.0.1"), None);
        assert_eq!(
            parse_ipv4_or_cidr("10.0.0.34/24"),
            Some((0x0a00_0000, 0x0a00_00ff))
        );
        assert_eq!(parse_ipv4_or_cidr("10.0.0.34/0"), Some((0, u32::MAX)));
        assert_eq!(parse_ipv4_or_cidr("10.0.0.34/33"), None);

        for query in [
            "ipv4_range(10.0.0.1)",
            "ipv4_range(10.0.0.1, 10.0.0.255)",
            "ip:ipv4_range(10.0.0.0/24)",
            "ip:ipv4_range(10.0.0.34/24)",
            "ip:ipv4_range(10.0.0.1/32)",
            "ip:ipv4_range(10.0.0.1,)",
            r#"ip:ipv4_range("010.000.000.001")"#,
            "service:IpV4_RaNgE(10.0.0.1)",
            "ip:ipv4_range(10.0.0.0/24) AND NOT ip:ipv4_range(10.0.0.128/25)",
            "* | filter nested.ip:ipv4_range(10.0.0.0/24)",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0).unwrap();
            assert!(plan.spec.metadata_exact.is_empty(), "{query}: {plan:?}");
            assert!(
                plan.spec.predicate.is_some() || !plan.pipeline.is_empty(),
                "{query}: {plan:?}"
            );
        }

        for word_filter in ["ipv4_range", "ip:ipv4_range"] {
            assert!(
                parse_at(word_filter, TimestampUnit::Microseconds, 0).is_ok(),
                "{word_filter}"
            );
        }

        for malformed in [
            "ipv4_range(",
            "ipv4_range()",
            "ipv4_range(10.0.0.256)",
            "ipv4_range(10.0.0.1/33)",
            "ipv4_range(10.0.0.1, 10.0.0)",
            "ipv4_range(10.0.0.1, 10.0.0.2, 10.0.0.3)",
            "ipv4_range(10.0.0.1 10.0.0.2)",
            "ipv4_range(10.0.0.1*)",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }
    }

    #[test]
    fn session_sixteen_ipv6_range_grammar_is_complete_and_strict() {
        assert_eq!(parse_ipv6_address("::"), Some([0; 16]));
        assert_eq!(
            parse_ipv6_address("::ffff:1.2.3.4"),
            parse_ipv6_address("1.2.3.4")
        );
        assert_eq!(
            parse_ipv6_address("2001:DB8::1").unwrap()[..4],
            [0x20, 0x01, 0x0d, 0xb8]
        );
        assert_eq!(parse_ipv6_address("2001:db8:::1"), None);
        assert_eq!(parse_ipv6_address("fe80::1%eth0"), None);
        assert_eq!(parse_ipv6_address("01.2.3.4"), None);
        assert_eq!(
            parse_ipv6_or_cidr("2001:db8::34/112"),
            Some((
                [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff,],
            ))
        );
        assert_eq!(parse_ipv6_or_cidr("::1/0"), Some(([0; 16], [u8::MAX; 16])));
        assert_eq!(parse_ipv6_or_cidr("::1/129"), None);
        let (mapped_minimum, mapped_maximum) = parse_ipv6_or_cidr("1.2.3.99/120").unwrap();
        assert_eq!(&mapped_minimum[10..], &[0xff, 0xff, 1, 2, 3, 0]);
        assert_eq!(&mapped_maximum[10..], &[0xff, 0xff, 1, 2, 3, 0xff]);

        for query in [
            "ipv6_range(::1)",
            "ipv6_range(::1, ::2)",
            "ip:ipv6_range(2001:db8::/112)",
            "ip:ipv6_range(2001:db8::34/112,)",
            r#"ip:ipv6_range("2001:DB8::1")"#,
            "ip:ipv6_range(2001:db8::1/128)",
            "ip:ipv6_range(::/0)",
            "ip:ipv6_range(1.2.3.4, 8.0.0.0)",
            "service:IpV6_RaNgE(2001:db8::1)",
            "ip:ipv6_range(2001:db8::/112) AND NOT ip:ipv6_range(2001:db8::8000/113)",
            "* | filter nested.ip:ipv6_range(2001:db8::/112)",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0).unwrap();
            assert!(plan.spec.metadata_exact.is_empty(), "{query}: {plan:?}");
            assert!(
                plan.spec.predicate.is_some() || !plan.pipeline.is_empty(),
                "{query}: {plan:?}"
            );
        }

        for word_filter in ["ipv6_range", "ip:ipv6_range"] {
            assert!(
                parse_at(word_filter, TimestampUnit::Microseconds, 0).is_ok(),
                "{word_filter}"
            );
        }

        for malformed in [
            "ipv6_range(",
            "ipv6_range()",
            "ipv6_range(2001:db8:::1)",
            "ipv6_range(2001:db8::1/129)",
            "ipv6_range(2001:db8::1, 2001:db8::gg)",
            "ipv6_range(::1, ::2, ::3)",
            "ipv6_range(::1 ::2)",
            "ipv6_range(::1*)",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }
    }

    #[test]
    fn session_sixteen_string_range_grammar_is_complete_and_strict() {
        for query in [
            "string_range(alpha, beta)",
            r#"probe:string_range("alpha,one", "beta) two")"#,
            r#"probe:string_range("", beta)"#,
            "probe:StRiNg_RaNgE(alpha, beta,)",
            r#"probe:string_range("é", "ê")"#,
            r#"probe:string_range("*", "**")"#,
            "service:string_range(alpha, beta)",
            "probe:string_range(alpha, beta) AND NOT probe:string_range(alphaz, beta)",
            "* | filter nested.probe:string_range(alpha, beta)",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0).unwrap();
            assert!(plan.spec.metadata_exact.is_empty(), "{query}: {plan:?}");
            assert!(
                plan.spec.predicate.is_some() || !plan.pipeline.is_empty(),
                "{query}: {plan:?}"
            );
        }

        for word_filter in ["string_range", "probe:string_range"] {
            assert!(
                parse_at(word_filter, TimestampUnit::Microseconds, 0).is_ok(),
                "{word_filter}"
            );
        }

        for malformed in [
            "string_range(",
            "string_range()",
            "string_range(alpha)",
            "string_range(alpha, beta, gamma)",
            "string_range(alpha beta)",
            "string_range(alpha*, beta)",
            "string_range(, beta)",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }
    }

    #[test]
    fn session_sixteen_len_range_grammar_is_complete_and_strict() {
        for query in [
            "len_range(1, 2)",
            r#"probe:LeN_RaNgE("5", 0b110,)"#,
            "probe:len_range(0x5, 1_0)",
            "probe:len_range(5B, 6B)",
            "probe:len_range(5ns, +Inf)",
            "probe:len_range(18446744073709551616, inf)",
            "service:len_range(5, 6)",
            "probe:len_range(5, 6) AND NOT probe:len_range(6, 6)",
            "* | filter nested.probe:len_range(5, 6)",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0).unwrap();
            assert!(
                format!("{:?}", plan.spec.predicate).contains("LenRange")
                    || format!("{:?}", plan.pipeline).contains("LenRange"),
                "{query}: {plan:?}"
            );
        }

        for word_filter in ["len_range", "probe:len_range"] {
            assert!(
                parse_at(word_filter, TimestampUnit::Microseconds, 0).is_ok(),
                "{word_filter}"
            );
        }

        for malformed in [
            "len_range(",
            "len_range()",
            "len_range(1)",
            "len_range(1, 2, 3)",
            "len_range(foo, bar)",
            "len_range(1, bar)",
            "len_range(-1, 2)",
            "len_range(1.2, 3.4)",
            "len_range(1, 2",
            "len_range(1 2)",
            "len_range(1,,2)",
            "len_range(08, 9)",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }

        let bounds = |query: &str| {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0).unwrap();
            match plan.spec.predicate.unwrap() {
                LogPredicate::LenRange {
                    minimum, maximum, ..
                } => (minimum, maximum),
                predicate => panic!("{query}: unexpected predicate {predicate:?}"),
            }
        };
        assert_eq!(bounds("len_range(0x10, 0b100101)"), (16, 37));
        assert_eq!(bounds("len_range(010, 010)"), (8, 8));
        assert_eq!(bounds("len_range(1.5KB, 22MB100KB)"), (1_500, 22_100_000));
        assert_eq!(
            bounds("len_range(1h5m35s, +Inf)"),
            (3_935_000_000_000, u64::MAX)
        );
        assert_eq!(
            bounds("len_range(18446744073709551616, inf)"),
            (i64::MAX as u64, u64::MAX),
            "VictoriaLogs saturates a human-readable bound after integer overflow"
        );
        assert_eq!(bounds("len_range(-0s, -0B)"), (0, 0));
    }

    #[test]
    fn session_sixteen_field_comparison_grammar_is_complete_and_strict() {
        for query in [
            "eq_field(peer)",
            r#""left field":Eq_FiElD('right field')"#,
            "left:le_field(right,)",
            "left:Lt_FiElD(right)",
            "service:eq_field(peer)",
            "left:eq_field(right) AND NOT left:lt_field(right)",
            "* | filter nested.left:le_field(nested.right)",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0).unwrap();
            assert!(
                format!("{:?}", plan.spec.predicate).contains("FieldCompare")
                    || format!("{:?}", plan.pipeline).contains("FieldCompare"),
                "{query}: {plan:?}"
            );
        }

        for word_filter in [
            "eq_field",
            "left:eq_field",
            "le_field",
            "left:le_field",
            "lt_field",
            "left:lt_field",
        ] {
            assert!(
                parse_at(word_filter, TimestampUnit::Microseconds, 0).is_ok(),
                "{word_filter}"
            );
        }

        for malformed in [
            "eq_field(",
            "eq_field()",
            "eq_field(left, right)",
            "eq_field(left right)",
            "eq_field(*)",
            "eq_field(left*)",
            "le_field(",
            "le_field()",
            "le_field(left, right)",
            "le_field(left right)",
            "lt_field(",
            "lt_field()",
            "lt_field(left, right)",
            "lt_field(left right)",
            "_time:eq_field(_time)",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }

        let eq = |left, right| logsql_field_comparison(left, right, FieldCompareOp::Equal);
        let le = |left, right| logsql_field_comparison(left, right, FieldCompareOp::LessOrEqual);
        let lt = |left, right| logsql_field_comparison(left, right, FieldCompareOp::Less);
        assert!(eq("2", "2"));
        assert!(!eq("2", "2.0"));
        assert!(lt("2", "10"), "both numeric projections use numeric order");
        assert!(!le("10", "2"));
        assert!(lt("500ms", "1s"));
        assert!(lt("1000B", "1KiB"));
        assert!(lt("2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z"));
        assert!(lt("10.0.0.2", "10.0.0.10"));
        assert!(
            lt("0b1111", "0x10"),
            "base-zero integers compare numerically"
        );
        assert!(
            lt("10x", "2"),
            "one failed numeric parse selects byte order"
        );
        assert!(
            lt("+Inf", "NaN"),
            "NaN selects byte order instead of float order"
        );
    }

    #[test]
    fn session_sixteen_field_prefix_grammar_is_complete_and_strict() {
        for query in [
            "cmp_*:alpha",
            "\"cmp_\"*:exact(alpha)",
            "*:string_range(alpha, beta)",
            "\"\"*:value_type(uint64)",
            "cmp_*:(bar AND foo)",
            "* | filter nested.*:alpha",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0).unwrap();
            assert!(
                format!("{plan:?}").contains("FieldPrefix"),
                "{query}: {plan:?}"
            );
        }

        for malformed in [
            "cmp_*:",
            "cmp_*:()",
            "cmp_*:(alpha",
            "cmp_**:alpha",
            "\"cmp_*:alpha",
            "cmp_*:eq_field(cmp_right)",
            "cmp_*:le_field(cmp_right)",
            "cmp_*:lt_field(cmp_right)",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed}");
        }
    }

    #[test]
    fn session_sixteen_day_range_grammar_is_complete_and_strict() {
        for query in [
            "_time:day_range[10:00, 12:00] offset 0h",
            "_time:day_range(10:00, 12:00) offset 2h",
            "_time:DAY_RANGE[1000, 1200] offset -2h",
            "_time:day_range[10:60, 12:00] offset 1h30m",
            "_time:day_range[00:00, 24:00]",
            "_time:day_range[10:00, 12:00] offset 0h AND level:=error",
            "* | filter _time:day_range[10:00, 12:00] offset 0h",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0)
                .unwrap_or_else(|error| panic!("{query}: {error:?}"));
            assert!(
                format!("{plan:?}").contains("DayRange"),
                "{query}: {plan:?}"
            );
        }

        for malformed in [
            "_time:day_range",
            "_time:day_range[foo, 12:00]",
            "_time:day_range[10:00, bar]",
            "_time:day_range[25:00, 26:00]",
            "_time:day_range[10:61, 12:00]",
            "_time:day_range[10:00, 12:00",
            "_time:day_range[10:00, 12:00] offset",
            "_time:day_range[10:00, 12:00] offset nope",
        ] {
            assert!(
                parse_at(malformed, TimestampUnit::Microseconds, 0).is_err(),
                "{malformed}"
            );
        }
    }

    #[test]
    fn session_sixteen_week_range_grammar_is_complete_and_strict() {
        for query in [
            "_time:week_range[Mon, Fri] offset 0h",
            "_time:week_range(Sun, Sat) offset 2h",
            "_time:WEEK_RANGE[Monday, Friday] offset -2h",
            "_time:week_range[Sun, Sun) offset 1h30m",
            "_time:week_range[Mon, Fri]",
            "_time:week_range[Mon, Fri] offset 0h AND level:=error",
            "* | filter _time:week_range[Mon, Fri] offset 0h",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0)
                .unwrap_or_else(|error| panic!("{query}: {error:?}"));
            assert!(
                format!("{plan:?}").contains("WeekRange"),
                "{query}: {plan:?}"
            );
        }

        for malformed in [
            "_time:week_range",
            "_time:week_range[foo, Fri]",
            "_time:week_range[Mon, bar]",
            "_time:week_range[mom, Wed]",
            "_time:week_range[Mon Fri]",
            "_time:week_range[Mon, Fri",
            "_time:week_range[Mon, Fri] offset",
            "_time:week_range[Mon, Fri] offset nope",
        ] {
            assert!(
                parse_at(malformed, TimestampUnit::Microseconds, 0).is_err(),
                "{malformed}"
            );
        }
    }

    #[test]
    fn session_sixteen_comments_multiline_semicolons_and_locations_are_strict() {
        for query in [
            "case:=\"word-exact\" # ignored",
            "# leading\ncase:=\"word-exact\"",
            "case:=\"word-exact\"#attached",
            "(case:=\"word-exact\" OR\n case:=\"word-case\")",
            "case:=\"word-exact\" |\n # projected below\n fields case",
            "comment_group:=\"comments\" \"hash#inside\"",
            "comment_group:=\"comments\" 'hash#inside'",
            "comment_group:=\"comments\" `hash#inside`",
            "\"hash#field\":=\"hash#value\"",
            "# windows\r\ncase:=\"word-exact\"",
            "case:=\"word-exact\";",
            "case:=\"word-exact\"; # finished",
            "\"hash;inside\"",
        ] {
            parse_at(query, TimestampUnit::Microseconds, 0)
                .unwrap_or_else(|error| panic!("{query:?}: {error:?}"));
        }

        for malformed in [
            "# no query",
            "case:=\"word-exact\" | # no pipe",
            ";",
            "case:=\"word-exact\";;",
            "case:=\"word-exact\"; # first query ended\ncase:=\"word-case\"",
            "case:=\"word-exact\" | fields # missing field",
        ] {
            assert!(
                parse_at(malformed, TimestampUnit::Microseconds, 0).is_err(),
                "{malformed:?}"
            );
        }

        let quote = parse_at(
            "# first line\n  \"hash#inside",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap_err();
        assert!(quote.message.contains("line 2, column 3"), "{quote:?}");

        let semicolon = parse_at(
            "case:=\"word-exact\";\n  case:=\"word-case\"",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap_err();
        assert!(
            semicolon.message.contains("line 1, column 19"),
            "{semicolon:?}"
        );

        assert!(matches!(
            prepare_query_layout("case:=\"word-exact\""),
            Ok(Cow::Borrowed(_))
        ));
    }

    #[test]
    fn session_seventeen_delete_pipe_grammar_is_complete_and_strict() {
        for query in [
            "* | delete foo",
            "* | del foo",
            "* | drop foo",
            "* | rm foo",
            "* | DELETE foo, bar*",
            "* | delete \"foo,bar\", \"foo|bar\", \"foo.\"*",
            "* | delete \"\"",
            "* | delete *",
            "* | fields foo, bar | delete foo | filter bar:*",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0)
                .unwrap_or_else(|error| panic!("{query:?}: {error:?}"));
            assert_eq!(plan.output, LogsqlOutput::Pipeline, "{query:?}");
        }

        for malformed in [
            "* | delete",
            "* | delete foo,",
            "* | delete , foo",
            "* | delete foo,,bar",
            "* | delete foo bar",
            "* | delete foo *",
            "* | delete *foo",
            "* | delete foo*bar",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed:?}");
        }
    }

    #[test]
    fn session_seventeen_query_stats_grammar_is_complete_and_strict() {
        for query in [
            "* | query_stats",
            "* | QUERY_STATS",
            "* | limit 1 | query_stats | keep RowsFound",
            "* | fields case | query_stats | keep RowsFound, ValuesRead",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0)
                .unwrap_or_else(|error| panic!("{query:?}: {error:?}"));
            assert_eq!(plan.output, LogsqlOutput::Pipeline, "{query:?}");
            assert!(
                plan.pipeline
                    .iter()
                    .any(|operation| matches!(operation, PipelineOp::QueryStats)),
                "{query:?}"
            );
            assert_eq!(plan.implicit_result_limit, None, "{query:?}");
        }

        for malformed in ["* | query_stats x", "* | query_stats 0"] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed:?}");
            assert_eq!(error.message, "LogsQL query_stats accepts no arguments");
        }
    }

    #[test]
    fn session_seventeen_first_grammar_is_complete_and_strict() {
        for query in [
            "* | first",
            "* | FIRST 2 (n DeSc, case)",
            "* | first by (n asc, case desc)",
            "* | first by ()",
            "* | first by (case,)",
            "* | first 2 by (n, case) partition by (group, zone) rank as position",
            "* | first partition (group) rank",
            "* | fields case, n | first by (n) | keep case",
            r#"* | first by ("field name") partition by ('group name') rank as `row rank`"#,
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0)
                .unwrap_or_else(|error| panic!("{query:?}: {error:?}"));
            assert_eq!(plan.output, LogsqlOutput::Pipeline, "{query:?}");
            assert_eq!(plan.implicit_result_limit, None, "{query:?}");
        }

        let plan = parse_at(
            "* | first 2 by (n desc, case) partition by (group) rank as position",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        let [PipelineOp::First(spec)] = plan.pipeline.as_slice() else {
            panic!("unexpected first plan: {plan:?}");
        };
        assert_eq!(spec.limit, 2);
        assert_eq!(spec.by_fields.len(), 2);
        assert!(spec.by_fields[0].descending);
        assert!(!spec.by_fields[1].descending);
        assert_eq!(spec.partition_by.len(), 1);
        assert!(matches!(
            &spec.rank_field,
            Some(PipelineField::Exact { name, .. }) if name == "position"
        ));

        for malformed in [
            "* | first 0",
            "* | first nope",
            "* | first by",
            "* | first by (case*)",
            "* | first by (,case)",
            "* | first partition by",
            "* | first partition by (*)",
            "* | first rank as",
            "* | first by (case) trailing",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed:?}");
        }
    }

    #[test]
    fn session_seventeen_last_grammar_is_complete_and_strict() {
        for query in [
            "* | last",
            "* | LAST 2 (n DeSc, case)",
            "* | last by (n asc, case desc)",
            "* | last by ()",
            "* | last by (case,)",
            "* | last 2 by (n, case) partition by (group, zone) rank as position",
            "* | last partition (group) rank",
            "* | fields case, n | last by (n) | keep case",
            r#"* | last by ("field name") partition by ('group name') rank as `row rank`"#,
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0)
                .unwrap_or_else(|error| panic!("{query:?}: {error:?}"));
            assert_eq!(plan.output, LogsqlOutput::Pipeline, "{query:?}");
            assert_eq!(plan.implicit_result_limit, None, "{query:?}");
        }

        for malformed in [
            "* | last 0",
            "* | last nope",
            "* | last by",
            "* | last by (case*)",
            "* | last partition by",
            "* | last partition by (*)",
            "* | last rank as",
            "* | last by (case) trailing",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed:?}");
        }
    }

    #[test]
    fn session_seventeen_top_grammar_is_complete_and_strict() {
        for query in [
            "* | top by (service)",
            "* | TOP 2 BY (service, level) HITS AS total RANK AS position",
            "* | top service",
            "* | top 5 service, level hits total rank",
            "* | top 5 service hits as hits rank as position",
            "* | top by (service,) rank as position hits as total",
            r#"* | top by ("field name", 'other field') hits as `hit count` rank as "row rank""#,
            "* | fields service, level | top 2 by (service) | keep service, hits",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0)
                .unwrap_or_else(|error| panic!("{query:?}: {error:?}"));
            assert_eq!(plan.output, LogsqlOutput::Pipeline, "{query:?}");
            assert_eq!(plan.implicit_result_limit, None, "{query:?}");
        }

        let plan = parse_at(
            "* | top 2 by (service, level) hits as total rank as position",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        let [PipelineOp::Top(spec)] = plan.pipeline.as_slice() else {
            panic!("unexpected top plan: {plan:?}");
        };
        assert_eq!(spec.limit, 2);
        assert_eq!(spec.by_fields.len(), 2);
        assert_eq!(spec.hits_field, "total");
        assert_eq!(spec.rank_field.as_deref(), Some("position"));

        let collisions = parse_at(
            "* | top by (hits, rank) rank",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        let [PipelineOp::Top(spec)] = collisions.pipeline.as_slice() else {
            panic!("unexpected collision plan: {collisions:?}");
        };
        assert_eq!(spec.hits_field, "hitss");
        assert_eq!(spec.rank_field.as_deref(), Some("ranks"));

        for malformed in [
            "* | top",
            "* | top 0 by (case)",
            "* | top -1 by (case)",
            "* | top 1.5 by (case)",
            "* | top nope by (case)",
            "* | top by",
            "* | top by ()",
            "* | top by (case*)",
            "* | top case level",
            "* | top case,",
            "* | top by (case) hits",
            "* | top by (case) rank as",
            "* | top by (case) trailing",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed:?}");
        }
    }

    #[test]
    fn session_seventeen_uniq_grammar_is_complete_and_strict() {
        for query in [
            "* | uniq by (service)",
            "* | UNIQ BY (service, level) WITH HITS LIMIT 10",
            "* | uniq service",
            "* | uniq service, level hits",
            "* | uniq (service) filter web with hits limit 2",
            r#"* | uniq by ("field name") filter "two words" hits"#,
            "* | uniq by (service, level) filter \"\"",
            "* | fields service, level | uniq service hits | keep service, hits",
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 0)
                .unwrap_or_else(|error| panic!("{query:?}: {error:?}"));
            assert_eq!(plan.output, LogsqlOutput::Pipeline, "{query:?}");
            assert_eq!(plan.implicit_result_limit, None, "{query:?}");
        }

        let plan = parse_at(
            "* | uniq by (service, level) with hits limit 2",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        let [PipelineOp::Uniq(spec)] = plan.pipeline.as_slice() else {
            panic!("unexpected uniq plan: {plan:?}");
        };
        assert_eq!(spec.by_fields.len(), 2);
        assert_eq!(spec.filter, None);
        assert_eq!(spec.hits_field.as_deref(), Some("hits"));
        assert_eq!(spec.limit, Some(2));

        let collision = parse_at(
            "* | uniq by (hits) hits limit 0",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        let [PipelineOp::Uniq(spec)] = collision.pipeline.as_slice() else {
            panic!("unexpected collision plan: {collision:?}");
        };
        assert_eq!(spec.hits_field.as_deref(), Some("hitss"));
        assert_eq!(spec.limit, Some(0));

        for malformed in [
            "* | uniq",
            "* | uniq hits",
            "* | uniq by",
            "* | uniq by ()",
            "* | uniq by (case*)",
            "* | uniq case level",
            "* | uniq case,",
            "* | uniq by (case) filter",
            "* | uniq by (case, level) filter x",
            "* | uniq by (case) with",
            "* | uniq by (case) limit",
            "* | uniq by (case) limit -1",
            "* | uniq by (case) limit nope",
            "* | uniq by (case) with hits trailing",
        ] {
            let error = parse_at(malformed, TimestampUnit::Microseconds, 0).unwrap_err();
            assert_eq!(error.kind, LogsqlErrorKind::Malformed, "{malformed:?}");
        }
    }

    #[test]
    fn session_seventeen_first_sort_order_matches_victorialogs_coercions() {
        let less = |left, right| logsql_sort_comparison(left, right) == Ordering::Less;
        assert!(less("-2", "0"));
        assert!(less("2", "10"));
        assert!(less("9007199254740992", "9007199254740993"));
        assert!(less("500ms", "1s"));
        assert!(less("1000B", "1KiB"));
        assert!(less("2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z"));
        assert!(less("file2", "file10"));
        assert!(less("2x", "alpha"));
        assert!(less("é2", "é10"));
        assert_eq!(logsql_sort_comparison("same", "same"), Ordering::Equal);
    }

    #[test]
    fn logs_ql_literals_decode_escapes_and_quoted_field_identifiers() {
        let double = parse_at(
            r#""line one\nline two\t\"quoted\"\\slash""#,
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(
            double.spec.message_phrase.as_deref(),
            Some("line one\nline two\t\"quoted\"\\slash")
        );

        let single = parse_at(
            r#"'single\'quote \x41 \u03bb'"#,
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(
            single.spec.message_phrase.as_deref(),
            Some("single'quote A λ")
        );

        let raw = parse_at(
            r#"`raw\n"double"'single\slash`"#,
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(
            raw.spec.message_phrase.as_deref(),
            Some(r#"raw\n"double"'single\slash"#)
        );

        let pipe = parse_at(r#""left|right""#, TimestampUnit::Microseconds, 0).unwrap();
        assert_eq!(pipe.spec.message_phrase.as_deref(), Some("left|right"));

        let field = parse_at(r#""log:level":="error""#, TimestampUnit::Microseconds, 0).unwrap();
        assert_eq!(
            field.spec.metadata_exact,
            vec![crate::MetadataExact {
                path: vec!["log:level".into()],
                expected: json!("error"),
            }]
        );

        for malformed in [r#""bad\q""#, r#"'bad\q'"#, r#""bad\uD800""#] {
            assert!(
                parse_at(malformed, TimestampUnit::Microseconds, 0).is_err(),
                "{malformed:?} was accepted"
            );
        }
    }

    #[test]
    fn logs_ql_pagination_sort_and_count_pipes_are_strict() {
        let asc = parse_at(
            "* | sort by (_time) asc | offset 2 | limit 3",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert!(!asc.spec.descending);
        assert_eq!(asc.spec.offset, 2);
        assert_eq!(asc.spec.limit, 3);
        assert_eq!(asc.output, LogsqlOutput::Rows);

        let aliases = parse_at(
            "* | order by (_time) desc | skip 4 | head 5",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert!(aliases.spec.descending);
        assert_eq!(aliases.spec.offset, 4);
        assert_eq!(aliases.spec.limit, 5);

        let default_head = parse_at("* | head", TimestampUnit::Microseconds, 0).unwrap();
        assert_eq!(default_head.spec.limit, 10);

        let count = parse_at(
            "level:error | stats count() as total",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(count.output, LogsqlOutput::Count);

        let custom_count = parse_at(
            "level:error | stats count() as other",
            TimestampUnit::Microseconds,
            0,
        )
        .unwrap();
        assert_eq!(custom_count.output, LogsqlOutput::Pipeline);

        for invalid in [
            "* | offset -1",
            "* | sort by (service) asc",
            "* | sort by (_time) sideways",
            "* | limit 2 | offset 1",
        ] {
            assert!(
                parse_at(invalid, TimestampUnit::Microseconds, 0).is_err(),
                "{invalid:?} was accepted"
            );
        }
    }

    #[test]
    fn session_thirteen_plans_typed_discovery_projection_filters_and_stats() {
        for query in [
            r#"* | field_values probe filter val limit 10"#,
            r#"* | field_names filter pro as field"#,
            r#"* | fields _time, _msg, level, nested.leaf, service*"#,
            r#"* | keep case, probe | where probe:*"#,
            r#"* | stats count(probe) as present, count_empty(probe) as empty"#,
            r#"* | stats count_uniq(probe) limit 10 as exact, count_uniq_hash(probe) as hashed"#,
            r#"* | stats uniq_values(probe) limit 10 as unique, values(probe) as values"#,
            r#"* | stats sum(n) as sum, avg(n) as avg, min(n) as min, max(n) as max, median(n) as median"#,
            r#"_time:1h | stats rate() as rate, rate_sum(n) as rate_sum"#,
        ] {
            let plan = parse_at(query, TimestampUnit::Microseconds, 1_800_000_000_000_000);
            assert!(plan.is_ok(), "{query}: {plan:?}");
            assert_eq!(plan.unwrap().output, LogsqlOutput::Pipeline, "{query}");
        }

        for malformed in [
            "* | field_values",
            "* | field_values *",
            "* | field_names filter",
            "* | fields",
            "* | filter",
            "* | stats count_uniq()",
            "* | stats count_uniq(pro*)",
            "* | stats rate(n)",
            "* | stats sum(n) limit 2",
            "* | stats sum(n) as value, avg(n) as value",
        ] {
            assert!(
                parse_at(malformed, TimestampUnit::Microseconds, 0).is_err(),
                "{malformed:?} was accepted"
            );
        }
    }
}
