//! Storage-independent PromQL syntax parsing.
//!
//! PromQL remains a Rust API concern: the SQLite extension never sees language
//! syntax.  The parser produces a complete AST, while `query` deliberately
//! lowers only matrix rows whose semantics have passed the pinned Prometheus
//! oracle.  A syntactically valid but unshipped expression is therefore an
//! explicit error rather than an empty result or a fallback execution path.

pub(crate) use promql_parser::label::MatchOp;
pub(crate) use promql_parser::parser::{AtModifier, Expr, NumberLiteral, Offset, VectorSelector};

pub(crate) fn parse(input: &str) -> Result<Expr, String> {
    if let Some(number) = parse_underscored_root_number(input)? {
        return Ok(Expr::NumberLiteral(NumberLiteral::new(number)));
    }
    promql_parser::parser::parse(input)
}

fn parse_underscored_root_number(input: &str) -> Result<Option<f64>, String> {
    let value = input.trim();
    if !value.contains('_') {
        return Ok(None);
    }
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let starts_numeric = unsigned
        .as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_digit())
        || unsigned.starts_with('.')
            && unsigned
                .as_bytes()
                .get(1)
                .is_some_and(|byte| byte.is_ascii_digit());
    if !starts_numeric {
        return Ok(None);
    }

    // This compatibility shim recognizes one complete root literal. An
    // underscore later in a metric name (for example `20 - metric_name`)
    // belongs to the ordinary PromQL parser, not to this numeric token.
    if !value.bytes().all(|byte| {
        byte.is_ascii_hexdigit()
            || matches!(byte, b'x' | b'X' | b'e' | b'E' | b'_' | b'.' | b'+' | b'-')
    }) {
        return Ok(None);
    }

    let bytes = value.as_bytes();
    let hexadecimal = unsigned.starts_with("0x") || unsigned.starts_with("0X");
    for (index, byte) in bytes.iter().enumerate() {
        if *byte != b'_' {
            continue;
        }
        let previous = index.checked_sub(1).and_then(|at| bytes.get(at)).copied();
        let next = bytes.get(index + 1).copied();
        let digit = |candidate: u8| {
            if hexadecimal {
                candidate.is_ascii_hexdigit()
            } else {
                candidate.is_ascii_digit()
            }
        };
        let follows_prefix = hexadecimal && matches!(previous, Some(b'x' | b'X'));
        if !previous.is_some_and(digit) && !follows_prefix || !next.is_some_and(digit) {
            return Err(format!(
                "invalid underscore in numeric literal at byte {index}"
            ));
        }
    }
    let compact = value.replace('_', "");
    promql_parser::util::parse_str_radix(&compact)
        .map(Some)
        .map_err(|error| format!("invalid numeric literal: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parses_selectors_range_values_and_complete_durations() {
        let Expr::VectorSelector(selector) =
            parse(r#"cpu_usage{host="web-1",env!="dev",zone=~"us-.*"}"#).unwrap()
        else {
            panic!("vector selector expected")
        };
        assert_eq!(selector.name.as_deref(), Some("cpu_usage"));
        assert_eq!(selector.matchers.matchers.len(), 3);

        let Expr::MatrixSelector(selector) = parse("cpu_usage[1h30m250ms]").unwrap() else {
            panic!("matrix selector expected")
        };
        assert_eq!(selector.range, Duration::from_millis(5_400_250));

        assert!(matches!(parse("NaN").unwrap(), Expr::NumberLiteral(_)));
        let Expr::NumberLiteral(number) = parse("00_1_23_4.56_7_8").unwrap() else {
            panic!("number literal expected")
        };
        assert_eq!(number.val, 1234.5678);
        let Expr::NumberLiteral(number) = parse("0x_1_2").unwrap() else {
            panic!("number literal expected")
        };
        assert_eq!(number.val, 18.0);
        assert!(matches!(
            parse(r#""hello\nworld""#).unwrap(),
            Expr::StringLiteral(_)
        ));
    }

    #[test]
    fn root_underscored_number_shim_does_not_capture_binary_metric_names() {
        let expression = parse("20 - arithmetic_lhs").unwrap();
        assert!(matches!(expression, Expr::Binary(_)));
        assert!(parse("1__0").is_err());
    }

    #[test]
    fn preserves_duplicate_matchers() {
        let Expr::VectorSelector(selector) =
            parse(r#"{__name__="cpu",host="nope",host="web-1"}"#).unwrap()
        else {
            panic!("vector selector expected")
        };
        assert_eq!(selector.matchers.matchers.len(), 3);
    }

    #[test]
    fn malformed_promql_is_rejected_by_the_parser() {
        for query in [
            "avg_over_time(cpu)",
            "cpu[0s]",
            "cpu +",
            "{host=",
            "1__2",
            "1_",
            "1._2",
        ] {
            assert!(parse(query).is_err(), "{query} unexpectedly parsed");
        }
    }
}
