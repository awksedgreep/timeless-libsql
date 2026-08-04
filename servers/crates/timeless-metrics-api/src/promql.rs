//! Storage-independent PromQL syntax parsing.
//!
//! PromQL remains a Rust API concern: the SQLite extension never sees language
//! syntax.  The parser produces a complete AST, while `query` deliberately
//! lowers only matrix rows whose semantics have passed the pinned Prometheus
//! oracle.  A syntactically valid but unshipped expression is therefore an
//! explicit error rather than an empty result or a fallback execution path.

pub(crate) use promql_parser::label::MatchOp;
pub(crate) use promql_parser::parser::{Expr, VectorSelector};

pub(crate) fn parse(input: &str) -> Result<Expr, String> {
    promql_parser::parser::parse(input)
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
        assert!(matches!(
            parse(r#""hello\nworld""#).unwrap(),
            Expr::StringLiteral(_)
        ));
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
        for query in ["avg_over_time(cpu)", "cpu[0s]", "cpu +", "{host="] {
            assert!(parse(query).is_err(), "{query} unexpectedly parsed");
        }
    }
}
