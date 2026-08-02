//! Storage-independent parsing for the deliberately small Session 4 PromQL slice.
//!
//! This module knows nothing about HTTP, SQLite, or the metrics extension.  It
//! produces an explicit plan which the query layer may lower onto public
//! extension surfaces.  A syntactically valid expression outside this slice is
//! rejected instead of being mistaken for an empty result.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MatcherOp {
    Eq,
    NotEq,
    Regex,
    NotRegex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Matcher {
    pub key: String,
    pub op: MatcherOp,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Selector {
    pub metric: String,
    pub matchers: Vec<Matcher>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Plan {
    Selector(Selector),
    AvgOverTime { selector: Selector, window: i64 },
}

pub(crate) fn parse(input: &str) -> Result<Plan, String> {
    Parser::new(input).parse()
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse(mut self) -> Result<Plan, String> {
        self.skip_ws();
        if self.input[self.pos..]
            .strip_prefix("avg_over_time")
            .is_some_and(|rest| rest.trim_start().starts_with('('))
        {
            let start = self.pos;
            let function = self.identifier(true)?;
            if function != "avg_over_time" {
                return self.unsupported(start);
            }
            self.skip_ws();
            self.expect(b'(')?;
            let selector = self.selector()?;
            self.skip_ws();
            self.expect(b'[')?;
            let window = self.duration()?;
            if window <= 0 {
                return Err("avg_over_time() requires a non-zero range window".into());
            }
            self.skip_ws();
            self.expect(b']')?;
            self.skip_ws();
            self.expect(b')')?;
            self.finish()?;
            Ok(Plan::AvgOverTime { selector, window })
        } else {
            let selector = self.selector()?;
            self.finish()?;
            Ok(Plan::Selector(selector))
        }
    }

    fn selector(&mut self) -> Result<Selector, String> {
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

        let name_positions: Vec<_> = matchers
            .iter()
            .enumerate()
            .filter_map(|(index, matcher)| (matcher.key == "__name__").then_some(index))
            .collect();
        if metric.is_some() && !name_positions.is_empty() {
            return Err("metric name specified twice".into());
        }
        if name_positions.len() > 1 {
            return Err("multiple __name__ matchers are outside the Session 4 PromQL slice".into());
        }
        let metric = match (metric, name_positions.first().copied()) {
            (Some(metric), None) => metric,
            (None, Some(index)) if matchers[index].op == MatcherOp::Eq => {
                matchers.remove(index).value
            }
            (None, Some(_)) => {
                return Err(
                    "regex and negative __name__ matchers are outside the Session 4 PromQL slice"
                        .into(),
                )
            }
            (None, None) => return Err(
                "a metric name or exact __name__ matcher is required by the Session 4 PromQL slice"
                    .into(),
            ),
            (Some(_), Some(_)) => unreachable!("duplicate metric rejected above"),
        };
        Ok(Selector { metric, matchers })
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
            let op = self.matcher_op()?;
            self.skip_ws();
            let value = self.quoted_string()?;
            matchers.push(Matcher { key, op, value });
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(matchers);
                }
                _ => return Err(format!("expected ',' or '}}' at byte {}", self.pos)),
            }
        }
    }

    fn matcher_op(&mut self) -> Result<MatcherOp, String> {
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

    fn duration(&mut self) -> Result<i64, String> {
        self.skip_ws();
        let start = self.pos;
        let mut total = 0_i64;
        let mut components = 0;
        while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            let number_start = self.pos;
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                self.pos += 1;
            }
            let number = self.input[number_start..self.pos]
                .parse::<i64>()
                .map_err(|_| format!("invalid duration at byte {start}"))?;
            let unit = self
                .peek()
                .ok_or_else(|| format!("duration requires a unit at byte {}", self.pos))?;
            let multiplier = match unit {
                b's' => 1,
                b'm' => 60,
                b'h' => 3_600,
                b'd' => 86_400,
                b'w' => 604_800,
                b'y' => 31_536_000,
                _ => return Err(format!("invalid duration unit at byte {}", self.pos)),
            };
            self.pos += 1;
            total = total
                .checked_add(
                    number
                        .checked_mul(multiplier)
                        .ok_or_else(|| "duration overflow".to_string())?,
                )
                .ok_or_else(|| "duration overflow".to_string())?;
            components += 1;
        }
        if components == 0 {
            Err(format!("expected duration at byte {start}"))
        } else {
            Ok(total)
        }
    }

    fn identifier(&mut self, metric: bool) -> Result<String, String> {
        let start = self.pos;
        let first = self
            .peek()
            .ok_or_else(|| format!("expected identifier at byte {}", self.pos))?;
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

    fn quoted_string(&mut self) -> Result<String, String> {
        let quote = self
            .peek()
            .filter(|byte| matches!(byte, b'"' | b'\''))
            .ok_or_else(|| format!("expected quoted string at byte {}", self.pos))?;
        self.pos += 1;
        let mut output = String::new();
        loop {
            let byte = self
                .peek()
                .ok_or_else(|| "unterminated matcher string".to_string())?;
            self.pos += 1;
            match byte {
                value if value == quote => return Ok(output),
                b'\\' => {
                    let escaped = self
                        .peek()
                        .ok_or_else(|| "unterminated matcher escape".to_string())?;
                    self.pos += 1;
                    output.push(match escaped {
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'\\' => '\\',
                        b'"' => '"',
                        b'\'' => '\'',
                        other => other as char,
                    });
                }
                value if value.is_ascii() => output.push(value as char),
                _ => {
                    let tail = &self.input[self.pos - 1..];
                    let ch = tail
                        .chars()
                        .next()
                        .ok_or_else(|| "invalid UTF-8 matcher string".to_string())?;
                    self.pos += ch.len_utf8() - 1;
                    output.push(ch);
                }
            }
        }
    }

    fn finish(&mut self) -> Result<(), String> {
        self.skip_ws();
        if self.pos == self.input.len() {
            Ok(())
        } else {
            self.unsupported(self.pos)
        }
    }

    fn unsupported<T>(&self, at: usize) -> Result<T, String> {
        let suffix = self.input[at..].trim();
        Err(format!(
            "unsupported PromQL expression in Session 4 near {suffix:?}; supported forms are metric{{...}} and avg_over_time(metric{{...}}[window])"
        ))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_pinned_selector_and_window_forms() {
        assert_eq!(
            parse(r#"cpu_usage{host="web-1",env!="dev",zone=~"us-.*"}"#).unwrap(),
            Plan::Selector(Selector {
                metric: "cpu_usage".into(),
                matchers: vec![
                    Matcher {
                        key: "host".into(),
                        op: MatcherOp::Eq,
                        value: "web-1".into(),
                    },
                    Matcher {
                        key: "env".into(),
                        op: MatcherOp::NotEq,
                        value: "dev".into(),
                    },
                    Matcher {
                        key: "zone".into(),
                        op: MatcherOp::Regex,
                        value: "us-.*".into(),
                    },
                ],
            })
        );
        assert_eq!(
            parse("avg_over_time(cpu_usage[1h30m])").unwrap(),
            Plan::AvgOverTime {
                selector: Selector {
                    metric: "cpu_usage".into(),
                    matchers: vec![],
                },
                window: 5_400,
            }
        );
        assert!(matches!(
            parse("avg_over_time{host=\"metric-name\"}").unwrap(),
            Plan::Selector(_)
        ));
    }

    #[test]
    fn preserves_duplicate_matchers_and_exact_name_form() {
        let Plan::Selector(selector) =
            parse(r#"{__name__="cpu",host="nope",host="web-1"}"#).unwrap()
        else {
            panic!("selector plan expected")
        };
        assert_eq!(selector.metric, "cpu");
        assert_eq!(selector.matchers.len(), 2);
    }

    #[test]
    fn unsupported_constructs_are_errors_not_empty_plans() {
        for query in [
            "rate(cpu[5m])",
            "sum(cpu)",
            "cpu + mem",
            "avg_over_time(cpu)",
            "avg_over_time(cpu[0s])",
        ] {
            assert!(parse(query).is_err(), "{query} unexpectedly parsed");
        }
    }
}
