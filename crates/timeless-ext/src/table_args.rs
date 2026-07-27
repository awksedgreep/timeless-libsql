//! CREATE VIRTUAL TABLE argument parsing shared by all three modules
//! (F2, FEATURE_PLAN.md). SQLite hands xCreate the raw comma-separated
//! argument bytes; each argument here is `name=value` with optional
//! single/double quoting around the value (the logs index_keys
//! convention, generalized).

/// Split raw CREATE args into (name, unquoted value) pairs. Unknown
/// names are the CALLER's problem — each module allowlists its own.
pub(crate) fn parse_kv_args(args: &[&[u8]]) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    for raw in args {
        let arg = String::from_utf8_lossy(raw);
        let arg = arg.trim();
        let Some((name, value)) = arg.split_once('=') else {
            return Err(format!(
                "unrecognized argument {arg:?}; expected name='value'"
            ));
        };
        let value = value.trim();
        let value = value
            .strip_prefix('\'')
            .and_then(|v| v.strip_suffix('\''))
            .or_else(|| value.strip_prefix('"').and_then(|v| v.strip_suffix('"')))
            .unwrap_or(value);
        out.push((name.trim().to_owned(), value.to_owned()));
    }
    Ok(out)
}

/// Parse a retention duration into the module's NATIVE ts units.
///
/// Accepts `<n>` (already native units) or `<n>s|m|h|d` converted via
/// `native_per_second` (metrics 1, logs 1_000, traces 1_000_000_000 —
/// the documented unit conventions). Must be positive; overflow is an
/// error, not a wrap.
pub(crate) fn parse_retention(value: &str, native_per_second: i64) -> Result<i64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("retention: empty value".into());
    }
    let (digits, per_unit) = match value.as_bytes().last() {
        Some(b's') => (&value[..value.len() - 1], native_per_second),
        Some(b'm') => (&value[..value.len() - 1], native_per_second.saturating_mul(60)),
        Some(b'h') => (
            &value[..value.len() - 1],
            native_per_second.saturating_mul(3600),
        ),
        Some(b'd') => (
            &value[..value.len() - 1],
            native_per_second.saturating_mul(86_400),
        ),
        _ => (value, 1),
    };
    let n: i64 = digits
        .trim()
        .parse()
        .map_err(|_| format!("retention: expected <n>[s|m|h|d], got {value:?}"))?;
    if n <= 0 {
        return Err(format!("retention must be positive, got {value:?}"));
    }
    let native = n
        .checked_mul(per_unit)
        .ok_or_else(|| format!("retention {value:?} overflows the table's ts unit range"))?;
    if native <= 0 {
        return Err(format!("retention {value:?} overflows the table's ts unit range"));
    }
    Ok(native)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_parsing() {
        let args: Vec<&[u8]> = vec![b"retention='30d'", b"index_keys=\"a,b\"", b"x= plain "];
        let kv = parse_kv_args(&args).unwrap();
        assert_eq!(
            kv,
            vec![
                ("retention".into(), "30d".into()),
                ("index_keys".into(), "a,b".into()),
                ("x".into(), "plain".into()),
            ]
        );
        assert!(parse_kv_args(&[b"noequals" as &[u8]]).is_err());
    }

    #[test]
    fn retention_units() {
        // metrics: seconds native
        assert_eq!(parse_retention("30d", 1).unwrap(), 30 * 86_400);
        assert_eq!(parse_retention("90s", 1).unwrap(), 90);
        assert_eq!(parse_retention("1200", 1).unwrap(), 1200); // bare = native
        // logs: ms native
        assert_eq!(parse_retention("7d", 1_000).unwrap(), 7 * 86_400 * 1_000);
        // traces: ns native
        assert_eq!(
            parse_retention("72h", 1_000_000_000).unwrap(),
            72 * 3_600 * 1_000_000_000
        );
        // rejections
        assert!(parse_retention("0", 1).is_err());
        assert!(parse_retention("-5d", 1).is_err());
        assert!(parse_retention("soon", 1).is_err());
        assert!(parse_retention("", 1).is_err());
        // overflow: 300_000d in nanoseconds
        assert!(parse_retention("300000d", 1_000_000_000).is_err());
    }
}
