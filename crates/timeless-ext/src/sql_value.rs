//! SQLite value coercions shared by virtual-table planners.

use rusqlite::types::Value;

/// Apply SQLite INTEGER affinity to a constraint value used as a durable ID.
///
/// Virtual tables receive the original dynamic value in `xFilter`; declaring a
/// column as INTEGER does not coerce it for the module. Once `xBestIndex` marks
/// a constraint omitted, the module must reproduce that affinity itself so
/// `series_id = 1`, `series_id = 1.0`, and `series_id = '1'` agree with an
/// ordinary INTEGER column. Values that cannot losslessly become an i64 cannot
/// match a catalog ID.
pub(crate) fn integer_affinity(value: Value) -> Option<i64> {
    match value {
        Value::Null | Value::Blob(_) => None,
        Value::Integer(value) => Some(value),
        Value::Real(value) => integral_f64(value),
        Value::Text(value) => {
            let value = value.trim();
            value
                .parse::<i64>()
                .ok()
                .or_else(|| value.parse::<f64>().ok().and_then(integral_f64))
        }
    }
}

fn integral_f64(value: f64) -> Option<i64> {
    // i64::MAX is not exactly representable as f64; use the exclusive upper
    // bound 2^63 so a saturated cast cannot turn it into a false match.
    const I64_MIN_F64: f64 = -9_223_372_036_854_775_808.0;
    const I64_MAX_PLUS_ONE_F64: f64 = 9_223_372_036_854_775_808.0;
    (value.is_finite()
        && value.fract() == 0.0
        && (I64_MIN_F64..I64_MAX_PLUS_ONE_F64).contains(&value))
    .then_some(value as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mirrors_integer_affinity_for_id_values() {
        for (value, expected) in [
            (Value::Integer(1), Some(1)),
            (Value::Real(1.0), Some(1)),
            (Value::Real(1.5), None),
            (Value::Text("1".into()), Some(1)),
            (Value::Text("  +1.0e0  ".into()), Some(1)),
            (Value::Text("1x".into()), None),
            (Value::Null, None),
            (Value::Blob(vec![1]), None),
            (Value::Real(f64::NAN), None),
            (Value::Real(9_223_372_036_854_775_808.0), None),
            (Value::Real(-9_223_372_036_854_775_808.0), Some(i64::MIN)),
        ] {
            assert_eq!(integer_affinity(value), expected);
        }
    }
}
