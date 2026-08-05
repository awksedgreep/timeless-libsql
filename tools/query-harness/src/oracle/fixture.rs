use std::collections::BTreeMap;

fn protobuf_varint(mut value: u64) -> Vec<u8> {
    let mut encoded = Vec::new();
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
    encoded
}

fn protobuf_bytes(field: u64, value: &[u8]) -> Vec<u8> {
    let mut encoded = protobuf_varint((field << 3) | 2);
    encoded.extend(protobuf_varint(value.len() as u64));
    encoded.extend_from_slice(value);
    encoded
}

fn snappy_literal(value: &[u8]) -> Vec<u8> {
    if value.is_empty() {
        return vec![0];
    }
    let mut encoded = protobuf_varint(value.len() as u64);
    let adjusted = value.len() - 1;
    if value.len() <= 60 {
        encoded.push((adjusted << 2) as u8);
    } else {
        let width = ((usize::BITS - adjusted.leading_zeros()) as usize)
            .div_ceil(8)
            .max(1);
        encoded.push(((59 + width) << 2) as u8);
        encoded.extend_from_slice(&adjusted.to_le_bytes()[..width]);
    }
    encoded.extend_from_slice(value);
    encoded
}

struct WriteRequest {
    timestamp_ms: i64,
    encoded: Vec<u8>,
}

impl WriteRequest {
    fn new(timestamp_ms: i64) -> Self {
        Self {
            timestamp_ms,
            encoded: Vec::new(),
        }
    }

    fn series(&mut self, name: &str, points: &[(f64, i64)], labels: &[(&str, &str)]) {
        let mut labels: BTreeMap<&str, &str> = labels.iter().copied().collect();
        labels.insert("__name__", name);
        labels.insert("job", "oracle");
        let mut series = Vec::new();
        for (name, value) in labels {
            let mut label = protobuf_bytes(1, name.as_bytes());
            label.extend(protobuf_bytes(2, value.as_bytes()));
            series.extend(protobuf_bytes(1, &label));
        }
        for (value, offset_ms) in points {
            let mut sample = vec![(1 << 3) | 1];
            sample.extend_from_slice(&value.to_le_bytes());
            sample.extend(protobuf_varint(2 << 3));
            sample.extend(protobuf_varint((self.timestamp_ms + offset_ms) as u64));
            series.extend(protobuf_bytes(2, &sample));
        }
        self.encoded.extend(protobuf_bytes(1, &series));
    }
}

pub(super) fn prometheus_remote_write(timestamp_ms: i64) -> Vec<u8> {
    let mut request = WriteRequest::new(timestamp_ms);
    request.series("oracle_lookback", &[(7.0, 0)], &[]);
    request.series(
        "oracle_temporal",
        &(-30_000..=30_000)
            .step_by(10_000)
            .enumerate()
            .map(|(index, offset)| (index as f64 + 1.0, offset))
            .collect::<Vec<_>>(),
        &[],
    );
    request.series(
        "oracle_step",
        &(-10_000..=20_000)
            .step_by(1_000)
            .enumerate()
            .map(|(index, offset)| (index as f64 + 1.0, offset))
            .collect::<Vec<_>>(),
        &[],
    );
    request.series(
        "oracle.metric/温度",
        &[(42.0, 30_000)],
        &[("node.name", "東京")],
    );
    request.series(
        "oracle.\"quoted\"\\温度",
        &[(43.0, 30_000)],
        &[("node.name", "大阪")],
    );
    for (name, value, labels) in [
        (
            "oracle_arithmetic_lhs",
            8.0,
            vec![("host", "a"), ("zone", "east")],
        ),
        (
            "oracle_arithmetic_rhs",
            2.0,
            vec![("host", "a"), ("zone", "east")],
        ),
        (
            "oracle_arithmetic_rhs_duplicate",
            3.0,
            vec![("host", "a"), ("zone", "east")],
        ),
        (
            "oracle_matching_lhs",
            8.0,
            vec![("host", "a"), ("shared", "x"), ("zone", "east")],
        ),
        (
            "oracle_matching_rhs",
            2.0,
            vec![("host", "a"), ("shared", "x"), ("zone", "west")],
        ),
        (
            "oracle_matching_rhs_duplicate",
            3.0,
            vec![("host", "a"), ("shared", "x"), ("zone", "north")],
        ),
    ] {
        request.series(name, &[(value, 30_000)], &labels);
    }
    for (pod, zone, value) in [("p1", "east", 8.0), ("p2", "west", 9.0)] {
        request.series(
            "oracle_group_many_lhs",
            &[(value, 30_000)],
            &[
                ("host", "a"),
                ("owner", "old"),
                ("pod", pod),
                ("zone", zone),
            ],
        );
    }
    for (team, value) in [("red", 4.0), ("blue", 5.0)] {
        request.series(
            "oracle_group_collision_many",
            &[(value, 30_000)],
            &[("host", "a"), ("team", team)],
        );
    }
    for (name, value, team) in [
        ("oracle_group_one_rhs", 2.0, "core"),
        ("oracle_group_one_rhs_duplicate", 3.0, "ops"),
        ("oracle_group_one_lhs", 8.0, "core"),
        ("oracle_group_one_lhs_duplicate", 9.0, "ops"),
    ] {
        request.series(name, &[(value, 30_000)], &[("host", "a"), ("team", team)]);
    }
    for (pod, zone, value) in [("p1", "east", 2.0), ("p2", "west", 3.0)] {
        request.series(
            "oracle_group_many_rhs",
            &[(value, 30_000)],
            &[("host", "a"), ("pod", pod), ("zone", zone)],
        );
    }
    for (host, region, first, second) in [
        ("a", "east", 1.0, 3.0),
        ("b", "east", 2.0, 4.0),
        ("c", "west", 5.0, 6.0),
    ] {
        request.series(
            "oracle_aggregation",
            &[(first, 20_000), (second, 30_000)],
            &[("host", host), ("region", region)],
        );
    }
    for (host, value) in [("a", 1e16), ("b", 1.0), ("c", -1e16)] {
        request.series(
            "oracle_avg_precision",
            &[(value, 30_000)],
            &[("host", host)],
        );
    }
    for (case, host, value) in [
        ("nan", "a", f64::NAN),
        ("nan", "b", 1.0),
        ("positive", "a", f64::INFINITY),
        ("positive", "b", 1.0),
        ("mixed", "a", f64::INFINITY),
        ("mixed", "b", f64::NEG_INFINITY),
    ] {
        request.series(
            "oracle_avg_ieee",
            &[(value, 30_000)],
            &[("case", case), ("host", host)],
        );
    }
    for (host, value) in [
        ("tiny", 1e-20),
        ("huge", 1e20),
        ("negative_zero", -0.0),
        ("positive_inf", f64::INFINITY),
        ("nan", f64::NAN),
    ] {
        request.series("oracle_count_values", &[(value, 30_000)], &[("host", host)]);
    }
    for host in ["a", "b"] {
        request.series("oracle_rank_tie", &[(5.0, 30_000)], &[("host", host)]);
    }
    request.series(
        "oracle_range_avg_precision",
        &[(1e16, 10_000), (1.0, 20_000), (-1e16, 30_000)],
        &[],
    );
    request.series(
        "oracle_range_avg_overflow",
        &[(f64::MAX, 20_000), (f64::MAX, 30_000)],
        &[],
    );
    for (case, values) in [
        ("nan", [f64::NAN, 1.0]),
        ("positive", [f64::INFINITY, 1.0]),
        ("mixed", [f64::INFINITY, f64::NEG_INFINITY]),
    ] {
        request.series(
            "oracle_range_avg_ieee",
            &[(values[0], 20_000), (values[1], 30_000)],
            &[("case", case)],
        );
    }
    for (case, values) in [
        ("all_nan", [f64::NAN, f64::NAN]),
        ("mixed", [f64::NAN, 2.0]),
        ("infinite", [f64::NEG_INFINITY, f64::INFINITY]),
        ("zero", [0.0, -0.0]),
        ("zero_reverse", [-0.0, 0.0]),
    ] {
        request.series(
            "oracle_range_extrema",
            &[(values[0], 20_000), (values[1], 30_000)],
            &[("case", case)],
        );
    }
    let counter_cases: Vec<(&str, Vec<(f64, i64)>)> = vec![
        (
            "steady",
            vec![(100.0, 10_000), (300.0, 30_000), (500.0, 50_000)],
        ),
        (
            "reset",
            vec![(100.0, 10_000), (150.0, 30_000), (20.0, 50_000)],
        ),
        ("sparse", vec![(100.0, 30_000), (200.0, 40_000)]),
        ("zero", vec![(1.0, 10_000), (101.0, 30_000)]),
        (
            "constant",
            vec![(7.0, 10_000), (7.0, 30_000), (7.0, 50_000)],
        ),
        (
            "repeated",
            vec![
                (1.0, 10_000),
                (1.0, 20_000),
                (2.0, 30_000),
                (2.0, 40_000),
                (1.0, 50_000),
            ],
        ),
        ("nan_repeat", vec![(f64::NAN, 20_000), (f64::NAN, 40_000)]),
        ("zero_sign", vec![(0.0, 20_000), (-0.0, 40_000)]),
        (
            "constant_inf",
            vec![
                (f64::INFINITY, 10_000),
                (f64::INFINITY, 30_000),
                (f64::INFINITY, 50_000),
            ],
        ),
        ("singleton", vec![(5.0, 50_000)]),
        ("nan", vec![(f64::NAN, 20_000), (2.0, 40_000)]),
        ("pos_inf", vec![(1.0, 20_000), (f64::INFINITY, 40_000)]),
        ("inf_drop", vec![(f64::INFINITY, 20_000), (1.0, 40_000)]),
        ("neg_inf", vec![(1.0, 20_000), (f64::NEG_INFINITY, 40_000)]),
    ];
    for (case, points) in counter_cases {
        request.series("oracle_counter", &points, &[("case", case)]);
    }
    type UnaryCase<'a> = (&'a str, &'a str, Vec<(f64, i64)>);
    let unary_cases: Vec<UnaryCase<'_>> = vec![
        ("oracle_transform", "positive", vec![(2.0, 30_000)]),
        (
            "oracle_transform",
            "negative",
            vec![(-3.0, 20_000), (-4.0, 30_000)],
        ),
        ("oracle_transform", "negative_zero", vec![(-0.0, 30_000)]),
        ("oracle_transform", "nan", vec![(f64::NAN, 30_000)]),
        (
            "oracle_transform",
            "positive_inf",
            vec![(f64::INFINITY, 30_000)],
        ),
        (
            "oracle_transform",
            "negative_inf",
            vec![(f64::NEG_INFINITY, 30_000)],
        ),
        (
            "oracle_round",
            "negative",
            vec![(-1.6, 20_000), (-2.6, 30_000)],
        ),
        ("oracle_round", "negative_tie", vec![(-1.5, 30_000)]),
        ("oracle_round", "negative_zero", vec![(-0.0, 30_000)]),
        ("oracle_round", "positive", vec![(1.6, 30_000)]),
        ("oracle_round", "positive_tie", vec![(1.5, 30_000)]),
        ("oracle_round", "nan", vec![(f64::NAN, 30_000)]),
        (
            "oracle_round",
            "positive_inf",
            vec![(f64::INFINITY, 30_000)],
        ),
        (
            "oracle_round",
            "negative_inf",
            vec![(f64::NEG_INFINITY, 30_000)],
        ),
        (
            "oracle_clamp",
            "below",
            vec![(-2.0, 20_000), (-3.0, 30_000)],
        ),
        ("oracle_clamp", "inside", vec![(2.0, 30_000)]),
        ("oracle_clamp", "above", vec![(8.0, 30_000)]),
        ("oracle_clamp", "negative_zero", vec![(-0.0, 30_000)]),
        ("oracle_clamp", "positive_zero", vec![(0.0, 30_000)]),
        ("oracle_clamp", "nan", vec![(f64::NAN, 30_000)]),
        (
            "oracle_clamp",
            "positive_inf",
            vec![(f64::INFINITY, 30_000)],
        ),
        (
            "oracle_clamp",
            "negative_inf",
            vec![(f64::NEG_INFINITY, 30_000)],
        ),
    ];
    for (name, case, points) in unary_cases {
        request.series(name, &points, &[("case", case)]);
    }
    for (name, cases) in [
        (
            "oracle_math",
            vec![
                ("sqrt", vec![(4.0, 20_000), (9.0, 30_000)]),
                ("zero", vec![(0.0, 30_000)]),
                ("negative_zero", vec![(-0.0, 30_000)]),
                ("one", vec![(1.0, 30_000)]),
                ("eight", vec![(8.0, 30_000)]),
                ("hundred", vec![(100.0, 30_000)]),
                ("negative", vec![(-4.0, 30_000)]),
                ("nan", vec![(f64::NAN, 30_000)]),
                ("positive_inf", vec![(f64::INFINITY, 30_000)]),
                ("negative_inf", vec![(f64::NEG_INFINITY, 30_000)]),
            ],
        ),
        (
            "oracle_inverse",
            vec![
                ("range", vec![(0.0, 20_000), (1.0, 30_000)]),
                ("negative_one", vec![(-1.0, 30_000)]),
                ("negative_zero", vec![(-0.0, 30_000)]),
                ("zero", vec![(0.0, 30_000)]),
                ("half", vec![(0.5, 30_000)]),
                ("one", vec![(1.0, 30_000)]),
                ("two", vec![(2.0, 30_000)]),
                ("nan", vec![(f64::NAN, 30_000)]),
                ("positive_inf", vec![(f64::INFINITY, 30_000)]),
                ("negative_inf", vec![(f64::NEG_INFINITY, 30_000)]),
            ],
        ),
        (
            "oracle_trig",
            vec![
                (
                    "range",
                    vec![(0.0, 20_000), (std::f64::consts::FRAC_PI_2, 30_000)],
                ),
                ("negative_zero", vec![(-0.0, 30_000)]),
                ("zero", vec![(0.0, 30_000)]),
                ("one", vec![(1.0, 30_000)]),
                ("nan", vec![(f64::NAN, 30_000)]),
                ("positive_inf", vec![(f64::INFINITY, 30_000)]),
                ("negative_inf", vec![(f64::NEG_INFINITY, 30_000)]),
            ],
        ),
        (
            "oracle_angle",
            vec![
                ("range", vec![(0.0, 20_000), (std::f64::consts::PI, 30_000)]),
                ("negative_zero", vec![(-0.0, 30_000)]),
                ("degrees", vec![(180.0, 30_000)]),
                ("nan", vec![(f64::NAN, 30_000)]),
                ("positive_inf", vec![(f64::INFINITY, 30_000)]),
                ("negative_inf", vec![(f64::NEG_INFINITY, 30_000)]),
            ],
        ),
    ] {
        for (case, points) in cases {
            request.series(name, &points, &[("case", case)]);
        }
    }
    for (case, service, zone, points) in [
        (
            "capture",
            Some("api:west"),
            Some("old"),
            vec![(1.0, 20_000), (6.0, 30_000)],
        ),
        ("missing", None, None, vec![(2.0, 30_000)]),
        ("empty", Some(""), None, vec![(3.0, 30_000)]),
        ("unmatched", Some("api"), None, vec![(4.0, 30_000)]),
        ("named", Some("west-api"), None, vec![(5.0, 30_000)]),
        ("newline", Some("api\nwest"), None, vec![(7.0, 30_000)]),
    ] {
        let mut labels = vec![("case", case)];
        if let Some(service) = service {
            labels.push(("service", service));
        }
        if let Some(zone) = zone {
            labels.push(("zone", zone));
        }
        request.series("oracle_label_replace", &points, &labels);
    }
    request.series(
        "oracle_absent_window",
        &[(9.0, 20_000)],
        &[("case", "boundary"), ("service", "api")],
    );
    request.series(
        "oracle_sort_range",
        &[(1.0, 20_000), (10.0, 30_000)],
        &[("host", "a")],
    );
    request.series(
        "oracle_sort_range",
        &[(2.0, 20_000), (0.0, 30_000)],
        &[("host", "b")],
    );
    for (host, buckets) in [
        (
            "a",
            vec![("0.1", 10.0), ("0.5", 20.0), ("1", 30.0), ("+Inf", 40.0)],
        ),
        (
            "b",
            vec![("0.1", 3.0), ("0.5", 6.0), ("1", 9.0), ("+Inf", 10.0)],
        ),
    ] {
        for (bound, count) in buckets {
            request.series(
                "oracle_histogram_bucket",
                &[
                    (count * 0.5, 10_000),
                    (count * 0.8, 20_000),
                    (count, 30_000),
                ],
                &[("host", host), ("le", bound)],
            );
        }
    }
    for (bound, count) in [("10", 10.0), ("+Inf", 10.0)] {
        request.series(
            "oracle_histogram_other_bucket",
            &[(count, 30_000)],
            &[("host", "a"), ("le", bound)],
        );
    }
    let histogram_cases: Vec<(&str, Vec<(&str, f64)>)> = vec![
        ("missing_inf", vec![("1", 10.0), ("2", 20.0)]),
        ("only_inf", vec![("+Inf", 10.0)]),
        ("zero_total", vec![("1", 0.0), ("+Inf", 0.0)]),
        (
            "negative_first",
            vec![("-5", 2.0), ("-1", 4.0), ("+Inf", 4.0)],
        ),
        ("positive_first", vec![("10", 10.0), ("+Inf", 10.0)]),
        ("decrease", vec![("1", 10.0), ("2", 9.0), ("+Inf", 20.0)]),
        (
            "tiny_delta",
            vec![("1", 1e12), ("2", 1e12 + 0.5), ("3", 2e12), ("+Inf", 3e12)],
        ),
        (
            "duplicate_bound",
            vec![("1", 3.0), ("1.0", 4.0), ("2", 10.0), ("+Inf", 10.0)],
        ),
        (
            "malformed_bound",
            vec![("bogus", 9.0), ("1", 5.0), ("+Inf", 10.0)],
        ),
        (
            "nan_count",
            vec![("1", f64::NAN), ("2", 10.0), ("+Inf", 20.0)],
        ),
        ("infinite_total", vec![("1", 10.0), ("+Inf", f64::INFINITY)]),
        (
            "infinite_finite",
            vec![("1", f64::INFINITY), ("+Inf", f64::INFINITY)],
        ),
    ];
    for (case, buckets) in histogram_cases {
        for (bound, count) in buckets {
            request.series(
                "oracle_histogram_special_bucket",
                &[(count, 30_000)],
                &[("case", case), ("le", bound)],
            );
        }
    }
    request.series(
        "oracle_histogram_special_bucket",
        &[(123.0, 30_000)],
        &[("case", "absent_bound")],
    );
    for index in 0..12 {
        let bound = format!("bad{index:02}");
        request.series(
            "oracle_histogram_special_bucket",
            &[(index as f64, 30_000)],
            &[("case", "many_malformed"), ("le", bound.as_str())],
        );
    }
    for source in ["a", "b"] {
        request.series(
            "oracle_histogram_special_bucket",
            &[(1.0, 30_000)],
            &[
                ("case", "duplicate_malformed"),
                ("le", "duplicate"),
                ("source", source),
            ],
        );
    }
    request.series(
        "oracle_mql_cpu",
        &[(1.0, 10_000), (3.0, 30_000)],
        &[("host", "a"), ("zone", "east")],
    );
    request.series(
        "oracle_mql_cpu",
        &[(2.0, 20_000), (4.0, 30_000)],
        &[("host", "b"), ("zone", "west")],
    );
    request.series(
        "oracle_mql_sparse",
        &[(10.0, 10_000), (30.0, 30_000)],
        &[("host", "a")],
    );
    request.series("oracle_mql_sparse", &[(20.0, 20_000)], &[("host", "b")]);
    request.series(
        "oracle_mql_disjoint_left",
        &[(1.0, 10_000)],
        &[("host", "c")],
    );
    request.series(
        "oracle_mql_disjoint_right",
        &[(2.0, 610_000)],
        &[("host", "c")],
    );
    request.series("oracle_keep_a", &[(2.0, 30_000)], &[("host", "shared")]);
    request.series("oracle_keep_b", &[(3.0, 30_000)], &[("host", "shared")]);
    snappy_literal(&request.encoded)
}
