use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Args;
use regex::Regex;
use rusqlite::types::Value;
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

#[derive(Args)]
pub(crate) struct SqlArgs {
    /// Release extension to load into the direct SQLite connection.
    #[arg(long, required = true)]
    extension: PathBuf,
    /// Optional persistent database path. The default is an owned temporary file.
    #[arg(long)]
    database: Option<PathBuf>,
}

#[derive(Debug)]
struct Recipe {
    identifier: String,
    statements: Vec<String>,
}

fn parse_recipes(path: &Path) -> Result<Vec<Recipe>> {
    let heading = Regex::new(r"^### (SQL-(?:PROM|MQL|LOG)-\d{3}):")?;
    let index_entry = Regex::new(r"^\|\s*\[`(SQL-(?:PROM|MQL|LOG)-\d{3})`\]")?;
    let content = fs::read_to_string(path)?;
    let mut recipes = Vec::new();
    let mut current: Option<Recipe> = None;
    let mut statement: Option<String> = None;
    for line in content.lines() {
        if let Some(captures) = heading.captures(line) {
            if let Some(recipe) = current.take() {
                recipes.push(recipe);
            }
            current = Some(Recipe {
                identifier: captures[1].to_owned(),
                statements: Vec::new(),
            });
            continue;
        }
        if line == "```sql" && current.is_some() && statement.is_none() {
            statement = Some(String::new());
            continue;
        }
        if line == "```" {
            if let Some(sql) = statement.take() {
                if !sql.trim().is_empty() {
                    current.as_mut().unwrap().statements.push(sql);
                }
            }
            continue;
        }
        if let Some(sql) = statement.as_mut() {
            sql.push_str(line);
            sql.push('\n');
        }
    }
    if statement.is_some() {
        bail!("{} has an unterminated SQL fence", path.display());
    }
    if let Some(recipe) = current {
        recipes.push(recipe);
    }
    let mut identifiers = BTreeSet::new();
    for recipe in &recipes {
        if !identifiers.insert(recipe.identifier.clone()) {
            bail!("duplicate SQL recipe {}", recipe.identifier);
        }
        if recipe.statements.is_empty() {
            bail!(
                "SQL recipe {} has no executable SQL fence",
                recipe.identifier
            );
        }
    }
    let recipe_ids = recipes
        .iter()
        .map(|recipe| recipe.identifier.clone())
        .collect::<BTreeSet<_>>();
    let mut index_ids = BTreeSet::new();
    for captures in content
        .lines()
        .filter_map(|line| index_entry.captures(line))
    {
        let identifier = captures[1].to_owned();
        if !index_ids.insert(identifier.clone()) {
            bail!("duplicate SQL recipe index entry {identifier}");
        }
    }
    if recipe_ids != index_ids {
        let missing = recipe_ids
            .difference(&index_ids)
            .cloned()
            .collect::<Vec<_>>();
        let unknown = index_ids
            .difference(&recipe_ids)
            .cloned()
            .collect::<Vec<_>>();
        bail!("SQL recipe index mismatch; missing {missing:?}; unknown {unknown:?}");
    }
    Ok(recipes)
}

fn open(extension: &Path, database: &Path) -> Result<Connection> {
    let connection = Connection::open(database)?;
    unsafe {
        connection.load_extension_enable()?;
        connection.load_extension(extension, None::<&str>)?;
        connection.load_extension_disable()?;
    }
    Ok(connection)
}

fn setup(connection: &mut Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE VIRTUAL TABLE metrics USING timeless_metrics;
         CREATE VIRTUAL TABLE logs USING timeless_logs(
           index_keys='service,host,path,status'
         );",
    )?;
    let transaction = connection.transaction()?;
    {
        let mut insert =
            transaction.prepare("INSERT INTO metrics(name,labels,ts,value) VALUES(?1,?2,?3,?4)")?;
        let rows = [
            ("cpu", r#"{"host":"web-1","service":"api"}"#, 100, 10.0),
            ("cpu", r#"{"host":"web-2","service":"api"}"#, 100, 20.0),
            ("cpu", r#"{"host":"web-1","service":"api"}"#, 110, 30.0),
            ("avg_precision", "{}", 10, 1e16),
            ("avg_precision", "{}", 20, 1.0),
            ("avg_precision", "{}", 30, -1e16),
            ("avg_overflow", "{}", 20, f64::MAX),
            ("avg_overflow", "{}", 30, f64::MAX),
            ("min_window", "{}", 10, 5.0),
            ("min_window", "{}", 20, 3.0),
            ("min_window", "{}", 30, 4.0),
            ("min_zero", "{}", 20, 0.0),
            ("min_zero", "{}", 30, -0.0),
            ("max_window", "{}", 10, 5.0),
            ("max_window", "{}", 20, 3.0),
            ("max_window", "{}", 30, 4.0),
            ("max_zero", "{}", 20, -0.0),
            ("max_zero", "{}", 30, 0.0),
            ("rate_counter", r#"{"case":"steady"}"#, 10, 100.0),
            ("rate_counter", r#"{"case":"steady"}"#, 30, 300.0),
            ("rate_counter", r#"{"case":"steady"}"#, 50, 500.0),
            ("rate_counter", r#"{"case":"reset"}"#, 10, 100.0),
            ("rate_counter", r#"{"case":"reset"}"#, 30, 150.0),
            ("rate_counter", r#"{"case":"reset"}"#, 50, 20.0),
            ("changes_metric", r#"{"case":"repeated"}"#, 10, 1.0),
            ("changes_metric", r#"{"case":"repeated"}"#, 20, 1.0),
            ("changes_metric", r#"{"case":"repeated"}"#, 30, 2.0),
            ("changes_metric", r#"{"case":"repeated"}"#, 40, 2.0),
            ("changes_metric", r#"{"case":"repeated"}"#, 50, 1.0),
            ("changes_metric", r#"{"case":"constant"}"#, 20, 7.0),
            ("changes_metric", r#"{"case":"constant"}"#, 40, 7.0),
            ("changes_metric", r#"{"case":"singleton"}"#, 50, 5.0),
            ("abs_metric", r#"{"case":"negative"}"#, 100, -3.0),
            (
                "abs_metric",
                r#"{"case":"negative_inf"}"#,
                100,
                f64::NEG_INFINITY,
            ),
            ("abs_metric", r#"{"case":"negative_zero"}"#, 100, -0.0),
            ("abs_metric", r#"{"case":"positive"}"#, 100, 2.0),
            ("round_metric", r#"{"case":"negative"}"#, 100, -1.6),
            ("round_metric", r#"{"case":"negative_zero"}"#, 100, -0.0),
            ("round_metric", r#"{"case":"positive"}"#, 100, 1.6),
            ("clamp_metric", r#"{"case":"below"}"#, 100, -3.0),
            ("clamp_metric", r#"{"case":"inside"}"#, 100, 2.0),
            ("clamp_metric", r#"{"case":"above"}"#, 100, 8.0),
            ("math_metric", r#"{"case":"sqrt"}"#, 100, 4.0),
            ("math_metric", r#"{"case":"zero"}"#, 100, 0.0),
            ("math_metric", r#"{"case":"one"}"#, 100, 1.0),
            ("math_metric", r#"{"case":"eight"}"#, 100, 8.0),
            ("math_metric", r#"{"case":"hundred"}"#, 100, 100.0),
            ("math_metric", r#"{"case":"negative"}"#, 100, -4.0),
            ("math_metric", r#"{"case":"two"}"#, 100, 2.0),
            (
                "label_join_metric",
                r#"{"case":"both","joined":"old","service":"api","zone":"west"}"#,
                100,
                1.0,
            ),
            (
                "label_join_metric",
                r#"{"case":"missing","service":"api"}"#,
                100,
                2.0,
            ),
            (
                "absent_late",
                r#"{"case":"late","service":"api"}"#,
                110,
                7.0,
            ),
            (
                "calendar_metric",
                r#"{"case":"date","host":"web-1"}"#,
                100,
                90061.9,
            ),
            (
                "calendar_leap_metric",
                r#"{"case":"leap","host":"web-1"}"#,
                100,
                1709208000.0,
            ),
            (
                "sql_histogram_bucket",
                r#"{"host":"web-1","le":"0.1"}"#,
                100,
                10.0,
            ),
            (
                "sql_histogram_bucket",
                r#"{"host":"web-1","le":"0.5"}"#,
                100,
                20.0,
            ),
            (
                "sql_histogram_bucket",
                r#"{"host":"web-1","le":"1"}"#,
                100,
                30.0,
            ),
            (
                "sql_histogram_bucket",
                r#"{"host":"web-1","le":"+Inf"}"#,
                100,
                40.0,
            ),
            ("errors_total", r#"{"host":"web-1"}"#, 100, 2.0),
            ("requests_total", r#"{"host":"web-1"}"#, 100, 10.0),
        ];
        for (name, labels, ts, value) in rows {
            insert.execute(params![name, labels, ts, value])?;
        }
    }
    transaction.execute("INSERT INTO metrics(metrics) VALUES ('flush')", [])?;
    {
        let mut insert = transaction
            .prepare("INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)")?;
        insert.execute(params![
            1000,
            "error",
            "request timeout",
            r#"{"service":"api","host":"web-1","deployment":{"region":"us-east"},"duration_ms":12,"client_ip":"010.000.000.001","nested":{"ok":true,"count":2,"none":null,"empty":"","array_text":"  [1,{\"x\":2},[3],null]  "},"tags":["prod","",123,true,false,null,{"nested":"ignored"},["ignored"],"a\u0062","*"]}"#,
        ])?;
        insert.execute(params![
            2000,
            "info",
            "request ok",
            r#"{"service":"api","host":"web-2","deployment":{"region":"us-west"},"duration_ms":4,"client_ip":"10.0.1.1","nested":{"ok":"true","count":"2","empty":null,"array_text":"  [1,{\"x\":2},[3],null]  "},"tags":["dev",1.5,-2,"123","a\"b","a\nb","a/b"]}"#,
        ])?;
    }
    transaction.execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
    transaction.commit()?;
    Ok(())
}

fn parameter(identifier: &str, name: &str) -> Value {
    let counter_window = matches!(
        identifier,
        "SQL-PROM-029"
            | "SQL-PROM-030"
            | "SQL-PROM-031"
            | "SQL-PROM-032"
            | "SQL-PROM-033"
            | "SQL-PROM-034"
            | "SQL-PROM-035"
            | "SQL-PROM-036"
            | "SQL-PROM-037"
    );
    let metric = match identifier {
        "SQL-PROM-002" => "avg_precision",
        "SQL-PROM-021" => "min_window",
        "SQL-PROM-022" => "max_window",
        "SQL-PROM-029" | "SQL-PROM-030" | "SQL-PROM-031" | "SQL-PROM-032" | "SQL-PROM-033"
        | "SQL-PROM-034" | "SQL-PROM-035" | "SQL-PROM-037" => "rate_counter",
        "SQL-PROM-036" => "changes_metric",
        "SQL-PROM-038" | "SQL-PROM-049" => "abs_metric",
        "SQL-PROM-039" => "round_metric",
        "SQL-PROM-040" => "clamp_metric",
        "SQL-PROM-041" | "SQL-PROM-042" | "SQL-PROM-043" | "SQL-PROM-044" | "SQL-PROM-045" => {
            "math_metric"
        }
        "SQL-PROM-046" => "label_join_metric",
        "SQL-PROM-047" | "SQL-PROM-048" => "absent_late",
        "SQL-PROM-052" => "calendar_metric",
        "SQL-PROM-053" => "calendar_leap_metric",
        "SQL-PROM-054" | "SQL-PROM-056" | "SQL-MQL-012" => "sql_histogram_bucket",
        "SQL-PROM-055" => "cpu",
        _ => "cpu",
    };
    match name {
        "metric" => Value::Text(metric.to_owned()),
        "lhs_metric" | "many_metric" => Value::Text("cpu".to_owned()),
        "rhs_metric" if identifier == "SQL-MQL-001" => Value::Text("requests_total".to_owned()),
        "rhs_metric" => Value::Text("cpu".to_owned()),
        "one_metric" => Value::Text("requests_total".to_owned()),
        "filter_json" | "lhs_filter" | "rhs_filter" | "many_filter" | "one_filter" => Value::Null,
        "error_filter" => Value::Text(r#"{"host":"web-1"}"#.to_owned()),
        "request_filter" => Value::Text(r#"{"host":"web-1"}"#.to_owned()),
        "at" => Value::Integer(110),
        "anchor" => Value::Integer(100),
        "start" => Value::Integer(if counter_window { 60 } else { 100 }),
        "end" => Value::Integer(if counter_window { 60 } else { 110 }),
        "step" | "resolution" => Value::Integer(10),
        "request_step" => Value::Integer(10),
        "multiple" => Value::Real(1.0),
        "fixed_offset" => Value::Integer(0),
        "lookback" | "window" => Value::Integer(if counter_window { 60 } else { 20 }),
        "history" => Value::Integer(300),
        "max_lookback" => Value::Integer(0),
        "offset" => Value::Integer(10),
        "max_work_points" => Value::Integer(100_000),
        "max_work_entries" => Value::Integer(100_000),
        "threshold" if identifier == "SQL-MQL-001" => Value::Real(15.0),
        "threshold" => Value::Real(0.0),
        "default_value" if identifier == "SQL-LOG-032" => Value::Text("fallback".to_owned()),
        "default_value" => Value::Real(0.0),
        "scalar" | "scalar_value" | "value" => Value::Real(2.0),
        "q" | "quantile" => Value::Real(0.5),
        "first_quantile" => Value::Real(0.25),
        "second_quantile" => Value::Real(0.75),
        "first_phi_label" => Value::Text("0.25".to_owned()),
        "second_phi_label" => Value::Text("0.75".to_owned()),
        "destination_path" => Value::Text("$.phi".to_owned()),
        "k" => Value::Integer(1),
        "horizon" => Value::Real(10.0),
        "nearest" => Value::Real(0.5),
        "minimum" => Value::Real(0.0),
        "maximum" => Value::Real(5.0),
        "lower" => Value::Real(0.1),
        "upper" => Value::Real(0.5),
        "destination" => Value::Text("joined".to_owned()),
        "first_metric" => Value::Text("cpu".to_owned()),
        "second_metric" => Value::Text("requests_total".to_owned()),
        "alias_metric" => Value::Text("cpu".to_owned()),
        "alias_name" => Value::Text("aliased_cpu".to_owned()),
        "collision_first_metric" => Value::Text("errors_total".to_owned()),
        "collision_second_metric" => Value::Text("requests_total".to_owned()),
        "collision_alias" => Value::Text("combined_total".to_owned()),
        "label_name" => Value::Text("environment".to_owned()),
        "label_path" => Value::Text("$.environment".to_owned()),
        "label_value" => Value::Text("production".to_owned()),
        "delete_label" => Value::Text("service".to_owned()),
        "delete_path" => Value::Text("$.service".to_owned()),
        "delete_path_1" => Value::Text("$.deployment.region".to_owned()),
        "delete_path_2" => Value::Text("$.duration_ms".to_owned()),
        "sort_path" => Value::Text("$.duration_ms".to_owned()),
        "tie_path" => Value::Text("$.host".to_owned()),
        "partition_path" => Value::Text("$.service".to_owned()),
        "group_path" => Value::Text("$.host".to_owned()),
        "filter_text" => Value::Text("web".to_owned()),
        "first_count" => Value::Integer(1),
        "last_count" => Value::Integer(1),
        "top_count" => Value::Integer(10),
        "uniq_limit" => Value::Integer(10),
        "facets_limit" => Value::Integer(10),
        "max_values_per_field" => Value::Integer(1_000),
        "max_value_len" => Value::Integer(128),
        "keep_const_fields" => Value::Integer(0),
        "timestamp_units_per_second" => Value::Integer(1_000),
        "start_ts" => Value::Integer(1_000),
        "end_ts" => Value::Integer(2_000),
        "source_path_1" => Value::Text("$.nested.empty".to_owned()),
        "source_path_2" => Value::Text("$.host".to_owned()),
        "source_path_3" => Value::Text("$.service".to_owned()),
        "copy_source_path" => Value::Text("$.duration_ms".to_owned()),
        "copy_destination_path" => Value::Text("$.copied".to_owned()),
        "rename_source_path" => Value::Text("$.duration_ms".to_owned()),
        "rename_destination_path" => Value::Text("$.moved".to_owned()),
        "format_source_path_1" => Value::Text("$.host".to_owned()),
        "format_source_path_2" => Value::Text("$.duration_ms".to_owned()),
        "format_pattern" => Value::Text("host=%s duration_ms=%s".to_owned()),
        "math_source_path_1" => Value::Text("$.duration_ms".to_owned()),
        "math_source_path_2" => Value::Text("$.nested.count".to_owned()),
        "math_multiplier" => Value::Real(2.0),
        "len_source_path" => Value::Text("$.host".to_owned()),
        "drop_empty_path" => Value::Text("$.nested.empty".to_owned()),
        "replace_path" => Value::Text("$.host".to_owned()),
        "replace_old" => Value::Text("web".to_owned()),
        "replace_new" => Value::Text("node".to_owned()),
        "replace_enabled" => Value::Integer(1),
        "extract_source_path" => Value::Text("$.client_ip".to_owned()),
        "extract_prefix" => Value::Text(String::new()),
        "extract_middle" | "extract_suffix" => Value::Text(".".to_owned()),
        "pack_path_1" => Value::Text("$.host".to_owned()),
        "pack_path_2" => Value::Text("$.duration_ms".to_owned()),
        "pack_path_3" => Value::Text("$.nested.ok".to_owned()),
        "pack_path_4" => Value::Text("$.nested.none".to_owned()),
        "pack_path_5" => Value::Text("$.tags".to_owned()),
        "pack_path_6" => Value::Text("$.nested.empty".to_owned()),
        "unpack_source_path" => Value::Text("$.nested".to_owned()),
        "unpack_path_1" => Value::Text("$.ok".to_owned()),
        "unpack_path_2" => Value::Text("$.count".to_owned()),
        "unpack_path_3" => Value::Text("$.none".to_owned()),
        "unpack_path_4" => Value::Text("$.empty".to_owned()),
        "unpack_path_5" => Value::Text("$.missing".to_owned()),
        "json_array_source_path" => Value::Text("$.tags".to_owned()),
        "stats_source_path" => Value::Text("$.duration_ms".to_owned()),
        "sum_len_source_path" => Value::Text("$.duration_ms".to_owned()),
        "any_source_path" => Value::Text("$.host".to_owned()),
        "extreme_source_path" => Value::Text("$.duration_ms".to_owned()),
        "extreme_result_path" => Value::Text("$.host".to_owned()),
        "row_any_path_1" => Value::Text("$.nested.ok".to_owned()),
        "row_any_path_2" => Value::Text("$.nested.none".to_owned()),
        "row_extreme_source_path" => Value::Text("$.duration_ms".to_owned()),
        "row_result_path_1" => Value::Text("$.host".to_owned()),
        "row_result_path_2" => Value::Text("$.nested".to_owned()),
        "with_hits" => Value::Integer(1),
        "max_result_rows" => Value::Integer(100),
        "separator" => Value::Text("/".to_owned()),
        "source_labels_json" => Value::Text(r#"["service","zone"]"#.to_owned()),
        "output_labels_json" => Value::Text(r#"{"case":"late","service":"api"}"#.to_owned()),
        "descending" | "variance_only" => Value::Integer(0),
        "evaluation_ts" => Value::Integer(1_704_153_845),
        "part" => Value::Text("minute".to_owned()),
        "aggregate" => Value::Text("avg".to_owned()),
        "start_ms" | "now_ms" => Value::Integer(1000),
        "end_ms" => Value::Integer(2000),
        "step_ms" => Value::Integer(1000),
        "level" => Value::Text("error".to_owned()),
        "service" => Value::Text("api".to_owned()),
        "limit" => Value::Integer(10),
        "needle" | "message_contains" => Value::Text("timeout".to_owned()),
        "exact_message" => Value::Text("request timeout".to_owned()),
        "message_prefix" => Value::Text("request".to_owned()),
        "message_value_1" => Value::Text("request timeout".to_owned()),
        "message_value_2" => Value::Text("request ok".to_owned()),
        "indexed_value_1" => Value::Text("web-1".to_owned()),
        "indexed_value_2" => Value::Text("web-2".to_owned()),
        "field_path" => Value::Text(
            match identifier {
                "SQL-LOG-017" => "$.tags",
                "SQL-LOG-018" => "$.client_ip",
                _ => "$.deployment.region",
            }
            .to_owned(),
        ),
        "left_path" | "right_path" => Value::Text("$.deployment.region".to_owned()),
        "comparison" => Value::Text("eq".to_owned()),
        "field_prefix" => Value::Text(
            if identifier == "SQL-LOG-022" {
                "deployment."
            } else {
                "us-"
            }
            .to_owned(),
        ),
        "exact_text" => Value::Text("us-east".to_owned()),
        "field_value_1" => Value::Text("us-east".to_owned()),
        "field_value_2" => Value::Text("us-west".to_owned()),
        "array_value_1" => Value::Text("prod".to_owned()),
        "array_value_2" => Value::Text("absent".to_owned()),
        "ipv4_min" => Value::Integer(0x0a00_0000),
        "ipv4_max" => Value::Integer(0x0a00_00ff),
        "string_min" => Value::Text("us-".to_owned()),
        "string_max" => Value::Text("ut".to_owned()),
        "length_min" => Value::Integer(7),
        "length_max" => Value::Integer(7),
        "day_start_ns" => Value::Integer(0),
        "day_end_ns" => Value::Integer(60_000_000_000),
        "week_start_day" => Value::Integer(0),
        "week_end_day" => Value::Integer(6),
        "start_inclusive" => Value::Integer(1),
        "end_inclusive" => Value::Integer(0),
        "offset_ns" => Value::Integer(0),
        "timestamp_scale_ns" => Value::Integer(1_000_000),
        "units_per_day" => Value::Integer(86_400_000),
        "empty_path" => Value::Text("$.nested.none".to_owned()),
        "any_path" => Value::Text("$.deployment.region".to_owned()),
        "duration_threshold" => Value::Integer(10),
        "excluded_level" => Value::Text("info".to_owned()),
        "field" => Value::Text(
            if identifier == "SQL-LOG-013" {
                "duration_ms"
            } else {
                "host"
            }
            .to_owned(),
        ),
        "max_values" => Value::Integer(100),
        "region" => Value::Text("us-east".to_owned()),
        "group_key" => Value::Text("level".to_owned()),
        _ => Value::Null,
    }
}

fn execute_statement(
    connection: &Connection,
    identifier: &str,
    ordinal: usize,
    sql: &str,
) -> Result<usize> {
    let mut statement = connection
        .prepare(sql)
        .with_context(|| format!("prepare {identifier} statement {ordinal}"))?;
    for index in 1..=statement.parameter_count() {
        let raw = statement
            .parameter_name(index)
            .with_context(|| format!("{identifier} parameter {index} is unnamed"))?;
        let name = raw.trim_start_matches([':', '$', '@']);
        statement.raw_bind_parameter(index, parameter(identifier, name))?;
    }
    if statement.column_count() == 0 {
        statement.raw_execute()?;
        return Ok(0);
    }
    let mut rows = statement.raw_query();
    let mut count = 0;
    while rows.next()?.is_some() {
        count += 1;
    }
    Ok(count)
}

fn split_sql(block: &str) -> Result<Vec<String>> {
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum State {
        Normal,
        SingleQuoted,
        DoubleQuoted,
        BacktickQuoted,
        BracketQuoted,
        LineComment,
        BlockComment,
    }

    let characters: Vec<_> = block.chars().collect();
    let mut statements = Vec::new();
    let mut statement = String::new();
    let mut state = State::Normal;
    let mut index = 0;
    while index < characters.len() {
        let current = characters[index];
        let next = characters.get(index + 1).copied();
        statement.push(current);
        match state {
            State::Normal => match (current, next) {
                ('\'', _) => state = State::SingleQuoted,
                ('"', _) => state = State::DoubleQuoted,
                ('`', _) => state = State::BacktickQuoted,
                ('[', _) => state = State::BracketQuoted,
                ('-', Some('-')) => {
                    statement.push('-');
                    index += 1;
                    state = State::LineComment;
                }
                ('/', Some('*')) => {
                    statement.push('*');
                    index += 1;
                    state = State::BlockComment;
                }
                (';', _) => {
                    if !statement
                        .trim_matches([';', ' ', '\t', '\r', '\n'])
                        .is_empty()
                    {
                        statements.push(statement.trim().to_owned());
                    }
                    statement.clear();
                }
                _ => {}
            },
            State::SingleQuoted if current == '\'' => {
                if next == Some('\'') {
                    statement.push('\'');
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::DoubleQuoted if current == '"' => {
                if next == Some('"') {
                    statement.push('"');
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::BacktickQuoted if current == '`' => state = State::Normal,
            State::BracketQuoted if current == ']' => state = State::Normal,
            State::LineComment if current == '\n' => state = State::Normal,
            State::BlockComment if current == '*' && next == Some('/') => {
                statement.push('/');
                index += 1;
                state = State::Normal;
            }
            _ => {}
        }
        index += 1;
    }
    if !matches!(state, State::Normal | State::LineComment) {
        bail!("unterminated quoted value or block comment in SQL recipe");
    }
    if !statement.trim().is_empty() {
        statements.push(statement.trim().to_owned());
    }
    Ok(statements)
}

fn execute_recipes(connection: &Connection, recipes: &[Recipe]) -> Result<(usize, usize)> {
    let mut statements = 0;
    for recipe in recipes {
        for block in &recipe.statements {
            for sql in split_sql(block)? {
                execute_statement(connection, &recipe.identifier, statements + 1, &sql)?;
                statements += 1;
            }
        }
    }
    Ok((recipes.len(), statements))
}

fn semantic_regressions(connection: &Connection, recipes: &[Recipe]) -> Result<()> {
    let instant: i64 = connection.query_row(
        "SELECT COUNT(*) FROM timeless_grid('metrics','cpu',NULL,110,110,1,20)",
        [],
        |row| row.get(0),
    )?;
    if instant != 2 {
        bail!("SQL-PROM-001 returned {instant} rows, expected 2");
    }

    let precision: f64 = connection.query_row(
        "SELECT value FROM timeless_window(
           'metrics','avg_precision',NULL,30,30,1,30,'avg')",
        [],
        |row| row.get(0),
    )?;
    if precision.to_bits() != (1.0_f64 / 3.0).to_bits() {
        bail!("SQL-PROM-002 lost compensated average precision: {precision:?}");
    }
    let overflow: f64 = connection.query_row(
        "SELECT value FROM timeless_window(
           'metrics','avg_overflow',NULL,30,30,1,20,'avg')",
        [],
        |row| row.get(0),
    )?;
    if overflow.to_bits() != f64::MAX.to_bits() {
        bail!("SQL-PROM-002 overflow-safe average changed: {overflow:?}");
    }

    let minimum: f64 = connection.query_row(
        "SELECT value FROM timeless_window(
           'metrics','min_window',NULL,30,30,1,20,'min')",
        [],
        |row| row.get(0),
    )?;
    let maximum: f64 = connection.query_row(
        "SELECT value FROM timeless_window(
           'metrics','max_window',NULL,30,30,1,20,'max')",
        [],
        |row| row.get(0),
    )?;
    if minimum != 3.0 || maximum != 4.0 {
        bail!("SQL-PROM-021/022 extrema changed: min={minimum}, max={maximum}");
    }
    let min_zero: f64 = connection.query_row(
        "SELECT value FROM timeless_window(
           'metrics','min_zero',NULL,30,30,1,20,'min')",
        [],
        |row| row.get(0),
    )?;
    let max_zero: f64 = connection.query_row(
        "SELECT value FROM timeless_window(
           'metrics','max_zero',NULL,30,30,1,20,'max')",
        [],
        |row| row.get(0),
    )?;
    if min_zero.to_bits() != 0.0_f64.to_bits() || max_zero.to_bits() != (-0.0_f64).to_bits() {
        bail!("SQL extrema signed-zero contract changed");
    }

    let cross_sum: Vec<(i64, f64)> = {
        let mut statement = connection.prepare(
            "WITH selected AS (
               SELECT ts,value FROM timeless_grid('metrics','cpu',NULL,100,110,10,20)
             ) SELECT ts,SUM(value) FROM selected GROUP BY ts ORDER BY ts",
        )?;
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        rows
    };
    if cross_sum != [(100, 30.0), (110, 50.0)] {
        bail!("SQL-PROM-003 cross-series sum changed: {cross_sum:?}");
    }

    let ratio: f64 = connection.query_row(
        "WITH errors AS (
           SELECT json_extract(labels,'$.host') host,value
           FROM timeless_grid('metrics','errors_total',NULL,100,100,1,20)
         ), requests AS (
           SELECT json_extract(labels,'$.host') host,value
           FROM timeless_grid('metrics','requests_total',NULL,100,100,1,20)
         ) SELECT errors.value/requests.value FROM errors JOIN requests USING(host)",
        [],
        |row| row.get(0),
    )?;
    if ratio.to_bits() != 0.2_f64.to_bits() {
        bail!("SQL-PROM-004 vector arithmetic changed: {ratio}");
    }

    let histogram_sql = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-PROM-054")
        .context("SQL-PROM-054 recipe")?
        .statements
        .first()
        .context("SQL-PROM-054 statement")?;
    let mut statement = connection.prepare(histogram_sql)?;
    for index in 1..=statement.parameter_count() {
        let name = statement
            .parameter_name(index)
            .unwrap()
            .trim_start_matches(':');
        statement.raw_bind_parameter(index, parameter("SQL-PROM-054", name))?;
    }
    let histogram: Option<(String, i64, f64)> = {
        let mut rows = statement.raw_query();
        rows.next()?
            .map(|row| {
                row.get::<_, String>(0)
                    .and_then(|labels| Ok((labels, row.get(1)?, row.get(2)?)))
            })
            .transpose()?
    };
    if histogram != Some((r#"{"host":"web-1"}"#.to_owned(), 100, 0.5)) {
        bail!("SQL-PROM-054 histogram result changed: {histogram:?}");
    }

    let atan_recipe = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-PROM-055")
        .context("SQL-PROM-055 recipe")?;
    if atan_recipe.statements.len() != 2 {
        bail!("SQL-PROM-055 must retain scalar/vector and vector/vector statements");
    }
    for (statement_index, expected_rows) in [(0, 4_usize), (1, 4_usize)] {
        let mut statement = connection.prepare(&atan_recipe.statements[statement_index])?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .unwrap()
                .trim_start_matches(':');
            statement.raw_bind_parameter(index, parameter("SQL-PROM-055", name))?;
        }
        let values = statement
            .raw_query()
            .mapped(|row| row.get::<_, f64>(2))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if values.len() != expected_rows || values.iter().any(|value| !value.is_finite()) {
            bail!(
                "SQL-PROM-055 statement {} changed: {values:?}",
                statement_index + 1
            );
        }
    }

    let fraction_sql = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-PROM-056")
        .context("SQL-PROM-056 recipe")?
        .statements
        .first()
        .context("SQL-PROM-056 statement")?;
    let mut statement = connection.prepare(fraction_sql)?;
    for index in 1..=statement.parameter_count() {
        let name = statement
            .parameter_name(index)
            .unwrap()
            .trim_start_matches(':');
        statement.raw_bind_parameter(index, parameter("SQL-PROM-056", name))?;
    }
    let fractions = statement
        .raw_query()
        .mapped(|row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, f64>(2)?,
            ))
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if fractions
        != [
            (r#"{"host":"web-1"}"#.to_owned(), 100, 0.25),
            (r#"{"host":"web-1"}"#.to_owned(), 110, 0.25),
        ]
    {
        bail!("SQL-PROM-056 histogram fraction changed: {fractions:?}");
    }

    let metricsql_recipe = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-MQL-001")
        .context("SQL-MQL-001 recipe")?;
    if metricsql_recipe.statements.len() != 3 {
        bail!("SQL-MQL-001 must retain default, if, and ifnot statements");
    }
    let expected_rows = [
        vec![
            (r#"{"host":"web-1","service":"api"}"#, 100_i64, 0.0_f64),
            (r#"{"host":"web-1","service":"api"}"#, 110_i64, 30.0_f64),
            (r#"{"host":"web-2","service":"api"}"#, 100_i64, 20.0_f64),
            (r#"{"host":"web-2","service":"api"}"#, 110_i64, 20.0_f64),
        ],
        vec![
            (r#"{"host":"web-1","service":"api"}"#, 100_i64, 10.0_f64),
            (r#"{"host":"web-1","service":"api"}"#, 110_i64, 30.0_f64),
        ],
        vec![
            (r#"{"host":"web-2","service":"api"}"#, 100_i64, 20.0_f64),
            (r#"{"host":"web-2","service":"api"}"#, 110_i64, 20.0_f64),
        ],
    ];
    for (ordinal, expected) in expected_rows.iter().enumerate() {
        let mut statement = connection.prepare(&metricsql_recipe.statements[ordinal])?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-MQL-001 parameter must be named")?
                .trim_start_matches(':');
            statement.raw_bind_parameter(index, parameter("SQL-MQL-001", name))?;
        }
        let rows = statement
            .raw_query()
            .mapped(|row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, f64>(2)?,
                ))
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let expected = expected
            .iter()
            .map(|(labels, ts, value)| ((*labels).to_owned(), *ts, *value))
            .collect::<Vec<_>>();
        if rows != expected {
            bail!("SQL-MQL-001 statement {} changed: {rows:?}", ordinal + 1);
        }
    }

    let keep_names_sql = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-MQL-002")
        .context("SQL-MQL-002 recipe")?
        .statements
        .first()
        .context("SQL-MQL-002 statement")?;
    let mut statement = connection.prepare(keep_names_sql)?;
    for index in 1..=statement.parameter_count() {
        let name = statement
            .parameter_name(index)
            .context("SQL-MQL-002 parameter must be named")?
            .trim_start_matches(':');
        statement.raw_bind_parameter(index, parameter("SQL-MQL-002", name))?;
    }
    let rows = statement
        .raw_query()
        .mapped(|row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, f64>(3)?,
            ))
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let expected = [
        (
            "cpu",
            r#"{"host":"web-1","service":"api"}"#,
            100_i64,
            10.0_f64,
        ),
        (
            "cpu",
            r#"{"host":"web-1","service":"api"}"#,
            110_i64,
            30.0_f64,
        ),
        (
            "cpu",
            r#"{"host":"web-2","service":"api"}"#,
            100_i64,
            20.0_f64,
        ),
        (
            "cpu",
            r#"{"host":"web-2","service":"api"}"#,
            110_i64,
            20.0_f64,
        ),
    ]
    .map(|(name, labels, ts, value)| (name.to_owned(), labels.to_owned(), ts, value));
    if rows != expected {
        bail!("SQL-MQL-002 keep_metric_names changed: {rows:?}");
    }

    let union_alias_recipe = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-MQL-003")
        .context("SQL-MQL-003 recipe")?;
    if union_alias_recipe.statements.len() != 3 {
        bail!("SQL-MQL-003 must retain union, alias, and collision statements");
    }
    let expected_rows = [
        vec![
            (
                "cpu",
                r#"{"host":"web-1","service":"api"}"#,
                100_i64,
                10.0_f64,
            ),
            (
                "cpu",
                r#"{"host":"web-1","service":"api"}"#,
                110_i64,
                30.0_f64,
            ),
            (
                "cpu",
                r#"{"host":"web-2","service":"api"}"#,
                100_i64,
                20.0_f64,
            ),
            (
                "cpu",
                r#"{"host":"web-2","service":"api"}"#,
                110_i64,
                20.0_f64,
            ),
            ("requests_total", r#"{"host":"web-1"}"#, 100_i64, 10.0_f64),
            ("requests_total", r#"{"host":"web-1"}"#, 110_i64, 10.0_f64),
        ],
        vec![
            (
                "aliased_cpu",
                r#"{"host":"web-1","service":"api"}"#,
                100_i64,
                10.0_f64,
            ),
            (
                "aliased_cpu",
                r#"{"host":"web-1","service":"api"}"#,
                110_i64,
                30.0_f64,
            ),
            (
                "aliased_cpu",
                r#"{"host":"web-2","service":"api"}"#,
                100_i64,
                20.0_f64,
            ),
            (
                "aliased_cpu",
                r#"{"host":"web-2","service":"api"}"#,
                110_i64,
                20.0_f64,
            ),
        ],
        vec![
            ("combined_total", r#"{"host":"web-1"}"#, 100_i64, 2.0_f64),
            ("combined_total", r#"{"host":"web-1"}"#, 110_i64, 2.0_f64),
        ],
    ];
    for (ordinal, expected) in expected_rows.iter().enumerate() {
        let mut statement = connection.prepare(&union_alias_recipe.statements[ordinal])?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-MQL-003 parameter must be named")?
                .trim_start_matches(':');
            statement.raw_bind_parameter(index, parameter("SQL-MQL-003", name))?;
        }
        let rows = statement
            .raw_query()
            .mapped(|row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, f64>(3)?,
                ))
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let expected = expected
            .iter()
            .map(|(name, labels, ts, value)| {
                ((*name).to_owned(), (*labels).to_owned(), *ts, *value)
            })
            .collect::<Vec<_>>();
        if rows != expected {
            bail!("SQL-MQL-003 statement {} changed: {rows:?}", ordinal + 1);
        }
    }

    let label_recipe = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-MQL-004")
        .context("SQL-MQL-004 recipe")?;
    if label_recipe.statements.len() != 2 {
        bail!("SQL-MQL-004 must retain label_set and label_del statements");
    }
    let expected_rows = [
        vec![
            (
                Some("cpu"),
                r#"{"host":"web-1","service":"api","environment":"production"}"#,
                100_i64,
                10.0_f64,
            ),
            (
                Some("cpu"),
                r#"{"host":"web-1","service":"api","environment":"production"}"#,
                110_i64,
                30.0_f64,
            ),
            (
                Some("cpu"),
                r#"{"host":"web-2","service":"api","environment":"production"}"#,
                100_i64,
                20.0_f64,
            ),
            (
                Some("cpu"),
                r#"{"host":"web-2","service":"api","environment":"production"}"#,
                110_i64,
                20.0_f64,
            ),
        ],
        vec![
            (Some("cpu"), r#"{"host":"web-1"}"#, 100_i64, 10.0_f64),
            (Some("cpu"), r#"{"host":"web-1"}"#, 110_i64, 30.0_f64),
            (Some("cpu"), r#"{"host":"web-2"}"#, 100_i64, 20.0_f64),
            (Some("cpu"), r#"{"host":"web-2"}"#, 110_i64, 20.0_f64),
        ],
    ];
    for (ordinal, expected) in expected_rows.iter().enumerate() {
        let mut statement = connection.prepare(&label_recipe.statements[ordinal])?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-MQL-004 parameter must be named")?
                .trim_start_matches(':');
            statement.raw_bind_parameter(index, parameter("SQL-MQL-004", name))?;
        }
        let rows = statement
            .raw_query()
            .mapped(|row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, f64>(3)?,
                ))
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let expected = expected
            .iter()
            .map(|(name, labels, ts, value)| {
                (name.map(str::to_owned), (*labels).to_owned(), *ts, *value)
            })
            .collect::<Vec<_>>();
        if rows != expected {
            bail!("SQL-MQL-004 statement {} changed: {rows:?}", ordinal + 1);
        }
    }

    for (ordinal, argument_name) in [(0_usize, "label_name"), (1_usize, "delete_label")] {
        let mut statement = connection.prepare(&label_recipe.statements[ordinal])?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-MQL-004 name parameter")?
                .trim_start_matches(':');
            let value = if name == argument_name {
                Value::Text("__name__".to_owned())
            } else if name == "label_value" {
                Value::Text(String::new())
            } else {
                parameter("SQL-MQL-004", name)
            };
            statement.raw_bind_parameter(index, value)?;
        }
        let name = statement
            .raw_query()
            .next()?
            .context("SQL-MQL-004 name projection row")?
            .get::<_, Option<String>>(0)?;
        if name.is_some() {
            bail!(
                "SQL-MQL-004 statement {} did not delete __name__",
                ordinal + 1
            );
        }
    }

    let rollup_recipe = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-MQL-005")
        .context("SQL-MQL-005 recipe")?;
    if rollup_recipe.statements.len() != 2 {
        bail!("SQL-MQL-005 must retain default and window statements");
    }
    let expected_rows = [
        vec![
            (
                "cpu",
                r#"{"host":"web-1","service":"api"}"#,
                100_i64,
                10.0_f64,
            ),
            (
                "cpu",
                r#"{"host":"web-1","service":"api"}"#,
                110_i64,
                30.0_f64,
            ),
            (
                "cpu",
                r#"{"host":"web-2","service":"api"}"#,
                100_i64,
                20.0_f64,
            ),
            (
                "cpu",
                r#"{"host":"web-2","service":"api"}"#,
                110_i64,
                20.0_f64,
            ),
        ],
        vec![
            (
                "cpu",
                r#"{"host":"web-1","service":"api"}"#,
                100_i64,
                10.0_f64,
            ),
            (
                "cpu",
                r#"{"host":"web-1","service":"api"}"#,
                110_i64,
                30.0_f64,
            ),
            (
                "cpu",
                r#"{"host":"web-2","service":"api"}"#,
                100_i64,
                20.0_f64,
            ),
        ],
    ];
    for (ordinal, expected) in expected_rows.iter().enumerate() {
        let mut statement = connection.prepare(&rollup_recipe.statements[ordinal])?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-MQL-005 parameter must be named")?
                .trim_start_matches(':');
            statement.raw_bind_parameter(index, parameter("SQL-MQL-005", name))?;
        }
        let rows = statement
            .raw_query()
            .mapped(|row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, f64>(3)?,
                ))
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let expected = expected
            .iter()
            .map(|(name, labels, ts, value)| {
                ((*name).to_owned(), (*labels).to_owned(), *ts, *value)
            })
            .collect::<Vec<_>>();
        if rows != expected {
            bail!("SQL-MQL-005 statement {} changed: {rows:?}", ordinal + 1);
        }
    }

    let range_recipe = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-MQL-006")
        .context("SQL-MQL-006 recipe")?;
    if range_recipe.statements.len() != 1 {
        bail!("SQL-MQL-006 must retain one parameterized range statement");
    }
    for (aggregate, expected_values) in [
        ("avg", [20.0_f64, 20.0_f64]),
        ("min", [10.0_f64, 20.0_f64]),
        ("max", [30.0_f64, 20.0_f64]),
        ("sum", [40.0_f64, 40.0_f64]),
    ] {
        let mut statement = connection.prepare(&range_recipe.statements[0])?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-MQL-006 parameter must be named")?
                .trim_start_matches(':');
            let value = if name == "aggregate" {
                Value::Text(aggregate.to_owned())
            } else {
                parameter("SQL-MQL-006", name)
            };
            statement.raw_bind_parameter(index, value)?;
        }
        let rows = statement
            .raw_query()
            .mapped(|row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, f64>(3)?,
                ))
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let expected = [
            (
                None,
                r#"{"host":"web-1","service":"api"}"#.to_owned(),
                100_i64,
                expected_values[0],
            ),
            (
                None,
                r#"{"host":"web-1","service":"api"}"#.to_owned(),
                110_i64,
                expected_values[0],
            ),
            (
                None,
                r#"{"host":"web-2","service":"api"}"#.to_owned(),
                100_i64,
                expected_values[1],
            ),
            (
                None,
                r#"{"host":"web-2","service":"api"}"#.to_owned(),
                110_i64,
                expected_values[1],
            ),
        ];
        if rows != expected {
            bail!("SQL-MQL-006 {aggregate} changed: {rows:?}");
        }
    }

    let mut precision_statement = connection.prepare(&range_recipe.statements[0])?;
    for index in 1..=precision_statement.parameter_count() {
        let name = precision_statement
            .parameter_name(index)
            .context("SQL-MQL-006 precision parameter must be named")?
            .trim_start_matches(':');
        let value = match name {
            "aggregate" => Value::Text("avg".to_owned()),
            "metric" => Value::Text("avg_precision".to_owned()),
            "start" => Value::Integer(10),
            "end" => Value::Integer(30),
            "step" => Value::Integer(10),
            "lookback" => Value::Integer(30),
            _ => parameter("SQL-MQL-006", name),
        };
        precision_statement.raw_bind_parameter(index, value)?;
    }
    let precision = precision_statement
        .raw_query()
        .mapped(|row| row.get::<_, f64>(3))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if precision.len() != 3
        || precision
            .iter()
            .any(|value| value.to_bits() != 0.0_f64.to_bits())
    {
        bail!("SQL-MQL-006 slot-indexed average changed: {precision:?}");
    }

    let running_recipe = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-MQL-007")
        .context("SQL-MQL-007 recipe")?;
    if running_recipe.statements.len() != 1 {
        bail!("SQL-MQL-007 must retain one parameterized running statement");
    }
    for (aggregate, expected_values) in [
        ("avg", [10.0_f64, 20.0_f64, 20.0_f64, 20.0_f64]),
        ("min", [10.0_f64, 10.0_f64, 20.0_f64, 20.0_f64]),
        ("max", [10.0_f64, 30.0_f64, 20.0_f64, 20.0_f64]),
        ("sum", [10.0_f64, 40.0_f64, 20.0_f64, 40.0_f64]),
    ] {
        let mut statement = connection.prepare(&running_recipe.statements[0])?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-MQL-007 parameter must be named")?
                .trim_start_matches(':');
            let value = if name == "aggregate" {
                Value::Text(aggregate.to_owned())
            } else {
                parameter("SQL-MQL-007", name)
            };
            statement.raw_bind_parameter(index, value)?;
        }
        let rows = statement
            .raw_query()
            .mapped(|row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, f64>(3)?,
                ))
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let expected = [
            (
                None,
                r#"{"host":"web-1","service":"api"}"#.to_owned(),
                100_i64,
                expected_values[0],
            ),
            (
                None,
                r#"{"host":"web-1","service":"api"}"#.to_owned(),
                110_i64,
                expected_values[1],
            ),
            (
                None,
                r#"{"host":"web-2","service":"api"}"#.to_owned(),
                100_i64,
                expected_values[2],
            ),
            (
                None,
                r#"{"host":"web-2","service":"api"}"#.to_owned(),
                110_i64,
                expected_values[3],
            ),
        ];
        if rows != expected {
            bail!("SQL-MQL-007 {aggregate} changed: {rows:?}");
        }
    }

    let mut running_precision = connection.prepare(&running_recipe.statements[0])?;
    for index in 1..=running_precision.parameter_count() {
        let name = running_precision
            .parameter_name(index)
            .context("SQL-MQL-007 precision parameter must be named")?
            .trim_start_matches(':');
        let value = match name {
            "aggregate" => Value::Text("avg".to_owned()),
            "metric" => Value::Text("avg_precision".to_owned()),
            "start" => Value::Integer(10),
            "end" => Value::Integer(30),
            "step" => Value::Integer(10),
            "lookback" => Value::Integer(30),
            _ => parameter("SQL-MQL-007", name),
        };
        running_precision.raw_bind_parameter(index, value)?;
    }
    let running_precision = running_precision
        .raw_query()
        .mapped(|row| row.get::<_, f64>(3))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if running_precision.len() != 3
        || running_precision[0].to_bits() != 1e16_f64.to_bits()
        || running_precision[1].to_bits() != 5e15_f64.to_bits()
        || running_precision[2].to_bits() != 0.0_f64.to_bits()
    {
        bail!("SQL-MQL-007 slot-indexed average changed: {running_precision:?}");
    }

    let relative_recipe = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-MQL-009")
        .context("SQL-MQL-009 recipe")?;
    if relative_recipe.statements.len() != 2 {
        bail!("SQL-MQL-009 must retain window and offset statements");
    }
    let expected_relative_rows = [
        vec![
            (
                None,
                r#"{"host":"web-1","service":"api"}"#.to_owned(),
                100_i64,
                1.0_f64,
            ),
            (
                None,
                r#"{"host":"web-1","service":"api"}"#.to_owned(),
                110_i64,
                1.0_f64,
            ),
            (
                None,
                r#"{"host":"web-2","service":"api"}"#.to_owned(),
                100_i64,
                1.0_f64,
            ),
        ],
        vec![
            (
                Some("cpu".to_owned()),
                r#"{"host":"web-1","service":"api"}"#.to_owned(),
                110_i64,
                10.0_f64,
            ),
            (
                Some("cpu".to_owned()),
                r#"{"host":"web-2","service":"api"}"#.to_owned(),
                110_i64,
                20.0_f64,
            ),
        ],
    ];
    for (ordinal, expected) in expected_relative_rows.iter().enumerate() {
        let mut statement = connection.prepare(&relative_recipe.statements[ordinal])?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-MQL-009 parameter must be named")?
                .trim_start_matches(':');
            statement.raw_bind_parameter(index, parameter("SQL-MQL-009", name))?;
        }
        let rows = statement
            .raw_query()
            .mapped(|row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, f64>(3)?,
                ))
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if rows != *expected {
            bail!("SQL-MQL-009 statement {} changed: {rows:?}", ordinal + 1);
        }
    }

    let context_recipe = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-MQL-010")
        .context("SQL-MQL-010 recipe")?;
    if context_recipe.statements.len() != 1 {
        bail!("SQL-MQL-010 must retain one parameterized context statement");
    }
    let mut context_statement = connection.prepare(&context_recipe.statements[0])?;
    for index in 1..=context_statement.parameter_count() {
        let name = context_statement
            .parameter_name(index)
            .context("SQL-MQL-010 parameter must be named")?
            .trim_start_matches(':');
        let value = match name {
            "start_ms" => Value::Integer(-500),
            "end_ms" => Value::Integer(2_500),
            "step_ms" => Value::Integer(1_500),
            _ => parameter("SQL-MQL-010", name),
        };
        context_statement.raw_bind_parameter(index, value)?;
    }
    let context_rows = context_statement
        .raw_query()
        .mapped(|row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3)?,
            ))
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let expected_context_rows = [
        (-500_i64, -0.5_f64, 2.5_f64, 1.5_f64),
        (1_000_i64, -0.5_f64, 2.5_f64, 1.5_f64),
        (2_500_i64, -0.5_f64, 2.5_f64, 1.5_f64),
    ];
    if context_rows != expected_context_rows {
        bail!("SQL-MQL-010 request context changed: {context_rows:?}");
    }

    let histogram_quantiles_recipe = recipes
        .iter()
        .find(|recipe| recipe.identifier == "SQL-MQL-012")
        .context("SQL-MQL-012 recipe")?;
    if histogram_quantiles_recipe.statements.len() != 1 {
        bail!("SQL-MQL-012 must retain one parameterized histogram statement");
    }
    let mut histogram_quantiles = connection.prepare(&histogram_quantiles_recipe.statements[0])?;
    for index in 1..=histogram_quantiles.parameter_count() {
        let name = histogram_quantiles
            .parameter_name(index)
            .context("SQL-MQL-012 parameter must be named")?
            .trim_start_matches(':');
        histogram_quantiles.raw_bind_parameter(index, parameter("SQL-MQL-012", name))?;
    }
    let histogram_quantile_rows = histogram_quantiles
        .raw_query()
        .mapped(|row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, f64>(2)?,
            ))
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let expected_histogram_quantile_rows = [
        (
            r#"{"host":"web-1","phi":"0.25"}"#.to_owned(),
            100_i64,
            0.1_f64,
        ),
        (
            r#"{"host":"web-1","phi":"0.25"}"#.to_owned(),
            110_i64,
            0.1_f64,
        ),
        (
            r#"{"host":"web-1","phi":"0.75"}"#.to_owned(),
            100_i64,
            1.0_f64,
        ),
        (
            r#"{"host":"web-1","phi":"0.75"}"#.to_owned(),
            110_i64,
            1.0_f64,
        ),
    ];
    if histogram_quantile_rows != expected_histogram_quantile_rows {
        bail!("SQL-MQL-012 histogram quantiles changed: {histogram_quantile_rows:?}");
    }

    let bounded: i64 = connection.query_row(
        "SELECT COUNT(*) FROM logs
         WHERE ts>=1000 AND ts<=2000 AND level='error' AND service='api'
           AND max_work_entries=100000",
        [],
        |row| row.get(0),
    )?;
    let substring: i64 = connection.query_row(
        "SELECT COUNT(*) FROM logs WHERE message_contains='TIMEOUT'",
        [],
        |row| row.get(0),
    )?;
    let count: i64 = connection.query_row(
        "SELECT n FROM timeless_log_count(
           'logs','{\"level\":\"error\",\"service\":\"api\"}',NULL,1000,2000,100000)",
        [],
        |row| row.get(0),
    )?;
    let values: i64 = connection.query_row(
        "SELECT COUNT(*) FROM timeless_log_values(
           'logs','host',NULL,NULL,1000,2000,100,100000)",
        [],
        |row| row.get(0),
    )?;
    let compatible_count: i64 =
        connection.query_row("SELECT n FROM timeless_log_count('logs')", [], |row| {
            row.get(0)
        })?;
    let compatible_values: i64 = connection.query_row(
        "SELECT COUNT(*) FROM timeless_log_values(
           'logs','host',NULL,NULL,1000,2000,100)",
        [],
        |row| row.get(0),
    )?;
    let nested: i64 = connection.query_row(
        "SELECT COUNT(*) FROM logs
         WHERE json_type(metadata,'$.deployment.region')='text'
           AND json_extract(metadata,'$.deployment.region')='us-east'",
        [],
        |row| row.get(0),
    )?;
    let typed: i64 = connection.query_row(
        "SELECT COUNT(*) FROM logs
         WHERE json_type(metadata,'$.nested.ok')='true'
           AND json_extract(metadata,'$.nested.ok')=1
           AND json_type(metadata,'$.nested.count')='integer'
           AND json_extract(metadata,'$.nested.count')=2
           AND json_type(metadata,'$.nested.none')='null'
           AND json_type(metadata,'$.nested.empty')='text'
           AND json_extract(metadata,'$.nested.empty')=''",
        [],
        |row| row.get(0),
    )?;
    let missing: i64 = connection.query_row(
        "SELECT COUNT(*) FROM logs WHERE json_type(metadata,'$.nested.none') IS NULL",
        [],
        |row| row.get(0),
    )?;
    let buckets: i64 = connection.query_row(
        "SELECT SUM(n) FROM timeless_log_buckets('logs','level',NULL,1000,2000,1000)",
        [],
        |row| row.get(0),
    )?;
    let recipe_sql = |identifier: &str, statement_index: usize| -> Result<String> {
        let recipe = recipes
            .iter()
            .find(|recipe| recipe.identifier == identifier)
            .with_context(|| format!("{identifier} recipe"))?;
        let statements = recipe
            .statements
            .iter()
            .map(|block| split_sql(block))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let sql = statements
            .get(statement_index)
            .with_context(|| format!("{identifier} statement {}", statement_index + 1))?;
        Ok(sql.clone())
    };
    let recipe_values = |identifier: &str, statement_index: usize| -> Result<Vec<Vec<Value>>> {
        let sql = recipe_sql(identifier, statement_index)?;
        let mut statement = connection
            .prepare(&sql)
            .with_context(|| format!("prepare {identifier} statement {}", statement_index + 1))?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("documented SQL parameter must be named")?
                .trim_start_matches(':');
            statement.raw_bind_parameter(index, parameter(identifier, name))?;
        }
        let columns = statement.column_count();
        let mut query = statement.raw_query();
        let mut output = Vec::new();
        while let Some(row) = query.next()? {
            output.push(
                (0..columns)
                    .map(|column| row.get(column))
                    .collect::<rusqlite::Result<Vec<Value>>>()?,
            );
        }
        Ok(output)
    };
    let recipe_rows = |identifier: &str, statement_index: usize| -> Result<usize> {
        Ok(recipe_values(identifier, statement_index)?.len())
    };
    let session_twelve_recipe_rows = [
        recipe_rows("SQL-LOG-007", 0)?,
        recipe_rows("SQL-LOG-008", 0)?,
        recipe_rows("SQL-LOG-008", 1)?,
        recipe_rows("SQL-LOG-008", 2)?,
        recipe_rows("SQL-LOG-009", 0)?,
    ];
    let session_thirteen_recipe_rows = [
        recipe_rows("SQL-LOG-010", 0)?,
        recipe_rows("SQL-LOG-010", 1)?,
        recipe_rows("SQL-LOG-011", 0)?,
        recipe_rows("SQL-LOG-011", 1)?,
        recipe_rows("SQL-LOG-012", 0)?,
        recipe_rows("SQL-LOG-012", 1)?,
        recipe_rows("SQL-LOG-012", 2)?,
        recipe_rows("SQL-LOG-013", 0)?,
        recipe_rows("SQL-LOG-013", 1)?,
    ];
    let exact_prefix_rows = [
        recipe_values("SQL-LOG-014", 0)?,
        recipe_values("SQL-LOG-014", 1)?,
    ];
    let multi_exact_rows = [
        recipe_values("SQL-LOG-015", 0)?,
        recipe_values("SQL-LOG-015", 1)?,
        recipe_values("SQL-LOG-015", 2)?,
    ];
    let field_noop_rows = recipe_values("SQL-LOG-016", 0)?;
    let json_array_rows = recipe_values("SQL-LOG-017", 0)?;
    let json_array_empty_rows = recipe_values("SQL-LOG-017", 1)?;
    let ipv4_range_rows = recipe_values("SQL-LOG-018", 0)?;
    let string_range_rows = recipe_values("SQL-LOG-019", 0)?;
    let len_range_rows = recipe_values("SQL-LOG-020", 0)?;
    let field_compare_rows = recipe_values("SQL-LOG-021", 0)?;
    let field_prefix_rows = recipe_values("SQL-LOG-022", 0)?;
    let day_range_rows = recipe_values("SQL-LOG-023", 0)?;
    let week_range_rows = recipe_values("SQL-LOG-024", 0)?;
    let delete_rows = recipe_values("SQL-LOG-025", 0)?;
    let query_stats_scan = recipe_values("SQL-LOG-026", 0)?;
    let query_stats_rows = recipe_values("SQL-LOG-026", 1)?;
    let first_rows = recipe_values("SQL-LOG-027", 0)?;
    let last_rows = recipe_values("SQL-LOG-028", 0)?;
    let top_rows = recipe_values("SQL-LOG-029", 0)?;
    let uniq_rows = recipe_values("SQL-LOG-030", 0)?;
    let facet_rows = recipe_values("SQL-LOG-031", 0)?;
    let coalesce_rows = recipe_values("SQL-LOG-032", 0)?;
    let copy_rows = recipe_values("SQL-LOG-033", 0)?;
    let rename_rows = recipe_values("SQL-LOG-034", 0)?;
    let format_rows = recipe_values("SQL-LOG-035", 0)?;
    let math_rows = recipe_values("SQL-LOG-036", 0)?;
    let len_rows = recipe_values("SQL-LOG-037", 0)?;
    let drop_empty_rows = recipe_values("SQL-LOG-038", 0)?;
    let replace_rows = recipe_values("SQL-LOG-039", 0)?;
    let extract_rows = recipe_values("SQL-LOG-040", 0)?;
    let pack_json_rows = recipe_values("SQL-LOG-041", 0)?;
    let unpack_json_rows = recipe_values("SQL-LOG-042", 0)?;
    let json_array_len_rows = recipe_values("SQL-LOG-043", 0)?;
    let quantile_stddev_rows = recipe_values("SQL-LOG-044", 0)?;
    let sum_len_rows = recipe_values("SQL-LOG-045", 0)?;
    let any_rows = recipe_values("SQL-LOG-046", 0)?;
    let field_extrema_rows = recipe_values("SQL-LOG-046", 1)?;
    let row_any_rows = recipe_values("SQL-LOG-047", 0)?;
    let row_extrema_rows = recipe_values("SQL-LOG-047", 1)?;
    if [
        bounded,
        substring,
        count,
        values,
        compatible_count,
        compatible_values,
        nested,
        typed,
        missing,
        buckets,
    ] != [1, 1, 1, 2, 2, 2, 1, 1, 1, 2]
    {
        bail!(
            "SQL-LOG recipe results changed: {:?}",
            [
                bounded,
                substring,
                count,
                values,
                compatible_count,
                compatible_values,
                nested,
                typed,
                missing,
                buckets
            ]
        );
    }
    if session_twelve_recipe_rows != [1, 1, 2, 2, 1] {
        bail!("Session 12 SQL-LOG recipe results changed: {session_twelve_recipe_rows:?}");
    }
    if session_thirteen_recipe_rows != [10, 2, 2, 1, 1, 2, 2, 1, 1] {
        bail!("Session 13 SQL-LOG recipe results changed: {session_thirteen_recipe_rows:?}");
    }
    for (ordinal, rows) in exact_prefix_rows.iter().enumerate() {
        let timestamps = rows
            .iter()
            .map(|row| row.first().cloned())
            .collect::<Vec<_>>();
        if timestamps != [Some(Value::Integer(2000)), Some(Value::Integer(1000))] {
            bail!("SQL-LOG-014 statement {} changed: {rows:?}", ordinal + 1);
        }
    }
    for (ordinal, rows) in multi_exact_rows.iter().enumerate() {
        let timestamps = rows
            .iter()
            .map(|row| row.first().cloned())
            .collect::<Vec<_>>();
        if timestamps != [Some(Value::Integer(2000)), Some(Value::Integer(1000))] {
            bail!("SQL-LOG-015 statement {} changed: {rows:?}", ordinal + 1);
        }
    }
    let field_noop_timestamps = field_noop_rows
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if field_noop_timestamps != [Some(Value::Integer(2000)), Some(Value::Integer(1000))] {
        bail!("SQL-LOG-016 field no-op changed: {field_noop_rows:?}");
    }
    let json_array_timestamps = json_array_rows
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if json_array_timestamps != [Some(Value::Integer(1000))] || !json_array_empty_rows.is_empty() {
        bail!(
            "SQL-LOG-017 primitive or empty-list membership changed: rows={json_array_rows:?} empty={json_array_empty_rows:?}"
        );
    }
    let ipv4_range_timestamps = ipv4_range_rows
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if ipv4_range_timestamps != [Some(Value::Integer(1000))] {
        bail!("SQL-LOG-018 IPv4 range changed: {ipv4_range_rows:?}");
    }
    let string_range_timestamps = string_range_rows
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if string_range_timestamps != [Some(Value::Integer(2000)), Some(Value::Integer(1000))] {
        bail!("SQL-LOG-019 string range changed: {string_range_rows:?}");
    }
    let len_range_timestamps = len_range_rows
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if len_range_timestamps != [Some(Value::Integer(2000)), Some(Value::Integer(1000))] {
        bail!("SQL-LOG-020 length range changed: {len_range_rows:?}");
    }
    let field_compare_timestamps = field_compare_rows
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if field_compare_timestamps != [Some(Value::Integer(2000)), Some(Value::Integer(1000))] {
        bail!("SQL-LOG-021 field equality changed: {field_compare_rows:?}");
    }
    let field_prefix_timestamps = field_prefix_rows
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if field_prefix_timestamps != [Some(Value::Integer(1000))] {
        bail!("SQL-LOG-022 field-prefix selection changed: {field_prefix_rows:?}");
    }
    let day_range_timestamps = day_range_rows
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if day_range_timestamps != [Some(Value::Integer(2000)), Some(Value::Integer(1000))] {
        bail!("SQL-LOG-023 UTC day-range selection changed: {day_range_rows:?}");
    }
    let week_range_timestamps = week_range_rows
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if week_range_timestamps != [Some(Value::Integer(2000)), Some(Value::Integer(1000))] {
        bail!("SQL-LOG-024 UTC week-range selection changed: {week_range_rows:?}");
    }
    let delete_timestamps = delete_rows
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if delete_timestamps != [Some(Value::Integer(2000)), Some(Value::Integer(1000))] {
        bail!("SQL-LOG-025 exact deletion ordering changed: {delete_rows:?}");
    }
    for row in &delete_rows {
        let Value::Text(metadata) = &row[3] else {
            bail!("SQL-LOG-025 metadata projection is not JSON text: {row:?}");
        };
        let metadata: serde_json::Value = serde_json::from_str(metadata)?;
        if metadata.pointer("/deployment/region").is_some()
            || metadata.pointer("/duration_ms").is_some()
            || metadata.pointer("/service").is_none()
        {
            bail!("SQL-LOG-025 exact deletion or retained metadata changed: {metadata}");
        }
    }
    let query_stats_timestamps = query_stats_scan
        .iter()
        .map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    if query_stats_timestamps != [Some(Value::Integer(1000)), Some(Value::Integer(2000))] {
        bail!("SQL-LOG-026 source query ordering changed: {query_stats_scan:?}");
    }
    let [query_stats] = query_stats_rows.as_slice() else {
        bail!("SQL-LOG-026 must return exactly one report row: {query_stats_rows:?}");
    };
    if query_stats.len() != 14
        || !query_stats
            .iter()
            .all(|value| matches!(value, Value::Text(_)))
    {
        bail!("SQL-LOG-026 report schema/type changed: {query_stats:?}");
    }
    let text_u64 = |index: usize| -> Result<u64> {
        let Value::Text(value) = &query_stats[index] else {
            bail!("SQL-LOG-026 column {index} is not TEXT")
        };
        value
            .parse::<u64>()
            .with_context(|| format!("SQL-LOG-026 column {index} is not an unsigned integer"))
    };
    if text_u64(0)? != 0
        || text_u64(1)? != 0
        || text_u64(2)? != 0
        || text_u64(4)? != 0
        || text_u64(5)? != 0
        || text_u64(12)? != 0
        || text_u64(3)? == 0
        || text_u64(3)? != text_u64(6)?
        || text_u64(7)? != 2
        || text_u64(8)? != 2
        || text_u64(9)? != 2
        || text_u64(10)? != 6
        || text_u64(11)? != 2
        || text_u64(13)? == 0
    {
        bail!("SQL-LOG-026 request-local report changed: {query_stats:?}");
    }
    let [first_row] = first_rows.as_slice() else {
        bail!("SQL-LOG-027 first-per-partition result changed: {first_rows:?}");
    };
    if first_row.len() != 5
        || first_row[0] != Value::Integer(2000)
        || first_row[1] != Value::Text("info".to_owned())
        || first_row[2] != Value::Text("request ok".to_owned())
        || first_row[4] != Value::Text("1".to_owned())
    {
        bail!("SQL-LOG-027 first-per-partition row changed: {first_row:?}");
    }
    let Value::Text(first_metadata) = &first_row[3] else {
        bail!("SQL-LOG-027 metadata is not retained JSON text: {first_row:?}");
    };
    let first_metadata: serde_json::Value = serde_json::from_str(first_metadata)?;
    if first_metadata.pointer("/duration_ms") != Some(&serde_json::json!(4))
        || first_metadata.pointer("/service") != Some(&serde_json::json!("api"))
        || first_metadata.pointer("/nested/empty") != Some(&serde_json::Value::Null)
    {
        bail!("SQL-LOG-027 retained metadata changed: {first_metadata}");
    }
    let [last_row] = last_rows.as_slice() else {
        bail!("SQL-LOG-028 last-per-partition result changed: {last_rows:?}");
    };
    if last_row.len() != 5
        || last_row[0] != Value::Integer(1000)
        || last_row[1] != Value::Text("error".to_owned())
        || last_row[2] != Value::Text("request timeout".to_owned())
        || last_row[4] != Value::Text("1".to_owned())
    {
        bail!("SQL-LOG-028 last-per-partition row changed: {last_row:?}");
    }
    let Value::Text(last_metadata) = &last_row[3] else {
        bail!("SQL-LOG-028 metadata is not retained JSON text: {last_row:?}");
    };
    let last_metadata: serde_json::Value = serde_json::from_str(last_metadata)?;
    if last_metadata.pointer("/duration_ms") != Some(&serde_json::json!(12))
        || last_metadata.pointer("/service") != Some(&serde_json::json!("api"))
        || last_metadata.pointer("/nested/empty") != Some(&serde_json::json!(""))
    {
        bail!("SQL-LOG-028 retained metadata changed: {last_metadata}");
    }
    if top_rows
        != [
            vec![
                Value::Text("web-1".into()),
                Value::Text("1".into()),
                Value::Text("1".into()),
            ],
            vec![
                Value::Text("web-2".into()),
                Value::Text("1".into()),
                Value::Text("2".into()),
            ],
        ]
    {
        bail!("SQL-LOG-029 top frequency result changed: {top_rows:?}");
    }
    if uniq_rows
        != [
            vec![Value::Text("web-1".into()), Value::Text("1".into())],
            vec![Value::Text("web-2".into()), Value::Text("1".into())],
        ]
    {
        bail!("SQL-LOG-030 unique result changed: {uniq_rows:?}");
    }
    if facet_rows.len() != 16
        || !facet_rows.contains(&vec![
            Value::Text("host".into()),
            Value::Text("web-1".into()),
            Value::Text("1".into()),
        ])
        || facet_rows
            .iter()
            .any(|row| matches!(row.first(), Some(Value::Text(name)) if name == "service" || name == "nested.ok" || name == "nested.count"))
    {
        bail!("SQL-LOG-031 facets result changed: {facet_rows:?}");
    }
    if coalesce_rows.len() != 2
        || coalesce_rows
            .iter()
            .map(|row| row.get(4))
            .collect::<Vec<_>>()
            != [
                Some(&Value::Text("web-1".into())),
                Some(&Value::Text("web-2".into())),
            ]
    {
        bail!("SQL-LOG-032 coalesce result changed: {coalesce_rows:?}");
    }
    if copy_rows.len() != 2 {
        bail!("SQL-LOG-033 copy result changed: {copy_rows:?}");
    }
    for row in &copy_rows {
        let Value::Text(metadata) = &row[3] else {
            bail!("SQL-LOG-033 copied metadata is not JSON text: {row:?}");
        };
        let metadata: serde_json::Value = serde_json::from_str(metadata)?;
        if metadata.pointer("/copied") != metadata.pointer("/duration_ms")
            || metadata.pointer("/host").is_none()
        {
            bail!("SQL-LOG-033 exact typed copy or source retention changed: {metadata}");
        }
    }
    if rename_rows.len() != 2 {
        bail!("SQL-LOG-034 rename result changed: {rename_rows:?}");
    }
    for row in &rename_rows {
        let Value::Text(metadata) = &row[3] else {
            bail!("SQL-LOG-034 renamed metadata is not JSON text: {row:?}");
        };
        let metadata: serde_json::Value = serde_json::from_str(metadata)?;
        if metadata.pointer("/moved").is_none()
            || metadata.pointer("/duration_ms").is_some()
            || metadata.pointer("/host").is_none()
        {
            bail!("SQL-LOG-034 exact typed move or source removal changed: {metadata}");
        }
    }
    if format_rows
        != [
            vec![
                Value::Integer(1000),
                Value::Text("error".into()),
                Value::Text("request timeout".into()),
                Value::Text("host=web-1 duration_ms=12".into()),
            ],
            vec![
                Value::Integer(2000),
                Value::Text("info".into()),
                Value::Text("request ok".into()),
                Value::Text("host=web-2 duration_ms=4".into()),
            ],
        ]
    {
        bail!("SQL-LOG-035 format result changed: {format_rows:?}");
    }
    if math_rows
        != [
            vec![
                Value::Integer(1000),
                Value::Text("error".into()),
                Value::Text("request timeout".into()),
                Value::Real(26.0),
            ],
            vec![
                Value::Integer(2000),
                Value::Text("info".into()),
                Value::Text("request ok".into()),
                Value::Null,
            ],
        ]
    {
        bail!("SQL-LOG-036 arithmetic result changed: {math_rows:?}");
    }
    if len_rows
        != [
            vec![
                Value::Integer(1000),
                Value::Text("error".into()),
                Value::Text("request timeout".into()),
                Value::Integer(5),
            ],
            vec![
                Value::Integer(2000),
                Value::Text("info".into()),
                Value::Text("request ok".into()),
                Value::Integer(5),
            ],
        ]
    {
        bail!("SQL-LOG-037 byte-length result changed: {len_rows:?}");
    }
    if drop_empty_rows.len() != 2 {
        bail!("SQL-LOG-038 result cardinality changed: {drop_empty_rows:?}");
    }
    for row in &drop_empty_rows {
        let metadata = match row.get(3) {
            Some(Value::Text(metadata)) => serde_json::from_str::<serde_json::Value>(metadata)?,
            _ => bail!("SQL-LOG-038 metadata result changed: {row:?}"),
        };
        if metadata.pointer("/nested/empty").is_some()
            || metadata.pointer("/nested/ok").is_none()
            || metadata.pointer("/tags").is_none()
        {
            bail!("SQL-LOG-038 typed empty-field removal changed: {metadata}");
        }
    }
    let source_empty_states: i64 = connection.query_row(
        "SELECT count(*) FROM logs
         WHERE json_type(metadata, '$.nested.empty') IN ('null', 'text')",
        [],
        |row| row.get(0),
    )?;
    if source_empty_states != 2 {
        bail!("SQL-LOG-038 mutated its public source: {source_empty_states}");
    }
    if replace_rows
        != [
            vec![
                Value::Integer(1000),
                Value::Text("error".into()),
                Value::Text("request timeout".into()),
                Value::Text("node-1".into()),
            ],
            vec![
                Value::Integer(2000),
                Value::Text("info".into()),
                Value::Text("request ok".into()),
                Value::Text("node-2".into()),
            ],
        ]
    {
        bail!("SQL-LOG-039 literal replacement changed: {replace_rows:?}");
    }
    let source_hosts = connection
        .prepare("SELECT json_extract(metadata, '$.host') FROM logs ORDER BY ts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_hosts != ["web-1", "web-2"] {
        bail!("SQL-LOG-039 mutated its public source: {source_hosts:?}");
    }
    if extract_rows
        != [
            vec![
                Value::Integer(1000),
                Value::Text("error".into()),
                Value::Text("request timeout".into()),
                Value::Text("010".into()),
                Value::Text("000".into()),
            ],
            vec![
                Value::Integer(2000),
                Value::Text("info".into()),
                Value::Text("request ok".into()),
                Value::Text("10".into()),
                Value::Text("0".into()),
            ],
        ]
    {
        bail!("SQL-LOG-040 literal extraction changed: {extract_rows:?}");
    }
    let source_ips = connection
        .prepare("SELECT json_extract(metadata, '$.client_ip') FROM logs ORDER BY ts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_ips != ["010.000.000.001", "10.0.1.1"] {
        bail!("SQL-LOG-040 mutated its public source: {source_ips:?}");
    }
    if pack_json_rows.len() != 2 {
        bail!("SQL-LOG-041 result cardinality changed: {pack_json_rows:?}");
    }
    let expected_packed = [
        serde_json::json!({
            "host":"web-1",
            "duration_ms":12,
            "nested":{"ok":true, "none":null, "empty":""},
            "tags":["prod", "", 123, true, false, null, {"nested":"ignored"}, ["ignored"], "ab", "*"]
        }),
        serde_json::json!({
            "host":"web-2",
            "duration_ms":4,
            "nested":{"ok":"true", "empty":null},
            "tags":["dev", 1.5, -2, "123", "a\"b", "a\nb", "a/b"]
        }),
    ];
    for (row, expected) in pack_json_rows.iter().zip(expected_packed) {
        let Some(Value::Text(packed)) = row.get(3) else {
            bail!("SQL-LOG-041 packed result is not JSON TEXT: {row:?}");
        };
        let packed = serde_json::from_str::<serde_json::Value>(packed)?;
        if packed != expected {
            bail!("SQL-LOG-041 typed packed result changed: {packed}");
        }
    }
    let source_packed_fields = connection
        .prepare("SELECT metadata FROM logs ORDER BY ts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_packed_fields.len() != 2
        || source_packed_fields
            .iter()
            .any(|metadata| serde_json::from_str::<serde_json::Value>(metadata).is_err())
    {
        bail!("SQL-LOG-041 mutated its public source: {source_packed_fields:?}");
    }
    if unpack_json_rows.len() != 2 {
        bail!("SQL-LOG-042 result cardinality changed: {unpack_json_rows:?}");
    }
    let expected_unpacked = [
        serde_json::json!({
            "ok":true,
            "count":2,
            "none":null,
            "empty":"",
            "missing":""
        }),
        serde_json::json!({
            "ok":"true",
            "count":"2",
            "none":"",
            "empty":null,
            "missing":""
        }),
    ];
    for (row, expected) in unpack_json_rows.iter().zip(expected_unpacked) {
        let Some(Value::Text(unpacked)) = row.get(3) else {
            bail!("SQL-LOG-042 unpacked result is not JSON TEXT: {row:?}");
        };
        let unpacked = serde_json::from_str::<serde_json::Value>(unpacked)?;
        if unpacked != expected {
            bail!("SQL-LOG-042 typed unpacked result changed: {unpacked}");
        }
    }
    let source_unpacked_fields = connection
        .prepare("SELECT metadata FROM logs ORDER BY ts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_unpacked_fields != source_packed_fields {
        bail!("SQL-LOG-042 mutated its public source: {source_unpacked_fields:?}");
    }
    if json_array_len_rows
        != [
            vec![
                Value::Integer(1000),
                Value::Text("error".into()),
                Value::Text("request timeout".into()),
                Value::Text("10".into()),
            ],
            vec![
                Value::Integer(2000),
                Value::Text("info".into()),
                Value::Text("request ok".into()),
                Value::Text("7".into()),
            ],
        ]
    {
        bail!("SQL-LOG-043 native array lengths changed: {json_array_len_rows:?}");
    }
    let json_array_len_sql = recipe_sql("SQL-LOG-043", 0)?;
    let measured_json_array_lengths = |source_path: &str| -> Result<Vec<String>> {
        let mut statement = connection.prepare(&json_array_len_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-043 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "json_array_source_path" => Value::Text(source_path.to_owned()),
                _ => parameter("SQL-LOG-043", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        statement
            .raw_query()
            .mapped(|row| row.get::<_, String>(3))
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    };
    for (path, expected) in [
        ("$.nested.array_text", vec!["4", "4"]),
        ("$.tags[7]", vec!["1", "0"]),
        ("$.nested.count", vec!["0", "0"]),
        ("$.nested", vec!["0", "0"]),
        ("$.client_ip", vec!["0", "0"]),
        ("$.nested.none", vec!["0", "0"]),
        ("$.missing", vec!["0", "0"]),
    ] {
        let actual = measured_json_array_lengths(path)?;
        if actual != expected {
            bail!("SQL-LOG-043 path {path:?} changed: {actual:?}");
        }
    }
    let source_after_array_lengths = connection
        .prepare("SELECT metadata FROM logs ORDER BY ts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_after_array_lengths != source_packed_fields {
        bail!("SQL-LOG-043 mutated its public source: {source_after_array_lengths:?}");
    }
    if quantile_stddev_rows != [vec![Value::Integer(2), Value::Real(12.0), Value::Real(4.0)]] {
        bail!("SQL-LOG-044 numeric statistics changed: {quantile_stddev_rows:?}");
    }
    let quantile_stddev_sql = recipe_sql("SQL-LOG-044", 0)?;
    let measured_statistics = |source_path: &str, quantile: f64| -> Result<Vec<Vec<Value>>> {
        let mut statement = connection.prepare(&quantile_stddev_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-044 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "stats_source_path" => Value::Text(source_path.to_owned()),
                "quantile" => Value::Real(quantile),
                _ => parameter("SQL-LOG-044", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        statement
            .raw_query()
            .mapped(|row| {
                (0..row.as_ref().column_count())
                    .map(|column| row.get(column))
                    .collect::<rusqlite::Result<Vec<Value>>>()
            })
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    };
    for (quantile, expected) in [(0.0, 4.0), (0.5, 12.0), (1.0, 12.0)] {
        let actual = measured_statistics("$.duration_ms", quantile)?;
        if actual.first().and_then(|row| row.get(1)) != Some(&Value::Real(expected)) {
            bail!("SQL-LOG-044 quantile {quantile} changed: {actual:?}");
        }
    }
    if measured_statistics("$.nested.count", 0.5)?
        != [vec![Value::Integer(1), Value::Real(2.0), Value::Real(0.0)]]
        || measured_statistics("$.missing", 0.5)?
            != [vec![Value::Integer(0), Value::Null, Value::Null]]
    {
        bail!("SQL-LOG-044 typed/missing behavior changed");
    }
    let source_after_statistics = connection
        .prepare("SELECT metadata FROM logs ORDER BY ts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_after_statistics != source_packed_fields {
        bail!("SQL-LOG-044 mutated its public source: {source_after_statistics:?}");
    }
    if sum_len_rows != [vec![Value::Integer(3)]] {
        bail!("SQL-LOG-045 aggregate byte length changed: {sum_len_rows:?}");
    }
    let sum_len_sql = recipe_sql("SQL-LOG-045", 0)?;
    let measured_sum_len = |source_path: &str| -> Result<i64> {
        let mut statement = connection.prepare(&sum_len_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-045 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "sum_len_source_path" => Value::Text(source_path.to_owned()),
                _ => parameter("SQL-LOG-045", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        let mut rows = statement.raw_query();
        let value = rows
            .next()?
            .context("SQL-LOG-045 returned no aggregate row")?
            .get(0)?;
        Ok(value)
    };
    for (path, expected) in [
        ("$.host", 10),
        ("$.nested.ok", 8),
        ("$.nested.none", 0),
        ("$.missing", 0),
    ] {
        let actual = measured_sum_len(path)?;
        if actual != expected {
            bail!("SQL-LOG-045 path {path:?} changed: {actual}");
        }
    }
    let source_after_sum_len = connection
        .prepare("SELECT metadata FROM logs ORDER BY ts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_after_sum_len != source_packed_fields {
        bail!("SQL-LOG-045 mutated its public source: {source_after_sum_len:?}");
    }
    if any_rows
        != [vec![
            Value::Text("text".into()),
            Value::Text(r#""web-1""#.into()),
        ]]
    {
        bail!("SQL-LOG-046 deterministic any changed: {any_rows:?}");
    }
    if field_extrema_rows
        != [vec![
            Value::Text("text".into()),
            Value::Text(r#""web-2""#.into()),
            Value::Text("text".into()),
            Value::Text(r#""web-1""#.into()),
        ]]
    {
        bail!("SQL-LOG-046 numeric companion extrema changed: {field_extrema_rows:?}");
    }
    let any_sql = recipe_sql("SQL-LOG-046", 0)?;
    let measured_any = |source_path: &str| -> Result<Vec<Vec<Value>>> {
        let mut statement = connection.prepare(&any_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-046 any parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "any_source_path" => Value::Text(source_path.to_owned()),
                _ => parameter("SQL-LOG-046", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        let columns = statement.column_count();
        statement
            .raw_query()
            .mapped(|row| {
                (0..columns)
                    .map(|column| row.get(column))
                    .collect::<rusqlite::Result<Vec<Value>>>()
            })
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    };
    if measured_any("$.nested.ok")?
        != [vec![Value::Text("true".into()), Value::Text("true".into())]]
        || !measured_any("$.missing")?.is_empty()
    {
        bail!("SQL-LOG-046 typed or missing any behavior changed");
    }
    let extrema_sql = recipe_sql("SQL-LOG-046", 1)?;
    let measured_extrema = |source_path: &str, result_path: &str| -> Result<Vec<Vec<Value>>> {
        let mut statement = connection.prepare(&extrema_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-046 extrema parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "extreme_source_path" => Value::Text(source_path.to_owned()),
                "extreme_result_path" => Value::Text(result_path.to_owned()),
                _ => parameter("SQL-LOG-046", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        let columns = statement.column_count();
        statement
            .raw_query()
            .mapped(|row| {
                (0..columns)
                    .map(|column| row.get(column))
                    .collect::<rusqlite::Result<Vec<Value>>>()
            })
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    };
    let rich_extrema = measured_extrema("$.duration_ms", "$.nested")?;
    let [rich_row] = rich_extrema.as_slice() else {
        bail!("SQL-LOG-046 rich companion extrema cardinality changed: {rich_extrema:?}");
    };
    if rich_row.first() != Some(&Value::Text("object".into()))
        || rich_row.get(2) != Some(&Value::Text("object".into()))
        || !matches!(rich_row.get(1), Some(Value::Text(value)) if serde_json::from_str::<serde_json::Value>(value).is_ok())
        || !matches!(rich_row.get(3), Some(Value::Text(value)) if serde_json::from_str::<serde_json::Value>(value).is_ok())
        || measured_extrema("$.missing", "$.host")?
            != [vec![Value::Null, Value::Null, Value::Null, Value::Null]]
    {
        bail!("SQL-LOG-046 rich or missing companion behavior changed: {rich_extrema:?}");
    }
    let source_after_any_extrema = connection
        .prepare("SELECT metadata FROM logs ORDER BY ts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_after_any_extrema != source_packed_fields {
        bail!("SQL-LOG-046 mutated its public source: {source_after_any_extrema:?}");
    }
    let [row_any_result] = row_any_rows.as_slice() else {
        bail!("SQL-LOG-047 row_any cardinality changed: {row_any_rows:?}");
    };
    let Some(Value::Text(row_any_json)) = row_any_result.first() else {
        bail!("SQL-LOG-047 row_any type changed: {row_any_rows:?}");
    };
    if serde_json::from_str::<serde_json::Value>(row_any_json)?
        != serde_json::json!({"nested": {"ok": true, "none": null}})
    {
        bail!("SQL-LOG-047 row_any rich result changed: {row_any_rows:?}");
    }
    let [row_extrema_result] = row_extrema_rows.as_slice() else {
        bail!("SQL-LOG-047 row extrema cardinality changed: {row_extrema_rows:?}");
    };
    let [Value::Text(minimum_json), Value::Text(maximum_json)] = row_extrema_result.as_slice()
    else {
        bail!("SQL-LOG-047 row extrema types changed: {row_extrema_rows:?}");
    };
    let minimum = serde_json::from_str::<serde_json::Value>(minimum_json)?;
    let maximum = serde_json::from_str::<serde_json::Value>(maximum_json)?;
    if minimum.get("host") != Some(&serde_json::json!("web-2"))
        || minimum.pointer("/nested/ok") != Some(&serde_json::json!("true"))
        || maximum.get("host") != Some(&serde_json::json!("web-1"))
        || maximum.pointer("/nested/ok") != Some(&serde_json::json!(true))
        || maximum.pointer("/nested/none") != Some(&serde_json::Value::Null)
    {
        bail!("SQL-LOG-047 row extrema rich results changed: {row_extrema_rows:?}");
    }
    let row_any_sql = recipe_sql("SQL-LOG-047", 0)?;
    let measured_row_any = |path_1: &str, path_2: &str| -> Result<Vec<Vec<Value>>> {
        let mut statement = connection.prepare(&row_any_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-047 row_any parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "row_any_path_1" => Value::Text(path_1.to_owned()),
                "row_any_path_2" => Value::Text(path_2.to_owned()),
                _ => parameter("SQL-LOG-047", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        let columns = statement.column_count();
        statement
            .raw_query()
            .mapped(|row| {
                (0..columns)
                    .map(|column| row.get(column))
                    .collect::<rusqlite::Result<Vec<Value>>>()
            })
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    };
    let typed_row_any = measured_row_any("$.nested.ok", "$.tags")?;
    let [typed_row] = typed_row_any.as_slice() else {
        bail!("SQL-LOG-047 typed row_any cardinality changed: {typed_row_any:?}");
    };
    let Some(Value::Text(typed_json)) = typed_row.first() else {
        bail!("SQL-LOG-047 typed row_any type changed: {typed_row_any:?}");
    };
    let typed = serde_json::from_str::<serde_json::Value>(typed_json)?;
    if typed.pointer("/nested/ok") != Some(&serde_json::json!(true))
        || !typed.get("tags").is_some_and(serde_json::Value::is_array)
        || !measured_row_any("$.missing", "$.also_missing")?.is_empty()
    {
        bail!("SQL-LOG-047 typed or missing row_any behavior changed: {typed_row_any:?}");
    }
    let row_extrema_sql = recipe_sql("SQL-LOG-047", 1)?;
    let measured_row_extrema =
        |source_path: &str, result_path_1: &str, result_path_2: &str| -> Result<Vec<Vec<Value>>> {
            let mut statement = connection.prepare(&row_extrema_sql)?;
            for index in 1..=statement.parameter_count() {
                let name = statement
                    .parameter_name(index)
                    .context("SQL-LOG-047 row extrema parameter must be named")?
                    .trim_start_matches(':');
                let value = match name {
                    "row_extreme_source_path" => Value::Text(source_path.to_owned()),
                    "row_result_path_1" => Value::Text(result_path_1.to_owned()),
                    "row_result_path_2" => Value::Text(result_path_2.to_owned()),
                    _ => parameter("SQL-LOG-047", name),
                };
                statement.raw_bind_parameter(index, value)?;
            }
            let columns = statement.column_count();
            statement
                .raw_query()
                .mapped(|row| {
                    (0..columns)
                        .map(|column| row.get(column))
                        .collect::<rusqlite::Result<Vec<Value>>>()
                })
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        };
    let empty_extrema = measured_row_extrema("$.missing", "$.host", "$.tags")?;
    if empty_extrema
        != [vec![
            Value::Text("{}".to_owned()),
            Value::Text("{}".to_owned()),
        ]]
    {
        bail!("SQL-LOG-047 empty row extrema changed: {empty_extrema:?}");
    }
    let array_extrema = measured_row_extrema("$.duration_ms", "$.missing", "$.tags")?;
    let [array_row] = array_extrema.as_slice() else {
        bail!("SQL-LOG-047 array row extrema cardinality changed: {array_extrema:?}");
    };
    if array_row.iter().any(|value| {
        !matches!(value, Value::Text(json) if serde_json::from_str::<serde_json::Value>(json)
            .is_ok_and(|value| value.get("tags").is_some_and(serde_json::Value::is_array)))
    }) {
        bail!("SQL-LOG-047 array or missing result behavior changed: {array_extrema:?}");
    }
    let source_after_row_selection = connection
        .prepare("SELECT metadata FROM logs ORDER BY ts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_after_row_selection != source_packed_fields {
        bail!("SQL-LOG-047 mutated its public source: {source_after_row_selection:?}");
    }
    let len_sql = recipe_sql("SQL-LOG-037", 0)?;
    let measured_lengths = |source_path: &str| -> Result<Vec<i64>> {
        let mut statement = connection.prepare(&len_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-037 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "len_source_path" => Value::Text(source_path.to_owned()),
                _ => parameter("SQL-LOG-037", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        statement
            .raw_query()
            .mapped(|row| row.get::<_, i64>(3))
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    };
    let sqlite_unicode_lengths = connection.query_row(
        "SELECT length('ßİ'), length(CAST('ßİ' AS BLOB))",
        [],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
    )?;
    if sqlite_unicode_lengths != (2, 4)
        || measured_lengths("$.nested.none")? != [0, 0]
        || measured_lengths("$.deployment")? != [0, 0]
        || measured_lengths("$.nested.ok")? != [4, 4]
        || measured_lengths("$.duration_ms")? != [2, 1]
        || measured_lengths("$.missing")? != [0, 0]
    {
        bail!(
            "SQL-LOG-037 UTF-8/rich-value projection changed: sqlite={sqlite_unicode_lengths:?} null={:?} object={:?} boolean={:?} number={:?} missing={:?}",
            measured_lengths("$.nested.none")?,
            measured_lengths("$.deployment")?,
            measured_lengths("$.nested.ok")?,
            measured_lengths("$.duration_ms")?,
            measured_lengths("$.missing")?,
        );
    }
    let coalesce_sql = recipe_sql("SQL-LOG-032", 0)?;
    let coalesced = |source_path_1: &str,
                     source_path_2: &str,
                     source_path_3: &str,
                     default_value: &str|
     -> Result<Vec<String>> {
        let mut statement = connection.prepare(&coalesce_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-032 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "source_path_1" => Value::Text(source_path_1.to_owned()),
                "source_path_2" => Value::Text(source_path_2.to_owned()),
                "source_path_3" => Value::Text(source_path_3.to_owned()),
                "default_value" => Value::Text(default_value.to_owned()),
                _ => parameter("SQL-LOG-032", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        statement
            .raw_query()
            .mapped(|row| row.get::<_, String>(4))
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    };
    if coalesced("$.nested.ok", "$.host", "$.service", "fallback")? != ["true", "true"] {
        bail!("SQL-LOG-032 boolean textual projection changed");
    }
    if coalesced("$.duration_ms", "$.host", "$.service", "fallback")? != ["12", "4"] {
        bail!("SQL-LOG-032 numeric textual projection changed");
    }
    if coalesced("$.absent", "$.missing", "$.never", "fallback")? != ["fallback", "fallback"] {
        bail!("SQL-LOG-032 default projection changed");
    }
    let copy_sql = recipe_sql("SQL-LOG-033", 0)?;
    let copied = |source_path: &str| -> Result<Vec<serde_json::Value>> {
        let mut statement = connection.prepare(&copy_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-033 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "copy_source_path" => Value::Text(source_path.to_owned()),
                "copy_destination_path" => Value::Text("$.copied".to_owned()),
                _ => parameter("SQL-LOG-033", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        statement
            .raw_query()
            .mapped(|row| {
                let metadata = row.get::<_, String>(3)?;
                serde_json::from_str(&metadata).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })
            })
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    };
    let boolean_or_string = copied("$.nested.ok")?;
    if boolean_or_string[0].pointer("/copied") != Some(&serde_json::json!(true))
        || boolean_or_string[1].pointer("/copied") != Some(&serde_json::json!("true"))
    {
        bail!("SQL-LOG-033 boolean/string type preservation changed: {boolean_or_string:?}");
    }
    let arrays = copied("$.tags")?;
    if !arrays.iter().all(|metadata| {
        metadata
            .pointer("/copied")
            .is_some_and(serde_json::Value::is_array)
    }) {
        bail!("SQL-LOG-033 array preservation changed: {arrays:?}");
    }
    let object_parents = copied("$.nested")?;
    if !object_parents
        .iter()
        .all(|metadata| metadata.pointer("/copied") == Some(&serde_json::json!("")))
    {
        bail!("SQL-LOG-033 flattened object-parent behavior changed: {object_parents:?}");
    }
    let missing = copied("$.absent")?;
    if !missing
        .iter()
        .all(|metadata| metadata.pointer("/copied") == Some(&serde_json::json!("")))
    {
        bail!("SQL-LOG-033 missing-source behavior changed: {missing:?}");
    }
    let null_or_missing = copied("$.nested.none")?;
    if null_or_missing[0].pointer("/copied") != Some(&serde_json::Value::Null)
        || null_or_missing[1].pointer("/copied") != Some(&serde_json::json!(""))
    {
        bail!("SQL-LOG-033 null/missing fidelity changed: {null_or_missing:?}");
    }
    let rename_sql = recipe_sql("SQL-LOG-034", 0)?;
    let renamed = |source_path: &str, destination_path: &str| -> Result<Vec<serde_json::Value>> {
        let mut statement = connection.prepare(&rename_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-034 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "rename_source_path" => Value::Text(source_path.to_owned()),
                "rename_destination_path" => Value::Text(destination_path.to_owned()),
                _ => parameter("SQL-LOG-034", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        statement
            .raw_query()
            .mapped(|row| {
                let metadata = row.get::<_, String>(3)?;
                serde_json::from_str(&metadata).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })
            })
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    };
    let boolean_or_string = renamed("$.nested.ok", "$.moved")?;
    if boolean_or_string[0].pointer("/moved") != Some(&serde_json::json!(true))
        || boolean_or_string[1].pointer("/moved") != Some(&serde_json::json!("true"))
        || boolean_or_string
            .iter()
            .any(|metadata| metadata.pointer("/nested/ok").is_some())
    {
        bail!("SQL-LOG-034 boolean/string move changed: {boolean_or_string:?}");
    }
    let arrays = renamed("$.tags", "$.moved")?;
    if !arrays.iter().all(|metadata| {
        metadata
            .pointer("/moved")
            .is_some_and(serde_json::Value::is_array)
            && metadata.pointer("/tags").is_none()
    }) {
        bail!("SQL-LOG-034 array move changed: {arrays:?}");
    }
    let object_parents = renamed("$.nested", "$.moved")?;
    if !object_parents.iter().all(|metadata| {
        metadata.pointer("/moved") == Some(&serde_json::json!(""))
            && metadata.pointer("/nested").is_some()
    }) {
        bail!("SQL-LOG-034 flattened object-parent behavior changed: {object_parents:?}");
    }
    let missing = renamed("$.absent", "$.moved")?;
    if !missing
        .iter()
        .all(|metadata| metadata.pointer("/moved") == Some(&serde_json::json!("")))
    {
        bail!("SQL-LOG-034 missing-source behavior changed: {missing:?}");
    }
    let null_or_missing = renamed("$.nested.none", "$.moved")?;
    if null_or_missing[0].pointer("/moved") != Some(&serde_json::Value::Null)
        || null_or_missing[0].pointer("/nested/none").is_some()
        || null_or_missing[1].pointer("/moved") != Some(&serde_json::json!(""))
    {
        bail!("SQL-LOG-034 null/missing fidelity changed: {null_or_missing:?}");
    }
    let identity = renamed("$.host", "$.host")?;
    if identity
        .iter()
        .any(|metadata| metadata.pointer("/host").is_none())
    {
        bail!("SQL-LOG-034 exact identity changed: {identity:?}");
    }
    let overwritten = renamed("$.duration_ms", "$.host")?;
    if overwritten.iter().any(|metadata| {
        metadata.pointer("/duration_ms").is_some()
            || !metadata
                .pointer("/host")
                .is_some_and(serde_json::Value::is_number)
    }) {
        bail!("SQL-LOG-034 destination overwrite changed: {overwritten:?}");
    }
    let format_sql = recipe_sql("SQL-LOG-035", 0)?;
    let formatted =
        |source_path_1: &str, source_path_2: &str, pattern: &str| -> Result<Vec<String>> {
            let mut statement = connection.prepare(&format_sql)?;
            for index in 1..=statement.parameter_count() {
                let name = statement
                    .parameter_name(index)
                    .context("SQL-LOG-035 parameter must be named")?
                    .trim_start_matches(':');
                let value = match name {
                    "format_source_path_1" => Value::Text(source_path_1.to_owned()),
                    "format_source_path_2" => Value::Text(source_path_2.to_owned()),
                    "format_pattern" => Value::Text(pattern.to_owned()),
                    _ => parameter("SQL-LOG-035", name),
                };
                statement.raw_bind_parameter(index, value)?;
            }
            statement
                .raw_query()
                .mapped(|row| row.get::<_, String>(3))
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        };
    if formatted("$.nested.ok", "$.nested.count", "%s|%s")? != ["true|2", "true|2"] {
        bail!("SQL-LOG-035 boolean/numeric textual projection changed");
    }
    if formatted("$.absent", "$.nested.empty", "<%s>|<%s>")? != ["<>|<>", "<>|<>"] {
        bail!("SQL-LOG-035 missing/null/empty projection changed");
    }
    let arrays = formatted("$.tags", "$.host", "%s|%s")?;
    if arrays.len() != 2
        || !arrays[0].starts_with(r#"["prod","",123,true,false,null,"#)
        || !arrays[0].ends_with("|web-1")
        || !arrays[1].starts_with(r#"["dev",1.5,-2,"#)
        || !arrays[1].ends_with("|web-2")
    {
        bail!("SQL-LOG-035 array/string textual projection changed: {arrays:?}");
    }
    let facets_sql = recipe_sql("SQL-LOG-031", 0)?;
    let facet_values = |max_values_per_field: i64,
                        max_value_len: i64,
                        keep_const_fields: i64,
                        max_result_rows: i64|
     -> Result<Vec<Vec<Value>>> {
        let mut statement = connection.prepare(&facets_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-031 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "max_values_per_field" => Value::Integer(max_values_per_field),
                "max_value_len" => Value::Integer(max_value_len),
                "keep_const_fields" => Value::Integer(keep_const_fields),
                "max_result_rows" => Value::Integer(max_result_rows),
                _ => parameter("SQL-LOG-031", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        let columns = statement.column_count();
        let mut query = statement.raw_query();
        let mut output = Vec::new();
        while let Some(row) = query.next()? {
            output.push(
                (0..columns)
                    .map(|column| row.get(column))
                    .collect::<rusqlite::Result<Vec<Value>>>()?,
            );
        }
        Ok(output)
    };
    let constants = facet_values(1_000, 128, 1, 100)?;
    for expected in [
        vec![
            Value::Text("service".into()),
            Value::Text("api".into()),
            Value::Text("2".into()),
        ],
        vec![
            Value::Text("nested.ok".into()),
            Value::Text("true".into()),
            Value::Text("2".into()),
        ],
        vec![
            Value::Text("nested.count".into()),
            Value::Text("2".into()),
            Value::Text("2".into()),
        ],
    ] {
        if !constants.contains(&expected) {
            bail!("SQL-LOG-031 keep-constant result changed: {constants:?}");
        }
    }
    if !facet_values(1, 128, 0, 100)?.is_empty() {
        bail!("SQL-LOG-031 must drop high-cardinality and constant fields");
    }
    let short = facet_values(1_000, 5, 0, 100)?;
    if short.len() != 6
        || short
            .iter()
            .any(|row| !matches!(row.first(), Some(Value::Text(name)) if name == "duration_ms" || name == "host" || name == "level"))
    {
        bail!("SQL-LOG-031 byte-length filtering changed: {short:?}");
    }
    if !facet_values(1_000, 128, 0, 2)?.is_empty() {
        bail!("SQL-LOG-031 must fail closed when the result limit is exceeded");
    }
    let uniq_sql = recipe_sql("SQL-LOG-030", 0)?;
    let uniq_values =
        |path: &str, filter: &str, limit: i64, with_hits: i64| -> Result<Vec<Vec<Value>>> {
            let mut statement = connection.prepare(&uniq_sql)?;
            for index in 1..=statement.parameter_count() {
                let name = statement
                    .parameter_name(index)
                    .context("SQL-LOG-030 parameter must be named")?
                    .trim_start_matches(':');
                let value = match name {
                    "group_path" => Value::Text(path.to_owned()),
                    "filter_text" => Value::Text(filter.to_owned()),
                    "uniq_limit" => Value::Integer(limit),
                    "with_hits" => Value::Integer(with_hits),
                    _ => parameter("SQL-LOG-030", name),
                };
                statement.raw_bind_parameter(index, value)?;
            }
            let columns = statement.column_count();
            let mut query = statement.raw_query();
            let mut output = Vec::new();
            while let Some(row) = query.next()? {
                output.push(
                    (0..columns)
                        .map(|column| row.get(column))
                        .collect::<rusqlite::Result<Vec<Value>>>()?,
                );
            }
            Ok(output)
        };
    if uniq_values("$.host", "web-2", 0, 0)? != [vec![Value::Text("web-2".into()), Value::Null]] {
        bail!("SQL-LOG-030 filter/without-hits behavior changed");
    }
    if uniq_values("$.host", "", 1, 1)?
        != [vec![Value::Text("web-1".into()), Value::Text("0".into())]]
    {
        bail!("SQL-LOG-030 overflow-hit behavior changed");
    }
    for path in ["$.nested.none", "$.nested.empty"] {
        if uniq_values(path, "", 0, 1)?
            != [vec![Value::Text(String::new()), Value::Text("2".into())]]
        {
            bail!("SQL-LOG-030 empty/null/missing behavior changed at {path}");
        }
    }
    let json_array_sql = recipe_sql("SQL-LOG-017", 0)?;
    let json_array_timestamps = |first: &str, second: &str, path: &str| -> Result<Vec<i64>> {
        let mut statement = connection.prepare(&json_array_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-017 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "array_value_1" => Value::Text(first.to_owned()),
                "array_value_2" => Value::Text(second.to_owned()),
                "field_path" => Value::Text(path.to_owned()),
                _ => parameter("SQL-LOG-017", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        Ok(statement
            .raw_query()
            .mapped(|row| row.get::<_, i64>(0))
            .collect::<rusqlite::Result<Vec<_>>>()?)
    };
    for (first, second, path, expected) in [
        ("123", "missing", "$.tags", vec![2000, 1000]),
        ("true", "null", "$.tags", vec![1000]),
        ("false", "missing", "$.tags", vec![1000]),
        ("1.5", "-2", "$.tags", vec![2000]),
        ("", "missing", "$.tags", vec![1000]),
        ("a/b", "ab", "$.tags", vec![2000, 1000]),
        ("a\"b", "a\nb", "$.tags", vec![2000]),
        ("*", "missing", "$.tags", vec![1000]),
        (
            r#"{"nested":"ignored"}"#,
            r#"["ignored"]"#,
            "$.tags",
            vec![],
        ),
        ("us-east", "missing", "$.deployment.region", vec![]),
        ("prod", "missing", "$.absent", vec![]),
    ] {
        let actual = json_array_timestamps(first, second, path)?;
        if actual != expected {
            bail!("SQL-LOG-017 values {first:?}, {second:?} at {path:?} changed: {actual:?}");
        }
    }
    let ipv4_sql = recipe_sql("SQL-LOG-018", 0)?;
    let ipv4_timestamps = |minimum: i64, maximum: i64, path: &str| -> Result<Vec<i64>> {
        let mut statement = connection.prepare(&ipv4_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-018 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "ipv4_min" => Value::Integer(minimum),
                "ipv4_max" => Value::Integer(maximum),
                "field_path" => Value::Text(path.to_owned()),
                _ => parameter("SQL-LOG-018", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        Ok(statement
            .raw_query()
            .mapped(|row| row.get::<_, i64>(0))
            .collect::<rusqlite::Result<Vec<_>>>()?)
    };
    for (minimum, maximum, path, expected) in [
        (0x0a00_0001, 0x0a00_0001, "$.client_ip", vec![1000]),
        (0x0a00_0101, 0x0a00_0101, "$.client_ip", vec![2000]),
        (0, 0xffff_ffff, "$.client_ip", vec![2000, 1000]),
        (0x0a00_0200, 0x0a00_0000, "$.client_ip", vec![]),
        (0, 0xffff_ffff, "$.service", vec![]),
        (0, 0xffff_ffff, "$.duration_ms", vec![]),
        (0, 0xffff_ffff, "$.absent", vec![]),
    ] {
        let actual = ipv4_timestamps(minimum, maximum, path)?;
        if actual != expected {
            bail!("SQL-LOG-018 bounds {minimum}..={maximum} at {path:?} changed: {actual:?}");
        }
    }
    let string_range_sql = recipe_sql("SQL-LOG-019", 0)?;
    let string_range_timestamps = |minimum: &str, maximum: &str, path: &str| -> Result<Vec<i64>> {
        let mut statement = connection.prepare(&string_range_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-019 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "string_min" => Value::Text(minimum.to_owned()),
                "string_max" => Value::Text(maximum.to_owned()),
                "field_path" => Value::Text(path.to_owned()),
                _ => parameter("SQL-LOG-019", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        Ok(statement
            .raw_query()
            .mapped(|row| row.get::<_, i64>(0))
            .collect::<rusqlite::Result<Vec<_>>>()?)
    };
    for (minimum, maximum, path, expected) in [
        ("us-east", "us-west", "$.deployment.region", vec![1000]),
        ("us-", "ut", "$.deployment.region", vec![2000, 1000]),
        ("", "!", "$.nested.none", vec![2000, 1000]),
        ("", "!", "$.nested.empty", vec![2000, 1000]),
        ("", "!", "$.absent", vec![2000, 1000]),
        ("", "", "$.deployment.region", vec![]),
        ("z", "a", "$.deployment.region", vec![]),
        ("0", "9", "$.duration_ms", vec![]),
        ("0", "2", "$.client_ip", vec![2000, 1000]),
    ] {
        let actual = string_range_timestamps(minimum, maximum, path)?;
        if actual != expected {
            bail!("SQL-LOG-019 bounds [{minimum:?}, {maximum:?}) at {path:?} changed: {actual:?}");
        }
    }
    let len_range_sql = recipe_sql("SQL-LOG-020", 0)?;
    let len_range_timestamps =
        |minimum: i64, maximum: i64, path: &str, start: i64, end: i64| -> Result<Vec<i64>> {
            let mut statement = connection.prepare(&len_range_sql)?;
            for index in 1..=statement.parameter_count() {
                let name = statement
                    .parameter_name(index)
                    .context("SQL-LOG-020 parameter must be named")?
                    .trim_start_matches(':');
                let value = match name {
                    "length_min" => Value::Integer(minimum),
                    "length_max" => Value::Integer(maximum),
                    "field_path" => Value::Text(path.to_owned()),
                    "start_ms" => Value::Integer(start),
                    "end_ms" => Value::Integer(end),
                    _ => parameter("SQL-LOG-020", name),
                };
                statement.raw_bind_parameter(index, value)?;
            }
            Ok(statement
                .raw_query()
                .mapped(|row| row.get::<_, i64>(0))
                .collect::<rusqlite::Result<Vec<_>>>()?)
        };
    for (minimum, maximum, path, expected) in [
        (7, 7, "$.deployment.region", vec![2000, 1000]),
        (8, 8, "$.deployment.region", vec![]),
        (0, 0, "$.nested.none", vec![2000, 1000]),
        (0, 0, "$.nested.empty", vec![2000, 1000]),
        (0, 0, "$.absent", vec![2000, 1000]),
        (1, 3, "$.duration_ms", vec![]),
        (0, 100, "$.deployment", vec![]),
        (8, 7, "$.deployment.region", vec![]),
    ] {
        let actual = len_range_timestamps(minimum, maximum, path, 1000, 2000)?;
        if actual != expected {
            bail!("SQL-LOG-020 bounds [{minimum}, {maximum}] at {path:?} changed: {actual:?}");
        }
    }
    let field_names = recipe_values("SQL-LOG-010", 0)?;
    if field_names
        != [
            vec![Value::Text("_msg".into()), Value::Integer(2)],
            vec![Value::Text("_time".into()), Value::Integer(2)],
            vec![Value::Text("client_ip".into()), Value::Integer(2)],
            vec![Value::Text("deployment".into()), Value::Integer(2)],
            vec![Value::Text("duration_ms".into()), Value::Integer(2)],
            vec![Value::Text("host".into()), Value::Integer(2)],
            vec![Value::Text("level".into()), Value::Integer(2)],
            vec![Value::Text("nested".into()), Value::Integer(2)],
            vec![Value::Text("service".into()), Value::Integer(2)],
            vec![Value::Text("tags".into()), Value::Integer(2)],
        ]
    {
        bail!("SQL-LOG-010 field discovery changed: {field_names:?}");
    }
    let projection = recipe_values("SQL-LOG-010", 1)?;
    if projection
        != [
            vec![
                Value::Integer(1000),
                Value::Text("error".into()),
                Value::Text("request timeout".into()),
                Value::Text(r#"{"host":"web-1"}"#.into()),
            ],
            vec![
                Value::Integer(2000),
                Value::Text("info".into()),
                Value::Text("request ok".into()),
                Value::Text(r#"{"host":"web-2"}"#.into()),
            ],
        ]
    {
        bail!("SQL-LOG-010 typed projection changed: {projection:?}");
    }
    let empty_counts = recipe_values("SQL-LOG-011", 1)?;
    if empty_counts != [vec![Value::Integer(2), Value::Integer(0)]] {
        bail!("SQL-LOG-011 empty counts changed: {empty_counts:?}");
    }
    let unique_values = recipe_values("SQL-LOG-012", 1)?;
    if unique_values
        != [
            vec![
                Value::Text("text".into()),
                Value::Text(r#""web-1""#.into()),
                Value::Integer(1),
            ],
            vec![
                Value::Text("text".into()),
                Value::Text(r#""web-2""#.into()),
                Value::Integer(1),
            ],
        ]
    {
        bail!("SQL-LOG-012 typed unique values changed: {unique_values:?}");
    }
    let presence = recipe_values("SQL-LOG-012", 2)?;
    if presence
        != [
            vec![
                Value::Integer(1000),
                Value::Integer(1),
                Value::Text("text".into()),
                Value::Text(r#""web-1""#.into()),
            ],
            vec![
                Value::Integer(2000),
                Value::Integer(1),
                Value::Text("text".into()),
                Value::Text(r#""web-2""#.into()),
            ],
        ]
    {
        bail!("SQL-LOG-012 presence states changed: {presence:?}");
    }
    let numeric = recipe_values("SQL-LOG-013", 0)?;
    if numeric
        != [vec![
            Value::Real(16.0),
            Value::Real(8.0),
            Value::Real(4.0),
            Value::Real(12.0),
            Value::Real(8.0),
        ]]
    {
        bail!("SQL-LOG-013 numeric aggregates changed: {numeric:?}");
    }
    let rates = recipe_values("SQL-LOG-013", 1)?;
    let [row] = rates.as_slice() else {
        bail!("SQL-LOG-013 rates changed: {rates:?}");
    };
    let [Value::Real(rate), Value::Real(rate_sum)] = row.as_slice() else {
        bail!("SQL-LOG-013 rate types changed: {rates:?}");
    };
    if (rate - 2.0 / 1.001).abs() > f64::EPSILON || (rate_sum - 16.0 / 1.001).abs() > f64::EPSILON {
        bail!("SQL-LOG-013 rate values changed: {rates:?}");
    }
    let work_error = connection
        .query_row(
            "SELECT message FROM logs
             WHERE message_contains='request' AND max_work_entries=1
             ORDER BY ts LIMIT 2",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap_err();
    if !work_error.to_string().contains("max_work_entries=1") {
        bail!("SQL log row work limit changed: {work_error}");
    }
    for sql in [
        "SELECT message FROM logs WHERE max_work_entries=0",
        "SELECT message FROM logs WHERE max_work_entries='not-an-integer'",
        "SELECT message FROM logs WHERE max_work_entries=NULL",
    ] {
        if connection
            .query_row(sql, [], |row| row.get::<_, String>(0))
            .is_ok()
        {
            bail!("SQL log row surface accepted invalid work guard: {sql}");
        }
    }
    let unbounded_hidden_rows: i64 = connection.query_row(
        "SELECT COUNT(*) FROM logs WHERE max_work_entries IS NULL",
        [],
        |row| row.get(0),
    )?;
    if unbounded_hidden_rows != 2 {
        bail!("SQL hidden-column unbounded NULL projection changed: {unbounded_hidden_rows}");
    }
    for (surface, sql) in [
        (
            "timeless_log_count",
            "SELECT n FROM timeless_log_count('logs',NULL,NULL,NULL,NULL,0)",
        ),
        (
            "timeless_log_values",
            "SELECT value FROM timeless_log_values('logs','host',NULL,NULL,NULL,NULL,10,0)",
        ),
        (
            "timeless_log_count NULL guard",
            "SELECT n FROM timeless_log_count('logs',NULL,NULL,NULL,NULL,NULL)",
        ),
        (
            "timeless_log_values NULL guard",
            "SELECT value FROM timeless_log_values('logs','host',NULL,NULL,NULL,NULL,10,NULL)",
        ),
    ] {
        let error = connection
            .query_row(sql, [], |row| row.get::<_, String>(0))
            .unwrap_err();
        let message = error.to_string();
        if !message.contains("max_work_entries must") || !message.contains("positive") {
            bail!("{surface} accepted an invalid work limit: {error}");
        }
    }

    let log_stats: BTreeMap<String, Value> = {
        let mut statement = connection.prepare(
            "SELECT key, value FROM timeless_stats('logs')
             WHERE key IN (
               'timestamp_unit','blocks','raw_blocks','compressed_blocks',
               'buffered_entries','disk_entries','total_entries',
               'bytes_on_disk','raw_bytes','compressed_bytes','terms',
               'index_bytes','ts_min','ts_max',
               'optimize_source_entries','optimize_source_bytes'
             )",
        )?;
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        rows
    };
    for key in [
        "timestamp_unit",
        "blocks",
        "raw_blocks",
        "compressed_blocks",
        "buffered_entries",
        "disk_entries",
        "total_entries",
        "bytes_on_disk",
        "raw_bytes",
        "compressed_bytes",
        "terms",
        "index_bytes",
        "ts_min",
        "ts_max",
        "optimize_source_entries",
        "optimize_source_bytes",
    ] {
        if !log_stats.contains_key(key) {
            bail!("public log statistic {key:?} is missing");
        }
    }
    let integer = |key: &str| match log_stats.get(key) {
        Some(Value::Integer(value)) => Ok(*value),
        other => bail!("public log statistic {key:?} is not an INTEGER: {other:?}"),
    };
    if log_stats.get("timestamp_unit") != Some(&Value::Text("ms".to_owned()))
        || integer("buffered_entries")? != 0
        || integer("disk_entries")? != 2
        || integer("total_entries")? != 2
        || integer("compressed_blocks")? != 0
        || integer("compressed_bytes")? != 0
        || integer("ts_min")? != 1000
        || integer("ts_max")? != 2000
        || integer("optimize_source_entries")? != 2
        || integer("optimize_source_bytes")? <= 0
        || integer("bytes_on_disk")? != integer("raw_bytes")?
        || integer("blocks")? != integer("raw_blocks")?
        || integer("terms")? <= 0
    {
        bail!("public log storage statistics changed: {log_stats:?}");
    }
    if !matches!(log_stats.get("index_bytes"), Some(Value::Integer(value)) if *value >= 0)
        && !matches!(log_stats.get("index_bytes"), Some(Value::Null))
    {
        bail!(
            "public log index_bytes must be a non-negative INTEGER or NULL: {:?}",
            log_stats.get("index_bytes")
        );
    }

    connection.execute(
        "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
        params![500_i64, "info", "unicode", r#"{"probe":"é"}"#],
    )?;
    connection.execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
    let unicode_length = len_range_timestamps(1, 1, "$.probe", 500, 500)?;
    if unicode_length != [500] {
        bail!("SQL-LOG-020 Unicode codepoint length changed: {unicode_length:?}");
    }

    connection.execute(
        "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
        params![
            3000_i64,
            "notice",
            "newest nonmatching row",
            r#"{"deployment":{"region":"elsewhere"}}"#
        ],
    )?;
    for _ in 0..2 {
        connection.execute(
            "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
            params![
                6002_i64,
                "info",
                "duplicate prefix fixture",
                r#"{"dup_field":"same"}"#
            ],
        )?;
    }
    connection.execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
    for (identifier, statement_index) in [
        ("SQL-LOG-014", 1),
        ("SQL-LOG-015", 2),
        ("SQL-LOG-017", 0),
        ("SQL-LOG-018", 0),
        ("SQL-LOG-019", 0),
        ("SQL-LOG-020", 0),
    ] {
        let sql = recipe_sql(identifier, statement_index)?;
        let mut statement = connection
            .prepare(&sql)
            .with_context(|| format!("prepare {identifier} post-filter limit regression"))?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("documented SQL parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "end_ms" => Value::Integer(3000),
                "limit" => Value::Integer(1),
                _ => parameter(identifier, name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        let timestamps = statement
            .raw_query()
            .mapped(|row| row.get::<_, i64>(0))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let expected = if matches!(identifier, "SQL-LOG-017" | "SQL-LOG-018") {
            [1000]
        } else {
            [2000]
        };
        if timestamps != expected {
            bail!(
                "{identifier} applied the result limit before retained-field filtering: {timestamps:?}"
            );
        }
    }

    connection.execute(
        "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
        params![
            4000_i64,
            "info",
            "field comparison fixture",
            r#"{"left":2,"right":"2","null_left":null,"array_left":[1],"array_right":"[1]","lex_left":"bar","lex_right":"foo","numeric_left":"10","numeric_right":"2","large_left":18446744073709551615,"large_copy":18446744073709551615,"large_right":18446744073709551614}"#
        ],
    )?;
    connection.execute(
        "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
        params![
            5000_i64,
            "info",
            "newest nonmatching field comparison fixture",
            r#"{"left":2,"right":"3","null_left":"x","array_left":[2],"array_right":"[3]","lex_left":"z","lex_right":"a","numeric_left":"9","numeric_right":"2","large_left":18446744073709551615,"large_copy":18446744073709551614,"large_right":18446744073709551614}"#
        ],
    )?;
    connection.execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
    let field_compare_sql = recipe_sql("SQL-LOG-021", 0)?;
    let field_compare_timestamps =
        |comparison: &str, left_path: &str, right_path: &str| -> Result<Vec<i64>> {
            let mut statement = connection.prepare(&field_compare_sql)?;
            for index in 1..=statement.parameter_count() {
                let name = statement
                    .parameter_name(index)
                    .context("SQL-LOG-021 parameter must be named")?
                    .trim_start_matches(':');
                let value = match name {
                    "comparison" => Value::Text(comparison.to_owned()),
                    "left_path" => Value::Text(left_path.to_owned()),
                    "right_path" => Value::Text(right_path.to_owned()),
                    "start_ms" => Value::Integer(4000),
                    "end_ms" => Value::Integer(5000),
                    "limit" => Value::Integer(1),
                    _ => parameter("SQL-LOG-021", name),
                };
                statement.raw_bind_parameter(index, value)?;
            }
            Ok(statement
                .raw_query()
                .mapped(|row| row.get::<_, i64>(0))
                .collect::<rusqlite::Result<Vec<_>>>()?)
        };
    for (comparison, left, right) in [
        ("eq", "$.left", "$.right"),
        ("eq", "$.null_left", "$.absent"),
        ("eq", "$.array_left", "$.array_right"),
        ("le_text", "$.lex_left", "$.lex_right"),
        ("lt_text", "$.lex_left", "$.lex_right"),
        ("lt_text", "$.numeric_left", "$.numeric_right"),
        ("eq", "$.large_left", "$.large_copy"),
    ] {
        let actual = field_compare_timestamps(comparison, left, right)?;
        if actual != [4000] {
            bail!(
                "SQL-LOG-021 {comparison} comparison between {left:?} and {right:?} changed: {actual:?}"
            );
        }
    }
    if !field_compare_timestamps("eq", "$.large_left", "$.large_right")?.is_empty() {
        bail!("SQL-LOG-021 collapsed distinct retained u64 values through binary64");
    }
    if !field_compare_timestamps("unknown", "$.left", "$.right")?.is_empty() {
        bail!("SQL-LOG-021 must reject an unknown comparison selector");
    }

    connection.execute(
        "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
        params![
            6000_i64,
            "info",
            "literal prefix fixture",
            r#"{"foo:bar:value":"needle","under_score":"literal"}"#
        ],
    )?;
    connection.execute(
        "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
        params![
            6001_i64,
            "info",
            "lookalike prefix fixture",
            r#"{"underXscore":"literal"}"#
        ],
    )?;
    connection.execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
    let field_prefix_sql = recipe_sql("SQL-LOG-022", 0)?;
    let field_prefix_timestamps =
        |prefix: &str, exact: &str, start: i64, end: i64| -> Result<Vec<i64>> {
            let mut statement = connection.prepare(&field_prefix_sql)?;
            for index in 1..=statement.parameter_count() {
                let name = statement
                    .parameter_name(index)
                    .context("SQL-LOG-022 parameter must be named")?
                    .trim_start_matches(':');
                let value = match name {
                    "field_prefix" => Value::Text(prefix.to_owned()),
                    "exact_text" => Value::Text(exact.to_owned()),
                    "start_ms" => Value::Integer(start),
                    "end_ms" => Value::Integer(end),
                    _ => parameter("SQL-LOG-022", name),
                };
                statement.raw_bind_parameter(index, value)?;
            }
            Ok(statement
                .raw_query()
                .mapped(|row| row.get::<_, i64>(0))
                .collect::<rusqlite::Result<Vec<_>>>()?)
        };
    for (prefix, exact, start, end, expected) in [
        ("deployment.", "us-east", 1000, 2000, vec![1000]),
        ("", "request timeout", 1000, 2000, vec![1000]),
        ("_", "request timeout", 1000, 2000, vec![1000]),
        ("_time", "1000", 1000, 2000, vec![1000]),
        ("nested.", "", 1000, 2000, vec![2000, 1000]),
        ("missing.", "", 1000, 2000, vec![]),
        ("foo:bar:", "needle", 6000, 6001, vec![6000]),
        ("under_", "literal", 6000, 6001, vec![6000]),
        ("dup_", "same", 6002, 6002, vec![6002, 6002]),
        ("deployment", r#"{"region":"us-east"}"#, 1000, 2000, vec![]),
    ] {
        let actual = field_prefix_timestamps(prefix, exact, start, end)?;
        if actual != expected {
            bail!("SQL-LOG-022 prefix {prefix:?} exact value {exact:?} changed: {actual:?}");
        }
    }

    for (ts, case) in [
        (35_999_999_i64, "day-before"),
        (36_000_000_i64, "day-start"),
        (39_600_123_i64, "day-middle"),
        (43_200_000_i64, "day-end"),
        (43_200_001_i64, "day-after"),
    ] {
        connection.execute(
            "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
            params![
                ts,
                "info",
                "clock fixture",
                format!(r#"{{"case":"{case}"}}"#)
            ],
        )?;
    }
    connection.execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
    let day_range_sql = recipe_sql("SQL-LOG-023", 0)?;
    let day_range_timestamps = |day_start_ns: i64,
                                day_end_ns: i64,
                                start_inclusive: i64,
                                end_inclusive: i64,
                                offset_ns: i64|
     -> Result<Vec<i64>> {
        let mut statement = connection.prepare(&day_range_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-023 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "day_start_ns" => Value::Integer(day_start_ns),
                "day_end_ns" => Value::Integer(day_end_ns),
                "start_inclusive" => Value::Integer(start_inclusive),
                "end_inclusive" => Value::Integer(end_inclusive),
                "offset_ns" => Value::Integer(offset_ns),
                "start_ms" => Value::Integer(35_999_999),
                "end_ms" => Value::Integer(43_200_001),
                _ => parameter("SQL-LOG-023", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        Ok(statement
            .raw_query()
            .mapped(|row| row.get::<_, i64>(0))
            .collect::<rusqlite::Result<Vec<_>>>()?)
    };
    let hour = 3_600_000_000_000_i64;
    for (start, end, start_inclusive, end_inclusive, offset, expected) in [
        (
            10 * hour,
            12 * hour,
            1,
            1,
            0,
            vec![43_200_000, 39_600_123, 36_000_000],
        ),
        (10 * hour, 12 * hour, 0, 0, 0, vec![39_600_123]),
        (
            12 * hour,
            14 * hour,
            1,
            1,
            2 * hour,
            vec![43_200_000, 39_600_123, 36_000_000],
        ),
        (
            0,
            0,
            1,
            0,
            0,
            vec![43_200_001, 43_200_000, 39_600_123, 36_000_000, 35_999_999],
        ),
        (12 * hour, 10 * hour, 1, 1, 0, vec![]),
    ] {
        let actual = day_range_timestamps(start, end, start_inclusive, end_inclusive, offset)?;
        if actual != expected {
            bail!("SQL-LOG-023 day range {start}..{end} offset {offset} changed: {actual:?}");
        }
    }

    const SUNDAY_MS: i64 = 1_798_934_400_000;
    const HOUR_MS: i64 = 3_600_000;
    const DAY_MS: i64 = 24 * HOUR_MS;
    for (ts, case) in [
        (-4 * DAY_MS, "week-pre-epoch-sun"),
        (SUNDAY_MS + 12 * HOUR_MS, "week-sun"),
        (SUNDAY_MS + 23 * HOUR_MS + HOUR_MS / 2, "week-sun-late"),
        (SUNDAY_MS + DAY_MS + HOUR_MS / 2, "week-mon-early"),
        (SUNDAY_MS + DAY_MS + 12 * HOUR_MS, "week-mon"),
        (SUNDAY_MS + 2 * DAY_MS + 12 * HOUR_MS, "week-tue"),
        (SUNDAY_MS + 3 * DAY_MS + 12 * HOUR_MS, "week-wed"),
        (SUNDAY_MS + 4 * DAY_MS + 12 * HOUR_MS, "week-thu"),
        (SUNDAY_MS + 5 * DAY_MS + 12 * HOUR_MS, "week-fri"),
        (SUNDAY_MS + 6 * DAY_MS + 12 * HOUR_MS, "week-sat"),
    ] {
        connection.execute(
            "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
            params![
                ts,
                "info",
                "week fixture",
                format!(r#"{{"case":"{case}"}}"#)
            ],
        )?;
    }
    connection.execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
    let week_range_sql = recipe_sql("SQL-LOG-024", 0)?;
    let week_range_timestamps = |week_start_day: i64,
                                 week_end_day: i64,
                                 offset_ns: i64,
                                 start_ms: i64,
                                 end_ms: i64|
     -> Result<Vec<i64>> {
        let mut statement = connection.prepare(&week_range_sql)?;
        for index in 1..=statement.parameter_count() {
            let name = statement
                .parameter_name(index)
                .context("SQL-LOG-024 parameter must be named")?
                .trim_start_matches(':');
            let value = match name {
                "week_start_day" => Value::Integer(week_start_day),
                "week_end_day" => Value::Integer(week_end_day),
                "offset_ns" => Value::Integer(offset_ns),
                "start_ms" => Value::Integer(start_ms),
                "end_ms" => Value::Integer(end_ms),
                _ => parameter("SQL-LOG-024", name),
            };
            statement.raw_bind_parameter(index, value)?;
        }
        Ok(statement
            .raw_query()
            .mapped(|row| row.get::<_, i64>(0))
            .collect::<rusqlite::Result<Vec<_>>>()?)
    };
    let week_end = SUNDAY_MS + 6 * DAY_MS + 12 * HOUR_MS;
    for (start, end, offset, lower, upper, expected) in [
        (
            1,
            5,
            0,
            SUNDAY_MS,
            week_end,
            vec![
                SUNDAY_MS + 5 * DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + 4 * DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + 3 * DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + 2 * DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + DAY_MS + HOUR_MS / 2,
            ],
        ),
        (
            0,
            6,
            0,
            SUNDAY_MS,
            week_end,
            vec![
                SUNDAY_MS + 6 * DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + 5 * DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + 4 * DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + 3 * DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + 2 * DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + DAY_MS + HOUR_MS / 2,
                SUNDAY_MS + 23 * HOUR_MS + HOUR_MS / 2,
                SUNDAY_MS + 12 * HOUR_MS,
            ],
        ),
        (5, 1, 0, SUNDAY_MS, week_end, vec![]),
        (
            1,
            1,
            5_400_000_000_000,
            SUNDAY_MS,
            week_end,
            vec![
                SUNDAY_MS + DAY_MS + 12 * HOUR_MS,
                SUNDAY_MS + DAY_MS + HOUR_MS / 2,
                SUNDAY_MS + 23 * HOUR_MS + HOUR_MS / 2,
            ],
        ),
        (0, 0, 0, -4 * DAY_MS, -4 * DAY_MS, vec![-4 * DAY_MS]),
    ] {
        let actual = week_range_timestamps(start, end, offset, lower, upper)?;
        if actual != expected {
            bail!("SQL-LOG-024 week range {start}..{end} offset {offset} changed: {actual:?}");
        }
    }

    connection.execute(
        "INSERT INTO logs(ts,level,message,metadata) VALUES(-1,'info','pre epoch','{}')",
        [],
    )?;
    connection.execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
    let mut pre_epoch_facets = connection.prepare(&facets_sql)?;
    for index in 1..=pre_epoch_facets.parameter_count() {
        let name = pre_epoch_facets
            .parameter_name(index)
            .context("SQL-LOG-031 pre-epoch parameter must be named")?
            .trim_start_matches(':');
        let value = match name {
            "start_ts" | "end_ts" => Value::Integer(-1),
            "timestamp_units_per_second" => Value::Integer(1_000_000),
            "keep_const_fields" => Value::Integer(1),
            _ => parameter("SQL-LOG-031", name),
        };
        pre_epoch_facets.raw_bind_parameter(index, value)?;
    }
    let pre_epoch_rows = pre_epoch_facets
        .raw_query()
        .mapped(|row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !pre_epoch_rows.contains(&(
        "_time".into(),
        "1969-12-31T23:59:59.999999Z".into(),
        "1".into(),
    )) {
        bail!("SQL-LOG-031 pre-epoch microsecond rendering changed: {pre_epoch_rows:?}");
    }
    Ok(())
}

pub(crate) fn run(root: &Path, args: SqlArgs) -> Result<()> {
    let extension = fs::canonicalize(root.join(args.extension))?;
    let temporary = args
        .database
        .is_none()
        .then(NamedTempFile::new)
        .transpose()?;
    let database = match (&args.database, &temporary) {
        (Some(path), _) => root.join(path),
        (None, Some(file)) => file.path().to_path_buf(),
        _ => unreachable!(),
    };
    let mut connection = open(&extension, &database)?;
    setup(&mut connection)?;
    let recipes_path = root.join("docs/QUERY_SQL_EQUIVALENTS.md");
    let recipes = parse_recipes(&recipes_path)?;
    let (recipe_count, statement_count) = execute_recipes(&connection, &recipes)?;
    semantic_regressions(&connection, &recipes)?;
    println!("query SQL equivalents: {recipe_count} recipes, {statement_count} statements: ok");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf()
    }

    #[test]
    fn every_recipe_has_unique_executable_sql() {
        let recipes = parse_recipes(&root().join("docs/QUERY_SQL_EQUIVALENTS.md")).unwrap();
        assert_eq!(recipes.len(), 113);
        assert_eq!(
            recipes
                .iter()
                .map(|recipe| recipe.statements.len())
                .sum::<usize>(),
            145
        );
        assert_eq!(
            recipes
                .iter()
                .flat_map(|recipe| &recipe.statements)
                .map(|block| split_sql(block).unwrap().len())
                .sum::<usize>(),
            151
        );
        assert!(recipes.iter().all(|recipe| !recipe.statements.is_empty()));
    }

    #[test]
    fn recipe_index_must_cover_every_executable_recipe() {
        let file = NamedTempFile::new().unwrap();
        fs::write(
            file.path(),
            "# SQL\n\n## Recipe index\n\n\
             | recipe | state |\n\
             |---|---|\n\
             | [`SQL-PROM-001`](#sql-prom-001-example) | current |\n\n\
             ### SQL-PROM-001: example\n\n```sql\nSELECT 1;\n```\n",
        )
        .unwrap();
        assert_eq!(parse_recipes(file.path()).unwrap().len(), 1);

        fs::write(
            file.path(),
            "# SQL\n\n## Recipe index\n\n\
             | recipe | state |\n\
             |---|---|\n\n\
             ### SQL-PROM-001: example\n\n```sql\nSELECT 1;\n```\n",
        )
        .unwrap();
        let error = parse_recipes(file.path()).unwrap_err().to_string();
        assert!(error.contains("missing [\"SQL-PROM-001\"]"), "{error}");
    }

    #[test]
    fn sql_splitter_preserves_semicolons_in_literals_and_comments() {
        let statements = split_sql(
            "SELECT ';' AS value; -- ; is not a boundary\n\
             SELECT \"also;not\"; /* neither;is this */ SELECT 3",
        )
        .unwrap();
        assert_eq!(statements.len(), 3);
        assert!(statements[0].contains("';'"));
        assert!(statements[1].contains("\"also;not\""));
        assert!(statements[2].ends_with("SELECT 3"));
    }

    #[test]
    fn every_shipped_sql_matrix_link_names_a_recipe() {
        let recipes: BTreeSet<_> = parse_recipes(&root().join("docs/QUERY_SQL_EQUIVALENTS.md"))
            .unwrap()
            .into_iter()
            .map(|recipe| recipe.identifier)
            .collect();
        let links = Regex::new(r"QUERY_SQL_EQUIVALENTS\.md#(sql-(?:prom|mql|log)-\d{3})").unwrap();
        for matrix in ["PROMQL_FEATURE_MATRIX.md", "LOGSQL_FEATURE_MATRIX.md"] {
            let content = fs::read_to_string(root().join("docs").join(matrix)).unwrap();
            for captures in links.captures_iter(&content) {
                let identifier = captures[1].to_uppercase();
                assert!(recipes.contains(&identifier), "missing {identifier}");
            }
        }
    }
}
