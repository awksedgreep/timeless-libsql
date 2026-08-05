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
            r#"{"service":"api","host":"web-1","deployment":{"region":"us-east"},"duration_ms":12,"nested":{"ok":true,"count":2,"none":null,"empty":""}}"#,
        ])?;
        insert.execute(params![
            2000,
            "info",
            "request ok",
            r#"{"service":"api","host":"web-2","deployment":{"region":"us-west"},"duration_ms":4,"nested":{"ok":"true","count":"2","empty":null}}"#,
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
        "SQL-PROM-054" | "SQL-PROM-056" => "sql_histogram_bucket",
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
        "lookback" | "window" => Value::Integer(if counter_window { 60 } else { 20 }),
        "history" => Value::Integer(300),
        "max_lookback" => Value::Integer(0),
        "offset" => Value::Integer(10),
        "max_work_points" => Value::Integer(100_000),
        "max_work_entries" => Value::Integer(100_000),
        "threshold" if identifier == "SQL-MQL-001" => Value::Real(15.0),
        "threshold" => Value::Real(0.0),
        "default_value" => Value::Real(0.0),
        "scalar" | "scalar_value" | "value" => Value::Real(2.0),
        "q" | "quantile" => Value::Real(0.5),
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
    if session_thirteen_recipe_rows != [8, 2, 2, 1, 1, 2, 2, 1, 1] {
        bail!("Session 13 SQL-LOG recipe results changed: {session_thirteen_recipe_rows:?}");
    }
    let field_names = recipe_values("SQL-LOG-010", 0)?;
    if field_names
        != [
            vec![Value::Text("_msg".into()), Value::Integer(2)],
            vec![Value::Text("_time".into()), Value::Integer(2)],
            vec![Value::Text("deployment".into()), Value::Integer(2)],
            vec![Value::Text("duration_ms".into()), Value::Integer(2)],
            vec![Value::Text("host".into()), Value::Integer(2)],
            vec![Value::Text("level".into()), Value::Integer(2)],
            vec![Value::Text("nested".into()), Value::Integer(2)],
            vec![Value::Text("service".into()), Value::Integer(2)],
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
        assert_eq!(recipes.len(), 76);
        assert_eq!(
            recipes
                .iter()
                .map(|recipe| recipe.statements.len())
                .sum::<usize>(),
            100
        );
        assert_eq!(
            recipes
                .iter()
                .flat_map(|recipe| &recipe.statements)
                .map(|block| split_sql(block).unwrap().len())
                .sum::<usize>(),
            106
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
