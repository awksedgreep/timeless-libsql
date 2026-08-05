use std::path::Path;

use anyhow::{ensure, Result};
use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection};

use super::super::open;

const I64_MIN: i64 = i64::MIN;
const I64_MAX: i64 = i64::MAX;

#[derive(Clone, Copy)]
struct Signal {
    table: &'static str,
    time: &'static str,
    equality: &'static str,
    columns: &'static str,
}

const SIGNALS: [Signal; 3] = [
    Signal {
        table: "metrics",
        time: "ts",
        equality: "name",
        columns: "name,ts,value",
    },
    Signal {
        table: "logs",
        time: "ts",
        equality: "level",
        columns: "ts,level,message",
    },
    Signal {
        table: "traces",
        time: "start_ts",
        equality: "name",
        columns: "trace_id,span_id,name,service,start_ts",
    },
];

fn setup(extension: &Path, database: &Path) -> Result<Connection> {
    let connection = open(extension, database)?;
    connection.execute_batch(
        "CREATE VIRTUAL TABLE metrics USING timeless_metrics;
         CREATE TABLE plain_metrics(name TEXT,ts INTEGER,value REAL);
         CREATE VIRTUAL TABLE logs USING timeless_logs;
         CREATE TABLE plain_logs(ts INTEGER,level TEXT,message TEXT);
         CREATE VIRTUAL TABLE traces USING timeless_traces;
         CREATE TABLE plain_traces(trace_id BLOB,span_id BLOB,name TEXT,service TEXT,start_ts INTEGER);",
    )?;
    for table in ["metrics", "plain_metrics"] {
        connection.execute(
            &format!("INSERT INTO {table}(name,ts,value) VALUES(?1,?2,?3)"),
            params!["edge", I64_MIN, -1.0],
        )?;
        connection.execute(
            &format!("INSERT INTO {table}(name,ts,value) VALUES(?1,?2,?3)"),
            params!["edge", I64_MAX, 1.0],
        )?;
    }
    for table in ["logs", "plain_logs"] {
        connection.execute(
            &format!("INSERT INTO {table}(ts,level,message) VALUES(?1,?2,?3)"),
            params![I64_MIN, "info", "min"],
        )?;
        connection.execute(
            &format!("INSERT INTO {table}(ts,level,message) VALUES(?1,?2,?3)"),
            params![I64_MAX, "error", "max"],
        )?;
    }
    for table in ["traces", "plain_traces"] {
        connection.execute(
            &format!("INSERT INTO {table}(trace_id,span_id,name,service,start_ts) VALUES(?1,?2,?3,?4,?5)"),
            params![vec![1_u8; 16], vec![1_u8; 8], "min", "svc", I64_MIN],
        )?;
        connection.execute(
            &format!("INSERT INTO {table}(trace_id,span_id,name,service,start_ts) VALUES(?1,?2,?3,?4,?5)"),
            params![vec![2_u8; 16], vec![2_u8; 8], "max", "svc", I64_MAX],
        )?;
    }
    Ok(connection)
}

fn rows(connection: &Connection, sql: &str, parameters: &[Value]) -> Result<Vec<Vec<Value>>> {
    let mut statement = connection.prepare(sql)?;
    let column_count = statement.column_count();
    let rows = statement
        .query_map(params_from_iter(parameters.iter()), |row| {
            (0..column_count)
                .map(|column| row.get::<_, Value>(column))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn parity(
    connection: &Connection,
    signal: Signal,
    predicate: &str,
    parameters: &[Value],
) -> Result<()> {
    let suffix = if predicate.is_empty() {
        String::new()
    } else {
        format!(" WHERE {predicate}")
    };
    let actual = rows(
        connection,
        &format!(
            "SELECT {} FROM {}{} ORDER BY {}",
            signal.columns, signal.table, suffix, signal.time
        ),
        parameters,
    )?;
    let expected = rows(
        connection,
        &format!(
            "SELECT {} FROM plain_{}{} ORDER BY {}",
            signal.columns, signal.table, suffix, signal.time
        ),
        parameters,
    )?;
    ensure!(
        actual == expected,
        "{} predicate {predicate:?}: {actual:?} != {expected:?}",
        signal.table
    );
    Ok(())
}

fn flush(connection: &Connection) -> Result<()> {
    for signal in SIGNALS {
        connection.execute(
            &format!(
                "INSERT INTO {}({}) VALUES ('flush')",
                signal.table, signal.table
            ),
            [],
        )?;
    }
    Ok(())
}

fn stages<F>(extension: &Path, database: &Path, mut check: F) -> Result<()>
where
    F: FnMut(&Connection) -> Result<()>,
{
    let mut connection = setup(extension, database)?;
    check(&connection)?;
    flush(&connection)?;
    check(&connection)?;
    drop(connection);
    connection = open(extension, database)?;
    check(&connection)?;
    Ok(())
}

pub(super) fn run(extension: &Path, temporary: &Path) -> Result<()> {
    stages(
        extension,
        &temporary.join("r8_unconstrained.db"),
        |connection| {
            for signal in SIGNALS {
                parity(connection, signal, "", &[])?;
            }
            Ok(())
        },
    )?;
    stages(
        extension,
        &temporary.join("r8_inclusive.db"),
        |connection| {
            for signal in SIGNALS {
                parity(
                    connection,
                    signal,
                    &format!("{} >= ?1 AND {} <= ?2", signal.time, signal.time),
                    &[Value::Integer(I64_MIN), Value::Integer(I64_MAX)],
                )?;
            }
            Ok(())
        },
    )?;
    stages(extension, &temporary.join("r8_null.db"), |connection| {
        for signal in SIGNALS {
            for predicate in [
                format!("{} >= ?1", signal.time),
                format!("{} <= ?1", signal.time),
                format!("{} = ?1", signal.equality),
            ] {
                parity(connection, signal, &predicate, &[Value::Null])?;
            }
        }
        Ok(())
    })?;
    stages(extension, &temporary.join("r8_strict.db"), |connection| {
        for signal in SIGNALS {
            for (operator, value) in [
                (">", I64_MIN),
                ("<", I64_MAX),
                (">", I64_MAX),
                ("<", I64_MIN),
            ] {
                parity(
                    connection,
                    signal,
                    &format!("{} {operator} ?1", signal.time),
                    &[Value::Integer(value)],
                )?;
            }
        }
        Ok(())
    })?;
    println!("PASS: timestamp extremes and NULL constraints match SQLite");
    Ok(())
}
