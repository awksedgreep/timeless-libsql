use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection};

use super::open;

fn count(connection: &Connection, table: &str) -> Result<i64> {
    Ok(
        connection.query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |row| {
            row.get(0)
        })?,
    )
}

pub(super) fn run(extension: &Path, database: &Path) -> Result<()> {
    let connection = open(extension, database)?;
    connection.execute("CREATE VIRTUAL TABLE dbhealth USING dbhealth(every=1)", [])?;
    thread::sleep(Duration::from_millis(4500));
    ensure!(
        count(&connection, "dbhealth")? > 0,
        "scheduler did not collect after create"
    );
    let mut statement = connection.prepare("SELECT DISTINCT name FROM dbhealth")?;
    let series = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(series.iter().any(|name| name == "db_pages"));
    ensure!(series.iter().any(|name| name == "db_file_bytes"));
    drop(statement);

    connection.execute("INSERT INTO dbhealth(dbhealth) VALUES ('sample')", [])?;
    let report = connection
        .prepare("SELECT \"check\", status FROM dbhealth_report")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()?;
    ensure!(report.len() == 7, "unexpected report: {report:?}");
    ensure!(report.get("sampling").map(String::as_str) == Some("ok"));
    let error = connection
        .execute("INSERT INTO dbhealth(dbhealth) VALUES ('bogus')", [])
        .expect_err("bogus command unexpectedly succeeded");
    ensure!(
        error.to_string().contains("sample"),
        "unexpected error: {error}"
    );
    drop(connection);

    thread::sleep(Duration::from_secs(2));
    let connection = open(extension, database)?;
    let before = count(&connection, "dbhealth")?;
    thread::sleep(Duration::from_millis(3500));
    let after = count(&connection, "dbhealth")?;
    ensure!(
        after > before,
        "scheduler did not resume ({before} -> {after})"
    );

    connection.execute("CREATE VIRTUAL TABLE manual USING dbhealth(every=0)", [])?;
    thread::sleep(Duration::from_millis(2600));
    ensure!(
        count(&connection, "manual")? == 0,
        "every=0 scheduled samples"
    );
    connection.execute("DROP TABLE manual", [])?;
    connection.execute("DROP TABLE dbhealth", [])?;
    let left: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name LIKE 'dbhealth%'",
        [],
        |row| row.get(0),
    )?;
    ensure!(left == 0, "{left} dbhealth objects remain after DROP");
    drop(connection);

    let connection = open(extension, database)?;
    connection.execute("CREATE VIRTUAL TABLE legacy USING dbhealth(every=0)", [])?;
    connection.execute(
        "UPDATE legacy_meta SET v=CAST(v AS BLOB) WHERE k IN ('health_flush_every','health_every')",
        [],
    )?;
    drop(connection);
    let connection = open(extension, database)?;
    let baseline = count(&connection, "legacy")?;
    connection.execute("INSERT INTO legacy(legacy) VALUES ('sample')", [])?;
    ensure!(count(&connection, "legacy")? > baseline);
    let kinds = connection
        .prepare("SELECT k,typeof(v) FROM legacy_meta WHERE k LIKE 'health%'")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()?;
    ensure!(kinds.get("health_flush_every").map(String::as_str) == Some("text"));
    connection.execute("DROP TABLE legacy", [])?;
    drop(connection);

    let sqld = database
        .parent()
        .context("dbhealth database needs a parent directory")?
        .join("demo.sqld");
    fs::create_dir(&sqld)?;
    let sqld_database = sqld.join("data");
    let connection = open(extension, &sqld_database)?;
    connection.execute("CREATE VIRTUAL TABLE dbhealth USING dbhealth(every=1)", [])?;
    thread::sleep(Duration::from_millis(2800));
    ensure!(
        count(&connection, "dbhealth")? == 0,
        "scheduler ran under .sqld layout"
    );
    connection.execute(
        "INSERT INTO dbhealth(dbhealth) VALUES (?1)",
        params!["sample"],
    )?;
    ensure!(count(&connection, "dbhealth")? > 0);
    println!("ALL DBHEALTH CHECKS PASSED");
    Ok(())
}
