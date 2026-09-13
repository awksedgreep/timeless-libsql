use std::path::Path;

use anyhow::{ensure, Result};
use rusqlite::Connection;

use super::super::open;

fn names(connection: &Connection, schema: &str) -> Result<Vec<String>> {
    Ok(connection
        .prepare(&format!(
            "SELECT name FROM \"{schema}\".timeless_metrics_series ORDER BY name"
        ))?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

pub(super) fn run(extension: &Path, temporary: &Path) -> Result<()> {
    let main = temporary.join("catalog-main.db");
    let auxiliary = temporary.join("catalog-aux.db");
    let backup = temporary.join("catalog-backup.db");
    let connection = open(extension, &main)?;
    connection.execute("ATTACH DATABASE ?1 AS aux", [auxiliary.to_string_lossy()])?;
    connection.execute_batch(
        "CREATE VIRTUAL TABLE main.metrics USING timeless_metrics;
         CREATE VIRTUAL TABLE aux.metrics USING timeless_metrics;
         INSERT INTO main.metrics(name,ts,value) VALUES('MAIN',1,1);
         INSERT INTO aux.metrics(name,ts,value) VALUES('ATTACHED',2,2);
         INSERT INTO main.metrics(metrics) VALUES('flush');
         INSERT INTO aux.metrics(metrics) VALUES('flush');",
    )?;
    ensure!(names(&connection, "main")? == ["MAIN"]);
    ensure!(names(&connection, "aux")? == ["ATTACHED"]);
    let span: (i64, i64, i64) = connection.query_row(
        "SELECT min_ts, max_ts, points FROM aux.timeless_metrics_series",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    ensure!(span == (2, 2, 1));
    ensure!(connection
        .execute(
            "INSERT INTO aux.timeless_metrics_series(name) VALUES('BAD')",
            []
        )
        .is_err());

    // Simulate the old view on the attached source. Upgrade only this object;
    // the independent latest view keeps its version and installation record.
    connection.execute_batch(
        "DROP TABLE aux.timeless_metrics_series;
         CREATE VIEW aux.timeless_metrics_series AS
           SELECT name, labels, series_id, min_ts, max_ts, points, chunks, buffered
             FROM timeless_series('metrics');
         UPDATE aux.timeless_schema_inventory SET object_kind='view',schema_version=1
           WHERE object_name='timeless_metrics_series';
         UPDATE aux.timeless_schema_inventory SET installed_at=123
           WHERE object_name='timeless_metrics_latest';",
    )?;
    connection.execute_batch("BEGIN; INSERT INTO aux.metrics(metrics) VALUES('schema');")?;
    ensure!(names(&connection, "aux")? == ["ATTACHED"]);
    connection.execute_batch("ROLLBACK")?;
    let kind: String = connection.query_row(
        "SELECT type FROM aux.sqlite_schema WHERE name='timeless_metrics_series'",
        [],
        |row| row.get(0),
    )?;
    ensure!(kind == "view", "rollback failed to restore the legacy view");
    connection.execute_batch("INSERT INTO aux.metrics(metrics) VALUES('schema');")?;
    ensure!(names(&connection, "aux")? == ["ATTACHED"]);
    let latest: (i64, i64) = connection.query_row(
        "SELECT schema_version,installed_at FROM aux.timeless_schema_inventory
          WHERE object_name='timeless_metrics_latest'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    ensure!(latest == (1, 123), "unrelated companion was upgraded");
    drop(connection);

    let standalone = open(extension, &auxiliary)?;
    ensure!(names(&standalone, "main")? == ["ATTACHED"]);
    standalone.execute("VACUUM INTO ?1", [backup.to_string_lossy()])?;
    standalone.execute_batch(
        "INSERT INTO metrics(name,ts,value) VALUES('LATER',3,3);
         INSERT INTO metrics(metrics) VALUES('flush');",
    )?;
    drop(standalone);

    let connection = open(extension, &main)?;
    connection.execute(
        "ATTACH DATABASE ?1 AS \"renamed telemetry\"",
        [auxiliary.to_string_lossy()],
    )?;
    connection.execute("ATTACH DATABASE ?1 AS copied", [backup.to_string_lossy()])?;
    ensure!(names(&connection, "main")? == ["MAIN"]);
    ensure!(names(&connection, "renamed telemetry")? == ["ATTACHED", "LATER"]);
    ensure!(names(&connection, "copied")? == ["ATTACHED"]);
    connection.execute_batch("DROP TABLE \"renamed telemetry\".metrics")?;
    ensure!(names(&connection, "copied")? == ["ATTACHED"]);
    drop(connection);
    let standalone_backup = open(extension, &backup)?;
    ensure!(names(&standalone_backup, "main")? == ["ATTACHED"]);
    println!("PASS test_metrics_companion_catalog_alias_reopen_backup_and_upgrade");
    Ok(())
}
