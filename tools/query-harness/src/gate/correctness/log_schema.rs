//! Reindexing reconciles configuration-dependent companions atomically (#65).
use super::super::open;
use anyhow::{ensure, Result};
use rusqlite::Connection;
use std::path::Path;

fn text(connection: &Connection, sql: &str) -> Result<String> {
    Ok(connection.query_row(sql, [], |row| row.get(0))?)
}

fn verify(connection: &Connection, keys: &[&str]) -> Result<()> {
    let stored = text(
        connection,
        "SELECT value FROM timeless_stats('logs') WHERE key='index_keys'",
    )?;
    ensure!(stored == keys.join(","), "stored keys {stored:?}");
    let fields_exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='timeless_logs_fields')",
        [],
        |row| row.get(0),
    )?;
    ensure!(fields_exists != keys.is_empty());
    if fields_exists {
        let fields = connection
            .prepare("SELECT field FROM timeless_logs_fields")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ensure!(fields == keys);
    }
    let services_exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='timeless_logs_services')",
        [],
        |row| row.get(0),
    )?;
    ensure!(services_exists == keys.contains(&"service"));
    if services_exists {
        ensure!(text(connection, "SELECT service FROM timeless_logs_services")? == "api");
    }
    for key in keys {
        let value = match *key {
            "service" => "api",
            "host" => "edge",
            "zone" => "west",
            _ => unreachable!(),
        };
        let count: i64 = connection.query_row(
            &format!("SELECT count(*) FROM logs WHERE {key}=?1"),
            [value],
            |row| row.get(0),
        )?;
        ensure!(count == 1, "indexed {key} query lost its stored row");
    }
    ensure!(text(connection, "SELECT message FROM user_report")? == "kept");
    let installed: i64 = connection.query_row("SELECT installed_at FROM timeless_schema_inventory WHERE object_name='timeless_logs_entries'", [], |row| row.get(0))?;
    ensure!(installed == 123, "unchanged entries view was reinstalled");
    Ok(())
}

pub(super) fn run(extension: &Path, temporary: &Path) -> Result<()> {
    let path = temporary.join("log-schema-reindex.db");
    let connection = open(extension, &path)?;
    connection.execute_batch("CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service');
        INSERT INTO logs(ts,level,message,metadata) VALUES(1700000000123,'info','kept','{\"service\":\"api\",\"host\":\"edge\",\"zone\":\"west\"}');
        INSERT INTO logs(logs) VALUES('flush');
        CREATE VIEW user_report AS SELECT message FROM logs;
        UPDATE timeless_schema_inventory SET installed_at=123 WHERE object_name='timeless_logs_entries';")?;
    drop(connection);
    for keys in [
        vec!["service", "host"],
        vec!["host"],
        vec!["host", "service", "zone"],
        vec![],
        vec!["service"],
    ] {
        let connection = open(extension, &path)?;
        connection.execute(
            "INSERT INTO logs(logs) VALUES(?1)",
            [format!("reindex:{}", keys.join(","))],
        )?;
        // Even on the old hidden-column layout, a following schema command
        // must use the new persisted configuration, not undo the refresh.
        connection.execute_batch("INSERT INTO logs(logs) VALUES('schema')")?;
        drop(connection);
        verify(&open(extension, &path)?, &keys)?;
    }
    let connection = open(extension, &path)?;
    connection.execute_batch("BEGIN; INSERT INTO logs(logs) VALUES('reindex:host'); ROLLBACK;")?;
    drop(connection);
    verify(&open(extension, &path)?, &["service"])?;

    // A collision cannot change either postings or inventory.
    let connection = open(extension, &path)?;
    connection.execute_batch("INSERT INTO logs(logs) VALUES('reindex:host')")?;
    drop(connection);
    let connection = open(extension, &path)?;
    connection.execute_batch("CREATE VIEW timeless_logs_services AS SELECT 'mine' AS service")?;
    ensure!(connection
        .execute_batch("INSERT INTO logs(logs) VALUES('reindex:service')")
        .is_err());
    ensure!(text(&connection, "SELECT service FROM timeless_logs_services")? == "mine");
    connection.execute_batch("DROP VIEW timeless_logs_services")?;
    verify(&connection, &["host"])?;
    connection.execute_batch("CREATE TRIGGER fail_log_install BEFORE INSERT ON timeless_schema_inventory WHEN NEW.object_name='timeless_logs_fields' BEGIN SELECT RAISE(ABORT,'injected install failure'); END;")?;
    ensure!(connection
        .execute_batch("INSERT INTO logs(logs) VALUES('reindex:service')")
        .is_err());
    connection.execute_batch("DROP TRIGGER fail_log_install")?;
    drop(connection);
    verify(&open(extension, &path)?, &["host"])?;

    // Repair an older stale catalog explicitly; reading it never does DDL.
    let connection = open(extension, &path)?;
    connection.execute_batch(
        "DROP VIEW timeless_logs_fields;
        CREATE VIEW timeless_logs_fields AS SELECT 'service' AS field;
        ALTER TABLE timeless_schema_inventory DROP COLUMN source_config;
        CREATE VIEW timeless_logs_services AS SELECT DISTINCT service FROM logs;
        INSERT INTO timeless_schema_inventory(source_database,source_table,object_name,object_kind,schema_version,description,installed_at)
        VALUES('main','logs','timeless_logs_services','view',1,'legacy service view',123);",
    )?;
    drop(connection);
    let connection = open(extension, &path)?;
    ensure!(text(&connection, "SELECT field FROM timeless_logs_fields")? == "service");
    ensure!(connection
        .prepare("SELECT * FROM timeless_logs_services")
        .is_err());
    connection.execute_batch("INSERT INTO logs(logs) VALUES('schema'); UPDATE timeless_schema_inventory SET installed_at=123 WHERE object_name='timeless_logs_entries';")?;
    verify(&connection, &["host"])?;
    // Definitions recorded by a newer extension are preserved on removal.
    connection.execute_batch("UPDATE timeless_schema_inventory SET schema_version=999 WHERE object_name='timeless_logs_fields'; INSERT INTO logs(logs) VALUES('reindex:');")?;
    ensure!(text(&connection, "SELECT field FROM timeless_logs_fields")? == "host");
    println!("PASS test_log_reindex_companions_configuration_rollback_and_legacy_upgrade");
    Ok(())
}
