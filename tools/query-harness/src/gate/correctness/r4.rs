use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{ensure, Result};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{params, Connection, OptionalExtension};

use super::super::open;

const SIGNALS: [&str; 3] = ["metrics", "logs", "traces"];

fn scalar(connection: &Connection, sql: &str) -> Result<i64> {
    Ok(connection.query_row(sql, [], |row| row.get(0))?)
}

fn create_tables(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE VIRTUAL TABLE metrics USING timeless_metrics;
         CREATE VIRTUAL TABLE logs USING timeless_logs;
         CREATE VIRTUAL TABLE traces USING timeless_traces;",
    )?;
    Ok(())
}

fn command(connection: &Connection, table: &str, value: &str) -> Result<()> {
    connection.execute(
        &format!("INSERT INTO \"{table}\"(\"{table}\") VALUES(?1)"),
        params![value],
    )?;
    Ok(())
}

fn flush_all(connection: &Connection) -> Result<()> {
    for table in SIGNALS {
        command(connection, table, "flush")?;
    }
    Ok(())
}

fn insert_rows(connection: &Connection, suffix: &str, timestamp: i64) -> Result<()> {
    connection.execute(
        "INSERT INTO metrics(name,ts,value) VALUES(?1,?2,?3)",
        params![format!("metric_{suffix}"), timestamp, timestamp as f64],
    )?;
    connection.execute(
        "INSERT INTO logs(ts,level,message) VALUES(?1,'info',?2)",
        params![timestamp, format!("log_{suffix}")],
    )?;
    let byte: u8 = if suffix.starts_with("flushed") {
        0x11
    } else {
        0x22
    };
    connection.execute(
        "INSERT INTO traces(trace_id,span_id,name,service,start_ts) VALUES(?1,?2,?3,'r4',?4)",
        params![
            vec![byte; 16],
            vec![byte; 8],
            format!("trace_{suffix}"),
            timestamp
        ],
    )?;
    Ok(())
}

fn counts(connection: &Connection) -> Result<[i64; 3]> {
    Ok([
        scalar(connection, "SELECT COUNT(*) FROM metrics")?,
        scalar(connection, "SELECT COUNT(*) FROM logs")?,
        scalar(connection, "SELECT COUNT(*) FROM traces")?,
    ])
}

fn instance_ids(connection: &Connection) -> Result<Vec<Option<Vec<u8>>>> {
    SIGNALS
        .iter()
        .map(|table| {
            connection
                .query_row(
                    &format!("SELECT v FROM {table}_meta WHERE k='instance_id'"),
                    [],
                    |row| row.get(0),
                )
                .optional()
                .map_err(Into::into)
        })
        .collect()
}

fn drop_all(connection: &Connection) -> Result<()> {
    for table in SIGNALS {
        connection.execute(&format!("DROP TABLE \"{table}\""), [])?;
    }
    Ok(())
}

fn rollback_preserves_buffered(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("rollback.db");
    let connection = open(extension, &database)?;
    create_tables(&connection)?;
    insert_rows(&connection, "flushed", 10)?;
    flush_all(&connection)?;
    insert_rows(&connection, "buffered", 20)?;
    let before = instance_ids(&connection)?;
    ensure!(counts(&connection)? == [2, 2, 2]);
    connection.execute("BEGIN", [])?;
    drop_all(&connection)?;
    connection.execute("ROLLBACK", [])?;
    ensure!(instance_ids(&connection)? == before);
    ensure!(counts(&connection)? == [2, 2, 2]);
    flush_all(&connection)?;
    drop(connection);
    ensure!(counts(&open(extension, &database)?)? == [2, 2, 2]);
    Ok(())
}

fn rollback_keeps_shared(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("shared.db");
    let first = open(extension, &database)?;
    create_tables(&first)?;
    let second = open(extension, &database)?;
    ensure!(counts(&second)? == [0, 0, 0]);
    insert_rows(&first, "buffered_a", 30)?;
    ensure!(counts(&second)? == [1, 1, 1]);
    first.execute("BEGIN", [])?;
    drop_all(&first)?;
    first.execute("ROLLBACK", [])?;
    ensure!(counts(&second)? == [1, 1, 1]);
    insert_rows(&second, "buffered_b", 40)?;
    ensure!(counts(&first)? == [2, 2, 2]);
    Ok(())
}

fn recreate_identity(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("recreate.db");
    let first = open(extension, &database)?;
    create_tables(&first)?;
    insert_rows(&first, "old_buffered", 50)?;
    let old = instance_ids(&first)?;
    first.execute("BEGIN", [])?;
    drop_all(&first)?;
    first.execute("COMMIT", [])?;
    create_tables(&first)?;
    let new = instance_ids(&first)?;
    ensure!(old.iter().zip(&new).all(|(old, new)| old != new));
    ensure!(counts(&first)? == [0, 0, 0]);
    ensure!(counts(&open(extension, &database)?)? == [0, 0, 0]);
    Ok(())
}

fn failed_destroy(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("failed-destroy.db");
    let first = open(extension, &database)?;
    first.execute("CREATE VIRTUAL TABLE metrics USING timeless_metrics", [])?;
    let second = open(extension, &database)?;
    ensure!(scalar(&second, "SELECT COUNT(*) FROM metrics")? == 0);
    first.execute(
        "INSERT INTO metrics(name,ts,value) VALUES('buffered_before_failure',60,1)",
        [],
    )?;
    let seen = Arc::new(AtomicBool::new(false));
    let hook_seen = Arc::clone(&seen);
    first.authorizer(Some(move |context: AuthContext<'_>| match context.action {
        AuthAction::DropTable {
            table_name: "metrics_chunks",
        } => {
            hook_seen.store(true, Ordering::SeqCst);
            Authorization::Deny
        }
        _ => Authorization::Allow,
    }))?;
    ensure!(first.execute("DROP TABLE metrics", []).is_err());
    first.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?;
    ensure!(seen.load(Ordering::SeqCst));
    ensure!(scalar(&first, "SELECT COUNT(*) FROM metrics")? == 1);
    let third = open(extension, &database)?;
    ensure!(scalar(&third, "SELECT COUNT(*) FROM metrics")? == 1);
    third.execute(
        "INSERT INTO metrics(name,ts,value) VALUES('after_failure',61,2)",
        [],
    )?;
    ensure!(scalar(&second, "SELECT COUNT(*) FROM metrics")? == 2);
    Ok(())
}

fn instance_migration(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("instance-migration.db");
    let connection = open(extension, &database)?;
    create_tables(&connection)?;
    insert_rows(&connection, "flushed_migration", 70)?;
    flush_all(&connection)?;
    drop(connection);
    let plain = Connection::open(&database)?;
    for table in SIGNALS {
        plain.execute(
            &format!("DELETE FROM {table}_meta WHERE k='instance_id'"),
            [],
        )?;
    }
    drop(plain);
    let migrated = open(extension, &database)?;
    for table in SIGNALS {
        migrated.query_row(
            &format!("SELECT timeless_upgrade('{table}')"),
            [],
            |_| Ok(()),
        )?;
    }
    ensure!(counts(&migrated)? == [1, 1, 1]);
    ensure!(instance_ids(&migrated)?
        .iter()
        .all(|id| id.as_ref().is_some_and(|id| id.len() == 16)));
    drop(migrated);
    let plain = Connection::open(&database)?;
    plain.execute("UPDATE metrics_meta SET v=x'01' WHERE k='instance_id'", [])?;
    drop(plain);
    let corrupt = open(extension, &database)?;
    ensure!(corrupt
        .query_row::<i64, _, _>("SELECT COUNT(*) FROM metrics", [], |row| row.get(0))
        .is_err());
    Ok(())
}

pub(super) fn run(extension: &Path, temporary: &Path) -> Result<()> {
    rollback_preserves_buffered(extension, temporary)?;
    println!("PASS test_drop_rollback_preserves_buffered_state");
    rollback_keeps_shared(extension, temporary)?;
    println!("PASS test_drop_rollback_keeps_preconnected_engine_shared");
    recreate_identity(extension, temporary)?;
    println!("PASS test_committed_drop_recreate_gets_fresh_identity");
    failed_destroy(extension, temporary)?;
    println!("PASS test_failed_destroy_does_not_split_registry");
    instance_migration(extension, temporary)?;
    println!("PASS test_instance_id_migration_and_validation");
    Ok(())
}
