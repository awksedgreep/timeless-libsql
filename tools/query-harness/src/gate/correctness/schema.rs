//! Companion lifecycle regressions against the real extension (#59).

use std::path::Path;

use anyhow::{ensure, Result};
use rusqlite::{Connection, OpenFlags};

use super::super::open;

const TABLES: [&str; 3] = ["metrics", "logs", "traces"];
const LAST_VIEWS: [&str; 3] = [
    "timeless_metrics_latest",
    "timeless_logs_fields",
    "timeless_traces_roots",
];

fn command(connection: &Connection, schema: &str, table: &str) -> Result<()> {
    connection.execute(
        &format!("INSERT INTO \"{schema}\".{table}({table}) VALUES('schema')"),
        [],
    )?;
    Ok(())
}

fn count(connection: &Connection, sql: &str) -> Result<i64> {
    Ok(connection.query_row(sql, [], |row| row.get(0))?)
}

fn snapshot(connection: &Connection) -> Result<Vec<(String, String, Option<String>)>> {
    Ok(connection
        .prepare("SELECT type, name, sql FROM sqlite_schema ORDER BY type, name")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?)
}

fn read_sources(connection: &Connection) -> Result<()> {
    for table in TABLES {
        ensure!(count(connection, &format!("SELECT count(*) FROM {table}"))? == 0);
    }
    Ok(())
}

fn legacy_fixture(extension: &Path, path: &Path) -> Result<()> {
    let connection = open(extension, path)?;
    connection.execute_batch(
        "CREATE VIRTUAL TABLE metrics USING timeless_metrics;
         CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys=service);
         CREATE VIRTUAL TABLE traces USING timeless_traces;",
    )?;
    // Simulate the pre-companion database shape without depending on an
    // obsolete binary. Shadow schemas and the signal tables stay intact.
    let views = connection
        .prepare("SELECT object_name FROM timeless_schema_inventory")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for view in views {
        connection.execute_batch(&format!("DROP VIEW \"{view}\""))?;
    }
    connection.execute_batch("DROP TABLE timeless_schema_inventory")?;
    Ok(())
}

pub(super) fn run(extension: &Path, temporary: &Path) -> Result<()> {
    let path = temporary.join("companion-lifecycle.db");
    legacy_fixture(extension, &path)?;

    // Both writable and read-only first touches preserve the entire schema.
    for readonly in [false, true] {
        let connection = if readonly {
            let connection = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            unsafe {
                let _guard = rusqlite::LoadExtensionGuard::new(&connection)?;
                connection.load_extension(extension, None::<&str>)?;
            }
            connection
        } else {
            open(extension, &path)?
        };
        let before = snapshot(&connection)?;
        let version = count(&connection, "PRAGMA schema_version")?;
        read_sources(&connection)?;
        ensure!(snapshot(&connection)? == before);
        ensure!(count(&connection, "PRAGMA schema_version")? == version);
        if readonly {
            for table in TABLES {
                ensure!(command(&connection, "main", table).is_err());
            }
            ensure!(snapshot(&connection)? == before);
        }
    }

    let connection = open(extension, &path)?;
    for view in LAST_VIEWS {
        connection.execute_batch(&format!("CREATE VIEW {view} AS SELECT 73 AS user_value"))?;
    }
    drop(connection);
    let connection = open(extension, &path)?;
    let before = snapshot(&connection)?;
    read_sources(&connection)?;
    for (table, view) in TABLES.into_iter().zip(LAST_VIEWS) {
        ensure!(count(&connection, &format!("SELECT user_value FROM {view}"))? == 73);
        let error = command(&connection, "main", table).unwrap_err();
        ensure!(error.to_string().contains("not owned"), "{error}");
        ensure!(
            snapshot(&connection)? == before,
            "collision changed {table} schema"
        );
    }
    // A user resolves the collision explicitly; Timeless never adopts it.
    for view in LAST_VIEWS {
        connection.execute_batch(&format!("DROP VIEW {view}"))?;
    }
    let before = snapshot(&connection)?;
    connection.execute_batch("BEGIN")?;
    for table in TABLES {
        command(&connection, "main", table)?;
    }
    ensure!(
        count(
            &connection,
            "SELECT count(*) FROM timeless_schema_inventory"
        )? == 11
    );
    connection.execute_batch("ROLLBACK")?;
    ensure!(
        snapshot(&connection)? == before,
        "rollback left orphan companions"
    );

    for table in TABLES {
        command(&connection, "main", table)?;
    }
    let version = count(&connection, "PRAGMA schema_version")?;
    for table in TABLES {
        command(&connection, "main", table)?;
    }
    ensure!(
        count(&connection, "PRAGMA schema_version")? == version,
        "install was not idempotent"
    );
    ensure!(
        count(
            &connection,
            "SELECT count(*) FROM timeless_schema_inventory"
        )? == 11
    );

    // A late failure must restore earlier DDL and inventory writes. Test
    // each signal, with its final object failing after its siblings changed.
    for (table, view) in TABLES.into_iter().zip(LAST_VIEWS) {
        connection.execute(
            "UPDATE timeless_schema_inventory SET schema_version=0 WHERE source_table=?1",
            [table],
        )?;
        connection.execute_batch(&format!(
            "CREATE TRIGGER fail_install BEFORE INSERT ON timeless_schema_inventory
             WHEN NEW.object_name='{view}' BEGIN SELECT RAISE(ABORT,'test install failure'); END"
        ))?;
        let before = snapshot(&connection)?;
        let old = count(
            &connection,
            "SELECT count(*) FROM timeless_schema_inventory WHERE schema_version=0",
        )?;
        let error = command(&connection, "main", table).unwrap_err();
        ensure!(
            error.to_string().contains("test install failure"),
            "{error}"
        );
        ensure!(
            snapshot(&connection)? == before,
            "failed upgrade changed {table} definitions"
        );
        ensure!(
            count(
                &connection,
                "SELECT count(*) FROM timeless_schema_inventory WHERE schema_version=0"
            )? == old
        );
        connection.execute_batch("DROP TRIGGER fail_install")?;
        command(&connection, "main", table)?;
        ensure!(
            count(
                &connection,
                "SELECT count(*) FROM timeless_schema_inventory WHERE schema_version=0"
            )? == 0
        );
    }
    connection.execute_batch("UPDATE timeless_schema_inventory SET schema_version=999")?;
    let before = snapshot(&connection)?;
    for table in TABLES {
        command(&connection, "main", table)?;
    }
    ensure!(snapshot(&connection)? == before);
    ensure!(
        count(
            &connection,
            "SELECT count(*) FROM timeless_schema_inventory WHERE schema_version=999"
        )? == 11
    );
    connection.execute_batch("UPDATE timeless_schema_inventory SET schema_version=0")?;
    drop(connection);

    // Ownership follows the file, not the alias used at installation time.
    let attached = open(extension, Path::new(":memory:"))?;
    attached.execute(
        "ATTACH DATABASE ?1 AS \"renamed telemetry\"",
        [path.to_string_lossy()],
    )?;
    for table in TABLES {
        command(&attached, "renamed telemetry", table)?;
        attached.execute_batch(&format!("DROP TABLE \"renamed telemetry\".{table}"))?;
    }
    ensure!(
        count(
            &attached,
            "SELECT count(*) FROM \"renamed telemetry\".timeless_schema_inventory"
        )? == 0
    );
    ensure!(
        count(
            &attached,
            "SELECT count(*) FROM \"renamed telemetry\".sqlite_schema WHERE type='view'"
        )? == 0
    );
    attached.execute_batch(
        "CREATE VIRTUAL TABLE \"renamed telemetry\".metrics USING timeless_metrics;
         CREATE VIRTUAL TABLE \"renamed telemetry\".logs USING timeless_logs(index_keys=service);
         CREATE VIRTUAL TABLE \"renamed telemetry\".traces USING timeless_traces;",
    )?;
    ensure!(
        count(
            &attached,
            "SELECT count(*) FROM \"renamed telemetry\".timeless_schema_inventory"
        )? == 11
    );
    println!("PASS test_companion_schema_explicit_atomic_owned_lifecycle");
    Ok(())
}
