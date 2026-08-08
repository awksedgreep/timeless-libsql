use std::collections::BTreeSet;
use std::ffi::CString;
use std::path::Path;
use std::time::Duration;

use anyhow::{ensure, Result};
use rusqlite::backup::Backup;
use rusqlite::{params, Connection, MAIN_DB};

use super::super::open;

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", quote(schema), quote(table))
}

fn connect(extension: &Path, main: &Path, attachments: &[(&str, &Path)]) -> Result<Connection> {
    let connection = open(extension, main)?;
    for (schema, path) in attachments {
        connection.execute(
            &format!("ATTACH DATABASE ?1 AS {}", quote(schema)),
            params![path.to_string_lossy()],
        )?;
    }
    Ok(connection)
}

fn scalar(connection: &Connection, sql: &str) -> Result<i64> {
    Ok(connection.query_row(sql, [], |row| row.get(0))?)
}

fn create_signal_tables(connection: &Connection, schema: &str) -> Result<()> {
    connection.execute(
        &format!(
            "CREATE VIRTUAL TABLE {} USING timeless_metrics",
            qualified(schema, "metrics")
        ),
        [],
    )?;
    connection.execute(
        &format!(
            "CREATE VIRTUAL TABLE {} USING timeless_logs(index_keys=service)",
            qualified(schema, "logs")
        ),
        [],
    )?;
    connection.execute(
        &format!(
            "CREATE VIRTUAL TABLE {} USING timeless_traces",
            qualified(schema, "traces")
        ),
        [],
    )?;
    Ok(())
}

fn command(connection: &Connection, schema: &str, table: &str, value: &str) -> Result<()> {
    connection.execute(
        &format!(
            "INSERT INTO {}({}) VALUES(?1)",
            qualified(schema, table),
            quote(table)
        ),
        params![value],
    )?;
    Ok(())
}

fn insert_signal_rows(
    connection: &Connection,
    schema: &str,
    marker: &str,
    base_ts: i64,
) -> Result<()> {
    connection.execute(
        &format!(
            "INSERT INTO {}(name,ts,value,labels) VALUES(?1,?2,?3,?4)",
            qualified(schema, "metrics")
        ),
        params![
            format!("metric_{marker}"),
            base_ts,
            1.5,
            format!(r#"{{"schema":"{marker}"}}"#)
        ],
    )?;
    connection.execute(
        &format!(
            "INSERT INTO {}(ts,level,message,metadata) VALUES(?1,'info',?2,?3)",
            qualified(schema, "logs")
        ),
        params![
            base_ts,
            format!("log_{marker}"),
            format!(r#"{{"service":"{marker}"}}"#)
        ],
    )?;
    let byte: u8 = if marker == "main" { 0x11 } else { 0x22 };
    connection.execute(
        &format!(
            "INSERT INTO {}(trace_id,span_id,name,service,start_ts) VALUES(?1,?2,?3,?4,?5)",
            qualified(schema, "traces")
        ),
        params![
            vec![byte; 16],
            vec![byte; 8],
            format!("trace_{marker}"),
            marker,
            base_ts
        ],
    )?;
    for table in ["metrics", "logs", "traces"] {
        command(connection, schema, table, "flush")?;
    }
    Ok(())
}

fn assert_signal_rows(connection: &Connection, schema: &str, marker: &str) -> Result<()> {
    for (table, column, value) in [
        ("metrics", "name", format!("metric_{marker}")),
        ("logs", "message", format!("log_{marker}")),
        ("traces", "name", format!("trace_{marker}")),
    ] {
        let count: i64 = connection.query_row(
            &format!(
                "SELECT COUNT(*) FROM {} WHERE {}=?1",
                qualified(schema, table),
                quote(column)
            ),
            params![value],
            |row| row.get(0),
        )?;
        ensure!(count == 1, "{schema}.{table} did not contain {marker}");
    }
    Ok(())
}

fn schema_objects(connection: &Connection, schema: &str) -> Result<BTreeSet<(String, String)>> {
    let sql = format!(
        "SELECT type,name FROM {}.sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        quote(schema)
    );
    Ok(connection
        .prepare(&sql)?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?)
}

fn complete_lifecycle(extension: &Path, temporary: &Path) -> Result<()> {
    let main = temporary.join("main.db");
    let aux = temporary.join("aux.db");
    let backup_path = temporary.join("aux-backup.db");
    let connection = connect(extension, &main, &[("aux", aux.as_path())])?;
    create_signal_tables(&connection, "aux")?;
    let expected = BTreeSet::from([
        ("table".into(), "metrics".into()),
        ("table".into(), "metrics_chunks".into()),
        ("table".into(), "metrics_meta".into()),
        ("table".into(), "metrics_series".into()),
        ("index".into(), "metrics_chunks_series_ts".into()),
        ("table".into(), "logs".into()),
        ("table".into(), "logs_blocks".into()),
        ("table".into(), "logs_terms".into()),
        ("table".into(), "logs_meta".into()),
        ("index".into(), "logs_blocks_ts".into()),
        ("table".into(), "traces".into()),
        ("table".into(), "traces_blocks".into()),
        ("table".into(), "traces_terms".into()),
        ("table".into(), "traces_trace_blocks".into()),
        ("table".into(), "traces_duration_bounds".into()),
        ("table".into(), "traces_meta".into()),
        ("index".into(), "traces_blocks_ts".into()),
    ]);
    ensure!(schema_objects(&connection, "aux")? == expected);
    ensure!(schema_objects(&connection, "main")?.is_empty());
    create_signal_tables(&connection, "main")?;
    insert_signal_rows(&connection, "main", "main", 100)?;
    insert_signal_rows(&connection, "aux", "aux", 200)?;
    assert_signal_rows(&connection, "main", "main")?;
    assert_signal_rows(&connection, "aux", "aux")?;
    for schema in ["main", "aux"] {
        ensure!(
            scalar(
                &connection,
                &format!("SELECT COUNT(*) FROM {schema}.metrics_chunks")
            )? == 1
        );
        ensure!(
            scalar(
                &connection,
                &format!("SELECT COUNT(*) FROM {schema}.logs_blocks")
            )? == 1
        );
        ensure!(
            scalar(
                &connection,
                &format!("SELECT COUNT(*) FROM {schema}.traces_blocks")
            )? == 1
        );
    }
    for (table, value) in [
        ("metrics", "compact"),
        ("logs", "optimize"),
        ("traces", "optimize"),
    ] {
        command(&connection, "aux", table, value)?;
    }
    for table in ["metrics", "logs", "traces"] {
        command(&connection, "aux", table, "prune:150")?;
    }
    assert_signal_rows(&connection, "main", "main")?;
    assert_signal_rows(&connection, "aux", "aux")?;
    connection.execute("DETACH DATABASE aux", [])?;
    connection.execute("ATTACH DATABASE ?1 AS aux", params![aux.to_string_lossy()])?;
    assert_signal_rows(&connection, "main", "main")?;
    assert_signal_rows(&connection, "aux", "aux")?;

    let mut destination = Connection::open(&backup_path)?;
    let aux_name = CString::new("aux")?;
    let backup =
        Backup::new_with_names(&connection, aux_name.as_c_str(), &mut destination, MAIN_DB)?;
    backup.run_to_completion(5, Duration::from_millis(10), None)?;
    drop(backup);
    drop(destination);
    drop(connection);

    let connection = connect(extension, &main, &[("aux", aux.as_path())])?;
    assert_signal_rows(&connection, "main", "main")?;
    assert_signal_rows(&connection, "aux", "aux")?;
    let backup = open(extension, &backup_path)?;
    assert_signal_rows(&backup, "main", "aux")?;
    drop(backup);
    for table in ["metrics", "logs", "traces"] {
        connection.execute(&format!("DROP TABLE {}", qualified("aux", table)), [])?;
    }
    ensure!(schema_objects(&connection, "aux")?.is_empty());
    assert_signal_rows(&connection, "main", "main")?;
    Ok(())
}

fn quoted_names(extension: &Path, temporary: &Path) -> Result<()> {
    let main = temporary.join("quoted-main.db");
    let aux = temporary.join("quoted-aux.db");
    let schema = "aux\"quoted";
    let tables = [
        ("metrics", "metrics\"quoted"),
        ("logs", "logs\"quoted"),
        ("traces", "traces\"quoted"),
    ];
    let connection = connect(extension, &main, &[(schema, aux.as_path())])?;
    connection.execute(
        &format!(
            "CREATE VIRTUAL TABLE {} USING timeless_metrics",
            qualified(schema, tables[0].1)
        ),
        [],
    )?;
    connection.execute(
        &format!(
            "CREATE VIRTUAL TABLE {} USING timeless_logs",
            qualified(schema, tables[1].1)
        ),
        [],
    )?;
    connection.execute(
        &format!(
            "CREATE VIRTUAL TABLE {} USING timeless_traces",
            qualified(schema, tables[2].1)
        ),
        [],
    )?;
    connection.execute(
        &format!(
            "INSERT INTO {}(name,ts,value) VALUES('quoted_metric',1,1)",
            qualified(schema, tables[0].1)
        ),
        [],
    )?;
    connection.execute(
        &format!(
            "INSERT INTO {}(ts,level,message) VALUES(1,'info','quoted_log')",
            qualified(schema, tables[1].1)
        ),
        [],
    )?;
    connection.execute(&format!("INSERT INTO {}(trace_id,span_id,name,service,start_ts) VALUES(zeroblob(16),zeroblob(8),'quoted_trace','quoted',1)", qualified(schema, tables[2].1)), [])?;
    for (_, table) in tables {
        command(&connection, schema, table, "flush")?;
    }
    for ((_, table), (column, value)) in tables.into_iter().zip([
        ("name", "quoted_metric"),
        ("message", "quoted_log"),
        ("name", "quoted_trace"),
    ]) {
        ensure!(
            scalar(
                &connection,
                &format!(
                    "SELECT COUNT(*) FROM {} WHERE {}='{}'",
                    qualified(schema, table),
                    quote(column),
                    value
                )
            )? == 1
        );
    }
    ensure!(schema_objects(&connection, "main")?.is_empty());
    drop(connection);
    let connection = connect(extension, &main, &[(schema, aux.as_path())])?;
    ensure!(
        scalar(
            &connection,
            &format!("SELECT COUNT(*) FROM {}", qualified(schema, tables[0].1))
        )? == 1
    );
    for (_, table) in tables {
        connection.execute(&format!("DROP TABLE {}", qualified(schema, table)), [])?;
    }
    ensure!(schema_objects(&connection, schema)?.is_empty());
    Ok(())
}

fn private_schemas(extension: &Path) -> Result<()> {
    let connection = connect(
        extension,
        Path::new(":memory:"),
        &[("aux_mem", Path::new(":memory:"))],
    )?;
    connection.execute_batch(
        "CREATE VIRTUAL TABLE main.metrics USING timeless_metrics;
         CREATE VIRTUAL TABLE aux_mem.metrics USING timeless_metrics;
         INSERT INTO main.metrics(name,ts,value) VALUES('main_private',1,1);
         INSERT INTO aux_mem.metrics(name,ts,value) VALUES('aux_private',2,2);",
    )?;
    ensure!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM main.metrics WHERE name='main_private'"
        )? == 1
    );
    ensure!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM main.metrics WHERE name='aux_private'"
        )? == 0
    );
    ensure!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM aux_mem.metrics WHERE name='aux_private'"
        )? == 1
    );
    ensure!(
        scalar(
            &connection,
            "SELECT COUNT(*) FROM aux_mem.metrics WHERE name='main_private'"
        )? == 0
    );
    command(&connection, "main", "metrics", "flush")?;
    command(&connection, "aux_mem", "metrics", "flush")?;
    ensure!(scalar(&connection, "SELECT COUNT(*) FROM main.metrics_chunks")? == 1);
    ensure!(scalar(&connection, "SELECT COUNT(*) FROM aux_mem.metrics_chunks")? == 1);
    Ok(())
}

fn aliased_file(extension: &Path, temporary: &Path) -> Result<()> {
    let telemetry = temporary.join("aliased-telemetry.db");
    let other = temporary.join("aliased-other-main.db");
    let owner = open(extension, &telemetry)?;
    create_signal_tables(&owner, "main")?;
    insert_signal_rows(&owner, "main", "main", 300)?;
    let attached = connect(
        extension,
        &other,
        &[("renamed_telemetry", telemetry.as_path())],
    )?;
    assert_signal_rows(&attached, "renamed_telemetry", "main")?;
    insert_signal_rows(&attached, "renamed_telemetry", "aux", 400)?;
    assert_signal_rows(&owner, "main", "aux")?;
    Ok(())
}

pub(super) fn run(extension: &Path, temporary: &Path) -> Result<()> {
    complete_lifecycle(extension, temporary)?;
    println!("PASS test_complete_isolation_and_lifecycle");
    quoted_names(extension, temporary)?;
    println!("PASS test_quoted_schema_and_table_names");
    private_schemas(extension)?;
    println!("PASS test_private_attached_schemas_do_not_share_engine");
    aliased_file(extension, temporary)?;
    println!("PASS test_same_file_under_different_aliases_uses_local_schema");
    Ok(())
}
