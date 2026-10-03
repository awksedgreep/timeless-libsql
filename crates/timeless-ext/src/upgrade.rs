//! Explicit additive upgrades for legacy extension-owned shadow schemas.

use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::{ffi, Connection, Error, OptionalExtension, Result};

use crate::{logs_vtab::LogsTab, metrics_vtab::MetricsTab, traces_vtab::TracesTab};

fn module_err(message: impl Into<String>) -> Error {
    Error::ModuleError(message.into())
}

/// Split `spec` into `(schema, table)`. A `.` only separates a schema
/// when the prefix names an attached database (checked via
/// `PRAGMA database_list`); otherwise the whole spec is a table name in
/// `main`. This keeps `timeless_upgrade('my.table')` working for a bare
/// table whose name contains a dot, while `main."my.table"` still
/// resolves to schema `main`, table `my.table`. One layer of
/// double-quote quoting is removed from the table part.
fn resolve_spec(conn: &Connection, spec: &str) -> Result<(String, String)> {
    fn unquote(identifier: &str) -> String {
        if identifier.len() >= 2 && identifier.starts_with('"') && identifier.ends_with('"') {
            identifier[1..identifier.len() - 1].replace("\"\"", "\"")
        } else {
            identifier.to_owned()
        }
    }
    if let Some((prefix, rest)) = spec.split_once('.') {
        let attached: bool = conn
            .prepare("PRAGMA database_list")
            .and_then(|mut stmt| {
                stmt.query_map([], |row| row.get::<_, String>(1))
                    .and_then(|names| {
                        names
                            .collect::<Result<Vec<_>, _>>()
                            .map(|names| names.iter().any(|n| n.eq_ignore_ascii_case(prefix)))
                    })
            })
            .unwrap_or(true);
        if attached {
            return Ok((prefix.to_owned(), unquote(rest)));
        }
    }
    Ok(("main".to_owned(), unquote(spec)))
}

fn shadow_exists(conn: &Connection, database: &str, name: &str) -> Result<bool> {
    let schema = crate::sql_ident::qualified(database, "sqlite_schema");
    conn.query_row(
        &format!("SELECT 1 FROM {schema} WHERE type = 'table' AND name = ?1"),
        [name],
        |_| Ok(()),
    )
    .optional()
    .map(|row| row.is_some())
}

pub(crate) fn register(db: &Connection) -> Result<()> {
    let handle = unsafe { db.handle() } as usize;
    db.create_scalar_function(
        "timeless_upgrade",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY,
        move |ctx: &Context<'_>| {
            let spec = ctx.get::<String>(0)?;
            let handle = handle as *mut ffi::sqlite3;
            let conn = unsafe { Connection::from_handle(handle) }?;
            let (database, table) =
                resolve_spec(&conn, &spec).map_err(|e| module_err(e.to_string()))?;
            if table.is_empty() || database.is_empty() {
                return Err(module_err(
                    "timeless_upgrade: expected 'table' or 'schema.table' (a table whose name contains a dot can be passed as 'schema.\"dotted.name\"')",
                ));
            }
            let chunks = format!("{table}_chunks");
            let trace_blocks = format!("{table}_trace_blocks");
            let blocks = format!("{table}_blocks");
            if shadow_exists(&conn, &database, &chunks)? {
                MetricsTab::upgrade_legacy_schema(handle, &database, &table)?;
                Ok("timeless_metrics")
            } else if shadow_exists(&conn, &database, &trace_blocks)? {
                TracesTab::upgrade_legacy_schema(handle, &database, &table)?;
                Ok("timeless_traces")
            } else if shadow_exists(&conn, &database, &blocks)? {
                LogsTab::upgrade_legacy_schema(handle, &database, &table)?;
                Ok("timeless_logs")
            } else {
                Err(module_err(format!(
                    "timeless_upgrade: {spec:?} is not a timeless virtual table"
                )))
            }
        },
    )
}
