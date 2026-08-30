//! Explicit additive upgrades for legacy extension-owned shadow schemas.

use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::{ffi, Connection, Error, OptionalExtension, Result};

use crate::{logs_vtab::LogsTab, metrics_vtab::MetricsTab, traces_vtab::TracesTab};

fn module_err(message: impl Into<String>) -> Error {
    Error::ModuleError(message.into())
}

fn split_spec(spec: &str) -> (&str, &str) {
    spec.split_once('.').unwrap_or(("main", spec))
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
            let (database, table) = split_spec(&spec);
            if table.is_empty() || database.is_empty() {
                return Err(module_err(
                    "timeless_upgrade: expected 'table' or 'schema.table'",
                ));
            }
            let handle = handle as *mut ffi::sqlite3;
            let conn = unsafe { Connection::from_handle(handle) }?;
            let chunks = format!("{table}_chunks");
            let trace_blocks = format!("{table}_trace_blocks");
            let blocks = format!("{table}_blocks");
            if shadow_exists(&conn, database, &chunks)? {
                MetricsTab::upgrade_legacy_schema(handle, database, table)?;
                Ok("timeless_metrics")
            } else if shadow_exists(&conn, database, &trace_blocks)? {
                TracesTab::upgrade_legacy_schema(handle, database, table)?;
                Ok("timeless_traces")
            } else if shadow_exists(&conn, database, &blocks)? {
                LogsTab::upgrade_legacy_schema(handle, database, table)?;
                Ok("timeless_logs")
            } else {
                Err(module_err(format!(
                    "timeless_upgrade: {spec:?} is not a timeless virtual table"
                )))
            }
        },
    )
}
