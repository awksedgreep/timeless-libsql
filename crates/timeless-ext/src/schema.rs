//! Observability schema installer — Phase 0 spike for #20/#21/#27.
//!
//! The friendly query surface (`timeless_<source>_<kind>` views plus a
//! machine-readable inventory) installs as a side effect of
//! `CREATE VIRTUAL TABLE`, refreshes best-effort on open, and uninstalls
//! on `DROP TABLE` — the same shape as the dbhealth companion views,
//! evaluated here against the #21 contract before the full surface is
//! built:
//!
//! - explicit source table names, deterministic derived object names;
//! - all collision checks run BEFORE anything is created, so a failed
//!   install alters no user objects (xCreate rollback would undo writes
//!   anyway; the pre-check turns it into a clean error instead);
//! - inventory is a real owned table, the source of truth for removal;
//! - every identifier passes through `sql_ident` quoting — the vtab and
//!   schema names are attacker-controlled.
//!
//! Spike scope: traces only, one span view. Logs/metrics surfaces arrive
//! in later phases behind this same installer.

use rusqlite::{Connection, Result};

use crate::sql_ident;

/// Version of the installed schema shape. Bumped whenever the emitted
/// DDL changes; connect refreshes older databases, removal drops only
/// rows at known versions... (v1: everything this module owns).
pub(crate) const SCHEMA_VERSION: u32 = 1;

/// The inventory table itself (one per database schema that hosts an
/// installed source table).
pub(crate) const INVENTORY_TABLE: &str = "timeless_schema_inventory";

/// Naming convention: companion objects derive deterministically from
/// the source table name. `timeless_<source>_<kind>`.
pub(crate) fn spans_view_name(source_table: &str) -> String {
    format!("timeless_{source_table}_spans")
}

/// DDL for the inventory table. Plain rowid table for maximum host
/// compatibility (no WITHOUT ROWID requirement).
pub(crate) fn inventory_ddl(database: &str) -> String {
    let inventory = sql_ident::qualified(database, INVENTORY_TABLE);
    format!(
        "CREATE TABLE IF NOT EXISTS {inventory} (\
           source_database TEXT NOT NULL, \
           source_table TEXT NOT NULL, \
           object_name TEXT NOT NULL, \
           object_kind TEXT NOT NULL, \
           schema_version INTEGER NOT NULL, \
           description TEXT NOT NULL DEFAULT '', \
           installed_at INTEGER NOT NULL, \
           PRIMARY KEY (source_database, source_table, object_name))"
    )
}

/// DDL for the spike span view: stable friendly shape over the vtab's
/// public columns only — hex-encoded IDs, native nanosecond timing kept
/// exact for joins and filtering, plus human-readable UTC (`start_time`,
/// ISO-8601 millis) and millisecond durations for people. No shadow
/// tables, no lossy projections: every column is either verbatim or an
/// exact formatting of a public column.
pub(crate) fn trace_spans_view_ddl(database: &str, table: &str) -> (String, String) {
    let name = spans_view_name(table);
    let view = sql_ident::qualified(database, &name);
    let source = sql_ident::qualified(database, table);
    let ddl = format!(
        "CREATE VIEW {view} AS \
         SELECT lower(hex(trace_id)) AS trace_id, \
                lower(hex(span_id)) AS span_id, \
                lower(hex(parent_span_id)) AS parent_span_id, \
                name, service, kind, status, \
                start_ts, \
                strftime('%Y-%m-%dT%H:%M:%fZ', start_ts / 1000000000.0, 'unixepoch') AS start_time, \
                duration_ns, \
                (duration_ns / 1000000.0) AS duration_ms, \
                attributes \
         FROM {source}"
    );
    (name, ddl)
}

/// Human description recorded for the spike view. Every future view
/// ships its own; the harness will require non-empty descriptions.
pub(crate) fn trace_spans_description() -> &'static str {
    "One row per span: hex trace/span IDs, service and timing (native \
     nanoseconds plus human-readable UTC), and verbatim attributes."
}

/// Names this installer would own for one source table (spike: one).
fn planned_objects(table: &str) -> Vec<(String, &'static str)> {
    vec![(spans_view_name(table), "view")]
}

/// True when `name` exists as a table/view/trigger in `database`.
fn object_exists(host: &Connection, database: &str, name: &str) -> Result<bool> {
    let sql = format!(
        "SELECT COUNT(*) FROM {}.sqlite_master WHERE name = ?1 \
         AND type IN ('table', 'view', 'trigger')",
        sql_ident::quote(database)
    );
    let count: i64 = host.query_row(&sql, [name], |row| row.get(0))?;
    Ok(count > 0)
}

/// Objects the inventory attributes to one source table, with their
/// recorded per-object schema versions.
fn owned_objects(host: &Connection, database: &str, table: &str) -> Result<Vec<(String, i64)>> {
    let sql = format!(
        "SELECT object_name, schema_version FROM {} \
         WHERE source_database = ?1 AND source_table = ?2",
        sql_ident::qualified(database, INVENTORY_TABLE)
    );
    let mut stmt = host.prepare(&sql)?;
    let names = stmt.query_map([database, table], |row| Ok((row.get(0)?, row.get(1)?)))?;
    names.collect()
}

/// Install the spike surface for one traces table. Fails cleanly (no
/// user object touched) when any planned name collides with an object
/// this installation does not own.
pub(crate) fn install_trace_views(
    host: &Connection,
    database: &str,
    table: &str,
) -> Result<Vec<String>> {
    let planned = planned_objects(table);
    // Collision sweep first: every name must either be absent or already
    // owned by this exact source. Nothing is created before this passes.
    // No inventory table yet is not a collision — it just means a
    // first install into this schema.
    let owned = owned_objects(host, database, table).unwrap_or_default();
    for (name, _) in &planned {
        if !owned.iter().any(|(o, _)| o == name) && object_exists(host, database, name)? {
            return Err(rusqlite::Error::ModuleError(format!(
                "timeless schema install refused: {name:?} already exists \
                 and is not owned by this installation"
            )));
        }
    }
    host.execute_batch(&inventory_ddl(database))?;
    let (name, ddl) = trace_spans_view_ddl(database, table);
    debug_assert!(planned.iter().any(|(n, _)| n == &name));
    host.execute_batch(&ddl)?;
    host.execute(
        &format!(
            "INSERT OR REPLACE INTO {} \
             (source_database, source_table, object_name, object_kind, \
              schema_version, description, installed_at) \
             VALUES (?1, ?2, ?3, 'view', ?4, ?5, unixepoch())",
            sql_ident::qualified(database, INVENTORY_TABLE)
        ),
        rusqlite::params![
            database,
            table,
            name,
            SCHEMA_VERSION as i64,
            trace_spans_description()
        ],
    )?;
    Ok(vec![name])
}

/// Best-effort refresh on open: every owned object whose recorded
/// version differs from [`SCHEMA_VERSION`] is dropped and reinstalled
/// at today's definition — per-release migration without a separate
/// step. Missing inventory (or a missing table) simply means nothing
/// to drop; install then lays down the current shape unless a user
/// object collides, in which case the old state is left alone.
/// Failures are swallowed — open must succeed on read-only
/// connections, and stale views fail loudly at query time (SQLite
/// errors on unknown columns) rather than returning wrong rows.
pub(crate) fn refresh_trace_views(host: &Connection, database: &str, table: &str) {
    let wanted = spans_view_name(table);
    let fresh = owned_objects(host, database, table)
        .map(|owned| {
            owned
                .iter()
                .any(|(name, version)| name == &wanted && *version == SCHEMA_VERSION as i64)
        })
        .unwrap_or(false);
    if fresh {
        return;
    }
    let _ = drop_trace_views(host, database, table);
    let _ = install_trace_views(host, database, table);
}

/// Remove exactly the objects the inventory attributes to one source
/// table, then forget them. Unknown (user) objects are never touched.
pub(crate) fn drop_trace_views(host: &Connection, database: &str, table: &str) -> Result<()> {
    let owned = owned_objects(host, database, table).unwrap_or_default();
    for (name, _) in &owned {
        let sql = format!(
            "DROP VIEW IF EXISTS {}",
            sql_ident::qualified(database, name)
        );
        host.execute_batch(&sql)?;
    }
    host.execute(
        &format!(
            "DELETE FROM {} WHERE source_database = ?1 AND source_table = ?2",
            sql_ident::qualified(database, INVENTORY_TABLE)
        ),
        [database, table],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_quotes_hostile_identifiers() {
        // Quotes, dots, and keywords in schema/table names must stay
        // inside their identifiers.
        let ddl = inventory_ddl("we\"ird");
        assert!(
            ddl.contains("\"we\"\"ird\".\"timeless_schema_inventory\""),
            "{ddl}"
        );
        let (name, ddl) = trace_spans_view_ddl("main", "odd\"table");
        assert_eq!(name, "timeless_odd\"table_spans");
        assert!(ddl.contains("\"odd\"\"table\""), "{ddl}");
        assert!(ddl.contains("\"timeless_odd\"\"table_spans\""), "{ddl}");
        // The view reads only the public vtab surface.
        for forbidden in ["_shadow", "_chunks", "_blocks", "_terms"] {
            assert!(!ddl.contains(forbidden), "{ddl}");
        }
    }

    #[test]
    fn planned_names_follow_the_convention() {
        assert_eq!(spans_view_name("traces"), "timeless_traces_spans");
        assert_eq!(spans_view_name("web"), "timeless_web_spans");
    }
}
