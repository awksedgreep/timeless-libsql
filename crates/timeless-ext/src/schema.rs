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

/// One installable companion object: its derived name, DDL, and the
/// human description recorded in the inventory.
pub(crate) struct SchemaObject {
    pub name: String,
    pub kind: &'static str,
    pub ddl: String,
    pub description: &'static str,
}

/// The full trace companion set for one source table, in dependency
/// order. Every builder reads only public vtab/view surfaces.
pub(crate) fn trace_objects(database: &str, table: &str) -> Vec<SchemaObject> {
    let spans = spans_view_name(table);
    let summary = format!("timeless_{table}_summary");
    let services = format!("timeless_{table}_services");
    let operations = format!("timeless_{table}_operations");
    let errors = format!("timeless_{table}_errors");
    let roots = format!("timeless_{table}_roots");
    vec![
        SchemaObject {
            name: spans.clone(),
            kind: "view",
            ddl: trace_spans_view_ddl(database, table).1,
            description: trace_spans_description(),
        },
        SchemaObject {
            name: summary.clone(),
            kind: "view",
            ddl: trace_summary_view_ddl(database, &spans, &summary),
            description: "One row per retained trace: span and error counts, \
                envelope timing (native plus human-readable), invalid-end \
                count, root rows/state (missing/unique/ambiguous, never \
                invented), service count and ordered service set, and \
                completeness always 'unknown'.",
        },
        SchemaObject {
            name: services.clone(),
            kind: "view",
            ddl: trace_services_view_ddl(database, &spans, &services),
            description: "Distinct service names retained in this source, \
                ordered. Backed by a spans-view scan, not the discovery TVF.",
        },
        SchemaObject {
            name: operations.clone(),
            kind: "view",
            ddl: trace_operations_view_ddl(database, &spans, &operations),
            description: "Distinct service/operation pairs retained in this \
                source, ordered. Backed by a spans-view scan.",
        },
        SchemaObject {
            name: errors.clone(),
            kind: "view",
            ddl: trace_filtered_view_ddl(database, &spans, &errors, "status = 'error'"),
            description: "Retained spans whose status is exactly 'error', \
                in spans-view shape.",
        },
        SchemaObject {
            name: roots.clone(),
            kind: "view",
            ddl: trace_filtered_view_ddl(database, &spans, &roots, "parent_span_id IS NULL"),
            description: "Retained root spans (no parent), in spans-view shape.",
        },
    ]
}

/// Service catalog over the spans view: distinct retained service
/// names, ordered. Plain SQL rather than the discovery TVF so the
/// definition stays inspectable in the view text itself.
pub(crate) fn trace_services_view_ddl(database: &str, spans: &str, name: &str) -> String {
    let view = sql_ident::qualified(database, name);
    let spans = sql_ident::quote(spans);
    format!(
        "CREATE VIEW {view} AS \
         SELECT DISTINCT service FROM {spans} ORDER BY service"
    )
}

/// Operation catalog: distinct retained service/operation pairs,
/// ordered. Same plain-SQL rationale as the service catalog.
pub(crate) fn trace_operations_view_ddl(database: &str, spans: &str, name: &str) -> String {
    let view = sql_ident::qualified(database, name);
    let spans = sql_ident::quote(spans);
    format!(
        "CREATE VIEW {view} AS \
         SELECT DISTINCT service, name AS operation FROM {spans} \
         ORDER BY service, operation"
    )
}

/// Span-shaped filtered views (error spans, root spans): the exact
/// spans-view column list with one predicate. Sharing the list keeps
/// the shape from drifting; see [`TRACE_SPAN_COLUMNS`].
pub(crate) fn trace_filtered_view_ddl(
    database: &str,
    spans: &str,
    name: &str,
    predicate: &str,
) -> String {
    let view = sql_ident::qualified(database, name);
    let spans = sql_ident::quote(spans);
    format!(
        "CREATE VIEW {view} AS SELECT {} FROM {spans} WHERE {predicate}",
        TRACE_SPAN_COLUMNS.join(", ")
    )
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
/// ISO-8601 millis) and millisecond durations for people. Absent parents
/// stay NULL (plain `lower(hex())` would render them as empty strings —
/// SQLite's `hex()` maps NULL to empty). No shadow tables, no lossy
/// projections: every column is either verbatim or an exact formatting
/// of a public column.
///
/// The `FROM` target is deliberately UNQUALIFIED: unqualified names in a
/// stored view resolve to the view's home database, so the view survives
/// direct opens, backup copies, and ATTACH under any alias. A qualified
/// `"aux"."traces"` would brick all three (verified the hard way: schema
/// validation fails when the file opens without that alias attached).
/// Only the `CREATE VIEW` target itself is schema-qualified.
pub(crate) fn trace_spans_view_ddl(database: &str, table: &str) -> (String, String) {
    let name = spans_view_name(table);
    let view = sql_ident::qualified(database, &name);
    let source = sql_ident::quote(table);
    let ddl = format!(
        "CREATE VIEW {view} AS \
         SELECT lower(hex(trace_id)) AS trace_id, \
                lower(hex(span_id)) AS span_id, \
                CASE WHEN parent_span_id IS NULL THEN NULL \
                     ELSE lower(hex(parent_span_id)) END AS parent_span_id, \
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

/// Shared select list for the span-shaped views, so the error and root
/// filters cannot drift from the spans view. A test pins the spans
/// view's own columns to this list via PRAGMA table_info.
pub(crate) const TRACE_SPAN_COLUMNS: &[&str] = &[
    "trace_id",
    "span_id",
    "parent_span_id",
    "name",
    "service",
    "kind",
    "status",
    "start_ts",
    "start_time",
    "duration_ns",
    "duration_ms",
    "attributes",
];

/// One-row-per-trace retained snapshot (TSQ-04/TSQ-05, SQL recipe
/// "Retained trace summaries"): exact group aggregates over the spans
/// view, generalized from `WHERE trace_id = ?1` to `GROUP BY trace_id`.
/// Repeated rows count repeatedly, roots/services are sets-or-states
/// (never invented scalars), completeness is always `unknown`, and
/// invalid durations NULL the envelope instead of coercing it.
pub(crate) fn trace_summary_view_ddl(database: &str, spans: &str, name: &str) -> String {
    let view = sql_ident::qualified(database, name);
    // Unqualified, like every view body: resolves to the home database
    // under direct opens, copies, and foreign aliases alike.
    let spans = sql_ident::quote(spans);
    format!(
        "CREATE VIEW {view} AS \
         WITH retained AS ( \
           SELECT trace_id, span_id, parent_span_id, name, service, status, \
                  start_ts, duration_ns, \
                  CASE WHEN duration_ns >= 0 \
                        AND start_ts <= 9223372036854775807 - duration_ns \
                       THEN start_ts + duration_ns END AS valid_end_ts \
           FROM {spans} \
         ) \
         SELECT trace_id, \
                count(*) AS span_rows, \
                count(DISTINCT span_id) AS distinct_span_ids, \
                count(*) FILTER (WHERE status = 'error') AS error_rows, \
                min(start_ts) AS start_ts, \
                strftime('%Y-%m-%dT%H:%M:%fZ', min(start_ts) / 1000000000.0, 'unixepoch') AS start_time, \
                max(valid_end_ts) AS end_ts, \
                CASE WHEN max(valid_end_ts) IS NULL THEN NULL \
                     ELSE strftime('%Y-%m-%dT%H:%M:%fZ', max(valid_end_ts) / 1000000000.0, 'unixepoch') END AS end_time, \
                CASE WHEN count(*) = 0 \
                       OR count(*) FILTER (WHERE valid_end_ts IS NULL) <> 0 THEN NULL \
                     WHEN min(start_ts) >= 0 THEN max(valid_end_ts) - min(start_ts) \
                     WHEN max(valid_end_ts) <= 9223372036854775807 + min(start_ts) \
                       THEN max(valid_end_ts) - min(start_ts) \
                END AS duration_ns, \
                (CASE WHEN count(*) = 0 \
                       OR count(*) FILTER (WHERE valid_end_ts IS NULL) <> 0 THEN NULL \
                     WHEN min(start_ts) >= 0 THEN max(valid_end_ts) - min(start_ts) \
                     WHEN max(valid_end_ts) <= 9223372036854775807 + min(start_ts) \
                       THEN max(valid_end_ts) - min(start_ts) \
                END) / 1000000.0 AS duration_ms, \
                count(*) FILTER (WHERE valid_end_ts IS NULL) AS invalid_end_rows, \
                count(*) FILTER (WHERE parent_span_id IS NULL) AS root_rows, \
                CASE WHEN count(*) FILTER (WHERE parent_span_id IS NULL) = 1 \
                  THEN min(span_id) FILTER (WHERE parent_span_id IS NULL) END AS root_span_id, \
                CASE WHEN count(*) FILTER (WHERE parent_span_id IS NULL) = 1 \
                  THEN min(name) FILTER (WHERE parent_span_id IS NULL) END AS root_name, \
                CASE WHEN count(*) FILTER (WHERE parent_span_id IS NULL) = 1 \
                  THEN min(service) FILTER (WHERE parent_span_id IS NULL) END AS root_service, \
                CASE count(*) FILTER (WHERE parent_span_id IS NULL) \
                  WHEN 0 THEN 'missing' WHEN 1 THEN 'unique' ELSE 'ambiguous' END AS root_state, \
                count(DISTINCT service) AS service_count, \
                (SELECT group_concat(service, ',') FROM \
                   (SELECT DISTINCT service FROM {spans} WHERE trace_id = retained.trace_id \
                    ORDER BY service)) AS services, \
                'unknown' AS completeness \
         FROM retained GROUP BY trace_id"
    )
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

/// Install one companion object and record it. The caller sweeps
/// collisions first; this runs inside the caller's transaction where
/// one exists (xCreate), so a later failure still rolls everything
/// back.
fn install_object(
    host: &Connection,
    database: &str,
    table: &str,
    object: &SchemaObject,
) -> Result<()> {
    host.execute_batch(&object.ddl)?;
    host.execute(
        &format!(
            "INSERT OR REPLACE INTO {} \
             (source_database, source_table, object_name, object_kind, \
              schema_version, description, installed_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, unixepoch())",
            sql_ident::qualified(database, INVENTORY_TABLE)
        ),
        rusqlite::params![
            database,
            table,
            object.name,
            object.kind,
            SCHEMA_VERSION as i64,
            object.description
        ],
    )?;
    Ok(())
}

/// Remove one owned object and forget it. Unknown names are a no-op
/// through `IF EXISTS`; the inventory row goes with the object.
fn drop_object(host: &Connection, database: &str, table: &str, name: &str) -> Result<()> {
    host.execute_batch(&format!(
        "DROP VIEW IF EXISTS {}",
        sql_ident::qualified(database, name)
    ))?;
    host.execute(
        &format!(
            "DELETE FROM {} WHERE source_database = ?1 AND source_table = ?2 \
             AND object_name = ?3",
            sql_ident::qualified(database, INVENTORY_TABLE)
        ),
        [database, table, name],
    )?;
    Ok(())
}

/// Install the trace companion set for one source table. Fails cleanly
/// (no user object touched) when any planned name collides with an
/// object this installation does not own.
/// Escape a TEXT value as a SQL string literal. TVF table arguments
/// travel as data, never identifiers: a hostile table name must stay
/// inside its quotes.
pub(crate) fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Companion set for one logs table (Phase 4). `per_second` bakes the
/// friendly-timestamp divisor into the entries view — millisecond
/// tables divide by `1000.0`, microsecond tables by `1000000.0` — so
/// the view never has to know the unit at query time. The services
/// view exists only when `service` is a declared index key (otherwise
/// its hidden column does not exist and the view could never
/// resolve); the fields view exists only for non-empty key sets.
/// Partial installs are inventoried exactly.
pub(crate) fn log_objects(
    database: &str,
    table: &str,
    index_keys: &[String],
    per_second: i64,
) -> Vec<SchemaObject> {
    let source = sql_ident::quote(table);
    let mut objects = Vec::new();
    let entries = format!("timeless_{table}_entries");
    objects.push(SchemaObject {
        name: entries.clone(),
        kind: "view",
        ddl: format!(
            "CREATE VIEW {} AS \
             SELECT ts, \
                    strftime('%Y-%m-%dT%H:%M:%fZ', ts / {per_second}.0, 'unixepoch') AS ts_time, \
                    level, message, metadata \
             FROM {source}",
            sql_ident::qualified(database, &entries)
        ),
        description: "One row per log entry: native timestamp plus \
            human-readable UTC, severity level, message, and verbatim \
            typed metadata JSON.",
    });
    if index_keys.iter().any(|key| key == "service") {
        let name = format!("timeless_{table}_services");
        objects.push(SchemaObject {
            name: name.clone(),
            kind: "view",
            ddl: format!(
                "CREATE VIEW {} AS \
                 SELECT DISTINCT service FROM {source} ORDER BY service",
                sql_ident::qualified(database, &name)
            ),
            description: "Distinct service values retained in this table, \
                ordered. Only installed when service is a declared index key.",
        });
    }
    if !index_keys.is_empty() {
        let name = format!("timeless_{table}_fields");
        let values = index_keys
            .iter()
            .map(|key| format!("({})", sql_literal(key)))
            .collect::<Vec<_>>()
            .join(", ");
        objects.push(SchemaObject {
            name: name.clone(),
            kind: "view",
            ddl: format!(
                "CREATE VIEW {} AS SELECT column1 AS field FROM (VALUES {values})",
                sql_ident::qualified(database, &name)
            ),
            description: "Declared index keys of this table, in declaration \
                order: the fields that filter efficiently. Arbitrary JSON \
                paths stay queryable via json_extract (SQL-LOG-005).",
        });
    }
    objects
}

/// Companion set for one metrics table (Phase 4): series catalog and
/// latest values. Both compose existing public TVFs and base-table SQL
/// with the source name baked in — no parameters, no new evaluators.
pub(crate) fn metric_objects(database: &str, table: &str) -> Vec<SchemaObject> {
    // Unqualified bodies (see the spans view): portable across direct
    // opens, copies, and foreign aliases. TVF arguments are string
    // literals, which carry no schema at all.
    let source = sql_ident::quote(table);
    let series = format!("timeless_{table}_series");
    let latest = format!("timeless_{table}_latest");
    vec![
        SchemaObject {
            name: series.clone(),
            kind: "view",
            ddl: format!(
                "CREATE VIEW {} AS \
                 SELECT name, labels, series_id, min_ts, max_ts, points, chunks, buffered \
                 FROM timeless_series({})",
                sql_ident::qualified(database, &series),
                sql_literal(table)
            ),
            description: "Every retained series: metric name, canonical \
                labels, id, span, and point/chunk/buffer counts. Catalog \
                reads only, never chunk payloads.",
        },
        SchemaObject {
            name: latest.clone(),
            kind: "view",
            // Exact arg-max join, not SQLite's bare-column min/max idiom:
            // duplicate (name, labels, ts) rows are all returned, never
            // silently deduplicated. Labels group by canonical text.
            ddl: format!(
                "CREATE VIEW {} AS \
                 SELECT m.name, m.labels, m.ts, \
                        strftime('%Y-%m-%dT%H:%M:%fZ', m.ts, 'unixepoch') AS ts_time, \
                        m.value \
                 FROM {source} m \
                 JOIN (SELECT name, labels, max(ts) AS ts FROM {source} \
                       GROUP BY name, labels) AS latest \
                   USING (name, labels, ts) \
                 ORDER BY m.name, m.labels, m.ts",
                sql_ident::qualified(database, &latest)
            ),
            description: "Newest sample per series with human-readable UTC. \
                Duplicate timestamps return all tied rows. Full scan with \
                documented cost.",
        },
    ]
}

/// Install an arbitrary companion set. Fails cleanly (no user object
/// touched) when any planned name collides with an object this
/// installation does not own.
pub(crate) fn install_objects(
    host: &Connection,
    database: &str,
    table: &str,
    planned: &[SchemaObject],
) -> Result<Vec<String>> {
    // Collision sweep first: every name must either be absent or already
    // owned by this exact source. Nothing is created before this passes.
    // No inventory table yet is not a collision — it just means a
    // first install into this schema.
    let owned = owned_objects(host, database, table).unwrap_or_default();
    for object in planned {
        if !owned.iter().any(|(o, _)| o == &object.name)
            && object_exists(host, database, &object.name)?
        {
            return Err(rusqlite::Error::ModuleError(format!(
                "timeless schema install refused: {:?} already exists \
                 and is not owned by this installation",
                object.name
            )));
        }
    }
    host.execute_batch(&inventory_ddl(database))?;
    for object in planned {
        install_object(host, database, table, object)?;
    }
    Ok(planned.iter().map(|object| object.name.clone()).collect())
}

pub(crate) fn install_trace_views(
    host: &Connection,
    database: &str,
    table: &str,
) -> Result<Vec<String>> {
    install_objects(host, database, table, &trace_objects(database, table))
}

pub(crate) fn install_log_views(
    host: &Connection,
    database: &str,
    table: &str,
    index_keys: &[String],
    per_second: i64,
) -> Result<Vec<String>> {
    install_objects(
        host,
        database,
        table,
        &log_objects(database, table, index_keys, per_second),
    )
}

pub(crate) fn install_metric_views(
    host: &Connection,
    database: &str,
    table: &str,
) -> Result<Vec<String>> {
    install_objects(host, database, table, &metric_objects(database, table))
}

/// Best-effort refresh on open, per owned object: anything whose
/// recorded version differs from [`SCHEMA_VERSION`] is dropped and
/// reinstalled at today's definition — per-release migration without a
/// separate step, and a failed object never blocks its siblings.
/// Missing inventory simply means nothing to drop. Failures are
/// swallowed — open must succeed on read-only connections, and stale
/// views fail loudly at query time (SQLite errors on unknown columns)
/// rather than returning wrong rows.
pub(crate) fn refresh_trace_views(host: &Connection, database: &str, table: &str) {
    refresh_objects(host, database, table, &trace_objects(database, table));
}

pub(crate) fn refresh_log_views(
    host: &Connection,
    database: &str,
    table: &str,
    index_keys: &[String],
    per_second: i64,
) {
    refresh_objects(
        host,
        database,
        table,
        &log_objects(database, table, index_keys, per_second),
    );
}

pub(crate) fn refresh_metric_views(host: &Connection, database: &str, table: &str) {
    refresh_objects(host, database, table, &metric_objects(database, table));
}

/// Best-effort refresh on open, per owned object: anything whose
/// recorded version differs from [`SCHEMA_VERSION`] is dropped and
/// reinstalled at today's definition — per-release migration without a
/// separate step, and a failed object never blocks its siblings.
/// Missing inventory simply means nothing to drop. Failures are
/// swallowed — open must succeed on read-only connections, and stale
/// views fail loudly at query time (SQLite errors on unknown columns)
/// rather than returning wrong rows.
///
/// NOTE: refresh rebuilds the planned set from CURRENT table config
/// (index keys, timestamp unit). A set that shrinks across versions —
/// e.g. a view retired upstream — leaves its owned row installed but
/// stale-versioned; removal of retired companions belongs to an
/// explicit uninstall, never to a best-effort open.
pub(crate) fn refresh_objects(
    host: &Connection,
    database: &str,
    table: &str,
    planned: &[SchemaObject],
) {
    let owned = owned_objects(host, database, table).unwrap_or_default();
    for object in planned {
        let fresh = owned
            .iter()
            .any(|(name, version)| name == &object.name && *version == SCHEMA_VERSION as i64);
        if fresh {
            continue;
        }
        let _ = drop_object(host, database, table, &object.name);
        let _ = install_object(host, database, table, object);
    }
}

/// Remove exactly the objects the inventory attributes to one source
/// table, then forget them. Unknown (user) objects are never touched.
/// Signal-agnostic: every vtab's `xDestroy` funnels through here.
pub(crate) fn drop_objects(host: &Connection, database: &str, table: &str) -> Result<()> {
    let owned = owned_objects(host, database, table).unwrap_or_default();
    for (name, _) in &owned {
        drop_object(host, database, table, name)?;
    }
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
        // CREATE targets stay schema-qualified; bodies never are (a
        // qualified body bricks direct opens, copies, and foreign
        // aliases — the view must resolve to its home database).
        assert!(
            ddl.starts_with("CREATE VIEW \"main\".\"timeless_odd\"\"table_spans\""),
            "{ddl}"
        );
        assert!(ddl.contains("FROM \"odd\"\"table\""), "{ddl}");
        assert!(!ddl.contains("\"main\".\"odd\"\"table\""), "{ddl}");
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
