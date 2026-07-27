//! Metadata shared by all three shadow-store families.

use rusqlite::{Connection, OptionalExtension};

use crate::sql_ident;

pub(crate) type InstanceId = [u8; 16];

fn decode_instance_id(bytes: Vec<u8>, table: &str) -> Result<InstanceId, String> {
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        format!(
            "{table}: instance_id is {} byte(s); expected 16",
            bytes.len()
        )
    })
}

/// Persist a small text setting in the table's `_meta` (any module —
/// all three share the (k, v) shape). Used for CREATE-argument settings
/// like retention that are properties of the DATA, not of the caller.
pub(crate) fn save_meta_text(
    conn: &Connection,
    database: &str,
    table: &str,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let meta = sql_ident::qualified_shadow(database, table, "meta");
    conn.execute(
        &format!("INSERT OR REPLACE INTO {meta}(k, v) VALUES(?1, ?2)"),
        rusqlite::params![key, value],
    )
    .map_err(|err| format!("{table}: failed to save {key} in _meta: {err}"))?;
    Ok(())
}

/// Load a text setting saved by save_meta_text (None = never set).
pub(crate) fn load_meta_text(
    conn: &Connection,
    database: &str,
    table: &str,
    key: &str,
) -> Result<Option<String>, String> {
    let meta = sql_ident::qualified_shadow(database, table, "meta");
    conn.query_row(
        &format!("SELECT v FROM {meta} WHERE k = ?1"),
        rusqlite::params![key],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(|err| format!("{table}: failed to load {key} from _meta: {err}"))
}

/// Load the persisted F2 retention setting (native ts units), if any.
pub(crate) fn load_retention(
    conn: &Connection,
    database: &str,
    table: &str,
) -> Result<Option<i64>, String> {
    match load_meta_text(conn, database, table, "retention")? {
        None => Ok(None),
        Some(text) => text
            .parse::<i64>()
            .map(Some)
            .map_err(|_| format!("{table}: retention in _meta is not an integer: {text:?}")),
    }
}

/// Load the table's durable instance identity, creating it transactionally
/// when upgrading a pre-R4 database. The read-first path avoids issuing a
/// write from normal xConnect calls, including read-only connections.
pub(crate) fn ensure_instance_id(
    conn: &Connection,
    database: &str,
    table: &str,
) -> Result<InstanceId, String> {
    let meta = sql_ident::qualified_shadow(database, table, "meta");
    let select = format!("SELECT v FROM {meta} WHERE k = 'instance_id'");
    if let Some(bytes) = conn
        .query_row(&select, [], |row| row.get::<_, Vec<u8>>(0))
        .optional()
        .map_err(|err| format!("{table}: failed to load instance_id: {err}"))?
    {
        return decode_instance_id(bytes, table);
    }

    conn.execute(
        &format!("INSERT OR IGNORE INTO {meta}(k, v) VALUES('instance_id', randomblob(16))"),
        [],
    )
    .map_err(|err| format!("{table}: failed to create instance_id: {err}"))?;
    let bytes = conn
        .query_row(&select, [], |row| row.get::<_, Vec<u8>>(0))
        .map_err(|err| format!("{table}: failed to reload instance_id: {err}"))?;
    decode_instance_id(bytes, table)
}
