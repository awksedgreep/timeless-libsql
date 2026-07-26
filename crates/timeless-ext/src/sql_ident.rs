//! One quoting/qualification path for all virtual-table shadow SQL.

use rusqlite::vtab::escape_double_quote;

pub(crate) fn quote(identifier: &str) -> String {
    format!("\"{}\"", escape_double_quote(identifier))
}

pub(crate) fn qualified(schema: &str, object: &str) -> String {
    format!("{}.{}", quote(schema), quote(object))
}

pub(crate) fn shadow_object(table: &str, suffix: &str) -> String {
    format!("{table}_{suffix}")
}

pub(crate) fn qualified_shadow(schema: &str, table: &str, suffix: &str) -> String {
    qualified(schema, &shadow_object(table, suffix))
}

pub(crate) fn quoted_shadow(table: &str, suffix: &str) -> String {
    quote(&shadow_object(table, suffix))
}

pub(crate) fn incremental_auto_vacuum(schema: &str) -> String {
    format!("PRAGMA {}.auto_vacuum = INCREMENTAL;", quote(schema))
}
