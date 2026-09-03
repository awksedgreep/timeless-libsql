//! Session 1 spike: a writable virtual table ("timeless_spike") that persists
//! rows into a shadow table on the host connection.
//!
//! This proved the two load-bearing unknowns from PLAN.md in one shot:
//!   Spike A - a writable vtab in Rust, built as a loadable .so
//!   Spike B - re-entrant SQL against the host connection from inside vtab
//!             callbacks (xCreate makes the shadow table, xUpdate inserts
//!             into it, the cursor reads it back) - the FTS5 pattern.
//!
//! It is kept compiling (and registered) as a reference implementation; the
//! real vtab lives in metrics_vtab.rs. The extension entry points moved to
//! lib.rs so they can register both modules.
//!
//! Usage:
//!   .load target/debug/libtimeless_ext
//!   CREATE VIRTUAL TABLE spike USING timeless_spike;
//!   INSERT INTO spike(ts, value) VALUES (1, 2.5);
//!   SELECT * FROM spike;

use std::borrow::Cow;
use std::ffi::{c_int, CStr};
use std::marker::PhantomData;

use rusqlite::ffi;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    Context, CreateVTab, Filters, IndexInfo, Inserts, Module, TransactionVTab, UpdateVTab, Updates,
    VTab, VTabConnection, VTabCursor, VTabKind,
};
use rusqlite::{Connection, Result};

use crate::sql_ident;

/// Register the "timeless_spike" module on a freshly-loaded connection.
/// Called from the shared extension entry point in lib.rs.
pub(crate) fn register(db: &Connection) -> Result<()> {
    // `Module::update_module_with_tx()` wires up xCreate/xConnect/xUpdate
    // AND xBegin/xCommit/xRollback (the TransactionVTab impl below).
    const MODULE: Module<SpikeTab> = Module::update_module_with_tx();
    db.create_module(c"timeless_spike", &MODULE, None::<()>)
}

// ---------------------------------------------------------------------------
// The virtual table
// ---------------------------------------------------------------------------

/// One instance per `CREATE VIRTUAL TABLE ... USING timeless_spike` (or per
/// re-connect to an existing one).
///
/// `#[repr(C)]` + `base` as FIRST field is mandatory: SQLite treats a pointer
/// to this struct as a pointer to `sqlite3_vtab` (C-style inheritance).
#[repr(C)]
struct SpikeTab {
    base: ffi::sqlite3_vtab,
    /// Raw handle to the HOST connection (the db the user's SQL runs on).
    /// Never stored as a `Connection`: a stored handle would dangle after
    /// `sqlite3_close`. Every callback re-wraps it with `host()` for the
    /// duration of that call only — the same discipline as the
    /// production vtabs.
    db: *mut ffi::sqlite3,
    /// Name of our shadow table, RAW ("<vtab_name>_shadow"). The vtab
    /// name is attacker-controlled (it can contain `"`), so this is
    /// quoted with `sql_ident::quote` at every SQL construction site,
    /// never interpolated bare.
    shadow: String,
    /// Pre-formatted SQL so the hot path allocates nothing per row.
    insert_sql: String,
}

impl SpikeTab {
    /// Borrow the host connection for the duration of one callback.
    ///
    /// `Connection::from_handle` wraps the raw pointer WITHOUT taking
    /// ownership - dropping it does not close the user's database. This is
    /// the re-entrancy trick (Spike B): FTS5 does exactly this in C.
    fn host(&self) -> Result<Connection> {
        unsafe { Connection::from_handle(self.db) }
    }

    fn connect_create(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        table_name: &[u8],
        _args: &[&[u8]],
        is_create: bool,
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let table = String::from_utf8_lossy(table_name).into_owned();
        let handle = unsafe { db.handle() };
        let shadow = sql_ident::shadow_object(&table, "shadow");
        let shadow_ident = sql_ident::quote(&shadow);
        let vtab = SpikeTab {
            base: ffi::sqlite3_vtab::default(),
            db: handle,
            insert_sql: format!("INSERT INTO {shadow_ident} (ts, value) VALUES (?1, ?2)"),
            shadow,
        };

        // xCreate runs for a brand-new vtab: make the shadow table.
        // xConnect runs when an existing db is reopened: it must already exist.
        if is_create {
            let host = vtab.host()?;
            host.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {} (ts INTEGER, value REAL)",
                sql_ident::quote(&vtab.shadow)
            ))?;
        }

        // This string tells SQLite what columns the vtab exposes. Only the
        // column list matters; the table name "x" is a placeholder.
        Ok((
            Cow::Borrowed(c"CREATE TABLE x(ts INTEGER, value REAL)"),
            vtab,
        ))
    }
}

unsafe impl<'vtab> VTab<'vtab> for SpikeTab {
    type Aux = ();
    type Cursor = SpikeCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        module_name: &[u8],
        database_name: &[u8],
        table_name: &[u8],
        args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        Self::connect_create(db, aux, module_name, database_name, table_name, args, false)
    }

    /// Query planning hook. The spike does no pushdown: every query is a full
    /// scan. (The real extension prunes on name/ts here.)
    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        info.set_estimated_cost(1_000_000.);
        Ok(true)
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        Ok(SpikeCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            db: self.db,
            shadow: self.shadow.clone(),
            rows: Vec::new(),
            pos: 0,
            phantom: PhantomData,
        })
    }
}

impl CreateVTab<'_> for SpikeTab {
    const KIND: VTabKind = VTabKind::Default;

    fn create(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        module_name: &[u8],
        database_name: &[u8],
        table_name: &[u8],
        args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        Self::connect_create(db, aux, module_name, database_name, table_name, args, true)
    }

    /// DROP TABLE on the vtab: remove the shadow table too.
    fn destroy(&self) -> Result<()> {
        self.host()?.execute_batch(&format!(
            "DROP TABLE IF EXISTS {}",
            sql_ident::quote(&self.shadow)
        ))?;
        Ok(())
    }
}

impl UpdateVTab<'_> for SpikeTab {
    /// INSERT: argv[0] is NULL, argv[1] is the requested rowid (usually NULL),
    /// COLUMNS START AT INDEX 2. Returns the new rowid.
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        let ts: i64 = args.get(2)?;
        let value: f64 = args.get(3)?;
        // Per-callback borrow, never the stored handle: the temporary
        // `Connection` is dropped at the end of this call, so it cannot
        // outlive `sqlite3_close` the way a stored `host` field could.
        let host = self.host()?;
        let mut stmt = host.prepare_cached(&self.insert_sql)?;
        stmt.execute((ts, value))?;
        Ok(host.last_insert_rowid())
    }

    /// DELETE: arg is the rowid of the row to remove.
    fn delete(&mut self, arg: ValueRef<'_>) -> Result<()> {
        let rowid = arg.as_i64()?;
        self.host()?.execute(
            &format!(
                "DELETE FROM {} WHERE rowid = ?1",
                sql_ident::quote(&self.shadow)
            ),
            [rowid],
        )?;
        Ok(())
    }

    /// UPDATE: argv[0] old rowid, argv[1] new rowid, columns from index 2.
    fn update(&mut self, args: &Updates<'_>) -> Result<()> {
        let rowid: i64 = args.get(0)?;
        let ts: i64 = args.get(2)?;
        let value: f64 = args.get(3)?;
        self.host()?.execute(
            &format!(
                "UPDATE {} SET ts = ?1, value = ?2 WHERE rowid = ?3",
                sql_ident::quote(&self.shadow)
            ),
            (ts, value, rowid),
        )?;
        Ok(())
    }
}

/// xBegin/xCommit/xRollback - default no-ops for the spike, but wiring the
/// trait now proves the hooks exist (PLAN.md risk R5: buffered-state rollback
/// in the real engine hangs off these).
impl TransactionVTab<'_> for SpikeTab {}

// ---------------------------------------------------------------------------
// The cursor (one per active SELECT scan)
// ---------------------------------------------------------------------------

#[repr(C)]
struct SpikeCursor<'vtab> {
    base: ffi::sqlite3_vtab_cursor,
    db: *mut ffi::sqlite3,
    shadow: String,
    /// Spike strategy: snapshot all rows at filter() time, then iterate.
    /// (rowid, ts, value)
    rows: Vec<(i64, i64, f64)>,
    pos: usize,
    /// Ties the cursor lifetime to its vtab so Rust prevents use-after-free.
    phantom: PhantomData<&'vtab SpikeTab>,
}

unsafe impl VTabCursor for SpikeCursor<'_> {
    /// Called at the start of every scan (re-entrant read: Spike B again).
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &Filters<'_>,
    ) -> Result<()> {
        let host = unsafe { Connection::from_handle(self.db) }?;
        let mut stmt = host.prepare(&format!(
            "SELECT rowid, ts, value FROM {} ORDER BY rowid",
            sql_ident::quote(&self.shadow)
        ))?;
        self.rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<std::result::Result<_, _>>()?;
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        let (_, ts, value) = self.rows[self.pos];
        match i {
            0 => ctx.set_result(&ts),
            _ => ctx.set_result(&value),
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rows[self.pos].0)
    }
}

// ---------------------------------------------------------------------------
// Regression tests (issue #43): the vtab name is attacker-controlled, so
// every shadow-table identifier must be quoted. A name containing `"`
// broke out of the old bare `"..."` interpolation (CREATE failed with a
// syntax error at best, executed attacker SQL at worst).
//
// These run only under the `embedded` feature: the default
// `entrypoints` build routes every SQLite call through the
// loadable-extension API table (initialized by `sqlite3_extension_init`
// at `.load` time), so `open_in_memory` panics there. The `embedded`
// build links the host SQLite directly and can drive modules
// in-process — the same mode `register_telemetry` serves.
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "embedded"))]
mod tests {
    use super::*;

    fn memdb() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        register(&db).unwrap();
        db
    }

    fn table_names(db: &Connection) -> Vec<String> {
        db.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[test]
    fn quoted_table_name_round_trips_through_shadow() {
        let db = memdb();
        // Embedded `"`: pre-fix, CREATE TABLE IF NOT EXISTS "we"ird_shadow"
        // was a syntax error (and a crafted name could inject SQL).
        db.execute_batch(r#"CREATE VIRTUAL TABLE "we""ird" USING timeless_spike;"#)
            .unwrap();
        db.execute(r#"INSERT INTO "we""ird" (ts, value) VALUES (7, 2.5);"#, [])
            .unwrap();
        let (ts, value): (i64, f64) = db
            .query_row(r#"SELECT ts, value FROM "we""ird""#, [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!((ts, value), (7, 2.5));
        // Exactly the vtab plus one shadow table with the LITERAL name —
        // nothing escaped the identifier.
        assert_eq!(
            table_names(&db),
            vec![r#"we"ird"#.to_string(), r#"we"ird_shadow"#.to_string()]
        );
    }

    #[test]
    fn quoted_name_supports_update_delete_and_drop() {
        let db = memdb();
        db.execute_batch(r#"CREATE VIRTUAL TABLE "odd""table" USING timeless_spike;"#)
            .unwrap();
        db.execute(
            r#"INSERT INTO "odd""table" (ts, value) VALUES (1, 1.0);"#,
            [],
        )
        .unwrap();
        // Exercises the UPDATE site.
        db.execute(r#"UPDATE "odd""table" SET value = 3.0 WHERE ts = 1;"#, [])
            .unwrap();
        let value: f64 = db
            .query_row(r#"SELECT value FROM "odd""table""#, [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, 3.0);
        // Exercises the DELETE site.
        db.execute(r#"DELETE FROM "odd""table" WHERE ts = 1;"#, [])
            .unwrap();
        let count: i64 = db
            .query_row(r#"SELECT COUNT(*) FROM "odd""table""#, [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
        // Exercises the DROP (xDestroy) site: the shadow goes with the vtab.
        db.execute_batch(r#"DROP TABLE "odd""table";"#).unwrap();
        assert!(table_names(&db).is_empty());
    }
}
