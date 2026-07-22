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
    Context, CreateVTab, Filters, IndexInfo, Inserts, Module, TransactionVTab, UpdateVTab,
    Updates, VTab, VTabConnection, VTabCursor, VTabKind,
};
use rusqlite::{Connection, Result};

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
    /// We keep it so callbacks can run SQL against shadow tables.
    db: *mut ffi::sqlite3,
    /// Name of our shadow table, quoted-safe ("<vtab_name>_shadow").
    shadow: String,
    /// Long-lived borrow of the host connection. Statements prepared through
    /// `prepare_cached` on this survive across xUpdate calls - the difference
    /// between parsing SQL once vs. once per row (measured ~4x on inserts).
    host: Connection,
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
        let shadow = format!("{table}_shadow");
        let vtab = SpikeTab {
            base: ffi::sqlite3_vtab::default(),
            db: handle,
            insert_sql: format!("INSERT INTO \"{shadow}\" (ts, value) VALUES (?1, ?2)"),
            shadow,
            host: unsafe { Connection::from_handle(handle) }?,
        };

        // xCreate runs for a brand-new vtab: make the shadow table.
        // xConnect runs when an existing db is reopened: it must already exist.
        if is_create {
            let host = vtab.host()?;
            host.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS \"{}\" (ts INTEGER, value REAL)",
                vtab.shadow
            ))?;
        }

        // This string tells SQLite what columns the vtab exposes. Only the
        // column list matters; the table name "x" is a placeholder.
        Ok((Cow::Borrowed(c"CREATE TABLE x(ts INTEGER, value REAL)"), vtab))
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
        self.host()?
            .execute_batch(&format!("DROP TABLE IF EXISTS \"{}\"", self.shadow))?;
        Ok(())
    }
}

impl UpdateVTab<'_> for SpikeTab {
    /// INSERT: argv[0] is NULL, argv[1] is the requested rowid (usually NULL),
    /// COLUMNS START AT INDEX 2. Returns the new rowid.
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        let ts: i64 = args.get(2)?;
        let value: f64 = args.get(3)?;
        let mut stmt = self.host.prepare_cached(&self.insert_sql)?;
        stmt.execute((ts, value))?;
        Ok(self.host.last_insert_rowid())
    }

    /// DELETE: arg is the rowid of the row to remove.
    fn delete(&mut self, arg: ValueRef<'_>) -> Result<()> {
        let rowid = arg.as_i64()?;
        self.host()?.execute(
            &format!("DELETE FROM \"{}\" WHERE rowid = ?1", self.shadow),
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
                "UPDATE \"{}\" SET ts = ?1, value = ?2 WHERE rowid = ?3",
                self.shadow
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
    fn filter(&mut self, _idx_num: c_int, _idx_str: Option<&str>, _args: &Filters<'_>) -> Result<()> {
        let host = unsafe { Connection::from_handle(self.db) }?;
        let mut stmt = host.prepare(&format!(
            "SELECT rowid, ts, value FROM \"{}\" ORDER BY rowid",
            self.shadow
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
