//! Session 1 gate check: load timeless-ext into a *libsql* (not rusqlite)
//! local connection and run the same vtab lifecycle the sqlite3 CLI test ran.

use std::error::Error;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let ext = std::env::args()
        .nth(1)
        .expect("usage: libsql-check <path-to-libtimeless_ext.so>");
    let db_path = "/tmp/libsql_check.db";
    let _ = std::fs::remove_file(db_path);

    let db = libsql::Builder::new_local(db_path).build().await?;
    let conn = db.connect()?;

    conn.load_extension_enable()?;
    conn.load_extension(&ext, None)?;
    println!("extension loaded OK via libsql");

    conn.execute("CREATE VIRTUAL TABLE spike USING timeless_spike", ())
        .await?;
    conn.execute(
        "INSERT INTO spike(ts, value) VALUES (100, 1.5), (200, 2.5)",
        (),
    )
    .await?;
    conn.execute("UPDATE spike SET value = 9.9 WHERE ts = 100", ())
        .await?;
    conn.execute("DELETE FROM spike WHERE ts = 200", ()).await?;

    let mut rows = conn.query("SELECT ts, value FROM spike", ()).await?;
    while let Some(row) = rows.next().await? {
        let ts: i64 = row.get(0)?;
        let value: f64 = row.get(1)?;
        println!("row: ts={ts} value={value}");
    }

    let _ = std::fs::remove_file(db_path);
    println!("libsql parity check PASSED");
    Ok(())
}
