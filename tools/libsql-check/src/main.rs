//! Load timeless-ext into a *libSQL* (not rusqlite) local connection and
//! exercise both the ordinary relational and packed query surfaces.

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

    conn.execute("CREATE VIRTUAL TABLE metrics USING timeless_metrics", ())
        .await?;
    conn.execute(
        "INSERT INTO metrics(name,labels,ts,value) VALUES
         ('cpu','{\"host\":\"a\"}',10,1.0),
         ('cpu','{\"host\":\"a\"}',20,3.0),
         ('cpu','{\"host\":\"b\"}',10,9.0)",
        (),
    )
    .await?;
    conn.execute("INSERT INTO metrics(metrics) VALUES ('flush')", ())
        .await?;

    let mut rows = conn
        .query(
            "SELECT series_id FROM timeless_series(
             'metrics','cpu','{\"host\":\"a\"}')",
            (),
        )
        .await?;
    let series_id: i64 = rows.next().await?.expect("series catalog row").get(0)?;

    let mut rows = conn
        .query(
            "SELECT value FROM timeless_aggregate(
             'metrics','cpu',NULL,0,30,'avg') WHERE series_id = ?",
            libsql::params![series_id],
        )
        .await?;
    let average: f64 = rows.next().await?.expect("selected aggregate row").get(0)?;
    assert_eq!(average, 2.0);
    assert!(rows.next().await?.is_none());

    let mut rows = conn
        .query(
            "SELECT frame FROM timeless_aggregate_frame(
             'metrics','cpu',NULL,0,30,'avg')",
            (),
        )
        .await?;
    let aggregate_frame: Vec<u8> = rows.next().await?.expect("aggregate frame row").get(0)?;
    assert_eq!(&aggregate_frame[..4], b"TAF1");

    let mut rows = conn
        .query(
            "SELECT frame FROM timeless_latest_frame('metrics','cpu',NULL,0,30)",
            (),
        )
        .await?;
    let latest_frame: Vec<u8> = rows.next().await?.expect("latest frame row").get(0)?;
    assert_eq!(&latest_frame[..4], b"TLF1");

    let mut rows = conn
        .query(
            "SELECT count(*) FROM timeless_series('metrics','cpu',NULL) AS s
             JOIN timeless_latest('metrics','cpu',NULL,0,30) AS q
               ON q.series_id=s.series_id",
            (),
        )
        .await?;
    let joined: i64 = rows.next().await?.expect("catalog join row").get(0)?;
    assert_eq!(joined, 2);

    let _ = std::fs::remove_file(db_path);
    println!("libsql relational + frame parity check PASSED");
    Ok(())
}
