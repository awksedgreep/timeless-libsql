//! Load the release extension into local libSQL connections and exercise only
//! the public production storage and query surfaces.

use std::error::Error;
use std::io;
use std::path::Path;

use libsql::Connection;
use serde_json::Value;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn missing(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, what)
}

async fn load_extension(connection: &Connection, extension: &Path) -> Result<()> {
    connection.load_extension_enable()?;
    connection.load_extension(extension, None)?;
    Ok(())
}

async fn scalar_i64(connection: &Connection, sql: &str) -> Result<i64> {
    let mut rows = connection.query(sql, ()).await?;
    Ok(rows
        .next()
        .await?
        .ok_or_else(|| missing("expected scalar row"))?
        .get(0)?)
}

async fn verify_capabilities(connection: &Connection) -> Result<()> {
    let mut rows = connection
        .query("SELECT timeless_capabilities()", ())
        .await?;
    let document: String = rows
        .next()
        .await?
        .ok_or_else(|| missing("timeless_capabilities() row"))?
        .get(0)?;
    let document: Value = serde_json::from_str(&document)?;
    assert_eq!(document["data_abi"], 1);
    assert_eq!(document["sql_surface_version"], 1);
    assert_eq!(
        document["sql_surfaces"]["storage_modules"],
        serde_json::json!(["timeless_metrics", "timeless_logs", "timeless_traces"])
    );
    assert_eq!(document["signals"]["logs"]["typed_metadata"], true);
    assert_eq!(document["signals"]["traces"]["rich_span_fidelity"], true);
    assert!(document["sql_surfaces"]["storage_modules"]
        .as_array()
        .expect("storage_modules is an array")
        .iter()
        .all(|name| name != "timeless_spike"));
    Ok(())
}

async fn create_and_flush(connection: &Connection) -> Result<()> {
    connection
        .execute("CREATE VIRTUAL TABLE metrics USING timeless_metrics", ())
        .await?;
    connection
        .execute(
            "CREATE VIRTUAL TABLE logs USING timeless_logs(
               index_keys='service', timestamp_unit='us'
             )",
            (),
        )
        .await?;
    connection
        .execute("CREATE VIRTUAL TABLE traces USING timeless_traces", ())
        .await?;

    connection
        .execute(
            "INSERT INTO metrics(name,labels,ts,value) VALUES(?1,?2,?3,?4)",
            libsql::params!["cpu", r#"{"host":"a"}"#, 10_i64, 1.0_f64],
        )
        .await?;
    connection
        .execute(
            "INSERT INTO metrics(name,labels,ts,value) VALUES(?1,?2,?3,?4)",
            libsql::params!["cpu", r#"{"host":"a"}"#, 20_i64, 3.0_f64],
        )
        .await?;
    connection
        .execute(
            "INSERT INTO metrics(name,labels,ts,value) VALUES(?1,?2,?3,?4)",
            libsql::params!["cpu", r#"{"host":"b"}"#, 10_i64, 9.0_f64],
        )
        .await?;
    connection
        .execute(
            "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
            libsql::params![
                1_000_001_i64,
                "emergency",
                "rich log",
                r#"{"attempt":2,"nested":{"ok":false},"service":"api","tags":["a","b"]}"#
            ],
        )
        .await?;
    connection
        .execute(
            "INSERT INTO traces(
               trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,
               duration_ns,attributes,status_description,events,resource,
               instrumentation_scope
             ) VALUES(?1,?2,NULL,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            libsql::params![
                vec![0x11_u8; 16],
                vec![0x22_u8; 8],
                "checkout",
                "api",
                "server",
                "error",
                1_000_000_001_i64,
                25_000_i64,
                r#"{"http":{"status":503},"retry":true}"#,
                "upstream unavailable",
                r#"[{"name":"retry","time_unix_nano":1000000010,"attributes":{"count":1}}]"#,
                r#"{"service.name":"api","zone":"west"}"#,
                r#"{"name":"otel","version":"1.0"}"#
            ],
        )
        .await?;

    for table in ["metrics", "logs", "traces"] {
        connection
            .execute(&format!("INSERT INTO {table}({table}) VALUES('flush')"), ())
            .await?;
    }
    Ok(())
}

async fn verify_production_surfaces(connection: &Connection) -> Result<()> {
    assert_eq!(
        scalar_i64(connection, "SELECT count(*) FROM metrics").await?,
        3
    );
    assert_eq!(
        scalar_i64(connection, "SELECT count(*) FROM logs").await?,
        1
    );
    assert_eq!(
        scalar_i64(connection, "SELECT count(*) FROM traces").await?,
        1
    );

    let mut rows = connection
        .query(
            "SELECT value FROM timeless_aggregate(
               'metrics','cpu','{\"host\":\"a\"}',0,30,'avg'
             )",
            (),
        )
        .await?;
    let average: f64 = rows
        .next()
        .await?
        .ok_or_else(|| missing("selected aggregate row"))?
        .get(0)?;
    assert_eq!(average, 2.0);
    assert!(rows.next().await?.is_none());
    drop(rows);

    let mut rows = connection
        .query(
            "SELECT frame FROM timeless_aggregate_frame(
               'metrics','cpu',NULL,0,30,'avg'
             )",
            (),
        )
        .await?;
    let aggregate_frame: Vec<u8> = rows
        .next()
        .await?
        .ok_or_else(|| missing("aggregate frame row"))?
        .get(0)?;
    assert_eq!(&aggregate_frame[..4], b"TAF1");
    drop(rows);

    let mut rows = connection
        .query("SELECT level,metadata FROM logs WHERE service='api'", ())
        .await?;
    let row = rows.next().await?.ok_or_else(|| missing("rich log row"))?;
    let severity: String = row.get(0)?;
    let metadata: String = row.get(1)?;
    assert_eq!(severity, "emergency");
    assert_eq!(
        serde_json::from_str::<Value>(&metadata)?,
        serde_json::json!({
            "attempt": 2,
            "nested": {"ok": false},
            "service": "api",
            "tags": ["a", "b"]
        })
    );
    assert!(rows.next().await?.is_none());
    drop(rows);

    let mut rows = connection
        .query(
            "SELECT status_description,events,resource,instrumentation_scope
             FROM traces WHERE trace_id=?1",
            libsql::params![vec![0x11_u8; 16]],
        )
        .await?;
    let row = rows.next().await?.ok_or_else(|| missing("rich span row"))?;
    let description: String = row.get(0)?;
    let events: String = row.get(1)?;
    let resource: String = row.get(2)?;
    let scope: String = row.get(3)?;
    assert_eq!(description, "upstream unavailable");
    assert_eq!(serde_json::from_str::<Value>(&events)?[0]["name"], "retry");
    assert_eq!(serde_json::from_str::<Value>(&resource)?["zone"], "west");
    assert_eq!(serde_json::from_str::<Value>(&scope)?["name"], "otel");
    assert!(rows.next().await?.is_none());
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let extension = std::env::args()
        .nth(1)
        .ok_or_else(|| missing("usage: libsql-check <path-to-libtimeless_ext.so>"))?;
    let extension = std::fs::canonicalize(extension)?;
    let temporary = tempfile::tempdir()?;
    let database_path = temporary.path().join("telemetry.db");

    let database = libsql::Builder::new_local(&database_path).build().await?;
    let writer = database.connect()?;
    load_extension(&writer, &extension).await?;
    verify_capabilities(&writer).await?;
    create_and_flush(&writer).await?;

    // A separate connection must load the extension independently, then see
    // the same flushed production data through the public virtual tables.
    let reader = database.connect()?;
    load_extension(&reader, &extension).await?;
    verify_capabilities(&reader).await?;
    verify_production_surfaces(&reader).await?;
    drop(reader);
    drop(writer);
    drop(database);

    // Reopen the durable file in a new libSQL database handle and prove the
    // rich values and packed metrics query still survive process-style churn.
    let reopened = libsql::Builder::new_local(&database_path).build().await?;
    let connection = reopened.connect()?;
    load_extension(&connection, &extension).await?;
    verify_capabilities(&connection).await?;
    verify_production_surfaces(&connection).await?;

    println!("libSQL production metrics, rich logs, rich traces, and reopen: ok");
    Ok(())
}
