//! Minimal in-process telemetry store using the public Rust embedding API.
//!
//! Run from the repository root with:
//! `cargo run -p timeless-ext --example embedded`

use rusqlite::{params, Connection};
use serde_json::Value;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use Connection::open("telemetry.db") for a durable application database.
    let connection = Connection::open_in_memory()?;
    timeless_ext::register_telemetry(&connection)?;

    let capabilities: String =
        connection.query_row("SELECT timeless_capabilities()", [], |row| row.get(0))?;
    let capabilities: Value = serde_json::from_str(&capabilities)?;
    assert_eq!(capabilities["data_abi"], 1);
    assert_eq!(capabilities["sql_surface_version"], 1);
    assert_eq!(
        capabilities["sql_surfaces"]["storage_modules"],
        serde_json::json!(["timeless_metrics", "timeless_logs", "timeless_traces"])
    );

    // The embedding entry point installs production telemetry only. The
    // compatibility/reference spike module is exclusive to the loadable .so.
    let spike_modules: i64 = connection.query_row(
        "SELECT count(*) FROM pragma_module_list WHERE name='timeless_spike'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(spike_modules, 0);

    connection.execute_batch(
        "CREATE VIRTUAL TABLE metrics USING timeless_metrics;
         CREATE VIRTUAL TABLE logs USING timeless_logs(
           index_keys='service', timestamp_unit='us'
         );
         CREATE VIRTUAL TABLE traces USING timeless_traces;",
    )?;

    connection.execute(
        "INSERT INTO metrics(name,ts,value,labels) VALUES(?1,?2,?3,?4)",
        params![
            "cpu_usage",
            1_753_000_000_i64,
            42.5_f64,
            r#"{"host":"edge-1"}"#
        ],
    )?;
    connection.execute(
        "INSERT INTO logs(ts,level,message,metadata) VALUES(?1,?2,?3,?4)",
        params![
            1_753_000_000_000_001_i64,
            "alert",
            "temperature limit",
            r#"{"attempt":2,"ok":false,"service":"sensor","tags":["edge","hot"]}"#
        ],
    )?;
    connection.execute(
        "INSERT INTO traces(
           trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,
           duration_ns,attributes,status_description,events,resource,
           instrumentation_scope
         ) VALUES(?1,?2,NULL,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![
            vec![0x11_u8; 16],
            vec![0x22_u8; 8],
            "sensor.read",
            "sensor",
            "client",
            "error",
            1_753_000_000_000_000_000_i64,
            25_000_i64,
            r#"{"sensor.id":"a-7","temperature":91.5}"#,
            "threshold exceeded",
            r#"[{"name":"alarm","time_unix_nano":1753000000000001000,"attributes":{"limit":90}}]"#,
            r#"{"service.name":"sensor","zone":"west"}"#,
            r#"{"name":"edge-sdk","version":"1.2.3"}"#
        ],
    )?;

    for table in ["metrics", "logs", "traces"] {
        connection.execute(&format!("INSERT INTO {table}({table}) VALUES('flush')"), [])?;
    }

    let metric: f64 = connection.query_row(
        "SELECT value FROM timeless_latest(
           'metrics','cpu_usage','{\"host\":\"edge-1\"}',0,2000000000
         )",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(metric, 42.5);

    let (severity, metadata): (String, String) = connection.query_row(
        "SELECT level,metadata FROM logs WHERE service='sensor'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(severity, "alert");
    assert_eq!(
        serde_json::from_str::<Value>(&metadata)?,
        serde_json::json!({
            "attempt": 2,
            "ok": false,
            "service": "sensor",
            "tags": ["edge", "hot"]
        })
    );

    let (status_description, events): (String, String) = connection.query_row(
        "SELECT status_description,events FROM traces WHERE trace_id=?1",
        params![vec![0x11_u8; 16]],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(status_description, "threshold exceeded");
    assert_eq!(serde_json::from_str::<Value>(&events)?[0]["name"], "alarm");

    println!("embedded metrics, logs, and rich traces: ok");
    Ok(())
}
