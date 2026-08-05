use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

use super::open;

struct RichEntry {
    timestamp: i64,
    level: String,
    message: String,
    metadata: Value,
}

fn framed(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

fn rich_batch(entries: &[RichEntry]) -> Result<Vec<u8>> {
    let mut out = vec![2, 0, 0, 0];
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        out.extend_from_slice(&entry.timestamp.to_le_bytes());
    }
    for entry in entries {
        framed(&mut out, entry.level.as_bytes());
    }
    for entry in entries {
        framed(&mut out, entry.message.as_bytes());
    }
    for entry in entries {
        framed(&mut out, serde_json::to_string(&entry.metadata)?.as_bytes());
    }
    Ok(out)
}

fn flat_batch(timestamp: i64) -> Vec<u8> {
    let mut out = vec![1, 0, 0, 0];
    out.extend_from_slice(&1_u32.to_le_bytes());
    out.extend_from_slice(&timestamp.to_le_bytes());
    out.push(1);
    framed(&mut out, b"legacy-info");
    framed(&mut out, br#"{"service":"legacy","status":"ok"}"#);
    out
}

fn capability(connection: &Connection) -> Result<Value> {
    let encoded: String =
        connection.query_row("SELECT timeless_capabilities()", [], |row| row.get(0))?;
    Ok(serde_json::from_str(&encoded)?)
}

pub(super) fn run(extension: &Path, database: &Path) -> Result<()> {
    let connection = open(extension, database)?;
    let capabilities = capability(&connection)?;
    ensure!(capabilities["data_abi"] == 1);
    ensure!(capabilities["signals"]["logs"]["batches"]
        .as_array()
        .is_some_and(|items| items.iter().any(|item| item == "rich-v1")));
    ensure!(capabilities["signals"]["logs"]["authoritative_batch_entries"] == 8192);

    let error = connection
        .execute(
            "CREATE VIRTUAL TABLE bad USING timeless_logs(timestamp_unit='seconds')",
            [],
        )
        .expect_err("unknown logs timestamp unit was accepted");
    ensure!(error.to_string().contains("expected 'ms' or 'us'"));
    connection.execute(
        "CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service,host',timestamp_unit='us')",
        [],
    )?;
    let unit: String = connection.query_row(
        "SELECT CAST(v AS TEXT) FROM logs_meta WHERE k='timestamp_unit'",
        [],
        |row| row.get(0),
    )?;
    ensure!(unit == "us");

    let timestamp = 1_700_000_000_123_456_i64;
    let typed = json!({
        "array": [1, true, null, {"nested": 2.5}],
        "bool": false,
        "count": 9_007_199_254_740_991_i64,
        "null": null,
        "service": "api"
    });
    let mut first_metadata = typed.as_object().context("typed metadata object")?.clone();
    first_metadata.insert("host".into(), Value::String("web-c".into()));
    let entries = vec![
        RichEntry {
            timestamp,
            level: "notice".into(),
            message: "b-message".into(),
            metadata: Value::Object(first_metadata.clone()),
        },
        RichEntry {
            timestamp,
            level: "critical".into(),
            message: "a-message".into(),
            metadata: json!({"service":"api","host":"web-a","code":503}),
        },
        RichEntry {
            timestamp: timestamp + 1,
            level: "emergency".into(),
            message: "c-message".into(),
            metadata: json!({"service":"ops","host":"web-b","fatal":true}),
        },
    ];
    connection.execute(
        "INSERT INTO logs(logs) VALUES (?1)",
        params![rich_batch(&entries)?],
    )?;
    let count: i64 = connection.query_row("SELECT COUNT(*) FROM logs", [], |row| row.get(0))?;
    ensure!(count == 3);
    connection.execute(
        "INSERT INTO logs(logs) VALUES (?1)",
        params![flat_batch(timestamp + 2)],
    )?;
    let count: i64 = connection.query_row("SELECT COUNT(*) FROM logs", [], |row| row.get(0))?;
    ensure!(count == 4);
    connection.execute("INSERT INTO logs(logs) VALUES ('flush')", [])?;
    connection.execute("INSERT INTO logs(logs) VALUES ('optimize')", [])?;
    drop(connection);

    let connection = open(extension, database)?;
    let rows = connection
        .prepare("SELECT ts,level,message,metadata FROM logs ORDER BY ts ASC LIMIT 10")?
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        rows.iter().map(|row| row.2.as_str()).collect::<Vec<_>>()
            == ["a-message", "b-message", "c-message", "legacy-info"]
    );
    ensure!(rows[0].1 == "critical" && rows[1].1 == "notice");
    ensure!(rows[2].1 == "emergency" && rows[3].1 == "info");
    ensure!(rows[1].0 == timestamp);
    let decoded: Value = serde_json::from_str(&rows[1].3)?;
    ensure!(decoded == Value::Object(first_metadata.clone()));
    ensure!(rows[1].3 == serde_json::to_string(&Value::Object(first_metadata))?);
    for (level, expected) in [("notice", 1_i64), ("critical", 1), ("error", 0)] {
        let actual: i64 = connection.query_row(
            "SELECT COUNT(*) FROM logs WHERE level=?1",
            params![level],
            |row| row.get(0),
        )?;
        ensure!(actual == expected, "level {level}: {actual} != {expected}");
    }

    let groups = connection
        .prepare("SELECT group_key,n FROM timeless_log_buckets('logs','level',NULL,?1,?2,10)")?
        .query_map(params![timestamp, timestamp + 9], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<BTreeSet<_>>>()?;
    ensure!(
        groups
            == BTreeSet::from([
                ("notice".into(), 1),
                ("critical".into(), 1),
                ("emergency".into(), 1),
                ("info".into(), 1),
            ])
    );
    let values = connection
        .prepare(
            "SELECT value FROM timeless_log_values('logs','host','{\"service\":\"api\"}',NULL,?1,?2,10)",
        )?
        .query_map(params![timestamp, timestamp], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(values == ["web-a", "web-c"]);
    let values = connection
        .prepare("SELECT value FROM timeless_log_values('logs','host',NULL,NULL,NULL,NULL,2)")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(values == ["web-a", "web-b"]);
    let mut statement = connection.prepare(
        "SELECT value FROM timeless_log_values('logs','host',NULL,NULL,NULL,NULL,100001)",
    )?;
    let mut rows = statement.query([])?;
    let error = match rows.next() {
        Ok(_) => anyhow::bail!("unbounded log field-values limit was accepted"),
        Err(error) => error,
    };
    ensure!(error
        .to_string()
        .contains("limit must be between 0 and 100000"));
    let messages = connection
        .prepare("SELECT message FROM logs ORDER BY ts ASC LIMIT 2")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(messages == ["a-message", "b-message"]);
    println!("PASS: capability handshake and rich logs v0/v1 compatibility");
    Ok(())
}
