use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection};

use super::{blobs, open, CliSection};

fn values(connection: &Connection, sql: &str, parameters: &[Value]) -> Result<Vec<Vec<Value>>> {
    let mut statement = connection.prepare(sql)?;
    let columns = statement.column_count();
    let rows = statement
        .query_map(params_from_iter(parameters), |row| {
            (0..columns)
                .map(|column| row.get::<_, Value>(column))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn scalar_i64(connection: &Connection, sql: &str) -> Result<i64> {
    Ok(connection.query_row(sql, [], |row| row.get(0))?)
}

fn shared_engine(extension: &Path, database: &Path) -> Result<()> {
    let a = open(extension, database)?;
    let b = open(extension, database)?;
    a.execute("CREATE VIRTUAL TABLE m USING timeless_metrics", [])?;
    ensure!(scalar_i64(&b, "SELECT COUNT(*) FROM m")? == 0);
    a.execute("INSERT INTO m(name,ts,value) VALUES('cpu',100,1.5)", [])?;
    a.execute("INSERT INTO m(m) VALUES('flush')", [])?;
    ensure!(
        values(&b, "SELECT name,ts,value FROM m", &[])?
            == [vec![
                Value::Text("cpu".into()),
                Value::Integer(100),
                Value::Real(1.5)
            ]]
    );
    let aggregate: f64 = b.query_row(
        "SELECT value FROM timeless_aggregate('m','cpu',NULL,0,500,'sum')",
        [],
        |row| row.get(0),
    )?;
    ensure!(aggregate == 1.5);
    a.execute("INSERT INTO m(name,ts,value) VALUES('cpu',200,2.5)", [])?;
    ensure!(scalar_i64(&b, "SELECT COUNT(*) FROM m WHERE name='cpu'")? == 2);
    let aggregate: f64 = b.query_row(
        "SELECT value FROM timeless_aggregate('m','cpu',NULL,0,500,'sum')",
        [],
        |row| row.get(0),
    )?;
    ensure!(aggregate == 4.0);
    b.busy_timeout(Duration::from_secs(2))?;
    a.execute("BEGIN", [])?;
    a.execute("INSERT INTO m(name,ts,value) VALUES('cpu',300,3.5)", [])?;
    let started = Instant::now();
    let error = b
        .execute("INSERT INTO m(name,ts,value) VALUES('mem',300,9.0)", [])
        .expect_err("second writer unexpectedly succeeded");
    let elapsed = started.elapsed();
    ensure!(
        error.to_string().contains("locked"),
        "unexpected lock error: {error}"
    );
    ensure!(elapsed >= Duration::from_millis(1500) && elapsed <= Duration::from_secs(20));
    b.busy_timeout(Duration::from_secs(30))?;
    a.execute("COMMIT", [])?;
    b.execute("INSERT INTO m(name,ts,value) VALUES('mem',300,9.0)", [])?;
    ensure!(scalar_i64(&a, "SELECT COUNT(*) FROM m")? == 4);
    a.execute("DROP TABLE m", [])?;
    a.execute("CREATE VIRTUAL TABLE m USING timeless_metrics", [])?;
    b.execute("INSERT INTO m(name,ts,value) VALUES('disk',400,7.0)", [])?;
    a.execute("INSERT INTO m(m) VALUES('flush')", [])?;
    ensure!(
        values(&a, "SELECT name,ts,value FROM m", &[])?
            == [vec![
                Value::Text("disk".into()),
                Value::Integer(400),
                Value::Real(7.0)
            ]]
    );
    ensure!(scalar_i64(&b, "SELECT COUNT(*) FROM m")? == 1);
    println!("PASS: shared engine publication, bounded locking, and recreate");
    Ok(())
}

fn read_u32(blob: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        blob.get(offset..offset + 4)
            .context("truncated u32")?
            .try_into()?,
    ))
}

fn read_i64s(blob: &[u8], offset: usize, count: usize) -> Result<Vec<i64>> {
    blob.get(offset..offset + count * 8)
        .context("truncated i64 column")?
        .chunks_exact(8)
        .map(|word| Ok(i64::from_le_bytes(word.try_into()?)))
        .collect()
}

fn read_u64s(blob: &[u8], offset: usize, count: usize) -> Result<Vec<u64>> {
    blob.get(offset..offset + count * 8)
        .context("truncated u64 column")?
        .chunks_exact(8)
        .map(|word| Ok(u64::from_le_bytes(word.try_into()?)))
        .collect()
}

fn packed_rollup(extension: &Path, database: &Path) -> Result<()> {
    let connection = open(extension, database)?;
    let (series_id, labels, blob): (i64, String, Vec<u8>) = connection.query_row(
        "SELECT series_id,labels,buckets FROM timeless_rollup_batches('m','cpu',NULL,60,0,99999)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    ensure!(blob.starts_with(b"TRB1"));
    let count = read_u32(&blob, 4)? as usize;
    ensure!(blob.len() == 8 + count * 64);
    let timestamps = read_i64s(&blob, 8, count)?;
    let counts = read_u64s(&blob, 8 + count * 8, count)?;
    let mut floating = BTreeMap::new();
    for (index, aggregate) in ["avg", "sum", "min", "max", "last"].into_iter().enumerate() {
        let column = if index < 4 { 2 + index } else { 7 };
        floating.insert(aggregate, read_u64s(&blob, 8 + column * count * 8, count)?);
    }
    let last_timestamps = read_i64s(&blob, 8 + 6 * count * 8, count)?;
    for (aggregate, words) in floating {
        let rows = connection
            .prepare(
                "SELECT ts,value FROM timeless_rollup('m','cpu',NULL,60,0,99999,?1) ORDER BY ts",
            )?
            .query_map(params![aggregate], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ensure!(rows.iter().map(|row| row.0).collect::<Vec<_>>() == timestamps);
        ensure!(rows.iter().map(|row| row.1.to_bits()).collect::<Vec<_>>() == words);
    }
    let count_rows = connection
        .prepare(
            "SELECT ts,value FROM timeless_rollup('m','cpu',NULL,60,0,99999,'count') ORDER BY ts",
        )?
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)? as u64))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(count_rows.iter().map(|row| row.0).collect::<Vec<_>>() == timestamps);
    ensure!(count_rows.iter().map(|row| row.1).collect::<Vec<_>>() == counts);
    ensure!(timestamps
        .iter()
        .zip(last_timestamps)
        .all(|(bucket, sample)| *bucket <= sample && sample < *bucket + 60));
    ensure!(series_id == 1 && labels == r#"{"host":"a"}"# && count == 16);
    println!("PASS: packed rollup blob matches all six row aggregates");
    Ok(())
}

fn latest_publication(extension: &Path, database: &Path) -> Result<()> {
    let first = open(extension, database)?;
    let second = open(extension, database)?;
    first.execute("CREATE VIRTUAL TABLE m USING timeless_metrics", [])?;
    first.execute(
        "INSERT INTO m(name,labels,ts,value) VALUES('cpu','{\"host\":\"a\"}',10,1.0)",
        [],
    )?;
    let row: (String, i64, f64) = second.query_row(
        "SELECT labels,ts,value FROM timeless_latest('m','cpu',NULL,0,100)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    ensure!(row == (r#"{"host":"a"}"#.into(), 10, 1.0));
    println!("PASS: latest publishes committed buffered writes across live connections");
    Ok(())
}

fn metric_rows(connection: &Connection) -> Result<Vec<(String, i64, f64)>> {
    Ok(connection
        .prepare("SELECT name,ts,value FROM m ORDER BY name,ts,value")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?)
}

fn catalog_publication(extension: &Path, database: &Path) -> Result<()> {
    let a = open(extension, database)?;
    let b = open(extension, database)?;
    a.execute("CREATE VIRTUAL TABLE m USING timeless_metrics", [])?;
    ensure!(scalar_i64(&b, "SELECT COUNT(*) FROM timeless_series('m')")? == 0);
    a.execute_batch(
        "INSERT INTO m(name,ts,value) VALUES('cpu',10,1.0);
         INSERT INTO m(name,ts,value) VALUES('cpu',20,2.0);
         INSERT INTO m(m) VALUES('flush');",
    )?;
    let cpu_two = vec![("cpu".into(), 10, 1.0), ("cpu".into(), 20, 2.0)];
    ensure!(metric_rows(&b)? == cpu_two);
    a.execute_batch(
        "BEGIN;
         INSERT INTO m(name,ts,value) VALUES('cpu',30,99.0);
         INSERT INTO m(m) VALUES('flush');
         INSERT INTO m(m) VALUES('prune:25');
         ROLLBACK;",
    )?;
    ensure!(metric_rows(&b)? == cpu_two);
    a.execute_batch(
        "INSERT INTO m(name,ts,value) VALUES('cpu',30,3.0);
         INSERT INTO m(m) VALUES('flush');
         INSERT INTO m(m) VALUES('compact');",
    )?;
    let cpu_three = vec![
        ("cpu".into(), 10, 1.0),
        ("cpu".into(), 20, 2.0),
        ("cpu".into(), 30, 3.0),
    ];
    ensure!(metric_rows(&b)? == cpu_three);
    ensure!(scalar_i64(&b, "SELECT COUNT(*) FROM m_chunks")? == 1);
    a.execute_batch(
        "INSERT INTO m(name,ts,value) VALUES('old',1,8.0);
         INSERT INTO m(m) VALUES('flush');
         INSERT INTO m(m) VALUES('prune:5');",
    )?;
    ensure!(metric_rows(&b)? == cpu_three);

    let mut child = Command::new("sqlite3")
        .arg(database)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let sql = format!(
        ".load {}\nINSERT INTO m(name,ts,value) VALUES('mem',40,4.0);\nINSERT INTO m(m) VALUES('flush');\n",
        extension.display()
    );
    child
        .stdin
        .take()
        .context("sqlite3 stdin")?
        .write_all(sql.as_bytes())?;
    let output = child.wait_with_output()?;
    ensure!(
        output.status.success(),
        "external sqlite3 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected = vec![
        ("cpu".into(), 10, 1.0),
        ("cpu".into(), 20, 2.0),
        ("cpu".into(), 30, 3.0),
        ("mem".into(), 40, 4.0),
    ];
    ensure!(metric_rows(&b)? == expected);
    drop(a);
    drop(b);
    let reopened = open(extension, database)?;
    ensure!(metric_rows(&reopened)? == expected);
    ensure!(scalar_i64(&reopened, "SELECT COUNT(*) FROM m_chunks")? == 2);
    println!("PASS: catalog commit, rollback, compact, prune, external invalidation, and reopen");
    Ok(())
}

fn strings(connection: &Connection, sql: &str) -> Result<Vec<String>> {
    Ok(connection
        .prepare(sql)?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?)
}

fn matcher_discovery(extension: &Path, database: &Path) -> Result<()> {
    let a = open(extension, database)?;
    let b = open(extension, database)?;
    a.execute("CREATE VIRTUAL TABLE m USING timeless_metrics", [])?;
    for (labels, value) in [
        (r#"{"code":"a","env":"prod","host":"web-1"}"#, 1.0),
        (r#"{"code":"é","env":"dev","host":"web-2"}"#, 2.0),
        (r#"{"host":"db-1"}"#, 3.0),
        (r#"{"env":"","host":"empty"}"#, 4.0),
    ] {
        a.execute(
            "INSERT INTO m(name,labels,ts,value) VALUES('cpu',?1,10,?2)",
            params![labels, value],
        )?;
    }
    ensure!(
        strings(&b, "SELECT labels FROM timeless_series('m','cpu','{\"host\":{\"re\":\"web-.*\"}}') ORDER BY labels")?
            == [r#"{"code":"a","env":"prod","host":"web-1"}"#, r#"{"code":"é","env":"dev","host":"web-2"}"#]
    );
    a.execute("INSERT INTO m(m) VALUES('flush')", [])?;
    ensure!(
        strings(&b, "SELECT labels FROM timeless_series('m','cpu','{\"host\":{\"re\":\"web-.*\"},\"env\":{\"neq\":\"dev\"}}') ORDER BY labels")?
            == [r#"{"code":"a","env":"prod","host":"web-1"}"#]
    );
    ensure!(
        strings(&b, "SELECT labels FROM timeless_series('m','cpu','{\"env\":{\"re\":\"\"}}') ORDER BY labels")?
            == [r#"{"env":"","host":"empty"}"#, r#"{"host":"db-1"}"#]
    );
    ensure!(
        strings(&b, "SELECT value FROM timeless_label_values('m','cpu','host','{\"env\":{\"neq\":\"dev\"}}')")?
            == ["db-1", "empty", "web-1"]
    );
    ensure!(scalar_i64(&b, "SELECT COUNT(*) FROM timeless_raw_batches('m','cpu','{\"host\":{\"re\":\"web-.*\"},\"env\":{\"neq\":\"dev\"}}',0,20)")? == 1);
    a.execute_batch(
        "BEGIN;
         INSERT INTO m(name,labels,ts,value) VALUES('cpu','{\"host\":\"tmp\"}',20,9.0);
         INSERT INTO m(m) VALUES('flush');
         ROLLBACK;",
    )?;
    ensure!(
        scalar_i64(
            &b,
            "SELECT COUNT(*) FROM timeless_series('m','cpu','{\"host\":\"tmp\"}')"
        )? == 0
    );
    drop(a);
    drop(b);
    ensure!(
        scalar_i64(
            &open(extension, database)?,
            "SELECT COUNT(*) FROM timeless_series('m','cpu','{\"host\":{\"re\":\"web-.*\"}}')"
        )? == 2
    );
    println!(
        "PASS: matcher-aware discovery covers buffered, combined, absent, rollback, and reopen"
    );
    Ok(())
}

fn raw_rows(connection: &Connection) -> Result<Vec<(i64, f64)>> {
    Ok(connection
        .prepare("SELECT ts,value FROM timeless_raw('m','cpu',NULL,0,100) ORDER BY ts,value")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?)
}

fn reader_gate(extension: &Path, database: &Path) -> Result<()> {
    let a = open(extension, database)?;
    let b = open(extension, database)?;
    a.execute_batch(
        "CREATE VIRTUAL TABLE m USING timeless_metrics;
         INSERT INTO m(name,ts,value) VALUES('cpu',10,1.0);
         INSERT INTO m(m) VALUES('flush');",
    )?;
    ensure!(raw_rows(&b)? == [(10, 1.0)]);
    a.execute_batch(
        "BEGIN;
         INSERT INTO m(name,ts,value) VALUES('cpu',20,2.0);
         INSERT INTO m(m) VALUES('flush');",
    )?;
    let error = raw_rows(&b).expect_err("reader escaped active write transaction");
    let message = error.to_string();
    ensure!(
        message.contains("active write transaction")
            && message.contains("retry")
            && message.contains("SQLITE_BUSY")
    );
    a.execute("ROLLBACK", [])?;
    ensure!(raw_rows(&b)? == [(10, 1.0)]);
    a.execute_batch(
        "BEGIN;
         INSERT INTO m(name,ts,value) VALUES('cpu',30,3.0);
         INSERT INTO m(m) VALUES('flush');
         COMMIT;",
    )?;
    ensure!(raw_rows(&b)? == [(10, 1.0), (30, 3.0)]);
    println!("PASS: reader gate reports conflict and publishes rollback/commit exactly");
    Ok(())
}

fn series_id(extension: &Path, database: &Path) -> Result<()> {
    let connection = open(extension, database)?;
    let series_id: i64 = connection.query_row(
        "SELECT series_id FROM timeless_series('m','cpu','{\"host\":\"a\"}')",
        [],
        |row| row.get(0),
    )?;
    ensure!(series_id == 1);
    let checks = [
        ("SELECT name,labels,min_ts,max_ts,points FROM timeless_series('m') WHERE series_id=?1", "SELECT name,labels,min_ts,max_ts,points FROM timeless_series('m','cpu','{\"host\":\"a\"}')"),
        ("SELECT ts,value FROM m WHERE series_id=?1 ORDER BY ts,value", "SELECT ts,value FROM m WHERE name='cpu' AND labels='{\"host\":\"a\"}' ORDER BY ts,value"),
        ("SELECT ts,value FROM timeless_raw('m','cpu',NULL,0,30) WHERE series_id=?1 ORDER BY ts,value", "SELECT ts,value FROM timeless_raw('m','cpu','{\"host\":\"a\"}',0,30) ORDER BY ts,value"),
        ("SELECT points FROM timeless_raw_batches('m','cpu',NULL,0,30) WHERE series_id=?1", "SELECT points FROM timeless_raw_batches('m','cpu','{\"host\":\"a\"}',0,30)"),
        ("SELECT value FROM timeless_aggregate('m','cpu',NULL,0,30,'avg') WHERE series_id=?1", "SELECT value FROM timeless_aggregate('m','cpu','{\"host\":\"a\"}',0,30,'avg')"),
        ("SELECT ts,value FROM timeless_latest('m','cpu',NULL,0,30) WHERE series_id=?1", "SELECT ts,value FROM timeless_latest('m','cpu','{\"host\":\"a\"}',0,30)"),
        ("SELECT ts,value FROM timeless_grid('m','cpu',NULL,0,20,10,10) WHERE series_id=?1 ORDER BY ts", "SELECT ts,value FROM timeless_grid('m','cpu','{\"host\":\"a\"}',0,20,10,10) ORDER BY ts"),
        ("SELECT ts,value FROM timeless_window('m','cpu',NULL,0,20,10,10,'avg') WHERE series_id=?1 ORDER BY ts", "SELECT ts,value FROM timeless_window('m','cpu','{\"host\":\"a\"}',0,20,10,10,'avg') ORDER BY ts"),
        ("SELECT buckets FROM timeless_window_batches('m','cpu',NULL,0,20,10,10,'avg') WHERE series_id=?1", "SELECT buckets FROM timeless_window_batches('m','cpu','{\"host\":\"a\"}',0,20,10,10,'avg')"),
        ("SELECT ts,value FROM timeless_rollup('m','cpu',NULL,10,0,100,'avg') WHERE series_id=?1 ORDER BY ts", "SELECT ts,value FROM timeless_rollup('m','cpu','{\"host\":\"a\"}',10,0,100,'avg') ORDER BY ts"),
        ("SELECT buckets FROM timeless_rollup_batches('m','cpu',NULL,10,0,100) WHERE series_id=?1", "SELECT buckets FROM timeless_rollup_batches('m','cpu','{\"host\":\"a\"}',10,0,100)"),
    ];
    for (selected, expected) in checks {
        ensure!(
            values(&connection, selected, &[Value::Integer(series_id)])?
                == values(&connection, expected, &[])?
        );
    }
    for (value, expected) in [
        (Value::Integer(1), 1),
        (Value::Real(1.0), 1),
        (Value::Text("1".into()), 1),
        (Value::Text("+1.0e0".into()), 1),
        (Value::Real(1.5), 0),
        (Value::Text("1x".into()), 0),
        (Value::Null, 0),
        (Value::Blob(b"1".to_vec()), 0),
    ] {
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM timeless_aggregate('m','cpu',NULL,0,30,'avg') WHERE series_id=?1",
            params![value],
            |row| row.get(0),
        )?;
        ensure!(count == expected);
    }
    ensure!(scalar_i64(&connection, "SELECT COUNT(*) FROM timeless_aggregate('m','cpu','{\"host\":\"b\"}',0,30,'avg') WHERE series_id=1")? == 0);
    ensure!(
        scalar_i64(
            &connection,
            "SELECT COUNT(*) FROM timeless_aggregate('m','mem',NULL,0,30,'avg') WHERE series_id=1"
        )? == 0
    );
    ensure!(scalar_i64(&connection, "SELECT COUNT(*) FROM timeless_aggregate('m','cpu',NULL,0,30,'avg') WHERE series_id=999999")? == 0);
    ensure!(
        values(&connection, "SELECT s.series_id,q.value FROM timeless_series('m','cpu',NULL) s JOIN timeless_aggregate('m','cpu',NULL,0,30,'avg') q ON q.series_id=s.series_id ORDER BY s.series_id", &[])?
            == [vec![Value::Integer(1), Value::Real(3.0)], vec![Value::Integer(2), Value::Real(10.0)]]
    );
    ensure!(values(&connection, "SELECT a.series_id FROM m a JOIN m b ON b.series_id=a.series_id WHERE a.ts=0 AND b.ts=10 ORDER BY a.series_id", &[])? == [vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    let plan = connection
        .prepare("EXPLAIN QUERY PLAN SELECT s.series_id,q.value FROM timeless_series('m','cpu',NULL) s JOIN timeless_aggregate('m','cpu',NULL,0,30,'avg') q ON q.series_id=s.series_id")?
        .query_map([], |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(plan.iter().any(|line| line.contains("107374")));
    connection.execute(
        "UPDATE m_chunks SET ts_data=x'00' WHERE series_id=2 AND resolution=0",
        [],
    )?;
    ensure!(
        scalar_i64(
            &connection,
            "SELECT COUNT(*) FROM timeless_raw('m','cpu',NULL,0,30) WHERE series_id=1"
        )? == 3
    );
    let error = connection
        .query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM timeless_raw('m','cpu',NULL,0,30)",
            [],
            |row| row.get(0),
        )
        .expect_err("broad scan ignored corrupt unrelated chunk");
    let message = error.to_string().to_lowercase();
    ensure!(["decode", "payload", "insufficientdata", "out of bounds"]
        .iter()
        .any(|needle| message.contains(needle)));
    println!("PASS: durable series-id parity, affinity, intersection, joins, and pruning");
    Ok(())
}

fn bitmap_ok(bitmap: &[u8], count: usize) -> bool {
    count.is_multiple_of(8)
        || bitmap
            .last()
            .is_some_and(|last| last & !((1 << (count % 8)) - 1) == 0)
}

type AggregateFrameRows = Vec<(i64, Option<Value>)>;

fn decode_aggregate(blob: &[u8]) -> Result<(u8, AggregateFrameRows)> {
    ensure!(blob.len() >= 12 && &blob[..4] == b"TAF1");
    let kind = blob[4];
    ensure!(kind <= 4 && blob[5..8] == [0, 0, 0]);
    let count = read_u32(blob, 8)? as usize;
    let bitmap_len = count.div_ceil(8);
    ensure!(blob.len() == 12 + count * 16 + bitmap_len);
    let ids = read_i64s(blob, 12, count)?;
    let bitmap_at = 12 + count * 8;
    let bitmap = &blob[bitmap_at..bitmap_at + bitmap_len];
    ensure!(bitmap_ok(bitmap, count));
    let words = read_u64s(blob, bitmap_at + bitmap_len, count)?;
    let mut rows = Vec::with_capacity(count);
    for (index, (id, word)) in ids.into_iter().zip(words).enumerate() {
        let valid = bitmap[index / 8] & (1 << (index % 8)) != 0;
        let value = if !valid {
            ensure!(word == 0 && kind != 4);
            None
        } else if kind == 4 {
            ensure!(word <= i64::MAX as u64);
            Some(Value::Integer(word as i64))
        } else {
            let value = f64::from_bits(word);
            ensure!(!value.is_nan());
            Some(Value::Real(value))
        };
        rows.push((id, value));
    }
    Ok((kind, rows))
}

fn decode_latest(blob: &[u8]) -> Result<Vec<(i64, i64, Option<f64>)>> {
    ensure!(blob.len() >= 8 && &blob[..4] == b"TLF1");
    let count = read_u32(blob, 4)? as usize;
    let bitmap_len = count.div_ceil(8);
    ensure!(blob.len() == 8 + count * 24 + bitmap_len);
    let ids = read_i64s(blob, 8, count)?;
    let timestamps_at = 8 + count * 8;
    let timestamps = read_i64s(blob, timestamps_at, count)?;
    let bitmap_at = timestamps_at + count * 8;
    let bitmap = &blob[bitmap_at..bitmap_at + bitmap_len];
    ensure!(bitmap_ok(bitmap, count));
    let words = read_u64s(blob, bitmap_at + bitmap_len, count)?;
    let mut rows = Vec::with_capacity(count);
    for (index, ((id, timestamp), word)) in ids.into_iter().zip(timestamps).zip(words).enumerate() {
        let valid = bitmap[index / 8] & (1 << (index % 8)) != 0;
        let value = if valid {
            let value = f64::from_bits(word);
            ensure!(!value.is_nan());
            Some(value)
        } else {
            ensure!(word == 0);
            None
        };
        rows.push((id, timestamp, value));
    }
    Ok(rows)
}

fn frames(extension: &Path, database: &Path, auxiliary: &[PathBuf]) -> Result<()> {
    ensure!(
        auxiliary.len() == 2,
        "frames needs aggregate and latest databases"
    );
    let aggregate = open(extension, &auxiliary[0])?;
    ensure!(strings(&aggregate, "SELECT name FROM pragma_module_list WHERE name IN ('timeless_aggregate_frame','timeless_latest_frame') ORDER BY name")? == ["timeless_aggregate_frame", "timeless_latest_frame"]);
    for (kind, name) in ["avg", "sum", "min", "max", "count"]
        .into_iter()
        .enumerate()
    {
        let metric = if name == "count" { "cpu" } else { "nan_metric" };
        let blob: Vec<u8> = aggregate.query_row(
            "SELECT frame FROM timeless_aggregate_frame('ag',?1,NULL,-10,20,?2)",
            params![metric, name],
            |row| row.get(0),
        )?;
        let (decoded_kind, mut decoded) = decode_aggregate(&blob)?;
        ensure!(decoded_kind == kind as u8);
        decoded.sort_by_key(|row| row.0);
        let expected = values(&aggregate, "SELECT series_id,value FROM timeless_aggregate('ag',?1,NULL,-10,20,?2) ORDER BY series_id", &[Value::Text(metric.into()), Value::Text(name.into())])?;
        let decoded_values = decoded
            .into_iter()
            .map(|(id, value)| vec![Value::Integer(id), value.unwrap_or(Value::Null)])
            .collect::<Vec<_>>();
        ensure!(decoded_values == expected);
    }
    ensure!(scalar_i64(&aggregate, "SELECT COUNT(*) FROM timeless_aggregate_frame('ag','cpu','{\"host\":\"missing\"}',0,20,'avg')")? == 0);
    drop(aggregate);
    let latest = open(extension, &auxiliary[1])?;
    let blob: Vec<u8> = latest.query_row(
        "SELECT frame FROM timeless_latest_frame('latest','cpu',NULL,0,100)",
        [],
        |row| row.get(0),
    )?;
    let mut decoded = decode_latest(&blob)?;
    decoded.sort_by_key(|row| row.0);
    let expected = latest
        .prepare("SELECT series_id,ts,value FROM timeless_latest('latest','cpu',NULL,0,100) ORDER BY series_id")?
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, Option<f64>>(2)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(decoded == expected);
    ensure!(scalar_i64(&latest, "SELECT COUNT(*) FROM timeless_latest_frame('latest','cpu','{\"host\":\"missing\"}',0,100)")? == 0);
    drop(latest);

    let a = open(extension, database)?;
    let b = open(extension, database)?;
    a.execute("CREATE VIRTUAL TABLE m USING timeless_metrics", [])?;
    a.execute(
        "INSERT INTO m(name,labels,ts,value) VALUES('cpu','{\"host\":\"a\"}',10,1.0)",
        [],
    )?;
    let first: Vec<u8> = b.query_row(
        "SELECT frame FROM timeless_latest_frame('m','cpu',NULL,0,100)",
        [],
        |row| row.get(0),
    )?;
    ensure!(decode_latest(&first)? == [(1, 10, Some(1.0))]);
    a.execute("BEGIN", [])?;
    a.execute(
        "INSERT INTO m(name,labels,ts,value) VALUES('cpu','{\"host\":\"a\"}',20,2.0)",
        [],
    )?;
    let error = b
        .query_row::<Vec<u8>, _, _>(
            "SELECT frame FROM timeless_latest_frame('m','cpu',NULL,0,100)",
            [],
            |row| row.get(0),
        )
        .expect_err("frame escaped active transaction");
    ensure!(error.to_string().contains("active write transaction"));
    a.execute("COMMIT", [])?;
    let second: Vec<u8> = b.query_row(
        "SELECT frame FROM timeless_latest_frame('m','cpu',NULL,0,100)",
        [],
        |row| row.get(0),
    )?;
    ensure!(decode_latest(&second)? == [(1, 20, Some(2.0))]);
    println!("PASS: Rust TAF1/TLF1 decoders match rows and preserve publication");
    Ok(())
}

fn stats(connection: &Connection, table: &str) -> Result<BTreeMap<String, Option<i64>>> {
    Ok(connection
        .prepare("SELECT key,CAST(value AS INTEGER) FROM timeless_stats(?1)")?
        .query_map(params![table], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?)
}

fn stat(current: &BTreeMap<String, Option<i64>>, key: &str) -> Result<i64> {
    current
        .get(key)
        .copied()
        .flatten()
        .with_context(|| format!("missing non-NULL stat {key}"))
}

fn logs_optimize(extension: &Path, database: &Path) -> Result<()> {
    let connection = open(extension, database)?;
    connection.execute(
        "CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service')",
        [],
    )?;
    for cycle in 0..40 {
        connection.execute(
            "INSERT INTO logs(logs) VALUES(?1)",
            params![blobs::log_batch(cycle * 256, 256, 1)],
        )?;
        connection.execute("INSERT INTO logs(logs) VALUES('flush')", [])?;
        connection.execute("INSERT INTO logs(logs) VALUES('optimize')", [])?;
    }
    let current = stats(&connection, "logs")?;
    ensure!(scalar_i64(&connection, "SELECT n FROM timeless_log_count('logs')")? == 10_240);
    for (key, expected) in [
        ("optimize_raw_entries", 10_240),
        ("optimize_merge_entries", 12_288),
        ("optimize_blocks_removed", 73),
        ("optimize_blocks_written", 42),
        ("blocks", 9),
        ("optimize_pending_raw_entries", 0),
        ("optimize_merge_ready_entries", 0),
        ("optimize_merge_deferred_blocks", 8),
        ("optimize_merge_deferred_entries", 2_048),
    ] {
        let actual = stat(&current, key)?;
        ensure!(actual == expected, "{key}: {actual} != {expected}");
    }
    ensure!(
        (stat(&current, "optimize_raw_entries")? + stat(&current, "optimize_merge_entries")?)
            as f64
            / 10_240.0
            == 2.2
    );
    connection.execute("CREATE VIRTUAL TABLE budgeted USING timeless_logs", [])?;
    for cycle in 0..4 {
        connection.execute(
            "INSERT INTO budgeted(budgeted) VALUES(?1)",
            params![blobs::log_batch(cycle * 3_600_001, 256, 1)],
        )?;
        connection.execute("INSERT INTO budgeted(budgeted) VALUES('flush')", [])?;
    }
    connection.execute("INSERT INTO budgeted(budgeted) VALUES('optimize:512')", [])?;
    let first = stats(&connection, "budgeted")?;
    ensure!(stat(&first, "raw_blocks")? == 2 && stat(&first, "optimize_raw_entries")? == 512);
    ensure!(
        stat(&first, "optimize_budgeted_count")? == 1
            && stat(&first, "optimize_budget_entries")? == 512
    );
    ensure!(stat(&first, "optimize_budget_limited_count")? == 1);
    connection.execute("INSERT INTO budgeted(budgeted) VALUES('optimize:512')", [])?;
    let second = stats(&connection, "budgeted")?;
    ensure!(stat(&second, "raw_blocks")? == 0 && stat(&second, "optimize_raw_entries")? == 1024);
    ensure!(scalar_i64(&connection, "SELECT COUNT(*) FROM budgeted")? == 1024);
    let error = connection
        .execute("INSERT INTO budgeted(budgeted) VALUES('optimize:0')", [])
        .expect_err("zero optimize budget accepted");
    ensure!(error.to_string().contains("budget must be positive"));
    println!("PASS: size-tiered optimize bounds rewrites and budgets work");
    Ok(())
}

fn trace_reads(extension: &Path, database: &Path) -> Result<()> {
    let connection = open(extension, database)?;
    connection.execute("CREATE VIRTUAL TABLE traces USING timeless_traces", [])?;
    for number in 0_i64..12 {
        let service = if number == 0 { "worker" } else { "api" };
        let operation = if service == "api" {
            "GET /items"
        } else {
            "tick"
        };
        let duration = if number == 5 { 5_000 } else { number + 1 };
        connection.execute(
            "INSERT INTO traces(trace_id,span_id,name,service,kind,status,start_ts,duration_ns,attributes,events,resource,instrumentation_scope) VALUES(?1,?2,?3,?4,'server','ok',?5,?6,'{}','[]','{}','{}')",
            params![number.to_be_bytes().repeat(2), number.to_be_bytes(), operation, service, number, duration],
        )?;
        if [3, 7, 11].contains(&number) {
            connection.execute("INSERT INTO traces(traces) VALUES('flush')", [])?;
        }
    }
    let services = strings(
        &connection,
        "SELECT value FROM timeless_trace_services('traces') ORDER BY value",
    )?
    .join(",");
    let operations = strings(
        &connection,
        "SELECT value FROM timeless_trace_operations('traces','api') ORDER BY value",
    )?
    .join(",");
    let newest = values(&connection, "SELECT start_ts FROM traces WHERE service='api' ORDER BY start_ts DESC,span_id DESC LIMIT 2 OFFSET 1", &[])?;
    let duration = values(&connection, "SELECT start_ts FROM traces WHERE service='api' AND duration_ns>=1000 ORDER BY start_ts DESC,span_id DESC LIMIT 2", &[])?;
    let current = stats(&connection, "traces")?;
    ensure!(services == "api,worker" && operations == "GET /items");
    ensure!(newest == [vec![Value::Integer(10)], vec![Value::Integer(9)]]);
    ensure!(duration == [vec![Value::Integer(5)]]);
    ensure!(stat(&current, "discovery_count")? == 2);
    ensure!(stat(&current, "query_bounded_count")? == 2);
    ensure!(stat(&current, "query_bounded_requested_spans")? == 5);
    ensure!(stat(&current, "query_bounded_max_spans")? == 3);
    ensure!(stat(&current, "query_stable_location_snapshots")? == 2);
    ensure!(stat(&current, "query_snapshot_payload_max_bytes")? == 0);
    ensure!(stat(&current, "query_blocks_skipped_by_bound")? >= 2);
    ensure!(
        scalar_i64(
            &connection,
            "SELECT COUNT(*) FROM traces_terms WHERE term='operations:'"
        )? == 3
    );
    let plan = connection
        .prepare("EXPLAIN QUERY PLAN SELECT start_ts FROM traces WHERE service='api' ORDER BY start_ts DESC,span_id DESC LIMIT 2 OFFSET 1")?
        .query_map([], |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .join(" ");
    ensure!(plan.contains("bounded-ts-desc-offset"));
    ensure!(scalar_i64(&connection, "SELECT COUNT(*) FROM traces")? == 12);
    let after = stats(&connection, "traces")?;
    ensure!(stat(&after, "query_count")? == 3);
    ensure!(stat(&after, "query_stable_location_snapshots")? == 3);
    ensure!(stat(&after, "query_snapshot_payload_max_bytes")? == 0);
    println!("PASS: trace discovery, bounded streaming reads, and stable snapshots");
    Ok(())
}

pub(super) fn run(
    section: CliSection,
    extension: &Path,
    database: &Path,
    auxiliary: &[PathBuf],
) -> Result<()> {
    match section {
        CliSection::SharedEngine => shared_engine(extension, database),
        CliSection::PackedRollup => packed_rollup(extension, database),
        CliSection::RichTraces => super::rich_traces::run(extension, database),
        CliSection::LatestPublication => latest_publication(extension, database),
        CliSection::CatalogPublication => catalog_publication(extension, database),
        CliSection::MatcherDiscovery => matcher_discovery(extension, database),
        CliSection::ReaderGate => reader_gate(extension, database),
        CliSection::SeriesId => series_id(extension, database),
        CliSection::Frames => frames(extension, database, auxiliary),
        CliSection::LogsOptimize => logs_optimize(extension, database),
        CliSection::TraceReads => trace_reads(extension, database),
    }
}
