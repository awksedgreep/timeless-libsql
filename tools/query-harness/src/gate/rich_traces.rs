use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{ensure, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

use super::open;

#[derive(Clone, Debug)]
struct Span {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    name: String,
    service: String,
    kind: u8,
    status: u8,
    start_ts: i64,
    duration_ns: i64,
    attributes: Value,
    status_description: String,
    events: Value,
    resource: Value,
    instrumentation_scope: Value,
}

#[derive(Debug, PartialEq)]
struct SemanticSpan {
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
    parent_span_id: Option<Vec<u8>>,
    name: String,
    service: String,
    kind: String,
    status: String,
    start_ts: i64,
    duration_ns: i64,
    attributes: Value,
    status_description: String,
    events: Value,
    resource: Value,
    instrumentation_scope: Value,
}

const COLUMNS: &str = "trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,duration_ns,attributes,status_description,events,resource,instrumentation_scope";

fn fixed_be<const N: usize>(number: u64) -> [u8; N] {
    let mut result = [0_u8; N];
    let bytes = number.to_be_bytes();
    result[N - bytes.len().min(N)..].copy_from_slice(&bytes[bytes.len().saturating_sub(N)..]);
    result
}

fn rich_span(number: u64, start_ts: Option<i64>) -> Span {
    Span {
        trace_id: fixed_be(number),
        span_id: fixed_be(number),
        parent_span_id: number
            .is_multiple_of(2)
            .then(|| fixed_be(number.saturating_sub(1).max(1))),
        name: if number % 2 == 1 {
            "GET /rich"
        } else {
            "db.query"
        }
        .into(),
        service: "explicit-must-not-win".into(),
        kind: (number % 5) as u8,
        status: (number % 3) as u8,
        start_ts: start_ts.unwrap_or(1_700_000_000_000_000_000 + number as i64),
        duration_ns: number as i64 * 101,
        attributes: json!({
            "array": [1, "two", false, null],
            "bool": true,
            "count": number,
            "nested": {"ratio": 1.25, "unicode": "空🔥"},
            "service.name": "rich-service"
        }),
        status_description: if number % 3 == 2 { "boom 🚀" } else { "" }.into(),
        events: json!([{
            "attributes": {"attempt": number, "fatal": false},
            "name": "exception",
            "timestamp": 1_700_000_000_000_000_001_i64 + number as i64
        }]),
        resource: json!({
            "deployment.environment": "test",
            "service.name": "resource-must-not-win"
        }),
        instrumentation_scope: json!({
            "attributes": {"debug": false},
            "name": "rich-lib",
            "version": "4.5.6"
        }),
    }
}

fn contract_fixture() -> Vec<Span> {
    let resource = json!({
        "debug": false,
        "replica": 7,
        "service.name": "contract-svc",
        "service.version": "1.2.3"
    });
    let scope = json!({"name":"contract-lib","version":"4.5.6"});
    vec![
        Span {
            trace_id: hex("00112233445566778899aabbccddeeff"),
            span_id: hex("0102030405060708"),
            parent_span_id: None,
            name: "GET /contract".into(),
            service: "explicit-must-not-win".into(),
            kind: 1,
            status: 2,
            status_description: "contract failure".into(),
            start_ts: 1_700_000_000_000_000_000,
            duration_ns: 120_000_000,
            attributes: json!({
                "http.method":"GET",
                "http.status_code":503,
                "retryable":true,
                "score":0.75
            }),
            events: json!([{
                "attributes":{"exception.type":"ContractError","handled":false},
                "name":"exception",
                "timestamp":1_700_000_000_040_000_000_i64
            }]),
            resource: resource.clone(),
            instrumentation_scope: scope.clone(),
        },
        Span {
            trace_id: hex("00112233445566778899aabbccddeeff"),
            span_id: hex("1112131415161718"),
            parent_span_id: Some(hex("0102030405060708")),
            name: "DB contract".into(),
            service: "explicit-must-not-win".into(),
            kind: 2,
            status: 0,
            status_description: String::new(),
            start_ts: 1_700_000_000_020_000_000,
            duration_ns: 60_000_000,
            attributes: json!({"db.system":"libsql","rows":3}),
            events: json!([]),
            resource,
            instrumentation_scope: scope,
        },
    ]
}

const PERCENTILE_START: i64 = 1_800_000_000_000_000_000;

fn percentile_cases() -> Vec<(String, Vec<i64>)> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let large = (0..8192)
        .map(|_| {
            state ^= state << 7;
            state ^= state >> 9;
            state ^= state << 8;
            (state % 1_000_003) as i64
        })
        .collect();
    vec![
        ("singleton".into(), vec![42]),
        (
            "duplicates".into(),
            (0..129).map(|index| [7, 7, 7, 11][index % 4]).collect(),
        ),
        ("ordered".into(), (0..257).map(i64::from).collect()),
        ("reverse".into(), (0..257).rev().map(i64::from).collect()),
        ("large-random".into(), large),
    ]
}

fn percentile_span(number: u64, service: &str, position: usize, duration_ns: i64) -> Span {
    Span {
        trace_id: fixed_be(number),
        span_id: fixed_be(number),
        parent_span_id: None,
        name: "percentile.contract".into(),
        service: service.into(),
        kind: 0,
        status: 1,
        start_ts: PERCENTILE_START + position as i64,
        duration_ns,
        attributes: json!({"service.name": service}),
        status_description: String::new(),
        events: json!([]),
        resource: json!({}),
        instrumentation_scope: json!({}),
    }
}

fn sorted_percentiles(values: &[i64]) -> (i64, i64, i64) {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = |percent: usize| {
        let one_based = (sorted.len() * percent).div_ceil(100);
        sorted[one_based.clamp(1, sorted.len()) - 1]
    };
    (rank(50), rank(95), rank(99))
}

fn percentile_contract(
    connection: &Connection,
    table: &str,
    cases: &[(String, Vec<i64>)],
) -> Result<()> {
    let empty: i64 = connection.query_row(
        "SELECT count(*) FROM timeless_trace_buckets(?1,?2,?3,?4,?5)",
        params![
            table,
            "empty",
            PERCENTILE_START,
            PERCENTILE_START + 20_000,
            100_000_i64
        ],
        |row| row.get(0),
    )?;
    ensure!(empty == 0, "empty percentile input unexpectedly emitted a bucket");

    for (service, values) in cases {
        let actual: (i64, i64, i64, i64) = connection.query_row(
            "SELECT spans,dur_p50,dur_p95,dur_p99 \
             FROM timeless_trace_buckets(?1,?2,?3,?4,?5)",
            params![
                table,
                service,
                PERCENTILE_START,
                PERCENTILE_START + 20_000,
                100_000_i64
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        let expected = sorted_percentiles(values);
        ensure!(
            actual == (values.len() as i64, expected.0, expected.1, expected.2),
            "nearest-rank mismatch for {service}: got {actual:?}, expected {expected:?}"
        );
    }
    Ok(())
}

fn hex<const N: usize>(value: &str) -> [u8; N] {
    assert_eq!(value.len(), N * 2);
    let mut output = [0_u8; N];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).unwrap();
    }
    output
}

fn framed(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

fn batch(spans: &[Span], version: u8) -> Result<Vec<u8>> {
    let mut out = vec![version, 0, 0, 0];
    out.extend_from_slice(&(spans.len() as u32).to_le_bytes());
    for span in spans {
        out.extend_from_slice(&span.trace_id);
    }
    for span in spans {
        out.extend_from_slice(&span.span_id);
    }
    for span in spans {
        out.extend_from_slice(&span.parent_span_id.unwrap_or([0; 8]));
    }
    for span in spans {
        framed(&mut out, span.name.as_bytes());
    }
    for span in spans {
        framed(&mut out, span.service.as_bytes());
    }
    out.extend(spans.iter().map(|span| span.kind));
    out.extend(spans.iter().map(|span| span.status));
    for span in spans {
        out.extend_from_slice(&span.start_ts.to_le_bytes());
    }
    for span in spans {
        out.extend_from_slice(&span.duration_ns.to_le_bytes());
    }
    for span in spans {
        framed(
            &mut out,
            serde_json::to_string(&span.attributes)?.as_bytes(),
        );
    }
    if version == 2 {
        for span in spans {
            framed(&mut out, span.status_description.as_bytes());
        }
        for span in spans {
            framed(&mut out, serde_json::to_string(&span.events)?.as_bytes());
        }
        for span in spans {
            framed(&mut out, serde_json::to_string(&span.resource)?.as_bytes());
        }
        for span in spans {
            framed(
                &mut out,
                serde_json::to_string(&span.instrumentation_scope)?.as_bytes(),
            );
        }
    }
    Ok(out)
}

fn insert_row(connection: &Connection, table: &str, span: &Span) -> Result<()> {
    let sql = format!(
        "INSERT INTO \"{table}\"({COLUMNS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)"
    );
    let kinds = ["internal", "server", "client", "producer", "consumer"];
    let statuses = ["unset", "ok", "error"];
    connection.execute(
        &sql,
        params![
            span.trace_id.as_slice(),
            span.span_id.as_slice(),
            span.parent_span_id.as_ref().map(<[u8; 8]>::as_slice),
            span.name,
            span.service,
            kinds[span.kind as usize],
            statuses[span.status as usize],
            span.start_ts,
            span.duration_ns,
            serde_json::to_string(&span.attributes)?,
            span.status_description,
            serde_json::to_string(&span.events)?,
            serde_json::to_string(&span.resource)?,
            serde_json::to_string(&span.instrumentation_scope)?,
        ],
    )?;
    Ok(())
}

fn semantic_rows(connection: &Connection, table: &str) -> Result<Vec<SemanticSpan>> {
    let sql = format!("SELECT {COLUMNS} FROM \"{table}\" ORDER BY start_ts,span_id");
    let mut statement = connection.prepare(&sql)?;
    let rows = statement
        .query_map([], |row| {
            let attributes = serde_json::from_str(&row.get::<_, String>(9)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    9,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            let events = serde_json::from_str(&row.get::<_, String>(11)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    11,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            let resource = serde_json::from_str(&row.get::<_, String>(12)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    12,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            let instrumentation_scope =
                serde_json::from_str(&row.get::<_, String>(13)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        13,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
            Ok(SemanticSpan {
                trace_id: row.get(0)?,
                span_id: row.get(1)?,
                parent_span_id: row.get(2)?,
                name: row.get(3)?,
                service: row.get(4)?,
                kind: row.get(5)?,
                status: row.get(6)?,
                start_ts: row.get(7)?,
                duration_ns: row.get(8)?,
                attributes,
                status_description: row.get(10)?,
                events,
                resource,
                instrumentation_scope,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn integer_stats(connection: &Connection, table: &str) -> Result<BTreeMap<String, i64>> {
    let mut statement = connection
        .prepare("SELECT key,value FROM timeless_stats(?1) WHERE typeof(value)='integer'")?;
    let rows = statement.query_map([table], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut stats = BTreeMap::new();
    for row in rows {
        let (key, value) = row?;
        stats.insert(key, value);
    }
    Ok(stats)
}

fn stat_delta(
    before: &BTreeMap<String, i64>,
    after: &BTreeMap<String, i64>,
    key: &str,
) -> Result<i64> {
    let before = before
        .get(key)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("timeless_stats omitted {key:?} before query"))?;
    let after = after
        .get(key)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("timeless_stats omitted {key:?} after query"))?;
    Ok(after.saturating_sub(before))
}

fn projection_contract(
    connection: &Connection,
    table: &str,
    fixture: &[Span],
    assert_selective_work: bool,
) -> Result<()> {
    let columns = COLUMNS.split(',').collect::<Vec<_>>();
    for column in &columns {
        let sql = format!(
            "SELECT {column} FROM \"{table}\" WHERE service='contract-svc' \
             ORDER BY start_ts,span_id"
        );
        let mut statement = connection.prepare(&sql)?;
        let mut rows = statement.query([])?;
        let mut count = 0;
        while let Some(row) = rows.next()? {
            let _ = row.get_ref(0)?;
            count += 1;
        }
        ensure!(count == fixture.len(), "individual projection {column} lost rows");
    }

    let mixed: (Vec<u8>, String, String, i64) = connection.query_row(
        &format!(
            "SELECT trace_id,attributes,events,duration_ns FROM \"{table}\" \
             WHERE status='error' AND service='contract-svc'"
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    ensure!(mixed.0 == fixture[0].trace_id);
    ensure!(serde_json::from_str::<Value>(&mixed.1)? == fixture[0].attributes);
    ensure!(serde_json::from_str::<Value>(&mixed.2)? == fixture[0].events);
    ensure!(mixed.3 == fixture[0].duration_ns);
    if !assert_selective_work {
        return Ok(());
    }

    let before = integer_stats(connection, table)?;
    let count: i64 = connection.query_row(
        &format!("SELECT count(*) FROM \"{table}\""),
        [],
        |row| row.get(0),
    )?;
    ensure!(count == fixture.len() as i64);
    let after = integer_stats(connection, table)?;
    ensure!(stat_delta(&before, &after, "query_decoded_columns")? == 0);
    ensure!(stat_delta(&before, &after, "query_decoded_column_bytes")? == 0);
    ensure!(stat_delta(&before, &after, "query_materialized_rich_values")? == 0);

    let before = after;
    let count: i64 = connection.query_row(
        &format!(
            "SELECT count(*) FROM \"{table}\" \
             WHERE trace_id=?1 AND name='missing-operation'"
        ),
        [fixture[0].trace_id.as_slice()],
        |row| row.get(0),
    )?;
    ensure!(count == 0);
    let after = integer_stats(connection, table)?;
    let payload_blocks = stat_delta(&before, &after, "query_payload_blocks_read")?;
    ensure!(payload_blocks > 0);
    ensure!(
        stat_delta(&before, &after, "query_decoded_columns")? == payload_blocks * 2
    );
    ensure!(stat_delta(&before, &after, "query_decoded_column_bytes")? > 0);
    ensure!(stat_delta(&before, &after, "query_materialized_rich_values")? == 0);

    let before = after;
    let attributes: String = connection.query_row(
        &format!(
            "SELECT attributes FROM \"{table}\" \
             WHERE trace_id=?1 AND name='GET /contract'"
        ),
        [fixture[0].trace_id.as_slice()],
        |row| row.get(0),
    )?;
    ensure!(serde_json::from_str::<Value>(&attributes)? == fixture[0].attributes);
    let after = integer_stats(connection, table)?;
    let payload_blocks = stat_delta(&before, &after, "query_payload_blocks_read")?;
    ensure!(payload_blocks > 0);
    ensure!(
        stat_delta(&before, &after, "query_decoded_columns")? == payload_blocks * 2 + 1
    );
    ensure!(stat_delta(&before, &after, "query_materialized_rich_values")? == 1);

    let before = after;
    connection.execute_batch("BEGIN")?;
    let value: String = connection.query_row(
        &format!(
            "SELECT attributes FROM \"{table}\" \
             WHERE trace_id=?1 AND name='GET /contract'"
        ),
        [fixture[0].trace_id.as_slice()],
        |row| row.get(0),
    )?;
    ensure!(serde_json::from_str::<Value>(&value)? == fixture[0].attributes);
    connection.execute_batch("ROLLBACK")?;
    let after = integer_stats(connection, table)?;
    ensure!(stat_delta(&before, &after, "query_count")? == 1);
    ensure!(stat_delta(&before, &after, "query_materialized_rich_values")? == 1);
    Ok(())
}

pub(super) fn run(extension: &Path, database: &Path) -> Result<()> {
    let connection = open(extension, database)?;
    connection.execute_batch(
        "CREATE VIRTUAL TABLE row_spans USING timeless_traces;
         CREATE VIRTUAL TABLE batch_spans USING timeless_traces;
         CREATE VIRTUAL TABLE threshold_spans USING timeless_traces;
         CREATE VIRTUAL TABLE txn_spans USING timeless_traces;
         CREATE VIRTUAL TABLE lifecycle_spans USING timeless_traces;
         CREATE VIRTUAL TABLE corrupt_spans USING timeless_traces;
         CREATE VIRTUAL TABLE percentile_spans USING timeless_traces;
         CREATE VIRTUAL TABLE v0_spans USING timeless_traces;",
    )?;
    let fixture = contract_fixture();
    for span in &fixture {
        insert_row(&connection, "row_spans", span)?;
    }
    connection.execute(
        "INSERT INTO batch_spans(batch_spans) VALUES (?1)",
        params![batch(&fixture, 2)?],
    )?;
    let row_spans = semantic_rows(&connection, "row_spans")?;
    let batch_spans = semantic_rows(&connection, "batch_spans")?;
    ensure!(row_spans == batch_spans);
    ensure!(batch_spans
        .iter()
        .all(|span| span.service == "contract-svc"));
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM batch_spans WHERE service='contract-svc'",
        [],
        |row| row.get(0),
    )?;
    ensure!(count == fixture.len() as i64);
    let explicit: i64 = connection.query_row(
        "SELECT COUNT(*) FROM batch_spans WHERE service='explicit-must-not-win'",
        [],
        |row| row.get(0),
    )?;
    ensure!(explicit == 0);
    let root = &batch_spans[0];
    ensure!(root.attributes["http.status_code"] == 503);
    ensure!(root.attributes["retryable"] == true);
    ensure!(root.attributes["score"] == 0.75);
    ensure!(root.status_description == "contract failure");
    ensure!(root.events[0]["timestamp"] == 1_700_000_000_040_000_000_i64);
    ensure!(root.events[0]["attributes"]["handled"] == false);
    ensure!(root.events[0]["attributes"]["exception.type"] == "ContractError");
    projection_contract(&connection, "batch_spans", &fixture, false)?;

    let percentile_cases = percentile_cases();
    let mut percentile_spans = Vec::new();
    let mut number = 100_000_u64;
    for (service, values) in &percentile_cases {
        for (position, duration) in values.iter().copied().enumerate() {
            percentile_spans.push(percentile_span(number, service, position, duration));
            number += 1;
        }
    }
    for spans in percentile_spans.chunks(8192) {
        connection.execute(
            "INSERT INTO percentile_spans(percentile_spans) VALUES (?1)",
            params![batch(spans, 2)?],
        )?;
    }
    percentile_contract(&connection, "percentile_spans", &percentile_cases)?;
    connection.execute(
        "INSERT INTO percentile_spans(percentile_spans) VALUES ('flush')",
        [],
    )?;
    connection.execute(
        "INSERT INTO percentile_spans(percentile_spans) VALUES ('optimize')",
        [],
    )?;
    percentile_contract(&connection, "percentile_spans", &percentile_cases)?;

    let mut bad = rich_span(4, None);
    bad.attributes = json!([]);
    ensure!(insert_row(&connection, "row_spans", &bad).is_err());
    ensure!(semantic_rows(&connection, "row_spans")?.len() == fixture.len());

    let mut legacy = rich_span(90, None);
    legacy.attributes = json!({"legacy":"string-only"});
    connection.execute(
        "INSERT INTO v0_spans(v0_spans) VALUES (?1)",
        params![batch(&[legacy], 1)?],
    )?;
    let legacy: (String, String, String, String, String) = connection.query_row(
        "SELECT attributes,status_description,events,resource,instrumentation_scope FROM v0_spans",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    ensure!(serde_json::from_str::<Value>(&legacy.0)? == json!({"legacy":"string-only"}));
    ensure!(legacy.1.is_empty() && legacy.2 == "[]" && legacy.3 == "{}" && legacy.4 == "{}");

    let threshold = (0..8191)
        .map(|index| rich_span(10_000 + index, None))
        .collect::<Vec<_>>();
    connection.execute(
        "INSERT INTO threshold_spans(threshold_spans) VALUES (?1)",
        params![batch(&threshold, 2)?],
    )?;
    let blocks: i64 =
        connection.query_row("SELECT COUNT(*) FROM threshold_spans_blocks", [], |row| {
            row.get(0)
        })?;
    ensure!(blocks == 0);
    insert_row(&connection, "threshold_spans", &rich_span(20_000, None))?;
    let blocks: i64 =
        connection.query_row("SELECT COUNT(*) FROM threshold_spans_blocks", [], |row| {
            row.get(0)
        })?;
    ensure!(blocks == 3);
    let count: i64 =
        connection.query_row("SELECT COUNT(*) FROM threshold_spans", [], |row| row.get(0))?;
    ensure!(count == 8192);

    let mut truncated = batch(&fixture, 2)?;
    truncated.pop();
    ensure!(connection
        .execute(
            "INSERT INTO txn_spans(txn_spans) VALUES (?1)",
            params![truncated]
        )
        .is_err());
    connection.execute("BEGIN", [])?;
    insert_row(&connection, "txn_spans", &rich_span(30_001, None))?;
    connection.execute("SAVEPOINT rich", [])?;
    connection.execute(
        "INSERT INTO txn_spans(txn_spans) VALUES (?1)",
        params![batch(&[rich_span(30_002, None)], 2)?],
    )?;
    connection.execute("ROLLBACK TO rich", [])?;
    connection.execute("RELEASE rich", [])?;
    connection.execute("COMMIT", [])?;
    ensure!(semantic_rows(&connection, "txn_spans")?[0].span_id == fixed_be::<8>(30_001));

    let old = rich_span(40_001, Some(100));
    let keep = rich_span(40_002, Some(200));
    connection.execute(
        "INSERT INTO lifecycle_spans(lifecycle_spans) VALUES (?1)",
        params![batch(&[old, keep.clone()], 2)?],
    )?;
    connection.execute(
        "INSERT INTO lifecycle_spans(lifecycle_spans) VALUES ('flush')",
        [],
    )?;
    connection.execute(
        "INSERT INTO lifecycle_spans(lifecycle_spans) VALUES ('optimize')",
        [],
    )?;
    ensure!(semantic_rows(&connection, "lifecycle_spans")?.len() == 2);
    connection.execute(
        "INSERT INTO lifecycle_spans(lifecycle_spans) VALUES ('prune:150')",
        [],
    )?;
    ensure!(semantic_rows(&connection, "lifecycle_spans")?[0].span_id == keep.span_id);

    let victim = rich_span(50_001, None);
    insert_row(&connection, "corrupt_spans", &victim)?;
    connection.execute(
        "INSERT INTO corrupt_spans(corrupt_spans) VALUES ('flush')",
        [],
    )?;
    let (block_id, payload): (i64, Vec<u8>) = connection.query_row(
        "SELECT id,data FROM corrupt_spans_blocks LIMIT 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    connection.execute(
        "UPDATE corrupt_spans_blocks SET data=?1 WHERE id=?2",
        params![&payload[..payload.len() - 1], block_id],
    )?;
    ensure!(connection
        .query_row::<i64, _, _>("SELECT COUNT(*) FROM corrupt_spans", [], |row| row.get(0))
        .is_err());
    connection.execute(
        "UPDATE corrupt_spans_blocks SET data=?1 WHERE id=?2",
        params![payload, block_id],
    )?;

    for table in ["row_spans", "batch_spans"] {
        connection.execute(
            &format!("INSERT INTO {table}({table}) VALUES ('flush')"),
            [],
        )?;
        connection.execute(
            &format!("INSERT INTO {table}({table}) VALUES ('optimize')"),
            [],
        )?;
    }
    projection_contract(&connection, "batch_spans", &fixture, true)?;
    percentile_contract(&connection, "percentile_spans", &percentile_cases)?;
    let expected_row = semantic_rows(&connection, "row_spans")?;
    let expected_batch = semantic_rows(&connection, "batch_spans")?;
    let expected_lifecycle = semantic_rows(&connection, "lifecycle_spans")?;
    drop(connection);

    let connection = open(extension, database)?;
    ensure!(semantic_rows(&connection, "row_spans")? == expected_row);
    ensure!(semantic_rows(&connection, "batch_spans")? == expected_batch);
    projection_contract(&connection, "batch_spans", &fixture, true)?;
    percentile_contract(&connection, "percentile_spans", &percentile_cases)?;
    ensure!(semantic_rows(&connection, "lifecycle_spans")? == expected_lifecycle);
    ensure!(semantic_rows(&connection, "corrupt_spans")?[0].attributes == victim.attributes);
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    ensure!(integrity == "ok");
    println!(
        "PASS: rich row/batch fidelity, threshold, transactions, maintenance, exact percentiles, corruption, reopen"
    );
    Ok(())
}
