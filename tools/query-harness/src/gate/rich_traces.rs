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
    links: Value,
    trace_state: String,
    trace_flags: u32,
    dropped_attributes_count: u32,
    dropped_events_count: u32,
    dropped_links_count: u32,
    resource_schema_url: String,
    scope_schema_url: String,
    resource_dropped_attributes_count: u32,
    scope_dropped_attributes_count: u32,
}

#[derive(Clone, Debug, PartialEq)]
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
    links: Value,
    trace_state: String,
    trace_flags: i64,
    dropped_attributes_count: i64,
    dropped_events_count: i64,
    dropped_links_count: i64,
    resource_schema_url: String,
    scope_schema_url: String,
    resource_dropped_attributes_count: i64,
    scope_dropped_attributes_count: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct RetainedTraceSummary {
    span_rows: i64,
    distinct_span_ids: i64,
    error_rows: i64,
    start_ts: Option<i64>,
    end_ts: Option<i64>,
    duration_ns: Option<i64>,
    invalid_end_rows: i64,
    root_rows: i64,
    root_span_id: Option<String>,
    root_name: Option<String>,
    root_service: Option<String>,
    root_state: String,
    service_count: i64,
    completeness: String,
}

const COLUMNS: &str = "trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,duration_ns,attributes,status_description,events,resource,instrumentation_scope,links,trace_state,trace_flags,dropped_attributes_count,dropped_events_count,dropped_links_count,resource_schema_url,scope_schema_url,resource_dropped_attributes_count,scope_dropped_attributes_count";

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
        links: json!([{
            "trace_id": format!("{:032x}", number.saturating_add(1_000_000)),
            "span_id": format!("{:016x}", number.saturating_add(2_000_000)),
            "trace_state": "linked=yes",
            "attributes": {"reason": "retry"},
            "dropped_attributes_count": 6,
            "flags": 257
        }]),
        trace_state: format!("vendor={number}"),
        trace_flags: u32::MAX - number as u32,
        dropped_attributes_count: (number % 5) as u32,
        dropped_events_count: (number % 7) as u32,
        dropped_links_count: (number % 11) as u32,
        resource_schema_url: "https://example.test/resource/1".into(),
        scope_schema_url: "https://example.test/scope/2".into(),
        resource_dropped_attributes_count: 12,
        scope_dropped_attributes_count: 13,
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
            links: json!([{
                "trace_id":"ffeeddccbbaa99887766554433221100",
                "span_id":"8877665544332211",
                "trace_state":"link=state",
                "attributes":{"reason":"retry"},
                "dropped_attributes_count":9,
                "flags":257
            }]),
            trace_state: "vendor=contract".into(),
            trace_flags: u32::MAX,
            dropped_attributes_count: 3,
            dropped_events_count: 4,
            dropped_links_count: 5,
            resource_schema_url: "https://example.test/resource/1".into(),
            scope_schema_url: "https://example.test/scope/2".into(),
            resource_dropped_attributes_count: 6,
            scope_dropped_attributes_count: 7,
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
            links: json!([]),
            trace_state: String::new(),
            trace_flags: 1,
            dropped_attributes_count: 0,
            dropped_events_count: 0,
            dropped_links_count: 0,
            resource_schema_url: "https://example.test/resource/1".into(),
            scope_schema_url: "https://example.test/scope/2".into(),
            resource_dropped_attributes_count: 6,
            scope_dropped_attributes_count: 7,
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
        links: json!([]),
        trace_state: String::new(),
        trace_flags: 0,
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        resource_schema_url: String::new(),
        scope_schema_url: String::new(),
        resource_dropped_attributes_count: 0,
        scope_dropped_attributes_count: 0,
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
    ensure!(
        empty == 0,
        "empty percentile input unexpectedly emitted a bucket"
    );

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

fn u32_column(out: &mut Vec<u8>, values: impl Iterator<Item = u32>) {
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
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
    if version >= 2 {
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
    if version == 3 {
        for span in spans {
            framed(&mut out, serde_json::to_string(&span.links)?.as_bytes());
        }
        for span in spans {
            framed(&mut out, span.trace_state.as_bytes());
        }
        u32_column(&mut out, spans.iter().map(|span| span.trace_flags));
        u32_column(
            &mut out,
            spans.iter().map(|span| span.dropped_attributes_count),
        );
        u32_column(&mut out, spans.iter().map(|span| span.dropped_events_count));
        u32_column(&mut out, spans.iter().map(|span| span.dropped_links_count));
        for span in spans {
            framed(&mut out, span.resource_schema_url.as_bytes());
        }
        for span in spans {
            framed(&mut out, span.scope_schema_url.as_bytes());
        }
        u32_column(
            &mut out,
            spans
                .iter()
                .map(|span| span.resource_dropped_attributes_count),
        );
        u32_column(
            &mut out,
            spans.iter().map(|span| span.scope_dropped_attributes_count),
        );
    }
    Ok(out)
}

fn insert_row(connection: &Connection, table: &str, span: &Span) -> Result<()> {
    let sql = format!(
        "INSERT INTO \"{table}\"({COLUMNS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24)"
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
            serde_json::to_string(&span.links)?,
            span.trace_state,
            i64::from(span.trace_flags),
            i64::from(span.dropped_attributes_count),
            i64::from(span.dropped_events_count),
            i64::from(span.dropped_links_count),
            span.resource_schema_url,
            span.scope_schema_url,
            i64::from(span.resource_dropped_attributes_count),
            i64::from(span.scope_dropped_attributes_count),
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
            let links = serde_json::from_str(&row.get::<_, String>(14)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    14,
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
                links,
                trace_state: row.get(15)?,
                trace_flags: row.get(16)?,
                dropped_attributes_count: row.get(17)?,
                dropped_events_count: row.get(18)?,
                dropped_links_count: row.get(19)?,
                resource_schema_url: row.get(20)?,
                scope_schema_url: row.get(21)?,
                resource_dropped_attributes_count: row.get(22)?,
                scope_dropped_attributes_count: row.get(23)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn retained_trace_summary(
    connection: &Connection,
    table: &str,
    trace_id: &[u8; 16],
) -> Result<RetainedTraceSummary> {
    let sql = format!(
        "WITH retained AS (\
           SELECT span_id,parent_span_id,name,service,status,start_ts,duration_ns,\
                  CASE WHEN duration_ns>=0 \
                             AND start_ts<=9223372036854775807-duration_ns \
                       THEN start_ts+duration_ns END AS valid_end_ts \
             FROM \"{table}\" WHERE trace_id=?1\
         ) \
         SELECT count(*) AS span_rows,\
                count(DISTINCT span_id) AS distinct_span_ids,\
                count(*) FILTER (WHERE status='error') AS error_rows,\
                min(start_ts) AS start_ts,max(valid_end_ts) AS end_ts,\
                CASE WHEN count(*)=0 \
                           OR count(*) FILTER (WHERE valid_end_ts IS NULL)<>0 THEN NULL \
                     WHEN min(start_ts)>=0 THEN max(valid_end_ts)-min(start_ts) \
                     WHEN max(valid_end_ts)<=9223372036854775807+min(start_ts) \
                       THEN max(valid_end_ts)-min(start_ts) \
                     ELSE NULL END AS duration_ns,\
                count(*) FILTER (WHERE valid_end_ts IS NULL) AS invalid_end_rows,\
                count(*) FILTER (WHERE parent_span_id IS NULL) AS root_rows,\
                CASE WHEN count(*) FILTER (WHERE parent_span_id IS NULL)=1 \
                     THEN lower(hex(min(span_id) FILTER (WHERE parent_span_id IS NULL))) END,\
                CASE WHEN count(*) FILTER (WHERE parent_span_id IS NULL)=1 \
                     THEN min(name) FILTER (WHERE parent_span_id IS NULL) END,\
                CASE WHEN count(*) FILTER (WHERE parent_span_id IS NULL)=1 \
                     THEN min(service) FILTER (WHERE parent_span_id IS NULL) END,\
                CASE count(*) FILTER (WHERE parent_span_id IS NULL) \
                     WHEN 0 THEN 'missing' WHEN 1 THEN 'unique' \
                     ELSE 'ambiguous' END AS root_state,\
                count(DISTINCT service) AS service_count,\
                'unknown' AS completeness \
           FROM retained"
    );
    connection
        .query_row(&sql, [trace_id.as_slice()], |row| {
            Ok(RetainedTraceSummary {
                span_rows: row.get(0)?,
                distinct_span_ids: row.get(1)?,
                error_rows: row.get(2)?,
                start_ts: row.get(3)?,
                end_ts: row.get(4)?,
                duration_ns: row.get(5)?,
                invalid_end_rows: row.get(6)?,
                root_rows: row.get(7)?,
                root_span_id: row.get(8)?,
                root_name: row.get(9)?,
                root_service: row.get(10)?,
                root_state: row.get(11)?,
                service_count: row.get(12)?,
                completeness: row.get(13)?,
            })
        })
        .map_err(Into::into)
}

fn trace_summary_contract(connection: &Connection, table: &str) -> Result<RetainedTraceSummary> {
    let trace_id = hex("abababababababababababababababab");
    let mut root = rich_span(70_001, Some(100));
    root.trace_id = trace_id;
    root.span_id = hex("0101010101010101");
    root.parent_span_id = None;
    root.name = "root-original".into();
    root.status = 0;
    root.duration_ns = 50;
    root.attributes = json!({"service.name":"root-service"});

    connection.execute(
        &format!("INSERT INTO \"{table}\"(\"{table}\") VALUES (?1)"),
        params![batch(std::slice::from_ref(&root), 3)?],
    )?;
    connection.execute(
        &format!("INSERT INTO \"{table}\"(\"{table}\") VALUES ('flush')"),
        [],
    )?;
    ensure!(
        retained_trace_summary(connection, table, &trace_id)?
            == RetainedTraceSummary {
                span_rows: 1,
                distinct_span_ids: 1,
                error_rows: 0,
                start_ts: Some(100),
                end_ts: Some(150),
                duration_ns: Some(50),
                invalid_end_rows: 0,
                root_rows: 1,
                root_span_id: Some("0101010101010101".into()),
                root_name: Some("root-original".into()),
                root_service: Some("root-service".into()),
                root_state: "unique".into(),
                service_count: 1,
                completeness: "unknown".into(),
            }
    );

    // There is no idempotency identity in the current append-only contract.
    // A retry with the same trace/span ids is another retained row and makes
    // the root ambiguous instead of being silently deduplicated.
    let mut retry = root.clone();
    retry.name = "root-retry".into();
    retry.start_ts = 110;
    retry.duration_ns = 60;
    retry.attributes = json!({"service.name":"retry-service"});
    connection.execute(
        &format!("INSERT INTO \"{table}\"(\"{table}\") VALUES (?1)"),
        params![batch(&[retry], 3)?],
    )?;
    connection.execute(
        &format!("INSERT INTO \"{table}\"(\"{table}\") VALUES ('flush')"),
        [],
    )?;
    let retried = retained_trace_summary(connection, table, &trace_id)?;
    ensure!(retried.span_rows == 2);
    ensure!(retried.distinct_span_ids == 1);
    ensure!(retried.root_rows == 2 && retried.root_state == "ambiguous");
    ensure!(retried.root_span_id.is_none());
    ensure!(retried.root_name.is_none() && retried.root_service.is_none());
    ensure!(retried.completeness == "unknown");

    // A child may arrive in any later public batch. It changes the retained
    // envelope and counts but cannot prove that the source trace is complete.
    let mut child = rich_span(70_002, Some(200));
    child.trace_id = trace_id;
    child.span_id = hex("0202020202020202");
    child.parent_span_id = Some(root.span_id);
    child.name = "late-child".into();
    child.status = 2;
    child.duration_ns = 25;
    child.attributes = json!({"service.name":"child-service"});
    connection.execute(
        &format!("INSERT INTO \"{table}\"(\"{table}\") VALUES (?1)"),
        params![batch(std::slice::from_ref(&child), 3)?],
    )?;
    connection.execute(
        &format!("INSERT INTO \"{table}\"(\"{table}\") VALUES ('flush')"),
        [],
    )?;
    let complete_snapshot = RetainedTraceSummary {
        span_rows: 3,
        distinct_span_ids: 2,
        error_rows: 1,
        start_ts: Some(100),
        end_ts: Some(225),
        duration_ns: Some(125),
        invalid_end_rows: 0,
        root_rows: 2,
        root_span_id: None,
        root_name: None,
        root_service: None,
        root_state: "ambiguous".into(),
        service_count: 3,
        completeness: "unknown".into(),
    };
    ensure!(retained_trace_summary(connection, table, &trace_id)? == complete_snapshot);

    connection.execute_batch("BEGIN")?;
    let mut rolled_back = child.clone();
    rolled_back.span_id = hex("0303030303030303");
    rolled_back.start_ts = 250;
    insert_row(connection, table, &rolled_back)?;
    connection.execute_batch("ROLLBACK")?;
    ensure!(retained_trace_summary(connection, table, &trace_id)? == complete_snapshot);

    connection.execute(
        &format!("INSERT INTO \"{table}\"(\"{table}\") VALUES ('optimize')"),
        [],
    )?;
    ensure!(retained_trace_summary(connection, table, &trace_id)? == complete_snapshot);

    // Retention removes both old root rows while leaving the later child.
    // The retained snapshot is exact, but no query can tell whether the root
    // was source-missing or retention-truncated without unbounded history.
    connection.execute(
        &format!("INSERT INTO \"{table}\"(\"{table}\") VALUES ('prune:150')"),
        [],
    )?;
    let partial = RetainedTraceSummary {
        span_rows: 1,
        distinct_span_ids: 1,
        error_rows: 1,
        start_ts: Some(200),
        end_ts: Some(225),
        duration_ns: Some(25),
        invalid_end_rows: 0,
        root_rows: 0,
        root_span_id: None,
        root_name: None,
        root_service: None,
        root_state: "missing".into(),
        service_count: 1,
        completeness: "unknown".into(),
    };
    ensure!(retained_trace_summary(connection, table, &trace_id)? == partial);
    Ok(partial)
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

fn stat_value(values: &BTreeMap<String, i64>, key: &str) -> Result<i64> {
    values
        .get(key)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("timeless_stats omitted {key:?}"))
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
        ensure!(
            count == fixture.len(),
            "individual projection {column} lost rows"
        );
    }

    let mixed: (Vec<u8>, String, String, i64, String, i64) = connection.query_row(
        &format!(
            "SELECT trace_id,attributes,events,duration_ns,links,trace_flags FROM \"{table}\" \
             WHERE status='error' AND service='contract-svc'"
        ),
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;
    ensure!(mixed.0 == fixture[0].trace_id);
    ensure!(serde_json::from_str::<Value>(&mixed.1)? == fixture[0].attributes);
    ensure!(serde_json::from_str::<Value>(&mixed.2)? == fixture[0].events);
    ensure!(mixed.3 == fixture[0].duration_ns);
    ensure!(serde_json::from_str::<Value>(&mixed.4)? == fixture[0].links);
    ensure!(mixed.5 == i64::from(fixture[0].trace_flags));
    if !assert_selective_work {
        return Ok(());
    }

    let before = integer_stats(connection, table)?;
    let count: i64 =
        connection.query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |row| {
            row.get(0)
        })?;
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
    ensure!(stat_delta(&before, &after, "query_decoded_columns")? == payload_blocks * 2);
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
    ensure!(stat_delta(&before, &after, "query_decoded_columns")? == payload_blocks * 2 + 1);
    ensure!(stat_delta(&before, &after, "query_materialized_rich_values")? == 1);

    let before = after;
    let links: String = connection.query_row(
        &format!(
            "SELECT links FROM \"{table}\" \
             WHERE trace_id=?1 AND name='GET /contract'"
        ),
        [fixture[0].trace_id.as_slice()],
        |row| row.get(0),
    )?;
    ensure!(serde_json::from_str::<Value>(&links)? == fixture[0].links);
    let after = integer_stats(connection, table)?;
    let payload_blocks = stat_delta(&before, &after, "query_payload_blocks_read")?;
    ensure!(payload_blocks > 0);
    ensure!(stat_delta(&before, &after, "query_decoded_columns")? == payload_blocks * 2 + 10);
    ensure!(stat_delta(&before, &after, "query_materialized_rich_values")? == 10);

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

fn attribute_count(connection: &Connection, table: &str, filter: &str) -> Result<i64> {
    let sql = format!("SELECT count(*) FROM \"{table}\" WHERE attribute_filter=?1");
    connection
        .query_row(&sql, [filter], |row| row.get(0))
        .map_err(Into::into)
}

fn attribute_index_contract(connection: &Connection) -> Result<(Vec<SemanticSpan>, SemanticSpan)> {
    let table = "attribute_spans";
    let mut spans = Vec::new();
    for (offset, typed) in [
        None,
        Some(Value::Null),
        Some(json!("")),
        Some(json!("1")),
        Some(json!(1)),
        Some(json!(1.0)),
        Some(json!([1])),
        Some(json!({"x":1})),
    ]
    .into_iter()
    .enumerate()
    {
        let mut span = rich_span(80_000 + offset as u64, Some(1_000 + offset as i64));
        span.status = 1;
        span.attributes = json!({"http.method":"GET","service.name":"attribute-svc"});
        if let Some(value) = typed {
            span.attributes["typed"] = value;
        }
        span.resource = json!({"debug":false,"service.name":"attribute-resource"});
        span.instrumentation_scope = json!({"name":"attribute-scope"});
        spans.push(span);
    }
    connection.execute(
        &format!("INSERT INTO {table}({table}) VALUES (?1)"),
        params![batch(&spans, 3)?],
    )?;

    // Buffered rows already obey the complete typed contract before a filter
    // row can exist on disk.
    for (filter, expected) in [
        (r#"{"scope":"span","path":"/missing","value":null}"#, 0),
        (r#"{"scope":"span","path":"/typed","value":null}"#, 1),
        (r#"{"scope":"span","path":"/typed","value":""}"#, 1),
        (r#"{"scope":"span","path":"/typed","value":"1"}"#, 1),
        (r#"{"scope":"span","path":"/typed","value":1}"#, 1),
        (r#"{"scope":"span","path":"/typed","value":1.0}"#, 1),
        (r#"{"scope":"resource","path":"/debug","value":false}"#, 8),
        (
            r#"{"scope":"scope","path":"/name","value":"attribute-scope"}"#,
            8,
        ),
    ] {
        let result = attribute_count(connection, table, filter);
        if filter.contains("/missing") {
            // The missing path is intentionally not configured; querying an
            // unconfigured field fails instead of silently scanning.
            ensure!(result.is_err());
        } else {
            ensure!(result? == expected, "attribute filter mismatch: {filter}");
        }
    }
    for invalid in [
        r#"{"scope":"span","path":"/typed","value":[1]}"#,
        r#"{"scope":"span","path":"/typed","value":{"x":1}}"#,
        r#"{"scope":"event","path":"/typed","value":1}"#,
        r#"{"scope":"span","path":"/typed","value":1,"extra":true}"#,
    ] {
        ensure!(attribute_count(connection, table, invalid).is_err());
    }
    ensure!(connection
        .execute(
            &format!("INSERT INTO {table}(attribute_filter) VALUES (?1)"),
            [r#"{"scope":"span","path":"/typed","value":1}"#],
        )
        .is_err());

    connection.execute_batch("BEGIN")?;
    let mut rolled_back = rich_span(80_100, Some(2_000));
    rolled_back.status = 1;
    rolled_back.attributes = json!({"service.name":"attribute-svc","typed":"rollback"});
    insert_row(connection, table, &rolled_back)?;
    connection.execute(
        &format!("INSERT INTO {table}({table}) VALUES ('flush')"),
        [],
    )?;
    connection.execute_batch("ROLLBACK")?;
    ensure!(
        attribute_count(
            connection,
            table,
            r#"{"scope":"span","path":"/typed","value":"rollback"}"#
        )? == 0
    );

    connection.execute(
        &format!("INSERT INTO {table}({table}) VALUES ('flush')"),
        [],
    )?;
    let stats = integer_stats(connection, table)?;
    let blocks = stat_value(&stats, "blocks")?;
    ensure!(blocks == 1);
    ensure!(stat_value(&stats, "attribute_index_fields")? == 4);
    ensure!(stat_value(&stats, "attribute_bloom_rows")? == blocks * 4);
    ensure!(stat_value(&stats, "attribute_bloom_bytes")? == blocks * 4 * 4096);

    // Exercise exact composition with existing bounds, deterministic order,
    // and LIMIT. The hidden input is returned when explicitly selected.
    let filter = r#"{"scope":"span","path":"/typed","value":1}"#;
    let selected: (String, String) = connection.query_row(
        &format!(
            "SELECT lower(hex(span_id)),attribute_filter FROM {table} \
             WHERE start_ts>=1000 AND start_ts<=2000 AND attribute_filter=?1 \
             ORDER BY start_ts,span_id LIMIT 1"
        ),
        [filter],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    ensure!(selected.0 == format!("{:016x}", 80_004));
    ensure!(selected.1 == filter);

    connection.execute(
        &format!("INSERT INTO {table}({table}) VALUES ('optimize')"),
        [],
    )?;
    ensure!(attribute_count(connection, table, filter)? == 1);

    // A missing per-block filter row is the legacy compatibility state: it
    // must decode and recheck, never prune. Other configured fields remain.
    connection.execute(
        "DELETE FROM attribute_spans_attribute_blooms \
         WHERE scope='span' AND path='/typed'",
        [],
    )?;
    ensure!(attribute_count(connection, table, filter)? == 1);

    // Bit/metadata corruption fails closed. Restore the exact blob afterward
    // so the database remains a valid reopen fixture.
    let (block_id, bits): (i64, Vec<u8>) = connection.query_row(
        "SELECT block_id,bits FROM attribute_spans_attribute_blooms \
         WHERE scope='resource' AND path='/debug'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    connection.execute(
        "UPDATE attribute_spans_attribute_blooms SET hash_version=2 \
         WHERE scope='resource' AND path='/debug' AND block_id=?1",
        [block_id],
    )?;
    ensure!(attribute_count(
        connection,
        table,
        r#"{"scope":"resource","path":"/debug","value":false}"#
    )
    .is_err());
    connection.execute(
        "UPDATE attribute_spans_attribute_blooms SET hash_version=1 \
         WHERE scope='resource' AND path='/debug' AND block_id=?1",
        [block_id],
    )?;
    connection.execute(
        "UPDATE attribute_spans_attribute_blooms SET bits=zeroblob(1) \
         WHERE scope='resource' AND path='/debug' AND block_id=?1",
        [block_id],
    )?;
    ensure!(attribute_count(
        connection,
        table,
        r#"{"scope":"resource","path":"/debug","value":false}"#
    )
    .is_err());
    connection.execute(
        "UPDATE attribute_spans_attribute_blooms SET bits=?1 \
         WHERE scope='resource' AND path='/debug' AND block_id=?2",
        params![&bits, block_id],
    )?;
    connection.execute(
        "UPDATE attribute_spans_attribute_blooms SET bits=zeroblob(length(bits)) \
         WHERE scope='resource' AND path='/debug' AND block_id=?1",
        [block_id],
    )?;
    ensure!(attribute_count(
        connection,
        table,
        r#"{"scope":"resource","path":"/debug","value":false}"#
    )
    .is_err());
    connection.execute(
        "UPDATE attribute_spans_attribute_blooms SET bits=?1 \
         WHERE scope='resource' AND path='/debug' AND block_id=?2",
        params![bits, block_id],
    )?;

    // Retention removes filter rows in the same operation as their blocks.
    let mut old = rich_span(81_000, Some(100));
    old.status = 1;
    old.attributes = json!({"service.name":"attribute-lifecycle","typed":"old"});
    let mut keep = rich_span(81_001, Some(200));
    keep.status = 1;
    keep.attributes = json!({"service.name":"attribute-lifecycle","typed":"keep"});
    for span in [&old, &keep] {
        connection.execute(
            "INSERT INTO attribute_lifecycle(attribute_lifecycle) VALUES (?1)",
            params![batch(std::slice::from_ref(span), 3)?],
        )?;
        connection.execute(
            "INSERT INTO attribute_lifecycle(attribute_lifecycle) VALUES ('flush')",
            [],
        )?;
    }
    connection.execute(
        "INSERT INTO attribute_lifecycle(attribute_lifecycle) VALUES ('prune:150')",
        [],
    )?;
    ensure!(
        attribute_count(
            connection,
            "attribute_lifecycle",
            r#"{"scope":"span","path":"/typed","value":"old"}"#
        )? == 0
    );
    ensure!(
        attribute_count(
            connection,
            "attribute_lifecycle",
            r#"{"scope":"span","path":"/typed","value":"keep"}"#
        )? == 1
    );
    let lifecycle_rows: i64 = connection.query_row(
        "SELECT count(*) FROM attribute_lifecycle_attribute_blooms",
        [],
        |row| row.get(0),
    )?;
    ensure!(lifecycle_rows == 1);

    // More candidates than one metadata-query chunk proves that public
    // attribute filtering is not bounded by SQLite's variable limit and does
    // not require an all-candidate Bloom working set.
    for index in 0..257_u64 {
        let mut span = rich_span(82_000 + index, Some(10_000 + index as i64));
        span.status = 1;
        span.attributes = json!({
            "service.name":"attribute-chunks",
            "key": if index == 256 { "target".to_owned() } else { format!("miss-{index}") },
        });
        connection.execute(
            "INSERT INTO attribute_chunks(attribute_chunks) VALUES (?1)",
            params![batch(&[span], 3)?],
        )?;
        connection.execute(
            "INSERT INTO attribute_chunks(attribute_chunks) VALUES ('flush')",
            [],
        )?;
    }
    let before = integer_stats(connection, "attribute_chunks")?;
    ensure!(
        attribute_count(
            connection,
            "attribute_chunks",
            r#"{"scope":"span","path":"/key","value":"target"}"#,
        )? == 1
    );
    let after = integer_stats(connection, "attribute_chunks")?;
    ensure!(stat_value(&after, "blocks")? == 257);
    ensure!(stat_value(&after, "attribute_bloom_rows")? == 257);
    ensure!(stat_delta(&before, &after, "query_candidate_blocks")? == 1);
    ensure!(stat_delta(&before, &after, "query_payload_blocks_read")? == 1);

    Ok((
        semantic_rows(connection, table)?,
        semantic_rows(connection, "attribute_lifecycle")?[0].clone(),
    ))
}

pub(super) fn run(extension: &Path, database: &Path) -> Result<()> {
    let connection = open(extension, database)?;
    connection.execute_batch(
        r#"CREATE VIRTUAL TABLE row_spans USING timeless_traces;
         CREATE VIRTUAL TABLE batch_spans USING timeless_traces;
         CREATE VIRTUAL TABLE threshold_spans USING timeless_traces;
         CREATE VIRTUAL TABLE txn_spans USING timeless_traces;
         CREATE VIRTUAL TABLE lifecycle_spans USING timeless_traces;
         CREATE VIRTUAL TABLE corrupt_spans USING timeless_traces;
         CREATE VIRTUAL TABLE percentile_spans USING timeless_traces;
         CREATE VIRTUAL TABLE summary_spans USING timeless_traces;
         CREATE VIRTUAL TABLE attribute_spans USING timeless_traces(
           attribute_indexes='[{"scope":"span","path":"/typed"},{"scope":"span","path":"/http.method"},{"scope":"resource","path":"/debug"},{"scope":"scope","path":"/name"}]'
         );
         CREATE VIRTUAL TABLE attribute_lifecycle USING timeless_traces(
           attribute_indexes='[{"scope":"span","path":"/typed"}]'
         );
         CREATE VIRTUAL TABLE attribute_chunks USING timeless_traces(
           attribute_indexes='[{"scope":"span","path":"/key"}]'
         );
         CREATE VIRTUAL TABLE attribute_legacy USING timeless_traces;
         CREATE VIRTUAL TABLE v0_spans USING timeless_traces;
         CREATE VIRTUAL TABLE v1_spans USING timeless_traces;"#,
    )?;
    for invalid in [
        r#"CREATE VIRTUAL TABLE bad_attribute_scope USING timeless_traces(attribute_indexes='[{"scope":"event","path":"/x"}]')"#,
        r#"CREATE VIRTUAL TABLE bad_link_attribute_scope USING timeless_traces(attribute_indexes='[{"scope":"link","path":"/x"}]')"#,
        r#"CREATE VIRTUAL TABLE bad_attribute_path USING timeless_traces(attribute_indexes='[{"scope":"span","path":"/~2"}]')"#,
        r#"CREATE VIRTUAL TABLE duplicate_attribute_path USING timeless_traces(attribute_indexes='[{"scope":"span","path":"/x"},{"scope":"span","path":"/x"}]')"#,
        r#"CREATE VIRTUAL TABLE too_many_attribute_paths USING timeless_traces(attribute_indexes='[{"scope":"span","path":"/a"},{"scope":"span","path":"/b"},{"scope":"span","path":"/c"},{"scope":"span","path":"/d"},{"scope":"span","path":"/e"},{"scope":"span","path":"/f"},{"scope":"span","path":"/g"},{"scope":"span","path":"/h"},{"scope":"span","path":"/i"}]')"#,
    ] {
        ensure!(
            connection.execute(invalid, []).is_err(),
            "accepted {invalid}"
        );
    }
    let fixture = contract_fixture();
    for span in &fixture {
        insert_row(&connection, "row_spans", span)?;
    }
    connection.execute(
        "INSERT INTO batch_spans(batch_spans) VALUES (?1)",
        params![batch(&fixture, 3)?],
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
            params![batch(spans, 3)?],
        )?;
    }
    percentile_contract(&connection, "percentile_spans", &percentile_cases)?;
    let expected_summary = trace_summary_contract(&connection, "summary_spans")?;
    let (expected_attributes, expected_attribute_lifecycle) =
        attribute_index_contract(&connection)?;
    let mut legacy_attribute_span = rich_span(83_000, Some(30_000));
    legacy_attribute_span.status = 1;
    legacy_attribute_span.attributes = json!({"service.name":"attribute-legacy","key":"value"});
    insert_row(&connection, "attribute_legacy", &legacy_attribute_span)?;
    connection.execute(
        "INSERT INTO attribute_legacy(attribute_legacy) VALUES ('flush')",
        [],
    )?;
    connection.execute(
        "DELETE FROM attribute_legacy_meta WHERE k='attribute_indexes'",
        [],
    )?;
    connection.execute_batch("DROP TABLE attribute_legacy_attribute_blooms")?;
    let expected_attribute_legacy = semantic_rows(&connection, "attribute_legacy")?;
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
    connection.execute(
        "INSERT INTO v1_spans(v1_spans) VALUES (?1)",
        params![batch(&[rich_span(91, None)], 2)?],
    )?;
    let legacy_v2_defaults: i64 = connection.query_row(
        "SELECT count(*) FROM v1_spans WHERE links='[]' AND trace_state='' AND trace_flags=0
                AND dropped_attributes_count=0 AND dropped_events_count=0
                AND dropped_links_count=0 AND resource_schema_url='' AND scope_schema_url=''
                AND resource_dropped_attributes_count=0 AND scope_dropped_attributes_count=0",
        [],
        |row| row.get(0),
    )?;
    ensure!(legacy_v2_defaults == 1);

    let threshold = (0..8191)
        .map(|index| rich_span(10_000 + index, None))
        .collect::<Vec<_>>();
    connection.execute(
        "INSERT INTO threshold_spans(threshold_spans) VALUES (?1)",
        params![batch(&threshold, 3)?],
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

    let mut truncated = batch(&fixture, 3)?;
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
        params![batch(&[rich_span(30_002, None)], 3)?],
    )?;
    connection.execute("ROLLBACK TO rich", [])?;
    connection.execute("RELEASE rich", [])?;
    connection.execute("COMMIT", [])?;
    ensure!(semantic_rows(&connection, "txn_spans")?[0].span_id == fixed_be::<8>(30_001));

    let old = rich_span(40_001, Some(100));
    let keep = rich_span(40_002, Some(200));
    connection.execute(
        "INSERT INTO lifecycle_spans(lifecycle_spans) VALUES (?1)",
        params![batch(&[old, keep.clone()], 3)?],
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
    ensure!(semantic_rows(&connection, "attribute_spans")? == expected_attributes);
    ensure!(semantic_rows(&connection, "attribute_lifecycle")?[0] == expected_attribute_lifecycle);
    ensure!(
        attribute_count(
            &connection,
            "attribute_spans",
            r#"{"scope":"span","path":"/typed","value":1}"#
        )? == 1
    );
    ensure!(semantic_rows(&connection, "attribute_legacy")? == expected_attribute_legacy);
    ensure!(attribute_count(
        &connection,
        "attribute_legacy",
        r#"{"scope":"span","path":"/key","value":"value"}"#,
    )
    .is_err());
    let migrated_attribute_rows: i64 = connection.query_row(
        "SELECT count(*) FROM attribute_legacy_attribute_blooms",
        [],
        |row| row.get(0),
    )?;
    ensure!(migrated_attribute_rows == 0);
    ensure!(
        retained_trace_summary(
            &connection,
            "summary_spans",
            &hex("abababababababababababababababab")
        )? == expected_summary
    );
    ensure!(semantic_rows(&connection, "corrupt_spans")?[0].attributes == victim.attributes);
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    ensure!(integrity == "ok");
    println!(
        "PASS: rich row/batch fidelity, trace summaries, bounded attribute equality, threshold, transactions, maintenance, exact percentiles, corruption, reopen"
    );
    Ok(())
}
