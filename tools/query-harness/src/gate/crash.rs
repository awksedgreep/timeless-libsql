use std::io::{self, BufWriter, Write};
use std::path::Path;

use anyhow::Result;

pub(super) fn write_sql(
    extension: &Path,
    rounds: usize,
    metrics_per_round: usize,
    logs_per_round: usize,
    traces_per_round: usize,
) -> Result<()> {
    let mut out = BufWriter::new(io::stdout().lock());
    writeln!(out, ".load {}", extension.display())?;
    writeln!(
        out,
        "CREATE VIRTUAL TABLE metrics USING timeless_metrics(rollups='60s@0');"
    )?;
    writeln!(
        out,
        "CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service');"
    )?;
    writeln!(out, "CREATE VIRTUAL TABLE traces USING timeless_traces;")?;
    for round in 1..=rounds {
        writeln!(out, "BEGIN;")?;
        for index in 0..metrics_per_round {
            writeln!(
                out,
                "INSERT INTO metrics(name, ts, value, labels) VALUES ('m{}', {}, {}.{}, '{{\"host\":\"h{}\"}}');",
                index % 3,
                1_700_000_000_u64 + round as u64 * 100 + index as u64,
                round,
                index,
                index % 2
            )?;
        }
        for index in 0..logs_per_round {
            let level = if index % 4 == 3 { "error" } else { "info" };
            writeln!(
                out,
                "INSERT INTO logs(ts, level, message, service) VALUES ({}, '{level}', 'round {round} entry {index}', 'svc{}');",
                1_700_000_000_000_u64 + round as u64 * 1000 + index as u64,
                index % 2
            )?;
        }
        for index in 0..traces_per_round {
            let status = if index % 5 == 0 { "error" } else { "ok" };
            let timestamp = 1_700_000_000_000_000_000_u64 + round as u64 * 1_000_000 + index as u64;
            let description = if status == "error" { "crash-path" } else { "" };
            writeln!(
                out,
                "INSERT INTO traces(trace_id, span_id, name, service, status, start_ts, attributes, status_description, events, resource, instrumentation_scope)"
            )?;
            writeln!(
                out,
                "VALUES (x'{:032x}', x'{:016x}', 'op{}', 'must-not-win', '{status}', {timestamp},",
                round * 31 + index % 7,
                round * 1000 + index,
                index % 3
            )?;
            writeln!(
                out,
                "'{{\"bool\":true,\"count\":{index},\"service.name\":\"s{}\"}}', '{description}',",
                index % 2
            )?;
            writeln!(
                out,
                "'[{{\"attributes\":{{\"attempt\":{index},\"fatal\":false}},\"name\":\"checkpoint\",\"timestamp\":{}}}]',",
                timestamp + 1
            )?;
            writeln!(
                out,
                "'{{\"deployment.environment\":\"crash\",\"service.name\":\"resource-must-not-win\"}}',"
            )?;
            writeln!(
                out,
                "'{{\"attributes\":{{\"debug\":false}},\"name\":\"crash-lib\",\"version\":\"1.0\"}}');"
            )?;
        }
        writeln!(out, "INSERT INTO metrics(metrics) VALUES ('flush');")?;
        writeln!(out, "INSERT INTO logs(logs) VALUES ('flush');")?;
        writeln!(out, "INSERT INTO traces(traces) VALUES ('flush');")?;
        if round % 25 == 0 {
            writeln!(out, "INSERT INTO logs(logs) VALUES ('optimize');")?;
            writeln!(out, "INSERT INTO traces(traces) VALUES ('optimize');")?;
            writeln!(out, "INSERT INTO metrics(metrics) VALUES ('compact');")?;
            writeln!(out, "INSERT INTO metrics(metrics) VALUES ('rollup');")?;
        }
        writeln!(out, "COMMIT;")?;
        writeln!(out, "SELECT 'WM {round}';")?;
    }
    out.flush()?;
    Ok(())
}
