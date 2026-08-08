use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use wait_timeout::ChildExt;

pub(super) fn run_and_kill(
    database: &Path,
    script: &Path,
    log: &Path,
    kill_after_ms: u64,
) -> Result<()> {
    ensure!(kill_after_ms > 0, "kill-after interval must be positive");
    let input =
        File::open(script).with_context(|| format!("open crash workload {}", script.display()))?;
    let output =
        File::create(log).with_context(|| format!("create crash log {}", log.display()))?;
    let mut child = Command::new("sqlite3")
        .arg(database)
        .stdin(Stdio::from(input))
        .stdout(Stdio::from(output))
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("start sqlite3 crash workload for {}", database.display()))?;

    if let Some(status) = child.wait_timeout(Duration::from_millis(kill_after_ms))? {
        bail!(
            "sqlite3 crash workload completed with {status} before the planned {kill_after_ms} ms SIGKILL; increase the generated rounds"
        );
    }

    // The child remains unreaped while we hold this handle, so its PID cannot
    // be reused between the bounded wait and Child::kill. This is the safety
    // property a shell sleep followed by `kill $pid` could not provide.
    child.kill().context("SIGKILL sqlite3 crash workload")?;
    let status = child.wait().context("reap sqlite3 crash workload")?;
    ensure!(
        !status.success(),
        "SIGKILLed sqlite3 crash workload unexpectedly exited successfully"
    );
    Ok(())
}

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
