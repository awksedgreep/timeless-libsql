use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use anyhow::{bail, ensure, Context, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use super::super::open;

#[derive(Debug, Serialize, Deserialize)]
struct MetricWrite {
    name: String,
    timestamp: i64,
    value: f64,
    labels: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum WorkerCommand {
    Write(Vec<MetricWrite>),
    Counts(Vec<String>),
    Stop,
}

#[derive(Debug, Serialize, Deserialize)]
enum WorkerReply {
    Ready,
    Ok,
    Counts(BTreeMap<String, i64>),
    Error(String),
}

struct Worker {
    child: Child,
    input: BufWriter<ChildStdin>,
    output: BufReader<ChildStdout>,
}

impl Worker {
    fn start(extension: &Path, database: &Path) -> Result<(Self, WorkerReply)> {
        let executable = std::env::current_exe()?;
        let mut child = Command::new(executable)
            .args(["gate", "metrics-worker", "--extension"])
            .arg(extension)
            .arg("--database")
            .arg(database)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .context("spawn Rust metrics worker")?;
        let input = BufWriter::new(child.stdin.take().context("worker stdin")?);
        let mut output = BufReader::new(child.stdout.take().context("worker stdout")?);
        let reply = read_reply(&mut output)?;
        Ok((
            Self {
                child,
                input,
                output,
            },
            reply,
        ))
    }

    fn ask(&mut self, command: &WorkerCommand) -> Result<WorkerReply> {
        serde_json::to_writer(&mut self.input, command)?;
        self.input.write_all(b"\n")?;
        self.input.flush()?;
        read_reply(&mut self.output)
    }

    fn write(&mut self, rows: Vec<MetricWrite>) -> Result<()> {
        ensure!(matches!(
            self.ask(&WorkerCommand::Write(rows))?,
            WorkerReply::Ok
        ));
        Ok(())
    }

    fn counts(&mut self, names: &[&str]) -> Result<BTreeMap<String, i64>> {
        match self.ask(&WorkerCommand::Counts(
            names.iter().map(|name| (*name).to_owned()).collect(),
        ))? {
            WorkerReply::Counts(counts) => Ok(counts),
            other => bail!("unexpected counts reply: {other:?}"),
        }
    }

    fn stop(mut self) -> Result<()> {
        ensure!(matches!(self.ask(&WorkerCommand::Stop)?, WorkerReply::Ok));
        ensure!(
            self.child.wait()?.success(),
            "metrics worker exited unsuccessfully"
        );
        Ok(())
    }
}

fn read_reply(reader: &mut BufReader<ChildStdout>) -> Result<WorkerReply> {
    let mut line = String::new();
    ensure!(
        reader.read_line(&mut line)? != 0,
        "metrics worker closed without a reply"
    );
    Ok(serde_json::from_str(line.trim_end())?)
}

fn send_reply(reply: &WorkerReply) -> Result<()> {
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, reply)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

pub(super) fn worker(extension: &Path, database: &Path) -> Result<()> {
    let connection = match open(extension, database).and_then(|connection| {
        connection.query_row::<i64, _, _>("SELECT COUNT(*) FROM metrics", [], |row| row.get(0))?;
        Ok(connection)
    }) {
        Ok(connection) => connection,
        Err(error) => {
            send_reply(&WorkerReply::Error(format!("{error:#}")))?;
            return Ok(());
        }
    };
    send_reply(&WorkerReply::Ready)?;
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let command: WorkerCommand = serde_json::from_str(&line?)?;
        let reply = match command {
            WorkerCommand::Write(rows) => {
                let result = (|| -> Result<()> {
                    for row in rows {
                        connection.execute(
                            "INSERT INTO metrics(name,ts,value,labels) VALUES(?1,?2,?3,?4)",
                            params![row.name, row.timestamp, row.value, row.labels],
                        )?;
                    }
                    connection.execute("INSERT INTO metrics(metrics) VALUES('flush')", [])?;
                    Ok(())
                })();
                match result {
                    Ok(()) => WorkerReply::Ok,
                    Err(error) => WorkerReply::Error(format!("{error:#}")),
                }
            }
            WorkerCommand::Counts(names) => {
                let result = names
                    .into_iter()
                    .map(|name| {
                        let count = connection.query_row(
                            "SELECT COUNT(*) FROM metrics WHERE name=?1",
                            params![name],
                            |row| row.get(0),
                        )?;
                        Ok((name, count))
                    })
                    .collect::<rusqlite::Result<BTreeMap<_, _>>>();
                match result {
                    Ok(counts) => WorkerReply::Counts(counts),
                    Err(error) => WorkerReply::Error(error.to_string()),
                }
            }
            WorkerCommand::Stop => {
                send_reply(&WorkerReply::Ok)?;
                return Ok(());
            }
        };
        send_reply(&reply)?;
    }
    Ok(())
}

fn write(name: &str, timestamp: i64, value: f64, labels: &str) -> MetricWrite {
    MetricWrite {
        name: name.into(),
        timestamp,
        value,
        labels: labels.into(),
    }
}

fn initialize(extension: &Path, database: &Path) -> Result<()> {
    open(extension, database)?
        .execute("CREATE VIRTUAL TABLE metrics USING timeless_metrics", [])?;
    Ok(())
}

fn encode_legacy_registry(entries: &[(i64, &str, BTreeMap<&str, &str>)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for (series_id, name, labels) in entries {
        out.extend_from_slice(&series_id.to_be_bytes());
        out.extend_from_slice(&(name.len() as u16).to_be_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&(labels.len() as u16).to_be_bytes());
        for (key, value) in labels {
            out.extend_from_slice(&(key.len() as u16).to_be_bytes());
            out.extend_from_slice(key.as_bytes());
            out.extend_from_slice(&(value.len() as u16).to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
    }
    out
}

fn distinct_ids(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("distinct.db");
    initialize(extension, &database)?;
    let (mut a, ready_a) = Worker::start(extension, &database)?;
    let (mut b, ready_b) = Worker::start(extension, &database)?;
    ensure!(matches!(ready_a, WorkerReply::Ready) && matches!(ready_b, WorkerReply::Ready));
    a.write(vec![write("from_a", 10, 1.0, "{}")])?;
    b.write(vec![write("from_b", 20, 2.0, "{}")])?;
    a.stop()?;
    b.stop()?;
    let (mut reader, ready) = Worker::start(extension, &database)?;
    ensure!(matches!(ready, WorkerReply::Ready));
    ensure!(
        reader.counts(&["from_a", "from_b"])?
            == BTreeMap::from([("from_a".into(), 1), ("from_b".into(), 1)])
    );
    reader.stop()?;
    let plain = Connection::open(&database)?;
    let ids = plain
        .prepare("SELECT series_id,COUNT(*) FROM metrics_chunks GROUP BY series_id")?
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(ids.len() == 2);
    Ok(())
}

fn same_identity(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("same.db");
    initialize(extension, &database)?;
    let (mut a, ready_a) = Worker::start(extension, &database)?;
    let (mut b, ready_b) = Worker::start(extension, &database)?;
    ensure!(matches!(ready_a, WorkerReply::Ready) && matches!(ready_b, WorkerReply::Ready));
    a.write(vec![
        write("prefix", 10, 1.0, "{}"),
        write("shared", 11, 2.0, r#"{"host":"a"}"#),
    ])?;
    b.write(vec![write("shared", 12, 3.0, r#"{"host":"a"}"#)])?;
    a.stop()?;
    b.stop()?;
    let (mut reader, ready) = Worker::start(extension, &database)?;
    ensure!(matches!(ready, WorkerReply::Ready));
    ensure!(
        reader.counts(&["prefix", "shared"])?
            == BTreeMap::from([("prefix".into(), 1), ("shared".into(), 2)])
    );
    reader.stop()?;
    let plain = Connection::open(&database)?;
    let ids = plain
        .prepare("SELECT DISTINCT c.series_id FROM metrics_chunks c JOIN metrics_series s ON s.id=c.series_id WHERE s.name='shared'")?
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(ids.len() == 1);
    Ok(())
}

fn refresh(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("refresh.db");
    initialize(extension, &database)?;
    let (mut reader, reader_ready) = Worker::start(extension, &database)?;
    let (mut writer, writer_ready) = Worker::start(extension, &database)?;
    ensure!(
        matches!(reader_ready, WorkerReply::Ready) && matches!(writer_ready, WorkerReply::Ready)
    );
    ensure!(reader.counts(&["external"])?["external"] == 0);
    writer.write(vec![write("external", 30, 4.0, "{}")])?;
    ensure!(reader.counts(&["external"])?["external"] == 1);
    reader.stop()?;
    writer.stop()?;
    Ok(())
}

fn corrupt_legacy(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("corrupt.db");
    initialize(extension, &database)?;
    let (mut writer, ready) = Worker::start(extension, &database)?;
    ensure!(matches!(ready, WorkerReply::Ready));
    writer.write(vec![write("existing", 40, 5.0, "{}")])?;
    writer.stop()?;
    let plain = Connection::open(&database)?;
    plain.execute("DROP TABLE IF EXISTS metrics_series", [])?;
    plain.execute(
        "UPDATE metrics_meta SET v=?1 WHERE k='series_registry'",
        params![vec![0_u8]],
    )?;
    drop(plain);
    let (mut process, reply) = Worker::start(extension, &database)?;
    ensure!(
        matches!(reply, WorkerReply::Error(_)),
        "corrupt registry reopened: {reply:?}"
    );
    ensure!(process.child.wait()?.success());
    Ok(())
}

fn legacy_migrates(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("legacy.db");
    initialize(extension, &database)?;
    let (mut writer, ready) = Worker::start(extension, &database)?;
    ensure!(matches!(ready, WorkerReply::Ready));
    writer.write(vec![write(
        "legacy_metric",
        50,
        6.0,
        r#"{"host":"legacy"}"#,
    )])?;
    writer.stop()?;
    let plain = Connection::open(&database)?;
    let series_id: i64 =
        plain.query_row("SELECT DISTINCT series_id FROM metrics_chunks", [], |row| {
            row.get(0)
        })?;
    let registry = encode_legacy_registry(&[(
        series_id,
        "legacy_metric",
        BTreeMap::from([("host", "legacy")]),
    )]);
    plain.execute("DROP TABLE IF EXISTS metrics_series", [])?;
    plain.execute(
        "INSERT OR REPLACE INTO metrics_meta(k,v) VALUES('series_registry',?1)",
        params![registry],
    )?;
    drop(plain);
    let (mut reader, ready) = Worker::start(extension, &database)?;
    ensure!(matches!(ready, WorkerReply::Ready));
    ensure!(reader.counts(&["legacy_metric"])?["legacy_metric"] == 1);
    reader.stop()?;
    let plain = Connection::open(&database)?;
    let rows = plain
        .prepare("SELECT id,name FROM metrics_series WHERE name='legacy_metric'")?
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(rows == [(series_id, "legacy_metric".into())]);
    Ok(())
}

fn catalog_transactions(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("catalog_tx.db");
    initialize(extension, &database)?;
    let connection = open(extension, &database)?;
    connection.execute_batch(
        "BEGIN;
         INSERT INTO metrics(name,ts,value,labels) VALUES('rolled',60,7,'{}');
         ROLLBACK;
         INSERT INTO metrics(name,ts,value,labels) VALUES('replacement',61,8,'{}');
         INSERT INTO metrics(name,ts,value,labels) VALUES('rolled',62,9,'{}');
         INSERT INTO metrics(metrics) VALUES('flush');
         BEGIN;
         SAVEPOINT catalog_sp;
         INSERT INTO metrics(name,ts,value,labels) VALUES('savepoint_rolled',63,10,'{}');
         ROLLBACK TO catalog_sp;
         RELEASE catalog_sp;
         INSERT INTO metrics(name,ts,value,labels) VALUES('savepoint_rolled',64,11,'{}');
         COMMIT;
         INSERT INTO metrics(metrics) VALUES('flush');",
    )?;
    for name in ["replacement", "rolled", "savepoint_rolled"] {
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM metrics WHERE name=?1",
            params![name],
            |row| row.get(0),
        )?;
        ensure!(count == 1, "{name} count was {count}");
    }
    let ids = connection
        .prepare("SELECT id FROM metrics_series WHERE name IN ('replacement','rolled','savepoint_rolled')")?
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<rusqlite::Result<BTreeSet<_>>>()?;
    ensure!(ids.len() == 3);
    Ok(())
}

pub(super) fn run(_root: &Path, extension: &Path, temporary: &Path) -> Result<()> {
    type CorrectnessTest = (&'static str, fn(&Path, &Path) -> Result<()>);
    for round in 0..3 {
        let round_directory = temporary.join(format!("round-{round}"));
        std::fs::create_dir(&round_directory)?;
        let tests: [CorrectnessTest; 6] = [
            ("test_distinct_series_ids", distinct_ids),
            ("test_same_identity_converges", same_identity),
            ("test_long_lived_reader_refreshes", refresh),
            ("test_corrupt_legacy_registry_fails_closed", corrupt_legacy),
            ("test_legacy_registry_migrates", legacy_migrates),
            (
                "test_catalog_rows_follow_transactions",
                catalog_transactions,
            ),
        ];
        for (name, test) in tests {
            test(extension, &round_directory)?;
            println!("PASS round {}: {name}", round + 1);
        }
    }
    Ok(())
}
