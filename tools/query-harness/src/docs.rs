use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{ensure, Context, Result};

fn sql_example(document: &str, id: &str) -> Result<String> {
    let start = format!("<!-- executable-doc:{id}:start -->");
    let end = format!("<!-- executable-doc:{id}:end -->");
    ensure!(
        document.matches(&start).count() == 1,
        "missing or duplicate {start}"
    );
    ensure!(
        document.matches(&end).count() == 1,
        "missing or duplicate {end}"
    );
    let marked = document
        .split_once(&start)
        .unwrap()
        .1
        .split_once(&end)
        .context("example end precedes start")?
        .0
        .trim();
    let sql = if let Some(sql) = marked.strip_prefix("```sql\n") {
        sql.strip_suffix("```").context("unclosed SQL fence")?
    } else {
        // The landing-page example wraps SQL in a copy/paste CLI command.
        let shell = marked
            .strip_prefix("```sh\n")
            .and_then(|body| body.strip_suffix("```"))
            .context("expected SQL or shell fence")?;
        let (command, sql) = shell.split_once('\n').context("missing heredoc")?;
        ensure!(
            command == "sqlite3 telemetry.db <<'SQL'",
            "unsupported CLI wrapper: {command}"
        );
        sql.strip_suffix("SQL\n").context("unclosed SQL heredoc")?
    };
    ensure!(!sql.trim().is_empty(), "empty SQL example {id}");
    Ok(sql.to_owned())
}

fn sqlite(root: &Path, database: &Path, sql: &str) -> Result<String> {
    let mut child = Command::new("sqlite3")
        .args(["-batch", "-bail", "-noheader"])
        .arg(database)
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("start extension-enabled sqlite3; see TESTING.md for macOS setup")?;
    let write = child.stdin.take().unwrap().write_all(sql.as_bytes());
    let output = child.wait_with_output()?;
    ensure!(
        output.status.success(),
        "documented SQL failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    write.context("write documented SQL to sqlite3")?;
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

pub(crate) fn run(root: &Path) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let examples = [
        (
            "README.md", "overview", Some(""),
            "SELECT (SELECT count(*) FROM metrics), (SELECT count(*) FROM logs), (SELECT count(*) FROM traces);",
            "0|0|0",
        ),
        (
            "README.md", "quickstart",
            Some(concat!(
                "cpu_usage|1753000000|42.5|{\"host\":\"web1\"}\n",
                "1753000000123000|error|payment declined\n",
                "4bf92f3577b34da6a3ce929d0e0e4736|GET /checkout|8500000"
            )),
            "SELECT (SELECT count(*) FROM metrics), (SELECT count(*) FROM logs), (SELECT count(*) FROM traces);",
            "1|1|1",
        ),
        (
            "docs/GUIDE.md", "metrics-tour",
            Some("cpu_usage|1753000015|43.1\ncpu_usage|1753000030|41.9\n41.9|42.5|43.1"),
            "SELECT count(*), min(value), avg(value), max(value) FROM metrics;",
            "3|41.9|42.5|43.1",
        ),
        (
            "docs/GUIDE.md", "dbhealth", None,
            "SELECT count(*) > 0 FROM dbhealth; SELECT count(*) > 0 FROM dbhealth_report;",
            "1\n1",
        ),
        (
            "docs/GUIDE.md", "cheat-sheet-setup", Some(""),
            "SELECT (SELECT count(*) FROM metrics), (SELECT count(*) FROM logs), (SELECT count(*) FROM traces); SELECT count(*) > 0 FROM dbhealth_report;",
            "0|0|0\n1",
        ),
    ];
    for (file, id, expected, reopened_sql, reopened_expected) in examples {
        let sql = sql_example(&fs::read_to_string(root.join(file))?, id)
            .with_context(|| format!("read {file} example {id}"))?;
        let database = directory.path().join(format!("{id}.db"));
        let output = sqlite(root, &database, &sql)
            .with_context(|| format!("execute {file} example {id}"))?;
        if let Some(expected) = expected {
            ensure!(
                output == expected,
                "{file} {id}: unexpected result: {output}"
            );
        }
        // Reuse the example's actual load commands: omitted or incorrect
        // paths must fail, including the separate health extension.
        let loads = sql
            .lines()
            .filter(|line| line.starts_with(".load "))
            .collect::<Vec<_>>()
            .join("\n");
        let reopened = sqlite(root, &database, &format!("{loads}\n{reopened_sql}\n"))?;
        ensure!(
            reopened == reopened_expected,
            "{file} {id}: cold reopen: {reopened}"
        );
        println!("documentation: {file} {id}: execute and cold reopen: ok");
    }
    Ok(())
}
