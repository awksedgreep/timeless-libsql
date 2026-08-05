mod blobs;
mod cli;
mod correctness;
mod crash;
mod dbhealth;
mod rich_logs;
mod rich_traces;

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use rusqlite::Connection;

#[derive(Args, Debug)]
pub(crate) struct GateArgs {
    #[command(subcommand)]
    command: GateCommand,
}

#[derive(Subcommand, Debug)]
enum GateCommand {
    /// Generate a public batch fixture without a language-specific helper.
    Fixture {
        #[arg(value_enum)]
        kind: FixtureKind,
        #[arg(required = true)]
        output: Vec<PathBuf>,
    },
    /// Run one direct-SQL CLI section that needs a persistent host process.
    Cli {
        #[arg(value_enum)]
        section: CliSection,
        #[arg(long)]
        extension: PathBuf,
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        auxiliary: Vec<PathBuf>,
    },
    /// Run one focused extension correctness regression.
    Correctness {
        #[arg(value_enum)]
        section: CorrectnessSection,
        #[arg(long)]
        extension: PathBuf,
        #[arg(long)]
        temporary: PathBuf,
    },
    /// Emit the deterministic SQL workload consumed by the kill -9 gate.
    CrashSql {
        #[arg(long)]
        extension: PathBuf,
        #[arg(long, default_value_t = 3000)]
        rounds: usize,
        #[arg(long, default_value_t = 10)]
        metrics_per_round: usize,
        #[arg(long, default_value_t = 10)]
        logs_per_round: usize,
        #[arg(long, default_value_t = 10)]
        traces_per_round: usize,
    },
    /// Prove the standalone dbhealth scheduler lifecycle.
    Dbhealth {
        #[arg(long)]
        extension: PathBuf,
        #[arg(long)]
        database: PathBuf,
    },
    /// Internal child process used by the R2 multi-process regression.
    #[command(hide = true)]
    MetricsWorker {
        #[arg(long)]
        extension: PathBuf,
        #[arg(long)]
        database: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum FixtureKind {
    MetricsV0,
    MetricsV0Truncated,
    MetricsV0OutOfRange,
    LogsTracesV0,
    LogsV0Malformed,
    ResolvedMetricsV1,
    MetricsNanV0,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliSection {
    SharedEngine,
    PackedRollup,
    RichTraces,
    LatestPublication,
    CatalogPublication,
    MatcherDiscovery,
    ReaderGate,
    SeriesId,
    Frames,
    LogsOptimize,
    TraceReads,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CorrectnessSection {
    R1,
    R2,
    R3,
    R4,
    R8,
    LogsRich,
}

pub(crate) fn run(root: &Path, args: GateArgs) -> Result<()> {
    match args.command {
        GateCommand::Fixture { kind, output } => blobs::write(kind, &output),
        GateCommand::Cli {
            section,
            extension,
            database,
            auxiliary,
        } => cli::run(section, &extension, &database, &auxiliary),
        GateCommand::Correctness {
            section,
            extension,
            temporary,
        } => correctness::run(root, section, &extension, &temporary),
        GateCommand::CrashSql {
            extension,
            rounds,
            metrics_per_round,
            logs_per_round,
            traces_per_round,
        } => crash::write_sql(
            &extension,
            rounds,
            metrics_per_round,
            logs_per_round,
            traces_per_round,
        ),
        GateCommand::Dbhealth {
            extension,
            database,
        } => dbhealth::run(&extension, &database),
        GateCommand::MetricsWorker {
            extension,
            database,
        } => correctness::metrics_worker(&extension, &database),
    }
}

fn open(extension: &Path, database: &Path) -> Result<Connection> {
    let connection = Connection::open(database)
        .with_context(|| format!("open SQLite database {}", database.display()))?;
    connection.busy_timeout(std::time::Duration::from_secs(10))?;
    unsafe {
        connection.load_extension_enable()?;
        connection
            .load_extension(extension, None::<&str>)
            .with_context(|| format!("load extension {}", extension.display()))?;
        connection.load_extension_disable()?;
    }
    Ok(connection)
}

fn require_outputs(outputs: &[PathBuf], count: usize, kind: FixtureKind) -> Result<()> {
    if outputs.len() != count {
        bail!(
            "fixture {kind:?} needs {count} output path(s), received {}",
            outputs.len()
        );
    }
    Ok(())
}
