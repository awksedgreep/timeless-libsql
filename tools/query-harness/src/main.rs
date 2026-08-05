mod contracts;
mod evidence;
mod gate;
mod oracle;
mod sql_equivalents;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "timeless-query-harness")]
#[command(about = "Rust-native query contracts, semantic oracles, SQL checks, and evidence")]
struct Cli {
    /// Repository root. Defaults to two directories above this crate.
    #[arg(long, global = true)]
    root: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate matrices, documentation links, test references, and server markers.
    Contracts,
    /// Validate or run immutable upstream query oracles.
    Oracle(oracle::OracleArgs),
    /// Capture public-API query performance and resource evidence.
    Evidence(evidence::EvidenceArgs),
    /// Run Rust-native extension fixtures and release-gate regressions.
    Gate(gate::GateArgs),
    /// Execute documented public SQL-equivalence recipes.
    Sql(sql_equivalents::SqlArgs),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let root = cli.root.unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("query harness must remain under tools/query-harness")
            .to_path_buf()
    });
    let root = root.canonicalize()?;

    match cli.command {
        Command::Contracts => contracts::run(&root),
        Command::Oracle(args) => oracle::run(&root, args),
        Command::Evidence(args) => evidence::run(&root, args),
        Command::Gate(args) => gate::run(&root, args),
        Command::Sql(args) => sql_equivalents::run(&root, args),
    }
}
