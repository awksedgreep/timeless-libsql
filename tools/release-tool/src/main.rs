mod archive;
mod inventory;
mod package;
mod sbom;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(about = "Build and verify one native Timeless release bundle")]
struct Args {
    /// Native Rust target triple from artifact-inventory.json.
    #[arg(long)]
    target: String,
    /// Distribution directory. Defaults to <repository>/dist.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Permit a diagnostic bundle from a dirty worktree.
    #[arg(long)]
    allow_dirty: bool,
    /// Replace a local bundle and archive with the same name.
    #[arg(long)]
    force: bool,
}

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("release tool is two directories below the repository root")
        .to_path_buf();
    let args = Args::parse();
    package::run(
        &root,
        args.target,
        args.output,
        args.allow_dirty,
        args.force,
    )
}
