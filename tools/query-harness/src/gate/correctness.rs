mod r1;
mod r2;
mod r3;
mod r4;
mod r8;

use std::path::Path;

use anyhow::Result;

use super::CorrectnessSection;

pub(super) fn run(
    root: &Path,
    section: CorrectnessSection,
    extension: &Path,
    temporary: &Path,
) -> Result<()> {
    match section {
        CorrectnessSection::R1 => r1::run(extension, temporary),
        CorrectnessSection::R2 => r2::run(root, extension, temporary),
        CorrectnessSection::R3 => r3::run(extension, temporary),
        CorrectnessSection::R4 => r4::run(extension, temporary),
        CorrectnessSection::R8 => r8::run(extension, temporary),
        CorrectnessSection::LogsRich => {
            super::rich_logs::run(extension, &temporary.join("logs-rich.db"))
        }
    }
}

pub(super) fn metrics_worker(extension: &Path, database: &Path) -> Result<()> {
    r2::worker(extension, database)
}
