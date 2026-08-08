use std::collections::BTreeSet;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct Target {
    pub(crate) triple: String,
    pub(crate) extension_suffix: String,
    pub(crate) platform: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Inventory {
    pub(crate) schema: u64,
    pub(crate) binaries: Vec<String>,
    pub(crate) targets: Vec<Target>,
    pub(crate) fixed_files: Vec<String>,
}

impl Inventory {
    pub(crate) fn load() -> Result<Self> {
        let inventory: Self = serde_json::from_str(include_str!("../artifact-inventory.json"))
            .context("decode artifact-inventory.json")?;
        inventory.validate()?;
        Ok(inventory)
    }

    pub(crate) fn target(&self, triple: &str) -> Result<&Target> {
        self.targets
            .iter()
            .find(|target| target.triple == triple)
            .with_context(|| {
                format!(
                    "unsupported target {triple}; expected one of {}",
                    self.targets
                        .iter()
                        .map(|target| target.triple.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
    }

    fn validate(&self) -> Result<()> {
        if self.schema != 1 {
            bail!("unsupported artifact inventory schema {}", self.schema);
        }
        if self.binaries.is_empty() || self.targets.is_empty() || self.fixed_files.is_empty() {
            bail!("artifact inventory cannot contain empty sections");
        }
        ensure_unique("binary", self.binaries.iter().map(String::as_str))?;
        ensure_unique(
            "target",
            self.targets.iter().map(|target| target.triple.as_str()),
        )?;
        ensure_unique("fixed file", self.fixed_files.iter().map(String::as_str))?;
        for target in &self.targets {
            if !matches!(target.extension_suffix.as_str(), "so" | "dylib") {
                bail!(
                    "target {} has unsupported extension suffix {}",
                    target.triple,
                    target.extension_suffix
                );
            }
            if !matches!(target.platform.as_str(), "linux" | "macos") {
                bail!(
                    "target {} has unsupported platform {}",
                    target.triple,
                    target.platform
                );
            }
        }
        Ok(())
    }
}

fn ensure_unique<'a>(kind: &str, values: impl Iterator<Item = &'a str>) -> Result<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        if value.is_empty() || !seen.insert(value) {
            bail!("artifact inventory contains an empty or duplicate {kind}: {value:?}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_inventory_is_valid_and_complete() {
        let inventory = Inventory::load().unwrap();
        assert_eq!(inventory.schema, 1);
        assert_eq!(inventory.targets.len(), 4);
        assert_eq!(inventory.binaries.len(), 3);
        assert_eq!(
            inventory
                .target("aarch64-apple-darwin")
                .unwrap()
                .extension_suffix,
            "dylib"
        );
        assert!(inventory
            .fixed_files
            .contains(&"artifact-manifest.json".to_owned()));
    }
}
