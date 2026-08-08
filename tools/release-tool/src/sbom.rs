use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<MetadataPackage>,
}

#[derive(Clone, Debug, Deserialize)]
struct MetadataPackage {
    name: String,
    version: String,
    source: Option<String>,
    license: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Lockfile {
    package: Vec<LockedPackage>,
}

#[derive(Debug, Deserialize)]
struct LockedPackage {
    name: String,
    version: String,
    #[serde(default)]
    source: String,
    checksum: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Notice {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) license: String,
    pub(crate) source: String,
}

pub(crate) fn make(
    root: &Path,
    version: &str,
    commit: &str,
    target: &str,
    created: &str,
) -> Result<(Value, Vec<Notice>)> {
    let workspaces = [
        (
            cargo_metadata(root, None)?,
            lock_checksums(&root.join("Cargo.lock"))?,
        ),
        (
            cargo_metadata(root, Some(&root.join("servers/Cargo.toml")))?,
            lock_checksums(&root.join("servers/Cargo.lock"))?,
        ),
    ];
    let mut unique = BTreeMap::new();
    let mut checksums = BTreeMap::new();
    for (metadata, workspace_checksums) in workspaces {
        checksums.extend(workspace_checksums);
        for package in metadata.packages {
            let key = (
                package.name.clone(),
                package.version.clone(),
                package.source.clone().unwrap_or_default(),
            );
            unique.insert(key, package);
        }
    }

    let mut packages = Vec::new();
    let mut notices = Vec::new();
    let mut relationships = Vec::new();
    for (number, (key, package)) in unique.into_iter().enumerate() {
        let (name, package_version, source) = key;
        let identifier = spdx_id(&name, &package_version, number + 1);
        let license = package.license.unwrap_or_else(|| "NOASSERTION".to_owned());
        let mut value = json!({
            "SPDXID": identifier,
            "name": name,
            "versionInfo": package_version,
            "downloadLocation": if source.is_empty() { "NOASSERTION" } else { &source },
            "filesAnalyzed": false,
            "licenseConcluded": "NOASSERTION",
            "licenseDeclared": license,
            "copyrightText": "NOASSERTION",
        });
        if let Some(checksum) =
            checksums.get(&(name.clone(), package_version.clone(), source.clone()))
        {
            value["checksums"] = json!([{"algorithm": "SHA256", "checksumValue": checksum}]);
        }
        packages.push(value);
        notices.push(Notice {
            name,
            version: package_version,
            license,
            source: if source.is_empty() {
                "workspace".to_owned()
            } else {
                source
            },
        });
        relationships.push(json!({
            "spdxElementId": "SPDXRef-DOCUMENT",
            "relationshipType": "DESCRIBES",
            "relatedSpdxElement": identifier,
        }));
    }

    Ok((
        json!({
            "spdxVersion": "SPDX-2.3",
            "dataLicense": "CC0-1.0",
            "SPDXID": "SPDXRef-DOCUMENT",
            "name": format!("timeless-telemetry-data-plane-{version}-{target}"),
            "documentNamespace": format!("https://timeless.dev/spdx/telemetry-data-plane/{version}/{target}/{commit}"),
            "creationInfo": {"created": created, "creators": ["Tool: timeless-release-tool-1"]},
            "packages": packages,
            "relationships": relationships,
        }),
        notices,
    ))
}

fn cargo_metadata(root: &Path, manifest: Option<&Path>) -> Result<CargoMetadata> {
    let mut command = Command::new("cargo");
    command.args(["metadata", "--locked", "--format-version", "1"]);
    if let Some(manifest) = manifest {
        command.arg("--manifest-path").arg(manifest);
    }
    let output = command.current_dir(root).output()?;
    if !output.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    serde_json::from_slice(&output.stdout).context("decode cargo metadata")
}

fn lock_checksums(path: &Path) -> Result<BTreeMap<(String, String, String), String>> {
    let lock: Lockfile = toml::from_str(&std::fs::read_to_string(path)?)?;
    Ok(lock
        .package
        .into_iter()
        .filter_map(|package| {
            package
                .checksum
                .map(|checksum| ((package.name, package.version, package.source), checksum))
        })
        .collect())
}

fn spdx_id(name: &str, version: &str, number: usize) -> String {
    let safe = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("SPDXRef-Package-{safe}-{version}-{number}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spdx_identifiers_are_stable_and_safe() {
        assert_eq!(
            spdx_id("timeless ext/sys", "0.4.0", 7),
            "SPDXRef-Package-timeless-ext-sys-0.4.0-7"
        );
    }

    #[test]
    fn cargo_lock_checksum_keys_include_source() {
        let temporary = tempfile::Builder::new()
            .prefix("sbom-test-")
            .tempdir_in(Path::new(env!("CARGO_MANIFEST_DIR")).join("target"))
            .unwrap();
        let path = temporary.path().join("Cargo.lock");
        std::fs::write(
            &path,
            r#"[[package]]
name = "crate"
version = "1.2.3"
source = "registry+https://example.invalid"
checksum = "abc"

[[package]]
name = "workspace"
version = "0.1.0"
"#,
        )
        .unwrap();
        let checksums = lock_checksums(&path).unwrap();
        assert_eq!(
            checksums.get(&(
                "crate".to_owned(),
                "1.2.3".to_owned(),
                "registry+https://example.invalid".to_owned()
            )),
            Some(&"abc".to_owned())
        );
        assert_eq!(checksums.len(), 1);
    }
}
