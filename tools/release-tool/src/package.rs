use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::archive::deterministic_tar;
use crate::inventory::{Inventory, Target};

#[derive(Debug, Serialize)]
struct FileRecord {
    path: String,
    bytes: u64,
    sha256: String,
}

pub(crate) fn run(
    root: &Path,
    target_triple: String,
    output: Option<PathBuf>,
    allow_dirty: bool,
    force: bool,
) -> Result<()> {
    let inventory = Inventory::load()?;
    let target = inventory.target(&target_triple)?.clone();
    require_native_target(root, &target)?;

    let dirty = !git(root, ["status", "--porcelain"])?.is_empty();
    if dirty && !allow_dirty {
        bail!("refusing to package a dirty tree; commit the release session first");
    }
    let version = workspace_version(root)?;
    let commit = git(root, ["rev-parse", "HEAD"])?;
    let epoch = git(root, ["show", "-s", "--format=%ct", &commit])?
        .parse::<u64>()
        .context("parse source commit timestamp")?;
    let created = DateTime::<Utc>::from_timestamp(epoch as i64, 0)
        .context("source commit timestamp is out of range")?
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

    build_payloads(root, &target, &commit, epoch)?;

    let output = absolute_output(root, output.as_deref().unwrap_or(Path::new("dist")))?;
    let bundle_name = format!("timeless-telemetry-data-plane-{version}-{}", target.triple);
    fs::create_dir_all(&output)?;
    let final_dir = output.join(&bundle_name);
    let tar_path = output.join(format!("{bundle_name}.tar.gz"));
    if !force && (final_dir.exists() || tar_path.exists()) {
        bail!("artifact already exists: {bundle_name}; pass --force to replace it");
    }

    let temporary = tempfile::Builder::new()
        .prefix(&format!(".{bundle_name}."))
        .tempdir_in(&output)?;
    let stage = temporary.path().join(&bundle_name);
    stage_bundle(
        root, &stage, &inventory, &target, &version, &commit, epoch, &created, dirty,
    )?;

    if final_dir.exists() {
        fs::remove_dir_all(&final_dir)?;
    }
    if tar_path.exists() {
        fs::remove_file(&tar_path)?;
    }
    fs::rename(&stage, &final_dir)?;
    deterministic_tar(&final_dir, &tar_path, epoch)?;
    write_outer_checksums(&output)?;
    verify_bundle(&final_dir, &inventory, &commit, &target.triple)?;
    install_remove_drill(&final_dir, &inventory)?;

    println!(
        "{}",
        serde_json::to_string(&json!({
            "bundle": final_dir,
            "archive": tar_path,
            "sha256": sha256(&tar_path)?,
        }))?
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn stage_bundle(
    root: &Path,
    stage: &Path,
    inventory: &Inventory,
    target: &Target,
    version: &str,
    commit: &str,
    epoch: u64,
    created: &str,
    dirty: bool,
) -> Result<()> {
    for directory in [stage.join("bin"), stage.join("lib"), stage.join("licenses")] {
        fs::create_dir_all(directory)?;
    }
    copy_mode(
        &root.join("tools/install_release.sh"),
        &stage.join("install.sh"),
        0o755,
    )?;
    copy_mode(
        &root.join("tools/uninstall_release.sh"),
        &stage.join("uninstall.sh"),
        0o755,
    )?;
    let extension_name = format!("libtimeless_ext.{}", target.extension_suffix);
    let extension_source = root
        .join("target")
        .join(&target.triple)
        .join("release")
        .join(&extension_name);
    let extension_destination = stage.join("lib").join(&extension_name);
    copy_mode(&extension_source, &extension_destination, 0o644)?;

    let mut server_identities = BTreeMap::new();
    let server_root = root
        .join("servers/target")
        .join(&target.triple)
        .join("release");
    for binary in &inventory.binaries {
        let destination = stage.join("bin").join(binary);
        copy_mode(&server_root.join(binary), &destination, 0o755)?;
        let encoded = command_output(Command::new(&destination).arg("--version"))?;
        server_identities.insert(
            binary.clone(),
            serde_json::from_str::<Value>(&encoded)
                .with_context(|| format!("decode {binary} --version"))?,
        );
    }
    copy_mode(
        &root.join("LICENSE"),
        &stage.join("licenses/timeless-libsql-MIT.txt"),
        0o644,
    )?;

    let (sbom, notices) = crate::sbom::make(root, version, commit, &target.triple, created)?;
    json_write(&stage.join("SBOM.spdx.json"), &sbom)?;
    let mut notice = String::from(
        "Timeless telemetry data plane third-party license inventory\n\
         Generated from the two locked Cargo workspaces. Consult each upstream source for full terms.\n\n",
    );
    for item in notices {
        notice.push_str(&format!(
            "{} {} | {} | {}\n",
            item.name, item.version, item.license, item.source
        ));
    }
    fs::write(stage.join("THIRD_PARTY_LICENSES.txt"), notice)?;

    let files = payload_files(stage)?
        .into_iter()
        .map(|path| {
            Ok(FileRecord {
                path: relative_string(stage, &path)?,
                bytes: fs::metadata(&path)?.len(),
                sha256: sha256(&path)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let identity = json!({
        "format_version": 1,
        "product": "timeless-telemetry-data-plane",
        "version": version,
        "commit": commit,
        "dirty": dirty,
        "target": target.triple,
        "platform": target.platform,
        "source_date_epoch": epoch,
        "created": created,
        "servers": server_identities,
        "extension": extension_identity(&extension_destination)?,
        "files": files,
    });
    json_write(&stage.join("artifact-manifest.json"), &identity)?;

    let payloads = payload_files(stage)?;
    let mut checksums = String::new();
    for path in payloads {
        checksums.push_str(&format!(
            "{}  {}\n",
            sha256(&path)?,
            relative_string(stage, &path)?
        ));
    }
    fs::write(stage.join("SHA256SUMS"), checksums)?;
    Ok(())
}

fn build_payloads(root: &Path, target: &Target, commit: &str, epoch: u64) -> Result<()> {
    let existing_flags = env::var("RUSTFLAGS").unwrap_or_default();
    let remap = format!(
        "--remap-path-prefix={}=/src/timeless-libsql",
        root.display()
    );
    let flags = format!("{existing_flags} {remap}").trim().to_owned();
    let configure = |command: &mut Command| {
        command
            .current_dir(root)
            .env("TIMELESS_BUILD_COMMIT", commit)
            .env("SOURCE_DATE_EPOCH", epoch.to_string())
            .env("CARGO_INCREMENTAL", "0")
            .env("RUSTFLAGS", &flags)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
    };
    let mut extension = Command::new("cargo");
    extension.args([
        "build",
        "--locked",
        "--release",
        "--target",
        &target.triple,
        "-p",
        "timeless-ext",
    ]);
    configure(&mut extension);
    require_success(&mut extension, "build release extension")?;

    let mut servers = Command::new("cargo");
    servers.args([
        "build",
        "--manifest-path",
        "servers/Cargo.toml",
        "--locked",
        "--release",
        "--target",
        &target.triple,
        "--workspace",
    ]);
    configure(&mut servers);
    require_success(&mut servers, "build release servers")
}

fn require_native_target(root: &Path, target: &Target) -> Result<()> {
    let output = command_output(Command::new("rustc").arg("-vV").current_dir(root))?;
    let host = output
        .lines()
        .find_map(|line| line.strip_prefix("host:"))
        .map(str::trim)
        .context("rustc -vV did not report a host target")?;
    if host != target.triple {
        bail!(
            "release bundles must run natively for identity verification: host={host}, target={}",
            target.triple
        );
    }
    Ok(())
}

fn workspace_version(root: &Path) -> Result<String> {
    let document: toml::Value = toml::from_str(&fs::read_to_string(root.join("Cargo.toml"))?)?;
    document
        .get("workspace")
        .and_then(|value| value.get("package"))
        .and_then(|value| value.get("version"))
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .context("workspace.package.version is missing")
}

fn extension_identity(path: &Path) -> Result<Value> {
    let connection = Connection::open_in_memory()?;
    // SAFETY: the artifact is the just-built production extension at an
    // explicit native path, and the connection remains local to this probe.
    unsafe {
        connection.load_extension_enable()?;
        connection.load_extension(path, None::<&str>)?;
        connection.load_extension_disable()?;
    }
    let encoded: String =
        connection.query_row("SELECT timeless_capabilities()", [], |row| row.get(0))?;
    serde_json::from_str(&encoded).context("decode timeless_capabilities()")
}

fn verify_bundle(bundle: &Path, inventory: &Inventory, commit: &str, target: &str) -> Result<()> {
    let manifest: Value =
        serde_json::from_slice(&fs::read(bundle.join("artifact-manifest.json"))?)?;
    if manifest.get("commit").and_then(Value::as_str) != Some(commit)
        || manifest.get("target").and_then(Value::as_str) != Some(target)
    {
        bail!("artifact manifest identity differs from the requested build");
    }
    let checksum_text = fs::read_to_string(bundle.join("SHA256SUMS"))?;
    for (line_number, line) in checksum_text.lines().enumerate() {
        let (expected, relative) = line
            .split_once("  ")
            .with_context(|| format!("invalid SHA256SUMS line {}", line_number + 1))?;
        let path = bundle.join(relative);
        if sha256(&path)? != expected {
            bail!("checksum mismatch for {relative}");
        }
    }
    for binary in &inventory.binaries {
        let encoded =
            command_output(Command::new(bundle.join("bin").join(binary)).arg("--version"))?;
        let identity: Value = serde_json::from_str(&encoded)?;
        if identity.get("commit").and_then(Value::as_str) != Some(commit)
            || identity.get("target").and_then(Value::as_str) != Some(target)
        {
            bail!("{binary} identity differs from artifact manifest");
        }
    }
    Ok(())
}

fn install_remove_drill(bundle: &Path, inventory: &Inventory) -> Result<()> {
    let scratch_root = bundle
        .parent()
        .context("release bundle has no parent directory")?;
    let temporary = tempfile::Builder::new()
        .prefix("timeless-release-install-")
        .tempdir_in(scratch_root)
        .context("create install/remove drill directory beside the bundle")?;
    let prefix = temporary.path().join("prefix");
    fs::create_dir_all(prefix.join("data"))?;
    fs::create_dir_all(prefix.join("configuration"))?;
    fs::write(prefix.join("data/preserve"), b"data\n")?;
    fs::write(prefix.join("configuration/preserve"), b"config\n")?;

    require_success(
        Command::new(bundle.join("install.sh"))
            .arg("--prefix")
            .arg(&prefix),
        "install generated bundle",
    )?;
    for binary in &inventory.binaries {
        if !fs::symlink_metadata(prefix.join("bin").join(binary))?
            .file_type()
            .is_symlink()
        {
            bail!("installer did not create the {binary} symlink");
        }
    }
    require_success(
        Command::new(bundle.join("uninstall.sh"))
            .arg("--prefix")
            .arg(&prefix),
        "uninstall generated bundle",
    )?;
    if fs::read(prefix.join("data/preserve"))? != b"data\n"
        || fs::read(prefix.join("configuration/preserve"))? != b"config\n"
    {
        bail!("install/remove drill changed data or configuration sentinels");
    }
    Ok(())
}

fn write_outer_checksums(output: &Path) -> Result<()> {
    let mut archives = fs::read_dir(output)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| {
                    name.starts_with("timeless-telemetry-data-plane-") && name.ends_with(".tar.gz")
                })
        })
        .collect::<Vec<_>>();
    archives.sort();
    let mut checksums = String::new();
    for archive in archives {
        checksums.push_str(&format!(
            "{}  {}\n",
            sha256(&archive)?,
            archive.file_name().unwrap().to_string_lossy()
        ));
    }
    fs::write(output.join("SHA256SUMS"), checksums)?;
    Ok(())
}

fn payload_files(root: &Path) -> Result<Vec<PathBuf>> {
    fn visit(directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        let mut children = fs::read_dir(directory)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        children.sort();
        for child in children {
            if child.is_dir() {
                visit(&child, files)?;
            } else if child.is_file() {
                files.push(child);
            } else {
                bail!("unsupported staged payload type: {}", child.display());
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(root, &mut files)?;
    Ok(files)
}

fn relative_string(root: &Path, path: &Path) -> Result<String> {
    path.strip_prefix(root)?
        .to_str()
        .map(str::to_owned)
        .context("release payload path is not UTF-8")
}

fn copy_mode(source: &Path, destination: &Path, mode: u32) -> Result<()> {
    fs::copy(source, destination)
        .with_context(|| format!("copy {} to {}", source.display(), destination.display()))?;
    fs::set_permissions(destination, fs::Permissions::from_mode(mode))?;
    Ok(())
}

fn json_write(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut output = File::create(path)?;
    serde_json::to_writer_pretty(&mut output, value)?;
    output.write_all(b"\n")?;
    Ok(())
}

fn sha256(path: &Path) -> Result<String> {
    let mut input = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn git<const N: usize>(root: &Path, arguments: [&str; N]) -> Result<String> {
    command_output(Command::new("git").args(arguments).current_dir(root))
}

fn command_output(command: &mut Command) -> Result<String> {
    let description = format!("{command:?}");
    let output = command.output()?;
    if !output.status.success() {
        bail!(
            "{description} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|error| anyhow!("{description} returned non-UTF-8 output: {error}"))
}

fn require_success(command: &mut Command, operation: &str) -> Result<()> {
    let status = command.status()?;
    if !status.success() {
        bail!("{operation} failed with {status}");
    }
    Ok(())
}

fn absolute_output(root: &Path, output: &Path) -> Result<PathBuf> {
    let output = if output.is_absolute() {
        output.to_path_buf()
    } else {
        root.join(output)
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(output)
}
