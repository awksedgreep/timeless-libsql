#!/usr/bin/env python3
"""Build one native, reproducible Timeless telemetry data-plane bundle."""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import pathlib
import shutil
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib


SUPPORTED_TARGETS = {
    "x86_64-unknown-linux-gnu": ("so", "linux"),
    "aarch64-unknown-linux-gnu": ("so", "linux"),
    "x86_64-apple-darwin": ("dylib", "macos"),
    "aarch64-apple-darwin": ("dylib", "macos"),
}
BINARIES = ("timeless-metrics-api", "timeless-logs-api", "timeless-traces-api")


def run(args: list[str], root: pathlib.Path, env: dict[str, str] | None = None) -> str:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    completed = subprocess.run(
        args,
        cwd=root,
        env=merged,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return completed.stdout.strip()


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_write(path: pathlib.Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def cargo_metadata(root: pathlib.Path, manifest: pathlib.Path | None = None) -> dict:
    command = ["cargo", "metadata", "--locked", "--format-version", "1"]
    if manifest:
        command += ["--manifest-path", str(manifest)]
    return json.loads(run(command, root))


def lock_checksums(lock_path: pathlib.Path) -> dict[tuple[str, str, str], str]:
    document = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    result: dict[tuple[str, str, str], str] = {}
    for package in document.get("package", []):
        source = package.get("source", "")
        checksum = package.get("checksum")
        if checksum:
            result[(package["name"], package["version"], source)] = checksum
    return result


def spdx_id(name: str, version: str, number: int) -> str:
    safe = "".join(character if character.isalnum() or character in ".-" else "-" for character in name)
    return f"SPDXRef-Package-{safe}-{version}-{number}"


def make_sbom(
    root: pathlib.Path,
    version: str,
    commit: str,
    target: str,
    created: str,
) -> tuple[dict, list[dict]]:
    workspaces = [
        (cargo_metadata(root), lock_checksums(root / "Cargo.lock")),
        (
            cargo_metadata(root, root / "servers" / "Cargo.toml"),
            lock_checksums(root / "servers" / "Cargo.lock"),
        ),
    ]
    unique: dict[tuple[str, str, str], dict] = {}
    checksums: dict[tuple[str, str, str], str] = {}
    for metadata, workspace_checksums in workspaces:
        checksums.update(workspace_checksums)
        for package in metadata["packages"]:
            key = (package["name"], package["version"], package.get("source") or "")
            unique[key] = package

    packages: list[dict] = []
    notices: list[dict] = []
    relationships: list[dict] = []
    for number, (key, package) in enumerate(sorted(unique.items()), start=1):
        name, package_version, source = key
        identifier = spdx_id(name, package_version, number)
        license_value = package.get("license") or "NOASSERTION"
        checksum = checksums.get(key)
        spdx_package = {
            "SPDXID": identifier,
            "name": name,
            "versionInfo": package_version,
            "downloadLocation": source or "NOASSERTION",
            "filesAnalyzed": False,
            "licenseConcluded": "NOASSERTION",
            "licenseDeclared": license_value,
            "copyrightText": "NOASSERTION",
        }
        if checksum:
            spdx_package["checksums"] = [{"algorithm": "SHA256", "checksumValue": checksum}]
        packages.append(spdx_package)
        notices.append(
            {
                "name": name,
                "version": package_version,
                "license": license_value,
                "source": source or "workspace",
            }
        )
        relationships.append(
            {
                "spdxElementId": "SPDXRef-DOCUMENT",
                "relationshipType": "DESCRIBES",
                "relatedSpdxElement": identifier,
            }
        )

    namespace = f"https://timeless.dev/spdx/telemetry-data-plane/{version}/{target}/{commit}"
    sbom = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": f"timeless-telemetry-data-plane-{version}-{target}",
        "documentNamespace": namespace,
        "creationInfo": {"created": created, "creators": ["Tool: timeless-package-release-1"]},
        "packages": packages,
        "relationships": relationships,
    }
    return sbom, notices


def extension_identity(path: pathlib.Path) -> dict:
    connection = sqlite3.connect(":memory:")
    try:
        connection.enable_load_extension(True)
        connection.load_extension(str(path))
        encoded = connection.execute("SELECT timeless_capabilities()").fetchone()[0]
        return json.loads(encoded)
    finally:
        connection.close()


def deterministic_tar(source: pathlib.Path, destination: pathlib.Path, epoch: int) -> None:
    prefix = source.name
    with destination.open("wb") as output:
        with gzip.GzipFile(filename="", mode="wb", fileobj=output, compresslevel=9, mtime=0) as zipped:
            with tarfile.open(fileobj=zipped, mode="w", format=tarfile.PAX_FORMAT) as archive:
                for path in [source, *sorted(source.rglob("*"))]:
                    relative = pathlib.Path(prefix) / path.relative_to(source)
                    info = archive.gettarinfo(str(path), arcname=str(relative))
                    info.uid = 0
                    info.gid = 0
                    info.uname = "root"
                    info.gname = "root"
                    info.mtime = epoch
                    info.pax_headers = {}
                    if info.isdir():
                        info.mode = 0o755
                        archive.addfile(info)
                    else:
                        executable = bool(path.stat().st_mode & stat.S_IXUSR)
                        info.mode = 0o755 if executable else 0o644
                        with path.open("rb") as contents:
                            archive.addfile(info, contents)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, choices=sorted(SUPPORTED_TARGETS))
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    root = pathlib.Path(__file__).resolve().parents[1]
    output = (args.output or root / "dist").resolve()
    target = args.target
    extension_suffix, platform = SUPPORTED_TARGETS[target]
    host = run(["rustc", "-vV"], root)
    host_target = next(line.split(":", 1)[1].strip() for line in host.splitlines() if line.startswith("host:"))
    if host_target != target:
        parser.error(f"release bundles must run natively for identity verification: host={host_target}, target={target}")

    dirty = bool(run(["git", "status", "--porcelain"], root))
    if dirty and not args.allow_dirty:
        parser.error("refusing to package a dirty tree; commit the release session first")

    workspace = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    version = workspace["workspace"]["package"]["version"]
    commit = run(["git", "rev-parse", "HEAD"], root)
    epoch = int(run(["git", "show", "-s", "--format=%ct", commit], root))
    created = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    environment = {
        "TIMELESS_BUILD_COMMIT": commit,
        "SOURCE_DATE_EPOCH": str(epoch),
        "CARGO_INCREMENTAL": "0",
        "RUSTFLAGS": (
            os.environ.get("RUSTFLAGS", "")
            + f" --remap-path-prefix={root}=/src/timeless-libsql"
        ).strip(),
    }

    run(
        ["cargo", "build", "--locked", "--release", "--target", target, "-p", "timeless-ext"],
        root,
        environment,
    )
    run(
        [
            "cargo",
            "build",
            "--manifest-path",
            "servers/Cargo.toml",
            "--locked",
            "--release",
            "--target",
            target,
            "--workspace",
        ],
        root,
        environment,
    )

    bundle_name = f"timeless-telemetry-data-plane-{version}-{target}"
    output.mkdir(parents=True, exist_ok=True)
    final_dir = output / bundle_name
    tar_path = output / f"{bundle_name}.tar.gz"
    if (final_dir.exists() or tar_path.exists()) and not args.force:
        parser.error(f"artifact already exists: {bundle_name}; pass --force to replace it")

    temporary = pathlib.Path(tempfile.mkdtemp(prefix=f".{bundle_name}.", dir=output))
    stage = temporary / bundle_name
    try:
        (stage / "bin").mkdir(parents=True)
        (stage / "lib").mkdir()
        (stage / "licenses").mkdir()
        shutil.copyfile(root / "tools" / "install_release.sh", stage / "install.sh")
        shutil.copyfile(root / "tools" / "uninstall_release.sh", stage / "uninstall.sh")
        os.chmod(stage / "install.sh", 0o755)
        os.chmod(stage / "uninstall.sh", 0o755)
        extension_source = root / "target" / target / "release" / f"libtimeless_ext.{extension_suffix}"
        extension_destination = stage / "lib" / extension_source.name
        shutil.copyfile(extension_source, extension_destination)
        os.chmod(extension_destination, 0o644)
        server_root = root / "servers" / "target" / target / "release"
        server_identities: dict[str, dict] = {}
        for binary in BINARIES:
            destination = stage / "bin" / binary
            shutil.copyfile(server_root / binary, destination)
            os.chmod(destination, 0o755)
            server_identities[binary] = json.loads(run([str(destination), "--version"], root))

        shutil.copyfile(root / "LICENSE", stage / "licenses" / "timeless-libsql-MIT.txt")
        sbom, notices = make_sbom(root, version, commit, target, created)
        json_write(stage / "SBOM.spdx.json", sbom)
        notice_lines = [
            "Timeless telemetry data plane third-party license inventory",
            "Generated from the two locked Cargo workspaces. Consult each upstream source for full terms.",
            "",
        ]
        notice_lines += [
            f"{item['name']} {item['version']} | {item['license']} | {item['source']}" for item in notices
        ]
        (stage / "THIRD_PARTY_LICENSES.txt").write_text("\n".join(notice_lines) + "\n", encoding="utf-8")

        identity = {
            "format_version": 1,
            "product": "timeless-telemetry-data-plane",
            "version": version,
            "commit": commit,
            "dirty": dirty,
            "target": target,
            "platform": platform,
            "source_date_epoch": epoch,
            "created": created,
            "servers": server_identities,
            "extension": extension_identity(extension_destination),
        }
        identity["files"] = [
            {
                "path": str(path.relative_to(stage)),
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
            for path in sorted(stage.rglob("*"))
            if path.is_file()
        ]
        json_write(stage / "artifact-manifest.json", identity)
        payloads = [path for path in sorted(stage.rglob("*")) if path.is_file()]
        (stage / "SHA256SUMS").write_text(
            "".join(f"{sha256(path)}  {path.relative_to(stage)}\n" for path in payloads),
            encoding="utf-8",
        )

        if final_dir.exists():
            shutil.rmtree(final_dir)
        if tar_path.exists():
            tar_path.unlink()
        os.replace(stage, final_dir)
        deterministic_tar(final_dir, tar_path, epoch)
        archives = sorted(output.glob("timeless-telemetry-data-plane-*.tar.gz"))
        (output / "SHA256SUMS").write_text(
            "".join(f"{sha256(path)}  {path.name}\n" for path in archives),
            encoding="utf-8",
        )
        print(json.dumps({"bundle": str(final_dir), "archive": str(tar_path), "sha256": sha256(tar_path)}))
        return 0
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
