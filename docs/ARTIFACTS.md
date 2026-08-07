# Release artifacts, installation, and removal

This document is the canonical inventory for the native telemetry data-plane
bundle produced by this repository. It describes the unreleased 0.4.x source
contract; it is not evidence that a tag, archive, container, or package has
been published. A release exists only when its immutable artifact and checksum
are present in the intended release channel and its commit is reachable from
`main`.

The standalone dbhealth extension is intentionally not in this bundle. Build
`dbhealth-ext` separately from compatible source when it is required.

## Checked native target matrix

<!-- public-artifact-targets:start -->

| Rust target | Platform | Extension file |
|---|---|---|
| `x86_64-unknown-linux-gnu` | Linux x86-64 GNU | `lib/libtimeless_ext.so` |
| `aarch64-unknown-linux-gnu` | Linux AArch64 GNU | `lib/libtimeless_ext.so` |
| `x86_64-apple-darwin` | macOS Intel | `lib/libtimeless_ext.dylib` |
| `aarch64-apple-darwin` | macOS Apple Silicon | `lib/libtimeless_ext.dylib` |

<!-- public-artifact-targets:end -->

Each target must be built and identity-checked natively. Windows, Linux musl,
and other targets may compile from source but are not claimed as checked
release artifacts. A GNU/Linux artifact also depends on a compatible runtime
libc; a target triple alone does not prove it will load in an older container.

## Archive and file inventory

The archive is named:

```text
timeless-telemetry-data-plane-<version>-<rust-target>.tar.gz
```

Its single top-level directory has the same name. These paths are exact and
are checked against the packager source:

<!-- public-artifact-files:start -->

| Archive path | Contract |
|---|---|
| `bin/timeless-metrics-api` | Signal-specific metrics HTTP/API owner. |
| `bin/timeless-logs-api` | Signal-specific logs HTTP/API owner. |
| `bin/timeless-traces-api` | Signal-specific traces HTTP/API owner. |
| `lib/libtimeless_ext.so` or `lib/libtimeless_ext.dylib` | Loadable production telemetry extension for the archive platform. |
| `install.sh` | Checksum-, identity-, and native-target-verifying installer. |
| `uninstall.sh` | Ownership-aware binary/artifact remover that preserves data. |
| `licenses/timeless-libsql-MIT.txt` | Timeless source license. |
| `SBOM.spdx.json` | SPDX 2.3 inventory derived from both locked Cargo workspaces. |
| `THIRD_PARTY_LICENSES.txt` | Package/version/license/source notice inventory. |
| `artifact-manifest.json` | Version, commit, target, build identities, capability document, and payload hashes. |
| `SHA256SUMS` | Checksums for every payload inside this archive. |

<!-- public-artifact-files:end -->

The distribution directory also contains a `SHA256SUMS` covering all complete
`.tar.gz` archives. The inner and outer files have different scopes; verify
the outer checksum before extraction and the inner checksum before install.

The bundle does not contain Phoenix, Elixir libraries, dashboards, UI,
Canvas, configuration, databases, migration sources, backups, service-manager
units, TLS keys, or auth policy. The three Rust binaries are independently
usable without those components; their exact routes and environment are in
the [server API reference](SERVER_API_REFERENCE.md).

## Building a candidate bundle

The current packager accepts exactly one native target and refuses a dirty
tree by default:

```sh
python3 tools/package_release.py \
  --target x86_64-unknown-linux-gnu \
  --output dist
```

It builds both locked Rust workspaces in release mode, embeds the exact Git
commit and target, executes every server's `--version`, loads the extension
and reads `timeless_capabilities()`, produces the SPDX/notice inventories,
normalizes archive ownership/mode/time metadata, and writes both checksum
levels. `--allow-dirty` is for local diagnostics only; a public artifact from
a dirty source tree is not a release candidate. `--force` replaces a local
candidate with the same name and must never be used to mutate an already
published release.

Because runtime identity includes the commit, package only after the intended
session is committed on `main`. Rebuild after any commit that changes source
identity. Before distribution, compare:

- archive checksum against the outer `SHA256SUMS`;
- `artifact-manifest.json` version/commit/target against the intended source;
- all three `--version` documents against that manifest;
- extension version, build identity, data ABI, SQL-surface version, signal
  batches, and required query guards against `timeless_capabilities()`; and
- the complete manifest file list and SPDX/license notices.

## Installing

Extract one native archive, verify its outer checksum, then run its installer:

```sh
tar -xzf timeless-telemetry-data-plane-0.4.0-x86_64-unknown-linux-gnu.tar.gz
cd timeless-telemetry-data-plane-0.4.0-x86_64-unknown-linux-gnu
sudo ./install.sh --prefix /opt/timeless
```

The installer re-verifies every inner checksum, parses the manifest identity,
rejects a target/host mismatch, executes all three binary identity probes, and
copies the complete bundle into the immutable directory:

```text
<prefix>/telemetry-data-plane/<version>-<target>-<commit>/
```

It then atomically points `<prefix>/bin/timeless-{metrics,logs,traces}-api`,
the platform extension link under `<prefix>/lib`, and
`<prefix>/telemetry-data-plane/CURRENT` at that directory. The default prefix
is `/opt/timeless`; pass an explicit writable prefix for an unprivileged
install.

Installation does not create, move, inspect, migrate, or remove data,
configuration, backups, retained legacy stores, service definitions, or
credentials. Configure database paths and policy separately, then perform the
capability/readiness preflight from the
[upgrade guide](UPGRADE.md#4-replace-and-start-in-dependency-order) before
admitting producers.

## Upgrading and rolling back artifacts

Keep the previous immutable release directory and a coordinated database
backup. Stop producers and the current owner normally, install the new bundle,
preflight it on a copy, start one signal owner, require readiness, and only
then resume producers. Changing the `CURRENT` symlink does not migrate or
validate a database by itself.

Artifact rollback and data rollback are separate operations. Restoring old
symlinks while leaving a database mutated by a newer incompatible owner is not
a safe downgrade. Follow the complete [upgrade and rollback procedure](UPGRADE.md)
and preserve both source identities in incident evidence.

## Removing an installed bundle

Run the `uninstall.sh` inside the immutable release directory so it can verify
the exact manifest-owned targets:

```sh
release_dir=$(sed -n '1p' /opt/timeless/telemetry-data-plane/CURRENT)
sudo "$release_dir/uninstall.sh" --prefix /opt/timeless --keep-artifact
```

`--keep-artifact` removes only symlinks and `CURRENT`; omit it to remove that
one immutable release directory too. The script removes a symlink only when
it still points to the release named by its own manifest. It never removes
telemetry databases, WAL/SHM files, backups, legacy rollback sources,
configuration, credentials, or unrelated files.

## Backup and restore boundary

The archive is executable software, not a data backup. The Rust signal
servers expose coordinated, no-overwrite backup routes that flush, maintain,
checkpoint, copy through SQLite's online-backup API, verify, fsync, and publish
the result. Direct SQLite/libSQL hosts must provide the equivalent ownership
and ordering. Restore is offline and operator-controlled. Exact commands and
failure rules are in the
[server API reference](SERVER_API_REFERENCE.md#flush-backup-restore-and-wal)
and [upgrade guide](UPGRADE.md#2-drain-and-create-the-rollback-point).
