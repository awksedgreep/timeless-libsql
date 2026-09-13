# Release artifacts, installation, and removal

This document is the canonical inventory for the native telemetry data-plane
bundle produced by this repository. A source tag, target row, workflow run,
or locally generated candidate is not evidence that a complete release was
published. A native release exists only when its immutable archives and outer
checksum set are present in the intended release channel and its commit is
reachable from `main`.

## Current publication status

[`v0.8.4`](https://github.com/awksedgreep/timeless-libsql/releases/tag/v0.8.4)
was published on 2026-09-13 at 21:58:31 UTC from `main` commit
`3453ec77436fde6eb09f1a603ce976227203e6eb`. It contains the correctness and
onboarding review fixes, configurable OpenTelemetry export, and dbhealth
schema fixes described in the [changelog](../CHANGELOG.md).

The [release workflow](https://github.com/awksedgreep/timeless-libsql/actions/runs/34784815700)
built, identity-checked, and install/remove-drilled all four native Linux/macOS
archives, verified the complete outer `SHA256SUMS`, and published the four
archives plus that checksum file as permanent release assets. All assets were
downloaded and independently verified after publication; the published macOS
installation also passed all-signal ingest/query, restart, backup/restore,
and data-preserving removal checks. See the [release validation record](RELEASE_0_8_4.md)
for the complete post-bump local checklist, pre-tag CI, and archive hashes.

This is the current download channel. Complete published releases also exist
for `v0.8.3`, `v0.8.2`, `v0.8.1`, `v0.8.0`, `v0.7.9`, `v0.7.8`, `v0.7.7`,
`v0.7.6`, `v0.7.5` back through `v0.7.1`, `v0.6.4`, `v0.6.2`, `v0.6.1`,
`v0.6.0`, `v0.5.0`, and `v0.4.2`.

Documentation on `main` also covers changes in the
[Unreleased changelog](../CHANGELOG.md#unreleased). Those changes require a
source build and are not yet part of the published bundle. For instructions
matching a downloaded version exactly, use the documentation at that release's
tag. The [compatibility contract](COMPATIBILITY.md) records current source
versions and pairing floors separately from publication status.

For history: some tags record source only, because their artifact runs failed
and the fix became the next patch — `v0.7.0` (authctl missed the `--version`
identity contract), `v0.6.3` (version bump without refreshed lockfiles under
`--locked`), and `v0.4.0` (the release tool linked Apple's restricted system
SQLite, so both macOS jobs failed). `v0.4.1` passed all four package jobs and
the outer checksum gate but predates Release publication; `v0.4.2` published
this repository's first GitHub Release, proving the tag→Release path.

The standalone dbhealth extension is intentionally not in this bundle. Build
`dbhealth-ext` separately from compatible source when it is required.

## Intended native target matrix

<!-- public-artifact-targets:start -->

| Rust target | Platform | Extension file |
|---|---|---|
| `x86_64-unknown-linux-gnu` | Linux x86-64 GNU | `lib/libtimeless_ext.so` |
| `aarch64-unknown-linux-gnu` | Linux AArch64 GNU | `lib/libtimeless_ext.so` |
| `x86_64-apple-darwin` | macOS Intel | `lib/libtimeless_ext.dylib` |
| `aarch64-apple-darwin` | macOS Apple Silicon | `lib/libtimeless_ext.dylib` |

<!-- public-artifact-targets:end -->

Each target must be built and identity-checked natively before the matrix is
published. The table is the packager contract, not the current publication
result. Windows, Linux musl, and other targets may compile from source but are
not claimed as release artifacts. A GNU/Linux artifact also depends on a
compatible runtime libc; a target triple alone does not prove it will load in
an older container.

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
| `bin/timeless-authctl` | Key generation, policy scaffolding, and token minting for opt-in auth. |
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
cargo run --release --manifest-path tools/release-tool/Cargo.toml \
  --locked -- \
  --target x86_64-unknown-linux-gnu \
  --output dist
```

It builds both locked Rust workspaces in release mode, embeds the exact Git
commit and target, executes every server's `--version`, loads the extension
and reads `timeless_capabilities()`, produces the SPDX/notice inventories,
normalizes archive ownership/mode/time metadata, and writes both checksum
levels. Before returning success it verifies the inner hashes and identities,
then installs and removes the candidate under an isolated temporary prefix and
proves data/configuration sentinels survive. `--allow-dirty` is for local
diagnostics only; a public artifact from a dirty source tree is not a release
candidate. `--force` replaces a local candidate with the same name and must
never be used to mutate an already published release.

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

The following Bash commands require `curl`, `tar`, and either `sha256sum`
(Linux) or `shasum` (macOS). No GitHub account is needed. They select the host's
native archive, download it and the outer `SHA256SUMS` from the same release,
verify before extraction, and install under a writable prefix. Run in a new
download directory. Set `TIMELESS_INSTALL_PREFIX` first to choose a different
destination; the default here is `$HOME/.local/timeless`.

<!-- executable-doc:install:start -->

```sh
set -euo pipefail
timeless_repo=awksedgreep/timeless-libsql
timeless_releases="https://github.com/$timeless_repo/releases"
timeless_release_url="$(curl --fail --silent --show-error --location --head \
  --output /dev/null --write-out '%{url_effective}' "$timeless_releases/latest")"
timeless_tag="${timeless_release_url##*/}"
case "$(uname -s):$(uname -m)" in
  Linux:x86_64) timeless_target=x86_64-unknown-linux-gnu ;;
  Linux:aarch64|Linux:arm64) timeless_target=aarch64-unknown-linux-gnu ;;
  Darwin:x86_64) timeless_target=x86_64-apple-darwin ;;
  Darwin:arm64) timeless_target=aarch64-apple-darwin ;;
  *) echo 'No published archive for this host; build from source.' >&2; exit 1 ;;
esac
timeless_bundle="timeless-telemetry-data-plane-${timeless_tag#v}-$timeless_target"
timeless_archive="$timeless_bundle.tar.gz"
curl --fail --show-error --location --output "$timeless_archive" \
  "$timeless_releases/download/$timeless_tag/$timeless_archive"
curl --fail --show-error --location --output SHA256SUMS \
  "$timeless_releases/download/$timeless_tag/SHA256SUMS"
awk -v archive="$timeless_archive" \
  '$2 == archive { print; found=1 } END { exit !found }' \
  SHA256SUMS > selected.SHA256SUMS
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum --check selected.SHA256SUMS
else
  shasum -a 256 --check selected.SHA256SUMS
fi
tar -xzf "$timeless_archive"
cd "$timeless_bundle"
./install.sh --prefix "${TIMELESS_INSTALL_PREFIX:-$HOME/.local/timeless}"
```

<!-- executable-doc:install:end -->

Use the installed binaries under `<prefix>/bin` and load
`<prefix>/lib/libtimeless_ext` (SQLite supplies the platform suffix).
For example, `$HOME/.local/timeless/bin/timeless-traces-api --version` prints
the installed binary's identity. Database paths are chosen separately when
starting a server; see the [launch contract](SERVER_API_REFERENCE.md#binaries-and-launch-contract).

The installer re-verifies every inner checksum, parses the manifest identity,
rejects a target/host mismatch, executes all three binary identity probes, and
copies the complete bundle into the immutable directory:

```text
<prefix>/telemetry-data-plane/<version>-<target>-<commit>/
```

It then atomically points `<prefix>/bin/timeless-{metrics,logs,traces}-api`,
`<prefix>/bin/timeless-authctl`,
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
timeless_prefix="${TIMELESS_INSTALL_PREFIX:-$HOME/.local/timeless}"
release_dir=$(sed -n '1p' "$timeless_prefix/telemetry-data-plane/CURRENT")
"$release_dir/uninstall.sh" --prefix "$timeless_prefix" --keep-artifact
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
