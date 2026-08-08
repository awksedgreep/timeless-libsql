# Timeless release tool

This detached Rust binary builds and validates one native telemetry data-plane
bundle. The canonical prerequisites, commands, and place in the full test
order are maintained in [TESTING.md](../../TESTING.md); the public artifact
contract is [docs/ARTIFACTS.md](../../docs/ARTIFACTS.md).

Run its unit and lint gates:

```sh
cargo test --manifest-path tools/release-tool/Cargo.toml --all-targets --locked
cargo clippy --manifest-path tools/release-tool/Cargo.toml \
  --all-targets --locked -- -D warnings
```

Build and validate a native Linux x86-64 candidate:

```sh
cargo run --release --manifest-path tools/release-tool/Cargo.toml \
  --locked -- \
  --target x86_64-unknown-linux-gnu \
  --output /tmp/timeless-dist
```

`artifact-inventory.json` is the machine-readable source of truth for native
targets, binaries, and fixed archive paths. The documentation-contract gate
compares it with `docs/ARTIFACTS.md`. The packager refuses dirty source and a
non-native target by default, builds both locked workspaces, verifies all
identities and checksums, and performs an isolated install/remove preservation
drill before it reports success. `--allow-dirty` and `--force` are for local
diagnostics only.
