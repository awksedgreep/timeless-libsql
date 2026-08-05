#!/usr/bin/env bash
# dbhealth standalone-extension test: build the health-only .so, then
# prove the headline contract — CREATE VIRTUAL TABLE begins collection,
# re-opening the database resumes it, the report renders, and the
# interactive 'sample' command still works. The Rust release-gate harness
# keeps one SQLite host process alive between scheduler ticks.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "== building dbhealth-ext (release) =="
cargo build -p dbhealth-ext --release --manifest-path "$ROOT/Cargo.toml"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cargo run --quiet --manifest-path "$ROOT/tools/query-harness/Cargo.toml" --locked -- \
  gate dbhealth --extension "$ROOT/target/release/libdbhealth_ext" \
  --database "$TMP/auto.db"
