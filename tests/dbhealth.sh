#!/usr/bin/env bash
# dbhealth standalone-extension test: build the health-only .so, then
# prove the headline contract — CREATE VIRTUAL TABLE begins collection,
# re-opening the database resumes it, the report renders, and the
# interactive 'sample' command still works. The Rust release-gate harness
# keeps one SQLite host process alive between scheduler ticks.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "$ROOT/tests/platform.sh"
if [[ -n "${TIMELESS_DBHEALTH_EXT:-}" ]]; then
  EXT="$(timeless_existing_library "$TIMELESS_DBHEALTH_EXT")"
else
  EXT="$(timeless_library_path "$ROOT/target/release/libdbhealth_ext")"
  echo "== building dbhealth-ext (release) =="
  cargo build -p dbhealth-ext --release --locked --manifest-path "$ROOT/Cargo.toml"
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cargo run --quiet --manifest-path "$ROOT/tools/query-harness/Cargo.toml" --locked -- \
  gate dbhealth --extension "$EXT" \
  --database "$TMP/auto.db"
