#!/usr/bin/env bash
# Focused correctness regressions from REVIEW_FIX_PLAN.md.
#
# Usage:
#   ./tests/correctness.sh [r1|r2|r3|r4|r8|logs-rich]
#
# TIMELESS_EXT may point at an already-built extension. Otherwise this script
# builds the release cdylib before running the selected section.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXT="${TIMELESS_EXT:-$ROOT/target/release/libtimeless_ext.so}"
SECTION="${1:-r1}"

case "$SECTION" in
  r1|r2|r3|r4|r8|logs-rich) ;;
  *)
    echo "unknown correctness section: $SECTION" >&2
    exit 2
    ;;
esac

if [[ -z "${TIMELESS_EXT:-}" ]]; then
  cargo build -p timeless-ext --release --manifest-path "$ROOT/Cargo.toml"
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "== Rust-native correctness section: $SECTION =="
cargo run --quiet --manifest-path "$ROOT/tools/query-harness/Cargo.toml" --locked -- \
  gate correctness "$SECTION" --extension "$EXT" --temporary "$TMP"
