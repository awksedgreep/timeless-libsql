#!/usr/bin/env bash
# Shared loadable-library naming for shell suites and TESTING.md.
# Usage: timeless_library_path /absolute/path/libtimeless_ext [Linux|Darwin]
timeless_library_path() {
  local stem="$1"
  local platform="${2:-$(uname -s)}"
  case "$platform" in
    Linux) printf '%s.so\n' "$stem" ;;
    Darwin) printf '%s.dylib\n' "$stem" ;;
    *) printf 'unsupported test platform: %s (set an explicit extension path)\n' "$platform" >&2; return 2 ;;
  esac
}

# Explicit overrides must already exist; resolve them before any suite
# changes its working directory (e.g. when invoking a detached Cargo tool).
timeless_existing_library() {
  if [[ ! -f "$1" ]]; then
    printf 'extension does not exist: %s\n' "$1" >&2
    return 2
  fi
  local directory
  directory="$(cd "$(dirname "$1")" && pwd -P)" || return
  printf '%s/%s\n' "$directory" "$(basename "$1")"
}
