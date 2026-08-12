#!/bin/sh
set -eu

prefix=/opt/timeless
keep_artifact=false
while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix) prefix=$2; shift 2 ;;
    --keep-artifact) keep_artifact=true; shift ;;
    *) echo "usage: uninstall.sh [--prefix PATH] [--keep-artifact]" >&2; exit 2 ;;
  esac
done

bundle=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
manifest="$bundle/artifact-manifest.json"
version=$(sed -n 's/^[[:space:]]*"version": "\([^"]*\)".*$/\1/p' "$manifest" | head -1)
target=$(sed -n 's/^[[:space:]]*"target": "\([^"]*\)".*$/\1/p' "$manifest" | head -1)
commit=$(sed -n 's/^[[:space:]]*"commit": "\([^"]*\)".*$/\1/p' "$manifest" | head -1)
release_id="$version-$target-$commit"
case "$release_id" in
  *[!A-Za-z0-9._-]*) echo "unsafe artifact release identity" >&2; exit 1 ;;
esac
destination="$prefix/telemetry-data-plane/$release_id"

remove_owned_link() {
  link=$1
  expected=$2
  if [ -L "$link" ] && [ "$(readlink "$link")" = "$expected" ]; then
    rm "$link"
  fi
}

for binary in timeless-metrics-api timeless-logs-api timeless-traces-api timeless-authctl; do
  remove_owned_link "$prefix/bin/$binary" "$destination/bin/$binary"
done
for extension in "$destination"/lib/libtimeless_ext.*; do
  [ -e "$extension" ] || continue
  remove_owned_link "$prefix/lib/$(basename "$extension")" "$extension"
done

current="$prefix/telemetry-data-plane/CURRENT"
if [ -f "$current" ] && [ "$(sed -n '1p' "$current")" = "$destination" ]; then
  rm "$current"
fi
if [ "$keep_artifact" = false ] && [ -d "$destination" ]; then
  rm -rf "$destination"
fi
rmdir "$prefix/bin" "$prefix/lib" "$prefix/telemetry-data-plane" 2>/dev/null || true

echo "removed Timeless telemetry data-plane release $release_id"
echo "all telemetry data, backups, legacy rollback sources, and configuration were preserved"
