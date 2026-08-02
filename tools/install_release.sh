#!/bin/sh
set -eu

prefix=/opt/timeless
while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix) prefix=$2; shift 2 ;;
    *) echo "usage: install.sh [--prefix PATH]" >&2; exit 2 ;;
  esac
done

bundle=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
manifest="$bundle/artifact-manifest.json"
checksums="$bundle/SHA256SUMS"
test -f "$manifest" && test -f "$checksums" || {
  echo "incomplete Timeless release bundle" >&2
  exit 1
}

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$bundle" && sha256sum -c SHA256SUMS)
elif command -v shasum >/dev/null 2>&1; then
  (cd "$bundle" && shasum -a 256 -c SHA256SUMS)
else
  echo "sha256sum or shasum is required to verify the release bundle" >&2
  exit 1
fi

version=$(sed -n 's/^[[:space:]]*"version": "\([^"]*\)".*$/\1/p' "$manifest" | head -1)
target=$(sed -n 's/^[[:space:]]*"target": "\([^"]*\)".*$/\1/p' "$manifest" | head -1)
commit=$(sed -n 's/^[[:space:]]*"commit": "\([^"]*\)".*$/\1/p' "$manifest" | head -1)
test -n "$version" && test -n "$target" && test -n "$commit" || {
  echo "invalid artifact manifest identity" >&2
  exit 1
}

case "$(uname -s):$(uname -m)" in
  Linux:x86_64) native=x86_64-unknown-linux-gnu ;;
  Linux:aarch64|Linux:arm64) native=aarch64-unknown-linux-gnu ;;
  Darwin:x86_64) native=x86_64-apple-darwin ;;
  Darwin:arm64) native=aarch64-apple-darwin ;;
  *) echo "unsupported install platform $(uname -s) $(uname -m)" >&2; exit 1 ;;
esac
test "$native" = "$target" || {
  echo "artifact target $target does not match host $native" >&2
  exit 1
}

release_id="$version-$target-$commit"
case "$release_id" in
  *[!A-Za-z0-9._-]*) echo "unsafe artifact release identity" >&2; exit 1 ;;
esac
releases="$prefix/telemetry-data-plane"
destination="$releases/$release_id"
staging="$releases/.install-$release_id-$$"
mkdir -p "$releases" "$prefix/bin" "$prefix/lib"
trap 'rm -rf "$staging"' EXIT HUP INT TERM

if [ ! -d "$destination" ]; then
  mkdir "$staging"
  cp -R "$bundle/." "$staging/"
  mv "$staging" "$destination"
fi

for binary in timeless-metrics-api timeless-logs-api timeless-traces-api; do
  identity=$("$destination/bin/$binary" --version)
  case "$identity" in
    *"\"commit\":\"$commit\""*"\"target\":\"$target\""*) ;;
    *) echo "$binary build identity does not match artifact manifest" >&2; exit 1 ;;
  esac
  link="$prefix/bin/$binary"
  temporary="$link.install-$$"
  ln -s "$destination/bin/$binary" "$temporary"
  mv -f "$temporary" "$link"
done

extension=$(find "$destination/lib" -maxdepth 1 -type f -name 'libtimeless_ext.*' -print)
test "$(printf '%s\n' "$extension" | grep -c .)" -eq 1 || {
  echo "release bundle must contain exactly one timeless extension" >&2
  exit 1
}
extension_link="$prefix/lib/$(basename "$extension")"
extension_temporary="$extension_link.install-$$"
ln -s "$extension" "$extension_temporary"
mv -f "$extension_temporary" "$extension_link"

printf '%s\n' "$destination" > "$releases/.CURRENT-$$"
mv -f "$releases/.CURRENT-$$" "$releases/CURRENT"
trap - EXIT HUP INT TERM
echo "installed Timeless telemetry data plane $release_id"
echo "data and configuration were not created, changed, or removed"
