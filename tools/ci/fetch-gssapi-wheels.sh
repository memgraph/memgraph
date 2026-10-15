#!/bin/bash

set -euo pipefail

# Fetch the prebuilt python `gssapi` wheels into a directory pip can resolve
# against with --find-links.
#
# gssapi ships no prebuilt linux wheels on PyPI, so we publish our own
# manylinux wheels (auditwheel-repaired, so the krb5 libs are bundled inside
# the wheel and no krb5 runtime package is required). Two wheels per
# architecture cover every interpreter we ship against: cp310 for Python 3.10
# and cp311-abi3 for Python >= 3.11. Both are dropped into the destination
# directory and pip picks the compatible one, so callers don't need to know
# which python lives in the image they're building.
#
# Sources, tried in order:
#   1. the mgdeps cache (http://mgdeps-cache:8000/gssapi/<version>/) — fast,
#      reachable from CI runners;
#   2. the S3 bucket the cache mirrors
#      (https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/gssapi/<version>/)
#      — works from a dev machine or any runner without the cache.

# The version to fetch is the pin memgraph's auth module actually requires, read
# straight from its requirements file so this script can't drift from it. The
# images pin the same version at their pip install, and --only-binary=gssapi
# means a bump with no published wheels fails the build instead of quietly
# building the sdist -- so keep those in step when changing the pin.
AUTH_REQUIREMENTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/src/auth/reference_modules/requirements.txt"

gssapi_version_from_requirements() {
  local pin
  pin="$(sed -n 's/^gssapi[[:space:]]*==[[:space:]]*\([^[:space:];#]*\).*/\1/p' "$AUTH_REQUIREMENTS" 2>/dev/null | head -n 1)"
  if [[ -z "$pin" ]]; then
    echo "Error: no 'gssapi==<version>' pin found in $AUTH_REQUIREMENTS" >&2
    return 1
  fi
  printf '%s' "$pin"
}

if [[ -z "${GSSAPI_VERSION:-}" ]]; then
  GSSAPI_VERSION="$(gssapi_version_from_requirements)"
fi
MGDEPS_CACHE_HOST="${MGDEPS_CACHE_HOST:-mgdeps-cache}"
MGDEPS_CACHE_PORT="${MGDEPS_CACHE_PORT:-8000}"
S3_BUCKET_URL="${S3_BUCKET_URL:-https://s3.eu-west-1.amazonaws.com/deps.memgraph.io}"

DEST_DIR=""
ARCH="$(uname -m)"

print_help() {
  cat <<EOF
Usage: $(basename "$0") --dest-dir DIR [OPTIONS]

Options:
  --dest-dir DIR    Directory the wheels are downloaded into (created if
                    missing). Required.
  --arch ARCH       Target architecture: amd|amd64|x86_64 or arm|arm64|aarch64
                    (default: \$(uname -m) = $ARCH).
  --version VER     gssapi version to fetch (default: $GSSAPI_VERSION).
  -h, --help        Print this help.

Environment overrides: GSSAPI_VERSION, MGDEPS_CACHE_HOST, MGDEPS_CACHE_PORT,
S3_BUCKET_URL.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dest-dir)
      DEST_DIR="$2"
      shift 2
    ;;
    --arch)
      ARCH="$2"
      shift 2
    ;;
    --version)
      GSSAPI_VERSION="$2"
      shift 2
    ;;
    -h|--help)
      print_help
      exit 0
    ;;
    *)
      echo "Error: Unknown flag '$1'" >&2
      print_help >&2
      exit 1
    ;;
  esac
done

if [[ -z "$DEST_DIR" ]]; then
  echo "Error: --dest-dir is required" >&2
  print_help >&2
  exit 1
fi

# Normalise the arch to the machine string used in the wheel platform tag.
case "$ARCH" in
  amd|amd64|x86_64) MACHINE="x86_64" ;;
  arm|arm64|aarch64) MACHINE="aarch64" ;;
  *)
    echo "Error: Unsupported --arch '$ARCH' (expected amd/amd64/x86_64 or arm/arm64/aarch64)" >&2
    exit 1
  ;;
esac

CACHE_BASE_URL="http://${MGDEPS_CACHE_HOST}:${MGDEPS_CACHE_PORT}/gssapi/${GSSAPI_VERSION}"
S3_BASE_URL="${S3_BUCKET_URL}/gssapi/${GSSAPI_VERSION}"

# List the wheel filenames a source offers. The cache is served by nginx
# autoindex (relative hrefs); S3 answers ListObjectsV2 with full keys, so strip
# the prefix off those. Both are filtered down to the target architecture: pip
# would ignore the foreign-arch wheels anyway, but there's no point pulling
# ~6MB each, and the offline installer bundles whatever it finds here verbatim.
list_wheels() {
  local source="$1" listing
  case "$source" in
    cache)
      listing="$(curl -fsS --connect-timeout 5 --max-time 60 "${CACHE_BASE_URL}/")" || return 1
      listing="$(printf '%s\n' "$listing" \
        | grep -oE 'href="[^"?]+\.whl"' | sed -e 's/^href="//' -e 's/"$//')" || return 1
    ;;
    s3)
      listing="$(curl -fsS --connect-timeout 10 --max-time 60 \
        "${S3_BUCKET_URL}/?list-type=2&prefix=gssapi/${GSSAPI_VERSION}/" \
        | grep -oE '<Key>[^<]+\.whl</Key>' | sed -e 's|^<Key>||' -e 's|</Key>$||' -e 's|.*/||')" || return 1
    ;;
  esac
  printf '%s\n' "$listing" | grep -E "_${MACHINE}\." || return 1
}

WHEELS=""
BASE_URL=""
for source in cache s3; do
  case "$source" in
    cache) BASE_URL="$CACHE_BASE_URL" ;;
    s3)    BASE_URL="$S3_BASE_URL" ;;
  esac
  if WHEELS="$(list_wheels "$source")"; then
    echo "Fetching gssapi $GSSAPI_VERSION ($MACHINE) wheels from $source: $BASE_URL"
    break
  fi
  echo "No gssapi $GSSAPI_VERSION ($MACHINE) wheels available from $source ($BASE_URL)"
  WHEELS=""
done

if [[ -z "$WHEELS" ]]; then
  echo "Error: could not fetch gssapi $GSSAPI_VERSION wheels for $MACHINE from the mgdeps cache or S3" >&2
  exit 1
fi

mkdir -p "$DEST_DIR"
# Drop wheels left behind by an earlier run: CI workspaces are reused, and a
# stale version or foreign-arch wheel in a --find-links dir is a confusing way
# to fail later.
rm -fv "$DEST_DIR"/gssapi-*.whl

while read -r wheel; do
  [[ -n "$wheel" ]] || continue
  echo "  $wheel"
  curl -fsSL --retry 3 --retry-delay 2 --connect-timeout 10 \
    -o "$DEST_DIR/$wheel" "$BASE_URL/$wheel"
done <<< "$WHEELS"

echo "gssapi wheels in $DEST_DIR:"
ls -1 "$DEST_DIR"/gssapi-*.whl
