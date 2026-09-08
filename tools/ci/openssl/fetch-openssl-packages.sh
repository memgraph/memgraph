#!/bin/bash

set -euo pipefail

# Fetch the prebuilt custom OpenSSL .deb packages (openssl CLI + libssl3t64,
# and the FIPS provider with --fips) into a directory the Docker builds install
# from.
#
# These are the packages the build.sh/build-*-deb.sh scripts next to this one
# produce, but built once per OpenSSL version by the infra pipeline
# (memgraph/infra: mgdeps-cache/ansible/build-openssl-playbook.yml) inside the
# same mgbuild container CI uses, rather than rebuilt from source in every
# packaging job. The local build scripts stay for local/dev use and as the
# fallback if a version has not been published yet.
#
# Sources, tried in order:
#   1. the mgdeps cache (http://mgdeps-cache:8000/openssl/<version>/<distro>/<arch>/)
#      — fast, reachable from CI runners;
#   2. the S3 bucket the cache mirrors
#      (https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/openssl/<version>/<distro>/<arch>/)
#      — works from a dev machine or any runner without the cache.
#
# Two variants are published, each under its own version directory so their
# identically-named .debs can never overwrite each other:
#
#   stock  openssl/<version>/<distro>/<arch>/
#          openssl, libssl3t64 — what memgraph ships today, no FIPS provider.
#   fips   openssl/<version>-fips<fips-version>/<distro>/<arch>/
#          the same libraries built with the FIPS provider available, plus the
#          openssl-fips-provider package. The provider is pinned to the OpenSSL
#          version whose fips.so carries the FIPS 140-3 validation, which is why
#          it is versioned independently of the core libraries.

# Keep in sync with the version the local build scripts default to
# (tools/ci/openssl/build.sh) and with build_openssl_version in the infra role.
OPENSSL_VERSION="${OPENSSL_VERSION:-3.5.4}"
# Only used with --fips. Must match build_openssl_fips_version in the infra role.
FIPS_VERSION="${FIPS_VERSION:-3.1.2}"
# The distro directory in the published layout. Only ubuntu-24.04 is built: the
# packages are installed into the Ubuntu 24.04 based Docker images.
DISTRO="${OPENSSL_DISTRO:-ubuntu-24.04}"
MGDEPS_CACHE_HOST="${MGDEPS_CACHE_HOST:-mgdeps-cache}"
MGDEPS_CACHE_PORT="${MGDEPS_CACHE_PORT:-8000}"
S3_BUCKET_URL="${S3_BUCKET_URL:-https://s3.eu-west-1.amazonaws.com/deps.memgraph.io}"

DEST_DIR=""
ARCH="$(uname -m)"
FIPS=false

print_help() {
  cat <<EOF
Usage: $(basename "$0") --dest-dir DIR [OPTIONS]

Options:
  --dest-dir DIR      Directory the .debs are downloaded into (created if
                      missing). Required.
  --arch ARCH         Target architecture: amd|amd64|x86_64 or arm|arm64|aarch64
                      (default: \$(uname -m) = $ARCH).
  --version VER       OpenSSL version to fetch (default: $OPENSSL_VERSION).
  --fips              Fetch the FIPS 140-3 build (adds openssl-fips-provider)
                      instead of the stock one.
  --fips-version VER  FIPS provider version, only with --fips
                      (default: $FIPS_VERSION).
  --distro DISTRO     Distro directory in the published layout
                      (default: $DISTRO).
  -h, --help          Print this help.

Environment overrides: OPENSSL_VERSION, FIPS_VERSION, OPENSSL_DISTRO,
MGDEPS_CACHE_HOST, MGDEPS_CACHE_PORT, S3_BUCKET_URL.
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
      OPENSSL_VERSION="$2"
      shift 2
    ;;
    --fips)
      FIPS=true
      shift
    ;;
    --fips-version)
      FIPS_VERSION="$2"
      shift 2
    ;;
    --distro)
      DISTRO="$2"
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

# Normalise the arch to the dpkg architecture the packages are named with.
case "$ARCH" in
  amd|amd64|x86_64) DEB_ARCH="amd64" ;;
  arm|arm64|aarch64) DEB_ARCH="arm64" ;;
  *)
    echo "Error: Unsupported --arch '$ARCH' (expected amd/amd64/x86_64 or arm/arm64/aarch64)" >&2
    exit 1
  ;;
esac

# The version directory encodes the variant, and the fips variant ships one
# package the stock one does not.
if [[ "$FIPS" == "true" ]]; then
  VARIANT="fips"
  VERSION_DIR="${OPENSSL_VERSION}-fips${FIPS_VERSION}"
  REQUIRED_PACKAGES=(openssl libssl3t64 openssl-fips-provider)
else
  VARIANT="stock"
  VERSION_DIR="${OPENSSL_VERSION}"
  REQUIRED_PACKAGES=(openssl libssl3t64)
fi

REL_PREFIX="openssl/${VERSION_DIR}/${DISTRO}/${DEB_ARCH}"
CACHE_BASE_URL="http://${MGDEPS_CACHE_HOST}:${MGDEPS_CACHE_PORT}/${REL_PREFIX}"
S3_BASE_URL="${S3_BUCKET_URL}/${REL_PREFIX}"

# List the .deb filenames a source offers. The cache is served by nginx
# autoindex (relative hrefs); S3 answers ListObjectsV2 with full keys, so strip
# the prefix off those.
list_debs() {
  local source="$1" listing
  case "$source" in
    cache)
      listing="$(curl -fsS --connect-timeout 5 --max-time 60 "${CACHE_BASE_URL}/")" || return 1
      listing="$(printf '%s\n' "$listing" \
        | grep -oE 'href="[^"?]+\.deb"' | sed -e 's/^href="//' -e 's/"$//')" || return 1
    ;;
    s3)
      listing="$(curl -fsS --connect-timeout 10 --max-time 60 \
        "${S3_BUCKET_URL}/?list-type=2&prefix=${REL_PREFIX}/" \
        | grep -oE '<Key>[^<]+\.deb</Key>' | sed -e 's|^<Key>||' -e 's|</Key>$||' -e 's|.*/||')" || return 1
    ;;
  esac
  # nginx percent-encodes the '+' in the fips libssl3t64 filename in its hrefs;
  # decode it so the name matches the object and the download URL is built from
  # the real filename.
  printf '%s\n' "$listing" | sed -e 's/%2B/+/g' -e 's/%2b/+/g' | awk 'NF'
}

# Which of this variant's packages a listing does not offer. All of them have to
# be there before a source is usable: the Dockerfiles install the .debs by glob,
# so a set missing one fails much later and far less clearly.
missing_packages() {
  local listing="$1" package missing=""
  for package in "${REQUIRED_PACKAGES[@]}"; do
    # Match on the "<package>_" name field so openssl does not match
    # openssl-fips-provider.
    if ! grep -q "^${package}_" <<< "$listing"; then
      missing="$missing $package"
    fi
  done
  printf '%s' "$missing"
}

# An incomplete source is skipped rather than fatal: the cache bakes its packages
# in when its image is built, so it can lag behind a version that is already in
# S3 - which is exactly the case the S3 fallback exists for.
DEBS=""
BASE_URL=""
for source in cache s3; do
  case "$source" in
    cache) BASE_URL="$CACHE_BASE_URL" ;;
    s3)    BASE_URL="$S3_BASE_URL" ;;
  esac
  listing="$(list_debs "$source")" || listing=""
  if [[ -z "$listing" ]]; then
    echo "No $VARIANT OpenSSL $OPENSSL_VERSION ($DEB_ARCH, $DISTRO) packages available from $source ($BASE_URL)"
    continue
  fi
  missing="$(missing_packages "$listing")"
  if [[ -n "$missing" ]]; then
    echo "Skipping $source ($BASE_URL): the $VARIANT variant package(s)$missing are not there, only:"
    awk '{print "  " $0}' <<< "$listing"
    continue
  fi
  echo "Fetching $VARIANT OpenSSL $OPENSSL_VERSION ($DEB_ARCH, $DISTRO) packages from $source: $BASE_URL"
  DEBS="$listing"
  break
done

if [[ -z "$DEBS" ]]; then
  echo "Error: could not fetch a complete set of $VARIANT OpenSSL $OPENSSL_VERSION packages for $DEB_ARCH from the mgdeps cache or S3." >&2
  echo "       Publish them first with memgraph/infra's mgdeps-cache/ansible/build-openssl-playbook.yml," >&2
  echo "       or build them locally with tools/ci/openssl/container-build.sh." >&2
  exit 1
fi

mkdir -p "$DEST_DIR"
# Drop packages left behind by an earlier run: CI workspaces are reused, and the
# Dockerfiles install every .deb they find here by glob, so a stale version or
# variant would get installed alongside (or instead of) this one.
rm -fv "$DEST_DIR"/openssl*.deb "$DEST_DIR"/libssl3t64*.deb

while read -r deb; do
  [[ -n "$deb" ]] || continue
  echo "  $deb"
  # The fips libssl3t64 filename carries a '+' in its version. S3 only serves
  # that key when the '+' is percent-encoded (a literal one 404s), and nginx
  # decodes the escape back before looking up the file, so encoding it suits
  # both sources. The file is still written under its real name.
  curl -fsSL --retry 3 --retry-delay 2 --connect-timeout 10 \
    -o "$DEST_DIR/$deb" "$BASE_URL/${deb//+/%2B}"
  # A truncated download is an unhelpful failure inside a docker build, so
  # check the archive here where the error still points at the download.
  if command -v dpkg-deb >/dev/null 2>&1; then
    if ! dpkg-deb --field "$DEST_DIR/$deb" Package >/dev/null 2>&1; then
      echo "Error: '$DEST_DIR/$deb' is not a readable .deb (download corrupted?)" >&2
      exit 1
    fi
  fi
done <<< "$DEBS"

echo "OpenSSL packages in $DEST_DIR:"
ls -1 "$DEST_DIR"/openssl*.deb "$DEST_DIR"/libssl3t64*.deb
