#!/bin/bash
# Make sure the FIPS wheels are in a local directory, the cheapest way possible:
# use what is already there, else pull from S3, else build them in a container
# and publish the result so the next run only has to download it.
#
# Versions come from the auth module's requirements file rather than being
# pinned here, so this cannot drift from what the image actually installs - a
# CVE bump there is picked up automatically, misses the local and S3 checks, and
# triggers a rebuild.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"
REQUIREMENTS="$REPO_ROOT/src/auth/reference_modules/requirements.txt"
PACKAGES=(cryptography xmlsec lxml)

function print_help() {
    echo "Usage: $0 --dest-dir <dir> [--container <name>] [--user <user>] [--distro <distro>] [--no-upload]"
    exit 1
}

DEST_DIR=""
CONTAINER=""
CONTAINER_USER="mg"
DISTRO="ubuntu-24.04"
UPLOAD=true
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)  ARCH=amd64 ;;
    aarch64) ARCH=arm64 ;;
esac
S3_URI="${S3_URI:-s3://deps.memgraph.io/fips-wheels}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dest-dir)  DEST_DIR=$2; shift 2 ;;
        --container) CONTAINER=$2; shift 2 ;;
        --user)      CONTAINER_USER=$2; shift 2 ;;
        --distro)    DISTRO=$2; shift 2 ;;
        --no-upload) UPLOAD=false; shift ;;
        -h|--help)   print_help ;;
        *)           print_help ;;
    esac
done
[ -n "$DEST_DIR" ] || print_help
S3_PREFIX="$S3_URI/$DISTRO/$ARCH"

SPECS=()
for p in "${PACKAGES[@]}"; do
    v="$(sed -n "s/^$p[[:space:]]*==[[:space:]]*\([^[:space:];#]*\).*/\1/p" "$REQUIREMENTS" | head -1)"
    [ -n "$v" ] || { echo "No '$p==<version>' pin in $REQUIREMENTS" >&2; exit 1; }
    SPECS+=("$p==$v")
done

mkdir -p "$DEST_DIR"

# Wheel filenames carry the version, so presence is a glob and no manifest or
# date-stamped prefix is needed: a version bump simply does not match.
missing() {
    local spec name ver
    for spec in "${SPECS[@]}"; do
        name="${spec%%==*}"; ver="${spec##*==}"
        compgen -G "$DEST_DIR/${name}-${ver}-*.whl" >/dev/null || printf '%s\n' "$spec"
    done
}

mapfile -t NEED < <(missing)
if [ "${#NEED[@]}" -eq 0 ]; then
    echo "All wheels already in $DEST_DIR"
    exit 0
fi
echo "Not in $DEST_DIR: ${NEED[*]}"

echo "Trying $S3_PREFIX"
for spec in "${NEED[@]}"; do
    name="${spec%%==*}"; ver="${spec##*==}"
    # Not fatal: a miss just means building it instead.
    aws s3 cp "$S3_PREFIX/" "$DEST_DIR/" --recursive \
        --exclude "*" --include "${name}-${ver}-*.whl" --no-progress || true
done

mapfile -t NEED < <(missing)
if [ "${#NEED[@]}" -eq 0 ]; then
    echo "Fetched from S3 into $DEST_DIR"
    exit 0
fi
echo "Not in S3: ${NEED[*]}"

[ -n "$CONTAINER" ] || { echo "Nothing to build with - pass --container <name>" >&2; exit 1; }
"$SCRIPT_DIR/container-build.sh" "$CONTAINER" --user "$CONTAINER_USER" \
    --output-dir "$DEST_DIR" -- "${NEED[@]}"

mapfile -t STILL < <(missing)
[ "${#STILL[@]}" -eq 0 ] || { echo "Build did not produce: ${STILL[*]}" >&2; exit 1; }

# Publish only what was just built, so the next run downloads instead. A failed
# upload is a warning, not an error: the wheels are where the caller asked for
# them, which is what was actually requested.
if [ "$UPLOAD" = "true" ]; then
    echo "Uploading to $S3_PREFIX"
    for spec in "${NEED[@]}"; do
        name="${spec%%==*}"; ver="${spec##*==}"
        for whl in "$DEST_DIR/${name}-${ver}-"*.whl; do
            aws s3 cp "$whl" "$S3_PREFIX/" --no-progress \
                || echo "WARNING: could not upload $(basename "$whl")" >&2
        done
    done
fi

echo "Built into $DEST_DIR"
