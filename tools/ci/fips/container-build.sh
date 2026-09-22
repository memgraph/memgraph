#!/bin/bash
# Run build-wheels.sh inside an already running container and copy the wheels
# back out. The container has to be the distro the wheels are for: they link its
# OpenSSL, libxml2 and xmlsec1.

set -euo pipefail

function print_help() {
    echo "Usage: $0 <container_name> [--output-dir <dir>] [--user <user>] [--] [<build-wheels.sh args>]"
    exit 1
}

CONTAINER_NAME=${1:-}
[ -n "$CONTAINER_NAME" ] || print_help
shift 1

OUTPUT_DIR="$PWD/build/fips-wheels"
CONTAINER_USER="mg"
BUILD_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir) OUTPUT_DIR=$2; shift 2 ;;
        --user)       CONTAINER_USER=$2; shift 2 ;;
        --)           shift; BUILD_ARGS+=("$@"); break ;;
        -h|--help)    print_help ;;
        *)            BUILD_ARGS+=("$1"); shift ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_DIR="/tmp/fips-wheels-build"

docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null | grep -qx true \
    || { echo "Container '$CONTAINER_NAME' is not running" >&2; exit 1; }

docker exec -u mg "$CONTAINER_NAME" rm -rf "$REMOTE_DIR"
docker exec -u mg "$CONTAINER_NAME" mkdir -p "$REMOTE_DIR"
docker cp "$SCRIPT_DIR/build-wheels.sh" "$CONTAINER_NAME:$REMOTE_DIR/build-wheels.sh"
docker exec -u root "$CONTAINER_NAME" chmod +x "$REMOTE_DIR/build-wheels.sh"
[ "$CONTAINER_USER" = "root" ] \
    || docker exec -u root "$CONTAINER_NAME" chown -R "$CONTAINER_USER" "$REMOTE_DIR"

docker exec -u "$CONTAINER_USER" "$CONTAINER_NAME" \
    "$REMOTE_DIR/build-wheels.sh" --output-dir "$REMOTE_DIR/output" "${BUILD_ARGS[@]}"

mkdir -p "$OUTPUT_DIR"
docker cp "$CONTAINER_NAME:$REMOTE_DIR/output/." "$OUTPUT_DIR/"
echo
echo "Wheels in $OUTPUT_DIR:"
ls -1 "$OUTPUT_DIR"
