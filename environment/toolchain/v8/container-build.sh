#!/bin/bash
# Build toolchain v8 inside a fresh ubuntu:20.04 container and drop the
# resulting archive into ./output/.
#
# Workflow:
#   1. Build the builder image (deps only — see Dockerfile).
#   2. Start a long-running container.
#   3. Stream the current repo (working tree, no .git) into the container.
#   4. Run build.sh inside.
#   5. docker cp the tarball back out.
#   6. Remove the container on success; leave it running on failure for
#      interactive debugging.

set -euo pipefail

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)
REPO_ROOT=$(cd "$DIR/../../.." >/dev/null 2>&1 && pwd)

IMAGE=mg-toolchain-v8-builder
CONTAINER=mg-toolchain-v8-build
OUTPUT_DIR="$DIR/output"

on_exit() {
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        echo >&2
        echo "Build failed (exit $rc). Container '$CONTAINER' left running:" >&2
        echo "    docker exec -it $CONTAINER bash" >&2
        echo "    docker rm -f $CONTAINER   # when done" >&2
    fi
    exit $rc
}
trap on_exit EXIT

echo "==> Building image $IMAGE"
docker build -t "$IMAGE" "$DIR"

echo "==> Starting container $CONTAINER"
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" "$IMAGE" sleep infinity >/dev/null

echo "==> Copying repo into container (excluding .git and stale build state)"
docker exec "$CONTAINER" mkdir -p /workspace/memgraph
# Exclude:
#   .git                                 — not needed by build.sh, just bulk
#   ./build                              — host-side memgraph build artifacts
#   ./environment/toolchain/v8/build     — host-side toolchain build state
#   ./environment/toolchain/v8/output    — host-side prior tarballs
# Keep environment/toolchain/v8/archives so downloaded source tarballs are
# reused (saves ~1GB of re-fetching on every run).
tar -C "$REPO_ROOT" \
    --exclude=./.git \
    --exclude=./build \
    --exclude=./environment/toolchain/v8/build \
    --exclude=./environment/toolchain/v8/output \
    -cf - . \
    | docker exec -i "$CONTAINER" tar -C /workspace/memgraph -xf -

echo "==> Running build.sh inside container"
docker exec -w /workspace/memgraph "$CONTAINER" ./environment/toolchain/v8/build.sh

echo "==> Copying output back to $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
docker cp \
    "$CONTAINER:/workspace/memgraph/environment/toolchain/v8/output/." \
    "$OUTPUT_DIR/"

echo "==> Removing container $CONTAINER"
docker rm -f "$CONTAINER" >/dev/null

echo
echo "Done. Archives in $OUTPUT_DIR:"
ls -lh "$OUTPUT_DIR/"*.tar.gz 2>/dev/null || echo "  (none found — check build.sh output)"
