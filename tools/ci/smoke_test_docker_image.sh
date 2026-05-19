#!/usr/bin/env bash

# Quick smoke test for a Memgraph (or MAGE) Docker image.
#
# Runs the image with no arguments and verifies that the container accepts
# Bolt connections. Catches regressions where the image can't start at all
# (for example, a Dockerfile with `CMD [""]` that passes an empty argument
# to the memgraph entrypoint).
#
# Usage: smoke_test_docker_image.sh <image_tag>
#   e.g. smoke_test_docker_image.sh memgraph/memgraph-mage:1.30.0

set -euo pipefail

if [[ $# -lt 1 || -z "${1:-}" ]]; then
  echo "Usage: $0 <image_tag>" >&2
  exit 1
fi

IMAGE="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WAIT_SCRIPT="$SCRIPT_DIR/wait_for_memgraph_bolt.sh"

if [[ ! -f "$WAIT_SCRIPT" ]]; then
  echo "Error: cannot find $WAIT_SCRIPT" >&2
  exit 1
fi

CONTAINER_NAME="memgraph-bolt-smoke-$$"

cleanup() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "Starting container $CONTAINER_NAME from $IMAGE (no arguments)..."
docker run -d --name "$CONTAINER_NAME" "$IMAGE" >/dev/null

# Give the entrypoint a moment to fail fast (e.g. bad args) before we exec.
sleep 1
status="$(docker inspect -f '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || echo missing)"
if [[ "$status" != "running" ]]; then
  echo "Smoke test FAILED for $IMAGE: container is not running (status=$status)." >&2
  echo "--- container logs ---" >&2
  docker logs "$CONTAINER_NAME" 2>&1 || true
  echo "----------------------" >&2
  exit 1
fi

# Copy the bolt wait helper into the container so we use the in-container
# mgconsole binary instead of requiring one on the runner host.
docker cp "$WAIT_SCRIPT" "$CONTAINER_NAME:/tmp/wait_for_memgraph_bolt.sh"

if ! docker exec "$CONTAINER_NAME" bash /tmp/wait_for_memgraph_bolt.sh 127.0.0.1 7687 30 1; then
  echo "Smoke test FAILED for $IMAGE: container did not accept Bolt connections." >&2
  echo "--- container logs ---" >&2
  docker logs "$CONTAINER_NAME" 2>&1 || true
  echo "----------------------" >&2
  exit 1
fi

echo "Smoke test PASSED for $IMAGE."
