#!/usr/bin/env bash

# Smoke test: Memgraph must shut down promptly even when the telemetry server
# does not respond.
#
# Background: on graceful shutdown Memgraph (with telemetry enabled, which is
# the image default) sends a final telemetry/license request to
# telemetry.memgraph.com. A regression once made the shutdown path BLOCK on
# that request until it timed out (~2 minutes) when the telemetry server was
# unresponsive, so `docker stop` / SIGTERM took ~2 minutes instead of seconds.
# This test guards against that regression.
#
# How it works: we point telemetry.memgraph.com (via --add-host) at a "stall"
# sidecar that completes the TCP handshake on :443 but then never responds, so
# the telemetry request hangs like a connected-but-unresponsive server (a
# stuck/overloaded endpoint or a silently dropped connection). We then send
# SIGTERM and measure how long the container takes to exit. A healthy build
# bounds the wait (its telemetry request times out quickly); a regressed build
# blocks well past the threshold.
#
# Usage: smoke_test_shutdown.sh <image_tag> [max_shutdown_seconds]
#   e.g. smoke_test_shutdown.sh memgraph/memgraph:3.11.0 20

set -euo pipefail

if [[ $# -lt 1 || -z "${1:-}" ]]; then
  echo "Usage: $0 <image_tag> [max_shutdown_seconds]" >&2
  exit 1
fi

IMAGE="$1"
MAX_SHUTDOWN_SECONDS="${2:-20}"
# Stop polling a bit past the threshold: once we are over the limit the verdict
# is already "fail", so there is no need to wait out the full ~2 min hang.
POLL_CAP_SECONDS=$(( MAX_SHUTDOWN_SECONDS + 10 ))

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WAIT_SCRIPT="$SCRIPT_DIR/wait_for_memgraph_bolt.sh"

if [[ ! -f "$WAIT_SCRIPT" ]]; then
  echo "Error: cannot find $WAIT_SCRIPT" >&2
  exit 1
fi

TELEMETRY_HOST="telemetry.memgraph.com"
STALL_IMAGE="alpine/socat:1.8.0.3"
NETWORK="mg-shutdown-net-$$"
STALL_NAME="mg-telemetry-stall-$$"
CONTAINER_NAME="mg-shutdown-smoke-$$"
STALL_IP=""  # assigned by Docker; resolved below once the sidecar is up

cleanup() {
  docker rm -f "$CONTAINER_NAME" "$STALL_NAME" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

elapsed_since() {
  # $1 = start epoch (with fractional seconds); prints elapsed seconds, 1 dp.
  awk "BEGIN{printf \"%.1f\", $(date +%s.%N) - $1}"
}

# Let Docker pick a free subnet/IP rather than hardcoding a CIDR, which could
# collide with an existing Docker network or a corporate/VPN route on the host.
echo "Creating isolated network $NETWORK..."
docker network create "$NETWORK" >/dev/null

echo "Starting telemetry stall sidecar..."
# Accepts the TCP connection on :443 then sleeps, so the telemetry request
# connects but never gets a response -- mimicking an unresponsive server.
docker run -d --name "$STALL_NAME" --network "$NETWORK" \
  "$STALL_IMAGE" TCP-LISTEN:443,fork,reuseaddr SYSTEM:'sleep 3600' >/dev/null

# Discover the address Docker assigned to the sidecar on this network.
STALL_IP="$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NETWORK\").IPAddress}}" "$STALL_NAME")"
if [[ -z "$STALL_IP" ]]; then
  echo "Error: could not determine stall sidecar IP address." >&2
  exit 1
fi
echo "Telemetry stall sidecar is at $STALL_IP."

echo "Starting container $CONTAINER_NAME from $IMAGE (telemetry -> stall sidecar)..."
# --telemetry-enabled=true is the image default, but we set it explicitly so the
# test keeps exercising the shutdown path even if the default ever changes.
docker run -d --name "$CONTAINER_NAME" --network "$NETWORK" \
  --add-host "$TELEMETRY_HOST:$STALL_IP" \
  "$IMAGE" --telemetry-enabled=true >/dev/null

# Give the entrypoint a moment to fail fast (e.g. bad args) before we exec.
sleep 1
status="$(docker inspect -f '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || echo missing)"
if [[ "$status" != "running" ]]; then
  echo "Shutdown smoke test FAILED for $IMAGE: container is not running (status=$status)." >&2
  echo "--- container logs ---" >&2
  docker logs "$CONTAINER_NAME" 2>&1 || true
  echo "----------------------" >&2
  exit 1
fi

# Wait for connectivity using the in-container mgconsole (same helper as the
# bolt smoke test), so we time the shutdown of a fully-started instance.
docker cp "$WAIT_SCRIPT" "$CONTAINER_NAME:/tmp/wait_for_memgraph_bolt.sh"
if ! docker exec "$CONTAINER_NAME" bash /tmp/wait_for_memgraph_bolt.sh 127.0.0.1 7687 30 1; then
  echo "Shutdown smoke test FAILED for $IMAGE: container did not accept Bolt connections." >&2
  echo "--- container logs ---" >&2
  docker logs "$CONTAINER_NAME" 2>&1 || true
  echo "----------------------" >&2
  exit 1
fi

# Confirm the telemetry override is actually in effect (getaddrinfo path that
# libcurl uses); otherwise the test would silently not exercise the hang.
resolved="$(docker exec "$CONTAINER_NAME" getent ahosts "$TELEMETRY_HOST" 2>/dev/null | awk '$2=="STREAM"{print $1; exit}')"
if [[ "$resolved" != "$STALL_IP" ]]; then
  echo "Shutdown smoke test FAILED for $IMAGE: $TELEMETRY_HOST resolved to '$resolved', expected '$STALL_IP'." >&2
  exit 1
fi

# Send SIGTERM and measure time until the container actually exits.
echo "Sending SIGTERM and timing graceful shutdown (threshold ${MAX_SHUTDOWN_SECONDS}s)..."
start="$(date +%s.%N)"
docker kill --signal=SIGTERM "$CONTAINER_NAME" >/dev/null

timed_out=false
while [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null || echo false)" == "true" ]]; do
  if awk "BEGIN{exit !($(elapsed_since "$start") > $POLL_CAP_SECONDS)}"; then
    timed_out=true
    break
  fi
  sleep 0.2
done

shutdown_seconds="$(elapsed_since "$start")"
echo "Shutdown time: ${shutdown_seconds}s"

if [[ "$timed_out" == "true" ]] || awk "BEGIN{exit !($shutdown_seconds > $MAX_SHUTDOWN_SECONDS)}"; then
  echo "Shutdown smoke test FAILED for $IMAGE: shutdown took ${shutdown_seconds}s (> ${MAX_SHUTDOWN_SECONDS}s)." >&2
  echo "This usually means shutdown is blocking on the telemetry request." >&2
  echo "--- container logs ---" >&2
  docker logs "$CONTAINER_NAME" 2>&1 | tail -n 50 || true
  echo "----------------------" >&2
  exit 1
fi

echo "Shutdown smoke test PASSED for $IMAGE (shutdown took ${shutdown_seconds}s)."
