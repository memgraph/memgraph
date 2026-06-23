#!/usr/bin/env bash
# Start a clean memgraph, pipe a .cypher file (comment-stripped) to mgconsole, tear down.
# Usage: ./mg.sh file.cypher
set -uo pipefail
REPO="/home/gareth/checkout/memgraph"
DATADIR="$(mktemp -d)"
"$REPO/build/memgraph" --data-directory="$DATADIR/data" --storage-properties-on-edges \
  --log-level=ERROR --storage-snapshot-on-exit=false --storage-wal-enabled=false \
  --data-recovery-on-startup=false --telemetry-enabled=false >/dev/null 2>&1 &
MG=$!
trap 'kill "$MG" 2>/dev/null || true; rm -rf "$DATADIR"' EXIT
for _ in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/7687) >/dev/null 2>&1 && break; sleep 0.2; done
grep -vE '^[[:space:]]*(//|$)' "$1" | mgconsole --output-format=tabular
