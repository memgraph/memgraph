#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_monitoring() {
  echo "FEATURE: Monitoring"
  response=$(curl -s -X GET "http://localhost:$MEMGRAPH_MONITORING_PORT/metrics")
  if ! grep -qE '^memgraph_vertex_count(\{[^}]*\})? ' <<< "$response"; then
    echo "Monitoring data is missing vertex count."
    exit 1
  fi
}
