#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_monitoring() {
  echo "FEATURE: Monitoring"
  response=$(curl -X GET "http://localhost:$MEMGRAPH_NEXT_MONITORING_PORT/metrics")
  if ! echo "$response" | jq -e '.General | has("vertex_count")'; then
    echo "Monitoring data is missing vertex count."
    exit 1
  fi
}
