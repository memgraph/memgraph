#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# NOTE: At the moment (2024-10-14), tests/mgbench/client.cpp has to be compiled in the Release mode.
cd "$SCRIPT_DIR/../tests/mgbench"

python3 benchmark.py \
  --vendor-name memgraph --installation-type native --vendor-binary "$SCRIPT_DIR/../build/memgraph" --vendor-specific telemetry-enabled=False query-modules-directory=$SCRIPT_DIR/../build/query_modules/ \
  --export-results "$SCRIPT_DIR/../build/bench-vector-search.json" \
  --no-authorization \
  "vector_search_index/*"
