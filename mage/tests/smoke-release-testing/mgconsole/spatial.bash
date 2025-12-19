#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_spatial() {
  echo "FEATURE: Spatial data types and functionalities"
  run_next "MATCH (n) DETACH DELETE n;"
  run_next "CREATE (n {xy: point({x:1, y:2})}) RETURN n.xy.x;"
  run_next "CREATE (n {xy: point({x:1, y:2})}) RETURN n;"
  run_next "CREATE POINT INDEX ON :School(location);"
  run_next "SHOW INDEX INFO;"
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  # NOTE: Take a look at session_trace.bash for the v1 implementation of binary-docker picker.
  trap cleanup_memgraph_binary_processes EXIT # To make sure cleanup is done.
  set -e # To make sure the script will return non-0 in case of a failure.
  run_memgraph_binary_and_test "--log-level=TRACE --log-file=mg_spatial_test.logs" test_spatial
fi
