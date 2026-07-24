#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_functions() {
  echo "FEATURE: Built-in functions"
  # Added in v3.1.
  run_next "RETURN toSet([1, 2, 1]) AS a;"
  run_next "RETURN length([1, 2, 3]) AS a;"
  run_next "CREATE (a), (b), (a)-[c:Type]->(b) RETURN project([a,b], [c]) AS x; MATCH (n) DETACH DELETE n;"
}
