#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_edge_type_operations() {
  echo "FEATURE: Edge Type Operations"
  run_next "WITH {my_edge_type: \"KNOWS\"} as x CREATE ()-[:x.my_edge_type]->() RETURN x; MATCH (n) DETACH DELETE n;"
}
