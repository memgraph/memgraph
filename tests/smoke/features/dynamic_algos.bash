#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_dynamic_algos() {
  echo "FEATURE: Dynamic Algorithms"
  run_query "CALL mg.procedures() YIELD name;" | grep "online"
}
