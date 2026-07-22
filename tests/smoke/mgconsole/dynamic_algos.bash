#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_dynamic_algos() {
  echo "FEATURE: Dynamic Algorithms"
  run_next "CALL mg.procedures() YIELD name;" | grep "online"
}
