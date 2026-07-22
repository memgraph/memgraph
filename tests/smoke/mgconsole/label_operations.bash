#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_label_operations() {
  echo "FEATURE: Label Operations"
  run_next "WITH {my_labels: [\"Label1\", \"Label2\"]} as x CREATE (n:x.my_labels) RETURN n; MATCH (n) DETACH DELETE n;"
}
