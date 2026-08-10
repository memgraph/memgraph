#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_durability() {
  echo "FEATURE: Durability and Persistence"

  echo "SUBFEATURE: Create snapshot query"
  run_next "CREATE SNAPSHOT;"

  echo "Durability testing completed successfully"
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  # NOTE: Take a look at session_trace.bash for the v1 implementation of binary-docker picker.
  trap cleanup_memgraph_binary_processes EXIT # To make sure cleanup is done.
  set -e # To make sure the script will return non-0 in case of a failure.
  run_memgraph_binary_and_test "--log-level=TRACE --log-file=mg_test_durability.logs" test_durability
fi
