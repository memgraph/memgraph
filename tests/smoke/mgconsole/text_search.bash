#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_text_search() {
  echo "FEATURE: Indexing: Text Search"

  run_next "CREATE TEXT INDEX index_name ON :Label(prop1, prop2, prop3);"
  run_next "SHOW INDEX INFO;"

  echo "Text search and text property indices testing completed successfully"
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  # NOTE: Take a look at session_trace.bash for the v1 implementation of binary-docker picker.
  trap cleanup_memgraph_binary_processes EXIT # To make sure cleanup is done.
  set -e # To make sure the script will return non-0 in case of a failure.
  run_memgraph_binary_and_test "--log-level=TRACE --log-file=mg_test_text_search.logs" test_text_search
fi
