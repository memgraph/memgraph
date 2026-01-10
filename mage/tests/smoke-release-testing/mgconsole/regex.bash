#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_regex() {
  echo "FEATURE: Regex"
  run_next "CREATE (:Hero {name: 'xSPIDERy'});"
  run_next "CREATE (:Hero {name: 'test'});"
  run_next "MATCH (h:Hero) WHERE h.name =~ \".*SPIDER.+\" RETURN h.name as PotentialSpiderDude ORDER BY PotentialSpiderDude;"
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  # NOTE: Take a look at session_trace.bash for the v1 implementation of binary-docker picker.
  trap cleanup_memgraph_binary_processes EXIT # To make sure cleanup is done.
  set -e # To make sure the script will return non-0 in case of a failure.
  run_memgraph_binary_and_test "--log-level=TRACE --log-file=mg_test_regex.logs" test_template
fi
