#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_ttl() {
  echo "FEATURE: Time-to-live TTL"
  run_query "ENABLE TTL EVERY '1d' AT '00:00:00';" # TODO(gitbuda): Skip TTL already running error.
  run_query "CREATE GLOBAL EDGE INDEX ON :(ttl);"
}
