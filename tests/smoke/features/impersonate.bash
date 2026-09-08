#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_impersonate_user() {
  echo "FEATURE: Impersonate User"
  # NOTE: TO USER -> "admin" is also a built-in role name.
  run_query_admin "GRANT IMPERSONATE_USER * TO USER admin;"
}
