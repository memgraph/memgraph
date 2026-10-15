#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_show_database_settings() {
  echo "FEATURE: Show Database Settings"
  run_query_admin "SHOW DATABASE SETTINGS;"
}

test_auth_roles() {
  echo "FEATURE: Auth Roles"
  run_query_admin "CREATE ROLE IF NOT EXISTS test_reader;"
  run_query_admin "SHOW ROLES;"
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  # A place to run tests using memgraph binary and debug issues directly.
  echo "pass"
fi
