#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_user_role_functions() {
  echo "FEATURE: User and Role Builtin Functions"

  echo "SUBFEATURE: USERNAME() function - returns current username"
  run_next_admin "RETURN username() AS current_user;"

  echo "SUBFEATURE: ROLES() function - returns current user roles"
  run_next_admin "RETURN roles() AS user_roles;"

  echo "SUBFEATURE: ROLES(db_name) function - returns roles for specific database"
  run_next_admin "RETURN roles('memgraph') AS db_roles;"

  echo "SUBFEATURE: Using USERNAME() in a query context"
  run_next_admin "MATCH (n) DETACH DELETE n;"
  run_next_admin "CREATE (u:User {name: username()}) RETURN u;"
  run_next_admin "MATCH (u:User) RETURN u.name;"
  run_next_admin "MATCH (n) DETACH DELETE n;"

  echo "Smoking User and Role Builtin Functions DONE"
}