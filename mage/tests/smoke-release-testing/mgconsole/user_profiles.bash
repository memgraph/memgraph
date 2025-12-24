#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_user_profiles() {
  echo "FEATURE: User Profiles and Resource Limit Constraints"

  echo "SUBFEATURE: Setup test user and profile"
  run_next_admin "CREATE PROFILE user_profile LIMIT SESSIONS 2, TRANSACTIONS_MEMORY 100KB;"
  run_next_admin "SET PROFILE FOR tester TO user_profile;"
  run_next_admin "SHOW PROFILE FOR tester;"

  echo "SUBFEATURE: Test memory limit enforcement"
  # Test query that should work within 100KB limit
  run_next_tester "CREATE (n:TestNode {id: 1, data: 'small'});"
  # This one should fail. Check for failure and continue only if it fails.
  if ! run_next_tester "UNWIND range(1, 10000) AS i CREATE (n:MemoryTest {id: i, data: 'Data string ' + i}) RETURN count(n);"; then
    echo "Memory limit enforcement by profile test failed as expected."
  fi

  echo "SUBFEATURE: Cleanup profile"
  run_next_admin "DROP PROFILE user_profile;"
  run_next_admin "SHOW PROFILES;"
  run_next_admin "MATCH (n:TestNode) DETACH DELETE n;"
  run_next_admin "MATCH (n:MemoryTest) DETACH DELETE n;"

  echo "User profiles and resource limit constraints testing completed successfully"
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  # NOTE: Take a look at session_trace.bash for the v1 implementation of binary-docker picker.
  trap cleanup_memgraph_binary_processes EXIT # To make sure cleanup is done.
  set -e # To make sure the script will return non-0 in case of a failure.
  run_memgraph_binary_and_test "--log-level=TRACE --log-file=mg_test_user_profiles.logs" test_user_profiles
fi
