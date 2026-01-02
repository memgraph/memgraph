#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

# Start timing the script execution
START_TIME=$(date +%s)
START_TIME_READABLE=$(date)
echo "Script execution started at: $START_TIME_READABLE"

# NOTE: 1st arg is how to pull LAST image, 2nd arg is how to pull NEXT image.
spinup_and_cleanup_memgraph_dockers Dockerhub RC
echo "Waiting for memgraph to initialize..."
wait_for_memgraph $MEMGRAPH_DEFAULT_HOST $MEMGRAPH_LAST_DATA_BOLT_PORT
wait_for_memgraph $MEMGRAPH_DEFAULT_HOST $MEMGRAPH_NEXT_DATA_BOLT_PORT
echo "Memgraph is up and running!"

# Test features using mgconsole.
for test_file_path in "$SCRIPT_DIR/mgconsole/"*; do
  if [ "$(basename $test_file_path)" == "README.md" ]; then
    continue
  fi
  source $test_file_path
  echo "Loaded $test_file_path..."
done

test_auth_roles
test_basic_auth
test_query
test_query_modules
test_session_trace
test_show_schema_info
test_spatial
test_storage
test_streams
test_ttl
test_type_constraints
test_vector_search
test_dynamic_algos
test_functions
test_label_operations
test_regex
test_edge_type_operations
test_composite_indices
test_monitoring
test_multi_tenancy
test_nested_indices
test_or_expression_for_labels
test_shortest_paths
test_text_search
test_durability
test_load_parquet

# NOTE: If the testing container is NOT restarted (each test having their own
# container), all the auth test have to come after all tests that assume there
# are no users.
# Add all the users to be able to perform the tests.
echo "CREATE USER admin IDENTIFIED BY 'admin1234'; GRANT ALL PRIVILEGES TO admin;" | $MGCONSOLE_NEXT_DEFAULT
echo "CREATE USER tester IDENTIFIED BY 'tester1234'; GRANT CREATE TO tester; GRANT CREATE ON NODES CONTAINING LABELS * TO tester; GRANT DELETE ON NODES CONTAINING LABELS * TO tester; GRANT DATABASE memgraph TO tester;" | $MGCONSOLE_NEXT_ADMIN
echo "SHOW USERS;" | $MGCONSOLE_NEXT_ADMIN
echo "SHOW ACTIVE USERS;" | $MGCONSOLE_NEXT_ADMIN
echo "NOTE: admin and tester users are created for testing purposes."

test_show_database_settings
test_auth_roles
test_impersonate_user
test_user_profiles

# End timing and calculate execution time
END_TIME=$(date +%s)
END_TIME_READABLE=$(date)
EXECUTION_TIME=$((END_TIME - START_TIME))
EXECUTION_TIME_MINUTES=$((EXECUTION_TIME / 60))
EXECUTION_TIME_SECONDS=$((EXECUTION_TIME % 60))

echo ""
echo "Script execution completed at: $END_TIME_READABLE"
echo "Total execution time: ${EXECUTION_TIME_MINUTES}m ${EXECUTION_TIME_SECONDS}s (${EXECUTION_TIME} seconds)"
echo ""
