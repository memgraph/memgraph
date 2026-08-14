#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

# Start timing the script execution
START_TIME=$(date +%s)
START_TIME_READABLE=$(date)
echo "Script execution started at: $START_TIME_READABLE"

# Set image type
IMAGE_TYPE=${1:-"mage"}
# exit if not memgraph or mage
if [[ "$IMAGE_TYPE" != "mage" && "$IMAGE_TYPE" != "memgraph" ]]; then
  echo "Error: Invalid image type '$IMAGE_TYPE'"
  exit 1
fi
echo "Testing container with image type: $IMAGE_TYPE"

# NOTE: The arg is how to pull the image under test.
spinup_and_cleanup_memgraph_docker RC
echo "Waiting for memgraph to initialize..."
wait_for_memgraph $MEMGRAPH_DEFAULT_HOST $MEMGRAPH_BOLT_PORT
echo "Memgraph is up and running!"

# check memgraph logs inside the container for errors loading query modules
source $SCRIPT_DIR/check_container_logs.sh
check_container_logs

# check that the container has the required licenses
source $SCRIPT_DIR/check_container_licenses.sh
check_container_licenses

# Test features using mgconsole. The list of tests lives in suite.bash so that
# test_k8s.bash can run exactly the same set.
source "$SCRIPT_DIR/suite.bash"
run_feature_tests "$IMAGE_TYPE" docker

# Add all the users to be able to perform the tests.
create_test_users
run_auth_feature_tests

# NOTE: Kerberos is deliberately outside run_feature_tests: SSO has to be
# enabled at startup, so this brings up its own memgraph container (from the
# same image) plus a throwaway KDC instead of reusing memgraph_smoke. It is
# also why test_k8s.bash, which shares run_feature_tests, doesn't run it.
test_kerberos_auth

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
