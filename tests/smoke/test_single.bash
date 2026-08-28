#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

# Start timing the script execution
START_TIME=$(date +%s)
START_TIME_READABLE=$(date)
echo "Script execution started at: $START_TIME_READABLE"

# Set image type, and whether this is the FIPS image.
IMAGE_TYPE="mage"
MODE="normal"
for arg in "$@"; do
  case "$arg" in
    --fips) MODE="fips" ;;
    *)      IMAGE_TYPE="$arg" ;;
  esac
done
# exit if not memgraph or mage
if [[ "$IMAGE_TYPE" != "mage" && "$IMAGE_TYPE" != "memgraph" ]]; then
  echo "Error: Invalid image type '$IMAGE_TYPE'"
  exit 1
fi
if [[ "$MODE" == "fips" && "$IMAGE_TYPE" != "memgraph" ]]; then
  echo "Error: --fips is only supported for the memgraph image (got '$IMAGE_TYPE')"
  exit 1
fi
echo "Testing container with image type: $IMAGE_TYPE (mode: $MODE)"

# Starts the container in approved mode. Exported so utils.bash picks it up
# when it runs the container.
if [[ "$MODE" == "fips" ]]; then
  export MEMGRAPH_FIPS_FLAGS="--fips-mode=true"
fi

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
run_feature_tests "$IMAGE_TYPE" docker "$MODE"

# Add all the users to be able to perform the tests.
create_test_users
run_auth_feature_tests "$MODE"

# NOTE: Kerberos is deliberately outside run_feature_tests: SSO has to be
# enabled at startup, so this brings up its own memgraph container (from the
# same image) plus a throwaway KDC instead of reusing memgraph_smoke. It is
# also why test_k8s.bash, which shares run_feature_tests, doesn't run it.
# Setup is its own step so that this script's errexit stops on the exact
# command that broke, rather than the feature having to police itself.
if [[ "$MODE" != "fips" ]]; then
  test_kerberos_auth_setup
  test_kerberos_auth
else
  echo "SKIP FEATURE: Kerberos SSO (its auth module is Python, absent from the FIPS image)"
fi

# Refusing to start on a non-approved password algorithm is a startup-time
# guarantee, so it needs its own container rather than the running one.
if [[ "$MODE" == "fips" ]]; then
  test_fips_refuses_non_approved_algorithm
fi

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
