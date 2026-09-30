#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

# NOTE: Use the below line if you just want to spin up the container and leave it running.
# run_memgraph_docker_container RC
# NOTE: Use the below line if you want to cleanup the container after run of this script.
spinup_and_cleanup_memgraph_docker RC
echo "Waiting for memgraph to initialize..."
wait_for_memgraph $MEMGRAPH_DEFAULT_HOST $MEMGRAPH_BOLT_PORT
echo "Memgraph is up and running!"

source ./features/load_xyz.bash
test_load_parquet $MEMGRAPH_DEFAULT_HOST $MEMGRAPH_BOLT_PORT

# NOTE: Test what's the exit status of the script by using `echo $?`:
#   * if it's == 0 -> all good
#   * if it's != 0 -> something went wrong.
