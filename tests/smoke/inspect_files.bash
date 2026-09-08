#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

spinup_and_cleanup_memgraph_docker
echo "Waiting for memgraph to initialize..."
wait_for_memgraph $MEMGRAPH_DEFAULT_HOST $MEMGRAPH_BOLT_PORT
echo "Memgraph is up and running!"

docker exec -it memgraph_smoke bash -c "test -x /usr/lib/memgraph/memgraph"
docker exec -it memgraph_smoke bash -c "test -d /usr/lib/memgraph/auth_module"
docker exec -it memgraph_smoke bash -c "test -d /usr/lib/memgraph/query_modules"
docker exec -it memgraph_smoke bash -c "test -d /usr/lib/memgraph/python_support"
