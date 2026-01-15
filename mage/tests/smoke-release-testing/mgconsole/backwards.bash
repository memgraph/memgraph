#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_backwards_compatibility() {
  __host="$1"
  echo "FEATURE: Backwards compatibility"

  # NOTE: The existing containers can't easily be reused because data is mixed between different container versions which is not what we want at all.
  cleanup_docker # Stop all because we have to mount different volumens.
  docker volume rm mg_lib
  run_memgraph_last_dockerhub_container_with_volume
  wait_for_memgraph $MEMGRAPH_DEFAULT_HOST $MEMGRAPH_LAST_DATA_BOLT_PORT
  # Create some data on the previous version of Memgraph.
  echo "CREATE (n1), (n2), (n3) CREATE (n1)-[:EdgeType]->(n2)-[:EdgeType]->(n3)-[:EdgeType]->(n1);" | \
    $MEMGRAPH_CONSOLE_BINARY --host $__host --port $MEMGRAPH_LAST_DATA_BOLT_PORT
  # Test if data is there under the next version of Memgraph.
  cleanup_docker
  run_memgraph_next_dockerhub_container_with_volume
  wait_for_memgraph $MEMGRAPH_DEFAULT_HOST $MEMGRAPH_NEXT_DATA_BOLT_PORT
  echo "MATCH (n)-[r]->(m) RETURN n, r, m;" | $MEMGRAPH_CONSOLE_BINARY --host $__host --port $MEMGRAPH_NEXT_DATA_BOLT_PORT
  # TODO(gitbuda): The actual verification is required.
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  # NOTE: Take a look at session_trace.bash for the v1 implementation of binary-docker picker.
  # NOTE: It's harder to make this work e.g. with LAST binary + NEXT docker because Docker volumes and persmissions.
  # TODO(gitbuda): If no named volume -> docker run fails; memgraph can't access /var/lib/docker/... (in any way).
  #   * How to take data from docker volume and inject into ./memgraph?
  # sudo chown -R buda:buda /var/lib/docker/volumes/mg_lib/_data

  trap cleanup_docker_exit EXIT
  test_backwards_compatibility localhost
fi
