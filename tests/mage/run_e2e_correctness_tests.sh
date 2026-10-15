#!/bin/bash -e

MEMGRAPH_PORT=$1
NEO4J_PORT=$2
NEO4J_CONTAINER=$3
MAGE_CONTAINER=$4
MEMGRAPH_NETWORK=$5


NEO4J_START_TIMEOUT="${NEO4J_START_TIMEOUT:-180}"

remove_neo4j() {
  docker stop "$NEO4J_CONTAINER" >/dev/null 2>&1 || true
  docker rm -f "$NEO4J_CONTAINER" >/dev/null 2>&1 || true
}

# Report why neo4j never came up, then take the container away.
neo4j_start_failed() {
  echo "$1"
  echo "--- last 50 lines of '$NEO4J_CONTAINER' logs ---"
  docker logs --tail 50 "$NEO4J_CONTAINER" 2>&1 || echo "(no logs available)"
  exit 1
}

remove_neo4j

echo "Start Neo4j..."
docker run \
    --name "$NEO4J_CONTAINER"  \
    --network "$MEMGRAPH_NETWORK" \
    -p 7474:7474 \
    -p "$NEO4J_PORT":7687 \
    -d \
    -v "$HOME/neo4j/plugins:/plugins" \
    --env NEO4J_AUTH=none  \
    -e NEO4J_apoc_export_file_enabled=true \
    -e NEO4J_apoc_import_file_enabled=true \
    -e NEO4J_apoc_import_file_use__neo4j__config=true  \
    -e NEO4J_PLUGINS='["apoc"]' neo4j:5.10.0
trap remove_neo4j EXIT INT TERM

echo "Waiting up to ${NEO4J_START_TIMEOUT}s for Neo4j Bolt to accept queries..."
counter=0
while ! docker exec "$NEO4J_CONTAINER" \
        cypher-shell -a bolt://localhost:7687 "RETURN 1" >/dev/null 2>&1; do
  if [ "$(docker inspect -f '{{.State.Running}}' "$NEO4J_CONTAINER" 2>/dev/null)" != "true" ]; then
    neo4j_start_failed "Neo4j container '$NEO4J_CONTAINER' stopped running after ${counter}s."
  fi
  sleep 1
  counter=$((counter+1))
  if [ $counter -gt "$NEO4J_START_TIMEOUT" ]; then
    neo4j_start_failed "Neo4j did not accept Bolt queries within ${NEO4J_START_TIMEOUT}s."
  fi
done
echo "Neo4j is up and running (Bolt ready after ${counter}s)."

echo "Running e2e correctness tests..."
python3 test_e2e_correctness.py --memgraph-port $MEMGRAPH_PORT --neo4j-port $NEO4J_PORT --neo4j-container $NEO4J_CONTAINER

echo "Stopping Neo4j..."
remove_neo4j
