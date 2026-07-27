#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_parallel_runtime() {
  echo "FEATURE: Parallel Runtime Execution"
  run_next "MATCH (n) DETACH DELETE n;"

  echo "SUBFEATURE: Create test data"
  create_graph_query="
    UNWIND range(1, 100) AS i
    CREATE (n:Node {id: i, value: i * 10, category: i % 5})
    WITH n, i
    WHERE i > 1
    MATCH (m:Node {id: i - 1})
    CREATE (m)-[:CONNECTED_TO {weight: i}]->(n);
  "
  run_next "$create_graph_query"

  echo "SUBFEATURE: Basic parallel execution scan"
  run_next "USING PARALLEL EXECUTION MATCH (n:Node) RETURN count(n);"

  echo "SUBFEATURE: Parallel execution with explicit thread count"
  run_next "USING PARALLEL EXECUTION 4 MATCH (n:Node) RETURN count(n);"

  echo "SUBFEATURE: Parallel execution with aggregation"
  run_next "USING PARALLEL EXECUTION MATCH (n:Node) RETURN n.category AS category, sum(n.value) AS total ORDER BY category;"

  echo "SUBFEATURE: Parallel execution with filtering"
  run_next "USING PARALLEL EXECUTION MATCH (n:Node) WHERE n.value > 500 RETURN n.id, n.value ORDER BY n.value;"

  echo "SUBFEATURE: Parallel execution with ordering"
  run_next "USING PARALLEL EXECUTION MATCH (n:Node) RETURN n.id, n.value ORDER BY n.value DESC LIMIT 10;"

  echo "SUBFEATURE: Parallel execution with edge scan"
  run_next "USING PARALLEL EXECUTION MATCH (n:Node)-[r:CONNECTED_TO]->(m:Node) RETURN count(r);"

  echo "SUBFEATURE: Parallel execution with distinct"
  run_next "USING PARALLEL EXECUTION MATCH (n:Node) RETURN DISTINCT n.category ORDER BY n.category;"

  run_next "MATCH (n) DETACH DELETE n;"
  echo "Smoking Parallel Runtime Execution DONE"
}
