#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_shortest_paths() {
  echo "FEATURE: Deep-path Traversal Capabilities"
  run_next "MATCH (n) DETACH DELETE n;"

  create_graph_query="
    CREATE
      (a:Node {id: 'A'}),
      (b:Node {id: 'B'}),
      (c:Node {id: 'C'}),
      (d:Node {id: 'D'}),
      (e:Node {id: 'E'}),
      (f:Node {id: 'F'}),
      (g:Node {id: 'G'}),
      (a)-[:REL {weight: 1}]->(b),
      (a)-[:REL {weight: 2}]->(c),
      (b)-[:REL {weight: 1}]->(e),
      (c)-[:REL {weight: 1}]->(e),
      (a)-[:REL {weight: 3}]->(e),
      (b)-[:REL {weight: 2}]->(d),
      (d)-[:REL {weight: 1}]->(e),
      (a)-[:REL {weight: 1}]->(f),
      (f)-[:REL {weight: 1}]->(g),
      (g)-[:REL {weight: 1}]->(e);
  "
  run_next "$create_graph_query"

  echo "SUBFEATURE: KShortest paths - find top 3 shortest paths from A to E"
  run_next "MATCH (n1:Node {id: 'A'}), (n2:Node {id: 'E'}) WITH n1, n2 MATCH p=(n1)-[*KShortest | 3]->(n2) RETURN p;"
  echo "SUBFEATURE: KShortest paths - with path length bounds"
  run_next "MATCH (n1:Node {id: 'A'}), (n2:Node {id: 'E'}) WITH n1, n2 MATCH p=(n1)-[*KShortest 2..4]->(n2) RETURN p;"
  echo "SUBFEATURE: Test KShortest paths with the bounds and limit"
  run_next "MATCH (n1:Node {id: 'A'}), (n2:Node {id: 'E'}) WITH n1, n2 MATCH p=(n1)-[*KShortest 2..4 | 5]->(n2) RETURN p;"

  echo "SUBFEATURE: AllShortest paths - find all shortest paths from A to E (with weight lambda)"
  run_next "MATCH (n1:Node {id: 'A'}), (n2:Node {id: 'E'}) WITH n1, n2 MATCH p=(n1)-[*AllShortest (r, n | r.weight)]->(n2) RETURN p;"
  echo "SUBFEATURE: AllShortest paths - with the upper bound and total_weight"
  run_next "MATCH (n1:Node {id: 'A'}), (n2:Node {id: 'E'}) WITH n1, n2 MATCH p=(n1)-[*AllShortest ..4 (r, n | r.weight) total_weight]->(n2) RETURN p, total_weight;"
  echo "SUBFEATURE: AllShortest paths - using weight lambda + total_weight + the filter"
  run_next "MATCH p=(n1:Node {id: 'A'})-[*AllShortest ..4 (r, n | r.weight) total_weight (r, n, p, w | r.weight > 0 AND length(p) > 0)]->(n2:Node {id: 'E'}) RETURN p;"

  run_next "MATCH (n) DETACH DELETE n;"
  echo "Smoking Deep-path Traversal Capabilities DONE"
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  # NOTE: Take a look at session_trace.bash for the v1 implementation of binary-docker picker.
  trap cleanup_memgraph_binary_processes EXIT # To make sure cleanup is done.
  set -e # To make sure the script will return non-0 in case of a failure.
  run_memgraph_binary_and_test "--log-level=TRACE --log-file=mg_test_shortest_paths.logs" test_shortest_paths
fi
