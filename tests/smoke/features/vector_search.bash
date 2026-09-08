#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_vector_search() {
  echo "FEATURE: Indexing: Vector Search"
  run_query "CREATE VECTOR INDEX vsi ON :Label(embedding) WITH CONFIG {\"dimension\":2, \"capacity\": 10};"
  run_query "CREATE VECTOR EDGE INDEX etvsi ON :EdgeType(embedding) WITH CONFIG {\"dimension\": 256, \"capacity\": 1000};"
  run_query "CALL vector_search.show_index_info() YIELD * RETURN *;"
  run_query "SHOW VECTOR INDEX INFO;"
}
