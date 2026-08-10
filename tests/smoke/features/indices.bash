#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_composite_indices() {
  echo "FEATURE: Label Property Composit Index"
  run_query "CREATE INDEX ON :Label(prop1, prop2);"
  run_query "CREATE (:Label {prop1:0, prop2: 1});"
  run_query "EXPLAIN MATCH (n:Label {prop1:0, prop2: 1}) RETURN n;" | grep -E "ScanAllByLabelProperties \(n :Label \{prop1, prop2\}\)"
  run_query "SHOW INDEXES;"
}

test_nested_indices() {
  echo "FEATURE: Nested Indices"
  run_query "CREATE INDEX ON :Project(delivery.status.due_date);"
  run_query "CREATE (:Project {delivery: {status: {due_date: date('2025-06-04'), milestone: 'v3.14'}}});"
  run_query "EXPLAIN MATCH (proj:Project) WHERE proj.delivery.status.due_date = date('2025-06-04') RETURN *;" | grep -E "ScanAllByLabelProperties"
  run_query "SHOW INDEXES;"
}
