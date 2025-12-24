#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_load_parquet() {
  # Added in v3.7.
  echo "FEATURE: LOAD PARQUET"
  run_next "MATCH (n) DETACH DELETE n;"
  run_next "LOAD PARQUET FROM '/data/nodes.parquet' AS row CREATE (n:Node {id: row.id});"
  run_next_csv "MATCH (n) RETURN n;" | python3 $SCRIPT_DIR/validator.py validate_number_of_results -e 1
}
