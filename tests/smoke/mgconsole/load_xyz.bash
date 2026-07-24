#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_load_csv() {
  # Added in v2.0.
  echo "FEATURE: LOAD CSV"
  run_next "MATCH (n) DETACH DELETE n;"
  run_next "LOAD CSV FROM '/data/nodes.csv' WITH HEADER AS row CREATE (n:Node {id: row.id});"
  run_next_csv "MATCH (n) RETURN n;" | python3 $SCRIPT_DIR/validator.py validate_number_of_results -e 1
}

test_load_csv_ssl() {
  echo "FEATURE: LOAD CSV via SSL"
  run_next "MATCH (n) DETACH DELETE n;"
  run_next "LOAD CSV FROM 'https://download.memgraph.com/datasets/icij-pandora-papers/csv/nodes-country.csv.gz' WITH HEADER IGNORE BAD AS row CREATE (:Country { name: row.name, iso_2_code: row.iso_2_code, iso_3_code: row.iso_3_code, region: row.region, sub_region: row.sub_region });"
  run_next_csv "MATCH (n) RETURN n;" | python3 $SCRIPT_DIR/validator.py validate_number_of_results -e 63
}

test_load_parquet() {
  # Added in v3.7.
  echo "FEATURE: LOAD PARQUET"
  run_next "MATCH (n) DETACH DELETE n;"
  run_next "LOAD PARQUET FROM '/data/nodes.parquet' AS row CREATE (n:Node {id: row.id});"
  run_next_csv "MATCH (n) RETURN n;" | python3 $SCRIPT_DIR/validator.py validate_number_of_results -e 1
}

test_load_jsonl() {
  # Added in v3.8.
  echo "FEATURE: LOAD JSONL"
  run_next "MATCH (n) DETACH DELETE n;"
  run_next "LOAD JSONL FROM '/data/nodes.jsonl' AS row CREATE (n:Node {id: row.id});"
  run_next_csv "MATCH (n) RETURN n;" | python3 $SCRIPT_DIR/validator.py validate_number_of_results -e 1
}
