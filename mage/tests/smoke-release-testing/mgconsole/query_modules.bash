#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_query_modules() {
  QUERY_MODULE_JSON=$(python3 $SCRIPT_DIR/../../../tools/ci/query_module_count/scan_query_modules.py --target all --compact)
  MG_PROC_COUNT=$(echo $QUERY_MODULE_JSON | jq -r '.memgraph.counts.procedures.total')
  MG_FUNC_COUNT=$(echo $QUERY_MODULE_JSON | jq -r '.memgraph.counts.functions.total')
  MAGE_PROC_COUNT=$(echo $QUERY_MODULE_JSON | jq -r '.mage.counts.procedures.total')
  MAGE_FUNC_COUNT=$(echo $QUERY_MODULE_JSON | jq -r '.mage.counts.functions.total')

  IMAGE_TYPE=${1:-"mage"}
  if [ "$IMAGE_TYPE" == "mage" ]; then
    expected_procedure_count=$(($MAGE_PROC_COUNT + $MG_PROC_COUNT))
    expected_function_count=$(($MAGE_FUNC_COUNT + $MG_FUNC_COUNT))
  elif [ "$IMAGE_TYPE" == "memgraph" ]; then
    expected_procedure_count=$MG_PROC_COUNT
    expected_function_count=$MG_FUNC_COUNT
  else
    echo "Invalid image type: $IMAGE_TYPE"
    exit 1
  fi
  echo "FEATURE: All Memgraph/MAGE query modules"
  run_next_csv "CALL mg.procedures() YIELD * RETURN count(*) AS cnt;" | python3 $SCRIPT_DIR/validator.py first_as_int -f cnt -e $expected_procedure_count
  run_next_csv "CALL mg.functions() YIELD * RETURN count(*) AS cnt;" | python3 $SCRIPT_DIR/validator.py first_as_int -f cnt -e $expected_function_count
  if [ "$IMAGE_TYPE" == "mage" ]; then
    run_next "CREATE (a), (b), (c), (d), (a)-[:ET]->(b), (c)-[:ET]->(d);"
    run_next "CALL leiden_community_detection.get() YIELD * RETURN communities, community_id, node;"
  fi
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  # NOTE: Take a look at session_trace.bash for the v1 implementation of binary-docker picker.
  trap cleanup_memgraph_binary_processes EXIT # To make sure cleanup is done.
  set -e # To make sure the script will return non-0 in case of a failure.
  # TODO/NOTE: Seems like SCRIPT_DIR gets override on the source call! -> FIX
  # TODO(gitbuda): When memgraph is started manually -> logs from basic.cpp module are visible -> under the below logs they are not because it's logs vs output -> FIX
  run_memgraph_binary_and_test "--log-level=TRACE --log-file=mg_test_query_modules.logs --query-modules-directory=$SCRIPT_DIR/query_modules/dist --also-log-to-stderr" test_query_modules
fi
