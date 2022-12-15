#!/bin/bash

set -euox pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WORKSPACE_DIR=${SCRIPT_DIR}/../../
CPUS=$(grep -c processor < /proc/cpuinfo)

# Get all benchmark files
BENCHMARK_FILES=$(find ${WORKSPACE_DIR}/tests/benchmark -type f -iname "data_structures_*")

function test_all() {
    for BENCH_FILE in ${BENCHMARK_FILES[@]}; do
        local BASE_NAME=$(basename $BENCH_FILE)
        local NAME=${BASE_NAME%%.*}
        echo "Running $NAME"
        local TEST_FILE=${WORKSPACE_DIR}/build/tests/benchmark/${NAME}
        if [[ -f "${TEST_FILE}" ]]; then
            pushd ${WORKSPACE_DIR}/build
                make -j${CPUS} memgraph__benchmark__${NAME}
            popd
            local JSON_OUTPUT=${NAME}_output.json
            # Run benchmakr test
            ${WORKSPACE_DIR}/build/tests/benchmark/${NAME} --benchmark_format=json --benchmark_out=${JSON_OUTPUT}
            # Run analyze script for benchmark test
            python3 ${WORKSPACE_DIR}/tools/plot/benchmark_datastructures.py --log_file=${JSON_OUTPUT}
        else
            echo "File ${TEST_FILE} does not exist!"
        fi
    done
}

function test_memory() {
    ## We are testing only insert
    local DATA_STRUCTURES=(SkipList StdMap StdSet BppTree)
    for DATA_STRUCTURE in ${DATA_STRUCTURES[@]}; do
        valgrind --tool=massif --massif-out-file=${DATA_STRUCTURE}.massif.out ${WORKSPACE_DIR}/build/tests/benchmark/data_structures_insert --benchmark_filter=BM_BenchmarkInsert${DATA_STRUCTURE}/10000 --benchmark_format=json --benchmark_out=${DATA_STRUCTURE}.json
    done
}

ARG_1=${1:-"all"}
case ${ARG_1} in
  all)
    test_all
    ;;
  memory)
    test_memory
    ;;
  *)
    echo "Select either `all` or `memory` benchmark!"
    ;;
esac
