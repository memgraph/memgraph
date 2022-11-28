#!/bin/bash

set -euox pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WORKSPACE_DIR=${SCRIPT_DIR}/../../
CPUS=$(grep -c processor < /proc/cpuinfo)

# Get all benchmark files
BENCHMARK_FILES=$(find ${WORKSPACE_DIR}/tests/benchmark -type f -iname "data_structures_*")
echo $(ls ${WORKSPACE_DIR}/tests/benchmark)
for BENCH_FILE in ${BENCHMARK_FILES}; do
    BASE_NAME=$(basename $BENCH_FILE)
    NAME=${BASE_NAME%%.*}
    echo "Running $NAME"
    TEST_FILE=${WORKSPACE_DIR}/build/tests/benchmark/${NAME}
    if [[ -f "${TEST_FILE}" ]]; then
        pushd ${WORKSPACE_DIR}/build
            make -j${CPUS} memgraph__benchmark__${NAME}
        popd
        JSON_OUTPUT=${NAME}_output.json
        # Run benchmakr test
        ${WORKSPACE_DIR}/build/tests/benchmark/${NAME} --benchmark_format=json --benchmark_out=${JSON_OUTPUT}
        # Run analyze script for benchmark test
        python3 ${WORKSPACE_DIR}/tools/plot/benchmark_datastructures.py --log_file=${JSON_OUTPUT}
    else
        echo "File ${TEST_FILE} does not exist!"
    fi
done
