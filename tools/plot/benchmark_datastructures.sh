#!/bin/bash

set -euox pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WORKSPACE_DIR=${SCRIPT_DIR}/../../

BENCHMARK_FILES=$(find ${WORKSPACE_DIR}/tests/benchmark -type f -iname data_structures_*)
echo $BENCHMARK_FILES
for bench_file in ${BENCHMARK_FILES}; do
    echo "Running $name"
    base_name=$(basename $bench_file)
    name=${base_name%%.*}
    ${WORKSPACE_DIR}/build/tests/benchmark/${name} --benchmark_format=json --benchmark_out=${name}_output.json
    python3 ${WORKSPACE_DIR}/tools/plot/benchmark_datastructures.py --log-file=${name}_output.json
done
