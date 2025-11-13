#!/bin/bash

CORE_PATH=${CORE_PATH:-/tmp/cores/core}

# Script for launching Memgraph with GDB inside a container. Override the entrypoint to use this script.
mkdir -p $(dirname ${CORE_PATH})
exec gdb -ex "set confirm off" -ex "set pagination off" -ex run -ex "thread apply all bt full" \
    -ex "generate-core-file ${CORE_PATH}" \
    --args /usr/lib/memgraph/memgraph "$@"
