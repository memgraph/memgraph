#!/bin/bash

# Script for launching Memgraph with GDB inside a container. Override the entrypoint to use this script.

exec gdb -ex "set confirm off" -ex "set pagination off" -ex run -ex "thread apply all bt full" \
    -ex "generate-core-file /var/lib/memgraph/core" \
    --args /usr/lib/memgraph/memgraph "$@"
