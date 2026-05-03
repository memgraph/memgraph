#!/bin/bash

CORE_PATH=${CORE_PATH:-/tmp/cores/core}

# Script for launching Memgraph with GDB inside a container. Override the entrypoint to use this script.
mkdir -p $(dirname ${CORE_PATH})

# Prefer the bundled toolchain gdb (16.x) shipped with relwithdebinfo images;
# distro gdb (15.x) crashes on our DWARF 5 + ThinLTO + split-DWARF binaries.
if [[ -x /usr/lib/memgraph/gdb/bin/gdb ]]; then
    GDB=/usr/lib/memgraph/gdb/bin/gdb
    GDB_DATA_DIR_ARG=("--data-directory" "/usr/lib/memgraph/gdb/share/gdb")
else
    GDB=gdb
    GDB_DATA_DIR_ARG=()
fi

# Compile-time -ffile-prefix-map normalized source paths to "./src/...".
# Map that to the source tree COPY'd into the image so backtraces show source.
SUBST_ARGS=()
if [[ -d /home/mg/memgraph/src ]]; then
    SUBST_ARGS=(-ex "set substitute-path ./ /home/mg/memgraph/")
fi

exec "$GDB" "${GDB_DATA_DIR_ARG[@]}" \
    -ex "set confirm off" -ex "set pagination off" \
    "${SUBST_ARGS[@]}" \
    -ex run -ex "thread apply all bt full" \
    -ex "generate-core-file ${CORE_PATH}" \
    --args /usr/lib/memgraph/memgraph "$@"
