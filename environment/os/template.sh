#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../util.sh"

check_operating_system "todo-os-name"
check_architecture "todo-arch-name"

TOOLCHAIN_BUILD_DEPS=(
    pkg
)

TOOLCHAIN_RUN_DEPS=(
    pkg
)

MEMGRAPH_BUILD_DEPS=(
    pkg
)

MEMGRAPH_TEST_DEPS=(
  pkg
)

MEMGRAPH_RUN_DEPS=(
    pkg
)

# NEW_DEPS is useful when you won't to test the installation of a new package.
# During the test you can put here packages like wget curl tar gzip
NEW_DEPS=(
  pkg
)

list() {
    echo "$1"
}

check() {
    echo "TODO: Implement ${FUNCNAME[0]}."
    exit 1
}

install() {
    echo "TODO: Implement ${FUNCNAME[0]}."
    exit 1
}

# http://ahmed.amayem.com/bash-indirect-expansion-exploration
deps=$2"[*]"
"$1" "${!deps}"
