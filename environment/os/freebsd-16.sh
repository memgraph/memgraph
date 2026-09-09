#!/usr/bin/env bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/lib.sh"

# FreeBSD builds against the base system's clang and lld rather than the bundled
# toolchain, so there is nothing to install for building or running one. The
# arrays stay declared because they are this adapter's interface.
TOOLCHAIN_BUILD_DEPS=(
)

TOOLCHAIN_RUN_DEPS=(
)

MEMGRAPH_BUILD_DEPS=(
    git # source code control
    cmake ninja # build system; the version in ports is what the conan profile names
    pkgconf # dependency discovery
    gmake # jemalloc and libbcrypt ship GNU-only makefiles
    bash # recipes that run shell scripts assume bash
    python3 py312-pip py312-sqlite3 # conan runs on python, and refuses to start without sqlite3
    openjdk21 # antlr4 is a java program
    flex bison # parser generation
)

# Extra packages on top of MEMGRAPH_BUILD_DEPS needed to run the test suites.
MEMGRAPH_TEST_DEPS=(
)

# The base system provides OpenSSL and the C++ runtime memgraph links against, so
# there is nothing to install to run it.
MEMGRAPH_RUN_DEPS=(
)

main "$@"
