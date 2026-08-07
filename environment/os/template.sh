#!/bin/bash
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/lib.sh"

# Copy this file to <os>-<version>.sh -- the filename IS the OS name (one
# script serves all architectures), used for the OS check and check-packages.py.
# Fill in the package arrays; lib.sh provides list/check/install and the
# dispatch. For distro-specific extras (extra repos, packages that need a
# non-repo install, post-install fixups) define the optional hooks documented
# in lib.sh: setup_repos, SPECIAL_PACKAGES + install_special_package,
# post_install. Remember to add the new OS to test.sh's image map.

TOOLCHAIN_BUILD_DEPS=(
    pkg
)

TOOLCHAIN_RUN_DEPS=(
    pkg
)

MEMGRAPH_BUILD_DEPS=(
    pkg
)

# Extra packages on top of MEMGRAPH_BUILD_DEPS needed to run the test suites.
MEMGRAPH_TEST_DEPS=(
    pkg
)

MEMGRAPH_RUN_DEPS=(
    pkg
)

main "$@"
