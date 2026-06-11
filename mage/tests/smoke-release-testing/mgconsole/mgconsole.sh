#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

test_mgconsole() {
    # tests whether the `mgconsole` binary exists and works inside the container
    expected_version="${1:-1.6}"
    expected_path="/usr/bin/mgconsole"
    # Use `command -v` (bash builtin) instead of `which` — minimal CentOS 10
    # and similar base images don't ship the `which` utility, causing
    # exit 127 + silent script abort under `set -e`.
    # Pipe through `realpath` so UsrMerge distros (Fedora, modern RHEL) where
    # /usr/sbin is a symlink to /usr/bin resolve to the canonical path
    # regardless of PATH ordering.
    path="$(docker exec -u memgraph memgraph_next_data bash -c 'realpath "$(command -v mgconsole)"')"
    if [ "$path" != "$expected_path" ]; then
        echo "Error: mgconsole binary not found inside container PATH"
        echo "Expected path: $expected_path"
        echo "Actual path: $path"
        exit 1
    fi
    version="$(docker exec -u memgraph memgraph_next_data mgconsole --version)"
    if [[ "$version" != *"$expected_version"* ]]; then
        echo "Error: mgconsole version not found at $version"
        exit 1
    fi
    echo "mgconsole binary found at $path and version $version"
}
