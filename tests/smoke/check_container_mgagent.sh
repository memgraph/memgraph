#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

check_container_mgagent() {
    # Check that the memgraph-agent package left both its binary and its EULA
    # in the image. The EULA lives under /usr/share/doc, which Ubuntu images
    # exclude by default, so it only survives through the path-include the
    # Dockerfiles add for it.
    echo "Checking mgagent..."

    if ! docker exec memgraph_smoke bash -c "test -x /usr/bin/mgagent" > /dev/null 2>&1; then
        echo "Error: mgagent binary not found (or not executable) at /usr/bin/mgagent"
        exit 1
    fi
    echo "mgagent binary found"

    if ! docker exec memgraph_smoke bash -c "test -s /usr/share/doc/memgraph-agent/EULA.txt" > /dev/null 2>&1; then
        echo "Error: mgagent EULA not found (or empty) at /usr/share/doc/memgraph-agent/EULA.txt"
        exit 1
    fi
    echo "mgagent EULA found"
}
