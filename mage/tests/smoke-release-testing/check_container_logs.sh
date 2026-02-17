#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

check_container_logs() {
    # Check container logs for `[error]` while loading query modules
    echo "Checking container logs for errors..."
    logs="$(docker exec -u root memgraph_next_data bash -c 'cat /var/log/memgraph/*')"
    if echo "$logs" | grep -q "\[error\]"; then
        echo "Error(s) found in container logs:"
        echo "$logs" | grep "\[error\]"
        exit 1
    else
        echo "No errors found in container logs"
    fi
}
