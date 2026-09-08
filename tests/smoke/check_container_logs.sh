#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

check_container_logs() {
    # Check container logs for `[error]` while loading query modules
    echo "Checking container logs for errors..."
    logs="$(docker exec -u root memgraph_smoke bash -c 'cat /var/log/memgraph/*')"
    if echo "$logs" | grep -q "\[error\]"; then
        echo "Error(s) found in container logs:"
        echo "$logs" | grep "\[error\]"
        echo "----- full memgraph log (memgraph_smoke) -----"
        echo "$logs"
        echo "----- end memgraph log -----"
        exit 1
    else
        echo "No errors found in container logs"
    fi
}
