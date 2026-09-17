#!/bin/bash

# Wait for the cluster refresh started by tools/ci/jepsen-cluster-refresh.sh to
# finish, then check that the cluster is actually ready to run tests.

set -u

state_dir="${RUNNER_TEMP:-/tmp}"
log_file="$state_dir/jepsen-cluster-refresh.log"
status_file="$state_dir/jepsen-cluster-refresh.status"

nodes_no=6
timeout=1800
interval=5
elapsed=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes-no)
            nodes_no="$2"
            shift 2
        ;;
        --timeout)
            timeout="$2"
            shift 2
        ;;
        *)
            echo "Unknown option: $1"
            exit 1
        ;;
    esac
done

echo "Waiting for the Jepsen cluster refresh to finish (timeout: ${timeout}s)..."
while [ ! -f "$status_file" ]; do
    if [ "$elapsed" -ge "$timeout" ]; then
        echo "Timeout: Jepsen cluster refresh did not finish after ${timeout} seconds"
        [ -f "$log_file" ] && cat "$log_file"
        exit 1
    fi
    sleep "$interval"
    elapsed=$((elapsed + interval))
    echo "Still waiting... (${elapsed}s/${timeout}s)"
done

[ -f "$log_file" ] && cat "$log_file"

status=$(cat "$status_file")
if [ "$status" -ne 0 ]; then
    echo "Jepsen cluster refresh FAILED with status $status"
    exit "$status"
fi

containers=("jepsen-control")
for iter in $(seq 1 "$nodes_no"); do
    containers+=("jepsen-n$iter")
done

for container in "${containers[@]}"; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null)" != "true" ]; then
        echo "Jepsen container $container is not running"
        docker ps -a --filter "name=jepsen*"
        exit 1
    fi
done

echo "Jepsen cluster is ready (${#containers[@]} containers running)."
