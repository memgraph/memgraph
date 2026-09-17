#!/bin/bash

# Refresh the Jepsen cluster (dealloc + up).
#
# Meant to be launched in the background at the start of a CI job so the cluster
# comes up while memgraph is being built. Wait for it with
# tools/ci/wait-for-jepsen-cluster.sh just before the first step that needs the
# cluster. All output goes to the log file, and the exit status of the refresh is
# written to the status file once it is done.

set -u

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
state_dir="${RUNNER_TEMP:-/tmp}"
log_file="$state_dir/jepsen-cluster-refresh.log"
status_file="$state_dir/jepsen-cluster-refresh.status"

nodes_no=6
while [[ $# -gt 0 ]]; do
    case "$1" in
        --nodes-no)
            nodes_no="$2"
            shift 2
        ;;
        *)
            echo "Unknown option: $1"
            exit 1
        ;;
    esac
done

rm -f "$log_file" "$status_file"
echo "Refreshing Jepsen cluster ($nodes_no nodes) in the background, logs: $log_file"

{
    echo "Jepsen cluster refresh started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    cd "$script_dir/../../tests/jepsen" && ./run.sh cluster-refresh --nodes-no "$nodes_no"
} > "$log_file" 2>&1
status=$?
echo "Jepsen cluster refresh finished at $(date -u +%Y-%m-%dT%H:%M:%SZ) with status $status" >> "$log_file"
echo "$status" > "$status_file"
exit $status
