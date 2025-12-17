#!/bin/bash

MEMGRAPH_BINARY="../../build/memgraph"

# Try to find mgconsole: first in build dir, then in toolchain
if [[ -x "../../build/mgconsole" ]]; then
    MGCONSOLE_BINARY="../../build/mgconsole"
elif [[ -n "$MG_TOOLCHAIN_ROOT" && -x "$MG_TOOLCHAIN_ROOT/bin/mgconsole" ]]; then
    MGCONSOLE_BINARY="$MG_TOOLCHAIN_ROOT/bin/mgconsole"
elif [[ -x "/opt/toolchain-v7/bin/mgconsole" ]]; then
    MGCONSOLE_BINARY="/opt/toolchain-v7/bin/mgconsole"
else
    MGCONSOLE_BINARY="mgconsole"  # Fall back to PATH lookup
fi

DATA_DIR_PREFIX="mg_data"
COORD_DIR_PREFIX="mg_coord"

# Default flags for Memgraph Data Nodes
DEFAULT_DATA_FLAGS=(
    "--storage-properties-on-edges=true"
    "--storage-snapshot-on-exit=false"
    "--storage-snapshot-interval-sec=600"
    "--storage-snapshot-retention-count=1"
    "--storage-wal-enabled=true"
    "--query-execution-timeout-sec=1200"
    "--log-file="
    "--log-level=ERROR"
    "--also-log-to-stderr=true"
)

# Default flags for Memgraph Coordinator Nodes
DEFAULT_COORD_FLAGS=(
    "--log-file="
    "--log-level=ERROR"
    "--also-log-to-stderr=true"
)

# Configuration for Data Nodes
DATA_NODES=(
    "--bolt-port=7687 --management-port=13011 --monitoring-port=7444 --metrics-port=9091 --data-directory=${DATA_DIR_PREFIX}_1"
    "--bolt-port=7688 --management-port=13012 --monitoring-port=7445 --metrics-port=9092 --data-directory=${DATA_DIR_PREFIX}_2"
    "--bolt-port=7689 --management-port=13013 --monitoring-port=7446 --metrics-port=9093 --data-directory=${DATA_DIR_PREFIX}_3"
)

# Configuration for Coordinator Nodes
COORD_NODES=(
    "--coordinator-id=1 --coordinator-hostname=127.0.0.1 --bolt-port=7691 --management-port=12121 --coordinator-port=10111 --monitoring-port=7447 --metrics-port=9094 --data-directory=${COORD_DIR_PREFIX}_1"
    "--coordinator-id=2 --coordinator-hostname=127.0.0.1 --bolt-port=7692 --management-port=12122 --coordinator-port=10112 --monitoring-port=7448 --metrics-port=9095 --data-directory=${COORD_DIR_PREFIX}_2"
    "--coordinator-id=3 --coordinator-hostname=127.0.0.1 --bolt-port=7693 --management-port=12123 --coordinator-port=10113 --monitoring-port=7449 --metrics-port=9096 --data-directory=${COORD_DIR_PREFIX}_3"
)

clean_data_directories() {
    echo "Cleaning up data and coordinator directories..."
    for i in {1..3}; do
        rm -rf "${DATA_DIR_PREFIX}_$i" "${COORD_DIR_PREFIX}_$i"
    done
}

merge_flags() {
    local default_flags=("$@")  # Default flags passed as arguments
    shift $#
    local provided_flags=("$@")  # Capture all provided flags

    local final_flags=("${default_flags[@]}")  # Start with default flags

    # Loop through user-provided flags
    for flag in "${provided_flags[@]}"; do
        flag_name="${flag%%=*}"  # Extract flag name (before '=')
        flag_value="${flag#*=}"  # Extract flag value (after '=')

        # Check if flag is overriding a default flag
        found=false
        for i in "${!final_flags[@]}"; do
            if [[ "${final_flags[$i]}" == "$flag_name="* ]]; then
                final_flags[$i]="$flag"  # Override existing flag
                found=true
                break
            fi
        done

        # If flag wasn't found in defaults, append it
        if [[ "$found" == false ]]; then
            final_flags+=("$flag")
        fi
    done

    echo "${final_flags[@]}"  # Return the merged flags as a single string
}

start_memgraph() {
    echo "Starting Memgraph HA Deployment..."

    # Ensure enterprise license env vars are exported for child processes (required for HA)
    export MEMGRAPH_ENTERPRISE_LICENSE
    export MEMGRAPH_ORGANIZATION_NAME

    # Ensure clean data directories before startup
    clean_data_directories

    rm -f memgraph_ha.pid  # Ensure no old PID file exists

    # Start data nodes
    for node in "${DATA_NODES[@]}"; do
        FINAL_FLAGS=$(merge_flags "${DEFAULT_DATA_FLAGS[@]}" "$@")
        CMD="$MEMGRAPH_BINARY $node $FINAL_FLAGS"
        echo "Executing: $CMD"
        $CMD &  # Run the command
        echo $! >> memgraph_ha.pid
    done

    # Start coordinator nodes
    for node in "${COORD_NODES[@]}"; do
        FINAL_FLAGS=$(merge_flags "${DEFAULT_COORD_FLAGS[@]}" "$@")
        CMD="$MEMGRAPH_BINARY $node $FINAL_FLAGS"
        echo "Executing: $CMD"
        $CMD &  # Run the command
        echo $! >> memgraph_ha.pid
    done

    wait_for_server 7687
    wait_for_server 7688
    wait_for_server 7689
    wait_for_server 7691
    wait_for_server 7692
    wait_for_server 7693

    setup_ha
}


setup_ha() {
    echo "Setting up HA configuration using mgconsole..."

    # Check if mgconsole binary exists
    if [[ ! -x "$MGCONSOLE_BINARY" ]]; then
        echo "ERROR: mgconsole binary not found at $MGCONSOLE_BINARY"
        stop_memgraph
        exit 1
    fi

    sleep 2  # Ensure coordinators are fully started

    echo "Adding coordinators..."
    if ! echo "
    ADD COORDINATOR 1 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7691\", \"coordinator_server\": \"127.0.0.1:10111\", \"management_server\": \"127.0.0.1:12121\"};
    ADD COORDINATOR 2 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7692\", \"coordinator_server\": \"127.0.0.1:10112\", \"management_server\": \"127.0.0.1:12122\"};
    ADD COORDINATOR 3 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7693\", \"coordinator_server\": \"127.0.0.1:10113\", \"management_server\": \"127.0.0.1:12123\"};
    " | $MGCONSOLE_BINARY --host 127.0.0.1 --port 7691; then
        echo "ERROR: Failed to add coordinators"
        stop_memgraph
        exit 1
    fi

    echo "Registering instances..."
    if ! echo "
    REGISTER INSTANCE instance_1 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7687\", \"management_server\": \"127.0.0.1:13011\", \"replication_server\": \"127.0.0.1:10001\"};
    REGISTER INSTANCE instance_2 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7688\", \"management_server\": \"127.0.0.1:13012\", \"replication_server\": \"127.0.0.1:10002\"};
    REGISTER INSTANCE instance_3 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7689\", \"management_server\": \"127.0.0.1:13013\", \"replication_server\": \"127.0.0.1:10003\"};
    SET INSTANCE instance_1 TO MAIN;
    " | $MGCONSOLE_BINARY --host 127.0.0.1 --port 7691; then
        echo "ERROR: Failed to register instances"
        stop_memgraph
        exit 1
    fi

    # Wait for the cluster to become healthy
    echo "Waiting for HA cluster to become healthy..."
    if ! wait_for_healthy_cluster; then
        echo "ERROR: HA cluster did not become healthy in time"
        stop_memgraph
        exit 1
    fi

    echo "HA setup completed!"
}

wait_for_healthy_cluster() {
    local max_retries=30
    local retry_interval=2

    for ((i=1; i<=max_retries; i++)); do
        # Check if we have a healthy MAIN instance
        result=$(echo "SHOW INSTANCES;" | $MGCONSOLE_BINARY --host 127.0.0.1 --port 7691 --output-format=csv 2>/dev/null)
        if echo "$result" | grep -q "main" && echo "$result" | grep -q "up"; then
            echo "HA cluster is healthy."
            return 0
        fi
        echo "Waiting for cluster health check ($i/$max_retries)..."
        sleep $retry_interval
    done

    return 1
}

stop_memgraph() {
    if [[ -f "memgraph_ha.pid" ]]; then
        echo "Stopping Memgraph HA Deployment..."

        while read -r pid; do
            echo "Stopping Memgraph process (PID: $pid)..."
            kill $pid
        done < memgraph_ha.pid

        # Loop to check if all processes have fully stopped
        for i in {1..10}; do  # Check for up to 10 seconds
            all_stopped=true
            while read -r pid; do
                if kill -0 $pid 2>/dev/null; then
                    all_stopped=false
                    break
                fi
            done < memgraph_ha.pid

            if $all_stopped; then
                echo "All Memgraph processes have stopped."
                rm -f memgraph_ha.pid

                # Cleanup data directories after Memgraph stops
                clean_data_directories
                return
            fi

            echo "Waiting for all Memgraph processes to stop..."
            sleep 1
        done

        echo "Warning: Some Memgraph processes are still running after 10 seconds."
    else
        echo "No running Memgraph processes found."
    fi
}

wait_for_server() {
    local port=$1
    echo "Waiting for Memgraph on port $port..."
    while ! nc -z 127.0.0.1 $port; do
        sleep 0.5
    done
    echo "Memgraph is running on port $port."
}

case "$1" in
    start)
        shift
        start_memgraph "$@"
        ;;
    stop)
        stop_memgraph
        ;;
    status)
        if nc -z 127.0.0.1 7687; then
            echo "Memgraph is running."
        else
            echo "Memgraph is not running."
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|status} [memgraph flags...]"
        exit 1
esac
