#!/bin/bash

MEMGRAPH_BINARY="../../build/memgraph"
DATA_DIR="stress_data"

# Default flags for Memgraph
DEFAULT_FLAGS=(
    "--telemetry-enabled=false"
    "--bolt-server-name-for-init=Neo4j/"
    "--storage-properties-on-edges=true"
    "--storage-snapshot-on-exit=false"
    "--storage-snapshot-interval-sec=600"
    "--storage-snapshot-retention-count=1"
    "--storage-wal-enabled=true"
    "--query-execution-timeout-sec=1200"
    "--log-level=TRACE"
    "--also-log-to-stderr=true"
    "--data-directory=$DATA_DIR"
)

clean_data_directory() {
    if [[ -d "$DATA_DIR" ]]; then
        echo "Removing existing data directory: $DATA_DIR"
        rm -rf "$DATA_DIR"
    fi
}

merge_flags() {
    local provided_flags=("$@")  # Capture all provided flags
    local final_flags=("${DEFAULT_FLAGS[@]}")  # Start with default flags

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
    echo "Starting Memgraph Standalone..."

    # Ensure clean data directory before startup
    clean_data_directory

    # Merge default flags with user-provided flags (overrides defaults)
    FINAL_FLAGS=$(merge_flags "$@")

    CMD="$MEMGRAPH_BINARY $FINAL_FLAGS"
    echo "Executing: $CMD"

    $CMD &  # Run the command
    MG_PID=$!
    echo $MG_PID > memgraph.pid
    wait_for_server 7687
}

stop_memgraph() {
    if [[ -f "memgraph.pid" ]]; then
        MG_PID=$(cat memgraph.pid)
        echo "Stopping Memgraph Standalone (PID: $MG_PID)..."

        kill $MG_PID

        # Loop to check if Memgraph has fully stopped
        for i in {1..10}; do  # Wait up to 10 seconds
            if ! kill -0 $MG_PID 2>/dev/null; then
                echo "Memgraph has stopped."
                rm -f memgraph.pid

                # Cleanup data directory after Memgraph stops
                clean_data_directory
                return
            fi
            echo "Waiting for Memgraph to stop..."
            sleep 1
        done

        echo "Warning: Memgraph process $MG_PID is still running after 10 seconds."
    else
        echo "No running Memgraph process found."
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
