#!/bin/bash

# Get absolute path to script directory and build directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$(cd "$SCRIPT_DIR/../../../../../build" 2>/dev/null && pwd)"

if [[ -z "$BUILD_DIR" || ! -d "$BUILD_DIR" ]]; then
    echo "ERROR: Build directory not found. Expected at: $SCRIPT_DIR/../../../../../build"
    echo "Please build Memgraph first."
    exit 1
fi

MEMGRAPH_BINARY="$BUILD_DIR/memgraph"

if [[ ! -x "$MEMGRAPH_BINARY" ]]; then
    echo "ERROR: Memgraph binary not found at: $MEMGRAPH_BINARY"
    exit 1
fi
DATA_DIR="stress_data"
LOG_DIR="${LOG_DIR:-stress_logs}"

# Default flags for Memgraph
DEFAULT_FLAGS=(
    "--also-log-to-stderr=true"
    "--bolt-server-name-for-init=Neo4j/"
    "--data-directory=$DATA_DIR"
    "--log-file=$LOG_DIR/memgraph.log"
    "--log-level=TRACE"
    "--query-execution-timeout-sec=1200"
    "--storage-snapshot-interval-sec=300"
    "--storage-properties-on-edges=true"
    "--storage-wal-enabled=true"
    "--storage-snapshot-on-exit=false"
    "--telemetry-enabled=false"
)

is_memgraph_running() {
    # Check if port is in use
    if nc -z 127.0.0.1 7687 2>/dev/null; then
        return 0
    fi
    # Check if data directory lock is held
    local lock_file="$DATA_DIR/.lock"
    if [[ -f "$lock_file" ]] && ! flock -n "$lock_file" true 2>/dev/null; then
        return 0
    fi
    # Check if PID is alive and not a zombie
    if [[ -f "memgraph.pid" ]]; then
        local pid
        pid=$(cat memgraph.pid)
        if kill -0 "$pid" 2>/dev/null; then
            local state
            state=$(awk '/^State:/{print $2}' /proc/"$pid"/status 2>/dev/null)
            if [[ "$state" != "Z" ]]; then
                return 0
            fi
        fi
    fi
    return 1
}

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

    # Ensure no lingering Memgraph process before starting
    if is_memgraph_running; then
        echo "ERROR: A Memgraph process is still running (port 7687 in use or data directory lock held)."
        exit 1
    fi

    # Merge default flags with user-provided flags (overrides defaults)
    FINAL_FLAGS=$(merge_flags "$@")

    # Ensure clean data directory before startup
    clean_data_directory

    CMD="$MEMGRAPH_BINARY $FINAL_FLAGS"
    echo "Executing: $CMD"

    mkdir -p "$LOG_DIR"
    $CMD &
    MG_PID=$!
    echo $MG_PID > memgraph.pid
    wait_for_server 7687
}

stop_memgraph() {
    local graceful_timeout_sec=60

    if [[ ! -f "memgraph.pid" ]]; then
        echo "No running Memgraph process found."
        return 0
    fi

    MG_PID=$(cat memgraph.pid)
    echo "Stopping Memgraph Standalone (PID: $MG_PID)..."
    kill "$MG_PID" 2>/dev/null || true

    echo "Waiting for Memgraph to stop..."
    for ((i=1; i<=graceful_timeout_sec; i++)); do
        if ! is_memgraph_running; then
            echo "Memgraph has stopped."
            rm -f memgraph.pid
            clean_data_directory
            return 0
        fi
        sleep 1
    done

    echo "Force killing..."
    kill -9 "$MG_PID" 2>/dev/null || true

    # Check if still running after SIGKILL
    sleep 2
    if is_memgraph_running; then
        echo "ERROR: Memgraph is still running after SIGKILL (port 7687 in use or data directory lock held). Aborting."
        exit 1
    fi

    echo "Memgraph has been force-killed."
    rm -f memgraph.pid
    clean_data_directory
}

wait_for_server() {
    local port=$1
    echo "Waiting for Memgraph on port $port..."
    while ! nc -z 127.0.0.1 $port; do
        if ! kill -0 "$MG_PID" 2>/dev/null; then
            echo "ERROR: Memgraph process $MG_PID exited before becoming ready."
            exit 1
        fi
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
        if is_memgraph_running; then
            echo "Memgraph is running."
        else
            echo "Memgraph is not running."
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|status} [memgraph flags...]"
        exit 1
esac
