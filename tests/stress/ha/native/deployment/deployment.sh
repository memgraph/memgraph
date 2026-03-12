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

# Try to find mgconsole: first in build dir, then in toolchain, then in PATH
if [[ -x "$BUILD_DIR/mgconsole" ]]; then
    MGCONSOLE_BINARY="$BUILD_DIR/bin/mgconsole"
elif [[ -n "$MG_TOOLCHAIN_ROOT" && -x "$MG_TOOLCHAIN_ROOT/bin/mgconsole" ]]; then
    MGCONSOLE_BINARY="$MG_TOOLCHAIN_ROOT/bin/mgconsole"
elif [[ -x "/opt/toolchain-v7/bin/mgconsole" ]]; then
    MGCONSOLE_BINARY="/opt/toolchain-v7/bin/mgconsole"
elif command -v mgconsole &>/dev/null; then
    MGCONSOLE_BINARY="$(command -v mgconsole)"
else
    echo "ERROR: mgconsole binary not found in build dir, toolchain, or PATH"
    exit 1
fi

DATA_DIR_PREFIX="mg_data"
COORD_DIR_PREFIX="mg_coord"
LOG_DIR="${LOG_DIR:-stress_logs}"

# Core dump configuration
CORES_DIR="${CORES_DIR:-/tmp/mg-cores}"

# Default flags for Memgraph Data Nodes
DEFAULT_DATA_FLAGS=(
    "--also-log-to-stderr=true"
    "--bolt-server-name-for-init=Neo4j/"
    "--log-file="
    "--log-level=TRACE"
    "--storage-properties-on-edges=true"
    "--storage-snapshot-interval-sec=300"
    "--storage-wal-enabled=true"
    "--storage-snapshot-on-exit=false"
    "--telemetry-enabled=false"
    "--query-execution-timeout-sec=1200"
)

# Default flags for Memgraph Coordinator Nodes
DEFAULT_COORD_FLAGS=(
    "--also-log-to-stderr=true"
    "--bolt-server-name-for-init=Neo4j/"
    "--log-file="
    "--log-level=WARNING"
    "--telemetry-enabled=false"
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
    # Usage: merge_flags <num_defaults> <default_flags...> <override_flags...>
    # First arg is the count of default flags so we can split the two arrays.
    local num_defaults=$1; shift

    local default_flags=("${@:1:$num_defaults}")
    local provided_flags=("${@:$((num_defaults + 1))}")

    local final_flags=("${default_flags[@]}")

    for flag in "${provided_flags[@]}"; do
        local flag_name="${flag%%=*}"

        local found=false
        for i in "${!final_flags[@]}"; do
            if [[ "${final_flags[$i]}" == "$flag_name="* ]]; then
                final_flags[$i]="$flag"
                found=true
                break
            fi
        done

        if [[ "$found" == false ]]; then
            final_flags+=("$flag")
        fi
    done

    echo "${final_flags[@]}"
}

_start_processes() {
    # Launches all data and coordinator processes, waits for ports.
    export MEMGRAPH_ENTERPRISE_LICENSE
    export MEMGRAPH_ORGANIZATION_NAME
    rm -f memgraph_ha.pid
    mkdir -p "$LOG_DIR"
    mkdir -p "$CORES_DIR"
    ulimit -c unlimited

    local i=1
    for node in "${DATA_NODES[@]}"; do
        FINAL_FLAGS=$(merge_flags ${#DEFAULT_DATA_FLAGS[@]} "${DEFAULT_DATA_FLAGS[@]}" "$@")
        CMD="$MEMGRAPH_BINARY $node $FINAL_FLAGS"
        echo "Executing: $CMD"
        $CMD > "$LOG_DIR/data_${i}.log" 2>&1 &
        echo $! >> memgraph_ha.pid
        ((i++))
    done

    local j=1
    for node in "${COORD_NODES[@]}"; do
        FINAL_FLAGS=$(merge_flags ${#DEFAULT_COORD_FLAGS[@]} "${DEFAULT_COORD_FLAGS[@]}" "$@")
        CMD="$MEMGRAPH_BINARY $node $FINAL_FLAGS"
        echo "Executing: $CMD"
        $CMD > "$LOG_DIR/coord_${j}.log" 2>&1 &
        echo $! >> memgraph_ha.pid
        ((j++))
    done

    wait_for_server 7687
    wait_for_server 7688
    wait_for_server 7689
    wait_for_server 7691
    wait_for_server 7692
    wait_for_server 7693
}

start_memgraph() {
    echo "Starting Memgraph HA Deployment..."
    clean_data_directories
    _start_processes "$@"
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

_stop_processes() {
    # Stops all Memgraph processes without cleaning data directories.
    local graceful_timeout_sec=60

    if [[ ! -f "memgraph_ha.pid" ]]; then
        echo "No running Memgraph processes found."
        return 0
    fi

    echo "Stopping Memgraph HA processes..."

    while read -r pid; do
        echo "Stopping Memgraph process (PID: $pid)..."
        kill "$pid" 2>/dev/null || true
    done < memgraph_ha.pid

    echo "Waiting for all Memgraph processes to stop..."
    for ((i=1; i<=graceful_timeout_sec; i++)); do
        all_stopped=true
        while read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                all_stopped=false
                break
            fi
        done < memgraph_ha.pid

        if $all_stopped; then
            echo "All Memgraph processes have stopped."
            rm -f memgraph_ha.pid
            return 0
        fi

        sleep 1
    done

    echo "Warning: Some Memgraph processes are still running after ${graceful_timeout_sec} seconds. Force killing..."
    local remaining_pids=()
    while read -r pid; do
        if kill -0 "$pid" 2>/dev/null; then
            remaining_pids+=("$pid")
        fi
    done < memgraph_ha.pid

    if [[ ${#remaining_pids[@]} -eq 0 ]]; then
        rm -f memgraph_ha.pid
        return 0
    fi

    for pid in "${remaining_pids[@]}"; do
        echo "Force killing Memgraph process (PID: $pid)..."
        kill -9 "$pid" 2>/dev/null || true
    done

    echo "Warning: Some Memgraph processes may still be in D-state (kernel I/O), continuing anyway."
    rm -f memgraph_ha.pid
    return 0
}

collect_logs() {
    local dest_dir="${1:-$LOG_DIR}"
    mkdir -p "$dest_dir"
    echo "Collecting logs to $dest_dir/..."

    if [[ -d "$LOG_DIR" && "$LOG_DIR" != "$dest_dir" ]]; then
        cp -v "$LOG_DIR"/*.log "$dest_dir/" 2>/dev/null || echo "No log files found in $LOG_DIR"
    elif [[ ! -d "$LOG_DIR" ]]; then
        echo "Log directory $LOG_DIR not found."
    else
        echo "Logs are already in $dest_dir/"
    fi

    echo "Logs collected."
}

collect_cores() {
    local dest_dir="${1:-stress_cores}"
    mkdir -p "$dest_dir"

    if [[ ! -d "$CORES_DIR" ]]; then
        echo "No core dump directory found ($CORES_DIR), skipping."
        return 0
    fi

    local found=false
    for core in "$CORES_DIR"/core.*; do
        [[ -f "$core" ]] || continue
        found=true
        local basename
        basename=$(basename "$core")

        echo "  Generating backtrace: $basename -> $dest_dir/${basename}.bt.txt.gz"
        gdb -batch -ex 'thread apply all bt' "$MEMGRAPH_BINARY" "$core" \
            | gzip > "$dest_dir/${basename}.bt.txt.gz"

        echo "  Compressing core dump: $basename -> $dest_dir/${basename}.gz"
        gzip -c "$core" > "$dest_dir/${basename}.gz"

        echo "  Removing raw core dump: $core"
        rm -f "$core"
    done

    if [[ "$found" == "false" ]]; then
        echo "No core dumps found in $CORES_DIR"
    else
        echo "Core dumps collected."
    fi
}

stop_memgraph() {
    collect_logs
    collect_cores
    _stop_processes
    clean_data_directories
}

restart_all() {
    echo "Restarting all Memgraph HA instances (preserving data)..."
    _stop_processes
    sleep 2
    _start_processes "$@"
    echo "All Memgraph HA instances restarted (data preserved)"
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
    collect-logs)
        collect_logs "$2"
        collect_cores "$2"
        ;;
    restart)
        shift
        restart_all "$@"
        ;;
    status)
        if nc -z 127.0.0.1 7687; then
            echo "Memgraph is running."
        else
            echo "Memgraph is not running."
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|collect-logs [dir]|restart|status} [memgraph flags...]"
        exit 1
esac
