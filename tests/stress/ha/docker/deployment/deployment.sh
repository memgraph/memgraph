#!/bin/bash

# Docker image to use
MEMGRAPH_IMAGE="${MEMGRAPH_IMAGE:-memgraph/memgraph-mage:3.8.0}"

# Container name prefix
CONTAINER_PREFIX="memgraph_stress"

# Monitoring configuration
ENABLE_MONITORING="${ENABLE_MONITORING:-false}"
PROMETHEUS_EXPORTER_IMAGE="${PROMETHEUS_EXPORTER_IMAGE:-memgraph/prometheus-exporter:latest}"
PROMETHEUS_EXPORTER_CONTAINER="${CONTAINER_PREFIX}_prometheus_exporter"
PROMETHEUS_EXPORTER_PORT=9100
PROMETHEUS_SERVER_IMAGE="${PROMETHEUS_SERVER_IMAGE:-prom/prometheus:latest}"
PROMETHEUS_SERVER_CONTAINER="${CONTAINER_PREFIX}_prometheus"
PROMETHEUS_SERVER_PORT=9090
GRAFANA_IMAGE="${GRAFANA_IMAGE:-grafana/grafana:latest}"
GRAFANA_CONTAINER="${CONTAINER_PREFIX}_grafana"
GRAFANA_PORT=3000
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-admin}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROMETHEUS_CONFIG_FILE="${SCRIPT_DIR}/prometheus_ha_config.yaml"
PROMETHEUS_SERVER_CONFIG="${SCRIPT_DIR}/prometheus/prometheus.yaml"

# Containers for running mgconsole commands
COORDINATOR_CONTAINER="${CONTAINER_PREFIX}_coord_1"
DATA_CONTAINER="${CONTAINER_PREFIX}_data_1"

# Default flags for Memgraph Data Nodes
DEFAULT_DATA_FLAGS=(
    "--storage-properties-on-edges=true"
    "--storage-snapshot-on-exit=false"
    "--storage-snapshot-interval-sec=600"
    "--storage-snapshot-retention-count=2"
    "--storage-wal-enabled=true"
    "--query-execution-timeout-sec=0"
    "--log-level=TRACE"
    "--also-log-to-stderr=true"
)

# Default flags for Memgraph Coordinator Nodes
DEFAULT_COORD_FLAGS=(
    "--log-level=TRACE"
    "--also-log-to-stderr=true"
)

# Data node configurations: name, bolt_port, management_port, monitoring_port, metrics_port
DATA_NODES=(
    "data_1 7687 13011 7444 9091"
    "data_2 7688 13012 7445 9092"
)

# Coordinator configurations: name, coordinator_id, bolt_port, management_port, coordinator_port, monitoring_port, metrics_port
COORD_NODES=(
    "coord_1 1 7691 12121 10111 7447 9094"
    "coord_2 2 7692 12122 10112 7448 9095"
    "coord_3 3 7693 12123 10113 7449 9096"
)

start_data_container() {
    local name=$1
    local bolt_port=$2
    local management_port=$3
    local monitoring_port=$4
    local metrics_port=$5
    shift 5
    local extra_flags="$@"

    local container_name="${CONTAINER_PREFIX}_${name}"
    local flags="${DEFAULT_DATA_FLAGS[*]} ${extra_flags}"

    echo "Starting data container: $container_name"
    docker run -d --name "$container_name" \
        --network host \
        -e MEMGRAPH_ENTERPRISE_LICENSE="$MEMGRAPH_ENTERPRISE_LICENSE" \
        -e MEMGRAPH_ORGANIZATION_NAME="$MEMGRAPH_ORGANIZATION_NAME" \
        "$MEMGRAPH_IMAGE" \
        --bolt-port="$bolt_port" \
        --management-port="$management_port" \
        --monitoring-port="$monitoring_port" \
        --metrics-port="$metrics_port" \
        $flags

    if [[ $? -ne 0 ]]; then
        echo "ERROR: Failed to start container $container_name"
        return 1
    fi
}

start_coord_container() {
    local name=$1
    local coord_id=$2
    local bolt_port=$3
    local management_port=$4
    local coordinator_port=$5
    local monitoring_port=$6
    local metrics_port=$7
    shift 7
    local extra_flags="$@"

    local container_name="${CONTAINER_PREFIX}_${name}"
    local flags="${DEFAULT_COORD_FLAGS[*]} ${extra_flags}"

    echo "Starting coordinator container: $container_name"
    docker run -d --name "$container_name" \
        --network host \
        -e MEMGRAPH_ENTERPRISE_LICENSE="$MEMGRAPH_ENTERPRISE_LICENSE" \
        -e MEMGRAPH_ORGANIZATION_NAME="$MEMGRAPH_ORGANIZATION_NAME" \
        "$MEMGRAPH_IMAGE" \
        --coordinator-id="$coord_id" \
        --coordinator-hostname=127.0.0.1 \
        --bolt-port="$bolt_port" \
        --management-port="$management_port" \
        --coordinator-port="$coordinator_port" \
        --monitoring-port="$monitoring_port" \
        --metrics-port="$metrics_port" \
        $flags

    if [[ $? -ne 0 ]]; then
        echo "ERROR: Failed to start container $container_name"
        return 1
    fi
}

start_prometheus_exporter() {
    echo "Starting Prometheus exporter..."

    # Check if config file exists
    if [[ ! -f "$PROMETHEUS_CONFIG_FILE" ]]; then
        echo "WARNING: Prometheus config file not found at $PROMETHEUS_CONFIG_FILE"
        echo "Skipping Prometheus exporter..."
        return 1
    fi

    docker run -d --name "$PROMETHEUS_EXPORTER_CONTAINER" \
        --network host \
        -v "$PROMETHEUS_CONFIG_FILE:/etc/ha_config.yaml:ro" \
        -e DEPLOYMENT_TYPE=HA \
        -e CONFIG_FILE=/etc/ha_config.yaml \
        "$PROMETHEUS_EXPORTER_IMAGE"

    if [[ $? -ne 0 ]]; then
        echo "WARNING: Failed to start Prometheus exporter"
        return 1
    fi

    echo "Prometheus exporter started on port $PROMETHEUS_EXPORTER_PORT"
    return 0
}

stop_prometheus_exporter() {
    if docker ps -a --format '{{.Names}}' | grep -q "^${PROMETHEUS_EXPORTER_CONTAINER}$"; then
        echo "Stopping Prometheus exporter..."
        docker stop "$PROMETHEUS_EXPORTER_CONTAINER" 2>/dev/null
        docker rm "$PROMETHEUS_EXPORTER_CONTAINER" 2>/dev/null
    fi
}

start_prometheus_server() {
    echo "Starting Prometheus server..."

    if [[ ! -f "$PROMETHEUS_SERVER_CONFIG" ]]; then
        echo "WARNING: Prometheus server config not found at $PROMETHEUS_SERVER_CONFIG"
        echo "Skipping Prometheus server..."
        return 1
    fi

    docker run -d --name "$PROMETHEUS_SERVER_CONTAINER" \
        --network host \
        -v "$PROMETHEUS_SERVER_CONFIG:/etc/prometheus/prometheus.yml:ro" \
        "$PROMETHEUS_SERVER_IMAGE"

    if [[ $? -ne 0 ]]; then
        echo "WARNING: Failed to start Prometheus server"
        return 1
    fi

    echo "Prometheus server started on http://localhost:$PROMETHEUS_SERVER_PORT"
    return 0
}

stop_prometheus_server() {
    if docker ps -a --format '{{.Names}}' | grep -q "^${PROMETHEUS_SERVER_CONTAINER}$"; then
        echo "Stopping Prometheus server..."
        docker stop "$PROMETHEUS_SERVER_CONTAINER" 2>/dev/null
        docker rm "$PROMETHEUS_SERVER_CONTAINER" 2>/dev/null
    fi
}

start_grafana() {
    echo "Starting Grafana..."

    local grafana_provisioning="${SCRIPT_DIR}/grafana/provisioning"

    docker run -d --name "$GRAFANA_CONTAINER" \
        --network host \
        -e GF_SECURITY_ADMIN_PASSWORD="$GRAFANA_ADMIN_PASSWORD" \
        -e GF_AUTH_ANONYMOUS_ENABLED=true \
        -e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer \
        -v "$grafana_provisioning/datasources:/etc/grafana/provisioning/datasources:ro" \
        -v "$grafana_provisioning/dashboards:/etc/grafana/provisioning/dashboards:ro" \
        "$GRAFANA_IMAGE"

    if [[ $? -ne 0 ]]; then
        echo "WARNING: Failed to start Grafana"
        return 1
    fi

    echo "Grafana started on http://localhost:$GRAFANA_PORT (admin/$GRAFANA_ADMIN_PASSWORD)"
    echo "Pre-configured with Memgraph Prometheus data source and dashboards"
    return 0
}

stop_grafana() {
    if docker ps -a --format '{{.Names}}' | grep -q "^${GRAFANA_CONTAINER}$"; then
        echo "Stopping Grafana..."
        docker stop "$GRAFANA_CONTAINER" 2>/dev/null
        docker rm "$GRAFANA_CONTAINER" 2>/dev/null
    fi
}

start_monitoring_if_enabled() {
    if [[ "$ENABLE_MONITORING" != "true" ]]; then
        return 0
    fi

    echo ""
    echo "Starting monitoring stack..."
    start_prometheus_exporter
    start_prometheus_server
    start_grafana
    echo "Monitoring stack started!"
    echo "  - Prometheus exporter: http://localhost:$PROMETHEUS_EXPORTER_PORT"
    echo "  - Prometheus server:   http://localhost:$PROMETHEUS_SERVER_PORT"
    echo "  - Grafana dashboard:   http://localhost:$GRAFANA_PORT (admin/$GRAFANA_ADMIN_PASSWORD)"
}

stop_monitoring() {
    stop_grafana
    stop_prometheus_server
    stop_prometheus_exporter
}

start_memgraph() {
    echo "Starting Memgraph HA Deployment (Docker)..."
    echo "Using image: $MEMGRAPH_IMAGE"

    # Verify license is set
    if [[ -z "$MEMGRAPH_ENTERPRISE_LICENSE" ]]; then
        echo "WARNING: MEMGRAPH_ENTERPRISE_LICENSE is not set!"
    else
        echo "Enterprise license is configured (length: ${#MEMGRAPH_ENTERPRISE_LICENSE})"
    fi
    if [[ -z "$MEMGRAPH_ORGANIZATION_NAME" ]]; then
        echo "WARNING: MEMGRAPH_ORGANIZATION_NAME is not set!"
    else
        echo "Organization name: $MEMGRAPH_ORGANIZATION_NAME"
    fi

    if [[ "$ENABLE_MONITORING" == "true" ]]; then
        echo "Monitoring enabled - Prometheus exporter and Grafana will be started"
    fi

    # Stop any existing containers first
    stop_memgraph 2>/dev/null

    # Start data nodes
    for node in "${DATA_NODES[@]}"; do
        read -r name bolt_port management_port monitoring_port metrics_port <<< "$node"
        start_data_container "$name" "$bolt_port" "$management_port" "$monitoring_port" "$metrics_port" "$@"
        if [[ $? -ne 0 ]]; then
            stop_memgraph
            exit 1
        fi
    done

    # Start coordinator nodes
    for node in "${COORD_NODES[@]}"; do
        read -r name coord_id bolt_port management_port coordinator_port monitoring_port metrics_port <<< "$node"
        start_coord_container "$name" "$coord_id" "$bolt_port" "$management_port" "$coordinator_port" "$monitoring_port" "$metrics_port" "$@"
        if [[ $? -ne 0 ]]; then
            stop_memgraph
            exit 1
        fi
    done

    # Wait for all instances to be ready
    wait_for_server data_1
    wait_for_server data_2
    wait_for_server coord_1
    wait_for_server coord_2
    wait_for_server coord_3

    setup_ha
}

run_query() {
    # Run a query via mgconsole inside a specific container
    # Usage: run_query <container> <port> <query>
    # Note: Use echo | docker exec -i (heredoc <<< doesn't work with docker exec)
    local container="$1"
    local port="$2"
    local query="$3"
    echo "$query" | docker exec -i "$container" mgconsole --host 127.0.0.1 --port "$port"
}

run_query_coordinator() {
    # Run a query via mgconsole on the coordinator (port 7691)
    local query="$1"
    run_query "$COORDINATOR_CONTAINER" 7691 "$query"
}

run_query_data_instance() {
    # Run a query via mgconsole on the MAIN data instance (port 7687)
    local query="$1"
    run_query "$DATA_CONTAINER" 7687 "$query"
}

setup_ha() {
    echo "Setting up HA configuration using mgconsole (via docker exec)..."

    sleep 2  # Ensure coordinators are fully started

    echo "Adding coordinators..."
    if ! run_query_coordinator "
    ADD COORDINATOR 1 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7691\", \"coordinator_server\": \"127.0.0.1:10111\", \"management_server\": \"127.0.0.1:12121\"};
    ADD COORDINATOR 2 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7692\", \"coordinator_server\": \"127.0.0.1:10112\", \"management_server\": \"127.0.0.1:12122\"};
    ADD COORDINATOR 3 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7693\", \"coordinator_server\": \"127.0.0.1:10113\", \"management_server\": \"127.0.0.1:12123\"};
    "; then
        echo "ERROR: Failed to add coordinators"
        stop_memgraph
        exit 1
    fi

    echo "Registering instances..."
    if ! run_query_coordinator "
    REGISTER INSTANCE instance_1 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7687\", \"management_server\": \"127.0.0.1:13011\", \"replication_server\": \"127.0.0.1:10001\"};
    REGISTER INSTANCE instance_2 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7688\", \"management_server\": \"127.0.0.1:13012\", \"replication_server\": \"127.0.0.1:10002\"};
    SET INSTANCE instance_1 TO MAIN;
    "; then
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

    # Show Memgraph version (query the MAIN data instance on port 7687)
    echo ""
    echo "Memgraph version:"
    run_query_data_instance "SHOW VERSION;" || echo "Could not retrieve version"
    echo ""

    start_monitoring_if_enabled
}

wait_for_healthy_cluster() {
    local max_retries=30
    local retry_interval=2

    for ((i=1; i<=max_retries; i++)); do
        # Check if we have a healthy MAIN instance
        result=$(run_query_coordinator "SHOW INSTANCES;" 2>/dev/null)
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
    echo "Stopping Memgraph HA Deployment (Docker)..."

    # Stop monitoring stack if running
    stop_monitoring

    # Stop and remove data containers
    for node in "${DATA_NODES[@]}"; do
        read -r name _ <<< "$node"
        local container_name="${CONTAINER_PREFIX}_${name}"
        if docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
            echo "Stopping container: $container_name"
            docker stop "$container_name" 2>/dev/null
            docker rm "$container_name" 2>/dev/null
        fi
    done

    # Stop and remove coordinator containers
    for node in "${COORD_NODES[@]}"; do
        read -r name _ <<< "$node"
        local container_name="${CONTAINER_PREFIX}_${name}"
        if docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
            echo "Stopping container: $container_name"
            docker stop "$container_name" 2>/dev/null
            docker rm "$container_name" 2>/dev/null
        fi
    done

    echo "All Memgraph containers stopped and removed."
}

wait_for_server() {
    local instance_name=$1
    local port=""
    local is_coordinator=false

    for node in "${DATA_NODES[@]}"; do
        read -r name bolt_port _ <<< "$node"
        if [[ "$name" == "$instance_name" ]]; then
            port="$bolt_port"
            break
        fi
    done
    if [[ -z "$port" ]]; then
        for node in "${COORD_NODES[@]}"; do
            read -r name _ bolt_port _ <<< "$node"
            if [[ "$name" == "$instance_name" ]]; then
                port="$bolt_port"
                is_coordinator=true
                break
            fi
        done
    fi

    if [[ -z "$port" ]]; then
        echo "ERROR: Unknown instance '$instance_name'"
        return 1
    fi

    local max_retries=60

    echo "Waiting for $instance_name (port $port)..."
    for ((i=1; i<=max_retries; i++)); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
            break
        fi
        sleep 0.5
    done

    if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
        echo "ERROR: $instance_name (port $port) did not start in time"
        return 1
    fi

    local query
    if [[ "$is_coordinator" == "true" ]]; then
        query="SHOW INSTANCES;"
    else
        query="RETURN 1;"
    fi

    echo "Verifying $instance_name with mgconsole ($query)..."
    for ((i=1; i<=max_retries; i++)); do
        if echo "$query" | mgconsole --host 127.0.0.1 --port "$port" --use-ssl=false 2>/dev/null; then
            echo "$instance_name (port $port) is running and responding."
            return 0
        fi
        sleep 0.5
    done

    echo "ERROR: $instance_name (port $port) is not responding to queries"
    return 1
}

check_status() {
    echo "Checking Memgraph HA Deployment status..."

    local running=0
    local total=0

    for node in "${DATA_NODES[@]}"; do
        read -r name _ <<< "$node"
        local container_name="${CONTAINER_PREFIX}_${name}"
        ((total++))
        if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
            echo "  $container_name: running"
            ((running++))
        else
            echo "  $container_name: not running"
        fi
    done

    for node in "${COORD_NODES[@]}"; do
        read -r name _ <<< "$node"
        local container_name="${CONTAINER_PREFIX}_${name}"
        ((total++))
        if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
            echo "  $container_name: running"
            ((running++))
        else
            echo "  $container_name: not running"
        fi
    done

    # Check monitoring stack
    echo ""
    echo "Monitoring:"
    if docker ps --format '{{.Names}}' | grep -q "^${PROMETHEUS_EXPORTER_CONTAINER}$"; then
        echo "  Exporter:   running (http://localhost:$PROMETHEUS_EXPORTER_PORT)"
    else
        echo "  Exporter:   not running"
    fi
    if docker ps --format '{{.Names}}' | grep -q "^${PROMETHEUS_SERVER_CONTAINER}$"; then
        echo "  Prometheus: running (http://localhost:$PROMETHEUS_SERVER_PORT)"
    else
        echo "  Prometheus: not running"
    fi
    if docker ps --format '{{.Names}}' | grep -q "^${GRAFANA_CONTAINER}$"; then
        echo "  Grafana:    running (http://localhost:$GRAFANA_PORT)"
    else
        echo "  Grafana:    not running"
    fi

    echo ""
    echo "Running: $running/$total Memgraph containers"

    if [[ $running -eq $total ]]; then
        echo "Memgraph HA cluster is running."
        return 0
    else
        echo "Memgraph HA cluster is not fully running."
        return 1
    fi
}

restart_container() {
    local instance_name=$1
    local container_name="${CONTAINER_PREFIX}_${instance_name}"

    if ! docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo "ERROR: Container $container_name does not exist"
        return 1
    fi

    echo "Restarting container: $container_name"
    docker restart "$container_name"

    if [[ $? -ne 0 ]]; then
        echo "ERROR: Failed to restart $container_name"
        return 1
    fi

    echo "Container $container_name restarted successfully"
    return 0
}

restart_all() {
    echo "Restarting all Memgraph HA containers..."
    stop_memgraph
    sleep 2
    start_memgraph
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
        check_status
        ;;
    restart)
        if [[ -n "$2" ]]; then
            restart_container "$2"
        else
            restart_all
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart [instance_name]} [memgraph flags...]"
        echo ""
        echo "Commands:"
        echo "  start              - Start the HA cluster"
        echo "  stop               - Stop the HA cluster"
        echo "  status             - Check cluster status"
        echo "  restart            - Restart all containers"
        echo "  restart <instance> - Restart specific instance (e.g., data_1, coord_2)"
        echo ""
        echo "Instance names: data_1, data_2, coord_1, coord_2, coord_3"
        echo ""
        echo "Monitoring (set ENABLE_MONITORING=true to enable):"
        echo "  Prometheus exporter: http://localhost:$PROMETHEUS_EXPORTER_PORT"
        echo "  Grafana dashboard:   http://localhost:$GRAFANA_PORT (admin/$GRAFANA_ADMIN_PASSWORD)"
        echo ""
        echo "Environment variables:"
        echo "  MEMGRAPH_IMAGE              - Docker image to use (default: memgraph/memgraph:latest)"
        echo "  ENABLE_MONITORING           - Enable monitoring stack (default: false)"
        echo "  PROMETHEUS_EXPORTER_IMAGE   - Exporter image (default: memgraph/prometheus-exporter:latest)"
        echo "  GRAFANA_IMAGE               - Grafana image (default: grafana/grafana:latest)"
        echo "  GRAFANA_ADMIN_PASSWORD      - Grafana admin password (default: admin)"
        echo "  MEMGRAPH_ENTERPRISE_LICENSE - Enterprise license key (required for HA)"
        echo "  MEMGRAPH_ORGANIZATION_NAME  - Organization name (required for HA)"
        exit 1
esac
