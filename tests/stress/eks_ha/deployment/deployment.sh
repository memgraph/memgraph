#!/bin/bash

# EKS HA Deployment Script for Memgraph
# This script manages a Memgraph HA deployment on AWS EKS

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Cluster configuration
CLUSTER_NAME="${CLUSTER_NAME:-test-cluster-ha}"
CLUSTER_REGION="${CLUSTER_REGION:-eu-west-1}"
CLUSTER_CONFIG_FILE="${SCRIPT_DIR}/cluster.yaml"

# Helm configuration
HELM_RELEASE_NAME="${HELM_RELEASE_NAME:-mem-ha-test}"
HELM_CHART_PATH="${HELM_CHART_PATH:-memgraph/memgraph-high-availability}"
HELM_REPO_NAME="memgraph"
HELM_REPO_URL="https://memgraph.github.io/helm-charts"

# Storage configuration
STORAGE_CLASS_NAME="gp3"
STORAGE_CLASS_FILE="${SCRIPT_DIR}/gp3-sc.yaml"

# Helm values file
HELM_VALUES_FILE="${SCRIPT_DIR}/values.yaml"

# Monitoring configuration
ENABLE_MONITORING="${ENABLE_MONITORING:-true}"
PROMETHEUS_NAMESPACE="monitoring"

# Timeouts
CLUSTER_CREATE_TIMEOUT="${CLUSTER_CREATE_TIMEOUT:-30m}"
POD_READY_TIMEOUT="${POD_READY_TIMEOUT:-600}"  # 10 minutes

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing_tools=()

    if ! command -v eksctl &> /dev/null; then
        missing_tools+=("eksctl")
    fi

    if ! command -v kubectl &> /dev/null; then
        missing_tools+=("kubectl")
    fi

    if ! command -v helm &> /dev/null; then
        missing_tools+=("helm")
    fi

    if ! command -v aws &> /dev/null; then
        missing_tools+=("aws-cli")
    fi

    if [[ ${#missing_tools[@]} -gt 0 ]]; then
        log_error "Missing required tools: ${missing_tools[*]}"
        echo "Please install the missing tools and try again."
        exit 1
    fi

    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured or invalid"
        echo "Please configure AWS credentials using 'aws configure' or environment variables"
        exit 1
    fi

    log_info "All prerequisites met"
}

check_config_files() {
    log_info "Checking configuration files..."

    local missing_files=()

    if [[ ! -f "$CLUSTER_CONFIG_FILE" ]]; then
        missing_files+=("$CLUSTER_CONFIG_FILE")
    fi

    if [[ ! -f "$STORAGE_CLASS_FILE" ]]; then
        missing_files+=("$STORAGE_CLASS_FILE")
    fi

    if [[ ! -f "$HELM_VALUES_FILE" ]]; then
        missing_files+=("$HELM_VALUES_FILE")
    fi

    if [[ ${#missing_files[@]} -gt 0 ]]; then
        log_error "Missing configuration files:"
        for f in "${missing_files[@]}"; do
            echo "  - $f"
        done
        exit 1
    fi

    log_info "All configuration files found"
}

create_cluster() {
    log_info "Creating EKS cluster: $CLUSTER_NAME in $CLUSTER_REGION..."

    # Check if cluster already exists
    if eksctl get cluster --name "$CLUSTER_NAME" --region "$CLUSTER_REGION" &> /dev/null; then
        log_warn "Cluster $CLUSTER_NAME already exists"
        log_info "Updating kubeconfig..."
        aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$CLUSTER_REGION"
        return 0
    fi

    # Create the cluster
    eksctl create cluster -f "$CLUSTER_CONFIG_FILE"

    if [[ $? -ne 0 ]]; then
        log_error "Failed to create EKS cluster"
        exit 1
    fi

    # Ensure kubectl context points to the new cluster
    log_info "Updating kubeconfig..."
    aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$CLUSTER_REGION"

    log_info "EKS cluster created successfully"
}

setup_helm_repo() {
    log_info "Setting up Helm repository..."

    # Add Memgraph Helm repo if not already added
    if ! helm repo list | grep -q "$HELM_REPO_NAME"; then
        helm repo add "$HELM_REPO_NAME" "$HELM_REPO_URL"
    fi

    # Add Prometheus community repo if monitoring is enabled
    if [[ "$ENABLE_MONITORING" == "true" ]]; then
        if ! helm repo list | grep -q "prometheus-community"; then
            helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
        fi
    fi

    helm repo update

    log_info "Helm repository configured"
}

install_kube_prometheus_stack() {
    if [[ "$ENABLE_MONITORING" != "true" ]]; then
        log_info "Monitoring disabled, skipping kube-prometheus-stack installation"
        return 0
    fi

    log_info "Installing kube-prometheus-stack for monitoring..."

    # Create monitoring namespace if it doesn't exist
    if ! kubectl get namespace "$PROMETHEUS_NAMESPACE" &> /dev/null; then
        kubectl create namespace "$PROMETHEUS_NAMESPACE"
    fi

    # Check if already installed
    if helm list -n "$PROMETHEUS_NAMESPACE" -q | grep -q "^kube-prometheus-stack$"; then
        log_info "kube-prometheus-stack already installed"
        return 0
    fi

    # Install kube-prometheus-stack with minimal config
    helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
        --namespace "$PROMETHEUS_NAMESPACE" \
        --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
        --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
        --set grafana.adminPassword=admin \
        --wait --timeout 10m

    if [[ $? -ne 0 ]]; then
        log_error "Failed to install kube-prometheus-stack"
        return 1
    fi

    # Wait for Prometheus operator to be ready
    log_info "Waiting for Prometheus operator to be ready..."
    kubectl wait --for=condition=available deployment/kube-prometheus-stack-operator \
        -n "$PROMETHEUS_NAMESPACE" --timeout=300s 2>/dev/null || true

    log_info "kube-prometheus-stack installed successfully"
    log_info "  Grafana: kubectl port-forward -n $PROMETHEUS_NAMESPACE svc/kube-prometheus-stack-grafana 3000:80"
    log_info "  Prometheus: kubectl port-forward -n $PROMETHEUS_NAMESPACE svc/kube-prometheus-stack-prometheus 9090:9090"
}

install_ebs_csi_driver() {
    log_info "Installing AWS EBS CSI driver..."

    # Check if already installed
    if kubectl get pods -n kube-system 2>/dev/null | grep -q "ebs-csi"; then
        log_info "EBS CSI driver already installed"
        return 0
    fi

    # Install EBS CSI driver
    kubectl apply -k "github.com/kubernetes-sigs/aws-ebs-csi-driver/deploy/kubernetes/overlays/stable/ecr/?ref=release-1.25"

    if [[ $? -ne 0 ]]; then
        log_error "Failed to install EBS CSI driver"
        exit 1
    fi

    # Wait for the driver to be ready
    log_info "Waiting for EBS CSI driver pods to be ready..."
    local max_retries=60
    for ((i=1; i<=max_retries; i++)); do
        local ready_pods
        ready_pods=$(kubectl get pods -n kube-system -l app.kubernetes.io/name=aws-ebs-csi-driver --no-headers 2>/dev/null | grep -c "Running") || ready_pods=0
        if [[ "$ready_pods" -ge 2 ]]; then
            log_info "EBS CSI driver is ready"
            return 0
        fi
        echo -ne "\rWaiting for EBS CSI driver ($i/$max_retries)..."
        sleep 5
    done

    log_warn "EBS CSI driver may not be fully ready, continuing..."
}

apply_storage_class() {
    log_info "Applying GP3 storage class..."

    kubectl apply -f "$STORAGE_CLASS_FILE"

    if [[ $? -ne 0 ]]; then
        log_error "Failed to apply storage class"
        exit 1
    fi

    log_info "Storage class applied successfully"
}

attach_ebs_policy() {
    log_info "Attaching EBS policy to node group roles..."

    local node_groups=("standard-workers" "heavy-workers")

    for ng in "${node_groups[@]}"; do
        log_info "Processing node group: $ng"

        # Get the node group role
        local role_arn=$(aws eks describe-nodegroup \
            --cluster-name "$CLUSTER_NAME" \
            --nodegroup-name "$ng" \
            --region "$CLUSTER_REGION" \
            --query 'nodegroup.nodeRole' \
            --output text 2>/dev/null)

        if [[ -z "$role_arn" || "$role_arn" == "None" ]]; then
            log_warn "Could not get role for node group $ng, skipping..."
            continue
        fi

        # Extract role name from ARN
        local role_name=$(echo "$role_arn" | awk -F'/' '{print $NF}')

        log_info "Attaching AmazonEC2FullAccess policy to role: $role_name"

        # Attach the policy
        aws iam attach-role-policy \
            --role-name "$role_name" \
            --policy-arn arn:aws:iam::aws:policy/AmazonEC2FullAccess 2>/dev/null || true

        # Verify
        if aws iam list-attached-role-policies --role-name "$role_name" | grep -q "AmazonEC2FullAccess"; then
            log_info "Policy attached successfully to $role_name"
        else
            log_warn "Could not verify policy attachment to $role_name"
        fi
    done
}

install_memgraph_ha() {
    log_info "Installing Memgraph HA using Helm..."

    # Check if release already exists
    if helm list -q | grep -q "^${HELM_RELEASE_NAME}$"; then
        log_warn "Helm release $HELM_RELEASE_NAME already exists"
        log_info "Use 'upgrade' command to update or 'stop' to remove first"
        return 0
    fi

    # Build helm install command with values file
    local helm_cmd="helm install $HELM_RELEASE_NAME $HELM_CHART_PATH -f $HELM_VALUES_FILE"

    # Override image tag if MEMGRAPH_IMAGE_TAG is set
    if [[ -n "${MEMGRAPH_IMAGE_TAG:-}" ]]; then
        helm_cmd+=" --set image.tag=$MEMGRAPH_IMAGE_TAG"
        log_info "Using image tag: $MEMGRAPH_IMAGE_TAG"
    fi

    # Override license if MEMGRAPH_ENTERPRISE_LICENSE is set
    if [[ -n "${MEMGRAPH_ENTERPRISE_LICENSE:-}" ]]; then
        helm_cmd+=" --set env.MEMGRAPH_ENTERPRISE_LICENSE=$MEMGRAPH_ENTERPRISE_LICENSE"
        log_info "Enterprise license configured"
    fi

    # Override organization name if MEMGRAPH_ORGANIZATION_NAME is set
    if [[ -n "${MEMGRAPH_ORGANIZATION_NAME:-}" ]]; then
        helm_cmd+=" --set env.MEMGRAPH_ORGANIZATION_NAME=$MEMGRAPH_ORGANIZATION_NAME"
        log_info "Organization name configured"
    fi

    log_info "Running: helm install $HELM_RELEASE_NAME $HELM_CHART_PATH -f $HELM_VALUES_FILE [+ overrides]"
    eval "$helm_cmd"

    if [[ $? -ne 0 ]]; then
        log_error "Failed to install Memgraph HA"
        exit 1
    fi

    log_info "Memgraph HA Helm release installed"
}

wait_for_pods() {
    log_info "Waiting for all Memgraph pods to be ready (timeout: ${POD_READY_TIMEOUT}s)..."

    local start_time=$(date +%s)
    local timeout=$POD_READY_TIMEOUT

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [[ $elapsed -ge $timeout ]]; then
            log_error "Timeout waiting for pods to be ready"
            log_info "Current pod status:"
            kubectl get pods -o wide | grep -E "^memgraph-"
            exit 1
        fi

        # Get pod status (match pods starting with memgraph-)
        local total_pods
        total_pods=$(kubectl get pods --no-headers 2>/dev/null | grep -c "^memgraph-") || total_pods=0
        local ready_pods
        ready_pods=$(kubectl get pods --no-headers 2>/dev/null | grep "^memgraph-" | grep -c "Running") || ready_pods=0

        if [[ $total_pods -gt 0 && $total_pods -eq $ready_pods ]]; then
            log_info "All $total_pods pods are running!"
            break
        fi

        echo -ne "\r[${elapsed}s] Waiting for pods: $ready_pods/$total_pods ready..."
        sleep 5
    done

    echo ""
    log_info "Pod status:"
    kubectl get pods -o wide | grep -E "^memgraph-|^NAME"
}

wait_for_external_ips() {
    local max_wait=300  # 5 minutes max
    local start_time=$(date +%s)

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [[ $elapsed -ge $max_wait ]]; then
            log_warn "Timeout waiting for external IPs (some may still be pending)"
            break
        fi

        # Count LoadBalancer services and those with assigned external IPs
        local lb_services
        lb_services=$(kubectl get svc --no-headers 2>/dev/null | grep "^memgraph-" | grep -c "LoadBalancer") || lb_services=0
        local assigned_ips
        assigned_ips=$(kubectl get svc --no-headers 2>/dev/null | grep "^memgraph-" | grep "LoadBalancer" | grep -v "<pending>" | grep -cv "<none>") || assigned_ips=0

        if [[ $lb_services -gt 0 && $lb_services -eq $assigned_ips ]]; then
            log_info "All external IPs assigned!"
            break
        fi

        echo -ne "\r[${elapsed}s] Waiting for external IPs: $assigned_ips/$lb_services assigned..."
        sleep 10
    done
    echo ""
}

setup_ha() {
    log_info "Setting up HA cluster configuration..."

    # Wait a bit for coordinators to be fully ready
    sleep 5

    # Internal K8s DNS names for cluster communication
    local coord1_internal="memgraph-coordinator-1.default.svc.cluster.local"
    local coord2_internal="memgraph-coordinator-2.default.svc.cluster.local"
    local coord3_internal="memgraph-coordinator-3.default.svc.cluster.local"
    local data0_internal="memgraph-data-0.default.svc.cluster.local"
    local data1_internal="memgraph-data-1.default.svc.cluster.local"

    log_info "Connecting to coordinator 1 via kubectl exec..."

    log_info "Adding coordinators..."

    local add_coord_query="
ADD COORDINATOR 1 WITH CONFIG {\"bolt_server\": \"${coord1_internal}:7687\", \"coordinator_server\": \"${coord1_internal}:12000\", \"management_server\": \"${coord1_internal}:10000\"};
ADD COORDINATOR 2 WITH CONFIG {\"bolt_server\": \"${coord2_internal}:7687\", \"coordinator_server\": \"${coord2_internal}:12000\", \"management_server\": \"${coord2_internal}:10000\"};
ADD COORDINATOR 3 WITH CONFIG {\"bolt_server\": \"${coord3_internal}:7687\", \"coordinator_server\": \"${coord3_internal}:12000\", \"management_server\": \"${coord3_internal}:10000\"};
"

    if ! kubectl exec -i memgraph-coordinator-1-0 -- mgconsole --host 127.0.0.1 --port 7687 <<< "$add_coord_query"; then
        log_error "Failed to add coordinators"
        return 1
    fi

    log_info "Registering data instances..."

    local register_query="
REGISTER INSTANCE instance_0 WITH CONFIG {\"bolt_server\": \"${data0_internal}:7687\", \"management_server\": \"${data0_internal}:10000\", \"replication_server\": \"${data0_internal}:20000\"};
REGISTER INSTANCE instance_1 WITH CONFIG {\"bolt_server\": \"${data1_internal}:7687\", \"management_server\": \"${data1_internal}:10000\", \"replication_server\": \"${data1_internal}:20000\"};
SET INSTANCE instance_0 TO MAIN;
"

    if ! kubectl exec -i memgraph-coordinator-1-0 -- mgconsole --host 127.0.0.1 --port 7687 <<< "$register_query"; then
        log_error "Failed to register instances"
        return 1
    fi

    # Wait for cluster to become healthy
    log_info "Waiting for HA cluster to become healthy..."
    if ! wait_for_healthy_cluster; then
        log_error "HA cluster did not become healthy in time"
        return 1
    fi

    log_info "HA cluster setup completed!"
}

wait_for_healthy_cluster() {
    local max_retries=30
    local retry_interval=2

    for ((i=1; i<=max_retries; i++)); do
        local result
        result=$(kubectl exec -i memgraph-coordinator-1-0 -- mgconsole --host 127.0.0.1 --port 7687 --output-format=csv <<< "SHOW INSTANCES;" 2>/dev/null)

        if echo "$result" | grep -qi "main" && echo "$result" | grep -qi "up"; then
            log_info "HA cluster is healthy!"
            return 0
        fi
        echo -ne "\rWaiting for cluster health ($i/$max_retries)..."
        sleep $retry_interval
    done
    echo ""

    return 1
}

start_memgraph() {
    log_info "Starting Memgraph HA Deployment on EKS..."

    check_prerequisites
    check_config_files
    create_cluster
    setup_helm_repo
    install_ebs_csi_driver
    apply_storage_class
    attach_ebs_policy
    install_kube_prometheus_stack
    install_memgraph_ha
    wait_for_pods

    # Check if there are any external services (LoadBalancer type)
    local external_svc_count
    external_svc_count=$(kubectl get svc --no-headers 2>/dev/null | grep "^memgraph-" | grep -c "LoadBalancer") || external_svc_count=0

    if [[ "$external_svc_count" -gt 0 ]]; then
        log_info "Waiting for external IPs to be assigned..."
        wait_for_external_ips
    fi

    setup_ha

    echo ""
    log_info "=========================================="
    log_info "Memgraph HA Deployment Complete!"
    log_info "=========================================="
    log_info "Cluster: $CLUSTER_NAME"
    log_info "Region:  $CLUSTER_REGION"
    log_info "Release: $HELM_RELEASE_NAME"
    echo ""

    # Show services
    log_info "Services:"
    echo ""
    printf "  %-35s %-15s %-20s\n" "SERVICE" "PORT" "CLUSTER-IP"
    printf "  %-35s %-15s %-20s\n" "-------" "----" "----------"
    while IFS= read -r line; do
        local svc_name=$(echo "$line" | awk '{print $1}')
        local cluster_ip=$(echo "$line" | awk '{print $3}')
        local ports=$(echo "$line" | awk '{print $5}' | cut -d'/' -f1)

        printf "  %-35s %-15s %-20s\n" "$svc_name" "$ports" "$cluster_ip"
    done < <(kubectl get svc --no-headers 2>/dev/null | grep "^memgraph-")

    # Show LoadBalancer external endpoints with DNS and resolved IP
    local lb_count
    lb_count=$(kubectl get svc --no-headers 2>/dev/null | grep "^memgraph-" | grep -c "LoadBalancer") || lb_count=0
    if [[ "$lb_count" -gt 0 ]]; then
        echo ""
        log_info "External Endpoints (LoadBalancer):"
        echo ""
        while IFS= read -r line; do
            local svc_name=$(echo "$line" | awk '{print $1}')
            local external_dns=$(echo "$line" | awk '{print $4}')
            local ports=$(echo "$line" | awk '{print $5}' | cut -d'/' -f1 | cut -d':' -f1)

            if [[ "$external_dns" != "<none>" && "$external_dns" != "<pending>" && -n "$external_dns" ]]; then
                # Resolve DNS to IP
                local external_ip
                external_ip=$(dig +short "$external_dns" 2>/dev/null | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | head -1) || external_ip=""

                echo "  $svc_name (port $ports):"
                echo "    DNS: $external_dns"
                if [[ -n "$external_ip" ]]; then
                    echo "    IP:  $external_ip"
                fi
                echo ""
            fi
        done < <(kubectl get svc --no-headers 2>/dev/null | grep "^memgraph-" | grep "LoadBalancer")
    fi

    # Show connection info
    echo ""
    log_info "To access Memgraph coordinator:"
    local coord_dns
    coord_dns=$(kubectl get svc memgraph-coordinator-1-external -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null) || coord_dns=""
    local coord_ip
    if [[ -n "$coord_dns" ]]; then
        coord_ip=$(dig +short "$coord_dns" 2>/dev/null | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | head -1) || coord_ip=""
    fi

    if [[ -n "$coord_ip" ]]; then
        echo "  mgconsole --host $coord_ip --port 7687"
    elif [[ -n "$coord_dns" ]]; then
        echo "  mgconsole --host $coord_dns --port 7687"
    else
        echo "  kubectl port-forward svc/memgraph-coordinator-1 7687:7687"
        echo "  mgconsole --host 127.0.0.1 --port 7687"
    fi
}

stop_memgraph() {
    log_info "Stopping Memgraph HA Deployment..."

    # Uninstall Helm release
    if helm list -q | grep -q "^${HELM_RELEASE_NAME}$"; then
        log_info "Uninstalling Helm release: $HELM_RELEASE_NAME"
        helm uninstall "$HELM_RELEASE_NAME"
    else
        log_warn "Helm release $HELM_RELEASE_NAME not found"
    fi

    # Delete PVCs
    log_info "Deleting PVCs..."
    kubectl get pvc --no-headers -o name 2>/dev/null | grep "memgraph-" | xargs -r kubectl delete 2>/dev/null || true

    log_info "Memgraph HA deployment stopped"
}

destroy_cluster() {
    log_info "Destroying EKS cluster: $CLUSTER_NAME..."

    # First stop Memgraph
    stop_memgraph 2>/dev/null || true

    # Delete the cluster
    log_info "Deleting EKS cluster (this may take 10-15 minutes)..."
    eksctl delete cluster --name "$CLUSTER_NAME" --region "$CLUSTER_REGION"

    if [[ $? -ne 0 ]]; then
        log_error "Failed to delete cluster"
        exit 1
    fi

    log_info "EKS cluster deleted successfully"
}

check_status() {
    log_info "Checking Memgraph HA Deployment status..."

    echo ""
    echo "=== EKS Cluster ==="
    if eksctl get cluster --name "$CLUSTER_NAME" --region "$CLUSTER_REGION" &> /dev/null; then
        echo "Cluster $CLUSTER_NAME: ACTIVE"
        kubectl cluster-info 2>/dev/null | head -2
    else
        echo "Cluster $CLUSTER_NAME: NOT FOUND"
        return 1
    fi

    echo ""
    echo "=== Nodes ==="
    kubectl get nodes -o wide 2>/dev/null || echo "Could not get nodes"

    echo ""
    echo "=== Helm Release ==="
    if helm list -q | grep -q "^${HELM_RELEASE_NAME}$"; then
        helm status "$HELM_RELEASE_NAME"
    else
        echo "Helm release $HELM_RELEASE_NAME: NOT FOUND"
    fi

    echo ""
    echo "=== Memgraph Pods ==="
    kubectl get pods -o wide 2>/dev/null | grep -E "^memgraph-|^NAME" || echo "No pods found"

    echo ""
    echo "=== Services ==="
    kubectl get svc 2>/dev/null | grep -E "^memgraph-|^NAME" || echo "No services found"

    echo ""
    echo "=== Persistent Volume Claims ==="
    kubectl get pvc 2>/dev/null | grep -E "^memgraph-|^NAME" || echo "No PVCs found"
}

upgrade_memgraph() {
    log_info "Upgrading Memgraph HA deployment..."

    if ! helm list -q | grep -q "^${HELM_RELEASE_NAME}$"; then
        log_error "Helm release $HELM_RELEASE_NAME not found. Use 'start' to install first."
        exit 1
    fi

    # Build helm upgrade command with values file
    local helm_cmd="helm upgrade $HELM_RELEASE_NAME $HELM_CHART_PATH -f $HELM_VALUES_FILE"

    # Override image tag if MEMGRAPH_IMAGE_TAG is set
    if [[ -n "${MEMGRAPH_IMAGE_TAG:-}" ]]; then
        helm_cmd+=" --set image.tag=$MEMGRAPH_IMAGE_TAG"
    fi

    # Override license if MEMGRAPH_ENTERPRISE_LICENSE is set
    if [[ -n "${MEMGRAPH_ENTERPRISE_LICENSE:-}" ]]; then
        helm_cmd+=" --set env.MEMGRAPH_ENTERPRISE_LICENSE=$MEMGRAPH_ENTERPRISE_LICENSE"
    fi

    # Override organization name if MEMGRAPH_ORGANIZATION_NAME is set
    if [[ -n "${MEMGRAPH_ORGANIZATION_NAME:-}" ]]; then
        helm_cmd+=" --set env.MEMGRAPH_ORGANIZATION_NAME=$MEMGRAPH_ORGANIZATION_NAME"
    fi

    # Pass any additional arguments
    helm_cmd+=" $@"

    log_info "Running: helm upgrade $HELM_RELEASE_NAME $HELM_CHART_PATH -f $HELM_VALUES_FILE [+ overrides]"
    eval "$helm_cmd"

    wait_for_pods
}

show_logs() {
    local pod_name=$1

    if [[ -z "$pod_name" ]]; then
        log_info "Available pods:"
        kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep "^memgraph-"
        echo ""
        echo "Usage: $0 logs <pod-name>"
        return 1
    fi

    kubectl logs "$pod_name" -f
}

exec_shell() {
    local pod_name=$1

    if [[ -z "$pod_name" ]]; then
        log_info "Available pods:"
        kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep "^memgraph-"
        echo ""
        echo "Usage: $0 exec <pod-name>"
        return 1
    fi

    kubectl exec -it "$pod_name" -- /bin/bash
}

port_forward() {
    local service=${1:-coordinator}
    local local_port=${2:-7687}
    local remote_port=${3:-7687}

    log_info "Setting up port forwarding..."

    local svc_name
    if [[ "$service" == "coordinator" ]]; then
        svc_name=$(kubectl get svc -o name | grep -i "memgraph-coordinator" | head -1)
    else
        svc_name=$(kubectl get svc -o name | grep -i "memgraph-.*$service" | head -1)
    fi

    if [[ -z "$svc_name" ]]; then
        log_error "Could not find service for: $service"
        kubectl get svc | grep -E "^memgraph-|^NAME"
        return 1
    fi

    log_info "Forwarding $local_port -> $svc_name:$remote_port"
    kubectl port-forward "$svc_name" "$local_port:$remote_port"
}

get_service_ip() {
    # Get the external IP of a LoadBalancer service
    # Returns the IP address (resolves DNS if needed)
    # Automatically appends -external suffix for the LoadBalancer service
    local service_name=$1

    if [[ -z "$service_name" ]]; then
        echo "Usage: $0 get-ip <service-name>" >&2
        return 1
    fi

    # Normalize service name (add memgraph- prefix if needed)
    local base_name
    if [[ "$service_name" == memgraph-* ]]; then
        base_name="$service_name"
    else
        base_name="memgraph-${service_name//_/-}"
    fi

    # Remove -external suffix if present (we'll add it back)
    base_name="${base_name%-external}"

    # The external LoadBalancer service has -external suffix
    local svc_name="${base_name}-external"

    # Check if service exists
    if ! kubectl get svc "$svc_name" &>/dev/null; then
        echo "Service $svc_name not found" >&2
        return 1
    fi

    # Try to get direct IP first
    local ip
    ip=$(kubectl get svc "$svc_name" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
    if [[ -n "$ip" && "$ip" != "<none>" ]]; then
        echo "$ip"
        return 0
    fi

    # Fall back to hostname (AWS returns DNS name)
    local hostname
    hostname=$(kubectl get svc "$svc_name" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null)
    if [[ -n "$hostname" && "$hostname" != "<none>" ]]; then
        # Resolve DNS to IP
        ip=$(dig +short "$hostname" 2>/dev/null | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | head -1)
        if [[ -n "$ip" ]]; then
            echo "$ip"
            return 0
        fi
        # Return hostname if DNS resolution failed
        echo "$hostname"
        return 0
    fi

    echo "Could not get external IP for service $svc_name (may still be pending)" >&2
    return 1
}

wait_service_ip() {
    # Wait for a LoadBalancer service to get an external IP
    local service_name=$1
    local timeout=${2:-300}  # Default 5 minutes

    if [[ -z "$service_name" ]]; then
        echo "Usage: $0 wait-ip <service-name> [timeout_seconds]" >&2
        return 1
    fi

    # Normalize service name
    local svc_name
    if [[ "$service_name" == memgraph-* ]]; then
        svc_name="$service_name"
    else
        svc_name="memgraph-${service_name//_/-}"
    fi

    local start_time=$(date +%s)
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [[ $elapsed -ge $timeout ]]; then
            echo "Timeout waiting for LoadBalancer IP for $svc_name" >&2
            return 1
        fi

        local ip
        ip=$(get_service_ip "$svc_name" 2>/dev/null)
        if [[ -n "$ip" && "$ip" != "<pending>" ]]; then
            echo "$ip"
            return 0
        fi

        echo "Waiting for LoadBalancer IP for $svc_name (${elapsed}s/${timeout}s)..." >&2
        sleep 10
    done
}

restart_instance() {
    # Restart an instance by deleting the pod (StatefulSet will recreate it)
    # Accepts formats: data_0, data-0, data_1, data-1, coordinator_1, coordinator-1, etc.
    local instance_name=$1

    if [[ -z "$instance_name" ]]; then
        log_error "Instance name is required"
        echo ""
        log_info "Available pods:"
        kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep "^memgraph-"
        echo ""
        echo "Usage: $0 restart <instance>"
        echo ""
        echo "Examples:"
        echo "  $0 restart data_0"
        echo "  $0 restart data_1"
        echo "  $0 restart coordinator_1"
        return 1
    fi

    # Normalize instance name: data_0 -> memgraph-data-0-0, coordinator_1 -> memgraph-coordinator-1-0
    local pod_name
    # Replace underscores with dashes
    local normalized_name="${instance_name//_/-}"

    # Check if it already has memgraph- prefix
    if [[ "$normalized_name" == memgraph-* ]]; then
        pod_name="${normalized_name}-0"
    else
        pod_name="memgraph-${normalized_name}-0"
    fi

    # Verify pod exists
    if ! kubectl get pod "$pod_name" &> /dev/null; then
        log_error "Pod '$pod_name' not found"
        log_info "Available pods:"
        kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep "^memgraph-"
        return 1
    fi

    log_info "Restarting instance: $pod_name"

    # Delete the pod (StatefulSet will recreate it automatically)
    kubectl delete pod "$pod_name" --wait=false
    log_info "Pod $pod_name deleted (will be recreated by StatefulSet)"
}

export_metrics() {
    # Export all Memgraph metrics from Prometheus to a JSON file
    local output_file=${1:-"metrics_$(date +%Y%m%d_%H%M%S).json"}
    local local_port=9090
    local pf_pid=""

    if [[ "$ENABLE_MONITORING" != "true" ]]; then
        log_error "Monitoring is not enabled. Set ENABLE_MONITORING=true and redeploy."
        return 1
    fi

    log_info "Exporting Prometheus metrics to: $output_file"

    # Check if Prometheus is available
    if ! kubectl get svc -n "$PROMETHEUS_NAMESPACE" kube-prometheus-stack-prometheus &>/dev/null; then
        log_error "Prometheus service not found in namespace $PROMETHEUS_NAMESPACE"
        return 1
    fi

    # Start port-forward in background
    log_info "Setting up port-forward to Prometheus..."
    kubectl port-forward -n "$PROMETHEUS_NAMESPACE" svc/kube-prometheus-stack-prometheus $local_port:9090 &>/dev/null &
    pf_pid=$!

    # Wait for port-forward to be ready
    sleep 3

    # Check if port-forward is still running
    if ! kill -0 $pf_pid 2>/dev/null; then
        log_error "Failed to establish port-forward to Prometheus"
        return 1
    fi

    # Cleanup function
    cleanup_pf() {
        if [[ -n "$pf_pid" ]]; then
            kill $pf_pid 2>/dev/null || true
        fi
    }
    trap cleanup_pf EXIT

    log_info "Querying Prometheus for Memgraph metrics..."

    # Query all memgraph metrics using Prometheus API
    # First, get the list of metric names that match memgraph_*
    local metric_names
    metric_names=$(curl -s "http://localhost:$local_port/api/v1/label/__name__/values" 2>/dev/null | \
        jq -r '.data[]' 2>/dev/null | grep -E '^memgraph_' || true)

    if [[ -z "$metric_names" ]]; then
        log_warn "No Memgraph metrics found. The exporter might not be running yet."
        # Try to get all metrics as fallback
        log_info "Fetching all available metrics..."
        # Use --data-urlencode to properly encode the regex query
        curl -s -G "http://localhost:$local_port/api/v1/query" \
            --data-urlencode 'query={__name__=~".+"}' 2>/dev/null | \
            jq '.' > "$output_file"
    else
        log_info "Found Memgraph metrics: $(echo "$metric_names" | wc -l | tr -d ' ') metric names"

        # Create a combined query for all memgraph metrics
        local result='{"timestamp": "'$(date -Iseconds)'", "cluster": "'$CLUSTER_NAME'", "metrics": []}'

        # Query each metric and combine results
        local tmp_file=$(mktemp)
        echo '[]' > "$tmp_file"

        for metric in $metric_names; do
            local metric_data
            metric_data=$(curl -s "http://localhost:$local_port/api/v1/query?query=$metric" 2>/dev/null)

            if [[ -n "$metric_data" ]]; then
                # Append to results
                jq --arg name "$metric" --argjson data "$metric_data" \
                    '. += [{"name": $name, "data": $data}]' "$tmp_file" > "${tmp_file}.new"
                mv "${tmp_file}.new" "$tmp_file"
            fi
        done

        # Build final output
        jq -n \
            --arg timestamp "$(date -Iseconds)" \
            --arg cluster "$CLUSTER_NAME" \
            --slurpfile metrics "$tmp_file" \
            '{timestamp: $timestamp, cluster: $cluster, metrics: $metrics[0]}' > "$output_file"

        rm -f "$tmp_file"
    fi

    # Cleanup port-forward
    cleanup_pf
    trap - EXIT

    if [[ -f "$output_file" ]]; then
        local file_size=$(du -h "$output_file" | cut -f1)
        log_info "Metrics exported successfully!"
        log_info "  File: $output_file"
        log_info "  Size: $file_size"
    else
        log_error "Failed to export metrics"
        return 1
    fi
}

print_usage() {
    echo "Usage: $0 {start|stop|destroy|status|upgrade|restart|export-metrics|logs|exec|port-forward} [options]"
    echo ""
    echo "Commands:"
    echo "  start               - Create EKS cluster and deploy Memgraph HA"
    echo "  stop                - Uninstall Memgraph HA and delete PVCs (keeps cluster)"
    echo "  destroy             - Delete entire EKS cluster"
    echo "  status              - Check deployment status"
    echo "  upgrade [flags]     - Upgrade Helm release with optional flags"
    echo "  restart <instance>  - Restart an instance (delete pod)"
    echo "  export-metrics [f]  - Export Prometheus metrics to JSON file"
    echo "  get-ip <service>    - Get external IP for a LoadBalancer service"
    echo "  wait-ip <service>   - Wait for external IP and return it"
    echo "  logs <pod>          - Follow logs for a pod"
    echo "  exec <pod>          - Open shell in a pod"
    echo "  port-forward [svc]  - Port forward to a service (default: coordinator)"
    echo ""
    echo "Configuration files (in eks/ directory):"
    echo "  cluster.yaml       - EKS cluster configuration"
    echo "  values.yaml        - Helm values for Memgraph HA"
    echo "  gp3-sc.yaml        - GP3 storage class configuration"
    echo ""
    echo "Environment variables:"
    echo "  CLUSTER_NAME                  - EKS cluster name (default: test-cluster-ha)"
    echo "  CLUSTER_REGION                - AWS region (default: eu-west-1)"
    echo "  HELM_RELEASE_NAME             - Helm release name (default: mem-ha-test)"
    echo "  HELM_CHART_PATH               - Path to Helm chart (default: memgraph/memgraph-high-availability)"
    echo "  POD_READY_TIMEOUT             - Timeout for pods to be ready in seconds (default: 600)"
    echo "  ENABLE_MONITORING             - Install kube-prometheus-stack (default: true)"
    echo ""
    echo "  MEMGRAPH_IMAGE_TAG            - Override image.tag in values.yaml"
    echo "  MEMGRAPH_ENTERPRISE_LICENSE   - Override env.MEMGRAPH_ENTERPRISE_LICENSE in values.yaml"
    echo "  MEMGRAPH_ORGANIZATION_NAME    - Override env.MEMGRAPH_ORGANIZATION_NAME in values.yaml"
    echo ""
    echo "Note: When ENABLE_MONITORING=true, also set prometheus.enabled=true in values.yaml"
    echo ""
    echo "Examples:"
    echo "  $0 start                           # Create cluster and deploy"
    echo "  $0 status                          # Check status"
    echo "  $0 restart data_1                  # Restart data instance 1"
    echo "  $0 export-metrics                  # Export metrics to timestamped file"
    echo "  $0 export-metrics results.json     # Export metrics to specific file"
    echo "  $0 port-forward coordinator 7687   # Port forward coordinator"
    echo "  $0 logs mem-ha-test-data-0         # View pod logs"
    echo "  $0 upgrade --set key=value         # Upgrade with custom values"
    echo "  $0 stop                            # Remove Memgraph deployment + delete PVCs"
    echo "  $0 destroy                         # Delete entire cluster"
}

# Main command handler
case "$1" in
    start)
        start_memgraph
        ;;
    stop)
        stop_memgraph
        ;;
    destroy)
        destroy_cluster
        ;;
    status)
        check_status
        ;;
    upgrade)
        shift
        upgrade_memgraph "$@"
        ;;
    restart)
        restart_instance "$2"
        ;;
    export-metrics)
        export_metrics "$2"
        ;;
    get-ip)
        get_service_ip "$2"
        ;;
    wait-ip)
        wait_service_ip "$2" "$3"
        ;;
    logs)
        show_logs "$2"
        ;;
    exec)
        exec_shell "$2"
        ;;
    port-forward|pf)
        port_forward "$2" "$3" "$4"
        ;;
    *)
        print_usage
        exit 1
        ;;
esac
