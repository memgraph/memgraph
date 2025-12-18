#!/bin/bash

# EKS HA Deployment Script for Memgraph
# This script manages a Memgraph HA deployment on AWS EKS

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Cluster configuration
CLUSTER_NAME="${CLUSTER_NAME:-test-cluster-ha}"
CLUSTER_REGION="${CLUSTER_REGION:-eu-west-1}"
CLUSTER_CONFIG_FILE="${SCRIPT_DIR}/eks/cluster.yaml"

# Helm configuration
HELM_RELEASE_NAME="${HELM_RELEASE_NAME:-mem-ha-test}"
HELM_CHART_PATH="${HELM_CHART_PATH:-memgraph/memgraph-high-availability}"
HELM_REPO_NAME="memgraph"
HELM_REPO_URL="https://memgraph.github.io/helm-charts"

# Storage configuration
STORAGE_CLASS_NAME="gp3"
STORAGE_CLASS_FILE="${SCRIPT_DIR}/eks/gp3-sc.yaml"

# Helm values file
HELM_VALUES_FILE="${SCRIPT_DIR}/eks/values.yaml"

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

    log_info "EKS cluster created successfully"
}

setup_helm_repo() {
    log_info "Setting up Helm repository..."

    # Add Memgraph Helm repo if not already added
    if ! helm repo list | grep -q "$HELM_REPO_NAME"; then
        helm repo add "$HELM_REPO_NAME" "$HELM_REPO_URL"
    fi

    helm repo update

    log_info "Helm repository configured"
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

    log_info "Running: $helm_cmd"
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

    # Show services with external access info
    log_info "Services:"
    echo ""
    printf "  %-35s %-10s %-20s\n" "SERVICE" "PORT" "CLUSTER-IP"
    printf "  %-35s %-10s %-20s\n" "-------" "----" "----------"
    while IFS= read -r line; do
        local svc_name=$(echo "$line" | awk '{print $1}')
        local cluster_ip=$(echo "$line" | awk '{print $3}')
        local ports=$(echo "$line" | awk '{print $5}' | cut -d'/' -f1)

        printf "  %-35s %-10s %-20s\n" "$svc_name" "$ports" "$cluster_ip"
    done < <(kubectl get svc --no-headers 2>/dev/null | grep "^memgraph-")

    # Show external endpoints separately with DNS and IP
    local lb_count
    lb_count=$(kubectl get svc --no-headers 2>/dev/null | grep "^memgraph-" | grep -c "LoadBalancer") || lb_count=0
    if [[ "$lb_count" -gt 0 ]]; then
        echo ""
        log_info "External Endpoints:"
        echo ""
        while IFS= read -r line; do
            local svc_name=$(echo "$line" | awk '{print $1}')
            local external_dns=$(echo "$line" | awk '{print $4}')
            local ports=$(echo "$line" | awk '{print $5}' | cut -d'/' -f1 | cut -d':' -f1)

            if [[ "$external_dns" != "<none>" && "$external_dns" != "<pending>" && -n "$external_dns" ]]; then
                # Resolve DNS to IP
                local external_ip
                external_ip=$(dig +short "$external_dns" 2>/dev/null | head -1) || external_ip=""

                echo "  $svc_name (port $ports):"
                echo "    DNS: $external_dns"
                if [[ -n "$external_ip" ]]; then
                    echo "    IP:  $external_ip"
                fi
                echo ""
            fi
        done < <(kubectl get svc --no-headers 2>/dev/null | grep "^memgraph-" | grep "LoadBalancer")
    fi

    log_info "To access Memgraph via port-forwarding:"
    echo "  kubectl port-forward svc/memgraph-coordinator-1 7687:7687"
    echo ""
    log_info "Then connect with:"
    echo "  mgconsole --host 127.0.0.1 --port 7687"
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

    # Optionally delete PVCs
    read -p "Delete persistent volume claims? (y/N): " delete_pvcs
    if [[ "$delete_pvcs" =~ ^[Yy]$ ]]; then
        log_info "Deleting PVCs..."
        kubectl get pvc --no-headers -o name 2>/dev/null | grep "memgraph-" | xargs -r kubectl delete 2>/dev/null || true
    fi

    log_info "Memgraph HA deployment stopped"
}

destroy_cluster() {
    log_info "Destroying EKS cluster: $CLUSTER_NAME..."

    read -p "Are you sure you want to delete the entire EKS cluster? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        log_info "Cluster deletion cancelled"
        return 0
    fi

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

    # Pass any additional arguments
    helm_cmd+=" $@"

    log_info "Running: $helm_cmd"
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

print_usage() {
    echo "Usage: $0 {start|stop|destroy|status|upgrade|logs|exec|port-forward} [options]"
    echo ""
    echo "Commands:"
    echo "  start              - Create EKS cluster and deploy Memgraph HA"
    echo "  stop               - Uninstall Memgraph HA (keeps cluster)"
    echo "  destroy            - Delete entire EKS cluster"
    echo "  status             - Check deployment status"
    echo "  upgrade [flags]    - Upgrade Helm release with optional flags"
    echo "  logs <pod>         - Follow logs for a pod"
    echo "  exec <pod>         - Open shell in a pod"
    echo "  port-forward [svc] - Port forward to a service (default: coordinator)"
    echo ""
    echo "Configuration files (in eks/ directory):"
    echo "  cluster.yaml       - EKS cluster configuration"
    echo "  values.yaml        - Helm values for Memgraph HA"
    echo "  gp3-sc.yaml        - GP3 storage class configuration"
    echo ""
    echo "Environment variables:"
    echo "  CLUSTER_NAME                - EKS cluster name (default: test-cluster-ha)"
    echo "  CLUSTER_REGION              - AWS region (default: eu-west-1)"
    echo "  HELM_RELEASE_NAME           - Helm release name (default: mem-ha-test)"
    echo "  HELM_CHART_PATH             - Path to Helm chart (default: memgraph/memgraph-high-availability)"
    echo "  POD_READY_TIMEOUT           - Timeout for pods to be ready in seconds (default: 600)"
    echo ""
    echo "Note: Set MEMGRAPH_ENTERPRISE_LICENSE and MEMGRAPH_ORGANIZATION_NAME in eks/values.yaml"
    echo ""
    echo "Examples:"
    echo "  $0 start                           # Create cluster and deploy"
    echo "  $0 status                          # Check status"
    echo "  $0 port-forward coordinator 7687   # Port forward coordinator"
    echo "  $0 logs mem-ha-test-data-0         # View pod logs"
    echo "  $0 upgrade --set key=value         # Upgrade with custom values"
    echo "  $0 stop                            # Remove Memgraph deployment"
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
