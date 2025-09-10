#!/usr/bin/env bash
set -Eeuo pipefail

# Function to display usage
usage() {
  echo "Usage: $0 <last_tag> <next_tag> [options]"
  echo ""
  echo "Arguments:"
  echo "  last_tag    The previous version tag (e.g., 2.3.3)"
  echo "  next_tag    The new version tag (e.g., 2.3.4)"
  echo ""
  echo "Options:"
  echo "  --test-routing, --test-routing=true    Enable routing tests"
  echo "  --test-routing=false, --no-test-routing    Disable routing tests (default)"
  echo ""
  echo "Example:"
  echo "  $0 2.3.3 2.3.4"
  echo "  $0 2.3.3 2.3.4 --test-routing"
  exit 1
}

# Check if at least 2 arguments are provided
if [ $# -lt 2 ]; then
  echo "Error: Missing required arguments"
  usage
fi

# Extract required arguments
LAST_TAG="$1"
NEXT_TAG="$2"
shift 2  # Remove the first two arguments from $@

SC_NAME="csi-hostpath-delayed"
RELEASE="memgraph-db"

RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'

# Display the tags being used
echo -e "${GREEN}Starting ISSU test:${NC}"
echo -e "${GREEN}  Last tag: ${LAST_TAG}${NC}"
echo -e "${GREEN}  Next tag: ${NEXT_TAG}${NC}"

# Generate YAML files from template
echo -e "${GREEN}Generating YAML files from template...${NC}"
if [ ! -f "values_template.yaml" ]; then
  echo -e "${RED}Error: values_template.yaml not found${NC}"
  exit 1
fi

# Generate old_values.yaml with LAST_TAG
sed "s/{{VERSION_TAG}}/${LAST_TAG}/g" values_template.yaml > old_values.yaml
echo -e "${GREEN}Generated old_values.yaml with tag: ${LAST_TAG}${NC}"

# Generate new_values.yaml with NEXT_TAG
sed "s/{{VERSION_TAG}}/${NEXT_TAG}/g" values_template.yaml > new_values.yaml
echo -e "${GREEN}Generated new_values.yaml with tag: ${NEXT_TAG}${NC}"

TEST_ROUTING=${TEST_ROUTING:-false}
for arg in "${@:-}"; do
  case "$arg" in
    --test-routing|--test-routing=true) TEST_ROUTING=true ;;
    --test-routing=false|--no-test-routing) TEST_ROUTING=false ;;
    *) echo "Warning: Unknown option '$arg'" ;;
  esac
done

_CLEANED=false
cleanup() {
  set +e
  echo -e "${YELLOW}Starting cleanup...${NC}"
  
  # Clean up Helm release
  helm uninstall "$RELEASE" >/dev/null 2>&1 || true
  
  # Clean up Kubernetes resources
  kubectl delete pvc --all >/dev/null 2>&1 || true
  kubectl delete storageclass "$SC_NAME" --ignore-not-found >/dev/null 2>&1 || true
  
  # Clean up generated YAML files
  rm -f old_values.yaml new_values.yaml
  
  # Clean up cloned helm-charts directory
  rm -rf helm-charts
  
  # Stop minikube cluster
  if command -v minikube >/dev/null 2>&1; then
    echo -e "${YELLOW}Stopping minikube cluster...${NC}"
    minikube stop >/dev/null 2>&1 || true
    minikube delete >/dev/null 2>&1 || true
  fi

  echo -e "${GREEN}Cleanup finished.${NC}"
}

trap cleanup EXIT
trap 'echo -e "'"${RED}"'Error on line $LINENO. Exiting…'"${NC}"'" >&2' ERR
trap 'echo -e "'"${RED}"'SIGINT received. Exiting…'"${NC}"'" >&2' INT
trap 'echo -e "'"${RED}"'SIGTERM received. Exiting…'"${NC}"'" >&2' TERM

# Check if minikube is available
if ! command -v minikube >/dev/null 2>&1; then
  echo -e "${RED}Error: minikube is not installed or not in PATH${NC}"
  echo "Please install minikube: https://minikube.sigs.k8s.io/docs/start/"
  exit 1
fi

# Check if kubectl is available
if ! command -v kubectl >/dev/null 2>&1; then
  echo -e "${RED}Error: kubectl is not installed or not in PATH${NC}"
  echo "Please install kubectl: https://kubernetes.io/docs/tasks/tools/"
  exit 1
fi

# Check if helm is available
if ! command -v helm >/dev/null 2>&1; then
  echo -e "${RED}Error: helm is not installed or not in PATH${NC}"
  echo "Please install helm: https://helm.sh/docs/intro/install/"
  exit 1
fi

# Check if git is available
if ! command -v git >/dev/null 2>&1; then
  echo -e "${RED}Error: git is not installed or not in PATH${NC}"
  echo "Please install git: https://git-scm.com/downloads"
  exit 1
fi

# Start minikube cluster
echo -e "${GREEN}Starting minikube cluster...${NC}"
minikube start --driver=docker --memory=4096 --cpus=2 --disk-size=20g

# Wait for cluster to be ready
echo -e "${GREEN}Waiting for cluster to be ready...${NC}"
kubectl wait --for=condition=ready node --all --timeout=300s

# Check if storage class file exists
if [ ! -f "sc.yaml" ]; then
  echo -e "${RED}Error: sc.yaml not found${NC}"
  echo "Please ensure sc.yaml exists in the current directory"
  exit 1
fi

# Create CSI storage class
kubectl apply -f sc.yaml
echo "Created $SC_NAME storage class"

# Start by bringing up nodes and labelling them
nodes=($(kubectl get nodes --no-headers -o custom-columns=":metadata.name"))

if [ ${#nodes[@]} -eq 0 ]; then
  echo "No nodes found in the cluster"
  exit 1
fi

echo "Found ${#nodes[@]} nodes in the cluster."
for i in "${!nodes[@]}"; do
  node="${nodes[$i]}"
  if [ $i -lt 3 ]; then
    echo "Labeling node '$node' with role=coordinator-node"
    kubectl label node "$node" role=coordinator-node --overwrite
  else
    echo "Labeling node '$node' with role=data-node"
    kubectl label node "$node" role=data-node --overwrite
  fi
done

# Clone helm-charts repository if it doesn't exist
HELM_CHARTS_DIR="helm-charts"
if [ ! -d "$HELM_CHARTS_DIR" ]; then
  echo -e "${GREEN}Cloning helm-charts repository...${NC}"
  git clone https://github.com/memgraph/helm-charts.git "$HELM_CHARTS_DIR"
else
  echo -e "${GREEN}Updating helm-charts repository...${NC}"
  cd "$HELM_CHARTS_DIR"
  git pull
  cd ..
fi

HELM_CHART_PATH="$HELM_CHARTS_DIR/charts/memgraph-high-availability"
if [ ! -d "$HELM_CHART_PATH" ]; then
  echo -e "${RED}Error: Helm chart not found at $HELM_CHART_PATH${NC}"
  echo "Please check the helm-charts repository structure"
  exit 1
fi

# Install Helm chart
helm repo update
helm install "$RELEASE" "$HELM_CHART_PATH" -f old_values.yaml

# Wait until pods became ready
echo "Waiting for 90s until all pods become ready"
kubectl wait --for=condition=ready pod --all --timeout=90s
sleep 5

echo "All pods became ready"

# Setup cluster on the 1st coordinator
kubectl cp setup.cypherl memgraph-coordinator-1-0:/var/lib/memgraph/setup.cypherl
kubectl exec memgraph-coordinator-1-0 -- bash -c "mgconsole < /var/lib/memgraph/setup.cypherl"
echo "Initialized cluster"

# Set-up authentication on MAIN instance
kubectl cp auth_pre_upgrade.cypherl memgraph-data-0-0:/var/lib/memgraph/auth_pre_upgrade.cypherl
kubectl exec memgraph-data-0-0 -- bash -c "mgconsole < /var/lib/memgraph/auth_pre_upgrade.cypherl"

# Copy test file into the pod and create some data
kubectl cp pre_upgrade_global.cypherl memgraph-data-0-0:/var/lib/memgraph/pre_upgrade_global.cypherl
kubectl cp pre_upgrade_mg.cypherl memgraph-data-0-0:/var/lib/memgraph/pre_upgrade_mg.cypherl
kubectl cp pre_upgrade_db1.cypherl memgraph-data-0-0:/var/lib/memgraph/pre_upgrade_db1.cypherl
kubectl exec memgraph-data-0-0 -- bash -c "mgconsole < /var/lib/memgraph/pre_upgrade_global.cypherl --username=system_admin_user --password=admin_password" 
kubectl exec memgraph-data-0-0 -- bash -c "mgconsole < /var/lib/memgraph/pre_upgrade_mg.cypherl --username=system_admin_user --password=admin_password" 
kubectl exec memgraph-data-0-0 -- bash -c "mgconsole < /var/lib/memgraph/pre_upgrade_db1.cypherl --username=tenant1_admin_user --password=t1_admin_pass" 
echo "Run test queries on old version"

# Upgrade to use newer version
helm upgrade "$RELEASE" "$HELM_CHART_PATH" -f new_values.yaml
echo "Updated versions"

# Rolling restarts, waiting each time
echo "Deleting pod memgraph-data-1-0 which serves as replica"
kubectl delete pod memgraph-data-1-0
kubectl wait --for=condition=ready pod --all --timeout=90s
echo "Upgrade of pod memgraph-data-1-0 passed successfully"

echo "Deleting pod memgraph-data-0-0 which serves as main"
kubectl scale statefulset memgraph-data-0 --replicas=0
sleep 5
kubectl scale statefulset memgraph-data-0 --replicas=1
kubectl wait --for=condition=ready pod --all --timeout=90s
echo "Upgrade of pod memgraph-data-0-0 passed successfully"

echo "Deleting pod memgraph-coordinator-3-0 which serves as follower"
kubectl delete pod memgraph-coordinator-3-0
kubectl wait --for=condition=ready pod --all --timeout=90s
echo "Upgrade of pod memgraph-coordinator-3-0 passed successfully"

echo "Deleting pod memgraph-coordinator-2-0 which serves as follower"
kubectl delete pod memgraph-coordinator-2-0
kubectl wait --for=condition=ready pod --all --timeout=90s
echo "Upgrade of pod memgraph-coordinator-2-0 passed successfully"

echo "Deleting pod memgraph-coordinator-1-0 which serves as leader"
kubectl delete pod memgraph-coordinator-1-0
kubectl wait --for=condition=ready pod --all --timeout=90s
echo "Upgrade of pod memgraph-coordinator-1-0 passed successfully"

# Test that you can run queries on the new main
kubectl cp post_upgrade_mg.cypherl memgraph-data-1-0:/var/lib/memgraph/post_upgrade_mg.cypherl
kubectl cp post_upgrade_db1.cypherl memgraph-data-1-0:/var/lib/memgraph/post_upgrade_db1.cypherl
kubectl exec memgraph-data-1-0 -- bash -c "mgconsole < /var/lib/memgraph/post_upgrade_mg.cypherl --username=system_admin_user --password=admin_password" 
kubectl exec memgraph-data-1-0 -- bash -c "mgconsole < /var/lib/memgraph/post_upgrade_db1.cypherl --username=tenant1_admin_user --password=t1_admin_pass" 

# Auth post
kubectl cp auth_post_upgrade.cypherl memgraph-data-1-0:/var/lib/memgraph/auth_post_upgrade.cypherl
kubectl exec memgraph-data-1-0 -- bash -c "mgconsole < /var/lib/memgraph/auth_post_upgrade.cypherl --username=system_admin_user --password=admin_password" 

if [[ "$TEST_ROUTING" == "true" ]]; then
  # Setup routing on coordinators
  kubectl cp routing.py memgraph-coordinator-1-0:/var/lib/memgraph/routing.py
  kubectl cp routing.py memgraph-coordinator-2-0:/var/lib/memgraph/routing.py
  kubectl cp routing.py memgraph-coordinator-3-0:/var/lib/memgraph/routing.py
  kubectl exec memgraph-coordinator-1-0 -- bash -c "python3.12 -m venv ~/env && source ~/env/bin/activate && pip install neo4j"
  kubectl exec memgraph-coordinator-2-0 -- bash -c "python3.12 -m venv ~/env && source ~/env/bin/activate && pip install neo4j"
  kubectl exec memgraph-coordinator-3-0 -- bash -c "python3.12 -m venv ~/env && source ~/env/bin/activate && pip install neo4j"
  kubectl exec memgraph-coordinator-1-0 -- bash -c "source ~/env/bin/activate && python3.12 /var/lib/memgraph/routing.py" 
  kubectl exec memgraph-coordinator-2-0 -- bash -c "source ~/env/bin/activate && python3.12 /var/lib/memgraph/routing.py" 
  kubectl exec memgraph-coordinator-3-0 -- bash -c "source ~/env/bin/activate && python3.12 /var/lib/memgraph/routing.py" 
else
  echo -e "${YELLOW}test_routing=false → skipping routing setup${NC}"
fi

echo "Test successfully finished!"
# (No explicit cleanup here—trap will run it)
