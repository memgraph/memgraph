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
  echo "  --test-routing, --test-routing=true         Enable routing tests"
  echo "  --test-routing=false, --no-test-routing     Disable routing tests (default)"
  echo "  --debug=true|false                          If true, also delete StorageClass and Minikube at the end (default: false)"
  echo ""
  echo "Example:"
  echo "  $0 2.3.3 2.3.4"
  echo "  $0 2.3.3 2.3.4 --test-routing --debug=true"
  exit 1
}

# --- Validate args ---
if [ $# -lt 2 ]; then
  echo "Error: Missing required arguments"
  usage
fi

# --- Required args ---
LAST_TAG="$1"
NEXT_TAG="$2"
shift 2

# --- Config ---
SC_NAME="csi-hostpath-delayed"
RELEASE="memgraph-db"
DESIRED_NODES=5
PROFILE="${PROFILE:-minikube}"  # can be overridden via env

RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'

# Defaults for flags/env overrides
TEST_ROUTING=${TEST_ROUTING:-false}
DEBUG=${DEBUG:-false}

# --- Parse flags ---
while [ "$#" -gt 0 ]; do
  case "$1" in
    --test-routing|--test-routing=true) TEST_ROUTING=true ;;
    --test-routing=false|--no-test-routing) TEST_ROUTING=false ;;
    --debug=true) DEBUG=true ;;
    --debug=false) DEBUG=false ;;
    --) shift; break ;;
    *) echo "Warning: Unknown option '$1'";;
  esac
  shift
done

# --- Banner ---
echo -e "${GREEN}Starting ISSU test:${NC}"
echo -e "${GREEN}  Last tag: ${LAST_TAG}${NC}"
echo -e "${GREEN}  Next tag: ${NEXT_TAG}${NC}"
echo -e "${GREEN}  Debug cleanup (extra): ${DEBUG}${NC}"

# --- Generate values files ---
echo -e "${GREEN}Generating YAML files from template...${NC}"
if [ ! -f "values_template.yaml" ]; then
  echo -e "${RED}Error: values_template.yaml not found${NC}"
  exit 1
fi

ENTERPRISE_LICENSE=${MEMGRAPH_ENTERPRISE_LICENSE:-""}
ORGANIZATION_NAME=${MEMGRAPH_ORGANIZATION_NAME:-""}

sed -e "s/{{VERSION_TAG}}/${LAST_TAG}/g" \
    -e "s/{{ENTERPRISE_LICENSE}}/${ENTERPRISE_LICENSE}/g" \
    -e "s/{{ORGANIZATION_NAME}}/${ORGANIZATION_NAME}/g" \
    values_template.yaml > old_values.yaml
echo -e "${GREEN}Generated old_values.yaml with tag: ${LAST_TAG}${NC}"

sed -e "s/{{VERSION_TAG}}/${NEXT_TAG}/g" \
    -e "s/{{ENTERPRISE_LICENSE}}/${ENTERPRISE_LICENSE}/g" \
    -e "s/{{ORGANIZATION_NAME}}/${ORGANIZATION_NAME}/g" \
    values_template.yaml > new_values.yaml
echo -e "${GREEN}Generated new_values.yaml with tag: ${NEXT_TAG}${NC}"

echo -e "${YELLOW}Debug: Checking generated values files...${NC}"
echo "old_values.yaml image tag:"; grep "tag:" old_values.yaml || echo "No tag found in old_values.yaml"
echo "new_values.yaml image tag:"; grep "tag:" new_values.yaml || echo "No tag found in new_values.yaml"

# --- Cleanup (always uninstall helm + delete PVCs; minikube only if --debug=true) ---
cleanup() {
  set +e
  echo -e "${YELLOW}Starting cleanup...${NC}"

  # Always remove generated files and local repo folder
  rm -f old_values.yaml new_values.yaml
  rm -rf helm-charts

  # Always uninstall Helm release and clean PVCs
  echo -e "${YELLOW}Uninstalling Helm release '${RELEASE}'...${NC}"
  helm uninstall "$RELEASE" >/dev/null 2>&1 || true

  echo -e "${YELLOW}Deleting PVCs in the current namespace...${NC}"
  kubectl delete pvc --all >/dev/null 2>&1 || true
  kubectl delete storageclass "$SC_NAME" --ignore-not-found >/dev/null 2>&1 || true


  if [[ "$DEBUG" == "false" ]]; then
    echo -e "${YELLOW}--debug=false → performing extra cleanup (SC + Minikube)${NC}"
    if command -v minikube >/dev/null 2>&1; then
      echo -e "${YELLOW}Stopping and deleting minikube profile '${PROFILE}'...${NC}"
      minikube stop -p "$PROFILE" >/dev/null 2>&1 || true
      minikube delete -p "$PROFILE" >/dev/null 2>&1 || true
    fi
  else
    echo -e "${YELLOW}--debug=false → leaving StorageClass & Minikube cluster running${NC}"
  fi

  echo -e "${GREEN}Cleanup finished.${NC}"
}

trap cleanup EXIT
trap 'echo -e "'"${RED}"'Error on line $LINENO. Exiting…'"${NC}"'" >&2' ERR
trap 'echo -e "'"${RED}"'SIGINT received. Exiting…'"${NC}"'" >&2' INT
trap 'echo -e "'"${RED}"'SIGTERM received. Exiting…'"${NC}"'" >&2' TERM

# --- Check required CLIs ---
for bin in minikube kubectl helm git; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo -e "${RED}Error: $bin is not installed or not in PATH${NC}"
    case "$bin" in
      minikube) echo "Install: https://minikube.sigs.k8s.io/docs/start/";;
      kubectl)  echo "Install: https://kubernetes.io/docs/tasks/tools/";;
      helm)     echo "Install: https://helm.sh/docs/intro/install/";;
      git)      echo "Install: https://git-scm.com/downloads";;
    esac
    exit 1
  fi
done

# Ensure kubectl context points to the profile (best-effort)
minikube update-context -p "$PROFILE" >/dev/null 2>&1 || true
kubectl config use-context "$PROFILE" >/dev/null 2>&1 || true

# --- Ensure Minikube with desired nodes ---
echo -e "${GREEN}Ensuring Minikube cluster '${PROFILE}' with ${DESIRED_NODES} nodes...${NC}"

existing_nodes=$(minikube kubectl -p "$PROFILE" -- get nodes --no-headers 2>/dev/null | wc -l || echo 0)
profile_status=$(minikube status -p "$PROFILE" 2>/dev/null || true)
if echo "$profile_status" | grep -qiE 'host: Running|apiserver: Running|kubelet: Running'; then
  cluster_running=true
else
  cluster_running=false
fi

if [[ "$cluster_running" == "true" ]]; then
  if [ "$existing_nodes" -ge "$DESIRED_NODES" ]; then
    echo -e "${GREEN}Cluster '${PROFILE}' already running with ${existing_nodes} nodes — skipping start.${NC}"
  else
    to_add=$(( DESIRED_NODES - existing_nodes ))
    echo -e "${YELLOW}Cluster running with ${existing_nodes} nodes; adding ${to_add} worker node(s)...${NC}"
    for _ in $(seq 1 "$to_add"); do
      minikube node add -p "$PROFILE" --worker
    done
  fi
else
  echo -e "${GREEN}Starting Minikube '${PROFILE}' with ${DESIRED_NODES} nodes...${NC}"
  minikube start -p "$PROFILE" --driver=docker --nodes="$DESIRED_NODES"
  minikube update-context -p "$PROFILE"
  kubectl config use-context "$PROFILE" >/dev/null 2>&1 || true
  minikube addons disable storage-provisioner
  minikube addons disable default-storageclass
  minikube addons enable volumesnapshots
  minikube addons enable csi-hostpath-driver
fi

# --- Wait for cluster ready ---
echo -e "${GREEN}Waiting for cluster to be ready...${NC}"
kubectl wait --for=condition=ready node --all --timeout=300s

# Load Docker images into minikube
echo -e "${GREEN}Loading Docker images into minikube...${NC}"

# Check if images exist using docker image inspect
if docker image inspect "memgraph/memgraph:${LAST_TAG}" >/dev/null 2>&1; then
  echo "Loading memgraph/memgraph:${LAST_TAG} into minikube..."
  minikube image load "memgraph/memgraph:${LAST_TAG}"
else
  echo -e "${YELLOW}Warning: memgraph/memgraph:${LAST_TAG} not found locally, will be pulled during pod creation${NC}"
fi

if docker image inspect "memgraph/memgraph:${NEXT_TAG}" >/dev/null 2>&1; then
  echo "Loading memgraph/memgraph:${NEXT_TAG} into minikube..."
  minikube image load "memgraph/memgraph:${NEXT_TAG}"
else
  echo -e "${YELLOW}Warning: memgraph/memgraph:${NEXT_TAG} not found locally, will be pulled during pod creation${NC}"
fi

kubectl apply -f sc.yaml
echo "Created $SC_NAME storage class"

# --- Label nodes ---
nodes=($(kubectl get nodes --no-headers -o custom-columns=":metadata.name"))
if [ ${#nodes[@]} -eq 0 ]; then
  echo "No nodes found in the cluster"
  exit 1
fi

echo "Found ${#nodes[@]} nodes in the cluster."
for i in "${!nodes[@]}"; do
  node="${nodes[$i]}"
  if [ "$i" -lt 3 ]; then
    echo "Labeling node '$node' with role=coordinator-node"
    kubectl label node "$node" role=coordinator-node --overwrite
  else
    echo "Labeling node '$node' with role=data-node"
    kubectl label node "$node" role=data-node --overwrite
  fi
done


# --- Helm chart prep ---
helm repo add memgraph https://memgraph.github.io/helm-charts


# --- Helm install ---
echo -e "${GREEN}Installing Helm chart...${NC}"
helm install "$RELEASE" memgraph/memgraph-high-availability -f old_values.yaml

# --- Wait & verify resources ---
echo -e "${GREEN}Waiting for resources to be created...${NC}"
sleep 10

echo -e "${GREEN}Checking created resources...${NC}"
kubectl get all -l app.kubernetes.io/instance="$RELEASE" || true
kubectl get pods || true

echo -e "${GREEN}Waiting for pods to become ready...${NC}"
echo "Current pod status:"
kubectl get pods -o wide

echo "Waiting up to 5 minutes for pods to be ready..."
if ! kubectl wait --for=condition=ready pod --all --timeout=300s; then
  echo -e "${YELLOW}Warning: Some pods may not be ready yet. Checking status...${NC}"
  kubectl get pods -o wide
  echo "Pod events:"
  kubectl get events --sort-by=.metadata.creationTimestamp | tail -20
  echo "Checking pod logs for any issues..."
  for pod in $(kubectl get pods --no-headers -o custom-columns=":metadata.name"); do
    echo "=== Logs for $pod ==="
    kubectl logs "$pod" --tail=10 || true
  done
  echo -e "${YELLOW}Continuing anyway - some pods might still be starting...${NC}"
fi
sleep 10

echo "All pods became ready"

# --- Pre-upgrade setup ---
kubectl cp setup.cypherl memgraph-coordinator-1-0:/var/lib/memgraph/setup.cypherl
kubectl exec memgraph-coordinator-1-0 -- bash -c "mgconsole < /var/lib/memgraph/setup.cypherl"
echo "Initialized cluster"

kubectl cp auth_pre_upgrade.cypherl memgraph-data-0-0:/var/lib/memgraph/auth_pre_upgrade.cypherl
kubectl exec memgraph-data-0-0 -- bash -c "mgconsole < /var/lib/memgraph/auth_pre_upgrade.cypherl"

kubectl cp pre_upgrade_global.cypherl memgraph-data-0-0:/var/lib/memgraph/pre_upgrade_global.cypherl
kubectl cp pre_upgrade_mg.cypherl     memgraph-data-0-0:/var/lib/memgraph/pre_upgrade_mg.cypherl
kubectl cp pre_upgrade_db1.cypherl    memgraph-data-0-0:/var/lib/memgraph/pre_upgrade_db1.cypherl
kubectl exec memgraph-data-0-0 -- bash -c "mgconsole < /var/lib/memgraph/pre_upgrade_global.cypherl --username=system_admin_user --password=admin_password"
kubectl exec memgraph-data-0-0 -- bash -c "mgconsole < /var/lib/memgraph/pre_upgrade_mg.cypherl     --username=system_admin_user --password=admin_password"
kubectl exec memgraph-data-0-0 -- bash -c "mgconsole < /var/lib/memgraph/pre_upgrade_db1.cypherl   --username=tenant1_admin_user --password=t1_admin_pass"
echo "Run test queries on old version"

# --- Upgrade chart values ---
helm upgrade "$RELEASE" memgraph/memgraph-high-availability -f new_values.yaml
echo "Updated versions"

# --- Rolling restarts ---
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

# --- Post-upgrade verification ---
kubectl cp post_upgrade_mg.cypherl  memgraph-data-1-0:/var/lib/memgraph/post_upgrade_mg.cypherl
kubectl cp post_upgrade_db1.cypherl memgraph-data-1-0:/var/lib/memgraph/post_upgrade_db1.cypherl
echo "Running post-upgrade tests on database 'memgraph'"
kubectl exec memgraph-data-1-0 -- bash -c "mgconsole < /var/lib/memgraph/post_upgrade_mg.cypherl  --username=system_admin_user --password=admin_password"
echo "Running post-upgrade tests on database 'db1'"
kubectl exec memgraph-data-1-0 -- bash -c "mgconsole < /var/lib/memgraph/post_upgrade_db1.cypherl --username=tenant1_admin_user --password=t1_admin_pass"

echo "Running auth post-upgrade tests"
kubectl cp auth_post_upgrade.cypherl memgraph-data-1-0:/var/lib/memgraph/auth_post_upgrade.cypherl
kubectl exec memgraph-data-1-0 -- bash -c "mgconsole < /var/lib/memgraph/auth_post_upgrade.cypherl --username=system_admin_user --password=admin_password"

# --- Optional routing tests ---
if [[ "$TEST_ROUTING" == "true" ]]; then
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
# Cleanup handled by trap:
# - Always uninstall helm & delete PVCs
# - Only delete StorageClass and Minikube if --debug=true
