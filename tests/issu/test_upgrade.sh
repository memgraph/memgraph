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
CLUSTER_SETUP_TIMEOUT="${CLUSTER_SETUP_TIMEOUT:-90s}"

RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'

# --- save images as tarballs ---
# save each image as tarball in the current directory because
# `minikube load image` is very flaky with loading images directly from Docker.
LAST_TARBALL="memgraph-${LAST_TAG}.tar"
NEXT_TARBALL="memgraph-${NEXT_TAG}.tar"
echo -e "${GREEN}Saving ${LAST_TAG} as ${LAST_TARBALL}"
docker save memgraph/memgraph:${LAST_TAG} -o ${LAST_TARBALL}
echo -e "${GREEN}Saving ${NEXT_TAG} as ${NEXT_TARBALL}"
docker save memgraph/memgraph:${NEXT_TAG} -o ${NEXT_TARBALL}
echo -e "${GREEN}Saved ${LAST_TAG} as ${LAST_TARBALL} and ${NEXT_TAG} as ${NEXT_TARBALL}"

# Defaults for flags/env overrides
TEST_ROUTING=${TEST_ROUTING:-false}
DEBUG=${DEBUG:-false}

# clear the minikube image cache
if [[ "$(arch)" == "aarch64" ]]; then
  ARCH="arm64"
else
  ARCH="amd64"
fi
echo "Clearing minikube image cache for ${ARCH} in ${HOME}/.minikube/cache/images/${ARCH}/memgraph"
rm -rfv "${HOME}/.minikube/cache/images/${ARCH}/memgraph"

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

# -- Handle breaking change with multi-label access control ---
CUTOFF_COMMIT="87ea38a14d3b4dc13edb1c4a0efdacbedc406dd5"
CUTOFF_VERSION="3.7.1"

# echo the commit-ish if present in the tag; empty otherwise
extract_commit_from_tag() {
  local tag="$1"
  # Heuristic: last underscore-separated field looks like a hex SHA (>=7 chars)
  local last_field="${tag##*_}"
  if [[ "$last_field" =~ ^[0-9a-fA-F]{7,}$ ]]; then
    echo "${last_field,,}"   # normalize to lowercase
  else
    echo ""
  fi
}

# echo the leading X.Y[.Z] from the tag (e.g., 3.6.0 from 3.6.0-relwithdebinfo)
extract_version_from_tag() {
  local tag="$1"
  # Grab leading digits and dots; stops at first non [0-9 or .]
  local v
  v="$(sed -E 's/^([0-9]+(\.[0-9]+){1,2}).*/\1/' <<<"$tag")"
  # Basic sanity (must contain at least one dot)
  if [[ "$v" =~ ^[0-9]+(\.[0-9]+){1,2}$ ]]; then
    echo "$v"
  else
    echo ""
  fi
}

# return 0 if $1 < $2 (semantic-ish via sort -V), 1 otherwise
version_lt() {
  local a="$1" b="$2"
  # Normalize to three components (X.Y.Z) so 3.7 == 3.7.0
  norm() {
    IFS='.' read -r x y z <<<"$1"
    : "${y:=0}" ; : "${z:=0}"
    echo "$x.$y.$z"
  }
  a="$(norm "$a")"
  b="$(norm "$b")"
  local first
  first="$(printf '%s\n%s\n' "$a" "$b" | sort -V | head -n1)"
  [[ "$first" == "$a" && "$a" != "$b" ]]
}

version_gte() {
  local a="$1" b="$2"
  ! version_lt "$a" "$b"
}

HA_39_MIGRATION_CUTOFF_VERSION="3.9.0"

requires_ha_39_migration() {
  local from_tag="$1"
  local to_tag="$2"
  local from_v to_v

  from_v="$(extract_version_from_tag "$from_tag")"
  to_v="$(extract_version_from_tag "$to_tag")"

  if [[ -z "$from_v" || -z "$to_v" ]]; then
    return 1
  fi

  if version_lt "$from_v" "$HA_39_MIGRATION_CUTOFF_VERSION" && version_gte "$to_v" "$HA_39_MIGRATION_CUTOFF_VERSION"; then
    return 0
  fi

  return 1
}

# Main entry: echoes "new" or "old"; returns 0 for "new", 1 for "old"
behavior_for_tag() {
  local tag="${1:?docker tag required}"

  local tag_commit
  tag_commit="$(extract_commit_from_tag "$tag")"

  if [[ -n "$tag_commit" ]]; then
    # Use git ancestry: is CUTOFF_COMMIT an ancestor of tag_commit?
    if git merge-base --is-ancestor "$CUTOFF_COMMIT" "$tag_commit"; then
      echo "auth_pre_upgrade.cypherl"   # tag is at/after the bad commit
      return
    else
      echo "auth_pre_upgrade_pre_3.7.1.cypherl"   # tag is before the bad commit (or on another branch)
      return
    fi
  fi

  # No commit in tag—fall back to version compare
  local v
  v="$(extract_version_from_tag "$tag")"
  if [[ -z "$v" ]]; then
    echo "auth_pre_upgrade.cypherl"   # default if we can't parse a version
    return
  fi

  if version_lt "$v" "$CUTOFF_VERSION"; then
    echo "auth_pre_upgrade_pre_3.7.1.cypherl"   # version is before 3.7.1
  else
    echo "auth_pre_upgrade.cypherl"   # 3.7.0 or later
  fi
}

auth_pre_upgrade_file=$(behavior_for_tag "$LAST_TAG")
echo -e "${GREEN}Using auth_pre_upgrade_file: ${auth_pre_upgrade_file}${NC}"
if requires_ha_39_migration "$LAST_TAG" "$NEXT_TAG"; then
  NEED_HA_39_COORDINATOR_RESET=true
  echo -e "${YELLOW}Detected upgrade crossing ${HA_39_MIGRATION_CUTOFF_VERSION}: coordinator data reset + cluster reconnect will be performed.${NC}"
else
  NEED_HA_39_COORDINATOR_RESET=false
fi

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
collect_cluster_logs() {
  if ! command -v kubectl >/dev/null 2>&1; then
    echo "kubectl not found; skipping cluster log collection."
    return 0
  fi

  local ts
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  local log_dir="artifacts/issu-logs-${ts}"
  mkdir -p "${log_dir}/pods"

  echo -e "${YELLOW}Collecting cluster diagnostics into ${log_dir}...${NC}"

  kubectl get nodes -o wide > "${log_dir}/nodes.txt" 2>&1 || true
  kubectl get all -o wide > "${log_dir}/all-resources.txt" 2>&1 || true
  kubectl get pods -o wide > "${log_dir}/pods.txt" 2>&1 || true
  kubectl get pvc,pv > "${log_dir}/storage.txt" 2>&1 || true
  kubectl get events --sort-by=.metadata.creationTimestamp > "${log_dir}/events.txt" 2>&1 || true

  kubectl get job cluster-setup -o yaml > "${log_dir}/cluster-setup-job.yaml" 2>&1 || true
  kubectl describe job cluster-setup > "${log_dir}/cluster-setup-job.describe.txt" 2>&1 || true
  kubectl logs job/cluster-setup --all-containers=true > "${log_dir}/cluster-setup-job.logs.txt" 2>&1 || true

  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    if [[ "$pod" == memgraph-* || "$pod" == cluster-setup* ]]; then
      local pod_dir="${log_dir}/pods/${pod}"
      mkdir -p "${pod_dir}"

      kubectl logs "${pod}" --all-containers=true > "${pod_dir}/containers.log" 2>&1 || true
      kubectl logs "${pod}" --all-containers=true --previous > "${pod_dir}/containers.previous.log" 2>&1 || true

      if [[ "$pod" == memgraph-* ]]; then
        kubectl exec "${pod}" -- bash -c '
          shopt -s nullglob
          files=(/var/log/memgraph/*)
          if [ ${#files[@]} -eq 0 ]; then
            echo "No files in /var/log/memgraph"
            exit 0
          fi
          for f in "${files[@]}"; do
            [ -f "$f" ] || continue
            bn="$(basename "$f")"
            echo "=== ${bn} ==="
            cat "$f"
            echo
          done
        ' > "${pod_dir}/memgraph-files.log" 2>&1 || true
      fi
    fi
  done < <(kubectl get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)

  echo -e "${GREEN}Cluster diagnostics collected in ${log_dir}.${NC}"
}

cleanup() {
  set +e
  echo -e "${YELLOW}Starting cleanup...${NC}"

  collect_cluster_logs || true

  # Always remove generated files and local repo folder
  rm -f old_values.yaml new_values.yaml
  rm -rf helm-charts

  # Always uninstall Helm release and clean PVCs
  echo -e "${YELLOW}Uninstalling Helm release '${RELEASE}'...${NC}"
  helm uninstall "$RELEASE" >/dev/null 2>&1 || true

  echo -e "${YELLOW}Deleting PVCs in the current namespace...${NC}"
  # Avoid blocking forever when PVC protection waits on terminating pods.
  kubectl delete pod -l app.kubernetes.io/instance="$RELEASE" --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
  kubectl delete pvc -l app.kubernetes.io/instance="$RELEASE" --wait=false >/dev/null 2>&1 || true

  # Limit this to PVCs created by the current Helm release to avoid affecting unrelated workloads.
  stuck_pvcs="$(kubectl get pvc -l "app.kubernetes.io/instance=${RELEASE}" -o jsonpath='{range .items[?(@.metadata.deletionTimestamp)]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  if [[ -n "$stuck_pvcs" ]]; then
    while IFS= read -r pvc; do
      [[ -z "$pvc" ]] && continue
      kubectl patch pvc "$pvc" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true
    done <<< "$stuck_pvcs"
  fi

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

  # remove the tar.gz files
  rm -vf ${LAST_TARBALL} ${NEXT_TARBALL} || true

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

ensure_minikube_storage_addons() {
  echo -e "${GREEN}Ensuring Minikube storage addons are enabled...${NC}"
  minikube addons disable storage-provisioner -p "$PROFILE" || true
  minikube addons disable default-storageclass -p "$PROFILE" || true
  minikube addons enable volumesnapshots -p "$PROFILE"
  minikube addons enable csi-hostpath-driver -p "$PROFILE"
}

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
fi

# Always ensure addons even when reusing an existing cluster.
ensure_minikube_storage_addons

# --- Wait for cluster ready ---
echo -e "${GREEN}Waiting for cluster to be ready...${NC}"
kubectl wait --for=condition=ready node --all --timeout=300s


minikube_has_image() {
  local image="$1"
  if minikube image ls | grep -q "${image}"; then
    return 0
  else
    return 1
  fi
}

minikube_image_load_safe() {
  local image="$1"
  local tar_file="$2"
  local attempts=${3:-3}
  for attempt in $(seq 1 $attempts); do
    if minikube -p "$PROFILE" image load "$tar_file"; then
      return 0
    fi
    echo "minikube image load failed (attempt $attempt) — purging cache and retrying..."
    rm -rf "${HOME}/.minikube/cache/images/${ARCH}/$(cut -d/ -f1 <<<"$image")"
  done
  return 1
}

load_into_minikube_if_missing() {
  local image="$1"
  local tar_file="$2"

  if minikube_has_image "${image}"; then
    echo -e "${GREEN}Minikube already has ${image}${NC}"
    return 0
  fi

  # Not in Minikube; try to load from local Docker
  if docker image inspect "${image}" >/dev/null 2>&1; then
    echo -e "${GREEN}Loading ${image} into Minikube...${NC}"
    minikube_image_load_safe "${image}" "${tar_file}"
    if minikube_has_image "${image}"; then
      echo "✅ Loaded ${image} into Minikube."
    else
      echo -e "${YELLOW}⚠️  Attempted to load ${image}, but it's not visible in Minikube yet.${NC}"
    fi
  else
    echo -e "${YELLOW}⚠️  ${image} not found locally and not present in Minikube. It will be pulled when a Pod starts (or pre-pull manually).${NC}"
  fi
}

echo -e "${GREEN}Ensuring images exist in Minikube...${NC}"
load_into_minikube_if_missing "memgraph/memgraph:${LAST_TAG}" "${LAST_TARBALL}"
load_into_minikube_if_missing "memgraph/memgraph:${NEXT_TAG}" "${NEXT_TARBALL}"

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
helm install "$RELEASE" memgraph/memgraph-high-availability -f old_values.yaml --timeout 120s --wait --debug | grep -E "(Happy\ Helming|NAME\: |LAST DEPLOYED\: |NAMESPACE\: |STATUS\: |REVISION\: | TEST SUITE\: )"

# --- Wait & verify resources ---
echo -e "${GREEN}Waiting for resources to be created...${NC}"
sleep 10

echo -e "${GREEN}Checking created resources...${NC}"
kubectl get all -l app.kubernetes.io/instance="$RELEASE" || true
kubectl get pods || true

echo -e "${GREEN}Waiting for pods to become ready...${NC}"
echo "Current pod status:"
kubectl get pods -o wide

echo "Waiting up to 5 minutes for memgraph pods to be ready..."

MEMGRAPH_PODS=(
  memgraph-coordinator-1-0
  memgraph-coordinator-2-0
  memgraph-coordinator-3-0
  memgraph-data-0-0
  memgraph-data-1-0
)

wait_memgraph_pods_ready() {
  local timeout="${1:-300s}"
  for p in "${MEMGRAPH_PODS[@]}"; do
    kubectl wait --for=condition=ready "pod/$p" --timeout="$timeout"
  done
}

run_coordinator_query_with_retry() {
  local query="$1"
  local attempts="${2:-60}"
  local sleep_s="${3:-2}"
  local out
  local i

  for i in $(seq 1 "$attempts"); do
    if out="$(printf '%s\n' "$query" | kubectl exec -i memgraph-coordinator-1-0 -- mgconsole 2>&1)"; then
      [[ -n "$out" ]] && echo "$out"
      return 0
    fi

    echo "Coordinator query failed (attempt $i/$attempts). Retrying in ${sleep_s}s..." >&2
    echo "$out"
    sleep "$sleep_s"
  done

  echo "Failed to execute coordinator query after ${attempts} attempts: $query" >&2
  return 1
}

resolve_main_data_pod() {
  local show_instances_out
  local main_instance

  show_instances_out="$(run_coordinator_query_with_retry 'SHOW INSTANCES;' 20 2)"
  main_instance="$(
    awk -F'|' '
      /"instance_[0-9]+"/ && /"main"/ {
        gsub(/"/, "", $2)
        gsub(/^[ \t]+|[ \t]+$/, "", $2)
        print $2
        exit
      }
    ' <<<"$show_instances_out"
  )"

  case "$main_instance" in
    instance_1) echo "memgraph-data-0-0" ;;
    instance_2) echo "memgraph-data-1-0" ;;
    *)
      if [[ "$NEED_HA_39_COORDINATOR_RESET" == "true" ]]; then
        echo "Could not resolve current main instance; defaulting to memgraph-data-0-0 after 3.9 reconnect." >&2
        echo "memgraph-data-0-0"
      else
        echo "Could not resolve current main instance; defaulting to memgraph-data-1-0." >&2
        echo "memgraph-data-1-0"
      fi
      ;;
  esac
}

wait_memgraph_pods_ready 300s

echo "All pods became ready"
run_coordinator_query_with_retry 'SHOW INSTANCES;'

kubectl wait --for=condition=complete job/cluster-setup --timeout=300s 2>/dev/null || true

# --- Pre-upgrade setup ---
run_coordinator_query_with_retry 'SHOW INSTANCES;'
echo "Initialized cluster"

echo "Waiting for cluster to become writable (MAIN elected)..."
for i in $(seq 1 90); do
  out="$(run_coordinator_query_with_retry 'SHOW INSTANCES;' 1 0 2>/dev/null || true)"

  if echo "$out" | grep -q '"coordinator_1"' \
    && echo "$out" | grep -q '"coordinator_2"' \
    && echo "$out" | grep -q '"coordinator_3"' \
    && echo "$out" | grep -qi '"main"' \
    && echo "$out" | grep -qi '"replica"'; then
      echo "Cluster is initialized and writable."
      break
  fi

  echo "Cluster not writable yet (attempt $i/90). Retrying in 2s..."
  sleep 2
done

run_coordinator_query_with_retry 'SHOW INSTANCES;'


kubectl cp $auth_pre_upgrade_file memgraph-data-0-0:/var/lib/memgraph/auth_pre_upgrade.cypherl
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
wait_memgraph_pods_ready 90s
echo "Upgrade of pod memgraph-data-1-0 passed successfully"

echo "Deleting pod memgraph-data-0-0 which serves as main"
kubectl scale statefulset memgraph-data-0 --replicas=0
sleep 5
kubectl scale statefulset memgraph-data-0 --replicas=1
wait_memgraph_pods_ready 90s
echo "Upgrade of pod memgraph-data-0-0 passed successfully"

if [[ "$NEED_HA_39_COORDINATOR_RESET" == "true" ]]; then
  echo "Running coordinator reset flow required by the 3.9 HA migration"
  COORDINATOR_PODS=(memgraph-coordinator-1-0 memgraph-coordinator-2-0 memgraph-coordinator-3-0)
  COORDINATOR_STS=(memgraph-coordinator-1 memgraph-coordinator-2 memgraph-coordinator-3)
  COORDINATOR_PVCS=()

  echo "Collecting coordinator PVCs before scaling down"
  for coord_pod in "${COORDINATOR_PODS[@]}"; do
    while IFS= read -r pvc; do
      [[ -z "$pvc" ]] && continue
      COORDINATOR_PVCS+=("$pvc")
    done < <(kubectl get pod "${coord_pod}" -o jsonpath='{range .spec.volumes[*]}{.persistentVolumeClaim.claimName}{"\n"}{end}' 2>/dev/null || true)
  done

  if [[ "${#COORDINATOR_PVCS[@]}" -gt 0 ]]; then
    mapfile -t COORDINATOR_PVCS < <(printf '%s\n' "${COORDINATOR_PVCS[@]}" | awk 'NF' | sort -u)
  fi

  for coord_sts in "${COORDINATOR_STS[@]}"; do
    echo "Scaling down ${coord_sts} to 0"
    kubectl scale statefulset "${coord_sts}" --replicas=0
  done

  for coord_pod in "${COORDINATOR_PODS[@]}"; do
    kubectl wait --for=delete "pod/${coord_pod}" --timeout=180s || true
  done

  if [[ "${#COORDINATOR_PVCS[@]}" -eq 0 ]]; then
    echo "No coordinator PVCs detected for deletion."
  else
    echo "Deleting coordinator PVCs: ${COORDINATOR_PVCS[*]}"
    for pvc in "${COORDINATOR_PVCS[@]}"; do
      kubectl delete pvc "${pvc}" --wait=true
    done
  fi

  for coord_sts in "${COORDINATOR_STS[@]}"; do
    echo "Scaling up ${coord_sts} to 1"
    kubectl scale statefulset "${coord_sts}" --replicas=1
  done

  wait_memgraph_pods_ready 180s
  echo "Coordinator StatefulSets restarted with clean data directories"

  echo "Reconnecting cluster after coordinator reset"
  run_coordinator_query_with_retry 'REGISTER INSTANCE instance_1 WITH CONFIG {"bolt_server": "memgraph-data-0.default.svc.cluster.local:7687", "management_server": "memgraph-data-0.default.svc.cluster.local:10000", "replication_server": "memgraph-data-0.default.svc.cluster.local:20000"};'
  run_coordinator_query_with_retry 'REGISTER INSTANCE instance_2 WITH CONFIG {"bolt_server": "memgraph-data-1.default.svc.cluster.local:7687", "management_server": "memgraph-data-1.default.svc.cluster.local:10000", "replication_server": "memgraph-data-1.default.svc.cluster.local:20000"};'
  run_coordinator_query_with_retry 'ADD COORDINATOR 1 WITH CONFIG {"bolt_server": "memgraph-coordinator-1.default.svc.cluster.local:7687", "coordinator_server": "memgraph-coordinator-1.default.svc.cluster.local:12000", "management_server": "memgraph-coordinator-1.default.svc.cluster.local:10000"};'
  run_coordinator_query_with_retry 'ADD COORDINATOR 2 WITH CONFIG {"bolt_server": "memgraph-coordinator-2.default.svc.cluster.local:7687", "coordinator_server": "memgraph-coordinator-2.default.svc.cluster.local:12000", "management_server": "memgraph-coordinator-2.default.svc.cluster.local:10000"};'
  run_coordinator_query_with_retry 'ADD COORDINATOR 3 WITH CONFIG {"bolt_server": "memgraph-coordinator-3.default.svc.cluster.local:7687", "coordinator_server": "memgraph-coordinator-3.default.svc.cluster.local:12000", "management_server": "memgraph-coordinator-3.default.svc.cluster.local:10000"};'
  run_coordinator_query_with_retry 'SET INSTANCE instance_1 TO MAIN;'
else
  echo "Deleting pod memgraph-coordinator-3-0 which serves as follower"
  kubectl delete pod memgraph-coordinator-3-0
  wait_memgraph_pods_ready 90s
  echo "Upgrade of pod memgraph-coordinator-3-0 passed successfully"

  echo "Deleting pod memgraph-coordinator-2-0 which serves as follower"
  kubectl delete pod memgraph-coordinator-2-0
  wait_memgraph_pods_ready 90s
  echo "Upgrade of pod memgraph-coordinator-2-0 passed successfully"

  echo "Deleting pod memgraph-coordinator-1-0 which serves as leader"
  kubectl delete pod memgraph-coordinator-1-0
  wait_memgraph_pods_ready 90s
  echo "Upgrade of pod memgraph-coordinator-1-0 passed successfully"
fi

run_coordinator_query_with_retry 'SHOW INSTANCES;'

# --- Post-upgrade verification ---
POST_UPGRADE_TARGET_POD="$(resolve_main_data_pod)"
echo -e "${GREEN}Running post-upgrade tests on main data pod: ${POST_UPGRADE_TARGET_POD}${NC}"
kubectl cp post_upgrade_mg.cypherl  "${POST_UPGRADE_TARGET_POD}:/var/lib/memgraph/post_upgrade_mg.cypherl"
kubectl cp post_upgrade_db1.cypherl "${POST_UPGRADE_TARGET_POD}:/var/lib/memgraph/post_upgrade_db1.cypherl"
echo "Running post-upgrade tests on database 'memgraph'"
kubectl exec "${POST_UPGRADE_TARGET_POD}" -- bash -c "mgconsole < /var/lib/memgraph/post_upgrade_mg.cypherl  --username=system_admin_user --password=admin_password"
echo "Running post-upgrade tests on database 'db1'"
kubectl exec "${POST_UPGRADE_TARGET_POD}" -- bash -c "mgconsole < /var/lib/memgraph/post_upgrade_db1.cypherl --username=tenant1_admin_user --password=t1_admin_pass"

echo "Running auth post-upgrade tests"
kubectl cp auth_post_upgrade.cypherl "${POST_UPGRADE_TARGET_POD}:/var/lib/memgraph/auth_post_upgrade.cypherl"
kubectl exec "${POST_UPGRADE_TARGET_POD}" -- bash -c "mgconsole < /var/lib/memgraph/auth_post_upgrade.cypherl --username=system_admin_user --password=admin_password"

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
