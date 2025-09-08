#!/usr/bin/env bash
set -Eeuo pipefail

# --- config you already use ---
SC_NAME="csi-hostpath-delayed"
RELEASE="memgraph-db"

# --- colors (optional) ---
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'

# --- cleanup that always runs exactly once ---
_CLEANED=false
cleanup() {
   # ignore any failures during cleanup
  set +e
  helm uninstall "$RELEASE" >/dev/null 2>&1 || true
  kubectl delete pvc --all >/dev/null 2>&1 || true
  kubectl delete storageclass "$SC_NAME" --ignore-not-found >/dev/null 2>&1 || true

  echo -e "${GREEN}Cleanup finished.${NC}"
}

# Run cleanup on any exit (success, error, SIGINT/SIGTERM)
trap cleanup EXIT
trap 'echo -e "'"${RED}"'"Error on line $LINENO. Exiting…'"${NC}"'" >&2' ERR
trap 'echo -e "'"${RED}"'SIGINT received. Exiting…'"${NC}"' >&2' INT
trap 'echo -e "'"${RED}"'SIGTERM received. Exiting…'"${NC}"' >&2' TERM

# --- your script body below (unchanged) ---
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

# Install Helm chart
helm repo update
helm install "$RELEASE" ~/Memgraph/code/helm-charts/charts/memgraph-high-availability -f old_values.yaml

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
helm upgrade "$RELEASE" ~/Memgraph/code/helm-charts/charts/memgraph-high-availability -f new_values.yaml
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

echo "Test successfully finished!"
# (No explicit cleanup here—trap will run it)
