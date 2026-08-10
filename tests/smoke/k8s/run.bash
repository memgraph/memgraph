#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"
source "$SMOKE_DIR/suite.bash" # The feature tests shared with the Docker path.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if command -v go > /dev/null 2>&1; then
  export PATH="$(go env GOPATH)/bin:$PATH" # kind might have been installed via `go install`.
fi
# NOTES:
#   * In the custom values file telemetry was disabled and NodePort was set
#   as the serviceType. The values file was copied from
#   https://github.com/memgraph/helm-charts/blob/main/charts/memgraph-high-availability/values.yaml.
#   * It's critical to run `helm repo update` because otherwise you'll inject
#   latest template that might not be compatible.
#   * `helm uninstall` has to be used otherwise on `kubectl delete pod
#   <pod-name>` the pod gets restarted.
#   * `helm uninstall` is not deleting PVCs, there is the `kubectl delete pvc
#   --all`.
#   * It takes some time to delete all PVs, check with `kubectl get pv`.
#   * If you want more details or helm dry run just append `--debug` of
#   `--dry-run`.
BOLT_SERVER="localhost:10000" # Just tmp value -> each coordinator should have a different value.
# E.g. if kubectl port-foward is used, the configured host values should be passed as `bolt_server`.
CHART_VERSION="1.0.1"

write_license_values() {
  # NOTE: A values overlay is used instead of `--set` because a license string
  # can contain characters that `--set` would interpret (commas, dots, ...).
  __out="$1"
  cat > "$__out" <<EOF
memgraphEnterpriseLicense: "$MEMGRAPH_ENTERPRISE_LICENSE"
memgraphOrganizationName: "$MEMGRAPH_ORGANIZATION_NAME"
EOF
}

setup_coordinator() {
  local i=$1
  echo "ADD COORDINATOR $i WITH CONFIG {\"bolt_server\": \"$BOLT_SERVER\", \"management_server\":  \"memgraph-coordinator-$i.default.svc.cluster.local:10000\", \"coordinator_server\":  \"memgraph-coordinator-$i.default.svc.cluster.local:12000\"};" | $MEMGRAPH_CONSOLE_BINARY --port 17687
  echo "coordinator $i DONE"
}
setup_replica() {
  local i=$1
  echo "REGISTER INSTANCE instance_$i WITH CONFIG {\"bolt_server\": \"$BOLT_SERVER\", \"management_server\": \"memgraph-data-$i.default.svc.cluster.local:10000\", \"replication_server\": \"memgraph-data-$i.default.svc.cluster.local:20000\"};" | $MEMGRAPH_CONSOLE_BINARY --port 17687
  echo "replica $i DONE"
}
setup_main() {
  local i=$1
  echo "SET INSTANCE instance_$i TO MAIN;" | $MEMGRAPH_CONSOLE_BINARY --port 17687
  echo "main DONE"
}
setup_cluster() {
  with_kubectl_portforward memgraph-coordinator-1-0 17687:7687 'wait_for_memgraph_coordinator localhost 17687 5' -- \
    'setup_coordinator 1' \
    'setup_coordinator 2' \
    'setup_coordinator 3' \
    'setup_replica 0' \
    'setup_main 0'
}

execute_query_against_main() {
  query="$1"
  with_kubectl_portforward memgraph-coordinator-1-0 17687:7687 'wait_for_memgraph_coordinator localhost 17687 5' -- \
    "MAIN_INSTANCE=\$(echo \"SHOW INSTANCES;\" | $MEMGRAPH_CONSOLE_BINARY --port 17687 --output-format=csv | python3 $SMOKE_DIR/validator.py get_main_parser)" \
    "echo \"NOTE: MAIN instance is \$MAIN_INSTANCE\"" \
    "echo \"MG_MAIN=\$MAIN_INSTANCE\" > $SCRIPT_DIR/mg_main.out" # Couldn't get export to move the info -> used file instead.
  source $SCRIPT_DIR/mg_main.out
  # NOTE: Waiting for MAIN is required because sometimes all instances are up, but MAIN is not yet fully configured.
  with_kubectl_portforward "$MG_MAIN-0" 17687:7687 'wait_for_memgraph localhost 17687 5' -- \
    "wait_for_memgraph_main localhost 17687 10" \
    "echo \"$query\" | $MEMGRAPH_CONSOLE_BINARY --port 17687"
}

validate_nodes_against_main() {
  expected=$1
  with_kubectl_portforward memgraph-coordinator-1-0 17687:7687 'wait_for_memgraph_coordinator localhost 17687 5' -- \
    "MAIN_INSTANCE=\$(echo \"SHOW INSTANCES;\" | $MEMGRAPH_CONSOLE_BINARY --port 17687 --output-format=csv | python3 $SMOKE_DIR/validator.py get_main_parser)" \
    "echo \"NOTE: MAIN instance is \$MAIN_INSTANCE\"" \
    "echo \"MG_MAIN=\$MAIN_INSTANCE\" > $SCRIPT_DIR/mg_main.out" # Couldn't get export to move the info -> used file instead.
  source $SCRIPT_DIR/mg_main.out
  with_kubectl_portforward "$MG_MAIN-0" 17687:7687 'wait_for_memgraph localhost 17687 5' -- \
    "echo \"MATCH (n) RETURN n;\" | $MEMGRAPH_CONSOLE_BINARY --port 17687 --output-format=csv | python3 $SMOKE_DIR/validator.py validate_number_of_results -e $expected"
  echo "validate_nodes_against_main PASSED"
}

test_k8s_single() {
  __image_type="${1:-mage}"
  echo "Test k8s single memgraph instance using image: $MEMGRAPH_DOCKERHUB_IMAGE"
  kind_load_image "$MEMGRAPH_DOCKERHUB_IMAGE"
  MEMGRAPH_DOCKERHUB_TAG="${MEMGRAPH_DOCKERHUB_IMAGE##*:}"
  MEMGRAPH_DOCKERHUB_REPO="${MEMGRAPH_DOCKERHUB_IMAGE%:*}"
  __license_values="$(mktemp)"
  write_license_values "$__license_values"
  # NOTE: The repository is overridden too because the values file points at
  # memgraph/memgraph -> the tag alone is not enough for e.g. a mage image.
  helm install memgraph-single-smoke memgraph/memgraph \
    --version $CHART_VERSION \
    -f "$SCRIPT_DIR/values-single.yaml" \
    -f "$__license_values" \
    --set "image.repository=$MEMGRAPH_DOCKERHUB_REPO" \
    --set "image.tag=$MEMGRAPH_DOCKERHUB_TAG"
  rm -f "$__license_values"
  kubectl wait --for=condition=Ready pod/memgraph-single-smoke-0 --timeout=120s

  # NOTE: Bolt and monitoring are forwarded to exactly the ports the feature
  # tests use against Docker -> the tests don't need to know they are talking
  # to a pod. Same for MEMGRAPH_EXEC, which test_mgconsole uses.
  MEMGRAPH_EXEC="kubectl exec memgraph-single-smoke-0 --"
  with_kubectl_portforward memgraph-single-smoke-0 \
    "$MEMGRAPH_BOLT_PORT:7687 $MEMGRAPH_MONITORING_PORT:9091" \
    "wait_for_memgraph localhost $MEMGRAPH_BOLT_PORT 5" -- \
    "run_feature_tests $__image_type k8s" \
    "create_test_users" \
    "run_auth_feature_tests"

  helm uninstall memgraph-single-smoke
}

helm_install_myhadb() {
  chart_path="$1"
  image_tag="$2"
  image_repository="$3"
  # NOTE: --dry-run|apply instead of a plain create so that a leftover secret
  # from a previous run doesn't fail the whole test.
  kubectl create secret generic memgraph-secrets \
    --from-literal=MEMGRAPH_ENTERPRISE_LICENSE=$MEMGRAPH_ENTERPRISE_LICENSE \
    --from-literal=MEMGRAPH_ORGANIZATION_NAME=$MEMGRAPH_ORGANIZATION_NAME \
    --dry-run=client -o yaml | kubectl apply -f -

  # Pin chart version when installing from the helm repo; --version is rejected for local chart paths.
  local version_arg=""
  if [ ! -d "$chart_path" ]; then
    version_arg="--version $CHART_VERSION"
  fi

  helm install myhadb $chart_path \
    $version_arg \
    -f "$SCRIPT_DIR/values-ha.yaml" \
    --set "image.repository=$image_repository" \
    --set "image.tag=$image_tag"
}

test_k8s_help() {
  echo "usage: test_k8s_ha [-p|--chart-path PATH]"
  echo "                   [-S|--run-cluster-setup] [-u|--skip-helm-uninstall] [-c|--skip-cleanup]"
  echo "                   [-n|--expected-nodes-no]"
  echo "                   [-h|--help]"
  echo "NOTE: The tested image is always \$MEMGRAPH_DOCKERHUB_IMAGE."
  exit 1
}

cleanup_k8s_all() {
  # NOTE: An attempt to cleanup any leftovers from kubectl port-forward...
  pkill -9 kubectl || true # NOTE: kill -9 $(pgrep ...) errors out when there is no match.
  if helm status myhadb > /dev/null 2>&1; then
    helm uninstall myhadb
  fi
  if helm status memgraph-single-smoke > /dev/null 2>&1; then
    helm uninstall memgraph-single-smoke
  fi
  # NOTE: `helm uninstall` leaves the chart's cluster-setup hook Job behind when
  # it never succeeded -> it keeps spawning retry pods forever.
  kubectl delete job cluster-setup --ignore-not-found
  kubectl delete pvc --all
  kubectl delete secret memgraph-secrets --ignore-not-found
}

test_k8s_ha() {
  MEMGRAPH_DOCKERHUB_TAG="${MEMGRAPH_DOCKERHUB_IMAGE##*:}"
  MEMGRAPH_DOCKERHUB_REPO="${MEMGRAPH_DOCKERHUB_IMAGE%:*}"
  CHART_PATH="memgraph/memgraph-high-availability"
  # NOTE: The chart ships a `cluster-setup` post-install hook Job that does the
  # ADD COORDINATOR / REGISTER INSTANCE / SET MAIN work -> running setup_cluster
  # on top of it fails with "instance with such id already exists". Opt in with
  # --run-cluster-setup when using a chart that does not do it.
  SKIP_CLUSTER_SETUP=true
  SKIP_CLEANUP=false
  SKIP_HELM_UNINSTALL=false
  EXPECTED_NODES_COUNT=1
  while [ "$#" -gt 0 ]; do
    case $1 in
      -p|--chart-path)          CHART_PATH="$2";           shift 2 ;;
      -S|--run-cluster-setup)   SKIP_CLUSTER_SETUP=false;  shift ;;
      -u|--skip-helm-uninstall) SKIP_HELM_UNINSTALL=true;  shift ;;
      -c|--skip-cleanup)        SKIP_CLEANUP=true;         shift ;;
      -n|--expected-nodes-no)   EXPECTED_NODES_COUNT="$2"; shift 2 ;;
      -h|--help)                test_k8s_help;             ;;
      *)                        shift;                     break ;;
    esac
  done
  echo "Test k8s HA memgraph cluster using image:"
  echo "  * image: $MEMGRAPH_DOCKERHUB_IMAGE"
  echo "  * tag: $MEMGRAPH_DOCKERHUB_TAG"
  echo "  * chart: $CHART_PATH"
  echo "  * expected nodes number: $EXPECTED_NODES_COUNT"
  echo "  * skip cluster setup: $SKIP_CLUSTER_SETUP (the chart cluster-setup hook does it)"
  echo "  * skip helm uninstall: $SKIP_HELM_UNINSTALL"
  echo "  * skip cleanup: $SKIP_CLEANUP"

  kind_load_image "$MEMGRAPH_DOCKERHUB_IMAGE"
  helm_install_myhadb $CHART_PATH $MEMGRAPH_DOCKERHUB_TAG $MEMGRAPH_DOCKERHUB_REPO
  sleep 1 # NOTE: Sometimes there is an Error from Server -> pod XYZ not found...
  kubectl wait --for=condition=Ready pod -l role=coordinator --timeout=120s
  kubectl wait --for=condition=Ready pod -l role=data --timeout=120s

  if [ "$SKIP_CLUSTER_SETUP" = false ]; then
    setup_cluster
  fi
  execute_query_against_main "SHOW VERSION;"
  execute_query_against_main "CREATE ();"
  validate_nodes_against_main $EXPECTED_NODES_COUNT
  if [ "$SKIP_HELM_UNINSTALL" = false ]; then
    helm uninstall myhadb
  fi
  if [ "$SKIP_CLEANUP" = false ]; then
    kubectl delete pvc --all
  fi
}

if [ "${BASH_SOURCE[0]}" -ef "$0" ]; then
  # NOTE: Developing workflow: define MEMGRAPH_DOCKERHUB_IMAGE (+ the license
  # variables) and uncomment whichever test you are iterating on. For the full
  # run use tests/smoke/test_k8s.bash.
  echo "Running $0 directly..."

  # test_k8s_single
  # test_k8s_ha -n 1
  # Keep the PVCs around and skip the cluster setup on a second run:
  # test_k8s_ha -n 1 -c
  # test_k8s_ha -n 2

  # How to inject local version of the helm chart because we want to test any local fixes upfront.
  # test_k8s_ha -p ~/Workspace/code/memgraph/helm-charts/charts/memgraph-high-availability
fi
