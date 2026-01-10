#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PATH="$(go env GOPATH)/bin:$PATH"
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
    "MAIN_INSTANCE=\$(echo \"SHOW INSTANCES;\" | $MEMGRAPH_CONSOLE_BINARY --port 17687 --output-format=csv | python3 $SCRIPT_DIR/../validator.py get_main_parser)" \
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
    "MAIN_INSTANCE=\$(echo \"SHOW INSTANCES;\" | $MEMGRAPH_CONSOLE_BINARY --port 17687 --output-format=csv | python3 $SCRIPT_DIR/../validator.py get_main_parser)" \
    "echo \"NOTE: MAIN instance is \$MAIN_INSTANCE\"" \
    "echo \"MG_MAIN=\$MAIN_INSTANCE\" > $SCRIPT_DIR/mg_main.out" # Couldn't get export to move the info -> used file instead.
  source $SCRIPT_DIR/mg_main.out
  with_kubectl_portforward "$MG_MAIN-0" 17687:7687 'wait_for_memgraph localhost 17687 5' -- \
    "echo \"MATCH (n) RETURN n;\" | $MEMGRAPH_CONSOLE_BINARY --port 17687 --output-format=csv | python3 $SCRIPT_DIR/../validator.py validate_number_of_results -e $expected"
  echo "validate_nodes_against_main PASSED"
}

test_k8s_single() {
  echo "Test k8s single memgraph instance using image: $MEMGRAPH_NEXT_DOCKERHUB_IMAGE"
  kind load docker-image $MEMGRAPH_NEXT_DOCKERHUB_IMAGE -n smoke-release-testing
  MEMGRAPH_NEXT_DOCKERHUB_TAG="${MEMGRAPH_NEXT_DOCKERHUB_IMAGE##*:}"
  helm install memgraph-single-smoke memgraph/memgraph \
    -f "$SCRIPT_DIR/values-single.yaml" \
    --set "image.tag=$MEMGRAPH_NEXT_DOCKERHUB_TAG"
  kubectl wait --for=condition=Ready pod/memgraph-single-smoke-0 --timeout=120s

  with_kubectl_portforward memgraph-single-smoke-0 17687:7687 "wait_for_memgraph localhost 17687 5" -- \
    "echo \"CREATE ();\" | $MEMGRAPH_CONSOLE_BINARY --port 17687" \
    "echo \"MATCH (n) RETURN n;\" | $MEMGRAPH_CONSOLE_BINARY --port 17687"

  helm uninstall memgraph-single-smoke
}

helm_install_myhadb() {
  chart_path="$1"
  image_tag="$2"
  helm install myhadb $chart_path \
    --set env.MEMGRAPH_ENTERPRISE_LICENSE=$MEMGRAPH_ENTERPRISE_LICENSE,env.MEMGRAPH_ORGANIZATION_NAME=$MEMGRAPH_ORGANIZATION_NAME \
    -f "$SCRIPT_DIR/values-ha.yaml" \
    --set "image.tag=$image_tag"
}

test_k8s_help() {
  echo "usage: test_k8s_ha LAST|NEXT [-p|--chart-path PATH]"
  echo "                             [-s|--skip-cluster-setup] [-u|--skip-helm-uninstall] [-c|--skip-cleanup]"
  echo "                             [-n|--expected-nodes-no]"
  echo "                             [-h|--help]"
  exit 1
}

cleanup_k8s_all() {
  # NOTE: An attempt to cleanup any leftovers from kubectl port-forward...
  kill -9 $(pgrep kubectl) || true
  if helm status myhadb > /dev/null 2>&1; then
    helm uninstall myhadb
  fi
  if helm status myhadb > /dev/null 2>&1; then
    helm uninstall memgraph-single-smoke
  fi
  kubectl delete pvc --all
}

test_k8s_ha() {
  if [ "$#" -lt 1 ]; then
    test_k8s_help
  fi
  WHICH="$1"
  WHICH_TMP="MEMGRAPH_${WHICH}_DOCKERHUB_IMAGE"
  WHICH_IMAGE="${!WHICH_TMP}"
  MEMGRAPH_DOCKERHUB_TAG="${WHICH_IMAGE##*:}"
  shift
  CHART_PATH="memgraph/memgraph-high-availability"
  SKIP_CLUSTER_SETUP=false
  SKIP_CLEANUP=false
  SKIP_HELM_UNINSTALL=false
  EXPECTED_NODES_COUNT=1
  while [ "$#" -gt 0 ]; do
    case $1 in
      -p|--chart-path)          CHART_PATH="$2";           shift 2 ;;
      -s|--skip-cluster-setup)  SKIP_CLUSTER_SETUP=true;   shift ;;
      -u|--skip-helm-uninstall) SKIP_HELM_UNINSTALL=true;  shift ;;
      -c|--skip-cleanup)        SKIP_CLEANUP=true;         shift ;;
      -n|--expected-nodes-no)   EXPECTED_NODES_COUNT="$2"; shift 2 ;;
      -h|--help)                test_k8s_help;             ;;
      *)                        shift;                     break ;;
    esac
  done
  echo "Test k8s HA memgraph cluster using image:"
  echo "  * image: $WHICH_IMAGE"
  echo "  * tag: $MEMGRAPH_DOCKERHUB_TAG"
  echo "  * chart: $CHART_PATH"
  echo "  * expected nodes number: $EXPECTED_NODES_COUNT"
  echo "  * skip cluster setup: $SKIP_CLUSTER_SETUP"
  echo "  * skip helm uninstall: $SKIP_HELM_UNINSTALL"
  echo "  * skip cleanup: $SKIP_CLEANUP"

  kind load docker-image $WHICH_IMAGE -n smoke-release-testing
  helm_install_myhadb $CHART_PATH $MEMGRAPH_DOCKERHUB_TAG
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
  # NOTE: Developing workflow: download+load required images and defined MEMGRAPH_NEXT_DOCKERHUB_IMAGE.
  echo "Running $0 directly..."

  # test_k8s_single
  # test_k8s_ha LAST -c # Skip cleanup because we want to recover from existing PVCs.
  # test_k8s_ha NEXT -s # Skip cluster setup because that should be recovered from PVCs.

  # How to inject local version of the helm chart because we want to test any local fixes upfront.
  # test_k8s_ha NEXT ~/Workspace/code/memgraph/helm-charts/charts/memgraph-high-availability
fi
