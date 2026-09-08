#!/bin/bash -e
# Runs the smoke tests against Memgraph deployed on Kubernetes (kind + the
# memgraph helm charts): one single instance and one HA cluster.
#
# This is a MANUAL test -> CI runs ./test_single.bash, which tests a plain
# running Docker image and needs no k8s tooling at all.
#
# Usage: ./test_k8s.bash [memgraph|mage]   (image type, defaults to mage)
# Prerequisites (once per machine): ./k8s/init.bash
# Required env vars: MEMGRAPH_DOCKERHUB_IMAGE, MEMGRAPH_ENTERPRISE_LICENSE,
#                    MEMGRAPH_ORGANIZATION_NAME
# NOTE: SMOKE_ROOT is used instead of SCRIPT_DIR because the sourced scripts
# define their own SCRIPT_DIR.
SMOKE_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

IMAGE_TYPE=${1:-"mage"}
if [[ "$IMAGE_TYPE" != "mage" && "$IMAGE_TYPE" != "memgraph" ]]; then
  echo "Error: Invalid image type '$IMAGE_TYPE'"
  exit 1
fi

# NOTE: Sourced before the tool check because it also puts $(go env GOPATH)/bin
# on PATH, which is where k8s/init.bash installs kind.
source "$SMOKE_ROOT/k8s/run.bash"

for tool in kind kubectl helm; do
  if ! command -v "$tool" > /dev/null 2>&1; then
    echo "Error: '$tool' not found -> run $SMOKE_ROOT/k8s/init.bash first."
    exit 1
  fi
done

if ! kubectl cluster-info --context "kind-$KIND_CLUSTER_NAME" > /dev/null 2>&1; then
  echo "Error: kind cluster '$KIND_CLUSTER_NAME' is not reachable -> run $SMOKE_ROOT/k8s/init.bash first."
  exit 1
fi

# The image has to be present locally because it's loaded into kind.
pull_docker_image Dockerhub

cleanup_k8s_all

# The single instance runs the full feature suite (the same one test_single.bash
# runs against Docker); the HA cluster is checked at the cluster level.
test_k8s_single "$IMAGE_TYPE"
test_k8s_ha -n 1
