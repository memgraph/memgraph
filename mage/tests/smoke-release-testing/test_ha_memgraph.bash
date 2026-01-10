#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

pull_docker_images none none

source $SCRIPT_DIR/k8s/run.bash
cleanup_k8s_all

test_k8s_single

# Test the upgrade scenario. PVCs are preserved between the two runs.
test_k8s_ha LAST -n 1 -c # Skip cleanup because we want to recover from existing PVCs.
test_k8s_ha NEXT -n 2 -s # Skip cluster setup because that should be recovered from PVCs.
