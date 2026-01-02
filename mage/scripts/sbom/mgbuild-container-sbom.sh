#!/bin/bash
# This script will build the SBOM within the mgbuild container and copy it back
# to the host machine. Assumes that the container is already running.

set -euo pipefail

CONTAINER_NAME=mgbuild
CONAN_REMOTE=${CONAN_REMOTE:-""}

docker exec -i -u root $CONTAINER_NAME bash -c "chmod -R a+rwX /home/mg/.conan2 2>/dev/null || true"

docker exec -i -u mg $CONTAINER_NAME bash -c "cd /home/mg/mage && export CONAN_REMOTE=$CONAN_REMOTE && ./scripts/sbom/memgraph-sbom.sh"

mkdir -p sbom
docker cp $CONTAINER_NAME:/home/mg/mage/cpp/memgraph/sbom/memgraph-build-sbom.json sbom/
