#!/bin/bash

CONTAINER_NAME=${1:-mgbuild}

ACTIVATE_TOOLCHAIN="source /opt/toolchain-v7/activate"
rm -rf heaptrack

docker exec -i -u root $CONTAINER_NAME bash -c "apt-get update && apt-get install -y libdw-dev libboost-all-dev"

docker exec -i -u root $CONTAINER_NAME bash -c "mkdir -p /tmp/heaptrack && chown mg:mg /tmp/heaptrack"

docker exec -u mg $CONTAINER_NAME bash -c "$ACTIVATE_TOOLCHAIN && cd \$HOME/mage && ./scripts/build-heaptrack.sh"

mkdir -p heaptrack
docker cp $CONTAINER_NAME:/tmp/heaptrack .
