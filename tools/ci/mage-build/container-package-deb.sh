#!/bin/bash
set -euo pipefail

ARCH=amd64
BUILD_TYPE=Release
CONTAINER_NAME=mgbuild
VERSION=unknown
MALLOC=false
CUDA=false
while [[ $# -gt 0 ]]; do
  case $1 in
    --arch)
      ARCH=$2
      shift 2
    ;;
    --build-type)
      BUILD_TYPE=$2
      shift 2
    ;;
    --container-name)
      CONTAINER_NAME=$2
      shift 2
    ;;
    --version)
      VERSION=$2
      shift 2
    ;;
    --malloc)
      MALLOC=$2
      shift 2
    ;;
    --cuda)
      CUDA=$2
      shift 2
    ;;
    *)
      echo "Unknown option: $1"
      exit 1
    ;;
  esac
done


docker exec -i -u root $CONTAINER_NAME bash -c "apt-get update && apt-get install -y debhelper"

docker exec -i -u mg $CONTAINER_NAME bash -c "cd /home/mg/memgraph/tools/ci/mage-build/package && ./build-deb.sh $ARCH $BUILD_TYPE $VERSION $MALLOC $CUDA"

mkdir -pv output
for path in $(docker exec -i -u mg $CONTAINER_NAME bash -c "ls /home/mg/memgraph/tools/ci/mage-build/package/memgraph-mage*.deb"); do
  docker cp $CONTAINER_NAME:$path output/
  echo "Package: $path"
done
