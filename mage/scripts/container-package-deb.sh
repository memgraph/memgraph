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

docker exec -i -u mg $CONTAINER_NAME bash -c "cd /home/mg/mage/scripts/package && ./build-deb.sh $ARCH $BUILD_TYPE $VERSION $MALLOC $CUDA"

package_name="$(docker exec -i -u mg $CONTAINER_NAME bash -c "ls /home/mg/mage/scripts/package/memgraph-mage*.deb")"
mkdir -pv output
docker cp $CONTAINER_NAME:$package_name output/
echo "Package: $package_name"
