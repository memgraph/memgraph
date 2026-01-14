#!/bin/bash


TOOLCHAIN="v7"
OS="ubuntu-24.04"
if [[ "$(arch)" == "x86_64" ]]; then
  ARCH="amd"
else
  ARCH="arm"
fi
IMAGE_TAG="custom"
MEMGRAPH_URL=""
MEMGRAPH_REF="$(git branch --show-current)"
BUILD_TYPE="Release"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-type)
      BUILD_TYPE=$2
      shift 2
    ;;
    --image-tag)
      IMAGE_TAG=$2
      shift 2
    ;;
    --memgraph-url)
      MEMGRAPH_URL=$2
      shift 2
    ;;
    *)
      echo "Unknown option: $1"
      exit 1
    ;;
  esac
done

# Fetch Memgraph package
if [[ -z "$MEMGRAPH_URL" ]]; then
  echo "Warning using latest Memgraph release"
  VERSION=$(./tools/ci/get_latest_tag.sh)
  OS_PATH="$OS"
  if [[ "$BUILD_TYPE" == "RelWithDebInfo" ]]; then
    OS_PATH="${OS_PATH}-relwithdebinfo"
  fi
  MEMGRAPH_URL="https://download.memgraph.com/memgraph/v${VERSION}/${OS_PATH}/memgraph_${VERSION}-1_${ARCH}64.deb"
fi
wget -O mage/memgraph.deb "$MEMGRAPH_URL"

# Launch mgbuild container to build MAGE
./release/package/mgbuild.sh \
  --toolchain $TOOLCHAIN \
  --os "$OS" \
  --arch $ARCH \
  run

./release/package/mgbuild.sh \
  --toolchain $TOOLCHAIN \
  --os "$OS" \
  --arch $ARCH \
  --build-type $BUILD_TYPE \
  build-mage

if [[ "$BUILD_TYPE" == "RelWithDebInfo" ]]; then
  ./release/package/mgbuild.sh \
    --toolchain $TOOLCHAIN \
    --os "$OS" \
    --arch $ARCH \
    build-heaptrack

  ./release/package/mgbuild.sh \
    --toolchain $TOOLCHAIN \
    --os "$OS" \
    --arch $ARCH \
    copy-heaptrack \
    --dest-dir "$(pwd)/mage"
fi

./release/package/mgbuild.sh \
  --toolchain $TOOLCHAIN \
  --os "$OS" \
  --arch $ARCH \
  stop --remove

mkdir mage/openssl
./release/package/mgbuild.sh \
  --toolchain $TOOLCHAIN \
  --os "$OS" \
  --arch $ARCH \
  --build-type $BUILD_TYPE \
  package-mage-docker \
  --docker-repository-name memgraph/memgraph-mage \
  --image-tag $IMAGE_TAG \
  --memgraph-ref $MEMGRAPH_REF \
  --cache-present false
rm -rf mage/openssl
rm -rf mage/memgraph.deb
rm -rf mage/mage.tar.gz

GREEN_BOLD='\033[1;32m'
RED_BOLD='\033[1;31m'
RESET='\033[0m'
echo -e "${GREEN_BOLD}MAGE Docker image ${RED_BOLD} memgraph/memgraph-mage:${IMAGE_TAG} ${GREEN_BOLD} built successfully${RESET}"
