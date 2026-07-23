#!/bin/bash
set -euo pipefail

# Local convenience wrapper: build a memgraph-mage docker image from the
# current checkout, end to end. Spins up an mgbuild container, builds MAGE in
# the unified CMake tree (build-mage => MG_BUILD_MAGE-only build), packages
# the memgraph-mage deb(s) with package.sh, stages the docker context under
# release/package/mage/ and drives package-mage-docker. The base memgraph
# package is downloaded (latest release by default, --memgraph-url to
# override) rather than built.
#
# Examples:
#   ./tools/ci/mage-build/build-docker-image.sh
#   ./tools/ci/mage-build/build-docker-image.sh --image-tag mybranch
#   ./tools/ci/mage-build/build-docker-image.sh \
#     --build-type RelWithDebInfo --package-flavour debug

TOOLCHAIN="v7"
OS="ubuntu-24.04"
if [[ "$(arch)" == "x86_64" ]]; then
  ARCH="amd"
else
  ARCH="arm"
fi
IMAGE_TAG="custom"
MEMGRAPH_URL=""
MEMGRAPH_DEBUGINFO_URL=""
MEMGRAPH_REF="$(git branch --show-current)"
BUILD_TYPE="Release"
CUGRAPH=false
PACKAGE_FLAVOUR="prod"
CONTEXT_DIR="release/package/mage"
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
    --memgraph-debuginfo-url)
      MEMGRAPH_DEBUGINFO_URL=$2
      shift 2
    ;;
    --cugraph)
      CUGRAPH=true
      shift 1
    ;;
    --package-flavour)
      PACKAGE_FLAVOUR=$2
      shift 2
    ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
    ;;
  esac
done

case "$PACKAGE_FLAVOUR" in
  prod) ;;
  debug)
    if [[ "$BUILD_TYPE" != "RelWithDebInfo" ]]; then
      echo "Error: --package-flavour debug requires --build-type RelWithDebInfo (got '$BUILD_TYPE')" >&2
      exit 1
    fi
  ;;
  *)
    echo "Error: --package-flavour must be 'prod' or 'debug' (got '$PACKAGE_FLAVOUR')" >&2
    exit 1
  ;;
esac

# Fetch the base Memgraph package into the docker build context
if [[ -z "$MEMGRAPH_URL" ]]; then
  echo "Warning: using latest Memgraph release"
  VERSION=$(./tools/ci/get_latest_tag.sh)
  OS_PATH="$OS"
  if [[ "$BUILD_TYPE" == "RelWithDebInfo" && "$PACKAGE_FLAVOUR" == "debug" ]]; then
    OS_PATH="${OS_PATH}-relwithdebinfo"
  fi
  MEMGRAPH_URL="https://download.memgraph.com/memgraph/v${VERSION}/${OS_PATH}/memgraph_${VERSION}-1_${ARCH}64.deb"
fi
wget -O $CONTEXT_DIR/memgraph.deb "$MEMGRAPH_URL"

# The debug image flavour additionally installs the memgraph debug symbols;
# derive the debuginfo package URL from the base one unless given explicitly.
if [[ "$PACKAGE_FLAVOUR" == "debug" ]]; then
  if [[ -z "$MEMGRAPH_DEBUGINFO_URL" ]]; then
    MEMGRAPH_DEBUGINFO_URL="${MEMGRAPH_URL/memgraph_/memgraph-debuginfo_}"
  fi
  wget -O $CONTEXT_DIR/memgraph-debuginfo.deb "$MEMGRAPH_DEBUGINFO_URL"
fi

MGBUILD_ARGS=(
  --toolchain $TOOLCHAIN
  --os "$OS"
  --arch $ARCH
  --build-type $BUILD_TYPE
)
if [[ "$CUGRAPH" = true ]]; then
  MGBUILD_ARGS+=("--cugraph" "true")
fi

BUILD_MAGE_ARGS=()
if [[ "$PACKAGE_FLAVOUR" == "debug" ]]; then
  # Split-debug build so package-mage-deb also produces the
  # memgraph-mage-debuginfo deb the debug image installs.
  BUILD_MAGE_ARGS+=("--split-debug")
fi

# Launch mgbuild container, build MAGE and package the mage deb(s)
./release/package/mgbuild.sh \
  ${MGBUILD_ARGS[*]} \
  run

./release/package/mgbuild.sh \
  ${MGBUILD_ARGS[*]} \
  build-memgraph --mage only ${BUILD_MAGE_ARGS[*]:-}

./release/package/mgbuild.sh \
  ${MGBUILD_ARGS[*]} \
  package-mage-deb

# gssapi has no prebuilt linux wheels on PyPI; the Dockerfile's python-base
# stage installs it from the staged wheels dir.
./release/package/mgbuild.sh \
  ${MGBUILD_ARGS[*]} \
  build-gssapi

if [[ "$PACKAGE_FLAVOUR" == "debug" ]]; then
  ./release/package/mgbuild.sh \
    ${MGBUILD_ARGS[*]} \
    build-heaptrack

  ./release/package/mgbuild.sh \
    ${MGBUILD_ARGS[*]} \
    copy-heaptrack \
    --dest-dir "$(pwd)/$CONTEXT_DIR"
fi

./release/package/mgbuild.sh \
  ${MGBUILD_ARGS[*]} \
  stop --remove

mkdir -p $CONTEXT_DIR/openssl
mkdir -p $CONTEXT_DIR/wheels
./release/package/mgbuild.sh \
  ${MGBUILD_ARGS[*]} \
  package-mage-docker \
  --docker-repository-name memgraph/memgraph-mage \
  --image-tag $IMAGE_TAG \
  --memgraph-ref $MEMGRAPH_REF \
  --cache-present false \
  --package-flavour $PACKAGE_FLAVOUR

# Clean the staged context artifacts (all gitignored, but keep the tree tidy)
rm -rf $CONTEXT_DIR/openssl
rm -f $CONTEXT_DIR/memgraph.deb $CONTEXT_DIR/memgraph-debuginfo.deb
rm -f $CONTEXT_DIR/memgraph-mage.deb $CONTEXT_DIR/memgraph-mage-debuginfo.deb
rm -rf $CONTEXT_DIR/heaptrack

GREEN_BOLD='\033[1;32m'
RED_BOLD='\033[1;31m'
RESET='\033[0m'
echo -e "${GREEN_BOLD}MAGE Docker image ${RED_BOLD} memgraph/memgraph-mage:${IMAGE_TAG} ${GREEN_BOLD} built successfully${RESET}"
