#!/usr/bin/env bash

set -eo pipefail

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

TARGET_OS=${TARGET_OS:-debian-11}
TOOLCHAIN_VERSION=${TOOLCHAIN_VERSION:-v4}
BUILD_TYPE=${BUILD_TYPE:-RelWithDebInfo}
THREAD_COUNT=${THREAD_COUNT:-24}
BINARY_BUILDER_IMAGE=memgraph/binary-builder:${TOOLCHAIN_VERSION}_${TARGET_OS}
BINARY_BUILDER_CNT=mg-binary-builder-${TOOLCHAIN_VERSION}-${TARGET_OS}

print_help() {
  echo -e "Env vars:"
  echo -e "  TARGET_OS            -> target os for memgraph build"
  echo -e "  TOOLCHAIN_VERSION    -> toolchain version to be used"
  echo -e "  THREAD_COUNT         -> number of threads to be used with make"
  echo -e "  BUILD_TYPE           -> Debug|Release|RelWithDebInfo"
  echo -e ""
  echo -e "How to run?"
  echo -e " $0 [build-builder|run-builder|build-memgraph|copy-binary dest_path|cleanup|help]"
  exit 1
}

if [[ "$#" -gt 2 || "$#" -lt 1 ]]; then
  print_help
fi
case "$1" in
  build-builder)
      cd $DIR/../..
      docker buildx build \
        --pull \
        --tag $BINARY_BUILDER_IMAGE \
        --build-arg OS=${TARGET_OS} \
        --build-arg TOOLCHAIN_VERSION=${TOOLCHAIN_VERSION} \
        --file release/binary/Dockerfile \
        .
  ;;

  build-memgraph)
      cd $DIR/../../build
      source /opt/toolchain-v4/activate
      cmake -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DMG_ENTERPRISE=OFF ..
      make -j$THREAD_COUNT
  ;;

  run-builder)
      docker run \
        --rm \
        -d \
        --name $BINARY_BUILDER_CNT \
        $BINARY_BUILDER_IMAGE
  ;;

  copy-binary)
      if [[ "$#" -ne 2 ]]; then
        print_help
      fi
      docker cp -L ${BINARY_BUILDER_CNT}:/memgraph/build/memgraph $2
  ;;

  cleanup)
      docker stop $BINARY_BUILDER_CNT
      docker rm $BINARY_BUILDER_CNT
  ;;

  *)
      print_help
  ;;
esac
