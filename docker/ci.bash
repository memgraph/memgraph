#!/usr/bin/env bash
set -eo pipefail

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
BUILD_DIR=$DIR/../build

OS=${OS:-debian-10}
TOOLCHAIN_VERSION=${TOOLCHAIN_VERSION:-4}
THREADS=${THREADS:-$(nproc)}
TAG=${TAG:-memgraph/mgbuilder:12345}
DOCKERFILE_PATH=${DOCKERFILE_PATH:-$DIR/mgbuilder/Dockerfile}
DOCKER_CONTEXT_PATH=${DOCKER_CONTEXT_PATH:-$DIR/..}
TOOLCHAIN_ACTIVATE=${TOOLCHAIN_ACTIVATE:-"source /opt/toolchain-v${TOOLCHAIN_VERSION}/activate"}

print_help() {
  echo -e "$0 build BUILD_TYPE | test TEST_NAME"
  exit 1
}

build() {
  case "$1" in
    community)
      cmake_cmd="cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DMG_ENTERPRISE=OFF .."
      make_cmd="make -j$THREADS"
    ;;
    debug)
      cmake_cmd="cmake -DCMAKE_BUILD_TYPE=Debug .."
      make_cmd="make -j$THREADS"
    ;;
    jepsen)
      cmake_cmd="cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .."
      make_cmd="make -j$THREADS memgraph"
    ;;
    release)
      cmake_cmd="cmake -DCMAKE_BUILD_TYPE=Release .."
      make_cmd="make -j$THREADS"
    ;;
    experimental_mt)
      cmake_cmd="cmake -DCMAKE_BUILD_TYPE=Release -DMG_EXPERIMENTAL_REPLICATION_MULTITENANCY=ON .."
      make_cmd="make -j$THREADS"
    ;;
    experimental_ha)
      cmake_cmd="cmake -DCMAKE_BUILD_TYPE=Release -DMG_EXPERIMENTAL_HIGH_AVAILABILITY=ON .."
      make_cmd="make -j$THREADS"
    ;;
    *) 
      print_help
    ;;
  esac

  docker buildx build \
  -f $DOCKERFILE_PATH \
  -t $TAG \
  --build-arg CMAKE_CMD="$cmake_cmd" \
  --build-arg MAKE_CMD="$make_cmd" \
  --build-arg OS=$OS \
  --build-arg THREADS=$THREADS \
  --build-arg TOOLCHAIN_VERSION=$TOOLCHAIN_VERSION \
  $DOCKER_CONTEXT_PATH

}

unit_tests() {
  cd $BUILD_DIR
  ctest -R memgraph__unit --output-on-failure -j$THREADS
}

run_test() {
  case "$1" in
    unit) 
      unit_tests
    ;;
    2|3) echo 2 or 3
    ;;
    *) echo default
    ;;
  esac
}

if [[ "$#" -eq 0 ]]; then
  print_help
else
  case "$1" in
    build)
      build $2
    ;;
    test)
      $TOOLCHAIN_ACTIVATE
      run_test $2
    ;;
    *)
      print_help
    ;;
  esac
fi
