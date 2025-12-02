#!/bin/bash
set -euo pipefail

# use default build container image
OS="ubuntu-24.04"
TOOLCHAIN="v7"
if [[ "$(arch)" == "aarch64" ]]; then
  ARCH="arm"
else
  ARCH="amd"
fi

build_container="mgbuild_${TOOLCHAIN}_${OS}"
if [[ "$ARCH" == "arm" ]]; then
  build_container="${build_container}-arm"
fi

# check if the --erase or --lru-period options are set
erase=false
LRU_PERIOD="1w"
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --erase)
      erase=true
      shift 1
    ;;
    --lru-period)
      LRU_PERIOD="$2"
      shift 2
    ;;
    *)
      echo "Error: Unknown flag '$1'"
      exit 1
    ;;
  esac
done

function cleanup() {
  docker stop $build_container
  docker rm $build_container || true
  exit $?
}
trap cleanup ERR EXIT

# start container using mgbuild script to set up the conan cache bind mount appropriately
./release/package/mgbuild.sh \
  --toolchain $TOOLCHAIN \
  --os $OS \
  --arch $ARCH \
  run

if [[ "$erase" == "true" ]]; then
    # clear all of the contents of the conan cache
    docker exec -u root $build_container bash -c "rm -rf /home/mg/.conan2/*"
else
    # clean the conan cache using the LRU period
    docker exec -u mg $build_container bash -c "
    cd \$HOME
    python3 -m venv env
    source env/bin/activate
    pip install conan
    conan profile detect --force
    conan remove "*#*" --lru=$LRU_PERIOD -c
    "
fi
echo "Conan cache cleaned"
