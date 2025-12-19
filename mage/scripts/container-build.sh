#!/bin/bash
set -euo pipefail

# Color codes
RED_BOLD='\033[1;31m'
GREEN_BOLD='\033[1;32m'
RESET='\033[0m'

ARCH=amd64
BUILD_TYPE=Release
CONTAINER_NAME=mgbuild
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
    *)
      echo "Unknown option: $1"
      exit 1
    ;;
  esac
done

exit_handler() {
    local exit_code=${1:-$?}
    if [[ $exit_code -ne 0 ]]; then
        # in red bold text
        echo -e "${RED_BOLD}Failed to build MAGE${RESET}"
    fi
    exit $exit_code
}

trap 'exit_handler $?' ERR EXIT

# get conan cache directory
CONAN_CACHE_DIR="$HOME/.conan2-ci"

echo -e "${GREEN_BOLD}Using conan cache directory: $CONAN_CACHE_DIR${RESET}"

if [[ "$ARCH" == "arm64" ]]; then
    MGBUILD_IMAGE="memgraph/mgbuild:v7_ubuntu-24.04-arm"
else
    MGBUILD_IMAGE=memgraph/mgbuild:v7_ubuntu-24.04
fi

echo -e "${GREEN_BOLD}Building MAGE - build type: $BUILD_TYPE, arch: $ARCH${RESET}"
echo -e "${GREEN_BOLD}Using base image: $MGBUILD_IMAGE${RESET}"

echo -e "${GREEN_BOLD}Starting container${RESET}"
docker run -d --rm -v $CONAN_CACHE_DIR:/home/mg/.conan2 --name $CONTAINER_NAME $MGBUILD_IMAGE

echo -e "${GREEN_BOLD}Copying repo into container${RESET}"
docker exec -i -u mg $CONTAINER_NAME mkdir -p /home/mg/mage
docker cp . $CONTAINER_NAME:/home/mg/memgraph
docker exec -i -u root $CONTAINER_NAME bash -c "chown -R mg:mg /home/mg/memgraph"

echo -e "${GREEN_BOLD}Building inside container${RESET}"
docker exec -i $CONTAINER_NAME bash -c "cd /home/mg/memgraph/mage && ./scripts/build.sh $BUILD_TYPE"

echo -e "${GREEN_BOLD}Compressing query modules${RESET}"
docker exec -i $CONTAINER_NAME bash -c "cd /home/mg/memgraph/mage && ./scripts/compress.sh"

echo -e "${GREEN_BOLD}Copying compressed query modules${RESET}"
docker cp $CONTAINER_NAME:/home/mg/mage.tar.gz .
