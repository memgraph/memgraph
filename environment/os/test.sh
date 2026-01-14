#!/bin/bash

set -euo pipefail

GREEN="\033[32m"
RED="\033[31m"
YELLOW="\033[33m"
RESET="\033[0m"

# bash script to test the installation of the dependencies
OS="$1"
if [[ "$(arch)" == "aarch64" ]]; then
  OS="$OS-arm"
fi

PACKAGE_GROUP="$2"

THIS_BRANCH="$(git branch --show-current)"
GIT_REF="${3:-${THIS_BRANCH}}"
echo -e "${YELLOW}Using git reference: ${GIT_REF}...${RESET}"

# OS and docker image map
declare -A OS_DOCKER_IMAGE_MAP=(
    [centos-9]="quay.io/centos/centos:stream9"
    [centos-10]="dokken/centos-stream-10"
    [debian-12]="debian:12"
    [debian-12-arm]="debian:12"
    [debian-13]="debian:trixie"
    [debian-13-arm]="debian:trixie"
    [fedora-42]="fedora:42"
    [fedora-42-arm]="fedora:42"
    [fedora-43]="fedora:43"
    [fedora-43-arm]="fedora:43"
    [rocky-10]="rockylinux/rockylinux:10"
    [ubuntu-22.04]="ubuntu:22.04"
    [ubuntu-24.04]="ubuntu:24.04"
    [ubuntu-24.04-arm]="ubuntu:24.04"
    [ubuntu-26.04]="ubuntu:26.04"
    [ubuntu-26.04-arm]="ubuntu:26.04"
)

if [[ ! -v OS_DOCKER_IMAGE_MAP[$OS] ]]; then
    echo "Error: Invalid OS: ${OS}"
    exit 1
fi
DOCKER_IMAGE=${OS_DOCKER_IMAGE_MAP[$OS]}
CONTAINER_NAME=${OS}_test

# list of package groups allowed
PACKAGE_GROUPS=(
    "TOOLCHAIN_RUN_DEPS"
    "TOOLCHAIN_BUILD_DEPS"
    "MEMGRAPH_BUILD_DEPS"
    "MEMGRAPH_TEST_DEPS"
    "MEMGRAPH_RUN_DEPS"
)

if [[ ! " ${PACKAGE_GROUPS[@]} " =~ " ${PACKAGE_GROUP} " ]]; then
    echo "Error: Invalid package group: ${PACKAGE_GROUP}"
    exit 1
fi

cleanup() {
    status=$?
    echo -e "${YELLOW}Stopping and removing docker container ${CONTAINER_NAME}...${RESET}"
    docker stop ${CONTAINER_NAME} || true
    exit $status
}

trap cleanup EXIT ERR



echo -e "${GREEN}Testing ${PACKAGE_GROUP} for ${OS}...${RESET}"

echo -e "${YELLOW}Pulling docker image ${DOCKER_IMAGE}...${RESET}"
docker pull ${DOCKER_IMAGE}

echo -e "${YELLOW}Running docker container ${DOCKER_IMAGE}...${RESET}"
docker run --rm -d --name ${CONTAINER_NAME} ${DOCKER_IMAGE} sleep infinity

echo -e "${YELLOW}Installing python and git in the container...${RESET}"
# for debian/ubuntu based distros use apt
if [[ "${DOCKER_IMAGE}" == *"debian"* || "${DOCKER_IMAGE}" == *"ubuntu"* ]]; then
    docker exec -it ${CONTAINER_NAME} bash -c "export DEBIAN_FRONTEND=noninteractive && apt update -y && apt install -y python3 git"
else
    docker exec -it ${CONTAINER_NAME} bash -c "dnf install -y python3 git"
fi

echo -e "${YELLOW}Cloning memgraph repository...${RESET}"
docker exec -it ${CONTAINER_NAME} bash -c "git clone https://github.com/memgraph/memgraph.git && cd /memgraph && git checkout ${GIT_REF}"

status=$?
if [[ $status -ne 0 ]]; then
    echo -e "${RED}Failed to clone memgraph repository...${RESET}"
    exit $status
fi

echo -e "${YELLOW}Installing dependencies in the container...${RESET}"
docker exec -it ${CONTAINER_NAME} bash -c "cd /memgraph && ./environment/os/install_deps.sh install ${PACKAGE_GROUP}"

status=$?
if [[ $status -ne 0 ]]; then
    echo -e "${RED}Failed to install dependencies in the container...${RESET}"
    exit $status
fi

echo -e "${GREEN}Testing ${PACKAGE_GROUP} for ${OS} COMPLETED SUCCESSFULLY...${RESET}"
