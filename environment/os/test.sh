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
    [centos-10]="quay.io/centos/centos:stream10"
    [debian-12]="debian:12"
    [debian-12-arm]="debian:12"
    [debian-13]="debian:13"
    [debian-13-arm]="debian:13"
    [fedora-42]="fedora:42"
    [fedora-42-arm]="fedora:42"
    [fedora-43]="fedora:43"
    [fedora-43-arm]="fedora:43"
    [fedora-44]="fedora:44"
    [fedora-44-arm]="fedora:44"
    [fedora-45]="fedora:45"
    [fedora-45-arm]="fedora:45"
    [rocky-10]="rockylinux/rockylinux:10"
    [ubuntu-22.04]="ubuntu:22.04"
    [ubuntu-22.04-arm]="ubuntu:22.04"
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

# "ALL" tests every package group, in the PACKAGE_GROUPS order above, inside
# one container.
if [[ "${PACKAGE_GROUP}" == "ALL" ]]; then
    GROUPS_TO_TEST=("${PACKAGE_GROUPS[@]}")
elif [[ " ${PACKAGE_GROUPS[*]} " =~ " ${PACKAGE_GROUP} " ]]; then
    GROUPS_TO_TEST=("${PACKAGE_GROUP}")
else
    echo "Error: Invalid package group: ${PACKAGE_GROUP} (valid: ${PACKAGE_GROUPS[*]} or ALL)"
    exit 1
fi

cleanup() {
    status=$?
    echo -e "${YELLOW}Stopping and removing docker container ${CONTAINER_NAME}...${RESET}"
    docker stop ${CONTAINER_NAME} || true
    exit $status
}

trap cleanup EXIT ERR



echo -e "${GREEN}Testing ${GROUPS_TO_TEST[*]} for ${OS}...${RESET}"

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

for group in "${GROUPS_TO_TEST[@]}"; do
    echo -e "${YELLOW}Installing ${group} in the container...${RESET}"
    if ! docker exec -it ${CONTAINER_NAME} bash -c "cd /memgraph && ./environment/os/install_deps.sh install ${group}"; then
        echo -e "${RED}Failed to install ${group} in the container...${RESET}"
        exit 1
    fi
    echo -e "${GREEN}${group} installed successfully...${RESET}"
done

echo -e "${GREEN}Testing ${GROUPS_TO_TEST[*]} for ${OS} COMPLETED SUCCESSFULLY...${RESET}"
