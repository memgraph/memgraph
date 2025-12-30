#!/bin/bash

function print_help() {
    echo "Usage: $0 <container_name> [--conan-remote <conan_remote>] [--version <version>]"
    exit 1
}

CONTAINER_NAME=$1
if [ -z "$CONTAINER_NAME" ]; then
    print_help
fi
shift 1
CONAN_REMOTE=""
VERSION="3.5.4"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --conan-remote)
            CONAN_REMOTE=$2
            shift 2
        ;;
        --version)
            VERSION=$2
            shift 2
        ;;
        *)
            print_help
            exit 1
        ;;
    esac
done

# check if memgraph directory exists in the container
exists=$(docker exec -u mg "$CONTAINER_NAME" bash -c "if [ -d /home/mg/memgraph ]; then echo 'true'; else echo 'false'; fi")
if [ "$exists" = "false" ]; then
    echo "Copying memgraph directory to the container"
    docker cp . "$CONTAINER_NAME:/home/mg/memgraph"
    docker exec -u root "$CONTAINER_NAME" bash -c "chown -R mg:mg /home/mg/memgraph"
fi

docker exec -u mg "$CONTAINER_NAME" bash -c "cd /home/mg/memgraph && ./tools/openssl/build.sh --version $VERSION --conan-remote $CONAN_REMOTE"
docker exec -u mg "$CONTAINER_NAME" bash -c "cd /home/mg/memgraph && ./tools/openssl/build-openssl-deb.sh $VERSION"
docker exec -u mg "$CONTAINER_NAME" bash -c "cd /home/mg/memgraph && ./tools/openssl/build-libssl3-deb.sh $VERSION"

ARCH="$(dpkg --print-architecture)"
docker cp "$CONTAINER_NAME:/home/mg/memgraph/build/openssl_${VERSION}-0ubuntu0custom1_${ARCH}.deb" build/openssl_${VERSION}-0ubuntu0custom1_${ARCH}.deb
docker cp "$CONTAINER_NAME:/home/mg/memgraph/build/libssl3t64_${VERSION}-0ubuntu0custom1_${ARCH}.deb" build/libssl3t64_${VERSION}-0ubuntu0custom1_${ARCH}.deb
