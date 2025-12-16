#!/bin/bash

function print_help() {
    echo "Usage: $0 <container_name> [--conan-remote <conan_remote>]"
    exit 1
}

CONTAINER_NAME=$1
if [ -z "$CONTAINER_NAME" ]; then
    print_help
fi
shift 1
CONAN_REMOTE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --conan-remote)
            CONAN_REMOTE=$2
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

docker exec -u mg "$CONTAINER_NAME" bash -c "cd /home/mg/memgraph && ./tools/openssl/build.sh ${CONAN_REMOTE}"

# copy required files from the container to the host
mkdir -pv build/openssl
docker cp "$CONTAINER_NAME:/home/mg/memgraph/build/openssl/libcrypto.so" build/openssl/
docker cp "$CONTAINER_NAME:/home/mg/memgraph/build/openssl/libcrypto.so.3" build/openssl/
docker cp "$CONTAINER_NAME:/home/mg/memgraph/build/openssl/libssl.so" build/openssl/
docker cp "$CONTAINER_NAME:/home/mg/memgraph/build/openssl/libssl.so.3" build/openssl/
