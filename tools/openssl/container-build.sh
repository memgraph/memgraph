#!/bin/bash

CONTAINER_NAME=$1

if [ -z "$CONTAINER_NAME" ]; then
    echo "Usage: $0 <container_name>"
    exit 1
fi

# check if memgraph directory exists in the container
exists=$(docker exec -u mg "$CONTAINER_NAME" bash -c "if [ -d /home/mg/memgraph ]; then echo 'true'; else echo 'false'; fi")
if [ "$exists" = "false" ]; then
    echo "Copying memgraph directory to the container"
    docker cp . "$CONTAINER_NAME:/home/mg/memgraph"
    docker exec -u root "$CONTAINER_NAME" bash -c "chown -R mg:mg /home/mg/memgraph"
fi

docker exec -u mg "$CONTAINER_NAME" bash -c "cd /home/mg/memgraph && ./tools/openssl/build.sh"

# copy required files from the container to the host
mkdir -pv build/openssl
docker cp "$CONTAINER_NAME:/home/mg/memgraph/build/openssl/libcrypto.so" build/openssl/
docker cp "$CONTAINER_NAME:/home/mg/memgraph/build/openssl/libcrypto.so.3" build/openssl/
docker cp "$CONTAINER_NAME:/home/mg/memgraph/build/openssl/libssl.so" build/openssl/
docker cp "$CONTAINER_NAME:/home/mg/memgraph/build/openssl/libssl.so.3" build/openssl/
