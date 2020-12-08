#!/bin/bash

set -Eeuo pipefail
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# NOTE: docker and docker-compose have to be installed on the system.

if [ ! -d "$script_dir/jepsen" ]; then
    git clone https://github.com/jepsen-io/jepsen.git -b "0.2.1" "$script_dir/jepsen"
fi

# Initialize testing context by copying source/binary files. Inside CI,
# Memgraph is tested on a single machine cluster based on Docker containers.
# Once these tests will be part of the official Jepsen repo, the majority of
# functionalities inside this script won't be needed because each node clones
# the public repo.
case $1 in
    # Start Jepsen Docker cluster.
    up)
        "$script_dir/jepsen/docker/bin/up" --daemon
    ;;
    # Copy Memgraph Jepsen project files and Memgraph binary if specified.
    copy)
        shift 1
        binary_path=$1
        if [ -L "$binary_path" ]; then
            binary_path=$(readlink "$binary_path")
        fi
        binary_name=$(basename -- "$binary_path")

        # Copy Memgraph build binary.
        docker exec jepsen-n1 mkdir -p /opt/memgraph
        docker exec jepsen-n2 mkdir -p /opt/memgraph
        docker exec jepsen-n3 mkdir -p /opt/memgraph
        docker exec jepsen-n4 mkdir -p /opt/memgraph
        docker exec jepsen-n5 mkdir -p /opt/memgraph
        docker cp "$binary_path" jepsen-n1:/opt/memgraph/"$binary_name"
        docker exec jepsen-n1 bash -c "rm -f /opt/memgraph/memgraph && ln -s /opt/memgraph/$binary_name /opt/memgraph/memgraph"
        docker cp "$binary_path" jepsen-n2:/opt/memgraph/"$binary_name"
        docker exec jepsen-n2 bash -c "rm -f /opt/memgraph/memgraph && ln -s /opt/memgraph/$binary_name /opt/memgraph/memgraph"
        docker cp "$binary_path" jepsen-n3:/opt/memgraph/"$binary_name"
        docker exec jepsen-n3 bash -c "rm -f /opt/memgraph/memgraph && ln -s /opt/memgraph/$binary_name /opt/memgraph/memgraph"
        docker cp "$binary_path" jepsen-n4:/opt/memgraph/"$binary_name"
        docker exec jepsen-n4 bash -c "rm -f /opt/memgraph/memgraph && ln -s /opt/memgraph/$binary_name /opt/memgraph/memgraph"
        docker cp "$binary_path" jepsen-n5:/opt/memgraph/"$binary_name"
        docker exec jepsen-n5 bash -c "rm -f /opt/memgraph/memgraph && ln -s /opt/memgraph/$binary_name /opt/memgraph/memgraph"

        # Copy tests/jepsen/memgraph required files into the control node.
        docker exec jepsen-control mkdir -p /jepsen/memgraph
        docker cp "$script_dir/src/." jepsen-control:/jepsen/memgraph/src/
        docker cp "$script_dir/test/." jepsen-control:/jepsen/memgraph/test/
        docker cp "$script_dir/project.clj" jepsen-control:/jepsen/memgraph/project.clj
    ;;
esac
