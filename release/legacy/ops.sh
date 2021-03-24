#!/bin/bash

# TODO(gitbuda): Docker file for each legacy OS. During build install deps.
# TODO(gitbuda): Docker compose to mange the cluster of Docker build containers.
# TODO(gitbuda): The ops.sh script should manage the cluster. If cluster is up, do nothing. If there is, e.g., --override, reboot the cluster.
# TODO(gitbuda): The ops.sh should be parametrized by Memgraph build type and target operating system.

docker-compose build
docker-compose up -d
