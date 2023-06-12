#!/bin/bash
set -Eeuo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
IFS=' '
# NOTE: docker_image_name could be local image build based on release/package images.
# NOTE: each line has to be under quotes, docker_container_type, script_name and docker_image_name separate with a space.
# "docker_container_type script_name docker_image_name"
OPERATING_SYSTEMS=(
  "mgrun amzn-2 amazonlinux:2"
  "mgrun centos-7 centos:7"
  "mgrun centos-9 dokken/centos-stream-9"
  "mgrun debian-10 debian:10"
  "mgrun debian-11 debian:11"
  "mgrun fedora-36 fedora:36"
  "mgrun ubuntu-18.04 ubuntu:18.04"
  "mgrun ubuntu-20.04 ubuntu:20.04"
  "mgrun ubuntu-22.04 ubuntu:22.04"
  # "mgbuild centos-7 package-mgbuild_centos-7"
)

# TODO(gitbuda): Copy and install system package from the mgbuild container.

if [ ! "$(docker info)" ]; then
  echo "ERROR: Docker is required"
  exit 1
fi
print_help () {
  echo -e "$0 all\t\t => starts all containers in background"
  echo -e "$0 check_all\t => checks all containers"
  echo -e "$0 delete_all\t => stops all containers"
}

# NOTE: This is an idempotent operation!
docker_run () {
  cnt_name="$1"
  cnt_image="$2"
  if [ ! "$(docker ps -q -f name=$cnt_name)" ]; then
      if [ "$(docker ps -aq -f status=exited -f name=$cnt_name)" ]; then
          echo "Cleanup of the old exited container..."
          docker rm $cnt_name
      fi
      docker run -d --volume "$SCRIPT_DIR/../../:/memgraph" --network host --name "$cnt_name" "$cnt_image" sleep infinity
  fi
  echo "The $cnt_image container is active under $cnt_name name!"
}

docker_exec () {
  cnt_name="$1"
  cnt_cmd="$2"
  docker exec -it "$cnt_name" bash -c "$cnt_cmd"
}

docker_stop_and_rm () {
  cnt_name="$1"
  if [ "$(docker ps -q -f name=$cnt_name)" ]; then
      docker stop "$1"
      if [ "$(docker ps -aq -f status=exited -f name=$cnt_name)" ]; then
        docker rm "$1"
      fi
  fi
}

start_all () {
  for script_docker_pair in "${OPERATING_SYSTEMS[@]}"; do
    read -a script_docker <<< "$script_docker_pair"
    docker_container_type="${script_docker[0]}"
    script_name="${script_docker[1]}"
    docker_image="${script_docker[2]}"
    docker_name="${docker_container_type}_$script_name"
    echo ""
    echo "~~~~ OPERATING ON $docker_image as $docker_name..."
    docker_run "$docker_name" "$docker_image"
    # TODO(gitbuda): This is not working on the mgbuild containers.
    docker_exec "$docker_name" "/memgraph/environment/os/$script_name.sh install NEW_DEPS"
    echo "---- DONE EVERYHING FOR $docker_image as $docker_name..."
    echo ""
  done
}

check_all () {
  for script_docker_pair in "${OPERATING_SYSTEMS[@]}"; do
    read -a script_docker <<< "$script_docker_pair"
    docker_container_type="${script_docker[0]}"
    script_name="${script_docker[1]}"
    docker_image="${script_docker[2]}"
    docker_name="${docker_container_type}_$script_name"
    echo ""
    echo "~~~~ OPERATING ON $docker_image as $docker_name..."
    docker_exec "$docker_name" "/memgraph/environment/os/$script_name.sh check NEW_DEPS"
    echo "---- DONE EVERYHING FOR $docker_image as $docker_name..."
    echo ""
  done
}

delete_all () {
  for script_docker_pair in "${OPERATING_SYSTEMS[@]}"; do
    read -a script_docker <<< "$script_docker_pair"
    docker_container_type="${script_docker[0]}"
    script_name="${script_docker[1]}"
    docker_image="${script_docker[2]}"
    docker_name="${docker_container_type}_$script_name"
    docker_stop_and_rm "$docker_name"
    echo "~~~~ $docker_image as $docker_name DELETED"
  done
}

if [ "$#" -eq 0 ]; then
  print_help
else
  case $1 in
    run)
      start_all
    ;;
    check)
      check_all
    ;;
    delete)
      delete_all
    ;;
    *)
      print_help
    ;;
  esac
fi
