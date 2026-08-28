#!/bin/bash
# NOTE: Every feature test file sources this one -> the guard keeps the checks
# below from running (and printing) dozens of times per run.
if [ -n "${MEMGRAPH_SMOKE_UTILS_LOADED:-}" ]; then
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  return 0
fi
MEMGRAPH_SMOKE_UTILS_LOADED=1

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# NOTE: SMOKE_DIR always points at tests/smoke.
SMOKE_DIR="$SCRIPT_DIR"

MEMGRAPH_BUILD_PATH="${MEMGRAPH_BUILD_PATH:-/tmp/memgraph/build}"
MEMGRAPH_CONSOLE_BINARY="${MEMGRAPH_CONSOLE_BINARY:-$SCRIPT_DIR/bin/mgconsole}"
MEMGRAPH_ENTERPRISE_LICENSE="${MEMGRAPH_ENTERPRISE_LICENSE:-provide_licanse_string}"
MEMGRAPH_ORGANIZATION_NAME="${MEMGRAPH_ORGANIZATION_NAME:-provide_organization_name_string}"
MEMGRAPH_DOCKERHUB_IMAGE="${MEMGRAPH_DOCKERHUB_IMAGE:-provide_dockerhub_image_name}"
MEMGRAPH_RC_DIRECT_DOCKER_IMAGE_ARM="${MEMGRAPH_RC_DIRECT_DOCKER_IMAGE_ARM:-provide_https_download_link}"
MEMGRAPH_RC_DIRECT_DOCKER_IMAGE_X86="${MEMGRAPH_RC_DIRECT_DOCKER_IMAGE_X86:-provide_https_download_link}"

print_help_and_exit_unsuccessfully() {
  echo "It's required to define the following environment variables:"
  echo "  MEMGRAPH_ENTERPRISE_LICENSE"
  echo "  MEMGRAPH_ORGANIZATION_NAME"
  echo "  MEMGRAPH_DOCKERHUB_IMAGE"
  echo "Optionally if you want to test daily or RC builds you can define the following environment variables:"
  echo "  MEMGRAPH_RC_DIRECT_DOCKER_IMAGE_ARM"
  echo "  MEMGRAPH_RC_DIRECT_DOCKER_IMAGE_X86"
  exit 1
}
check_dockerhub_image() {
  if [ "$MEMGRAPH_ENTERPRISE_LICENSE" = "provide_licanse_string" ]; then
    print_help_and_exit_unsuccessfully
  fi
  if [ "$MEMGRAPH_ORGANIZATION_NAME" = "provide_organization_name_string" ]; then
    print_help_and_exit_unsuccessfully
  fi
  if [ "$MEMGRAPH_DOCKERHUB_IMAGE" = "provide_dockerhub_image_name" ]; then
    print_help_and_exit_unsuccessfully
  fi
  echo "License and Docker image validation passed:"
  echo "  Image: $MEMGRAPH_DOCKERHUB_IMAGE"
}
check_dockerhub_image

MEMGRAPH_GENERAL_FLAGS="--telemetry-enabled=false --log-level=TRACE --also-log-to-stderr"
MEMGRAPH_ENTERPRISE_DOCKER_ENVS="-e MEMGRAPH_ENTERPRISE_LICENSE=$MEMGRAPH_ENTERPRISE_LICENSE -e MEMGRAPH_ORGANIZATION_NAME=$MEMGRAPH_ORGANIZATION_NAME"
MEMGRAPH_DOCKER_LOCAL_DATA_MOUNT_VOLUME_FLAGS="-v $SCRIPT_DIR/data:/data"
MEMGRAPH_FULL_PROPERTIES_SET="{id:0, name:\"tester\", age:37, height:175.0, merried:true}"
MEMGRAPH_PROPERTY_COMPRESSION_FLAGS="--storage-property-store-compression-enabled=true --storage-property-store-compression-level=mid"
MEMGRAPH_SHOW_SCHEMA_INFO_FLAG="--schema-info-enabled=true"
MEMGRAPH_SESSION_TRACE_FLAG="--query-log-directory=/var/log/memgraph/session_traces"
# Empty unless the caller asks for FIPS mode (test_single.bash --fips).
MEMGRAPH_FIPS_FLAGS="${MEMGRAPH_FIPS_FLAGS:-}"
MEMGRAPH_EXEC="${MEMGRAPH_EXEC:-docker exec -u memgraph memgraph_smoke}"
MEMGRAPH_DEFAULT_HOST="localhost"
MEMGRAPH_DEFAULT_PORT="7687"
MEMGRAPH_BOLT_PORT="8003"
MEMGRAPH_MONITORING_PORT="9002"

MGCONSOLE_DEFAULT="$MEMGRAPH_CONSOLE_BINARY --host $MEMGRAPH_DEFAULT_HOST --port $MEMGRAPH_BOLT_PORT"
MGCONSOLE_ADMIN="$MEMGRAPH_CONSOLE_BINARY --host $MEMGRAPH_DEFAULT_HOST --port $MEMGRAPH_BOLT_PORT --username admin --password admin1234"
MGCONSOLE_TESTER="$MEMGRAPH_CONSOLE_BINARY --host $MEMGRAPH_DEFAULT_HOST --port $MEMGRAPH_BOLT_PORT --username tester --password tester1234"
run_query() {
  __query="$1"
  echo "$__query" | $MGCONSOLE_DEFAULT
}
run_query_admin() {
  __query="$1"
  echo "$__query" | $MGCONSOLE_ADMIN
}
run_query_tester() {
  __query="$1"
  echo "$__query" | $MGCONSOLE_TESTER
}
run_query_csv() {
  __query="$1"
  echo "$__query" | $MGCONSOLE_DEFAULT --output-format=csv
}

wait_port() {
  __port="$1"
  while ! nc -z localhost $__port; do
    sleep 0.1
  done
}

wait_for_memgraph() {
  __host=$1
  __port=$2
  __max_retries=${3:-100}
  __retries=0
  while ! echo "RETURN 1;" | $MEMGRAPH_CONSOLE_BINARY --host $__host --port $__port > /dev/null 2>&1; do
    sleep 0.3
    __retries=$((__retries+1))
    if [ "$__retries" -ge "$__max_retries" ]; then
      echo "wait_for_memgraph: Reached max retries ($__max_retries) for $__host:$__port"
      return 1
    fi
  done
  return 0
}

run_memgraph_binary() {
  # note: printing anything is tricky if this is called under $(...).
  __args="$1"
  cd $MEMGRAPH_BUILD_PATH
  # https://stackoverflow.com/questions/10508843/what-is-dev-null-21
  ./memgraph $__args >> /dev/null 2>&1 &
  echo $!
}

run_memgraph_binary_and_test() {
  # NOTE: This function runs memgraph on the MEMGRAPH_BOLT_PORT because all
  # tests are mostly executed against that (a convention to shorten the code).
  __args="$1"
  __test_func_name=$2
  __mg_pid=$(run_memgraph_binary "--bolt-port $MEMGRAPH_BOLT_PORT $__args")
  # wait_port $MEMGRAPH_BOLT_PORT
  $__test_func_name
  kill -15 $__mg_pid
}

cleanup_memgraph_binary_processes() {
  set +e # This should be called on script EXIT and not fail.
  pids="$(pgrep -f "\./memgraph")" # Only match ./memgraph.
  if [ ! -z "$pids" ]; then
    kill -15 $pids
  fi
}

pull_dockerhub_image() {
  if ! docker image inspect $MEMGRAPH_DOCKERHUB_IMAGE >/dev/null 2>&1; then
    docker pull $MEMGRAPH_DOCKERHUB_IMAGE
  fi
}

pull_RC_image() {
  if ! docker image inspect $MEMGRAPH_DOCKERHUB_IMAGE >/dev/null 2>&1; then
    _url="$MEMGRAPH_RC_DIRECT_DOCKER_IMAGE_X86"
    if [ "$(arch)" == "arm64" ]; then
      _url="$MEMGRAPH_RC_DIRECT_DOCKER_IMAGE_ARM"
    fi
    _filename=$(basename $_url)
    wget "$_url" -O "$SCRIPT_DIR/$_filename"
    docker load -i "$SCRIPT_DIR/$_filename"
  fi
}

pull_docker_image() {
  __how_to_pull="$1"
  if [ "$__how_to_pull" == "RC" ]; then
    pull_RC_image
  fi
  if [ "$__how_to_pull" == "Dockerhub" ]; then
    pull_dockerhub_image
  fi
}

run_memgraph_dockerhub_container() {
  if [ ! "$(docker ps -q -f name=memgraph_smoke)" ]; then
    docker run -d --rm -p $MEMGRAPH_BOLT_PORT:7687 -p $MEMGRAPH_MONITORING_PORT:9091 \
      $MEMGRAPH_DOCKER_LOCAL_DATA_MOUNT_VOLUME_FLAGS \
      --name memgraph_smoke \
      $MEMGRAPH_ENTERPRISE_DOCKER_ENVS $MEMGRAPH_DOCKERHUB_IMAGE $MEMGRAPH_GENERAL_FLAGS \
      $MEMGRAPH_PROPERTY_COMPRESSION_FLAGS $MEMGRAPH_SHOW_SCHEMA_INFO_FLAG $MEMGRAPH_FIPS_FLAGS
  fi
}

run_memgraph_docker_container() {
  __how_to_pull="$1"
  pull_docker_image "$__how_to_pull"
  run_memgraph_dockerhub_container
}

docker_stop_if_there() {
  container_name="$1"
  if [ "$(docker ps -q -f name=$container_name)" ]; then
    docker stop $container_name
    docker rm $container_name || true # If container is started with --rm if will automatically get deleted.
  fi
}

cleanup_docker() {
  docker_stop_if_there memgraph_smoke || true

  if declare -F kerberos_cleanup >/dev/null; then
    kerberos_cleanup
  fi
}

cleanup_docker_exit() {
  ARG=$?
  cleanup_docker
  exit $ARG
}

spinup_and_cleanup_memgraph_docker() {
  __how_to_pull="$1"
  cleanup_docker # Run to stop and previously running containers.
  run_memgraph_docker_container "$__how_to_pull"
  trap cleanup_docker_exit EXIT
}
