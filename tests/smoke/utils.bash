#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

MEMGRAPH_BUILD_PATH="${MEMGRAPH_BUILD_PATH:-/tmp/memgraph/build}"
MEMGRAPH_CONSOLE_BINARY="${MEMGRAPH_CONSOLE_BINARY:-$SCRIPT_DIR/mgconsole.build/build/src/mgconsole}"
MEMGRAPH_ENTERPRISE_LICENSE="${MEMGRAPH_ENTERPRISE_LICENSE:-provide_licanse_string}"
MEMGRAPH_ORGANIZATION_NAME="${MEMGRAPH_ORGANIZATION_NAME:-provide_organization_name_string}"
MEMGRAPH_LAST_DOCKERHUB_IMAGE="${MEMGRAPH_LAST_DOCKERHUB_IMAGE:-provide_dockerhub_image_name}"
MEMGRAPH_NEXT_DOCKERHUB_IMAGE="${MEMGRAPH_NEXT_DOCKERHUB_IMAGE:-provide_dockerhub_image_name}"
MEMGRAPH_LAST_RC_DIRECT_DOCKER_IMAGE_ARM="${MEMGRAPH_LAST_RC_DIRECT_DOCKER_IMAGE_ARM:-provide_https_download_link}"
MEMGRAPH_NEXT_RC_DIRECT_DOCKER_IMAGE_ARM="${MEMGRAPH_NEXT_RC_DIRECT_DOCKER_IMAGE_ARM:-provide_https_download_link}"
MEMGRAPH_LAST_RC_DIRECT_DOCKER_IMAGE_X86="${MEMGRAPH_LAST_RC_DIRECT_DOCKER_IMAGE_X86:-provide_https_download_link}"
MEMGRAPH_NEXT_RC_DIRECT_DOCKER_IMAGE_X86="${MEMGRAPH_NEXT_RC_DIRECT_DOCKER_IMAGE_X86:-provide_https_donwload_link}"

print_help_and_exit_unsuccessfully() {
  echo "It's required to define the following environment variables:"
  echo "  MEMGRAPH_ENTERPRISE_LICENSE"
  echo "  MEMGRAPH_ORGANIZATION_NAME"
  echo "  MEMGRAPH_LAST_DOCKERHUB_IMAGE"
  echo "  MEMGRAPH_NEXT_DOCKERHUB_IMAGE"
  echo "Optionally if you want to test daily or RC builds you can define the following environment variables:"
  echo "  MEMGRAPH_LAST_RC_DIRECT_DOCKER_IMAGE_ARM"
  echo "  MEMGRAPH_NEXT_RC_DIRECT_DOCKER_IMAGE_ARM"
  echo "  MEMGRAPH_LAST_RC_DIRECT_DOCKER_IMAGE_X86"
  echo "  MEMGRAPH_NEXT_RC_DIRECT_DOCKER_IMAGE_X86"
  exit 1
}
check_dockerhub_images() {
  if [ "$MEMGRAPH_ENTERPRISE_LICENSE" = "provide_licanse_string" ]; then
    print_help_and_exit_unsuccessfully
  fi
  if [ "$MEMGRAPH_ORGANIZATION_NAME" = "provide_organization_name_string" ]; then
    print_help_and_exit_unsuccessfully
  fi
  if [ "$MEMGRAPH_LAST_DOCKERHUB_IMAGE" = "provide_dockerhub_image_name" ]; then
    print_help_and_exit_unsuccessfully
  fi
  if [ "$MEMGRAPH_NEXT_DOCKERHUB_IMAGE" = "provide_dockerhub_image_name" ]; then
    print_help_and_exit_unsuccessfully
  fi
  echo "License and Docker images validation passed:"
  echo "  LAST: $MEMGRAPH_LAST_DOCKERHUB_IMAGE"
  echo "  NEXT: $MEMGRAPH_NEXT_DOCKERHUB_IMAGE"
}
check_dockerhub_images

MEMGRAPH_GENERAL_FLAGS="--telemetry-enabled=false --log-level=TRACE --also-log-to-stderr"
MEMGRAPH_ENTERPRISE_DOCKER_ENVS="-e MEMGRAPH_ENTERPRISE_LICENSE=$MEMGRAPH_ENTERPRISE_LICENSE -e MEMGRAPH_ORGANIZATION_NAME=$MEMGRAPH_ORGANIZATION_NAME"
MEMGRAPH_DOCKER_MOUNT_VOLUME_FLAGS="-v mg_lib:/var/lib/memgraph"
MEMGRAPH_DOCKER_LOCAL_DATA_MOUNT_VOLUME_FLAGS="-v $SCRIPT_DIR/data:/data"
MEMGRAPH_FULL_PROPERTIES_SET="{id:0, name:\"tester\", age:37, height:175.0, merried:true}"
MEMGRAPH_PROPERTY_COMPRESSION_FLAGS="--storage-property-store-compression-enabled=true --storage-property-store-compression-level=mid"
MEMGRAPH_HA_COORDINATOR_FLAGS="--coordinator-port=10001 --bolt-port=7687 --coordinator-id=1 --experimental-enabled=high-availability --coordinator-hostname=localhost --management-port=11001"
MEMGRAPH_SHOW_SCHEMA_INFO_FLAG="--schema-info-enabled=true"
MEMGRAPH_SESSION_TRACE_FLAG="--query-log-directory=/var/log/memgraph/session_traces"
MEMGRAPH_DEFAULT_HOST="localhost"
MEMGRAPH_DEFAULT_PORT="7687"
MEMGRAPH_LAST_DATA_BOLT_PORT="8001"
MEMGRAPH_LAST_COORDINATOR_BOLT_PORT="8002"
MEMGRAPH_NEXT_DATA_BOLT_PORT="8003"
MEMGRAPH_NEXT_COORDINATOR_BOLT_PORT="8004"
MEMGRAPH_LAST_MONITORING_PORT="9001"
MEMGRAPH_NEXT_MONITORING_PORT="9002"

MGCONSOLE_NEXT_DEFAULT="$MEMGRAPH_CONSOLE_BINARY --host $MEMGRAPH_DEFAULT_HOST --port $MEMGRAPH_NEXT_DATA_BOLT_PORT"
MGCONSOLE_NEXT_ADMIN="$MEMGRAPH_CONSOLE_BINARY --host $MEMGRAPH_DEFAULT_HOST --port $MEMGRAPH_NEXT_DATA_BOLT_PORT --username admin --password admin1234"
MGCONSOLE_NEXT_TESTER="$MEMGRAPH_CONSOLE_BINARY --host $MEMGRAPH_DEFAULT_HOST --port $MEMGRAPH_NEXT_DATA_BOLT_PORT --username tester --password tester1234"
run_next() {
  __query="$1"
  echo "$__query" | $MGCONSOLE_NEXT_DEFAULT
}
run_next_admin() {
  __query="$1"
  echo "$__query" | $MGCONSOLE_NEXT_ADMIN
}
run_next_tester() {
  __query="$1"
  echo "$__query" | $MGCONSOLE_NEXT_TESTER
}
run_next_csv() {
  __query="$1"
  echo "$__query" | $MGCONSOLE_NEXT_DEFAULT --output-format=csv
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

wait_for_memgraph_coordinator() {
  __host=$1
  __port=$2
  __max_retries=${3:-100}
  __retries=0
  while ! echo "SHOW INSTANCE;" | $MEMGRAPH_CONSOLE_BINARY --host $__host --port $__port > /dev/null 2>&1; do
    sleep 0.3
    __retries=$((__retries+1))
    if [ "$__retries" -ge "$__max_retries" ]; then
      echo "wait_for_memgraph_coordinator: Reached max retries ($__max_retries) for $__host:$__port"
      return 1
    fi
  done
  return 0
}

wait_for_memgraph_main() {
  __host=$1
  __port=$2
  __max_retries=${3:-20}
  __retries=0
  while ! echo "SHOW REPLICATION ROLE;" | $MEMGRAPH_CONSOLE_BINARY --host $__host --port $__port --output-format=csv | python3 $SCRIPT_DIR/../validator.py validate_is_main > /dev/null 2>&1; do
    sleep 0.3
    __retries=$((__retries+1))
    if [ "$__retries" -ge "$__max_retries" ]; then
      echo "wait_for_memgraph_main: Reached max retries ($__max_retries) for $__host:$__port"
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
  # NOTE: This function runs memgraph on the NEXT_DATA_BOLT_PORT because all
  # tests are mostly executed against that (a convention to shorten the code).
  __args="$1"
  __test_func_name=$2
  __mg_pid=$(run_memgraph_binary "--bolt-port $MEMGRAPH_NEXT_DATA_BOLT_PORT $__args")
  # wait_port $MEMGRAPH_NEXT_DATA_BOLT_PORT
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

pull_dockerhub_last_image() {
  if ! docker image inspect $MEMGRAPH_LAST_DOCKERHUB_IMAGE >/dev/null 2>&1; then
    docker pull $MEMGRAPH_LAST_DOCKERHUB_IMAGE
  fi
}
pull_dockerhub_next_image() {
  if ! docker image inspect $MEMGRAPH_NEXT_DOCKERHUB_IMAGE >/dev/null 2>&1; then
    docker pull $MEMGRAPH_NEXT_DOCKERHUB_IMAGE
  fi
}

pull_RC_last_image() {
  if ! docker image inspect $MEMGRAPH_LAST_DOCKERHUB_IMAGE >/dev/null 2>&1; then
    _url_last="$MEMGRAPH_LAST_RC_DIRECT_DOCKER_IMAGE_X86"
    if [ "$(arch)" == "arm64" ]; then
      _url_last="$MEMGRAPH_LAST_RC_DIRECT_DOCKER_IMAGE_ARM"
    fi
    _filename_last=$(basename $_url_last)
    wget "$_url_last" -O "$SCRIPT_DIR/$_filename_last"
    docker load -i "$SCRIPT_DIR/$_filename_last"
  fi
}

pull_RC_next_image() {
  if ! docker image inspect $MEMGRAPH_NEXT_DOCKERHUB_IMAGE >/dev/null 2>&1; then
    _url_next="$MEMGRAPH_NEXT_RC_DIRECT_DOCKER_IMAGE_X86"
    if [ "$(arch)" == "arm64" ]; then
      _url_next="$MEMGRAPH_NEXT_RC_DIRECT_DOCKER_IMAGE_ARM"
    fi
    _filename_next=$(basename $_url_next)
    wget "$_url_next" -O "$SCRIPT_DIR/$_filename_next"
    docker load -i "$SCRIPT_DIR/$_filename_next"
  fi
}

pull_docker_images() {
  __how_to_pull_last="$1"
  __how_to_pull_next="$2"
  if [ "$__how_to_pull_last" == "RC" ]; then
    pull_RC_last_image
  fi
  if [ "$__how_to_pull_last" == "Dockerhub" ]; then
    pull_dockerhub_last_image
  fi
  if [ "$__how_to_pull_next" == "RC" ]; then
    pull_RC_next_image
  fi
  if [ "$__how_to_pull_next" == "Dockerhub" ]; then
    pull_dockerhub_next_image
  fi
}

run_memgraph_last_dockerhub_container() {
  if [ ! "$(docker ps -q -f name=memgraph_last_data)" ]; then
    docker run -d --rm -p $MEMGRAPH_LAST_DATA_BOLT_PORT:7687 -p $MEMGRAPH_LAST_MONITORING_PORT:9091 \
      $MEMGRAPH_DOCKER_LOCAL_DATA_MOUNT_VOLUME_FLAGS \
      --name memgraph_last_data \
      $MEMGRAPH_LAST_DOCKERHUB_IMAGE $MEMGRAPH_GENERAL_FLAGS
  fi
}

run_memgraph_next_dockerhub_container() {
  if [ ! "$(docker ps -q -f name=memgraph_next_data)" ]; then
    docker run -d --rm -p $MEMGRAPH_NEXT_DATA_BOLT_PORT:7687 -p $MEMGRAPH_NEXT_MONITORING_PORT:9091 \
      $MEMGRAPH_DOCKER_LOCAL_DATA_MOUNT_VOLUME_FLAGS \
      --name memgraph_next_data \
      $MEMGRAPH_ENTERPRISE_DOCKER_ENVS $MEMGRAPH_NEXT_DOCKERHUB_IMAGE $MEMGRAPH_GENERAL_FLAGS \
      $MEMGRAPH_PROPERTY_COMPRESSION_FLAGS $MEMGRAPH_SHOW_SCHEMA_INFO_FLAG
  fi
}

run_memgraph_last_dockerhub_container_with_volume() {
  docker run -d --rm -p $MEMGRAPH_LAST_DATA_BOLT_PORT:7687 $MEMGRAPH_DOCKER_MOUNT_VOLUME_FLAGS \
    --name memgraph_last_data $MEMGRAPH_LAST_DOCKERHUB_IMAGE $MEMGRAPH_GENERAL_FLAGS
}

run_memgraph_next_dockerhub_container_with_volume() {
  docker run -d --rm -p $MEMGRAPH_NEXT_DATA_BOLT_PORT:7687 $MEMGRAPH_DOCKER_MOUNT_VOLUME_FLAGS \
    --name memgraph_next_data $ENTERPRISE_DOCKER_ENVS_UNLIMITED $MEMGRAPH_NEXT_DOCKERHUB_IMAGE $MEMGRAPH_GENERAL_FLAGS $MEMGRAPH_PROPERTY_COMPRESSION_FLAGS
}

run_memgraph_coordinator_next_dockerhub_container() {
  docker run -d --rm -p $MEMGRAPH_NEXT_COORDINATOR_BOLT_PORT:7687 --name memgraph_next_coordinator \
    $MEMGRAPH_ENTERPRISE_DOCKER_ENVS $MEMGRAPH_NEXT_DOCKERHUB_IMAGE $MEMGRAPH_GENERAL_FLAGS \
    $MEMGRAPH_PROPERTY_COMPRESSION_FLAGS $MEMGRAPH_HA_COORDINATOR_FLAGS
}

run_memgraph_docker_containers() {
  __how_to_pull_last="$1"
  __how_to_pull_next="$2"
  pull_docker_images "$__how_to_pull_last" "$__how_to_pull_next"
  run_memgraph_last_dockerhub_container
  run_memgraph_next_dockerhub_container
}

docker_stop_if_there() {
  container_name="$1"
  if [ "$(docker ps -q -f name=$container_name)" ]; then
    docker stop $container_name
    docker rm $container_name || true # If container is started with --rm if will automatically get deleted.
  fi
}

cleanup_docker() {
  docker_stop_if_there memgraph_last_data || true
  docker_stop_if_there memgraph_next_data || true
  docker_stop_if_there memgraph_next_coordinator || true
}

cleanup_docker_exit() {
  ARG=$?
  cleanup_docker
  exit $ARG
}

spinup_and_cleanup_memgraph_dockers() {
  __how_to_pull_last="$1"
  __how_to_pull_next="$2"
  cleanup_docker # Run to stop and previously running containers.
  run_memgraph_docker_containers "$__how_to_pull_last" "$__how_to_pull_next"
  trap cleanup_docker_exit EXIT
}

with_kubectl_portforward() (
    local target=$1  # svc/foo, pod/bar, deployment/baz, …
    local map=$2     # 8080:80 or 8443 or 0.0.0.0:8080:80
    local probe="$3" # probe to test the target process, e.g. wait_for_xyz
    shift 4          # “--” + user commands; NOTE: Use an array of commands
                     #     inside a string: -- 'cmd1' 'cmd2' ...
                     #     because if you use ; or && to separate commands,
                     #     bash will treat that as one command + cleanup + the
                     #     rest.

    local log
    local pf_pid
    local retries=0
    local max_retries=5
    while true; do
        log=$(mktemp)
        kubectl port-forward "$target" "$map" >/dev/null 2>>"$log" &
        pf_pid=$!
        # NOTE: port-forward doesn't have built-in timeout + the target process
        # might take arbitrary time to initialize. -> The only way to know if
        # everything is right in the shortest amount of time is to inject the
        # target process probe as one of the required params.
        sleep 0.3
        if ! eval "$probe"; then
          kill -9 "$pf_pid" 2>/dev/null || true
          wait "$pf_pid" 2>/dev/null || true
          retries=$((retries+1))
          if [ $retries -ge $max_retries ]; then
            echo "kubectl port-forward failed after $max_retries attempts — see $log and inspect the target process" >&2
            exit 1
          fi
        else
          break
        fi
    done

    cleanup() {
        echo "calling port forward cleanup"
        kill -9 "$pf_pid" 2>/dev/null || true
        wait "$pf_pid" 2>/dev/null || true
    }
    trap cleanup EXIT INT TERM

    local lport=${map%%:*}              # leftmost number before the first ':'
    for _ in {1..50}; do                # ~10 s max (50×0.2 s)
        if nc -z 127.0.0.1 "$lport" 2>/dev/null; then break; fi
        if ! kill -0 "$pf_pid" 2>/dev/null; then
            echo "port-forward crashed — see $log" >&2
            exit 1
        fi
        sleep 0.2
    done

    for cmd in "$@"; do
      eval "$cmd"
    done
)
