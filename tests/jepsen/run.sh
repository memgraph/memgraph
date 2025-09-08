#!/bin/bash
set -Eeuo pipefail
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

MEMGRAPH_BUILD_PATH="$script_dir/../../build"
MEMGRAPH_BINARY_PATH="$MEMGRAPH_BUILD_PATH/memgraph"
MEMGRAPH_MTENANCY_DATASETS="$script_dir/datasets/"
# NOTE: Jepsen Git tags are not consistent, there are: 0.2.4, v0.3.0, 0.3.2, ...
JEPSEN_VERSION="${JEPSEN_VERSION:-v0.3.5}"
JEPSEN_ACTIVE_NODES_NO=5
CONTROL_LEIN_RUN_ARGS="test --nodes-config resources/replication-config.edn"
CONTROL_LEIN_RUN_STDOUT_LOGS=1
CONTROL_LEIN_RUN_STDERR_LOGS=1
_JEPSEN_RUN_EXIT_STATUS=0
ENTERPRISE_LICENSE=""
ORGANIZATION_NAME=""
WGET_OR_CLONE_TIMEOUT=60

PRINT_CONTEXT() {
    echo -e "MEMGRAPH_BINARY_PATH:\t\t $MEMGRAPH_BINARY_PATH"
    echo -e "MEMGRAPH_BUILD_PATH:\t\t $MEMGRAPH_BUILD_PATH"
    echo -e "JEPSEN_VERSION:\t\t\t $JEPSEN_VERSION"
    echo -e "JEPSEN_ACTIVE_NODES_NO:\t\t $JEPSEN_ACTIVE_NODES_NO"
    echo -e "CONTROL_LEIN_RUN_ARGS:\t\t $CONTROL_LEIN_RUN_ARGS"
    echo -e "CONTROL_LEIN_RUN_STDOUT_LOGS:\t $CONTROL_LEIN_RUN_STDOUT_LOGS"
    echo -e "CONTROL_LEIN_RUN_STDERR_LOGS:\t $CONTROL_LEIN_RUN_STDERR_LOGS"
    echo -e "ENTERPRISE_LICENSE:\t\t <LICENSE KEY>"
    echo -e "ORGANIZATION_NAME:\t\t <ORGANIZATION NAME>"
}

HELP_EXIT() {
    echo ""
    echo "HELP: $0 help|cluster-up|cluster-refresh|cluster-nodes-cleanup|cluster-dealloc|test|unit-tests|process-results [args]"
    echo ""
    echo "    test args --binary                 MEMGRAPH_BINARY_PATH"
    echo "              --ignore-run-stdout-logs Ignore lein run stdout logs."
    echo "              --ignore-run-stderr-logs Ignore lein run stderr logs."
    echo "              --nodes-no               JEPSEN_ACTIVE_NODES_NO"
    echo "              --run-args               \"CONTROL_LEIN_RUN_ARGS\" (NOTE: quotes)"
    echo "              --enterprise-license     \"ENTERPRISE_LICENSE\" (NOTE: quotes)"
    echo "              --organization-name      \"ORGANIZATION_NAME\" (NOTE: quotes)"
    echo ""
    exit 1
}

ERROR() {
    /bin/echo -e "\e[101m\e[97m[ERROR]\e[49m\e[39m" "$@"
}

INFO() {
    /bin/echo -e "\e[104m\e[97m[INFO]\e[49m\e[39m" "$@"
}

# Function to get shared libraries that the binary depends on and are in the build directory
GET_SHARED_LIBRARIES() {
    local binary_path="$1"
    local build_dir="$2"
    local libraries=()

    # Use ldd to get the list of shared libraries
    # Filter for libraries that are in the build directory
    while IFS= read -r line; do
        # Skip lines that don't contain library paths
        if [[ "$line" =~ =\>[[:space:]]+([^[:space:]]+) ]]; then
            local lib_path="${BASH_REMATCH[1]}"
            # Skip "not found" and system libraries
            if [[ "$lib_path" != "not found" && "$lib_path" != "/lib"* && "$lib_path" != "/usr/lib"* ]]; then
                # Check if the library is in the build directory
                if [[ "$lib_path" == "$build_dir"* ]]; then
                    # Always add the original path (symlink or file)
                    libraries+=("$lib_path")

                    # If it's a symlink, also add the resolved path
                    if [[ -L "$lib_path" ]]; then
                        local resolved_path=$(readlink -f "$lib_path")
                        if [[ "$resolved_path" != "$lib_path" ]]; then
                            libraries+=("$resolved_path")
                        fi
                    fi

                    echo "Found shared library in build directory: $lib_path" >&2
                    if [[ -L "$lib_path" ]]; then
                        echo "  -> resolves to: $(readlink -f "$lib_path")" >&2
                    fi
                fi
            fi
        fi
    done < <(ldd "$binary_path" 2>/dev/null || echo "")

    echo "${libraries[@]}"
}

if [[ "$#" -lt 1 || "$1" == "-h" || "$1" == "--help" ]]; then
    HELP_EXIT
fi

if ! command -v docker > /dev/null 2>&1 || ! command -v docker compose > /dev/null 2>&1; then
  ERROR "docker and docker-compose have to be installed."
  exit 1
fi

if [ ! -d "$script_dir/jepsen" ]; then
  echo "Cloning Jepsen $JEPSEN_VERSION ..."
  git clone http://mgdeps-cache:8000/git/jepsen.git -b "$JEPSEN_VERSION" "$script_dir/jepsen" &> /dev/null \
  || git clone https://github.com/jepsen-io/jepsen.git -b "$JEPSEN_VERSION" "$script_dir/jepsen"
  # Apply patch for Jepsen 0.3.5 which replaces openjdk-21-jdk-headless with openjdk-22-jdk-headless
  if [[ "$JEPSEN_VERSION" == "v0.3.5" ]]; then
    echo "Applying patch for Jepsen 0.3.5 ..."
    cd "$script_dir/jepsen"
    git apply "$script_dir/0-3-5.patch"
    cd "$script_dir"
  fi
fi

PROCESS_ARGS() {
    shift
    while [[ $# -gt 0 ]]; do
        key="$1"
        case $key in
            --binary)
                shift
                MEMGRAPH_BINARY_PATH="$1"
                shift
            ;;
            --ignore-run-stdout-logs)
                CONTROL_LEIN_RUN_STDOUT_LOGS=0
                shift
            ;;
            --ignore-run-stderr-logs)
                CONTROL_LEIN_RUN_STDERR_LOGS=0
                shift
            ;;
            --nodes-no)
                shift
                JEPSEN_ACTIVE_NODES_NO="$1"
                shift
            ;;
            --run-args)
                shift
                CONTROL_LEIN_RUN_ARGS="$1"
                shift
            ;;
            --enterprise-license)
                shift
                ENTERPRISE_LICENSE="$1"
                shift
            ;;
            --organization-name)
                shift
                ORGANIZATION_NAME="$1"
                shift
            ;;
            *)
                ERROR "Unknown option $1."
                HELP_EXIT
            ;;
        esac
    done
}

COPY_FILES() {
   # Copy Memgraph binary, handles both cases, when binary is a sym link
   # or a regular file.
   # Datasets need to be downloaded only if MT test is being run
   __control_lein_run_args="${1:-}"
   binary_path="$MEMGRAPH_BINARY_PATH"

   # Resolve binary symlink if needed
   if [ -L "$binary_path" ]; then
       binary_path=$(readlink "$binary_path")
   fi
   binary_name=$(basename -- "$binary_path")

   # Get all shared libraries that the binary depends on and are in the build directory
   INFO "Scanning binary dependencies for shared libraries in build directory..."
   shared_libs=($(GET_SHARED_LIBRARIES "$binary_path" "$MEMGRAPH_BUILD_PATH"))

   if [ ${#shared_libs[@]} -eq 0 ]; then
       INFO "No shared libraries found in build directory that the binary depends on"
   else
       INFO "Found ${#shared_libs[@]} shared library(ies) to copy:"
       for lib in "${shared_libs[@]}"; do
           INFO "  - $lib"
       done
   fi


   # If running MT test, we need to download pokec medium dataset from s3
   if [[ "$__control_lein_run_args" == *"ha-mt"* ]]; then
       mkdir -p datasets
       cd datasets
       INFO "Downloading pokec medium nodes.csv file..."
       timeout $WGET_OR_CLONE_TIMEOUT wget https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/nodes.csv
       INFO "Downloading pokec medium relationships.csv file..."
       timeout $WGET_OR_CLONE_TIMEOUT wget https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/relationships.csv
       INFO "Download of datasets finished"
       cd ..
   else
     INFO "None of datasets will be downloaded"
   fi


   for iter in $(seq 1 "$JEPSEN_ACTIVE_NODES_NO"); do
       jepsen_node_name="jepsen-n$iter"
       docker_exec="docker exec $jepsen_node_name bash -c"
       if [ "$binary_name" == "memgraph" ]; then
         _binary_name="memgraph_tmp"
       else
         _binary_name="$binary_name"
       fi
       $docker_exec "rm -rf /opt/memgraph/ && mkdir -p /opt/memgraph/src/query"
       docker cp "$binary_path" "$jepsen_node_name":/opt/memgraph/"$_binary_name"

       # Copy all shared libraries to the appropriate location
       for lib in "${shared_libs[@]}"; do
           if [[ -f "$lib" ]]; then
               lib_name=$(basename -- "$lib")
               # Copy to the same relative path structure as in build directory
               # Create a clean build path without trailing slash
               clean_build_path="${MEMGRAPH_BUILD_PATH%/}"

               # Use the original path (not resolved) to preserve symlink structure
               if [[ "$lib" == "$clean_build_path"* ]]; then
                   lib_rel_path="${lib#$clean_build_path/}"
               else
                   # Fallback: use realpath --relative-to for paths outside build directory
                   lib_rel_path=$(realpath --relative-to="$clean_build_path" "$lib")
               fi
               lib_dir=$(dirname -- "$lib_rel_path")

               # Create the directory structure in the container
               $docker_exec "mkdir -p /opt/memgraph/$lib_dir"

               # Copy the library
               docker cp "$lib" "$jepsen_node_name:/opt/memgraph/$lib_rel_path"
               INFO "Copied library $lib_name to $jepsen_node_name:/opt/memgraph/$lib_rel_path"
           else
               ERROR "Library file not found: $lib"
           fi
       done

       if [ -d "$MEMGRAPH_MTENANCY_DATASETS" ]; then
           docker cp "$MEMGRAPH_MTENANCY_DATASETS" "$jepsen_node_name:/opt/memgraph"
           INFO "Datasets copied successfully to $jepsen_node_name."
       else
           INFO "Datasets won't be copied to $jepsen_node_name."
       fi
       $docker_exec "ln -s /opt/memgraph/$_binary_name /opt/memgraph/memgraph"
       $docker_exec "touch /opt/memgraph/memgraph.log"
       INFO "Copying $binary_name to $jepsen_node_name DONE."
   done

   if [ -d "$MEMGRAPH_MTENANCY_DATASETS" ]; then
       rm -rf "$MEMGRAPH_MTENANCY_DATASETS"
       INFO "Datasets folder deleted..."
   fi

   # Copy test files into the control node.
   docker exec jepsen-control mkdir -p /jepsen/memgraph/store
   docker cp "$script_dir/src/." jepsen-control:/jepsen/memgraph/src/
   docker cp "$script_dir/test/." jepsen-control:/jepsen/memgraph/test/
   docker cp "$script_dir/resources/." jepsen-control:/jepsen/memgraph/resources/
   docker cp "$script_dir/project.clj" jepsen-control:/jepsen/memgraph/project.clj
   INFO "Copying test files to jepsen-control DONE."
}

RUN_JEPSEN() {
    __control_lein_run_args="$1"
    # NOTE: docker exec -t is NOT ok because gh CI user does NOT have TTY.
    # NOTE: ~/.bashrc has to be manually sourced when bash -c is used
    #       because some Jepsen config is there.
    # To be able to archive the run result even if the run fails.
    set +e
    if [ "$CONTROL_LEIN_RUN_STDOUT_LOGS" -eq 0 ]; then
        redirect_stdout_logs="/dev/null"
    else
        redirect_stdout_logs="/dev/stdout"
    fi
    if [ "$CONTROL_LEIN_RUN_STDERR_LOGS" -eq 0 ]; then
        redirect_stderr_logs="/dev/null"
    else
        redirect_stderr_logs="/dev/stderr"
    fi
    __final_run_args="$__control_lein_run_args --license $ENTERPRISE_LICENSE --organization $ORGANIZATION_NAME"

    docker exec jepsen-control bash -c "source ~/.bashrc && cd memgraph && lein run $__final_run_args" 1> $redirect_stdout_logs 2> $redirect_stderr_logs
    _JEPSEN_RUN_EXIT_STATUS=$?
    set -e
}

PROCESS_RESULTS() {
    INFO "Process results..."
    all_workloads=$(docker exec jepsen-control bash -c 'ls /jepsen/memgraph/store/' | grep test-)
    all_workload_run_folders=""
    for workload in $all_workloads; do
      workload_dir="/jepsen/memgraph/store/$workload/latest"
      # Construct the full path of jepsen.log
      log_file_path="$workload_dir/jepsen.log"
      # Check if the directory exists before proceeding
      if docker exec jepsen-control bash -c "[ -d '$workload_dir' ]"; then
        INFO "Checking jepsen.log for $workload/latest"
        # Execute the existence check and tail command in the same subshell to avoid context errors
        docker exec jepsen-control bash -c "if [ -f '$log_file_path' ]; then tail -n 50 '$log_file_path'; else echo 'jepsen.log does not exist for $workload'; fi"
        all_workload_run_folders="$all_workload_run_folders $workload_dir"
      else
        INFO "$workload_dir does not exist."
      fi
    done
    INFO "Packing results..."
    docker exec jepsen-control bash -c "tar --ignore-failed-read -czvf /jepsen/memgraph/Jepsen.tar.gz -h $all_workload_run_folders"
    docker cp jepsen-control:/jepsen/memgraph/Jepsen.tar.gz ./
    INFO "Result processing (printing and packing) DONE."
}

CLUSTER_UP() {
  PRINT_CONTEXT
  local cnt=0
  while [[ "$cnt" < 5 ]]; do
    if ! "$script_dir/jepsen/docker/bin/up" --daemon -n $JEPSEN_ACTIVE_NODES_NO; then
      cnt=$((cnt + 1))
      continue
    else
      sleep 10
      break
    fi
  done
  # Ensure all SSH connections between Jepsen containers work
  for node in $(docker ps --filter name=jepsen* --filter status=running --format "{{.Names}}"); do
      if [ "$node" == "jepsen-control" ]; then
          continue
      fi
      node_hostname="${node##jepsen-}"
      docker exec jepsen-control bash -c "ssh -oStrictHostKeyChecking=no -t $node_hostname exit"
  done
}

CLUSTER_DEALLOC() {
  ps=$(docker ps -a --filter name=jepsen* -q)
  if [[ ! -z ${ps} ]]; then
      echo "Killing ${ps}"
      docker rm -f ${ps}
      imgs=$(docker images "jepsen*" -q)
      if [[ ! -z ${imgs} ]]; then
          echo "Removing ${imgs}"
          docker images "jepsen*" -q | xargs docker image rmi -f
      else
          echo "No Jepsen images detected!"
      fi
  else
      echo "No Jepsen containers detected!"
  fi
  echo "Cluster dealloc DONE"
}

CLUSTER_NODES_CLEANUP() {
  jepsen_control_exec="docker exec jepsen-control bash -c"
  INFO "Deleting /jepsen/memgraph/store/* on jepsen-control"
  $jepsen_control_exec "rm -rf /jepsen/memgraph/store/*"
  for iter in $(seq 1 "$JEPSEN_ACTIVE_NODES_NO"); do
      jepsen_node_name="jepsen-n$iter"
      jepsen_node_exec="docker exec $jepsen_node_name bash -c"
      INFO "Deleting /opt/memgraph/* on $jepsen_node_name"
      $jepsen_node_exec "rm -rf /opt/memgraph/*"
  done
}

# Initialize testing context by copying source/binary files. Inside CI,
# Memgraph is tested on a single machine cluster based on Docker containers.
# Once these tests will be part of the official Jepsen repo, the majority of
# functionalities inside this script won't be needed because each node clones
# the public repo.
case $1 in
    # Start Jepsen Docker cluster of 5 nodes. To configure the cluster please
    # take a look under jepsen/docker/docker-compose.yml.
    # NOTE: If you delete the jepsen folder where docker config is located,
    # the current cluster is broken because it relies on the folder. That can
    # happen easiliy because the jepsen folder is git ignored.
    cluster-up)
        PROCESS_ARGS "$@"
        PRINT_CONTEXT
        CLUSTER_UP
        COPY_FILES
    ;;

    cluster-refresh)
        PROCESS_ARGS "$@"
        CLUSTER_DEALLOC
        CLUSTER_UP
    ;;

    cluster-dealloc)
        CLUSTER_DEALLOC
    ;;

    cluster-nodes-cleanup)
        CLUSTER_NODES_CLEANUP
    ;;

    unit-tests)
        PROCESS_ARGS "$@"
        PRINT_CONTEXT
        COPY_FILES
        docker exec jepsen-control bash -c "cd /jepsen/memgraph && lein test"
        _JEPSEN_RUN_EXIT_STATUS=$?
        if [ "$_JEPSEN_RUN_EXIT_STATUS" -ne 0 ]; then
            ERROR "Unit tests FAILED" # important for the coder
            exit "$_JEPSEN_RUN_EXIT_STATUS" # important for CI
        fi
    ;;

    test)
        PROCESS_ARGS "$@"
        PRINT_CONTEXT
        COPY_FILES "$CONTROL_LEIN_RUN_ARGS"
        start_time="$(docker exec jepsen-control bash -c 'date -u +"%Y%m%dT%H%M%S"').000Z"
        INFO "Jepsen run in progress... START_TIME: $start_time"
        RUN_JEPSEN "test $CONTROL_LEIN_RUN_ARGS"
        end_time="$(docker exec jepsen-control bash -c 'date -u +"%Y%m%dT%H%M%S"').000Z"
        INFO "Jepsen run DONE. END_TIME: $end_time"
        # Exit if the jepsen run status is not 0
        if [ "$_JEPSEN_RUN_EXIT_STATUS" -ne 0 ]; then
            ERROR "Jepsen FAILED" # important for the coder
            exit "$_JEPSEN_RUN_EXIT_STATUS" # important for CI
        fi
    ;;

    process-results)
      PROCESS_ARGS "$@"
      PROCESS_RESULTS
    ;;

    *)
        HELP_EXIT
    ;;
esac
