#!/bin/bash

set -Eeuo pipefail
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

HELP_EXIT() {
    echo ""
    echo "HELP: $0 help|cluster-up|test [args]"
    echo ""
    echo "    test args --binary   MEMGRAPH_BINARY_PATH"
    echo "              --nodes-no JEPSEN_ACTIVE_NODES_NO"
    echo "              --run-args \"CONTROL_LEIN_RUN_ARGS\" (NOTE: quotes)"
    echo ""
    exit 1
}

ERROR() {
    /bin/echo -e "\e[101m\e[97m[ERROR]\e[49m\e[39m" "$@"
}

INFO() {
    /bin/echo -e "\e[104m\e[97m[INFO]\e[49m\e[39m" "$@"
}

if ! command -v docker > /dev/null 2>&1 || ! command -v docker-compose > /dev/null 2>&1; then
  ERROR "docker and docker-compose have to be installed."
  exit 1
fi

MEMGRAPH_BINARY_PATH="../../build/memgraph"
JEPSEN_ACTIVE_NODES_NO=5
CONTROL_LEIN_RUN_ARGS="test-all"

if [ ! -d "$script_dir/jepsen" ]; then
    git clone https://github.com/jepsen-io/jepsen.git -b "0.2.1" "$script_dir/jepsen"
fi

# Initialize testing context by copying source/binary files. Inside CI,
# Memgraph is tested on a single machine cluster based on Docker containers.
# Once these tests will be part of the official Jepsen repo, the majority of
# functionalities inside this script won't be needed because each node clones
# the public repo.
case $1 in
    help)
        HELP_EXIT
    ;;
    # Start Jepsen Docker cluster of 5 nodes. To configure the cluster please
    # take a look under jepsen/docker/docker-compose.yml.
    # NOTE: If you delete the jepsen folder where docker config is located,
    # the current cluster is broken because it relies on the folder. That can
    # happen easiliy because the jepsen folder is git ignored.
    cluster-up)
        "$script_dir/jepsen/docker/bin/up" --daemon
    ;;
    # Run tests against the specified Memgraph binary.
    test)
        shift
        while [[ $# -gt 0 ]]; do
            key="$1"
            case $key in
                --binary)
                    shift
                    MEMGRAPH_BINARY_PATH="$1"
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
                *)
                    ERROR "Unknown option $1."
                    HELP_EXIT
                ;;
            esac
        done

        # Resolve binary path if it is a link.
        binary_path="$MEMGRAPH_BINARY_PATH"
        if [ -L "$binary_path" ]; then
            binary_path=$(readlink "$binary_path")
        fi
        binary_name=$(basename -- "$binary_path")

        # Copy Memgraph binary.
        for iter in $(seq 1 "$JEPSEN_ACTIVE_NODES_NO"); do
            jepsen_node_name="jepsen-n$iter"
            docker exec "$jepsen_node_name" mkdir -p /opt/memgraph
            docker cp "$binary_path" "$jepsen_node_name":/opt/memgraph/"$binary_name"
            docker exec "$jepsen_node_name" bash -c "rm -f /opt/memgraph/memgraph && ln -s /opt/memgraph/$binary_name /opt/memgraph/memgraph"
            INFO "Copying $binary_name to $jepsen_node_name DONE."
        done

        # Copy test files into the control node.
        docker exec jepsen-control mkdir -p /jepsen/memgraph
        docker cp "$script_dir/src/." jepsen-control:/jepsen/memgraph/src/
        docker cp "$script_dir/test/." jepsen-control:/jepsen/memgraph/test/
        docker cp "$script_dir/resources/." jepsen-control:/jepsen/memgraph/resources/
        docker cp "$script_dir/project.clj" jepsen-control:/jepsen/memgraph/project.clj
        INFO "Copying test files to jepsen-control DONE."

        # Run the test.
        # NOTE: docker exec -t is NOT ok because gh CI user does NOT have TTY.
        # NOTE: ~/.bashrc has to be manually sourced when bash -c is used
        #       because some Jepsen config is there.
        docker exec jepsen-control bash -c "source ~/.bashrc && cd memgraph && lein run $CONTROL_LEIN_RUN_ARGS"

        # Just pack all the latest test workloads. They might not be generated
        # within this run.
        all_workloads=$(docker exec jepsen-control bash -c 'ls /jepsen/memgraph/store/' | grep test-)
        all_latest_workflow_runs=""
        for workload in $all_workloads; do
            all_latest_workflow_runs="$all_latest_workflow_runs "'$(readlink -f /jepsen/memgraph/store/'"$workload"'/latest)'
        done
        docker exec jepsen-control bash -c "tar -czvf /jepsen/memgraph/Jepsen.tar.gz $all_latest_workflow_runs"
        docker cp jepsen-control:/jepsen/memgraph/Jepsen.tar.gz ./
        INFO "Test and results packing DONE."
    ;;
esac
