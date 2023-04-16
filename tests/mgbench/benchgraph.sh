#!/bin/bash -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd "$SCRIPT_DIR"

# TODO(gitbuda): Add relative path to the memgraph build repo and memgraph/libs/neo path (update the neo version there).
# TODO(gitbuda): Read vars from the env -> FOO="${VARIABLE:-default}".
mg_path="/home/buda/Workspace/code/memgraph/memgraph/build/memgraph"
neo_path="/home/buda/Downloads/neo4j-community-5.6.0/bin/neo4j"
workers="48"

check_binary () {
  binary_path=$1
  if [ -f "$binary_path" ]; then
    echo "$binary_path found."
  else
    echo "Failed to find $binary_path exiting..."
    exit 1
  fi
}

check_binary "$mg_path"
check_binary "$neo_path"
echo "WORKERS: $workers"

# If you want to skip some of the workloads, just comment lines under the
# WORKLOADS variable.
WORKLOADS=(
  pokec_small
  pokec_medium
  ldbc_interactive_sf0_1
  ldbc_interactive_sf1
  ldbc_bi_sf1
  # ldbc_interactive_sf3
  # ldbc_bi_sf3
)
# TODO(gitbuda): If we want to add multiple number of workers (we want xD), we just add another variable and add another for loop down below.

pokec_small () {
  echo "${FUNCNAME[0]}"
  return # TODO(gitbuda): tmp, remove this line
  python3 graph_bench.py --vendor memgraph $mg_path --vendor neo4j $neo_path \
    --dataset-name pokec --dataset-group basic  --dataset-size small \
    --realistic 500 30 70 0 0 \
    --realistic 500 30 70 0 0 \
    --realistic 500 50 50 0 0 \
    --realistic 500 70 30 0 0 \
    --realistic 500 30 40 10 20 \
    --mixed 500 30 0 0 0 70 \
    --num-workers-for-benchmark $workers
}

pokec_medium () {
  echo "${FUNCNAME[0]}"
  return # TODO(gitbuda): tmp, remove this line
  python3 graph_bench.py --vendor memgraph $mg_path --vendor neo4j $neo_path \
    --dataset-name pokec --dataset-group basic  --dataset-size medium \
    --realistic 500 30 70 0 0 \
    --realistic 500 30 70 0 0 \
    --realistic 500 50 50 0 0 \
    --realistic 500 70 30 0 0 \
    --realistic 500 30 40 10 20 \
    --mixed 500 30 0 0 0 70 \
    --num-workers-for-benchmark $workers
}

ldbc_interactive_sf0_1 () {
  echo "${FUNCNAME[0]}"
  return # TODO(gitbuda): tmp, remove this line
  python3 graph_bench.py --vendor memgraph $mg_path --vendor neo4j $neo_path \
    --dataset-name ldbc_interactive --dataset-group interactive  --dataset-size sf0.1 \
    --num-workers-for-benchmark $workers
}

ldbc_interactive_sf1 () {
  echo "${FUNCNAME[0]}"
  return # TODO(gitbuda): tmp, remove this line
  python3 graph_bench.py --vendor memgraph $mg_path --vendor neo4j $neo_path \
    --dataset-name ldbc_interactive --dataset-group interactive  --dataset-size sf1 \
    --num-workers-for-benchmark $workers
}

ldbc_bi_sf1 () {
  echo "${FUNCNAME[0]}"
  return # TODO(gitbuda): tmp, remove this line
  python3 graph_bench.py --vendor memgraph $mg_path --vendor neo4j $neo_path \
    --dataset-name ldbc_bi --dataset-group bi  --dataset-size sf1 \
    --num-workers-for-benchmark $workers
}

ldbc_interactive_sf3 () {
  echo "${FUNCNAME[0]}"
  return # TODO(gitbuda): tmp, remove this line
  python3 graph_bench.py --vendor memgraph $mg_path --vendor neo4j $neo_path \
    --dataset-name ldbc_interactive --dataset-group interactive  --dataset-size sf3 \
    --num-workers-for-benchmark $workers
}

ldbc_bi_sf3 () {
  echo "${FUNCNAME[0]}"
  return # TODO(gitbuda): tmp, remove this line
  python3 graph_bench.py --vendor memgraph $mg_path --vendor neo4j $neo_path \
    --dataset-name ldbc_bi --dataset-group bi  --dataset-size sf3 \
    --num-workers-for-benchmark $workers
}

for workload in "${WORKLOADS[@]}"; do
  $workload
  sleep 1
done

# TODO(gitbuda): Add zip of all result files.
# TODO(gitbuda): Optionally clean the cache files because a lot of data stays under the system after run of this.
