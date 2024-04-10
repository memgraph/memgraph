#!/bin/bash -e

pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd "$SCRIPT_DIR"

print_help () {
  echo -e "$0\t\t => runs all available benchmarks with the prompt"
  echo -e "$0 run_all\t => runs all available benchmarks"
  echo -e "$0 zip\t => packages all result files and info about the system"
  echo -e "$0 clean\t => removes all result files including the zip"
  echo -e "$0 -h\t => prints help"
  echo ""
  echo "  env vars:"
  echo "    MGBENCH_MEMGRAPH_BIN_PATH -> path to the memgraph binary in the release mode"
  echo "    MGBENCH_NEO_BIN_PATH -> path to the neo4j binary"
  exit 0
}

MG_PATH="${MGBENCH_MEMGRAPH_BIN_PATH:-$SCRIPT_DIR/../../build/memgraph}"
NEO_PATH="${MGBENCH_NEO_BIN_PATH:-$SCRIPT_DIR/../../libs/neo4j/bin/neo4j}"
# If you want to skip some of the workloads or workers, just comment lines
# under the WORKLOADS or WORKERS variables.
WORKLOADS=(
  pokec_small
  pokec_medium
  ldbc_interactive_sf0_1
  ldbc_interactive_sf1
  ldbc_bi_sf1
  ldbc_interactive_sf3
  ldbc_bi_sf3
)
WORKERS=(
  12
  24
  48
)

check_binary () {
  binary_path=$1
  if [ -f "$binary_path" ]; then
    echo "$binary_path found."
  else
    echo "Failed to find $binary_path exiting..."
    exit 1
  fi
}
check_all_binaries () {
  check_binary "$MG_PATH"
  check_binary "$NEO_PATH"
}

pokec_small () {
  workers=$1
  echo "running ${FUNCNAME[0]} with $workers client workers"
  python3 graph_bench.py --vendor memgraph "$MG_PATH" --vendor neo4j "$NEO_PATH" \
    --dataset-name pokec --dataset-group basic  --dataset-size small \
    --realistic 500 30 70 0 0 \
    --realistic 500 30 70 0 0 \
    --realistic 500 50 50 0 0 \
    --realistic 500 70 30 0 0 \
    --realistic 500 30 40 10 20 \
    --mixed 500 30 0 0 0 70 \
    --num-workers-for-benchmark "$workers"
}

pokec_medium () {
  workers=$1
  echo "running ${FUNCNAME[0]} with $workers client workers"
  python3 graph_bench.py --vendor memgraph "$MG_PATH" --vendor neo4j "$NEO_PATH" \
    --dataset-name pokec --dataset-group basic  --dataset-size medium \
    --realistic 500 30 70 0 0 \
    --realistic 500 30 70 0 0 \
    --realistic 500 50 50 0 0 \
    --realistic 500 70 30 0 0 \
    --realistic 500 30 40 10 20 \
    --mixed 500 30 0 0 0 70 \
    --num-workers-for-benchmark "$workers"
}

ldbc_interactive_sf0_1 () {
  workers=$1
  echo "running ${FUNCNAME[0]} with $workers client workers"
  python3 graph_bench.py --vendor memgraph "$MG_PATH" --vendor neo4j "$NEO_PATH" \
    --dataset-name ldbc_interactive --dataset-group interactive  --dataset-size sf0.1 \
    --num-workers-for-benchmark "$workers"
}

ldbc_interactive_sf1 () {
  workers=$1
  echo "running ${FUNCNAME[0]} with $workers client workers"
  python3 graph_bench.py --vendor memgraph "$MG_PATH" --vendor neo4j "$NEO_PATH" \
    --dataset-name ldbc_interactive --dataset-group interactive  --dataset-size sf1 \
    --num-workers-for-benchmark "$workers"
}

ldbc_bi_sf1 () {
  workers=$1
  echo "running ${FUNCNAME[0]} with $workers client workers"
  python3 graph_bench.py --vendor memgraph "$MG_PATH" --vendor neo4j "$NEO_PATH" \
    --dataset-name ldbc_bi --dataset-group bi  --dataset-size sf1 \
    --num-workers-for-benchmark "$workers"
}

ldbc_interactive_sf3 () {
  workers=$1
  echo "running ${FUNCNAME[0]} with $workers client workers"
  python3 graph_bench.py --vendor memgraph "$MG_PATH" --vendor neo4j "$NEO_PATH" \
    --dataset-name ldbc_interactive --dataset-group interactive  --dataset-size sf3 \
    --num-workers-for-benchmark "$workers"
}

ldbc_bi_sf3 () {
  workers=$1
  echo "running ${FUNCNAME[0]} with $workers client workers"
  python3 graph_bench.py --vendor memgraph "$MG_PATH" --vendor neo4j "$NEO_PATH" \
    --dataset-name ldbc_bi --dataset-group bi  --dataset-size sf3 \
    --num-workers-for-benchmark "$workers"
}

run_all () {
  for workload in "${WORKLOADS[@]}"; do
    for workers in "${WORKERS[@]}"; do
      $workload "$workers"
      sleep 1
    done
  done
}

package_all_results () {
  cat /proc/cpuinfo > cpu.sysinfo
  cat /proc/meminfo > mem.sysinfo
  zip data.zip ./*.json ./*.report ./*.log ./*.sysinfo
}

clean_all_results () {
  rm data.zip ./*.json ./*.report ./*.log ./*.sysinfo
}

if [ "$#" -eq 0 ]; then
  check_all_binaries
  read -p "Run all benchmarks? y|Y for YES, anything else NO " -n 1 -r
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    run_all
  fi
elif [ "$#" -eq 1 ]; then
  case $1 in
    run_all)
      run_all
    ;;
    zip)
      package_all_results
    ;;
    clean)
      clean_all_results
    ;;
    *)
      print_help
    ;;
  esac
else
  print_help
fi
