#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

print_help() {
  echo "    run_parallel args   1. Positional argument, number of processes (nprocesses)"
  echo "                        2. Optional positional argument, workload name as string"
  echo "                        3. Optional extra flags passed to runner_parallel.py (e.g. --save-data-dir)"
  echo -e ""
  echo -e "  Examples:"
  echo -e "    ./run_parallel.sh 4"
  echo -e "    ./run_parallel.sh 6 \"GraphQL sorting\" --save-data-dir"
  echo -e ""
  echo -e "  NOTE: some tests require enterprise licence key,"
  echo -e "        to run those define the following env vars:"
  echo -e "          * MEMGRAPH_ORGANIZATION_NAME"
  echo -e "          * MEMGRAPH_ENTERPRISE_LICENSE"
  exit 1
}

check_license() {
  if [ ! -v MEMGRAPH_ORGANIZATION_NAME ] || [ ! -v MEMGRAPH_ENTERPRISE_LICENSE ]; then
    echo "NOTE: MEMGRAPH_ORGANIZATION_NAME or MEMGRAPH_ENTERPRISE_LICENSE NOT defined -> dependent tests will NOT work"
  fi
}

source "$SCRIPT_DIR/../util.sh"
setup_node

if [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$#" -eq 0 ]; then
  print_help
fi

NPROCESSES="$1"
shift

if ! [[ "$NPROCESSES" =~ ^[0-9]+$ ]] || [ "$NPROCESSES" -lt 1 ]; then
  echo "ERROR: nprocesses must be a positive integer. Got: $NPROCESSES"
  exit 1
fi

check_license

WORKLOAD_NAME=""
if [ "$#" -ge 1 ] && [[ "$1" != --* ]]; then
  WORKLOAD_NAME="$1"
  shift
fi

RUNNER_ARGS=(
  --workloads-root-directory "$SCRIPT_DIR/../../build/tests/e2e"
  --nprocesses "$NPROCESSES"
)

if [ -n "$WORKLOAD_NAME" ]; then
  RUNNER_ARGS+=(--workload-name "$WORKLOAD_NAME")
fi

python3 runner_parallel.py "${RUNNER_ARGS[@]}" "$@"
