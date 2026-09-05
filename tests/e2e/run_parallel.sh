#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

print_help() {
  echo "    run_parallel args   1. Optional positional argument, number of processes (default: nproc)"
  echo "                        2. Optional positional argument, workload name as string"
  echo "                        3. Optional extra flags passed to runner_parallel.py"
  echo "                           (e.g. --save-data-dir, --keep-going, --port-offset-step N)"
  echo -e ""
  echo -e "  Every process runs workloads on its own range of ports; only tests that compare SHOW CONFIG"
  echo -e "  against the defaults run one at a time on the real ports."
  echo -e ""
  echo -e "  Examples:"
  echo -e "    ./run_parallel.sh"
  echo -e "    ./run_parallel.sh 4"
  echo -e "    ./run_parallel.sh 6 \"GraphQL sorting\" --save-data-dir"
  echo -e "    ./run_parallel.sh 8 --keep-going"
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
# Enabled after setup_node: the nvm scripts it sources are not written for errexit.
set -eo pipefail

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
  print_help
fi

# A leading integer is the process count, anything else leaves the default and is parsed as the workload name.
NPROCESSES="$(nproc)"
if [[ "$1" =~ ^[0-9]+$ ]]; then
  NPROCESSES="$1"
  shift
fi

if [ "$NPROCESSES" -lt 1 ]; then
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
