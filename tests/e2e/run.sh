#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

print_help() {
  echo -e "$0 ["workload name string"]"
  echo -e ""
  echo -e "  NOTE: some tests require enterprise licence key,"
  echo -e "        to run those define the folowing env vars:"
  echo -e "          * MEMGRAPH_ORGANIZATION_NAME"
  echo -e "          * MEMGRAPH_ENTERPRISE_LICENSE"
  exit 1
}

# Only viable inputs are none or one name argument (special case for help)
if [ "$#" -ge 1 ] && ( [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$#" -gt 1 ] ); then
  print_help
  return 0
fi

source "$SCRIPT_DIR/../util.sh"
setup_node

source "$SCRIPT_DIR/../../build/tests/enterprise_check.sh"
local mode=-1
check_mode
mode_str=$(if [ $mode -eq 0 ]; then echo "enterprise"; else echo "community"; fi)

if [ "$#" -eq 0 ]; then
  # NOTE: If you want to run all tests under specific folder/section just
  # replace the dot (root directory below) with the folder name, e.g.
  # `--workloads-root-directory replication`.
  python3 runner.py --workloads-root-directory "$SCRIPT_DIR/../../build/tests/e2e" --workload-type "$mode_str"
else # with a single name argument
  # NOTE: --workload-name comes from each individual folder/section
  # workloads.yaml file. E.g. `streams/workloads.yaml` has a list of
  # `workloads:` and each workload has it's `-name`.
  python3 runner.py --workloads-root-directory "$SCRIPT_DIR/../../build/tests/e2e" --workload-name "$1" --workload-type "$mode_str"
fi
