#!/bin/bash
# TODO(gitbuda): Setup mgclient and pymgclient properly.
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../../libs/mgclient/lib

print_help() {
  echo -e "$0 ["workload name string"]"
  echo -e ""
  echo -e "  NOTE: some tests require enterprise licence key,"
  echo -e "        to run those define the folowing env vars:"
  echo -e "          * MEMGRAPH_ORGANIZATION_NAME"
  echo -e "          * MEMGRAPH_ENTERPRISE_LICENSE"
  exit 1
}
check_license() {
  if [ ! -v MEMGRAPH_ORGANIZATION_NAME ] || [ ! -v MEMGRAPH_ENTERPRISE_LICENSE ]; then
    echo "NOTE: MEMGRAPH_ORGANIZATION_NAME or MEMGRAPH_ENTERPRISE_LICENSE NOT defined -> dependent tests will NOT work"
  fi
}

if [ "$#" -eq 0 ]; then
  check_license
  # NOTE: If you want to run all tests under specific folder/section just
  # replace the dot (root directory below) with the folder name, e.g.
  # `--workloads-root-directory replication`.
  python3 runner.py --workloads-root-directory .
elif [ "$#" -eq 1 ]; then
  if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    print_help
  fi
  check_license
  # NOTE: --workload-name comes from each individual folder/section
  # workloads.yaml file. E.g. `streams/workloads.yaml` has a list of
  # `workloads:` and each workload has it's `-name`.
  python3 runner.py --workloads-root-directory . --workload-name "$1"
else
  print_help
fi
