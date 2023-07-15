#!/bin/bash

print_help() {
  echo -e "$0 ["workload name string"]"
  exit 1
}
# TODO(gitbuda): Setup mgclient and pymgclient properly.
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../../libs/mgclient/lib
# TODO(gitbuda): Mention / read the license key here.

if [ "$#" -eq 0 ]; then
  # NOTE: If you want to run all tests under specific folder/section just
  # replace the dot (root directory below) with the folder name, e.g.
  # `--workloads-root-directory replication`
  python runner.py --workloads-root-directory .
elif [ "$#" -eq 1 ]; then
  if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    print_help
  fi
  python3 runner.py --workloads-root-directory . --workload-name "$1"
else
  print_help
fi
