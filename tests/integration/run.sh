#!/bin/bash -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

print_help() {
  echo -e "$0             => run all under tests/integration"
  echo -e "$0 folder_name => run single test under tests/integration"
  exit 1
}

test_one() {
  cd "$DIR"
  integration_test_folder_name="$1"
  pushd "$integration_test_folder_name" >/dev/null
  echo "Running: $integration_test_folder_name"
  if [ -x prepare.sh ]; then
    ./prepare.sh
  fi
  if [ -x runner.py ]; then
    ./runner.py
  elif [ -x runner.sh ]; then
    ./runner.sh
  fi
  echo
  popd >/dev/null
}

test_all() {
  cd "$DIR"
  for name in *; do
    if [ ! -d "$name" ]; then continue; fi
    test_one "$name"
  done
}

if [ "$#" -eq 0 ]; then
  test_all
else
  if [ "$#" -gt 1 ]; then
    print_help
  else
    if [ -d "$DIR/$1" ]; then
      test_one "$1"
    else
      print_help
    fi
  fi
fi
