#!/bin/bash -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# TODO(gitbuda): Add the picker to it's possible to run a single test.

cd $DIR
for name in *; do
  if [ ! -d $name ]; then continue; fi
  pushd $name >/dev/null
  echo "Running: $name"
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
done
