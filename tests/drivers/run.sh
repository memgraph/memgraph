#!/bin/bash

pushd () {
    command pushd "$@" > /dev/null
}

popd () {
    command popd "$@" > /dev/null
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

for i in *; do
    if [ ! -d $i ]; then continue; fi
    pushd $i
    echo "Running: $i"
    ./run.sh || exit 1
    echo
    popd
done
