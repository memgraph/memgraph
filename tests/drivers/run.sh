#!/bin/bash

pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }

function wait_for_server {
    port=$1
    while ! nc -z -w 1 127.0.0.1 $port; do
        sleep 0.1
    done
    sleep 1
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# Create a temporary directory.
tmpdir=/tmp/memgraph_drivers
if [ -d $tmpdir ]; then
    rm -rf $tmpdir
fi
mkdir -p $tmpdir

# Find memgraph binaries.
binary_dir="$DIR/../../build"
if [ ! -d $binary_dir ]; then
    binary_dir="$DIR/../../build_debug"
fi

# Start memgraph.
$binary_dir/memgraph \
    --durability-directory=$tmpdir \
    --query-execution-timeout-sec=5 \
    --session-inactivity-timeout=10 &
pid=$!
wait_for_server 7687

# Run all available tests
code_test=0
for i in *; do
    if [ ! -d $i ]; then continue; fi
    pushd $i
    echo "Running: $i"
    ./run.sh
    code_test=$?
    if [ $code_test -ne 0 ]; then
        echo "FAILED: $i"
        break
    fi
    echo
    popd
done

# Stop memgraph.
kill $pid
wait -n
code_mg=$?

# Temporary directory cleanup.
if [ -d $tmpdir ]; then
    rm -rf $tmpdir
fi

# Check memgraph exit code.
if [ $code_mg -ne 0 ]; then
    echo "The memgraph process didn't terminate properly!"
    exit $code_mg
fi

# Check test exit code.
if [ $code_test -ne 0 ]; then
    echo "One of the tests failed!"
    exit $code_test
fi
