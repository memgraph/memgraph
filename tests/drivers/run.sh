#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"
pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }
# https://stackoverflow.com/questions/59119904/process-terminated-couldnt-find-a-valid-icu-package-installed-on-the-system-in
export DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

function wait_for_server {
    port=$1
    while ! nc -z -w 1 127.0.0.1 $port; do
        sleep 0.1
    done
    sleep 1
}

# Create a temporary directory.
tmpdir=/tmp/memgraph_drivers
if [ -d $tmpdir ]; then
    rm -rf $tmpdir
fi
mkdir -p $tmpdir

# Find memgraph binaries.
mg_binary_dir="$DIR/../../build"

# Start memgraph.
$mg_binary_dir/memgraph \
    --cartesian-product-enabled=false \
    --data-directory=$tmpdir \
    --query-execution-timeout-sec=5 \
    --storage-access-timeout-sec=1 \
    --log-file=$tmpdir/logs/memgraph.log \
    --also-log-to-stderr \
    --telemetry-enabled=false \
    --log-level ERROR &
pid=$!
wait_for_server 7687

# Run all available tests
DISABLE_NODE=${DISABLE_NODE:-false}
code_test=0
for i in *; do
    if [ ! -d $i ]; then continue; fi

    if [[ ("$i" = "node" || "$i" = "javascript") && "$DISABLE_NODE" = "true" ]]; then
        echo "Skipping Node.js driver tests in CI"
        continue
    fi

    pushd $i
    echo "Running: $i"
    # run all versions
    for v in *; do
        #skip v1 (needs different server name)
        if [[ "$v" == "v1" || ! -d "$v" ]]; then continue; fi
        pushd $v
        echo "Running version: $v"
        ./run.sh
        code_test=$?
        if [ $code_test -ne 0 ]; then
            echo "FAILED: $i"
            break
        fi
        popd
    done;
    echo
    popd
done

# Stop memgraph.
kill $pid
wait $pid
code_mg=$?

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
