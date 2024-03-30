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

# create a temporary directory.
tmpdir=/tmp/memgraph_drivers
if [ -d $tmpdir ]; then
    rm -rf $tmpdir
fi

mkdir -p $tmpdir

# find memgraph binaries.
binary_dir="$DIR/../../build"

# Start instance_1
$binary_dir/memgraph \
    --bolt-port=7687 \
    --data-directory=$tmpdir/instance_1/ \
    --query-execution-timeout-sec=5 \
    --bolt-session-inactivity-timeout=10 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --bolt-cert-file="" \
    --log-file=$tmpdir/logs/instance1.log \
    --also-log-to-stderr \
    --management-port=10011 \
    --experimental-enabled=high-availability \
    --telemetry-enabled=false \
    --log-level ERROR &
pid_instance_1=$!
wait_for_server 7687

# Start instance_2
$binary_dir/memgraph \
    --bolt-port=7688 \
    --data-directory=$tmpdir/instance_2 \
    --query-execution-timeout-sec=5 \
    --bolt-session-inactivity-timeout=10 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --bolt-cert-file="" \
    --log-file=$tmpdir/logs/instance2.log \
    --also-log-to-stderr \
    --management-port=10012 \
    --experimental-enabled=high-availability \
    --telemetry-enabled=false \
    --log-level ERROR &
pid_instance_2=$!
wait_for_server 7688

# Start instance_3
$binary_dir/memgraph \
    --bolt-port=7689 \
    --data-directory=$tmpdir/instance_3 \
    --query-execution-timeout-sec=5 \
    --bolt-session-inactivity-timeout=10 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --bolt-cert-file="" \
    --log-file=$tmpdir/logs/instance3.log \
    --also-log-to-stderr \
    --management-port=10013 \
    --experimental-enabled=high-availability \
    --telemetry-enabled=false \
    --log-level ERROR &
pid_instance_3=$!
wait_for_server 7689


# Start coordinator_1
$binary_dir/memgraph \
    --bolt-port=7690 \
    --data-directory=$tmpdir/coordinator_1 \
    --query-execution-timeout-sec=5 \
    --bolt-session-inactivity-timeout=10 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --bolt-cert-file="" \
    --log-file=$tmpdir/logs/coordinator1.log \
    --also-log-to-stderr \
    --coordinator-id=1 \
    --coordinator-port=10111 \
    --experimental-enabled=high-availability \
    --telemetry-enabled=false \
    --log-level ERROR &
pid_coordinator_1=$!
wait_for_server 7690

# Start coordinator_2
$binary_dir/memgraph \
    --bolt-port=7691 \
    --data-directory=$tmpdir/coordinator_2 \
    --query-execution-timeout-sec=5 \
    --bolt-session-inactivity-timeout=10 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --bolt-cert-file="" \
    --log-file=$tmpdir/logs/coordinator2.log \
    --also-log-to-stderr \
    --coordinator-id=2 \
    --coordinator-port=10112 \
    --experimental-enabled=high-availability \
    --telemetry-enabled=false \
    --log-level ERROR &
pid_coordinator_2=$!
wait_for_server 7691

# Start coordinator_3
$binary_dir/memgraph \
    --bolt-port=7692 \
    --data-directory=$tmpdir/coordinator_3 \
    --query-execution-timeout-sec=5 \
    --bolt-session-inactivity-timeout=10 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --bolt-cert-file="" \
    --log-file=$tmpdir/logs/coordinator3.log \
    --also-log-to-stderr \
    --coordinator-id=3 \
    --coordinator-port=10113 \
    --experimental-enabled=high-availability \
    --telemetry-enabled=false \
    --log-level ERROR &
pid_coordinator_3=$!
wait_for_server 7692

sleep 5

echo 'ADD COORDINATOR 2 WITH CONFIG {"bolt_server": "127.0.0.1:7691", "coordinator_server":  "127.0.0.1:10112"};' | $binary_dir/bin/mgconsole --port 7690
echo 'ADD COORDINATOR 3 WITH CONFIG {"bolt_server": "127.0.0.1:7692", "coordinator_server":  "127.0.0.1:10113"};' | $binary_dir/bin/mgconsole --port 7690
echo 'REGISTER INSTANCE instance_1 WITH CONFIG {"bolt_server": "127.0.0.1:7687", "management_server": "127.0.0.1:10011", "replication_server": "127.0.0.1:10001"};'  | $binary_dir/bin/mgconsole --port 7690
echo 'REGISTER INSTANCE instance_2 WITH CONFIG {"bolt_server": "127.0.0.1:7688", "management_server": "127.0.0.1:10012", "replication_server": "127.0.0.1:10002"};'  | $binary_dir/bin/mgconsole --port 7690
echo 'REGISTER INSTANCE instance_3 WITH CONFIG {"bolt_server": "127.0.0.1:7689", "management_server": "127.0.0.1:10013", "replication_server": "127.0.0.1:10003"};'  | $binary_dir/bin/mgconsole --port 7690
echo 'SET INSTANCE instance_1 TO MAIN;' | $binary_dir/bin/mgconsole --port 7690


code_test=0
for lang in *; do
    if [ ! -d $lang ]; then continue; fi
    pushd $lang
    echo "Running tests for language: $lang"
    for version in *; do
        if [ ! -d $version ]; then continue; fi
        pushd $version
        if [ -f "run_cluster_tests.sh" ]; then
            echo "Running version: $version"
            ./run_cluster_tests.sh
            code_test=$?
            if [ $code_test -ne 0 ]; then
                echo "FAILED: $lang-$version"
                break
            fi
        fi
        popd
    done;
    popd
done


# Function to stop a process by PID and check its exit code
stop_process() {
    local pid=$1 # Capture the PID from the first argument

    # Stop the process
    kill $pid
    wait $pid
    local exit_code=$? # Capture the exit code

    # Check the process's exit code
    if [ $exit_code -ne 0 ]; then
        echo "The process with PID $pid didn't terminate properly!"
        exit $exit_code
    else
        echo "Process with PID $pid terminated successfully."
    fi
}

echo "Stopping coordinator1"
stop_process $pid_coordinator_1
echo "Stopping coordinator2"
stop_process $pid_coordinator_2
echo "Stopping coordinator3"
stop_process $pid_coordinator_3

echo "Stopping instance1"
stop_process $pid_instance_1
echo "Stopping instance2"
stop_process $pid_instance_2
echo "Stopping instance3"
stop_process $pid_instance_3


# Check test exit code.
if [ $code_test -ne 0 ]; then
    echo "One of the tests failed!"
    exit $code_test
fi

# Temporary directory cleanup.
if [ -d $tmpdir ]; then
    rm -rf $tmpdir
fi
