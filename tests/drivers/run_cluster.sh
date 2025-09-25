#!/bin/bash
pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }
# https://stackoverflow.com/questions/59119904/process-terminated-couldnt-find-a-valid-icu-package-installed-on-the-system-in
export DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
declare -a pids=()

function cleanup() {
    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping background process with PID $pid (it was still running)"
            kill "$pid"
            wait "$pid" 2>/dev/null
        fi
    done
}

trap cleanup EXIT INT TERM

function check_ports_unused() {
    for port in "$@"; do
        if nc -z -w 1 127.0.0.1 "$port" >/dev/null 2>&1; then
            echo "Port $port is in use."
            exit 1
        fi
    done
}

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
mg_binary_dir="$DIR/../../build"

# Start instance_1
check_ports_unused 7687 10011
$mg_binary_dir/memgraph \
    --bolt-port=7687 \
    --data-directory=$tmpdir/instance_1/ \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --log-file=$tmpdir/logs/instance1.log \
    --also-log-to-stderr \
    --management-port=10011 \
    --telemetry-enabled=false \
    --storage-wal-enabled=true \
    --log-level TRACE &

pid_instance_1=$!
wait_for_server 7687
pids+=($pid_instance_1)

# Start instance_2
check_ports_unused 7688 10012
$mg_binary_dir/memgraph \
    --bolt-port=7688 \
    --data-directory=$tmpdir/instance_2 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --log-file=$tmpdir/logs/instance2.log \
    --also-log-to-stderr \
    --management-port=10012 \
    --telemetry-enabled=false \
    --storage-wal-enabled=true \
    --log-level TRACE &

pid_instance_2=$!
wait_for_server 7688
pids+=($pid_instance_2)

# Start instance_3
check_ports_unused 7689 10013
$mg_binary_dir/memgraph \
    --bolt-port=7689 \
    --data-directory=$tmpdir/instance_3 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --log-file=$tmpdir/logs/instance3.log \
    --also-log-to-stderr \
    --management-port=10013 \
    --telemetry-enabled=false \
    --storage-wal-enabled=true \
    --log-level TRACE &

pid_instance_3=$!
wait_for_server 7689
pids+=($pid_instance_3)


# Start coordinator_1
check_ports_unused 7690 10121 10111
$mg_binary_dir/memgraph \
    --bolt-port=7690 \
    --data-directory=$tmpdir/coordinator_1 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --log-file=$tmpdir/logs/coordinator1.log \
    --also-log-to-stderr \
    --coordinator-id=1 \
    --coordinator-port=10111 \
    --coordinator-hostname="127.0.0.1" \
    --management-port=10121 \
    --telemetry-enabled=false \
    --log-level TRACE &

pid_coordinator_1=$!
wait_for_server 7690
pids+=($pid_coordinator_1)

# Start coordinator_2
check_ports_unused 7691 10122 10112
$mg_binary_dir/memgraph \
    --bolt-port=7691 \
    --data-directory=$tmpdir/coordinator_2 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --log-file=$tmpdir/logs/coordinator2.log \
    --also-log-to-stderr \
    --coordinator-id=2 \
    --coordinator-port=10112 \
    --coordinator-hostname="127.0.0.1" \
    --management-port=10122 \
    --telemetry-enabled=false \
    --log-level TRACE &

pid_coordinator_2=$!
wait_for_server 7691
pids+=($pid_coordinator_2)

# Start coordinator_3
check_ports_unused 7692 10123 10113
$mg_binary_dir/memgraph \
    --bolt-port=7692 \
    --data-directory=$tmpdir/coordinator_3 \
    --bolt-server-name-for-init="Neo4j/1.1" \
    --log-file=$tmpdir/logs/coordinator3.log \
    --also-log-to-stderr \
    --coordinator-id=3 \
    --coordinator-port=10113 \
    --management-port=10123 \
    --coordinator-hostname="127.0.0.1" \
    --telemetry-enabled=false \
    --log-level TRACE &

pid_coordinator_3=$!
wait_for_server 7692
pids+=($pid_coordinator_3)

sleep 5

max_retries=5

run_command() {
  local cmd=$1
  local attempt=0

  while [ $attempt -le $max_retries ]; do
    eval $cmd
    if [ $? -eq 0 ]; then
      return 0
    fi
    ((attempt++))
    sleep 1
  done

  echo "Failed to run command: $cmd after $max_retries attempts."

  return 1

}

# NOTE: Since v6 of toolchain mgconsole should be included.
mgconsole_bin=mgconsole
if ! which $mgconsole_bin > /dev/null; then
    mgconsole_bin="$mg_binary_dir/bin/mgconsole"
fi
commands=(
    "echo 'ADD COORDINATOR 1 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7690\", \"coordinator_server\":  \"127.0.0.1:10111\", \"management_server\": \"127.0.0.1:10121\"};' | $mgconsole_bin --port 7690"
    "echo 'ADD COORDINATOR 2 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7691\", \"coordinator_server\":  \"127.0.0.1:10112\", \"management_server\": \"127.0.0.1:10122\"};' | $mgconsole_bin --port 7690"
    "echo 'ADD COORDINATOR 3 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7692\", \"coordinator_server\":  \"127.0.0.1:10113\", \"management_server\": \"127.0.0.1:10123\"};' | $mgconsole_bin --port 7690"
    "echo 'REGISTER INSTANCE instance_1 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7687\", \"management_server\": \"127.0.0.1:10011\", \"replication_server\": \"127.0.0.1:10001\"};' | $mgconsole_bin --port 7690"
    "echo 'REGISTER INSTANCE instance_2 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7688\", \"management_server\": \"127.0.0.1:10012\", \"replication_server\": \"127.0.0.1:10002\"};' | $mgconsole_bin --port 7690"
    "echo 'REGISTER INSTANCE instance_3 WITH CONFIG {\"bolt_server\": \"127.0.0.1:7689\", \"management_server\": \"127.0.0.1:10013\", \"replication_server\": \"127.0.0.1:10003\"};' | $mgconsole_bin --port 7690"
    "echo 'SET INSTANCE instance_1 TO MAIN;' | $mgconsole_bin --port 7690"
)

for cmd in "${commands[@]}"; do
    run_command "$cmd"
    if [ $? -ne 0 ]; then
        echo "Failed to run command: $cmd"
        exit 1
    fi
done

sleep 1

instances=$(echo "SHOW INSTANCES;" | $mgconsole_bin --port 7690)

num_leaders=$(echo "$instances" | grep -c "leader")
if [ "$num_leaders" -ne 1 ]; then
    echo "Expected 1 leader after registration, got $num_leaders"
    exit 1
fi

num_followers=$(echo "$instances" | grep -c "follower")

if [ "$num_followers" -ne 2 ]; then
    echo "Expected 2 followers after registration, got $num_followers"
    exit 1
fi

num_mains=$(echo "$instances" | grep -c "main")

if [ "$num_mains" -ne 1 ]; then
    echo "Expected 1 main after registration, got $num_mains"
    exit 1
fi

num_replicas=$(echo "$instances" | grep -c "replica")

if [ "$num_replicas" -ne 2 ]; then
    echo "Expected 2 replicas after registration, got $num_replicas"
    exit 1
fi

echo "All instances registered successfully."

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

tar -zcvf test_report.tar.gz $tmpdir/logs/*.log

# Temporary directory cleanup.
if [ -d $tmpdir ]; then
    rm -rf $tmpdir
fi

# Check test exit code.
if [ $code_test -ne 0 ]; then
    echo "One of the tests failed!"
    exit $code_test
fi
