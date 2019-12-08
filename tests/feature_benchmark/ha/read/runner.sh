#!/bin/bash

## Helper functions

function wait_for_server {
    port=$1
    while ! nc -z -w 1 127.0.0.1 $port; do
        sleep 0.1
    done
    sleep 1
}

function echo_info { printf "\033[1;36m~~ $1 ~~\033[0m\n"; }
function echo_success { printf "\033[1;32m~~ $1 ~~\033[0m\n\n"; }
function echo_failure { printf "\033[1;31m~~ $1 ~~\033[0m\n\n"; }

## Environment setup

# Get script location.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# Find memgraph binaries.
binary_dir="$DIR/../../../../build"
if [ ! -d $binary_dir ]; then
    binary_dir="$DIR/../../../../build_release"
fi

# Results for apollo
RESULTS="$DIR/.apollo_measurements"

# Benchmark parameters
DURATION=10

# Startup
declare -a HA_PIDS
declare -a exit_codes

for server_id in 1 2 3
do
  $binary_dir/memgraph_ha --server_id $server_id \
    --coordination_config_file="coordination.json" \
    --raft_config_file="raft.json" \
    --bolt-port $((7686 + $server_id)) \
    --db-recover-on-startup=false \
    --durability_directory=dur$server_id &
  HA_PIDS[$server_id]=$!
  wait_for_server $((7686 + $server_id))
done

# Allow some time for leader election.
sleep 10

# Start the memgraph process and wait for it to start.
echo_info "Starting HA read benchmark"
$binary_dir/tests/feature_benchmark/ha/read/benchmark \
    --duration=$DURATION \
    --output-file=$RESULTS &
pid=$!

wait -n $pid
exit_codes[0]=$?

# Shutdown
for server_id in 1 2 3
do
  kill -15 ${HA_PIDS[$server_id]}
done

# Cleanup
for server_id in 1 2 3
do
  wait -n ${HA_PIDS[$server_id]}
  exit_codes[$server_id]=$?
  rm -r dur$server_id
done

failure=0

for i in `seq 0 3`; do
  code=${exit_codes[$i]}
  if [ $code -ne 0 ]; then
    if [ $i -eq 0 ]; then
      echo_failure "Benchmark exited with status $code"
    else
      echo_failure "Memgraph HA server $i exited with status $code"
    fi

    failure=1
  fi
done

if [ $failure -eq 0 ]; then
    echo_success "Benchmark finished successfully"
else
    echo_failure "Benchmark didn't finish successfully"
fi

exit $failure
