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

# Create a temporary directory.
tmpdir=/tmp/memgraph_benchmark_kafka
if [ -d $tmpdir ]; then
    rm -rf $tmpdir
fi
mkdir -p $tmpdir
cd $tmpdir

# Download the kafka binaries.
kafka="kafka_2.11-2.0.0"
wget -nv http://deps.memgraph.io/$kafka.tgz
tar -xf $kafka.tgz
mv $kafka kafka

# Find memgraph binaries.
binary_dir="$DIR/../../../build"
if [ ! -d $binary_dir ]; then
    binary_dir="$DIR/../../../build_release"
fi

# Cleanup old kafka logs.
if [ -d /tmp/kafka-logs ]; then
    rm -rf /tmp/kafka-logs
fi
if [ -d /tmp/zookeeper ]; then
    rm -rf /tmp/zookeeper
fi

# Results for apollo
RESULTS="$DIR/.apollo_measurements"

# Benchmark parameters
NODES=100000
EDGES=10000


## Startup

# Start the zookeeper process and wait for it to start.
echo_info "Starting zookeeper"
./kafka/bin/zookeeper-server-start.sh -daemon kafka/config/zookeeper.properties
wait_for_server 2181
echo_success "Started zookeeper"

# Start the kafka process and wait for it to start.
echo_info "Starting kafka"
./kafka/bin/kafka-server-start.sh -daemon kafka/config/server.properties
wait_for_server 9092
echo_success "Started kafka"

# Create the kafka topic.
echo_info "Creating kafka topic test"
./kafka/bin/kafka-topics.sh --create \
    --zookeeper 127.0.0.1:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic test
echo_success "Created kafka topic test"

# Start a http server to serve the transform script.
echo_info "Starting Python HTTP server"
mkdir serve
cd serve
cp "$DIR/transform.py" transform.py
python3 -m http.server &
http_server_pid=$!
wait_for_server 8000
cd ..
echo_success "Started Python HTTP server"


# Start the memgraph process and wait for it to start.
echo_info "Starting kafka benchmark"
$binary_dir/tests/feature_benchmark/kafka/benchmark \
    --import-count=$(($NODES + $EDGES)) \
    --timeout=60 \
    --kafka-uri="127.0.0.1:9092" \
    --topic-name="test" \
    --transform-uri="127.0.0.1:8000/transform.py" \
    --output-file=$RESULTS &
pid=$!

# Allow benchmark to initialize
sleep 5

echo_info "Generating kafka messages"
python3 "$DIR/generate.py" --nodes $NODES --edges $EDGES | \
    ./kafka/bin/kafka-console-producer.sh \
        --broker-list 127.0.0.1:9092 \
        --topic test \
        > /dev/null
echo_success "Finished generating kafka messages"

wait -n $pid
code=$?

if [ $code -eq 0 ]; then
    echo_success "Benchmark finished successfully"
else
    echo_failure "Benchmark didn't finish successfully"
fi

## Cleanup

echo_info "Starting test cleanup"

# Shutdown the http server.
kill $http_server_pid
wait -n

# Shutdown the kafka process.
./kafka/bin/kafka-server-stop.sh

# Shutdown the zookeeper process.
./kafka/bin/zookeeper-server-stop.sh

echo_success "Test cleanup done"

[ $code -ne 0 ] && exit $code
exit 0
