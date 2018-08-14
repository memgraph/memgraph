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
tmpdir=/tmp/memgraph_integration_kafka
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
    binary_dir="$DIR/../../../build_debug"
fi

# Cleanup old kafka logs.
if [ -d /tmp/kafka-logs ]; then
    rm -rf /tmp/kafka-logs
fi
if [ -d /tmp/zookeeper ]; then
    rm -rf /tmp/zookeeper
fi


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

# Start the memgraph process and wait for it to start.
echo_info "Starting memgraph"
$binary_dir/memgraph &
pid=$!
wait_for_server 7687
echo_success "Started memgraph"


## Run the test

# Create the kafka topic.
echo_info "Creating kafka topic"
./kafka/bin/kafka-topics.sh --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic test
echo_success "Created kafka topic"

# Start a http server to serve the transform script.
echo_info "Starting Python HTTP server"
mkdir serve
cd serve
cp "$DIR/transform.py" transform.py
python3 -m http.server &
wait_for_server 8000
http_pid=$!
cd ..
echo_success "Started Python HTTP server"

# Create and start the stream in memgraph.
echo_info "Defining and starting the stream in memgraph"
$binary_dir/tests/integration/kafka/tester --step start
code1=$?
if [ $code1 -eq 0 ]; then
    echo_success "Defined and started the stream in memgraph"
else
    echo_failure "Couldn't define and/or start the stream in memgraph"
fi

# Wait for the streams to start up.
sleep 10

# Produce some messages.
echo_info "Producing kafka messages"
./kafka/bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic test <<EOF
1
2
3
4
1 2
3 4
1 4
EOF
echo_success "Produced kafka messages"

# Wait for the messages to be consumed.
sleep 10

# Verify that the received graph is good.
echo_info "Checking the received graph in memgraph"
$binary_dir/tests/integration/kafka/tester --step verify
code2=$?
if [ $code2 -eq 0 ]; then
    echo_success "Checked the received graph in memgraph"
else
    echo_failure "Couldn't check the received graph in memgraph"
fi


## Cleanup

echo_info "Starting test cleanup"

# Shutdown the http server.
kill $http_pid
wait -n

# Shutdown the memgraph process.
kill $pid
wait -n
code_mg=$?

# Shutdown the kafka process.
./kafka/bin/kafka-server-stop.sh

# Shutdown the zookeeper process.
./kafka/bin/zookeeper-server-stop.sh

echo_success "Test cleanup done"

# Check memgraph exit code.
if [ $code_mg -ne 0 ]; then
    echo "The memgraph process didn't terminate properly!"
    exit $code_mg
fi

# Exit with the exitcode of the test.
[ $code1 -ne 0 ] && exit $code1
[ $code2 -ne 0 ] && exit $code2
exit 0
