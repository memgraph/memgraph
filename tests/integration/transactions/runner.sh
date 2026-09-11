#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

binary_dir="$DIR/../../../build"

# run-parallel.sh hands each test its own ports so concurrent memgraph instances don't collide.
bolt_port=${MG_INTEGRATION_BOLT_PORT:-7687}
monitoring_port=${MG_INTEGRATION_MONITORING_PORT:-7444}
metrics_port=${MG_INTEGRATION_METRICS_PORT:-9091}

# Start the memgraph process.
$binary_dir/memgraph --metrics-format=OpenMetrics --bolt-port=$bolt_port --monitoring-port=$monitoring_port --metrics-port=$metrics_port &
pid=$!

# Wait for the database to start up.
while ! nc -z -w 1 127.0.0.1 $bolt_port; do
    sleep 0.5
done

# Start the test on default db.
$binary_dir/tests/integration/transactions/tester --port $bolt_port
code=$?

# Start the test on another db.
$binary_dir/tests/integration/transactions/tester --port $bolt_port --use-db db1
code2=$?

# Shutdown the memgraph process.
kill $pid
wait $pid
code_mg=$?

# Check memgraph exit code.
if [ $code_mg -ne 0 ]; then
    echo "The memgraph process didn't terminate properly!"
    exit $code_mg
fi

# Exit with the exitcode of the test.
if [ $code -ne 0 ]; then
    echo "Default database tests failed!"
    exit $code
fi

if [ $code2 -ne 0 ]; then
    echo "Non default database tests failed!"
    exit $code2
fi
