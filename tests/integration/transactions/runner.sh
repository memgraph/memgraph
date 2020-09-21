#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

binary_dir="$DIR/../../../build"

# Start the memgraph process.
$binary_dir/memgraph &
pid=$!

# Wait for the database to start up.
while ! nc -z -w 1 127.0.0.1 7687; do
    sleep 0.5
done

# Start the test.
$binary_dir/tests/integration/transactions/tester
code=$?

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
exit $code
