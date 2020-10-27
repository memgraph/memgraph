#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

if ! which node >/dev/null; then
    echo "Please install nodejs!"
    exit 1
fi

if [ ! -d node_modules ]; then
    # Driver generated with: `npm install neo4j-driver`
    wget -nv https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/drivers/javascript/neo4j-javascript-driver-1.6.1.tar.gz -O driver.tar.gz || exit 1
    tar -xzf driver.tar.gz || exit 1
    rm driver.tar.gz || exit 1
fi

node basic.js
node max_query_length.js
