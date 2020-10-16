#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

if ! which node >/dev/null; then
    echo "Please install nodejs!"
    exit 1
fi

if [ ! -d node_modules ]; then
    # Driver generated with: `npm install neo4j-driver`
    npm install neo4j-driver
fi

node basic.js
node max_query_length.js
