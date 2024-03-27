#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

if ! which node >/dev/null; then
    echo "Please install nodejs!"
    exit 1
fi

if [ ! -d node_modules ]; then
    # Driver generated with: `npm install neo4j-driver`
    npm install --no-package-lock --no-save neo4j-driver@5.8.0
fi

node write_routing.js
node read_routing.js
