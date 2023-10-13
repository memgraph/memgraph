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

# node docs_how_to_query.js
# node max_query_length.js
node parallel_edge_import.js
