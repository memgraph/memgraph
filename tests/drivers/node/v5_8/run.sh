#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

source "$DIR/../../../util.sh"
setup_node


if ! which node >/dev/null; then
    echo "Please install nodejs!"
    exit 1
fi

if [ ! -d node_modules ]; then
    pnpm install --frozen-lockfile
fi

node docs_how_to_query.js
node max_query_length.js
