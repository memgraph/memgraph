#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# system check
if ! which virtualenv >/dev/null; then
    echo "Please install virtualenv!"
    exit 1
fi

# setup virtual environment
if [ ! -d "ve3" ]; then
    virtualenv -p python3 ve3 || exit 1
    source ve3/bin/activate
    python3 -m pip install neo4j==5.8.0 || exit 1
    deactivate
fi

# activate virtualenv
source ve3/bin/activate

# execute test
python3 docs_how_to_query.py || exit 1
python3 max_query_length.py || exit 1
python3 transactions.py || exit 1
python3 path.py || exit 1
python3 server_name.py || exit 1
python3 multi_tenancy.py || exit 1
# python3 parallel_edge_import.py || exit 1
