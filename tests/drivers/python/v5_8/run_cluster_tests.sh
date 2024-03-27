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
python3 write_routing.py || exit 1
python3 read_routing.py || exit 1
