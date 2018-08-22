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
    # Driver downloaded from: https://pypi.org/project/neo4j-driver/1.5.3/
    wget -nv http://deps.memgraph.io/drivers/python/neo4j-driver-1.5.3.tar.gz -O neo4j-driver.tar.gz || exit 1
    tar -xzf neo4j-driver.tar.gz || exit 1
    mv neo4j-driver-1.5.3 neo4j-driver || exit 1
    virtualenv -p python3 ve3 || exit 1
    source ve3/bin/activate
    cd neo4j-driver
    python3 setup.py install || exit 1
    cd ..
    deactivate
    rm -rf neo4j-driver neo4j-driver.tar.gz || exit 1
fi

# activate virtualenv
source ve3/bin/activate

# execute test
python3 basic.py || exit 1
python3 max_query_length.py || exit 1
python3 transactions.py || exit 1
