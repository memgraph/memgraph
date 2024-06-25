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
    wget -nv https://files.pythonhosted.org/packages/cf/7d/32204b1c2d6f9f9d729bbf8273515c2b3ef42c0b723617d319f3e435f69e/neo4j-driver-4.1.1.tar.gz || exit 1
    tar -xzf neo4j-driver-4.1.1.tar.gz || exit 1
    mv neo4j-driver-4.1.1 neo4j-driver || exit 1
    virtualenv -p python3 ve3 || exit 1
    source ve3/bin/activate
    cd neo4j-driver
    python3 setup.py install || exit 1
    cd ..
    deactivate
    rm -rf neo4j-driver neo4j-driver-4.1.1.tar.gz || exit 1
fi

# activate virtualenv
source ve3/bin/activate

# execute test
python3 docs_how_to_query.py || exit 1
python3 max_query_length.py || exit 1
python3 transactions.py || exit 1
python3 metadata.py || exit 1
python3 multi_tenancy.py || exit 1
# python3 parallel_edge_import.py || exit 1
