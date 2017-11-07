#!/bin/bash

set -e

VIRTUALENV=virtualenv
PIP=pip
PYTHON=python
WORKING_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd ${WORKING_DIR}

# system check
if ! which $VIRTUALENV >/dev/null; then
    echo "Please install virtualenv!"
    exit 1
fi

# setup virtual environment
if [ ! -d "ve3" ]; then
    virtualenv -p python3 ve3 || exit 1
fi
source ve3/bin/activate
$PIP install --upgrade pip
$PIP install neo4j-driver==1.5.0

# execute test
$PYTHON basic.py
$PYTHON max_query_length.py
$PYTHON transactions.py
