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
    python3 -m pip install neo4j==5.8.0 setuptools==80.9.0 tomlkit==0.13.3 --no-build-isolation  || exit 1
    deactivate
fi

# activate virtualenv
source ve3/bin/activate

# execute test
python3 routing.py || exit 1
