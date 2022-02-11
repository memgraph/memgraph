#!/bin/bash

# shellcheck disable=1091
set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PIP_DEPS=(
   "behave==1.2.6"
   "ldap3==2.6"
   "kafka-python==2.0.2"
   "requests==2.25.1"
   "neo4j-driver==4.1.1"
   "parse==1.18.0"
   "parse-type==0.5.2"
   "pytest==6.2.3"
   "pyyaml==5.4.1"
   "six==1.15.0"
)
cd "$DIR"

# Remove old virtualenv.
if [ -d ve3 ]; then
    rm -rf ve3
fi

# Create new virtualenv.
virtualenv -p python3 ve3
set +u
source "ve3/bin/activate"
set -u

# https://docs.python.org/3/library/sys.html#sys.version_info
PYTHON_MINOR=$(python3 -c 'import sys; print(sys.version_info[:][1])')

# install pulsar-client
# NOTE (2021-11-15): PyPi doesn't contain pulsar-client for Python 3.9 so we have to use
# our manually built wheel file. When they update the repository, pulsar-client can be
# added as a regular PIP dependancy
if [ $PYTHON_MINOR -lt 9 ]; then
  pip --timeout 1000 install "pulsar-client==2.8.1"
else
  pip --timeout 1000 install https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/pulsar_client-2.8.1-cp39-cp39-manylinux_2_5_x86_64.manylinux1_x86_64.whl
fi

for pkg in "${PIP_DEPS[@]}"; do
    pip --timeout 1000 install "$pkg"
done

# Install mgclient from source becasue of full flexibility.
pushd "$DIR/../libs/pymgclient" > /dev/null
export MGCLIENT_INCLUDE_DIR="$DIR/../libs/mgclient/include"
export MGCLIENT_LIB_DIR="$DIR/../libs/mgclient/lib"
CFLAGS="-std=c99" python3 setup.py build
CFLAGS="-std=c99" python3 setup.py install
popd > /dev/null

deactivate
