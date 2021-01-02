#!/bin/bash

# shellcheck disable=1091
set -Eeuo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PIP_DEPS=(
   "behave==1.2.6"
   "ldap3==2.6"
   "neo4j-driver==4.1.1"
   "parse==1.18.0"
   "parse-type==0.5.2"
   "pyyaml==5.3.1"
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

for pkg in "${PIP_DEPS[@]}"; do
    pip --timeout 1000 install "$pkg"
done

# Install mgclient from source becasue of full flexibility.
pushd "$DIR/../libs/pymgclient" > /dev/null
python3 setup.py build
python3 setup.py install
popd > /dev/null

deactivate
