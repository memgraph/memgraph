#!/bin/bash
# shellcheck disable=1091

set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

PIP_DEPS=(
   "behave==1.2.6"
   "ldap3==2.6"
   "kafka-python==2.0.2"
   "requests==2.25.1"
   "neo4j-driver==4.1.1"
   "parse==1.18.0"
   "parse-type==0.5.2"
   "pytest==7.3.2"
   "pyyaml==6.0.1"
   "six==1.15.0"
   "networkx==2.4"
   "gqlalchemy==1.3.3"
)

# Remove old and create a new virtualenv.
if [ -d ve3 ]; then
    rm -rf ve3
fi
virtualenv -p python3 ve3
set +u
source "ve3/bin/activate"
set -u

# https://docs.python.org/3/library/sys.html#sys.version_info
PYTHON_MINOR=$(python3 -c 'import sys; print(sys.version_info[:][1])')

# install pulsar-client
pip --timeout 1000 install "pulsar-client==3.1.0"
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

"$DIR"/e2e/graphql/setup.sh

# Check if setup needs to setup additional variables
if [ $# == 1 ]; then
    toolchain=$1
    if [ -f "$toolchain" ]; then
        # Get the LD_LIB from toolchain
        set +u
        OLD_LD_LIBRARY_PATH=$LD_LIBRARY_PATH
        source $toolchain
        NEW_LD_LIBRARY_PATH=$LD_LIBRARY_PATH
        deactivate
        set -u

        # Wrapper used to setup the correct libraries
        tee -a ve3/bin/activate_e2e <<EOF
#!/bin/bash

# Function to set the environment variable
set_env_variable() {
    export LD_LIBRARY_PATH=$NEW_LD_LIBRARY_PATH
}

# Function to activate the virtual environment and set the environment variable
activate_e2e() {
    source ve3/bin/activate
    set_env_variable
}

# Function to deactivate the virtual environment and unset the environment variable
deactivate_e2e() {
    deactivate
    export LD_LIBRARY_PATH=$OLD_LD_LIBRARY_PATH
}

# Activate the virtual environment and set the environment variable
activate_e2e
EOF

        chmod +x ve3/bin/activate_e2e
    else
        echo "Error: The toolchain virtual enviroonment activation is not a file."
    fi
fi
