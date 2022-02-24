#!/bin/bash

# Read the user given arguments
ARGS=$@
# Set the default bolt address
BOLT_ADDRESS="0.0.0.0"
# Set the default port
BOLT_PORT="7687"
# Read the configuration @ /etc/memgraph/memgraph.conf
CONFIG_FILE="/etc/memgraph/memgraph.conf"
# Default path for init cypher scripts
DATA_DIR="/usr/lib/memgraph/init"

function _extract_port {
    OTHER_BOLT_ADDRESS="$(perl -ne 'print "$1" while /^--bolt-address=(.*)/g' $CONFIG_FILE)"
    OTHER_BOLT_PORT="$(perl -ne 'print "$1" while /^--bolt-port=(.*)/g' $CONFIG_FILE)"

    if [ -n "$OTHER_BOLT_ADDRESS" ]; then
        BOLT_ADDRESS=$OTHER_BOLT_ADDRESS
    fi

    if [ -n "$OTHER_BOLT_PORT" ]; then
        BOLT_PORT=$OTHER_BOLT_PORT
    fi
}

function _read_other_config {
    # Read the other config
    OTHER_CONFIG="$(perl -ne 'print "$1" while /^--flag-file=(.*)/g' $CONFIG_FILE)"
    if [ -n "$OTHER_CONFIG" ]; then
        if [ -f "$OTHER_CONFIG" ]; then
            CONFIG_FILE="$OTHER_CONFIG"
        fi
    fi
}

function _wait_for_init() {
    # Wait till Memgraph is started
    cnt=0
    max_wait=300
    until [ $cnt -gt $max_wait ]; do
        if [[ $(nc -z -v $BOLT_ADDRESS $BOLT_PORT 2>&1 >/dev/null | grep 'succeeded') ]]; then
            # Signal that port is listening
            echo 1
            break
        fi
        sleep 1
        ((cnt++))
    done
    sleep 1
}


function _init() {
    # Read configuration for bolt address and port setup
    _extract_port
    # If configuration is different for other config
    _read_other_config
    _extract_port


    # Return if Memgraph is not initialized in max_wait time
    if [[ ! $(_wait_for_init)  ]]; then
        return
    fi

    # Initialize with Cypher data from /usr/lib/memgraph/init
    echo "Loading dataset ..."
    if [[ -d "$DATA_DIR" ]]
    then
        for file in $DATA_DIR/*.cypherl; do
            echo "Loading $file ..."
            mgconsole < $file
        done
    fi

}

function _main() {
    _init &
    /usr/lib/memgraph/memgraph $ARGS
}

_main


