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
    EXTRACTED_BOLT_ADDRESS="$(perl -ne 'print "$1" while /^--bolt-address=(.*)/g' $CONFIG_FILE)"
    EXTRACTED_BOLT_PORT="$(perl -ne 'print "$1" while /^--bolt-port=(.*)/g' $CONFIG_FILE)"

    if [ -n "$EXTRACTED_BOLT_ADDRESS" ]; then
        BOLT_ADDRESS=$EXTRACTED_BOLT_ADDRESS
    fi

    if [ -n "$EXTRACTED_BOLT_PORT" ]; then
        BOLT_PORT=$EXTRACTED_BOLT_PORT
    fi
}

function _read_other_config {
    # Read the other config
    EXTRACTED_CONFIG="$(perl -ne 'print "$1" while /^--flag-file=(.*)/g' $CONFIG_FILE)"
    if [ -n "$EXTRACTED_CONFIG" ]; then
        if [ -f "$EXTRACTED_CONFIG" ]; then
            CONFIG_FILE="$EXTRACTED_CONFIG"
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

function _extract_command_line_optional_port {
    COMMAND_LINE_ARGS=(`echo $ARGS | tr ',' ' '`)
    for arg in "${COMMAND_LINE_ARGS[@]}"
    do
        EXTRACTED_BOLT_ADDRESS=$(echo $arg | grep -oP "\--bolt-address=\K.*")
        EXTRACTED_BOLT_PORT=$(echo $arg | grep -oP "\--bolt-port=\K.*")

        if [ -n "$EXTRACTED_BOLT_ADDRESS" ]; then
            BOLT_ADDRESS=$EXTRACTED_BOLT_ADDRESS
        fi

        if [ -n "$EXTRACTED_BOLT_PORT" ]; then
            BOLT_PORT=$EXTRACTED_BOLT_PORT
        fi
    done
}


function _init() {
    # Read configuration for bolt address and port setup
    _extract_port
    # If configuration is different for other config
    _read_other_config
    _extract_port

    _extract_command_line_optional_port


    # Return if Memgraph is not initialized in max_wait time
    if [[ ! $(_wait_for_init)  ]]; then
        return
    fi

    # Initialize with Cypher data from /usr/lib/memgraph/init
    echo "Loading datasets ..."

    if [[ -d "$DATA_DIR" ]]
    then
        for cypherl_file in $DATA_DIR/*.cypherl; do
            echo "Loading $cypherl_file ..."
            mgconsole < $cypherl_file
        done
        for script_file in $DATA_DIR/*.sh; do
            $script_file
        done
    fi

    echo "Finished loading datasets!"
}

function _main() {
    _init &
    /usr/lib/memgraph/memgraph $ARGS
}

_main


