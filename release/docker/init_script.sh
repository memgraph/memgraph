#!/bin/bash

ARGS=$@

_init() {
    # Wait till Memgraph is started
    cnt=0
    max_wait=10
    initialized=0
    until [ $cnt -gt $max_wait ]; do
        sleep 2
        if [[ $(ps -u memgraph | grep memgraph) ]]; then
            initialized=1
            break
        fi
        ((cnt++))
    done

    # Return if Memgraph is not initialized in max_wait * 2 seconds
    if [[ $initialized = 0 ]]; then
        return
    fi

    # Initialize with Cypher data from /usr/lib/memgraph/init
    DATA_DIR="/usr/lib/memgraph/init"
    if [[ -d "$DATA_DIR" ]]
    then
        for file in $DATA_DIR/*.cypherl; do
            echo "Loading $file ..."
            mgconsole < $file
        done
    fi

}

_main() {
    _init &
    /usr/lib/memgraph/memgraph $ARGS
}

_main


