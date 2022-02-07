#!/bin/bash

ARGS=$@

_wait_for_init() {
    # Wait till Memgraph is started
    cnt=0
    max_wait=300
    port=7687
    until [ $cnt -gt $max_wait ]; do
        if [[ $(nc -z -v 127.0.0.1 $port 2>&1 >/dev/null | grep 'succeeded') ]]; then
            echo 1
            break
        fi
        sleep 1
        ((cnt++))
    done
    sleep 1
}


_init() {
    # Return if Memgraph is not initialized in max_wait time
    if [[ ! $(_wait_for_init)  ]]; then
        return
    fi

    # Initialize with Cypher data from /usr/lib/memgraph/init
    echo "Loading dataset ..."
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


