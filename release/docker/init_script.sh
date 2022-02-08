#!/bin/bash

# Read the user given arguments
ARGS=$@
# Read the configuration @ /etc/memgraph/memgraph.conf
FLAGS="$(perl -ne 'print "$1=$2"  while /^(-{2}\b[a-z][a-z\d]{2,}\b(?:-[a-z\d]+)*)=((?:[a-zA-Z][a-zA-Z0-9_]*$)|(?:(?:(?:[\/]*\/)*)(?:.*))|(?:[0-9]*))/g' /etc/memgraph/memgraph.conf | sed '/^\s*$/d')"
# Set the default port
PORT=7687

_wait_for_init() {
    # Wait till Memgraph is started
    cnt=0
    max_wait=300
    until [ $cnt -gt $max_wait ]; do
        if [[ $(nc -z -v 127.0.0.1 $PORT 2>&1 >/dev/null | grep 'succeeded') ]]; then
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
    /usr/lib/memgraph/memgraph $FLAGS $ARGS
}

_main


