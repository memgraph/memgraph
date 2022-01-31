#!/bin/bash

_init() {
    DATA_DIR="/tmp/lib/memgraph/init"
    if [[ -d "$DATA_DIR" ]]
    then
        for file in $DATA_DIR/*.cypherl; do
            mgconsole < $file
        done
    fi

}


_main() {
    # TODO: Include arguments from
    /usr/lib/memgraph/memgraph
    # TODO: Should wait till the Memgraph has started
    _init
}

_main


