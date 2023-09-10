#!/bin/bash

check_port_in_use() {
    lsof -i:$1
    if [ $? -eq 0 ]
    then
        echo "Service $2 is successfully working"
    else
        echo "Service $2 needs to be restarted"
        exit 1
    fi
}


check_port_in_use 7687 "Memgraph"
# check_port_in_use(9092, "Kafka bootstrap server")
# check_port_in_use(6650, "Pulsar service url")
