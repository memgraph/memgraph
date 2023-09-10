#!/bin/bash

check_service_in_use() {
    docker ps --format "{{.Names}}" | grep $1
    if [ $? -eq 0 ]
    then
        echo "Service $2 is successfully working"
    else
        echo "Service $2 needs to be restarted"
        exit 1
    fi
}


check_service_in_use "kafka" "Kafka service"
check_service_in_use "pulsar" "Pulsar service"
