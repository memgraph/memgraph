#!/bin/bash
set -euo pipefail

check_service_in_use() {
  if docker ps --format "{{.Names}}" | grep -q "$1"; then
    echo "$2 is successfully working"
  else
    echo "$2 needs to be available"
    exit 1
  fi
}

check_service_in_use "kafka" "Kafka service"
check_service_in_use "pulsar" "Pulsar service"
