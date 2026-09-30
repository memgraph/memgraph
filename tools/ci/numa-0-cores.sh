#!/usr/bin/env bash

NODE=0
CPU_LIST_FILE="/sys/devices/system/node/node${NODE}/cpulist"

if [[ ! -f "$CPU_LIST_FILE" ]]; then
  echo "NUMA node ${NODE} not found" >&2
  exit 1
fi

cat "$CPU_LIST_FILE"
