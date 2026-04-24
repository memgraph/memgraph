#!/usr/bin/env bash
# Print a safe build thread count: min(nproc, floor(MemTotalGB / mem_per_thread_gb)).
# Usage: compute-build-threads.sh <mem_per_thread_gb>

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <mem_per_thread_gb>" >&2
  exit 1
fi

mem_per_thread_gb="$1"
cpu_threads=$(nproc)
mem_total_kb=$(awk '/^MemTotal:/ {print $2; exit}' /proc/meminfo)
if [[ -z "$mem_total_kb" ]]; then
  echo "Failed to read MemTotal from /proc/meminfo" >&2
  exit 1
fi

awk -v cpus="$cpu_threads" -v mem_kb="$mem_total_kb" -v per="$mem_per_thread_gb" '
  BEGIN {
    if (per + 0 <= 0) { print "mem_per_thread_gb must be > 0" > "/dev/stderr"; exit 1 }
    mem_gb = mem_kb / 1024 / 1024
    mem_threads = int(mem_gb / per)
    if (mem_threads < 1) mem_threads = 1
    print (mem_threads < cpus) ? mem_threads : cpus
  }'
