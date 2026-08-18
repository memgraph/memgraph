#!/usr/bin/env bash
# Print a safe build thread count:
#   min(nproc, floor((MemAvailableGB - reserve_gb) / mem_per_thread_gb))
#
# reserve_gb carries the part of peak memory that does not scale with threads,
# so mem_per_thread_gb stays a pure per-thread figure and can be measured alone.
#
# Usage: compute-build-threads.sh <mem_per_thread_gb> [reserve_gb]

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <mem_per_thread_gb> [reserve_gb]" >&2
  exit 1
fi

mem_per_thread_gb="$1"
reserve_gb="${2:-0}"
cpu_threads=$(nproc)
mem_available_kb=$(awk '/^MemAvailable:/ {print $2; exit}' /proc/meminfo)
if [[ -z "$mem_available_kb" ]]; then
  echo "Failed to read MemAvailable from /proc/meminfo" >&2
  exit 1
fi

awk -v cpus="$cpu_threads" -v mem_kb="$mem_available_kb" \
    -v per="$mem_per_thread_gb" -v reserve="$reserve_gb" '
  BEGIN {
    if (per + 0 <= 0) { print "mem_per_thread_gb must be > 0" > "/dev/stderr"; exit 1 }
    if (reserve + 0 < 0) { print "reserve_gb must be >= 0" > "/dev/stderr"; exit 1 }
    mem_gb = mem_kb / 1024 / 1024 - reserve
    mem_threads = int(mem_gb / per)
    if (mem_threads < 1) mem_threads = 1
    print (mem_threads < cpus) ? mem_threads : cpus
  }'
