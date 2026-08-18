#!/usr/bin/env bash
# Print a safe build thread count:
#   min(nproc, floor((MemAvailableGB - reserve_gb) / mem_per_thread_gb))
#
# Peak build memory is close to affine in the thread count: a fixed cost that
# does not scale (the resident toolchain, the ninja process, whatever else the
# runner is holding) plus a per-thread cost. Charging the fixed part to
# reserve_gb rather than inflating mem_per_thread_gb keeps the per-thread figure
# meaning what it says, so it can be measured once and reused.
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
