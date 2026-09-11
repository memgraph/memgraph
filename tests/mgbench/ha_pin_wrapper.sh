#!/bin/bash
# ha_pin_wrapper.sh — CPU-pinning shim for the mgbench HA benchmark harness.
#
# The HA runner starts each Memgraph instance (main, replicas, coordinators) by
# exec'ing a "vendor binary".  Pass this script as that binary so every instance
# lands on its own core range; it resolves the right cpuset from the instance's
# bolt port, delegates to taskset, and exec's the real binary — the wrapper
# vanishes from the process table.
#
# Environment variables:
#   MG_REAL_BINARY  (required) — absolute path to the real memgraph binary.
#   MG_PIN_MAP      (optional) — comma-separated PORT:CPULIST pairs, e.g.
#                     "7687:0-3,7688:4-7,7689:8-11,7690:12,7691:12,7692:12"
#                   CPULIST is any spec accepted by taskset -c (ranges or lists).
#                   If unset/empty, the port is absent from the map, or taskset is
#                   not installed, the binary runs unpinned — no error is raised.

set -euo pipefail

# Validate required env var.
if [[ -z "${MG_REAL_BINARY:-}" ]]; then
    echo "ha_pin_wrapper: MG_REAL_BINARY is unset or empty — cannot exec memgraph" >&2
    exit 1
fi

# Parse --bolt-port from "$@".  Accepts both --bolt-port=PORT (equals form) and
# --bolt-port PORT (space form); stops at the first match.
bolt_port=""
prev=""
for arg in "$@"; do
    case "$arg" in
        --bolt-port=*)
            bolt_port="${arg#--bolt-port=}"
            break
            ;;
        --bolt-port)
            prev="--bolt-port"
            ;;
        *)
            if [[ "$prev" == "--bolt-port" ]]; then
                bolt_port="$arg"
                break
            fi
            prev=""
            ;;
    esac
done

# Resolve the CPULIST for this port from MG_PIN_MAP.
cpulist=""
if [[ -n "$bolt_port" && -n "${MG_PIN_MAP:-}" ]]; then
    IFS=',' read -ra entries <<< "$MG_PIN_MAP"
    for entry in "${entries[@]}"; do
        port="${entry%%:*}"
        cpus="${entry#*:}"
        if [[ "$port" == "$bolt_port" ]]; then
            cpulist="$cpus"
            break
        fi
    done
fi

# Exec the real binary, pinned when a cpulist was found and taskset is available.
if [[ -n "$cpulist" ]] && command -v taskset > /dev/null 2>&1; then
    exec taskset -c "$cpulist" "$MG_REAL_BINARY" "$@"
else
    exec "$MG_REAL_BINARY" "$@"
fi
