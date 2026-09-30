#!/bin/bash
# CPU-pinning shim — pass as the HA-runner "vendor binary". Requires MG_REAL_BINARY; exec's it and vanishes.
# MG_PIN_MODE: "cpu" (taskset -c CPULIST, default) or "numa" (NUMA node id; numactl, then /sys+taskset).
# MG_PIN_MAP: PORT:VALUE;... e.g. "7687:0-3;7688:4-7". Unpinned silently if port absent or no tool.

set -euo pipefail

if [[ -z "${MG_REAL_BINARY:-}" ]]; then
    echo "ha_pin_wrapper: MG_REAL_BINARY is unset or empty — cannot exec memgraph" >&2
    exit 1
fi

# Handles both --bolt-port=PORT and --bolt-port PORT; stops at the first match.
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

mode="${MG_PIN_MODE:-cpu}"

val=""
if [[ -n "$bolt_port" && -n "${MG_PIN_MAP:-}" ]]; then
    IFS=';' read -ra entries <<< "$MG_PIN_MAP"
    for entry in "${entries[@]}"; do
        port="${entry%%:*}"
        value="${entry#*:}"
        if [[ "$port" == "$bolt_port" ]]; then
            val="$value"
            break
        fi
    done
fi

if [[ "$mode" == "numa" && -n "$val" ]]; then
    if command -v numactl > /dev/null 2>&1; then
        exec numactl --cpunodebind="$val" --membind="$val" "$MG_REAL_BINARY" "$@"
    fi
    node_cpulist="/sys/devices/system/node/node$val/cpulist"
    if command -v taskset > /dev/null 2>&1 && [[ -r "$node_cpulist" ]]; then
        cl="$(< "$node_cpulist")"
        exec taskset -c "$cl" "$MG_REAL_BINARY" "$@"
    fi
    exec "$MG_REAL_BINARY" "$@"
elif [[ -n "$val" ]] && command -v taskset > /dev/null 2>&1; then
    exec taskset -c "$val" "$MG_REAL_BINARY" "$@"
else
    exec "$MG_REAL_BINARY" "$@"
fi
