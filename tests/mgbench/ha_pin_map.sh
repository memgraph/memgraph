#!/bin/bash
# ha_pin_map.sh — Compute a CPU pin map from the container's effective cpuset.
#
# Reads the container's effective cpuset and deterministically carves it into
# per-instance core assignments, then prints two shell-eval'able lines for the
# benchmark driver:
#
#   MG_PIN_MAP   — consumed by ha_pin_wrapper.sh; PORT:CPULIST pairs separated
#                  by commas.  CPULIST uses range notation (e.g. 0-3) so it
#                  does not collide with the comma delimiter that separates the
#                  PORT:CPULIST entries.
#   CLIENT_CPUS  — taskset -c cpulist for the benchmark client process.
#
# Core-reservation policy (N = total allowed CPUs):
#   CPD = min(4, floor((N-2)/3)), clamped to [1, 4]   — cores per data instance
#   main  (7687): CPUS[0 .. CPD-1]
#   rep1  (7688): CPUS[CPD .. 2*CPD-1]
#   rep2  (7689): CPUS[2*CPD .. 3*CPD-1]
#   coord (7690-7692): one shared core — CPUS[min(3*CPD, N-1)]
#   client: CPUS[coord_idx+1 .. N-1]; falls back to the coord core when empty.
# Indices are clamped to [0, N-1]: overlap/sharing under small cpusets is
# acceptable.
#
# Cpuset detection order: HA_PIN_CPUSET_OVERRIDE (for self-tests only) →
# /sys/fs/cgroup/cpuset.cpus.effective (cgroup v2) →
# /sys/fs/cgroup/cpuset/cpuset.cpus  (cgroup v1) →
# 0-$(nproc-1) fallback.
#
# Usage: eval "$(./ha_pin_map.sh)"          # default / --print mode
#        HA_PIN_CPUSET_OVERRIDE=0-7 ./ha_pin_map.sh   # self-test injection

set -euo pipefail

# ---------------------------------------------------------------------------
# _cpuset_spec — emit the raw cpuset string, honouring the override env var.
# ---------------------------------------------------------------------------
_cpuset_spec() {
    if [[ -n "${HA_PIN_CPUSET_OVERRIDE:-}" ]]; then
        printf '%s' "$HA_PIN_CPUSET_OVERRIDE"
        return
    fi
    local v2=/sys/fs/cgroup/cpuset.cpus.effective
    local v1=/sys/fs/cgroup/cpuset/cpuset.cpus
    if [[ -r "$v2" && -s "$v2" ]]; then
        tr -d '[:space:]' < "$v2"
    elif [[ -r "$v1" && -s "$v1" ]]; then
        tr -d '[:space:]' < "$v1"
    else
        printf '0-%d' "$(( $(nproc) - 1 ))"
    fi
}

# ---------------------------------------------------------------------------
# _expand SPEC — expand "a-b,c,d-e,..." into sorted unique integers, one/line.
# ---------------------------------------------------------------------------
_expand() {
    local spec="$1" segment lo hi cpu
    local -a segments
    IFS=',' read -ra segments <<< "$spec"
    for segment in "${segments[@]}"; do
        segment="${segment// /}"
        if [[ "$segment" == *-* ]]; then
            lo="${segment%-*}"
            hi="${segment#*-}"
            for ((cpu = lo; cpu <= hi; cpu++)); do printf '%d\n' "$cpu"; done
        elif [[ -n "$segment" ]]; then
            printf '%d\n' "$segment"
        fi
    done | sort -un
}

# ---------------------------------------------------------------------------
# _compact CPU... — collapse ascending integers into a compact taskset range
# spec with no trailing comma; e.g. "0 1 2 3" → "0-3", "5 8 9" → "5,8-9".
# Emits nothing for zero arguments.
# ---------------------------------------------------------------------------
_compact() {
    local -a cpus=("$@")
    local n=${#cpus[@]}
    (( n == 0 )) && return
    local result='' start=${cpus[0]} prev=${cpus[0]} cur i
    for ((i = 1; i < n; i++)); do
        cur=${cpus[$i]}
        if (( cur - prev == 1 )); then
            prev=$cur
        else
            [[ -n "$result" ]] && result+=','
            if (( prev == start )); then result+="$start"; else result+="${start}-${prev}"; fi
            start=$cur; prev=$cur
        fi
    done
    [[ -n "$result" ]] && result+=','
    if (( prev == start )); then result+="$start"; else result+="${start}-${prev}"; fi
    printf '%s' "$result"
}

# ---------------------------------------------------------------------------
# _bounded_slice NAMEREF START END — append CPUS[START..END] into the named
# array, clamping both indices to [0, N-1].  CPUS and N are read from the
# dynamic scope of the caller (main).
# ---------------------------------------------------------------------------
_bounded_slice() {
    local -n _bsl_out=$1
    local s=$2 e=$3
    (( s >= N )) && s=$(( N - 1 ))
    (( e >= N )) && e=$(( N - 1 ))
    (( e < s  )) && e=$s
    local j
    for ((j = s; j <= e; j++)); do _bsl_out+=("${CPUS[$j]}"); done
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
main() {
    local spec
    spec=$(_cpuset_spec)
    # Strip any residual whitespace the cgroup file might carry.
    spec="${spec//[$'\t\r\n ']}"

    local -a CPUS=()
    while IFS= read -r cpu; do CPUS+=("$cpu"); done < <(_expand "$spec")

    local N=${#CPUS[@]}
    if (( N == 0 )); then
        printf 'ha_pin_map: empty cpuset — cannot assign pins\n' >&2
        exit 1
    fi

    # Cores per data instance: floor((N-2)/3), clamped to [1, 4].
    local cpd=$(( (N - 2) / 3 ))
    (( cpd < 1 )) && cpd=1
    (( cpd > 4 )) && cpd=4

    local -a main_c=() rep1_c=() rep2_c=() client_c=()
    _bounded_slice main_c  0           $(( cpd - 1   ))
    _bounded_slice rep1_c  $((cpd))    $(( 2*cpd - 1 ))
    _bounded_slice rep2_c  $((2*cpd))  $(( 3*cpd - 1 ))

    local coord_idx=$(( 3*cpd < N ? 3*cpd : N - 1 ))
    local coord_core="${CPUS[$coord_idx]}"

    local cli_start=$(( coord_idx + 1 ))
    if (( cli_start < N )); then
        _bounded_slice client_c $cli_start $(( N - 1 ))
    else
        # Small cpuset: no spare core; share the coordinator core.
        client_c=("$coord_core")
    fi

    local main_s rep1_s rep2_s client_s
    main_s=$(_compact   "${main_c[@]}")
    rep1_s=$(_compact   "${rep1_c[@]}")
    rep2_s=$(_compact   "${rep2_c[@]}")
    client_s=$(_compact "${client_c[@]}")

    printf "MG_PIN_MAP='7687:%s,7688:%s,7689:%s,7690:%s,7691:%s,7692:%s'\n" \
        "$main_s" "$rep1_s" "$rep2_s" \
        "$coord_core" "$coord_core" "$coord_core"
    printf "CLIENT_CPUS='%s'\n" "$client_s"
}

main "$@"
