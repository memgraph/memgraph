#!/bin/bash
# ha_pin_map.sh — Compute a CPU pin map from the container's effective cpuset.
#
#   MG_PIN_MODE  — 'numa' or 'cpu'; ALWAYS the first line.  Selects how the
#                  remaining lines are interpreted downstream.
#   MG_PIN_MAP   — PORT:VALUE pairs, semicolon-separated; consumed by ha_pin_wrapper.sh.
#                  cpu: VALUE is a CPULIST range (e.g. 0-3, or 0,2 for a non-contiguous
#                  slice); numa: VALUE is a node id. Entries use ';' so a comma inside a
#                  VALUE (non-contiguous cpulist) does not collide with the delimiter.
#   CLIENT_CPUS  — taskset -c cpulist for the client.  In numa mode the raw
#                  /sys cpulist (may contain commas — own line, not in MG_PIN_MAP).
#   CLIENT_NODE  — ('numa' mode only) the NUMA node id the client shares with
#                  the three coordinators.
#
# NUMA-mode port→node assignment (A = available node ids, ascending):
#   main  (7687): A[0]     rep1 (7688): A[1]     rep2 (7689): A[2]
#   coord (7690-7692) + client: A[3]  (the three coordinators and the client
#   all share the fourth node).
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
# taskset -cp $$ (process affinity — always reflects --cpuset-cpus) →
# /sys/fs/cgroup/cpuset.cpus.effective (cgroup v2) →
# /sys/fs/cgroup/cpuset/cpuset.cpus  (cgroup v1) →
# 0-$(nproc-1) fallback.
#
# Usage: eval "$(./ha_pin_map.sh)"          # default / --print mode
#        HA_PIN_CPUSET_OVERRIDE=0-7 ./ha_pin_map.sh   # self-test injection

set -euo pipefail

_cpuset_spec() {
    if [[ -n "${HA_PIN_CPUSET_OVERRIDE:-}" ]]; then
        printf '%s' "$HA_PIN_CPUSET_OVERRIDE"
        return
    fi
    if command -v taskset &>/dev/null; then
        local _ts_list
        _ts_list=$(taskset -cp $$ 2>/dev/null | sed -n 's/.*: *//p')
        if [[ "$_ts_list" =~ ^[0-9]+([,-][0-9]+)*$ ]]; then
            printf '%s' "$_ts_list"
            return
        fi
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

# _compact: collapse CPUs to taskset range notation; emits nothing for empty input.
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

# _bounded_slice NAMEREF s e — append CPUS[s..e] (clamped to [0,N-1]) into the
# named array.  CPUS and N come from the dynamic scope of main.
_bounded_slice() {
    local -n _bsl_out=$1
    local s=$2 e=$3
    (( s >= N )) && s=$(( N - 1 ))
    (( e >= N )) && e=$(( N - 1 ))
    (( e < s  )) && e=$s
    local j
    for ((j = s; j <= e; j++)); do _bsl_out+=("${CPUS[$j]}"); done
}

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

    # Membership set S of the allowed cpus, for O(1) lookups.
    local -A in_S=()
    local c
    for c in "${CPUS[@]}"; do in_S[$c]=1; done

    # A node is AVAILABLE iff EVERY cpu in its cpulist is in S; an override
    # that carves a sub-node cpuset therefore yields no fully-available nodes.
    local -a avail_nodes=()            # ascending node ids
    local -A node_cpulist=()           # node id -> raw /sys cpulist string
    local nd nid list cpu ok
    # The glob may not match on non-NUMA hosts; the -d guard skips the literal
    # pattern that bash leaves behind when nothing matches.
    local -a node_dirs=(/sys/devices/system/node/node[0-9]*)
    for nd in "${node_dirs[@]}"; do
        [[ -d "$nd" && -r "$nd/cpulist" ]] || continue
        nid="${nd##*/node}"
        list=$(<"$nd/cpulist")
        list="${list//[$'\t\r\n ']}"
        [[ -n "$list" ]] || continue
        ok=1
        while IFS= read -r cpu; do
            if [[ -z "${in_S[$cpu]:-}" ]]; then ok=0; break; fi
        done < <(_expand "$list")
        if (( ok )); then
            avail_nodes+=("$nid")
            node_cpulist[$nid]="$list"
        fi
    done
    # Sort available node ids numerically ascending (glob order is lexical).
    if (( ${#avail_nodes[@]} > 1 )); then
        local -a sorted_nodes=()
        while IFS= read -r nid; do sorted_nodes+=("$nid"); done \
            < <(printf '%s\n' "${avail_nodes[@]}" | sort -n)
        avail_nodes=("${sorted_nodes[@]}")
    fi

    if (( ${#avail_nodes[@]} >= 4 )); then
        local a0=${avail_nodes[0]} a1=${avail_nodes[1]}
        local a2=${avail_nodes[2]} a3=${avail_nodes[3]}
        printf "MG_PIN_MODE='numa'\n"
        printf "MG_PIN_MAP='7687:%s;7688:%s;7689:%s;7690:%s;7691:%s;7692:%s'\n" \
            "$a0" "$a1" "$a2" "$a3" "$a3" "$a3"
        printf "CLIENT_CPUS='%s'\n" "${node_cpulist[$a3]}"
        printf "CLIENT_NODE='%s'\n" "$a3"
        return
    fi

    printf "MG_PIN_MODE='cpu'\n"

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

    printf "MG_PIN_MAP='7687:%s;7688:%s;7689:%s;7690:%s;7691:%s;7692:%s'\n" \
        "$main_s" "$rep1_s" "$rep2_s" \
        "$coord_core" "$coord_core" "$coord_core"
    printf "CLIENT_CPUS='%s'\n" "$client_s"
}

main "$@"
