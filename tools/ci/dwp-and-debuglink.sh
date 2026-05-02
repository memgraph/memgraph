#!/usr/bin/env bash
# POST_BUILD helper: bundle .dwo files into <binary>.dwp via llvm-dwp,
# then add a .gnu_debuglink section pointing at the .dwp via objcopy.
# Verbose by design -- runs as a single ninja step, so logs here are the
# first place to look when the build fails after linking.
#
# Usage: dwp-and-debuglink.sh <llvm-dwp> <objcopy-or-empty> <binary> [<dwo-dir>]
#   <objcopy-or-empty>: pass "" to skip the .gnu_debuglink step.
#   <dwo-dir>: optional. If provided, listed for diagnostics.

set -euo pipefail

llvm_dwp=$1
objcopy=$2
binary=$3
dwo_dir=${4:-}
dwp="${binary}.dwp"

echo "[dwp-and-debuglink] llvm-dwp:     ${llvm_dwp}"
echo "[dwp-and-debuglink] objcopy:      ${objcopy:-<unset, skipping debuglink>}"
echo "[dwp-and-debuglink] binary:       ${binary}"
echo "[dwp-and-debuglink] dwo_dir:      ${dwo_dir:-<unset>}"

if [[ ! -x "${llvm_dwp}" ]]; then
    echo "[dwp-and-debuglink] ERROR: llvm-dwp not executable" >&2
    exit 2
fi
if [[ ! -f "${binary}" ]]; then
    echo "[dwp-and-debuglink] ERROR: binary missing: ${binary}" >&2
    exit 3
fi

if [[ -n "${dwo_dir}" && -d "${dwo_dir}" ]]; then
    dwo_count=$(find "${dwo_dir}" -name '*.dwo' -type f 2>/dev/null | wc -l)
    empty_dwo=$(find "${dwo_dir}" -name '*.dwo' -type f -empty 2>/dev/null | wc -l)
    echo "[dwp-and-debuglink] dwo_dir contains ${dwo_count} .dwo files (${empty_dwo} empty)"
    if (( empty_dwo > 0 )); then
        # Diagnostics-first: gather everything that might explain WHY this
        # .dwo came out empty before we stub-and-recover. Run only when we
        # actually have empties (cheap on the hot path).
        binary_mtime_epoch=$(stat -c '%Y' "${binary}" 2>/dev/null || echo 0)
        # Sample timing for a NON-empty .dwo as a baseline: lld writes them
        # roughly contemporaneously with the link, so a big mtime gap to an
        # empty .dwo would point at a stale leftover.
        ref_dwo=$(find "${dwo_dir}" -name '*.dwo' -type f -size +0 2>/dev/null | head -1)
        if [[ -n "${ref_dwo}" ]]; then
            echo "[dwp-and-debuglink] reference non-empty .dwo for timing baseline:"
            echo "[dwp-and-debuglink]   $(stat -c '%Y %s %n' "${ref_dwo}")"
        fi
        echo "[dwp-and-debuglink] binary mtime: ${binary_mtime_epoch} ($(stat -c '%y' "${binary}" 2>/dev/null))"

        # Walk skeleton CUs in the binary to map each empty .dwo back to its
        # source TU. DWARF 5 uses DW_AT_dwo_name; the GNU extension is
        # DW_AT_GNU_dwo_name. The source file isn't in the skeleton CU
        # (lives in the .dwo, which is empty), but DW_AT_low_pc gives us a
        # text address we can symbolize via the binary's own symbols to a
        # function name and translation unit.
        dwarfdump=$(dirname "${llvm_dwp}")/llvm-dwarfdump
        symbolizer=$(dirname "${llvm_dwp}")/llvm-symbolizer
        skeleton_dump=$(mktemp)
        if [[ -x "${dwarfdump}" ]]; then
            "${dwarfdump}" --debug-info "${binary}" 2>/dev/null > "${skeleton_dump}"
        fi

        find "${dwo_dir}" -name '*.dwo' -type f -empty 2>/dev/null | while read -r f; do
            base=$(basename "${f}")
            mtime=$(stat -c '%Y' "${f}" 2>/dev/null || echo 0)
            mtime_h=$(stat -c '%y' "${f}" 2>/dev/null)
            delta=$((binary_mtime_epoch - mtime))
            echo "[dwp-and-debuglink] EMPTY .dwo: ${f}"
            echo "[dwp-and-debuglink]   stat: size=0 mtime='${mtime_h}' (binary -${delta}s)"

            if [[ ! -s "${skeleton_dump}" ]]; then
                continue
            fi
            # Print the skeleton CU stanza referencing this .dwo. dwarfdump
            # formats one CU per stanza with attributes indented; grep -B/-A
            # captures both the "Compile Unit:" header (above) and any tail
            # attributes (below) without depending on blank-line layout.
            cu_attrs=$(grep -B 2 -A 10 -F "/${base}\"" "${skeleton_dump}" 2>/dev/null | head -20)
            if [[ -n "${cu_attrs}" ]]; then
                echo "[dwp-and-debuglink]   skeleton CU referencing ${base}:"
                printf '%s\n' "${cu_attrs}" | sed 's/^/[dwp-and-debuglink]     /'
            fi

            # Symbolize the CU's text range so we know which TU it came from.
            # DW_AT_low_pc lives a couple of lines AFTER DW_AT_dwo_name in
            # DWARF 5 output, so search forwards.
            low_pc=$(grep -A 6 -F "/${base}\"" "${skeleton_dump}" 2>/dev/null \
                     | grep -oE 'DW_AT_low_pc[[:space:]]*\(0x[0-9a-f]+' \
                     | grep -oE '0x[0-9a-f]+' | head -1)
            if [[ -n "${low_pc}" && -x "${symbolizer}" ]]; then
                # --functions=linkage falls back to the binary's symbol table
                # when DWARF source name is missing (the .dwo is empty here).
                sym=$("${symbolizer}" --obj="${binary}" --functions=linkage "${low_pc}" 2>/dev/null | head -3)
                if [[ -n "${sym}" ]]; then
                    echo "[dwp-and-debuglink]   low_pc=${low_pc} -> $(echo "${sym}" | tr '\n' ' ')"
                fi
            fi
        done
        rm -f "${skeleton_dump}"

        # Print the link command that produced these dwo files (from build.ninja
        # if reachable). Helps reproduce the exact lld invocation upstream.
        ninja_file="${binary%/*}/build.ninja"
        if [[ -f "${ninja_file}" ]]; then
            echo "[dwp-and-debuglink] link command (truncated to first 600 chars):"
            grep -m1 "build $(basename "${binary}"):" -A1 "${ninja_file}" 2>/dev/null \
                | head -c 600 | sed 's/^/[dwp-and-debuglink]   /'
            echo
        fi

        # Recover: substitute a 208-byte minimal valid empty-DWO ELF stub
        # (output of `clang++ -g -gsplit-dwarf -c <empty.cpp>`) so llvm-dwp
        # reads each empty as a valid empty object instead of SIGBUSing on
        # mmap-deref-past-EOF, and we still produce a complete .dwp.
        empty_stub_b64='f0VMRgIBAQAAAAAAAAAAAAEAPgABAAAAAAAAAAAAAAAAAAAAAAAAAFAAAAAAAAAAAAAAAEAAAAAAAEAAAgABAAAuc3RydGFiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAJAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=='
        find "${dwo_dir}" -name '*.dwo' -type f -empty 2>/dev/null | while read -r f; do
            echo "[dwp-and-debuglink] stubbing: ${f}"
            echo "${empty_stub_b64}" | base64 -d > "${f}"
        done
    fi
fi
echo "[dwp-and-debuglink] disk free: $(df -h "${binary%/*}" | tail -1)"

# Classify .dwo files: count ELF vs non-ELF vs zero-byte, and call out the
# flat numeric-named files (lld ThinLTO partition outputs from
# --plugin-opt=dwo_dir=...). Run only on failure -- 250+ stat+od calls add up.
diagnose_dwo_dir() {
    local dir=$1
    [[ -n "${dir}" && -d "${dir}" ]] || return 0
    local total=0 elf=0 nonelf=0 zero=0
    local -a nonelf_samples=()
    while IFS= read -r -d '' f; do
        total=$((total + 1))
        if [[ ! -s "${f}" ]]; then
            zero=$((zero + 1))
            continue
        fi
        local magic
        magic=$(od -An -tx1 -N4 "${f}" 2>/dev/null | tr -d ' \n')
        if [[ "${magic}" == "7f454c46" ]]; then
            elf=$((elf + 1))
        else
            nonelf=$((nonelf + 1))
            if (( ${#nonelf_samples[@]} < 10 )); then
                nonelf_samples+=("${f} ($(stat -c %s "${f}") bytes, magic=${magic:-<empty>})")
            fi
        fi
    done < <(find "${dir}" -name '*.dwo' -type f -print0)
    echo "[dwp-and-debuglink]   classification: total=${total} elf=${elf} non-elf=${nonelf} zero-byte=${zero}" >&2
    if (( nonelf > 0 )); then
        echo "[dwp-and-debuglink]   non-ELF .dwo samples (likely ThinLTO partition stubs or compression mismatch):" >&2
        local s
        for s in "${nonelf_samples[@]}"; do
            echo "[dwp-and-debuglink]     ${s}" >&2
        done
    fi
    local lto_files
    lto_files=$(find "${dir}" -maxdepth 1 -regextype posix-extended -regex '.*/[0-9]+\.dwo' -type f 2>/dev/null | sort -V)
    if [[ -n "${lto_files}" ]]; then
        echo "[dwp-and-debuglink]   ThinLTO partition .dwo files (flat numeric, written by lld --plugin-opt=dwo_dir):" >&2
        while IFS= read -r f; do
            local m
            m=$(od -An -tx1 -N4 "${f}" 2>/dev/null | tr -d ' \n')
            echo "[dwp-and-debuglink]     $(basename "${f}") $(stat -c %s "${f}") bytes magic=${m:-<empty>}" >&2
        done <<<"${lto_files}"
    fi
}

# dwp is required: a missing .dwp means the resulting memgraph-debuginfo
# package would be empty, and that's not a thing we want to ship silently.
# On failure, log llvm-dwp stderr (otherwise hidden behind ninja's combined
# edge output), remove any partial .dwp, and propagate the exit code so the
# build fails loudly.
echo "[dwp-and-debuglink] running: ${llvm_dwp} -e ${binary} -o ${dwp}"
dwp_log=$(mktemp)
if "${llvm_dwp}" -e "${binary}" -o "${dwp}" 2>"${dwp_log}"; then
    cat "${dwp_log}"
    rm -f "${dwp_log}"
else
    rc=$?
    echo "[dwp-and-debuglink] ERROR: llvm-dwp exited ${rc}" >&2
    echo "[dwp-and-debuglink] llvm-dwp stderr:" >&2
    sed 's/^/[dwp-and-debuglink]   /' "${dwp_log}" >&2
    diagnose_dwo_dir "${dwo_dir}"
    rm -f "${dwp_log}" "${dwp}"
    exit ${rc}
fi

if [[ ! -s "${dwp}" ]]; then
    echo "[dwp-and-debuglink] ERROR: ${dwp} missing/empty after llvm-dwp" >&2
    rm -f "${dwp}"
    exit 4
fi
echo "[dwp-and-debuglink] wrote ${dwp} ($(stat -c '%s' "${dwp}") bytes)"

if [[ -n "${objcopy}" ]]; then
    if [[ ! -x "${objcopy}" ]]; then
        echo "[dwp-and-debuglink] WARNING: objcopy not executable, skipping debuglink: ${objcopy}" >&2
    else
        echo "[dwp-and-debuglink] running: ${objcopy} --add-gnu-debuglink=$(basename "${dwp}") ${binary}"
        # objcopy reads the .dwp from the cwd. Run from the binary's dir so the
        # debuglink stores just the basename, which is what gdb's resolver uses.
        cd "$(dirname "${binary}")"
        "${objcopy}" --add-gnu-debuglink="$(basename "${dwp}")" "${binary}"
        echo "[dwp-and-debuglink] added .gnu_debuglink"
    fi
fi
