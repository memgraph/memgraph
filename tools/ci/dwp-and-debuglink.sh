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
        find "${dwo_dir}" -name '*.dwo' -type f -empty 2>/dev/null | head -5 | while read -r f; do
            echo "[dwp-and-debuglink]   empty: ${f}"
        done
    fi
fi
echo "[dwp-and-debuglink] disk free: $(df -h "${binary%/*}" | tail -1)"

# Soft-fail: a .dwp issue (e.g., llvm-dwp SIGBUS on a 0-byte input) shouldn't
# take out the whole build -- the .dwp is a debug aid, the binary itself is
# what the rest of the pipeline needs. Capture stderr, log it, but exit 0 so
# the link target succeeds. The debuginfo deb/rpm just won't carry useful
# content for that build (memgraph-debuginfo is OPTIONAL in install rules).
echo "[dwp-and-debuglink] running: ${llvm_dwp} -e ${binary} -o ${dwp}"
dwp_log=$(mktemp)
if "${llvm_dwp}" -e "${binary}" -o "${dwp}" 2>"${dwp_log}"; then
    cat "${dwp_log}"
    rm -f "${dwp_log}"
else
    rc=$?
    echo "[dwp-and-debuglink] WARNING: llvm-dwp exited ${rc}; .dwp will be missing/incomplete." >&2
    echo "[dwp-and-debuglink] llvm-dwp stderr:" >&2
    sed 's/^/[dwp-and-debuglink]   /' "${dwp_log}" >&2
    rm -f "${dwp_log}"
    # Remove any partial .dwp so downstream consumers don't see garbage.
    rm -f "${dwp}"
    exit 0
fi

if [[ ! -s "${dwp}" ]]; then
    echo "[dwp-and-debuglink] WARNING: ${dwp} missing/empty; skipping debuglink" >&2
    rm -f "${dwp}"
    exit 0
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
