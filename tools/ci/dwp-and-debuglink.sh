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
    echo "[dwp-and-debuglink] dwo_dir contains ${dwo_count} .dwo files"
fi

echo "[dwp-and-debuglink] running: ${llvm_dwp} -e ${binary} -o ${dwp}"
"${llvm_dwp}" -e "${binary}" -o "${dwp}"

if [[ ! -s "${dwp}" ]]; then
    echo "[dwp-and-debuglink] ERROR: ${dwp} is missing or empty after llvm-dwp" >&2
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
