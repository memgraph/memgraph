#!/bin/bash
# Stage the toolchain gdb + its data files into /tmp/gdb-bundle so the relwithdebinfo
# Docker image can ship a gdb compatible with our DWARF 5 + ThinLTO + split-DWARF
# binaries. Distro gdb (ubuntu:24.04 ships 15.x) crashes on those.
#
# Output layout:
#   /tmp/gdb-bundle/bin/gdb          (stripped)
#   /tmp/gdb-bundle/share/gdb/...    (python plugins, syscalls, system-gdbinit)
#
# Runtime shared-library deps (libipt2, libmpfr6, libbabeltrace1, ...) come from
# the runtime distro and must be apt-installed by the consuming Dockerfile.

set -euo pipefail

TOOLCHAIN_ROOT=${TOOLCHAIN_ROOT:-/opt/toolchain-${MG_TOOLCHAIN_VERSION:-v7}}
DEST=${DEST:-/tmp/gdb-bundle}

if [[ ! -x "${TOOLCHAIN_ROOT}/bin/gdb" ]]; then
    echo "ERROR: ${TOOLCHAIN_ROOT}/bin/gdb not found" >&2
    exit 1
fi

rm -rf "${DEST}"
mkdir -p "${DEST}/bin" "${DEST}/share"

cp -v "${TOOLCHAIN_ROOT}/bin/gdb" "${DEST}/bin/gdb"
strip -s "${DEST}/bin/gdb"

cp -rv "${TOOLCHAIN_ROOT}/share/gdb" "${DEST}/share/gdb"

echo
echo "Bundle staged at ${DEST}:"
du -sh "${DEST}" "${DEST}/bin/gdb" "${DEST}/share/gdb"
"${DEST}/bin/gdb" --version | head -1
