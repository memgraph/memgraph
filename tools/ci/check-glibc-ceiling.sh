#!/bin/bash
# Fail when a binary references GLIBC symbols newer than the declared floor.
#
# The packages hand-write `libc6 (>= 2.31)` / run on el9 (glibc 2.34) because
# auto-shlibdeps is off (see release/CMakeLists.txt) — so nothing else catches
# a build that accidentally links host glibc symbols instead of the toolchain
# sysroot's (e.g. the hardcoded-v7-conan-profile incident: pthread_create@2.34,
# __isoc23_*@2.38 on an Ubuntu 24.04 builder). This is that guard.
#
# Usage: check-glibc-ceiling.sh <binary> [ceiling]   (ceiling default: 2.31)
set -euo pipefail

BINARY="$1"
CEILING="${2:-2.31}"

if [ ! -f "$BINARY" ]; then
    echo "check-glibc-ceiling: binary not found: $BINARY" >&2
    exit 1
fi

max=$(readelf -W --dyn-syms "$BINARY" | grep -oE 'GLIBC_2\.[0-9]+' | sort -uV | tail -1 || true)
if [ -z "$max" ]; then
    echo "check-glibc-ceiling: no versioned GLIBC symbols in $BINARY; nothing to check"
    exit 0
fi

max_ver="${max#GLIBC_}"
newest=$(printf '%s\n%s\n' "$CEILING" "$max_ver" | sort -V | tail -1)
if [ "$newest" != "$CEILING" ]; then
    echo "ERROR: $BINARY requires $max, above the declared glibc floor $CEILING." >&2
    echo "       The binary was not fully built against the toolchain sysroot." >&2
    echo "       Offending symbols:" >&2
    readelf -W --dyn-syms "$BINARY" \
        | grep -oE "[A-Za-z0-9_.]+@GLIBC_2\.[0-9]+" \
        | awk -F'@GLIBC_' -v floor="$CEILING" \
            '{ n = split($2, a, "."); if (a[1] > 2 || (a[1] == 2 && a[2] + 0 > substr(floor, 3) + 0)) print "         " $0 }' \
        | sort -u | head -40 >&2
    exit 1
fi

echo "check-glibc-ceiling: OK — max GLIBC symbol $max <= floor $CEILING ($BINARY)"
