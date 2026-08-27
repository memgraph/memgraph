#!/bin/bash
# Fail the build if the toolchain cannot resolve its own libraries, or resolves
# the C++ runtime from whatever machine it happens to be installed on.
#
# This is a third floor, independent of the two floors.sh checks. Those ask
# which glibc a binary needs; this one asks where it will find libstdc++ at
# run time. The toolchain ships a newer libstdc++ than the distributions it
# runs on, so a tool that reaches past it to the host's copy is limited by that
# host rather than by what we shipped, and stops working on anything older.
# Nothing errors at build time; the break surfaces on someone else's machine.
#
# Run: verify/runtime-links.sh [prefix]   (or `just check-links`)
set -uo pipefail

# The version is declared once, in versions/toolchain.env, and the prefix is
# named after it. Spelling the prefix out here would be a second copy of the
# version to keep in step when a new toolchain is cut.
_here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_here/../versions/toolchain.env"
PREFIX="${1:-${PREFIX:-/opt/toolchain-v$TOOLCHAIN_VERSION}}"
[[ -d "$PREFIX" ]] || { echo "no such prefix: $PREFIX" >&2; exit 2; }

# The sysroot holds libraries for the target, not for this machine, so runtime
# resolution there says nothing. Only the host side of the tree is checked.
readarray -t FILES < <(find "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/lib64" "$PREFIX/libexec" \
    -type f 2>/dev/null)

is_dynamic_elf() {
    file "$1" 2>/dev/null | grep -q 'ELF.*\(executable\|shared object\).*dynamically linked'
}

# GCC's own target libraries, which carry no runpath and so find libgcc_s on
# whatever machine they land on. Recorded rather than fixed: the knob for them
# is LDFLAGS_FOR_TARGET, whose $ORIGIN has to survive configure, make, a
# sub-make and libtool, and changing that stage invalidates every stage after
# it. Nothing we ship is exposed today -- a consumer is only affected if it
# reaches our libstdc++ without needing libgcc_s itself, and memgraph needs
# both directly -- so this waits for the pass that makes the whole tree
# relocatable, which has to rewrite these anyway.
# Patterns, not names: the file names carry the GCC version, so a bump would
# turn three tolerated exemptions into three hard failures with nothing saying
# the list moved rather than the toolchain.
KNOWN_HOST_CXX=(
    "lib64/libstdc++.so.*"
    "lib64/debug/libstdc++.so.*"
    "lib64/libgfortran.so.*"
)

is_known() {
    local k
    for k in "${KNOWN_HOST_CXX[@]}"; do
        # shellcheck disable=SC2053 -- the right side is a pattern on purpose
        [[ "$1" == $k ]] && return 0
    done
    return 1
}

status=0
unresolved=(); hostcxx=(); known=(); scanned=0

for f in "${FILES[@]}"; do
    is_dynamic_elf "$f" || continue
    scanned=$((scanned + 1))
    out=$(ldd "$f" 2>&1)

    grep -q 'not found' <<<"$out" && unresolved+=("${f#"$PREFIX"/}")

    # Only files that actually need the C++ runtime can escape to the host's.
    if readelf -d "$f" 2>/dev/null | grep -q 'NEEDED.*\(libstdc++\|libgcc_s\)'; then
        if grep -E '=> (/lib|/usr/lib)' <<<"$out" | grep -qE 'libstdc\+\+|libgcc_s'; then
            rel="${f#"$PREFIX"/}"
            if is_known "$rel"; then known+=("$rel"); else hostcxx+=("$rel"); fi
        fi
    fi
done

echo "== $scanned dynamically linked files under $PREFIX"

echo
echo "== every library must resolve"
if ((${#unresolved[@]})); then
    printf '   unresolved: %s\n' "${unresolved[@]}"
    status=1
else
    echo "   ok"
fi

echo
echo "== the C++ runtime must come from this toolchain, not the host"
if ((${#hostcxx[@]})); then
    printf '   host libstdc++/libgcc_s: %s\n' "${hostcxx[@]}"
    status=1
else
    echo "   ok"
fi
if ((${#known[@]})); then
    printf '   recorded, see KNOWN_HOST_CXX: %s\n' "${known[@]}"
fi

echo
echo "== libstdc++ floor"
# What the toolchain ships, against the highest any shipped binary asks for.
provided=$(strings -a "$PREFIX/lib64/libstdc++.so.6" 2>/dev/null \
    | grep -oE 'GLIBCXX_3\.4\.[0-9]+' | sort -uV | tail -1)
# Version needs, not version definitions: libstdc++ itself defines every
# GLIBCXX_ symbol it provides, so reading definitions would compare the library
# against itself and pass no matter what.
required=$(for f in "${FILES[@]}"; do
        readelf -V "$f" 2>/dev/null \
            | sed -n '/Version needs/,/^$/p' \
            | grep -oE 'GLIBCXX_3\.4\.[0-9]+'
    done | sort -uV | tail -1)
echo "   shipped libstdc++ provides: ${provided:-unknown}"
echo "   highest required by any shipped binary: ${required:-none}"
if [[ -n "$provided" && -n "$required" ]]; then
    if [[ "$(printf '%s\n%s\n' "$provided" "$required" | sort -V | tail -1)" != "$provided" ]]; then
        echo "   a shipped binary needs more than the shipped libstdc++ provides"
        status=1
    else
        echo "   ok"
    fi
fi

exit $status
