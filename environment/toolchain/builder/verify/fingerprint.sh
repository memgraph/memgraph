#!/bin/bash
# Describe an installed toolchain in a form two builds can be diffed on.
#
# Byte-identity is not the target: the tree is ~20k files carrying build paths,
# timestamps and link-order noise, and chasing that would sink the port. This
# covers the two things that actually decide whether a toolchain behaves the
# same -- what it installed, and how it is configured -- and deliberately does
# not prove it works. Building memgraph with it is what does that.
#
# Usage: fingerprint.sh [prefix]
set -euo pipefail

PREFIX="${1:-/opt/toolchain-v8}"
[[ -d "$PREFIX" ]] || { echo "no such prefix: $PREFIX" >&2; exit 1; }

echo "### structure"
# Paths relative to the prefix, sorted, so two prefixes with different names
# still compare. Sizes are excluded on purpose -- they move with build paths
# embedded in debug info without anything behaving differently.
( cd "$PREFIX" && find . -mindepth 1 \( -type f -o -type l \) | LC_ALL=C sort )

echo
echo "### tool identity"
# gdb links against the toolchain's own libstdc++, which is what the activation
# script puts on the path; without it gdb cannot start at all. A tool that
# still fails is recorded rather than fatal, so one broken tool does not hide
# the rest of the fingerprint.
export LD_LIBRARY_PATH="$PREFIX/lib:$PREFIX/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
for t in gcc g++ clang clang++ ld.lld ld.mold gdb cmake llvm-objcopy llvm-dwp llvm-profdata; do
    [[ -x "$PREFIX/bin/$t" ]] || continue
    printf '%s: ' "$t"
    # sed rather than head: head closes the pipe as soon as it has its line,
    # and the resulting SIGPIPE trips pipefail.
    { "$PREFIX/bin/$t" --version 2>&1 || echo "(failed to run)"; } \
        | sed -n "1{s|$PREFIX|\$PREFIX|g;p;}"
done

echo
echo "### compiler configuration"
if [[ -x "$PREFIX/bin/gcc" ]]; then
    echo "gcc -dumpmachine: $("$PREFIX/bin/gcc" -dumpmachine)"
    # GCC resolves its sysroot from argv[0], so this reports wherever the tree
    # currently sits. Two trees being compared are rarely at the same path, and
    # the difference says nothing about the toolchains.
    echo "gcc -print-sysroot: $("$PREFIX/bin/gcc" -print-sysroot | sed "s|$PREFIX|\$PREFIX|")"
    # The full -v output records every configure flag GCC was built with, which
    # is where a mis-ported flag shows up.
    "$PREFIX/bin/gcc" -v 2>&1 | grep -E '^Configured with:' | tr ' ' '\n' | LC_ALL=C sort | sed 's/^/  /'
fi
if [[ -x "$PREFIX/bin/clang" ]]; then
    echo "clang -dumpmachine: $("$PREFIX/bin/clang" -dumpmachine)"
    echo "clang -print-resource-dir: $("$PREFIX/bin/clang" -print-resource-dir | sed "s|$PREFIX|\$PREFIX|")"
fi

echo
echo "### llvm feature flags"
if [[ -f "$PREFIX/lib/cmake/llvm/LLVMConfig.cmake" ]]; then
    grep -E '^set\(LLVM_(ENABLE|USE|WITH)_[A-Z_]+ ' "$PREFIX/lib/cmake/llvm/LLVMConfig.cmake" | LC_ALL=C sort
fi

echo
echo "### runpaths"
# Prefix-relative, since the point is whether a binary reaches outside its own
# tree, not what the tree is called.
while IFS= read -r f; do
    readelf -h "$f" >/dev/null 2>&1 || continue
    # A file with no runpath makes grep exit non-zero, which pipefail turns
    # into a failed assignment; that is the normal case, not an error.
    rp=$(readelf -d "$f" 2>/dev/null | grep -E 'RPATH|RUNPATH' | sed 's/.*\[\(.*\)\]/\1/') || true
    if [[ -z "$rp" ]]; then continue; fi
    echo "${f#"$PREFIX"/}: ${rp//$PREFIX/\$PREFIX}"
done < <(find "$PREFIX" -maxdepth 2 -type f -executable | LC_ALL=C sort)

echo
echo "### glibc floors"
maxver() { objdump -T "$1" 2>/dev/null | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -V | tail -1 || true; }
worst=""
while IFS= read -r f; do
    readelf -h "$f" >/dev/null 2>&1 || continue
    v=$(maxver "$f")

    if [[ -z "$v" ]]; then continue; fi
    if [[ -z "$worst" ]] || [[ "$(printf '%s\n%s\n' "$v" "$worst" | sort -V | tail -1)" == "$v" ]]; then worst="$v"; fi
done < <(find "$PREFIX" -type f -executable -not -path "$PREFIX/sysroot/*")
echo "toolchain binaries require at most glibc: ${worst:-none}"
