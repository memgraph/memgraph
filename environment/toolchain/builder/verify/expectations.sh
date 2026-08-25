#!/bin/bash
# Assert the toolchain is what it claims to be.
#
# This is deliberately not the same job as compare.sh. Comparing two builds
# only shows they agree; if both are wrong in the same way it passes happily.
# These are absolute checks against declared values, so they catch a mistake
# that has been faithfully reproduced.
#
# What is checked, and why each one is a silent failure otherwise:
#   - the sysroot really contains the glibc we say it does, rather than
#     whatever was lying around
#   - that glibc targets the kernel floor we declare
#   - the compilers resolve the sysroot inside the prefix, so the toolchain
#     does not quietly compile against the host's headers
#   - clang has no sysroot baked in, because a baked one would override the
#     one the cmake toolchain file passes and nothing would say so
#   - nothing shipped reaches outside the prefix for a library at run time
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/glibc.env"
source "$TC_VERSIONS/linux-headers.env"

KERNEL_FLOOR="${LINUX_HEADERS_VERSION%.*}"

# How many shipped files may carry a RUNPATH with no $ORIGIN entry. Those are
# pinned to this prefix and will not follow a moved tree: binutils' tools and
# the sanitizer runtimes are the bulk of them. The number is pinned rather than
# required to be zero so that fixing it stays a separate piece of work, while a
# regression that adds more still shows up.
MAX_ABSOLUTE_ONLY_RUNPATHS="${TC_MAX_ABSOLUTE_ONLY_RUNPATHS:-122}"

status=0
ok()   { echo "  ok    $*"; }
bad()  { echo "  FAIL  $*"; status=1; }

echo "== sysroot glibc is $GLIBC_VERSION"
libc="$SYSROOT/usr/lib/libc.so.6"
if [[ ! -f "$libc" ]]; then
    bad "no libc at $libc"
else
    # It cannot be executed: the sysroot loader and the host loader disagree.
    got=$(strings "$libc" | sed -nE 's/.*stable release version ([0-9]+\.[0-9]+).*/\1/p' | head -1)
    [[ "$got" == "$GLIBC_VERSION" ]] && ok "sysroot libc reports $got" \
                                     || bad "sysroot libc reports ${got:-unknown}, expected $GLIBC_VERSION"
fi

echo "== sysroot glibc targets kernel $KERNEL_FLOOR"
note=$(readelf -n "$libc" 2>/dev/null | grep -oE 'ABI: [0-9]+\.[0-9]+' | head -1 | sed 's/ABI: //') || true
[[ "$note" == "$KERNEL_FLOOR" ]] && ok "min-kernel note is $note" \
                                 || bad "min-kernel note is ${note:-none}, expected $KERNEL_FLOOR"

echo "== compilers resolve the sysroot inside the prefix"
gcc_sysroot=$("$PREFIX/bin/gcc" -print-sysroot)
[[ "$gcc_sysroot" == "$SYSROOT" ]] && ok "gcc -print-sysroot is $gcc_sysroot" \
                                   || bad "gcc -print-sysroot is $gcc_sysroot, expected $SYSROOT"

if [[ "$for_arm" == "true" ]]; then want_triple=aarch64-linux-gnu; else want_triple=x86_64-linux-gnu; fi
got_triple=$("$PREFIX/bin/gcc" -dumpmachine)
[[ "$got_triple" == "$want_triple" ]] && ok "gcc -dumpmachine is $got_triple" \
                                      || bad "gcc -dumpmachine is $got_triple, expected $want_triple"

clang_sysroot=$("$PREFIX/bin/clang" -print-sysroot 2>/dev/null || true)
[[ -z "$clang_sysroot" ]] && ok "clang has no sysroot baked in" \
                          || bad "clang has a baked sysroot ($clang_sysroot); it would override the toolchain file"

echo "== nothing reaches outside the prefix for a library"
escaping=0
absolute_only=0
while IFS= read -r f; do
    readelf -h "$f" >/dev/null 2>&1 || continue
    rp=$(readelf -d "$f" 2>/dev/null | grep -E 'RPATH|RUNPATH' | sed 's/.*\[\(.*\)\]/\1/') || true
    if [[ -z "$rp" ]]; then continue; fi
    # every entry must be $ORIGIN-relative or live inside the prefix
    IFS=':' read -ra entries <<< "$rp"
    for e in "${entries[@]}"; do
        case "$e" in
            '$ORIGIN'*|"$PREFIX"/*|"$PREFIX") ;;
            *) bad "${f#"$PREFIX"/} has runpath entry outside the prefix: $e"; escaping=1 ;;
        esac
    done
    [[ "$rp" != *'$ORIGIN'* ]] && absolute_only=$((absolute_only + 1))
done < <(find "$PREFIX" -type f -executable)
[[ $escaping -eq 0 ]] && ok "no runpath entry escapes the prefix"

echo "== relocatability"
if [[ $absolute_only -le $MAX_ABSOLUTE_ONLY_RUNPATHS ]]; then
    ok "$absolute_only file(s) carry an absolute-only runpath (allowed $MAX_ABSOLUTE_ONLY_RUNPATHS)"
else
    bad "$absolute_only file(s) carry an absolute-only runpath, up from $MAX_ABSOLUTE_ONLY_RUNPATHS"
fi

if [[ $status -ne 0 ]]; then
    echo "expectations FAILED"
    exit 1
fi
echo "expectations passed"
