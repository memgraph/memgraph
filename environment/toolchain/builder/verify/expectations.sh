#!/bin/bash
# Assert the toolchain is what it claims to be.
#
# This is a different job from compare.sh. Comparing two builds only shows they
# agree; if both are wrong the same way it passes happily. These are absolute
# checks against declared values, so they catch a mistake that has been
# faithfully reproduced.
#
# It is also a different job from floors.sh, which asks what the binaries
# require. These ask what the toolchain is: that the sysroot holds the glibc
# the version set names, that the compilers resolve that sysroot rather than
# the host's headers, and that nothing is pinned to the path it was built at.
# Each is silent otherwise -- the build succeeds and the break surfaces on
# another machine, or against another sysroot.
#
# Runs after relocate.sh, because the runpath checks describe the tree as it
# ships rather than as it was linked.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/glibc.env"
source "$TC_VERSIONS/linux-headers.env"

# Derived the same way the glibc stage derives it, so the promise and the thing
# that implements it cannot disagree.
KERNEL_FLOOR="${LINUX_HEADERS_VERSION%.*}"

status=0
ok()  { echo "  ok    $*"; }
bad() { echo "  FAIL  $*"; status=1; }

echo "== the sysroot holds glibc $GLIBC_VERSION"
libc="$SYSROOT/usr/lib/libc.so.6"
if [[ ! -f "$libc" ]]; then
    bad "no libc at $libc"
else
    # It cannot be executed to ask: the sysroot loader and the host loader
    # disagree, so the version comes out of the binary instead.
    got=$(strings "$libc" | sed -nE 's/.*stable release version ([0-9]+\.[0-9]+).*/\1/p' | head -1)
    [[ "$got" == "$GLIBC_VERSION" ]] && ok "sysroot libc reports $got" \
                                     || bad "sysroot libc reports ${got:-unknown}, expected $GLIBC_VERSION"

    echo "== that glibc targets kernel $KERNEL_FLOOR"
    note=$(readelf -n "$libc" 2>/dev/null | grep -oE 'ABI: [0-9]+\.[0-9]+' | head -1 | sed 's/ABI: //') || true
    [[ "$note" == "$KERNEL_FLOOR" ]] && ok "min-kernel note is $note" \
                                     || bad "min-kernel note is ${note:-none}, expected $KERNEL_FLOOR"
fi

echo "== the compilers resolve the sysroot inside the prefix"
gcc_sysroot=$("$PREFIX/bin/gcc" -print-sysroot)
[[ "$gcc_sysroot" == "$SYSROOT" ]] && ok "gcc -print-sysroot is $gcc_sysroot" \
                                   || bad "gcc -print-sysroot is $gcc_sysroot, expected $SYSROOT"

if [[ "$for_arm" == "true" ]]; then want_triple=aarch64-linux-gnu; else want_triple=x86_64-linux-gnu; fi
got_triple=$("$PREFIX/bin/gcc" -dumpmachine)
[[ "$got_triple" == "$want_triple" ]] && ok "gcc -dumpmachine is $got_triple" \
                                      || bad "gcc -dumpmachine is $got_triple, expected $want_triple"

# A baked-in sysroot would override the one the cmake toolchain file passes,
# and nothing would report that it had.
clang_sysroot=$("$PREFIX/bin/clang" -print-sysroot 2>/dev/null || true)
[[ -z "$clang_sysroot" ]] && ok "clang has no sysroot baked in" \
                          || bad "clang has a baked sysroot ($clang_sysroot); it would override the toolchain file"

echo "== every runpath is relative to the file that carries it"
absolute_only=0
escaping=0
carried=0
while IFS= read -r f; do
    readelf -h "$f" >/dev/null 2>&1 || continue
    rp=$(readelf -d "$f" 2>/dev/null | grep -E 'RPATH|RUNPATH' | sed 's/.*\[\(.*\)\]/\1/') || true
    [[ -z "$rp" ]] && continue
    carried=$((carried + 1))
    IFS=':' read -ra entries <<< "$rp"
    for e in "${entries[@]}"; do
        case "$e" in
            '$ORIGIN'*|"$PREFIX"/*|"$PREFIX") ;;
            *) bad "${f#"$PREFIX"/} has a runpath entry outside the prefix: $e"; escaping=1 ;;
        esac
    done
    # An absolute-only runpath resolves to where the toolchain was built, so
    # the file stops finding its libraries the moment the tree is installed
    # anywhere else. relocate.sh rewrites these, and leaves none behind.
    if [[ "$rp" != *'$ORIGIN'* ]]; then
        bad "${f#"$PREFIX"/} has an absolute-only runpath: $rp"
        absolute_only=$((absolute_only + 1))
    fi
done < <(find "$PREFIX" -type f -executable)
[[ $escaping -eq 0 ]] && ok "no runpath entry escapes the prefix"
[[ $absolute_only -eq 0 ]] && ok "all $carried runpath(s) carry \$ORIGIN"

if [[ $status -ne 0 ]]; then
    echo "expectations FAILED"
    exit 1
fi
echo "expectations passed"
