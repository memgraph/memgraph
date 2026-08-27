#!/bin/bash
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/gcc-sysroot-libs.env"

pushd "$TC_BUILD"
# Expose GCC's runtime libraries (libstdc++, libgcc_s) inside the sysroot.
# clang with --sysroot=$SYSROOT looks for -lstdc++ / -lgcc_s in $SYSROOT/usr/lib*
# but GCC installs them outside the sysroot at $PREFIX/lib64. Symlink them in
# (both shared .so and static .a — mgconsole uses -static-libstdc++) so the
# LLVM runtimes sub-build and other sysroot-aware C++ links resolve them
# without extra -L flags. Relative symlinks keep the toolchain relocatable.
log_tool_name "expose GCC libstdc++/libgcc_s/libatomic in sysroot"
if [[ ! -L "$SYSROOT/usr/lib64/libstdc++.so.6" ]]; then
    mkdir -p $SYSROOT/usr/lib64
    for lib in $PREFIX/lib64/libstdc++.so* $PREFIX/lib64/libstdc++.a \
               $PREFIX/lib64/libgcc_s.so* $PREFIX/lib64/libsupc++.a \
               $PREFIX/lib64/libatomic.so* $PREFIX/lib64/libatomic.a; do
        [[ -e "$lib" ]] || continue
        ln -sf "../../../lib64/$(basename "$lib")" "$SYSROOT/usr/lib64/$(basename "$lib")"
    done
fi

# Also expose the sysroot glibc's runtime libs (libm.so.6, libc.so.6, ...) in
# $SYSROOT/usr/lib64 and $SYSROOT/usr/lib. glibc installs them into
# $SYSROOT/lib64 (slibdir), but ld resolves transitive DT_NEEDED entries
# (e.g. libstdc++.so -> libm.so.6) ONLY via its built-in sysroot-prefixed
# SEARCH_DIRs, never via -L: that's "=/lib64" on x86-64 hosts' ld but only
# "=/lib" and "=/usr/lib" on Debian-family aarch64 ld. Cover both layouts.
if [[ ! -e "$SYSROOT/usr/lib/libm.so.6" ]]; then
    for lib in $SYSROOT/lib64/*.so*; do
        [[ -e "$lib" ]] || continue
        ln -sf "../../lib64/$(basename "$lib")" "$SYSROOT/usr/lib64/$(basename "$lib")"
        ln -sf "../../lib64/$(basename "$lib")" "$SYSROOT/usr/lib/$(basename "$lib")"
    done
fi

popd
