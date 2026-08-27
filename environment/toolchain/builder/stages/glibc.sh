#!/bin/bash
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/glibc.env"
source "$TC_VERSIONS/linux-headers.env"

fetch https://ftp.gnu.org/gnu/glibc/glibc-$GLIBC_VERSION.tar.xz "$GLIBC_SHA256"

pushd "$TC_BUILD"
# Host deps (apt): gcc, g++, make (build-essential), gawk, bison, python3.
log_tool_name "glibc $GLIBC_VERSION"
tar -xf ../archives/glibc-$GLIBC_VERSION.tar.xz
pushd "glibc-$GLIBC_VERSION"
# Force the C variant of support/links-dso-program. The C++ variant links
# against the host's libstdc++, which on modern hosts depends on glibc
# symbols newer than 2.31 (stat@GLIBC_2.33, pthread_create@GLIBC_2.34,
# __isoc23_strtoul@GLIBC_2.38, ...) and fails to link against the
# just-built libc.so.6. The C variant exercises the same dlopen machinery
# without pulling in libstdc++.
sed -i 's|^LINKS_DSO_PROGRAM = links-dso-program$|LINKS_DSO_PROGRAM = links-dso-program-c|' support/Makefile
# misc/syslog.c calls syslog(INTERNALLOG, ...) recursively on invalid
# priority bits. With recent host GCCs (>=12) the fortified inline of
# syslog in <sys/syslog.h> is active when compiling syslog.c itself,
# and the recursive call can't be inlined → hard `inlining failed in
# call to always_inline 'syslog'` error. Route the recursion through
# the internal __syslog symbol (same body, no fortified wrapper).
sed -i 's|^\(\s*\)syslog *(INTERNALLOG,|\1__syslog(INTERNALLOG,|' misc/syslog.c
mkdir build && pushd build
if [[ "$for_arm" = true ]]; then
    glibc_target=aarch64-linux-gnu
else
    glibc_target=x86_64-linux-gnu
fi
# Built with the host compiler (system gcc + binutils). --enable-kernel
# drops glibc's compatibility code for kernels older than this, and so sets
# the oldest kernel anything built with this toolchain will run on. Derived
# from the headers version rather than written out again, because the two
# have to agree and a duplicated literal is what lets them drift apart.
# --disable-werror covers the spurious warnings glibc 2.31 emits under
# newer host compilers.
KERNEL_FLOOR="${LINUX_HEADERS_VERSION%.*}"
../configure \
    --prefix=/usr \
    --build=$glibc_target \
    --host=$glibc_target \
    --with-headers=$SYSROOT/usr/include \
    --enable-kernel=$KERNEL_FLOOR \
    --disable-werror \
    --disable-profile \
    libc_cv_slibdir=/lib64
make -j$CPUS
make install DESTDIR=$SYSROOT
popd && popd

popd
