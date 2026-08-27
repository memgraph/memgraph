#!/bin/bash
# elfutils: libelf and libdw in the sysroot.
#
# Needed by dwz and libabigail, both of which read DWARF. Building them against
# the host's copies instead would put host-glibc symbols into tools the
# toolchain ships, which is the floor violation the gates exist to catch.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/elfutils.env"

fetch https://sourceware.org/elfutils/ftp/$ELFUTILS_VERSION/elfutils-$ELFUTILS_VERSION.tar.bz2 "$ELFUTILS_SHA256"

# Host deps: make, m4, bzip2. zlib comes from the sysroot.
log_tool_name "elfutils $ELFUTILS_VERSION (sysroot)"
enter_source elfutils-$ELFUTILS_VERSION.tar.bz2 elfutils-$ELFUTILS_VERSION
# Static only, matching the other sysroot libraries: nothing the toolchain
# ships should acquire a runtime dependency on a .so inside the sysroot.
# The debuginfod client and its server are not built -- they pull in
# libcurl and libmicrohttpd, and nothing here consumes them.
./configure \
    --prefix=/usr \
    --libdir=/usr/lib \
    --disable-shared \
    --enable-static \
    --with-pic \
    --disable-debuginfod \
    --disable-libdebuginfod \
    --disable-nls \
    --program-prefix=eu-
make -j$CPUS
make install DESTDIR=$SYSROOT
leave_source
