#!/bin/bash
# elfutils: libelf and libdw in the sysroot.
#
# Needed by dwz and libabigail, both of which read DWARF. Building them against
# the host's copies instead would put host-glibc symbols into tools the
# toolchain ships, which is the floor violation the gates exist to catch.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/elfutils.env"

pushd "$TC_ARCHIVES"
if [[ ! -f elfutils-$ELFUTILS_VERSION.tar.bz2 ]]; then
    wget --https-only https://sourceware.org/elfutils/ftp/$ELFUTILS_VERSION/elfutils-$ELFUTILS_VERSION.tar.bz2
    echo "$ELFUTILS_SHA256  elfutils-$ELFUTILS_VERSION.tar.bz2" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps: make, m4, bzip2. zlib comes from the sysroot.
log_tool_name "elfutils $ELFUTILS_VERSION (sysroot)"
tar -xjf ../archives/elfutils-$ELFUTILS_VERSION.tar.bz2
pushd "elfutils-$ELFUTILS_VERSION"
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
popd

popd
