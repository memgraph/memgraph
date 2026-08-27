#!/bin/bash
# libabigail: abidiff and abidw, for comparing ABIs.
#
# memgraph ships a C API for query modules, so third-party modules bind to an
# ABI that nothing currently checks. abidw records a library's ABI and abidiff
# compares two of them, which turns "did this release break a module we do not
# build" from a question nobody can answer into one the build can.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/libabigail.env"

fetch https://sourceware.org/pub/libabigail/libabigail-$LIBABIGAIL_VERSION.tar.xz "$LIBABIGAIL_SHA256"

# Host deps: make. libxml2 and elfutils come from the sysroot, found through
# the PKG_CONFIG_LIBDIR that common.sh points at it.
log_tool_name "libabigail $LIBABIGAIL_VERSION"
enter_source libabigail-$LIBABIGAIL_VERSION.tar.xz libabigail-$LIBABIGAIL_VERSION
# fedabipkgdiff compares distro packages and wants python and rpm; nothing
# here uses it. The test suite pulls in a large corpus and is not run.
./configure \
    --prefix=$PREFIX \
    --disable-shared \
    --enable-static \
    --disable-fedabipkgdiff \
    --disable-zip-archive \
    --with-sysroot=$SYSROOT
make -j$CPUS
make install
leave_source
