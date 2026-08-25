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

pushd "$TC_ARCHIVES"
if [[ ! -f libabigail-$LIBABIGAIL_VERSION.tar.xz ]]; then
    wget --https-only https://sourceware.org/pub/libabigail/libabigail-$LIBABIGAIL_VERSION.tar.xz
    echo "$LIBABIGAIL_SHA256  libabigail-$LIBABIGAIL_VERSION.tar.xz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps: make. libxml2 and elfutils come from the sysroot, found through
# the PKG_CONFIG_LIBDIR that common.sh points at it.
log_tool_name "libabigail $LIBABIGAIL_VERSION"
if [[ ! -f "$PREFIX/bin/abidiff" ]]; then
    if [[ -d "libabigail-$LIBABIGAIL_VERSION" ]]; then
        rm -rf libabigail-$LIBABIGAIL_VERSION
    fi
    tar -xJf ../archives/libabigail-$LIBABIGAIL_VERSION.tar.xz
    pushd "libabigail-$LIBABIGAIL_VERSION"
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
    popd
fi

popd
