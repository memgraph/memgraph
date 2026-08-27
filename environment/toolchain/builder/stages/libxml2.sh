#!/bin/bash
# libxml2: in the sysroot, for libabigail.
#
# libabigail reads and writes its ABI corpora as XML, so this is a hard
# requirement rather than an option.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/libxml2.env"

pushd "$TC_ARCHIVES"
if [[ ! -f libxml2-$LIBXML2_VERSION.tar.xz ]]; then
    LIBXML2_SERIES="${LIBXML2_VERSION%.*}"
    wget --https-only https://download.gnome.org/sources/libxml2/$LIBXML2_SERIES/libxml2-$LIBXML2_VERSION.tar.xz
    echo "$LIBXML2_SHA256  libxml2-$LIBXML2_VERSION.tar.xz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps: make. zlib comes from the sysroot.
log_tool_name "libxml2 $LIBXML2_VERSION (sysroot)"
tar -xJf ../archives/libxml2-$LIBXML2_VERSION.tar.xz
pushd "libxml2-$LIBXML2_VERSION"
# Static, and trimmed to what libabigail reads: no python bindings, and no
# network fetching, which an ABI comparison has no business doing.
./configure \
    --prefix=/usr \
    --libdir=/usr/lib \
    --disable-shared \
    --enable-static \
    --with-pic \
    --without-python \
    --without-http \
    --without-lzma
make -j$CPUS
make install DESTDIR=$SYSROOT
popd

popd
