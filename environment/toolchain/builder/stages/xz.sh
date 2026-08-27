#!/bin/bash
# xz: liblzma in the sysroot, for libabigail.
#
# libabigail 2.10 requires liblzma outright, with no option to build without
# it. The builder image has a host copy, but pkg-config is deliberately pointed
# at the sysroot with no host fallback, and satisfying this from the host would
# put host-glibc symbols into a shipped tool.
#
# 5.8.3 is well clear of the 5.6.0 and 5.6.1 releases that carried the 2024
# backdoor; anything at or below those must not be used here.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/xz.env"

pushd "$TC_ARCHIVES"
if [[ ! -f xz-$XZ_VERSION.tar.xz ]]; then
    wget --https-only https://github.com/tukaani-project/xz/releases/download/v$XZ_VERSION/xz-$XZ_VERSION.tar.xz
    echo "$XZ_SHA256  xz-$XZ_VERSION.tar.xz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps: make. Only the library is wanted; the command line tools would
# shadow the host's xz on PATH for no benefit.
log_tool_name "xz $XZ_VERSION (sysroot)"
tar -xJf ../archives/xz-$XZ_VERSION.tar.xz
pushd "xz-$XZ_VERSION"
./configure \
    --prefix=/usr \
    --libdir=/usr/lib \
    --disable-shared \
    --enable-static \
    --with-pic \
    --disable-xz \
    --disable-xzdec \
    --disable-lzmadec \
    --disable-lzmainfo \
    --disable-scripts \
    --disable-doc \
    --disable-nls
make -j$CPUS
make install DESTDIR=$SYSROOT
popd

popd
