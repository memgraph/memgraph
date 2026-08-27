#!/bin/bash
# zstd: in the sysroot, so LLVM can compress debug info with it.
#
# Debug sections compress with either zlib or zstd, and zstd is both faster and
# smaller on this kind of data. Which one is available is decided when clang is
# built, not when it runs: without zstd linked in, clang rejects -gz=zstd with
# "cannot compress debug sections (zstd not enabled)" and the build falls back
# to zlib. So this has to exist before the LLVM stage, not alongside it.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/zstd.env"

pushd "$TC_ARCHIVES"
if [[ ! -f zstd-$ZSTD_VERSION.tar.gz ]]; then
    wget --https-only https://github.com/facebook/zstd/releases/download/v$ZSTD_VERSION/zstd-$ZSTD_VERSION.tar.gz -O zstd-$ZSTD_VERSION.tar.gz
    echo "$ZSTD_SHA256  zstd-$ZSTD_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps: make. Static only: LLVM links it in, and a shared copy in the
# sysroot would become another runtime dependency of every clang invocation.
log_tool_name "zstd $ZSTD_VERSION (sysroot)"
tar -xzf ../archives/zstd-$ZSTD_VERSION.tar.gz
pushd "zstd-$ZSTD_VERSION"
make -j$CPUS -C lib libzstd.a
make -C lib install-static install-includes PREFIX=/usr LIBDIR=/usr/lib DESTDIR=$SYSROOT
popd

popd
