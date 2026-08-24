#!/bin/bash
# zlib: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/zlib.env"

pushd "$TC_ARCHIVES"
if [[ ! -f zlib-$ZLIB_VERSION.tar.gz ]]; then
    wget --https-only https://zlib.net/zlib-$ZLIB_VERSION.tar.gz
    ZLIB_SHA256="bb329a0a2cd0274d05519d61c667c062e06990d72e125ee2dfa8de64f0119d16"
    echo "$ZLIB_SHA256  zlib-$ZLIB_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make only — compiler is the toolchain gcc from here on.
log_tool_name "zlib $ZLIB_VERSION (sysroot)"
if [[ ! -f "$SYSROOT/usr/lib/libz.a" ]]; then
    if [[ -d "zlib-$ZLIB_VERSION" ]]; then
        rm -rf zlib-$ZLIB_VERSION
    fi
    tar -xzf ../archives/zlib-$ZLIB_VERSION.tar.gz
    pushd "zlib-$ZLIB_VERSION"
    ./configure --prefix=/usr --static
    make -j$CPUS
    make install DESTDIR=$SYSROOT
    popd
fi

popd
