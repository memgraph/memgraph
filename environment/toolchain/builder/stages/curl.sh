#!/bin/bash
# curl: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/curl.env"

pushd "$TC_ARCHIVES"
if [[ ! -f curl-$CURL_VERSION.tar.gz ]]; then
    wget --https-only https://curl.se/download/curl-$CURL_VERSION.tar.gz
    CURL_SHA256="d9b327997999045a24cda50f3983e69e51c516bd8be6ef9842fc7f99135e33bb"
    echo "$CURL_SHA256  curl-$CURL_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make, pkg-config (resolves sysroot OpenSSL/zlib .pc files
# via PKG_CONFIG_LIBDIR).
log_tool_name "curl $CURL_VERSION (sysroot)"
if [[ ! -f "$SYSROOT/usr/lib/libcurl.a" ]]; then
    if [[ -d "curl-$CURL_VERSION" ]]; then
        rm -rf curl-$CURL_VERSION
    fi
    tar -xzf ../archives/curl-$CURL_VERSION.tar.gz
    pushd "curl-$CURL_VERSION"
    ./configure --prefix=/usr \
        --enable-static \
        --disable-shared \
        --with-openssl \
        --with-zlib \
        --without-libssh2 \
        --without-libpsl \
        --without-nghttp2 \
        --without-libidn2 \
        --without-brotli \
        --without-zstd \
        --disable-ldap
    make -j$CPUS
    make install DESTDIR=$SYSROOT
    popd
fi

popd
