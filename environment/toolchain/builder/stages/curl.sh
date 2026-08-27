#!/bin/bash
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/curl.env"

fetch https://curl.se/download/curl-$CURL_VERSION.tar.gz "$CURL_SHA256"

# Host deps (apt): make, pkg-config (resolves sysroot OpenSSL/zlib .pc files
# via PKG_CONFIG_LIBDIR).
log_tool_name "curl $CURL_VERSION (sysroot)"
enter_source curl-$CURL_VERSION.tar.gz curl-$CURL_VERSION
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
leave_source
