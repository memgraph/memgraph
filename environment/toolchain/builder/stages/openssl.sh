#!/bin/bash
# openssl: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/openssl.env"

pushd "$TC_ARCHIVES"
if [[ ! -f openssl-$OPENSSL_VERSION.tar.gz ]]; then
    wget --https-only https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/openssl-$OPENSSL_VERSION.tar.gz
    echo "$OPENSSL_SHA256  openssl-$OPENSSL_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make, perl (openssl's Configure is a perl script).
log_tool_name "openssl $OPENSSL_VERSION (sysroot)"
tar -xzf ../archives/openssl-$OPENSSL_VERSION.tar.gz
pushd "openssl-$OPENSSL_VERSION"
./config --prefix=/usr \
    --openssldir=/usr/ssl \
    no-shared \
    no-dso \
    enable-ec_nistp_64_gcc_128 \
    enable-static-engine \
    enable-deprecated
make -j$CPUS
make install_sw DESTDIR=$SYSROOT
popd

popd
