#!/bin/bash
# openssl: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/openssl.env"

pushd "$TC_ARCHIVES"
if [[ ! -f openssl-$OPENSSL_VERSION.tar.gz ]]; then
    wget --https-only https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/openssl-$OPENSSL_VERSION.tar.gz
    OPENSSL_SHA256="243a86649cf6f23eeb6a2ff2456e09e5d77dd9018a54d3d96b0c6bdd6ba6c7f1"
    echo "$OPENSSL_SHA256  openssl-$OPENSSL_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make, perl (openssl's Configure is a perl script).
log_tool_name "openssl $OPENSSL_VERSION (sysroot)"
if [[ ! -f "$SYSROOT/usr/lib64/libssl.a" && ! -f "$SYSROOT/usr/lib/libssl.a" ]]; then
    if [[ -d "openssl-$OPENSSL_VERSION" ]]; then
        rm -rf openssl-$OPENSSL_VERSION
    fi
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
fi

popd
