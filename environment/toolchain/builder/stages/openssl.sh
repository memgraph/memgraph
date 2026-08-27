#!/bin/bash
# openssl: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/openssl.env"

fetch https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/openssl-$OPENSSL_VERSION.tar.gz "$OPENSSL_SHA256"

# Host deps (apt): make, perl (openssl's Configure is a perl script).
log_tool_name "openssl $OPENSSL_VERSION (sysroot)"
enter_source openssl-$OPENSSL_VERSION.tar.gz openssl-$OPENSSL_VERSION
./config --prefix=/usr \
    --openssldir=/usr/ssl \
    no-shared \
    no-dso \
    enable-ec_nistp_64_gcc_128 \
    enable-static-engine \
    enable-deprecated
make -j$CPUS
make install_sw DESTDIR=$SYSROOT
leave_source
