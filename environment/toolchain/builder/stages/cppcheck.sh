#!/bin/bash
# cppcheck: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/cppcheck.env"

pushd "$TC_ARCHIVES"
if [[ ! -f cppcheck-$CPPCHECK_VERSION.tar.gz ]]; then
    wget --https-only https://github.com/cppcheck-opensource/cppcheck/archive/refs/tags/$CPPCHECK_VERSION.tar.gz -O cppcheck-$CPPCHECK_VERSION.tar.gz
    CPPCHECK_SHA256="ba750bd872ad7c01f951ff2d9dc8c68ea5852654545ec7a62a4c318d690c8e22"
    echo "$CPPCHECK_SHA256  cppcheck-$CPPCHECK_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make ("gcc"/"g++" resolve to the toolchain via PATH).
# The runpath keeps cppcheck on the libstdc++ built here rather than the one on
# whatever machine it is installed to; relative, so the tree still moves.
log_tool_name "cppcheck $CPPCHECK_VERSION"
if [[ ! -f "$PREFIX/bin/cppcheck" ]]; then
    if [[ -d "cppcheck-$CPPCHECK_VERSION" ]]; then
        rm -rf cppcheck-$CPPCHECK_VERSION
    fi
    tar -xvf ../archives/cppcheck-$CPPCHECK_VERSION.tar.gz
    pushd "cppcheck-$CPPCHECK_VERSION"
    env \
        CC=gcc \
        CXX=g++ \
        LDFLAGS="-Wl,-rpath,\$ORIGIN/../lib64" \
        PREFIX=$PREFIX \
        FILESDIR=$PREFIX/share/cppcheck \
        CFGDIR=$PREFIX/share/cppcheck/cfg \
            make -j$CPUS
    env \
        CC=gcc \
        CXX=g++ \
        LDFLAGS="-Wl,-rpath,\$ORIGIN/../lib64" \
        PREFIX=$PREFIX \
        FILESDIR=$PREFIX/share/cppcheck \
        CFGDIR=$PREFIX/share/cppcheck/cfg \
            make install
    popd
fi

popd
