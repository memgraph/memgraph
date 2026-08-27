#!/bin/bash
# cppcheck: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/cppcheck.env"

pushd "$TC_ARCHIVES"
if [[ ! -f cppcheck-$CPPCHECK_VERSION.tar.gz ]]; then
    wget --https-only https://github.com/cppcheck-opensource/cppcheck/archive/refs/tags/$CPPCHECK_VERSION.tar.gz -O cppcheck-$CPPCHECK_VERSION.tar.gz
    echo "$CPPCHECK_SHA256  cppcheck-$CPPCHECK_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make ("gcc"/"g++" resolve to the toolchain via PATH).
# The runpath keeps cppcheck on the libstdc++ built here rather than the one on
# whatever machine it is installed to; relative, so the tree still moves. It is
# spelled for make rather than the shell: a bare $O reads as a make variable and
# leaves RIGIN behind, and the recipe's shell expansion eats it a second time.
log_tool_name "cppcheck $CPPCHECK_VERSION"
tar -xvf ../archives/cppcheck-$CPPCHECK_VERSION.tar.gz
pushd "cppcheck-$CPPCHECK_VERSION"
env \
    CC=gcc \
    CXX=g++ \
    LDFLAGS="-Wl,-rpath,\\\$\$ORIGIN/../lib64" \
    PREFIX=$PREFIX \
    FILESDIR=$PREFIX/share/cppcheck \
    CFGDIR=$PREFIX/share/cppcheck/cfg \
        make -j$CPUS
env \
    CC=gcc \
    CXX=g++ \
    LDFLAGS="-Wl,-rpath,\\\$\$ORIGIN/../lib64" \
    PREFIX=$PREFIX \
    FILESDIR=$PREFIX/share/cppcheck \
    CFGDIR=$PREFIX/share/cppcheck/cfg \
        make install
popd

popd
