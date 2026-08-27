#!/bin/bash
# swig: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/swig.env"

fetch https://github.com/swig/swig/archive/refs/tags/v$SWIG_VERSION.tar.gz "$SWIG_SHA256" swig-$SWIG_VERSION.tar.gz
fetch https://github.com/PCRE2Project/pcre2/releases/download/pcre2-$PCRE2_VERSION/pcre2-$PCRE2_VERSION.tar.gz "$PCRE2_SHA256"

pushd "$TC_BUILD"
# Host deps (apt): autoconf (via automake), automake, libtool, bison, make —
# PCRE2 is built from our own pinned tarball, not libpcre2-dev.
log_tool_name "swig $SWIG_VERSION"
tar -xvf ../archives/swig-$SWIG_VERSION.tar.gz
pushd "swig-$SWIG_VERSION"
./autogen.sh
mkdir build && pushd build
# SWIG 4.4 has a hard PCRE2 build-time dependency. Tools/pcre-build.sh
# expects a pcre2-*.tar* in the directory configure runs from and stages
# a static PCRE2 in pcre/pcre-swig-install, which configure auto-detects.
cp ../../../archives/pcre2-$PCRE2_VERSION.tar.gz .
../Tools/pcre-build.sh
../configure --prefix=$DIR/build/swig-$SWIG_VERSION/install
make -j$CPUS
make install
popd && popd

popd
