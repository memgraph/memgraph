#!/bin/bash
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/binutils.env"

pushd "$TC_ARCHIVES"
if [[ ! -f binutils-$BINUTILS_VERSION.tar.gz ]]; then
    wget --https-only https://sourceware.org/pub/binutils/releases/binutils-$BINUTILS_VERSION.tar.gz
    wget --https-only https://sourceware.org/pub/binutils/releases/binutils-$BINUTILS_VERSION.tar.gz.sig
    gpg --keyserver keyserver.ubuntu.com --recv-keys 3A24BC1E8FB409FA9F14371813FCEF89DD9E3C4F
    gpg --verify binutils-$BINUTILS_VERSION.tar.gz.sig binutils-$BINUTILS_VERSION.tar.gz
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): gcc, g++, make, bison (gprofng's parser); zlib is bundled.
log_tool_name "binutils $BINUTILS_VERSION"
tar -xvf ../archives/binutils-$BINUTILS_VERSION.tar.gz
pushd "binutils-$BINUTILS_VERSION"
mkdir build && pushd build
# gprofng builds against the builder's glibc rather than the sysroot, so its
# five collector libraries need dlopen and pthread_create at GLIBC_2.34 --
# the libdl and libpthread consolidation -- against a floor of 2.31. Nothing
# in this toolchain or in memgraph's profiling uses gprofng, so it is
# dropped rather than the floor being raised to accommodate one tool.
BINUTILS_SPECIAL_FLAGS="--disable-gprofng"
if [[ "$for_arm" = true ]]; then
    # influenced by: https://buildd.debian.org/status/fetch.php?pkg=binutils&arch=arm64&ver=2.37.90.20220130-2&stamp=1643576183&raw=0
    env \
        CC=gcc \
        CXX=g++ \
        CFLAGS="-g -O2" \
        CXXFLAGS="-g -O2" \
        LDFLAGS="" \
        ../configure $BINUTILS_SPECIAL_FLAGS \
            --build=aarch64-linux-gnu \
            --host=aarch64-linux-gnu \
            --prefix=$PREFIX \
            --with-sysroot=$SYSROOT \
            --enable-ld=default \
            --enable-gold \
            --enable-lto \
            --enable-plugins \
            --enable-shared \
            --enable-threads \
                --enable-deterministic-archives \
            --disable-compressed-debug-sections \
            --disable-x86-used-note \
            --enable-obsolete \
            --enable-new-dtags \
            --disable-werror
else
    # influenced by: https://buildd.debian.org/status/fetch.php?pkg=binutils&arch=amd64&ver=2.32-7&stamp=1553247092
    env \
        CC=gcc \
        CXX=g++ \
        CFLAGS="-g -O2" \
        CXXFLAGS="-g -O2" \
        LDFLAGS="" \
        ../configure $BINUTILS_SPECIAL_FLAGS \
            --build=x86_64-linux-gnu \
            --host=x86_64-linux-gnu \
            --prefix=$PREFIX \
            --with-sysroot=$SYSROOT \
            --enable-ld=default \
            --enable-gold \
            --enable-lto \
            --enable-plugins \
            --enable-shared \
            --enable-threads \
                --enable-deterministic-archives \
            --disable-compressed-debug-sections \
            --enable-new-dtags \
            --disable-werror
fi
make -j$CPUS
make install
popd && popd

popd
