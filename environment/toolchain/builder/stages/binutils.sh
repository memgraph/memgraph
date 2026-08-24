#!/bin/bash
# binutils: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
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
if [[ ! -f "$PREFIX/bin/ld" ]]; then
    if [[ -d "binutils-$BINUTILS_VERSION" ]]; then
        rm -rf binutils-$BINUTILS_VERSION
    fi
    tar -xvf ../archives/binutils-$BINUTILS_VERSION.tar.gz
    pushd "binutils-$BINUTILS_VERSION"
    mkdir build && pushd build
    BINUTILS_SPECIAL_FLAGS=""
    if [[ "$for_arm" = true ]]; then
        # influenced by: https://buildd.debian.org/status/fetch.php?pkg=binutils&arch=arm64&ver=2.37.90.20220130-2&stamp=1643576183&raw=0
        # NOTE: On ARM, on Debian 11, there are errors like gprofng/libcollector/dispatcher.c: multiple definition of pthread_sigmask.
        # A simple solution is to disable gprofng because Debian 11 is not the main OS used.
        if [[ "${DISTRO}" == "debian-11" ]]; then
            BINUTILS_SPECIAL_FLAGS="--disable-gprofng"
        fi
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
            ../configure \
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
fi

popd
