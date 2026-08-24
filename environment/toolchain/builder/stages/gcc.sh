#!/bin/bash
# gcc: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/gcc.env"
source "$TC_VERSIONS/glibc.env"

pushd "$TC_ARCHIVES"
if [[ ! -f gcc-$GCC_VERSION.tar.gz ]]; then
    wget --https-only https://mirrorservice.org/sites/sourceware.org/pub/gcc/releases/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz
    wget --https-only https://mirrorservice.org/sites/sourceware.org/pub/gcc/releases/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz.sig
    gpg --keyserver keyserver.ubuntu.com --recv-keys 6C35B99309B5FA62 7F74F97C103468EE5D750B583AB00996FC26A641
    gpg --verify gcc-$GCC_VERSION.tar.gz.sig gcc-$GCC_VERSION.tar.gz
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): build-essential, m4 (in-tree gmp), wget + bzip2
# (download_prerequisites fetches gmp/mpfr/mpc/isl as .tar.bz2).
log_tool_name "GCC $GCC_VERSION"
if [[ ! -f "$PREFIX/bin/gcc" ]]; then
    if [[ -d "gcc-$GCC_VERSION" ]]; then
        rm -rf gcc-$GCC_VERSION
    fi
    tar -xvf ../archives/gcc-$GCC_VERSION.tar.gz
    pushd "gcc-$GCC_VERSION"
    ./contrib/download_prerequisites
    mkdir build && pushd build
    # influenced by: https://buildd.debian.org/status/fetch.php?pkg=gcc-11&arch=arm64&ver=11.2.0-14&stamp=1642052446&raw=0
    if [[ "$for_arm" = true ]]; then
        ../configure -v \
            --prefix=$PREFIX \
            --with-sysroot=$SYSROOT \
            --with-build-sysroot=$SYSROOT \
            --with-glibc-version=$GLIBC_VERSION \
            --disable-multilib \
            --enable-languages=c,c++,fortran \
            --enable-gold=yes \
            --enable-ld=yes \
            --disable-vtable-verify \
            --enable-libmpx \
            --without-cuda-driver \
            --enable-shared \
            --enable-linker-build-id \
            --without-included-gettext \
            --enable-threads=posix \
            --enable-nls \
            --enable-bootstrap \
            --enable-clocale=gnu \
            --enable-libstdcxx-debug \
            --enable-libstdcxx-time=yes \
            --with-default-libstdcxx-abi=new \
            --enable-gnu-unique-object \
            --disable-libquadmath \
            --disable-libquadmath-support \
            --enable-plugin \
            --enable-default-pie \
            --enable-libphobos-checking=release \
            --enable-objc-gc=auto \
            --enable-multiarch \
            --enable-fix-cortex-a53-843419 \
            --disable-werror \
            --enable-checking=release \
            --build=aarch64-linux-gnu \
            --host=aarch64-linux-gnu \
            --target=aarch64-linux-gnu \
            --with-build-config=bootstrap-lto-lean \
            --enable-link-serialization=4
    else
        # influenced by: https://buildd.debian.org/status/fetch.php?pkg=gcc-8&arch=amd64&ver=8.3.0-6&stamp=1554588545
        ../configure -v \
            --build=x86_64-linux-gnu \
            --host=x86_64-linux-gnu \
            --target=x86_64-linux-gnu \
            --prefix=$PREFIX \
            --with-sysroot=$SYSROOT \
            --with-build-sysroot=$SYSROOT \
            --with-glibc-version=$GLIBC_VERSION \
            --disable-multilib \
            --enable-checking=release \
            --enable-languages=c,c++,fortran \
            --enable-gold=yes \
            --enable-ld=yes \
            --enable-lto \
            --enable-bootstrap \
            --disable-vtable-verify \
            --disable-werror \
            --without-included-gettext \
            --enable-threads=posix \
            --enable-nls \
            --enable-clocale=gnu \
            --enable-libstdcxx-debug \
            --enable-libstdcxx-time=yes \
            --enable-gnu-unique-object \
            --enable-libmpx \
            --enable-plugin \
            --enable-default-pie \
            --with-tune=generic \
            --without-cuda-driver
    fi
    make -j$CPUS
    make install
    popd && popd
fi

# activate toolchain
export PATH=$PREFIX/bin:$PATH
export LD_LIBRARY_PATH=$PREFIX/lib64
# Pin CC/CXX so subsequent configure runs (gmp, mpfr, gdb, ...) don't fall back
# to the host /usr/bin/cc. Without this, autoconf prefers `cc` and we end up
# linking host-glibc symbols into libraries that should target the sysroot.
export CC=$PREFIX/bin/gcc
export CXX=$PREFIX/bin/g++
# Point pkg-config at the sysroot so cmake / configure scripts that use it
# (e.g. cmake's --system-curl) resolve to the sysroot's .pc files rather than
# the host's, which would otherwise drag in /usr/include and host-glibc deps.
# PKG_CONFIG_LIBDIR _replaces_ the default search path (no host fallback);
# PKG_CONFIG_SYSROOT_DIR rewrites the -I/-L paths in those .pc files.
export PKG_CONFIG_LIBDIR=$SYSROOT/usr/lib/pkgconfig:$SYSROOT/usr/lib64/pkgconfig:$SYSROOT/usr/share/pkgconfig
export PKG_CONFIG_SYSROOT_DIR=$SYSROOT

popd
