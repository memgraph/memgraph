#!/bin/bash
# libipt: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/libipt.env"

pushd "$TC_ARCHIVES"
if [[ ! -f libipt-$LIBIPT_VERSION.tar.gz ]]; then
    wget --https-only https://github.com/intel/libipt/archive/refs/tags/v$LIBIPT_VERSION.tar.gz -O libipt-$LIBIPT_VERSION.tar.gz
    LIBIPT_SHA256="f09a18fefba81d4fc2530d90858789e0c596f1b634e5777e6ccaf492966e9845"
    echo "$LIBIPT_SHA256  libipt-$LIBIPT_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make — built with the toolchain cmake/gcc from above.
# Intel PT decoding for GDB's btrace; x86-only (Intel PT is x86 hardware).
if [[ "$for_arm" = false ]]; then
    log_tool_name "libipt $LIBIPT_VERSION (sysroot)"
    if [[ ! -f "$SYSROOT/usr/lib/libipt.a" ]]; then
        if [[ -d "libipt-$LIBIPT_VERSION" ]]; then
            rm -rf libipt-$LIBIPT_VERSION
        fi
        tar -xzf ../archives/libipt-$LIBIPT_VERSION.tar.gz
        pushd "libipt-$LIBIPT_VERSION"
        mkdir build && pushd build
        # Static so GDB doesn't ship another .so; LIBDIR pinned because
        # GNUInstallDirs otherwise picks lib/lib64 based on the build host.
        # THREADS off: C11 mtx_* lives in libpthread until glibc 2.34, but
        # GDB's configure links plain -lipt (and decodes single-threaded).
        cmake .. \
            -DCMAKE_INSTALL_PREFIX=/usr \
            -DCMAKE_INSTALL_LIBDIR=lib \
            -DCMAKE_BUILD_TYPE=Release \
            -DBUILD_SHARED_LIBS=OFF \
            -DFEATURE_THREADS=OFF \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        make -j$CPUS
        make install DESTDIR=$SYSROOT
        popd && popd
    fi
fi
popd
