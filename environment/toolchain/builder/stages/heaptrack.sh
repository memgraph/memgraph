#!/bin/bash
# heaptrack: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/heaptrack.env"
source /tc/lib/clang-env.sh

pushd "$TC_BUILD"
log_tool_name "heaptrack $HEAPTRACK_TAG"
if [[ ! -f "$PREFIX/bin/heaptrack" ]]; then
    if [[ -d heaptrack ]]; then
        rm -rf heaptrack
    fi
    git clone https://github.com/KDE/heaptrack.git heaptrack
    pushd heaptrack
    git checkout $HEAPTRACK_TAG
    # heaptrack links against the HOST's boost/libdw (the sysroot doesn't carry
    # them), so the sysroot-pinning CXXFLAGS/LDFLAGS are cleared below. All of
    # boost, libdw and libstdc++ are linked statically, leaving only glibc
    # dynamic — the builder's glibc (2.31 on ubuntu-20.04) matches the sysroot
    # pin, so heaptrack runs anywhere the toolchain itself does.
    HOST_LIBDIR="/usr/lib/$(uname -m)-linux-gnu"
    # Elfutils 0.176 (20.04) predates the libebl->libdw merge (0.178), so
    # libebl.a must be linked explicitly, and it needs libdl for its runtime
    # backend dlopen. Missing backends on target machines degrade gracefully:
    # symbolization works via the default hooks (verified on bare 20.04/12).
    # Boost's cmake config links its compression deps as bare "-lz -lbz2 ..."
    # flags, which resolve to shared libs. Point the linker at a directory
    # holding only the static archives so those flags resolve statically.
    mkdir -p static-libs
    ln -sf "$HOST_LIBDIR"/lib{z,bz2,lzma,zstd}.a static-libs/
    env CFLAGS="" CXXFLAGS="" LDFLAGS="" \
    cmake -B build \
        -DCMAKE_C_COMPILER=$CC \
        -DCMAKE_CXX_COMPILER=$CXX \
        -DCMAKE_C_FLAGS="--gcc-toolchain=$PREFIX" \
        -DCMAKE_CXX_FLAGS="--gcc-toolchain=$PREFIX" \
        -DCMAKE_BUILD_TYPE=Release \
        -DHEAPTRACK_BUILD_GUI=OFF \
        -DHEAPTRACK_USE_LIBUNWIND=OFF \
        -DCMAKE_INSTALL_PREFIX=$PREFIX \
        -DBoost_USE_STATIC_LIBS=ON \
        -DZLIB_USE_STATIC_LIBS=ON \
        -DZSTD_LIBRARY=$HOST_LIBDIR/libzstd.a \
        -DLIBDW_LIBRARIES="$HOST_LIBDIR/libdw.a;$HOST_LIBDIR/libebl.a;$HOST_LIBDIR/libelf.a;$HOST_LIBDIR/libz.a;$HOST_LIBDIR/liblzma.a;$HOST_LIBDIR/libbz2.a;$HOST_LIBDIR/libdl.so" \
        -DLIBDW_INCLUDE_DIR=/usr/include \
        -DCMAKE_EXE_LINKER_FLAGS="--gcc-toolchain=$PREFIX -static-libstdc++ -static-libgcc -L$PWD/static-libs" \
        -DCMAKE_MODULE_LINKER_FLAGS="--gcc-toolchain=$PREFIX -static-libstdc++ -static-libgcc -Wl,--exclude-libs,ALL" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -Wno-dev
    cmake --build build -j$CPUS
    cmake --build build --target install
    popd
fi

popd
