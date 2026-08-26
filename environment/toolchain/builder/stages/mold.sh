#!/bin/bash
# mold: a parallel linker.
#
# Linking is the serial tail of every build: it cannot start until the last
# object is ready, and it is the step that decides how long an edit-rebuild
# cycle takes once compilation is cached. On a debug-heavy C++ link mold is
# several times faster than the ld this toolchain otherwise uses, at a fraction
# of the memory a large lld link wants.
#
# Shipping it here rather than relying on a copy from the build host is the
# point: a linker reached through the host's PATH is outside the sysroot, which
# is how an unmaintained gold came to be linking the compiler we ship.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/mold.env"

pushd "$TC_ARCHIVES"
if [[ ! -f mold-$MOLD_VERSION.tar.gz ]]; then
    wget --https-only https://github.com/rui314/mold/archive/refs/tags/v$MOLD_VERSION.tar.gz -O mold-$MOLD_VERSION.tar.gz
    echo "$MOLD_SHA256  mold-$MOLD_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make ("gcc"/"g++" resolve to the toolchain via PATH).
# mold carries its own TBB, mimalloc, blake3, xxhash, zlib and zstd, so this
# adds no sysroot library and no stage has to run before it but cmake.
log_tool_name "mold $MOLD_VERSION"
if [[ ! -f "$PREFIX/bin/mold" ]]; then
    if [[ -d "mold-$MOLD_VERSION" ]]; then
        rm -rf mold-$MOLD_VERSION
    fi
    tar -xzf ../archives/mold-$MOLD_VERSION.tar.gz
    pushd "mold-$MOLD_VERSION"
    # $ORIGIN, not an absolute path, so the installed tree can be moved: mold
    # needs the libstdc++ this toolchain builds, which is newer than the
    # sysroot's. The vendored dependencies are linked statically, which is
    # their default and why nothing here points at the system's copies.
    cmake -S . -B build \
        -DCMAKE_INSTALL_PREFIX=$PREFIX \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_EXE_LINKER_FLAGS="-L$PREFIX/lib64 -Wl,-rpath,\$ORIGIN/../lib64" \
        -DMOLD_USE_SYSTEM_TBB=OFF \
        -DMOLD_USE_SYSTEM_MIMALLOC=OFF
    cmake --build build -j$CPUS
    cmake --install build
    popd
fi

popd
