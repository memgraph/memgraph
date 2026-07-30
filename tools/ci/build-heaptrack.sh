#!/bin/bash
set -e  # Exit on any error

ARCH="$(uname -m)"

# Clean up existing directories if they exist
if [ -d "heaptrack" ]; then
    echo "Removing existing heaptrack directory..."
    rm -rf heaptrack
fi

if [ -d "build" ]; then
    echo "Removing existing build directory..."
    rm -rf build
fi

# Clone and checkout the specific version
echo "Cloning heaptrack repository..."
git clone https://github.com/KDE/heaptrack.git
cd heaptrack
git checkout v1.5.0

# Create build directory
mkdir build
cd build

# Configure with CMake, using clang from toolchain and helping it find system libraries.
# Boost, libdw/libelf and libstdc++/libgcc are linked statically so the resulting
# binaries depend only on glibc (plus ubiquitous compression libs for heaptrack_print).
echo "Configuring with CMake..."
LIBDIR="/usr/lib/$ARCH-linux-gnu"

# Boost's CMake config links its compression deps as bare "-lz -lbz2 ..." flags,
# which the linker resolves to shared libs. Point it at a directory holding only
# the static archives so those flags resolve statically instead.
STATIC_LIB_DIR="$(pwd)/static-libs"
mkdir -p "$STATIC_LIB_DIR"
ln -sf "$LIBDIR"/lib{z,bz2,lzma,zstd}.a "$STATIC_LIB_DIR/"

cmake .. \
    -DCMAKE_C_COMPILER=$(which clang) \
    -DCMAKE_CXX_COMPILER=$(which clang++) \
    -DCMAKE_BUILD_TYPE=Release \
    -DHEAPTRACK_BUILD_GUI=OFF \
    -DHEAPTRACK_USE_LIBUNWIND=OFF \
    -DCMAKE_INSTALL_PREFIX="/tmp/heaptrack" \
    -DCMAKE_PREFIX_PATH="/usr" \
    -DCMAKE_LIBRARY_PATH="$LIBDIR" \
    -DCMAKE_INCLUDE_PATH="/usr/include" \
    -DBoost_USE_STATIC_LIBS=ON \
    -DZLIB_USE_STATIC_LIBS=ON \
    -DLIBDW_LIBRARIES="$LIBDIR/libdw.a;$LIBDIR/libelf.a;$LIBDIR/libz.a;$LIBDIR/liblzma.a;$LIBDIR/libzstd.a;$LIBDIR/libbz2.a" \
    -DLIBDW_INCLUDE_DIR="/usr/include" \
    -DCMAKE_EXE_LINKER_FLAGS="-static-libstdc++ -static-libgcc -L$STATIC_LIB_DIR" \
    -DCMAKE_MODULE_LINKER_FLAGS="-static-libstdc++ -static-libgcc -Wl,--exclude-libs,ALL" \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -Wno-dev

# Build
echo "Building heaptrack..."
make -j$(nproc)

echo "Build completed successfully!"
make install
echo "Installed heaptrack to /tmp/heaptrack"
