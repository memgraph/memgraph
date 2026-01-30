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

# Configure with CMake, using clang from toolchain and helping it find system libraries
echo "Configuring with CMake..."
cmake .. \
    -DCMAKE_C_COMPILER=$(which clang) \
    -DCMAKE_CXX_COMPILER=$(which clang++) \
    -DCMAKE_BUILD_TYPE=Release \
    -DHEAPTRACK_BUILD_GUI=OFF \
    -DHEAPTRACK_USE_LIBUNWIND=OFF \
    -DCMAKE_INSTALL_PREFIX="/tmp/heaptrack" \
    -DCMAKE_PREFIX_PATH="/usr" \
    -DCMAKE_LIBRARY_PATH="/usr/lib/$ARCH-linux-gnu" \
    -DCMAKE_INCLUDE_PATH="/usr/include" \
    -DLIBDW_LIBRARIES="/usr/lib/$ARCH-linux-gnu/libdw.so" \
    -DLIBDW_INCLUDE_DIR="/usr/include" \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -Wno-dev

# Build
echo "Building heaptrack..."
make -j$(nproc)

echo "Build completed successfully!"
make install
echo "Installed heaptrack to /tmp/heaptrack"
