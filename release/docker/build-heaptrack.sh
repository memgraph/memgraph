#!/bin/bash
set -e  # Exit on any error

# Clone and checkout the specific version
echo "Cloning heaptrack repository..."
git clone https://github.com/KDE/heaptrack.git
chown -R memgraph:memgraph heaptrack

# Switch to memgraph user for toolchain activation and build
echo "Switching to memgraph user for build..."
su - memgraph -c "
    source /opt/toolchain-v6/activate
    cd /root/heaptrack
    git checkout v1.5.0
    
    # Create build directory
    mkdir build
    cd build
    
    # Configure with CMake, using clang from toolchain and helping it find system libraries
    echo 'Configuring with CMake...'
    cmake .. \
        -DCMAKE_C_COMPILER=\$(which clang) \
        -DCMAKE_CXX_COMPILER=\$(which clang++) \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DHEAPTRACK_BUILD_GUI=OFF \
        -DHEAPTRACK_USE_LIBUNWIND=OFF \
        -DCMAKE_INSTALL_PREFIX='/usr' \
        -DCMAKE_PREFIX_PATH='/usr' \
        -DCMAKE_LIBRARY_PATH='/usr/lib/x86_64-linux-gnu' \
        -DCMAKE_INCLUDE_PATH='/usr/include' \
        -DLIBDW_LIBRARIES='/usr/lib/x86_64-linux-gnu/libdw.so' \
        -DLIBDW_INCLUDE_DIR='/usr/include' \
        -Wno-dev
    
    # Build
    echo 'Building heaptrack...'
    make -j\$(nproc)
    
    echo 'Build completed successfully!'
    deactivate
"

# Switch back to root for installation
echo "Installing heaptrack as root..."
cd /heaptrack/build
make install
echo "Installed heaptrack to /usr"

rm -rf /heaptrack
