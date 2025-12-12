#!/bin/bash
set -euo pipefail

# helpers
pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Get CPU count on macOS
if command -v sysctl &> /dev/null; then
    CPUS=$(sysctl -n hw.ncpu)
else
    CPUS=4
fi

cd "$DIR"

function log_tool_name () {
    echo ""
    echo ""
    echo "#### $1 ####"
    echo ""
    echo ""
}

# Check we're on macOS
if [[ "$(uname)" != "Darwin" ]]; then
    echo "This script is for macOS only. Use build.sh for Linux."
    exit 1
fi

TOOLCHAIN_VERSION=7

# check installation directory
NAME=toolchain-v$TOOLCHAIN_VERSION
PREFIX=/opt/$NAME
mkdir -p $PREFIX >/dev/null 2>/dev/null || true
if [ ! -d $PREFIX ] || [ ! -w $PREFIX ]; then
    echo "Please make sure that the directory '$PREFIX' exists and is writable by the current user!"
    echo
    echo "If unsure, execute these commands:"
    echo "    sudo mkdir -p $PREFIX && sudo chown $USER:$USER $PREFIX"
    echo
    echo "Press <return> when you have created the directory and granted permissions."
    # wait for the directory to be created
    while true; do
        read
        if [ ! -d $PREFIX ] || [ ! -w $PREFIX ]; then
            echo
            echo "You can't continue before you have created the directory and granted permissions!"
            echo
            echo "Press <return> when you have created the directory and granted permissions."
        else
            break
        fi
    done
fi

# Create bin directory and symlink system tools
mkdir -p $PREFIX/bin
log_tool_name "Setting up system tool symlinks"
# Prefer LLVM clang over Apple clang if available
LLVM_PREFIX="/opt/homebrew/opt/llvm"
if [[ -x "$LLVM_PREFIX/bin/clang" && -x "$LLVM_PREFIX/bin/clang++" ]]; then
    echo "Using LLVM clang from $LLVM_PREFIX"
    # Symlink LLVM clang tools
    for tool in clang clang++; do
        if [[ -x "$LLVM_PREFIX/bin/$tool" ]]; then
            ln -sf "$LLVM_PREFIX/bin/$tool" "$PREFIX/bin/$tool" || true
        fi
    done
    # Symlink other LLVM tools if available (including clang-scan-deps for C++ modules)
    for tool in llvm-ar llvm-ranlib llvm-nm llvm-objcopy llvm-objdump llvm-strip lld clang-scan-deps; do
        if [[ -x "$LLVM_PREFIX/bin/$tool" ]]; then
            ln -sf "$LLVM_PREFIX/bin/$tool" "$PREFIX/bin/$tool" || true
        elif command -v $tool &> /dev/null; then
            ln -sf "$(which $tool)" "$PREFIX/bin/$tool" || true
        fi
    done
else
    echo "Using system clang (Apple clang)"
    # Symlink system clang tools
    for tool in clang clang++ llvm-ar llvm-ranlib llvm-nm llvm-objcopy llvm-objdump llvm-strip lld; do
        if command -v $tool &> /dev/null; then
            ln -sf "$(which $tool)" "$PREFIX/bin/$tool" || true
        fi
    done
fi
# Symlink cmake, ninja if available
for tool in cmake ninja; do
    if command -v $tool &> /dev/null; then
        ln -sf "$(which $tool)" "$PREFIX/bin/$tool" || true
    fi
done

# Check for required tools
if ! command -v clang &> /dev/null; then
    echo "Error: clang not found. Please install Xcode Command Line Tools:"
    echo "    xcode-select --install"
    exit 1
fi

if ! command -v cmake &> /dev/null; then
    echo "Error: cmake not found. Please install via Homebrew:"
    echo "    brew install cmake"
    exit 1
fi

# Check for autotools (needed for jemalloc and potentially others)
if ! command -v autoconf &> /dev/null || ! command -v automake &> /dev/null || ! command -v libtool &> /dev/null; then
    echo "Error: autotools (autoconf, automake, libtool) not found. Please install via Homebrew:"
    echo "    brew install autoconf automake libtool"
    exit 1
fi

# Check for curl (needed for Pulsar)
if ! brew list curl &> /dev/null; then
    echo "Warning: curl not installed via Homebrew. Pulsar build may fail."
    echo "To install: brew install curl"
    echo "Continuing anyway..."
fi

# Set up common build flags for macOS (matching Linux approach)
export CC=clang
export CXX=clang++
export CFLAGS="-fPIC"
export CXXFLAGS="-fPIC -stdlib=libc++"
export LDFLAGS=""

COMMON_CMAKE_FLAGS="-DCMAKE_INSTALL_PREFIX=$PREFIX
                    -DCMAKE_PREFIX_PATH=$PREFIX
                    -DCMAKE_BUILD_TYPE=Release
                    -DCMAKE_C_COMPILER=$CC
                    -DCMAKE_CXX_COMPILER=$CXX
                    -DBUILD_SHARED_LIBS=OFF
                    -DCMAKE_CXX_STANDARD=20
                    -DBUILD_TESTING=OFF
                    -DCMAKE_REQUIRED_INCLUDES=$PREFIX/include
                    -DCMAKE_POSITION_INDEPENDENT_CODE=ON"

COMMON_CONFIGURE_FLAGS="--enable-shared=no --prefix=$PREFIX"

# Library versions (matching Linux toolchain)
GFLAGS_COMMIT_HASH=b37ceb03a0e56c9f15ce80409438a555f8a67b7c
PROTOBUF_TAG="v3.12.4"
JEMALLOC_VERSION=5.2.1
ROCKSDB_TAG="v8.1.1"
NURAFT_COMMIT_HASH="4b148a7e76291898c838a7457eeda2b16f7317ea"
PULSAR_TAG="v3.7.1"
LIBBCRYPT_TAG="8aa32ad94ebe06b76853b0767c910c9fbf7ccef4"
LIBRDTSC_TAG="v0.3"
MGCONSOLE_TAG="v1.4.0"
CURL_TAG="curl-8_15_0"
ZLIB_VERSION="1.3.1"
BOOST_VERSION="1.86.0"
BOOST_VERSION_UNDERSCORES="1_86_0"
SNAPPY_VERSION="1.2.2"
ZSTD_VERSION="1.5.6"

# create build directory
mkdir -p build
pushd build

log_tool_name "gflags (memgraph fork $GFLAGS_COMMIT_HASH)"
if [[ ! -d "$PREFIX/include/gflags" ]]; then
    if [[ -d gflags ]]; then
        rm -rf gflags
    fi
    git clone https://github.com/memgraph/gflags.git gflags
    pushd gflags
    git checkout $GFLAGS_COMMIT_HASH
    # Remove BUILD file if it exists (conflicts with build directory on case-insensitive filesystems)
    rm -f BUILD
    mkdir -p build
    pushd build
    cmake .. $COMMON_CMAKE_FLAGS \
        -DREGISTER_INSTALL_PREFIX=OFF \
        -DBUILD_gflags_nothreads_LIB=OFF \
        -DGFLAGS_NO_FILENAMES=1 \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    make -j$CPUS install
    popd && popd
fi

log_tool_name "protobuf $PROTOBUF_TAG"
if [[ ! -f "$PREFIX/lib/libprotobuf.a" ]]; then
    if [[ -d protobuf ]]; then
        rm -rf protobuf
    fi
    git clone https://github.com/protocolbuffers/protobuf.git protobuf
    pushd protobuf
    git checkout $PROTOBUF_TAG
    # Use CMake from cmake/ subdirectory (protobuf v3.12.4 has CMake support there)
    pushd cmake
    # Patch CMakeLists.txt to require newer CMake version (compatibility with < 3.5 removed in newer CMake)
    sed -i '' 's/cmake_minimum_required(VERSION 3.1.3)/cmake_minimum_required(VERSION 3.5)/' CMakeLists.txt
    mkdir -p _build && pushd _build
    cmake .. $COMMON_CMAKE_FLAGS \
        -Dprotobuf_BUILD_TESTS=OFF \
        -Dprotobuf_BUILD_EXAMPLES=OFF
    make -j$CPUS install
    popd && popd && popd
fi

log_tool_name "jemalloc $JEMALLOC_VERSION"
if [[ ! -d "$PREFIX/include/jemalloc" ]]; then
    if [ -d jemalloc ]; then
        rm -rf jemalloc
    fi
    git clone https://github.com/jemalloc/jemalloc.git jemalloc
    pushd jemalloc
    git checkout $JEMALLOC_VERSION
    ./autogen.sh
    ./configure \
      --disable-cxx \
      --with-lg-page=12 \
      --with-lg-hugepage=21 \
      --with-jemalloc-prefix=je_ \
      --enable-shared=no --prefix=$PREFIX \
      --with-malloc-conf="background_thread:true,retain:false,percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000"
    make -j$CPUS install
    popd
fi

log_tool_name "rocksdb $ROCKSDB_TAG"
if [[ ! -f "$PREFIX/lib/librocksdb.a" ]]; then
    if [[ -d rocksdb ]]; then
        rm -rf rocksdb
    fi
    git clone https://github.com/facebook/rocksdb.git rocksdb
    pushd rocksdb
    git checkout $ROCKSDB_TAG
    # Apply patch if it exists
    if [[ -f "$DIR/rocksdb-$ROCKSDB_TAG.patch" ]]; then
        git apply "$DIR/rocksdb-$ROCKSDB_TAG.patch"
    fi
    mkdir -p _build && pushd _build
    cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$PREFIX \
      -DUSE_RTTI=ON \
      -DWITH_TESTS=OFF \
      -DGFLAGS_NOTHREADS=OFF \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=true \
      -DCMAKE_CXX_FLAGS="-include stdint.h -Wno-error=nontrivial-memaccess" \
      -DPORTABLE=ON \
      ..
    make -j$CPUS rocksdb rocksdb-shared install
    popd && popd
fi

log_tool_name "nuraft $NURAFT_COMMIT_HASH"
if [[ ! -f "$PREFIX/lib/libnuraft.a" ]]; then
    if [[ -d nuraft ]]; then
        rm -rf nuraft
    fi
    git clone https://github.com/eBay/NuRaft.git nuraft
    pushd nuraft
    git checkout $NURAFT_COMMIT_HASH
    # Apply patch if it exists
    if [[ -f "$DIR/nuraft.patch" ]]; then
        git apply "$DIR/nuraft.patch"
    fi
    ./prepare.sh # Downloads ASIO.
    cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$PREFIX \
      .
    make -j$CPUS install
    popd
fi

log_tool_name "zlib $ZLIB_VERSION"
if [[ ! -f "$PREFIX/lib/libz.a" ]]; then
    if [[ -d zlib-$ZLIB_VERSION ]]; then
        rm -rf zlib-$ZLIB_VERSION
    fi
    # Download zlib
    if [[ ! -f zlib-$ZLIB_VERSION.tar.gz ]]; then
        curl -L https://zlib.net/zlib-$ZLIB_VERSION.tar.gz -o zlib-$ZLIB_VERSION.tar.gz
    fi
    tar -xzf zlib-$ZLIB_VERSION.tar.gz
    pushd zlib-$ZLIB_VERSION
    mkdir -p build && pushd build
    cmake .. $COMMON_CMAKE_FLAGS
    make -j$CPUS install
    popd && popd
fi

log_tool_name "curl $CURL_TAG"
if [[ ! -f "$PREFIX/lib/libcurl.a" ]]; then
    if [[ -d curl ]]; then
        rm -rf curl
    fi
    git clone https://github.com/curl/curl.git curl
    pushd curl
    git checkout $CURL_TAG
    # Find OpenSSL from Homebrew
    OPENSSL_PREFIX="/opt/homebrew/opt/openssl@3"
    if [[ ! -d "$OPENSSL_PREFIX" ]]; then
        OPENSSL_PREFIX="/opt/homebrew/opt/openssl"
    fi
    if [[ ! -d "$OPENSSL_PREFIX" ]]; then
        echo "Error: OpenSSL not found. Please install via Homebrew:"
        echo "    brew install openssl@3"
        exit 1
    fi
    cmake -B build -DCURL_ENABLE_SSL=ON \
        -DCURL_USE_LIBSSH2=OFF \
        -DCURL_USE_LIBPSL=OFF \
        -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_STATIC_LIBS=ON \
        -DUSE_NGHTTP2=OFF \
        -DUSE_LIBIDN2=OFF \
        -DCURL_DISABLE_LDAP=ON \
        -DOPENSSL_ROOT_DIR=$OPENSSL_PREFIX \
        -DOPENSSL_INCLUDE_DIR=$OPENSSL_PREFIX/include \
        -DOPENSSL_SSL_LIBRARY=$OPENSSL_PREFIX/lib/libssl.a \
        -DOPENSSL_CRYPTO_LIBRARY=$OPENSSL_PREFIX/lib/libcrypto.a \
        -DZLIB_ROOT=$PREFIX \
        -DZLIB_INCLUDE_DIR=$PREFIX/include \
        -DZLIB_LIBRARY=$PREFIX/lib/libz.a \
        $COMMON_CMAKE_FLAGS
    pushd build
    make -j$CPUS
    make install
    popd && popd
fi

log_tool_name "zstd $ZSTD_VERSION"
if [[ ! -f "$PREFIX/include/zstd.h" ]]; then
    if [[ -d zstd-$ZSTD_VERSION ]]; then
        rm -rf zstd-$ZSTD_VERSION
    fi
    # Download zstd
    if [[ ! -f zstd-$ZSTD_VERSION.tar.gz ]]; then
        curl -L https://github.com/facebook/zstd/releases/download/v$ZSTD_VERSION/zstd-$ZSTD_VERSION.tar.gz -o zstd-$ZSTD_VERSION.tar.gz
    fi
    tar -xzf zstd-$ZSTD_VERSION.tar.gz
    pushd zstd-$ZSTD_VERSION
    mkdir -p _build && pushd _build
    cmake ../build/cmake $COMMON_CMAKE_FLAGS -DZSTD_BUILD_SHARED=OFF
    make -j$CPUS install
    popd && popd
fi

log_tool_name "snappy $SNAPPY_VERSION"
if [[ ! -f "$PREFIX/include/snappy.h" ]]; then
    if [[ -d snappy-$SNAPPY_VERSION ]]; then
        rm -rf snappy-$SNAPPY_VERSION
    fi
    # Download snappy
    if [[ ! -f snappy-$SNAPPY_VERSION.tar.gz ]]; then
        curl -L https://codeload.github.com/google/snappy/tar.gz/refs/tags/$SNAPPY_VERSION -o snappy-$SNAPPY_VERSION.tar.gz
    fi
    tar -xzf snappy-$SNAPPY_VERSION.tar.gz
    pushd snappy-$SNAPPY_VERSION
    # Apply patch if it exists
    if [[ -f "$DIR/snappy.patch" ]]; then
        patch -p1 < "$DIR/snappy.patch"
    fi
    mkdir -p build && pushd build
    cmake .. $COMMON_CMAKE_FLAGS \
        -DSNAPPY_BUILD_TESTS=OFF \
        -DSNAPPY_BUILD_BENCHMARKS=OFF \
        -DSNAPPY_FUZZING_BUILD=OFF
    make -j$CPUS install
    popd && popd
fi

log_tool_name "BOOST $BOOST_VERSION"
if [[ ! -d "$PREFIX/include/boost" ]]; then
    if [[ -d boost_$BOOST_VERSION_UNDERSCORES ]]; then
        rm -rf boost_$BOOST_VERSION_UNDERSCORES
    fi
    # Download Boost
    if [[ ! -f boost_$BOOST_VERSION_UNDERSCORES.tar.gz ]]; then
        curl -L https://archives.boost.io/release/$BOOST_VERSION/source/boost_$BOOST_VERSION_UNDERSCORES.tar.gz -o boost_$BOOST_VERSION_UNDERSCORES.tar.gz
    fi
    tar -xzf boost_$BOOST_VERSION_UNDERSCORES.tar.gz
    pushd boost_$BOOST_VERSION_UNDERSCORES
    ./bootstrap.sh --prefix=$PREFIX --with-toolset=clang --with-python=python3 --without-icu
    # Build Boost with libc++ (macOS default)
    ./b2 toolset=clang -j$CPUS install variant=release link=static cxxstd=20 --disable-icu \
        cxxflags="-stdlib=libc++" linkflags="-stdlib=libc++" \
        -sZLIB_SOURCE="$PREFIX" -sZLIB_INCLUDE="$PREFIX/include" -sZLIB_LIBPATH="$PREFIX/lib"
    popd
fi

log_tool_name "pulsar $PULSAR_TAG"
if [[ ! -f "$PREFIX/lib/libpulsarwithdeps.a" ]]; then
    if [[ -d pulsar ]]; then
        rm -rf pulsar
    fi
    git clone https://github.com/apache/pulsar-client-cpp.git pulsar
    pushd pulsar
    git checkout $PULSAR_TAG
    # Apply patch if it exists (same as Linux)
    if [[ -f "$DIR/pulsar-v3.7.1.patch" ]]; then
        git apply "$DIR/pulsar-v3.7.1.patch"
    fi
    # macOS-specific fix: Move cstdint include outside extern "C" in C API headers
    # The patch adds cstdint to defines.h, but defines.h is included inside extern "C"
    # blocks in C headers, causing "templates must have C++ linkage" error on macOS
    # Linux libc++ may be more lenient, but macOS requires cstdint before extern "C"
    # We need to include cstdint before extern "C" block starts
    for header in include/pulsar/c/*.h; do
        if [[ -f "$header" ]] && grep -q 'extern "C"' "$header" 2>/dev/null; then
            # Add cstdint include right after #ifdef __cplusplus and before extern "C"
            # This ensures cstdint is included before the extern "C" block
            if ! grep -q '^#include <cstdint>$' "$header" 2>/dev/null; then
                if grep -q '^#ifdef __cplusplus$' "$header" 2>/dev/null; then
                    # Insert after #ifdef __cplusplus, before extern "C"
                    sed -i '' '/^#ifdef __cplusplus$/a\
#include <cstdint>
' "$header"
                fi
            fi
        fi
    done
    mkdir -p build
    # Build Pulsar (matching Linux exactly, with macOS-specific warning suppression)
    # macOS clang treats deprecated ATOMIC_VAR_INIT as error, suppress it similar to Linux's nontrivial-memcall suppression
    PULSAR_CMAKE_FLAGS="$COMMON_CMAKE_FLAGS \
      -DBUILD_DYNAMIC_LIB=OFF \
      -DBUILD_STATIC_LIB=ON \
      -DBUILD_TESTS=OFF \
      -DLINK_STATIC=ON \
      -DPROTOC_PATH=$PREFIX/bin/protoc \
      -DBUILD_PYTHON_WRAPPER=OFF \
      -DBUILD_PERF_TOOLS=OFF \
      -DUSE_LOG4CXX=OFF"
    
    # Add warning suppression flag separately to avoid quoting issues (matching Linux pattern)
    PULSAR_CMAKE_FLAGS="$PULSAR_CMAKE_FLAGS -DCMAKE_CXX_FLAGS=-Wno-error=deprecated-pragma"
    
    cmake -B build $PULSAR_CMAKE_FLAGS
    pushd build
    cmake --build . -j$CPUS --target pulsarStaticWithDeps
    cp lib/libpulsarwithdeps.a $PREFIX/lib/ || cp lib/libpulsarwithdeps.a $PREFIX/lib64/ 2>/dev/null || true
    cmake --build . -j$CPUS --target install
    popd && popd
fi

#### librdkafka ####
KAFKA_TAG="v2.8.0"
log_tool_name "kafka $KAFKA_TAG"
if [[ ! -f "$PREFIX/lib/librdkafka++.a" ]]; then
    if [[ -d kafka ]]; then
        rm -rf kafka
    fi
    git clone https://github.com/confluentinc/librdkafka.git kafka
    pushd kafka
    git checkout $KAFKA_TAG
    cmake -B build $COMMON_CMAKE_FLAGS \
      -DRDKAFKA_BUILD_STATIC=ON \
      -DRDKAFKA_BUILD_EXAMPLES=OFF \
      -DRDKAFKA_BUILD_TESTS=OFF \
      -DWITH_ZSTD=OFF \
      -DENABLE_LZ4_EXT=OFF \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DWITH_SSL=ON \
      -DWITH_SASL=ON
    cmake --build build -j$CPUS --target install
    popd
fi

log_tool_name "libbcrypt $LIBBCRYPT_TAG"
if [[ ! -f "$PREFIX/lib/bcrypt.a" ]]; then
    if [[ -d libbcrypt ]]; then
        rm -rf libbcrypt
    fi
    git clone https://github.com/rg3/libbcrypt libbcrypt
    pushd libbcrypt
    git checkout $LIBBCRYPT_TAG
    # macOS sed requires backup extension (empty string for no backup)
    sed -i '' 's/-Wcast-align//' crypt_blowfish/Makefile
    make CC=clang
    # NOTE: The libbcrypt doesn't come with the install target.
    cp bcrypt.a $PREFIX/lib/
    mkdir -p $PREFIX/include/libbcrypt/
    cp bcrypt.h $PREFIX/include/libbcrypt/
    popd
fi

log_tool_name "librdtsc $LIBRDTSC_TAG"
# Skip librdtsc on macOS for now - it uses Linux-specific attributes (__always_inline)
# and is not critical for initial macOS port. Can be added later if needed.
echo "Skipping librdtsc build on macOS (Linux-specific, not critical for initial port)"
# TODO: Fix librdtsc for macOS or make it optional in Memgraph build

log_tool_name "mgconsole $MGCONSOLE_TAG"
if [[ ! -f "$PREFIX/bin/mgconsole" ]]; then
    if [[ -d mgconsole ]]; then
        rm -rf mgconsole
    fi
    git clone https://github.com/memgraph/mgconsole.git mgconsole
    pushd mgconsole
    git checkout $MGCONSOLE_TAG
    # Apply patch if it exists
    if [[ -f "$DIR/mgconsole.patch" ]]; then
        patch -p0 < "$DIR/mgconsole.patch"
    fi
    cmake -B build $COMMON_CMAKE_FLAGS
    cmake --build build -j$CPUS --target mgconsole install
    popd
fi

popd

# copy toolchain.cmake to the prefix
cp -v $DIR/toolchain.cmake $PREFIX/

# Create macOS-specific toolchain.cmake
cat > $PREFIX/toolchain-macos.cmake <<EOF
set(CMAKE_SYSTEM_NAME Darwin)

execute_process(
     COMMAND uname -m
     OUTPUT_VARIABLE uname_result
     OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(CMAKE_SYSTEM_PROCESSOR "\${uname_result}")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(MG_TOOLCHAIN_ROOT "\${CMAKE_CURRENT_LIST_DIR}")
message(STATUS "Toolchain directory: \${MG_TOOLCHAIN_ROOT}")

# Paths for find_package(), find_library(), etc.
set(CMAKE_FIND_ROOT_PATH "\${CMAKE_FIND_ROOT_PATH};\${MG_TOOLCHAIN_ROOT}")

# Configure how the find_* commands search
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE BOTH)

set(MG_TOOLCHAIN_VERSION $TOOLCHAIN_VERSION)

# Use system compiler (clang from Xcode)
# Don't override if already set
if(NOT CMAKE_C_COMPILER)
    set(CMAKE_C_COMPILER clang CACHE STRING "" FORCE)
endif()
if(NOT CMAKE_CXX_COMPILER)
    set(CMAKE_CXX_COMPILER clang++ CACHE STRING "" FORCE)
endif()

# Add toolchain to prefix path
list(APPEND CMAKE_PREFIX_PATH "\${MG_TOOLCHAIN_ROOT}")
EOF

# Create activation script
cat > $PREFIX/activate <<EOF
# Detect the shell
if [ -n "\$BASH_VERSION" ]; then
    current_shell="bash"
    SCRIPT_SOURCE="\${BASH_SOURCE[0]}"
elif [ -n "\$ZSH_VERSION" ]; then
    current_shell="zsh"
    SCRIPT_SOURCE="\${(%):-%N}"
else
    echo "Unsupported shell. Only bash and zsh are supported." >&2
    return 1 2>/dev/null || exit 1
fi

readonly SCRIPT_DIR=\$( cd -- "\$( dirname -- "\${SCRIPT_SOURCE}" )" &> /dev/null && pwd )
PREFIX=\$SCRIPT_DIR

# This file must be used with "source /path/to/toolchain/activate" *from bash*
# You can't run it directly!

env_error="You already have an active virtual environment!"

# zsh does not recognize the option -t of the command type
# therefore we use the alternative whence -w
if [[ "\$ZSH_NAME" == "zsh" ]]; then
    # check for active virtual environments
    if [ "\$( whence -w deactivate )" != "deactivate: none" ]; then
        echo \$env_error
        return 0;
    fi
# any other shell
else
    # check for active virtual environments
    if [ "\$( type -t deactivate )" != "" ]; then
        echo \$env_error
        return 0
    fi
fi

# check that we aren't root
if [[ "\$USER" == "root" ]]; then
    echo "You shouldn't use the toolchain as root!"
    return 0
fi

# save original environment
export ORIG_PATH=\$PATH
export ORIG_PS1=\$PS1
export ORIG_DYLD_LIBRARY_PATH=\$DYLD_LIBRARY_PATH
export ORIG_CXXFLAGS=\$CXXFLAGS
export ORIG_CFLAGS=\$CFLAGS

# activate new environment
export PATH=\$PREFIX/bin:\$PATH
export PS1="($NAME) \$PS1"
export DYLD_LIBRARY_PATH=\$PREFIX/lib
export CXXFLAGS=-isystem\ \$PREFIX/include\ \$CXXFLAGS
export CFLAGS=-isystem\ \$PREFIX/include\ \$CFLAGS
export MG_TOOLCHAIN_ROOT=\$PREFIX
export MG_TOOLCHAIN_VERSION=$TOOLCHAIN_VERSION

# create deactivation function
function deactivate() {
    export PATH=\$ORIG_PATH
    export PS1=\$ORIG_PS1
    export DYLD_LIBRARY_PATH=\$ORIG_DYLD_LIBRARY_PATH
    export CXXFLAGS=\$ORIG_CXXFLAGS
    export CFLAGS=\$ORIG_CFLAGS
    unset ORIG_PATH ORIG_PS1 ORIG_DYLD_LIBRARY_PATH ORIG_CXXFLAGS ORIG_CFLAGS
    unset MG_TOOLCHAIN_ROOT MG_TOOLCHAIN_VERSION
    unset -f deactivate
}
EOF

chmod +x $PREFIX/activate

echo -e "\n\n"
echo "All tools have been built. They are installed in '$PREFIX'."
echo "In order to use all of the newly compiled tools you should use the prepared activation script:"
echo
echo "    source $PREFIX/activate"
echo
echo "Or, for more advanced uses, you can add the following lines to your script:"
echo
echo "    export PATH=$PREFIX/bin:\$PATH"
echo "    export DYLD_LIBRARY_PATH=$PREFIX/lib"
echo
echo "Enjoy!"
