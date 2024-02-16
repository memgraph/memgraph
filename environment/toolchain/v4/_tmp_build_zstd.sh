#!/bin/bash -e

# helpers
pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CPUS=$( grep -c processor < /proc/cpuinfo )
cd "$DIR"
source "$DIR/../../util.sh"
DISTRO="$(operating_system)"

function log_tool_name () {
    echo ""
    echo ""
    echo "#### $1 ####"
    echo ""
    echo ""
}

TOOLCHAIN_VERSION=4
NAME=toolchain-v$TOOLCHAIN_VERSION
PREFIX=/opt/$NAME

rm -rf "$PREFIX/include/zstd.h"

# create archives directory
mkdir -p archives && pushd archives
ZSTD_VERSION=1.5.5
if [ ! -f zstd-$ZSTD_VERSION.tar.gz ]; then
    wget https://github.com/facebook/zstd/releases/download/v$ZSTD_VERSION/zstd-$ZSTD_VERSION.tar.gz -O zstd-$ZSTD_VERSION.tar.gz
fi
popd

# create build directory and activate toolchain
mkdir -p build
pushd build
source $PREFIX/activate
export CC=$PREFIX/bin/clang
export CXX=$PREFIX/bin/clang++
export CFLAGS="$CFLAGS -fPIC"
export PATH=$PREFIX/bin:$PATH
export LD_LIBRARY_PATH=$PREFIX/lib64
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

log_tool_name "zstd $ZSTD_VERSION"
if [ ! -f $PREFIX/include/zstd.h ]; then
    if [ -d zstd-$ZSTD_VERSION ]; then
        rm -rf zstd-$ZSTD_VERSION
    fi
    tar -xzf ../archives/zstd-$ZSTD_VERSION.tar.gz
    pushd zstd-$ZSTD_VERSION
    # build is used by facebook builder
    mkdir _build
    pushd _build
    cmake ../build/cmake $COMMON_CMAKE_FLAGS -DZSTD_BUILD_SHARED=OFF
    make -j$CPUS install
    popd && popd
fi
