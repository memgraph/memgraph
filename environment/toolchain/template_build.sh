#!/bin/bash -e

# NOTE: Copy this under memgraph/environment/toolchain/vN/tmp_build.sh, edit and test.

pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CPUS=$( grep -c processor < /proc/cpuinfo )
cd "$DIR"
source "$DIR/../../util.sh"
DISTRO="$(operating_system)"
TOOLCHAIN_VERSION=5
NAME=toolchain-v$TOOLCHAIN_VERSION
PREFIX=/opt/$NAME
function log_tool_name () {
    echo ""
    echo ""
    echo "#### $1 ####"
    echo ""
    echo ""
}

# HERE: Remove/clear dependencies from a given toolchain.

mkdir -p archives && pushd archives
# HERE: Download dependencies here.
popd

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

# HERE: Add dependencies to test below.
