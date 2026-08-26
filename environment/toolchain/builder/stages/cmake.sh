#!/bin/bash
# cmake: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/cmake.env"

pushd "$TC_ARCHIVES"
if [[ ! -f cmake-$CMAKE_VERSION.tar.gz ]]; then
    wget --https-only https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION.tar.gz
    CMAKE_SHA256="95d4721f3625fb0d9d6ca480dd59a46c84b4c157f7fadd2e9b179ef9c871174d"
    echo "$CMAKE_SHA256  cmake-$CMAKE_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make — compiler is the toolchain gcc; curl/ncurses/openssl/
# zlib come from the sysroot (--system-curl + CMAKE_SYSROOT below). Built
# before GDB because the sysroot libipt below needs a cmake.
log_tool_name "cmake $CMAKE_VERSION"
if [[ ! -f "$PREFIX/bin/cmake" ]]; then
    if [[ -d cmake-$CMAKE_VERSION ]]; then
        rm -rf cmake-$CMAKE_VERSION
    fi
    tar -xvf ../archives/cmake-$CMAKE_VERSION.tar.gz
    pushd "cmake-$CMAKE_VERSION"
    # influenced by: https://buildd.debian.org/status/fetch.php?pkg=cmake&arch=amd64&ver=3.13.4-1&stamp=1549799837
    echo 'set(CMAKE_SKIP_RPATH ON CACHE BOOL "Skip rpath" FORCE)' >> build-flags.cmake
    echo 'set(CMAKE_USE_RELATIVE_PATHS ON CACHE BOOL "Use relative paths" FORCE)' >> build-flags.cmake
    echo 'set(CMAKE_C_FLAGS "-g -O2 -fstack-protector-strong -Wformat -Werror=format-security -Wdate-time -D_FORTIFY_SOURCE=2" CACHE STRING "C flags" FORCE)' >> build-flags.cmake
    echo 'set(CMAKE_CXX_FLAGS "-g -O2 -fstack-protector-strong -Wformat -Werror=format-security -Wdate-time -D_FORTIFY_SOURCE=2" CACHE STRING "C++ flags" FORCE)' >> build-flags.cmake
    echo 'set(CMAKE_SKIP_BOOTSTRAP_TEST ON CACHE BOOL "Skip BootstrapTest" FORCE)' >> build-flags.cmake
    # Point cmake's find_* at the sysroot so libcurl/ncurses/openssl from
    # $SYSROOT/usr are found (and not the host's host-glibc-linked copies).
    echo "set(CMAKE_SYSROOT \"$SYSROOT\" CACHE PATH \"Sysroot\" FORCE)" >> build-flags.cmake
    # Force find_library / find_path / find_package to look ONLY inside the
    # sysroot — otherwise cmake's default BOTH mode happily picks up host
    # /usr/lib and /usr/include, which drags host-glibc-linked libs into the
    # build. PROGRAM stays default so build tools like git/make/sh are found.
    echo 'set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY CACHE STRING "" FORCE)' >> build-flags.cmake
    echo 'set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY CACHE STRING "" FORCE)' >> build-flags.cmake
    echo 'set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY CACHE STRING "" FORCE)' >> build-flags.cmake
    echo 'set(BUILD_CursesDialog ON CACHE BOOL "Build curses GUI" FORCE)' >> build-flags.cmake
    # cmake, ctest, cpack and ccmake are C++ and shipped without a runpath, so
    # they bind to whatever libstdc++ the machine they land on provides. That
    # makes the oldest usable machine a property of the host rather than of the
    # newer libstdc++ this toolchain ships alongside them. Relative, so the
    # installed tree still moves.
    echo 'set(CMAKE_EXE_LINKER_FLAGS "-Wl,-rpath,$ORIGIN/../lib64" CACHE STRING "" FORCE)' >> build-flags.cmake
    mkdir build && pushd build
    ../bootstrap \
        --prefix=$PREFIX \
        --init=../build-flags.cmake \
        --parallel=$CPUS \
        --system-curl
    make -j$CPUS
    # make test # run test suite
    make install
    popd && popd
fi

popd
