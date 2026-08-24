# Clang build environment shared by the stages that compile against the
# finished toolchain rather than bootstrapping it. Moved verbatim from the v8
# build script; sourced, never executed.

source $PREFIX/activate

export CC=$PREFIX/bin/clang
export CXX=$PREFIX/bin/clang++
export CFLAGS="${CFLAGS:-} -fPIC"
if [[ "$TOOLCHAIN_STDCXX" = "libstdc++" ]]; then
    export CXXFLAGS="${CXXFLAGS:-} -fPIC"
else
    export CXXFLAGS="${CXXFLAGS:-} -fPIC -stdlib=libc++"
fi

# possible fix for debian 13 arm
if [[ "$for_arm" = true ]]; then
    export EXTRA_CLANG_TOOLCHAIN_FLAGS="--gcc-toolchain=$PREFIX --target=aarch64-linux-gnu --sysroot=$SYSROOT"
else
    export EXTRA_CLANG_TOOLCHAIN_FLAGS="--gcc-toolchain=$PREFIX --target=x86_64-linux-gnu --sysroot=$SYSROOT"
fi

export CXXFLAGS="${CXXFLAGS:-} $EXTRA_CLANG_TOOLCHAIN_FLAGS"
export LDFLAGS="${LDFLAGS:-} $EXTRA_CLANG_TOOLCHAIN_FLAGS"

COMMON_CMAKE_FLAGS="-DCMAKE_INSTALL_PREFIX=$PREFIX
                    -DCMAKE_PREFIX_PATH=$PREFIX
                    -DCMAKE_SYSROOT=$SYSROOT
                    -DCMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY
                    -DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY
                    -DCMAKE_FIND_ROOT_PATH_MODE_PACKAGE=ONLY
                    -DCMAKE_BUILD_TYPE=Release
                    -DCMAKE_C_COMPILER=$CC
                    -DCMAKE_CXX_COMPILER=$CXX
                    -DBUILD_SHARED_LIBS=OFF
                    -DCMAKE_CXX_STANDARD=20
                    -DBUILD_TESTING=OFF
                    -DCMAKE_REQUIRED_INCLUDES=$PREFIX/include
                    -DCMAKE_POSITION_INDEPENDENT_CODE=ON"
COMMON_CONFIGURE_FLAGS="--enable-shared=no --prefix=$PREFIX"
COMMON_MAKE_INSTALL_FLAGS="-j$CPUS BUILD_SHARED=no PREFIX=$PREFIX install"

MGCONSOLE_TAG="v1.7.0"
# Host deps (apt): git, make — OpenSSL comes from the sysroot.
