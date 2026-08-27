#!/bin/bash
# llvm: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/llvm.env"
# the swig stage installed under $DIR/build; this needs its version to find it
source "$TC_VERSIONS/swig.env"

pushd "$TC_ARCHIVES"
if [[ ! -d llvmorg-$LLVM_VERSION ]]; then
    git clone --depth 1 --branch llvmorg-$LLVM_VERSION https://github.com/llvm/llvm-project.git llvmorg-$LLVM_VERSION
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make, python3 — cmake/gcc/binutils come from $PREFIX, swig
# from the stage above, zlib/libffi from the sysroot (FIND_ROOT_PATH=ONLY).
log_tool_name "LLVM $LLVM_VERSION"
cp -r ../archives/llvmorg-$LLVM_VERSION ./llvmorg-$LLVM_VERSION

# NOTE: Go under llvmorg-$LLVM_VERSION/llvm/CMakeLists.txt to see all
#       options, docs pages are not up to date.
TOOLCHAIN_LLVM_ENABLE_PROJECTS="$LLVM_PROJECTS"
TOOLCHAIN_LLVM_ENABLE_RUNTIMES="$(mg_llvm_runtimes)"

# Match GCC's target triple. Without this, LLVM defaults to
# x86_64-unknown-linux-gnu, but our GCC was configured with
# --target=x86_64-linux-gnu so its install dir is
# $PREFIX/lib/gcc/x86_64-linux-gnu/ — clang can't find crtbeginS.o /
# libstdc++ when the triples don't match.
if [[ "$for_arm" = true ]]; then
    TOOLCHAIN_LLVM_TARGET_TRIPLE=aarch64-linux-gnu
else
    TOOLCHAIN_LLVM_TARGET_TRIPLE=x86_64-linux-gnu
fi

pushd "llvmorg-$LLVM_VERSION"
# activate swig
export PATH=$DIR/build/swig-$SWIG_VERSION/install/bin:$PATH
# influenced by: https://buildd.debian.org/status/fetch.php?pkg=llvm-toolchain-7&arch=amd64&ver=1%3A7.0.1%7E%2Brc2-1%7Eexp1&stamp=1541506173&raw=0
# The linker flags name both link rules. CMAKE_CXX_LINK_FLAGS, which stood
# here before, expands only in the rule that links executables, so clang and
# the other programs got a runpath while libLLVM, libclang and lldb did not
# and fell back to whatever libstdc++ the machine happened to have. The path
# is relative, and names lib64: LLVM's own default runpath points at lib,
# where this toolchain's libstdc++ does not live. Three rules, not two:
# LLVMgold is a module rather than a shared library, and took neither of the
# other two.
#
# The runtimes need saying separately. compiler-rt and openmp are configured
# as a nested CMake build, which inherits none of the flags above, and they
# install further down the tree -- lib/<triple> and lib/clang/N/lib/<triple>
# -- so one relative path cannot reach lib64 from both. Two are given; a
# runpath entry that resolves to nothing is simply skipped.
#
# FORCE_ON rather than ON for zstd: ON quietly builds without it when the
# library is not found, and the only sign is clang refusing -gz=zstd much
# later, at which point the build has already fallen back to zlib.
#
# No -fuse-ld=gold. binutils dropped gold, so this toolchain ships none and
# the flag was silently selecting the *host's* gold -- an old one, outside
# the sysroot. It also fails outright on BOLT: gold 1.16 hits an internal
# error in do_layout linking merge-fdata. Without the flag the link uses the
# toolchain's own ld, which is what the rest of the build already uses.
cmake -S llvm -B build -G "Unix Makefiles" \
    -DCMAKE_INSTALL_PREFIX="$PREFIX" \
    -DCMAKE_SYSROOT="$SYSROOT" \
    -DLLVM_DEFAULT_TARGET_TRIPLE="$TOOLCHAIN_LLVM_TARGET_TRIPLE" \
    -DCMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY \
    -DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY \
    -DCMAKE_FIND_ROOT_PATH_MODE_PACKAGE=ONLY \
    -DCMAKE_C_COMPILER=$PREFIX/bin/gcc \
    -DCMAKE_CXX_COMPILER=$PREFIX/bin/g++ \
    -DCMAKE_EXE_LINKER_FLAGS="-L$PREFIX/lib64 -Wl,-rpath,\$ORIGIN/../lib64" \
    -DCMAKE_SHARED_LINKER_FLAGS="-L$PREFIX/lib64 -Wl,-rpath,\$ORIGIN/../lib64" \
    -DCMAKE_MODULE_LINKER_FLAGS="-L$PREFIX/lib64 -Wl,-rpath,\$ORIGIN/../lib64" \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O2 -DNDEBUG" \
    -DCMAKE_CXX_FLAGS=' -fPIC -Wno-unused-command-line-argument -Wno-unknown-warning-option' \
    -DCMAKE_C_FLAGS=' -fPIC -Wno-unused-command-line-argument -Wno-unknown-warning-option' \
    -DLLVM_ENABLE_PROJECTS="$TOOLCHAIN_LLVM_ENABLE_PROJECTS" \
    -DLLVM_ENABLE_RUNTIMES="$TOOLCHAIN_LLVM_ENABLE_RUNTIMES" \
    -DRUNTIMES_CMAKE_ARGS="-DCMAKE_C_FLAGS=--gcc-toolchain=$PREFIX;-DCMAKE_CXX_FLAGS=--gcc-toolchain=$PREFIX;-DLIBOMP_OMPD_SUPPORT=OFF;-DLIBOMP_OMPD_GDB_SUPPORT=OFF;-DCMAKE_SHARED_LINKER_FLAGS=-Wl,-rpath,\$ORIGIN/../../lib64 -Wl,-rpath,\$ORIGIN/../../../../../lib64" \
    -DBUILTINS_CMAKE_ARGS="-DCMAKE_C_FLAGS=--gcc-toolchain=$PREFIX;-DCMAKE_CXX_FLAGS=--gcc-toolchain=$PREFIX" \
    -DLLVM_LINK_LLVM_DYLIB=ON \
    -DLLVM_INSTALL_UTILS=ON \
    -DLLVM_VERSION_SUFFIX= \
    -DLLVM_BUILD_LLVM_DYLIB=ON \
    -DLLVM_ENABLE_RTTI=ON \
    -DLLVM_ENABLE_FFI=ON \
    -DLLVM_ENABLE_ZSTD=FORCE_ON \
    -DzstdSTATIC_LIBRARY=$SYSROOT/usr/lib/libzstd.a \
    -DLLVM_BINUTILS_INCDIR=$PREFIX/include/ \
    -DLLVM_INCLUDE_BENCHMARKS=OFF \
    -DLLVM_USE_PERF=yes \
    -DCOMPILER_RT_INCLUDE_TESTS=OFF \
    -DLIBCXX_INCLUDE_BENCHMARKS=OFF
pushd build
make -j$CPUS
if [[ "$for_arm" = "false" ]]; then
    # TODO(gitbuda): 5 tests fail 4/5 are cuda... -> fix (or just ignore
    # the cuda tests because cuda stuff is actually not used) and
    # uncomment.
    echo "Skipping LLVM tests..."
    # make -j$CPUS check-clang # run clang test suite
    # ldd is not used
    # make -j$CPUS check-lld # run lld test suite
fi
make install
popd && popd

popd
