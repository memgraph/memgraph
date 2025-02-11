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

for_arm=false
if [[ "$#" -eq 1 ]]; then
    if [[ "$1" == "--for-arm" ]]; then
        for_arm=true
    else
        echo "Invalid argument received. Use '--for-arm' if you want to build the toolchain for ARM based CPU."
        exit 1
   fi
fi
TOOLCHAIN_STDCXX="${TOOLCHAIN_STDCXX:-libstdc++}"
if [[ "$TOOLCHAIN_STDCXX" != "libstdc++" && "$TOOLCHAIN_STDCXX" != "libc++" ]]; then
    echo "Only GCC (libstdc++) or LLVM (libc++) C++ standard library implementations are supported."
    exit 1
fi
# TODO(gitbuda): Make LLVM linker configurable -DLLVM_ENABLE_LLD=ON + -fuse-ld=lld (gold vs lld).
# TODO(gitbuda): Add --skip-gpg and somehow make gpg check configurable per OS.
TOOLCHAIN_VERSION=6
# package versions used
GCC_VERSION=14.2.0
BINUTILS_VERSION=2.43
case "$DISTRO" in
    centos-7) # because GDB >= 9 does NOT compile with readline6.
        GDB_VERSION=8.3
    ;;
    *)
        GDB_VERSION=15.2
    ;;
esac
CMAKE_VERSION=3.31.3
CPPCHECK_VERSION=2.16.0
LLVM_VERSION=18.1.8
SWIG_VERSION=4.3.0 # used only for LLVM compilation

# define the name used to make the toolchain archive
DISTRO_FULL_NAME=${DISTRO}
if [[ "${DISTRO}" == centos* ]]; then
    if [[ "$for_arm" = "true" ]]; then
        DISTRO_FULL_NAME="$DISTRO_FULL_NAME-aarch64"
    else
        DISTRO_FULL_NAME="$DISTRO_FULL_NAME-x86_64"
    fi
else
    if [[ "$for_arm" = "true" ]]; then
        DISTRO_FULL_NAME="$DISTRO_FULL_NAME-arm64"
    else
        DISTRO_FULL_NAME="$DISTRO_FULL_NAME-amd64"
    fi
fi
if [ "$TOOLCHAIN_STDCXX" = "libstdc++" ]; then
    # Pass because infra scripts assume there is not C++ standard lib in the name.
    echo "NOTE: Not adding anything to the archive name, GCC C++ standard lib is used to build libraries."
else
    echo "NOTE: Adding libc++ to the archive name, all libraries are built with LLVM standard C++ library."
    DISTRO_FULL_NAME="$DISTRO_FULL_NAME-libc++"
fi

# Set the right operating system setup script.
ENV_SCRIPT_RELATIVE="environment/os/$DISTRO.sh"
if [[ "$for_arm" = true ]]; then
    ENV_SCRIPT_RELATIVE="environment/os/$DISTRO-arm.sh"
fi
ENV_SCRIPT="$DIR/../../../$ENV_SCRIPT_RELATIVE"
echo "ALL BUILD PACKAGES: $(${ENV_SCRIPT} list TOOLCHAIN_BUILD_DEPS)"
${ENV_SCRIPT} check TOOLCHAIN_BUILD_DEPS
echo "ALL RUN PACKAGES: $(${ENV_SCRIPT} list TOOLCHAIN_RUN_DEPS)"
${ENV_SCRIPT} check TOOLCHAIN_RUN_DEPS
. "$HOME/.cargo/env"

# check installation directory
NAME=toolchain-v$TOOLCHAIN_VERSION
PREFIX=/opt/$NAME
mkdir -p $PREFIX >/dev/null 2>/dev/null || true
if [ ! -d $PREFIX ] || [ ! -w $PREFIX ]; then
    echo "Please make sure that the directory '$PREFIX' exists and is writable by the current user!"
    echo
    echo "If unsure, execute these commands as root:"
    echo "    mkdir $PREFIX && chown $USER:$USER $PREFIX"
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

# create archives directory
mkdir -p archives && pushd archives
# download all archives
if [ ! -f gcc-$GCC_VERSION.tar.gz ]; then
    wget https://ftp.gnu.org/gnu/gcc/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz
fi
if [ ! -f binutils-$BINUTILS_VERSION.tar.gz ]; then
    wget https://ftp.gnu.org/gnu/binutils/binutils-$BINUTILS_VERSION.tar.gz
fi
if [ ! -f gdb-$GDB_VERSION.tar.gz ]; then
    wget https://ftp.gnu.org/gnu/gdb/gdb-$GDB_VERSION.tar.gz
fi
if [ ! -f cmake-$CMAKE_VERSION.tar.gz ]; then
    wget https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION.tar.gz
fi
if [ ! -f cppcheck-$CPPCHECK_VERSION.tar.gz ]; then
    wget https://github.com/danmar/cppcheck/archive/refs/tags/$CPPCHECK_VERSION.tar.gz -O cppcheck-$CPPCHECK_VERSION.tar.gz
fi
if [ ! -d llvmorg-$LLVM_VERSION ]; then
    git clone --depth 1 --branch llvmorg-$LLVM_VERSION https://github.com/llvm/llvm-project.git llvmorg-$LLVM_VERSION
fi
if [ ! -f pahole-gdb-master.zip ]; then
    wget https://github.com/PhilArmstrong/pahole-gdb/archive/master.zip -O pahole-gdb-master.zip
fi
if [ ! -f swig-$SWIG_VERSION.tar.gz ]; then
    wget https://github.com/swig/swig/archive/refs/tags/v$SWIG_VERSION.tar.gz -O swig-$SWIG_VERSION.tar.gz
fi

# verify all archives
# NOTE: Verification can fail if the archive is signed by another developer. I
# haven't added commands to download all developer GnuPG keys because the
# download is very slow. If the verification fails for you, figure out who has
# signed the archive and download their public key instead.
GPG="gpg --homedir .gnupg"
KEYSERVER="hkp://keyserver.ubuntu.com"
mkdir -p .gnupg
chmod 700 .gnupg

popd
# create build directory
mkdir -p build
pushd build

log_tool_name "GCC $GCC_VERSION"
if [ ! -f $PREFIX/bin/gcc ]; then
    if [ -d gcc-$GCC_VERSION ]; then
        rm -rf gcc-$GCC_VERSION
    fi
    tar -xvf ../archives/gcc-$GCC_VERSION.tar.gz
    pushd gcc-$GCC_VERSION
    ./contrib/download_prerequisites
    mkdir build && pushd build
    # influenced by: https://buildd.debian.org/status/fetch.php?pkg=gcc-11&arch=arm64&ver=11.2.0-14&stamp=1642052446&raw=0
    if [[ "$for_arm" = true ]]; then
        ../configure -v \
            --prefix=$PREFIX \
            --disable-multilib \
            --with-system-zlib \
            --enable-languages=c,c++,fortran \
            --enable-gold=yes \
            --enable-ld=yes \
            --disable-vtable-verify \
            --enable-libmpx \
            --without-cuda-driver \
            --enable-shared \
            --enable-linker-build-id \
            --without-included-gettext \
            --enable-threads=posix \
            --enable-nls \
            --enable-bootstrap \
            --enable-clocale=gnu \
            --enable-libstdcxx-debug \
            --enable-libstdcxx-time=yes \
            --with-default-libstdcxx-abi=new \
            --enable-gnu-unique-object \
            --disable-libquadmath \
            --disable-libquadmath-support \
            --enable-plugin \
            --enable-default-pie \
            --with-system-zlib \
            --enable-libphobos-checking=release \
            --with-target-system-zlib=auto \
            --enable-objc-gc=auto \
            --enable-multiarch \
            --enable-fix-cortex-a53-843419 \
            --disable-werror \
            --enable-checking=release \
            --build=aarch64-linux-gnu \
            --host=aarch64-linux-gnu \
            --target=aarch64-linux-gnu \
            --with-build-config=bootstrap-lto-lean \
            --enable-link-serialization=4
    else
        # influenced by: https://buildd.debian.org/status/fetch.php?pkg=gcc-8&arch=amd64&ver=8.3.0-6&stamp=1554588545
        ../configure -v \
            --build=x86_64-linux-gnu \
            --host=x86_64-linux-gnu \
            --target=x86_64-linux-gnu \
            --prefix=$PREFIX \
            --disable-multilib \
            --with-system-zlib \
            --enable-checking=release \
            --enable-languages=c,c++,fortran \
            --enable-gold=yes \
            --enable-ld=yes \
            --enable-lto \
            --enable-bootstrap \
            --disable-vtable-verify \
            --disable-werror \
            --without-included-gettext \
            --enable-threads=posix \
            --enable-nls \
            --enable-clocale=gnu \
            --enable-libstdcxx-debug \
            --enable-libstdcxx-time=yes \
            --enable-gnu-unique-object \
            --enable-libmpx \
            --enable-plugin \
            --enable-default-pie \
            --with-target-system-zlib \
            --with-tune=generic \
            --without-cuda-driver
            #--program-suffix=$( printf "$GCC_VERSION" | cut -d '.' -f 1,2 ) \
    fi
    make -j$CPUS
    # make -k check # run test suite
    make install
    popd && popd
fi

# activate toolchain
export PATH=$PREFIX/bin:$PATH
export LD_LIBRARY_PATH=$PREFIX/lib64

# NOTE: manually install gmp and mpfr (required by gdb)
log_tool_name "gmp (from gcc)"
if [ ! -f $PREFIX/lib/libgmp.a ]; then
    pushd $DIR/build/gcc-$GCC_VERSION/gmp
    if [[ "$for_arm" = true ]]; then
        ./configure \
            --build=aarch64-linux-gnu \
            --host=aarch64-linux-gnu \
            --prefix=$PREFIX
    else
        ./configure \
            --build=x86_64-linux-gnu \
            --host=x86_64-linux-gnu \
            --prefix=$PREFIX
    fi
    make install
    popd
fi

log_tool_name "mpfr (from gcc)"
if [ ! -f $PREFIX/lib/libmpfr.a ]; then
    pushd $DIR/build/gcc-$GCC_VERSION/mpfr
    if [[ "$for_arm" = true ]]; then
        ./configure \
            --build=aarch64-linux-gnu \
            --host=aarch64-linux-gnu \
            --prefix=$PREFIX
    else
        ./configure \
            --build=x86_64-linux-gnu \
            --host=x86_64-linux-gnu \
            --prefix=$PREFIX
    fi
    make install
    popd
fi

log_tool_name "binutils $BINUTILS_VERSION"
if [ ! -f $PREFIX/bin/ld.gold ]; then
    if [ -d binutils-$BINUTILS_VERSION ]; then
        rm -rf binutils-$BINUTILS_VERSION
    fi
    tar -xvf ../archives/binutils-$BINUTILS_VERSION.tar.gz
    pushd binutils-$BINUTILS_VERSION
    mkdir build && pushd build
    BINUTILS_SPECIAL_FLAGS=""
    if [[ "$for_arm" = true ]]; then
        # influenced by: https://buildd.debian.org/status/fetch.php?pkg=binutils&arch=arm64&ver=2.37.90.20220130-2&stamp=1643576183&raw=0
        # NOTE: On ARM, on Debian 11, there are errors like gprofng/libcollector/dispatcher.c: multiple definition of pthread_sigmask.
        # A simple solution is to disable gprofng because Debian 11 is not the main OS used.
        if [[ "${DISTRO}" == "debian-11" ]]; then
            BINUTILS_SPECIAL_FLAGS="--disable-gprofng"
        fi
        env \
            CC=gcc \
            CXX=g++ \
            CFLAGS="-g -O2" \
            CXXFLAGS="-g -O2" \
            LDFLAGS="" \
            ../configure $BINUTILS_SPECIAL_FLAGS \
                --build=aarch64-linux-gnu \
                --host=aarch64-linux-gnu \
                --prefix=$PREFIX \
                --enable-ld=default \
                --enable-gold \
                --enable-lto \
                --enable-pgo-build=lto \
                --enable-plugins \
                --enable-shared \
                --enable-threads \
                --with-system-zlib \
                --enable-deterministic-archives \
                --disable-compressed-debug-sections \
                --disable-x86-used-note \
                --enable-obsolete \
                --enable-new-dtags \
                --disable-werror
    else
        # influenced by: https://buildd.debian.org/status/fetch.php?pkg=binutils&arch=amd64&ver=2.32-7&stamp=1553247092
        env \
            CC=gcc \
            CXX=g++ \
            CFLAGS="-g -O2" \
            CXXFLAGS="-g -O2" \
            LDFLAGS="" \
            ../configure \
                --build=x86_64-linux-gnu \
                --host=x86_64-linux-gnu \
                --prefix=$PREFIX \
                --enable-ld=default \
                --enable-gold \
                --enable-lto \
                --enable-plugins \
                --enable-shared \
                --enable-threads \
                --with-system-zlib \
                --enable-deterministic-archives \
                --disable-compressed-debug-sections \
                --enable-new-dtags \
                --disable-werror
    fi
    make -j$CPUS
    # make -k check # run test suite
    make install
    popd && popd
fi

log_tool_name "GDB $GDB_VERSION"
if [[ ! -f "$PREFIX/bin/gdb" && "$DISTRO" != "amzn-2" ]]; then
    if [ -d gdb-$GDB_VERSION ]; then
        rm -rf gdb-$GDB_VERSION
    fi
    tar -xvf ../archives/gdb-$GDB_VERSION.tar.gz
    pushd gdb-$GDB_VERSION
    mkdir build && pushd build
    if [[ "$for_arm" = true ]]; then
        # https://buildd.debian.org/status/fetch.php?pkg=gdb&arch=arm64&ver=10.1-2&stamp=1614889767&raw=0
        env \
            CC=gcc \
            CXX=g++ \
            CFLAGS="-g -O2 -fstack-protector-strong -Wformat -Werror=format-security" \
            CXXFLAGS="-g -O2 -fstack-protector-strong -Wformat -Werror=format-security" \
            CPPFLAGS="-Wdate-time -D_FORTIFY_SOURCE=2 -fPIC" \
            LDFLAGS="-Wl,-z,relro" \
            PYTHON="" \
            ../configure \
                --build=aarch64-linux-gnu \
                --host=aarch64-linux-gnu \
                --prefix=$PREFIX \
                --disable-maintainer-mode \
                --disable-dependency-tracking \
                --disable-silent-rules \
                --disable-gdbtk \
                --disable-shared \
                --without-guile \
                --with-system-gdbinit=$PREFIX/etc/gdb/gdbinit \
                --with-system-readline \
                --with-expat \
                --with-system-zlib \
                --with-lzma \
                --without-babeltrace \
                --enable-tui \
                --with-python=python3
    else
        # https://buildd.debian.org/status/fetch.php?pkg=gdb&arch=amd64&ver=8.2.1-2&stamp=1550831554&raw=0
        env \
            CC=gcc \
            CXX=g++ \
            CFLAGS="-g -O2 -fstack-protector-strong -Wformat -Werror=format-security" \
            CXXFLAGS="-g -O2 -fstack-protector-strong -Wformat -Werror=format-security" \
            CPPFLAGS="-Wdate-time -D_FORTIFY_SOURCE=2 -fPIC" \
            LDFLAGS="-Wl,-z,relro" \
            PYTHON="" \
            ../configure \
                --build=x86_64-linux-gnu \
                --host=x86_64-linux-gnu \
                --prefix=$PREFIX \
                --disable-maintainer-mode \
                --disable-dependency-tracking \
                --disable-silent-rules \
                --disable-gdbtk \
                --disable-shared \
                --without-guile \
                --with-system-gdbinit=$PREFIX/etc/gdb/gdbinit \
                --with-system-readline \
                --with-expat \
                --with-system-zlib \
                --with-lzma \
                --with-babeltrace \
                --with-intel-pt \
                --enable-tui \
                --with-python=python3
    fi
    make -j$CPUS
    make install
    popd && popd
fi

log_tool_name "install pahole"
if [ ! -d $PREFIX/share/pahole-gdb ]; then
    unzip ../archives/pahole-gdb-master.zip
    mv pahole-gdb-master $PREFIX/share/pahole-gdb
fi

log_tool_name "setup system gdbinit"
if [ ! -f $PREFIX/etc/gdb/gdbinit ]; then
    mkdir -p $PREFIX/etc/gdb
    cat >$PREFIX/etc/gdb/gdbinit <<EOF
# improve formatting
set print pretty on
set print object on
set print static-members on
set print vtbl on
set print demangle on
set demangle-style gnu-v3
set print sevenbit-strings off

# load libstdc++ pretty printers
add-auto-load-scripts-directory $PREFIX/lib64
add-auto-load-safe-path $PREFIX

# load pahole
python
sys.path.insert(0, "$PREFIX/share/pahole-gdb")
import offsets
import pahole
end
EOF
fi

log_tool_name "cmake $CMAKE_VERSION"
if [ ! -f $PREFIX/bin/cmake ]; then
    if [ -d cmake-$CMAKE_VERSION ]; then
        rm -rf cmake-$CMAKE_VERSION
    fi
    tar -xvf ../archives/cmake-$CMAKE_VERSION.tar.gz
    pushd cmake-$CMAKE_VERSION
    # influenced by: https://buildd.debian.org/status/fetch.php?pkg=cmake&arch=amd64&ver=3.13.4-1&stamp=1549799837
    echo 'set(CMAKE_SKIP_RPATH ON CACHE BOOL "Skip rpath" FORCE)' >> build-flags.cmake
    echo 'set(CMAKE_USE_RELATIVE_PATHS ON CACHE BOOL "Use relative paths" FORCE)' >> build-flags.cmake
    echo 'set(CMAKE_C_FLAGS "-g -O2 -fstack-protector-strong -Wformat -Werror=format-security -Wdate-time -D_FORTIFY_SOURCE=2" CACHE STRING "C flags" FORCE)' >> build-flags.cmake
    echo 'set(CMAKE_CXX_FLAGS "-g -O2 -fstack-protector-strong -Wformat -Werror=format-security -Wdate-time -D_FORTIFY_SOURCE=2" CACHE STRING "C++ flags" FORCE)' >> build-flags.cmake
    echo 'set(CMAKE_SKIP_BOOTSTRAP_TEST ON CACHE BOOL "Skip BootstrapTest" FORCE)' >> build-flags.cmake
    echo 'set(BUILD_CursesDialog ON CACHE BOOL "Build curses GUI" FORCE)' >> build-flags.cmake
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

log_tool_name "cppcheck $CPPCHECK_VERSION"
if [ ! -f $PREFIX/bin/cppcheck ]; then
    if [ -d cppcheck-$CPPCHECK_VERSION ]; then
        rm -rf cppcheck-$CPPCHECK_VERSION
    fi
    tar -xvf ../archives/cppcheck-$CPPCHECK_VERSION.tar.gz
    pushd cppcheck-$CPPCHECK_VERSION
    env \
        CC=gcc \
        CXX=g++ \
        PREFIX=$PREFIX \
        FILESDIR=$PREFIX/share/cppcheck \
        CFGDIR=$PREFIX/share/cppcheck/cfg \
            make -j$CPUS
    env \
        CC=gcc \
        CXX=g++ \
        PREFIX=$PREFIX \
        FILESDIR=$PREFIX/share/cppcheck \
        CFGDIR=$PREFIX/share/cppcheck/cfg \
            make install
    popd
fi

log_tool_name "swig $SWIG_VERSION"
if [ ! -d swig-$SWIG_VERSION/install ]; then
    if [ -d swig-$SWIG_VERSION ]; then
        rm -rf swig-$SWIG_VERSION
    fi
    tar -xvf ../archives/swig-$SWIG_VERSION.tar.gz
    pushd swig-$SWIG_VERSION
    ./autogen.sh
    mkdir build && pushd build
    ../configure --prefix=$DIR/build/swig-$SWIG_VERSION/install
    make -j$CPUS
    make install
    popd && popd
fi

log_tool_name "LLVM $LLVM_VERSION"
if [ ! -f $PREFIX/bin/clang ]; then
    if [ -d llvmorg-$LLVM_VERSION ]; then
        rm -rf llvmorg-$LLVM_VERSION
    fi
    cp -r ../archives/llvmorg-$LLVM_VERSION ./llvmorg-$LLVM_VERSION

    # NOTE: Go under llvmorg-$LLVM_VERSION/llvm/CMakeLists.txt to see all
    #       options, docs pages are not up to date.
    TOOLCHAIN_LLVM_ENABLE_PROJECTS="clang;clang-tools-extra;compiler-rt;lldb;lld"
    TOOLCHAIN_LLVM_ENABLE_RUNTIMES="libunwind"
    if [ "$TOOLCHAIN_STDCXX" = "libc++" ]; then
        TOOLCHAIN_LLVM_ENABLE_RUNTIMES="$TOOLCHAIN_LLVM_ENABLE_RUNTIMES;libcxx;libcxxabi"
    fi

    pushd llvmorg-$LLVM_VERSION
    # activate swig
    export PATH=$DIR/build/swig-$SWIG_VERSION/install/bin:$PATH
    # influenced by: https://buildd.debian.org/status/fetch.php?pkg=llvm-toolchain-7&arch=amd64&ver=1%3A7.0.1%7E%2Brc2-1%7Eexp1&stamp=1541506173&raw=0
    cmake -S llvm -B build -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX="$PREFIX" \
        -DCMAKE_C_COMPILER=$PREFIX/bin/gcc \
        -DCMAKE_CXX_COMPILER=$PREFIX/bin/g++ \
        -DCMAKE_CXX_LINK_FLAGS="-L$PREFIX/lib64 -Wl,-rpath,$PREFIX/lib64" \
        -DCMAKE_INSTALL_PREFIX=$PREFIX \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O2 -DNDEBUG" \
        -DCMAKE_CXX_FLAGS=' -fuse-ld=gold -fPIC -Wno-unused-command-line-argument -Wno-unknown-warning-option' \
        -DCMAKE_C_FLAGS=' -fuse-ld=gold -fPIC -Wno-unused-command-line-argument -Wno-unknown-warning-option' \
        -DLLVM_ENABLE_PROJECTS="$TOOLCHAIN_LLVM_ENABLE_PROJECTS" \
        -DLLVM_ENABLE_RUNTIMES="$TOOLCHAIN_LLVM_ENABLE_RUNTIMES" \
        -DLLVM_LINK_LLVM_DYLIB=ON \
        -DLLVM_INSTALL_UTILS=ON \
        -DLLVM_VERSION_SUFFIX= \
        -DLLVM_BUILD_LLVM_DYLIB=ON \
        -DLLVM_ENABLE_RTTI=ON \
        -DLLVM_ENABLE_FFI=ON \
        -DLLVM_BINUTILS_INCDIR=$PREFIX/include/ \
        -DLLVM_INCLUDE_BENCHMARKS=OFF \
        -DLLVM_USE_PERF=yes \
        -DCOMPILER_RT_INCLUDE_TESTS=OFF \
        -DLIBCXX_INCLUDE_BENCHMARKS=OFF
    pushd build
    make -j$CPUS
    if [[ "$for_arm" = false ]]; then
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
fi

popd

# create README
if [ ! -f $PREFIX/README.md ]; then
    cat >$PREFIX/README.md <<EOF
# Memgraph Toolchain v$TOOLCHAIN_VERSION

## Included tools

 - GCC $GCC_VERSION
 - Binutils $BINUTILS_VERSION
 - GDB $GDB_VERSION
 - CMake $CMAKE_VERSION
 - Cppcheck $CPPCHECK_VERSION
 - LLVM (clang;clang-tools-extra;compiler-rt;libunwind;lldb[;libcxx;libcxxabi]) $LLVM_VERSION

## Required libraries

In order to be able to run all of these tools you should install the following
packages:
\`\`\`
./$ENV_SCRIPT_RELATIVE list TOOLCHAIN_RUN_DEPS)
\`\`\`
by executing:
\`\`\`
./$ENV_SCRIPT_RELATIVE install TOOLCHAIN_RUN_DEPS)
\`\`\`

## Usage

In order to use the toolchain you just have to source the activation script:

\`\`\`
source $PREFIX/activate
\`\`\`

On the other hand, \`deactivate\` will get back your original setup by restoring
the initial environment variables.
EOF
fi

# create activation script
if [ ! -f $PREFIX/activate ]; then
    cat >$PREFIX/activate <<EOF
SCRIPT_DIR=\$( cd -- "\$( dirname -- "\${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
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
export ORIG_LD_LIBRARY_PATH=\$LD_LIBRARY_PATH
export ORIG_CXXFLAGS=\$CXXFLAGS
export ORIG_CFLAGS=\$CFLAGS

# activate new environment
export PATH=\$PREFIX:\$PREFIX/bin:\$PATH
export PS1="($NAME) \$PS1"
export LD_LIBRARY_PATH=\$PREFIX/lib:\$PREFIX/lib64
export CXXFLAGS=-isystem\ \$PREFIX/include\ \$CXXFLAGS
export CFLAGS=-isystem\ \$PREFIX/include\ \$CFLAGS
export MG_TOOLCHAIN_ROOT=\$PREFIX
export MG_TOOLCHAIN_VERSION=$TOOLCHAIN_VERSION

# disable root
function su () {
    echo "You don't want to use root functions while using the toolchain!"
    return 1
}
function sudo () {
    echo "You don't want to use root functions while using the toolchain!"
    return 1
}

# create deactivation function
function deactivate() {
    export PATH=\$ORIG_PATH
    export PS1=\$ORIG_PS1
    export LD_LIBRARY_PATH=\$ORIG_LD_LIBRARY_PATH
    export CXXFLAGS=\$ORIG_CXXFLAGS
    export CFLAGS=\$ORIG_CFLAGS
    unset ORIG_PATH ORIG_PS1 ORIG_LD_LIBRARY_PATH ORIG_CXXFLAGS ORIG_CFLAGS
    unset MG_TOOLCHAIN_ROOT MG_TOOLCHAIN_VERSION
    unset -f su sudo deactivate
}
EOF
fi

###################################
#                                 #
# Third-party library compilation #
#                                 #
###################################
# variable:
#   * architecture    : ARM, x86
#   * operating system: Lin, Mac, Win (many distros and versions)
#   * compiler        : clang, gcc
#   * standard lib    : libstdc++, libc++
# options:
#   * extreme 1 -> move all libs + Memgraph compilation here, have one giant script
#   * extreme 2 -> build a granular package manager, each lib (for all variable) separated

# NOTE: pulsar v2.8.1 client can't be compield on 1.87+ of Boost -> both have to be upgraded.
# BOOST_SHA256=205666dea9f6a7cfed87c7a6dfbeb52a2c1b9de55712c9c1a87735d7181452b6
# BOOST_VERSION=1.81.0
BOOST_SHA256=be0d91732d5b0cc6fbb275c7939974457e79b54d6f07ce2e3dfdd68bef883b0b
BOOST_VERSION=1.85.0
BOOST_VERSION_UNDERSCORES=`echo "${BOOST_VERSION//./_}"`
BZIP2_SHA256=a2848f34fcd5d6cf47def00461fcb528a0484d8edef8208d6d2e2909dc61d9cd
BZIP2_VERSION=1.0.6
DOUBLE_CONVERSION_SHA256=8a79e87d02ce1333c9d6c5e47f452596442a343d8c3e9b234e8a62fce1b1d49c
DOUBLE_CONVERSION_VERSION=3.1.6
# NOTE: At some point Folly stopped supporting OpenSSL 1.0 which is critical
# for CentOS7. If you decide to bump FBLIBS_VERSION drop Folly of stop
# supporting CentOS7.
FBLIBS_VERSION=2022.01.31.00
FIZZ_SHA256=32a60e78d41ea2682ce7e5d741b964f0ea83642656e42d4fea90c0936d6d0c7d
FOLLY_SHA256=7b8d5dd2eb51757858247af0ad27af2e3e93823f84033a628722b01e06cd68a9
PROXYGEN_SHA256=5360a8ccdfb2f5a6c7b3eed331ec7ab0e2c792d579c6fff499c85c516c11fe14
WANGLE_SHA256=1002e9c32b6f4837f6a760016e3b3e22f3509880ef3eaad191c80dc92655f23f
# NOTE: spdlog depends on exact fmt versions -> UPGRADE fmt and spdlog TOGETHER.
FMT_SHA256=78b8c0a72b1c35e4443a7e308df52498252d1cefc2b08c9a97bc9ee6cfe61f8b
FMT_VERSION=10.1.1
SPDLOG_SHA256=4dccf2d10f410c1e2feaff89966bfc49a1abb29ef6f08246335b110e001e09a9
SPDLOG_VERSION=1.12.0
GFLAGS_COMMIT_HASH=b37ceb03a0e56c9f15ce80409438a555f8a67b7c
GLOG_SHA256=eede71f28371bf39aa69b45de23b329d37214016e2055269b3b5e7cfd40b59f5
GLOG_VERSION=0.5.0
JEMALLOC_VERSION=5.2.1 # Some people complained about 5.3.0 performance.
LIBAIO_VERSION=0.3.112
LIBEVENT_VERSION=2.1.12-stable
LIBSODIUM_VERSION=1.0.20
LIBUNWIND_VERSION=1.8.1
# LZ4_SHA256=0b0e3aa07c8c063ddf40b082bdf7e37a1562bda40a0ff5272957f3e987e0e54b
# LZ4_VERSION=1.9.4
LZ4_SHA256=537512904744b35e232912055ccf8ec66d768639ff3abe5788d90d792ec5f48b
LZ4_VERSION=1.10.0
# SNAPPY_SHA256=75c1fbb3d618dd3a0483bff0e26d0a92b495bbe5059c8b4f1c962b478b6e06e7
# SNAPPY_VERSION=1.1.9
SNAPPY_SHA256=736aeb64d86566d2236ddffa2865ee5d7a82d26c9016b36218fcc27ea4f09f86
SNAPPY_VERSION=1.2.1
XZ_VERSION=5.6.3 # for LZMA
ZLIB_VERSION=1.3.1
ZSTD_VERSION=1.5.6

pushd archives
if [ ! -f boost_$BOOST_VERSION_UNDERSCORES.tar.gz ]; then
    # do not redirect the download into a file, because it will download the file into a ".1" postfixed file
    # I am not sure why this is happening, but I think because of some redirects that happens during the download
    wget https://archives.boost.io/release/$BOOST_VERSION/source/boost_$BOOST_VERSION_UNDERSCORES.tar.gz -O boost_$BOOST_VERSION_UNDERSCORES.tar.gz
fi
if [ ! -f bzip2-$BZIP2_VERSION.tar.gz ]; then
    wget https://sourceware.org/pub/bzip2/bzip2-$BZIP2_VERSION.tar.gz -O bzip2-$BZIP2_VERSION.tar.gz
fi
if [ ! -f double-conversion-$DOUBLE_CONVERSION_VERSION.tar.gz ]; then
    wget https://github.com/google/double-conversion/archive/refs/tags/v$DOUBLE_CONVERSION_VERSION.tar.gz -O double-conversion-$DOUBLE_CONVERSION_VERSION.tar.gz
fi
if [ ! -f fizz-$FBLIBS_VERSION.tar.gz ]; then
    wget https://github.com/facebookincubator/fizz/releases/download/v$FBLIBS_VERSION/fizz-v$FBLIBS_VERSION.tar.gz -O fizz-$FBLIBS_VERSION.tar.gz
fi

if [ ! -f fmt-$FMT_VERSION.tar.gz ]; then
    wget https://github.com/fmtlib/fmt/archive/refs/tags/$FMT_VERSION.tar.gz -O fmt-$FMT_VERSION.tar.gz
fi
if [ ! -f spdlog-$SPDLOG_VERSION.tar.gz ]; then
    wget https://github.com/gabime/spdlog/archive/refs/tags/v$SPDLOG_VERSION.tar.gz -O spdlog-$SPDLOG_VERSION.tar.gz
fi
if [ ! -d folly-$FBLIBS_VERSION ]; then
    git clone --depth 1 --branch v$FBLIBS_VERSION https://github.com/facebook/folly.git folly-$FBLIBS_VERSION
fi
if [ ! -f glog-$GLOG_VERSION.tar.gz ]; then
    wget https://github.com/google/glog/archive/refs/tags/v$GLOG_VERSION.tar.gz -O glog-$GLOG_VERSION.tar.gz
fi
if [ ! -f libaio-$LIBAIO_VERSION.tar.gz ]; then
    wget https://releases.pagure.org/libaio/libaio-$LIBAIO_VERSION.tar.gz -O libaio-$LIBAIO_VERSION.tar.gz
fi
if [ ! -f libevent-$LIBEVENT_VERSION.tar.gz ]; then
    wget https://github.com/libevent/libevent/releases/download/release-$LIBEVENT_VERSION/libevent-$LIBEVENT_VERSION.tar.gz -O libevent-$LIBEVENT_VERSION.tar.gz
fi
if [ ! -f libsodium-$LIBSODIUM_VERSION.tar.gz ]; then
    curl https://download.libsodium.org/libsodium/releases/libsodium-$LIBSODIUM_VERSION.tar.gz -o libsodium-$LIBSODIUM_VERSION.tar.gz
fi
if [ ! -f libunwind-$LIBUNWIND_VERSION.tar.gz ]; then
    wget https://github.com/libunwind/libunwind/releases/download/v$LIBUNWIND_VERSION/libunwind-$LIBUNWIND_VERSION.tar.gz -O libunwind-$LIBUNWIND_VERSION.tar.gz
fi
if [ ! -f lz4-$LZ4_VERSION.tar.gz ]; then
    wget https://github.com/lz4/lz4/archive/v$LZ4_VERSION.tar.gz -O lz4-$LZ4_VERSION.tar.gz
fi
if [ ! -f proxygen-$FBLIBS_VERSION.tar.gz ]; then
    wget https://github.com/facebook/proxygen/releases/download/v$FBLIBS_VERSION/proxygen-v$FBLIBS_VERSION.tar.gz -O proxygen-$FBLIBS_VERSION.tar.gz
fi
if [ ! -f snappy-$SNAPPY_VERSION.tar.gz ]; then
    wget https://github.com/google/snappy/archive/refs/tags/$SNAPPY_VERSION.tar.gz -O snappy-$SNAPPY_VERSION.tar.gz
fi
if [ ! -f xz-$XZ_VERSION.tar.gz ]; then
    wget https://tukaani.org/xz/xz-$XZ_VERSION.tar.gz -O xz-$XZ_VERSION.tar.gz
fi
if [ ! -f zlib-$ZLIB_VERSION.tar.gz ]; then
    wget https://zlib.net/zlib-$ZLIB_VERSION.tar.gz -O zlib-$ZLIB_VERSION.tar.gz
fi
if [ ! -f zstd-$ZSTD_VERSION.tar.gz ]; then
    wget https://github.com/facebook/zstd/releases/download/v$ZSTD_VERSION/zstd-$ZSTD_VERSION.tar.gz -O zstd-$ZSTD_VERSION.tar.gz
fi
if [ ! -f wangle-$FBLIBS_VERSION.tar.gz ]; then
    wget https://github.com/facebook/wangle/releases/download/v$FBLIBS_VERSION/wangle-v$FBLIBS_VERSION.tar.gz -O wangle-$FBLIBS_VERSION.tar.gz
fi

# verify boost
echo "$BOOST_SHA256 boost_$BOOST_VERSION_UNDERSCORES.tar.gz" | sha256sum -c
# verify bzip2
echo "$BZIP2_SHA256 bzip2-$BZIP2_VERSION.tar.gz" | sha256sum -c
# verify double-conversion
echo "$DOUBLE_CONVERSION_SHA256 double-conversion-$DOUBLE_CONVERSION_VERSION.tar.gz" | sha256sum -c
# verify fizz
echo "$FIZZ_SHA256 fizz-$FBLIBS_VERSION.tar.gz" | sha256sum -c
# verify fmt
echo "$FMT_SHA256 fmt-$FMT_VERSION.tar.gz" | sha256sum -c
# verify spdlog
echo "$SPDLOG_SHA256 spdlog-$SPDLOG_VERSION.tar.gz" | sha256sum -c
# verify folly
# echo "$FOLLY_SHA256 folly-$FBLIBS_VERSION.tar.gz" | sha256sum -c
# verify glog
echo "$GLOG_SHA256  glog-$GLOG_VERSION.tar.gz" | sha256sum -c
# verify libaio
if [ ! -f libaio-CHECKSUMS ]; then
    wget https://releases.pagure.org/libaio/CHECKSUMS -O libaio-CHECKSUMS
fi
cat libaio-CHECKSUMS | grep "SHA256 (libaio-$LIBAIO_VERSION.tar.gz)" | sha256sum -c
# verify libevent
if [ ! -f libevent-$LIBEVENT_VERSION.tar.gz.asc ]; then
    wget https://github.com/libevent/libevent/releases/download/release-$LIBEVENT_VERSION/libevent-$LIBEVENT_VERSION.tar.gz.asc
fi
if false; then
    $GPG --keyserver $KEYSERVER --recv-keys 0x9E3AC83A27974B84D1B3401DB86086848EF8686D
    $GPG --verify libevent-$LIBEVENT_VERSION.tar.gz.asc libevent-$LIBEVENT_VERSION.tar.gz
fi
# verify libsodium
if [ ! -f libsodium-$LIBSODIUM_VERSION.tar.gz.sig ]; then
    curl https://download.libsodium.org/libsodium/releases/libsodium-$LIBSODIUM_VERSION.tar.gz.sig -o libsodium-$LIBSODIUM_VERSION.tar.gz.sig
fi
if false; then
    $GPG --keyserver $KEYSERVER --recv-keys 0x0C7983A8FD9A104C623172CB62F25B592B6F76DA
    $GPG --verify libsodium-$LIBSODIUM_VERSION.tar.gz.sig libsodium-$LIBSODIUM_VERSION.tar.gz
fi
if false; then
    # verify libunwind
    if [ ! -f libunwind-$LIBUNWIND_VERSION.tar.gz.sig ]; then
        wget https://github.com/libunwind/libunwind/releases/download/v$LIBUNWIND_VERSION/libunwind-$LIBUNWIND_VERSION.tar.gz.sig
    fi
    $GPG --keyserver $KEYSERVER --recv-keys 0x75D2CFC56CC2E935A4143297015A268A17D55FA4
    $GPG --verify libunwind-$LIBUNWIND_VERSION.tar.gz.sig libunwind-$LIBUNWIND_VERSION.tar.gz
fi
# verify lz4
echo "$LZ4_SHA256  lz4-$LZ4_VERSION.tar.gz" | sha256sum -c
# verify proxygen
echo "$PROXYGEN_SHA256 proxygen-$FBLIBS_VERSION.tar.gz" | sha256sum -c
# verify snappy
echo "$SNAPPY_SHA256  snappy-$SNAPPY_VERSION.tar.gz" | sha256sum -c
# verify xz
if [ ! -f xz-$XZ_VERSION.tar.gz.sig ]; then
    wget https://tukaani.org/xz/xz-$XZ_VERSION.tar.gz.sig
fi
if false; then
    $GPG --import ../xz_pgp.txt
    $GPG --verify xz-$XZ_VERSION.tar.gz.sig xz-$XZ_VERSION.tar.gz
fi
# verify zlib
if [ ! -f zlib-$ZLIB_VERSION.tar.gz.asc ]; then
    wget https://zlib.net/zlib-$ZLIB_VERSION.tar.gz.asc
fi
if false; then
    $GPG --keyserver $KEYSERVER --recv-keys 0x783FCD8E58BCAFBA
    $GPG --verify zlib-$ZLIB_VERSION.tar.gz.asc zlib-$ZLIB_VERSION.tar.gz
fi
#verify zstd
if [ ! -f zstd-$ZSTD_VERSION.tar.gz.sig ]; then
    wget https://github.com/facebook/zstd/releases/download/v$ZSTD_VERSION/zstd-$ZSTD_VERSION.tar.gz.sig
fi
if false; then
    $GPG --keyserver $KEYSERVER --recv-keys 0xEF8FE99528B52FFD
    $GPG --verify zstd-$ZSTD_VERSION.tar.gz.sig zstd-$ZSTD_VERSION.tar.gz
fi
# verify wangle
echo "$WANGLE_SHA256 wangle-$FBLIBS_VERSION.tar.gz" | sha256sum -c
popd

pushd build
source $PREFIX/activate
export CC=$PREFIX/bin/clang
export CXX=$PREFIX/bin/clang++
export CFLAGS="$CFLAGS -fPIC"
if [ "$TOOLCHAIN_STDCXX" = "libstdc++" ]; then
    export CXXFLAGS="$CXXFLAGS -fPIC"
else
    export CXXFLAGS="$CXXFLAGS -fPIC -stdlib=libc++"
fi
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
COMMON_MAKE_INSTALL_FLAGS="-j$CPUS BUILD_SHARED=no PREFIX=$PREFIX install"

log_tool_name "bzip2 $BZIP2_VERSION"
if [ ! -f $PREFIX/include/bzlib.h ]; then
    if [ -d bzip2-$BZIP2_VERSION ]; then
        rm -rf bzip2-$BZIP2_VERSION
    fi
    tar -xzf ../archives/bzip2-$BZIP2_VERSION.tar.gz
    pushd bzip2-$BZIP2_VERSION
    make $COMMON_MAKE_INSTALL_FLAGS
    popd
fi

log_tool_name "fmt $FMT_VERSION"
if [ ! -d $PREFIX/include/fmt ]; then
    if [ -d fmt-$FMT_VERSION ]; then
        rm -rf fmt-$FMT_VERSION
    fi
    tar -xzf ../archives/fmt-$FMT_VERSION.tar.gz
    pushd fmt-$FMT_VERSION
    mkdir build && pushd build
    cmake .. $COMMON_CMAKE_FLAGS -DFMT_TEST=OFF
    make -j$CPUS install
    popd && popd
fi

log_tool_name "spdlog $SPDLOG_VERSION"
if [ ! -d $PREFIX/include/spdlog ]; then
    if [ -d spdlog-$SPDLOG_VERSION ]; then
        rm -rf spdlog-$SPDLOG_VERSION
    fi
    tar -xzf ../archives/spdlog-$SPDLOG_VERSION.tar.gz
    pushd spdlog-$SPDLOG_VERSION
    mkdir build && pushd build
    cmake .. $COMMON_CMAKE_FLAGS
    make -j$CPUS install
    popd && popd
fi

log_tool_name "lz4 $LZ4_VERSION"
if [ ! -f $PREFIX/include/lz4.h ]; then
    if [ -d lz4-$LZ4_VERSION ]; then
        rm -rf lz4-$LZ4_VERSION
    fi
    tar -xzf ../archives/lz4-$LZ4_VERSION.tar.gz
    pushd lz4-$LZ4_VERSION
    make $COMMON_MAKE_INSTALL_FLAGS
    popd
fi

log_tool_name "xz $XZ_VERSION"
if [ ! -f $PREFIX/include/lzma.h ]; then
    if [ -d xz-$XZ_VERSION ]; then
        rm -rf xz-$XZ_VERSION
    fi
    tar -xzf ../archives/xz-$XZ_VERSION.tar.gz
    pushd xz-$XZ_VERSION
    ./configure $COMMON_CONFIGURE_FLAGS
    make -j$CPUS install
    popd
fi

log_tool_name "zlib $ZLIB_VERSION"
if [ ! -f $PREFIX/include/zlib.h ]; then
    if [ -d zlib-$ZLIB_VERSION ]; then
        rm -rf zlib-$ZLIB_VERSION
    fi
    tar -xzf ../archives/zlib-$ZLIB_VERSION.tar.gz
    pushd zlib-$ZLIB_VERSION
    mkdir build && pushd build
    cmake .. $COMMON_CMAKE_FLAGS
    make -j$CPUS install
    rm $PREFIX/lib/libz.so*
    popd && popd
fi

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

log_tool_name "jmalloc $JEMALLOC_VERSION"
if [ ! -d $PREFIX/include/jemalloc ]; then
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
      --enable-shared=no --prefix=$PREFIX \
      --with-malloc-conf="background_thread:true,retain:false,percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000"
    ##### NOTE: Old jmalloc config (toolchain-v5)
    # ./autogen.sh
    # MALLOC_CONF="retain:false,percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000" \
    # ./configure \
    #      --disable-cxx \
    #      $COMMON_CONFIGURE_FLAGS \
    #      --with-malloc-conf="retain:false,percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000"
    ##### NOTE: Old jmalloc config (toolchain-v4 and before).
    # ./autogen.sh --with-malloc-conf="percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000"
    # env \
    #     EXTRA_FLAGS="-DJEMALLOC_NO_PRIVATE_NAMESPACE -D_GNU_SOURCE -Wno-redundant-decls" \
    #     ./configure $COMMON_CONFIGURE_FLAGS --disable-cxx
    make -j$CPUS install
    popd
fi

log_tool_name "BOOST $BOOST_VERSION"
if [ ! -d $PREFIX/include/boost ]; then
    if [ -d boost_$BOOST_VERSION_UNDERSCORES ]; then
        rm -rf boost_$BOOST_VERSION_UNDERSCORES
    fi
    tar -xzf ../archives/boost_$BOOST_VERSION_UNDERSCORES.tar.gz
    pushd boost_$BOOST_VERSION_UNDERSCORES
    # TODO(gitbuda): Figure out why --with-libraries=python doesn't work for protobuf
    ./bootstrap.sh --prefix=$PREFIX --with-toolset=clang --with-python=python3 --without-icu
    if [ "$TOOLCHAIN_STDCXX" = "libstdc++" ]; then
        ./b2 toolset=clang -j$CPUS install variant=release link=static cxxstd=20 --disable-icu \
            -sZLIB_SOURCE="$PREFIX" -sZLIB_INCLUDE="$PREFIX/include" -sZLIB_LIBPATH="$PREFIX/lib" \
            -sBZIP2_SOURCE="$PREFIX" -sBZIP2_INCLUDE="$PREFIX/include" -sBZIP2_LIBPATH="$PREFIX/lib" \
            -sLZMA_SOURCE="$PREFIX" -sLZMA_INCLUDE="$PREFIX/include" -sLZMA_LIBPATH="$PREFIX/lib" \
            -sZSTD_SOURCE="$PREFIX" -sZSTD_INCLUDE="$PREFIX/include" -sZSTD_LIBPATH="$PREFIX/lib"
    else
        ./b2 toolset=clang -j$CPUS install variant=release link=static cxxstd=20 --disable-icu \
            cxxflags="-stdlib=libc++" linkflags="-stdlib=libc++" \
            -sZLIB_SOURCE="$PREFIX" -sZLIB_INCLUDE="$PREFIX/include" -sZLIB_LIBPATH="$PREFIX/lib" \
            -sBZIP2_SOURCE="$PREFIX" -sBZIP2_INCLUDE="$PREFIX/include" -sBZIP2_LIBPATH="$PREFIX/lib" \
            -sLZMA_SOURCE="$PREFIX" -sLZMA_INCLUDE="$PREFIX/include" -sLZMA_LIBPATH="$PREFIX/lib" \
            -sZSTD_SOURCE="$PREFIX" -sZSTD_INCLUDE="$PREFIX/include" -sZSTD_LIBPATH="$PREFIX/lib"
    fi
    popd
fi

log_tool_name "double-conversion $DOUBLE_CONVERSION_VERSION"
if [ ! -d $PREFIX/include/double-conversion ]; then
    if [ -d double-conversion-$DOUBLE_CONVERSION_VERSION ]; then
        rm -rf double-conversion-$DOUBLE_CONVERSION_VERSION
    fi
    tar -xzf ../archives/double-conversion-$DOUBLE_CONVERSION_VERSION.tar.gz
    pushd double-conversion-$DOUBLE_CONVERSION_VERSION
    # build is used by facebook builder
    mkdir build
    pushd build
    cmake .. $COMMON_CMAKE_FLAGS
    make -j$CPUS install
    popd && popd
fi

log_tool_name "gflags (memgraph fork $GFLAGS_COMMIT_HASH)"
if [ ! -d $PREFIX/include/gflags ]; then
    if [ -d gflags ]; then
        rm -rf gflags
    fi
    git clone https://github.com/memgraph/gflags.git gflags
    pushd gflags
    git checkout $GFLAGS_COMMIT_HASH
    mkdir build
    pushd build
    cmake .. $COMMON_CMAKE_FLAGS \
        -DREGISTER_INSTALL_PREFIX=OFF \
        -DBUILD_gflags_nothreads_LIB=OFF \
        -DGFLAGS_NO_FILENAMES=1
    make -j$CPUS install
    popd && popd
fi

log_tool_name "libunwind $LIBUNWIND_VERSION"
if [ ! -f $PREFIX/include/libunwind.h ]; then
    if [ -d libunwind-$LIBUNWIND_VERSION ]; then
        rm -rf libunwind-$LIBUNWIND_VERSION
    fi
    tar -xzf ../archives/libunwind-$LIBUNWIND_VERSION.tar.gz
    pushd libunwind-$LIBUNWIND_VERSION
    ./configure $COMMON_CONFIGURE_FLAGS \
        --disable-minidebuginfo # disable LZMA usage to not depend on libLZMA
    make -j$CPUS install
    popd
fi

log_tool_name "glog $GLOG_VERSION"
if [ ! -d $PREFIX/include/glog ]; then
    if [ -d glog-$GLOG_VERSION ]; then
        rm -rf glog-$GLOG_VERSION
    fi
    tar -xzf ../archives/glog-$GLOG_VERSION.tar.gz
    pushd glog-$GLOG_VERSION
    mkdir build
    pushd build
    cmake .. $COMMON_CMAKE_FLAGS -DGFLAGS_NOTHREADS=OFF
    make -j$CPUS install
    popd && popd
fi

log_tool_name "libevent $LIBEVENT_VERSION"
if [ ! -d $PREFIX/include/event2 ]; then
    if [ -d libevent-$LIBEVENT_VERSION ]; then
        rm -rf libevent-$LIBEVENT_VERSION
    fi
    tar -xzf ../archives/libevent-$LIBEVENT_VERSION.tar.gz
    pushd libevent-$LIBEVENT_VERSION
    mkdir build
    pushd build
    cmake .. $COMMON_CMAKE_FLAGS \
        -DEVENT__DISABLE_BENCHMARK=ON \
        -DEVENT__DISABLE_REGRESS=ON \
        -DEVENT__DISABLE_SAMPLES=ON \
        -DEVENT__DISABLE_TESTS=ON \
        -DEVENT__LIBRARY_TYPE="STATIC"
    make -j$CPUS install
    popd && popd
fi

log_tool_name "snappy $SNAPPY_VERSION"
if [ ! -f $PREFIX/include/snappy.h ]; then
    if [ -d snappy-$SNAPPY_VERSION ]; then
        rm -rf snappy-$SNAPPY_VERSION
    fi
    tar -xzf ../archives/snappy-$SNAPPY_VERSION.tar.gz
    pushd snappy-$SNAPPY_VERSION
    patch -p1 <  $DIR/snappy.patch
    mkdir build
    pushd build
    cmake .. $COMMON_CMAKE_FLAGS \
        -DSNAPPY_BUILD_TESTS=OFF \
        -DSNAPPY_BUILD_BENCHMARKS=OFF \
        -DSNAPPY_FUZZING_BUILD=OFF
    make -j$CPUS install
    popd && popd
fi

log_tool_name "libsodium $LIBSODIUM_VERSION"
if [ ! -f $PREFIX/include/sodium.h ]; then
    if [ -d libsodium-$LIBSODIUM_VERSION ]; then
        rm -rf libsodium-$LIBSODIUM_VERSION
    fi
    tar -xzf ../archives/libsodium-$LIBSODIUM_VERSION.tar.gz
    pushd libsodium-$LIBSODIUM_VERSION
    ./configure $COMMON_CONFIGURE_FLAGS
    make -j$CPUS install
    popd
fi

log_tool_name "libaio $LIBAIO_VERSION"
if [ ! -f $PREFIX/include/libaio.h ]; then
    if [ -d libaio-$LIBAIO_VERSION ]; then
        rm -rf libaio-$LIBAIO_VERSION
    fi
    tar -xzf ../archives/libaio-$LIBAIO_VERSION.tar.gz
    pushd libaio-$LIBAIO_VERSION
    make prefix=$PREFIX ENABLE_SHARED=0 -j$CPUS install
    popd
fi

ROCKSDB_TAG="v9.7.3"
log_tool_name "rocksdb $ROCKSDB_TAG"
if [ ! -f $PREFIX/lib/librocksdb.a ]; then
    if [ -d rocksdb ]; then
        rm -rf rocksdb
    fi
    git clone https://github.com/facebook/rocksdb.git rocksdb
    pushd rocksdb
    git checkout $ROCKSDB_TAG
    # NOTE: Disables building shared lib but then the find_package fails.
    # git apply "$DIR/rocksdb-$ROCKSDB_TAG.patch"
    mkdir -p _build && pushd _build
    cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$PREFIX \
      -DUSE_RTTI=ON \
      -DWITH_TESTS=OFF \
      -DGFLAGS_NOTHREADS=OFF \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=true \
      ..
    make -j$CPUS rocksdb rocksdb-shared install
    popd && popd
fi

NURAFT_COMMIT_HASH="4b148a7e76291898c838a7457eeda2b16f7317ea"
log_tool_name "nuraft $NURAFT_COMMIT_HASH"
if [ ! -f $PREFIX/lib/libnuraft.a ]; then
    if [ -d nuraft ]; then
        rm -rf nuraft
    fi
    git clone https://github.com/eBay/NuRaft.git nuraft
    pushd nuraft
    git checkout $NURAFT_COMMIT_HASH
    git apply "$DIR/nuraft.patch"
    ./prepare.sh # Downloads ASIO.
    cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$PREFIX \
      .
    make -j$CPUS install
    popd
fi

ANTLR_TAG="4.13.2" # 2024-08-03
log_tool_name "antlr4 $ANTLR_TAG"
if [ ! -f $PREFIX/lib/libantlr4-runtime.a ]; then
    if [ -d antlr4 ]; then
        rm -rf antlr4
    fi
    git clone https://github.com/antlr/antlr4.git antlr4
    pushd antlr4
    git checkout $ANTLR_TAG
    pushd runtime/Cpp
    # NOTE: WITH_LIBCXX=OFF is because of the Debian bug.
    cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$PREFIX \
      -DWITH_LIBCXX=OFF \
      -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=true \
      -DCMAKE_CXX_STANDARD=20 \
      -DANTLR_BUILD_CPP_TESTS=OFF \
      -DANTLR_BUILD_SHARED=OFF \
      .
    make -j$CPUS antlr4_static install
    popd && popd
fi
log_tool_name "antlr4-generator $ANTLR_TAG"
antlr_generator_url="https://www.antlr.org/download/antlr-$ANTLR_TAG-complete.jar"
antlr_generator_filename="$(basename "$antlr_generator_url")"
if [ ! -f "$PREFIX/bin/$antlr_generator_filename" ]; then
    timeout 15 wget -nv "$antlr_generator_url" -O "$PREFIX/bin/$antlr_generator_filename"
fi

PROTOBUF_TAG="v3.12.4"
log_tool_name "protobuf $PROTOBUF_TAG"
if [ ! -f $PREFIX/lib/libprotobuf.a ]; then
    if [ -d protobuf ]; then
        rm -rf protobuf
    fi
    git clone https://github.com/protocolbuffers/protobuf.git protobuf
    pushd protobuf
    git checkout $PROTOBUF_TAG
    ./autogen.sh && ./configure CC=clang CXX=clang++ --prefix=$PREFIX
    make -j$CPUS install
    popd
fi

# NOTE: Latest pulsar is different -> migrate in a separated PR.
# PULSAR_TAG="v3.6.0"
# log_tool_name "pulsar $PULSAR_TAG"
# if [ ! -f $PREFIX/lib/libpulsarwithdeps.a ]; then
#     if [ -d pulsar ]; then
#         rm -rf pulsar
#     fi
#     git clone https://github.com/apache/pulsar-client-cpp pulsar
#     pushd pulsar
#     git submodule update --init --recursive
#     git checkout $PULSAR_TAG
#     cmake -B build $COMMON_CMAKE_FLAGS \
#       -DBUILD_DYNAMIC_LIB=OFF \
#       -DBUILD_STATIC_LIB=ON \
#       -DBUILD_TESTS=OFF \
#       -DLINK_STATIC=ON \
#       -DPROTOC_PATH=$PREFIX/bin/protoc \
#       -DUSE_ASIO=OFF \
#       -DUSE_LOG4CXX=OFF
#     cmake --build build -j$CPUS --target install
#     popd
# fi
PULSAR_TAG="v2.8.1"
log_tool_name "pulsar $PULSAR_TAG"
if [ ! -f $PREFIX/lib/libpulsarwithdeps.a ]; then
    if [ -d pulsar ]; then
        rm -rf pulsar
    fi
    git clone https://github.com/apache/pulsar.git pulsar
    pushd pulsar
    git checkout $PULSAR_TAG
    git apply $DIR/pulsar.patch
    pushd pulsar-client-cpp
    cmake -B . $COMMON_CMAKE_FLAGS \
      -DBUILD_DYNAMIC_LIB=OFF \
      -DBUILD_STATIC_LIB=ON \
      -DBUILD_TESTS=OFF \
      -DLINK_STATIC=ON \
      -DPROTOC_PATH=$PREFIX/bin/protoc \
      -DBUILD_PYTHON_WRAPPER=OFF \
      -DBUILD_PERF_TOOLS=OFF \
      -DUSE_LOG4CXX=OFF
    cmake --build . -j$CPUS --target pulsarStaticWithDeps
    # NOTE: For some reason the withdeps is not make installed...
    cp lib/libpulsarwithdeps.a $PREFIX/lib/
    cmake --build . -j$CPUS --target install
    popd
    popd
fi

KAFKA_TAG="v2.6.1"
# NOTE: The lib doesn't compile because of SSL, in the future try to use the
# same version on all operating systems this was an exception to rollout
# toolchain-v6 on time.
if [ "$DISTRO" == "centos-10" ]; then
    KAFKA_TAG="v2.8.0"
fi
log_tool_name "kafka $KAFKA_TAG"
if [ ! -f $PREFIX/lib/librdkafka++.a ]; then
    if [ -d kafka ]; then
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

LIBBCRYPT_TAG="8aa32ad94ebe06b76853b0767c910c9fbf7ccef4"
log_tool_name "libbcrypt $LIBBCRYPT_TAG"
if [ ! -f $PREFIX/lib/bcrypt.a ]; then
    if [ -d libbcrypt ]; then
        rm -rf libbcrypt
    fi
    git clone https://github.com/rg3/libbcrypt
    pushd libbcrypt
    git checkout $LIBBCRYPT_TAG
    sed s/-Wcast-align// -i crypt_blowfish/Makefile
    make
    # NOTE: The libbcrypt doesn't come with the install target.
    cp bcrypt.a $PREFIX/lib/
    mkdir -p $PREFIX/include/libbcrypt/
    cp bcrypt.h $PREFIX/include/libbcrypt/
    popd
fi

GBENCH_TAG="v1.6.0"
log_tool_name "gbenchmark $GBENCH_TAG"
if [ ! -f $PREFIX/lib/libbenchmark.a ]; then
    if [ -d gbenchmark ]; then
        rm -rf gbenchmark
    fi
    git clone https://github.com/google/benchmark.git gbenchmark
    pushd gbenchmark
    git checkout $GBENCH_TAG
    cmake -B build $COMMON_CMAKE_FLAGS \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DBENCHMARK_ENABLE_TESTING=OFF
    cmake --build build -j$CPUS --target install
    popd
fi

GTEST_TAG="v1.14.0"
log_tool_name "gtest $GTEST_TAG"
if [ ! -f $PREFIX/lib/libgtest.a ]; then
    if [ -d googletest ]; then
      rm -rf googletest
    fi
    git clone https://github.com/google/googletest.git googletest
    pushd googletest
    git checkout $GTEST_TAG
    cmake -B build $COMMON_CMAKE_FLAGS \
      -DCMAKE_INSTALL_LIBDIR="$PREFIX/lib"
    cmake --build build -j$CPUS --target install
    popd
fi

MGCLIENT_TAG="v1.4.4"
log_tool_name "mgclient $MGCLIENT_TAG"
if [ ! -f $PREFIX/lib/libmgclient.a ]; then
    if [ -d mgclient ]; then
      rm -rf mgclient
    fi
    git clone https://github.com/memgraph/mgclient.git mgclient
    pushd mgclient
    git checkout $MGCLIENT_TAG
    sed -i 's/\${CMAKE_INSTALL_LIBDIR}/lib/' src/CMakeLists.txt
    cmake -B build $COMMON_CMAKE_FLAGS \
      -DBUILD_TESTING=OFF \
      -DBUILD_CPP_BINDINGS=ON
    cmake --build build -j$CPUS --target install
    popd
fi

MGCONSOLE_TAG="v1.4.0"
log_tool_name "mgconsole $MGCONSOLE_TAG"
if [ ! -f $PREFIX/bin/mgconsole ]; then
    if [ -d mgconsole ]; then
      rm -rf mgconsole
    fi
    git clone https://github.com/memgraph/mgconsole.git mgconsole
    pushd mgconsole
    git checkout $MGCONSOLE_TAG
    cmake -B build $COMMON_CMAKE_FLAGS
    cmake --build build -j$CPUS --target mgconsole install
    popd
fi

LIBRDTSC_TAG="v0.3"
log_tool_name "librdtsc $LIBRDTSC_TAG"
if [ ! -f $PREFIX/lib/librdtsc.a ]; then
    if [ -d librdtsc ]; then
      rm -rf librdtsc
    fi
    git clone https://github.com/gabrieleara/librdtsc.git librdtsc
    pushd librdtsc
    git checkout $LIBRDTSC_TAG
    git apply "$DIR/librdtsc.patch"
    if [[ "$for_arm" = "true" ]]; then
      cmake -B build $COMMON_CMAKE_FLAGS -DLIBRDTSC_ARCH_x86=OFF -DLIBRDTSC_ARCH_ARM64=ON
    else
      cmake -B build $COMMON_CMAKE_FLAGS -DLIBRDTSC_ARCH_x86=ON -DLIBRDTSC_ARCH_ARM64=OFF
    fi
    cmake --build build -j$CPUS --target rdtsc install
    popd
fi

MGCXX_TAG="v0.0.7"
log_tool_name "mgcxx $MGCXX_TAG"
if [ ! -f $PREFIX/lib/libmgcxx_text_search.a ]; then
    if [ -d mgcxx ]; then
      rm -rf mgcxx
    fi
    git clone https://github.com/memgraph/mgcxx.git mgcxx
    pushd mgcxx
    git checkout $MGCXX_TAG
    cmake -B build $COMMON_CMAKE_FLAGS \
      -DENABLE_TESTS=OFF
    cmake --build build -j$CPUS --target install
    popd
fi

# NOTE: Skip FBLIBS -> only used on project-pineapples
#   * older versions don't compile on the latest GCC
#   * newer versions don't work with OpenSSL 1.0 which is critical for CentOS7
if false; then
  log_tool_name "folly $FBLIBS_VERSION"
  if [ ! -d $PREFIX/include/folly ]; then
      if [ -d folly-$FBLIBS_VERSION ]; then
          rm -rf folly-$FBLIBS_VERSION
      fi
      cp -r ../archives/folly-$FBLIBS_VERSION ./folly-$FBLIBS_VERSION
      pushd folly-$FBLIBS_VERSION
      git apply $DIR/folly-$FBLIBS_VERSION.patch
      # build is used by facebook builder
      mkdir _build
      pushd _build
      cmake .. $COMMON_CMAKE_FLAGS \
          -DBOOST_LINK_STATIC=ON \
          -DBUILD_TESTS=OFF \
          -DGFLAGS_NOTHREADS=OFF \
          -DCXX_STD="c++20"
      make -j$CPUS install
      popd && popd
  fi

  log_tool_name "fizz $FBLIBS_VERSION"
  if [ ! -d $PREFIX/include/fizz ]; then
      if [ -d fizz-$FBLIBS_VERSION ]; then
          rm -rf fizz-$FBLIBS_VERSION
      fi
      mkdir fizz-$FBLIBS_VERSION
      tar -xzf ../archives/fizz-$FBLIBS_VERSION.tar.gz -C fizz-$FBLIBS_VERSION
      pushd fizz-$FBLIBS_VERSION
      # build is used by facebook builder
      mkdir _build
      pushd _build
      cmake ../fizz $COMMON_CMAKE_FLAGS \
          -DBUILD_TESTS=OFF \
          -DBUILD_EXAMPLES=OFF \
          -DGFLAGS_NOTHREADS=OFF
      make -j$CPUS install
      popd && popd
  fi

  log_tool_name "wangle FBLIBS_VERSION"
  if [ ! -d $PREFIX/include/wangle ]; then
      if [ -d wangle-$FBLIBS_VERSION ]; then
          rm -rf wangle-$FBLIBS_VERSION
      fi
      mkdir wangle-$FBLIBS_VERSION
      tar -xzf ../archives/wangle-$FBLIBS_VERSION.tar.gz -C wangle-$FBLIBS_VERSION
      pushd wangle-$FBLIBS_VERSION
      # build is used by facebook builder
      mkdir _build
      pushd _build
      cmake ../wangle $COMMON_CMAKE_FLAGS \
          -DBUILD_TESTS=OFF \
          -DBUILD_EXAMPLES=OFF \
          -DGFLAGS_NOTHREADS=OFF
      make -j$CPUS install
      popd && popd
  fi

  log_tool_name "proxygen $FBLIBS_VERSION"
  if [ ! -d $PREFIX/include/proxygen ]; then
      if [ -d proxygen-$FBLIBS_VERSION ]; then
          rm -rf proxygen-$FBLIBS_VERSION
      fi
      mkdir proxygen-$FBLIBS_VERSION
      tar -xzf ../archives/proxygen-$FBLIBS_VERSION.tar.gz -C proxygen-$FBLIBS_VERSION
      pushd proxygen-$FBLIBS_VERSION
      # build is used by facebook builder
      mkdir _build
      pushd _build
      cmake .. $COMMON_CMAKE_FLAGS \
          -DBUILD_TESTS=OFF \
          -DBUILD_SAMPLES=OFF \
          -DGFLAGS_NOTHREADS=OFF \
          -DBUILD_QUIC=OFF
      make -j$CPUS install
      popd && popd
  fi

  log_tool_name "fbthrift $FBLIBS_VERSION"
  if [ ! -d $PREFIX/include/thrift ]; then
      if [ -d fbthrift-$FBLIBS_VERSION ]; then
          rm -rf fbthrift-$FBLIBS_VERSION
      fi
      git clone --depth 1 --branch v$FBLIBS_VERSION https://github.com/facebook/fbthrift.git fbthrift-$FBLIBS_VERSION
      pushd fbthrift-$FBLIBS_VERSION
      # build is used by facebook builder
      mkdir _build
      pushd _build
      if [ "$TOOLCHAIN_STDCXX" = "libstdc++" ]; then
          CMAKE_CXX_FLAGS="-fsized-deallocation"
      else
          CMAKE_CXX_FLAGS="-fsized-deallocation -stdlib=libc++"
      fi
      cmake .. $COMMON_CMAKE_FLAGS \
          -Denable_tests=OFF \
          -DGFLAGS_NOTHREADS=OFF \
          -DCMAKE_CXX_FLAGS="$CMAKE_CXX_FLAGS"
      make -j$CPUS install
      popd
  fi
fi

popd
# NOTE: It's important/clean (e.g., easier upload to S3) to have a separated
# folder to the output archive.
mkdir -p output
pushd output
# Create the toolchain archive.
if [ ! -f $NAME-binaries-$DISTRO_FULL_NAME.tar.gz ]; then
    tar --owner=root --group=root -cpvzf $NAME-binaries-$DISTRO_FULL_NAME.tar.gz -C /opt $NAME
else
  echo "NOTE: Skipping archiving because the file already exists"
fi

# output final instructions
echo -e "\n\n"
echo "All tools have been built. They are installed in '$PREFIX'."
echo "In order to distribute the tools to someone else, an archive with the toolchain was created in the 'build' directory."
echo "If you want to install the packed tools you should execute the following command:"
echo
echo "    tar -xvzf output/$NAME-binaries.tar.gz -C /opt"
echo
echo "Because the tools were built on this machine, you should probably change the permissions of the installation directory using:"
echo
echo "    OPTIONAL: chown -R root:root $PREFIX"
echo
echo "In order to use all of the newly compiled tools you should use the prepared activation script:"
echo
echo "    source $PREFIX/activate"
echo
echo "Or, for more advanced uses, you can add the following lines to your script:"
echo
echo "    export PATH=$PREFIX/bin:\$PATH"
echo "    export LD_LIBRARY_PATH=$PREFIX/lib:$PREFIX/lib64"
echo
echo "Enjoy!"
