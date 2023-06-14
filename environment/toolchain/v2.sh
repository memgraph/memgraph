#!/bin/bash -e

# helpers
pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CPUS=$( grep -c processor < /proc/cpuinfo )
cd "$DIR"

source "$DIR/../util.sh"
DISTRO="$(operating_system)"

# toolchain version
TOOLCHAIN_VERSION=2

# package versions used
GCC_VERSION=10.2.0
BINUTILS_VERSION=2.35.1
case "$DISTRO" in
    centos-7) # because GDB >= 9 does NOT compile with readline6.
        GDB_VERSION=8.3
    ;;
    *)
        GDB_VERSION=10.1
    ;;
esac
CMAKE_VERSION=3.18.4
CPPCHECK_VERSION=2.2
LLVM_VERSION=11.0.0
SWIG_VERSION=4.0.2 # used only for LLVM compilation

# Check for the dependencies.
echo "ALL BUILD PACKAGES: $($DIR/../os/$DISTRO.sh list TOOLCHAIN_BUILD_DEPS)"
$DIR/../os/$DISTRO.sh check TOOLCHAIN_BUILD_DEPS
echo "ALL RUN PACKAGES: $($DIR/../os/$DISTRO.sh list TOOLCHAIN_RUN_DEPS)"
$DIR/../os/$DISTRO.sh check TOOLCHAIN_RUN_DEPS

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
mkdir -p archives

# download all archives
pushd archives
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
if [ ! -f swig-$SWIG_VERSION.tar.gz ]; then
    wget https://github.com/swig/swig/archive/rel-$SWIG_VERSION.tar.gz -O swig-$SWIG_VERSION.tar.gz
fi
if [ ! -f cppcheck-$CPPCHECK_VERSION.tar.gz ]; then
    wget https://github.com/danmar/cppcheck/archive/$CPPCHECK_VERSION.tar.gz -O cppcheck-$CPPCHECK_VERSION.tar.gz
fi
if [ ! -f llvm-$LLVM_VERSION.src.tar.xz ]; then
    wget https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/llvm-$LLVM_VERSION.src.tar.xz
    wget https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/clang-$LLVM_VERSION.src.tar.xz
    wget https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/lld-$LLVM_VERSION.src.tar.xz
    wget https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/clang-tools-extra-$LLVM_VERSION.src.tar.xz
    wget https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/compiler-rt-$LLVM_VERSION.src.tar.xz
fi
if [ ! -f pahole-gdb-master.zip ]; then
    wget https://github.com/PhilArmstrong/pahole-gdb/archive/master.zip -O pahole-gdb-master.zip
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
# verify gcc
if [ ! -f gcc-$GCC_VERSION.tar.gz.sig ]; then
    wget https://ftp.gnu.org/gnu/gcc/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz.sig
fi
# list of valid gcc gnupg keys: https://gcc.gnu.org/mirrors.html
$GPG --keyserver $KEYSERVER --recv-keys 0x3AB00996FC26A641
$GPG --verify gcc-$GCC_VERSION.tar.gz.sig gcc-$GCC_VERSION.tar.gz
# verify binutils
if [ ! -f binutils-$BINUTILS_VERSION.tar.gz.sig ]; then
    wget https://ftp.gnu.org/gnu/binutils/binutils-$BINUTILS_VERSION.tar.gz.sig
fi
$GPG --keyserver $KEYSERVER --recv-keys 0xDD9E3C4F
$GPG --verify binutils-$BINUTILS_VERSION.tar.gz.sig binutils-$BINUTILS_VERSION.tar.gz
# verify gdb
if [ ! -f gdb-$GDB_VERSION.tar.gz.sig ]; then
    wget https://ftp.gnu.org/gnu/gdb/gdb-$GDB_VERSION.tar.gz.sig
fi
$GPG --keyserver $KEYSERVER --recv-keys 0xFF325CF3
$GPG --verify gdb-$GDB_VERSION.tar.gz.sig gdb-$GDB_VERSION.tar.gz
# verify cmake
if [ ! -f cmake-$CMAKE_VERSION-SHA-256.txt ] || [ ! -f cmake-$CMAKE_VERSION-SHA-256.txt.asc ]; then
    wget https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION-SHA-256.txt
    wget https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION-SHA-256.txt.asc
    # Because CentOS 7 doesn't have the `--ignore-missing` flag for `sha256sum`
    # we filter out the missing files from the sums here manually.
    cat cmake-$CMAKE_VERSION-SHA-256.txt | grep "cmake-$CMAKE_VERSION.tar.gz" > cmake-$CMAKE_VERSION-SHA-256-filtered.txt
fi
$GPG --keyserver $KEYSERVER --recv-keys 0xC6C265324BBEBDC350B513D02D2CEF1034921684
sha256sum -c cmake-$CMAKE_VERSION-SHA-256-filtered.txt
$GPG --verify cmake-$CMAKE_VERSION-SHA-256.txt.asc cmake-$CMAKE_VERSION-SHA-256.txt
# verify llvm, cfe, lld, clang-tools-extra
if [ ! -f llvm-$LLVM_VERSION.src.tar.xz.sig ]; then
    wget https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/llvm-$LLVM_VERSION.src.tar.xz.sig
    wget https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/clang-$LLVM_VERSION.src.tar.xz.sig
    wget https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/lld-$LLVM_VERSION.src.tar.xz.sig
    wget https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/clang-tools-extra-$LLVM_VERSION.src.tar.xz.sig
    wget https://github.com/llvm/llvm-project/releases/download/llvmorg-$LLVM_VERSION/compiler-rt-$LLVM_VERSION.src.tar.xz.sig
fi
# list of valid llvm gnupg keys: https://releases.llvm.org/download.html
$GPG --keyserver $KEYSERVER --recv-keys 0x345AD05D
$GPG --verify llvm-$LLVM_VERSION.src.tar.xz.sig llvm-$LLVM_VERSION.src.tar.xz
$GPG --verify clang-$LLVM_VERSION.src.tar.xz.sig clang-$LLVM_VERSION.src.tar.xz
$GPG --verify lld-$LLVM_VERSION.src.tar.xz.sig lld-$LLVM_VERSION.src.tar.xz
$GPG --verify clang-tools-extra-$LLVM_VERSION.src.tar.xz.sig clang-tools-extra-$LLVM_VERSION.src.tar.xz
$GPG --verify compiler-rt-$LLVM_VERSION.src.tar.xz.sig compiler-rt-$LLVM_VERSION.src.tar.xz
popd

# create build directory
mkdir -p build
pushd build

# compile gcc
if [ ! -f $PREFIX/bin/gcc ]; then
    if [ -d gcc-$GCC_VERSION ]; then
        rm -rf gcc-$GCC_VERSION
    fi
    tar -xvf ../archives/gcc-$GCC_VERSION.tar.gz
    pushd gcc-$GCC_VERSION
    ./contrib/download_prerequisites
    mkdir build && pushd build
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
    make -j$CPUS
    # make -k check # run test suite
    make install
    popd && popd
fi

# activate toolchain
export PATH=$PREFIX/bin:$PATH
export LD_LIBRARY_PATH=$PREFIX/lib64

# compile binutils
if [ ! -f $PREFIX/bin/ld.gold ]; then
    if [ -d binutils-$BINUTILS_VERSION ]; then
        rm -rf binutils-$BINUTILS_VERSION
    fi
    tar -xvf ../archives/binutils-$BINUTILS_VERSION.tar.gz
    pushd binutils-$BINUTILS_VERSION
    mkdir build && pushd build
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
    make -j$CPUS
    # make -k check # run test suite
    make install
    popd && popd
fi

# compile gdb
if [ ! -f $PREFIX/bin/gdb ]; then
    if [ -d gdb-$GDB_VERSION ]; then
        rm -rf gdb-$GDB_VERSION
    fi
    tar -xvf ../archives/gdb-$GDB_VERSION.tar.gz
    pushd gdb-$GDB_VERSION
    mkdir build && pushd build
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
    make -j$CPUS
    make install
    popd && popd
fi

# install pahole
if [ ! -d $PREFIX/share/pahole-gdb ]; then
    unzip ../archives/pahole-gdb-master.zip
    mv pahole-gdb-master $PREFIX/share/pahole-gdb
fi

# setup system gdbinit
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

# compile cmake
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

# compile cppcheck
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

# compile swig
if [ ! -d swig-$SWIG_VERSION/install ]; then
    if [ -d swig-$SWIG_VERSION ]; then
        rm -rf swig-$SWIG_VERSION
    fi
    tar -xvf ../archives/swig-$SWIG_VERSION.tar.gz
    mv swig-rel-$SWIG_VERSION swig-$SWIG_VERSION
    pushd swig-$SWIG_VERSION
    ./autogen.sh
    mkdir build && pushd build
    ../configure --prefix=$DIR/build/swig-$SWIG_VERSION/install
    make -j$CPUS
    make install
    popd && popd
fi

# compile llvm
if [ ! -f $PREFIX/bin/clang ]; then
    if [ -d llvm-$LLVM_VERSION ]; then
        rm -rf llvm-$LLVM_VERSION
    fi
    tar -xvf ../archives/llvm-$LLVM_VERSION.src.tar.xz
    mv llvm-$LLVM_VERSION.src llvm-$LLVM_VERSION
    tar -xvf ../archives/clang-$LLVM_VERSION.src.tar.xz
    mv clang-$LLVM_VERSION.src llvm-$LLVM_VERSION/tools/clang
    tar -xvf ../archives/lld-$LLVM_VERSION.src.tar.xz
    mv lld-$LLVM_VERSION.src/ llvm-$LLVM_VERSION/tools/lld
    tar -xvf ../archives/clang-tools-extra-$LLVM_VERSION.src.tar.xz
    mv clang-tools-extra-$LLVM_VERSION.src/ llvm-$LLVM_VERSION/tools/clang/tools/extra
    tar -xvf ../archives/compiler-rt-$LLVM_VERSION.src.tar.xz
    mv compiler-rt-$LLVM_VERSION.src/ llvm-$LLVM_VERSION/projects/compiler-rt
    pushd llvm-$LLVM_VERSION
    mkdir build && pushd build
    # activate swig
    export PATH=$DIR/build/swig-$SWIG_VERSION/install/bin:$PATH
    # influenced by: https://buildd.debian.org/status/fetch.php?pkg=llvm-toolchain-7&arch=amd64&ver=1%3A7.0.1%7E%2Brc2-1%7Eexp1&stamp=1541506173&raw=0
    cmake .. \
        -DCMAKE_C_COMPILER=$PREFIX/bin/gcc \
        -DCMAKE_CXX_COMPILER=$PREFIX/bin/g++ \
        -DCMAKE_CXX_LINK_FLAGS="-L$PREFIX/lib64 -Wl,-rpath,$PREFIX/lib64" \
        -DCMAKE_INSTALL_PREFIX=$PREFIX \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O2 -DNDEBUG" \
        -DCMAKE_CXX_FLAGS=' -fuse-ld=gold -fPIC -Wno-unused-command-line-argument -Wno-unknown-warning-option' \
        -DCMAKE_C_FLAGS=' -fuse-ld=gold -fPIC -Wno-unused-command-line-argument -Wno-unknown-warning-option' \
        -DLLVM_LINK_LLVM_DYLIB=ON \
        -DLLVM_INSTALL_UTILS=ON \
        -DLLVM_VERSION_SUFFIX= \
        -DLLVM_BUILD_LLVM_DYLIB=ON \
        -DLLVM_ENABLE_RTTI=ON \
        -DLLVM_ENABLE_FFI=ON \
        -DLLVM_BINUTILS_INCDIR=$PREFIX/include/ \
        -DLLVM_USE_PERF=yes
    make -j$CPUS
    make -j$CPUS check-clang # run clang test suite
    make -j$CPUS check-lld # run lld test suite
    make install
    popd && popd
fi

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
 - LLVM (Clang, LLD, compiler-rt, Clang tools extra) $LLVM_VERSION

## Required libraries

In order to be able to run all of these tools you should install the following
packages:

\`\`\`
$($DIR/../os/$DISTRO.sh list TOOLCHAIN_RUN_DEPS)
\`\`\`

## Usage

In order to use the toolchain you just have to source the activation script:

\`\`\`
source $PREFIX/activate
\`\`\`
EOF
fi

# create activation script
if [ ! -f $PREFIX/activate ]; then
    cat >$PREFIX/activate <<EOF
# This file must be used with "source $PREFIX/activate" *from bash*
# You can't run it directly!

# check for active virtual environments
if [ "\$( type -t deactivate )" != "" ]; then
    echo "You already have an active virtual environment!"
    return 0
fi

# check that we aren't root
if [ "\$USER" == "root" ]; then
    echo "You shouldn't use the toolchain as root!"
    return 0
fi

# save original environment
export ORIG_PATH=\$PATH
export ORIG_PS1=\$PS1
export ORIG_LD_LIBRARY_PATH=\$LD_LIBRARY_PATH

# activate new environment
export PATH=$PREFIX/bin:\$PATH
export PS1="($NAME) \$PS1"
export LD_LIBRARY_PATH=$PREFIX/lib:$PREFIX/lib64

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
    unset ORIG_PATH ORIG_PS1 ORIG_LD_LIBRARY_PATH
    unset -f su sudo deactivate
}
EOF
fi

# create toolchain archive
if [ ! -f $NAME-binaries-$DISTRO.tar.gz ]; then
    tar --owner=root --group=root -cpvzf $NAME-binaries-$DISTRO.tar.gz -C /opt $NAME
fi

# output final instructions
echo -e "\n\n"
echo "All tools have been built. They are installed in '$PREFIX'."
echo "In order to distribute the tools to someone else, an archive with the toolchain was created in the 'build' directory."
echo "If you want to install the packed tools you should execute the following command:"
echo
echo "    tar -xvzf build/$NAME-binaries.tar.gz -C /opt"
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
