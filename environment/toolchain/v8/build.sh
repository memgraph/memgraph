#!/bin/bash
set -euo pipefail

# helpers
pushd () { command pushd "$@" > /dev/null; }
popd () { command popd "$@" > /dev/null; }
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CPUS=$( grep -c processor < /proc/cpuinfo )
cd "$DIR"
source "$DIR/../../util.sh"
DISTRO="$(operating_system)"
# this will remove the minor version from rocky linuxAdd commentMore actions
if [[ "$DISTRO" =~ ^rocky-([0-9]+)\.[0-9]+$ ]]; then
    DISTRO="rocky-${BASH_REMATCH[1]}"
fi

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
TOOLCHAIN_VERSION=8
# package versions used
GCC_VERSION=16.2.0
BINUTILS_VERSION=2.47
GDB_VERSION=17.2
CMAKE_VERSION=4.4.1
CPPCHECK_VERSION=2.21.1
LLVM_VERSION=22.1.8
SWIG_VERSION=4.4.1 # used only for LLVM compilation
PCRE2_VERSION=10.47 # build-time dep of SWIG 4.4+ (hard requirement)
# Sysroot: pin glibc/kernel-headers so the toolchain produces binaries that run
# on any Linux with glibc >= GLIBC_VERSION and kernel >= 5.4.
# Versions of Linux headers and GLIBC are for the sysroot - these define the base compatibility for Memgraph's toolchain.
LINUX_HEADERS_VERSION=5.4.302
GLIBC_VERSION=2.31
# Sysroot support libraries: needed by GDB / cmake / mgconsole. Installed into
# $SYSROOT/usr so the toolchain GCC finds them via --with-sysroot.
ZLIB_VERSION=1.3.2
NCURSES_VERSION=6.6
OPENSSL_VERSION=3.6.3
CURL_VERSION=8.21.0
LIBFFI_VERSION=3.7.1
LIBIPT_VERSION=2.2 # Intel PT decoding for GDB btrace; x86_64 only
# Python lives in the sysroot solely so GDB can be built with scripting
# support and ship libpython3.so alongside the toolchain. memgraph's CMake
# ignores this Python via CMAKE_IGNORE_PATH in toolchain.cmake; consumers
# of the toolchain that need Python use the host's interpreter.
PYTHON_VERSION=3.12.7
# Major.minor only — referenced by GDB --with-python path and by memgraph's
# toolchain.cmake IGNORE_PATH entries. Update both when bumping above.
PYTHON_MAJMIN=3.12

# Define the archive tag. The toolchain bundles its own sysroot (glibc, kernel
# headers, runtime libs), so a single archive per architecture is portable
# across distros with glibc >= GLIBC_VERSION — the build distro no longer
# belongs in the name.
if [[ "$for_arm" = "true" ]]; then
    ARCHIVE_ARCH_TAG="aarch64"
else
    ARCHIVE_ARCH_TAG="x86_64"
fi
if [ "$TOOLCHAIN_STDCXX" = "libstdc++" ]; then
    echo "NOTE: Not adding anything to the archive name, GCC C++ standard lib is used to build libraries."
else
    echo "NOTE: Adding libc++ to the archive name, all libraries are built with LLVM standard C++ library."
    ARCHIVE_ARCH_TAG="$ARCHIVE_ARCH_TAG-libc++"
fi

# Set the right operating system setup script.
ENV_SCRIPT_RELATIVE="environment/os/$DISTRO.sh"
ENV_SCRIPT="$DIR/../../../$ENV_SCRIPT_RELATIVE"
echo "ALL BUILD PACKAGES: $(${ENV_SCRIPT} list TOOLCHAIN_BUILD_DEPS)"
${ENV_SCRIPT} check TOOLCHAIN_BUILD_DEPS
echo "ALL RUN PACKAGES: $(${ENV_SCRIPT} list TOOLCHAIN_RUN_DEPS)"
${ENV_SCRIPT} check TOOLCHAIN_RUN_DEPS

# check installation directory
NAME=toolchain-v$TOOLCHAIN_VERSION
PREFIX=/opt/$NAME
SYSROOT=$PREFIX/sysroot
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
# Host deps (apt): wget, gnupg, ca-certificates, git (LLVM/mgconsole/heaptrack
# clones), coreutils (sha*sum), tar, gzip, bzip2, xz-utils.
mkdir -p archives && pushd archives
# download all archives
if [ ! -f gcc-$GCC_VERSION.tar.gz ]; then
    # wget https://ftp.gnu.org/gnu/gcc/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz
    wget https://mirrorservice.org/sites/sourceware.org/pub/gcc/releases/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz
    wget https://mirrorservice.org/sites/sourceware.org/pub/gcc/releases/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz.sig
    gpg --keyserver keyserver.ubuntu.com --recv-keys 6C35B99309B5FA62 7F74F97C103468EE5D750B583AB00996FC26A641
    gpg --verify gcc-$GCC_VERSION.tar.gz.sig gcc-$GCC_VERSION.tar.gz
fi
if [ ! -f binutils-$BINUTILS_VERSION.tar.gz ]; then
    # wget https://ftp.gnu.org/gnu/binutils/binutils-$BINUTILS_VERSION.tar.gz
    wget https://sourceware.org/pub/binutils/releases/binutils-$BINUTILS_VERSION.tar.gz
    wget https://sourceware.org/pub/binutils/releases/binutils-$BINUTILS_VERSION.tar.gz.sig
    gpg --keyserver keyserver.ubuntu.com --recv-keys 3A24BC1E8FB409FA9F14371813FCEF89DD9E3C4F
    gpg --verify binutils-$BINUTILS_VERSION.tar.gz.sig binutils-$BINUTILS_VERSION.tar.gz
fi
if [ ! -f gdb-$GDB_VERSION.tar.gz ]; then
    # wget https://ftp.gnu.org/gnu/gdb/gdb-$GDB_VERSION.tar.gz
    wget https://sourceware.org/pub/gdb/releases/gdb-$GDB_VERSION.tar.gz
    wget https://sourceware.org/pub/gdb/releases/sha512.sum
    # sourceware's sha512.sum lists every gdb release. Feed only our line into
    # sha512sum -c — otherwise it exits non-zero on the missing-file entries
    # for the other releases and pipefail kills the script.
    grep " gdb-$GDB_VERSION.tar.gz\$" sha512.sum | sha512sum -c -
fi
if [ ! -f cmake-$CMAKE_VERSION.tar.gz ]; then
    wget https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION.tar.gz
    CMAKE_SHA256="95d4721f3625fb0d9d6ca480dd59a46c84b4c157f7fadd2e9b179ef9c871174d"
    echo "$CMAKE_SHA256  cmake-$CMAKE_VERSION.tar.gz" | sha256sum -c -
fi
if [ ! -f cppcheck-$CPPCHECK_VERSION.tar.gz ]; then
    wget https://github.com/cppcheck-opensource/cppcheck/archive/refs/tags/$CPPCHECK_VERSION.tar.gz -O cppcheck-$CPPCHECK_VERSION.tar.gz
    CPPCHECK_SHA256="ba750bd872ad7c01f951ff2d9dc8c68ea5852654545ec7a62a4c318d690c8e22"
    echo "$CPPCHECK_SHA256  cppcheck-$CPPCHECK_VERSION.tar.gz" | sha256sum -c -
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
if [ ! -f pcre2-$PCRE2_VERSION.tar.gz ]; then
    wget https://github.com/PCRE2Project/pcre2/releases/download/pcre2-$PCRE2_VERSION/pcre2-$PCRE2_VERSION.tar.gz
    PCRE2_SHA256="c08ae2388ef333e8403e670ad70c0a11f1eed021fd88308d7e02f596fcd9dc16"
    echo "$PCRE2_SHA256  pcre2-$PCRE2_VERSION.tar.gz" | sha256sum -c -
fi
if [ ! -f linux-$LINUX_HEADERS_VERSION.tar.xz ]; then
    wget https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-$LINUX_HEADERS_VERSION.tar.xz
    LINUX_HEADERS_SHA256="ae6a3207f12aa4d6cfb0fa793ec9da4a6fcdfdcb57d869d63d6b77e3a8c1423d"
    echo "$LINUX_HEADERS_SHA256  linux-$LINUX_HEADERS_VERSION.tar.xz" | sha256sum -c -
fi
if [ ! -f glibc-$GLIBC_VERSION.tar.xz ]; then
    wget https://ftp.gnu.org/gnu/glibc/glibc-$GLIBC_VERSION.tar.xz
    GLIBC_SHA256="9246fe44f68feeec8c666bb87973d590ce0137cca145df014c72ec95be9ffd17"
    echo "$GLIBC_SHA256  glibc-$GLIBC_VERSION.tar.xz" | sha256sum -c -
fi
if [ ! -f zlib-$ZLIB_VERSION.tar.gz ]; then
    wget https://zlib.net/zlib-$ZLIB_VERSION.tar.gz
    ZLIB_SHA256="bb329a0a2cd0274d05519d61c667c062e06990d72e125ee2dfa8de64f0119d16"
    echo "$ZLIB_SHA256  zlib-$ZLIB_VERSION.tar.gz" | sha256sum -c -
fi
if [ ! -f ncurses-$NCURSES_VERSION.tar.gz ]; then
    wget https://invisible-island.net/archives/ncurses/ncurses-$NCURSES_VERSION.tar.gz
    NCURSES_SHA256="355b4cbbed880b0381a04c46617b7656e362585d52e9cf84a67e2009b749ff11"
    echo "$NCURSES_SHA256  ncurses-$NCURSES_VERSION.tar.gz" | sha256sum -c -
fi
if [ ! -f curl-$CURL_VERSION.tar.gz ]; then
    wget https://curl.se/download/curl-$CURL_VERSION.tar.gz
    CURL_SHA256="d9b327997999045a24cda50f3983e69e51c516bd8be6ef9842fc7f99135e33bb"
    echo "$CURL_SHA256  curl-$CURL_VERSION.tar.gz" | sha256sum -c -
fi
if [ ! -f openssl-$OPENSSL_VERSION.tar.gz ]; then
    wget https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/openssl-$OPENSSL_VERSION.tar.gz
    OPENSSL_SHA256="243a86649cf6f23eeb6a2ff2456e09e5d77dd9018a54d3d96b0c6bdd6ba6c7f1"
    echo "$OPENSSL_SHA256  openssl-$OPENSSL_VERSION.tar.gz" | sha256sum -c -
fi
if [ ! -f libffi-$LIBFFI_VERSION.tar.gz ]; then
    wget https://github.com/libffi/libffi/releases/download/v$LIBFFI_VERSION/libffi-$LIBFFI_VERSION.tar.gz
    LIBFFI_SHA256="d5e9a6638ddbd2513ddb54518eb67e4bbe6fa707bcc01c10f6212f0a088d819d"
    echo "$LIBFFI_SHA256  libffi-$LIBFFI_VERSION.tar.gz" | sha256sum -c -
fi
if [ ! -f Python-$PYTHON_VERSION.tgz ]; then
    wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
    PYTHON_SHA256="73ac8fe780227bf371add8373c3079f42a0dc62deff8d612cd15a618082ab623"
    echo "$PYTHON_SHA256  Python-$PYTHON_VERSION.tgz" | sha256sum -c -
fi
if [ ! -f libipt-$LIBIPT_VERSION.tar.gz ]; then
    wget https://github.com/intel/libipt/archive/refs/tags/v$LIBIPT_VERSION.tar.gz -O libipt-$LIBIPT_VERSION.tar.gz
    LIBIPT_SHA256="f09a18fefba81d4fc2530d90858789e0c596f1b634e5777e6ccaf492966e9845"
    echo "$LIBIPT_SHA256  libipt-$LIBIPT_VERSION.tar.gz" | sha256sum -c -
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

# ----------------------------------------------------------------------------
# Sysroot: install Linux kernel headers and a pinned glibc into $SYSROOT so
# that GCC (and everything it builds) targets that glibc/kABI rather than the
# host system's. Kept reproducible across hosts at the cost of one glibc build.
# ----------------------------------------------------------------------------

# Host deps (apt): gcc, make (headers_install compiles scripts/unifdef), rsync.
log_tool_name "Linux kernel headers $LINUX_HEADERS_VERSION"
if [[ ! -d "$SYSROOT/usr/include/linux" ]]; then
    if [[ -d "linux-$LINUX_HEADERS_VERSION" ]]; then
        rm -rf linux-$LINUX_HEADERS_VERSION
    fi
    tar -xf ../archives/linux-$LINUX_HEADERS_VERSION.tar.xz
    pushd "linux-$LINUX_HEADERS_VERSION"
    if [[ "$for_arm" = true ]]; then
        kernel_arch=arm64
    else
        kernel_arch=x86_64
    fi
    make ARCH=$kernel_arch INSTALL_HDR_PATH=$SYSROOT/usr headers_install
    popd
fi

# Host deps (apt): gcc, g++, make (build-essential), gawk, bison, python3.
log_tool_name "glibc $GLIBC_VERSION"
if [[ ! -f "$SYSROOT/lib64/libc.so.6" && ! -f "$SYSROOT/lib/libc.so.6" ]]; then
    if [[ -d "glibc-$GLIBC_VERSION" ]]; then
        rm -rf glibc-$GLIBC_VERSION
    fi
    tar -xf ../archives/glibc-$GLIBC_VERSION.tar.xz
    pushd "glibc-$GLIBC_VERSION"
    # Force the C variant of support/links-dso-program. The C++ variant links
    # against the host's libstdc++, which on modern hosts depends on glibc
    # symbols newer than 2.31 (stat@GLIBC_2.33, pthread_create@GLIBC_2.34,
    # __isoc23_strtoul@GLIBC_2.38, ...) and fails to link against the
    # just-built libc.so.6. The C variant exercises the same dlopen machinery
    # without pulling in libstdc++.
    sed -i 's|^LINKS_DSO_PROGRAM = links-dso-program$|LINKS_DSO_PROGRAM = links-dso-program-c|' support/Makefile
    # misc/syslog.c calls syslog(INTERNALLOG, ...) recursively on invalid
    # priority bits. With recent host GCCs (>=12) the fortified inline of
    # syslog in <sys/syslog.h> is active when compiling syslog.c itself,
    # and the recursive call can't be inlined → hard `inlining failed in
    # call to always_inline 'syslog'` error. Route the recursion through
    # the internal __syslog symbol (same body, no fortified wrapper).
    sed -i 's|^\(\s*\)syslog *(INTERNALLOG,|\1__syslog(INTERNALLOG,|' misc/syslog.c
    mkdir build && pushd build
    if [[ "$for_arm" = true ]]; then
        glibc_target=aarch64-linux-gnu
    else
        glibc_target=x86_64-linux-gnu
    fi
    # Built with the host compiler (system gcc + binutils). --enable-kernel
    # drops glibc's compatibility code for kernels older than 5.4.
    # --disable-werror covers the spurious warnings glibc 2.31 emits under
    # newer host compilers.
    ../configure \
        --prefix=/usr \
        --build=$glibc_target \
        --host=$glibc_target \
        --with-headers=$SYSROOT/usr/include \
        --enable-kernel=5.4 \
        --disable-werror \
        --disable-profile \
        libc_cv_slibdir=/lib64
    make -j$CPUS
    make install DESTDIR=$SYSROOT
    popd && popd
fi

# Host deps (apt): build-essential, m4 (in-tree gmp), wget + bzip2
# (download_prerequisites fetches gmp/mpfr/mpc/isl as .tar.bz2).
log_tool_name "GCC $GCC_VERSION"
if [ ! -f "$PREFIX/bin/gcc" ]; then
    if [ -d "gcc-$GCC_VERSION" ]; then
        rm -rf gcc-$GCC_VERSION
    fi
    tar -xvf ../archives/gcc-$GCC_VERSION.tar.gz
    pushd "gcc-$GCC_VERSION"
    ./contrib/download_prerequisites
    mkdir build && pushd build
    # influenced by: https://buildd.debian.org/status/fetch.php?pkg=gcc-11&arch=arm64&ver=11.2.0-14&stamp=1642052446&raw=0
    if [[ "$for_arm" = true ]]; then
        ../configure -v \
            --prefix=$PREFIX \
            --with-sysroot=$SYSROOT \
            --with-build-sysroot=$SYSROOT \
            --with-glibc-version=$GLIBC_VERSION \
            --disable-multilib \
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
            --enable-libphobos-checking=release \
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
            --with-sysroot=$SYSROOT \
            --with-build-sysroot=$SYSROOT \
            --with-glibc-version=$GLIBC_VERSION \
            --disable-multilib \
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
# Pin CC/CXX so subsequent configure runs (gmp, mpfr, gdb, ...) don't fall back
# to the host /usr/bin/cc. Without this, autoconf prefers `cc` and we end up
# linking host-glibc symbols into libraries that should target the sysroot.
export CC=$PREFIX/bin/gcc
export CXX=$PREFIX/bin/g++
# Point pkg-config at the sysroot so cmake / configure scripts that use it
# (e.g. cmake's --system-curl) resolve to the sysroot's .pc files rather than
# the host's, which would otherwise drag in /usr/include and host-glibc deps.
# PKG_CONFIG_LIBDIR _replaces_ the default search path (no host fallback);
# PKG_CONFIG_SYSROOT_DIR rewrites the -I/-L paths in those .pc files.
export PKG_CONFIG_LIBDIR=$SYSROOT/usr/lib/pkgconfig:$SYSROOT/usr/lib64/pkgconfig:$SYSROOT/usr/share/pkgconfig
export PKG_CONFIG_SYSROOT_DIR=$SYSROOT

# Expose GCC's runtime libraries (libstdc++, libgcc_s) inside the sysroot.
# clang with --sysroot=$SYSROOT looks for -lstdc++ / -lgcc_s in $SYSROOT/usr/lib*
# but GCC installs them outside the sysroot at $PREFIX/lib64. Symlink them in
# (both shared .so and static .a — mgconsole uses -static-libstdc++) so the
# LLVM runtimes sub-build and other sysroot-aware C++ links resolve them
# without extra -L flags. Relative symlinks keep the toolchain relocatable.
log_tool_name "expose GCC libstdc++/libgcc_s/libatomic in sysroot"
if [[ ! -L "$SYSROOT/usr/lib64/libstdc++.so.6" ]]; then
    mkdir -p $SYSROOT/usr/lib64
    for lib in $PREFIX/lib64/libstdc++.so* $PREFIX/lib64/libstdc++.a \
               $PREFIX/lib64/libgcc_s.so* $PREFIX/lib64/libsupc++.a \
               $PREFIX/lib64/libatomic.so* $PREFIX/lib64/libatomic.a; do
        [[ -e "$lib" ]] || continue
        ln -sf "../../../lib64/$(basename "$lib")" "$SYSROOT/usr/lib64/$(basename "$lib")"
    done
fi

# Also expose the sysroot glibc's runtime libs (libm.so.6, libc.so.6, ...) in
# $SYSROOT/usr/lib64 and $SYSROOT/usr/lib. glibc installs them into
# $SYSROOT/lib64 (slibdir), but ld resolves transitive DT_NEEDED entries
# (e.g. libstdc++.so -> libm.so.6) ONLY via its built-in sysroot-prefixed
# SEARCH_DIRs, never via -L: that's "=/lib64" on x86-64 hosts' ld but only
# "=/lib" and "=/usr/lib" on Debian-family aarch64 ld. Cover both layouts.
if [[ ! -e "$SYSROOT/usr/lib/libm.so.6" ]]; then
    for lib in $SYSROOT/lib64/*.so*; do
        [[ -e "$lib" ]] || continue
        ln -sf "../../lib64/$(basename "$lib")" "$SYSROOT/usr/lib64/$(basename "$lib")"
        ln -sf "../../lib64/$(basename "$lib")" "$SYSROOT/usr/lib/$(basename "$lib")"
    done
fi

# NOTE: manually install gmp and mpfr (required by gdb)
# Host deps (apt): m4, make (compiler is the just-built toolchain gcc via CC).
log_tool_name "gmp (from gcc)"
if [ ! -f "$PREFIX/lib/libgmp.a" ]; then
    pushd $DIR/build/gcc-$GCC_VERSION/gmp

    if [[ "$for_arm" = true ]]; then
        gmp_build_host="--build=aarch64-linux-gnu --host=aarch64-linux-gnu"
    else
        gmp_build_host="--build=x86_64-linux-gnu --host=x86_64-linux-gnu"
    fi
    # gmp's configure has a K&R-style "long long reliability" test that
    # declares `void g(){}` and calls it with arguments. GCC 14+ defaults to
    # gnu23 where this is a hard error, so force C17 mode for the test
    # compile.
    CFLAGS="${CFLAGS:-} -std=gnu17" ./configure \
        $gmp_build_host \
        --prefix=$PREFIX

    make install
    popd
fi

# Host deps (apt): make (gmp comes from $PREFIX above).
log_tool_name "mpfr (from gcc)"
if [ ! -f "$PREFIX/lib/libmpfr.a" ]; then
    pushd $DIR/build/gcc-$GCC_VERSION/mpfr
    if [[ "$for_arm" = true ]]; then
        CFLAGS="${CFLAGS:-} -std=gnu17" ./configure \
            --build=aarch64-linux-gnu \
            --host=aarch64-linux-gnu \
            --prefix=$PREFIX \
            --with-gmp=$PREFIX
    else
        CFLAGS="${CFLAGS:-} -std=gnu17" ./configure \
            --build=x86_64-linux-gnu \
            --host=x86_64-linux-gnu \
            --prefix=$PREFIX \
            --with-gmp=$PREFIX
    fi
    make install
    popd
fi

# Host deps (apt): gcc, g++, make, bison (gprofng's parser); zlib is bundled.
log_tool_name "binutils $BINUTILS_VERSION"
if [ ! -f "$PREFIX/bin/ld" ]; then
    if [ -d "binutils-$BINUTILS_VERSION" ]; then
        rm -rf binutils-$BINUTILS_VERSION
    fi
    tar -xvf ../archives/binutils-$BINUTILS_VERSION.tar.gz
    pushd "binutils-$BINUTILS_VERSION"
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
                --with-sysroot=$SYSROOT \
                --enable-ld=default \
                --enable-gold \
                --enable-lto \
                --enable-plugins \
                --enable-shared \
                --enable-threads \
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
                --with-sysroot=$SYSROOT \
                --enable-ld=default \
                --enable-gold \
                --enable-lto \
                --enable-plugins \
                --enable-shared \
                --enable-threads \
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

# ----------------------------------------------------------------------------
# Sysroot support libraries: zlib, ncurses, openssl, libcurl. Built with the
# toolchain GCC so they target the sysroot's glibc; installed into
# $SYSROOT/usr so the toolchain GCC (and anything it links) find them by
# default. Consumed by GDB / cmake / mgconsole.
# ----------------------------------------------------------------------------

# Host deps (apt): make only — compiler is the toolchain gcc from here on.
log_tool_name "zlib $ZLIB_VERSION (sysroot)"
if [[ ! -f "$SYSROOT/usr/lib/libz.a" ]]; then
    if [[ -d "zlib-$ZLIB_VERSION" ]]; then
        rm -rf zlib-$ZLIB_VERSION
    fi
    tar -xzf ../archives/zlib-$ZLIB_VERSION.tar.gz
    pushd "zlib-$ZLIB_VERSION"
    ./configure --prefix=/usr --static
    make -j$CPUS
    make install DESTDIR=$SYSROOT
    popd
fi

# Host deps (apt): make.
log_tool_name "ncurses $NCURSES_VERSION (sysroot)"
if [[ ! -f "$SYSROOT/usr/lib/libncurses.a" ]]; then
    if [[ -d "ncurses-$NCURSES_VERSION" ]]; then
        rm -rf ncurses-$NCURSES_VERSION
    fi
    tar -xzf ../archives/ncurses-$NCURSES_VERSION.tar.gz
    pushd "ncurses-$NCURSES_VERSION"
    # Narrow (8-bit) ncurses with flat /usr/include header layout. The toolchain
    # tools (ccmake, GDB TUI) don't need wide-char support, and the flat layout
    # matches cmake's FindCurses default expectations.
    # LDFLAGS bakes a RUNPATH into libncurses.so.6 so it finds the sysroot's
    # libtinfo.so.6 (NEEDED) at runtime rather than falling back to whatever
    # libtinfo the host's ld.so.cache happens to have — older hosts (Ubuntu
    # 20.04 ships ncurses 6.2) lack symbols this ncurses 6.6 introduced
    # (_nc_tiparm, etc.), and Python's build-time module import test fails.
    LDFLAGS="-Wl,-rpath,$SYSROOT/usr/lib" \
    ./configure --prefix=/usr \
        --with-shared \
        --disable-widec \
        --without-debug \
        --without-ada \
        --without-tests \
        --without-manpages \
        --with-termlib
    make -j$CPUS
    make install DESTDIR=$SYSROOT
    popd
fi

# Host deps (apt): make, perl (openssl's Configure is a perl script).
log_tool_name "openssl $OPENSSL_VERSION (sysroot)"
if [[ ! -f "$SYSROOT/usr/lib64/libssl.a" && ! -f "$SYSROOT/usr/lib/libssl.a" ]]; then
    if [[ -d "openssl-$OPENSSL_VERSION" ]]; then
        rm -rf openssl-$OPENSSL_VERSION
    fi
    tar -xzf ../archives/openssl-$OPENSSL_VERSION.tar.gz
    pushd "openssl-$OPENSSL_VERSION"
    ./config --prefix=/usr \
        --openssldir=/usr/ssl \
        no-shared \
        no-dso \
        enable-ec_nistp_64_gcc_128 \
        enable-static-engine \
        enable-deprecated
    make -j$CPUS
    make install_sw DESTDIR=$SYSROOT
    popd
fi

# Host deps (apt): make, pkg-config (resolves sysroot OpenSSL/zlib .pc files
# via PKG_CONFIG_LIBDIR).
log_tool_name "curl $CURL_VERSION (sysroot)"
if [[ ! -f "$SYSROOT/usr/lib/libcurl.a" ]]; then
    if [[ -d "curl-$CURL_VERSION" ]]; then
        rm -rf curl-$CURL_VERSION
    fi
    tar -xzf ../archives/curl-$CURL_VERSION.tar.gz
    pushd "curl-$CURL_VERSION"
    ./configure --prefix=/usr \
        --enable-static \
        --disable-shared \
        --with-openssl \
        --with-zlib \
        --without-libssh2 \
        --without-libpsl \
        --without-nghttp2 \
        --without-libidn2 \
        --without-brotli \
        --without-zstd \
        --disable-ldap
    make -j$CPUS
    make install DESTDIR=$SYSROOT
    popd
fi

# Host deps (apt): make.
log_tool_name "libffi $LIBFFI_VERSION (sysroot)"
if [[ ! -f "$SYSROOT/usr/lib/libffi.a" && ! -f "$SYSROOT/usr/lib64/libffi.a" ]]; then
    if [[ -d "libffi-$LIBFFI_VERSION" ]]; then
        rm -rf libffi-$LIBFFI_VERSION
    fi
    tar -xzf ../archives/libffi-$LIBFFI_VERSION.tar.gz
    pushd "libffi-$LIBFFI_VERSION"
    # --with-pic is required because libffi.a gets linked into LLVM's shared
    # libraries (libLLVM.so etc.). Without it the link fails with
    # "requires dynamic R_X86_64_PC32 reloc ... may overflow at runtime".
    ./configure --prefix=/usr --libdir=/usr/lib --disable-shared --enable-static --with-pic
    make -j$CPUS
    make install DESTDIR=$SYSROOT
    popd
fi

# Host deps (apt): make, pkg-config — zlib/ncurses/openssl/libffi deps all come
# from the sysroot built above.
log_tool_name "Python $PYTHON_VERSION (sysroot)"
if [[ ! -f "$SYSROOT/usr/lib/libpython${PYTHON_MAJMIN}.so" ]]; then
    if [[ -d "Python-$PYTHON_VERSION" ]]; then
        rm -rf Python-$PYTHON_VERSION
    fi
    tar -xzf ../archives/Python-$PYTHON_VERSION.tgz
    pushd "Python-$PYTHON_VERSION"
    # Python is included solely so GDB can build with --with-python. We keep
    # the build small (skip pip, tests, profile-guided optimisation) and link
    # against sysroot openssl / libffi already installed above.
    # --enable-shared so GDB gets libpython3.X.so to dlopen.
    # rpath is hardcoded to the final install location rather than $ORIGIN:
    # python's autoconf→make→shell substitution chain eats every plausible
    # escape ($ORIGIN → empty make var; $$ORIGIN → empty shell var after make
    # collapses $$→$). The toolchain prefix is fixed at /opt/toolchain-v8
    # throughout this script, so the absolute path is stable and reliable.
    LDFLAGS="-Wl,-rpath,$SYSROOT/usr/lib" \
    ./configure --prefix=/usr \
        --enable-shared \
        --without-ensurepip \
        --disable-test-modules \
        --with-openssl="$SYSROOT/usr" \
        --with-system-ffi
    make -j$CPUS
    make install DESTDIR=$SYSROOT
    popd
fi

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

# Host deps (apt): make — built with the toolchain cmake/gcc from above.
# Intel PT decoding for GDB's btrace; x86-only (Intel PT is x86 hardware).
if [[ "$for_arm" = false ]]; then
    log_tool_name "libipt $LIBIPT_VERSION (sysroot)"
    if [[ ! -f "$SYSROOT/usr/lib/libipt.a" ]]; then
        if [[ -d "libipt-$LIBIPT_VERSION" ]]; then
            rm -rf libipt-$LIBIPT_VERSION
        fi
        tar -xzf ../archives/libipt-$LIBIPT_VERSION.tar.gz
        pushd "libipt-$LIBIPT_VERSION"
        mkdir build && pushd build
        # Static so GDB doesn't ship another .so; LIBDIR pinned because
        # GNUInstallDirs otherwise picks lib/lib64 based on the build host.
        # THREADS off: C11 mtx_* lives in libpthread until glibc 2.34, but
        # GDB's configure links plain -lipt (and decodes single-threaded).
        cmake .. \
            -DCMAKE_INSTALL_PREFIX=/usr \
            -DCMAKE_INSTALL_LIBDIR=lib \
            -DCMAKE_BUILD_TYPE=Release \
            -DBUILD_SHARED_LIBS=OFF \
            -DFEATURE_THREADS=OFF \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        make -j$CPUS
        make install DESTDIR=$SYSROOT
        popd && popd
    fi
fi

# Host deps (apt): make — gmp/mpfr come from $PREFIX, python/ncurses/zlib and
# (x86) libipt from the sysroot; expat/lzma/babeltrace are disabled below.
log_tool_name "GDB $GDB_VERSION"
if [[ ! -f "$PREFIX/bin/gdb" ]]; then
    if [[ -d "gdb-$GDB_VERSION" ]]; then
        rm -rf gdb-$GDB_VERSION
    fi
    tar -xvf ../archives/gdb-$GDB_VERSION.tar.gz
    pushd "gdb-$GDB_VERSION"
    mkdir build && pushd build
    # GDB is built sysroot-aware via the toolchain GCC. --with-python points
    # at the libpython we installed into the sysroot above. Features that
    # require libraries not in the sysroot (expat, lzma, babeltrace,
    # system readline) are disabled — GDB falls back to its bundled
    # readline and skips the niche subsystems. TUI works because ncurses is
    # in the sysroot; on x86 intel-pt btrace works via the sysroot libipt
    # (ARM stays --without-intel-pt: Intel PT is x86-only hardware).
    if [[ "$for_arm" = true ]]; then
        # https://buildd.debian.org/status/fetch.php?pkg=gdb&arch=arm64&ver=10.1-2&stamp=1614889767&raw=0
        env \
            CC=$PREFIX/bin/gcc \
            CXX=$PREFIX/bin/g++ \
            CFLAGS="-g -O2 -fstack-protector-strong -Wformat -Werror=format-security" \
            CXXFLAGS="-g -O2 -fstack-protector-strong -Wformat -Werror=format-security" \
            CPPFLAGS="-Wdate-time -D_FORTIFY_SOURCE=2 -fPIC" \
            LDFLAGS="-Wl,-z,relro -Wl,-rpath,$PREFIX/sysroot/usr/lib" \
            ../configure \
                --build=aarch64-linux-gnu \
                --host=aarch64-linux-gnu \
                --prefix=$PREFIX \
                --with-gmp=$PREFIX \
                --with-mpfr=$PREFIX \
                --disable-maintainer-mode \
                --disable-dependency-tracking \
                --disable-silent-rules \
                --disable-gdbtk \
                --disable-shared \
                --without-guile \
                --with-system-gdbinit=$PREFIX/etc/gdb/gdbinit \
                --without-expat \
                --without-lzma \
                --without-babeltrace \
                --without-intel-pt \
                --enable-tui \
                --with-python=$SYSROOT/usr/bin/python$PYTHON_MAJMIN
    else
        # https://buildd.debian.org/status/fetch.php?pkg=gdb&arch=amd64&ver=8.2.1-2&stamp=1550831554&raw=0
        env \
            CC=$PREFIX/bin/gcc \
            CXX=$PREFIX/bin/g++ \
            CFLAGS="-g -O2 -fstack-protector-strong -Wformat -Werror=format-security" \
            CXXFLAGS="-g -O2 -fstack-protector-strong -Wformat -Werror=format-security" \
            CPPFLAGS="-Wdate-time -D_FORTIFY_SOURCE=2 -fPIC" \
            LDFLAGS="-Wl,-z,relro -Wl,-rpath,$PREFIX/sysroot/usr/lib" \
            ../configure \
                --build=x86_64-linux-gnu \
                --host=x86_64-linux-gnu \
                --prefix=$PREFIX \
                --with-gmp=$PREFIX \
                --with-mpfr=$PREFIX \
                --disable-maintainer-mode \
                --disable-dependency-tracking \
                --disable-silent-rules \
                --disable-gdbtk \
                --disable-shared \
                --without-guile \
                --with-system-gdbinit=$PREFIX/etc/gdb/gdbinit \
                --without-expat \
                --without-lzma \
                --without-babeltrace \
                --with-intel-pt \
                --enable-tui \
                --with-python=$SYSROOT/usr/bin/python$PYTHON_MAJMIN
    fi
    make -j$CPUS
    make install
    popd && popd
fi

# Host deps (apt): unzip.
log_tool_name "install pahole"
if [[ ! -d "$PREFIX/share/pahole-gdb" ]]; then
    unzip ../archives/pahole-gdb-master.zip
    mv pahole-gdb-master $PREFIX/share/pahole-gdb
fi

log_tool_name "setup system gdbinit"
if [[ ! -f "$PREFIX/etc/gdb/gdbinit" ]]; then
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

# Host deps (apt): make ("gcc"/"g++" resolve to the toolchain via PATH).
log_tool_name "cppcheck $CPPCHECK_VERSION"
if [[ ! -f "$PREFIX/bin/cppcheck" ]]; then
    if [[ -d "cppcheck-$CPPCHECK_VERSION" ]]; then
        rm -rf cppcheck-$CPPCHECK_VERSION
    fi
    tar -xvf ../archives/cppcheck-$CPPCHECK_VERSION.tar.gz
    pushd "cppcheck-$CPPCHECK_VERSION"
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

# Host deps (apt): autoconf (via automake), automake, libtool, bison, make —
# PCRE2 is built from our own pinned tarball, not libpcre2-dev.
log_tool_name "swig $SWIG_VERSION"
if [[ ! -d "swig-$SWIG_VERSION/install" ]]; then
    if [[ -d swig-$SWIG_VERSION ]]; then
        rm -rf swig-$SWIG_VERSION
    fi
    tar -xvf ../archives/swig-$SWIG_VERSION.tar.gz
    pushd "swig-$SWIG_VERSION"
    ./autogen.sh
    mkdir build && pushd build
    # SWIG 4.4 has a hard PCRE2 build-time dependency. Tools/pcre-build.sh
    # expects a pcre2-*.tar* in the directory configure runs from and stages
    # a static PCRE2 in pcre/pcre-swig-install, which configure auto-detects.
    cp ../../../archives/pcre2-$PCRE2_VERSION.tar.gz .
    ../Tools/pcre-build.sh
    ../configure --prefix=$DIR/build/swig-$SWIG_VERSION/install
    make -j$CPUS
    make install
    popd && popd
fi

# Host deps (apt): make, python3 — cmake/gcc/binutils come from $PREFIX, swig
# from the stage above, zlib/libffi from the sysroot (FIND_ROOT_PATH=ONLY).
log_tool_name "LLVM $LLVM_VERSION"
if [[ ! -f "$PREFIX/bin/clang" ]]; then
    if [[ -d llvmorg-$LLVM_VERSION ]]; then
        rm -rf llvmorg-$LLVM_VERSION
    fi
    cp -r ../archives/llvmorg-$LLVM_VERSION ./llvmorg-$LLVM_VERSION

    # NOTE: Go under llvmorg-$LLVM_VERSION/llvm/CMakeLists.txt to see all
    #       options, docs pages are not up to date.
    # compiler-rt and openmp moved out of LLVM_ENABLE_PROJECTS (deprecated as
    # projects since LLVM 16+, fatal error in future releases) — they're built
    # as runtimes by the just-built clang, not the host gcc.
    TOOLCHAIN_LLVM_ENABLE_PROJECTS="clang;clang-tools-extra;lldb;lld"
    TOOLCHAIN_LLVM_ENABLE_RUNTIMES="libunwind;compiler-rt;openmp"
    if [[ "$TOOLCHAIN_STDCXX" = "libc++" ]]; then
        TOOLCHAIN_LLVM_ENABLE_RUNTIMES="$TOOLCHAIN_LLVM_ENABLE_RUNTIMES;libcxx;libcxxabi"
    fi

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
    cmake -S llvm -B build -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX="$PREFIX" \
        -DCMAKE_SYSROOT="$SYSROOT" \
        -DLLVM_DEFAULT_TARGET_TRIPLE="$TOOLCHAIN_LLVM_TARGET_TRIPLE" \
        -DCMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY \
        -DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY \
        -DCMAKE_FIND_ROOT_PATH_MODE_PACKAGE=ONLY \
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
        -DRUNTIMES_CMAKE_ARGS="-DCMAKE_C_FLAGS=--gcc-toolchain=$PREFIX;-DCMAKE_CXX_FLAGS=--gcc-toolchain=$PREFIX;-DLIBOMP_OMPD_SUPPORT=OFF;-DLIBOMP_OMPD_GDB_SUPPORT=OFF" \
        -DBUILTINS_CMAKE_ARGS="-DCMAKE_C_FLAGS=--gcc-toolchain=$PREFIX;-DCMAKE_CXX_FLAGS=--gcc-toolchain=$PREFIX" \
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
fi

popd

# create README
if [[ ! -f "$PREFIX/README.md" ]]; then
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

# create activation script from template
if [[ ! -f "$PREFIX/activate" ]]; then
    sed -e "s|@NAME@|$NAME|g" \
        -e "s|@TOOLCHAIN_VERSION@|$TOOLCHAIN_VERSION|g" \
        "$DIR/activate.in" > "$PREFIX/activate"
fi

pushd build
source $PREFIX/activate

export CC=$PREFIX/bin/clang
export CXX=$PREFIX/bin/clang++
export CFLAGS="${CFLAGS:-} -fPIC"
if [ "$TOOLCHAIN_STDCXX" = "libstdc++" ]; then
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
log_tool_name "mgconsole $MGCONSOLE_TAG"
if [[ ! -f "$PREFIX/bin/mgconsole" ]]; then
    if [[ -d mgconsole ]]; then
      rm -rf mgconsole
    fi
    git clone https://github.com/memgraph/mgconsole.git mgconsole
    pushd mgconsole
    git checkout $MGCONSOLE_TAG
    # mgconsole builds mgclient as an ExternalProject, which does NOT inherit
    # CMAKE_SYSROOT / CMAKE_FIND_ROOT_PATH_MODE_* from this top-level cmake
    # call. Without a hint, mgclient's find_package(OpenSSL) finds the host's
    # /usr/include/openssl/ssl.h (where SSL_get_peer_certificate is a real
    # function under the 1.1 ABI), then links against the sysroot's OpenSSL
    # 3.x libs (where it was renamed SSL_get1_peer_certificate) and fails to
    # resolve. OPENSSL_ROOT_DIR is consulted by FindOpenSSL via the env, so
    # it crosses the parent→ExternalProject boundary cleanly.
    OPENSSL_ROOT_DIR="$SYSROOT/usr" \
    cmake -B build $COMMON_CMAKE_FLAGS
    OPENSSL_ROOT_DIR="$SYSROOT/usr" \
    cmake --build build -j$CPUS --target mgconsole install
    popd
fi

# Host deps (apt): git, make, libdw-dev (also pulls libelf-dev for
# libebl.a/libelf.a), libboost-filesystem-dev, libboost-program-options-dev,
# libboost-iostreams-dev, libboost-system-dev, and the static compression
# archives the link line below names: zlib1g-dev, libbz2-dev, liblzma-dev,
# libzstd-dev.
HEAPTRACK_TAG="v1.5.0"
log_tool_name "heaptrack $HEAPTRACK_TAG"
if [[ ! -f "$PREFIX/bin/heaptrack" ]]; then
    if [[ -d heaptrack ]]; then
        rm -rf heaptrack
    fi
    git clone https://github.com/KDE/heaptrack.git heaptrack
    pushd heaptrack
    git checkout $HEAPTRACK_TAG
    # heaptrack links against the HOST's boost/libdw (the sysroot doesn't carry
    # them), so the sysroot-pinning CXXFLAGS/LDFLAGS are cleared below. All of
    # boost, libdw and libstdc++ are linked statically, leaving only glibc
    # dynamic — the builder's glibc (2.31 on ubuntu-20.04) matches the sysroot
    # pin, so heaptrack runs anywhere the toolchain itself does.
    HOST_LIBDIR="/usr/lib/$(uname -m)-linux-gnu"
    # Elfutils 0.176 (20.04) predates the libebl->libdw merge (0.178), so
    # libebl.a must be linked explicitly, and it needs libdl for its runtime
    # backend dlopen. Missing backends on target machines degrade gracefully:
    # symbolization works via the default hooks (verified on bare 20.04/12).
    # Boost's cmake config links its compression deps as bare "-lz -lbz2 ..."
    # flags, which resolve to shared libs. Point the linker at a directory
    # holding only the static archives so those flags resolve statically.
    mkdir -p static-libs
    ln -sf "$HOST_LIBDIR"/lib{z,bz2,lzma,zstd}.a static-libs/
    env CFLAGS="" CXXFLAGS="" LDFLAGS="" \
    cmake -B build \
        -DCMAKE_C_COMPILER=$CC \
        -DCMAKE_CXX_COMPILER=$CXX \
        -DCMAKE_C_FLAGS="--gcc-toolchain=$PREFIX" \
        -DCMAKE_CXX_FLAGS="--gcc-toolchain=$PREFIX" \
        -DCMAKE_BUILD_TYPE=Release \
        -DHEAPTRACK_BUILD_GUI=OFF \
        -DHEAPTRACK_USE_LIBUNWIND=OFF \
        -DCMAKE_INSTALL_PREFIX=$PREFIX \
        -DBoost_USE_STATIC_LIBS=ON \
        -DZLIB_USE_STATIC_LIBS=ON \
        -DZSTD_LIBRARY=$HOST_LIBDIR/libzstd.a \
        -DLIBDW_LIBRARIES="$HOST_LIBDIR/libdw.a;$HOST_LIBDIR/libebl.a;$HOST_LIBDIR/libelf.a;$HOST_LIBDIR/libz.a;$HOST_LIBDIR/liblzma.a;$HOST_LIBDIR/libbz2.a;$HOST_LIBDIR/libdl.so" \
        -DLIBDW_INCLUDE_DIR=/usr/include \
        -DCMAKE_EXE_LINKER_FLAGS="--gcc-toolchain=$PREFIX -static-libstdc++ -static-libgcc -L$PWD/static-libs" \
        -DCMAKE_MODULE_LINKER_FLAGS="--gcc-toolchain=$PREFIX -static-libstdc++ -static-libgcc -Wl,--exclude-libs,ALL" \
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -Wno-dev
    cmake --build build -j$CPUS
    cmake --build build --target install
    popd
fi

popd

# copy toolchain.cmake to the prefix
cp -v $DIR/toolchain.cmake $PREFIX/

# NOTE: It's important/clean (e.g., easier upload to S3) to have a separated
# folder to the output archive.
mkdir -p output
pushd output
# Create the toolchain archive.
if [[ ! -f "$NAME-binaries-$ARCHIVE_ARCH_TAG.tar.gz" ]]; then
    tar --owner=root --group=root -cpvzf "$NAME-binaries-$ARCHIVE_ARCH_TAG.tar.gz" -C /opt $NAME
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
