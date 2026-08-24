#!/bin/bash
# gdb: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/gdb.env"
source "$TC_VERSIONS/python.env"

pushd "$TC_ARCHIVES"
if [[ ! -f gdb-$GDB_VERSION.tar.gz ]]; then
    wget --https-only https://sourceware.org/pub/gdb/releases/gdb-$GDB_VERSION.tar.gz
    wget --https-only https://sourceware.org/pub/gdb/releases/sha512.sum
    # sourceware's sha512.sum lists every gdb release. Feed only our line into
    # sha512sum -c — otherwise it exits non-zero on the missing-file entries
    # for the other releases and pipefail kills the script.
    grep " gdb-$GDB_VERSION.tar.gz\$" sha512.sum | sha512sum -c -
fi
popd

pushd "$TC_BUILD"
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

popd
