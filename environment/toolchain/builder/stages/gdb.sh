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
    # The runpath names this toolchain's own lib64 and lib, and not the
    # sysroot. GDB runs on the build host, under the host's dynamic linker, so
    # it has to use the host's libc; a runpath reaching into the sysroot made it
    # load the sysroot libc under the host loader instead, and the two have to
    # match. That left GDB unable to start at all. $ORIGIN rather than an
    # absolute path so the installed tree can still be moved.
    # Absolute, where the rest of the toolchain uses $ORIGIN. GDB's build is
    # recursive: this value crosses configure, a top Makefile, a sub-make and
    # the recipe shell, and each layer consumes an escaping level. Too few and
    # $O reads as a make variable, leaving a literal RIGIN; too many and the
    # shell expands $ORIGIN to nothing, leaving a path resolved against the
    # caller's working directory, which is worse than either. Rather than tune
    # the count to a recursion depth nobody controls, the path is spelled out.
    # It costs relocation for these three files, which the pass that makes the
    # whole tree relocatable has to rewrite anyway.
    #
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
            LDFLAGS="-Wl,-z,relro -Wl,-rpath,$PREFIX/lib64 -Wl,-rpath,$PREFIX/lib" \
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
            LDFLAGS="-Wl,-z,relro -Wl,-rpath,$PREFIX/lib64 -Wl,-rpath,$PREFIX/lib" \
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

    # Two of gdb's libraries are built only into the sysroot, and gdb is a host
    # tool. Naming the sysroot on the runpath would find them, but it also finds
    # the sysroot's libc, which is what stopped gdb starting at all. Link just
    # those two onto the host side instead: both are plain C libraries built
    # well below the host's glibc, so they load fine against it. The links are
    # relative, so the installed tree still moves.
    for _lib in libpython$PYTHON_MAJMIN.so.1.0 libxxhash.so.0; do
        if [[ -e "$SYSROOT/usr/lib/$_lib" && ! -e "$PREFIX/lib/$_lib" ]]; then
            ln -s "../sysroot/usr/lib/$_lib" "$PREFIX/lib/$_lib"
        fi
    done

    popd && popd
fi

popd
