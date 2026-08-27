#!/bin/bash
# ncurses: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/ncurses.env"

pushd "$TC_ARCHIVES"
if [[ ! -f ncurses-$NCURSES_VERSION.tar.gz ]]; then
    wget --https-only https://invisible-island.net/archives/ncurses/ncurses-$NCURSES_VERSION.tar.gz
    echo "$NCURSES_SHA256  ncurses-$NCURSES_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make.
log_tool_name "ncurses $NCURSES_VERSION (sysroot)"
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

popd
