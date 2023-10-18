#!/bin/bash -ex
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PREFIX=/opt/toolchain-v5

# NOTE: Often times when versions in the build script are changes, something
# doesn't work. To avoid rebuild of the whole toolchain but rebuild specific
# lib from 0, just comment specific line under this cript and run it. Don't
# forget to comment back to avoid unnecessary deletes next time your run this
# cript.

# rm -rf "$DIR/build"
# rm -rf "$DIR/output"

# rm -rf "$PREFIX/bin/gcc"
# rm -rf "$PREFIX/bin/ld.gold"
# rm -rf "$PREFIX/bin/gdb"
# rm -rf "$PREFIX/bin/cmake"
# rm -rf "$PREFIX/bin/clang"
# rm -rf "$PREFIX/include/bzlib.h"
# rm -rf "$PREFIX/include/fmt"
# rm -rf "$PREFIX/include/lz4.h"
# rm -rf "$PREFIX/include/lzma.h"
# rm -rf "$PREFIX/include/zlib.h"
# rm -rf "$PREFIX/include/zstd.h"
# rm -rf "$PREFIX/include/jemalloc"
# rm -rf "$PREFIX/include/boost"
# rm -rf "$PREFIX/include/double-conversion"
# rm -rf "$PREFIX/include/gflags"
# rm -rf "$PREFIX/include/libunwind.h"
# rm -rf "$PREFIX/include/glog"
# rm -rf "$PREFIX/include/event2"
# rm -rf "$PREFIX/include/sodium.h"
# rm -rf "$PREFIX/include/libaio.h"
# rm -rf "$PREFIX/include/FlexLexer.h"
# rm -rf "$PREFIX/include/snappy.h"
# rm -rf "$PREFIX/include/fizz"
# rm -rf "$PREFIX/include/folly"
# rm -rf "$PREFIX/include/proxygen"
# rm -rf "$PREFIX/include/wangle"
# rm -rf "$PREFIX/include/thrift"

# rm -rf "$PREFIX"
