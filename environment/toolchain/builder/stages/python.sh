#!/bin/bash
# python: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/python.env"

fetch https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz "$PYTHON_SHA256"

# Host deps (apt): make, pkg-config — zlib/ncurses/openssl/libffi deps all come
# from the sysroot built above.
log_tool_name "Python $PYTHON_VERSION (sysroot)"
enter_source Python-$PYTHON_VERSION.tgz Python-$PYTHON_VERSION
# Python is included solely so GDB can build with --with-python. We keep
# the build small (skip pip, tests, profile-guided optimisation) and link
# against sysroot openssl / libffi already installed above.
# --enable-shared so GDB gets libpython3.X.so to dlopen.
# rpath is hardcoded to the final install location rather than $ORIGIN:
# python's autoconf→make→shell substitution chain eats every plausible
# escape ($ORIGIN → empty make var; $$ORIGIN → empty shell var after make
# collapses $$→$). An absolute path is safe here because the prefix is
# fixed while the toolchain is being built, and the relocate stage rewrites
# every runpath under the prefix to an $ORIGIN-relative one afterwards.
LDFLAGS="-Wl,-rpath,$SYSROOT/usr/lib" \
./configure --prefix=/usr \
    --enable-shared \
    --without-ensurepip \
    --disable-test-modules \
    --with-openssl="$SYSROOT/usr" \
    --with-system-ffi
make -j$CPUS
make install DESTDIR=$SYSROOT
leave_source
