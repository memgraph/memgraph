#!/bin/bash
# python: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/python.env"

pushd "$TC_ARCHIVES"
if [[ ! -f Python-$PYTHON_VERSION.tgz ]]; then
    wget --https-only https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
    PYTHON_SHA256="73ac8fe780227bf371add8373c3079f42a0dc62deff8d612cd15a618082ab623"
    echo "$PYTHON_SHA256  Python-$PYTHON_VERSION.tgz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
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

popd
