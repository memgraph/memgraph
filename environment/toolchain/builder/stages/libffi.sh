#!/bin/bash
# libffi: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/libffi.env"

pushd "$TC_ARCHIVES"
if [[ ! -f libffi-$LIBFFI_VERSION.tar.gz ]]; then
    wget --https-only https://github.com/libffi/libffi/releases/download/v$LIBFFI_VERSION/libffi-$LIBFFI_VERSION.tar.gz
    echo "$LIBFFI_SHA256  libffi-$LIBFFI_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps (apt): make.
log_tool_name "libffi $LIBFFI_VERSION (sysroot)"
tar -xzf ../archives/libffi-$LIBFFI_VERSION.tar.gz
pushd "libffi-$LIBFFI_VERSION"
# --with-pic is required because libffi.a gets linked into LLVM's shared
# libraries (libLLVM.so etc.). Without it the link fails with
# "requires dynamic R_X86_64_PC32 reloc ... may overflow at runtime".
./configure --prefix=/usr --libdir=/usr/lib --disable-shared --enable-static --with-pic
make -j$CPUS
make install DESTDIR=$SYSROOT
popd

popd
