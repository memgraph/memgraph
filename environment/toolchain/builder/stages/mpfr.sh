#!/bin/bash
# mpfr: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/mpfr.env"
source "$TC_VERSIONS/gcc.env"

pushd "$TC_BUILD"
# Host deps (apt): make (gmp comes from $PREFIX above).
log_tool_name "mpfr (from gcc)"
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

popd
