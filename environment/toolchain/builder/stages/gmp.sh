#!/bin/bash
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/gmp.env"
source "$TC_VERSIONS/gcc.env"

pushd "$TC_BUILD"
# NOTE: manually install gmp and mpfr (required by gdb)
# Host deps (apt): m4, make (compiler is the just-built toolchain gcc via CC).
log_tool_name "gmp (from gcc)"
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

popd
