#!/bin/bash
# zlib: build commands moved verbatim from the v8 build script. Only the
# orchestration around them is new -- edit the recipe here, not its shape.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/zlib.env"

fetch https://zlib.net/zlib-$ZLIB_VERSION.tar.gz "$ZLIB_SHA256"

# Host deps (apt): make only — compiler is the toolchain gcc from here on.
log_tool_name "zlib $ZLIB_VERSION (sysroot)"
enter_source zlib-$ZLIB_VERSION.tar.gz zlib-$ZLIB_VERSION
./configure --prefix=/usr --static
make -j$CPUS
make install DESTDIR=$SYSROOT
leave_source
