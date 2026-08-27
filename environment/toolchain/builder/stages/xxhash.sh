#!/bin/bash
# xxhash: in the sysroot, for dwz.
#
# dwz has needed it since 0.16 and includes <xxhash.h> unconditionally; the
# only choice its build offers is header-only inlining versus linking against
# the library, and both want the header. Installing the static library as well
# means either path works.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/xxhash.env"

pushd "$TC_ARCHIVES"
if [[ ! -f xxhash-$XXHASH_VERSION.tar.gz ]]; then
    wget --https-only https://github.com/Cyan4973/xxHash/archive/refs/tags/v$XXHASH_VERSION.tar.gz -O xxhash-$XXHASH_VERSION.tar.gz
    echo "$XXHASH_SHA256  xxhash-$XXHASH_VERSION.tar.gz" | sha256sum -c -
fi
popd

pushd "$TC_BUILD"
# Host deps: make.
log_tool_name "xxhash $XXHASH_VERSION (sysroot)"
# the tarball unpacks to xxHash-$VERSION, capitalised differently
tar -xzf ../archives/xxhash-$XXHASH_VERSION.tar.gz
pushd "xxHash-$XXHASH_VERSION"
make -j$CPUS libxxhash.a
make install PREFIX=/usr LIBDIR=/usr/lib DESTDIR=$SYSROOT
popd

popd
