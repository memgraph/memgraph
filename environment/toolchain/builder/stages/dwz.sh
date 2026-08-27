#!/bin/bash
# dwz: DWARF deduplication.
#
# Compression shrinks debug sections and split DWARF moves them out of the
# binary; neither removes the duplication itself. dwz rewrites duplicate
# debugging information across compilation units into a shared form, which is
# the shape of the problem when the same library's debug info is repeated
# across hundreds of test binaries.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/dwz.env"

fetch https://sourceware.org/pub/dwz/releases/dwz-$DWZ_VERSION.tar.xz "$DWZ_SHA256"

pushd "$TC_BUILD"
# Host deps: make. libelf comes from the sysroot.
log_tool_name "dwz $DWZ_VERSION"
# the tarball unpacks to plain "dwz", not "dwz-$DWZ_VERSION"
tar -xJf ../archives/dwz-$DWZ_VERSION.tar.xz
pushd dwz
# dwz has no configure; it picks up CC and flags from the environment,
# which common.sh has already pointed at the toolchain compiler.
make -j$CPUS \
    CFLAGS="-O2 -g --sysroot=$SYSROOT" \
    LDFLAGS="--sysroot=$SYSROOT"
make install prefix=$PREFIX
popd

popd
