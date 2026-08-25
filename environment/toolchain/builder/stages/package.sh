#!/bin/bash
# package: place the cmake toolchain file and archive the prefix.
#
# The README and the activation script are written earlier, by the activate
# stage, because the stages that build against the finished toolchain need
# them before this point.
set -euo pipefail
source /tc/lib/common.sh

# copy toolchain.cmake to the prefix
cp -v $DIR/toolchain.cmake $PREFIX/

# The archive is the shipped artifact, so it is built deterministically: file
# order sorted rather than readdir order, timestamps and ownership fixed, and
# the atime/ctime pax headers dropped. gzip is invoked separately because tar's
# -z gives no way to pass -n, and without it the gzip header records a name and
# a timestamp of its own.
mkdir -p "$TC_OUTPUT"
pushd "$TC_OUTPUT"
tar --sort=name \
    --mtime="@${SOURCE_DATE_EPOCH}" \
    --owner=0 --group=0 --numeric-owner \
    --pax-option=exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime \
    -cpf - -C /opt $NAME \
  | gzip -6 -n > "$NAME-binaries-$ARCHIVE_ARCH_TAG.tar.gz"
popd

echo "Archive: $TC_OUTPUT/$NAME-binaries-$ARCHIVE_ARCH_TAG.tar.gz"
echo "Install with: tar -xvzf $NAME-binaries-$ARCHIVE_ARCH_TAG.tar.gz -C /opt"
echo "Then:         source $PREFIX/activate"
