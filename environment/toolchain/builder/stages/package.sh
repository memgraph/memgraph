#!/bin/bash
# package: assemble the README, the activation script and the toolchain cmake
# file, then archive the prefix. The README and activate blocks are moved
# verbatim from the v8 build script; the archive command is not, see below.
set -euo pipefail
source /tc/lib/common.sh

pushd "$TC_BUILD"
# create README
if [[ ! -f "$PREFIX/README.md" ]]; then
    cat >$PREFIX/README.md <<EOF
# Memgraph Toolchain v$TOOLCHAIN_VERSION

## Included tools

 - GCC $GCC_VERSION
 - Binutils $BINUTILS_VERSION
 - GDB $GDB_VERSION
 - CMake $CMAKE_VERSION
 - Cppcheck $CPPCHECK_VERSION
 - LLVM (clang;clang-tools-extra;compiler-rt;libunwind;lldb[;libcxx;libcxxabi]) $LLVM_VERSION

## Required libraries

In order to be able to run all of these tools you should install the following
packages:
\`\`\`
./$ENV_SCRIPT_RELATIVE list TOOLCHAIN_RUN_DEPS)
\`\`\`
by executing:
\`\`\`
./$ENV_SCRIPT_RELATIVE install TOOLCHAIN_RUN_DEPS)
\`\`\`

## Usage

In order to use the toolchain you just have to source the activation script:

\`\`\`
source $PREFIX/activate
\`\`\`

On the other hand, \`deactivate\` will get back your original setup by restoring
the initial environment variables.
EOF
fi

# create activation script from template
if [[ ! -f "$PREFIX/activate" ]]; then
    sed -e "s|@NAME@|$NAME|g" \
        -e "s|@TOOLCHAIN_VERSION@|$TOOLCHAIN_VERSION|g" \
        "$DIR/activate.in" > "$PREFIX/activate"
fi
popd

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
