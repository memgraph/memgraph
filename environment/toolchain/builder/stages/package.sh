#!/bin/bash
# package: name the toolchain, write its README, and archive it.
#
# This is the only stage that knows which version is being cut. Everything
# before it builds one unnamed tree at a fixed prefix, so bumping the version
# rebuilds this stage and not the thirty-three before it. Renaming the tree is
# safe because the relocate stage leaves every runpath $ORIGIN-relative and
# checks that nothing generated names the prefix.
#
# The activation script is written earlier, by the activate stage, because the
# stages that build against the finished toolchain source it.
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/toolchain.env"
# the README lists every tool it ships, so it needs their versions
source "$TC_VERSIONS/gcc.env"
source "$TC_VERSIONS/binutils.env"
source "$TC_VERSIONS/gdb.env"
source "$TC_VERSIONS/cmake.env"
source "$TC_VERSIONS/cppcheck.env"
source "$TC_VERSIONS/llvm.env"
source "$TC_VERSIONS/mold.env"
source "$TC_VERSIONS/dwz.env"
source "$TC_VERSIONS/libabigail.env"
source "$TC_VERSIONS/heaptrack.env"
source "$TC_VERSIONS/mgconsole.env"

# copy toolchain.cmake to the prefix
cp -v $DIR/toolchain.cmake $PREFIX/

NAME=toolchain-v$TOOLCHAIN_VERSION
RELEASE=/opt/$NAME
mv "$PREFIX" "$RELEASE"

cat >$RELEASE/README.md <<EOF
# Memgraph Toolchain v$TOOLCHAIN_VERSION

## Included tools

 - GCC $GCC_VERSION
 - Binutils $BINUTILS_VERSION
 - GDB $GDB_VERSION
 - CMake $CMAKE_VERSION
 - Cppcheck $CPPCHECK_VERSION
 - LLVM $LLVM_VERSION
   - projects: $LLVM_PROJECTS
   - runtimes: $(mg_llvm_runtimes)
 - mold $MOLD_VERSION
 - dwz $DWZ_VERSION
 - libabigail $LIBABIGAIL_VERSION
 - heaptrack $HEAPTRACK_TAG
 - mgconsole $MGCONSOLE_TAG
 - pahole-gdb (pinned by checksum)

## Required libraries

In order to be able to run all of these tools you should install the following
packages:
\`\`\`
./$ENV_SCRIPT_RELATIVE list TOOLCHAIN_RUN_DEPS
\`\`\`
by executing:
\`\`\`
./$ENV_SCRIPT_RELATIVE install TOOLCHAIN_RUN_DEPS
\`\`\`

## Usage

In order to use the toolchain you just have to source the activation script:

\`\`\`
source $RELEASE/activate
\`\`\`

On the other hand, \`deactivate\` will get back your original setup by restoring
the initial environment variables.
EOF

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
echo "Then:         source $RELEASE/activate"
