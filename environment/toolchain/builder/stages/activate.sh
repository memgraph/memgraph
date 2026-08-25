#!/bin/bash
# activate: write the toolchain's README and its activation script.
#
# This runs before the stages that build against the finished toolchain,
# because clang-env.sh sources $PREFIX/activate and they source that. In the
# original script the same ordering was implicit in the line order.
set -euo pipefail
source /tc/lib/common.sh
# the generated README lists every tool it ships, so it needs their versions
source "$TC_VERSIONS/gcc.env"
source "$TC_VERSIONS/binutils.env"
source "$TC_VERSIONS/gdb.env"
source "$TC_VERSIONS/cmake.env"
source "$TC_VERSIONS/cppcheck.env"
source "$TC_VERSIONS/llvm.env"

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
