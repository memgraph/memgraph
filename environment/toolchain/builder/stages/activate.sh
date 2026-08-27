#!/bin/bash
# activate: place the toolchain's activation script.
#
# This runs before the stages that build against the finished toolchain,
# because clang-env.sh sources $PREFIX/activate and they source that. In the
# original script the same ordering was implicit in the line order.
#
# The script is copied rather than filled in: it works out the toolchain's
# location, name and version from where it finds itself. So this stage does not
# need to know which version is being built, which is what stops a version bump
# from rebuilding it and everything after it. The README does name the version,
# and is written by the packaging stage instead.
set -euo pipefail
source /tc/lib/common.sh

pushd "$TC_BUILD"
cp "$DIR/activate" "$PREFIX/activate"
popd
