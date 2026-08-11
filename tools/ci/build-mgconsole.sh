#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$SCRIPT_DIR/../../"
MG_BUILD_DIR="$ROOT_DIR/build"

# Use MGCONSOLE_TAG from environment if set, otherwise default to v1.7.0
MGCONSOLE_TAG="${MGCONSOLE_TAG:-v1.7.0}"

# Build against the toolchain sysroot (glibc 2.31): a host-glibc build
# breaks the packaged /usr/bin/mgconsole on older distros (e.g. debian-12:
# `GLIBC_2.38' not found`). The toolchain gcc is configured --with-sysroot,
# so pinning CC/CXX targets the sysroot everywhere — including mgconsole's
# ExternalProjects (gflags, mgclient), which don't inherit CMake toolchain
# settings but do inherit the environment. Same approach as the toolchain's
# own mgconsole recipe (environment/toolchain/v8/build.sh).
TOOLCHAIN_ROOT="${MG_TOOLCHAIN_ROOT:-}"
if [[ -z "$TOOLCHAIN_ROOT" ]]; then
  # Toolchain not activated (e.g. bare ./package.sh); newest installed wins.
  TOOLCHAIN_ROOT="$(ls -d /opt/toolchain-* 2>/dev/null | sort -V | tail -1 || true)"
fi
if [[ -n "$TOOLCHAIN_ROOT" && -x "$TOOLCHAIN_ROOT/bin/gcc" ]]; then
  echo "Building mgconsole against the toolchain sysroot: $TOOLCHAIN_ROOT"
  export CC="$TOOLCHAIN_ROOT/bin/gcc"
  export CXX="$TOOLCHAIN_ROOT/bin/g++"
  # mgclient's find_package(OpenSSL) must also stay inside the sysroot.
  export OPENSSL_ROOT_DIR="$TOOLCHAIN_ROOT/sysroot/usr"
else
  echo "WARNING: no toolchain found; mgconsole will link the host glibc and" >&2
  echo "         may not run on older distros." >&2
fi

mkdir -pv build/mgconsole
cd build/mgconsole

git clone https://github.com/memgraph/mgconsole.git
cd mgconsole
git checkout $MGCONSOLE_TAG
cmake -B build -GNinja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$MG_BUILD_DIR/mgconsole .
cmake --build build -j$(nproc)
# --strip: the sysroot gcc build carries debug info (~21MB -> ~8MB).
cmake --install build --strip
