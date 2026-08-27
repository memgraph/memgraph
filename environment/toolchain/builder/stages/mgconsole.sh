#!/bin/bash
set -euo pipefail
source /tc/lib/common.sh
source "$TC_VERSIONS/mgconsole.env"
source /tc/lib/clang-env.sh

pushd "$TC_BUILD"
# Host deps (apt): git, make — OpenSSL comes from the sysroot.
log_tool_name "mgconsole $MGCONSOLE_TAG"
git clone https://github.com/memgraph/mgconsole.git mgconsole
pushd mgconsole
git checkout $MGCONSOLE_TAG
require_commit "$MGCONSOLE_COMMIT"
# mgconsole builds mgclient as an ExternalProject, which does NOT inherit
# CMAKE_SYSROOT / CMAKE_FIND_ROOT_PATH_MODE_* from this top-level cmake
# call. Without a hint, mgclient's find_package(OpenSSL) finds the host's
# /usr/include/openssl/ssl.h (where SSL_get_peer_certificate is a real
# function under the 1.1 ABI), then links against the sysroot's OpenSSL
# 3.x libs (where it was renamed SSL_get1_peer_certificate) and fails to
# resolve. OPENSSL_ROOT_DIR is consulted by FindOpenSSL via the env, so
# it crosses the parent→ExternalProject boundary cleanly.
OPENSSL_ROOT_DIR="$SYSROOT/usr" \
cmake -B build $COMMON_CMAKE_FLAGS
OPENSSL_ROOT_DIR="$SYSROOT/usr" \
cmake --build build -j$CPUS --target mgconsole install
popd

# Host deps (apt): git, make, libdw-dev (also pulls libelf-dev for
# libebl.a/libelf.a), libboost-filesystem-dev, libboost-program-options-dev,
# libboost-iostreams-dev, libboost-system-dev, and the static compression
# archives the link line below names: zlib1g-dev, libbz2-dev, liblzma-dev,
# libzstd-dev.
popd
