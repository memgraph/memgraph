#!/bin/bash
set -eo pipefail

BUILD_TYPE="${1:-Release}"
RUST_VERSION="${2:-1.85}"

source /opt/toolchain-v7/activate
curl https://sh.rustup.rs -sSf | sh -s -- -y
export PATH="$HOME/.cargo/bin:${PATH}"
rustup toolchain install $RUST_VERSION
rustup default $RUST_VERSION
mkdir -p $HOME/query_modules
python3 setup build -p $HOME/query_modules/ --cpp-build-flags CMAKE_BUILD_TYPE=${BUILD_TYPE}
