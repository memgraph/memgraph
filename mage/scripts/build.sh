#!/bin/bash
set -eo pipefail

BUILD_TYPE="${1:-Release}"

source /opt/toolchain-v7/activate
curl https://sh.rustup.rs -sSf | sh -s -- -y
export PATH="$HOME/.cargo/bin:${PATH}"
rustup toolchain install 1.85
rustup default 1.85
mkdir -p $HOME/query_modules
python3 setup build -p $HOME/query_modules/ --cpp-build-flags CMAKE_BUILD_TYPE=${BUILD_TYPE}
