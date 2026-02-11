#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$SCRIPT_DIR/../../"
MG_BUILD_DIR="$ROOT_DIR/build"

# Use MGCONSOLE_TAG from environment if set, otherwise default to v1.5.0
MGCONSOLE_TAG="${MGCONSOLE_TAG:-v1.5.0}"

mkdir -pv build/mgconsole
cd build/mgconsole

git clone https://github.com/memgraph/mgconsole.git
cd mgconsole
git checkout $MGCONSOLE_TAG
cmake -B build -GNinja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$MG_BUILD_DIR/mgconsole .
cmake --build build -j$(nproc)
cmake --install build
