#!/bin/bash
# NOTE: run this inside the build container
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_DIR="$( cd "$SCRIPT_DIR/../.." && pwd )"
CONAN_REMOTE=${CONAN_REMOTE:-""}

# move into the memgraph directory and attempt to generate
cd $SOURCE_DIR/cpp/memgraph

function cleanup() {
    exit_code=$?
    cd $SOURCE_DIR
    rm -rf $SOURCE_DIR/cpp/memgraph/env || true
    rm -rf $SOURCE_DIR/cpp/memgraph/build || true
    exit $exit_code
}

trap cleanup ERR EXIT

python3 -m venv env
source env/bin/activate
pip install conan

if [[ ! -f "$HOME/.conan2/profiles/default" ]]; then
  conan profile detect
fi

conan config install conan_config

if [[ -n "$CONAN_REMOTE" ]]; then
  conan remote add artifactory $CONAN_REMOTE --force
fi

# This is not ideal, because it means that we need to have the cache populated with all the dependencies
# or build the dependencies before generating the SBOM.
BUILD_TYPE=Release
MG_TOOLCHAIN_ROOT=/opt/toolchain-v7 conan install \
  . \
  --build=missing \
  -pr:h memgraph_template_profile \
  -pr:b memgraph_build_profile \
  -s build_type="$BUILD_TYPE" \
  -s:a os=Linux \
  -s:a os.distro="ubuntu-24.04"

./tools/sbom/build-sbom.sh
