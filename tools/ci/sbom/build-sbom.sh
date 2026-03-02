#!/bin/bash
# Generates the SBOM JSON and human-readable table for the Memgraph binary
# This is intended to be ran from within the root of the Memgraph repository,
# either after the build (in which case `conan install` will have already been
# run) or without the build (in this case we need $CONAN_REMOTE to be set).

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_DIR="$( cd "$SCRIPT_DIR/../../.." && pwd )"
CONAN_REMOTE=${CONAN_REMOTE:-""}

no_cleanup=false
while [[ $# -gt 0 ]]; do
  case $1 in
    --no-cleanup)
      no_cleanup=true
      shift
    ;;
    *)
      echo "Error: Unknown argument '$1'" >&2
      exit 1
    ;;
  esac
done

# check that the build has generated the sbom file
if [[ ! -f "$SOURCE_DIR/build/generators/sbom/memgraph-sbom.cdx.json" && -z "$CONAN_REMOTE" ]]; then
  echo "Error: sbom file not found - run conan install first" >&2
  exit 1
elif [[ ! -f "$SOURCE_DIR/build/generators/sbom/memgraph-sbom.cdx.json" && -n "$CONAN_REMOTE" ]]; then
  cd $SOURCE_DIR
  python3 -m venv env
  source env/bin/activate
  pip install "conan>=2.26.0"

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
  deactivate
  rm -rf env || true
fi

function cleanup() {
  exit_status=$?
  if [[ "$no_cleanup" == false ]]; then
    rm -rf tools/ci/sbom/env || true
    rm -f cyclonedx || true
  fi
  exit $exit_status
}

trap cleanup EXIT

# download cyclonedx
if [[ "$(arch)" == "x86_64" ]]; then
    CYCLONEDXURL="https://github.com/CycloneDX/cyclonedx-cli/releases/download/v0.29.1/cyclonedx-linux-x64"
else
    CYCLONEDXURL="https://github.com/CycloneDX/cyclonedx-cli/releases/download/v0.29.1/cyclonedx-linux-arm64"
fi

curl -L -o cyclonedx "$CYCLONEDXURL"
chmod +x cyclonedx

# combine SBOMs
mkdir -p sbom
./cyclonedx merge --input-files \
  build/generators/sbom/memgraph-sbom.cdx.json \
  libs/sbom.json \
  --output-format json \
  --output-file sbom/memgraph-build-sbom.json
echo "Generated SBOM file: sbom/memgraph-build-sbom.json"

# create venv for generating human-readable SBOM table
python3 -m venv tools/ci/sbom/env
source tools/ci/sbom/env/bin/activate
pip install rich==13.9.4

# generate human-readable SBOM table
python3 tools/ci/sbom/sbom-formatter.py sbom/memgraph-build-sbom.json
echo "Generated human-readable SBOM table: sbom/memgraph-build-sbom.txt"
