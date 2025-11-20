#!/bin/bash
# Generates the SBOM JSON and human-readable table for the Memgraph binary

set -euo pipefail

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
if [[ ! -f "build/generators/sbom/memgraph-sbom.cdx.json" ]]; then
    echo "Error: sbom file not found - run conan install first" >&2
    exit 1
fi

function cleanup() {
    exit_status=$?
    if [[ "$no_cleanup" == false ]]; then
        rm -rf tools/sbom/env || true
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
python3 -m venv tools/sbom/env
source tools/sbom/env/bin/activate
pip install rich==13.9.4

# generate human-readable SBOM table
python3 tools/sbom/sbom-formatter.py sbom/memgraph-build-sbom.json
echo "Generated human-readable SBOM table: sbom/memgraph-build-sbom.txt"
