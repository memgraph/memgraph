#!/bin/bash
# Generate the SBOM of the new MAGE image and combines it with that of memgraph

set -euo pipefail
IMAGE_NAME=$1

function cleanup() {
    exit_code=$?
    rm -rf sbom/env || true
    rm -rf syft.tar.gz || true
    rm -rf syft || true
    rm -rf cyclonedx || true
    rm -rf sbom/docker-sbom.json || true
    rm -rf sbom/memgraph-sbom.json || true
    exit $exit_code
}
trap cleanup ERR EXIT

# download syft binary from GitHub Releases
if [[ "$(arch)" == "x86_64" ]]; then
    SYFTURL="https://github.com/anchore/syft/releases/download/v1.38.0/syft_1.38.0_linux_amd64.tar.gz"
else
    SYFTURL="https://github.com/anchore/syft/releases/download/v1.38.0/syft_1.38.0_linux_arm64.tar.gz"
fi

curl -L -o syft.tar.gz "$SYFTURL"
tar -xzf syft.tar.gz syft
chmod +x syft
SYFT_FILE_METADATA_SELECTION=none ./syft \
  scan docker:${IMAGE_NAME} -o cyclonedx-json > sbom/docker-sbom.json

# download cyclonedx
if [[ "$(arch)" == "x86_64" ]]; then
    CYCLONEDXURL="https://github.com/CycloneDX/cyclonedx-cli/releases/download/v0.29.1/cyclonedx-linux-x64"
else
    CYCLONEDXURL="https://github.com/CycloneDX/cyclonedx-cli/releases/download/v0.29.1/cyclonedx-linux-arm64"
fi

curl -L -o cyclonedx "$CYCLONEDXURL"
chmod +x cyclonedx

./cyclonedx merge --input-files \
  sbom/docker-sbom.json \
  sbom/memgraph-build-sbom.json \
  --output-format json \
  --output-file sbom/mage-sbom.json
echo "Generated SBOM file: sbom/mage-sbom.json"
rm sbom/memgraph-build-sbom.json

python3 -m venv sbom/env
source sbom/env/bin/activate
pip install rich==13.9.4

python3 cpp/memgraph/tools/sbom/sbom-formatter.py sbom/mage-sbom.json
echo "Generated SBOM file: sbom/mage-sbom.txt"
