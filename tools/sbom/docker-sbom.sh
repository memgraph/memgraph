#!/bin/bash
# This will run the build-sbom.sh script to obtain the SBOM for the Memgraph binary
# then it will scan the Docker container and combine the reports.

set -euo pipefail

TAG=$1

function cleanup() {
    rm -rf tools/sbom/env || true
    rm -f syft
    rm -f syft.tar.gz
    rm -r cyclonedx
}

trap cleanup EXIT

# run the build-sbom.sh script to obtain the SBOM for the Memgraph binary
./tools/sbom/build-sbom.sh --no-cleanup

# download syft binary from GitHub Releases
if [[ "$(arch)" == "x86_64" ]]; then
    SYFTURL="https://github.com/anchore/syft/releases/download/v1.38.0/syft_1.38.0_linux_amd64.tar.gz"
else
    SYFTURL="https://github.com/anchore/syft/releases/download/v1.38.0/syft_1.38.0_linux_arm64.tar.gz"
fi

curl -L -o syft.tar.gz "$SYFTURL"
tar -xzf syft.tar.gz syft
chmod +x syft

# scan the Docker container and combine the reports
SYFT_FILE_METADATA_SELECTION=none ./syft \
  scan docker:memgraph/memgraph:${TAG} -o cyclonedx-json > sbom/docker-sbom.json

# combine the reports
./cyclonedx merge --input-files \
  sbom/memgraph-build-sbom.json \
  sbom/docker-sbom.json \
  --output-format json \
  --output-file sbom/memgraph-docker-sbom.json

# generate human-readable SBOM table
python3 tools/sbom/sbom-formatter.py sbom/memgraph-docker-sbom.json
