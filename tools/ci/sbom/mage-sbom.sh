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
    rm -rf sbom/rust-mage-sbom-files || true
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

SBOM_FILES=(
  sbom/docker-sbom.json
  sbom/memgraph-build-sbom.json
)

# collect Rust MAGE Cargo.toml files
cargo install --locked --version 0.5.9 cargo-cyclonedx
mapfile -t RUST_MAGE_CARGO_MANIFEST_FILES < <(find mage/rust -name "Cargo.toml" -type f -print)
mkdir -p sbom/rust-mage-sbom-files
for cargo_manifest_file in "${RUST_MAGE_CARGO_MANIFEST_FILES[@]}"; do
  ITEM_NAME=$(basename "$(dirname "$cargo_manifest_file")")
  OUTPUT_FILE="sbom/rust-mage-sbom-files/${ITEM_NAME}.json"
  GENERATED_FILE="$(dirname "$cargo_manifest_file")/${ITEM_NAME}.json"
  echo "file: $cargo_manifest_file, output: $ITEM_NAME"
  cargo cyclonedx --format json --override-filename "$ITEM_NAME" --manifest-path "$cargo_manifest_file" --no-build-deps
  mv "$GENERATED_FILE" "$OUTPUT_FILE"
  SBOM_FILES+=("$OUTPUT_FILE")
  echo "Generated SBOM file: $OUTPUT_FILE"
done

./cyclonedx merge --input-files \
  "${SBOM_FILES[@]}" \
  --output-format json \
  --output-file sbom/mage-sbom.json
echo "Generated SBOM file: sbom/mage-sbom.json"
rm sbom/memgraph-build-sbom.json
rm -rf sbom/rust-mage-sbom-files

python3 -m venv sbom/env
source sbom/env/bin/activate
pip install rich==13.9.4

python3 tools/ci/sbom/sbom-formatter.py sbom/mage-sbom.json
echo "Generated SBOM file: sbom/mage-sbom.txt"
