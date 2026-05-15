#!/bin/bash
set -euo pipefail
# Run me inside a clean ubuntu:24.04 container as root.
# Downloads every Python wheel needed by MAGE + the memgraph auth module into
# /output/wheels/. amd64 / CPU-only.
#
# Inputs (bind-mounted into the container):
#   /input/requirements.txt           — mage/python/requirements.txt
#   /input/auth-module-requirements.txt — src/auth/reference_modules/requirements.txt
#   /input/wheels-prebuilt/           — wheels already produced on the host
#                                       (gssapi etc., copied through verbatim)
# Output:
#   /output/wheels/*.whl
#
# Mirrors the CPU/amd64 branch of mage/install_python_requirements.sh — same
# torch / torch_geometric / dgl wheels from S3, same auth-module requirements.

OUTPUT_DIR=${OUTPUT_DIR:-/output/wheels}
INPUT_DIR=${INPUT_DIR:-/input}

mkdir -p "$OUTPUT_DIR"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  python3 python3-pip curl ca-certificates

export PIP_BREAK_SYSTEM_PACKAGES=1

# Copy any prebuilt wheels supplied by the host (gssapi is built by the
# build-gssapi mgbuild subcommand and lives in mage/wheels/). They must land
# in $OUTPUT_DIR *before* pip download runs — pip checks the dest dir for an
# existing wheel that satisfies the requirement and skips fetching it, which
# is how we avoid pip trying to build gssapi from its sdist (it has no PyPI
# wheel and the build needs krb5-config + libkrb5-dev).
if [[ -d "$INPUT_DIR/wheels-prebuilt" ]]; then
  shopt -s nullglob
  for whl in "$INPUT_DIR/wheels-prebuilt"/*.whl; do
    cp -v "$whl" "$OUTPUT_DIR/"
  done
  shopt -u nullglob
fi

# --prefer-binary biases pip toward wheels over sdists so we don't bundle
# anything that would need a compiler at install time on the target.
PIP_FLAGS=(--dest "$OUTPUT_DIR" --prefer-binary)

# Resolve mage's runtime requirements + transitive deps.
python3 -m pip download \
  "${PIP_FLAGS[@]}" \
  --requirement "$INPUT_DIR/requirements.txt"

# Resolve auth module requirements + transitive deps. Some overlap with the
# above is fine — pip download just rewrites identical filenames.
python3 -m pip download \
  "${PIP_FLAGS[@]}" \
  --requirement "$INPUT_DIR/auth-module-requirements.txt"

# Special wheels hosted in our S3 bucket (PyG ecosystem + DGL). install_python_
# requirements.sh fetches these as a fallback layer after the requirements.txt
# pass — we bundle them directly so the offline target never reaches out.
BASE_URL="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/wheels/amd64"
S3_WHEELS=(
  "torch_cluster-1.6.3-cp312-cp312-linux_x86_64.whl"
  "torch_geometric-2.8.0-py3-none-any.whl"
  "torch_scatter-2.1.2-cp312-cp312-linux_x86_64.whl"
  "torch_sparse-0.6.18-cp312-cp312-linux_x86_64.whl"
  "torch_spline_conv-1.2.2-cp312-cp312-linux_x86_64.whl"
  "dgl-2.5-cp312-cp312-linux_x86_64.whl"
)
for wheel in "${S3_WHEELS[@]}"; do
  echo "Fetching $wheel"
  curl -fsSL -o "$OUTPUT_DIR/$wheel" "$BASE_URL/$wheel"
done

echo "Downloaded $(ls "$OUTPUT_DIR" | wc -l) wheels to $OUTPUT_DIR"
