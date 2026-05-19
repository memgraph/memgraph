#!/bin/bash
set -euo pipefail
# Run me inside a clean ubuntu:24.04 container as root.
# Downloads every Python wheel needed by MAGE + the memgraph auth module into
# /output/wheels/. Mirrors the wheel-selection logic in
# mage/install_python_requirements.sh.
#
# Inputs (env vars):
#   ARCH         amd64 or arm64 (default amd64)
#   CUDA         true|false (default false; CUDA only valid for amd64)
#   CUDA_VERSION CUDA version for the S3 wheel path (default 13.0)
#
# Inputs (bind-mounted):
#   /input/requirements.txt              — mage's requirements file
#                                          (gpu file when CUDA=true, else cpu)
#   /input/auth-module-requirements.txt — src/auth/reference_modules/requirements.txt
#   /input/wheels-prebuilt/             — wheels already produced on the host
#                                         (gssapi etc., copied through verbatim)
# Output:
#   /output/wheels/*.whl

OUTPUT_DIR=${OUTPUT_DIR:-/output/wheels}
INPUT_DIR=${INPUT_DIR:-/input}
ARCH=${ARCH:-amd64}
CUDA=${CUDA:-false}
CUDA_VERSION=${CUDA_VERSION:-13.0}

case "$ARCH" in
  amd64|arm64) ;;
  *) echo "Error: ARCH must be amd64 or arm64 (got $ARCH)" >&2; exit 1 ;;
esac
if [[ "$CUDA" == "true" && "$ARCH" != "amd64" ]]; then
  echo "Error: CUDA wheels are only published for amd64 (got ARCH=$ARCH)" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  python3 python3-pip curl ca-certificates

export PIP_BREAK_SYSTEM_PACKAGES=1

# Copy prebuilt host wheels first (gssapi etc.). They must land in
# $OUTPUT_DIR before `pip download` runs — pip checks the dest dir for an
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

# S3 wheel paths and filename suffixes mirror mage/install_python_requirements.sh:
#   arm64                 -> wheels/arm64/                   + linux_aarch64 tag
#   amd64 + CUDA=false    -> wheels/amd64/                   + linux_x86_64 tag
#   amd64 + CUDA=true     -> wheels/cuda-${CUDA_VERSION}/    + linux_x86_64 tag
BASE_URL="https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/wheels"
if [[ "$ARCH" == "arm64" ]]; then
  BASE_URL="${BASE_URL}/arm64"
  TORCH_TAG="linux_aarch64"
  DGL_TAG="linux_aarch64"
elif [[ "$CUDA" == "true" ]]; then
  BASE_URL="${BASE_URL}/cuda-${CUDA_VERSION}"
  TORCH_TAG="linux_x86_64"
  DGL_TAG="linux_x86_64"
else
  BASE_URL="${BASE_URL}/amd64"
  TORCH_TAG="linux_x86_64"
  DGL_TAG="linux_x86_64"
fi

# Pull S3 wheels down first so subsequent `pip download` passes can resolve
# their transitive deps too (e.g. torch-geometric needs pyparsing).
S3_WHEELS=(
  "torch_cluster-1.6.3-cp312-cp312-${TORCH_TAG}.whl"
  "torch_geometric-2.8.0-py3-none-any.whl"
  "torch_scatter-2.1.2-cp312-cp312-${TORCH_TAG}.whl"
  "torch_sparse-0.6.18-cp312-cp312-${TORCH_TAG}.whl"
  "torch_spline_conv-1.2.2-cp312-cp312-${TORCH_TAG}.whl"
  "dgl-2.5-cp312-cp312-${DGL_TAG}.whl"
)
for wheel in "${S3_WHEELS[@]}"; do
  echo "Fetching $BASE_URL/$wheel"
  curl -fsSL -o "$OUTPUT_DIR/$wheel" "$BASE_URL/$wheel"
done

# --prefer-binary biases pip toward wheels over sdists so we don't bundle
# anything that would need a compiler at install time on the target.
# --find-links=$OUTPUT_DIR makes pip *resolve* against wheels we've already
# placed there (e.g. the prebuilt gssapi + S3 PyG wheels). Without it, pip
# only looks at $OUTPUT_DIR to detect "already downloaded" by exact filename
# — which fails for gssapi because PyPI ships only an sdist
# (gssapi-1.11.1.tar.gz) whereas our prebuilt is
# gssapi-1.11.1-cp311-abi3-linux_x86_64.whl, and pip would otherwise grab the
# sdist and try to build it (krb5-config missing).
PIP_FLAGS=(--dest "$OUTPUT_DIR" --find-links "$OUTPUT_DIR" --prefer-binary)

# Resolve mage's runtime requirements + transitive deps.
python3 -m pip download \
  "${PIP_FLAGS[@]}" \
  --requirement "$INPUT_DIR/requirements.txt"

# Resolve auth module requirements + transitive deps. Some overlap with the
# above is fine — pip download just rewrites identical filenames.
python3 -m pip download \
  "${PIP_FLAGS[@]}" \
  --requirement "$INPUT_DIR/auth-module-requirements.txt"

# Explicitly resolve the PyG/DGL extras now so their transitive deps land in
# the cache. They're already in $OUTPUT_DIR from the curl loop above, so pip
# picks those wheels via --find-links and only downloads anything missing
# (pyparsing for torch-geometric, etc.).
python3 -m pip download \
  "${PIP_FLAGS[@]}" \
  torch-cluster torch-geometric torch-scatter torch-sparse torch-spline-conv dgl

echo "Downloaded $(ls "$OUTPUT_DIR" | wc -l) wheels to $OUTPUT_DIR"
