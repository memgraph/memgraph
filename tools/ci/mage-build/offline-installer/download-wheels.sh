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

# Pinned PyG/DGL versions — shared between the S3 fetch and the `pip download`
# pass below so we never end up with two versions of the same package in the
# cache (e.g. pip preferring a newer PyPI wheel and `pip install` later
# picking that over the S3 one we tested with).
#
# IMPORTANT: keep these in sync with the URLs in
# mage/install_python_requirements.sh. The custom PyG/DGL wheels are hosted in
# our S3 bucket (not on PyPI), so both scripts have to agree on which version
# to fetch — there's no upstream registry to derive a single answer from. Any
# version bump here must be mirrored there and vice versa.
TORCH_CLUSTER_VERSION="1.6.3"
TORCH_GEOMETRIC_VERSION="2.8.0"
TORCH_SCATTER_VERSION="2.1.2"
TORCH_SPARSE_VERSION="0.6.18"
TORCH_SPLINE_CONV_VERSION="1.2.2"
DGL_VERSION="2.5"

# Pull S3 wheels down first so subsequent `pip download` passes can resolve
# their transitive deps too (e.g. torch-geometric needs pyparsing).
S3_WHEELS=(
  "torch_cluster-${TORCH_CLUSTER_VERSION}-cp312-cp312-${TORCH_TAG}.whl"
  "torch_geometric-${TORCH_GEOMETRIC_VERSION}-py3-none-any.whl"
  "torch_scatter-${TORCH_SCATTER_VERSION}-cp312-cp312-${TORCH_TAG}.whl"
  "torch_sparse-${TORCH_SPARSE_VERSION}-cp312-cp312-${TORCH_TAG}.whl"
  "torch_spline_conv-${TORCH_SPLINE_CONV_VERSION}-cp312-cp312-${TORCH_TAG}.whl"
  "dgl-${DGL_VERSION}-cp312-cp312-${DGL_TAG}.whl"
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
# the cache. They're already in $OUTPUT_DIR from the curl loop above; the
# version pins here guarantee pip picks our exact S3 wheel rather than
# downloading a newer PyPI wheel, which would leave two versions in the
# cache and let `pip install` later choose the wrong one.
python3 -m pip download \
  "${PIP_FLAGS[@]}" \
  "torch-cluster==${TORCH_CLUSTER_VERSION}" \
  "torch-geometric==${TORCH_GEOMETRIC_VERSION}" \
  "torch-scatter==${TORCH_SCATTER_VERSION}" \
  "torch-sparse==${TORCH_SPARSE_VERSION}" \
  "torch-spline-conv==${TORCH_SPLINE_CONV_VERSION}" \
  "dgl==${DGL_VERSION}"

echo "Downloaded $(ls "$OUTPUT_DIR" | wc -l) wheels to $OUTPUT_DIR"
