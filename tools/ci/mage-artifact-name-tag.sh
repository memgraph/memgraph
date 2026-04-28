#!/bin/bash
set -euo pipefail
# generate the MAGE artifact name and Docker tag

MEMGRAPH_VERSION=$1
ARCH=$2
BUILD_TYPE=$3
MALLOC=$4
CUDA=$5
CUGRAPH=$6
# Default to "prod" so callers that haven't been updated still produce
# a customer-shaped artifact name (no -relwithdebinfo suffix).
PACKAGE_FLAVOUR=${7:-prod}

IMAGE_TAG="${MEMGRAPH_VERSION}"
IMAGE_TAG="${IMAGE_TAG//+/_}"
IMAGE_TAG="${IMAGE_TAG//\~/_}"
ARTIFACT_NAME="mage-${IMAGE_TAG}"

# modify artifacts/tags for specific builds
if [[ "$ARCH" == 'arm' ]]; then
    ARTIFACT_NAME="${ARTIFACT_NAME}-arm64"
fi
# The -relwithdebinfo suffix marks the symbols-embedded debug variant.
# Prod-flavour RWD builds (stripped + symbols archived to symbol store) keep
# the customer-facing name with no suffix.
if [[ "$BUILD_TYPE" == 'RelWithDebInfo' && "$PACKAGE_FLAVOUR" == 'debug' ]]; then
    ARTIFACT_NAME="${ARTIFACT_NAME}-relwithdebinfo"
    IMAGE_TAG="${IMAGE_TAG}-relwithdebinfo"
fi
if [[ "$MALLOC" == "true" ]]; then
    ARTIFACT_NAME="${ARTIFACT_NAME}-malloc"
    IMAGE_TAG="${IMAGE_TAG}-malloc"
fi
if [[ "$CUGRAPH" == "true" ]]; then
    ARTIFACT_NAME="${ARTIFACT_NAME}-cugraph"
    IMAGE_TAG="${IMAGE_TAG}-cugraph"
elif [[ "$CUDA" == "true" ]]; then
    ARTIFACT_NAME="${ARTIFACT_NAME}-cuda"
    IMAGE_TAG="${IMAGE_TAG}-cuda"
fi

echo "$IMAGE_TAG $ARTIFACT_NAME"
