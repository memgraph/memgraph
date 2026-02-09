#!/bin/bash
set -euo pipefail
# generate the MAGE artifact name and Docker tag

MEMGRAPH_VERSION=$1
ARCH=$2
BUILD_TYPE=$3
MALLOC=$4
CUDA=$5
CUGRAPH=$6

IMAGE_TAG="${MEMGRAPH_VERSION}"
IMAGE_TAG="${IMAGE_TAG//+/_}"
IMAGE_TAG="${IMAGE_TAG//\~/_}"
ARTIFACT_NAME="mage-${IMAGE_TAG}"

# modify artifacts/tags for specific builds
if [[ "$ARCH" == 'arm' ]]; then
    ARTIFACT_NAME="${ARTIFACT_NAME}-arm64"
fi
if [[ "$BUILD_TYPE" == 'RelWithDebInfo' ]]; then
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
