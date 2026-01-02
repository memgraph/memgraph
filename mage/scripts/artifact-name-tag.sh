#!/bin/bash
set -euo pipefail

MEMGRAPH_VERSION=$1
SHORTEN_TAG=$2
ARCH=$3
BUILD_TYPE=$4
MALLOC=$5
CUDA=$6

# remove patch version if it's 0
if [[ $SHORTEN_TAG == "true" ]]; then
    MEMGRAPH_VERSION=${MEMGRAPH_VERSION%%-*}
    memgraph_patch_version=${MEMGRAPH_VERSION##*.}
    if [[ "$memgraph_patch_version" == "0" ]]; then
        MEMGRAPH_VERSION=${MEMGRAPH_VERSION%.*}
    fi
fi

# if these match, then tag the image with the shortest vesion, e.g. 3.1.1
# rather than 3.1.1-memgraph-3.1.1
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
if [[ "$CUDA" == "true" ]]; then
    ARTIFACT_NAME="${ARTIFACT_NAME}-cuda"
    IMAGE_TAG="${IMAGE_TAG}-cuda"
fi

echo "$IMAGE_TAG $ARTIFACT_NAME"
