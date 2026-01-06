#!/bin/bash

INPUT_REF=$1
CI_BRANCH=$2
GITHUB_REF=$3

# Get the current tag, or branch if a tag doesn't exist
# This is used later in image creation, so that the exact version of
# the Memgraph git repository can be checked out within the container,
# when we run the script to convert to "dev" container
if [ -z "$INPUT_REF" ]; then
    echo "No specific ref provided, using current branch/tag."
    # If no ref is provided, use the current branch or tag
    tag=$(./tools/ci/get_tag.sh)
else
    echo "Using provided ref: $INPUT_REF"
    tag="$INPUT_REF"
fi


# If tag is empty, fallback to the branch name
if [ -z "$tag" ]; then
    echo "No valid tag found; using branch name."
    # If CI_BRANCH is set (for PR events), use it; otherwise, use GITHUB_REF
    if [ -n "$CI_BRANCH" ]; then
        tag="$CI_BRANCH"
    else
        tag="${GITHUB_REF#refs/heads/}"
    fi
fi

echo "Using tag/branch: $tag"
echo "MEMGRAPH_REF=${tag}" >> $GITHUB_ENV
