#!/bin/bash

# Get the tag that points at HEAD
tag=$(git tag --points-at HEAD)

# If no tag is found, fall back to the current commit hash
if [ -z "$tag" ]; then
    tag=""
fi

echo "$tag"
