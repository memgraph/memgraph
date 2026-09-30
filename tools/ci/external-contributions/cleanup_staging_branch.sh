#!/bin/bash

set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <STAGING_BRANCH>"
    echo "Example: $0 staging/abc1234"
    exit 1
fi

STAGING_BRANCH="$1"

if [[ "$STAGING_BRANCH" != staging/* ]]; then
    echo "Error: branch must start with 'staging/'"
    exit 1
fi

# Delete the staging branch
git push origin --delete "$STAGING_BRANCH"
git fetch origin --prune
