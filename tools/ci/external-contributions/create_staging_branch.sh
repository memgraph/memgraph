#!/bin/bash

# Script to create a staging branch
# Usage: ./create_staging_branch.sh <BASE_MASTER> <BRANCH_NAME> <COMMIT_SHA> <STAGING_BRANCH> <REPO_URL> <REPO_OWNER>

set -e

if [ $# -ne 6 ]; then
    echo "Usage: $0 <BASE_MASTER> <BRANCH_NAME> <COMMIT_SHA> <STAGING_BRANCH> <REPO_URL> <REPO_OWNER>"
    echo "Example: $0 true feature-branch abc1234 staging/abc1234"
    exit 1
fi

BASE_MASTER="$1"
BRANCH_NAME="$2"
COMMIT_SHA="$3"
STAGING_BRANCH="$4"
REPO_URL="$5"
REPO_OWNER="$6"

# Fetch the PR branch or commit (handles both internal and external PRs)
echo "Fetching feature branch: $BRANCH_NAME"
if [ "$REPO_OWNER" = "memgraph" ]; then
    git fetch origin "$BRANCH_NAME"
else
    git fetch "$REPO_URL" "$BRANCH_NAME:$BRANCH_NAME"
fi

# Check existence of staging branch (local / remote)
EXISTS_LOCAL=false
EXISTS_REMOTE=false

if git show-ref --verify --quiet "refs/heads/$STAGING_BRANCH"; then
  EXISTS_LOCAL=true
fi

if git show-ref --verify --quiet "refs/remotes/origin/$STAGING_BRANCH"; then
  EXISTS_REMOTE=true
fi

# Check if staging branch already exists, delete if it does
if [[ "$EXISTS_LOCAL" == "true" || "$EXISTS_REMOTE" == "true" ]]; then
  echo "WARNING: Staging branch '$STAGING_BRANCH' already exists."

  if $EXISTS_LOCAL; then
    echo "Deleting local staging branch: $STAGING_BRANCH"
    git branch -D "$STAGING_BRANCH"
  fi

  if $EXISTS_REMOTE; then
    echo "Deleting remote staging branch: $STAGING_BRANCH"
    git push origin --delete "$STAGING_BRANCH" || true
    git fetch origin --prune
  fi
fi

if [ "$BASE_MASTER" = "true" ]; then
    echo "Creating staging branch from master and merging feature branch..."

    # Create staging branch from base reference
    git checkout master
    git pull origin master
    git checkout -b "$STAGING_BRANCH"

    # Merge the feature branch into staging
    if [ "$REPO_OWNER" = "memgraph" ]; then
        git merge "$COMMIT_SHA" --no-edit
    else
        git merge "$BRANCH_NAME" --no-edit
    fi

    echo "Created staging branch from master and merged feature branch"
else
    echo "Creating direct copy of feature branch..."

    # Create staging branch as direct copy of feature branch
    git checkout "$COMMIT_SHA"
    git checkout -b "$STAGING_BRANCH"

    echo "Created direct copy of feature branch"
fi


# Push the staging branch
echo "Pushing staging branch: $STAGING_BRANCH"
git push origin "$STAGING_BRANCH"

echo "Successfully created and pushed staging branch: $STAGING_BRANCH"
