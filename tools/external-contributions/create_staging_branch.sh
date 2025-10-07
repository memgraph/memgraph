#!/bin/bash

# Script to create a staging branch
# Usage: ./create_staging_branch.sh <BASE_MASTER> <BRANCH_NAME> <COMMIT_SHA> <STAGING_BRANCH> <REPO_URL> <REPO_OWNER>

set -e

if [ $# -ne 4 ]; then
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
    git fetch "$REPO_URL" "$BRANCH_NAME"
fi

# Check if staging branch already exists
if git show-ref --verify --quiet refs/heads/"$STAGING_BRANCH" || git show-ref --verify --quiet refs/remotes/origin/"$STAGING_BRANCH"; then
    echo "WARNING: Staging branch '$STAGING_BRANCH' already exists. Using existing branch."
    
    # Checkout the existing staging branch
    if git show-ref --verify --quiet refs/heads/"$STAGING_BRANCH"; then
        git checkout "$STAGING_BRANCH"
    else
        git checkout -b "$STAGING_BRANCH" origin/"$STAGING_BRANCH"
    fi
    
    echo "Using existing staging branch: $STAGING_BRANCH"
else
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
            git merge "$REPO_URL" "$COMMIT_SHA" --no-edit
        fi
        
        echo "Created staging branch from master and merged feature branch"
    else
        echo "Creating direct copy of feature branch..."
        
        # Create staging branch as direct copy of feature branch
        git checkout "$COMMIT_SHA"
        git checkout -b "$STAGING_BRANCH"
        
        echo "Created direct copy of feature branch"
    fi
fi

# Push the staging branch
echo "Pushing staging branch: $STAGING_BRANCH"
git push origin "$STAGING_BRANCH"

echo "Successfully created and pushed staging branch: $STAGING_BRANCH" 
